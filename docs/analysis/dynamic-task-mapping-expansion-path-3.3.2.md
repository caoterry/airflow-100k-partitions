# Airflow 3.3.2 (core) / Task SDK 1.3.2: `@task` returning 100k strings → `.expand(account=...)`

*Source trace produced 2026-09-29. Paths are relative to `site-packages/airflow/`. Main-branch clone checked at
`airflow-src` (commit 73b07c5a, 2026-09-30).*

## 1. XCom push of the 100k list

**SDK side.** `sdk/execution_time/task_runner.py:2262-2278` decides the push is a mapping source and computes the length in-process:

```python
has_mapped_dep = next(ti.task.iter_mapped_dependants(), None) is not None
...
if not ti.is_mapped and has_mapped_dep:
    if not is_mappable_value(xcom_value): raise UnmappableXComTypePushed(xcom_value)
    mapped_length = len(xcom_value)
```

`sdk/bases/xcom.py:78-98` runs `serde.serialize` (element-wise list walk, `sdk/serde/__init__.py:218-219`) then sends
`SetXCom(value=..., mapped_length=...)` over the supervisor socket as a length-prefixed msgspec frame (`comms.py:951-960`, `187/205`).
The supervisor (`supervisor.py:1826`) forwards via `client.xcoms.set` → `sdk/api/client.py:638-640`:
`params["mapped_length"] = mapped_length; self.client.post(..., json=value)`. The ~2 MB payload is (de)serialized ~5 times
(serde, pydantic `JsonValue` in task, msgspec, pydantic in supervisor, httpx, FastAPI Body). Seconds, not a bottleneck.

**Execution API route** `api_fastapi/execution_api/routes/xcoms.py:411-427` (main: 426-427, identical):

```python
task_map = TaskMap(dag_id=..., map_index=map_index, length=mapped_length, keys=None)
max_map_length = conf.getint("core", "max_map_length", fallback=1024)
if task_map.length > max_map_length:
    raise HTTPException(400, detail={"reason": "unmappable_return_value_length", ...})
session.merge(task_map)
```

**Hard break #1:** with defaults the push of a 100k list is rejected with HTTP 400 and the producer task fails.
`AIRFLOW__CORE__MAX_MAP_LENGTH` must be ≥ N.

**TaskMap row** (`models/taskmap.py:58-107`): only `length` is stored; `keys` is `None` for lists *and* dicts on this path (the
route hard-codes `keys=None`; `from_task_instance_xcom` at 110-120, which would store dict keys, is unused by the API). One row,
PK `(dag_id, task_id, run_id, map_index)`.

**XCom storage** (`models/xcom.py:76`): `value = mapped_column(JSON().with_variant(postgresql.JSONB, "postgresql"))`; `set()` (229-250)
does DELETE + INSERT + flush, no size check. There is **no `max_xcom_size` config** in 3.3.2. Expect ~1.5-2 MB JSONB
(TOAST-compressed) for 100k short strings.

## 2. Scheduler expansion

Entry: `models/dagrun.py:1347-1348` `task_instance_scheduling_decisions` → `get_task_instances(state=State.task_states)` →
`fetch_task_instances` (874-899): `select(TI).options(joinedload(TI.dag_run))...order_by(TI.task_id, TI.map_index)` — **every TI
of the run is hydrated as an ORM object on every scheduler loop**, for each of the ≤20 runs examined (`max_dagruns_per_loop_to_schedule`).

`_get_ready_tis` (1519-1628) calls `TaskMap.expand_mapped_task` (1567) for the unexpanded `map_index=-1` TI once its deps pass.
Length comes from `get_mapped_ti_count` (`serialization/definitions/mappedoperator.py:519-542`) → `models/expandinput.py:148-152`
→ `serialization/definitions/xcom_arg.py:188-194`:

```python
query = select(TaskMap.length).where(TaskMap.dag_id == dag_id, ..., TaskMap.map_index < 0)
```

**The 100k-element XCom is never loaded into scheduler memory** (0 times per loop); only `TaskMap.length`.

**TI creation** `models/taskmap.py:259-273` (main moved it to `models/taskinstance.py:2489-2503`, still a Python loop but
`session.add` via `_add_and_prime_mapped_ti`, main taskinstance.py:218):

```python
for index in indexes_to_map:
    # TODO: Make more efficient with bulk_insert_mappings/bulk_save_mappings.
    ti = TaskInstance(task, run_id=run_id, map_index=index, state=state, dag_version_id=dag_version_id)
    task_instance_mutation_hook(ti, dag_run=dr)
    ti = session.merge(ti)
    ti.context_carrier = new_task_run_carrier(dr.context_carrier)
    ti.refresh_from_task(task, dag_run=dr)
```

`TaskInstance.__init__` sets `self.id = uuid7()` (`models/taskinstance.py:754-755`), so `session.merge` has a PK and issues **one
SELECT per TI** (N round trips), then a single `session.flush()` (line 290). *Measured on Postgres 16: the flush is emitted as N
single-row `INSERT ... VALUES (...)` statements, not a multi-row insert.* (`_create_task_instances` at 1969 *does* use
`bulk_insert_mappings`, but only for parse-time literal lengths at run creation.)

Each new TI is immediately dep-checked in the same pass (`dagrun.py:1578-1582`, `itertools.chain(schedulable_tis, additional_tis)`
→ `are_dependencies_met`). Deps = `DEFAULT_OPERATOR_DEPS` (`serialization/definitions/baseoperator.py:52-56`).
`MappedTaskUpstreamDep` (`ti_deps/deps/mapped_task_upstream_dep.py:72-80`) runs `select(TaskInstance).where(task_id.in_(...),
map_index == -1)` **per TI** → another N queries. `TriggerRuleDep._evaluate_direct_relatives` (`trigger_rule_dep.py:361-425`)
avoids the DB when upstreams need no expansion (387-389) but iterates `finished_tis` per TI (small at expansion time).

**Transaction:** everything above plus `schedule_tis` runs inside one transaction committed in `_schedule_all_dag_runs`
(`jobs/scheduler_job_runner.py:2819-2827`, `guard.commit()` after all runs). *Measured: one 5.3-minute transaction at 100k.*

**Per-loop N-proportional work after expansion:** (a) the full TI load above; (b) `_revise_map_indexes_if_mapped`
(`dagrun.py:1994-2048`) runs once per task_id per loop while any TI of that task is schedulable: `select(TI.map_index)` for all N
indexes + TaskMap query; (c) `_are_premature_tis` (1630-1647) is `any(...)`, short-circuits on the first TI whose deps pass.

## 3. Per-loop scheduling of 100k TIs

`schedule_tis` (`dagrun.py:2135-2150`) sets **all** ready TIs to SCHEDULED in the loop it sees them, chunked by `max_tis_per_query`
(default 16) → 6,250 UPDATEs at 100k in one transaction.

**EmptyOperator short-circuit** applies to mapped operators: `is_schedulable` (`models/taskinstance.py:2379-2385`) tests
`not task.inherits_from_empty_operator or has_on_execute_callback or has_on_success_callback or outlets or inlets`;
`SerializedMappedOperator.inherits_from_empty_operator` returns `self._is_empty` (`serialization/definitions/mappedoperator.py:181-183`),
set at serialization from the SDK operator (`serialized_objects.py:1059-1060`). Disabled by any of those four attributes.

**Queueing** `_critical_section_enqueue_task_instances` (`scheduler_job_runner.py:1174-1178`):

```python
max_tis = min(self.job.max_tis_per_query, self._parallelism - num_occupied_slots)
```

= min(16, 32 − running) TIs per loop with defaults. The query (645-725) joins dag_run/dag_model, applies
`row_number() over (partition by dag_id, run_id order by -priority_weight, logical_date, map_index)` and
`row_num <= DM.max_active_tasks` (16), then `.limit(max_tis)` with `FOR UPDATE SKIP LOCKED`. The window function sorts **all
SCHEDULED TIs of the run** each loop — O(N log N) in Postgres. Also `pool_slots_free` caps (604-610; default_pool 128). Loop does
not sleep while work is queued (`1869-1874`).

**Indexes** (`models/taskinstance.py:668-678`): `ti_dag_state(dag_id,state)`, `ti_dag_run(dag_id,run_id)`, `ti_state(state)`,
`ti_state_lkp(dag_id,task_id,run_id,state)`, `ti_pool(pool,state,priority_weight)`, PK `id`, UNIQUE `(dag_id,task_id,run_id,map_index)`.
No index covers `(dag_id, run_id, state)` together.

Throughput ceiling: ≤16 TIs queued per loop (default), and each loop hydrates N TIs.

## 4. Task SDK overhead per mapped TI (LocalExecutor)

- Worker pool: `executors/local_executor.py:78-108`, one `run_workload` → `supervise_task` per TI (`base_executor.py:702-706`).
  Supervisor `os.fork()` at `supervisor.py:699`; on macOS `_FORK_EXEC_PLATFORMS = {"darwin"}` (488) → fork+exec a **fresh interpreter
  per TI** (full `airflow` import).
- API calls per TI: `task_instances.start` (1460 → `ti_run` route, includes XCom-key query 285-293), `set_rtif` (1834 → `ti_put_rtif`
  → `update_rtif` `taskinstance.py:1767-1773`: upsert + `delete_old_records` DELETE…NOT IN(subquery) per TI,
  `num_dag_runs_to_retain_rendered_fields`=30), `GetXCom`, `succeed` (1571), heartbeats only if the task lives ≥5 s
  (`MIN_HEARTBEAT_INTERVAL`, 1702). Plus `SetXCom` if the mapped task returns a value (100k XCom rows).
- DAG file is **re-parsed in every task process**: `startup()` → `parse(msg)` (`task_runner.py:1204`, `1005-1014` BundleDagBag).
- **Hard cost #2 — whole-list pull per TI.** `mappedoperator.py:840-841` → `expandinput.py:187-207` → `xcom_arg.py:338-341`:

```python
tg = self.operator.get_closest_mapped_task_group()
if tg is None:
    # No mapped task group - pull from unmapped instance
    map_indexes = None
```

→ `_normalize_xcom_pull_params` gives `[None]` (task_runner.py:469-470) → `XCom.get_one(map_index=None)` → `GetXCom` →
`client.xcoms.get` (client.py:592-598, no `map_index` param → server default −1) → `get_xcom` returns the whole JSONB
(xcoms.py:309-352) → back through the supervisor socket → `deserialize` → `value[found_index]` (`expandinput.py:171-172`).
**Each of the N TIs downloads and deserializes the full N-element list**: O(N²) bytes ≈ 200 GB of JSON through the API server
at 100k. `LazyXComSequence`/offset routes exist only for *mapped* upstreams (xcom_arg.py:336-337).
- RTIF row: `op_kwargs={"account": "..."}` — tiny; truncation at `core.max_templated_field_length` 4096 (task_runner.py:1314).
- Log: `logging.log_filename_template` `.../map_index={{ ti.map_index }}/attempt=N.log` → N directories.

## 5. UI / API

- Grid `routes/ui/grid.py:498-525`: `select(task_id,state,...).where(dag_id, run_id).order_by(task_id).execution_options(yield_per=1000)`
  then `_build_ti_summaries` `add_ti` per row (375-397): **all N rows streamed and aggregated in Python per run per refresh** — O(N) per grid poll.
- `GET .../taskInstances/{task_id}/listMapped` (`routes/public/task_instances.py:170-285`) is paginated via `paginated_select`
  (`common/db/common.py:180-201`), page ≤ `api.maximum_page_limit` (100), plus a `COUNT(*)` over the filtered set per page.
  `GET .../taskInstances` (456-680) has a cursor mode with bounded totals. Nothing loads all map indexes unpaginated except the grid summary aggregation.

## 6. Nested mapping

Not allowed. `sdk/definitions/mappedoperator.py:349-350`: `if self.get_closest_mapped_task_group() is not None: raise
NotImplementedError("operator expansion in an expanded task group is not yet supported")`; and
`sdk/definitions/_internal/expandinput.py:96-99`: `raise ValueError("Nested Mapped TaskGroups are not yet supported")`. Same in main (352 / 99).

## 7. Config knobs encountered

| key | default | read at |
|---|---|---|
| core.max_map_length | 1024 | xcoms.py:420 |
| core.parallelism | 32 | scheduler_job_runner.py:348 |
| core.max_active_tasks_per_dag | 16 | DM.max_active_tasks, scheduler_job_runner.py:663,715 |
| core.default_pool_task_slot_count | 128 | Pool.slots_stats, 600-610 |
| scheduler.max_tis_per_query | 16 | 1175-1178; dagrun.py:2137,2186 |
| scheduler.max_dagruns_per_loop_to_schedule | 20 | `get_running_dag_runs_to_examine` |
| scheduler.scheduler_idle_sleep_time | 1 | 320,1874 |
| scheduler.task_instance_heartbeat_timeout | 300 | supervisor.py:181 |
| workers.min_heartbeat_interval / max_failed_heartbeats | 5 / 3 | supervisor.py:183-184 |
| workers.execution_api_retries / wait_min / wait_max | 5 / 1.0 / 90.0 | client.py:1130-1132 |
| core.max_templated_field_length | 4096 | task_runner.py:1314 |
| core.num_dag_runs_to_retain_rendered_fields | 30 | renderedtifields.py:282 |
| api.maximum_page_limit / fallback_page_limit | 100 / 50 | parameters.py:84,154 |
| api.workers | 1 | uvicorn |
| logging.log_filename_template | map_index=… path | sdk/log.py |
| (no) core.max_xcom_size | — | absent in 3.3.2 |

## Ranked predicted bottlenecks (before measurement)

1. **Push rejected by `max_map_length`** (all N > 1024). Fix before benchmarking: raise the knob.
2. **Whole-list XCom pull per mapped TI** (xcom_arg.py:338-364). 10k: ~150 KB × 10k = 1.5 GB. 30k: ~500 KB × 30k = 15 GB.
   100k: ~2 MB × 100k ≈ 200 GB through the API server. Metric: `GET /execution/xcoms/...` bytes and p95 latency; API-server CPU.
3. **Expansion loop with per-TI SELECT+merge and per-TI `MappedTaskUpstreamDep` query, one transaction** (taskmap.py:259-290;
   mapped_task_upstream_dep.py:72-80). Metric: scheduler loop duration, `pg_stat_activity` transaction age. *Measured: 61 s / 117 s / 317 s at 10k / 30k / 100k.*
4. **Per-loop full TI hydration** (dagrun.py:1348, 874-899) plus `_revise_map_indexes_if_mapped` map_index scan (2016-2023) and the
   window-function critical-section query (scheduler_job_runner.py:688-725). Scales linearly with N for the run's whole life.
5. **Queue throughput ceiling `min(max_tis_per_query, parallelism−occupied)` per loop and `max_active_tasks_per_dag`.**
6. **Per-TI process + DAG re-parse + ~5 API calls** (supervisor.py:699/488, task_runner.py:1204, RTIF delete per TI).
7. **Grid UI summary aggregation** (grid.py:498-525): O(N) rows per poll per run.
8. **Filesystem/log and RTIF/XCom row volume**: N log directories, N RTIF rows, N XCom rows if the task returns.
