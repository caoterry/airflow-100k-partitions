# Airflow native asset partitions (AIP-76): 3.3.2 vs main, and the 100k-account question

*Source analysis produced 2026-09-29 against `apache/airflow` main `73b07c5` (3.4.0-dev) and the installed
`apache-airflow-core 3.3.2` / `task-sdk 1.3.2`. Line numbers refer to main unless noted; every partition-related file was
diffed against 3.3.2 and is byte-identical unless flagged **main-only**.*

## 1. Data model

All key columns are `StringID()` = `String(250)` (`core/models/base.py:59,86`).

| Table / column | Migration (version) | Meaning of one row / notes |
|---|---|---|
| `asset_event.partition_key` (nullable) | 0095 `665854ef0536` (3.2.0) | One event = one upstream (asset, key). Index `idx_asset_event_asset_id_partition_key(asset_id, partition_key)` is **main-only** (0127, tagged 3.4.0; `models/asset.py:864`). 3.3.2 has only `(asset_id, timestamp)`. |
| `dag_run.partition_key`, `dag_run.partition_date` | 0095 / 0107 (3.2.0) | A DagRun carries **exactly one** key (`str \| None`, type-checked `dagrun.py:484-488`). **No index on either column**; `__table_args__` (`dagrun.py:331-354`) only has dag_id/state/run_after/version indexes. |
| `asset_partition_dag_run` (APDR): `id` PK, `target_dag_id`, `partition_key`, `created_dag_run_id` FK→`dag_run.id` CASCADE, `partition_date` (0123, 3.3.0), `rollup_fingerprint` JSON (0121, 3.3.0), `created_at/updated_at` | 0095 (3.2.0) | A *provisional* downstream run for `(target_dag_id, partition_key)`. `created_dag_run_id IS NULL` = still waiting. Docstring: "Rows are never deleted… each dag run that gets created leaves its APDR record behind" (`models/asset.py:950-953`). **Only index is the PK**; no index on `(target_dag_id, partition_key)` nor `created_dag_run_id`, no unique constraint (duplicates tolerated, "always work on the latest"). |
| `partitioned_asset_key_log` (PAKL): `id`, `asset_id`, `asset_event_id`, `asset_partition_dag_run_id`, `source_partition_key`, `target_dag_id`, `target_partition_key`, `created_at` | 0095; index `idx_pakl_apdr_id` added 0121 (3.3.0) | One row = "event E (key K_src) contributed to APDR A as key K_dst". **No FKs at all**; `db clean` purges only rows whose APDR is gone (`utils/db_cleanup.py:242-255`); APDR itself is not in the cleanup table list. |
| `dag.timetable_partitioned`, `next_dagrun_partition_key`, `next_dagrun_partition_date` | 0107 (3.2.0) | Per-DAG flag/cursor (`models/dag.py:362,404-405`). |
| `dag.partition_mapper_info` JSON NOT NULL | 0120 (3.3.0) | Cached `[{name,uri,is_rollup}]` so the UI avoids deserializing timetables (`timetables/base.py:46`). |
| `backfill_dag_run.partition_key`; `dag_run.created_at` | 0106 (3.2.0) | Backfill bookkeeping by key. |

3.3.2's migration head is `0123`; nothing partition-related between 0124–0140 except 0127.

## 2. Authoring API

There is **no** `Asset(partition=...)` and no `DAG(partitioned=...)`. Partitioning is a *timetable* property:
`Timetable.partitioned` / `partitioned_at_runtime` (`core/timetables/base.py:233,240`; sdk `bases/timetable.py:50`).
Three timetables (all exported from `airflow.sdk`, identical in 3.3.2):

- `CronPartitionTimetable(cron, *, timezone, run_offset: int|None, key_format="%Y-%m-%dT%H:%M:%S", run_immediately)` — producer side, `partitioned=True`; key = formatted cron tick (`trigger.py:364-415`). `run_offset` must be int: "Run offset other than integer not supported yet." (`:412`).
- `PartitionedAssetTimetable(assets, partition_mapper_config: dict[BaseAsset, PartitionMapper] = {}, default_partition_mapper=IdentityMapper())` — consumer side (`sdk/definitions/timetables/assets.py`; core `simple.py:275-329`). Asset aliases: "Partitioned Asset Alias is not supported." (`simple.py:314`).
- `PartitionedAtRuntime()` — `NullTimetable` subclass, `can_be_scheduled=False`, `partitioned_at_runtime=True` (`simple.py:189-208`); keys are emitted by the task via `outlet_events[asset].add_partitions(str | list[str])` (`sdk/execution_time/context.py:1097-1121`; validates non-empty, ≤250 chars, rejects aliases). Each key becomes its own `AssetEvent` (`task_runner.py:1418-1423`); duplicates collapse via a `set`. Any timetable may set `partitioned_at_runtime=True` (example plugin `custom_partition_timetable.py` does so on a `CronTriggerTimetable`).

Mappers (`airflow.sdk`, sdk `definitions/partition_mappers/*`, core runtime in `core/partition_mappers/*`): `IdentityMapper`,
`StartOf{Hour,Day,Week,Month,Quarter,Year}Mapper(timezone, input_format, output_format)`, `ProductMapper(m0, m1, ..., delimiter="|")`,
`ChainMapper`, `AllowedKeyMapper([...])` (raises `ValueError` for unknown keys → event dropped with a `Log` row), `FixedKeyMapper(key)`,
`RollupMapper(upstream_mapper=, window=, wait_policy=WaitForAll()|MinimumCount(n))`, `FanOutMapper(upstream_mapper=, window=, downstream_mapper=, max_downstream_keys=)`,
windows `Hour/Day/Week/Month/Quarter/YearWindow(direction=)`, `SegmentWindow([...])`. Custom mappers/windows/timetables register via
`AirflowPlugin.partition_mappers / windows / timetables` (`example_dags/plugins/*`).

Minimal example — verified to import and build against 3.3.2:

```python
from airflow.sdk import DAG, Asset, task, PartitionedAtRuntime, PartitionedAssetTimetable, IdentityMapper
src = Asset(name="acct_positions", uri="s3://bucket/positions")

with DAG("load_positions", schedule=PartitionedAtRuntime()):      # or any cron timetable
    @task(outlets=[src])
    def load(*, outlet_events=None):
        outlet_events[src].add_partitions(["ACCT00012345", "ACCT00012346"])
    load()

with DAG("risk_per_account", schedule=PartitionedAssetTimetable(
        assets=src, default_partition_mapper=IdentityMapper())):
    @task
    def compute(dag_run=None): print(dag_run.partition_key)   # also {{ partition_key }} in templates
    compute()
```

## 3. Scheduling semantics

**Write path (inside the TI-success Execution API request**, `execution_api/routes/task_instances.py:745` →
`TaskInstance.register_asset_changes_in_db`, `models/taskinstance.py:1627-1709`): per emitted key,
`effective_pk = payload.partition_key or dag_run.partition_key`; `partition_date` is carried only when they match.
`AssetManager.register_asset_change` (`assets/manager.py:272`) inserts the `AssetEvent`, then `_queue_dagruns` (`:507-537`):
partitioned consumers go to `_queue_partitioned_dags`; **partitioned events never queue non-partition-aware DAGs** (`:534-535`).
`_queue_partitioned_dags` (`:540-702`) per consumer: `SerializedDagModel.get` + deserialize, `compute_rollup_fingerprint`,
`mapper.to_downstream(key)` (→ one or many target keys; cap = per-mapper `max_downstream_keys` or
`[scheduler] partition_mapper_max_downstream_keys`=1000, exceeded ⇒ *nothing queued* + `Log("partition fan-out exceeded")`),
then per target key `_get_or_create_apdr` (`:705-798`) under `_lock_asset_model` — a row lock on the asset
(`FOR … KEY SHARE/NO KEY UPDATE`, `:71-129`) — `SELECT … WHERE partition_key=? AND target_dag_id=? ORDER BY id DESC LIMIT 1`;
reuse if pending, else insert; then insert one PAKL row.

**Scheduler tick** `_create_dagruns_for_partitioned_asset_dags` (`scheduler_job_runner.py:2332-2560`, called first in
`_create_dagruns_for_dags:2572`): select pending APDRs (`created_dag_run_id IS NULL`, DAG not paused/draining/stale)
`ORDER BY created_at, id LIMIT 500` (`MAX_PARTITION_DAG_RUNS_PER_LOOP=500`, `:178`, not configurable) `FOR UPDATE SKIP LOCKED`;
drop APDRs whose `rollup_fingerprint` mismatches the current timetable (deletes their PAKL rows); load PAKL rows for the batch
joined to *active* assets; for each APDR compute per-asset satisfaction: non-rollup asset = satisfied if any PAKL row exists;
rollup = `wait_policy.is_satisfied_by_keys(matched=received_keys, expected=mapper.to_upstream(key))` (`:2205-2258`); a mapper
exception ⇒ held forever, logged. Then `AssetEvaluator.run(asset_condition, statuses)` (so `&`/`|` conditions work per key).
If satisfied: `create_dagrun(run_type=ASSET_TRIGGERED, logical_date=None, partition_key, partition_date=_resolve_partition_date(...), state=QUEUED)`,
attach consumed events, set `apdr.created_dag_run_id`. Comment `:2355-2358`: created **regardless of `max_active_runs`**;
gating happens at QUEUED→RUNNING. No per-key mutual exclusion; a later event for a key whose APDR already fired creates a
*new* APDR and a *new* run.

`CronPartitionTimetable` schedules via `next_dagrun_info_v2` (`trigger.py:547`), one run per tick with
`partition_key=_format_key(tick±offset)`; run_id gets `__{partition_key}__{random}` (`:588-597`). `PartitionedAtRuntime` never
schedules; producer `DagRun.partition_key` is only set if supplied at trigger time (`simple.py:193-199`). **Discrepancy:** docs
`assets.rst:991-992` claim single-key runtime runs "back-fill" `dag_run.partition_key`; the core docstring says they do **not**,
and no code path writes it (3.3.1 #67718 "provenance-only"). Treat the doc as stale.

## 4. Operational API

- `POST /dags/{dag_id}/dagRuns` body `partition_key` (`datamodels/dag_run.py:237`, validated by `SerializedDAG.validate_partition_key`
  `serialization/definitions/dag.py:547-578`: str, non-blank, ≤250, DAG must be `partitioned` or `partitioned_at_runtime`, else
  `DagNotPartitionedError`/`InvalidPartitionKeyError`, `exceptions.py:148-158`).
- `GET /dags/{dag_id}/dagRuns`: `partition_key_pattern`, `partition_key_prefix_pattern`, `partition_date_gte/lte`
  (3.3.2 `routes/public/dag_run.py:564-613`; date filters 3.3.1 #70304).
- `POST /dags/{dag_id}/clearDagRuns` with `PartitionSelectorMixin` (`partition_key` **or** `partition_date_start/end`, exactly one
  selector; `datamodels/dag_run.py:62-116,148`) and `POST /dags/{dag_id}/clearPartitions` (`ClearPartitionsBody`:
  `run_id|partition_key|date window`, `clear_task_instances`, `dry_run`) → `clear_partition_runs` (`dagrun.py:2448`) which **nulls**
  `partition_key/partition_date` (optionally re-queues TIs). Permission `DAG.RUN PUT` (`docs/security/api_permissions_ref.rst:274`).
- UI: `GET /ui/partitioned_dag_runs?dag_id&has_created_dag_run_id` and `GET /ui/pending_partitioned_dag_run/{dag_id}?partition_key=`
  (`routes/ui/partitioned_dag_runs.py`) with per-asset `received_keys/required_keys/is_rollup/mapper_error/asset_inactive`.
  **`limit/offset` pagination is main-only**; 3.3.2 returns every APDR row and runs rollup Python per row. Frontend:
  `AssetSchedule.tsx`, `PartitionScheduleModal.tsx`, `AssetProgressCell.tsx`, `PartitionPreviewTable.tsx`, run/trigger forms.
- Backfill: `airflow backfill create --from-date/--to-date` auto-detects partitioned DAGs and iterates `iter_partition_dagrun_infos`
  (`backfill.rst:97-113`; `backfill.py:491-535,753`). Implemented **only** by `CronPartitionTimetable` (`trigger.py:470`); base raises
  `NotImplementedError` (`base.py:261-283`) ⇒ no backfill for `PartitionedAssetTimetable`/runtime-partitioned DAGs, and no key-list backfill.
- CLI: `airflow partitions clear -d … [--run-id|-k KEY|-s/-e|--date a~b] [--clear-task-instances] [--dry-run]` (`partition_command.py`),
  `airflow dags clear` (`dag_command.py:136`), `dags next-execution` prints partition columns.
- Execution API: `2026-04-06 AddPartitionKeyField`, `2026-06-30 AddPartitionDateField` + `AddConsumedAssetEventPartitionKeyField`;
  task context `partition_key`/`partition_date` (`task_runner.py:365-366`; templates "Added in version 3.3.0", `templates-ref.rst:90-94`).
- **main-only:** `GET /assets/events` and Execution-API asset-event `partition_key` / `partition_key_regexp_pattern` filters
  (`routes/public/assets.py`, `execution_api/routes/asset_events.py`), SDK `inlet_events[a].partition_key(k)/.partition_key_regexp_pattern(p)`
  (`sdk/execution_time/context.py:1255-1263`), `[api] regexp_query_timeout` (config `version_added: 3.4.0`), `is_scheduled` DAG filter
  treating `PartitionedAtRuntime` as unscheduled.

## 5. Scale analysis: 100,000 account keys/day, one producer asset, one consumer DAG

Key format: free-form string, 1–250 chars, not whitespace-only; `ACCT00012345` is fine (`|` is only special to `ProductMapper`;
`/` only affects the UI path, which is why `partition_key` is a query param). No enumeration/registry of partitions exists
anywhere; `AllowedKeyMapper` is a Python list in the DAG file (impractical at 100k).

| Table | Rows/day | Growth | Hot query & index status |
|---|---|---|---|
| `asset_event` | 100k | never pruned except `db clean` | exact-key lookup indexed on **main only** |
| `asset_partition_dag_run` | 100k | **never deleted**, not in `db clean` (36.5M/yr) | (a) per-event `WHERE partition_key AND target_dag_id ORDER BY id DESC` — **unindexed seq scan per key**; (b) scheduler `WHERE created_dag_run_id IS NULL ORDER BY created_at LIMIT 500 FOR UPDATE` — **unindexed**; (c) UI `WHERE target_dag_id ORDER BY created_at DESC` — unindexed, unpaginated in 3.3.2 |
| `partitioned_asset_key_log` | 100k × consumers × fan-out | orphans only pruned | batch fetch by `asset_partition_dag_run_id` — indexed (3.3.0) |
| `dag_run` (+`task_instance` × tasks) | 100k | normal | filters/clears by `partition_key`/`partition_date` scan within `idx_dag_run_dag_id` |

Structural costs, both versions:
1. **Write path is O(keys × consumers) in one API request** (§3): 100k `add_partitions` keys from one task ⇒ 100k × (DAG deserialization + ≥5 queries + asset row lock) inside a single TI-state-update transaction. Mapped tasks emitting one account each serialize on the same asset row lock. Expect timeouts long before 100k.
2. **Scheduler drain ≤500 runs/tick** with FIFO by `created_at` and no rotation: comment `:2372-2377` — a permanently unsatisfiable APDR at the head "blocks newer ones"; with >500 stuck `WaitForAll` rollups the satisfiable ones are never selected. A 100k burst needs ≥200 ticks each doing 500 `create_dagrun`s.
3. **Throughput ceiling is `dag_run`, not partitions**: 100k runs/day = 1.16 runs/s; at `max_active_runs=16` each run must complete in ≤13.8 s on average, and queued runs pile up as 100k `dag_run` rows in QUEUED.
4. No config caps partition count; only the fan-out cap (1000) and the 500/tick constant.

## 6. Gaps vs Dagster, and contribution opportunities

- **Per-partition materialization status / missing-partition view:** none. Only the *pending* APDR progress view exists; "materialized" must be derived from `dag_run.partition_key` (unindexed). No partition definition, so "missing" is undefined.
- **Subset backfill:** only contiguous `partition_date` ranges for `CronPartitionTimetable`; no arbitrary key list, no backfill for asset-partitioned or runtime-partitioned DAGs. Workaround is one `POST /dagRuns` per key.
- **Partition mappings:** 1:1, N:1 rollup, 1:N fan-out, categorical only. Sliding/overlapping windows "cannot be expressed" (`window.py` docstring, AIP-76 "modifies-past-2-hours"); chained fan-out `TypeError` (`temporal.py:548-553`); `run_offset` int only; aliases unsupported.
- **Stale detection:** none by data. `rollup_fingerprint` only invalidates pending APDRs when the mapper/window *definition* changes. Re-emitting a key simply creates another run (no dedup, no "already materialized").
- Concrete PRs visible from the code: (1) indexes `asset_partition_dag_run(target_dag_id, partition_key, id)` and partial `(created_dag_run_id) WHERE NULL`; `dag_run(dag_id, partition_key)`, `(dag_id, partition_date)`; (2) batch `_queue_partitioned_dags` (cache serdag/mapper per consumer per request, bulk-insert APDR/PAKL); (3) backport `/ui/partitioned_dag_runs` pagination to 3.3.x; (4) APDR retention in `db_cleanup`; (5) `iter_partition_dagrun_infos` for `PartitionedAssetTimetable` + a `partition_keys: list[str]` backfill/clear selector; (6) readiness-aware APDR selection to avoid FIFO starvation; (7) make `MAX_PARTITION_DAG_RUNS_PER_LOOP` a config; (8) doc/docstring drift (`assets.rst:991` back-fill; `manager.py:719` references a non-existent `AssetPartitionDagRunMutexLock` table); (9) `GH-52141` TODO on keeping SDK/core `can_be_scheduled` in sync (`simple.py:97,154`).

## 7. Version matrix (evidence: `RELEASE_NOTES.rst` headers L27/236/470/1317, migration `airflow_version` tags, 3.3.2 diffs)

| Feature | First version | Evidence |
|---|---|---|
| APDR/PAKL tables, `asset_event.partition_key`, `dag_run.partition_key/partition_date`, `dag.timetable_partitioned`, `backfill_dag_run.partition_key` | 3.2.0 | migrations 0095/0106/0107 tagged 3.2.0; RN L1323 "headline feature" |
| `CronPartitionTimetable`, `PartitionedAssetTimetable`, `Identity/StartOf*/Product/Chain/AllowedKey` mappers, Exec-API 2026-04-06 `partition_key`, partitioned-runs UI view, docs | 3.2.0 | RN L1703-1704, 1826, 1871, 1968 |
| Per-Dag authz on `partitioned_dag_runs`; backfill `partition_date` fix | 3.2.1/3.2.2 | RN L1161, 1120 |
| `RollupMapper`, `FanOutMapper`, `FixedKeyMapper`+`SegmentWindow`, windows, `WaitForAll`/`MinimumCount`, `PartitionedAtRuntime`, `add_partitions`, `partition_key` in context/templates, `partitions clear` & `dags clear` CLI, `clearPartitions` REST + selectors, partition-range backfill, plugin registries, `[scheduler] partition_mapper_max_downstream_keys`, `dag.partition_mapper_info`, `rollup_fingerprint`, APDR `partition_date`, Exec-API 2026-06-30 fields, key validation exceptions | 3.3.0 | RN L476-487, 596-705; migrations 0120/0121/0123; config `version_added: 3.3.0` |
| `partition_date_gte/lte` on `GET /dagRuns`; `FanOutMapper`/wait policies exported from `airflow.partition_mappers`; many fixes | 3.3.1 | RN L334-456 |
| Asset-event `partition_key` gated behind Exec-API 2026-06-30 (#72827); Dag-existence disclosure fix on partitioned-runs listing (#72660) | 3.3.2 | RN L105, 125 |
| `idx_asset_event_asset_id_partition_key` (0127), asset-event `partition_key`/regexp filters (public + Exec API + SDK inlet accessor), `[api] regexp_query_timeout`, `/ui/partitioned_dag_runs` pagination, `is_scheduled` filter, draining-state exclusions | **main-only (3.4.0-dev)** | 3.3.2 diffs; `airflow_version="3.4.0"`; config `version_added: 3.4.0` |

**Bottom line:** 3.3.2 can *represent* 100k free-form account keys, and the authoring model (runtime `add_partitions` →
`PartitionedAssetTimetable` with `IdentityMapper`) is correct for it. It cannot *operate* at that scale as shipped: the per-key
write path runs inside one API request under an asset-level lock with unindexed APDR lookups, the scheduler drains 500
provisional runs per tick FIFO, APDR rows are never pruned, and there is no per-partition status, key-list backfill, or stale
detection. Those are the upstream contributions to make first.
