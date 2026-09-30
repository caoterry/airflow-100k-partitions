# Can Apache Airflow orchestrate 100k firm-account partitions?

**Status: draft — phase-2 benchmarks and the literature/MWAA review are still running; sections marked ⏳ will be completed.**

Date: 2026-09-29 · Airflow under test: 3.3.2 (company baseline 3.3.0) plus `apache/airflow` main (3.4.0-dev, commit 73b07c5) for
source study · Repo: https://github.com/caoterry/airflow-100k-partitions

## 1. Executive summary

⏳ (written last, from the measurements below)

## 2. The question, and the assumptions we had to make

A shared orchestration platform (balance-sheet + revenue teams) is being rebuilt cloud-native on Airflow or AWS MWAA. The revenue
team needs *partitioned processing keyed by firm account*, 30k–100k accounts. Dagster's partition model fits better but is not
allowed. "Can Airflow be pushed to 100k partitions?" splits into two questions that have different answers:

- **Fan-out:** can one cycle *run* 100k units of work?
- **Partition state:** can Airflow *track* 100k partitions (which account is materialized, re-run a chosen subset, history per account)?

Working assumptions (nobody was available to answer clarifying questions; see `docs/superpowers/specs/2026-09-29-airflow-100k-partitions-design.md`):
per-account work runs on an external engine and takes seconds to minutes; the cycle is daily; MWAA is the binding platform
constraint; experiments may use the newest 3.x.

## 3. What Airflow already ships for partitions (3.2 → 3.3 → main)

This changed the evaluation. AIP-76 "Asset Partitions" is not a proposal any more: **Airflow 3.2.0 (2026-04) shipped the data model
and 3.3.0 (2026-07) shipped the authoring API**, so the company's 3.3.0 baseline already has it. Full source analysis:
`docs/analysis/native-partitions-3.3.2-vs-main.md`.

- Producer: `outlet_events[asset].add_partitions([...keys])` inside a task (DAG on `PartitionedAtRuntime()` or any timetable).
- Consumer: `DAG(schedule=PartitionedAssetTimetable(assets=asset, default_partition_mapper=IdentityMapper()))` → **one DagRun per
  partition key**, `dag_run.partition_key` set, `{{ partition_key }}` in templates.
- Mappers: identity, temporal roll-ups (`StartOfDayMapper`…), `ProductMapper`, `FanOutMapper` (cap 1000), `AllowedKeyMapper`, `ChainMapper`.
- Operations: `GET /dagRuns?partition_key_pattern=…`, `POST /dags/{id}/clearPartitions`, `airflow partitions clear`, a
  "pending partitions" UI view.
- Not there: per-partition materialization status/"missing partitions" view, key-list backfill (only date ranges, only for
  `CronPartitionTimetable`), stale detection, dedup of re-emitted keys, retention for `asset_partition_dag_run` (rows are never deleted).

Keys are free-form strings ≤250 chars, so `ACCT00012345` works. Nothing caps the number of distinct keys.

## 4. Experiments

Environment: MacBook (8 cores, 8 GB RAM), Postgres 16 in Docker (`shared_buffers=512MB`, `synchronous_commit=off`), Airflow 3.3.2
in a Python 3.12 venv, LocalExecutor `parallelism=12`, `max_map_length=200000`, `max_active_tasks_per_dag=200000`, default pool
200000 slots, `max_tis_per_query=512`, everything else default. Harness: `bench/harness.py`; raw results: `bench/results/<label>/`.

### 4.1 Scenario A — flat dynamic task mapping, scheduler-only (`bench_flat_empty`)

One task returns N account ids; a mapped `EmptyOperator` subclass expands over them. The scheduler marks EmptyOperator instances
success without sending them to the executor, so this isolates **expansion + scheduling + metadata DB** cost.

| N | run duration | scheduler main loop blocked | scheduler peak RSS | API: list 100 TIs @offset 90k | UI grid summary |
|---|---|---|---|---|---|
| 1,000 | 12 s | – | 191 MB | – | 4 ms |
| 10,000 | 60 s | **61.5 s** | 313 MB | – | – |
| 30,000 | 116 s | **116.5 s** | 555 MB | – | – |
| 100,000 | 328 s | **316.7 s** | 959 MB | 381 ms | 6 ms |

It works, and it is close to linear (~3 ms per task instance at 100k). But the whole expansion happens **inside one scheduler loop
iteration and one database transaction**: `pg_stat_activity` showed a single transaction open for 5.3 minutes issuing 100,000
single-row `INSERT INTO task_instance …` statements followed by ORM `SELECT`s, while the scheduler's own heartbeat stopped
("Heartbeat recovered after 316.65 seconds"). Postgres was idle most of that time (`idle in transaction`, only three statements
over 100 ms); the cost is Python/ORM work in the scheduler process.

Why this is disqualifying rather than merely slow: the default scheduler health threshold is 30 s. A Kubernetes liveness probe
(`airflow jobs check`) or MWAA's managed health check would restart the scheduler mid-transaction, the transaction rolls back, the
run is picked up again and the cycle repeats. **Above roughly 5k mapped instances per task the expansion transaction already
outlives the default health threshold.**

The REST API and the endpoints the grid UI calls stayed interactive at 100k (all under 400 ms).

### 4.2 Scenario F — native partitions (`bench_partition_producer` → `bench_partition_consumer`)

Producer emits N keys via `add_partitions`; consumer has `PartitionedAssetTimetable` + `IdentityMapper` and one EmptyOperator.

| N (emitters) | events written | all N runs created | all N runs finished | scheduler peak RSS | heartbeat gaps |
|---|---|---|---|---|---|
| 1,000 (1) | 12 s | 22 s | 41 s | 116 MB | none |
| 10,000 (1) | 76 s | 219 s | 426 s | 158 MB | none |
| 100,000 (200 × 500, serialized) | ⏳ | ⏳ | ⏳ | ⏳ | ⏳ |

Two things the 10k run exposed:

1. **The write path is one HTTP request.** Registering 10k keys happened inside the task-success call to the Execution API and took
   **53 s (≈5.3 ms per key)**. The SDK client timeout (`[workers] execution_api_timeout`) defaults to **5 s**, so the client retried
   five times; all six requests queued on the asset row lock and completed within the same 12 ms. The task process was then killed
   ("Server indicated the task shouldn't be running anymore"), although the TI ended as `success` and no duplicate rows were written
   (10,000 distinct events / APDR / PAKL rows). Practical consequence: **at default settings a single task can emit at most ~900
   keys**, and concurrent emitters serialize on the asset lock, so they must be run one at a time.
2. **Run creation is capped at 500 per scheduler tick** (`MAX_PARTITION_DAG_RUNS_PER_LOOP`, hard-coded). Measured steady state:
   ~80 runs/s. 100k keys ⇒ ~21 minutes of run creation per cycle, single-threaded, before any work runs.

### 4.3 Does partition storage grow as n²? (question raised by a colleague's analysis)

No, not for identity (one account = one key) mapping. Row deltas after each run:

| table | 1k keys | 10k keys | exponent |
|---|---|---|---|
| `asset_event` | 1,000 | 10,000 | 1.00 |
| `asset_partition_dag_run` (APDR) | 1,000 | 10,000 | 1.00 |
| `partitioned_asset_key_log` (PAKL) | 1,000 | 10,000 | 1.00 |
| `dagrun_asset_event` | 1,000 | 10,000 | 1.00 |
| `dag_run` / `task_instance` (consumer) | 1,000 | 10,000 | 1.00 |

The code confirms why: when a partition run is created, only the events joined through *its own* PAKL rows are attached
(`scheduler_job_runner.py`, `PartitionedAssetKeyLog.asset_partition_dag_run_id == apdr.id`), not all events of the asset.
Quadratic growth would need a cross-product mapper (`FanOutMapper`/`ProductMapper`, bounded by `partition_mapper_max_downstream_keys`=1000).

The real storage problem is different: `asset_partition_dag_run` rows are **never deleted and are excluded from `airflow db clean`**
(see `models/asset.py` docstring and `utils/db_cleanup.py`), and the table has **no index other than the primary key** although the
per-key write path queries it by `(partition_key, target_dag_id)`. That is linear, unbounded growth on an unindexed hot table:
36.5 M rows/year at 100k keys/day. Separately, `task_instance` measured ~1 KB per row including indexes (142 MB for ~150k rows), so
100k task instances/day is ~36 GB/year without retention.

The genuinely quadratic behaviour in Airflow at this scale is elsewhere — in dynamic task mapping, not partitions: **each of the N
mapped task instances downloads the entire N-element upstream XCom** at start (`xcom_arg.py:338`, "No mapped task group - pull from
unmapped instance"), i.e. O(N²) bytes through the API server (≈200 GB at 100k). ⏳ measured in Scenario B.

### 4.4 Scenario B — flat mapping, real execution (`bench_flat_python`) ⏳
### 4.5 Scenario C — batched mapping, 100 × 1000 (`bench_batched`) ⏳
### 4.6 Scenario D — two-level DAG-of-DAGs, 100 child runs × 1000 (`bench_two_level_*`) ⏳
### 4.7 Scenario E — one DagRun per account, 10k (`bench_run_per_account`) ⏳

## 5. Where the time goes (source ↔ measurement)

Details with file:line references: `docs/analysis/dynamic-task-mapping-expansion-path-3.3.2.md`.

| # | Mechanism | Evidence | Scales as |
|---|---|---|---|
| 1 | Mapped-TI creation: Python loop, `session.merge` per TI (SELECT + single-row INSERT), per-TI `MappedTaskUpstreamDep` query, all in one transaction — the code carries `# TODO: Make more efficient with bulk_insert_mappings` | 61 / 117 / 317 s blocked loop at 10k / 30k / 100k | O(N) per expansion, blocking |
| 2 | Every scheduler loop re-hydrates all TIs of each running DagRun as ORM objects; critical-section query sorts all SCHEDULED TIs with a window function | scheduler RSS 959 MB at 100k | O(N) per loop for the life of the run |
| 3 | Each mapped TI pulls the whole upstream XCom | ⏳ | O(N²) bytes |
| 4 | Partition key registration runs inside the task-success API request under the asset row lock, ≥5 queries per key, APDR lookups unindexed | 5.3 ms/key; 53 s for 10k; client timeout 5 s | O(keys) per request |
| 5 | Partition runs created ≤500 per tick, FIFO, single scheduler | ~80 runs/s | O(N/500) ticks |
| 6 | `asset_partition_dag_run` never pruned, PK-only index | code | unbounded |

## 6. How to model 100k firm accounts on Airflow 3.3 today ⏳

## 7. What to contribute upstream ⏳

## 8. Platform constraints: MWAA vs self-hosted ⏳

## 9. Reproduce

```bash
git clone https://github.com/caoterry/airflow-100k-partitions && cd airflow-100k-partitions
# see README.md: Postgres in Docker, uv venv, bench/ctl.sh init && start, then bench/harness.py …
```
