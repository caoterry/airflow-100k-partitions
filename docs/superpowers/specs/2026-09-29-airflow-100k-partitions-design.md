# Design: Can Airflow orchestrate 100k firm-account partitions?

Date: 2026-09-29. Status: experiments complete; findings and recommendation are in [`REPORT.md`](../../../REPORT.md).

## 1. Problem

A shared orchestration platform (balance-sheet team + revenue team) is being rebuilt cloud-native. The company
standard is Apache Airflow (baseline 3.3.0) or AWS MWAA. The revenue team needs *partitioned processing keyed by firm
account*, with 30k–100k firm accounts. Dagster's partition model fits this better on paper, but is not an option.

Question: **can Airflow be pushed to 100k partitions, what breaks first, what is the least-bad way to model it today,
and what would have to be contributed upstream?**

## 2. What "partition" has to mean here (assumptions, made explicit)

Nobody was available to answer clarifying questions during this run, so these are the working assumptions. Each
changes the design if wrong.

| # | Assumption | If wrong |
|---|---|---|
| A1 | "Partition" has two independent requirements: (a) **fan-out**: run N units of work per cycle; (b) **state**: per-account materialization status, re-run a chosen subset, see history per account. | If only (a) matters, batching alone solves it. If only (b), the fan-out benchmarks are irrelevant. |
| A2 | Per-account work is seconds to a few minutes and runs on an external engine (Spark/Snowflake/K8s job/HTTP). Airflow only orchestrates. | If per-account work is milliseconds inside Airflow, per-task overhead dominates and batching is mandatory. |
| A3 | Cycle is daily (or a few times a day), so the steady-state DB growth is ~100k task or run rows per cycle. | Hourly cycles multiply every DB-size finding by 24. |
| A4 | The platform may land on either self-hosted Airflow (K8s, CeleryExecutor/KubernetesExecutor) or MWAA (Celery only, config allowlist). MWAA is the binding constraint. | If self-hosted only, several knobs (e.g. `max_map_length`, scheduler count) are free. |
| A5 | Experiments may use Airflow 3.3.2 (latest release) and read main; the company will upgrade eventually. | If pinned to 3.3.0 forever, main-only partition features are out of reach. |

## 3. Approaches considered

### 3.1 Flat dynamic task mapping — one mapped task instance per account
`make_ids() -> process.expand(account=ids)`, N = 100k. Native, one line, full per-account visibility in the grid.
Cost: N `task_instance` rows per run, N executor round-trips, the mapped input XCom, and the scheduler must expand N
instances synchronously. `[core] max_map_length` (default 1024) must be raised.

### 3.2 Batched mapping — one mapped instance per *batch* of accounts
`make_batches() -> process_batch.expand(accounts=chunks)`, 100 × 1000. Airflow sees 100 units; per-account
parallelism moves into the task (thread pool / external engine). Cheapest by ~1000×; loses per-account state in Airflow.

### 3.3 Two-level DAG-of-DAGs
Parent triggers B child runs (one per batch); each child expands K accounts. Bounds per-run TI count, gives per-batch
run state, spreads scheduler work across runs. More moving parts; cross-run dependencies are awkward.

### 3.4 One DagRun per account (`run_id = acct_<id>__<as_of>`, `conf.account`)
The closest Airflow-native analogue of a Dagster partition *today*: the DagRun row is the partition record, individually
re-runnable, queryable. Cost: N `dag_run` rows + N TIs per cycle; bulk creation goes through the REST API or a trigger
task; the scheduler must handle N concurrent runs.

### 3.5 Native partitions (Airflow ≥3.2/3.3, AIP-76 implementation)
Airflow 3.2 added `dag_run.partition_key`, 3.3 added `partition_date`, `PartitionedAssetTimetable`,
`PartitionedAtRuntime`, `CronPartitionTimetable`, partition mappers (`IdentityMapper`, `FanOutMapper`, `ProductMapper`,
`AllowedKeyMapper`, `ChainMapper`, `FixedKeyMapper`) and a "clear partitions" API. This is structurally 3.4 with
first-class keys: one DagRun per partition key, driven by asset events carrying keys. Whether it is designed for 100k
distinct keys is exactly what the source analysis has to establish.

### Recommendation (to be confirmed by measurements)
Use **3.5 where the release supports it, with 3.4 as the fallback shape** for partition *state*, and use **3.2 (batching)
for fan-out** inside each partition run when per-account work is small. Do not use 3.1 at 100k in a single run.

## 4. Experiment design

Environment: MacBook, 8 cores, 8 GB RAM; Postgres 16 in Docker (`shared_buffers=512MB`, `synchronous_commit=off`);
Airflow 3.3.2 in a Python 3.12 venv; LocalExecutor `parallelism=12`; `max_map_length=200000`;
`max_active_tasks_per_dag=200000`; default pool 200000 slots; `max_tis_per_query=512`.

Two tiers, because an 8 GB laptop cannot execute 100k real task processes in reasonable time and that is not the
question anyway (execution scales horizontally with workers; the scheduler and metadata DB do not):

1. **Scheduler-only tier** — mapped `EmptyOperator` subclass. The scheduler marks these success without sending them to
   the executor, so we measure pure expansion + scheduling + DB cost for N ∈ {1k, 10k, 30k, 100k}.
2. **Full-execution tier** — mapped TaskFlow no-op at N ∈ {1k, 10k} (100k if time allows), batched 100 × 1000,
   two-level 100 runs × 1000, and one-run-per-account at 10k.

Metrics per run (`bench/harness.py`): time until all mapped TIs exist (expansion), time to run completion, TI
throughput, scheduler and task-process RSS, system free memory, Postgres table sizes, REST/UI endpoint latency against
the finished run, scheduler heartbeat gaps, slow SQL statements (>100 ms) and a `pg_stat_activity` sample trace.

Success criterion for "100k is feasible": a 100k-partition cycle completes with no scheduler heartbeat gap longer than
the default health threshold (30 s), API/UI stay interactive (<2 s), and metadata growth per cycle is bounded and
cleanable.

## 5. Out of scope
Real business logic, CeleryExecutor/KubernetesExecutor tuning, MWAA trial environment (cannot be created from here),
UI rendering in a browser (we probe the endpoints the UI calls instead).
