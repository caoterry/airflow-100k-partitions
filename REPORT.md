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

**Is the use case aligned with where Airflow is going?** Yes. The "expanded data awareness" line (AIP-73/74/75/76) is complete
and AIP-103 (task/asset state store, 3.3.0) gives a per-asset key/value store suited to per-account watermarks. What is in flight
as of 2026-09-29 (3.4.0 feature freeze is 2026-10-05, release planned 2026-10-26):

- **AIP-104 "Iterable Tasks and Task Spreading"** (PR #62922, milestone 3.4.0, updated 2026-09-29): `.iterate()` processes a
  collection inside one task instance with threads/async, `.spread(across=N)` splits items round-robin over exactly N task
  instances. Its motivation is literally that dynamic task mapping makes "each item a separate Task Instance". This is the
  first-class version of the batching pattern in §4.5.
- **AIP-88** streaming/lazy task expansion (WIP) and **AIP-100** scheduler starvation (awaiting review); starvation issues #45636
  and #49508 are on the 3.4.0 milestone.
- Partition follow-ups: #71070/#71074 duplicate pending runs for one key (bug + fix), **#71072 "log and audit when the
  partitioned-run per-tick cap is reached"** (someone else has hit the 500/tick cap), #68778 rollup re-run policy,
  #67941 `AssetPartitionSensor` / #70225 `AssetEventSensor`, #68517 `batch_asset_events`, #73119/#70444 deleting partitioned
  events, #64610 (merged for 3.4) `asset_event(asset_id, partition_key)` index and key filters.
- Airflow Summit 2026 (Nov 4–5) has a dedicated AIP-76 session by Wei Lee (Astronomer, the main implementer) on "authoring
  ergonomics, observability, and partition-aware workflow capabilities" — the same three gaps this report finds.

Nobody has proposed a per-partition materialization/"missing partitions" view, a key-list backfill, or any work at 10⁵ keys.
AIP-76's own design discussion assumed static partition sets would carry "an artificial limit similar to dynamic task
mapping's 1024". Dagster is not comfortable there either: its docs recommend ≤100,000 partitions per asset (raised from 25,000
only in 2025), with field reports of sensor timeouts at ~15k, a 90 s partitions tab at ~34k and a stalled backfill daemon at
~50k. "One partition per account" is a legitimate requirement; "100k scheduler-visible units per cycle" is at the edge of
every orchestrator.

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
| 100,000 (200 × 500, serialized) | 1,505 s (25 min) | 1,524 s | stopped: 3.7k done after 26 min, ~3–10 runs/s | 155 MB | none |

Two things the 10k run exposed:

1. **The write path is one HTTP request.** Registering 10k keys happened inside the task-success call to the Execution API and took
   **53 s (≈5.3 ms per key)**. The SDK client timeout (`[workers] execution_api_timeout`) defaults to **5 s**, so the client retried
   five times; all six requests queued on the asset row lock and completed within the same 12 ms. The task process was then killed
   ("Server indicated the task shouldn't be running anymore"), although the TI ended as `success` and no duplicate rows were written
   (10,000 distinct events / APDR / PAKL rows). Practical consequence: **at default settings a single task can emit at most ~900
   keys**, and concurrent emitters serialize on the asset lock, so they must be run one at a time.
2. **Run creation is capped at 500 per scheduler tick** (`MAX_PARTITION_DAG_RUNS_PER_LOOP`, hard-coded). Measured steady state:
   ~80 runs/s. 100k keys ⇒ ~21 minutes of run creation per cycle, single-threaded, before any work runs.

The 100k run (200 emitters × 500 keys, serialized with `max_active_tis_per_dagrun=1`) added three more:

3. **The per-key write cost grows with the size of `asset_partition_dag_run`.** The task-success request for 500 keys took 3.6 s
   when the table was empty and 7.0 s at ~60k rows (API-server access log, 124 requests, monotonic). The reason is the
   per-key lookup `WHERE partition_key=? AND target_dag_id=? ORDER BY id DESC LIMIT 1`, which is a sequential scan: `EXPLAIN`
   showed 1,843 shared buffers per key at 65k rows. Past ~5 s the SDK client started timing out and retrying again.
   Creating the obvious index online (`(target_dag_id, partition_key, id DESC)`) at 00:36:07 dropped the same request to
   3.4 s immediately (4 buffers per lookup), removed the retries, and roughly tripled the scheduler's run-completion rate
   because the drain loop hits the same table. Because the table is never pruned, without the index this cost keeps growing
   across days, not just within a cycle.
4. **Run creation keeps up; run completion does not.** All 100k `dag_run` rows existed 3 s after the last emitter finished
   (creation is bounded by emission, not by the 500/tick cap, once emitters are serialized). But the runs then finish at only
   3–10 per second: the scheduler examines `max_dagruns_per_loop_to_schedule` runs per loop (default 20, we used 200), each
   run needs at least two examinations (one to schedule the task, one to notice it finished), and the loop is pure Python.
   We stopped the run after 26 minutes with 3,698 runs finished and 96k queued/running; completing 100k trivial runs would
   have taken about three hours on one scheduler, with `max_active_runs` set to 100000. At the default `max_active_runs=16`
   it would take far longer.
5. **Scheduler memory stayed flat** (155 MB peak) — unlike dynamic task mapping, the partition path never holds 100k ORM
   objects at once.

### 4.3 Does partition storage grow as n²? (question raised by a colleague's analysis)

**Storage: no.** For identity mapping (one account = one key) every table grows exactly linearly, measured at three scales:

| table | 1k keys | 10k keys | 100k keys | exponent |
|---|---|---|---|---|
| `asset_event` | 1,000 | 10,000 | 100,000 | 1.00 |
| `asset_partition_dag_run` (APDR) | 1,000 | 10,000 | 100,000 | 1.00 |
| `partitioned_asset_key_log` (PAKL) | 1,000 | 10,000 | 100,000 | 1.00 |
| `dagrun_asset_event` | 1,000 | 10,000 | 100,000 | 1.00 |
| `dag_run` / `task_instance` (consumer) | 1,000 | 10,000 | 100,000 | 1.00 |

**Time: yes, in the write path.** Registering key *i* scans the APDR table, which already holds *i−1* rows for this cycle plus
every previous cycle's rows (never pruned). Total write time per cycle is therefore O(N²) and gets worse every day. The
responsible table and columns are **`asset_partition_dag_run (target_dag_id, partition_key)`**, unindexed in 3.2.0 through
3.3.2 and on main as of 2026-09-30 (`models/asset.py`; only the primary key exists). Measured: 3.6 s → 7.0 s per 500 keys as
the table grew 0 → 60k rows; back to 3.4 s the moment an index existed (§4.2).

Why storage stays linear: when a partition run is created, only the events joined through *its own* PAKL rows are attached
(`scheduler_job_runner.py`, `PartitionedAssetKeyLog.asset_partition_dag_run_id == apdr.id`), not all events of the asset.
Quadratic *storage* would need a cross-product mapper (`FanOutMapper`/`ProductMapper`, bounded by `partition_mapper_max_downstream_keys`=1000).

The retention problem compounds it: `asset_partition_dag_run` rows are **never deleted and are excluded from `airflow db clean`**
(`models/asset.py` docstring, `utils/db_cleanup.py`), so at 100k keys/day the unindexed hot table reaches 36.5 M rows in a year.
Measured row costs: `task_instance` ≈ 1 KB/row and `dag_run` ≈ 0.8 KB/row including indexes (243 MB and 81 MB after ~250k and
~100k rows), so one 100k-partition cycle per day is roughly 65 GB/year of metadata without retention.

The genuinely quadratic behaviour in Airflow at this scale is elsewhere — in dynamic task mapping, not partitions: **each of the N
mapped task instances downloads the entire N-element upstream XCom** at start (`xcom_arg.py:338`, "No mapped task group - pull from
unmapped instance"), i.e. O(N²) bytes through the API server (≈200 GB at 100k). ⏳ measured in Scenario B.

### 4.4 Scenario B — flat mapping, real execution (`bench_flat_python`) ⏳
### 4.5 Scenario C — batched mapping, 100 × 1000 (`bench_batched`) ⏳
### 4.6 Scenario D — two-level DAG-of-DAGs, 100 child runs × 1000 (`bench_two_level_*`) ⏳
### 4.7 Scenario E — one DagRun per account, 10k (`bench_run_per_account`) ⏳
### 4.8 Before/after with two upstream-style patches (Patch A expansion, Patch C partition write path + index) ⏳

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

## 6. How to model 100k firm accounts on Airflow 3.3 today

Separate the two requirements from §2 and give each the mechanism that scales:

**Partition state (per account): native partition keys, one asset, identity mapping — but at the granularity the scheduler
can carry.** Two workable shapes, depending on what the platform allows:

- *Shape 1 — account-level runs (100k partition runs per cycle).* Correct semantics out of the box (`dag_run.partition_key` =
  account, `clearPartitions`, `GET /dagRuns?partition_key_pattern=`). Requires all of: the APDR index (needs DB access →
  self-hosted only), emitters serialized and ≤ ~900 keys each (or `[workers] execution_api_timeout` raised), `max_active_runs`
  raised far above 16, several schedulers, and acceptance that completion throughput is ~10 runs/s/scheduler today (§4.2).
  Not viable on MWAA as shipped.
- *Shape 2 — shard-level runs (e.g. 1,000 shards × 100 accounts), account-level state.* Partition key = shard id (hash bucket,
  book, region); inside the shard run, fan out over its accounts with a bounded `expand()` (≤ 1024, the default
  `max_map_length`, and small enough that the whole-list XCom pull is harmless) or an in-task loop / AIP-104 `.iterate()`
  when 3.4 lands. Per-account outcome and watermark go to the 3.3 **asset state store** (`asset_state_store`, PK asset+key),
  which is the sanctioned per-key store and holds 100k keys under one asset. Re-running one account = re-running its shard
  with a `conf` filter, or a small "repair" DAG that takes an explicit account list. Everything in this shape works on
  3.3.1/MWAA today with no code changes.

**Fan-out (per-cycle compute): never 100k task instances in one DagRun.** Scenario A shows the expansion transaction alone
blocks the scheduler for 5 minutes; the whole-list XCom pull makes real execution O(N²) in bytes. Keep any single `expand()`
in the low thousands, prefer batches (§4.5) or child runs (§4.6), and push per-account parallelism into the compute engine.

**Shared-platform hygiene.** Give revenue its own pool and `priority_weight`, cap `max_active_runs` per DAG, and budget
metadata retention per cycle (`airflow db clean` on `dag_run`/`task_instance`; APDR needs an upstream fix to be cleanable at
all). On MWAA, consider a separate environment for the partitioned workload rather than pools — `multi_team` is blocked there.

Recommendation: **Shape 2 now** (it is also what AIP-104 is formalizing), with the state-store convention designed so that a
later move to Shape 1 is a key-format change, not a re-architecture — once the upstream fixes in §7 ship in a release MWAA offers.

## 7. What to contribute upstream

Ordered by impact ÷ effort. Items 1–3 are small, self-contained, and backed by measurements in this repo; the 3.4.0 feature
freeze is 2026-10-05, so realistically these land in 3.4.x/3.5 and reach MWAA a month after that.

| # | Change | Evidence | Size |
|---|---|---|---|
| 1 | **Index `asset_partition_dag_run (target_dag_id, partition_key, id)`** and a partial index for `created_dag_run_id IS NULL`; add APDR to `db clean` | §4.2/§4.3: 7.0 s → 3.4 s per 500 keys, retries gone, ~3× faster drain | migration, tiny |
| 2 | **Bulk-create mapped task instances** (`TaskMap.expand_mapped_task`): `session.add` + one flush (already merged as #69565 for main but absent from 3.3.x wheels), then hoist the per-TI `MappedTaskUpstreamDep` query | §4.1: 317 s blocked loop at 100k; Patch A results in §4.8 | backport + small PR |
| 3 | **Request-scoped memoisation in `_queue_partitioned_dags`** (serialized DAG, fingerprint, asset, mapper are recomputed per key) and batching APDR/PAKL inserts per request | 5–7 ms per key of which most is Python; Patch C results in §4.8 | small |
| 4 | **Make `MAX_PARTITION_DAG_RUNS_PER_LOOP` configurable** and bulk-insert the created `dag_run`/`task_instance` rows (the time-based path already uses `bulk_insert_mappings`) | §4.2: ~80 runs/s creation ceiling | small |
| 5 | **Index-aware XCom fetch for mapped operands**: explode a mapped input into per-index rows at push time (the `LazyXComSequence` path that exists for mapped upstreams), so each mapped TI pulls one item | O(N²) bytes (§4.4) | medium |
| 6 | **Incremental ("level-triggered") expansion**: expand K instances per scheduler loop with a cursor, commit between chunks, keep the `map_index=-1` sentinel until complete | bounds heartbeat gap independent of N | medium, needs dev-list discussion |
| 7 | **Key-list backfill / clear** (`partition_keys: list[str]` selector on backfill and `clearPartitions`) and `iter_partition_dagrun_infos` for `PartitionedAssetTimetable` | today: date ranges only, cron timetable only | medium |
| 8 | **Per-partition status endpoint/view** ("materialized / pending / missing / stale" per key, derived from `dag_run.partition_key` + `asset_state_store`) | the Dagster gap that matters most to users | medium–large, UI |
| 9 | Readiness-aware APDR selection (avoid FIFO head-of-line blocking); fast path for single-asset identity consumers (skip fingerprint/APDR/PAKL) | code comments in `scheduler_job_runner.py` | medium |
| 10 | Doc fixes: `assets.rst` "back-fill partition_key" claim vs code; `manager.py` reference to a non-existent mutex table; document `execution_api_timeout` vs `add_partitions` size | trivial |

The benchmark harness in this repo (`bench/`) reproduces every number above on a laptop and is the natural attachment for
the dev-list thread and the PRs.

## 8. Platform constraints: MWAA vs self-hosted

Everything above gets harder on MWAA, and every fix arrives later (sources: MWAA user guide pages as read 2026-09-29, in
`docs/research/`):

- **Versions/images.** MWAA offers 3.3.1 (since 2026-09-01), 3.2.1 and 3.0.6; there is no 3.3.0 and no 3.1.x, and only
  AWS-built images are allowed. Upstream fixes reach MWAA 3–4 weeks after an Apache release at best. Local patches (§7) are
  impossible there.
- **No database access.** The metadata DB is a single-tenant Aurora PostgreSQL (max 8 vCPU/64 GB) that customers cannot
  connect to, size or tune. The missing APDR index cannot be added by the customer; `airflow db clean` runs only through the
  CLI endpoint and does not cover APDR rows anyway.
- **Executor and sizing.** CeleryExecutor only (executor, broker, `parallelism` and `worker_autoscale` are reserved), 2–5
  schedulers, 1–25 workers (50 by quota), largest class mw1.2xlarge = 16 vCPU/48 GB per scheduler. A 5-minute blocked
  scheduler loop (§4.1) sits under an AWS-managed health check whose threshold is not documented.
- **Execution API inside the webserver.** On Airflow 3 MWAA runs the Task Execution API in the 2–5 webserver Fargate
  containers (autoscale at CPU >70%). Both heavy paths we measured — the whole-list XCom pull per mapped task instance and the
  per-key partition registration — land there, with no published sizing guidance.
- **REST throttle 10 TPS.** Triggering or re-running partitions through the API tops out at ~36k requests/hour.
- **Config overrides are allowed** for `core.max_map_length`, `scheduler.max_tis_per_query`, `scheduler.max_dagruns_*`,
  `core.max_active_runs_per_dag` etc.; only `core.multi_team` and `triggerer.queues_enabled` are blocked.
- **MWAA Serverless** (GA 2025-11) is out of scope: Airflow 3.0.6, 100 workflows/account, 20 concurrent runs per workflow,
  no dynamic task mapping.

Self-hosted Airflow on Kubernetes removes the first two bullets entirely and gives control over the third. If MWAA is mandatory,
the design has to stay inside what 3.3.1 does well out of the box: ≤ ~900 keys per emitting task, a few thousand partition
runs per cycle, batching inside runs.

## 9. Reproduce

```bash
git clone https://github.com/caoterry/airflow-100k-partitions && cd airflow-100k-partitions
# see README.md: Postgres in Docker, uv venv, bench/ctl.sh init && start, then bench/harness.py …
```
