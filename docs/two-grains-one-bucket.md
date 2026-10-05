# Account-grain revenue on Airflow 3.3: two grains, one bucket

October 1, 2026 · a design note for review

## Summary

**Trigger at account granularity, execute at batch granularity, and put a leaky bucket between the two.** Native Airflow cannot run one `DagRun` per account at 100,000 accounts a day, so the account stays the unit of tracking and the batch becomes the unit of execution.

**Why native fails.** Native account-grain partitions mean one `DagRun` per account, and the scheduler pays per run: 100,000 one-task runs cost 0.7 to 1.1 hours of scheduler time a day as a lower bound. As shipped, on one scheduler, a 100,000-account test was stopped after 26 minutes with only 3,698 runs marked finished: 80,900 of their tasks had succeeded, but the scheduler was busy creating and starting new runs. With three extra indexes and a second scheduler, all 100,000 finished in 31 minutes, about one scheduler-hour. MWAA's managed database does not let us add those indexes.

**How we do it.** Producers emit one keyed `AssetEvent` per account (`partition_key` = firm account) for lineage, and one unkeyed event, the bell, whose `extra` lists `{account: version}`. No Dag subscribes to the keyed asset per partition, so Airflow schedules nothing per account. The batcher Dag is scheduled on the bell. Airflow creates one `DagRun` per consumed queue row, which can carry many bells (21 at 10,001 accounts), holds the Dag at `max_active_runs=1` while a run is active so that later bells coalesce into one queue row, and hands the run its bells as `triggering_asset_events`; there is no cron and no watermark of ours. Inside the run, `claim` reads the `done` dict once, drops accounts whose version is already published, splits the rest into `min(K, ceil(n / b_min))` batches and writes one state-store key per batch; one mapped `spark` task per batch runs on a pool of K engine slots; `publish` marks its batch done and emits keyed lineage events; `finalize` folds the batch results into the `done` and `failed` dicts once per run. The journal is Airflow's own `asset_state_store`, written by one Dag, with a fixed number of calls per run whatever the account count. A version that lands while its account is in flight waits in its bell, and the next run computes it.

That is a capacity-bounded drain. Measured on 10,001 accounts: `claim` 5.6 s and the whole run 171 s, of which about 90 s is Airflow registering the 10,000 keyed lineage events at `publish` success, the one per-key cost that remains. Section 9 sets out two roads: the direction is right and each custom piece maps to a reasonable upstream change, and until those land v2 runs on MWAA 3.3.1 with no upstream dependency.

Everything here was measured on Airflow 3.3.2 with Postgres 16 and the LocalExecutor on a laptop. The engine was a stand-in: each Spark job in the prototype is a sleep of start-up time plus per-account time. Nothing has run on MWAA yet, and the prototype has known gaps, listed in section 8.

Balance sheet is a different case: it aggregates across accounts, so it uses no bucket and keeps an OR schedule with a version-aware gate (section 6).

## 1. Vocabulary

Every later section uses these names. The table column is where the thing lives in the Airflow metadata database (Postgres); the last three rows are ours, not Airflow's.

| Airflow term | What it is | Table | Role in this design |
| --- | --- | --- | --- |
| Dag | A workflow definition, parsed from a `.py` file by the dag-processor | `dag` | Producer Dags, the batcher Dag, the balance-sheet Dag |
| DagRun | One execution of a Dag | `dag_run` | One per batcher tick; never one per account |
| TaskInstance | One task in one DagRun; a mapped task has one per `map_index` | `task_instance` | Each batch is one mapped TaskInstance |
| Pool | A named set of slots a task must hold to run | `slot_pool` | Size = engine capacity K |
| Asset | A named dataset Dags produce and consume | `asset` | `positions`, `pnl` |
| AssetEvent | One immutable record that an asset was updated; optional `partition_key` and `extra` | `asset_event` | One per changed account, version in `extra` |
| asset\_dag\_run\_queue | One pending row per (asset, consumer Dag); deleted when the run is created | `asset_dag_run_queue` | Used only by unkeyed events (balance sheet) |
| PartitionedAssetTimetable | A schedule that creates one DagRun per partition key | (on `dag`) | Deliberately not used on account-keyed assets |
| AssetPartitionDagRun (APDR) | A pending per-key run for a partitioned consumer | `asset_partition_dag_run` | Never written in this design |
| PartitionedAssetKeyLog (PAKL) | Which upstream keys fed an APDR | `partitioned_asset_key_log` | Never written in this design |
| Asset state store | Key-value store per (asset, key), overwritable | `asset_state_store` | Holds the journal: the done and failed dicts and one key per batch |
| XCom | Small values passed between tasks | `xcom` | Batch lists from `claim` to the mapped tasks |
| scheduler | Loop that reads the DB, creates DagRuns, queues TaskInstances; the executor runs inside it | `job` | Unchanged |
| api-server | Serves the UI, the REST API and the Execution API that workers call | (writes for workers) | Records events, XCom and task states |
| triggerer | Runs deferrable waits and asset watchers | `job` | Waits on the engine in production |
| Journal | The done and failed dicts (account to version) plus one key per batch | `asset_state_store, keys on asset v2_positions` | The bucket |
| Batcher Dag | `claim`, `mapped spark on the pool, mapped publish, then finalize; scheduled on the bell` | `dag_run`, `task_instance` | The dispatcher |
| Watermark | `None of ours: Airflow attaches every bell since the previous run to the new run (triggering_asset_events)` | dagrun\_asset\_event | Read position |

## 2. How Airflow 3.3 turns data into runs

An asset update becomes a run by one of three paths, and the path depends on two things: whether the `AssetEvent` carries a `partition_key`, and whether any Dag subscribes to that asset per partition. All rows below are real rows, read from the local test database (Postgres) after the runs. They are not committed to the repository.

```
Producer task succeeds: the api-server writes one asset_event row
│
├── A  unkeyed event (balance sheet)
│      asset_dag_run_queue row, one per consumer Dag
│      ──▶ consumer DagRun when the condition holds
│
├── B  keyed event, consumer has PartitionedAssetTimetable (native)
│      APDR + PAKL row, one per partition key
│      ──▶ one DagRun per key: 100,000 a day
│
└── C  keyed event, no partitioned consumer (this design)
       nothing scheduled: no queue row, no APDR
       ──▶ batcher Dag reads the rows into the journal
```

| Path | The event | What the api-server and scheduler write | Result |
| --- | --- | --- | --- |
| A | unkeyed (balance sheet, and the batcher's bell) | one `asset_dag_run_queue` row per consumer Dag | a consumer DagRun when the condition holds |
| B | keyed, consumer has `PartitionedAssetTimetable` (native) | one APDR + PAKL row per partition key | one DagRun per key: 100,000 a day at account grain |
| C | keyed, no partitioned consumer (this design) | nothing: no queue row, no APDR | the batcher Dag, woken by the bell, reads the work list from the bell's extra |

<sub>asset event routing in Airflow 3.3 · three paths</sub>

<details><summary>Image version (may not load on networks that block GitHub images)</summary>

![One asset_event, three paths to a run](img/two-grains-paths.png)

Vector: [two-grains-paths.svg](img/two-grains-paths.svg)

</details>

Path B is native account grain and costs one run per account; Path C is this design, where Airflow schedules nothing per account.

**Path A: unkeyed event, normal consumer (balance sheet).** When the producer task succeeds, the api-server writes one `asset_event` row and one `asset_dag_run_queue` row per consumer Dag. Each scheduler loop checks the consumer's asset condition; when it holds, the scheduler creates a `DagRun` and deletes the queue rows it consumed.

| `asset_event`.id | asset | partition\_key | extra |
| --- | --- | --- | --- |
| 131514 | bs\_ref\_rates | (none) | `{"as_of": "2026-09-30", "version": 1, "path": "s3://bs/rates/2026-09-30/v1"}` |
| 131515 | bs\_root\_positions | (none) | `{"as_of": "2026-09-30", "version": 1, ...}` |

The queue still holds two rows from the AND experiment. The consumer `exp_e2_consumer_and` needs three assets; two of them have a pending row and the third never got a new event, so it has waited since 15:43 and will wait forever. That is the AND limitation, visible as table rows.

| `asset_dag_run_queue`.asset\_id | target\_dag\_id | created\_at |
| --- | --- | --- |
| 4 | exp\_e2\_consumer\_and | 2026-09-30 15:43:01 |
| 2 | exp\_e2\_consumer\_and | 2026-09-30 15:43:31 |

**Path B: keyed event, partitioned consumer (native account grain).** A keyed event writes no queue row. Instead, a Dag with `PartitionedAssetTimetable` gets one `asset_partition_dag_run` (APDR) row per key, a `partitioned_asset_key_log` (PAKL) row linking it to the event, and then one `DagRun` per key. Below, one account, `ACCT00000937`, is followed through five tables. It comes from the 100k run with the extra indexes and two schedulers (section 3, wall 2b); the as-shipped run's rows are no longer in the database.

| Table | id | Key columns |
| --- | --- | --- |
| `asset_event` | 194031 | asset `bench_accounts`, partition\_key `ACCT00000937`, source\_dag\_id `bench_partition_producer`, 18:36:57 |
| `asset_partition_dag_run` | 153582 | target\_dag\_id `bench_partition_consumer`, partition\_key `ACCT00000937`, created\_dag\_run\_id 164133 |
| `partitioned_asset_key_log` | 153583 | asset\_event\_id 194031 → asset\_partition\_dag\_run\_id 153582, source key = target key |
| `dag_run` | 164133 | run\_type `asset_triggered`, partition\_key `ACCT00000937`, state `success`, created 18:37:01 |
| `task_instance` |  | task `process_partition`, map\_index -1, pool `default_pool`, success |

Multiply by 100,000 accounts a day: 100,000 rows in each of these tables and 100,000 `DagRun`s for the scheduler to drive. Section 3 shows what that costs.

**Path C: keyed event, no partitioned consumer (this design).** The api-server writes the `asset_event` rows and nothing else for the keyed events: no queue row, because the event is keyed; no APDR row and no asset row lock, because nobody subscribes per partition. Airflow schedules nothing per account, and a Dag scheduled on the keyed asset without `PartitionedAssetTimetable` gets no run either. What wakes the batcher is a second, unkeyed event the same producer task emits, the bell, which takes Path A: one queue row, one batcher run, and the bell's `extra` carries the accounts and versions (section 4). Each keyed event is still one `asset_event` insert inside the producer's task-success request, so that request grows with the number of keys.

| `asset_event`.id | asset | partition\_key | extra | source\_dag\_id |
| --- | --- | --- | --- | --- |
| 547610 | rev5\_positions | ACC100037 | `{"version": 1}` | rev5\_producer |
| 547609 | rev5\_positions | ACC100040 | `{"version": 1}` | rev5\_producer |
| 547608 | rev5\_positions | ACC100039 | `{"version": 1}` | rev5\_producer |

The rule that keeps Path C in place: no Dag may put `PartitionedAssetTimetable` on any asset this design emits keyed events on, including the pnl output that `publish` writes. In the first 100k batcher test, a partitioned consumer on that output asset (`rev5_pnl_consumer`, left on as a lineage check) created one run per published account, 54,000 runs, on the same scheduler. The input events stayed on Path C. Airflow does not block such a Dag by default. A cluster policy (`dag_policy`) could reject it at parse time, but that was not tested.

## 3. Why one run per account fails at 100,000

The scheduler pays per `DagRun`, and Path B gives it one per account. Three walls were measured. The per-run cost behind the second is the one no index removes.

**The scheduler loop.** Each loop of `SchedulerJobRunner._do_scheduling` creates new `DagRun`s (for partitioned consumers, at most `MAX_PARTITION_DAG_RUNS_PER_LOOP` = 500 APDR rows per loop), examines up to `max_dagruns_per_loop_to_schedule` running `DagRun`s (default 20; the tests used 200), and moves ready `TaskInstance`s to queued inside a critical section. The executor runs in the same process and the same loop. Each look at a run schedules every task whose upstream is done, so the number of looks follows the longest chain of tasks:

```math
T_{scheduler} \approx N_{runs} \times (L_{longest\ chain} + 1) \times c_{look}, \qquad c_{look} \approx 12\text{–}20\ \text{ms}
```

A one-task run, which needs two looks, measured 25 to 40 ms of scheduler time in total (26 to 42 runs finished per second). For 100,000 one-task runs that is 0.7 to 1.1 hours a day as a lower bound, before any business work.

| Wall | Shape | Measured | Limit it hits |
| --- | --- | --- | --- |
| 1 | 100,000 mapped `TaskInstance`s in one `DagRun` | Expansion blocks the scheduler loop 315 s (10k: 60 s, 30k: 114 s); an upstream backport cuts 100k to 173 s | `scheduler_health_check_threshold` = 30 s; a health probe would restart the scheduler mid-transaction |
| 2 | One `DagRun` per account (Path B), as shipped | Stopped after 26 min: all 100,000 runs created and 80,900 of their tasks succeeded, but only 3,698 runs marked finished | Per-run scheduler cost on one scheduler. It looks at never-examined runs first, so while new runs kept arriving it did not get back to mark finished runs done |
| 2b | Same, with three hand-made indexes and two schedulers | All 100,000 finished in 31 min, about 110 runs/s once run creation stopped | The index needs DB access MWAA does not give; the run also used a second scheduler |
| 3 | Registering 500 keys in one task-success call | 3.6 s on an empty APDR table, 7.0 s at 64,000 rows; about 5 ms per key under an asset row lock | `[workers] execution_api_timeout` = 5 s; clients retry. One emitter on an empty table reaches 5 s at about 700 to 950 keys; at 64,000 rows even 500 keys do not fit |

**Why wall 3 grows with the table.** For every emitted key, `AssetManager._get_or_create_apdr` looks up the latest APDR row for that key, and every scheduler loop scans for pending rows. On Airflow 3.3 neither query has an index. Measured on a fresh Postgres 16 database with 100,000 APDR rows, using the same ORM expressions as Airflow main:

```sql
-- per emitted key, inside the task-success request
SELECT * FROM asset_partition_dag_run
 WHERE partition_key = 'acct_50000' AND target_dag_id = 'consumer'
 ORDER BY id DESC LIMIT 1;

-- every scheduler loop
SELECT asset_partition_dag_run.* FROM asset_partition_dag_run
  JOIN dag ON dag.dag_id = asset_partition_dag_run.target_dag_id
 WHERE asset_partition_dag_run.created_dag_run_id IS NULL
   AND dag.is_paused IS false AND dag.is_draining IS false AND dag.is_stale IS false
 ORDER BY asset_partition_dag_run.created_at, asset_partition_dag_run.id
 LIMIT 500 FOR UPDATE OF asset_partition_dag_run SKIP LOCKED;
```

| Query | Without index | With the two indexes from apache/airflow#73983 |
| --- | --- | --- |
| Per-key lookup | Seq Scan + Sort, 99,999 rows filtered, 935 buffers, 6.96 ms | Index Scan Backward, 4 buffers, 0.008 ms |
| Scheduler pending scan | Seq Scan, 99,500 rows filtered, 935 buffers, 3.72 ms | Index Scan on `created_dag_run_id IS NULL`, 11 buffers, 0.43 ms |
| Deleting 100 `dag_run` rows (FK cascade) | 360 ms in the FK trigger | 0.42 ms |

```
APDR rows   seconds per 500-key request   (█ = 0.25 s; 5 s timeout = 20 █)

        0      ██████████████ 3.6 s
    4,000      ███████████████ 3.7 s
    8,000      ███████████████ 3.8 s
   12,000      ████████████████ 3.9 s
   16,000      █████████████████ 4.2 s
   20,000      █████████████████ 4.2 s
   24,000      ██████████████████ 4.6 s
   28,000      ███████████████████ 4.8 s
   32,000      ████████████████████ 5.0 s
   36,000      █████████████████████ 5.3 s  over 5 s
   40,000      ███████████████████████ 5.7 s  over 5 s
   44,000      ████████████████████████ 6.0 s  over 5 s
   48,000      ████████████████████████ 6.1 s  over 5 s
   52,000      ███████████████████████████ 6.7 s  over 5 s
   56,000      ██████████████████████████ 6.4 s  over 5 s
   60,000      ███████████████████████████ 6.7 s  over 5 s
   64,000      ████████████████████████████ 7.0 s  over 5 s
   64,500      ████████████████████████████ 7.1 s  over 5 s
   65,000      ████████████████████████████ 7.0 s  over 5 s
        ---- index created online: (target_dag_id, partition_key, id) ----
   65,500      ████████████████ 3.9 s
   66,000      ██████████████ 3.5 s
   66,500      ██████████████ 3.4 s
   67,000      ██████████████ 3.5 s
   67,500      ███████████████ 3.7 s
   68,000      ██████████████ 3.4 s
   68,500      ██████████████ 3.4 s
   69,000      ██████████████ 3.5 s
```

<sub>request_durations.csv from the 100k run on Airflow 3.3.2 (bench/results/part_100k_e200) · 27 samples</sub>

<details><summary>Image version (may not load on networks that block GitHub images)</summary>

![500-key registration time against rows in asset_partition_dag_run](img/two-grains-write-path.png)

Vector: [two-grains-write-path.svg](img/two-grains-write-path.svg)

</details>

In the live 100,000-key run the request passed the 5-second client timeout at about 36,000 rows, and clients began to time out and retry. The index, created online mid-run, brought the next request back to 3.9 s.

The index fixes wall 3, but MWAA's managed database does not let us add it. It does not remove wall 2: every run still costs scheduler work, and finishing 100,000 runs in 31 minutes also took a second scheduler. Wall 2b changed both at once, so the index's own share of that gain was not measured.

## 4. The design: two grains, one bucket

Three pieces, all inside Airflow: a producer that tags accounts on its events and rings one bell, a batcher Dag scheduled on the bell, and a journal that lives in Airflow's own `asset_state_store` table and is written by that one Dag. Airflow keeps every scheduling and execution decision; the journal records which version of each account was last published and which batch holds what. Code: `v2/dags/v2_dags.py` and `v2/dags/journal_store.py`. The rows below are from `v2/recordings/run4.json`, a recording of row changes in the local metadata database (Postgres) during a 10-account landing followed by a one-account version 2, polled every 0.5 s plus the time of its own queries, in practice every 2 to 3.5 s, so times are approximate.

This replaces three things from the previous draft of this doc: the journal table outside Airflow with its `SKIP LOCKED` claim (a schema in the old prototype; a database of its own, `v2_journal`, in variant A, `v2/variant_a/`, which already rang the bell), and, from the old prototype, the one-minute cron with its watermark and the `release` task. An intermediate variant B (`v2/variant_b/`) kept one state-store key per account and paid two state-store calls per account in `claim` and two more in `publish`; its numbers are in section 8 and are the reason for the `done` dict.

**Producer Dag.** The `land` task declares two outlets. On `v2_positions` it emits one keyed event per account with the version in `extra` (Path C of section 2: no queue row, no APDR row, nothing scheduled). On `v2_positions_landed`, the bell, it emits one unkeyed event whose `extra` carries the work list. It writes no table of ours (`v2_dags.py:39-44`).

```python
@task(outlets=[POSITIONS, BELL], do_xcom_push=False)
def land(params: dict | None = None, *, outlet_events=None) -> None:
    accounts, version = list(params["accounts"]), int(params["version"])
    outlet_events[POSITIONS].extra = {"version": version}
    outlet_events[POSITIONS].add_partitions(accounts)                       # lineage: one keyed asset_event per account
    outlet_events[BELL].extra = {"accounts": {a: version for a in accounts}}  # the bell carries the work list
```

Real rows from `dag_run` 38 (`v2_land`, 10 accounts, version 1): ten keyed `asset_event` rows 20120 to 20129 on `v2_positions` (`ACC11` to `ACC20`, `extra = {"version": 1}`) and one bell:

| `asset_event`.id | asset | partition\_key | extra | source\_dag\_id |
| --- | --- | --- | --- | --- |
| 20129 | v2\_positions | ACC13 | `{"version": 1}` | v2\_land |
| 20130 | v2\_positions\_landed | (none) | `{"accounts": {"ACC11": 1, "ACC12": 1, "ACC13": 1, ..., "ACC20": 1}}` | v2\_land |

Producer cost, measured on this producer with no partitioned consumer: 500 keyed events plus the bell per task took 1.1 s on average and 1.4 s at most (Path B's 500 keys took 3.6 to 7.0 s because of the APDR work under the asset lock). Every landing, 3 accounts or 100,000, goes through this one Dag; there is no separate lane for bulk loads and adjustments.

**The bell.** The bell is unkeyed, so it takes Path A: the api-server writes one `asset_dag_run_queue` row for `(v2_positions_landed, v2_batcher)` in the same transaction as the event (`assets/manager.py:352, 543, 828-835`). The primary key of that table is `(asset_id, target_dag_id)` (`models/asset.py:751-759`) and the insert is `ON CONFLICT DO NOTHING` (`manager.py:834`), so any number of bells while the batcher is busy leave one row. The scheduler holds the batcher while it has a run at `max_active_runs` (`models/dag.py:769-788`). When it creates the run it attaches every event on the bell asset whose timestamp lies after the previous asset-triggered run's `run_after` and at or before the queue row's `created_at` (`jobs/scheduler_job_runner.py:2593, 2600-2607, 2638-2639, 2664`), sets the new run's `run_after` to that `created_at` (`:2651`), and deletes the row (`:2672-2682`). Tasks see those events as `triggering_asset_events`; every TaskInstance of the run loads them when it starts (`api_fastapi/execution_api/routes/task_instances.py:256-264`), 21 bells at 10k, about 200 at 100k, not measured. In the recording: the queue row appeared at t = 5.4 s, in the same poll as `land`'s success; at t = 8.7 s the ten keyed events and the bell were visible, `dag_run` 39 (`v2_batcher`, `asset_triggered`) was created, `dagrun_asset_event (39, 20130)` was written, and the queue row was gone.

Why this matters: the old prototype kept its own read position over `asset_event`, a timestamp watermark with a 5 s overlap that could skip a late commit. Here Airflow decides which events a run owns, records it in `dagrun_asset_event`, and never hands the same event to two runs. The read position is gone. The exact window has one consequence of its own, set out in section 5.

**`claim`.** One task, two reads and at most K writes of the state store whatever the account count (`journal_store.py:24-44`):

```python
done   = store.get("done") or {}            # {account: version last published}
failed = store.get("failed") or {}          # {account: version whose batch failed}
todo = [(a, v) for a, v in sorted(wanted.items())
        if v > int(done.get(a, -1)) or (a in failed and v >= int(failed[a]))]
n_batches = max(1, min(k, math.ceil(len(todo) / b_min)))
```

`wanted` is the merge of the run's bells, newest version per account (`journal_store.py:14-21`). The split is `min(K, ceil(n / b_min))` batches of equal size with no per-batch cap: 10 accounts with K = 3 and b\_min = 4 gave 4, 4 and 2; 100,000 accounts give K batches. Each batch gets one key `batch/<run_id>#<i>` holding its accounts and versions, and the batch list goes to the mapped tasks through XCom (three lists of 3,334, 3,334 and 3,333 accounts at 10,001; ten of 10,000 at 100k, not measured). Real rows written by run 39's `claim` (`written_by` is the recorder's name for the store's `last_updated_by_task_id` column, `models/asset_state_store.py:50-54`):

| `asset_state_store` key (asset `v2_positions`) | value | written\_by |
| --- | --- | --- |
| `batch/asset_triggered__2026-10-05T21:04:04.579870+00:00_DRFmkgvl#0` | `{"state": "claimed", "versions": {"ACC11": 1, "ACC12": 1, "ACC13": 1, "ACC14": 1}, "error": null}` | claim |
| `batch/asset_triggered__2026-10-05T21:04:04.579870+00:00_DRFmkgvl#1` | `{"state": "claimed", "versions": {"ACC15": 1, "ACC16": 1, "ACC17": 1, "ACC18": 1}, "error": null}` | claim |
| `batch/asset_triggered__2026-10-05T21:04:04.579870+00:00_DRFmkgvl#2` | `{"state": "claimed", "versions": {"ACC19": 1, "ACC20": 1}, "error": null}` | claim |

`done` did not exist yet, so everything in the bell was due. At 10,001 accounts (21 bells on one run) this task took 5.6 s; variant B's per-account version took 138 s for 20,002 calls.

Why this matters: `claim` has no clock, no wait threshold and no view of running batches. It cannot decide when to run (the bell did) or how much capacity is free (the pool does). What is left is packing: which accounts, into how many batches.

**`spark`.** One mapped TaskInstance per batch, `pool="v2_spark"`, `retries=1` (`v2_dags.py:55-69`). The pool has K slots (`v2/ctl.sh:25`) and `claim` makes at most K batches, so every batch of a run gets a slot; were the pool smaller than K, the extra batches would wait in `scheduled` until a slot freed. The task sets its batch key to `running`, then calls the engine (here a sleep of 6 s plus 0.5 s per account). If it fails on what Airflow says is the last attempt (`ti.try_number > task.retries`, `v2_dags.py:66`), it marks only its own batch key `failed` with the error and re-raises. In the recording the three `spark` instances were `queued` in `v2_spark` at t = 8.7 s and `running` at t = 12.1 s, and each batch key changed to `{"state": "running", ...}` with `written_by = spark`.

**`publish`.** One mapped TaskInstance per batch (`v2_dags.py:71-77`). It sets its batch key to `done` and declares the accounts as partitions of the `v2_pnl` outlet with the batch id in `extra`. Airflow registers one keyed `asset_event` per account when the task succeeds (`models/taskinstance.py:1637-1663`). Real rows for batch #0:

| `asset_event`.id | asset | partition\_key | extra | source\_dag\_id |
| --- | --- | --- | --- | --- |
| 20140 | v2\_pnl | ACC13 | `{"batch_id": "asset_triggered__...DRFmkgvl#0", "versions": {"ACC11": 1, "ACC12": 1, "ACC13": 1, "ACC14": 1}}` | v2\_batcher |
| 20138 | v2\_pnl | ACC11 | same | v2\_batcher |

Correction made after this recording: `extra` now carries only `batch_id`. An outlet's `extra` is copied onto every keyed event it emits, and the recording's version carried the whole batch's `versions` dict in each one: the 10k test's 10,000 `v2_pnl` events held 504 MB of `extra` between them, and at 100,000 accounts in ten batches it would add about 15 GB a day to `asset_event`. The per-account version lives in the batch key, which the `batch_id` points to. The rows above and in sections 5 and 7 show the recorded, pre-fix `extra`.

This is the one per-key cost that stays. At 10,001 accounts the `publish` task bodies took 1.5 s per batch, but the api-server spent about 90 s registering the 10,000 keyed events at task success, 5 to 8 ms per key, serialized per asset. That is about half of the 171 s run. A linear extrapolation to 100,000 keys is about 15 min per run; it has not been measured, and bulk registration upstream is one of the pieces in section 9.

**`finalize`.** One task per run, `trigger_rule="all_done"`, downstream of every `publish` (`v2_dags.py:79-85`). It reads each batch key and folds the result into the two dicts: a batch in state `done` sets `done[account] = max(done, version)` and removes the account from `failed`; a batch in state `failed` sets `failed[account] = version` (`journal_store.py:67-84`). Then two writes. Real rows at the end of run 39:

| key | value | written\_by |
| --- | --- | --- |
| `done` | `{"ACC11": 1, "ACC12": 1, "ACC13": 1, "ACC14": 1, "ACC15": 1, "ACC16": 1, "ACC17": 1, "ACC18": 1, "ACC19": 1, "ACC20": 1}` | finalize |
| `failed` | `{}` | finalize |

Why one writer: the state store has `get`, `set`, `delete` and `clear` and nothing else (`api_fastapi/execution_api/routes/asset_state_store.py:109-241`); its write is an upsert where the last writer wins (`state/metastore.py:66-90`). A read-modify-write of `done` is safe only if nobody else writes it, and `max_active_runs=1` plus one `finalize` per run is what guarantees that. `max_active_runs=1` is therefore load-bearing, not a tuning choice; section 5 gives its cost.

**Config constants** (`v2_dags.py:29-33, 48-55`):

| Constant | Prototype | Production | Role |
| --- | --- | --- | --- |
| `K` | 3 | about 10 | engine slots = size of pool `v2_spark`; cap on batches per run |
| `B_MIN` | 4 | about 100 | smallest batch worth its own job; only shapes the split |
| `ENGINE_STARTUP_S`, `PER_ACCOUNT_S` | 6 s, 0.5 s | the engine's own | the stand-in engine's sleep |
| `retries` | claim 2, spark 1, 5 s delay | to agree | Airflow's retries; no retry logic of ours |
| `max_active_runs` | 1 | 1 | one writer of the state store |

**State-store calls per run** (`journal_store.py:7`), independent of the account count:

| Task | Calls | K = 3 | K = 10 |
| --- | --- | --- | --- |
| claim | 2 gets + up to K sets | 5 | 12 |
| spark | 2 per batch | 6 | 20 |
| publish | 2 per batch | 6 | 20 |
| finalize | K gets + 2 gets + 2 sets | 7 | 14 |
| total |  | 24 | 66 |

Variant B made 20,002 calls in `claim` alone at 10k. The price of the fixed count is one large value: the `done` dict is 130 KB at 10k, and a 100,000-entry dict round-trips from a task in 0.32 s (set) and 0.24 s (get).

**What Airflow does and what we own:**

| Airflow does | We own |
| --- | --- |
| Wakes the batcher on the bell, one queue row however many bells, held at `max_active_runs` (`models/dag.py:769-788`) | The convention that the bell's `extra` carries `{account: version}` |
| Hands the run its events (`triggering_asset_events`) and records them in `dagrun_asset_event` | `candidates`: newest version per account across the run's bells |
| Runs, retries and logs every batch as a TaskInstance; caps them with pool `v2_spark` | `claim`: drop what `done` already covers, split into at most K batches, write up to K batch keys |
| Registers the keyed lineage events on `v2_pnl` | `publish` and `finalize`: batch state, the `done` and `failed` dicts |
| Stores the journal (`asset_state_store`, with who wrote each key) | The rule that the journal holds no timing, capacity or retry logic |
| Waits on Glue or Spark in the triggerer once the operator is deferrable; the pool then needs `include_deferred` (`models/pool.py:289-294`) | `v2_requeue`: an operator rings the bell for failed accounts |

**Why each choice removes a "scheduler inside the scheduler" objection:**

| What a second scheduler would do | Old prototype | Variant C |
| --- | --- | --- |
| Decide when to dispatch | one-minute cron; `claim` waited for b\_min accounts or t\_max seconds | the bell; Airflow creates the run and holds it at `max_active_runs`; `claim` has no clock |
| Count free capacity | `claim` counted in-flight batches in the journal, a second copy of the pool | `claim` makes at most K batches; the pool is the only cap (spark 0 to 2 `queued` at t = 8.7 s, `running` at 12.1 s) |
| Keep a read position over Airflow's tables | timestamp watermark, 5 s overlap, could skip a late commit | none; Airflow attaches the run's events (`scheduler_job_runner.py:2664`) and `claim` never reads `asset_event` |
| Own a table and its locks | `journal.accounts` with `FOR UPDATE SKIP LOCKED` | `asset_state_store`, one writer; no lock needed and none exists |
| Retry and release | `release` returned every batch of the run; failed accounts re-claimed every tick | Airflow retries; `fail` after the last attempt marks one batch; `v2_requeue` is an operator action |
| Run a dispatcher loop | a continuous dispatcher was planned | none; each run is a plain DagRun of claim, mapped spark, mapped publish, finalize |

## 5. Overlapping reruns, row by row

Four rules: an account is never computed twice at once, batches never wait for each other inside a run, versions that pile up while an account waits collapse into one recompute, and one failed batch affects only its own accounts. The recording exercised the first three with ten accounts and a version 2 of `ACC13` that landed while batch #0 was running. Times are seconds since the recorder started; it polled every 0.5 s plus the time of its own queries, in practice every 2 to 3.5 s.

| Step | t (s) | What changed | Rows |
| --- | --- | --- | --- |
| 1 | 5.4 to 8.7 | Producer run 38 landed `ACC11` to `ACC20` v1 | `asset_event` 20120 to 20129 keyed, 20130 the bell; one `asset_dag_run_queue` row (added at t = 5.4 s, consumed at t = 8.7 s); `asset_state_store` empty |
| 2 | 8.7 | Scheduler consumed the queue row and created batcher run 39 | `dag_run` 39 `asset_triggered`; `dagrun_asset_event (39, 20130)`; queue row removed |
| 3 | 8.7 | `claim` read `done` (absent), split 10 into 4, 4, 2 | three `batch/...DRFmkgvl#0..2` keys, `state = claimed`, written by claim; `claim` success |
| 4 | 12.1 | Three `spark` instances running in pool `v2_spark` | batch keys `state = running`, written by spark |
| 5 | 14.9 to 17.4 | Producer run 40 landed `ACC13` v2 while batch #0 was running | `asset_event` 20131 (`ACC13`, `{"version": 2}`), 20132 the bell `{"accounts": {"ACC13": 2}}`; a new queue row and `dag_run` 40 at t = 14.9 s, the two events at t = 17.4 s; no state-store change; the batcher is at `max_active_runs`, so no run |
| 6 | 19.6 to 25.1 | `spark` done; `publish` marked batches done and emitted lineage; `finalize` wrote the dicts | batch keys `state = done`, written by publish; `asset_event` 20133 to 20142 on `v2_pnl` (20140 is `ACC13`, `"versions": {..., "ACC13": 1, ...}`); `done = {"ACC11": 1, ..., "ACC13": 1, ..., "ACC20": 1}`, `failed = {}`, written by finalize |
| 7 | 27.6 to 29.8 | Run 39 ended; the scheduler consumed the waiting queue row and created run 41; `claim` saw `ACC13` v2 > `done` 1 | `dag_run` 39 success, `dag_run` 41 running; `dagrun_asset_event (41, 20132)`; one key `batch/...TuaEyx3U#0` with `"versions": {"ACC13": 2}` |
| 8 | 36.2 to 43.0 | Run 41 computed, published and finalized | `asset_event` 20143 (`v2_pnl`, `ACC13`, `"versions": {"ACC13": 2}`); `done` changed to `{..., "ACC13": 2, ...}`, written by finalize; run 41 success |

Eleven account computations, two batcher DagRuns (plus producer runs 38 and 40), four batch keys plus `done` and `failed`, 24 `asset_event` rows, and no table outside Airflow. (Batch key `...TuaEyx3U#0` first appears as `running` because the poll missed its `claimed` state.)

**Never twice at once.** Two things make it hold. Inside a run, `claim` sorts the accounts and cuts the list into disjoint chunks (`journal_store.py:29, 37`). Across runs, `max_active_runs=1` means run 41's `claim` did not start until run 39's `finalize` had ended (t = 25.1 s against 27.6 s), so nothing is in flight when a run claims. The `done` dict then keeps a repeated bell from recomputing a version that is already published: `ACC13` v1 in a later bell would be skipped, v2 was not. There is no `inflight` status to check and no lock to take, because the serialization comes from Airflow's run count, not from the journal. The exception is an operator clearing a `spark` task in the UI: it reruns that batch from XCom outside `claim`. The cleared run goes back to `QUEUED` (`models/taskinstance.py:360, 473`) and waits at the `max_active_runs` gate (`scheduler_job_runner.py:2795`), so it cannot overlap another run, but it can compute an older version after a newer one has been published and emit its lineage events again; `done` cannot move backwards because `finalize` takes the max (`journal_store.py:78`). Not tested.

**No head-of-line blocking inside a run.** The three `spark` instances were all running at t = 12.1 s and each `publish` closed its own batch; a slow batch holds its own slot and nothing else. Across runs there is a cost: a bell that rings during a run waits for the whole run, including `finalize`. In the recording, bell 20132 had committed by t = 14.9 s (its queue row; the event itself was read at the next poll, 17.4 s), the slots were idle from t = 19.6 s, and run 41 was created at t = 27.6 s. `ACC13` v2 went from bell to `v2_pnl` event in 23 to 26 s with a 6 s engine start-up, 10 to 13 s of it waiting for run 39 to close. With K = 10 and uneven batches the tail of each run leaves slots idle for about one batch duration. Raising `max_active_runs` is not the fix, because it would put two `finalize` writers on the `done` dict; the per-key exclusion upstream in section 9 is.

**Coalescing.** In two places. Airflow's queue row is one per (asset, consumer), so bells that ring while the batcher is busy do not create extra runs. `candidates` then takes the newest version per account across the run's bells (`journal_store.py:14-21`): had `ACC13` v2 and v3 both rung before run 41, it would have computed v3 once. A version that lands while its account is in flight is not folded into the running batch and is not lost: it sits in its bell until the next run, which is what step 7 shows. Nothing marks it done early, because `finalize` writes the versions a batch actually carried (`journal_store.py:77-78`), not what arrived since.

**Which bells a run gets.** Airflow attaches the bell events with a timestamp after the previous asset-triggered run's `run_after` and at or before the queue row's `created_at`, and the new run's `run_after` is that `created_at` (`scheduler_job_runner.py:2593, 2600-2607, 2638-2639, 2651, 2664`). Two consequences, traced in source and not tested. First, a second or later bell that rings while a queue row already exists (bells B and C during one run) has a later timestamp than the row, so it is attached to the run after the next bell, not to the next run. Second, a bell can be skipped in one traced way. Producer P1 flushes its bell (`manager.py:352`), and in the few milliseconds before P1 inserts its own queue row (`:835`) producer P2 inserts that row first; P1's insert then waits on P2's uncommitted row (`INSERT ... ON CONFLICT DO NOTHING` on the primary key at `models/asset.py:759` waits for the conflicting transaction) and does nothing once P2 commits. If the scheduler creates the run from P2's row before P1's request commits (`task_instances.py:536`), P1's bell has a timestamp at or before that row's `created_at` and is never attached; P1's remaining keyed events, registered in set order (`models/taskinstance.py:1598-1602, 1665`), can keep its commit up to 1.1 to 1.4 s away. The entry window is milliseconds; the exposure after entry is up to one request. Outside that gap a later bell is held off by the row and takes the first consequence instead. There is no asset lock on this path (`_lock_asset_model` is called only for the partitioned path at `manager.py:749`). A second way exists on more than one api-server: the event timestamp and the queue row's `created_at` are set from each process's clock (`models/asset.py:824, 753`), so skew between replicas can put a bell at or before the previous run's `run_after`. Neither has been reproduced. In a steady stream the first consequence costs one extra run of delay. At the end of a stream, or after the batcher was paused (a paused Dag gets no queue row, `manager.py:354`, while its events stay; measured: the first bell after unpausing attached all 21 bells), nothing starts a run until another bell rings. An empty bell would do it: `v2_requeue` with no accounts rings one, and the run it starts attaches everything since the previous run. A time-based fallback such as `AssetOrTimeSchedule` would not, because a time-triggered run carries no `triggering_asset_events`. The empty-bell use of `v2_requeue` has not been tested, and the time-based fallback has not been built.

**Failed state and requeue.** When `spark` fails on its last attempt, `fail` marks only that batch key `failed` with the first 300 characters of the error (`v2_dags.py:64-68`, `journal_store.py:62-64`); the other batches keep running, and `finalize` runs anyway (`trigger_rule="all_done"`) and moves that batch's accounts into `failed` at their claimed version (`journal_store.py:79-81`). A failed account leaves that state when a newer version arrives (`v > done`) or when an operator runs `v2_requeue`, which rings the bell with the failed versions (`v2_dags.py:88-96`); `claim` accepts a version equal to the failed one (`journal_store.py:30`). Retries stay with Airflow (`retries=1` on `spark`). None of this has run: the recording has no failed task, the poison account `ACC42` was never landed, and `v2_requeue` was never triggered.

**Still unbuilt or untested.** The failure path and `v2_requeue`; the empty-bell sweep for a paused batcher or the last bell of a day; a test for a cleared `spark` task; this scenario as an automated test (today it is a recording); and the 100k run on this variant.

## 6. Balance sheet: OR schedule plus a version-aware gate

Balance sheet aggregates across accounts, so it uses no bucket: any input arrival wakes the Dag, and a gate task decides whether the business date can be computed and with which versions. A late version 2 of one input reruns the whole date with that version and the latest of everything else, which the native AND schedule cannot do (section 2, Path A).

```python
@dag(schedule=(rates | fx | bs_positions | bs_cashflows), max_active_runs=1)
def bs_pnl():
    @task.short_circuit(inlets=[rates, fx, bs_positions, bs_cashflows])
    def gate(inlet_events=None, triggering_asset_events=None):
        dates = sorted({e.extra["as_of"] for evs in triggering_asset_events.values() for e in evs})
        plans = []
        for as_of in dates:                              # one run can carry several business dates
            latest = {name: max((e for e in inlet_events[a] if e.extra["as_of"] == as_of),
                                key=lambda e: e.extra["version"], default=None)
                      for name, a in INPUTS.items()}
            if all(latest[n] for n in ("rates", "fx")) and any(latest[n] for n in ("positions", "cashflows")):
                plans.append({"as_of": as_of, "inputs": latest})
        return plans                                     # empty list: calc is skipped
    calc.expand(plan=gate())                             # one calc per ready date, emits bs_pnl_out
```

Balance sheet does not listen to the keyed positions asset that revenue uses. On Airflow 3.3 a keyed event writes no `asset_dag_run_queue` row, so it never starts a Dag without `PartitionedAssetTimetable` (in experiment E4 such a consumer got 0 runs). Balance sheet listens to separate unkeyed assets with one event per load, as the experiment did with `bs_root_positions` and `bs_root_cashflows`. This also keeps 100,000 keyed events a day out of the gate's `inlet_events` read.

The rule here is "all reference inputs (rates, fx) and at least one root input (positions, cashflows)". In E6 every arrival got its own `DagRun`, because the driver waited for each run to finish before landing the next input. Airflow's source shows that arrivals landing while a run is active under `max_active_runs=1` wait and are released together as one later run, which can carry several business dates; E6 did not exercise that case. The gate above therefore checks each date separately. That per-date gate is in the repository but was written after the E6 run and has not run yet; the E6 run used a gate that took only the latest date. The real runs from the experiment, one per arrival:

| Time | Event that woke the Dag | `gate` decision | `calc` | Output event `extra` |
| --- | --- | --- | --- | --- |
| 17:25:05 | rates v1 (2026-09-30) | fx missing | skipped |  |
| 17:25:19 | positions v1 | fx missing | skipped |  |
| 17:25:33 | fx v1 | references complete, one root | success | rates 1, fx 1, positions 1 |
| 17:25:49 | cashflows v1 | second root arrived | success | rates 1, fx 1, positions 1, cashflows 1 |
| 17:26:03 | **rates v2** | late version of a reference | **success** | **rates 2**, fx 1, positions 1, cashflows 1 |
| 17:26:17 | positions v1 (2026-10-01) | next date: references missing | skipped |  |
| 17:26:31 | rates v1 (2026-10-01) | fx missing | skipped |  |
| 17:26:45 | fx v1 (2026-10-01) | references complete, one root | success | rates 1, fx 1, positions 1 |

Three details the gate has to get right, all learned in the experiments:

- **Take the highest `extra.version`, not the last event.** `inlet_events[asset][-1]` is the last event registered, and when producers run concurrently that need not be the newest version. In experiment E2, five concurrent producer runs registered v3 to v7 in the order v5, v7, v6, v4, v3, so \[-1\] returned v3.
- **Page the event read.** `inlet_events` is unbounded; 10,000 events took 7.6 to 9.1 s, past the 5 s Execution API timeout. Read with `.after(watermark).limit(...)`.
- **Skipped runs are the cost.** Eight runs, four of which computed; each is a `dag_run` row and a few `task_instance` rows. Fine at a few inputs per day, not at 100,000 accounts, which is why revenue uses the bucket.

## 7. Observability: where is account X?

The two questions, "where is account X" and "why is it not done", are answered from Airflow's own tables; nothing is queried outside Airflow. The Airflow UI stays for operators and shows batches as TaskInstances, not accounts.

**Where to look.** Every row below is a real row from the recording after run 41.

| Question | Where | Row |
| --- | --- | --- |
| Which version of X is published? | the `done` dict, key `done` on asset `v2_positions` | `done["ACC13"] = 2`, `written_by = finalize`, changed at t = 43.0 s in the recording (the `updated_at` column, `models/asset_state_store.py:48`, was not captured by the recorder) |
| Is X in a failed batch, at which version? | the `failed` dict | `failed = {}` |
| Which batch computed X, and when? | `v2_pnl` events with `partition_key = X`, `extra.batch_id` | 20140: batch `...DRFmkgvl#0`, versions `{"ACC13": 1, ...}`; 20143: batch `...TuaEyx3U#0`, versions `{"ACC13": 2}` |
| Which TaskInstance ran that batch? | the batch id is `run_id#map_index` | `dag_run` 41, task `spark`, map\_index 0, pool `v2_spark`; its log is in the UI |
| Is a batch still running, with which accounts? | `batch/<run_id>#<i>` key: `state`, `versions`, `error` | `batch/...DRFmkgvl#0` went claimed (claim, t = 8.7 s), running (spark, t = 12.1 s), done (publish, t = 22.7 s) |
| Who wrote a journal key? | the store's `last_updated_by_*` columns: kind, dag\_id, run\_id, task\_id, map\_index (`models/asset_state_store.py:50-54`) | `done` by `finalize`; batch keys by `claim`, `spark`, `publish` |
| Has X landed but not been claimed yet? | bell events on `v2_positions_landed` not yet in `dagrun_asset_event`, plus the queue row | at t = 17.4 s: bell 20132 with `{"ACC13": 2}` and one `asset_dag_run_queue` row, run 41 not yet created |
| Which input version did X's result use? | `v2_positions` events for X, and the batch's `versions` | 20129 v1, 20131 v2 |
| Re-queue X | `v2_requeue` with `accounts = [X]` rings the bell at `failed[X]` | not exercised |

The batch id ties the journal to Airflow's run history in both directions: from an account's `v2_pnl` event to the `dag_run` and the mapped `spark` instance, and from a running TaskInstance to the accounts it holds.

**What the Airflow UI shows.** For run 39: `claim`, `spark` map\_index 0 to 2 in pool `v2_spark`, `publish` 0 to 2, `finalize`, each with its log, duration and tries; the asset view shows the bell events each run consumed and the keyed events each `publish` emitted. It does not show accounts: finding `ACC13` means reading the `done` dict or the keyed events.

**The gaps.**

- No reason. An account waits for one of two things, the current run to end or a slot to free, and nothing writes which. A view that joins the pending bells, the batch keys and the run states would answer it; not built.
- One big value. `done` is one key: 130 KB at 10k accounts, and about 1.3 MB at 100k if it grows linearly (not measured; a 100,000-entry dict round-trips in 0.24 s from a task). The public endpoint, `/api/v2/assets/{asset_id}/state-store` (`api_fastapi/core_api/routes/public/asset_state_store.py:43`), returns one entry by key or a paged list of entries (key, value, `updated_at`, writer); there is no filter by value, so "where is X" over REST means fetching the whole dict.
- No history in the journal. `done` keeps the latest version only. History is the keyed `asset_event` rows, which on 3.3 cannot be filtered by `partition_key` over REST (the filter arrives in 3.4), so a per-account history is a scan or a database view.
- MWAA throttles the REST API at a default 10 requests per second, which AWS Support can raise; a dashboard should read the dict once and cache it.
- Retention. Each account adds two keyed events a day (`v2_positions`, `v2_pnl`) plus the bells, about 200,000 rows a day at 100k; a cleanup Dag is needed and not built.
- The `error` field holds the first 300 characters of the engine's exception, nothing structured.

What is better than the previous draft: there is no second database to grant access to, every write names its task, and the writer columns plus `dagrun_asset_event` make each state change attributable to one TaskInstance.

## 8. Measured, designed, open

Eighteen of the claims in this doc were measured or simulated; four are known gaps in the prototype; five are designed but not built; six are open questions, two of which only the data side can answer. Evidence links go to the public test repository. The rows quoted in sections 2, 4, 5 and 7 come from the local test database and from the recording `v2/recordings/run4.json`.

| Claim | Evidence | Status |
| --- | --- | --- |
| 100,000 mapped `TaskInstance`s in one `DagRun` block the scheduler loop 315 s | [REPORT §4.1](../REPORT.md) | **Measured** |
| One `DagRun` per account as shipped, one scheduler: stopped after 26 min with 3,698 of 100,000 runs marked finished while runs were still being created | [REPORT §4.2](../REPORT.md) | **Measured** |
| Same with three hand-made indexes and two schedulers: 31 min | [REPORT §4.2a](../REPORT.md) | **Measured** |
| Registering 500 keys with one emitter under a partitioned consumer (Path B): 3.6 s on an empty APDR table, 7.0 s at 64,000 rows | [REPORT §4.2](../REPORT.md) | **Measured** |
| 100,000 accounts as 100 batches of 1,000 in one `DagRun`: 38 s | [REPORT §4.5](../REPORT.md) | **Measured** |
| Path C with up to 12 concurrent 500-key producers (old prototype, same registration path as v2): 195 of 200 requests over the 5 s timeout, worst 38.5 s, 3 producer processes killed | api-server log of the old 100k batcher test | **Measured** |
| Engine start-up sets latency: 60 s start-up misses a 2-min burst p95 (best 7.2 min); about 10 s meets it | [dynamic-batching §2](dynamic-batching.md) | **Simulated** |
| Balance sheet: OR schedule plus gate recomputes the date on a late version 2 | Section 6, experiment E6 | **Measured** |
| v2 producer: 500 keyed events plus one bell per task in 1.1 s on average, 1.4 s at most, with no partitioned consumer and so no APDR cost; whether producers overlapped was not recorded | Measured 2026-10-05 on the laptop; `v2/dags/v2_dags.py:39-44` | **Measured** |
| Variant B, one state-store key per account, 10,001 accounts: `claim` 138 s for 20,002 state-store calls at about 6.9 ms each; `publish` 65 s per batch of 3,334; whole run 370 s | Measured 2026-10-05; `v2/variant_b/journal_store.py:24-53` | **Measured** |
| Variant C, same 10,001 accounts, 21 bells attached to one run: `claim` 5.6 s, `spark` 15 s with engine time made tiny, `publish` 1.5 s per batch, `finalize` about 16 s, whole run 171 s | Measured 2026-10-05; `v2/dags/journal_store.py` | **Measured** |
| Of those 171 s about 90 s is the api-server registering the 10,000 keyed `v2_pnl` events at `publish` success, 5 to 8 ms per key, serialized per asset. A linear extrapolation to 100,000 keys is about 15 min per run, not measured | Measured 2026-10-05; `models/taskinstance.py:1637-1663` | **Measured** |
| State-store calls per run do not depend on the account count: 2 + up to K in `claim`, 2 per batch in `spark` and in `publish`, K + 4 in `finalize`; 24 at K = 3, 66 at K = 10 | `journal_store.py:7`; run 39 in `run4.json` shows the 11 writes (3 claim, 3 spark, 3 publish, 2 finalize) | **Measured** |
| `done` is 130 KB at 10k accounts; a 100,000-entry dict round-trips from a task in 0.32 s (set) and 0.24 s (get) | Measured 2026-10-05; `v2/dags/v2_sizetest.py` | **Measured** |
| `ACC13` v2 landing while its batch was in flight: run 39 computed v1, run 41 computed v2, `done` ends `{"ACC13": 2}` | `run4.json`: events 20129 and 20131, runs 39 and 41, `done` written by `finalize` | **Measured** |
| Inside a run no batch waits for another: `spark` 0 to 2 all running at t = 12.1 s in pool `v2_spark`; each `publish` closed its own batch | `run4.json` | **Measured** |
| Across runs a bell waits for the current run to end: bell 20132 committed by t = 14.9 s (its queue row; the event read at 17.4 s), slots idle from t = 19.6 s, run 41 created at t = 27.6 s; `ACC13` v2 took 23 to 26 s from bell to `v2_pnl` event with a 6 s engine start-up | `run4.json`; `max_active_runs=1` at `v2_dags.py:48` | **Measured** |
| Paused batcher: the queue row is dropped but the events stay; the first bell after unpausing created a run that attached all 21 bells | Measured 2026-10-05; `assets/manager.py:354`; `scheduler_job_runner.py:2593-2643` | **Measured** |
| A second or later bell that rings while a queue row already exists has a timestamp after that row's `created_at`, so it is attached to the run after the next bell, not the next run. A bell can be skipped in one traced way: in the milliseconds between a producer flushing its bell and inserting its own queue row, another producer inserts that row first; the first producer's insert waits on the uncommitted row and does nothing, and if the scheduler creates the run from that row before the first producer's request commits, up to 1.1 to 1.4 s later when its keyed events follow the bell, the bell is never attached. On more than one api-server, clock skew between replicas can also put a bell at or before the previous run's `run_after` | `scheduler_job_runner.py:2593, 2600-2607, 2638-2639, 2651, 2664`; `manager.py:352, 834-835`; `models/asset.py:753, 759, 824`; `execution_api/routes/task_instances.py:536, 670`; `models/taskinstance.py:1598-1602, 1665`; no asset lock on this path (`manager.py:749` is the partitioned path only); traced, not reproduced | **Known gap** |
| After a pause, or after the last bell of a day, nothing starts a run until a bell rings; a time-triggered run would carry no `triggering_asset_events`. An empty bell (`v2_requeue` with no accounts) would start a run that attaches everything since the previous run | `scheduler_job_runner.py:2664` attaches events only on the asset-triggered path; `v2_requeue` exists (`v2_dags.py:88-96`) but its empty-bell use is not tested; the time-based fallback is not built | **Known gap** |
| The failure path exists in code and has not run: `spark` failing on its last attempt marks only its batch, `finalize` folds it into `failed`, `v2_requeue` rings the bell at the failed version and `claim` accepts it | `v2_dags.py:64-68, 88-96`; `journal_store.py:30, 62-64, 79-81`; no failed task in any recording, `ACC42` never landed | **Known gap** |
| Clearing a `spark` task in the UI reruns its batch from XCom outside `claim`; the cleared run goes back to `QUEUED` and waits at the `max_active_runs` gate, so it cannot overlap another run, but it can compute an older version after a newer one has been published and emit its lineage events again; `done` cannot move backwards because `finalize` takes the max | `journal_store.py:78`; `v2_dags.py:83-85`; `models/taskinstance.py:360, 473`; `scheduler_job_runner.py:2795`; not tested | **Known gap** |
| Every `TaskInstance` of a run loads the run's consumed events when it starts; at 100k that is about 200 bells of 500 accounts each, on each of `claim`, K `spark`, K `publish` and `finalize` starts | `api_fastapi/execution_api/routes/task_instances.py:256-264`; not measured | **Open question** |
| Account status view and reason (waiting for the run to end, waiting for a slot, failed) | Section 7 | **Designed, not built** |
| `dag_policy` rejecting `PartitionedAssetTimetable` on `v2_positions` and `v2_pnl` | `policies.py:53`; whether MWAA loads `airflow_local_settings.py` from `plugins.zip` is unverified | **Designed, not built** |
| Deferrable engine operator with pool `include_deferred` | `models/pool.py:289-294` | **Designed, not built** |
| `asset_event` retention: about 200,000 rows a day at 100k (two keyed events per account plus the bells); a maintenance Dag around `airflow db clean` | `utils/db_cleanup.py`; MWAA exposes `db clean` only through the CLI endpoint | **Designed, not built** |
| The section 5 scenario as an automated test | Today it is a recording, `v2/recordings/run4.json` | **Designed, not built** |
| 100k end to end on variant C | Not run | **Open question** |
| The asset state store and the Execution API on MWAA 3.3.1: default backend, calls landing in the webserver containers, keys per request against `execution_api_timeout` | Not run on MWAA; `config_templates/config.yml:2045-2053` | **Open question** |
| Positions feed: full snapshot or deltas, and are zero-position accounts included | Data owner | **Open question** |
| Where the list of accounts expected on a business date comes from | Data owner, account master | **Open question** |
| Kafka or SQS ringing the bell through an asset watcher | Not tried; section 9 | **Open question** |

## 9. Two roads: native now, native later

The measure for "native" here is not how few lines we wrote but whether each custom piece is a reasonable, mergeable upstream change. That gives two roads, taken together. Road 1: the direction is right, and each custom piece of v2 has a counterpart that can land upstream later. Road 2: until those counterparts ship, v2 runs on MWAA 3.3.1 as it is, with no upstream dependency, and each custom piece is deleted when its counterpart ships.

### Road 1: the direction, and the upstream pieces

Airflow 3.3 already ships half of this design on the partitioned path: a per-key pending bucket that coalesces on write (`AssetPartitionDagRun` plus `PartitionedAssetKeyLog`, `assets/manager.py:749-789, 692-709`), a `FOR UPDATE SKIP LOCKED` drain (`jobs/scheduler_job_runner.py:2167-2188`), fan-in mappers (`FixedKeyMapper`, `RollupMapper`), and a record of exactly which events each run consumed. What is missing is the drain policy. Each row below is one custom piece of v2, what Airflow does today, and the change that would let the piece go.

| Our custom piece | What Airflow does today | The change | Reasonable? | Mergeable? | Route |
| --- | --- | --- | --- | --- | --- |
| Back-pressure: `max_active_runs=1` on the bell holds work until the run ends | Native on the unkeyed path (`models/dag.py:769-788`). On the partitioned path an APDR becomes a run even when the Dag is at `max_active_runs`; main's own docstring calls this the one genuine divergence (`scheduler_job_runner.py:2354-2356` on main) | Apply the unkeyed path's exclusion to the pending-APDR scan (`scheduler_job_runner.py:2173-2188`). Small: a filter in one query plus tests | Yes | Likely; main's comment names it | PR directly, once the firm's open-source approval is in |
| `claim` dedup against `done`, plus run serialization: an account never runs twice at once | No per-partition-key concurrency anywhere: a new APDR opens for a key whose run is still running (`manager.py:759, 791-806`); `max_active_runs` counts runs per Dag (`scheduler_job_runner.py:2722-2800`) | At run creation, hold back keys that sit in a `QUEUED` or `RUNNING` run of the same Dag and carry them into the next APDR; needs a PAKL index on (target\_dag\_id, source\_partition\_key). Medium. A draft exists: `docs/proposals/max-active-runs-per-partition-key.md` | Yes | Medium: no prior proposal upstream, so it needs a thread first; the nearest items, #71070 and #71074, remove duplicate pending runs only | GitHub issue plus a `[DISCUSS]` thread on dev@, linked to the September 2026 batching thread |
| `claim` split: `min(K, ceil(n / b_min))` batches | One pending APDR becomes exactly one run; no cap, no split (`scheduler_job_runner.py:2297-2357`); `MAX_PARTITION_DAG_RUNS_PER_LOOP = 500` is a per-tick cap, not a policy (`:170-175`) | A key cap or a split by free run slots, declared on the mapper or timetable. Medium. Inside a run, AIP-104 `.spread(across=N)` deals items over N task instances; its vote restarted 2026-10-01 and it is unlikely in 3.4 | Yes, as a declared policy | Pushback expected: AIP-76 fixes one `partition_key` per run, and clear, backfill and the UI assume it | dev@ with #56750 and #55956 as context; frame it as declared grouping, not batching |
| `b_min`, and "flush when no run overlaps" | Wait policies exist only for rollup mappers against a window that lists every expected key (`scheduler_job_runner.py:2040-2058`, `partition_mappers/base.py:191-197`); only `WaitForAll` and `MinimumCount` deserialize (`serialization/decoders.py:284-299`) | A core count-or-age `WaitPolicy` for non-rollup fan-in with no expected-key window. Small to medium | Yes; one PMC member wrote in the September 2026 thread that any batching "should be configurable: specifying the number of messages, the batching time windows etc." | Plausible | PR after the thread above |
| Lineage registration per key: 5 to 8 ms per key at `publish` success, about 90 s per 10k | One `register_asset_change` and one insert per keyed payload (`models/taskinstance.py:1637-1663`) | Bulk registration per request, one insert for all keys. Medium | Yes, performance only | Likely | Issue plus PR; already listed in REPORT §7 |
| The bell with `{account: version}` in `extra`, and `candidates` taking the newest version per account | Keyed events never queue a non-partitioned consumer (`manager.py:530-532`); every TaskInstance loads the run's consumed events on start (`execution_api/routes/task_instances.py:256-264`) | None proposed. Letting keyed events queue a plain consumer would remove the bell but load 100k event rows into every task start; the bell is stock Path A with a payload | As is | Not needed | None |
| The `done` and `failed` dicts and the batch keys in `asset_state_store` | No per-key store with compare-and-set or a query by value (`execution_api/routes/asset_state_store.py:109-241`, `state/metastore.py:66-90`); per-key status would need a PAKL to APDR to `dag_run` join no API offers; the `partition_key` filter on asset events arrives in 3.4 | A per-source-key status endpoint and UI view built from PAKL, APDR and `dag_run`. Medium (API plus UI); only useful once the rows above exist | Yes | After the model exists | Last |

The rule: each piece is deleted when its counterpart ships. The first two rows are the ones that matter. With APDRs held at `max_active_runs` and in-flight keys held back, the batcher becomes a partitioned consumer with a fan-in mapper and `max_active_runs = K`, the `done` dict becomes "the latest successful run that consumed the key's event", and `claim` shrinks to the split. The split and the count-or-age policy follow. The bell and the dicts go last, or stay as a compaction if the native shape stays expensive per key.

What the native shape on 3.3.1 would cost today, and why road 1 is not a plan for production: it registers every key under the asset row lock (`manager.py:749`; 3.6 to 7.0 s per 500 keys, section 3), fires about once per scheduler loop in a trickle, grows APDR without an index, has none of the first four rows, and its pending bucket can lose events when a producer writes while the scheduler drains (a plain `SELECT` with no lock on the APDR row at `manager.py:750-759`; traced, not reproduced). That last one is a correctness bug in shipped code; we would report it with a reproduction, as a bug, not as one of our pieces.

Timelines, as estimates: 3.4 freezes 2026-10-12 and releases about 2026-11-02 with nothing on partitions in scope; 3.5 is not announced and is about February to April 2027 on the recent cadence; MWAA has trailed each x.y.0 by six to eight weeks (3.3.1 of 2026-08-12 reached MWAA on 2026-09-01). A per-key limit merged before the 3.5 freeze is on MWAA about Q2 2027 at best; grouped runs through an AIP are 3.6 or later. Nothing here is something to plan production on.

**What we will not propose, and why.**

- The journal itself. An application table, in whatever store, drained by a task is the "second scheduler" objection in person; it has no place upstream.
- A DagRun that stands for a changing set of keys. AIP-76 fixed one `partition_key` per run, and clear, backfill and the UI build on it. We ask for declared grouping and per-key exclusion instead.
- A scheduler config knob. The maintainers call the per-tick cap "not a behavioural knob operators need to tune" (`scheduler_job_runner.py:170-175`); the accepted shape is a policy on the Dag, mapper or timetable.
- The word "batching". The September 2026 dev@ thread agreed that implicit batching should stop being the default and that any batching must be explicit and configurable (#68517, #55956, #56750). We say grouping and exclusion.
- Keyed events waking plain consumers by default. Every maintainer who replied in that thread wants one event, one run as the default.

### Road 2: what runs on MWAA 3.3.1 today

Everything v2 uses is a stock 3.3 feature. The body of each code path below is the same in the 3.3.1 tag (what MWAA ships) and in 3.3.2 (what was measured), checked with `git diff 3.3.1 3.3.2` on the fork; line numbers are 3.3.2's, and in 3.3.1 the scheduler path sits 30 lines earlier (2544-2652), the outlet registration 37 lines earlier (1600-1626) and the task-start load 5 lines later (261-269).

| Feature | Where (3.3.2 lines; same code in 3.3.1) | Used for |
| --- | --- | --- |
| Asset-triggered schedule on an unkeyed asset: one queue row per consumer, held at `max_active_runs` | `assets/manager.py:828-835`, `models/asset.py:751-759`, `models/dag.py:769-788` | the bell, coalescing, back-pressure |
| Events attached to the run and exposed as `triggering_asset_events` | `jobs/scheduler_job_runner.py:2574-2682` | the work list; no watermark |
| `asset_state_store` with `get` and `set` from tasks, and writer columns | `models/asset_state_store.py:42-54`, `api_fastapi/execution_api/routes/asset_state_store.py:109-241` | the journal |
| Dynamic task mapping, pools, `trigger_rule="all_done"`, task retries | core | batches, K slots, `finalize`, retries |
| Keyed outlet events with `add_partitions` | `models/taskinstance.py:1637-1663` | lineage on `v2_positions` and `v2_pnl` |
| Deferrable operators holding a pool slot with `include_deferred` | `models/pool.py:289-294` | engine waits, once wired |

No Airflow patch, no plugin and no table outside Airflow. MWAA accepts most configuration overrides; the research sweep lists `core.multi_team` and `triggerer.queues_enabled` as blocked and rates that list as only partly verified. Code arrives only through `requirements.txt`, `plugins.zip` or a startup script; on Airflow 3 the Execution API runs inside the webserver containers, so the per-key registration and the state-store calls land there. None of this has been run on MWAA.

Before the first MWAA run:

| Item | Why | Status |
| --- | --- | --- |
| Pool `v2_spark` with K slots, `include_deferred` once the engine operator is deferrable | the only capacity control | config |
| `[workers] execution_api_timeout` override and an agreed number of keys per producer task | default 5 s (`config_templates/config.yml:2045-2053`); 500 keys took 1.1 s on v2; concurrent producers were not re-measured on v2 | to measure |
| `dag_policy` rejecting `PartitionedAssetTimetable` on `v2_positions` and `v2_pnl` | the rule from section 2; whether MWAA loads `airflow_local_settings.py` from `plugins.zip` is unverified | not built |
| `asset_event` retention Dag | about 200,000 rows a day at 100k; `airflow db clean` only through the CLI endpoint on MWAA | not built |
| The failure path and `v2_requeue`, the empty-bell sweep, the 100k run | section 5 and section 8 | untested |
| The account status view over `done`, `failed`, the batch keys and the bells | section 7 | not built |

### The same drain on Kafka

The batcher is a Kafka consumer in shape, with the halves reversed: Airflow gives the orchestration and we add the per-key state; Kafka gives the per-key state and we would add the orchestration.

| Kafka | This design |
| --- | --- |
| Keyed messages on a topic | keyed `asset_event` rows, plus the bell with the work list |
| Consumer offset | the run's `triggering_asset_events`: Airflow decides which events a run owns and records it in `dagrun_asset_event`; no cursor of ours |
| KTable, latest value per key | `candidates` (newest version per account across the run's bells) and the `done` dict |
| Poll and batch | `claim`: at most K batches per run |
| Parallelism bounded by partitions, `pause()` and `resume()` | pool `v2_spark`, `max_active_runs=1` |
| Commit after processing | `finalize`, once per run |

|  | Airflow | Kafka |
| --- | --- | --- |
| Native | triggering, retries, deferrable waits, run history, UI, lineage, the state store; same platform as balance sheet; offered as MWAA | offsets, consumer groups, millisecond delivery, per-key state in Kafka Streams |
| We write | `candidates`, `claim`, `publish`, `fail`, `finalize`: `journal_store.py`, under 90 lines with comments | launching and retrying engine jobs, recording what each batch ran, a UI, lineage |
| Latency floor | one run cycle: in the recording `ACC13` v2 went from bell to `v2_pnl` event in 23 to 26 s with a 6 s engine start-up, 10 to 13 s of it waiting for the previous run | milliseconds of delivery |

The millisecond advantage pays only with a fast engine; in the simulator the engine start-up set the burst p95 whatever the message path (section 8). An `AssetWatcher` with `MessageQueueTrigger` could ring the bell from Kafka or SQS in the triggerer; a watcher's event carries no `partition_key`, so the accounts would travel in `extra["payload"]`, as they travel in the bell today. Not tried, and not on MWAA.

## 10. Decisions and questions

Four decisions move this forward, and five questions need someone other than the author to answer them.

The ask: agree that v2 is the design, take both roads at once, and run v2 on a firm MWAA 3.3.1 environment.

**Decisions**

- [ ] Revenue at account grain uses v2: keyed events for lineage, one bell per landing, a batcher Dag scheduled on the bell, and the journal as `done`, `failed` and batch keys in `asset_state_store`. No `DagRun` per account, no table outside Airflow, no cron, no watermark.
- [ ] Road 1: start the firm's open-source approval; then post the per-key concurrency draft as an issue and a `[DISCUSS]` thread, and the `max_active_runs` hold for APDRs as a PR; file the APDR write race as a bug with a reproduction. The evaluation lead handles the approval; the upstream work can proceed meanwhile.
- [ ] Road 2: next build on v2, in this order: exercise the failure path and `v2_requeue`; the empty-bell sweep for a paused batcher or the last bell of a day; the account status view; the `dag_policy`; the deferrable engine operator with `include_deferred`; the retention Dag. The section 5 recording becomes an automated test.
- [ ] Run v2 on a firm MWAA 3.3.1 environment wired to the revenue demo, measure the keys-per-request budget against `execution_api_timeout` there, then the 100k run.

**Questions for the data side**

- Is the positions feed a full snapshot per business date or only changes, and are zero-position accounts included? If an account's positions go to zero and it disappears, its PnL stays at yesterday's value unless something compares against the previous day.
- Where does the list of accounts expected on a business date come from? Accounts open and close daily, and "no event" only means "no data" against that list.
- Is the 2-minute p95 meant for the intraday trickle only or the start-of-day burst too, and what are the engine's start-up time and concurrency? Those two numbers set K and `b_min`, and with them the per-run tail in section 5.

**Questions for the streaming side**

- Would you keep the drain inside Airflow as here, or ring the bell from Kafka or SQS through an asset watcher and keep the journal in the state store?
- In the existing per-key system, what happened to an update that arrived while its key was being processed, and how was a failed batch replayed? v2's answers are "it waits for the next run" and "an operator rings the bell"; we want to know whether yours differed.
