# Study notes — Airflow at high partition cardinality

*Compiled 2026-09-30 from two days of experiments in this repo. Written for re-reading a week later: each key point is one
mechanism and one number; the quiz at the end is the same one used in the session.*

## The one analogy to keep

Accounts are **passengers** arriving at a stop. A calculation job is a **bus**: it needs `S` seconds to warm up (engine
start-up) and `p` seconds per passenger. The parking lot holds `K` buses (concurrent jobs — on AWS, bounded by subnet IPs).
Airflow is the **dispatcher** deciding when a bus leaves and with how many passengers. Every result below is a statement
about buses, warm-up time or parking spaces.

## Glossary (the words that kept coming up)

| term | meaning | where it bit us |
|---|---|---|
| DagRun | one execution of a DAG (`dag_run` row) | native partitions = one DagRun per partition key |
| TI (task instance) | one execution of one task inside a DagRun (`task_instance` row) | `expand()` over N items = N TIs |
| map_index | the TI's index within a mapped task; −1 = the unexpanded placeholder | |
| XCom | the small JSON a task returns, stored in `xcom`, read by downstream tasks | the mapped input list is one XCom that every mapped TI downloads |
| Asset / asset event | a dataset object; "asset X was updated" (`asset_event` row, may carry `extra` and `partition_key`) | passengers arriving |
| inlet / outlet | what a task consumes / produces (declared on the task) | `add_partitions` emits keyed events through an outlet |
| ADRQ | `asset_dag_run_queue`, one row per (asset, consumer DAG) — the trigger flag for non-partitioned scheduling | why bursts conflate into one run |
| APDR | `asset_partition_dag_run`, a provisional partition run waiting to fire | unindexed; trimmed only by cascade with `dag_run` cleanup |
| PAKL | `partitioned_asset_key_log`, "event E contributed to APDR A" | linear storage evidence |
| asset state store | 3.3 per-asset key/value table, `get/set/delete` from tasks | per-account status ledger |
| pool | global concurrency slots | parking spaces `K` |
| scheduler loop | the scheduler's main iteration every ~1 s | one `expand()` blocked it for 317 s |

## Seven key points (mechanism + number)

1. **`expand()` is one transaction.** Creating N mapped TIs happens inside one scheduler loop and one DB transaction:
   10k → 60 s, 100k → **317 s** blocked; health threshold 30 s. Python/ORM-bound (DB idle). Upstream #69565 halves it.
2. **Every mapped TI downloads the whole upstream list** (`xcom_arg.py:338`): O(N²) bytes. 10k → 1.5 GB (fine),
   100k → 150 GB (not). Flat mapping is a thousands-scale tool.
3. **Native partitions: storage linear, write time quadratic.** APDR/PAKL/events/runs all exactly N rows at 1k/10k/100k. But
   `asset_partition_dag_run(target_dag_id, partition_key)` is unindexed and grows for the whole run-retention window: 500-key registration went
   3.6 → 7.0 s as the table grew, back to 3.4 s with an index. SDK client timeout 5 s ⇒ ≤ ~900 keys per emitting task.
4. **No per-key concurrency control, no conflation, in the partition path.** A key that re-arrives while running gets a second
   concurrent run; `max_active_runs` is DAG-wide so unrelated keys queue behind duplicates. The non-partitioned OR path
   conflates per scheduler loop (one ADRQ row per asset/DAG). Extension point: `[core] asset_manager_class`. A 30-line
   prototype (Patch E: hold a pending run while the key is queued/running; absorb events into a not-yet-started run)
   turned E1 from 4 concurrent-ish ACC1 runs into 3 serialized ones with ACC5 admitted immediately.
5. **The three grains need not match.** Schedule at region/pack grain, partition and observe at account grain: one batch task
   emits `add_partitions(accounts)` for lineage and writes per-account status to the state store. Works on stock 3.3.2 (E4/E5).
6. **Burst latency is capacity, trickle latency is warm-up.** Burst p95 ≈ N·p/K + S regardless of policy; trickle p95 ≥ S + fill
   time. SLA levers are K (subnet IPs: a job with w workers needs w+1) and S (warm Spark / Lambda-class engines), not the
   batching policy. SLA-driven batching ("as large as the SLA allows, never below the capacity floor λS/(K−λp)") serves both
   regimes at 1.3–1.8× the cost of big fixed packs and never destabilises.
7. **MWAA makes every fix slower.** 3.3.1 only, no DB access (no index, no APDR cleanup), Celery only, Execution API in the
   webserver, 10 requests/s REST throttle. Self-hosted keeps the freedom to patch and index.

## The quiz (with answers — cover the right column)

| question | answer |
|---|---|
| A task returns 100k ids to `expand()` with defaults. What happens? | `[core] max_map_length` (default 1024) rejects the XCom push with HTTP 400 `unmappable_return_value_length`; the *upstream* task fails. It is a signpost, not the bottleneck. |
| Same key arrives 3× in 30 s. Partitioned consumer: how many runs? Non-partitioned OR consumer? | Partitioned: 3 (one per event; first two may run concurrently). OR: 1 if within one loop; 2 if the first run was already running (later events collapse into one follow-up). |
| How do you implement re-run ("all arrived, then v2 of one dataset")? | OR schedule + `short_circuit` gate reading `inlet_events[asset]`; pick `max(extra.version)` per input, never `[-1]` (that is registration order — a burst registered v3 last). |
| Why not bake a daily-changing region→account map into `AllowedKeyMapper`? | The mapper is serialized into the DAG (36 KB for 10k keys) and copied into every APDR row's `rollup_fingerprint` (33.6 KB/row → ~340 MB/day); a change discards all pending partition runs. Resolve the mapping in the producer. |
| 100 IPs left, 10-worker Glue jobs, S=60 s, p=1 s, 10k-account burst: p95? IPs for a 5-min SLA? | K = 100/11 = 9; p95 ≈ 10000/9 + 60 ≈ 1170 s ≈ 19.5 min; K ≥ 10000/(300−60) = 42 jobs = 462 IPs. |
| Four preconditions for account-grain native partitions at 10k/day; which fails on MWAA? | Serialized emitters ≤ ~900 keys; `max_active_runs` raised + accept 500/tick and 3–10 runs/s; own per-key mutex/conflation; APDR index + cleanup. The last one — no DB access on MWAA. |

## Balance-sheet readiness model (E6)

Inputs are *reference* (rates, FX) and *root* (positions, cashflows). Readiness = `all(reference for as-of D) and any(root for D)`,
first run and every run after. Implement as OR trigger + gate predicate scoped to the business date; choose `max(version)`
per input; put `{as_of, roots included, versions}` on the output event so partial results are distinguishable. Measured: 8
arrivals → 4 runs, exactly where the predicate says; a new business date does not inherit yesterday's reference data.

## What I would say in the meeting (three sentences)

Balance sheet can go live on Airflow 3.3 assets today: tens of inputs, a handful of partitions, re-run via OR + gate, all
measured. Revenue should be scheduled at pack grain and observed at account grain — Airflow's own three-grain split — with
dynamic, SLA-driven batching; account-grain scheduling at 100k is blocked by four concrete, contributable gaps, one of which
MWAA cannot take. The SLA is decided by engine start-up and subnet capacity, not by the orchestrator.
