# Dynamic, capacity-aware batching for account-grain PnL

*2026-09-30. Simulation: `bench/sim_batching.py`, `bench/sim_charts.py`. Airflow prototype: `bench/dags/exp_e5_dynamic_batcher.py`,
driver `bench/exp_e5.py`. Complements §6 of [REPORT.md](../REPORT.md) and Q3/Q6 of the [answers](answers-to-mwaa-discussion.md).*

## 1. The problem, stated as a control problem

Revenue's inputs arrive per firm account: a start-of-day burst (up to 10k–100k accounts within minutes) followed by a trickle
for the rest of the day. A calculation job costs `S + p·b` seconds of wall time (`S` = engine start-up, `p` = per-account
compute, `b` = accounts in the batch), and at most `K` jobs run concurrently. `K` is the real constraint on AWS: Glue/EMR
workers each take an ENI and a private IP, so a /24 subnet shared with other tenants leaves room for a handful to a few dozen
concurrent jobs. The SLA target on the proposal page is p95 ≤ 2 minutes from data arrival to result.

Fixed batching cannot serve both regimes: big packs amortize `S` in the burst but make the trickle wait to fill; small packs
serve the trickle but burn capacity on start-ups during the burst. **Dynamic batching** decides `b` at each scheduler tick from
the current ready set, the measured arrival rate `λ`, the free capacity and the oldest waiting account.

### Policies compared
| policy | rule |
|---|---|
| `per_account` | one job per account, FIFO through K slots |
| `fixed_pack` | dispatch when `pack_size` (500) accounts are ready or the oldest has waited `t_max` |
| `fixed_cadence` | every 5 min, everything ready as one job |
| `adaptive` | latency-optimal size `b = √(2·λ·S/p)`, never below the **capacity floor** `b_cap = λ·S / (K − λ·p)` (the smallest batch at which K slots sustain λ), split across free slots, dispatch when ≥ `b_min` ready or oldest > `t_max` |
| `adaptive_sla` | same floor, but size = the **largest** batch whose expected latency `b/λ + S + p·b` still meets the target — minimizes cost subject to the SLA |

The capacity floor is what makes the policy stable: without it (first version of the simulator) a 100k burst at K=50
saturated the slots with small jobs and the tail grew to 43 minutes.

## 2. Simulation results (10k-account burst over 10 min, then 1 account/s for 4 h)

![frontier](img/batching_frontier.png)

| engine profile | best policy for the 2-min trickle SLA | burst p95 (capacity-bound) | cost vs fixed pack |
|---|---|---|---|
| **Glue-class** S=60 s, p=1 s, K=20 | `adaptive_sla` (target 5 min): trickle p95 **2.1 min** | 9.7 min for every policy except `adaptive` (7.2) | 1.8× (15.7 vs 8.7 job-h) |
| Glue-class, subnet-bound K=8 | `adaptive_sla`: trickle 2.1 min | **30 min** — no policy helps; only K does | 1.7× |
| **Warm Spark** S=10 s, p=0.5 s, K=20 | `adaptive_sla`: **burst 2.0 / trickle 0.9 min** | 2.0 min | 1.3× (4.9 vs 3.7 job-h) |
| **Lambda-class** S=1 s, p=2 s, K=1000 | `per_account`: **p95 6 s** in both phases; `adaptive_sla` 1.7 / 0.5 min | — | per-account = 20 job-h of pure compute, no start-up waste |
| Lambda-class, 100k burst in 30 min | `per_account` p95 6 s; `adaptive_sla` 2.0 / 0.8 min | — | — |

`per_account` on a Glue-class engine is **unstable** at 1 account/s with K ≤ 50: 60 s of start-up per account means K
slots sustain only K/61 accounts per second, so the queue never drains (18,777 accounts still waiting after 4 h at K=20).

Three conclusions:

1. **In the burst, latency is set by capacity, not policy:** p95 ≈ N·p/K + S. Turned around, the capacity you need for an
   SLA is `K ≥ N·p / (SLA − S)`, and on AWS `K` translates to IP addresses: a job with `w` workers needs `w+1` IPs.
2. **In the trickle, latency is set by start-up cost:** p95 ≥ S + fill time. With S = 60 s only small batches meet 2 minutes,
   at 2–4× the job-hours; with S = 10 s the SLA is met at 1.3×; with S ≈ 1 s per-account execution is simply the best option.
   So the lever for the SLA is the engine's start-up cost, not the orchestration policy.
3. **What dynamic batching buys:** one policy that is never unstable, gives small-batch latency in the trickle and big-batch
   cost in the burst, and needs no per-phase tuning. `adaptive_sla` is the defensible default: "as large as the SLA allows".

## 3. Engine choice: why the 100k-unit case points away from Spark

If the per-account PnL has no cross-account netting (revenue) and inputs can be read per account (storage partitioned by
firm account, or a key-value slice), the work is embarrassingly parallel and a Lambda-class engine fits it: ~1 s start-up,
account-level concurrency in the thousands, and in a VPC it uses shared Hyperplane ENIs rather than one IP per worker, so the
subnet ceiling largely disappears. Rough cost per 100k accounts at 2 s × 2 GB each is in the single-digit dollars — the same
order as Glue's DPU-hours for the same compute — so cost is not the discriminator; latency and operational simplicity are.
The proposal page's own list ("alternative execution engines: kedro / polars") is this direction. Balance sheet, with
cross-account netting, remains a Spark-shaped job.

What this does to orchestration: **Airflow's unit stays the batch.** One task hands a batch of accounts to the fan-out
mechanism (Step Functions Distributed Map, SQS + Lambda, or a boto3 async fan-out inside the task), waits (deferrably), then
emits one keyed asset event per account and updates per-account status — exactly E4/E5 with a different `spark` step. Airflow
never sees 100k task instances; per-account parallelism lives in the engine. Per-account lineage and status stay in Airflow.

## 4. Mapping onto Airflow 3.3 primitives

| concept | Airflow primitive |
|---|---|
| capacity `K` (IPs, DPUs, engine concurrency) | a **pool** (`spark_jobs`, K slots); heterogeneous jobs use `pool_slots = workers + 1`; excess batches queue in Airflow = back-pressure |
| batch decision | `claim` task: ready set from the event log (`inlet_events[input].after(watermark)`), in-flight set and per-account status from the **asset state store**, `plan_batches()` = the policy |
| one job per batch, capacity-gated | `spark.expand(batch=batches)` with `pool="spark_jobs"`; in production `GlueJobOperator(deferrable=True)` / `LambdaInvoke…` so waiting does not hold a worker slot |
| per-account lineage from a batch | `publish.expand(...)`: `outlet_events[output].add_partitions(batch)` — one event per account, `source_run_id` = the batch run |
| failure semantics | `release` task with `trigger_rule="one_failed"` resets the claims; claims carry the batch id so a stale "running" can be detected |
| cadence | cron every minute (keyed events do not trigger non-partitioned DAGs); the policy's `t_max` bounds the added wait |

Two implementation details learned the hard way: the state-store accessor is scoped to the task's declared
inlets/outlets (declare `inlets=[input]` on every task that touches it), and "processed" must mean `status == done`, not
"a version was recorded", or released accounts are never re-claimed.

## 5. Airflow prototype (E5) — burst of 60 accounts, then 1 account every 6 s; K = 3, S = 15 s, p = 0.5 s

⏳ results of `bench/exp_e5.py --policy adaptive_sla` vs `--policy fixed` go here.
