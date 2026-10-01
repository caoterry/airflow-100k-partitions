# The core idea: two granularities, one leaky bucket

**Trigger at account granularity, execute at batch granularity, and put a leaky bucket between the two.**

Everything else in this repository (the measurements, the batcher, the journal, the gates) follows from that one decision. This
page explains why the decision is forced, what the bucket is made of, what the prototype does and does not do yet, and how the
same design compares with Kafka.

## 1. Native Airflow couples the two granularities

In native Airflow, the grain at which work is triggered is the grain at which it runs.

```
producer task succeeds
  → api-server writes an asset_event row            ("positions was updated")
  → api-server writes an asset_dag_run_queue row    (one per subscribing Dag; unkeyed events only)
  → scheduler sees the queue rows; once the consumer's asset condition is met, it creates a DagRun
    and deletes the consumed queue rows
```

A keyed event (one with a `partition_key`) writes no queue row at all. It reaches only consumers that subscribe per partition:
those get one `asset_partition_dag_run` row and then one DagRun **per key**, here per account.

That is the right model for a few dozen datasets. At 100,000 accounts it fails on scheduler time, because the scheduler pays per
run, not per account:

```
scheduler time ≈ runs × (longest chain of tasks + 1) × cost of one look at a run
```

- Each look at a run schedules every task whose dependencies are met, so the number of looks follows the longest chain, not the
  number of tasks. It is a lower bound: some looks find tasks still running and do nothing.
- A one-task run, which needs two looks, measured 25–40 ms of scheduler time in total, so roughly 12–20 ms per look
  (REPORT §4.7). 100,000 one-task runs therefore need about 0.7–1.1 hours of scheduler time per day as a lower bound; REPORT
  scales it to about 1 hour.
- Measured on Airflow 3.3.2 as shipped with native partitions and one scheduler: the 100k run was stopped after 26 minutes.
  All 100,000 runs had been created and 80,900 of their tasks had succeeded, but only 3,698 runs were marked finished: the
  scheduler looks at never-examined runs first, so while new runs kept arriving it did not get back to mark finished runs
  done (REPORT §4.2). With three hand-created indexes (two on `asset_partition_dag_run`, one on `dag_run`), which MWAA's
  managed metadata database does not let us add, and two schedulers: all 100,000 finished in 31 minutes, about 110 runs/s
  once run creation stopped (REPORT §4.2a). That run added the indexes and the second scheduler at once, so the index's own
  share of the gain was not measured.
- Storage grows linearly, not as n² (REPORT §4.3): 100,000 `dag_run` rows and one `task_instance` row per task per run per day,
  roughly 65 GB of metadata a year unless runs are cleaned with `airflow db clean`.

## 2. The proposed design separates them

```
  producers                 the bucket                          dispatcher                  engine
 ─────────────          ──────────────────────               ───────────────────          ──────────────
 emit one keyed  ───▶   one row per account,          ───▶   claims pending accounts ───▶  one job per batch
 asset event per        latest version only                  into the free engine          (a mapped task)
 changed account        (pending / in flight / done)         slots, sizes the batches            │
                                ▲                                                                  │
                                └───────────────────────────── publish: mark done, emit ◀──────────┘
                                                               per-account events for lineage
```

| Part | What it is | Real name |
|---|---|---|
| Input to the bucket | One event per changed account, version in `extra` | `asset_event` rows with `partition_key` = account, written via `outlet_events[asset].add_partitions(...)` |
| Read position | Where the dispatcher last read the event log | Watermark kept in the journal |
| The bucket | One row per account; versions that arrive while it waits collapse to the latest | Journal table (prototype: `bench_ledger.accounts`, columns `status`, `seen_version`, `done_version`; production must also record the version claimed, see §3) |
| Drain | Atomic claim of pending accounts, batch sizing | Batcher Dag's `claim` task: `SELECT … FOR UPDATE SKIP LOCKED` |
| Drain rate cap | Engine capacity K | Airflow pool (`spark_jobs`), size = K |
| Completion | Results written, accounts marked done, lineage emitted | Batcher Dag's `publish` task |

The scheduler no longer sees 100,000 account runs. It sees about 170 batch jobs a day (100,000 accounts at the prototype's
600-account batch cap), run as mapped tasks inside one batcher run per cadence tick, which is about 1,440 batcher runs a day on
a one-minute cron.

**What Airflow still does:** triggering, task retries while a batch is still in flight, waits on the engine (deferrable in
production; the prototype simulates the job with a sleeping task), back-pressure through the pool, a task-instance record and
logs for every batch, the UI, and lineage through keyed events. Clearing a batch task in the UI is not a safe rerun: it goes
around `claim` and replays the account list stored in XCom, even if those accounts are now in flight in another batch, and in
the prototype `publish` then marks them done whatever batch holds them. To recompute accounts, return them to pending in the
journal. The fix, not built yet, passes the batch id to `spark` and `publish` so both act only on rows still in flight under
that id.

**What we write:** the bucket itself, a journal table and the `claim`/`publish` logic, a few hundred lines with an owner. The
rule that keeps it from becoming a second scheduler: **no timing, dependency or retry logic inside the journal.** It only
records which accounts have an unprocessed version and which are in flight.

**One rule for the account grain:** keyed events must not be subscribed to per partition. A Dag with
`PartitionedAssetTimetable` on the output asset turns every account back into its own DagRun and writes rows to
`asset_partition_dag_run`, the unindexed table the open index PR targets. In the first 100k batcher run such a test consumer,
left on as a lineage check, created 54,000 runs, one per published account (docs/dynamic-batching.md §5a).

## 3. This is a leaky bucket

Arrivals are bursty: a start-of-day burst of up to 100,000 accounts, then a trickle of adjustments. The bucket absorbs the
burst and is meant to drain at the engine's capacity. It differs from a textbook leaky bucket in two ways:

- **Per-key coalescing.** Versions of one account that arrive while it waits in the bucket become one recompute on the latest
  version, because the journal coalesces on write. In the prototype, a version that arrives while the account is in flight is
  wrongly marked done when the batch publishes, because `done()` copies `seen_version`; a production journal must record the
  version it claimed and compare it at publish.
- **Adaptive drain.** The amount drained per step is not fixed. Each tick the dispatcher computes a size target from the
  measured arrival rate, `b_sla = (target − startup) / (1/arrival_rate + per_account_time)` (the largest batch whose fill time,
  start-up and run time still meet the target), raises it to a capacity floor when the slots could not otherwise keep up, and
  then splits everything pending across the free slots, up to a hard per-batch cap (`b_max`, 600 in the 100k run). The SLA term
  shapes batches in a trickle; in a burst, batch size is set by backlog ÷ free slots and the cap
  (`bench/dags/exp_e5_dynamic_batcher.py`, `plan_batches`).

Where it stands: in the 100k test the prototype published 84,000 of 100,040 accounts by the time the driver's 40-minute window
closed (6,000 more were in flight and 10,040 pending). No account was published until 20:15, 13 minutes after the burst began,
while `claim` ticks took 36–388 s as producers were still registering keys. From then until the window closed at 20:45 it ran
at about 2,800 accounts a minute. The engine was a stand-in, a sleep of 15 s plus 0.02 s per account in each of K = 10 slots,
which could take roughly 13,000 a minute, and the slots were never the limit (pool queue wait p50 0 s, max 7 s). The limit was
the one-minute cadence: each run sees only the slots free at its own claim time. Per-account latency, from input event to
published event, was p50 27 min and p95 37 min over the 84,000 published; the 16,040 not yet published, including all 40
trickle accounts, would raise both. A continuously running dispatcher that claims as slots free up is the next step, not yet
built (docs/dynamic-batching.md §5a).

The floor is key registration for lineage: each key is one `asset_event` insert inside the `publish` task-success request, so
that request grows with the batch. One request registering keys alone for a per-partition consumer measured about 5 ms per key
(REPORT §4.2), which would be 8–9 minutes of API-server work per 100,000 accounts. This design has no per-partition consumer and
takes no asset row lock, so publishes are not serialized. In the 100k run they overlapped: each 600-key publish request took
2.4–12.1 s and 102 of 140 passed the 5 s timeout (api-server log of that run;
docs/dynamic-batching.md §5a).

End-user status is meant to be a view on the journal (pending / in flight / done, with the version done). It is not built yet,
and it needs a failed state with the error and a timestamp. In the prototype, `release` returns every batch of a failed run to
pending, and the next `claim` takes the failed accounts again with no limit, so an account that always fails is retried on
every tick. In the design only the failed batch's accounts move to failed, and a failed account goes back to pending only when
a newer version arrives or an operator re-queues it. Retries stay with Airflow task retries, so the journal holds no retry logic.

**Balance sheet is the exception.** Its results aggregate across accounts, so a late input means recomputing the whole business
date. It uses no bucket: an OR trigger over its inputs plus a gate that checks "all reference inputs and at least one root
input" for the date and recomputes with the latest version of each (answers Q1, experiment E6).

## 4. Extension: the same bucket on Kafka

A Kafka-based design has the same shape. The pieces map closely, with the differences noted:

| Kafka | This design |
|---|---|
| Keyed messages on a topic | Keyed asset events (`asset_event.partition_key`) |
| Consumer offset | Journal watermark: the timestamp of the last event read, re-read with a 5-second overlap and advanced at `claim`, when events are merged into the journal. Unlike an offset it can skip an event: a row becomes visible only when its producer's request commits, up to 38 s after its timestamp in the 100k test, and an event that commits after a claim has moved the watermark more than 5 s past its timestamp is never read. None was missed in that test, but the test could not show it. Not fixed yet |
| Kafka Streams KTable (latest value per key; log compaction alone is lazy and a live consumer still sees every version) | Journal row per account with the latest version, coalesced on write |
| Consumer poll and batch | `claim` and batch sizing |
| Consumer pull pace (`max.poll.records`, `pause()`/`resume()`), parallelism bounded by partition count | Pool of K slots |
| Commit after processing | `publish` marks each account in the batch done in the journal; done is tracked per account, not by the read position |

The difference is which half comes for free:

| | Airflow | Kafka |
|---|---|---|
| Native | Orchestration: triggering, retries, waiting on Spark or Glue, back-pressure, run history, UI, lineage; the same platform as balance sheet; offered as MWAA | The bucket: offsets, consumer groups, millisecond delivery, and per-key coalescing through Kafka Streams state |
| Written by us | The bucket: journal plus `claim`/`publish` | The orchestration: launching and retrying engine jobs, recording what each batch ran, a UI, lineage |
| Latency floor | About a minute in a 60-account trickle run with K = 3: the one-minute dispatcher cadence plus task scheduling (p95 77 s, each job a 15–20 s sleep standing in for the engine), plus lineage registration in `publish`, which grows with the batch (a 600-account publish took 2.4–12.1 s in the 100k run). At 100k the one-minute dispatcher set the pace: p95 37 min | Milliseconds of delivery |

Kafka's latency advantage only pays off if the engine is fast. In the simulator (a 10,000-account burst, then 1 account/s,
K = 20), burst latency is set by engine capacity and trickle latency by engine start-up, not by the message path. With
Glue-class start-up of about 60 seconds, no policy brings the burst p95 under 2 minutes (best 7.2 min), and only small
per-arrival batches reach a 2-minute trickle p95 (1.6 min, at about 4x the job-hours of fixed packing). With warm Spark at about
10 seconds, SLA-driven batching meets 2 minutes in both phases (burst 2.0 min, trickle 0.9 min) (docs/dynamic-batching.md §2).
The prototype did not reach this. At 100k with 15 s start-up its p95 was 37 min, because the one-minute cron dispatcher set the
pace, not engine capacity. In every prototype run the engine was a sleep of start-up plus per-account time.

A hybrid is possible in principle. Airflow 3.3 can wake a Dag from a message queue: an `AssetWatcher` whose trigger runs in the
triggerer, using the common.messaging provider's `MessageQueueTrigger`. Released providers support Kafka, SQS, Redis pub/sub,
Google Pub/Sub, Azure Service Bus and IBM MQ; a Kinesis scheme is merged on main but not yet released, and MWAA 3.3.1 bundles
Amazon provider 9.34.0. Kafka or SQS would then feed the bucket and Airflow would stay the orchestrator, with two differences
from the keyed path: a watcher's asset event carries no `partition_key`, so the account would travel in `extra["payload"]`; and
by default the Kafka trigger commits each offset before Airflow records the event and handles one message per trigger run. The
Kafka provider (1.13.0 and later) accepts `commit_offset=False` through `MessageQueueTrigger`, which leaves the commit to
downstream tasks. None of this has been tried here, nor on MWAA.

**The deciding question** is what revenue users need more: every change reflected within seconds, or recomputes that are
traceable, rerunnable and run on the same platform as balance sheet. The current reading is the second, which favours Airflow
with a small hand-written bucket.
