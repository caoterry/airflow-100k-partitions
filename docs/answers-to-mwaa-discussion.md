# Answers to the "AWS MWAA Discussion" questions

*Written 2026-09-30 against Apache Airflow 3.3.2 (company baseline 3.3.0/3.3.1) with code references, benchmark data from
[REPORT.md](../REPORT.md) and three targeted semantics experiments (E1–E3, appendix). Where the answer is "not natively",
the least-bad pattern on 3.3.x and the upstream gap are both stated.*

**One-paragraph position.** Airflow 3.3 can orchestrate the balance-sheet workload as designed (tens of inputs, a handful of
netting-group partitions, 20-minute Glue calculations) and the evaluation matrix's Option B (assets) is the right basis. It
cannot, as shipped, schedule at firm-account grain for the revenue workload (10k+ keys per business date): the partition
machinery has no per-key concurrency control, no conflation, an unindexed and never-pruned bookkeeping table, and a per-key
write path that times out past ~900 keys per task. The modelling answer to Q6 is therefore "yes, use a coarser scheduling
grain" — but keep per-account lineage and status *inside* Airflow via partition keys emitted in bulk and the 3.3 asset state
store, not off-graph. Every gap below is a contained upstream change; the benchmark harness in this repo is the evidence for them.

---

## Q1. Reruns — "we want additional runs on any later update, not only when *all* assets updated again"

**What Airflow does.** An AND condition (`a1 & a2 & a3`) fires when every asset has at least one new event since the DAG's
last asset-triggered run; the queue that tracks this is `asset_dag_run_queue` (one row per asset × consumer DAG), and the rows
are deleted when the run is created. Experiment E2 reproduces the diagram exactly: the AND consumer fired once when the third
asset arrived and **did not** fire when `a3` re-arrived (the "red X").

**The pattern that works (your "custom-gate", with two corrections).**

1. Schedule the consumer on **OR** (`a1 | a2 | a3`): a run per update. Measured: a run for every event, and a burst of five
   events on one asset in the same scheduler loop produced **one** run. Mechanism (`models/asset.py:751-759`,
   `assets/manager.py:828-835`): the queue row `asset_dag_run_queue` is keyed `(asset_id, target_dag_id)` and upserted, so
   events conflate per loop; and for asset-triggered runs `max_active_runs` gates *creation* (queued + running count,
   `models/dag.py:767-788`), so at the cap further events keep conflating into the pending flag and are released as one run.
   Two caveats from the source check: repeat events that arrive after the flag was set may not be attached to that run's
   `triggering_asset_events` (they appear in the next run's window), and on main the queue becomes per-event
   (`(target_dag_id, asset_event_id)`), so every event is consumed.
2. First task = gate: `@task.short_circuit(inlets=[a1, a2, a3])` that reads every input's events via `inlet_events[asset]`
   (the asset's *full* history, not only the events that triggered this run; `GET /execution/asset-events/by-asset`, ordered
   by timestamp, **no default limit or pagination**, so always bound it with `.after(watermark)` / `.ascending(False).limit(n)`)
   and skips downstream until every input has one. Keep outlets off the gate: a short-circuit gate still *succeeds*, so
   outlets declared on it would emit events even when it skips.
3. **Pick the version explicitly, not positionally.** `inlet_events[asset][-1]` is the most recently *registered* event
   (ordered by `asset_event.timestamp`), which is not the newest business version when producers run concurrently. In E2 a
   burst v3…v7 registered as v5, v7, v6, v4, v3 and `[-1]` returned **v3**. Carry `{"version": n, "path": …}` in the event
   `extra` and take `max(events, key=version)` per input; that also gives you "v2 for the one that re-arrived, v1 for the
   others" for free.

"Echoing" (re-emitting the other assets' events to satisfy AND) is not recommended: it fabricates lineage, doubles event
volume, and still cannot express "which version". A reconcile daemon is not needed for correctness, only as a safety net.

**The full balance-sheet model (E6).** Inputs split into *reference* data (rates, FX) and *root* data (positions, cashflows);
the readiness predicate is `all(reference for as-of D present) and any(root for D present)`, first run and every run after.
That is a gate predicate, not a schedule expression: `schedule = OR over all inputs`, gate scoped to the business date named
by the triggering event, versions chosen as `max(version)` per input for that date, and the output event carries
`{as_of, roots included, versions used}` so downstream can tell a partial result from a complete one. Measured sequence
(`bench/exp_e6.py`): rates → SKIP; positions → SKIP (fx missing); fx → **RUN** (roots: positions); cashflows → **RUN**
(roots: both); rates v2 → **RUN** with rates 2 / others 1; next business date: positions → SKIP, rates → SKIP, fx → **RUN**
for 2026-10-01 — date scoping holds, and the "v2 for the one that re-arrived, v1 for the others" rule falls out of
`max(version)` per input with no daemon.

**Roadmap.** `batch_asset_events` (PR #68517, 3.4) makes conflation explicit; `AssetEventSensor` (#70225) and
`AssetPartitionSensor` (#67941) let a time-scheduled DAG wait for events. Nothing on the roadmap changes the AND semantics.

## Q2. Partition mappings — "region→account / account→trial changes daily; must mappers be static?"

**Yes, in 3.3 mappers are static data baked into the DAG, and the cost is worse than the page suspected.** The mapper is
serialized with the DAG, and the *entire mapper definition* is copied into `asset_partition_dag_run.rollup_fingerprint` for
every pending partition run, so that the scheduler can discard pending runs whose mapper definition changed. Experiment E3:

| | identity mapper | `AllowedKeyMapper` with 10,000 keys |
|---|---|---|
| serialized DAG | 1.3 KB | 36 KB |
| `dag.partition_mapper_info` | 76 B | 76 B |
| `rollup_fingerprint` **per APDR row** | 114 B | **33,659 B** |

A 10k-key mapper therefore writes ~34 KB per partition run (340 MB/day at 10k keys/day) into a table that is never pruned,
and every daily change to the mapping invalidates all pending partition runs. A disallowed key is silently dropped for that
consumer with a `Log` row ("failed to map partition_key"). Doing a DB call inside `to_downstream` is technically possible
(the mapper runs inside the API server's task-success request, under the asset row lock) and is not an intended pattern: it
would put reference-data latency into every task completion.

**Recommendation.** Keep mappers structural (identity, prefix/product, temporal). Resolve business mappings **in the
producer**, which knows the data: the task that lands region data looks up the affected accounts/trials and emits those as
partition keys (`outlet_events[asset].add_partitions(keys)`), ≤ ~900 keys per task (Q5). If the mapping must live in
orchestration, a tiny "router" DAG (OR-scheduled on the raw inputs) that reads the reference table and emits target keys is
the same idea with one more hop.

## Q3. Per-partition concurrency & batching

**3.1 Example variations (measured, E1).** Consumer on `PartitionedAssetTimetable`, `max_active_runs=2`, task sleeps 45 s:

| step | event | what Airflow 3.3.2 did |
|---|---|---|
| 1 | ACC1 | run #1 RUNNING |
| 2 | ACC1 again, run #1 running | **second APDR and second run, RUNNING concurrently** (no per-key mutex, no conflation) |
| 3 | ACC1 + ACC5, two runs running | both QUEUED behind `max_active_runs`; **ACC5 waits behind the duplicate ACC1** (coarse-grained, FIFO by queued time) |
| 4 | ACC1 while an ACC1 run is QUEUED | **another APDR/run** (not de-duplicated against the queued one) |
| end | | ACC1 ran 4 times, ACC5 once |

So: T=4 ACC5 *should* start (fine-grained) but with only `max_active_runs` as the knob it does not; T=4 ACC1 *should not* queue
a second time (conflated) but it does. #71070/#71074 (3.4) de-duplicate *pending* APDRs, not queued or running runs.

**3.2 Concurrency.** (a) There is no native "one run at a time per partition key" and no per-key ordering: partition runs are
created QUEUED without any `max_active_runs` check and released FIFO by a per-DAG running count with no partition term
(`models/dagrun.py:707-716,752-753`; `_lock_asset_model` serializes writers per *producer asset*, not per key). The pending-key
de-duplication PR (#71074 for #71070) is unmerged as of 2026-09-30 and only covers *pending* APDRs. (b) A gate task that polls
the REST API is a legitimate stop-gap; call the public `/api/v2`, never `/ui/*` (private, unversioned). Mind the filter
semantics: `partition_key_pattern` is a case-insensitive `ILIKE '%value%'` substring match with unescaped wildcards, so use
`partition_key_prefix_pattern` (a range scan) for exact keys, and there is no index containing `dag_run.partition_key`. A
deferrable version is not possible with stock triggers (they have no DB access and the count endpoint they can call filters
only by state/run ids, not partition key). At 10k keys polling does not scale anyway: every run spends a task slot polling, and
admission decisions belong where the run is created, not after. (c) **Where the extension belongs:** `[core] asset_manager_class`
is a documented extension point. A subclass of `AssetManager` that overrides `_queue_partitioned_dags` / `_get_or_create_apdr`
can reuse the pending-or-queued run for a key (conflation) or hold a key while one is running (mutex) **without patching core**
— that is the "extension" the evaluation matrix asks about, and the natural shape for an upstream `max_active_runs_per_partition_key`.
Pools cannot help (static, global; one per key would be 10k pools).

**Prototype of that extension (Patch E, `patches/patch_e_partition_key_mutex.py`, ~30 lines).** Scheduler side: a pending
partition run whose key already has a QUEUED/RUNNING run in the same DAG is held. Asset-manager side: a new event for a key
whose latest run has not started yet is attached to that run instead of provisioning another. E1 rerun with the patch:

| step | as shipped (3.3.2) | with Patch E |
|---|---|---|
| ACC1 again while run #1 running | second run, **concurrent** | pending, held; fires the moment run #1 finishes |
| ACC1 + ACC5 while an ACC1 run is running | both queue behind `max_active_runs`, FIFO | ACC5 **starts immediately**; ACC1 stays pending |
| ACC1 again while ACC1 pending/queued | another run | absorbed into the pending run |
| total | ACC1 × 4 (two concurrent), ACC5 × 1 | ACC1 × 3, never concurrent, one follow-up per "burst during a run"; ACC5 × 1 |

That is the T=1…5 table from the discussion page with "conflated delivery" and "fine-grained admission" both true. The same
logic is what an upstream `max_active_runs_per_partition_key` would carry; as a local extension it could live in a custom
`AssetManager` (`[core] asset_manager_class`) for the conflation half, but the mutex half needs the scheduler, i.e. a core
change — which is why it belongs upstream.

**3.3 Batching many ready accounts into one Spark job while keeping per-account lineage — supported, and it is the recommended
shape.** Build one **batcher DAG**, not account-level runs:

1. `schedule = (input_1 | … | input_6)` (or a short cron), `max_active_runs = N` (N concurrent batches).
2. Gate task: read the ready set (events since the last processed watermark) and the in-flight set from the **asset state
   store** (Airflow 3.3, per-asset key/value: `account → {status, batch_id, version}`); claim the accounts that are ready
   and not in flight; skip if none.
3. One task launches one Spark/Glue job for the claimed batch; on success it emits
   `outlet_events[output_asset].add_partitions(claimed_accounts)` — one event per account, so downstream partitioned consumers,
   `GET /dags/{id}/dagRuns/{run_id}/upstreamAssetEvents` (3.3.2) and the asset-event key filters (main/3.4) all see
   **per-account lineage from a single task**; then it writes per-account status back to the state store. Note that keyed
   events never queue non-partition-aware DAGs (`assets/manager.py:534-535`), so the batcher itself is cron/level-triggered
   (or the producer emits one extra un-keyed event as a poke).

Dynamic task mapping and task groups are the wrong tool here (one task instance per account: §4.1/§4.4 of the report).
This is also what AIP-104 (`.iterate()` / `.spread(across=N)`, PR #62922, 3.4) is formalizing. Batch *sizes* should not be
fixed: a capacity-aware, SLA-driven policy (size = largest batch that still meets the latency target, never below the batch
size at which K slots sustain the arrival rate) serves both the start-of-day burst and the trickle with one rule — see
[dynamic-batching.md](dynamic-batching.md) for the simulation across Glue / warm-Spark / Lambda-class engines and the E5
prototype on 3.3.2 (pool = capacity, `expand()` over batches, `add_partitions` for lineage).

## Q4. End-user tracking through Airflow APIs (10s–100s of users)

Not advisable. In Airflow 3 the same API server that serves `/api/v2` also serves the Execution API that every running task
heartbeats through (on MWAA it is the webserver, 2–5 Fargate containers), and MWAA throttles the REST endpoint at 10 requests/s.
The REST filters you would need exist (`partition_key_pattern`, `partition_key_prefix_pattern`, `partition_date_gte/lte`,
`state`), but page size is capped at 100 and the 3.3.x pending-partitions UI endpoint is unpaginated. Project status out
instead: DagRun listeners (`on_dag_run_running/success/failed`, fired in the scheduler with the ORM `DagRun` incl. `partition_key`;
QUEUED is deliberately not notified, exceptions are swallowed) or the batcher's own state-store writes feed a status table your
users query; Airflow's UI stays for operators. The asset state store itself is a point-lookup KV (`get/set/delete/clear` from
tasks, scoped to the task's declared inlets/outlets; REST `GET/PUT/DELETE /assets/{id}/state-store/{key}` plus a paginated
list); `[state_store] max_value_storage_bytes` (65,535) is enforced only on the REST path, and there is no retention for asset
entries. It is the right ledger for per-account status; it is not a query engine for end users.

## Q5. Scaling — "10k–100k account-grain partitions per business date: within intentions? where does it break?"

Not within intentions at 100k; feasible with care at 10k. Measured on 3.3.2 (details in the report):

| limit | where | number |
|---|---|---|
| keys per emitting task | `[workers] execution_api_timeout` 5 s vs ~5 ms/key registration | **~900 keys**; 10k in one task → 53 s request, 5 retries |
| registration cost growth | `asset_partition_dag_run` has no index on `(target_dag_id, partition_key)` and is never pruned | 3.6 s → 7.0 s per 500 keys as the table grew to 60k rows; 3.4 s with an index |
| partition-run creation | 500 per scheduler tick, hard-coded | ~80 runs/s |
| partition-run completion | scheduler loop, one scheduler | 3–10 runs/s (26–42 runs/s without the asset machinery) |
| mapped task instances per `expand()` | one blocking transaction | 10k → 60 s scheduler pause, 100k → 317 s (threshold 30 s) |
| mapped input | every mapped TI pulls the whole list | O(N²) bytes; fine at 1k, 150 GB per run at 100k |
| metadata growth | `dag_run` ≈ 0.8 KB, `task_instance` ≈ 1 KB per row | ~65 GB/year at 100k runs/day without retention; APDR not cleanable |
| `max_active_runs` | DAG-wide, default 16 | 100k queued runs drain at ≤16 concurrent |
| DAG count / workers / schedulers on MWAA | environment class | ≤ 4,000 DAGs (mw1.2xlarge), 2–5 schedulers, ≤ 25 (50) workers |
| what a 2nd scheduler buys | measured with two schedulers on one box | partition-run creation and completion ≈ 2× faster (`SKIP LOCKED` splits the work); a large `expand()` gets *slower* (contention) but only blocks the scheduler that owns the run |

At 10k/day with emitters serialized and ≤ 900 keys each, an APDR index (self-hosted only), `max_active_runs` raised and
retention in place, the partition path works. **With the index in place and two schedulers, 100k account-level partition runs
were created and finished in 31 minutes on a laptop** (report §4.2a) — so 100k/day is within reach once the index/cleanup PR
ships in a release the platform offers; until then it is a self-hosted-only option.

## Q6. Are we modelling this wrong?

1. **Coarser scheduling grain + per-account status off the run graph: yes** — but "off-graph" should mean the asset state
   store and per-account partition keys emitted in bulk, both inside Airflow, not an external system. That also covers the
   empty-account edge case (status is written by orchestration, not inferred from output data).
2. **One-node `@asset` DAGs are the intended granularity** in Airflow 3 (AIP-75: one asset function = one DAG), not a smell.
   For a gate → batch → emit pipeline a normal multi-task DAG is the natural fit; it can declare outlets and emit partition
   keys exactly like `@asset`. Do not avoid assets: they are the only source of lineage, push-based triggering (your matrix's
   "polling" column) and the partition machinery. Avoid *account-grain scheduling*, not assets.

---

## How this maps onto the "Proposal" page (Jobs + Datasets + Trigger Conditions)

The proposal's MVP — *Jobs and Datasets as first-class objects, statically wired with trigger conditions, declared next to the
business logic in GitLab, publishing data events, with lineage* — is, feature for feature, the Airflow 3 asset model: DAG =
Job, `Asset` = Dataset, `schedule=(a & (b | c))` / `PartitionedAssetTimetable` = trigger condition, `outlet_events` = data
event, `consumed_asset_events` / `upstreamAssetEvents` = lineage, and the 3.3 asset state store = "standardised state tracking"
(requirement 6.2). Building a bespoke equivalent on the in-house job runner works against requirement 6.3 ("eliminate bespoke components with
questionable ownership") unless Airflow demonstrably cannot do the job. What Airflow demonstrably cannot do, per this repo, is
**schedule at account grain at 100k/day** — and the proposal's own narrative already separates the three grains: "job
scheduling, data partitioning and observability … do not require the same grain as each other; the scheduler could schedule by
region … as long as the end-user can still observe availability by account". That split is exactly what §6 recommends and E4
demonstrates on 3.3.2: schedule by region (3) or pack (~500), partition and observe by account via keyed events and the state
store. On the SLA footnote (p95 38 min → 2 min): that is calculation time, not orchestration; at pack grain Airflow's own
overhead per unit is seconds, so faster engines and coarse-grained scheduling are complementary, not alternatives.

Where the proposal's Airflow caveat is right: "AIP-73 still maturing as of July 2026" — the partition features are one to two
minor releases old, the per-key concurrency, conflation, retention and index gaps in this document are real, and MWAA lags by
a month. Where it is out of date: AIP-76 is complete (3.3.0), the Dagster comparison is closer than it was, and the extension
points needed for the remaining gaps exist (`asset_manager_class`, listeners, state store).

## The evaluation matrix, filled in

| | 1. reruns | 2. observability | 3. polling | 4. scaling | 5. spark batching |
|---|---|---|---|---|---|
| **A classic DAGs (sensors)** | as the page says: DAG-level concurrency, whole-DAG reruns | per-task status only; no lineage; cross-date needs XCom/DB | reschedule-mode sensors poll the data platform; `up_for_reschedule` churn = scheduler load | account-level tasks: no (§4.1/§4.4); region/pack tasks: fine | no native batching; a "pack" task *is* the batch |
| **B assets (3.3)** | OR schedule + version-aware gate (Q1); not native, no core change needed | asset graph + per-key events + `partition_key` on runs; per-account status via state store | push-based, no polling | netting-group/shard grain: yes; account grain: 10k with care, 100k no (Q5) | **yes**: batcher DAG + `add_partitions` for per-account lineage (Q3.3) |

---

## Appendix — experiments (reproducible with `bench/exp_e1.py`, `bench/exp_e2.py`, DAGs in `bench/dags/exp_semantics.py`)

**E1 (per-key concurrency).** Output of `exp_e1.py` on 3.3.2, consumer `max_active_runs=2`, 45 s task:

```
step 1: after first ACC1 event      APDR: ACC1*            runs: ACC1:running
step 2: ACC1 again, #1 running      APDR: ACC1*, ACC1*     runs: ACC1:running, ACC1:running
step 3: ACC1+ACC5, 2 running        APDR: +ACC1*, ACC5*    runs: …, ACC1:queued, ACC5:queued
step 4: ACC1 while ACC1 queued      APDR: +ACC1*           runs: …, ACC1:queued
final                               runs per key: {'ACC1': 4, 'ACC5': 1}
```

**E2 (rerun semantics).** Events a1v1, a2v1, a1v2, a3v1, a3v2, then a burst a1 v3…v7:

```
AND consumer: 0,0,0 runs → 1 run at a3v1 → still 1 after a3v2 → still 1 after the burst
OR  consumer: 1,2,3,4,5 runs (gate skipped the first three: NOT READY) → 6 after the burst (5 events → 1 run)
calc after a3v2: {a1: v2, a2: v1, a3: v2}      calc after burst: {a1: v3 (!), a2: v1, a3: v2}
a1 event registration order in the burst: v5, v7, v6, v4, v3  → inlet_events[a1][-1] == v3
```

**E4 (the batcher, `bench/dags/exp_e4_batcher.py`, driver `bench/exp_e4.py`).** Producers emit keyed events on
`rev_positions`; `rev_batcher` (cron every minute, `max_active_runs=2`) claims ready accounts through the asset state store,
runs one simulated Spark job, and emits one `rev_pnl` event per account from that single task. Observed:

```
15:58 batch: claimed=[ACC1..ACC6]                         -> 6 rev_pnl events, source_run_id = the 15:58 batcher run
ACC1 v2 + ACC7 arrive while 15:58 runs
15:59 batch: claimed=[ACC1, ACC7], up_to_date=[ACC2..ACC6] -> 2 more events; in an earlier run: claimed=[ACC7] in_flight=[ACC1..6]
rev_pnl_consumer (PartitionedAssetTimetable): 8 runs, one per key (ACC1 twice), run_type=asset_triggered
rev_nonpart_consumer (plain asset schedule on rev_positions): 0 runs — keyed events never queue non-partitioned DAGs
asset_state_store rows: acct/ACC1 -> {"status": "done", "version": 2, "batch": "scheduled__…15:59…", …}
```

Two lessons from the first (failed) attempt: the state-store accessor is scoped to the task's declared inlets/outlets
(`asset_state_store[asset]` raises `KeyError` otherwise), and a claim needs a release path (`trigger_rule="one_failed"` task or
a lease timeout) or accounts stay "running" forever after a failed batch.

**E6 (balance-sheet readiness gate, `bench/dags/exp_e6_bs_gate.py`, driver `bench/exp_e6.py`).** Gate decisions for the
eight arrivals, verbatim:

```
as_of=2026-09-30 refs_ok=False roots_in=[]                      -> SKIP   (rates v1)
as_of=2026-09-30 refs_ok=False roots_in=['positions']           -> SKIP   (positions v1; fx missing)
as_of=2026-09-30 refs_ok=True  roots_in=['positions']           -> RUN    (fx v1)
as_of=2026-09-30 refs_ok=True  roots_in=['positions','cashflows'] -> RUN  (cashflows v1)
as_of=2026-09-30 refs_ok=True  versions rates=2 fx=1 ...        -> RUN    (rates v2)
as_of=2026-10-01 refs_ok=False roots_in=['positions']           -> SKIP   (positions for the next date)
as_of=2026-10-01 refs_ok=False                                  -> SKIP   (rates for the next date; fx missing)
as_of=2026-10-01 refs_ok=True  roots_in=['positions']           -> RUN    (fx for the next date)
```
Four result events were emitted, each with `{as_of, roots, versions}`; 8 consumer runs (one per arrival, gate skipped 4).

**E3 (mapper payload).** `AllowedKeyMapper(10_000 keys)`: serialized DAG 36,317 B; `rollup_fingerprint` 33,659 B in each of the
three APDR rows created by emitting three allowed keys; a fourth, disallowed key produced a `Log` row
`failed to map partition_key` and no run.
