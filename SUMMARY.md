# Summary: can Airflow 3.x carry 100k firm-account partitions?

Two days of measured evaluation (2026-09-29 and 2026-09-30) on Airflow 3.3.2, the release behind MWAA 3.3.1, with
`apache/airflow` main used for source study. Every number below was produced by code in this repository and can be re-run.

## The question

A shared balance-sheet / revenue platform on Airflow or AWS MWAA. Revenue is keyed by firm account: ~10k accounts per
business date, ~100k in the worst case. It needs late-input reruns (v2 of one input arriving after all inputs arrived),
per-partition concurrency and batching, per-account status for end users, and it must run on the firm's platform
(MWAA 3.3.1, or self-hosted Airflow).

## What was done

- A benchmark harness and six DAG shapes at 1k / 10k / 30k / 100k units: flat dynamic mapping, batched mapping, two-level
  DAG-of-DAGs, one run per account, and native AIP-76 partitions; plus 100k limit runs with one and two schedulers
  (`bench/`, results under `bench/results/`).
- Semantics experiments E1 to E6: per-key concurrency, AND/OR rerun rules, mapper payloads, a batcher with per-account lineage,
  capacity-aware dynamic batching with a locked Postgres ledger, and a balance-sheet readiness gate (`bench/exp_e*.py`, `bench/dags/exp_*.py`).
- A batching simulator comparing five dispatch policies under Glue-class and warm-Spark engine profiles
  (`bench/sim_batching.py`, write-up in [docs/dynamic-batching.md](docs/dynamic-batching.md)).
- Source traces of 3.3.2 vs main for the expansion and partition write paths, and a fact-checked sweep of MWAA limits, AIPs
  and Dagster ([docs/analysis/](docs/analysis/), [docs/research/](docs/research/)).
- The six evaluation questions answered one by one, the evaluation matrix filled in, and a mapping onto the
  Jobs + Datasets + Trigger Conditions proposal ([docs/answers-to-mwaa-discussion.md](docs/answers-to-mwaa-discussion.md)).
- One upstream fix opened: [apache/airflow#73983](https://github.com/apache/airflow/pull/73983), two indexes on the partition write path.

## What was found

| finding | measured | where |
|---|---|---|
| 100k mapped tasks inside one DagRun is disqualified, not merely slow | the expansion stalls the scheduler for about 5 minutes (317 s; health threshold 30 s); every mapped task downloads the whole input, O(N²) bytes, ~150 GB per run | REPORT §4.1, §4.4 |
| Batching the same 100k accounts | 100 batches × 1,000 finish in 38 s | REPORT §4.5 |
| Native partitions, write path | registering 500 keys took 3.6 s on an empty table and 7.0 s at 64k rows (sequential scan on `asset_partition_dag_run`); ~5 ms per key under a row lock, so at most ~900 keys per task against the 5 s SDK timeout | REPORT §4.2, §4.3 |
| Native partitions, one run per account, 100k, as shipped | 3.7k runs finished after 26 min, about 3 h projected | REPORT §4.2 |
| Same, with the index from #73983 and two schedulers | all 100k runs created and finished in 31 min | REPORT §4.2a |
| Capacity-aware batcher at 100k accounts (K = 10 engine slots) | 84k accounts published in 36 min; limited by the one-minute cadence, not capacity; the floor is ~8 min of key registration | dynamic-batching §5a |
| Batching policy (simulator, warm Spark, 2-min SLA) | SLA-driven batching meets the SLA at 4.9 job-hours; latency-optimal batching also meets it but at 11.6 job-hours | dynamic-batching §2 |
| Reruns on a late v2 input | an OR trigger plus a version-aware gate does it with no core change; the balance-sheet gate (all reference inputs and any root input, per business date) runs stateless | answers Q1; E2, E6 |
| End-user tracking through the REST API | not advisable: 10 requests/s throttle on MWAA, on the same server that runs the task execution API | answers Q4 |
| Storage growth with N | linear, not n² | REPORT §4.3 |

## What is proposed

Schedule at region / pack / shard grain; partition and observe at account grain.

- Runs are per shard (or pack, or region). Firm accounts are partition keys emitted in bulk with `add_partitions`; per-account
  outcome and watermark live in the 3.3 asset state store.
- Compute is dispatched by a capacity-aware batcher DAG: pool size = engine capacity, batch size driven by the SLA; per-account
  lineage is kept through keyed asset events.
- Reruns: OR trigger with a version-aware gate. Balance-sheet readiness: a stateless gate per business date.
- End-user status is projected out (DagRun listeners or the batcher's own state writes) into a status table; the Airflow UI
  stays for operators.

This shape runs on MWAA 3.3.1 as shipped and does not depend on any upstream change. One run per account at 100k per day is a
later step, once the upstream fixes (index, retention, bulk expansion, batched key registration) reach a release the platform offers.

The proposal's MVP (Jobs and Datasets as first-class objects, wired by trigger conditions, declared next to the business code,
with lineage and standardised state tracking) maps one-to-one onto the Airflow 3 asset model: DAG = Job, Asset = Dataset,
schedule expression = Trigger Condition, outlet events = data events, asset state store = standardised state tracking. The one
thing Airflow demonstrably cannot do today is schedule at account grain at 100k per day, and the proposal's own narrative already
separates scheduling grain from partition and observability grain.

## Platform

| option | runs today | index and upstream fixes | cost |
|---|---|---|---|
| MWAA 3.3.1 as shipped | the proposed shape | not needed | none |
| MWAA, wait for images | the proposed shape now; one run per account later | 3 to 4 weeks after an Apache release that contains them (3.4.0 freeze 2026-10-12, final planned 2026-11-02) | waiting |
| self-hosted on Kubernetes | one run per account now (100k in 31 min) | one SQL statement; code patches possible | the team operates Airflow, the database and upgrades |

Details: answers doc, "Platform options" under Q5; REPORT §8.

## Where to read what

- Findings and numbers: [REPORT.md](REPORT.md) (§1 executive summary, §4 experiments, §6 recommendation, §7 upstream list, §8 platform constraints).
- The six questions, the evaluation matrix, the mapping onto the proposal: [docs/answers-to-mwaa-discussion.md](docs/answers-to-mwaa-discussion.md).
- Batching design, simulator and prototype: [docs/dynamic-batching.md](docs/dynamic-batching.md).
- Source-level traces: [docs/analysis/](docs/analysis/).
- Every chart rendered as text, for image-blocked networks: [docs/charts.md](docs/charts.md).
- Reproduce: the quick start in [README.md](README.md); each folder under `bench/results/` records the command that produced it.

## Upstream work

- [apache/airflow#73983](https://github.com/apache/airflow/pull/73983) (open): indexes on `asset_partition_dag_run`.
- Draft proposal for `max_active_runs_per_partition_key` in [docs/proposals/](docs/proposals/), to be raised on the dev list after the 3.4.0 freeze.
