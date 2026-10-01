# Summary: can Airflow 3.x carry 100k firm-account partitions?

**At a glance**

- **Asked:** can Airflow (MWAA 3.3.1 or self-hosted) orchestrate revenue for 10k–100k firm accounts per business date, with late-input reruns, batching to engine capacity and per-account status.
- **Done:** two days of measured experiments on Airflow 3.3.2 (six DAG shapes at 1k–100k, four of them run at 100k, semantics experiments E1–E6, a batching simulator, source traces, the six evaluation questions answered); everything in this repo re-runs.
- **Breaks:** 100k task instances in one DagRun (5-minute scheduler stall) and one run per account at 100k (hours on one scheduler; 31 min only with indexes MWAA does not let us add).
- **Works on MWAA as shipped:** account-grain events into a leaky bucket (a small journal table we own) drained in capacity-sized batches, and an OR trigger plus a version gate for balance-sheet reruns; no upstream change needed, and as long as no Dag subscribes to the keyed events per partition it writes no rows to `asset_partition_dag_run`, the unindexed table the open index PR targets.
- **Upstream:** one PR open for the missing indexes (apache/airflow#73983); it matters only if account-grain runs are ever wanted.

Two days of evaluation (2026-09-29 and 2026-09-30), measured locally on Airflow 3.3.2 with `apache/airflow` main used for
source study. MWAA ships 3.3.1, one patch release earlier; the partition code paths traced here are the same in both. Every
number below was produced by code in this repository and can be re-run. Nothing here has been run on MWAA itself; the MWAA
constraints quoted are from the AWS documentation.

## The question

A shared balance-sheet / revenue platform on Airflow or AWS MWAA. Revenue is keyed by firm account: ~10k accounts per
business date, ~100k in the worst case. It needs late-input reruns (v2 of one input arriving after all inputs arrived),
per-partition concurrency and batching, per-account status for end users, and it must run on the firm's platform
(MWAA 3.3.1, or self-hosted Airflow).

## What was done

- A benchmark harness and six DAG shapes at 1k / 10k / 30k / 100k units (four of them run at 100k): flat dynamic mapping, batched mapping, two-level
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
| Same, with the index from #73983 and two schedulers | all 100k runs created and finished in 31 min (laptop, trivial tasks; self-hosted only until the index ships in an MWAA image) | REPORT §4.2a |
| Capacity-aware batcher at 100k accounts (K = 10 engine slots) | 84k accounts published in 36 min; limited by the one-minute cadence, not capacity; the floor is ~8 min of key registration | dynamic-batching §5a |
| Batching policy (simulator, warm Spark, 2-min SLA) | SLA-driven batching meets the SLA at 4.9 job-hours; latency-optimal batching also meets it but at 11.6 job-hours | dynamic-batching §2 |
| Reruns on a late v2 input | an OR trigger plus a version-aware gate does it with no core change; the balance-sheet gate (all reference inputs and any root input, per business date) runs stateless | answers Q1; E2, E6 |
| End-user tracking through the REST API | not advisable: 10 requests/s throttle on MWAA, on the same server that runs the task execution API | answers Q4 |
| Storage growth with N | linear, not n² | REPORT §4.3 |

## What is proposed

**Trigger at account granularity, execute at batch granularity, and put a leaky bucket between the two.** Native Airflow runs
work at the grain it is triggered at, and one run per account at 100k a day costs hours of scheduler time; separating the two
grains is what makes the account grain affordable. The full explanation, including a comparison with doing the same on Kafka,
is in [docs/design-core.md](docs/design-core.md); the in-depth version with real table rows, the overlap scenario row by
row and diagrams is [docs/two-grains-one-bucket.md](docs/two-grains-one-bucket.md).

- Producers emit one keyed asset event per changed account (`add_partitions`), with the version in `extra`.
- The bucket is a journal table with one row per account (pending / in flight / done, latest version only), so versions of an
  account that arrive while it waits collapse into one recompute. (The prototype mishandles a version that arrives while the
  account is in flight; the fix, recording the claimed version, is listed for the next iteration.)
- A dispatcher (the batcher DAG; today a one-minute cron, with a continuously looping version as the next step) claims pending
  accounts atomically and splits them across the free engine slots, using a size target derived from the SLA and the arrival
  rate and a hard cap per batch; an Airflow pool caps running jobs at the engine's capacity. About 170 batch jobs of 600 carry
  100,000 accounts.
- Balance sheet aggregates across accounts, so it uses no bucket: an OR trigger plus a gate that recomputes the whole business
  date with the latest version of each input once all reference inputs and at least one root input are in.
- End-user status is meant to be a view on the journal; it is not built yet and needs a failed state with timestamps. The
  Airflow UI stays for operators.

This shape needs nothing beyond what MWAA 3.3.1 ships and does not depend on any upstream change; it has been exercised locally,
not yet on MWAA. Its one stateful piece is the batcher's ledger (pending / in-flight / processed), which has to live in a locked
table outside the metadata database (RDS or DynamoDB on MWAA). One run per account at 100k per day is a later step, once the
upstream fixes (index, retention, bulk expansion, batched key registration, per-key concurrency) reach a release the platform offers.

The proposal's MVP (Jobs and Datasets as first-class objects, wired by trigger conditions, declared next to the business code,
with lineage and standardised state tracking) maps closely onto the Airflow 3 asset model: DAG = Job, Asset = Dataset,
schedule expression = Trigger Condition (conditions beyond AND/OR live in a small gate task), outlet events = data events, the journal
= standardised state tracking. The main thing Airflow cannot do today is schedule at account grain at 100k per day;
per-key concurrency control and conflation are also not native (the batcher's ledger provides them in the proposed shape). The
proposal's own narrative already separates scheduling grain from partition and observability grain, and ties coarse grain to
fast calculations; the batcher makes the grain a runtime variable, so that condition becomes a question about engine start-up
time rather than about the scheduler.

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
