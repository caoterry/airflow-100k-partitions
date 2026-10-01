# Handoff — state of the work as of 2026-09-30 ~16:10 UTC

Read this first if you are picking the work up (new session, different model, or Terry himself).

## What exists
- `REPORT.md` — the 100k-partition feasibility study (measurements, charts, recommendation, upstream list). Complete.
- `docs/answers-to-mwaa-discussion.md` — answers to the six questions on a colleague's internal evaluation page + that page's evaluation matrix + mapping to the internal
  "Proposal" page. Complete draft; evidence = E1–E4 + REPORT numbers + `docs/analysis/source-checks-mwaa-answers.md`.
- `docs/analysis/` — source traces (native partitions 3.3.2 vs main; mapped-task expansion path; source checks with skeptic verdicts).
- `docs/research/` — fact-checked literature/MWAA/Dagster sweep (36 claims checked).
- `bench/` — harness, six benchmark scenarios, experiment DAGs `exp_semantics.py` (E1–E3), `exp_e4_batcher.py` (E4), drivers
  `exp_e1.py`/`exp_e2.py`/`exp_e4.py`, result folders under `bench/results/`.
- `patches/` — Patch A (backport #69565, 1.6–1.8×), Patch C (no effect), `apdr_index.sql`.

## Environment
- Airflow 3.3.2 venv at `.venv` (site-packages **reverted to as-shipped**; re-apply with `python patches/<x>.py`).
- Postgres 16 in Docker `airflow-bench-pg` on :5433, holds all benchmark rows plus the extra indexes from `patches/apdr_index.sql`.
- Components: `source bench/env.sh && bench/ctl.sh start|stop|status`. `rev_batcher` (cron every minute) is **paused**; unpause only for E4.
- Airflow main shallow clone in `airflow-src/` (gitignored) for source reading.

## Key facts (don't re-derive)
- Expansion of N mapped TIs = one scheduler transaction: 60 s / 117 s / 317 s at 10k/30k/100k (health threshold 30 s). Patch A → 34/71/173 s.
- Each mapped TI pulls the whole upstream XCom (O(N²) bytes). Nested expand is not supported.
- Native partitions: storage linear (1k/10k/100k); write path ~5 ms/key of ~8 statements under the asset row lock, grows with the
  unindexed `asset_partition_dag_run` table (7.0→3.4 s per 500 keys after indexing); `execution_api_timeout` 5 s ⇒ ≤ ~900 keys per task;
  500 runs/tick creation; completion 3–10 runs/s (26–42 runs/s for plain run-per-account); no per-key mutex or conflation (E1).
- Non-partitioned asset scheduling: queue row per (asset, dag) ⇒ conflation per loop; `max_active_runs` gates creation; AND needs a new
  event per asset (E2). `inlet_events[a]` = full history ordered by timestamp, unbounded unless `.after()/.limit()`; `[-1]` is last
  *registered*, not highest version (E2 burst). Keyed events never queue non-partitioned DAGs.
- Mapper definitions are copied into every APDR row (`rollup_fingerprint`: 33.6 KB for a 10k-key AllowedKeyMapper) (E3).
- Extension points: `[core] asset_manager_class`, DagRun listeners (scheduler, carry partition_key), asset state store (task-scoped KV + REST).
- MWAA: 3.3.1/3.2.1/3.0.6 only, no DB access, Celery only, Exec API in webserver, 10 TPS REST throttle.

## Added 2026-09-30 afternoon
- `docs/dynamic-batching.md` + `bench/sim_batching.py` (+`sim_charts.py`): capacity-aware / SLA-driven batching policies vs
  fixed ones across engine profiles (Glue S=60 s, warm Spark S=10 s, Lambda S=1 s); key laws: burst latency = N·p/K + S,
  trickle latency floor = S; per-account jobs are unstable on Glue-class engines at ≥1 acct/s with K ≤ 50.
- E5 (`bench/dags/exp_e5_dynamic_batcher.py`, `bench/exp_e5.py`): the batcher with pool capacity and runtime batch sizing;
  adaptive_sla 64/77 s p95 vs fixed-10 92/128 s at K=3; 90/90 per-account lineage. Pool `spark_jobs` (3 slots) exists in the DB.
- Terry's two architecture points: subnet IPs bound K for Glue/EMR (pool_slots = workers+1); embarrassingly-parallel revenue
  PnL points to Lambda-class engines with Airflow scheduling at batch grain.

- E6 (`exp_e6_bs_gate.py`): balance-sheet readiness predicate all(ref)+any(root) per business date — works as OR + gate
  with max(version) per input; results in the answers doc (Q1). E5 at 10k: the claim task's unbounded `inlet_events` pull
  took 7.6–9.1 s > 5 s SDK timeout → task failed; fixed by paging (`.after().limit(2000)`) + one ledger key (rerun pending).
- `patches/patch_e_partition_key_mutex.py` run against E1: ACC1 4 → 3 runs, never concurrent, ACC5 admitted immediately (answers doc Q3.2). Reverted afterwards; site-packages is as-shipped again.
- E5 at 10k (v3): works with paged event pull + persistent ledger; ledger writer race throttles throughput (dynamic-batching.md §5a).

- HA test (2 schedulers, same laptop): partition 10k creation 143 → 67 s, completion 207 → 93 s (split 5,000/5,000 by job id);
  flat expand 30k 114 → 191 s (contention; second scheduler unaffected). Second scheduler stopped afterwards.

## Upstream branch (evening 2026-09-30)
- **RULE (2026-10-01): PR #73983 and `~/Code/airflow-fork` belong to another session — read-only from here; report issues to Terry.**
- Fork `caoterry/airflow`, branch `apdr-indexes-and-cleanup` (indexes only, amended; on top of apache/airflow main e364ee7648),
  pushed: https://github.com/caoterry/airflow/tree/apdr-indexes-and-cleanup . Local clone: `~/Code/airflow-fork` (blobless), dev env via `uv sync`.
- Contents: migration 0142 (renumbered on 2026-10-01 after #58543 took 0141) (`f954ddd21484`) with two indexes on `asset_partition_dag_run`; ORM `__table_args__`; `_REVISION_HEADS_MAP`;
  `migrations-ref.rst`; (db clean part dropped: fired rows already cascade with dag_run cleanup). Verified: test_db (30 passed),
  migration pattern tests (572 passed), SQLite migrate→downgrade→migrate round trip.
- PR description: `docs/proposals/pr-apdr-indexes-and-cleanup.md`. PR opened 2026-09-30: https://github.com/apache/airflow/pull/73983 (newsfragment `73983.improvement.rst` pushed); watch CI / reviewers. Original note: **Terry opens the PR**; then add `airflow-core/newsfragments/<PR>.improvement.rst`
  (one line: "Add indexes on ``asset_partition_dag_run`` and let ``airflow db clean`` purge partition runs whose Dag run has been created.") in a follow-up commit.
- Not run locally: `prek` hooks (needs `uv tool install prek`), `migration-round-trip`/`update-migration-references` (breeze); CI will run them.
- Dev-list proposal draft: `docs/proposals/max-active-runs-per-partition-key.md`.

## Suggested next steps (pick by available time)
1. Review `docs/answers-to-mwaa-discussion.md` with Terry; tighten wording; decide what goes back onto the internal page.
2. Learning notes: `docs/learning/` — three code walks (scheduler loop → expansion; task success → asset registration → APDR;
   partition run creation), each with the experiment that demonstrates it. Chat explanations in Chinese, notes in English.
3. Optional engineering: batched partition write path prototype (per-request bulk insert) — the one fix not yet tried; a
   `max_active_runs_per_partition_key` sketch via a custom `AssetManager` (`asset_manager_class`).
4. Optional measurements: E4 with a 90 s Spark step to show in-flight skipping live; E1 with `max_active_runs=1`.
5. ~~Before the 3.4.0 freeze (2026-10-05): dev-list post with the harness + APDR index PR.~~ Freeze is 2026-10-12 (RM dev@ post 2026-09-30); the index PR is #73983; the dev-list post is deferred until after the freeze and conditional on the platform decision (see HANDOVER 2026-09-30 addenda).

## Conventions
Chat with Terry in Chinese; everything committed is English. Commit messages carry the Claude co-author line. Never commit
`bench/results/profiles/logs-*` (100 MB) — already gitignored.
