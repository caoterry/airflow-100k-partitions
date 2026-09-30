# PR draft: Add indexes on `asset_partition_dag_run` and purge fired partition runs in `db clean`

*Branch: `caoterry/airflow` → `apdr-indexes-and-cleanup` (based on apache/airflow main e364ee7648). Terry opens the PR;
this file is the description to paste. Newsfragment `<PR>.improvement.rst` is added in a follow-up commit once the number exists.*

---

**What**

- New migration `0141_3_4_0_add_indexes_on_asset_partition_dag_run.py` (revision `f954ddd21484`, revises `e5a91c7f42b3`) adding
  two indexes on `asset_partition_dag_run`:
  - `idx_apdr_target_dag_id_partition_key_id (target_dag_id, partition_key, id)` — the lookup
    `AssetManager._get_or_create_apdr` runs for every emitted partition key
    (`WHERE partition_key = ? AND target_dag_id = ? ORDER BY id DESC LIMIT 1`).
  - `idx_apdr_created_dag_run_id_created_at_id (created_dag_run_id, created_at, id)` — the scheduler's pending scan in
    `_create_dagruns_for_partitioned_asset_dags` (`WHERE created_dag_run_id IS NULL ORDER BY created_at, id`).
  Matching `Index()` entries in `AssetPartitionDagRun.__table_args__`; `_REVISION_HEADS_MAP["3.4.0"]` and `migrations-ref.rst` updated.
- `airflow db clean` learns `asset_partition_dag_run`: rows whose Dag run has been created (`created_dag_run_id IS NOT NULL`)
  are purged by `created_at` age; pending rows (`created_dag_run_id IS NULL`) are scheduler state and are never purged.
  The existing `partitioned_asset_key_log` filter is widened accordingly: key-log rows are kept only while their partition run
  is still pending (previously: while it existed at all).
- Tests: `TestAssetPartitionDagRunCleanup` (old fired → purged; old pending and recent fired → kept), an "old but fired" case
  in `TestPartitionedAssetKeyLogCleanup`, and `asset_partition_dag_run` removed from the model-coverage exclusion list.
- Docstring of `AssetPartitionDagRun` no longer claims rows are never deleted.

**Why**

Both queries were sequential scans on a table that grows by one row per partition key per consumer per event and is never
pruned. Measured on Airflow 3.3.2 / Postgres 16 while emitting 100k keys from 200 tasks of 500 keys each: the task-success
request that registers 500 keys took 3.6 s with an empty table and 7.0 s at ~60k rows (`EXPLAIN`: 1,843 shared buffers per
key lookup), i.e. past the default 5 s `[workers] execution_api_timeout`, so clients started retrying; creating the composite
index online brought it back to 3.4 s (4 buffers per lookup) and the retries stopped. At 100k keys per business day the
table reaches ~36 M rows in a year with no way to clean it.

Reproduction and numbers: https://github.com/caoterry/airflow-100k-partitions (REPORT.md §4.2–4.3, `docs/charts.md` §2).

**Notes for reviewers**

- Plain composite indexes rather than a partial index on `created_dag_run_id IS NULL`, so the definition is identical on
  Postgres, MySQL and SQLite (precedent: `idx_asset_event_asset_id_partition_key`, migration 0127). Both indexed
  string columns are `StringID` (250), within MySQL's key-length limit.
- `db clean` never touches pending rows; the age filter applies only to fired rows, whose evidence rows in
  `partitioned_asset_key_log` are no longer needed by the scheduler.
- Generative AI tooling (Claude) was used to draft the change and tests; all of it was run locally against SQLite and the
  measurements above were taken on Postgres.

^ Add meaningful description above
Read the **[Pull Request Guidelines](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#pull-request-guidelines)** for more information.
- [x] I have added tests
- [x] Generative AI tooling was used: Claude Code (Fable 5.1)

Generated-by: Claude Code
