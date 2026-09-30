<!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

Part of making AIP-76 asset partitions usable at high key cardinality (entity-keyed partitions; tens of thousands of keys per business day in our case). The fix is a no-behaviour-change index addition that helps at any cardinality — both queries scan the whole table today, once per emitted key and once per scheduler loop.
Same plain-composite approach as `idx_asset_event_asset_id_partition_key` (migration 0127) for the neighbouring table;
migration shape follows the index-only batch migrations 0129/0130, with the MySQL foreign-key handling of 0086.

related: #71072

**What**

Two indexes on `asset_partition_dag_run`, matching the two queries the partitioned-asset scheduling path runs on it:

- `idx_apdr_target_dag_id_partition_key_id (target_dag_id, partition_key, id)` — `AssetManager._get_or_create_apdr` runs
  `WHERE partition_key = ? AND target_dag_id = ? ORDER BY id DESC LIMIT 1` for **every emitted partition key**, inside the
  task-success request and under the asset row lock.
- `idx_apdr_created_dag_run_id_created_at_id (created_dag_run_id, created_at, id)` — `SchedulerJobRunner._create_dagruns_for_partitioned_asset_dags`
  selects pending rows (`created_dag_run_id IS NULL`, ordered by `created_at, id`) on **every scheduler loop**. The leading
  column turns that from a full-table scan per loop into a scan of the pending rows only; the trailing columns give MySQL and
  SQLite the requested order (Postgres still sorts the pending set, which is bounded by the number of pending rows rather than
  the table).

Neither query had an index. The table grows by one row per partition key per consumer per event and shrinks only by cascade
when `dag_run` rows are cleaned (plus the scheduler's purge of stale pending rows after a mapper change).

On MySQL, `created_dag_run_id` carries `apdr_created_dag_run_id_fkey`; InnoDB's implicit index for it is silently dropped once
the new composite index exists, so dropping the composite on downgrade would fail with ER_DROP_INDEX_FK (1553). The migration
drops and recreates the constraint around the index changes, as 0086 does.

**Why / measurements**

Airflow 3.3.2, Postgres 16, one asset, one `PartitionedAssetTimetable` consumer with `IdentityMapper`, 100k keys emitted from
200 serialized tasks of 500 keys each:

- The task-success request registering 500 keys took 3.6 s with an empty table, 6.7 s at 60k rows and 7.0 s at 64k rows
  (`EXPLAIN` of the lookup at 65k rows: 1,843 shared buffers per key without an index), i.e. past the default 5 s
  `[workers] execution_api_timeout`, so the Task SDK client started retrying. Creating an equivalent index online, mid-run and with
  no other change, brought the same request to 3.4–3.9 s (4 buffers per lookup) and the retries stopped; the scheduler's
  run-completion rate roughly tripled at the same moment.
- With equivalent hand-created indexes present from the start **and a second scheduler**, all 100k partition runs were created
  and finished in 31 minutes on the same laptop. The as-shipped single-scheduler run had been stopped after 26 minutes with
  3,698 of 100k runs finished. A separate 10k control shows the second scheduler alone is worth roughly 2×, so the rest is the
  indexes.

The measured indexes were Postgres-specific equivalents (`(target_dag_id, partition_key, id DESC)` and a partial index for the
pending scan); the plain composites in this PR produce the same lookup plan on Postgres and were chosen so the definition is
identical on Postgres, MySQL and SQLite. Reproduction, harness, EXPLAIN captures and the request-duration series:
https://github.com/caoterry/airflow-100k-partitions (REPORT.md §4.2–4.3, `bench/results/part_100k_e200/`).

**Notes for reviewers**

- The `postgresql_where` / `sqlite_where` pattern of `idx_dag_run_running_dags` (`dagrun.py`) was considered; its MySQL fallback
  would be a plain `(created_at, id)` index that cannot filter `created_dag_run_id IS NULL`, so a composite leading with
  `created_dag_run_id` is the one definition that serves the scan on all three backends. Both indexed string columns are
  `StringID` (250) — within MySQL's key-length limit.
- Index-only migration (`batch_alter_table`, no table rebuild); `_REVISION_HEADS_MAP` and `migrations-ref.rst` updated;
  newsfragment `airflow-core/newsfragments/73983.improvement.rst`. Verified locally on SQLite: ORM-vs-migrations consistency and
  the migration-pattern tests (605 passed), plus a migrate → downgrade → migrate round trip; the MySQL path follows 0086 and is
  left to the CI migration job.

---

##### Was generative AI tooling used to co-author this PR?

- [X] Yes (Claude Code — Claude Fable 5.1; the change and this description were drafted with it and verified locally by the author)

Generated-by: Claude Code following [the guidelines](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#gen-ai-assisted-contributions)

---

* Read the **[Pull Request Guidelines](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#pull-request-guidelines)** for more information. Note: commit author/co-author name and email in commits become permanently public when merged.
* For fundamental code changes, an Airflow Improvement Proposal ([AIP](https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvement+Proposals)) is needed.
* When adding dependency, check compliance with the [ASF 3rd Party License Policy](https://www.apache.org/legal/resolved.html#category-x).
* For significant user-facing changes create newsfragment: `{pr_number}.significant.rst`, in [airflow-core/newsfragments](https://github.com/apache/airflow/tree/main/airflow-core/newsfragments). You can add this file in a follow-up commit after the PR is created so you know the PR number.
