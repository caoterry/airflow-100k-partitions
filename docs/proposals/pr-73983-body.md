<!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

Add the two indexes `asset_partition_dag_run` is queried by (AIP-76 partitions; same approach as `idx_asset_event_asset_id_partition_key`, migration 0127):

- `(target_dag_id, partition_key, id)` — `AssetManager._get_or_create_apdr`, run once per emitted partition key (`WHERE partition_key = ? AND target_dag_id = ? ORDER BY id DESC LIMIT 1`).
- `(created_dag_run_id, created_at, id)` — the scheduler's pending-run scan (`WHERE created_dag_run_id IS NULL ORDER BY created_at, id`), run on every loop.

Both were sequential scans of a table that only shrinks by cascade with `dag_run` cleanup. Measured on 3.3.2 / Postgres 16 with entity-keyed partitions (100k keys, 500 per task): the task-success request for 500 keys grew from 3.6 s (empty table) to 7.0 s (64k rows) — past the 5 s Task SDK client timeout, so clients retried — and dropped to 3.4–3.9 s once an equivalent index existed; run completion sped up ~3× at the same moment. Details, harness and `EXPLAIN` captures: https://github.com/caoterry/airflow-100k-partitions.

Notes:

- Plain composite indexes rather than partial ones so the definition is identical on Postgres, MySQL and SQLite (`StringID` = 250 chars, within MySQL's key limit).
- On MySQL, InnoDB drops its implicit index for `apdr_created_dag_run_id_fkey` once the composite exists, so the constraint is dropped and recreated around the index changes (as in migration 0086).
- Verified locally on SQLite (ORM-vs-migrations and migration-pattern tests, migrate → downgrade → migrate); MySQL is left to the CI migration job. Newsfragment: `73983.improvement.rst`.

related: #71072

---

##### Was generative AI tooling used to co-author this PR?

- [X] Yes (Claude Code — Claude Fable 5.1; verified locally by the author)

Generated-by: Claude Code following [the guidelines](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#gen-ai-assisted-contributions)

---

* Read the **[Pull Request Guidelines](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#pull-request-guidelines)** for more information. Note: commit author/co-author name and email in commits become permanently public when merged.
* For fundamental code changes, an Airflow Improvement Proposal ([AIP](https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvement+Proposals)) is needed.
* When adding dependency, check compliance with the [ASF 3rd Party License Policy](https://www.apache.org/legal/resolved.html#category-x).
* For significant user-facing changes create newsfragment: `{pr_number}.significant.rst`, in [airflow-core/newsfragments](https://github.com/apache/airflow/tree/main/airflow-core/newsfragments). You can add this file in a follow-up commit after the PR is created so you know the PR number.
