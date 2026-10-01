# EXPLAIN ANALYZE for apache/airflow#73983

Before/after query plans for the two indexes on `asset_partition_dag_run`, requested in review.

- Postgres 16, a fresh database migrated with the PR branch to `90e4d18ccadf` (the revision before the PR).
- `seed.sql`: 100,000 `asset_partition_dag_run` rows for one partitioned Dag; 99,500 fired (each with its own `dag_run`), the newest 500 pending.
- `explain.py`: compiles the same ORM expressions as `AssetManager._get_or_create_apdr` and
  `SchedulerJobRunner._create_dagruns_for_partitioned_asset_dags` on apache/airflow main (8f3e8466c6), plus a 100-row `dag_run`
  delete for the `ON DELETE CASCADE` path, and runs `EXPLAIN (ANALYZE, BUFFERS)` three times each (keeps the third; DELETE and
  `FOR UPDATE` are rolled back).
- Then `airflow db migrate` to the PR head (`f954ddd21484`), `ANALYZE asset_partition_dag_run`, and `explain.py` again.

```bash
psql "$DB" -f seed.sql
python explain.py before plans_before.txt
airflow db migrate && psql "$DB" -c 'analyze asset_partition_dag_run'
python explain.py after plans_after.txt
```

| path | before | after |
|---|---|---|
| per-key lookup | Seq Scan + Sort, 935 buffers, 6.96 ms | Index Scan Backward, 4 buffers, 0.008 ms |
| scheduler pending scan | Seq Scan, 935 buffers (scan), 3.72 ms | Index Scan, 11 buffers (scan), 0.43 ms |
| delete 100 `dag_run` (FK cascade trigger) | 360 ms | 0.42 ms |
| insert 10,000 rows (write cost, median of 5) | 55 ms without the two indexes | 136 ms with them (~8 µs per row) |
