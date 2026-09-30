-- Missing index on the partitioned-asset write path (Airflow 3.2.0 – 3.3.2; main has none either as of 2026-09-30).
-- AssetManager._get_or_create_apdr looks up
--   WHERE partition_key = ? AND target_dag_id = ? ORDER BY id DESC LIMIT 1
-- for every emitted key; without this index that is a sequential scan of a table that is never pruned.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_apdr_target_dag_partition_key
    ON asset_partition_dag_run (target_dag_id, partition_key, id DESC);
-- Scheduler drain query: WHERE created_dag_run_id IS NULL ORDER BY created_at, id LIMIT 500
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_apdr_pending
    ON asset_partition_dag_run (created_at, id) WHERE created_dag_run_id IS NULL;
-- Per-partition status lookups on dag_run (GET /dagRuns?partition_key_pattern=…, clearPartitions)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dag_run_dag_id_partition_key
    ON dag_run (dag_id, partition_key);
