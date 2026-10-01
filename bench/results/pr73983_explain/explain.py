"""EXPLAIN (ANALYZE, BUFFERS) of the APDR queries exactly as main builds them (same ORM expressions, compiled for Postgres)."""
import json, sys
from sqlalchemy import select, text
from airflow import settings
from airflow.models.asset import AssetPartitionDagRun
from airflow.models.dag import DagModel
from airflow.utils.sqlalchemy import with_row_locks

label, out = sys.argv[1], sys.argv[2]
settings.configure_orm()
session = settings.Session()
dialect = session.get_bind().dialect
def compiled(stmt): return str(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))

# assets/manager.py AssetManager._get_or_create_apdr (one lookup per emitted partition key)
lookup = (select(AssetPartitionDagRun)
          .where(AssetPartitionDagRun.partition_key == "acct_50000", AssetPartitionDagRun.target_dag_id == "consumer")
          .order_by(AssetPartitionDagRun.id.desc()).limit(1))
# jobs/scheduler_job_runner.py SchedulerJobRunner._create_dagruns_for_partitioned_asset_dags (every scheduler loop)
pending = with_row_locks(
    select(AssetPartitionDagRun)
    .join(DagModel, DagModel.dag_id == AssetPartitionDagRun.target_dag_id)
    .where(AssetPartitionDagRun.created_dag_run_id.is_(None), DagModel.is_paused.is_(False),
           DagModel.is_draining.is_(False), DagModel.is_stale.is_(False))
    .order_by(AssetPartitionDagRun.created_at, AssetPartitionDagRun.id).limit(500),
    of=AssetPartitionDagRun, skip_locked=True, key_share=False, session=session)
queries = {
    "lookup (_get_or_create_apdr)": compiled(lookup),
    "pending scan (scheduler loop)": compiled(pending),
    "dag_run delete, 100 runs (ON DELETE CASCADE into APDR)":
        "DELETE FROM dag_run WHERE id IN (SELECT id FROM dag_run WHERE dag_id = 'consumer' ORDER BY id LIMIT 100)",
}
result = {}
with open(out, "w") as f:
    for name, q in queries.items():
        for attempt in range(3):           # two warm-ups, keep the third
            plan = [r[0] for r in session.execute(text("EXPLAIN (ANALYZE, BUFFERS) " + q)).all()]
            session.rollback()             # the DELETE and FOR UPDATE are rolled back
        f.write(f"-- {label}: {name}\n-- SQL: {q}\n" + "\n".join(plan) + "\n\n")
        exe = next((l for l in plan if l.startswith("Execution Time")), "")
        trig = next((l for l in plan if "apdr_created_dag_run_id_fkey" in l), "")
        result[name] = {"top": plan[0].strip()[:150], "execution": exe, "fk_trigger": trig,
                        "nodes": [l.strip()[:110] for l in plan if ("Scan" in l or "Sort" in l) and "->" in l or l.startswith(("Limit", "Seq", "Index", "Sort"))][:6]}
print(json.dumps(result, indent=1))
