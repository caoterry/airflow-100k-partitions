#!/usr/bin/env python
"""Patch E — per-partition-key concurrency control for partitioned asset scheduling (apache-airflow-core 3.3.2).

Prototype of what an upstream `max_active_runs_per_partition_key = 1` would do. Two small changes:

1. scheduler (`_create_dagruns_for_partitioned_asset_dags`): a provisional partition run (APDR) whose key already has a
   QUEUED or RUNNING DagRun in the same DAG is *held* (stays pending) instead of being turned into a second concurrent run.
   -> per-key mutex; the held APDR keeps absorbing later events for the key (existing reuse-pending behaviour).
2. asset manager (`_get_or_create_apdr`): a new event for a key whose latest APDR became a DagRun that is still QUEUED
   (not started) re-uses that APDR instead of creating another one.
   -> conflation: events that arrive before the run starts collapse into it.

Together: at most one running + one pending run per key; the evaluation page's T=1..5 table becomes "conflated + fine-grained admission".
Controlled by AIRFLOW__SCHEDULER__PARTITION_KEY_MUTEX (default True once patched). Usage: patch_e_...py [--revert]
"""
from __future__ import annotations
import argparse, pathlib, shutil, subprocess, sys
import airflow
ROOT = pathlib.Path(airflow.__file__).parent

SCHED_OLD = '''            if not evaluator.run(timetable.asset_condition, statuses=statuses):
                continue

            partition_dag_ids.add(apdr.target_dag_id)
            run_after = timezone.utcnow()
'''
SCHED_NEW = '''            if not evaluator.run(timetable.asset_condition, statuses=statuses):
                continue

            # Patch E (100k-partition experiment): per-partition-key mutex. If a run for this key is already
            # QUEUED/RUNNING in this Dag, keep the provisional run pending; it will fire once that run finishes and
            # meanwhile keeps absorbing further events for the key (see AssetManager._get_or_create_apdr).
            if conf.getboolean("scheduler", "partition_key_mutex", fallback=True):
                from sqlalchemy import func as _sa_func

                _active = session.scalar(
                    select(_sa_func.count())
                    .select_from(DagRun)
                    .where(
                        DagRun.dag_id == apdr.target_dag_id,
                        DagRun.partition_key == apdr.partition_key,
                        DagRun.state.in_((DagRunState.QUEUED, DagRunState.RUNNING)),
                    )
                )
                if _active:
                    self.log.debug("Holding partition run for %s/%s: %s active", apdr.target_dag_id, apdr.partition_key, _active)
                    continue

            partition_dag_ids.add(apdr.target_dag_id)
            run_after = timezone.utcnow()
'''
MGR_OLD = '''            if latest_apdr and latest_apdr.created_dag_run_id is None:
                existing_partition_date = latest_apdr.partition_date'''
MGR_NEW = '''            if latest_apdr and latest_apdr.created_dag_run_id is not None:
                # Patch E (100k-partition experiment): conflation. If the latest run for this key has not started
                # yet (QUEUED), attach this event to it instead of provisioning another run.
                from airflow.models.dagrun import DagRun as _DagRun
                from airflow.utils.state import DagRunState as _DagRunState

                _state = session.scalar(select(_DagRun.state).where(_DagRun.id == latest_apdr.created_dag_run_id))
                if _state == _DagRunState.QUEUED:
                    cls.logger().debug("Absorbing event for key %s into queued run %s", target_key, latest_apdr.created_dag_run_id)
                    return latest_apdr
            if latest_apdr and latest_apdr.created_dag_run_id is None:
                existing_partition_date = latest_apdr.partition_date'''

FILES = {"jobs/scheduler_job_runner.py": (SCHED_OLD, SCHED_NEW), "assets/manager.py": (MGR_OLD, MGR_NEW)}

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--revert", action="store_true"); a = ap.parse_args()
    for rel, (old, new) in FILES.items():
        p = ROOT / rel; bak = p.with_suffix(".py.orig-patchE")
        if a.revert:
            if bak.exists(): shutil.copy(bak, p); bak.unlink(); print("reverted", rel)
            continue
        src = p.read_text()
        if "Patch E (100k-partition experiment)" in src: print("already patched", rel); continue
        if old not in src: sys.exit(f"anchor not found in {rel}")
        if not bak.exists(): shutil.copy(p, bak)
        p.write_text(src.replace(old, new, 1)); print("patched", rel)
    if a.revert: return
    subprocess.run([sys.executable, "-c", "import airflow.jobs.scheduler_job_runner, airflow.assets.manager; print('import check ok')"], check=True)

if __name__ == "__main__":
    main()
