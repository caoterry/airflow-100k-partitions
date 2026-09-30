#!/usr/bin/env python
"""Patch A — backport of apache/airflow#69565 "Speed up dynamic task mapping expansion" (merged 2026-07-14, milestone
3.3.1 but absent from the 3.3.1/3.3.2 wheels) onto an installed apache-airflow-core 3.3.2.

What it changes: mapped TaskInstances created during expansion are `session.add()`-ed and flushed ONCE, instead of
`session.merge()`-ed one by one. `merge()` issues a SELECT per TI and, through autoflush, forces a single-row INSERT
per TI; `add()` lets SQLAlchemy batch the INSERTs (insertmanyvalues) at one flush.

Usage: python patches/patch_a_expansion_no_merge.py [--revert]   (edits site-packages in place, keeps .orig backups)
"""
from __future__ import annotations
import argparse, pathlib, shutil, sys
import airflow
ROOT = pathlib.Path(airflow.__file__).parent

HELPER = '''
def _add_and_prime_mapped_ti(
    ti: TaskInstance,
    task: Operator,
    dag_run: DagRun,
    *,
    session: Session,
    context_carrier: dict | None = None,
) -> None:
    """
    Attach a newly-created mapped TI to the session and prime its ``dag_run`` cache.

    Backported from apache/airflow#69565 (Patch A of the 100k-partition experiment).
    :meta private:
    """
    from sqlalchemy.orm.attributes import set_committed_value

    task_instance_mutation_hook(ti, dag_run=dag_run)
    session.add(ti)
    if context_carrier is not None:
        ti.context_carrier = context_carrier
    ti.refresh_from_task(task, dag_run=dag_run)
    set_committed_value(ti, "dag_run", dag_run)


def _recalculate_dagrun_queued_at_deadlines('''

EDITS = {
    "models/taskinstance.py": [
        ("\ndef _recalculate_dagrun_queued_at_deadlines(", HELPER),
    ],
    "models/taskmap.py": [
        ("        from airflow.models.taskinstance import TaskInstance\n",
         "        from airflow.models.taskinstance import TaskInstance, _add_and_prime_mapped_ti\n"),
        ("""        for index in indexes_to_map:
            # TODO: Make more efficient with bulk_insert_mappings/bulk_save_mappings.
            ti = TaskInstance(
                task,
                run_id=run_id,
                map_index=index,
                state=state,
                dag_version_id=dag_version_id,
            )
            task.log.debug("Expanding TIs upserted %s", ti)
            task_instance_mutation_hook(ti, dag_run=dr)
            ti = session.merge(ti)
            ti.context_carrier = new_task_run_carrier(dr.context_carrier)
            ti.refresh_from_task(task, dag_run=dr)  # session.merge() loses task information.
            all_expanded_tis.append(ti)
""",
         """        new_tis: list[TaskInstance] = []
        for index in indexes_to_map:
            ti = TaskInstance(
                task,
                run_id=run_id,
                map_index=index,
                state=state,
                dag_version_id=dag_version_id,
            )
            task.log.debug("Expanding TIs upserted %s", ti)
            _add_and_prime_mapped_ti(
                ti, task, dr, session=session, context_carrier=new_task_run_carrier(dr.context_carrier)
            )
            new_tis.append(ti)
        if new_tis:
            session.flush()
        all_expanded_tis.extend(new_tis)
"""),
    ],
    "models/dagrun.py": [
        ("""    def _revise_map_indexes_if_mapped(
        self, task: Operator, *, dag_version_id: UUID | None, session: Session
    ) -> Iterator[TI]:
""",
         """    def _revise_map_indexes_if_mapped(
        self, task: Operator, *, dag_version_id: UUID | None, session: Session
    ) -> list[TI]:
"""),
        ("            return  # Not a mapped task, don't need to do anything.\n",
         "            return []  # Not a mapped task, don't need to do anything.\n"),
        ("            return  # Upstreams not ready, don't need to revise this yet.\n",
         "            return []  # Upstreams not ready, don't need to revise this yet.\n"),
        ("from airflow.models.taskinstance import TaskInstance as TI, clear_task_instances\n",
         "from airflow.models.taskinstance import TaskInstance as TI, _add_and_prime_mapped_ti, clear_task_instances\n"),
        ("""            ti = TI(task, run_id=self.run_id, map_index=index, state=None, dag_version_id=dag_version_id)
            self.log.debug("Expanding TIs upserted %s", ti)
            task_instance_mutation_hook(ti, dag_run=self)
            ti = session.merge(ti)
            ti.refresh_from_task(task, dag_run=self)
            session.flush()
            yield ti
""",
         """            ti = TI(task, run_id=self.run_id, map_index=index, state=None, dag_version_id=dag_version_id)
            self.log.debug("Expanding TIs upserted %s", ti)
            _add_and_prime_mapped_ti(ti, task, self, session=session)
            new_tis.append(ti)
        if new_tis:
            session.flush()
        return new_tis
"""),
        ("""        for index in range(total_length):
            if index in existing_indexes:
                continue
            ti = TI(task, run_id=self.run_id, map_index=index, state=None, dag_version_id=dag_version_id)
""",
         """        new_tis: list[TI] = []
        for index in range(total_length):
            if index in existing_indexes:
                continue
            ti = TI(task, run_id=self.run_id, map_index=index, state=None, dag_version_id=dag_version_id)
"""),
    ],
}


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--revert", action="store_true"); a = ap.parse_args()
    for rel, edits in EDITS.items():
        p = ROOT / rel; bak = p.with_suffix(p.suffix + ".orig-patchA")
        if a.revert:
            if bak.exists(): shutil.copy(bak, p); bak.unlink(); print("reverted", rel)
            continue
        src = p.read_text()
        if "_add_and_prime_mapped_ti" in src and rel != "models/taskinstance.py" or (rel == "models/taskinstance.py" and "Patch A of the 100k" in src):
            print("already patched", rel); continue
        if not bak.exists(): shutil.copy(p, bak)
        for old, new in edits:
            if old not in src:
                sys.exit(f"anchor not found in {rel}:\n{old[:120]}")
            src = src.replace(old, new, 1)
        p.write_text(src); print("patched", rel)
    if a.revert:
        return
    # Verify the patched modules import in a fresh interpreter (reloading ORM modules in-process is not possible).
    import subprocess
    subprocess.run([sys.executable, "-c", "import airflow.models.taskinstance, airflow.models.taskmap, airflow.models.dagrun; "
                    "from airflow.models.taskinstance import _add_and_prime_mapped_ti; print('import check ok')"], check=True)


if __name__ == "__main__":
    main()
