#!/usr/bin/env python
"""Patch C — request-scoped memoisation in the partitioned-asset write path (apache-airflow-core 3.3.2).

`AssetManager._queue_partitioned_dags` is called once per emitted partition key. For every key and every consumer
DAG it re-reads and re-deserialises the consumer's SerializedDagModel, recomputes the rollup fingerprint, re-selects
the AssetModel and rebuilds the partition mapper. None of that changes within one Execution-API request, so this
patch caches (timetable, fingerprint, asset_model, mapper) per (target_dag_id, asset_id) on `session.info`, which
lives exactly as long as the request's session.

It does NOT change the per-key APDR lookup / insert / PAKL insert, nor the asset row lock; pair it with the
`asset_partition_dag_run (target_dag_id, partition_key, id)` index (see patches/apdr_index.sql).

Usage: python patches/patch_c_partition_write_path_cache.py [--revert]
"""
from __future__ import annotations
import argparse, pathlib, shutil, sys
import airflow
ROOT = pathlib.Path(airflow.__file__).parent

OLD = '''            from airflow.models.serialized_dag import SerializedDagModel

            if not (serdag := SerializedDagModel.get(dag_id=target_dag.dag_id, session=session)):
                raise RuntimeError(f"Could not find serialized dag for dag_id={target_dag.dag_id}")

            timetable = serdag.dag.timetable
            if TYPE_CHECKING:
                assert isinstance(timetable, PartitionedAssetTimetable)

            fingerprint = compute_rollup_fingerprint(timetable)

            if (asset_model := session.scalar(select(AssetModel).where(AssetModel.id == asset_id))) is None:
                raise RuntimeError(f"Could not find asset for asset_id={asset_id}")

            mapper = timetable.get_partition_mapper(name=asset_model.name, uri=asset_model.uri)
'''
NEW = '''            from airflow.models.serialized_dag import SerializedDagModel

            # Patch C (100k-partition experiment): memoise per-request. Everything below is a pure function of
            # (consumer dag, asset) and is otherwise recomputed for every emitted key.
            _cache = session.info.setdefault("_bench_partition_cache", {})
            _ck = (target_dag.dag_id, asset_id)
            if _ck in _cache:
                timetable, fingerprint, asset_model, mapper = _cache[_ck]
            else:
                if not (serdag := SerializedDagModel.get(dag_id=target_dag.dag_id, session=session)):
                    raise RuntimeError(f"Could not find serialized dag for dag_id={target_dag.dag_id}")

                timetable = serdag.dag.timetable
                if TYPE_CHECKING:
                    assert isinstance(timetable, PartitionedAssetTimetable)

                fingerprint = compute_rollup_fingerprint(timetable)

                if (asset_model := session.scalar(select(AssetModel).where(AssetModel.id == asset_id))) is None:
                    raise RuntimeError(f"Could not find asset for asset_id={asset_id}")

                mapper = timetable.get_partition_mapper(name=asset_model.name, uri=asset_model.uri)
                _cache[_ck] = (timetable, fingerprint, asset_model, mapper)
'''

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--revert", action="store_true"); a = ap.parse_args()
    p = ROOT / "assets/manager.py"; bak = p.with_suffix(".py.orig-patchC")
    if a.revert:
        if bak.exists(): shutil.copy(bak, p); bak.unlink(); print("reverted assets/manager.py")
        return
    src = p.read_text()
    if "_bench_partition_cache" in src: print("already patched"); return
    if OLD not in src: sys.exit("anchor not found in assets/manager.py")
    if not bak.exists(): shutil.copy(p, bak)
    p.write_text(src.replace(OLD, NEW, 1)); print("patched assets/manager.py")
    import airflow.assets.manager  # noqa: F401  import check
    print("import check ok")

if __name__ == "__main__":
    main()
