"""Semantics experiments for the 'AWS MWAA Discussion' questions (E1-E3). Small volumes; behaviour, not throughput.

E1  per-partition-key concurrency / conflation (Q3):
    exp_e1_producer  (PartitionedAtRuntime) emits conf["keys"] on asset exp_e1_acct
    exp_e1_consumer  (PartitionedAssetTimetable, IdentityMapper, max_active_runs=conf'd in code) runs a task that sleeps 45 s.
    Sequence to test: emit ACC1 -> (run RUNNING) -> emit ACC1 again -> emit ACC1 a third time -> count APDRs / runs / states.

E2  rerun semantics (Q1): three non-partitioned assets a1,a2,a3 with a version in event extra.
    exp_e2_producer  emits one event for conf["asset"] with extra {"version": conf["version"]}
    exp_e2_consumer_and   schedule = a1 & a2 & a3   (docs semantics: every asset updated since last run)
    exp_e2_consumer_or    schedule = a1 | a2 | a3 + gate task: latest event per asset via inlet_events[a][-1]; skip until all present.

E3  mapper payload (Q2): exp_e3_consumer uses AllowedKeyMapper with 10,000 keys; we then measure serialized_dag size,
    dag.partition_mapper_info and rollup_fingerprint bytes per asset_partition_dag_run row after emitting a few keys.
"""
from __future__ import annotations

import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import AllowedKeyMapper, Asset, IdentityMapper, Param, PartitionedAssetTimetable, PartitionedAtRuntime, dag, task
from _common import account_ids

# ---------------- E1 ----------------
E1_ACCT = Asset(name="exp_e1_acct", uri="bench://exp/e1/acct")


@dag(dag_id="exp_e1_producer", schedule=PartitionedAtRuntime(), catchup=False,
     params={"keys": Param(["ACC1"], type="array")}, tags=["exp", "e1"])
def exp_e1_producer():
    @task(outlets=[E1_ACCT], do_xcom_push=False)
    def emit(params: dict | None = None, *, outlet_events=None) -> None:
        outlet_events[E1_ACCT].add_partitions(list(params["keys"]))
    emit()


@dag(dag_id="exp_e1_consumer",
     schedule=PartitionedAssetTimetable(assets=E1_ACCT, default_partition_mapper=IdentityMapper()),
     catchup=False, max_active_runs=2, tags=["exp", "e1"])
def exp_e1_consumer():
    @task(do_xcom_push=False)
    def calc(dag_run=None) -> None:
        print("partition_key =", dag_run.partition_key, "run_id =", dag_run.run_id)
        time.sleep(45)
    calc()


# ---------------- E2 ----------------
A1, A2, A3 = (Asset(name=f"exp_e2_a{i}", uri=f"bench://exp/e2/a{i}") for i in (1, 2, 3))
E2_ASSETS = {"a1": A1, "a2": A2, "a3": A3}


@dag(dag_id="exp_e2_producer", schedule=None, catchup=False,
     params={"asset": Param("a1", type="string", enum=["a1", "a2", "a3"]), "version": Param(1, type="integer")},
     tags=["exp", "e2"])
def exp_e2_producer():
    @task(outlets=[A1, A2, A3], do_xcom_push=False)
    def emit(params: dict | None = None, *, outlet_events=None) -> None:
        # Only the chosen asset gets an event: assign to outlet_events[<asset>] (the others emit nothing because we
        # remove them from the outlet list at runtime? No — declared outlets always emit). So declare per-asset tasks.
        raise RuntimeError("unused")

    # One task per asset; the branch picks which one runs so exactly one asset gets an event.
    @task.branch
    def pick(params: dict | None = None) -> str:
        return f"emit_{params['asset']}"

    def make_emitter(name: str, asset: Asset):
        @task(task_id=f"emit_{name}", outlets=[asset], do_xcom_push=False)
        def _emit(params: dict | None = None, *, outlet_events=None) -> None:
            outlet_events[asset].extra = {"version": int(params["version"]), "path": f"s3://bucket/{name}/v{params['version']}"}
        return _emit()

    pick() >> [make_emitter(n, a) for n, a in E2_ASSETS.items()]


@dag(dag_id="exp_e2_consumer_and", schedule=(A1 & A2 & A3), catchup=False, tags=["exp", "e2"])
def exp_e2_consumer_and():
    @task
    def calc(triggering_asset_events=None) -> dict:
        got = {k: [e.extra for e in v] for k, v in triggering_asset_events.items()} if triggering_asset_events else {}
        print("AND consumer triggered by:", got)
        return {str(k): v for k, v in got.items()}
    calc()


@dag(dag_id="exp_e2_consumer_or", schedule=(A1 | A2 | A3), catchup=False, max_active_runs=1, tags=["exp", "e2"])
def exp_e2_consumer_or():
    @task.short_circuit(inlets=[A1, A2, A3])
    def gate(inlet_events=None, triggering_asset_events=None) -> bool:
        """Latest-per-input: take the newest event of every input; run only once all three exist."""
        latest = {}
        for name, a in E2_ASSETS.items():
            evs = inlet_events[a]
            latest[name] = evs[-1].extra if len(evs) else None
        trig = {str(k): len(v) for k, v in (triggering_asset_events or {}).items()}
        print("triggered by:", trig, "latest per input:", latest)
        ready = all(v is not None for v in latest.values())
        print("READY" if ready else "NOT READY - skipping downstream")
        return ready

    @task
    def calc(inlet_events=None) -> dict:
        chosen = {name: inlet_events[a][-1].extra for name, a in E2_ASSETS.items()}
        print("calc with versions:", chosen)
        return chosen

    gate() >> calc.override(inlets=[A1, A2, A3])()


# ---------------- E3 ----------------
E3_ACCT = Asset(name="exp_e3_acct", uri="bench://exp/e3/acct")
E3_KEYS = account_ids(10_000)


@dag(dag_id="exp_e3_producer", schedule=PartitionedAtRuntime(), catchup=False,
     params={"keys": Param(["ACCT00000001"], type="array")}, tags=["exp", "e3"])
def exp_e3_producer():
    @task(outlets=[E3_ACCT], do_xcom_push=False)
    def emit(params: dict | None = None, *, outlet_events=None) -> None:
        outlet_events[E3_ACCT].add_partitions(list(params["keys"]))
    emit()


@dag(dag_id="exp_e3_consumer",
     schedule=PartitionedAssetTimetable(assets=E3_ACCT, default_partition_mapper=AllowedKeyMapper(E3_KEYS)),
     catchup=False, tags=["exp", "e3"])
def exp_e3_consumer():
    EmptyOperator(task_id="calc")


exp_e1_producer(); exp_e1_consumer()
exp_e2_producer(); exp_e2_consumer_and(); exp_e2_consumer_or()
exp_e3_producer(); exp_e3_consumer()
