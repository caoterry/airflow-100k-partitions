"""Scenario D — two-level DAG-of-DAGs.

Parent: make_batches -> TriggerDagRunOperator.expand(conf=[{start, count}...]) fires B child runs.
Child (bench_two_level_child): make_ids(start,count) -> NoopAccountOperator.expand  (K mapped TIs per run).
100k = 100 child runs x 1000 mapped TIs. Tests the scheduler across many concurrent DagRuns and gives
per-batch run state for free. Set child_mode=python to use real task execution in the child.
"""
from __future__ import annotations

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task, Param
from _common import DEFAULT_N, NoopAccountOperator


@dag(
    dag_id="bench_two_level_parent",
    schedule=None,
    catchup=False,
    params={
        "n": Param(DEFAULT_N, type="integer", minimum=1),
        "batch_size": Param(1000, type="integer", minimum=1),
        "child_mode": Param("empty", type="string", enum=["empty", "python"]),
    },
    tags=["bench", "two-level"],
)
def bench_two_level_parent():
    @task
    def make_batches(params: dict | None = None) -> list[dict]:
        n, bs = int(params["n"]), int(params["batch_size"])
        return [
            {"start": s, "count": min(bs, n - s + 1), "mode": params["child_mode"]}
            for s in range(1, n + 1, bs)
        ]

    TriggerDagRunOperator.partial(
        task_id="trigger_child",
        trigger_dag_id="bench_two_level_child",
        wait_for_completion=False,
        reset_dag_run=True,
    ).expand(conf=make_batches())


@dag(
    dag_id="bench_two_level_child",
    schedule=None,
    catchup=False,
    params={
        "start": Param(1, type="integer"),
        "count": Param(1000, type="integer"),
        "mode": Param("empty", type="string"),
    },
    tags=["bench", "two-level"],
)
def bench_two_level_child():
    @task
    def make_ids(params: dict | None = None) -> list[str]:
        s, c = int(params["start"]), int(params["count"])
        return [f"ACCT{i:08d}" for i in range(s, s + c)]

    @task(do_xcom_push=False)
    def process(account: str) -> None:
        assert account.startswith("ACCT")

    @task.branch
    def pick(params: dict | None = None) -> str:
        return "make_ids_python" if params["mode"] == "python" else "make_ids_empty"

    ids_empty = make_ids.override(task_id="make_ids_empty")()
    ids_python = make_ids.override(task_id="make_ids_python")()
    NoopAccountOperator.partial(task_id="noop").expand(account=ids_empty)
    process.expand(account=ids_python)
    pick() >> [ids_empty, ids_python]


bench_two_level_parent()
bench_two_level_child()
