"""Scenario A — flat expand, scheduler-only.

make_ids(n) -> NoopAccountOperator.expand(account=ids)   (N mapped TIs, never hit the executor)
Trigger with conf {"n": 100000}.
"""
from __future__ import annotations

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow.sdk import dag, task, Param
from _common import DEFAULT_N, NoopAccountOperator, account_ids


@dag(
    dag_id="bench_flat_empty",
    schedule=None,
    catchup=False,
    params={"n": Param(DEFAULT_N, type="integer", minimum=1)},
    tags=["bench", "scheduler-only"],
)
def bench_flat_empty():
    @task
    def make_ids(params: dict | None = None) -> list[str]:
        return account_ids(int(params["n"]))

    ids = make_ids()
    NoopAccountOperator.partial(task_id="noop").expand(account=ids)


bench_flat_empty()
