"""Scenario B — flat expand, real execution.

make_ids(n) -> process(account) mapped N times; each TI is a real task-SDK process that does nothing.
Measures executor/task-runner overhead per partition on top of scenario A.
"""
from __future__ import annotations

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow.sdk import dag, task, Param
from _common import DEFAULT_N, account_ids


@dag(
    dag_id="bench_flat_python",
    schedule=None,
    catchup=False,
    params={"n": Param(DEFAULT_N, type="integer", minimum=1)},
    tags=["bench", "full-exec"],
)
def bench_flat_python():
    @task
    def make_ids(params: dict | None = None) -> list[str]:
        return account_ids(int(params["n"]))

    @task(do_xcom_push=False)
    def process(account: str) -> None:
        # Real work would go here; we only want orchestration overhead.
        assert account.startswith("ACCT")

    process.expand(account=make_ids())


bench_flat_python()
