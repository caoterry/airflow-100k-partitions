"""Scenario E — one DagRun per firm account (closest Airflow-native analogue of a Dagster partition).

Each run: run_id = f"acct_{account}__{as_of}", conf = {"account": ..., "as_of": ...}; single task.
Runs are created in bulk by the harness (REST API, concurrent). The DagRun row IS the partition record:
state, timestamps, re-runnable individually, queryable by run_id prefix.
"""
from __future__ import annotations

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow.sdk import dag, task, Param


@dag(
    dag_id="bench_run_per_account",
    schedule=None,
    catchup=False,
    params={"account": Param("ACCT00000000", type="string"), "as_of": Param("2026-09-29", type="string")},
    tags=["bench", "run-per-partition"],
    max_active_runs=100000,
)
def bench_run_per_account():
    @task(do_xcom_push=False)
    def process(params: dict | None = None) -> None:
        assert params["account"].startswith("ACCT")

    process()


bench_run_per_account()
