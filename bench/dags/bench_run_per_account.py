"""Scenario E — one DagRun per firm account (closest Airflow-native analogue of a Dagster partition).

Each run: run_id = f"acct_{account}__{as_of}", conf = {"account": ..., "as_of": ...}; single EmptyOperator task, so the
measurement isolates the scheduler's per-DagRun cost (executor cost per task is measured by bench_flat_python).
Runs are created in bulk by the harness (REST API, concurrent). The DagRun row IS the partition record:
state, timestamps, re-runnable individually, queryable by run_id prefix.
"""
from __future__ import annotations

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, Param


@dag(
    dag_id="bench_run_per_account",
    schedule=None,
    catchup=False,
    params={"account": Param("ACCT00000000", type="string"), "as_of": Param("2026-09-29", type="string")},
    tags=["bench", "run-per-partition"],
    max_active_runs=100000,
)
def bench_run_per_account():
    EmptyOperator(task_id="process")


bench_run_per_account()
