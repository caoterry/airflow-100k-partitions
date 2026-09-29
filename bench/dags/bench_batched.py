"""Scenario C — batched expand (the 'hierarchical' pattern).

make_batches(n, batch_size) -> process_batch(accounts) mapped n/batch_size times.
100k accounts / 1000 per batch = 100 TIs. Loses per-account observability in Airflow, gains ~1000x less TIs.
"""
from __future__ import annotations

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow.sdk import dag, task, Param
from _common import DEFAULT_N, account_ids, chunks


@dag(
    dag_id="bench_batched",
    schedule=None,
    catchup=False,
    params={
        "n": Param(DEFAULT_N, type="integer", minimum=1),
        "batch_size": Param(1000, type="integer", minimum=1),
    },
    tags=["bench", "full-exec", "batched"],
)
def bench_batched():
    @task
    def make_batches(params: dict | None = None) -> list[list[str]]:
        return chunks(account_ids(int(params["n"])), int(params["batch_size"]))

    @task
    def process_batch(accounts: list[str]) -> dict:
        # Per-account work happens inside one TI; return a tiny per-batch summary as the partition record.
        return {"count": len(accounts), "first": accounts[0], "last": accounts[-1]}

    process_batch.expand(accounts=make_batches())


bench_batched()
