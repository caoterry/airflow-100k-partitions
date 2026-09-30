"""Scenario F — Airflow-native partitions (AIP-76 implementation, Airflow >= 3.3).

Producer (bench_partition_producer, PartitionedAtRuntime): emits N partition keys on asset `bench_accounts` via
outlet_events[asset].add_partitions(...). `emitters` controls how many mapped emitter tasks share the work
(1 = one task emits all N keys in a single TI-success API call; 100 = 100 mapped tasks emit N/100 keys each).

Consumer (bench_partition_consumer, PartitionedAssetTimetable + IdentityMapper): one DagRun per key, single
EmptyOperator task (scheduler short-circuits it, so we measure run creation, not execution).

Trigger the producer with conf {"n": 10000, "emitters": 1}. The harness `partition` command samples asset_event,
asset_partition_dag_run (pending/created) and consumer dag_run states.
"""
from __future__ import annotations

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, IdentityMapper, Param, PartitionedAssetTimetable, PartitionedAtRuntime, dag, task
from _common import DEFAULT_N, account_ids, chunks

ACCOUNTS = Asset(name="bench_accounts", uri="bench://accounts")


@dag(
    dag_id="bench_partition_producer",
    schedule=PartitionedAtRuntime(),
    catchup=False,
    params={
        "n": Param(DEFAULT_N, type="integer", minimum=1),
        "emitters": Param(1, type="integer", minimum=1),
    },
    tags=["bench", "partitions"],
)
def bench_partition_producer():
    @task
    def make_chunks(params: dict | None = None) -> list[list[str]]:
        n, e = int(params["n"]), int(params["emitters"])
        return chunks(account_ids(n), max(1, -(-n // e)))

    # Emitters are serialized on purpose: concurrent emitters queue on the asset row lock and the default
    # workers.execution_api_timeout (5 s) then turns lock wait into client retries.
    @task(outlets=[ACCOUNTS], do_xcom_push=False, max_active_tis_per_dagrun=1)
    def emit(keys: list[str], *, outlet_events=None) -> None:
        outlet_events[ACCOUNTS].add_partitions(keys)

    emit.expand(keys=make_chunks())


@dag(
    dag_id="bench_partition_consumer",
    schedule=PartitionedAssetTimetable(assets=ACCOUNTS, default_partition_mapper=IdentityMapper()),
    catchup=False,
    max_active_runs=100000,
    tags=["bench", "partitions"],
)
def bench_partition_consumer():
    EmptyOperator(task_id="process_partition")


bench_partition_producer()
bench_partition_consumer()
