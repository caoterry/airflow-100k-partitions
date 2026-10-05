"""v2: two grains, one bucket, with Airflow making every timing decision.

  v2_land          the producer. ANY arrival goes through it, 3 accounts or 100,000: it writes the journal (the bucket),
                   emits one keyed asset event per account on v2_positions (lineage), and one unkeyed event on
                   v2_positions_landed (the bell). There is no separate path for batch data and adjustments.
  v2_batcher       scheduled on the bell. claim -> spark (one mapped task per batch, pool v2_spark = K slots)
                   -> publish. No cron, no waiting thresholds, no capacity counting.

Rule: no Dag may put PartitionedAssetTimetable on v2_positions or v2_pnl (that would create one DagRun per account).
"""
from __future__ import annotations

import os
import sys
import time
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import journal
from airflow.sdk import Asset, Param, PartitionedAtRuntime, dag, task

POSITIONS = Asset(name="v2_positions", uri="v2://positions")              # keyed: partition_key = account
BELL = Asset(name="v2_positions_landed", uri="v2://positions-landed")     # unkeyed: "there is work in the journal"
PNL = Asset(name="v2_pnl", uri="v2://pnl")                                # keyed output, lineage only

K = 3                        # engine slots = the v2_spark pool size (production: 10)
B_MIN = 4                    # smallest batch worth its own job (tiny on purpose; production: about 100)
ENGINE_STARTUP_S = 6         # the Spark job is a stand-in: sleep(startup + per-account time)
PER_ACCOUNT_S = 0.5


@dag(dag_id="v2_land", schedule=PartitionedAtRuntime(), catchup=False,
     params={"accounts": Param(["ACC1"], type="array"), "version": Param(1, type="integer")}, tags=["v2"])
def v2_land():
    @task(outlets=[POSITIONS, BELL], do_xcom_push=False)
    def land(params: dict | None = None, *, outlet_events=None) -> None:
        accounts, version = list(params["accounts"]), int(params["version"])
        journal.note_versions(accounts, version)                      # 1. the bucket (our DB)
        outlet_events[POSITIONS].extra = {"version": version}
        outlet_events[POSITIONS].add_partitions(accounts)              # 2. lineage: one keyed asset_event per account
        outlet_events[BELL].extra = {"accounts": len(accounts), "version": version}   # 3. the bell: one unkeyed event
    land()


def _batch_failed(context) -> None:
    """Runs on the worker after Airflow's last retry failed: mark only this batch's accounts failed."""
    batch = context["ti"].xcom_pull(task_ids="claim", key="return_value", map_indexes=None)
    idx = context["ti"].map_index
    batch_id = batch[idx]["batch_id"] if isinstance(batch, list) else None
    if batch_id:
        journal.fail(batch_id, str(context.get("exception")))


@dag(dag_id="v2_batcher", schedule=[BELL], catchup=False, max_active_runs=1, tags=["v2"])
def v2_batcher():
    @task
    def claim(run_id: str | None = None) -> list[dict]:
        return journal.claim(run_id, K, B_MIN)

    @task(pool="v2_spark", retries=1, retry_delay=timedelta(seconds=5), on_failure_callback=_batch_failed)
    def spark(batch: dict) -> dict:
        journal.batch_running(batch["batch_id"])
        bad = journal.poisoned(batch["accounts"])
        time.sleep(ENGINE_STARTUP_S + PER_ACCOUNT_S * len(batch["accounts"]))
        if bad:
            raise ValueError(f"engine failed on {bad}")
        return batch

    @task(outlets=[PNL])
    def publish(batch: dict, *, outlet_events=None) -> dict:
        result = journal.publish(batch["batch_id"])
        if result["done"] or result["reopened"]:
            outlet_events[PNL].extra = {"batch_id": batch["batch_id"], "versions": result["versions"]}
            outlet_events[PNL].add_partitions(result["done"] + result["reopened"])
        return result

    publish.expand(batch=spark.expand(batch=claim()))


v2_land()
v2_batcher()
