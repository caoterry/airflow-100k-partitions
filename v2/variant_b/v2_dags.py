"""v2 (variant B): two grains, one bucket, with the journal inside Airflow's asset_state_store.

  v2_land      the producer. ANY arrival goes through it, 3 accounts or 100,000: it emits one keyed asset event per account
               on v2_positions (lineage) and one unkeyed event on v2_positions_landed (the bell) whose extra carries
               {account: version}. It writes no table of ours.
  v2_batcher   scheduled on the bell. claim -> spark (one mapped task per batch, pool v2_spark = K slots) -> publish.
               Airflow hands the run every bell since the previous run (triggering_asset_events), so there is no watermark.
  v2_requeue   operator action: ring the bell again for failed accounts.

Rule: no Dag may put PartitionedAssetTimetable on v2_positions or v2_pnl (that would create one DagRun per account).
"""
from __future__ import annotations

import os
import sys
import time
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import journal_store as js
from airflow.sdk import Asset, Param, PartitionedAtRuntime, dag, get_current_context, task

POSITIONS = Asset(name="v2_positions", uri="v2://positions")              # keyed: partition_key = account; also holds the journal keys
BELL = Asset(name="v2_positions_landed", uri="v2://positions-landed")     # unkeyed: "these accounts landed, at these versions"
PNL = Asset(name="v2_pnl", uri="v2://pnl")                                # keyed output, lineage only

K = 3                        # engine slots = the v2_spark pool size (production: 10)
B_MIN = 4                    # smallest batch worth its own job (tiny on purpose; production: about 100)
ENGINE_STARTUP_S = 6         # the Spark job is a stand-in: sleep(startup + per-account time)
PER_ACCOUNT_S = 0.5
POISON = {"ACC42"}           # test only: the stand-in engine fails on these accounts


@dag(dag_id="v2_land", schedule=PartitionedAtRuntime(), catchup=False,
     params={"accounts": Param(["ACC1"], type="array"), "version": Param(1, type="integer")}, tags=["v2"])
def v2_land():
    @task(outlets=[POSITIONS, BELL], do_xcom_push=False)
    def land(params: dict | None = None, *, outlet_events=None) -> None:
        accounts, version = list(params["accounts"]), int(params["version"])
        outlet_events[POSITIONS].extra = {"version": version}
        outlet_events[POSITIONS].add_partitions(accounts)                       # lineage: one keyed asset_event per account
        outlet_events[BELL].extra = {"accounts": {a: version for a in accounts}}  # the bell carries the work list
    land()


@dag(dag_id="v2_batcher", schedule=[BELL], catchup=False, max_active_runs=1, tags=["v2"])
def v2_batcher():
    @task(inlets=[POSITIONS], retries=2, retry_delay=timedelta(seconds=5))
    def claim(run_id: str | None = None, *, triggering_asset_events=None, asset_state_store=None) -> list[dict]:
        wanted = js.candidates(triggering_asset_events[BELL] if triggering_asset_events else [])
        return js.claim(asset_state_store[POSITIONS], wanted, run_id, K, B_MIN)

    @task(inlets=[POSITIONS], pool="v2_spark", retries=1, retry_delay=timedelta(seconds=5))
    def spark(batch: dict, *, asset_state_store=None) -> dict:
        store = asset_state_store[POSITIONS]
        js.running(store, batch["batch_id"])
        try:
            time.sleep(ENGINE_STARTUP_S + PER_ACCOUNT_S * len(batch["accounts"]))
            bad = sorted(POISON & set(batch["accounts"]))
            if bad:
                raise ValueError(f"engine failed on {bad}")
        except Exception as exc:
            ctx = get_current_context()
            if ctx["ti"].try_number > ctx["task"].retries:      # this was the last attempt Airflow will make
                js.fail(store, batch, str(exc))
            raise
        return batch

    @task(inlets=[POSITIONS], outlets=[PNL])
    def publish(batch: dict, *, outlet_events=None, asset_state_store=None) -> dict:
        result = js.publish(asset_state_store[POSITIONS], batch)
        if result["done"]:
            outlet_events[PNL].extra = {"batch_id": batch["batch_id"], "versions": result["versions"]}
            outlet_events[PNL].add_partitions(result["done"])
        return result

    publish.expand(batch=spark.expand(batch=claim()))


@dag(dag_id="v2_requeue", schedule=None, catchup=False, params={"accounts": Param(["ACC42"], type="array")}, tags=["v2"])
def v2_requeue():
    @task(inlets=[POSITIONS], outlets=[BELL], do_xcom_push=False)
    def ring(params: dict | None = None, *, outlet_events=None, asset_state_store=None) -> None:
        store = asset_state_store[POSITIONS]
        again = {a: int((store.get(f"acct/{a}") or {}).get("seen", 1)) for a in params["accounts"]}
        outlet_events[BELL].extra = {"accounts": again, "requeue": True}
    ring()


v2_land()
v2_batcher()
v2_requeue()
