"""v2 (variant D): two grains, one bucket. One Spark job per batcher run, K runs overlap (max_active_runs = K = pool size),
the journal lives in Airflow's asset_state_store and is written by one serial ledger Dag. Correctness of overlapping runs
comes from idempotent, version-ordered output writes (the calc writes (account, dataset_version); readers take the latest).

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
from airflow.sdk import Asset, Param, PartitionedAtRuntime, Variable, dag, get_current_context, task, task_group
from airflow.sdk.exceptions import AirflowSkipException

POSITIONS = Asset(name="v2_positions", uri="v2://positions")              # keyed: partition_key = account; also holds the journal keys
BELL = Asset(name="v2_positions_landed", uri="v2://positions-landed")     # unkeyed: "these accounts landed, at these versions"
FINISHED = Asset(name="v2_batches_finished", uri="v2://batches-finished") # unkeyed: "these batch keys are final", for the ledger
PNL = Asset(name="v2_pnl", uri="v2://pnl")                                # keyed output, lineage only

K = 3                        # engine slots = the v2_spark pool size = max_active_runs of the batcher (production: 10)
ENGINE_STARTUP_S = 6         # the Spark job is a stand-in: sleep(startup + per-account time)
PER_ACCOUNT_S = 0.5
def poison() -> set[str]:    # test only: the stand-in engine fails on these accounts; Airflow Variable v2_poison, comma-separated
    return {a.strip() for a in Variable.get("v2_poison", default="").split(",") if a.strip()}


@dag(dag_id="v2_land", schedule=PartitionedAtRuntime(), catchup=False,
     params={"accounts": Param(["ACC1"], type="array"), "version": Param(1, type="integer"),
             "markers": Param({}, type="object")}, tags=["v2"])
def v2_land():
    @task(outlets=[POSITIONS, BELL], do_xcom_push=False)
    def land(params: dict | None = None, *, outlet_events=None) -> None:
        """version = the dataset version of this landing (orders landings). markers = per-account row fingerprint
        (snapshot feeds); when absent the dataset version is the marker (delta feeds: every listed account changed)."""
        accounts, dv = list(params["accounts"]), int(params["version"])
        markers = {a: str((params.get("markers") or {}).get(a, dv)) for a in accounts}
        outlet_events[POSITIONS].extra = {"dataset_version": dv}
        outlet_events[POSITIONS].add_partitions(accounts)                       # lineage: one keyed asset_event per account
        outlet_events[BELL].extra = {"dataset_version": dv, "accounts": markers}  # the bell carries the work list
    land()


@dag(dag_id="v2_batcher", schedule=[BELL], catchup=False, max_active_runs=K, tags=["v2"])
def v2_batcher():
    @task(inlets=[POSITIONS], retries=2, retry_delay=timedelta(seconds=5))
    def claim(run_id: str | None = None, *, triggering_asset_events=None, asset_state_store=None) -> list[dict]:
        wanted = js.candidates(triggering_asset_events[BELL] if triggering_asset_events else [])
        return js.claim(asset_state_store[POSITIONS], wanted, run_id)

    @task(inlets=[POSITIONS], pool="v2_spark", retries=1, retry_delay=timedelta(seconds=5))
    def spark(batch: dict, *, asset_state_store=None) -> dict:
        store = asset_state_store[POSITIONS]
        js.running(store, batch["batch_id"])
        try:
            time.sleep(ENGINE_STARTUP_S + PER_ACCOUNT_S * len(batch["accounts"]))
            bad = sorted(poison() & set(batch["accounts"]))
            if bad:
                raise ValueError(f"engine failed on {bad}")
        except Exception as exc:
            ctx = get_current_context()
            if ctx["ti"].try_number > ctx["task"].retries:      # this was the last attempt Airflow will make
                culprits = [a for a in batch["accounts"] if a in str(exc)]   # the engine named them (stand-in: "engine failed on [...]")
                js.fail(store, batch, str(exc), culprits)
            raise
        return batch

    @task(inlets=[POSITIONS], outlets=[PNL])
    def publish(batch: dict, *, outlet_events=None, asset_state_store=None) -> dict:
        result = js.publish(asset_state_store[POSITIONS], batch)
        if result["done"]:
            # extra is shared by every keyed event of this outlet, so keep it small: only the batch id.
            # (Carrying the batch's whole versions dict here cost 500 MB of asset_event.extra for 10k accounts.)
            outlet_events[PNL].extra = {"batch_id": batch["batch_id"]}
            outlet_events[PNL].add_partitions(result["done"])
        return result

    @task(inlets=[POSITIONS], outlets=[FINISHED], trigger_rule="all_done")
    def finalize(batches: list[dict], *, asset_state_store=None, outlet_events=None) -> dict:
        """Summarise this run's batch keys (writes nothing shared) and tell the ledger which keys are final."""
        result = js.summarize(asset_state_store[POSITIONS], batches or [])
        if not result["batch_ids"]:
            raise AirflowSkipException("nothing claimed, nothing to fold")      # a skipped task emits no event
        outlet_events[FINISHED].extra = {"batch_ids": result["batch_ids"]}
        return result

    @task(outlets=[BELL], trigger_rule="all_done")
    def ring_retry(result: dict, *, outlet_events=None) -> None:
        """Blast-radius rule a+c: the healthy accounts of a failed batch ring the bell once more.
        A declared outlet emits an event on EVERY success, so this task SKIPS when there is nothing to retry
        (a skipped task emits nothing); otherwise every run would ring an empty bell and start the next run forever."""
        if not result or not result.get("retry"):
            raise AirflowSkipException("nothing to retry")
        dv = max(v[0] for v in result["retry"].values())
        outlet_events[BELL].extra = {"dataset_version": dv, "accounts": {a: m for a, (d, m) in result["retry"].items()},
                                     "retry_of": "failed batches of this run"}

    @task(trigger_rule="all_done")
    def check(result: dict) -> None:
        """Colours the run: the bookkeeping is done by now; this task fails only so the run shows red when a batch failed."""
        if result and result.get("failed"):
            raise RuntimeError(f"{result['failed']} account(s) moved to failed; see the batch keys and the failed dict")

    @task_group
    def run_batch(batch: dict):
        """One group instance per batch: spark[i] -> publish[i]. A mapped task group keeps the pairs independent, so a failed
        batch does not stop the publish of the others (expanding publish over spark's output would: Airflow cannot expand
        the downstream until every upstream instance has finished)."""
        publish(spark(batch))

    batches = claim()
    groups = run_batch.expand(batch=batches)
    fin = finalize(batches)
    groups >> fin
    ring_retry(fin)
    check(fin)


@dag(dag_id="v2_ledger", schedule=[FINISHED], catchup=False, max_active_runs=1, tags=["v2"])
def v2_ledger():
    """The only writer of done and failed. One run at a time; finished-batch events that arrive while it runs coalesce
    into one queue row and are folded by the next run."""
    @task(inlets=[POSITIONS])
    def fold(*, triggering_asset_events=None, asset_state_store=None) -> dict:
        ids = [bid for e in (triggering_asset_events[FINISHED] if triggering_asset_events else []) for bid in ((e.extra or {}).get("batch_ids") or [])]
        return js.fold(asset_state_store[POSITIONS], ids)
    fold()


@dag(dag_id="v2_requeue", schedule=None, catchup=False, params={"accounts": Param(["ACC42"], type="array")}, tags=["v2"])
def v2_requeue():
    @task(inlets=[POSITIONS], outlets=[BELL], do_xcom_push=False)
    def ring(params: dict | None = None, *, outlet_events=None, asset_state_store=None) -> None:
        store = asset_state_store[POSITIONS]
        failed = store.get("failed") or {}
        wanted = {a: failed[a] for a in params["accounts"] if a in failed}
        if not wanted:
            raise AirflowSkipException("none of these accounts is in failed")   # a skipped task rings no bell
        outlet_events[BELL].extra = {"dataset_version": max(v[0] for v in wanted.values()),
                                     "accounts": {a: m for a, (dv, m) in wanted.items()}, "requeue": True}
    ring()


v2_land()
v2_batcher()
v2_ledger()
v2_requeue()
