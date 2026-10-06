"""v2 (variant D): two grains, one bucket. One Spark job per batcher run, K runs overlap (max_active_runs = K = pool size),
the journal lives in Airflow's asset_state_store and is written by one serial ledger Dag. Correctness of overlapping runs
comes from idempotent, version-ordered output writes (the calc writes (account, dataset_version); readers take the latest).

  v2_land      the producer. ANY arrival goes through it, 3 accounts or 100,000: it emits one unkeyed event on v2_positions
               (the dataset was updated) and one on v2_positions_landed (the bell) whose extra carries {account: version}.
               It writes no table of ours. No keyed events: per-account lineage lives in the journal and in the versioned
               output rows, not in asset_event (decision D20; 100k keyed events cost 7 min of registration per run).
  v2_batcher   scheduled on the bell. claim -> spark (one mapped task per batch, pool v2_spark = K slots) -> publish.
               Airflow hands the run every bell since the previous run (triggering_asset_events), so there is no watermark.
  v2_requeue   operator action: ring the bell again for failed accounts.
  v2_exceptions an EXAMPLE downstream consumer (requirement R4): scheduled on v2_pnl, it gets one event per finished batch
               carrying the account list, so a downstream Dag chains at batch grain with no per-account runs.

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
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensorAsync

POSITIONS = Asset(name="v2_positions", uri="v2://positions")              # unkeyed; its asset_state_store holds the journal keys
BELL = Asset(name="v2_positions_landed", uri="v2://positions-landed")     # unkeyed: "these accounts landed, at these versions"
DEBOUNCED = Asset(name="v2_positions_debounced", uri="v2://positions-debounced")  # unkeyed: the bell after the debounce window
FINISHED = Asset(name="v2_batches_finished", uri="v2://batches-finished") # unkeyed: "these batch keys are final", for the ledger
PNL = Asset(name="v2_pnl", uri="v2://pnl")                                # unkeyed: one event per finished batch, the bell for downstream Dags

K = 3                        # engine slots = the v2_spark pool size = max_active_runs of the batcher (production: 10)
DEBOUNCE_S = 30              # idle-time accumulation window (production: about 60). Interim: Airflow has no debounce for asset-triggered Dags yet.
DEBOUNCE_MARGIN_S = 5        # bells stamped in the last few seconds wait for the next window (their producer may still be committing)
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
        outlet_events[POSITIONS].extra = {"dataset_version": dv, "accounts": len(accounts)}  # one event: the dataset moved
        outlet_events[BELL].extra = {"dataset_version": dv, "accounts": markers}  # the bell carries the work list
    land()


@dag(dag_id="v2_batcher", schedule=[DEBOUNCED], catchup=False, max_active_runs=K, tags=["v2"])
def v2_batcher():
    @task(inlets=[POSITIONS], retries=2, retry_delay=timedelta(seconds=5))
    def claim(run_id: str | None = None, *, triggering_asset_events=None, asset_state_store=None) -> list[dict]:
        wanted = js.candidates(triggering_asset_events[DEBOUNCED] if triggering_asset_events else [])
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
        # ONE unkeyed event per batch, same shape as the bell: {account: [dataset_version, marker]}. A downstream Dag
        # scheduled on PNL reads it from its triggering events and runs at batch grain. (Keyed events were dropped in D20:
        # 100k of them took 7 min to register, one api-server request, 3 SQL statements per key; nobody could consume them
        # without a run per account.)
        outlet_events[PNL].extra = {"batch_id": batch["batch_id"], "accounts": result["versions"]}
        return result

    @task(inlets=[POSITIONS], outlets=[FINISHED, DEBOUNCED], trigger_rule="all_done")
    def finalize(batches: list[dict], *, asset_state_store=None, outlet_events=None) -> dict:
        """Summarise this run's batch keys (writes nothing shared), tell the ledger which keys are final, and ring an empty
        tick on the debounced bell so that forwarded bells stamped after this run's queue row are swept by the next run.
        A run that claimed nothing skips here, so it rings no tick and the chain ends."""
        result = js.summarize(asset_state_store[POSITIONS], batches or [])
        if not result["batch_ids"]:
            raise AirflowSkipException("nothing claimed, nothing to fold")      # a skipped task emits no event
        outlet_events[FINISHED].extra = {"batch_ids": result["batch_ids"]}
        outlet_events[DEBOUNCED].extra = {"tick": True}
        return result

    @task(outlets=[DEBOUNCED], trigger_rule="all_done")
    def ring_retry(result: dict, *, outlet_events=None) -> None:
        """Blast-radius rule a+c: the healthy accounts of a failed batch ring the bell once more.
        A declared outlet emits an event on EVERY success, so this task SKIPS when there is nothing to retry
        (a skipped task emits nothing); otherwise every run would ring an empty bell and start the next run forever."""
        if not result or not result.get("retry"):
            raise AirflowSkipException("nothing to retry")
        outlet_events[DEBOUNCED].extra = {"accounts": result["retry"], "retry_of": "failed batches of this run"}

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


@dag(dag_id="v2_debounce", schedule=[BELL], catchup=False, max_active_runs=1, tags=["v2"])
def v2_debounce():
    """Idle-time accumulation, built from stock parts: hold the first bell for DEBOUNCE_S seconds (deferred, no worker held),
    then forward the merged work list. Bells that ring during the hold coalesce into this Dag's next run because
    max_active_runs=1. INTERIM: this is what a debounce window on asset-triggered Dags would do natively; it is one of the
    upstream proposals (a count-or-age wait policy), and this Dag is deleted when that ships. Only one parameter: the window."""
    hold = TimeDeltaSensorAsync(task_id="hold", delta=timedelta(seconds=DEBOUNCE_S))

    @task(inlets=[BELL, POSITIONS], outlets=[DEBOUNCED, BELL])
    def forward(*, inlet_events=None, asset_state_store=None, outlet_events=None) -> dict:
        """Forward every bell of this window as ONE debounced bell.

        Airflow attaches to a run only the bells stamped before its queue row, so triggering_asset_events would hold the first
        bell of the window and leave the rest for a later run. Instead the window is read from the bell asset's event log:
        all bells with a timestamp in (cutoff, now - margin], where cutoff is the last timestamp this Dag forwarded. The
        cutoff lives in the state store and is written only here (max_active_runs=1), and the margin leaves room for a bell
        whose producer committed after its timestamp. Then an EMPTY tick on the raw bell starts the next window if anything
        was forwarded, so bells that rang after the queue row are not stranded; a window that forwards nothing rings no tick.
        INTERIM: this is what a native debounce window on asset-triggered Dags would do; one of the upstream proposals."""
        from datetime import datetime, timezone
        store = asset_state_store[POSITIONS]
        cutoff = store.get("debounce/cutoff")
        upper = datetime.now(timezone.utc) - timedelta(seconds=DEBOUNCE_MARGIN_S)
        events = inlet_events[BELL].after(datetime.fromisoformat(cutoff)) if cutoff else inlet_events[BELL]
        window = [e for e in events if (not cutoff or e.timestamp.isoformat() > cutoff) and e.timestamp <= upper and (e.extra or {}).get("accounts")]
        merged = js.candidates(window)
        if not merged:
            raise AirflowSkipException("no bells with accounts in this window")
        store.set("debounce/cutoff", max(e.timestamp for e in window).isoformat())
        outlet_events[DEBOUNCED].extra = {"accounts": {a: [dv, m] for a, (dv, m, _, _) in merged.items()}, "bells": len(window)}
        outlet_events[BELL].extra = {"tick": True}
        return {"accounts": len(merged), "bells": len(window)}

    hold >> forward()


@dag(dag_id="v2_ledger", schedule=[FINISHED], catchup=False, max_active_runs=1, tags=["v2"])
def v2_ledger():
    """The only writer of done and failed. One run at a time; finished-batch events that arrive while it runs coalesce
    into one queue row and are folded by the next run."""
    @task(inlets=[POSITIONS])
    def fold(*, triggering_asset_events=None, asset_state_store=None) -> dict:
        ids = [bid for e in (triggering_asset_events[FINISHED] if triggering_asset_events else []) for bid in ((e.extra or {}).get("batch_ids") or [])]
        return js.fold(asset_state_store[POSITIONS], ids)
    fold()


@dag(dag_id="v2_exceptions", schedule=[PNL], catchup=False, max_active_runs=1, tags=["v2", "example-consumer"])
def v2_exceptions():
    """EXAMPLE of chaining (requirement R4). Scheduled on the PnL asset: every finished batch is one event whose extra carries
    the accounts and their versions, so this Dag runs once per batch (or once per several batches when they coalesce while a
    run is active) and can hand the whole account list to one job of its own. A real downstream Dag would repeat the
    batcher's shape: claim against its own done dict, one job, publish one event."""
    @task
    def review(*, triggering_asset_events=None) -> dict:
        events = triggering_asset_events[PNL] if triggering_asset_events else []
        accounts: dict[str, list] = {}
        for e in events:
            accounts.update((e.extra or {}).get("accounts") or {})
        return {"batches": [(e.extra or {}).get("batch_id") for e in events], "accounts": len(accounts),
                "sample": dict(sorted(accounts.items())[:3])}
    review()


@dag(dag_id="v2_requeue", schedule=None, catchup=False, params={"accounts": Param(["ACC42"], type="array")}, tags=["v2"])
def v2_requeue():
    @task(inlets=[POSITIONS], outlets=[DEBOUNCED], do_xcom_push=False)
    def ring(params: dict | None = None, *, outlet_events=None, asset_state_store=None) -> None:
        store = asset_state_store[POSITIONS]
        failed = store.get("failed") or {}
        wanted = {a: failed[a] for a in params["accounts"] if a in failed}
        if not wanted:
            raise AirflowSkipException("none of these accounts is in failed")   # a skipped task rings no bell
        outlet_events[DEBOUNCED].extra = {"accounts": wanted, "requeue": True}
    ring()


v2_land()
v2_debounce()
v2_batcher()
v2_ledger()
v2_exceptions()
v2_requeue()
