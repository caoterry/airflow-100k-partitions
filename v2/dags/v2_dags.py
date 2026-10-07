"""v2 (variant D, decisions D18-D23): two grains, one bucket. The RUN is the bucket; the PARTITION key is data.

One Spark job per batcher run, K runs overlap (max_active_runs = K = pool size), the journal lives in Airflow's asset_state_store
on the calc's output asset and every key has exactly one writer. Correctness of overlapping runs comes from idempotent,
version-ordered output writes (the calc writes (partition, versions); readers take the latest).

The key of the calc is a PARTITION (revenue PnL: a firm account; balance sheet: a region). A calc has N input DATASETS; every
producer of any of them rings the SAME bell. The journal records, per partition, the version and marker of each input dataset.

  v2_land        a producer. ANY arrival goes through it, 3 partitions or 100,000: ONE unkeyed event on the bell asset whose extra
                 is {dataset, version, partitions: {partition: marker}}. It writes no table of ours. No keyed events (D20).
  v2_debounce    scheduled on the bell, max_active_runs=1: a deferred hold until the window's first land + DEBOUNCE_S, then `forward`
                 reads the whole window from the bell asset's event log, writes it to ONE state-store key and emits ONE debounced
                 event that points at it (D21). The window's lower edge is the newest debounced event's window_end, so cutoff and
                 event are one transaction. `sweep` ticks a private asset only when bells are left past the window (D24).
  v2_batcher     scheduled on the debounced event (and its own tick), max_active_runs = K: claim -> one job (spark, pool v2_spark)
                 -> publish -> finalize. publish emits ONE event on v2_pnl pointing at the batch key. finalize emits FINISHED for the
                 ledger; `tick` rings the batcher's private tick asset so debounced events stamped after this run's queue row are
                 attached to the next run (LESSON 3; the batcher must use triggering events because K runs share no cutoff).
  v2_ledger      the only writer of done and failed: reads the FINISHED window (cutoff, now - margin] from the event log, folds, one
                 run at a time; `sweep` ticks a private asset only when events were left inside the margin (D24).
  v2_exceptions  an EXAMPLE downstream consumer: chained on v2_pnl at batch grain; folds by max version; ticks its own asset.
  v2_requeue     operator action: ring the bell again for failed partitions.

Rule: no Dag may put PartitionedAssetTimetable on these assets, and nothing emits keyed events (D20): above ~10k keys per landing
the unit of scheduling must be the batch.
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import journal_store as js
from airflow.sdk import Asset, Param, Variable, dag, get_current_context, task, task_group
from airflow.sdk.exceptions import AirflowSkipException
from airflow.providers.standard.sensors.date_time import DateTimeSensorAsync
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor

BELL = Asset(name="v2_inputs_landed", uri="v2://inputs-landed")           # producers ring it: {dataset, version, partitions: {p: marker}}
DEBOUNCED = Asset(name="v2_inputs_debounced", uri="v2://inputs-debounced") # forward: {"window": key, "window_end", "n_partitions", "bells"}
BATCHER_TICK = Asset(name="v2_batcher_tick", uri="v2://batcher-tick")       # empty events the batcher rings for itself (LESSON 3)
PNL = Asset(name="v2_pnl", uri="v2://pnl")                                  # one event per finished batch: {"batch": key, ...}; holds the journal
FINISHED = Asset(name="v2_batches_finished", uri="v2://batches-finished")   # finalize -> ledger: {"batch_ids": [...]}
EXC_TICK = Asset(name="v2_exceptions_tick", uri="v2://exceptions-tick")     # the example consumer's private tick
DEBOUNCE_TICK = Asset(name="v2_debounce_tick", uri="v2://debounce-tick")    # debounce's private tick: bells left past the window (margin hole)
LEDGER_TICK = Asset(name="v2_ledger_tick", uri="v2://ledger-tick")          # ledger's private tick: FINISHED events left inside the margin

K = 3                        # engine slots = the v2_spark pool size = max_active_runs of the batcher (production: 10)
DEBOUNCE_S = 10              # kata recordings: 10 s window (production about 60; the design value was 30). Interim: Airflow has no debounce for asset-triggered Dags yet.
DEBOUNCE_MARGIN_S = 5        # bells stamped in the last few seconds wait for the next window (their producer may still be committing)
LEDGER_MARGIN_S = 5          # same for FINISHED events: K finalizes commit concurrently, so timestamp order is not commit order
ENGINE_STARTUP_S = 6         # the Spark job is a stand-in: sleep(startup + per-partition time)
PER_PARTITION_S = 0.5
def poison() -> set[str]:    # test only: the stand-in engine fails on these partitions; Airflow Variable v2_poison, comma-separated
    return {a.strip() for a in Variable.get("v2_poison", default="").split(",") if a.strip()}

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()

def _latest_window_end(inlet_events) -> str | None:
    """The newest debounced event's window_end (payload events from ring_retry / requeue carry none and are skipped)."""
    for e in inlet_events[DEBOUNCED].ascending(False).limit(20):
        if (e.extra or {}).get("window_end"):
            return e.extra["window_end"]
    return None


@dag(dag_id="v2_land", schedule=None, catchup=False,      # schedule=None: a run partition_key would make the events keyed again (D20)
     params={"dataset": Param("positions", type="string"), "version": Param(1, type="integer"),
             "partitions": Param(["ACC1"], type="array"), "markers": Param({}, type="object")}, tags=["v2"])
def v2_land():
    @task(outlets=[BELL], do_xcom_push=False)
    def land(params: dict | None = None, *, outlet_events=None) -> None:
        """version orders the landings of this dataset. markers = per-partition row fingerprint (snapshot feeds); when absent
        the version is the marker (delta feeds: every listed partition changed)."""
        partitions, v = list(params["partitions"]), int(params["version"])
        markers = {p: str((params.get("markers") or {}).get(p, v)) for p in partitions}
        outlet_events[BELL].extra = {"dataset": str(params["dataset"]), "version": v, "partitions": markers}
    land()


@dag(dag_id="v2_debounce", schedule=(BELL | DEBOUNCE_TICK), catchup=False, max_active_runs=1, tags=["v2"])
def v2_debounce():
    """Idle-time accumulation from stock parts: hold the first bell for DEBOUNCE_S seconds (deferred, no worker held), then forward
    the merged window. Bells that land during a run coalesce into one queue row (max_active_runs=1) and start exactly one next
    run, which reads its window from the event log. The window opens at the FIRST LAND NOT YET FORWARDED and closes DEBOUNCE_S
    later (`plan` + deferred `hold`), not at the run's run_after: a queue row can carry the timestamp of a land the previous
    window already took, and anchoring to it made short windows under a steady stream (D25). A run with nothing pending skips.
    `sweep` closes the margin hole: a bell that landed before this run started has no queue row of its own, so if the window
    left it inside the margin, sweep rings the private tick (D24). INTERIM for a native debounce / count-or-age wait policy on
    asset-triggered Dags (upstream proposal); this Dag is deleted when that ships."""

    @task(inlets=[BELL, DEBOUNCED], multiple_outputs=False)
    def plan(*, inlet_events=None) -> str:
        """When does this window close: the first bell after the published cutoff + DEBOUNCE_S (ISO). Skips when nothing is pending."""
        cutoff = _latest_window_end(inlet_events) or _iso(datetime.now(timezone.utc) - timedelta(days=1))
        cutoff_dt = datetime.fromisoformat(cutoff)
        pending = [e for e in inlet_events[BELL].after(cutoff) if e.timestamp > cutoff_dt and (e.extra or {}).get("partitions")]
        if not pending:
            raise AirflowSkipException("nothing pending: the previous window already forwarded these bells")
        return _iso(min(e.timestamp for e in pending) + timedelta(seconds=DEBOUNCE_S))

    hold = DateTimeSensorAsync(task_id="hold", target_time="{{ ti.xcom_pull(task_ids='plan') }}")   # deferred; fires at once if past

    @task(inlets=[BELL, DEBOUNCED, PNL], outlets=[DEBOUNCED], multiple_outputs=False)
    def forward(*, inlet_events=None, asset_state_store=None, outlet_events=None) -> dict:
        """ONE debounced event per window. Lower edge = the newest debounced event's window_end (read from the DEBOUNCED event log,
        so the cutoff advances in the same transaction that publishes the event: a crash can never lose a window); upper edge =
        now - margin. The window's partitions go to a state-store key; the event carries a pointer (D21)."""
        store = asset_state_store[PNL]
        cutoff = _latest_window_end(inlet_events)
        now = datetime.now(timezone.utc)
        if cutoff is None:
            cutoff = _iso(now - timedelta(days=1))                          # first window ever: do not read the whole history
        upper = now - timedelta(seconds=DEBOUNCE_MARGIN_S)
        cutoff_dt = datetime.fromisoformat(cutoff)
        window = [e for e in inlet_events[BELL].after(cutoff) if cutoff_dt < e.timestamp <= upper and (e.extra or {}).get("partitions")]
        merged = js.merge_bells(window)
        if not merged:
            raise AirflowSkipException("no bells with partitions in this window")   # a skipped task emits no event
        window_end = _iso(max(e.timestamp for e in window))
        key = f"window/{window_end}"
        store.set(key, {"partitions": merged, "bells": len(window), "window_end": window_end})
        outlet_events[DEBOUNCED].extra = {"window": key, "window_end": window_end, "n_partitions": len(merged), "bells": len(window)}
        return {"window": key, "n_partitions": len(merged), "bells": len(window)}

    @task(inlets=[BELL, DEBOUNCED], outlets=[DEBOUNCE_TICK], trigger_rule="all_done")
    def sweep(*, inlet_events=None, outlet_events=None) -> None:
        """Ring the private tick only when bells with partitions are left after the cutoff forward just published (inside the margin,
        or forward failed). Skips in the usual case, so the chain ends. A declared outlet emits on every success (LESSON 1), hence
        a task of its own that skips."""
        cutoff = _latest_window_end(inlet_events) or _iso(datetime.now(timezone.utc) - timedelta(days=1))
        cutoff_dt = datetime.fromisoformat(cutoff)
        pending = [e for e in inlet_events[BELL].after(cutoff) if e.timestamp > cutoff_dt and (e.extra or {}).get("partitions")]
        # Only bells stamped before this run started can be stranded: they coalesced into the queue row this run consumed. A bell
        # stamped later made a new queue row, so a next run is coming for it anyway; ticking for it would only push that run's
        # deadline back (run_after = the newest queue row).
        started = get_current_context()["dag_run"].start_date
        stranded = [e for e in pending if started is None or e.timestamp <= started]
        if not stranded:
            raise AirflowSkipException("no stranded bells")
        outlet_events[DEBOUNCE_TICK].extra = {"tick": True, "stranded_bells": len(stranded)}

    plan() >> hold >> forward() >> sweep()


@dag(dag_id="v2_batcher", schedule=(DEBOUNCED | BATCHER_TICK), catchup=False, max_active_runs=K, tags=["v2"])
def v2_batcher():
    @task(inlets=[PNL], retries=2, retry_delay=timedelta(seconds=5))
    def claim(run_id: str | None = None, *, triggering_asset_events=None, asset_state_store=None) -> list[dict]:
        store = asset_state_store[PNL]
        events = list(triggering_asset_events[DEBOUNCED] if triggering_asset_events else [])
        wanted = js.candidates(events, store)                   # resolves window pointers; ticks fall out
        return js.claim(store, wanted, run_id)                  # reads done/failed only when there is something to decide

    @task(inlets=[PNL], pool="v2_spark", retries=1, retry_delay=timedelta(seconds=5), multiple_outputs=False)
    def spark(batch: dict, *, asset_state_store=None) -> dict:
        store = asset_state_store[PNL]
        js.running(store, batch["batch_id"])
        try:
            time.sleep(ENGINE_STARTUP_S + PER_PARTITION_S * len(batch["partitions"]))
            bad = sorted(poison() & set(batch["partitions"]))
            if bad:
                raise ValueError(f"engine failed on {bad}")
        except Exception as exc:
            ctx = get_current_context()
            if ctx["ti"].try_number > ctx["task"].retries:      # this was the last attempt Airflow will make
                culprits = [p for p in batch["partitions"] if p in str(exc)]   # the engine named them (stand-in: "engine failed on [...]")
                js.fail(store, batch, str(exc), culprits)
            raise
        return batch

    @task(inlets=[PNL], outlets=[PNL], retries=2, retry_delay=timedelta(seconds=5), multiple_outputs=False)
    def publish(batch: dict, *, outlet_events=None, asset_state_store=None) -> dict:
        """ONE event per batch, pointing at the batch key (D21): {"batch": "batch/<run>#<i>", "n_partitions", "datasets"}.
        A downstream Dag scheduled on PNL reads the key for the partition list. (Keyed events were dropped in D20: 100k of them
        took 7 min to register in one api-server request; a 2 MB payload extra rode on every task start of every consumer: D21.)"""
        result = js.publish(asset_state_store[PNL], batch)
        outlet_events[PNL].extra = {"batch": result["batch"], "n_partitions": result["n_partitions"], "datasets": result["datasets"]}
        return result

    @task(inlets=[PNL], outlets=[FINISHED], trigger_rule="all_done", multiple_outputs=False)
    def finalize(batches: list[dict], *, asset_state_store=None, outlet_events=None) -> dict:
        """Summarise this run's batch keys (writes only its own keys) and tell the ledger which keys are final.
        A run that claimed nothing skips here, emits nothing, and the chain ends."""
        result = js.summarize(asset_state_store[PNL], batches or [])
        if not result["batch_ids"]:
            raise AirflowSkipException("nothing claimed, nothing to fold")
        outlet_events[FINISHED].extra = {"batch_ids": result["batch_ids"]}
        return result

    @task(outlets=[BATCHER_TICK], trigger_rule="all_done")
    def tick(result: dict, *, outlet_events=None) -> None:
        """Empty event on the batcher's private tick asset when this run did work: Airflow attaches to a run only the events
        stamped before the run's queue row (LESSON 3), so a debounced event that landed while this run was active would otherwise
        wait for the event after it. The batcher cannot read a window by cutoff (K overlapping runs, no single writer), so it ticks.
        Skips when nothing was claimed (a skipped task emits nothing): one empty run per working run, then the chain ends."""
        if not result or not result.get("batch_ids"):
            raise AirflowSkipException("nothing claimed, no tick")
        outlet_events[BATCHER_TICK].extra = {"tick": True}

    @task(outlets=[DEBOUNCED], trigger_rule="all_done")
    def ring_retry(result: dict, *, outlet_events=None) -> None:
        """Blast-radius rule a+c: healthy partitions of a failed batch (and every partition of a job that never published) ring
        again, as a small payload event. Skips when there is nothing to retry."""
        if not result or not result.get("retry"):
            raise AirflowSkipException("nothing to retry")
        outlet_events[DEBOUNCED].extra = {"partitions": result["retry"], "retry_of": "this run's failed batches"}

    @task(trigger_rule="all_done")
    def check(result: dict) -> None:
        """Colours the run: the bookkeeping is done by now; this task fails only so the run shows red when partitions failed."""
        if result and result.get("failed"):
            raise RuntimeError(f"{result['failed']} partition(s) moved to failed; see the batch keys and the failed dict")

    @task_group
    def run_batch(batch: dict):
        """One group instance per batch: spark[i] -> publish[i]. A mapped task group keeps the pairs independent (LESSON 2)."""
        publish(spark(batch))

    batches = claim()
    groups = run_batch.expand(batch=batches)
    fin = finalize(batches)
    groups >> fin
    tick(fin)
    ring_retry(fin)
    check(fin)


@dag(dag_id="v2_ledger", schedule=(FINISHED | LEDGER_TICK), catchup=False, max_active_runs=1, tags=["v2"])
def v2_ledger():
    """The only writer of done and failed. One run at a time; it reads the FINISHED events in (ledger/cutoff, now - margin] from the
    event log (not only the events attached to the run). The margin matters because K finalizes commit concurrently: an event
    can become visible after a later-stamped one, and a cutoff past it would skip it forever. Events left inside the margin are
    swept by the private tick (D24). `settle` first waits (deferred) until the triggering event is older than the margin, so the
    usual run folds everything in one pass and the tick stays rare. done/failed are written before the cutoff: a crash between
    them re-folds, which is idempotent."""
    settle = TimeDeltaSensor(task_id="settle", delta=timedelta(seconds=LEDGER_MARGIN_S), deferrable=True)

    @task(inlets=[FINISHED, PNL], retries=2, retry_delay=timedelta(seconds=5), multiple_outputs=False)
    def fold(*, inlet_events=None, asset_state_store=None) -> dict:
        store = asset_state_store[PNL]
        cutoff = store.get("ledger/cutoff") or _iso(datetime.now(timezone.utc) - timedelta(days=1))
        cutoff_dt = datetime.fromisoformat(cutoff)
        upper = datetime.now(timezone.utc) - timedelta(seconds=LEDGER_MARGIN_S)
        after = [e for e in inlet_events[FINISHED].after(cutoff) if e.timestamp > cutoff_dt]
        events = [e for e in after if e.timestamp <= upper]
        ids = [bid for e in events for bid in ((e.extra or {}).get("batch_ids") or [])]
        result = js.fold(store, ids) if ids else {"done": 0, "failed": 0, "partitions_tracked": None}
        if events:
            store.set("ledger/cutoff", _iso(max(e.timestamp for e in events)))
        started = get_current_context()["dag_run"].start_date
        left = [e for e in after if e.timestamp > upper]
        stranded = [e for e in left if started is None or e.timestamp <= started]   # later ones made a new queue row
        return {**result, "finished_events": len(events), "pending": len(left), "stranded": len(stranded)}

    @task(outlets=[LEDGER_TICK], trigger_rule="all_done")
    def sweep(result: dict, *, outlet_events=None) -> None:
        """Ring the ledger's private tick only when FINISHED events stamped before this run started were left inside the margin
        (no queue row will start a run for them); skips otherwise."""
        if not result or not result.get("stranded"):
            raise AirflowSkipException("nothing stranded")
        outlet_events[LEDGER_TICK].extra = {"tick": True, "stranded": result["stranded"]}

    folded = fold()
    settle >> folded
    sweep(folded)


@dag(dag_id="v2_exceptions", schedule=(PNL | EXC_TICK), catchup=False, max_active_runs=1, tags=["v2", "example-consumer"])
def v2_exceptions():
    """EXAMPLE of downstream chaining, the shape every downstream Dag repeats. Scheduled on the PnL asset: a finished batch is
    one event pointing at its batch key, so this Dag runs at batch grain and can hand the whole partition list to one job of its
    own. Two rules for a consumer that uses triggering events: fold with js.candidates (max version per partition and dataset;
    Airflow does not order triggering_asset_events) and ring an empty tick on your OWN asset when you did work (LESSON 3/5; a
    private asset so consumers of a shared asset do not wake each other)."""
    @task(inlets=[PNL], multiple_outputs=False)
    def review(*, triggering_asset_events=None, asset_state_store=None) -> dict:
        events = list(triggering_asset_events[PNL] if triggering_asset_events else [])
        partitions = js.candidates(events, asset_state_store[PNL])      # resolves batch pointers; ticks fall out
        return {"batches": [(e.extra or {}).get("batch") for e in events if (e.extra or {}).get("batch")],
                "partitions": len(partitions), "sample": {p: v["inputs"] for p, v in sorted(partitions.items())[:3]}}

    @task(outlets=[EXC_TICK])
    def sweep(result: dict, *, outlet_events=None) -> None:
        if not result or not result.get("partitions"):
            raise AirflowSkipException("nothing reviewed, no tick")
        outlet_events[EXC_TICK].extra = {"tick": True}

    sweep(review())


@dag(dag_id="v2_requeue", schedule=None, catchup=False, params={"partitions": Param(["ACC42"], type="array")}, tags=["v2"])
def v2_requeue():
    @task(inlets=[PNL], outlets=[DEBOUNCED], do_xcom_push=False)
    def ring(params: dict | None = None, *, outlet_events=None, asset_state_store=None) -> None:
        failed = asset_state_store[PNL].get("failed") or {}
        wanted = {p: failed[p] for p in params["partitions"] if p in failed}
        if not wanted:
            raise AirflowSkipException("none of these partitions is in failed")   # a skipped task rings no bell
        outlet_events[DEBOUNCED].extra = {"partitions": wanted, "requeue": True}
    ring()


v2_land()
v2_debounce()
v2_batcher()
v2_ledger()
v2_exceptions()
v2_requeue()
