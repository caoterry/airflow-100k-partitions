"""E4 — the recommended shape for account-grain work: a level-triggered BATCHER with per-account lineage and status.

Assets
  rev_positions  (input)  producers emit one keyed event per firm account (partition_key=account, extra={"version": n})
  rev_pnl        (output) the batcher emits one keyed event per processed account -> per-account lineage downstream

DAGs
  rev_producer            PartitionedAtRuntime; conf {"accounts": [...], "version": n}
  rev_batcher             cron every minute, max_active_runs=2  (keyed events never trigger non-partitioned DAGs, so the
                          batcher is level-triggered: it reads the event log and the asset state store)
      claim   (short_circuit) ready = accounts whose newest input event version > last processed version and not in flight;
                              records {status: running, batch: run_id, version} per account in the asset state store
      spark   one job for the whole batch (simulated, 20 s)
      publish outlets=[rev_pnl]: add_partitions(claimed accounts); per-account {status: done, ...} in the state store
  rev_pnl_consumer        PartitionedAssetTimetable(rev_pnl): one run per account -> shows lineage from the batch run
  rev_nonpart_consumer    plain asset schedule on rev_positions: demonstrates that keyed events do NOT trigger it
"""
from __future__ import annotations

import os, sys, time
from datetime import datetime, timezone
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, IdentityMapper, Param, PartitionedAssetTimetable, PartitionedAtRuntime, dag, task

REV_POSITIONS = Asset(name="rev_positions", uri="bench://rev/positions")
REV_PNL = Asset(name="rev_pnl", uri="bench://rev/pnl")


@dag(dag_id="rev_producer", schedule=PartitionedAtRuntime(), catchup=False,
     params={"accounts": Param(["ACC1"], type="array"), "version": Param(1, type="integer")}, tags=["exp", "e4"])
def rev_producer():
    @task(outlets=[REV_POSITIONS], do_xcom_push=False)
    def land(params: dict | None = None, *, outlet_events=None) -> None:
        # one keyed event per account; the version rides in `extra`
        outlet_events[REV_POSITIONS].extra = {"version": int(params["version"])}
        outlet_events[REV_POSITIONS].add_partitions(list(params["accounts"]))
    land()


@dag(dag_id="rev_batcher", schedule="* * * * *", catchup=False, max_active_runs=2, tags=["exp", "e4"])
def rev_batcher():
    @task.short_circuit(inlets=[REV_POSITIONS])
    def claim(inlet_events=None, asset_state_store=None, run_id=None) -> list[str]:
        store = asset_state_store[REV_POSITIONS]
        # newest input version per account from the event log (keyed events; extra carries the business version)
        newest: dict[str, int] = {}
        for ev in inlet_events[REV_POSITIONS]:
            if ev.partition_key:
                newest[ev.partition_key] = max(newest.get(ev.partition_key, 0), int((ev.extra or {}).get("version", 0)))
        claimed: list[str] = []
        skipped_in_flight, up_to_date = [], []
        for acct, ver in sorted(newest.items()):
            rec = store.get(f"acct/{acct}") or {}
            if rec.get("status") == "running":
                skipped_in_flight.append(acct); continue
            if int(rec.get("version", -1)) >= ver:
                up_to_date.append(acct); continue
            store.set(f"acct/{acct}", {"status": "running", "version": ver, "batch": run_id, "claimed_at": datetime.now(timezone.utc).isoformat()})
            claimed.append(acct)
        print(f"newest={newest} claimed={claimed} in_flight={skipped_in_flight} up_to_date={up_to_date}")
        return claimed  # empty list is falsy -> short-circuit, no batch run

    @task
    def spark(claimed: list[str]) -> list[str]:
        print(f"one Spark/Glue job for {len(claimed)} accounts: {claimed}")
        time.sleep(20)
        return claimed

    @task(inlets=[REV_POSITIONS], outlets=[REV_PNL])   # inlets: the state-store accessor is scoped to declared assets
    def publish(claimed: list[str], *, outlet_events=None, asset_state_store=None, run_id=None) -> None:
        outlet_events[REV_PNL].extra = {"batch": run_id}
        outlet_events[REV_PNL].add_partitions(claimed)          # per-account lineage from one task
        store = asset_state_store[REV_POSITIONS]
        for acct in claimed:
            rec = store.get(f"acct/{acct}") or {}
            rec.update({"status": "done", "finished_at": datetime.now(timezone.utc).isoformat()})
            store.set(f"acct/{acct}", rec)

    @task(inlets=[REV_POSITIONS], trigger_rule="one_failed")
    def release(claimed: list[str], *, asset_state_store=None, run_id=None) -> None:
        """If the batch fails, give the accounts back (otherwise they stay 'running' forever)."""
        store = asset_state_store[REV_POSITIONS]
        for acct in claimed or []:
            rec = store.get(f"acct/{acct}") or {}
            if rec.get("batch") == run_id and rec.get("status") == "running":
                rec.update({"status": "failed"}); store.set(f"acct/{acct}", rec)
        print("released", claimed)

    c = claim(); s = spark(c); publish(s); release(c) << [s]


@dag(dag_id="rev_pnl_consumer",
     schedule=PartitionedAssetTimetable(assets=REV_PNL, default_partition_mapper=IdentityMapper()),
     catchup=False, tags=["exp", "e4"])
def rev_pnl_consumer():
    @task
    def downstream(dag_run=None, triggering_asset_events=None) -> None:
        srcs = [(e.source_dag_id, e.source_run_id, e.partition_key) for v in (triggering_asset_events or {}).values() for e in v]
        print("account", dag_run.partition_key, "produced by", srcs)
    downstream()


@dag(dag_id="rev_nonpart_consumer", schedule=REV_POSITIONS, catchup=False, tags=["exp", "e4"])
def rev_nonpart_consumer():
    EmptyOperator(task_id="never_triggered_by_keyed_events")


rev_producer(); rev_batcher(); rev_pnl_consumer(); rev_nonpart_consumer()
