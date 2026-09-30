"""E5 — DYNAMIC, capacity-aware batching for account-grain PnL on Airflow 3.3 primitives.

Difference from E4: batch sizes are decided at runtime from (ready set, arrival rate, free capacity, oldest wait),
and capacity is an Airflow POOL ("spark_jobs", K slots) that the mapped `spark` tasks occupy, so excess batches queue
in Airflow (back-pressure) instead of overrunning the engine.

  rev5_producer   PartitionedAtRuntime: emits keyed events on rev5_positions (conf accounts/version)
  rev5_batcher    cron every minute, max_active_runs=4
      claim    ready accounts (event log via inlet_events.after(watermark) + state store), policy -> list of batches
      spark    .expand(batch=...) pool="spark_jobs": sleeps startup_s + per_account_s * len(batch)   (one Glue job)
      publish  .expand over spark output: add_partitions(batch) on rev5_pnl + per-account status
  rev5_pnl_consumer  partitioned consumer (lineage check)

Policy knobs live in the state store key "policy" on rev5_positions (set by the driver), e.g.
  {"mode": "adaptive"|"fixed", "K": 3, "startup_s": 15, "per_account_s": 0.5, "b_min": 3, "b_max": 60, "t_max_s": 90, "fixed_size": 10}
"""
from __future__ import annotations

import math, os, sys, time
from datetime import datetime, timedelta, timezone
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow.sdk import Asset, IdentityMapper, Param, PartitionedAssetTimetable, PartitionedAtRuntime, dag, task

POS = Asset(name="rev5_positions", uri="bench://rev5/positions")
PNL = Asset(name="rev5_pnl", uri="bench://rev5/pnl")
DEFAULT_POLICY = {"mode": "adaptive_sla", "K": 3, "startup_s": 15.0, "per_account_s": 0.5, "b_min": 3, "b_max": 60, "t_max_s": 90, "fixed_size": 10, "target_s": 90}


def plan_batches(ready: list[tuple[str, float]], running_batches: int, lam: float, pol: dict, now: float) -> list[list[str]]:
    """ready = [(account, first_seen_epoch)], oldest first. Returns the batches to dispatch now (possibly none)."""
    if not ready:
        return []
    K, S, p = int(pol["K"]), float(pol["startup_s"]), float(pol["per_account_s"])
    b_min, b_max, t_max = int(pol["b_min"]), int(pol["b_max"]), float(pol["t_max_s"])
    oldest_wait = now - ready[0][1]
    accounts = [a for a, _ in ready]
    if pol.get("mode") == "fixed":
        size = int(pol["fixed_size"])
        if len(accounts) < size and oldest_wait <= t_max:
            return []
        return [accounts[i:i + size] for i in range(0, len(accounts), size)]
    free = max(K - running_batches, 0)
    if free == 0:
        return []                                     # saturated: let the ready set accumulate (bigger batches later)
    b_lat = math.sqrt(2 * lam * S / p) if p > 0 else b_max
    denom = K - lam * p
    b_cap = (lam * S / denom) * 1.25 if denom > 0 else b_max          # capacity floor: K slots must sustain lam
    if pol.get("mode") == "adaptive_sla":
        target = float(pol.get("target_s", 120))
        b_sla = (target - S) / (1.0 / lam + p) if target > S else b_min   # largest batch that still meets the target
        b_target = int(max(b_min, min(b_max, max(b_cap, b_sla))))
    else:
        b_target = int(max(b_min, min(b_max, max(b_lat, b_cap))))
    if len(accounts) < b_min and oldest_wait <= t_max:
        return []
    n_jobs = min(free, max(1, math.ceil(len(accounts) / b_target)))
    per = min(b_max, math.ceil(len(accounts) / n_jobs))
    return [accounts[i:i + per] for i in range(0, len(accounts), per)][:n_jobs]


@dag(dag_id="rev5_producer", schedule=PartitionedAtRuntime(), catchup=False,
     params={"accounts": Param(["ACC1"], type="array"), "version": Param(1, type="integer")}, tags=["exp", "e5"])
def rev5_producer():
    @task(outlets=[POS], do_xcom_push=False)
    def land(params: dict | None = None, *, outlet_events=None) -> None:
        outlet_events[POS].extra = {"version": int(params["version"])}
        outlet_events[POS].add_partitions(list(params["accounts"]))
    land()


@dag(dag_id="rev5_batcher", schedule="* * * * *", catchup=False, max_active_runs=4, tags=["exp", "e5"])
def rev5_batcher():
    @task.short_circuit(inlets=[POS])
    def claim(inlet_events=None, asset_state_store=None, run_id=None) -> list[list[str]]:
        store = asset_state_store[POS]
        pol = {**DEFAULT_POLICY, **(store.get("policy") or {})}
        now = time.time()
        # event log since (watermark - t_max) so late-registered events are not missed; newest version + first-seen per account
        wm = store.get("watermark")
        since = (datetime.fromisoformat(wm) - timedelta(seconds=float(pol["t_max_s"]) * 2)) if wm else datetime(2000, 1, 1, tzinfo=timezone.utc)
        newest: dict[str, tuple[int, float]] = {}
        recent = 0
        for ev in inlet_events[POS].after(since.isoformat()):
            if not ev.partition_key:
                continue
            ver = int((ev.extra or {}).get("version", 0)); ts = ev.timestamp.timestamp() if hasattr(ev.timestamp, "timestamp") else now
            v0 = newest.get(ev.partition_key)
            newest[ev.partition_key] = (max(ver, v0[0]) if v0 else ver, min(ts, v0[1]) if v0 else ts)
            if now - ts <= 600: recent += 1
        lam = max(recent / 600.0, 1e-3)
        ready, in_flight, running_batches = [], [], set()
        for acct, (ver, first_seen) in newest.items():
            rec = store.get(f"acct/{acct}") or {}
            if rec.get("status") == "running":
                in_flight.append(acct); running_batches.add(rec.get("batch")); continue
            if rec.get("status") == "done" and int(rec.get("version", -1)) >= ver:
                continue                                   # failed/released accounts are re-claimed
            ready.append((acct, first_seen))
        ready.sort(key=lambda x: x[1])
        batches = plan_batches(ready, len(running_batches), lam, pol, now)
        for bi, batch in enumerate(batches):
            for acct in batch:
                store.set(f"acct/{acct}", {"status": "running", "version": newest[acct][0], "batch": f"{run_id}#{bi}", "claimed_at": datetime.now(timezone.utc).isoformat()})
        if newest:
            store.set("watermark", datetime.now(timezone.utc).isoformat())
        print(f"policy={pol['mode']} lam={lam:.3f}/s ready={len(ready)} in_flight={len(in_flight)} running_batches={len(running_batches)} "
              f"-> {len(batches)} batches sizes={[len(b) for b in batches]} oldest_wait={(now - ready[0][1]) if ready else 0:.0f}s")
        return batches

    @task(pool="spark_jobs", inlets=[POS])   # inlets: state-store accessor is scoped to declared assets
    def spark(batch: list[str], asset_state_store=None) -> list[str]:
        pol = {**DEFAULT_POLICY, **(asset_state_store[POS].get("policy") or {})}
        dur = float(pol["startup_s"]) + float(pol["per_account_s"]) * len(batch)
        print(f"one Spark/Glue job for {len(batch)} accounts, {dur:.0f}s")
        time.sleep(dur)
        return batch

    @task(inlets=[POS], outlets=[PNL])
    def publish(batch: list[str], *, outlet_events=None, asset_state_store=None) -> None:
        outlet_events[PNL].extra = {"batch_size": len(batch)}
        outlet_events[PNL].add_partitions(batch)
        store = asset_state_store[POS]
        for acct in batch:
            rec = store.get(f"acct/{acct}") or {}
            rec.update({"status": "done", "finished_at": datetime.now(timezone.utc).isoformat()})
            store.set(f"acct/{acct}", rec)

    @task(inlets=[POS], trigger_rule="one_failed")
    def release(batches: list[list[str]], *, asset_state_store=None) -> None:
        store = asset_state_store[POS]
        for batch in batches or []:
            for acct in batch:
                rec = store.get(f"acct/{acct}") or {}
                if rec.get("status") == "running":
                    rec["status"] = "failed"; store.set(f"acct/{acct}", rec)

    b = claim()
    s = spark.override(task_id="spark").expand(batch=b)
    publish.expand(batch=s)
    release(b) << [s]


@dag(dag_id="rev5_pnl_consumer", schedule=PartitionedAssetTimetable(assets=PNL, default_partition_mapper=IdentityMapper()),
     catchup=False, tags=["exp", "e5"])
def rev5_pnl_consumer():
    from airflow.providers.standard.operators.empty import EmptyOperator
    EmptyOperator(task_id="downstream")


rev5_producer(); rev5_batcher(); rev5_pnl_consumer()
