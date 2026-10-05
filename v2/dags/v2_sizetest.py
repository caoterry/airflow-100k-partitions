"""Test only: how long does one state-store get/set take for a dict of N accounts (the `done` dict of variant C)?"""
from __future__ import annotations
import time
from airflow.sdk import Asset, Param, dag, task

POSITIONS = Asset(name="v2_positions", uri="v2://positions")

@dag(dag_id="v2_sizetest", schedule=None, catchup=False, params={"n": Param(100000, type="integer")}, tags=["v2", "test"])
def v2_sizetest():
    @task(inlets=[POSITIONS])
    def roundtrip(params: dict | None = None, *, asset_state_store=None) -> dict:
        store = asset_state_store[POSITIONS]
        n = int(params["n"]); big = {f"ACC{i:06d}": 1 for i in range(n)}
        t0 = time.time(); store.set("sizetest", big); t_set = time.time() - t0
        t0 = time.time(); back = store.get("sizetest"); t_get = time.time() - t0
        store.delete("sizetest")
        return {"n": n, "set_s": round(t_set, 2), "get_s": round(t_get, 2), "ok": len(back) == n}
    roundtrip()
v2_sizetest()
