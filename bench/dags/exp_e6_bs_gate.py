"""E6 — the balance-sheet readiness model: `all(reference data for as-of D) and any(root data for D)`, first and every run.

Assets: bs_ref_rates, bs_ref_fx (reference), bs_root_positions, bs_root_cashflows (root). Events carry
extra = {"as_of": "2026-09-30", "version": n, "path": ...}.
bs_pnl (OR over all four inputs, max_active_runs=1):
    gate    for the as_of named in the triggering event(s): every reference input has >= 1 event for that date AND at
            least one root input has one -> proceed, else skip. Picks max(version) per input for that date.
    calc    "computes" with the chosen versions; emits bs_pnl_out with extra = {as_of, inputs used, roots included}.
Demonstrates: first run needs all refs + any root; later runs on any arrival (root or a new reference version);
events for a different business date do not satisfy the gate (date scoping).
"""
from __future__ import annotations
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from airflow.sdk import Asset, Param, dag, task

REFS = {"rates": Asset(name="bs_ref_rates", uri="bench://bs/ref/rates"), "fx": Asset(name="bs_ref_fx", uri="bench://bs/ref/fx")}
ROOTS = {"positions": Asset(name="bs_root_positions", uri="bench://bs/root/positions"), "cashflows": Asset(name="bs_root_cashflows", uri="bench://bs/root/cashflows")}
ALL = {**REFS, **ROOTS}
OUT = Asset(name="bs_pnl_out", uri="bench://bs/pnl")


@dag(dag_id="bs_producer", schedule=None, catchup=False,
     params={"input": Param("rates", type="string", enum=list(ALL)), "as_of": Param("2026-09-30", type="string"), "version": Param(1, type="integer")},
     tags=["exp", "e6"])
def bs_producer():
    @task.branch
    def pick(params: dict | None = None) -> str:
        return f"land_{params['input']}"

    def make(name: str, asset: Asset):
        @task(task_id=f"land_{name}", outlets=[asset], do_xcom_push=False)
        def _land(params: dict | None = None, *, outlet_events=None) -> None:
            outlet_events[asset].extra = {"as_of": params["as_of"], "version": int(params["version"]), "path": f"s3://bs/{name}/{params['as_of']}/v{params['version']}"}
        return _land()
    pick() >> [make(n, a) for n, a in ALL.items()]


@dag(dag_id="bs_pnl", schedule=(REFS["rates"] | REFS["fx"] | ROOTS["positions"] | ROOTS["cashflows"]), catchup=False, max_active_runs=1, tags=["exp", "e6"])
def bs_pnl():
    @task.short_circuit(inlets=list(ALL.values()))
    def gate(inlet_events=None, triggering_asset_events=None) -> dict:
        # which business date(s) woke us up
        dates = {e.extra.get("as_of") for evs in (triggering_asset_events or {}).values() for e in evs if e.extra}
        if not dates:
            print("no as_of on the triggering events; skipping"); return {}
        as_of = sorted(dates)[-1]
        latest = {}
        for name, asset in ALL.items():
            best = None
            for e in inlet_events[asset]:
                x = e.extra or {}
                if x.get("as_of") == as_of and (best is None or int(x.get("version", 0)) > int(best.get("version", 0))):
                    best = x
            latest[name] = best
        refs_ok = all(latest[n] is not None for n in REFS)
        roots_in = [n for n in ROOTS if latest[n] is not None]
        ready = refs_ok and bool(roots_in)
        versions = {k: (v or {}).get("version") for k, v in latest.items()}
        print(f"as_of={as_of} refs_ok={refs_ok} roots_in={roots_in} versions={versions} -> {'RUN' if ready else 'SKIP'}")
        return {"as_of": as_of, "inputs": {k: v for k, v in latest.items() if v}, "roots": roots_in} if ready else {}

    @task(outlets=[OUT])
    def calc(plan: dict, *, outlet_events=None) -> None:
        print("calc", plan["as_of"], "roots", plan["roots"], "versions", {k: v["version"] for k, v in plan["inputs"].items()})
        outlet_events[OUT].extra = {"as_of": plan["as_of"], "roots": plan["roots"], "versions": {k: v["version"] for k, v in plan["inputs"].items()}}

    calc(gate())


bs_producer(); bs_pnl()
