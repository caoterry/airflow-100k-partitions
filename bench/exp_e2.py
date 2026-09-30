#!/usr/bin/env python
"""E2 — rerun semantics for AND vs OR asset schedules + latest-per-input gate (Airflow 3.3.2).
Sequence: a1 v1, a2 v1, a1 v2, a3 v1, a3 v2, then a burst of 5 events on a1 (v3..v7)."""
from __future__ import annotations
import sys, time
sys.path.insert(0, __import__("os").path.dirname(__file__))
import harness as H

tok = H.token(); c = H.client(tok)
conn = H.db(); conn.autocommit = True; cur = conn.cursor()
T0 = time.time()
def t(): return f"{time.time()-T0:6.1f}s"

def emit(asset, version, wait=True):
    rid = f"e2_{asset}_v{version}_{int(time.time()*1000)}"
    r = c.post("/api/v2/dags/exp_e2_producer/dagRuns", json={"dag_run_id": rid, "conf": {"asset": asset, "version": version}, "logical_date": None}); r.raise_for_status()
    print(f"[{t()}] EMIT {asset} v{version}")
    if wait:
        end = time.time() + 90
        while time.time() < end:
            cur.execute("select state from dag_run where dag_id='exp_e2_producer' and run_id=%s", (rid,))
            if cur.fetchone()[0] in ("success", "failed"): break
            time.sleep(1)

def snapshot(title):
    print(f"[{t()}] --- {title}")
    for d in ("exp_e2_consumer_and", "exp_e2_consumer_or"):
        cur.execute("select run_id, state, to_char(queued_at,'HH24:MI:SS') from dag_run where dag_id=%s order by id", (d,))
        rows = cur.fetchall(); print(f"        {d}: {len(rows)} runs " + ", ".join(f"{s}@{q}" for _, s, q in rows))
    cur.execute("select a.name, count(*), max((e.extra->>'version')::int) from asset_event e join asset a on a.id=e.asset_id where a.name like 'exp_e2_%' group by 1 order by 1")
    print("        events per asset (count, max version):", cur.fetchall())

def settle(sec=20):
    time.sleep(sec)

for st in ["delete from dagrun_asset_event where dag_run_id in (select id from dag_run where dag_id like 'exp_e2_%')",
           "delete from asset_dag_run_queue where target_dag_id like 'exp_e2_%'",
           "delete from task_instance where dag_id like 'exp_e2_%'", "delete from dag_run where dag_id like 'exp_e2_%'",
           "delete from asset_event where asset_id in (select id from asset where name like 'exp_e2_%')"]:
    cur.execute(st)
print("clean slate")

emit("a1", 1); settle(); snapshot("after a1 v1")
emit("a2", 1); settle(); snapshot("after a2 v1")
emit("a1", 2); settle(); snapshot("after a1 v2 (a3 still missing)")
emit("a3", 1); settle(25); snapshot("after a3 v1 — first complete set")
emit("a3", 2); settle(25); snapshot("after a3 v2 — the 'red X' case: only one input re-arrived")
for v in range(3, 8): emit("a1", v, wait=False)
settle(60); snapshot("after a burst of 5 events on a1 (v3..v7) — conflation?")

print("\n=== what the OR consumer's gate/calc logged (last runs) ===")
import glob, os
for f in sorted(glob.glob(os.path.expanduser("airflow_home/logs/dag_id=exp_e2_consumer_or/run_id=*/task_id=gate/attempt=1.log")), key=os.path.getmtime)[-3:]:
    print(f.split("run_id=")[1].split("/")[0], "->", [l.split('"event":"')[1].split('"')[0][:140] for l in open(f) if '"event":"' in l and ('latest per input' in l or 'READY' in l)])
for f in sorted(glob.glob("airflow_home/logs/dag_id=exp_e2_consumer_or/run_id=*/task_id=calc/attempt=1.log"), key=os.path.getmtime)[-2:]:
    print(f.split("run_id=")[1].split("/")[0], "->", [l.split('"event":"')[1].split('"')[0][:160] for l in open(f) if 'calc with versions' in l])
