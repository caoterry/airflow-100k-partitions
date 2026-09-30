#!/usr/bin/env python
"""E6 driver — sequence of arrivals across two business dates; prints gate decisions and emitted results."""
from __future__ import annotations
import sys, time, os, glob
sys.path.insert(0, os.path.dirname(__file__))
import harness as H
tok = H.token(); c = H.client(tok); conn = H.db(); conn.autocommit = True; cur = conn.cursor()
T0 = time.time(); t = lambda: f"{time.time()-T0:6.1f}s"

def land(inp, as_of, version):
    rid = f"e6_{inp}_{as_of}_v{version}_{int(time.time()*1000)}"
    r = c.post("/api/v2/dags/bs_producer/dagRuns", json={"dag_run_id": rid, "conf": {"input": inp, "as_of": as_of, "version": version}, "logical_date": None}); r.raise_for_status()
    print(f"[{t()}] LAND {inp} {as_of} v{version}")
    end = time.time() + 120
    while time.time() < end:  # wait for producer + any bs_pnl run to settle
        cur.execute("select count(*) from dag_run where dag_id in ('bs_producer','bs_pnl') and state in ('queued','running')")
        if cur.fetchone()[0] == 0 and time.time() - T0 > 5: break
        time.sleep(2)
    time.sleep(8)

for st in ["delete from dagrun_asset_event where dag_run_id in (select id from dag_run where dag_id like 'bs_%')", "delete from asset_dag_run_queue where target_dag_id='bs_pnl'",
           "delete from task_instance where dag_id like 'bs_%'", "delete from dag_run where dag_id like 'bs_%'",
           "delete from asset_event where asset_id in (select id from asset where name like 'bs_%')"]:
    cur.execute(st)
print("clean slate")
D1, D2 = "2026-09-30", "2026-10-01"
land("rates", D1, 1)          # ref only -> skip
land("positions", D1, 1)      # ref + root but fx missing -> skip
land("fx", D1, 1)             # all refs + one root -> RUN (roots: positions)
land("cashflows", D1, 1)      # second root arrives -> RUN (roots: both)
land("rates", D1, 2)          # reference re-arrives v2 -> RUN with rates v2, others v1
land("positions", D2, 1)      # next business date: root only, refs for D2 missing -> SKIP (date scoping)
land("rates", D2, 1); land("fx", D2, 1)   # refs for D2 -> RUN for D2 with positions

print("\n=== gate decisions ===")
for f in sorted(glob.glob("airflow_home/logs/dag_id=bs_pnl/run_id=*/task_id=gate/attempt=1.log"), key=os.path.getmtime):
    for l in open(f):
        if "as_of=" in l and ("RUN" in l or "SKIP" in l): print("  ", l.split('"event":"')[1].split('"')[0][:200])
print("\n=== bs_pnl runs ===")
cur.execute("select state, count(*) from dag_run where dag_id='bs_pnl' group by 1"); print("  ", cur.fetchall())
print("\n=== results emitted (bs_pnl_out) ===")
cur.execute("select extra from asset_event where asset_id=(select id from asset where name='bs_pnl_out') order by id")
for (x,) in cur.fetchall(): print("  ", x)
