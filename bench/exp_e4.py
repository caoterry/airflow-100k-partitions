#!/usr/bin/env python
"""E4 driver — batcher with per-account lineage + state store. Sequence:
  emit ACC1..ACC6 v1 -> first cron batch claims them -> while it runs: emit ACC1 v2 + ACC7 v1
  -> next batch claims ACC7 only (ACC1 in flight) -> following batch claims ACC1 v2. Then inspect lineage & state store."""
from __future__ import annotations
import sys, time, json
sys.path.insert(0, __import__("os").path.dirname(__file__))
import harness as H

tok = H.token(); c = H.client(tok)
conn = H.db(); conn.autocommit = True; cur = conn.cursor()
T0 = time.time()
def t(): return f"{time.time()-T0:6.1f}s"

def emit(accounts, version):
    rid = f"e4_{'_'.join(accounts)}_v{version}_{int(time.time()*1000)}"
    r = c.post("/api/v2/dags/rev_producer/dagRuns", json={"dag_run_id": rid, "conf": {"accounts": accounts, "version": version}, "logical_date": None}); r.raise_for_status()
    print(f"[{t()}] EMIT {accounts} v{version}")

def batcher_runs():
    cur.execute("select run_id, state, to_char(start_date,'HH24:MI:SS') from dag_run where dag_id='rev_batcher' order by id")
    return cur.fetchall()

def wait(pred_sql, timeout, what):
    end = time.time() + timeout
    while time.time() < end:
        cur.execute(pred_sql)
        if cur.fetchone()[0]: return True
        time.sleep(2)
    print(f"[{t()}] TIMEOUT: {what}"); return False

def claim_logs():
    import glob, os
    out = []
    for f in sorted(glob.glob("airflow_home/logs/dag_id=rev_batcher/run_id=*/task_id=claim/attempt=1.log"), key=os.path.getmtime):
        rid = f.split("run_id=")[1].split("/")[0][-25:]
        lines = [l for l in open(f) if "newest=" in l or "Traceback" in l or '"level":"error"' in l]
        for l in lines:
            if "newest=" in l: out.append((rid, l.split("newest=")[1].split('"')[0][:200]))
            else: out.append((rid, "ERROR " + l[:200]))
    return out

# clean slate for the E4 DAGs
c.patch("/api/v2/dags/rev_batcher", json={"is_paused": True})
for st in ["delete from partitioned_asset_key_log where target_dag_id='rev_pnl_consumer'",
           "delete from asset_partition_dag_run where target_dag_id='rev_pnl_consumer'",
           "delete from dagrun_asset_event where dag_run_id in (select id from dag_run where dag_id in ('rev_pnl_consumer','rev_nonpart_consumer','rev_batcher'))",
           "delete from asset_dag_run_queue where target_dag_id in ('rev_nonpart_consumer')",
           "delete from task_instance where dag_id like 'rev_%'", "delete from dag_run where dag_id like 'rev_%'",
           "delete from asset_event where asset_id in (select id from asset where name in ('rev_positions','rev_pnl'))",
           "delete from asset_state_store where asset_id in (select id from asset where name in ('rev_positions','rev_pnl'))"]:
    try: cur.execute(st)
    except Exception as e: print("clean:", st[:60], "->", str(e).splitlines()[0])
print("clean slate; unpausing batcher (cron every minute)")
c.patch("/api/v2/dags/rev_batcher", json={"is_paused": False})

emit(["ACC1", "ACC2", "ACC3", "ACC4", "ACC5", "ACC6"], 1)
wait("select (select count(*) from task_instance where dag_id='rev_batcher' and task_id='spark' and state='running') >= 1", 150, "first batch running")
print(f"[{t()}] first batch is RUNNING; now ACC1 v2 + ACC7 v1 arrive")
emit(["ACC1"], 2); emit(["ACC7"], 1)
wait("select (select count(*) from task_instance where dag_id='rev_batcher' and task_id='publish' and state='success') >= 3", 260, "three batches published")
time.sleep(25)

print(f"\n[{t()}] === batcher runs ===")
for r in batcher_runs(): print("   ", r)
print("=== claim decisions per batcher run ===")
for rid, l in claim_logs(): print(f"   {rid}: {l}")
print("=== rev_pnl events (per-account lineage from ONE task per batch) ===")
cur.execute("select e.partition_key, e.extra->>'batch' batch, e.source_run_id, to_char(e.timestamp,'HH24:MI:SS') from asset_event e join asset a on a.id=e.asset_id where a.name='rev_pnl' order by e.id")
for r in cur.fetchall(): print("   ", r)
print("=== rev_pnl_consumer runs (one per account, partition_key set) ===")
cur.execute("select partition_key, state, run_type from dag_run where dag_id='rev_pnl_consumer' order by partition_key"); print("   ", cur.fetchall())
print("=== rev_nonpart_consumer runs (keyed events should NOT trigger it) ===")
cur.execute("select count(*) from dag_run where dag_id='rev_nonpart_consumer'"); print("   ", cur.fetchone()[0])
print("=== asset state store (REST GET /assets/{id}/state-store) ===")
cur.execute("select id from asset where name='rev_positions'"); aid = cur.fetchone()[0]
r = c.get(f"/api/v2/assets/{aid}/state-store", params={"limit": 20}); print("   status", r.status_code)
for e in (r.json().get("entries") or r.json().get("items") or r.json().get("asset_state_store_entries") or [])[:12]:
    print("   ", e.get("key"), "->", e.get("value"))
c.patch("/api/v2/dags/rev_batcher", json={"is_paused": True}); print("batcher paused")
