#!/usr/bin/env python
"""E1 — per-partition-key concurrency & conflation on the native partition path (Airflow 3.3.2).

Consumer exp_e1_consumer: PartitionedAssetTimetable + IdentityMapper, max_active_runs=2, task sleeps 45 s.
Sequence (mirrors the T=1..5 table in the MWAA discussion page):
  step 1  emit ACC1                     -> expect run #1 RUNNING
  step 2  emit ACC1 again (run #1 running) -> is a 2nd APDR + 2nd run created?  (per-key mutex? conflation?)
  step 3  emit ACC1 + ACC5 (2 runs running) -> does ACC5 start (fine-grained) or wait behind queued ACC1 (coarse)?
  step 4  emit ACC1 while an ACC1 run is QUEUED -> another APDR/run, or de-duplicated against the queued one?
"""
from __future__ import annotations
import sys, time
sys.path.insert(0, __import__("os").path.dirname(__file__))
import harness as H

tok = H.token(); c = H.client(tok)
conn = H.db(); conn.autocommit = True; cur = conn.cursor()
T0 = time.time()
def t(): return f"{time.time()-T0:6.1f}s"

def emit(keys):
    rid = f"e1_{'_'.join(keys)}_{int(time.time()*1000)}"
    r = c.post("/api/v2/dags/exp_e1_producer/dagRuns", json={"dag_run_id": rid, "conf": {"keys": keys}, "logical_date": None}); r.raise_for_status()
    print(f"[{t()}] EMIT {keys}")

def snapshot(title):
    cur.execute("select partition_key, created_dag_run_id is not null as fired, to_char(created_at,'HH24:MI:SS') from asset_partition_dag_run where target_dag_id='exp_e1_consumer' order by id")
    apdr = cur.fetchall()
    cur.execute("select partition_key, state, to_char(queued_at,'HH24:MI:SS'), to_char(start_date,'HH24:MI:SS'), to_char(end_date,'HH24:MI:SS') from dag_run where dag_id='exp_e1_consumer' order by id")
    runs = cur.fetchall()
    print(f"[{t()}] --- {title}")
    print(f"        APDR rows ({len(apdr)}): " + ", ".join(f"{k}{'*' if f else '(pending)'}@{ts}" for k, f, ts in apdr))
    print(f"        consumer runs ({len(runs)}): " + ", ".join(f"{k}:{s}(q{q} s{sd} e{e})" for k, s, q, sd, e in runs))

def wait_until(pred, timeout=120, what=""):
    end = time.time() + timeout
    while time.time() < end:
        cur.execute(pred)
        if cur.fetchone()[0]: return True
        time.sleep(1)
    print(f"[{t()}] TIMEOUT waiting for {what}"); return False

def runs_in(state, key="ACC1"):
    return f"select (select count(*) from dag_run where dag_id='exp_e1_consumer' and partition_key='{key}' and state='{state}')"

# clean slate
for st in ["delete from partitioned_asset_key_log where target_dag_id='exp_e1_consumer'",
           "delete from asset_partition_dag_run where target_dag_id='exp_e1_consumer'",
           "delete from dagrun_asset_event where dag_run_id in (select id from dag_run where dag_id='exp_e1_consumer')",
           "delete from task_instance where dag_id in ('exp_e1_consumer','exp_e1_producer')",
           "delete from dag_run where dag_id in ('exp_e1_consumer','exp_e1_producer')",
           "delete from asset_event where asset_id in (select id from asset where name='exp_e1_acct')"]:
    cur.execute(st)
print("clean slate")

emit(["ACC1"])
wait_until(runs_in("running") + " >= 1", 120, "run #1 running"); snapshot("step 1: after first ACC1 event, run #1 running")

emit(["ACC1"])
wait_until(f"select (select count(*) from dag_run where dag_id='exp_e1_consumer' and partition_key='ACC1') >= 2", 60, "2nd run row"); time.sleep(8)
snapshot("step 2: ACC1 emitted again while run #1 RUNNING")

emit(["ACC1", "ACC5"])
time.sleep(15); snapshot("step 3: ACC1 + ACC5 emitted while 2 runs running (max_active_runs=2)")

emit(["ACC1"])
time.sleep(12); snapshot("step 4: ACC1 emitted while an ACC1 run is QUEUED")

wait_until("select count(*) from dag_run where dag_id='exp_e1_consumer' and state in ('queued','running')", 240, "…")  # just pass
wait_until("select count(*)=0 from dag_run where dag_id='exp_e1_consumer' and state in ('queued','running')", 400, "all runs finished")
snapshot("final: all runs finished")
cur.execute("select partition_key, count(*) from dag_run where dag_id='exp_e1_consumer' group by 1 order by 1"); print("runs per key:", dict(cur.fetchall()))
