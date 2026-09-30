#!/usr/bin/env python
"""E5 driver — run the dynamic batcher under a burst+trickle load with a given policy, then report batch sizes,
pool queueing and per-account latency. Usage: exp_e5.py --policy adaptive_sla|fixed --label <name>"""
from __future__ import annotations
import argparse, json, sys, time, glob, os
sys.path.insert(0, os.path.dirname(__file__))
import harness as H

ap = argparse.ArgumentParser(); ap.add_argument("--policy", default="adaptive_sla"); ap.add_argument("--label", required=True)
ap.add_argument("--burst", type=int, default=60); ap.add_argument("--trickle-every", type=float, default=6.0); ap.add_argument("--trickle-n", type=int, default=30)
a = ap.parse_args()
tok = H.token(); c = H.client(tok); conn = H.db(); conn.autocommit = True; cur = conn.cursor()
T0 = time.time(); t = lambda: f"{time.time()-T0:6.1f}s"
cur.execute("select id from asset where name='rev5_positions'"); AID = cur.fetchone()[0]

def emit(accounts, version=1):
    rid = f"e5_{a.label}_{accounts[0]}_{len(accounts)}_{int(time.time()*1000)}"
    r = c.post("/api/v2/dags/rev5_producer/dagRuns", json={"dag_run_id": rid, "conf": {"accounts": accounts, "version": version}, "logical_date": None}); r.raise_for_status()

# clean + policy
c.patch("/api/v2/dags/rev5_batcher", json={"is_paused": True})
for st in ["delete from partitioned_asset_key_log where target_dag_id='rev5_pnl_consumer'", "delete from asset_partition_dag_run where target_dag_id='rev5_pnl_consumer'",
           "delete from dagrun_asset_event where dag_run_id in (select id from dag_run where dag_id like 'rev5_%')",
           "delete from task_instance where dag_id like 'rev5_%'", "delete from dag_run where dag_id like 'rev5_%'",
           "delete from asset_event where asset_id in (select id from asset where name in ('rev5_positions','rev5_pnl'))",
           "delete from asset_state_store where asset_id in (select id from asset where name in ('rev5_positions','rev5_pnl'))"]:
    cur.execute(st)
policy = {"mode": a.policy, "K": 3, "startup_s": 15.0, "per_account_s": 0.5, "b_min": 3, "b_max": 60, "t_max_s": 90, "fixed_size": 10, "target_s": 90}
r = c.put(f"/api/v2/assets/{AID}/state-store/policy", json={"value": policy}); print("policy set:", r.status_code, policy)
c.patch("/api/v2/dags/rev5_batcher", json={"is_paused": False})

print(f"[{t()}] BURST: {a.burst} accounts in 3 producer runs")
ids = [f"ACC{i:04d}" for i in range(1, a.burst + 1)]
for k in range(3): emit(ids[k::3])
print(f"[{t()}] TRICKLE: 1 account every {a.trickle_every}s x {a.trickle_n}")
for i in range(a.trickle_n):
    emit([f"ACC{a.burst + 1 + i:04d}"]); time.sleep(a.trickle_every)
# wait for everything to be published
deadline = time.time() + 900
while time.time() < deadline:
    cur.execute("select count(distinct e.partition_key) from asset_event e where e.asset_id=(select id from asset where name='rev5_pnl')")
    done = cur.fetchone()[0]
    if done >= a.burst + a.trickle_n: break
    time.sleep(5)
print(f"[{t()}] published accounts: {done}/{a.burst + a.trickle_n}")
c.patch("/api/v2/dags/rev5_batcher", json={"is_paused": True})

print("\n=== claim decisions (batch sizes per batcher run) ===")
for f in sorted(glob.glob("airflow_home/logs/dag_id=rev5_batcher/run_id=*/task_id=claim/attempt=1.log"), key=os.path.getmtime):
    for l in open(f):
        if "policy=" in l and "batches sizes" in l:
            print("  ", f.split("run_id=")[1][11:19], l.split("policy=")[1].split('"')[0][:170])
print("\n=== spark task instances: pool queueing (queued -> running) ===")
cur.execute("select run_id, map_index, state, round(extract(epoch from (start_date-queued_dttm))::numeric,0) queue_wait_s, round(duration::numeric,0) dur_s from task_instance where dag_id='rev5_batcher' and task_id='spark' order by queued_dttm")
rows = cur.fetchall(); print("  ", len(rows), "spark TIs; queue wait p50/max:", sorted(r[3] or 0 for r in rows)[len(rows)//2] if rows else None, max((r[3] or 0) for r in rows) if rows else None)
print("\n=== per-account latency: input event -> pnl event ===")
cur.execute("""with i as (select partition_key k, min(timestamp) t0 from asset_event where asset_id=(select id from asset where name='rev5_positions') group by 1),
 o as (select partition_key k, min(timestamp) t1 from asset_event where asset_id=(select id from asset where name='rev5_pnl') group by 1)
 select case when i.k <= %s then 'burst' else 'trickle' end phase, count(*), round(percentile_cont(0.5) within group (order by extract(epoch from o.t1-i.t0))::numeric,0) p50_s,
 round(percentile_cont(0.95) within group (order by extract(epoch from o.t1-i.t0))::numeric,0) p95_s, round(max(extract(epoch from o.t1-i.t0))::numeric,0) max_s
 from i join o on o.k=i.k group by 1 order by 1""", (f"ACC{a.burst:04d}",))
lat = cur.fetchall(); [print("  ", r) for r in lat]
cur.execute("select count(*), round(avg(cnt)::numeric,1) from (select source_run_id||source_map_index::text b, count(*) cnt from asset_event where asset_id=(select id from asset where name='rev5_pnl') group by 1) x")
jobs = cur.fetchone(); print(f"   jobs (pnl-emitting batches): {jobs[0]}, avg batch {jobs[1]}")
cur.execute("select count(*) from dag_run where dag_id='rev5_pnl_consumer'"); print("   rev5_pnl_consumer runs:", cur.fetchone()[0])
out = {"label": a.label, "policy": policy, "latency": [list(map(lambda x: float(x) if x is not None else None, r[1:])) for r in lat], "phases": [r[0] for r in lat], "jobs": jobs[0], "avg_batch": float(jobs[1] or 0), "spark_tis": len(rows)}
os.makedirs(f"bench/results/{a.label}", exist_ok=True); json.dump(out, open(f"bench/results/{a.label}/summary.json", "w"), indent=1)
