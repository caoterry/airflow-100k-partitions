#!/usr/bin/env bash
# 100k accounts: 200 producer runs x 500 accounts with the batcher paused, then one bell (variant C, then D; VERSION=n selects the dataset version).
set -u
cd "$(dirname "$0")/.." && source env.sh
sed -i '' 's/^PER_PARTITION_S = 0.5.*/PER_PARTITION_S = 0.0002     # scale test: engine time per partition made tiny so bookkeeping dominates/' dags/v2_dags.py
sleep 10
airflow dags pause v2_batcher >/dev/null 2>&1
T0=$(date +%s); echo "t0 $(date -u +%H:%M:%S) triggering 200 producers"
python - <<'PY'
import json, subprocess
for i in range(200):
    accts=[f"X{n:06d}" for n in range(i*500+1, i*500+501)]
    subprocess.run(["airflow","dags","trigger","v2_land","-c",json.dumps({"dataset":"positions","partitions":accts,"version":int(__import__("os").environ.get("VERSION","1"))})],capture_output=True)
PY
echo "triggered in $(( $(date +%s) - T0 ))s"
for i in $(seq 1 240); do sleep 5; n=$(python - <<'PY'
import psycopg2
c=psycopg2.connect("postgresql://airflow:airflow@localhost:5433/airflow_v2"); cur=c.cursor()
cur.execute("SELECT count(*) FROM dag_run WHERE dag_id='v2_land' AND state='success' AND start_date > now() - interval '40 minutes'"); print(cur.fetchone()[0])
PY
); [ "$n" -ge 200 ] && break; done; echo "producers done: $n at $(( $(date +%s) - T0 ))s"
python - <<'PY'
import psycopg2
c=psycopg2.connect("postgresql://airflow:airflow@localhost:5433/airflow_v2"); cur=c.cursor()
cur.execute("""SELECT round(avg(extract(epoch from (end_date-start_date)))::numeric,2), round(max(extract(epoch from (end_date-start_date)))::numeric,2)
               FROM task_instance WHERE dag_id='v2_land' AND start_date > now() - interval '40 minutes' AND state='success'"""); print("producer task avg/max s:", cur.fetchone())
PY
airflow dags unpause v2_batcher >/dev/null 2>&1; sleep 3
airflow dags trigger v2_land -c '{"dataset": "positions", "partitions": ["X100001"], "version": 1}' >/dev/null 2>&1
T1=$(date +%s); echo "bell rung at $(date -u +%H:%M:%S)"
for i in $(seq 1 600); do sleep 10; s=$(python - <<'PY'
import psycopg2
c=psycopg2.connect("postgresql://airflow:airflow@localhost:5433/airflow_v2"); cur=c.cursor()
cur.execute("SELECT count(*) FROM dag_run WHERE dag_id IN ('v2_batcher','v2_debounce','v2_ledger') AND state IN ('running','queued')"); print(cur.fetchone()[0])
PY
); [ "$s" = "0" ] && [ $i -gt 6 ] && break; done
echo "batcher run $s after $(( $(date +%s) - T1 ))s"
python - <<'PY'
import psycopg2, json
c=psycopg2.connect("postgresql://airflow:airflow@localhost:5433/airflow_v2"); cur=c.cursor()
cur.execute("SELECT id, run_id FROM dag_run WHERE dag_id='v2_batcher' AND run_id IN (SELECT run_id FROM task_instance WHERE dag_id='v2_batcher' AND task_id='finalize' AND state='success') ORDER BY id DESC LIMIT 1"); rid, run_id = cur.fetchone()
cur.execute("SELECT count(*) FROM dagrun_asset_event WHERE dag_run_id=%s", (rid,)); print("bells consumed:", cur.fetchone()[0])
cur.execute("""SELECT task_id, map_index, state, to_char(start_date,'HH24:MI:SS'), to_char(end_date,'HH24:MI:SS'), round(extract(epoch from (end_date-start_date))::numeric,1)
               FROM task_instance WHERE dag_id='v2_batcher' AND run_id=%s ORDER BY start_date""", (run_id,))
for r in cur.fetchall(): print("  ", r)
cur.execute("SELECT length(value::text) FROM asset_state_store s JOIN asset a ON a.id=s.asset_id WHERE a.name='v2_pnl' AND s.key='done'"); print("done dict bytes:", (cur.fetchone() or [None])[0])
cur.execute("SELECT a.name, count(*), max(length(e.extra::text)) FROM asset_event e JOIN asset a ON a.id=e.asset_id WHERE e.source_run_id=%s GROUP BY a.name ORDER BY 1", (run_id,)); print("asset_event rows written by this run (name, rows, max extra bytes):", cur.fetchall())
PY
sed -i '' 's/^PER_PARTITION_S = 0.0002.*/PER_PARTITION_S = 0.5/' dags/v2_dags.py
echo DONE
