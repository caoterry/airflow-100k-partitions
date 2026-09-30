#!/usr/bin/env bash
# Phase 2b: the remaining phase-2 scenarios, after the 100k native-partition run was stopped once all runs existed.
set -uo pipefail
cd "$(dirname "$0")/.." && source bench/env.sh
stamp() { echo "===== $1 $(date -u +%H:%M:%S) ====="; }
gaps() { echo "--- heartbeat gaps ---"; grep -o "Heartbeat recovered after [0-9.]* seconds" airflow_home/proc-logs/scheduler.log | tail -2; }

stamp "stop, purge partition-scenario rows, restart with parallelism=8"
bench/ctl.sh stop; sleep 3
python - <<'PY'
import sys; sys.path.insert(0, "bench"); import harness; harness.part_clean()
PY
docker exec airflow-bench-pg psql -U airflow -Atc "vacuum analyze dag_run; vacuum analyze task_instance; vacuum analyze asset_partition_dag_run;" >/dev/null
BENCH_PARALLELISM=8 bench/ctl.sh start; sleep 15; bench/ctl.sh status

stamp "flat_python 1k"
python bench/harness.py run --dag bench_flat_python --conf '{"n": 1000}' --label flat_python_1k --interval 2 --timeout 1800 --task-id process 2>&1 | grep -v Warning; gaps
stamp "flat_python 10k"
python bench/harness.py run --dag bench_flat_python --conf '{"n": 10000}' --label flat_python_10k --interval 3 --timeout 5400 --task-id process 2>&1 | grep -v Warning; gaps
echo "--- xcom GET traffic during flat_python_10k (bytes) ---"; grep -E "method=GET path=/execution/xcoms/bench_flat_python" airflow_home/proc-logs/api-server.log | wc -l

stamp "batched 100k = 100 x 1000"
python bench/harness.py run --dag bench_batched --conf '{"n": 100000, "batch_size": 1000}' --label batched_100k_100x1000 --interval 2 --timeout 1800 --task-id process_batch 2>&1 | grep -v Warning; gaps

stamp "two_level 100k = 100 child runs x 1000 (empty)"
python bench/harness.py run --dag bench_two_level_parent --conf '{"n": 100000, "batch_size": 1000, "child_mode": "empty"}' --label two_level_100k_empty --interval 3 --timeout 5400 --task-id trigger_child --no-probe 2>&1 | grep -v Warning
echo "--- child run states ---"; docker exec airflow-bench-pg psql -U airflow -Atc "select state, count(*) from dag_run where dag_id='bench_two_level_child' group by 1"
docker exec airflow-bench-pg psql -U airflow -Atc "select min(start_date), max(end_date), max(end_date)-min(start_date) as span, count(*) from dag_run where dag_id='bench_two_level_child'"; gaps

stamp "run_per_account 10k"
python bench/harness.py bulk --dag bench_run_per_account --n 10000 --concurrency 32 --label rpa_10k --interval 3 --timeout 5400 2>&1 | grep -v Warning; gaps

stamp "PHASE2B DONE"
python bench/report.py
