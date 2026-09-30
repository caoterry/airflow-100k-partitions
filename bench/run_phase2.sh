#!/usr/bin/env bash
# Phase 2 chain: run after the scheduler-only ladder and the 1k/10k native-partition runs.
set -uo pipefail
cd "$(dirname "$0")/.." && source bench/env.sh
stamp() { echo "===== $1 $(date -u +%H:%M:%S) ====="; }
gaps() { echo "--- heartbeat gaps ---"; grep -o "Heartbeat recovered after [0-9.]* seconds" airflow_home/proc-logs/scheduler.log | tail -3; }

stamp "partition 100k, 200 serialized emitters x 500 keys"
python bench/harness.py partition --n 100000 --emitters 200 --label part_100k_e200 --clean --interval 5 --timeout 5400 2>&1 | grep -vE "Warning|^clean:"; gaps

stamp "restart with parallelism=8 for real-execution scenarios"
bench/ctl.sh stop; sleep 3; BENCH_PARALLELISM=8 bench/ctl.sh start; sleep 15; bench/ctl.sh status

stamp "flat_python 1k"
python bench/harness.py run --dag bench_flat_python --conf '{"n": 1000}' --label flat_python_1k --interval 2 --timeout 1800 --task-id process 2>&1 | grep -v Warning; gaps
stamp "flat_python 10k"
python bench/harness.py run --dag bench_flat_python --conf '{"n": 10000}' --label flat_python_10k --interval 3 --timeout 5400 --task-id process 2>&1 | grep -v Warning; gaps

stamp "batched 100k = 100 x 1000"
python bench/harness.py run --dag bench_batched --conf '{"n": 100000, "batch_size": 1000}' --label batched_100k_100x1000 --interval 2 --timeout 1800 --task-id process_batch 2>&1 | grep -v Warning; gaps

stamp "two_level 100k = 100 child runs x 1000 (empty)"
python bench/harness.py run --dag bench_two_level_parent --conf '{"n": 100000, "batch_size": 1000, "child_mode": "empty"}' --label two_level_100k_empty --interval 3 --timeout 5400 --task-id trigger_child --no-probe 2>&1 | grep -v Warning
echo "--- child run states ---"; docker exec airflow-bench-pg psql -U airflow -Atc "select state, count(*) from dag_run where dag_id='bench_two_level_child' group by 1"; docker exec airflow-bench-pg psql -U airflow -Atc "select min(start_date), max(end_date), max(end_date)-min(start_date) as span, count(*) from dag_run where dag_id='bench_two_level_child'"; gaps

stamp "run_per_account 10k"
python bench/harness.py bulk --dag bench_run_per_account --n 10000 --concurrency 32 --label rpa_10k --interval 3 --timeout 5400 2>&1 | grep -v Warning; gaps

stamp "PHASE2 DONE"
python bench/report.py
