#!/usr/bin/env bash
# Phase 3: before/after for the two upstream-style fixes.
#  A) backport #69565 (expansion: add()+single flush instead of merge() per TI)  -> rerun scheduler-only ladder
#  C) partition write path: request-scoped cache + APDR indexes                   -> rerun native partitions 10k, 1 emitter
set -uo pipefail
cd "$(dirname "$0")/.." && source bench/env.sh
stamp() { echo "===== $1 $(date -u +%H:%M:%S) ====="; }
gaps() { echo "--- heartbeat gaps ---"; grep -o "Heartbeat recovered after [0-9.]* seconds" airflow_home/proc-logs/scheduler.log | tail -2; }

stamp "apply Patch A and restart (parallelism back to 12)"
bench/ctl.sh stop; sleep 2
python patches/patch_a_expansion_no_merge.py || exit 1
BENCH_PARALLELISM=12 bench/ctl.sh start; sleep 15; bench/ctl.sh status

for N in 10000 30000 100000; do
  stamp "PATCH A: flat_empty n=$N"
  python bench/harness.py run --dag bench_flat_empty --conf "{\"n\": $N}" --label patchA_flat_empty_${N} --interval 1 --timeout 7200 --no-probe 2>&1 | grep -v Warning; gaps
done

stamp "apply APDR/dag_run indexes + Patch C, restart api-server"
docker exec -i airflow-bench-pg psql -U airflow -v ON_ERROR_STOP=0 < patches/apdr_index.sql
python patches/patch_c_partition_write_path_cache.py || exit 1
bench/ctl.sh stop; sleep 2; BENCH_PARALLELISM=12 bench/ctl.sh start; sleep 15

stamp "PATCH C + index: partition 10k, 1 emitter (as-shipped was 53 s write / 219 s created / 426 s done)"
python bench/harness.py partition --n 10000 --emitters 1 --label patchC_part_10k_e1 --clean --interval 3 --timeout 3600 2>&1 | grep -vE "Warning|^clean:"
echo "--- PATCH request durations for this run ---"; grep -E "method=PATCH path=/execution/task-instances/" airflow_home/proc-logs/api-server.log | awk '{for(i=1;i<=NF;i++){if($i ~ /^duration_us=/){d=substr($i,13)+0}} if(d>1000000) printf "%s %.1fs\n", $1, d/1e6}' | tail -5
gaps
stamp "PHASE3 DONE"
python bench/report.py
