#!/usr/bin/env bash
# v2 environment. Usage: ctl.sh {init|start|stop|status|logs [component] [n]}
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/env.sh"
PIDDIR="$AIRFLOW_HOME/pids"; LOGDIR="$AIRFLOW_HOME/proc-logs"; mkdir -p "$PIDDIR" "$LOGDIR"
COMPONENTS=(api-server dag-processor scheduler triggerer)
start_one() {
  local name="$1"
  if [[ -f "$PIDDIR/$name.pid" ]] && kill -0 "$(cat "$PIDDIR/$name.pid")" 2>/dev/null; then echo "$name already running"; return; fi
  nohup airflow "$name" >"$LOGDIR/$name.log" 2>&1 &
  echo $! >"$PIDDIR/$name.pid"; echo "started $name pid $!"
}
stop_one() {
  local name="$1"; [[ -f "$PIDDIR/$name.pid" ]] || return 0
  local pid; pid="$(cat "$PIDDIR/$name.pid")"
  if kill -0 "$pid" 2>/dev/null; then pkill -TERM -P "$pid" 2>/dev/null || true; kill -TERM "$pid" 2>/dev/null || true; fi
  for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
  kill -KILL "$pid" 2>/dev/null || true; rm -f "$PIDDIR/$name.pid"; echo "stopped $name"
}
case "${1:-}" in
  init)
    python "$HERE/tools/initdb.py"            # creates airflow_v2 and v2_journal if missing, plus the journal schema
    airflow db migrate
    airflow pools set v2_spark 3 "K engine slots for the v2 batcher"
    ;;
  start)  start_one api-server; sleep 5; start_one dag-processor; start_one scheduler; start_one triggerer ;;
  stop)   for c in triggerer scheduler dag-processor api-server; do stop_one "$c"; done ;;
  status) for c in "${COMPONENTS[@]}"; do if [[ -f "$PIDDIR/$c.pid" ]] && kill -0 "$(cat "$PIDDIR/$c.pid")" 2>/dev/null; then echo "$c: up"; else echo "$c: down"; fi; done ;;
  logs)   tail -n "${3:-40}" "$LOGDIR/${2:-scheduler}.log" ;;
  *) echo "usage: $0 {init|start|stop|status|logs [component] [n]}"; exit 1 ;;
esac
