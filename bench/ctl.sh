#!/usr/bin/env bash
# Start/stop Airflow 3 components for the benchmark. Usage: ctl.sh {init|start|stop|status|reset-db|logs}
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/env.sh"
PIDDIR="$AIRFLOW_HOME/pids"; LOGDIR="$AIRFLOW_HOME/proc-logs"; mkdir -p "$PIDDIR" "$LOGDIR"
COMPONENTS=(api-server dag-processor scheduler)

start_one() {
  local name="$1"; shift
  if [[ -f "$PIDDIR/$name.pid" ]] && kill -0 "$(cat "$PIDDIR/$name.pid")" 2>/dev/null; then echo "$name already running"; return; fi
  nohup airflow "$name" "$@" >"$LOGDIR/$name.log" 2>&1 &
  echo $! >"$PIDDIR/$name.pid"; echo "started $name pid $!"
}
stop_one() {
  local name="$1"
  [[ -f "$PIDDIR/$name.pid" ]] || return 0
  local pid; pid="$(cat "$PIDDIR/$name.pid")"
  if kill -0 "$pid" 2>/dev/null; then pkill -TERM -P "$pid" 2>/dev/null || true; kill -TERM "$pid" 2>/dev/null || true; fi
  for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
  kill -KILL "$pid" 2>/dev/null || true; rm -f "$PIDDIR/$name.pid"; echo "stopped $name"
}
case "${1:-}" in
  init)      airflow db migrate ;;
  reset-db)  "$0" stop; airflow db reset -y; ;;
  start)     start_one api-server; sleep 4; start_one dag-processor; start_one scheduler ;;
  stop)      for c in scheduler dag-processor api-server; do stop_one "$c"; done; pkill -f "airflow task" 2>/dev/null || true ;;
  status)    for c in "${COMPONENTS[@]}"; do if [[ -f "$PIDDIR/$c.pid" ]] && kill -0 "$(cat "$PIDDIR/$c.pid")" 2>/dev/null; then echo "$c: up ($(cat "$PIDDIR/$c.pid"))"; else echo "$c: down"; fi; done ;;
  logs)      tail -n "${3:-40}" "$LOGDIR/${2:-scheduler}.log" ;;
  *) echo "usage: $0 {init|start|stop|status|reset-db|logs [component] [n]}"; exit 1 ;;
esac
