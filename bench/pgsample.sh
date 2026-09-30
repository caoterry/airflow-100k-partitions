#!/usr/bin/env bash
# Poor-man's DB profiler: sample non-idle queries from pg_stat_activity every 0.5s. Usage: pgsample.sh <seconds> <outfile>
DUR=${1:-300}; OUT=${2:-/dev/stdout}; END=$((SECONDS+DUR))
Q="select to_char(now(),'HH24:MI:SS.MS'), pid, state, wait_event_type, left(regexp_replace(query, '\s+', ' ', 'g'), 200) from pg_stat_activity where datname='airflow' and state<>'idle' and query not like '%pg_stat_activity%'"
while [ $SECONDS -lt $END ]; do docker exec airflow-bench-pg psql -U airflow -Atc "$Q" >>"$OUT" 2>&1; sleep 0.5; done
