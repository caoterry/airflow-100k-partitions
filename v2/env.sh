# Source this before any airflow command for the v2 environment.
# Two databases on the same local Postgres (the bench container, port 5433):
#   airflow_v2  = Airflow's own metadata DB (on MWAA this is the managed DB we cannot touch)
#   v2_journal  = our journal (in production: an RDS database the team owns)
export V2_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
export REPO_ROOT="$(cd "$V2_ROOT/.." && pwd)"
export AIRFLOW_HOME="$V2_ROOT/airflow_home"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@localhost:5433/airflow_v2"
export V2_JOURNAL_DSN="postgresql://airflow:airflow@localhost:5433/v2_journal"
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__PARALLELISM=16
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
export AIRFLOW__CORE__DAGS_FOLDER="$V2_ROOT/dags"
export AIRFLOW__SCHEDULER__SCHEDULER_IDLE_SLEEP_TIME=0.5
export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=5
export AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL=5
export AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=False
export AIRFLOW__API__PORT=8090
export AIRFLOW__API__WORKERS=1
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="http://localhost:8090/execution/"
export AIRFLOW__CORE__AUTH_MANAGER="airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS="admin:admin"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True
export AIRFLOW__API_AUTH__JWT_SECRET="v2-local-only-not-secret-0123456789abcdef"
export AIRFLOW__API_AUTH__JWT_EXPIRATION_TIME=86400
export AIRFLOW__LOGGING__BASE_LOG_FOLDER="$AIRFLOW_HOME/logs"
export NO_PROXY="localhost,127.0.0.1"
export PYTHONUNBUFFERED=1
source "$REPO_ROOT/.venv/bin/activate"
