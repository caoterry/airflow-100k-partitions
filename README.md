# airflow-100k-partitions

Feasibility experiment: can Apache Airflow 3.3.x (company baseline 3.3.0; here 3.3.2 + main for source study)
orchestrate 100k "partitions" keyed by firm account, for a shared balance-sheet / revenue platform that must run on
self-hosted Airflow or AWS MWAA?

Layout
- `bench/env.sh`        env for a local Airflow 3.3.2 (Postgres 16 in Docker on :5433, LocalExecutor)
- `bench/ctl.sh`        `init | start | stop | status | reset-db | logs`
- `bench/dags/`         five scenarios: flat_empty (scheduler-only), flat_python (real exec), batched,
                        two_level (DAG-of-DAGs), run_per_account (one DagRun per partition)
- `bench/harness.py`    trigger + timeline sampler + API/UI latency probes; results in `bench/results/<label>/`
- `docs/superpowers/specs/`  design doc and findings
- `airflow-src/`        shallow clone of apache/airflow main (gitignored)

Quick start
```bash
docker run -d --name airflow-bench-pg -e POSTGRES_USER=airflow -e POSTGRES_PASSWORD=airflow -e POSTGRES_DB=airflow -p 5433:5432 postgres:16
uv venv .venv --python 3.12 && source .venv/bin/activate
uv pip install "apache-airflow[postgres]==3.3.2" --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt
uv pip install httpx psutil
source bench/env.sh && bench/ctl.sh init && bench/ctl.sh start
python bench/harness.py run --dag bench_flat_empty --conf '{"n": 10000}' --label flat_empty_10k
```
