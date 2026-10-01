# airflow-100k-partitions

Session entry: read the handover chain tip first — currently `docs/HANDOVER_2026-10-01.md`.

Feasibility experiment: can Apache Airflow 3.3.x (company baseline 3.3.0; here 3.3.2 + main for source study)
orchestrate 100k "partitions" keyed by firm account, for a shared balance-sheet / revenue platform that must run on
self-hosted Airflow or AWS MWAA?

**One page first: [SUMMARY.md](SUMMARY.md)** — what was asked, what was done, what was found, what is proposed, and where to read more.
The core design idea in one page: [docs/design-core.md](docs/design-core.md).
The same design in depth, in Airflow terms with real table rows and diagrams: [docs/two-grains-one-bucket.md](docs/two-grains-one-bucket.md).
Then **[REPORT.md](REPORT.md)** — findings, measurements, recommendation, and the upstream contribution list.
Deep dives: `docs/analysis/` (source traces of 3.3.2 vs main), `docs/research/` (fact-checked literature/MWAA/Dagster sweep),
`docs/superpowers/specs/` (design and assumptions). Charts in `docs/img/`.

Layout
- `bench/env.sh`        env for a local Airflow 3.3.2 (Postgres 16 in Docker on :5433, LocalExecutor)
- `bench/ctl.sh`        `init | start | stop | status | reset-db | logs`
- `bench/dags/`         six scenarios: flat_empty (scheduler-only), flat_python (real exec), batched,
                        two_level (DAG-of-DAGs), run_per_account (one DagRun per partition), partitions (native AIP-76 producer/consumer)
- `bench/harness.py`    trigger + timeline sampler + API/UI latency probes; results in `bench/results/<label>/`
- `bench/run_phase*.sh` the exact chains that produced the results; `bench/report.py` tabulates them; `bench/charts.py` renders `docs/img/`
- `patches/`            Patch A (backport of apache/airflow#69565, expansion), Patch C (partition write-path cache, no effect), `apdr_index.sql`
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

Notes
- The site-packages patches are applied/reverted with `python patches/<patch>.py [--revert]`; the checked-in results were
  produced with the patch state named in each `bench/results/<label>` (labels prefixed `patchA_`/`patchC_`).
- Component logs were not committed (≈100 MB); `bench/results/profiles/` keeps the `pg_stat_activity` samples.
- `harness.py partition --clean` deletes rows of the two partition DAGs directly in Postgres between runs.
