"""Create the two v2 databases (if missing) and the journal tables.

airflow_v2  : Airflow's own metadata DB (filled by `airflow db migrate`)
v2_journal  : our journal, one row per account (production: an RDS database the team owns)
"""
import os
import psycopg2

ADMIN_DSN = "postgresql://airflow:airflow@localhost:5433/postgres"

JOURNAL_DDL = """
CREATE TABLE IF NOT EXISTS accounts (
    account          text PRIMARY KEY,
    status           text NOT NULL CHECK (status IN ('pending', 'inflight', 'done', 'failed')),
    seen_version     int  NOT NULL,             -- newest version any producer has reported
    claimed_version  int,                       -- version the current batch is computing
    done_version     int  NOT NULL DEFAULT -1,  -- version last published
    failed_version   int,                       -- version whose batch failed after Airflow's retries
    first_seen       timestamptz NOT NULL DEFAULT now(),  -- when it started waiting (claim order)
    batch_id         text,                      -- batch holding it while inflight
    error            text,
    updated_at       timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id     text PRIMARY KEY,              -- '<batcher run_id>#<map_index>'
    accounts     int  NOT NULL,
    state        text NOT NULL CHECK (state IN ('claimed', 'running', 'done', 'failed')),
    claimed_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    error        text
);
CREATE TABLE IF NOT EXISTS poison (account text PRIMARY KEY);  -- test only: spark fails for these accounts
"""

def ensure_db(name: str) -> None:
    conn = psycopg2.connect(ADMIN_DSN); conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
        if cur.fetchone() is None:
            cur.execute(f'CREATE DATABASE "{name}" OWNER airflow')
            print(f"created database {name}")
        else:
            print(f"database {name} exists")
    conn.close()

if __name__ == "__main__":
    ensure_db("airflow_v2")
    ensure_db("v2_journal")
    with psycopg2.connect(os.environ["V2_JOURNAL_DSN"]) as conn, conn.cursor() as cur:
        cur.execute(JOURNAL_DDL)
    print("journal tables ready")
