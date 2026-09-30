"""A locked ledger for the batcher, in an EXTERNAL Postgres (here: the bench instance, schema `bench_ledger`).

Airflow 3 tasks must not touch Airflow's own metadata DB; a production ledger would live in the platform's database
(or DynamoDB with conditional writes). The point of this module is the concurrency contract, not the storage:
  - `merge_seen(newest)`   upsert accounts seen in the event log (pending if their version is newer than processed)
  - `claim(planner)`        SELECT ... FOR UPDATE SKIP LOCKED on ready rows, plan batches, mark them inflight — one transaction
  - `done(batch)` / `release(batch)`   flip inflight -> done / pending
Two batcher runs can call claim() concurrently and never pick the same account.
"""
from __future__ import annotations
import os
from datetime import datetime, timezone
import psycopg2

DSN = os.environ.get("BENCH_LEDGER_DSN", "dbname=airflow user=airflow password=airflow host=localhost port=5433")
DDL = """
create schema if not exists bench_ledger;
create table if not exists bench_ledger.accounts (
    account       text primary key,
    status        text not null default 'pending',   -- pending | inflight | done
    seen_version  int  not null default 0,           -- newest input version seen in the event log
    done_version  int  not null default -1,          -- version last published
    first_seen    timestamptz not null default now(),
    batch         text,
    updated_at    timestamptz not null default now()
);
create index if not exists ix_ledger_status_first_seen on bench_ledger.accounts (status, first_seen);
create table if not exists bench_ledger.watermark (k text primary key, v text);
"""


class PgLedger:
    def __init__(self):
        self.conn = psycopg2.connect(DSN); self.conn.autocommit = False
        with self.conn.cursor() as c:
            c.execute(DDL)
        self.conn.commit()

    def close(self):
        self.conn.close()

    def get_watermark(self):
        with self.conn.cursor() as c:
            c.execute("select v from bench_ledger.watermark where k='events'"); r = c.fetchone()
        return r[0] if r else None

    def set_watermark(self, v: str):
        with self.conn.cursor() as c:
            c.execute("insert into bench_ledger.watermark(k, v) values ('events', %s) on conflict (k) do update set v = excluded.v", (v,))
        self.conn.commit()

    def merge_seen(self, newest: dict[str, tuple[int, float]]):
        """newest: account -> (version, first_seen_epoch). Rows whose seen_version rises above done_version become pending."""
        if not newest: return
        rows = [(a, v, datetime.fromtimestamp(ts, timezone.utc)) for a, (v, ts) in newest.items()]
        with self.conn.cursor() as c:
            c.executemany("""
                insert into bench_ledger.accounts (account, status, seen_version, first_seen)
                values (%s, 'pending', %s, %s)
                on conflict (account) do update set
                    seen_version = greatest(bench_ledger.accounts.seen_version, excluded.seen_version),
                    first_seen   = case when bench_ledger.accounts.status = 'done' then excluded.first_seen else least(bench_ledger.accounts.first_seen, excluded.first_seen) end,
                    status       = case when bench_ledger.accounts.status = 'done' and excluded.seen_version > bench_ledger.accounts.done_version then 'pending'
                                        else bench_ledger.accounts.status end,
                    updated_at   = now()
            """, rows)
        self.conn.commit()

    def claim(self, planner, running_batches_fn=None) -> list[list[str]]:
        """Lock the ready rows, let `planner(ready, running_batches)` decide the batches, mark them inflight. Atomic."""
        with self.conn.cursor() as c:
            c.execute("select count(distinct batch) from bench_ledger.accounts where status='inflight'")
            running_batches = c.fetchone()[0]
            c.execute("""select account, extract(epoch from first_seen) from bench_ledger.accounts
                         where status='pending' and seen_version > done_version order by first_seen for update skip locked""")
            ready = [(a, float(ts)) for a, ts in c.fetchall()]
            batches = planner(ready, running_batches)
            for bi, batch in enumerate(batches):
                c.execute("update bench_ledger.accounts set status='inflight', batch=%s, updated_at=now() where account = any(%s)",
                          (f"{bi}", batch))
        self.conn.commit()
        return batches

    def tag_batches(self, run_id: str, batches: list[list[str]]):
        with self.conn.cursor() as c:
            for bi, batch in enumerate(batches):
                c.execute("update bench_ledger.accounts set batch=%s where account = any(%s)", (f"{run_id}#{bi}", batch))
        self.conn.commit()

    def done(self, batch: list[str]):
        with self.conn.cursor() as c:
            c.execute("update bench_ledger.accounts set status='done', done_version=seen_version, batch=null, updated_at=now() where account = any(%s)", (batch,))
        self.conn.commit()

    def release(self, batch: list[str]):
        with self.conn.cursor() as c:
            c.execute("update bench_ledger.accounts set status='pending', batch=null, updated_at=now() where account = any(%s) and status='inflight'", (batch,))
        self.conn.commit()

    def reset(self):
        with self.conn.cursor() as c:
            c.execute("truncate bench_ledger.accounts; delete from bench_ledger.watermark")
        self.conn.commit()
