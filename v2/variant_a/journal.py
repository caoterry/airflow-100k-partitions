"""The journal: one row per account in our own database (production: RDS).

It only does bookkeeping. Airflow decides when anything runs; these functions decide which accounts are
waiting and record what happened to them. Every write is one short transaction.

Row life cycle (see docs/two-grains-one-bucket.md section 5):
  (new) --note_versions--> pending --claim--> inflight --publish--> done
                              ^                  |  \\--publish, newer version arrived--> pending
                              |                  \\--fail (after Airflow's retries)--> failed
                              +---- note_versions with a newer version (from done or failed)
"""
from __future__ import annotations

import math
import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

DSN = os.environ.get("V2_JOURNAL_DSN", "postgresql://airflow:airflow@localhost:5433/v2_journal")


@contextmanager
def tx():
    conn = psycopg2.connect(DSN)
    try:
        with conn, conn.cursor() as cur:
            yield cur
    finally:
        conn.close()


def note_versions(accounts: list[str], version: int) -> None:
    """Producer side. Coalescing rule: one row per account keeps only the newest version.

    new account            -> pending
    pending / inflight     -> stays, seen_version rises (an inflight row is NOT claimed again)
    done / failed          -> pending again, but only if this version is newer than the one already handled
    """
    rows = [(a, version) for a in accounts]
    with tx() as cur:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO accounts AS j (account, status, seen_version) VALUES %s
            ON CONFLICT (account) DO UPDATE SET
              seen_version = GREATEST(j.seen_version, EXCLUDED.seen_version),
              status = CASE
                  WHEN j.status = 'done'   AND EXCLUDED.seen_version > j.done_version THEN 'pending'
                  WHEN j.status = 'failed' AND EXCLUDED.seen_version > j.failed_version THEN 'pending'
                  ELSE j.status END,
              first_seen = CASE
                  WHEN j.status = 'done'   AND EXCLUDED.seen_version > j.done_version   THEN now()
                  WHEN j.status = 'failed' AND EXCLUDED.seen_version > j.failed_version THEN now()
                  ELSE j.first_seen END,
              updated_at = now()
            """, [(a, "pending", v) for a, v in rows], template="(%s, %s, %s)")


def claim(run_id: str, k: int, b_min: int) -> list[dict]:
    """Batcher side. Take every pending account, oldest first, and split them into batches.

    Number of batches = min(K, ceil(n / b_min)): use all K engine slots when there is enough work, but never
    start a job for fewer than b_min accounts unless they are all there is. No size cap, no timing, no capacity
    counting: the Airflow pool decides when each batch runs.
    FOR UPDATE SKIP LOCKED: two batcher runs can never take the same row.
    """
    with tx() as cur:
        cur.execute("""SELECT account, seen_version FROM accounts
                       WHERE status = 'pending' ORDER BY first_seen, account FOR UPDATE SKIP LOCKED""")
        pending = cur.fetchall()
        if not pending:
            return []
        n_batches = max(1, min(k, math.ceil(len(pending) / b_min)))
        size = math.ceil(len(pending) / n_batches)
        batches = []
        for i in range(n_batches):
            chunk = pending[i * size:(i + 1) * size]
            if not chunk:
                break
            batch_id = f"{run_id}#{i}"
            cur.execute("""UPDATE accounts SET status = 'inflight', claimed_version = seen_version,
                                  batch_id = %s, error = NULL, updated_at = now()
                           WHERE account = ANY(%s)""", (batch_id, [a for a, _ in chunk]))
            cur.execute("INSERT INTO batches (batch_id, accounts, state) VALUES (%s, %s, 'claimed')", (batch_id, len(chunk)))
            batches.append({"batch_id": batch_id, "accounts": [a for a, _ in chunk]})
        return batches


def batch_running(batch_id: str) -> None:
    with tx() as cur:
        cur.execute("UPDATE batches SET state = 'running' WHERE batch_id = %s", (batch_id,))


def publish(batch_id: str) -> dict:
    """Publish only touches rows still inflight under THIS batch id (a stale or cleared task cannot mark others done).

    done_version = claimed_version. If a newer version arrived while the batch ran, the row re-opens to pending.
    """
    with tx() as cur:
        cur.execute("""UPDATE accounts SET
                          done_version = claimed_version,
                          status = CASE WHEN seen_version > claimed_version THEN 'pending' ELSE 'done' END,
                          first_seen = CASE WHEN seen_version > claimed_version THEN now() ELSE first_seen END,
                          batch_id = NULL, updated_at = now()
                       WHERE batch_id = %s AND status = 'inflight'
                       RETURNING account, status, claimed_version""", (batch_id,))
        rows = cur.fetchall()
        cur.execute("UPDATE batches SET state = 'done', finished_at = now() WHERE batch_id = %s", (batch_id,))
    return {"done": [a for a, s, _ in rows if s == "done"], "reopened": [a for a, s, _ in rows if s == "pending"],
            "versions": {a: v for a, _, v in rows}}


def fail(batch_id: str, error: str) -> None:
    """Called once Airflow has given up on the batch (after its retries). Only this batch's rows move to failed.

    A failed account leaves 'failed' when a newer version arrives (note_versions) or an operator re-queues it.
    """
    with tx() as cur:
        cur.execute("""UPDATE accounts SET status = 'failed', failed_version = claimed_version, error = %s,
                              batch_id = NULL, updated_at = now()
                       WHERE batch_id = %s AND status = 'inflight'""", (error[:500], batch_id))
        cur.execute("UPDATE batches SET state = 'failed', finished_at = now(), error = %s WHERE batch_id = %s",
                    (error[:500], batch_id))


def requeue(accounts: list[str]) -> None:
    """Operator action: put failed accounts back in the queue."""
    with tx() as cur:
        cur.execute("""UPDATE accounts SET status = 'pending', first_seen = now(), error = NULL, updated_at = now()
                       WHERE account = ANY(%s) AND status = 'failed'""", (accounts,))


def poisoned(accounts: list[str]) -> list[str]:
    with tx() as cur:
        cur.execute("SELECT account FROM poison WHERE account = ANY(%s)", (accounts,))
        return [r[0] for r in cur.fetchall()]
