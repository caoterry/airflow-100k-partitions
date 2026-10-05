"""The journal, variant B: rows live in Airflow's own asset_state_store table, one key per account.

Nothing outside Airflow. Single-writer rule: producers never write here (they put {account: version} into the bell
event's extra); only the batcher writes, and max_active_runs=1 means one claim at a time, so no lock is needed.
Key layout on the v2_positions asset:
  acct/<ACC>   {"status": pending|inflight|done|failed, "seen": v, "claimed": v, "done": v, "failed": v, "batch": id, "error": str}
  batch/<id>   {"accounts": n, "state": claimed|running|done|failed, "error": str}
"""
from __future__ import annotations

import math


def candidates(events) -> dict[str, int]:
    """Merge the bells this run consumed: newest version per account. Airflow attaches every bell since the
    previous run, so this is the read position; no watermark."""
    out: dict[str, int] = {}
    for e in events:
        for acct, v in ((e.extra or {}).get("accounts") or {}).items():
            out[acct] = max(out.get(acct, 0), int(v))
    return out


def claim(store, wanted: dict[str, int], run_id: str, k: int, b_min: int) -> list[dict]:
    """Decide which candidates need computing, then split them into min(K, ceil(n / b_min)) batches.

    Skip an account when the version is not newer than what is already done. A row left 'inflight' can only be
    stale here (max_active_runs=1: the previous run has ended), so it is claimed again.
    """
    todo = []
    for acct in sorted(wanted):
        v = wanted[acct]
        rec = store.get(f"acct/{acct}") or {}
        if rec.get("status") == "failed" and v >= int(rec.get("failed", -1)):
            todo.append((acct, v, rec)); continue
        if v > int(rec.get("done", -1)):
            todo.append((acct, v, rec))
    if not todo:
        return []
    n_batches = max(1, min(k, math.ceil(len(todo) / b_min)))
    size = math.ceil(len(todo) / n_batches)
    batches = []
    for i in range(n_batches):
        chunk = todo[i * size:(i + 1) * size]
        if not chunk:
            break
        batch_id = f"{run_id}#{i}"
        for acct, v, rec in chunk:
            store.set(f"acct/{acct}", {"status": "inflight", "seen": v, "claimed": v, "done": int(rec.get("done", -1)),
                                        "failed": rec.get("failed"), "batch": batch_id, "error": None})
        store.set(f"batch/{batch_id}", {"accounts": len(chunk), "state": "claimed", "error": None})
        batches.append({"batch_id": batch_id, "accounts": [a for a, _, _ in chunk]})
    return batches


def running(store, batch_id: str) -> None:
    rec = store.get(f"batch/{batch_id}") or {}
    store.set(f"batch/{batch_id}", {**rec, "state": "running"})


def publish(store, batch: dict) -> dict:
    """done = claimed, for the rows this batch holds. A newer version that arrived meanwhile is in a later bell,
    and the next run's claim will see it is newer than done and claim it again."""
    versions = {}
    for acct in batch["accounts"]:
        rec = store.get(f"acct/{acct}") or {}
        if rec.get("batch") != batch["batch_id"]:
            continue                                   # a stale or cleared task must not touch another batch's rows
        versions[acct] = rec["claimed"]
        store.set(f"acct/{acct}", {**rec, "status": "done", "done": rec["claimed"], "batch": None})
    rec = store.get(f"batch/{batch['batch_id']}") or {}
    store.set(f"batch/{batch['batch_id']}", {**rec, "state": "done"})
    return {"done": list(versions), "versions": versions}


def fail(store, batch: dict, error: str) -> None:
    """After Airflow's last retry: only this batch's rows go to failed. They come back when a newer version lands
    or an operator re-queues them (v2_requeue rings the bell with the same version)."""
    for acct in batch["accounts"]:
        rec = store.get(f"acct/{acct}") or {}
        if rec.get("batch") != batch["batch_id"]:
            continue
        store.set(f"acct/{acct}", {**rec, "status": "failed", "failed": rec["claimed"], "batch": None, "error": error[:300]})
    rec = store.get(f"batch/{batch['batch_id']}") or {}
    store.set(f"batch/{batch['batch_id']}", {**rec, "state": "failed", "error": error[:300]})
