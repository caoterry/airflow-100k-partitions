"""The journal, variant C: inside Airflow's asset_state_store, with a FIXED number of API calls per run.

Keys on the v2_positions asset (only the batcher writes; max_active_runs=1 means one writer at a time):
  done              {account: version last published}      one dict for all accounts; read once per claim, written once per run
  failed            {account: version whose batch failed}  same shape
  batch/<run>#<i>   {"state": claimed|running|done|failed, "versions": {account: version}, "error": str}
Per run: claim = 2 gets + K sets, spark = 2 per batch, publish = 2 per batch, finalize = K gets + 4. Independent of the account count.
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
    """Keep the candidates whose version is newer than done (or equal to a failed version: a re-queue), then
    split them into min(K, ceil(n / b_min)) batches. Two reads, K writes, whatever n is."""
    done = store.get("done") or {}
    failed = store.get("failed") or {}
    todo = [(a, v) for a, v in sorted(wanted.items())
            if v > int(done.get(a, -1)) or (a in failed and v >= int(failed[a]))]
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
        versions = {a: v for a, v in chunk}
        store.set(f"batch/{batch_id}", {"state": "claimed", "versions": versions, "error": None})
        batches.append({"batch_id": batch_id, "accounts": [a for a, _ in chunk], "versions": versions})
    return batches


def _set_state(store, batch_id: str, state: str, error: str | None = None) -> None:
    rec = store.get(f"batch/{batch_id}") or {}
    store.set(f"batch/{batch_id}", {**rec, "state": state, "error": error})


def running(store, batch_id: str) -> None:
    _set_state(store, batch_id, "running")


def publish(store, batch: dict) -> dict:
    """Marks the batch done. The per-account done versions are merged by finalize, once per run."""
    _set_state(store, batch["batch_id"], "done")
    return {"done": batch["accounts"], "versions": batch["versions"]}


def fail(store, batch: dict, error: str) -> None:
    """After Airflow's last retry: only this batch is marked failed; other batches keep running."""
    _set_state(store, batch["batch_id"], "failed", error[:300])


def finalize(store, batches: list[dict]) -> dict:
    """Once per run, after every publish has ended (trigger_rule all_done): fold the batch results into the two
    dicts. Single writer, so no lock is needed. A newer version that arrived meanwhile is in a later bell, and the
    next run's claim will see it is newer than done."""
    done = store.get("done") or {}
    failed = store.get("failed") or {}
    n_done = n_failed = 0
    for b in batches:
        rec = store.get(f"batch/{b['batch_id']}") or {}
        if rec.get("state") == "done":
            for a, v in b["versions"].items():
                done[a] = max(int(done.get(a, -1)), int(v)); failed.pop(a, None); n_done += 1
        elif rec.get("state") == "failed":
            for a, v in b["versions"].items():
                failed[a] = int(v); n_failed += 1
    store.set("done", done)
    store.set("failed", failed)
    return {"done": n_done, "failed": n_failed, "accounts_tracked": len(done)}
