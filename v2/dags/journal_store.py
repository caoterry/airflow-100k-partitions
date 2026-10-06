"""The journal, variant C: inside Airflow's asset_state_store, with a FIXED number of API calls per run.

Keys on the v2_positions asset (only the batcher writes; max_active_runs=1 means one writer at a time):
  done              {account: [dataset_version, marker]}   what was last published for the account
  failed            {account: [dataset_version, marker]}   what failed, after Airflow's retries
  batch/<run>#<i>   {"state": claimed|running|done|failed, "versions": {account: [dv, marker]}, "retry": bool,
                     "culprits": [accounts the engine named], "error": str}

Two version spaces (decision D13):
  dataset_version  orders landings: a late, older landing never overwrites a newer result
  marker           says whether the account's input changed (row fingerprint, lake_in_id, or the dataset version itself
                   for delta feeds); compared for equality only, so it needs no ordering
Per run: claim = 2 gets + K sets, spark = 2 per batch, publish = 2 per batch, finalize = K gets + 4. Independent of the account count.
"""
from __future__ import annotations

import math


def candidates(events) -> dict[str, list]:
    """Merge the bells this run consumed: per account keep the entry with the highest dataset_version.
    Each bell: extra = {"dataset_version": dv, "accounts": {account: marker}, "retry_of": batch_id | None, "requeue": bool}."""
    out: dict[str, list] = {}
    for e in events:
        x = e.extra or {}
        dv = int(x.get("dataset_version", 0))
        for acct, marker in (x.get("accounts") or {}).items():
            if acct not in out or dv > out[acct][0]:
                out[acct] = [dv, str(marker), bool(x.get("retry_of")), bool(x.get("requeue"))]
    return out


def claim(store, wanted: dict[str, list], run_id: str, k: int, b_min: int) -> list[dict]:
    """Keep the accounts whose input changed and is not older than what is done; split into min(K, ceil(n / b_min)) batches.

    Due when: never done; or dataset_version newer than done AND marker different (change, not just a newer landing);
    or a requeue of exactly what failed. Two reads, K writes, whatever n is."""
    done = store.get("done") or {}
    failed = store.get("failed") or {}
    todo = []
    for acct in sorted(wanted):
        dv, marker, retry, requeue = wanted[acct]
        prev = done.get(acct)
        if prev is None or (dv > int(prev[0]) and marker != prev[1]):
            todo.append((acct, dv, marker, retry))
        elif requeue and acct in failed and [dv, marker] == [int(failed[acct][0]), failed[acct][1]]:
            todo.append((acct, dv, marker, retry))
    if not todo:
        return []
    # Accounts that are currently failed ride in their own batch (quarantine): if they fail again they fail alone.
    quarantine = [t for t in todo if t[0] in failed]
    healthy = [t for t in todo if t[0] not in failed]
    k_healthy = max(1, k - 1) if quarantine else k
    n_batches = max(1, min(k_healthy, math.ceil(len(healthy) / b_min))) if healthy else 0
    size = math.ceil(len(healthy) / n_batches) if n_batches else 0
    chunks = [healthy[i * size:(i + 1) * size] for i in range(n_batches)] + ([quarantine] if quarantine else [])
    batches = []
    for i, chunk in enumerate(chunks):
        if not chunk:
            continue
        batch_id = f"{run_id}#{i}"
        versions = {a: [dv, m] for a, dv, m, _ in chunk}
        retry = any(r for _, _, _, r in chunk)
        store.set(f"batch/{batch_id}", {"state": "claimed", "versions": versions, "retry": retry, "culprits": [], "error": None})
        batches.append({"batch_id": batch_id, "accounts": [a for a, _, _, _ in chunk], "versions": versions, "retry": retry})
    return batches


def _set_state(store, batch_id: str, state: str, **fields) -> None:
    rec = store.get(f"batch/{batch_id}") or {}
    store.set(f"batch/{batch_id}", {**rec, "state": state, **fields})


def running(store, batch_id: str) -> None:
    _set_state(store, batch_id, "running")


def publish(store, batch: dict) -> dict:
    _set_state(store, batch["batch_id"], "done")
    return {"done": batch["accounts"], "versions": batch["versions"]}


def fail(store, batch: dict, error: str, culprits: list[str]) -> None:
    """After Airflow's last retry: only this batch is marked failed. `culprits` are the accounts the engine named, if any."""
    _set_state(store, batch["batch_id"], "failed", error=error[:300], culprits=sorted(culprits))


def finalize(store, batches: list[dict]) -> dict:
    """Once per run: fold batch results into done/failed (single writer). Returns the healthy accounts of failed batches
    that should be retried once (blast-radius rule a+c): when the engine named culprits and the batch was not already a
    retry, only the culprits go to failed and the rest ring the bell again."""
    done = store.get("done") or {}
    failed = store.get("failed") or {}
    retry_accounts: dict[str, list] = {}
    n_done = n_failed = 0
    for b in batches:
        rec = store.get(f"batch/{b['batch_id']}") or {}
        if rec.get("state") == "done":
            for a, (dv, m) in b["versions"].items():
                if a not in done or dv >= int(done[a][0]):
                    done[a] = [dv, m]
                failed.pop(a, None); n_done += 1
        elif rec.get("state") == "failed":
            culprits = set(rec.get("culprits") or [])
            isolate = bool(culprits) and not rec.get("retry")
            for a, (dv, m) in b["versions"].items():
                if isolate and a not in culprits:
                    retry_accounts[a] = [dv, m]
                else:
                    failed[a] = [dv, m]; n_failed += 1
    store.set("done", done)
    store.set("failed", failed)
    return {"done": n_done, "failed": n_failed, "retry": retry_accounts, "accounts_tracked": len(done)}
