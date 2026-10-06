"""The journal, variant C: inside Airflow's asset_state_store, with a FIXED number of API calls per run.

Keys on the v2_positions asset (only the batcher writes; max_active_runs=1 means one writer at a time):
  done              {account: [dataset_version, marker]}   written ONLY by the ledger Dag (v2_ledger), a single writer
  failed            {account: [dataset_version, marker]}   same
  batch/<run>#<i>   {"state": claimed|running|done|failed, "versions": {account: [dv, marker]}, "retry": bool,
                     "culprits": [accounts the engine named], "error": str}

Two version spaces (decision D13):
  dataset_version  orders landings: a late, older landing never overwrites a newer result
  marker           says whether the account's input changed (row fingerprint, lake_in_id, or the dataset version itself
                   for delta feeds); compared for equality only, so it needs no ordering
Variant D: one job per batcher run (plus a quarantine job), max_active_runs = K on the batcher so K runs overlap and the K
engine slots stay busy; correctness comes from idempotent, version-ordered output writes, not from exclusion.
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


def claim(store, wanted: dict[str, list], run_id: str) -> list[dict]:
    """Variant D: one job per run. Keep the accounts whose input changed and is not older than what is done
    (`done` is advisory here: it is written by the ledger Dag and may lag a few seconds, so at worst an account is
    computed once more; the versioned, idempotent output makes that harmless). Accounts currently in `failed` ride
    in their own quarantine job so that a persistently bad account fails alone."""
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
    healthy = [t for t in todo if t[0] not in failed]
    quarantine = [t for t in todo if t[0] in failed]
    batches = []
    for i, chunk in enumerate([c for c in (healthy, quarantine) if c]):
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


def summarize(store, batches: list[dict]) -> dict:
    """Per run: read this run's batch keys and report what happened. Writes nothing shared.
    Returns the healthy accounts of failed batches that should be retried once (blast-radius rule a+c)."""
    retry_accounts: dict[str, list] = {}
    n_done = n_failed = 0
    for b in batches:
        rec = store.get(f"batch/{b['batch_id']}") or {}
        if rec.get("state") == "done":
            n_done += len(b["versions"])
        elif rec.get("state") == "failed":
            culprits = set(rec.get("culprits") or [])
            isolate = bool(culprits) and not rec.get("retry")
            for a, (dv, m) in b["versions"].items():
                if isolate and a not in culprits:
                    retry_accounts[a] = [dv, m]
                else:
                    n_failed += 1
    return {"done": n_done, "failed": n_failed, "retry": retry_accounts, "batch_ids": [b["batch_id"] for b in batches]}


def fold(store, batch_ids: list[str]) -> dict:
    """The ledger (its own Dag, max_active_runs=1, the ONLY writer of done and failed): fold finished batch keys
    into the two dicts. done keeps the highest dataset_version per account; failed holds culprits (or the whole
    batch when the engine named nobody), minus accounts that a later batch computed."""
    done = store.get("done") or {}
    failed = store.get("failed") or {}
    n_done = n_failed = 0
    for bid in batch_ids:
        rec = store.get(f"batch/{bid}") or {}
        versions = rec.get("versions") or {}
        if rec.get("state") == "done":
            for a, (dv, m) in versions.items():
                if a not in done or dv >= int(done[a][0]):
                    done[a] = [dv, m]
                failed.pop(a, None); n_done += 1
        elif rec.get("state") == "failed":
            culprits = set(rec.get("culprits") or [])
            isolate = bool(culprits) and not rec.get("retry")
            for a, (dv, m) in versions.items():
                if not isolate or a in culprits:
                    failed[a] = [dv, m]; n_failed += 1
    store.set("done", done)
    store.set("failed", failed)
    return {"done": n_done, "failed": n_failed, "accounts_tracked": len(done)}
