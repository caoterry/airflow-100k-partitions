"""The journal (variant D, decision D22): inside Airflow's asset_state_store on the calc's OUTPUT asset (v2_pnl), a fixed number of
API calls per run. The key of the calc is a PARTITION (for revenue PnL a firm account; for the balance sheet a region); a calc
has N input DATASETS, each with its own version space.

  done                    {partition: {dataset: [version, marker]}}   written ONLY by the ledger Dag (v2_ledger, one run at a time)
  failed                  {partition: {dataset: [version, marker]}}   same: the input versions a partition failed on
  batch/<run>#<i>         {"state": claimed|running|done|failed, "versions": {partition: {dataset: [version, marker]}},
                           "retry": bool, "culprits": [partitions the engine named], "error": str}   written only by its own run
  window/<window_end>     {"partitions": {partition: {dataset: [version, marker]}}, "bells": n, "window_end": iso}
                          written only by v2_debounce.forward; the debounced event points at it (decision D21: extra = pointer)
  ledger/cutoff           timestamp of the last FINISHED event folded; written only by fold, after done/failed (re-folding is idempotent)

Every key has exactly one writer, so the store needs no locks. K batcher runs overlap (max_active_runs = K = pool size); each
writes only its own batch keys. Correctness of overlapping runs comes from idempotent, version-ordered output writes by the calc
(decision D18), not from exclusion.

Two version spaces per dataset (decision D13): version orders landings (a late, older landing never overwrites a newer result);
marker says whether the partition's input changed (row fingerprint, lake_in_id, or the version itself for delta feeds), compared
for equality only. A partition is due when ANY input dataset has a newer version with a different marker (requirement R1: recompute
with the new version of that input and the latest version of everything else).
"""
from __future__ import annotations

Inputs = dict[str, list]          # {dataset: [version, marker]}


def _merge_inputs(into: dict[str, Inputs], partition: str, inputs: Inputs) -> None:
    """Keep the highest version per (partition, dataset)."""
    cur = into.setdefault(partition, {})
    for ds, (v, m) in inputs.items():
        if ds not in cur or int(v) > int(cur[ds][0]):
            cur[ds] = [int(v), str(m)]


def _partitions_of(extra: dict, store) -> dict[str, Inputs]:
    """The {partition: {dataset: [version, marker]}} an event stands for. Three shapes:
      producer bell   {"dataset": ds, "version": v, "partitions": {partition: marker}}
      pointer         {"window": key} or {"batch": key}: the dict lives in the state store (D21)
      payload         {"partitions": {partition: {dataset: [v, m]}}} (ring_retry, requeue: small lists)
    A tick ({"tick": true}) stands for nothing."""
    extra = extra or {}
    if "dataset" in extra and "partitions" in extra:
        ds, v = str(extra["dataset"]), int(extra.get("version", 0))
        return {p: {ds: [v, str(m)]} for p, m in extra["partitions"].items()}
    key = extra.get("window") or extra.get("batch")
    if key:
        rec = (store.get(key) if store is not None else None) or {}
        return rec.get("partitions") or rec.get("versions") or {}
    return extra.get("partitions") or {}


def candidates(events, store=None) -> dict[str, dict]:
    """Merge the events a run consumed into {partition: {"inputs": {dataset: [version, marker]}, "retry": bool, "requeue": bool}}."""
    out: dict[str, dict] = {}
    for e in events:
        x = e.extra or {}
        for p, inputs in _partitions_of(x, store).items():
            rec = out.setdefault(p, {"inputs": {}, "retry": False, "requeue": False})
            _merge_inputs({"_": rec["inputs"]}, "_", inputs)
            rec["retry"] = rec["retry"] or bool(x.get("retry_of"))
            rec["requeue"] = rec["requeue"] or bool(x.get("requeue"))
    return out


def merge_bells(events) -> dict[str, Inputs]:
    """For the debounce window: producer bells -> {partition: {dataset: [version, marker]}}, highest version per dataset."""
    out: dict[str, Inputs] = {}
    for e in events:
        for p, inputs in _partitions_of(e.extra or {}, None).items():
            _merge_inputs(out, p, inputs)
    return out


def _due(wanted: Inputs, prev: Inputs | None) -> bool:
    prev = prev or {}
    for ds, (v, m) in wanted.items():
        if ds not in prev or (int(v) > int(prev[ds][0]) and str(m) != str(prev[ds][1])):
            return True
    return False


def claim(store, wanted: dict[str, dict], run_id: str) -> list[dict]:
    """Variant D: one job per run. Keep the partitions whose input changed and is not older than what is done (`done` is
    advisory: written by the ledger Dag, it may lag a few seconds, so at worst a partition is computed once more; the versioned,
    idempotent output makes that harmless). The job computes a partition with the newest version of every input it knows, so
    the batch records {**done[p], **wanted[p]}. Partitions currently in `failed` ride in their own quarantine job."""
    if not wanted:
        return []
    done = store.get("done") or {}
    failed = store.get("failed") or {}
    todo: list[tuple[str, Inputs, bool]] = []
    for p in sorted(wanted):
        inputs, retry, requeue = wanted[p]["inputs"], wanted[p]["retry"], wanted[p]["requeue"]
        if _due(inputs, done.get(p)):
            todo.append((p, {**(done.get(p) or {}), **inputs}, retry))
        elif requeue and p in failed and {k: [int(a), str(b)] for k, (a, b) in failed[p].items()} == {k: [int(a), str(b)] for k, (a, b) in inputs.items()}:
            todo.append((p, {**(done.get(p) or {}), **inputs}, retry))
    if not todo:
        return []
    healthy = [t for t in todo if t[0] not in failed]
    quarantine = [t for t in todo if t[0] in failed]
    batches = []
    for i, chunk in enumerate([c for c in (healthy, quarantine) if c]):
        batch_id = f"{run_id}#{i}"
        versions = {p: inputs for p, inputs, _ in chunk}
        retry = any(r for _, _, r in chunk)
        store.set(f"batch/{batch_id}", {"state": "claimed", "versions": versions, "retry": retry, "culprits": [], "error": None})
        batches.append({"batch_id": batch_id, "partitions": [p for p, _, _ in chunk], "versions": versions, "retry": retry})
    return batches


def _set_state(store, batch_id: str, state: str, **fields) -> None:
    rec = store.get(f"batch/{batch_id}") or {}
    store.set(f"batch/{batch_id}", {**rec, "state": state, **fields})


def running(store, batch_id: str) -> None:
    _set_state(store, batch_id, "running")


def publish(store, batch: dict) -> dict:
    _set_state(store, batch["batch_id"], "done")
    datasets = sorted({ds for inputs in batch["versions"].values() for ds in inputs})
    return {"done": batch["partitions"], "n_partitions": len(batch["partitions"]), "datasets": datasets, "batch": f"batch/{batch['batch_id']}"}


def fail(store, batch: dict, error: str, culprits: list[str]) -> None:
    """After Airflow's last retry: only this batch is marked failed. `culprits` are the partitions the engine named, if any."""
    _set_state(store, batch["batch_id"], "failed", error=error[:300], culprits=sorted(culprits))


def summarize(store, batches: list[dict]) -> dict:
    """Per run: read this run's batch keys and report what happened. Writes only this run's own keys.
    Returns the partitions that should ring again (blast-radius rule a+c: healthy partitions of a failed batch; and every
    partition of a batch whose job never reached publish, once)."""
    retry: dict[str, Inputs] = {}
    n_done = n_failed = 0
    for b in batches:
        key = f"batch/{b['batch_id']}"
        rec = store.get(key) or {}
        state = rec.get("state")
        if state == "done":
            n_done += len(b["versions"])
        elif state == "failed":
            culprits = set(rec.get("culprits") or [])
            isolate = bool(culprits) and not rec.get("retry")
            for p, inputs in b["versions"].items():
                if isolate and p not in culprits:
                    retry[p] = inputs
                else:
                    n_failed += 1
        else:
            # still claimed/running when the run finished: the job never reached publish (worker or bookkeeping failure, not the
            # engine). Ring the partitions once more; if this batch WAS already the retry, give up and mark it failed so the
            # ledger records it and the run turns red (no infinite loop on a job that keeps dying).
            if rec.get("retry"):
                store.set(key, {**rec, "state": "failed", "error": "job finished without publishing, twice", "culprits": []})
                n_failed += len(b["versions"])
            else:
                for p, inputs in b["versions"].items():
                    retry[p] = inputs
    return {"done": n_done, "failed": n_failed, "retry": retry, "batch_ids": [b["batch_id"] for b in batches]}


def fold(store, batch_ids: list[str]) -> dict:
    """The ledger (its own Dag, one run at a time, the ONLY writer of done and failed): fold finished batch keys into the two
    dicts. done keeps the highest version per (partition, dataset); failed holds culprits (or the whole batch when the engine
    named nobody), minus partitions that a later batch computed. Idempotent: folding the same key twice changes nothing."""
    done = store.get("done") or {}
    failed = store.get("failed") or {}
    n_done = n_failed = 0
    for bid in batch_ids:
        rec = store.get(f"batch/{bid}") or {}
        versions = rec.get("versions") or {}
        if rec.get("state") == "done":
            for p, inputs in versions.items():
                cur = done.setdefault(p, {})
                for ds, (v, m) in inputs.items():
                    if ds not in cur or int(v) >= int(cur[ds][0]):
                        cur[ds] = [int(v), str(m)]
                failed.pop(p, None); n_done += 1
        elif rec.get("state") == "failed":
            culprits = set(rec.get("culprits") or [])
            isolate = bool(culprits) and not rec.get("retry")
            for p, inputs in versions.items():
                if not isolate or p in culprits:
                    failed[p] = inputs; n_failed += 1
    store.set("done", done)
    store.set("failed", failed)
    return {"done": n_done, "failed": n_failed, "partitions_tracked": len(done)}
