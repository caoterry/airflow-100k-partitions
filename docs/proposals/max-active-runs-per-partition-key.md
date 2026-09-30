# Proposal: per-partition-key concurrency control for partitioned asset scheduling

*Draft for dev@airflow.apache.org / a GitHub discussion. Author: Terry Cao. Evidence: https://github.com/caoterry/airflow-100k-partitions
(experiment E1, `bench/exp_e1.py`; prototype `patches/patch_e_partition_key_mutex.py`). Target: Airflow 3.4/3.5.*

## Problem

With `PartitionedAssetTimetable` (AIP-76), every asset event for a key becomes its own provisional run
(`asset_partition_dag_run`) and, once satisfied, its own DagRun. There is no per-key coordination:

- an event for key K that arrives while a run for K is RUNNING creates a **second, concurrent** run for K;
- an event for K that arrives while a run for K is QUEUED creates **another** run instead of being absorbed;
- the only throttle is the DAG-wide `max_active_runs`, so unrelated keys queue FIFO behind duplicates of a hot key.

Measured on 3.3.2 (consumer with `max_active_runs=2`, 45-second task):

| step | event | as shipped |
|---|---|---|
| 1 | K1 | run #1 running |
| 2 | K1 again while #1 running | run #2 created and **running concurrently** |
| 3 | K1 + K5 while two K1 runs run | both queued; **K5 waits behind the K1 duplicate** |
| 4 | K1 again while a K1 run is queued | a third K1 run is provisioned |
| end | | K1 ran 4 times (two concurrently), K5 once |

For entity-keyed workloads (firm account, customer, device) this is the wrong default: re-computing a key while a run for
it is in flight wastes compute, can finish out of order (an older input version landing after a newer one), and lets one
noisy key starve the rest. Related: #71070 / #71074 de-duplicate *pending* provisional runs only; #71072 audits the
per-tick cap; neither addresses queued or running runs.

## Proposal

A per-DAG setting, default preserving today's behaviour:

```python
DAG(..., schedule=PartitionedAssetTimetable(...), max_active_runs_per_partition_key=1)
```

Semantics when set to 1 (the only value that needs to exist initially):

1. **Mutex (scheduler).** In `_create_dagruns_for_partitioned_asset_dags`, a satisfied provisional run whose key already has
   a QUEUED or RUNNING DagRun in the same DAG is *held* (stays pending) and re-evaluated on later ticks.
2. **Conflation (asset manager).** In `_get_or_create_apdr`, an event for a key whose latest provisional run has become a
   DagRun that is still QUEUED re-uses that provisional run (its `partitioned_asset_key_log` row is attached to it) instead
   of provisioning a new one. Events during a RUNNING run fall into the existing "reuse the pending APDR" path, so at most one
   pending run exists per key.

Net effect: at most one running and one pending run per key; every burst of events during a run yields exactly one
follow-up run; other keys are admitted as soon as capacity allows.

Prototype result (same sequence as above):

| step | with the prototype |
|---|---|
| 2 | provisional run held; run #2 starts the second run #1 finishes |
| 3 | **K5 starts immediately**; K1 stays pending |
| 4 | absorbed into the pending K1 run (no new row) |
| end | K1 ran 3 times, **never concurrently**; K5 once |

## Prototype diff (against 3.3.2; ~30 lines)

`jobs/scheduler_job_runner.py`, after the asset-condition evaluation and before `create_dagrun`:

```python
if not evaluator.run(timetable.asset_condition, statuses=statuses):
                continue

            # Patch E (100k-partition experiment): per-partition-key mutex. If a run for this key is already
            # QUEUED/RUNNING in this Dag, keep the provisional run pending; it will fire once that run finishes and
            # meanwhile keeps absorbing further events for the key (see AssetManager._get_or_create_apdr).
            if conf.getboolean("scheduler", "partition_key_mutex", fallback=True):
                from sqlalchemy import func as _sa_func

                _active = session.scalar(
                    select(_sa_func.count())
                    .select_from(DagRun)
                    .where(
                        DagRun.dag_id == apdr.target_dag_id,
                        DagRun.partition_key == apdr.partition_key,
                        DagRun.state.in_((DagRunState.QUEUED, DagRunState.RUNNING)),
                    )
                )
                if _active:
                    self.log.debug("Holding partition run for %s/%s: %s active", apdr.target_dag_id, apdr.partition_key, _active)
                    continue

            partition_dag_ids.add(apdr.target_dag_id)
            run_after = timezone.utcnow()
```

`assets/manager.py`, at the top of the `latest_apdr` handling in `_get_or_create_apdr`:

```python
if latest_apdr and latest_apdr.created_dag_run_id is not None:
                # Patch E (100k-partition experiment): conflation. If the latest run for this key has not started
                # yet (QUEUED), attach this event to it instead of provisioning another run.
                from airflow.models.dagrun import DagRun as _DagRun
                from airflow.utils.state import DagRunState as _DagRunState

                _state = session.scalar(select(_DagRun.state).where(_DagRun.id == latest_apdr.created_dag_run_id))
                if _state == _DagRunState.QUEUED:
                    cls.logger().debug("Absorbing event for key %s into queued run %s", target_key, latest_apdr.created_dag_run_id)
                    return latest_apdr
            if latest_apdr and latest_apdr.created_dag_run_id is None:
                existing_partition_date = latest_apdr.partition_date
```

(The prototype reads a global `[scheduler] partition_key_mutex` flag; the proposal is a per-DAG argument serialized with the
DAG, defaulting to unlimited.)

## Open questions

1. **Held runs and the per-tick cap.** Held provisional runs still count against `MAX_PARTITION_DAG_RUNS_PER_LOOP` (500) and
   sit at the head of the FIFO; with many hot keys they could starve satisfiable runs (the head-of-line issue already noted in
   the scheduler comments). Options: exclude keys with active runs in the pending query, or make the cap configurable.
2. **Where the query lives.** The mutex check is one `COUNT(*)` on `dag_run (dag_id, partition_key, state)` per satisfied
   provisional run; `dag_run.partition_key` is currently unindexed, so this should land together with an index on
   `(dag_id, partition_key)` (proposed separately with the `asset_partition_dag_run` indexes).
3. **Conflation and consumed events.** An event absorbed into a QUEUED run is not added to that run's
   `consumed_asset_events` in the prototype; it should be, so `triggering_asset_events` stays truthful.
4. **Values > 1.** A bounded number of concurrent runs per key is a natural generalization but has no known use case yet.
5. **Interaction with `clearPartitions` / manual reruns** — a manually triggered run with the same key should count as active.

## Why upstream rather than an extension

The conflation half fits `[core] asset_manager_class`; the mutex half is inside the scheduler's run-creation loop and cannot
be replaced by configuration. Both halves are needed for the semantics users expect from "one run per partition key at a time".
