#!/usr/bin/env python
"""100k experiments on the v2 design, landing through the public REST API the way a production producer would
(POST /api/v2/assets/events on v2_inputs_landed). Every number in the result comes from the metadata DB (asset_event,
dag_run, task_instance, asset_state_store) or from the client's own clock for the POSTs.

Scenarios (each lands 100,000 partitions X000001..X100000):
  one     one landing that lists all 100k partitions -> landing-to-done latency end to end
  burst   200 landings x 500 partitions, paced, with the batcher PAUSED; then unpause and ring one bell (the old scale test)
  stream  200 landings x 500 partitions, paced, batcher live (realistic flow)
  race    two input datasets: positions and trades land 100k each while an earlier job is still running, to test whether
          done ends up recording a (positions, trades) pair that no job computed (LESSON 6)

Engine stand-in time per partition is set with --per-partition-s (rewrites PER_PARTITION_S in dags/v2_dags.py; each task run
re-parses the file, so it takes effect for the next task). Run from v2/ with env.sh sourced.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import time
from pathlib import Path

import psycopg2
import requests

API = "http://localhost:8090/api/v2"
DSN = "postgresql://airflow:airflow@localhost:5433/airflow_v2"
V2 = Path(__file__).resolve().parents[1]
DAGS = V2 / "dags" / "v2_dags.py"
REC = V2 / "recordings"
MECH = ("v2_debounce", "v2_batcher", "v2_ledger")
N = 100_000
PARTS = [f"X{n:06d}" for n in range(1, N + 1)]

_conn = None
S = requests.Session()
S.trust_env = False


def cur():
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(DSN)
        _conn.autocommit = True
    return _conn.cursor()


def q(sql, args=()):
    c = cur()
    c.execute(sql, args)
    return c.fetchall() if c.description else None


def now_utc():
    return dt.datetime.now(dt.timezone.utc)


def log(msg):
    print(f"[{now_utc():%H:%M:%S}] {msg}", flush=True)


def asset_id(name):
    return q("SELECT id FROM asset WHERE name=%s", (name,))[0][0]


def land(dataset: str, version: int, parts: list[str], marker=lambda p, v: f"{v}:{p}") -> dict:
    body = {"asset_id": asset_id("v2_inputs_landed"),
            "extra": {"dataset": dataset, "version": version, "partitions": {p: marker(p, version) for p in parts}}}
    raw = json.dumps(body)
    t = time.time()
    r = S.post(f"{API}/assets/events", data=raw, headers={"Content-Type": "application/json"}, timeout=120)
    el = time.time() - t
    out = {"status": r.status_code, "seconds": round(el, 3), "body_bytes": len(raw), "n": len(parts), "sent_at": t}
    if r.ok:
        j = r.json()
        out.update(event_id=j.get("id"), event_ts=j.get("timestamp"))
    else:
        out["error"] = r.text[:300]
    return out


def pause(dag_id: str, flag: bool):
    r = S.patch(f"{API}/dags/{dag_id}", json={"is_paused": flag}, timeout=30)
    r.raise_for_status()


def set_engine(per_partition_s: float):
    src = DAGS.read_text()
    new = re.sub(r"^PER_PARTITION_S = .*$", f"PER_PARTITION_S = {per_partition_s}     # exp100k: stand-in engine time per partition",
                 src, count=1, flags=re.M)
    if new != src:
        DAGS.write_text(new)
    log(f"engine: PER_PARTITION_S = {per_partition_s}")
    time.sleep(2)


def active_runs():
    return q("SELECT dag_id, count(*) FROM dag_run WHERE dag_id = ANY(%s) AND state IN ('running','queued') GROUP BY 1", (list(MECH),))


def idle():
    if active_runs():
        return False
    return q("SELECT count(*) FROM asset_dag_run_queue")[0][0] == 0


def store_get(key):
    r = q("SELECT s.value FROM asset_state_store s JOIN asset a ON a.id=s.asset_id WHERE a.name='v2_pnl' AND s.key=%s", (key,))
    if not r:
        return None
    v = r[0][0]
    while isinstance(v, str):
        v = json.loads(v)
    return v


def done_count(dataset, version):
    d = store_get("done") or {}
    return sum(1 for p in PARTS if (d.get(p) or {}).get(dataset, [None])[0] == version)


def wait(cond, timeout, poll=3.0, what=""):
    t = time.time()
    while time.time() - t < timeout:
        if cond():
            return round(time.time() - t, 1)
        time.sleep(poll)
    raise TimeoutError(f"timeout after {timeout}s waiting for {what}")


def wait_done(dataset, version, timeout):
    """Until every partition has `version` of `dataset` in done AND the mechanism is idle."""
    state = {"n": 0}

    def ok():
        state["n"] = done_count(dataset, version)
        return state["n"] == N and idle()

    try:
        return wait(ok, timeout, 3.0, f"done {dataset} v{version}")
    except TimeoutError:
        log(f"TIMEOUT: done has {state['n']} of {N} at {dataset} v{version}; active runs {active_runs()}")
        raise


def overlap_max(intervals):
    ev = sorted([(s, 1) for s, e in intervals if s and e] + [(e, -1) for s, e in intervals if s and e], key=lambda x: (x[0], x[1]))
    cur_n = best = 0
    for _, d in ev:
        cur_n += d
        best = max(best, cur_n)
    return best


def collect(t0: dt.datetime, t1: dt.datetime) -> dict:
    """Everything the mechanism did between t0 and t1, from the metadata DB."""
    m = {}
    m["asset_events"] = [
        {"asset": a, "rows": n, "max_extra_bytes": mx}
        for a, n, mx in q("""SELECT a.name, count(*), max(length(coalesce(e.extra::text,''))) FROM asset_event e JOIN asset a ON a.id=e.asset_id
                             WHERE e.timestamp BETWEEN %s AND %s GROUP BY a.name ORDER BY a.name""", (t0, t1))]
    m["dag_runs"] = [{"dag": d, "state": s, "n": n} for d, s, n in q(
        """SELECT dag_id, state, count(*) FROM dag_run WHERE dag_id = ANY(%s) AND start_date BETWEEN %s AND %s GROUP BY 1,2 ORDER BY 1,2""",
        (list(MECH) + ["v2_exceptions"], t0, t1))]
    m["tasks"] = [{"dag": d, "task": t, "state": s, "n": n, "avg_s": float(a or 0), "max_s": float(x or 0), "retries": int(r or 0)}
                  for d, t, s, n, a, x, r in q(
        """SELECT dag_id, task_id, state, count(*), round(avg(extract(epoch from (end_date-start_date)))::numeric,1),
                  round(max(extract(epoch from (end_date-start_date)))::numeric,1), sum(greatest(try_number-1,0))
           FROM task_instance WHERE dag_id = ANY(%s) AND start_date BETWEEN %s AND %s GROUP BY 1,2,3 ORDER BY 1,2,3""",
        (list(MECH) + ["v2_exceptions"], t0, t1))]
    m["bad_tasks"] = [list(map(str, r)) for r in q(
        """SELECT dag_id, task_id, run_id, state, try_number FROM task_instance WHERE dag_id = ANY(%s) AND start_date BETWEEN %s AND %s
           AND (state IN ('failed','upstream_failed') OR try_number > 1)""", (list(MECH), t0, t1))]
    wins = q("""SELECT e.timestamp, e.extra FROM asset_event e JOIN asset a ON a.id=e.asset_id
                WHERE a.name='v2_inputs_debounced' AND e.timestamp BETWEEN %s AND %s ORDER BY e.timestamp""", (t0, t1))
    m["windows"] = [{"ts": ts.isoformat(), "bells": (x or {}).get("bells"), "n_partitions": (x or {}).get("n_partitions"),
                     "kind": "window" if (x or {}).get("window") else ("retry" if (x or {}).get("retry_of") else "payload")}
                    for ts, x in [(w[0], w[1] if isinstance(w[1], dict) else json.loads(w[1] or "null")) for w in wins]]
    brs = q("""SELECT run_id, start_date, end_date, state FROM dag_run WHERE dag_id='v2_batcher' AND start_date BETWEEN %s AND %s
               ORDER BY start_date""", (t0, t1))
    runs = []
    for run_id, s, e, st in brs:
        tis = q("""SELECT task_id, map_index, state, start_date, end_date FROM task_instance WHERE dag_id='v2_batcher' AND run_id=%s
                   ORDER BY start_date NULLS LAST""", (run_id,))
        d = {f"{t}[{i}]" if i >= 0 else t: (round((te - ts).total_seconds(), 1) if ts and te else None, stt) for t, i, stt, ts, te in tis}
        pnl = q("""SELECT e.extra FROM asset_event e JOIN asset a ON a.id=e.asset_id WHERE a.name='v2_pnl' AND e.source_run_id=%s""", (run_id,))
        n_parts = [((x if isinstance(x, dict) else json.loads(x)) or {}).get("n_partitions") for (x,) in pnl]
        consumed = q("""SELECT count(*) FROM dagrun_asset_event dae JOIN dag_run r ON r.id=dae.dag_run_id WHERE r.run_id=%s AND r.dag_id='v2_batcher'""", (run_id,))[0][0]
        runs.append({"run_id": run_id, "state": st, "start": s.isoformat() if s else None, "secs": round((e - s).total_seconds(), 1) if s and e else None,
                     "events_attached": consumed, "jobs": sum(1 for t, i, *_ in tis if t == "run_batch.spark" and i >= 0),
                     "partitions_per_job": n_parts, "tasks": d})
    m["batcher_runs"] = runs
    m["batcher_runs_working"] = sum(1 for r in runs if r["jobs"])
    m["jobs_total"] = sum(r["jobs"] for r in runs)
    m["max_concurrent_batcher_runs"] = overlap_max([(r[1], r[2]) for r in brs])
    sp = q("""SELECT start_date, end_date FROM task_instance WHERE dag_id='v2_batcher' AND task_id='run_batch.spark' AND start_date BETWEEN %s AND %s""", (t0, t1))
    m["max_concurrent_spark"] = overlap_max(sp)
    tl = {}
    for name, label in (("v2_inputs_landed", "landing"), ("v2_inputs_debounced", "forward"), ("v2_pnl", "publish"), ("v2_batches_finished", "finalize")):
        r = q("""SELECT min(e.timestamp), max(e.timestamp) FROM asset_event e JOIN asset a ON a.id=e.asset_id WHERE a.name=%s
                 AND e.timestamp BETWEEN %s AND %s""", (name, t0, t1))[0]
        tl[f"first_{label}"], tl[f"last_{label}"] = (r[0].isoformat() if r[0] else None), (r[1].isoformat() if r[1] else None)
    r = q("""SELECT max(end_date) FROM task_instance WHERE dag_id='v2_ledger' AND task_id='fold' AND state='success' AND start_date BETWEEN %s AND %s""", (t0, t1))[0]
    tl["last_fold_end"] = r[0].isoformat() if r[0] else None
    m["timeline"] = tl
    sizes = q("""SELECT split_part(s.key,'/',1), count(*), max(length(s.value::text)) FROM asset_state_store s JOIN asset a ON a.id=s.asset_id
                 WHERE a.name='v2_pnl' GROUP BY 1 ORDER BY 1""")
    m["state_store"] = [{"key": k, "n": n, "max_bytes": b} for k, n, b in sizes]
    return m


def secs(a, b):
    if not a or not b:
        return None
    return round((dt.datetime.fromisoformat(b) - dt.datetime.fromisoformat(a)).total_seconds(), 1)


def paced_landings(dataset, version, landings, interval):
    per = N // landings
    out = []
    for i in range(landings):
        t = time.time()
        out.append(land(dataset, version, PARTS[i * per:(i + 1) * per]))
        if out[-1]["status"] >= 300:
            log(f"landing {i} failed: {out[-1]}")
        time.sleep(max(0.0, interval - (time.time() - t)))
    return out


def summary_posts(posts):
    ok = [p for p in posts if p["status"] < 300]
    return {"posts": len(posts), "ok": len(ok), "avg_s": round(sum(p["seconds"] for p in ok) / max(1, len(ok)), 3),
            "max_s": max((p["seconds"] for p in ok), default=None), "max_body_bytes": max((p["body_bytes"] for p in posts), default=None),
            "first_sent": dt.datetime.fromtimestamp(posts[0]["sent_at"], dt.timezone.utc).isoformat() if posts else None,
            "last_sent": dt.datetime.fromtimestamp(posts[-1]["sent_at"], dt.timezone.utc).isoformat() if posts else None}


def run(args) -> dict:
    res = {"scenario": args.scenario, "version": args.version, "per_partition_s": args.per_partition_s, "debounce_s": 10,
           "load_avg_start": os.getloadavg()}
    wait(idle, 300, 3, "idle before start")
    set_engine(args.per_partition_s)
    t0 = now_utc() - dt.timedelta(seconds=1)
    mono0, wall0 = time.monotonic(), time.time()
    if args.scenario == "one":
        posts = [land(args.dataset, args.version, PARTS)]
        res["posts"] = summary_posts(posts)
        res["wait_s"] = wait_done(args.dataset, args.version, args.timeout)
    elif args.scenario in ("burst", "stream"):
        if args.scenario == "burst":
            pause("v2_batcher", True)
        posts = paced_landings(args.dataset, args.version, args.landings, args.interval)
        res["posts"] = summary_posts(posts)
        if args.scenario == "burst":
            # wait until the debounce has forwarded every bell, then let the batcher go and ring one more (same version, same marker)
            def all_forwarded():
                n = q("""SELECT coalesce(sum((e.extra->>'bells')::int),0) FROM asset_event e JOIN asset a ON a.id=e.asset_id
                         WHERE a.name='v2_inputs_debounced' AND e.timestamp >= %s""", (t0,))[0][0]
                return n >= args.landings and not [r for r in (active_runs() or []) if r[0] == "v2_debounce"]
            res["forward_wait_s"] = wait(all_forwarded, args.timeout, 3, "all bells forwarded")
            pause("v2_batcher", False)
            time.sleep(2)
            res["release_post"] = land(args.dataset, args.version, PARTS[:1])
        res["wait_s"] = wait_done(args.dataset, args.version, args.timeout)
    elif args.scenario == "race":
        # a: give every partition a trades version so both inputs are in done (fast engine)
        set_engine(0.0002)
        res["a_post"] = land("trades", args.version, PARTS)
        res["a_wait_s"] = wait_done("trades", args.version, args.timeout)
        ta = now_utc()
        pos_before = (store_get("done") or {}).get(PARTS[0], {})
        res["done_before_b"] = pos_before
        # b: slow engine; positions lands; once its job is running, trades lands again
        set_engine(args.per_partition_s)
        tb = now_utc()
        res["b1_post"] = land("positions", args.version + 1, PARTS)
        res["b1_job_started_after_s"] = wait(lambda: q("""SELECT count(*) FROM task_instance WHERE dag_id='v2_batcher' AND task_id='run_batch.spark'
                                                         AND state='running' AND start_date > %s""", (tb,))[0][0] > 0, 300, 1, "positions job running")
        res["b2_post"] = land("trades", args.version + 1, PARTS)
        res["wait_s"] = wait_done("trades", args.version + 1, args.timeout)
        wait_done("positions", args.version + 1, args.timeout)
        # what did each batch compute, and what does done claim?
        done = store_get("done") or {}
        computed: dict[str, set] = {}
        for (key,) in q("""SELECT s.key FROM asset_state_store s JOIN asset a ON a.id=s.asset_id WHERE a.name='v2_pnl' AND s.key LIKE 'batch/%%'"""):
            rec = store_get(key) or {}
            if rec.get("state") != "done":
                continue
            for p, inp in (rec.get("versions") or {}).items():
                if "positions" in inp and "trades" in inp:
                    computed.setdefault(p, set()).add((int(inp["positions"][0]), int(inp["trades"][0])))
        never = [p for p in PARTS if (int(done[p]["positions"][0]), int(done[p]["trades"][0])) not in computed.get(p, set())]
        res["race"] = {"done_pair_example": done[PARTS[0]], "computed_pairs_example": sorted(computed.get(PARTS[0], [])),
                       "partitions_whose_done_pair_no_job_computed": len(never), "of": N}
        res["race_t_a"], res["race_t_b"] = ta.isoformat(), tb.isoformat()
    t1 = now_utc() + dt.timedelta(seconds=1)
    res["slept"] = round((time.time() - wall0) - (time.monotonic() - mono0), 1)   # wall clock runs on during sleep, monotonic does not
    res["metrics"] = collect(t0, t1)
    tl = res["metrics"]["timeline"]
    res["derived"] = {
        "first_landing_to_last_fold_s": secs(tl.get("first_landing"), tl.get("last_fold_end")),
        "last_landing_to_last_fold_s": secs(tl.get("last_landing"), tl.get("last_fold_end")),
        "first_landing_to_first_forward_s": secs(tl.get("first_landing"), tl.get("first_forward")),
        "windows": sum(1 for w in res["metrics"]["windows"] if w["kind"] == "window"),
        "jobs": res["metrics"]["jobs_total"], "working_batcher_runs": res["metrics"]["batcher_runs_working"],
        "failed_or_retried_tasks": len(res["metrics"]["bad_tasks"]),
    }
    res["load_avg_end"] = os.getloadavg()
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("scenario", choices=["one", "burst", "stream", "race"])
    ap.add_argument("--version", type=int, required=True)
    ap.add_argument("--dataset", default="positions")
    ap.add_argument("--landings", type=int, default=200)
    ap.add_argument("--interval", type=float, default=1.5)
    ap.add_argument("--per-partition-s", type=float, default=0.0002)
    ap.add_argument("--timeout", type=int, default=1800)
    ap.add_argument("--out", default=None)
    ap.add_argument("--keep-engine", action="store_true", help="leave PER_PARTITION_S as the run set it")
    args = ap.parse_args()
    out = Path(args.out or REC / f"exp100k_{args.scenario}.json")
    original = DAGS.read_text()                  # restored at the end, once the mechanism is idle (a 0.5 s/partition engine
    try:                                         # would turn a pending 100k job into a 14-hour sleep)
        res = run(args)
    finally:
        try:
            pause("v2_batcher", False)
        except Exception:
            pass
        if not args.keep_engine:
            try:
                wait(idle, 600, 3, "idle before restoring the Dag file")
                DAGS.write_text(original)
                log("engine setting restored")
            except Exception as exc:
                log(f"NOT restored ({exc}); run: git checkout v2/dags/v2_dags.py once idle")
    out.write_text(json.dumps(res, indent=1, default=str))
    log(f"wrote {out}")
    print(json.dumps({k: res[k] for k in ("scenario", "slept", "derived") if k in res}, indent=1, default=str))
    if "posts" in res:
        print("posts", res["posts"])
    if "race" in res:
        print("race", res["race"])


if __name__ == "__main__":
    sys.exit(main())
