#!/usr/bin/env python
"""Benchmark harness for the 100k-partition experiments (Airflow 3.3.x, Postgres, LocalExecutor).

  harness.py run   --dag bench_flat_empty --conf '{"n": 10000}' --label flat_empty_10k [--timeout 7200]
  harness.py bulk  --dag bench_run_per_account --n 10000 --concurrency 32 --label rpa_10k
  harness.py probe --dag bench_flat_empty --run-id <run_id> --label flat_empty_10k     # API latency only
  harness.py dbsize

Writes bench/results/<label>/timeline.csv, summary.json, api_latency.json.
Timeline samples (every --interval s): TI state histogram for the run(s), process RSS by component,
Postgres table sizes. Summary derives: t_expanded (all mapped TIs exist), t_done, throughput.
"""
from __future__ import annotations

import argparse, asyncio, csv, json, os, sys, time
from datetime import datetime, timezone
from pathlib import Path

import httpx, psutil, psycopg2

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results"
API = os.environ.get("BENCH_API", "http://localhost:8080")
DSN = "dbname=airflow user=airflow password=airflow host=localhost port=5433"
TABLES = ["task_instance", "xcom", "task_map", "dag_run", "rendered_task_instance_fields", "task_instance_history", "log", "asset_event", "task_reschedule"]


def now() -> float:
    return time.time()


def iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------- auth ----------
def token() -> str:
    pw = "admin"
    gen = Path(os.environ.get("AIRFLOW_HOME", ROOT.parent / "airflow_home")) / "simple_auth_manager_passwords.json.generated"
    if gen.exists():
        try:
            pw = json.loads(gen.read_text()).get("admin", pw)
        except Exception:
            pass
    r = httpx.post(f"{API}/auth/token", json={"username": "admin", "password": pw}, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def client(tok: str, timeout: float = 120) -> httpx.Client:
    return httpx.Client(base_url=API, headers={"Authorization": f"Bearer {tok}"}, timeout=timeout)


# ---------- db ----------
def db():
    return psycopg2.connect(DSN)


def table_sizes(cur) -> dict:
    out = {}
    for t in TABLES:
        cur.execute("select pg_total_relation_size(%s)", (t,))
        out[t] = cur.fetchone()[0]
    cur.execute("select pg_database_size('airflow')")
    out["_database"] = cur.fetchone()[0]
    return out


def ti_hist(cur, dag_id: str, run_id: str | None) -> dict:
    if run_id:
        cur.execute("select coalesce(state,'none'), count(*) from task_instance where dag_id=%s and run_id=%s group by 1", (dag_id, run_id))
    else:
        cur.execute("select coalesce(state,'none'), count(*) from task_instance where dag_id=%s group by 1", (dag_id,))
    return dict(cur.fetchall())


def run_hist(cur, dag_id: str) -> dict:
    cur.execute("select coalesce(state,'none'), count(*) from dag_run where dag_id=%s group by 1", (dag_id,))
    return dict(cur.fetchall())


def mapped_count(cur, dag_id: str, run_id: str) -> int:
    cur.execute("select count(*) from task_instance where dag_id=%s and run_id=%s and map_index>=0", (dag_id, run_id))
    return cur.fetchone()[0]


def run_state(cur, dag_id: str, run_id: str):
    cur.execute("select state, start_date, end_date from dag_run where dag_id=%s and run_id=%s", (dag_id, run_id))
    return cur.fetchone()


# ---------- processes ----------
def proc_metrics() -> dict:
    agg = {"scheduler": [0, 0], "api-server": [0, 0], "dag-processor": [0, 0], "task": [0, 0], "other": [0, 0]}
    for p in psutil.process_iter(["cmdline", "memory_info", "name"]):
        try:
            cmd = " ".join(p.info["cmdline"] or [])
            if "airflow" not in cmd or "harness.py" in cmd:
                continue
            rss = p.info["memory_info"].rss
            if "scheduler" in cmd: k = "scheduler"
            elif "api-server" in cmd or "uvicorn" in cmd or "gunicorn" in cmd: k = "api-server"
            elif "dag-processor" in cmd: k = "dag-processor"
            elif "task" in cmd or "supervisor" in cmd or "LocalExecutor" in cmd: k = "task"
            else: k = "other"
            agg[k][0] += 1; agg[k][1] += rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    vm = psutil.virtual_memory()
    out = {f"{k}_n": v[0] for k, v in agg.items()} | {f"{k}_rss_mb": round(v[1] / 2**20, 1) for k, v in agg.items()}
    out["sys_used_mb"] = round((vm.total - vm.available) / 2**20)
    out["sys_avail_mb"] = round(vm.available / 2**20)
    out["cpu_pct"] = psutil.cpu_percent(interval=None)
    return out


# ---------- sampling loop ----------
def sample_loop(label: str, dag_id: str, run_id: str | None, expected: int, interval: float, timeout: float, done_fn, extra_fn=None):
    out = RESULTS / label; out.mkdir(parents=True, exist_ok=True)
    conn = db(); conn.autocommit = True; cur = conn.cursor()
    t0 = now(); rows = []; t_expanded = None; t_first_success = None; last_print = 0
    fields = None
    with open(out / "timeline.csv", "w", newline="") as f:
        w = None
        while True:
            t = now()
            h = ti_hist(cur, dag_id, run_id)
            rh = run_hist(cur, dag_id) if run_id is None else {}
            mc = mapped_count(cur, dag_id, run_id) if run_id else sum(h.values())
            if t_expanded is None and mc >= expected: t_expanded = t
            if t_first_success is None and h.get("success", 0) > 0: t_first_success = t
            row = {"ts": iso(), "t": round(t - t0, 1), "mapped_tis": mc, **{f"ti_{k}": v for k, v in h.items()},
                   **{f"run_{k}": v for k, v in rh.items()}, **proc_metrics(), **{f"sz_{k}_mb": round(v / 2**20, 1) for k, v in table_sizes(cur).items()}}
            if extra_fn:
                row.update(extra_fn(cur))
            rows.append(row)
            if w is None or set(row) - set(fields):
                fields = sorted(set(fields or []) | set(row), key=lambda k: (k != "ts", k != "t", k))
                f.seek(0); f.truncate(); w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
                for r in rows: w.writerow(r)
            else:
                w.writerow(row)
            f.flush()
            if t - last_print > 15:
                ex = {k: v for k, v in row.items() if k.startswith("p_")}
                print(f"[{row['t']:>7}s] mapped={mc} {h} {ex} sched_rss={row['scheduler_rss_mb']}MB tasks={row['task_n']} avail={row['sys_avail_mb']}MB", flush=True)
                last_print = t
            d = done_fn(cur)
            if d or (t - t0) > timeout:
                break
            time.sleep(interval)
    summary = {"label": label, "dag_id": dag_id, "run_id": run_id, "expected": expected, "t0": datetime.fromtimestamp(t0, timezone.utc).isoformat(),
               "t_expanded_s": None if t_expanded is None else round(t_expanded - t0, 1),
               "t_first_success_s": None if t_first_success is None else round(t_first_success - t0, 1),
               "t_done_s": round(now() - t0, 1), "timed_out": (now() - t0) > timeout, "final": rows[-1] if rows else None,
               "peak_scheduler_rss_mb": max((r["scheduler_rss_mb"] for r in rows), default=None),
               "peak_task_rss_mb": max((r["task_rss_mb"] for r in rows), default=None),
               "peak_task_n": max((r["task_n"] for r in rows), default=None),
               "min_sys_avail_mb": min((r["sys_avail_mb"] for r in rows), default=None)}
    if t_expanded and summary["final"]:
        dur = max(now() - t_expanded, 0.001)
        summary["exec_throughput_tis_per_s"] = round(summary["final"].get("ti_success", 0) / dur, 2)
    if run_id:
        st = run_state(cur, dag_id, run_id)
        if st:
            summary["dagrun_state"] = st[0]
            summary["dagrun_start"] = st[1].isoformat() if st[1] else None
            summary["dagrun_end"] = st[2].isoformat() if st[2] else None
            if st[1] and st[2]: summary["dagrun_duration_s"] = round((st[2] - st[1]).total_seconds(), 1)
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    print(json.dumps({k: v for k, v in summary.items() if k != "final"}, indent=2, default=str))
    return summary


# ---------- commands ----------
def cmd_run(a):
    tok = token(); c = client(tok)
    conf = json.loads(a.conf) if a.conf else {}
    run_id = a.run_id or f"{a.label}_{int(now())}"
    r = c.post(f"/api/v2/dags/{a.dag}/dagRuns", json={"dag_run_id": run_id, "conf": conf, "logical_date": None})
    if r.status_code >= 300:
        print(r.status_code, r.text); sys.exit(1)
    print("triggered", a.dag, run_id, conf, flush=True)
    expected = a.expected if a.expected is not None else int(conf.get("n", 0))
    if a.dag == "bench_batched": expected = -(-int(conf.get("n", 0)) // int(conf.get("batch_size", 1000)))
    if a.dag == "bench_two_level_parent": expected = -(-int(conf.get("n", 0)) // int(conf.get("batch_size", 1000)))

    def done(cur):
        st = run_state(cur, a.dag, run_id)
        return bool(st and st[0] in ("success", "failed"))

    s = sample_loop(a.label, a.dag, run_id, expected, a.interval, a.timeout, done)
    if not a.no_probe:
        cmd_probe(argparse.Namespace(dag=a.dag, run_id=run_id, label=a.label, task_id=a.task_id))
    return s


def cmd_bulk(a):
    """Create N dag runs (one per account) as fast as possible, then sample until all finished."""
    tok = token()
    as_of = a.as_of
    ids = [f"ACCT{i:08d}" for i in range(1, a.n + 1)]

    async def go():
        sem = asyncio.Semaphore(a.concurrency); lat = []; errs = 0
        async with httpx.AsyncClient(base_url=API, headers={"Authorization": f"Bearer {tok}"}, timeout=60) as ac:
            async def one(acct):
                nonlocal errs
                async with sem:
                    t = now()
                    r = await ac.post(f"/api/v2/dags/{a.dag}/dagRuns", json={"dag_run_id": f"acct_{acct}__{as_of}_{a.label}", "conf": {"account": acct, "as_of": as_of}, "logical_date": None})
                    lat.append(now() - t)
                    if r.status_code >= 300: errs += 1
            t0 = now()
            await asyncio.gather(*(one(x) for x in ids))
            return now() - t0, lat, errs

    dur, lat, errs = asyncio.run(go())
    lat.sort()
    trig = {"n": a.n, "concurrency": a.concurrency, "trigger_wall_s": round(dur, 1), "trigger_rate_per_s": round(a.n / dur, 1), "errors": errs,
            "p50_ms": round(lat[len(lat) // 2] * 1000, 1), "p99_ms": round(lat[int(len(lat) * 0.99)] * 1000, 1)}
    print("bulk trigger:", trig, flush=True)
    out = RESULTS / a.label; out.mkdir(parents=True, exist_ok=True)
    (out / "trigger.json").write_text(json.dumps(trig, indent=2))

    def done(cur):
        rh = run_hist(cur, a.dag)
        return rh.get("success", 0) + rh.get("failed", 0) >= a.n and rh.get("running", 0) == 0 and rh.get("queued", 0) == 0

    sample_loop(a.label, a.dag, None, a.n, a.interval, a.timeout, done)


PART_TABLES = {
    "asset_event": "select count(*) from asset_event e join asset a on a.id=e.asset_id where a.name='bench_accounts'",
    "apdr": "select count(*) from asset_partition_dag_run where target_dag_id='bench_partition_consumer'",
    "apdr_pending": "select count(*) from asset_partition_dag_run where target_dag_id='bench_partition_consumer' and created_dag_run_id is null",
    "pakl": "select count(*) from partitioned_asset_key_log where target_dag_id='bench_partition_consumer'",
    "dagrun_asset_event": "select count(*) from dagrun_asset_event x join dag_run r on r.id=x.dag_run_id where r.dag_id='bench_partition_consumer'",
    "consumer_dag_run": "select count(*) from dag_run where dag_id='bench_partition_consumer'",
    "consumer_ti": "select count(*) from task_instance where dag_id='bench_partition_consumer'",
    "log_rows": "select count(*) from log where dag_id in ('bench_partition_consumer','bench_partition_producer')",
}


def part_counts(cur) -> dict:
    out = {}
    for k, q in PART_TABLES.items():
        cur.execute(q); out[k] = cur.fetchone()[0]
    return out


def part_clean():
    """Remove all rows from previous partition-scenario runs so counts start from zero."""
    conn = db(); conn.autocommit = True; cur = conn.cursor()
    stmts = [
        "delete from partitioned_asset_key_log where target_dag_id='bench_partition_consumer'",
        "delete from asset_partition_dag_run where target_dag_id='bench_partition_consumer'",
        "delete from dagrun_asset_event where dag_run_id in (select id from dag_run where dag_id='bench_partition_consumer')",
        "delete from task_instance where dag_id in ('bench_partition_consumer','bench_partition_producer')",
        "delete from dag_run where dag_id in ('bench_partition_consumer','bench_partition_producer')",
        "delete from asset_event where asset_id in (select id from asset where name='bench_accounts')",
        "delete from task_map where dag_id='bench_partition_producer'",
        "delete from xcom where dag_id='bench_partition_producer'",
    ]
    for st in stmts:
        try:
            cur.execute(st); print("clean:", st.split(" where")[0], cur.rowcount)
        except Exception as e:
            print("clean failed:", st, e)


def cmd_partition(a):
    if a.clean:
        part_clean()
    tok = token(); c = client(tok)
    run_id = a.run_id or f"{a.label}_{int(now())}"
    conf = {"n": a.n, "emitters": a.emitters}
    r = c.post("/api/v2/dags/bench_partition_producer/dagRuns", json={"dag_run_id": run_id, "conf": conf, "logical_date": None})
    if r.status_code >= 300:
        print(r.status_code, r.text); sys.exit(1)
    print("triggered producer", run_id, conf, flush=True)
    conn0 = db(); base = part_counts(conn0.cursor()); conn0.close()
    marks = {}

    def extra(cur):
        pc = part_counts(cur)
        d = {f"p_{k}": v - base[k] for k, v in pc.items()}
        d["p_apdr_pending"] = pc["apdr_pending"]
        st = run_state(cur, "bench_partition_producer", run_id)
        d["p_producer_state"] = st[0] if st else None
        cur.execute("select coalesce(state,'none'), count(*) from task_instance where dag_id='bench_partition_producer' and run_id=%s and task_id='emit' group by 1", (run_id,))
        d["p_emit_tis"] = json.dumps(dict(cur.fetchall()))
        t = round(now() - marks["t0"], 1)
        if "producer_done_s" not in marks and st and st[0] in ("success", "failed"): marks["producer_done_s"] = t; marks["producer_state"] = st[0]
        if "events_s" not in marks and d["p_asset_event"] >= a.n: marks["events_s"] = t
        if "apdr_created_s" not in marks and d["p_apdr"] - pc["apdr_pending"] >= a.n: marks["apdr_created_s"] = t
        if "runs_created_s" not in marks and d["p_consumer_dag_run"] >= a.n: marks["runs_created_s"] = t
        return d

    def done(cur):
        st = run_state(cur, "bench_partition_producer", run_id)
        if not st or st[0] not in ("success", "failed"):
            return False
        if st[0] == "failed" and (now() - marks["t0"]) > 60:
            return True
        rh = run_hist(cur, "bench_partition_consumer")
        pc = part_counts(cur)
        finished = (pc["consumer_dag_run"] - base["consumer_dag_run"]) >= a.n and rh.get("running", 0) == 0 and rh.get("queued", 0) == 0
        return finished

    marks["t0"] = now()
    s = sample_loop(a.label, "bench_partition_consumer", None, a.n, a.interval, a.timeout, done, extra_fn=extra)
    conn1 = db(); after = part_counts(conn1.cursor()); conn1.close()
    s["row_deltas"] = {k: after[k] - base[k] for k in after}
    s["marks"] = {k: v for k, v in marks.items() if k != "t0"}
    s["conf"] = conf
    (RESULTS / a.label / "summary.json").write_text(json.dumps(s, indent=2, default=str))
    print("row deltas:", s["row_deltas"]); print("marks:", s["marks"])
    return s


def cmd_probe(a):
    """API/UI latency against a finished run with many mapped TIs."""
    tok = token(); c = client(tok, timeout=300); res = {}
    def timed(name, method, url, **kw):
        t = now()
        try:
            r = c.request(method, url, **kw); ms = round((now() - t) * 1000, 1)
            res[name] = {"ms": ms, "status": r.status_code, "bytes": len(r.content)}
            print(f"{name:45s} {r.status_code} {ms:>9.1f} ms {len(r.content):>10} B", flush=True)
        except Exception as e:
            res[name] = {"error": repr(e)}; print(name, "ERR", repr(e))
    d, rid = a.dag, a.run_id
    timed("GET dagRun", "GET", f"/api/v2/dags/{d}/dagRuns/{rid}")
    timed("GET taskInstances?limit=100", "GET", f"/api/v2/dags/{d}/dagRuns/{rid}/taskInstances", params={"limit": 100})
    timed("GET taskInstances?limit=100&offset=90000", "GET", f"/api/v2/dags/{d}/dagRuns/{rid}/taskInstances", params={"limit": 100, "offset": 90000})
    timed("GET mapped TI point lookup (idx 999)", "GET", f"/api/v2/dags/{d}/dagRuns/{rid}/taskInstances/{a.task_id}/999")
    timed("GET listMapped?limit=50", "GET", f"/api/v2/dags/{d}/dagRuns/{rid}/taskInstances/{a.task_id}/listMapped", params={"limit": 50})
    timed("GET listMapped?state=failed", "GET", f"/api/v2/dags/{d}/dagRuns/{rid}/taskInstances/{a.task_id}/listMapped", params={"limit": 50, "state": "failed"})
    timed("GET ui/grid/structure", "GET", f"/ui/grid/structure/{d}")
    timed("GET ui/grid/runs", "GET", f"/ui/grid/runs/{d}", params={"limit": 25})
    timed("GET ui/grid/ti_summaries", "GET", f"/ui/grid/ti_summaries/{d}/{rid}")
    timed("GET dags list", "GET", "/api/v2/dags", params={"limit": 50})
    (RESULTS / a.label).mkdir(parents=True, exist_ok=True)
    (RESULTS / a.label / "api_latency.json").write_text(json.dumps(res, indent=2))
    return res


def cmd_dbsize(a):
    conn = db(); cur = conn.cursor()
    for k, v in sorted(table_sizes(cur).items(), key=lambda kv: -kv[1]):
        print(f"{k:32s} {v/2**20:10.1f} MB")


if __name__ == "__main__":
    p = argparse.ArgumentParser(); sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run"); r.add_argument("--dag", required=True); r.add_argument("--conf"); r.add_argument("--label", required=True)
    r.add_argument("--run-id"); r.add_argument("--expected", type=int); r.add_argument("--interval", type=float, default=3.0)
    r.add_argument("--timeout", type=float, default=4 * 3600); r.add_argument("--no-probe", action="store_true"); r.add_argument("--task-id", default="noop")
    b = sub.add_parser("bulk"); b.add_argument("--dag", default="bench_run_per_account"); b.add_argument("--n", type=int, required=True)
    b.add_argument("--concurrency", type=int, default=32); b.add_argument("--label", required=True); b.add_argument("--as-of", default="2026-09-29")
    b.add_argument("--interval", type=float, default=3.0); b.add_argument("--timeout", type=float, default=4 * 3600)
    pr = sub.add_parser("probe"); pr.add_argument("--dag", required=True); pr.add_argument("--run-id", required=True); pr.add_argument("--label", required=True); pr.add_argument("--task-id", default="noop")
    pt = sub.add_parser("partition"); pt.add_argument("--n", type=int, required=True); pt.add_argument("--emitters", type=int, default=1)
    pt.add_argument("--label", required=True); pt.add_argument("--run-id"); pt.add_argument("--clean", action="store_true")
    pt.add_argument("--interval", type=float, default=3.0); pt.add_argument("--timeout", type=float, default=4 * 3600)
    sub.add_parser("dbsize")
    a = p.parse_args()
    {"run": cmd_run, "bulk": cmd_bulk, "probe": cmd_probe, "dbsize": cmd_dbsize, "partition": cmd_partition}[a.cmd](a)
