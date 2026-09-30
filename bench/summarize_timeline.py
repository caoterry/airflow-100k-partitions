#!/usr/bin/env python
"""Derive a summary.json from a timeline.csv when a harness run was stopped early.
Usage: summarize_timeline.py <label> [--n N] [--note "..."]"""
from __future__ import annotations
import argparse, csv, json
from pathlib import Path
RES = Path(__file__).resolve().parent / "results"
a = argparse.ArgumentParser(); a.add_argument("label"); a.add_argument("--n", type=int); a.add_argument("--note", default=""); a.add_argument("--dag", default=""); args = a.parse_args()
rows = list(csv.DictReader(open(RES / args.label / "timeline.csv")))
def f(r, k, d=0.0):
    try: return float(r.get(k) or d)
    except ValueError: return d
last = rows[-1]; n = args.n or int(f(last, "p_consumer_dag_run") or f(last, "mapped_tis"))
def first_t(pred):
    for r in rows:
        if pred(r): return f(r, "t")
    return None
s = {"label": args.label, "dag_id": args.dag, "expected": n, "stopped_early": True, "note": args.note,
     "t_last_sample_s": f(last, "t"),
     "events_s": first_t(lambda r: f(r, "p_asset_event") >= n),
     "runs_created_s": first_t(lambda r: f(r, "p_consumer_dag_run") >= n),
     "t_expanded_s": first_t(lambda r: f(r, "mapped_tis") >= n),
     "peak_scheduler_rss_mb": max(f(r, "scheduler_rss_mb") for r in rows),
     "peak_task_rss_mb": max(f(r, "task_rss_mb") for r in rows),
     "min_sys_avail_mb": min(f(r, "sys_avail_mb", 1e9) for r in rows),
     "final": {k: last.get(k) for k in last if k.startswith(("ti_", "run_", "p_", "sz_"))}}
# completion rate over the last 10 minutes of samples (runs finished per second)
tail = [r for r in rows if f(r, "t") >= f(last, "t") - 600]
if len(tail) > 2:
    def done(r): return f(r, "run_success") + f(r, "run_failed") if "run_success" in r else f(r, "ti_success")
    s["finish_rate_last10min_per_s"] = round((done(tail[-1]) - done(tail[0])) / max(f(tail[-1], "t") - f(tail[0], "t"), 1), 2)
(RES / args.label / "summary.json").write_text(json.dumps(s, indent=2))
print(json.dumps({k: v for k, v in s.items() if k != "final"}, indent=2))
