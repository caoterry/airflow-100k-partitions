#!/usr/bin/env python
"""Summarise bench/results/*/summary.json (+ trigger.json, api_latency.json) as markdown tables."""
from __future__ import annotations
import json, sys
from pathlib import Path

RES = Path(__file__).resolve().parent / "results"
rows = []
for d in sorted(RES.iterdir()):
    s = d / "summary.json"
    if not s.exists():
        continue
    j = json.loads(s.read_text()); f = j.get("final") or {}
    trig = json.loads((d / "trigger.json").read_text()) if (d / "trigger.json").exists() else {}
    lat = json.loads((d / "api_latency.json").read_text()) if (d / "api_latency.json").exists() else {}
    rows.append({
        "label": j["label"], "dag": j["dag_id"], "N": j["expected"],
        "expand_s": j.get("t_expanded_s"), "done_s": j.get("t_done_s"), "run_dur_s": j.get("dagrun_duration_s"),
        "state": j.get("dagrun_state") or ("timeout" if j.get("timed_out") else "-"),
        "success": f.get("ti_success", f.get("run_success")), "failed": f.get("ti_failed", f.get("run_failed", 0)),
        "tis/s": j.get("exec_throughput_tis_per_s"),
        "sched_peak_MB": j.get("peak_scheduler_rss_mb"), "task_peak_MB": j.get("peak_task_rss_mb"), "task_n_peak": j.get("peak_task_n"),
        "min_avail_MB": j.get("min_sys_avail_mb"),
        "ti_tbl_MB": f.get("sz_task_instance_mb"), "xcom_MB": f.get("sz_xcom_mb"), "rtif_MB": f.get("sz_rendered_task_instance_fields_mb"), "db_MB": f.get("sz__database_mb"),
        "trig_rate/s": trig.get("trigger_rate_per_s"), "trig_p99_ms": trig.get("p99_ms"),
        "api_list100_ms": (lat.get("GET taskInstances?limit=100") or {}).get("ms"),
        "grid_summ_ms": (lat.get("GET ui/grid/ti_summaries") or {}).get("ms"),
        "grid_runs_ms": (lat.get("GET ui/grid/runs") or {}).get("ms"),
    })
if not rows:
    sys.exit("no results")
cols = list(rows[0].keys())
print("| " + " | ".join(cols) + " |"); print("|" + "---|" * len(cols))
for r in rows:
    print("| " + " | ".join("" if r[c] is None else str(r[c]) for c in cols) + " |")
