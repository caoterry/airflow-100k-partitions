#!/usr/bin/env python
"""Latency–cost frontier of batching policies per engine profile (from sim_batching.simulate)."""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
from sim_batching import arrivals, simulate, summarize
from pathlib import Path
IMG = Path(__file__).resolve().parents[1] / "docs/img"
plt.rcParams.update({"figure.dpi": 130, "axes.spines.top": False, "axes.spines.right": False, "font.size": 8})
arr = arrivals(10000, 600, 1.0, 4 * 3600)
profiles = [("Glue-class: startup 60 s, 1 s/acct, K=20", 60, 1.0, 20, 30, 300),
            ("Glue-class, subnet-bound: K=8", 60, 1.0, 8, 30, 300),
            ("Warm Spark: startup 10 s, 0.5 s/acct, K=20", 10, 0.5, 20, 30, 120),
            ("Lambda-class: startup 1 s, 2 s/acct, K=1000", 1, 2.0, 1000, 5, 120)]
pols = [("per_account", "o", "#7f7f7f"), ("fixed_pack", "s", "#c0504d"), ("fixed_cadence", "D", "#9bbb59"), ("adaptive", "^", "#4f81bd"), ("adaptive_sla", "*", "#1f4e79")]
fig, axes = plt.subplots(2, 4, figsize=(13, 6.2), sharex="col")
for col, (title, S, p, K, tick, target) in enumerate(profiles):
    for pol, m, cclr in pols:
        jobs, lat, busy, backlog = simulate(pol, arr, K, S, p, tick, 500, 300, 120, target_s=target)
        r = summarize(pol, jobs, lat, busy, 600, K, backlog)
        for row, key in ((0, "burst_p95_min"), (1, "trickle_p95_min")):
            ax = axes[row][col]; y = r[key]
            if y != y or backlog:  # nan or unstable
                ax.plot([r["job_hours"]], [ax.get_ylim()[1] if ax.get_ylim()[1] > 1 else 100], marker=m, color=cclr, ls="", ms=7, alpha=0.4)
                ax.annotate("unstable", (r["job_hours"], ax.get_ylim()[1] if ax.get_ylim()[1] > 1 else 100), fontsize=6, color=cclr, textcoords="offset points", xytext=(4, -8))
                continue
            ax.plot([r["job_hours"]], [y], marker=m, color=cclr, ls="", ms=8 if pol == "adaptive_sla" else 6, label=pol if (row == 0 and col == 0) else None)
            ax.annotate(f"{y:.1f}", (r["job_hours"], y), fontsize=6, textcoords="offset points", xytext=(4, 2), color=cclr)
    axes[0][col].set_title(title, fontsize=8); axes[0][col].set_yscale("log"); axes[1][col].set_yscale("log")
    axes[0][col].axhline(target / 60, color="#4f9d69", ls="--", lw=0.8); axes[1][col].axhline(target / 60, color="#4f9d69", ls="--", lw=0.8)
    axes[1][col].set_xlabel("job-hours consumed (cost proxy)")
axes[0][0].set_ylabel("burst p95 latency (min, log)"); axes[1][0].set_ylabel("trickle p95 latency (min, log)")
axes[0][0].legend(frameon=False, fontsize=7, loc="upper right")
fig.suptitle("Batching policy vs engine profile — 10k-account burst + 1/s trickle; dashed = SLA target", fontsize=10)
fig.tight_layout(); fig.savefig(IMG / "batching_frontier.png"); print("wrote docs/img/batching_frontier.png")
