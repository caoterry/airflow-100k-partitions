#!/usr/bin/env python
"""Render the report charts into docs/img/ from bench/results/*/summary.json (+ a few values transcribed from logs)."""
from __future__ import annotations
import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RES = Path(__file__).resolve().parent / "results"; IMG = Path(__file__).resolve().parents[1] / "docs/img"; IMG.mkdir(exist_ok=True)
def S(label): 
    p = RES / label / "summary.json"; return json.loads(p.read_text()) if p.exists() else None
plt.rcParams.update({"figure.dpi": 130, "axes.spines.top": False, "axes.spines.right": False, "font.size": 9})
C1, C2, C3, C4 = "#1f4e79", "#c0504d", "#7f7f7f", "#4f9d69"

# 1. Scheduler loop blocked by one expansion, as shipped vs Patch A
Ns = [10000, 30000, 100000]
ship = [S(f"flat_empty_{n}") for n in Ns]; patched = [S(f"patchA_flat_empty_{n}") for n in Ns]
fig, ax = plt.subplots(figsize=(6.2, 3.6))
ax.plot(Ns, [s["t_expanded_s"] for s in ship], "o-", color=C2, label="Airflow 3.3.2 as shipped (merge() per TI)")
if all(patched):
    ax.plot(Ns, [s["t_expanded_s"] for s in patched], "s-", color=C1, label="Patch A: add() + single flush (backport of #69565)")
ax.axhline(30, color=C3, ls="--", lw=1); ax.text(Ns[0], 33, "default scheduler health threshold (30 s)", color=C3, fontsize=8)
ax.set_xlabel("mapped task instances in one expand()"); ax.set_ylabel("seconds until all TIs exist\n(= scheduler main loop blocked)")
ax.set_xticks(Ns); ax.set_xticklabels(["10k", "30k", "100k"]); ax.legend(frameon=False, fontsize=8); ax.set_title("Dynamic task mapping: expansion is one blocking transaction", fontsize=10)
for xs, ss, c in ((Ns, ship, C2), (Ns, patched, C1)):
    for x, s in zip(xs, ss):
        if s: ax.annotate(f'{s["t_expanded_s"]:.0f}s', (x, s["t_expanded_s"]), textcoords="offset points", xytext=(0, 6), ha="center", fontsize=8, color=c)
fig.tight_layout(); fig.savefig(IMG / "expansion_blocking.png"); plt.close(fig)

# 2. Partition write path: task-success request duration for 500 keys vs rows already in asset_partition_dag_run
# Values transcribed from the API-server access log of run part_100k_e200 (every 8th emitter before the index; every emitter after).
before = [(0, 3.6), (4000, 3.7), (8000, 3.8), (12000, 3.9), (16000, 4.2), (20000, 4.2), (24000, 4.6), (28000, 4.8), (32000, 5.0), (36000, 5.3), (40000, 5.7), (44000, 6.0), (48000, 6.1), (52000, 6.7), (56000, 6.4), (60000, 6.7), (64000, 7.0), (64500, 7.1), (65000, 7.0)]
after = [(65500, 3.9), (66000, 3.5), (66500, 3.4), (67000, 3.5), (67500, 3.7), (68000, 3.4), (68500, 3.4), (69000, 3.5)]
fig, ax = plt.subplots(figsize=(6.2, 3.6))
ax.plot(*zip(*before), "o-", color=C2, ms=3, label="no index (as shipped): sequential scan per key")
ax.plot(*zip(*after), "s-", color=C1, ms=3, label="after CREATE INDEX (target_dag_id, partition_key, id)")
ax.axhline(5, color=C3, ls="--", lw=1); ax.text(500, 5.15, "[workers] execution_api_timeout default 5 s → client retries", color=C3, fontsize=8)
ax.axvline(65000, color=C4, lw=1); ax.text(65500, 6.6, "index created\nonline", color=C4, fontsize=8)
ax.set_xlabel("rows already in asset_partition_dag_run (never pruned)"); ax.set_ylabel("task-success request, 500 keys (s)")
ax.set_ylim(0, 7.8); ax.legend(frameon=False, fontsize=8, loc="upper left"); ax.set_title("Native partitions: per-key write cost grows with table size (O(N²) time)", fontsize=10)
fig.tight_layout(); fig.savefig(IMG / "partition_write_path.png"); plt.close(fig)

# 3. Time to push 100k accounts through one cycle, by shape (scheduler-side, this laptop)
bars = [
    ("A  flat expand, 100k TIs\n(scheduler-only)", S("flat_empty_100000")["t_done_s"], C2),
    ("C  batched 100 × 1000\n(real tasks)", S("batched_100k_100x1000")["t_done_s"], C4),
    ("D  100 child runs × 1000\n(scheduler-only)", 20.4 * 60, C2),
    ("F  native partitions, 100k runs\ncreate only (finish ≈ 3 h)", S("part_100k_e200")["runs_created_s"], C2),
    ("E  run per account\n(10k measured × 10)", S("rpa_10k")["t_done_s"] * 10, C3),
]
fig, ax = plt.subplots(figsize=(6.6, 3.8))
ax.barh([b[0] for b in bars][::-1], [b[1] / 60 for b in bars][::-1], color=[b[2] for b in bars][::-1])
for i, b in enumerate(bars[::-1]):
    ax.text(b[1] / 60 + 0.5, i, f"{b[1]/60:.1f} min", va="center", fontsize=8)
ax.set_xlabel("minutes (one scheduler, 8 GB laptop; grey = linear extrapolation)"); ax.set_title("One cycle of 100k firm accounts, by orchestration shape", fontsize=10)
fig.tight_layout(); fig.savefig(IMG / "shapes_100k.png"); plt.close(fig)

# 4. Row growth per partition table (linear)
tables = ["asset_event", "apdr", "pakl", "dagrun_asset_event", "consumer_dag_run", "consumer_ti"]
pts = {n: S(l)["row_deltas"] for n, l in ((1000, "part_1k_e1"), (10000, "part_10k_e1"))}
pts[100000] = {t: 100000 for t in tables}  # snapshot after the 100k run (all tables exactly 100,000)
fig, ax = plt.subplots(figsize=(6.2, 3.4))
for t, m in zip(tables, "osd^vx"):
    ax.plot(sorted(pts), [pts[n][t] for n in sorted(pts)], marker=m, ms=4, lw=1, label=t)
ax.plot([1e3, 1e5], [1e3, 1e5], color=C3, ls=":", lw=1, label="y = x (linear)"); ax.plot([1e3, 1e5], [1e3, 1e7], color=C2, ls=":", lw=1, label="y ∝ x² (for reference)")
ax.set_xscale("log"); ax.set_yscale("log"); ax.set_xlabel("partition keys emitted"); ax.set_ylabel("rows added"); ax.legend(frameon=False, fontsize=7, ncol=2)
ax.set_title("Partition metadata storage grows linearly (identity mapping)", fontsize=10)
fig.tight_layout(); fig.savefig(IMG / "partition_rows.png"); plt.close(fig)
print("charts written:", sorted(p.name for p in IMG.glob("*.png")))
