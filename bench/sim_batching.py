#!/usr/bin/env python
"""Discrete-event simulation of batching policies for account-grain PnL calculations.

Model
  - Accounts' input events arrive over a business day: a start-of-day burst plus a trickle (configurable).
  - A calculation job (Glue/Spark) costs  startup_s + per_account_s * batch_size  seconds of wall time.
  - Capacity: at most K jobs run concurrently (Glue concurrency / cost cap). Jobs wait for a free slot.
  - A scheduler tick every tick_s seconds decides what to dispatch (Airflow batcher cadence). Event-triggered variants
    use tick_s small.
Policies
  per_account      one job per account, FIFO through K slots
  fixed_pack       accumulate until pack_size accounts, or oldest waits > t_max_s, then one job per full pack
  fixed_cadence    every cadence_s dispatch everything ready as ONE job (classic micro-batch)
  adaptive         at each tick: free = K - running; if free == 0: wait (accumulate). else split ready into
                   n = min(free, ceil(ready / b_target)) jobs where b_target balances startup overhead vs waiting
                   (b_target = clamp(sqrt(2 * lambda_est * startup_s * per_account_s) / per_account_s, b_min, b_max));
                   always dispatch if the oldest ready account has waited > t_max_s.
Metrics: latency (arrival -> job end) p50/p95/max per phase (burst / trickle), number of jobs, slot-utilisation.
"""
from __future__ import annotations
import argparse, heapq, json, math, random, statistics
from collections import deque
from dataclasses import dataclass, field


@dataclass
class Job:
    start: float; end: float; accounts: list


def arrivals(n_burst: int, burst_span_s: float, trickle_rate_per_s: float, trickle_span_s: float, seed: int = 7):
    rnd = random.Random(seed)
    ev = [rnd.uniform(0, burst_span_s) for _ in range(n_burst)]
    t = burst_span_s
    while t < burst_span_s + trickle_span_s:
        t += rnd.expovariate(trickle_rate_per_s) if trickle_rate_per_s > 0 else float("inf")
        if t < burst_span_s + trickle_span_s: ev.append(t)
    ev.sort()
    return ev


def simulate(policy: str, arr: list[float], K: int, startup_s: float, per_account_s: float, tick_s: float,
             pack_size: int = 500, cadence_s: float = 300, t_max_s: float = 120, b_min: int = 5, b_max: int = 2000,
             target_s: float = 300):
    ready: deque[float] = deque()          # arrival times of accounts waiting to be dispatched
    running: list[float] = []              # heap of job end times
    jobs: list[Job] = []
    latencies: list[tuple[float, float]] = []   # (arrival, latency)
    i = 0; t = 0.0; last_cadence = 0.0; busy_time = 0.0
    horizon = arr[-1] + 3 * 3600
    rate_window: deque[float] = deque()

    def dispatch(now, accounts):
        nonlocal busy_time
        # wait for a slot if needed
        while len(running) >= K:
            end = heapq.heappop(running); now = max(now, end)
        dur = startup_s + per_account_s * len(accounts)
        heapq.heappush(running, now + dur); busy_time += dur
        jobs.append(Job(now, now + dur, accounts))
        for a in accounts: latencies.append((a, now + dur - a))

    while t < horizon:
        # ingest arrivals up to t
        while i < len(arr) and arr[i] <= t:
            ready.append(arr[i]); rate_window.append(arr[i]); i += 1
        while rate_window and rate_window[0] < t - 600: rate_window.popleft()
        while running and running[0] <= t: heapq.heappop(running)
        free = K - len(running)
        if ready:
            oldest_wait = t - ready[0]
            if policy == "per_account":
                while ready and len(running) < K:
                    dispatch(t, [ready.popleft()])
            elif policy == "fixed_pack":
                while ready and (len(ready) >= pack_size or oldest_wait > t_max_s) and len(running) < K:
                    n = min(pack_size, len(ready)); dispatch(t, [ready.popleft() for _ in range(n)])
                    oldest_wait = (t - ready[0]) if ready else 0
            elif policy == "fixed_cadence":
                if t - last_cadence >= cadence_s and len(running) < K:
                    dispatch(t, [ready.popleft() for _ in range(len(ready))]); last_cadence = t
            elif policy in ("adaptive", "adaptive_sla"):
                if free > 0:
                    lam = max(len(rate_window) / 600.0, 1e-3)           # arrivals/s over last 10 min
                    # (1) latency-optimal size: balance waiting-to-fill against startup overhead
                    b_lat = math.sqrt(2 * lam * startup_s / per_account_s) if per_account_s > 0 else b_max
                    # (2) capacity floor: smallest batch at which K slots sustain the arrival rate,
                    #     K * b / (S + p*b) >= lam  =>  b >= lam*S / (K - lam*p); infeasible => b_max
                    denom = K - lam * per_account_s
                    b_cap = (lam * startup_s / denom) * 1.25 if denom > 0 else b_max   # 25% headroom
                    if policy == "adaptive_sla":
                        # largest batch whose expected latency (fill time + startup + processing) meets the target
                        b_sla = (target_s - startup_s) / (1.0 / lam + per_account_s) if target_s > startup_s else b_min
                        b_target = int(max(b_min, min(b_max, max(b_cap, b_sla))))
                    else:
                        b_target = int(max(b_min, min(b_max, max(b_lat, b_cap))))
                    n_jobs = min(free, max(1, math.ceil(len(ready) / b_target)))
                    if len(ready) >= b_min or oldest_wait > t_max_s:
                        per = min(b_max, math.ceil(len(ready) / n_jobs))
                        for _ in range(n_jobs):
                            if not ready: break
                            dispatch(t, [ready.popleft() for _ in range(min(per, len(ready)))])
        if i >= len(arr) and not ready and not running: break
        t += tick_s
    backlog = len(ready) + (len(arr) - i)
    return jobs, latencies, busy_time, backlog


def pct(xs, p):
    if not xs: return float("nan")
    xs = sorted(xs); k = max(0, min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))); return xs[k]


def summarize(name, jobs, lat, busy, burst_span, K, backlog=0):
    b = [l for a, l in lat if a <= burst_span]; tr = [l for a, l in lat if a > burst_span]
    span = max(j.end for j in jobs) - min(j.start for j in jobs) if jobs else 0
    return {"policy": name + (f" (UNSTABLE, {backlog} never done)" if backlog else ""), "jobs": len(jobs), "avg_batch": round(sum(len(j.accounts) for j in jobs) / max(len(jobs), 1), 1),
            "burst_p50_min": round(pct(b, 50) / 60, 1), "burst_p95_min": round(pct(b, 95) / 60, 1), "burst_max_min": round(max(b) / 60, 1) if b else None,
            "trickle_p50_min": round(pct(tr, 50) / 60, 1), "trickle_p95_min": round(pct(tr, 95) / 60, 1),
            "job_hours": round(busy / 3600, 1), "util_pct": round(100 * busy / (K * span), 1) if span else None}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-burst", type=int, default=10000); ap.add_argument("--burst-span", type=float, default=600)
    ap.add_argument("--trickle-rate", type=float, default=1.0, help="accounts per second after the burst")
    ap.add_argument("--trickle-span", type=float, default=4 * 3600)
    ap.add_argument("--K", type=int, default=20); ap.add_argument("--startup", type=float, default=60)
    ap.add_argument("--per-account", type=float, default=1.0); ap.add_argument("--tick", type=float, default=30)
    ap.add_argument("--pack", type=int, default=500); ap.add_argument("--cadence", type=float, default=300)
    ap.add_argument("--t-max", type=float, default=120); ap.add_argument("--json", action="store_true")
    ap.add_argument("--target", type=float, default=300, help="latency target (s) for the adaptive_sla policy")
    ap.add_argument("--policies", default="per_account,fixed_pack,fixed_cadence,adaptive,adaptive_sla")
    a = ap.parse_args()
    arr = arrivals(a.n_burst, a.burst_span, a.trickle_rate, a.trickle_span)
    rows = []
    for pol in a.policies.split(","):
        jobs, lat, busy, backlog = simulate(pol, arr, a.K, a.startup, a.per_account, a.tick, a.pack, a.cadence, a.t_max, target_s=a.target)
        rows.append(summarize(pol, jobs, lat, busy, a.burst_span, a.K, backlog))
    if a.json:
        print(json.dumps({"params": vars(a), "results": rows}, indent=1))
    else:
        print(f"arrivals: {len(arr)} accounts ({a.n_burst} in a {a.burst_span/60:.0f}-min burst, then {a.trickle_rate}/s for {a.trickle_span/3600:.0f} h); "
              f"K={a.K} concurrent jobs, startup {a.startup}s, {a.per_account}s/account, tick {a.tick}s, pack {a.pack}, cadence {a.cadence}s, t_max {a.t_max}s, SLA target {a.target}s")
        cols = list(rows[0].keys()); print("| " + " | ".join(cols) + " |"); print("|" + "---|" * len(cols))
        for r in rows: print("| " + " | ".join(str(r[c]) for c in cols) + " |")
