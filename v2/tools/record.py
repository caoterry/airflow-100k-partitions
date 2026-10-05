"""Record every row change in the Airflow DB and the journal DB while a scenario runs.

  python tools/record.py out.json   -- polls every 0.5 s until the last v2_batcher run is finished and quiet
Output: {"t0": iso, "events": [{"t": seconds, "table": ..., "kind": "add|change|remove", "key": ..., "before": {...}, "after": {...}}]}
"""
import json, os, sys, time
import psycopg2
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from snap import QUERIES, AIRFLOW_DSN, JOURNAL_DSN

def read_all(conns):
    out = {}
    for db, label, keys, sql in QUERIES:
        with conns[db].cursor() as cur:
            cur.execute(sql); cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, [str(v) if v is not None else None for v in r])) for r in cur.fetchall()]
        out[label] = {"|".join(str(r[k]) for k in keys): r for r in rows}
    return out

def main(out_path, quiet_s=6.0, max_s=600):
    conns = {"airflow": psycopg2.connect(AIRFLOW_DSN), "journal": psycopg2.connect(JOURNAL_DSN)}
    for c in conns.values(): c.autocommit = True
    prev = read_all(conns); t0 = time.time(); events = []; last_change = t0; seen_batcher = False
    print("recording...", flush=True)
    while True:
        time.sleep(0.5); cur = read_all(conns); t = round(time.time() - t0, 1)
        for label in cur:
            a, b = prev[label], cur[label]
            for k in b:
                if k not in a: events.append({"t": t, "table": label, "kind": "add", "key": k, "before": None, "after": b[k]}); last_change = time.time()
                elif a[k] != b[k]: events.append({"t": t, "table": label, "kind": "change", "key": k, "before": a[k], "after": b[k]}); last_change = time.time()
            for k in a:
                if k not in b: events.append({"t": t, "table": label, "kind": "remove", "key": k, "before": a[k], "after": None}); last_change = time.time()
        runs = [r for r in cur["dag_run"].values() if r["dag_id"] == "v2_batcher"]
        if any(r["state"] == "running" for r in runs): seen_batcher = True
        done = seen_batcher and all(r["state"] in ("success", "failed") for r in runs)
        prev = cur
        if (done and time.time() - last_change > quiet_s) or time.time() - t0 > max_s: break
    json.dump({"t0": t0, "events": events}, open(out_path, "w"))
    print(f"recorded {len(events)} row changes over {t:.0f} s -> {out_path}")

if __name__ == "__main__":
    main(sys.argv[1])
