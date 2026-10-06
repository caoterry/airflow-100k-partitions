"""Show how the databases change: snapshot the interesting tables, act, snapshot again, print only what changed.

  python tools/snap.py take before
  ... do something ...
  python tools/snap.py take after
  python tools/snap.py diff before after
"""
import json, os, sys
import psycopg2

AIRFLOW_DSN = os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"].replace("postgresql+psycopg2", "postgresql")
JOURNAL_DSN = os.environ["V2_JOURNAL_DSN"]
SNAPDIR = os.path.join(os.environ["AIRFLOW_HOME"], "snaps")

# (database, table label, key columns, SQL)
QUERIES = [
    ("airflow", "asset_event", ["id"],
     "SELECT e.id, a.name AS asset, e.source_task_id, left(e.extra::text, 240) AS extra, to_char(e.timestamp, 'HH24:MI:SS') AS timestamp "
     "FROM asset_event e JOIN asset a ON a.id = e.asset_id"),
    ("airflow", "asset_dag_run_queue", ["asset", "target_dag_id"],
     "SELECT a.name AS asset, q.target_dag_id, to_char(q.created_at, 'HH24:MI:SS') AS created_at FROM asset_dag_run_queue q JOIN asset a ON a.id = q.asset_id"),
    ("airflow", "dag_run", ["id"],
     "SELECT id, dag_id, run_type, state, run_id FROM dag_run"),
    ("airflow", "dagrun_asset_event", ["dag_run_id", "event_id"],
     "SELECT dag_run_id, event_id FROM dagrun_asset_event"),
    ("airflow", "task_instance", ["dag_id", "run_id", "task_id", "map_index"],
     "SELECT dag_id, run_id, task_id, map_index, state, try_number, pool FROM task_instance"),
    ("airflow", "trigger", ["id"],
     "SELECT id, split_part(classpath, '.', -1) AS classpath, to_char(created_date, 'HH24:MI:SS') AS created_date FROM trigger"),
    ("airflow", "xcom", ["dag_id", "run_id", "task_id", "map_index", "key"],
     "SELECT dag_id, run_id, task_id, map_index, key, left(value::text, 160) AS value FROM xcom"),
    ("airflow", "asset_state_store", ["asset", "key"],
     "SELECT a.name AS asset, s.key, left(s.value::text, 300) AS value, s.last_updated_by_task_id AS written_by FROM asset_state_store s JOIN asset a ON a.id = s.asset_id"),
    ("airflow", "variable", ["key"],
     "SELECT key, left(val, 80) AS val FROM variable"),
    ("journal", "accounts", ["account"],
     "SELECT account, status, seen_version, claimed_version, done_version, failed_version, batch_id, error FROM accounts"),
    ("journal", "batches", ["batch_id"],
     "SELECT batch_id, accounts, state, error FROM batches"),
]

def take(name: str) -> None:
    os.makedirs(SNAPDIR, exist_ok=True)
    out = {}
    conns = {"airflow": psycopg2.connect(AIRFLOW_DSN), "journal": psycopg2.connect(JOURNAL_DSN)}
    for db, label, keys, sql in QUERIES:
        with conns[db].cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, [str(v) if v is not None else None for v in r])) for r in cur.fetchall()]
        out[label] = {"keys": keys, "rows": {"|".join(str(r[k]) for k in keys): r for r in rows}}
    for c in conns.values(): c.close()
    json.dump(out, open(os.path.join(SNAPDIR, f"{name}.json"), "w"))
    print(f"snapshot '{name}': " + ", ".join(f"{t} {len(v['rows'])}" for t, v in out.items()))

def short(row: dict, keys: list) -> str:
    return "  ".join(f"{k}={v}" for k, v in row.items() if v is not None)

def diff(a: str, b: str, limit: int = 12) -> None:
    A = json.load(open(os.path.join(SNAPDIR, f"{a}.json"))); B = json.load(open(os.path.join(SNAPDIR, f"{b}.json")))
    any_change = False
    for label in B:
        ra, rb = A[label]["rows"], B[label]["rows"]
        added = [rb[k] for k in rb if k not in ra]
        removed = [ra[k] for k in ra if k not in rb]
        changed = [(ra[k], rb[k]) for k in rb if k in ra and ra[k] != rb[k]]
        if not (added or removed or changed): continue
        any_change = True
        db = "Airflow DB" if label not in ("accounts", "batches") else "journal DB"
        print(f"\n== {label}  ({db})  +{len(added)} added  ~{len(changed)} changed  -{len(removed)} removed")
        for r in added[:limit]: print("  + " + short(r, B[label]["keys"]))
        for o, n in changed[:limit]:
            key = "|".join(str(n[k]) for k in B[label]["keys"])
            delta = ", ".join(f"{c}: {o[c]} -> {n[c]}" for c in n if o.get(c) != n[c])
            print(f"  ~ {key}: {delta}")
        for r in removed[:limit]: print("  - " + short(r, A[label]["keys"]))
        hidden = max(0, len(added) - limit) + max(0, len(changed) - limit) + max(0, len(removed) - limit)
        if hidden: print(f"  ... {hidden} more")
    if not any_change: print("no changes")

if __name__ == "__main__":
    cmd = sys.argv[1]
    take(sys.argv[2]) if cmd == "take" else diff(sys.argv[2], sys.argv[3])
