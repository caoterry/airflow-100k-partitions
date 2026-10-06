"""Print a recording (tools/record.py) as a readable timeline: one line per interesting row change, with t in seconds.
  python tools/kata_timeline.py recordings/kata_bell.json [--all]
"""
import json, sys

def main(path, show_all=False):
    rec = json.load(open(path)); ev = rec["events"]
    print(f"# {path}: {len(ev)} row changes over {ev[-1]['t']} s")
    for e in ev:
        r = e["after"] or e["before"]; t = f"{e['t']:6.1f}"; k = e["kind"]
        tbl = e["table"]
        if tbl == "dag_run":
            print(f"{t} dag_run {k:6s} id={r['id']} {r['dag_id']} {r['run_type']} -> {r['state']}  run_id={r['run_id'][-12:]}")
        elif tbl == "asset_event" and k == "add":
            print(f"{t} asset_event add id={r['id']} {r['asset']} by {r['source_task_id']} extra={r['extra'][:160]}")
        elif tbl == "asset_dag_run_queue":
            print(f"{t} asset_dag_run_queue {k:6s} {r['asset']} -> {r['target_dag_id']} created_at={r['created_at']}")
        elif tbl == "dagrun_asset_event" and k == "add":
            print(f"{t} dagrun_asset_event add run {r['dag_run_id']} <- event {r['event_id']}")
        elif tbl == "trigger":
            print(f"{t} trigger {k:6s} id={r['id']} {r['classpath']}")
        elif tbl == "task_instance":
            if show_all or (k == "change" and r["state"] in ("running", "deferred", "success", "skipped", "failed", "up_for_retry", "upstream_failed")) or k == "add":
                print(f"{t} task_instance {k:6s} {r['dag_id']}.{r['task_id']}[{r['map_index']}] try={r['try_number']} -> {r['state']}  run_id={r['run_id'][-12:]} pool={r['pool']}")
        elif tbl == "asset_state_store":
            print(f"{t} asset_state_store {k:6s} {r['asset']} {r['key']} = {r['value'][:140]}  by {r['written_by']}")
        elif tbl == "xcom":
            print(f"{t} xcom {k:6s} {r['dag_id']}.{r['task_id']}[{r['map_index']}] {r['key']} = {r['value'][:120]}")
        elif tbl == "variable":
            print(f"{t} variable {k:6s} {r['key']} = {r['val'][:40]}")

if __name__ == "__main__":
    main(sys.argv[1], "--all" in sys.argv)
