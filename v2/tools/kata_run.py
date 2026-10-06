"""Drive one kata chapter against the running v2 environment while tools/record.py records every row change.

  python tools/kata_run.py <chapter> <out.json>        chapters: bell | versions | lanes | failure

Each chapter is a scripted sequence of producer landings (airflow dags trigger v2_land) with pauses, so that the recording shows
one mechanism clearly. The recorder stops when the batcher is quiet and this driver has written its stop file.
Kata constants: DEBOUNCE_S in dags/v2_dags.py is 10 for these recordings, K = 3, engine 6 s + 0.5 s/partition. Partition names: KATA* and LANE*.
"""
import json, os, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))

def trigger(partitions, version, markers=None, dataset="positions"):
    conf = {"dataset": dataset, "partitions": partitions, "version": version}
    if markers: conf["markers"] = markers
    r = subprocess.run(["airflow", "dags", "trigger", "v2_land", "-c", json.dumps(conf)], capture_output=True, text=True)
    print(f"  {time.strftime('%H:%M:%S')} landed {dataset} v{version}: {len(partitions)} partition(s)" + (f" markers={markers}" if markers else ""), flush=True)
    if r.returncode: print(r.stderr[-300:], flush=True)

def variable(key, value):
    subprocess.run(["airflow", "variables", "set", key, value], capture_output=True, text=True)
    print(f"  {time.strftime('%H:%M:%S')} Variable {key} = {value!r}", flush=True)

def names(prefix, a, b): return [f"{prefix}{n:03d}" for n in range(a, b + 1)]

CHAPTERS = {}
def chapter(name):
    def deco(f): CHAPTERS[name] = f; return f
    return deco

@chapter("bell")
def ch_bell():
    """One bell's life: a landing, a second landing during the debounce hold, one batcher run, the ledger, the downstream example."""
    trigger(names("KATA", 1, 4), 1)
    time.sleep(4)
    trigger(names("KATA", 5, 6), 1)          # rings during the hold: same window, same job

@chapter("versions")
def ch_versions():
    """When does a partition recompute: ANY input dataset with a newer version AND a changed marker. Two datasets, overlapping
    partitions; then a bell that changes nothing."""
    trigger(["KATA001", "KATA002"], 2, markers={"KATA001": "x9", "KATA002": "1"})   # positions v2: KATA001 due, KATA002 same marker -> not due
    time.sleep(2)
    trigger(["KATA002", "KATA003"], 1, dataset="trades")                           # a second dataset: both due (never seen trades)
    time.sleep(45)                                                                 # window closes, run finishes
    trigger(["KATA004"], 1)                   # positions v1 again: a window that claims nothing, the chain ends

@chapter("lanes")
def ch_lanes():
    """K = 3 lanes: three windows become three overlapping runs; a fourth bell waits in asset_dag_run_queue until a lane frees."""
    trigger(names("LANE", 1, 80), 1)         # spark 6 + 40 s
    time.sleep(12)
    trigger(names("LANE", 81, 110), 1)       # spark 6 + 15 s
    time.sleep(12)
    trigger(names("LANE", 111, 140), 1)
    time.sleep(12)
    trigger(names("LANE", 141, 145), 1)      # all three lanes busy: this debounced bell waits in the queue

@chapter("failure")
def ch_failure():
    """Blast radius a+c: the engine fails on one partition; Airflow retries; only that batch fails; healthy partitions ring again;
    the culprit waits in failed; a newer version lands and it is retried automatically in its own quarantine job."""
    variable("v2_poison", "KATA003")
    trigger(names("KATA", 1, 6), 3)
    time.sleep(75)                            # hold 10 + spark 9 + retry 5 + spark 9 + bookkeeping + retry run of the healthy five
    variable("v2_poison", "")
    trigger(["KATA003"], 4)                   # the new version is retried on its own

if __name__ == "__main__":
    name, out = sys.argv[1], sys.argv[2]
    stop = out + ".stop"
    if os.path.exists(stop): os.remove(stop)
    rec = subprocess.Popen([sys.executable, os.path.join(HERE, "record.py"), out, "6", stop])
    time.sleep(2.5)                           # first snapshot taken
    print(f"chapter {name}: {CHAPTERS[name].__doc__.strip()}", flush=True)
    CHAPTERS[name]()
    open(stop, "w").write("done")
    rec.wait(); os.remove(stop)
    print("recording complete", flush=True)
