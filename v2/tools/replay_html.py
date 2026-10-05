"""Turn a recording (tools/record.py) into a single-page, step-through HTML: one page per moment, the same tables on
every page, rows that changed in that step highlighted. Usage: python tools/replay_html.py recordings/run1.json steps.json out.html
"""
import html, json, re, sys

rec = json.load(open(sys.argv[1])); steps = json.load(open(sys.argv[2])); out = sys.argv[3]
events = [e for e in rec["events"] if e["table"] != "xcom"]


# ---- friendly names: long run_ids -> "run 5", batch ids -> "run 5 #0"
runs = {}
for e in rec["events"]:
    if e["table"] == "dag_run" and e["kind"] == "add":
        pass
# map run_id string -> dag_run id by matching task_instance rows in final state
runid_to_num = {}
# dag_run has no run_id column in our query; derive from batch ids and task_instance run_id by order of appearance
order = []
for e in rec["events"]:
    if e["table"] == "task_instance" and e["kind"] == "add":
        rid = e["after"]["run_id"]
        if rid not in order: order.append(rid)
dag_run_adds = [e["after"]["id"] for e in rec["events"] if e["table"] == "dag_run" and e["kind"] == "add"]
for rid, num in zip(order, dag_run_adds): runid_to_num[rid] = num

def nice(v):
    if v is None: return ""
    s = str(v)
    for rid, num in runid_to_num.items(): s = s.replace(rid, f"run {num}")
    s = re.sub(r'"batch_id": "run (\d+)#(\d+)"', r'"batch_id": "run \1 #\2"', s)
    s = re.sub(r"^run (\d+)#(\d+)$", r"run \1 #\2", s)
    return s

TABLES = [  # (db, label, columns shown, title, one-line meaning)
    ("Airflow DB", "dag_run", ["id", "dag_id", "run_type", "state"], "dag_run", "一个 run 一行。batcher run 就是这里的一行"),
    ("Airflow DB", "task_instance", ["dag_id", "run_id", "task_id", "map_index", "state", "pool"], "task_instance", "run 里面的 task。spark 的 pool 是 v2_spark，K=3"),
    ("Airflow DB", "asset_dag_run_queue", ["asset", "target_dag_id"], "asset_dag_run_queue", "bell 响了但 batcher 还没起 run 时，排队的那一行"),
    ("Airflow DB", "dagrun_asset_event", ["dag_run_id", "event_id"], "dagrun_asset_event", "哪个 run 消费了哪个事件"),
    ("Airflow DB", "asset_event", ["id", "asset", "partition_key", "extra"], "asset_event", "每个事件一行。带 partition_key 的是 lineage，不带的是 bell"),
    ("Airflow DB · 我们的 journal keys", "asset_state_store", ["key", "value", "written_by"], "asset_state_store", "Airflow 自带的 key-value 表。acct/… 每个账户一个 key，batch/… 每个 batch 一个 key。只有 batcher 写"),
    ("journal DB", "accounts", ["account", "status", "seen_version", "claimed_version", "done_version", "batch_id"], "accounts", "每个账户一行。我们自己的表"),
    ("journal DB", "batches", ["batch_id", "accounts", "state"], "batches", "每个 batch 一行"),
]
# only show tables that this recording touched
_touched = {e["table"] for e in events}
TABLES = [t for t in TABLES if t[1] in _touched]

# ---- resolve step boundaries and ids from the recording itself (so a re-recording needs no hand edits)
def first(pred):
    return next(e for e in events if pred(e))
def dr(e, dag, kind, state=None):
    return e["table"] == "dag_run" and e["kind"] == kind and (e["after"] or e["before"])["dag_id"] == dag and (state is None or e["after"]["state"] == state)
prod_ids = [e["after"]["id"] for e in events if dr(e, "v2_land", "add")]
bat_ids = [e["after"]["id"] for e in events if dr(e, "v2_batcher", "add")]
bells = [e["after"]["id"] for e in events if e["table"] == "asset_event" and e["kind"] == "add" and e["after"]["asset"].endswith("landed")]
ids = {"P1": prod_ids[0], "P2": prod_ids[1], "B1": bat_ids[0], "B2": bat_ids[1], "E1": bells[0], "E2": bells[1]}
def t_of(e): return e["t"] + 0.05
def run_success(e, dag, num):
    r = e["after"]
    return e["table"] == "dag_run" and r is not None and r["dag_id"] == dag and r["id"] == num and r["state"] == "success"
def is_batch(e, kind, state=None):
    r = e["after"] or e["before"]
    if e["table"] == "batches":
        return e["kind"] == kind and (state is None or r["state"] == state)
    if e["table"] == "asset_state_store" and r["key"].startswith("batch/"):
        return e["kind"] == kind and (state is None or f'"state": "{state}"' in r["value"])
    return False
batch_adds = [e for e in events if is_batch(e, "add")]
raw_b2 = next(rid for rid, num in runid_to_num.items() if num == ids["B2"])
first_run2_batch = next(e for e in batch_adds if raw_b2 in (e["after"])["key" if e["table"] == "asset_state_store" else "batch_id"])
marks = {
  "P1_DONE": t_of(first(lambda e: run_success(e, "v2_land", ids["P1"]))),
  "B1_START": t_of(first(lambda e: dr(e, "v2_batcher", "add") and e["after"]["id"] == ids["B1"])),
  "B1_CLAIM": t_of(batch_adds[0]),
  "B1_SPARK": t_of(first(lambda e: is_batch(e, "change", "running"))) + 0.5,
  "P2_DONE": max(t_of(first(lambda e: run_success(e, "v2_land", ids["P2"]))), t_of(first(lambda e: e["table"] == "asset_event" and e["kind"] == "add" and e["after"]["id"] == ids["E2"]))),
  "B1_DONE": t_of(first(lambda e: run_success(e, "v2_batcher", ids["B1"]))),
  "B2_CLAIM": t_of(first_run2_batch),
  "END": events[-1]["t"] + 1,
}
for st in steps:
    st["t_end"] = marks[st["t_end"]] if isinstance(st["t_end"], str) else st["t_end"]
    st["title"] = st["title"].format(**ids); st["text"] = st["text"].format(**ids)

# ---- replay: state after each step + the change set of that step
state = {t[1]: {} for t in TABLES}
pages = []
ei = 0
deferred = []
for st in steps:
    changes = {t[1]: {} for t in TABLES}     # key -> kind
    def batcher_side(e):
        r = e["after"] or e["before"]
        return (e["table"] in ("dag_run", "task_instance") and r.get("dag_id") == "v2_batcher") or e["table"] == "dagrun_asset_event" or (e["table"] == "asset_dag_run_queue" and e["kind"] != "add")
    def store_side(e):
        return e["table"] in ("asset_state_store", "accounts", "batches")
    def run2_side(e):
        r = e["after"] or e["before"]
        return (e["table"] == "dag_run" and r.get("id") == ids["B2"]) or (e["table"] == "task_instance" and r.get("run_id") == raw_b2) \
            or (e["table"] == "dagrun_asset_event" and r.get("dag_run_id") == ids["B2"]) or (e["table"] == "asset_dag_run_queue" and e["kind"] == "remove" and e["t"] >= marks["B1_DONE"] - 1)
    groups = {"batcher": batcher_side, "store": store_side, "run2": run2_side}
    defer_names = st.get("defer", ["batcher"] if st.get("defer_batcher") else [])
    batch_now, still = [], []
    for e in deferred:                      # rows deferred from the previous step: keep deferring the ones this step also defers
        (still if any(groups[g](e) for g in defer_names) else batch_now).append(e)
    deferred = still
    while ei < len(events) and events[ei]["t"] <= st["t_end"]:
        e = events[ei]; ei += 1
        if any(groups[g](e) for g in defer_names): deferred.append(e); continue
        batch_now.append(e)
    for e in batch_now:
        tbl = e["table"]
        if tbl not in state: continue
        if e["kind"] == "remove":
            state[tbl].pop(e["key"], None); changes[tbl][e["key"]] = ("remove", e["before"])
        else:
            state[tbl][e["key"]] = e["after"]
            prev_kind = changes[tbl].get(e["key"], (None,))[0]
            changes[tbl][e["key"]] = ("add" if (e["kind"] == "add" or prev_kind == "add") else "change", e["before"] if prev_kind is None else changes[tbl][e["key"]][1])
    # render rows: current rows + removed rows (struck through)
    rendered = {}
    for db, tbl, cols, title, meaning in TABLES:
        rows = []
        for k, r in state[tbl].items():
            kind, before = changes[tbl].get(k, (None, None))
            rows.append({"cells": [nice(r.get(c)) for c in cols], "kind": kind,
                         "changed": [bool(before) and nice(before.get(c)) != nice(r.get(c)) for c in cols] if kind == "change" else [False] * len(cols)})
        for k, (kind, before) in changes[tbl].items():
            if kind == "remove": rows.append({"cells": [nice(before.get(c)) for c in cols], "kind": "remove", "changed": [False] * len(cols)})
        def sortkey(row):
            out = []
            for x in row["cells"]:
                m = re.search(r"\d+", x)
                out.append((re.sub(r"\d+", "", x), int(m.group()) if m else -1, x))
            return out
        rows.sort(key=sortkey)
        rendered[tbl] = rows
    pages.append({"t": st["t_end"], "title": st["title"], "text": st["text"], "tables": rendered})

CSS = """
:root{--bg:#f6f5f1;--panel:#ffffff;--ink:#1c1b19;--quiet:#5f5d57;--faint:#8a877d;--rule:#e3e1da;--rule2:#eeece6;--btn:#cfccc3;
--add:#e8f4ea;--chg:#fff5d6;--rem:#fbe6e6;--remink:#9a4b4b;--hot:#8a3b00;--empty:#a9a69b}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--bg:#1b1a18;--panel:#232220;--ink:#ecebe6;--quiet:#b3b0a6;--faint:#8e8b82;--rule:#3a3934;--rule2:#2e2d2a;--btn:#55534c;
--add:#1f3a27;--chg:#4a3d12;--rem:#4a2323;--remink:#e09a9a;--hot:#ffb067;--empty:#7a776f;color-scheme:dark}}
:root[data-theme="dark"]{--bg:#1b1a18;--panel:#232220;--ink:#ecebe6;--quiet:#b3b0a6;--faint:#8e8b82;--rule:#3a3934;--rule2:#2e2d2a;--btn:#55534c;
--add:#1f3a27;--chg:#4a3d12;--rem:#4a2323;--remink:#e09a9a;--hot:#ffb067;--empty:#7a776f;color-scheme:dark}
*{box-sizing:border-box}body{margin:0;font:15px/1.5 -apple-system,BlinkMacSystemFont,"PingFang SC","Helvetica Neue",Arial,sans-serif;color:var(--ink);background:var(--bg)}
header{background:var(--panel);border-bottom:1px solid var(--rule);padding:14px 16px}header h1{font-size:18px;margin:0 0 4px;text-wrap:balance}header p{margin:0;color:var(--quiet);font-size:13.5px}
.nav{display:flex;gap:6px;padding:10px 16px;flex-wrap:wrap;background:var(--panel);border-bottom:1px solid var(--rule);position:sticky;top:env(safe-area-inset-top,0px);z-index:2}
.nav button{border:1px solid var(--btn);background:var(--panel);color:var(--ink);border-radius:16px;padding:4px 11px;font-size:13px;cursor:pointer}.nav button.on{background:var(--ink);color:var(--bg);border-color:var(--ink)}.nav button:focus-visible{outline:2px solid var(--hot)}
main{padding:18px 16px 60px;max-width:1400px}
.step{background:var(--panel);border:1px solid var(--rule);border-radius:10px;padding:16px 20px;margin-bottom:16px}
.step .t{font-size:12.5px;color:var(--faint)}.step h2{font-size:17px;margin:2px 0 6px;text-wrap:balance}.step p{margin:0;max-width:78ch}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}@media (max-width:900px){.grid{grid-template-columns:1fr}}
.db{border:1px solid var(--rule);border-radius:10px;background:var(--panel);padding:12px 14px;min-width:0}
.db h3{margin:0 0 8px;font-size:13px;letter-spacing:.04em;text-transform:uppercase;color:var(--quiet)}
.wrap{overflow-x:auto;margin-bottom:12px}table{border-collapse:collapse;width:100%;font-size:12.5px;font-variant-numeric:tabular-nums}
caption{text-align:left;font-weight:600;font-size:13px;padding:0 0 3px;color:var(--ink)}caption span{font-weight:400;color:var(--faint);margin-left:8px}
th{text-align:left;color:var(--quiet);font-weight:500;border-bottom:1px solid var(--btn);padding:3px 6px;white-space:nowrap}td{padding:3px 6px;border-bottom:1px solid var(--rule2);vertical-align:top;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;max-width:380px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
tr.add td{background:var(--add)}tr.change td{background:var(--chg)}tr.remove td{background:var(--rem);text-decoration:line-through;color:var(--remink)}
td.hot{font-weight:700;color:var(--hot)}tr.empty td{color:var(--empty);font-style:italic;font-family:inherit}
.k{display:inline-block;padding:0 6px;border-radius:4px}kbd{border:1px solid var(--btn);border-radius:4px;padding:0 5px;font-size:12px}
"""
JS = """
const pages=%s;let i=0;
function esc(s){return String(s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))}
function render(){const p=pages[i];document.querySelectorAll('.nav button').forEach((b,j)=>b.classList.toggle('on',j===i));
document.getElementById('t').textContent='t = '+p.t.toFixed(1)+' s · 第 '+(i+1)+' / '+pages.length+' 步';
document.getElementById('h').textContent=p.title;document.getElementById('x').innerHTML=p.text;
for(const [db,tbl,cols,title,meaning] of TABLES){const rows=p.tables[tbl];let h='<div class="wrap"><table><caption>'+title+'<span>'+meaning+'</span></caption><tr>'+cols.map(c=>'<th>'+c+'</th>').join('')+'</tr>';
if(!rows.length)h+='<tr class="empty"><td colspan="'+cols.length+'">（空）</td></tr>';
for(const r of rows)h+='<tr class="'+(r.kind||'')+'">'+r.cells.map((c,k)=>'<td class="'+(r.changed[k]?'hot':'')+'" title="'+esc(c)+'">'+esc(c)+'</td>').join('')+'</tr>';
h+='</table></div>';document.getElementById('tbl-'+tbl).innerHTML=h;}}
function go(j){i=Math.max(0,Math.min(pages.length-1,j));render();window.scrollTo(0,0)}
document.addEventListener('keydown',e=>{if(e.key==='ArrowRight'||e.key===' ')go(i+1);if(e.key==='ArrowLeft')go(i-1)});
const TABLES=%s;render();
"""
nav = "".join(f'<button onclick="go({k})">{k+1}</button>' for k in range(len(pages)))
tables_html = ""
for db in dict.fromkeys(t[0] for t in TABLES):
    tables_html += f'<div class="db"><h3>{db}</h3>' + "".join(f'<div id="tbl-{t[1]}"></div>' for t in TABLES if t[0] == db) + "</div>"
page = f"""<title>Batcher Run Replay</title><style>{CSS}</style>
<header><h1>一个 batcher run，从 bell 响到 done：真实记录，每半秒一次快照</h1>
<p>同样的 7 张表每一步都在。<span class="k" style="background:var(--add)">绿</span> 这一步新增的行 &nbsp;<span class="k" style="background:var(--chg)">黄</span> 这一步改过的行，改动的格子加粗 &nbsp;<span class="k" style="background:var(--rem)">红</span> 这一步删掉的行。键盘 <kbd>←</kbd> <kbd>→</kbd> 翻页，手机上点上面的数字。</p></header>
<div class="nav">{nav}</div>
<main><div class="step"><div class="t" id="t"></div><h2 id="h"></h2><p id="x"></p></div>
<div class="grid">{tables_html}</div></main>
<script>{JS % (json.dumps(pages, ensure_ascii=False), json.dumps(TABLES, ensure_ascii=False))}</script>
"""
open(out, "w", encoding="utf-8").write(page)
print("wrote", out, len(page))
