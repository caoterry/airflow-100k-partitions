"""Build the kata page from chapter recordings (tools/record.py): one page per moment, the same tables on every page, rows that
changed in that step highlighted, and a question per step with a hidden answer.

  python tools/kata_html.py recordings/kata_steps_zh.py out.html

The spec module defines TITLE, LEAD, TABLES and CHAPTERS; each chapter names a recording and a list of steps; each step says
`until` (a moment: a callable from this module applied to the chapter's events) and carries title/text/q/a strings. Strings may
use {placeholders} resolved by the chapter's `ids` (callables returning a value from the events). Nothing is hand-timed: a
re-recording rebuilds the page.
"""
from __future__ import annotations
import html, importlib.util, json, re, sys


# ---------------------------------------------------------------- moments: callables(events) -> t
def _row(e): return e["after"] if e["after"] is not None else e["before"]

def ev_match(e, table, kind=None, **fields):
    if e["table"] != table or (kind and e["kind"] != kind): return False
    row = _row(e)
    for k, v in fields.items():
        val = row.get(k)
        if callable(v):
            if not v(val): return False
        elif str(val) != str(v): return False
    return True

def first(table, kind=None, nth=1, **fields):
    """The nth event matching (1-based)."""
    def f(events):
        n = 0
        for e in events:
            if ev_match(e, table, kind, **fields):
                n += 1
                if n == nth: return e["t"] + 0.05
        raise LookupError(f"no event: {table} {kind} {fields} #{nth}")
    return f

def value(table, field, kind="add", nth=1, **fields):
    """The nth matching event's field value (for {placeholders})."""
    def f(events):
        n = 0
        for e in events:
            if ev_match(e, table, kind, **fields):
                n += 1
                if n == nth: return _row(e).get(field)
        raise LookupError(f"no value: {table} {field} {fields} #{nth}")
    return f

def latest(*moments): return lambda ev: max(m(ev) for m in moments)
def plus(moment, dt): return lambda ev: moment(ev) + dt
def end(): return lambda ev: ev[-1]["t"] + 1
def contains(sub): return lambda v: v is not None and sub in str(v)
def startswith(pre): return lambda v: v is not None and str(v).startswith(pre)


# ---------------------------------------------------------------- replay one chapter
def build_chapter(ch, tables, classify=None):
    rec = json.load(open(ch["recording"]))
    events = [e for e in rec["events"] if e["table"] in {t[1] for t in tables}]
    ids = {k: f(events) for k, f in ch.get("ids", {}).items()}
    # friendly names: dag_run.run_id -> "run <id>"; "<run_id>#i" batch ids -> "run <id> #i"
    runid_to_num, run_dag = {}, {}
    for e in events:
        if e["table"] == "dag_run" and e["kind"] == "add":
            runid_to_num[e["after"]["run_id"]] = e["after"]["id"]; run_dag[str(e["after"]["id"])] = e["after"]["dag_id"]
    ctx = {"run_dag": run_dag}
    def nice(v):
        if v is None: return ""
        s = str(v)
        for rid, num in sorted(runid_to_num.items(), key=lambda kv: -len(kv[0])): s = s.replace(rid, f"run {num}")
        s = re.sub(r"run (\d+)#(\d+)", r"run \1 #\2", s)
        return s
    touched = {e["table"] for e in events}
    tabs = [t for t in tables if t[1] in touched]
    state = {t[1]: {} for t in tabs}
    pages, ei = [], 0
    for st in ch["steps"]:
        t_end = st["until"](events)
        changes = {t[1]: {} for t in tabs}            # key -> (kind, before)
        while ei < len(events) and events[ei]["t"] <= t_end:
            e = events[ei]; ei += 1; tbl = e["table"]
            if e["kind"] == "remove":
                state[tbl].pop(e["key"], None); changes[tbl][e["key"]] = ("remove", e["before"])
            else:
                state[tbl][e["key"]] = e["after"]
                prev = changes[tbl].get(e["key"])
                if prev is None: changes[tbl][e["key"]] = (e["kind"], e["before"])
                elif prev[0] == "add": pass                                 # added then changed in one step: still "add"
                else: changes[tbl][e["key"]] = ("change", prev[1])
        rendered = {}
        for db, tbl, cols, title, meaning in tabs:
            rows = []
            def grp(raw):
                order, label = classify(tbl, raw, ctx) if classify else (0, "")
                return order, label
            for k, r in state[tbl].items():
                kind, before = changes[tbl].get(k, (None, None))
                hot = [bool(before) and nice(before.get(c)) != nice(r.get(c)) for c in cols] if kind == "change" else [False] * len(cols)
                order, label = grp(r)
                rows.append({"cells": [nice(r.get(c)) for c in cols], "kind": kind, "changed": hot, "group": label, "_o": order})
            for k, (kind, before) in changes[tbl].items():
                if kind == "remove":
                    order, label = grp(before)
                    rows.append({"cells": [nice(before.get(c)) for c in cols], "kind": "remove", "changed": [False] * len(cols), "group": label, "_o": order})
            def sortkey(row):
                return [row["_o"]] + [(re.sub(r"\d+", "", x), int(m.group()) if (m := re.search(r"\d+", x)) else -1, x) for x in row["cells"]]
            rows.sort(key=sortkey)
            for row in rows: row.pop("_o", None)
            rendered[tbl] = rows
        fmt = lambda s_: s_.format(**ids) if ids else s_
        pages.append({"t": round(t_end, 1), "title": fmt(st["title"]), "text": fmt(st["text"]), "q": fmt(st.get("q", "")),
                      "a": fmt(st.get("a", "")), "tables": rendered})
    return {"id": ch["id"], "title": ch["title"], "intro": ch.get("intro", ""), "tables": [list(t) for t in tabs], "pages": pages}


CSS = """
:root{--bg:#f6f5f1;--panel:#ffffff;--ink:#1c1b19;--quiet:#5f5d57;--faint:#8a877d;--rule:#e3e1da;--rule2:#eeece6;--btn:#cfccc3;
--add:#e8f4ea;--chg:#fff5d6;--rem:#fbe6e6;--remink:#9a4b4b;--hot:#8a3b00;--empty:#a9a69b;--q:#eef3f8;--qink:#1f4d7a}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--bg:#1b1a18;--panel:#232220;--ink:#ecebe6;--quiet:#b3b0a6;--faint:#8e8b82;--rule:#3a3934;--rule2:#2e2d2a;--btn:#55534c;
--add:#1f3a27;--chg:#4a3d12;--rem:#4a2323;--remink:#e09a9a;--hot:#ffb067;--empty:#7a776f;--q:#1e2a36;--qink:#9cc4ea;color-scheme:dark}}
:root[data-theme="dark"]{--bg:#1b1a18;--panel:#232220;--ink:#ecebe6;--quiet:#b3b0a6;--faint:#8e8b82;--rule:#3a3934;--rule2:#2e2d2a;--btn:#55534c;
--add:#1f3a27;--chg:#4a3d12;--rem:#4a2323;--remink:#e09a9a;--hot:#ffb067;--empty:#7a776f;--q:#1e2a36;--qink:#9cc4ea;color-scheme:dark}
*{box-sizing:border-box}body{margin:0;font:15px/1.5 -apple-system,BlinkMacSystemFont,"PingFang SC","Helvetica Neue",Arial,sans-serif;color:var(--ink);background:var(--bg)}
header{background:var(--panel);border-bottom:1px solid var(--rule);padding:14px 16px}header h1{font-size:18px;margin:0 0 4px;text-wrap:balance}header p{margin:0;color:var(--quiet);font-size:13.5px;max-width:90ch}
.chapters{display:flex;gap:6px;padding:10px 16px 0;flex-wrap:wrap;background:var(--panel)}
.chapters button{border:1px solid var(--btn);background:var(--panel);color:var(--ink);border-radius:8px;padding:5px 12px;font-size:13.5px;cursor:pointer}
.chapters button.on{background:var(--ink);color:var(--bg);border-color:var(--ink)}
.nav{display:flex;gap:6px;padding:10px 16px;flex-wrap:wrap;background:var(--panel);border-bottom:1px solid var(--rule);position:sticky;top:env(safe-area-inset-top,0px);z-index:2}
.nav button{border:1px solid var(--btn);background:var(--panel);color:var(--ink);border-radius:16px;padding:4px 11px;font-size:13px;cursor:pointer}.nav button.on{background:var(--ink);color:var(--bg);border-color:var(--ink)}
button:focus-visible{outline:2px solid var(--hot)}
main{padding:18px 16px 60px;max-width:1400px}
.intro{color:var(--quiet);font-size:14px;margin:0 0 12px;max-width:90ch}
.step{background:var(--panel);border:1px solid var(--rule);border-radius:10px;padding:16px 20px;margin-bottom:16px}
.step .t{font-size:12.5px;color:var(--faint)}.step h2{font-size:17px;margin:2px 0 6px;text-wrap:balance}.step p{margin:0 0 8px;max-width:80ch}
.q{background:var(--q);border-radius:8px;padding:10px 14px;margin-top:10px;max-width:80ch}.q b{color:var(--qink)}
.q button{margin-top:6px;border:1px solid var(--btn);background:var(--panel);color:var(--ink);border-radius:6px;padding:3px 10px;font-size:13px;cursor:pointer}
.q .a{margin-top:8px;border-top:1px dashed var(--btn);padding-top:8px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}@media (max-width:900px){.grid{grid-template-columns:1fr}}
.db{border:1px solid var(--rule);border-radius:10px;background:var(--panel);padding:12px 14px;min-width:0}
.db h3{margin:0 0 8px;font-size:13px;letter-spacing:.04em;text-transform:uppercase;color:var(--quiet)}
.wrap{overflow-x:auto;margin-bottom:12px}table{border-collapse:collapse;width:100%;font-size:12.5px;font-variant-numeric:tabular-nums}
caption{text-align:left;font-weight:600;font-size:13px;padding:0 0 3px;color:var(--ink)}caption span{font-weight:400;color:var(--faint);margin-left:8px}
th{text-align:left;color:var(--quiet);font-weight:500;border-bottom:1px solid var(--btn);padding:3px 6px;white-space:nowrap}
td{padding:3px 6px;border-bottom:1px solid var(--rule2);vertical-align:top;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;max-width:420px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
tr.grp td{background:transparent;color:var(--qink);font-family:inherit;font-size:11.5px;letter-spacing:.03em;padding:7px 6px 2px;border-bottom:1px solid var(--rule);font-weight:600}
tr.add td{background:var(--add)}tr.change td{background:var(--chg)}tr.remove td{background:var(--rem);text-decoration:line-through;color:var(--remink)}
td.hot{font-weight:700;color:var(--hot)}td.tipped{text-decoration:underline dotted var(--btn);text-underline-offset:3px;cursor:help}tr.empty td{color:var(--empty);font-style:italic;font-family:inherit}
.k{display:inline-block;padding:0 6px;border-radius:4px}kbd{border:1px solid var(--btn);border-radius:4px;padding:0 5px;font-size:12px}code{font-size:.92em}
details.legend{background:var(--panel);border:1px solid var(--rule);border-radius:10px;margin:0 16px 16px;padding:0}
details.legend summary{cursor:pointer;padding:10px 16px;font-weight:600;font-size:14px;list-style:none}details.legend summary::before{content:"▸ ";color:var(--faint)}details.legend[open] summary::before{content:"▾ "}
.lg{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,520px),1fr));gap:16px 28px;padding:0 16px 16px}
.lg h4{margin:0 0 6px;font-size:12.5px;letter-spacing:.04em;text-transform:uppercase;color:var(--quiet)}
.lg table{font-size:12.5px}.lg td{white-space:normal;font-family:inherit;font-size:12.5px;max-width:none;line-height:1.4}.lg td:first-child{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;white-space:nowrap;color:var(--qink);vertical-align:top;width:1%;padding-right:12px}.lg td{padding:5px 6px}
"""

JS = r"""
const CH=%s;const LEGEND=%s;let c=0,i=0;
const TIP={};for(const sec of LEGEND)for(const [term,meaning] of sec.rows)TIP[term]=meaning;
function tip(col,val){if(!val)return '';if(TIP[val])return TIP[val];if(col==='key'&&val.startsWith('batch/'))return TIP['batch/<run>#<i>']||'';if(col==='run_id')return TIP['run_id']||'';return ''}
try{const lg=document.getElementById('legend');lg.open=localStorage.getItem('kata-legend')==='open';lg.addEventListener('toggle',()=>{try{localStorage.setItem('kata-legend',lg.open?'open':'closed')}catch(e){}})}catch(e){}
try{const fl=document.getElementById('flow');if(fl){fl.open=localStorage.getItem('kata-flow')!=='closed';fl.addEventListener('toggle',()=>{try{localStorage.setItem('kata-flow',fl.open?'open':'closed')}catch(e){}})}}catch(e){}
function esc(s){return String(s).replace(/[&<>]/g,x=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[x]))}
function save(){try{localStorage.setItem('kata-pos',JSON.stringify([c,i]))}catch(e){}}
function restore(){try{const v=JSON.parse(localStorage.getItem('kata-pos')||'null');if(v){c=Math.min(v[0],CH.length-1);i=Math.min(v[1],CH[c].pages.length-1)}}catch(e){}}
function render(){const ch=CH[c],p=ch.pages[i];
document.getElementById('chapters').innerHTML=CH.map((x,j)=>'<button class="'+(j===c?'on':'')+'" onclick="goc('+j+')">'+(j+1)+' · '+esc(x.title)+'</button>').join('');
document.getElementById('nav').innerHTML=ch.pages.map((x,j)=>'<button class="'+(j===i?'on':'')+'" onclick="go('+j+')">'+(j+1)+'</button>').join('');
document.getElementById('intro').textContent=ch.intro;
document.getElementById('t').textContent='t = '+p.t.toFixed(1)+' s · 第 '+(c+1)+' 章 第 '+(i+1)+' / '+ch.pages.length+' 步';
document.getElementById('h').textContent=p.title;document.getElementById('x').innerHTML=p.text;
const q=document.getElementById('q');if(p.q){q.hidden=false;q.innerHTML='<b>练习</b> '+p.q+'<div><button onclick="document.getElementById(\'a\').hidden=!document.getElementById(\'a\').hidden">看答案</button></div><div class="a" id="a" hidden>'+p.a+'</div>'}else{q.hidden=true}
let g='';const dbs=[...new Set(ch.tables.map(t=>t[0]))];
for(const db of dbs){g+='<div class="db"><h3>'+esc(db)+'</h3>';
for(const [d,tbl,cols,title,meaning] of ch.tables){if(d!==db)continue;const rows=p.tables[tbl]||[];
g+='<div class="wrap"><table><caption>'+esc(title)+'<span>'+esc(meaning)+'</span></caption><tr>'+cols.map(x=>'<th>'+esc(x)+'</th>').join('')+'</tr>';
if(!rows.length)g+='<tr class="empty"><td colspan="'+cols.length+'">（空）</td></tr>';
let lastg=null;for(const r of rows){if(r.group&&r.group!==lastg){g+='<tr class="grp"><td colspan="'+cols.length+'">'+esc(r.group)+'</td></tr>';lastg=r.group}
g+='<tr class="'+(r.kind||'')+'">'+r.cells.map((x,k)=>{const t=tip(cols[k],x);return '<td class="'+(r.changed[k]?'hot':'')+(t?' tipped':'')+'" title="'+esc(t?x+' — '+t:x)+'">'+esc(x)+'</td>'}).join('')+'</tr>'}
g+='</table></div>'}g+='</div>'}
document.getElementById('grid').innerHTML=g;save()}
function go(j){i=Math.max(0,Math.min(CH[c].pages.length-1,j));render();window.scrollTo(0,0)}
function goc(j){c=Math.max(0,Math.min(CH.length-1,j));i=0;render();window.scrollTo(0,0)}
document.addEventListener('keydown',e=>{if(e.target.tagName==='INPUT')return;if(e.key==='ArrowRight'||e.key===' '){e.preventDefault();if(i<CH[c].pages.length-1)go(i+1);else if(c<CH.length-1)goc(c+1)}if(e.key==='ArrowLeft'){e.preventDefault();if(i>0)go(i-1);else if(c>0){c--;i=CH[c].pages.length-1;render()}}});
restore();render();
"""

def main(spec_path, out):
    spec = importlib.util.spec_from_file_location("kata_spec", spec_path)
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    chapters = [build_chapter(ch, mod.TABLES, getattr(mod, "classify", None)) for ch in mod.CHAPTERS]
    legend_html = "".join(
        f"<div><h4>{html.escape(sec['title'])}</h4><table>" + "".join(f"<tr><td>{html.escape(t)}</td><td>{m}</td></tr>" for t, m in sec["rows"]) + "</table></div>"
        for sec in getattr(mod, "LEGEND", []))
    flow_html = ""
    if getattr(mod, "FLOW_SVG", ""):
        flow_html = (f'<details class="legend flow" id="flow" open><summary>数据流：谁写哪张表、谁读哪张表、Airflow 在中间做什么</summary>'
                     f'<figure style="margin:0;padding:0 16px 14px"><div style="overflow-x:auto">{mod.FLOW_SVG}</div>'
                     f'<figcaption style="font-size:13px;color:var(--quiet);max-width:100ch;margin-top:6px">{mod.FLOW_CAPTION}</figcaption></figure></details>')
    page = f"""<title>{html.escape(mod.TITLE)}</title><style>{CSS}</style>
<header><h1>{html.escape(mod.HEADLINE)}</h1><p>{mod.LEAD}</p></header>
{flow_html}
<details class="legend" id="legend"><summary>名词表：run_type、state、每个 task 干什么、五个 asset、四种 key（表格里带虚线的格子悬停也有解释）</summary><div class="lg">{legend_html}</div></details>
<div class="chapters" id="chapters"></div>
<div class="nav" id="nav"></div>
<main><p class="intro" id="intro"></p><div class="step"><div class="t" id="t"></div><h2 id="h"></h2><p id="x"></p><div class="q" id="q" hidden></div></div>
<div class="grid" id="grid"></div></main>
<script>{JS % (json.dumps(chapters, ensure_ascii=False), json.dumps(getattr(mod, "LEGEND", []), ensure_ascii=False))}</script>
"""
    open(out, "w", encoding="utf-8").write(page)
    print("wrote", out, f"{len(page)/1024:.0f} KB,", sum(len(c['pages']) for c in chapters), "pages in", len(chapters), "chapters")

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
