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
:root{--bg:#f3f4f7;--panel:#ffffff;--ink:#171a21;--quiet:#596074;--faint:#8b91a3;--rule:#e1e4ea;--rule2:#edeff3;--btn:#c9ced8;
--add:#e3f3e8;--chg:#fdf1cf;--rem:#fbe3e3;--remink:#9a3f3f;--hot:#9a4a00;--empty:#a3a8b6;--q:#e9eef8;--qink:#2454c7;--accent:#2454c7;--accent-ink:#ffffff;--shadow:0 1px 2px rgba(20,24,40,.06),0 8px 24px -12px rgba(20,24,40,.18)}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--bg:#15171c;--panel:#1d2027;--ink:#e9ebf0;--quiet:#aab0c0;--faint:#7f8597;--rule:#2f3340;--rule2:#272a34;--btn:#4a5062;
--add:#1d3a2a;--chg:#4a3d14;--rem:#4a2426;--remink:#e3a0a0;--hot:#ffb36b;--empty:#767c8d;--q:#1c2538;--qink:#8fb0ff;--accent:#8fb0ff;--accent-ink:#0f1320;--shadow:0 1px 2px rgba(0,0,0,.4),0 8px 24px -12px rgba(0,0,0,.6);color-scheme:dark}}
:root[data-theme="dark"]{--bg:#15171c;--panel:#1d2027;--ink:#e9ebf0;--quiet:#aab0c0;--faint:#7f8597;--rule:#2f3340;--rule2:#272a34;--btn:#4a5062;
--add:#1d3a2a;--chg:#4a3d14;--rem:#4a2426;--remink:#e3a0a0;--hot:#ffb36b;--empty:#767c8d;--q:#1c2538;--qink:#8fb0ff;--accent:#8fb0ff;--accent-ink:#0f1320;--shadow:0 1px 2px rgba(0,0,0,.4),0 8px 24px -12px rgba(0,0,0,.6);color-scheme:dark}
*{box-sizing:border-box}
body{margin:0;font:15px/1.55 "Inter",-apple-system,BlinkMacSystemFont,"PingFang SC","Helvetica Neue",Arial,sans-serif;color:var(--ink);background:var(--bg)}
body.present{font-size:17px}
h1,h2,h3,.chapters button,.chapters .num{font-family:"Inter Tight","Inter",-apple-system,BlinkMacSystemFont,"PingFang SC",sans-serif}
code,td,.mono{font-family:"JetBrains Mono",ui-monospace,SFMono-Regular,Menlo,monospace}
header{display:flex;align-items:baseline;gap:14px;flex-wrap:wrap;padding:16px 20px 10px;max-width:1440px;margin:0 auto}
header h1{font-size:20px;margin:0;letter-spacing:-.01em;text-wrap:balance}header .sub{color:var(--quiet);font-size:13.5px;flex:1 1 420px;max-width:90ch}
header .tools{display:flex;gap:6px;margin-left:auto}
.tools button{border:1px solid var(--btn);background:var(--panel);color:var(--ink);border-radius:8px;padding:4px 10px;font-size:12.5px;cursor:pointer}
.tools button.on{background:var(--accent);color:var(--accent-ink);border-color:var(--accent)}
.chapters{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px;padding:8px 20px 4px;max-width:1440px;margin:0 auto}
.chapters button{text-align:left;border:1px solid var(--rule);background:var(--panel);color:var(--ink);border-radius:12px;padding:10px 12px;cursor:pointer;box-shadow:var(--shadow);display:grid;grid-template-columns:auto 1fr;gap:4px 10px;align-items:center}
.chapters button .num{font-size:22px;font-weight:700;color:var(--faint);line-height:1}
.chapters button .t{font-weight:600;font-size:13.5px;line-height:1.25}
.chapters button .c{grid-column:2;color:var(--quiet);font-size:12px;line-height:1.3}
.chapters button.on{border-color:var(--accent);outline:2px solid var(--accent);outline-offset:-1px}.chapters button.on .num{color:var(--accent)}
.nav{display:flex;gap:5px;padding:10px 20px;flex-wrap:wrap;align-items:center;position:sticky;top:env(safe-area-inset-top,0px);z-index:3;background:var(--bg);max-width:1440px;margin:0 auto}
.nav button{border:1px solid var(--btn);background:var(--panel);color:var(--ink);border-radius:999px;min-width:30px;height:28px;padding:0 9px;font-size:12.5px;cursor:pointer;font-variant-numeric:tabular-nums}
.nav button.done{border-color:var(--rule);color:var(--faint)}.nav button.on{background:var(--ink);color:var(--bg);border-color:var(--ink)}
.nav .bar{flex:1 1 120px;height:3px;background:var(--rule);border-radius:2px;overflow:hidden;margin-left:8px}.nav .bar i{display:block;height:100%;background:var(--accent);width:0}
button:focus-visible{outline:2px solid var(--hot)}
main{padding:10px 20px 70px;max-width:1440px;margin:0 auto}
.intro{color:var(--quiet);font-size:14px;margin:4px 0 12px;max-width:95ch}
.step{background:var(--panel);border:1px solid var(--rule);border-radius:14px;padding:18px 22px;margin-bottom:16px;box-shadow:var(--shadow)}
.step .t{font-size:12px;color:var(--faint);letter-spacing:.04em;text-transform:uppercase}
.step h2{font-size:19px;margin:4px 0 8px;letter-spacing:-.01em;text-wrap:balance;line-height:1.3}
.step p{margin:0 0 8px;max-width:88ch}
.q{background:var(--q);border-radius:10px;padding:12px 16px;margin-top:12px;max-width:88ch}.q b{color:var(--qink)}
.q button{margin-top:8px;border:1px solid var(--btn);background:var(--panel);color:var(--ink);border-radius:8px;padding:4px 12px;font-size:13px;cursor:pointer}
.q .a{margin-top:10px;border-top:1px dashed var(--btn);padding-top:10px}
body.noq .q{display:none}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}@media (max-width:980px){.grid{grid-template-columns:1fr}}
.db{border:1px solid var(--rule);border-radius:14px;background:var(--panel);padding:14px 16px;min-width:0;box-shadow:var(--shadow)}
.db h3{margin:0 0 10px;font-size:12px;letter-spacing:.06em;text-transform:uppercase;color:var(--quiet)}
.wrap{overflow-x:auto;margin-bottom:14px}table{border-collapse:collapse;width:100%;font-size:12.5px;font-variant-numeric:tabular-nums}
caption{text-align:left;font-weight:600;font-size:13.5px;padding:0 0 4px;color:var(--ink)}caption span{font-weight:400;color:var(--faint);margin-left:8px;font-size:12.5px}
th{text-align:left;color:var(--quiet);font-weight:500;border-bottom:1px solid var(--btn);padding:4px 7px;white-space:nowrap;font-size:12px}
td{padding:3px 7px;border-bottom:1px solid var(--rule2);vertical-align:top;font-size:12px;max-width:440px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
body.present td{font-size:13px}body.present table{font-size:13.5px}
tr.grp td{background:transparent;color:var(--qink);font-family:inherit;font-size:11.5px;letter-spacing:.03em;padding:8px 7px 2px;border-bottom:1px solid var(--rule);font-weight:600;white-space:normal}
tr.add td{background:var(--add)}tr.change td{background:var(--chg)}tr.remove td{background:var(--rem);text-decoration:line-through;color:var(--remink)}
td.hot{font-weight:700;color:var(--hot)}td.tipped{text-decoration:underline dotted var(--btn);text-underline-offset:3px;cursor:help}tr.empty td{color:var(--empty);font-style:italic;font-family:inherit}
.k{display:inline-block;padding:0 6px;border-radius:4px}kbd{border:1px solid var(--btn);border-radius:4px;padding:0 5px;font-size:12px;font-family:inherit}code{font-size:.92em}
details.legend{background:var(--panel);border:1px solid var(--rule);border-radius:14px;margin:0 20px 14px;padding:0;max-width:1400px}
@media (min-width:1441px){details.legend{margin-left:auto;margin-right:auto}}
details.legend summary{cursor:pointer;padding:11px 16px;font-weight:600;font-size:13.5px;list-style:none}details.legend summary::before{content:"▸ ";color:var(--faint)}details.legend[open] summary::before{content:"▾ "}
body.present details.legend{display:none}
.lg{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,520px),1fr));gap:16px 28px;padding:0 16px 16px}
.lg h4{margin:0 0 6px;font-size:12px;letter-spacing:.05em;text-transform:uppercase;color:var(--quiet)}
.lg table{font-size:12.5px}.lg td{white-space:normal;font-family:inherit;font-size:12.5px;max-width:none;line-height:1.45;padding:5px 6px}.lg td:first-child{font-family:"JetBrains Mono",ui-monospace,monospace;font-size:12px;white-space:nowrap;color:var(--qink);vertical-align:top;width:1%;padding-right:12px}
/* overview (html) pages */
.ov{display:grid;gap:14px}.ov .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px}
.ov .card{background:var(--panel);border:1px solid var(--rule);border-radius:14px;padding:14px 16px;box-shadow:var(--shadow)}
.ov .card h4{margin:0 0 6px;font-size:13px;letter-spacing:.04em;text-transform:uppercase;color:var(--quiet)}.ov .card .big{font-size:26px;font-weight:700;letter-spacing:-.02em;font-family:"Inter Tight","Inter",sans-serif;font-variant-numeric:tabular-nums}
.ov .card p{margin:4px 0 0;font-size:13.5px;color:var(--quiet)}
.ov table.plain td{white-space:normal;font-family:inherit;font-size:13.5px;max-width:none;line-height:1.45;padding:7px 8px;vertical-align:top}.ov table.plain th{font-size:12.5px}
.ov .flow{display:flex;align-items:stretch;gap:0;flex-wrap:wrap}
.ov .box{flex:1 1 150px;background:var(--panel);border:1px solid var(--rule);border-radius:12px;padding:12px 14px;box-shadow:var(--shadow);position:relative}
.ov .box b{display:block;font-size:14px;margin-bottom:4px}.ov .box span{font-size:12.5px;color:var(--quiet)}
.ov .arrow{flex:0 0 28px;display:flex;align-items:center;justify-content:center;color:var(--faint);font-size:18px}
.ov .box.ours{border-color:var(--accent)}.ov .tag{position:absolute;top:-9px;right:12px;font-size:10.5px;letter-spacing:.06em;text-transform:uppercase;background:var(--accent);color:var(--accent-ink);border-radius:999px;padding:1px 8px}
.ov .box.air .tag{background:var(--btn);color:var(--ink)}
.ov figure{margin:0;background:var(--panel);border:1px solid var(--rule);border-radius:14px;padding:14px 16px;box-shadow:var(--shadow)}.ov figcaption{font-size:13px;color:var(--quiet);margin-top:8px;max-width:100ch}
"""

JS = r"""
const CH=%s;const LEGEND=%s;let c=0,i=0;
const TIP={};for(const sec of LEGEND)for(const [term,meaning] of sec.rows)TIP[term]=meaning;
function tip(col,val){if(!val)return '';if(TIP[val])return TIP[val];if(col==='key'&&val.startsWith('batch/'))return TIP['batch/<run>#<i>']||'';if(col==='key'&&val.startsWith('window/'))return TIP['window/<window_end>']||'';if(col==='run_id')return TIP['run_id']||'';return ''}
function esc(s){return String(s).replace(/[&<>]/g,x=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[x]))}
function save(){try{localStorage.setItem('kata-pos',JSON.stringify([c,i]))}catch(e){}}
function restore(){try{const v=JSON.parse(localStorage.getItem('kata-pos')||'null');if(v){c=Math.min(v[0],CH.length-1);i=Math.min(v[1],CH[c].pages.length-1)}}catch(e){}}
function pref(k,on){try{localStorage.setItem(k,on?'1':'0')}catch(e){}}
function getpref(k){try{return localStorage.getItem(k)==='1'}catch(e){return false}}
function setmode(k,on){document.body.classList.toggle(k,on);pref('kata-'+k,on);document.querySelectorAll('[data-mode="'+k+'"]').forEach(b=>b.classList.toggle('on',on))}
try{const lg=document.getElementById('legend');if(lg){lg.open=localStorage.getItem('kata-legend')==='open';lg.addEventListener('toggle',()=>{try{localStorage.setItem('kata-legend',lg.open?'open':'closed')}catch(e){}})}}catch(e){}
function render(){const ch=CH[c],p=ch.pages[i];
document.getElementById('chapters').innerHTML=CH.map((x,j)=>'<button class="'+(j===c?'on':'')+'" onclick="goc('+j+')"><span class="num">'+j+'</span><span class="t">'+esc(x.title)+'</span><span class="c">'+esc(x.claim||'')+'</span></button>').join('');
document.getElementById('nav').innerHTML=ch.pages.map((x,j)=>'<button class="'+(j===i?'on':(j<i?'done':''))+'" onclick="go('+j+')" title="'+esc(x.title)+'">'+(j+1)+'</button>').join('')+'<span class="bar"><i style="width:'+Math.round(100*(i+1)/ch.pages.length)+'%%"></i></span>';
document.getElementById('intro').textContent=ch.intro;
const step=document.getElementById('step'),grid=document.getElementById('grid'),ov=document.getElementById('ov');
if(p.html){step.hidden=true;grid.hidden=true;ov.hidden=false;ov.innerHTML='<div class="step"><div class="t">'+esc(ch.label||'')+' · '+(i+1)+' / '+ch.pages.length+'</div><h2>'+esc(p.title)+'</h2></div><div class="ov">'+p.html+'</div>';save();return}
step.hidden=false;grid.hidden=false;ov.hidden=true;
document.getElementById('t').textContent='t = '+p.t.toFixed(1)+' s · '+(ch.label||'')+' · '+(i+1)+' / '+ch.pages.length;
document.getElementById('h').textContent=p.title;document.getElementById('x').innerHTML=p.text;
const q=document.getElementById('q');if(p.q){q.hidden=false;q.innerHTML='<b>'+esc(QLABEL)+'</b> '+p.q+'<div><button onclick="document.getElementById(\'a\').hidden=!document.getElementById(\'a\').hidden">'+esc(ALABEL)+'</button></div><div class="a" id="a" hidden>'+p.a+'</div>'}else{q.hidden=true}
let g='';const dbs=[...new Set(ch.tables.map(t=>t[0]))];
for(const db of dbs){g+='<div class="db"><h3>'+esc(db)+'</h3>';
for(const [d,tbl,cols,title,meaning] of ch.tables){if(d!==db)continue;const rows=p.tables[tbl]||[];
g+='<div class="wrap"><table><caption>'+esc(title)+'<span>'+esc(meaning)+'</span></caption><tr>'+cols.map(x=>'<th>'+esc(x)+'</th>').join('')+'</tr>';
if(!rows.length)g+='<tr class="empty"><td colspan="'+cols.length+'">'+esc(EMPTY)+'</td></tr>';
let lastg=null;for(const r of rows){if(r.group&&r.group!==lastg){g+='<tr class="grp"><td colspan="'+cols.length+'">'+esc(r.group)+'</td></tr>';lastg=r.group}
g+='<tr class="'+(r.kind||'')+'">'+r.cells.map((x,k)=>{const t=tip(cols[k],x);return '<td class="'+(r.changed[k]?'hot':'')+(t?' tipped':'')+'" title="'+esc(t?x+' — '+t:x)+'">'+esc(x)+'</td>'}).join('')+'</tr>'}
g+='</table></div>'}g+='</div>'}
grid.innerHTML=g;save()}
function go(j){i=Math.max(0,Math.min(CH[c].pages.length-1,j));render();window.scrollTo(0,0)}
function goc(j){c=Math.max(0,Math.min(CH.length-1,j));i=0;render();window.scrollTo(0,0)}
document.addEventListener('keydown',e=>{if(e.target.tagName==='INPUT')return;if(e.key==='ArrowRight'||e.key===' '){e.preventDefault();if(i<CH[c].pages.length-1)go(i+1);else if(c<CH.length-1)goc(c+1)}if(e.key==='ArrowLeft'){e.preventDefault();if(i>0)go(i-1);else if(c>0){c--;i=CH[c].pages.length-1;render()}}if(e.key==='p'||e.key==='P'){setmode('present',!document.body.classList.contains('present'))}});
const QLABEL=%s,ALABEL=%s,EMPTY=%s;
setmode('present',getpref('kata-present'));setmode('noq',getpref('kata-noq'));
restore();render();
"""

def main(spec_path, out):
    spec = importlib.util.spec_from_file_location("kata_spec", spec_path)
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    chapters = []
    for ch in mod.CHAPTERS:
        if ch.get("html_pages"):
            chapters.append({"id": ch["id"], "title": ch["title"], "claim": ch.get("claim", ""), "label": ch.get("label", ""), "intro": ch.get("intro", ""),
                             "tables": [], "pages": [{"title": p["title"], "html": p["html"]} for p in ch["html_pages"]]})
        else:
            built = build_chapter(ch, mod.TABLES, getattr(mod, "classify", None))
            built["claim"] = ch.get("claim", ""); built["label"] = ch.get("label", "")
            chapters.append(built)
    legend_html = "".join(
        f"<div><h4>{html.escape(sec['title'])}</h4><table>" + "".join(f"<tr><td>{html.escape(t)}</td><td>{m}</td></tr>" for t, m in sec["rows"]) + "</table></div>"
        for sec in getattr(mod, "LEGEND", []))
    L = getattr(mod, "LABELS", {})
    lab = lambda k, d: L.get(k, d)
    page = f"""<title>{html.escape(mod.TITLE)}</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter+Tight:wght@500;600;700&family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap">
<style>{CSS}</style>
<header><h1>{html.escape(mod.HEADLINE)}</h1><div class="sub">{mod.LEAD}</div>
<div class="tools"><button data-mode="present" onclick="setmode('present',!document.body.classList.contains('present'))" title="P">{html.escape(lab('present', '演示模式'))}</button><button data-mode="noq" onclick="setmode('noq',!document.body.classList.contains('noq'))">{html.escape(lab('noq', '隐藏练习'))}</button></div></header>
<div class="chapters" id="chapters"></div>
<div class="nav" id="nav"></div>
<details class="legend" id="legend"><summary>{lab('legend_title', '名词表：run_type、state、每个 task 干什么、asset、key（表格里带虚线的格子悬停也有解释）')}</summary><div class="lg">{legend_html}</div></details>
<main><p class="intro" id="intro"></p><div class="step" id="step"><div class="t" id="t"></div><h2 id="h"></h2><p id="x"></p><div class="q" id="q" hidden></div></div>
<div id="ov" hidden></div>
<div class="grid" id="grid"></div></main>
<script>{JS % (json.dumps(chapters, ensure_ascii=False), json.dumps(getattr(mod, "LEGEND", []), ensure_ascii=False), json.dumps(lab('q', '练习'), ensure_ascii=False), json.dumps(lab('a', '看答案'), ensure_ascii=False), json.dumps(lab('empty', '（空）'), ensure_ascii=False))}</script>
"""
    open(out, "w", encoding="utf-8").write(page)
    print("wrote", out, f"{len(page)/1024:.0f} KB,", sum(len(c['pages']) for c in chapters), "pages in", len(chapters), "chapters")

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
