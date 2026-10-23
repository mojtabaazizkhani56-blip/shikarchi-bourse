"""Shikarchi Bourse v2: local backend for Tehran market-wide screening.
Public market data only. Endpoint behavior can change; no trading execution.
"""
from __future__ import annotations
import json, os, sqlite3, threading, time
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "data" / "market.db"
TZ = ZoneInfo("Asia/Tehran")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
BASE = os.getenv("TSETMC_BASE", "https://cdn.tsetmc.com/api")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (compatible; ShikarchiBourse/2.0; public-data-client)", "Accept": "application/json,text/plain,*/*"})
app = FastAPI(title="Shikarchi Bourse API", version="2.0.0")
app.mount("/static", StaticFiles(directory=ROOT / "static"), name="static")


def db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB_PATH, timeout=15)
    c.row_factory = sqlite3.Row
    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, day TEXT NOT NULL,
      ins_code TEXT, symbol TEXT, name TEXT, last REAL, close REAL, yesterday REAL,
      change_pct REAL, volume REAL, value REAL, trades REAL, base_volume REAL,
      buyer_power REAL, net_real REAL, score REAL, raw TEXT)''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_snap_day_symbol ON snapshots(day,symbol)')
    c.execute('''CREATE TABLE IF NOT EXISTS status (
      id INTEGER PRIMARY KEY CHECK(id=1), last_success TEXT, last_error TEXT,
      market_open INTEGER DEFAULT 0, source TEXT, count INTEGER DEFAULT 0)''')
    c.execute('INSERT OR IGNORE INTO status(id) VALUES(1)')
    c.commit(); return c


def unwrap(obj: Any):
    if isinstance(obj, dict):
        for key in ('marketwatch','marketWatch','marketWatchDto','closingPriceInfo','clientTypeAllDto','data','items','result'):
            if key in obj and isinstance(obj[key], (list,dict)):
                val = obj[key]
                if isinstance(val, dict):
                    for k in ('marketwatch','marketWatch','items','data','result'):
                        if k in val and isinstance(val[k], list): return val[k]
                return val
        # single-wrapper APIs
        if len(obj) == 1:
            val = next(iter(obj.values()))
            if isinstance(val, (list,dict)): return val
    return obj


def pick(d: dict, *keys, default=None):
    for k in keys:
        if k in d and d[k] is not None and d[k] != '': return d[k]
    return default


def num(v, default=0.0):
    try: return float(str(v).replace(',', '').strip())
    except (TypeError,ValueError): return default


def fetch_marketwatch():
    # Public TSETMC REST endpoints; endpoint schema may evolve. Try the market-wide feed first.
    params = [('market','0'), ('withBestLimits','false'), ('hEven','0'), ('RefID','0')]
    params += [(f'paperTypes[{i}]', str(i+1)) for i in range(9)]
    urls = [f'{BASE}/ClosingPrice/GetMarketWatch']
    errors=[]
    for url in urls:
        try:
            r = SESSION.get(url, params=params, timeout=18)
            r.raise_for_status()
            data = r.json()
            rows = unwrap(data)
            if isinstance(rows, dict): rows = list(rows.values()) if all(isinstance(x,dict) for x in rows.values()) else []
            if not isinstance(rows,list) or not rows:
                errors.append(f'{url}: پاسخ خالی/ساختار ناشناخته'); continue
            # Normalize each instrument row without inventing missing data.
            out=[]
            for x in rows:
                if not isinstance(x,dict): continue
                sym = pick(x,'lVal18AFC','symbol','Symbol','insName','instrumentName','lVal30')
                code = pick(x,'insCode','InsCode','instrumentCode','instrumentID')
                if not sym or not code: continue
                last = num(pick(x,'pDrCotVal','last','lastPrice','pl','lastTradedPrice'))
                close = num(pick(x,'pClosing','close','closingPrice','pc'))
                yest = num(pick(x,'priceYesterday','yesterday','py','yesterdayPrice'))
                volume = num(pick(x,'qTotTran5J','volume','totalVolume','qTotTran'))
                value = num(pick(x,'qTotCap','value','tradeValue','totalValue'))
                trades = num(pick(x,'zTotTran','tradeCount','numberOfTrades'))
                basevol = num(pick(x,'baseVol','baseVolume'))
                pct = ((close/yest)-1)*100 if close and yest else num(pick(x,'percent','changePercent','change'))
                out.append({'ins_code':str(code),'symbol':str(sym).strip(),'name':str(pick(x,'lVal30','name','instrumentName',default=sym)).strip(),
                    'last':last,'close':close,'yesterday':yest,'change_pct':round(pct,3),'volume':volume,'value':value,'trades':trades,'base_volume':basevol,'raw':x})
            if out: return out, url
            errors.append(f'{url}: هیچ ردیف قابل‌خواندنی نبود')
        except Exception as e: errors.append(f'{url}: {type(e).__name__}: {e}')
    raise RuntimeError(' | '.join(errors))


def fetch_real_client_flow():
    # Bulk endpoint is optional; if unavailable, the scan clearly marks flow as unavailable.
    url=f'{BASE}/ClientType/GetClientTypeAll'
    try:
        r=SESSION.get(url,timeout=12); r.raise_for_status(); rows=unwrap(r.json())
        if isinstance(rows,dict): rows=list(rows.values()) if all(isinstance(x,dict) for x in rows.values()) else []
        result={}
        for x in rows if isinstance(rows,list) else []:
            if not isinstance(x,dict): continue
            code=str(pick(x,'insCode','InsCode','instrumentCode',default=''))
            if not code: continue
            bi=num(pick(x,'buy_I_Volume','buyIVolume','buyIndividualVolume'))
            si=num(pick(x,'sell_I_Volume','sellIVolume','sellIndividualVolume'))
            result[code]={'net_real':bi-si,'buyer_power':(bi/max(si,1)) if si else (bi if bi else 0)}
        return result
    except Exception: return {}


def is_market_open(now=None):
    now=now or datetime.now(TZ)
    # Tehran market commonly trades Saturday–Wednesday, 09:00–12:30; holidays are not inferred here.
    return now.weekday() in (5,6,0,1,2) and dtime(9,0) <= now.time() <= dtime(12,30)


def score_row(row, previous_volumes, flow):
    code=row['ins_code']; f=flow.get(code,{})
    row['net_real']=f.get('net_real')
    row['buyer_power']=f.get('buyer_power')
    prev=previous_volumes.get(code, [])
    avg=(sum(prev)/len(prev)) if prev else 0
    row['relative_volume']=round(row['volume']/avg,2) if avg else None
    # Conservative transparent screening score, not a price target or buy recommendation.
    score=0
    if row['change_pct'] > 0: score += min(18,row['change_pct']*4)
    if row['relative_volume'] is not None: score += max(0,min(25,(row['relative_volume']-1)*12))
    if row['net_real'] is not None:
        score += 22 if row['net_real']>0 else (5 if row['net_real']==0 else 0)
    if row['buyer_power'] is not None: score += max(0,min(20,(row['buyer_power']-1)*10))
    if row['value'] > 0: score += min(15, row['value']/1e11)
    row['score']=round(min(100,score),1)
    if row['score']>=65: row['status']='review'
    elif row['score']>=40: row['status']='watch'
    else: row['status']='risk'
    row['flow_available']=row['net_real'] is not None
    return row


def save_snapshot(rows, source):
    now=datetime.now(TZ); ts=now.isoformat(timespec='seconds'); day=now.date().isoformat()
    conn=db()
    # Historical average of recent intraday snapshots per symbol (not full-day average).
    prev={}
    for row in rows:
        rec=conn.execute('SELECT volume FROM snapshots WHERE ins_code=? AND day<>? ORDER BY id DESC LIMIT 10',(row['ins_code'],day)).fetchall()
        vals=[num(x['volume']) for x in rec if num(x['volume'])>0]
        if vals: prev[row['ins_code']]=vals
    flow=fetch_real_client_flow()
    rows=[score_row(r,prev,flow) for r in rows]
    conn.executemany('''INSERT INTO snapshots(ts,day,ins_code,symbol,name,last,close,yesterday,change_pct,volume,value,trades,base_volume,buyer_power,net_real,score,raw)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',[(ts,day,r['ins_code'],r['symbol'],r['name'],r['last'],r['close'],r['yesterday'],r['change_pct'],r['volume'],r['value'],r['trades'],r['base_volume'],r['buyer_power'],r['net_real'],r['score'],json.dumps(r['raw'],ensure_ascii=False)) for r in rows])
    conn.execute('UPDATE status SET last_success=?,last_error=NULL,market_open=?,source=?,count=? WHERE id=1',(ts,int(is_market_open(now)),source,len(rows)))
    conn.commit(); conn.close()
    return rows,ts


def latest_rows():
    conn=db()
    rows=conn.execute('''SELECT s.* FROM snapshots s JOIN (SELECT ins_code,MAX(id) maxid FROM snapshots GROUP BY ins_code) x ON s.id=x.maxid ORDER BY score DESC''').fetchall()
    out=[]
    for x in rows:
        d=dict(x); d['raw']=json.loads(d['raw']) if d.get('raw') else {}; d['flow_available']=d.get('net_real') is not None
        d['status']='review' if (d.get('score') or 0)>=65 else ('watch' if (d.get('score') or 0)>=40 else 'risk')
        # Relative volume is only comparable if an older day exists; avoid fabricated values.
        d['relative_volume']=None
        out.append(d)
    conn.close(); return out


def poll_once():
    try:
        rows,source=fetch_marketwatch(); save_snapshot(rows,source)
        return {'ok':True,'count':len(rows),'source':source}
    except Exception as e:
        conn=db(); conn.execute('UPDATE status SET last_error=? WHERE id=1',(str(e)[:1000],)); conn.commit(); conn.close()
        return {'ok':False,'error':str(e)}


def poller():
    while True:
        now=datetime.now(TZ)
        # Take one snapshot before market open, poll during the session, and one after close for daily summary.
        should_poll=is_market_open(now) or (now.weekday() in (5,6,0,1,2) and dtime(12,30) < now.time() < dtime(12,34))
        if should_poll: poll_once()
        time.sleep(POLL_SECONDS if is_market_open(now) else 120)

@app.on_event('startup')
def startup():
    db().close()
    if os.getenv('ENABLE_POLLER','1') == '1': threading.Thread(target=poller,daemon=True).start()

@app.get('/')
def index(): return FileResponse(ROOT/'static'/'index.html')

@app.get('/api/status')
def status():
    c=db(); row=dict(c.execute('SELECT * FROM status WHERE id=1').fetchone()); c.close()
    row['market_open_now']=is_market_open(); row['timezone']='Asia/Tehran'; row['poll_seconds']=POLL_SECONDS
    return row

@app.post('/api/refresh')
def refresh(): return poll_once()

@app.get('/api/market')
def market(q:str='', status_filter:str='all', sort:str='score', limit:int=1500):
    rows=latest_rows()
    if q: rows=[r for r in rows if q.casefold() in (r['symbol']+' '+r['name']).casefold()]
    if status_filter!='all': rows=[r for r in rows if r.get('status')==status_filter]
    key={'score':'score','change':'change_pct','value':'value','volume':'volume','money':'net_real'}.get(sort,'score')
    rows.sort(key=lambda r: (r.get(key) is not None, r.get(key) or 0), reverse=True)
    return {'count':len(rows),'rows':rows[:max(1,min(limit,5000))]}

@app.get('/api/summary')
def summary():
    rows=latest_rows(); c=db(); st=dict(c.execute('SELECT * FROM status WHERE id=1').fetchone())
    today=datetime.now(TZ).date().isoformat()
    hist=c.execute('SELECT COUNT(*) n, COUNT(DISTINCT symbol) symbols, MAX(ts) last FROM snapshots WHERE day=?',(today,)).fetchone(); c.close()
    changes=[r['change_pct'] for r in rows if r.get('change_pct') is not None]
    return {'symbols':len(rows),'positive':sum(x>0 for x in changes),'negative':sum(x<0 for x in changes),'unchanged':sum(x==0 for x in changes),
      'avg_change':round(sum(changes)/len(changes),2) if changes else None,'review':sum(r.get('status')=='review' for r in rows),
      'watch':sum(r.get('status')=='watch' for r in rows),'risk':sum(r.get('status')=='risk' for r in rows),
      'snapshots_today':hist['n'],'last_snapshot':hist['last'],'market_open_now':is_market_open(),'status':st}

@app.get('/api/report/daily')
def daily_report():
    rows=latest_rows(); c=db(); today=datetime.now(TZ).date().isoformat()
    n=c.execute('SELECT COUNT(*) FROM snapshots WHERE day=?',(today,)).fetchone()[0]
    c.close()
    changes=[r['change_pct'] for r in rows if r.get('change_pct') is not None]
    return {'date':today,'snapshot_count':n,'universe_count':len(rows),'breadth':{'positive':sum(x>0 for x in changes),'negative':sum(x<0 for x in changes),'unchanged':sum(x==0 for x in changes)},
      'avg_change_pct':round(sum(changes)/len(changes),2) if changes else None,
      'leaders':sorted(rows,key=lambda r:r.get('change_pct') or -999,reverse=True)[:10],
      'laggards':sorted(rows,key=lambda r:r.get('change_pct') or 999)[:10],
      'highest_value':sorted(rows,key=lambda r:r.get('value') or 0,reverse=True)[:10],
      'top_review':sorted([r for r in rows if r.get('status')=='review'],key=lambda r:r.get('score') or 0,reverse=True)[:15],
      'notes':['امتیازها برای اولویت‌بندی بررسی‌اند، نه سیگنال خرید.','جریان پول حقیقی فقط وقتی نمایش داده می‌شود که منبع داده آن قابل‌دسترسی باشد.','گزارش تا زمان آخرین دریافت موفق داده معتبر است.']}

@app.get('/api/history/{ins_code}')
def history(ins_code:str):
    c=db(); rows=c.execute('SELECT ts,day,last,close,change_pct,volume,value,buyer_power,net_real,score FROM snapshots WHERE ins_code=? ORDER BY id DESC LIMIT 300',(ins_code,)).fetchall(); c.close()
    return {'ins_code':ins_code,'rows':[dict(x) for x in rows]}

if __name__=='__main__':
    import uvicorn
    uvicorn.run('app:app',host=os.getenv('HOST','127.0.0.1'),port=int(os.getenv('PORT','8000')),reload=False)
