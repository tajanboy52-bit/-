"""
tk_minute.py — TK자동매매 1분봉 수집기 (장중 매수 · 매도 규칙을 검증하고 쓰기 위한 자료)

· 저장: %APPDATA%\\TKAuto\\minute.db (시장 · 장부 DB와 따로 — 하루 약 5~10MB)
· 날짜마다 수집 대상(그날 기준으로 알 수 있던 것만 → 생존 편향 · 미래 정보 없음)
    ① 직전 20거래일 평균 거래대금 상위 N (기본 200 · ETF/스팩/리츠/우선주/정지/관리 제외)
    ② 그 전날 신호 후보 상위 50 (LVH · REV) — 우리 시스템이 실제로 사려던 종목
    ③ 그날 보유 · 거래한 종목 (모의 · 실전 장부)
    ④ KODEX 코스닥150 · KODEX 200 (밤사이 칸)
    ⑤ 그날 급등(고가 +3%↑) · 급락(저가 −5%↓) 종목 — 대상 고르기에만 쓰고, 검증은 분마다 그때 알 수 있던 정보로
· KIS 주식일별분봉조회 FHKST03010230 (과거 · 한 번에 120봉 · KIS가 최대 1년 보관) → 안 되면 당일분봉조회 FHKST03010200 (오늘 · 30봉씩)
· 일정: 거래일 16:20 오늘 분봉 · 과거 채우기는 장 시간을 피해서(평일 18:30~07:00 · 주말) 최근 → 옛날 순서로 이어받기
· 시세 조회만 함 (주문 없음). 실전 키가 있으면 실전 도메인(초당 20건)으로, 없으면 모의(초당 2건 · 느림)
"""
import csv
import io
import os
import sqlite3
import threading
import time
import zipfile
from datetime import datetime

import tk_db as db

MINUTE_DB = os.path.join(db.DATA_DIR, 'minute.db')
SCHEMA = """
CREATE TABLE IF NOT EXISTS bars (ticker TEXT, date TEXT, hm INTEGER, open REAL, high REAL, low REAL, close REAL, vol INTEGER, amt REAL,
    PRIMARY KEY (ticker, date, hm)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_mbars_date ON bars(date, ticker);
CREATE TABLE IF NOT EXISTS universe (date TEXT, ticker TEXT, name TEXT, rank INTEGER, why TEXT, PRIMARY KEY (date, ticker));
CREATE TABLE IF NOT EXISTS done (ticker TEXT, date TEXT, n INTEGER, ts TEXT, PRIMARY KEY (ticker, date));
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
"""
ETFS = {'229200': 'KODEX 코스닥150', '069500': 'KODEX 200'}
STATE = {'running': False, 'msg': '', 'err': '', 'day': '', 'n': 0, 'total': 0, 'stop': False}
_local = threading.local()


def conn():
    c = getattr(_local, 'c', None)
    if c is None:
        c = sqlite3.connect(MINUTE_DB, timeout=30, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute('PRAGMA journal_mode=WAL')
        c.execute('PRAGMA synchronous=NORMAL')
        c.executescript(SCHEMA)
        c.commit()
        _local.c = c
    return c


def meta_get(k, default=''):
    r = conn().execute('SELECT v FROM meta WHERE k=?', (k,)).fetchone()
    return r[0] if r else default


def meta_set(k, v):
    conn().execute('INSERT OR REPLACE INTO meta VALUES (?,?)', (k, str(v)))
    conn().commit()


# ════════════════════════════════════════════
#  수집 대상 (그날 기준)
# ════════════════════════════════════════════
def universe(d, top=200, movers_max=300):
    """→ [(ticker, name, rank, why)] — d 이전 자료로 고른 대상 + 그날 급등락(대상 고르기에만)"""
    m = db.mconn()
    days = db.trading_days('0', d)
    prev = [x for x in days if x < d][-20:]
    st = db.stocks()
    ok = lambda t: t in st and not st[t]['excluded'] and not st[t]['halt'] and not st[t]['admin'] and st[t]['market'] != 'KONEX'
    out, seen = [], set()

    def add(t, why, rank=None):
        if t in seen:
            return
        seen.add(t)
        out.append((t, (st.get(t) or {}).get('name') or ETFS.get(t, t), rank, why))
    if prev:
        q = f"SELECT ticker, AVG(COALESCE(value, close*volume)) v FROM bars WHERE date IN ({','.join('?' * len(prev))}) GROUP BY ticker ORDER BY v DESC"
        k = 0
        for r in m.execute(q, prev):
            if ok(r[0]):
                k += 1
                add(r[0], '거래대금', k)
                if k >= top:
                    break
        for r in m.execute("SELECT ticker, rank, sleeve FROM cands WHERE date=? AND sleeve IN ('LVH','REV') AND rank<=50 ORDER BY rank", (prev[-1],)):
            add(r[0], f'후보 {r[2]}', r[1])
    for mode in ('paper', 'real'):
        try:
            x = db.conn(mode)
            for r in x.execute("SELECT DISTINCT ticker FROM orders WHERE date=?", (d,)):
                add(r[0], '거래')
            for r in x.execute("SELECT DISTINCT ticker FROM lots WHERE entry_date<=? AND (exit_date IS NULL OR exit_date>=?) AND entry_date IS NOT NULL", (d, d)):
                add(r[0], '보유')
        except Exception:
            pass
    for t in ETFS:
        add(t, 'ETF')
    if prev:                                                                        # 그날 급등 · 급락 (일봉이 들어온 뒤에만 알 수 있음)
        rows = m.execute("""SELECT b.ticker, b.high / p.close - 1, b.low / p.close - 1, b.value FROM bars b JOIN bars p ON p.ticker=b.ticker AND p.date=?
                            WHERE b.date=? AND p.close>0""", (prev[-1], d)).fetchall()
        mv = sorted([r for r in rows if ok(r[0]) and (r[3] or 0) >= 5e8 and (r[1] >= 0.03 or r[2] <= -0.05)], key=lambda r: -(r[3] or 0))[:movers_max]
        for r in mv:
            add(r[0], '급등' if r[1] >= 0.03 else '급락')
    return out


# ════════════════════════════════════════════
#  수집
# ════════════════════════════════════════════
def save(d, ticker, bars):
    c = conn()
    c.executemany('INSERT OR REPLACE INTO bars VALUES (?,?,?,?,?,?,?,?,?)', [(ticker, d, *b) for b in bars])
    c.execute('INSERT OR REPLACE INTO done VALUES (?,?,?,?)', (ticker, d, len(bars), db.now_s()))


def collect_day(kc, d, top=200, say=None, today=False):
    """하루치 대상 1분봉 받기 (이미 받은 종목은 건너뜀 · 중단해도 이어받기) → 받은 종목 수"""
    say = say or (lambda m: None)
    uni = universe(d, top)
    c = conn()
    c.executemany('INSERT OR REPLACE INTO universe VALUES (?,?,?,?,?)', [(d, t, n, r, w) for t, n, r, w in uni])
    c.commit()
    have = {r[0]: r[1] for r in c.execute('SELECT ticker, n FROM done WHERE date=?', (d,))}
    todo = [u for u in uni if u[0] not in have or (today and have[u[0]] < 380)]
    got = 0
    for i, (t, nm, _, why) in enumerate(todo):
        if STATE['stop']:
            break
        try:
            bars = kc.minute_day(t, d)
            if not bars and today:
                bars = kc.minute_today(t, d)
        except Exception as e:
            msg = str(e)
            if today:
                try:
                    bars = kc.minute_today(t, d)
                except Exception as e2:
                    db.log(f'[분봉] {nm} {d} 실패: {str(e2)[:120]}', 'warn')
                    continue
            else:
                db.log(f'[분봉] {nm} {d} 실패: {msg[:120]}', 'warn')
                if '보관' in msg or '조회할 자료' in msg or '없습니다' in msg:
                    save(d, t, [])                                                   # KIS에 없는 날짜 → 다시 안 받음
                continue
        save(d, t, bars)
        got += 1
        if i % 20 == 0:
            c.commit()
            STATE.update(n=i + 1, total=len(todo))
            say(f'{d} 분봉 {i + 1}/{len(todo)} · {nm}')
    c.commit()
    if not STATE['stop'] and not today:
        meta_set(f'day_{d}', '1')
    return got


def backfill_days(n_days=250):
    """채울 과거 거래일 (최근 → 옛날) — 시장 일봉이 있는 날 중 아직 다 안 받은 날"""
    days = db.trading_days('0', datetime.now().strftime('%Y%m%d'))
    days = [d for d in days if d < datetime.now().strftime('%Y%m%d')][-n_days:]
    return [d for d in reversed(days) if meta_get(f'day_{d}') != '1']


def run_today(kc, d, top=200):
    if STATE['running']:
        return 0
    STATE.update(running=True, stop=False, err='', day=d, msg=f'{d} 오늘 분봉')
    t0 = time.time()
    try:
        n = collect_day(kc, d, top, say=lambda m: STATE.update(msg=m), today=True)
        meta_set('today_done', d)
        meta_set(f'day_{d}', '1')
        db.log(f'[분봉] {d} 오늘 1분봉 {n}종목 ({time.time() - t0:.0f}초)')
        return n
    except Exception as e:
        STATE['err'] = str(e)[:200]
        db.log(f'[분봉] 오늘 수집 오류: {str(e)[:200]}', 'warn')
        return 0
    finally:
        STATE.update(running=False, msg=STATE['msg'] + ' · 끝')


def run_backfill(kc, n_days=250, top=200, allowed=lambda: True):
    """과거 채우기 — allowed()가 False가 되면(장 시간 가까움) 멈추고 다음에 이어받기"""
    if STATE['running']:
        return 0
    STATE.update(running=True, stop=False, err='', msg='과거 분봉 채우기')
    done = 0
    try:
        for d in backfill_days(n_days):
            if STATE['stop'] or not allowed():
                break
            STATE['day'] = d
            collect_day(kc, d, top, say=lambda m: STATE.update(msg='과거 · ' + m))
            done += 1
        if done:
            db.log(f'[분봉] 과거 {done}일 채움 · 남은 날 {len(backfill_days(n_days))}')
        return done
    except Exception as e:
        STATE['err'] = str(e)[:200]
        db.log(f'[분봉] 과거 채우기 오류: {str(e)[:200]}', 'warn')
        return done
    finally:
        STATE.update(running=False, msg=(STATE['msg'] or '') + ' · 멈춤/끝')


# ════════════════════════════════════════════
#  읽기 · 현황 · 내보내기 · 가져오기
# ════════════════════════════════════════════
def day_bars(ticker, d):
    """[(hm, open, high, low, close, vol, amt)] — 장중 규칙 · 연구에서 사용"""
    return [tuple(r) for r in conn().execute('SELECT hm, open, high, low, close, vol, amt FROM bars WHERE ticker=? AND date=? ORDER BY hm', (ticker, d))]


def status(n_days=250):
    c = conn()
    r = c.execute('SELECT COUNT(DISTINCT date), MIN(date), MAX(date) FROM done WHERE n>0').fetchone()
    size = sum(os.path.getsize(os.path.join(db.DATA_DIR, f)) for f in os.listdir(db.DATA_DIR) if f.startswith('minute.db'))
    last = c.execute('SELECT date, COUNT(*), SUM(n) FROM done WHERE date=(SELECT MAX(date) FROM done WHERE n>0)').fetchone()
    return {'days': r[0], 'first': r[1], 'last': r[2], 'mb': round(size / 1e6, 1), 'last_tickers': last[1] if last else 0,
            'last_bars': last[2] if last else 0, 'left': len(backfill_days(n_days)), 'today_done': meta_get('today_done'),
            'tickers': c.execute('SELECT COUNT(DISTINCT ticker) FROM done WHERE n>0').fetchone()[0], **{k: STATE[k] for k in ('running', 'msg', 'err', 'day')}}


def export_zip(frm='0', to='99999999'):
    """bars/날짜.csv (ticker, hm, open, high, low, close, vol, amt) · universe.csv — 단타 앱과 같은 형식"""
    c = conn()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z:
        for (d,) in c.execute('SELECT DISTINCT date FROM bars WHERE date BETWEEN ? AND ? ORDER BY date', (frm, to)).fetchall():
            s = io.StringIO()
            w = csv.writer(s)
            w.writerow(['ticker', 'hm', 'open', 'high', 'low', 'close', 'vol', 'amt'])
            w.writerows(c.execute('SELECT ticker, hm, open, high, low, close, vol, amt FROM bars WHERE date=? ORDER BY ticker, hm', (d,)))
            z.writestr(f'bars/{d}.csv', s.getvalue())
        s = io.StringIO()
        w = csv.writer(s)
        w.writerow(['date', 'ticker', 'name', 'rank', 'why'])
        w.writerows(c.execute('SELECT * FROM universe WHERE date BETWEEN ? AND ? ORDER BY date, rank', (frm, to)))
        z.writestr('universe.csv', '﻿' + s.getvalue())
    return buf.getvalue()


def import_csv(d, text):
    """bars/날짜.csv 하나 → minute.db (단타 앱 · 이 앱 내보내기 형식) → 줄 수"""
    rows = []
    for r in csv.DictReader(text):
        try:
            rows.append((r['ticker'].zfill(6), d, int(r['hm']), float(r['open']), float(r['high']), float(r['low']), float(r['close']),
                         int(float(r.get('vol') or 0)), float(r.get('amt') or 0)))
        except (KeyError, ValueError):
            continue
    c = conn()
    c.executemany('INSERT OR REPLACE INTO bars VALUES (?,?,?,?,?,?,?,?,?)', rows)
    cnt = {}
    for r in rows:
        cnt[r[0]] = cnt.get(r[0], 0) + 1
    c.executemany('INSERT OR REPLACE INTO done VALUES (?,?,?,?)', [(t, d, n, db.now_s()) for t, n in cnt.items()])
    c.commit()
    return len(rows)


def import_db(path):
    """단타 앱 danta.db (bars: ticker, date, hm, …) → minute.db"""
    src = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    cols = {r[1] for r in src.execute('PRAGMA table_info(bars)')}
    if 'hm' not in cols:
        src.close()
        return 0
    c = conn()
    n = 0
    cur = src.execute('SELECT ticker, date, hm, open, high, low, close, vol, amt FROM bars')
    while True:
        ch = cur.fetchmany(200000)
        if not ch:
            break
        c.executemany('INSERT OR REPLACE INTO bars VALUES (?,?,?,?,?,?,?,?,?)', [tuple(r) for r in ch])
        n += len(ch)
    c.execute("INSERT OR REPLACE INTO done SELECT ticker, date, COUNT(*), ? FROM bars GROUP BY ticker, date", (db.now_s(),))
    c.commit()
    src.close()
    return n
