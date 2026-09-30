"""
q_db.py — 台炅 퀀트 자동매매 (TK Quant) 저장소 · 데이터 읽기

· 자체 DB: %APPDATA%\\TKQuant\\quant.db (TKQUANT_DATA 환경변수로 바꿀 수 있음) — 주문 · 보유 묶음(lot) · 신호 · 평가 · 로그
· Scout(스윙 앱) 데이터를 **읽기 전용**으로 사용 — 전종목 일봉(수정주가 · 상장폐지 포함) · 종목 정보 · 외국인/연기금 수급 · ETF 일봉
  %APPDATA%\\StockScout\\scout.db · etf.db (SCOUT_DATA 환경변수로 바꿀 수 있음) — Scout 파일은 절대 바꾸지 않음
· 구성 종목 · 월별 재무(배당 · PBR · EPS): 처음엔 ../우량주/seed (2019-01~2026-09), 이후 매월 KRX(pykrx · KRX 계정)
"""
import csv
import os
import sqlite3
import threading
from datetime import datetime

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get('TKQUANT_DATA') or os.path.join(os.environ.get('APPDATA') or os.path.expanduser('~'), 'TKQuant')
os.makedirs(DATA_DIR, exist_ok=True)
DB_FILE = os.path.join(DATA_DIR, 'quant.db')
SEED_DIRS = [os.path.join(HERE, 'seed'), os.path.join(HERE, '..', '우량주', 'seed')]

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS members (month TEXT, ticker TEXT, idx TEXT, PRIMARY KEY (month, ticker));
CREATE TABLE IF NOT EXISTS monthly (month TEXT, date TEXT, ticker TEXT, market TEXT, name TEXT, sector TEXT, marcap REAL,
    eps REAL, div REAL, pbr REAL, PRIMARY KEY (month, ticker));
CREATE TABLE IF NOT EXISTS signals (date TEXT, sleeve TEXT, rank INTEGER, ticker TEXT, name TEXT, score REAL, ref REAL, info TEXT,
    PRIMARY KEY (date, sleeve, ticker));
CREATE TABLE IF NOT EXISTS lots (id INTEGER PRIMARY KEY AUTOINCREMENT, sleeve TEXT, ticker TEXT, name TEXT, sector TEXT,
    signal_date TEXT, entry_date TEXT, entry_px REAL, qty INTEGER DEFAULT 0, qty0 INTEGER DEFAULT 0, cost REAL DEFAULT 0, status TEXT,
    days INTEGER DEFAULT 0, last_px REAL, sell_flag INTEGER DEFAULT 0, sell_reason TEXT,
    exit_date TEXT, exit_px REAL, proceeds REAL DEFAULT 0, pnl REAL, ret REAL, updated TEXT);
CREATE INDEX IF NOT EXISTS ix_lots ON lots(status, sleeve);
CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, ts TEXT, sleeve TEXT, lot_id INTEGER,
    ticker TEXT, name TEXT, side TEXT, kind TEXT, qty INTEGER, ord_dvsn TEXT, price REAL, order_no TEXT, org_no TEXT,
    status TEXT, filled INTEGER DEFAULT 0, applied INTEGER DEFAULT 0, avg REAL, msg TEXT);
CREATE INDEX IF NOT EXISTS ix_orders ON orders(date, status);
CREATE TABLE IF NOT EXISTS equity (date TEXT PRIMARY KEY, cash REAL, value REAL, npos INTEGER, peak REAL);
CREATE TABLE IF NOT EXISTS sleeve_daily (date TEXT, sleeve TEXT, invested REAL, value REAL, realized REAL, npos INTEGER,
    PRIMARY KEY (date, sleeve));
CREATE TABLE IF NOT EXISTS log (id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, level TEXT, msg TEXT);
CREATE TABLE IF NOT EXISTS days (date TEXT PRIMARY KEY, done_at TEXT, note TEXT);
"""
_local = threading.local()


def conn():
    c = getattr(_local, 'c', None)
    if c is None:
        c = sqlite3.connect(DB_FILE, timeout=30, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute('PRAGMA journal_mode=WAL')
        c.executescript(SCHEMA)
        c.commit()
        _local.c = c
    return c


def meta_get(k, default=''):
    r = conn().execute('SELECT v FROM meta WHERE k=?', (k,)).fetchone()
    return r[0] if r else default


def meta_set(k, v):
    c = conn()
    c.execute('INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)', (k, str(v)))
    c.commit()


def now_s():
    return datetime.now().isoformat(timespec='seconds')


def log(msg, level='info'):
    line = f"[{datetime.now():%m-%d %H:%M:%S}] {msg}"
    print(line, flush=True)
    try:
        c = conn()
        c.execute('INSERT INTO log (ts, level, msg) VALUES (?,?,?)', (now_s(), level, str(msg)[:600]))
        c.commit()
    except Exception:
        pass


# ════════════════════════════════════════════
#  Scout 데이터 (읽기 전용)
# ════════════════════════════════════════════
def scout_dir():
    return os.environ.get('SCOUT_DATA') or os.path.join(os.environ.get('APPDATA') or os.path.expanduser('~'), 'StockScout')


def scout_path():
    f = os.path.join(scout_dir(), 'scout.db')
    return f if os.path.exists(f) else ''


def etf_path():
    f = os.path.join(scout_dir(), 'etf.db')
    return f if os.path.exists(f) else ''


def _ro(path):
    return sqlite3.connect(f'file:{path}?mode=ro', uri=True, timeout=30)


def scout_last():
    p = scout_path()
    if not p:
        return ''
    s = _ro(p)
    try:
        return s.execute("SELECT MAX(date) FROM candles WHERE ticker IN ('005930','000660')").fetchone()[0] or ''
    finally:
        s.close()


def trading_days(frm='0', to='99999999'):
    p = scout_path()
    if not p:
        return []
    s = _ro(p)
    try:
        return [r[0] for r in s.execute("SELECT DISTINCT date FROM candles WHERE ticker IN ('005930','000660') AND date BETWEEN ? AND ? ORDER BY date",
                                        (frm, to))]
    finally:
        s.close()


def stocks():
    """{ticker: {name, market, excluded, warns}} — Scout 종목 정보 (ETF · 스팩 · 리츠 · 우선주는 excluded에 사유)"""
    p = scout_path()
    if not p:
        return {}
    s = _ro(p)
    try:
        cols = {r[1] for r in s.execute('PRAGMA table_info(stocks)')}
        extra = ', warns' if 'warns' in cols else ", '' AS warns"
        return {r[0]: {'name': r[1], 'market': r[2], 'excluded': r[3] or '', 'warns': r[4] or ''}
                for r in s.execute(f'SELECT ticker, name, market, excluded{extra} FROM stocks')}
    finally:
        s.close()


def panel(frm, to, tickers=None):
    """Scout 일봉 → {'open','high','low','close','volume': DataFrame(날짜 × 종목)} (거래 없는 날은 NaN)"""
    p = scout_path()
    if not p:
        return {}
    s = _ro(p)
    try:
        if tickers is None:
            df = pd.read_sql_query('SELECT ticker, date, open, high, low, close, volume FROM candles WHERE date BETWEEN ? AND ?', s, params=(frm, to))
        else:
            tk = sorted(set(tickers))
            parts = []
            for i in range(0, len(tk), 400):
                q = tk[i:i + 400]
                parts.append(pd.read_sql_query(f"SELECT ticker, date, open, high, low, close, volume FROM candles WHERE date BETWEEN ? AND ? "
                                               f"AND ticker IN ({','.join('?' * len(q))})", s, params=(frm, to, *q)))
            df = pd.concat(parts) if parts else pd.DataFrame(columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'volume'])
    finally:
        s.close()
    if df.empty:
        return {}
    df['ticker'] = df.ticker.astype(str).str.zfill(6)
    out = {}
    for k in ('open', 'high', 'low', 'close', 'volume'):
        w = df.pivot_table(index='date', columns='ticker', values=k, aggfunc='last').sort_index().astype(float)
        out[k] = w.sort_index(axis=1)
    for k in ('open', 'high', 'low', 'close'):                      # 0 · 음수 가격은 없는 값으로
        out[k] = out[k].where(out[k] > 0)
    return out


def flows(frm, to):
    """Scout flows(원 단위 · KRX) → {'외국인': DataFrame, '연기금': DataFrame, '기관합계': DataFrame} (날짜 × 종목)"""
    p = scout_path()
    if not p:
        return {}
    s = _ro(p)
    try:
        if not s.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='flows'").fetchone():
            return {}
        df = pd.read_sql_query("SELECT date, ticker, investor, amt FROM flows WHERE date BETWEEN ? AND ? AND investor IN ('외국인','연기금','기관합계')",
                               s, params=(frm, to))
    finally:
        s.close()
    out = {}
    if df.empty:
        return out
    df['ticker'] = df.ticker.astype(str).str.zfill(6)
    for inv, g in df.groupby('investor'):
        out[inv] = g.pivot_table(index='date', columns='ticker', values='amt', aggfunc='sum').sort_index().astype(float)
    return out


def flows_last():
    p = scout_path()
    if not p:
        return ''
    s = _ro(p)
    try:
        if not s.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='flows'").fetchone():
            return ''
        return s.execute("SELECT MAX(date) FROM flows WHERE investor='외국인'").fetchone()[0] or ''
    finally:
        s.close()


def dilutive_events(since):
    """{ticker: 최근 날짜} — Scout DART 희석성 공시 (유상증자 · CB · BW …) since 이후"""
    p = scout_path()
    if not p:
        return {}
    s = _ro(p)
    try:
        if not s.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events'").fetchone():
            return {}
        return {r[0]: r[1] for r in s.execute("""SELECT ticker, MAX(date) FROM events WHERE date>=? AND type IN
                 ('유상증자','CB','BW','EB','감자','회생절차','영업정지','부도') GROUP BY ticker""", (since,))}
    finally:
        s.close()


def etf_bars(ticker, frm='0', to='99999999'):
    """Scout etf.db (ETF_수집) → DataFrame(date index, open high low close) · 없으면 Scout 일봉에서 찾아봄"""
    p = etf_path()
    if p:
        s = _ro(p)
        try:
            df = pd.read_sql_query('SELECT date, o AS open, h AS high, l AS low, c AS close FROM etf WHERE ticker=? AND date BETWEEN ? AND ? ORDER BY date',
                                   s, params=(ticker, frm, to))
        finally:
            s.close()
        if len(df):
            return df.set_index('date').astype(float)
    pn = panel(frm, to, [ticker])
    if pn and ticker in pn['close']:
        return pd.DataFrame({k: pn[k][ticker] for k in ('open', 'high', 'low', 'close')}).dropna()
    return pd.DataFrame(columns=['open', 'high', 'low', 'close'])


# ════════════════════════════════════════════
#  구성 종목 · 월별 재무
# ════════════════════════════════════════════
SECTOR_MAP = {'반도체': '전기·전자', 'IT부품': '전기·전자', '통신장비': '전기·전자', '정보기기': '전기·전자', '소프트웨어': 'IT 서비스', '인터넷': 'IT 서비스',
              '디지털컨텐츠': 'IT 서비스', '컴퓨터서비스': 'IT 서비스', '통신서비스': '통신', '방송서비스': '오락·문화', '출판·매체복제': 'IT 서비스',
              '기타금융': '금융', '증권': '금융', '보험': '금융', '은행': '금융', '전기·가스·수도': '전기·가스'}


def _f(x):
    try:
        v = float(str(x).replace(',', ''))
        return None if v != v else v
    except (TypeError, ValueError):
        return None


def seed_dir():
    for d in SEED_DIRS:
        if os.path.exists(os.path.join(d, 'const.csv')):
            return d
    return ''


def seed_import():
    """처음 한 번: seed(2019-01 ~ 2026-09 KRX 월별 구성 종목 · 업종 · 재무) → members · monthly"""
    c = conn()
    if c.execute('SELECT COUNT(*) FROM members').fetchone()[0]:
        return 0
    d = seed_dir()
    if not d:
        log('구성 종목 초기 자료(seed) 없음 — 배당·가치 칸은 KRX 월 자료를 받은 뒤부터', 'warn')
        return 0
    rows = [(r['date'][:6], r['ticker'].zfill(6), r['index']) for r in csv.DictReader(open(os.path.join(d, 'const.csv'), encoding='utf-8-sig'))]
    c.executemany('INSERT OR IGNORE INTO members VALUES (?,?,?)', rows)
    fund = {(r['date'], r['ticker'].zfill(6)): r for r in csv.DictReader(open(os.path.join(d, 'fund.csv'), encoding='utf-8-sig'))}
    out = []
    for r in csv.DictReader(open(os.path.join(d, 'sector.csv'), encoding='utf-8-sig')):
        tk = r['ticker'].zfill(6)
        fu = fund.get((r['date'], tk), {})
        out.append((r['date'][:6], r['date'], tk, r['market'], r['name'], SECTOR_MAP.get(r['sector'], r['sector']), _f(r['marcap']),
                    _f(fu.get('EPS')), _f(fu.get('DIV')), _f(fu.get('PBR'))))
    c.executemany('INSERT OR IGNORE INTO monthly VALUES (?,?,?,?,?,?,?,?,?,?)', out)
    c.commit()
    log(f'초기 자료 넣음: 구성 종목 {len(rows):,} · 월별 재무 {len(out):,} ({d})')
    return len(rows)


def krx_month(d, cfg):
    """KRX에서 d(그달 첫 거래일) 구성 종목 · 업종 · 재무를 받아 저장 — ⚙️ KRX 계정 필요"""
    import contextlib
    import io
    import sys
    import time
    kid, kpw = cfg.get('krx_id'), cfg.get('krx_pw')
    if not (kid and kpw):
        raise RuntimeError('KRX 계정 없음 (⚙️ 설정)')
    os.environ['KRX_ID'], os.environ['KRX_PW'] = kid, kpw
    for k in [k for k in sys.modules if k.startswith('pykrx')]:
        del sys.modules[k]
    cap = io.StringIO()
    with contextlib.redirect_stdout(cap):
        from pykrx import stock
    m = d[:6]
    mem = []
    for code, nm in (('1028', '코스피200'), ('2203', '코스닥150')):
        with contextlib.redirect_stdout(cap):
            t = stock.get_index_portfolio_deposit_file(code, d)
        time.sleep(1)
        mem += [(m, x, nm) for x in t]
    if len(mem) < 300:
        raise RuntimeError(f'KRX 구성 종목 응답 부족 ({len(mem)})')
    rows = []
    for mk in ('KOSPI', 'KOSDAQ'):
        with contextlib.redirect_stdout(cap):
            s = stock.get_market_sector_classifications(d, mk)
        time.sleep(1)
        with contextlib.redirect_stdout(cap):
            f = stock.get_market_fundamental_by_ticker(d, mk)
        time.sleep(1)
        for tk, r in s.iterrows():
            fu = f.loc[tk] if tk in f.index else {}
            g = (lambda k: _f(fu.get(k)) if len(fu) else None)
            rows.append((m, d, tk, mk, r['종목명'], SECTOR_MAP.get(r['업종명'], r['업종명']), _f(r['시가총액']), g('EPS'), g('DIV'), g('PBR')))
    c = conn()
    c.execute('DELETE FROM members WHERE month=?', (m,))
    c.executemany('INSERT OR REPLACE INTO members VALUES (?,?,?)', mem)
    c.executemany('INSERT OR REPLACE INTO monthly VALUES (?,?,?,?,?,?,?,?,?,?)', rows)
    c.commit()
    log(f'KRX {d} 월 자료: 구성 종목 {len(mem)} · 스냅샷 {len(rows)}')


def month_tables(upto=None):
    """→ (members {month: set}, monthly DataFrame) — 배당·가치 칸 계산용"""
    c = conn()
    q = 'SELECT month, ticker FROM members' + (' WHERE month<=?' if upto else '')
    mem = {}
    for m, t in c.execute(q, (upto,) if upto else ()):
        mem.setdefault(m, set()).add(t)
    mon = pd.read_sql_query('SELECT month, ticker, name, sector, eps, div, pbr FROM monthly' + (' WHERE month<=?' if upto else ''), c,
                            params=(upto,) if upto else None)
    return mem, mon
