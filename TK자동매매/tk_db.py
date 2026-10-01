"""
tk_db.py — TK자동매매 저장소 (다른 앱에 의존하지 않음)

데이터 폴더 %APPDATA%\\TKAuto (TKAUTO_DATA 환경변수로 바꿀 수 있음)
· market.db  시장 자료 — 종목(상장 + 상장폐지) · 일봉(원주가 + KRX 등락률) · 투자자별 순매수 · ETF · 구성 종목 · 월별 재무 · 로그
· trade_paper.db / trade_real.db  모드별 장부 — 묶음(lot) · 주문 · 평가 · 신호 · 설정 상태 (모의와 실전 장부는 절대 섞이지 않음)
수정주가: KRX 등락률은 권리락 · 액면분할 뒤의 '기준가' 대비라서, 원주가와 등락률로 전날 기준가를 거꾸로 구해 조정 비율을 만든다
"""
import os
import sqlite3
import threading
from datetime import datetime

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get('TKAUTO_DATA') or os.path.join(os.environ.get('APPDATA') or os.path.expanduser('~'), 'TKAuto')
os.makedirs(DATA_DIR, exist_ok=True)
MARKET_DB = os.path.join(DATA_DIR, 'market.db')

MARKET_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS stocks (ticker TEXT PRIMARY KEY, name TEXT, market TEXT, listed INTEGER DEFAULT 1, excluded TEXT DEFAULT '',
    halt INTEGER DEFAULT 0, admin INTEGER DEFAULT 0, warn TEXT DEFAULT '', updated TEXT);
CREATE TABLE IF NOT EXISTS bars (date TEXT, ticker TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL, value REAL, chg REAL,
    src TEXT, PRIMARY KEY (date, ticker));
CREATE INDEX IF NOT EXISTS ix_bars_tk ON bars(ticker, date);
CREATE TABLE IF NOT EXISTS flows (date TEXT, ticker TEXT, investor TEXT, amt REAL, PRIMARY KEY (date, ticker, investor));
CREATE INDEX IF NOT EXISTS ix_flows ON flows(investor, date);
CREATE TABLE IF NOT EXISTS etf (date TEXT, ticker TEXT, open REAL, high REAL, low REAL, close REAL, PRIMARY KEY (date, ticker));
CREATE TABLE IF NOT EXISTS members (month TEXT, ticker TEXT, idx TEXT, PRIMARY KEY (month, ticker));
CREATE TABLE IF NOT EXISTS monthly (month TEXT, date TEXT, ticker TEXT, market TEXT, name TEXT, sector TEXT, marcap REAL,
    eps REAL, div REAL, pbr REAL, PRIMARY KEY (month, ticker));
CREATE TABLE IF NOT EXISTS done (kind TEXT, key TEXT, n INTEGER, at TEXT, PRIMARY KEY (kind, key));
CREATE TABLE IF NOT EXISTS log (id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, mode TEXT, level TEXT, msg TEXT);
CREATE TABLE IF NOT EXISTS cands (date TEXT, sleeve TEXT, ticker TEXT, rank INTEGER, score REAL, close REAL, info TEXT, src TEXT,
    PRIMARY KEY (date, sleeve, ticker));
CREATE INDEX IF NOT EXISTS ix_cands_tk ON cands(ticker, date);
"""
TRADE_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
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
CREATE TABLE IF NOT EXISTS intraday (ts TEXT PRIMARY KEY, value REAL);
CREATE TABLE IF NOT EXISTS sleeve_daily (date TEXT, sleeve TEXT, invested REAL, value REAL, realized REAL, npos INTEGER,
    PRIMARY KEY (date, sleeve));
CREATE TABLE IF NOT EXISTS days (date TEXT PRIMARY KEY, done_at TEXT, note TEXT);
-- ── 거래 기록 (분석 · 고도화용 · 지우지 않음) ──
CREATE TABLE IF NOT EXISTS fills (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, ts TEXT, order_id INTEGER, order_no TEXT, lot_id INTEGER,
    sleeve TEXT, ticker TEXT, name TEXT, side TEXT, kind TEXT, qty INTEGER, price REAL, amount REAL, fee REAL, tax REAL, src TEXT);
CREATE INDEX IF NOT EXISTS ix_fills ON fills(date, ticker);
CREATE TABLE IF NOT EXISTS ws_execs (ts TEXT, exec_time TEXT, order_no TEXT, ticker TEXT, side TEXT, qty INTEGER, price REAL,
    PRIMARY KEY (order_no, exec_time, qty, price));
CREATE TABLE IF NOT EXISTS order_events (id INTEGER PRIMARY KEY AUTOINCREMENT, order_id INTEGER, ts TEXT, status TEXT, detail TEXT);
CREATE INDEX IF NOT EXISTS ix_oev ON order_events(order_id);
CREATE TABLE IF NOT EXISTS decisions (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, ts TEXT, sig_date TEXT, sleeve TEXT, ticker TEXT,
    name TEXT, rank INTEGER, score REAL, ref REAL, qty INTEGER, amt REAL, action TEXT, reason TEXT);
CREATE INDEX IF NOT EXISTS ix_dec ON decisions(date);
CREATE TABLE IF NOT EXISTS positions_daily (date TEXT, ticker TEXT, name TEXT, qty INTEGER, avg REAL, price REAL, value REAL, pnl REAL,
    PRIMARY KEY (date, ticker));
CREATE TABLE IF NOT EXISTS account_daily (date TEXT PRIMARY KEY, cash REAL, cash_d2 REAL, equity REAL, stock_value REAL, buy_amt REAL,
    sell_amt REAL, fee REAL, tax REAL, realized REAL, n_buy INTEGER, n_sell INTEGER, flow REAL, note TEXT);
CREATE TABLE IF NOT EXISTS broker_pnl (date TEXT, ticker TEXT, name TEXT, kind TEXT, buy_qty REAL, buy_amt REAL, sell_qty REAL,
    sell_amt REAL, pnl REAL, fee REAL, tax REAL, PRIMARY KEY (date, ticker, kind));
CREATE TABLE IF NOT EXISTS api_daily (date TEXT, tr TEXT, n INTEGER DEFAULT 0, err INTEGER DEFAULT 0, ms REAL DEFAULT 0, last_err TEXT,
    PRIMARY KEY (date, tr));
"""
# 예전 장부에 없던 열 (켤 때 자동 추가)
TRADE_COLUMNS = {
    'lots': [('sig_rank', 'INTEGER'), ('sig_score', 'REAL'), ('sig_ref', 'REAL'), ('entry_info', 'TEXT'), ('fee', 'REAL DEFAULT 0'),
             ('tax', 'REAL DEFAULT 0'), ('mae', 'REAL'), ('mfe', 'REAL'), ('model_ret', 'REAL'), ('slip_in', 'REAL'), ('slip_out', 'REAL'),
             ('gap_in', 'REAL'), ('entry_ts', 'TEXT'), ('exit_ts', 'TEXT'), ('exit_kind', 'TEXT'), ('post_at', 'TEXT')],
    'orders': [('sig_ref', 'REAL'), ('ack_ts', 'TEXT'), ('fill_ts', 'TEXT'), ('msg_cd', 'TEXT'), ('ord_time', 'TEXT'), ('resv_seq', 'TEXT')],
}
_local = threading.local()
MODE = ['paper']                                   # 지금 장부 모드 (tk_server가 설정에서 정함)


def set_mode(m):
    if m not in ('paper', 'real'):
        raise ValueError(m)
    MODE[0] = m


def mode():
    return MODE[0]


def _open(path, schema):
    c = sqlite3.connect(path, timeout=30, check_same_thread=False)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL')
    c.executescript(schema)
    c.commit()
    return c


def mconn():
    c = getattr(_local, 'm', None)
    if c is None:
        c = _local.m = _open(MARKET_DB, MARKET_SCHEMA)
    return c


def conn(m=None):
    """모드별 장부 DB (기본: 지금 모드)"""
    m = m or MODE[0]
    d = getattr(_local, 't', None)
    if d is None:
        d = _local.t = {}
    if m not in d:
        c = _open(os.path.join(DATA_DIR, f'trade_{m}.db'), TRADE_SCHEMA)
        for t, cols in TRADE_COLUMNS.items():
            have = {r[1] for r in c.execute(f'PRAGMA table_info({t})')}
            for col, typ in cols:
                if col not in have:
                    c.execute(f'ALTER TABLE {t} ADD COLUMN {col} {typ}')
        c.commit()
        d[m] = c
    return d[m]


def meta_get(k, default='', m=None):
    r = conn(m).execute('SELECT v FROM meta WHERE k=?', (k,)).fetchone()
    return r[0] if r else default


def meta_set(k, v, m=None):
    c = conn(m)
    c.execute('INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)', (k, str(v)))
    c.commit()


def gmeta_get(k, default=''):
    r = mconn().execute('SELECT v FROM meta WHERE k=?', (k,)).fetchone()
    return r[0] if r else default


def gmeta_set(k, v):
    c = mconn()
    c.execute('INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)', (k, str(v)))
    c.commit()


def backup(keep=30, market_every=7):
    """장부 DB 날마다 백업 (backups/ · 30개 보관) · 시장 DB(신호 후보 포함)는 7일마다 (4개 보관) — 실행 중에도 안전한 SQLite 백업"""
    import glob
    bd = os.path.join(DATA_DIR, 'backups')
    os.makedirs(bd, exist_ok=True)
    day = datetime.now().strftime('%Y%m%d')
    done = []
    for name in ('trade_paper', 'trade_real') + (('market',) if datetime.now().toordinal() % market_every == 0 else ()):
        src = os.path.join(DATA_DIR, f'{name}.db')
        if not os.path.exists(src):
            continue
        dst = os.path.join(bd, f'{name}_{day}.db')
        a = sqlite3.connect(src, timeout=60)
        b = sqlite3.connect(dst)
        try:
            a.backup(b)
        finally:
            b.close()
            a.close()
        done.append(os.path.basename(dst))
        olds = sorted(glob.glob(os.path.join(bd, f'{name}_*.db')))
        for f in olds[:-(4 if name == 'market' else keep)]:
            try:
                os.remove(f)
            except OSError:
                pass
    return done


def now_s():
    return datetime.now().isoformat(timespec='seconds')


SECRETS = []                                         # 로그에 절대 남기지 않을 값 (tk_config가 채움)


def log(msg, level='info'):
    msg = str(msg)
    for s in SECRETS:
        if s and len(s) > 3:
            msg = msg.replace(s, '●●●●')
    line = f"[{datetime.now():%m-%d %H:%M:%S}] {'[실전] ' if MODE[0] == 'real' else ''}{msg}"
    print(line, flush=True)
    try:
        c = mconn()
        c.execute('INSERT INTO log (ts, mode, level, msg) VALUES (?,?,?,?)', (now_s(), MODE[0], level, msg[:800]))
        c.commit()
    except Exception:
        pass


# ════════════════════════════════════════════
#  시장 자료 읽기
# ════════════════════════════════════════════
def trading_days(frm='0', to='99999999'):
    return [r[0] for r in mconn().execute("SELECT key FROM done WHERE kind='bars' AND n>0 AND key BETWEEN ? AND ? ORDER BY key", (frm, to))] \
        or [r[0] for r in mconn().execute('SELECT DISTINCT date FROM bars WHERE date BETWEEN ? AND ? ORDER BY date', (frm, to))]


def last_bar_day():
    r = mconn().execute("SELECT MAX(key) FROM done WHERE kind='bars' AND n>0").fetchone()[0]
    return r or (mconn().execute('SELECT MAX(date) FROM bars').fetchone()[0] or '')


def last_flow_day():
    return mconn().execute("SELECT MAX(key) FROM done WHERE kind='flow_외국인' AND n>0").fetchone()[0] or ''


def stocks():
    return {r['ticker']: dict(r) for r in mconn().execute('SELECT * FROM stocks')}


def adjust_factors(C, CHG):
    """원주가 C · KRX 등락률 CHG(%) → 날짜별 누적 조정 비율 (그 날 이전 가격에 곱함)
       전날 기준가 = 오늘 종가 ÷ (1 + 등락률) · 전날 원종가와 2% 넘게 다르면 권리락 · 분할 등 → 그 비율로 과거를 조정"""
    prev = C.ffill().shift(1)
    base = C / (1 + CHG / 100)
    r = (base / prev).where(CHG.notna() & prev.notna() & C.notna())
    ev = r.where((r - 1).abs() > 0.02).fillna(1.0).clip(0.02, 50)
    f = ev.iloc[::-1].cumprod().iloc[::-1].shift(-1).fillna(1.0)
    return f


def panel(frm, to, tickers=None, adjusted=True):
    """일봉 → {'open','high','low','close','volume','value': DataFrame(날짜 × 종목)} · adjusted면 수정주가"""
    c = mconn()
    q = 'SELECT ticker, date, open, high, low, close, volume, value, chg FROM bars WHERE date BETWEEN ? AND ?'
    if tickers is None:
        df = pd.read_sql_query(q, c, params=(frm, to))
    else:
        tk = sorted(set(tickers))
        parts = []
        for i in range(0, len(tk), 400):
            p_ = tk[i:i + 400]
            parts.append(pd.read_sql_query(q + f" AND ticker IN ({','.join('?' * len(p_))})", c, params=(frm, to, *p_)))
        df = pd.concat(parts) if parts else pd.DataFrame(columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'value', 'chg'])
    if df.empty:
        return {}
    out = {k: df.pivot_table(index='date', columns='ticker', values=k, aggfunc='last').sort_index().sort_index(axis=1).astype(float)
           for k in ('open', 'high', 'low', 'close', 'volume', 'value', 'chg')}
    for k in ('open', 'high', 'low', 'close'):
        out[k] = out[k].where(out[k] > 0)
    out['value'] = out['value'].where(out['value'] > 0, out['close'] * out['volume'])
    if adjusted:
        f = adjust_factors(out['close'], out['chg'])
        for k in ('open', 'high', 'low', 'close'):
            out[k] = out[k] * f
        out['volume'] = out['volume'] / f
    return out


def flows(frm, to):
    df = pd.read_sql_query("SELECT date, ticker, investor, amt FROM flows WHERE date BETWEEN ? AND ?", mconn(), params=(frm, to))
    return {inv: g.pivot_table(index='date', columns='ticker', values='amt', aggfunc='sum').sort_index().astype(float)
            for inv, g in df.groupby('investor')} if len(df) else {}


def etf_bars(ticker, frm='0', to='99999999'):
    df = pd.read_sql_query('SELECT date, open, high, low, close FROM etf WHERE ticker=? AND date BETWEEN ? AND ? ORDER BY date',
                           mconn(), params=(ticker, frm, to))
    return df.set_index('date').astype(float) if len(df) else pd.DataFrame(columns=['open', 'high', 'low', 'close'])


def members_of(idx):
    """{월: {종목}} — 한 지수(코스피200 · 코스닥150)만"""
    out = {}
    for m, t in mconn().execute('SELECT month, ticker FROM members WHERE idx=?', (idx,)):
        out.setdefault(m, set()).add(t)
    return out


def month_caps():
    """{월: {종목: 시가총액}}"""
    out = {}
    for m, t, v in mconn().execute('SELECT month, ticker, marcap FROM monthly WHERE marcap>0'):
        out.setdefault(m, {})[t] = v
    return out


def month_tables(upto=None):
    c = mconn()
    mem = {}
    for m, t in c.execute('SELECT month, ticker FROM members' + (' WHERE month<=?' if upto else ''), (upto,) if upto else ()):
        mem.setdefault(m, set()).add(t)
    mon = pd.read_sql_query('SELECT month, ticker, name, sector, eps, div, pbr FROM monthly' + (' WHERE month<=?' if upto else ''), c,
                            params=(upto,) if upto else None)
    return mem, mon
