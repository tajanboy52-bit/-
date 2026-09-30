"""
danta_db.py — 台炅 자동단타매매 (TK Danta) 데이터 저장소

※ 가상추천매매 시스템(Scout · 8082)과 완전히 별개. 데이터 폴더도 따로:
   윈도우: C:\\Users\\<사용자>\\AppData\\Roaming\\TKDanta   (Scout는 ...\\StockScout)
역할: 장중 전략 검증에 쓸 1분봉을 날마다 쌓는다 (+ danta_live.py의 가상 단타 기록)
  · 수집 대상 = 그날 기준 '직전 20거래일 평균 거래대금' 상위 N종목 (날짜마다 따로 — 오늘 잘나가는 종목으로 과거를 고르면 생존 편향)
              + 그날 장중 +3% 이상 올랐던 종목(급등주 · D0.2) + 가상 단타 탐지 후보
  · 거래대금 순위는 같은 PC의 Scout 일봉을 '읽기 전용'으로 사용 (Scout를 바꾸지 않음) · 없으면 KIS 거래대금 순위 30종목
"""
import os
import sqlite3
import threading
from datetime import datetime

DATA_DIR = os.environ.get('DANTA_DATA') or os.path.join(os.environ.get('APPDATA') or os.path.expanduser('~'), 'TKDanta')
os.makedirs(DATA_DIR, exist_ok=True)
DB_FILE = os.path.join(DATA_DIR, 'danta.db')

SCHEMA = """
CREATE TABLE IF NOT EXISTS bars (             -- 1분봉 (KRX 정규장)
    ticker TEXT, date TEXT, hm INTEGER,          -- hm: 0901 = 09:01 봉 (그 1분 동안의 체결)
    open REAL, high REAL, low REAL, close REAL, vol INTEGER, amt REAL,
    PRIMARY KEY (ticker, date, hm)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS universe (          -- 날짜별 수집 대상 (그날 기준 순위)
    date TEXT, ticker TEXT, name TEXT, market TEXT, rank INTEGER, avg_value REAL, src TEXT,
    PRIMARY KEY (date, ticker));
CREATE TABLE IF NOT EXISTS done (              -- (종목, 날짜) 수집 완료 표시 — 이어받기용
    ticker TEXT, date TEXT, n INTEGER, ts TEXT, PRIMARY KEY (ticker, date));
CREATE TABLE IF NOT EXISTS runs (              -- 작업 기록
    id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT, started TEXT, ended TEXT, status TEXT, msg TEXT);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
"""
_local = threading.local()
ETF_KW = ['KODEX', 'TIGER', 'KBSTAR', 'HANARO', 'KOSEF', 'ARIRANG', 'SOL ', 'ACE ', 'RISE ', 'PLUS ', 'TIMEFOLIO',
          'FOCUS', 'ETN', '레버리지', '인버스', '선물', '채권', '국채', 'ETF']
BAD_KW = ['스팩', '리츠', '제1호', '제2호', '제3호', '기업인수목적']


def conn():
    c = getattr(_local, 'c', None)
    if c is None:
        c = sqlite3.connect(DB_FILE, timeout=30, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute('PRAGMA journal_mode=WAL')
        c.execute('PRAGMA synchronous=NORMAL')
        _local.c = c
    return c


def init():
    conn().executescript(SCHEMA)
    conn().commit()


def meta_get(k, default=''):
    r = conn().execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    return r[0] if r else default


def meta_set(k, v):
    conn().execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (k, str(v)))
    conn().commit()


def excluded(ticker, name):
    """ETF/ETN · 스팩 · 리츠 · 우선주 제외 (단타 대상은 보통주만)"""
    up = (name or '').upper()
    if any(k in up for k in ETF_KW) or any(k in (name or '') for k in BAD_KW):
        return True
    if (name or '').endswith(('우', '우B', '우C')) or not str(ticker).endswith('0'):
        return True
    return False


# ════════════════════════════════════════════
#  수집 대상 (날짜별 · 그날 기준)
# ════════════════════════════════════════════
def scout_db_path():
    p = os.environ.get('SCOUT_DATA') or os.path.join(os.environ.get('APPDATA') or os.path.expanduser('~'), 'StockScout')
    f = os.path.join(p, 'scout.db')
    return f if os.path.exists(f) else ''


def universe_from_scout(dates, top_n):
    """Scout 일봉(읽기 전용)으로 날짜마다 직전 20거래일 평균 거래대금 상위 top_n → {date: [(ticker, name, market, avg)]}
       날짜마다 그날 이전 자료만 씀 (미래 정보 없음)"""
    import bisect
    f = scout_db_path()
    if not f or not dates:
        return {}
    s = sqlite3.connect(f'file:{f}?mode=ro', uri=True, timeout=30)
    try:
        names = {r[0]: (r[1], r[2]) for r in s.execute("SELECT ticker, name, market FROM stocks")}
        all_dates = [r[0] for r in s.execute("SELECT DISTINCT date FROM candles ORDER BY date")]
        lo_i = max(0, bisect.bisect_left(all_dates, min(dates)) - 25)
        since = all_dates[lo_i] if all_dates else '0'
        series = {}
        for tk, d, cl, vol in s.execute("SELECT ticker, date, close, volume FROM candles WHERE date>=? ORDER BY ticker, date", (since,)):
            a = series.setdefault(tk, ([], []))
            a[0].append(d)
            a[1].append((cl or 0) * (vol or 0))
        ok = {tk for tk in series if tk in names and names[tk][0] and not excluded(tk, names[tk][0])}
        out = {}
        for d in dates:
            cand = []
            for tk in ok:
                ds, vs = series[tk]
                j = bisect.bisect_left(ds, d)
                w = vs[max(0, j - 20):j]
                if len(w) < 15:
                    continue
                cand.append((tk, names[tk][0], names[tk][1], sum(w) / len(w)))
            if cand:
                cand.sort(key=lambda x: -x[3])
                out[d] = cand[:top_n]
        return out
    finally:
        s.close()


def surgers_from_scout(dates, up=3.0, min_value=5e8, cap=300):
    """그날 장중에 한 번이라도 +up% 이상 올랐거나 −5% 이상 밀린 종목 (거래대금 min_value 이상) → {date: [...]}
       (D0.5: 급락도 포함 — M6 장중 급락 매수 모델 검증용)
       급등주 단타 검증용 분봉 수집 대상 — 탐지 필터(등락률 3% 이상)를 한 번이라도 통과할 수 있었던 종목을 빠짐없이 담기 위함.
       ※ 수집 대상 선정에만 쓰고, 전략 검증은 분봉으로 '그 분에 알 수 있던 정보'만 씀 (미래 정보 없음)
       Scout에 그날 일봉이 있는 날짜만 결과에 들어감 (빈 목록이어도)"""
    f = scout_db_path()
    if not f or not dates:
        return {}
    s = sqlite3.connect(f'file:{f}?mode=ro', uri=True, timeout=30)
    try:
        names = {r[0]: (r[1], r[2]) for r in s.execute("SELECT ticker, name, market FROM stocks")}
        have = {r[0] for r in s.execute(f"SELECT DISTINCT date FROM candles WHERE date IN ({','.join('?' * len(dates))})", list(dates))}
        out = {}
        for d in sorted(have):
            prev = s.execute("SELECT MAX(date) FROM candles WHERE date<?", (d,)).fetchone()[0]
            if not prev:
                continue
            rows = s.execute(
                "SELECT c.ticker, c.close*c.volume AS val FROM candles c JOIN candles p ON p.ticker=c.ticker AND p.date=? "
                "WHERE c.date=? AND p.close>0 AND (c.high>=p.close*? OR c.low<=p.close*0.95) AND c.close*c.volume>=? ORDER BY val DESC",
                (prev, d, 1 + up / 100, min_value)).fetchall()
            items = []
            for tk, val in rows:
                nm = names.get(tk, ('', ''))
                if nm[0] and not excluded(tk, nm[0]):
                    items.append((tk, nm[0], nm[1], val))
                if len(items) >= cap:
                    break
            out[d] = items
        return out
    finally:
        s.close()


def prev_info(tickers, d):
    """Scout 일봉(읽기 전용)으로 d 전 거래일의 등락률 · 거래량 → {ticker: (prev_chg, prev_vol)}"""
    f = scout_db_path()
    if not f or not tickers:
        return {}
    s = sqlite3.connect(f'file:{f}?mode=ro', uri=True, timeout=30)
    out = {}
    try:
        for tk in tickers:
            r = s.execute("SELECT close, volume FROM candles WHERE ticker=? AND date<? ORDER BY date DESC LIMIT 2", (tk, d)).fetchall()
            if len(r) == 2 and r[1][0]:
                out[tk] = ((r[0][0] / r[1][0] - 1) * 100, r[0][1])
            elif len(r) == 1:
                out[tk] = (None, r[0][1])
    finally:
        s.close()
    return out


def add_universe(d, items, src):
    """기존 수집 대상에 없는 종목만 뒤에 추가 (급등 · 가상매매 종목) → 추가한 수"""
    c = conn()
    have = {r[0] for r in c.execute("SELECT ticker FROM universe WHERE date=?", (d,))}
    rk = c.execute("SELECT COALESCE(MAX(rank),0) FROM universe WHERE date=?", (d,)).fetchone()[0]
    new = []
    for tk, nm, mk, av in items:
        if tk in have:
            continue
        have.add(tk)
        rk += 1
        new.append((d, tk, nm, mk, rk, av, src))
    c.executemany("INSERT INTO universe VALUES(?,?,?,?,?,?,?)", new)
    c.commit()
    return len(new)


def save_universe(d, items, src):
    c = conn()
    c.execute("DELETE FROM universe WHERE date=?", (d,))
    c.executemany("INSERT INTO universe VALUES(?,?,?,?,?,?,?)",
                  [(d, tk, nm, mk, i + 1, av, src) for i, (tk, nm, mk, av) in enumerate(items)])
    c.commit()


# ════════════════════════════════════════════
#  분봉 저장 · 현황
# ════════════════════════════════════════════
def save_bars(ticker, d, bars):
    """bars: [(hm, o, h, l, c, vol, amt)] — 같은 날 다시 받으면 덮어씀"""
    c = conn()
    c.executemany("INSERT OR REPLACE INTO bars VALUES(?,?,?,?,?,?,?,?,?)", [(ticker, d, *b) for b in bars])
    c.execute("INSERT OR REPLACE INTO done VALUES(?,?,?,?)", (ticker, d, len(bars), datetime.now().isoformat(timespec='seconds')))
    c.commit()


def is_done(ticker, d, min_bars=300):
    """한 번 받은 과거 날짜는 다시 안 받음 (지난 분봉은 바뀌지 않음 · 거래정지 등으로 봉이 적거나 없어도) · 오늘은 300봉 미만이면 다시"""
    r = conn().execute("SELECT n FROM done WHERE ticker=? AND date=?", (ticker, d)).fetchone()
    if not r:
        return False
    return (r[0] or 0) >= min_bars or d < datetime.now().strftime('%Y%m%d')


def status():
    c = conn()
    r = c.execute("SELECT COUNT(*), MIN(date), MAX(date), COUNT(DISTINCT date) FROM done WHERE n>0").fetchone()
    per_day = [dict(x) for x in c.execute(
        "SELECT u.date, COUNT(*) AS want, SUM(CASE WHEN d.n>=300 THEN 1 ELSE 0 END) AS got, "
        "SUM(CASE WHEN u.src IN ('surge','live') THEN 1 ELSE 0 END) AS surge, "
        "SUM(CASE WHEN d.n>0 AND d.n<300 THEN 1 ELSE 0 END) AS part "
        "FROM universe u LEFT JOIN done d ON d.ticker=u.ticker AND d.date=u.date GROUP BY u.date ORDER BY u.date DESC LIMIT 400")]
    size = sum(os.path.getsize(os.path.join(DATA_DIR, f)) for f in os.listdir(DATA_DIR) if f.startswith('danta.db'))
    return {'pairs': r[0], 'first': r[1], 'last': r[2], 'days': r[3], 'per_day': per_day, 'db_mb': round(size / 1e6, 1)}


def log_run(kind, status_, msg, started):
    conn().execute("INSERT INTO runs (kind, started, ended, status, msg) VALUES(?,?,?,?,?)",
                   (kind, started, datetime.now().isoformat(timespec='seconds'), status_, msg[:500]))
    conn().commit()
