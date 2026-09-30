"""
scout_db.py — Stock Scout 데이터 레이어
============================================
· KIS 마스터파일로 전종목 목록 구축
· 일봉 250일 / 수급 20일 SQLite 캐시
· 후보풀(거래대금·유동성) 선별
· 장마감 후 증분 동기화

자동매매 서버(8080)와 완전 독립. 별도 토큰 사용.
"""
import os, io, json, time, zipfile, sqlite3, threading, urllib.request, urllib.parse
from datetime import datetime, timedelta

# ── 데이터 폴더: 프로그램 파일을 교체·이동해도 설정과 DB가 남도록 프로그램 폴더 밖에 저장
#    윈도우: C:\Users\<사용자>\AppData\Roaming\StockScout
PROGRAM_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get('SCOUT_DATA') or os.path.join(
    os.environ.get('APPDATA') or os.path.expanduser('~'), 'StockScout')
os.makedirs(DATA_DIR, exist_ok=True)


def _find_old(name):
    """예전 버전이 남긴 파일 찾기 — 프로그램 폴더 우선, 없으면 흔히 쓰는 폴더를 얕게 탐색"""
    cand = [os.path.join(PROGRAM_DIR, name)]
    home = os.path.expanduser('~')
    roots = [os.path.join(home, d) for d in ('Downloads', 'Desktop', 'Documents', '다운로드', '바탕 화면', '문서')]
    roots += [os.path.join(home, 'OneDrive', d) for d in ('Desktop', 'Documents', '바탕 화면', '문서')]
    roots += ['C:\\Scout', 'C:\\StockScout', 'D:\\Scout', 'D:\\StockScout', home]
    for r in roots:
        if not os.path.isdir(r):
            continue
        cand.append(os.path.join(r, name))
        try:
            for sub in os.listdir(r):                   # 한 단계 하위 폴더까지만
                sp = os.path.join(r, sub)
                if os.path.isdir(sp):
                    cand.append(os.path.join(sp, name))
        except OSError:
            pass
    found = [c for c in cand if os.path.isfile(c) and os.path.dirname(c) != DATA_DIR]
    # 여러 개면 가장 최근에 쓴 것
    return max(found, key=os.path.getmtime) if found else None


def data_file(name):
    """데이터 폴더의 파일 경로. 예전 버전이 다른 폴더에 남긴 파일이 있으면 자동으로 가져옴."""
    import shutil
    new = os.path.join(DATA_DIR, name)
    old = _find_old(name) if not os.path.exists(new) else None
    if old:
        if name.endswith('.db'):
            try:                                   # WAL 내용을 본 파일에 합친 뒤 이동
                c = sqlite3.connect(old)
                c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                c.close()
            except Exception:
                pass
        # 프로그램 폴더면 이동, 다른 폴더에서 찾은 거면 복사 (원본은 남겨둠)
        if os.path.dirname(old) == PROGRAM_DIR:
            shutil.move(old, new)
            for ext in ('-wal', '-shm'):
                try:
                    os.remove(old + ext)
                except OSError:
                    pass
        else:
            shutil.copy2(old, new)
        print(f"[데이터] 예전 {name} 발견 → {DATA_DIR} 로 가져옴 ({old})", flush=True)
    return new


DB_PATH = os.environ.get('SCOUT_DB') or data_file('scout.db')
KIS_URL = "https://openapi.koreainvestment.com:9443"
MASTER_URL = "https://new.real.download.dws.co.kr/common/master"

# ── KIS 유량 제한 (실계좌 초당 20건 → 안전하게 8건)
REQ_PER_SEC = float(os.environ.get('SCOUT_RPS', '8'))
_rate_lock = threading.Lock()
_last_req = [0.0]


def _throttle():
    with _rate_lock:
        gap = 1.0 / REQ_PER_SEC
        wait = _last_req[0] + gap - time.time()
        if wait > 0:
            time.sleep(wait)
        _last_req[0] = time.time()


# ════════════════════════════════════════════
#  스키마
# ════════════════════════════════════════════
SCHEMA = """
CREATE TABLE IF NOT EXISTS stocks (
    ticker TEXT PRIMARY KEY,
    name TEXT,
    market TEXT,              -- KOSPI / KOSDAQ
    in_pool INTEGER DEFAULT 0,-- 후보풀 포함 여부
    mktcap REAL DEFAULT 0,    -- 시가총액(억원)
    avg_value REAL DEFAULT 0, -- 20일 평균 거래대금(원)
    excluded TEXT DEFAULT '', -- 제외 사유
    updated TEXT
);
CREATE TABLE IF NOT EXISTS candles (
    ticker TEXT, date TEXT,
    open REAL, high REAL, low REAL, close REAL, volume INTEGER,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_candles_ticker ON candles(ticker, date);
CREATE TABLE IF NOT EXISTS investors (
    ticker TEXT, date TEXT,
    foreign_qty INTEGER, inst_qty INTEGER,
    foreign_amt INTEGER, inst_amt INTEGER,
    PRIMARY KEY (ticker, date)
);
CREATE TABLE IF NOT EXISTS scans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT, horizon TEXT, ticker TEXT, name TEXT,
    score REAL, strategy TEXT, entry REAL, stop REAL, target1 REAL, target2 REAL,
    payload TEXT
);
CREATE INDEX IF NOT EXISTS idx_scans_ts ON scans(ts);
CREATE TABLE IF NOT EXISTS outcomes (
    scan_id INTEGER PRIMARY KEY,
    d1 REAL, d3 REAL, d5 REAL, d20 REAL, checked TEXT
);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS tracking (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_id INTEGER, ticker TEXT, name TEXT, strategy TEXT, horizon TEXT,
    created TEXT, buy_type TEXT,
    entry REAL, stop REAL, target1 REAL, target2 REAL,
    valid_until TEXT, hold_max INTEGER, time_stop INTEGER,
    status TEXT,                 -- 대기/진입/1차도달/손절/시간손절/2차도달/본전청산/기간만료/신호소멸
    entry_date TEXT, entry_price REAL, cur_stop REAL,
    last_price REAL, last_check TEXT,
    alerts TEXT DEFAULT '[]',
    closed INTEGER DEFAULT 0, result_pct REAL
);
CREATE INDEX IF NOT EXISTS idx_tracking_open ON tracking(closed, ticker);
CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT, name TEXT, strategy TEXT, horizon TEXT,
    buy_date TEXT, buy_price REAL, qty INTEGER, remain_qty INTEGER,
    stop REAL, cur_stop REAL, target1 REAL, target2 REAL, time_stop INTEGER,
    status TEXT,                -- 보유 / 1차매도 / 청산
    realized REAL DEFAULT 0,    -- 실현손익(원)
    sells TEXT DEFAULT '[]',    -- [{date,price,qty}]
    last_price REAL, last_check TEXT,
    alerts TEXT DEFAULT '[]',
    memo TEXT DEFAULT '',
    closed INTEGER DEFAULT 0, close_date TEXT, result_pct REAL
);
CREATE TABLE IF NOT EXISTS flows (
    date TEXT, ticker TEXT, investor TEXT, amt REAL,
    PRIMARY KEY (date, ticker, investor)
);
CREATE INDEX IF NOT EXISTS idx_flows_inv ON flows(investor, date);
CREATE TABLE IF NOT EXISTS events (
    ticker TEXT, date TEXT, type TEXT, title TEXT, rcept_no TEXT,
    PRIMARY KEY (ticker, rcept_no)
);
"""

_local = threading.local()


def conn():
    if not hasattr(_local, 'db'):
        _local.db = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
        _local.db.row_factory = sqlite3.Row
        _local.db.execute("PRAGMA journal_mode=WAL")
        _local.db.execute("PRAGMA synchronous=NORMAL")
    return _local.db


# 기존 DB 호환 — 새 컬럼 자동 추가
MIGRATE = [
    ("stocks", "sector", "TEXT DEFAULT ''"),
    ("stocks", "shares", "REAL DEFAULT 0"),
    ("stocks", "per", "REAL DEFAULT 0"),
    ("stocks", "pbr", "REAL DEFAULT 0"),
    ("stocks", "foreign_ratio", "REAL DEFAULT 0"),
    ("stocks", "warns", "TEXT DEFAULT ''"),
    ("positions", "linked", "INTEGER DEFAULT 0"),   # KIS 잔고로 확인된 포지션
    ("stocks", "hist_done", "INTEGER DEFAULT 0"),    # 과거 이력 확장 완료 (상장일 도달 포함)
]


def init_db():
    c = conn()
    c.executescript(SCHEMA)
    for tbl, col, typ in MIGRATE:
        cols = {r[1] for r in c.execute(f"PRAGMA table_info({tbl})")}
        if col not in cols:
            c.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {typ}")
    c.commit()


def meta_get(k, default=None):
    r = conn().execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    return r['v'] if r else default


def meta_set(k, v):
    c = conn()
    c.execute("INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)", (k, str(v)))
    c.commit()


# ════════════════════════════════════════════
#  KIS 인증 / 요청
# ════════════════════════════════════════════
_token = {'v': '', 'exp': None}
_tok_lock = threading.Lock()


def get_token(app_key, app_secret):
    with _tok_lock:
        if _token['v'] and _token['exp'] and _token['exp'] > datetime.now():
            return _token['v']
    body = json.dumps({"grant_type": "client_credentials",
                       "appkey": app_key, "appsecret": app_secret}).encode()
    req = urllib.request.Request(f"{KIS_URL}/oauth2/tokenP", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        d = json.loads(r.read().decode())
    tok = d.get('access_token', '')
    if not tok:
        raise RuntimeError(f"토큰 발급 실패: {d}")
    with _tok_lock:
        _token['v'] = tok
        _token['exp'] = datetime.now() + timedelta(hours=23)
    return tok


def kis_get(path, tr_id, params, app_key, app_secret, token, retry=2):
    _throttle()
    url = f"{KIS_URL}{path}?{urllib.parse.urlencode(params)}"
    headers = {"Content-Type": "application/json; charset=utf-8",
               "authorization": f"Bearer {token}", "appkey": app_key,
               "appsecret": app_secret, "tr_id": tr_id, "custtype": "P"}
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 401 and retry > 0:
            _token['v'] = ''
            new = get_token(app_key, app_secret)
            return kis_get(path, tr_id, params, app_key, app_secret, new, retry - 1)
        if e.code in (429, 500, 502, 503) and retry > 0:
            time.sleep(1.5)
            return kis_get(path, tr_id, params, app_key, app_secret, token, retry - 1)
        raise
    except Exception:
        if retry > 0:
            time.sleep(1.0)
            return kis_get(path, tr_id, params, app_key, app_secret, token, retry - 1)
        raise


# ════════════════════════════════════════════
#  1단계 · 전종목 마스터 수집
# ════════════════════════════════════════════
# 우선주 / 스팩 / ETF·ETN / 리츠 제외 규칙
ETF_KW = ['KODEX', 'TIGER', 'KBSTAR', 'HANARO', 'KOSEF', 'ARIRANG', 'SOL ', 'ACE ',
          'RISE ', 'PLUS ', 'TIMEFOLIO', 'FOCUS', 'ETN', '레버리지', '인버스',
          '선물', '채권', '국채', 'ETF']
BAD_KW = ['스팩', '리츠', '제1호', '제2호', '제3호', '기업인수목적']


def _classify(ticker, name):
    """제외 사유 반환. 빈 문자열이면 통과."""
    up = name.upper()
    for kw in ETF_KW:
        if kw in up:
            return 'ETF/ETN'
    for kw in BAD_KW:
        if kw in name:
            return '스팩/리츠'
    # 우선주: 종목코드 끝자리가 0이 아님 (5,7,9 등) + 이름에 '우' 계열
    if not ticker.endswith('0'):
        return '우선주'
    return ''


def _download_master(market):
    """KIS 마스터파일 다운로드 → [(ticker, name), ...]"""
    fn = 'kospi_code' if market == 'KOSPI' else 'kosdaq_code'
    url = f"{MASTER_URL}/{fn}.mst.zip"
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=60) as r:
        raw = r.read()
    out = []
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        with z.open(f"{fn}.mst") as f:
            for line in io.TextIOWrapper(f, encoding='cp949', errors='ignore'):
                if len(line) < 25:
                    continue
                ticker = line[0:9].strip()
                # 21번째 문자부터 한글종목명 + 뒤쪽 고정폭(228바이트) 부가정보
                try:
                    b = line[21:].rstrip('\r\n').encode('cp949', errors='ignore')
                    name = b[:max(0, len(b) - 228)].decode('cp949', errors='ignore').strip()
                except Exception:
                    name = line[21:60].strip()
                if len(ticker) == 6 and name:
                    out.append((ticker, name))
    return out


def build_universe(app_key, app_secret, progress=None):
    """전종목 마스터 → stocks 테이블. 후보풀 판정은 sync_candles 이후."""
    init_db()
    c = conn()
    total = 0
    for market in ('KOSPI', 'KOSDAQ'):
        try:
            rows = _download_master(market)
        except Exception as e:
            if progress:
                progress(f"{market} 마스터 다운로드 실패: {e}")
            continue
        for ticker, name in rows:
            ex = _classify(ticker, name)
            c.execute("""INSERT INTO stocks(ticker,name,market,excluded,updated)
                         VALUES(?,?,?,?,?)
                         ON CONFLICT(ticker) DO UPDATE SET name=excluded.name,
                         market=excluded.market, excluded=excluded.excluded""",
                      (ticker, name, market, ex, datetime.now().isoformat()))
            total += 1
        c.commit()
        if progress:
            progress(f"{market} {len(rows)}종목 등록")
    meta_set('universe_built', datetime.now().isoformat())
    alive = c.execute("SELECT COUNT(*) n FROM stocks WHERE excluded=''").fetchone()['n']
    if progress:
        progress(f"전체 {total}종목 · 분석대상 {alive}종목")
    return {'total': total, 'tradable': alive}


# ════════════════════════════════════════════
#  2단계 · 일봉 수집
# ════════════════════════════════════════════
def fetch_candles(ticker, app_key, app_secret, token, days=250):
    """KIS 기간별시세(FHKST03010100) — 요청당 최대 100건(최신순)이라 구간을 이어서 수집.
       다음 구간은 반드시 '받은 가장 오래된 날짜 - 1일'부터 요청해야 누락이 없다.
       (요청 시작일 기준으로 넘어가면 100건을 넘친 거래일이 매번 버려짐)"""
    out = {}
    end = datetime.now()
    empty = 0
    for _ in range(days // 90 + 3):
        start = end - timedelta(days=150)
        try:
            r = kis_get("/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
                        "FHKST03010100",
                        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker,
                         "FID_INPUT_DATE_1": start.strftime('%Y%m%d'),
                         "FID_INPUT_DATE_2": end.strftime('%Y%m%d'),
                         "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"},
                        app_key, app_secret, token)
        except Exception:
            break
        got = []
        for it in r.get('output2') or []:
            d = it.get('stck_bsop_date', '')
            if not d:
                continue
            try:
                cl = float(it.get('stck_clpr', 0) or 0)
                if cl <= 0:
                    continue
                out[d] = (float(it.get('stck_oprc', 0) or 0),
                          float(it.get('stck_hgpr', 0) or 0),
                          float(it.get('stck_lwpr', 0) or 0),
                          cl, int(it.get('acml_vol', 0) or 0))
                got.append(d)
            except Exception:
                continue
        if got:
            end = datetime.strptime(min(got), '%Y%m%d') - timedelta(days=1)
            empty = 0
        else:
            end = start - timedelta(days=1)     # 상장 이전 구간
            empty += 1
            if empty >= 2:
                break
        if len(out) >= days:
            break
    rows = sorted(out.items())[-days:]
    return [{'date': d, 'open': v[0], 'high': v[1], 'low': v[2],
             'close': v[3], 'volume': v[4]} for d, v in rows]


def _session_open():
    n = datetime.now()
    return n.weekday() < 5 and n.strftime('%H:%M') < '15:40'


def _detect_adjustment(c, tk, cd):
    """새로 받은 봉과 DB의 같은 날짜 종가가 '모두 같은 비율'로 다르면 권리조정 → 비율(새/옛) 반환"""
    dates = [x['date'] for x in cd]
    old = {r[0]: r[1] for r in c.execute("SELECT date, close FROM candles WHERE ticker=? AND date BETWEEN ? AND ?",
                                          (tk, min(dates), max(dates)))}
    rs = sorted(x['close'] / old[x['date']] for x in cd if old.get(x['date']))
    if len(rs) < 3:
        return None
    med = rs[len(rs) // 2]
    if abs(med - 1) <= 0.01 or rs[-1] / rs[0] > 1.02:       # 차이가 없거나, 비율이 들쭉날쭉하면(일시적 정정) 무시
        return None
    return med


def _apply_adjustment(tk, ratio):
    """권리조정 비율만큼 보유 중인 가상매매·자동매매 기록을 새 기준으로 보정 (가짜 손익 방지)"""
    c = conn()
    try:
        c.execute("""UPDATE vtrades SET entry_price = entry_price * ?, signal_close = signal_close * ?,
                     last_close = last_close * ? WHERE ticker=? AND status IN ('대기','보유')""", (ratio, ratio, ratio, tk))
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("""UPDATE at_positions SET avg_price = avg_price * ?, qty = CAST(ROUND(qty / ?) AS INTEGER)
                     WHERE ticker=? AND status='보유'""", (ratio, ratio, tk))
    except sqlite3.OperationalError:
        pass
    c.execute("CREATE TABLE IF NOT EXISTS adj_events (ticker TEXT, detected_at TEXT, ratio REAL)")
    c.execute("INSERT INTO adj_events VALUES(?,?,?)", (tk, datetime.now().isoformat(timespec='seconds'), ratio))
    c.commit()


def save_candles(ticker, candles):
    # 장중(15:40 이전)에 받은 당일 봉은 미완성이므로 저장하지 않음
    #  → 저장하면 이후 동기화가 "오늘 이미 받음"으로 판단해 영원히 수정되지 않음
    if candles and _session_open():
        today = datetime.now().strftime('%Y%m%d')
        candles = [c for c in candles if c['date'] != today]
    if not candles:
        return
    c = conn()
    c.executemany("""INSERT OR REPLACE INTO candles(ticker,date,open,high,low,close,volume)
                     VALUES(?,?,?,?,?,?,?)""",
                  [(ticker, x['date'], x['open'], x['high'], x['low'],
                    x['close'], x['volume']) for x in candles])
    c.commit()


def load_candles(ticker, days=250):
    rows = conn().execute(
        "SELECT date,open,high,low,close,volume FROM candles WHERE ticker=? "
        "ORDER BY date DESC LIMIT ?", (ticker, days)).fetchall()
    return [dict(r) for r in reversed(rows)]


# ════════════════════════════════════════════
#  지수 ETF 비교 (v5.5) — 가상 계좌와 같은 금액으로 지수 ETF를 그냥 들고 있었다면
#  · 모델 판정 · 후보풀 · 모델 점수 계산에는 쓰지 않음 (비교 표시 전용)
# ════════════════════════════════════════════
BENCH = {'069500': 'KODEX 200'}
BENCH_TICKERS = tuple(BENCH)
ETF_COST = 0.0003          # 편도 — 수수료·호가 (ETF는 증권거래세 없음)


def sync_bench(app_key, app_secret, days=500):
    """비교용 지수 ETF 일봉 — 종목 일봉의 마지막 날짜를 넘지 않게 저장 (거래일 목록이 ETF 때문에 앞서가지 않도록)"""
    init_db()
    c = conn()
    q = ','.join('?' * len(BENCH_TICKERS))
    last = c.execute(f"SELECT MAX(date) FROM candles WHERE ticker NOT IN ({q})", BENCH_TICKERS).fetchone()[0]
    if not last:
        return {}
    token = get_token(app_key, app_secret)
    out = {}
    for tk in BENCH_TICKERS:
        have = c.execute("SELECT MAX(date), COUNT(*) FROM candles WHERE ticker=?", (tk,)).fetchone()
        need = days
        if have[1] and have[0]:
            if have[0] >= last:
                out[tk] = 0
                continue
            need = min(days, max(10, (datetime.now() - datetime.strptime(have[0], '%Y%m%d')).days + 5))
        cd = [x for x in fetch_candles(tk, app_key, app_secret, token, need) if x['date'] <= last]
        save_candles(tk, cd)
        out[tk] = len(cd)
    return out


def bench_curve(start_date, ticker=None):
    """지수 ETF 그냥 보유 — start_date 시가에 가상 계좌 금액 전부로 매수 → 매일 종가 평가(매도 비용 미리 뺌) · 모델 곡선과 같은 날짜"""
    tk = ticker or BENCH_TICKERS[0]
    if not start_date:
        return {'curve': [], 'stats': None}
    rows = conn().execute("SELECT date, open, close FROM candles WHERE ticker=? AND date>=? ORDER BY date",
                          (tk, start_date)).fetchall()
    if not rows or not (rows[0][1] or rows[0][2]):
        return {'curve': [], 'stats': None}
    ep = rows[0][1] or rows[0][2]
    units = ACCT['cash'] * (1 - ETF_COST) / ep
    curve = [{'date': d, 'equity': round(units * cl * (1 - ETF_COST))} for d, o, cl in rows if cl]
    peak, mdd = 0, 0.0
    for p_ in curve:
        peak = max(peak, p_['equity'])
        mdd = min(mdd, p_['equity'] / peak - 1)
    eq = curve[-1]['equity']
    return {'curve': curve, 'stats': {'ticker': tk, 'name': BENCH.get(tk, tk), 'start': rows[0][0], 'entry': ep,
                                      'equity': eq, 'return': round((eq / ACCT['cash'] - 1) * 100, 2),
                                      'mdd': round(mdd * 100, 2), 'last': curve[-1]['date']}}


def bench_info():
    """비교 지수 ETF 일봉 현황 (시스템 정보용)"""
    tk = BENCH_TICKERS[0]
    n, first, last = conn().execute("SELECT COUNT(*), MIN(date), MAX(date) FROM candles WHERE ticker=?", (tk,)).fetchone()
    return {'ticker': tk, 'name': BENCH[tk], 'candles': n, 'first': first, 'last': last}


def sync_candles(app_key, app_secret, tickers=None, days=250, progress=None, stop_flag=None):
    """일봉 수집. tickers=None이면 제외되지 않은 전종목."""
    init_db()
    token = get_token(app_key, app_secret)
    c = conn()
    if tickers is None:
        tickers = [r['ticker'] for r in c.execute(
            "SELECT ticker FROM stocks WHERE excluded='' ORDER BY ticker")]
    done, fail = 0, 0
    adjusted = []
    t0 = time.time()
    for i, tk in enumerate(tickers):
        if stop_flag and stop_flag():
            break
        try:
            have = c.execute("SELECT MAX(date) d, COUNT(*) n FROM candles WHERE ticker=?",
                             (tk,)).fetchone()
            need = days
            if have['n'] and have['n'] > 120 and have['d']:
                last = datetime.strptime(have['d'], '%Y%m%d')
                gap = (datetime.now() - last).days
                if gap <= 0:
                    done += 1
                    continue
                need = min(days, max(10, gap + 5))  # 증분
            cd = fetch_candles(tk, app_key, app_secret, token, need)
            if cd:
                ratio = _detect_adjustment(c, tk, cd) if have['n'] and have['n'] > 120 else None
                if ratio:
                    # 권리조정(분할·병합·증자): KIS는 과거 전체를 새 기준으로 다시 주므로 전체 재수집해 교체
                    full = fetch_candles(tk, app_key, app_secret, token, max(have['n'] + 30, 260))
                    if full and len(full) >= have['n'] * 0.8:
                        c.execute("DELETE FROM candles WHERE ticker=?", (tk,))
                        c.commit()
                        save_candles(tk, full)
                        _apply_adjustment(tk, ratio)
                        adjusted.append((tk, ratio))
                    else:
                        save_candles(tk, cd)
                else:
                    save_candles(tk, cd)
                done += 1
            else:
                fail += 1
        except Exception:
            fail += 1
        if progress and (i % 25 == 0 or i == len(tickers) - 1):
            el = time.time() - t0
            eta = (el / max(1, i + 1)) * (len(tickers) - i - 1)
            progress({'stage': 'candles', 'done': i + 1, 'total': len(tickers),
                      'ok': done, 'fail': fail, 'eta': int(eta)})
    meta_set('candles_synced', datetime.now().isoformat())
    return {'ok': done, 'fail': fail, 'adjusted': adjusted}


# ════════════════════════════════════════════
#  3단계 · 후보풀 확정
# ════════════════════════════════════════════
def rebuild_pool(min_value=3_000_000_000, min_days=120, min_price=1000, max_price=500000):
    """20일 평균 거래대금 기준으로 후보풀 재선정"""
    c = conn()
    c.execute("UPDATE stocks SET in_pool=0")
    rows = c.execute("SELECT ticker FROM stocks WHERE excluded=''").fetchall()
    picked = 0
    for r in rows:
        tk = r['ticker']
        cd = conn().execute(
            "SELECT close, volume FROM candles WHERE ticker=? ORDER BY date DESC LIMIT 20",
            (tk,)).fetchall()
        n = conn().execute("SELECT COUNT(*) n FROM candles WHERE ticker=?",
                           (tk,)).fetchone()['n']
        if len(cd) < 20 or n < min_days:
            continue
        vals = [x['close'] * x['volume'] for x in cd]
        avg = sum(vals) / len(vals)
        px = cd[0]['close']
        if avg < min_value or px < min_price or px > max_price:
            c.execute("UPDATE stocks SET avg_value=? WHERE ticker=?", (avg, tk))
            continue
        c.execute("UPDATE stocks SET in_pool=1, avg_value=? WHERE ticker=?", (avg, tk))
        picked += 1
    c.commit()
    meta_set('pool_size', picked)
    meta_set('pool_built', datetime.now().isoformat())
    return picked


def get_pool():
    return [dict(r) for r in conn().execute(
        "SELECT ticker,name,market,avg_value,sector,mktcap,shares,per,pbr,warns "
        "FROM stocks WHERE in_pool=1 ORDER BY ticker")]


# ════════════════════════════════════════════
#  4단계 · 수급 데이터 (후보풀 한정)
# ════════════════════════════════════════════
def sync_investors(app_key, app_secret, tickers, progress=None, stop_flag=None):
    token = get_token(app_key, app_secret)
    c = conn()
    ok = 0
    for i, tk in enumerate(tickers):
        if stop_flag and stop_flag():
            break
        try:
            r = kis_get("/uapi/domestic-stock/v1/quotations/inquire-investor",
                        "FHKST01010900",
                        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": tk},
                        app_key, app_secret, token)
            rows = []
            for it in (r.get('output') or [])[:20]:
                d = it.get('stck_bsop_date', '')
                if not d:
                    continue
                rows.append((tk, d,
                             int(it.get('frgn_ntby_qty', 0) or 0),
                             int(it.get('orgn_ntby_qty', 0) or 0),
                             int(it.get('frgn_ntby_tr_pbmn', 0) or 0),
                             int(it.get('orgn_ntby_tr_pbmn', 0) or 0)))
            if rows:
                c.executemany("""INSERT OR REPLACE INTO investors
                    (ticker,date,foreign_qty,inst_qty,foreign_amt,inst_amt)
                    VALUES(?,?,?,?,?,?)""", rows)
                c.commit()
                ok += 1
        except Exception:
            pass
        if progress and i % 25 == 0:
            progress({'stage': 'investors', 'done': i + 1, 'total': len(tickers), 'ok': ok})
    meta_set('investors_synced', datetime.now().isoformat())
    return ok


def load_investors(ticker, days=20):
    rows = conn().execute(
        "SELECT date,foreign_qty,inst_qty,foreign_amt,inst_amt FROM investors "
        "WHERE ticker=? ORDER BY date DESC LIMIT ?", (ticker, days)).fetchall()
    return [dict(r) for r in reversed(rows)]


# ════════════════════════════════════════════
#  추천 이력 / 성과 추적
# ════════════════════════════════════════════
def save_scan(horizon, results):
    c = conn()
    ts = datetime.now().isoformat()
    ids = []
    for r in results:
        cur = c.execute("""INSERT INTO scans
            (ts,horizon,ticker,name,score,strategy,entry,stop,target1,target2,payload)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                        (ts, horizon, r['ticker'], r['name'], r['score'],
                         r.get('strategy', ''), r.get('entry', 0), r.get('stop', 0),
                         r.get('target1', 0), r.get('target2', 0),
                         json.dumps(r, ensure_ascii=False)))
        ids.append(cur.lastrowid)
    c.commit()
    return ids


def last_scan(horizon):
    """가장 최근 스캔 결과 (서버를 다시 켜도 화면에 바로 보이도록 DB에서 복원)"""
    r = conn().execute("SELECT MAX(ts) FROM scans WHERE horizon=?", (horizon,)).fetchone()
    if not r or not r[0]:
        return None
    rows = conn().execute("SELECT payload FROM scans WHERE horizon=? AND ts=? ORDER BY id", (horizon, r[0])).fetchall()
    res = []
    for (pl,) in rows:
        try:
            res.append(json.loads(pl))
        except Exception:
            pass
    return {'results': res, 'ts': r[0], 'restored': True} if res else None


def track_outcomes():
    """과거 추천의 1/3/5/20일 수익률 갱신"""
    c = conn()
    rows = c.execute("""SELECT s.id,s.ticker,s.ts,s.entry FROM scans s
                        LEFT JOIN outcomes o ON o.scan_id=s.id
                        WHERE o.scan_id IS NULL OR o.d20 IS NULL""").fetchall()
    n = 0
    for r in rows:
        base_date = r['ts'][:10].replace('-', '')
        cd = c.execute("SELECT date,close FROM candles WHERE ticker=? AND date>? "
                       "ORDER BY date LIMIT 20", (r['ticker'], base_date)).fetchall()
        if not cd:
            continue
        base = r['entry'] or 0
        if base <= 0:
            b = c.execute("SELECT close FROM candles WHERE ticker=? AND date<=? "
                          "ORDER BY date DESC LIMIT 1", (r['ticker'], base_date)).fetchone()
            base = b['close'] if b else 0
        if base <= 0:
            continue

        def pct(i):
            return round((cd[i]['close'] - base) / base * 100, 2) if len(cd) > i else None
        c.execute("""INSERT OR REPLACE INTO outcomes(scan_id,d1,d3,d5,d20,checked)
                     VALUES(?,?,?,?,?,?)""",
                  (r['id'], pct(0), pct(2), pct(4), pct(19), datetime.now().isoformat()))
        n += 1
    c.commit()
    return n


def strategy_performance(days=180):
    """전략별 승률·평균수익률 — 자가학습 입력"""
    since = (datetime.now() - timedelta(days=days)).isoformat()
    rows = conn().execute("""
        SELECT s.strategy, COUNT(*) n,
               AVG(o.d5) avg5, AVG(o.d20) avg20,
               SUM(CASE WHEN o.d5>0 THEN 1 ELSE 0 END)*1.0/COUNT(*) win5
        FROM scans s JOIN outcomes o ON o.scan_id=s.id
        WHERE s.ts>? AND o.d5 IS NOT NULL
        GROUP BY s.strategy ORDER BY avg5 DESC""", (since,)).fetchall()
    return [dict(r) for r in rows]


def db_stats():
    c = conn()
    def one(q):
        r = c.execute(q).fetchone()
        return r[0] if r else 0
    return {
        'stocks': one("SELECT COUNT(*) FROM stocks"),
        'tradable': one("SELECT COUNT(*) FROM stocks WHERE excluded=''"),
        'pool': one("SELECT COUNT(*) FROM stocks WHERE in_pool=1"),
        'candles': one("SELECT COUNT(*) FROM candles"),
        'investors': one("SELECT COUNT(DISTINCT ticker) FROM investors"),
        'scans': one("SELECT COUNT(*) FROM scans"),
        'universe_built': meta_get('universe_built', ''),
        'candles_synced': meta_get('candles_synced', ''),
        'pool_built': meta_get('pool_built', ''),
    }


# ════════════════════════════════════════════
#  추천 추적 — 진입·목표·손절 상태 관리
# ════════════════════════════════════════════
ACTIVE = ('대기', '진입', '1차도달')


def bdays_after(start_date, n):
    """start_date(YYYY-MM-DD) 기준 n거래일 후 날짜 (주말만 제외)"""
    d = datetime.strptime(start_date[:10], '%Y-%m-%d')
    k = 0
    while k < n:
        d += timedelta(days=1)
        if d.weekday() < 5:
            k += 1
    return d.strftime('%Y-%m-%d')


def bdays_between(a, b):
    da = datetime.strptime(a[:10], '%Y-%m-%d')
    db_ = datetime.strptime(b[:10], '%Y-%m-%d')
    n, d = 0, da
    while d < db_:
        d += timedelta(days=1)
        if d.weekday() < 5:
            n += 1
    return n


def add_tracking(item, scan_id=None):
    """추천 1건을 추적 목록에 등록. 같은 종목이 이미 추적 중이면 건너뜀."""
    c = conn()
    dup = c.execute(f"SELECT id FROM tracking WHERE ticker=? AND status IN "
                    f"({','.join('?'*len(ACTIVE))})",
                    (item['ticker'], *ACTIVE)).fetchone()
    if dup:
        return None
    p = item['plan']
    today = datetime.now().strftime('%Y-%m-%d')
    immediate = p['buy_type'] == '즉시 매수'
    status = '진입' if immediate else '대기'
    cur = c.execute("""INSERT INTO tracking
        (scan_id,ticker,name,strategy,horizon,created,buy_type,entry,stop,target1,target2,
         valid_until,hold_max,time_stop,status,entry_date,entry_price,cur_stop,
         last_price,last_check)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (scan_id, item['ticker'], item['name'], item['strategy'], item.get('horizon', ''),
         datetime.now().isoformat(), p['buy_type'], p['entry'], p['stop'],
         p['target1'], p['target2'], bdays_after(today, p['valid_days']),
         p['hold_max'], p['time_stop'], status,
         today if immediate else None, item['price'] if immediate else None,
         p['stop'], item['price'], datetime.now().isoformat()))
    c.commit()
    return cur.lastrowid


def open_tracking():
    return [dict(r) for r in conn().execute(
        f"SELECT * FROM tracking WHERE status IN ({','.join('?'*len(ACTIVE))}) "
        f"ORDER BY id DESC", ACTIVE)]


def list_tracking(limit=100):
    return [dict(r) for r in conn().execute(
        "SELECT * FROM tracking ORDER BY closed ASC, id DESC LIMIT ?", (limit,))]


def update_tracking(tid, **kw):
    if not kw:
        return
    c = conn()
    cols = ','.join(f"{k}=?" for k in kw)
    c.execute(f"UPDATE tracking SET {cols} WHERE id=?", (*kw.values(), tid))
    c.commit()


def evaluate_tracking(t, price, high, low, today=None):
    """추적 1건 상태 전이. 반환: (변경필드 dict, 알림 list[(key, msg)])"""
    today = today or datetime.now().strftime('%Y-%m-%d')
    upd, alerts = {'last_price': price, 'last_check': datetime.now().isoformat()}, []
    sent = set(json.loads(t.get('alerts') or '[]'))
    st = t['status']
    nm = f"{t['name']}({t['ticker']})"

    def alert(key, msg):
        if key not in sent:
            alerts.append((key, msg))
            sent.add(key)

    def close(status, pct):
        upd.update(status=status, closed=1, result_pct=round(pct, 2))

    if st == '대기':
        hit = False
        if t['buy_type'] in ('돌파 대기', '관망') and high >= t['entry']:
            hit = True
        elif t['buy_type'] == '지정가 대기' and low <= t['entry']:
            hit = True
        if hit:
            # 진입 당일 손절선까지 밀렸으면 손절로 처리 (신호소멸로 숨기지 않음)
            if low <= t['stop']:
                fill = t['stop'] if price >= t['stop'] else price
                upd.update(entry_date=today, entry_price=t['entry'])
                close('손절', (fill - t['entry']) / t['entry'] * 100)
                alert('entry', f"🟢 {nm} 매수 신호 — {t['entry']:,.0f}원 도달")
                alert('stop', f"🔴 {nm} 진입 당일 손절 — {t['stop']:,.0f}원 이탈")
            else:
                upd.update(status='진입', entry_date=today, entry_price=t['entry'])
                alert('entry', f"🟢 {nm} 매수 신호 — {t['entry']:,.0f}원 도달 "
                               f"[{t['strategy']}] 손절 {t['stop']:,.0f}")
        elif low <= t['stop']:
            close('신호소멸', 0)
            alert('void', f"⚪ {nm} 신호소멸 — 진입 전 손절선 이탈")
        elif today > t['valid_until']:
            close('신호소멸', 0)
            alert('expire', f"⚪ {nm} 신호소멸 — 유효기간 내 미진입")
        elif t['buy_type'] == '돌파 대기' and price >= t['entry'] * 0.98:
            alert('near_entry', f"🔔 {nm} 돌파 임박 — 현재 {price:,.0f} / 돌파가 {t['entry']:,.0f}")

    elif st == '진입':
        ep = t['entry_price'] or t['entry']
        held = bdays_between(t['entry_date'] or today, today)
        if low <= t['cur_stop']:
            fill = t['cur_stop'] if price >= t['cur_stop'] else price   # 갭하락 시 현재가 체결
            close('손절', (fill - ep) / ep * 100)
            alert('stop', f"🔴 {nm} 손절 — {t['cur_stop']:,.0f}원 이탈 "
                          f"({(fill-ep)/ep*100:+.1f}%)")
        elif high >= t['target1']:
            upd.update(status='1차도달', cur_stop=ep)
            alert('t1', f"🎯 {nm} 1차 목표 도달 {t['target1']:,.0f}원 "
                        f"({(t['target1']-ep)/ep*100:+.1f}%) → 50% 매도 · 손절선 본전 {ep:,.0f}")
        elif held >= t['time_stop']:
            close('시간손절', (price - ep) / ep * 100)
            alert('tstop', f"⏱ {nm} 시간손절 — {held}거래일 경과, 1차 미달 "
                           f"({(price-ep)/ep*100:+.1f}%)")
        elif price <= t['cur_stop'] * 1.02:
            alert('near_stop', f"⚠️ {nm} 손절선 근접 — 현재 {price:,.0f} / 손절 {t['cur_stop']:,.0f}")

    elif st == '1차도달':
        ep = t['entry_price'] or t['entry']
        half = (t['target1'] - ep) / ep * 100 / 2
        held = bdays_between(t['entry_date'] or today, today)
        if high >= t['target2']:
            close('2차도달', half + (t['target2'] - ep) / ep * 100 / 2)
            alert('t2', f"🏆 {nm} 2차 목표 도달 {t['target2']:,.0f}원 → 잔량 전량 매도")
        elif low <= t['cur_stop']:
            fill = t['cur_stop'] if price >= t['cur_stop'] else price
            close('본전청산', half + (fill - ep) / ep * 100 / 2)
            alert('be', f"🟡 {nm} 본전 청산 — 1차 수익 {half*2:+.1f}%의 절반 확보")
        elif held >= t['hold_max']:
            close('기간만료', half + (price - ep) / ep * 100 / 2)
            alert('hold', f"⏱ {nm} 보유기간 만료 — 잔량 정리")

    upd['alerts'] = json.dumps(sorted(sent), ensure_ascii=False)
    return upd, alerts


def tracking_performance():
    rows = conn().execute("""
        SELECT strategy, COUNT(*) n,
               AVG(result_pct) avg_pct,
               SUM(CASE WHEN result_pct>0 THEN 1 ELSE 0 END)*1.0/COUNT(*) win
        FROM tracking WHERE closed=1 AND status!='신호소멸'
        GROUP BY strategy ORDER BY avg_pct DESC""").fetchall()
    return [dict(r) for r in rows]


def tracking_tickers():
    return {r['ticker'] for r in conn().execute(
        f"SELECT ticker FROM tracking WHERE status IN ({','.join('?'*len(ACTIVE))})", ACTIVE)}


# ════════════════════════════════════════════
#  내 보유종목 — 수동 매매 기록 · 실제 평단 기준 알림
# ════════════════════════════════════════════
def add_position(ticker, name, buy_price, qty, stop, target1, target2,
                 strategy='직접 등록', horizon='', time_stop=0, memo=''):
    c = conn()
    today = datetime.now().strftime('%Y-%m-%d')
    cur = c.execute("""INSERT INTO positions
        (ticker,name,strategy,horizon,buy_date,buy_price,qty,remain_qty,stop,cur_stop,
         target1,target2,time_stop,status,last_price,last_check,memo)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (ticker, name, strategy, horizon, today, buy_price, qty, qty, stop, stop,
         target1, target2, time_stop, '보유', buy_price, datetime.now().isoformat(), memo))
    c.commit()
    return cur.lastrowid


def get_position(pid):
    r = conn().execute("SELECT * FROM positions WHERE id=?", (pid,)).fetchone()
    return dict(r) if r else None


def list_positions(include_closed=True, limit=200):
    q = "SELECT * FROM positions" + ("" if include_closed else " WHERE closed=0")
    return [dict(r) for r in conn().execute(q + " ORDER BY closed ASC, id DESC LIMIT ?", (limit,))]


def update_position(pid, **kw):
    if not kw:
        return
    c = conn()
    c.execute(f"UPDATE positions SET {','.join(k+'=?' for k in kw)} WHERE id=?",
              (*kw.values(), pid))
    c.commit()


def sell_position(pid, price, qty):
    """일부/전량 매도 기록. 첫 일부매도 후 손절선을 본전으로 자동 상향."""
    p = get_position(pid)
    if not p or p['closed']:
        return None
    qty = min(int(qty), p['remain_qty'])
    if qty <= 0:
        return None
    sells = json.loads(p['sells'] or '[]')
    sells.append({'date': datetime.now().strftime('%Y-%m-%d'), 'price': price, 'qty': qty})
    realized = p['realized'] + (price - p['buy_price']) * qty
    remain = p['remain_qty'] - qty
    upd = {'sells': json.dumps(sells), 'realized': realized, 'remain_qty': remain}
    if remain <= 0:
        sold_amt = sum(x['price'] * x['qty'] for x in sells)
        upd.update(status='청산', closed=1, close_date=datetime.now().strftime('%Y-%m-%d'),
                   result_pct=round((sold_amt / (p['buy_price'] * p['qty']) - 1) * 100, 2))
    else:
        upd['status'] = '1차매도'
        if p['cur_stop'] < p['buy_price']:
            # 본전 손절로 상향 — 평단은 호가 단위가 아니므로 한 틱 위로 맞춤 (앱 입력 가능 + 본전 보장)
            from scout_strategies import round_tick
            upd['cur_stop'] = round_tick(p['buy_price'], 'up')
        # 손절선이 바뀌었으니 손절 관련 알림 재무장
        al = set(json.loads(p['alerts'] or '[]')) - {'stop', 'near_stop'}
        upd['alerts'] = json.dumps(sorted(al))
    update_position(pid, **upd)
    return get_position(pid)


def evaluate_position(p, price, high, low, today=None):
    """실제 보유 1건 점검 → 알림만 생성 (매매는 사용자가 직접)."""
    today = today or datetime.now().strftime('%Y-%m-%d')
    sent = set(json.loads(p.get('alerts') or '[]'))
    alerts = []
    nm = f"{p['name']}({p['ticker']})"
    bp = p['buy_price']
    pnl = (price - bp) / bp * 100 if bp else 0

    def alert(key, msg):
        if key not in sent:
            sent.add(key)
            alerts.append((key, msg))

    if low <= p['cur_stop']:
        be = p['cur_stop'] >= bp
        alert('stop', f"🔴 {nm} {'본전선' if be else '손절선'} {p['cur_stop']:,.0f}원 이탈 → "
                      f"{'잔량 정리' if be else '손절'} 검토 | 현재 {price:,.0f} ({pnl:+.1f}%)")
    elif price <= p['cur_stop'] * 1.02:
        alert('near_stop', f"⚠️ {nm} 손절선 근접 — 현재 {price:,.0f} / 손절 {p['cur_stop']:,.0f}")
    if p['target1'] and high >= p['target1'] and p['status'] == '보유':
        alert('t1', f"🎯 {nm} 1차 목표 {p['target1']:,.0f}원 도달 ({pnl:+.1f}%) → "
                    f"50% 매도 검토 · 매도 기록하면 손절선이 본전으로 올라갑니다")
    if p['target2'] and high >= p['target2']:
        alert('t2', f"🏆 {nm} 2차 목표 {p['target2']:,.0f}원 도달 ({pnl:+.1f}%) → 잔량 매도 검토")
    if p.get('time_stop') and p['status'] == '보유':
        held = bdays_between(p['buy_date'], today)
        if held >= p['time_stop'] and (not p['target1'] or price < p['target1']):
            alert('tstop', f"⏱ {nm} 보유 {held}거래일 · 1차 목표 미달 ({pnl:+.1f}%) → 정리 검토")
    return {'last_price': price, 'last_check': datetime.now().isoformat(),
            'alerts': json.dumps(sorted(sent), ensure_ascii=False)}, alerts


def position_summary():
    rows = list_positions(False)
    invested = sum(r['buy_price'] * r['remain_qty'] for r in rows)
    value = sum((r['last_price'] or r['buy_price']) * r['remain_qty'] for r in rows)
    realized_open = sum(r['realized'] for r in rows)
    closed = conn().execute("SELECT COALESCE(SUM(realized),0) s, COUNT(*) n, "
                            "SUM(CASE WHEN realized>0 THEN 1 ELSE 0 END) w "
                            "FROM positions WHERE closed=1").fetchone()
    return {'open': len(rows), 'invested': invested, 'value': value,
            'unrealized': value - invested, 'realized_open': realized_open,
            'closed_n': closed['n'], 'closed_win': closed['w'] or 0,
            'realized_closed': closed['s']}


def position_performance():
    return [dict(r) for r in conn().execute("""
        SELECT strategy, COUNT(*) n, AVG(result_pct) avg_pct,
               SUM(CASE WHEN result_pct>0 THEN 1 ELSE 0 END)*1.0/COUNT(*) win,
               SUM(realized) realized
        FROM positions WHERE closed=1 GROUP BY strategy ORDER BY avg_pct DESC""")]


def extend_history(app_key, app_secret, tickers, target=750, progress=None, stop_flag=None):
    """후보풀 종목의 과거 이력을 target 거래일까지 확장 (백테스트·장기 패턴용).
       이미 충분하면 건너뛰므로, 매일 돌려도 새로 후보풀에 들어온 종목만 작업."""
    token = get_token(app_key, app_secret)
    c = conn()
    done = 0
    for i, tk in enumerate(tickers):
        if stop_flag and stop_flag():
            break
        hd = c.execute("SELECT hist_done FROM stocks WHERE ticker=?", (tk,)).fetchone()
        if hd and hd['hist_done']:
            continue                                  # 이미 끝까지 받음 (상장일 도달)
        r = c.execute("SELECT COUNT(*) n, MIN(date) d FROM candles WHERE ticker=?", (tk,)).fetchone()
        if not r['n']:
            continue
        if r['n'] >= target:
            c.execute("UPDATE stocks SET hist_done=1 WHERE ticker=?", (tk,))
            continue
        end = datetime.strptime(r['d'], '%Y%m%d') - timedelta(days=1)
        have, empty = r['n'], 0
        while have < target and empty < 2:
            start = end - timedelta(days=150)
            try:
                res = kis_get("/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
                              "FHKST03010100",
                              {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": tk,
                               "FID_INPUT_DATE_1": start.strftime('%Y%m%d'),
                               "FID_INPUT_DATE_2": end.strftime('%Y%m%d'),
                               "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"},
                              app_key, app_secret, token)
            except Exception:
                break
            rows = []
            for it in res.get('output2') or []:
                try:
                    cl = float(it.get('stck_clpr', 0) or 0)
                    if cl > 0 and it.get('stck_bsop_date'):
                        rows.append({'date': it['stck_bsop_date'], 'open': float(it['stck_oprc']),
                                     'high': float(it['stck_hgpr']), 'low': float(it['stck_lwpr']),
                                     'close': cl, 'volume': int(it.get('acml_vol', 0) or 0)})
                except Exception:
                    continue
            if rows:
                save_candles(tk, rows)
                have += len(rows)
                end = datetime.strptime(min(x['date'] for x in rows), '%Y%m%d') - timedelta(days=1)
                empty = 0
            else:
                end = start - timedelta(days=1)      # 상장 이전 구간 → 두 번 비면 종료
                empty += 1
        if have >= target or empty >= 2:
            c.execute("UPDATE stocks SET hist_done=1 WHERE ticker=?", (tk,))
            c.commit()
        done += 1
        if progress and i % 20 == 0:
            progress({'stage': 'history', 'done': i + 1, 'total': len(tickers), 'ok': done})
    meta_set('history_extended', datetime.now().isoformat())
    return done



# ════════════════════════════════════════════
#  KRX 투자자별 순매수 (pykrx · 연기금 포함)
# ════════════════════════════════════════════
def save_flows(date, investor, rows):
    """rows: [(ticker, 순매수거래대금(원))]"""
    c = conn()
    c.executemany("INSERT OR REPLACE INTO flows VALUES(?,?,?,?)",
                  [(date, tk, investor, amt) for tk, amt in rows])
    c.commit()


def flow_dates(investor):
    return {r[0] for r in conn().execute(
        "SELECT DISTINCT date FROM flows WHERE investor=?", (investor,))}


def flow_sums(dates, investor):
    """최근 거래일 목록 안의 종목별 (합계, 일수)"""
    if not dates:
        return {}
    q = ','.join('?' * len(dates))
    return {r[0]: (r[1], r[2]) for r in conn().execute(
        f"SELECT ticker, SUM(amt), COUNT(*) FROM flows WHERE investor=? AND date IN ({q}) GROUP BY ticker",
        (investor, *dates))}


def recent_trading_dates(n):
    return [r[0] for r in conn().execute(
        "SELECT DISTINCT date FROM candles ORDER BY date DESC LIMIT ?", (n,))][::-1]


# ════════════════════════════════════════════
#  실전 가상매매 (포워드 테스트) — 백테스트와 같은 규칙을 미래 데이터로 검증
#  · 신호일 다음 거래일 시가 매수
#  · 매일 종가가 9일 EMA 이상이면 그날 종가 매도, 최대 10거래일
#  · 거래 중단(5거래일 이상 데이터 없음) 시 마지막 종가 청산
#  · 비용 왕복 0.25%
#  · grp: 'strategy'(Scout 후보) / 'control'(같은 날 무작위 대조군)
# ════════════════════════════════════════════
VT_COST = 0.25
# 모델별 청산 규칙 — 504개 조합 역검증(조정 기간 선택 → 검증 기간·계좌 단위 확인)으로 확정, 변경 불가
# 모두 '종가로 판정 → 다음 거래일 시가 매도' (자동매매가 실제로 실행하는 방식과 동일). V6.2는 자체 규칙 별도
TRACK_RULES = {
    'final':    {'ema': True,  'sl': None, 'tp': None, 'hold': 10},
    'strategy': {'ema': True,  'sl': None, 'tp': None, 'hold': 10},
    'rsi':      {'ema': True,  'sl': None, 'tp': None, 'hold': 10},
    'fdip':     {'ema': False, 'sl': None, 'tp': 0.05, 'hold': 20},
    'lvflow':   {'ema': False, 'sl': None, 'tp': None, 'hold': 20},
    'lvhigh':   {'ema': False, 'sl': None, 'tp': None, 'hold': 20},    # 차트 모델 (v5.4) — 3가지 규칙 중 조정 기간 1위
    'candle':   {'ema': False, 'sl': None, 'tp': 0.05, 'hold': 20},
    'control':  {'ema': True,  'sl': None, 'tp': None, 'hold': 10},
    # 종가베팅 (v5.6) — 신호일 종가(장후 시간외 종가 15:40~16:00)에 매수 → 다음 거래일 시가 매도 · 정의 그대로 고정
    'jongga':         {'ema': False, 'sl': None, 'tp': None, 'hold': 1, 'night': True},
    'control_jongga': {'ema': False, 'sl': None, 'tp': None, 'hold': 1, 'night': True},
}
CLOSE_ENTRY = {'jongga', 'control_jongga'}      # 신호일 종가 매수 (나머지는 다음 거래일 시가 매수)


def rule_of(grp):
    return TRACK_RULES.get(grp, TRACK_RULES['control'])


def rule_text(rule):
    if rule.get('night'):
        return '종가 매수 → 다음 거래일 시가 매도 · 손절 없음'
    parts = []
    if rule['ema']:
        parts.append('9일 EMA 복귀')
    if rule['tp']:
        parts.append(f"익절 +{rule['tp'] * 100:g}%")
    parts.append(f"손절 −{rule['sl'] * 100:g}%" if rule['sl'] else '손절 없음')
    parts.append(f"최대 {rule['hold']}거래일")
    return ' · '.join(parts)


def _rule_walk(dates, closes, ema, start_i, ep, rule):
    """start_i(매수일)부터 종가로 판정 → 다음 거래일 시가 매도.
       반환 (매도 인덱스 또는 None(다음 봉 대기), 사유) · 판정이 아직 없으면 None"""
    n = 0
    for i in range(start_i, len(dates)):
        n += 1
        c_ = closes[i]
        chg = c_ / ep - 1 if ep else 0
        why = None
        if rule['sl'] and chg <= -rule['sl']:
            why = f"손절 −{rule['sl'] * 100:g}%"
        elif rule['tp'] and chg >= rule['tp']:
            why = f"익절 +{rule['tp'] * 100:g}%"
        elif rule['ema'] and c_ >= ema[i]:
            why = '9EMA 복귀'
        elif n >= rule['hold']:
            why = '다음날 시가' if rule.get('night') else f"{rule['hold']}일 만기"
        if why:
            return (i + 1 if i + 1 < len(dates) else None), why
    return None
_defer = {'on': False}          # 배치 생성 중에는 커밋을 미뤄 한 번에 확정(오류 시 전체 취소)
VT_HOLD = 10


_vt_ready = set()


def _vt_init():
    # executescript()는 진행 중인 트랜잭션을 강제로 커밋하므로 연결(스레드)마다 한 번만 실행
    # (매번 실행하면 배치 도중 오류가 나도 앞부분이 확정돼 롤백이 안 됨)
    key = (threading.get_ident(), DB_PATH)
    if key in _vt_ready:
        return
    conn().executescript("""
    CREATE TABLE IF NOT EXISTS vtrades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        grp TEXT, signal_date TEXT, ticker TEXT, name TEXT, rank INTEGER, score REAL,
        rsi REAL, signal_close REAL,
        status TEXT,            -- 대기 / 보유 / 청산
        entry_date TEXT, entry_price REAL,
        exit_date TEXT, exit_price REAL, exit_reason TEXT,
        held INTEGER DEFAULT 0, ret REAL, last_close REAL, last_date TEXT,
        UNIQUE (grp, signal_date, ticker)
    );
    CREATE INDEX IF NOT EXISTS idx_vt_ticker ON vtrades(ticker);
    CREATE INDEX IF NOT EXISTS idx_vt_status ON vtrades(grp, status);""")
    if meta_get('vt_rsi_rule', '') != 'rsi2_le5':
        # RSI 트랙 규칙 변경(14일 ≤20 → 2일 ≤5): 이전 기록은 비교가 섞이지 않게 별도 그룹으로 보관
        conn().execute("UPDATE vtrades SET grp='rsi14_old' WHERE grp='rsi'")
        meta_set('vt_rsi_rule', 'rsi2_le5')
    conn().commit()
    _vt_ready.add(key)


def vt_batch_exists(signal_date):
    _vt_init()
    return conn().execute("SELECT COUNT(*) FROM vtrades WHERE signal_date=? AND grp='strategy'",
                          (signal_date,)).fetchone()[0] > 0


def vt_open_tickers(grp):
    _vt_init()
    return {r[0] for r in conn().execute(
        "SELECT ticker FROM vtrades WHERE grp=? AND status!='청산'", (grp,))}


def vt_add(grp, signal_date, items, commit=True):
    """items: [(ticker, name, rank, score, rsi, signal_close)] · commit=False면 호출한 쪽이 한 번에 커밋(롤백 가능)"""
    _vt_init()
    c = conn()
    now_ = [it for it in items if grp in CLOSE_ENTRY and it[5]]
    wait_ = [it for it in items if not (grp in CLOSE_ENTRY and it[5])]
    c.executemany("""INSERT OR IGNORE INTO vtrades
        (grp,signal_date,ticker,name,rank,score,rsi,signal_close,status)
        VALUES(?,?,?,?,?,?,?,?,'대기')""", [(grp, signal_date, *it) for it in wait_])
    c.executemany("""INSERT OR IGNORE INTO vtrades
        (grp,signal_date,ticker,name,rank,score,rsi,signal_close,status,entry_date,entry_price,held,last_close,last_date)
        VALUES(?,?,?,?,?,?,?,?,'보유',?,?,0,?,?)""", [(grp, signal_date, *it, signal_date, it[5], it[5], signal_date) for it in now_])
    if commit and not _defer['on']:
        c.commit()


def _ema9_upto(cl):
    k, e = 0.2, cl[0]
    out = [e]
    for x in cl[1:]:
        e = x * k + e * (1 - k)
        out.append(e)
    return out


V62_RULE = {'buy_cost': 0.0015, 'sell_cost': 0.0015, 'slip': 0.0005, 'init': 0.6,
            'adds': ((-0.07, 0.2), (-0.12, 0.2)), 'stop': -0.20, 'tp': 0.40, 'max_days': 20}


def _v62_exit(bars, open0):
    """bars: 진입일부터의 [(date, open, high, low, close)] · 예산 1 기준 손익(%)과 청산 정보 반환 (미청산이면 None)
       SCOUT v6.2 규칙 그대로: 60% 진입 · 첫 진입가 −7%/−12%에 20%씩 · −20% 손절(손절 우선) · 평단 +40% 익절 · 최대 20거래일"""
    r = V62_RULE
    e = open0 * (1 + r['slip'])
    shares = r['init'] / e
    cost = r['init'] * (1 + r['buy_cost'])
    done = [False] * len(r['adds'])
    for n, (d, o, h, l, c) in enumerate(bars, start=1):
        for a, (lv, fr) in enumerate(r['adds']):
            lvp = open0 * (1 + lv)
            if not done[a] and l <= lvp:
                px = (o if (n >= 2 and o <= lvp) else lvp) * (1 + r['slip'])
                shares += fr / px
                cost += fr * (1 + r['buy_cost'])
                done[a] = True
        stp = open0 * (1 + r['stop'])
        if l <= stp:
            px = (o if (n >= 2 and o <= stp) else stp) * (1 - r['slip'])
            return (shares * px * (1 - r['sell_cost']) - cost) * 100, n, d, px, '−20% 재난손절'
        avg = (cost / (1 + r['buy_cost'])) / shares
        tpp = avg * (1 + r['tp'])
        if h >= tpp:
            px = (o if (n >= 2 and o >= tpp) else tpp) * (1 - r['slip'])
            return (shares * px * (1 - r['sell_cost']) - cost) * 100, n, d, px, '+40% 익절'
        if n >= r['max_days']:
            px = c * (1 - r['slip'])
            return (shares * px * (1 - r['sell_cost']) - cost) * 100, n, d, px, '20일 만기'
    return ('open', shares, cost)


def vt_evaluate(today=None):
    """일봉 기준 가상매매 갱신 — 중간에 오류가 나면 이번 갱신 전체를 되돌림(롤백)"""
    try:
        return _vt_evaluate(today)
    except Exception:
        conn().rollback()
        raise


def _vt_evaluate(today=None):
    _vt_init()
    c = conn()
    closed = []
    rows = [dict(r) for r in c.execute("SELECT * FROM vtrades WHERE status!='청산'")]
    for t in rows:
        # 신호일 이전 120봉 ~ 이후 전부 (9일 EMA를 백테스트와 같게 계산하려면 과거가 필요)
        cd = [dict(r) for r in c.execute(
            "SELECT date,open,close FROM candles WHERE ticker=? ORDER BY date", (t['ticker'],))]
        if not cd:
            continue
        dates = [x['date'] for x in cd]
        closes = [x['close'] for x in cd]
        ema = _ema9_upto(closes)
        after = [i for i, d in enumerate(dates) if d > t['signal_date']]
        close_entry = t['grp'] in CLOSE_ENTRY
        upd = {}
        if t['status'] == '대기':
            if close_entry:                             # 종가베팅: 신호일 종가에 매수
                if t['signal_date'] not in dates:
                    continue
                i0 = dates.index(t['signal_date'])
                upd.update(status='보유', entry_date=dates[i0], entry_price=closes[i0])
            else:
                if not after:
                    continue
                i0 = after[0]
                upd.update(status='보유', entry_date=dates[i0], entry_price=cd[i0]['open'] or closes[i0])
            t.update(upd)
        ep = t['entry_price']
        if not ep:
            continue
        held_idx = [i for i in (range(len(dates)) if close_entry else after) if dates[i] >= t['entry_date']]
        if t['grp'] in V62_GROUPS:
            full = [dict(r) for r in c.execute(
                "SELECT date,open,high,low,close FROM candles WHERE ticker=? AND date>=? ORDER BY date",
                (t['ticker'], t['entry_date']))]
            res = _v62_exit([(x['date'], x['open'], x['high'], x['low'], x['close']) for x in full], ep)
            if full:
                upd.update(held=min(len(full), V62_RULE['max_days']), last_close=full[-1]['close'],
                           last_date=full[-1]['date'])
            if res and res[0] != 'open':
                ret, n_, d_, px, why = res
                upd.update(status='청산', exit_date=d_, exit_price=px, exit_reason=why, held=n_, ret=round(ret, 3),
                           last_close=px, last_date=d_)
                closed.append({**t, **upd})
            elif res and full and today:
                recent = [r[0] for r in c.execute(
                    "SELECT DISTINCT date FROM candles WHERE date > ? ORDER BY date LIMIT 6", (full[-1]['date'],))]
                if len(recent) >= 5:                        # 거래 중단 → 마지막 종가로 전량 청산 (분할매수 반영)
                    _, shares, cost = res
                    px = full[-1]['close'] * (1 - V62_RULE['slip'])
                    ret = (shares * px * (1 - V62_RULE['sell_cost']) - cost) * 100
                    upd.update(status='청산', exit_date=full[-1]['date'], exit_price=px, exit_reason='거래중단 청산',
                               held=len(full), ret=round(ret, 3))
                    closed.append({**t, **upd})
            if upd:
                c.execute(f"UPDATE vtrades SET {','.join(k + '=?' for k in upd)} WHERE id=?",
                          (*upd.values(), t['id']))
            continue
        exit_i, reason = None, ''
        opens = [x['open'] or x['close'] for x in cd]
        if held_idx:
            w = _rule_walk(dates, closes, ema, held_idx[0], ep, rule_of(t['grp']))
            if w and w[0] is not None:                  # 판정 다음 거래일 시가에 매도
                exit_i, reason = w
        last_i = held_idx[-1] if held_idx else None
        if exit_i is None and last_i is not None and today:
            # 마지막 봉 이후 5거래일 넘게 데이터 없음 → 거래 중단으로 보고 마지막 종가 청산
            recent = [r[0] for r in c.execute(
                "SELECT DISTINCT date FROM candles WHERE date > ? ORDER BY date LIMIT 6", (dates[last_i],))]
            if len(recent) >= 5:
                exit_i, reason = last_i, '거래중단 청산'
        if last_i is not None:
            n_held = len([i for i in held_idx if exit_i is None or i <= exit_i])
            upd.update(held=n_held - 1 if close_entry else n_held,              # 종가 매수는 밤 수 (매수일 제외)
                       last_close=closes[last_i if exit_i is None else exit_i],
                       last_date=dates[last_i if exit_i is None else exit_i])
        if exit_i is not None:
            px = closes[exit_i] if reason == '거래중단 청산' else opens[exit_i]
            upd.update(status='청산', exit_date=dates[exit_i], exit_price=px, exit_reason=reason,
                       ret=round((px / ep - 1) * 100 - VT_COST, 3))
            if reason != '거래중단 청산':
                upd['last_close'] = px
            closed.append({**t, **upd})
        if upd:
            c.execute(f"UPDATE vtrades SET {','.join(k + '=?' for k in upd)} WHERE id=?",
                      (*upd.values(), t['id']))
    c.commit()
    return closed


VT_TRACKS = ('final', 'strategy', 'candle', 'rsi', 'fdip', 'lvflow', 'lvhigh', 'jongga', 'v62',
             'control', 'control_v62', 'control_jongga')
CONTROLS = {'control', 'control_v62', 'control_jongga'}
V62_GROUPS = {'v62', 'control_v62'}          # V6.2 자체 매매규칙으로 청산 (분할매수 · +40% 익절 · −20% 손절 · 20일)
CONTROL_OF = {'v62': 'control_v62', 'jongga': 'control_jongga'}   # 판정 시 비교할 대조군 (나머지는 'control')


def vt_stats():
    _vt_init()
    out = {}
    for grp in VT_TRACKS:
        # 손익·보유일이 비어 있는 기록이 있어도 통계가 멈추지 않도록 제외
        rows = conn().execute("SELECT ret, held FROM vtrades WHERE grp=? AND status='청산' "
                              "AND ret IS NOT NULL ORDER BY exit_date", (grp,)).fetchall()
        rs = [r[0] for r in rows]
        hd = [r[1] or 0 for r in rows]
        n = len(rs)
        if n:
            s = sorted(rs)
            eq = 1.0
            for r in rs:
                eq *= 1 + r / 100 / 30               # 종목당 계좌 1/30 가정
            out[grp] = {'n': n, 'win': sum(1 for r in rs if r > 0) / n, 'avg': sum(rs) / n,
                        'median': s[n // 2], 'best': s[-1], 'worst': s[0],
                        'hold': sum(hd) / n, 'account': (eq - 1) * 100}
        else:
            out[grp] = {'n': 0}
        out[grp]['open'] = conn().execute(
            "SELECT COUNT(*) FROM vtrades WHERE grp=? AND status!='청산'", (grp,)).fetchone()[0]
    q = ','.join('?' * len(VT_TRACKS))
    first = conn().execute(f"SELECT MIN(signal_date) FROM vtrades WHERE grp IN ({q})", VT_TRACKS).fetchone()[0]
    out['since'] = first or ''
    return out


def vt_list(limit=200, grp=None):
    _vt_init()
    if grp:
        return [dict(r) for r in conn().execute(
            "SELECT * FROM vtrades WHERE grp=? ORDER BY (status='청산'), signal_date DESC, rank LIMIT ?",
            (grp, limit))]
    return [dict(r) for r in conn().execute(
        "SELECT * FROM vtrades WHERE grp!='control' ORDER BY (status='청산'), signal_date DESC, grp, rank LIMIT ?",
        (limit,))]


def vt_closed_on(date, grp='strategy'):
    _vt_init()
    return [dict(r) for r in conn().execute(
        "SELECT * FROM vtrades WHERE grp=? AND status='청산' AND exit_date=? ORDER BY ret DESC", (grp, date))]


def vt_open_rows(grp='strategy'):
    _vt_init()
    return [dict(r) for r in conn().execute(
        "SELECT * FROM vtrades WHERE grp=? AND status!='청산' ORDER BY signal_date, rank", (grp,))]


# ════════════════════════════════════════════
#  실전 가상매매 자동 판정 — 기준은 처음 실행 때 확정 저장 (결과 보고 바꾸지 않기)
# ════════════════════════════════════════════
VT_CRITERIA = {
    'min_trades': 60,          # 청산 60건 이상
    'min_days': 40,            # 신호일 40거래일 이상
    'alpha': 0.05,             # 전체 오판 확률 5% — 비교 모델 수만큼 나눠서 기준 강화 (본페로니)
    'concentration': 0.5,      # 상위 2건이 전체 수익의 50% 이상이면 '쏠림'
    'review_months': 4,        # 4개월 지나도 '보류'면 효과 없음으로 판단
}


def _z_one_sided(p):
    """표준정규 단측 임계값 (p=0.05 → 1.645)"""
    import math
    lo, hi = 0.0, 10.0
    for _ in range(80):
        mid = (lo + hi) / 2
        if 0.5 * math.erfc(mid / math.sqrt(2)) > p:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def vt_criteria():
    """최초 1회 확정 저장 후 계속 같은 기준 사용"""
    raw = meta_get('vt_criteria', '')
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    crit = dict(VT_CRITERIA, fixed_at=datetime.now().strftime('%Y-%m-%d'))
    meta_set('vt_criteria', json.dumps(crit, ensure_ascii=False))
    return crit


def vt_judge(expected_sign, expected_num=None):
    """expected_sign: {track: +1/-1/0} 백테스트상 대조군 대비 방향
       expected_num : {track: {'win','hold','avg'}} 백테스트 기대치 (재현성 점검용)
       반환: {track: {...판정}}, 기준"""
    expected_num = expected_num or {}
    import math
    _vt_init()
    crit = vt_criteria()
    tracks = [g for g in VT_TRACKS if g not in CONTROLS]
    m = max(1, len(tracks))
    z_need = _z_one_sided(crit['alpha'] / m)          # 모델이 많을수록 엄격
    c = conn()

    def closed(g):
        return [dict(r) for r in c.execute(
            "SELECT signal_date, ret, held FROM vtrades WHERE grp=? AND status='청산' AND ret IS NOT NULL", (g,))]
    ctrl_cache = {}
    cd_cache = {}

    def control_under(rule):
        """대조군 종목을 이 모델의 청산 규칙으로 다시 계산 (종목·매수는 그대로 → 공정한 짝 비교)"""
        out = []
        for r in c.execute("SELECT ticker, signal_date, entry_date, entry_price FROM vtrades "
                           "WHERE grp='control' AND entry_date IS NOT NULL AND entry_price>0"):
            tk = r['ticker']
            if tk not in cd_cache:
                rows_ = [dict(x) for x in c.execute("SELECT date, open, close FROM candles WHERE ticker=? ORDER BY date", (tk,))]
                ds = [x['date'] for x in rows_]
                cl = [x['close'] for x in rows_]
                cd_cache[tk] = (ds, [x['open'] or x['close'] for x in rows_], cl, _ema9_upto(cl) if cl else [])
            ds, op, cl, em = cd_cache[tk]
            if r['entry_date'] not in ds:
                continue
            i0 = ds.index(r['entry_date'])
            w = _rule_walk(ds, cl, em, i0, r['entry_price'], rule)
            if w and w[0] is not None:
                out.append({'signal_date': r['signal_date'], 'ret': (op[w[0]] / r['entry_price'] - 1) * 100 - VT_COST,
                            'held': w[0] - i0 + 1})
        return out

    def control_for(g):
        cg = CONTROL_OF.get(g, 'control')
        if cg == 'control' and rule_of(g) != rule_of('control'):
            key = 'control@' + g
            if key not in ctrl_cache:
                rows = control_under(rule_of(g))
                by = {}
                for r in rows:
                    by.setdefault(r['signal_date'], []).append(r['ret'])
                ctrl_cache[key] = (by, [r['ret'] for r in rows])
            return ctrl_cache[key]
        if cg not in ctrl_cache:
            rows = closed(cg)
            by = {}
            for r in rows:
                by.setdefault(r['signal_date'], []).append(r['ret'])
            ctrl_cache[cg] = (by, [r['ret'] for r in rows])
        return ctrl_cache[cg]
    out = {}
    for g in tracks:
        ctrl_by, all_ctrl = control_for(g)
        tr = closed(g)
        n = len(tr)
        days = len({r['signal_date'] for r in tr})
        first = c.execute("SELECT MIN(signal_date) FROM vtrades WHERE grp=?", (g,)).fetchone()[0]
        res = {'n': n, 'days': days, 'z_need': round(z_need, 2), 'models': m}
        if not tr or not all_ctrl:
            out[g] = {**res, 'verdict': '표본 부족', 'why': '아직 청산된 거래가 없습니다',
                      'repro': '표본 부족', 'repro_why': '', 'need_months': None, 'excess': None}
            continue
        # 같은 신호일끼리 짝지어 비교 (시장 상황 영향 제거)
        diffs = []
        by = {}
        for r in tr:
            by.setdefault(r['signal_date'], []).append(r['ret'])
        for d, v in by.items():
            if d in ctrl_by:
                diffs.append(sum(v) / len(v) - sum(ctrl_by[d]) / len(ctrl_by[d]))
        avg = sum(r['ret'] for r in tr) / n
        c_avg = sum(all_ctrl) / len(all_ctrl)
        excess = sum(diffs) / len(diffs) if diffs else avg - c_avg
        hold = sum((r['held'] or 1) for r in tr) / n
        t = 0.0
        if len(diffs) >= 3:
            mu = sum(diffs) / len(diffs)
            sd = math.sqrt(sum((x - mu) ** 2 for x in diffs) / (len(diffs) - 1))
            if sd > 0:
                # 매일 새로 사고 며칠씩 보유 → 인접 신호일 결과가 겹침. 평균 보유일만큼 보수적으로 보정
                t = mu / (sd / math.sqrt(len(diffs))) / math.sqrt(max(1.0, hold))
        pos = sorted((r['ret'] for r in tr), reverse=True)
        total_pos = sum(x for x in pos if x > 0)
        conc = (sum(x for x in pos[:2] if x > 0) / total_pos) if total_pos > 0 else 0
        exp = expected_sign.get(g, 0)
        res.update(avg=round(avg, 3), control_avg=round(c_avg, 3), excess=round(excess, 3),
                   t=round(t, 2), hold=round(hold, 1), concentration=round(conc, 2), expected=exp)
        months = 0
        if first:
            fd = datetime.strptime(first, '%Y%m%d')
            months = (datetime.now() - fd).days / 30.4
        if n < crit['min_trades'] or days < crit['min_days']:
            v, why = '표본 부족', f"청산 {n}/{crit['min_trades']}건 · 신호일 {days}/{crit['min_days']}일"
        elif excess > 0 and t >= z_need and exp >= 0 and conc < crit['concentration']:
            v, why = '합격', f"대조군보다 건당 {excess:+.2f}%p · t {t:.2f} ≥ {z_need:.2f} · 백테스트와 같은 방향"
        elif excess < 0 and t <= -z_need:
            v, why = '탈락', f"대조군보다 건당 {excess:+.2f}%p로 확실히 낮음 (t {t:.2f})"
        elif excess > 0 and t >= 1.0:
            reason = []
            if t < z_need:
                reason.append(f"통계 기준 미달 (t {t:.2f} < {z_need:.2f})")
            if exp < 0:
                reason.append('백테스트와 반대 방향 — 운 가능성 먼저 의심')
            if conc >= crit['concentration']:
                reason.append(f"상위 2건 쏠림 {conc:.0%}")
            v, why = '유망 (연장)', ' · '.join(reason) or '기준 근접'
        else:
            v, why = '보류', f"대조군과 뚜렷한 차이 없음 (건당 {excess:+.2f}%p · t {t:.2f})"
            if months >= crit['review_months']:
                v, why = '탈락', f"{crit['review_months']}개월이 지나도 대조군과 차이 없음 — 효과 없음으로 판단"
        # ── 확인에 필요한 기간 (지금 보이는 차이가 진짜라고 가정)
        need_months = None
        if len(diffs) >= 3 and excess != 0:
            mu = sum(diffs) / len(diffs)
            sd = math.sqrt(sum((x - mu) ** 2 for x in diffs) / (len(diffs) - 1))
            if sd > 0:
                need_days = (z_need * sd * math.sqrt(max(1.0, hold)) / abs(excess)) ** 2
                need_months = round(need_days / 21, 1)
        # ── 재현성: 실전이 백테스트 예상 범위 안에서 움직이는가 (두 달이면 이게 가장 쓸모 있는 확인)
        repro, repro_why = '표본 부족', ''
        e = expected_num.get(g)
        if e and n >= 30:
            rets = [r['ret'] for r in tr]
            mu_r = sum(rets) / n
            sd_r = math.sqrt(sum((x - mu_r) ** 2 for x in rets) / max(1, n - 1))
            win = sum(1 for x in rets if x > 0) / n
            tol_avg = 2 * sd_r / math.sqrt(n)                  # 표본 오차의 2배
            issues = []
            if abs(win - e['win']) > 0.12:
                issues.append(f"승률 {win:.0%} (예상 {e['win']:.0%})")
            if abs(hold - e['hold']) > 2.0:
                issues.append(f"보유 {hold:.1f}일 (예상 {e['hold']:.1f}일)")
            if abs(mu_r - e['avg']) > tol_avg:
                issues.append(f"건당 {mu_r:+.2f}% (예상 {e['avg']:+.2f}% ± {tol_avg:.2f})")
            repro = '백테스트와 비슷' if not issues else '백테스트와 다름'
            repro_why = ('승률·보유기간·건당 수익 모두 예상 범위 안' if not issues
                         else ' · '.join(issues) + ' → 구현 오류 또는 시장 변화 점검')
        out[g] = {**res, 'verdict': v, 'why': why, 'need_months': need_months,
                  'repro': repro, 'repro_why': repro_why}
    return out, crit



def flow_rows(dates, investor):
    """{ticker: {date: 금액}} — V6.2 트랙용 (종목별 일자 정렬)"""
    if not dates:
        return {}
    q = ','.join('?' * len(dates))
    out = {}
    for tk, d, a in conn().execute(f"SELECT ticker, date, amt FROM flows WHERE investor=? AND date IN ({q})",
                                   (investor, *dates)):
        out.setdefault(tk, {})[d] = a
    return out


def candles_since(date_from):
    """전 종목 일봉 (date_from 이후) — {ticker: [봉...]}"""
    out = {}
    q = ','.join('?' * len(BENCH_TICKERS))                  # 비교용 지수 ETF는 빼고 (V6.2 전 종목 순위가 바뀌지 않게)
    for r in conn().execute(f"SELECT ticker,date,open,high,low,close,volume FROM candles WHERE date>=? "
                            f"AND ticker NOT IN ({q}) ORDER BY ticker,date", (date_from, *BENCH_TICKERS)):
        out.setdefault(r[0], []).append({'date': r[1], 'open': r[2], 'high': r[3], 'low': r[4],
                                         'close': r[5], 'volume': r[6]})
    return out



# ════════════════════════════════════════════
#  백업 · 데이터 품질 · 계좌 단위 가상운용 (v3.8 — SCOUT v6.3 장점 도입)
# ════════════════════════════════════════════
BACKUP_KEEP = 120            # 하루 약 4개 → 한 달치


def vt_backup(tag='auto'):
    """가상매매 기록 전체를 압축 백업 (최근 60개 보관). 반환: 파일 경로"""
    import gzip
    _vt_init()
    d = os.path.join(DATA_DIR, 'backups')
    os.makedirs(d, exist_ok=True)
    rows = [dict(r) for r in conn().execute("SELECT * FROM vtrades")]
    meta = {k: meta_get(k, '') for k in ('vt_last_batch', 'vt_criteria', 'vt_rsi_rule')}
    path = os.path.join(d, f"vtrades_{datetime.now():%Y%m%d_%H%M%S_%f}_{tag}.json.gz")
    with gzip.open(path, 'wt', encoding='utf-8') as f:
        json.dump({'rows': rows, 'meta': meta, 'at': datetime.now().isoformat()}, f, ensure_ascii=False)
    files = sorted((x for x in os.listdir(d) if x.startswith('vtrades_')),
                   key=lambda x: os.path.getmtime(os.path.join(d, x)))       # 저장 시각 순 (시계·이름에 의존 안 함)
    for old in files[:-BACKUP_KEEP]:
        try:
            os.remove(os.path.join(d, old))
        except OSError:
            pass
    return path


def vt_restore(path):
    """백업 파일로 가상매매 기록 복원 (현재 기록은 복원 직전 자동 백업)"""
    import gzip
    vt_backup('before_restore')
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        data = json.load(f)
    c = conn()
    try:
        c.execute("DELETE FROM vtrades")
        if data['rows']:
            cols = list(data['rows'][0].keys())
            c.executemany(f"INSERT INTO vtrades ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
                          [tuple(r[k] for k in cols) for r in data['rows']])
        for k, v in data.get('meta', {}).items():
            if v:
                c.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (k, v))
        c.commit()
    except Exception:
        c.rollback()
        raise
    return len(data['rows'])


def latest_backup():
    d = os.path.join(DATA_DIR, 'backups')
    if not os.path.isdir(d):
        return None
    files = [os.path.join(d, x) for x in os.listdir(d) if x.startswith('vtrades_')]
    return max(files, key=os.path.getmtime) if files else None


def data_quality(date, krx_configured=False):
    """그날 데이터가 가상매매에 쓸 만큼 온전한지 점검
       · 후보풀 일봉 수집률 90% 이상 · 비정상 봉(가격제한 초과 급변·고가<저가 등) 2% 이하
       · KRX 계정이 있으면 외국인·연기금 수급이 그날 날짜로 들어왔는지"""
    c = conn()
    pool = [r[0] for r in c.execute("SELECT ticker FROM stocks WHERE in_pool=1")]
    n = len(pool)
    if not n:
        return {'date': date, 'status': 'HOLD', 'why': '후보풀 없음', 'candle_ratio': 0}
    q = ','.join('?' * n)
    today = {r[0]: r for r in c.execute(
        f"SELECT ticker, open, high, low, close, volume FROM candles WHERE date=? AND ticker IN ({q})", (date, *pool))}
    prev = {r[0]: r[1] for r in c.execute(
        f"""SELECT c.ticker, c.close FROM candles c JOIN (SELECT ticker, MAX(date) d FROM candles
            WHERE date<? AND ticker IN ({q}) GROUP BY ticker) m ON c.ticker=m.ticker AND c.date=m.d""", (date, *pool))}
    bad = 0
    for tk, r in today.items():
        _, o, h, l, cl, v = r
        if not cl or cl <= 0 or h < l or (o and (o > h * 1.001 or o < l * 0.999)):
            bad += 1
        elif prev.get(tk) and abs(cl / prev[tk] - 1) > 0.31:       # 가격제한폭(±30%) 초과 = 단위·보정 이상
            bad += 1
    ratio = len(today) / n
    flows_ok = None
    if krx_configured:
        got = {r[0] for r in c.execute("SELECT DISTINCT investor FROM flows WHERE date=?", (date,))}
        flows_ok = {'외국인', '연기금'} <= got
    why = []
    if ratio < 0.9:
        why.append(f"일봉 수집률 {ratio:.0%} (기준 90%)")
    if today and bad / len(today) > 0.02:
        why.append(f"비정상 봉 {bad}건 ({bad / len(today):.1%})")
    if flows_ok is False:
        why.append('그날 외국인·연기금 수급 미수집')
    res = {'date': date, 'pool': n, 'candle_ratio': round(ratio, 3), 'bad_bars': bad,
           'flows_ok': flows_ok, 'status': 'HOLD' if why else 'OK', 'why': ' · '.join(why) or '정상',
           'checked_at': datetime.now().isoformat(timespec='seconds')}
    meta_set('dq_last', json.dumps(res, ensure_ascii=False))
    return res


def last_quality():
    try:
        return json.loads(meta_get('dq_last', '') or '{}')
    except Exception:
        return {}


ACCT = {'cash': 10_000_000, 'slots': 20, 'buy_cost': 0.00035, 'sell_cost': 0.00215, 'min_order': 50_000, 'order_max': {}}
CAP_OF = {'control': 'final', 'control_v62': 'v62', 'control_jongga': 'jongga'}      # 대조군은 비교하는 모델과 같은 금액
RT_COST = {'v62': 0.40, 'control_v62': 0.40}           # 왕복 비용 % — V6.2 자체 규칙(매수·매도 0.15% + 슬리피지 0.05%씩), 나머지 VT_COST


def rt_cost(grp):
    """보유 중 종목을 지금 판다고 가정할 때 빼는 왕복 비용 (%) — 청산 손익과 같은 기준"""
    return RT_COST.get(grp, VT_COST)


def order_cap(grp):
    """모델별 1회 최대 주문금액(원) — 없으면 None(제한 없음). 대조군은 비교 대상 모델의 금액"""
    v = (ACCT.get('order_max') or {}).get(CAP_OF.get(grp, grp))
    return float(v) if v else None


def vt_account(grp='final'):
    """가상매매 기록을 1,000만 원 계좌로 재생 — 정수 주식 · 종목당 min(평가액의 1/20, 모델별 1회 최대 주문금액)
       · 수수료·세금·슬리피지(왕복 약 0.25%, 보유 중 종목은 매도 비용을 미리 뺀 평가) · 매수는 기록된 매수가(시가), 매도는 기록된 매도가.
       현금이 부족하거나 1주도 못 사는 신호는 건너뜀. 기록에서 매번 다시 계산하므로 상태가 꼬이지 않음"""
    _vt_init()
    c = conn()
    trades = [dict(r) for r in c.execute(
        "SELECT * FROM vtrades WHERE grp=? AND entry_date IS NOT NULL ORDER BY entry_date, rank", (grp,))]
    if not trades:
        return {'curve': [], 'stats': {'trades': 0}}
    start = trades[0]['entry_date']
    dates = [r[0] for r in c.execute("SELECT DISTINCT date FROM candles WHERE date>=? ORDER BY date", (start,))]
    tks = sorted({t['ticker'] for t in trades})
    q = ','.join('?' * len(tks))
    px = {}
    for tk, d, cl in c.execute(f"SELECT ticker,date,close FROM candles WHERE date>=? AND ticker IN ({q})", (start, *tks)):
        px[(tk, d)] = cl
    cash, pos = float(ACCT['cash']), {}           # pos: trade id -> (ticker, shares)
    by_entry, by_exit = {}, {}
    for t in trades:
        by_entry.setdefault(t['entry_date'], []).append(t)
        if t['status'] == '청산' and t['exit_date']:
            by_exit.setdefault(t['exit_date'], []).append(t)
    last = {}
    curve, skipped, realized, invested = [], 0, 0.0, []
    eq_prev = cash
    cap = order_cap(grp)
    exits_first = grp in CLOSE_ENTRY               # 종가베팅: 아침 시가에 어제 산 종목 매도 → 오후 종가에 새로 매수
    for d in dates:
        size = eq_prev / ACCT['slots']
        if cap:
            size = min(size, cap)

        def _sells():
            nonlocal cash, realized
            for t in by_exit.get(d, []):
                if t['id'] in pos:
                    tk, sh, cost = pos.pop(t['id'])
                    proceeds = sh * t['exit_price'] * (1 - ACCT['sell_cost'])
                    cash += proceeds
                    realized += proceeds - cost

        if exits_first:
            _sells()
        for t in by_entry.get(d, []):
            price = t['entry_price']
            if not price:
                continue
            amt = min(size, cash)
            sh = int(amt // (price * (1 + ACCT['buy_cost'])))
            if sh <= 0 or amt < ACCT['min_order']:
                skipped += 1
                continue
            cost = sh * price * (1 + ACCT['buy_cost'])
            cash -= cost
            pos[t['id']] = (t['ticker'], sh, cost)
        if not exits_first:
            _sells()
        mv = 0.0
        for tid, (tk, sh, cost) in pos.items():
            p_ = px.get((tk, d)) or last.get(tk) or (cost / sh)
            last[tk] = p_
            mv += sh * p_ * (1 - ACCT['sell_cost'])          # 지금 판다면 받을 금액 (매도 비용 미리 뺌)
        eq = cash + mv
        invested.append(mv / eq if eq > 0 else 0)
        curve.append({'date': d, 'equity': round(eq), 'cash': round(cash), 'positions': len(pos)})
        eq_prev = eq
    peak, mdd = 0, 0.0
    for p_ in curve:
        peak = max(peak, p_['equity'])
        mdd = min(mdd, p_['equity'] / peak - 1)
    eq_end = curve[-1]['equity'] if curve else ACCT['cash']
    return {'curve': curve,
            'stats': {'trades': len(trades), 'skipped': skipped, 'start': start,
                      'equity': eq_end, 'return': round((eq_end / ACCT['cash'] - 1) * 100, 2),
                      'mdd': round(mdd * 100, 2), 'realized': round(realized),
                      'invested': round(sum(invested) / len(invested) * 100, 1) if invested else 0,
                      'positions': len(pos), 'cash': round(cash)}}


def vt_curve(grp, cap_grp=None):
    """모델별 가상 계좌 곡선 (수익률 기준 · 모든 모델 공통) — 종목당 min(평가액의 1/20, 모델별 1회 최대 주문금액) 배정,
       청산 시 기록된 손익률(비용 반영, V6.2는 분할매수 포함)로 실현, 보유 중은 종가로 평가하되 왕복 비용을 미리 뺌.
       cap_grp: 다른 모델의 금액으로 계산 (모델 상세에서 대조군을 그 모델 금액으로 비교할 때)"""
    _vt_init()
    c = conn()
    tr = [dict(r) for r in c.execute(
        "SELECT * FROM vtrades WHERE grp=? AND entry_date IS NOT NULL AND entry_price>0 ORDER BY entry_date, rank", (grp,))]
    if not tr:
        return {'curve': [], 'stats': {'trades': 0}}
    dates = [r[0] for r in c.execute("SELECT DISTINCT date FROM candles WHERE date>=? ORDER BY date", (tr[0]['entry_date'],))]
    tks = sorted({t['ticker'] for t in tr})
    q = ','.join('?' * len(tks))
    px = {(a, b): v for a, b, v in c.execute(
        f"SELECT ticker,date,close FROM candles WHERE date>=? AND ticker IN ({q})", (tr[0]['entry_date'], *tks))}
    ent, ext = {}, {}
    for t in tr:
        ent.setdefault(t['entry_date'], []).append(t)
        if t['status'] == '청산' and t['ret'] is not None:
            ext.setdefault(t['exit_date'], []).append(t)
    cash, pos, last, curve, eq_prev = 1.0, {}, {}, [], 1.0
    capf = (order_cap(cap_grp or grp) or 0) / ACCT['cash']        # 시작 금액 대비 비율
    cst = rt_cost(grp) / 100                                      # 보유 중 종목도 비용을 뺀 값으로 평가
    exits_first = grp in CLOSE_ENTRY               # 종가베팅: 아침 시가 매도 → 종가 매수 순서
    for d in dates:
        if exits_first:
            for t in ext.get(d, []):
                if t['id'] in pos:
                    _, size, _ = pos.pop(t['id'])
                    cash += size * (1 + t['ret'] / 100)
        for t in ent.get(d, []):
            size = min(eq_prev / ACCT['slots'], cash)
            if capf > 0:
                size = min(size, capf)
            if size <= 0:
                continue
            cash -= size
            pos[t['id']] = (t['ticker'], size, t['entry_price'])
        if not exits_first:
            for t in ext.get(d, []):
                if t['id'] in pos:
                    _, size, _ = pos.pop(t['id'])
                    cash += size * (1 + t['ret'] / 100)
        mv = 0.0
        for tid, (tk, size, ep) in pos.items():
            p_ = px.get((tk, d)) or last.get(tk) or ep
            last[tk] = p_
            mv += size * (p_ / ep - cst)
        eq_prev = cash + mv
        curve.append({'date': d, 'equity': round(eq_prev * ACCT['cash'])})
    peak, mdd = 0, 0.0
    for p_ in curve:
        peak = max(peak, p_['equity'])
        mdd = min(mdd, p_['equity'] / peak - 1)
    return {'curve': curve, 'stats': {'trades': len(tr), 'equity': curve[-1]['equity'] if curve else ACCT['cash'],
                                      'return': round((eq_prev - 1) * 100, 2), 'mdd': round(mdd * 100, 2)}}
