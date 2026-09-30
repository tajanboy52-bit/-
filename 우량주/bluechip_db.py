"""
bluechip_db.py — 💎 TK Bluechip 우량주 반등 시스템 저장소 (SQLite)
데이터 폴더: %APPDATA%\\TKBluechip (BLUECHIP_DATA 환경변수로 바꿀 수 있음)
Scout 일봉은 읽기 전용으로만 봄 (Scout 파일은 절대 바꾸지 않음)
"""
import os
import sqlite3
import threading
from datetime import datetime

DATA_DIR = os.environ.get('BLUECHIP_DATA') or os.path.join(os.environ.get('APPDATA') or os.path.expanduser('~'), 'TKBluechip')
os.makedirs(DATA_DIR, exist_ok=True)
DB_FILE = os.path.join(DATA_DIR, 'bluechip.db')

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS members (           -- 매월 첫 거래일 코스피200 · 코스닥150 구성 종목 (KRX)
    month TEXT, ticker TEXT, idx TEXT, PRIMARY KEY (month, ticker));
CREATE TABLE IF NOT EXISTS monthly (           -- 매월 첫 거래일 스냅샷: 시장 · 업종 · 시가총액 · EPS · 배당 (KRX)
    month TEXT, date TEXT, ticker TEXT, market TEXT, name TEXT, sector TEXT, marcap REAL, eps REAL, div REAL,
    PRIMARY KEY (month, ticker));
CREATE TABLE IF NOT EXISTS universe (          -- 그달의 우량주 100 (편입 1년 · 흑자 · 배당 → 60일 변동성 낮은 100)
    month TEXT, rank INTEGER, ticker TEXT, name TEXT, market TEXT, sector TEXT, vol60 REAL, marcap REAL, eps REAL, div REAL,
    tenure INTEGER, PRIMARY KEY (month, ticker));
CREATE TABLE IF NOT EXISTS daily (             -- 날짜별 우량주 100 상태 (신호 판단 재료 · 화면용)
    date TEXT, ticker TEXT, close REAL, ma20gap REAL, r5 REAL, r20 REAL, rsi2 REAL, score REAL, sigA INTEGER, sigB INTEGER,
    PRIMARY KEY (date, ticker));
CREATE TABLE IF NOT EXISTS market (            -- 날짜별 시장 상태 (공포 온도 · 지수)
    date TEXT PRIMARY KEY, breadth REAL, mkt_r5 REAL, mkt_vol REAL, idx_close REAL, n_members INTEGER);
CREATE TABLE IF NOT EXISTS orders (            -- 신호 → 다음 거래일 시가 매수 예약
    id INTEGER PRIMARY KEY AUTOINCREMENT, model TEXT, signal_date TEXT, ticker TEXT, name TEXT, sector TEXT,
    prio REAL, score REAL, ma20gap REAL, status TEXT, note TEXT, trade_id INTEGER);
CREATE TABLE IF NOT EXISTS trades (            -- 가상 매매 (모델별 계좌)
    id INTEGER PRIMARY KEY AUTOINCREMENT, model TEXT, ticker TEXT, name TEXT, sector TEXT, signal_date TEXT,
    entry_date TEXT, entry_px REAL, qty INTEGER, tp_px REAL, status TEXT, days INTEGER DEFAULT 0, last_px REAL,
    exit_date TEXT, exit_px REAL, exit_reason TEXT, ret REAL, pnl REAL, score REAL, ma20gap REAL, rules TEXT);
CREATE TABLE IF NOT EXISTS equity (            -- 모델별 가상 계좌 종가 평가
    date TEXT, model TEXT, cash REAL, value REAL, npos INTEGER, PRIMARY KEY (date, model));
CREATE TABLE IF NOT EXISTS days (date TEXT PRIMARY KEY, done_at TEXT, note TEXT);
CREATE TABLE IF NOT EXISTS flows (             -- 종목별 투자자 순매수 (백만원) · 2023-09~ 초기 자료 + 매일 Scout(읽기 전용)에서 가져옴
    date TEXT, ticker TEXT, fo REAL, ins REAL, pen REAL, PRIMARY KEY (date, ticker));
CREATE INDEX IF NOT EXISTS idx_flows_tk ON flows(ticker, date);
CREATE TABLE IF NOT EXISTS flowsnap (          -- 날짜별 우량주 100 수급 요약 (신호 · 매매 기록용 · ticker '_MKT' = 구성 종목 전체)
    date TEXT, ticker TEXT, flow_date TEXT, fo1 REAL, ins1 REAL, pen1 REAL, fo5 REAL, ins5 REAL, pen5 REAL, fo20 REAL, ins20 REAL,
    tv20 REAL, sm5r REAL, PRIMARY KEY (date, ticker));
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT, started TEXT, ended TEXT, status TEXT, msg TEXT);
"""
_local = threading.local()


def conn():
    c = getattr(_local, 'c', None)
    if c is None:
        c = sqlite3.connect(DB_FILE, timeout=30, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute('PRAGMA journal_mode=WAL')
        c.executescript(SCHEMA)
        have = {r[1] for r in c.execute('PRAGMA table_info(trades)')}
        for col, typ in (('part_qty', 'INTEGER DEFAULT 0'), ('part_px', 'REAL'), ('peak', 'REAL'), ('sell_next', 'INTEGER DEFAULT 0')):
            if col not in have:                            # B0.4 H1 모델용 열 추가
                c.execute(f'ALTER TABLE trades ADD COLUMN {col} {typ}')
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


def run_start(kind):
    c = conn()
    c.execute("INSERT INTO runs (kind, started, status) VALUES (?, ?, 'running')", (kind, datetime.now().isoformat(timespec='seconds')))
    c.commit()
    return c.execute('SELECT last_insert_rowid()').fetchone()[0]


def run_end(rid, status, msg):
    c = conn()
    c.execute('UPDATE runs SET ended=?, status=?, msg=? WHERE id=?', (datetime.now().isoformat(timespec='seconds'), status, str(msg)[:500], rid))
    c.commit()


# ════════════════════════════════════════════
#  Scout 일봉 (읽기 전용)
# ════════════════════════════════════════════
def scout_db_path():
    p = os.environ.get('SCOUT_DATA') or os.path.join(os.environ.get('APPDATA') or os.path.expanduser('~'), 'StockScout')
    f = os.path.join(p, 'scout.db')
    return f if os.path.exists(f) else ''


def scout():
    f = scout_db_path()
    if not f:
        return None
    return sqlite3.connect(f'file:{f}?mode=ro', uri=True, timeout=30)
