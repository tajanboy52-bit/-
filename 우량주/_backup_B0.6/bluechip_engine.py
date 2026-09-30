"""
bluechip_engine.py — 💎 TK Bluechip 우량주 반등 엔진 (우량주 100 · 신호 · 가상 매매)

검증 근거: claude/우량주100_반등모델_검증.md (KRX 2019-01 ~ 2026-09 · 코스피200 · 코스닥150 실제 구성 종목)
· 우량주 100: 매월 첫 거래일 구성 종목 중 편입 1년 이상 · 흑자(EPS>0) · 배당 있음 → 60일 변동성 낮은 100
· 매수: 신호일 다음 거래일 시가 · 매도: +5% 닿으면 그 값(시가가 이미 넘으면 시가) · 최대 40거래일이면 그날 종가 · 손절 없음
· 계좌: 모델마다 1,000만 원 · 종목당 = 전날 평가액 ÷ 10 · 최대 14종목 · 업종당 2 · 정수 주 · 비용 왕복 0.25%
· 모델 A 단순형: 종가 ≤ 20일선 × 0.90 · 많이 빠진 순
· 모델 B TK 반등지수: 후보(20일선 −7% · 5일 −5% · RSI(2) ≤ 10) 중 TK 점수 ≥ 점수선 · 점수 높은 순
· 대조군 Z: A 신호가 난 날, A 신호 수만큼 우량주 100에서 무작위 (같은 매도 규칙) — 모델이 '아무거나 산 것'보다 나은지
일봉은 Scout(scout.db)를 읽기 전용으로 사용 · 모든 판단은 장 마감 뒤 확정 일봉으로
· 수급(B0.2): 외국인 · 기관 · 연기금 순매수 — 2023-09~2026-09 초기 자료(seed/flows.csv.gz) + 매일 Scout flows(18:10 확정치, 읽기 전용)
  2023-10~2026-09 검증에서 매수 규칙을 좋게 만들지 못해 (같은 날 후보끼리 비교 t≈0) 매수 판단에는 쓰지 않고,
  모든 신호 · 매매에 그날 수급을 기록 → 실전 거래가 쌓이면 다시 검증
"""
import bisect
import csv
import gzip
import json
import math
import os
import random
import sys
import time
from datetime import datetime

import numpy as np
import pandas as pd

import bluechip_db as db

CAP0 = 10_000_000
SLOTS, MAXPOS, SECCAP = 10, 14, 2
TP, HOLD, COST = 5.0, 40, 0.25
IDX = '069500'                      # KODEX 200 (코스피200 지수 대신 · Scout 일봉)
MODELS = {
    'A': {'name': '단순형', 'desc': '종가가 20일선보다 10% 이상 아래 → 다음날 시가 매수 · 많이 빠진 순',
          'evidence': '2020~2026 백테스트(1000만 · 10종목): 연 12.1% · 최대 낙폭 −36%(2020.3) · 승률 82% · 2023~ 연 17.0% · 최대 낙폭 −12%'},
    'B': {'name': 'TK 반등지수', 'desc': '후보(20일선 −7% · 5일 −5% · RSI2 ≤ 10) 중 TK 점수 ≥ 점수선 → 다음날 시가 매수 · 점수 높은 순',
          'evidence': '재료 5개(종목 눌림 · 5일 하락 · 시장 공포 온도 · 지수 5일 · 지수 변동성) · 전진 검증 건당 +2.89% vs 단순형 +2.21% · 계좌는 단순형이 앞섬(14.6% vs 11.4%) → 실전으로 판가름'},
    'H': {'name': 'H1 히든 (추적 · 재난손절)', 'desc': 'A와 같은 신호 · +5%에서 30% 익절 · 나머지는 종가가 최고 종가 −4%면 다음날 시가 매도 · −15% 재난 손절 · 최대 40일 · KIS 모의투자로 실제 주문하는 모델',
          'evidence': '2020~2026 백테스트: 연 14.8% · 최대 낙폭 −33.5% (A 12.1% · −36.3%) · 2023~ 22.8% · 순서 20가지 평균 +2.4%p · 반등이 긴 해에 강하고 2022 같은 오르내림 장에 약함 (claude/TK히든모델_H1_설계.md)'},
    'Z': {'name': '대조군 (무작위)', 'desc': 'A 신호가 난 날 같은 수만큼 우량주 100에서 무작위 · 같은 매도 규칙',
          'evidence': '같은 규칙 무작위 백테스트: 연 7.5% · 최대 낙폭 −33% — 모델이 이것보다 나아야 의미 있음'},
}
ORDER = ['A', 'B', 'H', 'Z']
H_PART, H_TRAIL, H_STOP, H_SLIP = 0.30, 4.0, 15.0, 0.2    # H1: 1차 익절 비율 · 추적 폭 % · 재난 손절 % · 손절 미끄러짐 %
# TK 반등지수 (2020-02 ~ 2026-09 우량주 100 후보 30,916건으로 학습 · Ridge · 결과 = 익절 5% · 40일)
TK = {"cols": ["depth", "r5", "breadth", "mkt_r5", "mkt_vol"],
      "mean": {"depth": -0.05356, "r5": -0.056076, "breadth": 0.667271, "mkt_r5": -0.014932, "mkt_vol": 0.267102},
      "std": {"depth": 0.04318, "r5": 0.035793, "breadth": 0.188644, "mkt_r5": 0.04877, "mkt_vol": 0.194208},
      "lo": {"depth": -0.217003, "r5": -0.194588, "breadth": 0.162857, "mkt_r5": -0.160227, "mkt_vol": 0.085713},
      "hi": {"depth": 0.042597, "r5": 0.020737, "breadth": 0.971429, "mkt_r5": 0.146409, "mkt_vol": 1.005773},
      "coef": {"depth": -0.303117, "r5": -0.23743, "breadth": 0.304453, "mkt_r5": -0.311947, "mkt_vol": 0.457708},
      "intercept": 1.210724, "threshold": 2.5105}
TK_PCT = [-1.0571, -0.495, -0.3674, -0.2827, -0.2184, -0.1556, -0.1072, -0.0665, -0.0239, 0.0117, 0.0483, 0.0829, 0.121, 0.1567, 0.184, 0.2163,
          0.2432, 0.273, 0.2975, 0.3212, 0.3469, 0.3714, 0.3952, 0.4199, 0.4429, 0.4664, 0.4898, 0.513, 0.5341, 0.5549, 0.5781, 0.5984, 0.6197,
          0.6422, 0.6647, 0.6861, 0.7091, 0.7291, 0.7499, 0.7728, 0.7935, 0.8143, 0.8365, 0.8572, 0.8778, 0.9022, 0.9222, 0.945, 0.9677, 0.9899,
          1.0099, 1.0316, 1.0545, 1.0798, 1.1044, 1.1278, 1.1523, 1.1774, 1.2014, 1.2261, 1.2498, 1.2742, 1.3007, 1.3258, 1.3555, 1.3808, 1.4062,
          1.4344, 1.4631, 1.492, 1.5226, 1.5532, 1.5837, 1.6182, 1.652, 1.6877, 1.7286, 1.7669, 1.8068, 1.8493, 1.8925, 1.9435, 1.9979, 2.0552,
          2.1178, 2.1905, 2.2686, 2.3526, 2.4465, 2.5491, 2.6684, 2.7968, 2.9526, 3.1217, 3.299, 3.4792, 3.697, 3.9385, 4.2887, 4.7478, 5.8964]
SECTOR_MAP = {'반도체': '전기·전자', 'IT부품': '전기·전자', '통신장비': '전기·전자', '정보기기': '전기·전자', '소프트웨어': 'IT 서비스', '인터넷': 'IT 서비스',
              '디지털컨텐츠': 'IT 서비스', '컴퓨터서비스': 'IT 서비스', '통신서비스': '통신', '방송서비스': '오락·문화', '출판·매체복제': 'IT 서비스',
              '기타금융': '금융', '증권': '금융', '보험': '금융', '은행': '금융', '전기·가스·수도': '전기·가스'}
STATE = {'running': False, 'msg': '', 'err': '', 'last': ''}


def log(msg):
    line = f"[{datetime.now():%m-%d %H:%M:%S}] {msg}"
    print(line, flush=True)
    try:
        p = os.path.join(db.DATA_DIR, 'bluechip_server.log')
        with open(p, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
        if os.path.getsize(p) > 5_000_000:
            txt = open(p, encoding='utf-8', errors='ignore').read()
            open(p, 'w', encoding='utf-8').write(txt[len(txt) // 2:])
    except Exception:
        pass


def tk_score(f):
    s = TK['intercept']
    for k in TK['cols']:
        v = f.get(k)
        if v is None or (isinstance(v, float) and math.isnan(v)):
            v = TK['mean'][k]
        v = min(max(v, TK['lo'][k]), TK['hi'][k])
        s += TK['coef'][k] * (v - TK['mean'][k]) / TK['std'][k]
    return s


def tk_pct(s):
    return int(min(100, max(0, bisect.bisect_left(TK_PCT, s) - 0.5)))


# ════════════════════════════════════════════
#  월별 KRX 자료 (구성 종목 · 업종 · 재무)
# ════════════════════════════════════════════
def seed_import(seed_dir):
    """처음 실행 때 2019-01 ~ 2026-09 월별 자료(BlueChip_Collect로 받은 KRX 데이터)를 넣음"""
    c = db.conn()
    if c.execute('SELECT COUNT(*) FROM members').fetchone()[0]:
        return 0
    n = 0
    p = os.path.join(seed_dir, 'const.csv')
    if not os.path.exists(p):
        log(f'초기 자료 없음: {p}')
        return 0
    rows = [(r['date'][:6], r['ticker'].zfill(6), r['index']) for r in csv.DictReader(open(p, encoding='utf-8-sig'))]
    c.executemany('INSERT OR IGNORE INTO members VALUES (?,?,?)', rows)
    n += len(rows)
    sec = {}
    for r in csv.DictReader(open(os.path.join(seed_dir, 'sector.csv'), encoding='utf-8-sig')):
        sec[(r['date'], r['ticker'].zfill(6))] = r
    fund = {}
    for r in csv.DictReader(open(os.path.join(seed_dir, 'fund.csv'), encoding='utf-8-sig')):
        fund[(r['date'], r['ticker'].zfill(6))] = r
    out = []
    for (d, tk), r in sec.items():
        fu = fund.get((d, tk), {})
        out.append((d[:6], d, tk, r['market'], r['name'], SECTOR_MAP.get(r['sector'], r['sector']), _f(r['marcap']), _f(fu.get('EPS')), _f(fu.get('DIV'))))
    c.executemany('INSERT OR IGNORE INTO monthly VALUES (?,?,?,?,?,?,?,?,?)', out)
    c.commit()
    log(f'초기 자료 넣음: 구성 종목 {len(rows):,}행 · 월별 스냅샷 {len(out):,}행')
    return n


def seed_flows(seed_dir):
    """처음 한 번: 2023-09 ~ 2026-09 종목별 수급 (Scout 전종목 수급 내보내기에서 코스피200 · 코스닥150 편입 이력 종목만)"""
    c = db.conn()
    if c.execute('SELECT COUNT(*) FROM flows').fetchone()[0]:
        return 0
    p = os.path.join(seed_dir, 'flows.csv.gz')
    if not os.path.exists(p):
        return 0
    rows = [(r['date'], r['ticker'].zfill(6), _f(r['fo']), _f(r['ins']), _f(r['pen']))
            for r in csv.DictReader(io_text(gzip.open(p, 'rb')))]
    c.executemany('INSERT OR IGNORE INTO flows VALUES (?,?,?,?,?)', rows)
    c.commit()
    log(f'초기 수급 자료 넣음: {len(rows):,}행 · {min(r[0] for r in rows)} ~ {max(r[0] for r in rows)}')
    return len(rows)


def io_text(fb):
    import io
    return io.TextIOWrapper(fb, encoding='utf-8-sig')


def flow_last():
    return db.conn().execute('SELECT MAX(date) FROM flows').fetchone()[0] or ''


def sync_flows():
    """Scout flows(외국인 · 기관합계 · 연기금, 원 단위)를 읽기 전용으로 가져와 백만원으로 저장 — 최근 10일은 다시 덮어씀(확정치 반영)"""
    s = db.scout()
    if not s:
        return 0
    try:
        last = flow_last()
        frm = s.execute('SELECT MIN(date) FROM flows').fetchone()[0] or ''
        if last:
            dl = [r[0] for r in s.execute('SELECT DISTINCT date FROM flows WHERE date<=? ORDER BY date DESC LIMIT 10', (last,))]
            frm = dl[-1] if dl else last
        data = {}
        for d, tk, inv, amt in s.execute("SELECT date, ticker, investor, amt FROM flows WHERE date>=? AND investor IN ('외국인','기관합계','연기금')", (frm,)):
            data.setdefault((d, str(tk).zfill(6)), {})[inv] = None if amt is None else amt / 1e6
    except Exception as e:
        log(f'Scout 수급 읽기 실패: {e}')
        return 0
    finally:
        s.close()
    rows = [(d, tk, v.get('외국인'), v.get('기관합계'), v.get('연기금')) for (d, tk), v in data.items()]
    c = db.conn()
    c.executemany('INSERT OR REPLACE INTO flows VALUES (?,?,?,?,?)', rows)
    c.commit()
    return len(rows)


def trade_values(tickers, frm, to):
    """{ticker: 20일 평균 거래대금(백만원)} — Scout 일봉 (읽기 전용)"""
    s = db.scout()
    if not s or not tickers:
        return {}
    try:
        tk = list(tickers)
        out = {}
        for i in range(0, len(tk), 400):
            part = tk[i:i + 400]
            q = f"SELECT ticker, close*volume FROM candles WHERE date BETWEEN ? AND ? AND ticker IN ({','.join('?' * len(part))}) ORDER BY ticker, date"
            acc = {}
            for t, v in s.execute(q, (frm, to, *part)):
                acc.setdefault(t, []).append(v or 0)
            for t, v in acc.items():
                v = v[-20:]
                out[t] = sum(v) / len(v) / 1e6 if v else None
        return out
    finally:
        s.close()


def flow_snapshot(d, days_list, month):
    """d 기준 우량주 100 수급 요약 저장 — 그날 수급이 아직 없으면(Scout 18:10 수집 전) 가장 최근 날짜 기준, 18:30 뒤 다시 계산"""
    c = db.conn()
    uni = [r[0] for r in c.execute('SELECT ticker FROM universe WHERE month=?', (month,))]
    mem = [r[0] for r in c.execute('SELECT ticker FROM members WHERE month=(SELECT MAX(month) FROM members WHERE month<=?)', (month,))]
    fd = c.execute('SELECT MAX(date) FROM flows WHERE date<=?', (d,)).fetchone()[0]
    if not fd or fd < back_date(days_list, d, 5):
        return None
    i = bisect.bisect_right(days_list, fd)
    w20 = days_list[max(0, i - 20):i]
    w5 = set(w20[-5:])
    tks = sorted(set(uni) | set(mem))
    fl = {}
    for j in range(0, len(tks), 400):
        part = tks[j:j + 400]
        for dd, tk, fo, ins, pen in c.execute(f"SELECT date, ticker, fo, ins, pen FROM flows WHERE date BETWEEN ? AND ? AND ticker IN ({','.join('?' * len(part))})",
                                             (w20[0], fd, *part)):
            fl.setdefault(tk, []).append((dd, fo or 0.0, ins or 0.0, pen or 0.0))
    tv = trade_values(tks, back_date(days_list, d, 25), d)
    rows, M = [], {'fo1': 0.0, 'ins1': 0.0, 'pen1': 0.0, 'fo5': 0.0, 'ins5': 0.0, 'pen5': 0.0, 'fo20': 0.0, 'ins20': 0.0, 'tv20': 0.0}
    for tk in tks:
        x = fl.get(tk, [])
        s1 = [v for v in x if v[0] == fd]
        s5 = [v for v in x if v[0] in w5]
        r = {'fo1': sum(v[1] for v in s1), 'ins1': sum(v[2] for v in s1), 'pen1': sum(v[3] for v in s1),
             'fo5': sum(v[1] for v in s5), 'ins5': sum(v[2] for v in s5), 'pen5': sum(v[3] for v in s5),
             'fo20': sum(v[1] for v in x), 'ins20': sum(v[2] for v in x), 'tv20': tv.get(tk)}
        if tk in mem and r['tv20']:
            for k in M:
                M[k] += r[k]
        if tk in uni:
            r['sm5r'] = (r['fo5'] + r['ins5']) / r['tv20'] if r['tv20'] else None
            rows.append((d, tk, fd, r['fo1'], r['ins1'], r['pen1'], r['fo5'], r['ins5'], r['pen5'], r['fo20'], r['ins20'], r['tv20'], r['sm5r']))
    M['sm5r'] = (M['fo5'] + M['ins5']) / M['tv20'] if M['tv20'] else None
    rows.append((d, '_MKT', fd, M['fo1'], M['ins1'], M['pen1'], M['fo5'], M['ins5'], M['pen5'], M['fo20'], M['ins20'], M['tv20'], M['sm5r']))
    c.execute('DELETE FROM flowsnap WHERE date=?', (d,))
    c.executemany('INSERT INTO flowsnap VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)', rows)
    c.commit()
    return fd


def refresh_flows(cfg=None):
    """18:30 뒤: Scout 당일 확정 수급을 가져와 마지막 처리일 수급 요약을 다시 계산"""
    n = sync_flows()
    c = db.conn()
    last = c.execute('SELECT MAX(date) FROM daily').fetchone()[0]
    if not last:
        return n, None
    days_list = trading_days('20230101', last)
    month = c.execute('SELECT MAX(month) FROM universe WHERE month<=?', (last[:6],)).fetchone()[0]
    fd = flow_snapshot(last, days_list, month) if month else None
    return n, fd


def _f(x):
    try:
        v = float(str(x).replace(',', ''))
        return v if not math.isnan(v) else None
    except (TypeError, ValueError):
        return None


def krx_month(d, cfg):
    """KRX에서 d(그달 첫 거래일)의 구성 종목 · 업종 · 재무를 받아 저장 — KRX 계정 필요 (설정)"""
    kid, kpw = cfg.get('krx_id'), cfg.get('krx_pw')
    if not (kid and kpw):
        raise RuntimeError('KRX 계정이 없습니다 (⚙️ 설정에서 저장)')
    os.environ['KRX_ID'], os.environ['KRX_PW'] = kid, kpw
    for k in [k for k in sys.modules if k.startswith('pykrx')]:
        del sys.modules[k]
    import io
    import contextlib
    cap = io.StringIO()
    with contextlib.redirect_stdout(cap):
        from pykrx import stock
    m = d[:6]
    c = db.conn()
    mem = []
    for code, nm in (('1028', '코스피200'), ('2203', '코스닥150')):
        with contextlib.redirect_stdout(cap):
            t = stock.get_index_portfolio_deposit_file(code, d)
        time.sleep(1)
        mem += [(m, x, nm) for x in t]
    if len(mem) < 300:
        raise RuntimeError(f'KRX 구성 종목 응답 부족 ({len(mem)}) — 로그인 · 날짜 확인')
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
            rows.append((m, d, tk, mk, r['종목명'], SECTOR_MAP.get(r['업종명'], r['업종명']), _f(r['시가총액']),
                         _f(fu.get('EPS') if len(fu) else None), _f(fu.get('DIV') if len(fu) else None)))
    c.execute('DELETE FROM members WHERE month=?', (m,))
    c.executemany('INSERT OR REPLACE INTO members VALUES (?,?,?)', mem)
    c.executemany('INSERT OR REPLACE INTO monthly VALUES (?,?,?,?,?,?,?,?,?)', rows)
    c.commit()
    log(f'KRX {d} 월별 자료: 구성 종목 {len(mem)} · 스냅샷 {len(rows)}')


# ════════════════════════════════════════════
#  Scout 일봉
# ════════════════════════════════════════════
def trading_days(frm='0', to='99999999'):
    s = db.scout()
    if not s:
        return []
    try:
        return [r[0] for r in s.execute("SELECT DISTINCT date FROM candles WHERE ticker IN ('005930',?) AND date BETWEEN ? AND ? ORDER BY date",
                                        (IDX, frm, to))]
    finally:
        s.close()


def scout_last():
    s = db.scout()
    if not s:
        return ''
    try:
        return s.execute("SELECT MAX(date) FROM candles WHERE ticker='005930'").fetchone()[0] or ''
    finally:
        s.close()


def prices(tickers, frm, to):
    """{ticker: DataFrame(date index, open high low close)} — Scout 일봉 (읽기 전용)"""
    s = db.scout()
    if not s or not tickers:
        return {}
    try:
        tk = list(tickers)
        out = {}
        for i in range(0, len(tk), 400):
            part = tk[i:i + 400]
            q = f"SELECT ticker, date, open, high, low, close FROM candles WHERE date BETWEEN ? AND ? AND ticker IN ({','.join('?' * len(part))}) ORDER BY ticker, date"
            df = pd.DataFrame(s.execute(q, (frm, to, *part)).fetchall(), columns=['ticker', 'date', 'open', 'high', 'low', 'close'])
            for t, g in df.groupby('ticker'):
                out[t] = g.set_index('date')[['open', 'high', 'low', 'close']].astype(float)
        return out
    finally:
        s.close()


def back_date(days_list, d, n):
    i = bisect.bisect_left(days_list, d)
    return days_list[max(0, i - n)]


# ════════════════════════════════════════════
#  우량주 100
# ════════════════════════════════════════════
def build_universe(month, first_day, days_list):
    c = db.conn()
    mem = [r[0] for r in c.execute('SELECT ticker FROM members WHERE month=?', (month,))]
    if not mem:
        return 0
    snap = {r['ticker']: dict(r) for r in c.execute('SELECT * FROM monthly WHERE month=?', (month,))}
    ten = {r[0]: r[1] for r in c.execute('SELECT ticker, COUNT(*) FROM members WHERE month<? GROUP BY ticker', (month,))}
    frm = back_date(days_list, first_day, 70)
    px = prices(mem, frm, first_day)
    cand = []
    for tk in mem:
        sp = snap.get(tk) or {}
        if ten.get(tk, 0) < 12 or not (sp.get('eps') or 0) > 0 or not (sp.get('div') or 0) > 0:
            continue
        p = px.get(tk)
        if p is None or first_day not in p.index:
            continue
        r = p.close.pct_change()
        r = r[r.abs() < 0.305].iloc[-60:]
        if len(r) < 40:
            continue
        cand.append((float(r.std()), tk, sp, ten.get(tk, 0)))
    cand.sort()
    rows = [(month, i + 1, tk, sp.get('name'), sp.get('market'), sp.get('sector'), v, sp.get('marcap'), sp.get('eps'), sp.get('div'), t)
            for i, (v, tk, sp, t) in enumerate(cand[:100])]
    c.execute('DELETE FROM universe WHERE month=?', (month,))
    c.executemany('INSERT INTO universe VALUES (?,?,?,?,?,?,?,?,?,?,?)', rows)
    c.commit()
    log(f'{month} 우량주 100 구성: 후보 {len(cand)} → {len(rows)}')
    return len(rows)


def ensure_universe(d, days_list, cfg):
    m = d[:6]
    c = db.conn()
    if c.execute('SELECT COUNT(*) FROM universe WHERE month=?', (m,)).fetchone()[0]:
        return m
    first = next((x for x in days_list if x[:6] == m), d)
    if not c.execute('SELECT COUNT(*) FROM members WHERE month=?', (m,)).fetchone()[0]:
        try:
            krx_month(first, cfg)
        except Exception as e:
            STATE['err'] = f'{m} KRX 월별 자료 실패: {e}'[:200]
            log(STATE['err'] + ' → 지난달 우량주 100 그대로 사용')
            prev = c.execute('SELECT MAX(month) FROM universe WHERE month<?', (m,)).fetchone()[0]
            if not prev:
                return None
            c.execute('INSERT INTO universe SELECT ?, rank, ticker, name, market, sector, vol60, marcap, eps, div, tenure FROM universe WHERE month=?', (m, prev))
            c.commit()
            db.meta_set(f'univ_fallback_{m}', prev)
            return m
    build_universe(m, first, days_list)
    return m


# ════════════════════════════════════════════
#  하루 계산 · 가상 매매
# ════════════════════════════════════════════
def day_features(d, days_list, month):
    c = db.conn()
    uni = {r['ticker']: dict(r) for r in c.execute('SELECT * FROM universe WHERE month=?', (month,))}
    mem = [r[0] for r in c.execute('SELECT ticker FROM members WHERE month=?', (month,))]
    if not mem:
        prev = c.execute('SELECT MAX(month) FROM members WHERE month<=?', (month,)).fetchone()[0]
        mem = [r[0] for r in c.execute('SELECT ticker FROM members WHERE month=?', (prev,))]
    frm = back_date(days_list, d, 80)
    px = prices(set(mem) | set(uni) | {IDX}, frm, d)
    below = [bool(p.close.iloc[-1] < p.close.iloc[-20:].mean()) for t, p in px.items()
             if t in mem and d in p.index and len(p) >= 20]
    breadth = float(np.mean(below)) if below else float('nan')
    ix = px.get(IDX)
    if ix is not None and d in ix.index and len(ix) > 21:
        mkt_r5 = float(ix.close.iloc[-1] / ix.close.iloc[-6] - 1)
        mkt_vol = float(ix.close.pct_change().iloc[-20:].std() * math.sqrt(250))
        idx_close = float(ix.close.iloc[-1])
    else:
        mkt_r5 = mkt_vol = idx_close = float('nan')
    c.execute('INSERT OR REPLACE INTO market VALUES (?,?,?,?,?,?)', (d, breadth, mkt_r5, mkt_vol, idx_close, len(below)))
    rows, feats = [], {}
    for tk, u in uni.items():
        p = px.get(tk)
        if p is None or d not in p.index or len(p) < 21:
            continue
        cl = p.close
        ma20 = cl.iloc[-20:].mean()
        dl = cl.diff()
        up, dn = dl.clip(lower=0).ewm(alpha=.5, adjust=False).mean(), (-dl).clip(lower=0).ewm(alpha=.5, adjust=False).mean()
        rsi2 = float(100 - 100 / (1 + up.iloc[-1] / dn.iloc[-1])) if dn.iloc[-1] > 0 else 100.0
        f = {'depth': float(cl.iloc[-1] / ma20 - 1), 'r5': float(cl.iloc[-1] / cl.iloc[-6] - 1),
             'r20': float(cl.iloc[-1] / cl.iloc[-21] - 1), 'rsi2': rsi2, 'breadth': breadth, 'mkt_r5': mkt_r5, 'mkt_vol': mkt_vol}
        f['score'] = tk_score(f)
        f['sigA'] = f['depth'] <= -0.10
        pool = f['depth'] <= -0.07 or f['r5'] <= -0.05 or rsi2 <= 10
        f['sigB'] = pool and f['score'] >= TK['threshold']
        f.update(name=u['name'], sector=u['sector'], close=float(cl.iloc[-1]))
        feats[tk] = f
        rows.append((d, tk, f['close'], f['depth'], f['r5'], f['r20'], rsi2, f['score'], int(f['sigA']), int(f['sigB'])))
    c.execute('DELETE FROM daily WHERE date=?', (d,))
    c.executemany('INSERT INTO daily VALUES (?,?,?,?,?,?,?,?,?,?)', rows)
    c.commit()
    try:
        flow_snapshot(d, days_list, month)                    # 수급은 기록만 (매수 판단에 안 씀)
    except Exception as e:
        log(f'{d} 수급 요약 실패: {e}')
    return feats


def _equity(model, d_prev):
    r = db.conn().execute('SELECT value FROM equity WHERE model=? AND date<=? ORDER BY date DESC LIMIT 1', (model, d_prev)).fetchone()
    return r[0] if r else CAP0


def _cash(model):
    r = db.conn().execute('SELECT cash FROM equity WHERE model=? ORDER BY date DESC LIMIT 1', (model,)).fetchone()
    return r[0] if r else CAP0


def run_day(d, prev, days_list, cfg):
    """d 장 마감 뒤: ① prev 신호 → d 시가 체결 ② 보유 종목 매도 판단 ③ 계좌 평가 ④ d 종가로 새 신호"""
    c = db.conn()
    month = ensure_universe(d, days_list, cfg)
    msgs = []
    held_tk = {r[0] for r in c.execute("SELECT DISTINCT ticker FROM trades WHERE status='보유'")}
    pend = [dict(r) for r in c.execute("SELECT * FROM orders WHERE status='대기' AND signal_date=?", (prev,))] if prev else []
    px = prices(held_tk | {o['ticker'] for o in pend}, d, d)
    for m in ORDER:
        cash = _cash(m)
        eq = _equity(m, prev or d)
        size = eq / SLOTS
        pos = [dict(r) for r in c.execute("SELECT * FROM trades WHERE model=? AND status='보유'", (m,))]
        held = {p['ticker'] for p in pos}
        secn = {}
        for p in pos:
            secn[p['sector']] = secn.get(p['sector'], 0) + 1
        for o in sorted([o for o in pend if o['model'] == m], key=lambda x: x['prio']):
            bar = px.get(o['ticker'])
            why = ''
            if bar is None or d not in bar.index or not bar.loc[d, 'open'] > 0:
                why = '시가 없음(거래정지 등)'
            elif o['ticker'] in held:
                why = '이미 보유'
            elif len(pos) >= MAXPOS:
                why = f'보유 {MAXPOS}종목 한도'
            elif secn.get(o['sector'], 0) >= SECCAP:
                why = f'업종 {SECCAP}종목 한도'
            if not why:
                op = float(bar.loc[d, 'open'])
                qty = int(min(size, cash) // op)
                if qty <= 0:
                    why = '1주 가격 > 종목당 금액' if op > size else '현금 부족'
            if why:
                c.execute("UPDATE orders SET status='안 삼', note=? WHERE id=?", (why, o['id']))
                continue
            cash -= qty * op
            rules = json.dumps({'tp': TP, 'hold': HOLD, 'size': round(size), 'cost': COST})
            c.execute("""INSERT INTO trades (model,ticker,name,sector,signal_date,entry_date,entry_px,qty,tp_px,status,days,last_px,score,ma20gap,rules)
                         VALUES (?,?,?,?,?,?,?,?,?,'보유',0,?,?,?,?)""",
                      (m, o['ticker'], o['name'], o['sector'], o['signal_date'], d, op, qty, op * (1 + TP / 100), op, o['score'], o['ma20gap'], rules))
            tid = c.execute('SELECT last_insert_rowid()').fetchone()[0]
            c.execute("UPDATE orders SET status='체결', trade_id=? WHERE id=?", (tid, o['id']))
            pos.append({'ticker': o['ticker'], 'sector': o['sector']})
            held.add(o['ticker'])
            secn[o['sector']] = secn.get(o['sector'], 0) + 1
            msgs.append(f"🟢 [{m}] 매수 {o['name']} {qty}주 @ {op:,.0f}")
        # 매도 판단 (오늘 산 종목 포함)
        for t in [dict(r) for r in c.execute("SELECT * FROM trades WHERE model=? AND status='보유'", (m,))]:
            bar = px.get(t['ticker'])
            if bar is None or d not in bar.index:
                continue                                    # 거래 없는 날 (정지) — 보유일 안 셈
            if m == 'H':
                cash += _h_exit(c, t, bar, d, msgs)
                continue
            o_, h_, cl = (float(bar.loc[d, k]) for k in ('open', 'high', 'close'))
            days = t['days'] + 1
            ex = None
            if d > t['entry_date'] and o_ >= t['tp_px']:
                ex = (o_, '시가 익절 (갭)')
            elif h_ >= t['tp_px']:
                ex = (t['tp_px'], f'익절 +{TP:g}%')
            elif days >= HOLD:
                ex = (cl, f'{HOLD}일 만료')
            if ex:
                ret = (ex[0] / t['entry_px'] - 1) * 100 - COST
                pnl = t['entry_px'] * t['qty'] * ret / 100
                cash += t['entry_px'] * t['qty'] * (1 + ret / 100)
                c.execute("UPDATE trades SET status='청산', days=?, last_px=?, exit_date=?, exit_px=?, exit_reason=?, ret=?, pnl=? WHERE id=?",
                          (days, cl, d, ex[0], ex[1], ret, pnl, t['id']))
                msgs.append(f"{'💰' if ret > 0 else '🔴'} [{m}] 매도 {t['name']} {ret:+.2f}% — {ex[1]}")
            else:
                c.execute('UPDATE trades SET days=?, last_px=? WHERE id=?', (days, cl, t['id']))
        val = cash + sum(r[0] * r[1] for r in c.execute("SELECT qty, last_px FROM trades WHERE model=? AND status='보유'", (m,)))
        npos = c.execute("SELECT COUNT(*) FROM trades WHERE model=? AND status='보유'", (m,)).fetchone()[0]
        c.execute('INSERT OR REPLACE INTO equity VALUES (?,?,?,?,?)', (d, m, cash, val, npos))
    c.execute("UPDATE orders SET status='만료', note='다음 거래일 지남' WHERE status='대기' AND signal_date<?", (d,))
    c.commit()
    # 새 신호 (d 종가) → 다음 거래일 시가
    feats = day_features(d, days_list, month)
    make_orders(d, feats)
    return msgs


def _h_exit(c, t, bar, d, msgs):
    """H1 매도 — 반환: 오늘 들어온 현금. 순서: 전날 추적 신호 → 시가 매도 · 재난 손절(시가 · 장중) · +5% 30% 익절 · 추적(종가) · 40일"""
    o_, h_, l_, cl = (float(bar.loc[d, k]) for k in ('open', 'high', 'low', 'close'))
    days = t['days'] + 1
    ent, qty, pq = t['entry_px'], t['qty'], t['part_qty'] or 0
    cost_all = ent * (qty + pq)
    stop = ent * (1 - H_STOP / 100)
    peak = t['peak'] or 0
    got, ex = 0.0, None
    part_px = t['part_px']
    if t['sell_next'] and d > t['entry_date']:
        ex = (o_, '추적 매도 (고점 −4% → 시가)')
    elif d > t['entry_date'] and o_ <= stop:
        ex = (o_ * (1 - H_SLIP / 100), f'재난 손절 −{H_STOP:g}% (시가 갭)')
    elif l_ <= stop:
        ex = (stop * (1 - H_SLIP / 100), f'재난 손절 −{H_STOP:g}%')
    if ex is None and not pq and h_ >= t['tp_px']:
        n1 = int(qty * H_PART)
        p1 = o_ if (d > t['entry_date'] and o_ >= t['tp_px']) else t['tp_px']
        if 0 < n1 < qty:
            pq, qty, part_px = n1, qty - n1, p1
            got += n1 * p1
            c.execute('UPDATE trades SET part_qty=?, part_px=?, qty=? WHERE id=?', (pq, p1, qty, t['id']))
            msgs.append(f"💰 [H] 1차 익절 {t['name']} {n1}주 @ {p1:,.0f} (+{TP:g}%)")
            peak = max(cl, p1)
        else:
            ex = (p1, f'익절 +{TP:g}% (1주라 전량)')
    sell_next = 0
    if ex is None and pq:
        peak = max(peak, cl)
        if cl <= peak * (1 - H_TRAIL / 100):
            sell_next = 1
    if ex is None and days >= HOLD:
        ex = (cl, f'{HOLD}일 만료')
    if ex:
        proceeds = pq * (part_px or 0) + qty * ex[0]
        pnl = proceeds - cost_all - cost_all * COST / 100
        ret = pnl / cost_all * 100
        got += qty * ex[0] - cost_all * COST / 100
        c.execute("UPDATE trades SET status='청산', days=?, last_px=?, exit_date=?, exit_px=?, exit_reason=?, ret=?, pnl=?, peak=?, sell_next=0 WHERE id=?",
                  (days, cl, d, ex[0], ex[1], ret, pnl, peak, t['id']))
        msgs.append(f"{'💰' if ret > 0 else '🔴'} [H] 매도 {t['name']} {ret:+.2f}% — {ex[1]}")
    else:
        c.execute('UPDATE trades SET days=?, last_px=?, peak=?, sell_next=? WHERE id=?', (days, cl, peak, sell_next, t['id']))
    return got


def ensure_h_orders():
    """B0.4 설치 직후: 대기 중인 A 신호를 H로도 복사 (한 번만)"""
    c = db.conn()
    for sd in [r[0] for r in c.execute("SELECT DISTINCT signal_date FROM orders WHERE model='A' AND status='대기'")]:
        if not c.execute("SELECT 1 FROM orders WHERE model='H' AND signal_date=?", (sd,)).fetchone():
            c.execute("""INSERT INTO orders (model,signal_date,ticker,name,sector,prio,score,ma20gap,status)
                         SELECT 'H',signal_date,ticker,name,sector,prio,score,ma20gap,status FROM orders WHERE model='A' AND signal_date=? AND status='대기'""", (sd,))
    c.commit()


def make_orders(d, feats):
    c = db.conn()
    c.execute("DELETE FROM orders WHERE signal_date=? AND status='대기'", (d,))
    rows = []
    A = [(f['r20'], tk, f) for tk, f in feats.items() if f['sigA']]
    B = [(-f['score'], tk, f) for tk, f in feats.items() if f['sigB']]
    for m, lst in (('A', A), ('H', A), ('B', B)):
        for p, tk, f in sorted(lst):
            rows.append((m, d, tk, f['name'], f['sector'], p, f['score'], f['depth'], '대기'))
    if A:                                                    # 대조군: A 신호 수만큼 무작위 (날짜로 고정된 난수 → 다시 계산해도 같음)
        rnd = random.Random(int(d))
        pool = sorted(tk for tk in feats if not feats[tk]['sigA'])
        for i, tk in enumerate(rnd.sample(pool, min(len(A), len(pool)))):
            f = feats[tk]
            rows.append(('Z', d, tk, f['name'], f['sector'], i, f['score'], f['depth'], '대기'))
    c.executemany('INSERT INTO orders (model,signal_date,ticker,name,sector,prio,score,ma20gap,status) VALUES (?,?,?,?,?,?,?,?,?)', rows)
    c.commit()
    db.meta_set('last_signal_date', d)
    return len(rows)


def catch_up(cfg, start=None, until=None):
    """시작일부터 Scout 일봉 마지막 날까지 처리 안 된 거래일을 차례로 처리"""
    if STATE['running']:
        return []
    STATE.update(running=True, msg='계산 중', err='')
    msgs = []
    try:
        start = start or db.meta_get('start_date') or cfg.get('start_date') or datetime.now().strftime('%Y%m%d')
        last = until or scout_last()
        days_list = trading_days('20230101', last)
        if not days_list:
            STATE['err'] = 'Scout 일봉을 찾을 수 없습니다 (Scout 설치 · 데이터 폴더 확인)'
            return []
        try:
            sync_flows()
        except Exception as e:
            log(f'수급 동기화 실패: {e}')
        done = {r[0] for r in db.conn().execute('SELECT date FROM days')}
        todo = [d for d in days_list if d >= start and d not in done]
        # 시작 전날 신호 (첫날 매수용)
        i0 = bisect.bisect_left(days_list, start)
        if i0 > 0 and not db.conn().execute('SELECT COUNT(*) FROM orders WHERE signal_date=?', (days_list[i0 - 1],)).fetchone()[0] \
                and not db.conn().execute('SELECT COUNT(*) FROM days').fetchone()[0]:
            p0 = days_list[i0 - 1]
            m0 = ensure_universe(p0, days_list, cfg)
            if m0:
                make_orders(p0, day_features(p0, days_list, m0))
        for d in todo:
            i = bisect.bisect_left(days_list, d)
            prev = days_list[i - 1] if i > 0 else None
            STATE['msg'] = f'{d} 처리 중'
            m = run_day(d, prev, days_list, cfg)
            msgs += [f'{d[4:6]}/{d[6:]} ' + x for x in m]
            db.conn().execute('INSERT OR REPLACE INTO days VALUES (?,?,?)', (d, datetime.now().isoformat(timespec='seconds'), f'{len(m)}건'))
            db.conn().commit()
        STATE['last'] = last
        STATE['msg'] = f'완료 · {len(todo)}일 처리 · 마지막 {last}'
        if todo:
            log(STATE['msg'])
    except Exception as e:
        import traceback
        STATE['err'] = f'계산 오류: {e}'[:300]
        log(STATE['err'] + '\n' + traceback.format_exc()[-800:])
    finally:
        STATE['running'] = False
    return msgs


# ════════════════════════════════════════════
#  화면용 요약
# ════════════════════════════════════════════
def model_summary():
    c = db.conn()
    out = []
    for m in ORDER:
        eq = [dict(r) for r in c.execute('SELECT date, value, cash, npos FROM equity WHERE model=? ORDER BY date', (m,))]
        cl = [r[0] for r in c.execute("SELECT ret FROM trades WHERE model=? AND status='청산'", (m,))]
        val = eq[-1]['value'] if eq else CAP0
        vals = [x['value'] for x in eq] or [CAP0]
        peak, mdd = CAP0, 0.0
        for v in vals:
            peak = max(peak, v)
            mdd = min(mdd, v / peak - 1)
        out.append({'key': m, **MODELS[m], 'value': val, 'ret': (val / CAP0 - 1) * 100, 'cash': eq[-1]['cash'] if eq else CAP0,
                    'npos': eq[-1]['npos'] if eq else 0, 'closed': len(cl), 'win': (np.mean([x > 0 for x in cl]) * 100) if cl else None,
                    'avg': float(np.mean(cl)) if cl else None, 'mdd': mdd * 100,
                    'buys': c.execute('SELECT COUNT(*) FROM trades WHERE model=?', (m,)).fetchone()[0],
                    'curve': [[x['date'], round(x['value'])] for x in eq]})
    return out
