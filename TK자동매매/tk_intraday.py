"""
tk_intraday.py — ⏱ 장중 연구실 + 장중 칸(IN · 기본 꺼짐)

기존 규칙(LVH · REV · ON · SW)은 그대로 두고, 낮 동안 놀고 있는 돈(아침에 판 밤사이 ETF · KODEX 200 자금 → 15:20에 다시 삼)으로
장중 규칙을 '더해서' 쓸 수 있는지 1분봉으로 먼저 검증한다. 통과한 규칙만, 켰을 때만 실제로 쓴다.

■ 연구실 (자료: minute.db 1분봉)
  · 분마다 '그 분까지 알 수 있던 정보'만 씀 — 분봉이 끝난 뒤 신호 → 다음 분 시가에 매수 (+미끄러짐)
  · 대상: 그날 기준으로 미리 정해진 종목만 (직전 20일 거래대금 상위 · 전날 후보) — 그날 급등락으로 고른 종목은 뺌(결과로 고른 표본)
  · 비용: 매수 · 매도 수수료 + 거래세 0.20% + 미끄러짐 한쪽 0.05% → 왕복 약 0.33%
  · 15:15까지 모두 정리 (15:20 밤사이 칸 매수 돈은 건드리지 않음)
  · 대조군: 같은 날 · 같은 시각에 대상 종목 중 무작위로 산 경우(같은 익절 · 손절 폭) → 규칙이 '그냥 그 시각에 산 것'보다 나은지
  · 앞 절반 · 뒤 절반 기간으로 나눠서 둘 다 확인
■ 미리 정한 판정 기준 (결과를 본 뒤 바꾸지 않음)
  · 거래일 40일 · 거래 60건 이상, 두 기간 각각 20건 이상 (모자라면 '자료 부족')
  · 두 기간 모두 비용 뺀 평균 수익 > 0
  · 두 기간 모두 대조군보다 나음
  · 전체 t값 ≥ 2.0
■ 미리 정한 규칙 4개 (값은 고정 — 결과 보고 고치지 않음)
  GAPREV 시가 급락 되돌림: 시가가 전날 종가보다 −3%~−15% → 09:05~10:00 사이 첫 5분 고가를 넘으면 매수
                           익절 = 전날 종가(갭 메움) · 손절 = 그때까지 저가 −0.5%
  ORB    장 초반 돌파: 09:00~09:29 범위(폭 1~8%) 고가를 09:30~11:00에 거래량 2배 · VWAP 위로 넘으면 매수 · 익절 +3% · 손절 범위 저가(최대 −3%)
  VWAP   VWAP 되찾기: 10:00~14:00, 30분 넘게 VWAP 아래 있다가 다시 위로 (전날 대비 −6%~+2%) · 익절 +2% · 손절 −2%
  PULL   후보 눌림: 전날 LVH · REV 후보 상위 20이 전날 종가 −2% 아래로 마감한 분 (시가는 그 위) · 09:05~14:30 · 익절 +3% · 손절 없음
■ 장중 칸 IN (설정 intraday_on · 기본 꺼짐)
  · 판정 '통과'한 규칙 중 설정에서 고른 것만 · 웹소켓 체결가로 1분봉을 직접 만들어 연구실과 '같은 함수'로 신호 판단
  · 돈: 그 순간 주문 가능 현금 중 (밤사이 + 남는 현금 칸 몫) × intraday_pct% · 동시에 intraday_slots자리
  · 09:05~14:30 새 매수 · 익절 · 손절은 실시간 체결가 · 15:15 남은 것 모두 시장가 정리 (못 팔면 다음 날 장전 매도)
  · 주문 안전장치 · 계좌 안전장치 · 새 매수 중지 · 자동주문 정지 모두 그대로 적용
"""
import json
import math
import os
import random
import threading
import time
from datetime import datetime

import numpy as np

import tk_db as db

RESULT = os.path.join(db.DATA_DIR, 'intraday_result.json')
STATE = {'running': False, 'msg': '', 'err': '', 'pct': 0}
SLIP = 0.0005                     # 한쪽 미끄러짐 (거래대금 상위 종목 호가 1칸 안팎)
FORCE_HM = 1515                   # 이 시각 시가에 모두 정리
LAST_ENTRY = 1430
CONTROL_K = 5                     # 거래 1건당 대조군 표본 수
SEED = 7
CRIT = {'min_days': 40, 'min_trades': 60, 'min_half': 20, 't': 2.0}
RULES = {
    'GAPREV': {'name': '시가 급락 되돌림', 'desc': '시가 −3%~−15% → 09:05~10:00 첫 5분 고가 돌파 매수 · 익절 전날 종가 · 손절 그때까지 저가'},
    'ORB': {'name': '장 초반 돌파', 'desc': '09:00~09:29 범위 고가를 09:30~11:00에 거래량 2배 · VWAP 위로 돌파 · 익절 +3% · 손절 범위 저가(최대 −3%)'},
    'VWAP': {'name': 'VWAP 되찾기', 'desc': '10:00~14:00 · 30분 넘게 VWAP 아래 → 위로 · 익절 +2% · 손절 −2%'},
    'PULL': {'name': '후보 장중 눌림', 'desc': '전날 LVH·REV 후보 상위 20 · 전날 종가 −2% 아래 마감한 분 매수 · 익절 +3% · 손절 없음'},
}
EXIT_KO = {'tp': '익절', 'sl': '손절', 'time': '15:15 정리'}


def _rates():
    try:
        import tk_journal as J
        return J.RATES['fee'] / 100, J.RATES['tax'] / 100
    except Exception:
        return 0.000140527, 0.002


# ════════════════════════════════════════════
#  규칙 (연구실 · 실제 매매 공용 — 받은 분봉까지만 봄)
# ════════════════════════════════════════════
def arr(bars):
    """[(hm, o, h, l, c, v, amt)] → numpy (15:20 이후 · 종가 동시호가 봉은 뺌)"""
    a = np.array([b for b in bars if b[0] <= 1520], dtype=float).reshape(-1, 7)
    return a


def _vwap(A):
    amt = np.where(A[:, 6] > 0, A[:, 6], A[:, 4] * A[:, 5])
    cv = np.cumsum(A[:, 5])
    return np.where(cv > 0, np.cumsum(amt) / np.maximum(cv, 1), A[:, 4])


def signal(rule, A, ctx):
    """→ None 또는 {'i': 신호 분(그 분 종가에 판단), 'tp_abs','tp_pct','sl_abs','sl_pct'} — 첫 신호만 · A[:i+1]만 사용"""
    n = len(A)
    pc = ctx.get('pc')
    if n < 6 or not pc:
        return None
    hm, o, h, l, c, v = A[:, 0], A[:, 1], A[:, 2], A[:, 3], A[:, 4], A[:, 5]
    if rule == 'GAPREV':
        gap = o[0] / pc - 1
        first = hm < 905
        if not (-0.15 <= gap <= -0.03) or not first.any():
            return None
        rh = h[first].max()
        idx = np.flatnonzero((hm >= 905) & (hm <= 1000) & (c > rh))
        if not len(idx):
            return None
        i = int(idx[0])
        return {'i': i, 'tp_abs': pc, 'tp_pct': None, 'sl_abs': float(l[:i + 1].min()) * 0.995, 'sl_pct': None}
    if rule == 'ORB':
        orm = hm < 930
        if orm.sum() < 20:
            return None
        orh, orl = h[orm].max(), l[orm].min()
        if not (0.01 <= orh / orl - 1 <= 0.08):
            return None
        medv = float(np.median(v[orm]))
        vw = _vwap(A)
        idx = np.flatnonzero((hm >= 930) & (hm <= 1100) & (c > orh) & (c > vw) & (v >= 2 * max(medv, 1)))
        if not len(idx):
            return None
        return {'i': int(idx[0]), 'tp_abs': None, 'tp_pct': 0.03, 'sl_abs': float(orl), 'sl_pct': 0.03}
    if rule == 'VWAP':
        vw = _vwap(A)
        below = c < vw
        run = np.zeros(n, dtype=int)                                    # run[i] = i 바로 앞까지 연속으로 VWAP 아래였던 분 수
        k = 0
        for j in range(n):
            run[j] = k
            k = k + 1 if below[j] else 0
        chg = c / pc - 1
        idx = np.flatnonzero((hm >= 1000) & (hm <= 1400) & (c > vw) & (run >= 30) & (chg >= -0.06) & (chg <= 0.02))
        if not len(idx):
            return None
        return {'i': int(idx[0]), 'tp_abs': None, 'tp_pct': 0.02, 'sl_abs': None, 'sl_pct': 0.02}
    if rule == 'PULL':
        if not ctx.get('cand'):
            return None
        lim = pc * 0.98
        if o[0] <= lim:
            return None
        idx = np.flatnonzero((hm >= 905) & (hm <= LAST_ENTRY) & (c <= lim))
        if not len(idx):
            return None
        return {'i': int(idx[0]), 'tp_abs': None, 'tp_pct': 0.03, 'sl_abs': None, 'sl_pct': None}
    return None


def levels(sig, epx):
    """체결가 → (익절가, 손절가) — 둘 다 있으면 더 가까운 손절"""
    tp = sig.get('tp_abs') or (epx * (1 + sig['tp_pct']) if sig.get('tp_pct') else None)
    sls = [x for x in (sig.get('sl_abs'), epx * (1 - sig['sl_pct']) if sig.get('sl_pct') else None) if x]
    return tp, (max(sls) if sls else None)


def exit_scan(A, ei, tp, sl):
    """ei 분(매수 체결 분)부터 → (분, 가격, 'tp'|'sl'|'time') · 아직 안 끝났으면 None
       같은 분에 익절 · 손절이 다 닿으면 손절로(보수적) · 시가가 이미 넘어 있으면 시가로"""
    for j in range(ei, len(A)):
        hm, o, h, l = A[j, 0], A[j, 1], A[j, 2], A[j, 3]
        if hm >= FORCE_HM:
            return j, o * (1 - SLIP), 'time'
        if sl and l <= sl:
            return j, min(o, sl) * (1 - SLIP), 'sl'
        if tp and h >= tp:
            return j, max(o, tp) if j > ei else tp, 'tp'
    return None


def net_ret(epx, xpx):
    fee, tax = _rates()
    return xpx * (1 - fee - tax) / (epx * (1 + fee)) - 1


def trade(rule, A, ctx):
    """연구실: 신호 → 다음 분 시가 매수 → 청산 → dict (없으면 None)"""
    s = signal(rule, A, ctx)
    if not s or s['i'] + 1 >= len(A) or A[s['i'] + 1, 0] > LAST_ENTRY + 1 or A[s['i'] + 1, 0] >= FORCE_HM:
        return None
    ei = s['i'] + 1
    epx = A[ei, 1] * (1 + SLIP)
    tp, sl = levels(s, epx)
    if tp and tp <= epx:                                                # 매수가가 이미 익절가 위 (갭을 다 메움) → 안 삼
        return None
    ex = exit_scan(A, ei, tp, sl)
    if not ex:
        ex = (len(A) - 1, A[-1, 4] * (1 - SLIP), 'time')
    return {'rule': rule, 'hm': int(A[ei, 0]), 'sig_hm': int(A[s['i'], 0]), 'epx': epx, 'xpx': ex[1], 'kind': ex[2], 'xhm': int(A[ex[0], 0]),
            'ret': net_ret(epx, ex[1]), 'tp_pct': (tp / epx - 1) if tp else None, 'sl_pct': (1 - sl / epx) if sl else None}


def control(A, hm, tp_pct, sl_pct):
    """대조군 1건: 그 시각 다음 분 시가에 사고 같은 폭으로 청산"""
    k = np.flatnonzero(A[:, 0] >= hm)
    if not len(k) or A[k[0], 0] >= FORCE_HM:
        return None
    ei = int(k[0])
    epx = A[ei, 1] * (1 + SLIP)
    ex = exit_scan(A, ei, epx * (1 + tp_pct) if tp_pct else None, epx * (1 - sl_pct) if sl_pct else None) or (len(A) - 1, A[-1, 4] * (1 - SLIP), 'time')
    return net_ret(epx, ex[1])


# ════════════════════════════════════════════
#  연구실 실행
# ════════════════════════════════════════════
def _days():
    import tk_minute as MN
    return [r[0] for r in MN.conn().execute('SELECT DISTINCT date FROM done WHERE n>0 ORDER BY date')]


def _day_data(d, prev):
    """→ ({ticker: A}, {ticker: ctx}, uni_src) — 그날 미리 정해진 대상만"""
    import tk_minute as MN
    c = MN.conn()
    uni = {r[0]: (r[1] or '') for r in c.execute('SELECT ticker, why FROM universe WHERE date=?', (d,))}
    if uni:
        keep = {t for t, w in uni.items() if w == '거래대금' or w.startswith('후보')}
        src = 'universe'
    else:                                                               # 가져온 분봉(대상 기록 없음) → ETF만 빼고 전부 (표시에 알림)
        keep = None
        src = 'all'
    m = db.mconn()
    pcs = {r[0]: r[1] for r in m.execute('SELECT ticker, close FROM bars WHERE date=? AND close>0', (prev,))} if prev else {}
    cand = {r[0] for r in m.execute("SELECT ticker FROM cands WHERE date=? AND sleeve IN ('LVH','REV') AND rank<=20", (prev,))} if prev else set()
    rows = c.execute('SELECT ticker, hm, open, high, low, close, vol, amt FROM bars WHERE date=? ORDER BY ticker, hm', (d,)).fetchall()
    by = {}
    for r in rows:
        t = r[0]
        if t in ('229200', '069500') or (keep is not None and t not in keep) or t not in pcs:
            continue
        by.setdefault(t, []).append(tuple(r[1:]))
    out = {}
    for t, b in by.items():
        A = arr(b)
        if len(A) >= 60 and A[0, 0] <= 905:
            out[t] = A
    return out, {t: {'pc': pcs[t], 'cand': t in cand} for t in out}, src


def _stat(rets):
    r = np.array(rets, dtype=float)
    if not len(r):
        return {'n': 0}
    sd = float(r.std(ddof=1)) if len(r) > 1 else 0.0
    return {'n': int(len(r)), 'avg': round(float(r.mean()) * 100, 3), 'win': round(float((r > 0).mean()) * 100, 1),
            't': round(float(r.mean() / sd * math.sqrt(len(r))), 2) if sd > 0 else None,
            'pf': round(float(r[r > 0].sum() / -r[r < 0].sum()), 2) if (r < 0).any() else None}


def verdict(st):
    """미리 정한 기준 → ('통과'|'탈락'|'자료 부족', [이유])"""
    why = []
    h1, h2, al = st['half1'], st['half2'], st['all']
    if st['days'] < CRIT['min_days'] or al['n'] < CRIT['min_trades'] or h1['n'] < CRIT['min_half'] or h2['n'] < CRIT['min_half']:
        return '자료 부족', [f"거래일 {st['days']}/{CRIT['min_days']} · 거래 {al['n']}/{CRIT['min_trades']} · 기간별 {h1['n']}·{h2['n']}/{CRIT['min_half']}"]
    for nm, h, c in (('앞 기간', h1, st['ctl1']), ('뒤 기간', h2, st['ctl2'])):
        if h['avg'] <= 0:
            why.append(f'{nm} 평균 {h["avg"]:+.3f}% ≤ 0')
        if c.get('n') and h['avg'] <= c['avg']:
            why.append(f'{nm} 대조군({c["avg"]:+.3f}%)보다 못함')
    if (al.get('t') or 0) < CRIT['t']:
        why.append(f"t값 {al.get('t')} < {CRIT['t']}")
    return ('탈락' if why else '통과'), why or ['기준 모두 통과']


def run(cfg=None, progress=None, days=None):
    """분봉 전체로 규칙 4개 검증 → intraday_result.json"""
    if STATE['running']:
        return None
    STATE.update(running=True, err='', msg='장중 연구실 시작', pct=0)
    say = progress or (lambda m: STATE.update(msg=m))
    t0 = time.time()
    try:
        days = days or _days()
        tdays = db.trading_days('0', '99999999')
        rng = random.Random(SEED)
        T = {r: [] for r in RULES}
        C = {r: [] for r in RULES}
        srcs = set()
        for k, d in enumerate(days):
            prev = ([x for x in tdays if x < d] or [None])[-1]
            data, ctxs, src = _day_data(d, prev)
            srcs.add(src)
            tks = sorted(data)
            for t in tks:
                for r in RULES:
                    tr_ = trade(r, data[t], ctxs[t])
                    if not tr_:
                        continue
                    tr_.update(date=d, ticker=t)
                    T[r].append(tr_)
                    pool = [u for u in tks if u != t]
                    cs = [control(data[u], tr_['hm'], tr_['tp_pct'], tr_['sl_pct']) for u in rng.sample(pool, min(CONTROL_K, len(pool)))]
                    cs = [x for x in cs if x is not None]
                    C[r].append((d, float(np.mean(cs)) if cs else None))
            STATE['pct'] = round((k + 1) / max(1, len(days)) * 100)
            if k % 5 == 0:
                say(f'장중 연구실 {d} ({k + 1}/{len(days)}일)')
        mid = days[len(days) // 2] if days else ''
        fwd = (cfg or {}).get('_shadow_start') or db.gmeta_get('shadow_start') or ''
        out = {'made': db.now_s(), 'days': len(days), 'first': days[0] if days else '', 'last': days[-1] if days else '', 'mid': mid,
               'uni_src': sorted(srcs), 'crit': CRIT, 'slip': SLIP * 100, 'cost': round((sum(_rates()) + _rates()[0] + 2 * SLIP) * 100, 3), 'rules': []}
        for r, m in RULES.items():
            tt = T[r]
            rets = [x['ret'] for x in tt]
            ctl = [(d, v) for d, v in C[r] if v is not None]
            st = {'days': len(days), 'all': _stat(rets), 'half1': _stat([x['ret'] for x in tt if x['date'] < mid]),
                  'half2': _stat([x['ret'] for x in tt if x['date'] >= mid]), 'ctl': _stat([v for _, v in ctl]),
                  'ctl1': _stat([v for d, v in ctl if d < mid]), 'ctl2': _stat([v for d, v in ctl if d >= mid]),
                  'fwd': _stat([x['ret'] for x in tt if fwd and x['date'] >= fwd])}
            v, why = verdict(st)
            by_h = {}
            for x in tt:
                by_h.setdefault(f"{x['hm'] // 100:02d}시", []).append(x['ret'])
            by_x = {}
            for x in tt:
                by_x.setdefault(EXIT_KO[x['kind']], []).append(x['ret'])
            daily = {}
            for x in sorted(tt, key=lambda x: (x['date'], x['hm'])):            # 칸 5자리 · 하루 먼저 온 5건만 (자리당 20%)
                daily.setdefault(x['date'], [])
                if len(daily[x['date']]) < 5:
                    daily[x['date']].append(x['ret'])
            eq, curve, peak, mdd = 1.0, [], 1.0, 0.0
            for d in days:
                eq *= 1 + sum(daily.get(d, [])) / 5
                peak = max(peak, eq)
                mdd = min(mdd, eq / peak - 1)
                curve.append([d, round(eq * 100, 2)])
            out['rules'].append({'key': r, **m, **st, 'edge': round(st['all']['avg'] - st['ctl']['avg'], 3) if st['all'].get('n') and st['ctl'].get('n') else None,
                                 'verdict': v, 'why': why, 'by_hour': {k: _stat(x) for k, x in sorted(by_h.items())},
                                 'by_exit': {k: _stat(x) for k, x in by_x.items()}, 'sleeve': {'ret': round((eq - 1) * 100, 2), 'mdd': round(mdd * 100, 2), 'curve': curve},
                                 'recent': [{k: (round(v_, 5) if isinstance(v_, float) else v_) for k, v_ in x.items()} for x in tt[-30:]]})
        json.dump(out, open(RESULT, 'w', encoding='utf-8'), ensure_ascii=False)
        ok = [x['key'] for x in out['rules'] if x['verdict'] == '통과']
        db.log(f"⏱ 장중 연구실 {len(days)}일 · 통과 {', '.join(ok) or '없음'} ({time.time() - t0:.0f}초)")
        STATE['msg'] = f'끝 · {time.time() - t0:.0f}초'
        return out
    except Exception as e:
        STATE['err'] = str(e)[:200]
        db.log(f'장중 연구실 오류: {str(e)[:200]}', 'warn')
        return None
    finally:
        STATE['running'] = False


def result():
    try:
        return json.load(open(RESULT, encoding='utf-8'))
    except Exception:
        return None


def passed():
    r = result() or {}
    return {x['key'] for x in r.get('rules', []) if x.get('verdict') == '통과'}


# ════════════════════════════════════════════
#  장중 칸 IN — 실시간 1분봉 (웹소켓 체결가 → 분봉)
# ════════════════════════════════════════════
BARS = {}                         # {ticker: {hm: [o, h, l, c, v, amt]}} — 오늘 것만
_bars_day = ['']
_lock = threading.Lock()
LIVE = {'watch': [], 'signals': [], 'last': '', 'msg': '', 'wkey': None, 'seeded': set()}


def on_tick(t, px, vol=0, hms=None):
    """tk_ws가 체결마다 부름 · hms='HHMMSS' (없으면 지금 시각)"""
    if not px:
        return
    now = datetime.now()
    d = now.strftime('%Y%m%d')
    hm = int((hms or now.strftime('%H%M%S'))[:4])
    with _lock:
        if _bars_day[0] != d:
            BARS.clear()
            _bars_day[0] = d
        b = BARS.setdefault(t, {}).get(hm)
        if b is None:
            BARS[t][hm] = [px, px, px, px, vol, px * vol]
        else:
            b[1], b[2], b[3] = max(b[1], px), min(b[2], px), px
            b[4] += vol
            b[5] += px * vol


def seed(t, bars):
    """재시작 등으로 놓친 앞부분을 REST 오늘 분봉으로 채움 (웹소켓으로 만든 분은 그대로)"""
    with _lock:
        cur = BARS.setdefault(t, {})
        for hm, o, h, l, c, v, a in bars:
            cur.setdefault(int(hm), [o, h, l, c, v, a])


def live_bars(t, upto_hm):
    """upto_hm 전까지 끝난 분봉 → A"""
    with _lock:
        b = BARS.get(t) or {}
        rows = [(hm, *x) for hm, x in sorted(b.items()) if hm < upto_hm]
    return arr(rows) if rows else np.zeros((0, 7))


def rules_on(cfg):
    """켠 규칙 ∩ 연구실 통과 규칙"""
    want = [r for r in (cfg.get('intraday_rules') or []) if r in RULES]
    ok = passed()
    return [r for r in want if r in ok], [r for r in want if r not in ok]


def watch(cfg, d=None):
    """장중 칸이 볼 종목 (웹소켓 자리 40개 중 보유 · 장전 예상체결과 나눠 씀) — 전날 후보 상위 + 직전 20일 거래대금 상위"""
    if not cfg.get('intraday_on'):
        return []
    on, _ = rules_on(cfg)
    if not on:
        return []
    import tk_trader as tr
    d = d or tr.today()
    key = (d, tuple(on), int(cfg.get('intraday_watch') or 25))
    if LIVE.get('wkey') == key:
        return LIVE['watch']
    prev = tr.prev_trading_day(d)
    m = db.mconn()
    out = []
    if 'PULL' in on:
        out += [r[0] for r in m.execute("SELECT ticker FROM cands WHERE date=? AND sleeve IN ('LVH','REV') AND rank<=20 ORDER BY rank", (prev,))]
    if set(on) - {'PULL'}:
        days = [x for x in db.trading_days('0', d) if x < d][-20:]
        if days:
            q = f"SELECT ticker FROM bars WHERE date IN ({','.join('?' * len(days))}) GROUP BY ticker ORDER BY AVG(COALESCE(value, close*volume)) DESC LIMIT 60"
            st = db.stocks()
            out += [r[0] for r in m.execute(q, days) if r[0] in st and not st[r[0]]['excluded'] and not st[r[0]]['halt'] and not st[r[0]]['admin']]
    n = int(cfg.get('intraday_watch') or 25)
    out = list(dict.fromkeys(out))[:n]
    LIVE.update(watch=out, wkey=key)
    return out


def prev_min(hm):
    return hm - 41 if hm % 100 == 0 else hm - 1


def budget(cfg, kc):
    """지금 장중 칸에 쓸 수 있는 돈 = min(주문 가능 현금, 운용 자금 − LVH·REV·DV 보유 원가 − 장중 칸 보유) × intraday_pct%
       낮에는 밤사이 ETF · KODEX 200을 아침에 팔아 둔 돈이 현금으로 놂 → 그 돈을 15:15까지만 씀 (15:20 매수 전에 돌아옴)"""
    import tk_trader as tr
    inv = sum((l['cost'] or 0) for l in tr.open_lots() if l['sleeve'] in ('LVH', 'REV', 'DV', 'IN') and l['status'] == '보유')
    cash = kc.buyable()['nrcvb']
    return max(0.0, min(cash, tr.cap(cfg) - inv) * float(cfg.get('intraday_pct') or 50) / 100)


def step(cfg, kc, d, prices=None):
    """장중 칸 한 번 (tk_trader.loop가 09:05~15:20 동안 10초마다) — 정리 → 익절 · 손절 → 새 매수"""
    import tk_journal as J
    import tk_trader as tr
    if not cfg.get('intraday_on'):
        return
    hm = int(tr.now().strftime('%H%M'))
    prices = prices or {}
    x = db.conn()
    mine = [l for l in tr.open_lots('IN')]
    # ① 15:15 정리
    if hm >= FORCE_HM:
        for l in mine:
            if l['status'] == '보유' and l['qty'] > 0:
                tr.send(cfg, kc, 'sell', 'in_close', l['id'], 'IN', l['ticker'], l['name'], l['qty'], sig_ref=prices.get(l['ticker']) or l['last_px'])
        return
    # ② 익절 · 손절 (실시간 체결가)
    for l in mine:
        if l['status'] != '보유' or not l['qty']:
            continue
        info = json.loads(l['entry_info'] or '{}')
        px = prices.get(l['ticker'])
        if not px:
            continue
        tp, sl = levels(info, l['entry_px'])
        kind = 'in_sl' if sl and px <= sl else ('in_tp' if tp and px >= tp else None)
        if kind:
            tr.send(cfg, kc, 'sell', kind, l['id'], 'IN', l['ticker'], l['name'], l['qty'], sig_ref=px)
    # ③ 새 매수
    if hm < 905 or hm > LAST_ENTRY or not tr.can_order(cfg) or tr.buy_paused(cfg) or db.meta_get('block_new'):
        return
    on, _ = rules_on(cfg)
    if not on:
        return
    slots = int(cfg.get('intraday_slots') or 5)
    if len(mine) >= slots:
        return
    done_today = {r[0] for r in x.execute("SELECT ticker FROM lots WHERE sleeve='IN' AND signal_date=?", (d,))}
    prev = tr.prev_trading_day(d)
    m = db.mconn()
    pcs = {r[0]: r[1] for r in m.execute('SELECT ticker, close FROM bars WHERE date=?', (prev,))}
    cand = {r[0] for r in m.execute("SELECT ticker FROM cands WHERE date=? AND sleeve IN ('LVH','REV') AND rank<=20", (prev,))}
    st = db.stocks()
    sigs = []
    for t in watch(cfg, d):
        if t in done_today or t not in pcs:
            continue
        if (t, d) not in LIVE['seeded'] and hasattr(kc, 'minute_today'):  # 늦게 켜졌으면 앞부분을 REST로 한 번 채움
            LIVE['seeded'].add((t, d))
            if not (BARS.get(t) or {}).get(900):
                try:
                    seed(t, kc.minute_today(t, d))
                except Exception as e:
                    tr.log(f'장중 칸 {t} 오늘 분봉 채우기 실패: {str(e)[:80]}', 'warn')
        A = live_bars(t, hm)
        if len(A) and A[0, 0] > 901:                                    # 장 시작 분봉이 없으면 판단 안 함 (시가 · 첫 범위가 틀어짐)
            continue                                            # 지금 만들어지는 분은 빼고 끝난 분만
        if len(A) < 6 or int(A[-1, 0]) != prev_min(hm):                # 마지막 끝난 분이 바로 앞 분이어야 (끊긴 자료로 판단 안 함)
            continue
        for r in on:
            s = signal(r, A, {'pc': pcs[t], 'cand': t in cand})
            if s and s['i'] == len(A) - 1:                              # 신호가 '방금 끝난 분'에 났을 때만 (연구실: 다음 분 시가 매수)
                sigs.append((t, r, s, int(A[-1, 0]), float(A[-1, 4])))
                break
    if not sigs:
        return
    room = slots - len(mine)
    money = budget(cfg, kc)
    per = money / max(1, room)
    for t, r, s, shm, last in sigs[:room]:
        px = prices.get(t) or last
        q = int(per // (px * 1.003)) if px else 0
        nm = (st.get(t) or {}).get('name', t)
        LIVE['signals'] = ([{'ts': db.now_s(), 'ticker': t, 'name': nm, 'rule': r, 'px': px, 'qty': q}] + LIVE['signals'])[:30]
        if q <= 0:
            tr.log(f'⏱ 장중 칸 {nm} {RULES[r]["name"]} 신호 — 돈 부족({per:,.0f}원)으로 건너뜀', 'warn')
            continue
        info = {'rule': r, 'tp_abs': s['tp_abs'], 'tp_pct': s['tp_pct'], 'sl_abs': s['sl_abs'], 'sl_pct': s['sl_pct'], 'sig_hm': shm}
        lid = tr.new_lot('IN', t, nm, (st.get(t) or {}).get('sector') or '', d, {'ref': px, 'info': json.dumps(info), 'score': None, 'rank': None})
        J.decision(x, d, d, {'sleeve': 'IN', 'ticker': t, 'name': nm, 'ref': px, 'qty': q, 'amt': q * px}, 'buy', f'장중 {RULES[r]["name"]}')
        x.commit()
        tr.send(cfg, kc, 'buy', 'in_buy', lid, 'IN', t, nm, q, sig_ref=px)
    LIVE['last'] = db.now_s()
