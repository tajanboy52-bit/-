"""
tk_stock.py — 🔍 종목분석 엔진 (우리 시스템 규칙 그대로 · AI 없이 · 같은 질문엔 같은 답)

· 판단: 오늘 신호(LVH · REV) 순위 · 점수 · 후보풀 통과 여부(거래대금 · 가격 · 상장일 · 급등 · 정지/관리/경고) · 갭 필터 · 악재 공시 · 보유 여부
· 전략: 매수 기준(다음 날 시가 · 갭 +5% 넘으면 안 삼) · 보유 기간(LVH 10일 · REV 9EMA 복귀/10일) · 목표 수익 · 주의 구간
         = 이 종목의 과거 '우리 시스템이 샀을 날'(후보 상위 3) 성적 + 과거 10일 수익 분포 + ATR(하루 변동폭)
· 차트: 캔들 · 9/20/60일선 · 거래량 · 외국인/기관 순매수 · 과거 신호(LVH · REV 상위 3) · 실제 매매(모의 · 실전) · DART 공시
· 모두 그날 종가까지 아는 값으로만 계산 (실전 · 백테스트와 같은 재료 함수)
"""
import json
import math

import numpy as np
import pandas as pd

import tk_db as db
import tk_signals as S

HOLD = 10
ETF_NAMES = {S.ON_TICKER: S.ON_NAME, S.SW_TICKER: S.SW_NAME}


def search(q, limit=20):
    """이름 · 코드 일부 → [{ticker, name, market, listed}] — 상장 · 이름 앞부분 일치 먼저"""
    q = (q or '').strip()
    if not q:
        return []
    m = db.mconn()
    rows = m.execute("""SELECT ticker, name, market, listed, excluded FROM stocks WHERE ticker LIKE ? OR name LIKE ?
                        ORDER BY listed DESC, (name LIKE ?) DESC, (ticker = ?) DESC, length(name), name LIMIT ?""",
                     (f'{q}%', f'%{q}%', f'{q}%', q, limit)).fetchall()
    return [{'ticker': r[0], 'name': r[1], 'market': r[2], 'listed': r[3], 'excluded': r[4]} for r in rows]


def _num(v, nd=2):
    try:
        v = float(v)
        return None if math.isnan(v) or math.isinf(v) else round(v, nd)
    except (TypeError, ValueError):
        return None


def _flows(t, frm, to):
    df = pd.read_sql_query('SELECT date, investor, amt FROM flows WHERE ticker=? AND date BETWEEN ? AND ?', db.mconn(), params=(t, frm, to))
    if not len(df):
        return {}
    return {inv: g.set_index('date')['amt'].astype(float).to_frame(t) for inv, g in df.groupby('investor')}


def _fwd(O, C, ema9, i, sleeve):
    """i일 신호 → 다음 날 시가 매수 · LVH: 10일 뒤 시가 매도 · REV: 종가 ≥ 9EMA 다음 날 시가 또는 10일 → (수익, 보유일)"""
    n = len(C)
    if i + 1 >= n or not O[i + 1] == O[i + 1] or O[i + 1] <= 0:
        return None
    buy = O[i + 1]
    if sleeve == 'LVH':
        j = i + 1 + HOLD
        if j >= n or not O[j] == O[j]:
            return None
        return O[j] / buy - 1, HOLD
    for k in range(i + 1, min(n, i + 1 + HOLD)):
        if C[k] == C[k] and ema9[k] == ema9[k] and C[k] >= ema9[k] and k + 1 < n:
            return O[k + 1] / buy - 1, k + 1 - (i + 1)
    j = i + 1 + HOLD
    if j >= n or not O[j] == O[j]:
        return None
    return O[j] / buy - 1, HOLD


def _stat(rets):
    r = np.array([x for x in rets if x is not None], dtype=float)
    if not len(r):
        return {'n': 0}
    return {'n': int(len(r)), 'win': round(float((r > 0).mean() * 100), 1), 'avg': round(float(r.mean() * 100), 2), 'med': round(float(np.median(r) * 100), 2),
            'p25': round(float(np.percentile(r, 25) * 100), 2), 'p75': round(float(np.percentile(r, 75) * 100), 2),
            'best': round(float(r.max() * 100), 2), 'worst': round(float(r.min() * 100), 2)}


def analyze(t, cfg=None, chart_days=500):
    """→ 종목분석 dict (화면 한 장)"""
    import tk_trader as tr
    cfg = cfg or {}
    t = str(t).strip().zfill(6)
    st = db.stocks().get(t)
    days = db.trading_days('0', '99999999')
    if not days:
        raise ValueError('일봉 자료 없음 (📥 데이터 수집)')
    last = days[-1]
    frm = days[max(0, len(days) - 760)]                                # 약 3년 (250일 고점 · 120일 상장일 판정 여유)
    P = db.panel(frm, last, [t])
    if not P or 'close' not in P or t not in P['close'].columns or P['close'][t].dropna().empty:
        if t in ETF_NAMES:
            raise ValueError(f'{ETF_NAMES[t]}는 밤사이 · 남는 현금 칸 ETF — 종목분석 대상 아님')
        raise ValueError('이 종목의 일봉이 없음 (상장 전 · 코드 확인)')
    FL = _flows(t, frm, last)
    excl = {t} if st and (st['excluded'] or st['halt'] or st['admin'] or st['warn'] or st['market'] == 'KONEX') else set()
    F = S.features(P, FL, excl)
    C = P['close'][t]
    valid = C.dropna()
    d = valid.index[-1]
    O, H, L, V = (P[k][t] for k in ('open', 'high', 'low', 'volume'))
    c = float(C[d])
    ema = {k: C.ewm(span=k, adjust=False, min_periods=k).mean() for k in (9, 20, 60)}
    atrp = _num(F['atrp'].at[d, t] * 100)
    r = lambda n: _num((C[d] / valid.iloc[-1 - n] - 1) * 100) if len(valid) > n else None
    hi52, lo52 = float(H.loc[valid.index[-250:]].max()), float(L.loc[valid.index[-250:]].min())
    val20 = F['val20'].at[d, t]
    info = {'ticker': t, 'name': (st or {}).get('name') or t, 'market': (st or {}).get('market'), 'listed': (st or {}).get('listed', 1),
            'date': d, 'close': c, 'chg': r(1), 'r5': r(5), 'r20': r(20), 'r60': r(60), 'hi52': hi52, 'lo52': lo52,
            'from_hi': _num((c / hi52 - 1) * 100), 'from_lo': _num((c / lo52 - 1) * 100), 'atrp': atrp,
            'rsi': _num(F['rsi14'].at[d, t], 1), 'heat': _num(F['heat'].at[d, t], 0), 'val20': _num(val20, 0),
            'vol_ratio': _num(V[d] / V.loc[valid.index[-21:-1]].mean(), 2) if len(valid) > 21 and V.loc[valid.index[-21:-1]].mean() else None,
            'ema9': _num(ema[9][d], 0), 'ema20': _num(ema[20][d], 0), 'ema60': _num(ema[60][d], 0),
            'fr20': _num(F['fr20'].at[d, t] * 100, 2) if F['fr20'].at[d, t] == F['fr20'].at[d, t] else None,
            'pen20': _num(F['pen20'].at[d, t] * 100, 2) if F['pen20'].at[d, t] == F['pen20'].at[d, t] else None,
            'flags': [k for k, v in (('거래정지', (st or {}).get('halt')), ('관리종목', (st or {}).get('admin')), ('투자경고', (st or {}).get('warn')),
                                     ('제외(ETF·스팩·우선주 등)', (st or {}).get('excluded'))) if v]}
    mrow = db.mconn().execute('SELECT sector, marcap, eps, div, pbr, month FROM monthly WHERE ticker=? ORDER BY month DESC LIMIT 1', (t,)).fetchone()
    if mrow:
        info.update(sector=mrow[0], marcap=_num(mrow[1], 0), eps=_num(mrow[2], 0), div=_num(mrow[3]), pbr=_num(mrow[4]),
                    per=_num(c / mrow[2], 1) if mrow[2] and mrow[2] > 0 else None)
    # ── 후보풀 통과 여부 (매수 대상인지)
    pool_ok = bool(F['pool'].at[d, t]) if F['pool'].at[d, t] == F['pool'].at[d, t] else False
    why = []
    if not pool_ok:
        if val20 != val20 or val20 < S.POOL['min_value']:
            why.append(f"20일 평균 거래대금 {(val20 or 0) / 1e8:,.0f}억 < {S.POOL['min_value'] / 1e8:.0f}억")
        if c < S.POOL['min_price'] or c > S.POOL['max_price']:
            why.append(f"가격 {c:,.0f}원 (허용 {S.POOL['min_price']:,}~{S.POOL['max_price']:,}원)")
        if F['nhist'].at[d, t] < S.POOL['min_days']:
            why.append(f"상장 뒤 {int(F['nhist'].at[d, t])}거래일 < {S.POOL['min_days']}일")
        if (r(1) or 0) >= S.POOL['max_up'] * 100:
            why.append(f"오늘 +{r(1)}% 급등 (+{S.POOL['max_up'] * 100:.0f}% 이상 제외)")
        why += [f'{x}' for x in info['flags']]
    # ── 오늘 순위 (signal_job이 저장한 후보풀 전체 순위)
    m = db.mconn()
    sd = db.meta_get('last_signal_date') or last
    ranks = {}
    try:
        for s_, rk, n, sc in m.execute('SELECT sleeve, rank, n, score FROM scores WHERE date=? AND ticker=?', (sd, t)):
            ranks[s_] = {'rank': rk, 'n': n, 'score': round(sc, 3), 'pct': round((1 - (rk - 1) / max(1, n - 1)) * 100, 1)}
    except Exception:
        pass
    sig = {r_[0]: r_[1] for r_ in db.conn().execute("SELECT sleeve, rank FROM signals WHERE date=? AND ticker=?", (sd, t))}
    al = tr.alloc(cfg) if cfg else dict(tr.DEFAULT_ALLOC)
    pk = tr.picks(cfg) if cfg else {'LVH': (0, 3), 'REV': (0, 3)}
    # ── DART · 보유
    dart = []
    try:
        import tk_dart as DART
        dart = [dict(x) for x in DART.conn().execute('SELECT date, tag, bad, report_nm FROM dart WHERE ticker=? ORDER BY date DESC LIMIT 60', (t,))]
        bad5 = DART.bad_recent(t, sd, int(cfg.get('dart_lookback') or 5))
    except Exception:
        bad5 = []
    held = []
    for mode in ('paper', 'real'):
        for l in db.conn(mode).execute("SELECT * FROM lots WHERE ticker=? AND status IN ('보유','주문')", (t,)):
            l = dict(l)
            held.append({'mode': mode, 'sleeve': l['sleeve'], 'qty': l['qty'], 'entry_px': l['entry_px'], 'entry_date': l['entry_date'], 'days': l['days'],
                         'sell_flag': l['sell_flag'], 'ret': _num((c / l['entry_px'] - 1) * 100) if l['entry_px'] else None})
    # ── 판정
    verdict, level, notes = '', 'none', []
    picked = {s_: rk for s_, rk in sig.items() if rk < 100 and al.get(s_, 0) > 0}
    if held:
        h = held[0]
        verdict, level = f"보유 중 · {h['sleeve']} {h['qty']}주 · 매수가 {h['entry_px'] or 0:,.0f}원 ({h['ret'] or 0:+.2f}%)", 'hold'
        if h['sell_flag']:
            notes.append('다음 장전 08:50 매도 예정')
    elif picked:
        s_ = min(picked, key=picked.get)
        verdict, level = f"매수 예정 · {S.SLEEVES[s_]['name']} {picked[s_]}위 → 다음 거래일 08:50 시가", 'buy'
    elif any(v['rank'] <= pk.get(s_, (0, 3))[0] + 10 for s_, v in ranks.items() if s_ in ('LVH', 'REV')):
        s_ = min(ranks, key=lambda k: ranks[k]['rank'])
        verdict, level = f"관심 · {S.SLEEVES[s_]['name']} {ranks[s_]['rank']}위 (매수는 상위 {pk.get(s_, (0, 3))[1]}개) — 자리가 비거나 순위가 오르면 매수", 'watch'
    elif ranks:
        s_ = min(ranks, key=lambda k: ranks[k]['rank'])
        verdict, level = f"매수 대상 아님 · 후보풀 안 · {S.SLEEVES[s_]['name']} {ranks[s_]['rank']}/{ranks[s_]['n']}위 (상위 {ranks[s_]['pct']}%)", 'none'
    elif pool_ok:
        verdict, level = '후보풀 통과 · 오늘 순위 정보 없음', 'none'
        notes.append('저녁 신호 계산(18:40) 뒤부터 전체 순위가 나옵니다 — 📥 데이터 탭 🎯 신호 지금 계산으로 바로 만들 수 있음')
    else:
        verdict, level = '매수 대상 아님 · 후보풀 밖', 'out'
        notes += why or ['재료 부족 (수급 · RSI 등 계산에 필요한 기간이 모자람)']
    if bad5:
        notes.append(f"⚠️ 악재 공시 {bad5[0][1]} ({bad5[0][0][4:6]}/{bad5[0][0][6:]})" + (' → 매수 거르기 켜져 있어 안 삼' if cfg.get('dart_filter') else ' — 매수 거르기는 꺼져 있음'))
    gap = tr.gap_limit(cfg) if cfg else 5
    # ── 과거 성적: 우리 시스템이 샀을 날 (후보 상위 3) · 후보권 (상위 50)
    Oa, Ca, Ea = O.values.astype(float), C.values.astype(float), ema[9].values.astype(float)
    pos = {x: i for i, x in enumerate(C.index)}
    hist = {}
    marks = []
    for s_ in ('LVH', 'REV'):
        top3, top50 = [], []
        for dd, rk in m.execute('SELECT date, rank FROM cands WHERE ticker=? AND sleeve=? ORDER BY date', (t, s_)):
            i = pos.get(dd)
            if i is None:
                continue
            f = _fwd(Oa, Ca, Ea, i, s_)
            if f:
                top50.append(f[0])
                if rk <= 3:
                    top3.append(f[0])
            if rk <= 3:
                marks.append({'date': dd, 'sleeve': s_, 'rank': rk, 'ret': _num(f[0] * 100) if f else None})
        hist[s_] = {'top3': _stat(top3), 'top50': _stat(top50)}
    # 모든 날 10일 수익 분포 (최근 2년) · ATR
    idx = list(range(max(0, len(C) - 500), len(C)))
    all10 = [Oa[i + 1 + HOLD] / Oa[i + 1] - 1 for i in idx if i + 1 + HOLD < len(C) and Oa[i + 1] == Oa[i + 1] and Oa[i + 1 + HOLD] == Oa[i + 1 + HOLD] and Oa[i + 1] > 0]
    dist = _stat(all10)
    # ── 전략
    sl = min(picked, key=picked.get) if picked else (min(ranks, key=lambda k: ranks[k]['rank']) if ranks else 'LVH')
    base = hist.get(sl, {}).get('top3') if hist.get(sl, {}).get('top3', {}).get('n', 0) >= 5 else dist
    a = (atrp or 2.5) / 100
    plan = {'sleeve': sl, 'ref': c, 'gap_max': round(c * (1 + gap / 100)) if gap else None, 'buy_lo': round(c * (1 - a / 2)), 'buy_hi': round(c * (1 + min(a, (gap or 5) / 100))),
            'hold': '10거래일 뒤 시가 매도' if sl == 'LVH' else f"종가가 9일선({info['ema9']:,.0f}원 · 지금 기준) 위로 올라선 다음 날 시가 · 늦어도 10거래일",
            'target_pct': base.get('p75'), 'expect_pct': base.get('med'), 'caution_pct': base.get('p25'), 'basis': '이 종목의 과거 신호(상위 3) 성적' if base is not dist else '이 종목의 최근 2년 10일 수익 분포',
            'target': round(c * (1 + (base.get('p75') or 0) / 100)) if base.get('n') else None, 'expect': round(c * (1 + (base.get('med') or 0) / 100)) if base.get('n') else None,
            'caution': round(c * (1 + (base.get('p25') or 0) / 100)) if base.get('n') else None,
            'atr1': round(c * a), 'cost_pct': 0.25, 'stop': '손절 없음 (시스템 규칙) — 주의 구간 아래로 가도 보유 기간대로'}
    # ── 차트
    cd = list(valid.index[-chart_days:])
    fl = {}
    for inv, df in FL.items():
        s2 = df[t].reindex(cd)
        fl[inv] = [None if v != v else round(float(v) / 1e8, 2) for v in s2.values]
    trades = []
    for mode in ('paper', 'real'):
        for l in db.conn(mode).execute('SELECT sleeve, entry_date, entry_px, exit_date, exit_px, ret FROM lots WHERE ticker=? AND entry_date IS NOT NULL', (t,)):
            trades.append({'mode': mode, 'sleeve': l[0], 'entry_date': l[1], 'entry_px': l[2], 'exit_date': l[3], 'exit_px': l[4], 'ret': _num(l[5])})
    chart = {'d': cd, 'o': [_num(O[x], 0) for x in cd], 'h': [_num(H[x], 0) for x in cd], 'l': [_num(L[x], 0) for x in cd], 'c': [_num(C[x], 0) for x in cd],
             'v': [_num(V[x], 0) for x in cd], 'e9': [_num(ema[9][x], 0) for x in cd], 'e20': [_num(ema[20][x], 0) for x in cd], 'e60': [_num(ema[60][x], 0) for x in cd],
             'flows': fl, 'marks': [x for x in marks if x['date'] >= cd[0]], 'trades': trades, 'dart': [x for x in dart if x['date'] >= cd[0]]}
    return {'info': info, 'pool_ok': pool_ok, 'pool_why': why, 'ranks': ranks, 'signal_date': sd, 'picked': picked, 'verdict': verdict, 'level': level, 'notes': notes,
            'held': held, 'dart': dart[:20], 'bad5': bad5, 'hist': hist, 'dist': dist, 'plan': plan, 'chart': chart,
            'feat': {k: info.get(k) for k in ('atrp', 'from_hi', 'heat', 'rsi', 'fr20', 'pen20')}}
