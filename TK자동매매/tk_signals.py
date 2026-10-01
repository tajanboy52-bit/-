"""
tk_signals.py — TK자동매매 신호 엔진 (실전 · 백테스트가 같은 함수를 씀 · 퀀트 Q1.0에서 옮김, 규칙 같음)

칸(sleeve) 4개 — 규칙은 모두 이미 검증된 정의 그대로 (여기서 새로 맞추지 않음):
  LVH 🏔 저변동고점   Scout 보고서 18장: 후보풀 안 ① ATR14/종가 낮음 ② 종가/250일 최고가 높음 ③ 25일 과열(거래량 실린 상승일 − 하락일) 낮음
                      세 백분위(동점 평균) 평균 상위 3 → 다음날 시가 매수 → 20거래일 보유 → 다음날 시가 매도
  REV 🔄 반전·수급    Scout 보고서 11장 '최종': [(과매도 RSI14 + 5일 하락)/2 + 외국인 20일 순매수 강도 + 연기금 20일 역방향] / 3
                      상위 3 → 다음날 시가 매수 → 종가 ≥ 9일 EMA 또는 10거래일이면 다음날 시가 매도
  DV  🏛 배당·가치    우량주 월별 검증: 코스피200 · 코스닥150 · 흑자 · 배당 · 편입 12개월+ → z(배당수익률) + z(1/PBR) · 업종당 4 · 15종목 · 30위 안이면 유지
  ON  🌙 밤사이 ETF   KODEX 코스닥150(229200) 종가 매수 → 다음날 시가 매도 (우량주 앱 근거 t 3.7)
후보풀 (Scout와 같음): 20일 평균 거래대금(종가×거래량) 30억+ · 1천~50만 원 · 이력 120일+ · 당일 +20% 미만 · ETF/스팩/리츠/우선주 제외
"""
import numpy as np
import pandas as pd

POOL = {'min_value': 3e9, 'min_price': 1000, 'max_price': 500000, 'min_days': 120, 'max_up': 0.20}
LVH = {'top': 3, 'hold': 10, 'heat_days': 25}        # 회전형: 20일 → 10일 보유 (설계서 14장)
REV = {'top': 3, 'hold': 10, 'ema': 9}
DV = {'n': 15, 'buf': 30, 'sec': 4, 'tenure': 12}
ON_TICKER, ON_NAME = '229200', 'KODEX 코스닥150'
SW_TICKER, SW_NAME = '069500', 'KODEX 200'          # 남는 현금을 넣어 두는 ETF (설정 sweep_on)
SW_MODES = {'night': '밤사이만 (종가 매수 → 다음 날 시가 매도 · 매일 회전)', 'ma60': '60일선 위에서만 보유 (추세)', 'vol': '변동성 15% 목표로 비중 조절', 'hold': '항상 보유'}


def sw_weight(close, mode='night'):
    """KODEX 200 보유 비중 0~1 (날짜별) — close: KODEX 200 종가 (마지막 값이 오늘 15:20 가격이어도 됨) · 자료가 모자라면 1"""
    import pandas as _pd
    c = _pd.Series(close, dtype=float).dropna()
    if mode == 'ma60':
        ma = c.rolling(60).mean()
        return (c > ma).astype(float).where(ma.notna(), 1.0)
    if mode == 'vol':
        v = c.pct_change().rolling(20).std() * (250 ** 0.5)
        return (0.15 / v).clip(upper=1.0).fillna(1.0)
    return _pd.Series(1.0, index=c.index)


GAP_SKIP = 5.0                                       # % — 예상 시가가 전날 종가보다 이만큼 넘게 높으면 LVH · REV 매수 안 함 (한국 시장 밤사이 과잉반응 → 장중 되돌림)
SLEEVES = {
    'LVH': {'name': '저변동고점', 'icon': '🏔', 'color': '#0f766e'},
    'REV': {'name': '반전·수급', 'icon': '🔄', 'color': '#7c3aed'},
    'DV': {'name': '배당·가치', 'icon': '🏛', 'color': '#b45309'},
    'ON': {'name': '밤사이 ETF', 'icon': '🌙', 'color': '#1d4ed8'},
    'SW': {'name': '남는 현금 → KODEX 200', 'icon': '💤', 'color': '#64748b'},
    'IN': {'name': '장중 (낮에 노는 돈)', 'icon': '⏱', 'color': '#be123c'},
}


# ════════════════════════════════════════════
#  재료 (날짜 × 종목 표를 한 번에)
# ════════════════════════════════════════════
def wilder_rsi(C, n=14):
    d = C.diff()
    g, l_ = d.clip(lower=0), (-d).clip(lower=0)
    ag = g.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    al = l_.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    rs = ag / al
    r = 100 - 100 / (1 + rs)
    return r.where(al > 0, 100.0).where(ag.notna())


def features(P, FL=None, excluded=None):
    """P: panel(open high low close volume) · FL: flows{'외국인','연기금','기관합계'} · excluded: 제외 종목 집합
       → 재료 표 dict (모두 날짜 × 종목, 그날 종가까지 아는 값만)"""
    O, H, L, C, V = (P[k] for k in ('open', 'high', 'low', 'close', 'volume'))
    V = V.fillna(0)
    val = P['value'] if 'value' in P else C * V                   # KRX 거래대금이 있으면 그것 (없으면 종가 × 거래량)
    val20 = val.where(C.notna()).rolling(20, min_periods=15).mean()
    nhist = C.notna().cumsum()
    up1 = C / C.shift(1) - 1
    pool = (val20 >= POOL['min_value']) & (C >= POOL['min_price']) & (C <= POOL['max_price']) & (nhist >= POOL['min_days']) \
        & ~(up1 >= POOL['max_up']) & C.notna()
    if excluded:
        cols = [c for c in C.columns if c in excluded]
        pool[cols] = False
    pc = C.shift(1)
    tr = np.fmax(np.fmax(H - L, (H - pc).abs()), (L - pc).abs()).where(pc.notna() & H.notna())
    atrp = tr.rolling(14, min_periods=14).mean() / C
    fromhi = C / H.rolling(250, min_periods=LVH['heat_days'] + 1).max() - 1
    vup = V > V.shift(1)
    heat = ((vup & (C > pc)).astype(int) - (vup & (C < pc)).astype(int)).rolling(LVH['heat_days'], min_periods=LVH['heat_days']).sum()
    rsi14 = wilder_rsi(C, 14)
    drop5 = -(C / C.shift(5) - 1)
    ema9 = C.ewm(span=REV['ema'], adjust=False, min_periods=REV['ema']).mean()
    F = {'pool': pool, 'atrp': atrp, 'fromhi': fromhi, 'heat': heat, 'rsi14': rsi14, 'drop5': drop5, 'ema9': ema9, 'close': C,
         'val20': val20, 'nhist': nhist}
    FL = FL or {}

    def strength(key):
        A = FL.get(key)
        if A is None or A.empty:
            return pd.DataFrame(np.nan, index=C.index, columns=C.columns)
        A = A.reindex(index=C.index, columns=C.columns)
        s = A.rolling(20, min_periods=1).sum()
        n = A.notna().astype(int).rolling(20, min_periods=1).sum()
        return (s / (val20 * n)).where(n >= 10)
    F['fr20'] = strength('외국인')
    pen = strength('연기금')
    ins = strength('기관합계')
    F['pen20'] = pen.where(pen.notna(), ins)                           # 연기금이 없으면 기관합계로 (Scout와 같은 대체)
    F['flow_src'] = '연기금' if FL.get('연기금') is not None else ('기관합계' if FL.get('기관합계') is not None else '')
    return F


def _pct(s, method):
    """백분위 0~1: (순위 − 1) / (n − 1)"""
    s = s.dropna()
    n = len(s)
    if n <= 1:
        return pd.Series(0.5, index=s.index)
    return (s.rank(method=method) - 1) / (n - 1)


# ════════════════════════════════════════════
#  그날 점수
# ════════════════════════════════════════════
def lvh_scores(F, d, ok=None):
    """저변동고점: {종목: 점수 0~1} · 재료가 모두 있는 후보풀 종목만"""
    ok = F['pool'].loc[d] if ok is None else ok
    tk = ok[ok].index
    a, h, t = F['atrp'].loc[d, tk], F['fromhi'].loc[d, tk], F['heat'].loc[d, tk]
    m = a.notna() & h.notna() & t.notna()
    tk = tk[m.values]
    if len(tk) == 0:
        return pd.Series(dtype=float)
    r = (_pct(-a[tk], 'average') + _pct(h[tk], 'average') + _pct(-t[tk], 'average')) / 3
    return r.sort_index()


def rev_scores(F, d, ok=None):
    """반전·수급 '최종': {종목: 점수 0~1}"""
    ok = F['pool'].loc[d] if ok is None else ok
    tk = ok[ok & (F['nhist'].loc[d] >= 60)].index
    rsi = F['rsi14'].loc[d, tk]
    raw = rsi.dropna().index
    if len(raw) == 0:
        return pd.Series(dtype=float)
    p_rev = _pct(-rsi[raw], 'min')
    fr, pn = F['fr20'].loc[d, raw], F['pen20'].loc[d, raw]
    both = fr.notna() & pn.notna()                                     # Scout: 외국인 · 연기금 둘 다 10일+ 있어야 씀, 아니면 둘 다 중립
    fr, pn = fr.where(both), pn.where(both)
    p_fr = _pct(fr, 'min').reindex(raw).fillna(0.5)
    p_in = _pct(-pn, 'min').reindex(raw).fillna(0.5)
    dr = F['drop5'].loc[d, tk].dropna()
    r_drop = _pct(dr.sort_index(), 'first')
    use = raw.intersection(r_drop.index)
    sc = ((p_rev[use] + r_drop[use]) / 2 + p_fr[use] + p_in[use]) / 3
    return sc.sort_index()


def shares(size, px, cash, fee=0.00125):
    """종목당 금액 size로 살 주식 수 — 1주도 안 되면 가격이 종목당 금액의 1.5배 이하일 때만 1주 (1,000만 원 계좌에서 비싼 종목을 통째로 놓치지 않게)"""
    if not (px and px > 0):
        return 0
    unit = px * (1 + fee)
    q = int(min(size, cash) // unit)
    if q == 0 and unit <= size * 1.5 and unit <= cash:
        q = 1
    return q


def top_n(scores, held, n, skip=0):
    """점수 높은 순(동점은 종목코드 순)으로 보유 중이 아닌 상위 n · skip이면 맨 위 skip개를 건너뛴 순위부터"""
    s = scores.sort_index().sort_values(ascending=False, kind='stable')
    return [t for t in s.index[skip:] if t not in held][:n]


def rev_exit(F, d, t):
    """종가 ≥ 9일 EMA면 매도 (다음날 시가)"""
    c, e = F['close'].at[d, t] if t in F['close'] else np.nan, F['ema9'].at[d, t] if t in F['ema9'] else np.nan
    return bool(c == c and e == e and c >= e)


# ════════════════════════════════════════════
#  배당·가치 (월 1회)
# ════════════════════════════════════════════
def dv_rank(members, monthly, month):
    """그달 순위 [(ticker, name, sector, score, div, pbr)] — 업종당 4 · 편입 12개월+ (그달 이전 구성 개월 수)"""
    mem = members.get(month) or set()
    if not mem:
        return []
    tenure = {}
    for m, s in members.items():
        if m < month:
            for t in s:
                tenure[t] = tenure.get(t, 0) + 1
    x = monthly[(monthly.month == month) & monthly.ticker.isin(mem)].copy()
    x = x[(x['eps'] > 0) & (x['div'] > 0) & (x['pbr'] > 0)]
    x = x[x.ticker.map(lambda t: tenure.get(t, 0) >= DV['tenure'])]
    if len(x) < 40:
        return []

    def z(v):
        v = v.clip(v.quantile(.02), v.quantile(.98))
        return (v - v.mean()) / (v.std() or 1)
    x['score'] = z(x['div']) + z(1 / x['pbr'])
    x = x.sort_values(['score', 'ticker'], ascending=[False, True])
    x = x.groupby('sector', group_keys=False, sort=False).head(DV['sec']).sort_values(['score', 'ticker'], ascending=[False, True])
    return [(t, n, sc_, float(s_), float(dv), float(pb)) for t, n, sc_, s_, dv, pb in zip(x['ticker'], x['name'], x['sector'], x['score'], x['div'], x['pbr'])]


def dv_targets(rank, held):
    """(유지할 보유, 새로 살 후보 순서) — 보유 중 30위 안이면 유지 · 빈자리는 상위부터"""
    pos = {t: i for i, (t, *_) in enumerate(rank)}
    keep = [t for t in held if pos.get(t, 10 ** 9) < DV['buf']]
    sell = [t for t in held if t not in keep]
    new = [r for r in rank if r[0] not in keep]
    return keep, sell, new
