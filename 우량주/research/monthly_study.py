"""
monthly_study.py — 우량주 수익 모델 월별 검증 (seed 자료만으로 · 인터넷 · Scout 없이 돌아감)

자료: seed/sector.csv(월초 시가총액) · seed/fund.csv(PBR · BPS · EPS · 배당수익률) · seed/const.csv(코스피200 · 코스닥150 구성)
월 수익률 = 다음 달 첫 거래일 시가총액 ÷ 이번 달 − 1 (가격 대용 · PBR×BPS 가격과 상관 0.92, 중앙 차이 0.23%)
  · 유상증자 등으로 시총 · 가격 수익률이 10%p 넘게 다르면 PBR×BPS 가격 수익률로 바꿈 · 배당은 배당수익률/12를 매달 더함
판단 시점: 월초 첫 거래일 종가 자료 → 그날 종가 매수 가정(실제로는 다음 날 시가 · 대형주라 차이 작음) · 비용 = 매매 금액의 0.125%씩(왕복 0.25%)
사용: python research/monthly_study.py  → 표를 화면에 찍고 research/monthly_study_result.md 로 저장
"""
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
SEED = os.path.join(HERE, '..', 'seed')
COST = 0.0025                     # 왕복


def load():
    s = pd.read_csv(os.path.join(SEED, 'sector.csv'), dtype={'ticker': str, 'date': str})
    f = pd.read_csv(os.path.join(SEED, 'fund.csv'), dtype={'ticker': str, 'date': str})
    c = pd.read_csv(os.path.join(SEED, 'const.csv'), dtype={'ticker': str, 'date': str})
    d = s.merge(f, on=['date', 'ticker'], how='left').sort_values(['ticker', 'date'])
    d['px'] = d.PBR * d.BPS
    g = d.groupby('ticker')
    nxt = g.date.shift(-1)
    months = sorted(d.date.unique())
    nm = dict(zip(months[:-1], months[1:]))
    ok = nxt == d.date.map(nm)                                 # 바로 다음 달 자료가 있을 때만
    r_cap = g.marcap.shift(-1) / d.marcap - 1
    r_px = g.px.shift(-1) / d.px - 1
    r = r_cap.where(~((r_cap - r_px).abs() > 0.10) | r_px.isna(), r_px)
    d['ret'] = r.where(ok).clip(-0.9, 3.0) + d.DIV.fillna(0) / 100 / 12
    d['pret'] = g.ret.shift(1)                                 # 지난달 수익률 (그달 월초에 이미 앎)
    # 지난 12개월(지난달 제외) 모멘텀 · 12개월 변동성 — 모두 월초 시점에 아는 값만
    lr = np.log1p(d.pret)
    d['mom'] = lr.groupby(d.ticker).transform(lambda x: x.shift(1).rolling(11, min_periods=9).sum())
    d['vol'] = d.pret.groupby(d.ticker).transform(lambda x: x.rolling(12, min_periods=9).std())
    d['roe'] = d.EPS / d.BPS
    d['bm'] = 1 / d.PBR.where(d.PBR > 0)
    c['month'] = c.date
    mem = c.groupby('date').ticker.apply(set).to_dict()
    d['member'] = [t in mem.get(dt, ()) for dt, t in zip(d.date, d.ticker)]
    ten = {}
    seen = {}
    for dt in months:                                           # 편입 개월 수 (그달 이전)
        for t in mem.get(dt, ()):
            ten[(dt, t)] = seen.get(t, 0)
        for t in mem.get(dt, ()):
            seen[t] = seen.get(t, 0) + 1
    d['tenure'] = [ten.get((dt, t), 0) for dt, t in zip(d.date, d.ticker)]
    return d, months


def bluechip100(x):
    """앱의 우량주 100 대용: 편입 12개월+ · 흑자 · 배당 → 12개월 월변동성 낮은 100 (앱은 60일 일변동성)"""
    y = x[(x.member) & (x.tenure >= 12) & (x.EPS > 0) & (x.DIV > 0) & x.vol.notna()]
    return y.nsmallest(100, 'vol')


def zs(v):
    v = v.astype(float)
    lo, hi = v.quantile(.02), v.quantile(.98)
    v = v.clip(lo, hi)
    return (v - v.mean()) / (v.std() or 1)


STRATS = {}


def strat(name):
    def deco(fn):
        STRATS[name] = fn
        return fn
    return deco


@strat('지수 구성 350 동일가중')
def s_members(x):
    return x[x.member]


@strat('우량주 100 동일가중')
def s_b100(x):
    return bluechip100(x)


@strat('우량주 100 중 지난달 하락 큰 10 (월 반전)')
def s_rev10(x):
    return bluechip100(x).nsmallest(10, 'pret')


@strat('우량주 100 중 지난달 상승 큰 10')
def s_win10(x):
    return bluechip100(x).nlargest(10, 'pret')


@strat('구성 350 중 저PBR 30')
def s_value(x):
    y = x[x.member & (x.PBR > 0)]
    return y.nsmallest(30, 'PBR')


@strat('구성 350 중 고배당 30')
def s_div(x):
    return x[x.member].nlargest(30, 'DIV')


@strat('구성 350 중 모멘텀 30 (12-1개월)')
def s_mom(x):
    return x[x.member & x.mom.notna()].nlargest(30, 'mom')


@strat('구성 350 중 저변동 30')
def s_lowvol(x):
    return x[x.member & x.vol.notna()].nsmallest(30, 'vol')


@strat('구성 350 중 고ROE 30 (흑자)')
def s_roe(x):
    y = x[x.member & (x.EPS > 0) & (x.BPS > 0)]
    return y.nlargest(30, 'roe')


@strat('★ 우량 가치 모멘텀 30 (QVM)')
def s_qvm(x):
    """구성 350 · 흑자 · 편입 12개월+ → z(저PBR) + z(ROE) + z(12-1 모멘텀) 합 상위 30 · 업종당 최대 5"""
    y = x[x.member & (x.EPS > 0) & (x.BPS > 0) & (x.PBR > 0) & x.mom.notna() & (x.tenure >= 12)].copy()
    if len(y) < 60:
        return y.iloc[:0]
    y['sc'] = zs(y.bm) + zs(y.roe) + zs(y.mom)
    y = y.sort_values('sc', ascending=False)
    return y.groupby('sector', group_keys=False).head(5).head(30)


@strat('★ 우량 가치 모멘텀 30 + 시장 필터')
def s_qvm_f(x):
    """QVM, 단 구성 350 동일가중의 12-1 모멘텀 중앙값 < 0 (하락장)이면 그달은 현금"""
    y = x[x.member & x.mom.notna()]
    if len(y) and y.mom.median() < 0:
        return x.iloc[:0]
    return s_qvm(x)


def run(d, months, fn, start='20190102'):
    rets, prev = [], set()
    for dt in months[:-1]:
        if dt < start:
            continue
        x = d[d.date == dt]
        pick = fn(x)
        cur = set(pick.ticker)
        r = pick.ret.dropna()
        gross = float(r.mean()) if len(r) else 0.0
        turn = 1.0 if not prev else len(cur ^ prev) / max(1, 2 * max(len(cur), len(prev)))
        if not cur and prev:
            turn = 0.5
        rets.append((dt, gross - turn * COST * (1 if cur or prev else 0), len(cur), turn))
        prev = cur
    return pd.DataFrame(rets, columns=['date', 'ret', 'n', 'turn']).set_index('date')


def stats(r):
    r = r.ret
    eq = (1 + r).cumprod()
    yrs = len(r) / 12
    cagr = eq.iloc[-1] ** (1 / yrs) - 1 if yrs else np.nan
    vol = r.std() * np.sqrt(12)
    mdd = (eq / eq.cummax() - 1).min()
    return cagr, vol, mdd, (r.mean() * 12) / vol if vol else np.nan


def main():
    d, months = load()
    rows, yearly = [], {}
    res = {k: run(d, months, fn) for k, fn in STRATS.items()}
    for k, r in res.items():
        a = stats(r)
        e = stats(r[r.index < '20230101'])
        l = stats(r[r.index >= '20230101'])
        rows.append((k, *a, e[0], l[0], r.turn.mean(), r.n.mean()))
        yearly[k] = r.ret.groupby(r.index.str[:4]).apply(lambda x: (1 + x).prod() - 1)
    T = pd.DataFrame(rows, columns=['전략', '연수익', '연변동', '최대낙폭', '샤프', '2019~22 연', '2023~26 연', '월회전', '평균종목']).set_index('전략')
    Y = pd.DataFrame(yearly).T
    fmt = T.copy()
    for c_ in ['연수익', '연변동', '최대낙폭', '2019~22 연', '2023~26 연', '월회전']:
        fmt[c_] = (T[c_] * 100).map(lambda v: f'{v:+.1f}%' if c_ != '월회전' and c_ != '연변동' else f'{v:.0f}%')
    fmt['샤프'] = T['샤프'].map(lambda v: f'{v:.2f}')
    fmt['평균종목'] = T['평균종목'].map(lambda v: f'{v:.0f}')
    Yf = (Y * 100).round(1)
    # 반전 효과 검정: 우량주 100 안에서 지난달 수익률 순위별(5분위) 다음 달 수익률
    q = []
    for dt in months[:-1]:
        b = bluechip100(d[d.date == dt]).dropna(subset=['pret', 'ret'])
        if len(b) < 50:
            continue
        b = b.assign(q=pd.qcut(b.pret.rank(method='first'), 5, labels=False))
        q.append(b.groupby('q').ret.mean().rename(dt))
    Q = pd.DataFrame(q)
    spread = Q[0] - Q[4]
    tq = spread.mean() / (spread.std() / np.sqrt(len(spread)))
    out = ['# 우량주 수익 모델 월별 검증 (seed 자료 · 2019-01 ~ 2026-09)', '',
           '월초 시가총액 수익률 + 배당수익률/12 · 왕복 비용 0.25% · 동일가중 · 월 1회 교체', '',
           fmt.to_markdown(), '', '## 연도별 수익 (%)', '', Yf.to_markdown(), '',
           '## 우량주 100 안에서 지난달 수익률 5분위 → 다음 달 평균 수익 (%)', '',
           (Q.mean() * 100).round(2).rename(lambda i: f'Q{i + 1}' + (' (지난달 하락 큼)' if i == 0 else (' (상승 큼)' if i == 4 else ''))).to_frame('다음 달').T.to_markdown(),
           '', f'Q1 − Q5 월 {spread.mean() * 100:+.2f}%p · t = {tq:.2f} · {len(spread)}개월']
    txt = '\n'.join(out)
    print(txt)
    open(os.path.join(HERE, 'monthly_study_result.md'), 'w', encoding='utf-8').write(txt + '\n')


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    main()
