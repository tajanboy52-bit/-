"""
daily_backtest.py — H1 현재 규칙 vs 개선안(H2) 일봉 백테스트 · 사용자 PC에서 실행 (Scout 일봉 필요)

실행:  python research\\daily_backtest.py            (Scout DB: %APPDATA%\\StockScout\\scout.db · SCOUT_DATA 환경변수로 바꿀 수 있음)
       python research\\daily_backtest.py --db 경로\\scout.db --start 20200203
결과:  화면 표 + research\\daily_backtest_result.md  → Claude에게 보내 점검

비교하는 것 (모두 같은 우량주 100 · 같은 신호 · 1,000만 · 비용 왕복 0.25%, 앱 bluechip_engine 규칙 그대로)
  H1        현재: 20일선 −10% → 다음날 시가 매수(많이 빠진 순) · +5% 30% 익절 · 고점 −4% 추적 · −15% 손절 · 40일 · 종목당 평가액/10 · 최대 14 · 업종당 2
  H1+조절    폭락장 조절: 하루 새 매수 최대 3종목 · 공포 온도(구성 종목 중 20일선 아래 비율) ≥ 80%인 날 신호는 반 크기
  H1+DV순서  같은 날 신호가 많을 때 배당+저PBR 점수 높은 순으로 (월 자료 검증에선 차이 작음 t 0.44 — 확인용)
  H1+코어    남는 현금을 코어(DV15)에 넣어 둠 → H1 매수 때 그만큼 코어를 팔아 씀 (옮기는 돈마다 편도 0.125%)
  H2         = H1 + 조절 + 코어  (제안 모델)
  DV15       코어만 (구성 350 · 흑자 · 배당 · 편입 1년 → 배당수익률 + 저PBR 점수 상위 15 · 업종당 4 · 30위 안이면 유지 · 월초 교체)
  지수 EW    구성 350 동일가중 (기준)
모든 판단은 그날 종가까지 아는 값 · 체결은 다음날 시가 · 배당은 넣지 않음(가격 수익만 → 코어에 불리한 쪽)
"""
import argparse
import os
import sqlite3
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
SEED = os.path.join(HERE, '..', 'seed')
sys.path.insert(0, os.path.join(HERE, '..'))
CAP0, SLOTS, MAXPOS, SECCAP = 10_000_000, 10, 14, 2
TP, HOLD, COST = 5.0, 40, 0.25
PART, TRAIL, STOP, SLIP = 0.30, 4.0, 15.0, 0.2
SW = 0.125                                   # 코어 ↔ 현금 옮길 때 편도 %
SECTOR_MAP = {'반도체': '전기·전자', 'IT부품': '전기·전자', '통신장비': '전기·전자', '정보기기': '전기·전자', '소프트웨어': 'IT 서비스', '인터넷': 'IT 서비스',
              '디지털컨텐츠': 'IT 서비스', '컴퓨터서비스': 'IT 서비스', '통신서비스': '통신', '방송서비스': '오락·문화', '출판·매체복제': 'IT 서비스',
              '기타금융': '금융', '증권': '금융', '보험': '금융', '은행': '금융', '전기·가스·수도': '전기·가스'}


def scout_path(arg):
    if arg:
        return arg
    p = os.environ.get('SCOUT_DATA') or os.path.join(os.environ.get('APPDATA') or os.path.expanduser('~'), 'StockScout')
    return os.path.join(p, 'scout.db')


def load(dbp):
    c = pd.read_csv(os.path.join(SEED, 'const.csv'), dtype={'ticker': str, 'date': str})
    s = pd.read_csv(os.path.join(SEED, 'sector.csv'), dtype={'ticker': str, 'date': str})
    f = pd.read_csv(os.path.join(SEED, 'fund.csv'), dtype={'ticker': str, 'date': str})
    for x in (c, s, f):
        x['ticker'] = x.ticker.str.zfill(6)
        x['month'] = x.date.str[:6]
    tks = sorted(set(c.ticker))
    con = sqlite3.connect(f'file:{dbp}?mode=ro', uri=True)
    rows = []
    for i in range(0, len(tks), 400):
        part = tks[i:i + 400]
        rows.append(pd.read_sql_query(f"SELECT ticker, date, open, high, low, close FROM candles WHERE date>='20181001' AND ticker IN ({','.join('?' * len(part))})",
                                      con, params=part))
    con.close()
    px = pd.concat(rows)
    px['ticker'] = px.ticker.astype(str).str.zfill(6)
    P = {k: px.pivot_table(index='date', columns='ticker', values=k).sort_index().astype(float) for k in ('open', 'high', 'low', 'close')}
    return c, s, f, P


def monthly_sets(c, s, f, P):
    """월별: 구성 종목 · 우량주 100 · DV 순위 · 업종 (앱 build_universe와 같은 규칙, 60일 일변동성)"""
    months = sorted(c.month.unique())
    mem = c.groupby('month').ticker.apply(list).to_dict()
    sec = s.assign(sector=s.sector.map(lambda v: SECTOR_MAP.get(v, v))).set_index(['month', 'ticker']).sector.to_dict()
    fu = f.set_index(['month', 'ticker'])[['EPS', 'DIV', 'PBR']].to_dict('index')
    first = {m: c[c.month == m].date.iloc[0] for m in months}
    ret = P['close'].pct_change()
    seen, out = {}, {}
    for m in months:
        d0 = first[m]
        ten = {t: seen.get(t, 0) for t in mem[m]}
        for t in mem[m]:
            seen[t] = seen.get(t, 0) + 1
        if d0 not in P['close'].index:
            continue
        i = P['close'].index.get_loc(d0)
        w = ret.iloc[max(0, i - 59):i + 1]
        cand, dv = [], []
        for t in mem[m]:
            x = fu.get((m, t)) or {}
            eps, div, pbr = x.get('EPS') or 0, x.get('DIV') or 0, x.get('PBR') or 0
            if ten[t] >= 12 and eps > 0 and div > 0:
                if t in w and w[t].notna().sum() >= 40 and pd.notna(P['close'].at[d0, t]):
                    r = w[t][w[t].abs() < 0.305]
                    cand.append((float(r.std()), t))
                if pbr > 0:
                    dv.append((t, div, 1 / pbr))
        cand.sort()
        dvf = pd.DataFrame(dv, columns=['t', 'div', 'bm']).set_index('t')
        for k in ('div', 'bm'):
            v = dvf[k].clip(dvf[k].quantile(.02), dvf[k].quantile(.98))
            dvf[k + '_z'] = (v - v.mean()) / (v.std() or 1)
        dvf['sc'] = dvf.div_z + dvf.bm_z
        dvf['sector'] = [sec.get((m, t), '') for t in dvf.index]
        rank = dvf.sort_values('sc', ascending=False).groupby('sector', group_keys=False).head(4).sort_values('sc', ascending=False)
        out[m] = {'first': d0, 'members': mem[m], 'uni': [t for _, t in cand[:100]], 'dv_rank': list(rank.index), 'dv_score': dvf.sc.to_dict(),
                  'sector': {t: sec.get((m, t), '') for t in mem[m]}}
    return out


class Book:
    def __init__(self, name, throttle=False, dv_order=False, core=False):
        self.name, self.throttle, self.dv_order, self.core = name, throttle, dv_order, core
        self.cash, self.pos, self.closed, self.curve, self.expo = float(CAP0), {}, [], [], []
        self.core_val, self.pend = 0.0, []

    def value(self, cl):
        return self.cash + self.core_val + sum(p['qty'] * _px(cl, t, p['last']) for t, p in self.pos.items())

    def need_cash(self, amt):
        """코어에서 amt만큼 현금으로"""
        if self.core and self.cash < amt and self.core_val > 0:
            take = min(self.core_val, amt - self.cash)
            self.core_val -= take
            self.cash += take * (1 - SW / 100)


def _px(row, t, default):
    v = row.get(t) if hasattr(row, 'get') else None
    return float(v) if v is not None and v == v and v > 0 else default


def simulate(M, P, start, end, books, core_ret, breadth):
    dates = [d for d in P['close'].index if start <= d <= end]
    O, H, L, C = P['open'], P['high'], P['low'], P['close']
    months = sorted(M)
    for di, d in enumerate(dates):
        m = max([x for x in months if x <= d[:6]], default=None)
        if m is None:
            continue
        info = M[m]
        o, h, l, cl = O.loc[d], H.loc[d], L.loc[d], C.loc[d]
        for b in books:
            if b.core and b.core_val > 0:
                b.core_val *= 1 + core_ret.get(d, 0.0)
            eq_prev = b.curve[-1][1] if b.curve else CAP0
            # ① 어제 신호 → 오늘 시가 매수
            secn = {}
            for p in b.pos.values():
                secn[p['sector']] = secn.get(p['sector'], 0) + 1
            nnew = 0
            for sig in b.pend:
                t = sig['t']
                op = _px(o, t, 0)
                if op <= 0 or t in b.pos or len(b.pos) >= MAXPOS or secn.get(sig['sector'], 0) >= SECCAP:
                    continue
                if b.throttle and nnew >= 3:
                    break
                size = eq_prev / SLOTS * (0.5 if (b.throttle and sig['breadth'] >= 0.8) else 1.0)
                b.need_cash(size)
                qty = int(min(size, b.cash) // op)
                if qty <= 0:
                    continue
                b.cash -= qty * op
                b.pos[t] = {'qty': qty, 'pq': 0, 'ppx': 0.0, 'entry': op, 'ed': d, 'days': 0, 'peak': 0.0, 'sell_next': 0, 'last': op, 'sector': sig['sector']}
                secn[sig['sector']] = secn.get(sig['sector'], 0) + 1
                nnew += 1
            b.pend = []
            # ② 매도 판단 (앱 _h_exit와 같은 순서)
            for t in list(b.pos):
                p = b.pos[t]
                op, hi, lo, c_ = _px(o, t, 0), _px(h, t, 0), _px(l, t, 0), _px(cl, t, 0)
                if c_ <= 0:
                    continue
                p['days'] += 1
                p['last'] = c_
                ent, stop, tp = p['entry'], p['entry'] * (1 - STOP / 100), p['entry'] * (1 + TP / 100)
                ex = None
                if p['sell_next'] and d > p['ed']:
                    ex = (op, '추적')
                elif d > p['ed'] and 0 < op <= stop:
                    ex = (op * (1 - SLIP / 100), '손절')
                elif 0 < lo <= stop:
                    ex = (stop * (1 - SLIP / 100), '손절')
                if ex is None and not p['pq'] and hi >= tp:
                    n1 = int(p['qty'] * PART)
                    p1 = op if (d > p['ed'] and op >= tp) else tp
                    if 0 < n1 < p['qty']:
                        p['pq'], p['ppx'], p['qty'] = n1, p1, p['qty'] - n1
                        b.cash += n1 * p1
                        p['peak'] = max(c_, p1)
                    else:
                        ex = (p1, '익절')
                p['sell_next'] = 0
                if ex is None and p['pq']:
                    p['peak'] = max(p['peak'], c_)
                    p['sell_next'] = int(c_ <= p['peak'] * (1 - TRAIL / 100))
                if ex is None and p['days'] >= HOLD:
                    ex = (c_, '40일')
                if ex:
                    cost_all = ent * (p['qty'] + p['pq'])
                    fee = cost_all * COST / 100
                    b.cash += p['qty'] * ex[0] - fee
                    pnl = p['pq'] * p['ppx'] + p['qty'] * ex[0] - cost_all - fee
                    b.closed.append((d, t, pnl / cost_all * 100, ex[1], p['days']))
                    del b.pos[t]
            # 남는 현금 → 코어 (1만 원 넘게 남으면)
            if b.core and b.cash > 10_000:
                b.core_val += b.cash * (1 - SW / 100)
                b.cash = 0.0
            v = b.value(cl)
            b.curve.append((d, v))
            b.expo.append(sum(p['qty'] * p['last'] for p in b.pos.values()) / v if v else 0)
        # ③ 오늘 종가로 새 신호
        uni = info['uni']
        if di + 1 < len(dates) and uni:
            i = C.index.get_loc(d)
            w = C.iloc[max(0, i - 20):i + 1]
            sig = []
            for t in uni:
                if t not in w or w[t].iloc[-20:].notna().sum() < 20 or pd.isna(w[t].iloc[-1]):
                    continue
                c0, ma20 = w[t].iloc[-1], w[t].iloc[-20:].mean()
                if c0 / ma20 - 1 <= -0.10:
                    r20 = c0 / w[t].iloc[0] - 1 if pd.notna(w[t].iloc[0]) else 0
                    sig.append({'t': t, 'r20': r20, 'dv': info['dv_score'].get(t, -9), 'sector': info['sector'].get(t, ''), 'breadth': breadth.get(d, 0)})
            for b in books:
                b.pend = sorted(sig, key=(lambda x: -x['dv']) if b.dv_order else (lambda x: x['r20']))
    return books


def core_series(M, P, start, end):
    """DV15 (업종당 4 · 30위 안이면 유지) 일간 수익률 · 월초 종가 교체 · 교체 비용 반영"""
    C = P['close']
    ret = C.pct_change()
    months = sorted(M)
    held, out = [], {}
    dates = [d for d in C.index if start <= d <= end]
    firsts = {M[m]['first'] for m in months}
    for d in dates:
        r = ret.loc[d, held].dropna() if held else pd.Series(dtype=float)
        out[d] = float(r.mean()) if len(r) else 0.0
        if d in firsts:
            m = d[:6]
            rank = M[m]['dv_rank']
            pos = {t: i for i, t in enumerate(rank)}
            keep = [t for t in held if pos.get(t, 1e9) < 30]
            new = [t for t in rank if t not in keep and pd.notna(C.at[d, t] if t in C else np.nan)][:max(0, 15 - len(keep))]
            nh = keep + new
            turn = len(set(nh) ^ set(held)) / max(1, 2 * max(len(nh), len(held))) if held else 1.0
            out[d] -= turn * 2 * SW / 100
            held = nh
    return out


def ew_series(M, P, start, end):
    C = P['close']
    ret = C.pct_change()
    out = {}
    for d in [d for d in C.index if start <= d <= end]:
        m = max([x for x in M if x <= d[:6]], default=None)
        r = ret.loc[d, [t for t in M[m]['members'] if t in ret.columns]].dropna() if m else []
        out[d] = float(r.clip(-0.3, 0.3).mean()) if len(r) else 0.0
    return out


def breadth_series(M, P):
    C = P['close']
    below = C < C.rolling(20).mean()
    out = {}
    for d in C.index:
        m = max([x for x in M if x <= d[:6]], default=None)
        if m:
            cols = [t for t in M[m]['members'] if t in C.columns]
            v = below.loc[d, cols][C.loc[d, cols].notna()]
            out[d] = float(v.mean()) if len(v) else 0.0
    return out


def stats_curve(dates, vals):
    s = pd.Series(vals, index=dates)
    r = s.pct_change().dropna()
    yrs = len(s) / 250
    cagr = (s.iloc[-1] / s.iloc[0]) ** (1 / yrs) - 1 if yrs > 0 else np.nan
    mdd = (s / s.cummax() - 1).min()
    sh = r.mean() / r.std() * np.sqrt(250) if r.std() else np.nan
    yr = s.groupby(s.index.str[:4]).last()
    yprev = pd.concat([pd.Series([s.iloc[0]]), yr.iloc[:-1]]).values
    return cagr, mdd, sh, dict(zip(yr.index, yr.values / yprev - 1))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default='')
    ap.add_argument('--start', default='20200203')
    ap.add_argument('--end', default='20260930')
    a = ap.parse_args()
    dbp = scout_path(a.db)
    if not os.path.exists(dbp):
        print(f'Scout 일봉 DB가 없습니다: {dbp}\n  --db 로 경로를 알려 주세요')
        return 1
    print('일봉 읽는 중 …', flush=True)
    c, s, f, P = load(dbp)
    M = monthly_sets(c, s, f, P)
    end = min(a.end, P['close'].index.max())
    print(f"우량주 100 월 {len(M)}개 · 일봉 {P['close'].shape} · 기간 {a.start} ~ {end}", flush=True)
    br = breadth_series(M, P)
    core = core_series(M, P, a.start, end)
    books = [Book('H1 현재'), Book('H1+조절', throttle=True), Book('H1+DV순서', dv_order=True), Book('H1+코어', core=True),
             Book('★ H2 (조절+코어)', throttle=True, core=True)]
    simulate(M, P, a.start, end, books, core, br)
    dates = [x for x, _ in books[0].curve]
    rows = []
    for b in books:
        cg, md, sh, yr = stats_curve(dates, [v for _, v in b.curve])
        rets = [x[2] for x in b.closed]
        rows.append((b.name, cg, md, sh, np.mean(b.expo), len(rets), np.mean([r > 0 for r in rets]) if rets else np.nan, np.mean(rets) if rets else np.nan, yr))
    for nm, ser in (('DV15 코어만', core), ('지수 구성 350 EW', ew_series(M, P, a.start, end))):
        v = np.cumprod([1 + ser[d] for d in dates]) * CAP0
        cg, md, sh, yr = stats_curve(dates, list(v))
        rows.append((nm, cg, md, sh, 1.0, 0, np.nan, np.nan, yr))
    years = sorted({y for r in rows for y in r[8]})
    head = '| 모델 | 연수익 | 최대낙폭 | 샤프 | H1 평균 투자 비중 | 청산 | 승률 | 건당 | ' + ' | '.join(years) + ' |'
    L = [f'# H1 vs H2 일봉 백테스트 ({dates[0]} ~ {dates[-1]} · 1,000만 · 가격 수익 · 비용 반영)', '', head, '|' + '---|' * (8 + len(years))]
    for nm, cg, md, sh, ex, n, w, av, yr in rows:
        L.append(f"| {nm} | {cg * 100:+.1f}% | {md * 100:.1f}% | {sh:.2f} | {ex * 100:.0f}% | {n} | {'-' if w != w else f'{w * 100:.0f}%'} | "
                 f"{'-' if av != av else f'{av:+.2f}%'} | " + ' | '.join(f"{yr.get(y, np.nan) * 100:+.1f}" for y in years) + ' |')
    h1, h2 = rows[0], rows[4]
    L += ['', f'- H1 평균 투자 비중 {h1[4] * 100:.0f}% → 나머지는 현금으로 놀았음. H2는 그 현금을 코어(DV15)에 둠',
          f'- 판정(사전 등록): H2가 H1보다 연수익 ≥ +3%p **그리고** 최대 낙폭이 H1보다 나쁘지 않으면(−3%p 이내) → 모의투자 칸으로 전환 검토',
          f"  → 이번 결과: 연수익 {(h2[1] - h1[1]) * 100:+.1f}%p · 최대 낙폭 {(h2[2] - h1[2]) * 100:+.1f}%p"]
    txt = '\n'.join(L)
    print(txt)
    open(os.path.join(HERE, 'daily_backtest_result.md'), 'w', encoding='utf-8').write(txt + '\n')
    return 0


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.exit(main())
