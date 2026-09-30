"""
tk_backtest.py — TK자동매매 포트폴리오 백테스트 (실전과 같은 신호 함수 tk_signals · 자체 수집 자료 · 상장폐지 포함)

실행: python tk_backtest.py            (또는 백테스트_실행.bat · 서버 화면 '🧪 백테스트' 버튼)
      python tk_backtest.py --start 20231024 --end 20260930 --alloc LVH=40,REV=25,DV=20,ON=15
결과: 화면 표 + %APPDATA%\\TKAuto\\backtest_result.md · backtest_result.json (서버 화면에 표시) → Claude에게 보내 점검

규칙 (실전과 같음 · 새로 맞춘 값 없음)
· 모든 판단은 그날 종가까지 아는 값 → 주식은 다음 거래일 시가 체결 · ON ETF는 그날 종가 매수 → 다음날 시가 매도
· 종목당 금액 = 칸 비율 ÷ 칸 자리 수 × 전날 계좌 평가액 (LVH 20자리 · REV 10자리 · DV 15자리) · 정수 주 · 현금 안에서만
· 비용: 주식 왕복 0.25% (매수 · 매도 각 0.125%) · ETF 왕복 0.05% (보수적으로) · 상장폐지 · 거래 끊김은 마지막 종가로 정리
· 기간 구분은 Scout 보고서와 같음: 조정 2023-10-24 ~ 2025-08-29 · 검증 2025-09-01 ~
"""
import argparse
import json
import math
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import tk_db as db
import tk_signals as S

SLOTS = {'LVH': 20, 'REV': 10, 'DV': 15}
ALLOC = {'LVH': 0.40, 'REV': 0.25, 'DV': 0.20, 'ON': 0.15}
PERIODS = {'조정': ('20231024', '20250829'), '검증': ('20250901', '99999999')}


def load(start, end):
    """백테스트 재료 한 번에: 일봉 · 수급 · 재료표 · 월 자료 · ETF"""
    days = db.trading_days('20180101', end)
    i0 = max(0, next((i for i, d in enumerate(days) if d >= start), len(days)) - 300)
    frm = days[i0] if days else start
    st = db.stocks()
    excl = {t for t, v in st.items() if v['excluded']}
    P = db.panel(frm, end)
    if not P:
        raise RuntimeError('일봉 자료가 없습니다 — 📥 데이터 탭에서 먼저 수집하세요')
    FL = db.flows(frm, end)
    F = S.features(P, FL, excl)
    mem, mon = db.month_tables()
    on = db.etf_bars(S.ON_TICKER, frm, end)
    return {'P': P, 'F': F, 'mem': mem, 'mon': mon, 'on': on, 'names': {t: v['name'] for t, v in st.items()}, 'flow_src': F['flow_src']}


def simulate(D, start, end, alloc, cap=10_000_000, cost=0.25, on_cost=0.05, seed_rank_cache=None):
    P, F = D['P'], D['F']
    O, C = P['open'], P['close']
    dates = [d for d in C.index if start <= d <= end]
    last_valid = C.apply(lambda s: s.last_valid_index())
    onb = D['on']
    fee, on_fee = cost / 200, on_cost / 200
    cash = float(cap)
    lots, trades, pend = [], [], {'LVH': [], 'REV': [], 'DV': []}
    on_lot = None
    curve, sleeve_val, sleeve_pnl = [], [], {s: 0.0 for s in list(SLOTS) + ['ON']}
    dv_month, dv_rank = None, []
    rank_cache = seed_rank_cache if seed_rank_cache is not None else {}
    eq_prev = float(cap)
    expo = []
    for i, d in enumerate(dates):
        # ── 시가: ON 매도 → 주식 매도 → 주식 매수
        if on_lot:
            px = onb['open'].get(d) if d in onb.index else None
            if px and px > 0:
                got = on_lot['qty'] * px * (1 - on_fee)
                pnl = got - on_lot['cost']
                cash += got
                sleeve_pnl['ON'] += pnl
                trades.append(('ON', S.ON_TICKER, on_lot['d'], d, pnl / on_lot['cost'] * 100, 'next_open'))
                on_lot = None
        for lt in [x for x in lots if x['flag']]:
            t = lt['t']
            px = O.at[d, t] if t in O.columns else np.nan
            if not (px == px and px > 0):
                lv = last_valid.get(t)
                if lv is not None and lv < d:                          # 상장폐지 · 거래 끊김 → 마지막 종가로
                    px = C.at[lv, t]
                else:
                    continue
            got = lt['qty'] * px * (1 - fee)
            pnl = got - lt['cost']
            cash += got
            sleeve_pnl[lt['s']] += pnl
            trades.append((lt['s'], t, lt['d'], d, pnl / lt['cost'] * 100, lt['flag']))
            lots.remove(lt)
        for s in ('LVH', 'REV', 'DV'):
            if alloc.get(s, 0) <= 0:
                pend[s] = []
                continue
            size = alloc[s] / SLOTS[s] * eq_prev
            for t in pend[s]:
                if sum(1 for x in lots if x['s'] == s) >= SLOTS[s]:
                    break
                px = O.at[d, t] if t in O.columns else np.nan
                if not (px == px and px > 0) or any(x['s'] == s and x['t'] == t for x in lots):
                    continue
                qty = S.shares(size, px, cash, fee)
                if qty <= 0:
                    continue
                c_ = qty * px * (1 + fee)
                cash -= c_
                lots.append({'s': s, 't': t, 'qty': qty, 'cost': c_, 'px': px, 'd': d, 'days': 0, 'last': px, 'flag': ''})
            pend[s] = []
        # ── 종가: 평가 · 매도 표시 · 새 신호
        row = C.loc[d]
        for lt in lots:
            c0 = row.get(lt['t'])
            if c0 == c0 and c0 and c0 > 0:
                lt['last'] = float(c0)
                lt['days'] += 1
            if lt['flag']:
                continue
            if lt['s'] == 'LVH' and lt['days'] >= S.LVH['hold']:
                lt['flag'] = 'hold20'
            elif lt['s'] == 'REV' and lt['days'] >= 1 and (S.rev_exit(F, d, lt['t']) or lt['days'] >= S.REV['hold']):
                lt['flag'] = 'ema9' if S.rev_exit(F, d, lt['t']) else 'hold10'
        if i + 1 < len(dates):
            if alloc.get('LVH', 0) > 0:
                held = {x['t'] for x in lots if x['s'] == 'LVH'}
                pend['LVH'] = S.top_n(S.lvh_scores(F, d), held, S.LVH['top'])
            if alloc.get('REV', 0) > 0:
                held = {x['t'] for x in lots if x['s'] == 'REV'}
                pend['REV'] = S.top_n(S.rev_scores(F, d), held, S.REV['top'])
            if alloc.get('DV', 0) > 0:
                m = d[:6]
                if m != dv_month:
                    if m not in rank_cache:
                        rank_cache[m] = S.dv_rank(D['mem'], D['mon'], m)
                    if rank_cache[m]:
                        dv_month, dv_rank = m, rank_cache[m]
                        held = [x['t'] for x in lots if x['s'] == 'DV' and not x['flag']]
                        keep, sell, _ = S.dv_targets(dv_rank, held)
                        for x in lots:
                            if x['s'] == 'DV' and x['t'] in sell:
                                x['flag'] = 'dv_rebal'
                if dv_rank:
                    live = [x['t'] for x in lots if x['s'] == 'DV' and not x['flag']]
                    need = SLOTS['DV'] - len(live)
                    pend['DV'] = [r[0] for r in dv_rank if r[0] not in live][:need + 5] if need > 0 else []
        eq_close = cash + sum(x['qty'] * x['last'] for x in lots)
        if alloc.get('ON', 0) > 0 and i + 1 < len(dates) and d in onb.index:
            cpx = onb.at[d, 'close']
            if cpx and cpx > 0:
                qty = int(min(alloc['ON'] * eq_close, cash) // (cpx * (1 + on_fee)))
                if qty > 0:
                    c_ = qty * cpx * (1 + on_fee)
                    cash -= c_
                    on_lot = {'qty': qty, 'cost': c_, 'd': d, 'px': cpx}
        on_val = on_lot['qty'] * on_lot['px'] if on_lot else 0.0
        eq = cash + sum(x['qty'] * x['last'] for x in lots) + on_val
        sv = {s: sum(x['qty'] * x['last'] for x in lots if x['s'] == s) for s in SLOTS}
        sv['ON'] = on_val
        curve.append((d, eq))
        sleeve_val.append((d, sv))
        expo.append({s: sv[s] / eq if eq else 0 for s in sv})
        eq_prev = eq
    return {'curve': pd.Series(dict(curve)), 'trades': trades, 'sleeve_pnl': sleeve_pnl, 'expo': pd.DataFrame(expo).mean().to_dict() if expo else {},
            'open_lots': lots}


def stats(curve):
    s = curve.dropna()
    if len(s) < 2:
        return {}
    r = s.pct_change().dropna()
    yrs = len(s) / 250
    cagr = (s.iloc[-1] / s.iloc[0]) ** (1 / yrs) - 1 if yrs > 0 else np.nan
    mdd = float((s / s.cummax() - 1).min())
    sh = float(r.mean() / r.std() * math.sqrt(250)) if r.std() else np.nan
    out = {'total': float(s.iloc[-1] / s.iloc[0] - 1), 'cagr': float(cagr), 'mdd': mdd, 'sharpe': sh, 'vol': float(r.std() * math.sqrt(250))}
    for k, (a, b) in PERIODS.items():
        w = s[(s.index >= a) & (s.index <= b)]
        if len(w) > 20:
            out[k] = {'ret': float(w.iloc[-1] / w.iloc[0] - 1), 'mdd': float((w / w.cummax() - 1).min())}
    yr = s.groupby(s.index.str[:4]).last()
    base = pd.concat([pd.Series([s.iloc[0]], index=['_']), yr.iloc[:-1]]).values
    out['years'] = {y: float(v) for y, v in zip(yr.index, yr.values / base - 1)}
    return out


def trade_stats(trades):
    out = {}
    for s in ('LVH', 'REV', 'DV', 'ON'):
        r = [x[4] for x in trades if x[0] == s]
        if r:
            out[s] = {'n': len(r), 'win': float(np.mean([v > 0 for v in r])), 'avg': float(np.mean(r)), 'worst': float(min(r))}
    return out


def benchmarks(D, start, end):
    out = {}
    k = db.etf_bars('069500', start, end)
    if len(k):
        out['KODEX 200 보유'] = k['close']
    C = D['P']['close']
    pool = D['F']['pool']
    r = C.pct_change().where(pool.shift(1).fillna(False).astype(bool)).clip(-0.3, 0.3)
    r = r[(r.index >= start) & (r.index <= end)].mean(axis=1).fillna(0)
    out['후보풀 동일가중'] = (1 + r).cumprod() * 1e7
    return out


def run(start='20231024', end='99999999', alloc=None, cap=10_000_000, progress=print):
    t0 = time.time()
    alloc = alloc or dict(ALLOC)
    progress('일봉 · 수급 읽는 중 …')
    D = load(start, end)
    end = min(end, D['P']['close'].index.max())
    progress(f"일봉 {D['P']['close'].shape[0]}일 × {D['P']['close'].shape[1]}종목 · 수급 {D['flow_src'] or '없음'} · ON ETF {len(D['on'])}일 · {time.time() - t0:.0f}초")
    cache = {}
    runs = {'★ TK자동매매 (합성)': alloc}
    for s in ('LVH', 'REV', 'DV', 'ON'):
        runs[f"{S.SLEEVES[s]['icon']} {S.SLEEVES[s]['name']}만 100%"] = {s: 1.0}
    res = {}
    for name, al in runs.items():
        progress(f'{name} 계산 중 …')
        res[name] = simulate(D, start, end, al, cap, seed_rank_cache=cache)
    bm = benchmarks(D, start, end)
    daily = pd.DataFrame({k: v['curve'] for k, v in res.items()}).pct_change()
    corr = daily[[k for k in res if k != '★ TK자동매매 (합성)']].corr().round(2)
    out = {'start': start, 'end': end, 'alloc': alloc, 'made': time.strftime('%Y-%m-%d %H:%M'), 'flow_src': D['flow_src'],
           'models': {k: {**stats(v['curve']), 'trades': trade_stats(v['trades']), 'expo': v['expo'], 'pnl': v['sleeve_pnl'],
                          'curve': [[d, round(x)] for d, x in v['curve'].items()]} for k, v in res.items()},
           'bench': {k: {**stats(v), 'curve': [[d, round(float(x) / float(v.iloc[0]) * cap)] for d, x in v.items()]} for k, v in bm.items()},
           'corr': {a: {b: (None if pd.isna(corr.at[a, b]) else float(corr.at[a, b])) for b in corr.columns} for a in corr.index}}
    md = report_md(out)
    open(os.path.join(db.DATA_DIR, 'backtest_result.md'), 'w', encoding='utf-8').write(md)
    json.dump(out, open(os.path.join(db.DATA_DIR, 'backtest_result.json'), 'w', encoding='utf-8'), ensure_ascii=False)
    progress(f'끝 · {time.time() - t0:.0f}초 · {db.DATA_DIR}\\backtest_result.md')
    return out, md


def report_md(o):
    f = lambda v: '-' if v is None or v != v else f'{v * 100:+.1f}%'
    L = [f"# TK자동매매 백테스트 ({o['start']} ~ {o['end']} · 1,000만 · 비용 반영)", '',
         f"칸 비율: {', '.join(f'{k} {v * 100:.0f}%' for k, v in o['alloc'].items())} · 수급: {o['flow_src'] or '없음(중립 0.5)'}", '',
         '| 모델 | 누적 | 연수익 | 최대낙폭 | 샤프 | 조정 기간 | 검증 기간 | 검증 낙폭 |', '|---|---|---|---|---|---|---|---|']
    rows = list(o['models'].items()) + list(o['bench'].items())
    for k, v in rows:
        a, b = v.get('조정', {}), v.get('검증', {})
        L.append(f"| {k} | {f(v.get('total'))} | {f(v.get('cagr'))} | {f(v.get('mdd'))} | {v.get('sharpe', float('nan')):.2f} | {f(a.get('ret'))} | {f(b.get('ret'))} | {f(b.get('mdd'))} |")
    years = sorted({y for _, v in rows for y in v.get('years', {})})
    L += ['', '## 연도별', '', '| 모델 | ' + ' | '.join(years) + ' |', '|---|' + '---|' * len(years)]
    for k, v in rows:
        L.append(f'| {k} | ' + ' | '.join(f(v.get('years', {}).get(y)) for y in years) + ' |')
    L += ['', '## 칸별 거래 (합성 모델 기준)', '', '| 칸 | 건수 | 승률 | 건당 | 최악 1건 | 평균 투자 비중 | 누적 손익 |', '|---|---|---|---|---|---|---|']
    m = o['models']['★ TK자동매매 (합성)']
    for s, t in m['trades'].items():
        L.append(f"| {s} | {t['n']} | {t['win'] * 100:.0f}% | {t['avg']:+.2f}% | {t['worst']:+.1f}% | {m['expo'].get(s, 0) * 100:.0f}% | {m['pnl'].get(s, 0):+,.0f}원 |")
    L += ['', '## 칸끼리 일간 수익 상관 (단독 100% 기준)', '']
    ks = list(o['corr'])
    L += ['| | ' + ' | '.join(ks) + ' |', '|---|' + '---|' * len(ks)]
    for a in ks:
        L.append(f'| {a} | ' + ' | '.join('-' if o['corr'][a][b] is None else f"{o['corr'][a][b]:+.2f}" for b in ks) + ' |')
    q = o['models']['★ TK자동매매 (합성)']
    ok1 = all(q.get(p, {}).get('ret', -1) > 0 for p in PERIODS)
    ok2 = q.get('검증', {}).get('mdd', -1) >= -0.20
    kb = o['bench'].get('KODEX 200 보유', {})
    ok3 = q.get('sharpe', 0) > kb.get('sharpe', 9)
    L += ['', '## 사전 등록 판정 (설계서 5장)',
          f"- 두 기간 모두 플러스: {'✅' if ok1 else '❌'}",
          f"- 검증 기간 최대 낙폭 −20% 이내: {'✅' if ok2 else '❌'}",
          f"- 샤프가 KODEX 200 보유보다 높음: {'✅' if ok3 else '❌'} ({q.get('sharpe', float('nan')):.2f} vs {kb.get('sharpe', float('nan')):.2f})",
          f"→ {'통과: 모의투자 그대로 진행' if ok1 and ok2 and ok3 else '미통과 항목 있음: 결과 파일을 Claude에게 보내 점검'}"]
    return '\n'.join(L)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', default='20231024')
    ap.add_argument('--end', default='99999999')
    ap.add_argument('--alloc', default='')
    a = ap.parse_args()
    al = dict(ALLOC)
    if a.alloc:
        al = {k: float(v) / 100 for k, v in (x.split('=') for x in a.alloc.split(','))}
    if not db.last_bar_day():
        print('일봉 자료가 없습니다 — TK_Run.bat → 📥 데이터 탭 → 전체 수집을 먼저 하세요')
        return 1
    _, md = run(a.start, a.end, al)
    print('\n' + md)
    return 0


if __name__ == '__main__':
    for s_ in (sys.stdout, sys.stderr):
        try:
            s_.reconfigure(encoding='utf-8', errors='replace')
        except Exception:
            pass
    sys.exit(main())
