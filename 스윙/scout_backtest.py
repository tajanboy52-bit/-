"""
scout_backtest.py — 워크포워드 백테스트
============================================
과거 각 시점으로 돌아가 "그날까지의 데이터만으로" 스캔 → 추천 →
매매계획(진입·분할매도·손절·시간손절)을 이후 실제 봉으로 시뮬레이션.

  python scout_backtest.py            # scout.db 실데이터로 실행
  python scout_backtest.py --synthetic # 합성 데이터 자체검증

미래 데이터 누수(look-ahead)를 막기 위해 모든 엔진에는 candles[:t+1]만 전달.
"""
import os, sys, math, random, time, json
from datetime import datetime, timedelta
from collections import defaultdict

import scout_engines as eng
import scout_strategies as strat
import scout_db as db

COST_PCT = 0.25     # 왕복 비용 (수수료 + 거래세 + 슬리피지)


# ════════════════════════════════════════════
#  합성 데이터 — 한국 주식 특성 반영
# ════════════════════════════════════════════
def synth_universe(n_stocks=120, n_days=480, seed=42, mode='regime'):
    """
    mode='regime' : 시장 팩터 + 종목별 추세 국면 전환 (모멘텀 존재)
    mode='random' : 완전 랜덤워크 (어떤 전략도 초과수익이 나오면 안 됨 → 누수 탐지용)
    """
    rnd = random.Random(seed)
    # 거래일 달력
    dates, d = [], datetime(2024, 1, 2)
    while len(dates) < n_days:
        if d.weekday() < 5:
            dates.append(d.strftime('%Y%m%d'))
        d += timedelta(days=1)

    # 시장 팩터 (국면 전환)
    mkt, regime = [], 0
    for i in range(n_days):
        if mode == 'regime' and rnd.random() < 0.015:
            regime = rnd.choice([-1, 0, 1, 1])
        drift = {-1: -0.0012, 0: 0.0, 1: 0.0010}[regime] if mode == 'regime' else 0
        mkt.append(rnd.gauss(drift, 0.011))

    uni = {}
    for s in range(n_stocks):
        tk = f"{s:06d}"
        px = rnd.choice([3000, 8000, 15000, 40000, 90000])
        beta = rnd.uniform(0.6, 1.5)
        vol = rnd.uniform(0.018, 0.038)
        base_v = rnd.uniform(2e5, 3e6)
        st_regime, out = 0, []
        for i in range(n_days):
            if mode == 'regime' and rnd.random() < 0.02:
                st_regime = rnd.choice([-1, 0, 0, 1])
            drift = {-1: -0.0020, 0: 0.0, 1: 0.0025}[st_regime] if mode == 'regime' else 0
            # 두꺼운 꼬리 (t분포 근사)
            z = rnd.gauss(0, 1) / math.sqrt(max(0.3, rnd.gammavariate(2.5, 1 / 2.5)))
            r = beta * mkt[i] + drift + vol * z * 0.8
            # 재료성 급등 (상한가 근처)
            if rnd.random() < 0.004:
                r = rnd.uniform(0.12, 0.29)
                if mode == 'random' and rnd.random() < 0.5:
                    r = -r          # 랜덤 모드는 급등·급락 대칭 (순수 무작위)
            r = max(-0.29, min(0.29, r))
            o = px * (1 + rnd.gauss(0, vol * 0.3))
            c = max(50, px * (1 + r))
            hi = max(o, c) * (1 + abs(rnd.gauss(0, vol * 0.35)))
            lo = min(o, c) * (1 - abs(rnd.gauss(0, vol * 0.35)))
            v = int(base_v * math.exp(rnd.gauss(0, 0.35)) * (1 + abs(r) * 25))
            out.append({'date': dates[i], 'open': round(o), 'high': round(hi),
                        'low': round(lo), 'close': round(c), 'volume': v})
            px = c
        uni[tk] = out
    return uni


def db_universe(min_len=150):
    """실데이터 — 모든 종목을 공통 날짜축에 맞춤 (상장 전·거래정지일은 None).
       길이를 가장 짧은 종목에 맞춰 자르면 3년 이력이 1년으로 줄어드는 문제 방지"""
    db.init_db()
    raw = {}
    for s in db.get_pool():
        cd = db.load_candles(s['ticker'], 1200)
        if len(cd) >= min_len:
            raw[s['ticker']] = cd
    if not raw:
        return {}
    dates = sorted({c['date'] for cd in raw.values() for c in cd})
    pos = {d: i for i, d in enumerate(dates)}
    uni = {}
    for tk, cd in raw.items():
        row = [None] * len(dates)
        for c in cd:
            row[pos[c['date']]] = c
        uni[tk] = row
    return uni


# ════════════════════════════════════════════
#  시점별 스캔 (서버 job_scan과 동일 로직)
# ════════════════════════════════════════════
def scan_at(uni, t, horizon='swing', top_n=5, min_value=0):
    """t번째 날까지의 데이터만으로 스캔 — 서버 job_scan과 같은 규칙(v2)
       min_value: 그 당시 20일 평균 거래대금 기준 (현재 후보풀 기준으로 과거를 고르면 성과가 부풀려짐)"""
    sliced = {}
    for tk, cd in uni.items():
        if len(cd) <= t or t < 150 or cd[t] is None:
            continue
        s = [c for c in cd[max(0, t + 1 - 260):t + 1] if c]
        if len(s) < 150:
            continue
        if min_value and sum(c['close'] * c['volume'] for c in s[-20:]) / 20 < min_value:
            continue
        sliced[tk] = s
    if not sliced:
        return [], None
    mk = eng.engine_market(sliced)
    rs = strat.rs_rankings(sliced)
    facs = eng.factor_table(sliced)
    res = []
    for tk, cd in sliced.items():
        if eng.risk_filter(cd):
            continue
        sc = eng.score_stock(cd, None, 0, None, None, mk['coef'])
        e = sc['engines']
        if horizon == 'long' and e['trend']['stage'] != 2:
            continue
        sts = strat.run_strategies(cd, rs.get(tk), None, e['supply'], None)
        sts = strat.filter_for_tab(sts, horizon)
        px = cd[-1]['close']
        fac = facs.get(tk)
        if horizon in ('swing', 'long') and fac:
            best, total = strat.factor_best(cd), fac['factor']
        else:
            if not sts:
                continue
            best = sts[0]
            total = (sc['base'] * 0.5 + best['score'] * 0.5 * 0.85) * mk['coef']
        plan = strat.build_plan(best, px)
        res.append({'ticker': tk, 'score': total, 'strategy': best['name'],
                    'key': best['key'], 'plan': plan, 'price': px,
                    'engines': {k: v['score'] for k, v in e.items()},
                    'all_strats': [x['key'] for x in sts] or [best['key']]})
    res.sort(key=lambda x: -x['score'])
    return res[:top_n], mk


# ════════════════════════════════════════════
#  매매계획 시뮬레이션 (체결 현실성 반영)
# ════════════════════════════════════════════
def simulate(plan, future, realistic=True):
    """
    future: 추천 다음날부터의 봉.
    realistic=True : 갭 발생 시 시가 체결 (갭하락 손절은 더 크게, 갭상승 진입은 더 비싸게)
    realistic=False: 서버 evaluate_tracking과 동일 (가격 정확히 체결 가정)
    """
    entry, stop, t1, t2 = plan['entry'], plan['stop'], plan['target1'], plan['target2']
    btype = plan['buy_type']
    st, ep, cur_stop, held, half = '대기', None, stop, 0, 0.0
    log = {'gap_entry': False, 'gap_stop': False, 'same_bar': False}

    for i, b in enumerate(future):
        o, h, l, c = b['open'], b['high'], b['low'], b['close']
        if st == '대기':
            if i >= plan['valid_days']:
                return {'status': '신호소멸', 'pct': 0, 'held': 0, **log}
            if btype == '즉시 매수' and i == 0:
                ep = o if realistic else entry     # 다음날 시가 매수
                st = '진입'
            elif btype in ('돌파 대기', '관망') and h >= entry:
                ep = max(o, entry) if realistic else entry
                log['gap_entry'] = realistic and o > entry
                st = '진입'
            elif btype == '지정가 대기' and l <= entry:
                ep = min(o, entry) if realistic else entry
                st = '진입'
            elif l <= stop:
                return {'status': '신호소멸', 'pct': 0, 'held': 0, **log}
            if st != '진입':
                continue
            # 진입 당일 손절선 동시 터치
            if l <= cur_stop:
                log['same_bar'] = True
                px = min(o, cur_stop) if realistic and o < cur_stop else cur_stop
                return {'status': '손절', 'pct': (px - ep) / ep * 100, 'held': 1, **log}
            continue

        held += 1
        if st == '진입':
            if l <= cur_stop:
                px = o if (realistic and o < cur_stop) else cur_stop
                log['gap_stop'] = realistic and o < cur_stop
                if h >= t1:
                    log['same_bar'] = True
                return {'status': '손절', 'pct': (px - ep) / ep * 100, 'held': held, **log}
            if h >= t1:
                fill = max(o, t1) if realistic else t1
                half = (fill - ep) / ep * 100 / 2
                cur_stop, st = ep, '1차도달'
                continue
            if held >= plan['time_stop']:
                return {'status': '시간손절', 'pct': (c - ep) / ep * 100, 'held': held, **log}
        elif st == '1차도달':
            if h >= t2:
                fill = max(o, t2) if realistic else t2
                return {'status': '2차도달', 'pct': half + (fill - ep) / ep * 100 / 2,
                        'held': held, **log}
            if l <= cur_stop:
                px = o if (realistic and o < cur_stop) else cur_stop
                return {'status': '본전청산', 'pct': half + (px - ep) / ep * 100 / 2,
                        'held': held, **log}
            if held >= plan['hold_max']:
                return {'status': '기간만료', 'pct': half + (c - ep) / ep * 100 / 2,
                        'held': held, **log}

    if st == '대기':
        return {'status': '신호소멸', 'pct': 0, 'held': 0, **log}
    last = future[-1]['close'] if future else ep
    p = (last - ep) / ep * 100
    return {'status': '미청산', 'pct': half + p / 2 if st == '1차도달' else p,
            'held': held, **log}


# ════════════════════════════════════════════
#  계획 무결성 검사
# ════════════════════════════════════════════
def check_plan(p, px):
    errs = []
    if not (p['stop'] < p['entry'] < p['target1'] < p['target2']):
        errs.append(f"가격순서 오류 stop{p['stop']} entry{p['entry']} t1{p['target1']} t2{p['target2']}")
    if p['stop_pct'] < -25:
        errs.append(f"손절폭 과대 {p['stop_pct']}%")
    if p['stop_pct'] > -1:
        errs.append(f"손절폭 과소 {p['stop_pct']}%")
    if p['rr'] < 0.8:
        errs.append(f"손익비 부족 {p['rr']}")
    return errs


# ════════════════════════════════════════════
#  실행
# ════════════════════════════════════════════
def run(uni, step=5, horizon='swing', top_n=5, realistic=True, label='', min_value=0):
    n = max(len(cd) for cd in uni.values())
    if n < 300:
        print(f"⚠ 이력 {n}일 — 백테스트에 최소 300일, 권장 750일이 필요합니다. "
              f"서버에서 증분 동기화를 한 번 돌리면 후보풀 이력이 자동 확장됩니다.")
    trades, plan_errs, fire = [], defaultdict(int), defaultdict(int)
    t0 = time.time()
    scans = 0
    regimes = defaultdict(int)
    steps = list(range(260, n - 20, step))
    for t in steps:
        picks, mk = scan_at(uni, t, horizon, top_n, min_value)
        scans += 1
        if scans % 10 == 0 or scans == len(steps):
            el = time.time() - t0
            print(f"  [{label}] {scans}/{len(steps)} 시점 · 추천 {len(trades)}건 · "
                  f"남은 약 {el / scans * (len(steps) - scans) / 60:.0f}분", flush=True)
        if mk:
            regimes[mk['regime']] += 1
        for p in picks:
            for k in p['all_strats']:
                fire[k] += 1
            for e in check_plan(p['plan'], p['price']):
                plan_errs[f"{p['strategy']}: {e.split(' ')[0]}"] += 1
            fut = [c for c in uni[p['ticker']][t + 1:t + 1 + 90] if c]
            r = simulate(p['plan'], fut, realistic)
            r.update(strategy=p['strategy'], buy_type=p['plan']['buy_type'],
                     score=p['score'], regime=mk['regime'] if mk else '-')
            trades.append(r)
    el = time.time() - t0
    return {'label': label, 'trades': trades, 'plan_errs': dict(plan_errs),
            'fire': dict(fire), 'scans': scans, 'sec': el, 'regimes': dict(regimes)}


def report(R):
    tr = [t for t in R['trades'] if t['status'] != '신호소멸']
    void = len(R['trades']) - len(tr)
    print(f"\n{'═'*66}\n  {R['label']}\n{'═'*66}")
    print(f"  스캔 {R['scans']}회 · 추천 {len(R['trades'])}건 · 체결 {len(tr)}건 · "
          f"신호소멸 {void}건 · {R['sec']:.0f}초")
    print(f"  시장국면 분포: {R['regimes']}")
    if not tr:
        print("  체결 없음")
        return
    pcts = [t['pct'] - COST_PCT for t in tr]
    win = sum(1 for p in pcts if p > 0) / len(pcts)
    avg = sum(pcts) / len(pcts)
    w = [p for p in pcts if p > 0]
    l = [p for p in pcts if p <= 0]
    pf = (sum(w) / abs(sum(l))) if l and sum(l) != 0 else float('inf')
    print(f"  승률 {win:.0%} · 평균 {avg:+.2f}% (비용 {COST_PCT}% 차감) · "
          f"평균익 {sum(w)/max(1,len(w)):+.2f}% · 평균손 {sum(l)/max(1,len(l)):+.2f}% · PF {pf:.2f}")

    by = defaultdict(list)
    for t in tr:
        by[t['strategy']].append(t['pct'] - COST_PCT)
    print(f"\n  {'전략':16s}{'건수':>5}{'승률':>7}{'평균':>9}{'보유':>6}")
    for k, v in sorted(by.items(), key=lambda x: -sum(x[1]) / len(x[1])):
        hd = [t['held'] for t in tr if t['strategy'] == k]
        print(f"  {k:16s}{len(v):>5}{sum(1 for x in v if x>0)/len(v):>7.0%}"
              f"{sum(v)/len(v):>+8.2f}%{sum(hd)/len(hd):>5.1f}일")

    st = defaultdict(int)
    for t in R['trades']:
        st[t['status']] += 1
    print(f"\n  청산 유형: {dict(st)}")
    bt = defaultdict(list)
    for t in tr:
        bt[t['buy_type']].append(t['pct'] - COST_PCT)
    print("  매수방식별: " + ' · '.join(f"{k} {len(v)}건 {sum(v)/len(v):+.2f}%" for k, v in bt.items()))
    g1 = sum(1 for t in tr if t['gap_stop'])
    g2 = sum(1 for t in tr if t['gap_entry'])
    g3 = sum(1 for t in tr if t['same_bar'])
    print(f"  체결 현실성: 갭하락 손절 {g1}건 · 갭상승 진입 {g2}건 · 동일봉 목표/손절 동시 {g3}건")
    print(f"  전략 발동 횟수(상위5 기준): {R['fire']}")
    if R['plan_errs']:
        print(f"  ⚠ 계획 무결성 오류: {R['plan_errs']}")
    else:
        print("  ✓ 계획 무결성 오류 없음")


class _Tee:
    """화면과 파일에 동시에 출력"""
    def __init__(self, path):
        self.f = open(path, 'w', encoding='utf-8')
        self.o = sys.stdout
    def write(self, x):
        self.o.write(x); self.f.write(x)
    def flush(self):
        self.o.flush(); self.f.flush()


if __name__ == '__main__':
    if '--save' in sys.argv:
        from scout_export import desktop
        _path = os.path.join(desktop(), f"backtest_결과_{datetime.now():%Y%m%d_%H%M}.txt")
        sys.stdout = _Tee(_path)
        import atexit
        atexit.register(lambda: print(f"\n결과 저장: {_path}"))
    if '--synthetic' in sys.argv:
        print("합성 데이터 생성 중...")
        u1 = synth_universe(120, 480, 42, 'regime')
        report(run(u1, label='① 국면전환 시장 (모멘텀 존재) · 현실 체결'))
        report(run(u1, realistic=False, label='② 같은 데이터 · 서버 방식(정확 체결 가정)'))
        u2 = synth_universe(120, 480, 7, 'random')
        report(run(u2, label='③ 완전 랜덤워크 · 누수 탐지 (수익 나면 버그)'))
    else:
        uni = db_universe()
        if not uni:
            print("scout.db 후보풀이 비어 있습니다. 서버에서 데이터 구축 먼저 하세요.")
            sys.exit(1)
        n = len(next(iter(uni.values())))
        _any = [c for cd in uni.values() for c in (cd[0], cd[-1]) if c]
        d0, d1 = min(c['date'] for c in _any), max(c['date'] for c in _any)
        print(f"실데이터 {len(uni)}종목 · {n}거래일 ({d0}~{d1})")
        print(f"검증 구간: 앞 260일은 지표 계산용, 이후 {max(0, n - 280)}일을 5일 간격으로 재현\n")
        for h, nm in (('swing', '스윙'), ('short', '단타')):          # 단기·스윙 전용
            report(run(uni, horizon=h, label=f'실데이터 · {nm}', min_value=3_000_000_000))
