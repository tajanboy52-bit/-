"""
scout_strategies.py — 고수 기법 7종
============================================
① 미너비니 트렌드템플릿 + VCP
② 오닐 CANSLIM (기술 파트)
③ 웨인스타인 스테이지 2
④ 다르바스 박스 돌파
⑤ 와이코프 매집 (Spring)
⑥ 한국형 눌림목
⑦ 세력 매집형

각 전략은 0~100점과 진입·손절·목표가를 독립 산출.
여러 전략에 동시 매칭될수록 신뢰도가 올라간다.
"""
from scout_engines import (sma, sma_series, atr, rsi, linreg_slope,
                           detect_vcp, detect_box, find_swings, obv_series)

HORIZON = {'short': '단타', 'swing': '스윙', 'long': '중장기'}


MAX_RISK = {'short': 0.07, 'swing': 0.10, 'long': 0.15}   # 손절폭 상한 (단타 7% — 국내 흔들기 폭 반영)
MIN_R1 = {'short': 2.0, 'swing': 1.5, 'long': 2.0}          # 1차 목표 최소 R배수 (단타 2:1 — 로스 카메론)
MIN_R2 = {'short': 3.0, 'swing': 2.5, 'long': 3.5}


def _pack(name, score, horizon, entry, stop, t1, t2, notes):
    if entry <= 0 or stop <= 0 or stop >= entry:
        return None
    # 구조적 손절이 너무 깊으면 상한으로 당기고 감점 (셋업 품질 낮음)
    cap = entry * (1 - MAX_RISK[horizon])
    if stop < cap:
        stop = cap
        score -= 6
        notes = notes + ' · 손절폭 상한 적용'
    risk = entry - stop
    # 목표가가 손익비 기준에 못 미치면 R배수로 끌어올림
    t1 = max(t1, entry + risk * MIN_R1[horizon])
    t2 = max(t2, entry + risk * MIN_R2[horizon], t1 * 1.03)
    return {
        'name': name, 'score': round(score, 1), 'horizon': horizon,
        'entry': round(entry), 'stop': round(stop),
        'target1': round(t1), 'target2': round(t2),
        'risk_pct': round((stop - entry) / entry * 100, 1),
        'reward_pct': round((t1 - entry) / entry * 100, 1),
        'rr': round((t1 - entry) / risk, 2) if risk > 0 else 0,
        'notes': notes,
    }


# ════════════════════════════════════════════
#  ① 미너비니 — 트렌드 템플릿 8조건 + VCP
# ════════════════════════════════════════════
def minervini(candles, rs_rank=None):
    if len(candles) < 200:
        return None
    cl = [c['close'] for c in candles]
    px = cl[-1]
    m50, m150, m200 = sma(cl, 50), sma(cl, 150), sma(cl, 200)
    if not all([m50, m150, m200]):
        return None
    win = cl[-250:]
    lo52, hi52 = min(win), max(win)

    cond = {
        '현재가>150·200일선': px > m150 and px > m200,
        '150일선>200일선': m150 > m200,
        '200일선 상승': len(cl) >= 222 and sma_series(cl, 200)[-1] > sma_series(cl, 200)[-22],
        '50일선>150·200일선': m50 > m150 and m50 > m200,
        '현재가>50일선': px > m50,
        '52주저점 대비 +30%': (px - lo52) / lo52 >= 0.30,
        '52주고점 대비 -25% 이내': (px - hi52) / hi52 >= -0.25,
        'RS 70 이상': (rs_rank is None) or (rs_rank >= 70),
    }
    passed = sum(1 for v in cond.values() if v)
    if passed < 8:
        return None

    v = detect_vcp(candles)
    a = atr(candles, 14) or (px * 0.03)
    if v:
        entry = v['pivot'] * 1.002
        stop = entry * 0.93
        score = 92
        note = f"VCP {len(v['contractions'])}회 수축 {v['contractions']} · 거래량 {v['vol_dry']}배"
    else:
        entry = max(c['high'] for c in candles[-10:]) * 1.002
        stop = entry - a * 2.5
        score = 66
        note = '트렌드 템플릿 8조건 충족 (VCP 미형성)'
    if rs_rank:
        score = min(100, score + (rs_rank - 70) * 0.2)
    return _pack('미너비니 VCP', score, 'long', entry, stop,
                 entry + (entry - stop) * 2, entry + (entry - stop) * 4, note)


# ════════════════════════════════════════════
#  ② 오닐 CANSLIM (기술 파트)
# ════════════════════════════════════════════
def oneil(candles, rs_rank=None, fund=None, supply=None):
    if len(candles) < 150 or rs_rank is None or rs_rank < 80:
        return None
    cl = [c['close'] for c in candles]
    px = cl[-1]
    hi52 = max(cl[-250:]) if len(cl) >= 250 else max(cl)
    if px < hi52 * 0.85:
        return None
    m50 = sma(cl, 50)
    if not m50 or px < m50:
        return None

    score = 60 + (rs_rank - 80) * 1.0
    notes = [f'RS {rs_rank}']
    if fund and fund.get('op_yoy', 0) >= 25:
        score += 12
        notes.append(f"영업이익 +{fund['op_yoy']:.0f}%")
    if supply and supply.get('foreign_streak', 0) >= 3:
        score += 8
        notes.append('외인 연속매수')
    if supply and supply.get('acc_days', 0) - supply.get('dis_days', 0) >= 4:
        score += 8
        notes.append('매집 우위')

    pivot = max(c['high'] for c in candles[-15:])
    entry = pivot * 1.002
    stop = entry * 0.93
    return _pack('오닐 CANSLIM', min(100, score), 'long', entry, stop,
                 entry * 1.20, entry * 1.45, ' · '.join(notes))


# ════════════════════════════════════════════
#  ③ 웨인스타인 스테이지 2 진입
# ════════════════════════════════════════════
def weinstein(candles):
    if len(candles) < 200:
        return None
    cl = [c['close'] for c in candles]
    px = cl[-1]
    m150 = sma(cl, 150)
    if not m150:
        return None
    ser = sma_series(cl, 150)[-25:]
    slope = linreg_slope(ser)
    if px <= m150 or slope <= 0.03:
        return None
    # 최근 30일 내 150일선 상향 돌파 여부
    cross_idx = None
    ser_full = sma_series(cl, 150)
    off = len(cl) - len(ser_full)
    for i in range(max(1, len(ser_full) - 30), len(ser_full)):
        if cl[off + i - 1] <= ser_full[i - 1] and cl[off + i] > ser_full[i]:
            cross_idx = off + i
    if cross_idx is None:
        return None
    # 돌파 시 거래량 확인
    vol_cross = candles[cross_idx]['volume']
    vol_avg = sum(c['volume'] for c in candles[cross_idx - 30:cross_idx]) / 30
    if vol_avg <= 0 or vol_cross < vol_avg * 1.5:
        return None

    score = 70 + min(20, slope * 100)
    base_low = min(c['low'] for c in candles[cross_idx - 20:cross_idx + 5])
    entry = px
    stop = max(base_low, m150 * 0.96)
    return _pack('웨인스타인 St.2', min(100, score), 'long', entry, stop,
                 entry * 1.25, entry * 1.60,
                 f'30주선 돌파 · 기울기 {slope:.2f} · 돌파거래량 {vol_cross/vol_avg:.1f}배')


# ════════════════════════════════════════════
#  ④ 다르바스 박스 돌파
# ════════════════════════════════════════════
def darvas(candles):
    # 박스는 어제까지로 확정하고, 오늘 봉이 박스를 돌파했는지 본다
    box = detect_box(candles[:-1], 20)
    if not box:
        return None
    px = candles[-1]['close']
    top, bot = box['top'], box['bottom']
    # 박스 상단 근접 (돌파 대기) 또는 막 돌파
    if px < top * 0.94:
        return None
    cl = [c['close'] for c in candles]
    m60 = sma(cl, 60)
    if m60 and px < m60:
        return None
    vol_avg = sum(c['volume'] for c in candles[-21:-1]) / 20
    vr = candles[-1]['volume'] / vol_avg if vol_avg else 1

    score = 62
    notes = [f"박스폭 {box['width_pct']}%"]
    if px >= top:
        score += 15
        notes.append('상단 돌파')
    if vr >= 1.5:
        score += 10
        notes.append(f'거래량 {vr:.1f}배')
    if box['width_pct'] <= 10:
        score += 8
        notes.append('타이트 박스')

    entry = top * 1.002
    stop = bot * 0.99
    return _pack('다르바스 박스', min(100, score), 'swing', entry, stop,
                 entry + (top - bot), entry + (top - bot) * 2, ' · '.join(notes))


# ════════════════════════════════════════════
#  ⑤ 와이코프 매집 (Spring)
# ════════════════════════════════════════════
def wyckoff(candles):
    if len(candles) < 90:
        return None
    seg = candles[-90:]
    cl = [c['close'] for c in seg]
    vols = [c['volume'] for c in seg]
    avg_v = sum(vols) / len(vols)

    # 1) Selling Climax: 전반부에 거래량 3배 이상 + 하락
    sc_idx = None
    for i in range(5, 45):
        if vols[i] > avg_v * 3 and seg[i]['close'] < seg[i - 1]['close']:
            sc_idx = i
    if sc_idx is None:
        return None
    sc_low = min(c['low'] for c in seg[sc_idx:sc_idx + 5])

    # 2) Spring: 후반부에 SC 저점 재시험하되 거래량 감소
    tail = seg[sc_idx + 10:]
    if len(tail) < 15:
        return None
    sp_idx = None
    for i, c in enumerate(tail):
        if c['low'] <= sc_low * 1.03 and c['volume'] < avg_v * 0.8:
            sp_idx = i
    if sp_idx is None:
        return None
    spring_low = tail[sp_idx]['low']

    # 3) 현재 반등 중
    px = cl[-1]
    if px <= spring_low * 1.01:
        return None
    recent_high = max(c['high'] for c in tail)

    score = 68
    notes = ['SC→Spring 매집 확인']
    obv = obv_series(seg)
    if len(obv) > 30 and obv[-1] > max(obv[-30:-5]):
        score += 12
        notes.append('OBV 상승')
    if px > sma(cl, 20):
        score += 8
        notes.append('20일선 회복')

    entry = px
    stop = spring_low * 0.98
    return _pack('와이코프 매집', min(100, score), 'swing', entry, stop,
                 recent_high, recent_high + (recent_high - spring_low) * 0.6,
                 ' · '.join(notes))


# ════════════════════════════════════════════
#  ⑥ 한국형 눌림목
# ════════════════════════════════════════════
def pullback_kr(candles):
    if len(candles) < 60:
        return None
    cl = [c['close'] for c in candles]
    px = cl[-1]
    m20 = sma(cl, 20)
    if not m20:
        return None

    # 최근 30일 내 급등 구간 탐색 (5일 +20% 이상)
    surge_idx = None
    for i in range(len(candles) - 30, len(candles) - 3):
        if i < 5:
            continue
        chg = (cl[i] - cl[i - 5]) / cl[i - 5] * 100
        if chg >= 20:
            surge_idx = i
    if surge_idx is None:
        return None

    peak = max(c['high'] for c in candles[surge_idx - 2:surge_idx + 4])
    drawdown = (px - peak) / peak * 100
    if drawdown < -18 or drawdown > -2:
        return None

    # 20일선 지지
    if px < m20 * 0.97:
        return None

    # 조정 중 거래량 감소 — 급등일 대비 30% 이하
    surge_vol = max(c['volume'] for c in candles[surge_idx - 2:surge_idx + 2])
    pull_vol = sum(c['volume'] for c in candles[-5:]) / 5
    if surge_vol <= 0 or pull_vol / surge_vol > 0.45:
        return None

    # 반등 양봉 확인
    last = candles[-1]
    rebound = last['close'] > last['open']

    score = 64
    notes = [f'급등후 {drawdown:.0f}% 조정', f'거래량 {pull_vol/surge_vol:.0%}로 감소']
    if rebound:
        score += 12
        notes.append('반등 양봉')
    if pull_vol / surge_vol <= 0.25:
        score += 10
        notes.append('매물 소화 우수')
    if px > m20:
        score += 6
        notes.append('20일선 지지')

    entry = px if rebound else peak * 0.97
    stop = min(m20 * 0.96, min(c['low'] for c in candles[-5:]) * 0.99)
    return _pack('한국형 눌림목', min(100, score), 'swing', entry, stop,
                 peak, peak * 1.12, ' · '.join(notes))


# ════════════════════════════════════════════
#  ⑦ 세력 매집형
# ════════════════════════════════════════════
def accumulation_play(candles, investors=None):
    if len(candles) < 70:
        return None
    vols = [c['volume'] for c in candles]
    cl = [c['close'] for c in candles]
    base_v = sum(vols[-70:-40]) / 30 if len(vols) >= 70 else sum(vols) / len(vols)
    if base_v <= 0:
        return None

    # 거래량 5배 급증일 (10~40일 전)
    spike_idx = None
    for i in range(len(candles) - 40, len(candles) - 10):
        if i < 0:
            continue
        if vols[i] >= base_v * 5:
            spike_idx = i
    if spike_idx is None:
        return None

    after = candles[spike_idx:]
    hi = max(c['high'] for c in after)
    lo = min(c['low'] for c in after)
    if (hi - lo) / lo * 100 > 22:     # 횡보 조건
        return None

    px = cl[-1]
    recent_v = sum(vols[-3:]) / 3
    quiet_v = sum(vols[spike_idx + 3:-3]) / max(1, len(vols[spike_idx + 3:-3]))

    score = 60
    notes = [f'거래량 {vols[spike_idx]/base_v:.0f}배 급증 후 {len(after)}일 횡보']
    if investors:
        f_net = sum(r['foreign_qty'] for r in investors)
        i_net = sum(r['inst_qty'] for r in investors)
        if f_net > 0 and i_net > 0:
            score += 15
            notes.append('외인·기관 동반 순매수')
        elif f_net > 0 or i_net > 0:
            score += 8
            notes.append('수급 유입')
    if quiet_v > 0 and recent_v / quiet_v >= 1.8:
        score += 15
        notes.append('거래량 재증가')
    if px >= hi * 0.97:
        score += 8
        notes.append('상단 근접')

    entry = hi * 1.002
    stop = lo * 0.98
    return _pack('세력 매집형', min(100, score), 'swing', entry, stop,
                 entry * 1.15, entry * 1.35, ' · '.join(notes))


# ════════════════════════════════════════════
#  ⑧ 로스 카메론 모멘텀 (Warrior Trading)
# ════════════════════════════════════════════
def _mean(v):
    return sum(v) / len(v) if v else 0


def ross_cameron(candles, profile=None, news=None):
    """
    5대 조건(한국형): ① 주가 1천~3만원 ② RSI 50~80 ③ 상대거래량 5배
                     ④ 저유통(상장 5천만주↓ 또는 시총 3천억↓) ⑤ 뉴스 촉매
    ①②는 필수, ③④⑤ 중 1개 이상.
    RSI: 불플래그는 현재 50~80, 갭앤고는 급등 전날 40~75 (급등 당일은 과열이 정상)
    셋업: A) 불플래그 — 강한 폴 후 얕은 깃발, 깃발 상단 돌파
          B) 갭앤고   — 오늘 거래량 폭발 + 강한 마감, 내일 오늘 고가 돌파
    """
    if len(candles) < 45:
        return None
    cl = [c['close'] for c in candles]
    px = cl[-1]
    if not (1000 <= px <= 30000):
        return None
    r = rsi(cl, 14)
    r_prev = rsi(cl[:-1], 14)
    if r is None or r_prev is None:
        return None

    low_float = bool(profile) and ((0 < profile.get('shares', 0) <= 50_000_000) or
                                   (0 < profile.get('mktcap', 0) <= 3000))
    catalyst = bool(news and news.get('catalyst'))
    a = atr(candles, 14) or px * 0.04
    setup = None

    # ── A. 불플래그 (현재 RSI 50~80 — 깃발 구간은 과열이 식은 상태여야 함)
    n = len(candles)
    for p_end in range(n - 2, n - 8, -1):
        if not (50 <= r <= 80):
            break
        # 폴의 마지막 봉은 양봉이어야 함 (깃발 첫 음봉이 폴에 섞이는 것 방지)
        if candles[p_end]['close'] <= candles[p_end]['open']:
            continue
        for plen in (1, 2, 3):
            p_start = p_end - plen
            if p_start < 22:
                continue
            base = candles[p_start]['close']
            gain = (candles[p_end]['close'] - base) / base * 100
            if gain < 15:
                continue
            pole = candles[p_start + 1:p_end + 1]
            pv = max(c['volume'] for c in pole)
            bv = _mean([c['volume'] for c in candles[p_start - 20:p_start]])
            if bv <= 0 or pv < bv * 3:
                continue
            flag = candles[p_end + 1:]
            if not 1 <= len(flag) <= 5:
                continue
            top = max(c['high'] for c in pole)
            f_low = min(c['low'] for c in flag)
            f_high = max(c['high'] for c in flag)
            retr = (top - f_low) / (top - base) if top > base else 1
            fv = _mean([c['volume'] for c in flag])
            if retr <= 0.5 and fv <= pv * 0.6 and px > f_low:
                setup = {'type': '불플래그', 'rvol': pv / bv, 'entry': f_high * 1.002,
                         'stop': f_low * 0.99, 'mm': top - base,
                         'note': f'폴 +{gain:.0f}% · 되돌림 {retr:.0%} · 깃발 {len(flag)}일'}
                break
        if setup:
            break

    # ── B. 갭앤고 (당일 급등 + 강한 마감)
    #    급등 당일 RSI는 당연히 80을 넘으므로, 급등 '전날' RSI 40~75로 과열 여부 판정
    if not setup and 40 <= r_prev <= 75 and r <= 92:
        t = candles[-1]
        prev = candles[-2]['close']
        chg = (t['close'] - prev) / prev * 100
        bv = _mean([c['volume'] for c in candles[-21:-1]])
        rng = t['high'] - t['low']
        clv = (t['close'] - t['low']) / rng if rng > 0 else 0
        if 10 <= chg < 20 and bv > 0 and t['volume'] >= bv * 5 and clv >= 0.7:
            setup = {'type': '갭앤고', 'rvol': t['volume'] / bv,
                     'entry': t['high'] * 1.002,
                     'stop': max(t['low'], t['high'] - a * 1.5),
                     'mm': rng, 'note': f'+{chg:.1f}% · 종가위치 {clv:.0%}'}

    if not setup:
        return None

    rvol5 = setup['rvol'] >= 5
    pillars = 2 + int(rvol5) + int(low_float) + int(catalyst)
    if pillars < 3:
        return None

    score = (66 if setup['type'] == '불플래그' else 62)
    rsi_txt = f'RSI {r:.0f}' if setup['type'] == '불플래그' else f'전일RSI {r_prev:.0f}'
    notes = [setup['type'], setup['note'], rsi_txt, f"상대거래량 {setup['rvol']:.1f}배"]
    if rvol5:
        score += 6
    if low_float:
        score += 8
        notes.append('저유통')
    if catalyst:
        score += 10
        notes.append('촉매: ' + news['catalyst'][0][:18])
    notes.insert(0, f'5대조건 {pillars}/5')

    entry, stop = setup['entry'], setup['stop']
    risk = entry - stop
    return _pack('로스 카메론', min(100, score), 'short', entry, stop,
                 entry + risk * 2, entry + max(risk * 3, setup['mm']), ' · '.join(notes))


# ════════════════════════════════════════════
#  전체 실행
# ════════════════════════════════════════════
ALL = [
    ('minervini', minervini),
    ('oneil', oneil),
    ('weinstein', weinstein),
    ('darvas', darvas),
    ('wyckoff', wyckoff),
    ('pullback_kr', pullback_kr),
    ('accumulation', accumulation_play),
    ('ross', ross_cameron),
]


def run_strategies(candles, rs_rank=None, fund=None, supply=None, investors=None,
                   profile=None, news=None):
    """매칭된 전략 전부 반환 (점수 내림차순)"""
    out = []
    for key, fn in ALL:
        if key in DISABLED:
            continue
        try:
            if key == 'minervini':
                r = fn(candles, rs_rank)
            elif key == 'oneil':
                r = fn(candles, rs_rank, fund, supply)
            elif key == 'accumulation':
                r = fn(candles, investors)
            elif key == 'ross':
                r = fn(candles, profile, news)
            else:
                r = fn(candles)
        except Exception:
            r = None
        if r:
            r['key'] = key
            out.append(r)
    out.sort(key=lambda x: -x['score'])
    return out


def rs_rankings(pool_candles):
    """상대강도 백분위 — 1/3/6개월 가중 수익률"""
    scores = {}
    for tk, cd in pool_candles.items():
        if len(cd) < 130:
            continue
        cl = [c['close'] for c in cd]
        px = cl[-1]
        def ret(n):
            return (px - cl[-n]) / cl[-n] * 100 if len(cl) > n and cl[-n] > 0 else 0
        # 오닐 가중식: 최근 분기에 2배 가중
        scores[tk] = ret(20) * 0.4 + ret(60) * 0.3 + ret(120) * 0.3
    if not scores:
        return {}
    ordered = sorted(scores.items(), key=lambda x: x[1])
    n = len(ordered)
    return {tk: round((i + 1) / n * 100) for i, (tk, _) in enumerate(ordered)}


# ════════════════════════════════════════════
#  매매계획 — 매수시점 · 매도시점 · 보유기간 · 무효화
# ════════════════════════════════════════════
# hold: (최소, 최대) 거래일 / tstop: 이 기간 내 1차 미달 시 시간손절
META = {
    'minervini': {'kind': 'breakout', 'hold': (20, 60), 'tstop': 15, 'valid': 5,
                  'stop_reason': '피벗 −7%',
                  'invalid': ['돌파 후 피벗 아래로 종가 복귀', '돌파일 거래량 50일 평균 미만']},
    'oneil': {'kind': 'breakout', 'hold': (20, 60), 'tstop': 20, 'valid': 5,
              'stop_reason': '매수가 −7%',
              'invalid': ['돌파일 거래량 평균 1.4배 미만', '50일선 종가 이탈']},
    'weinstein': {'kind': 'trend', 'hold': (20, 80), 'tstop': 30, 'valid': 5,
                  'stop_reason': '베이스 저점 / 30주선 −4%',
                  'invalid': ['30주선 아래 주간 종가', '30주선 기울기 하향 전환']},
    'darvas': {'kind': 'breakout', 'hold': (3, 10), 'tstop': 10, 'valid': 3,
               'stop_reason': '박스 하단 이탈',
               'invalid': ['거래량 없는 돌파', '박스 하단 종가 이탈']},
    'wyckoff': {'kind': 'reversal', 'hold': (5, 20), 'tstop': 15, 'valid': 3,
                'stop_reason': 'Spring 저점 이탈',
                'invalid': ['Spring 저점 하향 이탈', '거래량 동반 장대음봉']},
    'pullback_kr': {'kind': 'pullback', 'hold': (3, 10), 'tstop': 7, 'valid': 3,
                    'stop_reason': '20일선 −4% / 최근 저점',
                    'invalid': ['20일선 종가 이탈', '조정 중 거래량 급증 음봉']},
    'accumulation': {'kind': 'breakout', 'hold': (5, 15), 'tstop': 12, 'valid': 5,
                     'stop_reason': '횡보 하단 이탈',
                     'invalid': ['횡보 하단 종가 이탈', '대량 거래 음봉']},
    'ross': {'kind': 'breakout', 'hold': (1, 3), 'tstop': 2, 'valid': 2,
             'stop_reason': '깃발 저점 / 당일 저점 이탈',
             'invalid': ['돌파 시 거래량 미증가', '시가 갭하락 후 돌파 실패', 'RSI 80 초과 과열']},
    'reversal': {'kind': 'score', 'hold': (1, 10), 'tstop': 10, 'valid': 1,
                 'stop_reason': '고정 손절 없음 — 실데이터에서 좁은 손절이 성과를 악화 (−15%는 재난 대비선)',
                 'invalid': ['10거래일 경과 (시간 청산)', '매수 후 외국인 대량 순매도 전환']},
    'factor': {'kind': 'score', 'hold': (10, 30), 'tstop': 20, 'valid': 2,
               'stop_reason': 'ATR×2.5 (최대 −12%)',
               'invalid': ['52주 고점 대비 −25% 아래로 이탈', '변동성 급증 (일간 변동폭 평소 2배 이상)']},
    'none': {'kind': 'score', 'hold': (3, 10), 'tstop': 7, 'valid': 2,
             'stop_reason': 'ATR 2배',
             'invalid': ['20일선 종가 이탈']},
}

# ════════════════════════════════════════════
#  KRX 호가 단위 (2023.1 코스피·코스닥 통일)
# ════════════════════════════════════════════
def tick_size(p):
    if p < 2000: return 1
    if p < 5000: return 5
    if p < 20000: return 10
    if p < 50000: return 50
    if p < 200000: return 100
    if p < 500000: return 500
    return 1000


def round_tick(p, mode='near'):
    """증권앱에 그대로 입력 가능한 가격으로 보정.
       down: 매도 지정가(익절) — 조금 낮춰 체결 확률↑
       up  : 돌파 매수가 · 손절 감시가 — 조금 높여 먼저 반응"""
    import math
    if p <= 0:
        return 0
    t = tick_size(p)
    v = {'down': math.floor, 'up': math.ceil}.get(mode, round)(p / t) * t
    # 구간 경계에서 단위가 바뀌는 경우 재보정
    t2 = tick_size(v)
    if v % t2:
        v = {'down': math.floor, 'up': math.ceil}.get(mode, round)(v / t2) * t2
    return int(v)


CHASE_LIMIT = 0.05   # 돌파 후 추격 허용 한도 (오닐 5% 룰)
WATCH_GAP = 0.07     # 돌파가가 7% 이상 멀면 관망


def build_plan(best, px):
    """전략 결과 → 매수/매도 실행계획"""
    key = best.get('key', 'none')
    m = META.get(key, META['none'])
    entry, stop = best['entry'], best['stop']
    t1, t2 = best['target1'], best['target2']
    gap = (entry - px) / px if px else 0

    # ── 매수 방식 판정
    if m['kind'] == 'breakout':
        if px < entry * 0.99:
            if gap > WATCH_GAP:
                btype, bdesc = '관망', f'돌파가까지 +{gap*100:.1f}% — 근접 시 재확인'
            else:
                btype, bdesc = '돌파 대기', f'{round_tick(entry, "up"):,}원 돌파 시 매수'
        elif px <= entry * (1 + CHASE_LIMIT):
            btype = '즉시 매수'
            bdesc = f'돌파 구간 · {round_tick(entry*(1+CHASE_LIMIT), "down"):,}원까지 추격 가능'
        else:
            btype, bdesc = '지정가 대기', f'추격 한도 초과 · {round_tick(entry, "down"):,}원 되돌림 시 매수'
    else:
        if abs(gap) <= 0.01:
            btype, bdesc = '즉시 매수', f'현재가 부근 매수 ({round_tick(entry*0.99,"up"):,}~{round_tick(entry*1.01,"down"):,})'
        elif gap > 0.01:
            btype, bdesc = '돌파 대기', f'{round_tick(entry, "up"):,}원 회복 확인 후 매수'
        else:
            btype, bdesc = '지정가 대기', f'{round_tick(entry, "down"):,}원 되돌림 시 지정가 매수'

    # ── 증권앱 입력용 호가 단위 보정
    entry = round_tick(entry, 'down' if btype == '지정가 대기' else
                       ('up' if btype in ('돌파 대기', '관망') else 'near'))
    stop = round_tick(stop, 'up')
    t1 = round_tick(t1, 'down')
    t2 = round_tick(t2, 'down')

    risk = (stop - entry) / entry * 100 if entry else 0
    r1 = (t1 - entry) / entry * 100 if entry else 0
    r2 = (t2 - entry) / entry * 100 if entry else 0
    hmin, hmax = m['hold']

    return {
        'buy_type': btype, 'buy_desc': bdesc,
        'valid_days': m['valid'],
        'entry': round(entry), 'stop': round(stop),
        'stop_pct': round(risk, 1), 'stop_reason': m['stop_reason'],
        'target1': round(t1), 't1_pct': round(r1, 1),
        't1_action': '50% 매도 · 손절선 본전으로 상향',
        'target2': round(t2), 't2_pct': round(r2, 1),
        't2_action': '잔량 전량 매도',
        'hold_min': hmin, 'hold_max': hmax, 'time_stop': m['tstop'],
        'hold_desc': f'{hmin}~{hmax}거래일 · {m["tstop"]}일 내 1차 미달 시 시간손절',
        'invalid': m['invalid'],
        'rr': round(r1 / abs(risk), 2) if risk else 0,
    }


# 탭별 허용 전략 — 단타 탭에 20~60일짜리 중장기 셋업이 섞이지 않도록
TAB_STRATS = {
    # 실데이터 검증(v2): 단타는 두 기간 모두 무작위를 이긴 다르바스만
    'short': {'darvas'},
    # 스윙·중장기는 팩터 순위로 추천하고, 전략은 '확인 신호'로만 표시
    'swing': {'darvas', 'accumulation', 'minervini', 'weinstein', 'wyckoff', 'pullback_kr'},
    'long': {'minervini', 'weinstein'},
}

# 실데이터에서 두 기간 모두 손실 → 추천에서 제외 (코드는 남겨 두고 재검증 가능)
#   오닐 CANSLIM: 승률 28%/24%, 평균 −0.76%/−1.09%
#   로스 카메론 : 평균 −1.86%/−1.29%
DISABLED = {'oneil', 'ross'}


def filter_for_tab(sts, horizon):
    """탭에 맞는 전략만 남기고, 탭 본래 기간 전략을 앞으로 정렬"""
    if horizon not in TAB_STRATS:
        return sts
    ok = [s for s in sts if s['key'] in TAB_STRATS[horizon]]
    ok.sort(key=lambda s: (s['horizon'] != horizon, -s['score']))
    return ok



def factor_best(candles):
    """팩터 추천 종목의 매매계획 원형 — 실데이터 검증으로 고른 규칙
       손절 ATR×2.5 (최대 −12%) · 1차 2R · 2차 4R · 20일 시간손절 · 최대 30일"""
    px = candles[-1]['close']
    a = atr(candles, 14) or px * 0.03
    stop = max(px - a * 2.5, px * 0.88)
    R = px - stop
    return {'name': '팩터 상위 (저변동·고점근접·과열회피)', 'key': 'factor', 'score': 0,
            'horizon': 'swing', 'entry': px, 'stop': stop,
            'target1': px + R * 2, 'target2': px + R * 4, 'rr': 2.0, 'notes': ''}



def reversal_best(candles):
    """과매도 반등 후보의 매매계획 — 전종목 백테스트 기준
       매수: 다음날 시가(현재가 부근) · 매도: 종가가 9일 EMA 이상이면 매도(1차), 20일선(2차) · 최대 10일
       −15%는 손절선이 아니라 재난 대비 참고선"""
    cl = [c['close'] for c in candles]
    px = cl[-1]
    k = 2 / 10
    e = cl[0]
    for x in cl[1:]:
        e = x * k + e * (1 - k)
    ma20 = sum(cl[-20:]) / 20
    t1 = max(e, px * 1.02)
    t2 = max(ma20, t1 * 1.03)
    return {'name': '과매도 반등 후보 (반전·외국인·기관역)', 'key': 'reversal', 'score': 0,
            'horizon': 'swing', 'entry': px, 'stop': px * 0.85, 'target1': t1, 'target2': t2,
            'rr': 0, 'notes': ''}
