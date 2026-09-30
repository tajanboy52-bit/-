"""
scout_engines.py — 6대 분석 엔진
============================================
E1 캔들심리      15점   최세일식 CLV·복합캔들·거래량가중·위치가중
E2 추세/이평구조  20점   정배열·스테이지·52주 위치
E3 수급/거래량    20점   매집일·OBV·외국인/기관 연속성
E4 패턴/구조      15점   VCP·컵앤핸들·쌍바닥·박스·수렴
E5 펀더멘털/재료  15점   실적·재료 (provider 주입식)
E6 시장환경      계수   0.5~1.2배 (시장 내부 breadth로 자체 산출)

순수 계산 모듈. API 호출 없음 — 전부 캐시된 일봉/수급으로 계산.
"""
import math
from datetime import datetime, timedelta

# ════════════════════════════════════════════
#  기초 지표
# ════════════════════════════════════════════
def sma(v, n):
    return sum(v[-n:]) / n if len(v) >= n else None


def sma_series(v, n):
    if len(v) < n:
        return []
    return [sum(v[i - n + 1:i + 1]) / n for i in range(n - 1, len(v))]


def ema(v, n):
    if len(v) < n:
        return None
    k = 2 / (n + 1)
    e = sum(v[:n]) / n
    for x in v[n:]:
        e = x * k + e * (1 - k)
    return e


def rsi(closes, n=14):
    if len(closes) < n + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains[:n]) / n
    al = sum(losses[:n]) / n
    for i in range(n, len(gains)):
        ag = (ag * (n - 1) + gains[i]) / n
        al = (al * (n - 1) + losses[i]) / n
    if al == 0:
        return 100.0
    return 100 - 100 / (1 + ag / al)


def macd(closes, f=12, s=26, sig=9):
    if len(closes) < s + sig:
        return None, None, None
    def _ema_series(v, n):
        k = 2 / (n + 1)
        e = sum(v[:n]) / n
        out = [e]
        for x in v[n:]:
            e = x * k + e * (1 - k)
            out.append(e)
        return out
    ef, es = _ema_series(closes, f), _ema_series(closes, s)
    ef = ef[-len(es):]
    line = [a - b for a, b in zip(ef, es)]
    if len(line) < sig:
        return line[-1], None, None
    k = 2 / (sig + 1)
    sg = sum(line[:sig]) / sig
    for x in line[sig:]:
        sg = x * k + sg * (1 - k)
    return line[-1], sg, line[-1] - sg


def atr(candles, n=14):
    if len(candles) < n + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]['high'], candles[i]['low'], candles[i - 1]['close']
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-n:]) / n


def obv_series(candles):
    out, v = [], 0.0
    for i in range(1, len(candles)):
        if candles[i]['close'] > candles[i - 1]['close']:
            v += candles[i]['volume']
        elif candles[i]['close'] < candles[i - 1]['close']:
            v -= candles[i]['volume']
        out.append(v)
    return out


def linreg_slope(v):
    """단순 선형회귀 기울기 (정규화)"""
    n = len(v)
    if n < 3:
        return 0.0
    mx = (n - 1) / 2
    my = sum(v) / n
    num = sum((i - mx) * (y - my) for i, y in enumerate(v))
    den = sum((i - mx) ** 2 for i in range(n))
    if den == 0 or my == 0:
        return 0.0
    return (num / den) / abs(my) * 100  # % / bar


# ════════════════════════════════════════════
#  E1 · 캔들심리 (15점)
# ════════════════════════════════════════════
def candle_anatomy(c):
    """봉 1개의 해부 수치"""
    R = c['high'] - c['low']
    if R <= 0:
        return None
    B = abs(c['close'] - c['open'])
    UW = c['high'] - max(c['open'], c['close'])
    LW = min(c['open'], c['close']) - c['low']
    return {
        'R': R, 'body': B / R, 'uw': UW / R, 'lw': LW / R,
        'clv': (c['close'] - c['low']) / R,
        'bull': c['close'] >= c['open'],
        'range_pct': R / c['low'] * 100 if c['low'] else 0,
    }


def candle_psychology(a):
    """단일봉 심리점수 -100 ~ +100 + 판정명"""
    if not a:
        return 0, '-'
    b, uw, lw, clv, bull = a['body'], a['uw'], a['lw'], a['clv'], a['bull']

    if bull and b >= 0.7 and clv >= 0.8:
        return 80, '장대양봉'
    if (not bull) and b >= 0.7 and clv <= 0.2:
        return -80, '장대음봉'
    if lw >= 0.6 and b <= 0.3:
        return 60, '망치형'
    if uw >= 0.6 and b <= 0.3:
        return -60, '유성형'
    if b <= 0.1:
        # 도지 — 꼬리 방향으로 미세 편향
        if lw > uw * 1.5:
            return 20, '잠자리도지'
        if uw > lw * 1.5:
            return -20, '비석도지'
        return 0, '도지'
    if bull and lw >= 0.3 and clv >= 0.6:
        return 40, '아랫꼬리양봉'
    if (not bull) and uw >= 0.3 and clv <= 0.4:
        return -40, '윗꼬리음봉'
    if bull:
        return int(30 * clv), '양봉'
    return int(-30 * (1 - clv)), '음봉'


def merge_candles(candles):
    """복합캔들 — N봉을 1봉으로 합성 (30강 핵심 개념)"""
    if not candles:
        return None
    return {
        'open': candles[0]['open'],
        'close': candles[-1]['close'],
        'high': max(c['high'] for c in candles),
        'low': min(c['low'] for c in candles),
        'volume': sum(c['volume'] for c in candles),
    }


def volume_weight(candles):
    """거래량 가중 (31강 반영)"""
    if len(candles) < 21:
        return 1.0, 1.0
    avg = sum(c['volume'] for c in candles[-21:-1]) / 20
    if avg <= 0:
        return 1.0, 1.0
    vr = candles[-1]['volume'] / avg
    if vr >= 2.5:
        return 1.5, vr
    if vr >= 1.5:
        return 1.2, vr
    if vr >= 0.8:
        return 1.0, vr
    return 0.4, vr


def position_weight(candles, signal_positive):
    """20일 고저 범위 내 위치 → 같은 캔들도 위치에 따라 의미가 뒤집힘"""
    win = candles[-20:]
    hi = max(c['high'] for c in win)
    lo = min(c['low'] for c in win)
    if hi <= lo:
        return 1.0, 0.5
    pos = (candles[-1]['close'] - lo) / (hi - lo)
    if pos <= 0.3:
        w = 1.4 if signal_positive else 0.7
    elif pos >= 0.7:
        w = 0.8 if signal_positive else 1.4
    else:
        w = 1.0
    return w, pos


def engine_candle(candles):
    """E1 — 최대 15점"""
    if len(candles) < 25:
        return {'score': 0, 'detail': '데이터부족'}
    a = candle_anatomy(candles[-1])
    raw, label = candle_psychology(a)
    vw, vr = volume_weight(candles)
    pw, pos = position_weight(candles, raw > 0)
    single = raw * vw * pw

    # 복합캔들 2/3/5봉
    best_c, best_label, best_n = 0, '', 0
    for n in (2, 3, 5):
        if len(candles) < n:
            continue
        m = merge_candles(candles[-n:])
        ma = candle_anatomy(m)
        cr, cl = candle_psychology(ma)
        val = cr * pw
        if abs(val) > abs(best_c):
            best_c, best_label, best_n = val, cl, n

    final = max(single, best_c) if (single > 0 or best_c > 0) else min(single, best_c)
    score = max(0.0, min(15.0, (final + 100) / 200 * 15))

    return {
        'score': round(score, 1),
        'label': label, 'raw': raw, 'clv': round(a['clv'], 2) if a else 0,
        'body': round(a['body'], 2) if a else 0,
        'uw': round(a['uw'], 2) if a else 0, 'lw': round(a['lw'], 2) if a else 0,
        'vol_ratio': round(vr, 2), 'vol_weight': vw,
        'pos': round(pos, 2), 'pos_weight': pw,
        'combo': f"{best_n}봉 {best_label}" if best_n else '-',
        'combo_raw': round(best_c, 1),
        'detail': f"{label}·거래량{vr:.1f}배·위치{pos:.0%}",
    }


# ════════════════════════════════════════════
#  E2 · 추세 / 이평구조 (20점)
# ════════════════════════════════════════════
def engine_trend(candles):
    if len(candles) < 60:
        return {'score': 0, 'stage': 0, 'detail': '데이터부족'}
    cl = [c['close'] for c in candles]
    px = cl[-1]
    m5, m20, m60 = sma(cl, 5), sma(cl, 20), sma(cl, 60)
    m120 = sma(cl, 120) if len(cl) >= 120 else None
    m150 = sma(cl, 150) if len(cl) >= 150 else None
    m200 = sma(cl, 200) if len(cl) >= 200 else None

    s, notes = 0.0, []
    if m5 and m20 and m60 and m5 > m20 > m60:
        if m120 is None or m60 > m120:
            s += 8
            notes.append('정배열')
        else:
            s += 5
            notes.append('단기정배열')
    elif m20 and px > m20:
        s += 2

    if m150 and m200 and px > m150 and px > m200:
        s += 4
        notes.append('장기선 위')

    # MA200 상승 추세
    if len(cl) >= 230:
        ser = sma_series(cl, 200)[-30:]
        if len(ser) >= 2 and ser[-1] > ser[0]:
            s += 3
            notes.append('200일선 상승')

    # 52주 위치
    win = cl[-250:] if len(cl) >= 250 else cl
    lo52, hi52 = min(win), max(win)
    from_low = (px - lo52) / lo52 * 100 if lo52 else 0
    from_high = (px - hi52) / hi52 * 100 if hi52 else 0
    if from_low >= 30:
        s += 3
        notes.append(f'저점대비+{from_low:.0f}%')
    if from_high >= -25:
        s += 2
        notes.append(f'고점대비{from_high:.0f}%')

    # 웨인스타인 스테이지 (30주 ≈ 150일선 기울기 + 가격위치)
    stage, stage_name = 0, '판정불가'
    if m150 and len(cl) >= 180:
        ser150 = sma_series(cl, 150)[-20:]
        slope = linreg_slope(ser150)
        above = px > m150
        if above and slope > 0.05:
            stage, stage_name = 2, 'Stage 2 상승'
        elif above and abs(slope) <= 0.05:
            stage, stage_name = 1, 'Stage 1 바닥'
        elif (not above) and slope > 0.05:
            stage, stage_name = 3, 'Stage 3 천장'
        else:
            stage, stage_name = 4, 'Stage 4 하락'

    return {
        'score': round(min(20.0, s), 1), 'stage': stage, 'stage_name': stage_name,
        'ma5': m5, 'ma20': m20, 'ma60': m60, 'ma120': m120, 'ma150': m150, 'ma200': m200,
        'from_low52': round(from_low, 1), 'from_high52': round(from_high, 1),
        'detail': ' · '.join(notes) if notes else '추세 미형성',
    }


# ════════════════════════════════════════════
#  E3 · 수급 / 거래량 (20점)
# ════════════════════════════════════════════
def accumulation_days(candles, n=25):
    """매집일(상승+거래량증가) / 분산일(하락+거래량증가)"""
    if len(candles) < n + 2:
        return 0, 0
    acc = dis = 0
    for i in range(len(candles) - n, len(candles)):
        if i < 1:
            continue
        up = candles[i]['close'] > candles[i - 1]['close']
        volup = candles[i]['volume'] > candles[i - 1]['volume']
        if volup:
            if up:
                acc += 1
            elif candles[i]['close'] < candles[i - 1]['close']:
                dis += 1
    return acc, dis


def engine_supply(candles, investors=None, mktcap=0):
    if len(candles) < 30:
        return {'score': 0, 'detail': '데이터부족'}
    s, notes = 0.0, []
    acc, dis = accumulation_days(candles)
    net = acc - dis
    if net >= 5:
        s += 7
        notes.append(f'매집우위 {acc}:{dis}')
    elif net >= 2:
        s += 4
        notes.append(f'매집 {acc}:{dis}')
    elif net <= -5:
        notes.append(f'분산우위 {acc}:{dis}')

    # OBV 다이버전스 — 가격보다 먼저 신고가
    obv = obv_series(candles)
    obv_note = ''
    if len(obv) >= 40:
        recent, prior = obv[-1], max(obv[-40:-5])
        cl = [c['close'] for c in candles]
        p_new = cl[-1] >= max(cl[-40:-1])
        o_new = recent >= prior
        if o_new and not p_new:
            s += 5
            obv_note = 'OBV 선행 신고가'
            notes.append(obv_note)
        elif o_new and p_new:
            s += 3
            obv_note = 'OBV 동반 신고가'
            notes.append(obv_note)

    # 외국인 / 기관
    f_streak = i_streak = 0
    f_sum = i_sum = 0
    if investors:
        for r in reversed(investors):
            if r['foreign_qty'] > 0:
                f_streak += 1
            else:
                break
        for r in reversed(investors):
            if r['inst_qty'] > 0:
                i_streak += 1
            else:
                break
        f_sum = sum(r['foreign_amt'] for r in investors)
        i_sum = sum(r['inst_amt'] for r in investors)
        if f_streak >= 5:
            s += 6
            notes.append(f'외인 {f_streak}일 연속매수')
        elif f_streak >= 3:
            s += 3
            notes.append(f'외인 {f_streak}일 매수')
        if mktcap > 0:
            ratio = (f_sum + i_sum) / (mktcap * 100_000_000) * 100
            if ratio >= 0.5:
                s += 5
                notes.append(f'수급강도 {ratio:.2f}%')
            elif ratio >= 0.2:
                s += 2
        elif i_streak >= 3:
            s += 2
            notes.append(f'기관 {i_streak}일 매수')

    return {
        'score': round(min(20.0, s), 1),
        'acc_days': acc, 'dis_days': dis,
        'obv': obv_note, 'foreign_streak': f_streak, 'inst_streak': i_streak,
        'foreign_amt': f_sum, 'inst_amt': i_sum,
        'detail': ' · '.join(notes) if notes else '수급 특이사항 없음',
    }


# ════════════════════════════════════════════
#  E4 · 패턴 / 구조 (15점)
# ════════════════════════════════════════════
def find_swings(candles, w=5):
    """스윙 고점/저점 추출"""
    hi, lo = [], []
    for i in range(w, len(candles) - w):
        h = candles[i]['high']
        l = candles[i]['low']
        if h == max(c['high'] for c in candles[i - w:i + w + 1]):
            hi.append((i, h))
        if l == min(c['low'] for c in candles[i - w:i + w + 1]):
            lo.append((i, l))
    return hi, lo


def detect_vcp(candles):
    """변동성 수축 — 조정폭이 회차마다 축소 + 거래량 감소"""
    if len(candles) < 60:
        return None
    hi, lo = find_swings(candles[-90:], 4)
    if len(hi) < 2 or len(lo) < 2:
        return None
    # 최근 3개 수축폭
    conts = []
    for i in range(min(3, len(hi))):
        h = hi[-(i + 1)][1]
        after = [l for l in lo if l[0] > hi[-(i + 1)][0]]
        if not after:
            continue
        l = min(x[1] for x in after)
        conts.append((h - l) / h * 100)
    if len(conts) < 2:
        return None
    conts = list(reversed(conts))  # 과거→최근
    shrinking = all(conts[i] > conts[i + 1] for i in range(len(conts) - 1))
    last = conts[-1]
    if not shrinking or last > 15:
        return None
    # 거래량 수축 확인
    v_recent = sum(c['volume'] for c in candles[-10:]) / 10
    v_base = sum(c['volume'] for c in candles[-60:-10]) / 50
    if v_base <= 0 or v_recent / v_base > 0.9:
        return None
    pivot = max(c['high'] for c in candles[-10:])
    return {'name': 'VCP', 'contractions': [round(x, 1) for x in conts],
            'pivot': pivot, 'vol_dry': round(v_recent / v_base, 2)}


def detect_box(candles, days=20):
    """다르바스 박스 — 고점 미갱신 + 저점 유지"""
    if len(candles) < days + 5:
        return None
    win = candles[-days:]
    hi = max(c['high'] for c in win)
    lo = min(c['low'] for c in win)
    if hi <= 0 or (hi - lo) / lo * 100 > 18:
        return None
    idx_hi = max(range(len(win)), key=lambda i: win[i]['high'])
    if len(win) - idx_hi < 4:      # 고점 후 3일 이상 경과 필요
        return None
    if win[-1]['close'] < lo * 1.01:
        return None
    return {'name': '다르바스 박스', 'top': hi, 'bottom': lo,
            'width_pct': round((hi - lo) / lo * 100, 1)}


def detect_double_bottom(candles):
    if len(candles) < 60:
        return None
    hi, lo = find_swings(candles[-90:], 5)
    if len(lo) < 2:
        return None
    (i1, l1), (i2, l2) = lo[-2], lo[-1]
    if i2 - i1 < 10:
        return None
    if abs(l1 - l2) / min(l1, l2) > 0.05:
        return None
    mids = [h for h in hi if i1 < h[0] < i2]
    if not mids:
        return None
    neck = max(x[1] for x in mids)
    if (neck - l2) / l2 < 0.08:
        return None
    return {'name': '쌍바닥', 'neckline': neck, 'bottom': min(l1, l2)}


def detect_cup(candles):
    """컵앤핸들 간이 판정"""
    if len(candles) < 90:
        return None
    seg = candles[-120:] if len(candles) >= 120 else candles
    n = len(seg)
    left = max(c['high'] for c in seg[:n // 5])
    bottom = min(c['low'] for c in seg[n // 5: n * 4 // 5])
    right = max(c['high'] for c in seg[n * 4 // 5:])
    depth = (left - bottom) / left * 100
    if not (12 <= depth <= 50):
        return None
    if abs(right - left) / left > 0.08:
        return None
    handle = seg[-12:]
    h_top = max(c['high'] for c in handle)
    h_low = min(c['low'] for c in handle)
    if (h_top - h_low) / h_top * 100 > depth / 2:
        return None
    return {'name': '컵앤핸들', 'depth_pct': round(depth, 1), 'pivot': h_top}


def detect_squeeze(candles):
    """변동성 수축 (ATR 비율)"""
    if len(candles) < 50:
        return None
    a_short = atr(candles[-11:], 10)
    a_long = atr(candles[-41:], 40)
    if not a_short or not a_long or a_long <= 0:
        return None
    ratio = a_short / a_long
    if ratio > 0.65:
        return None
    return {'name': '변동성 수축', 'ratio': round(ratio, 2)}


def engine_pattern(candles):
    found = []
    for fn, pts in ((detect_vcp, 7), (detect_cup, 6), (detect_double_bottom, 5),
                    (detect_box, 4), (detect_squeeze, 3)):
        try:
            r = fn(candles)
        except Exception:
            r = None
        if r:
            r['points'] = pts
            found.append(r)
    score = min(15.0, sum(f['points'] for f in found))
    return {
        'score': round(score, 1),
        'patterns': found,
        'detail': ' + '.join(f['name'] for f in found) if found else '패턴 없음',
    }


# ════════════════════════════════════════════
#  E5 · 펀더멘털 / 재료 (15점)
# ════════════════════════════════════════════
def engine_fundamental(fund=None, news=None, profile=None):
    """E5 — 중립 7점에서 출발해 재료·밸류로 가감 (0~15)
       news   : scout_ext.news_sentiment() 결과
       profile: {'per','pbr','mktcap'}
       fund   : {'op_yoy','sales_yoy','roe','op_margin_delta'} (DART 재무 연동 시)"""
    s, notes = 7.0, []
    if news:
        sc = news.get('score', 0)
        s += max(-5.0, min(4.0, sc / 15))          # 감성 ±
        if news.get('catalyst'):
            s += 2
            notes.append('재료: ' + news['catalyst'][0][:22])
        if sc >= 20:
            notes.append(f"뉴스 {news.get('label')}")
        elif sc <= -20:
            notes.append(f"뉴스 {news.get('label')}")
        if not news.get('count'):
            notes.append('최근 뉴스 없음')
    if profile:
        per = profile.get('per', 0)
        if per < 0:
            s -= 2; notes.append('적자(PER<0)')
        elif per > 200:
            s -= 1.5; notes.append(f'PER {per:.0f}')
        elif 0 < per <= 12:
            s += 1; notes.append(f'저PER {per:.1f}')
    if fund:
        oy = fund.get('op_yoy')
        if oy is not None:
            if oy >= 25:
                s += 3; notes.append(f'영업이익 +{oy:.0f}%')
            elif oy < 0:
                s -= 2
        roe = fund.get('roe')
        if roe is not None and roe >= 15:
            s += 1; notes.append(f'ROE {roe:.0f}%')
    neutral = not (news or profile or fund)
    return {'score': round(max(0.0, min(15.0, s)), 1),
            'detail': ' · '.join(notes) if notes else ('재무·재료 미연동 (중립)' if neutral else '특이사항 없음'),
            'neutral': neutral}


# ════════════════════════════════════════════
#  E6 · 시장환경 계수 (0.5 ~ 1.2)
# ════════════════════════════════════════════
def engine_market(pool_candles):
    """pool_candles: {ticker: candles} — 후보풀 전체로 시장 내부 강도 산출.
       개별종목 점수 전체에 곱해지는 마켓타이밍 계수."""
    above20 = above60 = total = 0
    up = down = 0
    new_high = 0
    for tk, cd in pool_candles.items():
        if len(cd) < 60:
            continue
        total += 1
        cl = [c['close'] for c in cd]
        m20, m60 = sma(cl, 20), sma(cl, 60)
        if m20 and cl[-1] > m20:
            above20 += 1
        if m60 and cl[-1] > m60:
            above60 += 1
        if len(cl) >= 2:
            if cl[-1] > cl[-2]:
                up += 1
            elif cl[-1] < cl[-2]:
                down += 1
        if len(cl) >= 250 and cl[-1] >= max(cl[-250:]):
            new_high += 1
    if total == 0:
        return {'coef': 1.0, 'detail': '데이터부족', 'regime': '판정불가'}

    p20 = above20 / total * 100
    p60 = above60 / total * 100
    adr = (up / down * 100) if down else 200
    nh_pct = new_high / total * 100

    score = 0
    score += 2 if p20 >= 60 else (1 if p20 >= 45 else (-1 if p20 < 30 else 0))
    score += 2 if p60 >= 55 else (1 if p60 >= 40 else (-1 if p60 < 25 else 0))
    score += 1 if adr >= 110 else (-1 if adr < 80 else 0)
    score += 1 if nh_pct >= 3 else 0

    if score >= 5:
        coef, regime = 1.2, '강세'
    elif score >= 3:
        coef, regime = 1.1, '상승'
    elif score >= 1:
        coef, regime = 1.0, '중립'
    elif score >= -1:
        coef, regime = 0.8, '약세'
    else:
        coef, regime = 0.5, '하락장'

    return {
        'coef': coef, 'regime': regime,
        'pct_above_ma20': round(p20, 1), 'pct_above_ma60': round(p60, 1),
        'adr': round(adr, 1), 'new_high_pct': round(nh_pct, 1),
        'detail': f'{regime} · MA20상회 {p20:.0f}% · ADR {adr:.0f}',
    }


# ════════════════════════════════════════════
#  리스크 하드필터
# ════════════════════════════════════════════
BLOCK_WARNS = {'관리종목', '투자경고', '투자위험', '거래정지', '정리매매'}
BLOCK_EVENTS = {'유상증자', 'CB', 'BW', 'EB', '감자', '회생절차', '영업정지', '부도'}


def risk_filter(candles, stock_row=None, events=None, warns=None, news=None):
    """탈락 사유 리스트. 비어있으면 통과."""
    reasons = []
    if len(candles) < 120:
        return ['데이터 120일 미만']
    cl = [c['close'] for c in candles]
    if len(cl) >= 2:
        chg = (cl[-1] - cl[-2]) / cl[-2] * 100
        if chg >= 20:
            reasons.append(f'당일 +{chg:.0f}% 급등')
    vals = [c['close'] * c['volume'] for c in candles[-20:]]
    if sum(vals) / len(vals) < 1_000_000_000:
        reasons.append('거래대금 10억 미만')
    # 시장경고 지정 (KIS)
    for w in (warns or []):
        if w in BLOCK_WARNS:
            reasons.append(w)
    # DART 희석성 공시 (최근 60일)
    cut = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
    for e in (events or []):
        if e.get('type') in BLOCK_EVENTS and e.get('date', '99999999') >= cut:
            reasons.append(f"{e['type']} 공시({e['date'][4:6]}/{e['date'][6:]})")
            break
    # 악재 뉴스 (자동매매 v8 차단 키워드)
    if news and news.get('hard_bad'):
        reasons.append('악재뉴스: ' + news['hard_bad'][0].split(':')[0])
    return reasons


def sector_strength(pool_candles, sectors, n=20):
    """업종별 20일 평균수익률 → 백분위 (0~100). 주도 업종 판별."""
    from collections import defaultdict
    rets = defaultdict(list)
    for tk, cd in pool_candles.items():
        sec = sectors.get(tk)
        if not sec or len(cd) <= n:
            continue
        rets[sec].append((cd[-1]['close'] - cd[-1 - n]['close']) / cd[-1 - n]['close'] * 100)
    avg = {k: sum(v) / len(v) for k, v in rets.items() if len(v) >= 3}
    if not avg:
        return {}
    ordered = sorted(avg.items(), key=lambda x: x[1])
    m = len(ordered)
    return {k: {'pct': round((i + 1) / m * 100), 'ret': round(r, 1), 'n': len(rets[k])}
            for i, (k, r) in enumerate(ordered)}


# ════════════════════════════════════════════
#  통합 채점
# ════════════════════════════════════════════
def score_stock(candles, investors=None, mktcap=0, fund=None, news=None, market_coef=1.0,
                profile=None):
    e1 = engine_candle(candles)
    e2 = engine_trend(candles)
    e3 = engine_supply(candles, investors, mktcap)
    e4 = engine_pattern(candles)
    e5 = engine_fundamental(fund, news, profile)
    base = e1['score'] + e2['score'] + e3['score'] + e4['score'] + e5['score']
    return {
        'base': round(base, 1),
        'engines': {'candle': e1, 'trend': e2, 'supply': e3,
                    'pattern': e4, 'fundamental': e5},
        'market_coef': market_coef,
    }


# ════════════════════════════════════════════
#  팩터 점수 (v2 — 실데이터 검증으로 채택)
#  2023-03~2026-09 한국 후보풀 527종목 검증 결과, 두 기간 모두 일관되게 예측력이 있던 신호만 사용:
#    ① 변동성 낮을수록 ↑  (ATR% · IC −0.10 / −0.06)
#    ② 52주 고점에 가까울수록 ↑
#    ③ 거래량 동반 급등(매집일−분산일) 과열일수록 ↓  (추격 매수가 손해였음)
#  → 동일 가중 백분위 평균 0~100. 가중치를 미세조정하지 않은 건 과최적화를 피하기 위함.
# ════════════════════════════════════════════
def factor_table(pool_candles):
    raw = {}
    for tk, cd in pool_candles.items():
        if len(cd) < 60:
            continue
        cl = [c['close'] for c in cd]
        px = cl[-1]
        a = atr(cd, 14)
        if not a or px <= 0:
            continue
        hi = max(cl[-250:])
        acc, dis = accumulation_days(cd)
        raw[tk] = {'atrp': a / px * 100, 'from_high': (px / hi - 1) * 100, 'heat': acc - dis}
    if not raw:
        return {}

    def pct(key):
        vals = sorted(v[key] for v in raw.values())
        n = len(vals)
        import bisect
        return {tk: bisect.bisect_left(vals, v[key]) / max(1, n - 1) for tk, v in raw.items()}
    p_vol, p_high, p_heat = pct('atrp'), pct('from_high'), pct('heat')
    out = {}
    for tk, v in raw.items():
        calm, near, cool = 1 - p_vol[tk], p_high[tk], 1 - p_heat[tk]
        out[tk] = {'factor': round((calm + near + cool) / 3 * 100, 1),
                   'calm': round(calm * 100), 'near_high': round(near * 100), 'cool': round(cool * 100),
                   'atrp': round(v['atrp'], 2), 'from_high': round(v['from_high'], 1), 'heat': v['heat']}
    return out


# ════════════════════════════════════════════
#  반전·수급 점수 (v3 — 전종목·생존편향 제거 데이터 검증, 2026-09-22)
#  2022-09~2026-09 전종목 3,176개(상장폐지 307개 포함)에서 두 기간 모두 방향이 일관된 신호만 사용:
#    ① RSI 낮을수록 이후 수익 ↑   (IC t +10.6 / +4.9)
#    ② 외국인 20일 순매수 강도 ↑  (IC t +4.1 / +6.2)
#    ③ 기관 20일 순매수 강도 ↓    (IC t −3.8 / −4.5 · 연기금은 더 강하나 KIS 실시간 미제공)
#  동일 가중 백분위. 1~3종목 집중 시 통계적 우위는 약함(t<1) → '후보'로만 제시.
# ════════════════════════════════════════════
def reversal_flow_table(pool_candles, investors_map, krx=None):
    """krx: {'frgn': {tk: (합,일수)}, 'pens': {...}} — KRX 연기금 데이터가 있으면 기관 대신 사용"""
    krx = krx or {}
    raw = {}
    for tk, cd in pool_candles.items():
        if len(cd) < 60:
            continue
        cl = [c['close'] for c in cd]
        r = rsi(cl, 14)
        if r is None:
            continue
        val20 = sum(c['close'] * c['volume'] for c in cd[-20:]) / 20
        inv = investors_map.get(tk) or []
        fr = inr = None
        src = ''
        kf, kp = krx.get('frgn', {}).get(tk), krx.get('pens', {}).get(tk)
        if val20 > 0 and kf and kp and kf[1] >= 10 and kp[1] >= 10:
            # KRX 원 단위 순매수 ÷ (20일 평균 거래대금 × 일수)
            fr = kf[0] / (val20 * kf[1])
            inr = kp[0] / (val20 * kp[1])
            src = '연기금'
        elif val20 > 0 and len(inv) >= 10:
            n = len(inv)
            fr = sum(x.get('foreign_amt', 0) or 0 for x in inv) / (val20 * n)
            inr = sum(x.get('inst_amt', 0) or 0 for x in inv) / (val20 * n)
            src = '기관(대체)'
        raw[tk] = {'rsi': r, 'ret20': (cl[-1] / cl[-21] - 1) * 100 if len(cl) > 20 else 0,
                   'fr20': fr, 'in20': inr, 'src': src}
    if not raw:
        return {}
    import bisect

    def pct(key, sign=1):
        vals = sorted(sign * v[key] for v in raw.values() if v[key] is not None)
        n = len(vals)
        return {tk: (bisect.bisect_left(vals, sign * v[key]) / max(1, n - 1) if v[key] is not None and n > 1 else 0.5)
                for tk, v in raw.items()}
    p_rev, p_fr, p_in = pct('rsi', -1), pct('fr20', 1), pct('in20', -1)
    out = {}
    for tk, v in raw.items():
        sc = (p_rev[tk] + p_fr[tk] + p_in[tk]) / 3
        out[tk] = {'score': round(sc * 100, 1), 'rsi': round(v['rsi'], 1), 'ret20': round(v['ret20'], 1),
                   'oversold': round(p_rev[tk] * 100), 'foreign': round(p_fr[tk] * 100),
                   'inst_contra': round(p_in[tk] * 100), 'has_flow': v['fr20'] is not None,
                   'p_rev': p_rev[tk], 'p_fr': p_fr[tk], 'p_in': p_in[tk],          # 반올림 전 백분위 (순위 계산용)
                   'contra_src': v['src']}
    return out


# ════════════════════════════════════════════
#  실전 가상매매 트랙용 — 캔들 반전패턴 · 로스카메론 RSI
#  (백테스트 행렬 정의와 1:1로 같게 작성 — 대조 검증 대상)
# ════════════════════════════════════════════
def _bar(c):
    R = c['high'] - c['low']
    body = abs(c['close'] - c['open'])
    return {'R': R, 'body': body, 'bull': c['close'] > c['open'], 'bear': c['close'] < c['open'],
            'bodyr': body / R if R > 0 else None,
            'uw': (c['high'] - max(c['open'], c['close'])) / R if R > 0 else None,
            'lw': (min(c['open'], c['close']) - c['low']) / R if R > 0 else None,
            'clv': (c['close'] - c['low']) / R if R > 0 else None}


def _down_at(cd, k):
    """k번째 봉 기준 하락추세: 10일 수익률 < 0 이고 종가 < 20일선(당일 포함)"""
    if k < 19 or k - 10 < 0:
        return False
    cl = [c['close'] for c in cd]
    return (cl[k] / cl[k - 10] - 1 < 0) and (cl[k] < sum(cl[k - 19:k + 1]) / 20)


def _avgbody(cd, k):
    """k번째 봉 이전 10봉의 평균 몸통 (k 미포함)"""
    if k - 10 < 0:
        return None
    return sum(abs(c['close'] - c['open']) for c in cd[k - 10:k]) / 10


def candle_reversal_score(cd):
    """오늘 상승반전 캔들이면 (점수, 패턴명), 아니면 (None, None).
       점수 = 종가위치(CLV) × min(3, 오늘 몸통/평균 몸통)"""
    n = len(cd)
    if n < 30:
        return None, None
    t = n - 1
    b0, b1, b2 = _bar(cd[t]), _bar(cd[t - 1]), _bar(cd[t - 2])
    if b0['R'] <= 0:
        return None, None
    O0, C0, H0 = cd[t]['open'], cd[t]['close'], cd[t]['high']
    O1, C1, H1 = cd[t - 1]['open'], cd[t - 1]['close'], cd[t - 1]['high']
    O2, C2, H2 = cd[t - 2]['open'], cd[t - 2]['close'], cd[t - 2]['high']
    ab0, ab1, ab2 = _avgbody(cd, t), _avgbody(cd, t - 1), _avgbody(cd, t - 2)
    if None in (ab0, ab1, ab2) or ab0 <= 0:
        return None, None
    long1 = b1['body'] >= ab1
    long2 = b2['body'] >= ab2
    d1, d2 = _down_at(cd, t - 1), _down_at(cd, t - 2)
    win = cd[-250:]                                # 52주 위치: 이력이 짧으면 있는 만큼 (백테스트와 동일)
    hi, lo = max(c['high'] for c in win), min(c['low'] for c in win)
    pos52 = (C0 - lo) / (hi - lo) if hi > lo else None
    pat = None
    if b0['lw'] >= 0.6 and b0['bodyr'] <= 0.3 and b0['uw'] <= 0.1 and d1:
        pat = '망치형'
    elif b1['bear'] and b0['bull'] and O0 <= C1 and C0 >= O1 and b0['body'] > b1['body'] and d1:
        pat = '상승장악형'
    elif b1['bear'] and long1 and b0['bull'] and O0 < C1 and C0 > (O1 + C1) / 2 and C0 < O1 and d1:
        pat = '관통형'
    elif (b2['bear'] and long2 and b1['bodyr'] is not None and b1['bodyr'] <= 0.3 and max(O1, C1) <= C2
          and b0['bull'] and C0 > (O2 + C2) / 2 and d2):
        pat = '샛별형'
    elif (b2['bear'] and long2 and b1['bull'] and O1 >= C2 and C1 <= O2 and C0 > max(H2, H1) and d2):
        pat = '3내부상승'
    elif b0['bodyr'] <= 0.1 and b0['lw'] >= 0.6 and pos52 is not None and pos52 <= 0.3:
        pat = '잠자리도지'
    if not pat:
        return None, None
    return b0['clv'] * min(3.0, b0['body'] / ab0), pat


# ════════════════════════════════════════════
#  차트 모델 '저변동고점' (v5.4 · 2026-09-25) — 조용히 52주 고점 근처를 지키는 종목
#  전종목(상장폐지 포함) 백테스트에서 차트 신호 중 두 기간 모두 가장 일관된 세 가지만 동일 가중:
#    ① 변동성 낮음 (ATR% · 10일 예측력 IC +0.15 / +0.14)  ② 52주 고점 근접 (+0.04 / +0.06)
#    ③ 과열 없음 — 거래량 실린 급등이 적음 (+0.06 / +0.05)
#  점수 = 세 백분위(후보풀 안 · 동점은 평균 순위)의 평균 → 상위 3종목 · 20거래일 보유
#  (투매 반전 · 기준봉 눌림 등 흔한 차트 기법은 같은 검증에서 무작위보다 못해 제외 — 보고서 18장)
# ════════════════════════════════════════════
LVHIGH_HEAT_DAYS = 25


def lvhigh_features(cd):
    """저변동고점 재료 — 이력이 짧으면 None
       atrp   = 최근 14일 평균 하루 변동폭(ATR) ÷ 종가 (낮을수록 조용)
       fromhi = 종가 ÷ 최근 250거래일 최고가 − 1 (0에 가까울수록 52주 고점 근처)
       heat   = 최근 25거래일 중 '거래량이 늘며 오른 날' − '거래량이 늘며 내린 날' (낮을수록 과열 없음)"""
    n = len(cd)
    if n < LVHIGH_HEAT_DAYS + 1:
        return None
    a = atr(cd, 14)
    c0 = cd[-1]['close']
    if not a or not c0 or c0 <= 0:
        return None
    hi = max(x['high'] for x in cd[-250:])
    heat = 0
    for k in range(n - LVHIGH_HEAT_DAYS, n):
        if cd[k]['volume'] > cd[k - 1]['volume']:
            if cd[k]['close'] > cd[k - 1]['close']:
                heat += 1
            elif cd[k]['close'] < cd[k - 1]['close']:
                heat -= 1
    return {'atrp': a / c0, 'fromhi': (c0 / hi - 1) if hi > 0 else None, 'heat': heat}


def pct_avg(vals):
    """백분위 (0~1) — 동점은 평균 순위 · 값이 없으면 0.5 (과열 지수는 정수라 동점이 많아 임의 순서 대신 평균)"""
    have = sorted((v, t) for t, v in vals.items() if v is not None)
    n = len(have)
    out = {t: 0.5 for t in vals}
    if n <= 1:
        return out
    i = 0
    while i < n:
        j = i
        while j + 1 < n and have[j + 1][0] == have[i][0]:
            j += 1
        r = (i + j) / 2 / (n - 1)
        for k in range(i, j + 1):
            out[have[k][1]] = r
        i = j + 1
    return out


def lvhigh_scores(feats):
    """feats: {종목: lvhigh_features 결과} → ({종목: 점수 0~1}, (저변동, 고점근접, 과열없음) 백분위)
       재료가 없는 종목은 점수 없음 (백테스트와 동일)"""
    ok = {t: f for t, f in feats.items() if f and f.get('fromhi') is not None}
    r_lv = pct_avg({t: -f['atrp'] for t, f in ok.items()})
    r_hi = pct_avg({t: f['fromhi'] for t, f in ok.items()})
    r_ht = pct_avg({t: -f['heat'] for t, f in ok.items()})
    return {t: (r_lv[t] + r_hi[t] + r_ht[t]) / 3 for t in ok}, (r_lv, r_hi, r_ht)


# ════════════════════════════════════════════
#  종가베팅 (v5.6) — 최세일식 종가베팅 조건을 그대로 · 전종목 백테스트(2026-09-26) · 독립 재계산으로 선정 일치 확인
#  장 마감 뒤 확정 일봉으로 판단 → 그날 종가(장후 시간외 종가 15:40~16:00)에 매수 → 다음 거래일 시가 매도
#  조건: 양봉 · 당일 +2% 초과 · 종가가 당일 범위 상단 20% 안(CLV ≥ 0.8) · 20일선 위 ·
#        직전 60일 최고가의 97~100% (전고점 바로 아래까지 바짝) · 오늘 거래대금 ≥ 직전 20일 평균 × 1.5
#  여러 종목이면 당일 상승률 높은 순 (순위 후보 4개 중 조정 기간에서만 선택)
# ════════════════════════════════════════════
JONGGA = {'up': 0.02, 'clv': 0.8, 'near': 0.03, 'spike': 1.5, 'hi_days': 60, 'tv_days': 20}
JONGGA_LABELS = (('bull', '양봉'), ('up', '+2% 넘게 상승'), ('clv', '고가 근처 마감 (당일 범위 상단 20%)'), ('ma20', '20일선 위'),
                 ('near', '60일 전고점의 97~100%'), ('spike', '거래대금 20일 평균 1.5배 이상'))


def jongga_features(cd):
    """오늘(마지막 봉) 기준 종가베팅 조건 재료 — cd: 일봉(오래된 → 최신), 62봉 이상 필요 · 거래 없는 날(정지)은 전고점 · 평균 거래대금에서 뺌
       반환: {'chg','clv','ma20','hi60','near','tv','tv20','spike','checks':{조건: 통과},'ok'} 또는 None"""
    if not cd or len(cd) < JONGGA['hi_days'] + 2:
        return None
    x, p = cd[-1], cd[-2]
    o, h, l, c, v = x['open'], x['high'], x['low'], x['close'], x['volume']
    if not (c and p['close'] and h and l) or not v or v <= 0:
        return None
    chg = c / p['close'] - 1
    rg = h - l
    clv = (c - l) / rg if rg > 0 else None
    ma20 = sum(y['close'] for y in cd[-20:]) / 20
    prior = [y['high'] for y in cd[-JONGGA['hi_days'] - 1:-1] if y['volume'] and y['volume'] > 0 and y['high']]
    hi60 = max(prior) if prior else None
    tvs = [y['close'] * y['volume'] for y in cd[-JONGGA['tv_days'] - 1:-1] if y['volume'] and y['volume'] > 0]
    tv20 = sum(tvs) / len(tvs) if len(tvs) >= JONGGA['tv_days'] * 0.8 else None
    tv = c * v
    checks = {'bull': c > o, 'up': chg > JONGGA['up'], 'clv': clv is not None and clv >= JONGGA['clv'], 'ma20': c > ma20,
              'near': hi60 is not None and hi60 * (1 - JONGGA['near']) <= c <= hi60,
              'spike': tv20 is not None and tv >= JONGGA['spike'] * tv20}
    return {'chg': chg, 'clv': clv, 'ma20': ma20, 'hi60': hi60, 'near': (c / hi60) if hi60 else None,
            'tv': tv, 'tv20': tv20, 'spike': (tv / tv20) if tv20 else None, 'checks': checks, 'ok': all(checks.values())}


def jongga_scores(feats):
    """feats: {종목: jongga_features 결과} → 조건을 모두 통과한 종목만 {종목: 당일 상승률} (높은 순으로 최대 3종목 매수)"""
    return {t: f['chg'] for t, f in feats.items() if f and f.get('ok')}


# ════════════════════════════════════════════
#  SCOUT v6.2 동결 신호 (AUTO_SIGNAL_V62_FROZEN_20260922) — 외부 시스템 모델을 트랙으로 편입
#  원본 공식·가중치 그대로. 차이: 수정주가 사용(원본은 원시가격이라 액면분할 시 왜곡), 거래대금 = 종가×거래량
#  전종목 데이터로 원본 신호 20/20 재현 확인 (2026-09-22)
# ════════════════════════════════════════════
V62_WEIGHTS = {'fr5_rank': 0.09, 'fr20_rank': 0.165, 'ir5_rank': 0.07, 'ir20_rank': 0.12, 'fpersist20': 0.04,
               'ipersist20': 0.04, 'faccel_rank': 0.03, 'iaccel_rank': 0.03, 'mom5_rank': 0.03, 'mom20_rank': 0.07,
               'mom60_rank': 0.03, 'trend20_rank': 0.04, 'trend60_rank': 0.04, 'valaccel_rank': 0.03,
               'rsi_score': 0.03, 'breakout20_rank': 0.025, 'range_pos20_rank': 0.015, 'close_pos_rank': 0.015,
               'lowvol_rank': 0.035, 'lowatr_rank': 0.025, 'doji_confirm': 0.015, 'bull_engulf': 0.0075,
               'hammer': 0.0075}


def _mean_min(xs, minp):
    v = [x for x in xs if x is not None]
    return sum(v) / len(v) if len(v) >= minp else None


def v62_features(cd, frgn, inst):
    """cd: 일봉(오래된→최신), frgn/inst: 같은 날짜순 최근 20일 순매수 금액 리스트(없는 날 None)"""
    n = len(cd)
    if n < 2:
        return None
    cl = [c['close'] for c in cd]
    val = [c['close'] * c['volume'] for c in cd]
    t = n - 1
    f = {}
    val5 = sum(val[-5:]) if n >= 5 else None
    val20 = sum(val[-20:]) if n >= 20 else None
    fv5 = [x for x in frgn[-5:] if x is not None]; fv20 = [x for x in frgn[-20:] if x is not None]
    iv5 = [x for x in inst[-5:] if x is not None]; iv20 = [x for x in inst[-20:] if x is not None]
    f5 = sum(fv5) if len(fv5) >= 3 else None; f20 = sum(fv20) if len(fv20) >= 10 else None
    i5 = sum(iv5) if len(iv5) >= 3 else None; i20 = sum(iv20) if len(iv20) >= 10 else None
    div = lambda a, b: (a / b) if (a is not None and b) else None
    f['fr5_short'] = div(f5, val20); f['fr20'] = div(f20, val20)
    f['ir5_short'] = div(i5, val20); f['ir20'] = div(i20, val20)
    fr5, ir5 = div(f5, val5), div(i5, val5)
    f['faccel'] = (fr5 - f['fr20']) if (fr5 is not None and f['fr20'] is not None) else None
    f['iaccel'] = (ir5 - f['ir20']) if (ir5 is not None and f['ir20'] is not None) else None
    f['fpersist20'] = (sum(1 for x in fv20 if x > 0) / len(fv20)) if len(fv20) >= 10 else None
    f['ipersist20'] = (sum(1 for x in iv20 if x > 0) / len(iv20)) if len(iv20) >= 10 else None
    for k in (5, 20, 60):
        f[f'mom{k}'] = (cl[t] / cl[t - k] - 1) if n > k and cl[t - k] else None
    ma20 = sum(cl[-20:]) / len(cl[-20:]) if n >= 15 else None
    ma60 = sum(cl[-60:]) / len(cl[-60:]) if n >= 40 else None
    f['ma60'] = ma60
    f['trend20'] = (cl[t] / ma20 - 1) if ma20 else None
    f['trend60'] = (ma20 / ma60 - 1) if (ma20 and ma60) else None
    f['valaccel'] = ((val5 / 5) / (val20 / 20) - 1) if (val5 and val20) else None
    rets = [cl[k] / cl[k - 1] - 1 for k in range(max(1, n - 20), n)]
    if len(rets) >= 15:
        m = sum(rets) / len(rets)
        f['vol20'] = (sum((r - m) ** 2 for r in rets) / len(rets)) ** 0.5
    else:
        f['vol20'] = None
    trs = [max(cd[k]['high'] - cd[k]['low'], abs(cd[k]['high'] - cl[k - 1]), abs(cd[k]['low'] - cl[k - 1]))
           for k in range(max(1, n - 14), n)]
    atr = sum(trs) / len(trs) if len(trs) >= 10 else None
    f['atr14p'] = atr / cl[t] if atr else None
    ch = [cl[k] - cl[k - 1] for k in range(max(1, n - 14), n)]
    if len(ch) >= 10:
        ag = sum(max(x, 0) for x in ch) / len(ch); al = sum(max(-x, 0) for x in ch) / len(ch)
        f['rsi14'] = (100 - 100 / (1 + ag / al)) if al > 0 else None
    else:
        f['rsi14'] = None
    hi_prev = [c['high'] for c in cd[-21:-1]]
    hh_prev = max(hi_prev) if len(hi_prev) >= 15 else None
    hh20 = max(c['high'] for c in cd[-20:]) if n >= 15 else None
    ll20 = min(c['low'] for c in cd[-20:]) if n >= 15 else None
    f['breakout20'] = (cl[t] / hh_prev - 1) if hh_prev else None
    f['range_pos20'] = ((cl[t] - ll20) / (hh20 - ll20)) if (hh20 and ll20 is not None and hh20 > ll20) else None
    c0, c1 = cd[t], cd[t - 1]
    rng = c0['high'] - c0['low']
    f['close_pos'] = ((c0['close'] - c0['low']) / rng) if rng > 0 else None
    prng = c1['high'] - c1['low']
    body0 = abs(c0['close'] - c0['open'])
    lower = (min(c0['open'], c0['close']) - c0['low']) / rng if rng > 0 else None
    upper = (c0['high'] - max(c0['open'], c0['close'])) / rng if rng > 0 else None
    bodyr = body0 / rng if rng > 0 else None
    f['doji_confirm'] = 1.0 if (prng > 0 and abs(c1['close'] - c1['open']) / prng <= .10 and c0['close'] > c0['open']
                                and c0['close'] > c1['high']) else 0.0
    f['bull_engulf'] = 1.0 if (c1['close'] < c1['open'] and c0['close'] > c0['open'] and c0['open'] <= c1['close']
                               and c0['close'] >= c1['open']) else 0.0
    f['hammer'] = 1.0 if (lower is not None and lower >= .55 and bodyr <= .35 and upper <= .20
                          and c0['close'] >= c0['open']) else 0.0
    f['close'] = cl[t]
    return f


def v62_scores(feats):
    """feats: {ticker: v62_features 결과} — 그날 데이터가 있는 전 종목(유동성 필터 전) 기준 백분위 → 가중 합"""
    import bisect

    def pct(key, invert=False):
        vals = sorted(v[key] for v in feats.values() if v and v.get(key) is not None)
        n = len(vals)
        out = {}
        for tk, v in feats.items():
            x = v.get(key) if v else None
            if x is None or n < 2:
                out[tk] = None
                continue
            lo = bisect.bisect_left(vals, x); hi = bisect.bisect_right(vals, x)
            r = ((lo + 1 + hi) / 2) / n                   # pandas rank(pct=True, method='average')
            out[tk] = 1 - r if invert else r
        return out
    R = {'fr5_rank': pct('fr5_short'), 'fr20_rank': pct('fr20'), 'ir5_rank': pct('ir5_short'), 'ir20_rank': pct('ir20')}
    for k in ('faccel', 'iaccel', 'mom5', 'mom20', 'mom60', 'trend20', 'trend60', 'valaccel', 'breakout20',
              'range_pos20', 'close_pos'):
        R[k + '_rank'] = pct(k)
    R['lowvol_rank'] = pct('vol20', True); R['lowatr_rank'] = pct('atr14p', True)
    out = {}
    for tk, v in feats.items():
        if not v:
            continue
        rs = v.get('rsi14')
        comp = {k: R[k].get(tk) for k in R}
        comp['rsi_score'] = None if rs is None else max(0.0, min(1.0, 1 - abs(rs - 65) / 35))
        for k in ('fpersist20', 'ipersist20', 'doji_confirm', 'bull_engulf', 'hammer'):
            comp[k] = v.get(k)
        out[tk] = sum(w * (0.5 if comp.get(k) is None else comp[k]) for k, w in V62_WEIGHTS.items())
    return out
