"""
danta_models.py — 🎯 TK Danta 단타 매수 모델 8개 + 대조군 (실시간 가상 단타 · 분봉 연구실 공용)

모든 모델은 '그 분에 알 수 있는 정보'(st)만으로 매수 여부를 정한다 → 실시간(KIS 시세)과 연구실(과거 1분봉)이 같은 함수.
st: hm 'HH:MM' · price · open · high · low (그 분까지) · pc 전일 종가 · chg 등락률 · cum_vol · cum_amt · vwap
    orb_high (09:00~09:05 고가 · 모르면 None) · prev_vol 전일 거래량 · prev_chg 전일 등락률 (모르면 None)
    upper 상한가 · halted · hist [(hm, price)] 최근 분별 가격 (오래된 → 최근, 지금 분 제외)
    ask_upper 상한가에 매도 잔량이 있는지 (M7 체결 가능 · 모르면 None)

일봉 사전 점검 (Scout 전종목 일봉 2022-09~2026-09 · 비용 0.25% · 앞 ~2024 / 뒤 2025~) — 일봉으로 볼 수 있는 것만:
  추격 +5% 돌파 그날 종가 −0.45 / −0.19% · 조용한 출발(갭 0~2%)만 −0.11 / +0.01% (다음날 시가 +0.22 / +0.51%)
  전날 급등 · 거래대금 폭증 · 갭 5%↑ 추격 −1 ~ −4% · 상한가 종가 매수 → 다음날 시가 +5.3 / +6.0% (체결 가정)
  장중 −7% 매수 그날 종가 −0.39 / −0.17% · 강한 마감(+5~15% 고가권) 종가 매수 → 다음날 시가 +0.04 / +0.13%
  → 5분 고가 돌파 · VWAP 눌림 · 거래량 폭발 초기는 일봉으로 판단 불가 — 분봉 · 실시간 가상으로 판정
"""
import random

BASE_AMT = 1e9          # 누적 거래대금 10억 이상 (모든 모델 공통 유동성)


def tick(p):
    """KRX 호가 단위 (2023~)"""
    for lim, t in ((2000, 1), (5000, 5), (20000, 10), (50000, 50), (200000, 100), (500000, 500)):
        if p < lim:
            return t
    return 1000


def upper_price(pc):
    """상한가 = 전일 종가 × 1.3 을 호가 단위로 내림 (연구실용 · 실시간은 KIS 값)"""
    x = pc * 1.3
    t = tick(x)
    return int(x // t * t)


def _exit(tp, sl, ts=0.0, tg=0.0, hold_min=0, exit_by='15:15', hold_days=0, carry=1):
    return {'tp': float(tp), 'sl': float(sl), 'trail_start': float(ts), 'trail_gap': float(tg), 'hold_min': int(hold_min),
            'exit_by': exit_by, 'hold_days': int(hold_days), 'carry_limit': int(carry)}


MODELS = {
    'M1': {'name': '추격 돌파', 'win': ('09:01', '10:00'), 'exit': None, 'sort': 'chg_desc',
           'desc': '예전 v8 방식 — 등락률 +5% 이상 · 당일 고가 2% 이내 · 시가 +2% 이상 (📐 매매 규칙 탭에서 바꿀 수 있는 유일한 모델)',
           'evidence': '일봉: 그날 종가 −0.45 / −0.19% · 다음날 시가 −0.12 / +0.36% — 기준 모델'},
    'M2': {'name': '조용한 첫 급등', 'win': ('09:01', '10:00'), 'exit': _exit(8, -3, hold_days=1, exit_by='09:00'), 'sort': 'chg_desc',
           'desc': '추격 돌파 + 시가 갭 0~2%(조용한 출발) + 전날 +5% 미만 — 전날 달린 종목 · 갭 종목 추격을 뺌 · 다음날 시가 매도',
           'evidence': '일봉: 그날 종가 −0.11 / +0.01% · 다음날 시가 +0.22 / +0.51% — 추격 중 가장 나음'},
    'M3': {'name': '5분 고가 돌파', 'win': ('09:06', '10:30'), 'exit': _exit(5, -2.5, 3, 1.5), 'sort': 'chg_desc',
           'desc': '09:00~09:05 고가를 처음 넘는 순간 (등락률 +3~15%) — 장 초반 범위 돌파(ORB)',
           'evidence': '일봉으로 판단 불가 → 분봉 · 실시간으로 판정'},
    'M4': {'name': 'VWAP 눌림 반등', 'win': ('09:10', '11:00'), 'exit': _exit(4, -2, 2.5, 1.5), 'sort': 'chg_desc',
           'desc': '장중 +5% 이상 갔던 종목이 VWAP(거래량 가중 평균가)까지 밀렸다가 다시 오르는 분 — 추격 대신 눌림',
           'evidence': '일봉으로 판단 불가 → 분봉 · 실시간으로 판정'},
    'M5': {'name': '거래량 폭발 초기', 'win': ('09:01', '09:30'), 'exit': _exit(6, -3, 3, 2), 'sort': 'chg_asc',
           'desc': '09:30 전에 전날 하루 거래량의 절반을 넘긴 종목 중 아직 +3~8%만 오른 것 — 많이 오르기 전에',
           'evidence': '일봉으로 판단 불가 · 전날 거래대금 폭증 종목 추격은 −1% 이하였음(주의)'},
    'M6': {'name': '장중 급락 매수', 'win': ('09:05', '14:30'), 'exit': _exit(4, -4, carry=0), 'sort': 'chg_asc',
           'desc': '전일 종가 대비 −7 ~ −15% 로 밀린 유동성 종목 — 과매도 반등 (추격의 반대)',
           'evidence': '일봉: 그날 종가 −0.39 / −0.17% · 날짜 평균은 + (t 1.8 / 3.8) — 엇갈림'},
    'M7': {'name': '상한가 종가 매수', 'win': ('15:15', '15:19'), 'exit': _exit(0, -10, hold_days=1, exit_by='09:00'), 'sort': 'amt_desc',
           'desc': '15:15~15:19 상한가 & 상한가에 매도 잔량이 있어 실제로 살 수 있을 때만 → 다음날 시가 매도 (종가 상따)',
           'evidence': '일봉: 상한가 마감 → 다음날 시가 +5.3 / +6.0% (t 25 / 23) — 단, 잠긴 상한가는 못 삼 · 체결이 관건'},
    'M8': {'name': '강한 마감 종가 매수', 'win': ('15:15', '15:19'), 'exit': _exit(0, -8, hold_days=1, exit_by='09:00'), 'sort': 'chg_desc',
           'desc': '15:15~15:19 등락률 +5~15% · 당일 고가권(범위 상단 20%) → 다음날 시가 매도',
           'evidence': '일봉: 다음날 시가 +0.04 / +0.13% — 약함'},
    'Z': {'name': '대조군 (무작위)', 'win': ('09:01', '10:00'), 'exit': None, 'sort': None,
          'desc': 'M1이 산 그 분에, 필터를 통과한 다른 후보 중 무작위 1종목 — "아무 급등주나 산 것"보다 나은지 재는 잣대',
          'evidence': 'M1 매도 규칙과 같음'},
}
ORDER = ['M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8', 'Z']

# 검증 판정 기준 (사전 등록 — 3개월 뒤 이 기준으로 판단)
CRITERIA = {
    'live_min': 100,      # 실시간 가상 청산 건수
    'lab_min': 500,       # 분봉 연구실 건수
    't_min': 2.0,         # 날짜 평균 기준 t
    'vs_z': 0.3,          # 대조군보다 1회 평균 +0.3%p 이상
    'worst_day': -3.0,    # 최악의 하루 손익 ÷ 하루 최대 투입금(1회 금액 × 하루 최대 매수) ≥ −3%  (100만 × 5 = 500만 → −15만원)
}
CRITERIA_TEXT = [
    '실시간 가상 청산 100건 이상 + 분봉 연구실 500건 이상',
    '비용 · 슬리피지 뺀 1회 평균이 앞 · 뒤 기간 모두 플러스',
    '날짜 평균 t ≥ 2 (우연이 아닐 가능성)',
    '대조군(무작위)보다 1회 평균 +0.3%p 이상',
    '최악의 하루 손실이 하루 최대 투입금(1회 금액 × 하루 최대 매수)의 −3% 이내',
]


def exit_rule(model, main_settings):
    """모델의 매도 규칙 — M1 · Z는 '현재 설정'(📐 매매 규칙), 나머지는 모델 고정값"""
    m = MODELS[model]
    if m['exit'] is None:
        return {k: main_settings[k] for k in ('tp', 'sl', 'trail_start', 'trail_gap', 'hold_min', 'exit_by', 'hold_days', 'carry_limit')}
    return dict(m['exit'])


def window(model, main_settings=None):
    """M1 · Z는 '현재 설정'의 탐지 시각(📐 매매 규칙), 나머지는 모델 고정"""
    if model in ('M1', 'Z') and main_settings:
        return main_settings['scan_start'], main_settings['scan_end']
    return MODELS[model]['win']


def in_window(model, hm, main_settings=None):
    a, b = window(model, main_settings)
    return a <= hm <= b


def basic(st, s):
    """모든 모델 공통: 가격 · 유동성 · 거래정지"""
    if st.get('halted'):
        return '거래정지'
    if not (s['price_min'] <= st['price'] <= s['price_max']):
        return '가격'
    if (st.get('cum_amt') or 0) < BASE_AMT:
        return '거래대금'
    return ''


def m1_filter(st, s):
    """화면 '필터 통과' — M1(추격) 필터 (v8과 같음)"""
    why = []
    if not (s['price_min'] <= st['price'] <= s['price_max']):
        why.append('가격')
    if not (s['chg_min'] <= st['chg'] <= s['chg_max']):
        why.append('등락률')
    if (st.get('cum_vol') or 0) < s['vol_min']:
        why.append('거래량')
    if (st.get('cum_amt') or 0) < s['amt_min_eok'] * 1e8:
        why.append('거래대금')
    return why


def _chase(st, s, near=None, above=None, cmin=None):
    near = s['near_high'] if near is None else near
    above = s['above_open'] if above is None else above
    cmin = s['buy_chg_min'] if cmin is None else cmin
    r = []
    if st['chg'] < cmin:
        r.append(f"등락률 {st['chg']:.1f}<{cmin:g}")
    if st.get('high') and st['price'] < st['high'] * (1 - near / 100):
        r.append(f"고가 대비 {(st['price'] / st['high'] - 1) * 100:.1f}%")
    if st.get('open') and st['price'] < st['open'] * (1 + above / 100):
        r.append(f"시가 대비 {(st['price'] / st['open'] - 1) * 100:+.1f}%")
    if st.get('upper') and st['price'] >= st['upper']:
        r.append('상한가')
    return r


def signal(model, st, s):
    """→ (매수 여부, 사유 문자열)"""
    b = basic(st, s)
    if b:
        return False, b
    chg, p = st['chg'], st['price']
    if model == 'M1':
        w = m1_filter(st, s) + _chase(st, s)
        return (not w), ' · '.join(w)
    if model == 'M2':
        w = m1_filter(st, s) + _chase(st, s)
        if st.get('open') and st.get('pc'):
            gap = (st['open'] / st['pc'] - 1) * 100
            if not 0 <= gap <= 2:
                w.append(f'갭 {gap:+.1f}%')
        if st.get('prev_chg') is not None and st['prev_chg'] >= 5:
            w.append(f"전날 {st['prev_chg']:+.1f}%")
        return (not w), ' · '.join(w)
    if model == 'M3':
        o = st.get('orb_high')
        if not o:
            return False, '5분 고가 모름'
        if not 3 <= chg <= 15:
            return False, '등락률'
        prev = st['hist'][-1][1] if st.get('hist') else None
        if p <= o * 1.002:
            return False, '5분 고가 아래'
        if prev is None or prev > o:
            return False, '이미 돌파'
        if st.get('upper') and p >= st['upper']:
            return False, '상한가'
        return True, f'5분 고가 {o:,.0f} 돌파'
    if model == 'M4':
        v, h = st.get('vwap'), st.get('high')
        if not v or not h or not st.get('pc'):
            return False, 'VWAP 모름'
        if h < st['pc'] * 1.05:
            return False, '+5% 간 적 없음'
        if chg < 2:
            return False, '등락률'
        if not v <= p <= v * 1.012:
            return False, 'VWAP 근처 아님'
        if p > h * 0.97:
            return False, '눌림 아님'
        hs = [x[1] for x in (st.get('hist') or [])[-5:]]
        if not hs or min(hs) > v * 1.005:
            return False, 'VWAP 닿지 않음'
        if p <= hs[-1]:
            return False, '반등 전'
        return True, f'VWAP {v:,.0f} 반등'
    if model == 'M5':
        pv = st.get('prev_vol')
        if not pv:
            return False, '전날 거래량 모름'
        if (st.get('cum_vol') or 0) < pv * 0.5:
            return False, f"거래량 {st.get('cum_vol', 0) / pv * 100:.0f}% (전날 대비)"
        if not 3 <= chg <= 8:
            return False, '등락률'
        if st.get('open') and p < st['open']:
            return False, '시가 아래'
        return True, f"전날 거래량의 {st['cum_vol'] / pv * 100:.0f}%"
    if model == 'M6':
        if not -15 <= chg <= -7:
            return False, '등락률'
        return True, f'{chg:.1f}% 급락'
    if model == 'M7':
        if not st.get('upper') or p < st['upper']:
            return False, '상한가 아님'
        if st.get('ask_upper') is False:
            return False, '상한가 잠김 (매도 잔량 없음 · 못 삼)'
        return True, '상한가 · 매수 가능'
    if model == 'M8':
        if not 5 <= chg < 15:
            return False, '등락률'
        h, l = st.get('high'), st.get('low')
        if not h or not l or h <= l:
            return False, '고저 모름'
        loc = (p - l) / (h - l)
        if loc < 0.8:
            return False, f'고가권 아님 ({loc:.2f})'
        return True, f'강한 마감 (범위 {loc:.2f})'
    return False, ''


def rank(model, items):
    """같은 분에 여러 종목이 신호 → 사는 순서"""
    k = MODELS[model]['sort']
    if k == 'chg_desc':
        return sorted(items, key=lambda x: -x['chg'])
    if k == 'chg_asc':
        return sorted(items, key=lambda x: x['chg'])
    if k == 'amt_desc':
        return sorted(items, key=lambda x: -(x.get('cum_amt') or 0))
    return items


def pick_control(cands, exclude, seed=None):
    """대조군: 필터 통과 후보 중 무작위 1 (M1이 산 종목 · 이미 산 종목 제외)"""
    pool = [x for x in cands if x['ticker'] not in exclude]
    if not pool:
        return None
    return (random.Random(seed) if seed is not None else random).choice(pool)
