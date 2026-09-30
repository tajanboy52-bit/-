"""
danta_exit.py — 🎯 TK Danta 매도 엔진 (가상 단타 · 분봉 연구실이 '같은 코드'로 판단)

매수 후 매도 규칙 하나(프로필)를 받아, 지금 가격으로 '팔지 · 계속 들고 갈지'를 정한다.
  · 손절 · 익절 · 트레일링 · 최대 보유(분) · 청산 시각 · 보유 거래일(0=그날 청산 · 1 · 2) · 상한가 오버나잇
  · 같은 함수를 실시간 가상 단타(10초 감시)와 분봉 연구실(과거 분봉 되감기)이 함께 씀 → 두 결과가 같은 규칙

근거 (Scout 전종목 일봉 2022-09~2026-09 · 장중 +5% 돌파 진입 16.2만 건 · 비용 0.25% · 전반/후반 기간 따로):
  · 그날 종가 청산 평균 −0.45% / −0.21% (추격 매수 자체가 불리)
  · 상한가로 마감한 종목은 다음날 시가에 팔면 종가 매도보다 +6.5% / +7.1% (t 25 / 23 · 2022~2026 매년 +)
    → 상한가 오버나잇 규칙 (carry). 상한가가 아닌 +20~29% 마감은 차이 없음(−0.02 / +0.16)
  · 다음날 종가 · 이틀 보유는 다음날 시가 매도보다 나쁨 → 오버나잇은 '다음날 시가 매도'가 기본
  · 장중 순서(손절이 먼저냐 익절이 먼저냐)는 일봉으로 알 수 없음 → 분봉 연구실 · 동시 운용 프로필로 검증
"""
from datetime import datetime, timedelta

PKEYS = ('tp', 'sl', 'trail_start', 'trail_gap', 'hold_min', 'exit_by', 'hold_days', 'carry_limit')

# 매수마다 '동시에' 기록하는 비교용 매도 프로필 (같은 매수 · 다른 매도 → 매도 규칙만의 차이를 실전 시세로)
PROFILES = {
    'A': {'name': '빠른 단타', 'desc': 'D0.2 시작값 — +3 / −2 · 트레일링 2→1.5 · 30분 · 11:00 청산',
          'p': {'tp': 3.0, 'sl': -2.0, 'trail_start': 2.0, 'trail_gap': 1.5, 'hold_min': 30, 'exit_by': '11:00', 'hold_days': 0, 'carry_limit': 0}},
    'B': {'name': '당일 추세', 'desc': '+8 / −3 · 트레일링 4→2 · 15:15 청산 · 상한가면 다음날 시가',
          'p': {'tp': 8.0, 'sl': -3.0, 'trail_start': 4.0, 'trail_gap': 2.0, 'hold_min': 0, 'exit_by': '15:15', 'hold_days': 0, 'carry_limit': 1}},
    'C': {'name': '상한가 추적', 'desc': '익절 없음 · −3 · 트레일링 6→3 · 15:15 청산 · 상한가면 다음날 시가',
          'p': {'tp': 0.0, 'sl': -3.0, 'trail_start': 6.0, 'trail_gap': 3.0, 'hold_min': 0, 'exit_by': '15:15', 'hold_days': 0, 'carry_limit': 1}},
    'D': {'name': '다음날 시가', 'desc': '익절 없음 · −3 · 그날 들고 → 다음날 시가 매도 (일봉: 그날 종가보다 +0.3~0.5%)',
          'p': {'tp': 0.0, 'sl': -3.0, 'trail_start': 0.0, 'trail_gap': 0.0, 'hold_min': 0, 'exit_by': '09:00', 'hold_days': 1, 'carry_limit': 1}},
    'E': {'name': '최대 2일', 'desc': '+10 / −4 · 트레일링 6→3 · 이틀째 15:15 청산 · 상한가면 다음날 시가',
          'p': {'tp': 10.0, 'sl': -4.0, 'trail_start': 6.0, 'trail_gap': 3.0, 'hold_min': 0, 'exit_by': '15:15', 'hold_days': 2, 'carry_limit': 1}},
}
LAST_CHECK = '15:19'        # 상한가 유지 여부를 마지막으로 보는 시각 (15:20부터 종가 단일가)
SAFETY = '15:15'            # 그날 청산 대상이면 늦어도 이때


def trading_days(d0, d1, holidays=()):
    """d0(매수일) 다음부터 d1까지 지난 거래일 수 — 평일 − 알려진 휴장일"""
    if d1 <= d0:
        return 0
    a = datetime.strptime(d0, '%Y%m%d')
    b = datetime.strptime(d1, '%Y%m%d')
    n = 0
    x = a + timedelta(days=1)
    while x <= b:
        if x.weekday() < 5 and x.strftime('%Y%m%d') not in holidays:
            n += 1
        x += timedelta(days=1)
    return n


def decide(pos, p, now, q=None, tdays=0, first=False):
    """pos: {'buy_px','buy_ts','peak','plan', 규칙 PKEYS…} · p: 지금 가격 · q: {'open','upper'} · tdays: 매수 뒤 지난 거래일
       first: 그날 첫 확인(시가 직후) — 밤사이 갭은 시가(동시호가 체결가)로 판단
       → (행동, 가격, 사유, 새 plan) · 행동 None=계속 보유 · 'sell'=매도 (가격=판단 가격 · 호가 · 슬리피지는 부르는 쪽)
       plan: '' 보통 · 'upper' 상한가라 청산 보류 중 · 'open:YYYYMMDD' 다음 거래일 시가 매도 예약"""
    q = q or {}
    hm = now.strftime('%H:%M')
    today = now.strftime('%Y%m%d')
    plan = pos.get('plan') or ''
    buy = pos['buy_px']
    upper = q.get('upper') or 0
    at_upper = bool(upper) and p >= upper
    gap = bool(first and tdays > 0)                    # 밤을 넘긴 뒤 첫 확인
    ref = (q.get('open') or p) if gap else p
    pre = '시가 ' if gap else ''

    if plan.startswith('open:'):                       # 상한가 마감 → 다음 거래일 시가
        if today > plan[5:]:
            return 'sell', ref, '상한가 마감 → 다음날 시가', ''
        return None, p, '', plan
    if plan == 'upper':                                # 상한가라 청산을 미룬 상태
        if not at_upper:
            return 'sell', p, '상한가 풀림', ''
        return None, p, '', ('open:' + today) if hm >= LAST_CHECK else plan

    chg = (ref / buy - 1) * 100
    peak = max(pos.get('peak') or buy, p)
    hd = int(pos.get('hold_days') or 0)
    if chg <= pos['sl']:
        return 'sell', ref, f"{pre}손절 {pos['sl']:g}%", plan
    if pos.get('tp') and chg >= pos['tp']:
        return 'sell', ref, f"{pre}익절 +{pos['tp']:g}%", plan
    ts, tg = pos.get('trail_start') or 0, pos.get('trail_gap') or 0
    if ts and tg and (peak / buy - 1) * 100 >= ts and p <= peak * (1 - tg / 100):
        return 'sell', p, f"트레일링 (고점 {peak:,.0f} 대비 −{tg:g}%)", plan

    why = None
    ex = min(pos['exit_by'], SAFETY)
    if tdays > hd:
        why = f'보유 기한 지남 (D+{tdays})'
    elif tdays == hd:
        if hd == 0 and pos.get('hold_min'):
            held = (now - datetime.fromisoformat(pos['buy_ts'])).total_seconds() / 60
            if held >= pos['hold_min']:
                why = f"보유 {pos['hold_min']}분"
        if not why and hm >= ex:
            why = (f'시가 매도 (D+{hd})' if ex <= '09:00' else f'{ex} 청산' + (f' (D+{hd})' if hd else ''))
    if why:
        if pos.get('carry_limit') and at_upper:        # 상한가면 팔지 않음 → 풀리면 매도 · 마감까지 유지되면 다음날 시가
            return None, p, '', ('open:' + today) if hm >= LAST_CHECK else 'upper'
        return 'sell', ref if (gap and ex <= '09:00') or tdays > hd else p, why, plan
    return None, p, '', plan
