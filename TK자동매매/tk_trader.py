"""
tk_trader.py — TK자동매매 매매 · 일정 (모의 → 실전 같은 코드 · 모드별 장부)

하루 흐름 (거래일)
  08:05  휴장일 확인 (실전 키가 있으면 KIS 공식 휴장일 조회 CTCA0903R 하루 1번 · 없으면 내장 목록 + 거절 메시지로 감지)
  08:20  장전 점검 — KIS 연결 · 잔고 · 앱이 모르는 보유 종목 · 신호 날짜 · 데이터 품질 (주문 없음 · 문제면 텔레그램)
  08:50  장전 시장가(설정 preopen_time): ① 매도 표시 묶음(LVH 20일 · REV 9EMA/10일 · DV 교체 · 어제 산 ON ETF) ② 예상체결가로 갭 확인 → +5% 넘게 갭상승한
         LVH · REV 후보는 안 삼 ③ 새 매수(현금 안에서) → 시가 체결 · 현금이 모자란 매수만큼 KODEX 200(남는 현금)을 팜
  09:02  현금이 모자라 미뤄 둔 매수 (아침에 판 돈으로 · 매수가능금액 조회) · 장전 시간 때문에 거절된 주문 한 번 더
  장중   체결 반영(60초 · 웹소켓 체결 통보 즉시) · 계좌 실시간 평가 · 하루 손실 안전장치 · 매시 텔레그램 (선택)
  15:10  밤사이 칸 매수 자금이 모자라면 KODEX 200 일부 매도 (연속 매매 · 바로 체결)
  15:20  🌙 ON: KODEX 코스닥150 장마감 동시호가 시장가 (종가 체결) → 💤 남는 현금(평가액 5% 남김)으로 KODEX 200 매수 (설정 sweep_on)
  15:45  체결 마감 · 잔고 대조 · 평가 기록 · 계좌 안전장치
  장 마감 뒤: 자료 수집(tk_collect) → 신호 계산(signal_job) → 텔레그램 (tk_server 일정)
손절 없음 (전종목 검증: 손절은 모든 모델의 평균 수익을 깎음) — 꼬리 위험은 종목당 비중으로
안전: 자동주문 기본 OFF · 결과 불분명 주문 → 정지 · 모르는 보유 종목 → 새 매수 차단 · 1회 · 하루 한도 · 긴급 정지
모의 ↔ 실전: 같은 시스템 · 바뀌는 것은 계좌(앱키 · 시크릿 · 계좌번호 · 운용 한도)와 장부 파일뿐 · 모든 주문 · 체결 · 판단은 tk_journal에 쌓음
"""
import json
import threading
import time
from datetime import datetime, timedelta

import pandas as pd

import tk_config as CF
import tk_db as db
import tk_journal as J
import tk_signals as S
from tk_kis import KIS, KISError

HOLIDAYS = {'20261005', '20261009', '20261225', '20261231',
            '20270101', '20270208', '20270209', '20270301', '20270503', '20270505', '20270513', '20270719', '20270816',
            '20270914', '20270915', '20270916', '20271004', '20271011', '20271227', '20271231'}
SLOTS = {'LVH': 40, 'REV': 30, 'DV': 15}
DEFAULT_ALLOC = {'LVH': 40, 'REV': 25, 'DV': 0, 'ON': 35}         # 회전형: 배당·가치(장기 보유) 0 · 밤사이 35% (설계서 14장)
COSTS = {'LVH': 0.25, 'REV': 0.25, 'DV': 0.25, 'ON': 0.05}        # 손익 표시용 왕복 비용 추정 %
KIND = {'entry': '매수', 'hold20': '보유 기간 끝(LVH 10일)', 'ema9': '9EMA 복귀', 'hold10': '10일 만료', 'dv_rebal': '배당·가치 교체', 'on_buy': '밤사이 매수(종가)',
        'on_sell': '밤사이 매도(시가)', 'manual': '수동', 'delist': '거래 끊김 정리', 'sw_buy': '남는 현금 → KODEX 200', 'sw_sell': 'KODEX 200 → 현금'}
STATE = {'running': False, 'last_sync': '', 'last_err': ''}
_lock = threading.Lock()
NOTIFY = None                     # tk_server가 텔레그램 함수를 넣어 줌


def now():
    return datetime.now()


def today():
    return now().strftime('%Y%m%d')


def log(msg, level='info'):
    db.log(f'[매매] {msg}', level)


def is_trading_day(d=None):
    d = d or today()
    o = db.gmeta_get(f'open_{d}')                                      # KIS 공식 휴장일 조회 결과 (Y/N)
    if o in ('Y', 'N'):
        return o == 'Y'
    return datetime.strptime(d, '%Y%m%d').weekday() < 5 and d not in HOLIDAYS and db.gmeta_get(f'closed_{d}') != '1'


def prev_trading_day(d):
    t = datetime.strptime(d, '%Y%m%d')
    for _ in range(15):
        t -= timedelta(days=1)
        s = t.strftime('%Y%m%d')
        if t.weekday() < 5 and s not in HOLIDAYS:
            return s
    return ''


def next_trading_day(d):
    t = datetime.strptime(d, '%Y%m%d')
    for _ in range(15):
        t += timedelta(days=1)
        s = t.strftime('%Y%m%d')
        if t.weekday() < 5 and s not in HOLIDAYS:
            return s
    return ''


def slots(cfg):
    """칸별 자리 수 — 설정(실험)에서 바꿀 수 있음 · 백테스트도 같은 값을 씀"""
    return {**SLOTS, **{k: int(v) for k, v in (cfg.get('slots') or {}).items() if k in SLOTS and v}}


def picks(cfg):
    """LVH · REV 하루 매수 후보: (건너뛸 순위, 하루 수) — 기본 (0, 3) = 상위 1~3"""
    sk = cfg.get('pick_skip') or {}
    return {'LVH': (int(sk.get('LVH') or 0), S.LVH['top']), 'REV': (int(sk.get('REV') or 0), S.REV['top'])}


def gap_limit(cfg):
    v = cfg.get('gap_skip', S.GAP_SKIP)
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def gap_check(cfg, kc, o, when='pre'):
    """LVH · REV 매수 전 시가 갭 확인 → 넘으면 이유 문자열 (장전: 예상체결가 · 장중: 오늘 시가) · 조회 실패면 그냥 삼"""
    lim = gap_limit(cfg)
    if not lim or o.get('sleeve') not in ('LVH', 'REV'):
        return None
    try:
        if when == 'pre':
            if not hasattr(kc, 'expected'):
                return None
            g = kc.expected(o['ticker'])['gap']
        else:
            p = kc.price(o['ticker'])
            base = p['price'] / (1 + p['chg'] / 100) if p['price'] else 0
            g = (p['open'] / base - 1) * 100 if p['open'] and base else None
    except Exception as e:
        log(f"갭 확인 실패 {o.get('name')} (그대로 삼): {CF.clean(e)[:80]}", 'warn')
        return None
    if g is not None and g > lim:
        return f'시가 갭 {g:+.1f}% > +{lim:g}% (밤사이 과잉반응 → 안 삼)'
    return None


def sweep_on(cfg):
    return bool(cfg.get('sweep_on', True))


def sw_avail():
    """남는 현금 ETF 묶음별 팔 수 있는 수량 (오늘 나간 매도 주문 중 아직 반영 안 된 수량은 뺌)"""
    x = db.conn()
    out = []
    for l in [dict(r) for r in x.execute("SELECT * FROM lots WHERE sleeve='SW' AND status='보유' AND qty>0 ORDER BY id")]:
        pend = x.execute("SELECT COALESCE(SUM(qty - COALESCE(applied,0)),0) FROM orders WHERE lot_id=? AND side='sell' AND status IN ('보냄','접수','부분','예약')",
                         (l['id'],)).fetchone()[0]
        q = (l['qty'] or 0) - pend
        if q > 0:
            out.append((l, q))
    return out


def sweep_sell(cfg, kc, d, amount, why):
    """남는 현금 ETF를 amount원어치 시장가 매도 (오래된 묶음부터)"""
    have = sw_avail()
    if amount <= 0 or not have:
        return 0
    try:
        px = kc.price(S.SW_TICKER)['price']
    except Exception:
        px = have[0][0]['last_px'] or have[0][0]['entry_px']
    if not px:
        return 0
    need = int(-(-amount // px))
    sent = 0
    for l, q in have:
        if need <= 0:
            break
        k = min(q, need)
        if send(cfg, kc, 'sell', 'sw_sell', l['id'], 'SW', S.SW_TICKER, S.SW_NAME, k, sig_ref=px):
            sent += k
            need -= k
        if halted():
            break
    if sent:
        J.decision(db.conn(), d, d, {'sleeve': 'SW', 'ticker': S.SW_TICKER, 'name': S.SW_NAME, 'ref': px, 'qty': sent, 'amt': sent * px}, 'sell', why)
        db.conn().commit()
        log(f'💤 KODEX 200 {sent}주 매도 ({why})')
    return sent


def sweep_prep(cfg, kc, d):
    """15:10 — 밤사이 칸 매수 자금이 모자라면 KODEX 200을 지금(연속 매매) 팔아 둠"""
    al = alloc(cfg)
    if not sweep_on(cfg) or al.get('ON', 0) <= 0 or buy_paused(cfg) or db.meta_get('block_new'):
        return
    sync(kc, d)
    bal = kc.balance()
    need = min(bal['equity'] or cap(cfg), cap(cfg)) * al['ON'] / 100 * 1.01
    cash = kc.buyable()['nrcvb']
    if cash < need:
        sweep_sell(cfg, kc, d, need - cash, '밤사이 칸 매수 자금')


def sw_target(cfg, px):
    """KODEX 200 보유 비중 0~1 — 지난 종가들 + 지금 가격으로 (설정 sweep_mode: ma60 · vol · hold)"""
    mode = cfg.get('sweep_mode') or 'night'
    if mode in ('hold', 'night'):
        return 1.0, mode
    c = db.etf_bars(S.SW_TICKER, '0', today())['close']
    c = c[c.index < today()]
    if len(c) < 61:
        log(f'KODEX 200 일봉이 {len(c)}일뿐 → 지수 타이밍 없이 보유 (📥 데이터 수집 확인)', 'warn')
        return 1.0, mode
    s = __import__('pandas').concat([c, __import__('pandas').Series({today(): px})])
    return float(S.sw_weight(s, mode).iloc[-1]), mode


def sweep_buy(cfg, kc, d):
    """15:20 — 남는 현금 칸 조정: 목표 = (현금 + KODEX 200 평가 − 평가액 reserve%) × 지수 타이밍 비중 · 운용 한도 안
       목표보다 많으면 종가 동시호가에 팔고, 모자라면 삼 (지수도 오르내리므로 사고팖)"""
    if not sweep_on(cfg) or buy_paused(cfg) or db.meta_get('block_new'):
        return
    sync(kc, d)
    bal = kc.balance()
    base = min(bal['equity'] or cap(cfg), cap(cfg))
    keep = base * float(cfg.get('sweep_reserve') or 5) / 100
    cash = kc.buyable()['nrcvb']                                                   # 미체결 매수(밤사이 칸) 금액은 이미 빠진 값
    room = cap(cfg) - (bal['equity'] - cash) - keep                                 # 운용 한도를 넘지 않게
    px = kc.price(S.SW_TICKER)['price']
    if not px:
        return
    held = sum(q for _, q in sw_avail())
    w, mode = sw_target(cfg, px)
    target = max(0.0, min(cash + held * px - keep, room + held * px)) * w
    db.meta_set('sw_weight', f'{d} {w:.2f} {mode}')
    diff = target - held * px
    if diff < -px:                                                                  # 줄이기 (지수 하락 추세 · 변동성 높음)
        sweep_sell(cfg, kc, d, -diff, f'지수 타이밍 비중 {w:.0%} ({S.SW_MODES.get(mode, mode)})')
        return
    q = int(min(diff, cash - keep) // (px * 1.003)) if diff > 0 else 0
    if q <= 0:
        return
    lid = new_lot('SW', S.SW_TICKER, S.SW_NAME, 'ETF', d, {'ref': px})
    J.decision(db.conn(), d, d, {'sleeve': 'SW', 'ticker': S.SW_TICKER, 'name': S.SW_NAME, 'ref': px, 'qty': q, 'amt': q * px}, 'buy', '15:20 남는 현금 → KODEX 200')
    db.conn().commit()
    send(cfg, kc, 'buy', 'sw_buy', lid, 'SW', S.SW_TICKER, S.SW_NAME, q, sig_ref=px)


def alloc(cfg):
    a = dict(DEFAULT_ALLOC)
    a.update({k: float(v) for k, v in (cfg.get('alloc') or {}).items() if k in a})
    return a


def last_equity():
    """이 계좌의 마지막 평가액 (장 마감 기록 → 없으면 시작 평가액)"""
    try:
        r = db.conn().execute('SELECT value FROM equity ORDER BY date DESC LIMIT 1').fetchone()
        return float(r[0]) if r and r[0] else float(db.meta_get('start_value') or 0)
    except Exception:
        return 0.0


def cap(cfg):
    """운용 자금 — 기본은 '계좌 전체'(마지막 평가액 · 벌면 늘고 잃으면 줄어 복리로 굴림 · 장전 주문은 그날 아침 KIS 평가액으로)
       계좌별 상한(caps) 또는 '상한 고정'(cap_mode=fixed)일 때만 그 금액에서 멈춤 · 실전 단계 한도는 켰을 때만"""
    try:
        ceil = (cfg.get('caps') or {}).get(db.mode()) or (cfg.get('cap') if cfg.get('cap_mode') == 'fixed' else None)
        base = max(1_000_000, float(ceil)) if ceil else (last_equity() or float(cfg.get('cap') or 10_000_000))
    except Exception:
        base = 10_000_000
    return int(base * ramp(cfg) / 100)


def ramp(cfg):
    if db.mode() != 'real' or not cfg.get('real_ramp_on'):
        return 100
    steps = cfg.get('real_ramp') or [30, 60, 100]
    n = db.conn('real').execute('SELECT COUNT(*) FROM equity').fetchone()[0]
    return steps[min(len(steps) - 1, n // max(1, int(cfg.get('real_ramp_days') or 20)))]


def client(cfg, mode=None):
    m = mode or db.mode()
    a = CF.acct(cfg, m)
    return KIS(m, a.get('app_key'), a.get('app_secret'), a.get('account'), db.DATA_DIR, a.get('hts_id'))


def configured(cfg, mode=None):
    a = CF.acct(cfg, mode or db.mode())
    return bool(a.get('app_key') and a.get('app_secret') and a.get('account'))


def alert(msg, key):
    try:
        k = f'alert_{key}_{today()}'
        if db.meta_get(k) == '1' or not NOTIFY:
            return
        db.meta_set(k, '1')
        NOTIFY('🚨 ' + msg)
    except Exception as e:
        db.log(f'경고 알림 실패: {e}')


def halted():
    return db.meta_get('halt', '')


def halt(reason):
    db.meta_set('halt', f"{now():%m-%d %H:%M} {reason}"[:300])
    log(f'⛔ 자동주문 정지: {reason}', 'error')
    alert(f'자동주문 정지 — {reason}\nKIS 앱(모의투자)에서 체결 여부를 확인한 뒤 화면에서 정지 해제', 'halt')


def can_order(cfg):
    return bool(cfg.get('kis_on')) and not halted()


def buy_paused(cfg):
    return bool(cfg.get('pause_buy')) or bool(db.meta_get('auto_pause'))


def _done(job, d):
    return db.meta_get(f'done_{job}_{d}') == '1'


def _mark(job, d):
    db.meta_set(f'done_{job}_{d}', '1')


# ════════════════════════════════════════════
#  주문 · 체결
# ════════════════════════════════════════════
ACTIVE = ('보냄', '접수', '부분', '예약')
BIG_OK = ('on_buy', 'sw_buy', 'sw_sell', 'on_sell', 'manual')                     # 원래 큰 금액인 주문 (밤사이 · 남는 현금 · 수동은 따로 확인)


def guard(cfg, x, side, kind, lot_id, ticker, qty, sig_ref):
    """주문 안전장치 → 막을 이유 (없으면 '') — 분당 · 하루 주문 수 · 같은 묶음 중복 · 같은 종목 30초 안 중복 · 큰 금액 실수"""
    n_min = x.execute("SELECT COUNT(*) FROM orders WHERE ts >= ?", ((datetime.now() - timedelta(seconds=60)).isoformat(timespec='seconds'),)).fetchone()[0]
    if n_min >= int(cfg.get('guard_per_min') or 60):
        return f'분당 주문 {n_min}건 — 폭주 방지'
    n_day = x.execute("SELECT COUNT(*) FROM orders WHERE date=?", (today(),)).fetchone()[0]
    if n_day >= int(cfg.get('guard_per_day') or 400):
        halt(f'하루 주문 {n_day}건 — 폭주 방지 한도')
        return f'하루 주문 {n_day}건 — 폭주 방지 (자동주문 정지)'
    if lot_id and x.execute(f"SELECT 1 FROM orders WHERE lot_id=? AND side=? AND status IN ({','.join('?' * len(ACTIVE))})",
                            (lot_id, side, *ACTIVE)).fetchone():
        return '같은 묶음에 진행 중인 주문이 있음 — 중복 방지'
    if kind == 'manual' and x.execute("SELECT 1 FROM orders WHERE ticker=? AND side=? AND qty=? AND ts >= ? AND status NOT IN ('거절','취소','만료')",
                                      (ticker, side, int(qty), (datetime.now() - timedelta(seconds=30)).isoformat(timespec='seconds'))).fetchone():
        return '같은 종목 · 수량 주문이 30초 안에 또 — 중복 방지'
    if side == 'buy' and sig_ref and kind not in BIG_OK and qty * sig_ref > cap(cfg) * 0.2:
        return f'1건 {qty * sig_ref:,.0f}원 > 운용 자금의 20% — 큰 금액 실수 방지'
    return ''


def send(cfg, kc, side, kind, lot_id, sleeve, ticker, name, qty, ord_dvsn='01', price=0, sig_ref=None):
    x = db.conn()
    why = guard(cfg, x, side, kind, lot_id, ticker, qty, sig_ref)
    if why:
        x.execute("""INSERT INTO orders (date, ts, sleeve, lot_id, ticker, name, side, kind, qty, ord_dvsn, price, status, sig_ref, msg)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,'거절',?,?)""", (today(), db.now_s(), sleeve, lot_id, ticker, name, side, kind, int(qty), ord_dvsn,
                                                              float(price or 0), sig_ref, '안전장치: ' + why))
        J.event(x, x.execute('SELECT last_insert_rowid()').fetchone()[0], '거절', '안전장치: ' + why)
        x.commit()
        log(f'🛡 주문 막음 {name} {KIND.get(kind, kind)} {qty}주 — {why}', 'warn')
        alert(f'주문 안전장치 — {name}: {why}', 'guard')
        return None
    x.execute("""INSERT INTO orders (date, ts, sleeve, lot_id, ticker, name, side, kind, qty, ord_dvsn, price, status, sig_ref)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,'보냄',?)""", (today(), db.now_s(), sleeve, lot_id, ticker, name, side, kind, int(qty), ord_dvsn,
                                                           float(price or 0), sig_ref))
    oid = x.execute('SELECT last_insert_rowid()').fetchone()[0]
    J.event(x, oid, '보냄', f"{KIND.get(kind, kind)} {qty}주 {'시장가' if ord_dvsn == '01' else f'지정가 {price}'}")
    x.commit()
    try:
        r = kc.order(side, ticker, qty, ord_dvsn, price)
        x.execute("UPDATE orders SET status='접수', order_no=?, org_no=?, msg=?, msg_cd=?, ack_ts=?, ord_time=? WHERE id=?",
                  (r['order_no'], r['org_no'], r['msg'][:200], r.get('msg_cd', ''), db.now_s(), r.get('time', ''), oid))
        J.event(x, oid, '접수', f"주문번호 {r['order_no']} {r['msg'][:100]}")
        x.commit()
        log(f"{'🟢' if side == 'buy' else '🔵'} [{sleeve}] {KIND.get(kind, kind)} {name} {qty}주 {'시장가' if ord_dvsn == '01' else f'지정가 {price:,.0f}'}")
        return oid
    except KISError as e:
        msg = str(e)
        amb = 'AMBIGUOUS' in msg
        x.execute('UPDATE orders SET status=?, msg=?, msg_cd=? WHERE id=?', ('불분명' if amb else '거절', msg[:200], getattr(e, 'code', ''), oid))
        J.event(x, oid, '불분명' if amb else '거절', msg)
        x.commit()
        if amb:
            halt(f'{name} {KIND.get(kind, kind)} 주문 결과 불분명 — KIS 앱에서 체결 여부를 확인한 뒤 정지 해제')
        else:
            log(f'주문 거절 {name} {KIND.get(kind, kind)}: {msg}', 'warn')
        return None
    except Exception as e:                           # kc.order는 전송 뒤 오류를 모두 KISError로 바꿈 → 여기는 전송 전 오류
        x.execute("UPDATE orders SET status='거절', msg=? WHERE id=?", (f'전송 전 오류: {str(e)[:180]}', oid))
        J.event(x, oid, '거절', f'전송 전 오류: {str(e)[:180]}')
        x.commit()
        log(f'주문 전송 전 오류 {name}: {str(e)[:150]}', 'warn')
        return None


def sync(kc, d=None):
    """그날 체결 내역 → 주문 · 묶음(lot) 반영 (늘어난 만큼만 · 두 번 반영 안 함)"""
    d = d or today()
    x = db.conn()
    if now().strftime('%H:%M') >= '08:31' and x.execute("SELECT 1 FROM orders WHERE date=? AND status='예약'", (d,)).fetchone():
        resolve_resv(kc, d)
    fills = {f['order_no']: f for f in kc.fills(d) if f['order_no']}
    for o in [dict(r) for r in x.execute("SELECT * FROM orders WHERE date=? AND order_no IS NOT NULL AND order_no!='' AND status NOT IN ('체결','취소','거절','만료')", (d,))]:
        f = fills.get(o['order_no'])
        if not f:
            continue
        filled, avg = f['filled'], f['avg']
        st = '체결' if filled >= o['qty'] else ('취소' if f['cancelled'] or (f['remain'] == 0 and filled < o['qty']) else ('부분' if filled else o['status']))
        delta = filled - (o['applied'] or 0)
        x.execute('UPDATE orders SET filled=?, avg=?, status=?, applied=? WHERE id=?', (filled, avg, st, filled, o['id']))
        if st != o['status']:
            J.event(x, o['id'], st, f"누적 {filled}/{o['qty']}주 평균 {avg:,.0f}")
            if st == '체결':
                x.execute('UPDATE orders SET fill_ts=? WHERE id=?', (db.now_s(), o['id']))
        if delta > 0:
            _apply(x, o, filled, avg, delta, d)
        if st == '취소' and o['side'] == 'buy':
            _finish_buy(x, o, filled)
    x.commit()
    STATE['last_sync'] = now().strftime('%H:%M:%S')


def _apply(x, o, filled, avg, delta, d):
    lot = x.execute('SELECT * FROM lots WHERE id=?', (o['lot_id'],)).fetchone()
    if not lot:
        log(f"묶음 없는 체결 {o['name']} {filled}주 — 확인 필요", 'warn')
        return
    lot = dict(lot)
    got = filled * avg - (o['applied'] or 0) * (o['avg'] or 0)            # o는 반영 전 값 (누적 체결 · 평균가) → 이번 조각 금액
    fee, tax = J.fill(x, o, delta, got / delta if delta else avg)
    if o['side'] == 'buy':
        x.execute("""UPDATE lots SET qty=?, qty0=?, entry_px=?, cost=?, entry_date=?, status='보유', last_px=COALESCE(last_px, ?), updated=?,
                     fee=COALESCE(fee,0)+?, entry_ts=COALESCE(entry_ts, ?) WHERE id=?""",
                  (filled, filled, avg, filled * avg, d, avg, db.now_s(), fee, db.now_s(), lot['id']))
        log(f"✅ [{o['sleeve']}] 매수 체결 {o['name']} {filled}주 @ {avg:,.0f}")
        return
    left = max(0, (lot['qty'] or 0) - delta)
    proceeds = (lot['proceeds'] or 0) + got
    fee_all, tax_all = (lot['fee'] or 0) + fee, (lot['tax'] or 0) + tax
    if left > 0:
        x.execute('UPDATE lots SET qty=?, proceeds=?, fee=?, tax=?, updated=? WHERE id=?', (left, proceeds, fee_all, tax_all, db.now_s(), lot['id']))
        return
    pnl = proceeds - lot['cost'] - fee_all - tax_all
    x.execute("""UPDATE lots SET qty=0, proceeds=?, status='청산', exit_date=?, exit_px=?, pnl=?, ret=?, fee=?, tax=?, exit_ts=?, exit_kind=?,
                 updated=? WHERE id=?""",
              (proceeds, d, proceeds / (lot['qty0'] or filled or 1), pnl, pnl / lot['cost'] * 100 if lot['cost'] else 0, fee_all, tax_all,
               db.now_s(), o['kind'], db.now_s(), lot['id']))
    log(f"{'💰' if pnl > 0 else '🔻'} [{lot['sleeve']}] 청산 {lot['name']} {pnl / lot['cost'] * 100 if lot['cost'] else 0:+.2f}% · {pnl:+,.0f}원 ({KIND.get(o['kind'], o['kind'])})")


def _finish_buy(x, o, filled):
    """취소 · 만료 · 거절된 매수 → 묶음 정리 (일부만 체결됐으면 그만큼만 보유)"""
    lot = x.execute('SELECT * FROM lots WHERE id=?', (o['lot_id'],)).fetchone()
    if lot and lot['status'] == '주문' and not filled:
        x.execute("UPDATE lots SET status='미체결', updated=? WHERE id=?", (db.now_s(), lot['id']))


def new_lot(sleeve, ticker, name, sector, signal_date, sig=None):
    """sig: 신호 근거 {rank, score, ref, info} → 분석용으로 거래에 같이 저장"""
    x = db.conn()
    sig = sig or {}
    x.execute("""INSERT INTO lots (sleeve, ticker, name, sector, signal_date, status, updated, sig_rank, sig_score, sig_ref, entry_info)
                 VALUES (?,?,?,?,?,'주문',?,?,?,?,?)""",
              (sleeve, ticker, name, sector, signal_date, db.now_s(), sig.get('rank'), sig.get('score'), sig.get('ref'), sig.get('info')))
    return x.execute('SELECT last_insert_rowid()').fetchone()[0]


def open_lots(sleeve=None):
    q = "SELECT * FROM lots WHERE status IN ('주문','보유')" + (' AND sleeve=?' if sleeve else '')
    return [dict(r) for r in db.conn().execute(q, (sleeve,) if sleeve else ())]


# ════════════════════════════════════════════
#  계획 (장전 주문 · 화면 미리보기 공용)
# ════════════════════════════════════════════
def plan(cfg, equity, cash, sig_date):
    """→ {'sells': [...], 'buys': [...], 'defer': [...]} · buys/defer 항목: sleeve, ticker, name, qty, ref, amt, skip"""
    x = db.conn()
    base = min(equity or cap(cfg), cap(cfg))
    al = alloc(cfg)
    SLOTS = slots(cfg)
    sells = [dict(r) for r in x.execute("SELECT * FROM lots WHERE status='보유' AND sell_flag=1 AND qty>0")]
    lots = open_lots()
    buys, defer = [], []
    one_cap, day_cap, spent = base * 0.15, base * 0.60, 0.0
    cash = cash if cash is not None else base
    selling_ids = {l['id'] for l in sells}
    for s in ('LVH', 'REV', 'DV'):
        if al.get(s, 0) <= 0:
            continue
        size = min(base * al[s] / 100 / SLOTS[s], one_cap)
        mine = [l for l in lots if l['sleeve'] == s]
        live = [l for l in mine if l['id'] not in selling_ids]
        held = {l['ticker'] for l in live}
        free = SLOTS[s] - len(live)
        for r in x.execute('SELECT * FROM signals WHERE date=? AND sleeve=? AND rank<100 ORDER BY rank', (sig_date, s)):
            r = dict(r)
            if s == 'DV' and free <= 0:
                break
            o = {'sleeve': s, 'ticker': r['ticker'], 'name': r['name'], 'ref': r['ref'] or 0, 'rank': r['rank'], 'score': r['score'],
                 'info': r['info'], 'qty': 0, 'amt': 0, 'skip': ''}
            q = S.shares(size, o['ref'] * 1.02, 10 ** 15)                    # 시가 갭 여유 2%
            if r['ticker'] in held:
                o['skip'] = '이 칸이 이미 보유'
            elif free <= 0:
                o['skip'] = f'{s} {SLOTS[s]}자리 다 참'
            elif spent + size > day_cap:
                o['skip'] = '하루 매수 한도(평가액 60%)'
            elif q <= 0:
                o['skip'] = f"1주 {o['ref']:,.0f}원 > 종목당 {size:,.0f}원 × 1.5"
            if o['skip']:
                if s != 'DV':                                                 # DV는 빈자리만큼 다음 순위로 넘어가므로 건너뜀 표시 안 함
                    buys.append(o)
                continue
            o.update(qty=q, amt=q * o['ref'])
            if o['amt'] * 1.03 <= cash:
                cash -= o['amt'] * 1.03
                buys.append(o)
            else:
                o['skip'] = '현금 부족 → 09:02 (아침에 판 돈으로)'
                defer.append(o)
            spent += o['amt']
            held.add(r['ticker'])
            free -= 1
    return {'sells': sells, 'buys': buys, 'defer': defer, 'base': base}


# ════════════════════════════════════════════
#  하루 작업
# ════════════════════════════════════════════
def _balance_check(kc):
    bal = kc.balance()
    have = {}
    for l in open_lots():
        if l['status'] == '보유':
            have[l['ticker']] = have.get(l['ticker'], 0) + (l['qty'] or 0)
    unknown = [p for p in bal['positions'] if p['ticker'] not in have]
    return bal, unknown, have


def precheck(cfg, kc, d):
    probs = []
    try:
        bal, unknown, _ = _balance_check(kc)
        if unknown:
            probs.append('앱이 모르는 보유 종목: ' + ' · '.join(f"{p['name']} {p['qty']}주" for p in unknown[:5]) + ' → 새 매수 차단 예정')
        info = f"예수금 {bal['cash']:,.0f} · 평가 {bal['equity']:,.0f} · 보유 {len(bal['positions'])}"
    except Exception as e:
        probs.append(f'KIS 연결 실패: {str(e)[:150]}')
        info = ''
    sd = db.meta_get('last_signal_date')
    if sd != prev_trading_day(d):
        probs.append(f'신호가 전 거래일 것이 아님 (마지막 {sd or "없음"}) → 오늘 새 매수 없음 · 📥 데이터 수집 확인')
    if halted():
        probs.append(f'자동주문 정지 상태: {halted()}')
    if probs:
        log('장전 점검 — ' + ' / '.join(probs), 'warn')
        alert('장전 점검(08:20) 문제 — ' + ' / '.join(probs), 'precheck')
    else:
        log(f'장전 점검 OK — {info}')
        if not db.meta_get('start_value') and info:
            db.meta_set('start_value', bal['equity'])
            db.meta_set('start_date', d)


def preopen(cfg, kc, d):
    bal, unknown, _ = _balance_check(kc)
    if unknown:
        db.meta_set('block_new', ' · '.join(f"{p['name']} {p['qty']}주" for p in unknown)[:200])
        alert('새 매수 차단 — 앱이 모르는 보유 종목: ' + ' · '.join(p['name'] for p in unknown[:5]) + '\n모의계좌를 다른 프로그램과 같이 쓰는지 확인하세요', 'block')
    else:
        db.meta_set('block_new', '')
    sd = db.meta_get('last_signal_date')
    pl = plan(cfg, bal['equity'], min(bal['cash_d2'] or bal['cash'], bal['cash'] or bal['cash_d2']), sd)
    x = db.conn()
    resolve_resv(kc, d)
    rsv = reserved_lots(d)
    for l in pl['sells']:
        if l['id'] in rsv:                                                          # 어젯밤 예약주문으로 이미 나감
            continue
        J.decision(x, d, sd, {**l, 'ref': l['last_px'], 'amt': (l['last_px'] or 0) * l['qty']}, 'sell', KIND.get(l['sell_reason'], l['sell_reason'] or ''))
        x.commit()
        send(cfg, kc, 'sell', l['sell_reason'] or 'manual', l['id'], l['sleeve'], l['ticker'], l['name'], l['qty'], sig_ref=l['last_px'])
        if halted():
            return
    if sweep_on(cfg) and (cfg.get('sweep_mode') or 'night') == 'night':              # 밤사이 지수: 아침 시가에 전부 팖
        have = sum(q for _, q in sw_avail())
        if have:
            sweep_sell(cfg, kc, d, have * (kc.price(S.SW_TICKER)['price'] or 1) * 1.1, '밤사이 지수 → 시가 매도')
    why = ('모르는 보유 종목' if unknown else '매수 일시 중지' if buy_paused(cfg) else f'신호가 전 거래일 것이 아님 (마지막 {sd})' if sd != prev_trading_day(d)
           else f'{sd} 신호는 이미 주문함' if db.meta_get(f'plan_used_{sd}') == '1' else '')
    if why:
        log(f'새 매수 안 함 — {why}', 'warn')
        if db.meta_get(f'plan_used_{sd}') != '1':
            for o in pl['buys'] + pl['defer']:
                J.decision(x, d, sd, o, 'skip', o['skip'] or why)
            x.commit()
        return
    db.meta_set(f'plan_used_{sd}', '1')
    for o in pl['buys'] + pl['defer']:                                              # 예상체결가로 갭 확인
        if not o['skip']:
            g = gap_check(cfg, kc, o, 'pre')
            if g:
                o['skip'], o['gapped'] = g, True
    pl['buys'] += [o for o in pl['defer'] if o.get('gapped')]
    pl['defer'] = [o for o in pl['defer'] if not o.get('gapped')]
    for o in pl['buys'] + pl['defer']:
        J.decision(x, d, sd, o, 'skip' if o['skip'] and o not in pl['defer'] else ('defer' if o in pl['defer'] else 'buy'), o['skip'])
    x.commit()
    for o in pl['buys']:
        if o['skip']:
            continue
        lid = new_lot(o['sleeve'], o['ticker'], o['name'], '', sd, o)
        db.conn().commit()
        send(cfg, kc, 'buy', 'entry', lid, o['sleeve'], o['ticker'], o['name'], o['qty'], sig_ref=o['ref'])
        if halted():
            return
    db.meta_set(f'defer_{d}', json.dumps([{k: o.get(k) for k in ('sleeve', 'ticker', 'name', 'qty', 'ref', 'rank', 'score', 'info')} for o in pl['defer']],
                                         ensure_ascii=False))
    if pl['defer']:
        log(f"현금 부족으로 {len(pl['defer'])}건은 09:02에 (아침 매도 체결 뒤)")
        if sweep_on(cfg) and (cfg.get('sweep_mode') or 'night') != 'night':        # 모자란 만큼 KODEX 200을 시가에 팔아 09:02 매수 자금으로
            sells = sum((l['last_px'] or 0) * l['qty'] for l in pl['sells'])
            sweep_sell(cfg, kc, d, sum(o['amt'] for o in pl['defer']) * 1.03 - sells * 0.99, '09:02 미룬 매수 자금')


def resv_supported(kc):
    return getattr(kc, 'env', '') == 'real' or bool(getattr(kc, 'resv_ok', False))


def reserve_sells(cfg, kc, nd):
    """저녁(신호 계산 뒤) — 다음 거래일 팔 것(보유 끝 · 9EMA · 교체 · 밤사이 ETF · 남는 현금 KODEX 200)을 KIS 예약주문으로 미리 넣어 둠
       → 아침에 PC가 꺼져 있어도 매도는 시가 동시호가로 나감. 실전 계좌 전용 · 매수는 갭 확인이 필요해서 08:50에 그대로"""
    if not cfg.get('resv_on', True) or not resv_supported(kc) or not can_order(cfg):
        return 0
    if db.meta_get(f'resv_{nd}') == '1':
        return 0
    x = db.conn()
    items = [(l, l['qty']) for l in [dict(r) for r in x.execute("SELECT * FROM lots WHERE status='보유' AND sell_flag=1 AND qty>0")]]
    if sweep_on(cfg) and (cfg.get('sweep_mode') or 'night') == 'night':
        items += [(l, q) for l, q in sw_avail()]
    n = 0
    for l, q in items:
        if x.execute(f"SELECT 1 FROM orders WHERE lot_id=? AND side='sell' AND status IN ({','.join('?' * len(ACTIVE))})", (l['id'], *ACTIVE)).fetchone():
            continue
        kind = 'sw_sell' if l['sleeve'] == 'SW' else (l['sell_reason'] or 'manual')
        x.execute("""INSERT INTO orders (date, ts, sleeve, lot_id, ticker, name, side, kind, qty, ord_dvsn, price, status, sig_ref)
                     VALUES (?,?,?,?,?,?,'sell',?,?,'01',0,'예약 신청',?)""", (nd, db.now_s(), l['sleeve'], l['id'], l['ticker'], l['name'], kind, int(q), l['last_px']))
        oid = x.execute('SELECT last_insert_rowid()').fetchone()[0]
        x.commit()
        try:
            r = kc.order_resv('sell', l['ticker'], q)
            x.execute("UPDATE orders SET status='예약', resv_seq=?, msg=?, ack_ts=? WHERE id=?", (r['seq'], ('예약 ' + r['msg'])[:200], db.now_s(), oid))
            J.event(x, oid, '예약', f"{nd} 시가 매도 예약 #{r['seq']}")
            n += 1
        except Exception as e:
            x.execute("UPDATE orders SET status='예약 실패', msg=? WHERE id=?", (CF.clean(e)[:200], oid))
            J.event(x, oid, '예약 실패', CF.clean(e)[:200])
            log(f"예약주문 실패 {l['name']} (아침 08:50에 그대로 매도): {CF.clean(e)[:120]}", 'warn')
        x.commit()
    db.meta_set(f'resv_{nd}', '1')
    if n:
        log(f'📅 {nd} 시가 매도 예약 {n}건 (PC가 꺼져 있어도 나감)')
    return n


def resolve_resv(kc, d):
    """오늘 예약주문이 KIS에서 실제 주문으로 바뀌었는지 → 주문번호 붙여서 체결 반영 · 거부면 '거절'"""
    x = db.conn()
    pend = [dict(r) for r in x.execute("SELECT * FROM orders WHERE date=? AND status='예약' AND resv_seq IS NOT NULL", (d,))]
    if not pend or not hasattr(kc, 'resv_list'):
        return 0
    try:
        rows = {r['seq']: r for r in kc.resv_list(prev_trading_day(d) or d, d)}
    except Exception as e:
        log(f'예약주문 조회 실패: {CF.clean(e)[:120]}', 'warn')
        return 0
    n = 0
    for o in pend:
        r = rows.get(o['resv_seq'])
        if not r:
            continue
        if r['odno']:
            x.execute("UPDATE orders SET order_no=?, status='접수', msg=? WHERE id=?", (r['odno'], f"예약 → 주문 {r['odno']}", o['id']))
            J.event(x, o['id'], '접수', f"예약 #{o['resv_seq']} → 주문번호 {r['odno']}")
            n += 1
        elif r['reject'] or '거부' in r['result'] or r['cancel_dt']:
            x.execute("UPDATE orders SET status='거절', msg=? WHERE id=?", (f"예약 거부/취소: {r['reject'] or r['result']}"[:200], o['id']))
            J.event(x, o['id'], '거절', f"예약 거부/취소: {r['reject'] or r['result']}")
            log(f"예약주문 거부 {o['name']}: {r['reject'] or r['result']} → 지금 시장가로 다시", 'warn')
    x.commit()
    return n


def reserved_lots(d):
    """오늘 이미 매도 주문(예약 · 접수 · 체결)이 있는 묶음"""
    return {r[0] for r in db.conn().execute("SELECT lot_id FROM orders WHERE date=? AND side='sell' AND status IN ('예약','접수','부분','체결')", (d,))}


def resend_failed_resv(cfg, kc, d):
    """09:02 — 예약이 거부됐거나 아직도 처리 안 된 매도는 지금 시장가로"""
    x = db.conn()
    resolve_resv(kc, d)
    for o in [dict(r) for r in x.execute("SELECT * FROM orders WHERE date=? AND resv_seq IS NOT NULL AND status IN ('거절','예약')", (d,))]:
        if x.execute("SELECT 1 FROM orders WHERE lot_id=? AND date=? AND side='sell' AND resv_seq IS NULL AND status NOT IN ('거절','취소','만료')", (o['lot_id'], d)).fetchone():
            continue
        lot = x.execute("SELECT * FROM lots WHERE id=? AND status='보유' AND qty>0", (o['lot_id'],)).fetchone()
        if o['status'] == '예약':
            x.execute("UPDATE orders SET status='만료', msg='예약 미처리 → 시장가로 다시' WHERE id=?", (o['id'],))
            x.commit()
        if lot:
            send(cfg, kc, 'sell', o['kind'], lot['id'], lot['sleeve'], lot['ticker'], lot['name'], min(o['qty'], lot['qty']), sig_ref=lot['last_px'])


def late_open(cfg, kc, d):
    """PC가 장 시작(08:58) 뒤에 켜져 장전 주문을 놓쳤을 때 — 매도할 것(밤사이 ETF · KODEX 200 · 보유 끝)만 지금 시장가로 팖.
       새 매수는 하지 않음 (시가 매수를 전제로 한 신호라 장중 매수는 백테스트와 달라짐)"""
    log('⏰ 장전 주문 시간을 놓침 (PC가 늦게 켜짐) → 매도할 것만 지금 시장가 · 오늘 새 매수 없음', 'warn')
    alert('PC가 늦게 켜져 장전 주문을 놓쳤습니다 — 매도할 것만 지금 팔고 오늘 새 매수는 쉽니다', 'late')
    x = db.conn()
    sd = db.meta_get('last_signal_date')
    resolve_resv(kc, d)
    rsv = reserved_lots(d)
    for l in [dict(r) for r in x.execute("SELECT * FROM lots WHERE status='보유' AND sell_flag=1 AND qty>0")]:
        if l['id'] in rsv:
            continue
        J.decision(x, d, sd, {**l, 'ref': l['last_px'], 'amt': (l['last_px'] or 0) * l['qty']}, 'sell', '늦게 켜짐 · ' + KIND.get(l['sell_reason'], l['sell_reason'] or ''))
        x.commit()
        send(cfg, kc, 'sell', l['sell_reason'] or 'manual', l['id'], l['sleeve'], l['ticker'], l['name'], l['qty'], sig_ref=l['last_px'])
        if halted():
            return
    if sweep_on(cfg) and (cfg.get('sweep_mode') or 'night') == 'night':
        have = sum(q for _, q in sw_avail())
        if have:
            sweep_sell(cfg, kc, d, have * (kc.price(S.SW_TICKER)['price'] or 1) * 1.1, '늦게 켜짐 · 밤사이 지수 매도')
    if sd and db.meta_get(f'plan_used_{sd}') != '1':
        db.meta_set(f'plan_used_{sd}', '1')
        pl = plan(cfg, None, None, sd)
        for o in pl['buys'] + pl['defer']:
            J.decision(x, d, sd, o, 'skip', 'PC가 늦게 켜져 장전 매수를 놓침')
        x.commit()


def deferred(cfg, kc, d):
    """09:02 — 미뤄 둔 매수 (아침에 판 돈 · 밤사이 ETF 판 돈으로) + 장전 시간 때문에 거절된 매도 재시도"""
    sync(kc, d)
    x = db.conn()
    resend_failed_resv(cfg, kc, d)
    for o in [dict(r) for r in x.execute("SELECT * FROM orders WHERE date=? AND status='거절' AND side='sell' AND ord_dvsn='01' AND resv_seq IS NULL", (d,))]:
        if any(k in (o['msg'] or '') for k in ('시간', '장개시', '장시작', '장운영', '동시호가')):
            x.execute("UPDATE orders SET status='거절(재시도)' WHERE id=?", (o['id'],))
            x.commit()
            send(cfg, kc, 'sell', o['kind'], o['lot_id'], o['sleeve'], o['ticker'], o['name'], o['qty'])
    if buy_paused(cfg) or db.meta_get('block_new'):
        return
    todo = json.loads(db.meta_get(f'defer_{d}') or '[]')
    retry_buy = [dict(r) for r in x.execute("SELECT * FROM orders WHERE date=? AND status='거절' AND side='buy' AND kind='entry'", (d,))]
    if not todo and not retry_buy:
        return
    try:
        cash = kc.buyable()['nrcvb']                                         # 미수 없는 매수가능금액 (아침 매도 대금 포함)
    except Exception:
        bal = kc.balance()
        cash = min(bal['cash_d2'] or bal['cash'], bal['cash'] or bal['cash_d2'])
    for o in retry_buy:
        x.execute("UPDATE orders SET status='거절(재시도)' WHERE id=?", (o['id'],))
        x.execute("UPDATE lots SET status='미체결' WHERE id=? AND status='주문'", (o['lot_id'],))
        x.commit()
        lot = x.execute('SELECT sig_rank, sig_score, sig_ref, entry_info FROM lots WHERE id=?', (o['lot_id'],)).fetchone()
        todo.append({'sleeve': o['sleeve'], 'ticker': o['ticker'], 'name': o['name'], 'qty': o['qty'], 'ref': (lot['sig_ref'] if lot else None) or o['price'] or 0,
                     'rank': lot['sig_rank'] if lot else None, 'score': lot['sig_score'] if lot else None, 'info': lot['entry_info'] if lot else None})
    for o in todo:
        g = gap_check(cfg, kc, o, 'open')
        if g:
            J.decision(x, d, db.meta_get('last_signal_date'), o, 'skip', g)
            x.commit()
            continue
        try:
            px = kc.price(o['ticker'])['price'] or o['ref']
        except Exception:
            px = o['ref']
        if not px or px * o['qty'] * 1.01 > cash:
            q = int(cash / 1.01 // px) if px else 0
            if q <= 0:
                log(f"[{o['sleeve']}] {o['name']} 09:02 매수도 현금 부족 → 건너뜀", 'warn')
                J.decision(x, d, db.meta_get('last_signal_date'), o, 'skip', '09:02 현금 부족')
                x.commit()
                continue
            o['qty'] = min(o['qty'], q)
        J.decision(x, d, db.meta_get('last_signal_date'), {**o, 'amt': px * o['qty']}, 'buy', '09:02 (미룬 매수 · 재시도)')
        lid = new_lot(o['sleeve'], o['ticker'], o['name'], '', db.meta_get('last_signal_date'), o)
        x.commit()
        if send(cfg, kc, 'buy', 'entry', lid, o['sleeve'], o['ticker'], o['name'], o['qty'], sig_ref=o.get('ref')):
            cash -= px * o['qty'] * 1.01
        if halted():
            return
    db.meta_set(f'defer_{d}', '[]')


def on_buy(cfg, kc, d):
    """15:20 — 밤사이 칸: KODEX 코스닥150 장마감 동시호가 시장가 (종가 체결)"""
    al = alloc(cfg)
    if al.get('ON', 0) <= 0 or buy_paused(cfg) or db.meta_get('block_new'):
        return
    sync(kc, d)
    if open_lots('ON'):
        log('밤사이 ETF: 아침에 판 것이 아직 정리 안 됨 → 오늘 쉼', 'warn')
        return
    bal = kc.balance()
    px = kc.price(S.ON_TICKER)['price']
    base = min(bal['equity'] or cap(cfg), cap(cfg))
    cash = min(bal['cash_d2'] or bal['cash'], bal['cash'] or bal['cash_d2'])
    q = int(min(base * al['ON'] / 100, cash * 0.99) // (px * 1.005)) if px else 0
    if q <= 0:
        log('밤사이 ETF 매수 건너뜀 — 현금 부족', 'warn')
        return
    lid = new_lot('ON', S.ON_TICKER, S.ON_NAME, 'ETF', d, {'ref': px})
    db.conn().execute('UPDATE lots SET sell_flag=1, sell_reason=? WHERE id=?', ('on_sell', lid))
    J.decision(db.conn(), d, d, {'sleeve': 'ON', 'ticker': S.ON_TICKER, 'name': S.ON_NAME, 'ref': px, 'qty': q, 'amt': q * px}, 'buy', '15:20 종가 매수')
    db.conn().commit()
    if not send(cfg, kc, 'buy', 'on_buy', lid, 'ON', S.ON_TICKER, S.ON_NAME, q, sig_ref=px) and not halted():
        alert(f'밤사이 ETF 매수 거절 ({S.ON_NAME} {q}주) — 로그 확인', 'onfail')


def eod(cfg, kc, d):
    """15:45 — 체결 마감 · 못 산 매수 정리 · 잔고 대조 · 평가 기록 · 계좌 안전장치"""
    x = db.conn()
    sync(kc, d)
    for o in [dict(r) for r in x.execute("SELECT * FROM orders WHERE date=? AND status IN ('보냄','접수','부분','예약')", (d,))]:
        x.execute("UPDATE orders SET status='만료' WHERE id=?", (o['id'],))
        J.event(x, o['id'], '만료', f"장 마감 · 체결 {o['filled'] or 0}/{o['qty']}주")
        if o['side'] == 'buy':
            _finish_buy(x, o, o['filled'] or 0)
    x.execute("UPDATE lots SET status='미체결' WHERE status='주문' AND id NOT IN (SELECT lot_id FROM orders WHERE date=? AND side='buy')", (d,))
    x.commit()
    bal, unknown, have = _balance_check(kc)
    kpos = {p['ticker']: p for p in bal['positions']}
    bad = [f"{p['name']} KIS {p['qty']}주 · 앱 없음" for p in unknown]
    for t, q in have.items():
        k = kpos.get(t)
        if not k or k['qty'] != q:
            bad.append(f"{t} 앱 {q}주 · KIS {k['qty'] if k else 0}주")
    for l in open_lots():
        k = kpos.get(l['ticker'])
        if k and k.get('price'):
            x.execute('UPDATE lots SET last_px=? WHERE id=?', (k['price'], l['id']))
    cash = min(bal['cash_d2'] or bal['cash'], bal['cash'] or bal['cash_d2'])
    prev = x.execute('SELECT value, peak FROM equity WHERE date<? ORDER BY date DESC LIMIT 1', (d,)).fetchone()
    peak = max(bal['equity'], prev['peak'] if prev else bal['equity'])
    x.execute('INSERT OR REPLACE INTO equity VALUES (?,?,?,?,?)', (d, cash, bal['equity'], len(bal['positions']), peak))
    if not db.meta_get('start_value'):                                              # 장전 점검을 못 했어도 첫 기록을 시작값으로
        db.meta_set('start_value', prev['value'] if prev else bal['equity'])
        db.meta_set('start_date', d)
    for s in S.SLEEVES:
        ls = [l for l in open_lots(s) if l['status'] == '보유']
        inv = sum(l['cost'] * (l['qty'] / l['qty0'] if l['qty0'] else 1) for l in ls)
        val = sum(l['qty'] * (l['last_px'] or l['entry_px'] or 0) for l in ls)
        rz = x.execute("SELECT COALESCE(SUM(pnl),0) FROM lots WHERE sleeve=? AND status='청산'", (s,)).fetchone()[0]
        x.execute('INSERT OR REPLACE INTO sleeve_daily VALUES (?,?,?,?,?,?)', (d, s, inv, val, rz, len(ls)))
    broker = None
    try:
        broker = kc.trade_profit(d, d) if hasattr(kc, 'trade_profit') else None       # 실전만 (모의는 None → 추정 수수료 · 세금)
    except Exception as e:
        log(f'기간별 매매손익 조회 실패 (추정값 사용): {CF.clean(e)[:120]}', 'warn')
    J.day_close(x, d, bal, broker)
    x.commit()
    J.flush_api()
    if bad:
        log('장 마감 잔고 불일치 — ' + ' / '.join(bad[:6]), 'warn')
        alert('장 마감 잔고 불일치 — ' + ' / '.join(bad[:5]), 'eodmismatch')
    dd = bal['equity'] / peak - 1 if peak else 0
    day = bal['equity'] / prev['value'] - 1 if prev and prev['value'] else 0
    lim_dd, lim_day = float(cfg.get('dd_limit') or 15), float(cfg.get('day_loss_limit') or 4)
    if dd <= -lim_dd / 100 or day <= -lim_day / 100:
        why = f'고점 대비 {dd * 100:.1f}%' if dd <= -lim_dd / 100 else f'하루 {day * 100:.1f}%'
        db.meta_set('auto_pause', f'{d} {why}')
        log(f'계좌 안전장치: {why} → 새 매수 자동 중지 (매도는 계속 · 화면에서 해제)', 'error')
        alert(f'계좌 안전장치 — {why} → 새 매수 자동 중지. 확인 후 화면에서 해제하세요', 'breaker')


# ════════════════════════════════════════════
#  장 마감 뒤 신호 계산 (자료 수집 뒤)
# ════════════════════════════════════════════
def signal_job(cfg, d, progress=None):
    """d 종가 기준: 보유 일수 · 매도 표시 · 새 신호(LVH · REV 상위 3, DV 월 교체) → signals 표 · 다음 거래일 08:35에 주문"""
    t0 = time.time()
    days = db.trading_days('20180101', d)
    if not days or days[-1] != d:
        raise RuntimeError(f'{d} 일봉이 아직 없음 (📥 데이터 수집 확인)')
    if db.gmeta_get('dq_bad') == d:
        raise RuntimeError(f'{d} 데이터 품질 미달 → 신호 계산 보류 (📥 데이터 탭에서 다시 수집)')
    frm = days[max(0, len(days) - 400)]
    st = db.stocks()
    excl = {t for t, v in st.items() if v['excluded'] or v['halt'] or v['admin'] or v['warn'] or v['market'] == 'KONEX'}
    P = db.panel(frm, d)
    FL = db.flows(days[max(0, len(days) - 60)], d)
    F = S.features(P, FL, excl)
    x = db.conn()
    C = F['close']
    # ① 보유 일수 (체결일부터 d까지 거래된 날 수) · 매도 표시
    for l in [dict(r) for r in x.execute("SELECT * FROM lots WHERE status='보유' AND sleeve NOT IN ('ON','SW')")]:
        t = l['ticker']
        if t not in C.columns:
            continue
        col = C[t]
        nd = int(col[(col.index >= l['entry_date']) & (col.index <= d)].notna().sum())
        last = col.loc[:d].dropna()
        lp = float(last.iloc[-1]) if len(last) else l['last_px']
        flag, why = l['sell_flag'], l['sell_reason']
        if not flag:
            if l['sleeve'] == 'LVH' and nd >= S.LVH['hold']:
                flag, why = 1, 'hold20'
            elif l['sleeve'] == 'REV' and nd >= 1 and (S.rev_exit(F, d, t) or nd >= S.REV['hold']):
                flag, why = 1, ('ema9' if S.rev_exit(F, d, t) else 'hold10')
            elif len(last) and last.index[-1] < d and (datetime.strptime(d, '%Y%m%d') - datetime.strptime(last.index[-1], '%Y%m%d')).days > 30:
                flag, why = 1, 'delist'
        x.execute('UPDATE lots SET days=?, last_px=?, sell_flag=?, sell_reason=? WHERE id=?', (nd, lp, flag, why, l['id']))
    # ② DV 월 교체
    rows = []
    al = alloc(cfg)
    if al.get('DV', 0) > 0:
        m = d[:6]
        mem, mon = db.month_tables()
        if m not in mem:
            log(f'{m} 월 자료가 아직 없음 → 지난달 순위 사용 (KRX 계정 · 수집 확인)', 'warn')
        use = m if m in mem else max([k for k in mem if k <= m], default=None)
        rank = S.dv_rank(mem, mon, use) if use else []
        if rank:
            if db.meta_get('dv_month') != use:
                held = [l['ticker'] for l in open_lots('DV') if not l['sell_flag']]
                keep, sell, _ = S.dv_targets(rank, held)
                for l in open_lots('DV'):
                    if l['ticker'] in sell and l['status'] == '보유':
                        x.execute("UPDATE lots SET sell_flag=1, sell_reason='dv_rebal' WHERE id=?", (l['id'],))
                db.meta_set('dv_month', use)
                log(f"{use} 배당·가치 순위 갱신 · 교체 매도 {len(sell)} · 상위 {', '.join(r[1] for r in rank[:5])}")
            live = {l['ticker'] for l in open_lots('DV') if not l['sell_flag']}
            k = 0
            for t, nm, sec, sc, dv, pb in rank:
                if t in live or t not in C.columns or pd.isna(C.at[d, t]):
                    continue
                k += 1
                rows.append((d, 'DV', k, t, nm, sc, float(C.at[d, t]), json.dumps({'div': dv, 'pbr': pb, 'sector': sec}, ensure_ascii=False)))
                if k >= S.DV['n'] + 5:
                    break
    cands = [('DV', t, k, sc, float(C.at[d, t]) if t in C.columns and C.at[d, t] == C.at[d, t] else None, {'div': dv, 'pbr': pb, 'sector': sec})
             for k, (t, nm, sec, sc, dv, pb) in enumerate(rank[:50] if al.get('DV', 0) > 0 and rank else [], start=1)]
    # ③ LVH · REV 상위 3 (후보 상위 50은 분석용으로 따로 기록)
    pk = picks(cfg)
    for s, fn in (('LVH', S.lvh_scores), ('REV', S.rev_scores)):
        skip, top = pk[s]
        sc = fn(F, d)
        for k, t in enumerate(S.top_n(sc.dropna(), set(), 50), start=1):
            cands.append((s, t, k, float(sc[t]), float(C.at[d, t]), feat_info(F, d, t)))
        if al.get(s, 0) <= 0:
            continue
        held = {l['ticker'] for l in open_lots(s)}
        for k, t in enumerate(S.top_n(sc, held, top + 3, skip), start=1):            # +3은 예비(자리 · 가격 때문에 못 살 때 화면 참고용 · 주문은 top개만)
            info = {**feat_info(F, d, t), 'spare': k > top}
            rows.append((d, s, k if k <= top else 100 + k, t, st.get(t, {}).get('name', t), float(sc[t]), float(C.at[d, t]), json.dumps(info)))
    x.execute('DELETE FROM signals WHERE date=?', (d,))
    x.executemany('INSERT OR REPLACE INTO signals VALUES (?,?,?,?,?,?,?,?)', rows)
    x.execute('INSERT OR REPLACE INTO days VALUES (?,?,?)', (d, db.now_s(), f"{len(rows)} 신호 · 수급 {F['flow_src'] or '없음'}"))
    x.commit()
    J.save_cands(d, cands)
    db.meta_set('last_signal_date', d)
    for m in ('paper', 'real'):                                                      # 청산 거래 사후 계산 (오늘 일봉까지 들어왔으므로)
        try:
            J.enrich(m)
        except Exception as e:
            log(f'거래 사후 계산 실패({m}): {CF.clean(e)[:120]}', 'warn')
    log(f"{d} 신호 계산 끝 ({time.time() - t0:.0f}초 · 후보풀 {int(F['pool'].loc[d].sum())} · 수급 {F['flow_src'] or '없음'})")
    return rows


def feat_info(F, d, t):
    """진입 근거 지표 — 거래 · 후보에 같이 저장 (나중에 구간별 수익 분석)"""
    return {'rsi': _r(F['rsi14'], d, t), 'atrp': _r(F['atrp'], d, t), 'fromhi': _r(F['fromhi'], d, t), 'heat': _r(F['heat'], d, t),
            'fr20': _r(F['fr20'], d, t), 'pen20': _r(F['pen20'], d, t), 'val20': _r(F['val20'], d, t) if 'val20' in F else None}


def _r(T, d, t):
    try:
        v = T.at[d, t]
        return None if v != v else round(float(v), 5)
    except Exception:
        return None


# ════════════════════════════════════════════
#  장중 실시간 평가 · 안전장치 · 매시 리포트
# ════════════════════════════════════════════
def live_value(kc=None, prices=None):
    """보유 묶음 × (웹소켓 가격 → 없으면 마지막 가격) + 현금(마지막 평가 기록) → (평가액, 보유 평가)"""
    prices = prices or {}
    x = db.conn()
    hold = 0.0
    for l in open_lots():
        if l['status'] != '보유':
            continue
        px = prices.get(l['ticker']) or l['last_px'] or l['entry_px'] or 0
        hold += l['qty'] * px
    e = x.execute('SELECT cash FROM equity ORDER BY date DESC LIMIT 1').fetchone()
    return (e[0] if e else None), hold


def intraday(cfg, kc, d, prices):
    """장중 1분마다: 계좌 평가(KIS 잔고) 기록 · 하루 손실이 한도에 닿으면 새 매수 중지(매도는 그대로)"""
    bal = kc.balance()
    x = db.conn()
    x.execute('INSERT OR REPLACE INTO intraday VALUES (?,?)', (now().strftime('%Y%m%d%H%M'), bal['equity']))
    x.execute("DELETE FROM intraday WHERE ts < ?", ((now() - timedelta(days=10)).strftime('%Y%m%d%H%M'),))
    prev = x.execute('SELECT value FROM equity WHERE date<? ORDER BY date DESC LIMIT 1', (d,)).fetchone()
    x.commit()
    lim = float(cfg.get('day_loss_limit') or 4)
    if prev and prev[0] and bal['equity'] / prev[0] - 1 <= -lim / 100 and not db.meta_get('auto_pause'):
        why = f'장중 하루 {(bal["equity"] / prev[0] - 1) * 100:.1f}%'
        db.meta_set('auto_pause', f'{d} {why}')
        log(f'계좌 안전장치: {why} → 새 매수 자동 중지 (매도는 계속)', 'error')
        alert(f'계좌 안전장치 — {why} → 새 매수 자동 중지. 확인 후 화면에서 해제하세요', 'breaker')
    return bal


def hourly_text(cfg, bal, d):
    x = db.conn()
    prev = x.execute('SELECT value FROM equity WHERE date<? ORDER BY date DESC LIMIT 1', (d,)).fetchone()
    L = [f"⏰ {now():%H:%M} {'[실전] ' if db.mode() == 'real' else ''}계좌 {bal['equity']:,.0f}원"
         + (f" · 오늘 {bal['equity'] - prev[0]:+,.0f}원 ({(bal['equity'] / prev[0] - 1) * 100:+.2f}%)" if prev and prev[0] else '')]
    pos = sorted(bal['positions'], key=lambda p: -(p['pnl'] or 0))
    if pos:
        L.append('▲ ' + ' · '.join(f"{p['name']} {p['pnl']:+,.0f}" for p in pos[:3]))
        L.append('▼ ' + ' · '.join(f"{p['name']} {p['pnl']:+,.0f}" for p in pos[-3:][::-1]))
    fills = x.execute('SELECT COUNT(*) FROM orders WHERE date=? AND filled>0', (d,)).fetchone()[0]
    L.append(f'오늘 체결 {fills}건 · 보유 {len(pos)}종목')
    return '\n'.join(L)


def holiday_check(cfg, d):
    """08:05 하루 1번 — 실전 키가 있으면 KIS 공식 휴장일 조회 (모의 모드여도 조회만)"""
    if db.gmeta_get(f'open_{d}') in ('Y', 'N'):
        return
    if not configured(cfg, 'real') and not (CF.acct(cfg, 'real').get('app_key') and CF.acct(cfg, 'real').get('app_secret')):
        return
    try:
        a = CF.acct(cfg, 'real')
        kc = KIS('real', a.get('app_key'), a.get('app_secret'), a.get('account') or '', db.DATA_DIR)
        o = kc.is_open_day(d)
        if o is not None:
            db.gmeta_set(f'open_{d}', 'Y' if o else 'N')
            if not o:
                log(f'{d} KIS 휴장일 조회: 휴장 → 오늘 쉼')
    except Exception as e:
        log(f'휴장일 조회 실패(내장 목록 사용): {CF.clean(e)}', 'warn')


# ════════════════════════════════════════════
#  모의 → 실전 전환 판정
# ════════════════════════════════════════════
def gate(cfg):
    """모의 장부 기준 (사전 등록): 60거래일+ · 수익 > 0 · 최대 낙폭 −15% 이내 · 주문 사고 0 · 잔고 불일치 0 · 청산 30건+"""
    x = db.conn('paper')
    eq = [r[0] for r in x.execute('SELECT value FROM equity ORDER BY date')]
    sv = float(db.meta_get('start_value', '', 'paper') or (eq[0] if eq else 0) or 0)
    peak, mdd = 0.0, 0.0
    for v in eq:
        peak = max(peak, v)
        mdd = min(mdd, v / peak - 1) if peak else mdd
    ret = (eq[-1] / sv - 1) * 100 if eq and sv else None
    amb = x.execute("SELECT COUNT(*) FROM orders WHERE status='불분명'").fetchone()[0]
    closed = x.execute("SELECT COUNT(*) FROM lots WHERE status='청산' AND sleeve NOT IN ('ON','SW')").fetchone()[0]
    mism = db.mconn().execute("SELECT COUNT(*) FROM log WHERE mode='paper' AND msg LIKE '%잔고 불일치%'").fetchone()[0]
    need = int(cfg.get('min_paper_days') or 60)
    rows = [{'k': f'모의 운용 {need}거래일+', 'v': f'{len(eq)}일', 'ok': len(eq) >= need, 'prog': min(1, len(eq) / need)},
            {'k': '모의 수익 > 0 (비용 · 세금 반영된 계좌 기준)', 'v': '-' if ret is None else f'{ret:+.2f}%', 'ok': ret is not None and ret > 0, 'prog': None},
            {'k': '최대 낙폭 −15% 이내', 'v': f'{mdd * 100:.1f}%', 'ok': mdd >= -0.15 and bool(eq), 'prog': None},
            {'k': '주문 결과 불분명 0건', 'v': f'{amb}건', 'ok': amb == 0, 'prog': None},
            {'k': '장 마감 잔고 불일치 0건', 'v': f'{mism}건', 'ok': mism == 0, 'prog': None},
            {'k': '주식 청산 30건+ (재현성 확인)', 'v': f'{closed}건', 'ok': closed >= 30, 'prog': min(1, closed / 30)}]
    return {'rows': rows, 'pass': all(r['ok'] for r in rows)}


def switch_mode(cfg, target, confirm=False):
    """모의 ↔ 실전 — 바뀌는 것은 계좌 설정(앱키 · 시크릿 · 계좌 · 운용 한도)과 그 계좌의 장부뿐. 전략 · 일정 · 안전장치 · 자동주문 상태는 그대로.
       실전으로 갈 때만 실전 계좌가 저장돼 있어야 하고 화면에서 한 번 확인(confirm=True). 모의 성적 판정은 참고로 기록"""
    if target not in ('paper', 'real'):
        raise ValueError('모드는 paper · real')
    if target == cfg.get('mode', 'paper') == db.mode():
        return cfg
    if target == 'real':
        if not configured(cfg, 'real'):
            raise ValueError('실전 앱키 · 시크릿 · 계좌를 먼저 저장하세요')
        if not confirm:
            raise ValueError('실전 계좌로 바꾸려면 확인이 필요합니다')
    with _lock:                                                                   # 주문 · 체결 반영 중에는 기다렸다가 전환
        g = gate(cfg) if target == 'real' else None
        cfg['mode'] = target
        db.set_mode(target)
    log(f"모드 전환 → {'🔴 실전 계좌' if target == 'real' else '🟢 모의 계좌'} · 자동주문 {'ON' if cfg.get('kis_on') else 'OFF'} 그대로"
        + (f" · 모의 판정 {'통과' if g['pass'] else '미달'}" if g else ''), 'warn')
    return cfg


# ════════════════════════════════════════════
#  일정 루프
# ════════════════════════════════════════════
def loop(get_cfg, get_prices=lambda: {}, notify_hourly=None):
    time.sleep(10)
    last_sync, last_live = 0.0, 0.0
    while True:
        try:
            cfg = get_cfg()
            d, hm = today(), now().strftime('%H:%M')
            if '08:05' <= hm <= '08:15' and not _done('holiday', d):
                _mark('holiday', d)
                holiday_check(cfg, d)
            if configured(cfg) and is_trading_day(d) and '08:20' <= hm <= '23:59':
                kc = client(cfg)
                with _lock:
                    STATE['running'] = True
                    if '08:20' <= hm < '08:30' and cfg.get('kis_on') and not _done('check', d):
                        _mark('check', d)
                        precheck(cfg, kc, d)
                    if cfg.get('preopen_time', '08:50') <= hm <= '08:58' and not _done('pre', d) and can_order(cfg):
                        _mark('pre', d)
                        preopen(cfg, kc, d)
                    if '08:59' <= hm <= '15:00' and not _done('pre', d) and can_order(cfg):     # 장전 주문을 놓쳤으면 매도만
                        _mark('pre', d)
                        late_open(cfg, kc, d)
                    if '09:02' <= hm <= '09:20' and not _done('defer', d) and can_order(cfg):
                        _mark('defer', d)
                        deferred(cfg, kc, d)
                    if '09:01' <= hm <= '15:35' and time.time() - last_sync > 60:
                        last_sync = time.time()
                        sync(kc, d)
                    if '09:01' <= hm <= '15:30' and time.time() - last_live > 60 and cfg.get('kis_on'):
                        last_live = time.time()
                        bal = intraday(cfg, kc, d, get_prices())
                        hh = now().strftime('%H')
                        if cfg.get('hourly_report') and notify_hourly and hm[3:] < '05' and '10' <= hh <= '15' and not _done(f'hour{hh}', d):
                            _mark(f'hour{hh}', d)
                            notify_hourly(hourly_text(cfg, bal, d))
                    if '15:10' <= hm <= '15:17' and not _done('swprep', d) and can_order(cfg):
                        _mark('swprep', d)
                        sweep_prep(cfg, kc, d)
                    if '15:20' <= hm <= '15:27' and not _done('on', d) and can_order(cfg):
                        _mark('on', d)
                        on_buy(cfg, kc, d)
                        if not halted():
                            sweep_buy(cfg, kc, d)
                    if '15:45' <= hm <= '23:59' and not _done('eod', d):                       # 늦게 켜져도 그날 마감 기록
                        _mark('eod', d)
                        eod(cfg, kc, d)
                    STATE['running'] = False
        except Exception as e:
            STATE['running'] = False
            STATE['last_err'] = f'{now():%H:%M:%S} {CF.clean(e)[:200]}'
            log(f'일정 오류: {CF.clean(e)[:200]}', 'error')
            alert(f'일정 오류 — {CF.clean(e)[:200]}', 'looperr')
            time.sleep(50)
        time.sleep(10)
