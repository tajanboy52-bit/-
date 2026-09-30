"""
q_trader.py — TK Quant 실전(KIS 모의투자) 매매 · 일정

하루 흐름 (거래일)
  08:20  장전 점검 — KIS 연결 · 잔고 · 앱이 모르는 보유 종목 · 신호 날짜 (주문 없음 · 문제면 텔레그램)
  08:35  장전 시장가: ① 매도 표시된 묶음(LVH 20일 · REV 9EMA/10일 · DV 교체 · 어제 산 ON ETF) ② 새 매수(현금 안에서) → 시가 체결
  09:02  현금이 모자라 미뤄 둔 매수 (아침에 판 돈으로) · 장전 시간 때문에 거절된 주문 한 번 더
  장중   체결 반영 (60초 · 웹소켓 체결 통보가 오면 곧바로) — 손절 없음 (Scout 검증: 모든 모델에서 손절이 평균 수익을 깎음)
  15:20  🌙 ON: KODEX 코스닥150 장마감 동시호가 시장가 매수 (종가 체결)
  15:45  체결 마감 · 잔고 대조 · 평가 기록 · 계좌 안전장치(고점 대비 낙폭 · 하루 손실)
  18:40~ Scout 일봉 · 수급이 들어오면 신호 계산 → 매도 표시 · 다음 날 매수 후보 → 텔레그램 (q_server가 호출)
안전: 자동주문 기본 OFF · 결과 불분명 주문 → 정지(재주문 없음) · 모르는 보유 종목 → 새 매수 차단 · 1회 주문 한도 · 하루 매수 한도 · 긴급 정지
"""
import json
import threading
import time
from datetime import datetime, timedelta

import pandas as pd

import q_db as db
import q_signals as S
from q_kis import KISError, KISPaper

HOLIDAYS = {'20261005', '20261009', '20261225', '20261231',
            '20270101', '20270208', '20270209', '20270301', '20270503', '20270505', '20270513', '20270719', '20270816',
            '20270914', '20270915', '20270916', '20271004', '20271011', '20271227', '20271231'}
SLOTS = {'LVH': 20, 'REV': 10, 'DV': 15}
DEFAULT_ALLOC = {'LVH': 40, 'REV': 25, 'DV': 20, 'ON': 15}
COSTS = {'LVH': 0.25, 'REV': 0.25, 'DV': 0.25, 'ON': 0.05}        # 손익 표시용 왕복 비용 추정 %
KIND = {'entry': '매수', 'hold20': '20일 보유 끝', 'ema9': '9EMA 복귀', 'hold10': '10일 만료', 'dv_rebal': '배당·가치 교체', 'on_buy': '밤사이 매수(종가)',
        'on_sell': '밤사이 매도(시가)', 'manual': '수동', 'delist': '거래 끊김 정리'}
STATE = {'running': False, 'last_sync': '', 'last_err': ''}
_lock = threading.Lock()
NOTIFY = None                     # q_server가 텔레그램 함수를 넣어 줌


def now():
    return datetime.now()


def today():
    return now().strftime('%Y%m%d')


def log(msg, level='info'):
    db.log(f'[매매] {msg}', level)


def is_trading_day(d=None):
    d = d or today()
    return datetime.strptime(d, '%Y%m%d').weekday() < 5 and d not in HOLIDAYS and db.meta_get(f'closed_{d}') != '1'


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


def alloc(cfg):
    a = dict(DEFAULT_ALLOC)
    a.update({k: float(v) for k, v in (cfg.get('alloc') or {}).items() if k in a})
    return a


def cap(cfg):
    try:
        return max(1_000_000, int(float(cfg.get('cap') or 10_000_000)))
    except Exception:
        return 10_000_000


def client(cfg):
    import os
    return KISPaper(cfg.get('kis_app_key'), cfg.get('kis_app_secret'), cfg.get('kis_account'), os.path.join(db.DATA_DIR, 'kis_token.json'))


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
def send(cfg, kc, side, kind, lot_id, sleeve, ticker, name, qty, ord_dvsn='01', price=0):
    x = db.conn()
    x.execute("""INSERT INTO orders (date, ts, sleeve, lot_id, ticker, name, side, kind, qty, ord_dvsn, price, status)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,'보냄')""", (today(), db.now_s(), sleeve, lot_id, ticker, name, side, kind, int(qty), ord_dvsn, float(price or 0)))
    oid = x.execute('SELECT last_insert_rowid()').fetchone()[0]
    x.commit()
    try:
        r = kc.order(side, ticker, qty, ord_dvsn, price)
        x.execute("UPDATE orders SET status='접수', order_no=?, org_no=?, msg=? WHERE id=?", (r['order_no'], r['org_no'], r['msg'][:200], oid))
        x.commit()
        log(f"{'🟢' if side == 'buy' else '🔵'} [{sleeve}] {KIND.get(kind, kind)} {name} {qty}주 {'시장가' if ord_dvsn == '01' else f'지정가 {price:,.0f}'}")
        return oid
    except KISError as e:
        msg = str(e)
        amb = 'AMBIGUOUS' in msg
        x.execute('UPDATE orders SET status=?, msg=? WHERE id=?', ('불분명' if amb else '거절', msg[:200], oid))
        x.commit()
        if amb:
            halt(f'{name} {KIND.get(kind, kind)} 주문 결과 불분명 — KIS 앱에서 체결 여부를 확인한 뒤 정지 해제')
        else:
            log(f'주문 거절 {name} {KIND.get(kind, kind)}: {msg}', 'warn')
        return None
    except Exception as e:                           # kc.order는 전송 뒤 오류를 모두 KISError로 바꿈 → 여기는 전송 전 오류
        x.execute("UPDATE orders SET status='거절', msg=? WHERE id=?", (f'전송 전 오류: {str(e)[:180]}', oid))
        x.commit()
        log(f'주문 전송 전 오류 {name}: {str(e)[:150]}', 'warn')
        return None


def sync(kc, d=None):
    """그날 체결 내역 → 주문 · 묶음(lot) 반영 (늘어난 만큼만 · 두 번 반영 안 함)"""
    d = d or today()
    x = db.conn()
    fills = {f['order_no']: f for f in kc.fills(d) if f['order_no']}
    for o in [dict(r) for r in x.execute("SELECT * FROM orders WHERE date=? AND order_no IS NOT NULL AND order_no!='' AND status NOT IN ('체결','취소','거절','만료')", (d,))]:
        f = fills.get(o['order_no'])
        if not f:
            continue
        filled, avg = f['filled'], f['avg']
        st = '체결' if filled >= o['qty'] else ('취소' if f['cancelled'] or (f['remain'] == 0 and filled < o['qty']) else ('부분' if filled else o['status']))
        delta = filled - (o['applied'] or 0)
        x.execute('UPDATE orders SET filled=?, avg=?, status=?, applied=? WHERE id=?', (filled, avg, st, filled, o['id']))
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
    if o['side'] == 'buy':
        x.execute("UPDATE lots SET qty=?, qty0=?, entry_px=?, cost=?, entry_date=?, status='보유', last_px=COALESCE(last_px, ?), updated=? WHERE id=?",
                  (filled, filled, avg, filled * avg, d, avg, db.now_s(), lot['id']))
        log(f"✅ [{o['sleeve']}] 매수 체결 {o['name']} {filled}주 @ {avg:,.0f}")
        return
    got = filled * avg - (o['applied'] or 0) * (o['avg'] or 0)            # o는 반영 전 값 (누적 체결 · 평균가)
    left = max(0, (lot['qty'] or 0) - delta)
    proceeds = (lot['proceeds'] or 0) + got
    if left > 0:
        x.execute('UPDATE lots SET qty=?, proceeds=?, updated=? WHERE id=?', (left, proceeds, db.now_s(), lot['id']))
        return
    fee = lot['cost'] * COSTS.get(lot['sleeve'], 0.25) / 100
    pnl = proceeds - lot['cost'] - fee
    x.execute("""UPDATE lots SET qty=0, proceeds=?, status='청산', exit_date=?, exit_px=?, pnl=?, ret=?, updated=? WHERE id=?""",
              (proceeds, d, avg, pnl, pnl / lot['cost'] * 100 if lot['cost'] else 0, db.now_s(), lot['id']))
    log(f"{'💰' if pnl > 0 else '🔻'} [{lot['sleeve']}] 청산 {lot['name']} {pnl / lot['cost'] * 100 if lot['cost'] else 0:+.2f}% · {pnl:+,.0f}원 ({KIND.get(o['kind'], o['kind'])})")


def _finish_buy(x, o, filled):
    """취소 · 만료 · 거절된 매수 → 묶음 정리 (일부만 체결됐으면 그만큼만 보유)"""
    lot = x.execute('SELECT * FROM lots WHERE id=?', (o['lot_id'],)).fetchone()
    if lot and lot['status'] == '주문' and not filled:
        x.execute("UPDATE lots SET status='미체결', updated=? WHERE id=?", (db.now_s(), lot['id']))


def new_lot(sleeve, ticker, name, sector, signal_date):
    x = db.conn()
    x.execute("INSERT INTO lots (sleeve, ticker, name, sector, signal_date, status, updated) VALUES (?,?,?,?,?,'주문',?)",
              (sleeve, ticker, name, sector, signal_date, db.now_s()))
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
                 'qty': 0, 'amt': 0, 'skip': ''}
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
        probs.append(f'신호가 전 거래일 것이 아님 (마지막 {sd or "없음"}) → 오늘 새 매수 없음 · Scout 수집 확인')
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
    for l in pl['sells']:
        send(cfg, kc, 'sell', l['sell_reason'] or 'manual', l['id'], l['sleeve'], l['ticker'], l['name'], l['qty'])
        if halted():
            return
    if unknown or buy_paused(cfg):
        log('새 매수 안 함 — ' + ('모르는 보유 종목' if unknown else '매수 일시 중지'), 'warn')
        return
    if sd != prev_trading_day(d):
        log(f'신호가 전 거래일 것이 아님 (마지막 {sd}) → 오늘 새 매수 없음', 'warn')
        return
    if db.meta_get(f'plan_used_{sd}') == '1':
        log(f'{sd} 신호는 이미 주문함', 'warn')
        return
    db.meta_set(f'plan_used_{sd}', '1')
    for o in pl['buys']:
        if o['skip']:
            continue
        lid = new_lot(o['sleeve'], o['ticker'], o['name'], '', sd)
        db.conn().commit()
        send(cfg, kc, 'buy', 'entry', lid, o['sleeve'], o['ticker'], o['name'], o['qty'])
        if halted():
            return
    db.meta_set(f'defer_{d}', json.dumps([{k: o[k] for k in ('sleeve', 'ticker', 'name', 'qty', 'ref')} for o in pl['defer']], ensure_ascii=False))
    if pl['defer']:
        log(f"현금 부족으로 {len(pl['defer'])}건은 09:02에 (아침 매도 체결 뒤)")


def deferred(cfg, kc, d):
    """09:02 — 미뤄 둔 매수 (아침에 판 돈 · 밤사이 ETF 판 돈으로) + 장전 시간 때문에 거절된 매도 재시도"""
    sync(kc, d)
    x = db.conn()
    for o in [dict(r) for r in x.execute("SELECT * FROM orders WHERE date=? AND status='거절' AND side='sell' AND ord_dvsn='01'", (d,))]:
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
    bal = kc.balance()
    cash = min(bal['cash_d2'] or bal['cash'], bal['cash'] or bal['cash_d2'])
    for o in retry_buy:
        x.execute("UPDATE orders SET status='거절(재시도)' WHERE id=?", (o['id'],))
        x.execute("UPDATE lots SET status='미체결' WHERE id=? AND status='주문'", (o['lot_id'],))
        x.commit()
        todo.append({'sleeve': o['sleeve'], 'ticker': o['ticker'], 'name': o['name'], 'qty': o['qty'], 'ref': o['price'] or 0})
    for o in todo:
        try:
            px, _ = kc.price(o['ticker'])
        except Exception:
            px = o['ref']
        if not px or px * o['qty'] * 1.01 > cash:
            q = int(cash / 1.01 // px) if px else 0
            if q <= 0:
                log(f"[{o['sleeve']}] {o['name']} 09:02 매수도 현금 부족 → 건너뜀", 'warn')
                continue
            o['qty'] = min(o['qty'], q)
        lid = new_lot(o['sleeve'], o['ticker'], o['name'], '', db.meta_get('last_signal_date'))
        x.commit()
        if send(cfg, kc, 'buy', 'entry', lid, o['sleeve'], o['ticker'], o['name'], o['qty']):
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
    px, _ = kc.price(S.ON_TICKER)
    base = min(bal['equity'] or cap(cfg), cap(cfg))
    cash = min(bal['cash_d2'] or bal['cash'], bal['cash'] or bal['cash_d2'])
    q = int(min(base * al['ON'] / 100, cash * 0.99) // (px * 1.005)) if px else 0
    if q <= 0:
        log('밤사이 ETF 매수 건너뜀 — 현금 부족', 'warn')
        return
    lid = new_lot('ON', S.ON_TICKER, S.ON_NAME, 'ETF', d)
    db.conn().execute('UPDATE lots SET sell_flag=1, sell_reason=? WHERE id=?', ('on_sell', lid))
    db.conn().commit()
    if not send(cfg, kc, 'buy', 'on_buy', lid, 'ON', S.ON_TICKER, S.ON_NAME, q) and not halted():
        alert(f'밤사이 ETF 매수 거절 ({S.ON_NAME} {q}주) — 로그 확인', 'onfail')


def eod(cfg, kc, d):
    """15:45 — 체결 마감 · 못 산 매수 정리 · 잔고 대조 · 평가 기록 · 계좌 안전장치"""
    x = db.conn()
    sync(kc, d)
    for o in [dict(r) for r in x.execute("SELECT * FROM orders WHERE date=? AND status IN ('보냄','접수','부분')", (d,))]:
        x.execute("UPDATE orders SET status='만료' WHERE id=?", (o['id'],))
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
    for s in S.SLEEVES:
        ls = [l for l in open_lots(s) if l['status'] == '보유']
        inv = sum(l['cost'] * (l['qty'] / l['qty0'] if l['qty0'] else 1) for l in ls)
        val = sum(l['qty'] * (l['last_px'] or l['entry_px'] or 0) for l in ls)
        rz = x.execute("SELECT COALESCE(SUM(pnl),0) FROM lots WHERE sleeve=? AND status='청산'", (s,)).fetchone()[0]
        x.execute('INSERT OR REPLACE INTO sleeve_daily VALUES (?,?,?,?,?,?)', (d, s, inv, val, rz, len(ls)))
    x.commit()
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
#  장 마감 뒤 신호 계산 (Scout 데이터가 들어온 뒤)
# ════════════════════════════════════════════
def signal_job(cfg, d, progress=None):
    """d 종가 기준: 보유 일수 · 매도 표시 · 새 신호(LVH · REV 상위 3, DV 월 교체) → signals 표 · 다음 거래일 08:35에 주문"""
    t0 = time.time()
    days = db.trading_days('20180101', d)
    if not days or days[-1] != d:
        raise RuntimeError(f'Scout 일봉에 {d}이 아직 없음')
    frm = days[max(0, len(days) - 400)]
    st = db.stocks()
    excl = {t for t, v in st.items() if v['excluded'] or any(w in (v['warns'] or '') for w in ('관리종목', '투자경고', '투자위험', '거래정지', '정리매매'))}
    since = (datetime.strptime(d, '%Y%m%d') - timedelta(days=60)).strftime('%Y%m%d')
    excl |= set(db.dilutive_events(since))
    P = db.panel(frm, d)
    FL = db.flows(days[max(0, len(days) - 60)], d)
    F = S.features(P, FL, excl)
    x = db.conn()
    C = F['close']
    # ① 보유 일수 (체결일부터 d까지 거래된 날 수) · 매도 표시
    for l in [dict(r) for r in x.execute("SELECT * FROM lots WHERE status='보유' AND sleeve!='ON'")]:
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
        if m not in mem and cfg.get('krx_id'):
            try:
                db.krx_month(next(x_ for x_ in days if x_[:6] == m), cfg)
                mem, mon = db.month_tables()
            except Exception as e:
                log(f'KRX {m} 월 자료 실패 → 지난달 순위 사용: {str(e)[:120]}', 'warn')
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
    # ③ LVH · REV 상위 3
    for s, fn, top in (('LVH', S.lvh_scores, S.LVH['top']), ('REV', S.rev_scores, S.REV['top'])):
        if al.get(s, 0) <= 0:
            continue
        sc = fn(F, d)
        held = {l['ticker'] for l in open_lots(s)}
        for k, t in enumerate(S.top_n(sc, held, top + 3), start=1):                  # +3은 예비(자리 · 가격 때문에 못 살 때 화면 참고용 · 주문은 상위 3만)
            info = {'rsi': _r(F['rsi14'], d, t), 'atrp': _r(F['atrp'], d, t), 'fromhi': _r(F['fromhi'], d, t), 'heat': _r(F['heat'], d, t),
                    'fr20': _r(F['fr20'], d, t), 'pen20': _r(F['pen20'], d, t), 'spare': k > top}
            rows.append((d, s, k if k <= top else 100 + k, t, st.get(t, {}).get('name', t), float(sc[t]), float(C.at[d, t]), json.dumps(info)))
    x.execute('DELETE FROM signals WHERE date=?', (d,))
    x.executemany('INSERT OR REPLACE INTO signals VALUES (?,?,?,?,?,?,?,?)', rows)
    x.execute('INSERT OR REPLACE INTO days VALUES (?,?,?)', (d, db.now_s(), f"{len(rows)} 신호 · 수급 {F['flow_src'] or '없음'}"))
    x.commit()
    db.meta_set('last_signal_date', d)
    log(f"{d} 신호 계산 끝 ({time.time() - t0:.0f}초 · 후보풀 {int(F['pool'].loc[d].sum())} · 수급 {F['flow_src'] or '없음'})")
    return rows


def _r(T, d, t):
    try:
        v = T.at[d, t]
        return None if v != v else round(float(v), 5)
    except Exception:
        return None


# ════════════════════════════════════════════
#  일정 루프
# ════════════════════════════════════════════
def loop(get_cfg):
    time.sleep(10)
    last_sync = 0.0
    while True:
        try:
            cfg = get_cfg()
            d, hm = today(), now().strftime('%H:%M')
            ready = cfg.get('kis_app_key') and cfg.get('kis_account')
            if ready and is_trading_day(d) and '08:20' <= hm <= '16:30':
                kc = client(cfg)
                with _lock:
                    STATE['running'] = True
                    if '08:20' <= hm < '08:30' and cfg.get('kis_on') and not _done('check', d):
                        _mark('check', d)
                        precheck(cfg, kc, d)
                    if '08:35' <= hm <= '08:58' and not _done('pre', d) and can_order(cfg):
                        _mark('pre', d)
                        preopen(cfg, kc, d)
                    if '09:02' <= hm <= '09:20' and not _done('defer', d) and can_order(cfg):
                        _mark('defer', d)
                        deferred(cfg, kc, d)
                    if '09:01' <= hm <= '15:35' and time.time() - last_sync > 60:
                        last_sync = time.time()
                        sync(kc, d)
                    if '15:20' <= hm <= '15:27' and not _done('on', d) and can_order(cfg):
                        _mark('on', d)
                        on_buy(cfg, kc, d)
                    if '15:45' <= hm <= '16:30' and not _done('eod', d):
                        _mark('eod', d)
                        eod(cfg, kc, d)
                    STATE['running'] = False
        except Exception as e:
            STATE['running'] = False
            STATE['last_err'] = f'{now():%H:%M:%S} {str(e)[:200]}'
            log(f'일정 오류: {str(e)[:200]}', 'error')
            alert(f'일정 오류 — {str(e)[:200]}', 'looperr')
            time.sleep(50)
        time.sleep(10)
