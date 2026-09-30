"""
bluechip_broker.py — 🧾 H1 모델 KIS 모의투자 자동주문 (우량주 앱 안의 모의투자 계좌)

규칙(가상 모델 H와 같음): 우량주 앱 A 신호 → 다음 날 장전 시장가 매수 · 체결 뒤 +5% 지정가로 30% 매도(매일 아침 다시 넣음)
· 나머지는 종가가 보유 중 최고 종가 −4%면 다음 날 장전 시장가 매도 · 장중 30초마다 −15% 재난 손절(시장가)
· 40거래일째 15:20 장마감 시장가 · 종목당 = min(KIS 평가액, 운용 한도) ÷ 10 · 최대 14종목 · 업종당 2
안전: 자동주문 기본 OFF · 주문 결과 불분명(네트워크) → 재주문 없이 정지(HALT) · KIS 잔고가 기준 · 모르는 보유 종목이 있으면 신규 매수 차단
· 우량주 앱 신호가 전 거래일 것이 아니면 매수 안 함 · 1회 · 하루 매수 금액 한도 · 비밀 값은 로그 · 화면 · zip에 안 남김
"""
import os
import threading
import time
from datetime import datetime

import bluechip_db as db
import bluechip_engine as eng
from bluechip_kis import KISError, KISPaper, tick_up

HOLIDAYS = {'20261005', '20261009', '20261225', '20261231', '20270101', '20270208', '20270209', '20270301', '20270505'}
PART, TRAIL, STOP, HOLD, MAXPOS, SECCAP, SLOTS = 0.30, 4.0, 15.0, 40, 14, 2, 10
STATE = {'last_monitor': '', 'last_err': '', 'running': False}
_lock = threading.Lock()
SCHEMA = """
CREATE TABLE IF NOT EXISTS kis_pos (ticker TEXT PRIMARY KEY, name TEXT, sector TEXT, signal_date TEXT, entry_date TEXT, entry_px REAL,
    qty_total INTEGER, qty INTEGER, cost REAL, realized REAL DEFAULT 0, part_done INTEGER DEFAULT 0, part_px REAL, peak REAL DEFAULT 0,
    days INTEGER DEFAULT 0, sell_next INTEGER DEFAULT 0, last_px REAL, status TEXT, updated TEXT);
CREATE TABLE IF NOT EXISTS kis_orders (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, ts TEXT, ticker TEXT, name TEXT, side TEXT, kind TEXT,
    qty INTEGER, ord_dvsn TEXT, price REAL, order_no TEXT, org_no TEXT, status TEXT, filled INTEGER DEFAULT 0, applied INTEGER DEFAULT 0,
    avg REAL, msg TEXT);
CREATE TABLE IF NOT EXISTS kis_closed (id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, name TEXT, signal_date TEXT, entry_date TEXT,
    entry_px REAL, qty_total INTEGER, exit_date TEXT, cost REAL, proceeds REAL, pnl REAL, ret REAL, reason TEXT, days INTEGER);
CREATE TABLE IF NOT EXISTS kis_equity (date TEXT PRIMARY KEY, cash REAL, value REAL, npos INTEGER);
CREATE TABLE IF NOT EXISTS kis_log (id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, level TEXT, msg TEXT);
"""
KIND = {'entry': '매수', 'tp': '1차 익절 +5%', 'trail': '추적 매도', 'stop': '재난 손절', 'time': '40일 만료', 'manual': '수동'}


_ready = set()


def c():
    x = db.conn()
    if id(x) not in _ready:
        x.executescript(SCHEMA)
        _ready.add(id(x))
    return x


def now():
    return datetime.now()


def today():
    return now().strftime('%Y%m%d')


def log(msg, level='info'):
    c().execute('INSERT INTO kis_log (ts, level, msg) VALUES (?,?,?)', (now().isoformat(timespec='seconds'), level, str(msg)[:500]))
    c().commit()
    eng.log(f'[모의투자] {msg}')


def is_trading_day(d=None):
    d = d or today()
    return datetime.strptime(d, '%Y%m%d').weekday() < 5 and d not in HOLIDAYS and db.meta_get(f'kis_closed_{d}') != '1'


def prev_trading_day(d):
    from datetime import timedelta
    t = datetime.strptime(d, '%Y%m%d')
    for _ in range(15):
        t -= timedelta(days=1)
        s = t.strftime('%Y%m%d')
        if t.weekday() < 5 and s not in HOLIDAYS:
            return s
    return ''


def client(cfg):
    return KISPaper(cfg.get('kis_app_key'), cfg.get('kis_app_secret'), cfg.get('kis_account'), os.path.join(db.DATA_DIR, 'kis_token.json'))


def halted():
    return db.meta_get('kis_halt', '')


NOTIFY = None          # B0.6: 서버가 텔레그램 함수를 넣어 줌 — 건별 매매 알림이 아니라 '사람이 봐야 하는 경고'만


def alert(msg, key):
    """중요 경고를 텔레그램으로 (같은 경고는 하루 한 번). 실패해도 매매 흐름에 영향 없음"""
    try:
        k = f'kis_alert_{key}_{today()}'
        if db.meta_get(k) == '1' or not NOTIFY:
            return
        db.meta_set(k, '1')
        NOTIFY('🚨 ' + msg)
    except Exception as e:
        eng.log(f'[모의투자] 경고 알림 실패: {e}')


def halt(reason):
    db.meta_set('kis_halt', f"{now():%m-%d %H:%M} {reason}"[:300])
    log(f'⛔ 자동주문 정지: {reason}', 'error')
    alert(f"H1 모의투자 자동주문 정지 — {reason}\nKIS 앱(모의투자)에서 체결 여부를 확인한 뒤 우량주 앱 모의투자 탭에서 '정지 해제'", 'halt')


def can_order(cfg):
    return bool(cfg.get('kis_on')) and not halted()


def _done(job, d):
    return db.meta_get(f'kis_done_{job}_{d}') == '1'


def _mark(job, d):
    db.meta_set(f'kis_done_{job}_{d}', '1')


def _cap(cfg):
    try:
        return max(1_000_000, int(float(cfg.get('kis_cap') or 10_000_000)))
    except Exception:
        return 10_000_000


# ════════════════════════════════════════════
#  주문 · 체결
# ════════════════════════════════════════════
def send(cfg, kc, side, kind, ticker, name, qty, ord_dvsn='01', price=0):
    """주문 한 건 — 기록 · 결과 불분명이면 정지"""
    x = c()
    x.execute("INSERT INTO kis_orders (date, ts, ticker, name, side, kind, qty, ord_dvsn, price, status) VALUES (?,?,?,?,?,?,?,?,?,'보냄')",
              (today(), now().isoformat(timespec='seconds'), ticker, name, side, kind, int(qty), ord_dvsn, float(price or 0)))
    oid = x.execute('SELECT last_insert_rowid()').fetchone()[0]
    x.commit()
    try:
        r = kc.order(side, ticker, qty, ord_dvsn, price)
        x.execute("UPDATE kis_orders SET status='접수', order_no=?, org_no=?, msg=? WHERE id=?", (r['order_no'], r['org_no'], r['msg'][:200], oid))
        x.commit()
        log(f"{'🟢' if side == 'buy' else '🔵'} {KIND.get(kind, kind)} 주문 {name} {qty}주 {'시장가' if ord_dvsn == '01' else f'지정가 {price:,.0f}'}")
        return oid
    except KISError as e:
        msg = str(e)
        x.execute("UPDATE kis_orders SET status=?, msg=? WHERE id=?", ('불분명' if 'AMBIGUOUS' in msg else '거절', msg[:200], oid))
        x.commit()
        if 'AMBIGUOUS' in msg:
            halt(f'{name} {KIND.get(kind, kind)} 주문 결과 불분명 — KIS 앱에서 체결 여부를 확인한 뒤 정지 해제')
        else:
            log(f'주문 거절 {name} {KIND.get(kind, kind)}: {msg}', 'warn')
        return None


def cancel_open(kc, ticker, kinds=('tp',)):
    x = c()
    for o in [dict(r) for r in x.execute(f"SELECT * FROM kis_orders WHERE date=? AND ticker=? AND status IN ('접수','부분') AND kind IN ({','.join('?' * len(kinds))})",
                                          (today(), ticker, *kinds))]:
        try:
            kc.cancel(o['order_no'], o['org_no'])
            x.execute("UPDATE kis_orders SET status='취소요청' WHERE id=?", (o['id'],))
        except KISError as e:
            log(f"취소 실패 {o['name']}: {e}", 'warn')
    x.commit()


def sync(kc, d=None):
    """그날 체결 내역 → 주문 · 보유 반영 (증가분만, 두 번 반영 안 함)"""
    d = d or today()
    x = c()
    fills = {f['order_no']: f for f in kc.fills(d) if f['order_no']}
    for o in [dict(r) for r in x.execute("SELECT * FROM kis_orders WHERE date=? AND order_no IS NOT NULL AND order_no!='' AND status NOT IN ('체결','취소','거절')", (d,))]:
        f = fills.get(o['order_no'])
        if not f:
            continue
        filled, avg = f['filled'], f['avg']
        st = '체결' if filled >= o['qty'] else ('취소' if f['cancelled'] or (f['remain'] == 0 and filled < o['qty']) else ('부분' if filled else o['status']))
        x.execute('UPDATE kis_orders SET filled=?, avg=?, status=? WHERE id=?', (filled, avg, st, o['id']))
        delta = filled - (o['applied'] or 0)
        if delta > 0:
            _apply(x, o, filled, avg, d)
            x.execute('UPDATE kis_orders SET applied=? WHERE id=?', (filled, o['id']))
    x.commit()


def _apply(x, o, filled, avg, d):
    p = x.execute('SELECT * FROM kis_pos WHERE ticker=?', (o['ticker'],)).fetchone()
    if o['side'] == 'buy':
        if not p:
            sig = x.execute("SELECT signal_date, sector FROM orders WHERE model='H' AND ticker=? ORDER BY signal_date DESC LIMIT 1", (o['ticker'],)).fetchone()
            x.execute("""INSERT INTO kis_pos (ticker, name, sector, signal_date, entry_date, entry_px, qty_total, qty, cost, status, updated)
                         VALUES (?,?,?,?,?,?,?,?,?,'보유',?)""", (o['ticker'], o['name'], sig[1] if sig else '', sig[0] if sig else '', d, avg, filled, filled,
                                                               filled * avg, now().isoformat(timespec='seconds')))
        else:
            sold = p['qty_total'] - p['qty']
            x.execute('UPDATE kis_pos SET entry_px=?, qty_total=?, qty=?, cost=? WHERE ticker=?', (avg, filled, filled - sold, filled * avg, o['ticker']))
        log(f"✅ 매수 체결 {o['name']} {filled}주 @ {avg:,.0f}")
        return
    if not p:
        log(f"보유 기록 없는 매도 체결 {o['name']} {filled}주 — 확인 필요", 'warn')
        return
    p = dict(p)
    sells = x.execute("SELECT COALESCE(SUM(CASE WHEN id=? THEN ? ELSE filled END),0), COALESCE(SUM(CASE WHEN id=? THEN ?*? ELSE filled*avg END),0) "
                      "FROM kis_orders WHERE ticker=? AND side='sell' AND date>=? AND filled>0 OR id=?",
                      (o['id'], filled, o['id'], filled, avg, o['ticker'], p['entry_date'], o['id'])).fetchone()
    sold_qty, proceeds = int(sells[0]), float(sells[1])
    left = max(0, p['qty_total'] - sold_qty)
    upd = {'qty': left, 'realized': proceeds}
    if o['kind'] == 'tp':
        upd.update(part_done=1, part_px=avg, peak=max(p['peak'] or 0, avg))
    x.execute(f"UPDATE kis_pos SET {', '.join(k + '=?' for k in upd)}, updated=? WHERE ticker=?", (*upd.values(), now().isoformat(timespec='seconds'), o['ticker']))
    log(f"✅ {KIND.get(o['kind'], o['kind'])} 체결 {o['name']} {filled}주 @ {avg:,.0f}")
    if left == 0:
        fee = p['cost'] * eng.COST / 100
        pnl = proceeds - p['cost'] - fee
        x.execute("""INSERT INTO kis_closed (ticker, name, signal_date, entry_date, entry_px, qty_total, exit_date, cost, proceeds, pnl, ret, reason, days)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""", (p['ticker'], p['name'], p['signal_date'], p['entry_date'], p['entry_px'], p['qty_total'], d,
                                                              p['cost'], proceeds, pnl, pnl / p['cost'] * 100 if p['cost'] else 0, KIND.get(o['kind'], o['kind']), p['days']))
        x.execute('DELETE FROM kis_pos WHERE ticker=?', (p['ticker'],))
        log(f"{'💰' if pnl > 0 else '🔴'} 청산 {p['name']} {pnl / p['cost'] * 100:+.2f}% ({KIND.get(o['kind'], o['kind'])})")


# ════════════════════════════════════════════
#  하루 작업
# ════════════════════════════════════════════
def preopen(cfg, kc, d):
    """08:35 — 추적 매도 · 신규 매수 (장전 시장가 → 시가 체결)"""
    x = c()
    bal = kc.balance()
    mine = {r[0] for r in x.execute('SELECT ticker FROM kis_pos WHERE qty>0')}
    unknown = [p for p in bal['positions'] if p['ticker'] not in mine]
    if unknown:
        db.meta_set('kis_block_new', ' · '.join(f"{p['name']} {p['qty']}주" for p in unknown)[:200])
        log(f"모르는 보유 종목 {len(unknown)}개 → 신규 매수 차단 (전용 계좌인지 확인): " + ' · '.join(p['name'] for p in unknown[:5]), 'warn')
        alert("H1 모의투자 신규 매수 차단 — 앱이 모르는 보유 종목: " + ' · '.join(f"{p['name']} {p['qty']}주" for p in unknown[:5]) + "\n모의계좌를 다른 프로그램과 같이 쓰는지 확인하세요", 'block')
    else:
        db.meta_set('kis_block_new', '')
    for p in [dict(r) for r in x.execute('SELECT * FROM kis_pos WHERE qty>0 AND sell_next=1')]:
        send(cfg, kc, 'sell', 'trail', p['ticker'], p['name'], p['qty'])
        if halted():
            return
    if unknown:
        return
    last = max((r[0] for r in x.execute('SELECT date FROM days')), default='')
    if not last or last != prev_trading_day(d):
        log(f'우량주 앱 신호가 전 거래일({prev_trading_day(d)}) 것이 아님(마지막 처리 {last}) → 오늘 신규 매수 안 함', 'warn')
        return
    if db.meta_get(f'kis_plan_used_{last}') == '1':
        log(f'{last} 신호는 이미 주문함 → 다시 사지 않음', 'warn')
        return
    db.meta_set(f'kis_plan_used_{last}', '1')
    if cfg.get('kis_buy_pause'):
        log('신규 매수 일시 중지 중 (매도 · 손절은 계속) → 오늘 매수 안 함', 'warn')
        return
    for p in build_plan(cfg, bal['equity'], min(bal['cash_d2'] or bal['cash'], bal['cash'] or bal['cash_d2']), last, d):
        if p['skip']:
            log(f"{p['name']} 건너뜀 — {p['skip']}")
            continue
        send(cfg, kc, 'buy', 'entry', p['ticker'], p['name'], p['qty'])
        if halted():
            return


def build_plan(cfg, equity, cash, last, d=None):
    """H 신호 → 주문 수량 (장전 주문 · 화면 미리보기 공용). skip = 안 사는 이유"""
    x = c()
    plan = [dict(r) for r in x.execute("SELECT * FROM orders WHERE model='H' AND signal_date=? AND status IN ('대기','체결','안 삼') ORDER BY prio", (last,))]
    cap = _cap(cfg)
    base = min(equity or cap, cap)
    size = base / SLOTS
    cash = cash if cash else base
    day_cap, one_cap, spent = base * 0.40, base * 0.15, 0.0
    held = {r[0]: r[1] for r in x.execute('SELECT ticker, sector FROM kis_pos WHERE qty>0')}
    secn = {}
    for s_ in held.values():
        secn[s_] = secn.get(s_, 0) + 1
    out = []
    for o in plan:
        ref = x.execute('SELECT close FROM daily WHERE date=? AND ticker=?', (last, o['ticker'])).fetchone()
        ref = float(ref[0]) if ref and ref[0] else 0
        r = {'ticker': o['ticker'], 'name': o['name'], 'sector': o['sector'], 'ref': ref, 'ma20gap': o['ma20gap'], 'qty': 0, 'amt': 0, 'skip': ''}
        if len(held) >= MAXPOS:
            r['skip'] = f'보유 {MAXPOS}종목 한도'
        elif o['ticker'] in held:
            r['skip'] = '이미 보유'
        elif secn.get(o['sector'], 0) >= SECCAP:
            r['skip'] = f'업종 {SECCAP}종목 한도'
        elif d and x.execute("SELECT 1 FROM kis_orders WHERE date=? AND ticker=? AND kind='entry' AND status NOT LIKE '거절%'", (d, o['ticker'])).fetchone():
            r['skip'] = '오늘 이미 주문'
        elif ref <= 0:
            r['skip'] = '기준 가격 없음'
        else:
            amt = min(size, one_cap, cash / 1.03, day_cap - spent)
            qty = int(amt // (ref * 1.02))             # 시가 갭 여유 2%
            if qty <= 0 and ref * 1.02 <= min(size, cash / 1.03, day_cap - spent):
                qty = 1
            if qty <= 0:
                r['skip'] = f'금액 부족 · 한도 (1주 {ref:,.0f} > 종목당 {size:,.0f})' if ref > size else '현금 · 하루 한도 부족'
            else:
                r.update(qty=qty, amt=qty * ref)
                spent += qty * ref
                cash -= qty * ref * 1.02
                held[o['ticker']] = o['sector']
                secn[o['sector']] = secn.get(o['sector'], 0) + 1
        out.append(r)
    return out


def retry_rejected(cfg, kc, d):
    """09:01 — 장전 시간 때문에 거절된 주문을 장중 시장가로 한 번 더"""
    x = c()
    for o in [dict(r) for r in x.execute("SELECT * FROM kis_orders WHERE date=? AND status='거절' AND kind IN ('entry','trail') AND ord_dvsn='01'", (d,))]:
        m = o['msg'] or ''
        if any(k in m for k in ('시간', '장개시', '장시작', '장운영', '동시호가')):
            x.execute("UPDATE kis_orders SET status='거절(재시도)' WHERE id=?", (o['id'],))
            x.commit()
            send(cfg, kc, o['side'], o['kind'], o['ticker'], o['name'], o['qty'])
            if halted():
                return


def place_tp(cfg, kc, d):
    """09:03 — 1차 익절 전 종목에 +5% 지정가 (보유의 30%, 3주 이하는 전량) · 지정가는 당일만 유효라 매일"""
    x = c()
    for p in [dict(r) for r in x.execute("SELECT * FROM kis_pos WHERE qty>0 AND part_done=0 AND status='보유' AND sell_next=0")]:
        if x.execute("SELECT 1 FROM kis_orders WHERE date=? AND ticker=? AND kind='tp' AND status IN ('보냄','접수','부분','체결')", (d, p['ticker'])).fetchone():
            continue
        n1 = int(p['qty'] * PART)
        q = n1 if 0 < n1 < p['qty'] else p['qty']
        send(cfg, kc, 'sell', 'tp', p['ticker'], p['name'], q, '00', tick_up(p['entry_px'] * (1 + eng.TP / 100)))
        if halted():
            return


def monitor(cfg, kc, d):
    """장중 30초 — −15% 재난 손절"""
    x = c()
    for p in [dict(r) for r in x.execute("SELECT * FROM kis_pos WHERE qty>0 AND status='보유'")]:
        try:
            px, _ = kc.price(p['ticker'])
        except KISError as e:
            STATE['last_err'] = str(e)[:150]
            continue
        if px <= 0:
            continue
        x.execute('UPDATE kis_pos SET last_px=? WHERE ticker=?', (px, p['ticker']))
        if px <= p['entry_px'] * (1 - STOP / 100) and can_order(cfg):
            cancel_open(kc, p['ticker'], ('tp',))
            if send(cfg, kc, 'sell', 'stop', p['ticker'], p['name'], p['qty']):
                x.execute("UPDATE kis_pos SET status='손절 주문' WHERE ticker=?", (p['ticker'],))
            if halted():
                break
    x.commit()
    STATE['last_monitor'] = now().strftime('%H:%M:%S')


def time_exit(cfg, kc, d):
    """15:20 — 40거래일째 장마감 시장가"""
    x = c()
    for p in [dict(r) for r in x.execute("SELECT * FROM kis_pos WHERE qty>0 AND status='보유' AND days+1>=?", (HOLD,))]:
        cancel_open(kc, p['ticker'], ('tp',))
        send(cfg, kc, 'sell', 'time', p['ticker'], p['name'], p['qty'])
        if halted():
            return


def eod(cfg, kc, d):
    """15:45 — 체결 반영 · 보유일 · 종가 최고가 · 추적 매도 표시 · 잔고 대조 · 평가 기록"""
    x = c()
    sync(kc, d)
    bal = kc.balance()
    kpos = {p['ticker']: p for p in bal['positions']}
    bad = []
    for p in [dict(r) for r in x.execute('SELECT * FROM kis_pos WHERE qty>0')]:
        k = kpos.get(p['ticker'])
        cl = (k or {}).get('price') or p['last_px'] or p['entry_px']
        if not k:
            log(f"{p['name']} — 앱 기록엔 {p['qty']}주인데 KIS 잔고에 없음 (확인 필요)", 'warn')
            bad.append(f"{p['name']} 앱 {p['qty']}주 · KIS 없음")
        elif k['qty'] != p['qty']:
            log(f"{p['name']} — 앱 {p['qty']}주 vs KIS {k['qty']}주 불일치 (체결 반영 지연 가능 · 확인 필요)", 'warn')
            bad.append(f"{p['name']} 앱 {p['qty']}주 · KIS {k['qty']}주")
        peak, sell_next = p['peak'] or 0, 0
        if p['part_done']:
            peak = max(peak, cl)
            sell_next = int(cl <= peak * (1 - TRAIL / 100))
        st = '보유' if p['status'] == '보유' else p['status']
        x.execute('UPDATE kis_pos SET days=days+1, last_px=?, peak=?, sell_next=?, status=? WHERE ticker=?', (cl, peak, sell_next, st, p['ticker']))
        if sell_next:
            log(f"추적 매도 표시 {p['name']} (종가 {cl:,.0f} ≤ 최고 {peak:,.0f} × 0.96) → 내일 장전 시장가")
    x.execute('INSERT OR REPLACE INTO kis_equity VALUES (?,?,?,?)', (d, bal['cash'], bal['equity'], len(bal['positions'])))
    x.commit()
    if bad:
        alert('H1 모의투자 장 마감 잔고 불일치 — ' + ' / '.join(bad[:5]), 'eodmismatch')


def precheck(cfg, kc, d):
    """08:20 장전 점검 (B0.6) — 주문 없음. KIS 연결 · 잔고 · 오늘 매수 계획을 미리 확인해 문제가 있으면 08:35 전에 알림"""
    probs = []
    try:
        bal = kc.balance()
        mine = {r[0] for r in c().execute('SELECT ticker FROM kis_pos WHERE qty>0')}
        unknown = [p['name'] for p in bal['positions'] if p['ticker'] not in mine]
        if unknown:
            probs.append('앱이 모르는 보유 종목: ' + ' · '.join(unknown[:5]) + ' → 08:35 신규 매수 차단 예정')
        info = f"예수금 {bal['cash']:,.0f} · 평가 {bal['equity']:,.0f} · 보유 {len(bal['positions'])}"
    except Exception as e:
        probs.append(f'KIS 연결 실패: {str(e)[:150]}')
        info = ''
    pv = preview(cfg)
    if pv.get('stale') and pv.get('buys'):
        probs.append(f"매수 신호가 전 거래일 것이 아님({pv['signal_date']}) → 오늘 신규 매수 안 함 (우량주 계산 확인)")
    if halted():
        probs.append(f'자동주문 정지 상태: {halted()}')
    buys = [b['name'] for b in pv.get('buys', []) if not b.get('skip')]
    if probs:
        log('장전 점검 — ' + ' / '.join(probs), 'warn')
        alert('H1 모의투자 장전 점검(08:20) 문제 — ' + ' / '.join(probs), 'precheck')
    else:
        log(f"장전 점검 OK — {info} · 08:35 매수 예정 {len(buys)}종목 {', '.join(buys)}".rstrip())


# ════════════════════════════════════════════
#  일정 · 화면용
# ════════════════════════════════════════════
def loop(get_cfg):
    time.sleep(15)
    last_mon, last_sync = 0.0, 0.0
    while True:
        try:
            cfg = get_cfg()
            d, hm = today(), now().strftime('%H:%M')
            active = cfg.get('kis_app_key') and cfg.get('kis_account') and (cfg.get('kis_on') or c().execute('SELECT COUNT(*) FROM kis_pos').fetchone()[0])
            if active and cfg.get('kis_on') and is_trading_day(d) and '08:20' <= hm < '08:30' and not _done('check', d):
                _mark('check', d)
                with _lock:
                    precheck(cfg, client(cfg), d)
            if active and is_trading_day(d) and '08:30' <= hm <= '16:30':
                kc = client(cfg)
                with _lock:
                    STATE['running'] = True
                    if '08:35' <= hm <= '08:58' and not _done('pre', d) and can_order(cfg):
                        _mark('pre', d)
                        preopen(cfg, kc, d)
                    if '09:01' <= hm <= '09:15' and not _done('retry', d) and can_order(cfg):
                        _mark('retry', d)
                        retry_rejected(cfg, kc, d)
                    if '09:02' <= hm <= '15:19' and time.time() - last_sync > 180:
                        last_sync = time.time()
                        sync(kc, d)
                        if hm >= '09:03' and hm <= '15:00' and can_order(cfg):
                            place_tp(cfg, kc, d)                      # 늦게 반영된 매수 체결도 익절 주문
                        x = c()
                        if x.execute("SELECT COUNT(*) FROM kis_orders WHERE date=? AND status IN ('접수','부분','체결')", (d,)).fetchone()[0] == 0 and \
                                x.execute("SELECT COUNT(*) FROM kis_orders WHERE date=? AND status LIKE '거절%'", (d,)).fetchone()[0] >= 2 and \
                                all(any(k in (m[0] or '') for k in ('장운영', '휴장', '영업일')) for m in x.execute("SELECT msg FROM kis_orders WHERE date=? AND status LIKE '거절%'", (d,))):
                            db.meta_set(f'kis_closed_{d}', '1')
                            log(f'{d} 휴장으로 판단 — 오늘 모의투자 쉼', 'warn')
                    if '09:05' <= hm <= '15:18' and time.time() - last_mon > 30:
                        last_mon = time.time()
                        monitor(cfg, kc, d)
                    if '15:20' <= hm <= '15:27' and not _done('time', d) and can_order(cfg):
                        _mark('time', d)
                        time_exit(cfg, kc, d)
                    if '15:45' <= hm <= '16:30' and not _done('eod', d):
                        _mark('eod', d)
                        eod(cfg, kc, d)
                    STATE['running'] = False
        except Exception as e:
            STATE['running'] = False
            STATE['last_err'] = f'{now():%H:%M:%S} {str(e)[:200]}'
            try:
                log(f'일정 오류: {str(e)[:200]}', 'error')
                alert(f'H1 모의투자 일정 오류 — {str(e)[:200]}', 'looperr')
            except Exception:
                pass
            time.sleep(50)
        time.sleep(10)


def compare():
    """같은 신호 · 같은 날 매수의 가상 H ↔ KIS 모의 비교"""
    x = c()
    rows = []
    for k in [dict(r) for r in x.execute('SELECT * FROM kis_closed ORDER BY id DESC LIMIT 200')] + \
             [dict(r, exit_date=None, ret=None, reason='보유 중') for r in x.execute('SELECT * FROM kis_pos')]:
        v = x.execute("SELECT * FROM trades WHERE model='H' AND ticker=? AND entry_date=?", (k['ticker'], k['entry_date'])).fetchone()
        v = dict(v) if v else {}
        rows.append({'ticker': k['ticker'], 'name': k['name'], 'entry_date': k['entry_date'], 'kis_entry': k['entry_px'], 'v_entry': v.get('entry_px'),
                     'entry_diff': (k['entry_px'] / v['entry_px'] - 1) * 100 if v.get('entry_px') else None,
                     'kis_ret': k.get('ret'), 'v_ret': v.get('ret'), 'kis_exit': k.get('exit_date'), 'v_exit': v.get('exit_date'),
                     'kis_reason': k.get('reason'), 'v_reason': v.get('exit_reason') or v.get('status')})
    d = [r['entry_diff'] for r in rows if r['entry_diff'] is not None]
    rr = [r['kis_ret'] - r['v_ret'] for r in rows if r['kis_ret'] is not None and r['v_ret'] is not None]
    return {'rows': rows, 'entry_diff_avg': sum(d) / len(d) if d else None, 'n_entry': len(d),
            'ret_diff_avg': sum(rr) / len(rr) if rr else None, 'n_ret': len(rr)}


def phase(d=None, hm=None):
    d, hm = d or today(), hm or now().strftime('%H:%M')
    if not is_trading_day(d):
        return '휴장'
    if hm < '08:30':
        return '장 시작 전'
    if hm < '09:00':
        return '장전 동시호가'
    if hm < '15:20':
        return '장중'
    if hm < '15:30':
        return '장마감 동시호가'
    return '장 마감 후'


def schedule(cfg):
    d = today()
    td = is_trading_day(d)
    items = [('08:20', '장전 점검 (KIS 연결 · 잔고 · 매수 계획 · 문제 시 텔레그램)', 'check'), ('08:35', '장전 시장가 매수 · 추적 매도', 'pre'), ('09:01', '장전 거절 주문 재시도', 'retry'),
             ('09:03~15:00', '체결 반영(3분) · +5% 익절 주문', None), ('09:05~15:18', '−15% 재난 손절 감시(30초)', None),
             ('15:20', '40일 만료 장마감 매도', 'time'), ('15:45', '잔고 대조 · 추적 매도 표시 · 평가 기록', 'eod')]
    hm = now().strftime('%H:%M')
    out = []
    for t, what, job in items:
        end = t.split('~')[-1]
        if not td:
            st = '휴장'
        elif (job and _done(job, d)) or (not job and hm > end and cfg.get('kis_on')):
            st = '완료'
        elif t.split('~')[0] <= hm <= end:
            st = '진행 중' if cfg.get('kis_on') or job == 'eod' else '꺼짐'
        elif hm > end:
            st = '지남'
        else:
            st = '대기' if cfg.get('kis_on') or job == 'eod' else '꺼짐'
        out.append({'time': t, 'what': what, 'state': st})
    return out


def preview(cfg):
    """다음 장전 주문 미리보기 — KIS 호출 없음 (마지막 평가 기록 기준)"""
    x = c()
    last = max((r[0] for r in x.execute('SELECT date FROM days')), default='')
    eq = x.execute('SELECT * FROM kis_equity ORDER BY date DESC LIMIT 1').fetchone()
    equity, cash = (eq['value'], eq['cash']) if eq else (_cap(cfg), _cap(cfg))
    buys = build_plan(cfg, equity, cash, last) if last else []
    sells = [{'name': p['name'], 'ticker': p['ticker'], 'qty': p['qty'], 'why': '추적 매도 (고점 −4%) → 장전 시장가'}
             for p in x.execute('SELECT * FROM kis_pos WHERE qty>0 AND sell_next=1')]
    sells += [{'name': p['name'], 'ticker': p['ticker'], 'qty': p['qty'], 'why': f"40일 만료 ({p['days']}일째) → 15:20 장마감"}
              for p in x.execute('SELECT * FROM kis_pos WHERE qty>0 AND days+1>=? AND sell_next=0', (HOLD,))]
    nxt = today() if (is_trading_day() and now().strftime('%H:%M') < '08:35') else ''
    if not nxt:
        from datetime import timedelta
        t = now()
        for _ in range(15):
            t += timedelta(days=1)
            if is_trading_day(t.strftime('%Y%m%d')):
                nxt = t.strftime('%Y%m%d')
                break
    used = db.meta_get(f'kis_plan_used_{last}') == '1'
    stale = bool(nxt) and last != prev_trading_day(nxt)
    return {'signal_date': last, 'for_date': nxt, 'buys': buys, 'sells': sells, 'used': used, 'stale': stale,
            'basis': '마지막 KIS 평가 기록' if eq else '운용 한도 (평가 기록 없음)', 'equity': equity, 'cash': cash}


_live = {'t': 0.0, 'data': None}


def live(cfg, force=False):
    """KIS 실시간 잔고 · 오늘 체결 (15초 캐시)"""
    if not force and _live['data'] and time.time() - _live['t'] < 15:
        return _live['data']
    kc = client(cfg)
    bal = kc.balance()
    fills = kc.fills(today()) if is_trading_day() else []
    x = c()
    mine = {r['ticker']: dict(r) for r in x.execute('SELECT * FROM kis_pos')}
    for p in bal['positions']:
        m = mine.get(p['ticker'])
        p['mine'] = bool(m)
        if m:
            x.execute('UPDATE kis_pos SET last_px=? WHERE ticker=?', (p['price'], p['ticker']))
    x.commit()
    names = {r[0]: r[1] for r in x.execute('SELECT ticker, name FROM kis_orders')}
    for f in fills:
        f['name'] = names.get(f['ticker'], f['ticker'])
    data = {'ok': True, 'at': now().strftime('%H:%M:%S'), 'cash': bal['cash'], 'cash_d2': bal['cash_d2'], 'equity': bal['equity'],
            'positions': bal['positions'], 'fills': fills, 'account': kc.masked_account}
    _live.update(t=time.time(), data=data)
    return data


def manual_sell(cfg, ticker):
    x = c()
    p = x.execute('SELECT * FROM kis_pos WHERE ticker=? AND qty>0', (ticker,)).fetchone()
    if not p:
        raise KISError('앱 보유 기록이 없는 종목')
    kc = client(cfg)
    cancel_open(kc, ticker, ('tp',))
    oid = send(cfg, kc, 'sell', 'manual', ticker, p['name'], p['qty'])
    if not oid:
        raise KISError('주문 실패 — 로그 확인')
    x.execute("UPDATE kis_pos SET status='수동 매도 주문' WHERE ticker=?", (ticker,))
    x.commit()
    log(f"수동 전량 매도 요청 {p['name']} {p['qty']}주 (사용자)")
    return oid


def cancel_order(cfg, oid):
    x = c()
    o = x.execute('SELECT * FROM kis_orders WHERE id=?', (oid,)).fetchone()
    if not o or o['status'] not in ('접수', '부분') or not o['order_no']:
        raise KISError('취소할 수 있는 미체결 주문이 아님')
    client(cfg).cancel(o['order_no'], o['org_no'])
    x.execute("UPDATE kis_orders SET status='취소요청' WHERE id=?", (oid,))
    x.commit()
    log(f"주문 취소 요청 {o['name']} {KIND.get(o['kind'], o['kind'])} (사용자)")


def perf():
    x = c()
    eq = [dict(r) for r in x.execute('SELECT * FROM kis_equity ORDER BY date')]
    curves = {'KIS': [[r['date'], r['value']] for r in eq]}
    start = eq[0]['date'] if eq else ''
    for m in ('H', 'A'):
        v = [[r[0], r[1]] for r in x.execute('SELECT date, value FROM equity WHERE model=? AND date>=? ORDER BY date', (m, start or '99999999'))]
        curves[m] = v
    closed = [dict(r) for r in x.execute('SELECT * FROM kis_closed ORDER BY id')]
    by_reason, by_month = {}, {}
    for r in closed:
        a = by_reason.setdefault(r['reason'], {'n': 0, 'sum': 0.0, 'pnl': 0.0})
        a['n'] += 1; a['sum'] += r['ret']; a['pnl'] += r['pnl']
        b = by_month.setdefault((r['exit_date'] or '')[:6], {'n': 0, 'pnl': 0.0, 'win': 0})
        b['n'] += 1; b['pnl'] += r['pnl']; b['win'] += r['ret'] > 0
    peak, mdd = 0.0, 0.0
    for r in eq:
        peak = max(peak, r['value'])
        mdd = min(mdd, r['value'] / peak - 1) if peak else mdd
    return {'curves': curves, 'mdd': mdd * 100, 'start': start,
            'by_reason': [{'reason': k, 'n': v['n'], 'avg': v['sum'] / v['n'], 'pnl': v['pnl']} for k, v in by_reason.items()],
            'by_month': [{'month': k, **v} for k, v in sorted(by_month.items())]}


def criteria(cfg):
    """사전 등록 3개월 판정 기준 진행"""
    x = c()
    cmp_ = compare()
    closed = [r[0] for r in x.execute('SELECT ret FROM kis_closed')]
    incidents = x.execute("SELECT COUNT(*) FROM kis_orders WHERE status='불분명'").fetchone()[0] + \
        x.execute("SELECT COUNT(*) FROM kis_log WHERE level='error' AND msg LIKE '%정지%'").fetchone()[0]
    eq = [dict(r) for r in x.execute('SELECT * FROM kis_equity ORDER BY date')]
    p = perf()
    kret = (eq[-1]['value'] / eq[0]['value'] - 1) * 100 if len(eq) > 1 else None
    a = [r[0] for r in x.execute('SELECT value FROM equity WHERE model=? AND date>=? ORDER BY date', ('A', eq[0]['date'] if eq else '99999999'))]
    aret = (a[-1] / a[0] - 1) * 100 if len(a) > 1 else None
    orders = x.execute("SELECT COUNT(*), SUM(status IN ('취소','거절','거절(재시도)') AND kind='entry') FROM kis_orders WHERE kind='entry'").fetchone()
    miss = (orders[1] or 0) / orders[0] * 100 if orders[0] else None
    days = len(eq)
    avg = sum(closed) / len(closed) if closed else None
    d = cmp_['entry_diff_avg']
    return [
        {'k': '기간', 'v': f'{days}거래일 / 약 60', 'ok': days >= 60, 'prog': min(1, days / 60)},
        {'k': '주문 사고 0', 'v': f'{incidents}건', 'ok': incidents == 0, 'prog': 1 if incidents == 0 else 0},
        {'k': '체결 차이 ±0.3%', 'v': '-' if d is None else f'{d:+.2f}% ({cmp_["n_entry"]}건)', 'ok': d is not None and abs(d) <= 0.3, 'prog': None},
        {'k': '미체결 · 거절 5% 이하', 'v': '-' if miss is None else f'{miss:.1f}%', 'ok': miss is not None and miss <= 5, 'prog': None},
        {'k': '청산 20건+', 'v': f'{len(closed)} / 20', 'ok': len(closed) >= 20, 'prog': min(1, len(closed) / 20)},
        {'k': '건당 > 0', 'v': '-' if avg is None else f'{avg:+.2f}%', 'ok': avg is not None and avg > 0, 'prog': None},
        {'k': '가상 A 이상', 'v': '-' if kret is None or aret is None else f'모의 {kret:+.2f}% vs A {aret:+.2f}%', 'ok': kret is not None and aret is not None and kret >= aret, 'prog': None},
        {'k': '최대 낙폭 −20% 이내', 'v': f"{p['mdd']:.1f}%", 'ok': p['mdd'] >= -20, 'prog': None}]


def dash(cfg):
    """대시보드 카드용 요약 — KIS 호출 없음"""
    x = c()
    eq = [dict(r) for r in x.execute('SELECT * FROM kis_equity ORDER BY date')]
    cap = _cap(cfg)
    pos = [dict(r) for r in x.execute('SELECT * FROM kis_pos')]
    closed = [r[0] for r in x.execute('SELECT ret FROM kis_closed')]
    t = today()
    o = x.execute("SELECT COUNT(*), SUM(filled>0) FROM kis_orders WHERE date=?", (t,)).fetchone()
    last = eq[-1] if eq else None
    base = eq[0]['value'] if eq else None
    unreal = sum(((p['last_px'] or p['entry_px']) * p['qty'] + (p['realized'] or 0) - p['cost']) for p in pos)
    return {'on': bool(cfg.get('kis_on')), 'configured': bool(cfg.get('kis_app_key') and cfg.get('kis_account')), 'halt': halted(),
            'pause': bool(cfg.get('kis_buy_pause')), 'phase': phase(), 'cap': cap, 'value': last['value'] if last else None,
            'cash': last['cash'] if last else None, 'ret': (last['value'] / base - 1) * 100 if last and base else None,
            'npos': len(pos), 'unreal': unreal, 'closed': len(closed), 'win': sum(v > 0 for v in closed) / len(closed) * 100 if closed else None,
            'avg': sum(closed) / len(closed) if closed else None, 'orders_today': o[0] or 0, 'filled_today': o[1] or 0,
            'curve': [[r['date'], r['value']] for r in eq], 'block_new': db.meta_get('kis_block_new', ''),
            'next': next((s['time'] + ' ' + s['what'] for s in schedule(cfg) if s['state'] in ('대기', '진행 중')), '')}


def status(cfg):
    x = c()
    pos = [dict(r) for r in x.execute('SELECT * FROM kis_pos ORDER BY entry_date')]
    for p in pos:
        lp = p['last_px'] or p['entry_px']
        p['eval'] = ((p['realized'] or 0) + p['qty'] * lp - p['cost']) / p['cost'] * 100 if p['cost'] else None
        p['eval_won'] = (p['realized'] or 0) + p['qty'] * lp - p['cost']
        p['stop_px'] = p['entry_px'] * (1 - STOP / 100)
        p['tp_px'] = tick_up(p['entry_px'] * (1 + eng.TP / 100))
        p['trail_px'] = p['peak'] * (1 - TRAIL / 100) if p['part_done'] and p['peak'] else None
        p['open_orders'] = [dict(r) for r in x.execute("SELECT id, kind, qty, price, status FROM kis_orders WHERE ticker=? AND date=? AND status IN ('접수','부분')", (p['ticker'], today()))]
    closed = [dict(r) for r in x.execute('SELECT * FROM kis_closed ORDER BY id DESC LIMIT 300')]
    rets = [r['ret'] for r in closed]
    return {'on': bool(cfg.get('kis_on')), 'cap': _cap(cfg), 'halt': halted(), 'block_new': db.meta_get('kis_block_new', ''),
            'pause': bool(cfg.get('kis_buy_pause')), 'phase': phase(), 'schedule': schedule(cfg), 'preview': preview(cfg),
            'configured': bool(cfg.get('kis_app_key') and cfg.get('kis_app_secret') and cfg.get('kis_account')),
            'positions': pos, 'orders': [dict(r) for r in x.execute('SELECT * FROM kis_orders ORDER BY id DESC LIMIT 200')],
            'open_orders': [dict(r) for r in x.execute("SELECT * FROM kis_orders WHERE date=? AND status IN ('보냄','접수','부분','취소요청') ORDER BY id DESC", (today(),))],
            'closed': closed, 'equity': [dict(r) for r in x.execute('SELECT * FROM kis_equity ORDER BY date')],
            'log': [dict(r) for r in x.execute('SELECT * FROM kis_log ORDER BY id DESC LIMIT 200')],
            'stats': {'n': len(rets), 'win': sum(1 for v in rets if v > 0) / len(rets) * 100 if rets else None, 'avg': sum(rets) / len(rets) if rets else None,
                      'pnl': sum(r['pnl'] for r in closed), 'best': max(rets) if rets else None, 'worst': min(rets) if rets else None},
            'state': dict(STATE), 'compare': compare(), 'trading_day': is_trading_day(), 'perf': perf(), 'criteria': criteria(cfg), 'dash': dash(cfg)}


def day_lines(d):
    """텔레그램 마감 리포트의 H1 모의투자 부분 (B0.6 — 체결 건별 · 오늘 손익 · 보유 평가 · 가상 H 대비)"""
    x = c()
    o = x.execute("SELECT COUNT(*), SUM(side='buy' AND filled>0), SUM(side='sell' AND filled>0) FROM kis_orders WHERE date=?", (d,)).fetchone()
    fills = [dict(r) for r in x.execute("SELECT * FROM kis_orders WHERE date=? AND filled>0 ORDER BY ts, id", (d,))]
    cl = [dict(r) for r in x.execute('SELECT * FROM kis_closed WHERE exit_date=?', (d,))]
    eqs = [dict(r) for r in x.execute('SELECT * FROM kis_equity WHERE date<=? ORDER BY date DESC LIMIT 2', (d,))]
    eq = eqs[0] if eqs and eqs[0]['date'] == d else None
    pos = [dict(r) for r in x.execute('SELECT * FROM kis_pos ORDER BY entry_date')]
    if not (o[0] or pos or eq):
        return []
    L = ['🧾 H1 모의투자 (KIS 모의계좌 · 실제 주문)']
    if eq:
        base = x.execute('SELECT value FROM kis_equity ORDER BY date LIMIT 1').fetchone()[0]
        prev = eqs[1]['value'] if len(eqs) > 1 else base
        L.append(f" 평가 {eq['value']:,.0f}원 · 오늘 {eq['value'] - prev:+,.0f}원 ({(eq['value'] / prev - 1) * 100:+.2f}%) · 시작 대비 {(eq['value'] / base - 1) * 100:+.2f}%")
        L.append(f" 현금 {eq['cash'] / 1e4:,.0f}만 · 보유 {len(pos)}종목 · 주문 {o[0] or 0} · 체결 매수 {o[1] or 0} / 매도 {o[2] or 0}")
    else:
        L.append(f" 주문 {o[0] or 0} · 체결 매수 {o[1] or 0} / 매도 {o[2] or 0} · 보유 {len(pos)}종목 (평가 기록 전)")
    buys = [f for f in fills if f['side'] == 'buy']
    if buys:
        L.append(' 🟢 매수 체결')
        vh = {r['ticker']: r['entry_px'] for r in db.conn().execute("SELECT ticker, entry_px FROM trades WHERE model='H' AND entry_date=?", (d,))}
        for f in buys:
            gap = f" · 가상 H {vh[f['ticker']]:,.0f} 대비 {(f['avg'] / vh[f['ticker']] - 1) * 100:+.2f}%" if f.get('avg') and vh.get(f['ticker']) else ''
            L.append(f"  {f['name']} {f['filled']}주 @{(f['avg'] or 0):,.0f}{gap}")
    sells = [f for f in fills if f['side'] == 'sell']
    if sells:
        L.append(' 🔴 매도 체결')
        for f in sells:
            L.append(f"  {f['name']} {f['filled']}주 @{(f['avg'] or 0):,.0f} · {KIND.get(f['kind'], f['kind'])}")
    if cl:
        L.append(f" 💵 청산 {len(cl)}건 실현 {sum(r['pnl'] for r in cl):+,.0f}원: " + ' · '.join(f"{r['name']} {r['ret']:+.2f}%" for r in cl))
    if pos:
        ev = []
        for p in pos:
            lp = p['last_px'] or p['entry_px']
            v = ((p['realized'] or 0) + p['qty'] * lp - p['cost'])
            ev.append((p, v, v / p['cost'] * 100 if p['cost'] else 0))
        L.append(f" 📦 보유 평가 {sum(v for _, v, _ in ev):+,.0f}원: " + ' · '.join(f"{p['name']} {r:+.2f}% ({p['days']}/{HOLD}일)" for p, _, r in ev))
    if halted():
        L.append(f' ⛔ 정지 중: {halted()}')
    if db.meta_get('kis_block_new', ''):
        L.append(f" ⚠️ 신규 매수 차단: {db.meta_get('kis_block_new', '')}")
    return L
