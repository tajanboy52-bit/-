"""
bluechip_broker.py — 🧾 KIS 모의투자 자동매매 (B1.0 · 모의계좌 하나를 모델별 칸으로 나눠 운용)

칸(모델): 💎 H1 우량주 반등(계좌 70%) · 🌙 밤사이 코스닥150 ETF(계좌 30%) · ⚡ S5 단기 눌림은 B1.1부터 기본 꺼짐(설정에서 켤 수 있음)
H1 규칙(아래)과 안전장치는 B0.6과 같음. S5 · 밤사이는 B1.0에서 추가 (검증: claude/1000만_자금효율_설계_후보검증.md)

규칙(가상 모델 H와 같음): 우량주 앱 A 신호 → 다음 날 장전 시장가 매수 · 체결 뒤 +5% 지정가로 30% 매도(매일 아침 다시 넣음)
· 나머지는 종가가 보유 중 최고 종가 −4%면 다음 날 장전 시장가 매도 · 장중 30초마다 −15% 재난 손절(시장가)
· 40거래일째 15:20 장마감 시장가 · 종목당 = min(KIS 평가액, 운용 한도) ÷ 10 · 최대 14종목 · 업종당 2
안전: 자동주문 기본 OFF · 주문 결과 불분명(네트워크) → 재주문 없이 정지(HALT) · KIS 잔고가 기준 · 모르는 보유 종목이 있으면 신규 매수 차단
· 우량주 앱 신호가 전 거래일 것이 아니면 매수 안 함 · 1회 · 하루 매수 금액 한도 · 비밀 값은 로그 · 화면 · zip에 안 남김
"""
import os
import re
import threading
import time
from datetime import datetime

import bluechip_db as db
import bluechip_engine as eng
from bluechip_kis import KISError, KISPaper, tick_up

# KRX 휴장일 (주말 제외) — 2027년은 공휴일 · 대체공휴일(python holidays 패키지 기준)로 넣음 · 연말 KRX 휴장일 공지로 다시 확인할 것 (B1.2)
# 목록에 없는 휴장일은 주문 거절 메시지(장운영 · 휴장)로 알아채고 그날 쉼 (loop)
HOLIDAYS = {'20261005', '20261009', '20261225', '20261231',
            '20270101', '20270208', '20270209', '20270301', '20270503', '20270505', '20270513', '20270719', '20270816',
            '20270914', '20270915', '20270916', '20271004', '20271011', '20271227', '20271231'}
PART, TRAIL, STOP, HOLD, MAXPOS, SECCAP, SLOTS = 0.30, 4.0, 15.0, 40, 14, 2, 10
S5_PCT, S5_MAX, S5_TH, S5_MAXD = 5.0, 6, -0.07, 7          # S5: 종목당 계좌 5% · 최대 6종목 · 5일 −7% · 최대 7일
ON_TICKER, ON_NAME = '229200', 'KODEX 코스닥150'           # 밤사이 칸 ETF (1배 · 매매차익 비과세)
COSTS = {'H1': 0.25, 'S5': 0.25, 'ON': 0.03, 'CORE': 0.25}  # 손익 계산용 왕복 비용 추정 % (주식: 수수료 · 세금 · 여유 / ETF: 수수료)
CORE_N, CORE_BUF, CORE_SEC = 15, 30, 4                     # 코어: 15종목 · 30위 안이면 계속 보유 · 업종당 4
STRATS = {
    'H1': {'name': 'H1 우량주 반등', 'icon': '💎', 'color': '#0f766e',
           'rule': '우량주 100 중 종가가 20일선보다 10% 이상 아래 → 다음날 08:35 장전 시장가 매수 · +5% 지정가로 30% 익절(3주 이하는 전량) · 나머지는 종가가 보유 중 최고 종가 −4%면 다음날 시가 매도 · −15% 재난 손절 · 최대 40거래일',
           'size': '종목당 계좌의 10% · 칸 한도(계좌 70%) 안에서 · 최대 14종목 · 업종당 2',
           'evidence': '2020~2026 백테스트 연 14.8% · 최대 낙폭 −33.5% (실제 순서) · 2023~ 연 22.7%'},
    'S5': {'name': 'S5 우량주 단기 눌림', 'icon': '⚡', 'color': '#7c3aed',
           'rule': '우량주 100 중 200일선 위(상승 추세) 종목이 5일간 −7% 이상 빠짐(H1 신호 아닌 것) → 다음날 08:35 장전 시장가 매수 · 종가가 5일선 위로 오르면 다음날 시가 매도 · 최대 7일',
           'size': '종목당 계좌의 5% · 최대 6종목 · H1 칸(70%)의 남는 돈으로, H1이 먼저',
           'evidence': '비용 뺀 건당 +0.65%(2019~22, t 2.9) · +0.44%(2023~26, t 3.0) · 기준 −5 ~ −10% 모두 같은 방향 · 2022 · 2025는 약간 손해'},
    'ON': {'name': '밤사이 코스닥150 ETF', 'icon': '🌙', 'color': '#1d4ed8',
           'rule': '매일 15:21 장마감 동시호가 시장가로 KODEX 코스닥150 매수(종가 체결) → 다음날 08:35 장전 시장가 매도(시가 체결)',
           'size': '계좌의 30% (H1 · S5 칸과 돈을 섞지 않음)',
           'evidence': '코스닥150 밤사이 평균 +0.107%(2019~22, t 3.7) · +0.136%(2023~26, t 3.7), 낮은 마이너스 · 왕복 비용 0.05% 이하일 때만 확실히 이득 → 모의투자로 실제 비용 확인'},
    'CORE': {'name': '코어 배당·가치 15', 'icon': '🏛', 'color': '#b45309',
             'rule': '매월 첫 거래일 코스피200 · 코스닥150 중 흑자 · 배당 · 편입 1년 이상 → 배당수익률 + 저PBR 점수 상위 15(업종당 4) → 다음날 08:35 장전 시장가 매수 · 30위 밖으로 밀린 종목만 판다(완충) · 손절 없음',
             'size': '칸 한도(기본 꺼짐 · 켜면 계좌의 %) ÷ 15 종목당 · 다른 칸이 가진 종목은 건너뜀',
             'evidence': 'seed 월별 검증(2020-01~2026-09, 비용 뺌): 연 +24.4% · 최대 낙폭 −27% vs 구성 350 동일가중 +11.3% · −28% · 2020~22 +12.0% / 2023~26 +35.6% · 배당세 빼도 초과 · 초과 t 1.7(강하지 않음) · 반은 배당 자체 (research/monthly_study_result.md)'},
}
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
KIND = {'entry': '매수', 'tp': '1차 익절 +5%', 'trail': '추적 매도', 'stop': '재난 손절', 'time': '40일 만료', 'manual': '수동',
        'exit': '반등 매도(5일선)', 'exit7': '7일 만료 매도', 'night': '밤사이 매수(종가)', 'nsell': '밤사이 매도(시가)',
        'core': '코어 매수', 'crebal': '코어 교체 매도'}


_ready = set()


def c():
    x = db.conn()
    if id(x) not in _ready:
        x.executescript(SCHEMA)
        for t in ('kis_pos', 'kis_orders', 'kis_closed'):                 # B1.0: 모델(칸) 열
            if 'strat' not in {r[1] for r in x.execute(f'PRAGMA table_info({t})')}:
                x.execute(f"ALTER TABLE {t} ADD COLUMN strat TEXT DEFAULT 'H1'")
        x.execute("""CREATE TABLE IF NOT EXISTS kis_strat_daily (date TEXT, strat TEXT, invested REAL, value REAL, unreal REAL, realized REAL,
                     npos INTEGER, PRIMARY KEY (date, strat))""")
        x.commit()
        _ready.add(id(x))
    return x


def on(cfg, k):
    """칸 켜짐 (기본 켜짐)"""
    return {'S5': cfg.get('kis_s5_on', False), 'ON': cfg.get('kis_night_on', True), 'H1': True,
            'CORE': cfg.get('kis_core_on', False)}.get(k, False) is not False   # B1.1: S5 기본 끔 · B1.2: 코어 기본 끔


def pct(cfg, k):
    try:
        if k == 'H1':
            return min(100.0, max(30.0, float(cfg.get('kis_h1_pct') or 70)))
        if k == 'CORE':
            return min(60.0, max(0.0, float(cfg.get('kis_core_pct') or 0)))
        return min(60.0, max(0.0, float(cfg.get('kis_night_pct') if cfg.get('kis_night_pct') is not None else 30)))
    except Exception:
        return {'H1': 70.0, 'CORE': 0.0}.get(k, 30.0)


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


def start_value():
    """모의투자 시작 평가액 (첫 매수 전 계좌) — 첫 '장전 점검 OK' 기록의 평가액, 없으면 첫 평가 기록"""
    v = db.meta_get('kis_start_value')
    if v:
        return {'value': float(v), 'date': db.meta_get('kis_start_date') or ''}
    x = c()
    r = x.execute("SELECT ts, msg FROM kis_log WHERE msg LIKE '장전 점검 OK%' ORDER BY id LIMIT 1").fetchone()
    m = re.search(r'평가 ([\d,]+)', r[1]) if r else None
    val, dt = (float(m.group(1).replace(',', '')), r[0][:10].replace('-', '')) if m else (None, '')
    if val is None:
        e = x.execute('SELECT date, value FROM kis_equity ORDER BY date LIMIT 1').fetchone()
        val, dt = (e[1], e[0]) if e else (None, '')
    if val:
        db.meta_set('kis_start_value', val)
        db.meta_set('kis_start_date', dt)
        return {'value': val, 'date': dt}
    return None


def expire_limits():
    """지정가(익절)는 당일만 유효 — 15:31 뒤 · 지난 날짜의 '접수'를 '만료'로 (화면에 '대기'로 남지 않게)"""
    hm = now().strftime('%H:%M')
    x = c()
    n = x.execute("UPDATE kis_orders SET status='만료' WHERE status IN ('접수','부분','취소요청') AND ord_dvsn='00' AND (date<? OR (date=? AND ?>='15:31'))",
                  (today(), today(), hm)).rowcount
    x.commit()          # B1.2: 바뀐 행이 0이어도 커밋 — 안 하면 UPDATE가 연 쓰기 잠금이 남아 16:40 계산 등 다른 스레드 쓰기가 'database is locked'로 실패
    return n


def _cap(cfg):
    try:
        return max(1_000_000, int(float(cfg.get('kis_cap') or 10_000_000)))
    except Exception:
        return 10_000_000


# ════════════════════════════════════════════
#  주문 · 체결
# ════════════════════════════════════════════
def send(cfg, kc, side, kind, ticker, name, qty, ord_dvsn='01', price=0, strat=None):
    """주문 한 건 — 기록 · 결과 불분명이면 정지"""
    x = c()
    if strat is None:
        r = x.execute('SELECT strat FROM kis_pos WHERE ticker=?', (ticker,)).fetchone()
        strat = (r[0] if r else None) or 'H1'
    x.execute("INSERT INTO kis_orders (date, ts, ticker, name, side, kind, qty, ord_dvsn, price, status, strat) VALUES (?,?,?,?,?,?,?,?,?,'보냄',?)",
              (today(), now().isoformat(timespec='seconds'), ticker, name, side, kind, int(qty), ord_dvsn, float(price or 0), strat))
    oid = x.execute('SELECT last_insert_rowid()').fetchone()[0]
    x.commit()
    try:
        r = kc.order(side, ticker, qty, ord_dvsn, price)
        x.execute("UPDATE kis_orders SET status='접수', order_no=?, org_no=?, msg=? WHERE id=?", (r['order_no'], r['org_no'], r['msg'][:200], oid))
        x.commit()
        log(f"{'🟢' if side == 'buy' else '🔵'} [{strat}] {KIND.get(kind, kind)} 주문 {name} {qty}주 {'시장가' if ord_dvsn == '01' else f'지정가 {price:,.0f}'}")
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
    except Exception as e:                            # kc.order는 전송 뒤 오류를 모두 KISError로 바꿈 → 여기는 전송 전 오류
        x.execute("UPDATE kis_orders SET status='거절', msg=? WHERE id=?", (f'전송 전 오류: {str(e)[:180]}', oid))
        x.commit()
        log(f'주문 전송 전 오류 {name} {KIND.get(kind, kind)}: {str(e)[:150]}', 'warn')
        return None


def cancel_open(kc, ticker, kinds=('tp',)):
    """반환: 취소 요청한 주문 수"""
    x = c()
    n = 0
    for o in [dict(r) for r in x.execute(f"SELECT * FROM kis_orders WHERE date=? AND ticker=? AND status IN ('접수','부분') AND kind IN ({','.join('?' * len(kinds))})",
                                          (today(), ticker, *kinds))]:
        try:
            kc.cancel(o['order_no'], o['org_no'])
            x.execute("UPDATE kis_orders SET status='취소요청' WHERE id=?", (o['id'],))
            n += 1
        except KISError as e:
            log(f"취소 실패 {o['name']}: {e}", 'warn')
    x.commit()
    return n


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
        st = o.get('strat') or 'H1'
        if not p:
            if st == 'ON':
                sig = (d, 'ETF')
            elif st == 'CORE':
                sig = x.execute('SELECT month, sector FROM monthly WHERE ticker=? ORDER BY month DESC LIMIT 1', (o['ticker'],)).fetchone()
            else:
                sig = x.execute("SELECT signal_date, sector FROM orders WHERE model=? AND ticker=? ORDER BY signal_date DESC LIMIT 1",
                                ('S5' if st == 'S5' else 'H', o['ticker'])).fetchone()
            x.execute("""INSERT INTO kis_pos (ticker, name, sector, signal_date, entry_date, entry_px, qty_total, qty, cost, status, updated, strat, sell_next)
                         VALUES (?,?,?,?,?,?,?,?,?,'보유',?,?,?)""", (o['ticker'], o['name'], sig[1] if sig else '', sig[0] if sig else '', d, avg, filled, filled,
                                                                   filled * avg, now().isoformat(timespec='seconds'), st, 1 if st == 'ON' else 0))
        else:
            sold = p['qty_total'] - p['qty']
            x.execute('UPDATE kis_pos SET entry_px=?, qty_total=?, qty=?, cost=? WHERE ticker=?', (avg, filled, filled - sold, filled * avg, o['ticker']))
        log(f"✅ [{st}] 매수 체결 {o['name']} {filled}주 @ {avg:,.0f}")
        return
    if not p:
        log(f"보유 기록 없는 매도 체결 {o['name']} {filled}주 — 확인 필요", 'warn')
        return
    p = dict(p)
    # B1.2: 이 보유분의 매수 주문 뒤에 나온 매도만 셈 (예전엔 date>=entry_date → 같은 날 아침에 판 지난 밤사이 ETF까지 더해 손익이 부풀려짐)
    buy_id = x.execute("SELECT COALESCE(MAX(id),0) FROM kis_orders WHERE ticker=? AND side='buy' AND filled>0 AND id<?", (o['ticker'], o['id'])).fetchone()[0]
    sells = x.execute("SELECT COALESCE(SUM(CASE WHEN id=? THEN ? ELSE filled END),0), COALESCE(SUM(CASE WHEN id=? THEN ?*? ELSE filled*avg END),0) "
                      "FROM kis_orders WHERE ticker=? AND side='sell' AND id>? AND (filled>0 OR id=?)",
                      (o['id'], filled, o['id'], filled, avg, o['ticker'], buy_id, o['id'])).fetchone()
    sold_qty, proceeds = int(sells[0]), float(sells[1])
    left = max(0, p['qty_total'] - sold_qty)
    upd = {'qty': left, 'realized': proceeds}
    if o['kind'] == 'tp':
        upd.update(part_done=1, part_px=avg, peak=max(p['peak'] or 0, avg))
    x.execute(f"UPDATE kis_pos SET {', '.join(k + '=?' for k in upd)}, updated=? WHERE ticker=?", (*upd.values(), now().isoformat(timespec='seconds'), o['ticker']))
    log(f"✅ {KIND.get(o['kind'], o['kind'])} 체결 {o['name']} {filled}주 @ {avg:,.0f}")
    if left == 0:
        fee = p['cost'] * COSTS.get(p.get('strat') or 'H1', eng.COST) / 100
        pnl = proceeds - p['cost'] - fee
        x.execute("""INSERT INTO kis_closed (ticker, name, signal_date, entry_date, entry_px, qty_total, exit_date, cost, proceeds, pnl, ret, reason, days, strat)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (p['ticker'], p['name'], p['signal_date'], p['entry_date'], p['entry_px'], p['qty_total'], d,
                                                                p['cost'], proceeds, pnl, pnl / p['cost'] * 100 if p['cost'] else 0, KIND.get(o['kind'], o['kind']), p['days'],
                                                                p.get('strat') or 'H1'))
        x.execute('DELETE FROM kis_pos WHERE ticker=?', (p['ticker'],))
        log(f"{'💰' if pnl > 0 else '🔴'} [{p.get('strat') or 'H1'}] 청산 {p['name']} {pnl / p['cost'] * 100:+.2f}% · {pnl:+,.0f}원 ({KIND.get(o['kind'], o['kind'])})")


def repair_closed():
    """B1.2 한 번만: B1.0~1.1의 매도 합산 버그로 부풀려진 청산 기록(kis_closed)을 주문 기록에서 다시 계산"""
    if db.meta_get('fix_b12_closed') == '1':
        return 0
    x = c()
    n = 0
    for r in [dict(r) for r in x.execute('SELECT * FROM kis_closed')]:
        b = x.execute("SELECT MAX(id) FROM kis_orders WHERE ticker=? AND side='buy' AND filled>0 AND date=?", (r['ticker'], r['entry_date'])).fetchone()[0]
        if not b:
            continue
        nb = x.execute("SELECT MIN(id) FROM kis_orders WHERE ticker=? AND side='buy' AND filled>0 AND id>?", (r['ticker'], b)).fetchone()[0] or 1 << 62
        pr = x.execute("SELECT COALESCE(SUM(filled*avg),0) FROM kis_orders WHERE ticker=? AND side='sell' AND filled>0 AND id>? AND id<? AND date<=?",
                       (r['ticker'], b, nb, r['exit_date'])).fetchone()[0]
        if pr and abs(pr - (r['proceeds'] or 0)) > 1:
            fee = (r['cost'] or 0) * COSTS.get(r.get('strat') or 'H1', eng.COST) / 100
            pnl = pr - r['cost'] - fee
            x.execute('UPDATE kis_closed SET proceeds=?, pnl=?, ret=? WHERE id=?', (pr, pnl, pnl / r['cost'] * 100 if r['cost'] else 0, r['id']))
            log(f"청산 기록 고침 {r['name']} {r['exit_date']}: {r['pnl']:+,.0f}원 → {pnl:+,.0f}원 (B1.0~1.1 매도 합산 버그)", 'warn')
            n += 1
    x.commit()
    db.meta_set('fix_b12_closed', '1')
    return n


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
        st = p.get('strat') or 'H1'
        kind = {'H1': 'trail', 'ON': 'nsell', 'CORE': 'crebal'}.get(st) or ('exit7' if (p['days'] or 0) >= S5_MAXD else 'exit')
        send(cfg, kc, 'sell', kind, p['ticker'], p['name'], p['qty'], strat=st)
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
    cash = min(bal['cash_d2'] or bal['cash'], bal['cash'] or bal['cash_d2'])
    plan = build_plan(cfg, bal['equity'], cash, last, d)
    plan += build_s5(cfg, bal['equity'], cash, last, d, plan)
    try:
        plan += build_core(cfg, bal['equity'], cash - sum(p['qty'] * p['ref'] * 1.02 for p in plan if not p['skip']), last, d)
    except Exception as e:
        log(f'코어 계획 실패: {str(e)[:150]}', 'warn')
    for p in plan:
        if p['skip']:
            log(f"[{p['strat']}] {p['name']} 건너뜀 — {p['skip']}")
            continue
        send(cfg, kc, 'buy', 'core' if p['strat'] == 'CORE' else 'entry', p['ticker'], p['name'], p['qty'], strat=p['strat'])
        if halted():
            return


# ════════════════════════════════════════════
#  🏛 코어 배당·가치 칸 (B1.2 · 기본 꺼짐)
# ════════════════════════════════════════════
def core_rank(month=None):
    """그달 코스피200 · 코스닥150 중 흑자 · 배당 · PBR>0 · 편입 12개월+ → z(배당수익률) + z(1/PBR) 순위 (업종당 CORE_SEC)"""
    x = db.conn()
    month = month or x.execute('SELECT MAX(month) FROM members').fetchone()[0]
    if not month:
        return month, []
    ten = {r[0]: r[1] for r in x.execute('SELECT ticker, COUNT(*) FROM members WHERE month<? GROUP BY ticker', (month,))}
    rows = [dict(r) for r in x.execute('''SELECT m.ticker, m.name, m.sector, m.div, m.pbr, m.eps FROM monthly m
                                          JOIN members b ON b.month=m.month AND b.ticker=m.ticker WHERE m.month=?''', (month,))]
    rows = [r for r in rows if (r['eps'] or 0) > 0 and (r['div'] or 0) > 0 and (r['pbr'] or 0) > 0 and ten.get(r['ticker'], 0) >= 12]
    if len(rows) < 40:
        return month, []

    def z(vals):
        v = sorted(vals)
        lo, hi = v[int(len(v) * .02)], v[int(len(v) * .98) - 1]
        c_ = [min(max(a, lo), hi) for a in vals]
        mu = sum(c_) / len(c_)
        sd = (sum((a - mu) ** 2 for a in c_) / (len(c_) - 1)) ** .5 or 1
        return [(a - mu) / sd for a in c_]
    zd, zb = z([r['div'] for r in rows]), z([1 / r['pbr'] for r in rows])
    for r, a, b in zip(rows, zd, zb):
        r['score'] = a + b
    rows.sort(key=lambda r: -r['score'])
    secn, out = {}, []
    for r in rows:
        if secn.get(r['sector'], 0) >= CORE_SEC:
            continue
        secn[r['sector']] = secn.get(r['sector'], 0) + 1
        out.append(r)
    for i, r in enumerate(out):
        r['rank'] = i + 1
    return month, out


def core_mark(d):
    """장 마감 계산 뒤(매일, 새 달 자료가 들어오면 실제로 바뀜): 코어 보유 중 순위 CORE_BUF 밖 → 다음 장전 매도 표시"""
    month, rank = core_rank()
    if not rank or db.meta_get('core_month') == month:
        return 0
    pos = {r['ticker']: r['rank'] for r in rank}
    x = c()
    n = 0
    for p in [dict(r) for r in x.execute("SELECT * FROM kis_pos WHERE qty>0 AND strat='CORE'")]:
        if pos.get(p['ticker'], 10 ** 6) > CORE_BUF:
            x.execute('UPDATE kis_pos SET sell_next=1 WHERE ticker=?', (p['ticker'],))
            log(f"[CORE] 교체 매도 표시 {p['name']} (순위 {pos.get(p['ticker'], '밖')}) → 다음 장전 시장가")
            n += 1
    x.commit()
    db.meta_set('core_month', month)
    log(f"{month} 코어 순위 갱신: 상위 {', '.join(r['name'] for r in rank[:CORE_N])}")
    return n


def build_core(cfg, equity, cash, last, d=None):
    """코어 매수 계획: 목표 = 보유 중 순위 CORE_BUF 안 + 빈자리는 상위부터 · 칸 한도 ÷ CORE_N 종목당 · 모자란 종목은 다음 날 다시 (자동으로 채워짐)"""
    if not on(cfg, 'CORE') or pct(cfg, 'CORE') <= 0:
        return []
    month, rank = core_rank()
    if not rank:
        return []
    x = c()
    cap = _cap(cfg)
    base = min(equity or cap, cap)
    budget = base * pct(cfg, 'CORE') / 100
    size = budget / CORE_N
    held_all = {r[0]: r[1] for r in x.execute("SELECT ticker, COALESCE(strat,'H1') FROM kis_pos WHERE qty>0")}
    mine = {t for t, st in held_all.items() if st == 'CORE'}
    selling = {r[0] for r in x.execute("SELECT ticker FROM kis_pos WHERE qty>0 AND strat='CORE' AND sell_next=1")}
    keep = mine - selling
    room = budget - sleeve_invested(('CORE',)) + sum(r[0] for r in x.execute("SELECT qty*entry_px FROM kis_pos WHERE qty>0 AND strat='CORE' AND sell_next=1"))
    cash = cash if cash is not None else budget
    need = CORE_N - len(keep)
    px = eng.prices([r['ticker'] for r in rank[:CORE_BUF]], last, last) if last and need > 0 else {}
    out = []
    for r in rank:
        if need <= 0:
            break
        t = r['ticker']
        if t in keep:
            continue
        o = {'ticker': t, 'name': r['name'], 'sector': r['sector'], 'ref': 0.0, 'ma20gap': None, 'qty': 0, 'amt': 0, 'skip': '', 'strat': 'CORE',
             'rank': r['rank'], 'div': r['div'], 'pbr': r['pbr']}
        bar = px.get(t)
        ref = float(bar.close.iloc[-1]) if bar is not None and len(bar) else 0.0
        o['ref'] = ref
        if t in held_all:
            o['skip'] = '다른 칸이 보유 중'
        elif d and x.execute("SELECT 1 FROM kis_orders WHERE date=? AND ticker=? AND side='buy' AND status NOT LIKE '거절%'", (d, t)).fetchone():
            o['skip'] = '오늘 이미 주문'
        elif ref <= 0:
            o['skip'] = '기준 가격 없음'
        else:
            amt = min(size, room, cash / 1.03)
            qty = int(amt // (ref * 1.02)) if amt > 0 else 0
            if qty <= 0:
                o['skip'] = '1주 가격 > 종목당 금액' if ref * 1.02 > size else '칸 한도 · 현금 부족'
            else:
                o.update(qty=qty, amt=qty * ref)
                room -= qty * ref * 1.02
                cash -= qty * ref * 1.02
        out.append(o)
        if not o['skip']:
            need -= 1
    return [o for o in out if not o['skip'] or o['skip'] not in ('다른 칸이 보유 중',)][:CORE_N + 5]


def build_plan(cfg, equity, cash, last, d=None):
    """H 신호 → 주문 수량 (장전 주문 · 화면 미리보기 공용). skip = 안 사는 이유"""
    x = c()
    plan = [dict(r) for r in x.execute("SELECT * FROM orders WHERE model='H' AND signal_date=? AND status IN ('대기','체결','안 삼') ORDER BY prio", (last,))]
    cap = _cap(cfg)
    base = min(equity or cap, cap)
    size = base / SLOTS
    cash = cash if cash else base
    day_cap, one_cap, spent = base * 0.40, base * 0.15, 0.0
    room = base * pct(cfg, 'H1') / 100 - sleeve_invested()               # B1.0: H1 · S5 칸 한도(계좌 70%)
    held = {r[0]: r[1] for r in x.execute('SELECT ticker, sector FROM kis_pos WHERE qty>0')}
    secn = {}
    for s_ in held.values():
        secn[s_] = secn.get(s_, 0) + 1
    out = []
    for o in plan:
        ref = x.execute('SELECT close FROM daily WHERE date=? AND ticker=?', (last, o['ticker'])).fetchone()
        ref = float(ref[0]) if ref and ref[0] else 0
        r = {'ticker': o['ticker'], 'name': o['name'], 'sector': o['sector'], 'ref': ref, 'ma20gap': o['ma20gap'], 'qty': 0, 'amt': 0, 'skip': '', 'strat': 'H1'}
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
            amt = min(size, one_cap, cash / 1.03, day_cap - spent, room - spent)
            qty = int(amt // (ref * 1.02))             # 시가 갭 여유 2%
            if qty <= 0 and ref * 1.02 <= min(size, cash / 1.03, day_cap - spent, room - spent):
                qty = 1
            if qty <= 0:
                r['skip'] = f'금액 부족 · 한도 (1주 {ref:,.0f} > 종목당 {size:,.0f})' if ref > size else ('H1 칸 한도(계좌 %.0f%%) 다 씀' % pct(cfg, 'H1') if room - spent < size * 0.5 else '현금 · 하루 한도 부족')
            else:
                r.update(qty=qty, amt=qty * ref)
                spent += qty * ref
                cash -= qty * ref * 1.02
                held[o['ticker']] = o['sector']
                secn[o['sector']] = secn.get(o['sector'], 0) + 1
        out.append(r)
    return out


def sleeve_invested(strats=('H1', 'S5')):
    """칸에 들어가 있는 돈 (남은 수량 × 매수가)"""
    q = ','.join('?' * len(strats))
    r = c().execute(f"SELECT COALESCE(SUM(qty*entry_px),0) FROM kis_pos WHERE qty>0 AND COALESCE(strat,'H1') IN ({q})", strats).fetchone()
    return float(r[0] or 0)


def build_s5(cfg, equity, cash, last, d=None, h1_plan=()):
    """S5 신호 → 주문 수량. H1 계획을 먼저 빼고 남는 칸 한도 · 현금 안에서"""
    x = c()
    if not on(cfg, 'S5') or not last:
        return []
    sig = [dict(r) for r in x.execute("SELECT * FROM orders WHERE model='S5' AND signal_date=? AND status IN ('대기','체결','안 삼') ORDER BY prio", (last,))]
    cap = _cap(cfg)
    base = min(equity or cap, cap)
    size = base * S5_PCT / 100
    h1_amt = sum(p['qty'] * p['ref'] * 1.02 for p in h1_plan if not p['skip'])
    room = base * pct(cfg, 'H1') / 100 - sleeve_invested() - h1_amt
    cash = (cash if cash else base) - h1_amt
    held = {r[0]: r[1] for r in x.execute('SELECT ticker, strat FROM kis_pos WHERE qty>0')}
    h1_new = {p['ticker'] for p in h1_plan if not p['skip']}
    n5 = sum(1 for v in held.values() if v == 'S5')
    out = []
    for o in sig:
        ref = x.execute('SELECT close FROM daily WHERE date=? AND ticker=?', (last, o['ticker'])).fetchone()
        ref = float(ref[0]) if ref and ref[0] else float(o.get('score') or 0)
        r = {'ticker': o['ticker'], 'name': o['name'], 'sector': o['sector'], 'ref': ref, 'ma20gap': o['ma20gap'], 'r5': o['prio'], 'qty': 0, 'amt': 0, 'skip': '', 'strat': 'S5'}
        if o['ticker'] in held or o['ticker'] in h1_new:
            r['skip'] = '이미 보유 · H1과 겹침'
        elif n5 >= S5_MAX:
            r['skip'] = f'S5 {S5_MAX}종목 한도'
        elif d and x.execute("SELECT 1 FROM kis_orders WHERE date=? AND ticker=? AND kind='entry' AND status NOT LIKE '거절%'", (d, o['ticker'])).fetchone():
            r['skip'] = '오늘 이미 주문'
        elif ref <= 0:
            r['skip'] = '기준 가격 없음'
        else:
            amt = min(size, cash / 1.03, room)
            qty = int(amt // (ref * 1.02)) if amt > 0 else 0
            if qty <= 0:
                r['skip'] = '칸 한도 · 현금 부족 (H1이 먼저)' if amt < ref * 1.02 else '금액 부족'
            else:
                r.update(qty=qty, amt=qty * ref)
                room -= qty * ref * 1.02
                cash -= qty * ref * 1.02
                n5 += 1
                held[o['ticker']] = 'S5'
        out.append(r)
    return out


def make_s5(d):
    """장 마감 계산 뒤 S5 신호 (우량주 100 · 200일선 위 · 5일 −7% 이하 · H1 신호 아님) → orders(model='S5')"""
    x = c()
    m = x.execute('SELECT MAX(month) FROM universe WHERE month<=?', (d[:6],)).fetchone()[0]
    if not m:
        return []
    uni = {r['ticker']: dict(r) for r in x.execute('SELECT ticker, name, sector FROM universe WHERE month=?', (m,))}
    days_list = eng.trading_days('20230101', d)
    if not days_list or days_list[-1] != d:
        return []
    px = eng.prices(list(uni), eng.back_date(days_list, d, 215), d)
    sig = []
    for tk, u in uni.items():
        p = px.get(tk)
        if p is None or d not in p.index or len(p) < 200:
            continue
        cl = p.close
        c0 = float(cl.iloc[-1])
        ma200, ma20, r5 = float(cl.iloc[-200:].mean()), float(cl.iloc[-20:].mean()), c0 / float(cl.iloc[-6]) - 1
        if r5 <= S5_TH and c0 > ma200 and not c0 <= ma20 * 0.90:
            sig.append((r5, tk, u, c0, c0 / ma20 - 1))
    sig.sort()
    x.execute("DELETE FROM orders WHERE model='S5' AND signal_date=? AND status='대기'", (d,))
    x.executemany('INSERT INTO orders (model,signal_date,ticker,name,sector,prio,score,ma20gap,status) VALUES (?,?,?,?,?,?,?,?,?)',
                  [('S5', d, tk, u['name'], u['sector'], r5, c0, g, '대기') for r5, tk, u, c0, g in sig])
    x.commit()
    db.meta_set('kis_s5_date', d)
    if sig:
        log(f"{d} S5 신호 {len(sig)}종목: " + ' · '.join(f"{u['name']} {r5 * 100:+.1f}%" for r5, tk, u, c0, g in sig[:8]))
    return sig


def night_plan(cfg, equity=None, cash=None, price=None):
    """밤사이 칸 매수 계획 (화면 · 15:21 주문 공용)"""
    cap = _cap(cfg)
    base = min(equity or cap, cap)
    budget = base * pct(cfg, 'ON') / 100
    held = c().execute("SELECT qty FROM kis_pos WHERE strat='ON' AND qty>0").fetchone()
    amt = min(budget, (cash if cash is not None else budget) * 0.99)
    qty = int(amt // (price * 1.005)) if price and amt > 0 else 0
    skip = ''
    if not on(cfg, 'ON') or pct(cfg, 'ON') <= 0:
        skip = '밤사이 칸 꺼짐'
    elif held:
        skip = '어제 산 ETF가 아직 안 팔림 (오늘은 쉼)'
    elif price and qty <= 0:
        skip = '현금 부족'
    return {'ticker': ON_TICKER, 'name': ON_NAME, 'budget': budget, 'price': price, 'qty': qty, 'amt': qty * (price or 0), 'skip': skip}


def night_buy(cfg, kc, d):
    """15:21 — 밤사이 칸: KODEX 코스닥150 장마감 동시호가 시장가 매수 (종가 체결) → 다음날 08:35 장전 시장가 매도"""
    if cfg.get('kis_buy_pause'):
        log('신규 매수 일시 중지 중 → 밤사이 매수 쉼', 'warn')
        return
    sync(kc, d)
    bal = kc.balance()
    px, _ = kc.price(ON_TICKER)
    mine = {r[0] for r in c().execute('SELECT ticker FROM kis_pos WHERE qty>0')}
    if [p for p in bal['positions'] if p['ticker'] not in mine]:
        log('앱이 모르는 보유 종목이 있어 밤사이 매수 쉼', 'warn')
        return
    cash = min(bal['cash_d2'] or bal['cash'], bal['cash'] or bal['cash_d2'])
    pl = night_plan(cfg, bal['equity'], cash, px)
    if pl['skip'] or pl['qty'] <= 0:
        log(f"밤사이 매수 건너뜀 — {pl['skip'] or '수량 0'}")
        return
    if not send(cfg, kc, 'buy', 'night', ON_TICKER, ON_NAME, pl['qty'], strat='ON') and not halted():
        alert(f'밤사이 ETF 매수 주문이 거절됨 ({ON_NAME} {pl["qty"]}주) — 로그 확인', 'nightfail')


def retry_rejected(cfg, kc, d):
    """09:01 — 장전 시간 때문에 거절된 주문을 장중 시장가로 한 번 더"""
    x = c()
    for o in [dict(r) for r in x.execute("SELECT * FROM kis_orders WHERE date=? AND status='거절' AND kind IN ('entry','trail','exit','exit7','nsell') AND ord_dvsn='01'", (d,))]:
        m = o['msg'] or ''
        if any(k in m for k in ('시간', '장개시', '장시작', '장운영', '동시호가')):
            x.execute("UPDATE kis_orders SET status='거절(재시도)' WHERE id=?", (o['id'],))
            x.commit()
            send(cfg, kc, o['side'], o['kind'], o['ticker'], o['name'], o['qty'], strat=o.get('strat') or 'H1')
            if halted():
                return


def place_tp(cfg, kc, d):
    """09:03 — 1차 익절 전 종목에 +5% 지정가 (보유의 30%, 3주 이하는 전량) · 지정가는 당일만 유효라 매일"""
    x = c()
    for p in [dict(r) for r in x.execute("SELECT * FROM kis_pos WHERE qty>0 AND part_done=0 AND status='보유' AND sell_next=0 AND COALESCE(strat,'H1')='H1'")]:
        if x.execute("SELECT 1 FROM kis_orders WHERE date=? AND ticker=? AND kind='tp' AND status IN ('보냄','접수','부분','체결')", (d, p['ticker'])).fetchone():
            continue
        n1 = int(p['qty'] * PART)
        q = n1 if 0 < n1 < p['qty'] else p['qty']
        send(cfg, kc, 'sell', 'tp', p['ticker'], p['name'], q, '00', tick_up(p['entry_px'] * (1 + eng.TP / 100)), strat='H1')
        if halted():
            return


STOP_TRIES = 3


def try_stop(cfg, kc, p, px, src='30초 감시'):
    """−15% 재난 손절 주문 한 번 (30초 감시 · 웹소켓 공용). 거절되면 하루 STOP_TRIES번까지만 다시 (B1.2 — 예전엔 30초마다 끝없이)"""
    x = c()
    k = f"kis_stopfail_{p['ticker']}_{today()}"
    fails = int(db.meta_get(k) or 0)
    if fails >= STOP_TRIES:
        return False
    if cancel_open(kc, p['ticker'], ('tp',)):
        time.sleep(1.0)                                               # 익절 지정가에 묶인 수량이 풀릴 시간
    if send(cfg, kc, 'sell', 'stop', p['ticker'], p['name'], p['qty'], strat='H1'):
        x.execute("UPDATE kis_pos SET status='손절 주문' WHERE ticker=?", (p['ticker'],))
        x.commit()
        log(f"재난 손절 발동 {p['name']} 현재가 {px:,.0f} ≤ {p['entry_px'] * (1 - STOP / 100):,.0f} ({src})", 'warn')
        return True
    if not halted():
        db.meta_set(k, fails + 1)
        if fails + 1 >= STOP_TRIES:
            alert(f"{p['name']} 재난 손절 주문이 {STOP_TRIES}번 거절됨 — KIS 앱에서 직접 확인하세요", f"stopfail_{p['ticker']}")
    return False


def stop_hit(p, px):
    return (p.get('strat') or 'H1') == 'H1' and p.get('status') == '보유' and px > 0 and px <= p['entry_px'] * (1 - STOP / 100)


def monitor(cfg, kc, d, live_px=None):
    """장중 30초 — −15% 재난 손절 (웹소켓이 살아 있으면 틱마다 먼저 잡고, 이건 예비)
    live_px: {종목: 가격} 웹소켓 시세가 신선한 종목 → KIS 현재가 조회 없이 그 값으로 (조회 호출 절약)"""
    x = c()
    live_px = live_px or {}
    for p in [dict(r) for r in x.execute("SELECT * FROM kis_pos WHERE qty>0 AND status='보유'")]:
        px = live_px.get(p['ticker'])
        if px is None:
            try:
                px, _ = kc.price(p['ticker'])
            except KISError as e:
                STATE['last_err'] = str(e)[:150]
                continue
        if px <= 0:
            continue
        x.execute('UPDATE kis_pos SET last_px=? WHERE ticker=?', (px, p['ticker']))
        x.commit()
        if stop_hit(p, px) and can_order(cfg):
            try_stop(cfg, kc, p, px)
            if halted():
                break
    x.commit()
    STATE['last_monitor'] = now().strftime('%H:%M:%S')


def time_exit(cfg, kc, d):
    """15:20 — 40거래일째 장마감 시장가"""
    x = c()
    for p in [dict(r) for r in x.execute("SELECT * FROM kis_pos WHERE qty>0 AND status='보유' AND days+1>=? AND COALESCE(strat,'H1')='H1'", (HOLD,))]:
        cancel_open(kc, p['ticker'], ('tp',))
        send(cfg, kc, 'sell', 'time', p['ticker'], p['name'], p['qty'], strat='H1')
        if halted():
            return


def eod(cfg, kc, d):
    """15:45 — 체결 반영 · 보유일 · 종가 최고가 · 추적 매도 표시 · 잔고 대조 · 평가 기록"""
    x = c()
    sync(kc, d)
    x.execute("UPDATE kis_orders SET status='만료' WHERE date=? AND status IN ('접수','부분','취소요청') AND ord_dvsn='00'", (d,))   # 지정가는 당일만
    bal = kc.balance()
    kpos = {p['ticker']: p for p in bal['positions']}
    bad = []
    s5 = [dict(r) for r in x.execute("SELECT * FROM kis_pos WHERE qty>0 AND strat='S5'")]
    if s5:
        days_list = eng.trading_days('20230101', d)
        prev = [t for t in days_list if t < d]
        hist = eng.prices([p['ticker'] for p in s5], eng.back_date(prev, prev[-1], 10), prev[-1]) if prev else {}
    for p in [dict(r) for r in x.execute('SELECT * FROM kis_pos WHERE qty>0')]:
        k = kpos.get(p['ticker'])
        cl = (k or {}).get('price') or p['last_px'] or p['entry_px']
        st = p.get('strat') or 'H1'
        if st == 'CORE':                                          # B1.2 코어: 매도는 월 교체 표시(core_mark)만 · 여기선 평가만
            if not k:
                bad.append(f"{p['name']} 앱 {p['qty']}주 · KIS 없음")
            elif k['qty'] != p['qty']:
                bad.append(f"{p['name']} 앱 {p['qty']}주 · KIS {k['qty']}주")
            x.execute('UPDATE kis_pos SET days=days+1, last_px=? WHERE ticker=?', (cl, p['ticker']))
            continue
        if st != 'H1':
            if not k:
                bad.append(f"{p['name']} 앱 {p['qty']}주 · KIS 없음")
            sell_next, why = 1, ''
            if st == 'S5':
                h = hist.get(p['ticker'])
                closes = list(h.close.iloc[-4:]) if h is not None and len(h) >= 4 else []
                ma5 = (sum(closes) + cl) / 5 if len(closes) == 4 else None
                nd = (p['days'] or 0) + 1
                sell_next = int((ma5 is not None and cl > ma5) or nd >= S5_MAXD)
                why = f"종가 {cl:,.0f} > 5일선 {ma5:,.0f}" if ma5 is not None and cl > ma5 else (f'{nd}일째 (최대 {S5_MAXD}일)' if nd >= S5_MAXD else '')
            x.execute('UPDATE kis_pos SET days=days+1, last_px=?, sell_next=? WHERE ticker=?', (cl, sell_next, p['ticker']))
            if st == 'S5' and sell_next:
                log(f"[S5] 매도 표시 {p['name']} ({why}) → 내일 장전 시장가")
            continue
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
    cash = min(bal['cash_d2'] or bal['cash'], bal['cash'] or bal['cash_d2'])          # B1.0: 결제 뒤(D+2) 예수금
    x.execute('INSERT OR REPLACE INTO kis_equity VALUES (?,?,?,?)', (d, cash, bal['equity'], len(bal['positions'])))
    for st in STRATS:
        ps = [dict(r) for r in x.execute("SELECT * FROM kis_pos WHERE qty>0 AND COALESCE(strat,'H1')=?", (st,))]
        inv = sum(p['qty'] * p['entry_px'] for p in ps)
        val = sum(p['qty'] * ((kpos.get(p['ticker']) or {}).get('price') or p['last_px'] or p['entry_px']) for p in ps)
        rz = x.execute("SELECT COALESCE(SUM(pnl),0) FROM kis_closed WHERE COALESCE(strat,'H1')=?", (st,)).fetchone()[0] + \
            sum(partial_pnl(p) for p in ps)
        x.execute('INSERT OR REPLACE INTO kis_strat_daily VALUES (?,?,?,?,?,?,?)', (d, st, inv, val, val - inv, rz, len(ps)))
    x.commit()
    if bad:
        alert('H1 모의투자 장 마감 잔고 불일치 — ' + ' / '.join(bad[:5]), 'eodmismatch')


def partial_pnl(p):
    """1차 익절처럼 일부만 판 종목의 실현 손익 (판 수량만큼)"""
    sold = (p['qty_total'] or 0) - (p['qty'] or 0)
    if sold <= 0 or not p['qty_total']:
        return 0.0
    return (p['realized'] or 0) - p['cost'] * sold / p['qty_total']


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
    buys = [f"[{b['strat']}] {b['name']}" for b in pv.get('buys', []) + pv.get('s5', []) + pv.get('core', []) if not b.get('skip')]
    if probs:
        log('장전 점검 — ' + ' / '.join(probs), 'warn')
        alert('H1 모의투자 장전 점검(08:20) 문제 — ' + ' / '.join(probs), 'precheck')
    else:
        log(f"장전 점검 OK — {info} · 08:35 매수 예정 {len(buys)}종목 {', '.join(buys)} · 매도 예정 {len(pv.get('sells', []))}".rstrip())


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
            expire_limits()
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
                        try:
                            import bluechip_ws
                            fr = bluechip_ws.fresh()                  # B1.2: 실시간 시세가 신선한 종목은 조회 없이 그 값으로
                            live_px = {k: v[0] for k, v in list(bluechip_ws.PRICE.items()) if k in fr}
                        except Exception:
                            live_px = {}
                        monitor(cfg, kc, d, live_px)
                    if '15:20' <= hm <= '15:27' and not _done('time', d) and can_order(cfg):
                        _mark('time', d)
                        time_exit(cfg, kc, d)
                    if '15:21' <= hm <= '15:28' and not _done('night', d) and can_order(cfg) and on(cfg, 'ON') and pct(cfg, 'ON') > 0:
                        _mark('night', d)
                        night_buy(cfg, kc, d)
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
    """H1 모의 체결 ↔ 같은 신호 가상 계산 H (체결 차이 확인용 · 화면엔 체결 차이만)"""
    x = c()
    rows = []
    for k in [dict(r) for r in x.execute("SELECT * FROM kis_closed WHERE COALESCE(strat,'H1')='H1' ORDER BY id DESC LIMIT 200")] + \
             [dict(r, exit_date=None, ret=None, reason='보유 중') for r in x.execute("SELECT * FROM kis_pos WHERE COALESCE(strat,'H1')='H1'")]:
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
    items = [('08:20', '장전 점검 (KIS 연결 · 잔고 · 매수 계획 · 문제 시 텔레그램)', 'check'), ('08:35', '장전 시장가 매수(H1 · S5) · 매도(추적 · 5일선 · 밤사이 ETF)', 'pre'), ('09:01', '장전 거절 주문 재시도', 'retry'),
             ('09:03~15:00', '체결 반영(3분) · +5% 익절 주문', None), ('09:05~15:18', '−15% 재난 손절 감시(30초)', None),
             ('15:20', '40일 만료 장마감 매도', 'time'), ('15:21', '🌙 밤사이 ETF 장마감 동시호가 매수', 'night'),
             ('15:45', '잔고 대조 · 매도 표시(H1 추적 · S5 5일선) · 평가 기록', 'eod')]
    hm = now().strftime('%H:%M')
    out = []
    for t, what, job in items:
        end = t.split('~')[-1]
        if not td:
            st = '휴장'
        elif job == 'night' and not (on(cfg, 'ON') and pct(cfg, 'ON') > 0):
            st = '꺼짐'
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
    held_on = x.execute("SELECT COALESCE(SUM(qty*COALESCE(last_px,entry_px)),0) FROM kis_pos WHERE strat='ON' AND qty>0").fetchone()[0]
    cash_am = (cash or 0) - (held_on or 0) if eq else cash               # 08:35엔 밤사이 ETF가 아직 안 팔려 있음
    buys = build_plan(cfg, equity, cash_am, last) if last else []
    s5 = build_s5(cfg, equity, cash_am, last, None, buys) if last else []
    try:
        core = build_core(cfg, equity, (cash_am or 0) - sum(p['qty'] * p['ref'] * 1.02 for p in buys + s5 if not p['skip']), last) if last else []
    except Exception:
        core = []
    WHY = {'H1': '추적 매도 (종가 ≤ 최고 종가 −4%) → 장전 시장가', 'S5': 'S5 매도 (5일선 회복 · 7일) → 장전 시장가', 'ON': '밤사이 ETF → 장전 시장가 (시가 체결)',
           'CORE': f'코어 교체 (순위 {CORE_BUF}위 밖) → 장전 시장가'}
    sells = [{'name': p['name'], 'ticker': p['ticker'], 'qty': p['qty'], 'strat': p['strat'] or 'H1', 'why': WHY.get(p['strat'] or 'H1', '')}
             for p in x.execute('SELECT * FROM kis_pos WHERE qty>0 AND sell_next=1')]
    sells += [{'name': p['name'], 'ticker': p['ticker'], 'qty': p['qty'], 'strat': 'H1', 'why': f"40일 만료 ({p['days']}일째) → 15:20 장마감"}
              for p in x.execute("SELECT * FROM kis_pos WHERE qty>0 AND days+1>=? AND sell_next=0 AND COALESCE(strat,'H1')='H1'", (HOLD,))]
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
    return {'signal_date': last, 'for_date': nxt, 'buys': buys, 's5': s5, 'core': core, 'sells': sells, 'used': used, 'stale': stale,
            'night': night_plan(cfg, equity, cash), 'basis': '마지막 KIS 평가 기록' if eq else '운용 한도 (평가 기록 없음)', 'equity': equity, 'cash': cash}


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
    for st in STRATS:
        curves[st] = [[r[0], (r[1] or 0) + (r[2] or 0)] for r in x.execute('SELECT date, realized, unreal FROM kis_strat_daily WHERE strat=? ORDER BY date', (st,))]
    closed = [dict(r) for r in x.execute('SELECT * FROM kis_closed ORDER BY id')]
    by_reason, by_month, by_strat = {}, {}, {}
    for r in closed:
        a = by_reason.setdefault((r['strat'] or 'H1', r['reason']), {'n': 0, 'sum': 0.0, 'pnl': 0.0})
        a['n'] += 1; a['sum'] += r['ret']; a['pnl'] += r['pnl']
        b = by_month.setdefault((r['exit_date'] or '')[:6], {'n': 0, 'pnl': 0.0, 'win': 0})
        b['n'] += 1; b['pnl'] += r['pnl']; b['win'] += r['ret'] > 0
    peak, mdd = 0.0, 0.0
    for r in eq:
        peak = max(peak, r['value'])
        mdd = min(mdd, r['value'] / peak - 1) if peak else mdd
    return {'curves': curves, 'mdd': mdd * 100, 'start': eq[0]['date'] if eq else '',
            'by_reason': [{'strat': k[0], 'reason': k[1], 'n': v['n'], 'avg': v['sum'] / v['n'], 'pnl': v['pnl']} for k, v in by_reason.items()],
            'by_month': [{'month': k, **v} for k, v in sorted(by_month.items())]}


def strat_summary(cfg, lp=None):
    """칸(모델)별 요약 — 한도 · 들어간 돈 · 평가 손익 · 실현 손익(부분 익절 포함) · 거래 통계. lp: {종목: 현재가}"""
    x = c()
    lp = lp or {}
    eqr = x.execute('SELECT value FROM kis_equity ORDER BY date DESC LIMIT 1').fetchone()
    base = min(eqr[0] if eqr else _cap(cfg), _cap(cfg))
    t = today()
    out = []
    for st, m in STRATS.items():
        ps = [dict(r) for r in x.execute("SELECT * FROM kis_pos WHERE qty>0 AND COALESCE(strat,'H1')=?", (st,))]
        inv = sum(p['qty'] * p['entry_px'] for p in ps)
        val = sum(p['qty'] * (lp.get(p['ticker']) or p['last_px'] or p['entry_px']) for p in ps)
        cl = [dict(r) for r in x.execute("SELECT * FROM kis_closed WHERE COALESCE(strat,'H1')=?", (st,))]
        part = sum(partial_pnl(p) for p in ps)
        rets = [r['ret'] for r in cl]
        today_rz = sum(r['pnl'] for r in cl if r['exit_date'] == t)
        limit = base * (pct(cfg, 'H1') / 100 if st in ('H1', 'S5') else pct(cfg, st) / 100)
        if st == 'S5':
            limit = min(limit, base * S5_PCT / 100 * S5_MAX)
        out.append({'key': st, **m, 'on': on(cfg, st) and (st not in ('ON', 'CORE') or pct(cfg, st) > 0), 'limit': limit, 'invested': inv, 'value': val,
                    'unreal': val - inv, 'realized': sum(r['pnl'] for r in cl) + part, 'partial': part, 'today_realized': today_rz,
                    'npos': len(ps), 'closed': len(cl), 'win': sum(1 for v in rets if v > 0) / len(rets) * 100 if rets else None,
                    'avg': sum(rets) / len(rets) if rets else None, 'use': inv / limit * 100 if limit else 0})
    return out


def criteria(cfg):
    """사전 등록 3개월 판정 기준 진행 (모의투자 계좌 기준)"""
    x = c()
    cmp_ = compare()
    closed = [r[0] for r in x.execute("SELECT ret FROM kis_closed WHERE COALESCE(strat,'H1')='H1'")]       # B1.2: 판정은 H1만 (밤사이 ETF는 매일 청산돼 건수를 채워 버림)
    night = [r[0] for r in x.execute("SELECT ret FROM kis_closed WHERE strat='ON'")]
    incidents = x.execute("SELECT COUNT(*) FROM kis_orders WHERE status='불분명'").fetchone()[0] + \
        x.execute("SELECT COUNT(*) FROM kis_log WHERE level='error' AND msg LIKE '%정지%'").fetchone()[0]
    eq = [dict(r) for r in x.execute('SELECT * FROM kis_equity ORDER BY date')]
    p = perf()
    sv = start_value()
    kret = (eq[-1]['value'] / sv['value'] - 1) * 100 if eq and sv else None
    orders = x.execute("SELECT COUNT(*), SUM(status IN ('취소','거절','거절(재시도)')) FROM kis_orders WHERE kind IN ('entry','night')").fetchone()
    miss = (orders[1] or 0) / orders[0] * 100 if orders[0] else None
    days = len(eq)
    avg = sum(closed) / len(closed) if closed else None
    d = cmp_['entry_diff_avg']
    return [
        {'k': '기간', 'v': f'{days}거래일 / 약 60', 'ok': days >= 60, 'prog': min(1, days / 60)},
        {'k': '주문 사고 0', 'v': f'{incidents}건', 'ok': incidents == 0, 'prog': 1 if incidents == 0 else 0},
        {'k': 'H1 체결 차이 ±0.3% (계산 대비)', 'v': '-' if d is None else f'{d:+.2f}% ({cmp_["n_entry"]}건)', 'ok': d is not None and abs(d) <= 0.3, 'prog': None},
        {'k': '미체결 · 거절 5% 이하', 'v': '-' if miss is None else f'{miss:.1f}%', 'ok': miss is not None and miss <= 5, 'prog': None},
        {'k': 'H1 청산 30건+', 'v': f'{len(closed)} / 30', 'ok': len(closed) >= 30, 'prog': min(1, len(closed) / 30)},
        {'k': 'H1 건당 > 0', 'v': '-' if avg is None else f'{avg:+.2f}%', 'ok': avg is not None and avg > 0, 'prog': None},
        {'k': '밤사이 ETF 건당 > 0 (비용 뺀 뒤 · 참고)', 'v': '-' if not night else f'{sum(night) / len(night):+.3f}% ({len(night)}건)',
         'ok': bool(night) and sum(night) / len(night) > 0, 'prog': None},
        {'k': '계좌 수익 > 0', 'v': '-' if kret is None else f'{kret:+.2f}%', 'ok': kret is not None and kret > 0, 'prog': None},
        {'k': '최대 낙폭 −20% 이내', 'v': f"{p['mdd']:.1f}%", 'ok': p['mdd'] >= -20, 'prog': None}]


def dash(cfg):
    """대시보드 카드용 요약 — KIS 호출 없음"""
    x = c()
    eq = [dict(r) for r in x.execute('SELECT * FROM kis_equity ORDER BY date')]
    cap = _cap(cfg)
    pos = [dict(r) for r in x.execute('SELECT * FROM kis_pos')]
    closed = [dict(r) for r in x.execute('SELECT ret, pnl FROM kis_closed')]
    t = today()
    o = x.execute("SELECT COUNT(*), SUM(filled>0) FROM kis_orders WHERE date=?", (t,)).fetchone()
    last = eq[-1] if eq else None
    sv = start_value()
    base = sv['value'] if sv else (eq[0]['value'] if eq else None)
    unreal = sum(((p['last_px'] or p['entry_px']) * p['qty'] - p['entry_px'] * p['qty']) for p in pos)
    rets = [r['ret'] for r in closed]
    return {'on': bool(cfg.get('kis_on')), 'configured': bool(cfg.get('kis_app_key') and cfg.get('kis_account')), 'halt': halted(),
            'pause': bool(cfg.get('kis_buy_pause')), 'phase': phase(), 'cap': cap, 'value': last['value'] if last else None,
            'cash': last['cash'] if last else None, 'ret': (last['value'] / base - 1) * 100 if last and base else None,
            'npos': len(pos), 'unreal': unreal, 'realized': sum(r['pnl'] for r in closed) + sum(partial_pnl(p) for p in pos),
            'closed': len(closed), 'win': sum(v > 0 for v in rets) / len(rets) * 100 if rets else None,
            'avg': sum(rets) / len(rets) if rets else None, 'orders_today': o[0] or 0, 'filled_today': o[1] or 0,
            'curve': [[r['date'], r['value']] for r in eq], 'block_new': db.meta_get('kis_block_new', ''),
            'next': next((s['time'] + ' ' + s['what'] for s in schedule(cfg) if s['state'] in ('대기', '진행 중')), '')}


def status(cfg):
    x = c()
    expire_limits()
    pos = [dict(r) for r in x.execute('SELECT * FROM kis_pos ORDER BY strat, entry_date')]
    for p in pos:
        lp = p['last_px'] or p['entry_px']
        st = p['strat'] or 'H1'
        p['strat'] = st
        p['part_pnl'] = partial_pnl(p)
        p['eval_won'] = (lp - p['entry_px']) * p['qty']                              # 남은 수량 평가 손익
        p['eval'] = (lp / p['entry_px'] - 1) * 100 if p['entry_px'] else None
        p['stop_px'] = p['entry_px'] * (1 - STOP / 100) if st == 'H1' else None
        p['tp_px'] = tick_up(p['entry_px'] * (1 + eng.TP / 100)) if st == 'H1' else None
        p['trail_px'] = p['peak'] * (1 - TRAIL / 100) if st == 'H1' and p['part_done'] and p['peak'] else None
        p['max_days'] = HOLD if st == 'H1' else (S5_MAXD if st == 'S5' else 1)
        p['open_orders'] = [dict(r) for r in x.execute("SELECT id, kind, qty, price, status FROM kis_orders WHERE ticker=? AND date=? AND status IN ('접수','부분')", (p['ticker'], today()))]
    closed = [dict(r) for r in x.execute('SELECT * FROM kis_closed ORDER BY id DESC LIMIT 300')]
    rets = [r['ret'] for r in closed]
    return {'on': bool(cfg.get('kis_on')), 'cap': _cap(cfg), 'halt': halted(), 'block_new': db.meta_get('kis_block_new', ''),
            'pause': bool(cfg.get('kis_buy_pause')), 'phase': phase(), 'schedule': schedule(cfg), 'preview': preview(cfg),
            'configured': bool(cfg.get('kis_app_key') and cfg.get('kis_app_secret') and cfg.get('kis_account')),
            'positions': pos, 'orders': [dict(r) for r in x.execute('SELECT * FROM kis_orders ORDER BY id DESC LIMIT 300')],
            'open_orders': [dict(r) for r in x.execute("SELECT * FROM kis_orders WHERE date=? AND status IN ('보냄','접수','부분','취소요청') ORDER BY id DESC", (today(),))],
            'closed': closed, 'equity': [dict(r) for r in x.execute('SELECT * FROM kis_equity ORDER BY date')],
            'log': [dict(r) for r in x.execute('SELECT * FROM kis_log ORDER BY id DESC LIMIT 300')],
            'stats': {'n': len(rets), 'win': sum(1 for v in rets if v > 0) / len(rets) * 100 if rets else None, 'avg': sum(rets) / len(rets) if rets else None,
                      'pnl': sum(r['pnl'] for r in closed), 'best': max(rets) if rets else None, 'worst': min(rets) if rets else None},
            'strats': strat_summary(cfg), 'alloc': {'h1_pct': pct(cfg, 'H1'), 'night_pct': pct(cfg, 'ON'), 's5_on': on(cfg, 'S5'), 'night_on': on(cfg, 'ON'),
                                                     'core_pct': pct(cfg, 'CORE'), 'core_on': on(cfg, 'CORE'), 'core_n': CORE_N,
                                                     's5_pct': S5_PCT, 's5_max': S5_MAX, 'on_ticker': ON_TICKER, 'on_name': ON_NAME},
            'start': start_value(), 'state': dict(STATE), 'compare': compare(), 'trading_day': is_trading_day(), 'perf': perf(), 'criteria': criteria(cfg), 'dash': dash(cfg)}


def day_lines(d, cfg=None):
    """텔레그램 마감 리포트 (B1.0 — 모의투자 계좌 · 칸별 · 체결 건별 · 실현/평가 손익 · 내일 계획)"""
    x = c()
    cfg = cfg or {}
    fills = [dict(r) for r in x.execute("SELECT * FROM kis_orders WHERE date=? AND filled>0 ORDER BY ts, id", (d,))]
    cl = [dict(r) for r in x.execute('SELECT * FROM kis_closed WHERE exit_date=? ORDER BY id', (d,))]
    eqs = [dict(r) for r in x.execute('SELECT * FROM kis_equity WHERE date<=? ORDER BY date DESC LIMIT 2', (d,))]
    eq = eqs[0] if eqs and eqs[0]['date'] == d else None
    pos = [dict(r) for r in x.execute('SELECT * FROM kis_pos WHERE qty>0 ORDER BY strat, entry_date')]
    if not (fills or pos or eq):
        return []
    L = []
    if eq:
        sv = start_value()
        base = sv['value'] if sv else x.execute('SELECT value FROM kis_equity ORDER BY date LIMIT 1').fetchone()[0]
        prev = eqs[1]['value'] if len(eqs) > 1 else base
        L.append(f"💰 계좌 {eq['value']:,.0f}원 · 오늘 {eq['value'] - prev:+,.0f}원 ({(eq['value'] / prev - 1) * 100:+.2f}%) · 시작 대비 {(eq['value'] / base - 1) * 100:+.2f}%")
        L.append(f"   현금(D+2) {eq['cash'] / 1e4:,.0f}만 · 보유 {len(pos)}종목 · 오늘 실현 {sum(r['pnl'] for r in cl):+,.0f}원")
    L.append('')
    for s_ in strat_summary(cfg):
        L.append(f"{s_['icon']} {s_['name']}{'' if s_['on'] else ' (꺼짐)'}: 보유 {s_['npos']} · 들어간 돈 {s_['invested'] / 1e4:,.0f}만 / 한도 {s_['limit'] / 1e4:,.0f}만 · "
                 f"평가 {s_['unreal']:+,.0f}원 · 누적 실현 {s_['realized']:+,.0f}원" + (f" · 청산 {s_['closed']}건 승률 {s_['win']:.0f}%" if s_['closed'] else ''))
    buys = [f for f in fills if f['side'] == 'buy']
    if buys:
        L.append('\n🟢 매수 체결')
        vh = {r['ticker']: r['entry_px'] for r in db.conn().execute("SELECT ticker, entry_px FROM trades WHERE model='H' AND entry_date=?", (d,))}
        for f in buys:
            gap = f" · 계산 시가 대비 {(f['avg'] / vh[f['ticker']] - 1) * 100:+.2f}%" if (f.get('strat') or 'H1') == 'H1' and f.get('avg') and vh.get(f['ticker']) else ''
            L.append(f" [{f.get('strat') or 'H1'}] {f['name']} {f['filled']}주 @{(f['avg'] or 0):,.0f} = {f['filled'] * (f['avg'] or 0) / 1e4:,.0f}만{gap}")
    sells = [f for f in fills if f['side'] == 'sell']
    if sells:
        L.append('\n🔴 매도 체결')
        for f in sells:
            L.append(f" [{f.get('strat') or 'H1'}] {f['name']} {f['filled']}주 @{(f['avg'] or 0):,.0f} · {KIND.get(f['kind'], f['kind'])}")
    if cl:
        L.append(f"\n💵 청산 {len(cl)}건 · 실현 {sum(r['pnl'] for r in cl):+,.0f}원")
        L += [f" {'🟢' if r['pnl'] > 0 else '🔻'} [{r['strat'] or 'H1'}] {r['name']} {r['ret']:+.2f}% {r['pnl']:+,.0f}원 · {r['reason']}" for r in cl]
    hp = [p for p in pos if (p['strat'] or 'H1') != 'ON']
    if hp:
        L.append('\n📦 보유 (종가 평가)')
        for p in hp:
            lp = p['last_px'] or p['entry_px']
            L.append(f" [{p['strat'] or 'H1'}] {p['name']} {p['qty']}주 {(lp / p['entry_px'] - 1) * 100:+.2f}% ({(lp - p['entry_px']) * p['qty']:+,.0f}원) · {p['days']}일"
                     + (' · 내일 매도' if p['sell_next'] else ''))
    onp = [p for p in pos if p['strat'] == 'ON']
    if onp:
        L.append(f"\n🌙 밤사이: {ON_NAME} {onp[0]['qty']}주 @{onp[0]['entry_px']:,.0f} 보유 → 내일 시가 매도")
    if halted():
        L.append(f'\n⛔ 정지 중: {halted()}')
    if db.meta_get('kis_block_new', ''):
        L.append(f"⚠️ 신규 매수 차단: {db.meta_get('kis_block_new', '')}")
    return L
