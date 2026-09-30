"""
bluechip_ws.py — 📡 KIS 모의투자 실시간 웹소켓 (B1.2)

· 실시간 체결가 H0STCNT0 — 보유 종목(+ 밤사이 ETF) 구독 → 틱이 올 때마다 −15% 재난 손절을 바로 판단
  (예전 30초 감시는 예비로 남김: 웹소켓 시세가 60초 안에 온 종목은 KIS 조회 없이 그 값으로, 아니면 조회)
· 실시간 체결 통보 H0STCNI9(모의투자) — ⚙️ 설정에 HTS ID를 넣으면 구독 → 통보가 오면 곧바로 체결 반영(sync)
  · 매수 체결이면 +5% 익절 지정가도 바로 넣음 (예전엔 최대 3분 늦음). 통보 내용은 풀지 않고 '신호'로만 씀 → 복호화 부품 필요 없음
· 모의투자 주소만 씀: ws://ops.koreainvestment.com:31000 (실전 21000 없음) · 접속키 POST /oauth2/Approval
· 장중(08:30 ~ 15:35 · 거래일)에만 연결 · PINGPONG 응답 · 끊기면 5 → 10 → … → 60초 뒤 다시 접속
· 같은 앱키로 다른 프로그램이 웹소켓을 쓰고 있으면(ALREADY IN USE) 10분 쉬고 다시 시도 → 그동안 30초 조회로 동작
· 매매 규칙은 바꾸지 않음 — 같은 규칙을 더 빨리 · 정확히 실행하는 용도
"""
import json
import threading
import time
from datetime import datetime

import bluechip_db as db
import bluechip_broker as brk

WS_URL = 'ws://ops.koreainvestment.com:31000'          # 모의투자 전용
MAX_SUB = 40                                           # KIS 한 세션 구독 한도 41 (체결 통보 1칸 남김)
FRESH = 60                                             # 이 초 안에 시세가 온 종목은 30초 조회를 건너뜀
PRICE = {}                                             # {ticker: (가격, time.time(), 'HHMMSS')}
STATE = {'on': False, 'connected': False, 'subs': [], 'notice': False, 'ticks': 0, 'last_tick': '', 'err': '', 'reconnects': 0,
         'stops': 0, 'fills': 0, 'since': ''}
_stopping = set()
_sync_at = [0.0]
_started = [False]


def log(msg, level='info'):
    try:
        brk.log(f'[실시간] {msg}', level)
    except Exception:
        pass


def fresh(sec=FRESH):
    """웹소켓 시세가 sec초 안에 들어온 종목"""
    t = time.time()
    return {k for k, v in PRICE.items() if t - v[1] <= sec}


def market_open(n=None):
    n = n or datetime.now()
    return brk.is_trading_day(n.strftime('%Y%m%d')) and '08:30' <= n.strftime('%H:%M') <= '15:35'


# ════════════════════════════════════════════
#  메시지 해석
# ════════════════════════════════════════════
def parse(raw):
    """→ ('json', dict) · ('price', [(ticker, price, hhmmss)]) · ('notice', None) · (None, None)"""
    if not raw:
        return None, None
    if raw[0] in '01':
        parts = raw.split('|', 3)
        if len(parts) < 4:
            return None, None
        tr, cnt, body = parts[1], parts[2], parts[3]
        if tr == 'H0STCNT0':
            f = body.split('^')
            try:
                n = max(1, int(cnt))
            except ValueError:
                n = 1
            step = len(f) // n if n else len(f)
            out = []
            for i in range(n):
                r = f[i * step:(i + 1) * step]
                if len(r) > 2:
                    try:
                        out.append((r[0], float(r[2]), r[1]))
                    except ValueError:
                        pass
            return 'price', out
        if tr in ('H0STCNI9', 'H0STCNI0'):
            return 'notice', None
        return None, None
    try:
        return 'json', json.loads(raw)
    except ValueError:
        return None, None


def sub_msg(key, tr_id, tr_key, on=True):
    return json.dumps({'header': {'approval_key': key, 'custtype': 'P', 'tr_type': '1' if on else '2', 'content-type': 'utf-8'},
                       'body': {'input': {'tr_id': tr_id, 'tr_key': tr_key}}})


def wanted():
    """구독할 종목: 앱 보유 종목 (H1 먼저) + 밤사이 ETF"""
    x = brk.c()
    tks = [r[0] for r in x.execute("SELECT ticker FROM kis_pos WHERE qty>0 ORDER BY CASE COALESCE(strat,'H1') WHEN 'H1' THEN 0 ELSE 1 END, entry_date")]
    return list(dict.fromkeys(tks))[:MAX_SUB]


# ════════════════════════════════════════════
#  틱 · 통보 처리 (주문은 브로커 잠금 안에서, 웹소켓 받는 줄은 막지 않게 별도 스레드)
# ════════════════════════════════════════════
_pos_cache = {'t': 0.0, 'rows': {}}


def _positions():
    if time.time() - _pos_cache['t'] > 3:
        _pos_cache['rows'] = {r['ticker']: dict(r) for r in brk.c().execute("SELECT * FROM kis_pos WHERE qty>0")}
        _pos_cache['t'] = time.time()
    return _pos_cache['rows']


def on_tick(get_cfg, ticker, px, hhmmss):
    PRICE[ticker] = (px, time.time(), hhmmss)
    STATE['ticks'] += 1
    STATE['last_tick'] = f'{ticker} {px:,.0f} {hhmmss}'
    p = _positions().get(ticker)
    if not p or ticker in _stopping or not brk.stop_hit(p, px):
        return
    hm = datetime.now().strftime('%H:%M')
    cfg = get_cfg()
    if not ('09:00' <= hm <= '15:19' and brk.can_order(cfg)):
        return
    _stopping.add(ticker)
    threading.Thread(target=_do_stop, args=(get_cfg, ticker, px), daemon=True).start()


def _do_stop(get_cfg, ticker, px):
    try:
        if not brk._lock.acquire(timeout=30):
            log(f'{ticker} 손절 — 다른 작업이 30초 넘게 잠금 중 → 30초 조회가 이어서 처리', 'warn')
            return
        try:
            p = brk.c().execute("SELECT * FROM kis_pos WHERE ticker=? AND qty>0", (ticker,)).fetchone()
            p = dict(p) if p else None
            if p and brk.stop_hit(p, px):                              # 잠금 기다리는 사이 30초 조회가 먼저 냈으면 status가 바뀜
                if brk.try_stop(get_cfg(), brk.client(get_cfg()), p, px, '실시간 체결가'):
                    STATE['stops'] += 1
                _pos_cache['t'] = 0
        finally:
            brk._lock.release()
    except Exception as e:
        log(f'{ticker} 실시간 손절 처리 오류: {str(e)[:150]}', 'error')
    finally:
        _stopping.discard(ticker)


def on_notice(get_cfg):
    """체결 통보 → 1.5초 모아서 sync 한 번 (여러 통보가 한꺼번에 와도 한 번)"""
    now_ = time.time()
    if now_ - _sync_at[0] < 1.5:
        return
    _sync_at[0] = now_
    threading.Thread(target=_do_sync, args=(get_cfg,), daemon=True).start()


def _do_sync(get_cfg):
    time.sleep(1.5)
    try:
        if not brk._lock.acquire(timeout=30):
            return
        try:
            cfg = get_cfg()
            kc = brk.client(cfg)
            d = brk.today()
            before = brk.c().execute('SELECT COUNT(*) FROM kis_pos').fetchone()[0]
            brk.sync(kc, d)
            STATE['fills'] += 1
            _pos_cache['t'] = 0
            hm = datetime.now().strftime('%H:%M')
            if '09:03' <= hm <= '15:00' and brk.can_order(cfg):
                brk.place_tp(cfg, kc, d)                               # 방금 체결된 매수에 +5% 익절 지정가 바로
            if brk.c().execute('SELECT COUNT(*) FROM kis_pos').fetchone()[0] != before:
                log('체결 통보 → 보유 종목 바뀜, 구독 다시 맞춤')
        finally:
            brk._lock.release()
    except Exception as e:
        log(f'체결 통보 처리 오류: {str(e)[:150]}', 'warn')


# ════════════════════════════════════════════
#  연결 루프
# ════════════════════════════════════════════
def _session(get_cfg, ws_mod):
    cfg = get_cfg()
    kc = brk.client(cfg)
    key = kc.approval_key()
    ws = ws_mod.create_connection(WS_URL, timeout=10)
    ws.settimeout(1.0)
    STATE.update(connected=True, err='', since=datetime.now().strftime('%H:%M:%S'))
    subs, last_rx, last_check = set(), time.time(), 0.0
    hts = (cfg.get('kis_hts_id') or '').strip()
    notice_on = False
    try:
        if hts:
            ws.send(sub_msg(key, 'H0STCNI9', hts))
            notice_on = True
        while market_open() and get_cfg().get('kis_app_key'):
            if time.time() - last_check > 5:                           # 5초마다 구독 목록 맞춤
                last_check = time.time()
                want = set(wanted())
                for t in sorted(subs - want):
                    ws.send(sub_msg(key, 'H0STCNT0', t, False))
                    subs.discard(t)
                    PRICE.pop(t, None)
                for t in sorted(want - subs):
                    ws.send(sub_msg(key, 'H0STCNT0', t, True))
                    subs.add(t)
                STATE['subs'] = sorted(subs)
                STATE['notice'] = notice_on
            try:
                raw = ws.recv()
            except ws_mod.WebSocketTimeoutException:
                if time.time() - last_rx > 120:
                    raise RuntimeError('2분 동안 받은 것 없음 (PINGPONG 포함) → 다시 접속')
                continue
            last_rx = time.time()
            if isinstance(raw, bytes):
                raw = raw.decode('utf-8', 'ignore')
            kind, data = parse(raw)
            if kind == 'price':
                for tk, px, hh in data:
                    on_tick(get_cfg, tk, px, hh)
            elif kind == 'notice':
                on_notice(get_cfg)
            elif kind == 'json':
                h, b = data.get('header') or {}, data.get('body') or {}
                if h.get('tr_id') == 'PINGPONG':
                    ws.pong(raw)
                    continue
                msg = str(b.get('msg1', ''))
                if 'ALREADY IN USE' in msg.upper():
                    raise RuntimeError('ALREADY IN USE — 같은 앱키로 다른 프로그램이 웹소켓 사용 중')
                if str(b.get('rt_cd', '0')) not in ('0', ''):
                    log(f"구독 실패 {h.get('tr_id')} {h.get('tr_key', '')[-6:]}: {msg[:80]}", 'warn')
                    if h.get('tr_id') == 'H0STCNI9':
                        notice_on = False
    finally:
        STATE.update(connected=False, subs=[], notice=False)
        try:
            ws.close()
        except Exception:
            pass


def loop(get_cfg):
    try:
        import websocket as ws_mod                                     # pip install websocket-client
    except ImportError:
        STATE['err'] = 'websocket-client 부품 없음 → 30초 조회로만 동작 (Bluechip_Run.bat이 다음 실행 때 설치)'
        log(STATE['err'], 'warn')
        return
    back = 5
    while True:
        try:
            cfg = get_cfg()
            active = cfg.get('kis_app_key') and cfg.get('kis_account') and cfg.get('ws_on', True) and \
                (cfg.get('kis_on') or brk.c().execute('SELECT COUNT(*) FROM kis_pos WHERE qty>0').fetchone()[0])
            if not (active and market_open()):
                STATE['on'] = False
                time.sleep(20)
                continue
            STATE['on'] = True
            _session(get_cfg, ws_mod)
            back = 5
        except Exception as e:
            msg = str(e)[:200]
            STATE['err'] = f"{datetime.now():%H:%M:%S} {msg}"
            STATE['reconnects'] += 1
            wait = 600 if 'ALREADY IN USE' in msg.upper() else back
            log(f'연결 끊김: {msg} → {wait}초 뒤 다시 (그동안 30초 조회로 손절 감시)', 'warn')
            time.sleep(wait)
            back = min(60, back * 2)
            continue
        time.sleep(5)


def start(get_cfg):
    if _started[0]:
        return
    _started[0] = True
    threading.Thread(target=loop, args=(get_cfg,), daemon=True, name='bluechip_ws').start()


def status():
    t = time.time()
    return {**STATE, 'prices': {k: {'px': v[0], 'age': round(t - v[1]), 'time': v[2]} for k, v in PRICE.items()}}
