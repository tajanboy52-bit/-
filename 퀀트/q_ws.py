"""
q_ws.py — 📡 TK Quant 장중 실시간 (KIS 모의투자 웹소켓 · ws://ops.koreainvestment.com:31000 전용)

· 실시간 체결가 H0STCNT0 — 보유 종목 구독 → 화면의 실시간 평가 · 오늘 손익 (손절은 없음: Scout 검증에서 손절은 모든 모델의 평균 수익을 깎음)
· 실시간 체결 통보 H0STCNI9 — ⚙️ HTS ID를 넣으면 체결 즉시 반영 (없으면 60초 조회)
· 장중(08:30 ~ 15:35 · 거래일)에만 연결 · PINGPONG 응답 · 끊기면 5 → 60초 뒤 다시 · 같은 앱키를 다른 프로그램이 쓰면(ALREADY IN USE) 10분 쉼
· 우량주 앱 B1.2 웹소켓과 같은 방식 (가짜 KIS 서버로 구독 · PINGPONG · 체결 통보 시험)
"""
import json
import threading
import time
from datetime import datetime

import q_db as db
import q_trader as tr

WS_URL = 'ws://ops.koreainvestment.com:31000'
MAX_SUB = 40
PRICE = {}                                   # {ticker: (가격, time.time(), 'HHMMSS')}
STATE = {'on': False, 'connected': False, 'subs': [], 'notice': False, 'ticks': 0, 'err': '', 'reconnects': 0, 'fills': 0, 'since': ''}
_sync_at = [0.0]
_started = [False]


def market_open(n=None):
    n = n or datetime.now()
    return tr.is_trading_day(n.strftime('%Y%m%d')) and '08:30' <= n.strftime('%H:%M') <= '15:35'


def parse(raw):
    """→ ('json', dict) · ('price', [(ticker, price, hhmmss)]) · ('notice', None) · (None, None)"""
    if not raw:
        return None, None
    if raw[0] in '01':
        parts = raw.split('|', 3)
        if len(parts) < 4:
            return None, None
        trid, cnt, body = parts[1], parts[2], parts[3]
        if trid == 'H0STCNT0':
            f = body.split('^')
            try:
                n = max(1, int(cnt))
            except ValueError:
                n = 1
            step = len(f) // n
            out = []
            for i in range(n):
                r = f[i * step:(i + 1) * step]
                if len(r) > 2:
                    try:
                        out.append((r[0], float(r[2]), r[1]))
                    except ValueError:
                        pass
            return 'price', out
        if trid in ('H0STCNI9', 'H0STCNI0'):
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
    return list(dict.fromkeys(l['ticker'] for l in tr.open_lots() if l['status'] == '보유'))[:MAX_SUB]


def on_notice(get_cfg):
    if time.time() - _sync_at[0] < 1.5:
        return
    _sync_at[0] = time.time()
    threading.Thread(target=_do_sync, args=(get_cfg,), daemon=True).start()


def _do_sync(get_cfg):
    time.sleep(1.5)
    try:
        if not tr._lock.acquire(timeout=30):
            return
        try:
            tr.sync(tr.client(get_cfg()), tr.today())
            STATE['fills'] += 1
        finally:
            tr._lock.release()
    except Exception as e:
        db.log(f'[실시간] 체결 통보 처리 오류: {str(e)[:150]}', 'warn')


def _session(get_cfg, ws_mod):
    cfg = get_cfg()
    key = tr.client(cfg).approval_key()
    ws = ws_mod.create_connection(WS_URL, timeout=10)
    ws.settimeout(1.0)
    STATE.update(connected=True, err='', since=datetime.now().strftime('%H:%M:%S'))
    subs, last_rx, last_check = set(), time.time(), 0.0
    notice = False
    try:
        hts = (cfg.get('kis_hts_id') or '').strip()
        if hts:
            ws.send(sub_msg(key, 'H0STCNI9', hts))
            notice = True
        while market_open() and get_cfg().get('kis_app_key'):
            if time.time() - last_check > 5:
                last_check = time.time()
                want = set(wanted())
                for t in sorted(subs - want):
                    ws.send(sub_msg(key, 'H0STCNT0', t, False))
                    subs.discard(t)
                    PRICE.pop(t, None)
                for t in sorted(want - subs):
                    ws.send(sub_msg(key, 'H0STCNT0', t, True))
                    subs.add(t)
                STATE.update(subs=sorted(subs), notice=notice)
            try:
                raw = ws.recv()
            except ws_mod.WebSocketTimeoutException:
                if time.time() - last_rx > 120:
                    raise RuntimeError('2분 동안 받은 것 없음 → 다시 접속')
                continue
            last_rx = time.time()
            if isinstance(raw, bytes):
                raw = raw.decode('utf-8', 'ignore')
            kind, data = parse(raw)
            if kind == 'price':
                for t, px, hh in data:
                    PRICE[t] = (px, time.time(), hh)
                    STATE['ticks'] += 1
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
                    db.log(f"[실시간] 구독 실패 {h.get('tr_id')}: {msg[:80]}", 'warn')
                    if h.get('tr_id') == 'H0STCNI9':
                        notice = False
    finally:
        STATE.update(connected=False, subs=[], notice=False)
        try:
            ws.close()
        except Exception:
            pass


def loop(get_cfg):
    try:
        import websocket as ws_mod
    except ImportError:
        STATE['err'] = 'websocket-client 부품 없음 → 60초 조회로만 (Quant_Run.bat이 설치)'
        return
    back = 5
    while True:
        try:
            cfg = get_cfg()
            if not (cfg.get('kis_app_key') and cfg.get('kis_account') and cfg.get('ws_on', True) and market_open()):
                STATE['on'] = False
                time.sleep(20)
                continue
            STATE['on'] = True
            _session(get_cfg, ws_mod)
            back = 5
        except Exception as e:
            msg = str(e)[:200]
            STATE.update(err=f"{datetime.now():%H:%M:%S} {msg}", reconnects=STATE['reconnects'] + 1)
            wait = 600 if 'ALREADY IN USE' in msg.upper() else back
            db.log(f'[실시간] 연결 끊김: {msg} → {wait}초 뒤 다시', 'warn')
            time.sleep(wait)
            back = min(60, back * 2)
            continue
        time.sleep(5)


def start(get_cfg):
    if not _started[0]:
        _started[0] = True
        threading.Thread(target=loop, args=(get_cfg,), daemon=True, name='quant_ws').start()


def status():
    t = time.time()
    return {**STATE, 'prices': {k: {'px': v[0], 'age': round(t - v[1])} for k, v in PRICE.items()}}
