"""
tk_ws.py — 📡 TK자동매매 장중 실시간 (KIS 웹소켓 · 실전 21000 / 모의 31000)

공식 샘플 규칙: 한 연결 구독 최대 40(+체결통보) · 접속키 POST /oauth2/Approval · PINGPONG은 pong으로 되돌림
· 실시간 체결가 H0STCNT0 — 보유 종목 (화면 실시간 평가 · 장중 계좌 안전장치)
· 체결 통보 실전 H0STCNI0 · 모의 H0STCNI9 (tr_key = HTS ID) — AES-256-CBC 암호화(구독 응답의 key · iv)
  pycryptodome이 있으면 풀어서 바로 화면 · 로그에 표시하고, 없든 있든 곧바로 REST 체결 조회(sync)로 장부 반영 → 장부는 항상 REST가 기준
· 같은 앱키로 다른 프로그램이 웹소켓 사용 중(ALREADY IN USE)이면 10분 쉬고 다시 · 끊기면 5 → 60초 뒤 재접속
"""
import base64
import json
import threading
import time
from datetime import datetime

import tk_db as db

MAX_SUB = 40
NOTICE_COLS = ['CUST_ID', 'ACNT_NO', 'ODER_NO', 'OODER_NO', 'SELN_BYOV_CLS', 'RCTF_CLS', 'ODER_KIND', 'ODER_COND', 'STCK_SHRN_ISCD',
               'CNTG_QTY', 'CNTG_UNPR', 'STCK_CNTG_HOUR', 'RFUS_YN', 'CNTG_YN', 'ACPT_YN', 'BRNC_NO', 'ODER_QTY', 'ACNT_NAME',
               'ORD_COND_PRC', 'ORD_EXG_GB', 'POPUP_YN', 'FILLER', 'CRDT_CLS', 'CRDT_LOAN_DATE', 'CNTG_ISNM40', 'ODER_PRC']
PRICE = {}                                   # {ticker: (가격, time.time(), 등락률)}
EXP = {}                                     # 장전 예상체결 {ticker: (예상가, time.time(), 전일 대비 %)} — H0STANC0 (08:30~09:00 갭 확인용)
TICK_HOOKS = []                              # 체결마다 부를 함수 (장중 칸 1분봉 만들기)
NOTICES = []                                 # 최근 체결 통보 (화면용)
STATE = {'on': False, 'connected': False, 'subs': [], 'notice': False, 'decrypt': False, 'ticks': 0, 'err': '', 'reconnects': 0, 'since': ''}
_keys = {}
_hooks = {'on_notice': None, 'client': None, 'wanted': None, 'market_open': None, 'wanted_exp': None}
_sync_at = [0.0]
_started = [False]


def aes_dec(key, iv, text):
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad
    c = AES.new(key.encode('utf-8'), AES.MODE_CBC, iv.encode('utf-8'))
    return unpad(c.decrypt(base64.b64decode(text)), AES.block_size).decode('utf-8')


def parse(raw):
    """→ ('json', dict) · ('price', [(ticker, price, chg)]) · ('notice', dict|None) · (None, None)"""
    if not raw:
        return None, None
    if raw[0] in '01':
        parts = raw.split('|', 3)
        if len(parts) < 4:
            return None, None
        trid, cnt, body = parts[1], parts[2], parts[3]
        if trid == 'H0STANC0':                                                     # 장전 예상체결: 0 종목 · 2 예상가 · 5 전일 대비 %
            f = body.split('^')
            try:
                n = max(1, int(cnt))
            except ValueError:
                n = 1
            step = len(f) // n
            out = []
            for i in range(n):
                r = f[i * step:(i + 1) * step]
                if len(r) > 5:
                    try:
                        out.append((r[0], float(r[2]), float(r[5] or 0)))
                    except ValueError:
                        pass
            return 'exp', out
        if trid == 'H0STCNT0':                                                     # 0 종목 · 1 체결시각 · 2 체결가 · 5 전일 대비 % · 12 체결량
            f = body.split('^')
            try:
                n = max(1, int(cnt))
            except ValueError:
                n = 1
            step = len(f) // n
            out = []
            for i in range(n):
                r = f[i * step:(i + 1) * step]
                if len(r) > 5:
                    try:
                        vol = int(float(r[12] or 0)) if len(r) > 12 else 0
                        out.append((r[0], float(r[2]), float(r[5] or 0), vol, r[1]))
                    except ValueError:
                        pass
            return 'price', out
        if trid in ('H0STCNI0', 'H0STCNI9'):
            k = _keys.get(trid)
            if raw[0] == '1' and k:
                try:
                    f = aes_dec(k[0], k[1], body).split('^')
                    return 'notice', dict(zip(NOTICE_COLS, f))
                except Exception:
                    return 'notice', None
            return 'notice', None
        return None, None
    try:
        return 'json', json.loads(raw)
    except ValueError:
        return None, None


def sub_msg(key, tr_id, tr_key, on=True):
    return json.dumps({'header': {'approval_key': key, 'custtype': 'P', 'tr_type': '1' if on else '2', 'content-type': 'utf-8'},
                       'body': {'input': {'tr_id': tr_id, 'tr_key': tr_key}}})


def _notice(d):
    if d:
        filled = d.get('CNTG_YN') == '2'
        side = '매도' if d.get('SELN_BYOV_CLS') == '01' else '매수'
        row = {'t': datetime.now().strftime('%H:%M:%S'), 'ticker': d.get('STCK_SHRN_ISCD', ''), 'name': (d.get('CNTG_ISNM40') or '').strip(),
               'side': side, 'qty': d.get('CNTG_QTY'), 'price': d.get('CNTG_UNPR'), 'kind': '체결' if filled else ('거부' if d.get('RFUS_YN') == 'Y' else '접수'),
               'order_no': d.get('ODER_NO')}
        NOTICES.insert(0, row)
        del NOTICES[60:]
        if filled:
            db.log(f"[실시간] {side} 체결 통보 {row['name']} {row['qty']}주 @ {row['price']}")
            try:
                import tk_journal
                tk_journal.ws_exec({**row, 'side': 'sell' if side == '매도' else 'buy', 'exec_time': d.get('STCK_CNTG_HOUR', '')})
            except Exception:
                pass
    if time.time() - _sync_at[0] < 1.5:
        return
    _sync_at[0] = time.time()
    if _hooks['on_notice']:
        threading.Thread(target=_hooks['on_notice'], daemon=True).start()


def _session(ws_mod):
    kc = _hooks['client']()
    key = kc.approval_key()
    ws = ws_mod.create_connection(kc.ws_url, timeout=10)
    ws.settimeout(1.0)
    STATE.update(connected=True, err='', since=datetime.now().strftime('%H:%M:%S'))
    subs, last_rx, last_check, notice = set(), time.time(), 0.0, False
    xsubs = set()
    try:
        STATE['decrypt'] = True
        try:
            import Crypto.Cipher.AES  # noqa: F401
        except Exception:
            STATE['decrypt'] = False
        if kc.hts_id:
            ws.send(sub_msg(key, kc.notice_tr, kc.hts_id))
            notice = True
        while _hooks['market_open']():
            if time.time() - last_check > 5:
                last_check = time.time()
                want = set(_hooks['wanted']()[:MAX_SUB])
                for t in sorted(subs - want):
                    ws.send(sub_msg(key, 'H0STCNT0', t, False))
                    subs.discard(t)
                    PRICE.pop(t, None)
                for t in sorted(want - subs):
                    ws.send(sub_msg(key, 'H0STCNT0', t, True))
                    subs.add(t)
                wx = set((_hooks['wanted_exp']() if _hooks['wanted_exp'] else [])[:max(0, MAX_SUB - len(subs))])
                for t in sorted(xsubs - wx):
                    ws.send(sub_msg(key, 'H0STANC0', t, False))
                    xsubs.discard(t)
                for t in sorted(wx - xsubs):
                    ws.send(sub_msg(key, 'H0STANC0', t, True))
                    xsubs.add(t)
                STATE.update(subs=sorted(subs), exp_subs=sorted(xsubs), notice=notice)
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
                for t, px, chg, vol, hms in data:
                    PRICE[t] = (px, time.time(), chg)
                    STATE['ticks'] += 1
                    if TICK_HOOKS:
                        for fn in TICK_HOOKS:
                            try:
                                fn(t, px, vol, hms)
                            except Exception:
                                pass
            elif kind == 'exp':
                for t, px, chg in data:
                    EXP[t] = (px, time.time(), chg)
            elif kind == 'notice':
                _notice(data)
            elif kind == 'json':
                h, b = data.get('header') or {}, data.get('body') or {}
                if h.get('tr_id') == 'PINGPONG':
                    ws.pong(raw)
                    continue
                out = b.get('output') or {}
                if out.get('key') and out.get('iv'):
                    _keys[h.get('tr_id')] = (out['key'], out['iv'])
                msg = str(b.get('msg1', ''))
                if 'ALREADY IN USE' in msg.upper():
                    raise RuntimeError('ALREADY IN USE — 같은 앱키로 다른 프로그램이 웹소켓 사용 중')
                if str(b.get('rt_cd', '0')) not in ('0', ''):
                    db.log(f"[실시간] 구독 실패 {h.get('tr_id')}: {msg[:80]}", 'warn')
                    if h.get('tr_id') in ('H0STCNI0', 'H0STCNI9'):
                        notice = False
    finally:
        STATE.update(connected=False, subs=[], notice=False)
        try:
            ws.close()
        except Exception:
            pass


def loop(enabled):
    try:
        import websocket as ws_mod
    except ImportError:
        STATE['err'] = 'websocket-client 부품 없음 → 60초 조회로만 (TK_Run.bat이 설치)'
        return
    back = 5
    while True:
        try:
            if not (enabled() and _hooks['market_open']()):
                STATE['on'] = False
                time.sleep(15)
                continue
            STATE['on'] = True
            _session(ws_mod)
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


def exp_gap(t, max_age=90):
    """웹소켓 장전 예상체결 → 전일 대비 갭 % (90초 넘게 묵었거나 없으면 None)"""
    v = EXP.get(t)
    return v[2] if v and v[0] and time.time() - v[1] <= max_age else None


def start(enabled, client, wanted, market_open, on_notice, wanted_exp=None):
    _hooks.update(client=client, wanted=wanted, market_open=market_open, on_notice=on_notice, wanted_exp=wanted_exp)
    if not _started[0]:
        _started[0] = True
        threading.Thread(target=loop, args=(enabled,), daemon=True, name='tk_ws').start()


def status():
    t = time.time()
    return {**STATE, 'prices': {k: {'px': v[0], 'age': round(t - v[1]), 'chg': v[2]} for k, v in PRICE.items()}, 'notices': NOTICES[:20]}
