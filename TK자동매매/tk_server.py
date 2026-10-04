"""
tk_server.py — 台炅 TK자동매매 시스템 · http://127.0.0.1:8086 · 한국투자증권 Open API (모의 → 실전)

다른 앱 없이 혼자 돌아감: 자료 수집(KRX · KIS) → 신호 → 주문 → 체결 · 장부 → 리포트
보안 (v8 점검에서 나온 문제를 막음)
 · 이 PC 안에서만 접속 (127.0.0.1) · CORS 없음 · Host 머리글 확인(DNS 재바인딩 차단)
 · 모든 /api 호출은 실행 때마다 새로 만드는 세션 토큰(X-TK-Token) 필요 · 바꾸는 호출은 JSON만 받음 → 다른 사이트가 몰래 요청 못 함
 · 비밀 값은 DPAPI 암호화 저장 · 화면 · 로그 · zip에서 가림
"""
import asyncio
import csv
import io
import json
import os
import secrets
import sys
import threading
import time
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

SSL_MODE = '파이썬 기본 인증서'
try:                                                                             # HTTPS 인증서를 윈도우와 똑같이 확인 (백신 · 보안 프로그램의 HTTPS 검사 인증서도 신뢰)
    import truststore
    truststore.inject_into_ssl()
    SSL_MODE = '윈도우 인증서 저장소 (truststore)'
except Exception:
    pass

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

import tk_collect as col
import tk_config as CF
import tk_analyze as AN
import tk_db as db
import tk_journal as J
import tk_kis
import tk_minute as mn
import tk_brief as BR
import tk_dart as DART
import tk_export as XP
import tk_stock as SK
import tk_intraday as IL
import tk_shadow as SH
import tk_signals as S
import tk_trader as tr
import tk_ws as rtws

APP_NAME = 'TK자동매매 시스템'
APP_VERSION = 'T1.2'
PORT = int(os.environ.get('TKAUTO_PORT', '8086'))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TOKEN = secrets.token_urlsafe(24)
CFG = CF.load()
db.set_mode(CFG.get('mode', 'paper'))
JOB = {'signal': False, 'signal_msg': '', 'signal_err': '', 'bt': False, 'bt_msg': '', 'collect_msg': '', 'an': False, 'an_msg': '', 'bf': False, 'bf_msg': ''}
ANALYSIS = {'res': None}
app = FastAPI(title=APP_NAME, docs_url=None, redoc_url=None, openapi_url=None)


@app.middleware('http')
async def guard(request: Request, call_next):
    host = (request.headers.get('host') or '').split(':')[0]
    if host not in ('127.0.0.1', 'localhost'):
        return JSONResponse({'ok': False, 'error': '이 PC에서만 접속할 수 있습니다'}, 403)
    if request.url.path.startswith('/api/'):
        if not secrets.compare_digest(request.headers.get('x-tk-token', ''), TOKEN):
            return JSONResponse({'ok': False, 'error': '세션 토큰 없음 — 화면을 새로 고치세요'}, 401)
        if request.method == 'POST' and 'application/json' not in (request.headers.get('content-type') or ''):
            return JSONResponse({'ok': False, 'error': 'JSON 요청만 받습니다'}, 415)
    resp = await call_next(request)
    resp.headers['X-Frame-Options'] = 'DENY'
    resp.headers['Cache-Control'] = 'no-store'
    return resp


TG_SSL_HINT = (' → PC의 백신 · 보안 프로그램이 HTTPS를 검사하는 중 (인증서 끼워 넣음). TK_Run.bat을 다시 실행하면 truststore 부품이 설치되어 '
               '윈도우 인증서로 확인합니다 · 그래도 안 되면 백신의 "HTTPS/SSL 검사"에서 api.telegram.org 예외 추가')


def telegram(msg, head=None):
    t, ch = CFG.get('telegram_token'), CFG.get('telegram_chat')
    if not (t and ch):
        return False, '텔레그램 미설정'
    try:
        if head is None:                                                         # 브리핑은 자체 머리글(모드 포함)이 있으므로 붙이지 않음
            head = '' if msg.startswith(('☀', '🕐', '🌙')) else '[TK' + ('·실전' if db.mode() == 'real' else '·모의') + '] '
        body = urllib.parse.urlencode({'chat_id': ch, 'text': head + msg[:3900]}).encode()
        with urllib.request.urlopen(f'https://api.telegram.org/bot{t}/sendMessage', data=body, timeout=10) as r:
            ok = bool(json.loads(r.read().decode()).get('ok'))
        db.gmeta_set('tg_check', f"{datetime.now():%m-%d %H:%M} {'ok' if ok else 'fail 응답 ok=false'}")
        return ok, ''
    except Exception as e:
        db.log(f'텔레그램 실패: {CF.clean(e)}' + (TG_SSL_HINT if 'CERTIFICATE' in str(e) else ''), 'warn')
        db.gmeta_set('tg_check', f'{datetime.now():%m-%d %H:%M} fail {CF.clean(e)[:120]}')
        return False, CF.clean(e)


def secret_status():
    """화면용: KRX · 텔레그램이 저장됐는지(가린 값) · 마지막 확인 결과 — 비밀 값 자체는 내보내지 않음"""
    def chk(k):
        v = db.gmeta_get(k) or ''
        p = v.split(' ', 3)
        return {'at': ' '.join(p[:2]), 'ok': len(p) > 2 and p[2] == 'ok', 'msg': p[3] if len(p) > 3 else ''} if v else None
    return {'krx': {'id': CF.mask(CFG.get('krx_id'), 2), 'pw': bool(CFG.get('krx_pw')), 'check': chk('krx_check')},
            'tg': {'token': CF.mask(CFG.get('telegram_token')), 'chat': CF.mask(CFG.get('telegram_chat'), 3), 'check': chk('tg_check')},
            'dart': {'key': CF.mask(CFG.get('dart_key')), 'check': chk('dart_check')}}


def krx_test():
    """KRX 로그인 → 마지막 거래일 전종목 일봉 한 번 조회 (자료 수집과 같은 경로)"""
    if not (CFG.get('krx_id') and CFG.get('krx_pw')):
        raise ValueError('KRX 아이디 · 비밀번호가 저장되어 있지 않음')
    try:
        stock = col.krx(CFG)
        d = db.last_bar_day() or tr.prev_trading_day(datetime.now().strftime('%Y%m%d'))
        df = stock.get_market_ohlcv_by_ticker(d, 'ALL')
        n = 0 if df is None else len(df)
        if not n:
            raise RuntimeError(f'{d} 자료 0건 — 아이디 · 비밀번호 또는 KRX 사이트 확인')
        db.gmeta_set('krx_check', f'{datetime.now():%m-%d %H:%M} ok {d} {n}종목')
        return {'day': d, 'n': n}
    except Exception as e:
        db.gmeta_set('krx_check', f'{datetime.now():%m-%d %H:%M} fail {CF.clean(e)[:120]}')
        col.STOCK[0] = None
        raise


# ════════════════════════════════════════════
#  일정: 자료 수집 → 신호 → 리포트
# ════════════════════════════════════════════
def collect_run(full=False):
    if col.STATE['running']:
        return False
    kc = None
    try:
        if tr.configured(CFG):
            kc = tr.client(CFG)
    except Exception:
        kc = None
    JOB['collect_msg'] = ''                                                       # 지난 시도의 결과 · 오류는 지움 (새로 받는 중에 옛 오류가 같이 보이지 않게)
    try:
        r = col.run(CFG, kc=kc, full=full)
        JOB['collect_msg'] = f"끝 {datetime.now():%H:%M} · {r}"
        if r.get('src') != 'kis':
            db.gmeta_set('krx_check', f"{datetime.now():%m-%d %H:%M} ok 자료 수집 성공 (마지막 {r.get('last', '')})")
        return True
    except Exception as e:
        JOB['collect_msg'] = f'{datetime.now():%H:%M} 오류: {CF.clean(e)}'
        tr.alert(f'자료 수집 실패 — {CF.clean(e)}', 'collect')
        return False


def signal_run(d):
    if JOB['signal']:
        return False
    JOB.update(signal=True, signal_msg=f'{d} 신호 계산 중', signal_err='')
    try:
        tr.signal_job(CFG, d)
        JOB['signal_msg'] = f'{d} 신호 계산 끝 {datetime.now():%H:%M}'
        n = 0
        try:                                                                     # 📅 실전: 다음 거래일 매도를 예약주문으로 (PC가 아침에 꺼져 있어도)
            if tr.configured(CFG) and tr.can_order(CFG) and '15:40' <= datetime.now().strftime('%H:%M') <= '23:30':
                kc = tr.client(CFG)
                if tr.resv_supported(kc):
                    with tr._lock:
                        n = tr.reserve_sells(CFG, kc, tr.next_trading_day(d))
        except Exception as e:
            db.log(f'예약주문 오류: {CF.clean(e)}', 'warn')
        if CFG.get('hourly_report', True) and db.gmeta_get('brief_close') != d:  # 🌙 장마감 브리핑 (오늘 결과 + 내일 계획)
            db.gmeta_set('brief_close', d)
            telegram(BR.closing(CFG, d, True, n))
        try:                                                                     # 날마다 거래 분석 갱신 (보고서 파일 · 화면)
            ANALYSIS['res'] = AN.analyze()
            open(os.path.join(db.DATA_DIR, 'analysis_result.md'), 'w', encoding='utf-8').write(AN.report_md(ANALYSIS['res']))
        except Exception as e:
            db.log(f'거래 분석 실패: {CF.clean(e)}', 'warn')
        if CFG.get('shadow_on', True):                                          # 👥 그림자 운용 (설정 몇 개를 가상으로 나란히)
            SH.run(CFG)
        if CFG.get('intraday_lab', True) and mn.status()['days']:              # ⏱ 장중 연구실 (분봉이 쌓일수록 판정이 바뀜)
            IL.run(CFG)
        return True
    except Exception as e:
        JOB['signal_err'] = CF.clean(e)
        db.log(f'신호 계산 실패: {CF.clean(e)}', 'error')
        tr.alert(f'신호 계산 실패 — {CF.clean(e)}', 'signal')
        return False
    finally:
        JOB['signal'] = False


def minute_client():
    """분봉 조회용 — 실전 키가 있으면 실전 도메인(초당 20건 · 시세 조회만), 없으면 지금 모드"""
    a = CF.acct(CFG, 'real')
    if a.get('app_key') and a.get('app_secret'):
        return tk_kis.KIS('real', a['app_key'], a['app_secret'], a.get('account') or '', db.DATA_DIR)
    return tr.client(CFG)


def minute_window():
    """과거 분봉 채우기는 장 시간을 피해서: 평일 18:30~07:00 · 주말 · 휴장일"""
    n = datetime.now()
    hm = n.strftime('%H:%M')
    return not tr.is_trading_day(n.strftime('%Y%m%d')) or hm >= '18:30' or hm < '07:00'


def minute_run(kind):
    try:
        kc = minute_client()
        top = int(CFG.get('minute_top') or 200)
        if kind == 'today':
            mn.run_today(kc, datetime.now().strftime('%Y%m%d'), top)
        else:
            mn.run_backfill(kc, int(CFG.get('minute_days') or 250), top, allowed=lambda: minute_window() or kind == 'backfill_now')
    except Exception as e:
        mn.STATE['err'] = CF.clean(e)
        db.log(f'[분봉] {CF.clean(e)}', 'warn')


def scheduler():
    time.sleep(5)
    import_run()                                                                 # 처음 켤 때 내장 자료(seed) · 가져오기 폴더
    last_imp = time.time()
    while True:
        try:
            n = datetime.now()
            if time.time() - last_imp > 600 and not col.STATE['running']:         # 10분마다 가져오기 폴더 확인 → 새 파일 자동 누적
                last_imp = time.time()
                import_run()
                J.flush_api()
            d, hm = n.strftime('%Y%m%d'), n.strftime('%H:%M')
            if '07:40' <= hm <= '08:10' and db.gmeta_get('master_day') != d:
                db.gmeta_set('master_day', d)
                try:
                    k = col.kis_master()
                    db.log(f'KIS 종목 마스터 {k}종목 (거래정지 · 관리 · 경고 표시)')
                except Exception as e:
                    db.log(f'KIS 종목 마스터 실패: {CF.clean(e)}', 'warn')
            if tr.is_trading_day(d) and CFG.get('collect_time', '15:50') <= hm <= '22:00' and db.last_bar_day() < d and not col.STATE['running'] \
                    and time.time() - float(db.gmeta_get('collect_try') or 0) > 1800:                   # 실패하면 30분마다 다시 (22시까지)
                db.gmeta_set('collect_try', time.time())
                threading.Thread(target=collect_run, daemon=True).start()
            if tr.is_trading_day(d) and hm >= '20:30' and CFG.get('hourly_report', True) and db.gmeta_get('brief_close') != d \
                    and not JOB['signal'] and db.conn().execute('SELECT 1 FROM equity WHERE date=?', (d,)).fetchone():   # 신호가 늦으면 내일 계획 없이
                db.gmeta_set('brief_close', d)
                threading.Thread(target=telegram, args=(BR.closing(CFG, d, plan=db.meta_get('last_signal_date') == d),), daemon=True).start()
            if hm >= '16:40' and db.gmeta_get('backup_day') != d:                  # 장부 백업 (날마다 · 기록이 핵심이므로)
                db.gmeta_set('backup_day', d)
                try:
                    db.log(f"장부 백업 {', '.join(db.backup())}")
                except Exception as e:
                    db.log(f'장부 백업 실패: {CF.clean(e)}', 'warn')
            if CFG.get('dart_on', True) and CFG.get('dart_key') and not DART.STATE['running'] and not col.STATE['running'] \
                    and time.time() - float(db.gmeta_get('dart_try') or 0) > 3 * 3600 and (hm >= '18:05' or not tr.is_trading_day(d) or hm < '07:00'):   # 📰 DART: 저녁 오늘 공시 + 과거 이어받기
                if DART.todo_days(int(CFG.get('dart_days') or 250)):
                    db.gmeta_set('dart_try', time.time())
                    threading.Thread(target=lambda: DART.run(CFG.get('dart_key'), int(CFG.get('dart_days') or 250)), daemon=True).start()
            if CFG.get('minute_on', True) and not mn.STATE['running'] and not col.STATE['running']:      # ⏱ 1분봉 수집
                if tr.is_trading_day(d) and hm >= '16:20' and mn.meta_get('today_done') != d and (db.last_bar_day() >= d or hm >= '17:30'):
                    threading.Thread(target=minute_run, args=('today',), daemon=True).start()
                elif minute_window() and time.time() - float(db.gmeta_get('minute_bf_try') or 0) > 1800 and mn.backfill_days(int(CFG.get('minute_days') or 250)):
                    db.gmeta_set('minute_bf_try', time.time())
                    threading.Thread(target=minute_run, args=('backfill',), daemon=True).start()
            pd_ = tr.prev_trading_day(d)                                             # 아침 따라잡기: 어젯밤 PC가 꺼져 있었으면
            if tr.is_trading_day(d) and '06:00' <= hm <= '08:40' and not col.STATE['running'] and not JOB['signal']:
                if db.last_bar_day() < pd_ and time.time() - float(db.gmeta_get('collect_try') or 0) > 900:
                    db.gmeta_set('collect_try', time.time())
                    db.log(f'아침 따라잡기: {pd_} 자료 수집 (어젯밤 놓침)', 'warn')
                    threading.Thread(target=collect_run, daemon=True).start()
                elif db.last_bar_day() >= pd_ and db.meta_get('last_signal_date') < pd_:
                    db.log(f'아침 따라잡기: {pd_} 신호 계산 (어젯밤 놓침)', 'warn')
                    threading.Thread(target=signal_run, args=(pd_,), daemon=True).start()
            if tr.is_trading_day(d) and '18:15' <= hm <= '21:00' and db.gmeta_get('flow_day') != d and not col.STATE['running']:
                db.gmeta_set('flow_day', d)                                          # 수급 확정치 다시 (최근 3거래일)
                threading.Thread(target=collect_run, daemon=True).start()
            if tr.is_trading_day(d) and hm >= CFG.get('signal_time', '18:40') and db.meta_get('last_signal_date') != d \
                    and not JOB['signal'] and not col.STATE['running'] and db.last_bar_day() >= d:
                if db.last_flow_day() >= d or hm >= '20:00':
                    threading.Thread(target=signal_run, args=(d,), daemon=True).start()
        except Exception as e:
            db.log(f'일정 오류: {CF.clean(e)}', 'error')
        time.sleep(30)


# ════════════════════════════════════════════
#  화면 자료
# ════════════════════════════════════════════
def _lots_view():
    x = db.conn()
    live = rtws.PRICE
    lots = [dict(r) for r in x.execute("SELECT * FROM lots WHERE status IN ('주문','보유') ORDER BY sleeve, entry_date")]
    for l in lots:
        lp = (live.get(l['ticker']) or (None,))[0] or l['last_px'] or l['entry_px'] or 0
        l['px'], l['live'] = lp, l['ticker'] in live
        l['cost_left'] = l['cost'] * (l['qty'] / l['qty0']) if l['qty0'] else l['cost']
        l['eval'] = l['qty'] * lp - l['cost_left'] if l['qty'] else 0
        l['eval_pct'] = (lp / l['entry_px'] - 1) * 100 if l['entry_px'] else None
    return lots


def _state():
    x = db.conn()
    d = tr.today()
    eq = [dict(r) for r in x.execute('SELECT * FROM equity ORDER BY date')]
    lots = _lots_view()
    al = tr.alloc(CFG)
    base = min(eq[-1]['value'] if eq else tr.cap(CFG), tr.cap(CFG))
    sleeves = []
    for s, m in S.SLEEVES.items():
        ls = [l for l in lots if l['sleeve'] == s and l['status'] == '보유']
        cl = list(x.execute("SELECT pnl, ret FROM lots WHERE sleeve=? AND status='청산'", (s,)))
        if s == 'MAN':                                                              # ✋ 수동매수: 비율 없음 · 고른 청산 규칙
            v_ = sum(l['qty'] * l['px'] for l in ls)
            sleeves.append({'key': s, **m, 'pct': None, 'limit': v_ or 1, 'value': v_, 'npos': len(ls), 'slots': None, 'on': True, 'mode': '종목분석에서 직접',
                            'eval': sum(l['eval'] for l in ls), 'realized': sum(r[0] or 0 for r in cl), 'closed': len(cl),
                            'win': sum(1 for r in cl if (r[0] or 0) > 0) / len(cl) * 100 if cl else None, 'avg': sum(r[1] or 0 for r in cl) / len(cl) if cl else None})
            continue
        if s == 'IN':                                                               # 장중 칸: 낮에 노는 돈 · 그날 정리
            sleeves.append({'key': s, **m, 'pct': None, 'limit': sum(l['qty'] * l['px'] for l in ls) or 1, 'value': sum(l['qty'] * l['px'] for l in ls), 'npos': len(ls),
                            'slots': int(CFG.get('intraday_slots') or 5), 'on': bool(CFG.get('intraday_on')), 'mode': ', '.join(IL.rules_on(CFG)[0]) or '통과 규칙 없음',
                            'eval': sum(l['eval'] for l in ls), 'realized': sum(r[0] or 0 for r in cl), 'closed': len(cl),
                            'win': sum(1 for r in cl if (r[0] or 0) > 0) / len(cl) * 100 if cl else None, 'avg': sum(r[1] or 0 for r in cl) / len(cl) if cl else None})
            continue
        if s == 'SW':                                                               # 남는 현금 칸: 비율이 아니라 남는 만큼
            v_ = sum(l['qty'] * l['px'] for l in ls)
            sleeves.append({'key': s, **m, 'pct': None, 'limit': v_ or 1, 'value': v_, 'npos': len(ls), 'slots': None, 'on': tr.sweep_on(CFG),
                            'mode': S.SW_MODES.get(CFG.get('sweep_mode') or 'night', ''), 'eval': sum(l['eval'] for l in ls),
                            'realized': sum(r[0] or 0 for r in cl), 'closed': len(cl), 'win': sum(1 for r in cl if (r[0] or 0) > 0) / len(cl) * 100 if cl else None,
                            'avg': sum(r[1] or 0 for r in cl) / len(cl) if cl else None})
            continue
        sleeves.append({'key': s, **m, 'pct': al.get(s, 0), 'limit': base * al.get(s, 0) / 100, 'value': sum(l['qty'] * l['px'] for l in ls),
                        'npos': len(ls), 'slots': tr.slots(CFG).get(s, 1), 'eval': sum(l['eval'] for l in ls), 'realized': sum(r[0] or 0 for r in cl),
                        'closed': len(cl), 'win': sum(1 for r in cl if (r[0] or 0) > 0) / len(cl) * 100 if cl else None,
                        'avg': sum(r[1] or 0 for r in cl) / len(cl) if cl else None})
    sd = db.meta_get('last_signal_date')
    lastq = eq[-1] if eq else None
    pl = tr.plan(CFG, lastq['value'] if lastq else None, lastq['cash'] if lastq else None, sd) if sd else {'sells': [], 'buys': [], 'defer': []}
    closed = [dict(r) for r in x.execute("SELECT * FROM lots WHERE status='청산' ORDER BY exit_date DESC, id DESC LIMIT 500")]
    daily = {}
    for r in closed:
        daily.setdefault(r['exit_date'], 0.0)
        daily[r['exit_date']] += r['pnl'] or 0
    bt = None
    p = os.path.join(db.DATA_DIR, 'backtest_result.json')
    if os.path.exists(p):
        try:
            bt = json.load(open(p, encoding='utf-8'))
        except Exception:
            bt = None
    mc = db.mconn()
    acc = {m: {k: CF.mask(CF.acct(CFG, m).get(k)) for k in CF.ACCOUNT_KEYS} for m in ('paper', 'real')}
    return {'app': APP_NAME, 'version': APP_VERSION, 'now': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'mode': db.mode(),
            'trading_day': tr.is_trading_day(d), 'kis_on': bool(CFG.get('kis_on')), 'configured': tr.configured(CFG), 'conn': conn_status(),
            'halt': tr.halted(), 'auto_pause': db.meta_get('auto_pause'), 'pause_buy': bool(CFG.get('pause_buy')), 'block_new': db.meta_get('block_new'),
            'equity': lastq, 'start_value': float(db.meta_get('start_value') or 0), 'curve': [[r['date'], r['value']] for r in eq],
            'intraday': [[r[0][8:], r[1]] for r in x.execute('SELECT ts, value FROM intraday WHERE ts>=? ORDER BY ts', (d,))],
            'sleeves': sleeves, 'lots': lots, 'closed': closed, 'daily_pnl': sorted(daily.items()),
            'orders': [dict(r) for r in x.execute('SELECT * FROM orders ORDER BY id DESC LIMIT 300')],
            'signal_date': sd, 'signals': [dict(r) for r in x.execute('SELECT * FROM signals WHERE date=? ORDER BY sleeve, rank', (sd,))] if sd else [],
            'plan': {'sells': pl['sells'], 'buys': pl['buys'], 'defer': pl['defer']},
            'data': {'last_bar': db.last_bar_day(), 'last_flow': db.last_flow_day(), 'dq_bad': db.gmeta_get('dq_bad'),
                     'n_stocks': mc.execute('SELECT COUNT(*) FROM stocks').fetchone()[0], 'n_delisted': mc.execute('SELECT COUNT(*) FROM stocks WHERE listed=0').fetchone()[0],
                     'n_days': mc.execute("SELECT COUNT(*) FROM done WHERE kind='bars' AND n>0").fetchone()[0],
                     'first_bar': mc.execute("SELECT MIN(key) FROM done WHERE kind='bars' AND n>0").fetchone()[0],
                     'months': mc.execute("SELECT COUNT(*) FROM done WHERE kind='month'").fetchone()[0],
                     'last_month': mc.execute("SELECT MAX(key) FROM done WHERE kind='month'").fetchone()[0],
                     'etf_last': mc.execute('SELECT MAX(date) FROM etf').fetchone()[0], 'master_at': db.gmeta_get('master_at'),
                     'collect': {**col.STATE, **_eta(col.STATE)}, 'collect_msg': JOB['collect_msg'], 'minute': mn.status(int(CFG.get('minute_days') or 250)),
                     'dart': {**DART.status(int(CFG.get('dart_days') or 250)), 'key': bool(CFG.get('dart_key')), 'on': CFG.get('dart_on', True), 'filter': bool(CFG.get('dart_filter'))}},
            'job': dict(JOB), 'trader': dict(tr.STATE), 'ws': {**rtws.status(), 'enabled': CFG.get('ws_on', True)},
            'schedule': SCHEDULE, 'alloc': al, 'alloc_info': {m: {'sys': tr.ALLOC_SYS[m], 'user': tr.alloc_user(CFG, m), 'eff': tr.alloc(CFG, m), 'why': tr.ALLOC_WHY[m]} for m in tr.ALLOC_SYS}, 'cap': tr.cap(CFG), 'cap_set': CFG.get('cap'), 'cap_mode': 'fixed' if (CFG.get('caps') or {}).get(db.mode()) or CFG.get('cap_mode') == 'fixed' else 'auto', 'ramp': tr.ramp(CFG), 'slots': tr.slots(CFG), 'pick_skip': {k: v[0] for k, v in tr.picks(CFG).items()}, 'backtest': bt,
            'gate': tr.gate(CFG), 'journal': _journal_counts(),
            'cfg': {'accounts': acc, 'krx_id': CF.mask(CFG.get('krx_id')), 'telegram': bool(CFG.get('telegram_token')), 'secrets': secret_status(),
                    'protected': CF.protected(), **{k: CFG.get(k) for k in ('dd_limit', 'day_loss_limit', 'hourly_report', 'collect_time', 'signal_time',
                                                                               'ws_on', 'min_paper_days', 'real_ramp', 'real_ramp_days',
                                                                               'real_ramp_on', 'caps', 'cap', 'cap_mode', 'fee_pct', 'tax_pct',
                                                                               'sweep_on', 'sweep_mode', 'sweep_reserve', 'gap_skip', 'preopen_time',
                                                                               'tg_commands', 'resv_on', 'guard_per_min', 'guard_per_day',
                                                                               'intraday_on', 'intraday_rules', 'intraday_pct', 'intraday_slots', 'intraday_watch', 'dart_on', 'dart_filter')},
                    'sw_weight': db.meta_get('sw_weight')},
            'intraday': {'passed': sorted(IL.passed()), 'rules': IL.RULES, 'watch': IL.LIVE['watch'], 'signals': IL.LIVE['signals'][:10], 'last': IL.LIVE['last'],
                         'lab': dict(IL.STATE)},
            'log': [dict(r) for r in mc.execute('SELECT * FROM log ORDER BY id DESC LIMIT 300')]}


def conn_status():
    """헤더 왼쪽 연결 표시: KIS(지금 모드) · KRX · 텔레그램 · 실시간 → [{name, st: ok|bad|idle, tip}]"""
    out = []
    m = db.mode()
    a = CF.acct(CFG, m)
    if not tr.configured(CFG):
        out.append({'name': 'KIS ' + ('실전' if m == 'real' else '모의'), 'st': 'bad', 'tip': '앱키 · 시크릿 · 계좌 미설정 (⚙️ 설정)'})
    else:
        last = J.API_LAST.get(m)
        acc = CF.mask(a.get('account'))
        if last and time.time() - last[0] < 3600:
            out.append({'name': 'KIS ' + ('실전' if m == 'real' else '모의'), 'st': 'bad' if last[1] else 'ok',
                        'tip': f"{acc} · 마지막 호출 {datetime.fromtimestamp(last[0]):%H:%M}" + (f' · 오류: {last[1]}' if last[1] else ' 정상')})
        else:
            out.append({'name': 'KIS ' + ('실전' if m == 'real' else '모의'), 'st': 'idle', 'tip': f'{acc} · 설정됨 · 최근 1시간 호출 없음 (장 시간 · 🔌 연결 테스트로 확인)'})
    sc = secret_status()
    for key, nm, saved in (('krx', 'KRX', sc['krx']['id'] and sc['krx']['pw']), ('tg', '텔레그램', sc['tg']['token'] and sc['tg']['chat']), ('dart', 'DART', sc['dart']['key'])):
        c = sc[key]['check']
        if not saved:
            out.append({'name': nm, 'st': 'bad', 'tip': '저장 안 됨 (⚙️ 설정)'})
        elif c:
            out.append({'name': nm, 'st': 'ok' if c['ok'] else 'bad', 'tip': f"{c['at']} {'정상' if c['ok'] else '실패'} · {c['msg']}"})
        else:
            out.append({'name': nm, 'st': 'idle', 'tip': '저장됨 · 아직 확인 안 함 (⚙️ 설정에서 테스트)'})
    w = rtws.STATE
    if not CFG.get('ws_on', True):
        out.append({'name': '실시간', 'st': 'idle', 'tip': '웹소켓 꺼짐 (⚙️ 설정)'})
    elif w.get('connected'):
        out.append({'name': '실시간', 'st': 'ok', 'tip': f"웹소켓 연결 · {len(w.get('subs') or [])}종목 · 체결통보 {'켜짐' if w.get('notice') else '없음'}"})
    else:
        out.append({'name': '실시간', 'st': 'bad' if w.get('err') and tr.is_trading_day() and '09:00' <= datetime.now().strftime('%H:%M') <= '15:30' else 'idle',
                    'tip': '장 시간(08:30~15:35)에만 연결' + (f" · 마지막 오류 {w.get('err')[:80]}" if w.get('err') else '')})
    return out


def _eta(st):
    """진행률(pct)과 걸린 시간으로 남은 시간 추정 (KRX 수집 · 자료 넣기)"""
    t = mn.timing(st)
    p = st.get('pct') or 0
    t['eta'] = round(t['elapsed'] * (100 - p) / p) if st.get('running') and t.get('elapsed') and p >= 2 else None
    return t


_INV = {'at': 0.0, 'v': None}


def data_inventory(force=False):
    """📚 데이터 현황 — 데이터별 기간 · 날 수 · 종목 · 줄 수 · 최신 여부 (5분 캐시)"""
    if not force and _INV['v'] and time.time() - _INV['at'] < 300:
        return _INV['v']
    m = db.mconn()
    today = datetime.now().strftime('%Y%m%d')
    def _prev(d):
        try:
            return tr.prev_trading_day(d)
        except Exception:
            return max([x for x in db.trading_days('0', d) if x < d], default='')
    last_td = today if tr.is_trading_day(today) and datetime.now().strftime('%H:%M') >= '16:30' else _prev(today)
    rows = []

    def add(group, name, first, last, days=None, tickers=None, n=None, unit='줄', fresh_need=None, note=''):
        st = '없음' if not last else ('최신' if not fresh_need or last >= fresh_need else '늦음')
        rows.append({'group': group, 'name': name, 'first': first or '', 'last': last or '', 'days': days, 'tickers': tickers, 'n': n, 'unit': unit,
                     'status': st, 'note': note})
    b = m.execute("SELECT MIN(key), MAX(key), COUNT(*) FROM done WHERE kind='bars' AND n>0").fetchone()
    bt = m.execute('SELECT COUNT(*), COUNT(DISTINCT ticker) FROM bars').fetchone()
    add('시장', '일봉 (전종목 · 상장폐지 포함)', b[0], b[1], b[2], bt[1], bt[0], fresh_need=last_td)
    for inv in ('외국인', '기관합계', '연기금'):
        f = m.execute("SELECT MIN(key), MAX(key), COUNT(*) FROM done WHERE kind=? AND n>0", (f'flow_{inv}',)).fetchone()
        nn = m.execute('SELECT COUNT(*) FROM flows WHERE investor=?', (inv,)).fetchone()[0]
        add('시장', f'수급 · {inv}', f[0], f[1], f[2], None, nn, fresh_need=last_td)
    for t, nm in (('229200', 'KODEX 코스닥150'), ('069500', 'KODEX 200')):
        e = m.execute('SELECT MIN(date), MAX(date), COUNT(*) FROM etf WHERE ticker=?', (t,)).fetchone()
        add('시장', f'ETF 일봉 · {nm}', e[0], e[1], e[2], 1 if e[2] else None, e[2], fresh_need=last_td, note='없으면 백테스트는 근사 가격' if not e[2] else '')
    mo = m.execute('SELECT MIN(month), MAX(month), COUNT(DISTINCT month), COUNT(*) FROM members').fetchone()
    add('월 자료', '지수 구성 (코스피200 · 코스닥150)', mo[0] and mo[0] + '01', mo[1] and mo[1] + '28', mo[2], None, mo[3], '줄', fresh_need=last_td[:6] + '01', note='달 단위')
    mf = m.execute('SELECT MIN(month), MAX(month), COUNT(DISTINCT month), COUNT(DISTINCT ticker), COUNT(*) FROM monthly').fetchone()
    add('월 자료', '재무 · 업종 (EPS · 배당 · PBR · 시총)', mf[0] and mf[0] + '01', mf[1] and mf[1] + '28', mf[2], mf[3], mf[4], fresh_need=last_td[:6] + '01', note='달 단위')
    sk = m.execute('SELECT COUNT(*), SUM(listed=0), MAX(updated) FROM stocks').fetchone()
    add('월 자료', '종목 목록 · KIS 마스터', None, (db.gmeta_get('master_at') or sk[2] or '')[:10].replace('-', ''), None, sk[0], sk[0], '종목',
        note=f'상장폐지 {sk[1] or 0}')
    c = m.execute('SELECT MIN(date), MAX(date), COUNT(DISTINCT date), COUNT(*) FROM cands').fetchone()
    srcs = dict(m.execute('SELECT src, COUNT(DISTINCT date) FROM cands GROUP BY src').fetchall())
    add('분석', '신호 후보 (날마다 상위 50)', c[0], c[1], c[2], None, c[3], fresh_need=_prev(last_td) if last_td else None,
        note=' · '.join(f"{'실제' if k == 'live' else '과거 채움'} {v}일" for k, v in srcs.items()))
    ms = mn.status(int(CFG.get('minute_days') or 250))
    mb = mn.conn().execute('SELECT COUNT(*) FROM bars').fetchone()[0]
    add('분석', '1분봉', ms['first'], ms['last'], ms['days'], ms['tickers'], mb, '봉', fresh_need=last_td, note=f"{ms['mb']}MB · 과거 남은 날 {ms['left']}")
    ds = DART.status(int(CFG.get('dart_days') or 250))
    dt_ = DART.conn().execute('SELECT COUNT(DISTINCT ticker) FROM dart').fetchone()[0]
    add('분석', 'DART 공시 (주요사항 · 거래소 · 발행)', ds['first'], ds['last'], ds['days'], dt_, ds['rows'], '건', fresh_need=last_td,
        note=f"악재 {ds['bad']}건 · 과거 남은 날 {ds['left']}" + ('' if CFG.get('dart_key') else ' · 인증키 없음'))
    for mode in ('paper', 'real'):
        x = db.conn(mode)
        ko = '모의' if mode == 'paper' else '실전'
        o = x.execute('SELECT MIN(date), MAX(date), COUNT(DISTINCT date), COUNT(*) FROM orders').fetchone()
        f = x.execute('SELECT COUNT(*) FROM fills').fetchone()[0]
        add(f'{ko} 기록', '주문 · 체결', o[0], o[1], o[2], None, o[3], '주문', note=f'체결 조각 {f:,}')
        lt = x.execute("SELECT MIN(entry_date), MAX(COALESCE(exit_date, entry_date)), COUNT(*), SUM(status='청산'), SUM(status='보유'), COUNT(DISTINCT ticker) FROM lots WHERE entry_date IS NOT NULL").fetchone()
        add(f'{ko} 기록', '보유 · 거래 (매수 → 매도)', lt[0], lt[1], None, lt[5], lt[2], '거래', note=f'청산 {lt[3] or 0} · 지금 보유 {lt[4] or 0}')
        p = x.execute('SELECT MIN(date), MAX(date), COUNT(DISTINCT date), COUNT(DISTINCT ticker), COUNT(*) FROM positions_daily').fetchone()
        add(f'{ko} 기록', '잔고 이력 (날마다 보유 종목)', p[0], p[1], p[2], p[3], p[4])
        a = x.execute('SELECT MIN(date), MAX(date), COUNT(*) FROM account_daily').fetchone()
        add(f'{ko} 기록', '매매일지 · 계좌 평가', a[0], a[1], a[2], None, a[2], '일')
        dd = x.execute('SELECT MIN(date), MAX(date), COUNT(DISTINCT date), COUNT(*) FROM decisions').fetchone()
        add(f'{ko} 기록', '판단 기록 (산 것 · 못 산 것)', dd[0], dd[1], dd[2], None, dd[3])
    bd = os.path.join(db.DATA_DIR, 'backups')
    bks = sorted(os.listdir(bd)) if os.path.isdir(bd) else []
    files = {f: os.path.getsize(os.path.join(db.DATA_DIR, f)) for f in os.listdir(db.DATA_DIR) if f.endswith('.db')}
    v = {'rows': rows, 'made': db.now_s(), 'last_td': last_td, 'backups': {'n': len(bks), 'last': bks[-1] if bks else '',
                                                                          'mb': round(sum(os.path.getsize(os.path.join(bd, f)) for f in bks) / 1e6, 1)},
         'files': {k: round(v_ / 1e6, 1) for k, v_ in sorted(files.items())}}
    _INV.update(at=time.time(), v=v)
    return v


@app.get('/api/inventory')
async def api_inventory(force: int = 0):
    return await asyncio.to_thread(data_inventory, bool(force))


MODULES = [('tk_server.py', '서버 · 화면 API · 일정(수집 · 신호 · 분봉 · 백업 · 따라잡기) · 보안'), ('tk_trader.py', '매매 엔진 — 장전 · 09:02 · 15:10/15:20 · 마감 · 안전장치 · 계좌 전환'),
           ('tk_signals.py', '신호 엔진 — 저변동고점 · 반전·수급 · 배당·가치 · 밤사이 · 갭 · 지수 타이밍 (실전 · 백테스트 공용)'),
           ('tk_backtest.py', '백테스트 — 실전과 같은 규칙 · 비용 · 두 기간 판정'), ('tk_kis.py', '한국투자증권 REST — 주문 · 잔고 · 체결 · 시세 · 예상체결가 · 1분봉'),
           ('tk_ws.py', '웹소켓 — 실시간 체결가 · 체결 통보(AES 해독) · 재접속'), ('tk_collect.py', '자료 수집 — KRX 전종목 · 수급 · ETF · 월 재무 · 가져오기'),
           ('tk_minute.py', '⏱ 1분봉 수집기 — 날짜별 대상 · 이어받기 · zip'), ('tk_intraday.py', '⏱ 장중 연구실(규칙 4개 · 대조군 · 판정) · 장중 칸(기본 꺼짐)'),
           ('tk_shadow.py', '👥 그림자 운용 — 실험 설정을 가상으로 나란히'), ('tk_dart.py', '📰 DART 공시 — 수집 · 악재 분류 · 효과 연구 · 매수 거르기(기본 꺼짐)'),
           ('tk_brief.py', '📱 텔레그램 브리핑 (10시 · 13시 · 장마감)'), ('tk_export.py', '📦 모든 데이터 한 번에 저장 (기간 · 항목 · 조각)'), ('tk_stock.py', '🔍 종목분석 엔진 — 판단 · 전략 · 과거 성적 · 차트 (우리 규칙)'), ('tk_journal.py', '거래 기록 — 주문 상태 · 체결 조각 · 판단 · 매매일지 · 잔고 · 후보'),
           ('tk_analyze.py', '거래내역 조회 · 분석 · 고도화 후보 · 분석 패키지'), ('tk_db.py', '저장소 — 시장 DB · 모드별 장부 · 수정주가 · 백업'),
           ('tk_config.py', '설정 · 비밀 값 DPAPI 암호화'), ('tk_app.html', '화면 (우량주 앱 테마 7가지)')]
RESEARCH = [
    ('2026-09-30', '실제 자료 백테스트 (TK_STOCK_CHART_DB 278만 줄)', '합성 연 +15.7% → 기준선', '설계서 9장'),
    ('2026-09-30', '반전·수급 30자리 (보고서 크기로)', '채택', '설계서 9장'),
    ('2026-09-30', 'LVH · REV 순위 구간 · 자리 수', '미채택 (합성에서 두 기간 개선 없음) · 실험 설정으로', '설계서 11장'),
    ('2026-10-01', '변동성 관리 · 추세 필터 · 배당+ROE', '미채택', '설계서 12장'),
    ('2026-10-01', '시가 갭 +5% 넘으면 안 삼 (한국 밤사이 과잉반응 연구)', '채택', '설계서 12장'),
    ('2026-10-01', '남는 현금 → KODEX 200', '채택 → 밤사이 회전으로', '설계서 12 · 13장'),
    ('2026-10-01', '회전형 전환: 밤사이 35% · LVH 10일 · DV 0%', '채택 (연 +37.9% · 낙폭 −12.3% · 샤프 1.74)', '설계서 14장'),
    ('2026-10-01', '⏱ 1분봉 수집기 → 장중 규칙 검증 준비', '수집 중 (몇 달 뒤 검증)', '설계서 15장'),
    ('2026-10-01', '타사 API 비교: 예약주문 · 텔레그램 명령 · 그림자 운용 · 예상체결 웹소켓 · 주문 안전장치', '추가', '설계서 16장'),
    ('2026-10-01', '⏱ 장중 연구실 · 장중 칸(낮에 노는 돈 · 기본 꺼짐) — 기존 규칙에 더하기', '분봉 쌓이는 대로 판정 (40일 · 60건부터)', '설계서 17장'),
    ('2026-10-04', '📰 DART 악재 공시 거르기 (유상증자 · CB · BW · 감자 · 횡령배임 · 상장폐지 …)', '수집 · 효과 연구 중 (매수 거르기 기본 꺼짐)', '설계서 18장')]
SCHEDULE = [('07:30', '작업 스케줄러가 PC 깨워 실행 (절전 해제)'), ('06:00~08:40', '어젯밤 놓친 자료 수집 · 신호 계산 따라잡기'), ('07:40', 'KIS 종목 마스터 (정지 · 관리 · 경고)'),
            ('08:05', '휴장일 확인'), ('08:20', '장전 점검 — 연결 · 잔고 · 모르는 종목 · 신호 날짜 (주문 없음)'),
            ('08:50', '장전: 밤사이 ETF · KODEX 200 · 보유 끝 종목 시가 매도 → 예상체결가로 갭 확인 → 새 매수 (시가)'),
            ('09:02', '현금이 모자라 미룬 매수 · 장전 거절 재시도'), ('09:05~15:15', '⏱ 장중 칸 (켰을 때만 · 통과한 규칙만 · 15:15 모두 정리)'), ('09:01~15:30', '체결 반영(60초 · 체결 통보 즉시) · 실시간 평가 · 하루 손실 안전장치'), ('10:00 · 13:00', '📱 텔레그램 오전 브리핑(작동 상태 · 아침 매매) · 중간 브리핑'),
            ('15:10', '밤사이 칸 매수 자금 확인'), ('15:20', '🌙 KODEX 코스닥150 + 💤 KODEX 200 종가 매수 (밤사이)'), ('15:45', '잔고 대조 · 매매일지 · 잔고 이력 · 계좌 안전장치'),
            ('15:50', '📥 KRX 자료 수집 (실패하면 30분마다 · 22시까지)'), ('16:20', '⏱ 오늘 1분봉'), ('18:05', '📰 DART 오늘 공시 + 과거 이어받기'), ('16:40', '💾 장부 백업'), ('18:15', '수급 확정치'),
            ('18:40', '🎯 신호 계산 → 거래 분석 → 📅 다음 날 매도 예약(실전) → 📱 장마감 브리핑(오늘 결과 · 내일 계획) → 👥 그림자 운용 → ⏱ 장중 연구실'), ('18:30~07:00', '⏱ 과거 1분봉 채우기 (주말도)'), ('항상', 'PC 잠들지 않게 유지 (앱이 켜져 있는 동안 · 화면만 꺼짐)')]
SCHEDULE = sorted(SCHEDULE, key=lambda r: (not r[0][:1].isdigit(), r[0][:5]))         # 시각 순서 (항상은 맨 끝)


def sysinfo():
    import platform
    mods = []
    for f, role in MODULES:
        p = os.path.join(BASE_DIR, f)
        n = sum(1 for _ in open(p, encoding='utf-8')) if os.path.exists(p) else 0
        mods.append({'file': f, 'role': role, 'lines': n})
    eps = sorted({(r.methods and ' '.join(sorted(r.methods - {'HEAD'}))) + ' ' + r.path for r in app.routes if getattr(r, 'path', '').startswith('/api/')})
    tests = os.path.join(BASE_DIR, 'tests', 'test_tk.py')
    n_tests = len(__import__('re').findall(r'(?m)^\s*check\(', open(tests, encoding='utf-8').read())) if os.path.exists(tests) else 0
    return {'app': APP_NAME, 'version': APP_VERSION, 'python': platform.python_version(), 'os': platform.platform(terse=True), 'port': PORT, 'ssl': SSL_MODE,
            'data_dir': db.DATA_DIR, 'modules': mods, 'lines': sum(m['lines'] for m in mods), 'endpoints': len(eps), 'tests': n_tests,
            'schedule': SCHEDULE, 'research': RESEARCH,
            'safety': [('계좌 낙폭', f"고점 대비 −{CFG.get('dd_limit', 15)}% → 새 매수 자동 중지 (매도는 계속)"),
                       ('하루 손실', f"−{CFG.get('day_loss_limit', 4)}% (장중 1분마다 확인) → 새 매수 자동 중지"),
                       ('갭 필터', f"예상 시가가 +{tr.gap_limit(CFG) or 0:g}% 넘게 높으면 LVH · REV 안 삼" if tr.gap_limit(CFG) else '꺼짐'),
                       ('1회 · 하루 한도', '종목당 운용 한도의 15% · 하루 매수 60%'), ('운용 자금', (f'계좌 전체 — 지금 {tr.cap(CFG):,}원 (수익 따라 늘고 줄음)' if CFG.get('cap_mode') != 'fixed' and not (CFG.get('caps') or {}).get(db.mode())
                                   else f'상한 {tr.cap(CFG):,}원 고정')),
                       ('주문 결과 불분명', '재주문하지 않고 자동주문 정지 → KIS 앱에서 확인 뒤 해제'), ('모르는 보유 종목', '계좌에 앱이 모르는 종목이 있으면 새 매수 차단'),
                       ('잔고 불일치', '장 마감에 KIS 잔고와 장부 대조 → 다르면 알림'), ('긴급 정지', '미체결 취소 · 자동주문 끔 (보유는 그대로)'),
                       ('늦게 켜짐', '장전 주문을 놓치면 팔 것만 팔고 그날 새 매수 쉼'), ('손절', '없음 — 전종목 검증에서 손절은 모든 모델의 수익을 깎음 (꼬리 위험은 종목당 비중으로)')],
            'security': ['이 PC에서만 접속 (127.0.0.1) · Host 머리글 확인', '실행마다 새 세션 토큰 · 바꾸는 호출은 JSON만 · CORS 없음',
                         '앱키 · 시크릿 · 계좌 · KRX · 텔레그램은 윈도우 DPAPI 암호화', '화면 · 로그 · zip에서 비밀 값 가림']}


@app.get('/api/sysinfo')
async def api_sysinfo():
    return await asyncio.to_thread(sysinfo)


def _journal_counts():
    out = {}
    for m in ('paper', 'real'):
        x = db.conn(m)
        out[m] = {t: x.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0] for t in ('orders', 'fills', 'lots', 'decisions', 'account_daily', 'order_events')}
        out[m]['closed'] = x.execute("SELECT COUNT(*) FROM lots WHERE status='청산'").fetchone()[0]
        out[m]['first'] = x.execute('SELECT MIN(date) FROM orders').fetchone()[0]
    c = db.mconn().execute('SELECT COUNT(DISTINCT date), MIN(date), MAX(date) FROM cands').fetchone()
    out['cands'] = {'days': c[0], 'first': c[1], 'last': c[2]}
    return out


@app.get('/', response_class=HTMLResponse)
async def index():
    return open(os.path.join(BASE_DIR, 'tk_app.html'), encoding='utf-8').read().replace('__TK_TOKEN__', TOKEN)


@app.get('/api/state')
async def api_state():
    return await asyncio.to_thread(_state)


@app.get('/api/live')
async def api_live():
    """실시간 탭 — 2초마다 (KIS 호출 없음: 웹소켓 가격 · 장부)"""
    def f():
        lots = _lots_view()
        x = db.conn()
        return {'now': datetime.now().strftime('%H:%M:%S'), 'lots': lots, 'ws': {**rtws.status(), 'enabled': CFG.get('ws_on', True)},
                'open': [dict(r) for r in x.execute("SELECT * FROM orders WHERE date=? AND status IN ('보냄','접수','부분') ORDER BY id DESC", (tr.today(),))],
                'intraday': [[r[0][8:], r[1]] for r in x.execute('SELECT ts, value FROM intraday WHERE ts>=? ORDER BY ts', (tr.today(),))],
                'eval': sum(l['eval'] for l in lots if l['status'] == '보유')}
    return await asyncio.to_thread(f)


BAL = {}                                                                         # 모드별 잔고 조회 캐시 {mode: (time, 결과)}


def balance_view(force=False):
    """💼 계좌 잔고 — KIS 잔고(HTS 잔고 화면처럼) + 앱 칸 표시 + 앱/KIS 불일치 + 날마다 잔고 이력
       KIS 호출은 20초에 한 번까지 (안 되면 마지막 장 마감 잔고 기록으로)"""
    m = db.mode()
    x = db.conn()
    hist = [dict(r) for r in x.execute('SELECT * FROM account_daily ORDER BY date DESC LIMIT 60')]
    eq = [dict(r) for r in x.execute('SELECT date, value FROM equity ORDER BY date DESC LIMIT 2')]
    lots = [l for l in _lots_view() if l['status'] == '보유']
    by = {}
    for l in lots:
        by.setdefault(l['ticker'], []).append(l)
    c = BAL.get(m)
    out = None
    if c and not force and time.time() - c[0] < 20:
        out = c[1]
    elif tr.configured(CFG):
        try:
            kc = tr.client(CFG)
            b = kc.balance()
            try:
                can = kc.buyable()['nrcvb']
            except Exception:
                can = None
            out = {'src': 'kis', 'at': datetime.now().strftime('%H:%M:%S'), 'cash': b['cash'], 'cash_d2': b['cash_d2'], 'equity': b['equity'],
                   'buyable': can, 'positions': b['positions']}
            BAL[m] = (time.time(), out)
        except Exception as e:
            out = {'src': 'err', 'err': CF.clean(e)[:200]}
    if not out or out['src'] == 'err':                                           # 조회가 안 되면 마지막 장 마감 잔고
        d = x.execute('SELECT MAX(date) FROM positions_daily').fetchone()[0]
        a = hist[0] if hist else {}
        out = {**(out or {}), 'src': out['src'] if out else 'none', 'at': d or '', 'cash': a.get('cash'), 'cash_d2': a.get('cash_d2'), 'equity': a.get('equity'), 'buyable': None,
               'positions': [dict(r) for r in x.execute('SELECT ticker, name, qty, qty sellable, avg, price, value, pnl FROM positions_daily WHERE date=?', (d,))] if d else []}
    live = rtws.PRICE
    st = db.stocks()
    rows, tot_buy, tot_val = [], 0.0, 0.0
    for p in out['positions']:
        if not p.get('name') or p['name'] == p['ticker']:
            p = {**p, 'name': (st.get(p['ticker']) or {}).get('name') or {S.ON_TICKER: S.ON_NAME, S.SW_TICKER: S.SW_NAME}.get(p['ticker'], p['ticker'])}
        px = (live.get(p['ticker']) or (None,))[0] or p.get('price')
        buy = (p.get('avg') or 0) * p['qty']
        val = px * p['qty'] if px else (p.get('value') or 0)
        ls = by.get(p['ticker'], [])
        app_q = sum(l['qty'] or 0 for l in ls)
        rows.append({**p, 'px': px, 'live': p['ticker'] in live, 'buy': buy, 'val': val, 'pnl': val - buy if buy else None, 'ret': (val / buy - 1) * 100 if buy else None,
                     'sleeves': sorted({l['sleeve'] for l in ls}), 'app_qty': app_q, 'diff': p['qty'] - app_q})
        if buy:
            tot_buy += buy
        tot_val += val
    held = {p['ticker'] for p in out['positions']}
    missing = [{'ticker': t, 'name': ls[0]['name'], 'qty': sum(l['qty'] or 0 for l in ls), 'sleeves': sorted({l['sleeve'] for l in ls})} for t, ls in by.items() if t not in held]
    if out['src'] != 'kis':                                                      # 지난 기록과 지금 앱 기록은 비교하지 않음 (가짜 불일치)
        missing = []
        for r in rows:
            r['diff'] = 0
    for r in rows:
        r['weight'] = r['val'] / (out['equity'] or 1) * 100 if out.get('equity') else None
    prev = next((e['value'] for e in eq if e['date'] < tr.today()), None)
    return {'mode': m, 'account': tr.client(CFG).masked_account if tr.configured(CFG) else '', **{k: out.get(k) for k in ('src', 'at', 'err', 'cash', 'cash_d2', 'equity', 'buyable')},
            'rows': sorted(rows, key=lambda r: -r['val']), 'missing': missing, 'buy': tot_buy, 'val': tot_val,
            'pnl': sum(r['pnl'] for r in rows if r['pnl'] is not None) if tot_buy else None,
            'ret': sum(r['pnl'] for r in rows if r['pnl'] is not None) / tot_buy * 100 if tot_buy else None, 'prev': prev, 'hist': hist}


def adopt(ticker, action):
    """KIS에는 있는데 앱 장부에 없는 종목(다른 프로그램 · 직접 산 것) → 앱 장부로 가져오기
       keep = '수동' 묶음으로 보유 (자동 규칙이 팔지 않음 · 직접 매도) · sell = 가져와서 정리 (장중이면 지금 시장가 · 아니면 다음 장전 08:50 시가)
       가져오면 '앱이 모르는 종목 → 새 매수 차단'이 풀림"""
    if action not in ('keep', 'sell'):
        raise ValueError('keep 또는 sell')
    kc = tr.client(CFG)
    bal = kc.balance()
    p = next((p for p in bal['positions'] if p['ticker'] == str(ticker).zfill(6)), None)
    if not p:
        raise ValueError('KIS 잔고에 없는 종목')
    x = db.conn()
    have = x.execute("SELECT COALESCE(SUM(qty),0) FROM lots WHERE ticker=? AND status IN ('보유','주문')", (p['ticker'],)).fetchone()[0]
    q = p['qty'] - have
    if q <= 0:
        raise ValueError('이미 앱 장부에 있음')
    d = tr.today()
    p = {**p, 'avg': p.get('avg') or p.get('price') or 0}
    if not p.get('name') or p['name'] == p['ticker']:
        p['name'] = (db.stocks().get(p['ticker']) or {}).get('name') or p['ticker']
    with tr._lock:
        lid = tr.new_lot('MAN', p['ticker'], p['name'], '', d, {'ref': p['avg'], 'info': json.dumps({'adopted': d, 'from': 'KIS 잔고'}, ensure_ascii=False)})
        x.execute("UPDATE lots SET status='보유', qty=?, qty0=?, entry_px=?, cost=?, entry_date=?, last_px=?, entry_ts=?, updated=? WHERE id=?",
                  (q, q, p['avg'], q * p['avg'], d, p['price'], db.now_s(), db.now_s(), lid))
        x.commit()
        db.log(f"앱 밖 종목 가져옴: {p['name']} {q}주 @ {p['avg']:,.0f} → 수동 묶음" + (' · 정리(매도)' if action == 'sell' else ' · 보유'), 'warn')
        if action == 'keep':
            return f"{p['name']} {q}주 → 수동 묶음으로 보유 (자동 규칙이 팔지 않음 · 📡 실시간에서 직접 매도)"
        hm = datetime.now().strftime('%H:%M')
        if tr.is_trading_day(d) and '09:00' <= hm < '15:20' and tr.can_order(CFG):
            oid = tr.send(CFG, kc, 'sell', 'manual', lid, 'MAN', p['ticker'], p['name'], q, sig_ref=p['price'])
            if oid:
                return f"{p['name']} {q}주 지금 시장가 매도 주문"
        x.execute("UPDATE lots SET sell_flag=1, sell_reason='manual' WHERE id=?", (lid,))
        x.commit()
        return f"{p['name']} {q}주 → 다음 장전 08:50 시가 매도 예정 (자동주문이 켜져 있어야 함)"


@app.post('/api/adopt')
async def api_adopt(req: Request):
    b = await req.json()
    r = await _ok(adopt)(b.get('ticker'), b.get('action'))
    BAL.clear()
    return r


@app.get('/api/balance')
async def api_balance(force: int = 0):
    return await asyncio.to_thread(balance_view, bool(force))


def _ok(fn):
    async def run(*a):
        try:
            r = await asyncio.to_thread(fn, *a)
            return {'ok': True, **(r if isinstance(r, dict) else {'msg': r})}
        except Exception as e:
            return JSONResponse({'ok': False, 'error': CF.clean(e)}, 400)
    return run


@app.post('/api/config')
async def api_config(req: Request):
    b = await req.json()

    def f():
        for m in ('paper', 'real'):
            for k, v in (b.get('accounts', {}).get(m) or {}).items():
                if k in CF.ACCOUNT_KEYS and str(v).strip():
                    CFG['accounts'].setdefault(m, {})[k] = str(v).strip()
        for k in CF.GLOBAL_SECRETS:
            if str(b.get(k) or '').strip():
                CFG[k] = str(b[k]).strip()
                if k.startswith('krx'):                                          # 새 KRX 계정 → 다음 수집 때 새로 로그인 · 확인 기록 지움
                    col.STOCK[0] = None
                    db.gmeta_set('krx_check', '')
                elif k == 'dart_key':
                    db.gmeta_set('dart_check', '')
                    db.gmeta_set('dart_try', '0')
                else:
                    db.gmeta_set('tg_check', '')
        al_in = b.get('alloc_modes') or ({db.mode(): b['alloc']} if 'alloc' in b else {})
        for m, raw in al_in.items():                      # 모드별 칸 비율 — 시스템 기본과 같으면 유저값 지움(기본을 따라감)
            if m not in tr.ALLOC_SYS:
                continue
            a = {k: float(raw[k]) for k in tr.DEFAULT_ALLOC}
            if any(v < 0 or v > 80 for v in a.values()) or sum(a.values()) > 100:
                raise ValueError('칸마다 0~80% · 합계 100% 이하')
            sysd = {k: float(v) for k, v in tr.ALLOC_SYS[m].items()}
            u = None if a == sysd else a
            CFG.setdefault('alloc_user', {})
            if (CFG['alloc_user'] or {}).get(m) != u:
                CFG['alloc_user'] = {**(CFG['alloc_user'] or {}), m: u}
                db.log(f"칸 비율 ({'실전' if m == 'real' else '모의'}): " + ' · '.join(f'{k} {v:g}%' for k, v in a.items()) + (' · 유저값' if u else ' · 시스템 기본') + ' (다음 주문부터)')
        for m in (b.get('alloc_reset') or []):
            if m in tr.ALLOC_SYS:
                CFG['alloc_user'] = {**(CFG.get('alloc_user') or {}), m: None}
                db.log(f"칸 비율 ({'실전' if m == 'real' else '모의'}): 시스템 기본으로 되돌림 · " + ' · '.join(f'{k} {v:g}%' for k, v in tr.ALLOC_SYS[m].items()))
        if b.get('cap_mode') in ('auto', 'fixed'):
            CFG['cap_mode'] = b['cap_mode']
        for k, lo, hi in (('cap', 1_000_000, 2_000_000_000), ('dd_limit', 5, 50), ('day_loss_limit', 1, 20), ('min_paper_days', 20, 250)):
            if b.get(k) not in (None, ''):
                v = float(str(b[k]).replace(',', ''))
                if not lo <= v <= hi:
                    raise ValueError(f'{k} 범위 {lo:,}~{hi:,}')
                CFG[k] = int(v) if k in ('cap', 'min_paper_days') else v
        for k in ('collect_time', 'signal_time'):
            if b.get(k):
                t = str(b[k])
                lo = '15:40' if k == 'collect_time' else '17:30'
                if not (lo <= t <= '23:00' and len(t) == 5):
                    raise ValueError(f'{k}: {lo}~23:00')
                CFG[k] = t
        for k, lo, hi in (('sweep_reserve', 1, 50), ('gap_skip', 0, 30)):
            if b.get(k) not in (None, ''):
                v = float(b[k])
                if not lo <= v <= hi:
                    raise ValueError(f'{k} {lo}~{hi}')
                CFG[k] = v
        if b.get('sweep_mode'):
            if b['sweep_mode'] not in S.SW_MODES:
                raise ValueError('sweep_mode: ' + ' · '.join(S.SW_MODES))
            CFG['sweep_mode'] = b['sweep_mode']
        if b.get('preopen_time'):
            t = str(b['preopen_time'])
            if not ('08:31' <= t <= '08:55' and len(t) == 5):
                raise ValueError('장전 주문 시각 08:31~08:55')
            CFG['preopen_time'] = t
        for k in ('ws_on', 'hourly_report', 'real_ramp_on', 'sweep_on', 'tg_commands', 'resv_on', 'intraday_on', 'dart_on', 'dart_filter'):
            if k in b:
                CFG[k] = bool(b[k])
        if 'intraday_rules' in b:                                                # 장중 칸 규칙 (연구실 '통과'한 것만 실제로 씀)
            rs = [r for r in (b['intraday_rules'] or []) if r in IL.RULES]
            CFG['intraday_rules'] = rs
            bad = [r for r in rs if r not in IL.passed()]
            if bad:
                db.log(f"장중 칸: {', '.join(bad)} 은(는) 연구실 판정 '통과'가 아니라서 켜도 쓰지 않음", 'warn')
        for k, lo, hi in (('intraday_pct', 10, 100), ('intraday_slots', 1, 10), ('intraday_watch', 5, 35)):
            if b.get(k) not in (None, ''):
                v = float(b[k])
                if not lo <= v <= hi:
                    raise ValueError(f'{k} {lo}~{hi}')
                CFG[k] = v if k == 'intraday_pct' else int(v)
        if b.get('intraday_on'):
            db.log(f"⏱ 장중 칸 켬 · 규칙 {', '.join(IL.rules_on(CFG)[0]) or '없음(통과한 규칙 없음 → 매매 안 함)'} · 낮에 노는 돈의 {CFG.get('intraday_pct') or 50}%", 'warn')
        for m in ('paper', 'real'):                                              # 계좌별 운용 한도 (비우면 공통 한도)
            v = (b.get('caps') or {}).get(m) if 'caps' in b else None
            if v is not None:
                v = str(v).replace(',', '').strip()
                if v and not 1_000_000 <= float(v) <= 2_000_000_000:
                    raise ValueError('계좌별 운용 한도 1,000,000 ~ 2,000,000,000')
                CFG.setdefault('caps', {})[m] = int(float(v)) if v else None
        for k, lo, hi in (('LVH', 5, 80), ('REV', 5, 90), ('DV', 5, 30)):          # 실험: 자리 수
            v = (b.get('slots') or {}).get(k)
            if v not in (None, ''):
                if not lo <= int(v) <= hi:
                    raise ValueError(f'{k} 자리 {lo}~{hi}')
                CFG.setdefault('slots', {})[k] = int(v)
        for k in ('LVH', 'REV'):                                                 # 실험: 맨 위 몇 순위를 건너뛸지
            v = (b.get('pick_skip') or {}).get(k)
            if v not in (None, ''):
                if not 0 <= int(v) <= 30:
                    raise ValueError(f'{k} 건너뛸 순위 0~30')
                CFG.setdefault('pick_skip', {})[k] = int(v)
        if b.get('slots') or b.get('pick_skip'):
            db.log(f"실험 설정: 자리 {tr.slots(CFG)} · 건너뛸 순위 {CFG.get('pick_skip') or {}} (다음 신호부터)", 'warn')
        for k, lo, hi in (('fee_pct', 0, 1), ('tax_pct', 0, 1)):
            if b.get(k) not in (None, ''):
                v = float(b[k])
                if not lo <= v <= hi:
                    raise ValueError(f'{k} {lo}~{hi}%')
                CFG[k] = v
        CF.save(CFG)
        return '저장'
    return await _ok(f)()


@app.post('/api/kis/test')
async def api_kis_test(req: Request):
    b = await req.json()

    def f():
        m = b.get('mode') or db.mode()
        kc = tr.client(CFG, m)
        bal = kc.balance()
        return {'mode': m, 'account': kc.masked_account, 'cash': bal['cash'], 'equity': bal['equity'],
                'holdings': [f"{p['name']} {p['qty']}주" for p in bal['positions']][:20]}
    return await _ok(f)()


@app.post('/api/on')
async def api_on(req: Request):
    b = await req.json()

    def f():
        on = bool(b.get('on'))
        if on and not tr.configured(CFG):
            raise ValueError('지금 모드의 앱키 · 시크릿 · 계좌를 먼저 저장하세요')
        CFG['kis_on'] = on
        CF.save(CFG)
        db.log(f"자동주문 {'ON' if on else 'OFF'} ({'실전' if db.mode() == 'real' else '모의'} · 사용자)", 'warn' if on and db.mode() == 'real' else 'info')
        return '저장'
    return await _ok(f)()


@app.post('/api/pause')
async def api_pause(req: Request):
    b = await req.json()

    def f():
        CFG['pause_buy'] = bool(b.get('pause'))
        if not CFG['pause_buy']:
            db.meta_set('auto_pause', '')
        CF.save(CFG)
        db.log(f"새 매수 {'일시 중지' if CFG['pause_buy'] else '재개 (안전장치 해제 포함)'} (사용자)")
        return '저장'
    return await _ok(f)()


@app.post('/api/halt_clear')
async def api_halt_clear(req: Request):
    await req.json()
    db.meta_set('halt', '')
    db.log('정지 해제 (사용자)')
    return {'ok': True}


def emergency(who='사용자'):
    """긴급 정지 — 자동주문 끔 · 정지 · 오늘 미체결 취소 · 예약주문 취소 시도 (보유 종목은 그대로)"""
    CFG['kis_on'] = False
    CF.save(CFG)
    tr.halt(f'긴급 정지 ({who}) — 미체결 주문 취소')
    n = 0
    if not tr.configured(CFG):
        return 0
    kc = tr.client(CFG)
    for o in db.conn().execute("SELECT * FROM orders WHERE date=? AND status IN ('접수','부분')", (tr.today(),)).fetchall():
        try:
            kc.cancel(o['order_no'], o['org_no'])
            n += 1
        except Exception as ex:
            db.log(f"취소 실패 {o['name']}: {CF.clean(ex)}", 'warn')
    for o in db.conn().execute("SELECT * FROM orders WHERE status='예약' AND resv_seq IS NOT NULL").fetchall():      # 예약주문도 취소 시도
        try:
            kc.resv_cancel(o['resv_seq'], (o['ts'] or '')[:10].replace('-', ''))
            db.conn().execute("UPDATE orders SET status='취소', msg='긴급 정지로 예약 취소' WHERE id=?", (o['id'],))
            n += 1
        except Exception as ex:
            db.log(f"예약 취소 실패 {o['name']} — KIS 앱에서 예약주문 취소: {CF.clean(ex)}", 'error')
    db.conn().commit()
    return n


@app.post('/api/emergency')
async def api_emergency(req: Request):
    await req.json()
    return await _ok(lambda: {'cancelled': emergency()})()


# ════════════════════════════════════════════
#  📱 텔레그램 명령 (설정한 채팅방에서만)
# ════════════════════════════════════════════
TG_HELP = """📱 TK자동매매 명령
/상태 — 계좌 · 자동주문 · 정지 · 오늘 손익
/보유 — 보유 묶음과 평가
/오늘 — 지금 브리핑 (장중: 중간 · 마감 뒤: 장마감)
/매수중지 — 새 매수만 멈춤 (매도는 계속)
/재개 — 새 매수 다시 (계좌 안전장치 해제 포함)
/정지 — ⛔ 긴급 정지: 자동주문 끔 · 미체결 · 예약 취소 (보유는 그대로)
/정지해제 — 정지 표시만 풂 (자동주문 켜기는 앱에서)
/도움 — 이 목록"""


def tg_command(text):
    """텔레그램 명령 하나 → 답장 (화면 버튼과 같은 동작)"""
    c = (text or '').strip().split()[0].lower() if (text or '').strip() else ''
    c = c.split('@')[0]
    alias = {'/status': '/상태', '/hold': '/보유', '/today': '/오늘', '/pause': '/매수중지', '/resume': '/재개', '/stop': '/정지', '/unhalt': '/정지해제',
             '/help': '/도움', '/start': '/도움', '/brief': '/브리핑'}
    c = alias.get(c, c)
    x = db.conn()
    if c == '/상태':
        eq = [dict(r) for r in x.execute('SELECT * FROM equity ORDER BY date DESC LIMIT 2')]
        e, p = (eq[0] if eq else None), (eq[1] if len(eq) > 1 else None)
        lots = [l for l in _lots_view() if l['status'] == '보유']
        ev = sum(l['eval'] or 0 for l in lots)
        L = [f"{'🔴 실전' if db.mode() == 'real' else '🟢 모의'} · 자동주문 {'ON' if CFG.get('kis_on') else 'OFF'}",
             f"계좌 {e['value']:,.0f}원" + (f" · 전일 대비 {e['value'] - p['value']:+,.0f}원" if e and p else '') if e else '계좌 기록 없음',
             f"보유 {len(lots)}묶음 · 평가 손익 {ev:+,.0f}원 · 운용 자금 {tr.cap(CFG):,}원",
             f"신호 {db.meta_get('last_signal_date') or '-'} · 일봉 {db.last_bar_day() or '-'} · 실시간 {'연결' if rtws.STATE.get('connected') else '대기'}"]
        for k, v in (('⛔ 정지', tr.halted()), ('⏸ 매수 중지', db.meta_get('auto_pause') or ('사용자' if CFG.get('pause_buy') else '')), ('⚠️ 매수 차단', db.meta_get('block_new'))):
            if v:
                L.append(f'{k}: {v}')
        return '\n'.join(L)
    if c == '/보유':
        lots = sorted([l for l in _lots_view() if l['status'] == '보유'], key=lambda l: -(l['eval'] or 0))
        if not lots:
            return '보유 없음'
        return f'보유 {len(lots)}묶음\n' + '\n'.join(f"[{l['sleeve']}] {l['name']} {l['qty']}주 {l['eval_pct'] or 0:+.2f}% ({l['eval'] or 0:+,.0f})" for l in lots[:25])
    if c in ('/오늘', '/브리핑'):
        bal = {}
        try:
            bal = tr.client(CFG).balance() if tr.configured(CFG) else {}
        except Exception as e:
            db.log(f'브리핑 잔고 조회 실패: {CF.clean(e)[:100]}', 'warn')
        d = tr.today()
        return BR.closing(CFG, d, plan=db.meta_get('last_signal_date') == d) if datetime.now().strftime('%H:%M') >= '15:45' else BR.midday(CFG, bal, d)
    if c == '/매수중지':
        CFG['pause_buy'] = True
        CF.save(CFG)
        db.log('새 매수 일시 중지 (텔레그램)', 'warn')
        return '⏸ 새 매수 일시 중지 — 매도는 계속합니다. /재개 로 다시'
    if c == '/재개':
        CFG['pause_buy'] = False
        db.meta_set('auto_pause', '')
        CF.save(CFG)
        db.log('새 매수 재개 (텔레그램)', 'warn')
        return '▶ 새 매수 재개 (계좌 안전장치 표시도 해제)'
    if c == '/정지':
        n = emergency('텔레그램')
        return f'⛔ 긴급 정지 — 자동주문 꺼짐 · 주문 {n}건 취소 요청. 보유 종목은 그대로입니다. 다시 켜기는 앱에서.'
    if c == '/정지해제':
        db.meta_set('halt', '')
        db.log('정지 해제 (텔레그램)', 'warn')
        return '정지 표시 해제 — 자동주문은 지금 ' + ('ON' if CFG.get('kis_on') else 'OFF (앱에서 켜기)')
    return TG_HELP


def tg_poll():
    """텔레그램 명령 받기 (25초 긴 대기) — 설정한 채팅방 · 2분 안 메시지만 · 처음 켤 때 밀린 명령은 실행 안 함"""
    fails, last_err = 0, ''
    while True:
        t, ch = CFG.get('telegram_token'), str(CFG.get('telegram_chat') or '')
        if not (t and ch) or not CFG.get('tg_commands', True):
            time.sleep(30)
            continue
        try:
            off = db.gmeta_get('tg_offset')
            q = {'timeout': 0 if not off else 25, **({'offset': off} if off else {})}
            with urllib.request.urlopen(f'https://api.telegram.org/bot{t}/getUpdates?' + urllib.parse.urlencode(q), timeout=40) as r:
                ups = json.loads(r.read().decode()).get('result') or []
            for u in ups:
                db.gmeta_set('tg_offset', u['update_id'] + 1)
                m = u.get('message') or u.get('edited_message') or {}
                if not off:
                    continue                                                       # 처음: 밀린 것 건너뜀
                if str((m.get('chat') or {}).get('id')) != ch:
                    db.log(f"텔레그램: 등록 안 된 채팅방의 명령 무시 ({(m.get('chat') or {}).get('id')})", 'warn')
                    continue
                if time.time() - float(m.get('date') or 0) > 120 or not str(m.get('text') or '').startswith('/'):
                    continue
                db.log(f"텔레그램 명령: {m.get('text')[:30]}")
                try:
                    reply = tg_command(m['text'])
                except Exception as e:
                    reply = f'오류: {CF.clean(e)[:150]}'
                telegram(reply)
            if not off and not ups:
                db.gmeta_set('tg_offset', '1')
        except Exception as e:
            msg = CF.clean(e)[:120]
            fails = fails + 1 if msg == last_err else 1
            if fails == 1 or fails % 60 == 0:                                         # 같은 오류는 처음 한 번 · 그 뒤 약 1시간마다만 기록
                db.log(f'텔레그램 명령 받기 실패{f" ({fails}번째)" if fails > 1 else ""}: {msg}' + (TG_SSL_HINT if 'CERTIFICATE' in msg else ''), 'warn')
            last_err = msg
            time.sleep(60)
            continue
        fails, last_err = 0, ''


@app.post('/api/sell_lot')
async def api_sell_lot(req: Request):
    b = await req.json()

    def f():
        lid = int(b.get('id'))
        x = db.conn()
        l = x.execute("SELECT * FROM lots WHERE id=? AND status='보유'", (lid,)).fetchone()
        if not l:
            raise ValueError('보유 중인 묶음이 아님')
        hm = datetime.now().strftime('%H:%M')
        if tr.is_trading_day() and '09:00' <= hm <= '15:19' and tr.can_order(CFG):
            with tr._lock:
                if not tr.send(CFG, tr.client(CFG), 'sell', 'manual', l['id'], l['sleeve'], l['ticker'], l['name'], l['qty']):
                    raise RuntimeError('주문 실패 — 로그 확인')
            return '지금 시장가 매도 주문'
        x.execute("UPDATE lots SET sell_flag=1, sell_reason='manual' WHERE id=?", (lid,))
        x.commit()
        return '다음 장전 08:50 매도 예약'
    return await _ok(f)()


@app.post('/api/cancel')
async def api_cancel(req: Request):
    b = await req.json()

    def f():
        o = db.conn().execute("SELECT * FROM orders WHERE id=? AND status IN ('접수','부분')", (int(b.get('id')),)).fetchone()
        if not o:
            raise ValueError('취소할 수 있는 주문이 아님')
        tr.client(CFG).cancel(o['order_no'], o['org_no'])
        db.conn().execute("UPDATE orders SET status='취소요청' WHERE id=?", (o['id'],))
        db.conn().commit()
        db.log(f"주문 취소 요청 {o['name']} (사용자)")
        return '취소 요청'
    return await _ok(f)()


@app.post('/api/manual_order')
async def api_manual_order(req: Request):
    """수동 주문 (v8처럼) — 장부에 '수동' 칸 묶음으로 기록 · 1회 한도 · 실전은 확인 문구"""
    b = await req.json()

    def f():
        side, tk, qty = b.get('side'), str(b.get('ticker', '')).strip().zfill(6), int(b.get('qty') or 0)
        dv, price = ('00', float(b.get('price') or 0)) if b.get('limit') else ('01', 0)
        if side not in ('buy', 'sell') or len(tk) != 6 or qty <= 0:
            raise ValueError('매수/매도 · 종목코드 6자리 · 수량 1주 이상')
        if not tr.can_order(CFG):
            raise ValueError('자동주문이 꺼져 있거나 정지 상태')
        kc = tr.client(CFG)
        p = kc.price(tk)
        ref = price or p['price']
        base = tr.cap(CFG)
        if side == 'buy' and ref * qty > base * 0.15:
            raise ValueError(f'1회 한도(운용 한도의 15% = {base * 0.15:,.0f}원) 초과')
        nm = (p['raw'].get('hts_kor_isnm') or tk).strip()
        with tr._lock:
            if side == 'buy':
                lid = tr.new_lot('MAN', tk, nm, '', tr.today())
                db.conn().commit()
            else:
                l = db.conn().execute("SELECT * FROM lots WHERE ticker=? AND status='보유' ORDER BY id LIMIT 1", (tk,)).fetchone()
                if not l or l['qty'] < qty:
                    raise ValueError('앱 장부의 보유 수량보다 많이 팔 수 없음 (묶음 매도 버튼 사용)')
                lid = l['id']
            oid = tr.send(CFG, kc, side, 'manual', lid, 'MAN' if side == 'buy' else l['sleeve'], tk, nm, qty, dv, price)
        if not oid:
            raise RuntimeError('주문 실패 — 로그 확인')
        return f"{nm} {qty}주 {'매수' if side == 'buy' else '매도'} 주문"
    return await _ok(f)()


@app.post('/api/sync')
async def api_sync(req: Request):
    await req.json()
    return await _ok(lambda: tr.sync(tr.client(CFG)) or '체결 반영')()


@app.post('/api/job/collect')
async def api_job_collect(req: Request):
    b = await req.json()
    if col.STATE['running']:
        return {'ok': False, 'error': '이미 수집 중'}
    if b.get('kind') == 'master':
        return await _ok(lambda: f'마스터 {col.kis_master()}종목')()
    threading.Thread(target=collect_run, args=(bool(b.get('full')),), daemon=True).start()
    return {'ok': True, 'msg': '수집 시작'}


def import_run():
    """seed/ · 가져오기/ 에서 새 파일만 DB에 (지문으로 중복 방지)"""
    if col.STATE['running']:
        return 0
    col.STATE.update(running=True, err='', pct=0, msg='내장 · 가져오기 자료 확인', started=time.time(), ended=0.0)
    try:
        n = col.auto_import(say=lambda m: col.STATE.update(msg=m))
        col.STATE.update(msg=f'자료 확인 끝 · 새로 넣은 파일 {n}개 · 마지막 일봉 {db.last_bar_day() or "-"} · 수급 {db.last_flow_day() or "-"}', pct=100)
        return n
    except Exception as e:
        col.STATE['err'] = CF.clean(e)
        return 0
    finally:
        col.STATE.update(running=False, ended=time.time())


@app.post('/api/job/import')
async def api_job_import(req: Request):
    """가져오기 폴더를 지금 확인 (평소엔 10분마다 자동)"""
    await req.json()
    if col.STATE['running']:
        return {'ok': False, 'error': '수집 중 — 끝난 뒤에'}
    threading.Thread(target=import_run, daemon=True).start()
    return {'ok': True, 'msg': '가져오기 폴더 확인 시작'}


@app.post('/api/job/signal')
async def api_job_signal(req: Request):
    await req.json()
    last = db.last_bar_day()
    if not last:
        return JSONResponse({'ok': False, 'error': '일봉 자료 없음'}, 400)
    threading.Thread(target=signal_run, args=(last,), daemon=True).start()
    return {'ok': True, 'msg': f'{last} 신호 계산 시작'}


@app.post('/api/job/backtest')
async def api_job_backtest(req: Request):
    b = await req.json()
    if JOB['bt']:
        return {'ok': False, 'error': '이미 계산 중'}

    def run():
        JOB.update(bt=True, bt_msg='시작')
        try:
            import tk_backtest
            al = {k: v / 100 for k, v in tr.alloc(CFG).items()}
            tk_backtest.run(b.get('start') or '20231024', b.get('end') or '99999999', al, int(tr.cap(CFG) or 10_000_000),
                            slots=tr.slots(CFG), pick=tr.picks(CFG), sweep_on=tr.sweep_on(CFG), gap_skip=tr.gap_limit(CFG), sweep_mode=CFG.get('sweep_mode') or 'night',
                            progress=lambda m: JOB.update(bt_msg=m))
        except Exception as e:
            JOB['bt_msg'] = f'오류: {CF.clean(e)}'
            db.log(f'백테스트 오류: {CF.clean(e)}', 'error')
        finally:
            JOB['bt'] = False
    threading.Thread(target=run, daemon=True).start()
    return {'ok': True, 'msg': '백테스트 시작'}


@app.post('/api/mode')
async def api_mode(req: Request):
    b = await req.json()

    def f():
        tr.switch_mode(CFG, b.get('mode'), bool(b.get('confirm')))
        CF.save(CFG)
        return f"{'🔴 실전' if CFG['mode'] == 'real' else '🟢 모의'} 계좌로 전환 · 자동주문 {'ON' if CFG.get('kis_on') else 'OFF'} 그대로"
    return await _ok(f)()


@app.post('/api/krx/test')
async def api_krx_test(req: Request):
    await req.json()
    return await _ok(krx_test)()


@app.post('/api/tg_test')
async def api_tg_test(req: Request):
    await req.json()
    ok, err = await asyncio.to_thread(telegram, f'✅ 연결 테스트 {datetime.now():%Y-%m-%d %H:%M}')
    return {'ok': ok, 'error': err}


def build_package(frm='', to=''):
    """점검 · 분석 패키지 — 모의 · 실전 기록(기간 고르면 그 기간만) + 분석 보고서 + 로그 (비밀 값 없음)"""
    if True:
        res = AN.analyze(frm=frm, to=to) if (frm or to) else (ANALYSIS['res'] or AN.analyze())
        data = AN.package(res, {'app': APP_NAME, 'version': APP_VERSION, 'mode': db.mode(), 'made': db.now_s(), 'alloc': tr.alloc(CFG),
                                'cap': tr.cap(CFG), 'gate': tr.gate(CFG), 'data_last_bar': db.last_bar_day(), 'period': [frm, to]}, frm, to)
        buf = io.BytesIO(data)
        with zipfile.ZipFile(buf, 'a', zipfile.ZIP_DEFLATED) as z:
            cur = db.mconn().execute("SELECT * FROM log WHERE replace(substr(ts,1,10),'-','') BETWEEN ? AND ? ORDER BY id DESC LIMIT 20000", (frm or '0', to or '99999999'))
            s_ = io.StringIO()
            csv.writer(s_).writerows([[d_[0] for d_ in cur.description]] + cur.fetchall())
            z.writestr('log.csv', '\ufeff' + s_.getvalue())
            z.writestr('dart.csv', DART.export_csv(frm or '0', to or '99999999'))
        return buf.getvalue()


@app.get('/api/export')
async def api_export():
    data = await asyncio.to_thread(build_package)
    return Response(content=data, media_type='application/zip', headers={'Content-Disposition': f'attachment; filename="tk_record_{datetime.now():%Y%m%d}.zip"'})


# ════════════════════════════════════════════
#  거래 기록 조회 · 분석
# ════════════════════════════════════════════
@app.get('/api/journal')
async def api_journal(view: str = 'orders', mode: str = '', frm: str = '', to: str = '', q: str = ''):
    m = mode if mode in ('paper', 'real', 'all') else db.mode()

    def f():
        return {'ok': True, 'mode': m, 'views': {k: v[0] for k, v in AN.VIEWS.items()}, **AN.journal(view, m, frm, to, q[:40])}
    try:
        return await asyncio.to_thread(f)
    except Exception as e:
        return JSONResponse({'ok': False, 'error': CF.clean(e)}, 400)


@app.get('/api/journal.csv')
async def api_journal_csv(view: str = 'orders', mode: str = '', frm: str = '', to: str = '', q: str = ''):
    m = mode if mode in ('paper', 'real', 'all') else db.mode()
    data = await asyncio.to_thread(AN.journal_csv, view, m, frm, to, q[:40])
    return Response(content=data.encode('utf-8'), media_type='text/csv; charset=utf-8',
                    headers={'Content-Disposition': f'attachment; filename="tk_{view}_{m}_{datetime.now():%Y%m%d}.csv"'})


@app.post('/api/analysis')
async def api_analysis(req: Request):
    b = await req.json()
    if b.get('cached') and ANALYSIS['res']:
        return {'ok': True, 'res': ANALYSIS['res'], 'md': AN.report_md(ANALYSIS['res'])}
    modes = [m for m in (b.get('modes') or ['paper', 'real']) if m in ('paper', 'real')] or ['paper', 'real']

    def f():
        res = AN.analyze(tuple(modes), str(b.get('frm') or '').replace('-', ''), str(b.get('to') or '').replace('-', ''))
        ANALYSIS['res'] = res
        md = AN.report_md(res)
        open(os.path.join(db.DATA_DIR, 'analysis_result.md'), 'w', encoding='utf-8').write(md)
        return {'res': json.loads(json.dumps(res, default=str)), 'md': md}
    return await _ok(f)()


@app.post('/api/job/minute')
async def api_job_minute(req: Request):
    """⏱ 1분봉: today(오늘 분봉) · backfill(과거 채우기 — 지금 바로 · 장중이면 권장 안 함) · stop · probe(조회 점검)"""
    b = await req.json()
    k = b.get('kind')
    if k == 'stop':
        mn.STATE['stop'] = True
        return {'ok': True, 'msg': '멈추는 중 (이어받기 가능)'}
    if k == 'probe':
        def f():
            kc = minute_client()
            x = datetime.now()
            d = tr.prev_trading_day(x.strftime('%Y%m%d'))
            t0 = time.time()
            bars = kc.minute_day('005930', d)
            return {'msg': f"{'실전' if kc.env == 'real' else '모의'} 도메인 · 삼성전자 {d} 1분봉 {len(bars)}개 · {time.time() - t0:.1f}초"
                           + (f" · {bars[0][0]:04d}~{bars[-1][0]:04d}" if bars else ' (없음 — 이 도메인에서 과거 분봉이 안 될 수 있음)')}
        return await _ok(f)()
    if mn.STATE['running']:
        return {'ok': False, 'error': '이미 수집 중'}
    if k not in ('today', 'backfill'):
        return JSONResponse({'ok': False, 'error': 'kind: today · backfill · stop · probe'}, 400)
    threading.Thread(target=minute_run, args=(k if k == 'today' else 'backfill_now',), daemon=True).start()
    return {'ok': True, 'msg': '오늘 분봉 받기 시작' if k == 'today' else '과거 분봉 채우기 시작'}


def default_dir():
    """내보내기 기본 폴더 — 설정 export_dir → 바탕화면\\TK자료 → 내 다운로드"""
    d = CFG.get('export_dir')
    if d and os.path.isdir(d):
        return d
    home = os.path.expanduser('~')
    for cand in (os.path.join(home, 'Desktop'), os.path.join(home, 'OneDrive', '바탕 화면'), os.path.join(home, 'OneDrive', 'Desktop'), os.path.join(home, 'Downloads')):
        if os.path.isdir(cand):
            return os.path.join(cand, 'TK자료')
    return os.path.join(db.DATA_DIR, 'exports')


_pick_lock = threading.Lock()


def pick_folder(start=''):
    """이 PC에서 폴더 고르기 창 (윈도우 · 앱이 같은 PC에서 돌아가므로)"""
    if not _pick_lock.acquire(blocking=False):
        raise ValueError('폴더 선택 창이 이미 열려 있음 (작업 표시줄 확인)')
    try:
        import tkinter
        from tkinter import filedialog
        root = tkinter.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        p = filedialog.askdirectory(parent=root, initialdir=start or default_dir(), title='분봉 zip 저장할 폴더 선택', mustexist=False)
        root.destroy()
        return os.path.normpath(p) if p else ''
    finally:
        _pick_lock.release()


@app.post('/api/pick_folder')
async def api_pick_folder(req: Request):
    b = await req.json()
    return await _ok(lambda: {'path': pick_folder(b.get('start') or '')})()


@app.post('/api/minute/export_file')
async def api_minute_export_file(req: Request):
    """분봉 zip → 고른 폴더에 파일로 (진행률은 /api/minute/export_state)"""
    b = await req.json()
    if mn.EXPORT['running']:
        return {'ok': False, 'error': '이미 저장 중'}
    folder = os.path.normpath(str(b.get('folder') or '').strip() or default_dir())
    frm, to = (b.get('frm') or '0').replace('-', ''), (b.get('to') or '99999999').replace('-', '')
    try:
        os.makedirs(folder, exist_ok=True)
    except Exception as e:
        return {'ok': False, 'error': f'폴더를 만들 수 없음: {CF.clean(e)}'}
    if CFG.get('export_dir') != folder:
        CFG['export_dir'] = folder
        CF.save(CFG)
    name = f"tk_minute_{frm if frm != '0' else 'all'}_{to if to != '99999999' else datetime.now().strftime('%Y%m%d')}.zip"
    path = os.path.join(folder, name)
    part = float(b.get('part_mb') or 0)
    if part and not 1 <= part <= 2000:
        return {'ok': False, 'error': '나눌 크기 1~2000MB'}
    mn.EXPORT.update(running=True, err='', path=path, paths=[], pct=0, msg='준비')            # 상태를 먼저 '저장 중'으로 (화면이 바로 따라오게)
    threading.Thread(target=lambda: _quiet(mn.export_file, path, frm, to, part), daemon=True).start()
    return {'ok': True, 'msg': f'저장 시작 → {path}', 'path': path}


def _quiet(fn, *a):
    try:
        fn(*a)
    except Exception as e:
        db.log(f'저장 실패: {CF.clean(e)}', 'warn')


@app.get('/api/minute/export_state')
async def api_minute_export_state():
    return {'ok': True, **mn.EXPORT, 'default': default_dir(), 'dir': CFG.get('export_dir') or ''}


@app.get('/api/dart')
async def api_dart(frm: str = '', to: str = ''):
    def f():
        return {'ok': True, 'st': DART.status(int(CFG.get('dart_days') or 250)), 'recent': DART.recent(40), 'tags': DART.BAD_TAGS,
                'study': DART.study(int(CFG.get('dart_lookback') or 5), 5, (frm or '0').replace('-', ''), (to or '99999999').replace('-', ''))}
    return await asyncio.to_thread(f)


@app.post('/api/job/dart')
async def api_job_dart(req: Request):
    """DART: run(오늘 + 과거 이어받기) · today(오늘만) · stop · test(인증키 확인)"""
    b = await req.json()
    k = b.get('kind') or 'run'
    key = CFG.get('dart_key')
    if k == 'stop':
        DART.STATE['stop'] = True
        return {'ok': True, 'msg': '멈추는 중 (지금 날짜까지 받고 멈춤)'}
    if not key:
        return {'ok': False, 'error': 'DART 인증키 없음 → ⚙️ 설정에 저장 (opendart.fss.or.kr 무료 신청)'}
    if k == 'test':
        def t():
            try:
                r = DART.test_key(key)
                db.gmeta_set('dart_check', f"{datetime.now():%m-%d %H:%M} ok 오늘 주요사항보고 {r['n']}건")
                return {'msg': f"✅ DART 연결 · 오늘 주요사항보고 {r['n']}건"}
            except Exception as e:
                db.gmeta_set('dart_check', f'{datetime.now():%m-%d %H:%M} fail {CF.clean(e)[:120]}')
                raise
        return await _ok(t)()
    if DART.STATE['running']:
        return {'ok': False, 'error': '이미 수집 중'}
    threading.Thread(target=lambda: DART.run(key, int(CFG.get('dart_days') or 250), only_today=(k == 'today')), daemon=True).start()
    return {'ok': True, 'msg': 'DART 공시 수집 시작' + (' (오늘만)' if k == 'today' else ' (오늘 + 과거 이어받기)')}


@app.post('/api/export_all')
async def api_export_all(req: Request):
    """📦 모든 데이터 한 번에 → 저장 폴더 (기간 · 항목 · 조각 크기)"""
    b = await req.json()
    if XP.STATE['running']:
        return {'ok': False, 'error': '이미 저장 중'}
    parts = [p for p in (b.get('parts') or list(XP.PARTS)) if p in XP.PARTS]
    if not parts:
        return {'ok': False, 'error': '항목을 하나 이상 고르세요'}
    part = float(b.get('part_mb') or 0)
    if part and not 1 <= part <= 2000:
        return {'ok': False, 'error': '조각 크기 1~2000MB'}
    folder = CFG.get('export_dir') or default_dir()
    XP.STATE.update(running=True, err='', msg='준비', pct=0, paths=[])
    threading.Thread(target=lambda: _quiet(XP.export_all, folder, b.get('frm') or '', b.get('to') or '', parts, part, CFG), daemon=True).start()
    return {'ok': True, 'msg': f'모든 데이터 저장 시작 → {folder}'}


@app.get('/api/export_all/state')
async def api_export_all_state():
    last = db.gmeta_get('last_export_to') or ''
    return {'ok': True, **{k: v for k, v in XP.STATE.items()}, 'parts': XP.PARTS, 'last_to': last, 'folder': CFG.get('export_dir') or default_dir()}


@app.get('/api/stock/search')
async def api_stock_search(q: str = ''):
    return {'ok': True, 'rows': await asyncio.to_thread(SK.search, q[:20])}


@app.get('/api/stock/{ticker}')
async def api_stock(ticker: str, days: int = 500):
    """🔍 종목분석 (우리 엔진)"""
    def f():
        return {'ok': True, **SK.analyze(ticker[:6], CFG, max(60, min(int(days or 500), 2000)))}
    try:
        return await asyncio.to_thread(f)
    except Exception as e:
        return JSONResponse({'ok': False, 'error': CF.clean(e)}, 400)


RULES_KO = {'LVH': '🏔 저변동고점 방식 — 10거래일 뒤 시가 매도', 'REV': '🔄 반전·수급 방식 — 종가가 9일선 위로 올라선 다음 날 시가 · 늦어도 10거래일', 'none': '직접 매도 (자동 매도 안 함)'}


def stock_buy(b):
    """✋ 종목분석에서 수동매수 — 장중이면 지금 주문 · 장 밖이면 다음 장전 08:50 시가로 대기 · 청산은 고른 규칙대로 자동"""
    tk = str(b.get('ticker') or '').strip().zfill(6)
    rule = b.get('rule') if b.get('rule') in RULES_KO else 'none'
    if not tr.configured(CFG):
        raise ValueError(f"{'실전' if db.mode() == 'real' else '모의'} 계좌 설정이 없음 (⚙️ 설정)")
    if not tr.can_order(CFG):
        raise ValueError('자동주문이 꺼져 있거나 정지 상태 (⚙️ 설정에서 켜기)')
    if tr.buy_paused(CFG) or db.meta_get('block_new'):
        raise ValueError('새 매수 중지 · 차단 상태 — 대시보드 경고 확인')
    if db.mode() == 'real' and b.get('confirm') != '실전매수':
        raise ValueError('실전 계좌 — 확인 필요')
    st = db.stocks().get(tk) or {}
    if st.get('halt'):
        raise ValueError('거래정지 종목')
    kc = tr.client(CFG)
    d = tr.today()
    hm = datetime.now().strftime('%H:%M')
    live = tr.is_trading_day(d) and '09:00' <= hm < '15:20'
    try:
        p = kc.price(tk)
        ref, nm = float(p['price'] or 0), (p['raw'].get('hts_kor_isnm') or st.get('name') or tk).strip()
    except Exception:
        r_ = db.mconn().execute('SELECT close FROM bars WHERE ticker=? ORDER BY date DESC LIMIT 1', (tk,)).fetchone()
        ref, nm = float(r_[0]) if r_ else 0.0, st.get('name') or tk
    if ref <= 0:
        raise ValueError('가격을 알 수 없음')
    price = float(b.get('price') or 0)
    qty = int(b.get('qty') or 0) or int(float(b.get('amt') or 0) // ((price or ref) * 1.003))
    if qty <= 0:
        raise ValueError('수량 또는 금액을 넣으세요 (1주 이상)')
    base = tr.cap(CFG)
    if (price or ref) * qty > base * 0.15:
        raise ValueError(f'1회 한도(운용 자금의 15% = {base * 0.15:,.0f}원) 초과 — {(price or ref) * qty:,.0f}원')
    info = {'exit_rule': rule, 'manual': True, 'from': '종목분석', 'qty': qty, 'limit': price or None}
    with tr._lock:
        lid = tr.new_lot('MAN', tk, nm, '', d, {'ref': ref, 'info': json.dumps(info, ensure_ascii=False)})
        x = db.conn()
        if not live:
            x.execute("UPDATE lots SET status='대기' WHERE id=?", (lid,))
            x.commit()
            db.log(f"✋ 수동매수 대기: {nm} {qty}주 → 다음 장전 08:50 시가 · 청산 {RULES_KO[rule]}", 'warn')
            return {'msg': f'{nm} {qty}주 — 장 밖이라 다음 거래일 08:50 장전 시가로 주문 대기 · 청산: {RULES_KO[rule]}', 'queued': True, 'lot_id': lid}
        J.decision(x, d, d, {'sleeve': 'MAN', 'ticker': tk, 'name': nm, 'ref': ref, 'qty': qty, 'amt': qty * ref}, 'buy', '✋ 수동매수 (종목분석)')
        x.commit()
        oid = tr.send(CFG, kc, 'buy', 'man_buy', lid, 'MAN', tk, nm, qty, '00' if price else '01', price, sig_ref=ref)
    if not oid:
        raise RuntimeError('주문 실패 — 로그 확인')
    return {'msg': f"{nm} {qty}주 {'지정가 ' + format(int(price), ',') + '원' if price else '시장가'} 매수 주문 · 청산: {RULES_KO[rule]}", 'queued': False, 'lot_id': lid}


@app.post('/api/stock/buy')
async def api_stock_buy(req: Request):
    b = await req.json()
    return await _ok(lambda: stock_buy(b))()


@app.post('/api/stock/cancel')
async def api_stock_cancel(req: Request):
    """수동매수 대기 취소 (아직 주문 전인 것만)"""
    b = await req.json()

    def f():
        x = db.conn()
        r_ = x.execute("SELECT name FROM lots WHERE id=? AND sleeve='MAN' AND status='대기'", (int(b.get('lot_id') or 0),)).fetchone()
        if not r_:
            raise ValueError('대기 중인 수동매수가 아님 (이미 주문됨)')
        x.execute("UPDATE lots SET status='취소', updated=? WHERE id=?", (db.now_s(), int(b['lot_id'])))
        x.commit()
        db.log(f'✋ 수동매수 대기 취소: {r_[0]}')
        return f'{r_[0]} 수동매수 대기 취소'
    return await _ok(f)()


@app.post('/api/save')
async def api_save(req: Request):
    """내려받기 대신 저장 폴더(⚙️ 설정 · 분봉 카드와 같은 폴더)에 파일로 바로 저장 → 저장 경로를 돌려줌
       kind: package(📦 분석 패키지) · journal(📒 거래내역 CSV) · csv(화면에서 만든 표)"""
    b = await req.json()

    def f():
        import re as _re
        k, stamp = b.get('kind'), datetime.now().strftime('%Y%m%d_%H%M')
        frm, to = str(b.get('frm') or '').replace('-', ''), str(b.get('to') or '').replace('-', '')
        if k == 'package':
            data = build_package(frm, to)
            name = f"tk_record_{(frm or 'all') + ('-' + to if to else '') if frm or to else 'all'}_{stamp}.zip"
        elif k == 'journal':
            v = str(b.get('view') or 'orders')
            m = b.get('mode') if b.get('mode') in ('paper', 'real', 'all') else db.mode()
            data = AN.journal_csv(v, m, b.get('frm') or '', b.get('to') or '', str(b.get('q') or '')[:40]).encode('utf-8')
            name = f'tk_{v}_{m}_{stamp}.csv'
        elif k == 'dart':
            data = DART.export_csv(frm or '0', to or '99999999').encode('utf-8')
            name = f"tk_dart_{(frm or 'all') + ('-' + to if to else '') if frm or to else 'all'}_{stamp}.csv"
        elif k == 'csv':
            txt = str(b.get('content') or '')
            if len(txt) > 30_000_000:
                raise ValueError('너무 큼')
            data, name = txt.encode('utf-8'), _re.sub(r'[^0-9A-Za-z가-힣_.-]', '_', str(b.get('name') or 'tk_table'))[:60] + f'_{stamp}.csv'
        else:
            raise ValueError('kind: package · journal · csv')
        folder = CFG.get('export_dir') or default_dir()
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, name)
        with open(path + '.part', 'wb') as fh:
            fh.write(data)
        os.replace(path + '.part', path)
        db.log(f'저장 {path} ({len(data) / 1e6:.1f}MB)')
        return {'path': path, 'mb': round(len(data) / 1e6, 2), 'msg': f'저장됨 → {path}'}
    return await _ok(f)()


@app.post('/api/export_dir')
async def api_export_dir(req: Request):
    """저장 폴더 바꾸기 (비우면 기본: 바탕화면\\TK자료)"""
    b = await req.json()

    def f():
        p = str(b.get('folder') or '').strip()
        if p:
            p = os.path.normpath(p)
            os.makedirs(p, exist_ok=True)
        CFG['export_dir'] = p or None
        CF.save(CFG)
        return {'path': p or default_dir(), 'msg': f'저장 폴더: {p or default_dir()}'}
    return await _ok(f)()


@app.post('/api/open_folder')
async def api_open_folder(req: Request):
    """저장한 폴더를 탐색기로 열기 (내보내기 폴더만)"""
    await req.json()
    p = CFG.get('export_dir') or default_dir()
    os.makedirs(p, exist_ok=True)
    if sys.platform != 'win32' or not os.path.isdir(p):
        return {'ok': False, 'error': p}
    os.startfile(p)
    return {'ok': True, 'msg': p}


@app.get('/api/minute/export')
async def api_minute_export(frm: str = '0', to: str = '99999999'):
    data = await asyncio.to_thread(mn.export_zip, (frm or '0').replace('-', ''), (to or '99999999').replace('-', ''))
    return Response(content=data, media_type='application/zip', headers={'Content-Disposition': f'attachment; filename="tk_minute_{datetime.now():%Y%m%d}.zip"'})


@app.get('/api/shadow')
async def api_shadow():
    return {'ok': True, 'state': dict(SH.STATE), 'res': await asyncio.to_thread(SH.result), 'variants': [n for n, _ in SH.variants(CFG)]}


@app.post('/api/job/shadow')
async def api_job_shadow(req: Request):
    await req.json()
    if SH.STATE['running']:
        return {'ok': False, 'error': '이미 계산 중'}
    threading.Thread(target=SH.run, args=(CFG,), daemon=True).start()
    return {'ok': True, 'msg': '그림자 운용 계산 시작 (1~3분)'}


@app.get('/api/intraday')
async def api_intraday():
    return {'ok': True, 'state': dict(IL.STATE), 'res': await asyncio.to_thread(IL.result), 'rules': IL.RULES, 'crit': IL.CRIT}


@app.post('/api/job/intraday')
async def api_job_intraday(req: Request):
    await req.json()
    if IL.STATE['running']:
        return {'ok': False, 'error': '이미 계산 중'}
    if not mn.status()['days']:
        return {'ok': False, 'error': '1분봉이 아직 없음 (📥 데이터 → ⏱ 1분봉)'}
    threading.Thread(target=IL.run, args=(CFG,), daemon=True).start()
    return {'ok': True, 'msg': '장중 연구실 계산 시작 (분봉 양에 따라 몇 분)'}


@app.post('/api/job/backfill')
async def api_job_backfill(req: Request):
    """과거 신호 후보 채우기 (분석의 '후보 순위별 사후 수익'을 처음부터 볼 수 있게)"""
    b = await req.json()
    if JOB['bf']:
        return {'ok': False, 'error': '이미 계산 중'}

    def run():
        JOB.update(bf=True, bf_msg='시작')
        try:
            AN.backfill_cands(b.get('start') or '20231024', '99999999', progress=lambda m: JOB.update(bf_msg=m))
        except Exception as e:
            JOB['bf_msg'] = f'오류: {CF.clean(e)}'
        finally:
            JOB['bf'] = False
    threading.Thread(target=run, daemon=True).start()
    return {'ok': True, 'msg': '과거 신호 후보 계산 시작'}


def _in_watch():
    try:
        return IL.watch(CFG) if CFG.get('intraday_on') and '09:00' <= datetime.now().strftime('%H:%M') <= '15:20' else []
    except Exception:
        return []


def keep_awake():
    """윈도우: 앱이 켜져 있는 동안 PC가 잠들지 않게 (설정 keep_awake='always' · 기본)
       — 절전 해제 타이머가 꺼진 PC가 많아 '밤에 재우고 07:30에 깨우기'는 믿을 수 없음 (2026-10-02: 06:05 절전 허용 → 07:05 잠듦 → 하루 매매 없음)
       keep_awake='trading'이면 예전처럼 거래일 06:00~21:30 · 작업 중에만"""
    if sys.platform != 'win32':
        return
    import ctypes
    ES_CONTINUOUS, ES_SYSTEM_REQUIRED = 0x80000000, 0x00000001
    on = None
    while True:
        try:
            n = datetime.now()
            hm = n.strftime('%H:%M')
            need = CFG.get('keep_awake', 'always') == 'always' \
                or (tr.is_trading_day(n.strftime('%Y%m%d')) and '06:00' <= hm <= '21:30') or col.STATE['running'] or JOB['signal'] or JOB['bt'] or JOB['bf'] \
                or mn.STATE['running'] or SH.STATE['running'] or IL.STATE['running'] or DART.STATE['running'] or XP.STATE['running']
            if need != on:
                ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | (ES_SYSTEM_REQUIRED if need else 0))
                db.log(('PC 잠들지 않게 유지' + (' (항상 · 앱이 켜져 있는 동안)' if CFG.get('keep_awake', 'always') == 'always' else '')) if need
                       else 'PC 절전 허용 (윈도우 절전 설정대로 · 내일 07:30 깨우기는 작업 스케줄러 · 절전 해제 타이머 필요)')
                on = need
        except Exception:
            pass
        time.sleep(60)


def main():
    import uvicorn
    threading.Thread(target=keep_awake, daemon=True).start()
    threading.Thread(target=tg_poll, daemon=True, name='tg_poll').start()
    tr.NOTIFY = lambda m: telegram(m)
    tk_kis.HOOK[0] = J.api_hit
    tr.EXP_GAP[0] = rtws.exp_gap
    threading.Thread(target=scheduler, daemon=True).start()
    threading.Thread(target=tr.loop, args=(lambda: CFG, lambda: {k: v[0] for k, v in rtws.PRICE.items()}, lambda m: telegram(m)), daemon=True).start()

    def wanted_exp():
        """08:30~08:59 장전: 오늘 살 후보(LVH · REV)의 예상체결가 실시간 구독 → 08:50 갭 확인"""
        if not ('08:30' <= datetime.now().strftime('%H:%M') < '09:00'):
            return []
        sd = db.meta_get('last_signal_date')
        return [r[0] for r in db.conn().execute("SELECT ticker FROM signals WHERE date=? AND sleeve IN ('LVH','REV') AND rank<100 ORDER BY rank", (sd,))] if sd else []

    def on_notice():
        if tr._lock.acquire(timeout=30):
            try:
                tr.sync(tr.client(CFG), tr.today())
            except Exception as e:
                db.log(f'체결 통보 반영 오류: {CF.clean(e)}', 'warn')
            finally:
                tr._lock.release()
    if IL.on_tick not in rtws.TICK_HOOKS:                                       # 장중 칸: 체결가 → 1분봉
        rtws.TICK_HOOKS.append(IL.on_tick)
    rtws.start(lambda: CFG.get('ws_on', True) and tr.configured(CFG), lambda: tr.client(CFG),
               lambda: list(dict.fromkeys([l['ticker'] for l in tr.open_lots() if l['status'] == '보유'] + _in_watch())),
               lambda: tr.is_trading_day() and '08:30' <= datetime.now().strftime('%H:%M') <= '15:35', on_notice, wanted_exp)
    print(f"""
╔══════════════════════════════════════════════╗
║   台炅 TK자동매매 시스템 {APP_VERSION}                     ║
║   http://127.0.0.1:{PORT}   (이 PC에서만 접속)        ║
║   지금 모드: {'🔴 실전' if db.mode() == 'real' else '🟢 모의'}                               ║
╚══════════════════════════════════════════════╝
""", flush=True)
    def _hello():                                                                # 켜질 때 한 번 알림 (아침에 깨어났는지 · 다시 켜졌는지 폰으로 확인)
        time.sleep(20)
        n = datetime.now()
        telegram(f"🟢 TK자동매매 켜짐 {n:%m/%d %H:%M} · 자동주문 {'ON' if CFG.get('kis_on') else 'OFF'}"
                 + (' · 오늘 거래일' if tr.is_trading_day(n.strftime('%Y%m%d')) else ' · 오늘 휴장'))
    threading.Thread(target=_hello, daemon=True).start()
    db.log(f'{APP_NAME} {APP_VERSION} 시작 · 모드 {db.mode()} · 데이터 {db.DATA_DIR} · 비밀 값 {"DPAPI 암호화" if CF.protected() else "base64(윈도우 아님)"} · HTTPS 인증서 {SSL_MODE}')
    uvicorn.run(app, host='127.0.0.1', port=PORT, log_level='warning')


if __name__ == '__main__':
    main()
