"""
tk_server.py — 🏦 TK자동매매 시스템 · http://127.0.0.1:8086 · 한국투자증권 Open API (모의 → 실전)

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

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

import tk_collect as col
import tk_config as CF
import tk_analyze as AN
import tk_db as db
import tk_journal as J
import tk_kis
import tk_signals as S
import tk_trader as tr
import tk_ws as rtws

APP_NAME = 'TK자동매매 시스템'
APP_VERSION = 'T1.1'
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


def telegram(msg):
    t, ch = CFG.get('telegram_token'), CFG.get('telegram_chat')
    if not (t and ch):
        return False, '텔레그램 미설정'
    try:
        head = '[TK' + ('·실전' if db.mode() == 'real' else '·모의') + '] '
        body = urllib.parse.urlencode({'chat_id': ch, 'text': head + msg[:3900]}).encode()
        with urllib.request.urlopen(f'https://api.telegram.org/bot{t}/sendMessage', data=body, timeout=10) as r:
            return bool(json.loads(r.read().decode()).get('ok')), ''
    except Exception as e:
        db.log(f'텔레그램 실패: {CF.clean(e)}', 'warn')
        return False, CF.clean(e)


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
    try:
        r = col.run(CFG, kc=kc, full=full)
        JOB['collect_msg'] = f"끝 {datetime.now():%H:%M} · {r}"
        return True
    except Exception as e:
        JOB['collect_msg'] = f'오류: {CF.clean(e)}'
        tr.alert(f'자료 수집 실패 — {CF.clean(e)}', 'collect')
        return False


def signal_run(d):
    if JOB['signal']:
        return False
    JOB.update(signal=True, signal_msg=f'{d} 신호 계산 중', signal_err='')
    try:
        tr.signal_job(CFG, d)
        JOB['signal_msg'] = f'{d} 신호 계산 끝 {datetime.now():%H:%M}'
        telegram(report(d))
        try:                                                                     # 날마다 거래 분석 갱신 (보고서 파일 · 화면)
            ANALYSIS['res'] = AN.analyze()
            open(os.path.join(db.DATA_DIR, 'analysis_result.md'), 'w', encoding='utf-8').write(AN.report_md(ANALYSIS['res']))
        except Exception as e:
            db.log(f'거래 분석 실패: {CF.clean(e)}', 'warn')
        return True
    except Exception as e:
        JOB['signal_err'] = CF.clean(e)
        db.log(f'신호 계산 실패: {CF.clean(e)}', 'error')
        tr.alert(f'신호 계산 실패 — {CF.clean(e)}', 'signal')
        return False
    finally:
        JOB['signal'] = False


def report(d):
    x = db.conn()
    wd = '월화수목금토일'[datetime.strptime(d, '%Y%m%d').weekday()]
    L = [f"🏦 {d[4:6]}/{d[6:]}({wd}) 장 마감 {'🔴 실전' if db.mode() == 'real' else '🟢 모의'}", '━━━━━━━━━━━━━━']
    eq = [dict(r) for r in x.execute('SELECT * FROM equity WHERE date<=? ORDER BY date DESC LIMIT 2', (d,))]
    sv = float(db.meta_get('start_value') or 0)
    if eq and eq[0]['date'] == d:
        e, p = eq[0], (eq[1] if len(eq) > 1 else None)
        L.append(f"💰 계좌 {e['value']:,.0f}원" + (f" · 오늘 {e['value'] - p['value']:+,.0f}원 ({(e['value'] / p['value'] - 1) * 100:+.2f}%)" if p else '')
                 + (f" · 시작 대비 {(e['value'] / sv - 1) * 100:+.2f}%" if sv else ''))
        L.append(f"   고점 대비 {(e['value'] / e['peak'] - 1) * 100:+.1f}% · 현금(D+2) {e['cash'] / 1e4:,.0f}만" + (f" · 한도 {tr.ramp(CFG)}%" if db.mode() == 'real' else ''))
    for s, m in S.SLEEVES.items():
        ls = [l for l in tr.open_lots(s) if l['status'] == '보유']
        val = sum(l['qty'] * (l['last_px'] or l['entry_px'] or 0) for l in ls)
        inv = sum(l['cost'] * (l['qty'] / l['qty0'] if l['qty0'] else 1) for l in ls)
        cl = [dict(r) for r in x.execute("SELECT pnl FROM lots WHERE sleeve=? AND status='청산'", (s,))]
        L.append(f"{m['icon']} {m['name']}: 보유 {len(ls)} · 평가 {val - inv:+,.0f}원 · 누적 실현 {sum(r['pnl'] or 0 for r in cl):+,.0f}원")
    fills = [dict(r) for r in x.execute('SELECT * FROM orders WHERE date=? AND filled>0 ORDER BY id', (d,))]
    if fills:
        L.append('\n🧾 오늘 체결')
        L += [f" {'🟢' if f['side'] == 'buy' else '🔵'} [{f['sleeve']}] {f['name']} {f['filled']}주 @{f['avg'] or 0:,.0f} · {tr.KIND.get(f['kind'], f['kind'])}" for f in fills[:25]]
    done = [dict(r) for r in x.execute("SELECT * FROM lots WHERE exit_date=? AND status='청산'", (d,))]
    if done:
        L.append(f"\n💵 청산 {len(done)}건 · {sum(r['pnl'] or 0 for r in done):+,.0f}원")
        L += [f" {'🟢' if (r['pnl'] or 0) > 0 else '🔻'} [{r['sleeve']}] {r['name']} {r['ret'] or 0:+.2f}%" for r in done[:20]]
    sells = [dict(r) for r in x.execute("SELECT * FROM lots WHERE status='보유' AND sell_flag=1 AND sleeve!='ON'")]
    sig = [dict(r) for r in x.execute('SELECT * FROM signals WHERE date=? AND rank<100 ORDER BY sleeve, rank', (d,))]
    L.append('\n🗓 다음 거래일 08:50')
    L.append(' 매도: ' + (', '.join(f"[{l['sleeve']}] {l['name']}" for l in sells) or '없음'))
    for s in ('LVH', 'REV', 'DV'):
        ss = [r['name'] for r in sig if r['sleeve'] == s]
        if ss:
            L.append(f" {S.SLEEVES[s]['icon']} 후보: {', '.join(ss[:8])}" + (f' 외 {len(ss) - 8}' if len(ss) > 8 else ''))
    for k, v in (('⛔ 정지', tr.halted()), ('⏸ 매수 중지', db.meta_get('auto_pause')), ('⚠️ 새 매수 차단', db.meta_get('block_new'))):
        if v:
            L.append(f'{k}: {v}')
    if not CFG.get('kis_on'):
        L.append('○ 자동주문 꺼짐')
    return '\n'.join(L)


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
            if hm >= '16:40' and db.gmeta_get('backup_day') != d:                  # 장부 백업 (날마다 · 기록이 핵심이므로)
                db.gmeta_set('backup_day', d)
                try:
                    db.log(f"장부 백업 {', '.join(db.backup())}")
                except Exception as e:
                    db.log(f'장부 백업 실패: {CF.clean(e)}', 'warn')
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
            'trading_day': tr.is_trading_day(d), 'kis_on': bool(CFG.get('kis_on')), 'configured': tr.configured(CFG),
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
                     'collect': dict(col.STATE), 'collect_msg': JOB['collect_msg']},
            'job': dict(JOB), 'trader': dict(tr.STATE), 'ws': {**rtws.status(), 'enabled': CFG.get('ws_on', True)},
            'alloc': al, 'cap': tr.cap(CFG), 'cap_set': CFG.get('cap'), 'ramp': tr.ramp(CFG), 'slots': tr.slots(CFG), 'pick_skip': {k: v[0] for k, v in tr.picks(CFG).items()}, 'backtest': bt,
            'gate': tr.gate(CFG), 'journal': _journal_counts(),
            'cfg': {'accounts': acc, 'krx_id': CF.mask(CFG.get('krx_id')), 'telegram': bool(CFG.get('telegram_token')),
                    'protected': CF.protected(), **{k: CFG.get(k) for k in ('dd_limit', 'day_loss_limit', 'hourly_report', 'collect_time', 'signal_time',
                                                                               'ws_on', 'min_paper_days', 'real_ramp', 'real_ramp_days',
                                                                               'real_ramp_on', 'caps', 'cap', 'fee_pct', 'tax_pct',
                                                                               'sweep_on', 'sweep_mode', 'sweep_reserve', 'gap_skip', 'preopen_time')},
                    'sw_weight': db.meta_get('sw_weight')},
            'log': [dict(r) for r in mc.execute('SELECT * FROM log ORDER BY id DESC LIMIT 300')]}


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
        if 'alloc' in b:
            a = {k: float(b['alloc'][k]) for k in tr.DEFAULT_ALLOC}
            if any(v < 0 or v > 80 for v in a.values()) or sum(a.values()) > 100:
                raise ValueError('칸마다 0~80% · 합계 100% 이하')
            CFG['alloc'] = a
            db.log('칸 비율: ' + ' · '.join(f'{k} {v:g}%' for k, v in a.items()) + ' (다음 주문부터)')
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
        for k in ('ws_on', 'hourly_report', 'real_ramp_on', 'sweep_on'):
            if k in b:
                CFG[k] = bool(b[k])
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


@app.post('/api/emergency')
async def api_emergency(req: Request):
    await req.json()

    def f():
        CFG['kis_on'] = False
        CF.save(CFG)
        tr.halt('긴급 정지 (사용자) — 미체결 주문 취소')
        n = 0
        kc = tr.client(CFG)
        for o in db.conn().execute("SELECT * FROM orders WHERE date=? AND status IN ('접수','부분')", (tr.today(),)).fetchall():
            try:
                kc.cancel(o['order_no'], o['org_no'])
                n += 1
            except Exception as ex:
                db.log(f"취소 실패 {o['name']}: {CF.clean(ex)}", 'warn')
        return {'cancelled': n}
    return await _ok(f)()


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
    col.STATE.update(running=True, err='', pct=0, msg='내장 · 가져오기 자료 확인')
    try:
        n = col.auto_import(say=lambda m: col.STATE.update(msg=m))
        col.STATE.update(msg=f'자료 확인 끝 · 새로 넣은 파일 {n}개 · 마지막 일봉 {db.last_bar_day() or "-"} · 수급 {db.last_flow_day() or "-"}', pct=100)
        return n
    except Exception as e:
        col.STATE['err'] = CF.clean(e)
        return 0
    finally:
        col.STATE['running'] = False


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
            tk_backtest.run(b.get('start') or '20231024', b.get('end') or '99999999', al, int(CFG.get('cap') or 10_000_000),
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


@app.post('/api/tg_test')
async def api_tg_test(req: Request):
    await req.json()
    ok, err = await asyncio.to_thread(telegram, f'✅ 연결 테스트 {datetime.now():%Y-%m-%d %H:%M}')
    return {'ok': ok, 'error': err}


@app.get('/api/export')
async def api_export():
    """점검 · 분석 패키지 — 모의 · 실전 기록 전체 + 분석 보고서 + 로그 (비밀 값 없음)"""
    def build():
        res = ANALYSIS['res'] or AN.analyze()
        data = AN.package(res, {'app': APP_NAME, 'version': APP_VERSION, 'mode': db.mode(), 'made': db.now_s(), 'alloc': tr.alloc(CFG),
                                'cap': tr.cap(CFG), 'gate': tr.gate(CFG), 'data_last_bar': db.last_bar_day()})
        buf = io.BytesIO(data)
        with zipfile.ZipFile(buf, 'a', zipfile.ZIP_DEFLATED) as z:
            cur = db.mconn().execute('SELECT * FROM log ORDER BY id DESC LIMIT 20000')
            s_ = io.StringIO()
            csv.writer(s_).writerows([[d_[0] for d_ in cur.description]] + cur.fetchall())
            z.writestr('log.csv', '\ufeff' + s_.getvalue())
        return buf.getvalue()
    data = await asyncio.to_thread(build)
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


def keep_awake():
    """윈도우: 거래일 07:25~21:30 · 수집/신호/백테스트 중에는 PC가 잠들지 않게 · 그 밖에는 윈도우 절전 설정대로 (밤엔 잠들어도 됨 → 07:30 작업 스케줄러가 깨움)"""
    if sys.platform != 'win32':
        return
    import ctypes
    ES_CONTINUOUS, ES_SYSTEM_REQUIRED = 0x80000000, 0x00000001
    on = None
    while True:
        try:
            n = datetime.now()
            hm = n.strftime('%H:%M')
            need = (tr.is_trading_day(n.strftime('%Y%m%d')) and '07:25' <= hm <= '21:30') or col.STATE['running'] or JOB['signal'] or JOB['bt'] or JOB['bf']
            if need != on:
                ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | (ES_SYSTEM_REQUIRED if need else 0))
                db.log('PC 잠들지 않게 유지' if need else 'PC 절전 허용 (윈도우 절전 설정대로)')
                on = need
        except Exception:
            pass
        time.sleep(60)


def main():
    import uvicorn
    threading.Thread(target=keep_awake, daemon=True).start()
    tr.NOTIFY = lambda m: telegram(m)
    tk_kis.HOOK[0] = J.api_hit
    threading.Thread(target=scheduler, daemon=True).start()
    threading.Thread(target=tr.loop, args=(lambda: CFG, lambda: {k: v[0] for k, v in rtws.PRICE.items()}, lambda m: telegram(m)), daemon=True).start()

    def on_notice():
        if tr._lock.acquire(timeout=30):
            try:
                tr.sync(tr.client(CFG), tr.today())
            except Exception as e:
                db.log(f'체결 통보 반영 오류: {CF.clean(e)}', 'warn')
            finally:
                tr._lock.release()
    rtws.start(lambda: CFG.get('ws_on', True) and tr.configured(CFG), lambda: tr.client(CFG),
               lambda: list(dict.fromkeys(l['ticker'] for l in tr.open_lots() if l['status'] == '보유')),
               lambda: tr.is_trading_day() and '08:30' <= datetime.now().strftime('%H:%M') <= '15:35', on_notice)
    print(f"""
╔══════════════════════════════════════════════╗
║   🏦 TK자동매매 시스템 {APP_VERSION}                      ║
║   http://127.0.0.1:{PORT}   (이 PC에서만 접속)        ║
║   지금 모드: {'🔴 실전' if db.mode() == 'real' else '🟢 모의'}                               ║
╚══════════════════════════════════════════════╝
""", flush=True)
    db.log(f'{APP_NAME} {APP_VERSION} 시작 · 모드 {db.mode()} · 데이터 {db.DATA_DIR} · 비밀 값 {"DPAPI 암호화" if CF.protected() else "base64(윈도우 아님)"}')
    uvicorn.run(app, host='127.0.0.1', port=PORT, log_level='warning')


if __name__ == '__main__':
    main()
