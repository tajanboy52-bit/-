"""
q_server.py — 📈 台炅 퀀트 자동매매 (TK Quant) · 포트 8085 · KIS 모의투자 전용 (실전 주소 없음)

Scout(8082) · Danta(8083) · Bluechip(8084)와 별개인 네 번째 프로그램. 데이터 %APPDATA%\\TKQuant · 텔레그램 머리말 [퀀트]
Scout 데이터(전종목 일봉 · 수급 · ETF)는 읽기 전용 — Scout가 켜져 있어 매일 15:50 일봉 · 18:10 수급을 모아야 함
"""
import asyncio
import csv
import io
import json
import os
import sys
import threading
import time
import urllib.error
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

import q_db as db
import q_signals as S
import q_trader as tr
import q_ws as rtws

APP_NAME = '台炅 퀀트 자동매매 (TK Quant)'
APP_VERSION = 'Q1.0'
PORT = int(os.environ.get('TKQUANT_PORT', '8085'))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(db.DATA_DIR, 'quant_config.json')
SECRET_KEYS = ('kis_app_key', 'kis_app_secret', 'kis_account', 'kis_hts_id', 'krx_id', 'krx_pw', 'telegram_token', 'telegram_chat')
DEFAULT_CFG = {'kis_on': False, 'cap': 10_000_000, 'alloc': dict(tr.DEFAULT_ALLOC), 'signal_time': '18:40', 'ws_on': True,
               'dd_limit': 15, 'day_loss_limit': 4, 'pause_buy': False}
app = FastAPI(title=APP_NAME)
JOB = {'running': False, 'msg': '', 'err': '', 'bt_running': False, 'bt_msg': ''}


def load_cfg():
    c = json.loads(json.dumps(DEFAULT_CFG))
    for p in (CONFIG_FILE, CONFIG_FILE + '.bak'):
        if os.path.exists(p):
            try:
                c.update(json.load(open(p, encoding='utf-8-sig')))
                break
            except Exception:
                pass
    return c


def save_cfg(c):
    tmp = CONFIG_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(c, f, ensure_ascii=False, indent=1)
    os.replace(tmp, CONFIG_FILE)
    try:
        import shutil
        shutil.copyfile(CONFIG_FILE, CONFIG_FILE + '.bak')
    except Exception:
        pass


CFG = load_cfg()


def mask(v, keep=4):
    v = str(v or '')
    return '' if not v else '●' * min(8, max(4, len(v) - keep)) + (v[-keep:] if len(v) > keep + 2 else '')


def clean(e):
    m = str(e)
    for k in SECRET_KEYS:
        if CFG.get(k):
            m = m.replace(str(CFG[k]), '●●●●')
    return m[:300]


def telegram(msg):
    t, ch = CFG.get('telegram_token'), CFG.get('telegram_chat')
    if not (t and ch):
        return False, '텔레그램 미설정'
    try:
        body = urllib.parse.urlencode({'chat_id': ch, 'text': '[퀀트] ' + msg[:3900]}).encode()
        with urllib.request.urlopen(f'https://api.telegram.org/bot{t}/sendMessage', data=body, timeout=10) as r:
            return bool(json.loads(r.read().decode()).get('ok')), ''
    except Exception as e:
        db.log(f'텔레그램 실패: {clean(e)}', 'warn')
        return False, clean(e)


# ════════════════════════════════════════════
#  장 마감 뒤 신호 계산 · 리포트
# ════════════════════════════════════════════
def signal_run(d, force=False):
    if JOB['running']:
        return False
    JOB.update(running=True, msg=f'{d} 신호 계산 중', err='')
    try:
        tr.signal_job(CFG, d)
        JOB['msg'] = f'{d} 신호 계산 끝'
        telegram(report(d))
        return True
    except Exception as e:
        JOB['err'] = clean(e)
        db.log(f'신호 계산 실패: {clean(e)}', 'error')
        return False
    finally:
        JOB['running'] = False


def report(d):
    x = db.conn()
    wd = '월화수목금토일'[datetime.strptime(d, '%Y%m%d').weekday()]
    L = [f'📈 퀀트 {d[4:6]}/{d[6:]}({wd}) 장 마감', '━━━━━━━━━━━━━━']
    eq = [dict(r) for r in x.execute('SELECT * FROM equity WHERE date<=? ORDER BY date DESC LIMIT 2', (d,))]
    sv = float(db.meta_get('start_value') or 0)
    if eq and eq[0]['date'] == d:
        e, p = eq[0], (eq[1] if len(eq) > 1 else None)
        L.append(f"💰 계좌 {e['value']:,.0f}원" + (f" · 오늘 {e['value'] - p['value']:+,.0f}원 ({(e['value'] / p['value'] - 1) * 100:+.2f}%)" if p else '')
                 + (f" · 시작 대비 {(e['value'] / sv - 1) * 100:+.2f}%" if sv else ''))
        L.append(f"   고점 대비 {(e['value'] / e['peak'] - 1) * 100:+.1f}% · 현금(D+2) {e['cash'] / 1e4:,.0f}만")
    for s, m in S.SLEEVES.items():
        ls = [l for l in tr.open_lots(s) if l['status'] == '보유']
        val = sum(l['qty'] * (l['last_px'] or l['entry_px'] or 0) for l in ls)
        inv = sum(l['cost'] * (l['qty'] / l['qty0'] if l['qty0'] else 1) for l in ls)
        cl = [dict(r) for r in x.execute("SELECT pnl, ret FROM lots WHERE sleeve=? AND status='청산'", (s,))]
        L.append(f"{m['icon']} {m['name']}: 보유 {len(ls)} · 평가 {val - inv:+,.0f}원 · 누적 실현 {sum(r['pnl'] or 0 for r in cl):+,.0f}원"
                 + (f" · 청산 {len(cl)}건 승률 {sum(1 for r in cl if (r['pnl'] or 0) > 0) / len(cl) * 100:.0f}%" if cl else ''))
    fills = [dict(r) for r in x.execute('SELECT * FROM orders WHERE date=? AND filled>0 ORDER BY id', (d,))]
    if fills:
        L.append('\n🧾 오늘 체결')
        L += [f" {'🟢' if f['side'] == 'buy' else '🔵'} [{f['sleeve']}] {f['name']} {f['filled']}주 @{f['avg'] or 0:,.0f} · {tr.KIND.get(f['kind'], f['kind'])}" for f in fills[:25]]
    done = [dict(r) for r in x.execute("SELECT * FROM lots WHERE exit_date=? AND status='청산'", (d,))]
    if done:
        L.append(f"\n💵 청산 {len(done)}건 · {sum(r['pnl'] or 0 for r in done):+,.0f}원")
        L += [f" {'🟢' if (r['pnl'] or 0) > 0 else '🔻'} [{r['sleeve']}] {r['name']} {r['ret'] or 0:+.2f}% ({tr.KIND.get(r['sell_reason'], r['sell_reason'] or '')})" for r in done[:20]]
    sells = [dict(r) for r in x.execute("SELECT * FROM lots WHERE status='보유' AND sell_flag=1 AND sleeve!='ON'")]
    sig = [dict(r) for r in x.execute('SELECT * FROM signals WHERE date=? AND rank<100 ORDER BY sleeve, rank', (d,))]
    L.append(f'\n🗓 다음 거래일 08:35')
    L.append(' 매도: ' + (', '.join(f"[{l['sleeve']}] {l['name']}({tr.KIND.get(l['sell_reason'], '')})" for l in sells) or '없음'))
    for s in ('LVH', 'REV', 'DV'):
        ss = [r['name'] for r in sig if r['sleeve'] == s]
        if ss:
            L.append(f" {S.SLEEVES[s]['icon']} 후보: {', '.join(ss[:8])}" + (f' 외 {len(ss) - 8}' if len(ss) > 8 else ''))
    flags = []
    if tr.halted():
        flags.append(f'⛔ 정지: {tr.halted()}')
    if db.meta_get('auto_pause'):
        flags.append(f"⏸ 안전장치 매수 중지: {db.meta_get('auto_pause')}")
    if db.meta_get('block_new'):
        flags.append(f"⚠️ 새 매수 차단: {db.meta_get('block_new')}")
    if not CFG.get('kis_on'):
        flags.append('○ 자동주문 꺼짐')
    return '\n'.join(L + ([''] + flags if flags else []))


def scheduler():
    time.sleep(12)
    waited = ''
    while True:
        try:
            n = datetime.now()
            d = n.strftime('%Y%m%d')
            hm = n.strftime('%H:%M')
            if tr.is_trading_day(d) and hm >= CFG.get('signal_time', '18:40') and db.meta_get('last_signal_date') != d and not JOB['running']:
                if db.scout_last() >= d:
                    fl = db.flows_last()
                    if fl >= d or hm >= '20:00':
                        if fl < d:
                            db.log(f'{d} 수급이 20시까지 안 들어옴 → 전날까지 수급으로 계산 (Scout 18:10 수집 확인)', 'warn')
                        signal_run(d)
                elif hm >= '21:00' and waited != d:
                    waited = d
                    db.log(f'{d} Scout 일봉이 21시까지 없음 (Scout가 켜져 있는지 · 휴장인지 확인)', 'warn')
                    tr.alert(f'{d} Scout 일봉이 21시까지 없어 신호를 못 만듦 — Scout 확인', 'noscout')
        except Exception as e:
            db.log(f'일정 오류: {clean(e)}', 'error')
        time.sleep(60)


# ════════════════════════════════════════════
#  API
# ════════════════════════════════════════════
@app.get('/', response_class=HTMLResponse)
async def index():
    return open(os.path.join(BASE_DIR, 'q_app.html'), encoding='utf-8').read()


def _state():
    x = db.conn()
    d = tr.today()
    eq = [dict(r) for r in x.execute('SELECT * FROM equity ORDER BY date')]
    lots = [dict(r) for r in x.execute("SELECT * FROM lots WHERE status IN ('주문','보유') ORDER BY sleeve, entry_date")]
    live = rtws.PRICE
    for l in lots:
        lp = live.get(l['ticker'], (None,))[0] or l['last_px'] or l['entry_px'] or 0
        l['px'] = lp
        l['cost_left'] = l['cost'] * (l['qty'] / l['qty0']) if l['qty0'] else l['cost']
        l['eval'] = l['qty'] * lp - l['cost_left'] if l['qty'] else 0
        l['eval_pct'] = (lp / l['entry_px'] - 1) * 100 if l['entry_px'] else None
    closed = [dict(r) for r in x.execute("SELECT * FROM lots WHERE status='청산' ORDER BY exit_date DESC, id DESC LIMIT 300")]
    al = tr.alloc(CFG)
    base = min(eq[-1]['value'] if eq else tr.cap(CFG), tr.cap(CFG))
    sleeves = []
    for s, m in S.SLEEVES.items():
        ls = [l for l in lots if l['sleeve'] == s and l['status'] == '보유']
        cl = [r for r in x.execute("SELECT pnl, ret FROM lots WHERE sleeve=? AND status='청산'", (s,))]
        val = sum(l['qty'] * l['px'] for l in ls)
        sleeves.append({'key': s, **m, 'pct': al.get(s, 0), 'limit': base * al.get(s, 0) / 100, 'value': val, 'npos': len(ls),
                        'slots': tr.SLOTS.get(s, 1), 'eval': sum(l['eval'] for l in ls), 'realized': sum(r[0] or 0 for r in cl), 'closed': len(cl),
                        'win': sum(1 for r in cl if (r[0] or 0) > 0) / len(cl) * 100 if cl else None,
                        'avg': sum(r[1] or 0 for r in cl) / len(cl) if cl else None,
                        'curve': [[r[0], r[1]] for r in x.execute('SELECT date, value + realized FROM sleeve_daily WHERE sleeve=? ORDER BY date', (s,))]})
    sd = db.meta_get('last_signal_date')
    lastq = eq[-1] if eq else None
    pl = tr.plan(CFG, lastq['value'] if lastq else None, lastq['cash'] if lastq else None, sd) if sd else {'sells': [], 'buys': [], 'defer': []}
    sig = [dict(r) for r in x.execute('SELECT * FROM signals WHERE date=? ORDER BY sleeve, rank', (sd,))] if sd else []
    sv = float(db.meta_get('start_value') or 0)
    bt = None
    p = os.path.join(db.DATA_DIR, 'backtest_result.json')
    if os.path.exists(p):
        try:
            bt = json.load(open(p, encoding='utf-8'))
        except Exception:
            bt = None
    return {'app': APP_NAME, 'version': APP_VERSION, 'now': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'trading_day': tr.is_trading_day(d),
            'kis_on': bool(CFG.get('kis_on')), 'configured': bool(CFG.get('kis_app_key') and CFG.get('kis_account')), 'halt': tr.halted(),
            'auto_pause': db.meta_get('auto_pause'), 'pause_buy': bool(CFG.get('pause_buy')), 'block_new': db.meta_get('block_new'),
            'equity': eq[-1] if eq else None, 'start_value': sv, 'curve': [[r['date'], r['value']] for r in eq], 'sleeves': sleeves,
            'lots': lots, 'closed': closed, 'orders': [dict(r) for r in x.execute('SELECT * FROM orders ORDER BY id DESC LIMIT 200')],
            'signal_date': sd, 'signals': sig, 'plan': {'sells': pl['sells'], 'buys': pl['buys'], 'defer': pl['defer']},
            'scout': {'path': db.scout_path(), 'last': db.scout_last(), 'flows': db.flows_last(), 'etf': bool(db.etf_path())},
            'job': dict(JOB), 'trader': dict(tr.STATE), 'ws': {**rtws.status(), 'enabled': CFG.get('ws_on', True), 'hts': bool(CFG.get('kis_hts_id'))},
            'alloc': al, 'cap': tr.cap(CFG), 'slots': tr.SLOTS, 'backtest': bt,
            'cfg': {**{k: mask(CFG.get(k)) for k in SECRET_KEYS}, 'signal_time': CFG.get('signal_time'), 'dd_limit': CFG.get('dd_limit'),
                    'day_loss_limit': CFG.get('day_loss_limit'), 'ws_on': CFG.get('ws_on', True)},
            'log': [dict(r) for r in x.execute('SELECT * FROM log ORDER BY id DESC LIMIT 250')]}


@app.get('/api/state')
async def api_state():
    return await asyncio.to_thread(_state)


@app.post('/api/config')
async def api_config(req: Request):
    b = await req.json()
    for k in SECRET_KEYS:
        if b.get(k):
            CFG[k] = str(b[k]).strip()
    if 'alloc' in b:
        try:
            a = {k: float(b['alloc'][k]) for k in tr.DEFAULT_ALLOC}
        except Exception:
            return JSONResponse({'ok': False, 'error': '칸 비율은 숫자'}, 400)
        if any(v < 0 or v > 80 for v in a.values()) or sum(a.values()) > 100:
            return JSONResponse({'ok': False, 'error': '칸마다 0~80% · 합계 100% 이하'}, 400)
        CFG['alloc'] = a
        db.log('칸 비율 변경: ' + ' · '.join(f'{k} {v:g}%' for k, v in a.items()) + ' (다음 주문부터)')
    for k, lo, hi in (('cap', 1_000_000, 1_000_000_000), ('dd_limit', 5, 50), ('day_loss_limit', 1, 20)):
        if b.get(k) not in (None, ''):
            try:
                v = float(str(b[k]).replace(',', ''))
                if not lo <= v <= hi:
                    raise ValueError
                CFG[k] = int(v) if k == 'cap' else v
            except ValueError:
                return JSONResponse({'ok': False, 'error': f'{k} 범위 {lo}~{hi}'}, 400)
    if b.get('signal_time'):
        t = str(b['signal_time'])
        if not ('17:00' <= t <= '23:00' and len(t) == 5):
            return JSONResponse({'ok': False, 'error': '신호 계산 시각 17:00~23:00'}, 400)
        CFG['signal_time'] = t
    if 'ws_on' in b:
        CFG['ws_on'] = bool(b['ws_on'])
    save_cfg(CFG)
    return {'ok': True}


@app.post('/api/kis/test')
async def api_kis_test():
    def t():
        kc = tr.client(CFG)
        b = kc.balance()
        return {'ok': True, 'account': kc.masked_account, 'cash': b['cash'], 'equity': b['equity'],
                'holdings': [f"{p['name']} {p['qty']}주" for p in b['positions']][:20]}
    try:
        return await asyncio.to_thread(t)
    except Exception as e:
        return JSONResponse({'ok': False, 'error': clean(e)}, 400)


@app.post('/api/on')
async def api_on(req: Request):
    b = await req.json()
    on = bool(b.get('on'))
    if on and not (CFG.get('kis_app_key') and CFG.get('kis_app_secret') and CFG.get('kis_account')):
        return JSONResponse({'ok': False, 'error': 'KIS 모의투자 키 · 계좌를 먼저 저장하세요'}, 400)
    CFG['kis_on'] = on
    save_cfg(CFG)
    db.log(f"자동주문 {'ON' if on else 'OFF'} (사용자)")
    return {'ok': True}


@app.post('/api/pause')
async def api_pause(req: Request):
    b = await req.json()
    CFG['pause_buy'] = bool(b.get('pause'))
    if not CFG['pause_buy']:
        db.meta_set('auto_pause', '')
    save_cfg(CFG)
    db.log(f"새 매수 {'일시 중지' if CFG['pause_buy'] else '재개 (안전장치 해제 포함)'} (사용자)")
    return {'ok': True}


@app.post('/api/halt_clear')
async def api_halt_clear():
    db.meta_set('halt', '')
    db.log('정지 해제 (사용자)')
    return {'ok': True}


@app.post('/api/emergency')
async def api_emergency():
    def e():
        CFG['kis_on'] = False
        save_cfg(CFG)
        tr.halt('긴급 정지 (사용자) — 미체결 주문 취소')
        n = 0
        try:
            kc = tr.client(CFG)
            for o in db.conn().execute("SELECT * FROM orders WHERE date=? AND status IN ('접수','부분')", (tr.today(),)).fetchall():
                try:
                    kc.cancel(o['order_no'], o['org_no'])
                    n += 1
                except Exception as ex:
                    db.log(f"취소 실패 {o['name']}: {clean(ex)}", 'warn')
        except Exception as ex:
            db.log(f'긴급 정지 중 오류: {clean(ex)}', 'error')
        return n
    return {'ok': True, 'cancelled': await asyncio.to_thread(e)}


@app.post('/api/sell')
async def api_sell(req: Request):
    """묶음 하나를 다음 장전(또는 장중이면 지금) 시장가로 — 장중이면 바로 주문"""
    b = await req.json()
    lid = int(b.get('id'))

    def s():
        x = db.conn()
        l = x.execute("SELECT * FROM lots WHERE id=? AND status='보유'", (lid,)).fetchone()
        if not l:
            raise RuntimeError('보유 중인 묶음이 아님')
        hm = datetime.now().strftime('%H:%M')
        if tr.is_trading_day() and '09:00' <= hm <= '15:19' and tr.can_order(CFG):
            with tr._lock:
                if not tr.send(CFG, tr.client(CFG), 'sell', 'manual', l['id'], l['sleeve'], l['ticker'], l['name'], l['qty']):
                    raise RuntimeError('주문 실패 — 로그 확인')
            return '지금 시장가 매도 주문'
        x.execute("UPDATE lots SET sell_flag=1, sell_reason='manual' WHERE id=?", (lid,))
        x.commit()
        return '다음 장전 08:35 매도 예약'
    try:
        m = await asyncio.to_thread(s)
        db.log(f'수동 매도: 묶음 {lid} — {m} (사용자)')
        return {'ok': True, 'msg': m}
    except Exception as e:
        return JSONResponse({'ok': False, 'error': clean(e)}, 400)


@app.post('/api/sync')
async def api_sync():
    try:
        await asyncio.to_thread(lambda: tr.sync(tr.client(CFG)))
        return {'ok': True}
    except Exception as e:
        return JSONResponse({'ok': False, 'error': clean(e)}, 400)


@app.post('/api/job/signal')
async def api_job_signal():
    last = db.scout_last()
    if not last:
        return JSONResponse({'ok': False, 'error': 'Scout 일봉 없음'}, 400)
    threading.Thread(target=signal_run, args=(last, True), daemon=True).start()
    return {'ok': True, 'date': last}


@app.post('/api/job/backtest')
async def api_job_backtest(req: Request):
    b = await req.json()
    if JOB['bt_running']:
        return {'ok': False, 'error': '이미 계산 중'}

    def run():
        JOB.update(bt_running=True, bt_msg='시작')
        try:
            import q_backtest
            al = {k: v / 100 for k, v in tr.alloc(CFG).items()}
            q_backtest.run(b.get('start') or '20231024', b.get('end') or '99999999', al, tr.cap(CFG), progress=lambda m: JOB.update(bt_msg=m))
        except Exception as e:
            JOB['bt_msg'] = f'오류: {clean(e)}'
            db.log(f'백테스트 오류: {clean(e)}', 'error')
        finally:
            JOB['bt_running'] = False
    threading.Thread(target=run, daemon=True).start()
    return {'ok': True}


@app.post('/api/tg_test')
async def api_tg_test():
    ok, err = await asyncio.to_thread(telegram, f'✅ 연결 테스트 {datetime.now():%Y-%m-%d %H:%M}')
    return {'ok': ok, 'error': err}


@app.get('/api/export')
async def api_export():
    """Claude 점검용 zip — 비밀 값 없음"""
    def build():
        c = db.conn()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z:
            for t in ('lots', 'orders', 'equity', 'sleeve_daily', 'signals', 'days', 'log'):
                cur = c.execute(f'SELECT * FROM {t}')
                s = io.StringIO()
                w = csv.writer(s)
                w.writerow([d[0] for d in cur.description])
                w.writerows(cur.fetchall())
                z.writestr(f'{t}.csv', '﻿' + s.getvalue())
            for f in ('backtest_result.md', 'backtest_result.json'):
                p = os.path.join(db.DATA_DIR, f)
                if os.path.exists(p):
                    z.write(p, f)
            z.writestr('summary.json', json.dumps({'app': APP_NAME, 'version': APP_VERSION, 'made': db.now_s(), 'alloc': tr.alloc(CFG), 'cap': tr.cap(CFG),
                                                   'start_value': db.meta_get('start_value')}, ensure_ascii=False, indent=1))
        return buf.getvalue()
    data = await asyncio.to_thread(build)
    return Response(content=data, media_type='application/zip', headers={'Content-Disposition': f'attachment; filename="quant_{datetime.now():%Y%m%d}.zip"'})


if __name__ == '__main__':
    import uvicorn
    try:
        db.seed_import()
    except Exception as e:
        db.log(f'초기 자료 실패: {e}', 'warn')
    tr.NOTIFY = lambda m: telegram(m)
    threading.Thread(target=scheduler, daemon=True).start()
    threading.Thread(target=tr.loop, args=(lambda: CFG,), daemon=True).start()
    rtws.start(lambda: CFG)
    print(f"""
╔════════════════════════════════════════════╗
║   📈 台炅 퀀트 자동매매 (TK Quant) {APP_VERSION}       ║
║   http://localhost:{PORT}                    ║
║   KIS 모의투자 전용 (실전 주문 없음)           ║
╚════════════════════════════════════════════╝
""", flush=True)
    db.log(f'{APP_NAME} {APP_VERSION} 시작 · 데이터 {db.DATA_DIR} · Scout {db.scout_path() or "없음"}')
    uvicorn.run(app, host='127.0.0.1', port=PORT, log_level='warning')
