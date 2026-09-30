"""
bluechip_server.py — 💎 台炅 우량주 반등 (TK Bluechip) · 포트 8084

※ 가상추천매매 Scout(8082) · 자동단타매매 Danta(8083)와 완전히 별개인 세 번째 프로그램
   데이터: %APPDATA%\\TKBluechip (bluechip.db · bluechip_config.json) · 텔레그램 머리말 [우량주]
   일봉은 Scout(scout.db)를 읽기 전용으로만 사용 (Scout 파일은 바꾸지 않음) · 주문 기능 없음 (가상 매매만)

하루 흐름 (평일)
  16:40 이후 Scout 일봉에 오늘 날짜가 들어오면 → ① 어제 신호의 오늘 시가 가상 체결 ② 보유 종목 +5% 익절 · 40일 만료 판단
  ③ 모델별 가상 계좌 평가 ④ 오늘 종가로 새 신호(내일 시가 매수 예약) ⑤ 텔레그램 요약
  18:30 이후 Scout에 당일 확정 수급(18:10 수집)이 들어오면 → 수급 요약 다시 계산 (기록 · 화면용, 매수 판단엔 안 씀)
  매월 첫 거래일: KRX에서 코스피200 · 코스닥150 구성 종목 · 업종 · 재무를 받아 '우량주 100'을 다시 뽑음 (KRX 계정 필요)
"""
import asyncio
import csv
import io
import json
import os
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

import bluechip_db as db
import bluechip_engine as eng
import bluechip_broker as brk

APP_NAME = '台炅 우량주 반등 (TK Bluechip)'
APP_VERSION = 'B0.6'
PORT = int(os.environ.get('BLUECHIP_PORT', '8084'))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(db.DATA_DIR, 'bluechip_config.json')
START_DATE = '20260929'                 # 가상 검증 시작일 (9/28 종가 신호 → 9/29 시가 매수)
DEFAULT_CFG = {'krx_id': '', 'krx_pw': '', 'telegram_token': '', 'telegram_chat': '', 'run_time': '16:40', 'start_date': START_DATE}
SECRET_KEYS = ('krx_id', 'krx_pw', 'telegram_token', 'telegram_chat', 'kis_app_key', 'kis_app_secret', 'kis_account')
log = eng.log
app = FastAPI(title=APP_NAME)


def load_cfg():
    c = dict(DEFAULT_CFG)
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


def mask(v, keep=4):
    v = str(v or '')
    if not v:
        return ''
    return '●' * min(8, max(4, len(v) - keep)) + (v[-keep:] if len(v) > keep + 2 else '')


CFG = load_cfg()


def telegram(msg, raise_err=False):
    """실패하면 로그 파일에도 남김 (Danta 교훈)"""
    t, ch = CFG.get('telegram_token'), CFG.get('telegram_chat')
    if not (t and ch):
        return False, '텔레그램 미설정'
    try:
        body = urllib.parse.urlencode({'chat_id': ch, 'text': '[우량주] ' + msg[:3900]}).encode()
        with urllib.request.urlopen(f'https://api.telegram.org/bot{t}/sendMessage', data=body, timeout=10) as r:
            ok = json.loads(r.read().decode()).get('ok')
        return bool(ok), ''
    except urllib.error.HTTPError as e:
        try:
            desc = json.loads(e.read().decode(errors='ignore')).get('description', '')
        except Exception:
            desc = ''
        err = f'HTTP {e.code} {desc}'.strip()
    except Exception as e:
        err = str(e)[:150]
    log(f'텔레그램 전송 실패: {err}')
    return False, err


def flows_for(pairs):
    """[(date, ticker)] → {(date, ticker): flowsnap 행}"""
    c = db.conn()
    out = {}
    for d in {p[0] for p in pairs if p[0]}:
        for r in c.execute('SELECT * FROM flowsnap WHERE date=?', (d,)):
            out[(d, r['ticker'])] = dict(r)
    return out


def attach_flows(rows, dkey):
    fx = flows_for([(r.get(dkey), r.get('ticker')) for r in rows])
    for r in rows:
        f = fx.get((r.get(dkey), r.get('ticker'))) or {}
        r['flow'] = {k: f.get(k) for k in ('flow_date', 'fo1', 'ins1', 'pen1', 'fo5', 'ins5', 'pen5', 'sm5r')} if f else None
    return rows


def mkt_flow(d):
    r = db.conn().execute("SELECT * FROM flowsnap WHERE date=? AND ticker='_MKT'", (d,)).fetchone()
    return dict(r) if r else None


def eok(v):
    return '-' if v is None else f'{v / 100:+,.0f}억'


def day_text(d):
    """장 마감 텔레그램 리포트 (B0.6 — 모의투자 체결 · 가상 모델 평가/오늘 손익 · 매수/매도 건별 · 내일 예약). 실패하면 예전 형식"""
    try:
        return _day_text(d)
    except Exception as e:
        log(f'새 리포트 실패 → 예전 형식: {e}')
        return _day_text_old(d)


def _day_text(d):
    c = db.conn()
    wd = '월화수목금토일'[datetime.strptime(d, '%Y%m%d').weekday()]
    L = [f'💎 우량주 {d[4:6]}/{d[6:]}({wd}) 장 마감 실적', '━━━━━━━━━━━━━━']
    try:
        k = brk.day_lines(d)
        if k:
            L += k + ['']
    except Exception as e:
        log(f'모의투자 리포트 줄 실패: {e}')
    L.append('💰 가상 모델 (각 1,000만 시작 · 주문 없음)')
    for s_ in eng.model_summary():
        m = s_['key']
        cv = s_['curve']
        prev = cv[-2][1] if len(cv) > 1 else eng.CAP0
        dd = s_['value'] - prev if cv and cv[-1][0] == d else 0
        nb = c.execute('SELECT COUNT(*) FROM trades WHERE model=? AND entry_date=?', (m, d)).fetchone()[0]
        sold = [dict(r) for r in c.execute("SELECT ret, pnl FROM trades WHERE model=? AND exit_date=? AND status='청산'", (m, d))]
        L.append(f" [{m} {s_['name']}] {s_['value'] / 1e4:,.0f}만 {s_['ret']:+.2f}% · 오늘 {dd:+,.0f}원 · 보유 {s_['npos']}"
                 + (f" · 매수 {nb}" if nb else '') + (f" · 매도 {len(sold)} ({sum(r['pnl'] or 0 for r in sold):+,.0f}원)" if sold else ''))
    buys = [dict(r) for r in c.execute("SELECT * FROM trades WHERE entry_date=? AND model!='Z' ORDER BY model, id", (d,))]
    if buys:
        L.append(f"\n🟢 오늘 매수 {len(buys)}건 (시가 · Z 제외)")
        L += [f" [{t['model']}] {t['name']} {t['qty']}주 @{t['entry_px']:,.0f}" for t in buys[:15]]
    sells = [dict(r) for r in c.execute("SELECT * FROM trades WHERE exit_date=? AND status='청산' AND model!='Z' ORDER BY model, id", (d,))]
    if sells:
        L.append(f"\n🔴 오늘 매도 {len(sells)}건 · {sum(t['pnl'] or 0 for t in sells):+,.0f}원 (Z 제외)")
        L += [f" {'🟢' if (t['ret'] or 0) > 0 else '🔻'} [{t['model']}] {t['name']} {(t['ret'] or 0):+.2f}% {(t['pnl'] or 0):+,.0f}원 · {t['exit_reason'] or ''} · {t['days']}일"
              for t in sells[:15]]
    od = [dict(r) for r in c.execute("SELECT model, name FROM orders WHERE signal_date=? AND status='대기' ORDER BY model, prio", (d,))]
    by = {}
    for r in od:
        by.setdefault(r['model'], []).append(r['name'])
    L.append('\n🗓 내일 시가 매수 예약' + ('' if by else ' 없음 (조건 맞는 우량주 없음)'))
    for m in ('H', 'A', 'B'):
        if by.get(m):
            L.append(f" [{m}] " + ', '.join(by[m]) + (' ← 모의투자도 08:35 주문' if m == 'H' and CFG.get('kis_on') else ''))
    mk = c.execute('SELECT * FROM market WHERE date=?', (d,)).fetchone()
    if mk and mk['breadth'] == mk['breadth']:
        mf = mkt_flow(d)
        L.append(f"\n🌡 시장 공포 온도 {mk['breadth'] * 100:.0f}% · 지수 5일 {mk['mkt_r5'] * 100:+.1f}%"
                 + (f"\n   수급 5일: 외국인 {eok(mf['fo5'])} · 기관 {eok(mf['ins5'])} · 연기금 {eok(mf['pen5'])}" if mf else ''))
    acc = [s_ for s_ in eng.model_summary() if s_['closed']]
    if acc:
        L.append('\n📈 누적 청산')
        L += [f" {s_['key']}: {s_['closed']}건 · 승률 {s_['win']:.0f}% · 건당 {s_['avg']:+.2f}% · 최대 낙폭 {s_['mdd']:.1f}%" for s_ in acc]
    return '\n'.join(L)


def _day_text_old(d):
    """장 마감 뒤 텔레그램 거래 실적 리포트 (B0.3 — 건별 매수 · 매도 줄 없이 하루 한 통)"""
    c = db.conn()
    lines = [f'📊 {d[4:6]}/{d[6:]} 우량주 장 마감 리포트 (가상 · 주문 없음)']
    mk = c.execute('SELECT * FROM market WHERE date=?', (d,)).fetchone()
    if mk and mk['breadth'] == mk['breadth']:
        lines.append(f"시장 공포 온도 {mk['breadth'] * 100:.0f}% · 지수 5일 {mk['mkt_r5'] * 100:+.1f}%")
    mf = mkt_flow(d)
    if mf:
        lines.append(f"수급 5일({mf['flow_date'][4:6]}/{mf['flow_date'][6:]}까지): 외국인 {eok(mf['fo5'])} · 기관 {eok(mf['ins5'])} · 연기금 {eok(mf['pen5'])}")
    for s in eng.model_summary():
        m = s['key']
        nb = c.execute('SELECT COUNT(*) FROM trades WHERE model=? AND entry_date=?', (m, d)).fetchone()[0]
        sold = [dict(r) for r in c.execute('SELECT ret, pnl FROM trades WHERE model=? AND exit_date=?', (m, d))]
        nod = c.execute("SELECT COUNT(*) FROM orders WHERE model=? AND signal_date=? AND status='대기'", (m, d)).fetchone()[0]
        today = f"오늘 매수 {nb} · 매도 {len(sold)}" + (f" ({sum(r['pnl'] or 0 for r in sold):+,.0f}원)" if sold else '') + f" · 내일 예약 {nod}"
        lines.append(f"[{m} {s['name']}] {s['value'] / 1e4:,.0f}만 ({s['ret']:+.2f}%) · 보유 {s['npos']} · {today}")
        if s['closed']:
            lines.append(f"   누적 청산 {s['closed']}건 · 승률 {s['win']:.0f}% · 건당 {s['avg']:+.2f}% · 최대 낙폭 {s['mdd']:.1f}%")
    try:
        lines += brk.day_lines(d)
    except Exception:
        pass
    return '\n'.join(lines)


def daily_job(force=False):
    last_before = max((r[0] for r in db.conn().execute('SELECT date FROM days')), default='')
    msgs = eng.catch_up(CFG)
    last_after = max((r[0] for r in db.conn().execute('SELECT date FROM days')), default='')
    if last_after and last_after != last_before:
        telegram(day_text(last_after))
    return msgs


def scheduler():
    time.sleep(8)
    try:
        daily_job()                                          # 켜질 때 밀린 날 처리
    except Exception as e:
        log(f'시작 계산 오류: {e}')
    try:
        c = db.conn()
        last = c.execute('SELECT MAX(date) FROM daily').fetchone()[0]
        if last and not c.execute('SELECT 1 FROM flowsnap WHERE date=? AND ticker=?', (last, '_MKT')).fetchone():
            k, fd = eng.refresh_flows()                      # 수급 요약이 없는 마지막 날 채우기 (업데이트 직후 등)
            log(f'{last} 수급 요약 계산 (수급 기준일 {fd})')
    except Exception as e:
        log(f'수급 요약 오류: {e}')
    waited = ''
    flow_try = 0.0
    while True:
        try:
            n = datetime.now()
            if n.weekday() < 5 and '18:30' <= n.strftime('%H:%M') <= '21:00' and time.time() - flow_try > 600 and not eng.STATE['running']:
                c = db.conn()
                last = c.execute('SELECT MAX(date) FROM days').fetchone()[0]
                if last:
                    r = c.execute("SELECT flow_date FROM flowsnap WHERE date=? AND ticker='_MKT'", (last,)).fetchone()
                    if not r or r[0] < last:
                        flow_try = time.time()
                        k, fd = eng.refresh_flows()
                        if fd == last:
                            log(f'{last} 당일 수급 반영 (Scout {k:,}행)')
            if n.weekday() < 5 and n.strftime('%H:%M') >= CFG.get('run_time', '16:40'):
                today = n.strftime('%Y%m%d')
                done = db.conn().execute('SELECT 1 FROM days WHERE date=?', (today,)).fetchone()
                if not done and not eng.STATE['running']:
                    last = eng.scout_last()
                    if last >= today:
                        daily_job()
                    elif waited != today and n.strftime('%H:%M') >= '20:00':
                        waited = today
                        log(f'{today} Scout 일봉이 20시까지 안 들어옴 (휴장이거나 Scout 동기화 확인)')
        except Exception as e:
            log(f'일정 오류: {e}')
        time.sleep(60)


# ════════════════════════════════════════════
#  API
# ════════════════════════════════════════════
@app.get('/', response_class=HTMLResponse)
async def index():
    return open(os.path.join(BASE_DIR, 'bluechip.html'), encoding='utf-8').read()


@app.get('/api/dash')
async def api_dash():
    c = db.conn()
    last = max((r[0] for r in c.execute('SELECT date FROM days')), default='')
    sig = db.meta_get('last_signal_date') or c.execute('SELECT MAX(signal_date) FROM orders').fetchone()[0] or ''
    mk = [dict(r) for r in c.execute('SELECT * FROM market ORDER BY date DESC LIMIT 60')][::-1]
    pend = [dict(r) for r in c.execute("SELECT * FROM orders WHERE signal_date=? ORDER BY model, prio", (sig,))]
    pos = attach_flows([dict(r) for r in c.execute("SELECT * FROM trades WHERE status='보유' ORDER BY model, entry_date")], 'signal_date')
    attach_flows(pend, 'signal_date')
    recent = [dict(r) for r in c.execute("SELECT * FROM trades WHERE status='청산' ORDER BY exit_date DESC, id DESC LIMIT 40")]
    return {'app': APP_NAME, 'version': APP_VERSION, 'now': datetime.now().strftime('%Y-%m-%d %H:%M'), 'last_day': last,
            'signal_date': sig, 'scout_last': eng.scout_last(), 'scout_ok': bool(db.scout_db_path()), 'state': dict(eng.STATE),
            'models': eng.model_summary(), 'market': mk, 'kis': brk.dash(CFG), 'mflow': mkt_flow(mk[-1]['date']) if mk else None, 'flow_last': eng.flow_last(), 'orders': pend, 'positions': pos, 'recent': recent,
            'start_date': db.meta_get('start_date') or CFG.get('start_date'), 'cap0': eng.CAP0,
            'rules': {'tp': eng.TP, 'hold': eng.HOLD, 'slots': eng.SLOTS, 'maxpos': eng.MAXPOS, 'seccap': eng.SECCAP, 'cost': eng.COST,
                      'tk_threshold': eng.TK['threshold']}}


@app.get('/api/universe')
async def api_universe(month: str = ''):
    c = db.conn()
    month = month or c.execute('SELECT MAX(month) FROM universe').fetchone()[0] or ''
    rows = [dict(r) for r in c.execute('SELECT * FROM universe WHERE month=? ORDER BY rank', (month,))]
    d = c.execute('SELECT MAX(date) FROM daily').fetchone()[0] or ''
    daily = {r['ticker']: dict(r) for r in c.execute('SELECT * FROM daily WHERE date=?', (d,))}
    for r in rows:
        x = daily.get(r['ticker']) or {}
        r.update({k: x.get(k) for k in ('close', 'ma20gap', 'r5', 'r20', 'rsi2', 'score', 'sigA', 'sigB')})
        r['tk'] = eng.tk_pct(x['score']) if x.get('score') is not None else None
        r['date'] = d
    attach_flows(rows, 'date')
    months = [r[0] for r in c.execute('SELECT DISTINCT month FROM universe ORDER BY month DESC LIMIT 24')]
    return {'month': month, 'date': d, 'rows': rows, 'months': months, 'fallback': db.meta_get(f'univ_fallback_{month}')}


@app.get('/api/trades')
async def api_trades(model: str = '', status: str = ''):
    q, a = 'SELECT * FROM trades WHERE 1=1', []
    if model:
        q += ' AND model=?'; a.append(model)
    if status:
        q += ' AND status=?'; a.append(status)
    rows = attach_flows([dict(r) for r in db.conn().execute(q + ' ORDER BY entry_date DESC, id DESC LIMIT 2000', a)], 'signal_date')
    orders = [dict(r) for r in db.conn().execute("SELECT * FROM orders WHERE status NOT IN ('대기') ORDER BY signal_date DESC, model, prio LIMIT 400")]
    return {'rows': rows, 'orders': orders}


@app.get('/api/status')
async def api_status():
    c = db.conn()
    runs = [dict(r) for r in c.execute('SELECT * FROM runs ORDER BY id DESC LIMIT 15')]
    logp = os.path.join(db.DATA_DIR, 'bluechip_server.log')
    tail = open(logp, encoding='utf-8', errors='ignore').read()[-4000:] if os.path.exists(logp) else ''
    return {'app': APP_NAME, 'version': APP_VERSION, 'port': PORT, 'data_dir': db.DATA_DIR, 'scout_db': db.scout_db_path(),
            'scout_last': eng.scout_last(), 'state': dict(eng.STATE), 'runs': runs, 'log': tail,
            'counts': {k: c.execute(f'SELECT COUNT(*) FROM {k}').fetchone()[0] for k in ('members', 'monthly', 'universe', 'trades', 'orders', 'days', 'flows', 'flowsnap')},
            'flow_last': eng.flow_last(), 'flow_first': c.execute('SELECT MIN(date) FROM flows').fetchone()[0] or '',
            'months': [r[0] for r in c.execute('SELECT DISTINCT month FROM members ORDER BY month DESC LIMIT 3')],
            'cfg': {**{k: mask(CFG.get(k)) for k in SECRET_KEYS}, 'run_time': CFG.get('run_time'), 'start_date': db.meta_get('start_date') or CFG.get('start_date')},
            'models': eng.MODELS, 'tk': eng.TK}


@app.post('/api/config')
async def api_config(req: Request):
    b = await req.json()
    for k in SECRET_KEYS:
        if b.get(k):
            CFG[k] = str(b[k]).strip()
    if b.get('run_time'):
        t = str(b['run_time']).strip()
        if not ('16:00' <= t <= '23:00' and len(t) == 5):
            return JSONResponse({'ok': False, 'error': '실행 시각은 16:00 ~ 23:00 (HH:MM)'}, 400)
        CFG['run_time'] = t
    if b.get('kis_cap'):
        try:
            v = int(float(str(b['kis_cap']).replace(',', '')))
            if not 1_000_000 <= v <= 1_000_000_000:
                raise ValueError
            CFG['kis_cap'] = v
        except ValueError:
            return JSONResponse({'ok': False, 'error': '운용 한도는 100만 ~ 10억 원'}, 400)
    save_cfg(CFG)
    return {'ok': True}


# ── 🧾 H1 KIS 모의투자 ──
@app.get('/api/kis')
async def api_kis():
    st = await asyncio.to_thread(brk.status, CFG)
    st['account'] = mask(CFG.get('kis_account'), 2)
    st['key'] = mask(CFG.get('kis_app_key'))
    return st


@app.post('/api/kis/test')
async def api_kis_test():
    def t():
        kc = brk.client(CFG)
        b = kc.balance()
        return {'ok': True, 'account': kc.masked_account, 'cash': b['cash'], 'equity': b['equity'],
                'holdings': [f"{p['name']} {p['qty']}주" for p in b['positions']][:20]}
    try:
        return await asyncio.to_thread(t)
    except Exception as e:
        msg = str(e)
        for k in SECRET_KEYS:
            if CFG.get(k):
                msg = msg.replace(str(CFG[k]), '●●●●')
        return JSONResponse({'ok': False, 'error': msg[:200]}, 400)


@app.post('/api/kis/on')
async def api_kis_on(req: Request):
    b = await req.json()
    on = bool(b.get('on'))
    if on and not (CFG.get('kis_app_key') and CFG.get('kis_app_secret') and CFG.get('kis_account')):
        return JSONResponse({'ok': False, 'error': '모의투자 키 · 계좌를 먼저 저장하세요'}, 400)
    CFG['kis_on'] = on
    save_cfg(CFG)
    brk.log(f"자동주문 {'ON' if on else 'OFF'} (사용자)")
    return {'ok': True}


@app.post('/api/kis/halt_clear')
async def api_kis_halt_clear():
    db.meta_set('kis_halt', '')
    brk.log('정지 해제 (사용자)')
    return {'ok': True}


@app.post('/api/kis/emergency')
async def api_kis_emergency():
    def e():
        CFG['kis_on'] = False
        save_cfg(CFG)
        brk.halt('긴급 정지 (사용자) — 미체결 주문 취소')
        n = 0
        try:
            kc = brk.client(CFG)
            for o in brk.c().execute("SELECT * FROM kis_orders WHERE date=? AND status IN ('접수','부분')", (brk.today(),)).fetchall():
                try:
                    kc.cancel(o['order_no'], o['org_no'])
                    n += 1
                except Exception as ex:
                    brk.log(f"취소 실패 {o['name']}: {ex}", 'warn')
        except Exception as ex:
            brk.log(f'긴급 정지 중 오류: {ex}', 'error')
        return n
    n = await asyncio.to_thread(e)
    return {'ok': True, 'cancelled': n}


def _clean(e):
    msg = str(e)
    for k in SECRET_KEYS:
        if CFG.get(k):
            msg = msg.replace(str(CFG[k]), '●●●●')
    return msg[:200]


@app.get('/api/kis/live')
async def api_kis_live(force: int = 0):
    try:
        return await asyncio.to_thread(brk.live, CFG, bool(force))
    except Exception as e:
        return JSONResponse({'ok': False, 'error': _clean(e)}, 400)


@app.post('/api/kis/sell')
async def api_kis_sell(req: Request):
    b = await req.json()
    try:
        await asyncio.to_thread(brk.manual_sell, CFG, str(b.get('ticker', '')).zfill(6))
        return {'ok': True}
    except Exception as e:
        return JSONResponse({'ok': False, 'error': _clean(e)}, 400)


@app.post('/api/kis/cancel')
async def api_kis_cancel(req: Request):
    b = await req.json()
    try:
        await asyncio.to_thread(brk.cancel_order, CFG, int(b.get('id')))
        return {'ok': True}
    except Exception as e:
        return JSONResponse({'ok': False, 'error': _clean(e)}, 400)


@app.post('/api/kis/pause')
async def api_kis_pause(req: Request):
    b = await req.json()
    CFG['kis_buy_pause'] = bool(b.get('pause'))
    save_cfg(CFG)
    brk.log(f"신규 매수 {'일시 중지' if CFG['kis_buy_pause'] else '재개'} (사용자) — 매도 · 손절은 계속")
    return {'ok': True}


@app.post('/api/kis/sync')
async def api_kis_sync():
    try:
        await asyncio.to_thread(lambda: brk.sync(brk.client(CFG)))
        return {'ok': True}
    except Exception as e:
        return JSONResponse({'ok': False, 'error': str(e)[:200]}, 400)


@app.post('/api/tg_test')
async def api_tg_test():
    ok, err = await asyncio.to_thread(telegram, f'✅ 연결 테스트 {datetime.now():%Y-%m-%d %H:%M}')
    return {'ok': ok, 'error': err}


@app.post('/api/job/{kind}')
async def api_job(kind: str):
    if eng.STATE['running']:
        return {'ok': False, 'error': '이미 계산 중입니다'}
    if kind == 'daily':
        threading.Thread(target=daily_job, daemon=True).start()
        return {'ok': True}
    if kind == 'universe':                                  # 이번 달 우량주 100 다시 뽑기 (KRX 월별 자료부터)
        def _u():
            rid = db.run_start('universe')
            try:
                days = eng.trading_days('20230101', eng.scout_last())
                m = datetime.now().strftime('%Y%m')
                first = next((x for x in days if x[:6] == m), None)
                if not first:
                    raise RuntimeError('이번 달 거래일 일봉이 아직 없음')
                eng.krx_month(first, CFG)
                n = eng.build_universe(m, first, days)
                db.run_end(rid, 'ok', f'{m} 우량주 {n}종목')
            except Exception as e:
                eng.STATE['err'] = f'우량주 100 갱신 실패: {e}'[:200]
                log(eng.STATE['err'])
                db.run_end(rid, 'error', e)
        threading.Thread(target=_u, daemon=True).start()
        return {'ok': True}
    return JSONResponse({'ok': False, 'error': '알 수 없는 작업'}, 400)


@app.get('/api/verify/export')
async def api_verify_export():
    """Claude 점검용 zip — 비밀 값 없음"""
    def build():
        c = db.conn()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z:
            for name, q in (('trades.csv', 'SELECT * FROM trades ORDER BY id'), ('orders.csv', 'SELECT * FROM orders ORDER BY id'),
                            ('equity.csv', 'SELECT * FROM equity ORDER BY date, model'), ('market.csv', 'SELECT * FROM market ORDER BY date'),
                            ('universe.csv', 'SELECT * FROM universe ORDER BY month, rank'), ('daily.csv', 'SELECT * FROM daily ORDER BY date, ticker'),
                            ('days.csv', 'SELECT * FROM days ORDER BY date'), ('flowsnap.csv', 'SELECT * FROM flowsnap ORDER BY date, ticker'),
                            ('kis_orders.csv', 'SELECT * FROM kis_orders ORDER BY id'), ('kis_closed.csv', 'SELECT * FROM kis_closed ORDER BY id'),
                            ('kis_pos.csv', 'SELECT * FROM kis_pos'), ('kis_equity.csv', 'SELECT * FROM kis_equity ORDER BY date'),
                            ('kis_log.csv', 'SELECT * FROM kis_log ORDER BY id'), ('runs.csv', 'SELECT * FROM runs ORDER BY id')):
                cur = c.execute(q)
                s = io.StringIO()
                w = csv.writer(s)
                w.writerow([x[0] for x in cur.description])
                w.writerows(cur.fetchall())
                z.writestr(name, '﻿' + s.getvalue())
            summ = {'app': APP_NAME, 'version': APP_VERSION, 'made': datetime.now().isoformat(timespec='seconds'),
                    'start_date': db.meta_get('start_date') or CFG.get('start_date'), 'models': eng.model_summary(),
                    'rules': {'tp': eng.TP, 'hold': eng.HOLD, 'slots': eng.SLOTS, 'maxpos': eng.MAXPOS, 'seccap': eng.SECCAP, 'cost': eng.COST},
                    'tk': eng.TK, 'scout_last': eng.scout_last()}
            z.writestr('summary.json', json.dumps(summ, ensure_ascii=False, indent=1))
            p = os.path.join(db.DATA_DIR, 'bluechip_server.log')
            if os.path.exists(p):
                txt = open(p, encoding='utf-8', errors='ignore').read()[-200000:]
                for k in SECRET_KEYS:
                    if CFG.get(k):
                        txt = txt.replace(str(CFG[k]), '●●●●')
                z.writestr('server_log.txt', txt)
        return buf.getvalue()
    data = await asyncio.to_thread(build)
    fn = f"bluechip_verify_{datetime.now():%Y%m%d}.zip"
    return Response(content=data, media_type='application/zip', headers={'Content-Disposition': f'attachment; filename="{fn}"'})


if __name__ == '__main__':
    import uvicorn
    if not db.meta_get('start_date'):
        db.meta_set('start_date', CFG.get('start_date') or START_DATE)
    try:
        eng.seed_import(os.path.join(BASE_DIR, 'seed'))
    except Exception as e:
        log(f'초기 자료 넣기 실패: {e}')
    try:
        eng.seed_flows(os.path.join(BASE_DIR, 'seed'))
    except Exception as e:
        log(f'초기 수급 자료 넣기 실패: {e}')
    try:
        brk.c()
        eng.ensure_h_orders()
    except Exception as e:
        log(f'H1 준비 오류: {e}')
    threading.Thread(target=scheduler, daemon=True).start()
    brk.NOTIFY = lambda m: telegram(m)                      # B0.6: 모의투자 중요 경고만 (건별 매매 알림 없음)
    threading.Thread(target=brk.loop, args=(lambda: CFG,), daemon=True).start()
    print(f"""
╔══════════════════════════════════════════╗
║   💎 台炅 우량주 반등 (TK Bluechip) {APP_VERSION}   ║
║   http://localhost:{PORT}                  ║
║   가상 매매 전용 · 주문 기능 없음           ║
║   (Scout 8082 · Danta 8083과 별개)         ║
╚══════════════════════════════════════════╝
""", flush=True)
    log(f'{APP_NAME} {APP_VERSION} 시작 · 데이터 {db.DATA_DIR} · Scout {db.scout_db_path() or "없음"}')
    uvicorn.run(app, host='127.0.0.1', port=PORT, log_level='warning')
