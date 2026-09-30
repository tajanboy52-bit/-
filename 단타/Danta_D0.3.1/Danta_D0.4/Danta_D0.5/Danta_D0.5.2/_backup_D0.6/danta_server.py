"""
danta_server.py — 🎯 台炅 자동단타매매 (TK Danta) · 포트 8083

※ 가상추천매매 시스템(📘 Scout · 포트 8082 · StockScout 폴더)과 완전히 별개인 프로그램
   데이터: %APPDATA%\\TKDanta (danta.db · danta_config.json) · 텔레그램 알림 머리말 [단타]

진행 단계 (주문 기능은 아직 없음 — 코드에 주문 API 호출 자체가 없음)
  ① 분봉 수집 — 거래대금 상위 + 그날 급등주 1분봉을 날마다 쌓고, 과거 분봉도 채움
  ② 전략 검증 — 분봉으로 조정/검증 기간 · 같은 시각 무작위 대조군 · 비용 반영 시험 (Claude)
  ③ 실시간 가상 단타 (D0.2부터 ①과 함께) — 08:55 장전 기록 · 09:01~10:00 매분 급등 후보 탐지 → 가상 매수
       · 10초마다 보유 감시 → 가상 매도. 실제 호가로 체결가 계산 · 비용 0.25% 반영
       · D0.3: 매도 엔진(danta_exit) — 보유 거래일 0~2 · 상한가 오버나잇 · 같은 매수에 매도 프로필 A~E 동시 기록
               분봉 연구실(danta_lab) — 쌓인 1분봉을 되감아 매도 규칙 격자 비교 (앞/뒤 기간 따로)
  ④ 실전 소액 — 가상 기록이 기준을 넘은 뒤
"""
import asyncio
import csv
import io
import json
import os
import sys
import threading
import time
import traceback
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timedelta

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

import danta_db as db
import danta_kis as kis
import danta_live as live
import danta_exit as ex
import danta_lab as lab
import danta_models as mdl

APP_NAME = '台炅 자동단타매매 (TK Danta)'
APP_VERSION = 'D0.6'
PORT = int(os.environ.get('DANTA_PORT', '8083'))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(db.DATA_DIR, 'danta_config.json')
DEFAULT_CFG = {'app_key': '', 'app_secret': '', 'telegram_token': '', 'telegram_chat': '',
               'top_n': 200, 'surge_n': 300, 'backfill_days': 120, 'rps': 8, 'collect_time': '16:30', 'live_on': True}
SECRET_KEYS = ('app_key', 'app_secret', 'telegram_token', 'telegram_chat')

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


db.init()
live.init()
CFG = load_cfg()
if CFG.get('collect_time') == '15:45' and not db.meta_get('mig_ct'):   # D0.1 기본값 → Scout 일봉(15:50) 뒤로 (그날 급등주를 알아야 함)
    CFG['collect_time'] = '16:30'
    db.meta_set('mig_ct', '1')
    if os.path.exists(CONFIG_FILE):
        save_cfg(CFG)
kis.RPS[0] = float(CFG.get('rps') or 8)
JOB = {'running': False, 'kind': '', 'msg': '', 'done': 0, 'total': 0, 'started': '', 'error': ''}
STOP = {'flag': False}
_job_lock = threading.Lock()


def mask(v, keep=4):
    v = str(v or '')
    if not v:
        return ''
    return '●' * min(8, max(4, len(v) - keep)) + (v[-keep:] if len(v) > keep + 2 else '')


def telegram(msg):
    t, ch = CFG.get('telegram_token'), CFG.get('telegram_chat')
    if not (t and ch):
        return
    try:
        body = urllib.parse.urlencode({'chat_id': ch, 'text': '[단타] ' + msg}).encode()
        urllib.request.urlopen(f'https://api.telegram.org/bot{t}/sendMessage', data=body, timeout=10).read()
    except Exception as e:
        print(f'[TG] {e}', flush=True)


LOG_FILE = os.path.join(db.DATA_DIR, 'danta_server.log')


def log(msg):
    line = f"[{datetime.now():%m-%d %H:%M:%S}] {msg}"
    print(line, flush=True)
    try:                                                   # 검증 zip에 넣을 서버 로그 (5MB 넘으면 뒤쪽 절반만 남김)
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > 5_000_000:
            with open(LOG_FILE, 'rb') as f:
                f.seek(-2_500_000, 2)
                tail = f.read()
            with open(LOG_FILE, 'wb') as f:
                f.write(tail)
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception:
        pass


# ════════════════════════════════════════════
#  거래일 · 수집 대상
# ════════════════════════════════════════════
def trading_dates(n):
    """최근 거래일 n개 — Scout 일봉 날짜(읽기 전용)가 있으면 그것, 없으면 평일"""
    f = db.scout_db_path()
    ds = []
    if f:
        import sqlite3
        s = sqlite3.connect(f'file:{f}?mode=ro', uri=True, timeout=30)
        try:
            ds = [r[0] for r in s.execute("SELECT DISTINCT date FROM candles ORDER BY date DESC LIMIT ?", (n + 5,))]
        finally:
            s.close()
    if not ds:
        d = datetime.now()
        while len(ds) < n + 5:
            if d.weekday() < 5:
                ds.append(d.strftime('%Y%m%d'))
            d -= timedelta(days=1)
    today = datetime.now().strftime('%Y%m%d')
    if datetime.now().weekday() < 5 and today not in ds and datetime.now().strftime('%H:%M') >= '15:40':
        ds.insert(0, today)                         # 오늘은 Scout 동기화(15:50) 전이라 목록에 없을 수 있음
    return sorted(set(ds))[-n:]


def ensure_universe(dates):
    """날짜별 수집 대상 — 없으면 만들기 (그날 기준 직전 20거래일 거래대금 순위)"""
    c = db.conn()
    have = {r[0] for r in c.execute("SELECT DISTINCT date FROM universe")}
    need = [d for d in dates if d not in have]
    made = _add_surge_live(dates, set(need))
    if not need:
        return made
    top = int(CFG.get('top_n') or 200)
    u = db.universe_from_scout(need, top)
    for d in need:
        if d in u:
            db.save_universe(d, u[d], 'scout')
            made += 1
    today = datetime.now().strftime('%Y%m%d')
    if today in need and today not in u:           # Scout 일봉이 없을 때 — KIS 거래대금 순위(30종목)
        try:
            items = kis.value_rank(CFG)
            if items:
                db.save_universe(today, items, 'kis_rank')
                made += 1
        except Exception as e:
            log(f'거래대금 순위 조회 실패: {e}')
    _add_surge_live(need, set())
    return made


def _add_surge_live(dates, skip):
    """급등주(그날 장중 +3% 이상 · Scout 일봉) · 가상 단타 탐지 후보를 수집 대상에 더함 — 날짜마다 한 번 (Scout 일봉이 생긴 뒤)"""
    n = 0
    cap = int(CFG.get('surge_n') or 0)
    todo = [d for d in dates if d not in skip and not db.meta_get('surge2_' + d)]
    if cap and todo:
        try:
            sg = db.surgers_from_scout(todo, cap=cap)
        except Exception as e:
            log(f'급등주 목록 실패: {e}')
            sg = {}
        for d, items in sg.items():
            if db.conn().execute("SELECT 1 FROM universe WHERE date=? LIMIT 1", (d,)).fetchone():
                n += db.add_universe(d, items, 'surge')
                db.meta_set('surge2_' + d, '1')
    c = db.conn()
    for d in dates:
        if d in skip or not c.execute("SELECT 1 FROM universe WHERE date=? LIMIT 1", (d,)).fetchone():
            continue
        rows = c.execute("SELECT ticker, MAX(name), MAX(amt) FROM snaps WHERE date=? AND (passed=1 OR buy=1) GROUP BY ticker "
                         "UNION SELECT ticker, MAX(name), 0 FROM vtrades WHERE date=? GROUP BY ticker", (d, d)).fetchall()
        for r in c.execute("SELECT t.ticker, t.name, t.status, t.sell_ts, (SELECT MAX(sell_ts) FROM vexits x WHERE x.trade_id=t.id), "
                           "(SELECT COUNT(*) FROM vexits x WHERE x.trade_id=t.id AND x.status='보유') FROM vtrades t WHERE t.date<?", (d,)):
            last = max((r[3] or ''), (r[4] or '')).replace('-', '')[:8]
            if r[2] == '보유' or r[5] or last >= d:            # 밤을 넘겨 들고 있던 종목 — 다음날 분봉도 받아야 연구실이 되감을 수 있음
                rows.append((r[0], r[1], 0))
        items = [(r[0], r[1], '', r[2] or 0) for r in rows if not db.excluded(r[0], r[1])]
        if items:
            n += db.add_universe(d, items, 'live')
    return n


# ════════════════════════════════════════════
#  수집 작업
# ════════════════════════════════════════════
def set_job(**k):
    JOB.update(k)


def _run(kind, fn):
    if not _job_lock.acquire(blocking=False):
        return False
    STOP['flag'] = False
    started = datetime.now().isoformat(timespec='seconds')
    set_job(running=True, kind=kind, msg='시작', done=0, total=0, started=started, error='')
    try:
        msg = fn()
        db.log_run(kind, 'ok', msg or '', started)
        set_job(running=False, kind='', msg=msg or '완료')
    except Exception as e:
        traceback.print_exc()
        db.log_run(kind, 'error', str(e), started)
        set_job(running=False, kind='', msg='실패', error=str(e)[:200])
        telegram(f'⚠️ {kind} 실패: {str(e)[:150]}')
    finally:
        _job_lock.release()
    return True


def _collect_dates(dates, label, window=None):
    """dates(새것 → 옛것)의 수집 대상 중 아직 안 받은 (종목, 날짜)를 받음. window(): False면 멈춤(장중 보호)"""
    ensure_universe(dates)
    c = db.conn()
    todo = []
    for d in sorted(dates, reverse=True):
        for r in c.execute("SELECT ticker, name FROM universe WHERE date=? ORDER BY rank", (d,)):
            if not db.is_done(r[0], d):
                todo.append((d, r[0], r[1]))
    set_job(total=len(todo), done=0, msg=f'{label} {len(todo)}건')
    ok = fail = empty = 0
    today = datetime.now().strftime('%Y%m%d')
    t0 = time.time()
    for i, (d, tk, nm) in enumerate(todo):
        if STOP['flag']:
            return f'{label} 중단 · 완료 {ok} · 실패 {fail} · 남음 {len(todo) - i}'
        if window and not window():
            return f'{label} 장중이라 멈춤 (장 마감 뒤 이어서) · 완료 {ok} · 남음 {len(todo) - i}'
        try:
            try:
                bars = kis.minute_day(CFG, tk, d)
            except Exception:
                if d != today:
                    raise
                bars = kis.minute_today(CFG, tk, d)       # 오늘분은 당일분봉 API로 대체
            db.save_bars(tk, d, bars)
            if bars:
                ok += 1
            else:
                empty += 1
        except Exception as e:
            fail += 1
            if fail <= 3 or fail % 50 == 0:
                log(f'{nm}({tk}) {d} 분봉 실패: {e}')
            if fail >= 30 and ok == 0:
                raise RuntimeError(f'분봉 조회가 계속 실패합니다 — {e}')
        el = time.time() - t0
        set_job(done=i + 1, msg=f"{label} {i + 1}/{len(todo)} · {d[4:6]}/{d[6:]} {nm} · 남은 시간 약 {int(el / (i + 1) * (len(todo) - i - 1) / 60)}분")
    return f'{label} 완료 · 받음 {ok} · 빈 날 {empty} · 실패 {fail}'


def job_today():
    if not (CFG.get('app_key') and CFG.get('app_secret')):
        return False
    def fn():
        d = datetime.now().strftime('%Y%m%d')
        if datetime.now().weekday() >= 5:
            return '주말 — 오늘 분봉 없음'
        m = _collect_dates([d], '오늘 분봉')
        st = db.conn().execute("SELECT COUNT(*), SUM(n) FROM done WHERE date=? AND n>0", (d,)).fetchone()
        if st[0]:
            telegram(f"📥 {d[4:6]}/{d[6:]} 1분봉 수집 — {st[0]}종목 · {st[1]:,}봉")
        return m
    return _run('today', fn)


def _backfill_window():
    """장중(평일 08:00~16:00)에는 과거 채우기를 멈춤 — 장 마감 뒤 · 주말에 이어서"""
    n = datetime.now()
    return n.weekday() >= 5 or not ('08:00' <= n.strftime('%H:%M') < '16:00')


def _market_guard():
    """버튼으로 시작한 채우기도 장중(평일 08:30~15:40)에는 멈춤 — 가상 단타 탐지와 KIS 호출을 나눠 쓰지 않게 (18:30에 자동으로 이어감)"""
    n = datetime.now()
    return n.weekday() >= 5 or not ('08:30' <= n.strftime('%H:%M') < '15:40')


def job_backfill(force=False):
    if not (CFG.get('app_key') and CFG.get('app_secret')):
        return False
    def fn():
        days = int(CFG.get('backfill_days') or 120)
        dates = trading_dates(days)
        today = datetime.now().strftime('%Y%m%d')
        dates = [d for d in dates if d < today]
        return _collect_dates(dates, '과거 분봉 채우기', _market_guard if force else _backfill_window)
    return _run('backfill', fn)


# ════════════════════════════════════════════
#  일정 (평일)
# ════════════════════════════════════════════
def scheduler():
    last = ''
    time.sleep(10)
    # 켜질 때: 오늘분이 비었고 수집 시각이 지났으면 받기 → 과거 채우기 이어서
    try:
        n = datetime.now()
        if n.weekday() < 5 and n.strftime('%H:%M') >= CFG.get('collect_time', '15:45'):
            job_today()
        if _backfill_window():
            job_backfill()
    except Exception as e:
        log(f'시작 작업 오류: {e}')
    while True:
        try:
            n = datetime.now()
            key = n.strftime('%Y%m%d%H%M')
            if key != last:
                last = key
                hm = n.strftime('%H:%M')
                if n.weekday() < 5 and hm == CFG.get('collect_time', '15:45'):
                    threading.Thread(target=job_today, daemon=True).start()
                elif (n.weekday() < 5 and hm in ('18:30', '22:00', '02:00')) or (n.weekday() >= 5 and n.minute == 0 and n.hour % 3 == 0):
                    threading.Thread(target=job_backfill, daemon=True).start()
        except Exception as e:
            log(f'일정 오류: {e}')
        time.sleep(15)


# ════════════════════════════════════════════
#  실시간 가상 단타 (주문 없음)
# ════════════════════════════════════════════
LIVE = {'open': '', 'day': '', 'pre': '', 'scan': '', 'watch': 0.0, 'sum': '', 'err': '', 'try': 0.0}


def _say(msgs):
    for m in msgs or []:
        log(m)
        telegram(m)


def live_tick(now=None):
    """1초마다 불림 — 할 일이 있을 때만 KIS 호출"""
    n = now or datetime.now()
    if n.weekday() >= 5 or not CFG.get('live_on', True) or not (CFG.get('app_key') and CFG.get('app_secret')):
        return
    d, hm = n.strftime('%Y%m%d'), n.strftime('%H:%M')
    if LIVE['day'] != d:
        LIVE.update(day=d, open='', pre='', scan='', sum='', err='')
    s = live.settings(CFG)
    if '08:55' <= hm < '09:00' and not LIVE['pre']:                  # 장전 스냅샷 (기록만)
        LIVE['pre'] = hm
        live.scan(CFG, n, premarket=True)
        return
    if hm < '09:00' or hm > '15:20':
        return
    if LIVE['open'] == '':
        if (n.second < 20 and hm == '09:00') or time.time() - LIVE['try'] < 20:   # 09:00 첫 봉이 생길 때까지 · 실패 시 20초 뒤
            return
        LIVE['try'] = time.time()
        try:
            LIVE['open'] = live.market_open(CFG, d, n)
        except Exception as e:
            LIVE['err'] = f'개장 확인 실패: {e}'[:200]
        if LIVE['open'] == '0':
            log('오늘은 휴장 — 가상 단타 쉼')
        if LIVE['open'] != '1':
            return
    if LIVE['open'] != '1':
        return
    if any(mdl.in_window(m, hm, s) for m in mdl.ORDER if m != 'Z') and LIVE['scan'] != hm:  # 매분 탐지 (모델 9개 중 하나라도 시간 안)
        LIVE['scan'] = hm
        try:
            _say(live.scan(CFG, n))
            LIVE['err'] = ''
        except Exception as e:
            LIVE['err'] = f'탐지 실패: {e}'[:200]
            log(LIVE['err'])
    if time.time() - LIVE['watch'] >= 10:                               # 10초마다 보유 감시
        LIVE['watch'] = time.time()
        if live.any_open():
            try:
                _say(live.watch(CFG, n))
            except Exception as e:
                LIVE['err'] = f'감시 실패: {e}'[:200]
                log(LIVE['err'])
    if hm >= '15:20' and not LIVE['sum']:                               # 하루 요약
        LIVE['sum'] = hm
        telegram(live.day_text(d))


def live_loop():
    time.sleep(5)
    while True:
        try:
            live_tick()
        except Exception as e:
            LIVE['err'] = str(e)[:200]
            traceback.print_exc()
        time.sleep(1)


# ════════════════════════════════════════════
#  API
# ════════════════════════════════════════════
@app.get('/', response_class=HTMLResponse)
async def index():
    return open(os.path.join(BASE_DIR, 'danta.html'), encoding='utf-8').read()


@app.get('/api/status')
async def api_status():
    st = await asyncio.to_thread(db.status)
    runs = [dict(r) for r in db.conn().execute("SELECT * FROM runs ORDER BY id DESC LIMIT 15")]
    return {'app': APP_NAME, 'version': APP_VERSION, 'port': PORT, 'data_dir': db.DATA_DIR, 'job': JOB, 'status': st, 'runs': runs,
            'scout_db': bool(db.scout_db_path()), 'probe': json.loads(db.meta_get('probe', '') or 'null'),
            'cfg': {'app_key': mask(CFG.get('app_key')), 'app_secret': mask(CFG.get('app_secret'), 0) if CFG.get('app_secret') else '',
                    'telegram_token': mask(CFG.get('telegram_token')), 'telegram_chat': mask(CFG.get('telegram_chat')),
                    'top_n': CFG.get('top_n'), 'surge_n': CFG.get('surge_n'), 'backfill_days': CFG.get('backfill_days'), 'rps': CFG.get('rps'),
                    'collect_time': CFG.get('collect_time')},
            'now': datetime.now().strftime('%Y-%m-%d %H:%M'), 'backfill_open': _backfill_window()}


@app.post('/api/config')
async def api_config(req: Request):
    b = await req.json()
    for k in SECRET_KEYS:
        if b.get(k):
            CFG[k] = str(b[k]).strip()
    if 'live_on' in b:
        CFG['live_on'] = bool(b['live_on'])
    for k, lo, hi in (('top_n', 20, 600), ('surge_n', 0, 600), ('backfill_days', 5, 260), ('rps', 1, 18)):
        if k in b and b[k] not in (None, ''):
            try:
                v = int(float(b[k]))
            except (TypeError, ValueError):
                return JSONResponse({'ok': False, 'error': f'{k} 값이 숫자가 아닙니다'}, 400)
            if not lo <= v <= hi:
                return JSONResponse({'ok': False, 'error': f'{k}는 {lo}~{hi}'}, 400)
            CFG[k] = v
    if b.get('collect_time'):
        t = str(b['collect_time']).strip()
        if not ('15:35' <= t <= '23:59' and len(t) == 5):
            return JSONResponse({'ok': False, 'error': '수집 시각은 15:35 ~ 23:59 (HH:MM)'}, 400)
        CFG['collect_time'] = t
    kis.RPS[0] = float(CFG.get('rps') or 8)
    save_cfg(CFG)
    return {'ok': True}


@app.get('/api/live')
async def api_live(model: str = '', pmodel: str = ''):
    d = datetime.now().strftime('%Y%m%d')
    if live.STATE['cands'] and ((live.STATE['last_scan'] or '')[:10].replace('-', '') == d or live.STATE['preview']):
        ts, cands = live.STATE['last_scan'], live.STATE['cands']
    else:
        ts, cands = live.last_cands(d)
    s = live.settings(CFG)
    log_rows = [dict(r) for r in db.conn().execute("SELECT * FROM settings_log ORDER BY ts DESC LIMIT 20")]
    return {'on': bool(CFG.get('live_on', True)), 'key': bool(CFG.get('app_key') and CFG.get('app_secret')),
            'now': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'live': {k: LIVE[k] for k in ('open', 'pre', 'scan', 'sum', 'err')},
            'last_scan': ts, 'preview': bool(live.STATE['preview']), 'cands': cands, 'err': live.STATE['err'],
            'last_watch': live.STATE['last_watch'], 'settings': s, 'default': live.DEFAULT, 'label': live.LABEL,
            'summary': await asyncio.to_thread(live.summary, 60, model or None), 'settings_log': log_rows,
            'profiles': await asyncio.to_thread(live.profiles, pmodel or None), 'pkeys': list(ex.PKEYS),
            'models': await asyncio.to_thread(live.models_report, s, lab.result_models()), 'order': mdl.ORDER,
            'criteria': mdl.CRITERIA_TEXT}


EXIT_KEYS = ex.PKEYS


def _apply_exit(rule, src):
    new = {k: rule[k] for k in EXIT_KEYS}
    s = live.settings(CFG)
    if new['hold_days'] == 0 and new['exit_by'] < s['scan_end']:
        new['exit_by'] = s['scan_end']
    err = live.set_settings(CFG, new)
    if not err:
        save_cfg(CFG)
        log(f'매도 규칙 적용 ({src}): {new}')
    return err


@app.post('/api/live/profile')
async def api_live_profile(req: Request):
    """비교 프로필 A~E의 매도 규칙을 '현재 설정'으로 (사람이 버튼으로)"""
    k = (await req.json()).get('key')
    if k not in ex.PROFILES:
        return JSONResponse({'ok': False, 'error': '알 수 없는 프로필'}, 400)
    err = _apply_exit(ex.PROFILES[k]['p'], '프로필 ' + k)
    return JSONResponse({'ok': False, 'error': err}, 400) if err else {'ok': True, 'settings': live.settings(CFG)}


@app.get('/api/lab')
async def api_lab():
    return {'job': lab.JOB, 'result': lab.result(), 'models': lab.result_models(), 'bars_days': db.status()['days']}


@app.post('/api/lab/run')
async def api_lab_run(req: Request):
    b = await req.json()
    src = 'vtrades' if b.get('src') == 'vtrades' else 'bars'
    try:
        cap = max(100, min(20000, int(b.get('cap') or 3000)))
    except (TypeError, ValueError):
        cap = 3000
    frm, to = (b.get('frm') or '').replace('-', ''), (b.get('to') or '').replace('-', '')
    if b.get('src') == 'models':
        ok = lab.start_models(live.settings(CFG), frm, to)
    else:
        ok = lab.start(live.settings(CFG), frm, to, src, cap)
    return {'ok': True} if ok else JSONResponse({'ok': False, 'error': '이미 실행 중'}, 409)


@app.post('/api/lab/apply')
async def api_lab_apply(req: Request):
    k = (await req.json()).get('key')
    r = lab.result() or {}
    row = next((x for x in r.get('rows', []) if x['key'] == k), None)
    if not row:
        return JSONResponse({'ok': False, 'error': '연구 결과에 없는 규칙'}, 400)
    err = _apply_exit(row['rule'], '연구실 ' + row['name'])
    return JSONResponse({'ok': False, 'error': err}, 400) if err else {'ok': True, 'settings': live.settings(CFG)}


@app.post('/api/live/settings')
async def api_live_settings(req: Request):
    b = await req.json()
    if b.get('reset'):
        err = live.set_settings(CFG, dict(live.DEFAULT))
    else:
        err = live.set_settings(CFG, b.get('values') or {})
    if err:
        return JSONResponse({'ok': False, 'error': err}, 400)
    save_cfg(CFG)
    return {'ok': True, 'settings': live.settings(CFG)}


@app.post('/api/live/toggle')
async def api_live_toggle(req: Request):
    b = await req.json()
    CFG['live_on'] = bool(b.get('on'))
    save_cfg(CFG)
    return {'ok': True, 'on': CFG['live_on']}


@app.post('/api/live/preview')
async def api_live_preview():
    """지금 순위로 후보 보기 — 매수도 기록도 안 함 (장 밖이면 마지막 거래일 자료)"""
    if not (CFG.get('app_key') and CFG.get('app_secret')):
        return JSONResponse({'ok': False, 'error': 'KIS 앱키를 먼저 설정하세요'}, 400)
    try:
        await asyncio.to_thread(live.scan, CFG, None, False, True)
    except Exception as e:
        return JSONResponse({'ok': False, 'error': str(e)[:200]}, 500)
    return {'ok': True, 'n': len(live.STATE['cands'])}


@app.get('/api/verify/info')
async def api_verify_info():
    r = db.conn().execute("SELECT MIN(date), MAX(date), COUNT(*) FROM vtrades").fetchone()
    s = db.conn().execute("SELECT MIN(date) FROM snaps WHERE hm>='09:00'").fetchone()
    first = min([x for x in (r[0], s[0]) if x] or [datetime.now().strftime('%Y%m%d')])
    return {'first': first, 'last': datetime.now().strftime('%Y%m%d'), 'trades': r[2]}


@app.get('/api/verify/export')
async def api_verify_export(frm: str = '', to: str = ''):
    import danta_verify as dv
    info = await api_verify_info()
    frm = (frm or info['first']).replace('-', '')
    to = (to or info['last']).replace('-', '')
    if frm > to:
        return JSONResponse({'ok': False, 'error': '시작일이 끝일보다 늦습니다'}, 400)
    data, fn, _ = await asyncio.to_thread(dv.build, CFG, frm, to, APP_VERSION, [CFG.get(k) for k in SECRET_KEYS])
    log(f'검증 데이터 zip {frm}~{to} ({len(data) // 1024}KB)')
    return Response(content=data, media_type='application/zip', headers={'Content-Disposition': f'attachment; filename="{fn}"'})


@app.post('/api/probe')
async def api_probe():
    today = datetime.now().strftime('%Y%m%d')
    past = [d for d in trading_dates(8) if d < today]
    r = await asyncio.to_thread(kis.probe, CFG, '005930', past[-3] if len(past) >= 3 else None)
    r['ts'] = datetime.now().isoformat(timespec='seconds')
    db.meta_set('probe', json.dumps(r, ensure_ascii=False, default=str))
    return r


@app.post('/api/job/{kind}')
async def api_job(kind: str):
    if JOB['running']:
        return JSONResponse({'ok': False, 'error': f"이미 실행 중: {JOB['kind']}"}, 409)
    if not (CFG.get('app_key') and CFG.get('app_secret')):
        return JSONResponse({'ok': False, 'error': 'KIS 앱키를 먼저 설정하세요'}, 400)
    if kind == 'today':
        threading.Thread(target=job_today, daemon=True).start()
    elif kind == 'backfill':
        threading.Thread(target=job_backfill, kwargs={'force': True}, daemon=True).start()
    else:
        return JSONResponse({'ok': False, 'error': '알 수 없는 작업'}, 400)
    return {'ok': True}


@app.post('/api/stop')
async def api_stop():
    STOP['flag'] = True
    return {'ok': True}


@app.get('/api/bars')
async def api_bars(ticker: str, date: str):
    rows = [dict(r) for r in db.conn().execute("SELECT hm, open, high, low, close, vol, amt FROM bars WHERE ticker=? AND date=? ORDER BY hm",
                                                (ticker, date))]
    u = db.conn().execute("SELECT name, rank FROM universe WHERE ticker=? AND date=?", (ticker, date)).fetchone()
    return {'ticker': ticker, 'date': date, 'name': u[0] if u else '', 'rank': u[1] if u else None, 'bars': rows}


@app.get('/api/universe')
async def api_universe(date: str):
    rows = [dict(r) for r in db.conn().execute(
        "SELECT u.rank, u.ticker, u.name, u.market, u.avg_value, u.src, d.n FROM universe u "
        "LEFT JOIN done d ON d.ticker=u.ticker AND d.date=u.date WHERE u.date=? ORDER BY u.rank", (date,))]
    return {'date': date, 'rows': rows}


README_ZIP = """TK Danta 데이터 ({frm} ~ {to}) · 만든 시각 {now} · 버전 {ver}
Claude에게 그대로 올리면 단타 전략 · 가상 단타 기록을 검증합니다. 앱키 · 텔레그램 등 비밀 값은 들어 있지 않습니다.

bars/날짜.csv   1분봉: ticker,hm(0901=09:01봉),open,high,low,close,vol,amt(원)   (가상 단타 기록만 받을 때는 없음)
universe.csv    날짜별 수집 대상 · src: scout=직전 20거래일 거래대금 상위(생존 편향 없음) · surge=그날 장중 +3% 이상(급등주) · live=가상 단타 탐지 후보
done.csv        (종목, 날짜)별 받은 봉 수
runs.csv        수집 작업 기록
live/vtrades.csv        가상 단타 거래 (매수 · 매도 시각/가격(호가 + 슬리피지) · 수익률(비용 0.25% 차감) · 사유 · 매수 당시 설정)
live/snaps/날짜.csv     매분 탐지 스냅샷 전부 (순위 출처 · 현재가 · 등락률 · 거래량 · 거래대금 · 시가 · 고가 · 필터 통과 · 매수 · 사유)
live/settings_log.csv   가상 단타 설정 변경 이력
live/vexits.csv         같은 매수에 매도 프로필 A~E를 동시에 적용한 결과 (trade_id · prof · 매도 시각/가격 · 수익률 · 사유)
live/profiles.json      프로필 정의 · 프로필별 누적 비교 (전체 · 모델별)
live/models.json        모델 9개 + 대조군 검증 현황 · 판정 · 기준 (D0.5 · D0.6 M9)
live/lab_result.json    분봉 연구실 마지막 결과 (있을 때)
live/summary.json       누적 요약 · 현재 설정
"""


def _csv(z, name, cur):
    s = io.StringIO()
    w = csv.writer(s)
    w.writerow([x[0] for x in cur.description])
    w.writerows(cur)
    z.writestr(name, s.getvalue().encode('utf-8-sig'))


def build_export(frm, to, bars=True):
    c = db.conn()
    buf = io.BytesIO()
    n = 0
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        dates = [r[0] for r in c.execute("SELECT DISTINCT date FROM done WHERE date BETWEEN ? AND ? AND n>0 ORDER BY date", (frm, to))] if bars else []
        for d in dates:
            s = io.StringIO()
            w = csv.writer(s)
            w.writerow(['ticker', 'hm', 'open', 'high', 'low', 'close', 'vol', 'amt'])
            for r in c.execute("SELECT ticker, hm, open, high, low, close, vol, amt FROM bars WHERE date=? ORDER BY ticker, hm", (d,)):
                w.writerow(list(r))
                n += 1
            z.writestr(f'bars/{d}.csv', s.getvalue())
        for name, sql in (('universe.csv', "SELECT * FROM universe WHERE date BETWEEN ? AND ? ORDER BY date, rank"),
                          ('done.csv', "SELECT * FROM done WHERE date BETWEEN ? AND ? ORDER BY date, ticker")):
            s = io.StringIO()
            w = csv.writer(s)
            cur = c.execute(sql, (frm, to))
            w.writerow([x[0] for x in cur.description])
            w.writerows(cur)
            z.writestr(name, s.getvalue().encode('utf-8-sig'))
        s = io.StringIO()
        w = csv.writer(s)
        cur = c.execute("SELECT * FROM runs ORDER BY id")
        w.writerow([x[0] for x in cur.description])
        w.writerows(cur)
        z.writestr('runs.csv', s.getvalue().encode('utf-8-sig'))
        _csv(z, 'live/vtrades.csv', c.execute("SELECT * FROM vtrades WHERE date BETWEEN ? AND ? ORDER BY id", (frm, to)))
        for d in [r[0] for r in c.execute("SELECT DISTINCT date FROM snaps WHERE date BETWEEN ? AND ? ORDER BY date", (frm, to))]:
            _csv(z, f'live/snaps/{d}.csv', c.execute("SELECT * FROM snaps WHERE date=? ORDER BY ts, passed DESC, chg DESC", (d,)))
        _csv(z, 'live/settings_log.csv', c.execute("SELECT * FROM settings_log ORDER BY ts"))
        _csv(z, 'live/vexits.csv', c.execute("SELECT x.* FROM vexits x JOIN vtrades t ON t.id=x.trade_id WHERE t.date BETWEEN ? AND ? "
                                             "ORDER BY x.trade_id, x.prof", (frm, to)))
        z.writestr('live/profiles.json', json.dumps({'defs': ex.PROFILES, 'result': live.profiles(),
                                                     'by_model': {m: live.profiles(m) for m in mdl.ORDER}}, ensure_ascii=False, indent=1))
        z.writestr('live/models.json', json.dumps({'criteria': mdl.CRITERIA, 'criteria_text': mdl.CRITERIA_TEXT,
                                                   'report': live.models_report(live.settings(CFG), lab.result_models())}, ensure_ascii=False, indent=1, default=str))
        if db.meta_get('lab_result'):
            z.writestr('live/lab_result.json', db.meta_get('lab_result'))
        if db.meta_get('lab_models'):
            z.writestr('live/lab_models.json', db.meta_get('lab_models'))
        sm = live.summary()
        sm.pop('rows', None)
        z.writestr('live/summary.json', json.dumps({'summary': sm, 'settings': live.settings(CFG), 'default': live.DEFAULT,
                                                    'live_on': bool(CFG.get('live_on', True))}, ensure_ascii=False, indent=1))
        z.writestr('README.txt', README_ZIP.format(frm=frm, to=to, now=datetime.now().strftime('%Y-%m-%d %H:%M'), ver=APP_VERSION))
    return buf.getvalue(), len(dates), n


@app.get('/api/export')
async def api_export(frm: str = '', to: str = '', kind: str = 'bars'):
    st = db.status()
    if kind == 'live':
        r = db.conn().execute("SELECT MIN(d), MAX(d) FROM (SELECT date d FROM vtrades UNION SELECT date FROM snaps)").fetchone()
        frm, to = (frm or r[0] or '').replace('-', ''), (to or r[1] or '').replace('-', '')
    else:
        frm = (frm or st['first'] or '').replace('-', '')
        to = (to or st['last'] or '').replace('-', '')
    if not frm or not to or frm > to:
        return JSONResponse({'ok': False, 'error': '받을 자료가 없거나 날짜가 잘못됐습니다'}, 400)
    data, nd, nb = await asyncio.to_thread(build_export, frm, to, kind != 'live')
    name = f'danta_live_{frm}_{to}.zip' if kind == 'live' else f'danta_bars_{frm}_{to}.zip'
    return Response(content=data, media_type='application/zip', headers={'Content-Disposition': f'attachment; filename="{name}"'})


if __name__ == '__main__':
    import uvicorn
    db.init()
    threading.Thread(target=scheduler, daemon=True).start()
    threading.Thread(target=live_loop, daemon=True).start()
    print(f"""
╔══════════════════════════════════════════╗
║   🎯 台炅 자동단타매매 (TK Danta) {APP_VERSION}     ║
║   http://localhost:{PORT}                  ║
║   분봉 수집 + 가상 단타 · 주문 기능 없음    ║
║   (가상추천매매 Scout는 8082 · 별개)       ║
╚══════════════════════════════════════════╝
""", flush=True)
    uvicorn.run(app, host='0.0.0.0', port=PORT, log_level='warning')
