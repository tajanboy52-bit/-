"""
scout_server.py — 台炅 TK Stock Scout 추천 엔진 서버 (포트 8082)
============================================
자동매매 서버(8080) / 종목분석기(8081)와 완전 독립.
별도 KIS 토큰 · 별도 DB · 주문 기능 없음 (분석 전용).

실행:  python scout_server.py
설정:  scout_config.json  또는 환경변수 KIS_APP_KEY / KIS_APP_SECRET
"""
import os, sys, json, time, asyncio, threading, traceback, functools, platform
from datetime import datetime, timedelta

# 자동 실행(작업 스케줄러 · 로그 파일로 출력)일 때 윈도우 기본 인코딩(cp949)에 없는 글자(╔ — ⚠ 등)를
# 찍다가 서버가 시작하자마자 멈추던 문제 방지 → 출력은 항상 UTF-8, 못 쓰는 글자는 ?로 대체
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, Response
import uvicorn

import scout_db as db
import scout_autotrade as at
import scout_rt as rt
import scout_verify as verify
import scout_vacct as vacct
import scout_engines as eng
import scout_strategies as strat
import scout_ext as ext

PORT = int(os.environ.get('SCOUT_PORT', '8082'))
APP_NAME = '台炅 TK Stock Scout'
APP_VERSION = 'v6.1.3'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))       # 어디서 실행해도 같은 폴더 사용
CONFIG_FILE = os.environ.get('SCOUT_CONFIG') or db.data_file('scout_config.json')   # 프로그램 폴더 밖
HTML_FILE = os.path.join(BASE_DIR, 'scout.html')

app = FastAPI(title="TK Stock Scout")

# ════════════════════════════════════════════
#  설정
# ════════════════════════════════════════════
DEFAULT_CFG = {
    "app_key": "", "app_secret": "",
    "min_trade_value": 3_000_000_000,   # 후보풀 최소 20일 평균 거래대금
    "min_price": 1000, "max_price": 500000,
    "top_n": 20,                         # 추천 노출 개수
    "require_stage2": True,              # Stage 2만 통과 (중장기)
    "auto_track": 5,                     # 스캔마다 상위 N개 자동 추적
    "swing_top": 3,                      # 스윙 후보 최대 개수 (v3: 소수 집중)
    "krx_id": "", "krx_pw": "",          # KRX 정보데이터시스템 계정 (연기금 수급 자동 수집 · 이 PC에만 저장)
    "legacy_features": False,            # 예전 기능(스윙·단타 탭, 수동 기록, 가상 추적, 성과) — 기본 숨김·정지
    # ── 실전 자동매매 (기본 OFF · LIVE는 가상매매 검증 통과 후에만)
    "at_mode": "OFF", "at_track": "final", "at_slots": 30, "at_max_order_krw": 1000000,
    "at_max_daily_buys": 3, "at_max_positions": 15, "at_daily_loss_stop": 3.0, "at_dry_cash": 10000000,
    "at_capital_live": 0,                        # 실전 운용 금액 (0 = 계좌 전체)
    "at_stop_pct": 0, "at_take_pct": 0,          # 손절·익절 % (0 = 사용 안 함 — 검증된 규칙)
    "at_max_hold": 10,                           # 최대 보유 거래일 (검증된 규칙 10)
    "rt_settings": {},                          # 실전형 가상매매 모델별 장중 매도 설정 (v5.8 · 언제든 변경 · 이력 기록)
    "vt_cash": 10000000, "vt_slots": 20,         # 가상 계좌 시작 금액 · 종목당 비중 1/N (계좌 곡선 계산용)
    # 모델별 1회 최대 주문금액(원) — 가상 계좌 곡선 · 실전 자동매매 주문에 적용 (추천 근거: order_reco)
    "model_order_max": {"final": 500000, "strategy": 500000, "rsi": 350000, "fdip": 250000,
                        "lvflow": 200000, "lvhigh": 200000, "candle": 250000, "jongga": 2050000, "v62": 200000},
    "dart_key": "",                      # DART 오픈API 키 (희석성 공시 필터)
    "enrich_top": 40,                    # 2단계 정밀분석 대상 수
    "max_per_sector": 2,                 # 추천 내 동일 업종 최대 개수
    "account_size": 0,                   # 계좌 금액(원) — 매수 수량 계산용
    "risk_pct": 1.5,                     # 1회 매매 허용 손실 (계좌 대비 %)
    "max_pos_pct": 25,                   # 종목당 최대 비중 (계좌 대비 %)
    "virtual_alerts": False,             # 가상 추적도 텔레그램 알림 (기본 끔)
    "account_no": "",                    # KIS 계좌번호 앞 8자리 (잔고 '조회'만 — 주문 없음)
    "account_cd": "01",                  # 계좌상품코드
    "auto_register": True,               # 앱에서 새로 산 종목 자동 등록
    "history_days": 750,                 # 후보풀 과거 이력 (백테스트용, 약 3년)
    "telegram_token": "", "telegram_chat": "",
}


ORDER_MIN, ORDER_MAX = 50_000, 100_000_000        # 1회 최대 주문금액 입력 범위 (원)


def _norm_order_max(v):
    """모델별 1회 최대 주문금액 → 기본값과 합친 새 dict (모르는 모델 · 범위 밖 값은 기본값 유지)"""
    out = dict(DEFAULT_CFG['model_order_max'])
    if isinstance(v, dict):
        for g, x in v.items():
            if g not in out:
                continue
            try:
                x = int(float(str(x).replace(',', '')))
            except (TypeError, ValueError):
                continue
            if ORDER_MIN <= x <= ORDER_MAX:
                out[g] = x
    return out


def load_cfg():
    cfg = dict(DEFAULT_CFG)
    for path in (CONFIG_FILE, CONFIG_FILE + '.bak'):
        if os.path.exists(path):
            try:
                cfg.update(json.load(open(path, encoding='utf-8-sig')))
                break
            except Exception:
                print(f"[설정] {path} 읽기 실패 — 백업에서 복구 시도", flush=True)
    cfg['model_order_max'] = _norm_order_max(cfg.get('model_order_max'))
    cfg['app_key'] = os.environ.get('KIS_APP_KEY') or cfg['app_key']
    cfg['app_secret'] = os.environ.get('KIS_APP_SECRET') or cfg['app_secret']
    # v5.6.1: 모의투자(PAPER) 제거 — 저장돼 있던 모의투자 앱키·시크릿·계좌는 지우고, PAPER 모드였다면 끔
    gone = [k for k in PAPER_KEYS if k in cfg]
    for k in gone:
        cfg.pop(k, None)
    if cfg.get('at_mode') not in ('OFF', 'DRY', 'LIVE'):
        cfg['at_mode'] = 'OFF'
        gone.append('at_mode')
    cfg['_purged'] = bool(gone)
    return cfg


def save_cfg(cfg):
    """임시 파일에 쓴 뒤 교체 → 저장 도중 꺼져도 깨지지 않음.
       저장 성공 후 같은 내용을 .bak에도 둬서, 본 파일이 손상돼도 최신 설정으로 복구."""
    import shutil
    tmp = CONFIG_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, CONFIG_FILE)
    try:
        shutil.copyfile(CONFIG_FILE, CONFIG_FILE + '.bak')
    except Exception:
        pass


PAPER_KEYS = ('paper_app_key', 'paper_app_secret', 'paper_account_no', 'paper_account_cd', 'at_capital_paper')
CFG = load_cfg()
if CFG.pop('_purged', False):
    save_cfg(CFG)
    print("[설정] 모의투자(PAPER) 설정을 지웠습니다 — 검증은 가상매매로만 합니다", flush=True)
db.init_db()   # 스키마·마이그레이션 (기존 DB에 새 컬럼 자동 추가)
db._vt_init()  # 가상매매 표도 시작할 때 준비 (새로 설치한 PC에서 첫 화면이 오류 나지 않게)

# ════════════════════════════════════════════
#  작업 상태 (동기화·스캔 진행률)
# ════════════════════════════════════════════
JOB = {'running': False, 'kind': '', 'msg': '', 'done': 0, 'total': 0,
       'eta': 0, 'started': '', 'error': ''}
_stop = {'flag': False}
_clients = set()
_last_scan = {'short': None, 'swing': None, 'long': None}
_market = {'coef': 1.0, 'regime': '미판정', 'detail': ''}


async def _push(event, data):
    dead = []
    for ws in list(_clients):
        try:
            await ws.send_text(json.dumps({'event': event, 'data': data},
                                          ensure_ascii=False))
        except Exception:
            dead.append(ws)
    for d in dead:
        _clients.discard(d)


_loop = None


def push(event, data):
    if _loop:
        asyncio.run_coroutine_threadsafe(_push(event, data), _loop)


_last_print = {'t': 0.0, 'msg': ''}


def set_job(**kw):
    JOB.update(kw)
    push('job', dict(JOB))
    # 검은 창에도 진행 상황 표시 (5초에 한 번, 또는 단계가 바뀔 때)
    msg = JOB.get('msg', '')
    stage = msg.split(' ')[0] if msg else ''
    now = time.time()
    if msg and (now - _last_print['t'] >= 5 or stage != _last_print['msg'] or not JOB['running']):
        pct = f" {JOB['done']*100//JOB['total']}%" if JOB.get('total') else ''
        eta = f" · 남은 약 {JOB['eta']//60 + 1}분" if JOB.get('eta') else ''
        err = f" · 오류: {JOB['error']}" if JOB.get('error') else ''
        print(f"[{datetime.now():%H:%M:%S}] {msg}{pct}{eta}{err}", flush=True)
        _last_print.update(t=now, msg=stage)


# ════════════════════════════════════════════
#  1단계 · 데이터 구축
# ════════════════════════════════════════════

# ════════════════════════════════════════════
#  작업 실행 기록 — 시스템 정보 탭 (언제 · 얼마나 · 성공/오류). 동작은 그대로, 기록만 추가
#  DB가 아닌 파일(job_runs.json)에 저장 → 진행 중인 가상매매 기록(트랜잭션)에 영향 없음
# ════════════════════════════════════════════
JOB_LABELS = {'job_build': '데이터 동기화', 'job_scan': '후보 스캔', 'job_flows': 'KRX 수급 수집',
              'job_virtual': '가상매매 갱신', 'job_vt_report': '가상매매 주간 보고',
              'job_at_morning': '자동매매 아침 주문', 'job_at_close': '자동매매 장마감 정리',
              'job_at_fill': '리허설(DRY) 시가 체결 확인',
              'job_morning_orders': '주문표 (예전 기능)', 'job_track': '추적 점검 (예전 기능)'}
_RUNS_FILE = os.path.join(db.DATA_DIR, 'job_runs.json')
_runs_lock = threading.Lock()


def _load_runs():
    try:
        with open(_RUNS_FILE, encoding='utf-8') as f:
            d = json.load(f)
        return dict(d.get('last') or {}), list(d.get('hist') or [])
    except Exception:
        return {}, []


JOB_LAST, JOB_HIST = _load_runs()


def _save_runs():
    try:
        tmp = _RUNS_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump({'last': JOB_LAST, 'hist': JOB_HIST[:80]}, f, ensure_ascii=False)
        os.replace(tmp, _RUNS_FILE)
    except Exception:
        pass


def logged_job(fn):
    @functools.wraps(fn)
    def run(*a, **kw):
        name = fn.__name__
        t0 = datetime.now()
        ok, err = True, ''
        tracked = name in ('job_build', 'job_scan')          # 진행률 표시를 쓰는 작업: 실패·건너뜀을 JOB 상태로 판단
        started0 = JOB.get('started', '') if tracked else None
        try:
            return fn(*a, **kw)
        except Exception as e:
            ok, err = False, f'{type(e).__name__}: {e}'[:240]
            raise
        finally:
            try:
                skipped = False
                if tracked and ok:
                    if JOB.get('started', '') == started0:     # 다른 작업이 진행 중이라 이번 호출은 실행 안 됨
                        skipped = True
                    elif JOB.get('error'):
                        ok, err = False, str(JOB['error'])[:240]
                label = JOB_LABELS.get(name, name)
                if name == 'job_build':
                    label = '전체 갱신 (종목 목록 포함)' if (a[0] if a else kw.get('full', True)) else '증분 동기화'
                arg = ' '.join(str(x) for x in a if isinstance(x, str))
                rec_ = {'job': name, 'label': label, 'arg': arg, 'start': t0.strftime('%Y-%m-%d %H:%M:%S'),
                        'sec': round((datetime.now() - t0).total_seconds(), 1), 'ok': ok, 'err': err, 'skip': skipped}
                with _runs_lock:
                    prev = JOB_LAST.get(name) or {}
                    if not skipped:
                        JOB_LAST[name] = {**rec_, 'count': int(prev.get('count', 0)) + 1,
                                          'fails': int(prev.get('fails', 0)) + (0 if ok else 1)}
                    JOB_HIST.insert(0, rec_)
                    del JOB_HIST[80:]
                    _save_runs()
            except Exception:
                pass
    return run


@logged_job
def job_build(full=True):
    """유니버스 구축 → 일봉 수집 → 후보풀 확정 → 수급 수집"""
    if JOB['running']:
        return
    _stop['flag'] = False
    set_job(running=True, kind='build', msg='시작', done=0, total=0,
            started=datetime.now().isoformat(), error='')
    try:
        if not CFG['app_key'] or not CFG['app_secret']:
            raise RuntimeError('KIS 앱키가 설정되지 않았습니다')

        if full:
            set_job(msg='전종목 마스터 수집 중')
            r = db.build_universe(CFG['app_key'], CFG['app_secret'],
                                  progress=lambda m: set_job(msg=m))

        set_job(msg='일봉 수집 중 (최초 1회는 시간이 걸립니다)')

        def prog(p):
            if isinstance(p, dict):
                set_job(msg=f"일봉 {p['done']}/{p['total']} · 성공 {p['ok']}",
                        done=p['done'], total=p['total'], eta=p.get('eta', 0))
            else:
                set_job(msg=str(p))

        # 증분도 전종목 대상 — 후보풀 밖 종목이 거래대금 늘면 다음날 후보풀에 들어오도록
        tickers = None
        sres = db.sync_candles(CFG['app_key'], CFG['app_secret'], tickers,
                               days=250, progress=prog, stop_flag=lambda: _stop['flag'])
        adj = (sres or {}).get('adjusted') or []
        if adj:
            names = {r[0]: r[1] for r in db.conn().execute("SELECT ticker, name FROM stocks")}
            lines = [f"· {names.get(t, t)}({t}) 가격 ×{r:.3f}" for t, r in adj]
            print('[ADJ] 권리조정 감지·복구: ' + ', '.join(f'{t}×{r:.3f}' for t, r in adj), flush=True)
            telegram("🔧 권리조정(분할·병합·증자) 감지 — 과거 일봉 재수집 · 보유 중 가상/자동매매 가격 보정\n" + '\n'.join(lines[:15]))
        try:                                        # 지수 ETF 비교선 (KODEX 200) — 실패해도 동기화는 계속
            db.sync_bench(CFG['app_key'], CFG['app_secret'])
        except Exception as e:
            print(f'[BENCH] 지수 ETF 일봉 동기화 실패: {e}', flush=True)

        set_job(msg='후보풀 선정 중')
        n = db.rebuild_pool(CFG['min_trade_value'], 120,
                            CFG['min_price'], CFG['max_price'])
        set_job(msg=f'후보풀 {n}종목 확정 · 수급 수집 중')

        pool = [s['ticker'] for s in db.get_pool()]
        db.sync_investors(CFG['app_key'], CFG['app_secret'], pool,
                          progress=prog, stop_flag=lambda: _stop['flag'])

        set_job(msg='후보풀 과거 이력 확장 (백테스트용 · 최초 1회만 오래 걸림)')
        db.extend_history(CFG['app_key'], CFG['app_secret'], pool, int(CFG.get('history_days', 750)),
                          progress=lambda p: set_job(msg=f"과거 이력 {p['done']}/{p['total']}",
                                                     done=p['done'], total=p['total']),
                          stop_flag=lambda: _stop['flag'])

        set_job(msg='종목 프로필 수집 (업종·시총·지정경고)')
        ext.sync_profiles(CFG['app_key'], CFG['app_secret'], pool,
                          progress=lambda p: set_job(msg=f"프로필 {p['done']}/{p['total']}",
                                                     done=p['done'], total=p['total']),
                          stop_flag=lambda: _stop['flag'])
        if CFG.get('dart_key'):
            set_job(msg='DART 주요사항보고 수집')
            ext.sync_dart(CFG['dart_key'], 90)

        db.track_outcomes()
        _gate_and_evaluate()
        set_job(running=False, kind='', msg=f'완료 · 후보풀 {n}종목',
                done=0, total=0, eta=0)
    except Exception as e:
        traceback.print_exc()
        set_job(running=False, kind='', msg='실패', error=str(e))


# ════════════════════════════════════════════
#  2단계 · 스캔
# ════════════════════════════════════════════
def _load_pool_candles(limit_days=250):
    out = {}
    for s in db.get_pool():
        cd = db.load_candles(s['ticker'], limit_days)
        if len(cd) >= 120:
            out[s['ticker']] = cd
    return out


def _chart(cd, n=60):
    return [{'d': c['date'], 'o': c['open'], 'h': c['high'], 'l': c['low'],
             'c': c['close'], 'v': c['volume']} for c in cd[-n:]]


def _evaluate(tk, cd, info, ctx, horizon, news=None, profile=None, strength=None):
    """종목 1개 채점 → 결과 dict (탈락 시 None)"""
    warns = [w for w in (profile or {}).get('warns', info.get('warns_list', [])) if w]
    events = ext.load_events(tk)
    reasons = eng.risk_filter(cd, events=events, warns=warns, news=news)
    if reasons:
        return None
    inv = db.load_investors(tk, 20)
    prof = profile or info.get('profile')
    sc = eng.score_stock(cd, inv, (prof or {}).get('mktcap', 0), None, news,
                         ctx['coef'], prof)
    e = sc['engines']
    if horizon == 'long' and CFG['require_stage2'] and e['trend']['stage'] != 2:
        return None

    sts = strat.run_strategies(cd, ctx['rs'].get(tk), None, e['supply'], inv, prof, news)
    sts = strat.filter_for_tab(sts, horizon)
    if horizon == 'short' and not sts:
        return None                 # 단타 탭은 단기 셋업이 있어야만 추천
    px = cd[-1]['close']
    prev = cd[-2]['close'] if len(cd) > 1 else px
    fac = ctx.get('factors', {}).get(tk)
    rev = ctx.get('rev', {}).get(tk)

    # ── 스윙: 반전·수급 점수 (v3 · 전종목 검증). 전략은 확인 신호로만 표시
    if horizon == 'swing' and rev:
        best = strat.reversal_best(cd)
        strat_score = 0
    elif horizon == 'long' and fac:
        best = strat.factor_best(cd)
        strat_score = 0
    else:
        best = sts[0] if sts else None
        strat_score = best['score'] if best else 0
    if not best:
        a = eng.atr(cd, 14) or px * 0.03
        best = {'name': '전략 미매칭 (점수 상위)', 'key': 'none',
                'entry': round(px), 'stop': round(px - a * 2),
                'target1': round(px + a * 3), 'target2': round(px + a * 5), 'rr': 1.5}
        strat_score = 0

    # 업종 강도 가감
    sec = (prof or {}).get('sector') or info.get('sector', '')
    ss = ctx['sectors'].get(sec)
    sec_bonus = 0
    if ss:
        sec_bonus = 3 if ss['pct'] >= 80 else (-3 if ss['pct'] <= 20 else 0)

    # 단타: 체결강도 가감
    str_bonus = 0
    if horizon == 'short' and strength:
        str_bonus = 4 if strength >= 130 else (2 if strength >= 110 else (-4 if strength < 80 else 0))

    if horizon == 'swing' and rev:
        # 반전·수급 점수가 순위 기준 (업종·캔들·체결강도는 실데이터에서 예측력 없음)
        total = rev['score']
    elif horizon == 'long' and fac:
        total = fac['factor']
    else:
        total = (sc['base'] * 0.5 + strat_score * 0.5 * 0.85) * ctx['coef'] + sec_bonus + str_bonus
    plan = strat.build_plan(best, px)
    if best.get('key') == 'reversal':
        plan['t1_action'] = ('종가가 9일 EMA 이상이면 매도 검토 · 9일 EMA는 매일 내려와 가격과 만나므로 '
                             '실제 매도가는 보통 이보다 낮음 (매일 아침 갱신)')
        plan['t2_action'] = '20일선 — 추가 반등 시 참고'
        plan['stop_reason'] = strat.META['reversal']['stop_reason']
        plan['hold_desc'] = '최대 10거래일 · 9일 EMA 복귀 시 매도 · 매일 아침 목표가 갱신'
    return {
        'ticker': tk, 'name': info.get('name', tk), 'horizon': horizon,
        'price': px, 'chg_pct': round((px - prev) / prev * 100, 2),
        'score': round(total, 1), 'base': sc['base'], 'strat_score': strat_score,
        'rs': ctx['rs'].get(tk, 0), 'sector': sec,
        'sector_rank': ss['pct'] if ss else None, 'sector_ret': ss['ret'] if ss else None,
        'sec_bonus': sec_bonus, 'strength': strength, 'str_bonus': str_bonus,
        'warns': warns,
        'news': ({'label': news['label'], 'score': news['score'], 'headline': news['headline'],
                  'catalyst': news['catalyst'][:2], 'pos': news['pos'][:3], 'neg': news['neg'][:3],
                  'count': news['count']} if news else None),
        'strategy': best['name'], 'strategies': sts[:3], 'plan': plan, 'factor': fac, 'rev': rev,
        'entry': plan['entry'], 'stop': plan['stop'],
        'target1': plan['target1'], 'target2': plan['target2'], 'rr': plan['rr'],
        'sizing': _sizing(plan['entry'], plan['stop']),
        'engines': {k: e[k] for k in ('candle', 'trend', 'supply', 'pattern', 'fundamental')},
        'chart': _chart(cd),
    }


@logged_job
def job_scan(horizon='swing', scheduled=False):
    if JOB['running']:
        return
    _stop['flag'] = False
    set_job(running=True, kind='scan', msg='후보풀 로드 중', done=0, total=0,
            started=datetime.now().isoformat(), error='')
    t0 = time.time()
    try:
        pool_candles = _load_pool_candles()
        if not pool_candles:
            raise RuntimeError('후보풀이 비어 있습니다. 먼저 데이터를 구축하세요.')
        infos = {}
        for s_ in db.get_pool():
            s_['warns_list'] = [w for w in (s_.get('warns') or '').split(',') if w]
            s_['profile'] = ({'sector': s_.get('sector', ''), 'mktcap': s_.get('mktcap', 0),
                              'shares': s_.get('shares', 0), 'per': s_.get('per', 0),
                              'pbr': s_.get('pbr', 0), 'warns': s_['warns_list']}
                             if s_.get('sector') else None)
            infos[s_['ticker']] = s_

        # E6 시장환경 + 전일 미증시 보정
        mk = eng.engine_market(pool_candles)
        us = ext.fetch_us_market()
        adj, us_txt = ext.us_adjust(us)
        coef = round(max(0.4, min(1.25, mk['coef'] + adj)), 2)
        mk.update(coef_base=mk['coef'], coef=coef, us=us, us_adj=adj, us_text=us_txt,
                  detail=mk['detail'] + f' · {us_txt}')
        _market.update(mk)
        push('market', mk)

        ctx = {'coef': coef, 'rs': strat.rs_rankings(pool_candles),
               'factors': eng.factor_table(pool_candles),
               'rev': eng.reversal_flow_table(
                   pool_candles, {t: db.load_investors(t, 20) for t in pool_candles},
                   {'frgn': db.flow_sums(db.recent_trading_dates(20), '외국인'),
                    'pens': db.flow_sums(db.recent_trading_dates(20), '연기금')}),
               'sectors': eng.sector_strength(
                   pool_candles, {t: i.get('sector', '') for t, i in infos.items()})}

        # ── 1단계: 캐시만으로 전체 채점
        set_job(msg=f"시장 {mk['regime']} ×{coef} · 1차 채점", total=len(pool_candles))
        prelim = []
        for i, (tk, cd) in enumerate(pool_candles.items()):
            if _stop['flag']:
                break
            if i % 50 == 0:
                set_job(msg=f'1차 채점 {i}/{len(pool_candles)}', done=i)
            r = _evaluate(tk, cd, infos.get(tk, {}), ctx, horizon)
            if r:
                prelim.append(r)
        prelim.sort(key=lambda x: -x['score'])

        # ── 2단계: 상위 후보 실시간 보강 (뉴스·프로필·당일봉·체결강도)
        K = int(CFG.get('enrich_top', 40))
        cand = [r['ticker'] for r in prelim[:K]]
        has_kis = bool(CFG['app_key'] and CFG['app_secret'])
        live = ext.is_market_hours()
        token = None
        if has_kis:
            try:
                token = db.get_token(CFG['app_key'], CFG['app_secret'])
            except Exception:
                token = None
        if horizon == 'short' and live and token:
            ranks = ext.live_ranks(CFG['app_key'], CFG['app_secret'], token)
            extra = [t for t in ranks if t in pool_candles and t not in cand]
            cand += extra[:20]

        results = []
        today = datetime.now().strftime('%Y%m%d')
        for j, tk in enumerate(cand):
            if _stop['flag']:
                break
            set_job(msg=f'2차 정밀분석 {j+1}/{len(cand)} · 뉴스·수급·체결', done=j + 1,
                    total=len(cand))
            cd = pool_candles[tk]
            news = None
            try:
                news = ext.news_sentiment(ext.fetch_news(tk))
            except Exception:
                pass
            profile, strength = None, None
            if token:
                try:
                    profile = ext.fetch_profile(tk, CFG['app_key'], CFG['app_secret'], token)
                    lv = profile['live']
                    # 장중이면 당일 봉을 붙여서 재채점
                    if live and lv['close'] > 0 and cd[-1]['date'] != today:
                        cd = cd + [{'date': today, 'open': lv['open'] or lv['close'],
                                    'high': lv['high'] or lv['close'],
                                    'low': lv['low'] or lv['close'],
                                    'close': lv['close'], 'volume': lv['volume']}]
                except Exception:
                    profile = None
                if horizon == 'short' and live:
                    strength = ext.fetch_strength(tk, CFG['app_key'], CFG['app_secret'], token)
            r = _evaluate(tk, cd, infos.get(tk, {}), ctx, horizon, news, profile, strength)
            if r:
                results.append(r)

        # ── 업종 분산: 같은 업종 최대 N개
        results.sort(key=lambda x: -x['score'])
        cap = int(CFG.get('max_per_sector', 2))
        top, per_sec = [], {}
        for r in results:
            sec = r.get('sector') or '-'
            if sec != '-' and per_sec.get(sec, 0) >= cap:
                continue
            per_sec[sec] = per_sec.get(sec, 0) + 1
            top.append(r)
            if len(top) >= (int(CFG.get('swing_top', 3)) if horizon == 'swing' else CFG['top_n']):
                break

        ids = db.save_scan(horizon, top)
        # ── 실전 가상매매: 하루 1회, 연기금 반영 후(18:15 이후) 또는 다음날 장전(보충)만
        if horizon == 'swing' and scheduled:
            hm = datetime.now().strftime('%H:%M')
            if hm >= '18:15' or hm < '09:00':
                try:
                    vt_create_batch(results, pool_candles, infos, ctx.get('rev'))
                except Exception as e:
                    print(f'[VT] 배치 생성 실패: {e}', flush=True)
        tracked = 0
        for sid, r in zip(ids, top):
            if horizon == 'swing':
                break                                   # 스윙은 위의 실전 가상매매로 기록
            if tracked >= int(CFG.get('auto_track', 5)):
                break
            if r['plan']['buy_type'] == '관망' or r['strategy'].startswith('전략 미매칭'):
                continue
            if db.add_tracking(r, sid):
                tracked += 1
        active = db.tracking_tickers()
        for r in top:
            r['tracked'] = r['ticker'] in active

        _last_scan[horizon] = {'ts': datetime.now().isoformat(), 'market': mk,
                               'results': top, 'scanned': len(pool_candles),
                               'enriched': len(cand)}
        push('scan', _last_scan[horizon])
        set_job(running=False, kind='', msg=f'스캔 완료 · {len(top)}종목 추천 '
                f'({time.time()-t0:.0f}초)', done=0, total=0)
    except Exception as e:
        traceback.print_exc()
        set_job(running=False, kind='', msg='스캔 실패', error=str(e))


# ════════════════════════════════════════════
#  3단계 · 추천 추적 (돌파·목표·손절 감시)
# ════════════════════════════════════════════
_track_lock = threading.Lock()


def telegram(msg):
    tok, chat = CFG.get('telegram_token'), CFG.get('telegram_chat')
    if not tok or not chat:
        return
    try:
        import urllib.request, urllib.parse
        data = urllib.parse.urlencode({'chat_id': chat, 'text': msg}).encode()
        urllib.request.urlopen(f"https://api.telegram.org/bot{tok}/sendMessage",
                               data=data, timeout=10)
    except Exception as e:
        print(f"[TG] 전송 실패: {e}")


def tg_trade(msg):
    """거래 단위 알림(매수 · 매도 · 주문표 · 보유 알림 · 자동매매 주문) — v6.1.2부터 기본 끔.
       텔레그램은 장 마감 리포트(18:20 가상매매 일일 보고) · 주간 요약 · 오류 경고만. 다시 켜려면 설정 파일 tg_trade_alerts: true"""
    if CFG.get('tg_trade_alerts'):
        telegram(msg)


def _quote(ticker):
    """현재가·당일 고저 — KIS 실패 시 마지막 일봉"""
    try:
        if CFG['app_key'] and CFG['app_secret']:
            tok = db.get_token(CFG['app_key'], CFG['app_secret'])
            r = db.kis_get("/uapi/domestic-stock/v1/quotations/inquire-price",
                           "FHKST01010100",
                           {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
                           CFG['app_key'], CFG['app_secret'], tok)
            o = r.get('output') or {}
            px = float(o.get('stck_prpr', 0) or 0)
            if px > 0:
                return (px, float(o.get('stck_hgpr', px) or px),
                        float(o.get('stck_lwpr', px) or px))
    except Exception:
        pass
    cd = db.load_candles(ticker, 1)
    if cd:
        return cd[-1]['close'], cd[-1]['high'], cd[-1]['low']
    return None


@logged_job
def job_track():
    """가상 추적(전략 검증) + 내 보유(실제) 점검. 텔레그램은 실제 보유만."""
    if not _track_lock.acquire(blocking=False):
        return
    try:
        quotes = {}

        def q(tk):
            if tk not in quotes:
                quotes[tk] = _quote(tk)
            return quotes[tk]

        # ① 가상 추적 — 전략 성적 기록용, 알림 없음(설정 시에만)
        for t in db.open_tracking():
            qt = q(t['ticker'])
            if not qt:
                continue
            upd, alerts = db.evaluate_tracking(t, *qt)
            db.update_tracking(t['id'], **upd)
            if CFG.get('virtual_alerts'):
                for _, m in alerts:
                    tg_trade('[가상] ' + m)

        # ② 증권앱 체결 반영 (KIS 잔고 조회)
        fired = []
        try:
            fired += sync_balance()
        except Exception as e:
            print(f'[BALANCE] {e}')

        # ③ 과매도 반등 종목: 목표가를 오늘의 9일 EMA·20일선으로 갱신 (백테스트 청산 규칙)
        for p in db.list_positions(False):
            if not str(p.get('strategy', '')).startswith('과매도 반등'):
                continue
            cd = db.load_candles(p['ticker'], 60)
            if len(cd) < 20:
                continue
            b = strat.reversal_best(cd)
            t1 = strat.round_tick(max(b['target1'], p['buy_price'] * 1.01), 'down')
            t2 = strat.round_tick(max(b['target2'], t1 * 1.02), 'down')
            if int(t1) != int(p['target1']) or int(t2) != int(p['target2']):
                db.update_position(p['id'], target1=t1, target2=t2)

        # ④ 내 보유 — 실제 평단 기준 알림
        for p in db.list_positions(False):
            qt = q(p['ticker'])
            if not qt:
                continue
            upd, alerts = db.evaluate_position(p, *qt)
            db.update_position(p['id'], **upd)
            fired += [m for _, m in alerts]
        for m in fired:
            tg_trade(m)
            push('alert', {'msg': m})
        push('positions', {'rows': db.list_positions(True, 100), 'summary': db.position_summary()})
        push('tracking', db.list_tracking(100))
        return len(fired)
    finally:
        _track_lock.release()


def _sizing(entry, stop):
    """계좌 리스크 기반 매수 수량 (로스 카메론 1~2% 원칙)"""
    acct = float(CFG.get('account_size') or 0)
    if acct <= 0 or entry <= 0 or stop >= entry:
        return None
    risk_amt = acct * float(CFG.get('risk_pct', 1.5)) / 100
    q_risk = int(risk_amt // (entry - stop))
    q_cap = int(acct * float(CFG.get('max_pos_pct', 25)) / 100 // entry)
    qty = max(0, min(q_risk, q_cap))
    return {'qty': qty, 'amount': qty * entry, 'weight': round(qty * entry / acct * 100, 1),
            'loss_at_stop': round(qty * (entry - stop)), 'capped': q_cap < q_risk}


# ════════════════════════════════════════════
#  API
# ════════════════════════════════════════════
@app.get("/", response_class=HTMLResponse)
async def index():
    if os.path.exists(HTML_FILE):
        return HTMLResponse(open(HTML_FILE, encoding='utf-8').read())
    return HTMLResponse("<h1>scout.html 파일이 없습니다</h1>")


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    global _loop
    _loop = asyncio.get_running_loop()
    await ws.accept()
    _clients.add(ws)
    try:
        await ws.send_text(json.dumps({'event': 'job', 'data': dict(JOB)},
                                      ensure_ascii=False))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _clients.discard(ws)


def _mask(v, keep=4):
    """민감 값 가리기 — keep=0이면 전부 가림 (주의: 파이썬 v[-0:]은 전체 문자열이 됨)"""
    v = str(v or '')
    if not v:
        return ''
    if keep <= 0 or len(v) <= keep + 2:      # 짧은 값은 끝자리도 보이지 않게 전부 가림
        return '●' * min(8, max(4, len(v)))
    return '●' * max(2, min(6, len(v) - keep)) + v[-keep:]


def _masked():
    """저장된 민감 설정을 가려서 반환 — 화면에서 '저장됨'을 확인할 수 있게"""
    return {
        'app_key': _mask(CFG.get('app_key')),
        'app_secret': _mask(CFG.get('app_secret'), 2),
        'account_no': _mask(CFG.get('account_no')),
        'telegram_token': _mask(CFG.get('telegram_token')),
        'telegram_chat': _mask(CFG.get('telegram_chat')),
        'dart_key': _mask(CFG.get('dart_key')),
        'krx_id': _mask(CFG.get('krx_id')),
        'krx_pw': _mask(CFG.get('krx_pw'), 0) if CFG.get('krx_pw') else '',
        'account_cd': CFG.get('account_cd', '01'),
    }


@app.get("/api/virtual")
async def api_virtual():
    judge, crit = db.vt_judge(VT_EXPECT_SIGN, VT_EXPECT_NUM)
    return {'stats': db.vt_stats(), 'rows': db.vt_list(200), 'expect': VT_EXPECT, 'names': VT_NAMES,
            'judge': judge, 'criteria': crit}


@app.get("/api/at")
async def api_at():
    at.init()
    c = db.conn()
    mode = CFG.get('at_mode', 'OFF')
    vm = mode if mode != 'OFF' else 'DRY'
    judge, _ = db.vt_judge(VT_EXPECT_SIGN, VT_EXPECT_NUM)
    g = at.gate(CFG.get('at_track', 'final'), judge)
    sig = db.meta_get('vt_last_batch', '')
    plan_buy = [dict(r) for r in c.execute("SELECT rank, ticker, name, signal_close FROM vtrades WHERE grp=? AND signal_date=? "
                                           "ORDER BY rank", (CFG.get('at_track', 'final'), sig))] if sig else []
    pos = [dict(r) for r in c.execute("SELECT * FROM at_positions WHERE mode=? ORDER BY (status='청산'), id DESC LIMIT 100", (vm,))]
    for p_ in pos:
        if p_['status'] == '보유' and p_.get('avg_price'):
            p_['last'] = at._last_close(p_['ticker'])
            p_['upnl'] = round((p_['last'] / p_['avg_price'] - 1) * 100 - db.VT_COST, 2) if p_['last'] else None   # 청산 손익과 같게 비용 반영
    snaps = [dict(r) for r in c.execute("SELECT date, equity, cash FROM at_snap WHERE mode=? ORDER BY date", (vm,))]
    jt = judge.get(CFG.get('at_track', 'final')) or {}
    crit_ = db.vt_criteria()
    dry_days = c.execute("SELECT COUNT(DISTINCT date) FROM at_snap WHERE mode='DRY'").fetchone()[0]
    live_days = c.execute("SELECT COUNT(DISTINCT date) FROM at_snap WHERE mode='LIVE'").fetchone()[0]
    orders = [dict(r) for r in c.execute("SELECT * FROM at_orders WHERE mode=? ORDER BY id DESC LIMIT 60", (vm,))]
    logs = [dict(r) for r in c.execute("SELECT * FROM at_log ORDER BY ts DESC LIMIT 40")]
    lim = {k: CFG.get(k) for k in ('at_track', 'at_slots', 'at_max_order_krw', 'at_max_daily_buys', 'at_max_positions',
                                   'at_daily_loss_stop', 'at_dry_cash', 'at_capital_live')}
    lim['model_cap'] = (CFG.get('model_order_max') or {}).get(CFG.get('at_track', 'final'))
    tr_ = CFG.get('at_track', 'final')
    lim['exit_rule'] = db.rule_text(db.rule_of(tr_))
    lim['exit_note'] = RULE_NOTE.get(tr_, '')
    return {'mode': mode, 'view_mode': vm, 'gate': g, 'limits': lim, 'signal_date': sig, 'plan_buy': plan_buy,
            'plan_sell': [p for p in pos if p['status'] == '보유' and p.get('sell_flag')], 'positions': pos,
            'orders': orders, 'log': logs, 'live_phrase': at.LIVE_PHRASE,
            'has_live': bool(CFG.get('account_no') and CFG.get('app_key')),
            'snaps': snaps, 'dry_days': dry_days, 'live_days': live_days,
            'progress': {'n': jt.get('n') or 0, 'days': jt.get('days') or 0, 'repro': jt.get('repro'), 'verdict': jt.get('verdict'),
                         'need_n': crit_.get('min_trades', 60), 'need_days': crit_.get('min_days', 40)},
            'has_kis': bool(CFG.get('app_key') and CFG.get('app_secret')),
            'has_tg': bool(CFG.get('telegram_token') and CFG.get('telegram_chat'))}


@app.get("/api/at/account")
async def api_at_account(view: str = ''):
    """실전 계좌(LIVE) · 장부(DRY) 잔고 — 실전 계좌 종목은 이 화면에서만 보여줌"""
    mode = CFG.get('at_mode', 'OFF')
    if not view:                                   # 기본: 지금 모드의 계좌 → 꺼져 있으면 실전 계좌(있으면)
        view = mode if mode in ('DRY', 'LIVE') else ('LIVE' if CFG.get('account_no') else 'DRY')
    if view not in ('DRY', 'LIVE'):
        return JSONResponse({'view': view, 'error': '모의투자(PAPER)는 v5.6.1에서 뺐습니다 — 실전 · 장부(DRY)만 조회'}, 200)
    try:
        if view == 'DRY':
            at.init()
            eq, cash = at.equity('DRY', CFG)
            hold = [{'ticker': r['ticker'], 'name': r['name'], 'qty': r['qty'], 'avg': r['avg_price'],
                     'price': at._last_close(r['ticker']) or r['avg_price']}
                    for r in db.conn().execute("SELECT * FROM at_positions WHERE mode='DRY' AND status='보유'")]
            for h in hold:
                h['pnl'] = (h['price'] - h['avg']) * h['qty']
                h['pnl_pct'] = (h['price'] / h['avg'] - 1) * 100 if h['avg'] else 0
            return {'view': 'DRY', 'cash': cash, 'total': eq, 'pnl': sum(h['pnl'] for h in hold), 'holdings': hold}
        a = at.fetch_account(view, CFG)
        return {'view': view, **a}
    except Exception as e:
        return JSONResponse({'view': view, 'error': str(e)[:200]}, 200)


@app.post("/api/at/config")
async def api_at_config(req: Request):
    b = await req.json()
    mode = b.get('mode', CFG.get('at_mode', 'OFF'))
    if mode not in at.MODES:
        return JSONResponse({'ok': False, 'error': '알 수 없는 모드'}, 400)
    for k, lo, hi in (('at_capital_live', 0, 1e11),):
        if k in b and b[k] not in (None, ''):
            try:
                v = float(b[k])
            except (TypeError, ValueError):
                return JSONResponse({'ok': False, 'error': f'{k} 값이 숫자가 아닙니다'}, 400)
            if not lo <= v <= hi:
                return JSONResponse({'ok': False, 'error': f'{k}는 {lo:g} ~ {hi:g} 사이여야 합니다'}, 400)
            CFG[k] = int(v)
    for k in ('at_slots', 'at_max_order_krw', 'at_max_daily_buys', 'at_max_positions', 'at_daily_loss_stop', 'at_dry_cash'):
        if k in b and b[k] not in (None, ''):
            try:
                v = float(b[k])
            except (TypeError, ValueError):
                return JSONResponse({'ok': False, 'error': f'{k} 값이 숫자가 아닙니다'}, 400)
            if v <= 0:
                return JSONResponse({'ok': False, 'error': f'{k}는 0보다 커야 합니다'}, 400)
            CFG[k] = int(v) if k not in ('at_daily_loss_stop',) else v
    if mode == 'LIVE':
        judge, _ = db.vt_judge(VT_EXPECT_SIGN, VT_EXPECT_NUM)
        g = at.gate(CFG.get('at_track', 'final'), judge)
        if not g['pass']:
            return JSONResponse({'ok': False, 'error': '가상매매 검증 미통과 — ' + ' · '.join(g['reasons'])}, 400)
        if not (CFG.get('account_no') and CFG.get('app_key')):
            return JSONResponse({'ok': False, 'error': '실전 계좌번호와 KIS 앱키가 필요합니다'}, 400)
        if (b.get('confirm') or '').strip() != at.LIVE_PHRASE:
            return JSONResponse({'ok': False, 'error': f"확인 문구 '{at.LIVE_PHRASE}'를 정확히 입력하세요"}, 400)
    prev = CFG.get('at_mode', 'OFF')
    CFG['at_mode'] = mode
    save_cfg(CFG)
    if prev != mode:
        at.init()
        at.log(mode, f'모드 변경: {prev} → {mode}', 'warn' if mode == 'LIVE' else 'info')
        telegram(f"🤖 자동매매 모드 변경: {prev} → {mode}")
    return {'ok': True, 'mode': mode}


@app.get("/api/rt")
async def api_rt():
    """실전형 가상매매 — 모델별 성과 · 설정 · 감시 상태"""
    lc = json.loads(db.meta_get('rt_last_cycle', '') or '{}')
    return {'start': db.meta_get('rt_start', ''), 'stats': rt.stats(CFG), 'names': VT_NAMES, 'order': MODEL_ORDER,
            'defaults': rt.DEFAULT, 'labels': rt.LABEL, 'limits': rt.LIMITS, 'market': list(rt.MARKET),
            'monitor': {'running': RT_STATE['running'], 'last': RT_STATE['last'] or lc.get('ts'),
                        'tickers': lc.get('tickers'), 'errors': lc.get('errors'), 'err': RT_STATE['err'],
                        'has_kis': bool(CFG.get('app_key') and CFG.get('app_secret'))}}


@app.post("/api/rt/settings")
async def api_rt_settings(req: Request):
    b = await req.json()
    g = b.get('grp', '')
    vals = rt.model_default(g) if (b.get('reset') and g in rt.MODELS) else {k: b[k] for k in rt.KEYS if k in b and b[k] not in (None, '')}
    err = rt.set_settings(CFG, g, vals)
    if err:
        return JSONResponse({'ok': False, 'error': err}, 400)
    save_cfg(CFG)
    return {'ok': True, 'settings': rt.settings(CFG, g if g != 'all' else 'final')}


def _verify_default_from():
    """검증 시작일 = 실전형 시작 신호 다음 거래일 (없으면 그 신호일)"""
    st = db.meta_get('rt_start', '') or datetime.now().strftime('%Y%m%d')
    r = db.conn().execute("SELECT MIN(date) FROM candles WHERE date>?", (st,)).fetchone()
    if r and r[0]:
        return r[0]
    d = datetime.strptime(st, '%Y%m%d') + timedelta(days=1)
    while d.weekday() >= 5:                               # 주말 건너뛰기
        d += timedelta(days=1)
    return d.strftime('%Y%m%d')


@app.get("/api/vacct")
async def api_vacct(kind: str = 'bt', g: str = 'final', full: int = 0):
    """가상 계좌 — 예수금 · 보유 종목 · 평가손익 · 거래 내역 · 일별 추이 (kind: bt 백테스트형 · rt 실전형)
       기본은 두 계좌 기간을 맞춰 실전형 시작 신호일부터 · full=1이면 백테스트형 전체 기간"""
    if kind not in ('bt', 'rt') or g not in MODEL_ORDER:
        return JSONResponse({'error': '알 수 없는 계좌'}, 404)
    since = '' if full else db.meta_get('rt_start', '')
    a = await asyncio.to_thread(vacct.account, kind, g, since)
    a['name'] = VT_NAMES.get(g, g)
    a['since'] = since
    return a


@app.get("/api/vacct/all")
async def api_vacct_all(kind: str = 'bt', full: int = 0):
    """9개 모델 가상 계좌 요약 (모델 비교용)"""
    if kind not in ('bt', 'rt'):
        return JSONResponse({'error': '알 수 없는 계좌'}, 404)
    since = '' if full else db.meta_get('rt_start', '')
    out = await asyncio.to_thread(lambda: {g: vacct.account(kind, g, since)['summary'] for g in MODEL_ORDER})
    return {'kind': kind, 'order': MODEL_ORDER, 'names': VT_NAMES, 'summary': out, 'since': since}


@app.get("/api/verify/info")
async def api_verify_info():
    today = datetime.now().strftime('%Y%m%d')
    return {'from': min(_verify_default_from(), today), 'to': today, 'start': _verify_default_from(), 'rt_start': db.meta_get('rt_start', '')}


@app.get("/api/verify/export")
async def api_verify_export(frm: str = '', to: str = '', check: int = 0):
    """검증 데이터 zip — frm~to (기본: 검증 시작일 ~ 오늘). check=1이면 zip 대신 자동 점검 결과만"""
    to = (to or datetime.now().strftime('%Y%m%d')).replace('-', '')[:8]
    frm = (frm or min(_verify_default_from(), to)).replace('-', '')[:8]
    if not (frm.isdigit() and to.isdigit() and len(frm) == 8 and len(to) == 8) or frm > to:
        return JSONResponse({'ok': False, 'error': '날짜 형식은 YYYYMMDD, 시작일 ≤ 끝일'}, 400)
    secrets = [CFG.get(k) for k in verify.SECRET_KEYS]
    data, fn, info = await asyncio.to_thread(verify.build, CFG, frm, to, APP_VERSION, BASE_DIR, secrets)
    if check:
        return {'ok': True, 'from': frm, 'to': to, **info}
    return Response(content=data, media_type='application/zip',
                    headers={'Content-Disposition': f'attachment; filename="{fn}"'})


@app.get("/api/rt/model/{g}")
async def api_rt_model(g: str):
    if g not in rt.MODELS:
        return JSONResponse({'error': '알 수 없는 모델'}, 404)
    return {'g': g, 'name': VT_NAMES.get(g, g), 'trades': rt.trades(g), 'log': rt.settings_log(g),
            'settings': rt.settings(CFG, g), 'settings_text': rt.settings_text(rt.settings(CFG, g))}


@app.post("/api/at/kill")
async def api_at_kill():
    res = at.kill(CFG)
    prev = CFG.get('at_mode', 'OFF')
    CFG['at_mode'] = 'OFF'
    save_cfg(CFG)
    telegram(f"🛑 자동매매 긴급 정지 ({prev} → OFF)\n" + ('\n'.join('· ' + r for r in res) or '· 미체결 주문 없음'))
    return {'ok': True, 'cancelled': res}


VT_DESC = {
    'final': ('반전(과매도·5일 하락 평균) + 외국인 20일 매수 + 연기금 20일 역방향, 동일 가중', None),
    'strategy': ('과매도(RSI14) + 외국인 20일 매수 + 연기금 역방향 (연기금 없으면 기관)', None),
    'candle': ('상승반전 캔들(망치·상승장악·관통·샛별·3내부상승·잠자리도지) 중 강하게 마감한 순', None),
    'rsi': ('로스카메론 극단값 — 2일 RSI 5 이하, 깊은 순', None),
    'fdip': ('최근 5일 많이 빠졌는데 외국인이 사는 종목', None),
    'lvflow': ('변동성 낮고 외국인이 사는 종목', None),
    'lvhigh': ('차트 모델 — 변동성 낮고 52주 고점 근처이며 거래량 실린 급등이 적은 종목 (조용한 강세 · 세 가지 동일 가중)', None),
    'jongga': ('종가베팅 — 양봉 · +2% 넘게 올라 고가 근처(당일 범위 상단 20%)에서 마감 · 20일선 위 · 60일 전고점의 97~100%까지 바짝 · '
               '거래대금이 20일 평균의 1.5배 이상 → 그날 종가(장후 시간외 종가 15:40~16:00)에 매수, 여럿이면 많이 오른 순', None),
    'v62': ('SCOUT v6.2 — 모멘텀 + 외국인·기관 수급 23요소 가중합',
            '60% 진입 · −7/−12% 추가매수 · 평단 +40% 익절 · 첫 진입가 −20% 손절 · 최대 20일 (장중 도달 시)'),
}
# 청산 규칙 역검증 근거 (504개 조합 · 조정 기간에서 선택 → 검증 기간 계좌 단위로 채점)
RULE_NOTE = {
    'final': '현행 유지 — 조정기간 최적(손절20·익절15·10일)은 검증기간 계좌 +19.0%로 현행 +18.0%와 비슷하지만 최대낙폭 −29% vs −13%',
    'strategy': '현행 유지 — 조정기간 최적(익절15·15일)이 검증기간 계좌 +6.2%로 현행 +9.5%보다 낮음',
    'rsi': '현행 유지 — 조정기간 최적(9EMA·익절20·7일)이 검증기간 계좌 −13.5%로 현행 −12.6%보다 낮음',
    'fdip': '변경 — 익절 +5%·20일이 계좌 기준 조정 +41.5%→+60.6%, 검증 +3.7%→+16.0% (반등형: 빨리 챙기기)',
    'lvflow': '변경 — 20일 보유가 계좌 기준 조정 −3.8%→+61.0%, 검증 +10.2%→+20.1% (추세형: 오래 들고 가기)',
    'lvhigh': '신규(v5.4) — 시스템 청산 규칙 3가지 중 조정 기간 보유 하루당 수익 1위인 20일 보유(+0.066 · 익절5%·20일 +0.056 · '
              '9EMA +0.017). 504개 조합의 조정 최적(익절 20%·20일 +0.072)과 거의 같아 단순한 쪽 · 계좌 기준 조정 +34.0% / 검증 +22.4% '
              '(최대낙폭 −5.8% / −7.6%, 같은 규칙 대조군 −9.6% / +5.7%)',
    'candle': '변경 — 익절 +5%·20일이 계좌 기준 조정 −12.3%→+5.6%, 검증 −4.7%→+7.3% (최대낙폭은 −39%로 커짐)',
    'jongga': '신규(v5.6) — 종가베팅의 정의(종가 매수 → 다음날 아침 시가 매도) 그대로 고정. 다음날 종가 매도였다면 조정 −0.41% / '
              '검증 +0.54%로 기간마다 엇갈려 쓰지 않음 · 계좌 기준 조정 −1.9% / 검증 +4.8% (최대낙폭 −5.0% / −2.1%, '
              '같은 규칙 대조군 300회 평균 −7.5% / +2.5%) · 대조군 대비 차이는 작음 (t 0.2 / 0.5)',
    'v62': '원래 설계 유지 — 조정기간 최적과 거의 같고(하루당 +0.025 vs +0.024) 검증기간은 원래 설계가 최선(+0.077)',
}
MODEL_ORDER = ['final', 'strategy', 'fdip', 'lvflow', 'lvhigh', 'rsi', 'candle', 'jongga', 'v62']


@app.get("/api/models")
async def api_models():
    judge, crit = db.vt_judge(VT_EXPECT_SIGN, VT_EXPECT_NUM)
    st = db.vt_stats()
    cur = {g: db.vt_curve(g) for g in MODEL_ORDER + ['control', 'control_v62', 'control_jongga']}
    acc = {g: cur[g]['stats'] for g in cur}
    starts = [cur[g]['curve'][0]['date'] for g in MODEL_ORDER if cur[g]['curve']]
    bench, bench_cv = _bench_view(min(starts)) if starts else (None, [])
    return {'order': MODEL_ORDER, 'names': VT_NAMES, 'desc': VT_DESC, 'expect': VT_EXPECT, 'stats': st,
            'judge': judge, 'accounts': acc, 'criteria': crit,
            'curves': {g: [[p_['date'], p_['equity']] for p_ in cur[g]['curve']] for g in cur},
            'rules': {g: (VT_DESC.get(g, ('', None))[1] or db.rule_text(db.rule_of(g))) for g in MODEL_ORDER},
            'notes': RULE_NOTE, 'order_max': {g: db.order_cap(g) for g in MODEL_ORDER + ['control', 'control_v62', 'control_jongga']},
            'month': {g: _model_month(g, cur[g]['curve']) for g in MODEL_ORDER},
            'bench': bench, 'bench_curve': [[p_['date'], p_['equity']] for p_ in bench_cv],
            'z_need': next(iter(judge.values()))['z_need'] if judge else None}


@app.get("/api/model/{g}")
async def api_model(g: str):
    if g not in VT_NAMES:
        return JSONResponse({'error': '알 수 없는 모델'}, 404)
    c = db.conn()
    ctrl = db.CONTROL_OF.get(g, 'control')
    last = c.execute("SELECT MAX(signal_date) FROM vtrades WHERE grp=?", (g,)).fetchone()[0]
    picks = [dict(r) for r in c.execute("SELECT * FROM vtrades WHERE grp=? AND signal_date=? ORDER BY rank", (g, last))] if last else []
    opens = [dict(r) for r in c.execute("SELECT * FROM vtrades WHERE grp=? AND status='보유' ORDER BY entry_date DESC", (g,))]
    closed = [dict(r) for r in c.execute("SELECT * FROM vtrades WHERE grp=? AND status='청산' ORDER BY exit_date DESC LIMIT 300", (g,))]
    # 가상매매 vs 실전 리허설(DRY) — 같은 신호일·같은 종목의 체결가 · 손익 비교
    rehearsal = []
    try:
        at.init()
        for p in c.execute("SELECT * FROM at_positions WHERE mode='DRY' AND track=? ORDER BY signal_date DESC LIMIT 100", (g,)):
            v = c.execute("SELECT * FROM vtrades WHERE grp=? AND signal_date=? AND ticker=?", (g, p['signal_date'], p['ticker'])).fetchone()
            if not v or not v['entry_price']:
                continue
            rehearsal.append({'name': p['name'], 'ticker': p['ticker'], 'signal_date': p['signal_date'],
                              'v_entry': v['entry_price'], 'p_entry': p['avg_price'],
                              'entry_gap': (p['avg_price'] / v['entry_price'] - 1) * 100 if p['avg_price'] else None,
                              'v_ret': v['ret'], 'p_ret': p['ret'], 'v_status': v['status'], 'p_status': p['status']})
        r_sig = c.execute("SELECT COUNT(DISTINCT signal_date) FROM at_orders WHERE mode='DRY' AND side='buy'").fetchone()[0]
        r_miss = c.execute("SELECT COUNT(*) FROM at_orders WHERE mode='DRY' AND side='buy' AND status='미체결'").fetchone()[0]
    except Exception:
        rehearsal, r_sig, r_miss = [], 0, 0
    dsc = VT_DESC.get(g) or ('', None)
    curve_g = db.vt_curve(g)['curve']
    bench_g = db.bench_curve(curve_g[0]['date'] if curve_g else None)      # 같은 기간 지수 ETF 그냥 보유
    return {'g': g, 'name': VT_NAMES[g], 'desc': (dsc[0], dsc[1] or db.rule_text(db.rule_of(g))),
            'rule_note': RULE_NOTE.get(g, ''), 'expect': VT_EXPECT.get(g), 'control': ctrl,
            'curve': curve_g, 'ctrl_curve': db.vt_curve(ctrl, cap_grp=g)['curve'],   # 대조군도 이 모델 금액으로
            'month': _model_month(g, curve_g) if g in MODEL_ORDER else None,
            'bench': bench_g['stats'], 'bench_curve': bench_g['curve'],
            'order_cap': db.order_cap(g),
            'last_signal': last, 'picks': picks, 'opens': opens, 'closed': closed,
            'rehearsal': rehearsal, 'rehearsal_track': CFG.get('at_track', 'final'),
            'rehearsal_days': r_sig, 'rehearsal_miss': r_miss, 'at_mode': CFG.get('at_mode', 'OFF')}


@app.get("/api/flows/summary")
async def api_flows_summary():
    """수급 데이터 현황 — KRX(외국인·기관·연기금, 전종목) + KIS(후보풀 외국인·기관) · 최근일 순매수 상위/하위"""
    c = db.conn()
    names = {r[0]: r[1] for r in c.execute("SELECT ticker, name FROM stocks")}
    out = {'krx': [], 'kis': {}}
    for inv in ('외국인', '기관합계', '연기금'):
        r = c.execute("SELECT COUNT(DISTINCT date), MIN(date), MAX(date), COUNT(*) FROM flows WHERE investor=?", (inv,)).fetchone()
        days, d0, d1, rows = r[0] or 0, r[1], r[2], r[3] or 0
        top, bot, tot = [], [], None
        if d1:
            q = "SELECT ticker, amt FROM flows WHERE investor=? AND date=? ORDER BY amt {} LIMIT 5"
            top = [{'ticker': t, 'name': names.get(t, t), 'amt': a} for t, a in c.execute(q.format('DESC'), (inv, d1))]
            bot = [{'ticker': t, 'name': names.get(t, t), 'amt': a} for t, a in c.execute(q.format('ASC'), (inv, d1))]
            tot = c.execute("SELECT SUM(amt), COUNT(*) FROM flows WHERE investor=? AND date=?", (inv, d1)).fetchone()
        out['krx'].append({'investor': inv, 'days': days, 'first': d0, 'last': d1,
                           'per_day': round(rows / days) if days else 0, 'top': top, 'bottom': bot,
                           'net_total': tot[0] if tot else None, 'n_last': tot[1] if tot else 0})
    r = c.execute("SELECT COUNT(DISTINCT ticker), COUNT(DISTINCT date), MIN(date), MAX(date) FROM investors").fetchone()
    out['kis'] = {'tickers': r[0] or 0, 'days': r[1] or 0, 'first': r[2], 'last': r[3]}
    return out


@app.get("/api/vt/trades")
async def api_vt_trades(limit: int = 3000):
    rows = [dict(r) for r in db.conn().execute(
        "SELECT * FROM vtrades WHERE grp!='rsi14_old' ORDER BY signal_date DESC, grp, rank LIMIT ?", (limit,))]
    return {'rows': rows, 'names': VT_NAMES}


@app.get("/api/rt/trades")
async def api_rt_trades(limit: int = 3000):
    """실전형 가상매매 거래 목록 — 매매내역 탭에서 백테스트형과 나란히 (같은 모양의 행)"""
    rt.init()
    out = []
    for r in db.conn().execute("SELECT * FROM rt_trades ORDER BY signal_date DESC, grp, rank LIMIT ?", (limit,)):
        r = dict(r)
        fills = json.loads(r.get('fills') or '[]')
        out.append({'signal_date': r['signal_date'], 'grp': r['grp'], 'ticker': r['ticker'], 'name': r['name'], 'rank': r['rank'],
                    'status': r['status'], 'entry_date': r['entry_date'], 'entry_price': r['entry_price'],
                    'exit_date': r['exit_date'], 'exit_price': r['exit_price'], 'ret': r['ret'], 'held': r['held'],
                    'exit_reason': r['exit_reason'] or (r['sell_flag'] and f"매도 예정 — {r['sell_flag']}") or '',
                    'last_close': r['last_price'], 'remain': r['remain'], 'n_fills': len(fills),
                    'fills_text': ' · '.join(f"{f['d'][4:6]}/{f['d'][6:]} {str(f['t'])[:5]} {f['why']} {round(f['frac'] * 100)}%" for f in fills)})
    return {'rows': out, 'names': VT_NAMES, 'start': db.meta_get('rt_start', '')}


@app.post("/api/kis/test")
async def api_kis_test():
    """KIS 연결 확인 — 토큰 발급 + 삼성전자 현재가 조회 (주문 없음)"""
    if not (CFG.get('app_key') and CFG.get('app_secret')):
        return {'ok': False, 'msg': 'KIS 앱키가 설정되지 않았습니다'}
    t0 = time.time()
    try:
        tok = db.get_token(CFG['app_key'], CFG['app_secret'])
        r = db.kis_get('/uapi/domestic-stock/v1/quotations/inquire-price', 'FHKST01010100',
                       {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': '005930'}, CFG['app_key'], CFG['app_secret'], tok)
        px = (r.get('output') or {}).get('stck_prpr')
        ok = r.get('rt_cd') == '0' and px
        return {'ok': bool(ok), 'ms': round((time.time() - t0) * 1000),
                'msg': f"연결 정상 · 삼성전자 현재가 {int(float(px)):,}원" if ok else f"응답 이상: {r.get('msg1', '')}"}
    except Exception as e:
        return {'ok': False, 'msg': f'연결 실패: {str(e)[:120]}'}


@app.get("/api/account")
async def api_account():
    return {g: db.vt_account(g) for g in ('final', 'control')}


_BOOT = datetime.now()


@app.get("/api/health")
async def api_health():
    import shutil
    checks = []

    def add(name, ok, detail, level='warn'):
        checks.append({'name': name, 'ok': bool(ok), 'detail': detail, 'level': 'ok' if ok else level})
    add('KIS 앱키', CFG.get('app_key') and CFG.get('app_secret'), '설정됨' if CFG.get('app_key') else '미설정', 'err')
    add('KRX 계정 (연기금 수급)', CFG.get('krx_id') and CFG.get('krx_pw'),
        '설정됨' if CFG.get('krx_id') else '미설정 — 종합·최종 트랙이 기관으로 대체됨')
    add('텔레그램', CFG.get('telegram_token') and CFG.get('telegram_chat'),
        '설정됨' if CFG.get('telegram_token') else '미설정 — 일일 보고를 받을 수 없음')
    last = (db.recent_trading_dates(1) or [''])[-1]
    stale = True
    if last:
        stale = (datetime.now() - datetime.strptime(last, '%Y%m%d')).days > 4
    add('일봉 최신일', last and not stale, f"{last[:4]}-{last[4:6]}-{last[6:]}" if last else '없음', 'err')
    fl = max(db.flow_dates('연기금') or {''})
    add('연기금 수급 최신일', fl and fl >= (last or '0'), f"{fl[:4]}-{fl[4:6]}-{fl[6:]}" if fl else '없음')
    dq = db.last_quality()
    add('데이터 품질', dq.get('status') == 'OK', dq.get('why', '아직 점검 전') + (f" ({dq.get('date')})" if dq.get('date') else ''))
    b = db.latest_backup()
    add('가상매매 백업', b is not None, os.path.basename(b) if b else '아직 없음 (첫 기록 때 자동 생성)')
    vb = db.meta_get('vt_last_batch', '')
    add('가상매매 마지막 기록', bool(vb), vb or '아직 없음 — 첫 기록은 18:20')
    free = shutil.disk_usage(db.DATA_DIR).free / 1e9
    add('디스크 여유', free > 2, f'{free:.1f} GB')
    up = datetime.now() - _BOOT
    add('서버 가동', True, f"{up.days}일 {up.seconds // 3600}시간")
    bad = [c for c in checks if not c['ok']]
    overall = 'err' if any(c['level'] == 'err' for c in bad) else ('warn' if bad else 'ok')
    return {'overall': overall, 'checks': checks}


# ════════════════════════════════════════════
#  시스템 정보 탭 — 프로그램 · 저장 공간 · 데이터 · 가상매매 · 자동매매 · 연결 · 일정 · 점검 · 파일
#  (비밀 값은 보내지 않음: 키·비밀번호·토큰은 '설정됨' 여부와 가린 끝자리만)
# ════════════════════════════════════════════
SCHED_INFO = {
    'at_morning': ('job_at_morning', '자동매매 아침 주문', '전날 판정한 매도 + 신규 매수 (시가 동시호가) · 자동매매를 켰을 때만 주문'),
    'build': ('job_build', '장마감 증분 동기화', '일봉·수급 수집 → 데이터 품질 점검 → 가상매매 매수·매도 갱신'),
    'at_close': ('job_at_close', '자동매매 장마감 정리', '체결 확인 · 다음 날 매도 판정 · 증권사 잔고 대조'),
    'at_fill': ('job_at_fill', '리허설(DRY) 시가 체결 확인', 'KIS 현재가로 오늘 실제 시가 체결 처리 · 텔레그램 알림 · DRY일 때만'),
    'flows': ('job_flows', 'KRX 수급 수집', '외국인 · 기관 · 연기금 순매수 (당일 확정치)'),
    'vtreport': ('job_vt_report', '가상매매 주간 보고', '금요일만 · 텔레그램으로 발송'),
    'orders': ('job_morning_orders', '주문표', '예전 기능'),
}
SCAN_INFO = {'08:40': ('장전 후보 스캔', '전일 종가 기준 · 놓친 날 가상매매 자동 보충'),
             '18:20': ('다음 날 후보 → 가상매매 기록', '연기금 수급 반영 · 9개 모델 + 대조군 기록 (종가베팅은 그날 종가 매수)')}


def _proc_mem_mb():
    try:
        import psutil
        return round(psutil.Process().memory_info().rss / 1e6)
    except Exception:
        pass
    try:
        if sys.platform == 'win32':
            import ctypes
            from ctypes import wintypes as wt

            class PMC(ctypes.Structure):
                _fields_ = [('cb', wt.DWORD), ('PageFaultCount', wt.DWORD), ('PeakWorkingSetSize', ctypes.c_size_t),
                            ('WorkingSetSize', ctypes.c_size_t), ('QuotaPeakPagedPoolUsage', ctypes.c_size_t),
                            ('QuotaPagedPoolUsage', ctypes.c_size_t), ('QuotaPeakNonPagedPoolUsage', ctypes.c_size_t),
                            ('QuotaNonPagedPoolUsage', ctypes.c_size_t), ('PagefileUsage', ctypes.c_size_t),
                            ('PeakPagefileUsage', ctypes.c_size_t)]
            k32 = ctypes.windll.kernel32
            k32.GetCurrentProcess.restype = wt.HANDLE
            fn = k32.K32GetProcessMemoryInfo
            fn.argtypes = [wt.HANDLE, ctypes.POINTER(PMC), wt.DWORD]
            pmc = PMC()
            pmc.cb = ctypes.sizeof(PMC)
            if fn(k32.GetCurrentProcess(), ctypes.byref(pmc), pmc.cb):
                return round(pmc.WorkingSetSize / 1e6)
            return None
        with open('/proc/self/status') as f:
            for line in f:
                if line.startswith('VmRSS:'):
                    return round(int(line.split()[1]) / 1e3)
    except Exception:
        pass
    return None


def _sys_mem():
    try:
        import psutil
        vm = psutil.virtual_memory()
        return {'total': vm.total, 'avail': vm.available}
    except Exception:
        pass
    try:
        if sys.platform == 'win32':
            import ctypes

            class MS(ctypes.Structure):
                _fields_ = [('dwLength', ctypes.c_ulong), ('dwMemoryLoad', ctypes.c_ulong),
                            ('ullTotalPhys', ctypes.c_ulonglong), ('ullAvailPhys', ctypes.c_ulonglong),
                            ('ullTotalPageFile', ctypes.c_ulonglong), ('ullAvailPageFile', ctypes.c_ulonglong),
                            ('ullTotalVirtual', ctypes.c_ulonglong), ('ullAvailVirtual', ctypes.c_ulonglong),
                            ('ullAvailExtendedVirtual', ctypes.c_ulonglong)]
            m = MS()
            m.dwLength = ctypes.sizeof(MS)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m)):
                return {'total': m.ullTotalPhys, 'avail': m.ullAvailPhys}
            return None
        ps = os.sysconf('SC_PAGE_SIZE')
        return {'total': os.sysconf('SC_PHYS_PAGES') * ps, 'avail': os.sysconf('SC_AVPHYS_PAGES') * ps}
    except Exception:
        return None


def _fsize(p):
    try:
        return os.path.getsize(p)
    except Exception:
        return 0


def _mtime(p):
    try:
        return datetime.fromtimestamp(os.path.getmtime(p)).strftime('%Y-%m-%d %H:%M')
    except Exception:
        return None


def _dir_size(d):
    tot = n = 0
    if os.path.isdir(d):
        for root, _, files in os.walk(d):
            for f in files:
                tot += _fsize(os.path.join(root, f))
                n += 1
    return tot, n


def _si_app():
    up = datetime.now() - _BOOT
    try:
        osname = f"{platform.system()} {platform.release()}"
        if sys.platform == 'win32':
            osname += f" (빌드 {platform.version()})"
    except Exception:
        osname = sys.platform
    return {'name': APP_NAME, 'version': APP_VERSION, 'boot': _BOOT.strftime('%Y-%m-%d %H:%M:%S'),
            'uptime_sec': int(up.total_seconds()), 'python': platform.python_version(), 'python_exe': sys.executable,
            'os': osname, 'machine': platform.machine(), 'cpu': os.cpu_count(), 'pid': os.getpid(), 'port': PORT,
            'base_dir': BASE_DIR, 'clients': len(_clients), 'proc_mb': _proc_mem_mb(), 'mem': _sys_mem(),
            'legacy': bool(CFG.get('legacy_features')), 'job': {k: JOB.get(k) for k in ('running', 'kind', 'msg', 'started')}}


def _si_storage():
    import shutil
    dbp = db.DB_PATH
    bdir = os.path.join(db.DATA_DIR, 'backups')
    bsz, bn = _dir_size(bdir)
    lb = db.latest_backup()
    ldir = os.path.join(BASE_DIR, 'logs')
    lsz, ln = _dir_size(ldir)
    du = shutil.disk_usage(db.DATA_DIR)
    return {'data_dir': db.DATA_DIR, 'db_path': dbp, 'db_bytes': _fsize(dbp) + _fsize(dbp + '-wal') + _fsize(dbp + '-shm'),
            'config': CONFIG_FILE, 'config_mtime': _mtime(CONFIG_FILE),
            'backups': {'dir': bdir, 'count': bn, 'bytes': bsz, 'keep': db.BACKUP_KEEP,
                        'latest': os.path.basename(lb) if lb else None, 'latest_time': _mtime(lb) if lb else None},
            'logs': {'dir': ldir, 'count': ln, 'bytes': lsz},
            'disk': {'total': du.total, 'free': du.free}}


def _si_data():
    c = db.conn()
    st = db.db_stats()
    last = (db.recent_trading_dates(1) or [''])[-1]
    ref = c.execute("SELECT ticker, name FROM stocks WHERE in_pool=1 ORDER BY avg_value DESC LIMIT 1").fetchone()
    first = c.execute("SELECT MIN(date), COUNT(*) FROM candles WHERE ticker=?", (ref[0],)).fetchone() if ref else (None, 0)
    flows = []
    for inv in ('외국인', '기관합계', '연기금'):
        r = c.execute("SELECT COUNT(DISTINCT date), MIN(date), MAX(date) FROM flows WHERE investor=?", (inv,)).fetchone()
        flows.append({'investor': inv, 'days': r[0] or 0, 'first': r[1], 'last': r[2]})
    kis = c.execute("SELECT COUNT(DISTINCT ticker), MAX(date) FROM investors").fetchone()
    meta = {k: db.meta_get(k, '') for k in ('universe_built', 'pool_built', 'pool_size', 'candles_synced', 'history_extended',
                                            'investors_synced', 'profiles_synced', 'dart_synced')}
    return {**st, 'first_candle': first[0], 'ref_stock': (ref[1] if ref else None), 'ref_bars': first[1], 'last_candle': last,
            'flows': flows, 'kis_flow': {'tickers': kis[0] or 0, 'last': kis[1]}, 'quality': db.last_quality(),
            'meta': meta, 'market': dict(_market), 'last_scan': {k: (v['ts'] if v else None) for k, v in _last_scan.items()}}


def _si_vt():
    c = db.conn()
    judge, crit = db.vt_judge(VT_EXPECT_SIGN, VT_EXPECT_NUM)
    by = {}
    for g, st_, n in c.execute("SELECT grp, status, COUNT(*) FROM vtrades WHERE grp!='rsi14_old' GROUP BY grp, status"):
        by.setdefault(g, {})[st_] = n
    tot = {k: sum(v.get(k, 0) for v in by.values()) for k in ('대기', '보유', '청산')}
    f = c.execute("SELECT MIN(signal_date), MAX(signal_date), COUNT(DISTINCT signal_date) FROM vtrades WHERE grp='final'").fetchone()
    try:
        hold = len(json.loads(db.meta_get('dq_hold_dates', '[]') or '[]'))
    except Exception:
        hold = 0
    tr_ = CFG.get('at_track', 'final')
    return {'order': MODEL_ORDER, 'names': VT_NAMES, 'by': by, 'total': tot, 'first': f[0], 'last': f[1], 'days': f[2] or 0,
            'criteria': crit, 'gate': at.gate(tr_, judge), 'gate_track': tr_, 'gate_passed_at': db.meta_get('gate_passed_at', ''),
            'dq_hold': hold, 'cash': db.ACCT['cash'], 'slots': db.ACCT['slots'], 'last_batch': db.meta_get('vt_last_batch', ''),
            'order_max': dict(CFG.get('model_order_max') or {}), 'bench': db.bench_info(),
            'rules': {g: (VT_DESC.get(g, ('', None))[1] or db.rule_text(db.rule_of(g))) for g in MODEL_ORDER},
            'judge': {g: {k: (judge.get(g) or {}).get(k) for k in ('n', 'days', 'verdict', 'repro')} for g in MODEL_ORDER}}


def _si_at():
    at.init()
    c = db.conn()
    today = datetime.now().strftime('%Y%m%d')
    pos = {m: c.execute("SELECT COUNT(*) FROM at_positions WHERE mode=? AND status='보유'", (m,)).fetchone()[0]
           for m in ('DRY', 'LIVE')}
    orders = {m: c.execute("SELECT COUNT(*) FROM at_orders WHERE mode=? AND date=?", (m, today)).fetchone()[0]
              for m in ('DRY', 'LIVE')}
    log = [dict(r) for r in c.execute("SELECT ts, mode, level, msg FROM at_log ORDER BY ts DESC LIMIT 8")]
    lim = {k: CFG.get(k) for k in ('at_track', 'at_slots', 'at_max_order_krw', 'at_max_daily_buys', 'at_max_positions',
                                   'at_daily_loss_stop', 'at_dry_cash', 'at_capital_live')}
    lim['model_cap'] = (CFG.get('model_order_max') or {}).get(CFG.get('at_track', 'final'))
    return {'mode': CFG.get('at_mode', 'OFF'), 'limits': lim, 'exit_rule': db.rule_text(db.rule_of(lim['at_track'] or 'final')),
            'buy_window': list(at.BUY_WINDOW), 'positions': pos, 'orders_today': orders, 'log': log,
            'has_live': bool(CFG.get('account_no') and CFG.get('app_key'))}


def _si_conn():
    import importlib.util
    import importlib.metadata as md

    def ver(p_):
        try:
            return md.version(p_)
        except Exception:
            return None
    tok = getattr(db, '_token', None) or {}
    exp = tok.get('exp')
    return {'kis': bool(CFG.get('app_key') and CFG.get('app_secret')), 'kis_key': _mask(CFG.get('app_key')),
            'kis_token': bool(tok.get('v') and exp and exp > datetime.now()),
            'kis_token_exp': exp.strftime('%m-%d %H:%M') if exp else None,
            'account': _mask(CFG.get('account_no')), 'account_cd': CFG.get('account_cd', '01'),
            'krx': bool(CFG.get('krx_id') and CFG.get('krx_pw')), 'krx_id': _mask(CFG.get('krx_id')),
            'telegram': bool(CFG.get('telegram_token') and CFG.get('telegram_chat')),
            'dart': bool(CFG.get('dart_key')),
            'ws_lib': bool(importlib.util.find_spec('websockets') or importlib.util.find_spec('wsproto')),
            'clients': len(_clients), 'flows_state': dict(FLOW_STATE),
            'packages': {p_: ver(p_) for p_ in ('fastapi', 'uvicorn', 'websockets', 'pykrx', 'pandas', 'numpy', 'requests', 'psutil')}}


def _si_schedule():
    now = datetime.now()
    ds, hm = now.strftime('%Y%m%d'), now.strftime('%H:%M')
    try:
        hc = json.loads(db.meta_get('holiday_cache', '{}') or '{}')
    except Exception:
        hc = {}
    weekend = now.weekday() >= 5
    trading = (not weekend) and bool(hc.get(ds, True))
    if weekend:
        session = '휴장 (주말)'
    elif not trading:
        session = '휴장 (공휴일)'
    elif hm < '08:30':
        session = '장 시작 전'
    elif hm < '09:00':
        session = '장전 동시호가'
    elif hm < '15:20':
        session = '장중'
    elif hm < '15:30':
        session = '장마감 동시호가'
    else:
        session = '장 마감'
    items = []
    for t, (kind, arg) in sorted(_active_schedule().items()):
        if kind == 'scan':
            lab, desc = SCAN_INFO.get(t, ('후보 스캔', f'{arg} (예전 기능)'))
            job = 'job_scan'
        else:
            job, lab, desc = SCHED_INFO.get(kind, (kind, kind, ''))
        items.append({'time': t, 'kind': kind, 'job': job, 'label': lab, 'desc': desc})
    hol = sorted(k for k, v in hc.items() if v is False and k > ds and datetime.strptime(k, '%Y%m%d').weekday() < 5)
    return {'now': now.strftime('%Y-%m-%d %H:%M'), 'weekday': '월화수목금토일'[now.weekday()], 'trading_today': trading,
            'checked': ds in hc, 'session': session, 'items': items, 'holidays': hol[:8],
            'weekly': [{'when': '월 07:30', 'label': '종목 목록·데이터 전체 갱신'}, {'when': '금 18:30', 'label': '가상매매 주간 보고 (휴장일이어도 발송)'}],
            'catch_up': '서버를 켤 때 놓친 동기화·수급·가상매매를 자동으로 보충'}


def _si_files():
    out = []
    for f in sorted(os.listdir(BASE_DIR)):
        if f.lower().endswith(('.py', '.html', '.bat', '.ps1', '.md')):
            p_ = os.path.join(BASE_DIR, f)
            out.append({'name': f, 'bytes': _fsize(p_), 'mtime': _mtime(p_)})
    return out


@app.get("/api/sysinfo")
async def api_sysinfo():
    out = {'ts': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    for key, fn in (('app', _si_app), ('storage', _si_storage), ('data', _si_data), ('vt', _si_vt), ('at', _si_at),
                    ('conn', _si_conn), ('schedule', _si_schedule), ('files', _si_files)):
        try:
            out[key] = fn()
        except Exception as e:                       # 한 칸이 실패해도 나머지는 보여줌
            out[key] = {'error': f'{type(e).__name__}: {e}'[:200]}
    try:
        out['health'] = await api_health()
    except Exception as e:
        out['health'] = {'error': str(e)[:200]}
    with _runs_lock:
        out['runs'] = {'last': dict(JOB_LAST), 'hist': list(JOB_HIST[:30])}
    return out


def _tg_test():
    import urllib.request, urllib.parse, urllib.error
    tok, chat = CFG.get('telegram_token'), CFG.get('telegram_chat')
    if not (tok and chat):
        return {'ok': False, 'error': '텔레그램 봇 토큰과 채팅 ID를 먼저 저장하세요'}
    data = urllib.parse.urlencode({'chat_id': chat, 'text': f"✅ {APP_NAME} 텔레그램 연결 테스트 — {datetime.now():%Y-%m-%d %H:%M}"}).encode()
    try:
        with urllib.request.urlopen(f"https://api.telegram.org/bot{tok}/sendMessage", data=data, timeout=10) as r:
            js = json.loads(r.read().decode())
        return {'ok': bool(js.get('ok')), 'error': '' if js.get('ok') else str(js.get('description', js))[:200]}
    except urllib.error.HTTPError as e:
        try:
            desc = json.loads(e.read().decode(errors='ignore')).get('description', '')
        except Exception:
            desc = ''
        hint = {401: '봇 토큰이 틀렸습니다', 400: '채팅 ID를 확인하세요 (봇에게 먼저 말을 걸어야 합니다)', 403: '봇이 차단됐거나 채팅방에 없습니다'}.get(e.code, '')
        return {'ok': False, 'error': f"{hint} ({e.code} {desc})".strip()[:200]}
    except Exception as e:
        return {'ok': False, 'error': str(e).replace(tok, '●●●')[:200]}


@app.post("/api/telegram/test")
async def api_telegram_test():
    return await asyncio.to_thread(_tg_test)


def _secret_values():
    return [v for v in (CFG.get(k) for k in ('app_key', 'app_secret', 'telegram_token', 'krx_pw', 'dart_key')) if v and len(str(v)) >= 6]


@app.get("/api/logs/tail")
async def api_logs_tail(n: int = 60):
    """자동 실행(조용한 실행) 서버 로그의 마지막 줄들 — 키·토큰 같은 비밀 값은 가림"""
    p_ = os.path.join(BASE_DIR, 'logs', 'server.log')
    if not os.path.exists(p_):
        return {'exists': False, 'path': p_, 'lines': []}
    size = os.path.getsize(p_)
    with open(p_, 'rb') as f:
        f.seek(max(0, size - 200_000))
        raw = f.read()
    lines = []
    for b in raw.split(b'\n')[-max(1, min(int(n), 300)):]:
        b = b.rstrip(b'\r')
        for enc in ('utf-8', 'cp949'):
            try:
                t = b.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        else:
            t = b.decode('utf-8', errors='replace')
        for v in _secret_values():
            t = t.replace(str(v), '●●●')
        lines.append(t[:400])
    while lines and not lines[-1].strip():
        lines.pop()
    return {'exists': True, 'path': p_, 'bytes': size, 'mtime': _mtime(p_), 'lines': lines}

@app.post("/api/backup")
async def api_backup():
    return {'ok': True, 'file': os.path.basename(db.vt_backup('manual'))}


@app.post("/api/flows")
async def api_flows():
    if not (CFG.get('krx_id') and CFG.get('krx_pw')):
        return JSONResponse({'ok': False, 'error': '설정에서 KRX 아이디·비밀번호를 먼저 저장하세요'}, 400)
    threading.Thread(target=job_flows, daemon=True).start()
    return {'ok': True}


@app.get("/api/status")
async def api_status():
    return {'job': dict(JOB), 'db': db.db_stats(), 'market': _market,
            'sizing_cfg': {k: CFG.get(k) for k in ('account_size', 'risk_pct', 'max_pos_pct')},
            'saved': _masked(),
            'data_dir': db.DATA_DIR,
            'cfg': {k: CFG.get(k) for k in ('top_n', 'auto_track', 'max_per_sector',
                                            'min_trade_value', 'min_price', 'max_price',
                                            'require_stage2', 'virtual_alerts', 'auto_register', 'legacy_features')},
            'positions': db.position_summary(),
            'legacy': bool(CFG.get('legacy_features')),
            'vt_cash': db.ACCT['cash'], 'vt_slots': db.ACCT['slots'],
            'flows': {'last': max(db.flow_dates('연기금') or {''}), 'msg': FLOW_STATE['msg'],
                      'running': FLOW_STATE['running'], 'configured': bool(CFG.get('krx_id') and CFG.get('krx_pw'))},
            'configured': bool(CFG['app_key'] and CFG['app_secret']),
            'last_scan': {k: (v['ts'] if v else None)
                          for k, v in _last_scan.items()}}


def set_balance_baseline():
    """계좌 연동 시작 시점의 보유종목을 '원래 있던 종목'으로 기록.
       첫 동기화 때 하면, 연동 직후 산 추천 종목까지 제외되는 문제가 생김."""
    acct = CFG.get('account_no')
    if not (acct and CFG['app_key'] and CFG['app_secret']):
        return None
    tok = db.get_token(CFG['app_key'], CFG['app_secret'])
    hold = ext.fetch_balance(CFG['app_key'], CFG['app_secret'], tok, acct, CFG.get('account_cd', '01'))
    open_tk = {p['ticker'] for p in db.list_positions(False)}
    pre = set(hold) - open_tk
    db.meta_set('ignore_tickers', ','.join(sorted(_ignored() | pre)))
    db.meta_set('balance_baseline', datetime.now().isoformat())
    db.meta_set('baseline_account', acct)
    if pre:
        telegram(f"🔗 KIS 잔고 연동 시작 — 기존 보유 {len(pre)}종목은 제외했습니다 "
                 f"(필요하면 내 보유에서 직접 등록)")
    return len(pre)


@app.post("/api/config")
async def api_config(req: Request):
    body = await req.json()
    new_acct = body.get('account_no')
    acct_changed = bool(new_acct) and new_acct != db.meta_get('baseline_account', '')
    for k in ('app_key', 'app_secret', 'min_trade_value', 'min_price',
              'max_price', 'top_n', 'require_stage2', 'auto_track',
              'telegram_token', 'telegram_chat', 'dart_key', 'enrich_top',
              'max_per_sector', 'account_size', 'risk_pct', 'max_pos_pct',
              'virtual_alerts', 'account_no', 'account_cd', 'auto_register', 'history_days', 'legacy_features',
              'krx_id', 'krx_pw'):
        if k in body:
            CFG[k] = body[k]
    for k, lo, hi in (('vt_cash', 1_000_000, 10_000_000_000), ('vt_slots', 2, 100)):
        if k in body:
            try:
                v = int(float(body[k]))
            except (TypeError, ValueError):
                return JSONResponse({'ok': False, 'error': f'{k} 값이 숫자가 아닙니다'}, 400)
            if not lo <= v <= hi:
                return JSONResponse({'ok': False, 'error': f'{k}는 {lo:,} ~ {hi:,} 사이여야 합니다'}, 400)
            CFG[k] = v
    if 'model_order_max' in body:                     # 모델별 1회 최대 주문금액 — 전부 확인한 뒤 한 번에 반영
        v = body['model_order_max']
        if not isinstance(v, dict):
            return JSONResponse({'ok': False, 'error': '모델별 주문금액 형식이 올바르지 않습니다'}, 400)
        new = dict(CFG.get('model_order_max') or DEFAULT_CFG['model_order_max'])
        for g, x in v.items():
            if g not in new:
                continue
            try:
                x = int(float(str(x).replace(',', '')))
            except (TypeError, ValueError):
                return JSONResponse({'ok': False, 'error': f"{VT_NAMES.get(g, g)} 주문금액이 숫자가 아닙니다"}, 400)
            if not ORDER_MIN <= x <= ORDER_MAX:
                return JSONResponse({'ok': False, 'error': f"{VT_NAMES.get(g, g)} 주문금액은 {ORDER_MIN:,} ~ {ORDER_MAX:,}원 사이여야 합니다"}, 400)
            new[g] = x
        CFG['model_order_max'] = new
    _apply_vt_account()
    save_cfg(CFG)
    if acct_changed:
        try:
            n = set_balance_baseline()
            return {'ok': True, 'baseline': n}
        except Exception as e:
            return {'ok': True, 'baseline_error': f'잔고 조회 실패: {e}'}
    return {'ok': True}


@app.post("/api/build")
async def api_build(req: Request):
    body = await req.json() if req.headers.get('content-length') else {}
    full = body.get('full', True)
    threading.Thread(target=job_build, args=(full,), daemon=True).start()
    return {'ok': True}


@app.post("/api/scan")
async def api_scan(req: Request):
    body = await req.json() if req.headers.get('content-length') else {}
    horizon = body.get('horizon', 'swing')
    if horizon not in ('swing', 'short'):
        return JSONResponse({'ok': False, 'error': '단기·스윙 전용 시스템입니다 (중장기 제외)'}, 400)
    threading.Thread(target=job_scan, args=(horizon,), daemon=True).start()
    return {'ok': True}


@app.post("/api/stop")
async def api_stop():
    _stop['flag'] = True
    return {'ok': True}


@app.get("/api/results/{horizon}")
async def api_results(horizon: str):
    if not _last_scan.get(horizon):
        saved = db.last_scan(horizon)          # 재시작·구축 중에도 마지막 스캔 결과 표시
        if saved:
            _last_scan[horizon] = saved
    return _last_scan.get(horizon) or {'results': [], 'ts': None}


@app.get("/api/detail/{ticker}")
async def api_detail(ticker: str):
    cd = db.load_candles(ticker, 250)
    if not cd:
        return JSONResponse({'error': '데이터 없음'}, 404)
    inv = db.load_investors(ticker, 20)
    sc = eng.score_stock(cd, inv, 0, None, None, _market.get('coef', 1.0))
    sts = strat.run_strategies(cd, None, None, sc['engines']['supply'], inv)
    row = db.conn().execute("SELECT name FROM stocks WHERE ticker=?",
                            (ticker,)).fetchone()
    return {'ticker': ticker, 'name': row['name'] if row else ticker,
            'score': sc, 'strategies': sts,
            'chart': [{'d': c['date'], 'o': c['open'], 'h': c['high'],
                       'l': c['low'], 'c': c['close'], 'v': c['volume']}
                      for c in cd[-120:]]}


# ════════════════════════════════════════════
#  종목 차트 · 대시보드 (v3.9)
# ════════════════════════════════════════════
_CHO = 'ㄱㄲㄴㄷㄸㄹㅁㅂㅃㅅㅆㅇㅈㅉㅊㅋㅌㅍㅎ'


def _choseong(s_):
    return ''.join(_CHO[(ord(ch) - 0xAC00) // 588] if '가' <= ch <= '힣' else ch for ch in s_)


@app.get("/api/stock/search")
async def api_stock_search(q: str = ''):
    q = q.strip()
    if not q:
        return []
    qc = q.replace(' ', '')
    # 정렬: 이름·코드가 정확히 같음 → 이름이 입력으로 시작 → 후보풀 → 시가총액 큰 순 → 짧은 이름 ('삼성' → 삼성전자가 맨 앞)
    if len(qc) >= 2 and all(ch in _CHO for ch in qc):          # 초성 검색 (ㅅㅅㅈㅈ → 삼성전자)
        rows = [dict(r) for r in db.conn().execute("SELECT ticker, name, market, in_pool, mktcap FROM stocks")]
        hit = [r for r in rows if qc in _choseong((r['name'] or '').replace(' ', ''))]
        hit.sort(key=lambda r: (not _choseong((r['name'] or '').replace(' ', '')).startswith(qc), -(r['in_pool'] or 0),
                                -(r['mktcap'] or 0), len(r['name'] or '')))
        return [{k: r[k] for k in ('ticker', 'name', 'market', 'in_pool')} for r in hit[:15]]
    rows = db.conn().execute(
        """SELECT ticker, name, market, in_pool FROM stocks
           WHERE ticker LIKE ? OR name LIKE ? OR REPLACE(name,' ','') LIKE ?
           ORDER BY (name = ? OR REPLACE(name,' ','') = ?) DESC, (ticker = ?) DESC,
                    (name LIKE ? OR REPLACE(name,' ','') LIKE ?) DESC, in_pool DESC, mktcap DESC, LENGTH(name) LIMIT 15""",
        (q + '%', '%' + q + '%', '%' + qc + '%', q, qc, q, q + '%', qc + '%')).fetchall()
    return [dict(r) for r in rows]


@app.get("/api/stock/{ticker}")
async def api_stock(ticker: str, days: int = 160):
    """차트(캔들·20일선·9일EMA·거래량) + RSI(14·2) + 외국인·연기금·기관 순매수 + 가상매매 매수/매도 지점 + 추천 이력 + 내 보유"""
    c = db.conn()
    info = c.execute("SELECT * FROM stocks WHERE ticker=?", (ticker,)).fetchone()
    cd = db.load_candles(ticker, 670)      # 화면 최대 420일 + 앞 250일: 캔들 패턴의 52주 위치 · 이동평균 · RSI를 그날 기준 그대로 계산
    if not cd:
        return JSONResponse({'error': '일봉 데이터가 없습니다 (후보풀 밖이거나 신규 상장)'}, 404)
    cl = [x['close'] for x in cd]
    k, e, ema = 0.2, cl[0], []
    for x in cl:
        e = x * k + e * (1 - k)
        ema.append(e)

    def rsi_series(n):
        out, ag, al = [None] * len(cl), None, None
        for i in range(1, len(cl)):
            d_ = cl[i] - cl[i - 1]
            u, w = max(d_, 0), max(-d_, 0)
            if i <= n:
                ag = (ag or 0) + u / n
                al = (al or 0) + w / n
                if i == n:
                    out[i] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
            else:
                ag = (ag * (n - 1) + u) / n
                al = (al * (n - 1) + w) / n
                out[i] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
        return out
    r14, r2 = rsi_series(14), rsi_series(2)
    # 저변동고점(차트 모델) 재료 — 봉마다 그날 기준: 52주(250거래일) 최고가 · ATR14 ÷ 종가 · 과열 지수(25일)
    trs = [None] + [max(cd[i]['high'] - cd[i]['low'], abs(cd[i]['high'] - cl[i - 1]), abs(cd[i]['low'] - cl[i - 1]))
                    for i in range(1, len(cd))]
    hq = [0] + [((1 if cl[i] > cl[i - 1] else -1 if cl[i] < cl[i - 1] else 0) if cd[i]['volume'] > cd[i - 1]['volume'] else 0)
                for i in range(1, len(cd))]
    start = max(0, len(cd) - days)
    dates = [x['date'] for x in cd[start:]]
    flows = {}
    if dates:
        for d_, inv, amt in c.execute(
                "SELECT date, investor, amt FROM flows WHERE ticker=? AND date>=? AND investor IN ('외국인','연기금','기관합계')",
                (ticker, dates[0])):
            flows.setdefault(d_, {})[inv] = amt
    bars = []
    for i in range(start, len(cd)):
        x = cd[i]
        f = flows.get(x['date'], {})
        try:
            psc, pat = eng.candle_reversal_score(cd[:i + 1]) if i >= 29 else (None, None)
        except Exception:
            psc, pat = None, None
        bars.append({'d': x['date'], 'o': x['open'], 'h': x['high'], 'l': x['low'], 'c': x['close'], 'v': x['volume'],
                     'ma20': sum(cl[max(0, i - 19):i + 1]) / min(20, i + 1) if i >= 19 else None,
                     'ema9': ema[i], 'rsi14': r14[i], 'rsi2': r2[i],
                     'frgn': f.get('외국인'), 'pens': f.get('연기금'), 'inst': f.get('기관합계'),
                     'pat': pat, 'pat_sc': round(psc, 2) if psc is not None else None,
                     'hi250': max(y['high'] for y in cd[max(0, i - 249):i + 1]),
                     'atrp': round(sum(trs[i - 13:i + 1]) / 14 / x['close'] * 100, 2) if i >= 14 and x['close'] else None,
                     'heat': sum(hq[i - 24:i + 1]) if i >= 25 else None})
    vt = [dict(r) for r in c.execute("SELECT * FROM vtrades WHERE ticker=? ORDER BY signal_date DESC", (ticker,))]
    scans = [dict(r) for r in c.execute(
        "SELECT ts, horizon, score, strategy, entry, stop, target1, target2 FROM scans WHERE ticker=? ORDER BY ts DESC LIMIT 30",
        (ticker,))]
    pos = [dict(r) for r in c.execute("SELECT * FROM positions WHERE ticker=? ORDER BY id DESC", (ticker,))]
    val20 = sum(x['close'] * x['volume'] for x in cd[-20:]) / min(20, len(cd))
    d20 = [x['date'] for x in cd[-20:]]

    def strength(inv):
        v = [flows.get(d, {}).get(inv) for d in d20]
        v = [x for x in v if x is not None]
        return (sum(v) / (val20 * len(v))) if (v and val20) else None
    hi, lo = max(x['high'] for x in cd[-250:]), min(x['low'] for x in cd[-250:])
    summary = {'close': cl[-1], 'chg': (cl[-1] / cl[-2] - 1) * 100 if len(cl) > 1 else 0,
               'ret5': (cl[-1] / cl[-6] - 1) * 100 if len(cl) > 5 else None,
               'ret20': (cl[-1] / cl[-21] - 1) * 100 if len(cl) > 20 else None,
               'rsi14': r14[-1], 'rsi2': r2[-1], 'ema9': ema[-1],
               'pos52': (cl[-1] - lo) / (hi - lo) * 100 if hi > lo else None, 'fromhi': (cl[-1] / hi - 1) * 100 if hi else None,
               'val20': val20, 'frgn20': strength('외국인'), 'pens20': strength('연기금')}
    return {'ticker': ticker, 'info': dict(info) if info else {'ticker': ticker, 'name': ticker},
            'bars': bars, 'summary': summary, 'vtrades': vt, 'scans': scans, 'positions': pos,
            'names': VT_NAMES}



# ════════════════════════════════════════════
#  종목 분석 — 9개 모델이 이 종목을 지금 어떻게 보는지 (가상매매 기록과 같은 계산 · 화면 표시 전용)
# ════════════════════════════════════════════
_MV = {'key': None, 'data': None, 'at': None, 'sec': None, 'chk': 0}
_MV_lock = threading.Lock()
_RD = {'at': 0, 'd': []}


def _recent_dates_cached():
    """최근 70거래일 (60초 캐시) — 종목 분석을 여러 번 열어도 전체 일봉 스캔(수십만 행)을 반복하지 않게"""
    if time.time() - _RD['at'] > 60 or not _RD['d']:
        _RD.update(at=time.time(), d=db.recent_trading_dates(70))
    return _RD['d']


def _pct_map(vals):
    """백분위 (0~1) — 가상매매 기록의 prank_map과 같은 방식 (값 없으면 0.5)"""
    have = sorted((v, t) for t, v in vals.items() if v is not None)
    n = len(have)
    out = {t: 0.5 for t in vals}
    for k, (v, t) in enumerate(have):
        out[t] = k / (n - 1) if n > 1 else 0.5
    return out


def _rank_of(scores):
    items = sorted(((v, t) for t, v in scores.items() if v is not None), key=lambda x: -x[0])
    n = len(items)
    return {t: (i + 1, n) for i, (v, t) in enumerate(items)}


def _model_view_all():
    """후보풀 전체의 모델별 점수·순위 (18:20 가상매매 기록과 같은 계산) — 마지막 거래일·수급일이 바뀔 때만 다시 계산"""
    if _MV['data'] is not None and time.time() - _MV['chk'] < 60:
        return _MV['data']
    ds = _recent_dates_cached()
    last = ds[-1] if ds else ''
    key = (last, db.conn().execute("SELECT MAX(date) FROM flows").fetchone()[0])
    if _MV['key'] == key and _MV['data'] is not None:
        _MV['chk'] = time.time()
        return _MV['data']
    with _MV_lock:
        if _MV['key'] == key and _MV['data'] is not None:
            return _MV['data']
        t0 = time.time()
        pool = _load_pool_candles()
        ok = {t: cd for t, cd in pool.items() if not eng.risk_filter(cd)}
        risk = {t: eng.risk_filter(cd) for t, cd in pool.items() if t not in ok}
        cand = {}
        for t, cd in ok.items():
            sc, pat = eng.candle_reversal_score(cd)
            if sc is not None:
                cand[t] = (sc, pat)
        rsi2 = {t: eng.rsi([c['close'] for c in cd], 2) for t, cd in ok.items()}
        fr = _foreign_strength(ok)
        r_fr = _pct_map(fr)
        drop5 = {t: (-(cd[-1]['close'] / cd[-6]['close'] - 1) if len(cd) > 6 and cd[-6]['close'] > 0 else None)
                 for t, cd in ok.items()}
        lowv = {}
        for t, cd in ok.items():
            a = eng.atr(cd, 14)
            lowv[t] = (-a / cd[-1]['close']) if a and cd[-1]['close'] > 0 else None
        r_drop, r_lowv = _pct_map(drop5), _pct_map(lowv)
        lvh_f = {t: eng.lvhigh_features(cd) for t, cd in ok.items()}          # 저변동고점 (차트 모델)
        lvh_s, (lvh_lv, lvh_hi, lvh_ht) = eng.lvhigh_scores(lvh_f)
        lvh_c = {t: {'lv': lvh_lv.get(t), 'hi': lvh_hi.get(t), 'ht': lvh_ht.get(t), **(lvh_f.get(t) or {})} for t in lvh_s}
        jg_f = {t: eng.jongga_features(cd) for t, cd in ok.items()}              # 종가베팅 (v5.6)
        jg_s = eng.jongga_scores(jg_f)
        d20 = ds[-20:]
        rev = eng.reversal_flow_table(pool, {t: db.load_investors(t, 20) for t in pool},
                                      {'frgn': db.flow_sums(d20, '외국인'), 'pens': db.flow_sums(d20, '연기금')})
        fin, fin_c = {}, {}
        for t in ok:
            v = rev.get(t)
            if not v or drop5.get(t) is None:
                continue
            pr, pf, pi = v.get('p_rev', v['oversold'] / 100), v.get('p_fr', v['foreign'] / 100), v.get('p_in', v['inst_contra'] / 100)
            fin[t] = ((pr + r_drop[t]) / 2 + pf + pi) / 3
            fin_c[t] = {'p_rev': pr, 'drop': r_drop[t], 'p_fr': pf, 'p_in': pi, 'src': v.get('contra_src') or '연기금'}
        v62 = {}
        try:
            dates60 = ds
            allc = db.candles_since(dates60[0]) if dates60 else {}
            ffl, ifl = db.flow_rows(d20, '외국인'), db.flow_rows(d20, '기관합계')
            feats = {}
            for t, cd in allc.items():
                if not cd or cd[-1]['date'] != last:
                    continue
                ds = [x['date'] for x in cd[-20:]]
                feats[t] = eng.v62_features(cd, [ffl.get(t, {}).get(d) for d in ds], [ifl.get(t, {}).get(d) for d in ds])
            vs = eng.v62_scores(feats)
            v62 = {t: vs[t] for t in ok if t in vs and feats.get(t) and feats[t].get('ma60') and feats[t]['close'] >= 1000}
        except Exception as e:
            print(f'[분석] V6.2 계산 실패: {e}', flush=True)
        data = {'date': last, 'n_pool': len(pool), 'n_ok': len(ok), 'risk': risk, 'cand': cand, 'rsi2': rsi2,
                'fr': fr, 'r_fr': r_fr, 'r_drop': r_drop, 'r_lowv': r_lowv, 'drop5': drop5,
                'fdip': {t: (r_drop[t] + r_fr[t]) / 2 for t in ok if drop5[t] is not None},
                'lvflow': {t: (r_lowv[t] + r_fr[t]) / 2 for t in ok if lowv[t] is not None},
                'final': fin, 'final_c': fin_c, 'v62': v62, 'lvhigh': lvh_s, 'lvhigh_c': lvh_c,
                'jongga': jg_s, 'jongga_f': jg_f}
        for k in ('fdip', 'lvflow', 'lvhigh', 'final', 'v62', 'jongga'):
            data['rank_' + k] = _rank_of(data[k])
        data['rank_candle'] = _rank_of({t: v[0] for t, v in cand.items()})
        data['rank_rsi'] = _rank_of({t: -v for t, v in rsi2.items() if v is not None and v <= 5})
        _MV.update(key=key, data=data, at=datetime.now().strftime('%H:%M:%S'), sec=round(time.time() - t0, 1), chk=time.time())
        return data


def _stock_analysis(tk):
    c = db.conn()
    info = c.execute("SELECT ticker, name, market, in_pool FROM stocks WHERE ticker=?", (tk,)).fetchone()
    cd = db.load_candles(tk, 260)
    if not cd or len(cd) < 30:
        return {'error': '일봉 데이터가 부족해 분석할 수 없습니다'}
    cl = [x['close'] for x in cd]
    ema = db._ema9_upto(cl)
    ma20 = sum(cl[-20:]) / min(20, len(cl))
    ma20_5 = sum(cl[-25:-5]) / 20 if len(cl) >= 25 else None
    r14, r2 = eng.rsi(cl, 14), eng.rsi(cl, 2)
    a14 = eng.atr(cd, 14)
    win = cd[-250:]
    hi, lo = max(x['high'] for x in win), min(x['low'] for x in win)
    val20 = sum(x['close'] * x['volume'] for x in cd[-20:]) / min(20, len(cd))
    vol20 = sum(x['volume'] for x in cd[-21:-1]) / max(1, min(20, len(cd) - 1))
    lvf = eng.lvhigh_features(cd) or {}
    pats = []
    for i in range(max(29, len(cd) - 10), len(cd)):
        sc, pat = eng.candle_reversal_score(cd[:i + 1])
        if pat:
            pats.append({'d': cd[i]['date'], 'pat': pat, 'score': round(sc, 2)})
    d20 = _recent_dates_cached()[-20:]
    fl = {}
    if d20:
        q = ','.join('?' * len(d20))
        for inv, s_, n_ in c.execute(f"SELECT investor, SUM(amt), COUNT(*) FROM flows WHERE ticker=? AND date IN ({q}) "
                                     f"AND investor IN ('외국인','기관합계','연기금') GROUP BY investor", (tk, *d20)):
            fl[inv] = {'sum': s_, 'days': n_, 'strength': (s_ / (val20 * n_)) if val20 and n_ else None}
    tech = {'date': cd[-1]['date'], 'close': cl[-1], 'chg': (cl[-1] / cl[-2] - 1) * 100 if len(cl) > 1 else 0,
            'ret5': (cl[-1] / cl[-6] - 1) * 100 if len(cl) > 5 else None, 'ret20': (cl[-1] / cl[-21] - 1) * 100 if len(cl) > 20 else None,
            'ema9': ema[-1], 'vs_ema9': (cl[-1] / ema[-1] - 1) * 100 if ema[-1] else None,
            'ma20': ma20, 'vs_ma20': (cl[-1] / ma20 - 1) * 100 if ma20 else None,
            'ma20_slope': (ma20 / ma20_5 - 1) * 100 if ma20_5 else None,
            'rsi14': r14, 'rsi2': r2, 'atr_pct': (a14 / cl[-1] * 100) if a14 and cl[-1] else None,
            'pos52': (cl[-1] - lo) / (hi - lo) * 100 if hi > lo else None, 'hi52': hi, 'lo52': lo,
            'val20': val20, 'vol_ratio': (cd[-1]['volume'] / vol20) if vol20 else None,
            'fromhi': (lvf['fromhi'] * 100) if lvf.get('fromhi') is not None else None, 'heat': lvf.get('heat'),
            'patterns': pats, 'flows': fl}
    # 이 종목의 가상 포지션 (모델별) — 대기(다음 거래일 시가 매수) · 보유(평가손익 · 오늘 종가로 매도 조건 충족 여부)
    opens = {}
    rows_o = [dict(r) for r in c.execute(
        "SELECT grp, status, signal_date, entry_date, entry_price, last_close, held FROM vtrades "
        "WHERE ticker=? AND status!='청산' AND grp!='rsi14_old' ORDER BY signal_date", (tk,))]
    if rows_o:
        full = [dict(r) for r in c.execute("SELECT date, close FROM candles WHERE ticker=? ORDER BY date", (tk,))]
        dates_f, closes_f = [x['date'] for x in full], [x['close'] for x in full]
        ema_f = db._ema9_upto(closes_f) if closes_f else []
        for o in rows_o:
            ep = o.get('entry_price')
            o['upnl'] = round((o['last_close'] / ep - 1) * 100 - db.VT_COST, 2) if ep and o.get('last_close') else None
            o['sell_next'] = None
            if o['status'] == '보유' and ep and o['grp'] not in db.V62_GROUPS and o['entry_date'] in dates_f:
                try:           # 대시보드 · 가상매매와 같은 규칙 함수
                    w = db._rule_walk(dates_f, closes_f, ema_f, dates_f.index(o['entry_date']), ep, db.rule_of(o['grp']))
                    if w and w[0] is None:
                        o['sell_next'] = w[1]
                except Exception:
                    pass
            opens[o['grp']] = o
    # 모델별 — 후보풀 종목만 (가상매매 기록 대상)
    in_pool = bool(info and info['in_pool'])
    models, meta = {}, {}
    if in_pool:
        mv = _model_view_all()
        meta = {'date': mv['date'], 'n_pool': mv['n_pool'], 'n_ok': mv['n_ok'], 'computed_at': _MV['at'], 'sec': _MV['sec'],
                'batch': db.meta_get('vt_last_batch', '')}
        picked = {g for g, o in opens.items() if o['status'] == '대기'}
        held = {g for g, o in opens.items() if o['status'] == '보유'}
        risk = mv['risk'].get(tk)

        def rk(key):
            r_ = mv['rank_' + key].get(tk)
            return {'rank': r_[0], 'n': r_[1]} if r_ else {'rank': None, 'n': len(mv['rank_' + key])}
        base = lambda g: {'picked': g in picked, 'held': g in held}
        fc = mv['final_c'].get(tk) or {}
        models['final'] = {**base('final'), **rk('final'), 'score': mv['final'].get(tk),
                           'parts': [['반전(과매도)', fc.get('p_rev')], ['5일 하락', fc.get('drop')], ['외국인 매수', fc.get('p_fr')],
                                     [f"{fc.get('src') or '연기금'} 역방향", fc.get('p_in')]] if fc else []}
        models['fdip'] = {**base('fdip'), **rk('fdip'), 'score': mv['fdip'].get(tk),
                          'parts': [['5일 하락', mv['r_drop'].get(tk)], ['외국인 매수', mv['r_fr'].get(tk)]]}
        models['lvflow'] = {**base('lvflow'), **rk('lvflow'), 'score': mv['lvflow'].get(tk),
                            'parts': [['낮은 변동성', mv['r_lowv'].get(tk)], ['외국인 매수', mv['r_fr'].get(tk)]]}
        lc = (mv.get('lvhigh_c') or {}).get(tk) or {}
        models['lvhigh'] = {**base('lvhigh'), **rk('lvhigh'), 'score': (mv.get('lvhigh') or {}).get(tk),
                            'parts': [['낮은 변동성', lc.get('lv')], ['52주 고점 근접', lc.get('hi')], ['과열 없음', lc.get('ht')]] if lc else [],
                            'raw': {k: lc.get(k) for k in ('atrp', 'fromhi', 'heat')} if lc else None}
        rv = mv['rsi2'].get(tk)
        models['rsi'] = {**base('rsi'), **rk('rsi'), 'value': rv, 'cond': rv is not None and rv <= 5}
        jf = (mv.get('jongga_f') or {}).get(tk)
        models['jongga'] = {**base('jongga'), **rk('jongga'), 'cond': bool(jf and jf['ok']), 'score': (mv.get('jongga') or {}).get(tk),
                            'checks': [[lb, bool(jf['checks'][k])] for k, lb in eng.JONGGA_LABELS] if jf else [],
                            'raw': {k: jf.get(k) for k in ('chg', 'clv', 'near', 'spike')} if jf else None}
        cp = mv['cand'].get(tk)
        models['candle'] = {**base('candle'), **rk('candle'), 'pattern': cp[1] if cp else None, 'score': cp[0] if cp else None}
        models['v62'] = {**base('v62'), **rk('v62'), 'score': mv['v62'].get(tk)}
        scan = _last_scan.get('swing') or db.last_scan('swing') or {}
        res = sorted(scan.get('results') or [], key=lambda x: -(x.get('score') or 0))
        pos_ = next((i for i, r_ in enumerate(res) if r_.get('ticker') == tk), None)
        models['strategy'] = {**base('strategy'), 'rank': pos_ + 1 if pos_ is not None else None, 'n': len(res),
                              'score': res[pos_]['score'] if pos_ is not None else None,
                              'why': res[pos_].get('strategy') if pos_ is not None else None, 'scan_ts': scan.get('ts')}
        meta['risk'] = risk
    hist = {g: {'n': n, 'win': (w or 0) / n if n else None, 'avg': a} for g, n, w, a in c.execute(
        "SELECT grp, COUNT(*), SUM(ret>0), AVG(ret) FROM vtrades WHERE ticker=? AND status='청산' AND grp!='rsi14_old' GROUP BY grp", (tk,))}
    return {'ticker': tk, 'name': info['name'] if info else tk, 'in_pool': in_pool, 'tech': tech, 'models': models,
            'meta': meta, 'hist': hist, 'opens': opens, 'names': VT_NAMES, 'order': MODEL_ORDER,
            'desc': {g: VT_DESC.get(g, ('', None))[0] for g in MODEL_ORDER},
            'rules': {g: (VT_DESC.get(g, ('', None))[1] or db.rule_text(db.rule_of(g))) for g in MODEL_ORDER}}


@app.get("/api/stock/{ticker}/analysis")
async def api_stock_analysis(ticker: str):
    try:
        return await asyncio.to_thread(_stock_analysis, ticker)
    except Exception as e:
        traceback.print_exc()
        return JSONResponse({'error': f'분석 실패: {e}'[:200]}, 200)


@app.get("/api/dashboard")
async def api_dashboard():
    c = db.conn()
    last = db.meta_get('vt_last_batch', '')
    today_picks = {}
    if last:
        for r in c.execute("SELECT grp, rank, ticker, name, rsi FROM vtrades WHERE signal_date=? "
                           "AND grp NOT IN ('control','control_v62','control_jongga','rsi14_old') ORDER BY grp, rank", (last,)):
            today_picks.setdefault(r['grp'], []).append(dict(r))
    recent = [dict(r) for r in c.execute(
        "SELECT grp, name, ticker, exit_date, ret, exit_reason, held FROM vtrades WHERE status='청산' "
        "AND grp NOT IN ('control','control_v62','control_jongga','rsi14_old') ORDER BY exit_date DESC, ret DESC LIMIT 10")]
    holding = c.execute("SELECT COUNT(*) FROM vtrades WHERE status='보유' AND grp NOT IN ('control','control_v62','control_jongga')").fetchone()[0]
    judge, _ = db.vt_judge(VT_EXPECT_SIGN, VT_EXPECT_NUM)
    now = datetime.now().strftime('%H:%M')
    job_lbl = {'scan': '스캔', 'flows': '수급 수집', 'orders': '주문표', 'vtreport': '(금) 주간보고',
               'at_morning': '자동매매 주문', 'at_close': '자동매매 정리'}      # (파이썬 3.10~3.11에서도 읽히는 문법)
    nxt = [f"{t} {job_lbl.get(k[0], k[0])}" + (' ' + k[1] if k[0] == 'scan' and k[1] else '')
           for t, k in sorted(_active_schedule().items()) if t > now][:4]
    fdays = c.execute("SELECT COUNT(DISTINCT signal_date) FROM vtrades WHERE grp='final'").fetchone()[0]
    fin = today_picks.get('final', [])
    for p_ in fin:
        r_ = c.execute("SELECT signal_close FROM vtrades WHERE grp='final' AND signal_date=? AND ticker=?",
                       (last, p_['ticker'])).fetchone()
        p_['signal_close'] = r_[0] if r_ else None
    try:
        hold_days = len(json.loads(db.meta_get('dq_hold_dates', '[]') or '[]'))
    except Exception:
        hold_days = 0
    accs = {g: db.vt_account(g) for g in ('final', 'control')}
    bd = db.bench_curve((accs['final']['stats'] or {}).get('start'))    # 같은 날 같은 금액으로 지수 ETF 그냥 보유
    hold_by = {g: n for g, n in c.execute("SELECT grp, COUNT(*) FROM vtrades WHERE status='보유' AND grp!='rsi14_old' GROUP BY grp")}
    return {'last_candle': (db.recent_trading_dates(1) or [''])[-1], 'forward_days': fdays,
            'curves': {g: [[p_['date'], p_['equity']] for p_ in accs[g]['curve']] for g in accs},
            'holding_final': _holdings_with_plan('final'), 'hold_by': hold_by,
            'rule_final': db.rule_text(db.rule_of('final')),
            'regime': _market.get('regime', '미판정'), 'coef': _market.get('coef', 1.0),
            'at_mode': CFG.get('at_mode', 'OFF'), 'gate': at.gate('final', judge), 'dq_hold_days': hold_days,
            'weight_pct': round(100 / db.ACCT['slots'], 1), 'order_cap_final': db.order_cap('final'),
            'month_final': _model_month('final', accs['final']['curve']),
            'bench': bd['stats'], 'bench_curve': [[p_['date'], p_['equity']] for p_ in bd['curve']],
            'last_batch': last, 'picks': today_picks, 'recent': recent, 'holding': holding,
            'stats': db.vt_stats(), 'judge': judge, 'account': {g: accs[g]['stats'] for g in accs},
            'quality': db.last_quality(), 'next': nxt, 'names': VT_NAMES}


def _holdings_with_plan(grp):
    """가상 보유 종목 · 평가손익 · 오늘 종가 기준 '다음 거래일 시가 매도' 예정 여부 (같은 규칙 함수 사용)"""
    c = db.conn()
    rows = [dict(r) for r in c.execute(
        "SELECT id, ticker, name, rank, signal_date, entry_date, entry_price, last_close, last_date, held FROM vtrades "
        "WHERE grp=? AND status='보유' ORDER BY entry_date DESC, rank", (grp,))]
    rule = db.rule_of(grp)
    for h in rows:
        ep = h.get('entry_price')
        h['upnl'] = round((h['last_close'] / ep - 1) * 100 - db.VT_COST, 2) if ep and h.get('last_close') else None
        h['sell_next'] = None
        if grp in db.V62_GROUPS or not ep:
            continue
        try:
            cd = [dict(r) for r in c.execute("SELECT date, close FROM candles WHERE ticker=? ORDER BY date", (h['ticker'],))]
            dates = [x['date'] for x in cd]
            closes = [x['close'] for x in cd]
            if h['entry_date'] in dates:
                w = db._rule_walk(dates, closes, db._ema9_upto(closes), dates.index(h['entry_date']), ep, rule)
                if w and w[0] is None:          # 오늘 종가로 조건 충족 → 다음 거래일 시가 매도
                    h['sell_next'] = w[1]
        except Exception:
            pass
    return rows


@app.get("/api/performance")
async def api_performance():
    db.track_outcomes()
    return {'strategies': db.strategy_performance(180)}


@app.get("/api/history")
async def api_history(limit: int = 50):
    rows = db.conn().execute("""
        SELECT s.id,s.ts,s.horizon,s.ticker,s.name,s.score,s.strategy,
               s.entry,o.d1,o.d3,o.d5,o.d20
        FROM scans s LEFT JOIN outcomes o ON o.scan_id=s.id
        ORDER BY s.id DESC LIMIT ?""", (limit,)).fetchall()
    return {'rows': [dict(r) for r in rows]}


@app.get("/api/tracking")
async def api_tracking():
    return {'rows': db.list_tracking(150), 'perf': db.tracking_performance()}


@app.post("/api/track")
async def api_track(req: Request):
    """스캔 결과 카드에서 수동으로 추적 추가"""
    body = await req.json()
    h, tk = body.get('horizon', 'swing'), body.get('ticker')
    scan = _last_scan.get(h) or {}
    item = next((r for r in scan.get('results', []) if r['ticker'] == tk), None)
    if not item:
        return JSONResponse({'ok': False, 'error': '스캔 결과에 없는 종목'}, 400)
    tid = db.add_tracking(item)
    if not tid:
        return {'ok': False, 'error': '이미 추적 중'}
    item['tracked'] = True
    return {'ok': True, 'id': tid}


@app.post("/api/track/remove/{tid}")
async def api_track_remove(tid: int):
    db.update_tracking(tid, status='수동해제', closed=1)
    return {'ok': True}


@app.post("/api/track/check")
async def api_track_check():
    threading.Thread(target=job_track, daemon=True).start()
    return {'ok': True}


# ════════════════════════════════════════════
#  보유 등록 · 주문표 · 증권앱 연동
# ════════════════════════════════════════════
def _find_plan(tk):
    """최근 추천(메모리 → DB 5일)에서 이 종목의 매매계획 찾기"""
    for h in ('short', 'swing', 'long'):
        sc = _last_scan.get(h) or {}
        for r in sc.get('results', []):
            if r['ticker'] == tk:
                return r
    since = (datetime.now() - timedelta(days=5)).isoformat()
    row = db.conn().execute("SELECT payload FROM scans WHERE ticker=? AND ts>? "
                            "ORDER BY id DESC LIMIT 1", (tk, since)).fetchone()
    if row:
        try:
            return json.loads(row['payload'])
        except Exception:
            pass
    return None


def create_position(tk, bp, qty, stop=None, t1=None, t2=None, item=None, memo='',
                    head='📥 보유 등록'):
    """매수 기록 생성. 추천 계획이 있으면 실제 평단 기준으로 목표 재계산.
       반환: (position, error)"""
    row = db.conn().execute("SELECT name FROM stocks WHERE ticker=?", (tk,)).fetchone()
    name = (item or {}).get('name') or (row['name'] if row else tk)
    if item and item.get('plan'):
        p = item['plan']
        stop = float(stop or p['stop'])
        r0 = p['entry'] - p['stop']
        m1 = (p['target1'] - p['entry']) / r0 if r0 > 0 else 2
        m2 = (p['target2'] - p['entry']) / r0 if r0 > 0 else 3
        strategy, horizon, tstop = item['strategy'], item.get('horizon', ''), p['time_stop']
    else:
        cd = db.load_candles(tk, 30)
        a = eng.atr(cd, 14) if len(cd) > 15 else None
        stop = float(stop or max(bp - (a or bp * 0.03) * 2, bp * 0.90))
        m1, m2 = 2.0, 3.0
        strategy, horizon, tstop = '직접 등록', '', 0
    if stop >= bp:
        return None, (f'손절가({stop:,.0f})는 매수가({bp:,.0f})보다 낮아야 합니다')
    # 같은 종목이 이미 보유 중이면 새로 만들지 않고 추가매수로 병합
    cur = next((x for x in db.list_positions(False) if x['ticker'] == tk), None)
    if cur:
        nq = cur['remain_qty'] + qty
        avg = (cur['buy_price'] * cur['remain_qty'] + bp * qty) / nq
        db.update_position(cur['id'], qty=cur['qty'] + qty, remain_qty=nq, buy_price=round(avg, 2))
        pos = db.get_position(cur['id'])
        tg_trade(order_sheet_text(pos, head=f'➕ 추가매수 병합 (평단 {avg:,.0f})'))
        return pos, None
    R = bp - stop
    stop = strat.round_tick(stop, 'up')
    t1 = strat.round_tick(float(t1 or bp + R * m1), 'down')
    t2 = strat.round_tick(float(t2 or bp + R * m2), 'down')
    pid = db.add_position(tk, name, bp, qty, stop, t1, t2, strategy, horizon, tstop, memo)
    pos = db.get_position(pid)
    tg_trade(order_sheet_text(pos, head=head))
    return pos, None


def order_sheet(p):
    """증권앱에 걸어둘 주문 — 1차 익절 · 2차 익절 · 손절"""
    q = p['remain_qty']
    first = p['status'] == '보유'
    q1 = q // 2 if first and q >= 2 else 0
    rows = []
    if q1:
        rows.append({'kind': '1차 익절', 'type': '지정가 매도', 'price': int(p['target1']), 'qty': q1})
    rows.append({'kind': '2차 익절', 'type': '지정가 매도', 'price': int(p['target2']), 'qty': q - q1})
    be = p['cur_stop'] >= p['buy_price']
    rows.append({'kind': '본전 손절' if be else '손절', 'type': '스탑로스(손실제한) · 시장가',
                 'price': int(p['cur_stop']), 'qty': q})
    return rows


def order_sheet_text(p, head='📋 주문표'):
    lines = [f"{head} — {p['name']}({p['ticker']})",
             f"평단 {p['buy_price']:,.0f} · {p['remain_qty']}주 · {p['strategy']}"]
    for r in order_sheet(p):
        lines.append(f"· {r['kind']}: {r['price']:,}원 × {r['qty']}주 ({r['type']})")
    return '\n'.join(lines)


def _ignored():
    return set(filter(None, (db.meta_get('ignore_tickers', '') or '').split(',')))


def sync_balance():
    """KIS 잔고와 내 보유 대조 → 앱에서 체결된 매도·매수 자동 반영"""
    acct = CFG.get('account_no')
    if not (acct and CFG['app_key'] and CFG['app_secret']):
        return []
    tok = db.get_token(CFG['app_key'], CFG['app_secret'])
    hold = ext.fetch_balance(CFG['app_key'], CFG['app_secret'], tok, acct, CFG.get('account_cd', '01'))
    try:
        fills = ext.fetch_today_fills(CFG['app_key'], CFG['app_secret'], tok, acct,
                                      CFG.get('account_cd', '01'))
    except Exception:
        fills = {}
    msgs = []

    # 기준선이 없으면(설정 파일 직접 수정 등) 이번 조회를 기준선으로 — 신규 등록은 다음 조회부터
    if db.meta_get('baseline_account', '') != acct:
        set_balance_baseline()
        return msgs

    open_pos = {p['ticker']: p for p in db.list_positions(False)}
    for tk, p in open_pos.items():
        kq = hold.get(tk, {}).get('qty', 0)
        if not p.get('linked'):
            # 직접 등록한 종목: KIS 잔고에서 처음 확인될 때 연동 시작. 없으면 다른 증권사 종목 → 건드리지 않음
            if kq > 0:
                db.update_position(p['id'], linked=1)
                if kq != p['remain_qty']:
                    db.update_position(p['id'], qty=p['qty'] - p['remain_qty'] + kq, remain_qty=kq,
                                       buy_price=hold[tk]['avg'] or p['buy_price'])
                    msgs.append(f"🔄 {p['name']} KIS 잔고와 수량 맞춤 {p['remain_qty']} → {kq}주")
            continue
        if kq < p['remain_qty']:                        # 앱에서 매도 체결
            sold = p['remain_qty'] - kq
            f = fills.get(tk, {}).get('sell')
            price = None
            if f and f[1] > 0:
                # 체결조회는 당일 누적 평균 → 이미 기록한 오늘 매도분을 빼서 이번 체결가만 산출
                today = datetime.now().strftime('%Y-%m-%d')
                done = [x for x in json.loads(p['sells'] or '[]') if x['date'] == today]
                dq = sum(x['qty'] for x in done)
                da = sum(x['qty'] * x['price'] for x in done)
                nq, na = f[0] - dq, f[0] * f[1] - da
                if nq > 0 and na > 0:
                    price, src = na / nq, '체결가'
            if price is None:
                price, src = (p['last_price'] or p['buy_price']), '추정가'
            np_ = db.sell_position(p['id'], round(price), sold)
            pct = (price - p['buy_price']) / p['buy_price'] * 100
            m = (f"📤 {p['name']} {sold}주 매도 체결 확인 — {price:,.0f}원({src}) {pct:+.1f}%")
            if np_ and not np_['closed']:
                m += (f"\n→ 잔량 {np_['remain_qty']}주 · 앱의 손절 주문을 본전 "
                      f"{np_['cur_stop']:,.0f}원, 수량 {np_['remain_qty']}주로 수정하세요")
            elif np_:
                m += f"\n→ 청산 완료 · 최종 {np_['result_pct']:+.2f}%"
            msgs.append(m)
        elif kq > p['remain_qty']:                      # 추가 매수
            add = kq - p['remain_qty']
            avg = hold[tk]['avg'] or p['buy_price']
            db.update_position(p['id'], qty=p['qty'] + add, remain_qty=kq, buy_price=avg)
            msgs.append(f"➕ {p['name']} {add}주 추가매수 감지 — 평단 {avg:,.0f}원으로 갱신")

    if CFG.get('auto_register', True):
        ign = _ignored() | at.owned_tickers()          # 자동매매 종목은 수동 기록에 섞지 않음
        for tk, h in hold.items():
            if tk in open_pos or tk in ign:
                continue
            item = _find_plan(tk)
            src = f"추천 [{item['strategy']}] 계획" if item else '추천 이력 없음 · ATR 손절'
            # 등록 알림 1건에 주문표까지 포함 (중복 발송 방지)
            pos, err = create_position(tk, h['avg'], h['qty'], item=item, memo='앱 매수 자동등록',
                                       head=f'📥 앱 매수 감지 → 자동 등록 ({src})')
            if pos:
                db.update_position(pos['id'], linked=1)
                push('alert', {'msg': f"📥 {pos['name']} 자동 등록"})
            elif err:
                msgs.append(f"⚠️ {h['name']} 매수 감지 — 자동 등록 실패: {err}")
    return msgs


_flow_lock = threading.Lock()
FLOW_STATE = {'last': '', 'msg': '', 'running': False}


@logged_job
def job_flows(days=25):
    """KRX 전종목 외국인·기관·연기금 순매수 — 최근 N거래일 중 빠진 날짜만 (처음 25일, 이후 하루 3건)"""
    if not (CFG.get('krx_id') and CFG.get('krx_pw')):
        return
    if not _flow_lock.acquire(blocking=False):
        return
    try:
        FLOW_STATE.update(running=True, msg='수급 수집 중')
        dates = db.recent_trading_dates(days)
        today = datetime.now().strftime('%Y%m%d')
        if datetime.now().strftime('%H:%M') < '18:00':
            dates = [d for d in dates if d != today]       # 당일 수급은 18시 이후 확정치 사용
        got, err = ext.collect_krx_flows(CFG['krx_id'], CFG['krx_pw'], dates)
        last = max(db.flow_dates('연기금') or {''})
        FLOW_STATE.update(last=last, msg=err or f'완료 · 신규 {got}건 · 최근 {last}')
        print(f"[{datetime.now():%H:%M:%S}] 수급 수집: {FLOW_STATE['msg']}", flush=True)
        if err:
            telegram(f"⚠️ 연기금·외국인 수급 수집 실패: {err}")
    except Exception as e:
        FLOW_STATE['msg'] = f'오류: {str(e)[:80]}'
        print(f"[FLOW] {e}", flush=True)
    finally:
        FLOW_STATE['running'] = False
        _flow_lock.release()


VT_NAMES = {'strategy': '종합', 'candle': '캔들', 'rsi': 'RSI',
            'fdip': '외인저가', 'lvflow': '저변동외인', 'lvhigh': '저변동고점', 'jongga': '종가베팅', 'final': '최종', 'v62': 'V6.2',
            'control': '대조군', 'control_v62': '대조군(V6.2규칙)', 'control_jongga': '대조군(종가베팅규칙)'}
# 전종목 백테스트에서 대조군 대비 방향: +1 우위 · −1 열위 · 0 불분명 (합격하려면 0 이상이어야)
VT_EXPECT_SIGN = {'strategy': 0, 'candle': -1, 'rsi': -1, 'fdip': 1, 'lvflow': 1, 'lvhigh': 1, 'jongga': 1, 'final': 1, 'v62': 1}
# 재현성 점검용 백테스트 기대치 (조정·검증 기간 평균)
VT_EXPECT_NUM = {'final': {'win': 0.63, 'hold': 6.4, 'avg': 0.67}, 'strategy': {'win': 0.61, 'hold': 6.2, 'avg': 0.41},
                 'rsi': {'win': 0.565, 'hold': 8.0, 'avg': 0.27}, 'fdip': {'win': 0.655, 'hold': 12.7, 'avg': 1.48},
                 'lvflow': {'win': 0.53, 'hold': 21.0, 'avg': 1.94}, 'lvhigh': {'win': 0.51, 'hold': 21.0, 'avg': 1.88},
                 'candle': {'win': 0.625, 'hold': 13.3, 'avg': 0.18}, 'jongga': {'win': 0.42, 'hold': 1.0, 'avg': 0.11},
                 'v62': {'win': 0.49, 'hold': 17.7, 'avg': 0.85}}
# 전종목 백테스트 기대치 (2023-10~2025-08 / 2025-09~2026-09, 보유 중이면 다음 순위 규칙, 9EMA·10일 청산)
VT_EXPECT = {   # 전종목 백테스트 · 확정 청산 규칙 · 다음날 시가 매도 기준 (조정 / 검증)
    'final': {'win': '63% / 63%', 'avg': '+0.50% / +0.83%', 'hold': '약 6일'},
    'strategy': {'win': '62% / 60%', 'avg': '+0.37% / +0.45%', 'hold': '약 6일'},
    'rsi': {'win': '56% / 57%', 'avg': '+0.24% / +0.29%', 'hold': '약 8일'},
    'fdip': {'win': '64% / 67%', 'avg': '+1.58% / +1.38%', 'hold': '약 13일'},
    'lvflow': {'win': '53% / 53%', 'avg': '+2.22% / +1.66%', 'hold': '21일'},
    'lvhigh': {'win': '51% / 51%', 'avg': '+1.38% / +2.39%', 'hold': '21일'},
    'candle': {'win': '61% / 64%', 'avg': '+0.53% / −0.18%', 'hold': '약 13일'},
    'jongga': {'win': '40% / 44%', 'avg': '−0.05% / +0.27%', 'hold': '1일 (밤사이)'},
    'v62': {'win': '49% / 49%', 'avg': '+0.45% / +1.25%', 'hold': '약 17일 (자체 규칙)'},
    'control': {'win': '56% / 57%', 'avg': '+0.41% / +0.42%', 'hold': '약 5일 (9EMA 규칙 기준)'},
    'control_v62': {'win': '—', 'avg': '−0.30% / +0.09%', 'hold': '약 17일 (자체 규칙)'},
    'control_jongga': {'win': '—', 'avg': '−0.11% / +0.06%', 'hold': '1일 (종가 매수 → 다음날 시가)'},
}


# ════════════════════════════════════════════
#  모델별 1회 최대 주문금액 — 추천 (v5.2)
#  ① 자금 회전: 하루 3종목 × 평균 보유일 = 동시에 들고 있을 종목 수 → 계좌 ÷ 동시 보유 수
#     (이보다 크게 사면 현금이 모자라 뒤에 온 신호를 건너뜀 → 모델을 설계대로 검증하지 못함)
#  ② 꼬리 위험: 최악 5% 거래의 평균 손실이 나도 계좌의 1% 이내 (손절 대신 비중으로 관리 — 검증 보고서 12-3)
#  → 둘 중 작은 값을 5만 원 단위로 내림 · 최소 20만 원 (1주 값이 주문금액보다 비싼 종목은 살 수 없어서)
# ════════════════════════════════════════════
ORDER_TAIL = {'final': 17.9, 'strategy': 16.5, 'rsi': 26.0, 'fdip': 22.6, 'lvflow': 7.8, 'lvhigh': 18.9, 'candle': 18.0,
              'jongga': 4.8, 'v62': 20.0}      # 최악 5% 거래 평균 손실 % — 모델 점검(2026-09-23) · V6.2는 자체 손절 −20% 기준 · 저변동고점 v5.4 · 종가베팅 v5.6 검증
ORDER_PICKS, ORDER_TAIL_BUDGET, ORDER_FLOOR, ORDER_UNIT = 3, 0.01, 200_000, 50_000


def order_reco(cash):
    """가상 계좌 금액 기준 모델별 추천 1회 최대 주문금액과 근거"""
    floor = min(ORDER_FLOOR, int(cash / 10 // ORDER_UNIT) * ORDER_UNIT) or ORDER_UNIT
    out = {}
    for g in MODEL_ORDER:
        hold, tail = VT_EXPECT_NUM[g]['hold'], ORDER_TAIL[g]
        conc = ORDER_PICKS * hold
        by_cash, by_tail = cash / conc, cash * ORDER_TAIL_BUDGET / (tail / 100)
        raw = min(by_cash, by_tail)
        out[g] = {'value': max(floor, int(raw // ORDER_UNIT) * ORDER_UNIT), 'hold': hold, 'conc': round(conc), 'tail': tail,
                  'by_cash': int(by_cash), 'by_tail': int(by_tail),
                  'bound': 'floor' if raw < floor else ('tail' if by_tail < by_cash else 'cash')}
    return out


def _pool_prices():
    """후보풀 종목의 최근 종가 (오름차순) — 주문금액으로 1주도 못 사는 종목 비율 계산용"""
    ds = _recent_dates_cached()
    if not ds:
        return []
    rows = db.conn().execute("SELECT close FROM candles WHERE date=? AND ticker IN (SELECT ticker FROM stocks WHERE in_pool=1)",
                             (ds[-1],)).fetchall()
    return sorted(int(r[0]) for r in rows if r[0])


# ════════════════════════════════════════════
#  한 달 수익 — 예상(백테스트 기대값) → 실제(가상매매 계좌 곡선) (v5.3)
#  예상 = 한 달 거래 수 × 종목당 금액 × 백테스트 건당 평균(비용 반영) ÷ 계좌 평가액
#    한 달 거래 수 = 21거래일 × 하루 매수 종목(최대 3 · 캔들·RSI처럼 조건이 드문 모델은 실제 기록 평균)
#                   — 계좌가 모자라면 (평가액 ÷ 종목당 금액) ÷ 평균 보유일 로 줄임
#  실제 = 계좌 곡선의 최근 21거래일 변화. 한 달이 차기 전에는 시작부터 지금까지 누적(확인 중)
# ════════════════════════════════════════════
MONTH_DAYS = 21


def _expect_avg_pair(g):
    """백테스트 건당 평균 수익 (조정 기간, 검증 기간) % — VT_EXPECT 문구에서 읽음"""
    s = str((VT_EXPECT.get(g) or {}).get('avg', ''))
    v = []
    for x in s.split('/'):
        x = x.strip().replace('%', '').replace('−', '-').replace('+', '')
        try:
            v.append(float(x))
        except ValueError:
            pass
    return (v[0], v[1]) if len(v) >= 2 else (VT_EXPECT_NUM[g]['avg'], VT_EXPECT_NUM[g]['avg'])


def _picks_per_day(g, days=60):
    """최근 신호일당 실제 매수 종목 수 (대조군의 신호일 기준) — 신호일 5일 미만이면 None
       이 모델이 기록을 시작한 날부터만 셈 (나중에 추가된 모델이 시작 전 날짜 때문에 적게 잡히지 않게)"""
    c = db.conn()
    first = c.execute("SELECT MIN(signal_date) FROM vtrades WHERE grp=?", (g,)).fetchone()[0]
    if not first:
        return None
    ds = [r[0] for r in c.execute(
        "SELECT DISTINCT signal_date FROM vtrades WHERE grp='control' AND signal_date>=? ORDER BY signal_date DESC LIMIT ?",
        (first, days))]
    if len(ds) < 5:
        return None
    q = ','.join('?' * len(ds))
    n = c.execute(f"SELECT COUNT(*) FROM vtrades WHERE grp=? AND signal_date IN ({q})", (g, *ds)).fetchone()[0]
    return n / len(ds)


def _model_month(g, curve):
    """모델 하나의 한 달 예상 · 실제 · 달력 월별 (curve: [{'date','equity'}] — 모델 탭은 vt_curve, 대시보드는 vt_account)"""
    cash = db.ACCT['cash']
    eq = float(curve[-1]['equity']) if curve else float(cash)
    cap = db.order_cap(g)
    size = eq / db.ACCT['slots']
    if cap:
        size = min(size, cap)
    hold = VT_EXPECT_NUM[g]['hold']
    ppd = _picks_per_day(g)
    picks = min(3.0, ppd) if ppd is not None else 3.0
    per_day = min(picks, (eq / size) / hold) if size > 0 else 0.0
    n = MONTH_DAYS * per_day
    avg = VT_EXPECT_NUM[g]['avg']
    lo, hi = sorted(_expect_avg_pair(g))
    k = n * size / 100                                # 건당 수익률 1%p 당 한 달 수익금
    exp = {'equity': round(eq), 'size': round(size), 'hold': hold, 'picks': round(picks, 2),
           'picks_src': 'actual' if ppd is not None else 'max', 'trades': round(n, 1), 'limited': per_day < picks * 0.98,
           'avg': avg, 'lo_avg': lo, 'hi_avg': hi, 'profit': round(k * avg), 'ret': round(k * avg / eq * 100, 2),
           'profit_lo': round(k * lo), 'profit_hi': round(k * hi), 'lo': round(k * lo / eq * 100, 2), 'hi': round(k * hi / eq * 100, 2)}
    act, months = _curve_actual(curve)
    return {'expect': exp, 'actual': act, 'months': months, 'month_days': MONTH_DAYS}


def _curve_actual(curve):
    """곡선의 최근 한 달(21거래일) 변화 · 달력 월별 — 모델과 지수 ETF 공통"""
    cash = db.ACCT['cash']
    eq = float(curve[-1]['equity']) if curve else float(cash)
    act, months = None, []
    if curve:
        days = len(curve)
        if days > MONTH_DAYS:
            e0 = float(curve[-MONTH_DAYS - 1]['equity'])
            act = {'full': True, 'days': MONTH_DAYS, 'from': curve[-MONTH_DAYS - 1]['date'], 'to': curve[-1]['date'],
                   'ret': round((eq / e0 - 1) * 100, 2), 'profit': round(eq - e0), 'since_ret': round((eq / cash - 1) * 100, 2),
                   'since_days': days}
        else:
            act = {'full': False, 'days': days, 'from': curve[0]['date'], 'to': curve[-1]['date'],
                   'ret': round((eq / cash - 1) * 100, 2), 'profit': round(eq - cash), 'since_ret': round((eq / cash - 1) * 100, 2),
                   'since_days': days}
        by = {}
        for p_ in curve:
            by.setdefault(p_['date'][:6], []).append(p_)
        prev, keys = float(cash), sorted(by)
        for i, m in enumerate(keys):
            end = float(by[m][-1]['equity'])
            months.append({'month': m, 'ret': round((end / prev - 1) * 100, 2), 'profit': round(end - prev), 'days': len(by[m]),
                           'start': i == 0, 'current': i == len(keys) - 1})
            prev = end
    return act, months


def _bench_view(start):
    """지수 ETF(KODEX 200) 그냥 보유 — start(가장 먼저 매수한 날)부터, 같은 계좌 금액"""
    b = db.bench_curve(start)
    if not b['curve']:
        return None, []
    act, months = _curve_actual(b['curve'])
    return {**b['stats'], 'month': {'actual': act, 'months': months, 'month_days': MONTH_DAYS}}, b['curve']


@app.get("/api/order/max")
async def api_order_max():
    """모델별 1회 최대 주문금액 — 현재값 · 추천값(가상 계좌 금액 기준) · 근거 · 후보풀 가격 분포"""
    cash = db.ACCT['cash']
    return {'values': dict(CFG.get('model_order_max') or {}), 'defaults': DEFAULT_CFG['model_order_max'],
            'reco': order_reco(cash), 'cash': cash, 'slots': db.ACCT['slots'], 'order': MODEL_ORDER, 'names': VT_NAMES,
            'prices': await asyncio.to_thread(_pool_prices), 'limits': {'min': ORDER_MIN, 'max': ORDER_MAX},
            'rule': {'picks': ORDER_PICKS, 'tail_budget': ORDER_TAIL_BUDGET * 100, 'floor': ORDER_FLOOR, 'unit': ORDER_UNIT},
            'at_track': CFG.get('at_track', 'final'), 'at_mode': CFG.get('at_mode', 'OFF'),
            'at_slots': CFG.get('at_slots'), 'at_max_order_krw': CFG.get('at_max_order_krw')}


def _foreign_strength(ok):
    """외국인 20일 순매수 강도 = 순매수 합 / (20일 평균 거래대금 × 일수).
       KRX 수급이 쌓여 있으면 KRX로 통일, 아니면 KIS 투자자 데이터로 통일 (단위가 달라 섞지 않음)"""
    dates = db.recent_trading_dates(20)
    krx = db.flow_sums(dates, '외국인')
    use_krx = sum(1 for t in ok if krx.get(t, (0, 0))[1] >= 10) >= len(ok) * 0.5
    out = {}
    for t, cd in ok.items():
        val20 = sum(c['close'] * c['volume'] for c in cd[-20:]) / 20
        if val20 <= 0:
            out[t] = None
            continue
        if use_krx:
            s_, n_ = krx.get(t, (0, 0))
            out[t] = s_ / (val20 * n_) if n_ >= 10 else None
        else:
            inv = db.load_investors(t, 20)
            out[t] = (sum(x.get('foreign_amt', 0) or 0 for x in inv) / (val20 * len(inv))) if len(inv) >= 10 else None
    return out


def vt_create_batch(results, pool_candles, infos, rev_table=None):
    """신호일당 1회 · 데이터 품질 확인 → 백업 → 전 트랙 기록을 한 번에 확정 (오류 시 전체 취소)"""
    sig = db.recent_trading_dates(1)
    if not sig:
        return
    signal_date = sig[-1]
    if db.meta_get('vt_last_batch', '') == signal_date or db.vt_batch_exists(signal_date):
        return
    krx = bool(CFG.get('krx_id') and CFG.get('krx_pw'))
    dq = db.data_quality(signal_date, krx)
    if dq['status'] == 'HOLD' and dq.get('flows_ok') is False:
        job_flows()                                   # 빠진 수급을 바로 재수집 후 다시 점검
        dq = db.data_quality(signal_date, krx)
    if dq['status'] == 'HOLD':
        _dq_alert(signal_date, dq, '가상매매 기록 보류')
        return
    db.vt_backup('batch')
    c = db.conn()
    db._defer['on'] = True
    try:
        _vt_create_batch_core(results, pool_candles, infos, rev_table, signal_date)
        c.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('vt_last_batch',?)", (signal_date,))
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        db._defer['on'] = False
    db.vt_backup('batch_done')                        # 확정 직후 상태도 백업 → 최신 백업 = 최신 정상 상태
    try:
        n = rt.sync(CFG)                              # 실전형 가상매매: 같은 신호를 매수 대기로
        print(f"[RT] 실전형 신호 {n}건 추가", flush=True)
    except Exception as e:
        print(f'[RT] 신호 가져오기 실패: {e}', flush=True)


def _apply_vt_account():
    """가상 계좌 설정 반영 — 매매 기록은 그대로, 계좌 곡선 재생 계산만 바뀜"""
    db.ACCT['cash'] = int(CFG.get('vt_cash', 10_000_000) or 10_000_000)
    db.ACCT['slots'] = int(CFG.get('vt_slots', 20) or 20)
    db.ACCT['order_max'] = dict(CFG.get('model_order_max') or {})


_apply_vt_account()


def _dq_alert(date, dq, what):
    try:
        days = set(json.loads(db.meta_get('dq_hold_dates', '[]') or '[]'))
        days.add(date)
        db.meta_set('dq_hold_dates', json.dumps(sorted(days)))
    except Exception:
        pass
    msg = f"⚠️ 데이터 품질 미달 ({date}) — {what}\n· {dq['why']}\n· 다음 실행 때 자동으로 다시 시도합니다"
    print(f"[DQ] {msg}", flush=True)
    push('alert', {'msg': f"데이터 품질 미달 — {what}: {dq['why']}"})
    if db.meta_get('dq_alerted', '') != f"{date}:{what}":
        db.meta_set('dq_alerted', f"{date}:{what}")
        telegram(msg)


def _vt_create_batch_core(results, pool_candles, infos, rev_table, signal_date):
    import random
    held = db.vt_open_tickers('strategy')
    picks = []
    for rank, r in enumerate(sorted(results, key=lambda x: -x['score']), start=1):
        if r['ticker'] in held:
            continue
        v = r.get('rev') or {}
        picks.append((r['ticker'], r['name'], len(picks) + 1, r['score'], v.get('rsi'), r['price']))
        if len(picks) >= int(CFG.get('swing_top', 3)):
            break
    if not picks:
        picks = []
    db.vt_add('strategy', signal_date, picks)
    # 매수 조건(후보풀 · 위험필터 통과) — 캔들·RSI·대조군 공통
    ok = {t: cd for t, cd in pool_candles.items() if not eng.risk_filter(cd)}
    name = lambda t: infos.get(t, {}).get('name', t)

    def top3(scored, grp):
        held_g = db.vt_open_tickers(grp)
        out = []
        for sc, t, extra in sorted(scored, key=lambda x: -x[0]):
            if t in held_g:
                continue
            out.append((t, name(t), len(out) + 1, round(sc, 3), extra, ok[t][-1]['close']))
            if len(out) >= 3:
                break
        return out
    # ② 캔들: 오늘 상승반전 패턴 중 강하게 마감한 순
    cands = []
    for t, cd in ok.items():
        sc, pat = eng.candle_reversal_score(cd)
        if sc is not None:
            cands.append((sc, t, eng.rsi([c['close'] for c in cd], 14)))
    candle = top3(cands, 'candle')
    db.vt_add('candle', signal_date, candle)
    # ③ 로스카메론 RSI: 극단값 5 이하를 2일 RSI에 적용 (1분봉 극단을 일봉에서 재현), 깊은 순
    #    14일 RSI ≤ 20은 일봉에서 월 6건 수준으로 너무 드물어 검증이 안 됨 → 2026-09-23 변경
    rs = []
    for t, cd in ok.items():
        r = eng.rsi([c['close'] for c in cd], 2)
        if r is not None and r <= 5:
            rs.append((-r, t, r))
    rsi_p = top3(rs, 'rsi')
    db.vt_add('rsi', signal_date, rsi_p)
    # ⑤·⑥ 외국인 수급 결합 트랙 (2026-09-23 추가 · 후보 12개 중 원리·일관성·차별성으로 선정)
    fr = _foreign_strength(ok)

    def prank_map(vals):
        """백분위 (0~1). 값이 없는 종목은 0.5 (백테스트의 중립 처리와 동일)"""
        have = sorted((v, t) for t, v in vals.items() if v is not None)
        n = len(have)
        out = {t: 0.5 for t in vals}
        for k, (v, t) in enumerate(have):
            out[t] = k / (n - 1) if n > 1 else 0.5
        return out
    r_fr = prank_map(fr)
    drop5 = {t: (-(cd[-1]['close'] / cd[-6]['close'] - 1) if len(cd) > 6 and cd[-6]['close'] > 0 else None)
             for t, cd in ok.items()}
    lowv = {}
    for t, cd in ok.items():
        a = eng.atr(cd, 14)
        lowv[t] = (-a / cd[-1]['close']) if a and cd[-1]['close'] > 0 else None
    r_drop, r_lowv = prank_map(drop5), prank_map(lowv)
    rsi14 = lambda t: eng.rsi([c['close'] for c in ok[t]], 14)
    # ⑤ 외국인 저가매수: 최근 5일 많이 빠졌는데 외국인이 사는 종목
    fdip = top3([((r_drop[t] + r_fr[t]) / 2, t, rsi14(t)) for t in ok if drop5[t] is not None], 'fdip')
    db.vt_add('fdip', signal_date, fdip)
    # ⑥ 저변동 + 외국인: 변동성 낮고 외국인이 사는 종목
    lvflow = top3([((r_lowv[t] + r_fr[t]) / 2, t, rsi14(t)) for t in ok if lowv[t] is not None], 'lvflow')
    db.vt_add('lvflow', signal_date, lvflow)
    # ⑧ 저변동고점 (차트 모델 · v5.4): 변동성 낮고 52주 고점 근처, 거래량 실린 급등이 적은 종목 — 수급 없이 차트만
    lvh_s, _ = eng.lvhigh_scores({t: eng.lvhigh_features(cd) for t, cd in ok.items()})
    lvhigh = top3([(sc, t, rsi14(t)) for t, sc in lvh_s.items()], 'lvhigh')
    db.vt_add('lvhigh', signal_date, lvhigh)
    # ⑨ 종가베팅 (v5.6): 6가지 조건을 모두 만족한 종목 중 많이 오른 순 — 신호일 종가에 매수(장후 시간외 종가) → 다음 거래일 시가 매도
    jg_s = eng.jongga_scores({t: eng.jongga_features(cd) for t, cd in ok.items()})
    jongga = top3([(sc, t, rsi14(t)) for t, sc in jg_s.items()], 'jongga')
    db.vt_add('jongga', signal_date, jongga)
    held_cj = db.vt_open_tickers('control_jongga')                 # 같은 규칙 대조군: 후보풀 무작위 3종목 종가 매수
    pool_j = sorted(t for t in ok if t not in held_cj)
    rng_j = random.Random(int(signal_date) + 88)
    db.vt_add('control_jongga', signal_date, [(t, name(t), i + 1, 0, None, ok[t][-1]['close'])
                                              for i, t in enumerate(rng_j.sample(pool_j, min(3, len(pool_j))))])
    # ⑦ 최종: 지금까지 살아남은 신호만 동일 가중 — 반전그룹(과매도·5일하락 평균) + 외국인 + 연기금역 (2026-09-23)
    rev_table = rev_table or {}
    fin = []
    for t in ok:
        v = rev_table.get(t)
        if not v or drop5.get(t) is None:
            continue
        sc = ((v.get('p_rev', v['oversold'] / 100) + r_drop[t]) / 2 + v.get('p_fr', v['foreign'] / 100)
              + v.get('p_in', v['inst_contra'] / 100)) / 3
        fin.append((sc, t, v.get('rsi')))
    final = top3(fin, 'final')
    db.vt_add('final', signal_date, final)
    # ⑧ SCOUT v6.2 (외부 모델 편입 · 모멘텀+수급) — 순위는 그날 데이터가 있는 전 종목 기준, 후보는 후보풀
    v62 = []
    try:
        dates60 = db.recent_trading_dates(70)
        allc = db.candles_since(dates60[0]) if dates60 else {}
        d20 = db.recent_trading_dates(20)
        ffl, ifl = db.flow_rows(d20, '외국인'), db.flow_rows(d20, '기관합계')
        feats = {}
        for t, cd in allc.items():
            if not cd or cd[-1]['date'] != signal_date:
                continue
            ds = [x['date'] for x in cd[-20:]]
            feats[t] = eng.v62_features(cd, [ffl.get(t, {}).get(d) for d in ds], [ifl.get(t, {}).get(d) for d in ds])
        vs = eng.v62_scores(feats)
        cands = [(vs[t], t, (feats[t] or {}).get('rsi14')) for t in ok
                 if t in vs and feats.get(t) and feats[t].get('ma60') and feats[t]['close'] >= 1000]
        v62 = top3(cands, 'v62')
    except Exception as e:
        print(f'[VT] V6.2 트랙 계산 실패: {e}', flush=True)
    db.vt_add('v62', signal_date, v62)
    held_cv = db.vt_open_tickers('control_v62')
    pool_v = sorted(t for t in ok if t not in held_cv)
    rng_v = random.Random(int(signal_date) + 62)
    db.vt_add('control_v62', signal_date, [(t, name(t), i + 1, 0, None, ok[t][-1]['close'])
                                           for i, t in enumerate(rng_v.sample(pool_v, min(3, len(pool_v))))])
    # ④ 대조군: 매수 조건을 만족하는 종목 중 무작위 3개 — 신호일로 시드 고정
    held_c = db.vt_open_tickers('control')
    pool = sorted(t for t in ok if t not in held_c)
    rng = random.Random(int(signal_date))
    ctrl = [(t, name(t), i + 1, 0, None, ok[t][-1]['close'])
            for i, t in enumerate(rng.sample(pool, min(3, len(pool))))]
    db.vt_add('control', signal_date, ctrl)
    print(f"[VT] {signal_date} 가상매수 예약 · 종합 {len(picks)} · 캔들 {len(candle)} · RSI {len(rsi_p)} · "
          f"외인저가 {len(fdip)} · 저변동외인 {len(lvflow)} · 저변동고점 {len(lvhigh)} · 종가베팅 {len(jongga)}(종가 매수) · "
          f"최종 {len(final)} · V6.2 {len(v62)} · 대조군 {len(ctrl)}", flush=True)
    push('alert', {'msg': f'가상매매 {signal_date} 기록 · 종합 {len(picks)} · 캔들 {len(candle)} · RSI {len(rsi_p)}'})
    vt_daily_report(signal_date, {'strategy': picks, 'candle': candle, 'rsi': rsi_p,
                                  'fdip': fdip, 'lvflow': lvflow, 'lvhigh': lvhigh, 'jongga': jongga, 'final': final, 'v62': v62})


def vt_daily_report(signal_date, picks_by_track):
    """매일 18:20 장 마감 리포트 (v6.1.3 — 가상계좌 평가 · 오늘 손익 · 매수/매도/보유 건별 · 대조군 · 지수 비교).
       실패하면 예전 형식으로 보냄 (리포트 문제로 알림이 끊기지 않게)"""
    try:
        telegram(_vt_report_text(signal_date, picks_by_track))
    except Exception as e:
        print(f'[VT] 새 리포트 실패 → 예전 형식: {e}', flush=True)
        _vt_daily_report_old(signal_date, picks_by_track)


def _vt_report_text(signal_date, picks_by_track):
    c_ = db.conn()
    md = f"{int(signal_date[4:6])}/{int(signal_date[6:])}"
    wd = '월화수목금토일'[datetime.strptime(signal_date, '%Y%m%d').weekday()]
    late = datetime.now().strftime('%H:%M') < '09:00'
    won = lambda v: f"{v:+,.0f}원"
    man = lambda v: f"{v / 1e4:,.0f}만"
    L = [f"📘 Scout {md}({wd}) 장 마감 실적 · 가상 (주문 없음){' · 아침 보충' if late else ''}", '━━━━━━━━━━━━━━']
    # ① 가상계좌 (최종 트랙 · 1,000만) — 대시보드와 같은 계산
    acc = db.vt_account('final')
    cv, stt = acc.get('curve') or [], acc.get('stats') or {}
    if cv:
        eq = cv[-1]['equity']
        prev = cv[-2]['equity'] if len(cv) > 1 else db.ACCT['cash']
        ctl = db.vt_account('control').get('stats') or {}
        bn = (db.bench_curve(stt.get('start')).get('stats') or {})
        L.append('💰 가상계좌 · 최종 (1,000만 시작)')
        L.append(f" 평가 {eq:,.0f}원 · 오늘 {won(eq - prev)} ({(eq / prev - 1) * 100:+.2f}%)")
        L.append(f" 누적 {stt.get('return', 0):+.2f}% ({won(eq - db.ACCT['cash'])})"
                 + (f" · 대조군 {ctl['return']:+.2f}%" if ctl.get('return') is not None else '')
                 + (f" · {bn.get('name', '지수 ETF')} {bn['return']:+.2f}%" if bn.get('return') is not None else ''))
        L.append(f" 현금 {man(stt.get('cash', 0))} · 보유 {stt.get('positions', 0)}종목 · 최대낙폭 {stt.get('mdd', 0):.1f}%")
    # ② 최종 트랙 오늘 매수 · 매도 · 보유
    buys = [dict(r) for r in c_.execute("SELECT * FROM vtrades WHERE grp='final' AND entry_date=? ORDER BY rank", (signal_date,))]
    L.append(f"\n🟢 오늘 매수 · 최종 {len(buys)}건 (시가)")
    L += [f" {b['name']} @{b['entry_price']:,.0f}" for b in buys if b.get('entry_price')] or [' 없음']
    sold = db.vt_closed_on(signal_date, 'final')
    L.append(f"\n🔴 오늘 매도 · 최종 {len(sold)}건" + (f" · 평균 {sum(x['ret'] for x in sold) / len(sold):+.2f}%" if sold else ''))
    L += [f" {'🟢' if x['ret'] > 0 else '🔻'} {x['name']} {x['ret']:+.2f}% · {x['exit_reason']} · {x['held']}일" for x in sold] or [' 없음']
    hold = [r for r in db.vt_open_rows('final') if r['status'] == '보유' and r.get('entry_price')]
    if hold:
        evs = [((r.get('last_close') or r['entry_price']) / r['entry_price'] - 1) * 100 for r in hold]
        mx = (db.rule_of('final') or {}).get('hold')
        L.append(f"\n📦 보유 · 최종 {len(hold)}종목 · 평균 {sum(evs) / len(evs):+.2f}% (이익 {sum(1 for v in evs if v > 0)} · 손실 {sum(1 for v in evs if v <= 0)})")
        for r, v in sorted(zip(hold, evs), key=lambda z: -z[1])[:10]:
            L.append(f" {'▲' if v > 0 else '▼'} {r['name']} {v:+.2f}% · {r['held']}{('/' + str(mx)) if mx else ''}일")
    # ③ 실전형(장중 감시)
    try:
        rb = c_.execute("SELECT COUNT(*) FROM rt_trades WHERE entry_date=? AND grp NOT IN ('jongga')", (signal_date,)).fetchone()[0]
        rc = [r[0] for r in c_.execute("SELECT ret FROM rt_trades WHERE exit_date=? AND status='청산'", (signal_date,)) if r[0] is not None]
        ro = c_.execute("SELECT COUNT(*) FROM rt_trades WHERE status='보유'").fetchone()[0]
        L.append(f"\n⚡ 실전형(장중 감시) 오늘: 매수 {rb} · 청산 {len(rc)}" + (f" · 평균 {sum(rc) / len(rc):+.2f}% · 승 {sum(1 for x in rc if x > 0)}/{len(rc)}" if rc else '')
                 + f" · 보유 {ro}")
    except Exception as e:
        print(f'[VT] 실전형 요약 실패: {e}', flush=True)
    # ④ 다른 모델 오늘 매도 · 대조군
    oth = []
    for g in ('strategy', 'fdip', 'lvflow', 'lvhigh', 'rsi', 'candle', 'jongga', 'v62'):
        for x in db.vt_closed_on(signal_date, g):
            oth.append(f" {'🟢' if x['ret'] > 0 else '🔻'} [{VT_NAMES[g]}] {x['name']} {x['ret']:+.1f}% · {x['exit_reason']} · {x['held']}일")
    cc = db.vt_closed_on(signal_date, 'control')
    if oth or cc:
        L.append(f"\n🧪 다른 모델 오늘 매도 {len(oth)}건")
        L += oth[:15] + ([f" … 외 {len(oth) - 15}건"] if len(oth) > 15 else [])
        if cc:
            L.append(f" (대조군 {len(cc)}건 평균 {sum(x['ret'] for x in cc) / len(cc):+.2f}%)")
    # ⑤ 다음 거래일 매수 예정
    L.append('\n🗓 다음 거래일 시가 매수 예정')
    fp = picks_by_track.get('final') or []
    L.append(" [최종] " + (', '.join(f"{p[1]}" for p in fp) if fp else '없음') + (f" · 종목당 약 {man(min(stt.get('equity', db.ACCT['cash']) / db.ACCT['slots'], db.order_cap('final') or 1e18))}" if fp and cv else ''))
    for g in ('strategy', 'fdip', 'lvflow', 'lvhigh', 'candle', 'rsi', 'v62'):
        ps = picks_by_track.get(g) or []
        if ps:
            L.append(f" [{VT_NAMES[g]}] " + ', '.join(p[1] for p in ps))
    ps = picks_by_track.get('jongga') or []
    if ps:
        L.append(f" [{VT_NAMES['jongga']}] {md} 종가 매수 → 다음 시가 매도: " + ', '.join(p[1] for p in ps))
    # ⑥ 누적 (트랙별)
    st = db.vt_stats()
    if any(st.get(g, {}).get('n') for g in db.VT_TRACKS):
        since = st['since']
        L.append(f"\n📈 누적 청산 ({int(since[4:6])}/{int(since[6:])}~) 건수 · 승률 · 건당")
        for g in db.VT_TRACKS:
            x = st.get(g, {})
            if x.get('n'):
                L.append(f" {VT_NAMES[g]}: {x['n']}건 · {x['win']:.0%} · {x['avg']:+.2f}%")
    out = '\n'.join(L)
    return out if len(out) < 3900 else out[:3880] + '\n…(길어서 줄임 — 앱에서 확인)'


def _vt_daily_report_old(signal_date, picks_by_track):
    """매일 18:20 — 트랙별 내일 매수 예정 · 오늘 매도 · 누적 성과(대조군 대비)"""
    md = f"{int(signal_date[4:6])}/{int(signal_date[6:])}"
    late = datetime.now().strftime('%H:%M') < '09:00'
    L = [f"📈 가상매매 장 마감 리포트 ({md}{' · 아침 보충' if late else ''})"]
    try:                                                   # v6.1.2: 실전형(장중 감시) 오늘 실적을 맨 위에
        c_ = db.conn()
        rb = c_.execute("SELECT COUNT(*) FROM rt_trades WHERE entry_date=? AND grp NOT IN ('jongga')", (signal_date,)).fetchone()[0]
        rc = [r[0] for r in c_.execute("SELECT ret FROM rt_trades WHERE exit_date=? AND status='청산'", (signal_date,)) if r[0] is not None]
        ro = c_.execute("SELECT COUNT(*) FROM rt_trades WHERE status='보유'").fetchone()[0]
        L.append(f"■ 실전형 오늘: 매수 {rb}건 · 청산 {len(rc)}건" + (f" · 평균 {sum(rc) / len(rc):+.2f}% · 승 {sum(1 for x in rc if x > 0)}/{len(rc)}" if rc else '')
                 + f" · 보유 {ro}건")
    except Exception as e:
        print(f'[VT] 실전형 요약 실패: {e}', flush=True)
    L.append('■ 다음 거래일 시가 매수 예정')
    for g in ('final', 'strategy', 'candle', 'rsi', 'fdip', 'lvflow', 'lvhigh', 'v62'):
        ps = picks_by_track.get(g) or []
        names = ', '.join(p[1] for p in ps) if ps else '없음'
        L.append(f" [{VT_NAMES[g]}] {names}")
    ps = picks_by_track.get('jongga') or []
    L.append(f"■ {md} 종가 매수 → 다음 거래일 시가 매도\n [{VT_NAMES['jongga']}] " + (', '.join(p[1] for p in ps) if ps else '없음'))
    L.append('■ 오늘 매도')
    any_closed = False
    for g in ('final', 'strategy', 'candle', 'rsi', 'fdip', 'lvflow', 'lvhigh', 'jongga', 'v62'):
        for c_ in db.vt_closed_on(signal_date, g):
            any_closed = True
            L.append(f" {'🟢' if c_['ret'] > 0 else '🔴'} [{VT_NAMES[g]}] {c_['name']} {c_['ret']:+.1f}% "
                     f"({c_['exit_reason']} · {c_['held']}일)")
    if not any_closed:
        L.append(' 없음')
    st = db.vt_stats()
    if any(st.get(g, {}).get('n') for g in db.VT_TRACKS):
        since = st['since']
        L.append(f"■ 누적 ({int(since[4:6])}/{int(since[6:])}~) 건수 · 승률 · 건당")
        for g in db.VT_TRACKS:
            x = st.get(g, {})
            L.append(f" {VT_NAMES[g]}: " + (f"{x['n']}건 · {x['win']:.0%} · {x['avg']:+.2f}%" if x.get('n') else '청산 없음'))
    telegram('\n'.join(L))


def _gate_and_evaluate():
    """오늘 일봉이 온전할 때만 가상매매 갱신. 미달이면 보류하고 알림 (다음 실행 때 자동 따라잡기)"""
    try:
        last = db.recent_trading_dates(1)
        if not last:
            return
        now = datetime.now()
        today = now.strftime('%Y%m%d')
        expect = last[-1]
        try:
            if now.strftime('%H:%M') >= '15:40' and ext.is_trading_day(now, CFG['app_key'], CFG['app_secret']):
                expect = today
        except Exception:
            pass
        if expect != last[-1]:
            dq = {'status': 'HOLD', 'why': f'오늘({today}) 일봉이 수집되지 않음', 'date': expect}
            db.meta_set('dq_last', json.dumps(dq, ensure_ascii=False))
        else:
            dq = db.data_quality(expect)
        if dq['status'] == 'HOLD':
            _dq_alert(expect, dq, '가상매매 갱신 보류')
            return
        db.vt_backup('eval')
        job_virtual()
        db.vt_backup('eval_done')
        _gate_notice()
        job_rt_close(expect)
        job_at_close()
    except Exception as e:
        print(f'[VT] 품질 확인/갱신 실패: {e}', flush=True)


def _gate_notice():
    """최종 트랙이 실전 전환 게이트를 처음 통과한 날 한 번만 알림"""
    try:
        judge, _ = db.vt_judge(VT_EXPECT_SIGN, VT_EXPECT_NUM)
        g = at.gate(CFG.get('at_track', 'final'), judge)
        if g['pass'] and not db.meta_get('gate_passed_at', ''):
            db.meta_set('gate_passed_at', datetime.now().isoformat(timespec='seconds'))
            j = judge.get(CFG.get('at_track', 'final'), {})
            telegram("🚦 실전 전환 게이트 통과 — 최종 모델 가상매매 검증 조건 충족\n"
                     f"· 청산 {j.get('n')}건 · {j.get('days')}거래일 · 재현성 {j.get('repro')} · 대조군 비교 {j.get('verdict')}\n"
                     "· 권장: 실전 리허설(DRY) 결과가 가상매매와 비슷한지 확인한 뒤 실전 자동매매 탭에서 LIVE 소액 전환")
    except Exception as e:
        print(f'[GATE] {e}', flush=True)


@logged_job
def job_virtual():
    """장마감 동기화 뒤 일봉으로 가상매매 갱신"""
    try:
        closed = db.vt_evaluate(datetime.now().strftime('%Y%m%d'))
        if closed:
            print(f"[VT] 청산 {len(closed)}건: " + ', '.join(
                f"{c['name']} {c['ret']:+.1f}%" for c in closed if c['grp'] == 'strategy'), flush=True)
    except Exception as e:
        print(f'[VT] 평가 실패: {e}', flush=True)


@logged_job
def job_vt_report():
    """매주 금요일 실전 가상매매 요약 — 트랙별 · 백테스트 기대치와 비교"""
    st = db.vt_stats()
    if not any(st.get(g, {}).get('n') for g in db.VT_TRACKS):
        return
    judge, crit = db.vt_judge(VT_EXPECT_SIGN, VT_EXPECT_NUM)
    L = [f"📊 실전 가상매매 주간 요약 ({st['since']}~)"]
    for g in db.VT_TRACKS:
        x, e = st.get(g, {}), VT_EXPECT[g]
        now = (f"{x['n']}건 · 승률 {x['win']:.0%} · 건당 {x['avg']:+.2f}% · 보유 {x['hold']:.1f}일"
               if x.get('n') else '청산 없음')
        L.append(f"■ {VT_NAMES[g]}: {now}\n   (백테스트 기대 승률 {e['win']} · 건당 {e['avg']}) · 진행 중 {x.get('open', 0)}")
        if g in judge:
            jg = judge[g]
            L.append(f"   ▶ 재현성: {jg.get('repro', '표본 부족')}" + (f" — {jg['repro_why']}" if jg.get('repro_why') else ''))
            L.append(f"   ▶ 대조군 비교: {jg['verdict']} — {jg['why']}"
                     + ((" (이 차이가 진짜라면 확인까지 " + ('5년 이상' if jg['need_months'] > 60
                         else f"약 {jg['need_months']}개월") + ")") if jg.get('need_months') else ''))
    L.append(f"※ 판정 기준은 {crit.get('fixed_at', '')}에 확정 · 모델 {len(judge)}개 비교라 t ≥ "
             f"{next(iter(judge.values()))['z_need'] if judge else '-'} 필요")
    telegram('\n'.join(L))


@logged_job
def job_at_morning():
    if CFG.get('at_mode', 'OFF') == 'OFF':
        return
    try:
        msgs = at.morning(CFG)
        if msgs:
            tg_trade(f"🤖 자동매매 [{CFG['at_mode']}] 아침 주문\n" + '\n'.join('· ' + m for m in msgs))
    except Exception as e:
        at.log(CFG.get('at_mode'), f'아침 실행 오류: {e}', 'error')
        telegram(f"⚠️ 자동매매 아침 실행 오류: {e}")
    if CFG.get('at_mode') == 'DRY' and datetime.now().strftime('%H:%M') >= '09:01':
        job_at_fill()                     # 서버를 늦게 켰으면 바로 체결 확인


def job_rt_close(today=None):
    """실전형 가상매매 장마감 — 일봉 보충 · 모델 규칙 판정 · 텔레그램 요약"""
    today = today or datetime.now().strftime('%Y%m%d')
    try:
        rt.sync(CFG)
        msgs = rt.after_close(CFG, today)
        c = db.conn()
        buys = c.execute("SELECT COUNT(*) FROM rt_trades WHERE entry_date=? AND grp NOT IN ('jongga')", (today,)).fetchone()[0]
        closed = [dict(r) for r in c.execute("SELECT grp, ret FROM rt_trades WHERE exit_date=? AND status='청산'", (today,))]
        plans = sum(1 for m in msgs if '내일 시가 매도 예정' in m)
        if buys or closed or plans:
            avg = sum(r['ret'] for r in closed) / len(closed) if closed else 0
            print(f"[RT] {today} 장마감 · 매수 {buys}건 · 청산 {len(closed)}건"          # v6.1.2: 텔레그램은 18:20 마감 리포트에 합침
                  + (f" (평균 {avg:+.2f}%)" if closed else '') + f" · 내일 시가 매도 예정 {plans}건", flush=True)
    except Exception as e:
        print(f'[RT] 장마감 처리 실패: {e}', flush=True)


RT_STATE = {'running': False, 'last': None, 'err': None, 'trading': {}}


def rt_monitor():
    """실전형 가상매매 장중 감시 — 거래일 09:00~15:19, 보유 · 매수 대기 종목 현재가를 계속 조회"""
    import time as _t
    try:
        rt.sync(CFG)
    except Exception as e:
        print(f'[RT] 시작 동기화 실패: {e}', flush=True)
    while True:
        now = datetime.now()
        hm = now.strftime('%H:%M')
        wait = 20
        try:
            ds = now.strftime('%Y%m%d')
            if now.weekday() < 5 and rt.MARKET[0] <= hm <= rt.MARKET[1] and CFG.get('app_key') and CFG.get('app_secret'):
                if ds not in RT_STATE['trading']:
                    RT_STATE['trading'] = {ds: ext.is_trading_day(now, CFG['app_key'], CFG['app_secret'])}
                if RT_STATE['trading'][ds]:
                    RT_STATE['running'] = True
                    t0 = _t.time()
                    msgs = rt.cycle(CFG, now)
                    RT_STATE.update(last=now.isoformat(timespec='seconds'), err=None)
                    if msgs:
                        print('[RT] ' + ' | '.join(msgs[:6]) + (f' 외 {len(msgs) - 6}건' if len(msgs) > 6 else ''), flush=True)
                        push('rt', {'n': len(msgs)})
                    wait = max(5, 30 - (_t.time() - t0))          # 30초마다 한 바퀴 (종목이 많으면 조회 시간만큼)
            else:
                RT_STATE['running'] = False
        except Exception as e:
            RT_STATE['err'] = str(e)[:200]
            print(f'[RT] 감시 오류: {e}', flush=True)
        _t.sleep(wait)


@logged_job
def job_at_fill():
    """DRY 실전 리허설 — 장 시작 직후 실제 시가로 체결 처리 (증권사 주문만 없음)"""
    if CFG.get('at_mode') != 'DRY':
        return
    try:
        msgs = at.dry_fill(CFG)
        if msgs:
            tg_trade("🎭 실전 리허설(DRY) 체결 — 증권사 주문 없음 · 실제 시가 기준\n" + '\n'.join('· ' + m for m in msgs))
    except Exception as e:
        at.log('DRY', f'리허설 체결 확인 오류: {e}', 'error')


@logged_job
def job_at_close():
    if CFG.get('at_mode', 'OFF') == 'OFF':
        return
    try:
        msgs = at.after_close(CFG)
        if msgs:
            tg_trade(f"🤖 자동매매 [{CFG['at_mode']}] 장마감 정리\n" + '\n'.join('· ' + m for m in msgs))
    except Exception as e:
        at.log(CFG.get('at_mode'), f'장마감 정리 오류: {e}', 'error')


@logged_job
def job_morning_orders():
    """장 시작 전 주문표 — 일반 지정가 주문은 당일만 유효하므로 매일 확인"""
    rows = db.list_positions(False)
    if not rows:
        return
    tg_trade("🌅 오늘 걸어둘 매도 주문\n\n" + '\n\n'.join(order_sheet_text(p, head='📋') for p in rows))



# ════════════════════════════════════════════
#  내 보유 (수동 매매 기록)
# ════════════════════════════════════════════
@app.get("/api/positions")
async def api_positions():
    rows = db.list_positions(True, 150)
    for r in rows:
        if not r['closed']:
            r['orders'] = order_sheet(r)
    return {'rows': rows, 'summary': db.position_summary(), 'perf': db.position_performance(),
            'linked': bool(CFG.get('account_no'))}


@app.post("/api/positions/sync")
async def api_positions_sync():
    try:
        msgs = sync_balance()
    except Exception as e:
        return JSONResponse({'ok': False, 'error': f'잔고 조회 실패: {e}'}, 400)
    for m in msgs:
        tg_trade(m)
    return {'ok': True, 'messages': msgs}


@app.get("/api/sizing")
async def api_sizing(entry: float, stop: float):
    return _sizing(entry, stop) or {'qty': 0, 'error': '설정에서 계좌 금액을 입력하세요'}


@app.post("/api/positions")
async def api_position_add(req: Request):
    b = await req.json()
    tk = str(b.get('ticker', '')).strip().zfill(6)
    bp = float(b.get('buy_price') or 0)
    qty = int(b.get('qty') or 0)
    if not tk or bp <= 0 or qty <= 0:
        return JSONResponse({'ok': False, 'error': '종목코드·매수가·수량을 입력하세요'}, 400)
    item = None
    if b.get('horizon'):
        scan = _last_scan.get(b['horizon']) or {}
        item = next((r for r in scan.get('results', []) if r['ticker'] == tk), None)
    pos, err = create_position(tk, bp, qty, b.get('stop'), b.get('target1'), b.get('target2'),
                               item, b.get('memo', ''))
    if err:
        return JSONResponse({'ok': False, 'error': err}, 400)
    # 직접 지운 종목을 다시 등록하면 연동 제외 목록에서 해제
    ign = _ignored() - {tk}
    db.meta_set('ignore_tickers', ','.join(sorted(ign)))
    return {'ok': True, 'id': pos['id'], 'position': pos, 'orders': order_sheet(pos)}


@app.post("/api/positions/{pid}/edit")
async def api_position_edit(pid: int, req: Request):
    b = await req.json()
    upd = {}
    for k in ('stop', 'target1', 'target2', 'memo', 'time_stop'):
        if k in b and b[k] not in (None, ''):
            upd[k] = b[k] if k == 'memo' else float(b[k])
    if 'stop' in upd:
        upd['cur_stop'] = upd['stop']
    p = db.get_position(pid)
    if p and upd:
        # 기준이 바뀐 알림은 다시 울릴 수 있게 해제
        al = set(__import__('json').loads(p['alerts'] or '[]'))
        if 'stop' in upd:
            al -= {'stop', 'near_stop'}
        if 'target1' in upd:
            al.discard('t1')
        if 'target2' in upd:
            al.discard('t2')
        upd['alerts'] = __import__('json').dumps(sorted(al))
    for k in ('stop', 'target1', 'target2'):
        if k in upd:
            upd[k] = strat.round_tick(upd[k], 'up' if k == 'stop' else 'down')
    if 'stop' in upd:
        upd['cur_stop'] = upd['stop']
    db.update_position(pid, **upd)
    pos = db.get_position(pid)
    if any(k in upd for k in ('stop', 'target1', 'target2')):
        tg_trade(order_sheet_text(pos, head='✏️ 주문 변경 — 앱 주문도 수정하세요'))
    return {'ok': True, 'position': pos}


@app.post("/api/positions/{pid}/sell")
async def api_position_sell(pid: int, req: Request):
    b = await req.json()
    p = db.sell_position(pid, float(b.get('price') or 0), int(b.get('qty') or 0))
    if not p:
        return JSONResponse({'ok': False, 'error': '매도 기록 실패 (수량 확인)'}, 400)
    return {'ok': True, 'position': p}


@app.post("/api/positions/{pid}/delete")
async def api_position_delete(pid: int):
    p = db.get_position(pid)
    if p and not p['closed']:
        # 보유 중인 종목을 지우면 잔고 연동이 다시 등록하지 않도록 제외 목록에 추가
        db.meta_set('ignore_tickers', ','.join(sorted(_ignored() | {p['ticker']})))
    db.conn().execute("DELETE FROM positions WHERE id=?", (pid,))
    db.conn().commit()
    return {'ok': True}


# ════════════════════════════════════════════
#  스케줄러
# ════════════════════════════════════════════
def _spawn(fn, *a):
    threading.Thread(target=fn, args=a, daemon=True).start()


def _last_close_time(now=None):
    """가장 최근 거래일 동기화 기준시각 (평일 15:50)"""
    now = now or datetime.now()
    d = now
    while True:
        cut = d.replace(hour=15, minute=50, second=0, microsecond=0)
        if d.weekday() < 5 and cut <= now:
            return cut
        d = (d - timedelta(days=1)).replace(hour=23, minute=59)


def catch_up():
    """서버를 꺼둔 사이 놓친 동기화 자동 보충"""
    time.sleep(8)
    if not (CFG['app_key'] and CFG['app_secret']):
        return
    last = db.meta_get('candles_synced', '')
    if not db.meta_get('universe_built', ''):
        return                      # 최초 구축 전 — 사용자가 '전체 구축'을 눌러야 함
    if not last or datetime.fromisoformat(last) < _last_close_time():
        print('[SCHED] 놓친 동기화 감지 → 증분 동기화 시작')
        job_build(False)
    try:
        if not db.bench_info()['candles']:          # v5.5 첫 실행 — 지수 ETF 비교선용 일봉
            print(f"[BENCH] 지수 ETF 일봉 받기: {db.sync_bench(CFG['app_key'], CFG['app_secret'])}", flush=True)
    except Exception as e:
        print(f'[BENCH] 지수 ETF 일봉 동기화 실패: {e}', flush=True)
    job_flows()                                   # 빠진 수급 날짜 보충 (계정 설정 시)
    job_virtual()                                 # 가상매매 갱신


# 평일 자동 일정
SCHEDULE = {
    '08:40': ('scan', 'swing'),     # 장전 — 전일 종가 기준 스윙 후보
    '08:50': ('orders', None),      # 오늘 걸어둘 매도 주문표 텔레그램
    '09:30': ('scan', 'short'),     # 장초반 30분 이후 — 로스 카메론 갭앤고 구간
    '10:30': ('scan', 'swing'),
    '11:00': ('scan', 'short'),
    '13:00': ('scan', 'swing'),
    '14:30': ('scan', 'short'),
    '15:50': ('build', False),      # 장마감 증분 동기화 (전종목 일봉·수급·프로필·DART)
    '16:40': ('scan', 'swing'),     # 종가 확정 후 익일 스윙 후보
    '08:35': ('at_morning', None),  # 자동매매: 전날 판정 매도 + 신규 매수 (시가 동시호가)
    '09:02': ('at_fill', None),     # DRY 실전 리허설: 시가 동시호가 결과(오늘 시가)로 체결 처리
    '09:15': ('at_fill', None),     # 〃 재확인 (거래정지 · 조회 실패분)
    '17:30': ('at_close', None),    # 자동매매 장마감 정리 보충 (동기화 뒤 이미 실행됐으면 같은 결과)
    '18:10': ('flows', None),       # KRX 연기금·외국인·기관 순매수 (당일 확정치)
    '18:20': ('scan', 'swing'),     # 연기금 반영된 익일 후보 → 실전 가상매매 기록
    '18:30': ('vtreport', None),    # (금요일) 실전 가상매매 주간 요약
}


CORE_SCANS = {'08:40', '18:20'}     # 가상매매 기록(18:20)과 놓친 날 아침 보충(08:40) — 항상 유지


def _active_schedule():
    """예전 기능이 꺼져 있으면: 단타 스캔·장중 스윙 스캔·수동 기록 주문표를 빼고 핵심 작업만"""
    if CFG.get('legacy_features'):
        return SCHEDULE
    out = {}
    for t, (kind, arg) in SCHEDULE.items():
        if kind == 'orders':
            continue
        if kind == 'scan' and (arg != 'swing' or t not in CORE_SCANS):
            continue
        out[t] = (kind, arg)
    return out


def _sched_tick(now):
    """한 분에 실행할 작업 결정 (테스트 가능하도록 분리)"""
    hm = now.strftime('%H:%M')
    wd = now.weekday()
    # 휴장일(공휴일·명절)은 스캔·주문표·추적 모두 쉼. 월요일 마스터 갱신만 예외
    if wd < 5 and not ext.is_trading_day(now, CFG['app_key'], CFG['app_secret']):
        if wd == 0 and hm == '07:30':
            _spawn(job_build, True)
        if wd == 4 and hm == '18:30':          # 금요일이 휴장일이어도 주간 보고는 발송
            _spawn(job_vt_report)
        return
    if wd == 0 and hm == '07:30':
        _spawn(job_build, True)
    elif wd < 5 and hm in _active_schedule():
        kind, arg = _active_schedule()[hm]
        if kind == 'build':
            _spawn(job_build, arg)
        elif kind == 'orders':
            _spawn(job_morning_orders)
        elif kind == 'flows':
            _spawn(job_flows)
        elif kind == 'at_morning':
            _spawn(job_at_morning)
        elif kind == 'at_close':
            _spawn(job_at_close)
        elif kind == 'at_fill':
            _spawn(job_at_fill)
        elif kind == 'vtreport':
            if wd == 4:
                _spawn(job_vt_report)
        else:
            _spawn(job_scan, arg, True)
    # 추적 점검(수동 기록 잔고 연동·단타 가상 추적)은 예전 기능을 켰을 때만
    if CFG.get('legacy_features') and wd < 5 and (('09:00' <= hm <= '15:30' and now.minute % 10 == 0) or hm == '16:05'):
        _spawn(job_track)


def scheduler():
    last = ''
    while True:
        try:
            now = datetime.now()
            key = now.strftime('%Y%m%d%H%M')
            if key != last:
                last = key
                _sched_tick(now)
        except Exception:
            traceback.print_exc()
        time.sleep(15)


def _quiet_windows():
    """윈도우에서 브라우저 연결이 끊길 때 나는 WinError 10054 소음 제거 (동작에는 영향 없음)"""
    import sys
    if sys.platform != 'win32':
        return
    try:
        from asyncio import proactor_events as pe
        orig = pe._ProactorBasePipeTransport._call_connection_lost

        def safe(self, exc):
            try:
                orig(self, exc)
            except (ConnectionResetError, ConnectionAbortedError, OSError):
                pass
        pe._ProactorBasePipeTransport._call_connection_lost = safe
    except Exception:
        pass


def _check_ws_lib():
    try:
        import websockets  # noqa
        return True
    except ImportError:
        try:
            import wsproto  # noqa
            return True
        except ImportError:
            print("\n  ⚠ websockets 부품이 없어 화면 실시간 갱신이 안 됩니다 (화면은 3초마다 자동 조회로 대신합니다)."
                  "\n    해결: 창을 닫고  python -m pip install websockets  실행 후 다시 시작\n", flush=True)
            return False


if __name__ == '__main__':
    _quiet_windows()
    _check_ws_lib()
    db.init_db()
    threading.Thread(target=scheduler, daemon=True).start()
    threading.Thread(target=catch_up, daemon=True).start()
    threading.Thread(target=rt_monitor, daemon=True).start()
    print(f"""
╔══════════════════════════════════════════╗
║   TK Stock Scout — 종목추천 엔진         ║
║   http://localhost:{PORT}                  ║
║   가상 검증 · 주문 기본 꺼짐             ║
╚══════════════════════════════════════════╝""")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")
