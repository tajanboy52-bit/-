"""
scout_autotrade.py — 실전 자동매매 (Scout 가상매매 검증 완료 후 사용)

모드
  OFF   : 아무것도 하지 않음 (기본값)
  DRY   : 실전 리허설 — 주문만 증권사에 보내지 않고 나머지는 실전과 같음. 08:35 주문 → 09:02 KIS 현재가로
          그날 실제 시가 체결 확인(상한가 시가 매수 · 하한가 시가 매도는 미체결) → 텔레그램 체결 알림
  LIVE  : 실전 주문. 검증 게이트 통과 + 확인 문구 입력 후에만 선택 가능

실행 규칙 (가상매매 최종 트랙과 동일한 종목 · 백테스트로 정한 실행 방식)
  매수: 신호 다음 거래일 08:35 시장가 → 시가 동시호가 체결
  매도: 장마감 후 종가 ≥ 9일 EMA 또는 보유 10거래일 → 다음 거래일 08:35 시장가 (시가 동시호가)
        (백테스트: 종가 매도보다 건당 +0.2%p 내외, 대조군도 같은 폭 → 실행상 유리하고 추정 오차 없음)

안전장치
  종목당 평가액의 1/at_slots · 주문당 상한 at_max_order_krw · 모델별 1회 최대 주문금액 model_order_max
  · 하루 최대 매수 at_max_daily_buys
  최대 보유 at_max_positions · 전일 대비 계좌 −at_daily_loss_stop% 이하면 신규 매수 중단
  같은 (모드·신호일·종목·방향) 주문은 한 번만 · 긴급 정지 = 미체결 취소 + OFF
"""
import json, time, urllib.request, urllib.parse
from datetime import datetime

import scout_db as db

URL = {'LIVE': 'https://openapi.koreainvestment.com:9443'}
TR = {'LIVE': {'buy': 'TTTC0802U', 'sell': 'TTTC0801U', 'cancel': 'TTTC0803U', 'ccld': 'TTTC8001R', 'bal': 'TTTC8434R'}}
MODES = ('OFF', 'DRY', 'LIVE')          # v5.6.1: 모의투자(PAPER) 제거 — 검증은 가상매매로만
LIVE_PHRASE = '실전 자동매매 시작'
BUY_WINDOW = ('08:30', '08:59')          # 시가 동시호가 참여 가능 시간
SELL_CATCHUP_END = '15:15'               # 아침을 놓친 매도는 장중 시장가로라도 (리스크 관리 우선)


# ════════════════════════════════════════════
#  저장소
# ════════════════════════════════════════════
def init():
    c = db.conn()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS at_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, mode TEXT, date TEXT, signal_date TEXT,
        ticker TEXT, name TEXT, side TEXT, qty INTEGER, status TEXT, odno TEXT, orgno TEXT,
        filled_qty INTEGER DEFAULT 0, avg_price REAL, reason TEXT, msg TEXT,
        UNIQUE (mode, signal_date, ticker, side));
    CREATE TABLE IF NOT EXISTS at_positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, mode TEXT, ticker TEXT, name TEXT, track TEXT, signal_date TEXT,
        entry_date TEXT, qty INTEGER, avg_price REAL, status TEXT, sell_flag TEXT, sell_signal_date TEXT,
        exit_date TEXT, exit_price REAL, exit_reason TEXT, held INTEGER DEFAULT 0, ret REAL);
    CREATE TABLE IF NOT EXISTS at_snap (mode TEXT, date TEXT, equity REAL, cash REAL, PRIMARY KEY (mode, date));
    CREATE TABLE IF NOT EXISTS at_log (ts TEXT, mode TEXT, level TEXT, msg TEXT);
    """)
    c.commit()


def log(mode, msg, level='info'):
    db.conn().execute("INSERT INTO at_log VALUES(?,?,?,?)", (datetime.now().isoformat(timespec='seconds'), mode, level, msg))
    db.conn().commit()
    print(f"[AT:{mode}] {msg}", flush=True)


# ════════════════════════════════════════════
#  KIS 통신 (LIVE는 Scout 기존 토큰 재사용 — 같은 앱키로 토큰을 두 번 발급받지 않음)
# ════════════════════════════════════════════
def _creds(mode, cfg):
    return cfg['app_key'], cfg['app_secret'], cfg.get('account_no', ''), cfg.get('account_cd', '01')


def _token(mode, cfg):
    k, s, _, _ = _creds(mode, cfg)
    return db.get_token(k, s)


def _headers(mode, cfg, tr_id):
    k, s, _, _ = _creds(mode, cfg)
    return {"Content-Type": "application/json; charset=utf-8", "authorization": f"Bearer {_token(mode, cfg)}",
            "appkey": k, "appsecret": s, "tr_id": tr_id, "custtype": "P"}


def http(method, mode, cfg, path, tr_id, params=None, body=None):
    """테스트에서 교체 가능한 단일 통신 함수"""
    url = URL[mode] + path + ('?' + urllib.parse.urlencode(params) if params else '')
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers=_headers(mode, cfg, tr_id), method=method)
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode())


def send_order(mode, cfg, side, ticker, qty):
    """시장가 주문. 반환: (성공, 주문번호, 주문조직번호, 메시지)"""
    if mode == 'DRY':
        return True, f"DRY{datetime.now():%H%M%S%f}", '', '가상 주문 (증권사 미전송)'
    _, _, acct, prod = _creds(mode, cfg)
    r = http('POST', mode, cfg, '/uapi/domestic-stock/v1/trading/order-cash', TR[mode][side],
             body={"CANO": acct, "ACNT_PRDT_CD": prod, "PDNO": ticker, "ORD_DVSN": "01",
                   "ORD_QTY": str(int(qty)), "ORD_UNPR": "0"})
    out = r.get('output') or {}
    return r.get('rt_cd') == '0', out.get('ODNO', ''), out.get('KRX_FWDG_ORD_ORGNO', ''), r.get('msg1', '')


def cancel_order(mode, cfg, orgno, odno):
    if mode == 'DRY':
        return True, '가상 취소'
    _, _, acct, prod = _creds(mode, cfg)
    r = http('POST', mode, cfg, '/uapi/domestic-stock/v1/trading/order-rvsecncl', TR[mode]['cancel'],
             body={"CANO": acct, "ACNT_PRDT_CD": prod, "KRX_FWDG_ORD_ORGNO": orgno, "ORGN_ODNO": odno,
                   "ORD_DVSN": "00", "RVSE_CNCL_DVSN_CD": "02", "ORD_QTY": "0", "ORD_UNPR": "0",
                   "QTY_ALL_ORD_YN": "Y"})
    return r.get('rt_cd') == '0', r.get('msg1', '')


def fetch_fills(mode, cfg, date):
    """당일 체결 → {주문번호: (체결수량, 평균가)}"""
    _, _, acct, prod = _creds(mode, cfg)
    r = http('GET', mode, cfg, '/uapi/domestic-stock/v1/trading/inquire-daily-ccld', TR[mode]['ccld'],
             params={"CANO": acct, "ACNT_PRDT_CD": prod, "INQR_STRT_DT": date, "INQR_END_DT": date,
                     "SLL_BUY_DVSN_CD": "00", "INQR_DVSN": "00", "PDNO": "", "CCLD_DVSN": "01", "ORD_GNO_BRNO": "",
                     "ODNO": "", "INQR_DVSN_3": "00", "INQR_DVSN_1": "", "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""})
    out = {}
    for it in r.get('output1') or []:
        q = int(float(it.get('tot_ccld_qty', 0) or 0))
        if q > 0:
            out[it.get('odno', '')] = (q, float(it.get('avg_prvs', 0) or 0))
    return out


def fetch_account(mode, cfg):
    """실전 계좌 잔고 → {'cash','total','pnl','holdings':[...]}"""
    _, _, acct, prod = _creds(mode, cfg)
    r = http('GET', mode, cfg, '/uapi/domestic-stock/v1/trading/inquire-balance', TR[mode]['bal'],
             params={"CANO": acct, "ACNT_PRDT_CD": prod, "AFHR_FLPR_YN": "N", "OFL_YN": "", "INQR_DVSN": "02",
                     "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "01",
                     "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""})
    if r.get('rt_cd') not in ('0', None):
        raise RuntimeError(r.get('msg1', '잔고 조회 실패'))
    hold = []
    for it in r.get('output1') or []:
        q = int(float(it.get('hldg_qty', 0) or 0))
        if q > 0:
            hold.append({'ticker': it.get('pdno', ''), 'name': it.get('prdt_name', ''), 'qty': q,
                         'avg': float(it.get('pchs_avg_pric', 0) or 0), 'price': float(it.get('prpr', 0) or 0),
                         'pnl': float(it.get('evlu_pfls_amt', 0) or 0), 'pnl_pct': float(it.get('evlu_pfls_rt', 0) or 0)})
    o2 = (r.get('output2') or [{}])[0]
    return {'cash': float(o2.get('dnca_tot_amt', 0) or 0), 'total': float(o2.get('tot_evlu_amt', 0) or 0),
            'pnl': float(o2.get('evlu_pfls_smtl_amt', 0) or 0), 'holdings': hold}


# ════════════════════════════════════════════
#  검증 게이트
# ════════════════════════════════════════════
def gate(track, judge):
    """LIVE 허용 조건 — 가상매매 검증 완료"""
    j = judge.get(track) or {}
    reasons = []
    if (j.get('n') or 0) < 60:
        reasons.append(f"청산 {j.get('n') or 0}/60건")
    if (j.get('days') or 0) < 40:
        reasons.append(f"신호일 {j.get('days') or 0}/40거래일")
    if j.get('repro') != '백테스트와 비슷':
        reasons.append(f"재현성: {j.get('repro') or '표본 부족'}")
    if j.get('verdict') == '탈락':
        reasons.append('대조군 비교 탈락')
    return {'pass': not reasons, 'reasons': reasons, 'verdict': j.get('verdict'), 'repro': j.get('repro')}


# ════════════════════════════════════════════
#  계좌 평가 (한도 계산용)
# ════════════════════════════════════════════
def _last_close(ticker):
    r = db.conn().execute("SELECT close FROM candles WHERE ticker=? ORDER BY date DESC LIMIT 1", (ticker,)).fetchone()
    return r[0] if r else None


def capital_limit(mode, cfg):
    """계좌별 운용 금액 (0이면 계좌 전체). DRY는 장부 시작 금액 자체가 운용 금액"""
    return float(cfg.get('at_capital_live', 0) or 0) if mode == 'LIVE' else 0.0


def invested_cost(mode):
    """자동매매로 보유 중인 종목의 매수 원금 합계"""
    r = db.conn().execute("SELECT SUM(qty * avg_price) FROM at_positions WHERE mode=? AND status='보유'", (mode,)).fetchone()
    return float(r[0] or 0)


def equity(mode, cfg):
    """(평가액, 현금) — DRY는 장부 기준(초기 at_dry_cash), LIVE는 증권사 잔고"""
    if mode == 'DRY':
        c = db.conn()
        cash = float(cfg.get('at_dry_cash', 10_000_000))
        for side, q, px in c.execute("SELECT side, filled_qty, avg_price FROM at_orders WHERE mode='DRY' AND filled_qty>0"):
            cash += (-1 if side == 'buy' else 1) * q * px * (1 + (0.00035 if side == 'buy' else -0.00215))
        mv = 0.0
        for tk, q in c.execute("SELECT ticker, qty FROM at_positions WHERE mode='DRY' AND status='보유'"):
            mv += q * (_last_close(tk) or 0)
        return cash + mv, cash
    a = fetch_account(mode, cfg)
    return a['total'], a['cash']


# ════════════════════════════════════════════
#  아침 실행 (08:35) — 전날 판정된 매도 → 신규 매수
# ════════════════════════════════════════════
def morning(cfg, now=None):
    now = now or datetime.now()
    mode = cfg.get('at_mode', 'OFF')
    if mode not in ('DRY', 'LIVE'):
        return []
    init()
    c = db.conn()
    today = now.strftime('%Y%m%d')
    hm = now.strftime('%H:%M')
    out = []
    # ① 매도 (전날 종가 기준 판정분)
    if hm <= SELL_CATCHUP_END:
        for p in [dict(r) for r in c.execute("SELECT * FROM at_positions WHERE mode=? AND status='보유' AND sell_flag IS NOT NULL",
                                             (mode,))]:
            out.append(_order(mode, cfg, 'sell', p['ticker'], p['name'], p['qty'], p['sell_signal_date'], today,
                              p['sell_flag'], now))
    # ② 매수 (시가 동시호가 시간에만 — 놓치면 그날은 건너뜀: 장중 가격으로 사면 검증한 규칙과 달라짐)
    if not (BUY_WINDOW[0] <= hm <= BUY_WINDOW[1]):
        if hm > BUY_WINDOW[1]:
            log(mode, f'시가 동시호가 시간({BUY_WINDOW[0]}~{BUY_WINDOW[1]})이 지나 오늘 신규 매수는 건너뜀')
        return out
    sig = db.meta_get('vt_last_batch', '')
    if not sig or sig >= today:
        log(mode, '매수할 신호가 없음 (어제 18:20 가상매매 기록 확인)')
        return out
    track = cfg.get('at_track', 'final')
    picks = [dict(r) for r in c.execute("SELECT ticker, name, signal_close FROM vtrades WHERE grp=? AND signal_date=? "
                                        "ORDER BY rank", (track, sig))]
    held = {r[0] for r in c.execute("SELECT ticker FROM at_positions WHERE mode=? AND status='보유'", (mode,))}
    n_hold = len(held)
    try:
        eq, cash = equity(mode, cfg)
    except Exception as e:
        log(mode, f'계좌 조회 실패로 매수 중단: {e}', 'error')
        return out
    prev = c.execute("SELECT equity FROM at_snap WHERE mode=? AND date<? ORDER BY date DESC LIMIT 1", (mode, today)).fetchone()
    if prev and prev[0] and (eq / prev[0] - 1) * 100 <= -float(cfg.get('at_daily_loss_stop', 3.0)):
        log(mode, f"계좌가 전일 대비 {(eq / prev[0] - 1) * 100:.1f}% → 손실 한도로 오늘 신규 매수 중단", 'warn')
        return out
    cap = capital_limit(mode, cfg)
    base = min(eq, cap) if cap > 0 else eq                           # 운용 금액이 정해져 있으면 그 안에서만
    mcap = float((cfg.get('model_order_max') or {}).get(track) or 0)   # 모델별 1회 최대 주문금액 (0 = 제한 없음)
    size = min(base / int(cfg.get('at_slots', 30)), float(cfg.get('at_max_order_krw', 1_000_000)),
               mcap if mcap > 0 else float('inf'))
    room = (cap - invested_cost(mode)) if cap > 0 else float('inf')    # 운용 금액 중 남은 한도
    bought = 0
    for p in picks:
        if bought >= int(cfg.get('at_max_daily_buys', 3)):
            break
        if p['ticker'] in held:
            continue
        if n_hold + bought >= int(cfg.get('at_max_positions', 15)):
            log(mode, f"최대 보유 {cfg.get('at_max_positions', 15)}종목 도달 — 추가 매수 중단")
            break
        ref = p['signal_close'] or _last_close(p['ticker'])
        if not ref:
            continue
        qty = int(min(size, cash * 0.98, room) // (ref * 1.03))      # 시가 갭 상승 대비 3% 여유
        if qty <= 0 or qty * ref < 50_000:
            log(mode, f"{p['name']} 매수 건너뜀 — " + ('운용 금액 한도 도달' if room < size else '매수 금액 부족'))
            continue
        out.append(_order(mode, cfg, 'buy', p['ticker'], p['name'], qty, sig, today, f'{track} 트랙 신호', now))
        cash -= qty * ref * 1.03
        room -= qty * ref * 1.03
        bought += 1
    return out


# ════════════════════════════════════════════
#  DRY 실전 리허설 — 장 시작 직후 실제 시가로 체결 (증권사 주문만 없음)
# ════════════════════════════════════════════
def quote(cfg, ticker):
    """KIS 현재가 → 오늘 시가 · 상한가 · 하한가 · 거래정지 (테스트에서 교체 가능)"""
    tok = db.get_token(cfg['app_key'], cfg['app_secret'])
    r = db.kis_get("/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100",
                   {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker}, cfg['app_key'], cfg['app_secret'], tok)
    o = r.get('output') or {}

    def f(k):
        try:
            return float(str(o.get(k, 0) or 0).replace(',', ''))
        except (TypeError, ValueError):
            return 0.0
    return {'open': f('stck_oprc'), 'price': f('stck_prpr'), 'upper': f('stck_mxpr'), 'lower': f('stck_llam'),
            'halt': str(o.get('temp_stop_yn', 'N')).upper() == 'Y'}


def _limit_block(side, px, upper, lower):
    """상한가 시가 매수 · 하한가 시가 매도는 체결 안 된 것으로 (실전에선 잔량이 쌓여 거의 못 삼/못 팖)"""
    if side == 'buy' and upper and px >= upper:
        return '상한가 시가 — 매수 못 한 것으로 처리'
    if side == 'sell' and lower and px <= lower:
        return '하한가 시가 — 매도 못 한 것으로 처리'
    return None


def _apply_fill(c, mode, cfg, o, q, px, today):
    """체결 1건 반영 → 보유/청산 장부 갱신. 반환: 알림 문구"""
    c.execute("UPDATE at_orders SET status='체결', filled_qty=?, avg_price=? WHERE id=?", (q, px, o['id']))
    if o['side'] == 'buy':
        c.execute("""INSERT INTO at_positions (mode,ticker,name,track,signal_date,entry_date,qty,avg_price,status,held)
                     VALUES(?,?,?,?,?,?,?,?,'보유',0)""",
                  (mode, o['ticker'], o['name'], cfg.get('at_track', 'final'), o['signal_date'], today, q, px))
        return f"매수 체결: {o['name']} {q}주 @ {px:,.0f}"
    p = c.execute("SELECT * FROM at_positions WHERE mode=? AND ticker=? AND status='보유' ORDER BY id LIMIT 1",
                  (mode, o['ticker'])).fetchone()
    if not p:
        return f"매도 체결: {o['name']} {q}주 @ {px:,.0f} (장부에 보유 없음)"
    ret = (px / p['avg_price'] - 1) * 100 - 0.25
    remain = p['qty'] - q
    if remain > 0:
        c.execute("UPDATE at_positions SET qty=? WHERE id=?", (remain, p['id']))
    else:
        c.execute("""UPDATE at_positions SET status='청산', exit_date=?, exit_price=?, exit_reason=?, ret=?,
                     sell_flag=NULL WHERE id=?""", (today, px, p['sell_flag'] or o['reason'], round(ret, 3), p['id']))
    return f"매도 체결: {o['name']} {q}주 @ {px:,.0f} ({ret:+.2f}%)"


def dry_fill(cfg, now=None, quote_fn=None):
    """DRY 리허설: 09:00 시가 동시호가가 끝난 뒤 KIS 현재가의 '오늘 시가'로 접수 주문을 체결 처리"""
    now = now or datetime.now()
    if cfg.get('at_mode') != 'DRY' or now.strftime('%H:%M') < '09:01':
        return []
    init()
    c = db.conn()
    today = now.strftime('%Y%m%d')
    qf = quote_fn or (lambda tk: quote(cfg, tk))
    msgs = []
    for o in [dict(r) for r in c.execute("SELECT * FROM at_orders WHERE mode='DRY' AND date=? AND status='접수'", (today,))]:
        try:
            qt = qf(o['ticker'])
        except Exception as e:
            log('DRY', f"{o['name']} 시세 조회 실패 — 다음 확인 때 재시도: {e}", 'warn')
            continue
        if qt.get('halt') or not qt.get('open'):
            log('DRY', f"{o['name']} 아직 시가 없음(거래정지 등) — 다음 확인 때 재시도")
            continue
        late = (o.get('ts') or '')[11:16] >= '09:00'      # 장 시작 뒤 낸 주문(서버를 늦게 켬) → 실전처럼 그때 현재가
        px = (qt.get('price') or qt['open']) if late else qt['open']
        why = _limit_block(o['side'], px, qt.get('upper'), qt.get('lower'))
        if why:
            c.execute("UPDATE at_orders SET status='미체결', msg=? WHERE id=?", (why, o['id']))
            msgs.append(f"미체결: {o['name']} {'매수' if o['side'] == 'buy' else '매도'} — {why}")
            continue
        msgs.append(_apply_fill(c, 'DRY', cfg, o, o['qty'], px, today) + (' (장중 늦은 주문 · 현재가)' if late else ''))
    c.commit()
    for m in msgs:
        log('DRY', m)
    return msgs


def _order(mode, cfg, side, ticker, name, qty, signal_date, today, reason, now=None):
    c = db.conn()
    prev = c.execute("SELECT id, status FROM at_orders WHERE mode=? AND signal_date=? AND ticker=? AND side=?",
                     (mode, signal_date, ticker, side)).fetchone()
    if prev and prev['status'] in ('접수', '체결'):
        return f"중복 주문 방지: {name} {side} (이미 주문함)"
    if prev:
        # 거부(휴장일·통신 오류)·미체결·취소된 주문은 재시도 허용 — 기록은 실행 로그에 남음
        log(mode, f"{name} {side} 이전 주문({prev['status']}) → 재주문")
        c.execute("DELETE FROM at_orders WHERE id=?", (prev['id'],))
    try:
        ok, odno, orgno, msg = send_order(mode, cfg, side, ticker, qty)
    except Exception as e:
        ok, odno, orgno, msg = False, '', '', f'통신 오류: {e}'
    c.execute("""INSERT INTO at_orders (ts,mode,date,signal_date,ticker,name,side,qty,status,odno,orgno,reason,msg)
                 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              ((now or datetime.now()).isoformat(timespec='seconds'), mode, today, signal_date, ticker, name, side, qty,
               '접수' if ok else '실패', odno, orgno, reason, msg))
    c.commit()
    txt = f"{'매수' if side == 'buy' else '매도'} {name} {qty}주 — {'접수' if ok else '실패: ' + msg}"
    log(mode, txt, 'info' if ok else 'error')
    return txt


# ════════════════════════════════════════════
#  장마감 후 (동기화 뒤) — 체결 반영 → 매도 판정 → 계좌 스냅샷
# ════════════════════════════════════════════
def _ema9(cl):
    e = cl[0]
    for x in cl[1:]:
        e = x * 0.2 + e * 0.8
    return e


def after_close(cfg, today=None):
    mode = cfg.get('at_mode', 'OFF')
    if mode not in ('DRY', 'LIVE'):
        return []
    init()
    c = db.conn()
    today = today or datetime.now().strftime('%Y%m%d')
    msgs = []
    # ① 체결 반영
    orders = [dict(r) for r in c.execute("SELECT * FROM at_orders WHERE mode=? AND date=? AND status='접수'", (mode, today))]
    fills = {}
    if orders and mode != 'DRY':
        try:
            fills = fetch_fills(mode, cfg, today)
        except Exception as e:
            log(mode, f'체결 조회 실패 — 다음 실행 때 재시도: {e}', 'error')
            return msgs
    for o in orders:
        why = None
        if mode == 'DRY':
            # 아침 체결 확인(09:02)을 놓친 주문 — 오늘 일봉 시가로 (상한가 · 하한가 시가는 전일 종가 ±29.5%로 판단)
            col = 'close' if (o.get('ts') or '')[11:16] >= '09:00' else 'open'     # 늦은 주문은 보수적으로 종가
            r = c.execute(f"SELECT {col} FROM candles WHERE ticker=? AND date=?", (o['ticker'], today)).fetchone()
            pc = c.execute("SELECT close FROM candles WHERE ticker=? AND date<? ORDER BY date DESC LIMIT 1",
                           (o['ticker'], today)).fetchone()
            q, px = (o['qty'], r[0]) if r and r[0] else (0, None)
            if q and pc and pc[0]:
                why = _limit_block(o['side'], px, pc[0] * 1.295, pc[0] * 0.705)
        else:
            q, px = fills.get(o['odno'], (0, None))
        if q <= 0 or why:
            c.execute("UPDATE at_orders SET status='미체결', msg=COALESCE(?, msg) WHERE id=?", (why, o['id']))
            msgs.append(f"미체결: {o['name']} {o['side']}" + (f" — {why}" if why else ''))
            continue
        msgs.append(_apply_fill(c, mode, cfg, o, q, px, today))
    c.commit()
    # ② 매도 판정 (오늘 종가 기준 → 내일 시가 매도)
    for p in [dict(r) for r in c.execute("SELECT * FROM at_positions WHERE mode=? AND status='보유'", (mode,))]:
        cd = db.load_candles(p['ticker'], 250)
        if not cd or cd[-1]['date'] != today:
            continue
        held = sum(1 for x in cd if x['date'] >= p['entry_date'])
        cl = [x['close'] for x in cd]
        flag = None
        if p.get('sell_flag'):
            # 이미 매도 신호가 났는데 아직 안 팔림(미체결·일부체결·주문실패) → 조건이 사라져도 팔릴 때까지 매일 재주문
            flag = p['sell_flag']
            msgs.append(f"매도 재시도(내일 시가): {p['name']} — {flag} · 전일 미체결")
        else:
            # 모델별 확정 청산 규칙 (가상매매와 동일 · 변경 불가) — 종가 판정 → 다음 거래일 시가 매도
            rule = db.rule_of(p.get('track') or cfg.get('at_track', 'final'))
            chg = cl[-1] / p['avg_price'] - 1 if p['avg_price'] else 0
            if rule['sl'] and chg <= -rule['sl']:
                flag = f"손절 −{rule['sl'] * 100:g}%"
            elif rule['tp'] and chg >= rule['tp']:
                flag = f"익절 +{rule['tp'] * 100:g}%"
            elif rule['ema'] and cl[-1] >= _ema9(cl):
                flag = '9EMA 복귀'
            elif held >= rule['hold']:
                flag = f"{rule['hold']}일 만기"
        c.execute("UPDATE at_positions SET held=?, sell_flag=?, sell_signal_date=? WHERE id=?",
                  (held, flag, today if flag else None, p['id']))
        if flag and not p.get('sell_flag'):
            msgs.append(f"매도 예정(내일 시가): {p['name']} — {flag}")
    c.commit()
    # ③ 장부 대조 (LIVE): Scout 기록 vs 증권사 실제 잔고 — 수량이 다르거나 한쪽에만 있으면 경고
    if mode == 'LIVE':
        try:
            acct = fetch_account(mode, cfg)
            broker = {h['ticker']: h['qty'] for h in acct['holdings']}
            mine = {}
            for tk, q in c.execute("SELECT ticker, SUM(qty) FROM at_positions WHERE mode=? AND status='보유' GROUP BY ticker", (mode,)):
                mine[tk] = q
            diffs = []
            for tk in sorted(set(broker) | set(mine)):
                b, m = broker.get(tk, 0), mine.get(tk, 0)
                if b != m and not (mode == 'LIVE' and m == 0):       # 실전 계좌의 수동 보유 종목은 제외
                    diffs.append(f"{tk}: 기록 {m}주 / 증권사 {b}주")
            if diffs:
                msgs.append('⚠️ 장부 불일치 — ' + ' · '.join(diffs[:8]))
                log(mode, '장부 불일치: ' + ' · '.join(diffs), 'warn')
            else:
                log(mode, f"장부 대조 정상 ({len(mine)}종목)")
        except Exception as e:
            log(mode, f'장부 대조 실패: {e}', 'warn')
    # ④ 계좌 스냅샷 (다음날 손실 한도 계산용)
    try:
        eq, cash = equity(mode, cfg)
        c.execute("INSERT OR REPLACE INTO at_snap VALUES(?,?,?,?)", (mode, today, eq, cash))
        c.commit()
    except Exception as e:
        log(mode, f'계좌 스냅샷 실패: {e}', 'warn')
    for m in msgs:
        log(mode, m)
    return msgs


def kill(cfg):
    """긴급 정지: 오늘 미체결 주문 취소 → 모드 OFF (호출한 쪽이 설정 저장)"""
    mode = cfg.get('at_mode', 'OFF')
    init()
    c = db.conn()
    today = datetime.now().strftime('%Y%m%d')
    res = []
    for o in [dict(r) for r in c.execute("SELECT * FROM at_orders WHERE mode=? AND date=? AND status='접수'", (mode, today))]:
        try:
            ok, msg = cancel_order(mode, cfg, o['orgno'], o['odno']) if mode in ('LIVE', 'DRY') else (True, '')
        except Exception as e:
            ok, msg = False, str(e)
        c.execute("UPDATE at_orders SET status=?, msg=? WHERE id=?", ('취소' if ok else '취소실패', msg, o['id']))
        res.append(f"{o['name']} {o['side']} 취소 {'성공' if ok else '실패: ' + msg}")
    c.commit()
    log(mode, '긴급 정지 실행 — ' + (', '.join(res) or '미체결 주문 없음'), 'warn')
    return res


def owned_tickers():
    """자동매매가 보유 중인 종목 (LIVE) — 수동 기록의 잔고 자동 등록에서 제외"""
    try:
        init()
        return {r[0] for r in db.conn().execute("SELECT ticker FROM at_positions WHERE mode='LIVE' AND status='보유'")}
    except Exception:
        return set()
