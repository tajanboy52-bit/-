"""
tk_journal.py — TK자동매매 거래 기록 (HTS급 · 분석 · 고도화용)

모든 것을 지우지 않고 쌓는다 — 모의 · 실전은 같은 표 구조로 각자 장부(trade_paper.db · trade_real.db)에 (분석은 둘을 합쳐서 봄)
· orders        주문 한 건 (보낸 시각 · 접수 시각 · 신호 기준가 · KIS 응답 코드/메시지)
· order_events  주문 상태가 바뀔 때마다 (보냄 → 접수 → 부분 → 체결 / 거절 · 취소 · 만료 · 불분명)
· fills         체결 조각 (늘어난 수량 × 그 조각의 가격 · 수수료 · 세금)
· ws_execs      웹소켓 체결 통보 원본 (체결 시각 초 단위)
· decisions     장전 계획 판단 — 산 것 · 미룬 것 · 못 산 것과 이유 (놓친 거래 분석용)
· lots          거래(매수 → 매도) + 진입 근거(순위 · 점수 · 지표) + 사후 계산(시가 대비 체결 차이 · 최대 역행/순행 · 모델 수익)
· positions_daily · account_daily  날마다 잔고 · 계좌 (HTS 잔고 · 매매일지)
· broker_pnl    실전: KIS 기간별 매매손익(TTTC8715R · HTS 0856 화면과 같은 값 · 실제 수수료 · 세금)
· api_daily     KIS 호출 수 · 오류 · 평균 응답 시간 (안정성 점검)
· market.db cands  날마다 신호 후보 상위 50 (거래 안 한 것 포함 → 순위별 사후 수익 · 신호 약화 감시)
"""
import json
import threading
import time
from datetime import datetime

import tk_db as db

RATES = {'fee': 0.0140527, 'tax': 0.20}          # % — 한국투자 비대면 수수료 · 증권거래세(2026: 코스피 0.05 + 농특세 0.15 · 코스닥 0.20) · 설정에서 바꿈
ETF_PREFIX = ('KODEX', 'TIGER', 'KBSTAR', 'RISE', 'ACE', 'SOL', 'HANARO', 'ARIRANG', 'KOSEF', 'PLUS', 'TIMEFOLIO', 'WON', '1Q', 'KIWOOM', 'BNK', 'HK')


def is_etf(name):
    n = (name or '').upper().strip()
    return any(n.startswith(p) for p in ETF_PREFIX)


def costs(side, amount, name=''):
    """→ (수수료, 세금) 원 — 증권사 방식대로 원 미만 버림 · ETF 매도는 거래세 없음"""
    fee = int(amount * RATES['fee'] / 100)
    tax = int(amount * RATES['tax'] / 100) if side == 'sell' and not is_etf(name) else 0
    return fee, tax


# ── 주문 상태 ──
def event(x, order_id, status, detail=''):
    x.execute('INSERT INTO order_events (order_id, ts, status, detail) VALUES (?,?,?,?)', (order_id, db.now_s(), status, str(detail or '')[:300]))


def fill(x, o, qty, price, src='sync'):
    """체결 조각 기록 → (수수료, 세금)"""
    amt = qty * price
    fee, tax = costs(o['side'], amt, o['name'])
    x.execute("""INSERT INTO fills (date, ts, order_id, order_no, lot_id, sleeve, ticker, name, side, kind, qty, price, amount, fee, tax, src)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (o['date'], db.now_s(), o['id'], o['order_no'], o['lot_id'], o['sleeve'], o['ticker'], o['name'], o['side'], o['kind'],
               int(qty), float(price), amt, fee, tax, src))
    return fee, tax


def decision(x, d, sig_date, o, action, reason=''):
    x.execute("""INSERT INTO decisions (date, ts, sig_date, sleeve, ticker, name, rank, score, ref, qty, amt, action, reason)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (d, db.now_s(), sig_date, o.get('sleeve'), o.get('ticker'), o.get('name'), o.get('rank'), o.get('score'), o.get('ref'),
               int(o.get('qty') or 0), float(o.get('amt') or 0), action, reason or o.get('skip') or ''))


def ws_exec(row):
    """웹소켓 체결 통보 한 건 (tk_ws가 부름)"""
    try:
        x = db.conn()
        x.execute('INSERT OR IGNORE INTO ws_execs VALUES (?,?,?,?,?,?,?)',
                  (db.now_s(), row.get('exec_time') or '', row.get('order_no') or '', row.get('ticker') or '', row.get('side') or '',
                   int(float(row.get('qty') or 0)), float(row.get('price') or 0)))
        x.commit()
    except Exception as e:
        db.log(f'체결 통보 기록 실패: {e}', 'warn')


# ── KIS 호출 통계 (tk_kis.HOOK) ──
_api = {}
_api_lock = threading.Lock()


def api_hit(env, tr, ms, err=''):
    k = (env, datetime.now().strftime('%Y%m%d'), tr)
    with _api_lock:
        a = _api.setdefault(k, [0, 0, 0.0, ''])
        a[0] += 1
        a[2] += ms
        if err:
            a[1] += 1
            a[3] = str(err)[:200]


def flush_api():
    with _api_lock:
        items = list(_api.items())
        _api.clear()
    for (env, d, tr), (n, err, ms, last) in items:
        try:
            x = db.conn(env)
            x.execute("""INSERT INTO api_daily (date, tr, n, err, ms, last_err) VALUES (?,?,?,?,?,?)
                         ON CONFLICT(date, tr) DO UPDATE SET n=n+excluded.n, err=err+excluded.err, ms=ms+excluded.ms,
                         last_err=CASE WHEN excluded.last_err!='' THEN excluded.last_err ELSE last_err END""", (d, tr, n, err, ms, last))
            x.commit()
        except Exception:
            pass


# ── 날마다 계좌 · 잔고 ──
def day_close(x, d, bal, broker_rows=None):
    """장 마감: 잔고 이력 · 매매일지 한 줄 (실전은 KIS 실제 수수료 · 세금으로)"""
    x.execute('DELETE FROM positions_daily WHERE date=?', (d,))
    x.executemany('INSERT OR REPLACE INTO positions_daily VALUES (?,?,?,?,?,?,?,?)',
                  [(d, p['ticker'], p.get('name'), p['qty'], p.get('avg'), p.get('price'), p.get('value'), p.get('pnl')) for p in bal['positions']])
    f = x.execute("""SELECT COALESCE(SUM(CASE WHEN side='buy' THEN amount END),0), COALESCE(SUM(CASE WHEN side='sell' THEN amount END),0),
                     COALESCE(SUM(fee),0), COALESCE(SUM(tax),0), COUNT(DISTINCT CASE WHEN side='buy' THEN order_id END),
                     COUNT(DISTINCT CASE WHEN side='sell' THEN order_id END) FROM fills WHERE date=?""", (d,)).fetchone()
    buy, sell, fee, tax, nb, ns = f
    note = '추정 수수료 · 세금'
    if broker_rows:
        x.execute('DELETE FROM broker_pnl WHERE date=?', (d,))
        x.executemany('INSERT OR REPLACE INTO broker_pnl VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                      [(r['date'], r['ticker'], r['name'], r['kind'], r['buy_qty'], r['buy_amt'], r['sell_qty'], r['sell_amt'], r['pnl'], r['fee'], r['tax'])
                       for r in broker_rows])
        fee, tax = sum(r['fee'] for r in broker_rows), sum(r['tax'] for r in broker_rows)
        note = 'KIS 실제 수수료 · 세금'
    rz = x.execute("SELECT COALESCE(SUM(pnl),0) FROM lots WHERE exit_date=? AND status='청산'", (d,)).fetchone()[0]
    stock = sum(p['value'] for p in bal['positions'])
    x.execute('INSERT OR REPLACE INTO account_daily VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
              (d, bal['cash'], bal['cash_d2'], bal['equity'], stock, buy, sell, fee, tax, rz, nb, ns, None, note))


# ── 사후 계산 (일봉이 들어온 뒤) ──
def enrich(m=None, limit=2000):
    """청산된 거래에 시가 대비 체결 차이 · 전날 종가 대비 갭 · 최대 역행/순행(MAE/MFE) · 모델 수익(백테스트 가정) 계산"""
    x = db.conn(m)
    todo = [dict(r) for r in x.execute("""SELECT * FROM lots WHERE status='청산' AND post_at IS NULL AND entry_date IS NOT NULL
                                          AND exit_date IS NOT NULL ORDER BY id LIMIT ?""", (limit,))]
    if not todo:
        return 0
    last = db.last_bar_day()
    todo = [l for l in todo if l['exit_date'] <= last]
    if not todo:
        return 0
    frm, to = min(l['entry_date'] for l in todo), max(l['exit_date'] for l in todo)
    raw = db.panel(frm, to, [l['ticker'] for l in todo], adjusted=False)
    n = 0
    for l in todo:
        t = l['ticker']
        try:
            O, H, L_, C = (raw[k][t] for k in ('open', 'high', 'low', 'close'))
        except (KeyError, TypeError):
            x.execute('UPDATE lots SET post_at=? WHERE id=?', (db.now_s(), l['id']))
            continue
        win = (O.index >= l['entry_date']) & (O.index <= l['exit_date'])
        o_in = _f(O.get(l['entry_date']))
        o_out = _f(O.get(l['exit_date']))
        buy_close = l['sleeve'] == 'ON'                                  # 밤사이는 종가에 사서 다음 날 시가에 팜
        ref_in = _f(C.get(l['entry_date'])) if buy_close else o_in
        slip_in = (l['entry_px'] / ref_in - 1) * 100 if ref_in and l['entry_px'] else None
        slip_out = (l['exit_px'] / o_out - 1) * 100 if o_out and l['exit_px'] else None
        gap_in = (o_in / l['sig_ref'] - 1) * 100 if o_in and l['sig_ref'] and not buy_close else None
        model = (o_out / ref_in - 1) * 100 if o_out and ref_in else None
        hi, lo = H[win].max(), L_[win].min()
        base = l['entry_px'] or ref_in
        mfe = (hi / base - 1) * 100 if base and hi == hi else None
        mae = (lo / base - 1) * 100 if base and lo == lo else None
        # 보유 중 권리락 · 분할이 있으면 원주가 비교가 틀어짐 → 그 거래는 MAE/MFE · 모델 수익을 비움
        chg = raw['chg'][t][win] if 'chg' in raw else None
        if chg is not None:
            prev = C.shift(1)[win]
            base_ = C[win] / (1 + chg / 100)
            if ((base_ / prev - 1).abs() > 0.02).any():
                mfe = mae = model = None
        x.execute('UPDATE lots SET slip_in=?, slip_out=?, gap_in=?, model_ret=?, mae=?, mfe=?, post_at=? WHERE id=?',
                  (_f(slip_in), _f(slip_out), _f(gap_in), _f(model), _f(mae), _f(mfe), db.now_s(), l['id']))
        n += 1
    x.commit()
    return n


def _f(v):
    try:
        v = float(v)
        return None if v != v or v in (float('inf'), float('-inf')) else round(v, 4)
    except (TypeError, ValueError):
        return None


# ── 신호 후보 기록 (market.db · 모드와 무관) ──
def save_cands(d, rows, src='live'):
    """rows: [(sleeve, ticker, rank, score, close, info_dict)]"""
    c = db.mconn()
    c.execute('DELETE FROM cands WHERE date=? AND src=?', (d, src))
    c.executemany('INSERT OR REPLACE INTO cands VALUES (?,?,?,?,?,?,?,?)',
                  [(d, s, t, int(k), _f(sc), _f(cl), json.dumps(info, ensure_ascii=False) if info else None, src) for s, t, k, sc, cl, info in rows])
    c.commit()
