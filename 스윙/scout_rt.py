"""
scout_rt.py — 실전형 가상매매 (v5.8)

백테스트형 가상매매(vtrades)와 **같은 매수 신호**를 실전처럼 운용해서 따로 기록한다 (증권사 주문은 없음).
  매도 기준 = **모델별로 정해 둔 매도 규칙** (기본값) — 실행만 실전처럼:
    익절 · 손절 % → 장중 KIS 현재가가 닿는 순간 그 가격으로 매도 (백테스트형: 종가 판정 → 다음날 시가)
    9EMA 복귀 · 최대 보유일 → 장 마감 직전(15:15) 현재가로 판정 → 그날 종가에 매도 (백테스트형: 다음날 시가)
    종가베팅 → 신호일 종가 매수 · 다음 거래일 시가 매도 (정의 그대로)
  매수 : 다음 거래일 09:00 실제 시가 (시가가 상한가면 못 산 것으로)
  설정값(모델 규칙 + 선택: 분할 1차 익절 · 트레일링)은 모델별로 언제든 바꿀 수 있음 — 거래마다 매수 시점 설정 저장
  서버가 장중에 꺼져 있던 날은 장마감에 일봉(시가·고가·저가)으로 보충하고 '(일봉 보충)'으로 표시
"""
import json
from datetime import datetime

import scout_db as db

MODELS = ('final', 'strategy', 'fdip', 'lvflow', 'lvhigh', 'rsi', 'candle', 'jongga', 'v62')
KEYS = ('tp', 'sl', 'ema', 'hold', 'tp1', 'tp1_ratio', 'trail')
EXTRA = {'tp1': 0.0, 'tp1_ratio': 50, 'trail': 0.0}                  # 모델 규칙에 없는 선택 항목 (기본 끔)
LIMITS = {'tp': (0, 100), 'sl': (-50, 0), 'ema': (0, 1), 'hold': (1, 60), 'tp1': (0, 50), 'tp1_ratio': (10, 100), 'trail': (0, 30)}
LABEL = {'tp': '익절 % (0=없음)', 'sl': '손절 % (0=없음)', 'ema': '9EMA 복귀 매도 (1=켬 · 0=끔)', 'hold': '최대 보유 거래일',
         'tp1': '분할 1차 익절 % (0=끔)', 'tp1_ratio': '1차 매도 비율 %', 'trail': '트레일링 % (1차 익절 뒤 · 0=끔)'}
MARKET = ('09:00', '15:19')          # 장중 감시 시간 (15:20부터는 종가 동시호가)
CLOSE_CHECK = '15:15'                # 9EMA · 보유일 판정 → 그날 종가 매도
V62_RULE = {'ema': False, 'sl': 0.20, 'tp': 0.40, 'hold': 20}
CLOSE_TAG = ' (종가 매도)'


def model_default(grp):
    """모델별로 정해 둔 매도 규칙 → 실전형 설정 형식"""
    r = V62_RULE if grp == 'v62' else db.rule_of(grp)
    d = {'tp': round((r['tp'] or 0) * 100, 2), 'sl': -round((r['sl'] or 0) * 100, 2), 'ema': 1 if r['ema'] else 0,
         'hold': int(r['hold'])}
    d.update(EXTRA)
    return d


DEFAULT = model_default('final')    # 화면 기본 표시용 (모델마다 model_default 사용)


def init():
    c = db.conn()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS rt_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT, grp TEXT, signal_date TEXT, ticker TEXT, name TEXT, rank INTEGER,
        signal_close REAL, status TEXT, entry_date TEXT, entry_price REAL, remain REAL DEFAULT 1.0,
        peak REAL, last_price REAL, last_ts TEXT, tp1_done INTEGER DEFAULT 0,
        sell_flag TEXT, sell_signal_date TEXT, fills TEXT DEFAULT '[]', exit_date TEXT, exit_price REAL,
        ret REAL, exit_reason TEXT, held INTEGER DEFAULT 0, settings TEXT, created TEXT,
        UNIQUE (grp, signal_date, ticker));
    CREATE TABLE IF NOT EXISTS rt_settings_log (ts TEXT, grp TEXT, settings TEXT);
    CREATE TABLE IF NOT EXISTS rt_monitor (day TEXT PRIMARY KEY, first_ts TEXT, last_ts TEXT, cycles INTEGER DEFAULT 0,
        errors INTEGER DEFAULT 0, max_tickers INTEGER DEFAULT 0, max_sec REAL DEFAULT 0, events INTEGER DEFAULT 0);
    """)
    c.commit()


# ════════════════════════════════════════════
#  설정
# ════════════════════════════════════════════
def _merge(grp, saved):
    s = model_default(grp)
    if saved and 'hold' in saved:                  # v5.8 첫 배포의 옛 형식(tp2 등)은 무시하고 모델 규칙으로
        s.update({k: v for k, v in saved.items() if k in KEYS})
    return s


def settings(cfg, grp):
    return _merge(grp, (cfg.get('rt_settings') or {}).get(grp))


def snap(t):
    """거래에 저장된 매수 시점 설정"""
    try:
        return _merge(t['grp'], json.loads(t['settings'] or '{}'))
    except Exception:
        return model_default(t['grp'])


def validate(new):
    """{키: 값} → (정리된 값, 오류문구)"""
    out = {}
    for k, v in new.items():
        if k not in KEYS:
            continue
        try:
            x = float(str(v).replace('%', '').replace('+', '').strip())
        except (TypeError, ValueError):
            return None, f'{LABEL[k]} 값이 숫자가 아닙니다'
        lo, hi = LIMITS[k]
        if not lo <= x <= hi:
            return None, f'{LABEL[k]}는 {lo:g} ~ {hi:g} 사이'
        out[k] = int(x) if k in ('ema', 'hold') else round(x, 2)
    return out, None


def set_settings(cfg, grp, new):
    """모델 하나(또는 'all')의 설정 변경 → cfg 갱신 + 이력. 호출한 쪽이 설정 파일 저장"""
    val, err = validate(new)
    if err:
        return err
    targets = tuple(g for g in MODELS if g != 'jongga') if grp == 'all' else (grp,)
    if grp == 'jongga':
        return '종가베팅은 정의(종가 매수 → 다음날 시가 매도) 그대로라 설정이 없습니다'
    if any(g not in MODELS for g in targets):
        return '알 수 없는 모델'
    init()
    rs = dict(cfg.get('rt_settings') or {})
    ts = datetime.now().isoformat(timespec='seconds')
    new_s = {}
    for g in targets:
        s = settings(cfg, g)
        s.update(val)
        if s['tp1'] and s['tp'] and s['tp'] <= s['tp1']:
            return '익절은 분할 1차 익절보다 커야 합니다 (분할을 끄려면 1차 익절 0)'
        new_s[g] = s
    for g, s in new_s.items():
        rs[g] = s
        db.conn().execute("INSERT INTO rt_settings_log VALUES(?,?,?)", (ts, g, json.dumps(s)))
    db.conn().commit()
    cfg['rt_settings'] = rs
    return None


def settings_text(s, grp=None):
    if isinstance(s, str):
        try:
            s = json.loads(s)
        except Exception:
            return '-'
    if grp:
        s = _merge(grp, s)
    if not s or 'hold' not in s:
        return '-'
    p = []
    if s.get('tp1'):
        p.append(f"분할 +{s['tp1']:g}%에서 {s['tp1_ratio']:g}%")
    if s.get('tp'):
        p.append(f"익절 +{s['tp']:g}%")
    if s.get('ema'):
        p.append('9EMA 복귀')
    p.append(f"최대 {s['hold']:g}일")
    p.append(f"손절 {s['sl']:g}%" if s.get('sl') else '손절 없음')
    if s.get('trail'):
        p.append(f"트레일 {s['trail']:g}%")
    return ' · '.join(p)


def is_model_rule(grp, s):
    d = model_default(grp)
    return all(float(s.get(k, 0) or 0) == float(d[k] or 0) for k in KEYS)


# ════════════════════════════════════════════
#  신호 가져오기 (백테스트형과 같은 신호)
# ════════════════════════════════════════════
def sync(cfg):
    """vtrades의 9개 모델 신호 → rt_trades (실전형 시작일 이후만)"""
    init()
    c = db.conn()
    start = db.meta_get('rt_start', '')
    if not start:
        start = db.meta_get('vt_last_batch', '') or datetime.now().strftime('%Y%m%d')
        db.meta_set('rt_start', start)
    n = 0
    now = datetime.now().isoformat(timespec='seconds')
    for v in [dict(r) for r in c.execute(
            f"SELECT grp, signal_date, ticker, name, rank, signal_close FROM vtrades WHERE signal_date>=? "
            f"AND grp IN ({','.join('?' * len(MODELS))})", (start, *MODELS))]:
        if c.execute("SELECT 1 FROM rt_trades WHERE grp=? AND signal_date=? AND ticker=?",
                     (v['grp'], v['signal_date'], v['ticker'])).fetchone():
            continue
        if v['grp'] in db.CLOSE_ENTRY and v['signal_close']:
            # 종가베팅: 신호일 종가 매수 → 다음 거래일 시가 매도가 정의 (장중 설정값 대상 아님)
            c.execute("""INSERT INTO rt_trades (grp,signal_date,ticker,name,rank,signal_close,status,entry_date,entry_price,
                         peak,last_price,sell_flag,sell_signal_date,settings,created) VALUES(?,?,?,?,?,?,'보유',?,?,?,?,?,?,?,?)""",
                      (v['grp'], v['signal_date'], v['ticker'], v['name'], v['rank'], v['signal_close'], v['signal_date'],
                       v['signal_close'], v['signal_close'], v['signal_close'], '다음날 시가', v['signal_date'],
                       json.dumps(settings(cfg, v['grp'])), now))
        else:
            c.execute("""INSERT INTO rt_trades (grp,signal_date,ticker,name,rank,signal_close,status,created)
                         VALUES(?,?,?,?,?,?,'대기',?)""",
                      (v['grp'], v['signal_date'], v['ticker'], v['name'], v['rank'], v['signal_close'], now))
        n += 1
    c.commit()
    return n


# ════════════════════════════════════════════
#  체결 기록
# ════════════════════════════════════════════
def _cost(grp):
    return getattr(db, 'RT_COST', {}).get(grp, db.VT_COST)


def _sell(c, t, frac, px, why, day, hm):
    """보유 t의 frac(원래 수량 대비 비율)을 px에 매도 → t 갱신 · 다 팔면 청산 확정"""
    frac = min(frac, t['remain'])
    if frac <= 1e-9:
        return None
    fills = json.loads(t['fills'] or '[]')
    fills.append({'d': day, 't': hm, 'px': px, 'frac': round(frac, 4), 'why': why})
    t['fills'] = json.dumps(fills, ensure_ascii=False)
    t['remain'] = round(t['remain'] - frac, 6)
    upd = {'fills': t['fills'], 'remain': t['remain']}
    msg = f"{t['name']} {why} {frac * 100:.0f}% @ {px:,.0f} ({(px / t['entry_price'] - 1) * 100:+.1f}%)"
    if t['remain'] <= 1e-6:
        tot = sum(f['frac'] for f in fills)
        avg = sum(f['px'] * f['frac'] for f in fills) / tot
        ret = sum(f['frac'] * (f['px'] / t['entry_price'] - 1) for f in fills) / tot * 100 - _cost(t['grp'])
        n = c.execute("SELECT COUNT(DISTINCT date) FROM candles WHERE ticker=? AND date>=? AND date<=?",
                      (t['ticker'], t['entry_date'] or day, day)).fetchone()[0] or 0
        if not c.execute("SELECT 1 FROM candles WHERE ticker=? AND date=?", (t['ticker'], day)).fetchone():
            n += 1                                        # 장중 청산 — 오늘 일봉은 아직 없음
        upd['held'] = max(1, n) if t['grp'] not in db.CLOSE_ENTRY else max(1, n - 1)
        upd.update(status='청산', remain=0, exit_date=day, exit_price=round(avg, 2), ret=round(ret, 3),
                   exit_reason=' → '.join(dict.fromkeys(f['why'] for f in fills)), sell_flag=None)
        t.update(upd)
        msg += f" · 청산 {ret:+.2f}%"
    c.execute(f"UPDATE rt_trades SET {', '.join(k + '=?' for k in upd)} WHERE id=?", (*upd.values(), t['id']))
    return msg


def _enter(c, cfg, t, px, day):
    s = settings(cfg, t['grp'])
    t.update(status='보유', entry_date=day, entry_price=px, peak=px, last_price=px, settings=json.dumps(s))
    c.execute("UPDATE rt_trades SET status='보유', entry_date=?, entry_price=?, peak=?, last_price=?, settings=? WHERE id=?",
              (day, px, px, px, t['settings'], t['id']))


def _intraday(c, t, px, day, hm, tag=''):
    """장중 가격 px 하나로 설정값 판정 (여러 조건이 한꺼번에 맞으면 순서대로)"""
    s = snap(t)
    out = []
    t['peak'] = max(t['peak'] or px, px)
    chg = (px / t['entry_price'] - 1) * 100
    if s.get('sl') and chg <= s['sl']:
        m = _sell(c, t, t['remain'], px, f"손절 {s['sl']:g}%{tag}", day, hm)
        return [m] if m else []
    if not t['tp1_done'] and s.get('tp1') and chg >= s['tp1']:
        m = _sell(c, t, s['tp1_ratio'] / 100.0, px, f"분할 익절 +{s['tp1']:g}%{tag}", day, hm)
        t['tp1_done'] = 1
        c.execute("UPDATE rt_trades SET tp1_done=1 WHERE id=?", (t['id'],))
        out.append(m)
    if t['status'] == '보유' and s.get('tp') and chg >= s['tp']:
        out.append(_sell(c, t, t['remain'], px, f"익절 +{s['tp']:g}%{tag}", day, hm))
    elif t['status'] == '보유' and t['tp1_done'] and s.get('trail') and px <= t['peak'] * (1 - s['trail'] / 100.0):
        out.append(_sell(c, t, t['remain'], px, f"트레일링 −{s['trail']:g}%{tag}", day, hm))
    return [m for m in out if m]


# ════════════════════════════════════════════
#  장중 한 바퀴 (서버 감시 스레드가 반복 호출)
# ════════════════════════════════════════════
def quote(cfg, ticker):
    tok = db.get_token(cfg['app_key'], cfg['app_secret'])
    r = db.kis_get("/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100",
                   {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker}, cfg['app_key'], cfg['app_secret'], tok)
    o = r.get('output') or {}

    def f(k):
        try:
            return float(str(o.get(k, 0) or 0).replace(',', ''))
        except (TypeError, ValueError):
            return 0.0
    return {'open': f('stck_oprc'), 'price': f('stck_prpr'), 'high': f('stck_hgpr'), 'low': f('stck_lwpr'),
            'upper': f('stck_mxpr'), 'lower': f('stck_llam'), 'halt': str(o.get('temp_stop_yn', 'N')).upper() == 'Y'}


def cycle(cfg, now=None, quote_fn=None):
    """보유 · 매수 대기 종목 시세 조회 → 매수(시가) · 전날 판정분 매도(시가) · 설정값 판정. 반환: 체결 문구"""
    now = now or datetime.now()
    hm = now.strftime('%H:%M')
    if not (MARKET[0] <= hm <= MARKET[1]):
        return []
    init()
    c = db.conn()
    day = now.strftime('%Y%m%d')
    hms = now.strftime('%H:%M:%S')
    qf = quote_fn or (lambda tk: quote(cfg, tk))
    rows = [dict(r) for r in c.execute("SELECT * FROM rt_trades WHERE status IN ('대기','보유') ORDER BY id")]
    rows = [t for t in rows if t['status'] == '보유' or t['signal_date'] < day]
    msgs, errs, seen = [], 0, {}
    for tk in dict.fromkeys(t['ticker'] for t in rows):
        try:
            seen[tk] = qf(tk)
        except Exception:
            errs += 1
    for t in rows:
        q = seen.get(t['ticker'])
        if not q or q.get('halt') or not q.get('open') or not q.get('price'):
            continue
        first = t['last_ts'] is None or t['last_ts'][:8] != day       # 오늘 처음 보는 가격 = 시가 기준
        if t['status'] == '대기':
            if q['upper'] and q['open'] >= q['upper']:
                c.execute("UPDATE rt_trades SET status='미체결', exit_reason='상한가 시가 — 매수 못 함', exit_date=? WHERE id=?",
                          (day, t['id']))
                msgs.append(f"[{t['grp']}] {t['name']} 매수 못 함 (상한가 시가)")
                continue
            _enter(c, cfg, t, q['open'], day)
            msgs.append(f"[{t['grp']}] {t['name']} 매수 @ {q['open']:,.0f} (시가)")
            first = True
        if t['sell_flag'] and (t['sell_signal_date'] or '') < day:
            px = q['open'] if first else q['price']
            if q['lower'] and px <= q['lower']:
                continue                                                 # 하한가 — 팔 수 없음, 다음 조회 때 재시도
            m = _sell(c, t, t['remain'], px, t['sell_flag'] + ('' if first else ' (시가 놓침 · 현재가)'), day, hms)
            if m:
                msgs.append(f"[{t['grp']}] {m}")
        elif t['status'] == '보유' and t['grp'] not in db.CLOSE_ENTRY:
            if first and t['entry_date'] != day:
                msgs += [f"[{t['grp']}] {m}" for m in _intraday(c, t, q['open'], day, '09:00:00', ' (시가)')]
            if t['status'] == '보유':
                msgs += [f"[{t['grp']}] {m}" for m in _intraday(c, t, q['price'], day, hms)]
        if t['status'] == '보유':
            c.execute("UPDATE rt_trades SET peak=?, last_price=?, last_ts=? WHERE id=?",
                      (t.get('peak') or q['price'], q['price'], day + ' ' + hms, t['id']))
        else:
            c.execute("UPDATE rt_trades SET last_price=?, last_ts=? WHERE id=?", (q['price'], day + ' ' + hms, t['id']))
    if hm >= CLOSE_CHECK and db.meta_get('rt_close_pass', '') != day:
        msgs += _close_pass(c, rows, seen, day, hms)
        db.meta_set('rt_close_pass', day)
    c.commit()
    db.meta_set('rt_last_cycle', json.dumps({'ts': now.isoformat(timespec='seconds'), 'tickers': len(seen),
                                             'errors': errs, 'rows': len(rows)}))
    # 날짜별 감시 가동 기록 (검증 데이터용)
    sec = (datetime.now() - now).total_seconds() if quote_fn is None else 0
    ts = now.isoformat(timespec='seconds')
    c.execute("""INSERT INTO rt_monitor (day, first_ts, last_ts, cycles, errors, max_tickers, max_sec, events)
                 VALUES(?,?,?,1,?,?,?,?) ON CONFLICT(day) DO UPDATE SET last_ts=excluded.last_ts, cycles=cycles+1,
                 errors=errors+excluded.errors, max_tickers=MAX(max_tickers, excluded.max_tickers),
                 max_sec=MAX(max_sec, excluded.max_sec), events=events+excluded.events""",
              (day, ts, ts, errs, len(seen), round(sec, 1), len(msgs)))
    c.commit()
    return msgs


# ════════════════════════════════════════════
#  장 마감 직전 (15:15) — 9EMA 복귀 · 최대 보유일 판정 → 그날 종가에 매도
# ════════════════════════════════════════════
def _ema9(cl):
    e = cl[0]
    for x in cl[1:]:
        e = x * 0.2 + e * 0.8
    return e


def _close_rule(t, closes_incl_today, held_today):
    """모델 규칙 중 종가 판정 항목 → 사유 또는 None"""
    s = snap(t)
    if s.get('ema') and closes_incl_today[-1] >= _ema9(closes_incl_today):
        return '9EMA 복귀'
    if s.get('hold') and held_today >= s['hold']:
        return f"{s['hold']:g}일 만기"
    return None


def _close_pass(c, rows, seen, day, hms):
    out = []
    for t in rows:
        if t['status'] != '보유' or t['grp'] in db.CLOSE_ENTRY or t['sell_flag']:
            continue
        q = seen.get(t['ticker'])
        if not q or not q.get('price'):
            continue
        cd = [x for x in db.load_candles(t['ticker'], 260) if x['date'] < day]
        if not cd:
            continue
        held = sum(1 for x in cd if x['date'] >= t['entry_date']) + 1
        why = _close_rule(t, [x['close'] for x in cd] + [q['price']], held)
        if why:
            c.execute("UPDATE rt_trades SET sell_flag=?, sell_signal_date=? WHERE id=?", (why + CLOSE_TAG, day, t['id']))
            t['sell_flag'], t['sell_signal_date'] = why + CLOSE_TAG, day
            out.append(f"[{t['grp']}] {t['name']} 오늘 종가 매도 — {why} ({hms[:5]} 판정)")
    return out


# ════════════════════════════════════════════
#  장마감 (일봉 동기화 뒤)
# ════════════════════════════════════════════

def _daily_fill(c, t, bar, today):
    """장중 감시가 없던 날의 보충: 시가(갭) → 저가(손절 먼저 가정 · 손절가 체결) → 고가(익절가 체결) → 종가(트레일링)"""
    tag = ' (일봉 보충)'
    out = []
    s = snap(t)
    ep = t['entry_price']
    if t['entry_date'] != today:
        out += _intraday(c, t, bar['open'], today, '09:00:00', tag)
    if t['status'] == '보유' and s.get('sl') and bar['low'] <= ep * (1 + s['sl'] / 100.0):
        out.append(_sell(c, t, t['remain'], round(ep * (1 + s['sl'] / 100.0), 2), f"손절 {s['sl']:g}%{tag}", today, '장중'))
    if t['status'] == '보유' and not t['tp1_done'] and s.get('tp1') and bar['high'] >= ep * (1 + s['tp1'] / 100.0):
        out.append(_sell(c, t, s['tp1_ratio'] / 100.0, round(ep * (1 + s['tp1'] / 100.0), 2), f"분할 익절 +{s['tp1']:g}%{tag}", today, '장중'))
        t['tp1_done'] = 1
        c.execute("UPDATE rt_trades SET tp1_done=1 WHERE id=?", (t['id'],))
    if t['status'] == '보유' and s.get('tp') and bar['high'] >= ep * (1 + s['tp'] / 100.0):
        out.append(_sell(c, t, t['remain'], round(ep * (1 + s['tp'] / 100.0), 2), f"익절 +{s['tp']:g}%{tag}", today, '장중'))
    if t['status'] == '보유':
        t['peak'] = max(t['peak'] or ep, bar['high'])
        c.execute("UPDATE rt_trades SET peak=? WHERE id=?", (t['peak'], t['id']))
        if t['tp1_done'] and s.get('trail') and bar['close'] <= t['peak'] * (1 - s['trail'] / 100.0):
            out.append(_sell(c, t, t['remain'], bar['close'], f"트레일링 −{s['trail']:g}%{tag}", today, '15:30:00'))
    return [m for m in out if m]


def after_close(cfg, today):
    """① 오늘 종가 매도분 체결 ② 감시를 못 한 날은 일봉으로 보충 ③ 종가 판정을 놓쳤으면 규칙 판정 → 다음날 시가 ④ 보유일 · 종가 갱신"""
    init()
    c = db.conn()
    msgs = []
    lc = json.loads(db.meta_get('rt_last_cycle', '') or '{}')
    watched = (lc.get('ts') or '')[:10].replace('-', '') == today
    close_done = db.meta_get('rt_close_pass', '') == today
    for t in [dict(r) for r in c.execute("SELECT * FROM rt_trades WHERE status IN ('대기','보유') ORDER BY id")]:
        cd = [x for x in db.load_candles(t['ticker'], 260) if x['date'] <= today]
        if not cd or cd[-1]['date'] != today:
            continue
        bar, prev = cd[-1], (cd[-2] if len(cd) > 1 else None)
        if t['status'] == '대기':
            if t['signal_date'] >= today:
                continue
            # 감시 스레드가 시가 매수를 못 한 경우 — 일봉 시가로 보충 (상한가 시가는 전일 종가 +29.5%로 판단)
            if prev and bar['open'] >= prev['close'] * 1.295:
                c.execute("UPDATE rt_trades SET status='미체결', exit_reason='상한가 시가 — 매수 못 함 (일봉 보충)', exit_date=? "
                          "WHERE id=?", (today, t['id']))
                continue
            _enter(c, cfg, t, bar['open'], today)
            msgs.append(f"[{t['grp']}] {t['name']} 매수 @ {bar['open']:,.0f} (일봉 보충)")
        if t['sell_flag'] and (t['sell_flag'] or '').endswith(CLOSE_TAG) and t['sell_signal_date'] == today:
            m = _sell(c, t, t['remain'], bar['close'], t['sell_flag'], today, '15:30:00')      # 15:15 판정 → 오늘 종가
            msgs += [f"[{t['grp']}] {m}"] if m else []
            continue
        if not watched and t['sell_flag'] and (t['sell_signal_date'] or '') < today and t['status'] == '보유':
            m = _sell(c, t, t['remain'], bar['open'], t['sell_flag'] + ' (일봉 보충)', today, '09:00:00')
            msgs += [f"[{t['grp']}] {m}"] if m else []
        elif t['status'] == '보유' and not watched and t['grp'] not in db.CLOSE_ENTRY:
            msgs += [f"[{t['grp']}] {m}" for m in _daily_fill(c, t, bar, today)]
        if t['status'] != '보유':
            continue
        held = sum(1 for x in cd if x['date'] >= t['entry_date'])
        cl = [x['close'] for x in cd]
        flag = t['sell_flag']
        if not flag and not close_done and t['grp'] not in db.CLOSE_ENTRY:
            # 15:15 판정을 못 한 날 (서버 꺼짐) — 확정 종가로 판정해 다음 거래일 시가 매도
            why = _close_rule(t, cl, held)
            if why:
                flag = why + ' (종가 판정 놓침 → 다음날 시가)'
                msgs.append(f"[{t['grp']}] {t['name']} 내일 시가 매도 예정 — {flag}")
        c.execute("UPDATE rt_trades SET held=?, last_price=?, sell_flag=?, sell_signal_date=? WHERE id=?",
                  (held, cl[-1], flag, t['sell_signal_date'] if t['sell_flag'] else (today if flag else None), t['id']))
    c.commit()
    return msgs


# ════════════════════════════════════════════
#  성과 (모델별 · 백테스트형과 짝 비교 · 설정별)
# ════════════════════════════════════════════
def _mean(a):
    return sum(a) / len(a) if a else None


def stats(cfg):
    init()
    c = db.conn()
    out = {}
    for g in MODELS:
        rows = [dict(r) for r in c.execute("SELECT * FROM rt_trades WHERE grp=?", (g,))]
        closed = [r for r in rows if r['status'] == '청산']
        opn = [r for r in rows if r['status'] == '보유']
        rets = [r['ret'] for r in closed]
        pairs = []
        for r in closed:
            v = c.execute("SELECT ret FROM vtrades WHERE grp=? AND signal_date=? AND ticker=? AND status='청산'",
                          (g, r['signal_date'], r['ticker'])).fetchone()
            if v and v[0] is not None:
                pairs.append((r['ret'], v[0]))
        by = {}
        for r in closed:
            by.setdefault(settings_text(r['settings'], g), []).append(r['ret'])
        unreal = [(r['last_price'] / r['entry_price'] - 1) * 100 * r['remain'] for r in opn if r['last_price'] and r['entry_price']]
        why = {}
        for r in closed:
            for f in json.loads(r['fills'] or '[]'):
                k = f['why'].split(' ')[0] if not f['why'].startswith('1차') and not f['why'].startswith('2차') else f['why'][:5]
                why[k] = why.get(k, 0) + 1
        out[g] = {'n': len(closed), 'open': len(opn), 'wait': sum(1 for r in rows if r['status'] == '대기'),
                  'miss': sum(1 for r in rows if r['status'] == '미체결'),
                  'win': round(sum(1 for x in rets if x > 0) / len(rets) * 100, 1) if rets else None,
                  'avg': round(_mean(rets), 3) if rets else None, 'sum': round(sum(rets), 2) if rets else 0,
                  'pairs': len(pairs), 'pair_rt': round(_mean([a for a, _ in pairs]), 3) if pairs else None,
                  'pair_bt': round(_mean([b for _, b in pairs]), 3) if pairs else None,
                  'unreal': round(_mean(unreal), 2) if unreal else None,
                  'by_settings': [{'settings': k, 'n': len(v), 'avg': round(_mean(v), 3)} for k, v in by.items()],
                  'why': why, 'settings': settings(cfg, g), 'settings_text': settings_text(settings(cfg, g)),
                  'model_rule': is_model_rule(g, settings(cfg, g)), 'model_rule_text': settings_text(model_default(g))}
    return out


def trades(g, limit=300):
    init()
    rows = [dict(r) for r in db.conn().execute(
        "SELECT * FROM rt_trades WHERE grp=? ORDER BY (status='청산'), signal_date DESC, rank LIMIT ?", (g, limit))]
    for r in rows:
        r['fills'] = json.loads(r['fills'] or '[]')
        r['settings_text'] = settings_text(r['settings'], g) if r['settings'] else '-'
        v = db.conn().execute("SELECT status, ret, exit_reason FROM vtrades WHERE grp=? AND signal_date=? AND ticker=?",
                              (g, r['signal_date'], r['ticker'])).fetchone()
        r['bt'] = dict(v) if v else None
    return rows


def settings_log(g, limit=30):
    init()
    return [{'ts': r[0], 'settings': settings_text(r[1], g)} for r in db.conn().execute(
        "SELECT ts, settings FROM rt_settings_log WHERE grp=? ORDER BY ts DESC LIMIT ?", (g, limit))]
