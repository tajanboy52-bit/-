"""
danta_live.py — 🎯 TK Danta 급등주 탐지 + 가상 단타매매 (실전처럼 · 주문 없음)

기존 v8 자동매매의 단타 흐름을 GPT 없이 규칙으로:
  08:55  프리마켓 스냅샷 (거래량 · 거래대금 · 거래증가율 순위) — 기록만
  09:01~10:00  1분마다 급등 후보 탐지 → 매수 조건을 넘으면 '가상 매수' (실제 매도호가 + 슬리피지)
  보유 중  10초마다 현재가 → 익절 · 손절 · 트레일링 · 보유 시간 · 오전 마감 · 15:15 안전 청산 → '가상 매도' (매수호가 − 슬리피지)
설정값은 화면에서 바꿀 수 있고(이력 저장) 거래마다 매수 시점 설정을 같이 기록 — 설정별 성과 비교
탐지 스냅샷은 매분 전부 저장 → 나중에 '그때 알 수 있던 정보'만으로 조건을 다시 검증 (미래 정보 없음)
"""
import json
import threading
import time
from datetime import datetime

import danta_db as db
import danta_kis as kis
import danta_exit as ex
import danta_models as mdl

DEFAULT = {
    # 탐지 (v8과 같은 기본 필터)
    'price_min': 1000, 'price_max': 50000, 'chg_min': 3.0, 'chg_max': 25.0, 'vol_min': 10000, 'amt_min_eok': 10,
    'scan_start': '09:01', 'scan_end': '10:00',
    # 매수
    'buy_chg_min': 5.0, 'near_high': 2.0, 'above_open': 2.0, 'max_buys': 5, 'max_hold': 3, 'amount': 1000000,
    # 매도
    'tp': 3.0, 'sl': -2.0, 'trail_start': 2.0, 'trail_gap': 1.5, 'hold_min': 30, 'exit_by': '11:00',
    'hold_days': 0, 'carry_limit': 1,           # D0.3: 보유 거래일(0=그날 청산) · 상한가 오버나잇(일봉 16만 건 근거)
    'slip': 0.1,
}
INT_KEYS = ('price_min', 'price_max', 'vol_min', 'max_buys', 'max_hold', 'amount', 'hold_min', 'hold_days', 'carry_limit')
LABEL = {'price_min': '가격 하한(원)', 'price_max': '가격 상한(원)', 'chg_min': '등락률 하한(%)', 'chg_max': '등락률 상한(%)',
         'vol_min': '누적 거래량 하한(주)', 'amt_min_eok': '누적 거래대금 하한(억)', 'scan_start': '탐지 시작', 'scan_end': '탐지 끝(신규 매수 마감)',
         'buy_chg_min': '매수: 등락률 이상(%)', 'near_high': '매수: 당일 고가에서 이내(%)', 'above_open': '매수: 시가보다 이상(%)',
         'max_buys': '하루 최대 매수(종목)', 'max_hold': '동시 보유(종목)', 'amount': '1회 매수 금액(원)',
         'tp': '익절(%)', 'sl': '손절(%)', 'trail_start': '트레일링 시작(%)', 'trail_gap': '트레일링 고점 대비(%)',
         'hold_min': '최대 보유(분 · 0=없음 · 그날 청산일 때만)', 'exit_by': '청산 시각 (마지막 날 · 09:00=시가 매도)',
         'hold_days': '보유 거래일 (0=그날 청산 · 1 · 2)', 'carry_limit': '상한가면 안 팔고 다음날 시가 (1=예 · 0=아니오)',
         'slip': '슬리피지(%)'}
TIME_KEYS = ('scan_start', 'scan_end', 'exit_by')
COST = 0.25                 # 왕복 비용 %(세금 · 수수료) — 슬리피지는 체결가에 따로 반영
STATE = {'last_scan': None, 'last_watch': None, 'cands': [], 'err': None, 'preview': False, 'day': '', 'hist': {}, 'orb': {}, 'prev': {}}


def init():
    db.conn().executescript("""
    CREATE TABLE IF NOT EXISTS snaps (ts TEXT, date TEXT, hm TEXT, ticker TEXT, name TEXT, src TEXT, price REAL, chg REAL,
        vol INTEGER, amt REAL, open REAL, high REAL, passed INTEGER, buy INTEGER, why TEXT);
    CREATE INDEX IF NOT EXISTS idx_snaps ON snaps(date, hm);
    CREATE TABLE IF NOT EXISTS vtrades (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, ticker TEXT, name TEXT,
        buy_ts TEXT, buy_px REAL, qty INTEGER, sig_px REAL, sig_chg REAL, sig_amt REAL, status TEXT, peak REAL, last_px REAL,
        sell_ts TEXT, sell_px REAL, ret REAL, pnl REAL, reason TEXT, settings TEXT, src TEXT, pre INTEGER);
    CREATE TABLE IF NOT EXISTS settings_log (ts TEXT, settings TEXT);
    CREATE TABLE IF NOT EXISTS vexits (trade_id INTEGER, prof TEXT, status TEXT, peak REAL, plan TEXT, chk TEXT,
        sell_ts TEXT, sell_px REAL, ret REAL, pnl REAL, reason TEXT, PRIMARY KEY (trade_id, prof));
    """)
    have = {r[1] for r in db.conn().execute("PRAGMA table_info(vtrades)")}
    for col in ('plan', 'chk'):                            # D0.2에서 만든 표에 열 추가
        if col not in have:
            db.conn().execute(f"ALTER TABLE vtrades ADD COLUMN {col} TEXT")
    if 'model' not in have:                                # D0.5: 모델별 가상 매매 (예전 기록은 M1 추격 돌파)
        db.conn().execute("ALTER TABLE vtrades ADD COLUMN model TEXT DEFAULT 'M1'")
        db.conn().execute("UPDATE vtrades SET model='M1' WHERE model IS NULL")
    have = {r[1] for r in db.conn().execute("PRAGMA table_info(snaps)")}
    for col, typ in (('low', 'REAL'), ('vwap', 'REAL'), ('models', 'TEXT'), ('bought', 'TEXT')):
        if col not in have:
            db.conn().execute(f"ALTER TABLE snaps ADD COLUMN {col} {typ}")
    db.conn().commit()


def settings(cfg):
    s = dict(DEFAULT)
    s.update({k: v for k, v in (cfg.get('live') or {}).items() if k in DEFAULT})
    return s


def set_settings(cfg, new):
    s = settings(cfg)
    for k, v in new.items():
        if k not in DEFAULT or v in (None, ''):
            continue
        if k in TIME_KEYS:
            v = str(v).strip()
            lo, hi = ('09:00', '15:15') if k == 'exit_by' else ('08:00', '15:20')
            if len(v) != 5 or v[2] != ':' or not (lo <= v <= hi):
                return f'{LABEL[k]}은 HH:MM ({lo}~{hi})'
            s[k] = v
            continue
        try:
            x = float(str(v).replace(',', ''))
        except ValueError:
            return f'{LABEL[k]} 값이 숫자가 아닙니다'
        s[k] = int(x) if k in INT_KEYS else x
    if s['sl'] >= 0 or s['tp'] < 0:
        return '손절은 − 값, 익절은 + 값(0=익절 없음)이어야 합니다'
    if s['hold_days'] not in (0, 1, 2) or s['carry_limit'] not in (0, 1):
        return '보유 거래일은 0 · 1 · 2, 상한가 오버나잇은 0 · 1'
    if not s['scan_start'] < s['scan_end']:
        return '탐지 시작 < 탐지 끝'
    if s['hold_days'] == 0 and not s['scan_end'] <= s['exit_by']:
        return '그날 청산이면 탐지 끝 ≤ 청산 시각'
    init()
    cfg['live'] = s
    db.conn().execute("INSERT INTO settings_log VALUES(?,?)", (datetime.now().isoformat(timespec='seconds'), json.dumps(s)))
    db.conn().commit()
    return None


# ════════════════════════════════════════════
#  KIS 조회
# ════════════════════════════════════════════
def _f(x):
    try:
        return float(str(x).replace(',', '') or 0)
    except (TypeError, ValueError):
        return 0.0


def ranks(cfg):
    """거래량 · 거래대금 · 거래증가율 순위 (v8에서 실전으로 쓰던 volume-rank 호출과 같은 형식) → {ticker: dict}"""
    out = {}
    for code, src in (('0', '거래량'), ('3', '거래대금'), ('1', '거래증가율')):
        try:
            r = kis.get(cfg, '/uapi/domestic-stock/v1/quotations/volume-rank', 'FHPST01710000',
                        {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_COND_SCR_DIV_CODE': '20171', 'FID_INPUT_ISCD': '0000',
                         'FID_DIV_CLS_CODE': '1', 'FID_BLNG_CLS_CODE': code, 'FID_TRGT_CLS_CODE': '111111111',
                         'FID_TRGT_EXLS_CLS_CODE': '000000', 'FID_INPUT_PRICE_1': '0', 'FID_INPUT_PRICE_2': '0',
                         'FID_VOL_CNT': '0', 'FID_INPUT_DATE_1': ''})
        except Exception as e:
            STATE['err'] = f'순위 조회 실패: {e}'
            continue
        for x in (r.get('output') or [])[:30]:
            tk = x.get('mksc_shrn_iscd', '')
            if not tk:
                continue
            d = out.setdefault(tk, {'ticker': tk, 'name': x.get('hts_kor_isnm', ''), 'src': []})
            d['src'].append(src)
            d.update(price=_f(x.get('stck_prpr')), chg=_f(x.get('prdy_ctrt')), vol=int(_f(x.get('acml_vol'))),
                     amt=_f(x.get('acml_tr_pbmn')))
    return out


def limit_ranks(cfg):
    """등락률 상위 30 (KIS 등락률 순위) 중 +20% 이상 → {ticker: dict} — D0.6
       거래량 · 거래대금 · 거래증가율 순위에 안 들어온 상한가 종목도 M7 · M9가 볼 수 있게 (상한가 매수 모델 시간에만 호출)"""
    out = {}
    try:
        r = kis.get(cfg, '/uapi/domestic-stock/v1/ranking/fluctuation', 'FHPST01700000',
                    {'fid_cond_mrkt_div_code': 'J', 'fid_cond_scr_div_code': '20170', 'fid_input_iscd': '0000',
                     'fid_rank_sort_cls_code': '0', 'fid_input_cnt_1': '0', 'fid_prc_cls_code': '0', 'fid_input_price_1': '',
                     'fid_input_price_2': '', 'fid_vol_cnt': '', 'fid_trgt_cls_code': '0', 'fid_trgt_exls_cls_code': '0',
                     'fid_div_cls_code': '0', 'fid_rsfl_rate1': '', 'fid_rsfl_rate2': ''})
    except Exception as e:
        STATE['err'] = f'등락률 순위 조회 실패: {e}'[:200]
        return out
    rows = r.get('output') or []
    if not rows and r.get('rt_cd') not in (None, '0'):
        STATE['err'] = f"등락률 순위 응답 오류: {r.get('msg1', '')}"[:200]
    for x in rows[:30]:
        tk = x.get('stck_shrn_iscd', '') or x.get('mksc_shrn_iscd', '')
        chg = _f(x.get('prdy_ctrt'))
        if not tk or chg < 20:
            continue
        price, vol = _f(x.get('stck_prpr')), int(_f(x.get('acml_vol')))
        amt = _f(x.get('acml_tr_pbmn')) or price * vol                  # 거래대금 없으면 현재가 × 거래량으로 어림 (관찰 종목 고르기용)
        out[tk] = {'ticker': tk, 'name': x.get('hts_kor_isnm', ''), 'src': ['등락률'], 'price': price, 'chg': chg, 'vol': vol, 'amt': amt}
    return out


def quote(cfg, ticker):
    r = kis.get(cfg, '/uapi/domestic-stock/v1/quotations/inquire-price', 'FHKST01010100',
                {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': ticker})
    o = r.get('output') or {}
    return {'price': _f(o.get('stck_prpr')), 'open': _f(o.get('stck_oprc')), 'high': _f(o.get('stck_hgpr')),
            'low': _f(o.get('stck_lwpr')), 'chg': _f(o.get('prdy_ctrt')), 'amt': _f(o.get('acml_tr_pbmn')),
            'vol': int(_f(o.get('acml_vol'))), 'upper': _f(o.get('stck_mxpr')), 'lower': _f(o.get('stck_llam')),
            'halt': str(o.get('temp_stop_yn', 'N')).upper() == 'Y', 'diff': _f(o.get('prdy_vrss')),
            'vrate': _f(o.get('prdy_vrss_vol_rate'))}


def hoga_full(cfg, ticker):
    """매도1 · 매수1 호가 + 매도1 잔량 (상한가 체결 가능 여부)"""
    try:
        r = kis.get(cfg, '/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn', 'FHKST01010200',
                    {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': ticker})
        o = r.get('output1') or {}
        return _f(o.get('askp1')), _f(o.get('bidp1')), _f(o.get('askp_rsqn1'))
    except Exception:
        return 0.0, 0.0, None


def hoga(cfg, ticker):
    """매도1 · 매수1 호가 — 가상 체결을 실전에 가깝게 (실패하면 현재가 ± 슬리피지)"""
    try:
        r = kis.get(cfg, '/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn', 'FHKST01010200',
                    {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': ticker})
        o = r.get('output1') or {}
        return _f(o.get('askp1')), _f(o.get('bidp1'))
    except Exception:
        return 0.0, 0.0


# ════════════════════════════════════════════
#  탐지 · 가상 매수 (1분마다)
# ════════════════════════════════════════════
def _now():
    return datetime.now()


def market_open(cfg, d, now=None):
    """오늘 장이 열렸는지 — 삼성전자 당일 분봉의 날짜로 확인 (휴장일엔 순위 API가 전 거래일 자료를 돌려줘서 가짜 매수가 생기므로)
       '1' 열림 · '0' 휴장 · '' 아직 모름(봉이 아직 없음 → 잠시 뒤 다시)"""
    v = db.meta_get('open_' + d, '')
    if v:
        return v
    now = now or _now()
    r = kis.get(cfg, '/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice', 'FHKST03010200',
                {'FID_ETC_CLS_CODE': '', 'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': '005930',
                 'FID_INPUT_HOUR_1': now.strftime('%H%M%S'), 'FID_PW_DATA_INCU_YN': 'N'})
    ds = {str(x.get('stck_bsop_date', '')) for x in (r.get('output2') or []) if x.get('stck_prpr')}
    if not ds:
        return ''
    v = '1' if d in ds else '0'
    db.meta_set('open_' + d, v)
    if v == '0':                                           # 휴장 — 장전 스냅샷은 전 거래일 자료였으니 지움
        db.conn().execute("DELETE FROM snaps WHERE date=?", (d,))
        db.conn().commit()
    return v


def _day_reset(d):
    """날짜가 바뀌면 분별 기록 초기화 · 서버를 다시 켰으면 오늘 스냅샷으로 복원"""
    if STATE['day'] == d:
        return
    STATE.update(day=d, hist={}, orb={}, prev={})
    c = db.conn()
    for tk, hm, price, high in c.execute("SELECT ticker, hm, price, high FROM snaps WHERE date=? AND hm>='09:00' ORDER BY ts", (d,)):
        if price:
            h = STATE['hist'].setdefault(tk, [])
            if h and h[-1][0] == hm:
                h[-1] = (hm, price)
            else:
                h.append((hm, price))
        if high and hm <= '09:05':
            STATE['orb'][tk] = max(STATE['orb'].get(tk, 0), high)


def _prev(tickers, d):
    """전일 등락률 · 전일 거래량 — 같은 PC Scout 일봉(읽기 전용) · 없으면 None"""
    need = [t for t in tickers if t not in STATE['prev']]
    if need:
        got = db.prev_info(need, d)
        for t in need:
            STATE['prev'][t] = got.get(t, (None, None))
    return STATE['prev']


def scan(cfg, now=None, premarket=False, preview=False):
    """매분: 순위 3종 → 관찰 종목 시세 → 모델 9개 신호 → 모델별 가상 매수 (+ 대조군). 스냅샷 전부 저장
       premarket: 장전 기록만 · preview: 화면 '지금 후보 보기' — 매수도 기록도 안 함"""
    init()
    now = now or _now()
    s = settings(cfg)
    d, hm = now.strftime('%Y%m%d'), now.strftime('%H:%M')
    c = db.conn()
    _day_reset(d)
    rk = ranks(cfg)
    lim_on = not premarket and any(preview or mdl.in_window(m, hm, s) for m in mdl.LIMIT_UP)
    if lim_on:                                                          # D0.6: 상한가 매수 모델 시간 → 등락률 순위도 합침
        for tk, x in limit_ranks(cfg).items():
            if tk in rk:
                rk[tk]['src'].append('등락률')
            else:
                rk[tk] = x
    pre = {r[0] for r in c.execute("SELECT DISTINCT ticker FROM snaps WHERE date=? AND hm<'09:00'", (d,))}
    ts = now.isoformat(timespec='seconds')
    cands, msgs = [], []
    for tk, x in rk.items():
        why = ['ETF·우선주 등'] if db.excluded(tk, x['name']) else []
        why += mdl.m1_filter({'price': x['price'], 'chg': x['chg'], 'cum_vol': x['vol'], 'cum_amt': x['amt']}, s)
        x.update(passed=not why, why=' · '.join(why), buy=0, pre=int(tk in pre), open=None, high=None, low=None, vwap=None,
                 models=[], bought=[], excl=db.excluded(tk, x['name']))
        cands.append(x)
    cands.sort(key=lambda x: (-x['passed'], -x['chg']))
    if premarket:
        _save_snaps(c, cands, ts, d, hm)
        STATE.update(last_scan=ts, cands=cands[:60], preview=False)
        return msgs
    active = [m for m in mdl.ORDER if m != 'Z' and (preview or mdl.in_window(m, hm, s))]
    # 관찰 종목: 가격 범위 · 거래대금 5억 이상 · 보통주 — 거래대금 순 최대 45 (필터 통과 종목은 항상)
    watch = [x for x in cands if not x['excl'] and s['price_min'] * 0.7 <= x['price'] <= s['price_max'] and x['amt'] >= mdl.BASE_AMT * 0.5]
    # D0.6: 상한가 매수 모델 시간엔 +29% 이상(상한가 근처)을 먼저 — 필터(+25% 이하)를 못 통과해 45개 밖으로 밀리지 않게
    watch.sort(key=lambda x: (-(lim_on and x['chg'] >= 29), -x['passed'], -x['amt']))
    watch = watch[:45]
    prev = _prev([x['ticker'] for x in watch], d)
    sts = {}
    for x in watch:
        tk = x['ticker']
        try:
            q = quote(cfg, tk)
        except Exception as e:
            x['why'] = f'시세 실패 {e}'[:60]
            continue
        p = q['price'] or x['price']
        if hm <= '09:05' and q['high']:
            STATE['orb'][tk] = max(STATE['orb'].get(tk, 0), q['high'])
        hist = STATE['hist'].setdefault(tk, [])
        past = [h for h in hist if h[0] < hm]
        pc = p - q['diff'] if q.get('diff') and p - q['diff'] > 0 else (p / (1 + q['chg'] / 100) if q['chg'] > -100 else 0)
        pchg, pvol = prev.get(tk, (None, None))
        if not pvol and q.get('vrate'):
            pvol = q['vol'] / (q['vrate'] / 100) if q['vrate'] > 0 else None
        st = {'hm': hm, 'price': p, 'open': q['open'], 'high': q['high'], 'low': q['low'], 'pc': pc, 'chg': q['chg'],
              'cum_vol': q['vol'], 'cum_amt': q['amt'], 'vwap': q['amt'] / q['vol'] if q['vol'] else None,
              'orb_high': STATE['orb'].get(tk) if hm > '09:05' else None, 'prev_vol': pvol, 'prev_chg': pchg,
              'upper': q['upper'], 'halted': q['halt'], 'hist': past[-15:], 'ask_upper': None}
        if any(m in active for m in mdl.LIMIT_UP) and q['upper'] and p >= q['upper']:
            ask, _, aq = hoga_full(cfg, tk)
            st['ask_upper'] = None if aq is None else bool(ask and ask <= q['upper'] and aq > 0)
        if not preview:
            if hist and hist[-1][0] == hm:
                hist[-1] = (hm, p)
            else:
                hist.append((hm, p))
        sts[tk] = st
        x.update(open=q['open'], high=q['high'], low=q['low'], price=p, chg=q['chg'], vwap=st['vwap'])
        if x['passed']:
            ok, w = mdl.signal('M1', st, s)
            x['why'] = '매수 조건 충족' if ok else w
    by = {x['ticker']: x for x in cands}
    sig = {m: [] for m in active}
    for tk, st in sts.items():
        for m in active:
            ok, w = mdl.signal(m, st, s)
            if ok:
                sig[m].append(dict(st, ticker=tk, why=w))
                by[tk]['models'].append(m)
    if not preview:
        m1_buys = []
        for m in active:
            held = {r[0] for r in c.execute("SELECT ticker FROM vtrades WHERE date=? AND model=?", (d, m))}
            n_buys = c.execute("SELECT COUNT(*) FROM vtrades WHERE date=? AND model=?", (d, m)).fetchone()[0]
            n_open = c.execute("SELECT COUNT(*) FROM vtrades WHERE status='보유' AND model=?", (m,)).fetchone()[0]
            for st in mdl.rank(m, sig[m]):
                tk = st['ticker']
                if tk in held:
                    continue
                if n_buys >= s['max_buys'] or n_open >= s['max_hold']:
                    by[tk]['why'] = (by[tk]['why'] + f' · {m} 한도').strip(' ·')
                    break
                r = _vbuy(cfg, c, m, by[tk], st, s, d, ts, hm)
                if r:
                    msgs.append(r)
                    held.add(tk)
                    n_buys += 1
                    n_open += 1
                    if m == 'M1':
                        m1_buys.append(tk)
        if m1_buys and 'Z' in mdl.ORDER:                   # 대조군 — M1이 산 그 분, 필터 통과 다른 후보 무작위 1
            zheld = {r[0] for r in c.execute("SELECT ticker FROM vtrades WHERE date=? AND model='Z'", (d,))}
            pool = [dict(sts[x['ticker']], ticker=x['ticker']) for x in cands if x['passed'] and x['ticker'] in sts]
            for tk in m1_buys:
                z = mdl.pick_control(pool, set(m1_buys) | zheld)
                if not z:
                    break
                r = _vbuy(cfg, c, 'Z', by[z['ticker']], z, s, d, ts, hm)
                if r:
                    msgs.append(r)
                    zheld.add(z['ticker'])
        for x in cands:
            if x['bought']:
                x['buy'] = 1
                x['why'] = '가상 매수 ' + ' · '.join(x['bought'])
            elif x['models'] and not x['why'].startswith('매수 조건'):
                x['why'] = ('신호 ' + ' · '.join(x['models']) + (' · ' + x['why'] if x['why'] else '')).strip()
        _save_snaps(c, cands, ts, d, hm)
    else:
        for x in cands:
            if x['models']:
                x['why'] = '신호 ' + ' · '.join(x['models']) + ' (미리보기)'
    STATE.update(last_scan=ts, cands=cands[:60], preview=preview, err=None if rk else STATE['err'])
    return msgs


def _vbuy(cfg, c, m, x, st, s, d, ts, hm):
    """가상 매수 1건 — 매도1호가 + 슬리피지 (상한가 매수는 상한가 그대로) · 매도 프로필 A~E 동시 기록 행 추가"""
    ask, _ = hoga(cfg, x['ticker'])
    px = (ask or st['price']) * (1 + s['slip'] / 100)
    if st.get('upper'):
        px = min(px, st['upper'])
    qty = int(s['amount'] // px)
    if qty <= 0:
        return None
    rules = dict(s)
    rules.update(mdl.exit_rule(m, s))
    c.execute("""INSERT INTO vtrades (date,ticker,name,buy_ts,buy_px,qty,sig_px,sig_chg,sig_amt,status,peak,last_px,settings,src,pre,model)
                 VALUES(?,?,?,?,?,?,?,?,?,'보유',?,?,?,?,?,?)""",
              (d, x['ticker'], x['name'], ts, round(px, 2), qty, st['price'], st['chg'], st.get('cum_amt') or x.get('amt'), px, st['price'],
               json.dumps(rules), ','.join(x['src']), x['pre'], m))
    tid = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    c.executemany("INSERT OR IGNORE INTO vexits (trade_id, prof, status, peak, plan) VALUES(?,?,'보유',?,'')",
                  [(tid, k, px) for k in ex.PROFILES])
    x['bought'].append(m)
    return f"🟢 [{m} {mdl.MODELS[m]['name']}] 가상 매수 {x['name']} {qty}주 @ {px:,.0f} ({st['chg']:+.1f}% · {hm})"


def _save_snaps(c, cands, ts, d, hm):
    c.executemany("INSERT INTO snaps (ts,date,hm,ticker,name,src,price,chg,vol,amt,open,high,passed,buy,why,low,vwap,models,bought) "
                  "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  [(ts, d, hm, x['ticker'], x['name'], ','.join(x['src']), x['price'], x['chg'], x['vol'], x['amt'],
                    x.get('open'), x.get('high'), int(x['passed']), int(bool(x.get('buy'))), x['why'], x.get('low'), x.get('vwap'),
                    ','.join(x.get('models') or []), ','.join(x.get('bought') or [])) for x in cands])
    c.commit()


def last_cands(d):
    """서버를 다시 켠 뒤에도 화면에 오늘 마지막 탐지 결과를 보여 주기 위해"""
    c = db.conn()
    r = c.execute("SELECT MAX(ts) FROM snaps WHERE date=?", (d,)).fetchone()
    if not r or not r[0]:
        return None, []
    rows = [dict(x) for x in c.execute("SELECT * FROM snaps WHERE ts=? ORDER BY passed DESC, buy DESC, chg DESC", (r[0],))]
    pre = {x[0] for x in c.execute("SELECT DISTINCT ticker FROM snaps WHERE date=? AND hm<'09:00'", (d,))}
    for x in rows:
        x['src'] = (x['src'] or '').split(',')
        x['pre'] = int(x['ticker'] in pre)
        x['models'] = [m for m in (x.get('models') or '').split(',') if m]
        x['bought'] = [m for m in (x.get('bought') or '').split(',') if m]
    return r[0], rows


# ════════════════════════════════════════════
#  보유 감시 · 가상 매도 (10초마다)
# ════════════════════════════════════════════
def _ret(t, px):
    ret = (px / t['buy_px'] - 1) * 100 - COST
    return ret, t['qty'] * t['buy_px'] * ret / 100


def _sell(c, t, px, why, ts):
    ret, pnl = _ret(t, px)
    c.execute("UPDATE vtrades SET status='청산', sell_ts=?, sell_px=?, ret=?, pnl=?, reason=?, plan='' WHERE id=?",
              (ts, round(px, 2), round(ret, 3), round(pnl), why, t['id']))
    return f"{'🔴' if ret < 0 else '💰'} 가상 매도 {t['name']} @ {px:,.0f} ({ret:+.2f}%) — {why}"


def holidays():
    return {r[0][5:] for r in db.conn().execute("SELECT k FROM meta WHERE k LIKE 'open_%' AND v='0'")}


def _pos(t, rules, row=None):
    """매도 엔진에 넘길 보유 정보 (규칙 + 매수 + 진행 상태)"""
    p = {k: rules[k] for k in ex.PKEYS}
    p.update(buy_px=t['buy_px'], buy_ts=t['buy_ts'], peak=(row or t)['peak'], plan=(row or t).get('plan') or '')
    return p


def watch(cfg, now=None, quote_fn=None, hoga_fn=None):
    """보유 가상 종목 10초 감시 — 현재 설정(매수 당시 규칙) + 비교 프로필 A~E를 같은 시세로 동시에 판단"""
    init()
    now = now or _now()
    s_now = settings(cfg)
    d, ts = now.strftime('%Y%m%d'), now.isoformat(timespec='seconds')
    c = db.conn()
    qf, hf = quote_fn or (lambda tk: quote(cfg, tk)), hoga_fn or (lambda tk: hoga(cfg, tk))
    hol = holidays()
    main = [dict(r) for r in c.execute("SELECT * FROM vtrades WHERE status='보유'")]
    shadow = [dict(r) for r in c.execute("SELECT x.*, t.ticker, t.name, t.buy_px, t.buy_ts, t.qty, t.date, t.settings "
                                         "FROM vexits x JOIN vtrades t ON t.id=x.trade_id WHERE x.status='보유'")]
    Q, B = {}, {}

    def q_of(tk):
        if tk not in Q:
            try:
                Q[tk] = qf(tk)
            except Exception:
                Q[tk] = None
        return Q[tk]

    def fill(tk, px, p, s, q):
        """판단 가격 → 가상 체결가. 시가(동시호가) 판단이면 시가 그대로, 아니면 매수1호가 − 슬리피지"""
        if abs(px - p) > 1e-9:
            return px * (1 - s['slip'] / 100)
        if tk not in B:
            B[tk] = hf(tk)[1]
        return (B[tk] or p) * (1 - s['slip'] / 100)

    msgs = []
    for t in main:
        s = dict(s_now)
        s.update(json.loads(t['settings'] or '{}'))
        q = q_of(t['ticker'])
        if not q or not q['price']:
            continue
        p = q['price']
        td = ex.trading_days(t['date'], d, hol)
        act, px, why, plan = ex.decide(_pos(t, s), p, now, q, td, first=(t.get('chk') or t['date']) < d or not t.get('chk'))
        peak = max(t['peak'] or p, p)
        c.execute("UPDATE vtrades SET peak=?, last_px=?, plan=?, chk=? WHERE id=?", (peak, p, plan, d, t['id']))
        if act == 'sell':
            if q.get('lower') and p <= q['lower'] and abs(px - p) < 1e-9:
                continue                                       # 하한가 — 못 팖, 다음 확인 때
            msgs.append(_sell(c, t, fill(t['ticker'], px, p, s, q), why, ts))
    for x in shadow:
        s = dict(s_now)
        s.update(json.loads(x['settings'] or '{}'))
        s.update(ex.PROFILES[x['prof']]['p']) if x['prof'] in ex.PROFILES else None
        q = q_of(x['ticker'])
        if not q or not q['price']:
            continue
        p = q['price']
        td = ex.trading_days(x['date'], d, hol)
        act, px, why, plan = ex.decide(_pos(x, s, x), p, now, q, td, first=(x.get('chk') or '') < d)
        peak = max(x['peak'] or p, p)
        if act == 'sell' and not (q.get('lower') and p <= q['lower'] and abs(px - p) < 1e-9):
            fp = fill(x['ticker'], px, p, s, q)
            ret, pnl = _ret(x, fp)
            c.execute("UPDATE vexits SET status='청산', peak=?, plan='', chk=?, sell_ts=?, sell_px=?, ret=?, pnl=?, reason=? "
                      "WHERE trade_id=? AND prof=?", (peak, d, ts, round(fp, 2), round(ret, 3), round(pnl), why, x['trade_id'], x['prof']))
        else:
            c.execute("UPDATE vexits SET peak=?, plan=?, chk=? WHERE trade_id=? AND prof=?", (peak, plan, d, x['trade_id'], x['prof']))
    c.commit()
    STATE['last_watch'] = ts
    return msgs


def any_open():
    c = db.conn()
    return bool(c.execute("SELECT 1 FROM vtrades WHERE status='보유' LIMIT 1").fetchone()
                or c.execute("SELECT 1 FROM vexits WHERE status='보유' LIMIT 1").fetchone())


# ════════════════════════════════════════════
#  요약
# ════════════════════════════════════════════
def reason_cat(x):
    x = x or ''
    for pre, name in (('시가 익절', '시가 익절(갭)'), ('시가 손절', '시가 손절(갭)'), ('익절', '익절'), ('손절', '손절'), ('트레일링', '트레일링'),
                      ('보유 기한', '보유 기한 지남'), ('보유 ', '최대 보유 시간'), ('상한가 마감', '상한가 → 다음날 시가'),
                      ('상한가 풀림', '상한가 풀림'), ('시가 매도', '다음날 시가 매도'), ('15:15', '15:15 청산'), ('전날', '전날 미청산')):
        if x.startswith(pre):
            return name
    return '시각 청산' if '청산' in x else (x or '기타')


def summary(days=60, model=None):
    """model: None = 모델 전체(대조군 제외) · 'ALL' = 대조군 포함 · 'M1'… = 한 모델"""
    init()
    c = db.conn()
    if model == 'ALL':
        q, a = "SELECT * FROM vtrades ORDER BY id DESC LIMIT 3000", ()
    elif model:
        q, a = "SELECT * FROM vtrades WHERE model=? ORDER BY id DESC LIMIT 3000", (model,)
    else:
        q, a = "SELECT * FROM vtrades WHERE COALESCE(model,'M1')!='Z' ORDER BY id DESC LIMIT 3000", ()
    rows = [dict(r) for r in c.execute(q, a)]
    closed = [r for r in rows if r['status'] == '청산']
    by_day = {}
    for r in closed:
        a = by_day.setdefault(r['date'], {'n': 0, 'win': 0, 'pnl': 0, 'ret': 0})
        a['n'] += 1
        a['win'] += r['ret'] > 0
        a['pnl'] += r['pnl'] or 0
        a['ret'] += r['ret'] or 0
    rets = [r['ret'] for r in closed]
    why = {}
    for r in closed:
        k = reason_cat(r['reason'])
        why[k] = why.get(k, 0) + 1
    return {'n': len(closed), 'open': sum(1 for r in rows if r['status'] == '보유'),
            'win': round(sum(1 for x in rets if x > 0) / len(rets) * 100, 1) if rets else None,
            'avg': round(sum(rets) / len(rets), 3) if rets else None, 'pnl': round(sum(r['pnl'] or 0 for r in closed)),
            'days': len(by_day), 'by_day': [{'date': k, **v} for k, v in sorted(by_day.items(), reverse=True)][:days],
            'why': why, 'rows': rows[:300]}


def profiles(model=None):
    """매도 규칙 비교 — 같은 매수에 '모델 자체 매도'와 프로필 A~E를 동시에 적용한 결과 (모두 청산된 매수만 공정 비교)
       model: 'M1'… 한 모델만 · None 전체"""
    init()
    c = db.conn()
    mw, ma = ("AND t.model=?", (model,)) if model else ("", ())
    done_all = {r[0] for r in c.execute(
        "SELECT t.id FROM vtrades t WHERE t.status='청산' AND NOT EXISTS (SELECT 1 FROM vexits x WHERE x.trade_id=t.id AND x.status='보유') "
        "AND EXISTS (SELECT 1 FROM vexits x WHERE x.trade_id=t.id) " + mw, ma)}
    rows = {'M': [dict(r) for r in c.execute("SELECT id AS trade_id, date, ret, pnl, reason, sell_ts, buy_ts FROM vtrades t WHERE status='청산' " + mw, ma)]}
    for k in ex.PROFILES:
        rows[k] = [dict(r) for r in c.execute("SELECT x.trade_id, t.date, x.ret, x.pnl, x.reason, x.sell_ts, t.buy_ts FROM vexits x "
                                              "JOIN vtrades t ON t.id=x.trade_id WHERE x.prof=? AND x.status='청산' " + mw, (k,) + ma)]
    out = []
    for k, rs in rows.items():
        fair = [r for r in rs if r['trade_id'] in done_all]
        rets = [r['ret'] for r in fair]
        hold = [(datetime.fromisoformat(r['sell_ts']) - datetime.fromisoformat(r['buy_ts'])).total_seconds() / 60 for r in fair if r['sell_ts']]
        why = {}
        for r in fair:
            w = reason_cat(r['reason'])
            why[w] = why.get(w, 0) + 1
        n_open = (c.execute("SELECT COUNT(*) FROM vtrades t WHERE status='보유' " + mw, ma).fetchone()[0] if k == 'M' else
                  c.execute("SELECT COUNT(*) FROM vexits x JOIN vtrades t ON t.id=x.trade_id WHERE x.prof=? AND x.status='보유' " + mw, (k,) + ma).fetchone()[0])
        info = ({'name': '모델 자체 매도', 'desc': '각 모델의 매도 규칙 (M1 · 대조군은 📐 매매 규칙의 현재 설정)'} if not model else
                {'name': f'{model} 자체 매도', 'desc': 'M1 · 대조군은 📐 매매 규칙의 현재 설정 · 나머지는 모델 고정 규칙'}) if k == 'M' else ex.PROFILES[k]
        out.append({'key': k, 'name': info['name'], 'desc': info['desc'], 'n': len(fair), 'open': n_open,
                    'win': round(sum(1 for x in rets if x > 0) / len(rets) * 100, 1) if rets else None,
                    'avg': round(sum(rets) / len(rets), 3) if rets else None, 'pnl': round(sum(r['pnl'] or 0 for r in fair)),
                    'worst': round(min(rets), 2) if rets else None, 'best': round(max(rets), 2) if rets else None,
                    'hold_min': round(sorted(hold)[len(hold) // 2]) if hold else None, 'why': why,
                    'p': None if k == 'M' else ex.PROFILES[k]['p']})
    return out


def _tstat(by_day):
    m = list(by_day.values())
    if len(m) < 3:
        return None
    mu = sum(m) / len(m)
    sd = (sum((a - mu) ** 2 for a in m) / (len(m) - 1)) ** 0.5
    return round(mu / (sd / len(m) ** 0.5), 2) if sd > 0 else None


def models_report(settings_, lab_result=None):
    """모델별 검증 현황 · 판정 (기준: danta_models.CRITERIA — 미리 정해 둔 것)"""
    init()
    c = db.conn()
    C = mdl.CRITERIA
    budget = settings_['amount'] * settings_['max_buys']
    lab = {}
    for r in (lab_result or {}).get('models', []) if lab_result else []:
        if r.get('exit') == 'own':
            lab[r['model']] = r
    rows = {m: [dict(r) for r in c.execute("SELECT date, ret, pnl FROM vtrades WHERE status='청산' AND model=? ORDER BY date", (m,))] for m in mdl.ORDER}
    z = [r['ret'] for r in rows.get('Z', [])]
    z_avg = sum(z) / len(z) if z else None
    out = []
    for m in mdl.ORDER:
        rs = rows[m]
        rets = [r['ret'] for r in rs]
        by, byp = {}, {}
        for r in rs:
            by.setdefault(r['date'], []).append(r['ret'])
            byp[r['date']] = byp.get(r['date'], 0) + (r['pnl'] or 0)
        dm = {k: sum(v) / len(v) for k, v in by.items()}
        dates = sorted(by)
        mid = dates[len(dates) // 2] if dates else ''
        h1 = [r['ret'] for r in rs if r['date'] < mid]
        h2 = [r['ret'] for r in rs if r['date'] >= mid]
        avg = sum(rets) / len(rets) if rets else None
        info = mdl.MODELS[m]
        rec = {'key': m, 'name': info['name'], 'desc': info['desc'], 'evidence': info['evidence'],
               'win_time': '~'.join(mdl.window(m, settings_)), 'exit': mdl.exit_rule(m, settings_),
               'n': len(rets), 'open': c.execute("SELECT COUNT(*) FROM vtrades WHERE status='보유' AND model=?", (m,)).fetchone()[0],
               'avg': round(avg, 3) if avg is not None else None, 'win': round(sum(1 for x in rets if x > 0) / len(rets) * 100, 1) if rets else None,
               'pnl': round(sum(r['pnl'] or 0 for r in rs)), 't': _tstat(dm), 'days': len(dates),
               'h1': round(sum(h1) / len(h1), 3) if h1 else None, 'h2': round(sum(h2) / len(h2), 3) if h2 else None,
               'worst_day': round(min(byp.values()) / budget * 100, 2) if byp and budget else None,
               'vs_z': round(avg - z_avg, 3) if avg is not None and z_avg is not None and m != 'Z' else None,
               'lab': lab.get(m)}
        rec['checks'], rec['verdict'] = _verdict(rec, C)
        out.append(rec)
    return out


def _verdict(r, C):
    lab = r.get('lab') or {}
    ln = lab.get('n') or 0
    checks = [
        ('건수', r['n'] >= C['live_min'] and ln >= C['lab_min'], f"실시간 {r['n']}/{C['live_min']} · 분봉 {ln}/{C['lab_min']}"),
        ('두 기간 +', (r['h1'] or -1) > 0 and (r['h2'] or -1) > 0 and (not ln or ((lab.get('is_avg') or -1) > 0 and (lab.get('oos_avg') or -1) > 0)),
         f"실시간 {r['h1'] if r['h1'] is not None else '-'} / {r['h2'] if r['h2'] is not None else '-'}" +
         (f" · 분봉 {lab.get('is_avg')} / {lab.get('oos_avg')}" if ln else '')),
        ('t ≥ 2', (r['t'] or 0) >= C['t_min'], f"t {r['t'] if r['t'] is not None else '-'}"),
        ('대조군 +0.3', r['key'] == 'Z' or (r['vs_z'] or -9) >= C['vs_z'], '-' if r['key'] == 'Z' else f"{r['vs_z'] if r['vs_z'] is not None else '-'}%p"),
        ('최악의 날', r['worst_day'] is None or r['worst_day'] >= C['worst_day'], f"{r['worst_day'] if r['worst_day'] is not None else '-'}%"),
    ]
    if r['key'] == 'Z':
        return checks, '잣대'
    if r['n'] < 30 and ln < 200:
        return checks, '데이터 부족'
    if (ln >= 200 and (lab.get('is_avg') or 0) < 0 and (lab.get('oos_avg') or 0) < 0) or (r['n'] >= 60 and (r['avg'] or 0) < 0 and (r['t'] or 0) < -1):
        return checks, '탈락 경향'
    if all(ok for _, ok, _ in checks):
        return checks, '합격'
    if (r['n'] >= 30 and (r['avg'] or 0) > 0) or (ln >= 200 and (lab.get('is_avg') or 0) > 0 and (lab.get('oos_avg') or 0) > 0):
        return checks, '유망'
    return checks, '보류'


def day_text(d):
    """장 마감 뒤 텔레그램 거래 실적 리포트 (D0.6.1 — 건별 매수 · 매도 알림 대신 하루 한 통)"""
    init()
    c = db.conn()
    day = f"{d[:4]}-{d[4:6]}-{d[6:]}"
    bought = [dict(r) for r in c.execute("SELECT * FROM vtrades WHERE date=? ORDER BY id", (d,))]
    sold = [dict(r) for r in c.execute("SELECT * FROM vtrades WHERE status='청산' AND sell_ts LIKE ? ORDER BY id", (day + '%',))]
    held = [dict(r) for r in c.execute("SELECT * FROM vtrades WHERE status='보유' ORDER BY id")]
    n_snap = c.execute("SELECT COUNT(DISTINCT hm), COUNT(DISTINCT ticker) FROM snaps WHERE date=? AND hm>='09:00'", (d,)).fetchone()
    mo = lambda r: r.get('model') or 'M1'
    real = [r for r in sold if mo(r) != 'Z']
    pnl = sum(r['pnl'] or 0 for r in real)
    win = sum(1 for r in real if (r['ret'] or 0) > 0)
    L = [f"📋 {d[4:6]}/{d[6:]} 단타 장 마감 리포트 (가상 · 주문 없음)",
         f"■ 오늘: 매수 {sum(1 for r in bought if mo(r) != 'Z')}건 · 매도 {len(real)}건 · 승 {win}/{len(real)} · 손익 {pnl:+,.0f}원 (비용 반영 · 대조군 제외)"
         + (f" · 탐지 {n_snap[0] or 0}회" if not bought else '')]
    if sold or bought:
        L.append('■ 모델별 (오늘 매도 기준)')
        for m in mdl.ORDER:
            s_ = [r for r in sold if mo(r) == m]
            b_ = sum(1 for r in bought if mo(r) == m)
            if not s_ and not b_:
                continue
            rets = [r['ret'] or 0 for r in s_]
            L.append(f" {m}{' 대조군' if m == 'Z' else ''}: 매수 {b_} · 매도 {len(s_)}"
                     + (f" · 승 {sum(1 for x in rets if x > 0)}/{len(s_)} · 평균 {sum(rets) / len(rets):+.2f}% · {sum(r['pnl'] or 0 for r in s_):+,.0f}원" if s_ else ''))
    if held:
        by = {}
        for r in held:
            by[mo(r)] = by.get(mo(r), 0) + 1
        L.append('■ 보유 이월: ' + ' · '.join(f"{k} {v}종목" for k, v in by.items()) + ' (다음 날 시가 판단)')
    allc = [dict(r) for r in c.execute("SELECT model, ret, pnl, date FROM vtrades WHERE status='청산'")]
    rc = [r for r in allc if (r['model'] or 'M1') != 'Z']
    z = [r['ret'] or 0 for r in allc if r['model'] == 'Z']
    if rc:
        rr = [r['ret'] or 0 for r in rc]
        L.append(f"■ 누적 ({min(r['date'] for r in rc)[4:6]}/{min(r['date'] for r in rc)[6:]}~): {len(rc)}건 · 승률 {sum(1 for x in rr if x > 0) / len(rr) * 100:.0f}% · "
                 f"건당 {sum(rr) / len(rr):+.2f}% · {sum(r['pnl'] or 0 for r in rc):+,.0f}원" + (f" · 대조군 건당 {sum(z) / len(z):+.2f}%" if z else ''))
    return '\n'.join(L)
