"""
danta_kis.py — TK Danta KIS 통신 (시세 조회 전용 · 1단계에는 주문 기능 없음)

· 토큰은 TKDanta 데이터 폴더에 따로 보관 (Scout와 섞지 않음 · 같은 앱키여도 KIS가 기존 토큰을 돌려줌)
· 초당 호출 제한: 기본 8회 (Scout와 같은 앱키면 둘이 나눠 쓰므로 설정에서 낮출 수 있음)
· 분봉 API
    주식일별분봉조회 FHKST03010230 — 과거 날짜 분봉 (한 번에 최대 120개, KIS 서버 보관 기간만큼)
    주식당일분봉조회 FHKST03010200 — 오늘 분봉 (한 번에 30개) · 과거 조회가 안 될 때 대체
"""
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

import danta_db as db

URL = 'https://openapi.koreainvestment.com:9443'
_lock = threading.Lock()
_last = [0.0]
RPS = [8.0]
_tok = {'v': '', 'exp': 0.0, 'key': ''}
TOKEN_FILE = os.path.join(db.DATA_DIR, 'kis_token.json')


def _throttle():
    with _lock:
        wait = _last[0] + 1.0 / RPS[0] - time.time()
        if wait > 0:
            time.sleep(wait)
        _last[0] = time.time()


def token(cfg):
    k, s = cfg.get('app_key', ''), cfg.get('app_secret', '')
    if not (k and s):
        raise RuntimeError('KIS 앱키가 설정되지 않았습니다')
    if _tok['v'] and _tok['key'] == k and _tok['exp'] > time.time():
        return _tok['v']
    try:
        d = json.load(open(TOKEN_FILE, encoding='utf-8'))
        if d.get('key') == k[-6:] and d.get('exp', 0) > time.time():
            _tok.update(v=d['v'], exp=d['exp'], key=k)
            return _tok['v']
    except Exception:
        pass
    body = json.dumps({"grant_type": "client_credentials", "appkey": k, "appsecret": s}).encode()
    req = urllib.request.Request(URL + '/oauth2/tokenP', data=body, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            d = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"토큰 발급 실패: {_err_text(e)}")
    if not d.get('access_token'):
        raise RuntimeError(f"토큰 발급 실패: {d.get('error_description') or d}")
    _tok.update(v=d['access_token'], exp=time.time() + 23 * 3600, key=k)
    try:
        json.dump({'v': _tok['v'], 'exp': _tok['exp'], 'key': k[-6:]}, open(TOKEN_FILE, 'w', encoding='utf-8'))
    except Exception:
        pass
    return _tok['v']


def _err_text(e):
    try:
        d = json.loads(e.read().decode('utf-8', 'replace'))
        return f"{d.get('msg1') or d.get('error_description') or d} ({d.get('msg_cd') or d.get('error_code') or e.code})"
    except Exception:
        return f'HTTP {e.code}'


def get(cfg, path, tr_id, params, retry=2):
    """테스트에서 교체 가능한 단일 조회 함수"""
    _throttle()
    url = URL + path + '?' + urllib.parse.urlencode(params)
    h = {"Content-Type": "application/json; charset=utf-8", "authorization": f"Bearer {token(cfg)}",
         "appkey": cfg['app_key'], "appsecret": cfg['app_secret'], "tr_id": tr_id, "custtype": "P"}
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=20) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        msg = _err_text(e)
        if 'token' in msg.lower() or 'EGW0012' in msg:
            _tok['v'] = ''
            try:
                os.remove(TOKEN_FILE)
            except Exception:
                pass
        if retry > 0 and e.code in (401, 429, 500, 502, 503):
            time.sleep(1.2)
            return get(cfg, path, tr_id, params, retry - 1)
        raise RuntimeError(msg)
    except (urllib.error.URLError, TimeoutError) as e:
        if retry > 0:
            time.sleep(1.0)
            return get(cfg, path, tr_id, params, retry - 1)
        raise RuntimeError(f'통신 오류: {e}')


def _f(x):
    try:
        return float(str(x).replace(',', '') or 0)
    except (TypeError, ValueError):
        return 0.0


def _parse(rows, d):
    """KIS 분봉 행 → {hm: (hm,o,h,l,c,vol,amt_cum)} (그 날짜 · 정규장 09:00~15:30만)"""
    out = {}
    for r in rows or []:
        if str(r.get('stck_bsop_date', d)) != d:
            continue
        t = str(r.get('stck_cntg_hour', '')).zfill(6)
        hm = int(t[:4]) if t.isdigit() else None
        if hm is None or not (900 <= hm <= 1530):
            continue
        c = _f(r.get('stck_prpr'))
        if c <= 0:
            continue
        out[hm] = (hm, _f(r.get('stck_oprc')) or c, _f(r.get('stck_hgpr')) or c, _f(r.get('stck_lwpr')) or c, c,
                   int(_f(r.get('cntg_vol'))), _f(r.get('acml_tr_pbmn')))
    return out


def _finish(got):
    """누적 거래대금 → 분당 거래대금 · 시간순 정렬"""
    bars, prev = [], 0.0
    for hm in sorted(got):
        _, o, h, l, c, v, cum = got[hm]
        amt = max(0.0, cum - prev) if cum and cum >= prev else v * c
        prev = cum or prev
        bars.append((hm, o, h, l, c, v, round(amt)))
    return bars


def minute_day(cfg, ticker, d):
    """과거(또는 오늘) 하루치 1분봉 — 주식일별분봉조회를 15:30부터 거꾸로 넘기며"""
    got, hour = {}, '153000'
    for _ in range(8):
        r = get(cfg, '/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice', 'FHKST03010230',
                {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': ticker, 'FID_INPUT_HOUR_1': hour,
                 'FID_INPUT_DATE_1': d, 'FID_PW_DATA_INCU_YN': 'N', 'FID_FAKE_TICK_INCU_YN': ''})
        if r.get('rt_cd') not in ('0', None):
            raise RuntimeError(f"{r.get('msg1', '조회 실패')} ({r.get('msg_cd', '')})")
        part = _parse(r.get('output2'), d)
        new = {k: v for k, v in part.items() if k not in got}
        if not new:
            break
        got.update(new)
        first = min(new)
        if first <= 901:
            break
        t = datetime.strptime(f'{first:04d}', '%H%M') - timedelta(minutes=1)
        hour = t.strftime('%H%M') + '00'
    return _finish(got)


def minute_today(cfg, ticker, d):
    """오늘 1분봉 — 주식당일분봉조회 (30개씩)"""
    got, hour = {}, '153000'
    for _ in range(16):
        r = get(cfg, '/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice', 'FHKST03010200',
                {'FID_ETC_CLS_CODE': '', 'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': ticker,
                 'FID_INPUT_HOUR_1': hour, 'FID_PW_DATA_INCU_YN': 'N'})
        if r.get('rt_cd') not in ('0', None):
            raise RuntimeError(f"{r.get('msg1', '조회 실패')} ({r.get('msg_cd', '')})")
        part = _parse(r.get('output2'), d)
        new = {k: v for k, v in part.items() if k not in got}
        if not new:
            break
        got.update(new)
        first = min(new)
        if first <= 901:
            break
        hour = (datetime.strptime(f'{first:04d}', '%H%M') - timedelta(minutes=1)).strftime('%H%M') + '00'
    return _finish(got)


def value_rank(cfg):
    """거래대금 상위 (KIS 순위 API · 최대 30종목) — Scout 일봉이 없을 때만 사용"""
    r = get(cfg, '/uapi/domestic-stock/v1/quotations/volume-rank', 'FHPST01710000',
            {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_COND_SCR_DIV_CODE': '20171', 'FID_INPUT_ISCD': '0000',
             'FID_DIV_CLS_CODE': '0', 'FID_BLNG_CLS_CODE': '3', 'FID_TRGT_CLS_CODE': '111111111',
             'FID_TRGT_EXLS_CLS_CODE': '0000000000', 'FID_INPUT_PRICE_1': '', 'FID_INPUT_PRICE_2': '',
             'FID_VOL_CNT': '', 'FID_INPUT_DATE_1': ''})
    out = []
    for x in r.get('output') or []:
        tk, nm = x.get('mksc_shrn_iscd', ''), x.get('hts_kor_isnm', '')
        if tk and not db.excluded(tk, nm):
            out.append((tk, nm, '', _f(x.get('acml_tr_pbmn'))))
    return out


def probe(cfg, ticker='005930', d=None):
    """과거 분봉 조회가 되는지 점검 — 한 번 호출해 결과 요약 (화면 '연결 점검' 버튼)"""
    if not d:
        x = datetime.now() - timedelta(days=5)
        while x.weekday() >= 5:                          # 평일로
            x -= timedelta(days=1)
        d = x.strftime('%Y%m%d')
    res = {'ticker': ticker, 'date': d}
    t0 = time.time()
    try:
        r = get(cfg, '/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice', 'FHKST03010230',
                {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': ticker, 'FID_INPUT_HOUR_1': '153000',
                 'FID_INPUT_DATE_1': d, 'FID_PW_DATA_INCU_YN': 'N', 'FID_FAKE_TICK_INCU_YN': ''})
        rows = r.get('output2') or []
        ds = sorted({str(x.get('stck_bsop_date')) for x in rows})
        res.update(ok=r.get('rt_cd') in ('0', None) and bool(rows), rows=len(rows), dates=ds[:3] + (['…'] if len(ds) > 3 else []),
                   msg=r.get('msg1', ''), first=rows[-1] if rows else None, last=rows[0] if rows else None,
                   fields=sorted((rows[0] or {}).keys()) if rows else [])
    except Exception as e:
        res.update(ok=False, msg=str(e)[:200])
    res['ms'] = round((time.time() - t0) * 1000)
    return res
