"""
scout_ext.py — 외부 데이터 연동
============================================
자동매매 v8에서 검증된 모듈을 Scout용으로 이식 + 보강

· 네이버 종목뉴스 + 키워드 감성분석 + 악재 하드필터
· DART 주요사항보고 (유상증자·CB·BW·감자) 리스크 필터
· KIS 종목 프로필 (업종·시총·상장주식수·PER·관리/경고 지정)
· KIS 실시간 순위 (거래량·거래대금·등락률) — 단타 후보 보강
· KIS 체결강도
· 전일 미국증시 (나스닥·S&P) → 시장계수 보정
"""
import re, json, time, urllib.request, urllib.parse, urllib.error
from datetime import datetime, timedelta

import scout_db as db

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}


def _get_json(url, timeout=5):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8'))


# ════════════════════════════════════════════
#  뉴스 + 감성 (자동매매 v8 키워드 이식)
# ════════════════════════════════════════════
POSITIVE = [
    '신고가', '역대최대', '사상최대', '실적호전', '어닝서프라이즈', '수주', '흑자전환', '특허',
    'FDA승인', '임상성공', '대규모투자', '자사주매입', '자사주 소각', '배당확대', '목표가상향',
    '매수추천', '수출최대', '영업이익증가', '순이익증가', '매출증가', '점유율확대',
    '전략적제휴', '대형계약', '공급계약', '정부지원', '국책사업', '신사업진출', '규제완화',
    '상향', '최대 실적', '호실적',
]
NEGATIVE = [
    '하한가', '신저가', '적자전환', '적자확대', '실적악화', '어닝쇼크', '하향', '감자',
    '상장폐지', '횡령', '배임', '소송', '분식회계', '리콜', '영업정지', '목표가하향',
    '매도추천', '투자경고', '투자위험', '관리종목', '불성실공시', '감사의견', '부도',
    '워크아웃', '법정관리', '파산', '영업이익감소', '매출감소', '적자지속',
    '대주주매도', '블록딜', '유상증자', '전환사채', 'CB발행', '오버행', '거래정지',
]
# 발견 즉시 추천 제외 (점수 무관)
HARD_BAD = ['상장폐지', '횡령', '배임', '분식회계', '관리종목', '감사의견', '거래정지',
            '부도', '파산', '법정관리', '워크아웃', '영업정지', '불성실공시', '주가조작']
# 재료(촉매) 판정용 — 로스 카메론 "뉴스 촉매 필수"
CATALYST = ['수주', '공급계약', '흑자전환', '신고가', 'FDA', '임상', '특허', '어닝서프라이즈',
            '사상최대', '역대최대', '최대 실적', '인수', '합병', '자사주', '대형계약', '정부']

_news_cache = {}
_breaker = {'fail': 0, 'until': 0}   # 연속 실패 시 10분간 뉴스 조회 중단


def fetch_news(ticker, max_days=5):
    """네이버 모바일 종목뉴스 → [{'title','date'}]"""
    c = _news_cache.get(ticker)
    if c and time.time() - c['ts'] < 1800:
        return c['items']
    if time.time() < _breaker['until']:
        return []
    items = []
    cutoff = datetime.now() - timedelta(days=max_days)
    for url in (f'https://m.stock.naver.com/api/news/stock/{ticker}?pageSize=20&page=1',
                f'https://m.stock.naver.com/api/stock/{ticker}/news?page=1&pageSize=20',
                f'https://m.stock.naver.com/api/stocks/{ticker}/news?page=1&pageSize=20'):
        try:
            d = _get_json(url)
        except urllib.error.HTTPError:
            continue            # 경로만 틀림 → 다음 경로
        except Exception:
            break               # 네트워크 자체 불통 → 즉시 포기
        rows = d if isinstance(d, list) else d.get('news', d.get('items', d.get('list', [])))
        flat = []
        for r in rows or []:
            # 새 API는 [{items:[...]}] 형태로 묶여 옴
            if isinstance(r, dict) and isinstance(r.get('items'), list):
                flat.extend(r['items'])
            else:
                flat.append(r)
        for it in flat:
            if not isinstance(it, dict):
                continue
            t = it.get('title') or it.get('articleTitle') or it.get('tit') or ''
            t = re.sub(r'<[^>]+>|&quot;|&amp;', '', t).strip()
            ds = str(it.get('datetime') or it.get('date') or it.get('wrtDt') or '')
            m = re.search(r'(\d{4})[.\-]?(\d{2})[.\-]?(\d{2})', ds)
            dt = datetime(int(m[1]), int(m[2]), int(m[3])) if m else None
            if dt and dt < cutoff:
                continue
            if len(t) > 8:
                items.append({'title': t, 'date': dt.strftime('%m.%d') if dt else ''})
        if items:
            break
    if items:
        _breaker['fail'] = 0
    else:
        _breaker['fail'] += 1
        if _breaker['fail'] >= 4:
            _breaker['until'] = time.time() + 600
            _breaker['fail'] = 0
            print('[NEWS] 연속 실패 — 10분간 뉴스 조회 중단')
    _news_cache[ticker] = {'ts': time.time(), 'items': items[:15]}
    return items[:15]


def news_sentiment(items):
    """제목 키워드 감성 → {'score':-100~100, 'label', 'pos', 'neg', 'hard_bad', 'catalyst'}"""
    pos, neg, hard, cat = [], [], [], []
    for it in items:
        t = it['title'].replace(' ', '')
        for kw in HARD_BAD:
            if kw.replace(' ', '') in t:
                hard.append(f"{kw}: {it['title'][:36]}")
                break
        for kw in POSITIVE:
            if kw.replace(' ', '') in t:
                pos.append(it['title'][:40])
                break
        for kw in NEGATIVE:
            if kw.replace(' ', '') in t:
                neg.append(it['title'][:40])
                break
        for kw in CATALYST:
            if kw in it['title']:
                cat.append(it['title'][:40])
                break
    score = max(-100, min(100, len(pos) * 15 - len(neg) * 20))
    label = ('강한호재' if score >= 50 else '호재' if score >= 20 else
             '강한악재' if score <= -50 else '악재' if score <= -20 else '중립')
    return {'score': score, 'label': label, 'pos': pos[:4], 'neg': neg[:4],
            'hard_bad': hard[:3], 'catalyst': cat[:3], 'count': len(items),
            'headline': items[0]['title'][:50] if items else ''}


# ════════════════════════════════════════════
#  KIS 종목 프로필 — 업종·시총·상장주식수·PER·지정경고
# ════════════════════════════════════════════
WARN = {'01': '투자주의', '02': '투자경고', '03': '투자위험'}


def fetch_profile(ticker, app_key, app_secret, token):
    r = db.kis_get("/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100",
                   {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
                   app_key, app_secret, token)
    o = r.get('output') or {}

    def f(k):
        try:
            return float(str(o.get(k, 0) or 0).replace(',', ''))
        except Exception:
            return 0.0
    warns = []
    wc = str(o.get('mrkt_warn_cls_code', '00') or '00')
    if wc in WARN:
        warns.append(WARN[wc])
    if str(o.get('mang_issu_cls_code', 'N')).upper() == 'Y':
        warns.append('관리종목')
    if str(o.get('temp_stop_yn', 'N')).upper() == 'Y':
        warns.append('거래정지')
    if str(o.get('sltr_yn', 'N')).upper() == 'Y':
        warns.append('정리매매')
    if str(o.get('short_over_yn', 'N')).upper() == 'Y':
        warns.append('단기과열')
    return {
        'sector': (o.get('bstp_kor_isnm') or '').strip(),
        'mktcap': f('hts_avls'),              # 억원
        'shares': f('lstn_stcn'),             # 상장주식수
        'per': f('per'), 'pbr': f('pbr'), 'eps': f('eps'),
        'foreign_ratio': f('hts_frgn_ehrt'),
        'warns': warns,
        # 장중 당일 봉
        'live': {'open': f('stck_oprc'), 'high': f('stck_hgpr'), 'low': f('stck_lwpr'),
                 'close': f('stck_prpr'), 'volume': int(f('acml_vol')),
                 'chg': f('prdy_ctrt')},
    }


def sync_profiles(app_key, app_secret, tickers, progress=None, stop_flag=None):
    """후보풀 프로필 일괄 수집 → stocks 테이블 저장"""
    token = db.get_token(app_key, app_secret)
    c = db.conn()
    ok = 0
    for i, tk in enumerate(tickers):
        if stop_flag and stop_flag():
            break
        try:
            p = fetch_profile(tk, app_key, app_secret, token)
            c.execute("""UPDATE stocks SET sector=?, mktcap=?, shares=?, per=?, pbr=?,
                         foreign_ratio=?, warns=? WHERE ticker=?""",
                      (p['sector'], p['mktcap'], p['shares'], p['per'], p['pbr'],
                       p['foreign_ratio'], ','.join(p['warns']), tk))
            ok += 1
            if ok % 50 == 0:
                c.commit()
        except Exception:
            pass
        if progress and i % 25 == 0:
            progress({'stage': 'profile', 'done': i + 1, 'total': len(tickers), 'ok': ok})
    c.commit()
    db.meta_set('profiles_synced', datetime.now().isoformat())
    return ok


# ════════════════════════════════════════════
#  DART 주요사항보고 — 희석성 이벤트
# ════════════════════════════════════════════
DART_BAD = {'유상증자결정': '유상증자', '전환사채권발행결정': 'CB', '신주인수권부사채권발행결정': 'BW',
            '교환사채권발행결정': 'EB', '감자결정': '감자', '회생절차': '회생절차',
            '영업정지': '영업정지', '부도발생': '부도'}


def sync_dart(dart_key, days=90, progress=None):
    """최근 N일 주요사항보고(pblntf_ty=B) 전수 조회 → events 테이블"""
    if not dart_key:
        return 0
    c = db.conn()
    end = datetime.now()
    bgn = end - timedelta(days=days)
    saved, page = 0, 1
    while page <= 60:
        q = urllib.parse.urlencode({
            'crtfc_key': dart_key, 'bgn_de': bgn.strftime('%Y%m%d'),
            'end_de': end.strftime('%Y%m%d'), 'pblntf_ty': 'B',
            'page_no': page, 'page_count': 100})
        try:
            d = _get_json(f"https://opendart.fss.or.kr/api/list.json?{q}", 15)
        except Exception:
            break
        if d.get('status') != '000':
            break
        for it in d.get('list', []):
            tk = (it.get('stock_code') or '').strip()
            nm = it.get('report_nm', '')
            if not tk:
                continue
            typ = next((v for k, v in DART_BAD.items() if k in nm.replace(' ', '')), None)
            if not typ:
                continue
            c.execute("""INSERT OR IGNORE INTO events(ticker,date,type,title,rcept_no)
                         VALUES(?,?,?,?,?)""",
                      (tk, it.get('rcept_dt', ''), typ, nm, it.get('rcept_no', '')))
            saved += 1
        total_page = int(d.get('total_page', 1) or 1)
        if progress:
            progress({'stage': 'dart', 'done': page, 'total': total_page, 'ok': saved})
        if page >= total_page:
            break
        page += 1
        time.sleep(0.2)
    c.commit()
    db.meta_set('dart_synced', datetime.now().isoformat())
    return saved


def load_events(ticker, days=90):
    since = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
    return [dict(r) for r in db.conn().execute(
        "SELECT date,type,title FROM events WHERE ticker=? AND date>=? ORDER BY date DESC",
        (ticker, since))]


# ════════════════════════════════════════════
#  실시간 순위 · 체결강도 (단타 탭)
# ════════════════════════════════════════════
def live_ranks(app_key, app_secret, token, limit=30):
    """거래량 / 거래대금 / 등락률 순위 합집합 → {ticker: {chg, vol}}"""
    out = {}
    for div, blng in (('0', '0'), ('0', '3'), ('1', '0')):
        try:
            r = db.kis_get("/uapi/domestic-stock/v1/quotations/volume-rank", "FHPST01710000",
                           {"FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "20171",
                            "FID_INPUT_ISCD": "0000", "FID_DIV_CLS_CODE": div,
                            "FID_BLNG_CLS_CODE": blng, "FID_TRGT_CLS_CODE": "111111111",
                            "FID_TRGT_EXLS_CLS_CODE": "000000", "FID_INPUT_PRICE_1": "0",
                            "FID_INPUT_PRICE_2": "0", "FID_VOL_CNT": "0", "FID_INPUT_DATE_1": ""},
                           app_key, app_secret, token)
            for s in (r.get('output') or [])[:limit]:
                tk = s.get('mksc_shrn_iscd', '')
                if len(tk) == 6:
                    out[tk] = {'chg': float(s.get('prdy_ctrt', 0) or 0),
                               'vol': int(s.get('acml_vol', 0) or 0)}
        except Exception:
            continue
    return out


def fetch_strength(ticker, app_key, app_secret, token):
    """당일 체결강도 (매수체결/매도체결 ×100)"""
    try:
        r = db.kis_get("/uapi/domestic-stock/v1/quotations/inquire-ccnl", "FHKST01010300",
                       {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
                       app_key, app_secret, token)
        rows = r.get('output') or []
        if rows:
            v = float(rows[0].get('tday_rltv', 0) or 0)
            if v > 0:
                return v
    except Exception:
        pass
    # 대체: 호가 잔량비
    try:
        r = db.kis_get("/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
                       "FHKST01010200",
                       {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
                       app_key, app_secret, token)
        o = r.get('output1') or {}
        bid = float(o.get('total_bidp_rsqn', 0) or 0)
        ask = float(o.get('total_askp_rsqn', 0) or 0)
        if ask > 0:
            return round(bid / ask * 100, 1)
    except Exception:
        pass
    return None


def is_market_hours():
    n = datetime.now()
    return n.weekday() < 5 and '09:00' <= n.strftime('%H:%M') <= '15:30'


# ════════════════════════════════════════════
#  전일 미국증시 → 시장계수 보정
# ════════════════════════════════════════════
_us_cache = {'ts': 0, 'data': None}


def fetch_us_market():
    if _us_cache['data'] and time.time() - _us_cache['ts'] < 1800:
        return _us_cache['data']
    codes = {'nasdaq': 'NAS@IXIC', 'sp500': 'SPI@SPX', 'sox': 'NAS@SOX'}
    out = {}
    try:
        d = _get_json('https://polling.finance.naver.com/api/realtime/worldstock/index/'
                      + ','.join(codes.values()))
        for it in d.get('datas', []):
            cd = it.get('cd') or it.get('reutersCode') or ''
            for k, v in codes.items():
                if v.split('@')[-1] in cd:
                    try:
                        out[k] = float(str(it.get('cr') or it.get('fluctuationsRatio') or 0))
                    except Exception:
                        pass
    except Exception:
        pass
    if not out and not _us_cache.get('dead'):
        for k, sym in (('nasdaq', '.IXIC'), ('sp500', '.INX'), ('sox', '.SOX')):
            try:
                d = _get_json(f'https://api.stock.naver.com/index/{sym}/basic')
                out[k] = float(str(d.get('fluctuationsRatio', 0)))
            except urllib.error.HTTPError:
                continue
            except Exception:
                _us_cache['dead'] = True
                break
    _us_cache.update(ts=time.time(), data=out)
    return out


def us_adjust(us):
    """나스닥·필라델피아반도체 등락 → 시장계수 가감 (-0.15 ~ +0.05)"""
    if not us:
        return 0.0, '미증시 데이터 없음'
    nq = us.get('nasdaq', 0)
    sx = us.get('sox', nq)
    avg = (nq + sx) / 2
    if avg <= -3:
        adj = -0.15
    elif avg <= -1.5:
        adj = -0.08
    elif avg >= 1.5:
        adj = 0.05
    else:
        adj = 0.0
    txt = f"나스닥 {nq:+.2f}%" + (f" · SOX {sx:+.2f}%" if 'sox' in us else '')
    return adj, txt


# ════════════════════════════════════════════
#  KIS 잔고 · 당일 체결 (읽기 전용 — 주문 기능 아님)
#  증권앱에서 직접 사고판 내역을 Scout가 따라가기 위한 조회
# ════════════════════════════════════════════
def fetch_balance(app_key, app_secret, token, account, prod='01'):
    """보유종목 → {ticker: {'name','qty','avg','price'}}"""
    r = db.kis_get("/uapi/domestic-stock/v1/trading/inquire-balance", "TTTC8434R",
                   {"CANO": account, "ACNT_PRDT_CD": prod, "AFHR_FLPR_YN": "N", "OFL_YN": "",
                    "INQR_DVSN": "02", "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N",
                    "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "01",
                    "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""},
                   app_key, app_secret, token)
    if r.get('rt_cd') not in ('0', None):
        raise RuntimeError(r.get('msg1', '잔고 조회 실패'))
    out = {}
    for it in r.get('output1') or []:
        q = int(float(it.get('hldg_qty', 0) or 0))
        if q <= 0:
            continue
        out[it.get('pdno', '')] = {'name': it.get('prdt_name', ''), 'qty': q,
                                   'avg': float(it.get('pchs_avg_pric', 0) or 0),
                                   'price': float(it.get('prpr', 0) or 0)}
    return out


def fetch_today_fills(app_key, app_secret, token, account, prod='01'):
    """당일 체결 → {ticker: {'sell': (qty, avg), 'buy': (qty, avg)}}"""
    today = datetime.now().strftime('%Y%m%d')
    r = db.kis_get("/uapi/domestic-stock/v1/trading/inquire-daily-ccld", "TTTC8001R",
                   {"CANO": account, "ACNT_PRDT_CD": prod, "INQR_STRT_DT": today,
                    "INQR_END_DT": today, "SLL_BUY_DVSN_CD": "00", "INQR_DVSN": "00",
                    "PDNO": "", "CCLD_DVSN": "01", "ORD_GNO_BRNO": "", "ODNO": "",
                    "INQR_DVSN_3": "00", "INQR_DVSN_1": "", "CTX_AREA_FK100": "",
                    "CTX_AREA_NK100": ""},
                   app_key, app_secret, token)
    agg = {}
    for it in r.get('output1') or []:
        q = int(float(it.get('tot_ccld_qty', 0) or 0))
        if q <= 0:
            continue
        side = 'sell' if it.get('sll_buy_dvsn_cd') == '01' else 'buy'
        px = float(it.get('avg_prvs', 0) or 0)
        d = agg.setdefault(it.get('pdno', ''), {}).setdefault(side, [0, 0.0])
        d[0] += q
        d[1] += q * px
    return {tk: {k: (v[0], v[1] / v[0] if v[0] else 0) for k, v in sides.items()}
            for tk, sides in agg.items()}


# ════════════════════════════════════════════
#  KRX 휴장일 (KIS 국내휴장일조회 — 하루 1회 권장)
# ════════════════════════════════════════════
def is_trading_day(d, app_key, app_secret):
    """d(datetime)가 개장일인지. 조회 실패 시 평일이면 개장으로 간주."""
    if d.weekday() >= 5:
        return False
    ds = d.strftime('%Y%m%d')
    try:
        cache = json.loads(db.meta_get('holiday_cache', '{}') or '{}')
    except Exception:
        cache = {}
    if ds in cache:
        return cache[ds]
    if not (app_key and app_secret):
        return True
    try:
        tok = db.get_token(app_key, app_secret)
        r = db.kis_get("/uapi/domestic-stock/v1/quotations/chk-holiday", "CTCA0903R",
                       {"BASS_DT": ds, "CTX_AREA_NK": "", "CTX_AREA_FK": ""},
                       app_key, app_secret, tok)
        for it in r.get('output') or []:
            if it.get('bass_dt'):
                cache[it['bass_dt']] = (it.get('opnd_yn', 'Y') == 'Y')
        # 오래된 항목 정리
        cut = (d - timedelta(days=10)).strftime('%Y%m%d')
        cache = {k: v for k, v in cache.items() if k >= cut}
        db.meta_set('holiday_cache', json.dumps(cache))
    except Exception:
        return True
    return cache.get(ds, True)



# ════════════════════════════════════════════
#  KRX 수급 (pykrx) — 외국인 · 기관합계 · 연기금
#  KRX 정보데이터시스템 회원 로그인 필요 (2025-12 회원제 전환)
# ════════════════════════════════════════════
FLOW_INVESTORS = ('외국인', '기관합계', '연기금')
_pykrx = {'stock': None, 'id': None}


def _pykrx_stock(krx_id, krx_pw):
    import os, io, contextlib
    if _pykrx['stock'] is not None and _pykrx['id'] == krx_id:
        return _pykrx['stock']
    os.environ['KRX_ID'], os.environ['KRX_PW'] = krx_id, krx_pw
    cap = io.StringIO()
    with contextlib.redirect_stdout(cap), contextlib.redirect_stderr(cap):
        from pykrx import stock          # 환경변수 설정 뒤 불러와야 로그인됨
    _pykrx.update(stock=stock, id=krx_id)
    return stock


def collect_krx_flows(krx_id, krx_pw, dates, progress=None, sleep=1.0):
    """dates 중 아직 없는 날짜의 전종목 순매수를 받아 저장. 반환: (받은 건수, 오류 메시지)"""
    import io, contextlib
    try:
        stock = _pykrx_stock(krx_id, krx_pw)
    except ImportError:
        return 0, 'pykrx 미설치 — Scout_실행.bat을 다시 실행하면 설치됩니다'
    except Exception as e:
        return 0, f'KRX 로그인 실패: {str(e)[:80]}'
    got, fails = 0, 0
    for inv in FLOW_INVESTORS:
        have = db.flow_dates(inv)
        for d in [x for x in dates if x not in have]:
            try:
                cap = io.StringIO()
                with contextlib.redirect_stdout(cap), contextlib.redirect_stderr(cap):
                    df = stock.get_market_net_purchases_of_equities_by_ticker(d, d, 'ALL', inv)
                if df is None or len(df) == 0:
                    fails += 1
                    if fails >= 5:
                        return got, 'KRX 응답 없음 (로그인 또는 점검 확인)'
                    continue
                col = '순매수거래대금' if '순매수거래대금' in df.columns else df.columns[-1]
                db.save_flows(d, inv, [(str(t), float(v)) for t, v in df[col].items()])
                got += 1
                fails = 0
                if progress:
                    progress(f'수급 {inv} {d}')
            except Exception as e:
                fails += 1
                if fails >= 5:
                    return got, f'연속 실패: {str(e)[:60]}'
            time.sleep(sleep)
    return got, ''
