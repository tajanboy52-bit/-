"""
tk_collect.py — TK자동매매 시장 자료 수집 (자체 · 다른 앱 필요 없음)

주 경로 KRX 정보데이터시스템 (pykrx · ⚙️ KRX 계정 필요 · data.krx.co.kr 무료 가입)
  · 종목 목록: 상장 + 상장폐지 (생존 편향 없는 백테스트)
  · 일봉: 날짜 하나당 전종목 1번 호출 (시가 · 고가 · 저가 · 종가 · 거래량 · 거래대금 · 등락률) — 등락률로 수정주가 계산
  · 투자자별 순매수: 외국인 · 기관합계 · 연기금 (날짜 하나당 투자자별 1번) — 최근 3거래일은 매번 다시 받아 확정치 반영
  · ETF 일봉: KODEX 코스닥150(229200) · KODEX 200(069500)
  · 월 자료: 코스피200 · 코스닥150 구성 종목 · 업종 · 배당수익률 · PBR · EPS (배당·가치 칸)
예비 경로 KIS API — KRX가 안 될 때 오늘 일봉을 후보풀 종목만 현재가 조회로 (등락률 포함) · 수급은 이날 없음(중립)
KIS 종목 마스터(매일): 거래정지 · 정리매매 · 관리종목 · 시장경고 · ETF · 스팩 · 우선주 표시 (공식 샘플의 고정폭 규격)
데이터 품질: 그날 종목 수가 최근 5일 중앙값의 90% 미만이면 '불량'으로 표시 → 신호 계산 · 매수 안 함
"""
import contextlib
import csv
import io
import os
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timedelta

import tk_db as db

SLEEP = 1.0                                   # KRX 과부하 방지
INVESTORS = ['외국인', '기관합계', '연기금']
ETFS = {'229200': 'KODEX 코스닥150', '069500': 'KODEX 200'}
ETF_KW = ['KODEX', 'TIGER', 'KBSTAR', 'HANARO', 'KOSEF', 'ARIRANG', 'SOL ', 'ACE ', 'RISE ', 'PLUS ', 'TIMEFOLIO', 'FOCUS', 'ETN',
          '레버리지', '인버스', '선물', '채권', '국채', 'ETF']
BAD_KW = ['스팩', '리츠', '제1호', '제2호', '제3호', '기업인수목적']
MASTER_URL = 'https://new.real.download.dws.co.kr/common/master'
SECTOR_MAP = {'반도체': '전기·전자', 'IT부품': '전기·전자', '통신장비': '전기·전자', '정보기기': '전기·전자', '소프트웨어': 'IT 서비스', '인터넷': 'IT 서비스',
              '디지털컨텐츠': 'IT 서비스', '컴퓨터서비스': 'IT 서비스', '통신서비스': '통신', '방송서비스': '오락·문화', '출판·매체복제': 'IT 서비스',
              '기타금융': '금융', '증권': '금융', '보험': '금융', '은행': '금융', '전기·가스·수도': '전기·가스'}
STATE = {'running': False, 'msg': '', 'err': '', 'pct': 0, 'last': ''}
STOCK = [None]                                # pykrx.stock (시험에서는 가짜를 넣음)


def classify(ticker, name, market=''):
    up = (name or '').upper()
    if market == 'KONEX':
        return '코넥스'
    if any(k in up for k in ETF_KW):
        return 'ETF/ETN'
    if any(k in (name or '') for k in BAD_KW):
        return '스팩/리츠'
    if not str(ticker).endswith('0'):
        return '우선주'
    return ''


def _f(x):
    try:
        v = float(str(x).replace(',', ''))
        return None if v != v else v
    except (TypeError, ValueError):
        return None


def quiet(fn, *a, **k):
    cap = io.StringIO()
    with contextlib.redirect_stdout(cap), contextlib.redirect_stderr(cap):
        r = fn(*a, **k)
    return r, cap.getvalue()


def krx(cfg):
    """KRX 로그인 → pykrx.stock"""
    if STOCK[0] is not None:
        return STOCK[0]
    kid, kpw = cfg.get('krx_id'), cfg.get('krx_pw')
    if not (kid and kpw):
        raise RuntimeError('KRX 계정 없음 (⚙️ 설정 · data.krx.co.kr 무료 가입)')
    os.environ['KRX_ID'], os.environ['KRX_PW'] = kid, kpw
    for k in [k for k in sys.modules if k.startswith('pykrx')]:
        del sys.modules[k]
    cap = io.StringIO()
    with contextlib.redirect_stdout(cap), contextlib.redirect_stderr(cap):
        from pykrx import stock
    STOCK[0] = stock
    return stock


def _col(df, *names):
    for n in names:
        if n in df.columns:
            return df[n]
    raise KeyError(f'컬럼 없음 {names} / {list(df.columns)}')


def _retry(fn, *a, tries=3):
    last = None
    for k in range(tries):
        try:
            df, noise = quiet(fn, *a)
            if (df is None or len(df) == 0) and 'rror' in noise:
                raise RuntimeError(noise.strip().splitlines()[-1][:100])
            return df
        except Exception as e:
            last = e
            time.sleep(3 * (k + 1))
    raise last


def business_days(stock, frm, to):
    try:
        d, _ = quiet(stock.get_previous_business_days, fromdate=frm, todate=to)
        out = [x.strftime('%Y%m%d') for x in d]
        if out:
            return out
    except Exception:
        pass
    a, b, out = datetime.strptime(frm, '%Y%m%d'), datetime.strptime(to, '%Y%m%d'), []
    while a <= b:
        if a.weekday() < 5:
            out.append(a.strftime('%Y%m%d'))
        a += timedelta(days=1)
    return out


def _done(kind, key, n):
    c = db.mconn()
    c.execute('INSERT OR REPLACE INTO done VALUES (?,?,?,?)', (kind, key, n, db.now_s()))
    c.commit()


def done_keys(kind, minn=1):
    return {r[0] for r in db.mconn().execute('SELECT key FROM done WHERE kind=? AND n>=?', (kind, minn))}


# ════════════════════════════════════════════
#  KRX 수집
# ════════════════════════════════════════════
def tickers(stock):
    c = db.mconn()
    try:
        from pykrx.website.krx.market.ticker import StockTicker
        st, _ = quiet(StockTicker)
        rows = []
        for listed, df in ((1, st.listed), (0, st.delisted)):
            if df is None or len(df) == 0:
                continue
            for tk, r in df.iterrows():
                nm, mk = str(r.get('종목', '')), str(r.get('시장', ''))
                rows.append((str(tk), nm, mk, listed, classify(tk, nm, mk)))
    except Exception:
        rows = []
        for mk in ('KOSPI', 'KOSDAQ', 'KONEX'):
            lst, _ = quiet(stock.get_market_ticker_list, market=mk)
            for tk in lst:
                nm, _ = quiet(stock.get_market_ticker_name, tk)
                rows.append((tk, str(nm), mk, 1, classify(tk, str(nm), mk)))
    for tk, nm, mk, listed, ex in rows:
        c.execute("""INSERT INTO stocks (ticker, name, market, listed, excluded, updated) VALUES (?,?,?,?,?,?)
                     ON CONFLICT(ticker) DO UPDATE SET name=excluded.name, market=excluded.market, listed=excluded.listed,
                     excluded=CASE WHEN stocks.excluded LIKE '마스터:%' THEN stocks.excluded ELSE excluded.excluded END, updated=excluded.updated""",
                  (tk, nm, mk, listed, ex, db.now_s()))
    c.commit()
    return len(rows)


def bars_day(stock, day):
    df = _retry(stock.get_market_ohlcv_by_ticker, day, 'ALL')
    if df is None or len(df) == 0:
        return 0
    o, h, l, cl = _col(df, '시가'), _col(df, '고가'), _col(df, '저가'), _col(df, '종가')
    v, val, chg = _col(df, '거래량'), _col(df, '거래대금'), _col(df, '등락률')
    if bool((cl == 0).all()):
        return 0                                                    # 휴장일
    rows = []
    for tk in df.index:
        try:
            c0 = float(cl[tk])
            if c0 > 0:
                rows.append((day, str(tk), float(o[tk]), float(h[tk]), float(l[tk]), c0, float(v[tk]), float(val[tk]), float(chg[tk]), 'krx'))
        except (TypeError, ValueError):
            continue
    c = db.mconn()
    c.executemany('INSERT OR REPLACE INTO bars VALUES (?,?,?,?,?,?,?,?,?,?)', rows)
    c.commit()
    return len(rows)


def flows_day(stock, day, inv):
    df = _retry(stock.get_market_net_purchases_of_equities_by_ticker, day, day, 'ALL', inv)
    if df is None or len(df) == 0:
        return 0
    amt = _col(df, '순매수거래대금')
    rows = [(day, str(tk), inv, float(amt[tk])) for tk in df.index if _f(amt[tk]) is not None]
    c = db.mconn()
    c.executemany('INSERT OR REPLACE INTO flows VALUES (?,?,?,?)', rows)
    c.commit()
    return len(rows)


def etf_range(stock, frm, to):
    n = 0
    c = db.mconn()
    for tk in ETFS:
        df = _retry(stock.get_etf_ohlcv_by_date, frm, to, tk)
        if df is None or len(df) == 0:
            continue
        rows = []
        for d, r in df.iterrows():
            ds = d.strftime('%Y%m%d') if hasattr(d, 'strftime') else str(d)
            cl = _f(r.get('종가'))
            if cl and cl > 0:
                rows.append((ds, tk, _f(r.get('시가')), _f(r.get('고가')), _f(r.get('저가')), cl))
        c.executemany('INSERT OR REPLACE INTO etf VALUES (?,?,?,?,?,?)', rows)
        n += len(rows)
        time.sleep(SLEEP)
    c.commit()
    return n


def month(stock, day):
    """d(그달 첫 거래일) 구성 종목 · 업종 · 재무"""
    m = day[:6]
    mem = []
    for code, nm in (('1028', '코스피200'), ('2203', '코스닥150')):
        t = _retry(stock.get_index_portfolio_deposit_file, code, day)
        mem += [(m, x, nm) for x in (t or [])]
        time.sleep(SLEEP)
    if len(mem) < 300:
        raise RuntimeError(f'{m} 구성 종목 응답 부족 ({len(mem)})')
    rows = []
    for mk in ('KOSPI', 'KOSDAQ'):
        s = _retry(stock.get_market_sector_classifications, day, mk)
        time.sleep(SLEEP)
        f = _retry(stock.get_market_fundamental_by_ticker, day, mk)
        time.sleep(SLEEP)
        for tk, r in s.iterrows():
            fu = f.loc[tk] if tk in f.index else {}
            g = (lambda k: _f(fu.get(k)) if len(fu) else None)
            rows.append((m, day, tk, mk, r['종목명'], SECTOR_MAP.get(r['업종명'], r['업종명']), _f(r['시가총액']), g('EPS'), g('DIV'), g('PBR')))
    c = db.mconn()
    c.execute('DELETE FROM members WHERE month=?', (m,))
    c.executemany('INSERT OR REPLACE INTO members VALUES (?,?,?)', mem)
    c.executemany('INSERT OR REPLACE INTO monthly VALUES (?,?,?,?,?,?,?,?,?,?)', rows)
    c.commit()
    _done('month', m, len(mem))
    return len(mem)


def seed_months(path):
    """(선택) 우량주 앱 seed 폴더가 있으면 2019-01~ 월 자료를 한 번에 (KRX 호출 절약) — 없어도 KRX로 받음"""
    if not os.path.exists(os.path.join(path, 'const.csv')):
        return 0
    c = db.mconn()
    rows = [(r['date'][:6], r['ticker'].zfill(6), r['index']) for r in csv.DictReader(open(os.path.join(path, 'const.csv'), encoding='utf-8-sig'))]
    c.executemany('INSERT OR IGNORE INTO members VALUES (?,?,?)', rows)
    fund = {(r['date'], r['ticker'].zfill(6)): r for r in csv.DictReader(open(os.path.join(path, 'fund.csv'), encoding='utf-8-sig'))}
    out = []
    for r in csv.DictReader(open(os.path.join(path, 'sector.csv'), encoding='utf-8-sig')):
        tk = r['ticker'].zfill(6)
        fu = fund.get((r['date'], tk), {})
        out.append((r['date'][:6], r['date'], tk, r['market'], r['name'], SECTOR_MAP.get(r['sector'], r['sector']), _f(r['marcap']),
                    _f(fu.get('EPS')), _f(fu.get('DIV')), _f(fu.get('PBR'))))
    c.executemany('INSERT OR IGNORE INTO monthly VALUES (?,?,?,?,?,?,?,?,?,?)', out)
    c.commit()
    for m in sorted({r[0] for r in rows}):
        _done('month', m, 1)
    return len(rows)


# ════════════════════════════════════════════
#  KIS 종목 마스터 · KIS 예비 일봉
# ════════════════════════════════════════════
def kis_master():
    """KOSPI · KOSDAQ 마스터(고정폭) → 거래정지 · 정리매매 · 관리종목 · 시장경고 · ETP · SPAC · 우선주 표시"""
    specs = {'kospi': (228, [2, 1, 4, 4, 4] + [1] * 26 + [9, 5, 5, 1, 1, 1, 2, 1, 1, 1, 2, 2, 2, 3, 1, 3, 12, 12, 8, 15, 21, 2, 7, 1]),
             'kosdaq': (222, [2, 1, 4, 4, 4] + [1] * 21 + [9, 5, 5, 1, 1, 1, 2, 1, 1, 1, 2, 2, 2, 3, 1, 3, 12, 12, 8, 15, 21, 2, 7, 1])}
    idx = {'kospi': {'halt': 34, 'clean': 35, 'admin': 36, 'warn': 37, 'pref': 54},        # 공식 kis_kospi_code_mst.py 열 순서
           'kosdaq': {'halt': 29, 'clean': 30, 'admin': 31, 'warn': 32, 'pref': 49}}      # 공식 kis_kosdaq_code_mst.py 열 순서
    c = db.mconn()
    n = 0
    for mk, (tail, widths) in specs.items():
        req = urllib.request.Request(f'{MASTER_URL}/{mk}_code.mst.zip', headers={'User-Agent': 'Mozilla/5.0'})
        raw = urllib.request.urlopen(req, timeout=60).read()
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            text = z.open(f'{mk}_code.mst').read().decode('cp949', errors='ignore')
        for row in text.splitlines():
            if len(row) <= tail:
                continue
            head, rest = row[:len(row) - tail], row[-tail:]
            tk, nm = head[0:9].strip(), head[21:].strip()
            if len(tk) != 6:
                continue
            f, p = [], 0
            for w in widths:
                f.append(rest[p:p + w].strip())
                p += w
            ix = idx[mk]
            g = (lambda k: f[ix[k]] if ix[k] < len(f) else '')
            halt, admin = g('halt') == 'Y', g('admin') == 'Y'
            warn = g('warn') if g('warn') in ('02', '03') else ''                 # 02 투자경고 · 03 투자위험 (01 투자주의는 허용 — Scout와 같음)
            ex = '마스터:정리매매' if g('clean') == 'Y' else ('마스터:우선주' if g('pref') not in ('', '0') else '')   # ETF · 스팩은 이름으로 분류
            c.execute("""INSERT INTO stocks (ticker, name, market, listed, excluded, halt, admin, warn, updated) VALUES (?,?,?,1,?,?,?,?,?)
                         ON CONFLICT(ticker) DO UPDATE SET halt=excluded.halt, admin=excluded.admin, warn=excluded.warn,
                         excluded=CASE WHEN excluded.excluded!='' THEN excluded.excluded ELSE stocks.excluded END, listed=1, updated=excluded.updated""",
                      (tk, nm, 'KOSPI' if mk == 'kospi' else 'KOSDAQ', ex or classify(tk, nm), int(halt), int(admin), warn, db.now_s()))
            n += 1
    c.commit()
    db.gmeta_set('master_at', db.now_s())
    return n


def kis_today(kc, day, tickers_):
    """KRX가 안 될 때: 오늘 일봉을 현재가 조회로 (장 마감 뒤 · 등락률 포함 → 수정주가 계산 가능) — 후보풀 종목만"""
    rows = []
    for t in tickers_:
        try:
            p = kc.price(t)
        except Exception:
            continue
        if p['price'] > 0:
            rows.append((day, t, p['open'], p['high'], p['low'], p['price'], p['vol'], p['value'], p['chg'], 'kis'))
    c = db.mconn()
    c.executemany('INSERT OR REPLACE INTO bars VALUES (?,?,?,?,?,?,?,?,?,?)', rows)
    c.commit()
    return len(rows)


# ════════════════════════════════════════════
#  품질 · 전체 흐름
# ════════════════════════════════════════════
def quality(day):
    """그날 종목 수가 최근 5거래일 중앙값의 90% 이상이면 OK"""
    c = db.mconn()
    n = c.execute('SELECT COUNT(*) FROM bars WHERE date=?', (day,)).fetchone()[0]
    prev = [r[0] for r in c.execute("SELECT n FROM done WHERE kind='bars' AND key<? AND n>0 ORDER BY key DESC LIMIT 5", (day,))]
    if not prev:
        return n > 0, n, 0
    med = sorted(prev)[len(prev) // 2]
    return n >= med * 0.9, n, med


def run(cfg, start='20220601', flow_start='20230601', month_start='202106', kc=None, progress=None, full=True):
    """처음(backfill)이든 매일이든 같은 함수 — 이미 받은 것은 건너뜀 · 최근 3거래일 수급은 다시 받음"""
    say = progress or (lambda m: None)
    STATE.update(running=True, err='', msg='시작', pct=0)
    today = datetime.now().strftime('%Y%m%d')
    try:
        try:
            stock = krx(cfg)
        except Exception as e:
            stock = None
            STATE['err'] = str(e)[:200]
            db.log(f'KRX 수집 불가: {e} → KIS 예비 경로', 'warn')
        if stock is None:
            if kc is None:
                raise RuntimeError('KRX 계정도 KIS 키도 없어 자료를 받을 수 없음')
            d = today
            if datetime.now().strftime('%H:%M') < '15:40' or datetime.now().weekday() >= 5:
                raise RuntimeError('KIS 예비 경로는 평일 장 마감(15:40) 뒤에만')
            pool = pool_tickers()
            say(f'KIS 예비: {len(pool)}종목 현재가로 오늘 일봉')
            n = kis_today(kc, d, pool)
            _done('bars', d, n)
            db.log(f'KIS 예비 일봉 {d}: {n}종목 (수급 없음 → 중립)', 'warn')
            return {'bars': n, 'src': 'kis'}
        if full or not db.mconn().execute('SELECT COUNT(*) FROM stocks').fetchone()[0]:
            say('종목 목록 (상장 + 상장폐지)')
            tickers(stock)
        days = business_days(stock, start, today)
        if datetime.now().strftime('%H:%M') < '15:45' and days and days[-1] == today:
            days = days[:-1]                                                 # 오늘 자료는 장 마감 뒤에
        got = done_keys('bars', 1)
        todo = [d for d in days if d not in got]
        for i, d in enumerate(todo):
            STATE.update(msg=f'일봉 {d} ({i + 1}/{len(todo)})', pct=int(i / max(1, len(todo)) * 60))
            n = bars_day(stock, d)
            _done('bars', d, n)
            if n == 0:
                db.log(f'{d} 일봉 0건 (휴장 또는 KRX 오류)', 'warn')
            time.sleep(SLEEP)
        fdays = [d for d in days if d >= flow_start]
        recent = set(fdays[-3:])
        for inv in INVESTORS:
            fg = done_keys(f'flow_{inv}', 1)
            ftodo = [d for d in fdays if d not in fg or d in recent]
            for i, d in enumerate(ftodo):
                STATE.update(msg=f'{inv} 순매수 {d} ({i + 1}/{len(ftodo)})', pct=60 + int(i / max(1, len(ftodo)) * 12))
                n = flows_day(stock, d, inv)
                _done(f'flow_{inv}', d, n)
                time.sleep(SLEEP)
        if days:
            last_etf = db.mconn().execute('SELECT MAX(date) FROM etf').fetchone()[0] or start
            STATE.update(msg='ETF 일봉', pct=90)
            etf_range(stock, min(last_etf, days[-1]), days[-1])
        firsts = {}
        for d in days:
            firsts.setdefault(d[:6], d)
        mg = done_keys('month', 1)
        for m, d in sorted(firsts.items()):
            if m >= month_start and m not in mg:
                STATE.update(msg=f'월 자료 {m}', pct=94)
                try:
                    month(stock, d)
                except Exception as e:
                    db.log(f'{m} 월 자료 실패: {str(e)[:120]}', 'warn')
        ok, n, med = quality(days[-1]) if days else (False, 0, 0)
        if days and not ok:
            db.gmeta_set('dq_bad', days[-1])
            db.log(f'{days[-1]} 데이터 품질 미달: {n}종목 (최근 중앙값 {med}) → 신호 계산 보류', 'warn')
        else:
            db.gmeta_set('dq_bad', '')
        STATE.update(msg=f'끝 · 마지막 {days[-1] if days else "-"}', pct=100, last=days[-1] if days else '')
        return {'bars_days': len(todo), 'last': days[-1] if days else '', 'quality': ok}
    except Exception as e:
        STATE['err'] = str(e)[:300]
        db.log(f'수집 오류: {str(e)[:200]}', 'error')
        raise
    finally:
        STATE['running'] = False


def pool_tickers(min_value=3e9):
    """최근 20거래일 평균 거래대금 30억 이상 (KIS 예비 경로 대상)"""
    c = db.mconn()
    days = [r[0] for r in c.execute('SELECT DISTINCT date FROM bars ORDER BY date DESC LIMIT 20')]
    if not days:
        return []
    q = ','.join('?' * len(days))
    return [r[0] for r in c.execute(f"""SELECT b.ticker FROM bars b JOIN stocks s ON s.ticker=b.ticker AND s.excluded=''
                                        WHERE b.date IN ({q}) GROUP BY b.ticker HAVING AVG(b.value) >= ?""", (*days, min_value))]
