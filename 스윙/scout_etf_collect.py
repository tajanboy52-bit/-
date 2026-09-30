"""
scout_etf_collect.py — 지수 · ETF · 시장 투자자별 순매수 수집 (지수 ETF 타이밍 모델 검증용, v5.5)

· 지수 일봉: 코스피 · 코스피 200 · 코스닥 · 코스닥 150 (거래대금 · 상장 시가총액 포함)
· ETF 일봉 + NAV + 기초지수: KODEX 200 · TIGER 200 · KODEX 200TR · KODEX 코스피 · 레버리지 · 인버스 · 200선물인버스2X ·
  코스닥150 (+레버리지 · 선물인버스) · 반도체 · 은행 · 2차전지산업 · TIGER 200 IT · 미국 S&P500 · 나스닥100 · 단기채권 · 국고채10년
· 시장 전체 투자자별 순매수 금액: 코스피 · 코스닥 (개인 · 외국인 · 연기금 · 금융투자 등 세부)
· 2022-09 ~ 오늘 · 중간에 끊겨도 다시 실행하면 받은 구간은 건너뛰고 이어서 받음 (올해 구간은 매번 새로 받음)
· 끝나면 바탕화면에 scout_etf_날짜.zip 1개 → Claude에게 올려주세요

한국거래소 정보데이터시스템 계정 필요 (data.krx.co.kr 무료 회원가입 · 전종목_수집과 같은 계정)
"""
import os, sys, io, csv, time, zipfile, sqlite3
from datetime import datetime, timedelta

# 배치 파일 없이 실행하거나 출력을 파일로 돌려도 윈도우 기본 인코딩(cp949)에 없는 글자(✓ — 등) 때문에
# 멈추지 않게 → 출력은 항상 UTF-8, 못 쓰는 글자는 ?로 대체 (서버와 같은 방식)
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

import scout_db as db
from scout_flow_collect import ask_login, import_pykrx
from scout_allmarket_collect import quiet

START = '20220901'
INDEXES = {'1001': '코스피', '1028': '코스피 200', '2001': '코스닥', '2203': '코스닥 150'}
ETFS = {'069500': 'KODEX 200', '102110': 'TIGER 200', '278530': 'KODEX 200TR', '226490': 'KODEX 코스피',
        '122630': 'KODEX 레버리지', '114800': 'KODEX 인버스', '252670': 'KODEX 200선물인버스2X',
        '229200': 'KODEX 코스닥150', '233740': 'KODEX 코스닥150레버리지', '251340': 'KODEX 코스닥150선물인버스',
        '091160': 'KODEX 반도체', '091170': 'KODEX 은행', '305720': 'KODEX 2차전지산업', '139260': 'TIGER 200 IT',
        '360750': 'TIGER 미국S&P500', '133690': 'TIGER 미국나스닥100', '153130': 'KODEX 단기채권', '148070': 'KOSEF 국고채10년'}
MARKETS = ['KOSPI', 'KOSDAQ']
SLEEP = 1.0          # KRX 과부하 방지


def store():
    c = sqlite3.connect(os.path.join(db.DATA_DIR, 'etf.db'))
    c.execute("""CREATE TABLE IF NOT EXISTS idx (date TEXT, code TEXT, o REAL, h REAL, l REAL, c REAL, v REAL, val REAL,
                 cap REAL, PRIMARY KEY (date, code))""")
    c.execute("""CREATE TABLE IF NOT EXISTS etf (date TEXT, ticker TEXT, nav REAL, o REAL, h REAL, l REAL, c REAL, v REAL,
                 val REAL, base REAL, PRIMARY KEY (date, ticker))""")
    c.execute("""CREATE TABLE IF NOT EXISTS mflow (date TEXT, market TEXT, investor TEXT, val REAL,
                 PRIMARY KEY (date, market, investor))""")
    c.execute("CREATE TABLE IF NOT EXISTS done (kind TEXT, code TEXT, chunk TEXT, n INTEGER, PRIMARY KEY (kind, code, chunk))")
    c.execute("CREATE TABLE IF NOT EXISTS names (ticker TEXT PRIMARY KEY, name TEXT)")
    c.commit()
    return c


def chunks(start, end):
    """연도별 구간 [(시작, 끝, 올해 구간인지)] — KRX 조회 기간 제한을 피하고 이어받기 단위로 씀"""
    out = []
    y0, y1 = int(start[:4]), int(end[:4])
    for y in range(y0, y1 + 1):
        a = start if y == y0 else f'{y}0101'
        b = end if y == y1 else f'{y}1231'
        out.append((a, b, y == y1))
    return out


def colv(df, idx, *names):
    """이름이 조금 달라도 찾는 컬럼 값 (없으면 None)"""
    for n in names:
        if n in df.columns:
            try:
                return float(df.loc[idx, n])
            except (TypeError, ValueError):
                return None
    return None


def d8(idx):
    return idx.strftime('%Y%m%d') if hasattr(idx, 'strftime') else str(idx).replace('-', '')[:8]


def save_index(c, code, df):
    rows = []
    for i in df.index:
        cl = colv(df, i, '종가')
        if not cl or cl <= 0:
            continue
        rows.append((d8(i), code, colv(df, i, '시가'), colv(df, i, '고가'), colv(df, i, '저가'), cl,
                     colv(df, i, '거래량'), colv(df, i, '거래대금'), colv(df, i, '상장시가총액', '시가총액')))
    c.executemany("INSERT OR REPLACE INTO idx VALUES(?,?,?,?,?,?,?,?,?)", rows)
    return len(rows)


def save_etf(c, tk, df):
    rows = []
    for i in df.index:
        cl = colv(df, i, '종가')
        if not cl or cl <= 0:
            continue
        rows.append((d8(i), tk, colv(df, i, 'NAV'), colv(df, i, '시가'), colv(df, i, '고가'), colv(df, i, '저가'), cl,
                     colv(df, i, '거래량'), colv(df, i, '거래대금'), colv(df, i, '기초지수')))
    c.executemany("INSERT OR REPLACE INTO etf VALUES(?,?,?,?,?,?,?,?,?,?)", rows)
    return len(rows)


def save_mflow(c, mkt, df):
    rows = []
    for i in df.index:
        for inv in df.columns:
            v = colv(df, i, inv)
            if v is not None:
                rows.append((d8(i), mkt, str(inv), v))
    c.executemany("INSERT OR REPLACE INTO mflow VALUES(?,?,?,?)", rows)
    return len({r[0] for r in rows})


def run_jobs(c, stock, today):
    jobs = []
    for a, b, cur in chunks(START, today):
        jobs += [('idx', code, a, b, cur) for code in INDEXES]
        jobs += [('etf', tk, a, b, cur) for tk in ETFS]
        jobs += [('mflow', m, a, b, cur) for m in MARKETS]
    done = {(k, code, ch) for k, code, ch in c.execute("SELECT kind, code, chunk FROM done WHERE n>0")}
    todo = [j for j in jobs if j[4] or (j[0], j[1], j[2]) not in done]
    print(f"\n[2/3] 지수 {len(INDEXES)}개 · ETF {len(ETFS)}개 · 시장 수급 {len(MARKETS)}개 — 전체 {len(jobs)}구간 중 남은 {len(todo)}구간"
          f" · 예상 {len(todo) * (SLEEP + 1.5) / 60:.0f}분\n")
    fails, streak = [], 0
    t0 = time.time()
    for n, (kind, code, a, b, cur) in enumerate(todo, 1):
        ok, last = False, ''
        for attempt in range(3):
            try:
                if kind == 'idx':
                    df, noise = quiet(stock.get_index_ohlcv_by_date, a, b, code)
                elif kind == 'etf':
                    df, noise = quiet(stock.get_etf_ohlcv_by_date, a, b, code)
                else:
                    df, noise = quiet(stock.get_market_trading_value_by_date, a, b, code, detail=True)
                if (df is None or len(df) == 0) and 'rror' in noise:
                    raise RuntimeError(noise.strip().splitlines()[-1][:80])
                k = 0 if df is None or len(df) == 0 else (save_index(c, code, df) if kind == 'idx'
                                                          else save_etf(c, code, df) if kind == 'etf' else save_mflow(c, code, df))
                c.execute("INSERT OR REPLACE INTO done VALUES(?,?,?,?)", (kind, code, a, k))
                c.commit()
                ok, streak = True, 0
                break
            except KeyboardInterrupt:
                raise
            except Exception as e:
                last = str(e)[:80]
                time.sleep(3 * (attempt + 1))
        if not ok:
            streak += 1
            fails.append(f"{kind} {code} {a}~{b}: {last}")
            print(f"  ! {kind} {code} {a}~{b} 실패 ({last}) — 다음 실행 때 다시 받습니다")
            if streak >= 8:
                print("\n[중단] 연속으로 실패하고 있습니다. KRX 접속이 막혔거나 점검 중인 것 같습니다.")
                print("  → 10분쯤 뒤 ETF_수집.bat 을 다시 실행하세요. 받은 구간은 건너뜁니다.")
                return fails
        if n % 10 == 0 or n == len(todo):
            el = time.time() - t0
            print(f"[{datetime.now():%H:%M:%S}] {n}/{len(todo)} · 남은 약 {el / n * (len(todo) - n) / 60:.0f}분", flush=True)
        time.sleep(SLEEP)
    return fails


def fetch_names(c, stock):
    """ETF 이름 확인 (코드가 맞는지 점검용) — 실패해도 진행"""
    for tk, nm in ETFS.items():
        got = ''
        try:
            got, _ = quiet(stock.get_etf_ticker_name, tk)
        except Exception:
            got = ''
        c.execute("INSERT OR REPLACE INTO names VALUES(?,?)", (tk, str(got or nm)))
    c.commit()


def collect():
    db.init_db()
    ask_login()
    stock = import_pykrx()
    c = store()
    today = datetime.now().strftime('%Y%m%d')
    print("\n[1/3] KRX 접속 확인")
    try:
        year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        df, noise = quiet(stock.get_index_ohlcv_by_date, year_ago, today, '1001')
        if df is None or len(df) == 0:
            raise RuntimeError(noise.strip().splitlines()[-1] if noise.strip() else '빈 응답')
        print(f"  ✓ 코스피 지수 최근 1년 {len(df)}거래일 · 마지막 {d8(df.index[-1])} · 항목: {', '.join(map(str, df.columns))}")
    except Exception as e:
        print(f"\n[오류] 지수 조회 실패: {str(e)[:150]}")
        print("  → KRX 로그인 실패이거나 KRX 사이트 점검 중일 수 있습니다.")
        sys.exit(1)
    fetch_names(c, stock)
    fails = run_jobs(c, stock, today)
    return c, fails


def export(c, fails):
    from scout_export import desktop
    print("\n[3/3] 압축 파일 만들기")
    stamp = datetime.now().strftime('%Y%m%d_%H%M')
    path = os.path.join(desktop(), f"scout_etf_{stamp}.zip")
    names = dict(c.execute("SELECT ticker, name FROM names"))

    def table(header, rows):
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(header)
        k = 0
        for r in rows:
            w.writerow(r)
            k += 1
        return buf.getvalue().encode('utf-8-sig'), k
    rnd = lambda v, d=2: None if v is None else round(v, d)
    idx, ni = table(['date', 'code', 'name', 'open', 'high', 'low', 'close', 'volume', 'value_mil', 'mktcap_eok'],
                    ((d, code, INDEXES.get(code, code), rnd(o), rnd(h), rnd(l), rnd(cl), v,
                      None if val is None else round(val / 1e6), None if cap is None else round(cap / 1e8))
                     for d, code, o, h, l, cl, v, val, cap in c.execute("SELECT * FROM idx ORDER BY code, date")))
    etf, ne = table(['date', 'ticker', 'name', 'nav', 'open', 'high', 'low', 'close', 'volume', 'value_mil', 'base_index'],
                    ((d, tk, names.get(tk, ETFS.get(tk, tk)), rnd(nav), o, h, l, cl, v,
                      None if val is None else round(val / 1e6), rnd(base))
                     for d, tk, nav, o, h, l, cl, v, val, base in c.execute("SELECT * FROM etf ORDER BY ticker, date")))
    mf, nm = table(['date', 'market', 'investor', 'net_mil'],
                   ((d, m, inv, round(v / 1e6)) for d, m, inv, v in c.execute("SELECT * FROM mflow ORDER BY market, investor, date")))
    rng = lambda q: c.execute(q).fetchone()
    i_r, e_r, m_r = (rng("SELECT MIN(date), MAX(date), COUNT(DISTINCT code) FROM idx"),
                     rng("SELECT MIN(date), MAX(date), COUNT(DISTINCT ticker) FROM etf"),
                     rng("SELECT MIN(date), MAX(date), COUNT(DISTINCT market) FROM mflow"))
    per = {tk: n for tk, n in c.execute("SELECT ticker, COUNT(*) FROM etf GROUP BY ticker")}
    info = (f"생성 {datetime.now():%Y-%m-%d %H:%M}\n"
            f"지수 {i_r[2]}개 · {i_r[0]}~{i_r[1]} · {ni:,}행\n"
            f"ETF {e_r[2]}개 · {e_r[0]}~{e_r[1]} · {ne:,}행\n"
            f"시장 투자자별 순매수 {m_r[2]}개 시장 · {m_r[0]}~{m_r[1]} · {nm:,}행 (금액 백만원)\n"
            f"가격: ETF 원 · 지수 포인트 · 거래대금 백만원 · 시가총액 억원\n"
            + ''.join(f"  {tk} {names.get(tk, ETFS[tk])}: {per.get(tk, 0)}일\n" for tk in ETFS)
            + (f"\n실패 {len(fails)}건 (다시 실행하면 이어서 받음):\n" + '\n'.join('  ' + f for f in fails[:30]) + '\n' if fails else ''))
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        z.writestr('idx.csv', idx)
        z.writestr('etf.csv', etf)
        z.writestr('mflow.csv', mf)
        z.writestr('info.txt', info.encode('utf-8'))
    print(info)
    print(f"완료! 바탕화면의 {os.path.basename(path)} ({os.path.getsize(path) / 1e6:.1f}MB)을 Claude에게 올려주세요.")
    missing = [f"{tk} {ETFS[tk]}" for tk in ETFS if not per.get(tk)]
    if missing:
        print("※ 받지 못한 ETF: " + ', '.join(missing) + " — 코드가 바뀌었거나 상장폐지일 수 있습니다 (나머지로 분석 가능)")
    return path


if __name__ == '__main__':
    c, fails = collect()
    export(c, fails)
