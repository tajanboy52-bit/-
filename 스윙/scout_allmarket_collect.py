"""
scout_allmarket_collect.py — 전종목 일봉 수집 (상장폐지 종목 포함, 생존편향 없는 백테스트용)

· 거래일마다 전 시장 시세를 한 번에 받음 (시가·고가·저가·종가·거래량·거래대금·등락률)
· 그날 상장돼 있던 종목은 나중에 상장폐지됐어도 전부 포함
· 상장/상폐 종목 목록으로 종목명·시장(코스피/코스닥/코넥스) 기록
· 원시 가격 저장 → 액면분할·증자 보정은 분석 단계에서 등락률로 처리
· 중간에 끊겨도 다시 실행하면 이어서 받음
· 끝나면 바탕화면에 연도별 zip 생성 (scout_allmarket_2022.zip ...)

한국거래소 정보데이터시스템 계정 필요 (data.krx.co.kr 무료 회원가입)
"""
import os, sys, io, csv, time, zipfile, sqlite3, contextlib
from datetime import datetime, timedelta

import scout_db as db
from scout_flow_collect import ask_login, import_pykrx

START = '20220901'      # 가격: 2023년부터 52주 지표를 쓰려면 1년 앞부터 필요
FLOW_START = '20230901' # 수급: 검증 구간(2024~) 앞 여유분만 있으면 충분
INVESTORS = ['외국인', '기관합계', '연기금']   # 날짜별 전종목 순매수 (신형 영문코드 문제 없음)
SLEEP = 1.0             # KRX 과부하 방지
REFETCH_RECENT = 3      # 최근 3영업일은 매번 다시 받음 (당일 확정치 반영)


def store():
    c = sqlite3.connect(os.path.join(db.DATA_DIR, 'allmarket.db'))
    c.execute("""CREATE TABLE IF NOT EXISTS px (date TEXT, ticker TEXT, o REAL, h REAL, l REAL, c REAL,
                 v INTEGER, val REAL, chg REAL, PRIMARY KEY (date, ticker))""")
    c.execute("CREATE TABLE IF NOT EXISTS done (date TEXT PRIMARY KEY, n INTEGER)")
    c.execute("""CREATE TABLE IF NOT EXISTS tickers (ticker TEXT, name TEXT, market TEXT, isin TEXT,
                 listed INTEGER, PRIMARY KEY (ticker, isin))""")
    c.execute("""CREATE TABLE IF NOT EXISTS flow (date TEXT, ticker TEXT, investor TEXT, amt REAL, qty REAL,
                 PRIMARY KEY (date, ticker, investor))""")
    c.execute("CREATE TABLE IF NOT EXISTS flow_done (date TEXT, investor TEXT, n INTEGER, PRIMARY KEY (date, investor))")
    c.commit()
    return c


def quiet(fn, *a, **k):
    """pykrx가 화면에 찍는 메시지를 가로채고, 에러 흔적을 함께 반환"""
    cap = io.StringIO()
    with contextlib.redirect_stdout(cap), contextlib.redirect_stderr(cap):
        r = fn(*a, **k)
    return r, cap.getvalue()


def col(df, *names):
    for n in names:
        if n in df.columns:
            return df[n]
    raise KeyError(f"컬럼 없음: {names} / 실제: {list(df.columns)}")


def fetch_tickers(c):
    """상장 + 상장폐지 종목 목록 (종목명·시장). 한 번만."""
    if c.execute("SELECT COUNT(*) FROM tickers").fetchone()[0] > 0:
        return
    try:
        from pykrx.website.krx.market.ticker import StockTicker
        st, noise = quiet(StockTicker)
        rows = []
        for listed, df in ((1, st.listed), (0, st.delisted)):
            if df is None or len(df) == 0:
                continue
            for tk, r in df.iterrows():
                rows.append((str(tk), str(r.get('종목', '')), str(r.get('시장', '')), str(r.get('ISIN', '')), listed))
        c.executemany("INSERT OR REPLACE INTO tickers VALUES(?,?,?,?,?)", rows)
        c.commit()
        nl = sum(1 for r in rows if r[4] == 1)
        print(f"  ✓ 종목 목록: 상장 {nl}개 · 상장폐지 {len(rows) - nl}개")
    except Exception as e:
        print(f"  ! 종목 목록 실패 ({str(e)[:70]}) — 이름 없이 진행 (분석엔 지장 없음)")


def business_days(stock, start, end):
    try:
        days, _ = quiet(stock.get_previous_business_days, fromdate=start, todate=end)
        out = [d.strftime('%Y%m%d') for d in days]
        if out:
            return out
    except Exception:
        pass
    # 대체: 평일 전부 (휴장일은 빈 응답으로 걸러짐)
    d, e, out = datetime.strptime(start, '%Y%m%d'), datetime.strptime(end, '%Y%m%d'), []
    while d <= e:
        if d.weekday() < 5:
            out.append(d.strftime('%Y%m%d'))
        d += timedelta(days=1)
    return out


def save_day(c, day, df):
    if df is None or len(df) == 0:
        return 0
    o, h, l, cl = col(df, '시가'), col(df, '고가'), col(df, '저가'), col(df, '종가')
    v, val, chg = col(df, '거래량'), col(df, '거래대금'), col(df, '등락률')
    if bool((cl == 0).all()):
        return 0                                        # 휴장일 (전부 0)
    rows = []
    for tk in df.index:
        try:
            ci = float(cl[tk])
            if ci <= 0:
                continue
            rows.append((day, str(tk), float(o[tk]), float(h[tk]), float(l[tk]), ci,
                         int(v[tk]), float(val[tk]), float(chg[tk])))
        except (TypeError, ValueError):
            continue
    c.executemany("INSERT OR REPLACE INTO px VALUES(?,?,?,?,?,?,?,?,?)", rows)
    return len(rows)


def save_flow(c, day, investor, df):
    if df is None or len(df) == 0:
        return 0
    amt, qty = col(df, '순매수거래대금'), col(df, '순매수거래량')
    rows = []
    for tk in df.index:
        try:
            rows.append((day, str(tk), investor, float(amt[tk]), float(qty[tk])))
        except (TypeError, ValueError):
            continue
    c.executemany("INSERT OR REPLACE INTO flow VALUES(?,?,?,?,?)", rows)
    return len(rows)


def collect_flows(c, stock, days):
    """투자자별 날짜 단위 전종목 순매수. 투자자 하나씩 전 기간을 끝내고 다음으로 (중요한 것부터)"""
    fdays = [d for d in days if d >= FLOW_START]
    recent = set(days[-REFETCH_RECENT:])
    for inv in INVESTORS:
        done = {r[0] for r in c.execute("SELECT date FROM flow_done WHERE investor=? AND n>0", (inv,))}
        todo = [d for d in fdays if d not in done or d in recent]
        if not todo:
            print(f"  ✓ {inv} 순매수 — 이미 수집 완료")
            continue
        print(f"\n  [{inv} 순매수] {len(todo)}일 남음 · 예상 {len(todo) * (SLEEP + 1.5) / 60:.0f}분")
        t0, n, streak, rows = time.time(), 0, 0, 0
        for day in todo:
            ok = False
            for attempt in range(3):
                try:
                    df, noise = quiet(stock.get_market_net_purchases_of_equities_by_ticker, day, day, 'ALL', inv)
                    if (df is None or len(df) == 0) and 'rror' in noise:
                        raise RuntimeError(noise.strip().splitlines()[-1][:80])
                    k = save_flow(c, day, inv, df)
                    c.execute("INSERT OR REPLACE INTO flow_done VALUES(?,?,?)", (day, inv, k))
                    c.commit()
                    rows += k
                    ok, streak = True, 0
                    break
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    last = str(e)[:80]
                    time.sleep(3 * (attempt + 1))
            if not ok:
                streak += 1
                print(f"  ! {day} {inv} 실패 ({last}) — 다음 실행 때 다시 받습니다")
                if streak >= 5:
                    print("\n[중단] 연속 실패. 10분쯤 뒤 전종목_수집.bat 을 다시 실행하세요 (받은 건 건너뜀).")
                    return False
            n += 1
            if n % 25 == 0 or n == len(todo):
                el = time.time() - t0
                print(f"[{datetime.now():%H:%M:%S}] {inv} {n}/{len(todo)}일 · 누적 {rows:,}행 · "
                      f"남은 약 {el / n * (len(todo) - n) / 60:.0f}분", flush=True)
            time.sleep(SLEEP)
    return True


def collect():
    db.init_db()
    ask_login()
    stock = import_pykrx()
    c = store()
    today = datetime.now().strftime('%Y%m%d')

    print("\n[1/4] 거래일·종목 목록 확인")
    days = business_days(stock, START, today)
    if not days:
        print("[오류] 거래일 목록을 못 받았습니다. KRX 로그인을 확인하세요.")
        sys.exit(1)
    fetch_tickers(c)

    # 접속 확인 (가장 최근 거래일)
    try:
        df, noise = quiet(stock.get_market_ohlcv_by_ticker, days[-1], 'ALL')
        if df is None or len(df) == 0:
            raise RuntimeError(noise.strip().splitlines()[-1] if noise.strip() else '빈 응답')
        print(f"  ✓ KRX 접속 확인 ({days[-1]} 전종목 {len(df)}개 · 항목: {', '.join(map(str, df.columns))})")
    except Exception as e:
        print(f"\n[오류] 전종목 시세 조회 실패: {str(e)[:150]}")
        print("  → KRX 로그인 실패이거나 KRX 사이트 점검 중일 수 있습니다.")
        sys.exit(1)

    done = {r[0] for r in c.execute("SELECT date FROM done")}
    recent = set(days[-REFETCH_RECENT:])
    todo = [d for d in days if d not in done or d in recent]
    print(f"\n[2/4] 일별 전종목 시세 수집 — 전체 {len(days)}거래일 ({days[0]}~{days[-1]}) · 남은 {len(todo)}일"
          f" · 예상 {len(todo) * (SLEEP + 1.5) / 60:.0f}분")
    print("      창을 닫아도 다음에 이어서 받습니다.\n")

    t0, n, streak, total_rows = time.time(), 0, 0, 0
    for day in todo:
        ok = False
        for attempt in range(3):
            try:
                df, noise = quiet(stock.get_market_ohlcv_by_ticker, day, 'ALL')
                if (df is None or len(df) == 0) and 'rror' in noise:
                    raise RuntimeError(noise.strip().splitlines()[-1][:80])
                k = save_day(c, day, df)
                c.execute("INSERT OR REPLACE INTO done VALUES(?,?)", (day, k))
                c.commit()
                total_rows += k
                ok = True
                streak = 0
                break
            except KeyboardInterrupt:
                raise
            except Exception as e:
                last = str(e)[:80]
                time.sleep(3 * (attempt + 1))
        if not ok:
            streak += 1
            print(f"  ! {day} 실패 ({last}) — 다음 실행 때 다시 받습니다")
            if streak >= 5:
                print("\n[중단] 연속으로 실패하고 있습니다. KRX 접속이 막혔거나 점검 중인 것 같습니다.")
                print("  → 10분쯤 뒤 전종목_수집.bat 을 다시 실행하세요. 받은 날짜는 건너뜁니다.")
                return c
        n += 1
        if n % 20 == 0 or n == len(todo):
            el = time.time() - t0
            print(f"[{datetime.now():%H:%M:%S}] {n}/{len(todo)}일 · 최근 {day} · 누적 {total_rows:,}행 · "
                  f"남은 약 {el / n * (len(todo) - n) / 60:.0f}분", flush=True)
        time.sleep(SLEEP)

    print(f"\n[3/4] 투자자별 순매수 (외국인 → 기관합계 → 연기금, {FLOW_START}~)")
    collect_flows(c, stock, days)
    return c


def export(c):
    from scout_export import desktop
    print("\n[4/4] 연도별 압축 파일 만들기")
    years = [r[0] for r in c.execute("SELECT DISTINCT substr(date,1,4) FROM px ORDER BY 1")]
    stamp = datetime.now().strftime('%Y%m%d_%H%M')
    outs = []
    for i, y in enumerate(years):
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(['date', 'ticker', 'open', 'high', 'low', 'close', 'volume', 'value_mil', 'chg'])
        k = 0
        for r in c.execute("SELECT date,ticker,o,h,l,c,v,val,chg FROM px WHERE substr(date,1,4)=? "
                           "ORDER BY ticker,date", (y,)):
            w.writerow((r[0], r[1], int(r[2]), int(r[3]), int(r[4]), int(r[5]), r[6],
                        round(r[7] / 1e6), round(r[8], 2)))
            k += 1
        path = os.path.join(desktop(), f"scout_allmarket_{y}_{stamp}.zip")
        with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as z:
            z.writestr(f'px_{y}.csv', buf.getvalue().encode('utf-8'))
            if i == 0:
                tb = io.StringIO()
                tw = csv.writer(tb)
                tw.writerow(['ticker', 'name', 'market', 'isin', 'listed'])
                tw.writerows(c.execute("SELECT ticker,name,market,isin,listed FROM tickers"))
                z.writestr('tickers.csv', tb.getvalue().encode('utf-8-sig'))
                nd = c.execute("SELECT COUNT(*), MIN(date), MAX(date) FROM done WHERE n>0").fetchone()
                nt = c.execute("SELECT COUNT(DISTINCT ticker) FROM px").fetchone()[0]
                z.writestr('info.txt', (f"생성 {datetime.now():%Y-%m-%d %H:%M}\n거래일 {nd[0]} ({nd[1]}~{nd[2]})\n"
                                        f"종목 {nt}개 (상장폐지 포함)\n가격: 원시(수정 전) · 거래대금: 백만원\n"
                                        f"파일: 연도별 {len(years)}개\n").encode('utf-8'))
        mb = os.path.getsize(path) / 1e6
        outs.append((path, mb, k))
        print(f"  ✓ {os.path.basename(path)}  {k:,}행 · {mb:.1f}MB")
    # 수급 (백만원 단위, 투자자별 가로형)
    fyears = [r[0] for r in c.execute("SELECT DISTINCT substr(date,1,4) FROM flow ORDER BY 1")]
    for y in fyears:
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(['date', 'ticker'] + [f'{i}_mil' for i in INVESTORS])
        cur, row, k = None, None, 0
        for d, tk, inv, amt in c.execute("SELECT date,ticker,investor,amt FROM flow WHERE substr(date,1,4)=? "
                                         "ORDER BY ticker,date", (y,)):
            if (d, tk) != cur:
                if row:
                    w.writerow(row); k += 1
                cur, row = (d, tk), [d, tk] + [''] * len(INVESTORS)
            if inv in INVESTORS:
                row[2 + INVESTORS.index(inv)] = round(amt / 1e6)
        if row:
            w.writerow(row); k += 1
        path = os.path.join(desktop(), f"scout_allflow_{y}_{stamp}.zip")
        with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as z:
            z.writestr(f'flow_{y}.csv', buf.getvalue().encode('utf-8'))
        mb = os.path.getsize(path) / 1e6
        outs.append((path, mb, k))
        print(f"  ✓ {os.path.basename(path)}  {k:,}행 · {mb:.1f}MB")
    print(f"\n완료! 바탕화면의 scout_allmarket_* · scout_allflow_* zip {len(outs)}개를 Claude에게 올려주세요.")
    if any(mb > 30 for _, mb, _ in outs):
        print("※ 30MB 넘는 파일이 있으면 알려주세요. 더 잘게 나누는 방법을 드리겠습니다.")


if __name__ == '__main__':
    c = collect()
    export(c)
