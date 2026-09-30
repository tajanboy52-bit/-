"""
scout_flow_collect.py — 후보풀 종목의 3년치 수급 이력 수집 (백테스트 검증용)

· 투자자별 순매수 금액 (외국인·기관·개인·기타법인, 가능하면 연기금·투신 등 세부)
· 공매도 잔고 (KRX는 T+2일 기준 제공)
· 외국인 지분율

한국거래소 정보데이터시스템이 2025년 12월부터 회원제로 바뀌어 KRX 계정 로그인이 필요합니다.
(data.krx.co.kr 무료 회원가입)

중간에 끊겨도 다시 실행하면 받은 종목은 건너뛰고 이어서 받습니다.
끝나면 바탕화면에 scout_flows_날짜.zip 을 만듭니다.
"""
import os, sys, io, csv, time, zipfile, getpass, sqlite3
from datetime import datetime

import scout_db as db

SLEEP = 1.0          # KRX 과부하 방지 — 너무 빠르면 접속 차단될 수 있음 (pykrx 권고)


def ask_login():
    if os.environ.get('KRX_ID') and os.environ.get('KRX_PW'):
        return
    print("\n한국거래소(KRX) 정보데이터시스템 계정이 필요합니다.")
    print("  계정이 없으면 https://data.krx.co.kr 에서 무료 회원가입 후 다시 실행하세요.\n")
    kid = input("  KRX 아이디: ").strip()
    kpw = getpass.getpass("  KRX 비밀번호 (입력해도 화면에 안 보입니다): ").strip()
    if not kid or not kpw:
        print("아이디/비밀번호가 비어 있습니다.")
        sys.exit(1)
    os.environ['KRX_ID'], os.environ['KRX_PW'] = kid, kpw


def import_pykrx():
    try:
        from pykrx import stock          # 환경변수 설정 뒤에 불러와야 로그인됨
        return stock
    except Exception as e:
        msg = str(e)
        print(f"\n[오류] pykrx 불러오기 실패: {msg[:200]}")
        if '로그인' in msg or 'KRX_ID' in msg or 'login' in msg.lower():
            print("  → KRX 아이디/비밀번호를 확인하세요.")
        sys.exit(1)


def store():
    c = sqlite3.connect(os.path.join(db.DATA_DIR, 'flows.db'))
    c.execute("""CREATE TABLE IF NOT EXISTS flow (ticker TEXT, date TEXT, kind TEXT, col TEXT, val REAL,
                 PRIMARY KEY (ticker, date, kind, col))""")
    c.execute("CREATE TABLE IF NOT EXISTS done (ticker TEXT, kind TEXT, n INTEGER, PRIMARY KEY (ticker, kind))")
    c.commit()
    return c


def save_df(c, tk, kind, df):
    """DataFrame(날짜 인덱스 × 컬럼) → 긴 형식 저장. 컬럼명이 바뀌어도 그대로 보존"""
    if df is None or len(df) == 0:
        c.execute("INSERT OR REPLACE INTO done VALUES(?,?,0)", (tk, kind))
        c.commit()
        return 0
    rows = []
    for idx, r in df.iterrows():
        d = idx.strftime('%Y%m%d') if hasattr(idx, 'strftime') else str(idx).replace('-', '')[:8]
        for col, v in r.items():
            try:
                fv = float(v)
            except (TypeError, ValueError):
                continue
            rows.append((tk, d, kind, str(col), fv))
    c.executemany("INSERT OR REPLACE INTO flow VALUES(?,?,?,?,?)", rows)
    c.execute("INSERT OR REPLACE INTO done VALUES(?,?,?)", (tk, kind, len(df)))
    c.commit()
    return len(df)


def probe(stock, start, end):
    """로그인·데이터 접근 확인 (삼성전자 최근 구간)"""
    try:
        df = stock.get_market_trading_value_by_date(end[:6] + '01', end, '005930')
    except Exception as e:
        print(f"\n[오류] 수급 조회 실패: {str(e)[:200]}")
        print("  → KRX 로그인 실패이거나 KRX 사이트 점검 중일 수 있습니다.")
        return False
    if df is None or len(df) == 0:
        print("\n[오류] 수급 데이터가 비어 있습니다. KRX 로그인 정보를 확인하세요.")
        return False
    print(f"  ✓ KRX 접속 확인 (삼성전자 {len(df)}일 · 항목: {', '.join(map(str, df.columns))})")
    return True


def _fetch(stock, kind, tk, start, end, end_short, detail_ok):
    if kind == 'flow':
        if detail_ok[0]:
            try:
                df = stock.get_market_trading_value_by_date(start, end, tk, detail=True)
                if df is not None and len(df) and len(df.columns) > 5:
                    return df
            except Exception:
                pass
            detail_ok[0] = False             # 개별종목 세부 투자자 미지원 → 기본 항목으로
        return stock.get_market_trading_value_by_date(start, end, tk)
    if kind == 'short':
        return stock.get_shorting_balance_by_date(start, end_short, tk)
    return stock.get_exhaustion_rates_of_foreign_investment(start, end, tk)


def _business_days_back(yyyymmdd, n):
    from datetime import timedelta
    d = datetime.strptime(yyyymmdd, '%Y%m%d')
    k = 0
    while k < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            k += 1
    return d.strftime('%Y%m%d')


def collect():
    import contextlib
    db.init_db()
    pool = [r['ticker'] for r in db.conn().execute("SELECT ticker FROM stocks WHERE in_pool=1 ORDER BY ticker")]
    if not pool:
        print("후보풀이 비어 있습니다. Scout에서 '전체 구축'을 먼저 해주세요.")
        sys.exit(1)
    rng = db.conn().execute("SELECT MIN(date), MAX(date) FROM candles WHERE ticker IN (%s)"
                            % ','.join('?' * len(pool)), pool).fetchone()
    start, end = rng[0], rng[1]
    end_short = _business_days_back(end, 4)     # 공매도 잔고는 KRX가 2거래일 늦게 공개
    print(f"\n후보풀 {len(pool)}종목 · 기간 {start}~{end}")

    ask_login()
    stock = import_pykrx()
    if not probe(stock, start, end):
        sys.exit(1)

    c = store()
    # 실패(-1)한 항목과 비어 있던 공매도(0)는 다시 받음
    done = {(r[0], r[1]) for r in c.execute("SELECT ticker, kind, n FROM done")
            if not (r[2] == -1 or (r[1] == 'short' and r[2] == 0))}

    for code, nm in (('1001', '코스피'), ('2001', '코스닥')):      # 비교 기준 지수
        if ('IDX' + code, 'index') in done:
            continue
        try:
            save_df(c, 'IDX' + code, 'index', stock.get_index_ohlcv(start, end, code))
            print(f"  ✓ {nm} 지수 수신")
        except Exception as e:
            print(f"  ! {nm} 지수 실패: {str(e)[:80]} (없어도 분석은 가능)")
        time.sleep(SLEEP)

    detail_ok = [True]
    jobs = [('flow', '투자자별 순매수'), ('short', '공매도 잔고'), ('foreign', '외국인 지분율')]
    total = len(pool) * len(jobs)
    remain = sum(1 for tk in pool for k, _ in jobs if (tk, k) not in done)
    print(f"  남은 작업 {remain}/{total}건 · 예상 {remain * (SLEEP + 1.2) / 60:.0f}분 "
          f"(창을 닫아도 다음에 이어서 받습니다)\n")

    t0, n, errs, streak, warned = time.time(), 0, 0, 0, set()
    for i, tk in enumerate(pool):
        for kind, label in jobs:
            if (tk, kind) in done:
                continue
            if streak >= 8:
                print("\n[중단] 연속으로 실패하고 있습니다. KRX 로그인이 만료됐거나 일시 차단된 것 같습니다.")
                print("  → 10분쯤 뒤 수급이력_수집.bat 을 다시 실행하세요. 받은 데이터는 그대로 두고 이어서 받습니다.")
                return c, pool
            for attempt in range(3):
                try:
                    cap = io.StringIO()               # pykrx가 화면에 찍는 에러 가로채기
                    with contextlib.redirect_stdout(cap), contextlib.redirect_stderr(cap):
                        df = _fetch(stock, kind, tk, start, end, end_short, detail_ok)
                    noise = cap.getvalue()
                    if (df is None or len(df) == 0) and 'rror' in noise:
                        raise RuntimeError(noise.strip().splitlines()[-1][:80])
                    save_df(c, tk, kind, df)
                    streak = 0
                    break
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    if attempt == 2:
                        errs += 1
                        streak += 1
                        if kind not in warned:
                            print(f"  ! {label} 조회 실패 ({str(e)[:70]}) — 이 항목만 건너뛰고 계속합니다")
                            warned.add(kind)
                        c.execute("INSERT OR REPLACE INTO done VALUES(?,?,-1)", (tk, kind))
                        c.commit()
                    else:
                        time.sleep(3 * (attempt + 1))
            n += 1
            time.sleep(SLEEP)
        if (i + 1) % 10 == 0 or i == len(pool) - 1:
            el = time.time() - t0
            left = (el / max(1, n)) * (remain - n)
            print(f"[{datetime.now():%H:%M:%S}] {i+1}/{len(pool)}종목 · 실패 {errs} · 남은 약 {left/60:.0f}분", flush=True)
    return c, pool


def export(c, pool):
    """항목별 가로형 CSV로 압축 — 순매수는 백만원 단위 정수로 줄여 용량 최소화"""
    from scout_export import desktop
    out = os.path.join(desktop(), f"scout_flows_{datetime.now():%Y%m%d_%H%M}.zip")
    KEEP = {'short': ('공매도잔고', '비중'), 'foreign': ('지분율', '한도소진율'), 'index': ('시가', '고가', '저가', '종가')}
    summary = {}
    with zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        for kind in ('flow', 'short', 'foreign', 'index'):
            cols = [r[0] for r in c.execute("SELECT DISTINCT col FROM flow WHERE kind=? ORDER BY col", (kind,))]
            if kind in KEEP:
                cols = [x for x in cols if x in KEEP[kind]]
            else:
                cols = [x for x in cols if x != '전체']
            if not cols:
                continue
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(['ticker', 'date'] + cols)
            cur, row, n = None, None, 0
            q = ("SELECT ticker,date,col,val FROM flow WHERE kind=? AND col IN (%s) ORDER BY ticker,date"
                 % ','.join('?' * len(cols)))
            for tk, d, col, v in c.execute(q, (kind, *cols)):
                if (tk, d) != cur:
                    if row:
                        w.writerow(row)
                        n += 1
                    cur = (tk, d)
                    row = [tk, d] + [''] * len(cols)
                vv = round(v / 1e6) if kind == 'flow' else round(v, 3)   # 순매수: 백만원
                row[2 + cols.index(col)] = vv
            if row:
                w.writerow(row)
                n += 1
            z.writestr(f'{kind}.csv', buf.getvalue().encode('utf-8-sig'))
            summary[kind] = n
        fails = c.execute("SELECT COUNT(*) FROM done WHERE n=-1").fetchone()[0]
        z.writestr('info.txt', (f"생성 {datetime.now():%Y-%m-%d %H:%M}\n후보풀 {len(pool)}종목\n"
                                f"행 수 {summary}\n실패 {fails}건\n순매수 단위: 백만원\n").encode('utf-8'))
    print(f"\n완료!  {summary} · 실패 {fails}건")
    print(f"파일: {out}  ({os.path.getsize(out)/1e6:.1f}MB)")
    print("이 zip 파일을 Claude에게 올려주세요.")


if __name__ == '__main__':
    c, pool = collect()
    export(c, pool)
