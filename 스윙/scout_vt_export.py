"""
scout_vt_export.py — 실전 가상매매 기록을 Claude에게 올리기 좋게 압축
· vtrades.csv   : 모든 가상매매 (트랙·신호일·종목·매수/매도일·가격·사유·수익률)
· summary.txt   : 트랙별 누적 성과 + 자동 판정 (재현성 · 대조군 비교)
· candles.csv   : 가상매매한 종목들의 해당 기간 일봉 (매도 판정을 다시 검증할 수 있게)
· flows.csv     : 같은 기간 외국인·연기금 순매수 (점수 재계산 검증용)
바탕화면에 scout_vtrades_날짜.zip 생성. 서버가 켜져 있어도 실행 가능 (읽기만 함).
"""
import os, io, csv, json, zipfile
from datetime import datetime, timedelta

import scout_db as db


def desktop():
    home = os.path.expanduser('~')
    for d in (os.path.join(home, 'Desktop'), os.path.join(home, 'OneDrive', 'Desktop'),
              os.path.join(home, 'OneDrive', '바탕 화면'), os.path.join(home, '바탕 화면')):
        if os.path.isdir(d):
            return d
    return os.path.dirname(os.path.abspath(__file__))


def to_csv(cur):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([d[0] for d in cur.description])
    n = 0
    for r in cur:
        w.writerow(r)
        n += 1
    return buf.getvalue().encode('utf-8-sig'), n


def main():
    db.init_db()
    db._vt_init()
    c = db.conn()
    first = c.execute("SELECT MIN(signal_date) FROM vtrades").fetchone()[0]
    if not first:
        print("아직 가상매매 기록이 없습니다. 18:20 이후 첫 기록이 생깁니다.")
        return
    # 판정 기준·기대치는 서버와 같은 값을 사용
    try:
        import scout_server as srv
        judge, crit = db.vt_judge(srv.VT_EXPECT_SIGN, srv.VT_EXPECT_NUM)
        names = srv.VT_NAMES
    except Exception:
        judge, crit, names = {}, db.vt_criteria(), {}
    st = db.vt_stats()
    start = (datetime.strptime(first, '%Y%m%d') - timedelta(days=60)).strftime('%Y%m%d')
    tickers = [r[0] for r in c.execute("SELECT DISTINCT ticker FROM vtrades")]
    q = ','.join('?' * len(tickers))
    out = os.path.join(desktop(), f"scout_vtrades_{datetime.now():%Y%m%d_%H%M}.zip")
    with zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        data, n1 = to_csv(c.execute("SELECT * FROM vtrades ORDER BY signal_date, grp, rank"))
        z.writestr('vtrades.csv', data)
        data, n2 = to_csv(c.execute(f"SELECT ticker,date,open,high,low,close,volume FROM candles "
                                    f"WHERE ticker IN ({q}) AND date >= ? ORDER BY ticker,date", (*tickers, start)))
        z.writestr('candles.csv', data)
        data, n3 = to_csv(c.execute("SELECT date,ticker,investor,amt FROM flows WHERE date >= ? "
                                    "AND investor IN ('외국인','연기금') ORDER BY date,ticker", (start,)))
        z.writestr('flows.csv', data)
        L = [f"생성 {datetime.now():%Y-%m-%d %H:%M} · 가상매매 시작 {first}",
             f"가상매매 {n1}건 · 일봉 {n2:,}행 · 수급 {n3:,}행", '',
             f"판정 기준 (확정 {crit.get('fixed_at', '')}): {json.dumps(crit, ensure_ascii=False)}", '']
        for g in db.VT_TRACKS:
            x = st.get(g, {})
            nm = names.get(g, g)
            if x.get('n'):
                L.append(f"[{nm}] 청산 {x['n']}건 · 승률 {x['win']:.0%} · 건당 {x['avg']:+.2f}% · 중앙 {x['median']:+.2f}% "
                         f"· 최악 {x['worst']:+.1f}% · 보유 {x['hold']:.1f}일 · 진행중 {x['open']}")
            else:
                L.append(f"[{nm}] 청산 없음 · 진행중 {x.get('open', 0)}")
            if g in judge:
                j = judge[g]
                L.append(f"    재현성: {j['repro']} {j['repro_why']}")
                L.append(f"    대조군 비교: {j['verdict']} — {j['why']}")
        z.writestr('summary.txt', '\n'.join(L).encode('utf-8'))
    print('\n'.join(L))
    print(f"\n파일: {out}  ({os.path.getsize(out)/1e6:.1f}MB)")
    print("이 zip 파일을 Claude에게 올려주세요.")


if __name__ == '__main__':
    main()
