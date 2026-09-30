"""
scout_export.py — 백테스트·고도화용 데이터 내보내기
후보풀 종목의 일봉(3년치)·수급·종목정보를 CSV로 묶어 바탕화면에 zip 저장.
서버가 켜져 있어도 실행 가능 (읽기만 함).
"""
import os, io, csv, sys, zipfile
from datetime import datetime

import scout_db as db


def desktop():
    home = os.path.expanduser('~')
    for d in (os.path.join(home, 'Desktop'), os.path.join(home, 'OneDrive', 'Desktop'),
              os.path.join(home, 'OneDrive', '바탕 화면'), os.path.join(home, '바탕 화면')):
        if os.path.isdir(d):
            return d
    return os.path.dirname(os.path.abspath(__file__))


def write_csv(z, name, header, rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    n = 0
    for r in rows:
        w.writerow(r)
        n += 1
    z.writestr(name, buf.getvalue().encode('utf-8-sig'))   # 엑셀에서 열어도 한글 안 깨지게
    return n


def main():
    db.init_db()
    c = db.conn()
    pool = [r['ticker'] for r in c.execute("SELECT ticker FROM stocks WHERE in_pool=1")]
    if not pool:
        print("후보풀이 비어 있습니다. 서버에서 '전체 구축'을 먼저 해주세요.")
        return
    out = os.path.join(desktop(), f"scout_data_{datetime.now():%Y%m%d_%H%M}.zip")
    q = ','.join('?' * len(pool))
    print(f"후보풀 {len(pool)}종목 내보내는 중...")
    with zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        n1 = write_csv(z, 'stocks.csv',
                       ['ticker', 'name', 'market', 'sector', 'mktcap', 'shares', 'per', 'pbr', 'avg_value', 'warns'],
                       c.execute(f"SELECT ticker,name,market,sector,mktcap,shares,per,pbr,avg_value,warns "
                                 f"FROM stocks WHERE ticker IN ({q})", pool))
        n2 = write_csv(z, 'candles.csv', ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume'],
                       c.execute(f"SELECT ticker,date,open,high,low,close,volume FROM candles "
                                 f"WHERE ticker IN ({q}) ORDER BY ticker,date", pool))
        n3 = write_csv(z, 'investors.csv',
                       ['ticker', 'date', 'foreign_qty', 'inst_qty', 'foreign_amt', 'inst_amt'],
                       c.execute(f"SELECT ticker,date,foreign_qty,inst_qty,foreign_amt,inst_amt "
                                 f"FROM investors WHERE ticker IN ({q}) ORDER BY ticker,date", pool))
        n4 = write_csv(z, 'events.csv', ['ticker', 'date', 'type', 'title'],
                       c.execute("SELECT ticker,date,type,title FROM events ORDER BY date"))
        rng = c.execute(f"SELECT MIN(date), MAX(date) FROM candles WHERE ticker IN ({q})", pool).fetchone()
        z.writestr('info.txt', (f"생성: {datetime.now():%Y-%m-%d %H:%M}\n후보풀 {n1}종목\n일봉 {n2:,}건 "
                                f"({rng[0]}~{rng[1]})\n수급 {n3:,}건\n공시 {n4}건\n").encode('utf-8'))
    mb = os.path.getsize(out) / 1e6
    print(f"\n완료!  종목 {n1} · 일봉 {n2:,}건 ({rng[0]}~{rng[1]}) · 수급 {n3:,}건 · 공시 {n4}건")
    print(f"파일: {out}  ({mb:.1f}MB)")
    if mb > 30:
        print("※ 30MB를 넘어 업로드가 안 되면 알려주세요. 나눠서 만드는 방법을 드리겠습니다.")


if __name__ == '__main__':
    main()
