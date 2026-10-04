"""
tk_dart.py — 📰 DART 공시 수집 · 악재 분류 · 효과 연구 (금융감독원 OpenDART)

· 인증키: opendart.fss.or.kr 무료 가입 → 인증키 신청 → ⚙️ 설정 'DART 인증키' (DPAPI 암호화 저장)
· 수집: 공시검색 API(list.json) — 날짜마다 주요사항보고(B) · 거래소공시(I) · 발행공시(C)만 (전체의 핵심 · 호출 수 절약)
         상장사(유가 Y · 코스닥 K)만 저장 → market.db dart 표 · 이미 받은 날은 건너뜀 · 최근 → 옛날 순서로 이어받기
· 일정: 거래일 18:05 오늘 공시 · 그 뒤 과거 채우기(기본 250거래일) — 하루 호출 한도(약 2만 건) 안에서
· 분류(tag): 공시 제목 키워드 → 악재(유상증자 · CB · BW · EB · 감자 · 횡령배임 · 상장폐지 · 관리종목 · 불성실공시 · 회생 · 소송 · 거래정지 · 영업정지)
             · 호재(자사주 취득 · 무상증자 · 공급계약 · 현금배당) · 기타
· 연구(study): 신호 후보(LVH · REV 상위 50) 중 '직전 5거래일 안에 악재 공시'가 있었던 종목 vs 없던 종목의 다음 날 시가 → 5일 뒤 종가 수익
· 매수 거르기(설정 dart_filter · 기본 꺼짐): 켜면 LVH · REV 후보 중 직전 5거래일 악재 공시 종목은 사지 않음 (이유를 판단 기록에 남김)
"""
import csv
import io
import json
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime

import tk_db as db

API = 'https://opendart.fss.or.kr/api/list.json'
KINDS = ('B', 'I', 'C')                                     # 주요사항보고 · 거래소공시 · 발행공시
STATE = {'running': False, 'stop': False, 'msg': '', 'err': '', 'day': '', 'day_i': 0, 'days_n': 0, 'n': 0, 'calls': 0, 'started': 0.0, 'ended': 0.0, 'pct': 0, 'eta': None}
SCHEMA = """
CREATE TABLE IF NOT EXISTS dart (rcept_no TEXT PRIMARY KEY, date TEXT, ticker TEXT, corp_name TEXT, cls TEXT, kind TEXT, report_nm TEXT,
    flr_nm TEXT, rm TEXT, tag TEXT, bad INTEGER);
CREATE INDEX IF NOT EXISTS ix_dart_tk ON dart(ticker, date);
CREATE INDEX IF NOT EXISTS ix_dart_d ON dart(date);
"""
BAD = [('유상증자', ('유상증자',)), ('CB', ('전환사채',)), ('BW', ('신주인수권부사채',)), ('EB', ('교환사채',)), ('감자', ('감자결정', '자본감소')),
       ('횡령배임', ('횡령', '배임')), ('상장폐지', ('상장폐지', '상장적격성')), ('관리종목', ('관리종목',)), ('불성실공시', ('불성실공시',)),
       ('회생', ('회생절차', '파산', '부도')), ('소송', ('소송등의제기', '소송 등의 제기', '가처분')), ('거래정지', ('매매거래정지', '거래정지')),
       ('영업정지', ('영업정지',)), ('감사의견', ('감사의견', '감사보고서 제출지연', '의견거절'))]
GOOD = [('자사주취득', ('자기주식취득', '자기주식 취득', '자사주 취득')), ('무상증자', ('무상증자',)), ('공급계약', ('단일판매', '공급계약')),
        ('현금배당', ('현금ㆍ현물배당', '현금배당', '현물배당'))]
BAD_TAGS = [t for t, _ in BAD]
_lock = threading.Lock()


def conn():
    c = db.mconn()
    if not getattr(conn, '_ok', False):
        c.executescript(SCHEMA)
        c.commit()
        conn._ok = True
    return c


def classify(title):
    """공시 제목 → (tag, 악재 1/호재 0/기타 0) — 정정 공시도 같은 태그 (원래 결정의 정정)"""
    t = (title or '').replace(' ', '')
    for tag, kws in BAD:
        if any(k.replace(' ', '') in t for k in kws):
            if tag in ('유상증자',) and '무상' in t and '유상' not in t:
                continue
            if tag == '거래정지' and '해제' in t:
                return '거래재개', 0
            return tag, 1
    for tag, kws in GOOD:
        if any(k.replace(' ', '') in t for k in kws):
            return tag, 0
    return '기타', 0


def fetch(key, d, kind, page=1):
    """OpenDART 공시검색 한 쪽 → dict (tests에서 바꿔 끼움)"""
    q = urllib.parse.urlencode({'crtfc_key': key, 'bgn_de': d, 'end_de': d, 'pblntf_ty': kind, 'page_no': page, 'page_count': 100, 'sort': 'date', 'sort_mth': 'asc'})
    with urllib.request.urlopen(f'{API}?{q}', timeout=20) as r:
        return json.loads(r.read().decode('utf-8'))


def collect_day(key, d):
    """하루치 → 저장한 건수 (상장사만) · 오류면 예외"""
    rows = []
    for kind in KINDS:
        page = 1
        while True:
            j = fetch(key, d, kind, page)
            STATE['calls'] += 1
            st = str(j.get('status', ''))
            if st == '013':                                              # 조회된 데이터 없음
                break
            if st != '000':
                raise RuntimeError(f"DART {st}: {j.get('message', '')}")
            for r in j.get('list') or []:
                if r.get('corp_cls') not in ('Y', 'K') or not r.get('stock_code'):
                    continue
                tag, bad = classify(r.get('report_nm'))
                rows.append((r['rcept_no'], r.get('rcept_dt') or d, str(r['stock_code']).zfill(6), r.get('corp_name'), r.get('corp_cls'), kind,
                             (r.get('report_nm') or '').strip(), r.get('flr_nm'), r.get('rm'), tag, bad))
            if page >= int(j.get('total_page') or 1):
                break
            page += 1
            time.sleep(0.15)
    c = conn()
    c.executemany('INSERT OR REPLACE INTO dart VALUES (?,?,?,?,?,?,?,?,?,?,?)', rows)
    c.execute('INSERT OR REPLACE INTO done VALUES (?,?,?,?)', ('dart', d, max(1, len(rows)), db.now_s()))
    c.commit()
    return len(rows)


def todo_days(n_days=250, today=None):
    today = today or datetime.now().strftime('%Y%m%d')
    days = [d for d in db.trading_days('0', today) if d <= today][-n_days:]
    if today not in days and datetime.now().weekday() < 5:
        days.append(today)
    got = {r[0] for r in conn().execute("SELECT key FROM done WHERE kind='dart'")}
    return [d for d in reversed(days) if d not in got or d == today]


def run(key, n_days=250, only_today=False, allowed=lambda: True):
    """오늘 + 과거 채우기 (최근 → 옛날) · 멈춤 · 하루 호출 한도에 닿으면 멈추고 다음에 이어받기"""
    if not key:
        raise ValueError('DART 인증키 없음 (⚙️ 설정 · opendart.fss.or.kr 무료 신청)')
    if not _lock.acquire(blocking=False):
        return 0
    days = todo_days(n_days)
    today = datetime.now().strftime('%Y%m%d')
    if only_today:
        days = [d for d in days if d == today]
    STATE.update(running=True, stop=False, err='', msg='시작', day_i=0, days_n=len(days), n=0, calls=0, started=time.time(), ended=0.0, pct=0, eta=None)
    done = 0
    try:
        for i, d in enumerate(days):
            if STATE['stop'] or not allowed():
                break
            STATE.update(day=d, day_i=i, msg=f'{d} ({i + 1}/{len(days)}일)')
            try:
                n = collect_day(key, d)
            except RuntimeError as e:
                if '020' in str(e) or '한도' in str(e):                      # 하루 사용 한도 초과 → 내일 이어서
                    STATE['err'] = '하루 호출 한도 초과 → 내일 이어받기'
                    break
                raise
            STATE['n'] += n
            done += 1
            el = time.time() - STATE['started']
            STATE.update(pct=int((i + 1) / len(days) * 100), eta=el / (i + 1) * (len(days) - i - 1))
        if done:
            db.log(f"📰 DART 공시 {done}일 · {STATE['n']}건 (호출 {STATE['calls']}회) · 남은 날 {len([x for x in todo_days(n_days) if x != today])}")
        STATE['msg'] = f"끝 · {done}일 · {STATE['n']}건"
        return done
    except Exception as e:
        STATE['err'] = str(e)[:200]
        db.log(f'DART 수집 오류: {str(e)[:200]}', 'warn')
        return done
    finally:
        STATE.update(running=False, ended=time.time())
        _lock.release()


def test_key(key):
    j = fetch(key, datetime.now().strftime('%Y%m%d'), 'B')
    st = str(j.get('status', ''))
    if st not in ('000', '013'):
        raise RuntimeError(f"DART {st}: {j.get('message', '')}")
    return {'ok': True, 'n': int(j.get('total_count') or 0)}


# ════════════════════════════════════════════
#  조회 · 매수 거르기 · 연구 · 내보내기
# ════════════════════════════════════════════
def status(n_days=250):
    c = conn()
    r = c.execute("SELECT COUNT(DISTINCT key), MIN(key), MAX(key) FROM done WHERE kind='dart'").fetchone()
    t = c.execute('SELECT COUNT(*), SUM(bad) FROM dart').fetchone()
    return {'days': r[0] or 0, 'first': r[1], 'last': r[2], 'rows': t[0] or 0, 'bad': t[1] or 0, 'left': len(todo_days(n_days)),
            **{k: STATE[k] for k in ('running', 'msg', 'err', 'day', 'pct', 'eta', 'n', 'calls', 'day_i', 'days_n')},
            'started_at': datetime.fromtimestamp(STATE['started']).strftime('%H:%M:%S') if STATE['started'] else '',
            'elapsed': round((STATE['ended'] or time.time()) - STATE['started']) if STATE['started'] else None}


def recent(limit=30, bad_only=True):
    q = 'SELECT * FROM dart' + (' WHERE bad=1' if bad_only else '') + ' ORDER BY date DESC, rcept_no DESC LIMIT ?'
    return [dict(r) for r in conn().execute(q, (limit,))]


def bad_recent(ticker, d, lookback=5):
    """d(그날 포함) 직전 lookback거래일 안의 악재 공시 → [(date, tag, 제목)]"""
    days = [x for x in db.trading_days('0', d) if x <= d][-lookback:]
    if not days:
        return []
    return [(r[0], r[1], r[2]) for r in conn().execute('SELECT date, tag, report_nm FROM dart WHERE ticker=? AND bad=1 AND date BETWEEN ? AND ? ORDER BY date DESC',
                                                       (ticker, days[0], d))]


def study(lookback=5, hold=5, frm='0', to='99999999'):
    """후보(LVH · REV 상위 50) 중 직전 lookback일 악재 공시 있음 vs 없음 → 다음 날 시가 매수 · hold일 뒤 종가 수익 (비용 전)"""
    m = conn()
    dd = {r[0] for r in m.execute("SELECT key FROM done WHERE kind='dart'")}
    if not dd:
        return {'n_days': 0, 'rows': []}
    days = db.trading_days('0', '99999999')
    idx = {d: i for i, d in enumerate(days)}
    cand = [tuple(r) for r in m.execute("SELECT date, sleeve, ticker FROM cands WHERE sleeve IN ('LVH','REV') AND rank<=50 AND date BETWEEN ? AND ?",
                                        (max(frm, min(dd)), min(to, max(dd))))]
    bad = {}
    for t, d, tag in m.execute('SELECT ticker, date, tag FROM dart WHERE bad=1'):
        bad.setdefault(t, []).append((d, tag))
    px = {}
    out = {}
    for d, s, t in cand:
        i = idx.get(d)
        if i is None or i + hold >= len(days):
            continue
        win = days[max(0, i - lookback + 1)], d
        tags = sorted({tg for dt, tg in bad.get(t, []) if win[0] <= dt <= win[1]})
        key = (s, tags[0] if tags else '없음')
        d1, dn = days[i + 1], days[i + hold]
        if (t, d1) not in px:
            r1 = m.execute('SELECT open FROM bars WHERE ticker=? AND date=?', (t, d1)).fetchone()
            rn = m.execute('SELECT close FROM bars WHERE ticker=? AND date=?', (t, dn)).fetchone()
            px[(t, d1)] = (r1[0] if r1 and r1[0] else None, rn[0] if rn and rn[0] else None)
        o, c = px[(t, d1)]
        if o and c:
            out.setdefault(key, []).append(c / o - 1)
    rows = []
    for (s, tag), v in sorted(out.items(), key=lambda kv: (kv[0][0], kv[0][1] != '없음', -len(kv[1]))):
        n = len(v)
        rows.append({'sleeve': s, 'tag': tag, 'n': n, 'avg': round(sum(v) / n * 100, 2), 'win': round(sum(1 for x in v if x > 0) / n * 100, 1),
                     'worst': round(min(v) * 100, 1)})
    return {'n_days': len(dd), 'lookback': lookback, 'hold': hold, 'rows': rows}


def export_csv(frm='0', to='99999999'):
    s = io.StringIO()
    w = csv.writer(s)
    cur = conn().execute('SELECT date, ticker, corp_name, cls, kind, tag, bad, report_nm, flr_nm, rm, rcept_no FROM dart WHERE date BETWEEN ? AND ? ORDER BY date, ticker',
                         (frm or '0', to or '99999999'))
    w.writerow([d[0] for d in cur.description])
    w.writerows(cur.fetchall())
    return '﻿' + s.getvalue()
