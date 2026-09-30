"""
danta_verify.py — 🎯 TK Danta 검증 데이터 내려받기 (D0.5.1 · Scout의 검증 데이터 zip과 같은 방식)

3개월 가상 검증 동안 날짜 구간을 골라 거래 · 매도 비교 · 매분 탐지 · 모델 판정 · 해당 종목 1분봉 · 로그를 zip 하나로.
Claude에게 그대로 올리면 오류 · 문제점 · 모델 성과를 검증할 수 있게 원자료 + 자동 점검(checks.txt)을 같이 넣는다.
비밀 값(앱키 · 시크릿 · 텔레그램)은 넣지 않고, 로그에 섞여 있어도 가린다.
"""
import csv
import io
import json
import os
import zipfile
from datetime import datetime, timedelta

import danta_db as db
import danta_exit as ex
import danta_live as live
import danta_models as mdl

LOG_FILE = os.path.join(db.DATA_DIR, 'danta_server.log')
README = """TK Danta 검증 데이터 ({frm} ~ {to}) · 만든 시각 {now} · 버전 {ver}

이 zip을 Claude에게 그대로 올리면 기간 중 가상 단타 거래 · 매도 · 탐지 · 모델 판정의 오류와 성과를 검증할 수 있습니다.
비밀 값(앱키 · 시크릿 · 텔레그램)은 들어 있지 않습니다.

checks.txt        자동 점검 결과 — 먼저 볼 것 (문제 후보 목록)
summary.json      날짜별 · 모델별 요약 · 모델 판정(models) · 판정 기준 · 설정
vtrades.csv       가상 단타 거래 (모델 · 매수/매도 시각 · 가격 · 수익률(비용 0.25% 차감) · 사유 · 매수 당시 규칙)
vexits.csv        같은 매수에 매도 프로필 A~E를 동시 적용한 결과
snaps.csv         매분 탐지 스냅샷 전부 (순위 출처 · 가격 · 등락률 · 거래량 · 거래대금 · 시가 · 고가 · 저가 · VWAP · 모델 신호 · 매수)
bars.csv          거래한 종목의 1분봉 (매수일 ~ 2거래일 뒤) — 체결가 · 매도 시점 재검증용
daily.csv         거래한 종목의 일봉 (Scout 일봉 · 읽기 전용) — 전일 종가 · 다음날 시가 확인용
settings_log.csv  가상 단타 규칙 변경 이력
days.csv          날짜별 개장 확인 · 탐지 횟수 · 첫/마지막 탐지 시각 (서버 가동 확인)
lab_models.json   분봉 모델 연구 마지막 결과 (있을 때)
runs.csv          분봉 수집 작업 기록
server_log.txt    서버 로그 마지막 부분 (비밀 값 가림)
"""


def _csv(rows, cols=None):
    buf = io.StringIO()
    w = csv.writer(buf)
    if rows and not cols:
        cols = list(rows[0].keys())
    w.writerow(cols or [])
    for r in rows:
        w.writerow([r.get(k) for k in cols])
    return buf.getvalue()


def _q(sql, args=()):
    try:
        return [dict(r) for r in db.conn().execute(sql, args)]
    except Exception:
        return []


def _hm_i(ts):
    return int(ts[11:13]) * 100 + int(ts[14:16])


def weekdays(frm, to):
    a, b = datetime.strptime(frm, '%Y%m%d'), datetime.strptime(to, '%Y%m%d')
    out = []
    while a <= b:
        if a.weekday() < 5:
            out.append(a.strftime('%Y%m%d'))
        a += timedelta(days=1)
    return out


def day_rows(frm, to):
    """날짜별 가동 기록 — 개장 확인 · 탐지 횟수 · 첫/마지막 탐지"""
    out = []
    for d in weekdays(frm, to):
        r = db.conn().execute("SELECT COUNT(DISTINCT hm), MIN(hm), MAX(hm) FROM snaps WHERE date=? AND hm>='09:00'", (d,)).fetchone()
        pre = db.conn().execute("SELECT COUNT(*) FROM snaps WHERE date=? AND hm<'09:00'", (d,)).fetchone()[0]
        out.append({'date': d, 'open': db.meta_get('open_' + d, ''), 'premarket_rows': pre, 'scan_minutes': r[0] or 0,
                    'first': r[1] or '', 'last': r[2] or '',
                    'buys': db.conn().execute("SELECT COUNT(*) FROM vtrades WHERE date=?", (d,)).fetchone()[0]})
    return out


def checks(frm, to, trades, bars, days, settings_):
    """자동 점검 — 문제 후보를 사람이 읽는 문장으로"""
    out = []

    def add(level, msg):
        out.append(f"[{level}] {msg}")
    B = {}
    for b in bars:
        B.setdefault((b['ticker'], b['date']), {})[int(b['hm'])] = b
    slip = settings_.get('slip', 0.1) / 100
    for t in trades:
        tag = f"[{t.get('model') or 'M1'}] {t['name']}({t['ticker']}) {t['date']}"
        bb = B.get((t['ticker'], t['date']))
        # ① 매수가가 매수 분의 1분봉 범위(± 슬리피지) 안인지
        if bb:
            h = _hm_i(t['buy_ts'])
            near = [bb[k] for k in (h - 1, h, h + 1) if k in bb]
            if near:
                lo, hi = min(x['low'] for x in near), max(x['high'] for x in near)
                if not (lo * (1 - 0.003) <= t['buy_px'] <= hi * (1 + slip + 0.003)):
                    add('오류', f"{tag} 매수가 {t['buy_px']:,.0f} 가 {t['buy_ts'][11:16]} 전후 1분봉 범위({lo:,.0f}~{hi:,.0f}) 밖")
            else:
                add('참고', f"{tag} {t['buy_ts'][11:16]} 1분봉 없음 — 매수가 재검증 불가 (분봉 수집 대상 밖)")
        # ② 매도가가 매도 분의 1분봉 범위 안인지
        if t['status'] == '청산' and t.get('sell_ts'):
            sd = t['sell_ts'][:10].replace('-', '')
            sb = B.get((t['ticker'], sd))
            if sb:
                h = _hm_i(t['sell_ts'])
                near = [sb[k] for k in (h - 1, h, h + 1) if k in sb] or ([sb[min(sb)]] if 'D+' in (t.get('reason') or '') or '다음날' in (t.get('reason') or '') else [])
                if near:
                    lo, hi = min(x['low'] for x in near), max(x['high'] for x in near)
                    if not (lo * (1 - slip - 0.003) <= t['sell_px'] <= hi * 1.003):
                        add('오류', f"{tag} 매도가 {t['sell_px']:,.0f} 가 {t['sell_ts'][11:16]} 전후 1분봉 범위({lo:,.0f}~{hi:,.0f}) 밖 — {t.get('reason')}")
        # ③ 규칙보다 오래 보유
        if t['status'] == '보유':
            rules = json.loads(t.get('settings') or '{}')
            hd = int(rules.get('hold_days') or 0)
            td = ex.trading_days(t['date'], to, live.holidays())
            if td > hd + 1:
                add('오류', f"{tag} 보유 {td}거래일째 — 규칙 {hd}일 (+상한가 1일) 넘음")
        # ④ 매수 시각이 모델 시간 밖
        m = t.get('model') or 'M1'
        if m in mdl.MODELS:
            a, b_ = mdl.window(m, json.loads(t.get('settings') or '{}') or settings_)
            if not (a <= t['buy_ts'][11:16] <= b_):
                add('확인', f"{tag} 매수 {t['buy_ts'][11:16]} 가 모델 시간({a}~{b_}) 밖")
    nob = sum(1 for t in trades if (t['ticker'], t['date']) not in B)
    if nob:
        add('참고', f"1분봉이 없는 거래 {nob}건 / {len(trades)}건 — 체결가 재검증 불가 (분봉 수집 대상 밖이거나 오늘분 수집 16:30 전)")
    # ⑤ 같은 모델 · 같은 종목 · 같은 날 두 번
    seen = {}
    for t in trades:
        k = (t.get('model') or 'M1', t['ticker'], t['date'])
        seen[k] = seen.get(k, 0) + 1
    for k, n in seen.items():
        if n > 1:
            add('오류', f"[{k[0]}] {k[1]} {k[2]} 같은 모델 · 같은 종목을 하루 {n}번 매수")
    # ⑥ 가동 기록
    for d in days:
        if d['open'] == '0':
            add('참고', f"{d['date']} 휴장 — 쉼")
            continue
        if d['scan_minutes'] == 0:
            add('확인', f"{d['date']} 탐지 기록 없음 (서버 꺼짐 · 앱키 없음 · 가상 단타 꺼짐 · 휴장)")
            continue
        if d['first'] > '09:03':
            add('참고', f"{d['date']} 첫 탐지 {d['first']} — 장 초반 모델(M1 · M2 · M5) 기회를 놓쳤을 수 있음")
        if d['last'] < '15:15':
            add('확인', f"{d['date']} 마지막 탐지 {d['last']} — 종가 모델(M7 · M8) 탐지 못 함")
        exp = 300               # 모델 시간 합계 09:01~14:30 + 15:15~15:19 ≈ 335분
        if d['scan_minutes'] < exp:
            add('참고', f"{d['date']} 탐지 {d['scan_minutes']}분 (서버가 중간에 꺼졌거나 조회가 느렸음)")
        if d['premarket_rows'] == 0:
            add('참고', f"{d['date']} 08:55 장전 기록 없음")
    # ⑦ 대조군 수
    by = {}
    for t in trades:
        by.setdefault(t['date'], {}).setdefault(t.get('model') or 'M1', 0)
        by[t['date']][t.get('model') or 'M1'] += 1
    for d, m in sorted(by.items()):
        if m.get('M1', 0) and m.get('Z', 0) < m.get('M1', 0):
            add('참고', f"{d} 대조군 {m.get('Z', 0)}건 < M1 {m['M1']}건 (필터 통과 다른 후보가 없던 분)")
    return out or ['[정상] 자동 점검에서 문제 후보 없음']


def build(cfg, frm, to, version='', secrets=()):
    """(zip bytes, 파일 이름, 요약)"""
    live.init()
    s = live.settings(cfg)
    trades = _q("SELECT * FROM vtrades WHERE date BETWEEN ? AND ? OR (status='보유' AND date<=?) ORDER BY id", (frm, to, to))
    ids = [t['id'] for t in trades]
    vex = []
    for i in range(0, len(ids), 500):
        part = ids[i:i + 500]
        vex += _q(f"SELECT * FROM vexits WHERE trade_id IN ({','.join('?' * len(part))}) ORDER BY trade_id, prof", part)
    snaps = _q("SELECT * FROM snaps WHERE date BETWEEN ? AND ? ORDER BY ts, passed DESC, chg DESC", (frm, to))
    # 거래 종목의 분봉 (매수일 ~ 2거래일 뒤)
    want = set()
    for t in trades:
        d0 = datetime.strptime(t['date'], '%Y%m%d')
        for k in range(0, 5):
            want.add((t['ticker'], (d0 + timedelta(days=k)).strftime('%Y%m%d')))
    bars = []
    for tk, d in sorted(want):
        bars += [dict(r, ticker=tk, date=d) for r in _q("SELECT hm, open, high, low, close, vol, amt FROM bars WHERE ticker=? AND date=? ORDER BY hm", (tk, d))]
    daily = []
    f = db.scout_db_path()
    if f and trades:
        import sqlite3
        sc = sqlite3.connect(f'file:{f}?mode=ro', uri=True, timeout=30)
        try:
            c0 = (datetime.strptime(frm, '%Y%m%d') - timedelta(days=10)).strftime('%Y%m%d')
            c1 = (datetime.strptime(to, '%Y%m%d') + timedelta(days=5)).strftime('%Y%m%d')
            for tk in sorted({t['ticker'] for t in trades}):
                daily += [dict(zip(('ticker', 'date', 'open', 'high', 'low', 'close', 'volume'), r)) for r in
                          sc.execute("SELECT ticker,date,open,high,low,close,volume FROM candles WHERE ticker=? AND date BETWEEN ? AND ? ORDER BY date",
                                     (tk, c0, c1))]
        finally:
            sc.close()
    days = day_rows(frm, to)
    chk = checks(frm, to, trades, bars, days, s)
    by_day = {}
    for t in trades:
        a = by_day.setdefault(t['date'], {})
        m = t.get('model') or 'M1'
        a.setdefault(m, {'buy': 0, 'closed': 0, 'rets': []})
        a[m]['buy'] += 1
        if t['status'] == '청산':
            a[m]['closed'] += 1
            a[m]['rets'].append(t['ret'])
    for a in by_day.values():
        for v in a.values():
            r = v.pop('rets')
            v['avg'] = round(sum(r) / len(r), 3) if r else None
    import danta_lab as lab
    summary = {'range': [frm, to], 'version': version, 'made': datetime.now().isoformat(timespec='seconds'),
               'counts': {'trades': len(trades), 'vexits': len(vex), 'snaps': len(snaps), 'bars': len(bars), 'daily': len(daily)},
               'by_day': dict(sorted(by_day.items())), 'models': live.models_report(s, lab.result_models()),
               'criteria': mdl.CRITERIA, 'criteria_text': mdl.CRITERIA_TEXT, 'settings': s,
               'profiles': {k: v['p'] for k, v in ex.PROFILES.items()},
               'model_defs': {k: {x: v[x] for x in ('name', 'win', 'exit', 'desc', 'evidence')} for k, v in mdl.MODELS.items()}}
    log_txt = ''
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'rb') as fh:
            fh.seek(max(0, os.path.getsize(LOG_FILE) - 2_000_000))
            log_txt = fh.read().decode('utf-8', 'replace')
    files = {
        'README.txt': README.format(frm=frm, to=to, now=datetime.now().strftime('%Y-%m-%d %H:%M'), ver=version),
        'checks.txt': '\n'.join(chk) + '\n',
        'summary.json': json.dumps(summary, ensure_ascii=False, indent=1, default=str),
        'vtrades.csv': _csv(trades), 'vexits.csv': _csv(vex), 'snaps.csv': _csv(snaps),
        'bars.csv': _csv(bars, ['ticker', 'date', 'hm', 'open', 'high', 'low', 'close', 'vol', 'amt']),
        'daily.csv': _csv(daily, ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']),
        'settings_log.csv': _csv(_q("SELECT * FROM settings_log ORDER BY ts")),
        'days.csv': _csv(days), 'runs.csv': _csv(_q("SELECT * FROM runs ORDER BY id")),
        'lab_models.json': db.meta_get('lab_models', '') or 'null',
        'server_log.txt': log_txt,
    }
    sec = [str(x) for x in secrets if x and len(str(x)) >= 6]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        for name, txt in files.items():
            for x in sec:
                if x in txt:
                    txt = txt.replace(x, '●●●●')
            z.writestr(name, txt.encode('utf-8-sig') if name.endswith('.csv') else txt.encode('utf-8'))
    return buf.getvalue(), f'danta_verify_{frm}_{to}.zip', {'checks': chk, 'counts': summary['counts']}
