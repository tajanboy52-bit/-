"""
scout_verify.py — 검증 데이터 내려받기 (v5.9)

가상 검증 기간(3개월) 동안 매일 쌓인 거래 · 체결 · 판정 · 로그를 날짜 구간으로 묶어 zip 하나로 만든다.
Claude에게 그대로 올리면 오류 · 문제점을 검증할 수 있게 원자료 + 자동 점검 결과를 같이 넣는다.
비밀 값(앱키 · 시크릿 · 계좌번호 · 텔레그램 · KRX · DART)은 넣지 않고, 로그에 섞여 있어도 가린다.
"""
import csv
import io
import json
import os
import zipfile
from datetime import datetime, timedelta

import scout_db as db
import scout_rt as rt

SAFE_CFG = ('rt_settings', 'model_order_max', 'vt_cash', 'vt_slots', 'at_mode', 'at_track', 'at_slots',
            'at_max_order_krw', 'at_max_daily_buys', 'at_max_positions', 'at_daily_loss_stop', 'at_dry_cash',
            'at_capital_live', 'legacy_features', 'min_trade_value', 'min_price', 'max_price', 'top_n')
SECRET_KEYS = ('app_key', 'app_secret', 'telegram_token', 'telegram_chat', 'krx_id', 'krx_pw', 'dart_key', 'account_no')
README = """TK Stock Scout 검증 데이터 ({frm} ~ {to}) · 만든 시각 {now} · 버전 {ver}

이 zip을 Claude에게 그대로 올리면 기간 중 거래 · 체결 · 판정의 오류와 문제점을 검증할 수 있습니다.
비밀 값(앱키 · 시크릿 · 계좌번호 · 텔레그램 · KRX · DART)은 들어 있지 않습니다.

checks.txt          자동 점검 결과 — 먼저 볼 것 (문제 후보 목록)
summary.json        날짜별 · 모델별 요약 (매수 · 청산 · 평균 수익 · 감시 가동) + 모델별 가상 계좌(백테스트형 · 실전형) 잔고 요약
rt_trades.csv       ⚡ 실전형 가상매매 거래 (모델 규칙을 장중 실시간으로 실행)
rt_fills.csv        실전형 매도 체결 한 건씩 (시각 · 가격 · 비율 · 사유)
rt_monitor.csv      장중 감시 가동 기록 (날짜별 첫/마지막 조회 · 횟수 · 실패 · 한 바퀴 시간)
rt_settings_log.csv 실전형 매도 설정 변경 이력
vtrades.csv         백테스트형 가상매매 (9개 모델 + 대조군 · 판정 기준)
at_orders.csv · at_positions.csv · at_snap.csv · at_log.csv   실전 리허설(DRY) · 실전(LIVE) 주문 · 보유 · 계좌 · 실행 로그
candles.csv         위 거래 종목들의 일봉 (기간 30일 전부터) — 체결가 · 판정 재검증용
bench.csv           비교 지수 ETF(KODEX 200) 일봉
job_runs.json       예약 작업 실행 기록 (동기화 · 스캔 · 수급 등 성공/실패 · 걸린 시간)
meta.json           배치 · 데이터 품질 · 게이트 상태
settings.json       매매 관련 설정 (비밀 값 제외)
server_log.txt      서버 로그 마지막 부분 (비밀 값 가림)
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


def _in(d, frm, to):
    return bool(d) and frm <= str(d)[:8] <= to


def _trade_rows(table, frm, to, open_status):
    rows = _q(f"SELECT * FROM {table}")
    return [r for r in rows if _in(r.get('signal_date'), frm, to) or _in(r.get('entry_date'), frm, to)
            or _in(r.get('exit_date'), frm, to) or (r.get('status') in open_status and (r.get('signal_date') or '') <= to)]


def checks(frm, to, rt_rows, vt_rows, candles, mon):
    """자동 점검 — 문제 후보를 사람이 읽는 문장으로"""
    out = []
    bar = {(c['ticker'], c['date']): c for c in candles}
    days = sorted({c['date'] for c in candles if frm <= c['date'] <= to})
    trading = [d for d in (db.recent_trading_dates(400) or []) if frm <= d <= to] or days

    def add(level, msg):
        out.append(f"[{level}] {msg}")
    # ① 실전형 매수가 = 그날 시가인지 (감시로 산 것)
    for t in rt_rows:
        if t['entry_price'] and t['entry_date'] and t['grp'] not in db.CLOSE_ENTRY:
            b = bar.get((t['ticker'], t['entry_date']))
            if b and b['open'] and abs(t['entry_price'] / b['open'] - 1) > 0.003:
                add('확인', f"실전형 {t['grp']} {t['name']}({t['ticker']}) {t['entry_date']} 매수가 {t['entry_price']:,.0f} ≠ 일봉 시가 {b['open']:,.0f}")
        # ② 매도 체결가가 그날 고가~저가 밖이면 불가능한 체결
        for f in json.loads(t.get('fills') or '[]'):
            b = bar.get((t['ticker'], f['d']))
            if b and b['low'] and b['high'] and not (b['low'] * 0.995 <= f['px'] <= b['high'] * 1.005):
                add('오류', f"실전형 {t['grp']} {t['name']} {f['d']} {f['t']} 매도가 {f['px']:,.0f} 가 그날 범위({b['low']:,.0f}~{b['high']:,.0f}) 밖 — {f['why']}")
        # ③ 매수 대기가 이틀 넘게 그대로
        if t['status'] == '대기':
            later = [d for d in trading if d > t['signal_date']]
            if len(later) >= 2:
                add('오류', f"실전형 {t['grp']} {t['name']} 신호 {t['signal_date']} 인데 {len(later)}거래일째 매수 대기")
        # ④ 보유일을 넘겨도 안 팔림
        if t['status'] == '보유' and t['held']:
            s = rt.snap(t)
            if s.get('hold') and t['held'] > s['hold'] + 1:
                add('오류', f"실전형 {t['grp']} {t['name']} 보유 {t['held']}일 > 규칙 {s['hold']}일 인데 아직 보유")
        if '일봉 보충' in (t.get('exit_reason') or '') or '일봉 보충' in (t.get('fills') or ''):
            add('참고', f"실전형 {t['grp']} {t['name']} {t.get('exit_date') or t.get('entry_date')} 일봉 보충 (그 시간 서버 꺼짐)")
    # ⑤ 백테스트형 매수가 = 시가 (종가베팅은 신호일 종가)
    for v in vt_rows:
        if v.get('entry_price') and v.get('entry_date'):
            b = bar.get((v['ticker'], v['entry_date']))
            ref = (b or {}).get('close' if v['grp'] in db.CLOSE_ENTRY else 'open')
            if ref and abs(v['entry_price'] / ref - 1) > 0.003:
                add('확인', f"백테스트형 {v['grp']} {v['name']} {v['entry_date']} 매수가 {v['entry_price']:,.0f} ≠ 일봉 {ref:,.0f}")
    # ⑥ 거래일인데 장중 감시 기록이 없음 / 한 바퀴가 너무 김
    md = {m['day']: m for m in mon}
    for d in trading:
        m = md.get(d)
        if not m:
            add('확인', f"{d} 장중 감시 기록 없음 (서버 꺼짐 또는 KIS 조회 실패) — 실전형은 일봉 보충")
        else:
            if (m.get('first_ts') or '')[11:16] > '09:05':
                add('참고', f"{d} 장중 감시 시작이 늦음 ({m['first_ts'][11:19]}) — 시가 매수·매도는 그때 가격")
            if (m.get('last_ts') or '')[11:16] < '15:15':
                add('확인', f"{d} 장중 감시가 {m['last_ts'][11:19]}에 멈춤 — 15:15 종가 판정 놓쳤을 수 있음")
            if (m.get('errors') or 0) > 0:
                add('참고', f"{d} 시세 조회 실패 {m['errors']}회")
            if (m.get('max_sec') or 0) > 60:
                add('참고', f"{d} 한 바퀴 최대 {m['max_sec']:.0f}초 (조회 종목 {m.get('max_tickers')}개) — 30초 목표보다 느림")
    # ⑦ 신호 배치가 없는 거래일
    batches = {v['signal_date'] for v in vt_rows}
    for d in trading:
        if d < to and d not in batches:
            add('확인', f"{d} 가상매매 신호 기록(18:20) 없음 — 데이터 품질 보류 또는 서버 꺼짐")
    return out or ['[정상] 자동 점검에서 문제 후보 없음']


def build(cfg, frm, to, version='', base_dir='', secrets=()):
    """(zip bytes, 파일 이름, 요약)"""
    rt.init()
    rt_rows = _trade_rows('rt_trades', frm, to, ('대기', '보유'))
    vt_rows = _trade_rows('vtrades', frm, to, ('대기', '보유'))
    at_orders = [r for r in _q("SELECT * FROM at_orders") if _in(r.get('date'), frm, to)]
    at_pos = [r for r in _q("SELECT * FROM at_positions") if _in(r.get('entry_date'), frm, to) or _in(r.get('exit_date'), frm, to) or r.get('status') == '보유']
    at_snap = [r for r in _q("SELECT * FROM at_snap") if _in(r.get('date'), frm, to)]
    at_log = [r for r in _q("SELECT * FROM at_log") if _in((r.get('ts') or '').replace('-', ''), frm, to)]
    mon = [r for r in _q("SELECT * FROM rt_monitor ORDER BY day") if _in(r.get('day'), frm, to)]
    slog = [r for r in _q("SELECT * FROM rt_settings_log ORDER BY ts") if (r.get('ts') or '')[:10].replace('-', '') <= to]
    tickers = sorted({r['ticker'] for r in rt_rows + vt_rows + at_orders + at_pos if r.get('ticker')})
    c0 = (datetime.strptime(frm, '%Y%m%d') - timedelta(days=45)).strftime('%Y%m%d')
    candles = []
    for i in range(0, len(tickers), 400):
        part = tickers[i:i + 400]
        candles += _q(f"SELECT ticker,date,open,high,low,close,volume FROM candles WHERE ticker IN ({','.join('?' * len(part))}) "
                      f"AND date BETWEEN ? AND ? ORDER BY ticker,date", (*part, c0, to))
    bench = _q(f"SELECT ticker,date,open,high,low,close FROM candles WHERE ticker IN ({','.join('?' * len(db.BENCH_TICKERS))}) "
               f"AND date BETWEEN ? AND ? ORDER BY date", (*db.BENCH_TICKERS, c0, to))
    fills = []
    for t in rt_rows:
        for f in json.loads(t.get('fills') or '[]'):
            fills.append({'grp': t['grp'], 'signal_date': t['signal_date'], 'ticker': t['ticker'], 'name': t['name'],
                          'entry_date': t['entry_date'], 'entry_price': t['entry_price'], 'fill_date': f['d'], 'fill_time': f['t'],
                          'fill_price': round(f['px'], 2), 'fraction': f['frac'], 'reason': f['why'],
                          'ret_pct': round((f['px'] / t['entry_price'] - 1) * 100, 3) if t['entry_price'] else None})
    # 요약
    by_day = {}
    for t in rt_rows:
        if _in(t['entry_date'], frm, to):
            by_day.setdefault(t['entry_date'], {}).setdefault('rt_buy', 0)
            by_day[t['entry_date']]['rt_buy'] += 1
        if t['status'] == '청산' and _in(t['exit_date'], frm, to):
            d = by_day.setdefault(t['exit_date'], {})
            d['rt_close'] = d.get('rt_close', 0) + 1
            d.setdefault('rt_rets', []).append(t['ret'])
    for d in by_day.values():
        r = d.pop('rt_rets', [])
        if r:
            d['rt_avg'] = round(sum(r) / len(r), 3)
    for m in mon:
        by_day.setdefault(m['day'], {})['monitor'] = f"{(m['first_ts'] or '')[11:19]}~{(m['last_ts'] or '')[11:19]} · {m['cycles']}회"
    summary = {'range': [frm, to], 'version': version, 'made': datetime.now().isoformat(timespec='seconds'),
               'counts': {'rt_trades': len(rt_rows), 'rt_fills': len(fills), 'vtrades': len(vt_rows), 'at_orders': len(at_orders),
                          'monitor_days': len(mon), 'tickers': len(tickers)},
               'by_day': dict(sorted(by_day.items())), 'rt_models': rt.stats(cfg)}
    try:
        import scout_vacct as vacct
        summary['vacct'] = {k: {g: vacct.account(k, g)['summary'] for g in rt.MODELS} for k in ('bt', 'rt')}
    except Exception as e:
        summary['vacct'] = {'error': str(e)[:200]}
    chk = checks(frm, to, rt_rows, vt_rows, candles, mon)
    meta = {k: db.meta_get(k, '') for k in ('vt_last_batch', 'rt_start', 'rt_last_cycle', 'rt_close_pass', 'dq_last',
                                            'dq_hold_dates', 'gate_passed_at', 'balance_baseline')}
    runs = {}
    try:
        runs = json.load(open(os.path.join(db.DATA_DIR, 'job_runs.json'), encoding='utf-8'))
    except Exception:
        pass
    log_txt = ''
    p_ = os.path.join(base_dir or '.', 'logs', 'server.log')
    if os.path.exists(p_):
        with open(p_, 'rb') as f:
            f.seek(max(0, os.path.getsize(p_) - 3_000_000))
            log_txt = f.read().decode('utf-8', 'replace')
    files = {
        'README.txt': README.format(frm=frm, to=to, now=datetime.now().strftime('%Y-%m-%d %H:%M'), ver=version),
        'checks.txt': '\n'.join(chk) + '\n',
        'summary.json': json.dumps(summary, ensure_ascii=False, indent=1, default=str),
        'rt_trades.csv': _csv(rt_rows), 'rt_fills.csv': _csv(fills), 'rt_monitor.csv': _csv(mon),
        'rt_settings_log.csv': _csv(slog), 'vtrades.csv': _csv(vt_rows),
        'at_orders.csv': _csv(at_orders), 'at_positions.csv': _csv(at_pos), 'at_snap.csv': _csv(at_snap), 'at_log.csv': _csv(at_log),
        'candles.csv': _csv(candles, ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']), 'bench.csv': _csv(bench),
        'job_runs.json': json.dumps(runs, ensure_ascii=False, indent=1, default=str),
        'meta.json': json.dumps(meta, ensure_ascii=False, indent=1),
        'settings.json': json.dumps({k: cfg.get(k) for k in SAFE_CFG}, ensure_ascii=False, indent=1),
        'server_log.txt': log_txt,
    }
    sec = [str(s) for s in secrets if s and len(str(s)) >= 10]                 # 긴 비밀 값: 모든 파일에서 가림
    short = [str(s) for s in secrets if s and 4 <= len(str(s)) < 10]           # 계좌번호 등 짧은 값: 숫자 자료가 깨지지 않게 로그 파일에서만
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        for name, txt in files.items():
            for s in sec + (short if name in ('server_log.txt', 'at_log.csv', 'job_runs.json', 'meta.json') else []):
                if s in txt:
                    txt = txt.replace(s, '●●●●')
            z.writestr(name, txt.encode('utf-8-sig') if name.endswith('.csv') else txt.encode('utf-8'))
    fn = f"scout_verify_{frm}_{to}.zip"
    return buf.getvalue(), fn, {'checks': chk, 'counts': summary['counts']}
