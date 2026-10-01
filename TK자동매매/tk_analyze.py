"""
tk_analyze.py — TK자동매매 거래 기록 조회(HTS급) · 분석 · 고도화 후보

쌓인 기록(tk_journal)으로 "무엇이 돈을 벌고 무엇이 깎아 먹는지"를 숫자로 보여 준다
· 성적: 모드(모의 · 실전) · 칸 · 청산 이유 · 월 · 요일
· 진입 근거 구간: 순위 · 점수 · RSI · 변동성 · 고점 거리 · 과열 · 수급 · 갭 → 구간별 수익 (필터 후보 찾기)
· 체결 품질: 시가 대비 체결 차이 · 신호가 대비 갭 · 거절 · 접수/체결 시간 · 모델(백테스트 가정) 대비 실제
· 최대 역행/순행(MAE/MFE): 청산 규칙 연구용
· 신호 후보 순위별 사후 수익 · 월별 순위 상관(IC) → 신호가 약해지는지 감시
· 놓친 거래: 자리 · 현금 · 한도 때문에 못 산 종목의 사후 수익
· 모의 ↔ 실전 비교
· 고도화 후보: 근거 수치와 함께 목록으로만 제시 — 규칙은 자동으로 바꾸지 않는다 (과최적화 방지: 백테스트 → 모의 → 실전 순서로 확인)
"""
import csv
import io
import json
import math
import os
import zipfile
from datetime import datetime

import numpy as np
import pandas as pd

import tk_db as db

MODES = ('paper', 'real')
MODE_KO = {'paper': '모의', 'real': '실전'}
SLEEVE_KO = {'LVH': '저변동고점', 'REV': '반전·수급', 'DV': '배당·가치', 'ON': '밤사이', 'SW': '남는 현금(KODEX 200)', 'IN': '장중', 'MAN': '수동'}
EXIT_KO = {'hold20': '보유 기간 끝(LVH 10일)', 'ema9': '9EMA 복귀', 'hold10': '10일 만료', 'dv_rebal': '배당·가치 교체', 'on_sell': '밤사이 매도(시가)',
           'manual': '수동', 'delist': '거래 끊김 정리', 'sw_sell': 'KODEX 200 매도', 'sw_buy': 'KODEX 200 매수',
           'in_tp': '장중 익절', 'in_sl': '장중 손절', 'in_close': '장중 15:15 정리'}
HORIZON = {'LVH': 20, 'REV': 5, 'DV': 20}                      # 후보 사후 수익 기간(거래일) — 각 칸의 보통 보유 기간
FEATS = [('sig_rank', '순위'), ('sig_score', '점수'), ('rsi', 'RSI14'), ('atrp', 'ATR%'), ('fromhi', '250일 고점 거리'), ('heat', '과열'),
         ('fr20', '외국인 20일'), ('pen20', '연기금 20일'), ('div', '배당수익률'), ('pbr', 'PBR'), ('gap_in', '시가 갭%')]


# ════════════════════════════════════════════
#  HTS급 조회 화면
# ════════════════════════════════════════════
VIEWS = {
    'orders': ('주문 · 체결 내역', """SELECT date 날짜, substr(ts,12,8) 주문시각, sleeve 칸, name 종목, ticker 코드, CASE side WHEN 'buy' THEN '매수' ELSE '매도' END 구분,
                kind 사유, qty 주문수량, filled 체결수량, ROUND(avg) 체결평균가, CASE ord_dvsn WHEN '01' THEN '시장가' ELSE '지정가 '||CAST(price AS INT) END 방식,
                status 상태, order_no 주문번호, substr(ack_ts,12,8) 접수시각, substr(fill_ts,12,8) 체결확인, ROUND(sig_ref) 신호가, msg 메시지
                FROM orders WHERE date BETWEEN :frm AND :to {q} ORDER BY id DESC""", ('name', 'ticker', 'sleeve')),
    'fills': ('체결 조각', """SELECT date 날짜, substr(ts,12,8) 확인시각, sleeve 칸, name 종목, ticker 코드, CASE side WHEN 'buy' THEN '매수' ELSE '매도' END 구분,
                kind 사유, qty 수량, ROUND(price,1) 가격, ROUND(amount) 금액, fee 수수료, tax 세금, order_no 주문번호, src 출처
                FROM fills WHERE date BETWEEN :frm AND :to {q} ORDER BY id DESC""", ('name', 'ticker', 'sleeve')),
    'daily': ('매매일지 (날짜별)', """SELECT a.date 날짜, ROUND(a.equity) 평가금액, ROUND(a.equity - LAG(a.equity) OVER (ORDER BY a.date)) 전일대비,
                ROUND((a.equity / LAG(a.equity) OVER (ORDER BY a.date) - 1) * 100, 2) "등락%", ROUND(a.buy_amt) 매수금액, ROUND(a.sell_amt) 매도금액,
                ROUND(a.realized) 실현손익, a.fee 수수료, a.tax 세금, a.n_buy 매수건, a.n_sell 매도건, ROUND(a.cash) 예수금, ROUND(a.stock_value) 주식평가, a.note 비고
                FROM account_daily a WHERE a.date BETWEEN :frm AND :to ORDER BY a.date DESC""", ()),
    'stocks': ('종목별 손익', """SELECT ticker 코드, MAX(name) 종목, COUNT(*) 거래수, ROUND(AVG(CASE WHEN pnl>0 THEN 100.0 ELSE 0 END)) "승률%",
                ROUND(SUM(pnl)) 실현손익, ROUND(AVG(ret),2) "평균수익%", ROUND(SUM(fee)) 수수료, ROUND(SUM(tax)) 세금, GROUP_CONCAT(DISTINCT sleeve) 칸,
                MIN(entry_date) 첫매수, MAX(exit_date) 마지막매도 FROM lots WHERE status='청산' AND exit_date BETWEEN :frm AND :to {q}
                GROUP BY ticker ORDER BY SUM(pnl) DESC""", ('name', 'ticker', 'sleeve')),
    'trades': ('거래 (매수 → 매도 · 근거 · 체결 품질)', """SELECT id 번호, sleeve 칸, name 종목, ticker 코드, signal_date 신호일, entry_date 매수일, exit_date 매도일, days 보유일,
                ROUND(entry_px) 매수가, ROUND(exit_px) 매도가, qty0 수량, ROUND(pnl) 손익, ROUND(ret,2) "수익%", fee 수수료, tax 세금, exit_kind 청산,
                sig_rank 순위, ROUND(sig_score,3) 점수, slip_in "매수 시가대비%", slip_out "매도 시가대비%", gap_in "시가 갭%", mae "최대역행%", mfe "최대순행%",
                model_ret "모델수익%", entry_info 근거 FROM lots WHERE status='청산' AND exit_date BETWEEN :frm AND :to {q} ORDER BY exit_date DESC, id DESC""",
               ('name', 'ticker', 'sleeve')),
    'open': ('보유 묶음', """SELECT id 번호, sleeve 칸, name 종목, ticker 코드, entry_date 매수일, days 보유일, qty 수량, ROUND(entry_px) 매수가, ROUND(last_px) 현재가,
                ROUND((last_px/entry_px-1)*100,2) "평가%", ROUND(qty*(last_px-entry_px)) 평가손익, sig_rank 순위, sell_flag 매도표시, sell_reason 매도사유
                FROM lots WHERE status IN ('보유','주문') {q} ORDER BY sleeve, entry_date""", ('name', 'ticker', 'sleeve')),
    'positions': ('잔고 이력 (KIS 잔고 · 날짜별)', """SELECT date 날짜, ticker 코드, name 종목, qty 수량, ROUND(avg) 평균단가, ROUND(price) 현재가, ROUND(value) 평가금액,
                ROUND(pnl) 평가손익 FROM positions_daily WHERE date BETWEEN :frm AND :to {q} ORDER BY date DESC, value DESC""", ('name', 'ticker')),
    'decisions': ('판단 기록 (산 것 · 미룬 것 · 못 산 것)', """SELECT date 날짜, substr(ts,12,8) 시각, sig_date 신호일, sleeve 칸, name 종목, ticker 코드, rank 순위,
                ROUND(score,3) 점수, ROUND(ref) 기준가, qty 수량, ROUND(amt) 금액, CASE action WHEN 'buy' THEN '매수' WHEN 'sell' THEN '매도' WHEN 'defer' THEN '09:02로 미룸'
                ELSE '안 삼' END 판단, reason 이유 FROM decisions WHERE date BETWEEN :frm AND :to {q} ORDER BY id DESC""", ('name', 'ticker', 'sleeve')),
    'events': ('주문 상태 이력', """SELECT e.ts 시각, o.date 날짜, o.name 종목, CASE o.side WHEN 'buy' THEN '매수' ELSE '매도' END 구분, o.qty 수량, e.status 상태, e.detail 내용
                FROM order_events e JOIN orders o ON o.id=e.order_id WHERE o.date BETWEEN :frm AND :to {q} ORDER BY e.id DESC""", ('o.name', 'o.ticker', 'o.sleeve')),
    'broker': ('KIS 기간별 매매손익 (실전 · HTS 0856)', """SELECT date 날짜, ticker 코드, name 종목, kind 구분, buy_qty 매수수량, buy_amt 매수금액, sell_qty 매도수량,
                sell_amt 매도금액, pnl 실현손익, fee 수수료, tax 세금 FROM broker_pnl WHERE date BETWEEN :frm AND :to {q} ORDER BY date DESC""", ('name', 'ticker')),
    'ws': ('실시간 체결 통보 원본', """SELECT ts 받은시각, exec_time 체결시각, order_no 주문번호, ticker 코드, side 구분, qty 수량, price 가격 FROM ws_execs
                WHERE substr(replace(ts,'-',''),1,8) BETWEEN :frm AND :to {q} ORDER BY ts DESC""", ('ticker',)),
    'api': ('KIS 호출 통계', """SELECT date 날짜, tr TR, n 호출, err 오류, ROUND(ms / MAX(n,1)) "평균ms", last_err 마지막오류 FROM api_daily
                WHERE date BETWEEN :frm AND :to ORDER BY date DESC, n DESC""", ()),
}


def journal(view, mode='paper', frm='', to='', q='', limit=5000):
    """→ {'title', 'cols', 'rows'} · mode 'all'이면 모의 · 실전을 합치고 첫 열에 모드"""
    if view not in VIEWS:
        raise ValueError(f'알 수 없는 화면: {view}')
    title, sql, qcols = VIEWS[view]
    frm, to = (frm or '0').replace('-', ''), (to or '99999999').replace('-', '')
    qs = ''
    if q and qcols:
        qs = 'AND (' + ' OR '.join(f"{c} LIKE :q" for c in qcols) + ')'
    sql = sql.replace('{q}', qs)
    cols, rows = None, []
    for m in (MODES if mode == 'all' else (mode,)):
        cur = db.conn(m).execute(sql + f' LIMIT {int(limit)}', {'frm': frm, 'to': to, 'q': f'%{q}%'})
        c = [d[0] for d in cur.description]
        r = [list(x) for x in cur.fetchall()]
        if mode == 'all':
            c = ['모드'] + c
            r = [[MODE_KO[m]] + x for x in r]
        cols = c
        rows += r
    return {'title': title, 'cols': cols or [], 'rows': rows[:limit]}


def journal_csv(view, mode='paper', frm='', to='', q=''):
    j = journal(view, mode, frm, to, q, limit=1_000_000)
    s = io.StringIO()
    w = csv.writer(s)
    w.writerow(j['cols'])
    w.writerows(j['rows'])
    return '﻿' + s.getvalue()


# ════════════════════════════════════════════
#  분석
# ════════════════════════════════════════════
def _trades(modes=MODES, frm='', to=''):
    parts = []
    for m in modes:
        df = pd.read_sql_query("SELECT * FROM lots WHERE status='청산' AND exit_date BETWEEN ? AND ?", db.conn(m),
                               params=((frm or '0'), (to or '99999999')))
        if len(df):
            df['mode'] = m
            parts.append(df)
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    info = df['entry_info'].apply(lambda s: json.loads(s) if isinstance(s, str) and s.startswith('{') else {})
    for k, _ in FEATS:
        if k not in df.columns:
            df[k] = info.apply(lambda d: d.get(k) if isinstance(d, dict) else None)
    for c in ['ret', 'pnl', 'fee', 'tax', 'cost', 'slip_in', 'slip_out', 'gap_in', 'mae', 'mfe', 'model_ret', 'days'] + [k for k, _ in FEATS]:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    df['gross'] = df['ret'] + (df['fee'].fillna(0) + df['tax'].fillna(0)) / df['cost'].where(df['cost'] > 0) * 100
    return df


def _stat(r, pnl=None):
    r = pd.Series(r).dropna()
    n = len(r)
    if not n:
        return {'n': 0}
    sd = r.std(ddof=1) if n > 1 else float('nan')
    out = {'n': n, 'win': round(float((r > 0).mean() * 100), 1), 'avg': round(float(r.mean()), 3), 'med': round(float(r.median()), 3),
           't': round(float(r.mean() / (sd / math.sqrt(n))), 2) if n > 2 and sd and sd == sd else None,
           'best': round(float(r.max()), 2), 'worst': round(float(r.min()), 2)}
    if pnl is not None:
        p = pd.Series(pnl).dropna()
        g, l_ = p[p > 0].sum(), -p[p < 0].sum()
        out.update(pnl=round(float(p.sum())), pf=round(float(g / l_), 2) if l_ > 0 else None)
    return out


def _equity(m):
    rows = db.conn(m).execute('SELECT date, value FROM equity ORDER BY date').fetchall()
    if not rows:
        return None
    v = pd.Series([r[1] for r in rows], index=[r[0] for r in rows], dtype=float)
    sv = float(db.meta_get('start_value', '', m) or v.iloc[0] or 0)
    dd = (v / v.cummax() - 1).min()
    dr = v.pct_change().dropna()
    return {'days': len(v), 'first': v.index[0], 'last': v.index[-1], 'start': sv, 'end': float(v.iloc[-1]),
            'ret': round((float(v.iloc[-1]) / sv - 1) * 100, 2) if sv else None, 'mdd': round(float(dd) * 100, 2),
            'sharpe': round(float(dr.mean() / dr.std() * math.sqrt(252)), 2) if len(dr) > 5 and dr.std() > 0 else None}


def cand_forward(frm='', to='', min_days=20):
    """신호 후보(market.db cands) 순위 구간별 사후 수익 · 월별 IC"""
    c = db.mconn()
    cd = pd.read_sql_query('SELECT date, sleeve, ticker, rank, score FROM cands WHERE date BETWEEN ? AND ?', c, params=(frm or '0', to or '99999999'))
    if cd.empty:
        return None
    days = db.trading_days(cd['date'].min(), '99999999')
    end_i = min(len(days) - 1, len(days))
    P = db.panel(cd['date'].min(), days[end_i], cd['ticker'].unique().tolist())
    if not P:
        return None
    O = P['open']
    out = {'buckets': [], 'ic': [], 'recent': [], 'days': int(cd['date'].nunique()), 'src': dict(c.execute('SELECT src, COUNT(*) FROM cands GROUP BY src').fetchall())}
    for s, H in HORIZON.items():
        g = cd[cd['sleeve'] == s]
        if g.empty:
            continue
        fwd = (O.shift(-(1 + H)) / O.shift(-1) - 1) * 100                   # 다음 날 시가에 사서 H거래일 뒤 시가에 판다고 할 때
        st = fwd.stack()
        st.index.names = ['date', 'ticker']
        g = g.join(st.rename('fwd'), on=['date', 'ticker']).dropna(subset=['fwd'])
        g['fwd'] = g['fwd'].clip(-60, 150)
        if g['date'].nunique() < min_days:
            continue
        g['bk'] = pd.cut(g['rank'], [0, 3, 10, 20, 50], labels=['1~3', '4~10', '11~20', '21~50'])
        base = g['fwd'].mean()
        recent_days = sorted(g['date'].unique())[-60:]
        for b, x in g.groupby('bk', observed=True):
            xr = x[x['date'].isin(recent_days)]
            out['buckets'].append({'sleeve': s, 'h': H, 'bucket': str(b), 'n': len(x), 'avg': round(float(x['fwd'].mean()), 3),
                                   'win': round(float((x['fwd'] > 0).mean() * 100), 1), 'vs_all': round(float(x['fwd'].mean() - base), 3),
                                   'recent_avg': round(float(xr['fwd'].mean()), 3) if len(xr) else None, 'recent_n': len(xr)})
        ic = pd.Series({d_: -x['rank'].rank().corr(x['fwd'].rank()) for d_, x in g.groupby('date') if len(x) >= 10}, dtype=float).dropna()  # 순위 상관(스피어만)
        if len(ic):
            mi = ic.groupby(ic.index.str[:6]).mean()
            out['ic'] += [{'sleeve': s, 'month': k, 'ic': round(float(v), 3)} for k, v in mi.items()]
            out['recent'].append({'sleeve': s, 'ic_all': round(float(ic.mean()), 3), 'ic_60': round(float(ic.iloc[-60:].mean()), 3),
                                  'ic_t': round(float(ic.mean() / (ic.std() / math.sqrt(len(ic)))), 2) if len(ic) > 2 and ic.std() > 0 else None,
                                  'days': len(ic)})
    return out


def missed(modes=MODES):
    """못 산 후보(자리 · 현금 · 한도 등)의 사후 수익 vs 산 것"""
    parts = []
    for m in modes:
        df = pd.read_sql_query("SELECT date, sleeve, ticker, action, reason FROM decisions WHERE sleeve IN ('LVH','REV','DV') AND action!='sell'", db.conn(m))
        if len(df):
            df['mode'] = m
            parts.append(df)
    if not parts:
        return []
    df = pd.concat(parts, ignore_index=True)
    days = db.trading_days(df['date'].min(), '99999999')
    P = db.panel(df['date'].min(), days[-1] if days else '99999999', df['ticker'].unique().tolist())
    if not P:
        return []
    O = P['open']
    rows = []
    for s, H in HORIZON.items():
        g = df[df['sleeve'] == s]
        if g.empty:
            continue
        fwd = (O.shift(-H) / O - 1) * 100                                    # 판단한 날(=매수일) 시가 → H거래일 뒤 시가
        st = fwd.stack()
        st.index.names = ['date', 'ticker']
        g = g.join(st.rename('fwd'), on=['date', 'ticker'])
        g['why'] = np.where(g['action'].isin(['buy', 'defer']), '샀음(또는 09:02)', g['reason'].fillna('').str.replace(r'[\d,]+원', '…원', regex=True).str[:30])
        for (why), x in g.groupby('why'):
            f = x['fwd'].dropna().clip(-60, 150)
            rows.append({'sleeve': s, 'h': H, 'why': why, 'n': len(x), 'n_fwd': len(f), 'avg': round(float(f.mean()), 3) if len(f) else None})
    return rows


def analyze(modes=MODES, frm='', to=''):
    df = _trades(modes, frm, to)
    res = {'made': db.now_s(), 'modes': list(modes), 'frm': frm, 'to': to, 'summary': [], 'by_sleeve': [], 'by_exit': [], 'by_month': [],
           'by_weekday': [], 'buckets': [], 'execution': [], 'orders': [], 'api': [], 'mae_mfe': [], 'compare': [], 'equity': {}, 'cands': None,
           'missed': [], 'ideas': []}
    for m in modes:
        e = _equity(m)
        if e:
            res['equity'][m] = e
    if len(df):
        for m, g in df.groupby('mode'):
            s = _stat(g['ret'], g['pnl'])
            s.update(mode=MODE_KO[m], fee=round(float(g['fee'].sum())), tax=round(float(g['tax'].sum())), days=round(float(g['days'].mean()), 1),
                     first=g['entry_date'].min(), last=g['exit_date'].max())
            res['summary'].append(s)
        for (m, sl), g in df.groupby(['mode', 'sleeve']):
            res['by_sleeve'].append({'mode': MODE_KO[m], 'sleeve': sl, 'name': SLEEVE_KO.get(sl, sl), **_stat(g['ret'], g['pnl']),
                                     'days': round(float(g['days'].mean()), 1), 'gross': round(float(g['gross'].mean()), 3)})
        for (sl, ek), g in df.groupby(['sleeve', df['exit_kind'].fillna('?')]):
            res['by_exit'].append({'sleeve': sl, 'exit': EXIT_KO.get(ek, ek), **_stat(g['ret'], g['pnl'])})
        for (m, mo), g in df.groupby(['mode', df['exit_date'].str[:6]]):
            res['by_month'].append({'mode': MODE_KO[m], 'month': mo, **_stat(g['ret'], g['pnl'])})
        wd = pd.to_datetime(df['entry_date'], format='%Y%m%d', errors='coerce').dt.weekday
        for k, g in df.groupby(wd):
            res['by_weekday'].append({'weekday': '월화수목금토일'[int(k)], **_stat(g['ret'], g['pnl'])})
        # 진입 근거 구간별
        for sl, g in df[df['sleeve'].isin(['LVH', 'REV', 'DV'])].groupby('sleeve'):
            for k, ko in FEATS:
                v = g[[k, 'ret']].dropna()
                if len(v) < 15 or v[k].nunique() < 3:
                    continue
                try:
                    b = pd.qcut(v[k], min(5, v[k].nunique()), duplicates='drop')
                except ValueError:
                    continue
                for iv, x in v.groupby(b, observed=True):
                    res['buckets'].append({'sleeve': sl, 'feat': k, 'feat_ko': ko, 'range': f'{iv.left:.3g} ~ {iv.right:.3g}', **_stat(x['ret'])})
        # 체결 품질
        for (m, sl), g in df.groupby(['mode', 'sleeve']):
            res['execution'].append({'mode': MODE_KO[m], 'sleeve': sl, 'n': len(g),
                                     'slip_in': _mean(g['slip_in']), 'slip_out': _mean(g['slip_out']), 'gap_in': _mean(g['gap_in']),
                                     'model': _mean(g['model_ret']), 'gross': _mean(g['gross'].where(g['model_ret'].notna())),
                                     'drag': _mean((g['gross'] - g['model_ret']).dropna()), 'cost': _mean((g['gross'] - g['ret']).dropna())})
            v = g[['mae', 'mfe', 'ret']].dropna()
            if len(v):
                res['mae_mfe'].append({'mode': MODE_KO[m], 'sleeve': sl, 'n': len(v), 'mae': _mean(v['mae']), 'mfe': _mean(v['mfe']),
                                       'loser_mfe5': round(float(((v['ret'] <= 0) & (v['mfe'] >= 5)).sum() / max(1, (v['ret'] <= 0).sum()) * 100), 1),
                                       'winner_mae5': round(float(((v['ret'] > 0) & (v['mae'] <= -5)).sum() / max(1, (v['ret'] > 0).sum()) * 100), 1)})
        # 모의 ↔ 실전
        if df['mode'].nunique() == 2:
            for sl, g in df.groupby('sleeve'):
                a, b = g[g['mode'] == 'paper'], g[g['mode'] == 'real']
                if len(a) and len(b):
                    res['compare'].append({'sleeve': sl, 'paper_n': len(a), 'real_n': len(b), 'paper_avg': _mean(a['ret']), 'real_avg': _mean(b['ret']),
                                           'paper_slip': _mean(a['slip_in']), 'real_slip': _mean(b['slip_in'])})
    for m in modes:
        x = db.conn(m)
        o = x.execute("""SELECT COUNT(*), SUM(status='체결'), SUM(status='거절' OR status='거절(재시도)'), SUM(status='불분명'), SUM(status='만료'),
                         AVG((julianday(ack_ts)-julianday(ts))*86400), SUM(filled), SUM(qty) FROM orders WHERE date BETWEEN ? AND ?""",
                      (frm or '0', to or '99999999')).fetchone()
        if o[0]:
            top = x.execute("""SELECT msg, COUNT(*) n FROM orders WHERE status LIKE '거절%' AND date BETWEEN ? AND ? GROUP BY msg ORDER BY n DESC LIMIT 5""",
                            (frm or '0', to or '99999999')).fetchall()
            res['orders'].append({'mode': MODE_KO[m], 'n': o[0], 'done': o[1] or 0, 'rejected': o[2] or 0, 'ambiguous': o[3] or 0, 'expired': o[4] or 0,
                                  'ack_sec': round(o[5], 2) if o[5] is not None else None, 'fill_rate': round((o[6] or 0) / o[7] * 100, 1) if o[7] else None,
                                  'reject_top': [[r[0], r[1]] for r in top]})
        for r in x.execute('SELECT tr, SUM(n), SUM(err), SUM(ms) FROM api_daily WHERE date BETWEEN ? AND ? GROUP BY tr ORDER BY SUM(n) DESC',
                           (frm or '0', to or '99999999')):
            res['api'].append({'mode': MODE_KO[m], 'tr': r[0], 'n': r[1], 'err': r[2], 'err_pct': round(r[2] / r[1] * 100, 2) if r[1] else 0,
                               'ms': round(r[3] / r[1]) if r[1] else None})
    try:
        res['cands'] = cand_forward(frm, to)
    except Exception as e:
        res['cands'] = None
        db.log(f'후보 사후 수익 계산 실패: {e}', 'warn')
    try:
        res['missed'] = missed(modes)
    except Exception as e:
        db.log(f'놓친 거래 계산 실패: {e}', 'warn')
    res['ideas'] = ideas(res, df)
    return res


def _mean(s):
    s = pd.to_numeric(pd.Series(s), errors='coerce').dropna()
    return round(float(s.mean()), 3) if len(s) else None


# ════════════════════════════════════════════
#  고도화 후보 (근거와 함께 · 자동 변경 없음)
# ════════════════════════════════════════════
CHECK = '확인 순서: 🧪 백테스트에서 바꾼 규칙이 두 기간 모두 나은지 → 모의 20거래일 → 실전'


def ideas(res, df):
    out = []

    def add(level, title, why, what):
        out.append({'level': level, 'title': title, 'why': why, 'what': what})

    for r in res['by_sleeve']:
        if r['n'] >= 30 and r.get('t') is not None:
            if r['avg'] < 0 and r['t'] < -1.5:
                add('🔴', f"{r['mode']} {r['name']} 칸이 손실", f"거래 {r['n']}건 평균 {r['avg']:+.2f}% (t {r['t']}) · 승률 {r['win']}%",
                    '칸 비율을 줄이거나 쉬는 것을 검토 · 🎯 신호 탭에서 최근 신호 근거 확인')
            elif r['avg'] > 0 and r['t'] > 2:
                add('🟢', f"{r['mode']} {r['name']} 칸이 꾸준히 벌고 있음", f"거래 {r['n']}건 평균 {r['avg']:+.2f}% (t {r['t']})", '지금 규칙 유지 · 비중 확대는 백테스트로 먼저 확인')
    for r in res['execution']:
        if r['n'] >= 20 and r['slip_in'] is not None and r['slip_in'] > 0.3:
            add('🟠', f"{r['mode']} {r['sleeve']} 매수 체결이 시가보다 불리", f"평균 {r['slip_in']:+.2f}% ({r['n']}건)",
                '장전 동시호가 지정가(시가 근처 상한) 주문 방식 검토')
        if r['n'] >= 20 and r['drag'] is not None and r['drag'] < -0.3:
            add('🟠', f"{r['mode']} {r['sleeve']} 실제가 모델(백테스트 가정)보다 낮음", f"거래당 {r['drag']:+.2f}%p (수수료 · 세금 제외 기준)",
                '체결 시각 · 방식 점검 — 백테스트 비용 가정도 이 값으로 올려서 다시 계산')
    for r in res['mae_mfe']:
        if r['n'] >= 20 and r['loser_mfe5'] >= 40:
            add('🟡', f"{r['mode']} {r['sleeve']} 손실 거래 중 {r['loser_mfe5']}%가 한때 +5% 이상이었음", f"평균 최대순행 {r['mfe']:+.2f}% · 최대역행 {r['mae']:+.2f}%",
                '이익 보호 청산 규칙 연구 후보 (단, 전종목 검증에서 손절은 수익을 깎았음 → 백테스트 필수)')
    for r in res['orders']:
        if r['ambiguous']:
            add('🔴', f"{r['mode']} 결과 불분명 주문 {r['ambiguous']}건", '네트워크 · KIS 응답 문제', '🖥 시스템 로그 · 📒 주문 상태 이력 확인 (실전 전환 판정 항목)')
        if r['n'] >= 20 and r['rejected'] / r['n'] > 0.05:
            add('🟠', f"{r['mode']} 주문 거절 {r['rejected']}/{r['n']}건", ' · '.join(f'{m} {n}건' for m, n in r['reject_top'][:3]), '거절 메시지별 원인 수정 (시간 · 가격 · 잔고)')
    for r in res['api']:
        if r['n'] >= 100 and r['err_pct'] > 5:
            add('🟠', f"{r['mode']} KIS {r['tr']} 오류율 {r['err_pct']}%", f"{r['n']}회 중 {r['err']}회", '호출 간격 · 토큰 · 네트워크 점검')
    c = res.get('cands') or {}
    bk = {(b['sleeve'], b['bucket']): b for b in c.get('buckets', [])}
    for s in HORIZON:
        a, b = bk.get((s, '1~3')), bk.get((s, '4~10'))
        if a and b and a['n'] >= 60 and b['avg'] - a['avg'] > 0.5:
            add('🟡', f"{s} 순위 4~10이 1~3보다 나음", f"{a['h']}거래일 사후 수익 1~3 {a['avg']:+.2f}% · 4~10 {b['avg']:+.2f}% ({a['n']}/{b['n']}건)",
                '상위 3만 사지 말고 더 넓게(자리 늘리고 종목당 비중 줄이기) 검토')
        if a and a['n'] >= 60 and a['vs_all'] < -0.3:
            add('🟠', f"{s} 상위 3 후보가 전체 후보 평균보다 못함", f"평균 대비 {a['vs_all']:+.2f}%p", '점수 정의 재검토 — 신호가 약해졌을 수 있음')
    for r in c.get('recent', []):
        if r['days'] >= 60 and r['ic_all'] > 0.02 and r['ic_60'] < 0:
            add('🟠', f"{r['sleeve']} 최근 60일 신호 약화", f"순위 상관(IC) 전체 {r['ic_all']:+.3f} → 최근 {r['ic_60']:+.3f}", '국면 변화인지 관찰 · 계속되면 비중 축소 검토')
    mb = {}
    for r in res['missed']:
        mb.setdefault(r['sleeve'], []).append(r)
    for s, rows in mb.items():
        got = next((r for r in rows if r['why'].startswith('샀음')), None)
        for r in rows:
            if got and r is not got and r['n_fwd'] >= 20 and r['avg'] is not None and got['avg'] is not None and r['avg'] > got['avg'] + 0.5:
                add('🟡', f"{s} 못 산 후보({r['why']})가 산 것보다 나음", f"못 산 {r['n_fwd']}건 {r['avg']:+.2f}% · 산 것 {got['avg']:+.2f}%",
                    '자리 수 · 칸 비율 · 하루 한도 검토')
    for sl in ('LVH', 'REV', 'DV'):
        rows = [b for b in res['buckets'] if b['sleeve'] == sl and b['n'] >= 20]
        for f in {b['feat'] for b in rows}:
            fb = [b for b in rows if b['feat'] == f]
            if len(fb) >= 3:
                worst = min(fb, key=lambda b: b['avg'])
                rest = [b['avg'] for b in fb if b is not worst]
                if worst['avg'] < -1 and min(rest) > 0:
                    add('🟡', f"{sl} {worst['feat_ko']} {worst['range']} 구간만 손실", f"{worst['n']}건 평균 {worst['avg']:+.2f}% · 다른 구간은 모두 플러스",
                        '이 구간을 거르는 필터 후보 (구간 수가 많으면 우연일 수 있음 → 백테스트 두 기간으로 꼭 확인)')
    if len(res['compare']):
        for r in res['compare']:
            if r['real_n'] >= 20 and r['paper_avg'] is not None and r['real_avg'] is not None and r['real_avg'] < r['paper_avg'] - 0.5:
                add('🟠', f"{r['sleeve']} 실전이 모의보다 거래당 {r['paper_avg'] - r['real_avg']:.2f}%p 낮음", f"모의 {r['paper_avg']:+.2f}% · 실전 {r['real_avg']:+.2f}%",
                    '모의 체결이 실제보다 좋게 나오는 것 — 백테스트 비용 가정을 실전 값으로')
    n_tr = len(df) if df is not None else 0
    if n_tr < 30:
        add('ℹ️', f'청산 거래 {n_tr}건 — 아직 판단하기엔 적음', '거래 30건 이상 · 칸별 20건 이상부터 숫자가 의미 있음', '그대로 쌓기 (모든 주문 · 체결 · 판단이 자동 저장됨)')
    for o in out:
        o['check'] = CHECK
    return out


# ════════════════════════════════════════════
#  보고서 · 분석 패키지
# ════════════════════════════════════════════
def report_md(res):
    f = lambda v, d=2: '-' if v is None or (isinstance(v, float) and v != v) else (f'{v:+.{d}f}' if isinstance(v, (int, float)) else str(v))
    L = [f"# TK자동매매 거래 분석 ({res['made']})", '', f"대상: {' · '.join(MODE_KO[m] for m in res['modes'])} · 기간 {res['frm'] or '처음'} ~ {res['to'] or '지금'}", '']
    L += ['## 계좌', '', '| 모드 | 기록일 | 시작 | 지금 | 수익 | 최대낙폭 | 샤프 |', '|---|---|---|---|---|---|---|']
    for m, e in res['equity'].items():
        L.append(f"| {MODE_KO[m]} | {e['days']}일 ({e['first']}~{e['last']}) | {e['start']:,.0f} | {e['end']:,.0f} | {f(e['ret'])}% | {f(e['mdd'])}% | {f(e['sharpe'])} |")
    L += ['', '## 성적 (청산 거래)', '', '| 모드 | 건수 | 승률 | 평균 | t | 손익 | PF | 수수료 | 세금 | 평균 보유 |', '|---|---|---|---|---|---|---|---|---|---|']
    for s in res['summary']:
        L.append(f"| {s['mode']} | {s['n']} | {s['win']}% | {f(s['avg'])}% | {f(s.get('t'))} | {s['pnl']:,} | {f(s.get('pf'))} | {s['fee']:,} | {s['tax']:,} | {s['days']}일 |")
    L += ['', '## 칸별', '', '| 모드 | 칸 | 건수 | 승률 | 평균 | 비용 전 | t | 손익 | PF |', '|---|---|---|---|---|---|---|---|---|']
    for s in res['by_sleeve']:
        L.append(f"| {s['mode']} | {s['sleeve']} {s['name']} | {s['n']} | {s['win']}% | {f(s['avg'])}% | {f(s['gross'])}% | {f(s.get('t'))} | {s['pnl']:,} | {f(s.get('pf'))} |")
    L += ['', '## 청산 이유별', '', '| 칸 | 청산 | 건수 | 승률 | 평균 |', '|---|---|---|---|---|']
    L += [f"| {s['sleeve']} | {s['exit']} | {s['n']} | {s['win']}% | {f(s['avg'])}% |" for s in res['by_exit']]
    L += ['', '## 체결 품질 (%, 사후 계산된 거래)', '', '| 모드 | 칸 | 건수 | 매수-시가 | 매도-시가 | 신호가→시가 갭 | 모델 | 실제(비용 전) | 차이 | 비용 |', '|---|---|---|---|---|---|---|---|---|---|']
    L += [f"| {r['mode']} | {r['sleeve']} | {r['n']} | {f(r['slip_in'])} | {f(r['slip_out'])} | {f(r['gap_in'])} | {f(r['model'])} | {f(r['gross'])} | {f(r['drag'])} | {f(r['cost'])} |"
          for r in res['execution']]
    L += ['', '## 주문', '', '| 모드 | 주문 | 체결 | 거절 | 불분명 | 만료 | 접수 시간(초) | 체결률 | 거절 이유 |', '|---|---|---|---|---|---|---|---|---|']
    L += [f"| {r['mode']} | {r['n']} | {r['done']} | {r['rejected']} | {r['ambiguous']} | {r['expired']} | {f(r['ack_sec'])} | {f(r['fill_rate'], 1)}% | "
          + ' · '.join(f'{m} ({n})' for m, n in r['reject_top'][:3]) + ' |' for r in res['orders']]
    L += ['', '## 최대 역행 · 순행 (MAE / MFE)', '', '| 모드 | 칸 | 건수 | 평균 역행 | 평균 순행 | 손실 중 한때 +5% | 이익 중 한때 −5% |', '|---|---|---|---|---|---|---|']
    L += [f"| {r['mode']} | {r['sleeve']} | {r['n']} | {f(r['mae'])}% | {f(r['mfe'])}% | {r['loser_mfe5']}% | {r['winner_mae5']}% |" for r in res['mae_mfe']]
    if res['buckets']:
        L += ['', '## 진입 근거 구간별 수익', '', '| 칸 | 지표 | 구간 | 건수 | 승률 | 평균 |', '|---|---|---|---|---|---|']
        L += [f"| {b['sleeve']} | {b['feat_ko']} | {b['range']} | {b['n']} | {b['win']}% | {f(b['avg'])}% |" for b in res['buckets']]
    c = res.get('cands')
    if c:
        L += ['', f"## 신호 후보 순위별 사후 수익 ({c['days']}일 · 출처 {c['src']})", '', '| 칸 | 기간 | 순위 | 건수 | 평균 | 승률 | 전체 대비 | 최근 60일 |', '|---|---|---|---|---|---|---|---|']
        L += [f"| {b['sleeve']} | {b['h']}일 | {b['bucket']} | {b['n']} | {f(b['avg'])}% | {b['win']}% | {f(b['vs_all'])}%p | {f(b['recent_avg'])}% |" for b in c['buckets']]
        L += ['', '| 칸 | 순위 상관(IC) 전체 | 최근 60일 | t | 날 수 |', '|---|---|---|---|---|']
        L += [f"| {r['sleeve']} | {f(r['ic_all'], 3)} | {f(r['ic_60'], 3)} | {f(r['ic_t'])} | {r['days']} |" for r in c['recent']]
    if res['missed']:
        L += ['', '## 산 것 vs 못 산 것 (판단일 시가 → 보유 기간 뒤 시가)', '', '| 칸 | 판단 | 건수 | 사후 평균 |', '|---|---|---|---|']
        L += [f"| {r['sleeve']} | {r['why']} | {r['n']} | {f(r['avg'])}% |" for r in res['missed']]
    if res['compare']:
        L += ['', '## 모의 ↔ 실전', '', '| 칸 | 모의 건수 | 실전 건수 | 모의 평균 | 실전 평균 | 모의 체결차 | 실전 체결차 |', '|---|---|---|---|---|---|---|']
        L += [f"| {r['sleeve']} | {r['paper_n']} | {r['real_n']} | {f(r['paper_avg'])}% | {f(r['real_avg'])}% | {f(r['paper_slip'])} | {f(r['real_slip'])} |" for r in res['compare']]
    if res['api']:
        L += ['', '## KIS 호출', '', '| 모드 | TR | 호출 | 오류 | 오류율 | 평균 ms |', '|---|---|---|---|---|---|']
        L += [f"| {r['mode']} | {r['tr']} | {r['n']:,} | {r['err']} | {r['err_pct']}% | {r['ms']} |" for r in res['api'][:20]]
    L += ['', '## 고도화 후보 (자동으로 바꾸지 않음)', '']
    L += [f"- {i['level']} **{i['title']}** — {i['why']} → {i['what']}" for i in res['ideas']] or ['- 없음']
    L += ['', f'> {CHECK}']
    return '\n'.join(L)


PACKAGE_TABLES = ('lots', 'orders', 'order_events', 'fills', 'ws_execs', 'decisions', 'positions_daily', 'account_daily', 'broker_pnl', 'equity',
                  'sleeve_daily', 'signals', 'api_daily', 'days')


def package(res=None, extra=None):
    """분석 패키지 zip — 모의 · 실전 기록 전체(CSV) + 분석 보고서 + 신호 후보 + 백테스트 결과 · 비밀 값 없음 (계좌번호 · 키는 기록에 없음)"""
    res = res or analyze()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z:
        for m in MODES:
            x = db.conn(m)
            for t in PACKAGE_TABLES:
                cur = x.execute(f'SELECT * FROM {t}')
                rows = cur.fetchall()
                if not rows:
                    continue
                s = io.StringIO()
                w = csv.writer(s)
                w.writerow([d[0] for d in cur.description])
                w.writerows(rows)
                z.writestr(f'{m}/{t}.csv', '﻿' + s.getvalue())
        cur = db.mconn().execute('SELECT * FROM cands ORDER BY date, sleeve, rank')
        s = io.StringIO()
        w = csv.writer(s)
        w.writerow([d[0] for d in cur.description])
        w.writerows(cur.fetchall())
        z.writestr('cands.csv', '﻿' + s.getvalue())
        z.writestr('analysis.md', report_md(res))
        z.writestr('analysis.json', json.dumps(res, ensure_ascii=False, default=str, indent=1))
        for f_ in ('backtest_result.md', 'backtest_result.json'):
            p = os.path.join(db.DATA_DIR, f_)
            if os.path.exists(p):
                z.write(p, f_)
        z.writestr('README.txt', 'TK자동매매 분석 패키지\n'
                   '- paper/ · real/ : 모의 · 실전 장부의 모든 표 (lots=거래 · orders=주문 · order_events=상태 이력 · fills=체결 조각 · decisions=판단 · '
                   'positions_daily=잔고 이력 · account_daily=매매일지 · broker_pnl=KIS 실제 손익 · api_daily=호출 통계)\n'
                   '- cands.csv : 날마다 신호 후보 상위 50 (거래 안 한 것 포함)\n- analysis.md : 분석 보고서 · 고도화 후보\n'
                   '이 파일을 Claude에게 보내면 기록을 바탕으로 전략 고도화를 점검합니다. 앱키 · 시크릿 · 계좌번호는 들어 있지 않습니다.\n')
        if extra:
            z.writestr('summary.json', json.dumps(extra, ensure_ascii=False, default=str, indent=1))
    return buf.getvalue()


# ════════════════════════════════════════════
#  과거 신호 후보 채우기 (분석을 처음부터 쓸 수 있게)
# ════════════════════════════════════════════
def backfill_cands(start='20231024', end='99999999', progress=print):
    """시장 자료로 과거 날마다 신호 후보 상위 50을 계산해 cands에 (src='backfill' · 실제 운용 기록 'live'는 덮지 않음)"""
    import tk_backtest as B
    import tk_signals as S
    import tk_journal as J
    D = B.load(start, end)
    F = D['F']
    C = F['close']
    days = [d for d in C.index if start <= d <= end]
    have = {r[0] for r in db.mconn().execute("SELECT DISTINCT date FROM cands WHERE src='live'")}
    months = sorted(D['mem'])
    dv_cache = {}
    n = 0
    for i, d in enumerate(days):
        if d in have:
            continue
        rows = []
        for s, fn in (('LVH', S.lvh_scores), ('REV', S.rev_scores)):
            sc = fn(F, d).dropna()
            for k, t in enumerate(S.top_n(sc, set(), 50), start=1):
                rows.append((s, t, k, float(sc[t]), float(C.at[d, t]), None))
        m = max([x for x in months if x <= d[:6]], default=None)
        if m:
            if m not in dv_cache:
                dv_cache[m] = S.dv_rank(D['mem'], D['mon'], m)
            for k, (t, nm, sec, sc_, dv, pb) in enumerate(dv_cache[m][:50], start=1):
                cl = C.at[d, t] if t in C.columns else None
                rows.append(('DV', t, k, sc_, float(cl) if cl == cl and cl is not None else None, {'div': dv, 'pbr': pb, 'sector': sec}))
        J.save_cands(d, rows, 'backfill')
        n += 1
        if i % 50 == 0:
            progress(f'과거 신호 후보 {d} ({i + 1}/{len(days)})')
    progress(f'과거 신호 후보 {n}일 채움')
    return n
