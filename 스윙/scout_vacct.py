"""
scout_vacct.py — 가상 계좌 (v6.0) : 실전 계좌처럼 예수금 · 보유 종목 · 평가손익 · 거래 내역 · 일별 추이

백테스트형(vtrades)과 실전형(rt_trades) 기록을 모델별 가상 계좌로 재생한다 (기록에서 매번 다시 계산 → 상태가 꼬이지 않음).
  시작 금액 · 종목당 비중 = 가상 계좌 설정(vt_cash · vt_slots) · 종목당 한도 = 모델별 1회 최대 주문금액
  정수 주식 · 매수 수수료 0.035% · 매도 수수료+세금 0.215% · 현금이 모자라거나 1주도 못 사면 '건너뜀'
  실전형은 분할 매도(비율)를 주식 수로 나눠 시각 순서대로 반영, 보유 종목 현재가는 장중 감시 가격
"""
import json
from datetime import datetime

import scout_db as db


def _px_table(tickers, start):
    if not tickers:
        return {}
    c = db.conn()
    out = {}
    tk = sorted(tickers)
    for i in range(0, len(tk), 400):
        part = tk[i:i + 400]
        for t, d, cl in c.execute(f"SELECT ticker,date,close FROM candles WHERE date>=? AND ticker IN ({','.join('?' * len(part))})",
                                  (start, *part)):
            out[(t, d)] = cl
    return out


def _events_bt(grp, since=''):
    c = db.conn()
    rows = [dict(r) for r in c.execute(
        "SELECT * FROM vtrades WHERE grp=? AND entry_date IS NOT NULL AND entry_price>0 AND signal_date>=? ORDER BY entry_date, rank",
        (grp, since or ''))]
    ev = []
    close_entry = grp in db.CLOSE_ENTRY
    for t in rows:
        ev.append({'d': t['entry_date'], 't': '15:30:00' if close_entry else '09:00:00', 'side': 'buy', 'key': t['id'],
                   'ticker': t['ticker'], 'name': t['name'], 'px': t['entry_price'], 'rank': t['rank'], 'why': f"신호 {t['signal_date']}"})
        if t['status'] == '청산' and t['exit_date'] and t['exit_price']:
            ev.append({'d': t['exit_date'], 't': '09:00:00', 'side': 'sell', 'key': t['id'], 'frac': 1.0, 'last': True,
                       'ticker': t['ticker'], 'name': t['name'], 'px': t['exit_price'], 'why': t['exit_reason'] or '청산'})
    return ev, {}


def _events_rt(grp, since=''):
    c = db.conn()
    rows = [dict(r) for r in c.execute(
        "SELECT * FROM rt_trades WHERE grp=? AND entry_date IS NOT NULL AND entry_price>0 AND signal_date>=? ORDER BY entry_date, rank",
        (grp, since or ''))]
    ev, live = [], {}
    close_entry = grp in db.CLOSE_ENTRY
    for t in rows:
        ev.append({'d': t['entry_date'], 't': '15:30:00' if close_entry else '09:00:00', 'side': 'buy', 'key': t['id'],
                   'ticker': t['ticker'], 'name': t['name'], 'px': t['entry_price'], 'rank': t['rank'], 'why': f"신호 {t['signal_date']}"})
        fills = json.loads(t['fills'] or '[]')
        for i, f in enumerate(fills):
            ev.append({'d': f['d'], 't': str(f['t']) if ':' in str(f['t']) else '12:00:00', 'side': 'sell', 'key': t['id'],
                       'frac': f['frac'], 'last': t['status'] == '청산' and i == len(fills) - 1,
                       'ticker': t['ticker'], 'name': t['name'], 'px': f['px'], 'why': f['why']})
        if t['status'] == '보유' and t['last_price'] and t['last_ts']:
            live[t['ticker']] = (t['last_price'], t['last_ts'])
    return ev, live


def account(kind, grp, since=''):
    """kind 'bt'(백테스트형) · 'rt'(실전형) → 요약 · 보유 · 거래 내역 · 일별. since: 이 신호일부터의 거래만 (비교 기간 맞추기)"""
    if kind == 'rt':
        import scout_rt as rt
        rt.init()
        ev, live = _events_rt(grp, since)
    else:
        db._vt_init()
        ev, live = _events_bt(grp, since)
    A = db.ACCT
    start_cash = float(A['cash'])
    empty = {'kind': kind, 'grp': grp, 'summary': {'start_cash': start_cash, 'equity': start_cash, 'cash': start_cash, 'stock': 0,
                                                  'unreal': 0, 'realized': 0, 'return': 0, 'mdd': 0, 'positions': 0, 'buys': 0,
                                                  'sells': 0, 'skipped': 0, 'win': None, 'start': None, 'closed': 0,
                                                  'slots': A['slots'], 'cap': db.order_cap(grp)},
             'holdings': [], 'ledger': [], 'daily': []}
    if not ev:
        return empty
    start = min(e['d'] for e in ev)
    c = db.conn()
    dates = [r[0] for r in c.execute("SELECT DISTINCT date FROM candles WHERE date>=? ORDER BY date", (start,))]
    today = datetime.now().strftime('%Y%m%d')
    if kind == 'rt' and today > (dates[-1] if dates else '') and any(ts[:8] == today for _, ts in live.values()):
        dates.append(today)                                   # 장중: 오늘 줄을 감시 가격으로
    px = _px_table({e['ticker'] for e in ev}, start)
    cap = db.order_cap(grp)
    by_day = {}
    for e in ev:
        by_day.setdefault(e['d'], []).append(e)
    cash, pos, last = start_cash, {}, {}
    ledger, daily, closed_pl = [], [], []
    eq_prev, peak, mdd, skipped, realized = start_cash, start_cash, 0.0, 0, 0.0
    for d in dates:
        size = eq_prev / A['slots']
        if cap:
            size = min(size, cap)
        day_real = 0.0
        for e in sorted(by_day.get(d, []), key=lambda x: (x['t'], 0 if x['side'] == 'sell' else 1, x.get('rank') or 0)):
            if e['side'] == 'buy':
                amt = min(size, cash)
                sh = int(amt // (e['px'] * (1 + A['buy_cost'])))
                if sh <= 0 or amt < A['min_order']:
                    skipped += 1
                    ledger.append({'d': d, 't': e['t'], 'side': '건너뜀', 'ticker': e['ticker'], 'name': e['name'], 'qty': 0,
                                   'px': e['px'], 'amt': 0, 'pl': None, 'ret': None,
                                   'why': '현금 부족' if cash < A['min_order'] else '1주도 못 삼 (주가 > 종목당 금액)'})
                    continue
                cost = sh * e['px'] * (1 + A['buy_cost'])
                cash -= cost
                pos[e['key']] = {'ticker': e['ticker'], 'name': e['name'], 'sh': sh, 'sh0': sh, 'cost': cost, 'd': d,
                                 'px0': e['px']}
                ledger.append({'d': d, 't': e['t'], 'side': '매수', 'ticker': e['ticker'], 'name': e['name'], 'qty': sh,
                               'px': e['px'], 'amt': round(cost), 'pl': None, 'ret': None, 'why': e['why']})
            else:
                p = pos.get(e['key'])
                if not p:
                    continue                                   # 건너뛴 매수의 매도
                sh = p['sh'] if e.get('last') else min(p['sh'], max(1, round(p['sh0'] * e.get('frac', 1.0))))
                if sh <= 0:
                    continue
                cost_part = p['cost'] * sh / p['sh']
                proceeds = sh * e['px'] * (1 - A['sell_cost'])
                cash += proceeds
                pl = proceeds - cost_part
                realized += pl
                day_real += pl
                p['sh'] -= sh
                p['cost'] -= cost_part
                ledger.append({'d': d, 't': e['t'], 'side': '매도', 'ticker': e['ticker'], 'name': e['name'], 'qty': sh,
                               'px': round(e['px'], 2), 'amt': round(proceeds), 'pl': round(pl), 'ret': round(pl / cost_part * 100, 2),
                               'why': e['why']})
                p.setdefault('pl', 0.0)
                p['pl'] += pl
                p.setdefault('cost_sold', 0.0)
                p['cost_sold'] += cost_part
                if p['sh'] <= 0:
                    closed_pl.append(p['pl'])
                    pos.pop(e['key'])
        stock = 0.0
        for p in pos.values():
            q = px.get((p['ticker'], d))
            if kind == 'rt' and p['ticker'] in live and live[p['ticker']][1][:8] == d:
                q = live[p['ticker']][0]
            q = q or last.get(p['ticker']) or p['px0']
            last[p['ticker']] = q
            p['cur'] = q
            stock += p['sh'] * q
        eq = cash + stock * (1 - A['sell_cost'])
        peak = max(peak, eq)
        mdd = min(mdd, eq / peak - 1)
        daily.append({'d': d, 'equity': round(eq), 'cash': round(cash), 'stock': round(stock), 'positions': len(pos),
                      'day_pl': round(eq - eq_prev), 'day_ret': round((eq / eq_prev - 1) * 100, 2) if eq_prev else 0,
                      'realized': round(day_real)})
        eq_prev = eq
    hold = []
    for p in pos.values():
        cur = p.get('cur') or p['px0']
        avg = p['cost'] / p['sh'] if p['sh'] else 0
        val = p['sh'] * cur
        pl = val * (1 - A['sell_cost']) - p['cost']
        held = sum(1 for d in dates if d >= p['d'])
        hold.append({'ticker': p['ticker'], 'name': p['name'], 'qty': p['sh'], 'avg': round(avg), 'cur': cur, 'value': round(val),
                     'pl': round(pl), 'ret': round(pl / p['cost'] * 100, 2) if p['cost'] else 0, 'entry': p['d'], 'held': held,
                     'partial': p['sh'] < p['sh0']})
    hold.sort(key=lambda h: -h['value'])
    eq_end = daily[-1]['equity'] if daily else start_cash
    stock_end = daily[-1]['stock'] if daily else 0
    unreal = sum(h['pl'] for h in hold)
    return {'kind': kind, 'grp': grp, 'asof': dates[-1] if dates else None,
            'summary': {'start_cash': start_cash, 'equity': eq_end, 'cash': round(cash), 'stock': stock_end, 'unreal': round(unreal),
                        'realized': round(realized), 'return': round((eq_end / start_cash - 1) * 100, 2), 'mdd': round(mdd * 100, 2),
                        'positions': len(hold), 'buys': sum(1 for x in ledger if x['side'] == '매수'),
                        'sells': sum(1 for x in ledger if x['side'] == '매도'), 'skipped': skipped,
                        'closed': len(closed_pl), 'win': round(sum(1 for x in closed_pl if x > 0) / len(closed_pl) * 100, 1) if closed_pl else None,
                        'start': start, 'slots': A['slots'], 'cap': cap},
            'holdings': hold, 'ledger': ledger[::-1], 'daily': daily[::-1]}
