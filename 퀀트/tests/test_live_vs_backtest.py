"""
실전 코드(q_trader) ↔ 백테스트(q_backtest) 대조 시험 — 가짜 KIS 계좌로 하루씩 돌려 같은 종목을 같은 날 사고파는지 확인

사용: SCOUT_DATA=<scout.db 폴더> TKQUANT_DATA=<빈 폴더> python tests/test_live_vs_backtest.py [일수=40]
가짜 KIS: 장전 시장가 = 그날 시가 체결 · 15:20 ON = 그날 ETF 종가 체결 · 잔고 평가 = 그날 종가
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
import q_backtest as B
import q_db as db
import q_signals as S
import q_trader as tr


class FakeKIS:
    def __init__(self, P, onb, cash=10_000_000):
        self.P, self.onb, self.cash, self.pos, self.orders, self.d = P, onb, float(cash), {}, [], None
        self.masked_account = 'FAKE'

    def _px(self, t, k):
        if t == S.ON_TICKER:
            return float(self.onb.at[self.d, k]) if self.d in self.onb.index else 0
        v = self.P[k].at[self.d, t] if t in self.P[k].columns else float('nan')
        return float(v) if v == v else 0

    def order(self, side, t, q, dv='01', p=0):
        no = str(len(self.orders) + 1)
        self.orders.append({'order_no': no, 'd': self.d, 't': t, 'side': side, 'qty': int(q), 'filled': 0, 'avg': 0.0, 'when': 'close' if tr.now().strftime('%H:%M') >= '15:00' else 'open'})
        return {'order_no': no, 'org_no': '1', 'msg': 'ok'}

    def settle(self, when):
        for o in self.orders:
            if o['d'] != self.d or o['filled'] or o['when'] != when:
                continue
            px = self._px(o['t'], 'open' if when == 'open' else 'close')
            if px <= 0:
                continue
            if o['side'] == 'buy':
                if px * o['qty'] > self.cash + 1e-6:
                    continue
                self.cash -= px * o['qty']
                self.pos[o['t']] = self.pos.get(o['t'], 0) + o['qty']
            else:
                q = min(o['qty'], self.pos.get(o['t'], 0))
                if q <= 0:
                    continue
                self.cash += px * q
                self.pos[o['t']] -= q
                o['qty'] = q
            o.update(filled=o['qty'], avg=px)

    def fills(self, d):
        return [{'order_no': o['order_no'], 'filled': o['filled'], 'avg': o['avg'], 'remain': o['qty'] - o['filled'], 'cancelled': False}
                for o in self.orders if o['d'] == d]

    def balance(self):
        ps = [{'ticker': t, 'name': t, 'qty': q, 'price': self._px(t, 'close'), 'value': q * self._px(t, 'close')} for t, q in self.pos.items() if q > 0]
        eq = self.cash + sum(p['value'] for p in ps)
        return {'positions': ps, 'cash': self.cash, 'cash_d2': self.cash, 'equity': eq}

    def price(self, t):
        return self._px(t, 'close'), {}


def main(n=40):
    D = B.load('20180101', '99999999')
    C = D['P']['close']
    days = list(C.index)
    run_days = days[-n:]
    start = run_days[0]
    kc = FakeKIS(D['P'], D['on'])
    cfg = {'kis_on': True, 'kis_app_key': 'x', 'kis_account': '12345678-01', 'cap': 10_000_000}
    clock = {'d': days[-n - 1], 'hm': '19:00'}
    tr.today = lambda: clock['d']
    tr.now = lambda: __import__('datetime').datetime.strptime(clock['d'] + clock['hm'], '%Y%m%d%H:%M')
    tr.prev_trading_day = lambda d: days[days.index(d) - 1]
    tr.is_trading_day = lambda d=None: True
    tr.signal_job(cfg, clock['d'])                                          # 첫날 전날 신호
    for d in run_days:
        clock['d'] = d
        kc.d = d
        clock['hm'] = '08:35'
        tr.preopen(cfg, kc, d)
        kc.settle('open')
        clock['hm'] = '09:02'
        tr.deferred(cfg, kc, d)
        kc.settle('open')                                                    # 09:02 매수도 시가 근처로 (시험 단순화)
        tr.sync(kc, d)
        clock['hm'] = '15:20'
        tr.on_buy(cfg, kc, d)
        kc.settle('close')
        clock['hm'] = '15:45'
        tr.eod(cfg, kc, d)
        clock['hm'] = '19:00'
        tr.signal_job(cfg, d)
    x = db.conn()
    live = {(r['sleeve'], r['ticker'], r['entry_date']) for r in x.execute("SELECT * FROM lots WHERE sleeve IN ('LVH','REV') AND entry_date IS NOT NULL")}
    live_exit = {(r['sleeve'], r['ticker'], r['entry_date'], r['exit_date']) for r in x.execute("SELECT * FROM lots WHERE sleeve IN ('LVH','REV') AND status='청산'")}
    res = B.simulate(D, days[-n - 1], run_days[-1], {'LVH': .40, 'REV': .25, 'DV': .20, 'ON': .15})     # 백테스트도 전날 종가 신호부터
    bt = {(t[0], t[1], t[2]) for t in res['trades'] if t[0] in ('LVH', 'REV')} | {(l['s'], l['t'], l['d']) for l in res['open_lots'] if l['s'] in ('LVH', 'REV')}
    bt_exit = {(t[0], t[1], t[2], t[3]) for t in res['trades'] if t[0] in ('LVH', 'REV')}
    same = live & bt
    print(f'LVH·REV 매수: 실전 {len(live)} · 백테스트 {len(bt)} · 같은 매수 {len(same)} ({len(same) / max(1, len(live | bt)) * 100:.0f}%)')
    print(f'청산(매수일 · 매도일까지 같음): 실전 {len(live_exit)} · 백테스트 {len(bt_exit)} · 같음 {len(live_exit & bt_exit)}')
    only_l, only_b = sorted(live - bt)[:5], sorted(bt - live)[:5]
    if only_l or only_b:
        print('실전만:', only_l, '\n백테스트만:', only_b)
    eq = [r['value'] for r in x.execute('SELECT value FROM equity ORDER BY date')]
    print(f"실전 계좌 {eq[0]:,.0f} → {eq[-1]:,.0f} ({(eq[-1] / 1e7 - 1) * 100:+.2f}%) · 백테스트 {res['curve'].iloc[-1]:,.0f} ({(res['curve'].iloc[-1] / 1e7 - 1) * 100:+.2f}%)")
    dv_l = x.execute("SELECT COUNT(*) FROM lots WHERE sleeve='DV' AND status='보유'").fetchone()[0]
    on_l = x.execute("SELECT COUNT(*), AVG(ret) FROM lots WHERE sleeve='ON' AND status='청산'").fetchone()
    print(f'DV 보유 {dv_l} · ON 청산 {on_l[0]}건 평균 {on_l[1] or 0:+.3f}%')
    bad = x.execute("SELECT COUNT(*) FROM log WHERE level IN ('warn','error') AND msg LIKE '%불일치%'").fetchone()[0]
    print('잔고 불일치 경고:', bad)
    return len(same) / max(1, len(live | bt))


if __name__ == '__main__':
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 40)
