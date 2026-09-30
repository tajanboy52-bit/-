"""
가짜 KRX(pykrx.stock) · 가짜 KIS — 시험용. 실제 응답 모양(한글 열 이름 · 원주가 + 기준가 대비 등락률)을 흉내 냄
· 종목 400개 · 2022-06-01 ~ 2026-09-30 평일 · 일부 늦게 상장 · 일부 상장폐지 · 종목 SPLIT_TK는 SPLIT_DAY에 1:5 액면분할
· 229200 ETF는 밤사이 +0.1% 를 심어 둠
"""
import numpy as np
import pandas as pd

SPLIT_TK, SPLIT_DAY = '000100', '20250602'


class FakeKRX:
    def __init__(self, n=400, seed=7, start='2022-06-01', end='2026-09-30'):
        rng = np.random.default_rng(seed)
        self.days = list(pd.bdate_range(start, end).strftime('%Y%m%d'))
        D = len(self.days)
        self.tks = [f'{i:06d}' for i in range(100, 100 + n * 10, 10)][:n]
        self.tks[0] = SPLIT_TK
        mkt = rng.normal(0.0003, 0.011, D)
        self.px = {}
        for k, t in enumerate(self.tks):
            vol = rng.uniform(.01, .03)
            r = mkt * rng.uniform(.5, 1.5) + rng.normal(0, vol, D)
            cl = rng.uniform(8000, 120000) * np.exp(np.cumsum(r))
            op = cl / np.exp(r) * np.exp(rng.normal(0, .003, D))
            hi = np.maximum(op, cl) * (1 + abs(rng.normal(0, vol / 2, D)))
            lo = np.minimum(op, cl) * (1 - abs(rng.normal(0, vol / 2, D)))
            v = rng.uniform(5e9, 4e10) / cl[0] * rng.uniform(.5, 1.5, D)
            s = 0 if k % 9 else int(rng.integers(50, 300))
            e = D if k % 23 else int(rng.integers(500, D - 50))
            self.px[t] = {'o': op, 'h': hi, 'l': lo, 'c': cl, 'v': v, 's': s, 'e': e}
        self.idx = {d: i for i, d in enumerate(self.days)}
        self.fl = {inv: rng.normal(0, 1, (D, n)) for inv in ('외국인', '기관합계', '연기금')}
        self.sector = {t: ['전기·전자', '금융', '화학', '운송장비·부품', '유통', '음식료·담배', '건설', '의약품'][k % 8] for k, t in enumerate(self.tks)}
        self.fund = {t: (rng.uniform(-500, 5000), rng.uniform(0, 7), rng.uniform(.2, 3)) for t in self.tks}
        on = rng.normal(0.001, .006, D)
        day = rng.normal(-0.0007, .01, D)
        c = 10000 * np.exp(np.cumsum(on + day))
        self.etf = {'229200': (c / np.exp(day), c), '069500': (c * 3 / np.exp(day * 0.9), c * 3)}
        self.calls = 0

    def _raw(self, t, i):
        """원주가 — 분할일부터 1/5"""
        f = 0.2 if (t == SPLIT_TK and self.days[i] >= SPLIT_DAY) else 1.0
        p = self.px[t]
        return p['o'][i] * f, p['h'][i] * f, p['l'][i] * f, p['c'][i] * f, p['v'][i] / f

    # ── pykrx.stock 흉내 ──
    def get_previous_business_days(self, fromdate, todate):
        return [pd.Timestamp(d) for d in self.days if fromdate <= d <= todate]

    def get_market_ticker_list(self, market='KOSPI'):
        return [t for k, t in enumerate(self.tks) if (k % 2 == 0) == (market == 'KOSPI')] if market != 'KONEX' else []

    def get_market_ticker_name(self, t):
        return f'가짜{t}'

    def get_market_ohlcv_by_ticker(self, day, market='ALL'):
        self.calls += 1
        i = self.idx.get(day)
        if i is None:
            return pd.DataFrame()
        rows = {}
        for t in self.tks:
            p = self.px[t]
            if not (p['s'] <= i < p['e']):
                continue
            o, h, l_, c, v = self._raw(t, i)
            if i > p['s']:
                pc = p['c'][i - 1]                                           # 조정된 전날 (등락률 기준가)
                chg = (p['c'][i] / pc - 1) * 100
            else:
                chg = 0.0
            rows[t] = {'시가': round(o), '고가': round(h), '저가': round(l_), '종가': round(c), '거래량': int(v), '거래대금': float(round(c) * int(v)), '등락률': round(chg, 2)}
        return pd.DataFrame.from_dict(rows, orient='index')

    def get_market_net_purchases_of_equities_by_ticker(self, frm, to, market, inv):
        self.calls += 1
        i = self.idx.get(frm)
        if i is None:
            return pd.DataFrame()
        rows = {t: {'순매수거래대금': float(self.fl[inv][i, k] * 3e8)} for k, t in enumerate(self.tks) if self.px[t]['s'] <= i < self.px[t]['e']}
        return pd.DataFrame.from_dict(rows, orient='index')

    def get_etf_ohlcv_by_date(self, frm, to, tk):
        o, c = self.etf[tk]
        ix = [i for i, d in enumerate(self.days) if frm <= d <= to]
        return pd.DataFrame({'시가': o[ix], '고가': np.maximum(o[ix], c[ix]), '저가': np.minimum(o[ix], c[ix]), '종가': c[ix]},
                            index=[pd.Timestamp(self.days[i]) for i in ix])

    def get_index_portfolio_deposit_file(self, code, day):
        i = self.idx.get(day, 0)
        live = [t for t in self.tks if self.px[t]['s'] <= i < self.px[t]['e']]
        return live[:200] if code == '1028' else live[200:350]

    def get_market_sector_classifications(self, day, mk):
        i = self.idx.get(day, 0)
        tks = [t for k, t in enumerate(self.tks) if (k % 2 == 0) == (mk == 'KOSPI') and self.px[t]['s'] <= i < self.px[t]['e']]
        return pd.DataFrame({'종목명': [f'가짜{t}' for t in tks], '업종명': [self.sector[t] for t in tks],
                             '시가총액': [self.px[t]['c'][i] * 1e7 for t in tks]}, index=tks)

    def get_market_fundamental_by_ticker(self, day, mk):
        tks = [t for k, t in enumerate(self.tks) if (k % 2 == 0) == (mk == 'KOSPI')]
        return pd.DataFrame({'EPS': [self.fund[t][0] for t in tks], 'DIV': [self.fund[t][1] for t in tks], 'PBR': [self.fund[t][2] for t in tks]}, index=tks)


class FakeKIS:
    """tk_kis.KIS와 같은 메서드 — 장전 시장가 = 그날 시가 · 15:20 = 그날 종가 · 잔고 평가 = 그날 종가"""
    def __init__(self, P, onb, cash=10_000_000, env='paper'):
        self.P, self.onb, self.cash, self.pos, self.orders, self.d = P, onb, float(cash), {}, [], None
        self.env, self.masked_account, self.hts_id, self.notice_tr = env, 'FAKE', '', 'H0STCNI9'
        self.now = lambda: '08:35'

    def _px(self, t, k):
        if t == '229200':
            return float(self.onb.at[self.d, k]) if self.d in self.onb.index else 0
        v = self.P[k].at[self.d, t] if t in self.P[k].columns else float('nan')
        return float(v) if v == v else 0

    def order(self, side, t, q, dv='01', p=0):
        no = str(len(self.orders) + 1)
        self.orders.append({'order_no': no, 'd': self.d, 't': t, 'side': side, 'qty': int(q), 'filled': 0, 'avg': 0.0,
                            'when': 'close' if self.now() >= '15:00' else 'open'})
        return {'order_no': no, 'org_no': '1', 'msg': 'ok'}

    def cancel(self, *a):
        return {}

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
        ps = [{'ticker': t, 'name': t, 'qty': q, 'price': self._px(t, 'close'), 'value': q * self._px(t, 'close'), 'pnl': 0} for t, q in self.pos.items() if q > 0]
        return {'positions': ps, 'cash': self.cash, 'cash_d2': self.cash, 'equity': self.cash + sum(p['value'] for p in ps)}

    def buyable(self, ticker='005930', price=0):
        return {'cash': self.cash, 'nrcvb': self.cash, 'qty': 0}

    def price(self, t):
        px = self._px(t, 'close')
        return {'price': px, 'open': px, 'high': px, 'low': px, 'chg': 0, 'vol': 0, 'value': 0, 'halt': False, 'vi': False, 'raw': {'hts_kor_isnm': t}}
