"""
TK자동매매 종합 시험 — 인터넷 · 실제 계좌 없이 가짜 KRX · 가짜 KIS · 가짜 KIS 서버로
사용: TKAUTO_DATA=<빈 폴더> python tests/test_tk.py
 1. 자료 수집(가짜 KRX) · 상장폐지 · 1:5 액면분할 → 수정주가 자동 보정
 2. 실전 코드(tk_trader) ↔ 백테스트(tk_backtest) 40거래일 매수 · 청산 일치
 3. 서버 보안: 토큰 없음 401 · 다른 Host 403 · JSON 아닌 POST 415 · 토큰 있으면 200
 4. KIS 클라이언트(가짜 KIS 서버): 모의 TR V 변환 · 연속조회 · EGW00201 한도 초과 재시도 · 주문 결과 불분명 → AMBIGUOUS · 토큰 파일 재사용
 5. 체결 통보 AES 해독 · 종목 마스터 고정폭 해석 · 모드 전환 판정
"""
import base64
import io
import json
import os
import sys
import threading
import time
import zipfile
from http.server import BaseHTTPRequestHandler, HTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
import tempfile
os.environ['TKAUTO_DATA'] = tempfile.mkdtemp(prefix='tk_test_')          # 매번 빈 데이터 폴더에서
sys.path.insert(0, os.path.join(HERE, '..'))
sys.path.insert(0, HERE)
import numpy as np

import fake_market as FM
import tk_collect as col
import tk_config as CF
import tk_db as db
import tk_signals as S
import tk_trader as tr
import tk_backtest as B

OK = []


def check(name, cond, detail=''):
    OK.append(bool(cond))
    print(f"{'✅' if cond else '❌'} {name} {detail}")


# ── 1. 수집 ──
fk = FM.FakeKRX()
col.STOCK[0] = fk
col.SLEEP = 0
cfg = CF.load()
t0 = time.time()
r = col.run(cfg, start='20220601', flow_start='20230601', month_start='202206', full=True)
m = db.mconn()
check('수집: 일봉 거래일', r['bars_days'] > 1000, f"{r['bars_days']}일 · {time.time() - t0:.0f}초 · KRX 호출 {fk.calls}")
check('수집: 품질 OK', r['quality'])
nd = m.execute("SELECT COUNT(DISTINCT ticker) FROM bars").fetchone()[0]
dead = sum(1 for t in fk.tks if fk.px[t]['e'] < len(fk.days))
check('수집: 상장폐지 종목 포함', nd == len(fk.tks), f'{nd}종목 (중간 폐지 {dead})')
check('수집: 수급 3종 · ETF · 월 자료', all(db.last_flow_day() for _ in [0]) and m.execute('SELECT COUNT(*) FROM etf').fetchone()[0] > 1000
      and m.execute("SELECT COUNT(*) FROM done WHERE kind='month'").fetchone()[0] >= 50)
P = db.panel('20250501', '20250701', [FM.SPLIT_TK])
raw = db.panel('20250501', '20250701', [FM.SPLIT_TK], adjusted=False)
i = list(P['close'].index).index(FM.SPLIT_DAY)
true = fk.px[FM.SPLIT_TK]['c'][fk.idx[FM.SPLIT_DAY]] / fk.px[FM.SPLIT_TK]['c'][fk.idx[FM.SPLIT_DAY] - 1] - 1
adj_r = P['close'][FM.SPLIT_TK].iloc[i] / P['close'][FM.SPLIT_TK].iloc[i - 1] - 1
raw_r = raw['close'][FM.SPLIT_TK].iloc[i] / raw['close'][FM.SPLIT_TK].iloc[i - 1] - 1
check('수정주가: 1:5 분할일 수익률', abs(adj_r - true) < 0.002, f'원주가 {raw_r * 100:+.1f}% → 수정 {adj_r * 100:+.2f}% (실제 {true * 100:+.2f}%)')
before = P['close'][FM.SPLIT_TK].iloc[:i].values / raw['close'][FM.SPLIT_TK].iloc[:i].values
check('수정주가: 분할 전 가격이 1/5로', np.allclose(before, 0.2, atol=0.002), f'비율 {before.mean():.4f}')
# 20일 30억 이상 ETF/분할 종목 외 이상 없음 — 다른 종목은 조정 없음
Pa = db.panel('20240101', '20260930')
Pr = db.panel('20240101', '20260930', adjusted=False)
diff = (Pa['close'] / Pr['close']).drop(columns=[FM.SPLIT_TK]).stack().dropna()
check('수정주가: 다른 종목은 그대로', (abs(diff - 1) < 1e-9).all(), f'최대 차이 {abs(diff - 1).max():.2e}')

# ── 2. 실전 ↔ 백테스트 ──
n = 40
D = B.load('20180101', '99999999')
days = list(D['P']['close'].index)
run_days = days[-n:]
kc = FM.FakeKIS(D['P'], D['on'], swb=db.etf_bars('069500'))
kc.resv_ok = True                                    # 예약주문 경로까지 (실전 계좌처럼)
clock = {'d': days[-n - 1], 'hm': '19:00'}
tr.today = lambda: clock['d']
import datetime as _dt
tr.now = lambda: _dt.datetime.strptime(clock['d'] + clock['hm'], '%Y%m%d%H:%M')
kc.now = lambda: clock['hm']
tr.prev_trading_day = lambda d: days[days.index(d) - 1]
tr.is_trading_day = lambda d=None: True
cfg.update(kis_on=True, cap=10_000_000, guard_per_min=10 ** 9, guard_per_day=10 ** 9)   # 40일을 몇 초에 돌리므로 폭주 방지는 아래에서 따로 시험
cfg['accounts']['paper'] = {'app_key': 'x' * 20, 'app_secret': 'y' * 20, 'account': '12345678-01'}
tr.signal_job(cfg, clock['d'])
for i_d, d in enumerate(run_days):
    clock['d'] = d
    kc.d = d
    clock['hm'] = '08:30'
    kc.transmit_resv()                                   # 어젯밤 예약주문 → 장 시작 전 실제 주문
    clock['hm'] = '08:35'
    tr.preopen(cfg, kc, d)
    kc.settle('open')
    clock['hm'] = '09:02'
    tr.deferred(cfg, kc, d)
    kc.settle('open')
    tr.sync(kc, d)
    clock['hm'] = '10:00'
    tr.intraday(cfg, kc, d, {})
    clock['hm'] = '15:10'
    tr.sweep_prep(cfg, kc, d)
    clock['hm'] = '15:20'
    tr.on_buy(cfg, kc, d)
    tr.sweep_buy(cfg, kc, d)
    kc.settle('close')
    clock['hm'] = '15:45'
    tr.eod(cfg, kc, d)
    clock['hm'] = '19:00'
    tr.signal_job(cfg, d)
    if i_d + 1 < len(run_days):
        tr.reserve_sells(cfg, kc, run_days[i_d + 1])
x = db.conn()
live = {(r['sleeve'], r['ticker'], r['entry_date']) for r in x.execute("SELECT * FROM lots WHERE sleeve IN ('LVH','REV') AND entry_date IS NOT NULL")}
live_x = {(r['sleeve'], r['ticker'], r['entry_date'], r['exit_date']) for r in x.execute("SELECT * FROM lots WHERE sleeve IN ('LVH','REV') AND status='청산'")}
_sw = db.etf_bars('069500')
res = B.simulate(D, days[-n - 1], run_days[-1], {'LVH': .40, 'REV': .25, 'DV': .0, 'ON': .35}, gap_skip=0.05, sweep_etf=(_sw['close'], _sw['open']),
                 sw_signal=S.sw_weight(_sw['close'], 'night'), sw_overnight=True)
bt = {(t[0], t[1], t[2]) for t in res['trades'] if t[0] in ('LVH', 'REV')} | {(l['s'], l['t'], l['d']) for l in res['open_lots'] if l['s'] in ('LVH', 'REV')}
bt_x = {(t[0], t[1], t[2], t[3]) for t in res['trades'] if t[0] in ('LVH', 'REV')}
check('실전 ↔ 백테스트 매수 일치', live == bt and len(live) > 50, f'{len(live & bt)}/{len(live | bt)}')
check('실전 ↔ 백테스트 청산 일치', live_x == bt_x and len(live_x) > 20, f'{len(live_x & bt_x)}/{len(live_x | bt_x)}')
sw_b = x.execute("SELECT COUNT(*) FROM orders WHERE kind='sw_buy' AND status='체결'").fetchone()[0]
sw_s = x.execute("SELECT COUNT(*) FROM orders WHERE kind='sw_sell' AND status='체결'").fetchone()[0]
gp = x.execute("SELECT COUNT(*) FROM decisions WHERE reason LIKE '시가 갭%'").fetchone()[0]
sw_days = x.execute("SELECT COUNT(DISTINCT date) FROM positions_daily WHERE ticker='069500'").fetchone()[0]
check('KODEX 200 밤사이 회전: 날마다 종가 매수 → 다음 날 시가 매도', sw_b >= n - 1 and sw_s >= n - 2 and sw_days >= n - 1,
      f'매수 {sw_b} · 매도 {sw_s} · 장 마감 보유 {sw_days}일 · 갭으로 안 산 후보 {gp}')
check('(참고) 남는 현금 칸 기록', True,
      f'보유 {sw_days}일 · 매수 {sw_b} · 매도 {sw_s} · 갭으로 안 산 후보 {gp}')
dv = 15  # 회전형 기본은 배당·가치 0% → 아래 검사는 밤사이만
on = x.execute("SELECT COUNT(*), AVG(ret) FROM lots WHERE sleeve='ON' AND status='청산'").fetchone()
check('밤사이 ETF 매일 (자산 35%)', on[0] >= n - 2, f'ON {on[0]}건 평균 {on[1]:+.3f}%')
bad = db.mconn().execute("SELECT COUNT(*) FROM log WHERE msg LIKE '%불일치%'").fetchone()[0]
check('잔고 불일치 0', bad == 0)
_rs = dict(x.execute("SELECT status, COUNT(*) FROM orders WHERE resv_seq IS NOT NULL GROUP BY status").fetchall())
_dup = x.execute("SELECT COUNT(*) FROM (SELECT lot_id, date FROM orders WHERE side='sell' AND status='체결' GROUP BY lot_id, date HAVING SUM(filled) > (SELECT qty0 FROM lots WHERE id=lot_id))").fetchone()[0]
check('📅 예약주문: 저녁에 다음 날 매도 예약 → 아침 주문번호 연결 → 체결 · 중복 매도 없음', _rs.get('체결', 0) > 50 and not _rs.get('예약') and _dup == 0, str(_rs))
check('장중 평가 기록 (최근 10일만 보관)', 5 <= x.execute('SELECT COUNT(*) FROM intraday').fetchone()[0] <= 12, f"{x.execute('SELECT COUNT(*) FROM intraday').fetchone()[0]}건")
g = tr.gate(cfg)
check('판정: 40일이라 60일 기준 미달', not g['pass'] and not g['rows'][0]['ok'], g['rows'][0]['v'])
# 지수가 60일선 아래로 → 15:20에 KODEX 200 전부 팖
import pandas as _pd
_eb = db.etf_bars
db.etf_bars = lambda t, f='0', to='99999999': _pd.DataFrame({'close': [100.0 + i for i in range(80)]}, index=[f'2026{i:04d}' for i in range(80)])
w_up, _ = tr.sw_target({**cfg, 'sweep_mode': 'ma60'}, 200.0)
w_dn, _ = tr.sw_target({**cfg, 'sweep_mode': 'ma60'}, 120.0)
_held0 = sum(q for _, q in tr.sw_avail())
_o = tr.sw_target
tr.sw_target = lambda c, px: (0.0, 'ma60')
clock['hm'] = '15:20'
tr.sweep_buy(cfg, kc, run_days[-1])
_held1 = sum(q for _, q in tr.sw_avail())
tr.sw_target, db.etf_bars = _o, _eb
check('KODEX 200 하락 추세 → 전부 매도 주문 (60일선 계산 포함)', w_up == 1.0 and w_dn == 0.0 and _held0 > 0 and _held1 == 0, f'비중 위 {w_up} · 아래 {w_dn} · 보유 {_held0} → 팔 수 있는 수량 {_held1}')
kc.settle('close')
tr.sync(kc, run_days[-1])

# 🛡 주문 안전장치
_g = {'guard_per_min': 3, 'guard_per_day': 10 ** 9, 'cap_mode': 'fixed', 'cap': 10_000_000}
x.execute("INSERT INTO orders (date, ts, side, ticker, qty, status, kind) VALUES (?,?,?,?,?,?,?)", (clock['d'], db.now_s(), 'buy', 'Z1', 1, '거절', 'x'))
x.execute("INSERT INTO orders (date, ts, side, ticker, qty, status, kind) VALUES (?,?,?,?,?,?,?)", (clock['d'], db.now_s(), 'buy', 'Z2', 1, '거절', 'x'))
x.execute("INSERT INTO orders (date, ts, side, ticker, qty, status, kind) VALUES (?,?,?,?,?,?,?)", (clock['d'], db.now_s(), 'buy', 'Z3', 1, '거절', 'x'))
g1 = tr.guard(_g, x, 'buy', 'entry', None, 'A', 1, 1000)
_lid0 = x.execute("SELECT lot_id FROM orders WHERE lot_id IS NOT NULL AND status='접수' LIMIT 1").fetchone()
_g['guard_per_min'] = 10 ** 9
g2 = tr.guard(_g, x, 'sell', 'manual', _lid0[0], 'A', 1, 1000) if _lid0 else '같은 묶음'
g3 = tr.guard(_g, x, 'buy', 'entry', None, 'A', 100, 30_000)
g4 = tr.guard(_g, x, 'buy', 'on_buy', None, 'A', 100, 30_000)
g5 = tr.guard(_g, x, 'buy', 'entry', None, 'A', 10, 30_000)
x.execute("DELETE FROM orders WHERE ticker IN ('Z1','Z2','Z3')")
x.commit()
check('🛡 주문 안전장치: 분당 폭주 · 같은 묶음 중복 · 큰 금액 실수 막고 정상 주문은 통과', '분당' in g1 and '같은 묶음' in g2 and '20%' in g3 and not g4 and not g5,
      f'{g1} / {g2} / {g3}')

# ── 2-1. 거래 기록 (HTS급) ──
import tk_journal as J
o_all = [dict(r) for r in x.execute('SELECT * FROM orders')]
fq = {r[0]: r[1] for r in x.execute('SELECT order_id, SUM(qty) FROM fills GROUP BY order_id')}
check('기록: 체결 조각 합 = 주문 체결 수량', all(fq.get(o['id'], 0) == (o['filled'] or 0) for o in o_all) and len(fq) > 100, f'{len(fq)}건 주문 · 체결 조각 {sum(fq.values()):,}주')
ev = {}
for r in x.execute('SELECT order_id, status FROM order_events'):
    ev.setdefault(r[0], []).append(r[1])
check('기록: 주문마다 상태 이력 (보냄/예약 → 접수 → 체결)', all(ev.get(o['id'], [None])[0] in ('보냄', '예약') for o in o_all if o['status'] != '거절' or o['resv_seq'] is None)
      and all('접수' in ev[o['id']] and '체결' in ev[o['id']] for o in o_all if o['status'] == '체결'), f'이벤트 {sum(len(v) for v in ev.values())}건')
cl = [dict(r) for r in x.execute("SELECT * FROM lots WHERE status='청산'")]
okp = all(abs(l['pnl'] - (l['proceeds'] - l['cost'] - l['fee'] - l['tax'])) < 1 for l in cl)
tax_ok = all((l['tax'] > 0) == (l['sleeve'] not in ('ON', 'SW')) for l in cl)
check('기록: 손익 = 매도 - 매수 - 수수료 - 세금 (ETF 거래세 없음)', okp and tax_ok and len(cl) > 50, f"청산 {len(cl)} · 수수료 {sum(l['fee'] for l in cl):,.0f} · 세금 {sum(l['tax'] for l in cl):,.0f}")
nb = x.execute("SELECT COUNT(*) FROM orders WHERE side='buy' AND kind='entry'").fetchone()[0]
dec = dict(x.execute('SELECT action, COUNT(*) FROM decisions GROUP BY action').fetchall())
check('기록: 판단 기록 (산 것 · 미룬 것 · 못 산 것)', dec.get('buy', 0) + dec.get('defer', 0) >= nb > 0 and dec.get('sell', 0) > 0, str(dec))
sig_ok = x.execute("SELECT COUNT(*) FROM lots WHERE sleeve IN ('LVH','REV') AND entry_date IS NOT NULL AND (sig_rank IS NULL OR entry_info IS NULL OR sig_ref IS NULL)").fetchone()[0]
check('기록: 거래마다 진입 근거 (순위 · 점수 · 지표 · 신호가)', sig_ok == 0)
ad = x.execute('SELECT COUNT(*) FROM account_daily').fetchone()[0]
pdn = x.execute('SELECT COUNT(DISTINCT date) FROM positions_daily').fetchone()[0]
check('기록: 날마다 매매일지 · 잔고 이력', ad == n and pdn >= n - 1, f'매매일지 {ad}일 · 잔고 {pdn}일')
nc = db.mconn().execute("SELECT COUNT(DISTINCT date), COUNT(*) FROM cands WHERE src='live'").fetchone()
check('기록: 신호 후보 상위 50 날마다', nc[0] == n + 1 and nc[1] >= (n + 1) * 100, f'{nc[0]}일 · {nc[1]:,}줄')
post = [dict(r) for r in x.execute("SELECT * FROM lots WHERE status='청산' AND sleeve IN ('LVH','REV') AND post_at IS NOT NULL")]
gap = [abs(l['ret'] + (l['fee'] + l['tax']) / l['cost'] * 100 - l['model_ret']) for l in post if l['model_ret'] is not None]
check('사후 계산: 가짜 체결(시가) → 체결 차이 0 · 실제 = 모델 수익', len(post) > 20 and all(abs(l['slip_in'] or 0) < 0.01 for l in post) and max(gap) < 0.05
      and all(l['mae'] <= 0.001 and l['mfe'] >= -0.001 for l in post if l['mae'] is not None), f'{len(post)}건 · 최대 차이 {max(gap):.4f}%p')

# PC가 늦게 켜짐 (10:00) → 매도할 것만 시장가 · 새 매수 없음
_lid = tr.new_lot('LVH', run_days and list(kc.pos)[0] if kc.pos else '005930', '늦게켜짐시험', '', run_days[-1])
x.execute("UPDATE lots SET status='보유', qty=1, qty0=1, entry_px=1, cost=1, entry_date=?, sell_flag=1, sell_reason='hold20' WHERE id=?", (run_days[-1], _lid))
x.commit()
db.meta_set(f"plan_used_{db.meta_get('last_signal_date')}", '')
clock['hm'] = '10:00'
_n0 = x.execute('SELECT COUNT(*) FROM orders').fetchone()[0]
tr.late_open(cfg, kc, run_days[-1])
_new = [dict(r) for r in x.execute('SELECT * FROM orders WHERE id > (SELECT MAX(id) FROM orders) - ?', (x.execute('SELECT COUNT(*) FROM orders').fetchone()[0] - _n0,))]
_sk = x.execute("SELECT COUNT(*) FROM decisions WHERE reason LIKE 'PC가 늦게%'").fetchone()[0]
check('PC 늦게 켜짐: 매도할 것만 지금 시장가 · 오늘 새 매수 없음 (이유 기록)', any(o['lot_id'] == _lid and o['side'] == 'sell' for o in _new)
      and not any(o['side'] == 'buy' for o in _new) and _sk > 0, f'새 주문 {len(_new)}건 (매수 0) · 안 산 후보 {_sk}')

# ── 2-2. 모의 ↔ 실전 (계좌 설정만 바뀜) ──
try:
    tr.switch_mode(cfg, 'real', True)
    check('모드: 실전 계좌 없으면 거부', False)
except ValueError as e:
    check('모드: 실전 계좌 없으면 거부', True, str(e)[:40])
cfg['accounts']['real'] = {'app_key': 'r' * 20, 'app_secret': 's' * 20, 'account': '87654321-01'}
try:
    tr.switch_mode(cfg, 'real', False)
    check('모드: 화면 확인 없으면 거부', False)
except ValueError:
    check('모드: 화면 확인 없으면 거부', True)
cfg['caps'] = {'paper': None, 'real': 5_000_000}
tr.switch_mode(cfg, 'real', True)
check('모드: 실전 → 자동주문 · 설정 그대로 · 계좌 · 한도 · 장부만 바뀜',
      db.mode() == 'real' and cfg['kis_on'] and db.conn().execute('SELECT COUNT(*) FROM lots').fetchone()[0] == 0 and tr.cap(cfg) == 5_000_000
      and tr.client(cfg).cano == '87654321' and tr.alloc(cfg) == tr.alloc({**cfg, 'mode': 'paper'}),
      f'실전 장부 lots {db.conn().execute("SELECT COUNT(*) FROM lots").fetchone()[0]} · 한도 {tr.cap(cfg):,} · 계좌 {tr.client(cfg).masked_account}')
tr.switch_mode(cfg, 'paper')
check('모드: 모의로 돌아오면 모의 장부 · 모의 계좌 그대로', db.mode() == 'paper' and db.conn().execute('SELECT COUNT(*) FROM lots').fetchone()[0] == len(
    x.execute('SELECT id FROM lots').fetchall()) and tr.client(cfg).cano == '12345678' and tr.cap(cfg) == int(tr.last_equity()))
_eq = tr.last_equity()
check('운용 자금: 기본은 계좌 전체(마지막 평가액 · 수익 따라) · 상한 고정이면 그 금액', abs(tr.cap(cfg) - _eq) < 1 and _eq != 10_000_000
      and tr.cap({**cfg, 'cap_mode': 'fixed', 'cap': 10_000_000}) == 10_000_000, f'계좌 전체 {tr.cap(cfg):,} · 고정 10,000,000')

check('실험 설정: 자리 · 건너뛸 순위가 실전 · 백테스트에 같이', tr.slots({'slots': {'LVH': 40}})['LVH'] == 40 and tr.slots({})['REV'] == 30
      and tr.picks({'pick_skip': {'REV': 3}})['REV'] == (3, 3) and tr.picks({})['LVH'] == (0, 3)
      and S.top_n(__import__('pandas').Series({'a': 3, 'b': 2, 'c': 1, 'd': 0}), {'d'}, 2, 1) == ['b', 'c'])

class _K:
    def expected(self, t): return {'gap': {'A': 7.2, 'B': 2.0}.get(t)}
    def price(self, t): return {'price': 10600, 'chg': 6.0, 'open': 10700}
g1 = tr.gap_check({}, _K(), {'sleeve': 'LVH', 'ticker': 'A', 'name': 'A'}, 'pre')
g2 = tr.gap_check({}, _K(), {'sleeve': 'LVH', 'ticker': 'B', 'name': 'B'}, 'pre')
g3 = tr.gap_check({}, _K(), {'sleeve': 'DV', 'ticker': 'A', 'name': 'A'}, 'pre')
g4 = tr.gap_check({'gap_skip': 0}, _K(), {'sleeve': 'REV', 'ticker': 'A', 'name': 'A'}, 'pre')
g5 = tr.gap_check({}, _K(), {'sleeve': 'REV', 'ticker': 'A', 'name': 'A'}, 'open')
check('갭 필터: +5% 넘으면 LVH·REV 안 삼 · DV는 상관없음 · 0이면 끔 · 장중은 오늘 시가로', g1 and not g2 and not g3 and not g4 and g5, f'{g1} / 장중 {g5}')

# ── 2-3. 분석 ──
import tk_analyze as A
res = A.analyze()
check('분석: 칸별 · 청산 이유 · 지표 구간 · 체결 품질 · 후보 순위 · 놓친 거래',
      res['summary'] and res['by_sleeve'] and res['by_exit'] and res['buckets'] and res['execution'] and res['cands'] is not None and 'missed' in res,
      f"거래 {res['summary'][0]['n'] if res['summary'] else 0} · 구간표 {len(res['buckets'])} · 제안 {len(res['ideas'])} · "
      + str({k: bool(res[k]) if k != 'cands' else res[k] is not None for k in ('summary', 'by_sleeve', 'by_exit', 'buckets', 'execution', 'cands', 'missed')}))
md = A.report_md(res)
check('분석: 보고서 · 패키지(zip · 비밀 값 없음)', '# TK자동매매 거래 분석' in md and '12345678' not in json.dumps(res, ensure_ascii=False, default=str))

# ── 2-4. ⏱ 1분봉 수집기 ──
import tk_minute as MN
from tk_kis import KIS as _KIS


def _fake_minutes(o, h, l, c, v):
    """일봉 하나 → 381개 1분봉 (09:00~15:20 + 15:30) — 시가에서 종가로 걸어가며 고가 · 저가를 한 번씩 찍음"""
    hms = [9 * 100 + m for m in range(60)] + [h_ * 100 + m for h_ in range(10, 15) for m in range(60)] + [1500 + m for m in range(21)] + [1530]
    n = len(hms)
    out = []
    for i, hm in enumerate(hms):
        p = o + (c - o) * i / (n - 1)
        hi, lo = (h if i == n // 3 else max(p, p * 1.001)), (l if i == 2 * n // 3 else min(p, p * 0.999))
        out.append((hm, p, max(hi, p), min(lo, p), p, int(v / n), round(p * v / n)))
    return out


class _MK(FM.FakeKIS):
    calls = 0
    def minute_day(self, t, d):
        _MK.calls += 1
        P = D['P']
        if t in ('229200', '069500') or t not in P['close'].columns or d not in P['close'].index or P['close'].at[d, t] != P['close'].at[d, t]:
            return []
        return _fake_minutes(*(float(P[k].at[d, t]) for k in ('open', 'high', 'low', 'close')), float(P['volume'].at[d, t] or 1000))
    minute_today = minute_day


mk = _MK(D['P'], D['on'])
d_m = run_days[-1]
uni = MN.universe(d_m, top=50)
why = {}
for t, nm, rk, w in uni:
    why[w.split()[0]] = why.get(w.split()[0], 0) + 1
_prev20 = [x for x in db.trading_days('0', d_m) if x < d_m][-20:]
check('분봉 대상: 직전 20일 거래대금(그날 자료 안 씀) · 전날 후보 · 보유 · ETF · 급등락', why.get('거래대금') == 50 and why.get('후보', 0) > 0 and why.get('보유', 0) + why.get('거래', 0) > 0
      and {'229200', '069500'} <= {u[0] for u in uni} and d_m not in _prev20, str(why))
MN.STATE.update(started=time.time() - 30, ended=0.0, done_tk=0, day_i=0, days_n=3, running=True)
n1 = MN.collect_day(mk, d_m, top=50)
_st = MN.status()
check('분봉 진행: 걸린 시간 · 남은 시간 · 진행률 · 속도', _st['elapsed'] >= 30 and _st['eta'] is not None and _st['eta'] > 0 and 30 <= _st['pct'] <= 36 and _st['rate'],
      f"걸린 {_st['elapsed']}초 · 남은 약 {_st['eta']:.0f}초 · {_st['pct']}% · 분당 {_st['rate']}종목")
MN.STATE.update(running=False)
c1 = _MK.calls
n2 = MN.collect_day(mk, d_m, top=50)
bars = MN.day_bars(uni[0][0], d_m)
ok_ohlc = bars and abs(bars[0][1] - float(D['P']['open'].at[d_m, uni[0][0]])) < 1e-6 and abs(bars[-1][4] - float(D['P']['close'].at[d_m, uni[0][0]])) < 1e-6
check('분봉 수집: 대상 받기 · 다시 돌리면 이미 받은 것 건너뜀 · 시가/종가 맞음', n1 > 50 and n2 == 0 and _MK.calls == c1 and len(bars) == 382 and ok_ohlc,
      f'{n1}종목 · 두 번째 {n2} · 1종목 {len(bars)}봉')
zb = MN.export_zip(d_m, d_m)
_zz = zipfile.ZipFile(io.BytesIO(zb))
_tot = MN.conn().execute('SELECT COUNT(*) FROM bars WHERE date=?', (d_m,)).fetchone()[0]
MN.conn().execute('DELETE FROM bars')
MN.conn().execute('DELETE FROM done')
MN.conn().commit()
_back = MN.import_csv(d_m, io.TextIOWrapper(_zz.open(f'bars/{d_m}.csv'), encoding='utf-8-sig'))
check('분봉 zip 내보내기 → 다시 가져오기 (단타 앱과 같은 형식)', f'bars/{d_m}.csv' in _zz.namelist() and 'universe.csv' in _zz.namelist() and _back == _tot > 10000,
      f'{_tot:,}봉 → {_back:,}봉')
# 단타 앱 danta.db 형식 가져오기 (가져오기 폴더)
import sqlite3 as _sq
_dp = os.path.join(db.DATA_DIR, 'danta_test.db')
_c = _sq.connect(_dp)
_c.execute('CREATE TABLE bars (ticker TEXT, date TEXT, hm INTEGER, open REAL, high REAL, low REAL, close REAL, vol INTEGER, amt REAL, PRIMARY KEY (ticker, date, hm))')
_c.executemany('INSERT INTO bars VALUES (?,?,?,?,?,?,?,?,?)', [('999999', '20250102', 900 + i, 100, 101, 99, 100, 10, 1000) for i in range(30)])
_c.commit()
_c.close()
_r = col.import_file(_dp)
check('단타 앱 danta.db → 1분봉으로 가져오기', _r.get('minute') == 30 and len(MN.day_bars('999999', '20250102')) == 30, str(_r.get('minute')))
# KIS 분봉 조회: 15:30부터 거꾸로 120봉씩 · 누적 거래대금 → 분당
_kp = _KIS('paper', 'APPKEYMINUTE00001', 'S', '12345678-01', db.DATA_DIR)
_src = _fake_minutes(100, 110, 95, 105, 38100)
_cum, _rows = 0, []
for hm, o_, h_, l_, c_, v_, a_ in _src:
    _cum += a_
    _rows.append({'stck_bsop_date': '20260102', 'stck_cntg_hour': f'{hm:04d}00', 'stck_oprc': o_, 'stck_hgpr': h_, 'stck_lwpr': l_, 'stck_prpr': c_, 'cntg_vol': v_, 'acml_tr_pbmn': _cum})
_seen = []
def _get(path, tr_id, params, tr_cont='', retry=4):
    _seen.append((tr_id, params['FID_INPUT_HOUR_1']))
    hh = int(params['FID_INPUT_HOUR_1'][:4])
    pg = [r for r in _rows if int(r['stck_cntg_hour'][:4]) <= hh][-120:][::-1]
    return {'rt_cd': '0', 'output2': pg}, {}
_kp.get = _get
_mb = _kp.minute_day('005930', '20260102')
check('KIS 1분봉: FHKST03010230 · 120봉씩 4쪽 · 누적 거래대금 → 분당', len(_mb) == len(_src) == 382 and len(_seen) == 4 and _seen[0] == ('FHKST03010230', '153000')
      and abs(sum(b[6] for b in _mb) - _cum) < 2 and _mb[0][0] == 900 and _mb[-1][0] == 1530, f'{len(_mb)}봉 · 호출 {len(_seen)} · {_seen[:2]}')

# ── 2-4b. ⏱ 장중 연구실 · 장중 칸 ──
import tk_intraday as IL
_H = [9 * 100 + m for m in range(60)] + [h_ * 100 + m for h_ in range(10, 15) for m in range(60)] + [1500 + m for m in range(21)]


def _mkA(path, vol=None):
    out = []
    for i, (hm, p) in enumerate(zip(_H, path)):
        pv = path[i - 1] if i else p
        out.append((hm, pv, max(pv, p), min(pv, p), p, (vol[i] if vol else 1000), p * 1000))
    return IL.arr(out)


_n = len(_H)
_g = IL.trade('GAPREV', _mkA([95, 94.5, 94, 94.2, 94.6] + list(np.linspace(94.8, 101, _n - 5))), {'pc': 100})
_g0 = IL.trade('GAPREV', _mkA([99, 98.5, 98] + list(np.linspace(98, 101, _n - 3))), {'pc': 100})          # 갭 −1% → 해당 없음
_o = IL.trade('ORB', _mkA([100 + (i % 10) * 0.3 for i in range(30)] + [102.5] * 10 + [103.5] + list(np.linspace(103.6, 107, _n - 41)),
                          [1000] * 40 + [5000] + [1000] * (_n - 41)), {'pc': 100})
_o0 = IL.trade('ORB', _mkA([100 + (i % 10) * 0.3 for i in range(30)] + [102.5] * 10 + [103.5] + list(np.linspace(103.6, 107, _n - 41))), {'pc': 100})  # 거래량 없음
_v = IL.trade('VWAP', _mkA([100] * 5 + list(np.linspace(100, 96, 60)) + [96] * 20 + list(np.linspace(96.5, 100, _n - 85))), {'pc': 100})
_pp = [100] + list(np.linspace(100, 97.5, 30)) + [97.5] * (_n - 31)
_p1, _p0 = IL.trade('PULL', _mkA(_pp), {'pc': 100, 'cand': True}), IL.trade('PULL', _mkA(_pp), {'pc': 100, 'cand': False})
_fee, _tax = IL._rates()
check('⏱ 장중 규칙 4개: 시가 급락 되돌림 · 장 초반 돌파(거래량 2배) · VWAP 되찾기 · 후보 눌림 — 신호 · 다음 분 시가 매수 · 익절/15:15 정리 · 비용',
      _g and _g['sig_hm'] == 918 and _g['hm'] == 919 and _g['kind'] == 'tp' and not _g0 and _o and _o['kind'] == 'tp' and abs(_o['ret'] - (1.03 / (1 + _fee) * (1 - _fee - _tax) - 1)) < 1e-9
      and not _o0 and _v and _v['sig_hm'] >= 1000 and _v['kind'] == 'tp' and _p1 and _p1['kind'] == 'time' and _p1['xhm'] == 1515 and not _p0,
      f"GAPREV {_g['sig_hm']}→{_g['ret'] * 100:+.2f}% · ORB {_o['ret'] * 100:+.2f}% · VWAP {_v['ret'] * 100:+.2f}% · PULL {_p1['ret'] * 100:+.2f}%")
_A = _mkA([100] * _n)
_A[6, 2], _A[6, 3] = 103, 97                                             # 한 분에 고가 103 · 저가 97
check('장중 청산: 같은 분에 익절 · 손절 둘 다 닿으면 손절로(보수적) · 15:15 시가 정리', IL.exit_scan(_A, 5, 102, 98)[2] == 'sl' and IL.exit_scan(_A, 9, 120, 50)[1:] == (100 * (1 - IL.SLIP), 'time'))
_rng = np.random.default_rng(3)
_bad, _hits = [], 0
for _k in range(40):                                                    # 무작위 분봉: 실시간처럼 한 분씩 늘려 가며 판단 = 연구실 판단 (앞날 정보 안 씀)
    _w = 100 * np.exp(np.cumsum(_rng.normal(0, 0.004, _n)))
    _w = _w / _w[0] * (100 * (0.96 if _k % 3 == 0 else 1.0))
    _AA = _mkA(list(_w), list(_rng.integers(500, 6000, _n)))
    for _r in IL.RULES:
        _ctx = {'pc': 100, 'cand': True}
        _full = IL.signal(_r, _AA, _ctx)
        _live = next((k_ - 1 for k_ in range(6, _n + 1) if (s_ := IL.signal(_r, _AA[:k_], _ctx)) and s_['i'] == k_ - 1), None)
        _hits += _full is not None
        if (_full['i'] if _full else None) != _live:
            _bad.append((_k, _r))
check('장중 신호: 실시간(한 분씩) = 연구실(하루 전체) — 미래 분봉 안 씀', not _bad and _hits >= 10, f'신호 {_hits}건 · 불일치 {_bad[:3]}')
_ir = IL.run(cfg)
check('⏱ 장중 연구실: 1분봉 전체 · 규칙 4개 · 대조군 · 두 기간 · 미리 정한 기준 → 자료 모자라면 "자료 부족"', _ir and len(_ir['rules']) == 4 and _ir['days'] >= 1
      and all(r_['verdict'] == '자료 부족' for r_ in _ir['rules']) and 'universe' in _ir['uni_src'] and _ir['cost'] > 0.3,
      f"{_ir['days']}일 · 비용 {_ir['cost']}% · " + ' · '.join(f"{r_['key']} {r_['all']['n']}건" for r_ in _ir['rules']) if _ir else str(IL.STATE))
_s = lambda n, a: {'n': n, 'avg': a, 't': 3.0}
_vp = IL.verdict({'days': 60, 'all': _s(100, .2), 'half1': _s(50, .1), 'half2': _s(50, .3), 'ctl1': _s(50, .0), 'ctl2': _s(50, .1)})
_vf = IL.verdict({'days': 60, 'all': _s(100, .2), 'half1': _s(50, .1), 'half2': _s(50, .3), 'ctl1': _s(50, .0), 'ctl2': _s(50, .4)})
_vn = IL.verdict({'days': 60, 'all': _s(100, .2), 'half1': _s(50, -.1), 'half2': _s(50, .5), 'ctl1': _s(50, -.3), 'ctl2': _s(50, .1)})
check('장중 판정: 두 기간 모두 + · 둘 다 대조군보다 나음 · t≥2 → 통과 / 하나라도 아니면 탈락', _vp[0] == '통과' and _vf[0] == '탈락' and _vn[0] == '탈락', f'{_vf[1]} · {_vn[1]}')
# 장중 칸 (가짜 KIS · 모의) — 통과 규칙만 · 웹소켓 분봉 → 신호 → 매수 → 익절 · 15:15 정리
_d = run_days[-1]
clock['d'], kc.d = _d, _d
_save = open(IL.RESULT, encoding='utf-8').read()
_cfgi = {**cfg, 'intraday_on': True, 'intraday_rules': ['PULL'], 'intraday_pct': 50, 'intraday_slots': 5}
clock['hm'] = '09:26'
_none = IL.watch(_cfgi, _d)
check('장중 칸: 연구실 통과 못 한 규칙은 켜도 안 씀 (볼 종목도 없음)', _none == [] and IL.rules_on(_cfgi) == ([], ['PULL']))
_fake = json.loads(_save)
for r_ in _fake['rules']:
    r_['verdict'] = '통과' if r_['key'] == 'PULL' else '탈락'
json.dump(_fake, open(IL.RESULT, 'w', encoding='utf-8'), ensure_ascii=False)
_prev = tr.prev_trading_day(_d)
_pcs = {r_[0]: r_[1] for r_ in db.mconn().execute('SELECT ticker, close FROM bars WHERE date=?', (_prev,))}
_held = {l_['ticker'] for l_ in tr.open_lots()}
_wl = IL.watch(_cfgi, _d)
_cands = [r_[0] for r_ in db.mconn().execute("SELECT ticker FROM cands WHERE date=? AND sleeve IN ('LVH','REV') AND rank<=20 ORDER BY rank", (_prev,))]
_tk = [t_ for t_ in _cands if t_ in _wl and t_ in _pcs and t_ not in _held and float(D['P']['close'].at[_d, t_] or 0) > 0][:2]
for t_ in _tk:
    pc_ = _pcs[t_]
    IL.LIVE['seeded'].add((t_, _d))
    IL.seed(t_, [(hm_, pc_ * .99, pc_ * .995, pc_ * .985, pc_ * (.975 if hm_ == 925 else .99), 1000, pc_ * 1000) for hm_ in range(900, 926)])
kc.cash += 5_000_000
x = db.conn()
_n0 = x.execute("SELECT COUNT(*) FROM orders WHERE sleeve='IN'").fetchone()[0]
tr.db.meta_set('auto_pause', '')
IL.step(_cfgi, kc, _d, {})
IL.step(_cfgi, kc, _d, {})                                                # 같은 분에 또 불러도 중복 매수 없음
tr.sync(kc, _d)
_ib = [dict(r_) for r_ in x.execute("SELECT o.*, l.entry_info, l.entry_px, l.status ls FROM orders o JOIN lots l ON l.id=o.lot_id WHERE o.sleeve='IN' AND o.kind='in_buy'")]
check('장중 칸: 웹소켓 분봉 → 통과 규칙(후보 눌림) 신호가 방금 끝난 분에 → 시장가 매수 · 종목당 하루 1번 · 체결 → 보유',
      len(_wl) > 0 and len(_tk) == 2 and len(_ib) == 2 and all(json.loads(o_['entry_info'])['rule'] == 'PULL' and o_['ls'] == '보유' for o_ in _ib) and _n0 == 0,
      f"볼 종목 {len(_wl)} · 매수 {[(o_['name'], o_['qty']) for o_ in _ib]}")
clock['hm'] = '10:00'
_l1 = [l_ for l_ in tr.open_lots('IN')][0]
IL.step(_cfgi, kc, _d, {_l1['ticker']: _l1['entry_px'] * 1.031})
tr.sync(kc, _d)
_x1 = x.execute('SELECT status, exit_kind FROM lots WHERE id=?', (_l1['id'],)).fetchone()
clock['hm'] = '15:15'
IL.step(_cfgi, kc, _d, {})
tr.sync(kc, _d)
_left = [l_ for l_ in tr.open_lots('IN')]
_kinds = [r_[0] for r_ in x.execute("SELECT kind FROM orders WHERE sleeve='IN' ORDER BY id")]
check('장중 칸: 익절(+3%) 실시간 체결가로 · 15:15 남은 것 모두 정리 · 장중 칸 보유 0', tuple(_x1) == ('청산', 'in_tp') and not _left and _kinds.count('in_close') == 1,
      f'{_kinds}')
check('장중 칸 끄면 아무것도 안 함', IL.step({**_cfgi, 'intraday_on': False}, kc, _d, {}) is None and IL.watch({**_cfgi, 'intraday_on': False}, _d) == [])
kc.cash -= 5_000_000
open(IL.RESULT, 'w', encoding='utf-8').write(_save)

# ── 2-5. 👥 그림자 운용 ──
import tk_shadow as SHD
db.gmeta_set('shadow_start', run_days[-10])
_sh = SHD.run(cfg)
_names = [r_['name'] for r_ in _sh['rows']] if _sh else []
check('👥 그림자 운용: 지금 설정 + 실험 7개 · 앞으로(시작일부터) · 최근 60일 · 기본 대비', _sh and len(_sh['rows']) == 8 and _names[0].startswith('기본')
      and all(r_['forward']['days'] >= 8 and r_['recent']['days'] >= 50 for r_ in _sh['rows']) and _sh['rows'][1]['forward'].get('vs') is not None,
      ' · '.join(f"{r_['name'][:8]} {r_['forward']['ret']:+.1f}%" for r_ in _sh['rows'][:4]) if _sh else str(SHD.STATE))

# ── 3. 서버 보안 ──
from fastapi.testclient import TestClient
import tk_server as SV
cli = TestClient(SV.app, base_url='http://127.0.0.1:8086')
check('보안: 토큰 없으면 401', cli.get('/api/state').status_code == 401)
check('보안: 토큰 있으면 200', cli.get('/api/state', headers={'X-TK-Token': SV.TOKEN}).status_code == 200)
evil = TestClient(SV.app, base_url='http://evil.example.com')
check('보안: 다른 Host 403 (DNS 재바인딩)', evil.get('/api/state', headers={'X-TK-Token': SV.TOKEN}).status_code == 403)
rr = cli.post('/api/pause', content='{"pause":true}', headers={'X-TK-Token': SV.TOKEN, 'content-type': 'text/plain'})
check('보안: JSON 아닌 POST 415 (CSRF 단순 요청 차단)', rr.status_code == 415)
check('보안: 화면에 토큰 주입', SV.TOKEN in cli.get('/').text)
_t1 = SV.tg_command('/상태')
_t2 = SV.tg_command('/매수중지'); _p1 = SV.CFG.get('pause_buy')
_t3 = SV.tg_command('/재개'); _p2 = SV.CFG.get('pause_buy')
_t4 = SV.tg_command('/정지'); _h = bool(SV.tr.halted()) and not SV.CFG.get('kis_on')
_t5 = SV.tg_command('/정지해제'); _h2 = not SV.tr.halted()
_t6 = SV.tg_command('/아무거나')
check('📱 텔레그램 명령: /상태 · /매수중지 · /재개 · /정지(자동주문 끔) · /정지해제 · 모르는 명령은 도움말', ('계좌' in _t1 or '기록' in _t1) and _p1 is True and _p2 is False
      and _h and _h2 and '/정지' in _t6, _t1.split(chr(10))[0])
_si = cli.get('/api/sysinfo', headers={'X-TK-Token': SV.TOKEN}).json()
check('📋 시스템 정보: 모듈 · 줄 수 · API · 하루 흐름 · 안전장치 · 연구 이력', len(_si['modules']) >= 12 and _si['lines'] > 4000 and _si['endpoints'] >= 25
      and len(_si['schedule']) >= 15 and len(_si['safety']) >= 8 and _si['research'], f"{_si['lines']:,}줄 · API {_si['endpoints']} · 일정 {len(_si['schedule'])}")
_inv = cli.get('/api/inventory?force=1', headers={'X-TK-Token': SV.TOKEN}).json()
_names = {r['name'] for r in _inv['rows']}
check('📚 데이터 현황: 일봉 · 수급 · ETF · 월 자료 · 후보 · 1분봉 · 모의/실전 기록 기간', len(_inv['rows']) >= 20 and '1분봉' in _names
      and any(r['name'].startswith('일봉') and r['first'] and r['last'] and r['days'] > 1000 for r in _inv['rows'])
      and any(r['group'] == '모의 기록' and r['n'] for r in _inv['rows']), f"{len(_inv['rows'])}줄 · 파일 {list(_inv['files'])}")
_iq = cli.get('/api/intraday', headers={'X-TK-Token': SV.TOKEN}).json()
_st = cli.get('/api/state', headers={'X-TK-Token': SV.TOKEN}).json()
check('⏱ 장중 연구실 API · 상태에 장중 칸(기본 꺼짐)', _iq['ok'] and len(_iq['res']['rules']) == 4 and _st['cfg']['intraday_on'] is False
      and any(s_['key'] == 'IN' for s_ in _st['sleeves']) and 'passed' in _st['intraday'])
SV.CFG.update(krx_id='krxuser01', krx_pw='pw')
_kt = SV.krx_test()
_sc = cli.get('/api/state', headers={'X-TK-Token': SV.TOKEN}).json()['cfg']['secrets']
_js = json.dumps(_sc, ensure_ascii=False)
check('⚙️ 저장 확인: KRX · 텔레그램 저장 여부(가린 값) · KRX 연결 테스트 결과 · 비밀 값은 화면에 안 나감', _kt['n'] > 0 and _sc['krx']['pw'] and _sc['krx']['id'].endswith('01')
      and _sc['krx']['check']['ok'] and 'krxuser' not in _js and '"pw"' in _js and _sc['tg']['check'] is None, f"{_sc['krx']['id']} · {_sc['krx']['check']}")
_SVtc, _SVcf = SV.tr.client, SV.tr.configured
SV.tr.client = lambda c, m=None: kc
SV.tr.configured = lambda c, m=None: True
_bv = cli.get('/api/balance?force=1', headers={'X-TK-Token': SV.TOKEN}).json()
SV.tr.client, SV.tr.configured = _SVtc, _SVcf
_kq = {t_: q_ for t_, q_ in kc.pos.items() if q_ > 0}
check('💼 계좌 잔고: KIS 잔고 그대로(예수금 · 주문 가능 · 종목별 평단 · 평가 손익) + 앱 칸 표시 + 잔고 이력', _bv['src'] == 'kis' and len(_bv['rows']) == len(_kq)
      and all(r_['qty'] == _kq[r_['ticker']] for r_ in _bv['rows']) and any(r_['sleeves'] for r_ in _bv['rows']) and _bv['buyable'] is not None and len(_bv['hist']) > 10
      and abs(_bv['equity'] - kc.balance()['equity']) < 1, f"{len(_bv['rows'])}종목 · 평가 {_bv['equity']:,.0f} · 불일치 {[r_['name'] for r_ in _bv['rows'] if r_['diff']][:3]}")
check('보안: CORS 헤더 없음', 'access-control-allow-origin' not in {k.lower() for k in cli.get('/api/state', headers={'X-TK-Token': SV.TOKEN, 'Origin': 'http://evil.com'}).headers})

# ── 4. KIS 클라이언트 (가짜 KIS 서버) ──
seen = []
state = {'rate': 1, 'orders': 0}


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, obj, code=200, hdr=None):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header('content-type', 'application/json')
        for k, v in (hdr or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(b)

    def do_POST(self):
        ln = int(self.headers.get('content-length', 0))
        body = json.loads(self.rfile.read(ln) or b'{}')
        seen.append(('POST', self.path, self.headers.get('tr_id'), body))
        if self.path == '/oauth2/tokenP':
            return self._send({'access_token': 'TOK', 'expires_in': 86400})
        if self.path.endswith('order-cash'):
            state['orders'] += 1
            if body['PDNO'] == '999990':
                time.sleep(0.2)
                self.close_connection = True
                return                                                          # 응답 없이 끊김 → 불분명
            return self._send({'rt_cd': '0', 'msg1': '주문 전송 완료', 'output': {'ODNO': '0001', 'KRX_FWDG_ORD_ORGNO': '91252'}})
        self._send({'rt_cd': '1', 'msg1': '?'})

    def do_GET(self):
        seen.append(('GET', self.path, self.headers.get('tr_id'), self.headers.get('tr_cont')))
        if 'inquire-price' in self.path:
            if state['rate'] > 0:
                state['rate'] -= 1
                return self._send({'rt_cd': '1', 'msg_cd': 'EGW00201', 'msg1': '초당 거래건수를 초과하였습니다.'})
            return self._send({'rt_cd': '0', 'output': {'stck_prpr': '71500', 'prdy_ctrt': '1.2', 'stck_mxpr': '92900', 'stck_llam': '50100', 'hts_kor_isnm': '삼성전자'}})
        if 'inquire-balance' in self.path:
            page2 = self.headers.get('tr_cont') == 'N'
            out1 = [{'pdno': '005930' if not page2 else '000660', 'prdt_name': 'x', 'hldg_qty': '3', 'ord_psbl_qty': '3', 'pchs_avg_pric': '1', 'prpr': '2', 'evlu_amt': '6', 'evlu_pfls_amt': '3'}]
            return self._send({'rt_cd': '0', 'output1': out1, 'output2': [{'dnca_tot_amt': '1000', 'prvs_rcdl_excc_amt': '900', 'tot_evlu_amt': '1012'}],
                               'ctx_area_fk100': 'F' if not page2 else '', 'ctx_area_nk100': 'N' if not page2 else ''}, hdr={'tr_cont': 'M' if not page2 else 'D'})
        self._send({'rt_cd': '0', 'output': {}})


srv = HTTPServer(('127.0.0.1', 18765), H)
threading.Thread(target=srv.serve_forever, daemon=True).start()
from tk_kis import KIS, KISError, ENV
ENV['paper'] = {**ENV['paper'], 'rest': 'http://127.0.0.1:18765'}
k = KIS('paper', 'APPKEY1234567890', 'SECRET', '12345678-01', db.DATA_DIR)
p = k.price('005930')
check('KIS: EGW00201 한도 초과 뒤 재시도로 성공', p['price'] == 71500 and sum(1 for s_ in seen if 'inquire-price' in s_[1]) == 2)
check('KIS: 시세 TR은 모의에서도 그대로', [s_[2] for s_ in seen if 'inquire-price' in s_[1]][0] == 'FHKST01010100')
b = k.balance()
check('KIS: 잔고 연속조회 2쪽 · 모의 TR VTTC8434R', len(b['positions']) == 2 and [s_[2] for s_ in seen if 'inquire-balance' in s_[1]][0] == 'VTTC8434R'
      and [s_[3] for s_ in seen if 'inquire-balance' in s_[1]][1] == 'N', f"{[p_['ticker'] for p_ in b['positions']]} · D+2 {b['cash_d2']}")
o = k.order('buy', '005930', 1)
check('KIS: 모의 매수 TR VTTC0012U · 거래소 KRX', [s_[2] for s_ in seen if s_[1].endswith('order-cash')][0] == 'VTTC0012U'
      and [s_[3] for s_ in seen if s_[1].endswith('order-cash')][0]['EXCG_ID_DVSN_CD'] == 'KRX' and o['order_no'] == '0001')
try:
    k.order('sell', '999990', 1)
    check('KIS: 응답 없는 주문 → AMBIGUOUS', False)
except KISError as e:
    check('KIS: 응답 없는 주문 → AMBIGUOUS (재주문 안 함)', 'AMBIGUOUS' in str(e) and state['orders'] == 2, str(e)[:60])
check('KIS: 매도 TR VTTC0011U', [s_[2] for s_ in seen if s_[1].endswith('order-cash')][-1] == 'VTTC0011U')
toks = sum(1 for s_ in seen if s_[1] == '/oauth2/tokenP')
k2 = KIS('paper', 'APPKEY1234567890', 'SECRET', '12345678-01', db.DATA_DIR)
k2.token()
check('KIS: 토큰 파일 재사용 (다시 발급 안 함)', sum(1 for s_ in seen if s_[1] == '/oauth2/tokenP') == toks == 1)
kr = KIS('real', 'APPKEY1234567890', 'SECRET', '12345678-01', db.DATA_DIR)
check('KIS: 실전 TR은 그대로 · 모의는 V', kr.tr('TTTC0012U') == 'TTTC0012U' and k.tr('TTTC0012U') == 'VTTC0012U' and k.tr('FHKST01010100') == 'FHKST01010100'
      and kr.tr('CTCA0903R') == 'CTCA0903R' and k.tr('CTSC9215R') == 'VTSC9215R')
t1 = time.monotonic()
for _ in range(4):
    k._throttle()
check('KIS: 모의 호출 간격 0.55초 (4회 = 간격 3번)', time.monotonic() - t1 >= 0.55 * 3 - 0.02, f'{time.monotonic() - t1:.2f}초/4회')

# ── 5. 체결 통보 · 마스터 ──
import tk_ws as W
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
key, iv = 'k' * 32, 'i' * 16
fields = ['CUST', '1234567801', '0000123', '', '02', '0', '00', '0', '005930', '10', '71500', '093015', 'N', '2', 'Y', '', '10', '', '', '1', '', '', '', '', '삼성전자', '0']
ct = base64.b64encode(AES.new(key.encode(), AES.MODE_CBC, iv.encode()).encrypt(pad('^'.join(fields).encode(), 16))).decode()
W._keys['H0STCNI9'] = (key, iv)
kind, dd = W.parse('1|H0STCNI9|001|' + ct)
check('체결 통보 AES 해독', kind == 'notice' and dd['STCK_SHRN_ISCD'] == '005930' and dd['CNTG_QTY'] == '10' and dd['CNTG_YN'] == '2')
kind, pr = W.parse('0|H0STCNT0|001|' + '^'.join(['005930', '093015', '71600', '2', '100', '0.14'] + ['0'] * 40))
check('실시간 체결가 해석', kind == 'price' and pr[0][:2] == ('005930', 71600.0) and pr[0][2] == 0.14)
kind, ex = W.parse('0|H0STANC0|002|' + '^'.join(['005930', '084830', '75000', '2', '3500', '4.90'] + ['0'] * 39 + ['000660', '084830', '210000', '2', '15000', '7.69'] + ['0'] * 39))
for t_, px_, chg_ in ex:
    W.EXP[t_] = (px_, time.time(), chg_)
_tr_exp = tr.EXP_GAP[0]
tr.EXP_GAP[0] = W.exp_gap
class _NoRest:
    def expected(self, t): raise AssertionError('REST 호출하면 안 됨')
_ga = tr.gap_check({}, _NoRest(), {'sleeve': 'LVH', 'ticker': '000660', 'name': 'B'}, 'pre')
_gb = tr.gap_check({}, _NoRest(), {'sleeve': 'LVH', 'ticker': '005930', 'name': 'A'}, 'pre')
tr.EXP_GAP[0] = _tr_exp
check('📡 장전 예상체결 웹소켓(H0STANC0) 해석 · 갭 판단에 사용 (REST 호출 없음)', kind == 'exp' and len(ex) == 2 and ex[1] == ('000660', 210000.0, 7.69) and _ga and not _gb, f'{ex} · {_ga}')


def mst_line(tk, name, tail, sets):
    rest = [' '] * tail
    for pos, val in sets:
        for j, ch in enumerate(val):
            rest[pos + j] = ch
    return tk.ljust(9) + 'KR7000000000'[:12] + name + ''.join(rest)


# kospi: widths 앞 31개(2,1,4,4,4 + 1×26)=41 → 기준가(9) 41~49 · 수량단위 50~54 · 55~59 · 거래정지 60 · 정리매매 61 · 관리 62 · 경고 63~64 · … 우선주
kospi = [mst_line('005930', '삼성전자', 228, []), mst_line('000020', '정지회사', 228, [(60, 'Y')]), mst_line('000030', '관리회사', 228, [(62, 'Y')]),
         mst_line('000040', '경고회사', 228, [(63, '02')])]
kosdaq = [mst_line('100010', '코스닥사', 222, [(55, 'Y')])]
import urllib.request as UR
real_open = UR.urlopen


class _R:
    def __init__(self, b):
        self.b = b

    def read(self):
        return self.b


def fake_urlopen(req, timeout=0):
    mk = 'kospi' if 'kospi' in req.full_url else 'kosdaq'
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, 'w') as z:
        z.writestr(f'{mk}_code.mst', '\n'.join(kospi if mk == 'kospi' else kosdaq).encode('cp949'))
    return _R(bio.getvalue())


col.urllib.request.urlopen = fake_urlopen
col.kis_master()
st = db.stocks()
check('종목 마스터: 거래정지 · 관리 · 경고 표시', st['000020']['halt'] == 1 and st['000030']['admin'] == 1 and st['000040']['warn'] == '02'
      and st['005930']['halt'] == 0 and st['100010']['halt'] == 1)
print(f"\n{sum(OK)}/{len(OK)} 통과")
sys.exit(0 if all(OK) else 1)
