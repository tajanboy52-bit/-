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
kc = FM.FakeKIS(D['P'], D['on'])
clock = {'d': days[-n - 1], 'hm': '19:00'}
tr.today = lambda: clock['d']
import datetime as _dt
tr.now = lambda: _dt.datetime.strptime(clock['d'] + clock['hm'], '%Y%m%d%H:%M')
kc.now = lambda: clock['hm']
tr.prev_trading_day = lambda d: days[days.index(d) - 1]
tr.is_trading_day = lambda d=None: True
cfg.update(kis_on=True, cap=10_000_000)
cfg['accounts']['paper'] = {'app_key': 'x' * 20, 'app_secret': 'y' * 20, 'account': '12345678-01'}
tr.signal_job(cfg, clock['d'])
for d in run_days:
    clock['d'] = d
    kc.d = d
    clock['hm'] = '08:35'
    tr.preopen(cfg, kc, d)
    kc.settle('open')
    clock['hm'] = '09:02'
    tr.deferred(cfg, kc, d)
    kc.settle('open')
    tr.sync(kc, d)
    clock['hm'] = '10:00'
    tr.intraday(cfg, kc, d, {})
    clock['hm'] = '15:20'
    tr.on_buy(cfg, kc, d)
    kc.settle('close')
    clock['hm'] = '15:45'
    tr.eod(cfg, kc, d)
    clock['hm'] = '19:00'
    tr.signal_job(cfg, d)
x = db.conn()
live = {(r['sleeve'], r['ticker'], r['entry_date']) for r in x.execute("SELECT * FROM lots WHERE sleeve IN ('LVH','REV') AND entry_date IS NOT NULL")}
live_x = {(r['sleeve'], r['ticker'], r['entry_date'], r['exit_date']) for r in x.execute("SELECT * FROM lots WHERE sleeve IN ('LVH','REV') AND status='청산'")}
res = B.simulate(D, days[-n - 1], run_days[-1], {'LVH': .40, 'REV': .25, 'DV': .20, 'ON': .15})
bt = {(t[0], t[1], t[2]) for t in res['trades'] if t[0] in ('LVH', 'REV')} | {(l['s'], l['t'], l['d']) for l in res['open_lots'] if l['s'] in ('LVH', 'REV')}
bt_x = {(t[0], t[1], t[2], t[3]) for t in res['trades'] if t[0] in ('LVH', 'REV')}
check('실전 ↔ 백테스트 매수 일치', live == bt and len(live) > 50, f'{len(live & bt)}/{len(live | bt)}')
check('실전 ↔ 백테스트 청산 일치', live_x == bt_x and len(live_x) > 20, f'{len(live_x & bt_x)}/{len(live_x | bt_x)}')
dv = x.execute("SELECT COUNT(*) FROM lots WHERE sleeve='DV' AND status='보유'").fetchone()[0]
on = x.execute("SELECT COUNT(*), AVG(ret) FROM lots WHERE sleeve='ON' AND status='청산'").fetchone()
check('배당·가치 15자리 · 밤사이 매일', dv >= 12 and on[0] >= n - 2, f'DV {dv} · ON {on[0]}건 평균 {on[1]:+.3f}%')
bad = db.mconn().execute("SELECT COUNT(*) FROM log WHERE msg LIKE '%불일치%'").fetchone()[0]
check('잔고 불일치 0', bad == 0)
check('장중 평가 기록 (최근 10일만 보관)', 5 <= x.execute('SELECT COUNT(*) FROM intraday').fetchone()[0] <= 12, f"{x.execute('SELECT COUNT(*) FROM intraday').fetchone()[0]}건")
g = tr.gate(cfg)
check('판정: 40일이라 60일 기준 미달', not g['pass'] and not g['rows'][0]['ok'], g['rows'][0]['v'])
# ── 2-1. 거래 기록 (HTS급) ──
import tk_journal as J
o_all = [dict(r) for r in x.execute('SELECT * FROM orders')]
fq = {r[0]: r[1] for r in x.execute('SELECT order_id, SUM(qty) FROM fills GROUP BY order_id')}
check('기록: 체결 조각 합 = 주문 체결 수량', all(fq.get(o['id'], 0) == (o['filled'] or 0) for o in o_all) and len(fq) > 100, f'{len(fq)}건 주문 · 체결 조각 {sum(fq.values()):,}주')
ev = {}
for r in x.execute('SELECT order_id, status FROM order_events'):
    ev.setdefault(r[0], []).append(r[1])
check('기록: 주문마다 상태 이력 (보냄 → 접수 → 체결)', all(ev.get(o['id'], [None])[0] == '보냄' for o in o_all)
      and all('접수' in ev[o['id']] and '체결' in ev[o['id']] for o in o_all if o['status'] == '체결'), f'이벤트 {sum(len(v) for v in ev.values())}건')
cl = [dict(r) for r in x.execute("SELECT * FROM lots WHERE status='청산'")]
okp = all(abs(l['pnl'] - (l['proceeds'] - l['cost'] - l['fee'] - l['tax'])) < 1 for l in cl)
tax_ok = all((l['tax'] > 0) == (l['sleeve'] != 'ON') for l in cl)
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
    x.execute('SELECT id FROM lots').fetchall()) and tr.client(cfg).cano == '12345678' and tr.cap(cfg) == 10_000_000)

# ── 2-3. 분석 ──
import tk_analyze as A
res = A.analyze()
check('분석: 칸별 · 청산 이유 · 지표 구간 · 체결 품질 · 후보 순위 · 놓친 거래',
      res['summary'] and res['by_sleeve'] and res['by_exit'] and res['buckets'] and res['execution'] and res['cands'] is not None and 'missed' in res,
      f"거래 {res['summary'][0]['n'] if res['summary'] else 0} · 구간표 {len(res['buckets'])} · 제안 {len(res['ideas'])} · "
      + str({k: bool(res[k]) if k != 'cands' else res[k] is not None for k in ('summary', 'by_sleeve', 'by_exit', 'buckets', 'execution', 'cands', 'missed')}))
md = A.report_md(res)
check('분석: 보고서 · 패키지(zip · 비밀 값 없음)', '# TK자동매매 거래 분석' in md and '12345678' not in json.dumps(res, ensure_ascii=False, default=str))

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
