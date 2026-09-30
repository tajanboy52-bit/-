"""
tk_kis.py — TK자동매매 한국투자증권(KIS) Open API 클라이언트 · 실전 / 모의 공용

공식 샘플(koreainvestment/open-trading-api · kis_auth.py · domestic_stock_functions.py)에서 확인한 규칙대로 만듦
· 도메인   실전 https://openapi.koreainvestment.com:9443        웹소켓 ws://ops.koreainvestment.com:21000
           모의 https://openapivts.koreainvestment.com:29443    웹소켓 ws://ops.koreainvestment.com:31000
· TR ID    실전 TR의 첫 글자 T · J · C를 V로 바꾸면 모의 TR (예: 매수 TTTC0012U → VTTC0012U)
           현금 주문: 매도 TTTC0011U · 매수 TTTC0012U (예전 0801U · 0802U는 구버전) · 정정/취소 TTTC0013U
           잔고 TTTC8434R · 일별 체결 TTTC0081R(3개월 이내) · 매수가능 TTTC8908R(nrcvb_buy_amt = 미수 없는 매수금액)
           시세(FHK…)는 실전 · 모의 같은 TR · 휴장일 CTCA0903R은 실전 전용이고 하루 1번만 부르라고 공식 안내
· 호출 한도 공식 샘플의 대기값: 실전 0.05초(초당 20건) · 모의 0.5초(초당 2건) → 여유를 두고 실전 0.06 · 모의 0.55초
           한도 초과 응답 EGW00201 → 잠깐 쉬고 다시 (조회만 · 주문은 절대 자동 재시도 안 함)
· 토큰     유효 1일 · 6시간 안에 다시 받으면 같은 토큰 · 받을 때마다 카카오 알림톡 → 파일에 저장해 재사용, 1분에 1번까지만 발급
· 주문 POST가 네트워크 오류로 결과를 모르면 ORDER_SUBMISSION_AMBIGUOUS (재주문하지 않고 호출한 쪽이 정지 처리)
"""
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

ENV = {
    'real': {'rest': 'https://openapi.koreainvestment.com:9443', 'ws': 'ws://ops.koreainvestment.com:21000', 'gap': 0.06, 'notice': 'H0STCNI0'},
    'paper': {'rest': 'https://openapivts.koreainvestment.com:29443', 'ws': 'ws://ops.koreainvestment.com:31000', 'gap': 0.55, 'notice': 'H0STCNI9'},
}
_PACE = {}                       # {(env, 앱키 끝 6자리): [lock, 마지막 호출 시각]} — 같은 앱키는 프로세스 전체가 한 줄로
_PACE_LOCK = threading.Lock()
_APPROVAL = {}
HOOK = [None]                    # 호출 통계 (tk_journal.api_hit) — (env, tr_id, ms, 오류)


def _hit(env, tr, t0, err=''):
    if HOOK[0]:
        try:
            HOOK[0](env, tr, (time.monotonic() - t0) * 1000, err)
        except Exception:
            pass


class KISError(RuntimeError):
    def __init__(self, msg, code=''):
        super().__init__(msg)
        self.code = code


def _num(v, default=0.0):
    try:
        return float(str(v).replace(',', ''))
    except Exception:
        return default


def split_account(raw, product_default='01'):
    s = ''.join(ch for ch in str(raw or '') if ch.isdigit())
    if len(s) >= 10:
        return s[:8], s[8:10]
    if len(s) == 8:
        return s, product_default
    raise KISError('계좌번호는 8자리 또는 8-2 형태로 입력하세요')


def tick_size(p):
    """KRX 호가 단위 (2023-01 통합 호가)"""
    for lim, t in ((2000, 1), (5000, 5), (20000, 10), (50000, 50), (200000, 100), (500000, 500)):
        if p < lim:
            return t
    return 1000


def tick_round(p, mode='down'):
    t = tick_size(p)
    return int((p // t) * t) if mode == 'down' else int(-(-p // t) * t)


class KIS:
    def __init__(self, env, appkey, appsecret, account, data_dir, hts_id=''):
        if env not in ENV:
            raise KISError(f'모드는 real 또는 paper ({env})')
        self.env = env
        self.appkey, self.appsecret = (appkey or '').strip(), (appsecret or '').strip()
        if not (self.appkey and self.appsecret):
            raise KISError(f"{'실전' if env == 'real' else '모의'} 앱키 · 시크릿을 설정에 입력하세요")
        self.cano, self.product = split_account(account) if account else ('', '01')
        self.base = ENV[env]['rest']
        self.ws_url = ENV[env]['ws']
        self.notice_tr = ENV[env]['notice']
        self.hts_id = (hts_id or '').strip()
        self.token_file = os.path.join(data_dir, f'kis_token_{env}_{self.appkey[-6:]}.json')
        self._token, self._expires = None, 0.0
        key = (env, self.appkey[-6:])
        with _PACE_LOCK:
            self._pace = _PACE.setdefault(key, [threading.Lock(), 0.0])
        self.calls = 0

    @property
    def is_paper(self):
        return self.env == 'paper'

    @property
    def masked_account(self):
        return (self.cano[:2] + '****' + self.cano[-2:] + '-' + self.product) if self.cano else '(계좌 없음)'

    def tr(self, tr_id):
        return 'V' + tr_id[1:] if self.is_paper and tr_id[0] in 'TJC' else tr_id

    # ── 저수준 ──
    def _throttle(self):
        lock, _ = self._pace
        with lock:
            w = ENV[self.env]['gap'] - (time.monotonic() - self._pace[1])
            if w > 0:
                time.sleep(w)
            self._pace[1] = time.monotonic()
        self.calls += 1

    def _http(self, method, path, headers, params=None, body=None, timeout=20):
        url = self.base + path + ('?' + urllib.parse.urlencode(params) if params else '')
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode('utf-8')), {k.lower(): v for k, v in r.headers.items()}

    def token(self, force=False):
        now = time.time()
        if not force and self._token and now < self._expires - 600:
            return self._token
        try:
            j = json.load(open(self.token_file, encoding='utf-8'))
            if not force and now < float(j.get('expires_at', 0)) - 600:
                self._token, self._expires = j['access_token'], float(j['expires_at'])
                return self._token
            last = float(j.get('issued_at', 0))
            if now - last < 61:                                      # 발급은 1분에 1번
                time.sleep(61 - (now - last))
        except Exception:
            pass
        self._throttle()
        try:
            j, _ = self._http('POST', '/oauth2/tokenP', {'content-type': 'application/json'},
                              body={'grant_type': 'client_credentials', 'appkey': self.appkey, 'appsecret': self.appsecret})
        except urllib.error.HTTPError as e:
            raise KISError(f'토큰 발급 실패 HTTP {e.code} — 앱키 · 시크릿 · 실전/모의 구분 확인')
        except Exception as e:
            raise KISError(f'토큰 발급 실패: {str(e)[:150]}')
        tok = j.get('access_token')
        if not tok:
            raise KISError(f"토큰 발급 실패: {j.get('error_description') or j.get('msg1') or '응답 없음'}")
        self._token, self._expires = tok, time.time() + int(j.get('expires_in', 86400))
        try:
            json.dump({'access_token': tok, 'expires_at': self._expires, 'issued_at': time.time()}, open(self.token_file, 'w', encoding='utf-8'))
        except Exception:
            pass
        return tok

    def _headers(self, tr_id, tr_cont=''):
        h = {'content-type': 'application/json; charset=utf-8', 'authorization': f'Bearer {self.token()}',
             'appkey': self.appkey, 'appsecret': self.appsecret, 'tr_id': self.tr(tr_id), 'custtype': 'P'}
        if tr_cont:
            h['tr_cont'] = tr_cont
        return h

    def get(self, path, tr_id, params, tr_cont='', retry=4):
        """조회 — 한도 초과 · 네트워크 · 토큰 만료는 다시 시도"""
        last = None
        for k in range(retry):
            try:
                self._throttle()
                t0 = time.monotonic()
                try:
                    j, h = self._http('GET', path, self._headers(tr_id, tr_cont), params=params)
                except Exception as e:
                    _hit(self.env, tr_id, t0, str(e)[:100] or 'error')
                    raise
                ok = str(j.get('rt_cd', '0')) == '0'
                _hit(self.env, tr_id, t0, '' if ok else f"{j.get('msg_cd', '')} {j.get('msg1', '')}")
                if ok:
                    return j, h
                code, msg = j.get('msg_cd', ''), j.get('msg1', '')
                last = KISError(f'{code} {msg}'.strip(), code)
                if code == 'EGW00201':                                   # 초당 한도 초과
                    time.sleep(1.0 + k)
                    continue
                if code in ('EGW00123', 'EGW00121') or 'token' in msg.lower():
                    self.token(force=True)
                    continue
                raise last
            except urllib.error.HTTPError as e:
                last = KISError(f'HTTP {e.code}', str(e.code))
                if e.code in (401, 403):
                    self._token = None
                    try:
                        self.token(force=True)
                    except KISError:
                        pass
                elif e.code == 500 and k >= 1:
                    time.sleep(1.0)
            except KISError:
                raise
            except Exception as e:
                last = KISError(f'연결 오류: {str(e)[:150]}')
            time.sleep(0.6 * (k + 1))
        raise last

    def post_order(self, path, tr_id, body):
        """주문 — 한 번만. 거절은 KISError, 전송 결과를 모르면 ORDER_SUBMISSION_AMBIGUOUS"""
        try:
            headers = self._headers(tr_id)                              # 토큰 실패는 주문이 안 나간 것
        except KISError:
            raise
        except Exception as e:
            raise KISError(f'주문 전 준비 실패(주문 안 나감): {str(e)[:150]}')
        self._throttle()
        t0 = time.monotonic()
        try:
            j, _ = self._http('POST', path, headers, body=body, timeout=25)
            _hit(self.env, tr_id, t0, '' if str(j.get('rt_cd', '0')) == '0' else f"{j.get('msg_cd', '')} {j.get('msg1', '')}")
        except urllib.error.HTTPError as e:
            _hit(self.env, tr_id, t0, f'HTTP {e.code}')
            if e.code in (400, 401, 403, 404, 429):
                raise KISError(f'주문 거절 HTTP {e.code}', str(e.code))
            raise KISError(f'ORDER_SUBMISSION_AMBIGUOUS: HTTP {e.code}')
        except Exception as e:
            _hit(self.env, tr_id, t0, 'AMBIGUOUS')
            raise KISError(f'ORDER_SUBMISSION_AMBIGUOUS: {str(e)[:150]}')
        if str(j.get('rt_cd', '0')) != '0':
            raise KISError(f"주문 거절: {j.get('msg_cd', '')} {j.get('msg1', '')}".strip(), j.get('msg_cd', ''))
        return j

    def _paged(self, path, tr_id, params, key1='output1', key2='output2', fk='CTX_AREA_FK100', nk='CTX_AREA_NK100', pages=20):
        a1, a2, cont = [], [], ''
        for _ in range(pages):
            j, h = self.get(path, tr_id, params, tr_cont=cont)
            o1, o2 = j.get(key1) or [], j.get(key2) or []
            a1 += o1 if isinstance(o1, list) else [o1]
            a2 += o2 if isinstance(o2, list) else [o2]
            f_, n_, c = j.get(fk.lower(), ''), j.get(nk.lower(), ''), h.get('tr_cont', '')
            if c not in ('M', 'F') or not (f_ or n_):
                break
            params[fk], params[nk], cont = f_, n_, 'N'
        return a1, a2

    # ── 주문 ──
    def order(self, side, ticker, qty, ord_dvsn='01', price=0):
        """side buy/sell · ord_dvsn 00 지정가 · 01 시장가 · 05 장전 시간외 · 06 장후 시간외 · 07 시간외 단일가"""
        qty = int(qty)
        if qty <= 0:
            raise KISError('주문 수량 0')
        if not self.cano:
            raise KISError('계좌번호가 없습니다')
        body = {'CANO': self.cano, 'ACNT_PRDT_CD': self.product, 'PDNO': str(ticker).zfill(6), 'ORD_DVSN': str(ord_dvsn),
                'ORD_QTY': str(qty), 'ORD_UNPR': str(int(price or 0)), 'EXCG_ID_DVSN_CD': 'KRX',
                'SLL_TYPE': '01' if side == 'sell' else '', 'CNDT_PRIC': ''}
        j = self.post_order('/uapi/domestic-stock/v1/trading/order-cash', 'TTTC0012U' if side == 'buy' else 'TTTC0011U', body)
        o = j.get('output') or {}
        return {'order_no': str(o.get('ODNO') or o.get('odno') or ''), 'org_no': str(o.get('KRX_FWDG_ORD_ORGNO') or o.get('krx_fwdg_ord_orgno') or ''),
                'time': str(o.get('ORD_TMD') or o.get('ord_tmd') or ''), 'msg': j.get('msg1', ''), 'msg_cd': j.get('msg_cd', '')}

    def cancel(self, order_no, org_no=''):
        body = {'CANO': self.cano, 'ACNT_PRDT_CD': self.product, 'KRX_FWDG_ORD_ORGNO': str(org_no or ''), 'ORGN_ODNO': str(order_no),
                'ORD_DVSN': '00', 'RVSE_CNCL_DVSN_CD': '02', 'ORD_QTY': '0', 'ORD_UNPR': '0', 'QTY_ALL_ORD_YN': 'Y', 'EXCG_ID_DVSN_CD': 'KRX'}
        return self.post_order('/uapi/domestic-stock/v1/trading/order-rvsecncl', 'TTTC0013U', body)

    # ── 계좌 ──
    def balance(self):
        p = {'CANO': self.cano, 'ACNT_PRDT_CD': self.product, 'AFHR_FLPR_YN': 'N', 'OFL_YN': '', 'INQR_DVSN': '02', 'UNPR_DVSN': '01',
             'FUND_STTL_ICLD_YN': 'N', 'FNCG_AMT_AUTO_RDPT_YN': 'N', 'PRCS_DVSN': '00', 'CTX_AREA_FK100': '', 'CTX_AREA_NK100': ''}
        a1, a2 = self._paged('/uapi/domestic-stock/v1/trading/inquire-balance', 'TTTC8434R', p)
        pos = []
        for r in a1:
            q = int(_num(r.get('hldg_qty')))
            if q > 0:
                pos.append({'ticker': str(r.get('pdno', '')).zfill(6), 'name': r.get('prdt_name', ''), 'qty': q, 'sellable': int(_num(r.get('ord_psbl_qty'))),
                            'avg': _num(r.get('pchs_avg_pric')), 'price': _num(r.get('prpr')), 'value': _num(r.get('evlu_amt')),
                            'pnl': _num(r.get('evlu_pfls_amt'))})
        s = a2[0] if a2 else {}
        cash = _num(s.get('dnca_tot_amt'))
        d2 = _num(s.get('prvs_rcdl_excc_amt')) or cash
        eq = _num(s.get('tot_evlu_amt')) or (d2 + sum(p['value'] for p in pos))
        return {'positions': pos, 'cash': cash, 'cash_d2': d2, 'equity': eq}

    def buyable(self, ticker='005930', price=0):
        """미수 없이 살 수 있는 금액 (시장가 기준 · 종목 증거금률 반영)"""
        j, _ = self.get('/uapi/domestic-stock/v1/trading/inquire-psbl-order', 'TTTC8908R',
                        {'CANO': self.cano, 'ACNT_PRDT_CD': self.product, 'PDNO': str(ticker).zfill(6), 'ORD_UNPR': str(int(price or 0)),
                         'ORD_DVSN': '01', 'CMA_EVLU_AMT_ICLD_YN': 'N', 'OVRS_ICLD_YN': 'N'})
        o = j.get('output') or {}
        return {'cash': _num(o.get('ord_psbl_cash')), 'nrcvb': _num(o.get('nrcvb_buy_amt')), 'qty': int(_num(o.get('nrcvb_buy_qty')))}

    def fills(self, day):
        """그날 주문 · 체결 → [{order_no, ticker, side, qty, filled, avg, remain, cancelled, time}]"""
        p = {'CANO': self.cano, 'ACNT_PRDT_CD': self.product, 'INQR_STRT_DT': day, 'INQR_END_DT': day, 'SLL_BUY_DVSN_CD': '00',
             'PDNO': '', 'CCLD_DVSN': '00', 'INQR_DVSN': '00', 'INQR_DVSN_3': '00', 'ORD_GNO_BRNO': '', 'ODNO': '',
             'INQR_DVSN_1': '', 'CTX_AREA_FK100': '', 'CTX_AREA_NK100': '', 'EXCG_ID_DVSN_CD': 'KRX'}
        a1, _ = self._paged('/uapi/domestic-stock/v1/trading/inquire-daily-ccld', 'TTTC0081R', p)
        return [{'order_no': str(r.get('odno', '')), 'ticker': str(r.get('pdno', '')).zfill(6), 'name': r.get('prdt_name', ''),
                 'side': 'sell' if str(r.get('sll_buy_dvsn_cd', '')) == '01' else 'buy', 'qty': int(_num(r.get('ord_qty'))),
                 'filled': int(_num(r.get('tot_ccld_qty'))), 'avg': _num(r.get('avg_prvs')), 'remain': int(_num(r.get('rmn_qty'))),
                 'cancelled': str(r.get('cncl_yn', 'N')) == 'Y', 'time': str(r.get('ord_tmd', '')), 'amt': _num(r.get('tot_ccld_amt')),
                 'rejected': int(_num(r.get('rjct_qty'))), 'ord_price': _num(r.get('ord_unpr')), 'ord_kind': r.get('ord_dvsn_name', '')}
                for r in a1 if r.get('odno')]

    def trade_profit(self, frm, to):
        """기간별 매매손익 TTTC8715R (HTS 0856 · 실전 전용) — 종목 · 날짜별 실제 수수료 · 세금 · 실현손익 · 모의면 None"""
        if self.is_paper:
            return None
        p = {'CANO': self.cano, 'ACNT_PRDT_CD': self.product, 'SORT_DVSN': '01', 'INQR_STRT_DT': frm, 'INQR_END_DT': to, 'CBLC_DVSN': '00',
             'PDNO': '', 'CTX_AREA_FK100': '', 'CTX_AREA_NK100': ''}
        a1, _ = self._paged('/uapi/domestic-stock/v1/trading/inquire-period-trade-profit', 'TTTC8715R', p)
        return [{'date': r.get('trad_dt', ''), 'ticker': str(r.get('pdno', '')).zfill(6), 'name': r.get('prdt_name', ''),
                 'kind': r.get('trad_dvsn_name', ''), 'buy_qty': _num(r.get('buy_qty')), 'buy_amt': _num(r.get('buy_amt')),
                 'sell_qty': _num(r.get('sll_qty')), 'sell_amt': _num(r.get('sll_amt')), 'pnl': _num(r.get('rlzt_pfls')),
                 'fee': _num(r.get('fee')), 'tax': _num(r.get('tl_tax'))} for r in a1 if r.get('pdno')]

    # ── 시세 ──
    def price(self, ticker):
        j, _ = self.get('/uapi/domestic-stock/v1/quotations/inquire-price', 'FHKST01010100',
                        {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': str(ticker).zfill(6)})
        o = j.get('output') or {}
        return {'price': _num(o.get('stck_prpr')), 'open': _num(o.get('stck_oprc')), 'high': _num(o.get('stck_hgpr')), 'low': _num(o.get('stck_lwpr')),
                'upper': _num(o.get('stck_mxpr')), 'lower': _num(o.get('stck_llam')), 'chg': _num(o.get('prdy_ctrt')), 'vol': _num(o.get('acml_vol')),
                'value': _num(o.get('acml_tr_pbmn')), 'halt': str(o.get('temp_stop_yn', 'N')) == 'Y', 'vi': str(o.get('vi_cls_code', 'N')) not in ('N', '', '0'),
                'status': str(o.get('iscd_stat_cls_code', '')), 'raw': o}

    def daily(self, ticker, frm, to, adjusted=True):
        """일봉 (한 번에 최대 100개) · 기간이 길면 나눠서 → [{date, open, high, low, close, volume, value}] 오래된 것부터"""
        out, end = {}, to
        for _ in range(40):
            j, _ = self.get('/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice', 'FHKST03010100',
                            {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': str(ticker).zfill(6), 'FID_INPUT_DATE_1': frm,
                             'FID_INPUT_DATE_2': end, 'FID_PERIOD_DIV_CODE': 'D', 'FID_ORG_ADJ_PRC': '0' if adjusted else '1'})
            rows = [r for r in (j.get('output2') or []) if r.get('stck_bsop_date')]
            for r in rows:
                d = r['stck_bsop_date']
                if frm <= d <= to and _num(r.get('stck_clpr')) > 0:
                    out[d] = {'date': d, 'open': _num(r.get('stck_oprc')), 'high': _num(r.get('stck_hgpr')), 'low': _num(r.get('stck_lwpr')),
                              'close': _num(r.get('stck_clpr')), 'volume': _num(r.get('acml_vol')), 'value': _num(r.get('acml_tr_pbmn'))}
            if len(rows) < 100:
                break
            first = min(r['stck_bsop_date'] for r in rows)
            if first <= frm:
                break
            import datetime as _dt
            end = (_dt.datetime.strptime(first, '%Y%m%d') - _dt.timedelta(days=1)).strftime('%Y%m%d')
        return [out[d] for d in sorted(out)]

    def investor(self, ticker):
        """최근 약 30일 외국인 · 기관 순매수 (거래대금 백만원) → [{date, foreign, inst}]"""
        j, _ = self.get('/uapi/domestic-stock/v1/quotations/inquire-investor', 'FHKST01010900',
                        {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': str(ticker).zfill(6)})
        return [{'date': r.get('stck_bsop_date'), 'foreign': _num(r.get('frgn_ntby_tr_pbmn')) * 1e6, 'inst': _num(r.get('orgn_ntby_tr_pbmn')) * 1e6}
                for r in (j.get('output') or []) if r.get('stck_bsop_date')]

    def is_open_day(self, day):
        """휴장일 조회 CTCA0903R — 실전 전용 · 하루 1번만 (공식 안내) · 모의면 None"""
        if self.is_paper:
            return None
        j, _ = self.get('/uapi/domestic-stock/v1/quotations/chk-holiday', 'CTCA0903R', {'BASS_DT': day, 'CTX_AREA_NK': '', 'CTX_AREA_FK': ''})
        for r in j.get('output') or []:
            if r.get('bass_dt') == day:
                return r.get('opnd_yn') == 'Y'
        return None

    def approval_key(self):
        k = _APPROVAL.get((self.env, self.appkey[-6:]))
        if k and time.time() - k[1] < 12 * 3600:
            return k[0]
        self._throttle()
        try:
            j, _ = self._http('POST', '/oauth2/Approval', {'content-type': 'application/json; charset=utf-8'},
                              body={'grant_type': 'client_credentials', 'appkey': self.appkey, 'secretkey': self.appsecret})
        except Exception as e:
            raise KISError(f'웹소켓 접속키 발급 실패: {str(e)[:150]}')
        key = j.get('approval_key')
        if not key:
            raise KISError('웹소켓 접속키 발급 실패: 응답 없음')
        _APPROVAL[(self.env, self.appkey[-6:])] = (key, time.time())
        return key
