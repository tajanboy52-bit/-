"""
q_kis.py — TK Quant KIS 모의투자 REST 클라이언트 (국내주식 · 모의투자 도메인 전용, 실전 주소 없음 · 우량주 앱 B1.2 클라이언트와 같음)
차트매매 앱(SWING_CHART_APP/src/kis_broker_client.py)의 검증된 호출 방식을 표준 라이브러리(urllib)로 옮김
TR: 매수 VTTC0012U · 매도 VTTC0011U · 정정/취소 VTTC0013U · 잔고 VTTC8434R · 일별 체결 VTTC0081R · 현재가 FHKST01010100
주문 POST가 네트워크 오류로 결과가 불분명하면 절대 재시도하지 않고 ORDER_SUBMISSION_AMBIGUOUS 오류를 올림
"""
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

PAPER_BASE = 'https://openapivts.koreainvestment.com:29443'
_APPROVAL = {}                                  # 웹소켓 접속키 캐시 {앱키 끝 6자리: (키, 발급 시각)}


class KISError(RuntimeError):
    pass


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
    raise KISError('모의계좌번호는 8자리 또는 8-2 형태로 입력하세요')


def tick_up(p):
    """KRX 호가 단위로 올림 (2023~ 통합 호가)"""
    p = float(p)
    for lim, t in ((2000, 1), (5000, 5), (20000, 10), (50000, 50), (200000, 100), (500000, 500)):
        if p < lim:
            return int(-(-p // t) * t)
    return int(-(-p // 1000) * 1000)


class KISPaper:
    def __init__(self, appkey, appsecret, account, token_file):
        self.appkey, self.appsecret = (appkey or '').strip(), (appsecret or '').strip()
        if not (self.appkey and self.appsecret and account):
            raise KISError('모의투자 앱키 · 시크릿 · 계좌번호를 설정에 입력하세요')
        self.cano, self.product = split_account(account)
        self.base = PAPER_BASE
        self.token_file = token_file
        self._token, self._expires, self._last = None, 0.0, 0.0
        self._lock = threading.Lock()
        self.min_interval = 0.6                  # 모의투자 초당 호출 한도 대비

    @property
    def masked_account(self):
        return self.cano[:2] + '****' + self.cano[-2:] + '-' + self.product

    # ── 저수준 ──
    def _throttle(self):
        with self._lock:
            w = self.min_interval - (time.monotonic() - self._last)
            if w > 0:
                time.sleep(w)
            self._last = time.monotonic()

    def _http(self, method, path, headers, params=None, body=None, timeout=20):
        url = self.base + path
        if params:
            url += '?' + urllib.parse.urlencode(params)
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode('utf-8')), {k.lower(): v for k, v in r.headers.items()}

    def token(self):
        now = time.time()
        if self._token and now < self._expires - 300:
            return self._token
        try:
            j = json.load(open(self.token_file, encoding='utf-8'))
            if now < float(j.get('expires_at', 0)) - 300 and j.get('key_tail') == self.appkey[-4:]:
                self._token, self._expires = j['access_token'], float(j['expires_at'])
                return self._token
        except Exception:
            pass
        self._throttle()
        _, j, _ = self._http('POST', '/oauth2/tokenP', {'content-type': 'application/json'},
                             body={'grant_type': 'client_credentials', 'appkey': self.appkey, 'appsecret': self.appsecret})
        tok = j.get('access_token')
        if not tok:
            raise KISError(f"토큰 발급 실패: {j.get('error_description') or j.get('msg1') or j}")
        self._token, self._expires = tok, now + int(j.get('expires_in', 86400))
        try:
            json.dump({'access_token': tok, 'expires_at': self._expires, 'key_tail': self.appkey[-4:]}, open(self.token_file, 'w', encoding='utf-8'))
        except Exception:
            pass
        return tok

    def approval_key(self):
        """실시간 웹소켓 접속키 (B1.2) — 24시간 유효, 프로세스 안에서 앱키별로 12시간 재사용"""
        k = _APPROVAL.get(self.appkey[-6:])
        if k and time.time() - k[1] < 12 * 3600:
            return k[0]
        self._throttle()
        try:
            _, j, _ = self._http('POST', '/oauth2/Approval', {'content-type': 'application/json; charset=utf-8'},
                                 body={'grant_type': 'client_credentials', 'appkey': self.appkey, 'secretkey': self.appsecret})
        except Exception as e:
            raise KISError(f'웹소켓 접속키 발급 실패: {str(e)[:150]}')
        key = j.get('approval_key')
        if not key:
            raise KISError(f"웹소켓 접속키 발급 실패: {j.get('error_description') or j.get('msg1') or '응답 없음'}")
        _APPROVAL[self.appkey[-6:]] = (key, time.time())
        return key

    def _headers(self, tr_id, hashkey=None, tr_cont=''):
        h = {'content-type': 'application/json; charset=utf-8', 'authorization': f'Bearer {self.token()}',
             'appkey': self.appkey, 'appsecret': self.appsecret, 'tr_id': tr_id, 'custtype': 'P'}
        if hashkey:
            h['hashkey'] = hashkey
        if tr_cont:
            h['tr_cont'] = tr_cont
        return h

    def _hashkey(self, body):
        self._throttle()
        _, j, _ = self._http('POST', '/uapi/hashkey', {'content-type': 'application/json', 'appkey': self.appkey, 'appsecret': self.appsecret}, body=body)
        h = j.get('HASH') or j.get('hash')
        if not h:
            raise KISError(f'hashkey 실패: {j}')
        return h

    def _get(self, path, tr_id, params, tr_cont='', retry=3):
        last = None
        for k in range(retry):
            try:
                self._throttle()
                _, j, h = self._http('GET', path, self._headers(tr_id, None, tr_cont), params=params)
                if str(j.get('rt_cd', '0')) == '0':
                    return j, h
                last = KISError(f"{j.get('msg_cd', '')} {j.get('msg1', '')}".strip())
            except urllib.error.HTTPError as e:
                if e.code == 401:
                    self._token, self._expires = None, 0
                last = KISError(f'HTTP {e.code}')
            except Exception as e:
                last = KISError(str(e)[:200])
            time.sleep(1.0 * (k + 1))
        raise last

    def _post_order(self, path, tr_id, body):
        """주문 POST — 한 번만. 거절(rt_cd≠0)은 KISError, 전송 결과가 불분명하면 ORDER_SUBMISSION_AMBIGUOUS"""
        try:                                          # hashkey · 토큰 실패는 주문이 안 나간 것 → 거절 (불분명 아님, B1.2)
            hk = self._hashkey(body)
            headers = self._headers(tr_id, hk)
        except KISError:
            raise
        except Exception as e:
            raise KISError(f'주문 전 준비 실패(주문 안 나감): {str(e)[:150]}')
        self._throttle()
        try:
            _, j, _ = self._http('POST', path, headers, body=body, timeout=25)
        except urllib.error.HTTPError as e:
            if e.code in (400, 401, 403, 404):
                raise KISError(f'주문 거절 HTTP {e.code}')
            raise KISError(f'ORDER_SUBMISSION_AMBIGUOUS: HTTP {e.code}')
        except Exception as e:
            raise KISError(f'ORDER_SUBMISSION_AMBIGUOUS: {str(e)[:150]}')
        if str(j.get('rt_cd', '0')) != '0':
            raise KISError(f"주문 거절: {j.get('msg_cd', '')} {j.get('msg1', '')}".strip())
        return j

    # ── 기능 ──
    def price(self, ticker):
        j, _ = self._get('/uapi/domestic-stock/v1/quotations/inquire-price', 'FHKST01010100',
                         {'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': str(ticker).zfill(6)})
        o = j.get('output') or {}
        return _num(o.get('stck_prpr')), o

    def order(self, side, ticker, qty, ord_dvsn='01', price=0):
        """side buy/sell · ord_dvsn '01' 시장가 · '00' 지정가"""
        qty = int(qty)
        if qty <= 0:
            raise KISError('주문 수량 0')
        tr = 'VTTC0012U' if side == 'buy' else 'VTTC0011U'
        body = {'CANO': self.cano, 'ACNT_PRDT_CD': self.product, 'PDNO': str(ticker).zfill(6), 'ORD_DVSN': str(ord_dvsn),
                'ORD_QTY': str(qty), 'ORD_UNPR': str(int(price or 0)), 'EXCG_ID_DVSN_CD': 'KRX',
                'SLL_TYPE': '01' if side == 'sell' else '', 'CNDT_PRIC': ''}
        j = self._post_order('/uapi/domestic-stock/v1/trading/order-cash', tr, body)
        o = j.get('output') or {}
        return {'order_no': str(o.get('ODNO') or o.get('odno') or ''), 'org_no': str(o.get('KRX_FWDG_ORD_ORGNO') or o.get('krx_fwdg_ord_orgno') or ''),
                'time': str(o.get('ORD_TMD') or o.get('ord_tmd') or ''), 'msg': j.get('msg1', '')}

    def cancel(self, order_no, org_no=''):
        body = {'CANO': self.cano, 'ACNT_PRDT_CD': self.product, 'KRX_FWDG_ORD_ORGNO': str(org_no or ''), 'ORGN_ODNO': str(order_no),
                'ORD_DVSN': '00', 'RVSE_CNCL_DVSN_CD': '02', 'ORD_QTY': '0', 'ORD_UNPR': '0', 'QTY_ALL_ORD_YN': 'Y', 'EXCG_ID_DVSN_CD': 'KRX'}
        return self._post_order('/uapi/domestic-stock/v1/trading/order-rvsecncl', 'VTTC0013U', body)

    def balance(self):
        params = {'CANO': self.cano, 'ACNT_PRDT_CD': self.product, 'AFHR_FLPR_YN': 'N', 'OFL_YN': '', 'INQR_DVSN': '02', 'UNPR_DVSN': '01',
                  'FUND_STTL_ICLD_YN': 'N', 'FNCG_AMT_AUTO_RDPT_YN': 'N', 'PRCS_DVSN': '00', 'CTX_AREA_FK100': '', 'CTX_AREA_NK100': ''}
        a1, a2, cont = [], [], ''
        for _ in range(10):
            j, h = self._get('/uapi/domestic-stock/v1/trading/inquire-balance', 'VTTC8434R', params, tr_cont=cont)
            o1, o2 = j.get('output1') or [], j.get('output2') or []
            a1 += o1 if isinstance(o1, list) else [o1]
            a2 += o2 if isinstance(o2, list) else [o2]
            fk, nk, c = j.get('ctx_area_fk100', ''), j.get('ctx_area_nk100', ''), h.get('tr_cont', '')
            if c not in ('M', 'F') or not (fk or nk):
                break
            params['CTX_AREA_FK100'], params['CTX_AREA_NK100'], cont = fk, nk, 'N'
        pos = []
        for r in a1:
            q = int(_num(r.get('hldg_qty')))
            if q > 0:
                pos.append({'ticker': str(r.get('pdno', '')).zfill(6), 'name': r.get('prdt_name', ''), 'qty': q,
                            'sellable': int(_num(r.get('ord_psbl_qty'))), 'avg': _num(r.get('pchs_avg_pric')),
                            'price': _num(r.get('prpr')), 'value': _num(r.get('evlu_amt'))})
        s = a2[0] if a2 else {}
        cash = _num(s.get('dnca_tot_amt'))
        d2 = _num(s.get('prvs_rcdl_excc_amt')) or cash           # D+2 예수금 (주문 가능 추정)
        eq = _num(s.get('tot_evlu_amt')) or (cash + sum(p['value'] for p in pos))
        return {'positions': pos, 'cash': cash, 'cash_d2': d2, 'equity': eq}

    def fills(self, day):
        """그날 주문 · 체결 내역 → [{order_no, ticker, side, qty, filled, avg, status, time}]"""
        params = {'CANO': self.cano, 'ACNT_PRDT_CD': self.product, 'INQR_STRT_DT': day, 'INQR_END_DT': day, 'SLL_BUY_DVSN_CD': '00',
                  'PDNO': '', 'CCLD_DVSN': '00', 'INQR_DVSN': '00', 'INQR_DVSN_3': '00', 'ORD_GNO_BRNO': '', 'ODNO': '',
                  'INQR_DVSN_1': '', 'CTX_AREA_FK100': '', 'CTX_AREA_NK100': '', 'EXCG_ID_DVSN_CD': 'KRX'}
        out, cont = [], ''
        for _ in range(10):
            j, h = self._get('/uapi/domestic-stock/v1/trading/inquire-daily-ccld', 'VTTC0081R', params, tr_cont=cont)
            x = j.get('output1') or []
            out += x if isinstance(x, list) else [x]
            fk, nk, c = j.get('ctx_area_fk100', ''), j.get('ctx_area_nk100', ''), h.get('tr_cont', '')
            if c not in ('M', 'F') or not (fk or nk):
                break
            params['CTX_AREA_FK100'], params['CTX_AREA_NK100'], cont = fk, nk, 'N'
        res = []
        for r in out:
            res.append({'order_no': str(r.get('odno', '')), 'ticker': str(r.get('pdno', '')).zfill(6),
                        'side': 'sell' if str(r.get('sll_buy_dvsn_cd', '')) == '01' else 'buy',
                        'qty': int(_num(r.get('ord_qty'))), 'filled': int(_num(r.get('tot_ccld_qty'))),
                        'avg': _num(r.get('avg_prvs')), 'remain': int(_num(r.get('rmn_qty'))),
                        'cancelled': str(r.get('cncl_yn', 'N')) == 'Y', 'time': str(r.get('ord_tmd', ''))})
        return res
