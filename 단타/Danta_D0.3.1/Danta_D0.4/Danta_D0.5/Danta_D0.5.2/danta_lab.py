"""
danta_lab.py — 🔬 TK Danta 매도 규칙 연구실 (쌓인 1분봉을 되감아 매도 규칙을 비교)

· 진입: 쌓인 1분봉에서 '현재 매수 조건'을 분마다 다시 계산해 처음 충족한 분의 다음 분 시가 + 슬리피지로 매수
        (탐지 시각 · 가격 · 등락률 · 누적 거래량/거래대금 · 시가 대비 · 고가 대비 · 상한가 제외 — 실시간 가상 단타와 같은 조건)
        ※ 실시간은 KIS 순위 상위 30 안에서만 찾지만 여기선 수집된 종목 전체 → 표본이 많음 (순위 조건은 재현 불가)
        · 또는 '실제 가상 매수만' (vtrades)
· 매도: danta_exit.decide — 실시간 가상 단타와 '같은 코드'. 분봉 안에서는 저가 → 고가 → 종가 순으로 봄 (손절 먼저 · 보수적)
        다음날 이후 분봉이 없으면 Scout 일봉(시가 → 저가 → 고가 → 종가)으로 대신
· 비용 0.25% · 슬리피지 반영 · 날짜를 앞/뒤 절반으로 나눠 두 기간 모두 좋은 규칙만 위로 (한 기간만 좋은 건 우연일 수 있음)
· 결과는 참고용 — 규칙 적용은 사람이 버튼으로 (자동으로 바꾸지 않음)
"""
import bisect
import json
import math
import sqlite3
import threading
import time
from datetime import datetime, timedelta

import danta_db as db
import danta_exit as ex
import danta_models as mdl

COST = 0.25
JOB = {'running': False, 'msg': '', 'done': 0, 'total': 0, 'error': ''}
_lock = threading.Lock()


def grid():
    """비교할 매도 규칙 — 프로필 A~E + 격자 (익절 × 손절 × 트레일링 × 청산 방식)"""
    out = [{'key': k, 'name': f"{k} {v['name']}", **v['p']} for k, v in ex.PROFILES.items()]
    modes = [('당일 11:00', {'exit_by': '11:00', 'hold_days': 0, 'carry_limit': 0}),
             ('당일 15:15 · 상한가 오버나잇', {'exit_by': '15:15', 'hold_days': 0, 'carry_limit': 1}),
             ('다음날 시가', {'exit_by': '09:00', 'hold_days': 1, 'carry_limit': 1})]
    for tp in (0, 3, 5, 8):
        for sl in (-2, -3, -5):
            for tr in ((0, 0), (4, 2)):
                for mn, m in modes:
                    name = f"익절 {'없음' if not tp else '+%g' % tp} · 손절 {sl:g} · {'트레일링 %g→%g' % tr if tr[0] else '트레일링 없음'} · {mn}"
                    out.append({'key': f'g{tp}_{-sl}_{tr[0]}_{m["hold_days"]}{m["carry_limit"]}{m["exit_by"][:2]}', 'name': name,
                                'tp': float(tp), 'sl': float(sl), 'trail_start': float(tr[0]), 'trail_gap': float(tr[1]),
                                'hold_min': 0, **m})
    return out


def _scout():
    f = db.scout_db_path()
    return sqlite3.connect(f'file:{f}?mode=ro', uri=True, timeout=30) if f else None


def _daily(s, tickers, frm):
    """Scout 일봉 {ticker: (dates, rows)} — 전일 종가 · 다음날 이후 대체용"""
    out = {}
    if not s:
        return out
    for tk in tickers:
        rows = s.execute("SELECT date, open, high, low, close FROM candles WHERE ticker=? AND date>=? ORDER BY date", (tk, frm)).fetchall()
        out[tk] = ([r[0] for r in rows], rows)
    return out


def _bars(c, tk, d):
    return c.execute("SELECT hm, open, high, low, close, vol, amt FROM bars WHERE ticker=? AND date=? ORDER BY hm", (tk, d)).fetchall()


def _hm(h):
    return f'{h // 100:02d}:{h % 100:02d}'


def entries(s, frm='', to='', src='bars', cap=3000):
    """진입 목록 [(date, ticker, buy_hm, buy_px, prev_close)]"""
    c = db.conn()
    out = []
    if src == 'vtrades':
        for r in c.execute("SELECT date, ticker, buy_ts, buy_px, sig_px, sig_chg FROM vtrades WHERE date BETWEEN ? AND ? ORDER BY date",
                           (frm or '0', to or '9')):
            pc = r[4] / (1 + r[5] / 100) if r[5] is not None and r[4] else 0
            out.append((r[0], r[1], r[2][11:16], r[3], pc))
        return out[-cap:]
    sc = _scout()
    try:
        pairs = c.execute("SELECT DISTINCT date, ticker FROM done WHERE n>=300 AND date BETWEEN ? AND ? ORDER BY date DESC",
                          (frm or '0', to or '9')).fetchall()
        tks = {p[1] for p in pairs}
        dmin = min((p[0] for p in pairs), default='0')
        dl = _daily(sc, tks, str(int(dmin) - 100)) if sc else {}
        lo, hi = s['scan_start'], s['scan_end']
        for d, tk in pairs:
            ds, rows = dl.get(tk, ([], []))
            j = bisect.bisect_left(ds, d)
            if j == 0 or not rows:
                continue
            pc = rows[j - 1][4]
            if not pc:
                continue
            B = _bars(c, tk, d)
            if len(B) < 30:
                continue
            day_open = B[0][1]
            hi_run, cv, ca = 0.0, 0, 0.0
            for i, (h, o, hh, ll, cl, v, a) in enumerate(B):
                hi_run = max(hi_run, hh)
                cv += v or 0
                ca += a or 0
                t = _hm(h)
                if t < lo:
                    continue
                if t > hi or i + 1 >= len(B):
                    break
                chg = (cl / pc - 1) * 100
                if not (s['price_min'] <= cl <= s['price_max'] and s['chg_min'] <= chg <= s['chg_max'] and chg >= s['buy_chg_min']):
                    continue
                if cv < s['vol_min'] or ca < s['amt_min_eok'] * 1e8:
                    continue
                if cl < day_open * (1 + s['above_open'] / 100) or cl < hi_run * (1 - s['near_high'] / 100) or cl >= mdl.upper_price(pc):
                    continue
                nb = B[i + 1]
                out.append((d, tk, _hm(nb[0]), nb[1] * (1 + s['slip'] / 100), pc))
                break
            if len(out) >= cap:
                break
    finally:
        if sc:
            sc.close()
    return out


def _path(c, dl, tk, d, days_after):
    """진입일 이후 날짜들의 (date, [(hm, o, h, l, c)]) — 분봉이 없으면 Scout 일봉 4점"""
    ds, rows = dl.get(tk, ([], []))
    j = bisect.bisect_left(ds, d)
    out = []
    for k in range(1, days_after + 1):
        if j + k >= len(ds):
            break
        dd = ds[j + k]
        B = _bars(c, tk, dd)
        if len(B) >= 300:
            out.append((dd, [(b[0], b[1], b[2], b[3], b[4]) for b in B], rows[j + k - 1][4]))
        else:
            _, o, h, l, cl = rows[j + k]
            out.append((dd, [(900, o, o, o, o), (1200, o, h, l, (h + l) / 2), (1515, cl, cl, cl, cl)], rows[j + k - 1][4]))
    return out


def simulate(rule, e, day0, later, slip):
    """한 진입 · 한 규칙 → (수익률 %, 사유). day0: 진입일 분봉 [(hm,o,h,l,c)] · later: [(date, bars, 전일 종가)]
       아무 일도 일어날 수 없는 분봉은 건너뜀(속도) — 판단은 모두 ex.decide"""
    d, tk, bhm, bpx, pc = e
    pos = {k: rule[k] for k in ex.PKEYS}
    pos.update(buy_px=bpx, buy_ts=f'{d[:4]}-{d[4:6]}-{d[6:]}T{bhm}:00', peak=bpx, plan='')
    sl_px = bpx * (1 + rule['sl'] / 100)
    tp_px = bpx * (1 + rule['tp'] / 100) if rule.get('tp') else float('inf')
    ts, tg = rule.get('trail_start') or 0, rule.get('trail_gap') or 0
    ts_px = bpx * (1 + ts / 100) if ts and tg else float('inf')
    hd = int(rule.get('hold_days') or 0)
    bh = int(bhm[:2]) * 100 + int(bhm[3:])
    seq = [(d, day0, pc)] + later
    for di, (dd, B, prev) in enumerate(seq):
        upper = mdl.upper_price(prev) if prev else 0
        first = di > 0
        t_ex = 0 if di > hd else (9999 if di < hd else int(min(rule['exit_by'], ex.SAFETY).replace(':', '')))
        if di == 0 and hd == 0 and rule.get('hold_min'):
            m = bh // 100 * 60 + bh % 100 + int(rule['hold_min'])
            t_ex = min(t_ex, m // 60 * 100 + m % 60)
        for (h, o, hh, ll, cl) in B:
            if di == 0 and h < bh:
                continue
            if h > 1520:
                break
            peak = pos['peak']
            npk = max(peak, hh)
            if (not first and not pos['plan'] and h < t_ex and ll > sl_px and hh < tp_px
                    and (npk < ts_px or ll > npk * (1 - tg / 100)) and h < 1515):
                pos['peak'] = npk
                continue
            now = datetime(int(dd[:4]), int(dd[4:6]), int(dd[6:]), h // 100, h % 100)
            pre = now - timedelta(minutes=1)                # 봉 안의 저가 · 고가는 '분이 끝나기 전' — 시각 청산은 종가에서만
            pts = ([(o, now)] if first else []) + [(ll, pre), (hh, pre), (cl, now)]
            for pi, (p, tnow) in enumerate(pts):
                act, px, why, plan = ex.decide(pos, p, tnow, {'open': o, 'upper': upper}, di, first=first and pi == 0)
                pos['peak'] = max(pos['peak'], p)
                pos['plan'] = plan
                if act == 'sell':
                    return (px * (1 - slip / 100) / bpx - 1) * 100 - COST, why
            first = False
    last = seq[-1][1][-1][4] if seq and seq[-1][1] else bpx                     # 자료 끝 — 마지막 종가로 정리
    return (last * (1 - slip / 100) / bpx - 1) * 100 - COST, '자료 끝'


def run(cfg_settings, frm='', to='', src='bars', cap=3000):
    """연구 실행 → 결과 dict (meta 'lab_result'에 저장)"""
    s = cfg_settings
    t0 = time.time()
    JOB.update(running=True, msg='진입 찾는 중', done=0, total=0, error='')
    E = entries(s, frm, to, src, cap)
    c = db.conn()
    sc = _scout()
    try:
        dl = _daily(sc, {e[1] for e in E}, str(int(min((e[0] for e in E), default='20000101')) - 100)) if sc else {}
        paths = []
        for e in E:
            B = [(b[0], b[1], b[2], b[3], b[4]) for b in _bars(c, e[1], e[0])]
            if len(B) < 30:                                    # 진입일 분봉이 없으면 되감을 수 없음
                continue
            paths.append((e, B, _path(c, dl, e[1], e[0], 2)))
        E = [p[0] for p in paths]
    finally:
        if sc:
            sc.close()
    from danta_live import reason_cat as cat
    G = grid()
    cur = {'key': 'M', 'name': '현재 설정', **{k: s[k] for k in ex.PKEYS}}
    G = [cur] + G
    dates = sorted({e[0] for e in E})
    mid = dates[len(dates) // 2] if dates else ''
    if not paths:
        G = []
    JOB.update(total=len(G), msg=f'진입 {len(E)}건 × 규칙 {len(G)}개')
    res = []
    for gi, rule in enumerate(G):
        rs = []
        why = {}
        for e, B, later in paths:
            r, w = simulate(rule, e, B, later, s['slip'])
            rs.append((e[0], r))
            w = cat(w)
            why[w] = why.get(w, 0) + 1
        res.append(_stats(rule, rs, mid, why))
        JOB.update(done=gi + 1)
    for r in res:
        r['score'] = min(r['is_avg'], r['oos_avg']) if r['is_n'] >= 30 and r['oos_n'] >= 30 else None
    res.sort(key=lambda r: (r['score'] is None, -(r['score'] or 0)))
    out = {'ts': datetime.now().isoformat(timespec='seconds'), 'src': src, 'n_entries': len(E), 'dates': [dates[0], dates[-1]] if dates else [],
           'split': mid, 'n_days': len(dates), 'secs': round(time.time() - t0), 'rows': res,
           'entry_rule': {k: s[k] for k in ('scan_start', 'scan_end', 'price_min', 'price_max', 'chg_min', 'chg_max', 'buy_chg_min',
                                            'near_high', 'above_open', 'vol_min', 'amt_min_eok', 'slip')}}
    db.meta_set('lab_result', json.dumps(out, ensure_ascii=False))
    JOB.update(running=False, msg=f"완료 · 진입 {len(E)}건 · {len(dates)}일 · {out['secs']}초")
    return out


def _stats(rule, rs, mid, why):
    def part(x):
        v = [r for _, r in x]
        if not v:
            return 0, None, None, None
        by = {}
        for d, r in x:
            by.setdefault(d, []).append(r)
        m = [sum(a) / len(a) for a in by.values()]
        t = None
        if len(m) > 2:
            mu = sum(m) / len(m)
            sd = math.sqrt(sum((a - mu) ** 2 for a in m) / (len(m) - 1))
            t = round(mu / (sd / math.sqrt(len(m))), 2) if sd > 0 else None
        return len(v), round(sum(v) / len(v), 3), round(sum(1 for a in v if a > 0) / len(v) * 100, 1), t
    n, avg, win, t = part(rs)
    a = part([x for x in rs if x[0] < mid])
    b = part([x for x in rs if x[0] >= mid])
    return {'key': rule['key'], 'name': rule['name'], 'rule': {k: rule[k] for k in ex.PKEYS}, 'n': n, 'avg': avg, 'win': win, 't': t,
            'is_n': a[0], 'is_avg': a[1] if a[1] is not None else -99, 'oos_n': b[0], 'oos_avg': b[1] if b[1] is not None else -99,
            'why': dict(sorted(why.items(), key=lambda x: -x[1])[:6])}


def start(settings, frm='', to='', src='bars', cap=3000):
    if not _lock.acquire(blocking=False):
        return False

    def go():
        try:
            run(settings, frm, to, src, cap)
        except Exception as e:
            import traceback
            traceback.print_exc()
            JOB.update(running=False, msg='실패', error=str(e)[:200])
        finally:
            _lock.release()
    threading.Thread(target=go, daemon=True).start()
    return True


def result():
    return json.loads(db.meta_get('lab_result', '') or 'null')


# ════════════════════════════════════════════
#  모델 9개 + 대조군 × 매도 6개 (D0.5 · D0.6 M9)
# ════════════════════════════════════════════
def _hmi(h):
    return f'{h // 100:02d}:{h % 100:02d}'


def model_entries(s, frm='', to='', seed=7, prog=None):
    """쌓인 분봉에서 모델 9개의 신호를 분마다 다시 계산 → 진입 목록 {model: [(date, tk, hm, px, pc)]}
       신호가 난 분의 다음 분 시가 + 슬리피지로 매수 (M7 · M8 · M9는 그 분 가격 · 상한가는 상한가 그대로)
       대조군 Z: M1이 산 그 분에 M1 필터를 통과한 다른 종목 중 무작위 1 → 다음 분 시가"""
    c = db.conn()
    sc = _scout()
    rnd = __import__('random').Random(seed)
    out = {m: [] for m in mdl.ORDER}
    try:
        pairs = c.execute("SELECT DISTINCT date, ticker FROM done WHERE n>=300 AND date BETWEEN ? AND ? ORDER BY date",
                          (frm or '0', to or '9')).fetchall()
        dmin = min((p[0] for p in pairs), default='0')
        dl = _daily(sc, {p[1] for p in pairs}, str(int(dmin) - 100)) if sc else {}
        wins = {m: tuple(int(x.replace(':', '')) for x in mdl.window(m, s)) for m in mdl.ORDER if m != 'Z'}
        zpool = {}                                            # (date, 신호 분) → [(tk, 매수 분, 매수가, 전일 종가)]
        m1sig = []                                            # M1 (date, tk, 신호 분)
        by_date = {}
        for d, tk in pairs:
            by_date.setdefault(d, []).append(tk)
        done_n = 0
        for d in sorted(by_date):
            for tk in by_date[d]:
                done_n += 1
                if prog and done_n % 200 == 0:
                    prog(done_n, len(pairs))
                ds, rows = dl.get(tk, ([], []))
                j = bisect.bisect_left(ds, d)
                if j == 0 or not rows or j > len(rows) or not rows[j - 1][4]:
                    continue
                pc = rows[j - 1][4]
                pchg = (pc / rows[j - 2][4] - 1) * 100 if j >= 2 and rows[j - 2][4] else None
                pvol = None
                r2 = sc.execute("SELECT volume FROM candles WHERE ticker=? AND date=?", (tk, ds[j - 1])).fetchone() if sc else None
                if r2:
                    pvol = r2[0]
                B = _bars(c, tk, d)
                if len(B) < 30:
                    continue
                upper = mdl.upper_price(pc)
                day_open = B[0][1]
                hi = lo = None
                cv = ca = 0.0
                orb = 0.0
                hist = []
                got = set()
                for i, (h, o, hh, ll, cl, v, a) in enumerate(B):
                    hi = hh if hi is None else max(hi, hh)
                    lo = ll if lo is None else min(lo, ll)
                    cv += v or 0
                    ca += a or 0
                    if h <= 905:
                        orb = max(orb, hh)
                    chg = (cl / pc - 1) * 100
                    t = _hmi(h)
                    nxt = B[i + 1][1] if i + 1 < len(B) else None
                    if ca >= mdl.BASE_AMT and s['price_min'] <= cl <= s['price_max']:
                        st = {'hm': t, 'price': cl, 'open': day_open, 'high': hi, 'low': lo, 'pc': pc, 'chg': chg, 'cum_vol': cv, 'cum_amt': ca,
                              'vwap': ca / cv if cv else None, 'orb_high': orb if h > 905 and orb else None, 'prev_vol': pvol,
                              'prev_chg': pchg, 'upper': upper, 'halted': False, 'hist': hist[-15:],
                              'ask_upper': (ll < upper) if cl >= upper else None}
                        for m, (wa, wb) in wins.items():
                            if m in got or not (wa <= h <= wb):
                                continue
                            if m in ('M1', 'M2', 'M3', 'M5', 'M8') and chg < 2:
                                continue
                            if m == 'M6' and chg > -7:
                                continue
                            if m in mdl.LIMIT_UP and cl < upper:
                                continue
                            ok, _ = mdl.signal(m, st, s)
                            if not ok:
                                continue
                            if m in ('M7', 'M8', 'M9'):
                                px = min(cl * (1 + s['slip'] / 100), upper) if m == 'M8' else upper
                                out[m].append((d, tk, t, px, pc))
                                got.add(m)
                            elif nxt:
                                out[m].append((d, tk, _hmi(B[i + 1][0]), nxt * (1 + s['slip'] / 100), pc))
                                got.add(m)
                                if m == 'M1':
                                    m1sig.append((d, tk, t))
                        wa, wb = wins['M1']
                        if nxt and wa <= h <= wb and not mdl.m1_filter(st, s):
                            zpool.setdefault((d, t), []).append((tk, _hmi(B[i + 1][0]), nxt * (1 + s['slip'] / 100), pc))
                    hist.append((t, cl))
        zheld = set()
        for (d, tk, t) in m1sig:
            pool = [z for z in zpool.get((d, t), []) if z[0] != tk and (d, z[0]) not in zheld]
            if pool:
                z = rnd.choice(pool)
                zheld.add((d, z[0]))
                out['Z'].append((d, z[0], z[1], z[2], z[3]))
    finally:
        if sc:
            sc.close()
    return out


def run_models(s, frm='', to='', seed=7):
    t0 = time.time()
    JOB.update(running=True, msg='분봉에서 모델 신호 찾는 중', done=0, total=0, error='')
    E = model_entries(s, frm, to, seed, prog=lambda a, b: JOB.update(msg=f'신호 찾는 중 {a}/{b} (종목·일)'))
    from danta_live import reason_cat as cat
    c = db.conn()
    sc = _scout()
    allE = [e for m in E for e in E[m]]
    dates = sorted({e[0] for e in allE})
    mid = dates[len(dates) // 2] if dates else ''
    rows = []
    try:
        dl = _daily(sc, {e[1] for e in allE}, str(int(min(dates, default='20000101')) - 100)) if sc else {}
        cache = {}
        exits = [('own', '모델 자체 매도', None)] + [(k, f"{k} {v['name']}", v['p']) for k, v in ex.PROFILES.items()]
        total = sum(len(v) for v in E.values())
        JOB.update(total=total, done=0, msg=f'진입 {total}건 × 매도 {len(exits)}개 되감기')
        n = 0
        for m in mdl.ORDER:
            res = {k: [] for k, _, _ in exits}
            why = {k: {} for k, _, _ in exits}
            for e in E[m]:
                n += 1
                if n % 100 == 0:
                    JOB.update(done=n)
                key = (e[0], e[1])
                if key not in cache:
                    B = [(b[0], b[1], b[2], b[3], b[4]) for b in _bars(c, e[1], e[0])]
                    cache[key] = (B, _path(c, dl, e[1], e[0], 2))
                    if len(cache) > 4000:
                        cache.pop(next(iter(cache)))
                B, later = cache[key]
                for k, _, p in exits:
                    rule = dict(p) if p else mdl.exit_rule(m, s)
                    r, w = simulate(rule, e, B, later, s['slip'])
                    res[k].append((e[0], r))
                    w = cat(w)
                    why[k][w] = why[k].get(w, 0) + 1
            for k, nm, _ in exits:
                st = _stats({'key': f'{m}:{k}', 'name': nm, **(mdl.exit_rule(m, s) if k == 'own' else ex.PROFILES[k]['p'])}, res[k], mid, why[k])
                st.update(model=m, exit=k, model_name=mdl.MODELS[m]['name'])
                rows.append(st)
    finally:
        if sc:
            sc.close()
    zown = next((r for r in rows if r['model'] == 'Z' and r['exit'] == 'own'), None)
    for r in rows:
        r['vs_z'] = round(r['avg'] - zown['avg'], 3) if zown and zown['avg'] is not None and r['avg'] is not None and r['model'] != 'Z' else None
    out = {'ts': datetime.now().isoformat(timespec='seconds'), 'n_days': len(dates), 'dates': [dates[0], dates[-1]] if dates else [],
           'split': mid, 'secs': round(time.time() - t0), 'models': rows, 'n_entries': {m: len(E[m]) for m in E}}
    db.meta_set('lab_models', json.dumps(out, ensure_ascii=False))
    JOB.update(running=False, msg=f"완료 · 모델 진입 {sum(out['n_entries'].values())}건 · {len(dates)}일 · {out['secs']}초")
    return out


def start_models(settings, frm='', to=''):
    if not _lock.acquire(blocking=False):
        return False

    def go():
        try:
            run_models(settings, frm, to)
        except Exception as e:
            import traceback
            traceback.print_exc()
            JOB.update(running=False, msg='실패', error=str(e)[:200])
        finally:
            _lock.release()
    threading.Thread(target=go, daemon=True).start()
    return True


def result_models():
    return json.loads(db.meta_get('lab_models', '') or 'null')
