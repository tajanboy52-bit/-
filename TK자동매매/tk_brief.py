"""
tk_brief.py — 📱 텔레그램 브리핑 (하루 3번)

  ☀️ 10:00 오전 브리핑  — 시스템 작동 상태 · 계좌 · 오늘 아침 매매(장전 · 09:02) · 칸별 · 오른/내린 종목
  🕐 13:00 중간 브리핑  — 계좌 · 오늘 흐름(장중 최고 · 최저) · 칸별 · 오른/내린 종목 · 남은 일정
  🌙 장마감 브리핑      — 신호 계산 뒤(18:40쯤): 오늘 결과 · 누적 성적 · 청산 · 밤사이 매수 · 내일 계획 · 연구 상태
                           (신호가 20:30까지 안 나오면 내일 계획 없이 보냄)
  위험 알림(🚨 안전장치 · 주문 거절 · 오류)은 이와 별도로 바로 보냄
"""
from datetime import datetime

import tk_db as db
import tk_signals as S

WD = '월화수목금토일'
LINE = '─────────────'


def _won(v, sign=False):
    if v is None:
        return '-'
    return f'{v:+,.0f}원' if sign else f'{v:,.0f}원'


def _pct(v):
    return '-' if v is None else f'{v:+.2f}%'


def _arrow(v):
    return '🔺' if (v or 0) > 0 else ('🔻' if (v or 0) < 0 else '▫️')


def _day(d):
    return f"{d[4:6]}/{d[6:]}({WD[datetime.strptime(d, '%Y%m%d').weekday()]})"


def _head(icon, title, d, cfg, hm=None):
    mode = '🔴 실전' if db.mode() == 'real' else '🟢 모의'
    return [f"{icon} {title}", f"{_day(d)} {hm or datetime.now().strftime('%H:%M')} · {mode} · 자동주문 {'ON' if cfg.get('kis_on') else 'OFF'}", LINE]


def _prev_close(d):
    import tk_trader as tr
    pd_ = tr.prev_trading_day(d)
    return {r[0]: r[1] for r in db.mconn().execute('SELECT ticker, close FROM bars WHERE date=?', (pd_,))}


def _prev_equity(d):
    r = db.conn().execute('SELECT value FROM equity WHERE date<? ORDER BY date DESC LIMIT 1', (d,)).fetchone()
    return r[0] if r and r[0] else None


# ════════════════════════════════════════════
#  조각
# ════════════════════════════════════════════
def status(cfg, d, market=True):
    """→ (한 줄 판정, [확인할 것]) — 정지 · 매수 중지 · 차단 · 자료 · 신호 · 실시간 · 장전 점검"""
    import tk_trader as tr
    import tk_ws as W
    warn = []
    if tr.halted():
        warn.append(f'⛔ 자동주문 정지: {tr.halted()}')
    if not cfg.get('kis_on'):
        warn.append('○ 자동주문 꺼짐 → 주문 안 나감 (⚙️ 설정에서 켜기)')
    if db.meta_get('auto_pause'):
        warn.append(f"⏸ 계좌 안전장치로 새 매수 중지: {db.meta_get('auto_pause')}")
    if cfg.get('pause_buy'):
        warn.append('⏸ 새 매수 일시 중지 (사용자)')
    if db.meta_get('block_new'):
        warn.append(f"🚫 앱 밖 종목 때문에 새 매수 차단: {db.meta_get('block_new')[:80]} → 💼 계좌 잔고에서 가져오기")
    pd_ = tr.prev_trading_day(d)
    if db.meta_get('last_signal_date') and db.meta_get('last_signal_date') < pd_:
        warn.append(f"📅 신호가 오래됨 ({db.meta_get('last_signal_date')}) → 오늘 새 매수 없음")
    pc = db.meta_get('precheck')
    if pc and pc.startswith(d) and 'OK' not in pc:
        warn.append('🔎 장전 점검: ' + pc[9:][:120])
    if market and cfg.get('ws_on', True) and not W.STATE.get('connected'):
        warn.append('📡 실시간 웹소켓 연결 안 됨 (1분 조회로 대신)')
    ok = not [w for w in warn if not w.startswith('📡')]
    return ('✅ 정상 작동' if ok else '⚠️ 확인 필요'), warn


def account(bal, d):
    if not bal:
        return ['💰 계좌', '  KIS 잔고 조회 실패 (연결 확인) — 📡 실시간 · 💼 계좌 잔고 탭에서 확인']
    prev = _prev_equity(d)
    eq = bal.get('equity')
    pos = bal.get('positions') or []
    buy = sum((p.get('avg') or 0) * p['qty'] for p in pos)
    val = sum((p.get('price') or 0) * p['qty'] for p in pos)
    L = ['💰 계좌', f"  총 평가  {_won(eq)}"]
    if prev and eq:
        L.append(f"  오늘     {_arrow(eq - prev)} {_won(eq - prev, True)} ({_pct((eq / prev - 1) * 100)})")
    L.append(f"  예수금   {_won(bal.get('cash_d2') or bal.get('cash'))} (D+2)")
    if pos:
        L.append(f"  주식     {_won(val)} · {len(pos)}종목" + (f" · 평가손익 {_won(val - buy, True)}" if buy else ''))
    return L


def trades(d):
    """오늘 매매 요약 (지금까지)"""
    import tk_trader as tr
    x = db.conn()
    o = [dict(r) for r in x.execute('SELECT * FROM orders WHERE date=?', (d,))]
    sells = [r for r in o if r['side'] == 'sell' and (r['filled'] or 0) > 0]
    buys = [r for r in o if r['side'] == 'buy' and (r['filled'] or 0) > 0]
    rej = [r for r in o if r['status'] == '거절']
    pend = [r for r in o if r['status'] in ('보냄', '접수', '부분', '예약')]
    done = [dict(r) for r in x.execute("SELECT * FROM lots WHERE exit_date=? AND status='청산'", (d,))]
    gap = x.execute("SELECT COUNT(*) FROM decisions WHERE date=? AND reason LIKE '%갭%'", (d,)).fetchone()[0]
    L = ['🔄 오늘 매매']
    if not (sells or buys or rej or pend):
        L.append('  아직 체결 없음')
        return L
    if sells:
        win = sum(1 for r in done if (r['pnl'] or 0) > 0)
        L.append(f"  매도 {len(sells)}건" + (f" · 실현 {_won(sum(r['pnl'] or 0 for r in done), True)} (승 {win} · 패 {len(done) - win})" if done else ''))
    if buys:
        amt = sum((r['filled'] or 0) * (r['avg'] or 0) for r in buys)
        by = {}
        for r in buys:
            by[r['sleeve']] = by.get(r['sleeve'], 0) + 1
        L.append(f"  매수 {len(buys)}건 · {_won(amt)} (" + ' · '.join(f'{k} {v}' for k, v in by.items()) + ')')
    if gap:
        L.append(f'  건너뜀 {gap}건 (시가 갭 +{tr.gap_limit({}) or 5:g}% 넘음)')
    if pend:
        L.append(f'  미체결 {len(pend)}건')
    if rej:
        L.append(f"  ❗ 거절 {len(rej)}건: " + ' · '.join(f"{r['name']}({(r['msg'] or '')[:20]})" for r in rej[:3]))
    if done:
        best, worst = max(done, key=lambda r: r['ret'] or 0), min(done, key=lambda r: r['ret'] or 0)
        L.append(f"  최고 {best['name']} {_pct(best['ret'])} · 최저 {worst['name']} {_pct(worst['ret'])}")
    return L


def sleeves(bal, d, cfg):
    """칸별 보유 · 오늘 손익 (어제 종가 대비 · 오늘 산 것은 매수가 대비)"""
    import tk_trader as tr
    px = {p['ticker']: p.get('price') for p in (bal.get('positions') or [])}
    pc = _prev_close(d)
    al = tr.alloc(cfg)
    L = ['📊 칸별']
    for s, m in S.SLEEVES.items():
        ls = [l for l in tr.open_lots(s) if l['status'] == '보유' and l['qty']]
        if not ls:
            continue
        today = 0.0
        for l in ls:
            p = px.get(l['ticker']) or l['last_px'] or l['entry_px'] or 0
            base = l['entry_px'] if l['entry_date'] == d else (pc.get(l['ticker']) or l['entry_px'] or p)
            today += l['qty'] * (p - base)
        L.append(f"  {m['icon']} {m['name']}  {len(ls)}종목 · 오늘 {_won(today, True)}" + (f" · 비중 {al[s]:g}%" if al.get(s) else ''))
    if len(L) == 1:
        L.append('  보유 없음')
    return L


def movers(bal, d, n=3):
    pc = _prev_close(d)
    st = db.stocks()
    rows = []
    for p in bal.get('positions') or []:
        b = pc.get(p['ticker'])
        nm = p.get('name') if p.get('name') and p.get('name') != p['ticker'] else ((st.get(p['ticker']) or {}).get('name') or p['ticker'])
        if b and p.get('price'):
            rows.append((nm, (p['price'] / b - 1) * 100))
    if len(rows) < 2:
        return []
    rows.sort(key=lambda r: -r[1])
    up = [f'{nm} {_pct(c)}' for nm, c in rows[:n] if c > 0]
    dn = [f'{nm} {_pct(c)}' for nm, c in rows[::-1][:n] if c < 0]
    L = ['📈 오늘 움직임 (어제 종가 대비)']
    if up:
        L.append('  🔺 ' + ' · '.join(up))
    if dn:
        L.append('  🔻 ' + ' · '.join(dn))
    return L if len(L) > 1 else []


def _sys_line(cfg, d, market=True):
    import tk_ws as W
    head, warn = status(cfg, d, market)
    L = ['⚙️ 시스템', f'  {head}']
    if market:
        L.append(f"  실시간 {'연결 · ' + str(len(W.STATE.get('subs') or [])) + '종목' if W.STATE.get('connected') else '대기'} · 일봉 {(db.last_bar_day() or '-')[4:]} · 신호 {(db.meta_get('last_signal_date') or '-')[4:]}")
    L += [f'  {w}' for w in warn]
    return L


def _join(*blocks):
    out = []
    for b in blocks:
        if b:
            out += b + ['']
    return '\n'.join(out).rstrip()


# ════════════════════════════════════════════
#  브리핑 3종
# ════════════════════════════════════════════
def morning(cfg, bal, d):
    """☀️ 10:00 — 시스템 작동 상태 + 오늘 아침 매매"""
    return _join(_head('☀️', '오전 브리핑', d, cfg), _sys_line(cfg, d), account(bal, d), trades(d), sleeves(bal, d, cfg), movers(bal, d),
                 ['⏭ 다음: 13:00 중간 브리핑 · 15:20 밤사이 매수'])


def midday(cfg, bal, d):
    """🕐 13:00 — 오늘 흐름 중간 점검"""
    x = db.conn()
    rows = [r for r in x.execute('SELECT ts, value FROM intraday WHERE ts>=? ORDER BY ts', (d,))]
    prev = _prev_equity(d)
    flow = []
    if rows and prev:
        hi, lo = max(rows, key=lambda r: r[1]), min(rows, key=lambda r: r[1])
        flow = ['🕒 오늘 흐름 (1분 평가)', f"  최고 {hi[0][8:10]}:{hi[0][10:12]} {_pct((hi[1] / prev - 1) * 100)} · 최저 {lo[0][8:10]}:{lo[0][10:12]} {_pct((lo[1] / prev - 1) * 100)}"]
    import tk_trader as tr
    al = tr.alloc(cfg)
    nxt = ['⏭ 남은 일정']
    if al.get('ON'):
        nxt.append(f"  15:20 🌙 KODEX 코스닥150 종가 매수 (평가액 {al['ON']:g}%)")
    if tr.sweep_on(cfg):
        nxt.append('  15:20 💤 남는 현금 → KODEX 200')
    nxt.append('  장 마감 뒤 🌙 장마감 브리핑')
    _, warn = status(cfg, d)
    st = ['⚙️ 시스템 ' + ('✅ 정상' if not [w for w in warn if not w.startswith('📡')] else '⚠️ 확인 필요')] + [f'  {w}' for w in warn]
    return _join(_head('🕐', '중간 브리핑', d, cfg), account(bal, d), flow, trades(d), sleeves(bal, d, cfg), movers(bal, d), st, nxt)


def closing(cfg, d, plan=True, resv=0):
    """🌙 장마감 — 오늘 결과 · 누적 · 내일 계획 (장 마감 기록 기준)"""
    import tk_trader as tr
    x = db.conn()
    eq = [dict(r) for r in x.execute('SELECT * FROM equity WHERE date<=? ORDER BY date DESC LIMIT 2', (d,))]
    sv = float(db.meta_get('start_value') or 0)
    acc = ['💰 오늘 결과']
    if eq and eq[0]['date'] == d:
        e, p = eq[0], (eq[1] if len(eq) > 1 else None)
        acc.append(f"  총 평가  {_won(e['value'])}")
        if p:
            acc.append(f"  오늘     {_arrow(e['value'] - p['value'])} {_won(e['value'] - p['value'], True)} ({_pct((e['value'] / p['value'] - 1) * 100)})")
        if sv:
            sd = db.meta_get('start_date')
            acc.append(f"  누적     {_pct((e['value'] / sv - 1) * 100)}" + (f" ({sd[4:6]}/{sd[6:]} 시작)" if sd else ''))
        acc.append(f"  고점 대비 {(e['value'] / e['peak'] - 1) * 100:+.1f}% · 예수금(D+2) {_won(e['cash'])}")
    else:
        acc.append('  장 마감 기록 없음 (15:45 마감 처리 확인)')
    al = tr.alloc(cfg)
    sl = ['📊 칸별 (보유 · 평가 · 누적 실현)']
    for s, m in S.SLEEVES.items():
        ls = [l for l in tr.open_lots(s) if l['status'] == '보유' and l['qty']]
        cl = [dict(r) for r in x.execute("SELECT pnl FROM lots WHERE sleeve=? AND status='청산'", (s,))]
        if not ls and not cl:
            continue
        if not ls and al.get(s, 0 if s == 'IN' else 1) == 0 and not x.execute('SELECT 1 FROM lots WHERE sleeve=? AND exit_date=?', (s, d)).fetchone():
            continue
        val = sum(l['qty'] * (l['last_px'] or l['entry_px'] or 0) for l in ls)
        inv = sum(l['cost'] * (l['qty'] / l['qty0'] if l['qty0'] else 1) for l in ls)
        win = sum(1 for r in cl if (r['pnl'] or 0) > 0)
        rz = f"실현 {_won(sum(r['pnl'] or 0 for r in cl), True)}" + (f" (승률 {win / len(cl) * 100:.0f}%)" if cl else '')
        sl.append(f"  {m['icon']} {m['name']}  " + (f"{len(ls)}종목 · 평가 {_won(val - inv, True)} · {rz}" if ls else f"보유 없음 · {rz}"))
    night = []
    on = [dict(r) for r in x.execute("SELECT * FROM orders WHERE date=? AND kind IN ('on_buy','sw_buy') AND filled>0", (d,))]
    if on:
        night = ['🌙 밤사이 보유 (내일 시가에 팜)'] + [f"  {r['name']} {r['filled']}주 · {_won((r['filled'] or 0) * (r['avg'] or 0))}" for r in on]
    tm = []
    if plan:
        sells = [dict(r) for r in x.execute("SELECT * FROM lots WHERE status='보유' AND sell_flag=1 AND sleeve NOT IN ('ON','SW')")]
        sig = [dict(r) for r in x.execute('SELECT * FROM signals WHERE date=? AND rank<100 ORDER BY sleeve, rank', (d,))]
        nd = tr.next_trading_day(d)
        tm = [f'🗓 내일 {_day(nd)} 08:50 계획']
        tm.append(f"  매도 {len(sells)}건" + (': ' + ', '.join(l['name'] for l in sells[:8]) + (f' 외 {len(sells) - 8}' if len(sells) > 8 else '') if sells else ''))
        for s in ('LVH', 'REV', 'DV'):
            ss = [r['name'] for r in sig if r['sleeve'] == s]
            if ss:
                tm.append(f"  {S.SLEEVES[s]['icon']} 매수 후보 {', '.join(ss[:6])}" + (f' 외 {len(ss) - 6}' if len(ss) > 6 else ''))
        if not sig:
            tm.append('  매수 후보 없음')
        if resv:
            tm.append(f'  📅 매도 {resv}건 예약주문 완료 (PC가 꺼져 있어도 나감)')
        tm.append(f"  갭 +{tr.gap_limit(cfg) or 0:g}% 넘게 오르면 매수 안 함" if tr.gap_limit(cfg) else '  갭 필터 꺼짐')
    else:
        tm = ['🗓 내일 계획', '  ⚠️ 오늘 신호 계산이 아직 안 됨 → 📥 데이터 탭 확인 (내일 아침 따라잡기 시도)']
    lab = []
    try:
        import tk_intraday as IL
        import tk_minute as MN
        ms = MN.status()
        ok = sorted(IL.passed())
        if ms.get('days'):
            lab = ['🔬 연구', f"  ⏱ 1분봉 {ms['days']}일" + (f" (남은 {ms['left']}일)" if ms.get('left') else '') + f" · 장중 규칙 통과 {', '.join(ok) if ok else '아직 없음'}"]
    except Exception:
        pass
    _, warn = status(cfg, d, market=False)
    st = ['⚙️ 시스템 ' + ('✅ 정상' if not warn else '⚠️ 확인 필요')] + [f'  {w}' for w in warn]
    return _join(_head('🌙', '장마감 브리핑', d, cfg), acc, trades(d), sl, night, tm, lab, st)
