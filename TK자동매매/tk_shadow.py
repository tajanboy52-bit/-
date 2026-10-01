"""
tk_shadow.py — 👥 그림자 운용 (가상 동시 운용)

지금 설정(기본)과 미리 정한 실험 설정 몇 개를 **같은 시장 자료 · 같은 규칙 엔진(백테스트와 동일)**으로 날마다 나란히 굴린다. 주문은 없음.
· 앞으로: 처음 켠 날(shadow_start)부터 — 그 뒤에 나온 자료라 **진짜 표본 밖** 비교 (설정을 바꿀 근거로 가장 믿을 만함)
· 참고: 최근 60거래일 — 바로 볼 수 있지만 이미 지나간 구간
· 날마다 신호 계산 뒤 자동 갱신 (tk_server) · 결과 %APPDATA%\\TKAuto\\shadow_result.json
판단은 사람이: 실험 설정이 '앞으로' 구간에서 꾸준히 나으면 ⚙️ 설정에 반영 → 모의 → 실전
"""
import json
import os
import time

import numpy as np
import pandas as pd

import tk_backtest as B
import tk_db as db
import tk_signals as S

RESULT = os.path.join(db.DATA_DIR, 'shadow_result.json')
STATE = {'running': False, 'msg': '', 'err': ''}


def base_kwargs(cfg):
    """지금 실전 설정 → 백테스트 엔진 인자"""
    import tk_trader as tr
    al = {k: v / 100 for k, v in tr.alloc(cfg).items() if v > 0}
    return {'alloc': al, 'slots': tr.slots(cfg), 'pick': tr.picks(cfg), 'gap': tr.gap_limit(cfg), 'sweep': tr.sweep_on(cfg),
            'sweep_mode': cfg.get('sweep_mode') or 'night', 'hold': {}}


def variants(cfg):
    """미리 정한 비교 대상 (지금 설정에서 하나씩만 바꿈)"""
    b = base_kwargs(cfg)
    al = b['alloc']
    out = [('기본 (지금 설정)', b)]

    def v(name, **kw):
        out.append((name, {**b, **kw}))
    v('LVH 20일 보유 (원래 검증값)', hold={'LVH': 20}, slots={**b['slots'], 'LVH': 20})
    v('LVH 순위 4~10', pick={**b['pick'], 'LVH': (3, 7)})
    v('갭 필터 끔', gap=None)
    v('남는 현금 지수 없음', sweep=False)
    v('지수 60일선 보유', sweep_mode='ma60')
    v('DV 20% 켜기 (LVH −10 · REV −10)', alloc={**al, 'DV': 0.20, 'LVH': max(0, al.get('LVH', 0) - .10), 'REV': max(0, al.get('REV', 0) - .10)})
    v('REV 끄고 LVH로', alloc={**{k: x for k, x in al.items() if k != 'REV'}, 'LVH': al.get('LVH', 0) + al.get('REV', 0)})
    return out


def _sim(D, start, end, kw, cap, sw):
    sws = S.sw_weight(sw[0], kw['sweep_mode']) if kw['sweep'] else None
    return B.simulate(D, start, end, kw['alloc'], cap, seed_rank_cache={}, slots=kw['slots'], pick=kw['pick'],
                      gap_skip=kw['gap'] / 100 if kw['gap'] else None, sweep_etf=sw if kw['sweep'] else None, sw_signal=sws,
                      sw_overnight=kw['sweep'] and kw['sweep_mode'] == 'night', hold=kw['hold'] or None)


def _stat(curve, base=None):
    c = curve.dropna()
    if len(c) < 2:
        return {'days': len(c), 'ret': None}
    r = c.pct_change().dropna()
    out = {'days': len(c) - 1, 'ret': round((c.iloc[-1] / c.iloc[0] - 1) * 100, 2), 'mdd': round(float((c / c.cummax() - 1).min()) * 100, 2),
           'sharpe': round(float(r.mean() / r.std() * np.sqrt(250)), 2) if len(r) > 5 and r.std() > 0 else None,
           'curve': [[d, round(float(v / c.iloc[0] * 100), 2)] for d, v in c.items()]}
    if base is not None and base.get('ret') is not None:
        out['vs'] = round(out['ret'] - base['ret'], 2)
    return out


def run(cfg, progress=None):
    """그림자 운용 계산 → 결과 저장 (앞으로 · 최근 60거래일)"""
    if STATE['running']:
        return None
    STATE.update(running=True, err='', msg='그림자 운용 계산 시작')
    say = progress or (lambda m: STATE.update(msg=m))
    t0 = time.time()
    try:
        days = db.trading_days('0', '99999999')
        if len(days) < 80:
            raise RuntimeError('일봉 자료가 모자람')
        last = days[-1]
        start = db.gmeta_get('shadow_start')
        if not start:
            start = last
            db.gmeta_set('shadow_start', start)
        r60 = days[-61]
        frm = min(start, r60)
        D = B.load(frm, last)
        sw = B.sweep_series(D, frm, last)
        cap = 10_000_000
        out = {'made': db.now_s(), 'start': start, 'last': last, 'recent_from': r60, 'rows': [], 'proxy': bool(D.get('on_proxy') or D.get('sw_proxy'))}
        base_f = base_r = None
        for name, kw in variants(cfg):
            say(f'그림자 운용 · {name}')
            rr = _sim(D, r60, last, kw, cap, sw)
            stat_r = _stat(rr['curve'], base_r)
            stat_f = _stat(_sim(D, start, last, kw, cap, sw)['curve'], base_f) if start < last else {'days': 0, 'ret': None}
            if base_r is None:
                base_r, base_f = stat_r, stat_f
            out['rows'].append({'name': name, 'forward': stat_f, 'recent': stat_r,
                                'setting': {k: (v if not isinstance(v, dict) else {kk: (list(vv) if isinstance(vv, tuple) else vv) for kk, vv in v.items()})
                                            for k, v in kw.items()}})
        json.dump(out, open(RESULT, 'w', encoding='utf-8'), ensure_ascii=False)
        db.log(f"👥 그림자 운용 {len(out['rows'])}개 설정 · 앞으로 {start}~ · 최근 60일 ({time.time() - t0:.0f}초)")
        STATE['msg'] = f'끝 · {time.time() - t0:.0f}초'
        return out
    except Exception as e:
        STATE['err'] = str(e)[:200]
        db.log(f'그림자 운용 오류: {str(e)[:200]}', 'warn')
        return None
    finally:
        STATE['running'] = False


def result():
    try:
        return json.load(open(RESULT, encoding='utf-8'))
    except Exception:
        return None
