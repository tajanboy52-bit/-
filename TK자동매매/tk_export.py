"""
tk_export.py — 📦 모든 데이터 한 번에 저장 (기간 · 항목 고르기 · 첨부용 크기로 나눔)

· 항목: 시장 일봉 · 수급 · ETF · 월 자료(지수 구성 · 재무 · 종목 목록) · 신호 후보 · DART 공시 · 거래 장부(모의 · 실전) · 결과/보고서 · 1분봉
· 기간: frm~to (날짜가 있는 자료만 거름 · 종목 목록 같은 표는 통째로) — '지난 저장 이후'로 새 날짜만 이어서 받을 수 있음
· 저장: 저장 폴더\\tk_all_기간_시각_p01.zip, p02 … (각 part_mb 이하 · 큰 표는 달/날 단위 CSV로 쪼개서 넣음 · 조각마다 따로 열림)
· 비밀 값(앱키 · 시크릿 · 계좌번호 · 토큰 · KRX · 텔레그램 · DART 키)은 절대 넣지 않음
"""
import csv
import io
import json
import os
import time
import zipfile
from datetime import datetime

import tk_db as db

PARTS = {'market': '시장 일봉', 'flows': '수급', 'etf': 'ETF 일봉', 'monthly': '월 자료 · 종목 목록', 'cands': '신호 후보', 'dart': 'DART 공시',
         'ledger': '거래 장부 (모의 · 실전)', 'reports': '결과 · 보고서 · 로그', 'minute': '1분봉'}
STATE = {'running': False, 'msg': '', 'err': '', 'pct': 0, 'paths': [], 'mb': 0.0, 'folder': '', 'started': 0.0}


class RollingZip:
    """조각 zip — 파일을 넣다가 limit에 가까워지면 다음 조각으로 (파일 하나는 쪼개지 않음 → 큰 표는 미리 잘게)"""

    def __init__(self, base, limit_mb):
        self.base, self.lim, self.n, self.z, self.paths, self.files = base, float(limit_mb or 0) * 1e6, 0, None, [], []
        self._new()

    def _new(self):
        if self.z:
            self.z.writestr('README.txt', f'TK자동매매 전체 데이터 · {self.n}번째 조각 (파일 {len(self.cur)}개) · 전체 목록은 마지막 조각 README\n')
            self.z.close()
            os.replace(self.tmp, self.final)
            self.paths.append(self.final)
        self.n += 1
        self.final = f'{self.base}_p{self.n:02d}.zip' if self.lim else f'{self.base}.zip'
        self.tmp = self.final + '.part'
        self.z = zipfile.ZipFile(self.tmp, 'w', zipfile.ZIP_DEFLATED)
        self.cur = []

    def add(self, name, text):
        if self.lim and self.cur and os.path.getsize(self.tmp) + len(text) * 0.3 > self.lim * 0.95:
            self._new()
        self.z.writestr(name, text)
        self.cur.append(name)
        self.files.append((self.n, name))

    def close(self, readme):
        self.z.writestr('README.txt', readme + f'\n이 조각: {self.n}번째 · 파일 {len(self.cur)}개\n')
        self.z.close()
        os.replace(self.tmp, self.final)
        self.paths.append(self.final)
        if not self.lim:
            return self.paths
        return self.paths

    def abort(self):
        try:
            self.z.close()
            os.remove(self.tmp)
        except Exception:
            pass

    def size_mb(self):
        return round((sum(os.path.getsize(p) for p in self.paths if os.path.exists(p)) + (os.path.getsize(self.tmp) if os.path.exists(self.tmp) else 0)) / 1e6, 1)


def _csv(cur):
    s = io.StringIO()
    w = csv.writer(s)
    w.writerow([d[0] for d in cur.description])
    w.writerows(cur.fetchall())
    return '﻿' + s.getvalue()


def _months(days):
    out = {}
    for d in days:
        out.setdefault(d[:6], []).append(d)
    return out


def export_all(folder, frm='', to='', parts=None, part_mb=25, cfg=None):
    """→ [조각 경로] · 진행 STATE"""
    parts = [p for p in (parts or list(PARTS)) if p in PARTS]
    f, e = (frm or '0').replace('-', ''), (to or '99999999').replace('-', '')
    os.makedirs(folder, exist_ok=True)
    rng = f"{frm.replace('-', '') if frm else 'all'}{'-' + to.replace('-', '') if to else ''}"
    base = os.path.join(folder, f"tk_all_{rng}_{datetime.now():%Y%m%d_%H%M}")
    STATE.update(running=True, err='', msg='시작', pct=0, paths=[], mb=0.0, folder=folder, started=time.time())
    z = RollingZip(base, part_mb)
    m = db.mconn()
    days = [d for d in db.trading_days(f, e)]
    steps = len(parts)
    done = []
    try:
        def tick(i, msg):
            STATE.update(pct=int(i / steps * 100), msg=msg, mb=z.size_mb())
        for i, p in enumerate(parts):
            if p == 'market':
                for ym, ds in _months(days).items():
                    tick(i, f'시장 일봉 {ym}')
                    z.add(f'market/bars/{ym}.csv', _csv(m.execute(f"SELECT * FROM bars WHERE date BETWEEN ? AND ? ORDER BY date, ticker", (ds[0], ds[-1]))))
            elif p == 'flows':
                for ym, ds in _months(days).items():
                    tick(i, f'수급 {ym}')
                    z.add(f'market/flows/{ym}.csv', _csv(m.execute('SELECT * FROM flows WHERE date BETWEEN ? AND ? ORDER BY date, investor, ticker', (ds[0], ds[-1]))))
            elif p == 'etf':
                tick(i, 'ETF')
                z.add('market/etf.csv', _csv(m.execute('SELECT * FROM etf WHERE date BETWEEN ? AND ? ORDER BY ticker, date', (f, e))))
            elif p == 'monthly':
                tick(i, '월 자료')
                z.add('market/stocks.csv', _csv(m.execute('SELECT * FROM stocks ORDER BY ticker')))
                z.add('market/members.csv', _csv(m.execute('SELECT * FROM members WHERE month BETWEEN ? AND ? ORDER BY month', (f[:6], e[:6]))))
                z.add('market/monthly.csv', _csv(m.execute('SELECT * FROM monthly WHERE month BETWEEN ? AND ? ORDER BY month, ticker', (f[:6], e[:6]))))
            elif p == 'cands':
                tick(i, '신호 후보')
                z.add('research/cands.csv', _csv(m.execute('SELECT * FROM cands WHERE date BETWEEN ? AND ? ORDER BY date, sleeve, rank', (f, e))))
            elif p == 'dart':
                tick(i, 'DART 공시')
                import tk_dart as DART
                z.add('research/dart.csv', DART.export_csv(f, e))
            elif p == 'ledger':
                import tk_analyze as AN
                for mode in ('paper', 'real'):
                    x = db.conn(mode)
                    for t in sorted(set(AN.PACKAGE_TABLES) | {'signals', 'sleeve_daily', 'intraday', 'days'}):
                        try:
                            wh, args = AN._period_sql(x, t, f if frm else '', e if to else '')
                            cur = x.execute(f'SELECT * FROM {t}' + wh, args)
                        except Exception:
                            continue
                        tick(i, f'장부 {mode} · {t}')
                        z.add(f'ledger/{mode}/{t}.csv', _csv(cur))
            elif p == 'reports':
                tick(i, '결과 · 보고서')
                for fn in ('analysis_result.md', 'backtest_result.md', 'backtest_result.json', 'shadow_result.json', 'intraday_result.json'):
                    fp = os.path.join(db.DATA_DIR, fn)
                    if os.path.exists(fp):
                        z.add(f'reports/{fn}', open(fp, encoding='utf-8').read())
                z.add('reports/log.csv', _csv(m.execute("SELECT * FROM log WHERE replace(substr(ts,1,10),'-','') BETWEEN ? AND ? ORDER BY id", (f, e))))
                if cfg:
                    import tk_config as CF
                    safe = {k: v for k, v in cfg.items() if k not in ('accounts',) + CF.GLOBAL_SECRETS and 'key' not in k.lower() and 'secret' not in k.lower()}
                    z.add('reports/settings.json', json.dumps(safe, ensure_ascii=False, indent=1, default=str))
            elif p == 'minute':
                import tk_minute as MN
                c = MN.conn()
                md = [r[0] for r in c.execute('SELECT DISTINCT date FROM bars WHERE date BETWEEN ? AND ? ORDER BY date', (f, e))]
                for k, d in enumerate(md):
                    STATE.update(pct=int((i + (k + 1) / max(1, len(md))) / steps * 100), msg=f'1분봉 {d} ({k + 1}/{len(md)}일)', mb=z.size_mb())
                    z.add(f'minute/bars/{d}.csv', _csv(c.execute('SELECT ticker, hm, open, high, low, close, vol, amt FROM bars WHERE date=? ORDER BY ticker, hm', (d,))))
                if md:
                    z.add('minute/universe.csv', _csv(c.execute('SELECT * FROM universe WHERE date BETWEEN ? AND ? ORDER BY date, rank', (md[0], md[-1]))))
            done.append(PARTS[p])
        readme = (f"TK자동매매 전체 데이터 · 기간 {frm or '처음'} ~ {to or '끝'} · 만든 시각 {db.now_s()}\n"
                  f"항목: {' · '.join(done)}\n폴더: market/ (일봉 · 수급 · ETF · 월 자료) · research/ (신호 후보 · DART) · ledger/ (모의 · 실전 장부) · reports/ · minute/\n"
                  "비밀 값(앱키 · 시크릿 · 계좌번호 · 토큰 · 각종 키)은 들어 있지 않습니다. 조각은 하나씩 따로 열 수 있습니다.\n")
        paths = z.close(readme)
        db.gmeta_set('last_export_to', (to or datetime.now().strftime('%Y-%m-%d')).replace('-', ''))
        STATE.update(paths=paths, pct=100, mb=round(sum(os.path.getsize(p) for p in paths) / 1e6, 1),
                     msg=f"끝 · {len(paths)}개 조각 · {' · '.join(done)}")
        db.log(f"📦 모든 데이터 저장 {len(paths)}개 · {STATE['mb']}MB · {folder}")
        return paths
    except Exception as ex:
        z.abort()
        STATE['err'] = str(ex)[:200]
        db.log(f'모든 데이터 저장 실패: {str(ex)[:200]}', 'warn')
        raise
    finally:
        STATE['running'] = False
