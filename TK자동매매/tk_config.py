"""
tk_config.py — TK자동매매 설정 · 비밀 값 보관

· 설정 파일 %APPDATA%\\TKAuto\\tk_config.json
· 비밀 값(앱키 · 시크릿 · 계좌 · HTS ID · KRX 비밀번호 · 텔레그램 토큰)은 윈도우 DPAPI로 암호화해 저장
  → 이 PC의 이 윈도우 사용자만 풀 수 있음 (파일이 새어 나가도 다른 PC에서는 못 씀). 윈도우가 아니면 base64(경고 표시)
· 모의(paper)와 실전(real) 키 · 계좌를 따로 보관 · 지금 모드는 'mode'
"""
import base64
import ctypes
import json
import os
import sys

import tk_db as db

CONFIG_FILE = os.path.join(db.DATA_DIR, 'tk_config.json')
ACCOUNT_KEYS = ('app_key', 'app_secret', 'account', 'hts_id')
GLOBAL_SECRETS = ('krx_id', 'krx_pw', 'telegram_token', 'telegram_chat')
DEFAULT = {'mode': 'paper', 'kis_on': False, 'cap': 10_000_000, 'alloc': {'LVH': 40, 'REV': 25, 'DV': 20, 'ON': 15},
           'accounts': {'paper': {}, 'real': {}}, 'ws_on': True, 'dd_limit': 15, 'day_loss_limit': 4, 'pause_buy': False,
           'hourly_report': True, 'collect_time': '15:50', 'signal_time': '18:40', 'real_ramp': [30, 60, 100], 'real_ramp_days': 20,
           'min_paper_days': 60}


# ── DPAPI ──
class _Blob(ctypes.Structure):
    _fields_ = [('cbData', ctypes.c_uint32), ('pbData', ctypes.POINTER(ctypes.c_char))]


def _dpapi(data, protect=True):
    crypt32, kernel32 = ctypes.windll.crypt32, ctypes.windll.kernel32
    buf = ctypes.create_string_buffer(data, len(data))
    src, dst = _Blob(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_char))), _Blob()
    fn = crypt32.CryptProtectData if protect else crypt32.CryptUnprotectData
    if not fn(ctypes.byref(src), None, None, None, None, 0x01, ctypes.byref(dst)):         # CRYPTPROTECT_UI_FORBIDDEN
        raise OSError('DPAPI 실패')
    try:
        return ctypes.string_at(dst.pbData, dst.cbData)
    finally:
        kernel32.LocalFree(dst.pbData)


def enc(v):
    if not v:
        return ''
    b = str(v).encode('utf-8')
    if sys.platform == 'win32':
        try:
            return 'dpapi:' + base64.b64encode(_dpapi(b, True)).decode()
        except Exception:
            pass
    return 'b64:' + base64.b64encode(b).decode()


def dec(v):
    if not v or not isinstance(v, str):
        return v or ''
    try:
        if v.startswith('dpapi:'):
            return _dpapi(base64.b64decode(v[6:]), False).decode('utf-8')
        if v.startswith('b64:'):
            return base64.b64decode(v[4:]).decode('utf-8')
    except Exception:
        return ''
    return v


def protected():
    return sys.platform == 'win32'


def load():
    c = json.loads(json.dumps(DEFAULT))
    for p in (CONFIG_FILE, CONFIG_FILE + '.bak'):
        if os.path.exists(p):
            try:
                raw = json.load(open(p, encoding='utf-8-sig'))
                c.update({k: v for k, v in raw.items() if k != 'accounts'})
                for m in ('paper', 'real'):
                    c['accounts'][m] = {k: dec(v) for k, v in (raw.get('accounts', {}).get(m) or {}).items()}
                for k in GLOBAL_SECRETS:
                    c[k] = dec(raw.get(k, ''))
                break
            except Exception:
                pass
    _register(c)
    return c


def save(c):
    raw = {k: v for k, v in c.items() if k not in ('accounts',) + GLOBAL_SECRETS}
    raw['accounts'] = {m: {k: enc(v) for k, v in (c['accounts'].get(m) or {}).items() if v} for m in ('paper', 'real')}
    for k in GLOBAL_SECRETS:
        raw[k] = enc(c.get(k, ''))
    tmp = CONFIG_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(raw, f, ensure_ascii=False, indent=1)
    os.replace(tmp, CONFIG_FILE)
    try:
        import shutil
        shutil.copyfile(CONFIG_FILE, CONFIG_FILE + '.bak')
    except Exception:
        pass
    _register(c)


def _register(c):
    """로그 · 오류 메시지에서 가릴 값"""
    vals = [c.get(k) for k in GLOBAL_SECRETS]
    for m in ('paper', 'real'):
        vals += [c['accounts'].get(m, {}).get(k) for k in ACCOUNT_KEYS]
    db.SECRETS[:] = [v for v in vals if v and len(str(v)) > 3]


def acct(c, mode=None):
    return c['accounts'].get(mode or c.get('mode', 'paper')) or {}


def mask(v, keep=4):
    v = str(v or '')
    return '' if not v else '●' * min(8, max(4, len(v) - keep)) + (v[-keep:] if len(v) > keep + 2 else '')


def clean(msg):
    m = str(msg)
    for s in db.SECRETS:
        m = m.replace(str(s), '●●●●')
    return m[:300]
