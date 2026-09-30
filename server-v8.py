# -*- coding: utf-8 -*-
"""
=== AI 주식 자동매매 시스템 서버 v8.0 ===
한국투자증권 OpenAPI + GPT-4o/Claude AI 연동
FastAPI + WebSocket 실시간 통신
AI 매도 고도화 + 고수익 단타 전문 시스템

v8.0 변경사항 (v7.0 → v8.0):
  [CORE] v7.0 전체 기능 보존 (단타 매매 엔진 100% 유지)
  [DEL]  중장기(swing) 탭 삭제 — HTML rSwing 898줄 제거
  [DEL]  AI채팅(chat) 탭 삭제 — HTML rChat 1,136줄 제거
  [DEL]  서버 swing 매도엔진 270줄 비활성화 (_check_rules 경량화)
  [DEL]  서버 swing 스캔 비활성화 (_rules_loop 경량화)
  [NEW]  swing_tickers → auto_tickers 자동 이관 (기존 중장기 종목 단타 관리)
  [NEW]  단타 탭에서 모든 보유종목 통합 표시 (swing 필터 제거)
  [OPT]  HTML 5,178줄 → ~3,200줄 (38% 감소) — 렌더링 속도 향상
  [OPT]  탭 6개 → 4개 (시스템구조/단타설정/AI자동매매/결과)
  [OPT]  _price_push_loop swing 종목 조회 제거 — KIS API 부하 감소
  [VER]  server-v8.py + stock-analyzer-v8.html + start-v8.bat 체계

v7.0 변경사항 (v6.0 → v7.0):
  [CORE] v6.0 전체 기능 100% 보존
  [VER]  하위호환: v6 URL 접근 시 자동 리다이렉트

v6.0 변경사항 (v5.0 → v6.0):
  [CORE] v5.0 전체 기능 100% 보존
  
  ★ AI 매수 미세조정 (3일 실거래 데이터 기반):
  [NEW]  장초반 09:05 전 매수 차단: 급등락 진정 대기 (스캔은 계속, 매수만 홀드)
         → 3일 데이터: 09:00~09:04 = -₩41,577 vs 09:05~ = +₩4,065 흑자전환
  [NEW]  확신도 최소 기준 70% → 75% 상향: 약한 종목 필터링 강화
         → 3/19 시뮬: 18건+₩14,689(50%) → 9건+₩26,260(67%) 예상
  
  ★ 설정탭에서 사용자 직접 조정 권장:
  - max_positions: 5~7 권장 (적게 매수, 금액 크게)
  - max_buy_amount: 총자본의 10~15% 권장
  
  ★ AI 매도 학습 데이터:
  [NEW]  매도 시 stock_data 수집: 시가/고가/저가/전일가/거래량/체결강도
  [NEW]  매수시점/보유시간/고점대비/시가대비 기록
  [NEW]  15:35 종가 업데이트: 매도 후 변동% (너무 일찍/늦게 팔았는지)
  
  ★ 시스템 개선:
  [NEW]  거래비용 0.3% 반영: 순수익 표시
  [NEW]  시간별 텔레그램 리포트: 9:30~15:30 (7회)
  [NEW]  실적 AI 주입: 매 스캔마다 직전 실적 인지
  [NEW]  장마감 1시간 전 매수 스캔 중지
  [NEW]  날짜 기반 daily_briefing 완전 리셋
  [NEW]  결과탭 실적리포트 서브탭
  [NEW]  다운로드 리포트 날짜 필터 적용
  [FIX]  글로벌 브리핑 트리거 윈도우 확대 (820~1520)
  [FIX]  스캔 추천없음 시 사유 표시

사용법:
  pip install fastapi uvicorn websockets
  python server-v8.py

필요 정보:
  - 한국투자증권 APP_KEY, APP_SECRET, 계좌번호
  - OpenAI API Key 또는 Anthropic API Key
"""

import json
import time
import urllib.request
import urllib.parse
import webbrowser
import os
import sys

# ★ Windows 콘솔 한글 깨짐 방지 (UTF-8 강제 설정)
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except: pass

import threading
import asyncio
from datetime import datetime, timedelta
from urllib.error import URLError, HTTPError

# FastAPI + WebSocket
try:
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
    from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
    from fastapi.staticfiles import StaticFiles
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
except ImportError:
    print("❌ FastAPI/uvicorn 미설치! 다음 명령어를 실행하세요:")
    print("   pip install fastapi uvicorn websockets")
    sys.exit(1)

PORT = 8080

# ============= FastAPI App =============
app = FastAPI(title="태경 AI 자동매매 v8.0", version="7.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ============= WebSocket 관리 =============
connected_clients: set = set()
_event_loop = None  # uvicorn 이벤트루프 참조

async def broadcast(event: str, data: dict):
    """연결된 모든 WebSocket 클라이언트에 이벤트 전송"""
    if not connected_clients:
        return
    message = json.dumps({"event": event, "data": data}, ensure_ascii=False, default=str)
    dead = set()
    for client in connected_clients.copy():
        try:
            await client.send_text(message)
        except Exception:
            dead.add(client)
    connected_clients -= dead

def sync_broadcast(event: str, data: dict):
    """동기 스레드(AutoTrader)에서 async broadcast 호출
    WebSocket이 끊겨도 매매는 계속됨 (안전)"""
    try:
        loop = _event_loop
        if loop and loop.is_running():
            asyncio.run_coroutine_threadsafe(broadcast(event, data), loop)
    except Exception:
        pass  # WebSocket 없어도 매매는 계속

# ============= KIS API CONFIG =============
KIS_PROD_URL = "https://openapi.koreainvestment.com:9443"

# In-memory state (saved to file for persistence)
STATE_FILE_LIVE = "trading_state_live.json"
STATE_FILE = "trading_state.json"  # 구버전 마이그레이션용
token_store = {}  # {mode: {token, expires}}
_state_lock = __import__('threading').Lock()  # 동시성 보호
auto_rules = []   # auto-trading rules
trade_log = []    # trade execution log
auto_tickers = [] # tickers bought by auto-trading
swing_tickers = []  # v8: always empty (state file compat)
swing_config = {}   # v8: disabled
swing_avg_count = {}
auto_avg_count = {}
swing_running = False
swing_sell_stage = {}
swing_buy_routes = {}
holding_evaluations = {}
auto_avg_last_ts = {}
swing_dip_flag = {}
pending_cfg  = {}
api_usage = {'month': '', 'input_tokens': 0, 'output_tokens': 0, 'calls': 0}
monitoring = {"active": False, "interval": 10}

# ── 글로벌 잔고 캐시 (프론트 대시보드 조회 결과를 AI 매수 로직이 공유)
_bal_cache = {'data': None, 'ts': 0}

# ── 현재가 캐시 (paper/status 반복 KIS 호출 방지) {ticker: (price, timestamp)}
_price_cache = {}
PRICE_CACHE_TTL = 30  # 화면표시용 캐시 30초 (감시루프는 캐시 무시하고 실시간 조회)

# ── 영구 블랙리스트
perm_blocked = {}

def _is_truly_halted(ticker):
    """진짜 거래정지 종목인지 (스캔제외와 구분)"""
    v = perm_blocked.get(ticker, '')
    return v.startswith('HALTED:')

# ── 텔레그램 알림 설정
telegram_config = {'token': '', 'chat_id': '', 'enabled': False,
                   'on_buy': True, 'on_sell': True, 'on_briefing': True, 'on_error': True}

def tg_send(msg, parse_mode='HTML', buttons=None):
    """텔레그램 메시지 전송 (비차단, 인라인 버튼 지원)"""
    if not telegram_config.get('enabled') or not telegram_config.get('token') or not telegram_config.get('chat_id'):
        return
    def _send():
        try:
            token = telegram_config['token']
            chat_id = telegram_config['chat_id']
            payload = {'chat_id': chat_id, 'text': msg, 'parse_mode': parse_mode}
            if buttons:
                payload['reply_markup'] = json.dumps({
                    'inline_keyboard': [[{'text': b[0], 'callback_data': b[1]} for b in row] for row in buttons]
                })
            data = json.dumps(payload).encode('utf-8')
            url = f'https://api.telegram.org/bot{token}/sendMessage'
            req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
            with urllib.request.urlopen(req, timeout=10) as r:
                result = json.loads(r.read().decode('utf-8'))
                if not result.get('ok'):
                    print(f"[TG] ❌ 전송 실패: {result}")
        except Exception as e:
            print(f"[TG] ❌ 오류: {e}")
    threading.Thread(target=_send, daemon=True).start()

def tg_buy(name, ticker, qty, price, confidence=0, reason='', trade_mode='auto'):
    """매수 알림"""
    if not telegram_config.get('on_buy'): return
    msg = (f"🟢 <b>매수 체결</b>\n"
           f"━━━━━━━━━━━━━━\n"
           f"종목: <b>{name}</b>({ticker})\n"
           f"수량: {qty}주 | {'시장가' if not price else f'₩{int(price):,}'}\n"
           f"금액: ₩{int(qty*price):,}\n"
           f"{'확신도: '+str(confidence)+'%' if confidence else ''}\n"
           f"{'사유: '+reason if reason else ''}\n"
           f"⏰ {datetime.now().strftime('%H:%M:%S')}")
    tg_send(msg)

def tg_avg_down(name, ticker, qty, price, avg_cnt, pnl_pct, hold_qty):
    """물타기 매수 알림"""
    if not telegram_config.get('on_buy'): return
    msg = (f"💧 <b>물타기 매수 ({avg_cnt}차)</b>\n"
           f"━━━━━━━━━━━━━━\n"
           f"종목: <b>{name}</b>({ticker})\n"
           f"추가: {qty}주 × ₩{int(price):,} = ₩{int(qty*price):,}\n"
           f"기존보유: {hold_qty}주 | 현재손익: {pnl_pct:+.1f}%\n"
           f"→ 물타기 후 평단 하락, 반등 시 수익 극대화\n"
           f"⏰ {datetime.now().strftime('%H:%M:%S')}")
    tg_send(msg)

def tg_sell(name, ticker, qty, price, pnl_pct=0, pnl_amt=0, sell_type='', trade_mode='auto'):
    """매도 알림 — 익절/손절 시각 차별화"""
    if not telegram_config.get('on_sell'): return
    label = {'tp1':'1차익절','tp2':'2차익절','tp3':'3차익절','sl':'손절','trailing':'트레일링',
             'ai_sell':'AI매도','force_close':'장마감청산','chat':'채팅매도','manual':'수동매도'}.get(sell_type, sell_type)
    # ★ v8.0: 익절/손절 시각 차별화
    if pnl_amt >= 0:
        emoji = '💰'
        header = f"💰 <b>익절 매도</b> [{label}]"
        result_line = f"🎉 수익: <b>+{pnl_pct:.1f}%</b> (+₩{int(pnl_amt):,})"
    else:
        emoji = '🔻'
        header = f"🔻 <b>손절 매도</b> [{label}]"
        result_line = f"💸 손실: <b>{pnl_pct:.1f}%</b> (₩{int(pnl_amt):,})"
    msg = (f"{header}\n"
           f"━━━━━━━━━━━━━━\n"
           f"종목: <b>{name}</b>({ticker})\n"
           f"수량: {qty}주 | ₩{int(price):,}\n"
           f"{result_line}\n"
           f"⏰ {datetime.now().strftime('%H:%M:%S')}")
    tg_send(msg)

def tg_briefing(title, summary):
    """브리핑 알림"""
    if not telegram_config.get('on_briefing'): return
    msg = (f"📊 <b>{title}</b>\n{summary}\n⏰ {datetime.now().strftime('%H:%M')}")
    tg_send(msg)
    # ★ v3.0: WebSocket 브리핑 이벤트 push
    sync_broadcast('briefing', {
        'title': title, 'summary': summary[:300],
        'time': datetime.now().strftime('%H:%M:%S')
    })

def tg_error(msg_text):
    """에러 알림"""
    if not telegram_config.get('on_error'): return
    tg_send(f"⚠️ <b>시스템 오류</b>\n{msg_text}\n⏰ {datetime.now().strftime('%H:%M')}")

# ★ v3.0 G: 텔레그램 차트 이미지 전송
def tg_send_photo(image_path, caption=''):
    """텔레그램에 이미지 파일 전송"""
    token = telegram_config.get('token', '')
    chat_id = telegram_config.get('chat_id', '')
    if not token or not chat_id:
        return
    try:
        import io
        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        boundary = '----TKBoundary'
        body = b''
        # chat_id
        body += f'--{boundary}\r\nContent-Disposition: form-data; name="chat_id"\r\n\r\n{chat_id}\r\n'.encode()
        # caption
        if caption:
            body += f'--{boundary}\r\nContent-Disposition: form-data; name="caption"\r\n\r\n{caption}\r\n'.encode()
            body += f'--{boundary}\r\nContent-Disposition: form-data; name="parse_mode"\r\n\r\nHTML\r\n'.encode()
        # photo file
        with open(image_path, 'rb') as f:
            photo_data = f.read()
        body += f'--{boundary}\r\nContent-Disposition: form-data; name="photo"; filename="chart.png"\r\nContent-Type: image/png\r\n\r\n'.encode()
        body += photo_data
        body += f'\r\n--{boundary}--\r\n'.encode()
        
        req = urllib.request.Request(url, data=body,
            headers={'Content-Type': f'multipart/form-data; boundary={boundary}'})
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
        if result.get('ok'):
            print(f"[TG] 차트 이미지 전송 완료")
        return result
    except Exception as e:
        print(f"[TG] 차트 이미지 전송 실패: {e}")

def generate_pnl_chart():
    """일별 수익 차트 생성 (matplotlib) → PNG 파일 경로 반환"""
    try:
        import matplotlib
        matplotlib.use('Agg')  # 비GUI 백엔드
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from collections import defaultdict
        
        today = datetime.now().strftime('%Y-%m-%d')
        cutoff = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        # 날짜별 실현손익 집계
        daily_pnl = defaultdict(float)
        for t in trade_log:
            if t.get('type') in ('SELL','FORCE_CLOSE','AI_SELL') and t.get('success'):
                d = t.get('date','') or (t.get('time','') or '')[:10]
                if d >= cutoff:
                    daily_pnl[d] += float(t.get('pnl',0) or 0)
        
        if len(daily_pnl) < 2:
            return None
        
        dates = sorted(daily_pnl.keys())
        values = [int(daily_pnl[d]) for d in dates]
        cumulative = []
        s = 0
        for v in values:
            s += v
            cumulative.append(s)
        
        # 차트 생성
        fig, ax1 = plt.subplots(figsize=(8, 4), dpi=100)
        fig.patch.set_facecolor('#0a0e17')
        ax1.set_facecolor('#0a0e17')
        
        x = range(len(dates))
        colors = ['#00d68f' if v >= 0 else '#ff4757' for v in values]
        ax1.bar(x, values, color=colors, alpha=0.6, width=0.6)
        
        # 누적 곡선
        ax2 = ax1.twinx()
        ax2.plot(x, cumulative, color='#2d7ff9', linewidth=2, marker='o', markersize=3)
        ax2.set_facecolor('#0a0e17')
        
        # 스타일
        for ax in [ax1, ax2]:
            ax.tick_params(colors='#8b99b4', labelsize=8)
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_color('#1e2d48')
            ax.spines['left'].set_color('#1e2d48')
            ax.spines['bottom'].set_color('#1e2d48')
        
        ax1.set_xticks(x[::max(1, len(x)//6)])
        ax1.set_xticklabels([dates[i][5:] for i in range(0, len(dates), max(1, len(dates)//6))],
                           rotation=45, color='#8b99b4', fontsize=7)
        ax1.set_ylabel('일별 손익', color='#8b99b4', fontsize=9)
        ax2.set_ylabel('누적 수익', color='#2d7ff9', fontsize=9)
        ax1.axhline(y=0, color='#1e2d48', linewidth=0.5, linestyle='--')
        
        # 마지막 누적 수익 표시
        c = '#00d68f' if cumulative[-1] >= 0 else '#ff4757'
        sign = '+' if cumulative[-1] >= 0 else ''
        ax2.annotate(f'{sign}{cumulative[-1]:,}', xy=(len(x)-1, cumulative[-1]),
                    fontsize=10, fontweight='bold', color=c,
                    xytext=(5, 10), textcoords='offset points')
        
        plt.title(f'TK AutoTrader 수익 ({dates[0][5:]}~{dates[-1][5:]})',
                 color='#e8ecf4', fontsize=11, fontweight='bold', pad=10)
        plt.tight_layout()
        
        # 파일 저장
        chart_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'daily_chart.png')
        plt.savefig(chart_path, facecolor='#0a0e17', edgecolor='none')
        plt.close()
        
        print(f"[CHART] 차트 생성 완료: {chart_path}")
        return chart_path
    except ImportError:
        print("[CHART] matplotlib 미설치 — pip install matplotlib")
        return None
    except Exception as e:
        print(f"[CHART] 차트 생성 실패: {e}")
        return None

# ── 텔레그램 원격 명령어 (외부 제어)
_tg_last_update_id = 0
def tg_poll_commands():
    """텔레그램에서 명령어 확인 (/status, /stop, /resume)"""
    global _tg_last_update_id
    if not telegram_config.get('enabled') or not telegram_config.get('token'):
        return None
    try:
        token = telegram_config['token']
        url = f'https://api.telegram.org/bot{token}/getUpdates?offset={_tg_last_update_id+1}&timeout=1&limit=5'
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=5) as r:
            data = json.loads(r.read().decode('utf-8'))
        if not data.get('ok'): return None
        for upd in data.get('result', []):
            _tg_last_update_id = upd['update_id']
            # ★ 인라인 버튼 클릭 처리
            cb = upd.get('callback_query')
            if cb:
                cb_data = cb.get('data', '')
                cb_chat = str(cb.get('message', {}).get('chat', {}).get('id', ''))
                cb_id = cb.get('id', '')
                if cb_chat != telegram_config.get('chat_id', ''): continue
                # 버튼 클릭 응답 (로딩 해제)
                try:
                    _ans_url = f"https://api.telegram.org/bot{token}/answerCallbackQuery?callback_query_id={cb_id}"
                    urllib.request.urlopen(urllib.request.Request(_ans_url), timeout=5)
                except: pass
                if cb_data == 'cmd_restart':
                    if hasattr(auto_trader, 'running') and auto_trader.running:
                        tg_send("ℹ️ 이미 가동 중입니다.")
                    elif hasattr(auto_trader, 'config') and auto_trader.config:
                        try:
                            auto_trader.start(auto_trader.config)
                            tg_send("🤖 <b>자동매매 재시작 완료!</b>\n이전 설정 그대로 적용\n⏰ " + datetime.now().strftime('%H:%M'))
                        except Exception as e:
                            tg_send(f"❌ 재시작 실패: {e}")
                    else:
                        tg_send("❌ 이전 설정이 없습니다.\n웹 UI에서 시작해주세요.")
                elif cb_data == 'cmd_status':
                    _st = '🟢 가동 중' if (hasattr(auto_trader, 'running') and auto_trader.running) else '⏹ 중단'
                    _pos = len(auto_tickers)
                    _today = datetime.now().strftime('%Y-%m-%d')
                    _buys = len([t for t in trade_log if t.get('date') == _today and t.get('type') == 'AI_BUY' and t.get('success')])
                    _sells = len([t for t in trade_log if t.get('date') == _today and t.get('type') == 'SELL' and t.get('success')])
                    _pnl = sum(float(t.get('pnl',0) or 0) for t in trade_log if t.get('date') == _today and t.get('type') == 'SELL' and t.get('success'))
                    tg_send(f"📊 <b>현황</b>\n상태: {_st}\n보유: {_pos}종목\n오늘: 매수{_buys} 매도{_sells}\n손익: {'+'if _pnl>=0 else ''}₩{int(_pnl):,}",
                        buttons=[[('🔄 재시작','cmd_restart'),('⏸ 정지/재개','cmd_pause')]] if not (hasattr(auto_trader,'running') and auto_trader.running) else [[('⏸ 일시정지','cmd_pause'),('⏹ 중단','cmd_stop')]])
                elif cb_data == 'cmd_stop':
                    if hasattr(auto_trader, 'running') and auto_trader.running:
                        auto_trader.stop()
                        tg_send("⏹ <b>자동매매 중단됨</b>", buttons=[[('🔄 재시작','cmd_restart')]])
                elif cb_data == 'cmd_pause':
                    if hasattr(auto_trader, 'running') and auto_trader.running:
                        auto_trader.paused = not auto_trader.paused
                        st = '⏸ 일시정지' if auto_trader.paused else '▶️ 재개'
                        tg_send(f"{st} 완료")
                # ★ v3.0: 포트폴리오 갱신 버튼
                elif cb_data == 'cmd_portfolio':
                    # /portfolio 명령과 동일 동작 (비동기)
                    threading.Thread(target=lambda: tg_poll_commands.__code__ and None, daemon=True).start()
                    # 간단 재조회
                    try:
                        _cfg3 = getattr(auto_trader, 'config', None) or {}
                        _ak3 = _cfg3.get('app_key') or telegram_config.get('app_key','')
                        _as3 = _cfg3.get('app_secret') or telegram_config.get('app_secret','')
                        _acct3 = _cfg3.get('account') or telegram_config.get('account','')
                        if _ak3 and _acct3:
                            _token3 = kis_get_token(_ak3, _as3, 'live')
                            bal3 = get_balance(_ak3, _as3, 'live', _token3, _acct3, '01', max_age=5)
                            _pos3 = [p for p in bal3.get('output1',[]) if int(p.get('hldg_qty','0') or 0) > 0]
                            _out2_3 = (bal3.get('output2',[{}]) or [{}])[0] or {}
                            _msg3 = f"💼 <b>포트폴리오</b> ({len(_pos3)}종목)\n"
                            _msg3 += f"총평가: ₩{int(_out2_3.get('tot_evlu_amt','0') or 0):,}\n"
                            for _p3 in _pos3[:8]:
                                _r3 = float(_p3.get('evlu_pfls_rt','0') or 0)
                                _msg3 += f"{'🟢' if _r3>=0 else '🔴'} {_p3.get('prdt_name','?')} {'+'if _r3>=0 else ''}{_r3:.1f}%\n"
                            tg_send(_msg3)
                    except Exception as _e3:
                        tg_send(f"갱신 실패: {_e3}")
                elif cb_data.startswith('trade_'):
                    # trade_BUY_005930_2 → 매수 실행
                    parts = cb_data.split('_')
                    if len(parts) >= 4:
                        _type = parts[1]  # BUY or SELL
                        _tk = parts[2]    # ticker
                        _qty = int(parts[3])
                        _nm = get_stock_name_naver(_tk) or _tk
                        if not (hasattr(auto_trader, 'config') and auto_trader.config):
                            tg_send("❌ 자동매매 설정이 없습니다.\n웹에서 먼저 시작해주세요.")
                        else:
                            cfg = auto_trader.config
                            try:
                                _token = kis_get_token(cfg['app_key'], cfg['app_secret'], 'live')
                                if _type == 'BUY':
                                    auto_trader._execute_buy(cfg, _token, _tk, _nm, _qty, 0, f'텔레그램 매수')
                                    # ★ v8.0: 텔레그램 매수 → auto_tickers 통합
                                    if _tk not in auto_tickers:
                                        auto_tickers.append(_tk)
                                        swing_avg_count[_tk] = 0
                                    # v8: swing_buy_routes 사용 안 함
                                    for _tl in reversed(trade_log[-5:]):
                                        if _tl.get('ticker') == _tk and _tl.get('type') == 'AI_BUY':
                                            _tl['type'] = 'TG_BUY'
                                            _tl['trade_mode'] = 'swing'
                                            _tl['buy_route'] = 'telegram'
                                            break
                                    save_state()
                                    tg_send(f"✅ 📈텔레그램 매수 전송: {_nm}({_tk}) {_qty}주 시장가")
                                else:
                                    auto_trader._execute_sell(cfg, _token, _tk, _nm, _qty, 0, f'텔레그램 채팅 매도')
                                    tg_send(f"✅ 매도 주문 전송: {_nm}({_tk}) {_qty}주 시장가")
                            except Exception as e:
                                tg_send(f"❌ 주문 실패: {e}")
                continue
            msg = upd.get('message', {})
            text = (msg.get('text') or '').strip().lower()
            chat_id = str(msg.get('chat', {}).get('id', ''))
            # 본인 chat_id만 허용
            if chat_id != telegram_config.get('chat_id', ''): continue
            if text == '/status':
                _st = '🟢 가동 중' if (hasattr(auto_trader, 'running') and auto_trader.running) else '⏹ 대기 중'
                _pos = len(auto_tickers)
                _today = datetime.now().strftime('%Y-%m-%d')
                _buys = len([t for t in trade_log if t.get('date') == _today and t.get('type') == 'AI_BUY' and t.get('success')])
                _sells = len([t for t in trade_log if t.get('date') == _today and t.get('type') == 'SELL' and t.get('success')])
                _pnl = sum(float(t.get('pnl',0) or 0) for t in trade_log if t.get('date') == _today and t.get('type') == 'SELL' and t.get('success'))
                tg_send(f"📊 <b>시스템 상태</b>\n상태: {_st}\n보유: {_pos}종목\n오늘: 매수{_buys}건 매도{_sells}건\n실현손익: {'+'if _pnl>=0 else ''}₩{int(_pnl):,}\n⏰ {datetime.now().strftime('%H:%M')}")
            elif text == '/stop':
                if hasattr(auto_trader, 'running') and auto_trader.running:
                    auto_trader.stop()
                    tg_send("⏹ <b>자동매매 중단됨</b> (텔레그램 원격)\n재시작: 웹 UI에서 시작 버튼")
                else:
                    tg_send("ℹ️ 이미 중단 상태입니다.")
            elif text == '/resume' or text == '/pause':
                if hasattr(auto_trader, 'running') and auto_trader.running:
                    auto_trader.paused = not auto_trader.paused
                    st = '⏸ 일시정지' if auto_trader.paused else '▶️ 재개'
                    tg_send(f"{st} <b>자동매매 {'일시정지' if auto_trader.paused else '재개'}</b> (텔레그램 원격)")
                else:
                    tg_send("ℹ️ 자동매매가 실행 중이 아닙니다.\n웹 UI에서 시작해주세요.")
            elif text == '/restart' or text == '/start':
                if hasattr(auto_trader, 'running') and auto_trader.running:
                    tg_send("ℹ️ 이미 가동 중입니다.\n일시정지 상태라면 /pause 로 재개하세요.")
                elif hasattr(auto_trader, 'config') and auto_trader.config:
                    try:
                        auto_trader.start(auto_trader.config)
                        tg_send("🤖 <b>자동매매 재시작 완료</b> (텔레그램 원격)\n이전 설정 그대로 적용\n⏰ " + datetime.now().strftime('%H:%M'))
                    except Exception as e:
                        tg_send(f"❌ 재시작 실패: {e}")
                else:
                    tg_send("❌ 이전 설정이 없습니다.\n웹 UI에서 처음 시작해주세요.")
            elif text == '/help':
                tg_send("📋 <b>명령어 목록</b>\n"
                    "/status — 현재 상태+수익\n"
                    "/portfolio — 보유종목 상세 현황\n"
                    "/today — 오늘 매매 요약\n"
                    "/history — 최근 거래내역 10건\n"
                    "/report — 일일 수익 리포트\n"
                    "/restart — 자동매매 재시작\n"
                    "/stop — 자동매매 중단\n"
                    "/pause — 일시정지/재개\n"
                    "/help — 명령어 목록\n"
                    "\n💬 <b>간편 매수/매도</b>\n"
                    "/buy 삼성전자 3 — 삼성전자 3주 시장가 매수\n"
                    "/sell 005930 5 — 005930 5주 시장가 매도\n"
                    "\n💬 일반 메시지 = AI 채팅\n"
                    "예: 삼성전자 분석해줘\n"
                    "예: 시장 현황 알려줘")

            # ★ v3.0: /portfolio — 보유종목 상세 현황
            elif text == '/portfolio' or text == '/pos':
                try:
                    _cfg = getattr(auto_trader, 'config', None) or {}
                    _ak = _cfg.get('app_key') or telegram_config.get('app_key','')
                    _as = _cfg.get('app_secret') or telegram_config.get('app_secret','')
                    _acct = _cfg.get('account') or telegram_config.get('account','')
                    if not _ak or not _acct:
                        tg_send("❌ KIS 키가 없습니다. 웹에서 자동매매를 먼저 시작해주세요.")
                        continue
                    _token = kis_get_token(_ak, _as, 'live')
                    bal = get_balance(_ak, _as, 'live', _token, _acct, _cfg.get('account_cd','01'), max_age=10)
                    positions = [p for p in bal.get('output1', []) if int(p.get('hldg_qty','0') or 0) > 0]
                    out2 = (bal.get('output2', [{}]) or [{}])[0] or {}
                    tot_eval = int(out2.get('tot_evlu_amt','0') or 0)
                    cash = calc_ord_psbl_cash(bal)
                    tot_pnl = int(out2.get('evlu_pfls_smtl_amt','0') or 0)
                    
                    msg_text = f"💼 <b>보유종목 현황</b>\n"
                    msg_text += f"━━━━━━━━━━━━━━\n"
                    msg_text += f"총평가: ₩{tot_eval:,}\n예수금: ₩{cash:,}\n"
                    msg_text += f"평가손익: {'+'if tot_pnl>=0 else ''}₩{tot_pnl:,}\n"
                    msg_text += f"━━━━━━━━━━━━━━\n"
                    
                    if positions:
                        for i, p in enumerate(positions, 1):
                            _nm = p.get('prdt_name','?')
                            _tk = p.get('pdno','')
                            _qty = int(p.get('hldg_qty','0') or 0)
                            _avg = int(float(p.get('pchs_avg_pric','0') or 0))
                            _cur = int(p.get('prpr','0') or 0)
                            _pnl_r = float(p.get('evlu_pfls_rt','0') or 0)
                            _pnl_a = int(p.get('evlu_pfls_amt','0') or 0)
                            _emoji = '🟢' if _pnl_r >= 0 else '🔴'
                            _auto = ' [AI]' if _tk in auto_tickers else ''
                            msg_text += f"\n{_emoji} <b>{_nm}</b>{_auto}\n"
                            msg_text += f"   {_qty}주 | 평단 ₩{_avg:,} → 현재 ₩{_cur:,}\n"
                            msg_text += f"   {'+'if _pnl_r>=0 else ''}{_pnl_r:.2f}% ({'+'if _pnl_a>=0 else ''}₩{_pnl_a:,})\n"
                    else:
                        msg_text += "\n보유종목 없음 (전량 현금)\n"
                    
                    msg_text += f"\n⏰ {datetime.now().strftime('%H:%M:%S')}"
                    tg_send(msg_text, buttons=[[('📊 현황','cmd_status'),('🔄 갱신','cmd_portfolio')]])
                except Exception as e:
                    tg_send(f"❌ 포트폴리오 조회 실패: {e}")

            # ★ v3.0: /today — 오늘 매매 요약
            elif text == '/today':
                _today = datetime.now().strftime('%Y-%m-%d')
                _t_buys = [t for t in trade_log if t.get('date')==_today and t.get('type')=='AI_BUY' and t.get('success')]
                _t_sells = [t for t in trade_log if t.get('date')==_today and t.get('type') in ('SELL','FORCE_CLOSE') and t.get('success')]
                _t_blocked = [t for t in trade_log if t.get('date')==_today and t.get('type') in ('BLOCKED','CAPITAL_BLOCK')]
                _t_pnl = sum(float(t.get('pnl',0) or 0) for t in _t_sells)
                
                msg_text = f"📊 <b>오늘 매매 요약</b> ({_today})\n"
                msg_text += f"━━━━━━━━━━━━━━\n"
                msg_text += f"매수: {len(_t_buys)}건 | 매도: {len(_t_sells)}건 | 차단: {len(_t_blocked)}건\n"
                msg_text += f"실현손익: {'+'if _t_pnl>=0 else ''}₩{int(_t_pnl):,}\n"
                
                if _t_buys:
                    msg_text += f"\n🟢 <b>매수 내역</b>\n"
                    for t in _t_buys[-5:]:
                        msg_text += f"  {t.get('name','?')} {t.get('qty',0)}주 ₩{int(t.get('price',0)):,}\n"
                
                if _t_sells:
                    msg_text += f"\n🔴 <b>매도 내역</b>\n"
                    for t in _t_sells[-5:]:
                        _sp = float(t.get('pnl_pct',0) or 0)
                        msg_text += f"  {t.get('name','?')} {t.get('qty',0)}주 {'+'if _sp>=0 else ''}{_sp:.1f}%\n"
                
                msg_text += f"\n⏰ {datetime.now().strftime('%H:%M')}"
                tg_send(msg_text)

            # ★ v3.0: /history — 최근 거래내역 10건
            elif text == '/history':
                recent = [t for t in trade_log if t.get('type') in ('AI_BUY','SELL','CHAT_BUY','CHAT_SELL','FORCE_CLOSE') and t.get('success')][-10:]
                if not recent:
                    tg_send("📝 거래내역이 없습니다.")
                    continue
                msg_text = "📝 <b>최근 거래 10건</b>\n━━━━━━━━━━━━━━\n"
                for t in reversed(recent):
                    _dt = (t.get('time','') or '')[:16].replace('T',' ')
                    _type = '🟢매수' if 'BUY' in t.get('type','') else '🔴매도'
                    _pnl_str = ''
                    if 'SELL' in t.get('type','') or 'CLOSE' in t.get('type',''):
                        _pp = float(t.get('pnl_pct',0) or 0)
                        _pa = int(float(t.get('pnl',0) or 0))
                        _pnl_str = f" {'+'if _pp>=0 else ''}{_pp:.1f}% (₩{_pa:,})"
                    msg_text += f"{_dt}\n  {_type} {t.get('name','?')} {t.get('qty',0)}주{_pnl_str}\n"
                tg_send(msg_text)

            # ★ v3.0: /report — 일일 수익 리포트
            elif text == '/report':
                threading.Thread(target=_tg_daily_report, daemon=True).start()

            # ★ v3.0: /buy 종목명 수량 — 간편 매수
            elif text.startswith('/buy '):
                _parts = text.replace('/buy ', '').strip().split()
                if len(_parts) < 1:
                    tg_send("사용법: /buy 삼성전자 3\n또는: /buy 005930 5")
                    continue
                _query = _parts[0]
                _qty = int(_parts[1]) if len(_parts) >= 2 else 1
                # 종목코드 감지
                _ticker = ''
                if _query.isdigit() and len(_query) == 6:
                    _ticker = _query
                else:
                    # 네이버 검색
                    try:
                        _enc = urllib.parse.quote(_query)
                        _url = f'https://ac.stock.naver.com/ac?q={_enc}&target=stock'
                        _req = urllib.request.Request(_url, headers={'User-Agent':'Mozilla/5.0'})
                        with urllib.request.urlopen(_req, timeout=5) as _r:
                            _data = json.loads(_r.read().decode('utf-8'))
                        for _it in (_data.get('items') or [])[:3]:
                            if isinstance(_it, dict):
                                _c = str(_it.get('code','')).strip()
                                if _c and len(_c)==6 and _c.isdigit():
                                    _ticker = _c
                                    break
                    except: pass
                if not _ticker:
                    tg_send(f"❌ '{_query}' 종목을 찾을 수 없습니다.")
                    continue
                _nm = get_stock_name_naver(_ticker) or _ticker
                tg_send(f"🟢 매수 주문 확인\n{_nm}({_ticker}) {_qty}주 시장가",
                    buttons=[[
                        (f'✅ 매수 실행', f'trade_BUY_{_ticker}_{_qty}'),
                        ('❌ 취소', 'cmd_status')
                    ]])

            # ★ v3.0: /sell 종목명 수량 — 간편 매도
            elif text.startswith('/sell '):
                _parts = text.replace('/sell ', '').strip().split()
                if len(_parts) < 1:
                    tg_send("사용법: /sell 삼성전자 3\n또는: /sell 005930 전량")
                    continue
                _query = _parts[0]
                _qty_str = _parts[1] if len(_parts) >= 2 else '전량'
                _ticker = ''
                if _query.isdigit() and len(_query) == 6:
                    _ticker = _query
                else:
                    try:
                        _enc = urllib.parse.quote(_query)
                        _url = f'https://ac.stock.naver.com/ac?q={_enc}&target=stock'
                        _req = urllib.request.Request(_url, headers={'User-Agent':'Mozilla/5.0'})
                        with urllib.request.urlopen(_req, timeout=5) as _r:
                            _data = json.loads(_r.read().decode('utf-8'))
                        for _it in (_data.get('items') or [])[:3]:
                            if isinstance(_it, dict):
                                _c = str(_it.get('code','')).strip()
                                if _c and len(_c)==6 and _c.isdigit():
                                    _ticker = _c
                                    break
                    except: pass
                if not _ticker:
                    tg_send(f"❌ '{_query}' 종목을 찾을 수 없습니다.")
                    continue
                # 전량 매도 시 잔고 조회
                _qty = 0
                if _qty_str in ('전량','all','전부','모두'):
                    try:
                        _cfg2 = getattr(auto_trader, 'config', None) or {}
                        _ak2 = _cfg2.get('app_key') or telegram_config.get('app_key','')
                        _as2 = _cfg2.get('app_secret') or telegram_config.get('app_secret','')
                        _acct2 = _cfg2.get('account') or telegram_config.get('account','')
                        _token2 = kis_get_token(_ak2, _as2, 'live')
                        _bal2 = get_balance(_ak2, _as2, 'live', _token2, _acct2, '01', max_age=10)
                        for _p in _bal2.get('output1', []):
                            if _p.get('pdno') == _ticker:
                                _qty = int(_p.get('hldg_qty','0') or 0)
                                break
                    except: pass
                    if _qty <= 0:
                        tg_send(f"❌ {_ticker} 보유수량이 없습니다.")
                        continue
                else:
                    try: _qty = int(_qty_str)
                    except: _qty = 1
                _nm = get_stock_name_naver(_ticker) or _ticker
                tg_send(f"🔴 매도 주문 확인\n{_nm}({_ticker}) {_qty}주 시장가",
                    buttons=[[
                        (f'✅ 매도 실행', f'trade_SELL_{_ticker}_{_qty}'),
                        ('❌ 취소', 'cmd_status')
                    ]])
            elif not text.startswith('/'):
                # ★ 일반 메시지 → AI 채팅 처리 (비동기)
                _orig_text = (msg.get('text') or '').strip()  # 원본 (대소문자 유지)
                threading.Thread(target=_tg_chat_handler, args=(_orig_text,), daemon=True).start()
    except Exception as e:
        pass  # 폴링 실패는 무시 (네트워크 일시 불안정 등)

def _tg_daily_report():
    """★ v5.0: 텔레그램 일일 수익 리포트 (자동/수동 분리)"""
    try:
        _today = datetime.now().strftime('%Y-%m-%d')
        _t_all = [t for t in trade_log if t.get('date')==_today and t.get('success')]
        
        # 자동매매 (trade_mode != 'swing')
        _a_buys = [t for t in _t_all if t.get('type') in ('AI_BUY','BUY') and t.get('trade_mode') != 'swing']
        _a_sells = [t for t in _t_all if t.get('type') in ('SELL','FORCE_CLOSE','AI_SELL') and t.get('trade_mode') != 'swing']
        _a_pnl = sum(float(t.get('pnl',0) or 0) for t in _a_sells)
        _a_wins = len([t for t in _a_sells if float(t.get('pnl',0) or 0) > 0])
        _a_losses = len([t for t in _a_sells if float(t.get('pnl',0) or 0) < 0])
        _a_wr = round(_a_wins / max(_a_wins + _a_losses, 1) * 100)
        
        # 수동매매 (trade_mode == 'swing')
        _s_buys = [t for t in _t_all if t.get('type') in ('AI_BUY','BUY','CHAT_BUY','TG_BUY','SWING_BUY') and t.get('trade_mode') == 'swing']
        _s_sells = [t for t in _t_all if t.get('type') in ('SELL','CHAT_SELL') and t.get('trade_mode') == 'swing']
        _s_pnl = sum(float(t.get('pnl',0) or 0) for t in _s_sells)
        _s_wins = len([t for t in _s_sells if float(t.get('pnl',0) or 0) > 0])
        _s_losses = len([t for t in _s_sells if float(t.get('pnl',0) or 0) < 0])
        _s_wr = round(_s_wins / max(_s_wins + _s_losses, 1) * 100)
        
        # 합계
        _total_pnl = _a_pnl + _s_pnl
        
        msg = f"📊 <b>일일 수익 리포트</b>\n📅 {_today}\n━━━━━━━━━━━━━━━━━\n\n"
        
        # 🤖 자동매매
        msg += f"🤖 <b>단타 자동매매</b>\n"
        msg += f"  매수 {len(_a_buys)}건 | 매도 {len(_a_sells)}건 | 승률 {_a_wr}%\n"
        msg += f"  실현손익: {'+'if _a_pnl>=0 else ''}₩{int(_a_pnl):,}\n"
        msg += f"  보유: {len(auto_tickers)}종목\n\n"
        
        # 📈 수동매매
        # v8: 중장기 섹션 제거
        
        msg += f"━━━━━━━━━━━━━━━━━\n"
        msg += f"💎 <b>종합</b>: {'+'if _total_pnl>=0 else ''}₩{int(_total_pnl):,}\n"
        
        # 종목별 수익 TOP
        _stock_pnl = {}
        for t in _a_sells + _s_sells:
            _tk = t.get('ticker','')
            _nm = t.get('name', _tk)
            _mode = '📈' if t.get('trade_mode') == 'swing' else '🤖'
            if _tk not in _stock_pnl:
                _stock_pnl[_tk] = {'name': _nm, 'pnl': 0, 'cnt': 0, 'mode': _mode}
            _stock_pnl[_tk]['pnl'] += float(t.get('pnl',0) or 0)
            _stock_pnl[_tk]['cnt'] += 1
        
        if _stock_pnl:
            sorted_stocks = sorted(_stock_pnl.items(), key=lambda x: x[1]['pnl'], reverse=True)
            msg += f"\n<b>종목별 실현손익</b>\n"
            for _tk, _info in sorted_stocks[:8]:
                _emoji = '🟢' if _info['pnl'] >= 0 else '🔴'
                msg += f"  {_info['mode']}{_emoji} {_info['name']} {'+'if _info['pnl']>=0 else ''}₩{int(_info['pnl']):,} ({_info['cnt']}건)\n"
        
        msg += f"\n⏰ {datetime.now().strftime('%H:%M')}"
        tg_send(msg, buttons=[[('💼 포트폴리오','cmd_portfolio'),('📊 현황','cmd_status')]])
        
        try:
            chart_path = generate_pnl_chart()
            if chart_path and os.path.exists(chart_path):
                tg_send_photo(chart_path, caption=f"📈 {_today} 수익 차트")
        except: pass
        
    except Exception as e:
        tg_send(f"❌ 리포트 생성 실패: {e}")

def _tg_hourly_report(is_closing=False):
    """★ v8.0: 시간별 실적 리포트 — 마감리포트 수준으로 상세"""
    try:
        _today = datetime.now().strftime('%Y-%m-%d')
        _hm = datetime.now().strftime('%H:%M')
        _all = [t for t in trade_log if t.get('date')==_today]
        
        # 매수/매도 집계
        _buys = [t for t in _all if t.get('type') in ('AI_BUY','BUY') and t.get('success')]
        _sells = [t for t in _all if t.get('type') in ('SELL','FORCE_CLOSE','AI_SELL') and t.get('success')]
        _avg_downs = [t for t in _all if t.get('type') == 'AVG_DOWN' and t.get('success')]
        _blocked = [t for t in _all if t.get('type') in ('BLOCKED','CAPITAL_BLOCK')]
        _errors = [t for t in _all if 'ERROR' in t.get('type', '')]
        
        _pnl = sum(float(t.get('pnl',0) or 0) for t in _sells)
        _wins = len([t for t in _sells if float(t.get('pnl',0) or 0) > 0])
        _losses = len([t for t in _sells if float(t.get('pnl',0) or 0) < 0])
        _wr = round(_wins/max(_wins+_losses,1)*100)
        
        msg = f"📊 <b>{_hm} 실적 리포트</b>\n{'━'*20}\n"
        msg += f"💰 실현손익: <b>{'+'if _pnl>=0 else ''}₩{int(_pnl):,}</b> ({_wins}승{_losses}패 · 승률{_wr}%)\n"
        msg += f"📈 매수 {len(_buys)}건 · 매도 {len(_sells)}건 · 물타기 {len(_avg_downs)}건\n"
        if _blocked or _errors:
            msg += f"🚫 차단 {len(_blocked)}건 · ❌ 오류 {len(_errors)}건\n"
        
        # 매도 상세 (최근 5건)
        if _sells:
            msg += f"\n{'━'*20}\n📋 <b>매도 내역</b>\n"
            for t in _sells[-5:]:
                _nm = t.get('name','?')[:6]
                _p = float(t.get('pnl',0) or 0)
                _pp = float(t.get('pnl_pct',0) or 0)
                _q = t.get('qty',0)
                _r = t.get('reason','')[:10]
                _icon = '🟢' if _p >= 0 else '🔴'
                msg += f"{_icon} {_nm} {_q}주 {'+'if _p>=0 else ''}₩{int(_p):,} ({_pp:+.1f}%) {_r}\n"
        
        # 현재 보유종목 상태
        _total_eval_pnl = 0
        if auto_tickers and _bal_cache.get('data'):
            msg += f"\n{'━'*20}\n📦 <b>보유 {len(auto_tickers)}종목</b>\n"
            for _p in _bal_cache['data'].get('output1', []):
                _tk = _p.get('pdno','')
                if _tk not in auto_tickers: continue
                _q = int(_p.get('hldg_qty','0') or 0)
                if _q <= 0: continue
                _nm = _p.get('prdt_name','')[:6]
                _cur = float(_p.get('prpr','0') or 0)
                _avg = float(_p.get('pchs_avg_pric','0') or 0)
                _ep = round((_cur-_avg)*_q) if _avg>0 else 0
                _epp = round((_cur-_avg)/_avg*100,1) if _avg>0 else 0
                _total_eval_pnl += _ep
                _ac = auto_avg_count.get(_tk, 0)
                _icon = '🟢' if _ep >= 0 else '🔴'
                msg += f"{_icon} {_nm} {_q}주 {'+'if _epp>=0 else ''}{_epp}% ₩{int(_ep):,}{f' 물{_ac}' if _ac else ''}\n"
            msg += f"📊 평가손익: {'+'if _total_eval_pnl>=0 else ''}₩{int(_total_eval_pnl):,}\n"
        
        msg += f"\n{'━'*20}\n"
        if _total_eval_pnl != 0:
            msg += f"💎 실현{'+'if _pnl>=0 else ''}₩{int(_pnl):,} + 평가{'+'if _total_eval_pnl>=0 else ''}₩{int(_total_eval_pnl):,} = <b>{'+'if (_pnl+_total_eval_pnl)>=0 else ''}₩{int(_pnl+_total_eval_pnl):,}</b>"
        else:
            msg += f"💎 실현손익: <b>{'+'if _pnl>=0 else ''}₩{int(_pnl):,}</b>"
        
        tg_send(msg)
        
        if 'hourly_perf' not in daily_briefing:
            daily_briefing['hourly_perf'] = []
        daily_briefing['hourly_perf'].append({
            'time': _hm, 'auto_pnl': int(_pnl), 'swing_pnl': 0,
            'total': int(_pnl), 'is_closing': is_closing
        })
        save_state()
        print(f"[TG] ⏰ {_hm} 리포트 전송 (실현₩{int(_pnl):,} 보유{len(auto_tickers)}종목)")
    except Exception as e:
        print(f"[TG] 시간별 리포트 실패: {e}")

def _tg_weekly_report():
    """★ v3.0 I: 주간 수익 리포트 (매주 금요일 장마감 후)"""
    try:
        from collections import defaultdict
        today = datetime.now()
        # 이번 주 월요일 ~ 오늘
        monday = today - timedelta(days=today.weekday())
        week_start = monday.strftime('%Y-%m-%d')
        week_end = today.strftime('%Y-%m-%d')
        
        week_trades = [t for t in trade_log 
                      if t.get('type') in ('SELL','FORCE_CLOSE','AI_SELL') and t.get('success')
                      and week_start <= (t.get('date','') or '') <= week_end]
        week_buys = [t for t in trade_log
                    if t.get('type')=='AI_BUY' and t.get('success')
                    and week_start <= (t.get('date','') or '') <= week_end]
        
        realized = sum(float(t.get('pnl',0) or 0) for t in week_trades)
        wins = sum(1 for t in week_trades if float(t.get('pnl',0) or 0) > 0)
        losses = sum(1 for t in week_trades if float(t.get('pnl',0) or 0) < 0)
        win_rate = round(wins / max(wins + losses, 1) * 100)
        
        # 일별 집계
        daily = defaultdict(float)
        for t in week_trades:
            daily[t.get('date','')] += float(t.get('pnl',0) or 0)
        
        best_day = max(daily.items(), key=lambda x: x[1]) if daily else ('', 0)
        worst_day = min(daily.items(), key=lambda x: x[1]) if daily else ('', 0)
        
        msg = f"📅 <b>주간 수익 리포트</b>\n"
        msg += f"{week_start} ~ {week_end}\n"
        msg += f"━━━━━━━━━━━━━━━━━\n\n"
        msg += f"💰 주간 실현손익: {'+'if realized>=0 else ''}₩{int(realized):,}\n"
        msg += f"🔢 매수 {len(week_buys)}건 | 매도 {len(week_trades)}건\n"
        msg += f"🎯 승률: {win_rate}% ({wins}승 {losses}패)\n\n"
        
        if daily:
            msg += f"<b>일별 손익</b>\n"
            for d in sorted(daily.keys()):
                _dp = daily[d]
                _emoji = '🟢' if _dp >= 0 else '🔴'
                msg += f"  {_emoji} {d[5:]}: {'+'if _dp>=0 else ''}₩{int(_dp):,}\n"
        
        if best_day[1] != 0:
            msg += f"\n🏆 최고일: {best_day[0][5:]} (+₩{int(best_day[1]):,})\n"
        if worst_day[1] != 0:
            msg += f"💀 최악일: {worst_day[0][5:]} (₩{int(worst_day[1]):,})\n"
        
        msg += f"\n⏰ {today.strftime('%H:%M')}"
        tg_send(msg)
        
        # 차트 이미지도 전송
        try:
            chart_path = generate_pnl_chart()
            if chart_path and os.path.exists(chart_path):
                tg_send_photo(chart_path, caption=f"📊 주간 수익 차트 ({week_start}~{week_end})")
        except: pass
        
    except Exception as e:
        print(f"[WEEKLY] 주간 리포트 실패: {e}")

def _tg_monthly_report():
    """★ v3.0 I: 월간 수익 리포트 (매월 마지막 거래일)"""
    try:
        from collections import defaultdict
        today = datetime.now()
        this_month = today.strftime('%Y-%m')
        
        month_trades = [t for t in trade_log
                       if t.get('type') in ('SELL','FORCE_CLOSE','AI_SELL') and t.get('success')
                       and (t.get('date','') or '').startswith(this_month)]
        month_buys = [t for t in trade_log
                     if t.get('type')=='AI_BUY' and t.get('success')
                     and (t.get('date','') or '').startswith(this_month)]
        
        realized = sum(float(t.get('pnl',0) or 0) for t in month_trades)
        wins = sum(1 for t in month_trades if float(t.get('pnl',0) or 0) > 0)
        losses = sum(1 for t in month_trades if float(t.get('pnl',0) or 0) < 0)
        win_rate = round(wins / max(wins + losses, 1) * 100)
        
        # 주별 집계
        weekly = defaultdict(float)
        for t in month_trades:
            d = t.get('date','')
            if d:
                _dt = datetime.strptime(d, '%Y-%m-%d')
                week_num = _dt.isocalendar()[1]
                weekly[f"W{week_num}"] += float(t.get('pnl',0) or 0)
        
        # 종목별 TOP
        stock_pnl = defaultdict(lambda: {'pnl':0,'name':'','count':0})
        for t in month_trades:
            tk = t.get('ticker','')
            if tk:
                stock_pnl[tk]['pnl'] += float(t.get('pnl',0) or 0)
                stock_pnl[tk]['name'] = t.get('name', tk)
                stock_pnl[tk]['count'] += 1
        
        best_stocks = sorted(stock_pnl.items(), key=lambda x: x[1]['pnl'], reverse=True)[:3]
        worst_stocks = sorted(stock_pnl.items(), key=lambda x: x[1]['pnl'])[:3]
        
        # 거래일수
        active_days = len(set(t.get('date','') for t in month_buys + month_trades if t.get('date','')))
        
        msg = f"📆 <b>월간 수익 리포트</b>\n"
        msg += f"{this_month}\n"
        msg += f"━━━━━━━━━━━━━━━━━\n\n"
        msg += f"💰 월간 실현손익: {'+'if realized>=0 else ''}₩{int(realized):,}\n"
        msg += f"🔢 매수 {len(month_buys)}건 | 매도 {len(month_trades)}건\n"
        msg += f"🎯 승률: {win_rate}% ({wins}승 {losses}패)\n"
        msg += f"📅 거래일: {active_days}일\n\n"
        
        if weekly:
            msg += f"<b>주별 손익</b>\n"
            for w in sorted(weekly.keys()):
                _wp = weekly[w]
                msg += f"  {'🟢' if _wp>=0 else '🔴'} {w}: {'+'if _wp>=0 else ''}₩{int(_wp):,}\n"
        
        if best_stocks:
            msg += f"\n🏆 <b>수익 TOP</b>\n"
            for tk, v in best_stocks:
                if v['pnl'] > 0:
                    msg += f"  🟢 {v['name']} +₩{int(v['pnl']):,} ({v['count']}건)\n"
        if worst_stocks:
            msg += f"💀 <b>손실 TOP</b>\n"
            for tk, v in worst_stocks:
                if v['pnl'] < 0:
                    msg += f"  🔴 {v['name']} ₩{int(v['pnl']):,} ({v['count']}건)\n"
        
        # AI 예측 점수 트렌드
        try:
            history = load_briefing_history()
            month_scores = []
            for d, br in history.items():
                if d.startswith(this_month):
                    cl = br.get('closing', {})
                    sc = cl.get('forecast_accuracy', {}).get('overall_score', '')
                    if sc: month_scores.append(sc)
            if month_scores:
                msg += f"\n🤖 AI 예측 점수: {', '.join(month_scores)}\n"
        except: pass
        
        msg += f"\n⏰ {today.strftime('%H:%M')}"
        tg_send(msg)
        
        try:
            chart_path = generate_pnl_chart()
            if chart_path and os.path.exists(chart_path):
                tg_send_photo(chart_path, caption=f"📊 월간 수익 차트 ({this_month})")
        except: pass
        
    except Exception as e:
        print(f"[MONTHLY] 월간 리포트 실패: {e}")

def _tg_chat_handler(user_msg):
    """텔레그램 채팅 → AI 분석 → 텔레그램 응답 (채팅 탭과 동일 로직)"""
    global tg_chat_history
    try:
        # ★ 유저 메시지 히스토리 저장
        tg_chat_history.append({'role':'user','content':user_msg,'_ts':time.time(),'source':'tg'})
        if len(tg_chat_history) > 50: tg_chat_history = tg_chat_history[-50:]
        
        tg_send(f"🔍 분석 중... ({user_msg[:20]})")
        
        # 1. 종목 감지
        import re
        detected_ticker = ''
        detected_name = ''
        code_match = re.search(r'\b(\d{6})\b', user_msg)
        if code_match:
            detected_ticker = code_match.group(1)
        
        if not detected_ticker:
            filter_words = ['오늘','얼마','분석','주세요','주가','시장','추천','종목','매수','매도',
                           '현황','상황','뉴스','이슈','지금','어때','알려줘','해줘','알려','확인']
            words = [w for w in re.findall(r'[가-힣]{2,8}', user_msg) if w not in filter_words]
            for w in words[:2]:
                try:
                    ua = {'User-Agent': 'Mozilla/5.0'}
                    enc = urllib.parse.quote(w)
                    url = f'https://ac.stock.naver.com/ac?q={enc}&target=stock,index,etf,fund,bond,warr'
                    req = urllib.request.Request(url, headers=ua)
                    with urllib.request.urlopen(req, timeout=6) as r:
                        data = json.loads(r.read().decode('utf-8'))
                    raw_items = data.get('items') if isinstance(data, dict) else None
                    if raw_items and isinstance(raw_items, list):
                        for it in raw_items[:5]:
                            if isinstance(it, dict):
                                _c = str(it.get('code','')).strip()
                                _n = str(it.get('name','')).strip()
                                if _c and len(_c)==6 and _c.isdigit():
                                    detected_ticker = _c
                                    detected_name = _n
                                    break
                    if detected_ticker: break
                except: pass
            if not detected_ticker and words:
                detected_name = words[0]
        
        # 2. 시세 조회 (네이버)
        sys_ctx = ''
        now = datetime.now()
        day_names = ['월','화','수','목','금','토','일']
        _ds = f"{now.year}년 {now.month}월 {now.day}일({day_names[now.weekday()]}) {now.hour}:{now.minute:02d}"
        _we = now.weekday() >= 5
        sys_ctx += f'\n[📅 {_ds} {"주말(휴장)" if _we else "장중" if 900<=now.hour*100+now.minute<=1530 else "장외"}]\n'
        
        price_info = ''
        if detected_ticker:
            # ★ KIS 실시간 심층 분석 (웹 채팅탭과 동일)
            _cfg = getattr(auto_trader, 'config', None) or {}
            _ak = _cfg.get('app_key') or telegram_config.get('app_key','')
            _as = _cfg.get('app_secret') or telegram_config.get('app_secret','')
            _kis_ok = bool(_ak and _as)
            
            if _kis_ok:
                try:
                    _token = kis_get_token(_ak, _as, 'live')
                    # collect_single_stock: 시세+수급3일+뉴스 (웹 채팅과 동일 함수)
                    detail_str, raw = collect_single_stock({}, _token, _ak, _as, 'live', detected_ticker)
                    if detail_str:
                        sys_ctx += f'\n[★ KIS 실시간 {raw.get("name",detected_ticker)}({detected_ticker})]\n{detail_str}'
                        detected_name = raw.get('name') or detected_name
                        price_info = f'현재가 {int(raw.get("cur_price",0)):,}원 ({raw.get("chg_pct",0):+.2f}%)'
                        print(f"[TG_CHAT] ✅ KIS 심층: {detected_name} {price_info}")
                    
                    # 52주 최고/최저 + 시총 (stock-detail과 동일)
                    try:
                        _pr = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
                            _ak, _as, 'live', _token, "FHKST01010100",
                            params={"FID_COND_MRKT_DIV_CODE":"J","FID_INPUT_ISCD":detected_ticker})
                        _o = _pr.get('output', {})
                        w52h = _o.get('stck_dryy_hgpr','')
                        w52l = _o.get('stck_dryy_lwpr','')
                        mktcap = _o.get('hts_avls','')
                        if w52h: sys_ctx += f"52주 최고: {int(w52h):,}원 / "
                        if w52l: sys_ctx += f"52주 최저: {int(w52l):,}원\n"
                        if mktcap: sys_ctx += f"시가총액: {int(mktcap):,}억원\n"
                        if w52h and w52l and raw.get('cur_price'):
                            _h,_l,_p = int(w52h),int(w52l),int(raw['cur_price'])
                            if _h > _l:
                                sys_ctx += f"52주 범위 내 위치: {round((_p-_l)/(_h-_l)*100)}%\n"
                    except: pass
                    
                    # 뉴스 7일치 (심층)
                    try:
                        news7 = fetch_naver_news(ticker=detected_ticker, max_days=7)
                        if news7 and len(news7) > len(raw.get('news',[])):
                            sys_ctx += '뉴스(7일): ' + ' / '.join(news7[:5]) + '\n'
                    except: pass
                    
                except Exception as e:
                    print(f"[TG_CHAT] KIS 심층 실패: {e}, 네이버 fallback")
                    _kis_ok = False
            
            # KIS 실패 시 네이버 fallback
            if not _kis_ok:
                try:
                    ua = {'User-Agent':'Mozilla/5.0','Referer':'https://finance.naver.com/'}
                    req = urllib.request.Request(f'https://m.stock.naver.com/api/stock/{detected_ticker}/basic', headers=ua)
                    with urllib.request.urlopen(req, timeout=8) as r:
                        d = json.loads(r.read().decode('utf-8'))
                    nm = d.get('stockName') or d.get('name','')
                    pr = d.get('closePrice') or d.get('stockEndPrice') or d.get('now','')
                    if pr:
                        detected_name = nm or detected_name
                        _p = int(str(pr).replace(',',''))
                        chg = d.get('compareToPreviousClosePrice','')
                        pct = d.get('fluctuationsRatio','')
                        sys_ctx += f'\n[네이버 {detected_name}({detected_ticker})]\n현재가: {_p:,}원'
                        if chg: sys_ctx += f' ({chg}원, {pct}%)'
                        sys_ctx += '\n'
                        price_info = f'현재가 {_p:,}원'
                except: pass
                try:
                    news = fetch_naver_news(ticker=detected_ticker, max_days=3)
                    if news: sys_ctx += '뉴스: ' + ' / '.join(news[:3]) + '\n'
                except: pass
        
        # 2.5 KIS 계좌 데이터 (보유종목/수익률/예수금)
        _acct_kw = ['보유','계좌','수익','손익','잔고','매도','팔','현황','포지션','종목현황','전체','얼마']
        if any(kw in user_msg for kw in _acct_kw):
            try:
                # auto_trader config 또는 telegram_config에서 KIS 키 가져오기
                _cfg = getattr(auto_trader, 'config', None) or {}
                _ak = _cfg.get('app_key') or telegram_config.get('app_key','')
                _as = _cfg.get('app_secret') or telegram_config.get('app_secret','')
                _acct = _cfg.get('account') or telegram_config.get('account','')
                _acct_cd = _cfg.get('account_cd','01')
                if _ak and _as and _acct:
                    _token = kis_get_token(_ak, _as, 'live')
                    bal = get_balance(_ak, _as, 'live', _token, _acct, _acct_cd, max_age=30)
                    if bal and bal.get('output1'):
                        positions = [p for p in bal['output1'] if int(p.get('hldg_qty','0') or 0) > 0]
                        out2 = bal.get('output2', [{}])
                        tot_eval = int(out2[0].get('tot_evlu_amt','0') or 0) if out2 else 0
                        cash = calc_ord_psbl_cash(bal) if bal else 0
                        tot_pnl = int(out2[0].get('evlu_pfls_smtl_amt','0') or 0) if out2 else 0
                        
                        sys_ctx += f'\n[★ KIS 실시간 계좌]\n'
                        sys_ctx += f'총평가: ₩{tot_eval:,} | 예수금: ₩{cash:,} | 평가손익: {"+" if tot_pnl>=0 else ""}₩{tot_pnl:,}\n'
                        
                        if positions:
                            sys_ctx += f'보유종목 {len(positions)}개:\n'
                            for p in positions:
                                _nm = p.get('prdt_name','?')
                                _tk = p.get('pdno','')
                                _qty = int(p.get('hldg_qty','0') or 0)
                                _avg = int(float(p.get('pchs_avg_pric','0') or 0))
                                _cur = int(p.get('prpr','0') or 0)
                                _pnl_r = float(p.get('evlu_pfls_rt','0') or 0)
                                _pnl_a = int(p.get('evlu_pfls_amt','0') or 0)
                                sys_ctx += f'  {_nm}({_tk}) {_qty}주 평균₩{_avg:,} 현재₩{_cur:,} {"+" if _pnl_r>=0 else ""}{_pnl_r:.1f}% ({"+" if _pnl_a>=0 else ""}₩{_pnl_a:,})\n'
                        else:
                            sys_ctx += '보유종목: 없음 (전량 현금)\n'
                        
                        # 오늘 매매 실적
                        _today = datetime.now().strftime('%Y-%m-%d')
                        _t_buys = [t for t in trade_log if t.get('date')==_today and t.get('type')=='AI_BUY' and t.get('success')]
                        _t_sells = [t for t in trade_log if t.get('date')==_today and t.get('type')=='SELL' and t.get('success')]
                        _t_pnl = sum(float(t.get('pnl',0) or 0) for t in _t_sells)
                        if _t_buys or _t_sells:
                            sys_ctx += f'오늘실적: 매수{len(_t_buys)}건 매도{len(_t_sells)}건 실현손익{"+" if _t_pnl>=0 else ""}₩{int(_t_pnl):,}\n'
                        
                        print(f"[TG_CHAT] ✅ 계좌 데이터 주입: {len(positions)}종목, 평가₩{tot_eval:,}")
                else:
                    sys_ctx += '\n[계좌 정보 없음 - 자동매매 시작 후 사용 가능]\n'
            except Exception as e:
                print(f"[TG_CHAT] 계좌 조회 실패: {e}")
                sys_ctx += '\n[계좌 조회 실패]\n'
        
        # 3. AI 호출 — 사용자가 선택한 프로바이더 우선
        _oai = ai_config.get('openai_key','') or telegram_config.get('openai_key','')
        _ant = ai_config.get('anthropic_key','') or telegram_config.get('anthropic_key','')
        # ★ 사용자가 설정에서 선택한 프로바이더를 우선 사용
        _prov = telegram_config.get('ai_provider','') or ai_config.get('provider','')
        print(f"[TG_CHAT] 키 감지: provider={_prov} oai={'✅' if _oai else '❌'}({len(_oai)}자) ant={'✅' if _ant else '❌'}({len(_ant)}자)")
        if 'openai' in _prov and _oai:
            ai_config['openai_key'] = _oai
            ai_config['provider'] = 'openai'
        elif ('anthropic' in _prov or 'claude' in _prov) and _ant:
            ai_config['anthropic_key'] = _ant
            ai_config['provider'] = 'anthropic'
        elif _oai:
            ai_config['openai_key'] = _oai
            ai_config['provider'] = 'openai'
        elif _ant:
            ai_config['anthropic_key'] = _ant
            ai_config['provider'] = 'anthropic'
        else:
            tg_send("❌ AI 키가 없습니다.\n설정 탭에서 AI키 입력 후 📱테스트 전송을 다시 해주세요.")
            return
        
        system = f"한국 주식시장 AI 어드바이저. {_ds}.\n{sys_ctx}\n"
        system += "간결하게 답변. 텔레그램용이므로 3~5줄 이내. 날짜 명시.\n"
        system += "매수 요청 시 [[ACTION:BUY:코드:이름:수량:가격]] 태그 포함.\n"
        system += "매도 요청 시 [[ACTION:SELL:코드:이름:수량:가격]] 태그 포함.\n"
        system += "보유종목 분석 시: 각 종목별 수익률/매도 의견(보유/익절/손절) + 종합 의견.\n"
        system += "종목 분석 시 매수 평가: 점수/100 + 등급(강력추천/추천/보통/보류) + 근거.\n"
        system += "계좌/보유종목 데이터가 있으면 반드시 활용해서 구체적으로 답변.\n"
        
        ai_reply = call_ai(user_msg, system, max_tokens=800, web_search=True)
        
        if not ai_reply or len(ai_reply.strip()) < 5:
            tg_send("❌ AI 응답을 받지 못했습니다.")
            return
        
        # 4. ACTION 태그 파싱 → 매수/매도 버튼
        action_match = re.search(r'\[\[ACTION:(BUY|SELL):(\d{6}):([^:]+):(\d+):(\d+)\]\]', ai_reply)
        reply_clean = re.sub(r'\[\[ACTION:[^\]]*\]\]', '', ai_reply).strip()
        # RATING 태그 → 텍스트로 변환
        def _rating_fmt(m):
            sc,gr,rs = m.group(1),m.group(2),m.group(3)
            return f"\n📊 매수평가: {sc}/100 [{gr}]\n근거: {rs}"
        reply_clean = re.sub(r'\[\[RATING:(\d+):([^:]+):([^\]]+)\]\]', _rating_fmt, reply_clean)
        
        # HTML 태그 제거 (텔레그램은 간단한 HTML만 지원)
        reply_clean = reply_clean.replace('**', '').replace('##', '').replace('# ', '')
        
        # 헤더 추가
        header = ''
        if detected_name and detected_ticker:
            header = f"📌 <b>{detected_name}({detected_ticker})</b>"
            if price_info: header += f" | {price_info}"
            header += '\n\n'
        
        buttons = None
        if action_match:
            _type = action_match.group(1)
            _tk = action_match.group(2)
            _nm = action_match.group(3)
            _qty = action_match.group(4)
            _emoji = '🟢 매수' if _type=='BUY' else '🔴 매도'
            buttons = [[
                (f'{_emoji} {_nm} {_qty}주 실행', f'trade_{_type}_{_tk}_{_qty}'),
                ('❌ 취소', 'cmd_status')
            ]]
        
        tg_send(header + reply_clean[:3500], buttons=buttons)
        
        # ★ AI 응답 히스토리 저장
        tg_chat_history.append({'role':'assistant','content':ai_reply,'_ts':time.time(),'source':'tg'})
        if len(tg_chat_history) > 50: tg_chat_history = tg_chat_history[-50:]
        
        # 5. 매매 실행 콜백은 tg_poll_commands의 callback_query에서 처리
        
    except Exception as e:
        print(f"[TG_CHAT] ❌ 오류: {e}")
        import traceback; traceback.print_exc()
        tg_send(f"❌ 처리 오류: {str(e)[:100]}")

# ── 텔레그램-웹 채팅 공유 히스토리
tg_chat_history = []  # [{role, content, _ts, source:'tg'|'web'}]

# ── 독립 텔레그램 폴링 스레드 (자동매매와 무관하게 항상 실행)
_tg_poller_running = False
def _tg_poller_loop():
    """10초마다 텔레그램 메시지 확인 — 서버 시작 시 자동 실행"""
    global _tg_poller_running
    _tg_poller_running = True
    print("[TG] ✅ 텔레그램 폴링 스레드 시작")
    while _tg_poller_running:
        try:
            if telegram_config.get('enabled') and telegram_config.get('token'):
                tg_poll_commands()
        except: pass
        time.sleep(10)

def start_tg_poller():
    """텔레그램 폴링 시작 (중복 방지)"""
    global _tg_poller_running
    if _tg_poller_running: return
    t = threading.Thread(target=_tg_poller_loop, daemon=True)
    t.start()

# ── 현금 트래커: KIS 잔고 API 실시간 미반영 보완
# KIS 잔고 조회 시 업데이트, 매수 성공 시 직접 차감
_cash_tracker = {'amount': 0, 'ts': 0}  # amount=가용현금, ts=마지막KIS조회시각

def get_balance(app_key, app_secret, mode, token, account, account_cd='01', max_age=120):
    """잔고 조회 - 캐시 우선 사용 (max_age초 이내면 KIS API 재호출 안함)"""
    if _bal_cache['data'] and (time.time() - _bal_cache['ts']) < max_age:
        return _bal_cache['data']
    tr_id = "TTTC8434R"
    bal = kis_request("GET", "/uapi/domestic-stock/v1/trading/inquire-balance",
        app_key, app_secret, mode, token, tr_id,
        params={"CANO": account, "ACNT_PRDT_CD": account_cd,
                "AFHR_FLPR_YN": "N", "OFL_YN": "", "INQR_DVSN": "02",
                "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N",
                "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "01",
                "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""})
    if bal.get('rt_cd') == '0':
        # ★ v3.0 근본 FIX: KIS output2에 ord_psbl_cash가 없음!
        # 총평가 - 보유종목평가 = 주문가능금액으로 직접 계산해서 삽입
        try:
            out2 = (bal.get('output2', [{}]) or [{}])[0] or {}
            tot = int(out2.get('tot_evlu_amt', '0') or 0)
            stock_eval = int(out2.get('evlu_amt_smtl_amt', '0') or 0)
            if not stock_eval:
                stock_eval = sum(int(p.get('evlu_amt', '0') or 0)
                    for p in bal.get('output1', []) if int(p.get('hldg_qty', '0') or 0) > 0)
            avail = max(tot - stock_eval, 0)
            out2['ord_psbl_cash'] = str(avail)
        except Exception:
            pass
        
        _bal_cache['data'] = bal
        _bal_cache['ts'] = time.time()
    return bal

# ★ 조기 선언: load_state에서 참조하므로 함수 정의 전에 선언
peak_prices = {}
_tp1_triggered = set()
daily_briefing = {}  # ★ v8.0: 마감 실적리포트 + hourly_perf + 매매일지만 사용
_sell_stage = {}  # ★ 3차 익절 단계 추적 {ticker: 0/1/2/3}
_dip_flag = {}    # ★ 눌림 감지 플래그 {ticker: True/False}
_dip_time = {}    # ★ v6.0: 눌림 감지 시각 {ticker: timestamp} — 30분 타임아웃용
_sell_plans = {}  # ★★★ v5.0: 종목별 AI 매도 전략 {ticker: {tp1,tp2,sl,time_limit,atr_mult,...}}
_ai_sell_states = {}  # ★★★ v5.0: AI 상태 분류 {ticker: {state,reason,ts,atr_mult_adj}}
_ai_classify_ts = 0   # ★★★ v5.0: 마지막 상태 분류 시점

def load_state(mode=None):
    """mode별 state 파일 로드. mode=None이면 mock+live 둘 다 시도 후 병합"""
    global auto_rules, trade_log, auto_tickers, api_usage, perm_blocked, daily_briefing, _sell_stage, _dip_flag
    global swing_tickers, swing_config, swing_avg_count, swing_running, swing_sell_stage, swing_dip_flag, auto_avg_count
    try:
        # 구버전 trading_state.json 마이그레이션
        import os
        if os.path.exists(STATE_FILE) and not os.path.exists(STATE_FILE_LIVE):
            try:
                import shutil
                shutil.copy(STATE_FILE, STATE_FILE_LIVE)
                print("[MIGRATE] trading_state.json → trading_state_live.json")
            except Exception as _me: print(f"[WARN] migrate: {_me}")
    except Exception as _e: print(f"[WARN] migration check: {_e}")

    # ★ FIX #1: 실제 JSON 파일에서 상태 복원 (기존 누락)
    sf = get_state_file()
    try:
        import os
        if os.path.exists(sf):
            with open(sf, 'r', encoding='utf-8') as f:
                data = json.load(f)
            auto_rules = data.get('auto_rules', [])
            trade_log = data.get('trade_log', [])
            # ★ auto_tickers: list → set 통일 (Fix #4 연계)
            _at = data.get('auto_tickers', [])
            auto_tickers.clear()
            auto_tickers.extend(_at if isinstance(_at, list) else list(_at))
            api_usage.update(data.get('api_usage', {}))
            perm_blocked.update(data.get('perm_blocked', {}))
            # ★★★ v8.0: 기존 거래정지 라벨 → HALTED: 형식으로 변환 ★★★
            for _pk, _pv in list(perm_blocked.items()):
                if ('거래정지' in _pv or '매매정지' in _pv) and not _pv.startswith('HALTED:'):
                    perm_blocked[_pk] = f"HALTED: {_pv}"
                    print(f"[STATE] {_pk} 거래정지 라벨 변환 → HALTED:")
            # ★ 1차 익절 기록 복원 (재시작 시 50% 재매도 방지)
            _tp1_restored = data.get('tp1_triggered', [])
            if _tp1_restored:
                _tp1_triggered.update(_tp1_restored)
                print(f"[STATE] tp1_triggered 복원: {_tp1_triggered}")
            # ★ peak_prices 복원 (트레일링 스톱 연속성)
            _peaks = data.get('peak_prices', {})
            if _peaks:
                peak_prices.update(_peaks)
                print(f"[STATE] peak_prices 복원: {len(_peaks)}종목")
            # ★ 3차 익절 단계 복원
            _ss = data.get('sell_stage', {})
            if _ss:
                _sell_stage.update(_ss)
                print(f"[STATE] sell_stage 복원: {_ss}")
            _df = data.get('dip_flag', {})
            if _df:
                _dip_flag.update(_df)
            # ★ v5.0: sell_plans 복원
            _sp = data.get('sell_plans', {})
            if _sp:
                _sell_plans.update(_sp)
                print(f"[STATE] sell_plans 복원: {len(_sp)}종목")
            # ★ v5.0: swing 데이터 복원
            _swt = data.get('swing_tickers', [])
            if _swt:
                swing_tickers.clear()
                swing_tickers.extend(_swt)
                print(f"[STATE] swing_tickers 복원: {len(_swt)}종목")
            _swc = data.get('swing_config', {})
            if _swc:
                swing_config.update(_swc)
            _swa = data.get('swing_avg_count', {})
            if _swa:
                swing_avg_count.update(_swa)
            _aac = data.get('auto_avg_count', {})
            if _aac:
                auto_avg_count.update(_aac)
            swing_running = data.get('swing_running', False)
            _sws = data.get('swing_sell_stage', {})
            if _sws:
                swing_sell_stage.update(_sws)
                print(f"[STATE] swing_sell_stage 복원: {_sws}")
            _swdf = data.get('swing_dip_flag', {})
            if _swdf:
                swing_dip_flag.update(_swdf)
            _sbr = data.get('swing_buy_routes', {})
            if _sbr:
                swing_buy_routes.update(_sbr)
                print(f"[STATE] swing_buy_routes 복원: {_sbr}")
            # ★ v6.0: 보유종목 평가 복원
            _he = data.get('holding_evaluations', {})
            if _he and _he.get('items'):
                holding_evaluations.update(_he)
                print(f"[STATE] holding_evaluations 복원: {len(_he.get('items',{}))}종목 ({_he.get('session','')}, {_he.get('time','')[:16]})")
            # ★ v6.0: 기존 종목 매수경로 마이그레이션 (trade_log에서 복원)
            # ★ 1회성 재마이그레이션: manual_add로 잘못 저장된 것 수정
            _need_remigrate = any(v == 'manual_add' for v in swing_buy_routes.values())
            for _stk in swing_tickers:
                if _stk not in swing_buy_routes or (_need_remigrate and swing_buy_routes.get(_stk) == 'manual_add'):
                    _found_route = ''
                    # 전체 trade_log 역순 검색 (500개 제한 없음)
                    for _tl in reversed(trade_log):
                        if _tl.get('ticker') != _stk: continue
                        _br = _tl.get('buy_route', '')
                        _tp = _tl.get('type', '')
                        _tm = _tl.get('trade_mode', '')
                        # 1순위: buy_route 직접 기록
                        if _br in ('chat', 'telegram', 'manual_add', 'ai_swing'):
                            _found_route = _br; break
                        # 2순위: 타입으로 판별
                        if _tp == 'CHAT_BUY': _found_route = 'chat'; break
                        if _tp == 'TG_BUY': _found_route = 'telegram'; break
                        if _tp == 'SWING_BUY': _found_route = 'ai_swing'; break
                        if _tp == 'MANUAL_ADD': _found_route = 'manual_add'; break
                        # 3순위: AI_BUY + swing 모드 (v5 호환)
                        if _tp == 'AI_BUY' and _tm == 'swing': _found_route = 'ai_swing'; break
                    swing_buy_routes[_stk] = _found_route or 'manual_add'
                    print(f"[STATE] 매수경로 마이그레이션: {_stk} → {swing_buy_routes[_stk]}")
            # ★ v8.0: daily_briefing 복원 (마감 실적리포트 + 매매일지만 사용)
            _bf = data.get('daily_briefing', {})
            if _bf and _bf.get('date') == datetime.now().strftime('%Y-%m-%d'):
                # 마감 리포트/일지/hourly_perf만 복원 (시황 브리핑은 v7에서 삭제됨, v8 유지)
                daily_briefing['date'] = _bf['date']
                for _key in ('closing', 'closing_done', 'closing_at', 'journal', 'hourly_perf'):
                    if _bf.get(_key):
                        daily_briefing[_key] = _bf[_key]
                _restored = [k for k in ('closing', 'journal') if daily_briefing.get(k)]
                if _restored:
                    print(f"[STATE] ✅ daily_briefing 복원: {', '.join(_restored)}")
                else:
                    print(f"[STATE] daily_briefing 날짜 설정: {_bf['date']}")
            elif _bf:
                daily_briefing.clear()
            print(f"[STATE] ✅ 로드 완료 ({sf}): rules={len(auto_rules)} log={len(trade_log)} "
                  f"tickers={len(auto_tickers)} blocked={len(perm_blocked)}")
        else:
            print(f"[STATE] ⚠️ 상태 파일 없음 ({sf}) - 신규 시작")
    except json.JSONDecodeError as _je:
        print(f"[STATE] ❌ JSON 파싱 실패 ({sf}): {_je} - 백업 후 신규 시작")
        try:
            import shutil
            shutil.copy(sf, sf + '.bak')
        except: pass
    except Exception as _e:
        print(f"[STATE] ❌ 로드 실패: {_e}")

_log_cache = {'data': None, 'data_full': None, 'ts': 0, 'ts_full': 0, 'log_len': 0}

# ★★★ v8.0: hold_times 캐시 (5000건 순회+datetime파싱 → 10초 캐시) ★★★
_hold_times_cache = {'data': {}, 'ts': 0}

def _get_hold_times_cached():
    """보유시간 계산 — 10초 캐시 (5000건 순회 방지)"""
    now = time.time()
    if (now - _hold_times_cache['ts']) < 10:
        return _hold_times_cache['data']
    
    _now = datetime.now()
    _today = _now.date()
    _today_open = _now.replace(hour=9, minute=0, second=0, microsecond=0)
    _result = {}
    
    # 역순으로 순회 — 최신 매수 기록만 필요 (이미 찾은 종목은 스킵)
    _buy_types = {'AI_BUY','BUY','CHAT_BUY','TG_BUY','SWING_BUY','MANUAL_ADD','SYSTEM_BUY','AVG_DOWN'}
    for t in reversed(trade_log):
        _tk = t.get('ticker', '')
        if not _tk or _tk in _result:
            continue
        if t.get('type', '') not in _buy_types:
            continue
        if not t.get('success', True):
            continue
        _tstr = t.get('time', '')
        if not _tstr:
            continue
        try:
            _buy_dt = datetime.fromisoformat(_tstr)
            if _buy_dt.date() < _today:
                _result[_tk] = max(0, int((_now - _today_open).total_seconds() / 60))
            else:
                _result[_tk] = max(0, int((_now - _buy_dt).total_seconds() / 60))
        except:
            _result[_tk] = 0
    
    _hold_times_cache['data'] = _result
    _hold_times_cache['ts'] = now
    return _result

def _build_log_for_client(logs, full=False):
    """★★★ v8.0: 캐시 적용 — 5초 이내 재호출 시 캐시 반환 ★★★"""
    now = time.time()
    _key = 'data_full' if full else 'data'
    _ts_key = 'ts_full' if full else 'ts'
    
    # 캐시 유효: 5초 이내 + 로그 크기 안 변했으면
    if _log_cache[_key] and (now - _log_cache[_ts_key]) < 5 and _log_cache['log_len'] == len(logs):
        return _log_cache[_key]
    
    if full:
        from datetime import date, timedelta
        day3_s = (date.today() - timedelta(days=3)).isoformat()
        IMPORTANT_TYPES = {'AI_BUY','BUY','CHAT_BUY','TG_BUY','SWING_BUY','MANUAL_ADD','SELL','CHAT_SELL','AI_SELL',
                           'FORCE_CLOSE','FORCE_CLOSE_MARKET','BLOCKED','CAPITAL_BLOCK',
                           'AVG_DOWN','HOLDING_EVAL','TARGET_HIT','DAILY_STOP','AVG_WAITING',
                           'SELL_ERROR','BUY_ERROR','ERROR'}
        important = [l for l in logs if l.get('type','') in IMPORTANT_TYPES]
        noise = [l for l in logs if l.get('type','') not in IMPORTANT_TYPES
                 and (l.get('date') or l.get('time',''))[:10] >= day3_s]
        result = sorted(important + noise, key=lambda x: x.get('time',''))
    else:
        _today = datetime.now().strftime('%Y-%m-%d')
        BUY_SELL_TYPES = {'AI_BUY','BUY','CHAT_BUY','TG_BUY','SWING_BUY','MANUAL_ADD',
                          'SELL','CHAT_SELL','AI_SELL','FORCE_CLOSE','FORCE_CLOSE_MARKET','AVG_DOWN'}
        today_logs = [l for l in logs if l.get('date', l.get('time','')[:10]) == _today]
        old_trades = [l for l in logs if l.get('type','') in BUY_SELL_TYPES
                      and l.get('date', l.get('time','')[:10]) != _today]
        result = old_trades + today_logs
    
    _log_cache[_key] = result
    _log_cache[_ts_key] = now
    _log_cache['log_len'] = len(logs)
    return result

def get_stock_name_naver(ticker):
    """네이버 금융 JSON API로 종목명 조회 (인증 불필요)"""
    urls = [
        f"https://m.stock.naver.com/api/stock/{ticker}/basic",
        f"https://api.stock.naver.com/stock/{ticker}/basic",
    ]
    for url in urls:
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'Referer': 'https://m.stock.naver.com/',
                'Accept': 'application/json'
            })
            with urllib.request.urlopen(req, timeout=3) as r:
                data = json.loads(r.read())
            name = (data.get('stockName') or data.get('name') or
                    data.get('corporateName') or data.get('itemName') or '')
            if name:
                return name.strip()
        except:
            pass
    # fallback: finance.naver.com html title
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        with urllib.request.urlopen(req, timeout=4) as r:
            html = r.read().decode('euc-kr','ignore')
        import re as _re2
        m = _re2.search(r'<title>\s*([^:\|<]+?)\s*[:\|]', html)
        if m:
            return m.group(1).strip()
    except:
        pass
    return ''

def ensure_name(ticker, name=''):
    """★ 종목명 보증: 빈 값/코드만이면 네이버에서 조회. trade_log 기록 전 반드시 호출."""
    name = (name or '').strip()
    if name and name != ticker and any(c.isalpha() for c in name):
        return name
    return get_stock_name_naver(ticker) or ticker

def get_state_file():
    return STATE_FILE_LIVE

def save_state():
    global _last_save_time, peak_prices, recently_sold
    now = time.time()
    # ★ v8.0: 디바운스 3초 (파일 쓰기 빈도 감소 → 성능 개선)
    if hasattr(save_state, '_last') and now - save_state._last < 3:
        if not getattr(save_state, '_pending', False):
            save_state._pending = True
            def _deferred():
                time.sleep(3)
                save_state._pending = False
                save_state._last = 0
                save_state()
            threading.Thread(target=_deferred, daemon=True).start()
        return
    save_state._last = now
    save_state._pending = False
    try:
        # ★ v3.0: trade_log 절대 안 자름 — 매매 기록 영구 보존
        # (오래된 스캔 로그만 메모리에서 정리, 파일에는 전부 저장)
        trimmed_log = trade_log
        
        # ★ 메모리 누수 방지: peak_prices, recently_sold 정리 (7일 이상 된 항목 제거)
        cutoff_time = now - (7 * 24 * 3600)  # 7일 전
        
        # peak_prices 정리: auto_tickers에 없는 종목 제거
        stale_peaks = [ticker for ticker in peak_prices.keys() if ticker not in auto_tickers]
        for ticker in stale_peaks[:50]:  # 한번에 최대 50개씩 정리
            del peak_prices[ticker]
        
        # recently_sold 정리: 7일 이상 된 항목 제거
        stale_sells = [ticker for ticker, sell_time in recently_sold.items() if sell_time < cutoff_time]
        for ticker in stale_sells[:50]:  # 한번에 최대 50개씩 정리
            del recently_sold[ticker]
            
        if stale_peaks or stale_sells:
            print(f"[MEMORY] 정리: peak_prices -{len(stale_peaks)}개, recently_sold -{len(stale_sells)}개")
        
        sf = get_state_file()
        tmp_sf = sf + '.tmp'
        with open(tmp_sf, 'w', encoding='utf-8') as f:
            json.dump({'auto_rules': auto_rules, 'trade_log': trimmed_log, 'auto_tickers': auto_tickers, 'api_usage': api_usage, 'perm_blocked': perm_blocked,
                       'tp1_triggered': list(_tp1_triggered),
                       'peak_prices': {k:v for k,v in peak_prices.items() if k in auto_tickers},
                       'daily_briefing': daily_briefing,
                       'sell_stage': _sell_stage,
                       'dip_flag': _dip_flag,
                       'sell_plans': _sell_plans,
                       'swing_tickers': [],  # v8: always empty
                       'swing_config': swing_config,
                       'swing_avg_count': {},
                       'auto_avg_count': auto_avg_count,
                       'swing_running': False,  # v8: disabled
                       'swing_sell_stage': {},
                       'swing_dip_flag': {},
                       'swing_buy_routes': {},
                       'holding_evaluations': {}
                      }, f, ensure_ascii=False)
        os.replace(tmp_sf, sf)  # 원자적 교체 (파일 손상 방지)
    except Exception as _e: print(f"[WARN] save_state: {_e}")

def track_api_usage(response_data):
    """Track API token usage per-model (OpenAI Responses + Claude)"""
    global api_usage
    current_month = datetime.now().strftime('%Y-%m')
    if api_usage.get('month') != current_month:
        api_usage = {'month': current_month, 'calls': 0, 'web_searches': 0,
                     # legacy totals (kept for compatibility)
                     'input_tokens': 0, 'output_tokens': 0,
                     # per-model breakdown
                     'models': {}}
    if 'models' not in api_usage:
        api_usage['models'] = {}

    raw_model = response_data.get('model', 'unknown')
    # normalize: gpt-4o-mini-xxxx → gpt-4o-mini, claude-sonnet-xxx → claude-sonnet
    if 'gpt-4o-mini' in raw_model:
        model_key = 'gpt-4o-mini'
    elif 'gpt-4o' in raw_model:
        model_key = 'gpt-4o'
    elif 'claude' in raw_model and 'opus' in raw_model:
        model_key = 'claude-opus'
    elif 'claude' in raw_model:
        model_key = 'claude-sonnet'
    else:
        model_key = raw_model or 'unknown'

    usage = response_data.get('usage', {})
    in_tok = usage.get('input_tokens', usage.get('prompt_tokens', 0))
    out_tok = usage.get('output_tokens', usage.get('completion_tokens', 0))
    ws = sum(1 for item in response_data.get('output', []) if item.get('type') == 'web_search_call')

    # per-model accumulation
    if model_key not in api_usage['models']:
        api_usage['models'][model_key] = {'input_tokens': 0, 'output_tokens': 0, 'calls': 0, 'web_searches': 0}
    api_usage['models'][model_key]['input_tokens'] += in_tok
    api_usage['models'][model_key]['output_tokens'] += out_tok
    api_usage['models'][model_key]['calls'] += 1
    api_usage['models'][model_key]['web_searches'] += ws

    # legacy totals (for old display compat)
    api_usage['input_tokens'] = api_usage.get('input_tokens', 0) + in_tok
    api_usage['output_tokens'] = api_usage.get('output_tokens', 0) + out_tok
    api_usage['calls'] = api_usage.get('calls', 0) + 1
    api_usage['web_searches'] = api_usage.get('web_searches', 0) + ws
    api_usage['last_model'] = model_key  # last called model (for display only)
    save_state()

load_state()

# ★★★ v8.0: 시황 브리핑 삭제 — daily_briefing은 마감 실적리포트 + hourly_perf만 사용 ★★★
if not daily_briefing.get('date'):
    daily_briefing['date'] = datetime.now().strftime('%Y-%m-%d')

# ★★★ v8.0: 재시작 시 마감 리포트 done 플래그 처리 ★★★
_startup_hm = datetime.now().hour * 100 + datetime.now().minute
_startup_today = datetime.now().strftime('%Y-%m-%d')
if daily_briefing.get('date') == _startup_today:
    if daily_briefing.get('closing') and not daily_briefing.get('closing_done'):
        daily_briefing['closing_done'] = True
        print(f"[STARTUP] 🔒 마감 리포트 _done 설정 (데이터 존재)")
    if _startup_hm >= 1640 and not daily_briefing.get('closing_done'):
        daily_briefing['closing_done'] = True
        print(f"[STARTUP] 🔒 마감 리포트 _done 설정 (16:40 이후)")
elif daily_briefing.get('date'):
    # ★★★ v8.0: 전날 state → 매도 상태 전부 초기화 (전날 보유종목도 새 날 새 출발) ★★★
    daily_briefing.clear()
    daily_briefing['date'] = _startup_today
    _sell_stage.clear()
    _tp1_triggered.clear()
    peak_prices.clear()
    _dip_flag.clear()
    auto_avg_count.clear()
    swing_sell_stage.clear()
    swing_dip_flag.clear()
    # swing_avg_count는 유지 (중장기=며칠 보유 → 물타기 횟수 누적)
    print(f"[STARTUP] 🔄 전날 state → 새 날 초기화: 단타+중장기 sell_stage/peak/avg_count 리셋")

# ============= KIS API HELPERS =============
def kis_base_url(mode):
    return KIS_PROD_URL

_token_lock = __import__('threading').Lock()

def kis_get_token(app_key, app_secret, mode):
    """Get or refresh KIS access token (thread-safe - fixed race condition)"""
    with _token_lock:  # ★ 전체 로직을 atomic하게 보호
        key = mode
        now = datetime.now()
        
        # Return cached token if still valid
        if key in token_store:
            stored = token_store[key]
            if stored.get('expires') and datetime.fromisoformat(stored['expires']) > now:
                return stored['token']
        
        # Clear expired token
        if key in token_store:
            del token_store[key]
        
        # Token issuance outside lock to avoid holding lock during network I/O
        url = f"{kis_base_url(mode)}/oauth2/tokenP"
        body = json.dumps({
            "grant_type": "client_credentials",
            "appkey": app_key,
            "appsecret": app_secret
        }).encode('utf-8')
    
    # Network I/O outside lock to prevent blocking other threads
    try:
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            token = data.get('access_token', '')
            if not token:
                raise Exception(f"토큰 발급 실패: {data}")
            expires = (now + timedelta(hours=23)).isoformat()
            
            # ★ 토큰 저장도 락으로 보호 (double-check 패턴)
            with _token_lock:
                # Double-check: 네트워크 I/O 중 다른 스레드가 토큰 발급했을 수 있음
                if key in token_store:
                    stored = token_store[key]
                    if stored.get('expires') and datetime.fromisoformat(stored['expires']) > now:
                        print(f"[KIS] 토큰 중복발급 방지 ({mode}) - 다른 스레드에서 이미 발급됨")
                        return stored['token']
                
                token_store[key] = {'token': token, 'expires': expires}
                print(f"[KIS] 토큰 발급 성공 ({mode}) - 만료: {expires}")
                return token
                
    except urllib.error.HTTPError as e:
        err_body = e.read().decode('utf-8')
        print(f"[KIS] 토큰 오류 {e.code}: {err_body}")
        raise Exception(f"KIS 토큰 오류 {e.code}: {err_body}")
    except Exception as e:
        print(f"[KIS] 토큰 연결 실패: {str(e)}")
        raise

def kis_request(method, path, app_key, app_secret, mode, token, tr_id, params=None, body=None):
    """Make a KIS API request with auto token refresh on 401"""
    base = kis_base_url(mode)
    
    if method == "GET" and params:
        query = urllib.parse.urlencode(params)
        url = f"{base}{path}?{query}"
    else:
        url = f"{base}{path}"
    
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": tr_id,
        "custtype": "P"
    }
    
    if method == "POST" and body:
        data = json.dumps(body).encode('utf-8')
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    else:
        req = urllib.request.Request(url, headers=headers)
    
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        # ★ FIX: 401 토큰 만료 시 자동 재발급 후 1회 재시도
        if e.code == 401:
            print(f"[KIS] 401 토큰 만료 감지 → 재발급 후 재시도 ({path})")
            # 캐시에서 강제 삭제하여 재발급 유도
            with _token_lock:
                if mode in token_store:
                    del token_store[mode]
            new_token = kis_get_token(app_key, app_secret, mode)
            headers["authorization"] = f"Bearer {new_token}"
            if method == "POST" and body:
                req2 = urllib.request.Request(url, data=json.dumps(body).encode('utf-8'), headers=headers, method="POST")
            else:
                req2 = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req2, timeout=30) as resp2:
                return json.loads(resp2.read().decode('utf-8'))
        # ★ 500/503 일시적 서버 에러 → 최대 2회 재시도 (1초 간격)
        if e.code in (500, 503, 502):
            for _retry in range(2):
                time.sleep(1)
                try:
                    req_r = urllib.request.Request(url,
                        data=json.dumps(body).encode('utf-8') if method=="POST" and body else None,
                        headers=headers, method=method)
                    with urllib.request.urlopen(req_r, timeout=30) as resp_r:
                        return json.loads(resp_r.read().decode('utf-8'))
                except urllib.error.HTTPError as e2:
                    if _retry == 1:
                        raise e2
                    continue
                except Exception:
                    if _retry == 1:
                        raise
        raise  # 401/404 등 다른 오류는 그대로 전파

# ============= v4.0 PHASE 1: 기술적 지표 엔진 =============

def fetch_daily_candles(app_key, app_secret, mode, token, ticker, days=60):
    """KIS API로 일봉 데이터 조회 (최근 N일)
    Returns: [{'date','open','high','low','close','volume'}, ...]
    """
    try:
        today = datetime.now().strftime('%Y%m%d')
        start = (datetime.now() - timedelta(days=days+10)).strftime('%Y%m%d')
        result = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-daily-price",
            app_key, app_secret, mode, token, "FHKST01010400",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker,
                    "FID_INPUT_DATE_1": start, "FID_INPUT_DATE_2": today,
                    "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"})
        candles = []
        for item in result.get('output', []):
            try:
                candles.append({
                    'date': item.get('stck_bsop_date', ''),
                    'open': float(item.get('stck_oprc', 0)),
                    'high': float(item.get('stck_hgpr', 0)),
                    'low': float(item.get('stck_lwpr', 0)),
                    'close': float(item.get('stck_clpr', 0)),
                    'volume': int(item.get('acml_vol', 0)),
                })
            except:
                continue
        candles.sort(key=lambda x: x['date'])
        return candles[-days:] if len(candles) > days else candles
    except Exception as e:
        print(f"[CANDLE] {ticker} 일봉 조회 실패: {e}")
        return []

# ── 기술적 지표 캐시 (종목별, 10분 TTL)
_ta_cache = {}  # {ticker: {'data': {...}, 'ts': timestamp}}
TA_CACHE_TTL = 600  # 10분

def calc_technical_indicators(candles):
    """일봉 데이터로 기술적 지표 계산
    Returns: {rsi, macd, macd_signal, macd_hist, bb_upper, bb_middle, bb_lower,
              ma5, ma20, ma60, atr, volume_ratio, trend}
    """
    if len(candles) < 20:
        return {}
    
    closes = [c['close'] for c in candles]
    highs = [c['high'] for c in candles]
    lows = [c['low'] for c in candles]
    volumes = [c['volume'] for c in candles]
    
    result = {}
    
    # 1. RSI (14일)
    try:
        period = 14
        if len(closes) >= period + 1:
            deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
            gains = [max(d, 0) for d in deltas]
            losses = [abs(min(d, 0)) for d in deltas]
            avg_gain = sum(gains[:period]) / period
            avg_loss = sum(losses[:period]) / period
            for i in range(period, len(deltas)):
                avg_gain = (avg_gain * (period - 1) + gains[i]) / period
                avg_loss = (avg_loss * (period - 1) + losses[i]) / period
            if avg_loss == 0:
                result['rsi'] = 100.0
            else:
                rs = avg_gain / avg_loss
                result['rsi'] = round(100 - (100 / (1 + rs)), 1)
    except:
        pass
    
    # 2. MACD (12, 26, 9)
    try:
        if len(closes) >= 26:
            def ema(data, period):
                k = 2 / (period + 1)
                e = [data[0]]
                for i in range(1, len(data)):
                    e.append(data[i] * k + e[-1] * (1 - k))
                return e
            ema12 = ema(closes, 12)
            ema26 = ema(closes, 26)
            macd_line = [ema12[i] - ema26[i] for i in range(len(closes))]
            signal_line = ema(macd_line, 9)
            result['macd'] = round(macd_line[-1], 2)
            result['macd_signal'] = round(signal_line[-1], 2)
            result['macd_hist'] = round(macd_line[-1] - signal_line[-1], 2)
            # MACD 크로스 감지
            if len(macd_line) >= 2:
                prev_diff = macd_line[-2] - signal_line[-2]
                curr_diff = macd_line[-1] - signal_line[-1]
                if prev_diff < 0 and curr_diff >= 0:
                    result['macd_cross'] = 'golden'  # 골든크로스
                elif prev_diff > 0 and curr_diff <= 0:
                    result['macd_cross'] = 'dead'  # 데드크로스
                else:
                    result['macd_cross'] = 'none'
    except:
        pass
    
    # 3. 볼린저 밴드 (20일, 2σ)
    try:
        period = 20
        if len(closes) >= period:
            sma20 = sum(closes[-period:]) / period
            variance = sum((c - sma20) ** 2 for c in closes[-period:]) / period
            std = variance ** 0.5
            result['bb_upper'] = round(sma20 + 2 * std, 0)
            result['bb_middle'] = round(sma20, 0)
            result['bb_lower'] = round(sma20 - 2 * std, 0)
            # 볼린저 밴드 위치 (0~100, 0=하단, 100=상단)
            bb_width = result['bb_upper'] - result['bb_lower']
            if bb_width > 0:
                result['bb_position'] = round((closes[-1] - result['bb_lower']) / bb_width * 100, 1)
            # 밴드폭 (변동성 지표)
            result['bb_width_pct'] = round(bb_width / sma20 * 100, 2) if sma20 > 0 else 0
    except:
        pass
    
    # 4. 이동평균
    try:
        for p in [5, 20, 60]:
            if len(closes) >= p:
                result[f'ma{p}'] = round(sum(closes[-p:]) / p, 0)
        # 이동평균 배열 (정배열/역배열)
        if all(f'ma{p}' in result for p in [5, 20, 60]):
            if result['ma5'] > result['ma20'] > result['ma60']:
                result['ma_alignment'] = 'bullish'  # 정배열 (상승추세)
            elif result['ma5'] < result['ma20'] < result['ma60']:
                result['ma_alignment'] = 'bearish'  # 역배열 (하락추세)
            else:
                result['ma_alignment'] = 'mixed'  # 혼조
    except:
        pass
    
    # 5. ATR (Average True Range, 14일) — 동적 tp/sl의 핵심
    try:
        period = 14
        if len(candles) >= period + 1:
            tr_list = []
            for i in range(1, len(candles)):
                h = candles[i]['high']
                l = candles[i]['low']
                pc = candles[i-1]['close']
                tr = max(h - l, abs(h - pc), abs(l - pc))
                tr_list.append(tr)
            atr = sum(tr_list[-period:]) / period
            result['atr'] = round(atr, 0)
            result['atr_pct'] = round(atr / closes[-1] * 100, 2) if closes[-1] > 0 else 0
    except:
        pass
    
    # 6. 거래량 비율 (오늘 vs 20일 평균)
    try:
        if len(volumes) >= 20 and volumes[-1] > 0:
            avg_vol20 = sum(volumes[-21:-1]) / 20 if len(volumes) > 20 else sum(volumes[:-1]) / max(len(volumes)-1, 1)
            result['volume_ratio'] = round(volumes[-1] / max(avg_vol20, 1), 2)
    except:
        pass
    
    # 7. 종합 추세 판단
    try:
        score = 0
        # ★ v6.0: RSI 가중치 강화 (로스 카메론 방식)
        _rsi = result.get('rsi', 50)
        if 50 <= _rsi <= 70: score += 2    # 모멘텀 확인 구간 ★최적
        elif 70 < _rsi <= 80: score += 1   # 강한 모멘텀 (주의)
        elif _rsi > 80: score -= 3          # 과매수 천정 → 강력 감점
        elif 30 <= _rsi < 50: score += 0    # 약한 모멘텀 (가산 없음)
        elif _rsi < 30: score -= 3           # 폭락 중 → 강력 감점
        if result.get('macd_hist', 0) > 0: score += 1
        if result.get('macd_cross') == 'golden': score += 2
        if result.get('macd_cross') == 'dead': score -= 2
        if result.get('ma_alignment') == 'bullish': score += 2
        if result.get('ma_alignment') == 'bearish': score -= 2
        if result.get('bb_position', 50) < 20: score += 1  # 밴드 하단 → 반등 기대
        if result.get('bb_position', 50) > 80: score -= 1  # 밴드 상단 → 과열
        if result.get('volume_ratio', 1) > 2: score += 1  # 거래량 급증
        
        if score >= 3: result['trend'] = '강한상승'
        elif score >= 1: result['trend'] = '상승'
        elif score <= -3: result['trend'] = '강한하락'
        elif score <= -1: result['trend'] = '하락'
        else: result['trend'] = '중립'
        result['trend_score'] = score
    except:
        result['trend'] = '판단불가'
        result['trend_score'] = 0
    
    # 8. ★★★ v4.1: 최근 5일 차트 흐름 (AI 종목 판단 핵심) ★★★
    try:
        recent = candles[-5:] if len(candles) >= 5 else candles
        chart_flow = []
        for i, c in enumerate(recent):
            prev_close = recent[i-1]['close'] if i > 0 else candles[-6]['close'] if len(candles) > 5 else c['open']
            chg = round((c['close'] - prev_close) / max(prev_close, 1) * 100, 1)
            chart_flow.append({
                'date': c.get('date', ''),
                'open': c['open'], 'high': c['high'], 'low': c['low'], 'close': c['close'],
                'volume': c['volume'], 'chg_pct': chg
            })
        result['chart_flow'] = chart_flow
        
        # 패턴 자동 판단
        if len(recent) >= 3:
            _chgs = [cf['chg_pct'] for cf in chart_flow]
            _vols = [cf['volume'] for cf in chart_flow]
            _up_days = sum(1 for c in _chgs if c > 0)
            _prices = [c['close'] for c in recent]
            _high5 = max(c['high'] for c in recent)
            _low5 = min(c['low'] for c in recent)
            
            # 패턴 분류
            if _up_days >= 4:
                result['chart_pattern'] = '연속상승(과열주의)'
            elif _up_days >= 3 and _chgs[-1] > 0:
                result['chart_pattern'] = '상승추세(매수적합)'
            elif _chgs[-2] < -1 and _chgs[-1] > 0.5:
                result['chart_pattern'] = '눌림목반등(매수적합)'
            elif _up_days <= 1:
                result['chart_pattern'] = '하락추세(매수위험)'
            elif _chgs[-1] < -1 and _chgs[-2] > 0:
                result['chart_pattern'] = '하락전환(매수위험)'
            else:
                result['chart_pattern'] = '횡보(관망)'
            
            result['high_5d'] = _high5
            result['low_5d'] = _low5
            result['support'] = _low5  # 단순 지지선
            result['resistance'] = _high5  # 단순 저항선
    except:
        pass
    
    return result

def get_technical_indicators(app_key, app_secret, mode, token, ticker):
    """기술적 지표 조회 (캐시 적용)"""
    now = time.time()
    cached = _ta_cache.get(ticker)
    if cached and (now - cached['ts']) < TA_CACHE_TTL:
        return cached['data']
    
    candles = fetch_daily_candles(app_key, app_secret, mode, token, ticker)
    if not candles:
        return {}
    
    ta = calc_technical_indicators(candles)
    _ta_cache[ticker] = {'data': ta, 'ts': now}
    print(f"[TA] {ticker}: RSI={ta.get('rsi','?')} MACD={ta.get('macd_hist','?')} "
          f"BB={ta.get('bb_position','?')}% ATR={ta.get('atr_pct','?')}% 추세={ta.get('trend','?')}")
    return ta

def build_ta_context(ta, ticker='', name=''):
    """기술적 지표를 AI 프롬프트용 텍스트로 변환"""
    if not ta:
        return ""
    ctx = f"\n[기술적 분석 {name}({ticker})]" if ticker else "\n[기술적 분석]"
    if 'rsi' in ta:
        rsi = ta['rsi']
        rsi_label = '과매수⚠️' if rsi > 70 else ('과매도🟢' if rsi < 30 else '중립')
        ctx += f"\nRSI(14): {rsi} ({rsi_label})"
    if 'macd' in ta:
        cross = {'golden': '골든크로스🟢', 'dead': '데드크로스🔴', 'none': ''}.get(ta.get('macd_cross',''), '')
        ctx += f"\nMACD: {ta['macd']} / 시그널: {ta.get('macd_signal','')} / 히스토그램: {ta.get('macd_hist','')} {cross}"
    if 'bb_position' in ta:
        bp = ta['bb_position']
        bp_label = '밴드상단(과열)' if bp > 80 else ('밴드하단(반등기대)' if bp < 20 else '밴드중간')
        ctx += f"\n볼린저: 상{ta.get('bb_upper','')} / 중{ta.get('bb_middle','')} / 하{ta.get('bb_lower','')} | 위치: {bp}% ({bp_label})"
        ctx += f"\n밴드폭: {ta.get('bb_width_pct','')}% {'(수렴→큰움직임예고)' if ta.get('bb_width_pct',0) < 3 else ''}"
    if 'ma5' in ta:
        ctx += f"\n이동평균: 5일={ta.get('ma5','')} / 20일={ta.get('ma20','')} / 60일={ta.get('ma60','')}"
        align = {'bullish': '정배열(상승추세)🟢', 'bearish': '역배열(하락추세)🔴', 'mixed': '혼조'}.get(ta.get('ma_alignment',''), '')
        if align: ctx += f" → {align}"
    if 'atr_pct' in ta:
        ctx += f"\nATR(14): ₩{ta.get('atr','')} ({ta.get('atr_pct','')}%) — 일평균 변동폭"
    if 'volume_ratio' in ta:
        vr = ta['volume_ratio']
        vr_label = '🔥거래량폭발' if vr > 3 else ('📈거래량증가' if vr > 1.5 else ('📉거래량감소' if vr < 0.5 else ''))
        ctx += f"\n거래량비율: {vr}배 (20일평균대비) {vr_label}"
    ctx += f"\n★ 종합추세: {ta.get('trend','')} (점수 {ta.get('trend_score',0)}/10)"
    
    # ★★★ v4.1: 최근 5일 차트 흐름 ★★★
    chart_flow = ta.get('chart_flow', [])
    if chart_flow:
        ctx += f"\n[최근 {len(chart_flow)}일 차트]"
        for cf in chart_flow:
            _sign = '+' if cf['chg_pct'] >= 0 else ''
            _arrow = '📈' if cf['chg_pct'] > 1 else ('📉' if cf['chg_pct'] < -1 else '➡️')
            _vol_k = cf['volume'] // 1000
            ctx += f"\n  {cf.get('date','')[-5:]}: {cf['close']:,}원({_sign}{cf['chg_pct']}%) 거래{_vol_k:,}K {_arrow}"
        if ta.get('chart_pattern'):
            ctx += f"\n  → 패턴: {ta['chart_pattern']}"
        if ta.get('high_5d') and ta.get('low_5d'):
            ctx += f"\n  → 5일 고점:{ta['high_5d']:,} 저점:{ta['low_5d']:,} 지지:{ta.get('support',0):,} 저항:{ta.get('resistance',0):,}"
    
    return ctx

# ============= v4.0 PHASE 1: 호가잔량 + 체결강도 분석 =============

_orderbook_cache = {}  # {ticker: {'data': {...}, 'ts': timestamp}}
ORDERBOOK_CACHE_TTL = 15  # 15초 (호가는 빠르게 변함)

def fetch_orderbook(app_key, app_secret, mode, token, ticker):
    """KIS API로 호가잔량 + 체결강도 조회
    Returns: {bid_total, ask_total, strength, bid_wall, ask_wall, spread_pct, signal}
    """
    now = time.time()
    cached = _orderbook_cache.get(ticker)
    if cached and (now - cached['ts']) < ORDERBOOK_CACHE_TTL:
        return cached['data']
    
    result = {}
    try:
        # 호가 조회 (10호가)
        ob = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
            app_key, app_secret, mode, token, "FHKST01010200",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker})
        
        output = ob.get('output1', ob.get('output', {}))
        if not output:
            return {}
        
        # 매수호가 잔량 합계 (1~10호가)
        bid_total = 0
        ask_total = 0
        bids = []
        asks = []
        for i in range(1, 11):
            bid_qty = int(output.get(f'bidp_rsqn{i}', '0') or 0)
            ask_qty = int(output.get(f'askp_rsqn{i}', '0') or 0)
            bid_price = int(output.get(f'bidp{i}', '0') or 0)
            ask_price = int(output.get(f'askp{i}', '0') or 0)
            bid_total += bid_qty
            ask_total += ask_qty
            if bid_qty > 0:
                bids.append({'price': bid_price, 'qty': bid_qty})
            if ask_qty > 0:
                asks.append({'price': ask_price, 'qty': ask_qty})
        
        result['bid_total'] = bid_total
        result['ask_total'] = ask_total
        
        # ★ v4.1: 최우선호가 (매수/매도 지정가 주문용)
        result['bid1_price'] = bids[0]['price'] if bids else 0  # 매수1호가 (매도 시 사용)
        result['ask1_price'] = asks[0]['price'] if asks else 0  # 매도1호가 (매수 시 사용)
        
        # 체결강도 (매수잔량/매도잔량 × 100)
        if ask_total > 0:
            result['strength'] = round(bid_total / ask_total * 100, 1)
        else:
            result['strength'] = 999  # 매도 없음 → 극강
        
        # 매수벽 감지 (1호가 잔량이 평균의 3배 이상)
        if bids:
            avg_bid = bid_total / len(bids)
            result['bid_wall'] = bids[0]['qty'] > avg_bid * 3  # 1호가에 대량 매수
            result['bid_wall_price'] = bids[0]['price'] if result['bid_wall'] else 0
            result['bid_wall_qty'] = bids[0]['qty'] if result['bid_wall'] else 0
        
        # 매도벽 감지
        if asks:
            avg_ask = ask_total / len(asks)
            result['ask_wall'] = asks[0]['qty'] > avg_ask * 3  # 1호가에 대량 매도
            result['ask_wall_price'] = asks[0]['price'] if result['ask_wall'] else 0
        
        # 호가 스프레드 (1호가 기준)
        if bids and asks and bids[0]['price'] > 0:
            spread = asks[0]['price'] - bids[0]['price']
            result['spread'] = spread
            result['spread_pct'] = round(spread / bids[0]['price'] * 100, 3)
        
        # 종합 시그널
        strength = result.get('strength', 100)
        if strength >= 150 and result.get('bid_wall', False):
            result['signal'] = '강한매수세🟢'
        elif strength >= 120:
            result['signal'] = '매수우위🟢'
        elif strength <= 60:
            result['signal'] = '강한매도세🔴'
        elif strength <= 80:
            result['signal'] = '매도우위🔴'
        else:
            result['signal'] = '균형'
        
        _orderbook_cache[ticker] = {'data': result, 'ts': now}
        print(f"[ORDERBOOK] {ticker}: 체결강도={strength}% 매수벽={'O' if result.get('bid_wall') else 'X'} "
              f"스프레드={result.get('spread_pct',0):.3f}% {result.get('signal','')}")
    except Exception as e:
        print(f"[ORDERBOOK] {ticker} 조회 실패: {e}")
    
    return result

def build_orderbook_context(ob, ticker=''):
    """호가 분석 결과를 AI 프롬프트용 텍스트로 변환"""
    if not ob:
        return ""
    ctx = f"\n[호가분석 {ticker}]" if ticker else "\n[호가분석]"
    ctx += f"\n체결강도: {ob.get('strength','')}% ({ob.get('signal','')})"
    ctx += f"\n매수잔량: {ob.get('bid_total',0):,} / 매도잔량: {ob.get('ask_total',0):,}"
    if ob.get('bid_wall'):
        ctx += f"\n⚡ 매수벽 감지: ₩{ob.get('bid_wall_price',0):,} × {ob.get('bid_wall_qty',0):,}주 (강한 지지)"
    if ob.get('ask_wall'):
        ctx += f"\n⚠️ 매도벽 감지: ₩{ob.get('ask_wall_price',0):,} (저항선)"
    if ob.get('spread_pct', 0) > 0.3:
        ctx += f"\n⚠️ 스프레드 {ob['spread_pct']:.2f}% (넓음 → 슬리피지 주의)"
    return ctx

# ============= v4.0 PHASE 1: ATR 기반 동적 tp/sl 계산 =============

def calc_dynamic_tp_sl(ta, base_tp1=5, base_tp2=15, base_sl=-5, base_trail=3):
    """종목별 ATR 기반 동적 tp/sl 계산
    
    ★ 핵심: 사용자 설정값(base)을 기준으로 ATR로 미세 조정
    - ATR 높은 종목(고변동성) → 사용자 설정의 최대 130%까지 확대
    - ATR 낮은 종목(저변동성) → 사용자 설정의 최소 80%까지 축소
    - 사용자 설정을 절대 무시하지 않음
    
    Returns: (tp1, tp2, sl, trailing, reason)
    """
    atr_pct = ta.get('atr_pct', 0) if ta else 0
    
    if atr_pct <= 0:
        return base_tp1, base_tp2, base_sl, base_trail, 'ATR없음(기본값)'
    
    # ★ ATR 기반 스케일링 팩터 (0.8 ~ 1.3 범위)
    # 기준: ATR 3% = 보통 → 스케일 1.0
    # ATR 1% = 저변동 → 스케일 0.8 (tp/sl 축소)
    # ATR 6% = 고변동 → 스케일 1.3 (tp/sl 확대)
    _scale = max(0.8, min(1.3, atr_pct / 3.0))
    
    tp1 = round(max(1.5, base_tp1 * _scale), 1)
    tp2 = round(max(3.0, base_tp2 * _scale), 1)
    sl = round(min(-1.0, base_sl * _scale), 1)
    trail = round(max(1.0, base_trail * _scale), 1)
    
    # RSI 보정: 과매수 → tp 낮춤(빨리 익절), 과매도 → tp 높임(여유)
    rsi = ta.get('rsi', 50)
    if rsi > 70:
        tp1 = round(tp1 * 0.85, 1)
        tp2 = round(tp2 * 0.85, 1)
        reason = f'ATR{atr_pct:.1f}%×{_scale:.1f}+과매수RSI{rsi}'
    elif rsi < 30:
        tp1 = round(tp1 * 1.15, 1)
        sl = round(sl * 1.2, 1)  # 과매도 → 손절 여유
        reason = f'ATR{atr_pct:.1f}%×{_scale:.1f}+과매도RSI{rsi}'
    else:
        reason = f'ATR{atr_pct:.1f}%×{_scale:.1f}'
    
    return tp1, tp2, sl, trail, reason

# ============= v4.0 PHASE 1: 동적 포지션 사이징 =============

def calc_position_size(max_buy_amount, confidence, cur_price, ta=None, ob=None):
    """1회 최대 주문금액에 맞춰 수량 계산 (AI 추천 = 풀매수)
    
    confidence: 참고용 로그만 (감액 안 함)
    Returns: (amount, qty, reason)
    """
    amount = int(max_buy_amount)
    
    if cur_price > amount:
        print(f"[POS_SIZE] 주가 ₩{cur_price:,} > 한도 ₩{amount:,} → 매수 불가")
        return 0, 0, f"주가초과(₩{cur_price:,}>₩{amount:,})"
    
    qty = max(1, int(amount / cur_price)) if cur_price > 0 else 1
    actual_amount = qty * cur_price
    
    reason_str = f'확신도{confidence}% | 한도₩{max_buy_amount:,.0f}→{qty}주'
    print(f"[POS_SIZE] ₩{max_buy_amount:,} → {qty}주 × ₩{cur_price:,} = ₩{actual_amount:,.0f} | {reason_str}")
    
    return actual_amount, qty, reason_str
    
    reason_str = ' / '.join(reasons)
    print(f"[POS_SIZE] ₩{max_buy_amount:,} × {ratio:.0%} = ₩{actual_amount:,.0f} ({qty}주) | {reason_str}")
    
    return actual_amount, qty, reason_str

# ============= v4.0 PHASE 1: 분할매수/매도 엔진 =============

# 분할매수 추적: {ticker: {'phase': 1or2, 'qty1': N, 'price1': P, 'ts': time}}
_split_buy_tracker = {}

def plan_split_buy(total_qty, cur_price, ta=None):
    """분할매수 계획 수립
    1차: 60% 즉시 시장가
    2차: 40% 눌림목(-1% 이상) 시 추가매수 (2분 대기)
    
    Returns: (qty1, qty2, plan_text)
    """
    if total_qty <= 2:
        return total_qty, 0, '소량→일괄매수'
    
    # 기술적 지표에 따라 비율 조정
    if ta and ta.get('trend_score', 0) >= 3:
        # 강한 상승 → 1차 비중 높임 (빠른 진입)
        ratio1 = 0.75
        plan = '강세→1차75%'
    elif ta and ta.get('trend_score', 0) <= -1:
        # 하락추세 → 1차 비중 낮춤 (신중)
        ratio1 = 0.50
        plan = '약세→1차50%'
    else:
        ratio1 = 0.60
        plan = '보통→1차60%'
    
    qty1 = max(1, int(total_qty * ratio1))
    qty2 = total_qty - qty1
    
    if qty2 <= 0:
        return total_qty, 0, f'{plan}(잔량부족→일괄)'
    
    return qty1, qty2, plan

def check_split_buy_phase2(ticker, cur_price, config):
    """분할매수 2차 진입 조건 체크
    Returns: (should_buy, qty, reason) or (False, 0, '')
    """
    tracker = _split_buy_tracker.get(ticker)
    if not tracker or tracker.get('phase') != 1:
        return False, 0, ''
    
    elapsed = time.time() - tracker['ts']
    price1 = tracker['price1']
    qty2 = tracker.get('qty2', 0)
    
    if qty2 <= 0:
        return False, 0, ''
    
    # 조건 1: 최소 30초 경과
    if elapsed < 30:
        return False, 0, ''
    
    # 조건 2: 5분 초과 → 2차 취소
    if elapsed > 300:
        del _split_buy_tracker[ticker]
        print(f"[SPLIT_BUY] {ticker} 2차 매수 타임아웃 (5분)")
        return False, 0, ''
    
    # 조건 3: 1차 매수가 대비 눌림 체크
    dip_pct = (cur_price - price1) / price1 * 100 if price1 > 0 else 0
    
    if dip_pct <= -0.8:
        # 눌림목 진입! (-0.8% 이상 하락)
        reason = f'분할2차: 눌림{dip_pct:.1f}% ({elapsed:.0f}초 후)'
        del _split_buy_tracker[ticker]
        return True, qty2, reason
    elif dip_pct >= 2.0 and elapsed > 60:
        # 즉시 +2% 상승 → 2차도 추격 매수 (모멘텀)
        reason = f'분할2차: 모멘텀+{dip_pct:.1f}% ({elapsed:.0f}초 후)'
        del _split_buy_tracker[ticker]
        return True, qty2, reason
    
    return False, 0, ''

# ============= v4.0 PHASE 1: 주문 체결 추적 =============

# 미체결 주문 추적: {order_no: {ticker, name, qty, type, ts, price}}
_pending_orders = {}

def track_pending_order(order_no, ticker, name, qty, order_type, price=0):
    """미체결 주문 등록"""
    if order_no:
        _pending_orders[order_no] = {
            'ticker': ticker, 'name': name, 'qty': qty,
            'type': order_type, 'price': price, 'ts': time.time()
        }
        print(f"[TRACK] 주문 등록: {order_no} {name}({ticker}) {order_type} {qty}주")

def check_pending_orders(app_key, app_secret, mode, token, account, account_cd='01'):
    """미체결 주문 확인 + 오래된 주문 자동 취소 (3분 초과)"""
    if not _pending_orders:
        return
    
    try:
        # KIS 미체결 조회
        result = kis_request("GET", "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl",
            app_key, app_secret, mode, token, "TTTC8036R",
            params={"CANO": account, "ACNT_PRDT_CD": account_cd,
                    "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
                    "INQR_DVSN_1": "0", "INQR_DVSN_2": "0"})
        
        # 체결된 주문 제거
        filled_orders = set()
        pending_in_kis = set()
        for item in result.get('output', []):
            odno = item.get('odno', '')
            if odno in _pending_orders:
                remaining = int(item.get('psbl_qty', '0') or 0)
                if remaining <= 0:
                    filled_orders.add(odno)
                    print(f"[TRACK] ✅ 체결 완료: {odno} {_pending_orders[odno]['name']}")
                else:
                    pending_in_kis.add(odno)
        
        for odno in filled_orders:
            del _pending_orders[odno]
        
        # 3분 초과 미체결 → 자동 취소
        now = time.time()
        for odno, info in list(_pending_orders.items()):
            if now - info['ts'] > 180 and odno not in pending_in_kis:
                # KIS에도 없고 3분 지남 → 이미 체결되었거나 취소됨
                del _pending_orders[odno]
                print(f"[TRACK] 📋 추적 해제: {odno} (3분 경과)")
            elif now - info['ts'] > 180 and odno in pending_in_kis:
                # KIS에 아직 미체결 → 취소 시도
                try:
                    cancel_result = kis_request("POST", "/uapi/domestic-stock/v1/trading/order-rvsecncl",
                        app_key, app_secret, mode, token, "TTTC0803U",
                        body={"CANO": account, "ACNT_PRDT_CD": account_cd,
                              "KRX_FWDG_ORD_ORGNO": "", "ORGN_ODNO": odno,
                              "ORD_DVSN": "00", "RVSE_CNCL_DVSN_CD": "02",
                              "ORD_QTY": str(info['qty']), "ORD_UNPR": "0",
                              "QTY_ALL_ORD_YN": "Y"})
                    print(f"[TRACK] ❌ 미체결 취소: {odno} {info['name']} → {cancel_result.get('msg1','')}")
                    del _pending_orders[odno]
                except Exception as e:
                    print(f"[TRACK] 취소 실패: {odno} {e}")
    except Exception as e:
        print(f"[TRACK] 미체결 조회 실패: {e}")

# ============= v4.0 PHASE 2: KIS 실시간 WebSocket 시세 =============

_kis_ws_prices = {}  # {ticker: {'price': int, 'change': float, 'volume': int, 'ts': float}}
_kis_ws_connected = False
_kis_ws_subscribed = set()  # 구독 중인 종목

def kis_ws_get_approval_key(app_key, app_secret):
    """KIS WebSocket 접속 승인키 발급"""
    try:
        url = "https://openapi.koreainvestment.com:9443/oauth2/Approval"
        body = json.dumps({
            "grant_type": "client_credentials",
            "appkey": app_key,
            "secretkey": app_secret
        }).encode('utf-8')
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode('utf-8'))
        key = data.get('approval_key', '')
        if key:
            print(f"[KIS_WS] ✅ 승인키 발급 성공")
        return key
    except Exception as e:
        print(f"[KIS_WS] ❌ 승인키 발급 실패: {e}")
        return ''

def start_kis_ws_thread(app_key, app_secret, tickers):
    """KIS 실시간 시세 WebSocket 스레드 시작 (별도 스레드)"""
    def _ws_worker():
        global _kis_ws_connected, _kis_ws_prices, _kis_ws_subscribed
        
        try:
            import websocket as ws_lib
        except ImportError:
            print("[KIS_WS] ⚠️ websocket-client 미설치 → REST 폴링 모드 유지")
            print("[KIS_WS]    pip install websocket-client 로 설치 후 재시작")
            return
        
        approval_key = kis_ws_get_approval_key(app_key, app_secret)
        if not approval_key:
            return
        
        ws_url = "ws://ops.koreainvestment.com:21000"
        
        def on_message(ws, message):
            """실시간 체결가 수신 처리"""
            try:
                # KIS WebSocket 메시지 파싱 (|로 구분)
                if '|' in message:
                    parts = message.split('|')
                    if len(parts) >= 4:
                        header = parts[0]
                        tr_id = parts[1]
                        count = parts[2]
                        data_str = parts[3]
                        
                        if tr_id == 'H0STCNT0':  # 실시간 체결
                            fields = data_str.split('^')
                            if len(fields) >= 20:
                                ticker = fields[0]
                                cur_price = int(fields[2]) if fields[2] else 0
                                change_pct = float(fields[5]) if fields[5] else 0
                                volume = int(fields[12]) if fields[12] else 0
                                
                                if cur_price > 0:
                                    _kis_ws_prices[ticker] = {
                                        'price': cur_price,
                                        'change': change_pct,
                                        'volume': volume,
                                        'ts': time.time()
                                    }
                else:
                    # JSON 응답 (구독 확인 등)
                    try:
                        resp = json.loads(message)
                        if resp.get('header', {}).get('tr_id') == 'PINGPONG':
                            ws.send(message)  # PONG 응답
                    except:
                        pass
            except Exception as e:
                pass  # 파싱 실패는 무시 (다음 메시지 기다림)
        
        def on_open(ws):
            global _kis_ws_connected
            _kis_ws_connected = True
            print(f"[KIS_WS] ✅ WebSocket 연결 성공")
            
            # 보유종목 구독
            for ticker in tickers:
                _subscribe_ticker(ws, approval_key, ticker)
        
        def on_close(ws, close_status_code, close_msg):
            global _kis_ws_connected
            _kis_ws_connected = False
            print(f"[KIS_WS] ⚠️ WebSocket 연결 종료 → 10초 후 재연결")
            time.sleep(10)
        
        def on_error(ws, error):
            print(f"[KIS_WS] ❌ WebSocket 에러: {error}")
        
        # 재연결 루프
        while True:
            try:
                ws = ws_lib.WebSocketApp(ws_url,
                    on_message=on_message,
                    on_open=on_open,
                    on_close=on_close,
                    on_error=on_error)
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                print(f"[KIS_WS] 재연결 대기: {e}")
                time.sleep(10)
            
            # 장 종료 시 루프 탈출
            hm = datetime.now().hour * 100 + datetime.now().minute
            if hm > 1540:
                print("[KIS_WS] 장 종료 → WebSocket 종료")
                break
    
    t = threading.Thread(target=_ws_worker, daemon=True)
    t.start()
    print(f"[KIS_WS] 🔌 실시간 시세 스레드 시작 ({len(tickers)}종목)")
    return t

def _subscribe_ticker(ws, approval_key, ticker):
    """종목 실시간 체결가 구독"""
    global _kis_ws_subscribed
    if ticker in _kis_ws_subscribed:
        return
    try:
        sub_msg = json.dumps({
            "header": {
                "approval_key": approval_key,
                "custtype": "P",
                "tr_type": "1",
                "content-type": "utf-8"
            },
            "body": {
                "input": {
                    "tr_id": "H0STCNT0",
                    "tr_key": ticker
                }
            }
        })
        ws.send(sub_msg)
        _kis_ws_subscribed.add(ticker)
        print(f"[KIS_WS] 📡 {ticker} 구독 시작")
    except Exception as e:
        print(f"[KIS_WS] 구독 실패 {ticker}: {e}")

def get_realtime_price(ticker):
    """실시간 가격 조회 — KIS WebSocket 우선, 없으면 REST 캐시"""
    ws_data = _kis_ws_prices.get(ticker)
    if ws_data and (time.time() - ws_data['ts']) < 5:
        return ws_data['price']
    
    # REST 캐시 fallback
    cached = _price_cache.get(ticker)
    if cached and (time.time() - cached[1]) < PRICE_CACHE_TTL:
        return cached[0]
    
    return 0

# ============= v4.0 PHASE 2: 병렬 AI 스캔 엔진 =============

from concurrent.futures import ThreadPoolExecutor, as_completed

_scan_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix='ai_scan')

def parallel_ta_fetch(app_key, app_secret, mode, token, tickers):
    """여러 종목의 기술적 지표를 병렬 조회 (3종목 동시)
    Returns: {ticker: ta_data}
    """
    results = {}
    futures = {}
    
    for ticker in tickers[:10]:  # 최대 10종목
        future = _scan_executor.submit(
            get_technical_indicators, app_key, app_secret, mode, token, ticker)
        futures[future] = ticker
    
    for future in as_completed(futures, timeout=30):
        ticker = futures[future]
        try:
            ta = future.result()
            if ta:
                results[ticker] = ta
        except Exception as e:
            print(f"[PARALLEL_TA] {ticker} 실패: {e}")
    
    print(f"[PARALLEL_TA] {len(results)}/{len(tickers)}종목 병렬 조회 완료")
    return results

def parallel_orderbook_fetch(app_key, app_secret, mode, token, tickers):
    """여러 종목의 호가잔량을 병렬 조회
    Returns: {ticker: ob_data}
    """
    results = {}
    futures = {}
    
    for ticker in tickers[:10]:
        future = _scan_executor.submit(
            fetch_orderbook, app_key, app_secret, mode, token, ticker)
        futures[future] = ticker
    
    for future in as_completed(futures, timeout=15):
        ticker = futures[future]
        try:
            ob = future.result()
            if ob:
                results[ticker] = ob
        except Exception as e:
            print(f"[PARALLEL_OB] {ticker} 실패: {e}")
    
    print(f"[PARALLEL_OB] {len(results)}/{len(tickers)}종목 호가 병렬 조회 완료")
    return results

# ============= v4.0 PHASE 2: 투자자별 수급 실시간 조회 =============

_investor_cache = {}  # {ticker: {'data': {...}, 'ts': float}}
INVESTOR_CACHE_TTL = 180  # 3분

def fetch_investor_trend(app_key, app_secret, mode, token, ticker):
    """KIS API로 투자자별 매매동향 조회 (외국인/기관/개인)
    Returns: {'foreign': +/-금액, 'institution': +/-금액, 'individual': +/-금액, 'signal': str}
    """
    now = time.time()
    cached = _investor_cache.get(ticker)
    if cached and (now - cached['ts']) < INVESTOR_CACHE_TTL:
        return cached['data']
    
    result = {}
    try:
        today = datetime.now().strftime('%Y%m%d')
        resp = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-investor",
            app_key, app_secret, mode, token, "FHKST01010900",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker})
        
        items = resp.get('output', [])
        if items:
            item = items[0]  # 당일 데이터
            foreign = int(item.get('frgn_ntby_qty', '0') or 0)
            institution = int(item.get('orgn_ntby_qty', '0') or 0)
            individual = int(item.get('prsn_ntby_qty', '0') or 0)
            
            result = {
                'foreign': foreign,
                'institution': institution, 
                'individual': individual,
                'foreign_amt': int(item.get('frgn_ntby_tr_pbmn', '0') or 0),
                'institution_amt': int(item.get('orgn_ntby_tr_pbmn', '0') or 0),
            }
            
            # 수급 시그널
            signals = []
            if foreign > 0: signals.append(f'외인+{foreign:,}주')
            if foreign < 0: signals.append(f'외인{foreign:,}주')
            if institution > 0: signals.append(f'기관+{institution:,}주')
            if institution < 0: signals.append(f'기관{institution:,}주')
            
            if foreign > 0 and institution > 0:
                result['signal'] = f'🟢쌍끌이매수({", ".join(signals)})'
            elif foreign > 0 or institution > 0:
                result['signal'] = f'📈수급양호({", ".join(signals)})'
            elif foreign < 0 and institution < 0:
                result['signal'] = f'🔴쌍매도({", ".join(signals)})'
            else:
                result['signal'] = f'중립({", ".join(signals)})'
            
            _investor_cache[ticker] = {'data': result, 'ts': now}
            print(f"[INVESTOR] {ticker}: {result['signal']}")
    except Exception as e:
        print(f"[INVESTOR] {ticker} 조회 실패: {e}")
    
    return result

def build_investor_context(inv, ticker=''):
    """투자자 수급 데이터를 AI 프롬프트 텍스트로 변환"""
    if not inv:
        return ""
    ctx = f"\n수급: {inv.get('signal','')}"
    if inv.get('foreign_amt'):
        ctx += f" | 외인 {inv['foreign_amt']:,}원"
    if inv.get('institution_amt'):
        ctx += f" | 기관 {inv['institution_amt']:,}원"
    return ctx

# ============= v4.0 PHASE 3: 멀티타임프레임 분석 (1분/5분/15분봉) =============

_mtf_cache = {}  # {ticker: {'data': {...}, 'ts': float}}
MTF_CACHE_TTL = 120  # 2분

def fetch_minute_candles(app_key, app_secret, mode, token, ticker, period='1'):
    """KIS API 분봉 데이터 조회 (1분/5분/15분/30분/60분)
    Returns: [{'time','open','high','low','close','volume'}, ...]
    """
    try:
        now = datetime.now()
        time_str = now.strftime('%H%M%S')
        result = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
            app_key, app_secret, mode, token, "FHKST03010200",
            params={"FID_ETC_CLS_CODE": "", "FID_COND_MRKT_DIV_CODE": "J",
                    "FID_INPUT_ISCD": ticker, "FID_INPUT_HOUR_1": time_str,
                    "FID_PW_DATA_INCU_YN": "N"})
        candles = []
        for item in result.get('output2', result.get('output', [])):
            try:
                t = item.get('stck_cntg_hour', '')
                candles.append({
                    'time': f"{t[:2]}:{t[2:4]}:{t[4:6]}" if len(t) >= 6 else t,
                    'open': float(item.get('stck_oprc', 0)),
                    'high': float(item.get('stck_hgpr', 0)),
                    'low': float(item.get('stck_lwpr', 0)),
                    'close': float(item.get('stck_prpr', 0)),
                    'volume': int(item.get('cntg_vol', 0)),
                })
            except:
                continue
        candles.sort(key=lambda x: x['time'])
        return candles
    except Exception as e:
        print(f"[MTF] {ticker} 분봉 조회 실패: {e}")
        return []

# ═══════════════════════════════════════════════════════
# ★★★ v6.0: 급등주 흔들기 패턴 분석 (분봉 기반) ★★★
# ═══════════════════════════════════════════════════════
surge_pattern_history = []

def _load_surge_patterns():
    global surge_pattern_history
    try:
        import os
        fp = os.path.join(os.path.dirname(get_state_file()), 'surge_patterns.json')
        if os.path.exists(fp):
            with open(fp, 'r', encoding='utf-8') as f:
                surge_pattern_history = json.load(f)
            print(f"[PATTERN] 패턴 데이터 로드: {len(surge_pattern_history)}건")
    except Exception as e:
        print(f"[PATTERN] 로드 실패: {e}")

def _save_surge_patterns():
    try:
        import os
        fp = os.path.join(os.path.dirname(get_state_file()), 'surge_patterns.json')
        with open(fp, 'w', encoding='utf-8') as f:
            json.dump(surge_pattern_history[-500:], f, ensure_ascii=False)
    except Exception as e:
        print(f"[PATTERN] 저장 실패: {e}")

def analyze_surge_pattern(candles, prev_close=0):
    if not candles or len(candles) < 10:
        return None
    opens = [c['open'] for c in candles]
    highs = [c['high'] for c in candles]
    lows = [c['low'] for c in candles]
    base = prev_close if prev_close > 0 else opens[0]
    if base <= 0: return None
    open_chg = (opens[0] - base) / base * 100
    first30 = min(30, len(highs))
    peak_price = max(highs[:first30])
    peak_idx = highs[:first30].index(peak_price)
    peak_chg = (peak_price - base) / base * 100
    if peak_idx < len(lows) - 1:
        after_peak_lows = lows[peak_idx+1:min(peak_idx+30, len(lows))]
        if after_peak_lows:
            dip_price = min(after_peak_lows)
            dip_idx = peak_idx + 1 + after_peak_lows.index(dip_price)
            dip_chg = (dip_price - base) / base * 100
            dip_from_peak = (dip_price - peak_price) / peak_price * 100
        else:
            return None
    else:
        return None
    if dip_idx < len(highs) - 1:
        after_dip_highs = highs[dip_idx+1:min(dip_idx+20, len(highs))]
        if after_dip_highs:
            bounce_price = max(after_dip_highs)
            bounce_from_dip = (bounce_price - dip_price) / dip_price * 100
            bounce_chg = (bounce_price - base) / base * 100
        else:
            bounce_chg = dip_chg
            bounce_from_dip = 0
    else:
        bounce_chg = dip_chg
        bounce_from_dip = 0
    return {
        'open_chg': round(open_chg, 2), 'peak_chg': round(peak_chg, 2), 'peak_min': peak_idx,
        'dip_chg': round(dip_chg, 2), 'dip_from_peak': round(dip_from_peak, 2), 'dip_min': dip_idx,
        'bounce_chg': round(bounce_chg, 2), 'bounce_from_dip': round(bounce_from_dip, 2),
        'shake_duration': dip_idx - peak_idx,
    }

def collect_today_patterns(app_key, app_secret, mode, token):
    today = datetime.now().strftime('%Y-%m-%d')
    today_buys = set()
    for l in trade_log:
        if l.get('type') in ('AI_BUY','BUY') and l.get('success') and (l.get('date','') == today):
            today_buys.add((l.get('ticker',''), l.get('name',''), float(l.get('price', 0))))
    if not today_buys:
        print(f"[PATTERN] 오늘 매수 종목 없음")
        return
    collected = 0
    for ticker, name, buy_price in today_buys:
        if not ticker: continue
        if any(p.get('date') == today and p.get('ticker') == ticker for p in surge_pattern_history):
            continue
        try:
            time.sleep(0.5)
            candles = fetch_minute_candles(app_key, app_secret, mode, token, ticker)
            if not candles or len(candles) < 10: continue
            daily = fetch_daily_candles(app_key, app_secret, mode, token, ticker, days=5)
            prev_close = daily[-2]['close'] if daily and len(daily) >= 2 else 0
            pattern = analyze_surge_pattern(candles, prev_close)
            if pattern:
                # ★ v6.0: dip=0 = 불완전 데이터 (장외시간 등) → 건너뜀
                if pattern.get('dip_from_peak', 0) == 0:
                    print(f"[PATTERN] {name}({ticker}) 불완전 데이터 (dip=0) → skip")
                    continue
                pattern['date'] = today
                pattern['ticker'] = ticker
                pattern['name'] = name
                surge_pattern_history.append(pattern)
                collected += 1
                print(f"[PATTERN] {name}({ticker}) peak{pattern['peak_chg']:+.1f}% dip{pattern['dip_from_peak']:.1f}% bounce{pattern['bounce_from_dip']:.1f}%")
        except Exception as e:
            print(f"[PATTERN] {ticker} 실패: {e}")
    if collected > 0:
        _save_surge_patterns()
        print(f"[PATTERN] {collected}종목 수집 (전체 {len(surge_pattern_history)}건)")

_load_surge_patterns()

def calc_mtf_signals(app_key, app_secret, mode, token, ticker):
    """멀티타임프레임 신호 분석 (v4.0 강화)
    1분봉: 초단기 모멘텀 (급등/급락 감지)
    5분봉: 단기 추세 확인 (매수 타이밍 정밀화)
    15분봉: 중단기 추세 (추세 지속성 확인)
    일봉: 중기 추세 (방향성)
    
    Returns: {'m1_signal', 'm5_signal', 'm15_signal', 'daily_signal', 'combined', 'score', 'alignment'}
    """
    now = time.time()
    cached = _mtf_cache.get(ticker)
    if cached and (now - cached['ts']) < MTF_CACHE_TTL:
        return cached['data']
    
    result = {}
    
    def _analyze_candles(candles, label):
        """분봉 데이터에서 모멘텀+거래량 신호 추출"""
        if len(candles) < 10:
            return {}
        recent5 = candles[-5:]
        prev5 = candles[-10:-5]
        avg_recent = sum(c['close'] for c in recent5) / 5
        avg_prev = sum(c['close'] for c in prev5) / 5
        chg = (avg_recent - avg_prev) / avg_prev * 100 if avg_prev > 0 else 0
        vol_recent = sum(c['volume'] for c in recent5)
        vol_prev = sum(c['volume'] for c in prev5)
        vol_surge = vol_recent / max(vol_prev, 1)
        
        if chg > 0.5 and vol_surge > 2: sig = 'strong_up'
        elif chg > 0.2: sig = 'up'
        elif chg < -0.5: sig = 'strong_down'
        elif chg < -0.2: sig = 'down'
        else: sig = 'flat'
        
        # 5분/15분봉은 기울기도 확인 (지속적 상승/하락)
        if len(candles) >= 3:
            last3 = [c['close'] for c in candles[-3:]]
            if last3[0] < last3[1] < last3[2]: slope = 'rising'
            elif last3[0] > last3[1] > last3[2]: slope = 'falling'
            else: slope = 'mixed'
        else:
            slope = 'unknown'
        
        return {f'{label}_signal': sig, f'{label}_change': round(chg, 2), 
                f'{label}_vol_surge': round(vol_surge, 1), f'{label}_slope': slope}
    
    try:
        # 1분봉
        m1 = fetch_minute_candles(app_key, app_secret, mode, token, ticker, '1')
        result.update(_analyze_candles(m1, 'm1'))
        
        # ★ 5분봉 (KIS는 1분봉 데이터를 5분 단위로 그룹핑)
        if len(m1) >= 25:
            m5 = []
            for i in range(0, len(m1) - 4, 5):
                chunk = m1[i:i+5]
                if len(chunk) == 5:
                    m5.append({
                        'time': chunk[0]['time'],
                        'open': chunk[0]['open'],
                        'high': max(c['high'] for c in chunk),
                        'low': min(c['low'] for c in chunk),
                        'close': chunk[-1]['close'],
                        'volume': sum(c['volume'] for c in chunk),
                    })
            result.update(_analyze_candles(m5, 'm5'))
        
        # ★ 15분봉 (1분봉 15개씩 그룹핑)
        if len(m1) >= 45:
            m15 = []
            for i in range(0, len(m1) - 14, 15):
                chunk = m1[i:i+15]
                if len(chunk) == 15:
                    m15.append({
                        'time': chunk[0]['time'],
                        'open': chunk[0]['open'],
                        'high': max(c['high'] for c in chunk),
                        'low': min(c['low'] for c in chunk),
                        'close': chunk[-1]['close'],
                        'volume': sum(c['volume'] for c in chunk),
                    })
            result.update(_analyze_candles(m15, 'm15'))
        
        # 일봉 기술적 지표 (기존 캐시)
        daily_ta = _ta_cache.get(ticker, {}).get('data', {})
        if daily_ta:
            result['daily_signal'] = daily_ta.get('trend', 'unknown')
            result['daily_rsi'] = daily_ta.get('rsi', 50)
            result['daily_macd_cross'] = daily_ta.get('macd_cross', 'none')
        
        # ★ 타임프레임 정렬 체크 (모든 TF가 같은 방향 = 강한 신호)
        signals = []
        for tf in ['m1', 'm5', 'm15']:
            s = result.get(f'{tf}_signal', 'flat')
            if 'up' in s: signals.append(1)
            elif 'down' in s: signals.append(-1)
            else: signals.append(0)
        ds = result.get('daily_signal', '')
        if '상승' in ds: signals.append(1)
        elif '하락' in ds: signals.append(-1)
        else: signals.append(0)
        
        all_up = all(s >= 0 for s in signals) and sum(s > 0 for s in signals) >= 3
        all_down = all(s <= 0 for s in signals) and sum(s < 0 for s in signals) >= 3
        if all_up: result['alignment'] = 'bullish_aligned'
        elif all_down: result['alignment'] = 'bearish_aligned'
        else: result['alignment'] = 'mixed'
        
        # 종합 점수 (-8 ~ +8)
        score = 0
        for tf in ['m1', 'm5', 'm15']:
            s = result.get(f'{tf}_signal', 'flat')
            if s == 'strong_up': score += 2
            elif s == 'up': score += 1
            elif s == 'strong_down': score -= 2
            elif s == 'down': score -= 1
        
        if '강한상승' in ds: score += 2
        elif '상승' in ds: score += 1
        elif '강한하락' in ds: score -= 2
        elif '하락' in ds: score -= 1
        
        if result.get('daily_macd_cross') == 'golden': score += 1
        if result.get('daily_macd_cross') == 'dead': score -= 1
        if result['alignment'] == 'bullish_aligned': score += 1
        if result['alignment'] == 'bearish_aligned': score -= 1
        
        result['score'] = score
        if score >= 4: result['combined'] = 'strong_buy'
        elif score >= 2: result['combined'] = 'buy'
        elif score <= -4: result['combined'] = 'strong_sell'
        elif score <= -2: result['combined'] = 'sell'
        else: result['combined'] = 'neutral'
        
        _mtf_cache[ticker] = {'data': result, 'ts': now}
        align_kr = {'bullish_aligned':'🟢전체정렬','bearish_aligned':'🔴전체정렬','mixed':'혼조'}.get(result['alignment'],'')
        print(f"[MTF] {ticker}: 1분={result.get('m1_signal','')} 5분={result.get('m5_signal','')} "
              f"15분={result.get('m15_signal','')} 일봉={result.get('daily_signal','')} "
              f"{align_kr} 종합={result.get('combined','')}({score}점)")
    except Exception as e:
        print(f"[MTF] {ticker} 분석 실패: {e}")
    
    return result

def build_mtf_context(mtf, ticker=''):
    """멀티타임프레임 분석을 AI 프롬프트 텍스트로 변환"""
    if not mtf:
        return ""
    sig_kr = {'strong_up':'급등','up':'상승','flat':'횡보','down':'하락','strong_down':'급락'}
    ctx = f"\n[멀티타임프레임 {ticker}]"
    for tf, label in [('m1','1분봉'), ('m5','5분봉'), ('m15','15분봉')]:
        s = mtf.get(f'{tf}_signal')
        if s:
            ctx += f"\n{label}: {sig_kr.get(s,'?')} ({mtf.get(f'{tf}_change',0):+.2f}%)"
            if mtf.get(f'{tf}_vol_surge', 1) > 2:
                ctx += f" 거래량{mtf[f'{tf}_vol_surge']}배!"
            slope = mtf.get(f'{tf}_slope', '')
            if slope == 'rising': ctx += " ↗연속상승"
            elif slope == 'falling': ctx += " ↘연속하락"
    if mtf.get('daily_signal'):
        ctx += f"\n일봉추세: {mtf['daily_signal']}"
    align_kr = {'bullish_aligned':'🟢전TF 상승정렬(강한매수)','bearish_aligned':'🔴전TF 하락정렬(매수금지)','mixed':'혼조'}.get(mtf.get('alignment',''),'')
    if align_kr: ctx += f"\n정렬: {align_kr}"
    combined_kr = {'strong_buy':'강한매수','buy':'매수','neutral':'중립','sell':'매도','strong_sell':'강한매도'}.get(mtf.get('combined',''),'?')
    ctx += f"\n★ 멀티TF 종합: {combined_kr} ({mtf.get('score',0):+d}점)"
    return ctx

# ============= v4.0 PHASE 3: AI 백테스트 시뮬레이터 =============

def run_backtest(days=30):
    """과거 매매 데이터로 v4.0 전략 백테스트
    
    기존 trade_log에서 매수/매도 기록을 추출하여:
    1. v3.0 고정 tp/sl로 매도했을 때 결과
    2. v4.0 ATR 동적 tp/sl로 매도했을 때 가상 결과
    를 비교하여 전략 개선 효과를 측정
    
    Returns: {v3_result, v4_result, improvement, details}
    """
    from collections import defaultdict
    today = datetime.now()
    cutoff = (today - timedelta(days=days)).strftime('%Y-%m-%d')
    
    buys = [t for t in trade_log 
            if t.get('type') in ('AI_BUY','CHAT_BUY') and t.get('success')
            and (t.get('date','') or t.get('time','')[:10]) >= cutoff
            and t.get('price') and float(t.get('price',0)) > 0]
    
    sells = [t for t in trade_log
            if t.get('type') in ('SELL','FORCE_CLOSE','AI_SELL') and t.get('success')
            and (t.get('date','') or t.get('time','')[:10]) >= cutoff]
    
    if len(sells) < 5:
        return {'error': f'매매 데이터 부족 (매도 {len(sells)}건, 최소 5건 필요)'}
    
    # v3.0 실제 결과
    v3_wins = sum(1 for t in sells if float(t.get('pnl',0) or 0) > 0)
    v3_losses = sum(1 for t in sells if float(t.get('pnl',0) or 0) < 0)
    v3_total_pnl = sum(float(t.get('pnl',0) or 0) for t in sells)
    v3_win_rate = round(v3_wins / max(v3_wins + v3_losses, 1) * 100, 1)
    v3_avg_win = round(sum(float(t.get('pnl',0) or 0) for t in sells if float(t.get('pnl',0) or 0) > 0) / max(v3_wins,1))
    v3_avg_loss = round(abs(sum(float(t.get('pnl',0) or 0) for t in sells if float(t.get('pnl',0) or 0) < 0)) / max(v3_losses,1))
    
    # v4.0 시뮬레이션: ATR 기반 tp/sl 적용 시 가상 결과
    # 각 매도의 pnl_pct를 분석하여 ATR tp/sl이었다면 어떤 결과였을지 추정
    v4_improved = 0  # ATR로 개선된 매도 수
    v4_details = []
    
    for sell in sells:
        ticker = sell.get('ticker', '')
        pnl_pct = float(sell.get('pnl_pct', 0) or 0)
        pnl_amt = float(sell.get('pnl', 0) or 0)
        reason = sell.get('reason', '')
        
        # ATR 데이터 확인
        ta = _ta_cache.get(ticker, {}).get('data', {})
        atr_pct = ta.get('atr_pct', 0)
        
        improvement = ''
        if atr_pct > 0:
            v4_tp1 = round(atr_pct * 1.5, 1)
            v4_sl = round(-atr_pct * 1.0, 1)
            
            # 손절인데 ATR sl이 더 타이트했으면 → 손실 줄었을 것
            if pnl_pct < 0 and v4_sl > pnl_pct:
                saved = round(abs(pnl_pct - v4_sl) * abs(pnl_amt / max(abs(pnl_pct), 0.01)))
                improvement = f'ATR손절({v4_sl}%)이면 ₩{int(saved):,} 절약'
                v4_improved += 1
            # 익절인데 ATR tp1이 더 높았으면 → 수익 더 클 수 있었음
            elif pnl_pct > 0 and '1차' in reason and v4_tp1 > pnl_pct:
                extra = round((v4_tp1 - pnl_pct) / max(pnl_pct, 0.01) * pnl_amt)
                improvement = f'ATR tp1({v4_tp1}%)이면 +₩{int(abs(extra)):,} 추가 가능'
                v4_improved += 1
        
        v4_details.append({
            'ticker': ticker, 'name': sell.get('name',''),
            'pnl_pct': pnl_pct, 'pnl': pnl_amt,
            'reason': reason[:30], 'improvement': improvement
        })
    
    return {
        'period': f'{days}일',
        'total_trades': len(sells),
        'v3_result': {
            'win_rate': v3_win_rate,
            'wins': v3_wins, 'losses': v3_losses,
            'total_pnl': round(v3_total_pnl),
            'avg_win': v3_avg_win, 'avg_loss': v3_avg_loss,
        },
        'v4_improvement': {
            'improved_trades': v4_improved,
            'improvement_rate': round(v4_improved / max(len(sells), 1) * 100, 1),
        },
        'details': v4_details[-20:],  # 최근 20건
        'recommendation': (
            'ATR 동적 tp/sl이 효과적 (개선률 높음)' if v4_improved > len(sells) * 0.3
            else 'ATR 동적 tp/sl 적용 시 소폭 개선 기대'
        )
    }

# ============= v4.0 PHASE 3: 섹터 로테이션 (자금흐름 추적) =============

_sector_flow_cache = {'data': {}, 'ts': 0}
SECTOR_FLOW_TTL = 300  # 5분

def fetch_sector_flows():
    """업종별 등락률 + 거래대금 → 자금 흐름 분석
    Returns: {sectors: [{name, change, volume_ratio, flow_signal}], hot_sectors, cold_sectors}
    """
    now = time.time()
    if _sector_flow_cache['data'] and (now - _sector_flow_cache['ts']) < SECTOR_FLOW_TTL:
        return _sector_flow_cache['data']
    
    import re as _re
    ua = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    result = {'sectors': [], 'hot_sectors': [], 'cold_sectors': []}
    
    try:
        # 네이버 업종별 시세 (HTML 스크래핑)
        req = urllib.request.Request("https://finance.naver.com/sise/sise_group.naver?type=upjong", headers=ua)
        with urllib.request.urlopen(req, timeout=10) as resp:
            html = resp.read().decode('euc-kr', errors='replace')
        
        # 업종명 + 등락률 파싱
        rows = _re.findall(r'type=upjong&no=\d+[^>]*>([^<]+)</a>.*?class="number"[^>]*>\s*([0-9,.%-]+)', html[:50000], _re.DOTALL)
        
        sectors = []
        for name, change_str in rows[:25]:
            name = name.strip()
            try:
                change = float(change_str.replace('%','').replace(',','').replace('+',''))
            except:
                change = 0
            
            flow = 'hot' if change > 1.5 else ('cold' if change < -1.5 else 'neutral')
            sectors.append({
                'name': name,
                'change': change,
                'flow_signal': flow,
            })
        
        # 정렬
        sectors.sort(key=lambda x: x['change'], reverse=True)
        result['sectors'] = sectors
        result['hot_sectors'] = [s['name'] for s in sectors if s['change'] > 1.0][:5]
        result['cold_sectors'] = [s['name'] for s in sectors if s['change'] < -1.0][:5]
        
        _sector_flow_cache['data'] = result
        _sector_flow_cache['ts'] = now
        
        print(f"[SECTOR] 업종 {len(sectors)}개 분석 완료 | HOT: {result['hot_sectors'][:3]} | COLD: {result['cold_sectors'][:3]}")
    except Exception as e:
        print(f"[SECTOR] 업종 분석 실패: {e}")
    
    return result

def build_sector_flow_context():
    """섹터 로테이션 데이터를 AI 프롬프트 텍스트로 변환"""
    sf = fetch_sector_flows()
    if not sf or not sf.get('sectors'):
        return ""
    ctx = "\n[★ v4.0 섹터 로테이션 — 자금 흐름]"
    if sf.get('hot_sectors'):
        ctx += f"\n자금 유입(HOT): {', '.join(sf['hot_sectors'][:5])}"
    if sf.get('cold_sectors'):
        ctx += f"\n자금 유출(COLD): {', '.join(sf['cold_sectors'][:5])}"
    # TOP 5 / BOTTOM 5
    top5 = sf['sectors'][:5]
    bot5 = sf['sectors'][-5:] if len(sf['sectors']) > 5 else []
    ctx += "\n상승 TOP: " + ', '.join(f"{s['name']}({s['change']:+.1f}%)" for s in top5)
    if bot5:
        ctx += "\n하락 TOP: " + ', '.join(f"{s['name']}({s['change']:+.1f}%)" for s in bot5)
    ctx += "\n★ HOT 섹터 종목 우선! COLD 섹터 매수 금지!"
    return ctx

# ============= v4.0 PHASE 4: 뉴스 센티멘트 AI 점수화 =============

_sentiment_cache = {}  # {ticker: {'score': int, 'label': str, 'reasons': [], 'ts': float}}
SENTIMENT_CACHE_TTL = 600  # 10분

# 호재/악재 키워드 사전 (AI 호출 없이 빠른 1차 판별)
_POSITIVE_KEYWORDS = [
    '신고가','역대최대','실적호전','상향','수주','흑자전환','특허','FDA승인','임상성공',
    '대규모투자','자사주매입','배당확대','목표가상향','매수추천','강력매수',
    '수출최대','영업이익증가','순이익증가','매출증가','점유율확대',
    '전략적제휴','대형계약','정부지원','국책사업','신사업진출',
    '외국인매수','기관매수','순매수','바이든','트럼프','규제완화',
    'AI','반도체','2차전지','로봇','자율주행','양자컴퓨터',
]
_NEGATIVE_KEYWORDS = [
    '하한가','신저가','적자전환','적자확대','실적악화','하향','감자','상장폐지',
    '횡령','배임','소송','분식회계','공매도','리콜','영업정지',
    '목표가하향','매도추천','투자경고','관리종목','불성실공시',
    '감사의견거절','부도','워크아웃','법정관리','파산',
    '수출감소','영업이익감소','매출감소','적자지속',
    '대주주매도','블록딜','유상증자','전환사채','CB발행',
    '금리인상','경기침체','전쟁','제재','규제강화',
]

def calc_news_sentiment(news_titles, ticker=''):
    """뉴스 제목 리스트 → 센티멘트 점수 (-100 ~ +100)
    
    1단계: 키워드 매칭 (빠름, AI 호출 없음)
    2단계: 종합 점수 산출
    
    Returns: {'score': -100~+100, 'label': str, 'positive': [], 'negative': [], 'count': int}
    """
    if not news_titles:
        return {'score': 0, 'label': '뉴스없음', 'positive': [], 'negative': [], 'count': 0}
    
    # 캐시 체크
    now = time.time()
    cached = _sentiment_cache.get(ticker)
    if cached and (now - cached['ts']) < SENTIMENT_CACHE_TTL:
        return cached
    
    positive_hits = []
    negative_hits = []
    
    for title in news_titles:
        title_clean = title.replace('[', '').replace(']', '').replace('오늘', '').strip()
        
        for kw in _POSITIVE_KEYWORDS:
            if kw in title_clean:
                positive_hits.append(f"{kw}: {title_clean[:40]}")
                break  # 제목당 1개만
        
        for kw in _NEGATIVE_KEYWORDS:
            if kw in title_clean:
                negative_hits.append(f"{kw}: {title_clean[:40]}")
                break
    
    # 점수 계산: 호재 +15점, 악재 -20점 (악재가 더 임팩트 큼)
    raw_score = len(positive_hits) * 15 - len(negative_hits) * 20
    
    # -100 ~ +100 클램프
    score = max(-100, min(100, raw_score))
    
    # 레이블
    if score >= 50: label = '강한호재'
    elif score >= 20: label = '호재'
    elif score >= 5: label = '약한호재'
    elif score <= -50: label = '강한악재'
    elif score <= -20: label = '악재'
    elif score <= -5: label = '약한악재'
    else: label = '중립'
    
    result = {
        'score': score,
        'label': label,
        'positive': positive_hits[:5],
        'negative': negative_hits[:5],
        'count': len(news_titles),
        'ts': now,
    }
    
    if ticker:
        _sentiment_cache[ticker] = result
    
    print(f"[SENTIMENT] {ticker}: {score}점({label}) 호재{len(positive_hits)} 악재{len(negative_hits)} (뉴스{len(news_titles)}건)")
    return result

def get_stock_sentiment(ticker, app_key='', app_secret='', mode='live', token=''):
    """종목 뉴스 조회 → 센티멘트 점수 (통합 함수)"""
    # 캐시 먼저 체크
    now = time.time()
    cached = _sentiment_cache.get(ticker)
    if cached and (now - cached.get('ts', 0)) < SENTIMENT_CACHE_TTL:
        return cached
    
    # 뉴스 수집
    news = fetch_naver_news(ticker=ticker, max_days=2)
    if not news:
        return {'score': 0, 'label': '뉴스없음', 'positive': [], 'negative': [], 'count': 0}
    
    return calc_news_sentiment(news, ticker)

def build_sentiment_context(sentiment, ticker='', name=''):
    """센티멘트 점수를 AI 프롬프트 텍스트로 변환"""
    if not sentiment or sentiment.get('count', 0) == 0:
        return ""
    
    s = sentiment
    score = s.get('score', 0)
    label = s.get('label', '중립')
    
    emoji = '🟢' if score >= 20 else ('🔴' if score <= -20 else '⚪')
    ctx = f"\n뉴스센티멘트: {emoji}{label}({score:+d}점, {s['count']}건)"
    
    if s.get('positive'):
        ctx += f"\n  호재: {' / '.join(s['positive'][:3])}"
    if s.get('negative'):
        ctx += f"\n  악재: {' / '.join(s['negative'][:3])}"
    
    # AI 매수 가이드
    if score <= -30:
        ctx += f"\n  ⚠️ 악재 뉴스 다수 → 매수 금지!"
    elif score >= 30:
        ctx += f"\n  ★ 호재 뉴스 다수 → 매수 가산점!"
    
    return ctx

# ============= v4.0 PHASE 4: 자동 파라미터 최적화 =============

_param_optimize_cache = {'data': None, 'ts': 0}
PARAM_OPTIMIZE_FILE = 'param_optimize_history.json'

def analyze_optimal_params(days=14):
    """최근 N일 매매 데이터 분석 → 최적 tp/sl/ATR 계수 계산
    
    매일 마감 브리핑 때 실행하여 다음 날 파라미터에 반영.
    
    분석 방법:
    1. 익절 매도 중 가장 수익이 좋았던 tp% 구간 찾기
    2. 손절 매도 중 더 빨리 잘랐으면 좋았을 케이스 분석
    3. 트레일링으로 빠진 것 중 더 오른 케이스 분석
    
    Returns: {optimal_tp1, optimal_sl, optimal_trail, atr_multiplier, analysis}
    """
    from collections import defaultdict
    today = datetime.now().strftime('%Y-%m-%d')
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    sells = [t for t in trade_log
             if t.get('type') in ('SELL', 'FORCE_CLOSE', 'AI_SELL') and t.get('success')
             and (t.get('date', '') or t.get('time', '')[:10]) >= cutoff
             and t.get('pnl_pct') is not None]
    
    if len(sells) < 10:
        return {'error': f'데이터 부족 (매도 {len(sells)}건, 최소 10건 필요)'}
    
    # 1. 익절 분석: 실제 익절% 분포
    tp_sells = [t for t in sells if float(t.get('pnl_pct', 0) or 0) > 0]
    sl_sells = [t for t in sells if float(t.get('pnl_pct', 0) or 0) < 0]
    
    tp_pcts = [float(t.get('pnl_pct', 0) or 0) for t in tp_sells]
    sl_pcts = [abs(float(t.get('pnl_pct', 0) or 0)) for t in sl_sells]
    
    # 2. 최적 tp1 = 실제 익절 중간값 × 0.8 (조금 일찍 잡기)
    if tp_pcts:
        tp_pcts_sorted = sorted(tp_pcts)
        median_tp = tp_pcts_sorted[len(tp_pcts_sorted) // 2]
        optimal_tp1 = round(max(2.0, median_tp * 0.8), 1)
    else:
        optimal_tp1 = 5.0
    
    # 3. 최적 sl = 실제 손절 중간값 × 0.8 (조금 일찍 잡기)  
    if sl_pcts:
        sl_pcts_sorted = sorted(sl_pcts)
        median_sl = sl_pcts_sorted[len(sl_pcts_sorted) // 2]
        optimal_sl = round(min(-1.5, -(median_sl * 0.8)), 1)
    else:
        optimal_sl = -5.0
    
    # 4. 평균 보유시간 분석 (빨리 판 게 좋았는지, 오래 들고 있는 게 좋았는지)
    hold_times = []
    for sell in sells:
        ticker = sell.get('ticker', '')
        sell_time = sell.get('time', '')
        # 매수 시간 찾기
        for buy in reversed(trade_log):
            if (buy.get('ticker') == ticker and buy.get('type') in ('AI_BUY', 'CHAT_BUY') 
                and buy.get('success') and buy.get('time', '') < sell_time):
                try:
                    buy_dt = datetime.fromisoformat(buy['time'])
                    sell_dt = datetime.fromisoformat(sell_time)
                    hold_min = (sell_dt - buy_dt).total_seconds() / 60
                    pnl = float(sell.get('pnl', 0) or 0)
                    hold_times.append({'minutes': hold_min, 'pnl': pnl, 'pnl_pct': float(sell.get('pnl_pct', 0) or 0)})
                except:
                    pass
                break
    
    # 5. 승률 vs 보유시간 분석
    short_holds = [h for h in hold_times if h['minutes'] < 30]  # 30분 미만
    long_holds = [h for h in hold_times if h['minutes'] >= 30]  # 30분 이상
    
    short_win_rate = round(sum(1 for h in short_holds if h['pnl'] > 0) / max(len(short_holds), 1) * 100, 1)
    long_win_rate = round(sum(1 for h in long_holds if h['pnl'] > 0) / max(len(long_holds), 1) * 100, 1)
    
    short_avg_pnl = round(sum(h['pnl'] for h in short_holds) / max(len(short_holds), 1))
    long_avg_pnl = round(sum(h['pnl'] for h in long_holds) / max(len(long_holds), 1))
    
    # 6. ATR 계수 추천
    # 현재 ATR×1.5=tp1인데, 실제 익절 데이터에서 최적 계수 역산
    atr_multiplier = 1.5  # 기본값
    if tp_pcts and _ta_cache:
        # 캐시된 종목들의 평균 ATR
        avg_atr = sum(v.get('data', {}).get('atr_pct', 3) for v in _ta_cache.values()) / max(len(_ta_cache), 1)
        if avg_atr > 0:
            atr_multiplier = round(optimal_tp1 / avg_atr, 2)
            atr_multiplier = max(1.0, min(3.0, atr_multiplier))  # 1.0~3.0 범위
    
    # 7. 전략 추천
    analysis = []
    total_pnl = sum(float(t.get('pnl', 0) or 0) for t in sells)
    win_rate = round(len(tp_sells) / max(len(tp_sells) + len(sl_sells), 1) * 100, 1)
    
    analysis.append(f"기간: {days}일 | 매도 {len(sells)}건 | 승률 {win_rate}%")
    analysis.append(f"총 실현손익: {'+'if total_pnl>=0 else ''}₩{int(total_pnl):,}")
    
    if win_rate < 45:
        analysis.append("★ 승률 낮음 → tp1 낮추고(빠른 익절), AI 매수 확신도 85%↑로 상향")
    elif win_rate >= 60:
        analysis.append("★ 승률 양호 → 현재 전략 유지, tp1 약간 올려 수익 극대화")
    
    if sl_pcts and sum(sl_pcts) / len(sl_pcts) > 5:
        analysis.append("★ 평균 손절폭 큼 → sl 더 타이트하게 (현재 평균 -{:.1f}%)".format(sum(sl_pcts)/len(sl_pcts)))
    
    if short_win_rate > long_win_rate + 10:
        analysis.append(f"★ 단타({short_win_rate}%) > 보유({long_win_rate}%) → 빠른 회전 전략 유지")
    elif long_win_rate > short_win_rate + 10:
        analysis.append(f"★ 보유({long_win_rate}%) > 단타({short_win_rate}%) → 보유시간 늘리기 검토")
    
    result = {
        'period': f'{days}일',
        'total_trades': len(sells),
        'win_rate': win_rate,
        'total_pnl': round(total_pnl),
        'optimal_tp1': optimal_tp1,
        'optimal_tp2': round(optimal_tp1 * 2.5, 1),
        'optimal_sl': optimal_sl,
        'optimal_trail': round(optimal_tp1 * 0.6, 1),
        'atr_multiplier': atr_multiplier,
        'hold_analysis': {
            'short_win_rate': short_win_rate,
            'long_win_rate': long_win_rate,
            'short_avg_pnl': short_avg_pnl,
            'long_avg_pnl': long_avg_pnl,
            'total_analyzed': len(hold_times),
        },
        'analysis': analysis,
        'generated_at': datetime.now().isoformat(),
    }
    
    # 히스토리 저장
    try:
        history = {}
        if os.path.exists(PARAM_OPTIMIZE_FILE):
            with open(PARAM_OPTIMIZE_FILE, 'r', encoding='utf-8') as f:
                history = json.load(f)
        history[today] = result
        # 최근 90일만 보관
        cutoff_keys = sorted(history.keys())
        if len(cutoff_keys) > 90:
            for k in cutoff_keys[:-90]:
                del history[k]
        with open(PARAM_OPTIMIZE_FILE, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False)
    except Exception as e:
        print(f"[PARAM_OPT] 히스토리 저장 실패: {e}")
    
    print(f"[PARAM_OPT] 최적화 완료: tp1={optimal_tp1}% sl={optimal_sl}% trail={result['optimal_trail']}% "
          f"ATR×{atr_multiplier} | 승률={win_rate}% ({len(sells)}건)")
    
    return result

def auto_apply_optimal_params(config):
    """최적 파라미터를 자동 적용 (마감 브리핑 후 실행)
    
    안전장치: 급격한 변경 방지
    - 기존값 대비 ±30% 이내만 변경
    - 최소/최대 범위 제한
    """
    opt = analyze_optimal_params(14)
    if opt.get('error'):
        print(f"[PARAM_OPT] 최적화 스킵: {opt['error']}")
        return None
    
    changes = []
    
    # tp1 조정 (기존 대비 ±30% 이내)
    cur_tp1 = float(config.get('tp1', 5))
    new_tp1 = opt['optimal_tp1']
    if abs(new_tp1 - cur_tp1) / max(cur_tp1, 0.1) <= 0.3:
        new_tp1 = round(max(2.0, min(15.0, new_tp1)), 1)
        if new_tp1 != cur_tp1:
            changes.append(f"tp1: {cur_tp1}% → {new_tp1}%")
            config['tp1'] = str(new_tp1)
    
    # sl 조정
    cur_sl = float(config.get('sl', -5))
    new_sl = opt['optimal_sl']
    if abs(new_sl - cur_sl) / max(abs(cur_sl), 0.1) <= 0.3:
        new_sl = round(max(-15.0, min(-1.5, new_sl)), 1)
        if new_sl != cur_sl:
            changes.append(f"sl: {cur_sl}% → {new_sl}%")
            config['sl'] = str(new_sl)
    
    # trailing 조정
    cur_trail = float(config.get('trailing_pct', 3))
    new_trail = opt['optimal_trail']
    if abs(new_trail - cur_trail) / max(cur_trail, 0.1) <= 0.3:
        new_trail = round(max(1.0, min(8.0, new_trail)), 1)
        if new_trail != cur_trail:
            changes.append(f"trail: {cur_trail}% → {new_trail}%")
            config['trailing_pct'] = str(new_trail)
    
    if changes:
        msg = f"🔧 파라미터 자동최적화 ({opt['period']} 데이터)\n" + '\n'.join(changes)
        msg += f"\n승률 {opt['win_rate']}% | ATR계수 ×{opt['atr_multiplier']}"
        print(f"[PARAM_OPT] {msg}")
        trade_log.append({
            "time": datetime.now().isoformat(),
            "date": datetime.now().strftime('%Y-%m-%d'),
            "type": "PARAM_OPTIMIZE",
            "message": msg
        })
        save_state()
        tg_send(f"🔧 <b>파라미터 자동최적화</b>\n{msg}\n⏰ {datetime.now().strftime('%H:%M')}")
        return changes
    else:
        print(f"[PARAM_OPT] 변경 없음 (현재 파라미터 적정)")
        return []

def build_param_optimize_context():
    """최적화 결과를 AI 프롬프트에 주입용 텍스트로 변환"""
    opt = _param_optimize_cache.get('data')
    if not opt or not opt.get('analysis'):
        return ""
    ctx = "\n[★ 파라미터 최적화 피드백]"
    for line in opt['analysis']:
        ctx += f"\n{line}"
    ha = opt.get('hold_analysis', {})
    if ha.get('total_analyzed', 0) > 5:
        ctx += f"\n보유시간: 30분미만 승률{ha['short_win_rate']}% vs 30분이상 {ha['long_win_rate']}%"
    return ctx

# ============= v4.0 PHASE 5: 백테스트 가상매매 시뮬레이터 =============

def run_virtual_backtest(app_key, app_secret, mode, token, ticker, days=20, 
                         tp1=5, sl=-5, trail=3, initial_amount=500000):
    """종목별 가상매매 시뮬레이션 (과거 일봉 데이터로)
    
    과거 N일 데이터에서 tp/sl 설정으로 매매했을 때 결과를 시뮬레이션.
    실제 주문 없이 전략 검증.
    
    Returns: {trades, total_pnl, win_rate, max_drawdown, details}
    """
    candles = fetch_daily_candles(app_key, app_secret, mode, token, ticker, days + 5)
    if len(candles) < 10:
        return {'error': f'데이터 부족 ({len(candles)}일)'}
    
    trades = []
    position = None  # {'entry_price', 'qty', 'entry_day', 'peak'}
    cash = initial_amount
    total_pnl = 0
    peak_equity = initial_amount
    max_dd = 0
    
    for i, candle in enumerate(candles):
        cur = candle['close']
        high = candle['high']
        low = candle['low']
        
        if position:
            entry = position['entry_price']
            pnl_pct_high = (high - entry) / entry * 100
            pnl_pct_low = (low - entry) / entry * 100
            pnl_pct_close = (cur - entry) / entry * 100
            
            if position['peak'] < high:
                position['peak'] = high
            
            sold = False
            sell_price = 0
            sell_reason = ''
            
            # 손절 체크 (장중 저가 기준)
            if pnl_pct_low <= sl:
                sell_price = entry * (1 + sl / 100)
                sell_reason = f'손절({sl}%)'
                sold = True
            # 익절 체크 (장중 고가 기준)
            elif pnl_pct_high >= tp1:
                sell_price = entry * (1 + tp1 / 100)
                sell_reason = f'익절({tp1}%)'
                sold = True
            # 트레일링 (고점 대비)
            elif position['peak'] > entry * (1 + tp1 / 100 * 0.5):
                drop = (position['peak'] - cur) / position['peak'] * 100
                if drop >= trail:
                    sell_price = cur
                    sell_reason = f'트레일({drop:.1f}%하락)'
                    sold = True
            # 5일 타임아웃
            elif i - position['entry_day'] >= 5:
                sell_price = cur
                sell_reason = '5일타임아웃'
                sold = True
            
            if sold:
                pnl = (sell_price - entry) * position['qty']
                pnl_pct = (sell_price - entry) / entry * 100
                cash += sell_price * position['qty']
                total_pnl += pnl
                trades.append({
                    'day': candle['date'], 'type': 'SELL', 'price': round(sell_price),
                    'pnl': round(pnl), 'pnl_pct': round(pnl_pct, 2), 'reason': sell_reason
                })
                position = None
                
                # MDD
                equity = cash
                if equity > peak_equity: peak_equity = equity
                dd = (peak_equity - equity) / peak_equity * 100
                if dd > max_dd: max_dd = dd
        
        elif i < len(candles) - 1:  # 마지막 날은 매수 안함
            # 매수 조건: 전일 대비 양봉 + 거래량 증가
            if i > 0:
                prev = candles[i-1]
                if cur > prev['close'] and candle['volume'] > prev['volume'] * 1.2:
                    qty = max(1, int(min(cash * 0.3, initial_amount * 0.5) / cur))
                    if qty > 0 and cash >= cur * qty:
                        cash -= cur * qty
                        position = {'entry_price': cur, 'qty': qty, 'entry_day': i, 'peak': cur}
                        trades.append({
                            'day': candle['date'], 'type': 'BUY', 'price': round(cur), 'qty': qty
                        })
    
    wins = [t for t in trades if t.get('type') == 'SELL' and t.get('pnl', 0) > 0]
    losses = [t for t in trades if t.get('type') == 'SELL' and t.get('pnl', 0) < 0]
    
    return {
        'ticker': ticker, 'days': days,
        'settings': {'tp1': tp1, 'sl': sl, 'trail': trail},
        'total_trades': len([t for t in trades if t['type'] == 'SELL']),
        'wins': len(wins), 'losses': len(losses),
        'win_rate': round(len(wins) / max(len(wins) + len(losses), 1) * 100, 1),
        'total_pnl': round(total_pnl),
        'max_drawdown': round(max_dd, 1),
        'final_equity': round(cash + (position['entry_price'] * position['qty'] if position else 0)),
        'trades': trades[-20:],
    }

# ============= v4.0 PHASE 5: 켈리 기준 자금관리 =============

def calc_kelly_fraction(days=30):
    """켈리 기준(Kelly Criterion)으로 최적 투자비율 계산
    
    f* = (bp - q) / b
    b = 평균수익/평균손실 비율 (odds)
    p = 승률
    q = 패률 (1-p)
    
    풀켈리는 너무 공격적 → Half-Kelly 사용 (절반)
    
    Returns: {'full_kelly', 'half_kelly', 'recommended_pct', 'max_buy_ratio', 'analysis'}
    """
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    sells = [t for t in trade_log
             if t.get('type') in ('SELL', 'FORCE_CLOSE') and t.get('success')
             and (t.get('date', '') or t.get('time', '')[:10]) >= cutoff
             and t.get('pnl') is not None]
    
    if len(sells) < 10:
        return {'error': f'데이터 부족 ({len(sells)}건, 최소 10건)', 
                'recommended_pct': 10, 'max_buy_ratio': 0.5}
    
    wins = [t for t in sells if float(t.get('pnl', 0) or 0) > 0]
    losses = [t for t in sells if float(t.get('pnl', 0) or 0) < 0]
    
    p = len(wins) / max(len(wins) + len(losses), 1)  # 승률
    q = 1 - p
    
    avg_win = sum(abs(float(t.get('pnl_pct', 0) or 0)) for t in wins) / max(len(wins), 1)
    avg_loss = sum(abs(float(t.get('pnl_pct', 0) or 0)) for t in losses) / max(len(losses), 1)
    
    b = avg_win / max(avg_loss, 0.1)  # 손익비
    
    # 켈리 공식
    full_kelly = (b * p - q) / max(b, 0.01)
    half_kelly = full_kelly / 2
    
    # 안전 범위 (5% ~ 30%)
    recommended = max(5, min(30, round(half_kelly * 100, 1)))
    
    # max_buy_amount 비율 변환
    max_buy_ratio = max(0.3, min(1.0, recommended / 20))
    
    analysis = []
    analysis.append(f"승률: {p*100:.1f}% ({len(wins)}승/{len(losses)}패)")
    analysis.append(f"손익비: {b:.2f} (평균익절 {avg_win:.1f}% / 평균손절 {avg_loss:.1f}%)")
    analysis.append(f"풀켈리: {full_kelly*100:.1f}% → 하프켈리: {half_kelly*100:.1f}%")
    
    if full_kelly <= 0:
        analysis.append("⚠️ 켈리 음수 → 현재 전략 수익 기대값 마이너스! 매매 축소 권장")
        recommended = 5
        max_buy_ratio = 0.3
    elif recommended > 20:
        analysis.append("★ 켈리 높음 → 전략 우수, 적극 매매 가능")
    elif recommended < 10:
        analysis.append("★ 켈리 낮음 → 소극적 매매, 종목당 투자금 줄이기")
    
    result = {
        'full_kelly': round(full_kelly * 100, 1),
        'half_kelly': round(half_kelly * 100, 1),
        'recommended_pct': recommended,
        'max_buy_ratio': round(max_buy_ratio, 2),
        'win_rate': round(p * 100, 1),
        'payoff_ratio': round(b, 2),
        'analysis': analysis,
    }
    
    print(f"[KELLY] 승률{p*100:.0f}% 손익비{b:.2f} → 하프켈리{half_kelly*100:.1f}% 추천{recommended}%")
    return result

# ============= v4.0 PHASE 5: 종목간 상관관계 분석 =============

_correlation_cache = {'data': {}, 'ts': 0}
CORRELATION_CACHE_TTL = 600  # 10분

def calc_stock_correlation(app_key, app_secret, mode, token, tickers, days=20):
    """보유종목간 가격 상관관계 분석
    
    같은 방향으로 움직이는 종목이 많으면 = 리스크 집중
    상관관계 높은 종목 과집중 방지
    
    Returns: {pairs: [{t1, t2, correlation, risk}], concentration_score, warning}
    """
    now = time.time()
    cache_key = ','.join(sorted(tickers))
    cached = _correlation_cache.get('data', {}).get(cache_key)
    if cached and (now - _correlation_cache.get('ts', 0)) < CORRELATION_CACHE_TTL:
        return cached
    
    if len(tickers) < 2:
        return {'pairs': [], 'concentration_score': 0, 'warning': ''}
    
    # 각 종목 일봉 수익률 수집
    returns_map = {}  # {ticker: [daily_return, ...]}
    for ticker in tickers[:10]:
        candles = fetch_daily_candles(app_key, app_secret, mode, token, ticker, days)
        if len(candles) >= 5:
            rets = []
            for i in range(1, len(candles)):
                prev_c = candles[i-1]['close']
                if prev_c > 0:
                    rets.append((candles[i]['close'] - prev_c) / prev_c * 100)
            returns_map[ticker] = rets
    
    # 종목 쌍별 상관계수 계산 (피어슨)
    pairs = []
    tickers_with_data = list(returns_map.keys())
    
    for i in range(len(tickers_with_data)):
        for j in range(i + 1, len(tickers_with_data)):
            t1, t2 = tickers_with_data[i], tickers_with_data[j]
            r1, r2 = returns_map[t1], returns_map[t2]
            min_len = min(len(r1), len(r2))
            if min_len < 5:
                continue
            r1, r2 = r1[-min_len:], r2[-min_len:]
            
            # 피어슨 상관계수
            n = min_len
            sum1 = sum(r1); sum2 = sum(r2)
            sum1sq = sum(x**2 for x in r1); sum2sq = sum(x**2 for x in r2)
            psum = sum(r1[k]*r2[k] for k in range(n))
            
            num = psum - (sum1 * sum2 / n)
            den1 = (sum1sq - sum1**2 / n)
            den2 = (sum2sq - sum2**2 / n)
            den = (den1 * den2) ** 0.5 if den1 > 0 and den2 > 0 else 1
            
            corr = round(num / max(den, 0.001), 3)
            
            risk = 'high' if abs(corr) > 0.7 else ('medium' if abs(corr) > 0.4 else 'low')
            pairs.append({'t1': t1, 't2': t2, 'correlation': corr, 'risk': risk})
    
    # 집중도 점수 (0~100, 높을수록 위험)
    if pairs:
        high_corr = sum(1 for p in pairs if abs(p['correlation']) > 0.7)
        concentration = round(high_corr / max(len(pairs), 1) * 100)
    else:
        concentration = 0
    
    warning = ''
    if concentration > 50:
        warning = '⚠️ 포트폴리오 과집중! 같은 방향 종목이 너무 많음 → 분산 필요'
    elif concentration > 30:
        warning = '📋 일부 종목 상관관계 높음 → 동반 하락 리스크 주의'
    
    result = {
        'pairs': sorted(pairs, key=lambda x: abs(x['correlation']), reverse=True)[:10],
        'concentration_score': concentration,
        'warning': warning,
        'tickers_analyzed': len(tickers_with_data),
    }
    
    _correlation_cache['data'][cache_key] = result
    _correlation_cache['ts'] = now
    
    print(f"[CORR] {len(tickers_with_data)}종목 분석: 집중도{concentration}% "
          f"고상관{sum(1 for p in pairs if abs(p['correlation'])>0.7)}쌍 {warning[:20]}")
    return result

def build_correlation_context(corr):
    """상관관계 분석을 AI 프롬프트 텍스트로 변환"""
    if not corr or not corr.get('pairs'):
        return ""
    ctx = f"\n[포트폴리오 상관관계 — 집중도 {corr['concentration_score']}%]"
    if corr.get('warning'):
        ctx += f"\n{corr['warning']}"
    high_pairs = [p for p in corr['pairs'] if abs(p['correlation']) > 0.6]
    if high_pairs:
        ctx += "\n고상관 종목쌍: " + ', '.join(f"{p['t1']}-{p['t2']}({p['correlation']:.2f})" for p in high_pairs[:5])
        ctx += "\n★ 위 종목과 상관관계 높은 종목 추가 매수 자제!"
    return ctx

# ============= v4.0 PHASE 5: AI 매매일지 자동생성 =============

TRADE_JOURNAL_FILE = 'trade_journal.json'

def generate_daily_journal():
    """매일 장 마감 후 자동 매매일지 생성
    
    오늘 매매 전체를 복기하여:
    1. 잘한 점 (수익 종목 분석)
    2. 못한 점 (손실 종목 분석)  
    3. 패턴 인사이트 (반복되는 실수/성공)
    4. 내일 개선점
    
    Returns: {date, summary, good_trades, bad_trades, patterns, improvements, stats}
    """
    today = datetime.now().strftime('%Y-%m-%d')
    today_all = [t for t in trade_log if t.get('date') == today]
    
    buys = [t for t in today_all if t.get('type') in ('AI_BUY', 'CHAT_BUY') and t.get('success')]
    sells = [t for t in today_all if t.get('type') in ('SELL', 'FORCE_CLOSE', 'AI_SELL') and t.get('success')]
    blocked = [t for t in today_all if t.get('type') in ('BLOCKED', 'CAPITAL_BLOCK')]
    
    if not buys and not sells:
        return {'date': today, 'summary': '오늘 매매 없음'}
    
    # 기본 통계
    total_pnl = sum(float(t.get('pnl', 0) or 0) for t in sells)
    wins = [t for t in sells if float(t.get('pnl', 0) or 0) > 0]
    losses = [t for t in sells if float(t.get('pnl', 0) or 0) < 0]
    win_rate = round(len(wins) / max(len(wins) + len(losses), 1) * 100, 1)
    
    # 잘한 매매 (수익 TOP 3)
    good_trades = []
    for t in sorted(sells, key=lambda x: float(x.get('pnl', 0) or 0), reverse=True)[:3]:
        pnl = float(t.get('pnl', 0) or 0)
        if pnl > 0:
            good_trades.append({
                'name': t.get('name', ''), 'ticker': t.get('ticker', ''),
                'pnl': round(pnl), 'pnl_pct': float(t.get('pnl_pct', 0) or 0),
                'reason': t.get('reason', '')[:50],
            })
    
    # 못한 매매 (손실 TOP 3)
    bad_trades = []
    for t in sorted(sells, key=lambda x: float(x.get('pnl', 0) or 0))[:3]:
        pnl = float(t.get('pnl', 0) or 0)
        if pnl < 0:
            bad_trades.append({
                'name': t.get('name', ''), 'ticker': t.get('ticker', ''),
                'pnl': round(pnl), 'pnl_pct': float(t.get('pnl_pct', 0) or 0),
                'reason': t.get('reason', '')[:50],
            })
    
    # 패턴 분석
    patterns = []
    
    # 시간대별 성과
    from collections import defaultdict
    hour_pnl = defaultdict(list)
    for t in sells:
        h = (t.get('time', '') or '')[11:13]
        if h: hour_pnl[h].append(float(t.get('pnl', 0) or 0))
    
    best_hour = max(hour_pnl.items(), key=lambda x: sum(x[1]), default=('', []))
    worst_hour = min(hour_pnl.items(), key=lambda x: sum(x[1]), default=('', []))
    if best_hour[0] and sum(best_hour[1]) > 0:
        patterns.append(f"수익 좋은 시간: {best_hour[0]}시 (+₩{int(sum(best_hour[1])):,})")
    if worst_hour[0] and sum(worst_hour[1]) < 0:
        patterns.append(f"손실 많은 시간: {worst_hour[0]}시 (₩{int(sum(worst_hour[1])):,})")
    
    # 매수 후 보유시간 분석
    quick_sells = [t for t in sells if t.get('reason') and ('1차' in t['reason'] or '손절' in t['reason'])]
    if len(quick_sells) > len(sells) * 0.7:
        patterns.append("빠른 정리 비율 높음 → 진입 타이밍 재검토 필요")
    
    # 차단 분석
    if len(blocked) > len(buys) * 2:
        patterns.append(f"차단 {len(blocked)}건 vs 매수 {len(buys)}건 → 필터링 과도하거나 시장 악재")
    
    # 개선점
    improvements = []
    if win_rate < 45:
        improvements.append("승률 낮음 → AI 확신도 기준 상향, 진입 조건 강화")
    if bad_trades and abs(bad_trades[0].get('pnl', 0)) > sum(t.get('pnl', 0) for t in good_trades[:2]):
        improvements.append("대손실 1건이 수익 전체보다 큼 → 손절 더 빠르게")
    if len(buys) > 10:
        improvements.append(f"매수 {len(buys)}건 과다 → 종목당 투자금 늘리고 건수 줄이기")
    if not improvements:
        improvements.append("전반적으로 양호 — 현재 전략 유지")
    
    journal = {
        'date': today,
        'summary': f"매수{len(buys)} 매도{len(sells)} 차단{len(blocked)} | "
                   f"승률{win_rate}% | 손익{'+'if total_pnl>=0 else ''}₩{int(total_pnl):,}",
        'stats': {
            'buys': len(buys), 'sells': len(sells), 'blocked': len(blocked),
            'win_rate': win_rate, 'total_pnl': round(total_pnl),
            'wins': len(wins), 'losses': len(losses),
        },
        'good_trades': good_trades,
        'bad_trades': bad_trades,
        'patterns': patterns,
        'improvements': improvements,
        'generated_at': datetime.now().isoformat(),
    }
    
    # 파일 저장
    try:
        history = {}
        if os.path.exists(TRADE_JOURNAL_FILE):
            with open(TRADE_JOURNAL_FILE, 'r', encoding='utf-8') as f:
                history = json.load(f)
        history[today] = journal
        # 90일 보관
        keys = sorted(history.keys())
        if len(keys) > 90:
            for k in keys[:-90]:
                del history[k]
        with open(TRADE_JOURNAL_FILE, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
        print(f"[JOURNAL] 매매일지 저장: {today}")
    except Exception as e:
        print(f"[JOURNAL] 저장 실패: {e}")
    
    # 텔레그램 발송
    try:
        msg = f"📝 <b>매매일지 {today}</b>\n"
        msg += f"{journal['summary']}\n"
        if good_trades:
            msg += f"\n✅ 잘한 매매:\n"
            for g in good_trades:
                msg += f"  {g['name']} +₩{g['pnl']:,} ({g['pnl_pct']:+.1f}%)\n"
        if bad_trades:
            msg += f"\n❌ 아쉬운 매매:\n"
            for b in bad_trades:
                msg += f"  {b['name']} ₩{b['pnl']:,} ({b['pnl_pct']:+.1f}%)\n"
        if patterns:
            msg += f"\n📊 패턴:\n  " + '\n  '.join(patterns)
        if improvements:
            msg += f"\n💡 내일 개선:\n  " + '\n  '.join(improvements)
        tg_send(msg)
    except:
        pass
    
    return journal

# ============= v4.0 PHASE 3: AI 매도 판단 강화 =============

def build_sell_analysis_context(ticker, name, qty, avg_price, cur_price, pnl_pct, ta=None, ob=None, mtf=None):
    """보유종목의 매도 판단을 위한 종합 컨텍스트 빌드
    기술적 지표 + 호가 + 멀티타임프레임 + 수급을 종합하여
    AI가 '지금 매도해야 하는지' 판단할 수 있는 데이터 제공
    
    Returns: context_text, sell_urgency_score (-5~+5, 양수=매도 권장)
    """
    ctx = f"\n[보유종목 매도 분석: {name}({ticker})]"
    ctx += f"\n보유: {qty}주 | 평단₩{avg_price:,.0f} → 현재₩{cur_price:,.0f} | 수익률 {pnl_pct:+.1f}%"
    
    urgency = 0  # 양수 = 매도 방향, 음수 = 보유 방향
    reasons = []
    
    # 1. 기술적 지표 기반
    if ta:
        rsi = ta.get('rsi', 50)
        if rsi > 75:
            urgency += 2
            reasons.append(f'RSI{rsi}과매수→매도')
        elif rsi > 65:
            urgency += 1
            reasons.append(f'RSI{rsi}고구간')
        elif rsi < 30:
            urgency -= 2
            reasons.append(f'RSI{rsi}과매도→보유')
        
        if ta.get('macd_cross') == 'dead':
            urgency += 2
            reasons.append('MACD데드크로스→매도')
        elif ta.get('macd_cross') == 'golden':
            urgency -= 1
            reasons.append('MACD골든→보유')
        
        if ta.get('bb_position', 50) > 90:
            urgency += 1
            reasons.append('BB상단돌파→과열')
        
        ctx += build_ta_context(ta, ticker, name)
    
    # 2. 호가 기반
    if ob:
        strength = ob.get('strength', 100)
        if strength < 70:
            urgency += 2
            reasons.append(f'체결강도{strength}%약→매도')
        elif strength > 150:
            urgency -= 1
            reasons.append(f'체결강도{strength}%강→보유')
        ctx += build_orderbook_context(ob, ticker)
    
    # 3. 멀티타임프레임
    if mtf:
        if mtf.get('m1_signal') == 'strong_down':
            urgency += 2
            reasons.append('1분봉급락→매도')
        elif mtf.get('m1_signal') == 'strong_up':
            urgency -= 1
            reasons.append('1분봉급등→보유')
        ctx += build_mtf_context(mtf, ticker)
    
    # 4. 수익률 기반
    if pnl_pct > 5:
        urgency += 1
        reasons.append(f'+{pnl_pct:.1f}%수익→익절검토')
    elif pnl_pct < -3:
        urgency += 1
        reasons.append(f'{pnl_pct:.1f}%손실→손절검토')
    
    # 종합
    if urgency >= 3:
        verdict = '강한매도신호'
    elif urgency >= 1:
        verdict = '매도검토'
    elif urgency <= -2:
        verdict = '보유유지'
    else:
        verdict = '중립'
    
    ctx += f"\n★ AI 매도 판단: {verdict} (긴급도 {urgency:+d}) | {' / '.join(reasons[:4])}"
    
    return ctx, urgency

# ============= AUTO TRADING ENGINE =============
# Track peak prices for trailing stop (조기 선언 후 load_state에서 복원됨)
if not peak_prices:
    peak_prices = {}

# 중복 매도 방지: {ticker: last_sell_timestamp}
# 동일 종목 60초 내 재매도 차단
recently_sold = {}  # {ticker: time.time()}
SELL_COOLDOWN = 300  # seconds (5분 — 1차 익절 후 재발동 방지)
if not _tp1_triggered:
    _tp1_triggered = set()  # ★ v3.0: tp1 1회 발동된 종목 (같은 날 재발동 방지)

# AI provider config (set by auto/start or manual/sync)
ai_config = {'provider': 'openai', 'anthropic_key': '', 'openai_key': ''}

def _build_handover_ctx():
    """이전 브리핑들의 핵심 내용을 인수인계 컨텍스트로 조합"""
    ctx = ""
    # 08:30 글로벌 브리핑
    if daily_briefing.get('data'):
        d = daily_briefing['data']
        ok = d.get('korea_outlook', {})
        tp = d.get('trading_plan', {})
        ctx += f"[08:30 글로벌브리핑] 방향:{ok.get('direction','')}({ok.get('confidence',0)}%) {ok.get('summary','')[:80]}\n"
        themes = ', '.join(t.get('theme','') for t in tp.get('main_themes',[])[:3])
        if themes: ctx += f"  오늘테마: {themes}\n"
    # 10:00 오전 브리핑
    if daily_briefing.get('morning'):
        m = daily_briefing['morning']
        mr = m.get('morning_result', {})
        ctx += f"[10:00 오전브리핑] 코스피:{mr.get('kospi_change','')} 코스닥:{mr.get('kosdaq_change','')} {mr.get('summary','')[:80]}\n"
        ao = m.get('afternoon_strategy', {})
        if ao.get('direction'): ctx += f"  오전전략: {ao.get('direction','')} / {ao.get('hot_sectors',[])}\n"
    # 12:00 점심 브리핑
    if daily_briefing.get('noon'):
        n = daily_briefing['noon']
        nr = n.get('noon_result', {})
        ctx += f"[12:00 점심브리핑] {nr.get('summary','')[:80]}\n"
        ns = n.get('afternoon_strategy', {})
        if ns.get('strategy'): ctx += f"  오후전략: {ns.get('strategy','')[:80]}\n"
    # 14:00 오후 브리핑
    if daily_briefing.get('afternoon'):
        a = daily_briefing['afternoon']
        ar = a.get('afternoon_result', {})
        ctx += f"[14:00 오후브리핑] {ar.get('summary','')[:80]}\n"
        fc = a.get('final_strategy', {})
        if fc.get('strategy'): ctx += f"  마감전략: {fc.get('strategy','')[:80]}\n"
    # 중간 브리핑 (11:00 → 이전 방식)
    if daily_briefing.get('midday') and not daily_briefing.get('noon'):
        md = daily_briefing['midday']
        mr2 = md.get('morning_result', {})
        ctx += f"[중간브리핑] 코스피:{mr2.get('kospi_change','')} {mr2.get('summary','')[:60]}\n"
    return ctx

def safe_json_loads(raw_json, label=""):
    """JSON 파싱 보호: AI가 반환한 불완전 JSON 강력 자동 복구"""
    import re as _re_json
    
    _err_pos = 0  # 에러 위치 저장
    
    # 0단계: 그대로 파싱
    try:
        return json.loads(raw_json)
    except json.JSONDecodeError as e:
        _err_pos = e.pos or 0
        print(f"[{label}] ⚠️ JSON 1차 파싱 실패: {e} → 자동 복구 시도")
    
    # 1단계: 기본 수정
    _fixed = raw_json
    _fixed = _re_json.sub(r'^```(?:json)?\s*', '', _fixed, flags=_re_json.MULTILINE)
    _fixed = _re_json.sub(r'\s*```\s*$', '', _fixed, flags=_re_json.MULTILINE)
    _fixed = _re_json.sub(r',\s*}', '}', _fixed)
    _fixed = _re_json.sub(r',\s*]', ']', _fixed)
    _fixed = _re_json.sub(r'}\s*{', '},{', _fixed)
    _fixed = _re_json.sub(r'"\s*\n\s*"', '",\n"', _fixed)
    _fixed = _re_json.sub(r'(\d)\s*\n\s*"', r'\1,\n"', _fixed)
    _fixed = _re_json.sub(r'(true|false|null)\s*\n\s*"', r'\1,\n"', _fixed)
    _fixed = _re_json.sub(r'}\s*\n\s*"', '},\n"', _fixed)
    _fixed = _re_json.sub(r']\s*\n\s*"', '],\n"', _fixed)
    _fixed = _re_json.sub(r'"\s+"(?=[a-zA-Z_])', '", "', _fixed)
    _fixed = _re_json.sub(r'(\d)\s+"(?=[a-zA-Z_])', r'\1, "', _fixed)
    _fixed = _re_json.sub(r'([가-힣])"(\s*\n\s*")', r'\1",\2', _fixed)
    
    try:
        result = json.loads(_fixed)
        print(f"[{label}] ✅ JSON 자동 복구 성공 (1단계)")
        return result
    except json.JSONDecodeError as e:
        _err_pos2 = e.pos or 0
        print(f"[{label}] ⚠️ 1단계 실패: {e}")
    
    # 2단계: 에러 위치 기준 절삭
    for _pos in [_err_pos, _err_pos2]:
        if _pos > 100:
            _truncated = _fixed[:_pos-1].rstrip().rstrip(',')
            _open_b = _truncated.count('{') - _truncated.count('}')
            _open_k = _truncated.count('[') - _truncated.count(']')
            _truncated += ']' * max(_open_k, 0) + '}' * max(_open_b, 0)
            try:
                result = json.loads(_truncated)
                print(f"[{label}] ✅ JSON 절삭 복구 성공 (pos={_pos})")
                return result
            except:
                pass
    
    # 3단계: 가장 큰 완전한 JSON 객체 추출
    _depth = 0
    _start = -1
    _best = ""
    for i, ch in enumerate(_fixed):
        if ch == '{':
            if _depth == 0: _start = i
            _depth += 1
        elif ch == '}':
            _depth -= 1
            if _depth == 0 and _start >= 0:
                _candidate = _fixed[_start:i+1]
                if len(_candidate) > len(_best): _best = _candidate
    if _best:
        try:
            _best = _re_json.sub(r',\s*}', '}', _best)
            _best = _re_json.sub(r',\s*]', ']', _best)
            result = json.loads(_best)
            print(f"[{label}] ✅ JSON 최대 객체 추출 성공 ({len(_best)}자)")
            return result
        except:
            pass
    
    raise json.JSONDecodeError(f"[{label}] 모든 복구 실패", raw_json[:200], 0)

_ai_call_lock = __import__('threading').Lock()

def call_ai(prompt, system_prompt="", max_tokens=1000, web_search=True, tier='auto'):
    """Universal AI call - routes to selected provider.
    
    ★ v6.0 GPT 하이브리드:
    tier='scan'     : 급등주 단타 스캔 → GPT-4o-mini (속도+비용)
    tier='sell'     : 매도 상태 분류 → GPT-4o-mini
    tier='briefing' : 브리핑/수동매매/분석 → GPT-4o (품질)
    tier='auto'     : web_search 여부로 자동 판단
    
    → GPT 선택 시 = 전체 GPT-4o (매수/매도/브리핑 모두 최고 품질)
    → Claude 선택 시 = 전체 Claude Sonnet
    """
    # ★ v6.0: AI 동시호출 방지 (429 근본 해결)
    acquired = _ai_call_lock.acquire(timeout=60)
    if not acquired:
        raise Exception("AI 호출 대기 60초 초과 — 다른 AI 호출이 진행 중")
    try:
        return _call_ai_impl(prompt, system_prompt, max_tokens, web_search, tier)
    finally:
        _ai_call_lock.release()

def _call_ai_impl(prompt, system_prompt="", max_tokens=1000, web_search=True, tier='auto'):
    """실제 AI 호출 구현 (락 내부에서 실행)"""
    provider = ai_config.get('provider', 'openai')
    
    # ★ 모든 AI 호출에 현재 날짜/시간/장상태 자동 주입
    now = datetime.now()
    day_names = ['월','화','수','목','금','토','일']
    _is_hol, _hol_reason = is_market_holiday(now)
    hhmm = now.hour * 100 + now.minute
    market_open = not _is_hol and 900 <= hhmm <= 1530
    time_ctx = (f"[현재] {now.strftime('%Y년 %m월 %d일')} {day_names[now.weekday()]}요일 "
                f"{now.strftime('%H시 %M분')} | "
                f"{f'휴장({_hol_reason})' if _is_hol else ('장중' if market_open else '장외시간')}")
    if _is_hol:
        time_ctx += f" | ※휴장({_hol_reason})이므로 주가는 직전 거래일 종가"
    
    if system_prompt:
        system_prompt = time_ctx + "\n" + system_prompt
    else:
        system_prompt = time_ctx
    
    # ★ 하이브리드 tier 결정
    if tier == 'auto':
        tier = 'briefing' if web_search else 'scan'
    # sell은 그대로 유지
    
    estimated_cost = 40 if web_search else (0.1 if tier == 'sell' else (0.8 if tier == 'scan' else 5))
    call_type = {'scan': '스캔(매수)', 'sell': '매도상태(mini)', 'briefing': '브리핑(full)'}.get(tier, tier)
    print(f"[AI] {call_type} 호출 시작 ({time_ctx}) (모델: {provider}, 예상비용: ₩{estimated_cost})")
    
    if provider in ('openai', 'openai4o'):
        api_key = ai_config.get('openai_key', '')
        if not api_key:
            raise Exception("OpenAI API key not configured")
        
        # ★ v6.0: scan/sell → GPT-4o-mini (속도+비용), briefing/분석 → GPT-4o (품질)
        if tier in ('sell', 'scan'):
            model = 'gpt-4o-mini'  # 단타 스캔 + 매도 상태 = 속도 우선
        else:
            model = 'gpt-4o'  # 브리핑/심층분석/수동매매 = 품질 우선
        ai_config['last_model'] = model  # ★ v6.0: 마지막 사용 모델 추적
        print(f"[AI] → {model} (tier={tier})")
        
        input_msgs = []
        if system_prompt:
            input_msgs.append({"role": "developer", "content": system_prompt})
        input_msgs.append({"role": "user", "content": prompt})
        
        body = {
            "model": model,
            "input": input_msgs,
            "max_output_tokens": max_tokens,
            "temperature": 0.7
        }
        # ★ 웹검색은 브리핑에서만 사용 (단타 스캔은 KIS 데이터로 충분)
        if web_search:
            body["tools"] = [{"type": "web_search"}]

        payload = json.dumps(body).encode('utf-8')
        # ★ v6.0: 429 자동 재시도 (최대 3회) + 응답 본문 로깅 + chat/completions 폴백
        _last_err = None
        for _ai_retry in range(3):
            try:
                _req = urllib.request.Request('https://api.openai.com/v1/responses',
                    data=payload,
                    headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {api_key}'})
                with urllib.request.urlopen(_req, timeout=180) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                    track_api_usage(result)
                    
                    usage = result.get('usage', {})
                    in_tokens = usage.get('input_tokens', usage.get('prompt_tokens', 0))
                    out_tokens = usage.get('output_tokens', usage.get('completion_tokens', 0))
                    print(f"[AI] {call_type} 완료 - 토큰: {in_tokens}+{out_tokens} (예상: ₩{estimated_cost})")
                    
                    text = ''
                    for item in result.get('output', []):
                        if item.get('type') == 'message':
                            for c in item.get('content', []):
                                if c.get('type') == 'output_text':
                                    text += c.get('text', '')
                    return text
            except urllib.error.HTTPError as _he:
                _last_err = _he
                # ★ 429 응답 본문 읽기 (어떤 제한인지 파악)
                _err_body = ''
                try:
                    _err_body = _he.read().decode('utf-8', errors='replace')[:500]
                except: pass
                _retry_after = _he.headers.get('retry-after', '?') if hasattr(_he, 'headers') else '?'
                _ratelimit = _he.headers.get('x-ratelimit-remaining-requests', '?') if hasattr(_he, 'headers') else '?'
                _ratelimit_tokens = _he.headers.get('x-ratelimit-remaining-tokens', '?') if hasattr(_he, 'headers') else '?'
                
                if _he.code == 429:
                    print(f"[AI] ❌ 429 상세: retry-after={_retry_after}, remaining-req={_ratelimit}, remaining-tok={_ratelimit_tokens}")
                    print(f"[AI] ❌ 429 본문: {_err_body[:200]}")
                    
                    if _ai_retry < 2:
                        _bk = 15 * (2 ** _ai_retry)  # 15초, 30초
                        print(f"[AI] ⏳ 429 → {_bk}초 대기 후 재시도 ({_ai_retry+1}/3)")
                        time.sleep(_bk)
                    else:
                        # ★ 3차 실패 → /v1/chat/completions 폴백 시도
                        print(f"[AI] 🔄 responses API 3회 실패 → chat/completions 폴백")
                        try:
                            _fb_body = {
                                "model": model,
                                "messages": [],
                                "max_tokens": max_tokens,
                                "temperature": 0.7
                            }
                            if system_prompt:
                                _fb_body["messages"].append({"role": "system", "content": system_prompt})
                            _fb_body["messages"].append({"role": "user", "content": prompt})
                            _fb_payload = json.dumps(_fb_body).encode('utf-8')
                            _fb_req = urllib.request.Request('https://api.openai.com/v1/chat/completions',
                                data=_fb_payload,
                                headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {api_key}'})
                            with urllib.request.urlopen(_fb_req, timeout=180) as _fb_resp:
                                _fb_result = json.loads(_fb_resp.read().decode('utf-8'))
                                track_api_usage(_fb_result)
                                _fb_text = _fb_result.get('choices', [{}])[0].get('message', {}).get('content', '')
                                print(f"[AI] ✅ chat/completions 폴백 성공")
                                return _fb_text
                        except Exception as _fb_err:
                            print(f"[AI] ❌ chat/completions 폴백도 실패: {_fb_err}")
                            raise _last_err
                else:
                    raise
        raise _last_err
    else:
        # Claude/Anthropic
        api_key = ai_config.get('anthropic_key', '')
        if not api_key:
            raise Exception("Anthropic API key not configured")
        payload = {
            # ★ v5.0: sell tier → Haiku (속도 우선), scan/briefing → Sonnet
            "model": "claude-haiku-4-5-20251001" if tier == 'sell' else "claude-sonnet-4-20250514",
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}]}
        if system_prompt:
            payload["system"] = system_prompt
        # ★ 웹검색은 브리핑에서만
        if web_search:
            payload["tools"] = [{"type": "web_search_20250305", "name": "web_search"}]
        ai_body = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request('https://api.anthropic.com/v1/messages',
            data=ai_body,
            headers={'Content-Type': 'application/json', 'x-api-key': api_key, 'anthropic-version': '2023-06-01'})
        with urllib.request.urlopen(req, timeout=180) as resp:
            result = json.loads(resp.read().decode('utf-8'))
            track_api_usage(result)
            
            # ★ 실제 토큰 사용량과 비용 로깅
            usage = result.get('usage', {})
            in_tokens = usage.get('input_tokens', 0)
            out_tokens = usage.get('output_tokens', 0)
            print(f"[AI] {call_type} 완료 - 토큰: {in_tokens}+{out_tokens} (예상: ₩{estimated_cost})")
            
            text = ''.join(b.get('text', '') for b in result.get('content', []) if b.get('type') == 'text')
            return text

if not daily_briefing:  # ★ load_state에서 복원된 데이터가 있으면 유지
    daily_briefing = {}  # {date, data, generated_at} - pre-market global analysis
premarket_watchlist = {}  # {date, picks:[...]} - 8:50 프리마켓 워치리스트 (9:00 즉시매수용)
BRIEFING_HISTORY_FILE = "briefing_history.json"

def save_briefing_history():
    """Save daily_briefing to file for next-day handover"""
    try:
        history = load_briefing_history()
        today = datetime.now().strftime('%Y-%m-%d')
        history[today] = daily_briefing
        # Keep last 30 days
        dates = sorted(history.keys())
        if len(dates) > 30:
            for d in dates[:-30]:
                del history[d]
        with open(BRIEFING_HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False)
    except Exception as e:
        print(f"[BRIEFING] Save history error: {e}")

def load_briefing_history():
    try:
        with open(BRIEFING_HISTORY_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {}

def get_yesterday_closing():
    # ★★★ v8.0: handover+learning functions 삭제 (시황분석 제거, 실시간 데이터만 사용) ★★★

    """Fetch US indices, exchange rates, oil, gold using Naver JSON APIs"""
    result = {}
    ua = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    import re as _re
    
    def fetch_json(url):
        try:
            req = urllib.request.Request(url, headers=ua)
            with urllib.request.urlopen(req, timeout=8) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except Exception as e:
            pass  # [GLOBAL] API 404는 조용히 무시 (중요 로그 오염 방지)
            return None
    
    def fetch_html(url, enc='euc-kr'):
        try:
            req = urllib.request.Request(url, headers=ua)
            with urllib.request.urlopen(req, timeout=8) as resp:
                return resp.read().decode(enc, errors='replace')
        except Exception as e:
            pass
            return None
    
    # Method 1: Naver polling API (returns {pollingInterval, datas:[{cd,nv,cv,cr,...}], time})
    polling_codes = {
        'sp500': 'SPI@SPX', 'nasdaq': 'NAS@IXIC', 'dow': 'DJI@DJI',
        'nikkei': 'NII@NI225', 'shanghai': 'SHS@000001',
    }
    
    # Batch call - all indices at once
    try:
        codes_str = ','.join(polling_codes.values())
        api_url = f'https://polling.finance.naver.com/api/realtime/worldstock/index/{codes_str}'
        data = fetch_json(api_url)
        if data and data.get('datas'):
            print(f"[GLOBAL] Polling batch: {len(data['datas'])} items")
            for item in data['datas']:
                item_code = item.get('cd', '')
                for key, pcode in polling_codes.items():
                    if pcode == item_code or pcode.split('@')[-1] in item_code:
                        nv = item.get('nv') or item.get('closePrice') or ''  # 현재가
                        cv = item.get('cv') or item.get('change') or ''      # 전일대비
                        cr = item.get('cr') or item.get('changeRate') or ''   # 등락률
                        nm = item.get('nm') or item.get('name') or key       # 이름
                        if nv:
                            try:
                                val_f = float(str(nv).replace(',',''))
                                if val_f > 100:
                                    sign = '+' if float(str(cv).replace(',','') or '0') >= 0 else ''
                                    result[key] = {'value': f"{val_f:,.2f}", 'change': f"{sign}{cv} ({cr}%)"}
                                    print(f"[GLOBAL] {key}({nm}) = {val_f}")
                            except Exception as _e: print(f"[WARN] : {_e}")
                        break
    except Exception as e:
        pass
    
    # Individual calls for missing (especially nikkei, shanghai)
    alt_urls = {
        'nikkei': ['https://polling.finance.naver.com/api/realtime/worldstock/index/NII@NI225',
                   'https://polling.finance.naver.com/api/realtime/worldstock/stock/NII@NI225'],
        'shanghai': ['https://polling.finance.naver.com/api/realtime/worldstock/index/SHS@000001',
                     'https://polling.finance.naver.com/api/realtime/worldstock/stock/SHS@000001'],
    }
    for key, code in polling_codes.items():
        if key in result:
            continue
        urls = alt_urls.get(key, [f'https://polling.finance.naver.com/api/realtime/worldstock/index/{code}'])
        for api_url in urls:
            try:
                data = fetch_json(api_url)
                if data:
                    items = data.get('datas', [])
                    print(f"[GLOBAL] {key} individual: {len(items)} items from {api_url}")
                    if items:
                        item = items[0]
                        pass
                        nv = item.get('nv') or item.get('closePrice') or item.get('now') or ''
                        if nv:
                            val_f = float(str(nv).replace(',',''))
                            if val_f > 10:
                                cv = item.get('cv', ''); cr = item.get('cr', '')
                                sign = '+' if float(str(cv).replace(',','') or '0') >= 0 else ''
                                result[key] = {'value': f"{val_f:,.2f}", 'change': f"{sign}{cv} ({cr}%)"}
                                print(f"[GLOBAL] {key} = {val_f}")
                                break
            except Exception as e:
                pass
    
    # Method 2: Fallback - HTML scraping
    if not result.get('sp500'):
        html = fetch_html("https://finance.naver.com/world/", 'euc-kr')
        if html:
            for key, pat in {'sp500': r'SPX.*?([0-9,]+.[0-9]+)', 'nasdaq': r'IXIC.*?([0-9,]+.[0-9]+)', 'dow': r'DJI.*?([0-9,]+.[0-9]+)'}.items():
                m = _re.search(pat, html, _re.DOTALL)
                if m and key not in result:
                    result[key] = {'value': m.group(1), 'change': ''}
    
    # Exchange rates
    if not result.get('usd_krw'):
        for api_url in ['https://api.stock.naver.com/marketindex/exchange/FX_USDKRW',
                        'https://polling.finance.naver.com/api/realtime/marketindex/exchange/FX_USDKRW',
                        'https://m.stock.naver.com/api/exchange/FX_USDKRW/basic']:
            data = fetch_json(api_url)
            if not data: continue
            print(f"[GLOBAL] usd_krw from {api_url}: keys={list(data.keys())[:8]}")
            
            # Direct fields
            for field in ['closePrice','exchangeRate','basePrice','now','price','tradePrice']:
                v = data.get(field)
                if v:
                    try:
                        vf = float(str(v).replace(',',''))
                        if vf > 500:
                            result['usd_krw'] = str(v)
                            print(f"[GLOBAL] usd_krw = {v} (direct field: {field})")
                            break
                    except Exception as _e: print(f"[WARN] : {_e}")
            
            # Nested in exchangeInfo
            if not result.get('usd_krw') and data.get('exchangeInfo'):
                ei = data['exchangeInfo']
                if isinstance(ei, dict):
                    for field in ['closePrice','basePrice','price','tradePrice','cashBuyingPrice']:
                        v = ei.get(field)
                        if v:
                            try:
                                vf = float(str(v).replace(',',''))
                                if vf > 500:
                                    result['usd_krw'] = str(v)
                                    print(f"[GLOBAL] usd_krw = {v} (exchangeInfo.{field})")
                                    break
                            except Exception as _e: print(f"[WARN] : {_e}")
            
            # Nested in marketIndexCdList
            if not result.get('usd_krw') and data.get('marketIndexCdList'):
                for item in data['marketIndexCdList'][:5]:
                    if isinstance(item, dict):
                        for field in ['closePrice','basePrice','price']:
                            v = item.get(field)
                            if v:
                                try:
                                    vf = float(str(v).replace(',',''))
                                    if vf > 500:
                                        result['usd_krw'] = str(v)
                                        print(f"[GLOBAL] usd_krw from marketIndexCdList: {v}")
                                        break
                                except Exception as _e: print(f"[WARN] : {_e}")
                    if result.get('usd_krw'): break
            
            if result.get('usd_krw'): break
    
    # Fallback: marketindex HTML
    if not result.get('usd_krw'):
        html2 = fetch_html("https://finance.naver.com/marketindex/", 'euc-kr')
        if html2:
            m = _re.search(r'USD.*?([0-9]{1},?[0-9]{3}.[0-9]+)', html2, _re.DOTALL)
            if m: result['usd_krw'] = m.group(1)
    
    # WTI & Gold - try multiple API patterns + HTML fallback
    commodity_configs = {
        'wti': {
            'apis': ['https://polling.finance.naver.com/api/realtime/worldstock/futures/OIL_CL',
                     'https://polling.finance.naver.com/api/realtime/marketindex/productPrice/OIL_CL',
                     'https://api.stock.naver.com/marketindex/productPrice/OIL_CL',
                     'https://m.stock.naver.com/api/commodity/OIL_CL/basic'],
            'html_pattern': r'WTI.*?([0-9]+.[0-9]{2})',
            'min_val': 20, 'max_val': 200,
        },
        'gold': {
            'apis': ['https://polling.finance.naver.com/api/realtime/worldstock/futures/CMDT_GC',
                     'https://polling.finance.naver.com/api/realtime/marketindex/productPrice/CMDT_GC',
                     'https://api.stock.naver.com/marketindex/productPrice/CMDT_GC',
                     'https://m.stock.naver.com/api/commodity/CMDT_GC/basic'],
            'html_pattern': r'금 시세.*?([0-9,]+.[0-9]{2})',
            'min_val': 500, 'max_val': 10000,
        },
    }
    for key, cfg_c in commodity_configs.items():
        for api_url in cfg_c['apis']:
            data = fetch_json(api_url)
            if data:
                items = data.get('datas', [data])
                if isinstance(items, dict): items = [items]
                for item in items:
                    if isinstance(item, dict):
                        pass
                        for field in ['nv','closePrice','now','reutersPrice','price','tradePrice','basePrice','ncv','ov','hv','lv']:
                            v = item.get(field)
                            if v:
                                try:
                                    vf = float(str(v).replace(',',''))
                                    if cfg_c['min_val'] < vf < cfg_c['max_val']:
                                        result[key] = f"{vf:,.2f}"
                                        print(f"[GLOBAL] {key} = {vf} (field: {field})")
                                        break
                                except Exception as _e: print(f"[WARN] : {_e}")
                    if result.get(key): break
            if result.get(key): break
        
        # HTML fallback
        if not result.get(key):
            try:
                html = fetch_html("https://finance.naver.com/marketindex/", 'euc-kr')
                if html:
                    m = _re.search(cfg_c['html_pattern'], html, _re.DOTALL | _re.IGNORECASE)
                    if m:
                        vf = float(m.group(1).replace(',',''))
                        if cfg_c['min_val'] < vf < cfg_c['max_val']:
                            result[key] = m.group(1)
                            print(f"[GLOBAL] {key} = {m.group(1)} (HTML)")
                        else:
                            print(f"[GLOBAL] {key} HTML value {vf} out of range {cfg_c['min_val']}-{cfg_c['max_val']}")
            except Exception as _e: print(f"[WARN] : {_e}")
    
    # ★ v3.0 TIER 2: VIX 공포지수, 나스닥선물, 필라델피아반도체, 미국10년국채
    extra_indices = {
        'vix': {
            'apis': ['https://polling.finance.naver.com/api/realtime/worldstock/index/SPI@VIX',
                     'https://polling.finance.naver.com/api/realtime/worldstock/stock/SPI@VIX'],
            'min_val': 5, 'max_val': 100,
        },
        'nasdaq_futures': {
            'apis': ['https://polling.finance.naver.com/api/realtime/worldstock/futures/CME@NQ1!',
                     'https://polling.finance.naver.com/api/realtime/worldstock/index/CME@NQ'],
            'min_val': 5000, 'max_val': 50000,
        },
        'sox': {  # 필라델피아 반도체지수
            'apis': ['https://polling.finance.naver.com/api/realtime/worldstock/index/SPI@SOX',
                     'https://polling.finance.naver.com/api/realtime/worldstock/stock/SPI@SOX'],
            'min_val': 1000, 'max_val': 10000,
        },
        'us10y': {  # 미국 10년 국채금리
            'apis': ['https://polling.finance.naver.com/api/realtime/worldstock/bond/US10YT=XX',
                     'https://polling.finance.naver.com/api/realtime/worldstock/futures/BOND_US10Y'],
            'min_val': 0.5, 'max_val': 15,
        },
    }
    for key, cfg_e in extra_indices.items():
        for api_url in cfg_e['apis']:
            try:
                data = fetch_json(api_url)
                if data:
                    items = data.get('datas', [data])
                    if isinstance(items, dict): items = [items]
                    for item in items:
                        if isinstance(item, dict):
                            for field in ['nv','closePrice','now','reutersPrice','price','tradePrice','basePrice','ncv']:
                                v = item.get(field)
                                if v:
                                    try:
                                        vf = float(str(v).replace(',',''))
                                        if cfg_e['min_val'] < vf < cfg_e['max_val']:
                                            cv = item.get('cv', ''); cr = item.get('cr', '')
                                            sign = '+' if float(str(cv).replace(',','') or '0') >= 0 else ''
                                            result[key] = {'value': f"{vf:,.2f}", 'change': f"{sign}{cv} ({cr}%)"}
                                            print(f"[GLOBAL] {key} = {vf}")
                                            break
                                    except: pass
                        if result.get(key): break
                if result.get(key): break
            except: pass

    # News
    try:
        html3 = fetch_html("https://finance.naver.com/news/mainnews.naver", 'euc-kr')
        if html3:
            titles = _re.findall(r'title="([^"]{10,80})"', html3)
            result['news'] = [t.strip() for t in titles if len(t.strip()) > 12 and '\xc3' not in t][:8]
        else:
            result['news'] = []
    except Exception:
        result['news'] = []
    
    print(f"[GLOBAL] Fetched: {list(result.keys())}")
    return result

def build_global_context():
    """Build text context from global market data for AI prompts.
    단타 스캔에서 바로 활용할 수 있는 구조화된 요약도 포함.
    """
    g = fetch_global_market_data()
    ctx = f"[해외증시/환율/원자재 - {datetime.now().strftime('%Y-%m-%d %H:%M')}]\n"

    sp500_chg = ''
    nasdaq_chg = ''
    if g.get('sp500'):
        ctx += f"S&P500: {g['sp500']['value']} {g['sp500'].get('change','')}\n"
        sp500_chg = g['sp500'].get('change', '')
    if g.get('nasdaq'):
        ctx += f"나스닥: {g['nasdaq']['value']} {g['nasdaq'].get('change','')}\n"
        nasdaq_chg = g['nasdaq'].get('change', '')
    if g.get('dow'):
        ctx += f"다우: {g['dow']['value']} {g['dow'].get('change','')}\n"
    if g.get('nikkei'):
        ctx += f"닛케이: {g['nikkei']['value']} {g['nikkei'].get('change','')}\n"
    if g.get('shanghai'):
        ctx += f"상해종합: {g['shanghai']['value']} {g['shanghai'].get('change','')}\n"
    if g.get('usd_krw'):
        ctx += f"USD/KRW: {g['usd_krw']}\n"
    if g.get('wti'):
        ctx += f"WTI: ${g['wti']}\n"
    if g.get('gold'):
        ctx += f"금: ${g['gold']}\n"
    # ★ v3.0 TIER 2: 추가 글로벌 지표
    if g.get('vix'):
        vix_val = g['vix']
        vix_v = float(str(vix_val['value']).replace(',','')) if isinstance(vix_val, dict) else float(str(vix_val).replace(',',''))
        vix_level = '🟢안정' if vix_v < 20 else ('🟡경계' if vix_v < 30 else '🔴공포')
        ctx += f"VIX(공포지수): {vix_val['value'] if isinstance(vix_val,dict) else vix_val} {vix_level} {vix_val.get('change','') if isinstance(vix_val,dict) else ''}\n"
    if g.get('nasdaq_futures'):
        nf = g['nasdaq_futures']
        ctx += f"나스닥선물: {nf['value']} {nf.get('change','')} (프리마켓 방향 지표)\n"
    if g.get('sox'):
        sox = g['sox']
        ctx += f"필라델피아반도체(SOX): {sox['value']} {sox.get('change','')} (반도체섹터 선행지표)\n"
    if g.get('us10y'):
        us10 = g['us10y']
        ctx += f"미국10년국채: {us10['value']}% {us10.get('change','')} (금리→성장주 영향)\n"
    if g.get('news'):
        ctx += "[국제/증시뉴스] " + ' / '.join(g['news'][:5]) + "\n"
    if not any(g.get(k) for k in ['sp500', 'nasdaq', 'dow', 'usd_krw']):
        ctx += "(해외증시 데이터 수집 실패)\n"

    # ★ 단타 스캔용 구조화 요약 — 스캔 프롬프트에서 바로 참조
    def _sign_emoji(chg_str):
        try:
            val = float(str(chg_str).replace('%','').replace('+','').replace(',',''))
            return '🟢상승' if val >= 0 else '🔴하락'
        except:
            return ''

    us_signal = _sign_emoji(sp500_chg) or _sign_emoji(nasdaq_chg)
    usd_krw_val = ''
    try:
        usd_krw_val = float(str(g.get('usd_krw', '0')).replace(',', ''))
    except:
        pass
    krw_signal = ''
    if usd_krw_val:
        krw_signal = '🔴원화약세(수출주유리)' if usd_krw_val >= 1350 else '🟢원화강세(내수주유리)'

    ctx += f"\n[★ 단타 핵심 요약]\n"
    ctx += f"미국 전날: {us_signal} (S&P500 {sp500_chg} / 나스닥 {nasdaq_chg})\n"
    if krw_signal:
        ctx += f"환율: {g.get('usd_krw','')}원 → {krw_signal}\n"
    if g.get('wti'):
        ctx += f"유가: ${g['wti']} (에너지/화학 섹터 영향)\n"
    # ★ v3.0: 추가 지표 핵심 시그널
    if g.get('vix'):
        try:
            _vv = float(str(g['vix']['value'] if isinstance(g['vix'],dict) else g['vix']).replace(',',''))
            if _vv >= 30: ctx += f"⚠️ VIX {_vv:.1f} → 공포 구간! 방어적 매매, 소량 진입\n"
            elif _vv >= 25: ctx += f"⚠️ VIX {_vv:.1f} → 경계 구간. 변동성 큼, 신중 매매\n"
            elif _vv < 15: ctx += f"✅ VIX {_vv:.1f} → 안정 구간. 적극 매매 가능\n"
        except: pass
    if g.get('sox'):
        try:
            _sox_chg = g['sox'].get('change','')
            if '+' in str(_sox_chg): ctx += f"반도체(SOX) {_sox_chg} → 반도체/AI 섹터 강세 시그널\n"
            elif '-' in str(_sox_chg): ctx += f"반도체(SOX) {_sox_chg} → 반도체 약세 주의\n"
        except: pass
    if g.get('nasdaq_futures'):
        try:
            _nf_chg = g['nasdaq_futures'].get('change','')
            ctx += f"나스닥선물: {_nf_chg} (오늘 장 방향 선행지표)\n"
        except: pass

    # 브리핑 전략 구조화 주입 (daily_briefing 있을 경우)
    try:
        plan = daily_briefing.get('data', {}).get('trading_plan', {})
        if plan:
            themes = [t.get('theme', '') for t in plan.get('main_themes', [])[:3]]
            avoid = plan.get('avoid_themes', [])[:3]
            entry = plan.get('entry_strategy', '')
            if themes:
                ctx += f"오늘 공략 테마: {' / '.join(themes)}\n"
            if avoid:
                ctx += f"회피 섹터: {' / '.join(avoid)}\n"
            if entry:
                ctx += f"진입전략: {entry}\n"
    except Exception:
        pass

    return ctx

def calc_ord_psbl_cash(bal):
    """주문가능금액 계산 (총평가 - 보유종목평가)
    KIS inquire-balance output2에 ord_psbl_cash가 없으므로 직접 계산.
    매도 후 즉시 반영됨 (T+2 예수금과 달리)
    """
    try:
        out2 = (bal.get('output2', [{}]) or [{}])[0] or {}
        tot = int(out2.get('tot_evlu_amt', '0') or 0)
        # 보유종목 평가합계
        stock_eval = int(out2.get('evlu_amt_smtl_amt', '0') or 0)
        if not stock_eval:
            # fallback: output1에서 직접 합산
            stock_eval = sum(int(p.get('evlu_amt', '0') or 0) for p in bal.get('output1', [])
                           if int(p.get('hldg_qty', '0') or 0) > 0)
        available = tot - stock_eval
        return max(available, 0)
    except:
        return int((bal.get('output2', [{}]) or [{}])[0].get('dnca_tot_amt', '0') or 0)

def calc_auto_pnl_today(bal_output1, today):
    """자동매매(auto_tickers) 전용 당일 실현+평가 손익 계산
    
    자동매매(auto_tickers) 기반 손익 계산.
    - 실현손익: trade_log의 AI_BUY/AI_SELL 당일 체결 기준
    - 평가손익: auto_tickers에 있는 보유 종목만
    
    Returns: (auto_pnl: float, auto_pnl_pct: float, detail: str)
    """
    # 1. 당일 자동매매(단타) 실현손익 (매도 체결 기준)
    # ★★★ v8.0: swing(중장기) 매도는 제외 — 단타 탭 실적만 계산 ★★★
    realized = 0.0
    for t in trade_log:
        if t.get('date') != today:
            continue
        if t.get('type') in ('AI_SELL', 'SELL', 'FORCE_CLOSE') and t.get('success', True):
            # ★ swing 매도 제외 (trade_mode='swing'이면 중장기 탭 소속)
            if t.get('trade_mode') == 'swing':
                continue
            ticker = t.get('ticker', '')
            # ★ v3.0 FIX: trade_log에 pnl이 직접 있으면 그걸 사용 (기존 보유종목 매도 호환)
            if t.get('pnl') is not None and float(t.get('pnl', 0) or 0) != 0:
                realized += float(t.get('pnl', 0))
                continue
            # pnl 없으면 매수 로그에서 평단가 역추적
            sell_amt = float(t.get('price', 0) or 0) * int(t.get('qty', 0) or 0)
            buy_price = 0.0
            for b in reversed(trade_log):
                if (b.get('ticker') == ticker and
                        b.get('type') in ('AI_BUY', 'BUY', 'CHAT_BUY') and
                        b.get('success', True)):
                    buy_price = float(b.get('price', 0) or 0)
                    break
            if buy_price > 0:
                qty = int(t.get('qty', 0) or 0)
                realized += (float(t.get('price', 0) or 0) - buy_price) * qty

    # 2. 현재 자동매매 보유 평가손익
    unrealized = 0.0
    for p in bal_output1:
        ticker = p.get('pdno', '')
        if ticker not in auto_tickers:
            continue  # auto_tickers 외 종목 제외
        qty = int(p.get('hldg_qty', '0') or '0')
        if qty <= 0:
            continue
        cur_price = float(p.get('prpr', '0') or '0')
        avg_price = float(p.get('pchs_avg_pric', '0') or '0')
        if avg_price > 0:
            unrealized += (cur_price - avg_price) * qty

    auto_pnl = realized + unrealized
    seed = 10000000  # 기본값 (호출자가 cfg 시드 주입 가능)
    return auto_pnl, realized, unrealized

# ★ v3.0 E/F: 한국 증시 휴장일 캘린더
def is_market_holiday(date=None):
    """한국 증시 휴장일 체크 (주말 + 공휴일 + 대체휴일)
    Returns: (is_holiday: bool, reason: str)
    """
    if date is None:
        date = datetime.now()
    
    # 주말
    if date.weekday() >= 5:
        return True, '주말'
    
    mmdd = date.strftime('%m%d')
    yyyy = date.year
    
    # ★ 고정 공휴일 (매년 동일)
    fixed_holidays = {
        '0101': '신정',
        '0301': '삼일절',
        '0505': '어린이날',
        '0606': '현충일',
        '0815': '광복절',
        '1003': '개천절',
        '1009': '한글날',
        '1225': '크리스마스',
    }
    
    if mmdd in fixed_holidays:
        return True, fixed_holidays[mmdd]
    
    # ★ 음력 공휴일 (연도별 양력 변환) — 2025~2027
    # 설날(음력 1/1 ±1일), 추석(음력 8/15 ±1일), 부처님오신날(음력 4/8)
    lunar_holidays = {
        2025: {
            '0128': '설날 전날', '0129': '설날', '0130': '설날 다음날',
            '0505': '부처님오신날',  # 2025 어린이날과 겹침
            '1003': '추석 전날',  # 2025 개천절과 겹침
            '1004': '추석 전날',  # 대체
            '1005': '추석', '1006': '추석 다음날', '1007': '추석 대체휴일',
        },
        2026: {
            '0216': '설날 전날', '0217': '설날', '0218': '설날 다음날',
            '0524': '부처님오신날',
            '0921': '추석 전날', '0922': '추석', '0923': '추석 다음날',
        },
        2027: {
            '0205': '설날 전날', '0206': '설날', '0207': '설날 다음날',
            '0208': '설날 대체휴일',
            '0513': '부처님오신날',
            '1010': '추석 전날', '1011': '추석', '1012': '추석 다음날',
        },
    }
    
    year_holidays = lunar_holidays.get(yyyy, {})
    if mmdd in year_holidays:
        return True, year_holidays[mmdd]
    
    # ★ 선거일 등 임시 공휴일 (필요 시 추가)
    special_holidays = {
        # '20260603': '지방선거',  # 예시
    }
    yyyymmdd = date.strftime('%Y%m%d')
    if yyyymmdd in special_holidays:
        return True, special_holidays[yyyymmdd]
    
    return False, ''

def build_trade_summary(days=30):
    """Build trade history summary for AI context (last N days)"""
    from collections import defaultdict
    today = datetime.now()
    cutoff = (today - __import__('datetime').timedelta(days=days)).strftime('%Y-%m-%d')
    
    recent = [t for t in trade_log if (t.get('date', '') or t.get('time', '')[:10]) >= cutoff]
    buys = [t for t in recent if t.get('type') in ('BUY', 'AI_BUY') and t.get('success', True)]
    sells = [t for t in recent if t.get('type') in ('SELL') and t.get('success', True)]
    errors = [t for t in recent if 'ERROR' in t.get('type', '')]
    blocked = [t for t in recent if t.get('type') in ('BLOCKED', 'CAPITAL_BLOCK')]
    
    if not buys and not sells:
        return ""
    
    ctx = f"[매매 히스토리 최근 {days}일]\n"
    ctx += f"총 매수 {len(buys)}건 / 매도 {len(sells)}건 / 차단 {len(blocked)}건 / 에러 {len(errors)}건\n"
    
    # 종목별 매매 통계
    stock_stats = defaultdict(lambda: {'buys': 0, 'sells': 0, 'buy_amt': 0, 'sell_amt': 0, 'names': set()})
    for t in buys:
        tk = t.get('ticker', '')
        if tk:
            stock_stats[tk]['buys'] += 1
            stock_stats[tk]['buy_amt'] += float(t.get('price', 0) or 0) * int(t.get('qty', 0) or 0)
            stock_stats[tk]['names'].add(t.get('name', ''))
    for t in sells:
        tk = t.get('ticker', '')
        if tk:
            stock_stats[tk]['sells'] += 1
            stock_stats[tk]['sell_amt'] += float(t.get('price', 0) or 0) * int(t.get('qty', 0) or 0)
            stock_stats[tk]['names'].add(t.get('name', ''))
    
    # 수익/손실 종목
    profit_stocks = []
    loss_stocks = []
    for tk, st in stock_stats.items():
        name = list(st['names'])[0] if st['names'] else tk
        if st['sells'] > 0 and st['buy_amt'] > 0:
            pnl_pct = ((st['sell_amt'] - st['buy_amt']) / st['buy_amt'] * 100) if st['buy_amt'] > 0 else 0
            entry = f"{name}({tk}) 매수{st['buys']}회/매도{st['sells']}회"
            if pnl_pct > 0:
                profit_stocks.append(f"{entry} +{pnl_pct:.1f}%")
            else:
                loss_stocks.append(f"{entry} {pnl_pct:.1f}%")
    
    if profit_stocks:
        ctx += f"수익종목: {', '.join(profit_stocks[:5])}\n"
    if loss_stocks:
        ctx += f"손실종목: {', '.join(loss_stocks[:5])}\n"
    
    # 최근 5건 매매
    recent_trades = sorted(buys + sells, key=lambda x: x.get('time', ''), reverse=True)[:5]
    if recent_trades:
        ctx += "최근거래: " + ', '.join(
            f"{t.get('name','?')} {'매수' if t.get('type','').endswith('BUY') else '매도'} {t.get('qty',0)}주 W{t.get('price',0)} ({t.get('date',t.get('time','')[:10])})"
            for t in recent_trades
        ) + "\n"
    
    # 자주 매매한 종목 TOP3
    freq = sorted(stock_stats.items(), key=lambda x: x[1]['buys'] + x[1]['sells'], reverse=True)[:3]
    if freq:
        ctx += "자주거래: " + ', '.join(f"{list(v['names'])[0] if v['names'] else k}({k}) {v['buys']+v['sells']}회" for k, v in freq) + "\n"
    
    # 일별 매매 패턴
    day_stats = defaultdict(lambda: {'buys': 0, 'sells': 0})
    for t in buys:
        d = t.get('date', t.get('time', '')[:10])
        day_stats[d]['buys'] += 1
    for t in sells:
        d = t.get('date', t.get('time', '')[:10])
        day_stats[d]['sells'] += 1
    active_days = len(day_stats)
    avg_trades = (len(buys) + len(sells)) / max(active_days, 1)
    ctx += f"활성일수: {active_days}일 / 일평균 {avg_trades:.1f}건\n"
    
    return ctx

def fetch_naver_market_data():
    """Fetch real-time KOSPI/KOSDAQ + volume top using JSON API"""
    import re as _re
    result = {}
    ua = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    def fetch_json(url):
        try:
            req = urllib.request.Request(url, headers=ua)
            with urllib.request.urlopen(req, timeout=8) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except Exception as e:
            print(f"[NAVER] JSON error {url}: {e}")
            return None
    
    # KOSPI/KOSDAQ via JSON API
    for idx_name, idx_code in [('kospi', 'KOSPI'), ('kosdaq', 'KOSDAQ')]:
        data = fetch_json(f'https://m.stock.naver.com/api/index/{idx_code}/basic')
        if data:
            val = data.get('closePrice') or data.get('stockEndPrice') or data.get('now') or data.get('reutersPrice') or ''
            chg = data.get('compareToPreviousClosePrice') or ''
            pct = data.get('fluctuationsRatio') or ''
            if val:
                result[idx_name] = str(val)
                try:
                    sign = '+' if float(str(chg).replace(',','') or '0') >= 0 else ''
                except Exception:
                    sign = ''
                result[f'{idx_name}_change'] = f"{sign}{chg} ({pct}%)"
        else:
            print(f"[NAVER] ⚠️ {idx_code} JSON API 응답 없음")
    
    # Fallback: HTML scraping
    if not result.get('kospi'):
        try:
            req = urllib.request.Request("https://finance.naver.com/sise/", headers=ua)
            with urllib.request.urlopen(req, timeout=8) as resp:
                html = resp.read().decode('euc-kr', errors='replace')
            m = _re.search(r'KOSPI.*?([0-9,]+.[0-9]+)', html, _re.DOTALL)
            if m: result['kospi'] = m.group(1)
            m2 = _re.search(r'KOSDAQ.*?([0-9]+.[0-9]+)', html, _re.DOTALL)
            if m2: result['kosdaq'] = m2.group(1)
        except Exception as e:
            print(f"[NAVER] Sise HTML error: {e}")
    
    # Volume top - 제거 (404 에러 반복 방지, KIS로 대체)
    result['volume_top'] = []
    
    # Fallback: HTML volume top
    if not result.get('volume_top'):
        try:
            req = urllib.request.Request("https://finance.naver.com/sise/sise_quant.naver", headers=ua)
            with urllib.request.urlopen(req, timeout=8) as resp:
                html = resp.read().decode('euc-kr', errors='replace')
            rows = _re.findall(r'code=([0-9]{6})"[^>]*>([^<]+)</a>.*?class="number"[^>]*>\s*([0-9,]+)', html[:30000], _re.DOTALL)
            result['volume_top'] = [{'ticker':r[0],'name':r[1].strip(),'price':r[2].strip(),'change':''} for r in rows[:10]]
        except Exception as e:
            print(f"[NAVER] Volume HTML error: {e}")
            result['volume_top'] = []
    
    _hm_now = datetime.now().hour * 100 + datetime.now().minute
    _market_open = 840 <= _hm_now <= 1530
    if _market_open:
        print(f"[NAVER] Market: kospi={result.get('kospi','?')} kosdaq={result.get('kosdaq','?')} top={len(result.get('volume_top',[]))}")
    return result

def fetch_market_day_chart():
    """★ v3.0: 당일 코스피/코스닥 분봉 데이터 (9시부터 현재까지)"""
    ua = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    history = []
    try:
        kospi_data = {}
        kosdaq_data = {}
        # 여러 URL 시도 (네이버 API 변경 대비)
        _urls = [
            'https://m.stock.naver.com/api/index/{}/chart?period=day',
            'https://api.stock.naver.com/chart/domestic/index/{}/day',
            'https://m.stock.naver.com/api/index/{}/price?period=day',
        ]
        for idx, store in [('KOSPI', kospi_data), ('KOSDAQ', kosdaq_data)]:
            _fetched = False
            for url_tpl in _urls:
                if _fetched: break
                try:
                    _url = url_tpl.format(idx)
                    req = urllib.request.Request(_url, headers=ua)
                    with urllib.request.urlopen(req, timeout=8) as resp:
                        data = json.loads(resp.read().decode('utf-8'))
                    # 응답 형식 자동 감지
                    items = data if isinstance(data, list) else data.get('priceInfos', data.get('chartPrices', data.get('prices', [])))
                    if not isinstance(items, list): items = []
                    for item in items:
                        t = item.get('localTradedAt', '') or item.get('dt', '') or item.get('date', '')
                        v = item.get('closePrice') or item.get('tradePrice') or item.get('closeVal') or item.get('cp') or ''
                        if t and v:
                            hhmm = t[11:16] if len(t) > 11 else t[:5]
                            try:
                                store[hhmm] = float(str(v).replace(',',''))
                                _fetched = True
                            except: pass
                    if _fetched:
                        print(f"[CHART] {idx} 분봉 성공: {_url} → {len(store)}개")
                except Exception as e:
                    continue
            if not _fetched:
                print(f"[CHART] {idx} 모든 URL 실패 — 3분 수집 방식으로 대체")
        
        all_times = sorted(set(list(kospi_data.keys()) + list(kosdaq_data.keys())))
        last_k, last_d = 0, 0
        for t in all_times:
            k = kospi_data.get(t, last_k)
            d = kosdaq_data.get(t, last_d)
            if k > 0: last_k = k
            if d > 0: last_d = d
            if k > 0 or d > 0:
                history.append({'t': t, 'k': round(k, 2), 'd': round(d, 2)})
        print(f"[CHART] ✅ 당일 분봉 {len(history)}개")
    except Exception as e:
        print(f"[CHART] ❌ 분봉 조회 실패: {e}")
    return history

def fetch_naver_news(query='', ticker='', max_days=2):
    """Fetch latest news - JSON API first, HTML fallback
    max_days: 단타용 기본 2일(당일+전날). 종목분석용은 7일 등 상위에서 지정.
    """
    import re as _re
    from datetime import timedelta as _td
    news = []
    ua = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    today_dt = datetime.now()
    cutoff_dt = today_dt - _td(days=max_days)  # 이 날짜 이전 뉴스는 제외

    def _parse_date(date_str):
        """날짜 문자열 → datetime 또는 None"""
        if not date_str:
            return None
        m = _re.search(r'([0-9]{4})[.\-]([0-9]{1,2})[.\-]([0-9]{1,2})', str(date_str))
        if m:
            try:
                return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except Exception:
                return None
        # 숫자만 있는 경우 (20260311)
        m2 = _re.search(r'([0-9]{4})([0-9]{2})([0-9]{2})', str(date_str))
        if m2:
            try:
                return datetime(int(m2.group(1)), int(m2.group(2)), int(m2.group(3)))
            except Exception:
                return None
        return None

    def _date_label(dt):
        if dt:
            return f"[{dt.strftime('%m.%d')}] "
        return "[오늘] "

    # Method 1: JSON API for stock-specific news (단수/복수 둘 다 시도)
    if ticker:
        for _news_path in [f'https://m.stock.naver.com/api/stock/{ticker}/news?page=1&pageSize=15',
                           f'https://m.stock.naver.com/api/stocks/{ticker}/news?page=1&pageSize=15']:
            try:
                req = urllib.request.Request(_news_path, headers=ua)
                with urllib.request.urlopen(req, timeout=8) as resp:
                    data = json.loads(resp.read().decode('utf-8'))
                items = data if isinstance(data, list) else data.get('news', data.get('items', data.get('list', [])))
                for item in items[:15]:
                    title = item.get('title') or item.get('articleTitle') or item.get('tit') or ''
                    title = _re.sub(r'<[^>]+>', '', title).strip()
                    date_str = item.get('date') or item.get('publishDate') or item.get('datetime') or item.get('wrtDt') or ''
                    article_dt = _parse_date(date_str)
                    if article_dt and article_dt < cutoff_dt:
                        continue
                    if len(title) > 10:
                        news.append(f"{_date_label(article_dt)}{title}")
                if news:
                    return news[:8]
            except Exception as e:
                continue  # 이 경로 실패 → 다음 경로 시도

    # Method 2: HTML scraping (fallback or general news)
    try:
        if ticker:
            url = f"https://finance.naver.com/item/news_news.naver?code={ticker}&page=1"
        else:
            url = "https://finance.naver.com/news/mainnews.naver"
        req = urllib.request.Request(url, headers=ua)
        with urllib.request.urlopen(req, timeout=8) as resp:
            html = resp.read().decode('euc-kr', errors='replace')
        titles = _re.findall(r'title="([^"]{10,80})"', html)
        if not titles:
            titles = _re.findall(r'class="articleSubject"[^>]*>.*?<a[^>]*title="([^"]{10,80})"', html, _re.DOTALL)
        # HTML은 날짜 파싱 어려워서 [오늘] 태그만 붙임
        news = [f"[오늘] {t.strip()}" for t in titles if len(t.strip()) > 12 and '네이버' not in t][:8]
    except Exception as e:
        print(f"[NEWS] HTML error: {e}")

    return news

def fetch_dart_recent(dart_key, ticker=''):
    """Fetch recent DART disclosures"""
    if not dart_key:
        return []
    disclosures = []
    try:
        today = datetime.now()
        bgn = (today - __import__('datetime').timedelta(days=7)).strftime('%Y%m%d')
        end = today.strftime('%Y%m%d')
        dart_url = f"https://opendart.fss.or.kr/api/list.json?crtfc_key={dart_key}&bgn_de={bgn}&end_de={end}&page_count=10"
        if ticker:
            # Need corp_code mapping - skip for now, use general
            pass
        req = urllib.request.Request(dart_url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode('utf-8'))
        if data.get('status') == '000':
            for item in data.get('list', [])[:10]:
                disclosures.append({
                    'corp': item.get('corp_name', ''),
                    'title': item.get('report_nm', ''),
                    'date': item.get('rcept_dt', '')
                })
    except Exception as e:
        print(f"[DART] Error: {e}")
    return disclosures

def collect_market_context(cfg, skip_tickers=None):
    """공통 시장 데이터 수집 (KIS 1순위 → 네이버 2순위)
    Returns: (market_ctx: str, global_ctx: str, sources: list)
    skip_tickers: 보유/차단 종목 → 거래량TOP에서 제거
    """
    _ctx_skip = set(skip_tickers or [])
    sources = []
    market_ctx = ""
    
    # KIS 인증
    kis_key = cfg.get('app_key', '')
    kis_secret = cfg.get('app_secret', '')
    kis_mode = 'live'
    kis_token = None
    
    if kis_key and kis_secret:
        try:
            kis_token = kis_get_token(kis_key, kis_secret, kis_mode)
        except Exception:
            pass
    
    # ===== 1순위: KIS 증권사 API =====
    kospi = kosdaq = ''
    if kis_token:
        # 1-1) 코스피/코스닥 지수
        for idx_name, idx_code in [('코스피', '0001'), ('코스닥', '1001')]:
            try:
                data = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-index-price",
                    kis_key, kis_secret, kis_mode, kis_token, "FHPUP02100000",
                    params={"FID_COND_MRKT_DIV_CODE": "U", "FID_INPUT_ISCD": idx_code})
                o = data.get('output', {})
                val = o.get('bstp_nmix_prpr', '')
                chg = o.get('bstp_nmix_prdy_vrss', '')
                pct = o.get('bstp_nmix_prdy_ctrt', '')
                vol = o.get('acml_vol', '')
                amt = o.get('acml_tr_pbmn', '')
                if val:
                    sign = '+' if float(str(chg).replace(',','') or '0') >= 0 else ''
                    market_ctx += f"{idx_name}: {val} ({sign}{chg}, {pct}%) 거래량 {vol}\n"
                    if idx_name == '코스피': kospi = val
                    else: kosdaq = val
            except Exception:
                pass
        
        if kospi:
            sources.append('kis_index')
            print(f"[MARKET] ✅ KIS 지수: 코스피 {kospi} 코스닥 {kosdaq}")
        
        # 1-2) 거래량 순위 TOP
        try:
            data = kis_request("GET", "/uapi/domestic-stock/v1/quotations/volume-rank",
                kis_key, kis_secret, kis_mode, kis_token, "FHPST01710000",
                params={"FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "20171",
                        "FID_INPUT_ISCD": "0000", "FID_DIV_CLS_CODE": "0",
                        "FID_BLNG_CLS_CODE": "0", "FID_TRGT_CLS_CODE": "111111111",
                        "FID_TRGT_EXLS_CLS_CODE": "000000", "FID_INPUT_PRICE_1": "0",
                        "FID_INPUT_PRICE_2": "0", "FID_VOL_CNT": "0",
                        "FID_INPUT_DATE_1": ""})
            items = data.get('output', [])
            if items:
                market_ctx += f"\n[거래량TOP - KIS 실시간]\n"
                for s in items[:10]:
                    nm = s.get('hts_kor_isnm', '')
                    cd = s.get('mksc_shrn_iscd', '')
                    if cd and cd in _ctx_skip:
                        continue  # ★ 보유/차단 종목 제외
                    pr = s.get('stck_prpr', '')
                    ct = s.get('prdy_ctrt', '')
                    vl = s.get('acml_vol', '')
                    sign = '+' if float(str(ct).replace(',','') or '0') >= 0 else ''
                    market_ctx += f"  {nm}({cd}) {pr}원 {sign}{ct}% 거래량 {vl}\n"
                sources.append('kis_volume')
                print(f"[MARKET] ✅ KIS 거래량TOP: {len(items[:10])}종목")
                time.sleep(0.5)  # KIS rate limit 보호
        except Exception as e:
            print(f"[MARKET] KIS volume error: {e}")
        
        # 1-3) 외국인/기관 매매동향
        try:
            data = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-investor",
                kis_key, kis_secret, kis_mode, kis_token, "FHKST03010100",
                params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": "0001"})
            items = data.get('output', [])
            if items:
                latest = items[0]
                frgn = latest.get('frgn_ntby_qty', '')
                orgn = latest.get('orgn_ntby_qty', '')
                prsn = latest.get('prsn_ntby_qty', '')
                market_ctx += f"\n[투자자별 매매동향 - KIS]\n외국인: {frgn} / 기관: {orgn} / 개인: {prsn}\n"
                sources.append('kis_investor')
                print(f"[MARKET] ✅ KIS 수급: 외국인 {frgn} 기관 {orgn}")
                time.sleep(0.5)  # KIS rate limit 보호
        except Exception as e:
            print(f"[MARKET] KIS investor error: {e}")
        
        # 1-4) 해외지수
        for key, info in {'S&P500': {'code': 'SPX', 'excd': 'NAS'}, '나스닥': {'code': 'COMP', 'excd': 'NAS'}, '다우': {'code': '.DJI', 'excd': 'NYS'}}.items():
            try:
                data = kis_request("GET", "/uapi/overseas-price/v1/quotations/price",
                    kis_key, kis_secret, kis_mode, kis_token, "HHDFS00000300",
                    params={"AUTH": "", "EXCD": info['excd'], "SYMB": info['code']})
                o = data.get('output', {})
                price = o.get('last') or o.get('stck_prpr') or ''
                diff = o.get('diff') or o.get('prdy_vrss') or ''
                rate = o.get('rate') or o.get('prdy_ctrt') or ''
                if price and float(str(price).replace(',','')) > 100:
                    market_ctx += f"{key}: {price} ({'+' if float(str(diff).replace(',','') or '0')>=0 else ''}{diff}, {rate}%)\n"
                    if 'kis_overseas' not in sources:
                        sources.append('kis_overseas')
            except Exception:
                pass
    
    # ===== 2순위: 네이버 API (KIS 실패 시 fallback) =====
    _naver = None
    if 'kis_index' not in sources or 'kis_volume' not in sources:
        try:
            _naver = fetch_naver_market_data()
        except Exception as e:
            print(f"[MARKET] 네이버 fallback error: {e}")
    
    # 지수 fallback
    if 'kis_index' not in sources and _naver:
        if _naver.get('kospi') and _naver['kospi'] != '?':
            market_ctx += f"\n코스피: {_naver['kospi']} ({_naver.get('kospi_change','')})\n"
            market_ctx += f"코스닥: {_naver.get('kosdaq','?')} ({_naver.get('kosdaq_change','')})\n"
            sources.append('naver_index')
            print(f"[MARKET] ⚠️ 네이버 지수 fallback")
    
    # 거래량TOP fallback
    if 'kis_volume' not in sources and _naver:
        if _naver.get('volume_top'):
            market_ctx += f"\n[거래량TOP - 네이버]\n"
            for s in _naver['volume_top'][:10]:
                if s.get('ticker','') in _ctx_skip:
                    continue  # ★ 보유/차단 종목 제외
                market_ctx += f"  {s.get('name','')}({s.get('ticker','')}) {s.get('price','')}원\n"
            sources.append('naver_volume')
    
    # 뉴스
    try:
        news = fetch_naver_news()
        if news:
            market_ctx += "\n[최신 증시뉴스]\n" + '\n'.join(f"- {n}" for n in news[:5]) + "\n"
            sources.append('news')
    except Exception:
        pass
    
    # DART
    dk = cfg.get('dart_key', '') or ai_config.get('dart_key', '')
    if dk:
        try:
            dart = fetch_dart_recent(dk)
            if dart:
                market_ctx += "\n[최근 전자공시]\n" + '\n'.join(f"- {d['corp']} {d['title']}" for d in dart[:5]) + "\n"
                sources.append('dart')
        except Exception:
            pass
    
    # 해외증시/환율/유가 (KIS 해외지수와 병합)
    global_ctx = build_global_context()
    if global_ctx:
        sources.append('global')
    
    print(f"[MARKET] 수집 완료: {sources}")
    return market_ctx, global_ctx, sources

def collect_wide_candidates(app_key, app_secret, mode, token, exclude_etf=True, skip_tickers=None):
    """★ 광범위 후보종목 풀 수집 - 8가지 소스 통합, 목표 200개
    
    소스: 거래량TOP×2 + 상승률TOP×2(코스피/코스닥) + 거래량코스닥 + 등락률TOP
         + 외국인순매수TOP + 기관순매수TOP
    skip_tickers: 차단/보유 종목 → 수집 단계에서 바로 제거
    """
    _skip = set(skip_tickers or [])
    ua = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    # ★★★ v6.0: 네이버 API 가격 헬퍼 (장중 closePrice 빈 문자열 대응) ★★★
    def _naver_price(s):
        """네이버 응답에서 가격 추출 — 여러 필드 순서대로 시도"""
        for key in ('closePrice', 'currentPrice', 'dealPrice', 'openPrice', 'basePrice'):
            v = s.get(key, '')
            if v and str(v).replace(',','').replace('.','').isdigit():
                return str(v).replace(',','')
        return '0'
    
    candidates = {}  # ticker → info dict (중복 자동 제거)

    # ★ ETF/인버스/레버리지/채권/리츠 제외 키워드
    ETF_KEYWORDS = ['KODEX','TIGER','KBSTAR','HANARO','KOSEF','ARIRANG','TIMEFOLIO',
                    'SOL','ACE','FOCUS','RISE','PLUS','BNK','WOORI',
                    '인버스','레버리지','2X','3X','선물','채권','국채','금리',
                    'ETF','etf','나스닥','S&P','미국','일본','중국','베트남',
                    '리츠','부동산','원자재','금','은','원유','달러']

    def _add(ticker, name, price, chg_pct, source, extra='', volume=0):
        if not ticker or len(str(ticker)) != 6:
            return
        t = str(ticker).zfill(6)
        # ★ 차단/보유 종목 수집 단계 제거
        if t in _skip:
            return
        # ★ ETF/인버스/레버리지 제외
        if exclude_etf:
            name_str = str(name)
            for kw in ETF_KEYWORDS:
                if kw in name_str:
                    return
        # ★★★ v6.0: 3만원 초과 고가주는 수집 단계에서 즉시 제거 ★★★
        try:
            _p = float(str(price).replace(',','') or 0)
            if _p > 30000:
                return
        except:
            pass
        # ★★★ v6.0: 거래량 없는 종목 후보풀 제외 ★★★
        # 거래량 1만주 미만 = 유동성 부족, 매도 시 슬리피지 위험
        try:
            _v = int(str(volume).replace(',','') or 0)
            if _v > 0 and _v < 10000:
                return
        except:
            pass
        if t not in candidates:
            candidates[t] = {'ticker': t, 'name': name, 'price': str(price),
                              'chg_pct': str(chg_pct), 'sources': [], 'extra': ''}
        else:
            # ★ v6.0: 기존에 가격 없고 새로 가격 있으면 업데이트
            try:
                _old_p = float(str(candidates[t]['price']).replace(',','') or 0)
                _new_p = float(str(price).replace(',','') or 0)
                if _old_p <= 0 and _new_p > 0:
                    candidates[t]['price'] = str(price)
            except: pass
        candidates[t]['sources'].append(source)
        if extra:
            candidates[t]['extra'] += extra

    # ── 1. KIS 거래량TOP (코스피+코스닥 합산, ISCD=0000)
    try:
        data = kis_request("GET", "/uapi/domestic-stock/v1/quotations/volume-rank",
            app_key, app_secret, mode, token, "FHPST01710000",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "20171",
                    "FID_INPUT_ISCD": "0000", "FID_DIV_CLS_CODE": "0",
                    "FID_BLNG_CLS_CODE": "0", "FID_TRGT_CLS_CODE": "111111111",
                    "FID_TRGT_EXLS_CLS_CODE": "000000", "FID_INPUT_PRICE_1": "0",
                    "FID_INPUT_PRICE_2": "0", "FID_VOL_CNT": "0", "FID_INPUT_DATE_1": ""})
        for s in data.get('output', [])[:20]:
            _add(s.get('mksc_shrn_iscd',''), s.get('hts_kor_isnm',''),
                 s.get('stck_prpr',''), s.get('prdy_ctrt',''), '거래량TOP',
                 volume=s.get('acml_vol','0'))
        time.sleep(0.3)
        print(f"[WIDE] KIS 거래량TOP: {len(candidates)}개")
    except Exception as e:
        print(f"[WIDE] KIS volume-rank error: {e}")

    # ── 2. 네이버 상승률TOP - 코스피
    try:
        req = urllib.request.Request(
            'https://m.stock.naver.com/api/stocks/up/KOSPI?page=1&pageSize=30', headers=ua)
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode('utf-8'))
        for s in data.get('stocks', [])[:20]:
            _add(s.get('stockCode',''), s.get('stockName',''),
                 _naver_price(s), s.get('fluctuationsRatio',''), '상승률코스피',
                 volume=s.get('accumulatedTradingVolume', s.get('tradeVolume', '0')))
        print(f"[WIDE] 네이버 상승률KOSPI: {len(candidates)}개 누적")
    except Exception as e:
        print(f"[WIDE] Naver rise KOSPI error: {e}")

    # ── 3. 네이버 상승률TOP - 코스닥
    try:
        req = urllib.request.Request(
            'https://m.stock.naver.com/api/stocks/up/KOSDAQ?page=1&pageSize=30', headers=ua)
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode('utf-8'))
        for s in data.get('stocks', [])[:20]:
            _add(s.get('stockCode',''), s.get('stockName',''),
                 _naver_price(s), s.get('fluctuationsRatio',''), '상승률코스닥',
                 volume=s.get('accumulatedTradingVolume', s.get('tradeVolume', '0')))
        print(f"[WIDE] 네이버 상승률KOSDAQ: {len(candidates)}개 누적")
    except Exception as e:
        print(f"[WIDE] Naver rise KOSDAQ error: {e}")

    # ── 4. 네이버 거래량TOP - 코스닥 (404 제거, KIS로 대체 완료)
    # ── 5. KIS 등락률순위 (당일 급등 종목 - 거래량TOP와 다른 군)
    try:
        data = kis_request("GET", "/uapi/domestic-stock/v1/quotations/volume-rank",
            app_key, app_secret, mode, token, "FHPST01710000",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "20171",
                    "FID_INPUT_ISCD": "0000", "FID_DIV_CLS_CODE": "1",  # ★ 1=등락률순
                    "FID_BLNG_CLS_CODE": "0", "FID_TRGT_CLS_CODE": "111111111",
                    "FID_TRGT_EXLS_CLS_CODE": "000000", "FID_INPUT_PRICE_1": "0",
                    "FID_INPUT_PRICE_2": "0", "FID_VOL_CNT": "0", "FID_INPUT_DATE_1": ""})
        for s in data.get('output', [])[:20]:
            _add(s.get('mksc_shrn_iscd',''), s.get('hts_kor_isnm',''),
                 s.get('stck_prpr',''), s.get('prdy_ctrt',''), '등락률TOP',
                 volume=s.get('acml_vol','0'))
        time.sleep(0.3)
        print(f"[WIDE] KIS 등락률TOP: {len(candidates)}개 누적")
    except Exception as e:
        print(f"[WIDE] KIS rate-rank error: {e}")

    # ── 6. 네이버 거래량TOP 제거 (404, KIS volume-rank로 대체 완료)

    # ── 7. KIS 외국인 순매수 TOP (코스피)
    try:
        data = kis_request("GET", "/uapi/domestic-stock/v1/quotations/foreign-institution-total",
            app_key, app_secret, mode, token, "FHPTJ04400000",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "20444",
                    "FID_INPUT_ISCD": "0001", "FID_DIV_CLS_CODE": "0",
                    "FID_RANK_SORT_CLS_CODE": "0", "FID_ETC_CLS_CODE": "0"})
        for s in data.get('output', [])[:20]:
            _add(s.get('mksc_shrn_iscd',''), s.get('hts_kor_isnm',''),
                 s.get('stck_prpr',''), s.get('prdy_ctrt',''), '외국인순매수',
                 f" 외국인:{s.get('frgn_ntby_qty','')}주",
                 volume=s.get('acml_vol','0'))
        time.sleep(0.3)
        print(f"[WIDE] KIS 외국인순매수: {len(candidates)}개 누적")
    except Exception as e:
        print(f"[WIDE] KIS foreign error: {e}")

    # ── 8. KIS 기관 순매수 TOP (코스피)
    try:
        data = kis_request("GET", "/uapi/domestic-stock/v1/quotations/foreign-institution-total",
            app_key, app_secret, mode, token, "FHPTJ04400000",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "20444",
                    "FID_INPUT_ISCD": "0001", "FID_DIV_CLS_CODE": "1",
                    "FID_RANK_SORT_CLS_CODE": "0", "FID_ETC_CLS_CODE": "0"})
        for s in data.get('output', [])[:20]:
            _add(s.get('mksc_shrn_iscd',''), s.get('hts_kor_isnm',''),
                 s.get('stck_prpr',''), s.get('prdy_ctrt',''), '기관순매수',
                 f" 기관:{s.get('orgn_ntby_qty','')}주",
                 volume=s.get('acml_vol','0'))
        time.sleep(0.3)
        print(f"[WIDE] KIS 기관순매수: {len(candidates)}개 누적")
    except Exception as e:
        print(f"[WIDE] KIS institution error: {e}")

    # ── 9. 네이버 상승률 2페이지 (코스피+코스닥) - 매 스캔마다 다양성 확보
    try:
        for market in ['KOSPI', 'KOSDAQ']:
            req = urllib.request.Request(
                f'https://m.stock.naver.com/api/stocks/up/{market}?page=2&pageSize=30', headers=ua)
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read().decode('utf-8'))
            for s in data.get('stocks', [])[:15]:
                _add(s.get('stockCode',''), s.get('stockName',''),
                     _naver_price(s), s.get('fluctuationsRatio',''), f'상승률{market}2p',
                     volume=s.get('accumulatedTradingVolume', s.get('tradeVolume', '0')))
        print(f"[WIDE] 상승률 2페이지: {len(candidates)}개 누적")
    except Exception as e:
        print(f"[WIDE] Naver rise 2p error: {e}")

    # ★★★ v6.0: 가격 없는 후보는 KIS 현재가 조회 (1순위 KIS) ★★★
    _no_price = [c for c in candidates.values() if float(str(c.get('price','0')).replace(',','') or 0) <= 0]
    if _no_price and token:
        _price_fixed = 0
        for c in _no_price[:15]:  # 최대 15종목 (API 부하 제한)
            try:
                _pr = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
                    app_key, app_secret, mode, token, "FHKST01010100",
                    params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": c['ticker']})
                _kis_p = int(float(_pr.get('output', {}).get('stck_prpr', '0') or '0'))
                if _kis_p > 0:
                    c['price'] = str(_kis_p)
                    _price_fixed += 1
                time.sleep(0.08)
            except: pass
        if _price_fixed:
            print(f"[WIDE] KIS 가격 보완: {_price_fixed}/{len(_no_price)}종목 가격 확보")
    
    # ★ 가격 확인 후 최종 필터: 가격 없음(0) + 30000 초과 제거
    def _get_price(c):
        try: return float(str(c.get('price','0')).replace(',','') or 0)
        except: return 0
    result = [c for c in candidates.values() if 0 < _get_price(c) <= 30000]
    
    import random
    random.shuffle(result)
    _removed_final = len(candidates) - len(result)
    print(f"[WIDE] 총 후보: {len(result)}개 (수집{len(candidates)}→가격필터-{_removed_final}→셔플)")
    return result

def collect_stock_details(cfg, token, app_key, app_secret, mode, max_stocks=5, exclude_etf=True, skip_tickers=None):
    """★ 광범위 후보풀 → 수급/뉴스 상세 수집 후 AI 프롬프트용 텍스트 반환
    
    v6.0: 상세 데이터 대상을 다양하게 선별 (등락률 일변도 → 3그룹 분산)
      그룹A: 거래량 급증 TOP (모멘텀 초입)
      그룹B: 등락률 +1~5% (상승 초입, 고점 아님)
      그룹C: 등락률 상위 (기존 호환)
    → AI가 다양한 성격의 종목에 5점수를 매길 수 있음
    """
    # ── 후보 풀 수집
    all_candidates = collect_wide_candidates(app_key, app_secret, mode, token, exclude_etf=exclude_etf, skip_tickers=skip_tickers)
    if not all_candidates:
        return ""

    # ── 후보 요약 (전체 목록 - 가격/등락만, AI에게 전체 보여줌)
    result = f"\n[★ 후보 종목 풀 - {len(all_candidates)}개 (거래량/상승률/코스피+코스닥)]\n"
    result += "※ 아래 종목들 중에서 선별할 것\n"
    
    # 등락률 기준 정렬 (목록 표시용)
    def _chg(c):
        try: return float(str(c.get('chg_pct',0)).replace('%','') or 0)
        except: return 0
    sorted_cands = sorted(all_candidates, key=_chg, reverse=True)
    
    # ★★★ v6.0: AI에게 보여줄 후보 목록 (2천~3만원만, 15개 제한) ★★★
    _display_limit = min(15, len(sorted_cands))
    _displayed = 0
    for c in sorted_cands:
        if _displayed >= _display_limit:
            break
        # 가격 확인 — 범위 밖이면 표시 안 함
        try:
            _dp = float(str(c.get('price','0')).replace(',','') or 0)
            if _dp > 0 and _dp > 30000:
                continue
        except: pass
        src_str = '+'.join(set(c.get('sources',[])))
        try:
            chg_f = _chg(c)
            sign = '+' if chg_f >= 0 else ''
        except:
            sign = ''; chg_f = 0
        result += f"  {c['name']}({c['ticker']}) {c['price']}원 {sign}{chg_f:.1f}% [{src_str}]\n"
        _displayed += 1
    if len(sorted_cands) > _displayed:
        result += f"  ... 외 {len(sorted_cands) - _displayed}개\n"

    # ★★★ v6.0: 상세 데이터 대상을 3그룹으로 다양하게 선별 ★★★
    # 기존: 등락률 1등~5등에만 상세 데이터 → 고점 추격매수 위험
    # 변경: 거래량급증/상승초입/등락상위 3그룹에서 골고루 선별
    
    _detail_targets = []  # 상세 데이터 받을 종목 리스트
    _detail_tickers = set()  # 중복 방지
    
    # ★ v6.0: 30000원 초과 제외 헬퍼
    def _price_ok(c):
        try:
            _pp = float(str(c.get('price','0')).replace(',','') or 0)
            if _pp > 30000: return False
        except: pass
        return True
    
    # 그룹A: 거래량 급증 (sources에 '거래량TOP' 포함 + 등락률 양수) — 모멘텀 초입
    _vol_surge = [c for c in sorted_cands 
                  if '거래량TOP' in c.get('sources',[]) and _chg(c) > 0 and _chg(c) < 10 and _price_ok(c)]
    for c in _vol_surge[:3]:
        if c['ticker'] not in _detail_tickers:
            _detail_targets.append(c)
            _detail_tickers.add(c['ticker'])
    
    # 그룹B: 등락률 +1~5% (상승 초입, 고점 아님) — 핵심 타겟
    _sweet_spot = [c for c in sorted_cands if 1.0 <= _chg(c) <= 5.0 and _price_ok(c)]
    for c in _sweet_spot[:3]:
        if c['ticker'] not in _detail_tickers:
            _detail_targets.append(c)
            _detail_tickers.add(c['ticker'])
    
    # 그룹C: 등락률 상위 (기존 호환) — 강한 모멘텀
    for c in sorted_cands[:4]:
        if c['ticker'] not in _detail_tickers and _price_ok(c):
            _detail_targets.append(c)
            _detail_tickers.add(c['ticker'])
    
    # max_stocks 제한 (기본 5 → 호출 시 max(5, max_picks+2)로 이미 7~12)
    detail_count = min(max(max_stocks, 8), len(_detail_targets))
    _detail_targets = _detail_targets[:detail_count]
    
    _group_log = f"거래량급증{min(3,len(_vol_surge))} + 초입{min(3,len(_sweet_spot))} + 상위{detail_count - min(3,len(_vol_surge)) - min(3,len(_sweet_spot))}"
    print(f"[V6_DETAIL] 상세 데이터 {detail_count}종목 선별: {_group_log}")
    
    if detail_count > 0:
        result += f"\n[상세 {detail_count}개 데이터 - 수급/뉴스 (거래량급증+상승초입+상위 혼합)]\n"
    
    detailed = 0
    # ★ 섹터 분산용: ticker→sector 매핑
    sector_map = {}  # {ticker: sector_name}
    for c in _detail_targets:
        if detailed >= detail_count:
            break
        ticker = c['ticker']
        name   = c['name']
        
        # 수급: 외국인/기관 최근 3일 + ★ 섹터(업종) 정보 수집
        try:
            iv = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-investor",
                app_key, app_secret, mode, token, "FHKST03010100",
                params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker})
            rows = iv.get('output', [])[:3]
            if rows:
                fg = sum(int(r.get('frgn_ntby_qty', '0').replace(',', '') or 0) for r in rows)
                og = sum(int(r.get('orgn_ntby_qty', '0').replace(',', '') or 0) for r in rows)
                fg_sign = '🟢' if fg > 0 else '🔴' if fg < 0 else '⬜'
                og_sign = '🟢' if og > 0 else '🔴' if og < 0 else '⬜'
                result += f"\n▶ {name}({ticker}) {c['price']}원 {c.get('chg_pct','')}%\n"
                result += f"  수급(3일): 외국인{fg_sign}{'+' if fg>=0 else ''}{fg:,}주 기관{og_sign}{'+' if og>=0 else ''}{og:,}주\n"
            time.sleep(0.2)
        except Exception:
            result += f"\n▶ {name}({ticker}) {c['price']}원\n"

        # ★ 섹터 정보: KIS inquire-price에서 업종명 추출
        try:
            _sp = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
                app_key, app_secret, mode, token, "FHKST01010100",
                params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker})
            _sector = _sp.get('output', {}).get('bstp_kor_isnm', '').strip()
            _detail_price = int(float(_sp.get('output', {}).get('stck_prpr', '0') or '0'))
            if _sector:
                sector_map[ticker] = _sector
                result += f"  🏷️ 업종: {_sector}\n"
            # ★ v6.0: 실제 가격 업데이트
            if _detail_price > 0:
                c['price'] = str(_detail_price)
            time.sleep(0.15)
        except Exception:
            pass

        # 당일+전날 뉴스
        try:
            news = fetch_naver_news(ticker=ticker, max_days=2)
            if news:
                result += f"  📰 뉴스: {' / '.join(news[:3])}\n"
        except Exception:
            pass

        detailed += 1

    # ★★★ v4.0: 섹터 정보 확대 수집 (상세 5개 외 추가 25개 = 총 30개) ★★★
    # 섹터 분산 강제 차단이 효과적이려면 후보풀 대부분의 섹터를 알아야 함
    _sector_bulk_count = 0
    _sector_bulk_target = 30 - len(sector_map)  # 이미 수집된 것 제외
    for c in sorted_cands:
        if _sector_bulk_count >= _sector_bulk_target:
            break
        ticker = c['ticker']
        if ticker in sector_map:
            continue  # 이미 수집됨
        try:
            _sp2 = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
                app_key, app_secret, mode, token, "FHKST01010100",
                params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker})
            _sec2 = _sp2.get('output', {}).get('bstp_kor_isnm', '').strip()
            _sec2_price = int(float(_sp2.get('output', {}).get('stck_prpr', '0') or '0'))
            if _sec2:
                sector_map[ticker] = _sec2
                _sector_bulk_count += 1
            # ★ v6.0: 현재가 저장 (후보풀 가격 필터용)
            if _sec2_price > 0:
                # 후보 데이터에 실제 가격 업데이트
                for _c_upd in all_candidates:
                    if _c_upd['ticker'] == ticker:
                        _c_upd['price'] = str(_sec2_price)
                        break
            time.sleep(0.08)  # 0.08초 × 25 = 2초 (빠르게)
        except:
            pass
    if _sector_bulk_count > 0:
        print(f"[SECTOR_BULK] 추가 {_sector_bulk_count}종목 섹터 수집 완료 (총 {len(sector_map)}종목)")

    # ★ valid_tickers: 후보풀에서 검증된 실제 코드 세트 (AI 응답 검증용)
    # ★★★ v6.0: 가격대 필터 — 2천~3만원 범위 밖 종목은 후보에서 완전 제거 ★★★
    _price_cache = {}  # ticker → actual price (from sector bulk query)
    for c in all_candidates:
        tk = c['ticker']
        # 후보풀 가격 + 섹터 벌크 조회 가격 활용
        _cp = 0
        try:
            _cp = int(float(str(c.get('price','0')).replace(',','') or 0))
        except: pass
        # 섹터 벌크에서 가져온 실시간 가격이 있으면 우선 사용
        if tk in sector_map:
            try:
                # 이미 조회된 가격이 있으면 사용
                _cp2 = _price_cache.get(tk, 0)
                if _cp2 > 0:
                    _cp = _cp2
            except: pass
        _price_cache[tk] = _cp
    
    _filtered = {}
    for c in all_candidates:
        tk = c['ticker']
        _cp = _price_cache.get(tk, 0)
        # 가격 알 때: 30000 초과만 제거
        if _cp > 30000:
            continue
        _filtered[tk] = c['name']
    _removed = len(all_candidates) - len(_filtered)
    if _removed > 0:
        print(f"[PRICE_FILTER] 후보풀에서 {_removed}종목 가격대 제외 (3만원 초과)")
    
    valid_tickers = set(_filtered.keys())
    valid_candidates = _filtered
    # ★ sector_map 반환 추가
    return result, valid_tickers, valid_candidates, sector_map

def collect_single_stock(cfg, token, app_key, app_secret, mode, ticker):
    """단일 종목 풀 데이터 수집 - 단타 매수 직전 검증용.
    수급/재무/뉴스 포함. 리턴: (detail_str, raw_data_dict)
    raw_data_dict를 verify에 재사용해 이중 API 호출 방지.
    """
    result = ""
    raw = {}  # verify_before_buy에 재사용할 원시 데이터
    try:
        pd_data = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
            app_key, app_secret, mode, token, "FHKST01010100",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker})
        o = pd_data.get('output', {})
        name = o.get('hts_kor_isnm', ticker)
        cur_price = float(o.get('stck_prpr', '0') or '0')
        chg_pct = float(o.get('prdy_ctrt', '0') or '0')
        acml_vol  = int(o.get('acml_vol', '0').replace(',', '') or 0)
        avrg_vol  = int(o.get('avrg_vol_cnt', '0').replace(',', '') or 0)  # 평균거래량
        vol_ratio = (acml_vol / avrg_vol) if avrg_vol > 0 else 0.0  # ★ FIX: 0.0=데이터없음 (1.0은 1배를 의미해버림)
        raw.update({'name': name, 'cur_price': cur_price, 'chg_pct': chg_pct,
                    'per': o.get('per', ''), 'frgn_ehrt': o.get('hts_frgn_ehrt', ''),
                    'acml_vol': acml_vol, 'avrg_vol': avrg_vol, 'vol_ratio': vol_ratio,
                    'open_price': float(o.get('stck_oprc','0') or 0),
                    'high_price': float(o.get('stck_hgpr','0') or 0)})
        result += f"종목: {name}({ticker})\n"
        result += f"현재가: {o.get('stck_prpr','?')}원 ({'+' if chg_pct >= 0 else ''}{chg_pct}%)\n"
        result += f"시가: {o.get('stck_oprc','?')} 고가: {o.get('stck_hgpr','?')} 저가: {o.get('stck_lwpr','?')}\n"
        result += f"거래량: {acml_vol:,} (평균대비 {vol_ratio:.1f}배) / 외국인보유: {o.get('hts_frgn_ehrt','?')}%\n"
        result += f"PER: {o.get('per','?')} / PBR: {o.get('pbr','?')}\n"
        time.sleep(0.2)
    except Exception as e:
        result += f"KIS 시세 실패: {e}\n"

    # 수급 3일
    try:
        iv = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-investor",
            app_key, app_secret, mode, token, "FHKST03010100",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker})
        rows = iv.get('output', [])[:3]
        if rows:
            fg = sum(int(r.get('frgn_ntby_qty', '0').replace(',', '') or 0) for r in rows)
            og = sum(int(r.get('orgn_ntby_qty', '0').replace(',', '') or 0) for r in rows)
            raw.update({'frgn_3d': fg, 'orgn_3d': og})
            fg_sign = '🟢' if fg > 0 else '🔴'
            og_sign = '🟢' if og > 0 else '🔴'
            result += f"수급(3일): 외국인{fg_sign}{fg:+,}주 기관{og_sign}{og:+,}주\n"
        time.sleep(0.2)
    except Exception:
        pass

    # 뉴스: 당일+전날 2일
    try:
        news = fetch_naver_news(ticker=ticker, max_days=2)
        raw['news'] = news
        if news:
            result += f"📰 뉴스(2일):\n" + '\n'.join(f"  - {n}" for n in news[:5]) + "\n"
    except Exception:
        pass

    return result, raw

def verify_before_buy(name, ticker, raw_data):
    """단타 매수 직전 즉각 차단 필터 (지능형 모멘텀 판단)
    
    핵심 원칙:
    - vol_ratio=0.0 → 평균거래량 데이터 없음 → 거래량 기준 차단 면제
    - +20% 이상 → 상한가 근처 → 무조건 차단
    - +10~15% → 거래량 3배+ AND 수급 양수 → 허용 / 데이터없으면 수급만 봄
    - +5~10%  → 거래량 2배+ → 허용 / 데이터없으면 통과
    - 외국인+기관 동시 순매도 → 차단 (거래량폭발+급등이면 면제)
    """
    chg_pct   = float(raw_data.get('chg_pct', 0) or 0)
    frgn_3d   = int(raw_data.get('frgn_3d', 0) or 0)
    orgn_3d   = int(raw_data.get('orgn_3d', 0) or 0)
    per_str   = raw_data.get('per', '')
    news_list = raw_data.get('news', [])
    vol_ratio = float(raw_data.get('vol_ratio', 0.0) or 0.0)
    # ★ vol_ratio=0.0 → 평균거래량 데이터 없음 (API 미지원/모의투자)
    vol_unknown = (vol_ratio == 0.0)

    # ─── 1. 급등 판단 ─────────────────────────────────────────────────
    if chg_pct >= 15:
        return False, f"🚫 상한가 근처 +{chg_pct:.1f}% - 추격 불가"

    elif chg_pct >= 10:
        if vol_unknown:
            # 평균거래량 데이터 없음 → 수급으로만 판단
            if frgn_3d < 0 and orgn_3d < 0:
                return False, f"⛔ +{chg_pct:.1f}% 급등 + 외국인·기관 동시 순매도 - 차단"
            # 수급 중립/양수면 통과 (vol 데이터 없어서 차단하지 않음)
        else:
            if vol_ratio >= 3.0 and (frgn_3d > 0 or orgn_3d > 0):
                pass  # 거래량폭발+수급 → 모멘텀 초입 허용
            elif vol_ratio >= 2.0:
                pass  # 거래량 2배+ 이면 허용
            else:
                return False, (f"⛔ +{chg_pct:.1f}% 급등, 거래량 {vol_ratio:.1f}배 "
                               f"(수급 뒷받침 부족) - 고점 추격 차단")

    elif chg_pct >= 5:
        if vol_unknown:
            pass  # 데이터 없으면 통과 (+5~10%는 관대하게)
        else:
            if vol_ratio >= 1.5:
                pass  # 거래량 1.5배 이상이면 허용 (기존 2.0→1.5 완화)
            else:
                return False, (f"⛔ +{chg_pct:.1f}% 상승, 거래량 {vol_ratio:.1f}배 "
                               f"(거래량 뒷받침 없는 급등) - 차단")

    # ─── 2. 쌍방 순매도 차단 ─────────────────────────────────────────
    if frgn_3d < 0 and orgn_3d < 0:
        if not vol_unknown and vol_ratio >= 3.0 and chg_pct >= 3:
            pass  # 거래량 폭발 모멘텀 → 수급 데이터 후행성 감안, 통과
        elif vol_unknown and chg_pct >= 3:
            pass  # 거래량 불명 + 등락 양수 → 통과
        else:
            return False, f"🔴 외국인+기관 3일 순매도 ({frgn_3d:+,}/{orgn_3d:+,}) - 수급 차단"

    # ─── 3. 악재 뉴스 차단 ───────────────────────────────────────────
    BAD_KEYWORDS = ['상장폐지', '관리종목', '횡령', '배임', '검찰', '영업정지',
                    '파산', '부도', '감자', '대규모 적자', '주가 조작']
    news_text = ' '.join(str(n) for n in news_list)
    for kw in BAD_KEYWORDS:
        if kw in news_text:
            return False, f"📰 악재 뉴스: '{kw}' - 차단"

    # ─── 4. 극단 고평가 차단 (모멘텀 종목은 PER 면제) ─────────────
    try:
        per_val = float(str(per_str).replace(',', '') or '0')
        if per_val > 200 and (vol_unknown or vol_ratio < 2.0):
            return False, f"PER {per_val:.0f} > 200 (거래량 뒷받침 없음) - 고평가 차단"
    except Exception:
        pass

    # ─── 통과 ─────────────────────────────────────────────────────────
    vol_str = f"거래량 {vol_ratio:.1f}배" if not vol_unknown else "거래량데이터없음"
    if chg_pct >= 5:
        return True, f"✅ 통과 (모멘텀 +{chg_pct:.1f}%, {vol_str} - 초입 판단)"
    return True, f"✅ 통과 ({vol_str})"

class AutoTrader:
    def __init__(self):
        self.running = False
        self.paused = False       # ★ 일시정지 상태 추가
        self.ai_sell_mode = False  # ★★★ v5.0: AI 매도 모드 (False=수동/기계적, True=AI판단)
        self.thread = None        # scan/브리핑 스레드
        self.rules_thread = None  # 고빈도 익절/손절 전용 스레드
        self._scan_running = False  # AI 스캔 중 여부 (중복 방지)
        self._slots_full = False    # ★ v8.0: 슬롯 풀 플래그 초기화
        self._sell_locks = {}     # ★ 종목별 매도 락 (중복 매도 방지)
        self.config = {}          # ★ 항상 초기화

    def start(self, config):
        if self.running:
            return
        self.running = True
        self.paused = False      # ★ 시작 시 일시정지 해제
        self.ai_sell_mode = False  # ★ v6.0: AI매도 삭제 → 항상 기계적 매도
        self._scan_running = False  # ★ 근본 FIX: 이전 세션 잔존 플래그 리셋
        self._scan_lock = threading.Lock()  # ★ 락도 새로 생성
        self._start_timestamp = time.time()  # ★ 시작 시간 (grace period용)
        self.config = config
        
        # ★★★ v8.0: swing_tickers → auto_tickers 자동 이관 (중장기 탭 삭제) ★★★
        _migrated = []
        for _stk in list(swing_tickers):
            if _stk not in auto_tickers:
                auto_tickers.append(_stk)
                _migrated.append(_stk)
        if _migrated:
            print(f"[V8] ★ swing→auto 이관: {len(_migrated)}종목 → 단타 매도엔진으로 관리")
            for _mtk in _migrated:
                _nm = ensure_name(_mtk, '')
                print(f"  → {_nm}({_mtk}) auto_tickers 등록")
            trade_log.append({
                "time": datetime.now().isoformat(),
                "date": datetime.now().strftime('%Y-%m-%d'),
                "type": "SYSTEM",
                "message": f"[V8] swing→auto 이관: {len(_migrated)}종목 (중장기 탭 삭제 → 단타 통합관리)"
            })
            swing_tickers.clear()  # swing 리스트 비움
            swing_buy_routes.clear()  # ★ v8.0: swing 경로도 비움 (슬롯 카운트 정상화)
            save_state()
        
        # ★★★ v8.0: daily_briefing 날짜 체크 (마감 리포트용) ★★★
        _today_s = datetime.now().strftime('%Y-%m-%d')
        if not daily_briefing.get('date') or daily_briefing['date'] != _today_s:
            daily_briefing.clear()
            daily_briefing['date'] = _today_s
            # ★★★ v8.0: 새 날 → 전날 매도 상태 초기화 (전날 보유종목도 새로 시작) ★★★
            _sell_stage.clear()
            _tp1_triggered.clear()
            peak_prices.clear()
            _dip_flag.clear()
            auto_avg_count.clear()
            swing_sell_stage.clear()
            swing_dip_flag.clear()
            # swing_avg_count는 유지 (중장기=며칠 보유 → 물타기 횟수 누적)
            print(f"[START] 📅 새 날 초기화: 단타+중장기 sell_stage + peak + avg_count ({_today_s})")
        
        # ★★★ F3: 재시작 시 DAILY_STOP 무효화 → 일손익 체크 활성화 ★★★
        _today_s = datetime.now().strftime('%Y-%m-%d')
        _ds_cleared = 0
        for t in trade_log:
            if t.get('date') == _today_s and t.get('type') == 'DAILY_STOP':
                t['type'] = 'DAILY_STOP_CLEARED'
                t['message'] = t.get('message', '') + ' [재시작으로 무효화됨]'
                _ds_cleared += 1
        if _ds_cleared:
            print(f"[START] DAILY_STOP {_ds_cleared}건 무효화 → 일손익 체크 재활성화")
        
        # ★ v3.0: 기존 보유종목 자동 인수인계
        # 계좌에 이미 보유 중인 종목을 auto_tickers에 등록 → AI 매도/규칙 체크 대상
        try:
            _ak = config.get('app_key','')
            _as = config.get('app_secret','')
            _acct = config.get('account','')
            if _ak and _as and _acct:
                # ★ 토큰 발급 (1분당 1회 제한 → 실패 시 대기 후 재시도)
                _tok = None
                for _retry in range(3):
                    try:
                        _tok = kis_get_token(_ak, _as, 'live')
                        if _tok:
                            break
                    except Exception as _te:
                        print(f"[START] 토큰 발급 실패 ({_retry+1}/3): {_te}")
                        if _retry < 2:
                            print(f"[START] 60초 대기 후 재시도...")
                            time.sleep(60)  # KIS 1분당 1회 제한 대기
                
                if not _tok:
                    print("[START] ⚠️ 토큰 발급 실패 → 인수인계 건너뜀 (첫 스캔에서 자동 처리)")
                    _bal = {}
                else:
                    _bal = get_balance(_ak, _as, 'live', _tok, _acct, config.get('account_cd','01'))
                
                _existing = []
                # ★ v8.0: 모든 보유종목 auto_tickers 등록
                
                for _p in _bal.get('output1', []):
                    _tk = _p.get('pdno','')
                    _qty = int(_p.get('hldg_qty','0') or 0)
                    if _qty > 0 and _tk and _tk not in auto_tickers:
                        auto_tickers.append(_tk)
                        _nm = _p.get('prdt_name', _tk)
                        _avg = float(_p.get('pchs_avg_pric','0') or 0)
                        _cur = float(_p.get('prpr','0') or 0)
                        _pnl = round((_cur - _avg) / _avg * 100, 1) if _avg > 0 else 0
                        _existing.append(f"{_nm}({_tk}) {_qty}주 {'+'if _pnl>=0 else ''}{_pnl}%")
                        if _tk not in peak_prices:
                            peak_prices[_tk] = _cur
                        
                        # ★ v8.0: 거래정지 여부 체크 — 장 시작 후(09:05~)에만 (장 전에는 모든 종목이 시가=0)
                        _hm_chk = datetime.now().hour * 100 + datetime.now().minute
                        if _hm_chk >= 905:
                            try:
                                _chk = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
                                    _ak, _as, 'live', _tok, "FHKST01010100",
                                    params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": _tk})
                                _chk_out = _chk.get('output', {})
                                _chk_vol = int(_chk_out.get('acml_vol', '0') or 0)
                                _chk_oprc = int(_chk_out.get('stck_oprc', '0') or 0)
                                if _chk_oprc == 0 and _chk_vol == 0:
                                    perm_blocked[_tk] = f"HALTED: 거래정지 (시가=0, 거래량=0)"
                                    print(f"[START] ⛔ {_nm}({_tk}) 거래정지 확인 → perm_blocked 등록")
                            except Exception as _chk_e:
                                print(f"[START] 종목상태 체크 실패 ({_tk}): {_chk_e}")
                        
                        # ★ v3.0 근본 FIX: 가상 매수 로그 생성 (매도 시 손익 계산 가능하게!)
                        # 기존 보유종목은 매수 로그가 없어서 매도해도 pnl=0이 됨
                        # 가상 BUY 로그를 넣으면 _execute_sell에서 avg_price 역추적 가능
                        _has_buy_log = any(t.get('ticker') == _tk and t.get('type') in ('AI_BUY','BUY','SYSTEM_BUY') 
                                          and t.get('success') for t in trade_log)
                        if not _has_buy_log and _avg > 0:
                            trade_log.append({
                                "time": datetime.now().isoformat(),
                                "date": datetime.now().strftime('%Y-%m-%d'),
                                "type": "AI_BUY", "ticker": _tk, "name": _nm,
                                "qty": _qty, "price": _avg,
                                "success": True,
                                "message": f"📦 기존 보유종목 인수인계 (평단₩{_avg:,.0f}×{_qty}주)"
                            })
                            print(f"  📦 {_nm}({_tk}) 가상매수로그 생성: ₩{_avg:,.0f}×{_qty}주")
                
                if _existing:
                    _msg = f"📦 기존 보유종목 {len(_existing)}종목 인수인계 → AI 매도/손절 관리 시작"
                    print(f"[START] {_msg}")
                    for _e in _existing:
                        print(f"  → {_e}")
                    
                    # ★ v3.0: 기존 보유종목 평가손익을 session_baseline에 설정
                    # → "오늘 이 시스템이 만든 손익"만 일일 한도에 적용
                    # → 기존 -9% 손실 때문에 시작하자마자 손실한도 걸리는 문제 방지
                    try:
                        _today_s = datetime.now().strftime('%Y-%m-%d')
                        _existing_pnl, _, _ = calc_auto_pnl_today(_bal.get('output1', []), _today_s)
                        config['session_baseline'] = str(_existing_pnl)
                        print(f"[START] 세션 baseline 설정: ₩{_existing_pnl:,.0f} (기존 평가손익 제외)")
                    except Exception as _be:
                        print(f"[START] baseline 설정 실패: {_be}")
                    
                    trade_log.append({
                        "time": datetime.now().isoformat(),
                        "date": datetime.now().strftime('%Y-%m-%d'),
                        "type": "SYSTEM",
                        "message": f"{_msg}\n" + '\n'.join(_existing)
                    })
                    save_state()
                    tg_send(f"📦 <b>기존 보유종목 인수인계</b>\n{len(_existing)}종목 → AI 매도/손절 관리 시작\n" + '\n'.join(f"  {e}" for e in _existing[:10]))
                else:
                    # state에서 이미 로드된 경우 (auto_tickers에 이미 있음)
                    if auto_tickers:
                        print(f"[START] state에서 {len(auto_tickers)}종목 이미 로드됨 → 보유현황 전송")
                        # 잔고에서 현황 조회해서 텔레그램 알림
                        _status_lines = []
                        for _p in _bal.get('output1', []):
                            _tk2 = _p.get('pdno','')
                            _qty2 = int(_p.get('hldg_qty','0') or 0)
                            if _qty2 > 0 and _tk2 in auto_tickers:
                                _nm2 = _p.get('prdt_name', _tk2)
                                _avg2 = float(_p.get('pchs_avg_pric','0') or 0)
                                _cur2 = float(_p.get('prpr','0') or 0)
                                _pnl2 = round((_cur2 - _avg2) / _avg2 * 100, 1) if _avg2 > 0 else 0
                                _emoji2 = '🟢' if _pnl2 >= 0 else '🔴'
                                _status_lines.append(f"{_emoji2} {_nm2} {_qty2}주 {'+'if _pnl2>=0 else ''}{_pnl2}%")
                                
                                # ★ v3.0 근본 FIX: 매수 로그 없으면 가상 생성
                                _has_buy2 = any(t.get('ticker') == _tk2 and t.get('type') in ('AI_BUY','BUY')
                                               and t.get('success') for t in trade_log)
                                if not _has_buy2 and _avg2 > 0:
                                    trade_log.append({
                                        "time": datetime.now().isoformat(),
                                        "date": datetime.now().strftime('%Y-%m-%d'),
                                        "type": "AI_BUY", "ticker": _tk2, "name": _nm2,
                                        "qty": _qty2, "price": _avg2,
                                        "success": True,
                                        "message": f"📦 기존 보유종목 (평단₩{_avg2:,.0f}×{_qty2}주)"
                                    })
                                    print(f"  📦 {_nm2}({_tk2}) 가상매수로그 생성")
                        
                        if _status_lines:
                            # session_baseline 설정
                            try:
                                _today_s = datetime.now().strftime('%Y-%m-%d')
                                _existing_pnl, _, _ = calc_auto_pnl_today(_bal.get('output1', []), _today_s)
                                config['session_baseline'] = str(_existing_pnl)
                                print(f"[START] 세션 baseline 설정: ₩{_existing_pnl:,.0f}")
                            except: pass
                            
                            tg_send(f"🤖 <b>자동매매 시작</b>\n📦 보유 {len(_status_lines)}종목 관리 시작\n" + '\n'.join(f"  {s}" for s in _status_lines[:10]))
                            trade_log.append({
                                "time": datetime.now().isoformat(),
                                "date": datetime.now().strftime('%Y-%m-%d'),
                                "type": "SYSTEM",
                                "message": f"📦 기존 {len(_status_lines)}종목 관리 시작 (state 로드)\n" + '\n'.join(_status_lines)
                            })
                            save_state()
                        else:
                            print("[START] auto_tickers 있지만 실제 보유수량 0 → 빈 계좌")
                    else:
                        print("[START] 보유종목 없음 — 빈 계좌로 시작")
        except Exception as _e:
            print(f"[START] 기존 보유종목 인수인계 실패: {_e} (정상 시작 계속)")
        
        # ★★★ v6.0: 물타기 카운트는 state.json에서 복원됨 ★★★
        # 보유 안 한 종목만 정리 (옛 찌꺼기 제거)
        _held_now = set(auto_tickers)  # v8: swing removed
        _stale = [k for k in auto_avg_count if k not in _held_now]
        for k in _stale:
            auto_avg_count.pop(k, None)
        if _stale:
            print(f"[START] auto_avg_count 정리: {len(_stale)}개 비보유 종목 제거")
        # ★ v6.0: 카운트 0인 보유종목 → trade_log에서 복원
        _today = datetime.now().strftime('%Y-%m-%d')
        for _tk in _held_now:
            if auto_avg_count.get(_tk, 0) == 0:
                _avg_cnt = sum(1 for t in trade_log
                    if t.get('ticker') == _tk and t.get('type') == 'AVG_DOWN'
                    and t.get('success') and t.get('date', '') == _today)
                if _avg_cnt > 0:
                    auto_avg_count[_tk] = _avg_cnt
                    print(f"[START] 물타기 복원: {_tk} → {_avg_cnt}회 (trade_log)")
        
        # ★★★ v8.0: 모든 보유종목 거래정지 체크 — 장 시작 후(09:05~)에만 ★★★
        _hm_start = datetime.now().hour * 100 + datetime.now().minute
        if _hm_start >= 905:
            try:
                _tok2 = kis_get_token(config.get('app_key',''), config.get('app_secret',''), 'live') if config.get('app_key') else None
                if _tok2 and auto_tickers:
                    for _stk in list(auto_tickers):
                        if _stk in perm_blocked:
                            continue
                        try:
                            _schk = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
                                config['app_key'], config['app_secret'], 'live', _tok2, "FHKST01010100",
                                params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": _stk})
                            _so2 = _schk.get('output', {})
                            _s_oprc = int(_so2.get('stck_oprc', '0') or 0)
                            _s_vol = int(_so2.get('acml_vol', '0') or 0)
                            if _s_oprc == 0 and _s_vol == 0:
                                perm_blocked[_stk] = f"HALTED: 거래정지 (시가=0, 거래량=0)"
                                print(f"[START] ⛔ {_stk} 거래정지 → perm_blocked 등록")
                        except:
                            pass
                    save_state()
            except Exception as _pe:
                print(f"[START] 거래정지 체크 실패: {_pe}")
        else:
            print(f"[START] 장 시작 전({_hm_start}) → 거래정지 체크 건너뜀 (09:05 이후 자동 체크)")
        
        # ★ v3.0 근본 FIX: 기존 매도 로그 중 pnl=0 (평단가 없음) 건 자동 보정
        # 가상 매수 로그 또는 같은 종목 매수 기록에서 평단가를 찾아서 재계산
        try:
            _fixed = 0
            for t in trade_log:
                if t.get('type') not in ('SELL', 'FORCE_CLOSE'):
                    continue
                if not t.get('success'):
                    continue
                _t_avg = float(t.get('avg_price', 0) or 0)
                _t_pnl = float(t.get('pnl', 0) or 0)
                _t_price = float(t.get('price', 0) or 0)
                _t_qty = int(t.get('qty', 0) or 0)
                _t_ticker = t.get('ticker', '')
                
                # avg_price=0이고 pnl=0인 매도 건 → 보정 대상
                if _t_avg <= 0 and _t_pnl == 0 and _t_price > 0 and _t_ticker:
                    # 같은 종목 매수 로그에서 평단가 역추적
                    for b in reversed(trade_log):
                        if (b.get('ticker') == _t_ticker and 
                            b.get('type') in ('AI_BUY', 'BUY') and 
                            b.get('success') and
                            float(b.get('price', 0) or 0) > 0):
                            _b_price = float(b.get('price', 0))
                            _new_pnl = round((_t_price - _b_price) * _t_qty)
                            _new_pct = round((_t_price - _b_price) / _b_price * 100, 2) if _b_price > 0 else 0
                            t['avg_price'] = _b_price
                            t['pnl'] = _new_pnl
                            t['pnl_pct'] = _new_pct
                            _fixed += 1
                            break
            if _fixed > 0:
                print(f"[START] ★ 매도 로그 {_fixed}건 손익 보정 완료")
                save_state()
        except Exception as _fe:
            print(f"[START] 매도 로그 보정 실패: {_fe}")
        
        # ★ 스레드 1: 고빈도 익절/손절 전용 (2초 간격)
        self.rules_thread = threading.Thread(target=self._rules_loop, daemon=True)
        self.rules_thread.start()
        # ★ 스레드 2: AI 스캔 + 브리핑 (느린 작업 전담)
        self.thread = threading.Thread(target=self._scan_loop, daemon=True)
        self.thread.start()
        # ★ 스레드 3: 시작 직후 보유종목 이름 KIS 교정 (토큰 캐시 대기)
        def _fix_names():
            try:
                time.sleep(30)  # 토큰 캐시 확보 후 실행 (1분 제한 회피)
                _tk = kis_get_token(config.get('app_key',''), config.get('app_secret',''), 'live')
                pass  # 이름교정은 auto/fix_names API로 처리
            except Exception as _e:
                print(f"[FIX] 이름교정 실패: {_e}")
        threading.Thread(target=_fix_names, daemon=True).start()
        
        # ★ v4.0 PHASE 2: KIS 실시간 WebSocket 시세 시작
        _start_hm = datetime.now().hour * 100 + datetime.now().minute
        _is_hol_ws, _ = is_market_holiday()
        if not _is_hol_ws and 830 <= _start_hm <= 1540 and auto_tickers:
            try:
                _ws_tickers = list(set(auto_tickers))[:20]  # v8: auto only
                start_kis_ws_thread(config.get('app_key',''), config.get('app_secret',''), _ws_tickers)
                trade_log.append({
                    "time": datetime.now().isoformat(),
                    "date": datetime.now().strftime('%Y-%m-%d'),
                    "type": "SYSTEM",
                    "message": f"🔌 v4.0 KIS 실시간 WebSocket 시작 ({len(_ws_tickers)}종목)"
                })
                save_state()
            except Exception as _wse:
                print(f"[KIS_WS] 시작 실패 (REST 폴링 모드 유지): {_wse}")
        
        # ★ v3.0: 장외시간(15:25~16:20)에 시작하면 즉시 마감 브리핑 실행 (평일만)
        _start_hm = datetime.now().hour * 100 + datetime.now().minute
        _is_hol_s, _ = is_market_holiday()
        if not _is_hol_s and 1530 <= _start_hm <= 1630 and not daily_briefing.get('closing_done'):
            print("[CLOSING] ★ 장외시간 시작 → 즉시 마감 브리핑 실행!")
            threading.Thread(target=self._run_closing_briefing, daemon=True).start()
        
        # ★★★ v8.0: 글로벌 브리핑 삭제 — 단타/중장기 모두 자체 실시간 데이터 사용 ★★★

    def stop(self):
        self.running = False
        self.paused = False      # ★ 중지 시 일시정지도 해제
    
    def pause(self):
        """일시정지 - 익절/손절은 계속, 신규 매수만 중단"""
        self.paused = True
        trade_log.append({
            "time": datetime.now().isoformat(),
            "type": "SYSTEM",
            "message": "⏸️ 자동매매 일시정지 (익절/손절 유지, 신규 매수 중단)"
        })
        save_state()
    
    def resume(self):
        """일시정지 해제 + DAILY_STOP 무효화 + 새 session_baseline 설정"""
        self.paused = False
        
        # ★★★ F2: DAILY_STOP 무효화 → 일손익 체크 다시 활성화 ★★★
        today = datetime.now().strftime('%Y-%m-%d')
        _cleared = 0
        for t in trade_log:
            if t.get('date') == today and t.get('type') == 'DAILY_STOP':
                t['type'] = 'DAILY_STOP_CLEARED'  # 타입 변경으로 무효화
                t['message'] = t.get('message', '') + ' [재개로 무효화됨]'
                _cleared += 1
        
        # ★ 새 session_baseline 설정 (현재까지 PnL을 기준점으로)
        try:
            cfg = self.config
            if cfg.get('app_key'):
                _token = kis_get_token(cfg['app_key'], cfg['app_secret'], 'live')
                _bal = get_balance(cfg['app_key'], cfg['app_secret'], 'live', 
                                   _token, cfg['account'], cfg.get('account_cd', '01'), max_age=0)
                _cur_pnl, _, _ = calc_auto_pnl_today(_bal.get('output1', []), today)
                cfg['session_baseline'] = str(_cur_pnl)
                print(f"[RESUME] 새 session_baseline: ₩{_cur_pnl:,.0f} (현재 PnL 기준)")
        except Exception as _e:
            print(f"[RESUME] baseline 설정 실패: {_e}")
        
        trade_log.append({
            "time": datetime.now().isoformat(), "date": today,
            "type": "SYSTEM", 
            "message": f"▶️ 자동매매 재개 (DAILY_STOP {_cleared}건 무효화, 새 baseline 설정)"
        })
        save_state()
        print(f"[RESUME] ▶️ 재개 완료 | DAILY_STOP {_cleared}건 클리어 | 일손익 체크 재활성화")

    def _price_push_loop(self):
        """★★★ v8.0: 실시간 가격 push 독립 스레드 — _check_rules 블로킹과 완전 분리 ★★★
        2초마다 WS 실시간 가격으로 보유종목 정보 push → HTML 즉시 갱신
        """
        if not hasattr(self, '_price_detail_cache'):
            self._price_detail_cache = {}
            self._price_detail_ts = 0
            self._market_cache = {}
            self._market_cache_ts = 0
            self._market_history = []
            _today_s = datetime.now().strftime('%Y-%m-%d')
            try:
                with open(f'market_history_{_today_s}.json', 'r') as _hf:
                    self._market_history = json.load(_hf)
                print(f"[CHART] 파일에서 {len(self._market_history)}개 복원")
            except:
                try: self._market_history = fetch_market_day_chart()
                except: pass
        
        while self.running:  # v8: swing removed
            try:
                _hm = datetime.now().hour * 100 + datetime.now().minute
                if _hm > 1535 or _hm < 840:
                    time.sleep(30)
                    continue
                
                if not _bal_cache['data'] or not auto_tickers:  # v8: swing removed
                    time.sleep(3)
                    continue
                
                # ★★★ v8.0: _bal_cache 너무 오래되면 직접 갱신 (KIS WS 끊겼을 때 가격 정체 방지) ★★★
                if _bal_cache['data'] and (time.time() - _bal_cache['ts']) > 8:
                    try:
                        _pp_cfg = self.config
                        _pp_tok = kis_get_token(_pp_cfg.get('app_key',''), _pp_cfg.get('app_secret',''), 'live')
                        if _pp_tok:
                            get_balance(_pp_cfg.get('app_key',''), _pp_cfg.get('app_secret',''), 'live', _pp_tok,
                                       _pp_cfg.get('account',''), _pp_cfg.get('account_cd','01'), max_age=5)
                    except:
                        pass
                
                _all_tracked = set(auto_tickers)  # v8: swing removed
                _pos_list = []
                for _p in _bal_cache['data'].get('output1', []):
                    _tk = _p.get('pdno', '')
                    if _tk not in _all_tracked:
                        continue
                    _qty = int(_p.get('hldg_qty', '0') or 0)
                    if _qty <= 0:
                        continue
                    _ws_price = get_realtime_price(_tk)
                    _cur = _ws_price if _ws_price > 0 else float(_p.get('prpr', '0') or 0)
                    _avg = float(_p.get('pchs_avg_pric', '0') or 0)
                    _pnl_pct = round((_cur - _avg) / _avg * 100, 2) if _avg > 0 else 0
                    _pnl_amt = round((_cur - _avg) * _qty)
                    _pd = self._price_detail_cache.get(_tk, {})
                    _pos_list.append({
                        'ticker': _tk, 'name': _p.get('prdt_name', _tk),
                        'qty': _qty, 'avg_price': round(_avg), 'cur_price': round(_cur),
                        'pnl_pct': _pnl_pct, 'pnl_amt': _pnl_amt,
                        'peak': peak_prices.get(_tk, round(_cur)),
                        'day_high': _pd.get('high', 0), 'day_low': _pd.get('low', 0),
                        'day_open': _pd.get('open', 0),
                        'market_name': _pd.get('market', ''), 'sector': _pd.get('sector', '')
                    })
                
                if _pos_list:
                    _out2 = (_bal_cache['data'].get('output2', [{}]) or [{}])[0] or {}
                    
                    # 증시 데이터 (3분 캐시)
                    if time.time() - self._market_cache_ts > 180:
                        self._market_cache_ts = time.time()
                        try:
                            _nv = fetch_naver_market_data()
                            if _nv.get('kospi'):
                                self._market_cache = {
                                    'kospi': _nv.get('kospi',''), 'kospi_chg': _nv.get('kospi_change',''),
                                    'kosdaq': _nv.get('kosdaq',''), 'kosdaq_chg': _nv.get('kosdaq_change','')
                                }
                                _kv = float(str(_nv.get('kospi','')).replace(',',''))
                                _dv = float(str(_nv.get('kosdaq','')).replace(',',''))
                                _now_hm = datetime.now().strftime('%H:%M')
                                if _now_hm not in {h['t'] for h in self._market_history}:
                                    self._market_history.append({'t': _now_hm, 'k': round(_kv,2), 'd': round(_dv,2)})
                                if len(self._market_history) > 100:
                                    self._market_history = self._market_history[-100:]
                                try:
                                    with open(f'market_history_{datetime.now().strftime("%Y-%m-%d")}.json','w') as _hf2:
                                        json.dump(self._market_history, _hf2)
                                except: pass
                        except: pass
                    
                    # 고저가 (2분 캐시, 별도 스레드)
                    if time.time() - self._price_detail_ts > 120:
                        self._price_detail_ts = time.time()
                        def _fetch_pd(_pl, _cfg):
                            try:
                                _ptk = kis_get_token(_cfg.get('app_key',''), _cfg.get('app_secret',''), 'live')
                                for _pp in _pl[:10]:
                                    try:
                                        _pd = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
                                            _cfg.get('app_key',''), _cfg.get('app_secret',''), 'live', _ptk, "FHKST01010100",
                                            params={"FID_COND_MRKT_DIV_CODE":"J","FID_INPUT_ISCD":_pp['ticker']})
                                        _po = _pd.get('output',{})
                                        _h = int(_po.get('stck_hgpr','0') or 0)
                                        _l = int(_po.get('stck_lwpr','0') or 0)
                                        if _h > 0 and _l > 0:
                                            self._price_detail_cache[_pp['ticker']] = {
                                                'high': _h, 'low': _l, 'open': int(_po.get('stck_oprc','0') or 0),
                                                'market': _po.get('rprs_mrkt_kor_name', ''), 'sector': _po.get('bstp_kor_isnm', '')
                                            }
                                        time.sleep(0.15)
                                    except: pass
                            except: pass
                        threading.Thread(target=_fetch_pd, args=([p.copy() for p in _pos_list], self.config.copy()), daemon=True).start()
                    
                    sync_broadcast('positions', {
                        'positions': _pos_list,
                        'total_eval': int(_out2.get('tot_evlu_amt', '0') or 0),
                        'cash': calc_ord_psbl_cash(_bal_cache['data']),
                        'ord_psbl_cash': calc_ord_psbl_cash(_bal_cache['data']),
                        'dnca_tot_amt': int(_out2.get('dnca_tot_amt', '0') or 0),
                        'total_pnl': int(_out2.get('evlu_pfls_smtl_amt', '0') or 0),
                        'market': self._market_cache,
                        'market_history': self._market_history,
                        'time': datetime.now().strftime('%H:%M:%S')
                    })
            except Exception as _ppe:
                print(f"[PRICE_PUSH] ❌ {_ppe}")
            time.sleep(2)  # ★ 2초 고정 — _check_rules와 완전 독립
    
    def _rules_loop(self):
        """★ v4.0 고빈도 전용 루프: 익절/손절/트레일링/타임아웃 체크
        + 미체결 주문 추적 + 기술적 지표 프리로드
        ★ v8.0: 중장기 보유종목 있으면 단타 중지해도 계속 실행
        """
        _ta_preload_ts = 0  # 기술적 지표 프리로드 타이머
        _pending_check_ts = 0  # 미체결 체크 타이머
        
        # ★★★ v8.0: 가격 push 독립 스레드 시작 (2초마다, _check_rules 블로킹 무관) ★★★
        if not hasattr(self, '_price_thread') or not self._price_thread or not self._price_thread.is_alive():
            self._price_thread = threading.Thread(target=self._price_push_loop, daemon=True)
            self._price_thread.start()
            print("[PRICE_PUSH] ★ 실시간 가격 push 독립 스레드 시작 (2초 주기)")
        
        while self.running:  # v8: swing removed
            try:
                # ★★★ F2: 휴일/주말 + 장외시간 → 30초 sleep (CPU 절약) ★★★
                _hm_now = datetime.now().hour * 100 + datetime.now().minute
                _is_hol_r, _ = is_market_holiday()
                if _is_hol_r or _hm_now > 1530 or _hm_now < 840:
                    time.sleep(30)
                    continue
                
                # ★ 1. 규칙 기반 익절/손절/트레일/타임아웃 (항상 2초 주기)
                self._check_rules()
                
                # ★ v4.0: 미체결 주문 추적 (30초마다)
                _hm_now = datetime.now().hour * 100 + datetime.now().minute
                if time.time() - _pending_check_ts > 30 and 900 <= _hm_now <= 1530:
                    _pending_check_ts = time.time()
                    try:
                        cfg = self.config
                        if _pending_orders and cfg.get('app_key'):
                            _pc_token = kis_get_token(cfg['app_key'], cfg['app_secret'], 'live')
                            check_pending_orders(cfg['app_key'], cfg['app_secret'], 'live',
                                                _pc_token, cfg['account'], cfg.get('account_cd', '01'))
                    except Exception as _pce:
                        print(f"[PENDING] 체크 실패: {_pce}")
                
                # ★ v4.0: 보유종목 기술적 지표 프리로드 (5분마다)
                if time.time() - _ta_preload_ts > 300 and 900 <= _hm_now <= 1530:
                    _ta_preload_ts = time.time()
                    try:
                        cfg = self.config
                        if auto_tickers and cfg.get('app_key'):
                            _ta_token = kis_get_token(cfg['app_key'], cfg['app_secret'], 'live')
                            _ta_loaded = 0
                            for _ta_tk in auto_tickers[:5]:
                                _ta = get_technical_indicators(cfg['app_key'], cfg['app_secret'], 'live', _ta_token, _ta_tk)
                                if _ta:
                                    _ta_loaded += 1
                            if _ta_loaded > 0:
                                print(f"[V4_TA] 보유종목 {_ta_loaded}개 기술적 지표 프리로드 완료")
                    except Exception as _tlre:
                        print(f"[V4_TA] 프리로드 실패: {_tlre}")

                # ★★★ v8.0: 중장기 스캔 완전 비활성화 (swing 탭 삭제) ★★★
                # swing_tickers는 start() 시 auto_tickers로 이관됨
                # 아래 swing 스캔 로직 제거 → CPU/API 절약

                # ★ v8.0: positions broadcast는 _price_push_loop 독립 스레드에서 처리

                # ★ 3. 일일 목표 달성 체크 (full 모드)
                if self.config.get('auto_level') == 'full':
                    self._check_daily_target()

                # ★ 4. 장마감 강제청산 (force_close 설정 시)
                now_hhmm = datetime.now().hour * 100 + datetime.now().minute
                _today_s = datetime.now().strftime('%Y-%m-%d')
                _force_close = self.config.get('force_close', False)
                _force_close_time = int(self.config.get('force_close_time', 1515))
                
                if _force_close and now_hhmm >= _force_close_time and now_hhmm <= _force_close_time + 5:
                    _already_closed = any(t.get('type') == 'FORCE_CLOSE_MARKET' and t.get('date') == _today_s for t in trade_log)
                    if not _already_closed and auto_tickers:
                        _close_count = len(auto_tickers)
                        trade_log.append({"time": datetime.now().isoformat(), "date": _today_s, "mode": "live",
                            "type": "FORCE_CLOSE_MARKET",
                            "message": f"⏹ 장마감 강제청산 시작 ({_force_close_time//100}:{_force_close_time%100:02d}) — {_close_count}종목"})
                        save_state()
                        # _liquidate_all 호출 (cfg, token 필요)
                        try:
                            _cfg = self.config
                            _fc_token = kis_get_token(_cfg.get('app_key',''), _cfg.get('app_secret',''), 'live')
                            _fc_bal = get_balance(_cfg.get('app_key',''), _cfg.get('app_secret',''), 'live',
                                                  _fc_token, _cfg.get('account',''), _cfg.get('account_cd','01'))
                            _fc_sold = 0
                            for pos in _fc_bal.get('output1', []):
                                tkr = pos.get('pdno','')
                                if tkr not in auto_tickers: continue
                                if tkr in perm_blocked: continue  # ★ 거래정지 skip
                                qty = int(pos.get('hldg_qty', 0))
                                if qty <= 0: continue
                                cp = float(pos.get('prpr', 0)) or float(pos.get('pchs_avg_pric', 0))
                                try:
                                    self._execute_sell(_cfg, _fc_token, tkr, pos.get('prdt_name', tkr),
                                        qty, cp, f"⏹ 장마감 강제청산 ({_force_close_time//100}:{_force_close_time%100:02d})",
                                        avg_price=float(pos.get('pchs_avg_pric', 0)))
                                    _fc_sold += 1
                                except Exception as _fce:
                                    print(f"[FORCE_CLOSE] {tkr} 청산 실패: {_fce}")
                            trade_log.append({"time": datetime.now().isoformat(), "date": _today_s, "mode": "live",
                                "type": "FORCE_CLOSE_MARKET",
                                "message": f"✅ 장마감 강제청산 완료 — {_fc_sold}/{_close_count}종목 매도"})
                            # ★ 강제청산 후 스캔 트리거 억제
                            self._scan_trigger = False
                            self._slots_full = True
                            # ★ v5.0: 강제청산 후 sell_plan/상태 전체 정리
                            _sell_plans.clear()
                            _ai_sell_states.clear()
                            tg_send(f"⏹ <b>장마감 강제청산 완료</b>\n{_fc_sold}/{_close_count}종목 매도\n⏰ {datetime.now().strftime('%H:%M')}",
                                buttons=[[('📊 현황', 'cmd_status')]])
                            save_state()
                        except Exception as _fce:
                            trade_log.append({"time": datetime.now().isoformat(), "date": _today_s,
                                "type": "ERROR", "message": f"장마감 청산 오류: {_fce}"})
                            save_state()
                
                elif now_hhmm >= 1520 and now_hhmm <= 1525:
                    # force_close OFF일 때는 기존처럼 알림만
                    _already = any(t.get('type') in ('KRX_CLOSE','FORCE_CLOSE_MARKET') and t.get('date') == _today_s for t in trade_log)
                    if not _already:
                        trade_log.append({"time": datetime.now().isoformat(), "date": _today_s, "mode": "live",
                            "type": "KRX_CLOSE", "message": "⏹ KRX 장 마감 (계속 보유 - 장마감청산 OFF)"})
                        save_state()

            except Exception as e:
                trade_log.append({"time": datetime.now().isoformat(), "type": "ERROR",
                    "message": f"[rules_loop] {e}"})
                save_state()
            # ★ 적응적 익절/손절 체크 간격 (급등/급락 시 더 빠르게)
            if hasattr(self, '_last_volatility_check'):
                volatility = getattr(self, '_market_volatility', 'normal')  # high/normal/low
            else:
                volatility = 'normal'
                
            if self._is_scalp_mode():
                sleep_sec = 1 if volatility == 'high' else 1.5
            else:
                # ★ 통일: 변동성 높으면 2초, 보통 3초
                sleep_sec = 2 if volatility == 'high' else 3
            
            # ★ Fix: 장외 시간(15:30 이후)에는 30초 대기 (불필요 API 호출 방지)
            _now_hm = datetime.now().hour * 100 + datetime.now().minute
            if _now_hm > 1530 or _now_hm < 840:
                sleep_sec = 30
                
            time.sleep(sleep_sec)

    def _scan_loop(self):
        """★ 저빈도 전용 루프: AI 스캔 + 브리핑 (논블로킹)
        - _rules_loop와 완전 독립 → 스캔 중에도 익절/손절 즉시 반응
        - 브리핑도 여기서 별도 스레드로 실행
        """
        last_ai_scan = 0
        last_market_phase = None  # 장 구간 변화 감지 → 즉시 스캔용
        # ★ AI 스캔 중복 방지용 락 추가
        if not hasattr(self, '_scan_lock'):
            self._scan_lock = threading.Lock()
            
        while self.running:
            try:
                now = datetime.now()
                current_time = now.hour * 100 + now.minute
                
                # ★ 주말 + 공휴일 체크 — API 비용 낭비 방지
                _is_holiday, _holiday_reason = is_market_holiday(now)
                if _is_holiday:
                    if not getattr(self, '_weekend_warned', False):
                        print(f"[SCAN] ⏸ 휴장({_holiday_reason}) — 스캔 대기 중")
                        self._weekend_warned = True
                    time.sleep(60)  # 1분마다 체크
                    continue
                self._weekend_warned = False
                
                # ★★★ 날짜 변경 감지 → daily_briefing 자동 리셋 (서버 안 끄고 다음날) ★★★
                today_str = now.strftime('%Y-%m-%d')
                if daily_briefing.get('date') and daily_briefing['date'] != today_str:
                    _old = daily_briefing.get('date', '?')
                    # 어제 데이터 briefing_history에 저장 후 리셋
                    try: save_briefing_history()
                    except: pass
                    daily_briefing.clear()
                    daily_briefing['date'] = today_str
                    # 마감 관련 플래그도 리셋
                    self._market_open_scan_done = False
                    self._bal_prewarmed = False
                    self._premarket_done = False  # ★ v8.0: 08:55 프리마켓 리셋
                    self._preclose_warned = False  # ★ v8.0: 12:00 차단 경고 리셋
                    self._live_precache_done = False  # ★ v6.0: 09:01 예비 수집 리셋
                    self._hourly_report_done = set()  # ★ 시간별 리포트 리셋
                    self._closing_price_done = False  # ★ 종가 업데이트 리셋
                    # ★ v6.0: 물타기 카운트 리셋 (새 날)
                    auto_avg_count.clear()
                    # ★★★ v8.0: 전날 매도 상태 초기화 (물타기 정상작동 보장) ★★★
                    _sell_stage.clear()
                    _tp1_triggered.clear()
                    peak_prices.clear()
                    _dip_flag.clear()
                    swing_sell_stage.clear()
                    swing_dip_flag.clear()
                    # swing_avg_count는 유지 (중장기=며칠 보유 → 물타기 횟수 누적)
                    print(f"[DATE] 새 날 초기화: 단타+중장기 sell_stage/peak/avg_count")
                    # ★ v5.0: AI 매도 상태 리셋
                    global _ai_classify_ts, _ai_sell_states
                    _ai_classify_ts = 0
                    _ai_sell_states.clear()
                    _sell_plans.clear()  # 어제 sell_plan 삭제 (보유종목은 새로 받음)
                    # ★ v8.0: 날짜 변경 시 perm_blocked 자동 해제 (HALTED/매매불가는 영구 유지)
                    _stale_perms = [k for k, v in list(perm_blocked.items())
                        if f"오늘({today_str})" not in v
                        and not v.startswith('HALTED:')
                        and '매매불가' not in v and '매매정지' not in v and '처리가 안되었습니다' not in v]
                    for _k in _stale_perms:
                        del perm_blocked[_k]
                    if _stale_perms:
                        print(f"[PERM] 날짜변경 → {len(_stale_perms)}개 해제 (HALTED {sum(1 for v in perm_blocked.values() if v.startswith('HALTED:'))}개 유지)")
                    print(f"[SCAN] 📅 날짜 변경! ({_old} → {today_str}) → daily_briefing 리셋")
                    trade_log.append({"time": now.isoformat(), "date": today_str,
                        "type": "SYSTEM", "message": f"📅 날짜 변경 ({_old} → {today_str}) → 시황 데이터 초기화"})
                    save_state()
                
                elapsed = time.time() - last_ai_scan
                scan_interval = self._get_scan_interval(current_time)

                # ★ 장 구간 전환 감지 → last_ai_scan 리셋 (즉시 스캔)
                # 8:40→프리마켓, 9:00→장시작, 11:00→소강, 14:00→오후, 15:20→마감
                cur_phase = (
                    'premarket' if current_time < 900
                    else 'morning' if current_time < 1100
                    else 'midday' if current_time < 1400
                    else 'afternoon' if current_time < 1520
                    else 'close'
                )
                if cur_phase != last_market_phase and last_market_phase is not None:
                    # 장 구간 바뀐 순간 → 바로 스캔 (타이머 무시)
                    last_ai_scan = 0
                    trade_log.append({"time": now.isoformat(), "type": "SCAN_TICK",
                        "message": f"🔄 구간 전환 ({last_market_phase}→{cur_phase}) → 즉시 스캔 실행"})
                    save_state()
                last_market_phase = cur_phase

                # ★ AI 스캔 atomic 체크-앤-셋 (중복 방지 강화)
                # ★ 슬롯 열리면 즉시 스캔 (매도 후 빈 슬롯 즉시 채우기)
                _force_scan = False
                if getattr(self, '_scan_trigger', False):
                    # ★★★ F2: 스캔 진행 중이면 트리거 소비 안 함 → 스캔 끝나고 즉시 재스캔
                    if getattr(self, '_scan_running', False):
                        _force_scan = True  # 다음 체크에서 시도할 수 있도록
                        # _scan_trigger는 False로 안 바꿈 → 스캔 끝나면 3초 내 재감지
                    else:
                        self._scan_trigger = False
                        self._slots_full = False  # 매도로 슬롯 열렸으니 리셋
                        self._last_full_log_ts = 0  # 로그 리셋
                        last_ai_scan = 0
                        _force_scan = True
                        print("[SCAN] 매도로 슬롯 열림 → 즉시 스캔 트리거")

                # ★★★ v8.0: auto_tickers 기준으로 직접 슬롯 풀 체크 (매번 갱신) ★★★
                _max_pos_cfg = int(self.config.get('max_positions', 6)) if self.config else 6
                _was_full = self._slots_full
                # ★ 거래정지 종목은 슬롯에서 제외 (매도 불가 → 슬롯 낭비 방지)
                _active_count = len([t for t in auto_tickers if not _is_truly_halted(t)])
                if _active_count >= _max_pos_cfg:
                    self._slots_full = True
                else:
                    self._slots_full = False  # ★ 슬롯 비면 즉시 해제
                    if _was_full:
                        # ★★★ v8.0: 슬롯이 꽉참→빈상태 전환 시 즉시 스캔 (설정변경/매도 모두 대응) ★★★
                        last_ai_scan = 0
                        _force_scan = True
                        self._last_full_log_ts = 0
                        print(f"[SCAN] 슬롯 열림 ({_active_count}/{_max_pos_cfg}) → 즉시 스캔")
                _skip_scan = self._slots_full and not _force_scan
                
                # ★★★ 장마감 청산 완료 후 스캔 완전 차단 ★★★
                _force_closed_today = any(t.get('type') == 'FORCE_CLOSE_MARKET' and t.get('date') == today_str 
                                         and '완료' in (t.get('message','')) for t in trade_log[-20:])
                if _force_closed_today:
                    _skip_scan = True
                
                # ★★★ v8.0: 12:00 이후 매수 스캔 중지 (오전만 매매) ★★★
                _fc_time = int(self.config.get('force_close_time', 1515))
                _fc_enabled = self.config.get('force_close', False)
                if current_time >= 1200 and not _skip_scan:
                    if not getattr(self, '_preclose_warned', False):
                        self._preclose_warned = True
                        trade_log.append({"time": now.isoformat(), "date": today_str,
                            "type": "SYSTEM",
                            "message": f"⏸ 12:00 이후 → 매수 스캔 중지, 매도만 계속{' (장마감청산 '+str(_fc_time//100)+':'+f'{_fc_time%100:02d}'+')' if _fc_enabled else ''}"})
                        save_state()
                        print(f"[SCAN] ⏸ 12:00 → 매수 스캔 중지")
                    _skip_scan = True

                if not _skip_scan and scan_interval > 0 and elapsed >= scan_interval and (_force_scan or not self.paused):
                    # ★★★ v8.0: 스캔 직전 슬롯 최종 확인 (설정 변경 즉시 반영) ★★★
                    _final_max = int(self.config.get('max_positions', 6)) if self.config else 6
                    _final_count = len([t for t in auto_tickers if not _is_truly_halted(t)])
                    if _final_count >= _final_max:
                        self._slots_full = True
                        if not hasattr(self, '_scan_block_log_ts') or time.time() - self._scan_block_log_ts > 60:
                            self._scan_block_log_ts = time.time()
                            print(f"[SCAN] ⏸ 슬롯 꽉참 {_final_count}/{_final_max} → 스캔 차단")
                    elif self._scan_lock.acquire(blocking=False):
                        try:
                            if self._scan_running:
                                continue
                            self._scan_running = True
                            last_ai_scan = time.time()
                            
                            phase = self._get_market_phase(current_time)
                            pause_label = " (⏸️매수중단)" if self.paused else ""
                            trade_log.append({"time": now.isoformat(), "type": "SCAN_TICK",
                                "message": f"⏱ {phase}{pause_label} | 스캔간격 {scan_interval//60}분"})
                            save_state()
                            t = threading.Thread(target=self._run_ai_scan_safe, daemon=True)
                            t.start()
                        finally:
                            self._scan_lock.release()

                # ★★★ v8.0: 08:55 프리마켓 스캔 (급등 후보 미리 수집 → 09:01 즉시 AI 판단) ★★★
                if 855 <= current_time <= 857 and not getattr(self, '_premarket_done', False):
                    self._premarket_done = True
                    try:
                        _pm_cfg = self.config
                        _pm_token = kis_get_token(_pm_cfg.get('app_key',''), _pm_cfg.get('app_secret',''), 'live')
                        _pm_candidates = []
                        _pm_skip = set(auto_tickers) | set(perm_blocked.keys()) | set(recently_sold.keys())
                        
                        # ① KIS 거래량 TOP 30 (프리마켓: 필터 완화 — 장 전에는 등락률/거래량이 0)
                        _pm_vol = kis_request("GET", "/uapi/domestic-stock/v1/quotations/volume-rank",
                            _pm_cfg.get('app_key',''), _pm_cfg.get('app_secret',''), 'live', _pm_token, "FHPST01710000",
                            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "20171",
                                    "FID_INPUT_ISCD": "0000", "FID_DIV_CLS_CODE": "0",
                                    "FID_BLNG_CLS_CODE": "0", "FID_TRGT_CLS_CODE": "111111111",
                                    "FID_TRGT_EXLS_CLS_CODE": "000000", "FID_INPUT_PRICE_1": "0",
                                    "FID_INPUT_PRICE_2": "0", "FID_VOL_CNT": "0", "FID_INPUT_DATE_1": ""})
                        for s in _pm_vol.get('output', [])[:30]:
                            tk = s.get('mksc_shrn_iscd','')
                            nm = s.get('hts_kor_isnm','')
                            pr = int(float(s.get('stck_prpr','0') or 0))
                            chg = float(s.get('prdy_ctrt','0') or 0)
                            vol = int(s.get('acml_vol','0') or 0)
                            if tk in _pm_skip: continue
                            if pr < 1000 or pr > 50000: continue
                            # ★ 프리마켓: 등락률/거래량 필터 없음 (장 전에는 0이므로)
                            if any(kw in nm for kw in ['KODEX','TIGER','KBSTAR','ETF','인버스','레버리지']): continue
                            _pm_candidates.append({'ticker': tk, 'name': nm, 'price': pr, 'chg': chg, 'volume': vol, 'source': '거래량'})
                        
                        time.sleep(0.2)
                        
                        # ② KIS 등락률 TOP 30
                        _pm_existing = set(c['ticker'] for c in _pm_candidates)
                        _pm_rate = kis_request("GET", "/uapi/domestic-stock/v1/quotations/volume-rank",
                            _pm_cfg.get('app_key',''), _pm_cfg.get('app_secret',''), 'live', _pm_token, "FHPST01710000",
                            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "20171",
                                    "FID_INPUT_ISCD": "0000", "FID_DIV_CLS_CODE": "1",
                                    "FID_BLNG_CLS_CODE": "0", "FID_TRGT_CLS_CODE": "111111111",
                                    "FID_TRGT_EXLS_CLS_CODE": "000000", "FID_INPUT_PRICE_1": "0",
                                    "FID_INPUT_PRICE_2": "0", "FID_VOL_CNT": "0", "FID_INPUT_DATE_1": ""})
                        for s in _pm_rate.get('output', [])[:30]:
                            tk = s.get('mksc_shrn_iscd','')
                            nm = s.get('hts_kor_isnm','')
                            pr = int(float(s.get('stck_prpr','0') or 0))
                            chg = float(s.get('prdy_ctrt','0') or 0)
                            vol = int(s.get('acml_vol','0') or 0)
                            if tk in _pm_skip or tk in _pm_existing: continue
                            if pr < 1000 or pr > 50000: continue
                            # ★ 프리마켓: 등락률/거래량 필터 없음
                            if any(kw in nm for kw in ['KODEX','TIGER','KBSTAR','ETF','인버스','레버리지']): continue
                            _pm_candidates.append({'ticker': tk, 'name': nm, 'price': pr, 'chg': chg, 'volume': vol, 'source': '등락률'})
                        
                        # ③ 캐시 저장 (09:01 스캔에서 사용)
                        self._premarket_cache = _pm_candidates
                        self._premarket_cache_ts = time.time()
                        
                        _pm_top3 = sorted(_pm_candidates, key=lambda x: x['chg'], reverse=True)[:5]
                        _pm_str = ', '.join(f"{c['name']}({c['ticker']})+{c['chg']:.1f}%" for c in _pm_top3)
                        print(f"[PREMARKET] ✅ 08:55 프리마켓 {len(_pm_candidates)}종목 수집 | TOP: {_pm_str}")
                        trade_log.append({"time": now.isoformat(), "date": today_str,
                            "type": "PREMARKET",
                            "message": f"📡 프리마켓 {len(_pm_candidates)}종목 수집: {_pm_str}"})
                        
                        # ④ 텔레그램 프리마켓 알림
                        if telegram_config.get('enabled') and _pm_candidates:
                            _tg_msg = f"📡 <b>08:55 프리마켓 스캔</b>\n━━━━━━━━━━━━━━\n"
                            _tg_msg += f"급등 후보 {len(_pm_candidates)}종목 발견\n\n"
                            for _c in _pm_top3:
                                _tg_msg += f"🔥 <b>{_c['name']}</b>({_c['ticker']}) +{_c['chg']:.1f}% ₩{_c['price']:,}\n"
                            _tg_msg += f"\n⏰ 09:01 첫 스캔에서 AI 매수 판단 예정"
                            try: tg_send(_tg_msg)
                            except: pass
                        
                        save_state()
                    except Exception as _pme:
                        print(f"[PREMARKET] ⚠️ 프리마켓 스캔 실패: {_pme}")
                
                # ★★★ v8.0: 8:58~8:59 스캔 준비 (토큰+잔고+시장+TA 프리캐싱) ★★★
                if 858 <= current_time <= 859 and not getattr(self, '_bal_prewarmed', False):
                    self._bal_prewarmed = True
                    _pw_ready = []
                    _pw_fail = []
                    try:
                        _pw_cfg = self.config
                        # ① 토큰 갱신
                        _pw_token = kis_get_token(_pw_cfg.get('app_key',''), _pw_cfg.get('app_secret',''), 'live')
                        _pw_ready.append('토큰')
                        
                        # ② 잔고 캐시
                        get_balance(_pw_cfg.get('app_key',''), _pw_cfg.get('app_secret',''), 'live',
                                    _pw_token, _pw_cfg.get('account',''), _pw_cfg.get('account_cd','01'), max_age=0)
                        _pw_ready.append(f'잔고({len(auto_tickers)}종목)')
                        
                        # ③ 네이버 시장 데이터 (코스피/코스닥 지수)
                        try:
                            fetch_naver_market_data()
                            _pw_ready.append('시장지수')
                        except: _pw_fail.append('시장지수')
                        
                        # ④ 보유종목 TA 프리로드
                        _ta_cnt = 0
                        for _pw_tk in list(auto_tickers)[:6]:
                            try:
                                _ta = get_technical_indicators(_pw_cfg.get('app_key',''), _pw_cfg.get('app_secret',''),
                                    'live', _pw_token, _pw_tk)
                                if _ta:
                                    _ta_cache[_pw_tk] = {'data': _ta, 'ts': time.time()}
                                    _ta_cnt += 1
                                time.sleep(0.15)
                            except: pass
                        if _ta_cnt > 0:
                            _pw_ready.append(f'TA({_ta_cnt}종목)')
                        
                        # ⑤ 준비 완료 로그
                        
                        _status = ' + '.join(_pw_ready)
                        _fail_str = f" | 실패: {', '.join(_pw_fail)}" if _pw_fail else ""
                        print(f"[PREWARM] ✅ 8:59 스캔 준비 완료: {_status}{_fail_str}")
                        trade_log.append({"time": now.isoformat(), "date": today_str,
                            "type": "SYSTEM",
                            "message": f"🏇 스캔 준비 완료: {_status} → 09:01 첫 스캔 대기{_fail_str}"})
                        save_state()
                    except Exception as _pwe:
                        print(f"[PREWARM] ⚠️ 프리워밍 실패: {_pwe}")
                
                # ★★★ v8.0: 09:01 장 시작 첫 스캔 (1분 실거래 데이터 축적 후) ★★★
                if 901 <= current_time <= 910 and not getattr(self, '_market_open_scan_done', False):
                    self._market_open_scan_done = True
                    last_ai_scan = 0  # 즉시 스캔 트리거
                    print(f"[SCAN] ⚡ 09:01 첫 스캔 출발! (현재 {current_time})")
                    trade_log.append({"time": now.isoformat(), "date": today_str,
                        "type": "SCAN_TICK",
                        "message": f"🏇 09:01 첫 스캔 출발! 달리는 말에 올라타기 ({current_time//100}:{current_time%100:02d})"})
                    save_state()

                # ★★★ v8.0: 시황분석/프리마켓 워치리스트 삭제 — 단타/중장기 모두 자체 데이터 사용 ★★★
                # (글로벌 브리핑, 보완 브리핑, 프리마켓 3단계, 오전/중간/점심/오후 브리핑 전부 제거)
                # → AI API 비용 하루 ₩3,000~7,000 절약 + 스캔 속도 향상

                # ★★★ v6.0: 보유종목 AI 평가 — 수동 버튼만 (자동 스케줄 삭제) ★★★

                # ⑦ 15:30 마감 브리핑 + 인수인계
                if 1530 <= current_time <= 1630 and not daily_briefing.get('closing_done'):
                    print(f"[CLOSING] ★ 마감 브리핑 트리거! (현재 {current_time}, closing_done={daily_briefing.get('closing_done')})")
                    t = threading.Thread(target=self._run_closing_briefing, daemon=True)
                    t.start()
                
                # ★★★ v4.1: 15:35 종가 확정 후 매도종목 종가 업데이트 ★★★
                if 1535 <= current_time <= 1540 and not getattr(self, '_closing_price_done', False):
                    self._closing_price_done = True
                    def _update_closing_prices():
                        try:
                            _today = datetime.now().strftime('%Y-%m-%d')
                            _updated = 0
                            _sell_tickers = set(t.get('ticker','') for t in trade_log 
                                if t.get('date')==_today and t.get('type')=='SELL' and t.get('success'))
                            _token = kis_get_token(cfg.get('app_key',''), cfg.get('app_secret',''), 'live')
                            for _stk in _sell_tickers:
                                try:
                                    _cp = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
                                        cfg.get('app_key',''), cfg.get('app_secret',''), 'live',
                                        _token, "FHKST01010100",
                                        params={"FID_COND_MRKT_DIV_CODE":"J","FID_INPUT_ISCD":_stk})
                                    _closing = int(_cp.get('output',{}).get('stck_prpr',0) or 0)
                                    if _closing > 0:
                                        for _st in trade_log:
                                            if _st.get('date')==_today and _st.get('ticker')==_stk and _st.get('type')=='SELL' and _st.get('success'):
                                                if 'stock_data' not in _st:
                                                    _st['stock_data'] = {}
                                                _st['stock_data']['closing_price'] = _closing
                                                _sell_p = _st.get('price', 0)
                                                if _sell_p > 0:
                                                    _st['stock_data']['after_sell_pct'] = round((_closing - _sell_p) / _sell_p * 100, 2)
                                        _updated += 1
                                    time.sleep(0.1)
                                except: pass
                            if _updated:
                                save_state()
                                print(f"[15:35] ✅ {_updated}종목 종가 업데이트 완료")
                                trade_log.append({"time": datetime.now().isoformat(), "date": _today,
                                    "type": "SYSTEM", "message": f"📊 {_updated}종목 종가 업데이트 (AI 매도학습 데이터)"})
                                save_state()
                        except Exception as _e:
                            print(f"[15:35] 종가 업데이트 실패: {_e}")
                    threading.Thread(target=_update_closing_prices, daemon=True).start()

                # ★★★ v6.0: 15:20 분봉 패턴 수집 (장 마감 전 데이터 확보) ★★★
                if 1520 <= current_time <= 1525 and not getattr(self, '_pattern_done', False):
                    self._pattern_done = True
                    def _collect_patterns():
                        try:
                            _token = kis_get_token(cfg.get('app_key',''), cfg.get('app_secret',''), 'live')
                            collect_today_patterns(cfg.get('app_key',''), cfg.get('app_secret',''), 'live', _token)
                        except Exception as e:
                            print(f"[PATTERN] 수집 실패: {e}")
                    threading.Thread(target=_collect_patterns, daemon=True).start()

                # ★★★ 시간별 실적 리포트 (텔레그램) ★★★
                # 9:30, 10:30, 11:30, 12:30, 13:30, 14:30
                if not hasattr(self, '_hourly_report_done'):
                    self._hourly_report_done = set()
                    # 재시작 시 이미 지난 시간은 skip
                    for _past_rt in [930, 1030, 1130, 1230, 1330, 1430]:
                        if current_time > _past_rt + 3:
                            self._hourly_report_done.add(_past_rt)
                _report_times = [930, 1030, 1130, 1230, 1330, 1430]  # ★ 1530 제거 (마감 브리핑에서 _tg_daily_report가 처리)
                for _rt in _report_times:
                    if current_time >= _rt and current_time <= _rt + 3 and _rt not in self._hourly_report_done:
                        self._hourly_report_done.add(_rt)
                        if telegram_config.get('enabled') and telegram_config.get('on_sell', True):
                            _is_closing = (_rt == 1530)
                            threading.Thread(target=_tg_hourly_report, args=(_is_closing,), daemon=True).start()
                            print(f"[TG] ⏰ {_rt//100}:{_rt%100:02d} {'마감' if _is_closing else '시간별'} 리포트 전송")
                        break  # 한 사이클에 하나만

            except Exception as e:
                trade_log.append({"time": datetime.now().isoformat(), "type": "ERROR",
                    "message": f"[scan_loop] {e}"})
                save_state()
            time.sleep(3)  # ★ F1: 10초→3초 (매도 후 빈슬롯 감지 3배 빨라짐)

    def _run_ai_scan_safe(self):
        """AI 스캔을 안전하게 실행 - 중복 방지 플래그 관리 + 소요시간 측정"""
        _scan_start = time.time()
        try:
            self._check_ai_autobuy()
        except Exception as e:
            trade_log.append({"time": datetime.now().isoformat(), "type": "AI_ERROR",
                "message": f"AI 스캔 오류: {e}"})
            save_state()
        finally:
            # ★ S3: 스캔 소요시간 로그 (AI가 병목 파악 + 자동 개선용)
            _scan_elapsed = time.time() - _scan_start
            _scan_min = int(_scan_elapsed // 60)
            _scan_sec = int(_scan_elapsed % 60)
            _timing = getattr(self, '_last_scan_timing', {})
            _timing_str = ' / '.join(f"{k}:{v:.0f}초" for k, v in _timing.items()) if _timing else ''
            _time_label = f"{_scan_min}분{_scan_sec}초" if _scan_min > 0 else f"{_scan_sec}초"
            print(f"[SCAN_TIME] ⏱ 스캔 완료 {_time_label} ({_timing_str})")
            trade_log.append({
                "time": datetime.now().isoformat(),
                "type": "SCAN_PERF",
                "message": f"⏱ 스캔 {_time_label} ({_timing_str})",
                "scan_seconds": round(_scan_elapsed, 1),
                "timing_detail": _timing
            })
            if _scan_elapsed > 120:
                print(f"[SCAN_TIME] ⚠️ 스캔 2분 초과! 데이터 수집 최적화 필요")
            save_state()
            # ★ 스캔 완료 후 플래그 해제
            self._scan_running = False

    def _loop(self):
        """하위 호환 유지용 (직접 호출 없음)"""
        pass

    # ★ v3.0 TIER 2: 멀티 전략 엔진 — 장세에 따라 최적 전략 자동 선택
    def _select_strategy(self, bal_output1=None):
        """시장 상황 분석 → 최적 전략 선택
        
        전략 3가지:
        1. MOMENTUM: 거래량 급증 + 상승 초입 종목 포착 (상승장/변동장)
        2. FLOW: 외국인/기관 수급 추종 (안정적 상승장)
        3. THEME: 테마/섹터 순환 매매 (횡보장/테마장)
        
        Returns: (strategy_name, strategy_prompt, strategy_emoji)
        """
        now = datetime.now()
        hhmm = now.hour * 100 + now.minute
        today = now.strftime('%Y-%m-%d')
        
        # 데이터 수집: 오늘 매매 성과
        t_sells = [t for t in trade_log if t.get('date')==today and t.get('type') in ('SELL','FORCE_CLOSE') and t.get('success')]
        t_buys = [t for t in trade_log if t.get('date')==today and t.get('type')=='AI_BUY' and t.get('success')]
        realized_pnl = sum(float(t.get('pnl',0) or 0) for t in t_sells)
        win_count = sum(1 for t in t_sells if float(t.get('pnl',0) or 0) > 0)
        loss_count = sum(1 for t in t_sells if float(t.get('pnl',0) or 0) < 0)
        
        # 브리핑 데이터
        outlook_dir = ''
        outlook_conf = 0
        us_trend = ''
        if daily_briefing.get('data'):
            bd = daily_briefing['data']
            ol = bd.get('korea_outlook', {})
            outlook_dir = ol.get('direction', '')
            outlook_conf = int(ol.get('confidence', 0) or 0)
            us_sp = bd.get('us_market', {}).get('sp500', {}).get('change_pct', '')
            try:
                us_val = float(str(us_sp).replace('%','').replace('+',''))
                us_trend = 'up' if us_val > 0.3 else ('down' if us_val < -0.3 else 'flat')
            except: us_trend = 'unknown'
        
        # 시간대별 기본 전략
        if hhmm < 1000:
            time_bias = 'MOMENTUM'  # 장 초반: 모멘텀
        elif hhmm < 1200:
            time_bias = 'FLOW'     # 오전: 수급 확인 후 진입
        elif hhmm < 1400:
            time_bias = 'THEME'    # 점심: 테마 순환
        else:
            time_bias = 'MOMENTUM'  # 오후: 모멘텀 마감 랠리
        
        # 장세 판단 → 전략 오버라이드
        strategy = time_bias
        
        if '하락' in outlook_dir or us_trend == 'down':
            strategy = 'FLOW'  # 하락장: 수급 좋은 것만 선별
        elif '상승' in outlook_dir and outlook_conf >= 70:
            strategy = 'MOMENTUM'  # 확실한 상승장: 모멘텀
        elif outlook_conf < 50:
            strategy = 'THEME'  # 불확실: 테마 분산
        
        # 오늘 손실 많으면 보수적 전략
        if realized_pnl < -50000 or (loss_count > win_count + 2):
            strategy = 'FLOW'  # 손실 구간: 수급 기반 안전 매매
        
        # 전략별 프롬프트 가이드
        prompts = {
            'MOMENTUM': {
                'name': '모멘텀 돌파',
                'emoji': '🚀',
                'guide': """[전략: 모멘텀 돌파 🚀]
최우선 기준: 거래량 급증(전일 대비 2배+) + 상승 초입(+1~5%)
- 거래량이 터지면서 막 올라가기 시작하는 종목 포착
- 이미 +10% 넘은 종목은 후발주 아닌지 검증
- 장 초반 갭상승 후 눌림목 지지 확인 종목도 OK
- 코스닥 소형주 모멘텀도 적극 발굴
핵심: 거래량이 답이다! 거래량 없는 상승은 패스"""
            },
            'FLOW': {
                'name': '수급 추종',
                'emoji': '🏦',
                'guide': """[전략: 수급 추종 🏦]
최우선 기준: 외국인 OR 기관 3일 연속 순매수 + 완만한 상승세
- 외국인/기관이 꾸준히 사는 종목 = 큰 손이 보는 종목
- 급등보다 안정적 우상향 선호 (+0.5~3% 완만 상승)
- 대형주/중형주 위주 (시총 5000억 이상)
- 뉴스에 악재 없는 깨끗한 차트
핵심: 큰 손 따라가기! 수급 역행 절대 금지"""
            },
            'THEME': {
                'name': '테마 순환',
                'emoji': '🎯',
                'guide': """[전략: 테마 순환 🎯]
최우선 기준: 오늘 주도 테마/섹터에서 아직 덜 오른 종목
- 반도체/AI/2차전지/바이오 등 오늘 주도 테마 파악
- 테마 대장주가 올랐으면 같은 테마 2~3등주 진입
- 오전에 안 올랐는데 오후에 테마 확산 기대되는 종목
- 정책/이벤트(금리, 실적발표) 수혜 테마 우선
핵심: 테마의 확산을 노린다! 주도 테마 바깥 종목은 패스"""
            }
        }
        
        selected = prompts[strategy]
        
        # 전략 로그
        print(f"[STRATEGY] {selected['emoji']} {selected['name']} 선택 "
              f"(시간:{hhmm}, 전망:{outlook_dir}({outlook_conf}%), 미국:{us_trend}, 손익:{realized_pnl:+,.0f})")
        
        return strategy, selected['guide'], selected['emoji'], selected['name']

    def _is_scalp_mode(self):
        """★ v3.0: 단타 모드 삭제 — 항상 일반 모드"""
        return False

    def _get_scan_interval(self, hhmm):
        """스캔 스케줄 — UI 커스텀 간격 적용
        ★ v8.0: 12:00 이후 스캔 종료 (오전만 매매)
        09:01~10:00  -> 황금시간 (2분)
        10:00~11:00  -> 장중반 (2분)
        11:00~12:00  -> 오전 (3분)
        12:00~       -> 스캔 종료
        """
        if hhmm < 901:
            return 0  # 09:01 전 스캔 없음
        si = self.config.get('scan_intervals', {})
        def iv(k, default):
            v = si.get(k, default)
            if isinstance(v, dict): v = v.get('min', default)
            try: return int(v) * 60
            except: return int(default) * 60
        if hhmm < 1000:  return iv('s2', 2)
        if hhmm < 1100:  return iv('s3', 2)
        if hhmm < 1200:  return iv('s3b', 3)
        return 0  # 12:00 이후 스캔 없음

    def _get_market_phase(self, hhmm):
        if hhmm < 815:    return "🌙 시스템 대기 중"
        if hhmm < 901:    return "⏳ 장 시작 대기"
        if hhmm < 903:    return "🚀 첫 스캔!"
        if hhmm < 1000:   return "🔥 황금시간 (2분)"
        if hhmm < 1200:   return "📈 오전 (2~3분)"
        if hhmm < 1520:   return "⏸ 매도만 (스캔종료)"
        if hhmm >= 1520:  return "⏹ 장 마감"
        return "⏹ 장 외"
    
    # ★★★ v8.0: 8 dead briefing methods 삭제 (시황분석 제거, 실시간 데이터만 사용) ★★★

    # ★★★ v6.0: 보유종목 AI 평가 (1시간 단위) ★★★
    def _run_closing_briefing(self):
        """15:30 마감 실적 리포트: 전체 매매결과 + 성과분석 + 시스템 고도화"""
        global daily_briefing
        import re
        cfg = self.config
        print(f"[CLOSING] 1단계: cfg 키 확인...")
        anthropic_key = cfg.get('anthropic_key', '') or ai_config.get('anthropic_key', '')
        openai_key = cfg.get('openai_key', '') or ai_config.get('openai_key', '')
        if not anthropic_key and not openai_key:
            print("[CLOSING] ⛔ AI 키 없음 — 마감 리포트 취소")
            daily_briefing['closing_done'] = False  # 재시도 가능
            return
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        if daily_briefing.get('closing_done'):
            print("[CLOSING] 이미 완료됨 — 스킵")
            return
        daily_briefing['closing_done'] = True
        
        print(f"[CLOSING] 2단계: 매매 데이터 수집...")
        trade_log.append({"time": datetime.now().isoformat(), "date": today, "mode": "live",
            "type": "CLOSING_BRIEFING", "message": "📊 마감 실적 리포트 생성 중..."})
        save_state()
        
        # Gather full day trade results in detail
        today_all = [t for t in trade_log if t.get('date') == today]
        buys = [t for t in today_all if t.get('type') in ['BUY','AI_BUY'] and t.get('success', True)]
        sells = [t for t in today_all if t.get('type') == 'SELL' and t.get('success', True)]
        manual_buys = [t for t in today_all if t.get('type') == 'BUY' and t.get('reason','')=='수동매수' and t.get('success', True)]
        manual_sells = [t for t in today_all if t.get('type') == 'SELL' and t.get('reason','')=='수동매도' and t.get('success', True)]
        blocked = [t for t in today_all if t.get('type') in ['BLOCKED','CAPITAL_BLOCK']]
        errors = [t for t in today_all if 'ERROR' in t.get('type', '')]
        avg_downs = [t for t in today_all if t.get('type') == 'AVG_DOWN']
        scans = [t for t in today_all if t.get('type') == 'SCAN_TICK']
        
        buy_detail = '; '.join(f"{t.get('name','?')}({t.get('ticker','')}) {t.get('qty',0)}주 W{t.get('price',0)}" for t in buys[:5])
        sell_detail = '; '.join(f"{t.get('name','?')} {t.get('reason','')}" for t in sells[:5])
        
        # Scan analysis
        scan_phases = {}
        for s in scans:
            msg = s.get('message', '')
            for phase_name in ['프리마켓', '스캘핑', '오전 집중', '소강', '오후 집중']:
                if phase_name in msg:
                    scan_phases[phase_name] = scan_phases.get(phase_name, 0) + 1
        scan_summary = ', '.join(f"{k} {v}회" for k, v in scan_phases.items()) if scan_phases else '스캔 없음'
        
        # ★★★ F2: SCAN_PERF 요약 → 마감 브리핑에 주입 (AI 자기 개선용) ★★★
        _scan_perfs = [t for t in trade_log if t.get('date') == today and t.get('type') == 'SCAN_PERF']
        _scan_perf_summary = ""
        if _scan_perfs:
            _perf_secs = [t.get('scan_seconds', 0) for t in _scan_perfs if t.get('scan_seconds')]
            _avg_sec = sum(_perf_secs) / len(_perf_secs) if _perf_secs else 0
            _max_sec = max(_perf_secs) if _perf_secs else 0
            _over_120 = sum(1 for s in _perf_secs if s > 120)
            # 단계별 평균
            _stage_totals = {}
            for p in _scan_perfs:
                td = p.get('timing_detail', {})
                if td:
                    for k, v in td.items():
                        _stage_totals.setdefault(k, []).append(v)
            _stage_avg = {k: round(sum(v)/len(v), 1) for k, v in _stage_totals.items() if v}
            _bottleneck = max(_stage_avg, key=_stage_avg.get) if _stage_avg else ''
            
            _scan_perf_summary = f"[스캔 성능 분석] 평균 {_avg_sec:.0f}초 / 최대 {_max_sec:.0f}초 / 2분초과 {_over_120}회\n"
            if _stage_avg:
                _scan_perf_summary += f"단계별 평균: {' | '.join(f'{k}:{v}초' for k,v in _stage_avg.items())}\n"
                _scan_perf_summary += f"병목: {_bottleneck} ({_stage_avg.get(_bottleneck,0)}초) → 이 단계 최적화 우선\n"
        
        # Get predictions
        # Current rules info
        rules_info = ', '.join(f"{r.get('type','')}({r.get('label','')})" for r in auto_rules[:5])
        preset = cfg.get('preset', 'F')
        
        try:
            try:
                trade_hist = build_trade_summary(30)
            except:
                trade_hist = ''
            
            # ★★★ v8.0: 종목별 상세 실적 (매수/매도 매칭) ★★★
            _trade_details = []
            for s in sells[:20]:
                _stk = s.get('ticker','')
                _buy_match = next((b for b in reversed(buys) if b.get('ticker')==_stk), None)
                _buy_price = float(_buy_match.get('price',0)) if _buy_match else float(s.get('avg_price',0))
                _sell_price = float(s.get('price',0))
                _pnl = float(s.get('pnl',0) or 0)
                _pnl_pct = float(s.get('pnl_pct',0) or 0)
                _reason = s.get('reason','')[:40]
                _hold_min = s.get('stock_data',{}).get('hold_minutes',0) or 0
                _trade_details.append(f"  {s.get('name','?')}({_stk}) 매수₩{_buy_price:,.0f}→매도₩{_sell_price:,.0f} {'+'if _pnl>=0 else ''}₩{_pnl:,.0f}({_pnl_pct:+.1f}%) {int(_hold_min)}분보유 [{_reason}]")
            _trade_detail_str = '\n'.join(_trade_details) if _trade_details else '매도 없음'
            
            # ★★★ v8.0: AI 매도 vs 수동 매도 비교 ★★★
            _ai_sells = [s for s in sells if s.get('sell_mode')=='ai']
            _manual_sells = [s for s in sells if s.get('sell_mode')!='ai']
            _ai_wins = sum(1 for s in _ai_sells if float(s.get('pnl',0) or 0)>0)
            _ai_total_pnl = sum(float(s.get('pnl',0) or 0) for s in _ai_sells)
            _man_wins = sum(1 for s in _manual_sells if float(s.get('pnl',0) or 0)>0)
            _man_total_pnl = sum(float(s.get('pnl',0) or 0) for s in _manual_sells)
            _sell_comparison = f"AI매도 {len(_ai_sells)}건(승{_ai_wins} PnL{'+'if _ai_total_pnl>=0 else ''}₩{_ai_total_pnl:,.0f}) / 수동매도 {len(_manual_sells)}건(승{_man_wins} PnL{'+'if _man_total_pnl>=0 else ''}₩{_man_total_pnl:,.0f})"
            
            # ★★★ v8.0: 시간대별 매수 성과 ★★★
            _time_perf = {}
            for b in buys:
                _bh = b.get('time','')[:13]  # YYYY-MM-DDTHH
                if _bh:
                    _hr = _bh[-2:]
                    _time_perf.setdefault(_hr, {'buys':0,'pnl':0})
                    _time_perf[_hr]['buys'] += 1
            for s in sells:
                _sh = s.get('time','')[:13]
                if _sh:
                    _hr = _sh[-2:]
                    if _hr in _time_perf:
                        _time_perf[_hr]['pnl'] += float(s.get('pnl',0) or 0)
            _time_perf_str = ' / '.join(f"{h}시:{v['buys']}건{'+'if v['pnl']>=0 else ''}₩{v['pnl']:,.0f}" for h,v in sorted(_time_perf.items())) if _time_perf else '데이터 없음'
            
            # ★★★ v8.0: 전체 손익 계산 ★★★
            _total_realized = sum(float(s.get('pnl',0) or 0) for s in sells)
            _total_wins = sum(1 for s in sells if float(s.get('pnl',0) or 0)>0)
            _total_losses = sum(1 for s in sells if float(s.get('pnl',0) or 0)<0)
            _win_rate = round(_total_wins/max(_total_wins+_total_losses,1)*100)
            _avg_win = round(sum(float(s.get('pnl',0) or 0) for s in sells if float(s.get('pnl',0) or 0)>0)/max(_total_wins,1))
            _avg_loss = round(abs(sum(float(s.get('pnl',0) or 0) for s in sells if float(s.get('pnl',0) or 0)<0))/max(_total_losses,1))
            _pf = round(_avg_win*_total_wins / max(_avg_loss*_total_losses, 1), 2)
            
            print(f"[CLOSING] 실적 데이터 수집 완료 (매수{len(buys)} 매도{len(sells)} 실현{'+'if _total_realized>=0 else ''}₩{_total_realized:,.0f})")
            
            prompt = f"""{trade_hist}

★★★ 오늘 매매 실적 상세 리포트 ★★★

[매매 총괄]
자동매수: {len(buys)}건 / 자동매도: {len(sells)}건
수동매수: {len(manual_buys)}건 / 수동매도: {len(manual_sells)}건
물타기: {len(avg_downs)}건 / 차단: {len(blocked)}건 / 오류: {len(errors)}건

[실적 요약]
실현손익: {'+'if _total_realized>=0 else ''}₩{_total_realized:,.0f}
승률: {_win_rate}% ({_total_wins}승 {_total_losses}패)
평균수익: +₩{_avg_win:,} / 평균손실: -₩{_avg_loss:,}
Profit Factor: {_pf}
{_sell_comparison}

[종목별 상세]
{_trade_detail_str}

[시간대별 성과]
{_time_perf_str}

[AI 스캔 활동]
총 스캔: {len(scans)}회
시간대별: {scan_summary}
{_scan_perf_summary}

분석해줘:
1. 오늘 매매 성과 종합 평가 (승률/PF/손익비 기준)
2. 종목별 매매 복기 — 잘한 매매 vs 아쉬운 매매 구체적으로
3. AI매도 vs 수동매도 비교 분석 (어느 쪽이 더 효과적이었나)
4. 시간대별 분석 — 언제 매수한 게 가장 수익이 좋았나
5. 매도 타이밍 평가 — 너무 일찍/늦게 판 건 없나 (보유시간 기준)
6. 손절 분석 — 손절 종목들의 공통점 (진입 시점, 종목 특성)
7. [핵심] 시스템 고도화 제안:
   - 익절/손절 비율 조정 필요?
   - 종목 수 변경 필요?
   - 스캔 빈도 조정 필요? (현재 {len(scans)}회/일)
   - AI 종목 선별 기준 개선점?
   - 특정 시간대 매매 패턴 변경?
8. 내일 매매 시 기억할 교훈 3가지

JSON만 반환:
{{"trade_performance":{{"total_realized":"{_total_realized:+,.0f}","win_rate":"{_win_rate}%","profit_factor":{_pf},"total_trades":{len(buys)+len(sells)},"auto_buys":{len(buys)},"auto_sells":{len(sells)},"manual_buys":{len(manual_buys)},"manual_sells":{len(manual_sells)},"blocked":{len(blocked)},"errors":{len(errors)},"avg_win":"+₩{_avg_win:,}","avg_loss":"-₩{_avg_loss:,}","evaluation":"성과 평가 3줄","good_trades":["잘한 매매 종목+이유"],"bad_trades":["아쉬운 매매 종목+이유"],"ai_vs_manual":"AI매도 vs 수동매도 비교 2줄","best_hour":"가장 수익 좋은 시간대","worst_hour":"가장 수익 나쁜 시간대","sell_timing":"매도 타이밍 평가 2줄","sl_analysis":"손절 종목 공통점 분석"}},"scan_activity":{{"total_scans":{len(scans)},"efficiency":"효율성 평가 1줄","recommendation":"스캔 빈도 조정 제안"}},"system_optimization":{{"overall_grade":"A~F","suggestions":[{{"category":"익절/손절/종목수/스캔/종목선별/시간대","current":"현재 설정","suggested":"권장 변경","reason":"변경 이유"}}],"priority_change":"가장 우선 변경사항 1줄","keep_settings":["유지해야 할 좋은 설정"]}},"lessons":["내일 기억할 교훈1","교훈2","교훈3"]}}"""

            print(f"[CLOSING] 3단계: AI 실적분석 시작 (web_search=False)...")
            ai_text = call_ai(prompt, "한국 주식 자동매매 실적 분석 전문가. 매매 성과 평가 + 시스템 개선점 제안. JSON만 반환.", 1500, web_search=False, tier='briefing')
            print(f"[CLOSING] 4단계: AI 응답 수신 ({len(ai_text)}자)")
            json_match = re.search(r'\{[\s\S]*\}', ai_text)
            
            if json_match:
                closing_data = safe_json_loads(json_match.group(), "CLOSING_BRIEFING")
                daily_briefing['closing'] = closing_data
                daily_briefing['closing_at'] = datetime.now().isoformat()
                daily_briefing['closing_done'] = True
                save_state()
                save_briefing_history()  # Save for next-day AI handover
                
                tp = closing_data.get('trade_performance', {})
                so = closing_data.get('system_optimization', {})
                lessons = closing_data.get('lessons', [])
                
                trade_log.append({"time": datetime.now().isoformat(), "date": today,
                    "type": "CLOSING_BRIEFING", "message": f"📊 실적: {tp.get('total_realized','?')} 승률{tp.get('win_rate','?')} PF{tp.get('profit_factor','?')}"})
                trade_log.append({"time": datetime.now().isoformat(), "date": today,
                    "type": "CLOSING_BRIEFING", "message": f"📊 매매성과: 매수{tp.get('auto_buys',0)}건 매도{tp.get('auto_sells',0)}건 | {tp.get('evaluation','')}"})
                if tp.get('good_trades'):
                    trade_log.append({"time": datetime.now().isoformat(), "date": today,
                        "type": "CLOSING_BRIEFING", "message": f"✅ 잘한 매매: {'; '.join(tp['good_trades'][:3])}"})
                if tp.get('bad_trades'):
                    trade_log.append({"time": datetime.now().isoformat(), "date": today,
                        "type": "CLOSING_BRIEFING", "message": f"⚠️ 아쉬운 매매: {'; '.join(tp['bad_trades'][:3])}"})
                if tp.get('ai_vs_manual'):
                    trade_log.append({"time": datetime.now().isoformat(), "date": today,
                        "type": "CLOSING_BRIEFING", "message": f"🧠 AI vs 수동: {tp['ai_vs_manual']}"})
                if lessons:
                    trade_log.append({"time": datetime.now().isoformat(), "date": today,
                        "type": "CLOSING_BRIEFING", "message": f"📝 내일 교훈: {' / '.join(lessons[:3])}"})
                
                # System optimization logs
                if so.get('suggestions'):
                    trade_log.append({"time": datetime.now().isoformat(), "date": today,
                        "type": "CLOSING_BRIEFING", "message": f"⚙️ 시스템 등급: {so.get('overall_grade','?')} | 우선변경: {so.get('priority_change','없음')}"})
                    for sug in so.get('suggestions', [])[:3]:
                        trade_log.append({"time": datetime.now().isoformat(), "date": today,
                            "type": "CLOSING_BRIEFING", "message": f"🔧 [{sug.get('category','')}] {sug.get('current','')} -> {sug.get('suggested','')} ({sug.get('reason','')})"})
                
                # ★ 텔레그램 마감 실적 알림
                _cs = f"📊 마감 실적 리포트\n"
                _cs += f"실현손익: {tp.get('total_realized','?')}\n"
                _cs += f"승률: {tp.get('win_rate','?')} / PF: {tp.get('profit_factor','?')}\n"
                _cs += f"매수{tp.get('auto_buys',0)}건 매도{tp.get('auto_sells',0)}건"
                if lessons: _cs += f"\n📝 교훈: {lessons[0]}"
                tg_briefing('📊 15:25 마감 실적 리포트', _cs)
                
                # ★ v3.0: 장마감 자동 일일 수익 리포트 (마감 브리핑 직후)
                try:
                    threading.Thread(target=_tg_daily_report, daemon=True).start()
                except: pass
                
                # ★ v3.0 I: 금요일 주간 리포트 자동 전송
                try:
                    if datetime.now().weekday() == 4:  # 금요일
                        import time as _t2; _t2.sleep(5)  # 일일 리포트 후 5초 대기
                        threading.Thread(target=_tg_weekly_report, daemon=True).start()
                        print("[WEEKLY] 금요일 주간 리포트 전송 예약")
                except: pass
                
                # ★ v3.0 I: 월말 월간 리포트 자동 전송
                try:
                    _tomorrow = datetime.now() + timedelta(days=1)
                    if _tomorrow.month != datetime.now().month:  # 내일이 다음 달 = 오늘이 월말
                        import time as _t3; _t3.sleep(10)
                        threading.Thread(target=_tg_monthly_report, daemon=True).start()
                        print("[MONTHLY] 월말 월간 리포트 전송 예약")
                except: pass
                
                # ★★★ v4.0 PHASE 4: 마감 후 파라미터 자동최적화 ★★★
                try:
                    print("[PARAM_OPT] 마감 브리핑 후 파라미터 최적화 시작...")
                    _opt_result = auto_apply_optimal_params(self.config)
                    if _opt_result:
                        _opt_msg = '\n'.join(_opt_result) if isinstance(_opt_result, list) else str(_opt_result)
                        trade_log.append({
                            "time": datetime.now().isoformat(), "date": today,
                            "type": "PARAM_OPTIMIZE",
                            "message": f"🔧 내일 파라미터 최적화: {_opt_msg}"
                        })
                    # 캐시에 결과 저장 (다음 날 AI 프롬프트 주입용)
                    _param_optimize_cache['data'] = analyze_optimal_params(14)
                    _param_optimize_cache['ts'] = time.time()
                except Exception as _ope:
                    print(f"[PARAM_OPT] 최적화 실패: {_ope}")
                
                # ★★★ v4.0 PHASE 5: 매매일지 자동생성 ★★★
                try:
                    print("[JOURNAL] 매매일지 자동생성 시작...")
                    _journal = generate_daily_journal()
                    if _journal and _journal.get('summary'):
                        trade_log.append({
                            "time": datetime.now().isoformat(), "date": today,
                            "type": "JOURNAL",
                            "message": f"📝 매매일지: {_journal['summary']}"
                        })
                        # ★★★ F1: 매매일지 → briefing_history 저장 (다음 날 인수인계용) ★★★
                        daily_briefing['journal'] = _journal
                        save_briefing_history()
                        print(f"[JOURNAL] ✅ 완료 + briefing_history 저장 (다음 날 인수인계 연결)")
                except Exception as _je:
                    print(f"[JOURNAL] 생성 실패: {_je}")
                
                save_state()
                
                # ★ 종가 업데이트는 15:35에 별도 실행 (장 마감 후 정확한 종가 반영)
                
        except Exception as e:
            import traceback; traceback.print_exc()
            trade_log.append({"time": datetime.now().isoformat(), "date": today,
                "type": "CLOSING_ERROR", "message": f"마감 시황 오류: {str(e)}"})
            daily_briefing['closing_done'] = False  # 재시도 가능
            save_state()
    
    def _check_daily_target(self):
        """일일 손익 한도 체크 - 수익목표/손실한도 (% 및 금액 기준)"""
        cfg = self.config
        today = datetime.now().strftime('%Y-%m-%d')

        # ★ v3.0 FIX: 시작 후 3분 grace period (기존 보유종목 정리 시간)
        # 기존 종목 매도 → baseline 재계산 완료까지 대기
        _start_ts = getattr(self, '_start_timestamp', 0)
        if _start_ts and time.time() - _start_ts < 180:  # 3분
            return

        # 이미 DAILY_STOP이면 패스
        if any(t.get('date') == today and t.get('type') == 'DAILY_STOP' for t in trade_log):
            return

        daily_loss_pct    = float(cfg.get('daily_loss_limit', 3) or 0)
        daily_target_pct  = float(cfg.get('daily_target', 10) or 0)
        daily_target_amt  = float(cfg.get('daily_target_amt', 0) or 0)
        daily_loss_amt    = float(cfg.get('daily_loss_amt', 0) or 0)
        target_liquidate  = bool(cfg.get('daily_target_liquidate', False))
        loss_liquidate    = bool(cfg.get('daily_loss_liquidate', False))

        # 아무것도 설정 안 됐으면 패스
        if not daily_loss_pct and not daily_target_pct and not daily_target_amt and not daily_loss_amt:
            return

        try:
            mode = 'live'

            # KIS 모의/실전: trade_log 기반 오늘 실현손익 사용
            if True:
                app_key = cfg.get('app_key')
                app_secret = cfg.get('app_secret')
                token = kis_get_token(app_key, app_secret, mode)
                bal = get_balance(app_key, app_secret, mode, token, cfg['account'], cfg.get('account_cd','01'))
                total_asset = float(bal.get('output2', [{}])[0].get('tot_evlu_amt', '0')) or 1
                auto_pnl, _, _ = calc_auto_pnl_today(bal.get('output1', []), today)

            # ★★★ v8.0: 오늘 실현손익 그대로 사용 (session_baseline 제거 — 버그 근원이었음) ★★★
            _today_realized = sum(float(t.get('pnl', 0) or 0) for t in trade_log 
                if t.get('date') == today and t.get('type') in ('SELL','FORCE_CLOSE','AI_SELL') 
                and t.get('success') in (True, 1, 'true'))
            session_pnl = _today_realized  # ★ 오늘 실현손익 그대로 (baseline 차감 없음)
            pnl_pct = session_pnl / total_asset * 100 if total_asset else 0
            
            if abs(session_pnl) > 0:
                if not hasattr(self, '_daily_log_ts') or time.time() - self._daily_log_ts > 60:
                    self._daily_log_ts = time.time()
                    print(f"[DAILY_TARGET] 오늘 실현손익: ₩{_today_realized:,.0f} ({pnl_pct:.2f}%)")

            def _liquidate_all(reason):
                """전종목 강제청산 - KIS 잔고 기준"""
                app_key = cfg.get('app_key','')
                app_secret = cfg.get('app_secret','')
                token2 = kis_get_token(app_key, app_secret, mode) if app_key else ''
                # KIS 잔고에서 auto_tickers에 있는 종목만 청산
                try:
                    bal = get_balance(app_key, app_secret, mode, token2, cfg.get('account',''), cfg.get('account_cd','01'))
                    for pos in bal.get('output1', []):
                        tkr = pos.get('pdno','')
                        if tkr not in auto_tickers: continue
                        # v8: 모든 auto_tickers 보유종목 강제청산 대상
                        if tkr in perm_blocked: continue
                        qty = int(pos.get('hldg_qty', 0))
                        if qty <= 0: continue
                        cp = float(pos.get('prpr', 0)) or float(pos.get('pchs_avg_pric', 0))
                        _avg = float(pos.get('pchs_avg_pric', 0) or 0)
                        try:
                            self._execute_sell(cfg, token2, tkr, pos.get('prdt_name', tkr), qty, cp, reason, avg_price=_avg)
                            print(f"[LIQUIDATE] {tkr} {qty}주 청산 (평단{_avg:,.0f} → 현재{cp:,.0f})")
                        except Exception as _le:
                            print(f"[LIQUIDATE] {tkr} 청산 실패: {_le}")
                    # ★ v3.0 FIX: 강제청산 후 잔고 캐시 강제 무효화
                    _bal_cache['ts'] = 0
                    print(f"[LIQUIDATE] 잔고 캐시 무효화 → 다음 조회 시 신선한 데이터")
                except Exception as _le:
                    print(f"[LIQUIDATE] 잔고조회 실패: {_le}")

            target_liq_buf = float(cfg.get('target_liq_buf', 10) or 0)
            loss_liq_buf   = float(cfg.get('loss_liq_buf', 10) or 0)

            # ── 수익 목표 도달 체크 (세션 기준) ──
            # ★★★ v8.0: 실현손익이 마이너스면 수익목표 절대 발동 안 함 (오청산 방지) ★★★
            target_hit = False
            target_msg = ''
            if _today_realized < 0:
                if not hasattr(self, '_target_skip_log_ts') or time.time() - self._target_skip_log_ts > 300:
                    self._target_skip_log_ts = time.time()
                    print(f"[DAILY_TARGET] ⏸ 실현손익 ₩{_today_realized:,.0f} < 0 → 수익목표 체크 건너뜀 (오청산 방지)")
            elif daily_target_amt > 0:
                # 버퍼 적용: 목표금액 × (1 + 버퍼%)
                trigger = daily_target_amt * (1 + target_liq_buf / 100)
                if session_pnl >= trigger:
                    target_hit = True
                    buf_str = f" (버퍼+{target_liq_buf:.0f}%)" if target_liq_buf > 0 else ""
                    _sess_label = ""
                    target_msg = f"🎯 수익목표 달성{buf_str}! +₩{int(session_pnl):,} >= ₩{int(trigger):,} (목표₩{int(daily_target_amt):,}){_sess_label}"
            elif daily_target_pct > 0:
                # ★ v8.0: % 목표에도 버퍼 적용
                _pct_trigger = daily_target_pct * (1 + target_liq_buf / 100)
                if pnl_pct >= _pct_trigger:
                    target_hit = True
                    buf_str = f" (버퍼+{target_liq_buf:.0f}%)" if target_liq_buf > 0 else ""
                    target_msg = f"🎯 수익목표 달성{buf_str}! {pnl_pct:.2f}% >= +{_pct_trigger:.1f}% (목표{daily_target_pct}%)"

            if target_hit:
                if target_liquidate:
                    _liquidate_all("🎯 수익목표 달성 강제청산")
                    target_msg += " → 전종목 강제청산 완료 | 스캔·매수 중단"
                    pass  # v8: session_baseline 미사용
                else:
                    target_msg += " → 신규매수 중단 (AI 매도 판단은 계속)"
                trade_log.append({"time": datetime.now().isoformat(), "date": today, "mode": "live",
                    "type": "DAILY_STOP", "message": target_msg})
                tg_send(f"🎯 <b>수익목표 달성</b>\n{target_msg}\n⏰ {datetime.now().strftime('%H:%M')}",
                    buttons=[[('🔄 재시작', 'cmd_restart'), ('📊 현황', 'cmd_status')]])
                save_state()
                if target_liquidate:
                    self.stop()  # 전량 청산이면 완전 중단
                else:
                    self.paused = True  # ★ 매수만 중단, 매도 판단 계속
                return

            # ── 손실 한도 도달 체크 (세션 기준) ──
            loss_hit = False
            loss_msg = ''
            if daily_loss_amt > 0:
                trigger_loss = daily_loss_amt * (1 + loss_liq_buf / 100)
                if session_pnl <= -trigger_loss:
                    loss_hit = True
                    buf_str = f" (버퍼+{loss_liq_buf:.0f}%)" if loss_liq_buf > 0 else ""
                    _sess_label = ""
                    loss_msg = f"🛑 손실한도 도달{buf_str}! ₩{int(session_pnl):,} <= -₩{int(trigger_loss):,} (한도₩{int(daily_loss_amt):,}){_sess_label}"
            elif daily_loss_pct > 0 and session_pnl < 0 and abs(pnl_pct) >= daily_loss_pct:
                loss_hit = True
                loss_msg = f"🛑 손실한도 도달! {pnl_pct:.2f}% >= -{daily_loss_pct}%"

            if loss_hit:
                if loss_liquidate:
                    _liquidate_all("🚨 손실한도 도달 강제청산")
                    loss_msg += " → 전종목 강제청산 완료 | 스캔·매수 중단"
                else:
                    loss_msg += " → 신규매수 중단 (AI 매도 판단은 계속)"
                trade_log.append({"time": datetime.now().isoformat(), "date": today, "mode": "live",
                    "type": "DAILY_STOP", "message": loss_msg})
                tg_send(f"🚨 <b>손실한도 도달</b>\n{loss_msg}\n⏰ {datetime.now().strftime('%H:%M')}",
                    buttons=[[('🔄 재시작', 'cmd_restart'), ('📊 현황', 'cmd_status')]])
                save_state()
                # ★ v3.0 FIX: self.stop() 대신 paused → AI 매도/손절은 계속 작동
                # 완전 중단하면 보유종목 손절/매도가 안 되서 손실이 커짐
                if not loss_liquidate:
                    self.paused = True  # 매수만 중단, 매도/스캔 계속
                    print(f"[DAILY_TARGET] 손실한도 → paused (매도 판단 계속, 매수만 중단)")
                else:
                    self.stop()  # 강제청산 선택 시에만 완전 중단

        except Exception as _e:
            print(f"[DAILY_TARGET] 체크 오류: {_e}")
    

    
    def _check_rules(self):
        cfg = self.config
        # ★★★ v8.0: 단타 미시작 시 telegram_config에서 KIS 키 폴백 ★★★
        if not cfg.get('app_key') and telegram_config.get('app_key'):
            cfg = {**cfg, 'app_key': telegram_config.get('app_key',''),
                   'app_secret': telegram_config.get('app_secret',''),
                   'account': telegram_config.get('account',''),
                   'account_cd': telegram_config.get('account_cd','01')}
        app_key = cfg.get('app_key')
        app_secret = cfg.get('app_secret')
        mode = 'live'
        today = datetime.now().strftime('%Y-%m-%d')  # ★ v8.0: 물타기/눌림감지 trade_log용
        acnt = cfg.get('account', '')
        acnt_cd = cfg.get('account_cd', '01')
        
        if not all([app_key, app_secret, acnt]):
            return
        
        # ★ Fix: 장외 시간(15:30~08:40)에는 규칙 체크 스킵
        _hm = datetime.now().hour * 100 + datetime.now().minute
        if _hm > 1530 or _hm < 840:
            return
        
        # ★ 매도 = 사용자 설정값 그대로 (3차 익절 시스템)
        
        try:
            token = kis_get_token(app_key, app_secret, mode)
        except:
            return

        # Get current positions (mock/live)
        tr_id = "TTTC8434R"
        try:
            positions = get_balance(app_key, app_secret, mode, token, acnt, acnt_cd, max_age=10)  # ★ v8.0: 10초 캐시 (실시간 가격 갱신)
        except:
            return
        
        output1 = positions.get('output1', [])
        self._last_output1 = output1  # ★ v6.0: scan_loop에서 swing 정리용 캐시
        
        # ★★★ v8.0: 모든 보유종목 = auto_tickers 하나로 관리 ★★★
        
        for _pos_chk in output1:
            _tk_chk = _pos_chk.get('pdno', '')
            _qty_chk = int(_pos_chk.get('hldg_qty', '0') or 0)
            if _qty_chk > 0 and _tk_chk and _tk_chk not in auto_tickers and _tk_chk not in perm_blocked:
                auto_tickers.append(_tk_chk)
                _nm_chk = ensure_name(_tk_chk, _pos_chk.get('prdt_name', ''))
                print(f"[RULES_SAFETY] ⚠️ {_nm_chk}({_tk_chk}) auto_tickers에 누락 → 자동 등록 (매도 관리 시작)")
                trade_log.append({
                    "time": datetime.now().isoformat(),
                    "date": datetime.now().strftime('%Y-%m-%d'),
                    "type": "SYSTEM",
                    "message": f"⚠️ {_nm_chk}({_tk_chk}) 보유중인데 미등록 → 자동 등록 (손절/익절 관리 시작)"
                })
                # peak_prices도 설정
                _cur_chk = float(_pos_chk.get('prpr', '0') or 0)
                if _cur_chk > 0 and _tk_chk not in peak_prices:
                    peak_prices[_tk_chk] = _cur_chk
                save_state()
        
        # Only check positions that were bought by auto-trading
        active_auto = set()
        sold_this_cycle = set()  # ★ 이번 루프에서 이미 매도 주문 낸 종목 (중복 rule hit 방지)
        _avg_done_this_cycle = set()  # ★ v8.0: 이번 루프에서 물타기 한 종목 (이중 실행 방지)
        scalp = self._is_scalp_mode()
        
        # ★ v8.0: AI매도 모드 삭제 — 설정값 기계적 매도만 사용
        for pos in output1:
            ticker = pos.get('pdno', '')
            name = ensure_name(ticker, pos.get('prdt_name', ''))
            qty = int(pos.get('hldg_qty', '0'))
            avg_price = float(pos.get('pchs_avg_pric', '0'))
            # ★ v4.0: 현재가 조회 우선순위
            #    1순위: KIS WebSocket 실시간 (0.1초 지연)
            #    2순위: KIS REST inquire-price (0.3초 지연)
            #    3순위: 잔고 prpr (최대 2분 지연)
            bal_price = float(pos.get('prpr', '0'))
            ws_price = get_realtime_price(ticker) if ticker in auto_tickers else 0
            if ws_price > 0:
                cur_price = ws_price
            elif ticker in auto_tickers:
                try:
                    pd = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
                        app_key, app_secret, mode, token, "FHKST01010100",
                        params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker})
                    _pd_out = pd.get('output', {})
                    realtime = int(_pd_out.get('stck_prpr', '0'))
                    cur_price = realtime if realtime > 0 else bal_price
                    # ★★★ v8.0: 장중 거래정지 자동 감지 (시가=0, 거래량=0) ★★★
                    _hm_now = datetime.now().hour * 100 + datetime.now().minute
                    if _hm_now >= 905:
                        _oprc = int(_pd_out.get('stck_oprc', '0') or 0)
                        _vol = int(_pd_out.get('acml_vol', '0') or 0)
                        if _oprc == 0 and _vol == 0 and not _is_truly_halted(ticker):
                            perm_blocked[ticker] = f"HALTED: 거래정지 (시가=0, 거래량=0, 장중감지)"
                            print(f"[RULES] ⛔ {name}({ticker}) 거래정지 감지 → HALTED 등록")
                            save_state()
                except Exception:
                    cur_price = bal_price
            else:
                cur_price = bal_price

            if qty <= 0 or avg_price <= 0 or cur_price <= 0:
                continue
            
            # ★ v8.0: 모든 보유종목은 auto_tickers 기준
            if ticker not in auto_tickers:
                continue
            active_auto.add(ticker)
            
            pnl_pct = ((cur_price - avg_price) / avg_price) * 100
            # ★ v5.0: 비용 차감 순수익률로 매도 판단 (0.3% 매도비용 반영)
            _cost_pct = float(cfg.get('trade_cost_rate', 0.3)) if auto_trader.running else 0.3
            pnl_pct -= _cost_pct  # 3.5% → 3.2% (비용 0.3% 차감)
            
            # Update peak price for trailing stop
            if ticker not in peak_prices or cur_price > peak_prices[ticker]:
                peak_prices[ticker] = cur_price
            
            # ★ 이번 루프에서 이미 매도한 종목 skip (rule 중복 hit 방지)
            if ticker in sold_this_cycle:
                continue
            
            # ★ v3.0 근본 FIX: recently_sold 쿨다운 체크 (매도 시도 전에!)
            # 이걸 _execute_sell 안에서만 체크하면 매 사이클(2초)마다 로그 폭주
            _last_sell = recently_sold.get(ticker, 0)
            if time.time() - _last_sell < SELL_COOLDOWN:
                continue  # 조용히 skip (로그 안 남김)
            
            # ★★★ v8.0: 진짜 거래정지만 매매 제외 (스캔제외는 매도/물타기 정상) ★★★
            if _is_truly_halted(ticker):
                continue

            # ★★★ 3차 익절 시스템 ★★★
            _tp1 = float(cfg.get('tp1', 3.5))
            _tp2 = float(cfg.get('tp2', 5.5))
            _tp3 = float(cfg.get('tp3', 8))
            _sl_val = float(cfg.get('sl', 0))  # ★ v8.0: 기본값 0 = 손절없음
            
            # 매도 단계 추적
            _stage = _sell_stage.get(ticker, 0)
            _dipped = _dip_flag.get(ticker, False)
            
            # ═══ 매도 엔진 (설정값 기반 기계적 매도) ═══
            
            # ── ★ v6.0: 익절 시작된 종목은 물타기 안 함, 복귀청산 우선 ──
            _stage = _sell_stage.get(ticker, 0)
            _dipped = _dip_flag.get(ticker, False)
            
            # ── tp3 도달 = 무조건 잔량 전부 매도 (최우선) ──
            if pnl_pct >= _tp3:
                self._execute_sell(cfg, token, ticker, name, qty, cur_price,
                    f"🎯 3차 익절 ({pnl_pct:.1f}% >= {_tp3}%) 전량매도", avg_price=avg_price)
                sold_this_cycle.add(ticker)
                _sell_stage.pop(ticker, None)
                auto_avg_count.pop(ticker, None)
            
            # ── ★ v6.0: 익절 규칙 (설정: 전량매도 or 분할익절) ──
            elif _stage == 0:
                _tp_mode = cfg.get('tp_mode', 'full')  # 'full'=1차전량 / 'split'=분할
                if pnl_pct >= _tp1:
                    print(f"[TP1_CHECK] {name}({ticker}) tp_mode='{_tp_mode}' pnl={pnl_pct:.1f}% >= tp1={_tp1}% qty={qty}")
                    if _tp_mode == 'full':
                        # ★ 1차 전량매도 모드
                        print(f"[TP1] {name}({ticker}) 전량{qty}주 매도 (tp_mode=full)")
                        self._execute_sell(cfg, token, ticker, name, qty, cur_price,
                            f"✅ 1차 익절 ({pnl_pct:.1f}% >= {_tp1}%) 전량{qty}주", avg_price=avg_price)
                        sold_this_cycle.add(ticker)
                        _sell_stage.pop(ticker, None)
                        auto_avg_count.pop(ticker, None)
                    else:
                        # ★ 분할매도 모드 (기존)
                        _tp1_ratio = int(cfg.get('tp1_ratio', 40)) / 100
                        sell_qty = max(1, int(qty * _tp1_ratio))
                        print(f"[TP1] {name}({ticker}) qty={qty} → {int(_tp1_ratio*100)}%={sell_qty} (stage 0→1)")
                        self._execute_sell(cfg, token, ticker, name, sell_qty, cur_price,
                            f"✅ 1차 익절 ({pnl_pct:.1f}% >= {_tp1}%) {sell_qty}주/{qty}주", avg_price=avg_price)
                        sold_this_cycle.add(ticker)
                        _sell_stage[ticker] = 1
                        _bal_cache['ts'] = 0
            
            elif _stage == 1:
                _tp_mode_s1 = cfg.get('tp_mode', 'full')
                if _tp_mode_s1 == 'full':
                    # ★ 전량매도 모드인데 stage=1(이전 분할잔량) → 즉시 전량 청산
                    print(f"[TP_FIX] {name}({ticker}) tp_mode=full인데 stage=1 → 잔량{qty}주 즉시 청산")
                    self._execute_sell(cfg, token, ticker, name, qty, cur_price,
                        f"📉 전량모드 잔량청산 ({pnl_pct:.1f}%) {qty}주", avg_price=avg_price)
                    sold_this_cycle.add(ticker)
                    _sell_stage.pop(ticker, None)
                    auto_avg_count.pop(ticker, None)
                elif pnl_pct >= _tp2:
                    _tp2_ratio = int(cfg.get('tp2_ratio', 30)) / 100
                    sell_qty = max(1, int(qty * (_tp2_ratio / max(1 - int(cfg.get('tp1_ratio', 40))/100, 0.1))))
                    sell_qty = min(sell_qty, qty)
                    self._execute_sell(cfg, token, ticker, name, sell_qty, cur_price,
                        f"✅ 2차 익절 ({pnl_pct:.1f}% >= {_tp2}%) {sell_qty}주/{qty}주", avg_price=avg_price)
                    sold_this_cycle.add(ticker)
                    _sell_stage[ticker] = 2
                    _bal_cache['ts'] = 0
                elif pnl_pct <= _tp1:
                    # tp1까지 내려옴 → 나머지 전량 청산
                    print(f"[TP1_RETURN] {name}({ticker}) stage=1 pnl={pnl_pct:.1f}%<={_tp1}% → 전량{qty}주 매도")
                    self._execute_sell(cfg, token, ticker, name, qty, cur_price,
                        f"📉 1차후 복귀청산 ({pnl_pct:.1f}% <= {_tp1}%) 전량{qty}주", avg_price=avg_price)
                    sold_this_cycle.add(ticker)
                    _sell_stage.pop(ticker, None)
                    auto_avg_count.pop(ticker, None)
            
            elif _stage == 2:
                _tp_mode_s2 = cfg.get('tp_mode', 'full')
                if _tp_mode_s2 == 'full':
                    print(f"[TP_FIX] {name}({ticker}) tp_mode=full인데 stage=2 → 잔량{qty}주 즉시 청산")
                    self._execute_sell(cfg, token, ticker, name, qty, cur_price,
                        f"📉 전량모드 잔량청산 ({pnl_pct:.1f}%) {qty}주", avg_price=avg_price)
                    sold_this_cycle.add(ticker)
                    _sell_stage.pop(ticker, None)
                    auto_avg_count.pop(ticker, None)
                elif pnl_pct <= _tp2:
                    self._execute_sell(cfg, token, ticker, name, qty, cur_price,
                        f"📉 2차후 복귀청산 ({pnl_pct:.1f}% <= {_tp2}%) 전량{qty}주", avg_price=avg_price)
                    sold_this_cycle.add(ticker)
                    _sell_stage.pop(ticker, None)
                    auto_avg_count.pop(ticker, None)
                    print(f"[TP] {ticker} 2차후 눌림 감지: {pnl_pct:.1f}% <= {_tp2:.1f}%")
            
            # ── ★★★ v8.1: 물타기 3차 (stage=0일 때만) ★★★
            _auto_avg_on_dbg = bool(cfg.get('auto_avg_down', True))
            _auto_avg_cnt_dbg = auto_avg_count.get(ticker, 0)
            _avg_settings_dbg = [
                float(cfg.get('avg_pct1', cfg.get('auto_avg_pct', -4))),
                float(cfg.get('avg_pct2', 0)),
                float(cfg.get('avg_pct3', 0)),
            ]
            _cur_pct_dbg = _avg_settings_dbg[_auto_avg_cnt_dbg] if _auto_avg_cnt_dbg < 3 else 0
            if _cur_pct_dbg < 0 and pnl_pct <= _cur_pct_dbg and _auto_avg_cnt_dbg < 3 and _auto_avg_on_dbg:
                _blocked_reasons = []
                if ticker in sold_this_cycle: _blocked_reasons.append('이번사이클매도')
                if ticker in _avg_done_this_cycle: _blocked_reasons.append('이번사이클물타기완료')
                if _stage != 0: _blocked_reasons.append(f'stage={_stage}(익절진행중)')
                if _is_truly_halted(ticker): _blocked_reasons.append('거래정지')
                _avg_last_dbg = auto_avg_last_ts.get(ticker, 0)
                if (time.time() - _avg_last_dbg) < 120: _blocked_reasons.append(f'쿨다운{120-(time.time()-_avg_last_dbg):.0f}초')
                if _blocked_reasons:
                    if not hasattr(self, '_avg_full_dbg') or time.time() - getattr(self, '_avg_full_dbg', 0) > 60:
                        self._avg_full_dbg = time.time()
                        print(f"[AVG_DBG] {name}({ticker}) 물타기 대상({pnl_pct:.1f}%<={_cur_pct_dbg}%) 차단사유: {', '.join(_blocked_reasons)}")
                else:
                    if not hasattr(self, '_avg_try_dbg') or time.time() - getattr(self, '_avg_try_dbg', 0) > 30:
                        self._avg_try_dbg = time.time()
                        print(f"[AVG_DBG] {name}({ticker}) 물타기 조건 충족! pnl={pnl_pct:.1f}% cnt={_auto_avg_cnt_dbg} stage={_stage} → 실행 시도")
            
            if ticker not in sold_this_cycle and ticker not in _avg_done_this_cycle and _stage == 0:
                _auto_avg_on = bool(cfg.get('auto_avg_down', True))
                _auto_avg_cnt = auto_avg_count.get(ticker, 0)
                _avg_last = auto_avg_last_ts.get(ticker, 0)
                _avg_cooldown_ok = (time.time() - _avg_last) >= 120
                # ★★★ v8.1: 물타기 3차 (회차별 독립 진입선+비율) ★★★
                _max_avg = 3  # 최대 3회
                _avg_settings = [
                    (float(cfg.get('avg_pct1', cfg.get('auto_avg_pct', -4))), int(cfg.get('avg_r1', 100)) / 100),
                    (float(cfg.get('avg_pct2', 0)), int(cfg.get('avg_r2', 100)) / 100),
                    (float(cfg.get('avg_pct3', 0)), int(cfg.get('avg_r3', 100)) / 100),
                ]
                # 현재 회차 설정 (0=없음이면 해당 회차 비활성)
                if _auto_avg_on and _auto_avg_cnt < _max_avg and _avg_cooldown_ok:
                    _cur_round = _auto_avg_cnt  # 0=1차, 1=2차, 2=3차
                    _cur_pct, _cur_ratio = _avg_settings[_cur_round] if _cur_round < len(_avg_settings) else (0, 1.0)
                    # pct=0이면 해당 회차 비활성 (없음)
                    if _cur_pct < 0 and pnl_pct <= _cur_pct:
                        _auto_avg_qty = max(1, int(qty * _cur_ratio))
                        _auto_avg_amt = cur_price * _auto_avg_qty
                        _max_buy = int(cfg.get('max_buy_amount', 500000))
                        if _auto_avg_amt <= _max_buy:
                            try:
                                _avg_result = kis_request("POST", "/uapi/domestic-stock/v1/trading/order-cash",
                                    app_key, app_secret, mode, token, "TTTC0802U",
                                    body={'CANO': cfg.get('account',''), 'ACNT_PRDT_CD': cfg.get('account_cd','01'),
                                          'PDNO': ticker, 'ORD_DVSN': '01',
                                          'ORD_QTY': str(_auto_avg_qty), 'ORD_UNPR': '0'})
                                if _avg_result.get('rt_cd') == '0':
                                    auto_avg_count[ticker] = _auto_avg_cnt + 1
                                    auto_avg_last_ts[ticker] = time.time()
                                    _avg_done_this_cycle.add(ticker)
                                    print(f"[AUTO_AVG] 🔄 {name}({ticker}) {_auto_avg_cnt+1}차 물타기 {_auto_avg_qty}주 @₩{cur_price:,} ({pnl_pct:+.1f}%<={_cur_pct}%)")
                                    trade_log.append({
                                        "time": datetime.now().isoformat(), "date": today,
                                        "type": "AVG_DOWN", "ticker": ticker, "name": name,
                                        "qty": _auto_avg_qty, "price": cur_price, "success": True,
                                        "message": f"🔄 {_auto_avg_cnt+1}차 물타기 ({pnl_pct:+.1f}%<={_cur_pct}%) {_auto_avg_qty}주 (보유{qty}주의 {int(_cur_ratio*100)}%)"
                                    })
                                    save_state()
                                    try:
                                        tg_avg_down(name, ticker, _auto_avg_qty, cur_price, _auto_avg_cnt+1, pnl_pct, qty)
                                    except: pass
                                else:
                                    _avg_msg = _avg_result.get('msg1', '')
                                    print(f"[AUTO_AVG] ❌ {name}({ticker}) 물타기 거절: {_avg_msg}")
                                    if '거래정지' in _avg_msg or '매매정지' in _avg_msg:
                                        perm_blocked[ticker] = f"HALTED: {_avg_msg[:50]}"
                                        save_state()
                                    elif '주문가능' in _avg_msg or '부족' in _avg_msg:
                                        auto_avg_last_ts[ticker] = time.time() + 180
                                    else:
                                        auto_avg_last_ts[ticker] = time.time() + 60
                            except Exception as _ae:
                                print(f"[AUTO_AVG] 물타기 실패: {_ae}")
                                if '500' in str(_ae) or '주문가능' in str(_ae) or '부족' in str(_ae):
                                    auto_avg_last_ts[ticker] = time.time() + 180
                # tp3 도달은 맨 위에서 이미 처리됨
            
            # ── 손절 체크 (★ v8.1: 물타기 전회차 완료 후 손절) ──
            if ticker not in sold_this_cycle:
                _auto_avg_cnt_sl = auto_avg_count.get(ticker, 0)
                _auto_avg_on_sl = bool(cfg.get('auto_avg_down', True))
                _sl_disabled = (_sl_val >= 0 or _sl_val <= -999)
                # 설정된 물타기 최대 회차 계산 (pct=0이면 비활성)
                _max_rounds = 0
                if _auto_avg_on_sl:
                    if float(cfg.get('avg_pct1', -4)) < 0: _max_rounds = 1
                    if float(cfg.get('avg_pct2', 0)) < 0: _max_rounds = 2
                    if float(cfg.get('avg_pct3', 0)) < 0: _max_rounds = 3
                # 물타기 ON이면 전회차 완료 후 손절, OFF이면 바로 손절
                _sl_ready = (not _auto_avg_on_sl) or (_auto_avg_cnt_sl >= _max_rounds)
                if not _sl_disabled and _sl_ready and pnl_pct <= _sl_val:
                    self._execute_sell(cfg, token, ticker, name, qty, cur_price,
                        f"🔴 손절 ({pnl_pct:.1f}% <= {_sl_val}%) 물타기{_auto_avg_cnt_sl}/{_max_rounds}회완료 전량매도", avg_price=avg_price)
                    sold_this_cycle.add(ticker)
                    _sell_stage.pop(ticker, None)
                    _dip_flag.pop(ticker, None)
                    _tp1_triggered.discard(ticker)
                    auto_avg_count.pop(ticker, None)
            
            # ── ★ v6.0: 시간초과 매도 (단순 기계적: 보유시간 >= 설정) ──
            if ticker not in sold_this_cycle:
                _max_hold_cfg = int(cfg.get('max_hold_min', 0))  # ★ v8.0: 기본값 0 = 시간제한없음
                if _max_hold_cfg > 0:
                    # peak_prices에 기록된 시간 또는 auto_tickers 진입 시간
                    _hold_min_mech = 0
                    # ★★★ v8.0: 전날 종목은 오늘 09:00부터 카운트 ★★★
                    for _t in reversed(trade_log):
                        if _t.get('ticker') == ticker and _t.get('type') in ('AI_BUY','BUY','CHAT_BUY','AVG_DOWN') and _t.get('time'):
                            try:
                                _buy_dt = datetime.fromisoformat(_t['time'])
                                _today_open = datetime.now().replace(hour=9, minute=0, second=0, microsecond=0)
                                if _buy_dt.date() < datetime.now().date():
                                    _hold_min_mech = max(0, int((datetime.now() - _today_open).total_seconds() / 60))
                                else:
                                    _hold_min_mech = int((datetime.now() - _buy_dt).total_seconds() / 60)
                            except: pass
                            break
                    if _hold_min_mech >= _max_hold_cfg:
                        _time_label = f"{'익절' if pnl_pct > 0 else '손절'} {pnl_pct:+.1f}%"
                        print(f"[TIMEOUT] ⏱ {name}({ticker}) {_hold_min_mech}분>={_max_hold_cfg}분 → 매도!")
                        self._execute_sell(cfg, token, ticker, name, qty, cur_price,
                            f"⏱ 시간초과 ({_hold_min_mech}분>={_max_hold_cfg}분) {_time_label}", avg_price=avg_price)
                        sold_this_cycle.add(ticker)
                        _sell_stage.pop(ticker, None)
                        _dip_flag.pop(ticker, None)
                        _tp1_triggered.discard(ticker)
                        auto_avg_count.pop(ticker, None)
            
            # ★ v4.0: 분할매수 비활성화 (단타 = 전량 일괄매수)
            # check_split_buy_phase2 호출 제거 — 단타에서 2차 매수 대기는 시간 낭비
                
        # ═══════════════════════════════════════════════════════════
        # ★★★ v8.0: 중장기 매도/물타기 엔진 비활성화 (270줄 제거) ★★★
        # → swing_tickers는 start() 시 auto_tickers로 이관됨
        # → 모든 보유종목은 위 단타 매도 엔진(tp1/tp2/tp3/sl/trailing)으로 통합 관리
        # ═══════════════════════════════════════════════════════════

        # Cleanup: remove auto_tickers that no longer have holdings
        stale = [t for t in auto_tickers if t not in active_auto]
        for t in stale:
            auto_tickers.remove(t)
            _sell_stage.pop(t, None)
            _dip_flag.pop(t, None)
            _tp1_triggered.discard(t)
            _sell_plans.pop(t, None)
            _ai_sell_states.pop(t, None)
            auto_avg_count.pop(t, None)  # ★ v6.0: 물타기 카운트 정리
        if stale:
            save_state()
    
    def _check_ai_autobuy(self):
        """AI-based auto buy: scan market or specific stocks and buy strong signals"""
        import re
        cfg = self.config

        now_hhmm = datetime.now().hour * 100 + datetime.now().minute
        today = datetime.now().strftime('%Y-%m-%d')

        # ★★★ v8.0: 12:00 이후 매수 스캔 차단 (오전만 매매) ★★★
        if now_hhmm >= 1200:
            return
        
        # ─── 프리마켓 구간 (8:40~9:00) — v8.0: 워치리스트 삭제, 대기만 ───
        if now_hhmm < 900:
            return

        # ─── 이후는 AI 키 + 룰 필수 ───
        anthropic_key = cfg.get('anthropic_key', '')
        openai_key = cfg.get('openai_key', '')
        if not anthropic_key and not openai_key:
            print("[AI_BUY] ⛔ AI 키 없음 - 매수 스킵")
            return
        
        ai_rules = [r for r in auto_rules if r.get('type') in ['ai_autobuy', 'ai_market_scan'] and r.get('active', True)]
        if not ai_rules:
            print(f"[AI_BUY] ⛔ ai_market_scan 룰 없음 (전체 룰 {len(auto_rules)}개) - 매수 스킵")
            return

        # (워치리스트 실행은 scan_loop에서 직접 처리 - 타이머 독립)
        
        mode = 'live'
        app_key = cfg.get('app_key')
        app_secret = cfg.get('app_secret')
        
        try:
            token = kis_get_token(app_key, app_secret, mode)
        except:
            return
        
        # ★ 잔고는 대시보드(_kis_balance)가 업데이트한 글로벌 캐시에서 읽기
        # → 별도 KIS API 호출 없음, rate limit 절감
        cache_age = time.time() - _bal_cache['ts']
        if _bal_cache['data'] and cache_age < 120:
            bal = _bal_cache['data']
            print(f"[AI_BUY] 📋 잔고 캐시 사용 (갱신 {cache_age:.0f}초 전)")
        else:
            # 캐시 없거나 2분 초과 시에만 직접 조회 (fallback)
            tr_id_bal = "TTTC8434R"
            try:
                bal = kis_request("GET", "/uapi/domestic-stock/v1/trading/inquire-balance",
                    app_key, app_secret, mode, token, tr_id_bal,
                    params={"CANO": cfg['account'], "ACNT_PRDT_CD": cfg.get('account_cd', '01'),
                            "AFHR_FLPR_YN": "N", "OFL_YN": "", "INQR_DVSN": "02",
                            "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N",
                            "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "01",
                            "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""})
                _bal_cache['data'] = bal
                _bal_cache['ts'] = time.time()
                print(f"[AI_BUY] 🔄 잔고 직접 조회 (캐시 {cache_age:.0f}초 만료)")
            except Exception as _e:
                print(f"[AI_BUY] ⛔ 잔고 조회 실패: {_e} → max_buy_amount 기준으로 계속")
                bal = {}
        try:
            b2_ai = bal.get('output2', [{}])[0]
            # cash는 수량계산용으로만 사용 (max_buy_amount 기준)
            cash = float(cfg.get('max_buy_amount', 1000000)) * 10  # 충분히 크게
            if cash <= 0:
                cash = float(cfg.get('max_buy_amount', 2000000)) * 3
            # ★ FIX #2: 실제 KIS 잔고에서 보유종목 추출 (paper 분기 제거)
            # ★★★ v8.0: 모든 보유종목 카운트 (swing 필터 제거) ★★★
            _cached_held = set(p.get('pdno') for p in bal.get('output1', []) if int(p.get('hldg_qty', '0') or '0') > 0)
            
            # ★★★ Fix: _cash_tracker를 실제 KIS 주문가능금액으로 초기화/동기화 ★★★
            _kis_avail = int(b2_ai.get('dnca_tot_amt', '0') or 0)
            if _kis_avail > 0 and (time.time() - _cash_tracker['ts'] > 60 or _cash_tracker['amount'] <= 0):
                _old_ct = _cash_tracker['amount']
                _cash_tracker['amount'] = _kis_avail
                _cash_tracker['ts'] = time.time()
                if _old_ct != _kis_avail:
                    print(f"[CASH_SYNC] 트래커 동기화: ₩{_old_ct:,.0f} → ₩{_kis_avail:,.0f} (KIS 예수금 기준)")
            
            print(f"[AI_BUY] 📋 보유종목: {_cached_held} | 예수금계산용: {cash:,.0f} | 트래커: ₩{_cash_tracker['amount']:,.0f}")
        except Exception as _e:
            print(f"[AI_BUY] ⛔ 잔고 파싱 실패: {_e} → 기본값으로 계속")
            cash = float(cfg.get('max_buy_amount', 2000000)) * 3
            _cached_held = set()
        
        today = datetime.now().strftime('%Y-%m-%d')
        print(f"[AI_BUY] 💰 예수금 {cash:,.0f}원 | ai_rules {len(ai_rules)}개")
        
        for rule in ai_rules:
            max_buy_amount = float(cfg.get('max_buy_amount', rule.get('max_buy_amount', 500000)))
            # ★★★ v8.0: self.config에서 직접 읽기 (스캔 중 설정 변경 즉시 반영) ★★★
            max_pos = int(self.config.get('max_positions', 6))
            # ★ 거래정지 종목은 슬롯에서 제외
            cur_pos_count = len([t for t in auto_tickers if not _is_truly_halted(t)])
            _slots_full = cur_pos_count >= max_pos
            self._slots_full = _slots_full
            if _slots_full:
                # ★ v8.0: 1회만 로그 (매도 시 _last_full_log_ts 리셋 → 다시 표시)
                _last_full_log = getattr(self, '_last_full_log_ts', 0)
                if _last_full_log == 0:
                    self._last_full_log_ts = time.time()
                    trade_log.append({
                        "time": datetime.now().isoformat(), "date": today,
                        "type": "SYSTEM",
                        "message": f"⏸ 보유 {cur_pos_count}/{max_pos}종목 꽉 참 → 신규 매수 중단, 매도는 설정값(tp/sl) 자동 처리"
                    })
                    save_state()
                return  # ★ AI 스캔 skip
            
            scan_mode = rule.get('type', '')
            tickers_to_check = []
            
            # ===== MARKET-WIDE SCAN MODE =====
            if scan_mode == 'ai_market_scan':
                market = rule.get('market', 'ALL')  # KOSPI, KOSDAQ, ALL
                strategy = rule.get('strategy', '')
                max_picks = int(rule.get('max_picks', 5))
                
                # ★ v3.0 H: 코스닥 전용 스캔 모드
                # 오전은 ALL, 점심 이후 코스닥 따로 한 번 더 스캔 (코스닥 소형주 발굴)
                _kosdaq_solo = False
                if market == 'ALL' and now_hhmm >= 1100:
                    # 2스캔 중 1번은 코스닥 전용 (교대)
                    _scan_count = len([t for t in trade_log if t.get('date')==today and t.get('type')=='AI_MARKET_SCAN'])
                    if _scan_count % 3 == 2:  # 매 3회 중 1회 코스닥 전용
                        market = 'KOSDAQ'
                        _kosdaq_solo = True
                        print(f"[SCAN] 🟣 코스닥 전용 스캔 모드 활성화 (#{_scan_count})")
                
                # 보유/오늘 매수완료 종목 (프롬프트 참조용)
                _one_hour_ago = (datetime.now() - timedelta(hours=1)).isoformat()
                # 1시간 내 임시차단 종목
                _temp_blocked = set(
                    t.get('ticker','') for t in trade_log
                    if t.get('type') in ('BLOCKED','CAPITAL_BLOCK')
                    and t.get('time','') >= _one_hour_ago
                    and t.get('ticker','')
                )
                # 영구차단 종목 (매매불가 등)
                _perm = set(perm_blocked.keys())
                # ★★★ v6.0: 당일 재매수 허용 — 설정 쿨다운 적용 ★★★
                _cooldown_min = int(cfg.get('cooldown_min', 10))
                _cooldown_ago = (datetime.now() - timedelta(minutes=_cooldown_min)).isoformat()
                _cooldown_tickers = set(
                    t.get('ticker','') for t in trade_log
                    if t.get('date','') == today and t.get('ticker','') and
                    t.get('type') in ('SELL','FORCE_CLOSE') and
                    t.get('time','') >= _cooldown_ago  # 설정 시간 내 매도한 종목
                ) - {''}
                held_tickers = set(_cached_held) | set(auto_tickers) | _cooldown_tickers | _temp_blocked | _perm
                bought_today = held_tickers  # alias (프롬프트용)

                # ★ 제외 목록: 코드만 사용 (이름 혼동 방지)
                # 코드 옆에 이름도 참고용으로 표시하되, AI는 코드 기준으로만 판단
                def _fmt_held(tickers):
                    if not tickers: return '없음'
                    parts = []
                    for t in sorted(tickers):
                        nm = (_valid_candidates.get(t) or
                              t)
                        parts.append(f"{t}({nm})" if nm != t else t)
                    return ', '.join(parts)
                
                # Ask AI to find top picks from the market
                market_label = {"KOSPI": "코스피", "KOSDAQ": "코스닥", "ALL": "코스피+코스닥"}.get(market, "코스피+코스닥")
                
                # ★ v3.0 H: 코스닥 전용 스캔 가이드
                _kosdaq_guide = ""
                if market == 'KOSDAQ' or _kosdaq_solo:
                    _kosdaq_guide = """
[🟣 코스닥 전용 스캔 모드]
★ 코스닥 소형주 특화 전략:
- 시총 500억~5000억 중소형주 집중 (대형주 제외)
- 거래량 폭발(전일 3배↑) + 상승 초입 종목 최우선
- 바이오/게임/엔터/2차전지소재 등 코스닥 특유 테마 주목
- 기관/외국인 동시 순매수 종목 우선 (개인 주도 종목 주의)
- 상한가 따라잡기 금지 (이미 +20% 넘은 종목 제외)
- 스팩/리츠/ETF 완전 제외
"""
                strategy_text = f"전략 조건: {strategy}" if strategy else "오늘 단타 수익 가능성이 높은 종목"
                scalp_mode = self._is_scalp_mode()
                now_hhmm = datetime.now().hour * 100 + datetime.now().minute

                # ★ v3.0 TIER 2: 멀티 전략 선택
                _strat_key, _strat_guide, _strat_emoji, _strat_name = self._select_strategy(bal.get('output1',[]))
                strategy_text = f"{_strat_emoji} 전략: {_strat_name}" + (f" + {strategy}" if strategy else "")
                
                # 전략 선택 로그
                trade_log.append({
                    "time": datetime.now().isoformat(), "date": today,
                    "type": "SCAN_TICK",
                    "message": f"{_strat_emoji} 전략: {_strat_name} |  {now_hhmm//100}:{now_hhmm%100:02d}"
                })
                save_state()

                # ★★★ v8.0: 시황 브리핑 데이터 주입 삭제 (단타는 실시간 급등주 데이터만 사용) ★★★

                # ★ v6.0: 보유+쿨다운10분+차단 (당일 재매수 허용)
                skip_tickers = held_tickers.copy()

                # ★★★ v6.0 단타 경량 스캔 — 달리는 말에 올라타기 ★★★
                _t_scan = {'수집': 0, 'AI호출': 0, '필터': 0}
                _t0 = time.time()
                
                # ── 1. KIS 거래량TOP + 등락률TOP 수집 ──
                _surge_candidates = []  # 급등 후보
                _valid_candidates = {}
                _valid_tickers = set()
                _sector_map = {}
                
                # ★★★ v8.0: 프리마켓 캐시 사용 (08:55 수집 → 09:01 첫 스캔 3~5초 단축) ★★★
                _pm_cache = getattr(self, '_premarket_cache', None)
                _pm_ts = getattr(self, '_premarket_cache_ts', 0)
                if _pm_cache and (time.time() - _pm_ts) < 600:  # 10분 이내 캐시
                    # 캐시에서 현재 보유/차단 종목 제외 후 사용
                    for _pc in _pm_cache:
                        _ptk = _pc['ticker']
                        if _ptk not in skip_tickers:
                            _surge_candidates.append(_pc)
                    print(f"[SCAN] ⚡ 프리마켓 캐시 사용: {len(_surge_candidates)}종목 (수집 생략 → AI 판단만)")
                
                try:
                    if _surge_candidates:
                        # ★ 프리마켓 캐시 사용 → KIS/네이버 수집 전부 건너뛰기
                        print(f"[SCAN] ⚡ 프리마켓 캐시 {len(_surge_candidates)}종목 사용 (KIS 수집 생략)")
                        _vol_raw = []
                        _raw_above3 = []
                        _skip_cnt = 0
                        _filter_log = []
                    else:
                        print(f"[SCAN] 실시간 KIS 수집 시작...")
                        # 거래량 TOP
                        _vol_data = kis_request("GET", "/uapi/domestic-stock/v1/quotations/volume-rank",
                            app_key, app_secret, mode, token, "FHPST01710000",
                            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "20171",
                                    "FID_INPUT_ISCD": "0000", "FID_DIV_CLS_CODE": "0",
                                    "FID_BLNG_CLS_CODE": "0", "FID_TRGT_CLS_CODE": "111111111",
                                    "FID_TRGT_EXLS_CLS_CODE": "000000", "FID_INPUT_PRICE_1": "0",
                                    "FID_INPUT_PRICE_2": "0", "FID_VOL_CNT": "0", "FID_INPUT_DATE_1": ""})
                        _vol_raw = _vol_data.get('output', [])
                        # ★ v8.0: 필터 전 원본 데이터 로그 (디버그)
                        _raw_above3 = [s for s in _vol_raw[:30] if float(s.get('prdy_ctrt','0') or 0) >= 3.0]
                        _skip_cnt = 0
                        _filter_log = []  # ★ v8.0: 탈락 이유 디버그
                        for s in _vol_raw[:30]:
                            tk = s.get('mksc_shrn_iscd','')
                            nm = s.get('hts_kor_isnm','')
                            pr = int(float(s.get('stck_prpr','0') or 0))
                            chg = float(s.get('prdy_ctrt','0') or 0)
                            vol = int(s.get('acml_vol','0') or 0)
                            prev_vol_rate = float(s.get('prdy_vrss_vol_rate', '0') or 0)
                            vol_ratio = round(prev_vol_rate) if prev_vol_rate > 0 else 0
                            if tk in skip_tickers:
                                _skip_cnt += 1
                                continue
                            if pr < 1000 or pr > 50000:  # ★ v8.0: ₩1,000~₩50,000
                                if chg >= 3.0: _filter_log.append(f"{nm}({tk}) 가격{pr}원")
                                continue
                            if chg < 3.0 or chg > 25.0:  # ★ v8.0: 3%~25% (25% 이상은 고점 매수 위험)
                                if chg >= 3.0: _filter_log.append(f"{nm}({tk}) 등락{chg}%")
                                continue
                            if vol < 10000:  # ★ v8.0: 거래량 1만주 이상 (유동성 확보)
                                if chg >= 3.0: _filter_log.append(f"{nm}({tk}) 거래량{vol}<1만")
                                continue
                            # ETF 제외
                            if any(kw in nm for kw in ['KODEX','TIGER','KBSTAR','ETF','인버스','레버리지']): continue
                            _surge_candidates.append({
                                'ticker': tk, 'name': nm, 'price': pr, 'chg': chg,
                                'volume': vol, 'vol_ratio': vol_ratio, 'source': '거래량'
                            })
                        time.sleep(0.2)
                    
                        # 등락률 TOP
                        _rate_data = kis_request("GET", "/uapi/domestic-stock/v1/quotations/volume-rank",
                            app_key, app_secret, mode, token, "FHPST01710000",
                            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "20171",
                                    "FID_INPUT_ISCD": "0000", "FID_DIV_CLS_CODE": "1",
                                    "FID_BLNG_CLS_CODE": "0", "FID_TRGT_CLS_CODE": "111111111",
                                    "FID_TRGT_EXLS_CLS_CODE": "000000", "FID_INPUT_PRICE_1": "0",
                                    "FID_INPUT_PRICE_2": "0", "FID_VOL_CNT": "0", "FID_INPUT_DATE_1": ""})
                        _existing_tks = set(c['ticker'] for c in _surge_candidates)
                        for s in _rate_data.get('output', [])[:30]:
                            tk = s.get('mksc_shrn_iscd','')
                            nm = s.get('hts_kor_isnm','')
                            pr = int(float(s.get('stck_prpr','0') or 0))
                            chg = float(s.get('prdy_ctrt','0') or 0)
                            vol = int(s.get('acml_vol','0') or 0)
                            prev_vol_rate = float(s.get('prdy_vrss_vol_rate', '0') or 0)
                            vol_ratio = round(prev_vol_rate) if prev_vol_rate > 0 else 0
                            if tk in skip_tickers or tk in _existing_tks: continue
                            if pr < 1000 or pr > 50000: continue  # ★ v8.0: ₩1,000~₩50,000
                            if chg < 3.0 or chg > 25.0: continue  # ★ v8.0: 3%~25%
                            if vol < 10000: continue  # ★ v8.0: 거래량 1만주 이상
                            if any(kw in nm for kw in ['KODEX','TIGER','KBSTAR','ETF','인버스','레버리지']): continue
                            _surge_candidates.append({
                                'ticker': tk, 'name': nm, 'price': pr, 'chg': chg,
                                'volume': vol, 'vol_ratio': vol_ratio, 'source': '등락률'
                            })
                except Exception as _se:
                    print(f"[SCAN] KIS 수집 에러: {_se}")
                
                    # ★ v8.0: 필터 전/후 비교 로그 (디버그)
                    if len(_surge_candidates) == 0:
                        _raw_top3 = sorted(_vol_raw[:10], key=lambda s: float(s.get('prdy_ctrt','0') or 0), reverse=True)[:3]
                        _raw_str = ', '.join(f"{s.get('hts_kor_isnm','')}({s.get('mksc_shrn_iscd','')})={s.get('prdy_ctrt','')}%" for s in _raw_top3)
                        print(f"[SCAN_DEBUG] KIS원본 {len(_vol_raw)}개, +3%↑ {len(_raw_above3)}개, skip {_skip_cnt}개 → 후보 {len(_surge_candidates)}개 | TOP: {_raw_str}")
                        if _filter_log:
                            print(f"[SCAN_DEBUG] 탈락사유: {' · '.join(_filter_log[:5])}")
                
                    # ── 1-2. 네이버 상승률 (KIS에 없는 종목 보완) ──
                    try:
                        _existing_tks2 = set(c['ticker'] for c in _surge_candidates)
                        ua = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
                        for _mkt in ['KOSPI', 'KOSDAQ']:
                            try:
                                _nv_req = urllib.request.Request(
                                    f'https://m.stock.naver.com/api/stocks/up/{_mkt}?page=1&pageSize=30', headers=ua)
                                with urllib.request.urlopen(_nv_req, timeout=8) as _nv_resp:
                                    _nv_data = json.loads(_nv_resp.read().decode('utf-8'))
                                for s in _nv_data.get('stocks', [])[:20]:
                                    tk = s.get('stockCode', '')
                                    nm = s.get('stockName', '')
                                    # 가격: 여러 필드 시도
                                    _npr = 0
                                    for _pk in ('closePrice', 'currentPrice', 'dealPrice', 'openPrice', 'basePrice'):
                                        _v = s.get(_pk, '')
                                        if _v and str(_v).replace(',','').replace('.','').isdigit():
                                            _npr = int(float(str(_v).replace(',','')))
                                            break
                                    chg = float(s.get('fluctuationsRatio', '0') or 0)
                                    if not tk or tk in skip_tickers or tk in _existing_tks2: continue
                                    if _npr < 1000 or _npr > 50000: continue  # ★ v8.0: ₩1,000~₩50,000
                                    if chg < 3.0 or chg > 25.0: continue  # ★ v8.0: 3%~25%
                                    if any(kw in nm for kw in ['KODEX','TIGER','KBSTAR','ETF','인버스','레버리지']): continue
                                    _surge_candidates.append({
                                        'ticker': tk, 'name': nm, 'price': _npr, 'chg': chg,
                                        'volume': 0, 'vol_ratio': 0, 'source': f'네이버{_mkt}'
                                    })
                                    _existing_tks2.add(tk)
                            except Exception as _nve:
                                print(f"[SCAN] 네이버 {_mkt} 실패: {_nve}")
                        print(f"[SCAN] KIS+네이버 급등 후보: {len(_surge_candidates)}개")
                    except Exception as _ne:
                        print(f"[SCAN] 네이버 수집 에러: {_ne}")
                
                _t_scan['수집'] = round(time.time() - _t0, 1)
                
                # ── 2. 체결강도 사전 조회 제거 (v6.0: 생동감 점수 단계에서 개별 조회) ──
                # → AI가 5개 추천 후, 그 5개만 체결강도 조회 (429 방지)
                
                # ── 3. 급등 후보 필터 (등락률만으로 필터) ──
                _final_surge = [c for c in _surge_candidates if c['chg'] >= 3.0]
                _final_surge = _final_surge[:20]  # AI에게 최대 20개 전달
                
                # valid 구성
                for c in _final_surge:
                    _valid_candidates[c['ticker']] = c['name']
                    _valid_tickers.add(c['ticker'])
                
                # ── 보유종목 섹터 조회 (sector_unique용) ──
                for _htk in list(_cached_held)[:10]:
                    if _htk not in _sector_map:
                        try:
                            _sp_h = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
                                app_key, app_secret, mode, token, "FHKST01010100",
                                params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": _htk})
                            _sec_h = _sp_h.get('output', {}).get('bstp_kor_isnm', '').strip()
                            if _sec_h:
                                _sector_map[_htk] = _sec_h
                            time.sleep(0.08)
                        except: pass
                
                # ── 4. 후보 목록 텍스트 ──
                _surge_text = ""
                for i, c in enumerate(_final_surge):
                    _surge_text += f"  {i+1}. {c['name']}({c['ticker']}) ₩{c['price']:,} +{c['chg']:.1f}% "
                    _surge_text += f"거래량{c['vol_ratio']}% 체결강도{c.get('strength','-')}% [{c['source']}]\n"
                
                print(f"[SCAN] ⚡ 급등 후보 {len(_final_surge)}개 (전체 {len(_surge_candidates)}개 중)")
                
                # ── 5. 간단 AI 프롬프트 (급등주 골라라) ──
                trade_log.append({
                    "time": datetime.now().isoformat(), "date": today,
                    "type": "AI_MARKET_SCAN",
                    "message": f"🔍 코스피+코스닥 스캔: " + ", ".join(f"{c['name']}({c['chg']:+.1f}%)" for c in _final_surge[:5])
                })
                save_state()
                
                if not _final_surge:
                    _top3 = sorted(_surge_candidates, key=lambda c: c['chg'], reverse=True)[:3]
                    _top3_str = ', '.join(f"{c['name']}({c['chg']:+.1f}%)" for c in _top3) if _top3 else '수집0개'
                    print(f"[SCAN] 급등 후보 0개 (수집 {len(_surge_candidates)}개, 3%↑ 미달) TOP: {_top3_str}")
                    trade_log.append({
                        "time": datetime.now().isoformat(), "date": today,
                        "type": "AI_MARKET_SCAN",
                        "message": f"⚠ 급등 후보 없음 (수집{len(_surge_candidates)}개, 3%↑미달) TOP: {_top3_str} → 다음 스캔 대기"
                    })
                    save_state()
                    continue
                
                # ★ 제외 종목 표시
                def _fmt_held(tickers):
                    parts = []
                    for t in list(tickers)[:20]:
                        nm = ensure_name(t, '')
                        parts.append(f"{t}({nm})" if nm != t else t)
                    return ', '.join(parts)
                
                scan_prompt = f"""지금 급등 중인 종목에서 단타 매수할 {max_picks}개 골라줘. 현재 {now_hhmm//100}:{now_hhmm%100:02d}

[🔥 급등 후보 — 지금 올라가고 있는 종목들]
{_surge_text}

★ 위 종목 중 지금 모멘텀이 가장 강하고 추가 상승 여력 있는 것만 골라라.
★ 이미 고점 찍고 내려오는 중이면 제외 (등락률만 보지 말고 흐름 판단)
★ 거래량 폭발 + 상승 중 = 최우선
★ 가격대 ₩50,000 이하만

절대 추천 금지: {_fmt_held(held_tickers)}

반드시 JSON만 반환:
{{"picks":[{{"ticker":"6자리코드","signal":"매수","confidence":0~100,"reason":"이유"}}],"no_pick_reason":"없을 때만"}}

★ 후보 중 가장 나은 것 반드시 1개 이상 추천! confidence 70% 이상이면 전부!"""

                picks = []  # ★ v6.0 FIX: AI 호출 실패 시에도 picks 정의 보장
                _t4 = time.time()
                try:
                    system_msg = "한국 주식 단타 급등주 스캐너. 지금 올라가는 종목 중 추가 상승 여력 있는 것 골라. JSON만 반환."
                    _t3 = time.time()
                    ai_text = call_ai(scan_prompt, system_msg, 1500, web_search=False, tier='scan')
                    _t_scan['AI호출'] = round(time.time() - _t3, 1)
                    json_match = re.search(r'\{[\s\S]*\}', ai_text)
                    if json_match:
                        scan_result = safe_json_loads(json_match.group(), "AI_SCAN")
                        picks = scan_result.get('picks', [])
                        picks = sorted(picks, key=lambda p: p.get('confidence', 0), reverse=True)
                except Exception as _ai_err:
                    # ★★★ v6.0: AI 실패 → 등락률TOP 폴백 (생동감 점수가 거름) ★★★
                    print(f"[AI_BUY] ⚠️ AI 호출 실패: {_ai_err} → 등락률TOP 폴백")
                    picks = [
                        {'ticker': c['ticker'], 'name': c['name'], 'confidence': 80, 'reason': f"AI폴백 +{c['chg']:.1f}%"}
                        for c in _final_surge[:max_picks]
                    ]
                    trade_log.append({
                        "time": datetime.now().isoformat(), "date": today,
                        "type": "AI_ERROR",
                        "message": f"AI 실패({str(_ai_err)[:30]}) → 등락률TOP {len(picks)}개 폴백"
                    })
                    save_state()

                # ── picks 처리 (AI 성공이든 폴백이든) ──
                try:
                    _no_pick_reason = ''
                    def _psname(p):
                        t = p.get('ticker','')
                        return _valid_candidates.get(t) or p.get('name','') or t
                    pick_summary = ', '.join(
                        f"{_psname(p)}({p.get('confidence',0)}%)"
                        for p in picks)
                    _display_summary = pick_summary or f"추천없음 ({_no_pick_reason or '사유 미제공'})"
                    _scan_model = 'mini' if any('폴백' in p.get('reason','') for p in picks) else 'mini'
                    print(f"[AI_BUY] 📊 picks {len(picks)}개 ({_scan_model}): {_display_summary}")
                    trade_log.append({
                        "time": datetime.now().isoformat(), "date": today,
                        "type": "AI_MARKET_SCAN",
                        "message": f"🔍 {market_label} 스캔(mini): {_display_summary}"
                    })
                    save_state()

                    # ★ skip_tickers 갱신: 재매매 정책 적용
                    # 손실 매도 → 당일 완전 제외 / 수익 매도 → 2시간 쿨다운 후 재매수 가능
                    _two_hours_ago = (datetime.now() - timedelta(hours=2)).isoformat()
                    _loss_sold_today = set(  # 손절/AI매도(악재) → 당일 제외
                        t.get('ticker','') for t in trade_log
                        if t.get('date','') == today and t.get('ticker','') and
                        t.get('type') == 'SELL' and t.get('success') is True and
                        (float(t.get('pnl', 0) or 0) < 0 or  # 손실 매도
                         '손절' in t.get('reason','') or 'AI매도' in t.get('reason','') or
                         '강력매도' in t.get('reason',''))
                    )
                    _profit_sold_recent = set(  # 수익 매도 2시간 이내 → 쿨다운
                        t.get('ticker','') for t in trade_log
                        if t.get('date','') == today and t.get('ticker','') and
                        t.get('type') == 'SELL' and t.get('success') is True and
                        float(t.get('pnl', 0) or 0) >= 0 and
                        t.get('time','') >= _two_hours_ago  # 2시간 이내면 아직 쿨다운
                    )
                    _one_hour_ago2 = (datetime.now() - timedelta(hours=1)).isoformat()
                    # ★ v6.0: 보유+쿨다운+차단 제외 (재매수는 10분 후 허용)
                    skip_tickers = held_tickers.copy()

                    # ★★★ v4.0: 비율 기반 섹터 분산 강제 ★★★
                    _sector_max_ratio = float(cfg.get('sector_max_ratio', 40)) / 100  # 기본 40%
                    _sector_counts = {}  # {sector: [ticker1, ticker2, ...]}
                    for _htk in _cached_held:
                        _hs = _sector_map.get(_htk, '')
                        if _hs:
                            _sector_counts.setdefault(_hs, []).append(_htk)
                    # 이번 스캔에서 매수한 것도 추적
                    _bought_sectors_this_scan = dict(_sector_counts)  # 복사

                    _buys_this_scan = 0  # 스캔당 최대 5종목
                    
                    # ★ 보유 꽉 참 OR 일시정지 → 매수 skip (매도 판단은 위에서 이미 완료)
                    if _slots_full or self.paused:
                        _reason = '슬롯 꽉' if _slots_full else '일시정지(손실한도/수동)'
                        print(f"[AI_BUY] {_reason} → 신규 매수 skip ({len(picks)}개 추천 보류)")
                        if picks:
                            _skip_names = ', '.join(f"{_valid_candidates.get(p.get('ticker',''),p.get('name','?'))}({p.get('confidence',0)}%)" for p in picks[:3])
                            trade_log.append({
                                "time": datetime.now().isoformat(), "date": today,
                                "type": "AI_MARKET_SCAN",
                                "message": f"📋 매수 보류({_reason}): {_skip_names}"
                            })
                            save_state()
                        picks = []  # 매수 실행 루프 진입 방지
                    
                    # ★★★ v6.0: 장 시작 즉시 매수 (급등주 올라타기) ★★★

                    for pick in picks:
                        try:
                            p_ticker = str(pick.get('ticker', '')).strip()
                            # ★ 이름은 AI 응답 무시, 후보풀 매핑에서만 가져옴
                            # 이름 우선순위: 후보풀 > AI 응답 > 네이버 조회 > 코드
                            _cand_name = _valid_candidates.get(p_ticker,'').strip()
                            _ai_name   = pick.get('name','').strip()
                            p_name     = _cand_name or _ai_name or ''
                            if not p_name or p_name == p_ticker:
                                p_name = get_stock_name_naver(p_ticker) or p_ticker
                            p_conf   = pick.get('confidence', 0)
                            p_signal = pick.get('signal', '')
                            print(f"[NAME] {p_ticker}: cand='{_cand_name}' ai='{_ai_name}' → 확정='{p_name}'")

                            # 기본 유효성만 체크 (6자리 코드, 중복 제외)
                            if not p_ticker or len(p_ticker) != 6:
                                print(f"[AI_BUY] ⏭ 코드 이상: '{p_ticker}' 스킵")
                                continue
                            # ★★★ 핵심: 후보풀에 없는 코드는 즉시 차단 (AI 환각 방지)
                            if _valid_tickers and p_ticker not in _valid_tickers:
                                trade_log.append({
                                    "time": datetime.now().isoformat(), "date": today,
                                    "type": "BLOCKED", "ticker": p_ticker, "name": p_name,
                                    "message": f"⛔ 후보풀 미등록 코드 차단: {p_name}({p_ticker}) → AI 환각 의심"
                                })
                                perm_blocked[p_ticker] = f"후보풀 미등록({p_name})"
                                skip_tickers.add(p_ticker)
                                print(f"[AI_BUY] ⛔ 후보풀 없음: {p_name}({p_ticker}) 차단")
                                continue
                            if p_ticker in skip_tickers:
                                _skip_reason = "보유중" if p_ticker in _cached_held else ("영구차단" if p_ticker in perm_blocked else "1시간차단/매수완료")
                                print(f"[AI_BUY] ⏭ {p_name}({p_ticker}) 스킵: {_skip_reason}")
                                continue
                            # ★ 현재가 + KIS 실제 종목명 최종 확정
                            cur_price = 0
                            try:
                                _pr = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
                                    app_key, app_secret, mode, token, "FHKST01010100",
                                    params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": p_ticker})
                                _out = _pr.get('output', {})
                                cur_price = int(float(_out.get('stck_prpr', '0') or '0'))
                                # KIS 이름이 있으면 무조건 덮어씀 (후보풀 이름보다 KIS가 더 정확)
                                _kis_name = _out.get('hts_kor_isnm', '').strip()
                                if _kis_name:
                                    p_name = _kis_name  # KIS 이름 최우선
                                elif _valid_candidates.get(p_ticker):
                                    p_name = _valid_candidates[p_ticker]  # 후보풀 이름 차선
                                # 이름이 어떻든 매수는 진행
                                print(f"[NAME_KIS] {p_ticker}: kis='{_kis_name}' → 최종='{p_name}'")
                            except Exception as _pe2:
                                print(f"[AI_BUY] ⚠️ {p_ticker} 가격조회 실패: {_pe2}")
                                continue

                            if cur_price <= 0:
                                print(f"[AI_BUY] ⚠️ {p_name}({p_ticker}) 현재가 0 - 스킵")
                                continue
                            
                            # ★★★ v8.0: 10만원 초과 차단 (스캔에서도 걸리지만 안전망) ★★★
                            if cur_price > 50000:
                                print(f"[AI_BUY] 🏢 {p_name}({p_ticker}) ₩{cur_price:,} 고가주 → 차단")
                                trade_log.append({"time": datetime.now().isoformat(), "date": today,
                                    "type": "BLOCKED", "ticker": p_ticker, "name": p_name,
                                    "message": f"🏢 고가주 차단(₩{cur_price:,}>50,000원)"})
                                save_state()
                                continue
                            
                            # ★★★ v8.0: 29.9% 이상 과급등 차단 (상한가 직전) ★★★
                            _day_chg = float(_out.get('prdy_ctrt', '0') or 0)
                            if _day_chg >= 25.0:
                                print(f"[AI_BUY] 🚫 {p_name}({p_ticker}) +{_day_chg:.1f}% 과급등 → 차단")
                                trade_log.append({"time": datetime.now().isoformat(), "date": today,
                                    "type": "BLOCKED", "ticker": p_ticker, "name": p_name,
                                    "message": f"🚫 과급등 차단(+{_day_chg:.1f}%≥25%)"})
                                save_state()
                                continue
                            
                            # ═══════════════════════════════════════════
                            # ★★★ v6.0: 생동감 점수 (Surge Score) 가중치 14점 만점 ★★★
                            # 핵심(2점×3) + 보조(1.5점×2) + 참고(1점×5) = 14점
                            # 10점 이상(71%) → 매수 / 9.5점 이하 → 차단
                            # ═══════════════════════════════════════════
                            _surge_score = 0
                            _surge_detail = []
                            
                            # 데이터 추출 (1순위 KIS inquire-price)
                            _hg = int(float(_out.get('stck_hgpr', '0') or 0))
                            _lw = int(float(_out.get('stck_lwpr', '0') or 0))
                            _oprc = int(float(_out.get('stck_oprc', '0') or 0))  # 시가
                            _sdpr = int(float(_out.get('stck_sdpr', '0') or 0))  # 전일종가
                            _vol = int(float(_out.get('acml_vol', '0') or 0))
                            _vol_ratio = float(_out.get('prdy_vrss_vol_rate', '0') or 0)
                            _trading_val = int(float(_out.get('acml_tr_pbmn', '0') or 0))
                            
                            # ★★★ 2순위 네이버 폴백 (KIS 데이터 누락 시) ★★★
                            _naver_used = False
                            if _hg <= 0 or _lw <= 0 or _oprc <= 0 or _trading_val <= 0:
                                try:
                                    _nv_url = f'https://m.stock.naver.com/api/stock/{p_ticker}/basic'
                                    _nv_req = urllib.request.Request(_nv_url, headers={'User-Agent': 'Mozilla/5.0'})
                                    with urllib.request.urlopen(_nv_req, timeout=5) as _nv_resp:
                                        _nv = json.loads(_nv_resp.read().decode('utf-8'))
                                    if _hg <= 0:
                                        _hg = int(float(_nv.get('high', _nv.get('highPrice', 0)) or 0))
                                    if _lw <= 0:
                                        _lw = int(float(_nv.get('low', _nv.get('lowPrice', 0)) or 0))
                                    if _oprc <= 0:
                                        _oprc = int(float(_nv.get('open', _nv.get('openPrice', 0)) or 0))
                                    if _sdpr <= 0:
                                        _sdpr = int(float(_nv.get('previousClose', _nv.get('closePrice', 0)) or 0))
                                    if _trading_val <= 0:
                                        _trading_val = int(float(_nv.get('accumulatedTradingValue', 0) or 0))
                                    if _vol_ratio <= 0:
                                        _nv_vol = int(float(_nv.get('accumulatedTradingVolume', _nv.get('volume', 0)) or 0))
                                        _nv_prev_vol = int(float(_nv.get('previousVolume', 0) or 0))
                                        if _nv_prev_vol > 0:
                                            _vol_ratio = round(_nv_vol / _nv_prev_vol * 100)
                                    _naver_used = True
                                    print(f"[SURGE] 📡 {p_ticker} 네이버 폴백 사용 (KIS 데이터 부분 누락)")
                                except:
                                    print(f"[SURGE] ⚠️ {p_ticker} 네이버 폴백도 실패")
                            
                            # 체결강도 + 호가잔량
                            _strength = 0
                            _buy_rem = 0
                            _sell_rem = 0
                            _ob = None  # ★ v8.0: 캐싱용 (이후 체결강도 필터에서 재사용)
                            try:
                                _ob = fetch_orderbook(app_key, app_secret, mode, token, p_ticker)
                                if _ob:
                                    _strength = _ob.get('strength', 0)
                                    _buy_rem = _ob.get('bid_total', 0)
                                    _sell_rem = _ob.get('ask_total', 0)
                                time.sleep(0.1)
                            except: pass
                            
                            # ═══ 핵심 (2점 × 3 = 6점) — 달리는 중인지 ═══
                            
                            # ★ 데이터 검증 로그 (월요일 확인용)
                            print(f"[SURGE_DATA] {p_name}({p_ticker}) 등락{_day_chg:+.1f}% 현재₩{cur_price:,} 고₩{_hg:,} 저₩{_lw:,} 시가₩{_oprc:,} 전일₩{_sdpr:,} 거래량비{_vol_ratio:.0f}% 대금{_trading_val/100000000:.0f}억 체결{_strength:.0f}% 매수잔{_buy_rem:,} 매도잔{_sell_rem:,}")
                            
                            # ① 등락률 3%↑ (★2점)
                            if _day_chg >= 3.0:
                                _surge_score += 2
                                _surge_detail.append(f"등락률+{_day_chg:.1f}%✅2")
                            else:
                                _surge_detail.append(f"등락률+{_day_chg:.1f}%❌0")
                            
                            # ② 체결강도 80%↑ (★2점)
                            if _strength >= 80:
                                _surge_score += 2
                                _surge_detail.append(f"체결{_strength:.0f}%✅2")
                            else:
                                _surge_detail.append(f"체결{_strength:.0f}%❌0")
                            
                            # ③ 고가 근접 97%↑ (★2점 — 아직 꼭대기)
                            _hg_ratio = round(cur_price / max(_hg, 1) * 100) if _hg > 0 else 0
                            if _hg_ratio >= 97:
                                _surge_score += 2
                                _surge_detail.append(f"고가{_hg_ratio}%✅2")
                            else:
                                _surge_detail.append(f"고가{_hg_ratio}%❌0")
                            
                            # ═══ 보조 (1.5점 × 2 = 3점) — 돈이 몰리는지 ═══
                            
                            # ④ 거래량 전일 200%↑ (1.5점)
                            if _vol_ratio >= 200:
                                _surge_score += 1.5
                                _surge_detail.append(f"거래량{_vol_ratio}%✅1.5")
                            else:
                                _surge_detail.append(f"거래량{_vol_ratio}%❌0")
                            
                            # ⑤ 거래대금 10억↑ (1.5점)
                            _tv_billion = _trading_val / 100000000
                            if _tv_billion >= 10:
                                _surge_score += 1.5
                                _surge_detail.append(f"대금{_tv_billion:.0f}억✅1.5")
                            else:
                                _surge_detail.append(f"대금{_tv_billion:.0f}억❌0")
                            
                            # ═══ 참고 (1점 × 5 = 5점) — 방향성 확인 ═══
                            
                            # ⑥ 현재가 > 시가 (1점)
                            if _oprc > 0 and cur_price > _oprc:
                                _surge_score += 1
                                _surge_detail.append("시가↑✅1")
                            else:
                                _surge_detail.append("시가↓❌0")
                            
                            # ⑦ 변동폭 3%↑ (1점)
                            _intra_range = (_hg - _lw) / max(_lw, 1) * 100 if _lw > 0 else 0
                            if _intra_range >= 3.0:
                                _surge_score += 1
                                _surge_detail.append(f"변동{_intra_range:.1f}%✅1")
                            else:
                                _surge_detail.append(f"변동{_intra_range:.1f}%❌0")
                            
                            # ⑧ 저가 이탈 아님 — 저가 대비 3%↑ (1점)
                            _lw_ratio = (cur_price - _lw) / max(_lw, 1) * 100 if _lw > 0 else 0
                            if _lw_ratio >= 3.0:
                                _surge_score += 1
                                _surge_detail.append(f"저가+{_lw_ratio:.1f}%✅1")
                            else:
                                _surge_detail.append(f"저가+{_lw_ratio:.1f}%❌0")
                            
                            # ⑨ 호가 매수우위 — 매수잔/매도잔 1.2↑ (1점)
                            _order_ratio = _buy_rem / max(_sell_rem, 1) if _sell_rem > 0 else 0
                            if _order_ratio >= 1.2:
                                _surge_score += 1
                                _surge_detail.append(f"호가{_order_ratio:.1f}배✅1")
                            else:
                                _surge_detail.append(f"호가{_order_ratio:.1f}배❌0")
                            
                            # ⑩ 시가 갭업 — 시가 > 전일종가 (1점)
                            if _oprc > 0 and _sdpr > 0 and _oprc > _sdpr:
                                _surge_score += 1
                                _gap = (_oprc - _sdpr) / _sdpr * 100
                                _surge_detail.append(f"갭업+{_gap:.1f}%✅1")
                            else:
                                _surge_detail.append("갭업❌0")
                            
                            # ═══ 판정 (14점 만점, 설정 기준%) ═══
                            _surge_pct = round(_surge_score / 14 * 100)
                            _detail_str = ' '.join(_surge_detail)
                            _surge_threshold_pct = int(cfg.get('surge_threshold', 70))
                            _surge_threshold_pts = _surge_threshold_pct / 100 * 14  # 70%→9.8점
                            
                            if _surge_score < _surge_threshold_pts:
                                print(f"[AI_BUY] 😴 {p_name}({p_ticker}) 생동감 {_surge_score}/14({_surge_pct}%) < {_surge_threshold_pct}%({_surge_threshold_pts:.1f}점) → 차단 [{_detail_str}]")
                                trade_log.append({"time": datetime.now().isoformat(), "date": today,
                                    "type": "BLOCKED", "ticker": p_ticker, "name": p_name,
                                    "message": f"😴 생동감 {_surge_score}/14({_surge_pct}%) [{_detail_str}]"})
                                save_state()
                                continue
                            
                            print(f"[AI_BUY] 🔥 {p_name}({p_ticker}) 생동감 {_surge_score}/14({_surge_pct}%) → 매수진행! [{_detail_str}]")
                            trade_log.append({"time": datetime.now().isoformat(), "date": today,
                                "type": "AI_MARKET_SCAN", "ticker": p_ticker, "name": p_name,
                                "message": f"🔥 생동감 {_surge_score}/14({_surge_pct}%) [{_detail_str}]"})

                            # ★★★ v4.0: 비율 기반 섹터 분산 강제 차단 ★★★
                            _pick_sector = _out.get('bstp_kor_isnm', '').strip()
                            if _pick_sector:
                                _sector_map[p_ticker] = _pick_sector
                                _sector_unique = bool(cfg.get('sector_unique', True))
                                
                                if _sector_unique:
                                    # ★ v8.0: 섹터 분산 ON → 같은 섹터 무조건 1종목만
                                    _cur_sector_list = _bought_sectors_this_scan.get(_pick_sector, [])
                                    _cur_sector_count = len(_cur_sector_list)
                                    if _cur_sector_count >= 1:
                                        _conflict_names = ', '.join(_cur_sector_list[:3])
                                        print(f"[SECTOR] 🚫 {p_name}({p_ticker}) [{_pick_sector}] 섹터 분산: 이미 {_conflict_names} 보유 → 차단")
                                        trade_log.append({
                                            "time": datetime.now().isoformat(), "date": today,
                                            "type": "BLOCKED", "ticker": p_ticker, "name": p_name,
                                            "message": f"🔒 섹터 분산 [{_pick_sector}] 이미 보유: {_conflict_names}"
                                        })
                                        save_state()
                                        skip_tickers.add(p_ticker)
                                        continue
                                    else:
                                        print(f"[SECTOR] ✅ {p_name} [{_pick_sector}] 섹터 분산 OK")
                                else:
                                    # ★★★ v8.0: 섹터 분산 OFF → 섹터 제한 없음 (설정값 존중) ★★★
                                    print(f"[SECTOR] ✅ {p_name} [{_pick_sector}] 섹터제한 OFF (제한없음)")

                            # ★★★ v4.0: 동적 포지션 사이징 + 호가 체크 + 분할매수 ★★★
                            
                            # 1. 기술적 지표 조회 (캐시 활용)
                            _pick_ta = get_technical_indicators(app_key, app_secret, mode, token, p_ticker)
                            
                            # ★★★ v6.0: 5일 횡보 종목 차단 (변동폭 3% 미만 = 단타 수익 불가) ★★★
                            # 5일 고점-저점 범위가 3% 미만이면 tp 3.5%에 절대 못 닿음
                            if _pick_ta and _pick_ta.get('high_5d') and _pick_ta.get('low_5d'):
                                _h5 = _pick_ta['high_5d']
                                _l5 = _pick_ta['low_5d']
                                _range_5d = (_h5 - _l5) / max(_l5, 1) * 100 if _l5 > 0 else 0
                                if 0 < _range_5d < 3.0:
                                    print(f"[AI_BUY] ➡️ {p_name}({p_ticker}) 5일 변동폭 {_range_5d:.1f}% < 3% → 횡보 제외")
                                    trade_log.append({
                                        "time": datetime.now().isoformat(), "date": today,
                                        "type": "BLOCKED", "ticker": p_ticker, "name": p_name,
                                        "message": f"➡️ 횡보 제외(5일 변동폭 {_range_5d:.1f}%<3%, 고:{_h5:,}/저:{_l5:,}) → 단타 수익 불가"
                                    })
                                    save_state()
                                    continue
                            
                            # 2. 호가잔량 + 체결강도 체크 (★ v8.0: 생동감 점수에서 이미 조회한 캐시 재사용)
                            _pick_ob = _ob if _ob else fetch_orderbook(app_key, app_secret, mode, token, p_ticker)
                            
                            # 2-1. 체결강도 약하면 매수 보류 (강한 매도세 = 위험)
                            if _pick_ob and _pick_ob.get('strength', 100) < 60:
                                print(f"[AI_BUY] ⚠️ {p_name}({p_ticker}) 체결강도 {_pick_ob['strength']}% 약함 → 매수 보류")
                                trade_log.append({
                                    "time": datetime.now().isoformat(), "date": today,
                                    "type": "BLOCKED", "ticker": p_ticker, "name": p_name,
                                    "message": f"⚠️ 체결강도 약함({_pick_ob['strength']}%<60%) → 매수 보류"
                                })
                                save_state()
                                continue
                            
                            # ★★★ v4.0 PHASE 4: 뉴스 센티멘트 체크 (악재 종목 매수 차단)
                            _pick_sent = get_stock_sentiment(p_ticker)
                            if _pick_sent and _pick_sent.get('score', 0) <= -30:
                                print(f"[AI_BUY] 🔴 {p_name}({p_ticker}) 악재뉴스 {_pick_sent['score']}점 → 매수 차단")
                                trade_log.append({
                                    "time": datetime.now().isoformat(), "date": today,
                                    "type": "BLOCKED", "ticker": p_ticker, "name": p_name,
                                    "message": f"🔴 악재뉴스 차단({_pick_sent['label']}, {_pick_sent['score']}점): {', '.join(_pick_sent.get('negative',[])[:2])}"
                                })
                                save_state()
                                skip_tickers.add(p_ticker)
                                continue
                            
                            # 3. 동적 포지션 사이징 (확신도 + 기술적 지표 + 호가)
                            _ps_amount, buy_qty, _ps_reason = calc_position_size(
                                max_buy_amount, p_conf, cur_price, _pick_ta, _pick_ob)
                            
                            if buy_qty <= 0:
                                print(f"[AI_BUY] {p_name}({p_ticker}) 포지션사이징 결과 0주 → 스킵")
                                continue
                            
                            # 4. 단타 전량매수 (분할매수 비활성화 — 단타는 한번에 전량 진입)
                            
                            # 5. ATR 기반 개별 tp/sl 로깅
                            if _pick_ta and _pick_ta.get('atr_pct', 0) > 0:
                                _ind_tp1, _ind_tp2, _ind_sl, _ind_trail, _ind_reason = calc_dynamic_tp_sl(_pick_ta)
                                print(f"[V4_TP] {p_name}: tp1={_ind_tp1}% tp2={_ind_tp2}% sl={_ind_sl}% trail={_ind_trail}% ({_ind_reason})")
                            
                            # ★★★ v8.0: sell_plan 삭제 — 설정탭 tp1/tp2/tp3/sl 값만 사용 ★★★
                            
                            # 7. ★ v6.0: RSI 기계적 필터 (매수 직전 최종 관문)
                            _buy_rsi = (_pick_ta or {}).get('rsi', 50)
                            if _buy_rsi > 80:
                                print(f"[RSI_BLOCK] 🚫 {p_name}({p_ticker}) RSI={_buy_rsi:.0f} > 80 과매수 → 매수 차단")
                                trade_log.append({"time": datetime.now().isoformat(), "date": today,
                                    "type": "AI_VERIFY_FAIL", "ticker": p_ticker, "name": p_name,
                                    "message": f"🚫 RSI {_buy_rsi:.0f} 과매수(>80) 차단 — 천정권 매수 방지"})
                                skip_tickers.add(p_ticker)
                                continue
                            if _buy_rsi < 25:
                                print(f"[RSI_BLOCK] 🚫 {p_name}({p_ticker}) RSI={_buy_rsi:.0f} < 25 폭락 → 매수 차단")
                                trade_log.append({"time": datetime.now().isoformat(), "date": today,
                                    "type": "AI_VERIFY_FAIL", "ticker": p_ticker, "name": p_name,
                                    "message": f"🚫 RSI {_buy_rsi:.0f} 폭락(<25) 차단 — 낙하산 잡기 방지"})
                                skip_tickers.add(p_ticker)
                                continue
                            
                            # 8. 전량 매수 실행
                            _premarket_tag = ''
                            _buy_reason = (f"AI스캔({p_name},{p_signal},{p_conf}%{_premarket_tag}) "
                                          f"| {_ps_reason} | 전량{buy_qty}주")
                            print(f"[AI_BUY] 🔴 v4매수: {p_name}({p_ticker}) {buy_qty}주(전량) x {cur_price:,}원")
                            try:
                                self._execute_buy(cfg, token, p_ticker, p_name, buy_qty, cur_price, _buy_reason)
                            except Exception as _be:
                                print(f"[AI_BUY] 매수 실패: {_be}")
                                skip_tickers.add(p_ticker)
                                continue

                            # 성공 여부 확인
                            buy_success = any(
                                t.get('ticker') == p_ticker and t.get('type') == 'AI_BUY' and t.get('success') is True
                                for t in trade_log[-5:]
                            )
                            if buy_success:
                                peak_prices[p_ticker] = cur_price
                                skip_tickers.add(p_ticker)
                                _cached_held.add(p_ticker)
                                # ★ v4.0 섹터 분산: 매수 성공한 종목의 섹터 등록
                                if _pick_sector:
                                    _bought_sectors_this_scan.setdefault(_pick_sector, []).append(p_ticker)
                                    _sc = len(_bought_sectors_this_scan[_pick_sector])
                                    _sl = max(2, int(int(cfg.get('max_positions', 6) or 6) * _sector_max_ratio + 0.5))
                                    print(f"[SECTOR] {p_name} [{_pick_sector}] 섹터 등록 {_sc}/{_sl}")
                                _spent = cur_price * buy_qty
                                cash -= _spent
                                # ★ 현금 트래커 직접 차감 (KIS 실시간 미반영 보완)
                                _cash_tracker['amount'] = max(0, _cash_tracker['amount'] - _spent)
                                _cash_tracker['ts'] = time.time() + 1  # KIS캐시보다 항상 최신
                                print(f"[AI_BUY] ✅ 매수 완료: {p_name}({p_ticker}) | 차감 {_spent:,.0f} | 잔여 {_cash_tracker['amount']:,.0f}")
                                _buys_this_scan += 1
                                if _buys_this_scan >= 5:  # 스캔당 최대 5종목
                                    print(f"[AI_BUY] 📦 스캔당 3종목 매수 완료 → 다음 스캔 대기")
                                    break
                                time.sleep(0.3)  # 연속 매수 간 짧은 딜레이
                            else:
                                print(f"[AI_BUY] ⛔ {p_name}({p_ticker}) 실패 → 다음 종목")
                                skip_tickers.add(p_ticker)  # 이번 사이클 재시도 방지
                                # ★ continue (break 없음) → 다음 종목 자동 시도

                        except Exception as _pe:
                            print(f"[AI_BUY] ❌ pick 처리 오류 {pick.get('ticker','?')}: {_pe}")
                            continue

                except Exception as e:
                    print(f"[AI_BUY] ❌ 스캔 처리 오류: {e}")
                    trade_log.append({
                        "time": datetime.now().isoformat(), "date": today,
                        "type": "AI_ERROR",
                        "message": f"스캔 처리 오류: {str(e)[:50]}"
                    })
                    save_state()
                # ★ picks가 있는데 전부 스킵됐으면 이유 로그
                # ★ S3: 필터 소요시간 기록 + _last_scan_timing 저장
                try:
                    _t_scan['필터'] = round(time.time() - _t4, 1)
                except (NameError, UnboundLocalError):
                    pass
                try:
                    self._last_scan_timing = _t_scan
                    _timing_log = ' | '.join(f"{k}:{v}초" for k, v in _t_scan.items() if v > 0)
                    if _timing_log:
                        print(f"[SCAN_TIME] 📊 단계별: {_timing_log}")
                except (NameError, UnboundLocalError):
                    pass
                
                _all_skipped = picks and all(p.get('ticker','') in skip_tickers for p in picks)
                if _all_skipped:
                    _skip_detail = ', '.join(f"{p.get('name','')}({'보유' if p.get('ticker','') in _cached_held else '차단'})" for p in picks[:5])
                    trade_log.append({
                        "time": datetime.now().isoformat(), "date": today, "mode": "live",
                        "type": "AI_MARKET_SCAN",
                        "message": f"⚠️ 모든 추천종목 스킵 ({_skip_detail}) → 다음 스캔 대기"
                    })
                    # ★ 스킵된 차단 종목 → 당일 perm_blocked 등록 (반복 추천 완전 차단)
                    _now = datetime.now().isoformat()
                    for p in picks:
                        _t = p.get('ticker','')
                        if _t and _t not in _cached_held and _t not in perm_blocked:
                            perm_blocked[_t] = f"오늘({today}) 반복스킵 → 당일제외"
                            trade_log.append({
                                "time": _now, "date": today,
                                "type": "BLOCKED", "ticker": _t, "name": p.get('name',''),
                                "message": f"⛔ AI추천 스킵 → {p.get('name','')}({_t}) 당일 제외 등록"
                            })
                    save_state()
            
            # ===== SPECIFIC TICKER MODE =====
            elif scan_mode == 'ai_autobuy':
                ticker = rule.get('ticker', '')
                if not ticker or ticker == '*':
                    continue
                
                # 이미 보유 중이면 스킵 (★ v8.0: auto_tickers + KIS 잔고 둘 다 체크)
                if ticker in _cached_held or ticker in auto_tickers:
                    continue
                
                # ★ FIX: collect_single_stock으로 수급/뉴스/재무까지 한 번에 수집
                try:
                    detail_str, raw_data = collect_single_stock(cfg, token, app_key, app_secret, mode, ticker)
                    cur_price = int(raw_data.get('cur_price', 0))
                    price_change = float(raw_data.get('chg_pct', 0))  # ★ FIX: collect_single_stock 반환 키는 'chg_pct'
                    volume = raw_data.get('volume', '0')
                    name_from_raw = raw_data.get('name', ticker)
                except Exception:
                    continue
                
                if cur_price <= 0:
                    continue
                
                try:
                    # ★ v8.0: 시황 데이터 삭제 — 종목 자체 데이터만 사용
                    ai_prompt = f"""한국 주식 {ticker}({name_from_raw}) 매수 여부를 판단해줘.
[종목 실시간 데이터]
{detail_str}

현재가: {cur_price:,}원 | 전일대비: {price_change:+.2f}% | 거래량: {volume}

판단 기준:
- 수급(외국인/기관 3일 순매수 여부)
- 뉴스 호재/악재
- 현재 시장 방향과의 연동성
- 기술적 위치 (당일 등락률 +5% 미만 초입인지 추격인지)

JSON만 반환.
{{"signal":"강력매수/매수/보유/매도/강력매도","confidence":0~100,"reason":"수급/뉴스/시장맥락 기반 판단근거","targetPrice":목표가,"stopLoss":손절가}}"""
                    
                    ai_text = call_ai(ai_prompt,
                        "한국 주식 매매 신호 분석가. 수급·뉴스·시장 맥락 기반으로 판단. JSON만 반환. 한글.",
                        600, web_search=False, tier='briefing')
                    json_match = re.search(r'\{[\s\S]*\}', ai_text)
                    if not json_match:
                        continue
                    ai_signal = safe_json_loads(json_match.group(), "AI_SELL")
                    
                    signal = ai_signal.get('signal', '보유')
                    confidence = float(ai_signal.get('confidence', 0))
                    min_confidence = float(rule.get('min_confidence', 80))
                    
                    trade_log.append({
                        "time": datetime.now().isoformat(), "date": today,
                        "type": "AI_SCAN", "ticker": ticker,
                        "message": f"AI분석[{name_from_raw}]: {signal} (확신도 {confidence}%) - {ai_signal.get('reason','')}",
                        "signal": signal, "confidence": confidence
                    })
                    save_state()
                    
                    if signal in ['강력매수', '매수']:
                        buy_qty = int(min(max_buy_amount, cash) / cur_price)
                        if buy_qty == 0 and cash >= cur_price:
                            buy_qty = 1
                        if buy_qty > 0:
                            self._execute_buy(cfg, token, ticker, name_from_raw, buy_qty, cur_price,
                                f"AI자동매수 ({signal}, {confidence}%)", cached_bal=bal)
                            peak_prices[ticker] = cur_price
                    
                except Exception as e:
                    trade_log.append({
                        "time": datetime.now().isoformat(), "date": today,
                        "type": "AI_ERROR", "ticker": ticker,
                        "message": f"AI 분석 오류: {str(e)}"
                    })
                    save_state()
    
    # ★★★ v8.0: _execute_watchlist 삭제 (시황분석 제거, 실시간 데이터만 사용) ★★★

    def _execute_buy(self, cfg, token, ticker, name, qty, price, reason, cached_bal=None, limit_price=0):
        # ★ 이름이 코드 그대로면 KIS → 네이버 순으로 조회 (매수는 항상 진행)
        if not name or name == ticker or not any(c.isalpha() for c in name):
            _got_name = ''
            # KIS 단건 조회 시도
            try:
                _nm_r = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
                    cfg.get('app_key',''), cfg.get('app_secret',''), 'live',
                    token, "FHKST01010100",
                    params={"FID_COND_MRKT_DIV_CODE":"J","FID_INPUT_ISCD":ticker})
                _got_name = _nm_r.get('output',{}).get('hts_kor_isnm','').strip()
            except:
                pass
            # KIS 이름 없으면 네이버 조회
            if not _got_name:
                _got_name = get_stock_name_naver(ticker)
            name = _got_name or ticker
            print(f"[BUY_NAME] {ticker} → '{name}'")
        mode = 'live'
        price = int(float(str(price).replace(',', '') if price else 0))
        qty = int(float(str(qty).replace(',', '') if qty else 0))
        if price <= 0 or qty <= 0:
            return

        now = datetime.now()
        today = now.strftime('%Y-%m-%d')
        hhmm = now.hour * 100 + now.minute
        mode = 'live'
        # paper 모드는 장시간 제한 없음 (프리마켓/애프터마켓 포함)
        if mode != 'paper' and (hhmm < 900 or hhmm > 1530):
            trade_log.append({"time": now.isoformat(), "date": today, "mode": "live",
                "type": "BLOCKED", "ticker": ticker, "name": name,
                "message": f"장운영시간 외 매수 차단 ({now.strftime('%H:%M')}) - {name or ticker}"})
            save_state()
            return
        
        # ★★★ v8.0: 12:00 이후 신규 매수 차단 (오전만 매매) ★★★
        if mode != 'paper' and hhmm >= 1200:
            trade_log.append({"time": now.isoformat(), "date": today, "mode": "live",
                "type": "BLOCKED", "ticker": ticker, "name": name,
                "message": f"⏸ 12:00 이후 매수 차단 ({now.strftime('%H:%M')}) - {name or ticker}"})
            save_state()
            return
        
        # ★ Fix: 장마감 강제청산 10분 전 신규 매수 중단 (매수→즉시청산 방지)
        _fc_on = cfg.get('force_close', False)
        _fc_time = int(cfg.get('force_close_time', 1515))
        _fc_cutoff = _fc_time - 10  # 청산 10분 전
        if _fc_on and hhmm >= _fc_cutoff:
            trade_log.append({"time": now.isoformat(), "date": today, "mode": "live",
                "type": "BLOCKED", "ticker": ticker, "name": name,
                "message": f"⏹ 장마감 청산 {_fc_time//100}:{_fc_time%100:02d} 전 매수 중단 ({now.strftime('%H:%M')}) - {name or ticker}"})
            save_state()
            return
        
        # ★ 15:20 이후 스캔 중단이지만 혹시 들어온 매수도 차단
        if hhmm >= 1520:
            trade_log.append({"time": now.isoformat(), "date": today, "mode": "live",
                "type": "BLOCKED", "ticker": ticker, "name": name,
                "message": f"⏹ 장 마감 후 매수 차단 ({now.strftime('%H:%M')}) - {name or ticker}"})
            save_state()
            return

        tr_id = "TTTC0802U"

        today = datetime.now().strftime('%Y-%m-%d')
        # ★ 매수 한도는 매수만 카운트 (매도까지 포함하면 매수 한도 빨리 소진)
        # 일일 매수 횟수 제한 없음 - 보유종목 수/일일손실한도로만 제한
        
        # === CAPITAL MANAGEMENT: 종목수 한도 체크 ===
        buy_amount = price * qty
        # ★★★ v8.0: self.config에서 직접 읽기 (스캔 중 설정 변경 즉시 반영) ★★★
        max_pos = int(self.config.get('max_positions', 6)) if hasattr(self, 'config') and self.config else int(cfg.get('max_positions', 6))
        today = datetime.now().strftime('%Y-%m-%d')

        # ★★★ v8.0: 보유종목 수 (거래정지 제외 — 매도 불가 종목은 슬롯 차지 안 함) ★★★
        held_count = len([t for t in auto_tickers if not _is_truly_halted(t)])
        if held_count >= max_pos:
            trade_log.append({"time": datetime.now().isoformat(), "date": today, "type": "CAPITAL_BLOCK",
                "ticker": ticker, "name": name or "",
                "message": f"📋 종목수 제한: {held_count}종목 / 최대 {max_pos}종목"})
            save_state()
            print(f"[BUY_BLOCK] {name}({ticker}) 슬롯 꽉참 {held_count}/{max_pos} → 매수 차단")
            return
        
        # ★★★ v8.0: 재매수 가격 체크 — 직전 익절가보다 높으면 매수 차단 ★★★
        # 익절한 종목을 더 비싸게 다시 사면 손해 → 익절가 아래에서만 재매수 허용
        _last_sell_price = 0
        for _t in reversed(trade_log):
            if (_t.get('ticker') == ticker and _t.get('date') == today 
                and _t.get('type') in ('SELL', 'FORCE_CLOSE') 
                and _t.get('success') in (True, 1, 'true')
                and float(_t.get('pnl', 0) or 0) > 0):  # 익절 건만
                _last_sell_price = int(float(_t.get('price', 0) or 0))
                break
        if _last_sell_price > 0 and price >= _last_sell_price:
            trade_log.append({"time": datetime.now().isoformat(), "date": today, "type": "BLOCKED",
                "ticker": ticker, "name": name or "",
                "message": f"🚫 재매수 차단: 현재₩{price:,} >= 익절가₩{_last_sell_price:,} (익절가 아래서만 재매수)"})
            save_state()
            print(f"[BUY_BLOCK] {name}({ticker}) 재매수 차단: 현재₩{price:,} >= 익절가₩{_last_sell_price:,}")
            return
        
        # === EXECUTE ORDER ===
        # 가격은 scan에서 이미 조회했으므로 추가 확인 없이 그대로 주문
            
        try:
            # ★★★ v4.1: 최우선호가 지정가 매수 + 30초 미체결 시 시장가 전환 ★★★
            _best_ask = 0
            try:
                _ob = fetch_orderbook(cfg.get('app_key',''), cfg.get('app_secret',''), mode, token, ticker)
                if _ob and _ob.get('ask1_price'):
                    _best_ask = int(_ob['ask1_price'])
            except:
                pass
            
            if _best_ask > 0 and _best_ask <= price * 1.03:  # 현재가 대비 3% 이내만 지정가
                ord_dvsn = "00"  # 지정가
                ord_unpr = str(_best_ask)
                _order_type = f"최우선호가 ₩{_best_ask:,}"
            elif limit_price and int(limit_price) > 0:
                ord_dvsn = "00"  # 지정가
                ord_unpr = str(int(limit_price))
                _order_type = f"지정가 ₩{int(limit_price):,}"
            else:
                ord_dvsn = "01"  # 시장가
                ord_unpr = "0"
                _order_type = "시장가"
            
            print(f"[BUY] {_order_type} 주문: {name}({ticker}) {qty}주")
            
            result = kis_request("POST", "/uapi/domestic-stock/v1/trading/order-cash",
                cfg['app_key'], cfg['app_secret'], mode, token, tr_id,
                body={"CANO": cfg['account'], "ACNT_PRDT_CD": cfg.get('account_cd', '01'),
                      "PDNO": ticker, "ORD_DVSN": ord_dvsn, "ORD_QTY": str(qty),
                      "ORD_UNPR": ord_unpr})
            
            success = result.get('rt_cd') == '0'
            msg1 = result.get('msg1', '')
            _order_no = result.get('output', {}).get('ODNO', '')
            
            # ★ 지정가 주문 성공 → 2초 후 미체결 확인 → 시장가 전환 (5초→2초 단축)
            if success and ord_dvsn == "00" and _order_no:
                import time as _time_mod
                _time_mod.sleep(2)  # 2초 대기 후 체결 확인 (최우선호가는 보통 1~2초 내 체결)
                
                # 체결 여부: 잔고에 종목이 있으면 체결됨
                _filled = False
                try:
                    _chk_bal = get_balance(cfg['app_key'], cfg['app_secret'], mode, token,
                                          cfg['account'], cfg.get('account_cd','01'), max_age=0)
                    for _bp in _chk_bal.get('output1', []):
                        if _bp.get('pdno') == ticker and int(_bp.get('hldg_qty','0') or 0) > 0:
                            _filled = True
                            break
                except:
                    _filled = True  # 확인 실패 시 체결된 것으로 간주
                
                if not _filled:
                    # 미체결 → 주문 취소 후 시장가 재주문
                    print(f"[BUY] ⏳ {name}({ticker}) 지정가 미체결 → 취소 후 시장가 전환")
                    try:
                        # 주문 취소
                        kis_request("POST", "/uapi/domestic-stock/v1/trading/order-rvsecncl",
                            cfg['app_key'], cfg['app_secret'], mode, token, "TTTC0803U",
                            body={"CANO": cfg['account'], "ACNT_PRDT_CD": cfg.get('account_cd','01'),
                                  "KRX_FWDG_ORD_ORGNO": "", "ORGN_ODNO": _order_no,
                                  "ORD_DVSN": "01", "RVSE_CNCL_DVSN_CD": "02",
                                  "ORD_QTY": str(qty), "ORD_UNPR": "0", "QTY_ALL_ORD_YN": "Y"})
                        _time_mod.sleep(0.5)
                        
                        # 시장가 재주문
                        result = kis_request("POST", "/uapi/domestic-stock/v1/trading/order-cash",
                            cfg['app_key'], cfg['app_secret'], mode, token, tr_id,
                            body={"CANO": cfg['account'], "ACNT_PRDT_CD": cfg.get('account_cd','01'),
                                  "PDNO": ticker, "ORD_DVSN": "01", "ORD_QTY": str(qty),
                                  "ORD_UNPR": "0"})
                        success = result.get('rt_cd') == '0'
                        msg1 = f"지정가→시장가전환: {result.get('msg1','')}"
                        _order_type = "최우선→시장가전환"
                        print(f"[BUY] ✅ 시장가 전환 {'성공' if success else '실패'}")
                    except Exception as _cancel_e:
                        print(f"[BUY] 취소/재주문 실패: {_cancel_e}")
                        msg1 = f"취소실패: {_cancel_e}"
                else:
                    print(f"[BUY] ✅ 최우선호가 체결 완료: {name} @ ₩{_best_ask:,}")
            
            # ★ 주문가능금액 부족 → rt_cd='1' 응답으로 오는 경우 수량 절반 재시도
            # ★ 주문가능금액 부족 시 캐시 무효화 → 다음 매수 때 신선한 잔고 재조회
            if not success and any(kw in msg1 for kw in ['주문가능금액', '가능금액', '부족']):
                _bal_cache['ts'] = 0
            if not success and any(kw in msg1 for kw in ['주문가능금액', '가능금액', '부족', '매매불가', '매매정지', '종목', '처리가 안되었습니다']):
                retry_qty = qty // 2
                if retry_qty > 0:
                    print(f"[BUY_RETRY] {name}({ticker}) 주문가능금액 부족 → {qty}주→{retry_qty}주 재시도")
                    retry_result = kis_request("POST", "/uapi/domestic-stock/v1/trading/order-cash",
                        cfg['app_key'], cfg['app_secret'], mode, token, tr_id,
                        body={"CANO": cfg['account'], "ACNT_PRDT_CD": cfg.get('account_cd', '01'),
                              "PDNO": ticker, "ORD_DVSN": ord_dvsn, "ORD_QTY": str(retry_qty),
                              "ORD_UNPR": ord_unpr})
                    if retry_result.get('rt_cd') == '0':
                        result = retry_result
                        qty = retry_qty
                        buy_amount = price * qty
                        success = True
                        msg1 = f"↩️재시도({retry_qty}주) " + retry_result.get('msg1', '')
                        print(f"[BUY_RETRY] ✅ 성공")
                    else:
                        msg1 = f"재시도실패: {retry_result.get('msg1','')}"
                        print(f"[BUY_RETRY] ❌ {msg1}")

            # ★ v8.0: sell_plan 삭제 — 설정탭 값만 사용
            
            trade_log.append({
                "time": datetime.now().isoformat(), "date": today, "mode": "live",
                "type": "AI_BUY", "ticker": ticker, "name": name,
                "qty": qty, "price": price, "reason": reason,
                "success": success, "message": msg1,
                "ai_model": ai_config.get('last_model', 'unknown'),  # ★ v6.0: 매수 AI 모델
                "sector": '',
                "order_no": result.get('output', {}).get('ODNO', ''),
                "sell_plan": _sell_plans.get(ticker, {})  # ★ 이제 값이 있음!
            })
            # Track as auto-trading position
            if success and ticker not in auto_tickers:
                auto_tickers.append(ticker)
            if success:
                auto_avg_count[ticker] = 0  # ★ v6.0: 신규매수 물타기 횟수 초기화
                _sell_stage.pop(ticker, None)  # ★ v6.0: 익절 단계 초기화 (재매수 시 즉시매도 방지)
                _tp1_triggered.discard(ticker)  # ★ v6.0: tp1 트리거 초기화
                _bal_cache['ts'] = 0  # ★ v6.0: 잔고 캐시 무효화 (보유종목 즉시 반영)
                _auto_status_cache['ts'] = 0  # ★ v8.0: auto/status 캐시 즉시 무효화
                save_state()  # ★ v6.0: 매수 즉시 저장
                tg_buy(name, ticker, qty, price, reason=reason)
                # ★ v3.0: WebSocket 매수 이벤트 push
                sync_broadcast('buy', {
                    'ticker': ticker, 'name': name, 'qty': qty, 'price': price,
                    'reason': reason, 'time': datetime.now().strftime('%H:%M:%S'),
                    'success': True
                })
            # ★ v4.0: 주문 체결 추적 등록
            _odno = result.get('output', {}).get('ODNO', '')
            if success and _odno:
                track_pending_order(_odno, ticker, name, qty, 'BUY', price)
            # 주문 실패 분류: 영구차단 vs 임시차단(60분)
            if not success and msg1:
                _perm_kws = ['매매불가', '매매정지', '매매종목', '처리가 안되었습니다', '상장폐지', '거래정지']
                if any(kw in msg1 for kw in _perm_kws):
                    # ★ v8.0: 거래정지 명시면 HALTED:, 아니면 BUY_FAIL:
                    if '거래정지' in msg1 or '매매정지' in msg1:
                        perm_blocked[ticker] = f"HALTED: {msg1[:50]}"
                    else:
                        perm_blocked[ticker] = f"BUY_FAIL: {msg1[:50]}"
                    trade_log.append({
                        "time": datetime.now().isoformat(), "date": today,
                        "type": "BLOCKED", "ticker": ticker, "name": name,
                        "message": f"🚫 영구차단: {name}({ticker}) - {msg1[:40]} → 이후 스캔 완전 제외"
                    })
                    save_state()
                else:
                    # 임시 차단: 60분 쿨다운
                    trade_log.append({
                        "time": datetime.now().isoformat(), "date": today, "mode": "live",
                        "type": "BLOCKED", "ticker": ticker, "name": name,
                        "message": f"⛔ 주문거부({msg1[:40]}) → {name}({ticker}) 60분 스캔 제외"
                    })
        except Exception as e:
            err_msg = str(e)
            trade_log.append({
                "time": datetime.now().isoformat(), "date": today,
                "type": "AI_BUY", "ticker": ticker, "name": name,
                "qty": qty, "price": price, "reason": reason,
                "success": False,
                "message": f"❌ 주문실패: {err_msg[:80]}"
            })
            print(f"[BUY_FAIL] {name}({ticker}) {err_msg}")
            
            # HTTP 에러도 블랙리스트 분류
            _perm_kws2 = ['매매불가', '매매정지', '처리가 안되었습니다', '상장폐지', '거래정지']
            if any(kw in err_msg for kw in _perm_kws2):
                if '거래정지' in err_msg or '매매정지' in err_msg:
                    perm_blocked[ticker] = f"HALTED: {err_msg[:50]}"
                else:
                    perm_blocked[ticker] = f"BUY_FAIL: {err_msg[:50]}"
                trade_log.append({
                    "time": datetime.now().isoformat(), "date": today,
                    "type": "BLOCKED", "ticker": ticker, "name": name,
                    "message": f"🚫 영구차단: {name}({ticker}) - {err_msg[:40]}"
                })
                save_state()
            elif any(kw in err_msg for kw in ['500', '503', '502', 'Internal Server', 'Server Error']):
                trade_log.append({
                    "time": datetime.now().isoformat(), "date": today, "mode": "live",
                    "type": "BLOCKED", "ticker": ticker, "name": name,
                    "message": f"🛑 KIS서버오류({err_msg[:40]}) → {name}({ticker}) 60분 스캔 제외"
                })
            # ★ 주문가능금액 부족 → 수량 절반으로 줄여서 1회 재시도
            elif any(kw in err_msg for kw in ['주문가능금액', '가능금액', '부족', '잔고부족']):
                retry_qty = qty // 2
                if retry_qty > 0:
                    print(f"[BUY_RETRY] {name}({ticker}) 수량 {qty}→{retry_qty}주 재시도")
                    try:
                        retry_result = kis_request("POST", "/uapi/domestic-stock/v1/trading/order-cash",
                            cfg['app_key'], cfg['app_secret'], mode, token, tr_id,
                            body={"CANO": cfg['account'], "ACNT_PRDT_CD": cfg.get('account_cd', '01'),
                                  "PDNO": ticker, "ORD_DVSN": ord_dvsn, "ORD_QTY": str(retry_qty),
                                  "ORD_UNPR": ord_unpr})
                        retry_ok = retry_result.get('rt_cd') == '0'
                        trade_log[-1]['message'] = f"↩️재시도({retry_qty}주): " + retry_result.get('msg1', '')
                        trade_log[-1]['qty'] = retry_qty
                        trade_log[-1]['success'] = retry_ok
                        if retry_ok and ticker not in auto_tickers:
                            auto_tickers.append(ticker)
                        print(f"[BUY_RETRY] {'✅ 성공' if retry_ok else '❌ 실패'}: {retry_result.get('msg1','')}")
                    except Exception as _re:
                        print(f"[BUY_RETRY] ❌ 재시도 실패: {_re}")
            # ★ 매매불가 종목 → 오늘 스캔에서 제외
            elif any(kw in err_msg for kw in ['매매불가', '주문처리가', '불가 종목']):
                trade_log.append({
                    "time": datetime.now().isoformat(), "date": today,
                    "type": "BLOCKED", "ticker": ticker, "name": name,
                    "message": f"🚫 매매불가 종목 등록 - 오늘 스캔 제외: {name}({ticker})"
                })
        save_state()
    
    # ★★★ v5.0: AI 매도 판단 함수 ★★★
    _ai_sell_cache = {}  # {ticker: {'decision': ..., 'ts': time.time()}}
    AI_SELL_COOLDOWN = 60  # 같은 종목 AI 매도 판단 60초 쿨다운
    
    # ★★★ v8.0: _build_sell_learning_ctx 삭제 (호출 0곳, 매도학습은 종가 업데이트로 대체) ★★★
    
    def _build_target_context(self, cfg, today_pnl):
        """당일 목표 달성률 컨텍스트 생성 (AI 매도 프롬프트용)
        ★ AI 목표 = 설정목표 × 1.2 (버퍼 20%) → 목표 달성해도 매매 계속"""

        lines = []
        _target_amt = float(cfg.get('daily_target_amt', 0) or 0)
        _target_pct = float(cfg.get('daily_target', 10) or 0)
        _loss_amt = float(cfg.get('daily_loss_amt', 0) or 0)
        _loss_pct = float(cfg.get('daily_loss_limit', 3) or 0)
        _target_liq = bool(cfg.get('daily_target_liquidate', False))
        _loss_liq = bool(cfg.get('daily_loss_liquidate', False))
        _target_buf = float(cfg.get('target_liq_buf', 10) or 10)  # 버퍼 %
        _loss_buf = float(cfg.get('loss_liq_buf', 10) or 10)
        
        if _target_amt > 0:
            # ★ AI 목표 = 설정목표 + 20% (버퍼) → 목표 도달해도 더 벌도록
            _ai_target = _target_amt * 1.2
            _progress = today_pnl / _ai_target * 100 if _ai_target else 0
            _remain = _ai_target - today_pnl
            lines.append(f"수익목표: ₩{_ai_target:,.0f} (달성률 {_progress:.0f}%, 남은금액 ₩{_remain:+,.0f})")
            if _progress >= 100:
                lines.append("🎯 목표 초과 달성! → 수익 보존, 리스크 줄이되 매매는 계속")
            elif _progress >= 80:
                lines.append("⚠️ 목표 근접(80%+) → 확실한 수익 위주, 무리한 홀드 자제")
            elif _progress >= 50:
                lines.append("📈 목표 절반 달성 → 균형잡힌 판단 (공격+방어)")
            # 실제 청산선 정보 (버퍼 포함)
            if _target_liq:
                _actual_liq = _target_amt * (1 + _target_buf / 100)
                lines.append(f"청산선: ₩{_actual_liq:,.0f} (목표+버퍼{_target_buf}%) 도달 시 전종목 청산")
        elif _target_pct > 0:
            lines.append(f"수익목표: {_target_pct}% (금액 미설정)")
        
        if _loss_amt > 0:
            # ★ 손실한도도 버퍼 적용
            _ai_loss = _loss_amt * 1.2
            _loss_progress = abs(today_pnl) / _ai_loss * 100 if today_pnl < 0 and _ai_loss else 0
            lines.append(f"손실한도: ₩{_ai_loss:,.0f} (현재손실 ₩{today_pnl:,.0f}, 한도소진 {_loss_progress:.0f}%)")
            if _loss_progress >= 70:
                lines.append("🚨 손실한도 70%+ 소진 → 추가 손실 절대 방지! 빠른 매도 우선")
            elif _loss_progress >= 50:
                lines.append("⚠️ 손실한도 절반 소진 → 방어적 매도 권장")
            if _loss_liq:
                _actual_loss_liq = _loss_amt * (1 + _loss_buf / 100)
                lines.append(f"손실청산선: ₩{_actual_loss_liq:,.0f} (한도+버퍼{_loss_buf}%) 도달 시 전종목 청산")
        elif _loss_pct > 0:
            lines.append(f"손실한도: {_loss_pct}%")
        
        # 현재 상황 요약
        if today_pnl > 0:
            lines.append(f"현재 상태: 수익 중 (₩{today_pnl:+,.0f}) → 계속 공격 가능")
        elif today_pnl < 0:
            lines.append(f"현재 상태: 손실 중 (₩{today_pnl:+,.0f}) → 방어적 매도 권장")
        else:
            lines.append("현재 상태: 본전")
        
        return '\n'.join(lines) if lines else "목표 미설정"
    
    
    def _ai_sell_decision(self, cfg, token, ticker, name, qty, cur_price, avg_price, pnl_pct, trigger_reason):
        """AI에게 매도 여부 판단 요청. 리턴: 'SELL'/'HOLD'/'PARTIAL_40'/'PARTIAL_30'"""
        
        # 쿨다운 체크
        _cd_sec = int(cfg.get('ai_sell_cooldown', 60))
        _cached = self._ai_sell_cache.get(ticker)
        if _cached and (time.time() - _cached['ts']) < _cd_sec:
            return _cached['decision']
        
        try:
            _t0 = time.time()
            
            # ─── 데이터 수집 (캐시 활용, 추가 API 최소화) ───
            _snap = {}
            try:
                _sp = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
                    cfg['app_key'], cfg['app_secret'], 'live', token, "FHKST01010100",
                    params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker})
                _so = _sp.get('output', {})
                _snap = {
                    'open': int(_so.get('stck_oprc', 0) or 0),
                    'high': int(_so.get('stck_hgpr', 0) or 0),
                    'low': int(_so.get('stck_lwpr', 0) or 0),
                    'cur': int(_so.get('stck_prpr', 0) or 0),
                    'prev_close': int(_so.get('stck_sdpr', 0) or 0),
                    'day_chg': float(_so.get('prdy_ctrt', 0) or 0),
                    'volume': int(_so.get('acml_vol', 0) or 0),
                    'strength': float(_so.get('tday_rltv', 0) or 0),
                }
            except: pass
            
            # 호가잔량 (캐시 15초)
            _ob = {}
            try:
                _ob = fetch_orderbook(cfg['app_key'], cfg['app_secret'], 'live', token, ticker) or {}
            except: pass
            
            # 보유시간
            _hold_min = 0
            _buy_confidence = 0
            _buy_entry = next((t for t in reversed(trade_log) 
                if t.get('ticker') == ticker and t.get('type') in ('AI_BUY','BUY','CHAT_BUY') 
                and t.get('success')), None)
            if _buy_entry:
                try:
                    _bt = datetime.fromisoformat(_buy_entry['time'])
                    _hold_min = int((datetime.now() - _bt).total_seconds() / 60)
                except: pass
                _buy_confidence = _buy_entry.get('confidence', 0)
            
            # 고점 대비
            _peak = peak_prices.get(ticker, cur_price)
            _from_peak = round((cur_price - _peak) / max(_peak, 1) * 100, 2) if _peak > 0 else 0
            
            # 오늘 실적 상황
            _today = datetime.now().strftime('%Y-%m-%d')
            _today_sells = [t for t in trade_log if t.get('date') == _today and t.get('type') == 'SELL' and t.get('success')]
            _today_pnl = sum(float(t.get('pnl', 0)) for t in _today_sells)
            _today_wins = sum(1 for t in _today_sells if float(t.get('pnl', 0)) > 0)
            _today_losses = sum(1 for t in _today_sells if float(t.get('pnl', 0)) < 0)
            
            # 과거 매도학습: 이 종목과 비슷한 상황에서의 결과
            _similar_sells = [t for t in trade_log if t.get('type') == 'SELL' and t.get('success') 
                and t.get('stock_data', {}).get('after_sell_pct') is not None][-20:]  # 최근 20건
            _premature_sells = sum(1 for t in _similar_sells if (t.get('stock_data', {}).get('after_sell_pct', 0) or 0) > 3)
            _good_sells = sum(1 for t in _similar_sells if (t.get('stock_data', {}).get('after_sell_pct', 0) or 0) < -1)
            
            # ─── AI 프롬프트 ───
            _tp1 = float(cfg.get('tp1', 3.5))
            _sl_val = float(cfg.get('sl', 0))  # ★ v8.0: 기본값 0 = 손절없음
            _is_loss_trigger = pnl_pct < 0  # 손실 상황인지
            _stage = _sell_stage.get(ticker, 0)
            
            # 상황별 선택지
            _is_proactive = '선제점검' in trigger_reason
            if _is_loss_trigger:
                _action_guide = """★ 손실 구간 선택지 (2개만):
  "SELL" = 전량 즉시 매도 (손절은 빠르게, 한번에!)
  "HOLD" = 홀드 (일시적 하락이라 판단, 반등 기대)
  ※ 손절은 분할매도 불가! SELL=무조건 전량"""
            elif _is_proactive:
                _action_guide = f"""★ AI 선제 점검 선택지 (4개):
  "SELL" = {'전량 매도' if pnl_pct < 0 else '전량 익절'} (추세 전환 감지, 지금이 최적)
  "PARTIAL_40" = 40% 부분매도 (일부 수익 확보, 리스크 줄이기)
  "PARTIAL_30" = 30% 부분매도 (소량만 확보)
  "HOLD" = 계속 보유 (아직 추세 유지 중)
  ※ AI 선제 점검: tp/sl 미도달이지만 현재 추세·수급·분봉 기반으로 매도 필요성 판단
  ※ 매도할 확실한 이유가 없으면 HOLD 추천 (불필요한 매도 자제)
  현재 매도단계: {_stage}차 완료"""
            else:
                _action_guide = f"""★ 수익 구간 선택지 (4개):
  "SELL" = 전량 매도 (수익 확정, 더 안 간다 판단)
  "PARTIAL_40" = 40% 부분매도 (수익 일부 확보, 나머지 홀드)
  "PARTIAL_30" = 30% 부분매도 (소량 확보, 상승 여력 큼)
  "HOLD" = 홀드 (아직 더 간다, 매도 안 함)
  현재 매도단계: {_stage}차 완료 (0=미매도, 1=40%매도됨, 2=70%매도됨)"""
            
            prompt = f"""[AI 매도 판단 요청]

종목: {name}({ticker})
매수가: ₩{avg_price:,.0f} → 현재가: ₩{cur_price:,.0f} ({pnl_pct:+.1f}%)
보유시간: {_hold_min}분
매수 확신도: {_buy_confidence}%
트리거: {trigger_reason}
상황: {'📉 손실구간' if _is_loss_trigger else ('🔍 AI선제점검 (tp/sl미도달)' if _is_proactive else '📈 수익구간')}

[당일 시세]
시가: ₩{_snap.get('open',0):,} | 고가: ₩{_snap.get('high',0):,} | 저가: ₩{_snap.get('low',0):,}
등락률: {_snap.get('day_chg',0):+.1f}% | 거래량: {_snap.get('volume',0):,}주
체결강도: {_snap.get('strength',0):.0f}%

[수급/호가]
매수1호가: {_ob.get('bid1_price',0)} ({_ob.get('bid1_qty',0)}주)
매도1호가: {_ob.get('ask1_price',0)} ({_ob.get('ask1_qty',0)}주)
호가스프레드: {round(abs(int(_ob.get('ask1_price',0) or 0) - int(_ob.get('bid1_price',0) or 0)) / max(cur_price,1) * 100, 2)}%

[포지션]
고점: ₩{_peak:,.0f} (고점대비 {_from_peak:+.1f}%)
설정: 익절 +{_tp1}% / 손절 {_sl_val}%

[오늘 실적]
매도 {len(_today_sells)}건 | 수익₩{_today_pnl:+,.0f} | {_today_wins}승/{_today_losses}패

[당일 목표]
{self._build_target_context(cfg, _today_pnl)}

[과거 매도학습]
최근 매도 {len(_similar_sells)}건 중 너무 일찍 판 건(매도후+3%↑): {_premature_sells}건
적절히 판 건(매도후-1%↓): {_good_sells}건

★ 판단 기준:
{'- ⚠️ 손실 구간: SELL이면 반드시 전량! 분할매도 절대 불가' if _is_loss_trigger else ('- 🔍 선제점검: 확실한 매도 신호 없으면 HOLD! 불필요한 매도 자제\n- 🔍 매도 신호: 분봉하락전환+체결강도↓+외인매도 = 3개 이상 겹치면 SELL' if _is_proactive else '- 수익 구간: 상승 강도에 따라 전량/부분/홀드 유연하게 판단')}
- 장초반(~09:30) 급락은 일시적일 수 있음 → 외인매수+거래량↑이면 홀드 고려
- 체결강도 120%+이면 매수세 강함 → 급하게 팔지 말 것
- 체결강도 80% 이하면 매도세 → 빨리 매도
- 호가 스프레드 2%+이면 슬리피지 위험 → 빨리 매도
- 보유시간 3분 이내 + 손실 = 장초반 변동성 가능성
- 과거에 너무 일찍 팔아서 손해 본 패턴 참고
- ★ 당일 목표 근접 시: 확실한 수익 확보 우선 (욕심 자제)
- ★ 당일 목표 초과 달성 시: 수익 보존, 공격적 홀드 자제
- ★ 당일 손실 한도 근접 시: 추가 손실 방지 최우선, 빠른 매도

{_action_guide}

★ 반드시 아래 JSON만 응답:
{{"action": "{'SELL 또는 HOLD' if _is_loss_trigger else 'SELL 또는 HOLD 또는 PARTIAL_40 또는 PARTIAL_30'}",
  "reason": "판단 근거 30자 이내",
  "confidence": 70~100,
  "hold_minutes": 0~30 (HOLD시 재판단까지 대기분)}}"""

            system_msg = ("한국 주식 매도 타이밍 전문가. 보유종목의 매도/홀드를 판단. "
                "데이터 기반 냉정한 판단. JSON만 반환. 확실하지 않으면 기계적 매도(SELL) 추천.")
            
            ai_text = call_ai(prompt, system_msg, 500, web_search=False, tier='sell')
            
            # JSON 파싱
            json_match = re.search(r'\{[\s\S]*\}', ai_text)
            if not json_match:
                print(f"[AI_SELL] {name} JSON 파싱 실패 → 기계적 매도")
                return 'SELL'
            
            result = safe_json_loads(json_match.group(), "AI_SELL")
            action = result.get('action', 'SELL').upper()
            reason = result.get('reason', '')
            confidence = int(result.get('confidence', 70))
            hold_min = int(result.get('hold_minutes', 0))
            
            # 유효성 검증
            if action not in ('SELL', 'HOLD', 'PARTIAL_40', 'PARTIAL_30'):
                action = 'SELL'
            
            # ★ 손실 구간: PARTIAL 강제 차단 → SELL로 전환
            if pnl_pct < 0 and action in ('PARTIAL_40', 'PARTIAL_30'):
                action = 'SELL'
                reason += ' (손절=전량, 분할불가)'
                print(f"[AI_SELL] {name} 손실구간 PARTIAL→SELL 강제전환")
            
            # HOLD인데 확신도 낮으면 → SELL
            _min_conf = int(cfg.get('ai_sell_min_conf', 70))
            if action == 'HOLD' and confidence < _min_conf:
                action = 'SELL'
                reason += f' (확신도{confidence}%<{_min_conf}%→매도)'
            
            # 쿨다운 설정
            _cd = max(hold_min * 60, _cd_sec) if action == 'HOLD' else _cd_sec
            self._ai_sell_cache[ticker] = {'decision': action, 'ts': time.time(), 'reason': reason}
            
            _elapsed = round(time.time() - _t0, 1)
            print(f"[AI_SELL] {name}({ticker}) {pnl_pct:+.1f}% → {action} ({confidence}%) '{reason}' [{_elapsed}초]")
            
            # 로그 기록
            trade_log.append({
                "time": datetime.now().isoformat(), "date": _today,
                "type": "AI_SELL_DECISION", "ticker": ticker, "name": name,
                "message": f"🧠 {action}: {name} {pnl_pct:+.1f}% | {reason} ({confidence}%)",
                "ai_action": action, "ai_reason": reason, "ai_confidence": confidence,
                "pnl_pct": pnl_pct, "trigger": trigger_reason
            })
            
            return action
            
        except Exception as e:
            print(f"[AI_SELL] ❌ AI 판단 실패 ({name}): {e} → 기계적 매도")
            return 'SELL'  # AI 실패 시 안전하게 기계적 매도
    
    # ★★★ v5.0: 계층2 — 미니AI 보유종목 상태 분류 (GPT-4o-mini, 2~3초) ★★★
                # v8: _classify_positions 삭제 (AI매도 모드 제거)
    def _execute_sell(self, cfg, token, ticker, name, qty, price, reason, avg_price=0, trade_mode='auto'):
        mode = 'live'
        today = datetime.now().strftime('%Y-%m-%d')
        
        # ★ v3.0 근본 FIX: avg_price=0이면 KIS 잔고 캐시에서 자동 조회
        # 이렇게 하면 어떤 경로에서 매도해도 항상 평단가가 들어감
        if avg_price <= 0 or price <= 0:
            try:
                for _p in (_bal_cache.get('data', {}) or {}).get('output1', []):
                    if _p.get('pdno') == ticker:
                        if avg_price <= 0:
                            avg_price = float(_p.get('pchs_avg_pric', '0') or 0)
                        if price <= 0:
                            price = float(_p.get('prpr', '0') or 0) or float(_p.get('pchs_avg_pric', '0') or 0)
                        break
                if avg_price > 0:
                    print(f"[SELL] 평단가 자동 조회: {name}({ticker}) avg=₩{avg_price:,.0f}")
            except Exception:
                pass

        # ★ 이름 보정: 빈 값/코드만/공백이면 KIS→네이버 순으로 조회
        name = (name or '').strip()
        if not name or name == ticker or not any(c.isalpha() for c in name):
            try:
                _nm_r = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
                    cfg.get('app_key',''), cfg.get('app_secret',''), 'live',
                    token, "FHKST01010100",
                    params={"FID_COND_MRKT_DIV_CODE":"J","FID_INPUT_ISCD":ticker})
                name = _nm_r.get('output',{}).get('hts_kor_isnm','').strip()
            except: pass
            if not name:
                name = get_stock_name_naver(ticker) or ticker
            print(f"[SELL_NAME] {ticker} → '{name}'")

        tr_id = "TTTC0801U"

        today = datetime.now().strftime('%Y-%m-%d')

        # ★ 강화된 중복 매도 방지: 종목별 락 + 쿨다운
        import threading
        if not hasattr(self, '_sell_locks'):
            self._sell_locks = {}
        
        if ticker not in self._sell_locks:
            self._sell_locks[ticker] = threading.Lock()
        
        # 논블로킹 락: 이미 매도 중이면 즉시 포기
        if not self._sell_locks[ticker].acquire(blocking=False):
            trade_log.append({"time": datetime.now().isoformat(), "date": today, "type": "SELL_BLOCKED",
                "ticker": ticker, "name": name,
                "message": f"🔒 매도 중복 방지: {name or ticker} 이미 매도 처리 중 ({reason[:20]})"})
            save_state()
            return
        
        try:
            # 기존 쿨다운도 유지 (안전망)
            now_ts = time.time()
            last_sell_ts = recently_sold.get(ticker, 0)
            if now_ts - last_sell_ts < SELL_COOLDOWN:
                remaining = int(SELL_COOLDOWN - (now_ts - last_sell_ts))
                trade_log.append({"time": datetime.now().isoformat(), "date": today, "type": "BLOCKED",
                    "ticker": ticker, "name": name,
                    "message": f"🛡 중복매도 차단: {name or ticker} 쿨다운 {remaining}초 남음 ({reason[:30]})"})
                save_state()
                return

            # ★ 매도 일일 한도 (매수와 별도로 관리, 기본값 999 - 사실상 무제한)
            today_sells = [t for t in trade_log if t.get('date', '').startswith(today) and t.get('type') == 'SELL' and t.get('success')]
            max_daily_sells = cfg.get('max_daily_sells', 999)
            if len(today_sells) >= max_daily_sells:
                trade_log.append({"time": datetime.now().isoformat(), "date": today, "type": "BLOCKED",
                    "message": f"일일 매도 한도 초과 ({max_daily_sells}회) - {name} 매도 차단"})
                save_state()
                return

            # KIS API 매도 주문 실행
            # ★★★ v4.1: 익절은 최우선호가 지정가 / 손절·강제는 시장가 ★★★
            _is_stoploss = any(kw in reason for kw in ['손절', '강제', 'FORCE', 'sl:', 'manual'])
            
            if _is_stoploss or mode == "mock":
                # 손절/강제/수동 → 시장가 (속도 우선)
                _sell_ord_dvsn = "00" if mode == "mock" else "01"
                _sell_ord_unpr = str(int(price)) if mode == "mock" else "0"
                _sell_type_label = "시장가(손절)"
            else:
                # 익절 → 매수1호가(최우선) 지정가 시도
                _best_bid = 0
                try:
                    _sell_ob = fetch_orderbook(cfg.get('app_key',''), cfg.get('app_secret',''), mode, token, ticker)
                    if _sell_ob and _sell_ob.get('bid1_price'):
                        _best_bid = int(_sell_ob['bid1_price'])
                except:
                    pass
                
                if _best_bid > 0:
                    _sell_ord_dvsn = "00"  # 지정가
                    _sell_ord_unpr = str(_best_bid)
                    _sell_type_label = f"최우선호가(익절) ₩{_best_bid:,}"
                else:
                    _sell_ord_dvsn = "01"  # 호가 조회 실패 → 시장가 폴백
                    _sell_ord_unpr = "0"
                    _sell_type_label = "시장가(호가조회실패)"
            
            print(f"[SELL] {_sell_type_label}: {name}({ticker}) {qty}주")
            
            result = kis_request("POST", "/uapi/domestic-stock/v1/trading/order-cash",
                cfg['app_key'], cfg['app_secret'], mode, token, tr_id,
                body={"CANO": cfg['account'], "ACNT_PRDT_CD": cfg.get('account_cd', '01'),
                      "PDNO": ticker, "ORD_DVSN": _sell_ord_dvsn, "ORD_QTY": str(qty),
                      "ORD_UNPR": _sell_ord_unpr})
            
            success = result.get('rt_cd') == '0'
            msg1 = result.get('msg1', '')
            _sell_order_no = result.get('output', {}).get('ODNO', '')
            
            # ★ 익절 지정가: 15초 미체결 → 시장가 전환
            if success and _sell_ord_dvsn == "00" and not _is_stoploss and _sell_order_no:
                import time as _time_mod2
                _time_mod2.sleep(3)  # 3초 대기
                
                # 체결 확인 (잔고에서 수량 감소 확인)
                _sell_filled = False
                try:
                    _chk_bal2 = get_balance(cfg['app_key'], cfg['app_secret'], mode, token,
                                           cfg['account'], cfg.get('account_cd','01'), max_age=0)
                    _still_held = False
                    for _sp in _chk_bal2.get('output1', []):
                        if _sp.get('pdno') == ticker and int(_sp.get('hldg_qty','0') or 0) >= qty:
                            _still_held = True
                            break
                    _sell_filled = not _still_held
                except:
                    _sell_filled = True  # 확인 실패 시 체결 간주
                
                if not _sell_filled:
                    print(f"[SELL] ⏳ {name}({ticker}) 익절 지정가 미체결 → 시장가 전환")
                    try:
                        kis_request("POST", "/uapi/domestic-stock/v1/trading/order-rvsecncl",
                            cfg['app_key'], cfg['app_secret'], mode, token, "TTTC0803U",
                            body={"CANO": cfg['account'], "ACNT_PRDT_CD": cfg.get('account_cd','01'),
                                  "KRX_FWDG_ORD_ORGNO": "", "ORGN_ODNO": _sell_order_no,
                                  "ORD_DVSN": "01", "RVSE_CNCL_DVSN_CD": "02",
                                  "ORD_QTY": str(qty), "ORD_UNPR": "0", "QTY_ALL_ORD_YN": "Y"})
                        _time_mod2.sleep(0.5)
                        result = kis_request("POST", "/uapi/domestic-stock/v1/trading/order-cash",
                            cfg['app_key'], cfg['app_secret'], mode, token, tr_id,
                            body={"CANO": cfg['account'], "ACNT_PRDT_CD": cfg.get('account_cd','01'),
                                  "PDNO": ticker, "ORD_DVSN": "01", "ORD_QTY": str(qty),
                                  "ORD_UNPR": "0"})
                        success = result.get('rt_cd') == '0'
                        msg1 = f"익절→시장가전환: {result.get('msg1','')}"
                        print(f"[SELL] ✅ 시장가 전환 {'성공' if success else '실패'}")
                    except Exception as _sc_e:
                        print(f"[SELL] 취소/재주문 실패: {_sc_e}")
            # ★ 거래비용 계산 (수수료+거래세, 기본 0.3%)
            _cost_rate = float(cfg.get('trade_cost_rate', 0.3)) / 100
            _sell_amount = price * qty
            _buy_amount = avg_price * qty if avg_price > 0 else 0
            _trade_cost = round(_sell_amount * _cost_rate)
            pnl_amt = round((price - avg_price) * qty, 0) if avg_price > 0 else 0
            pnl_net = pnl_amt - _trade_cost  # 비용 차감 순수익
            pnl_pct_val = round((price - avg_price) / avg_price * 100, 2) if avg_price > 0 else 0
            pnl_pct_net = round(pnl_net / _buy_amount * 100, 2) if _buy_amount > 0 else 0
            
            # ★★★ v4.1: AI 매도학습용 종목 데이터 수집 ★★★
            _stock_snapshot = {}
            try:
                _snap = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
                    cfg['app_key'], cfg['app_secret'], mode, token, "FHKST01010100",
                    params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker})
                _so = _snap.get('output', {})
                _stock_snapshot = {
                    'open': int(_so.get('stck_oprc', 0) or 0),      # 시가
                    'high': int(_so.get('stck_hgpr', 0) or 0),      # 고가
                    'low': int(_so.get('stck_lwpr', 0) or 0),       # 저가
                    'cur': int(_so.get('stck_prpr', 0) or 0),       # 현재가
                    'prev_close': int(_so.get('stck_sdpr', 0) or 0),# 전일종가
                    'day_chg': float(_so.get('prdy_ctrt', 0) or 0), # 등락률
                    'volume': int(_so.get('acml_vol', 0) or 0),     # 거래량
                    'tr_amt': int(_so.get('acml_tr_pbmn', 0) or 0), # 거래대금
                    'strength': float(_so.get('tday_rltv', 0) or 0),# 체결강도
                }
                # 매수시점 찾기
                _buy_entry = next((t for t in reversed(trade_log) 
                    if t.get('ticker') == ticker and t.get('type') in ('AI_BUY','BUY','CHAT_BUY') 
                    and t.get('success')), None)
                if _buy_entry:
                    _stock_snapshot['buy_time'] = _buy_entry.get('time', '')
                    _stock_snapshot['buy_price'] = _buy_entry.get('price', 0)
                    _stock_snapshot['buy_confidence'] = _buy_entry.get('confidence', 0)
                    # 보유시간 계산
                    try:
                        _bt = datetime.fromisoformat(_buy_entry['time'])
                        _hold_min = int((datetime.now() - _bt).total_seconds() / 60)
                        _stock_snapshot['hold_minutes'] = _hold_min
                    except: pass
                # 고점 대비 하락폭
                _peak = peak_prices.get(ticker, price)
                _stock_snapshot['peak_price'] = _peak
                _stock_snapshot['from_peak_pct'] = round((price - _peak) / max(_peak, 1) * 100, 2) if _peak > 0 else 0
                # 시가 대비 매도가
                if _stock_snapshot.get('open', 0) > 0:
                    _stock_snapshot['from_open_pct'] = round((price - _stock_snapshot['open']) / _stock_snapshot['open'] * 100, 2)
            except Exception as _snpe:
                print(f"[SELL_DATA] 종목 스냅샷 수집 실패: {_snpe}")
            
            # ★ 종목 market/sector 정보 (매도 실적 태그 표시용)
            _sell_market = ''
            _sell_sector = ''
            try:
                # 1순위: 잔고 캐시에서 market 정보
                for _bp in (_bal_cache.get('data',{}) or {}).get('output1',[]):
                    if _bp.get('pdno') == ticker:
                        _mkt_code = _bp.get('fncg_prdt_cd', '') or _bp.get('pdt_name', '')
                        if any(kw in str(_bp) for kw in ['코스닥','KOSDAQ']):
                            _sell_market = '코스닥'
                        else:
                            _sell_market = '코스피'
                        break
                # 2순위: 매수 로그에서 sector 정보
                _buy_log = next((t for t in reversed(trade_log) 
                    if t.get('ticker') == ticker and t.get('type') in ('AI_BUY','BUY','CHAT_BUY')
                    and t.get('success')), None)
                if _buy_log:
                    _sell_sector = _buy_log.get('sector', '')
                    if not _sell_market and _buy_log.get('market'):
                        _sell_market = _buy_log.get('market', '')
            except: pass
            
            trade_log.append({
                "time": datetime.now().isoformat(), "date": today, "mode": "live",
                "type": "SELL", "ticker": ticker, "name": name,
                "trade_mode": trade_mode,  # ★ v6.0: 'auto' 또는 'swing'
                "sell_mode": "ai" if ("[AI매도]" in reason or "AI추세" in reason or "AI손절" in reason) else "manual",
                "qty": qty, "price": price, "avg_price": avg_price,
                "pnl": pnl_net, "pnl_pct": pnl_pct_net,
                "pnl_gross": pnl_amt, "pnl_pct_gross": pnl_pct_val,
                "trade_cost": _trade_cost, "cost_rate": float(cfg.get('trade_cost_rate', 0.3)),
                "market": _sell_market, "sector": _sell_sector,
                "reason": reason,
                "success": success, "message": msg1,
                "order_no": result.get('output', {}).get('ODNO', ''),
                "stock_data": _stock_snapshot,
                "sell_plan": _sell_plans.get(ticker, {}),
                # ★★★ v6.0: 매도 학습용 추가 데이터 ★★★
                "sell_stage": _sell_stage.get(ticker, 0),       # 몇 차 익절 단계에서 매도했는지
                "dip_detected": _dip_flag.get(ticker, False),   # 눌림이 있었는지
                "dip_duration": round((time.time() - _dip_time.get(ticker, time.time())) / 60, 1) if ticker in _dip_time else 0,  # 눌림 지속시간(분)
                "sl_moved": (_sell_stage.get(ticker, 0) >= 1),  # 1차 후 본전스톱 적용됐는지
                "peak_from_buy": round((peak_prices.get(ticker, price) - avg_price) / max(avg_price, 1) * 100, 2) if avg_price > 0 else 0  # 매수가 대비 최고점%
            })
            
            # ★ v3.0 근본 FIX: 매도 실패 시 거래정지/매매불가 감지 → perm_blocked 등록
            if not success:
                recently_sold[ticker] = time.time()  # 실패해도 쿨다운 등록 (무한 재시도 방지)
                _block_keywords = ['거래정지', '매매불가', '취소주문만', '정정불가', '처리가 안되었습니다']
                if any(kw in msg1 for kw in _block_keywords):
                    # ★ v8.0: KIS가 명시적으로 '거래정지'라고 하면 HALTED:, 아니면 SELL_FAIL:
                    if '거래정지' in msg1 or '매매정지' in msg1:
                        perm_blocked[ticker] = f"HALTED: {msg1[:50]}"
                    else:
                        perm_blocked[ticker] = f"SELL_FAIL: {msg1[:50]}"
                    print(f"[SELL] 🚫 {name}({ticker}) 매매불가 → perm_blocked 등록: {msg1[:50]}")
                    # auto_tickers에서도 제거 (더 이상 체크 불필요)
                    if ticker in auto_tickers:
                        auto_tickers.remove(ticker)
                        print(f"[SELL] {ticker} auto_tickers 제거 (매매불가)")
                return
            
            # ★ 매도 성공 시 쿨다운 등록
            recently_sold[ticker] = time.time()
            # ★ v4.0: 매도 주문 체결 추적
            _sell_odno = result.get('output', {}).get('ODNO', '')
            if _sell_odno:
                track_pending_order(_sell_odno, ticker, name, qty, 'SELL', price)
            # ★ 텔레그램 매도 알림
            _st = 'force_close' if 'FORCE_CLOSE' in reason.upper() else ('ai_sell' if 'AI' in reason.upper() else ('trailing' if 'trailing' in reason.lower() else ('sl' if '손절' in reason else ('tp2' if 'tp2' in reason or '2차' in reason else ('tp1' if 'tp1' in reason or '1차' in reason else 'sell')))))
            _tm = 'auto'  # v8: 단타 통합
            tg_sell(name, ticker, qty, price, pnl_pct=pnl_pct_net, pnl_amt=pnl_net, sell_type=_st, trade_mode=_tm)
            # ★ v3.0: WebSocket 매도 이벤트 push
            sync_broadcast('sell', {
                'ticker': ticker, 'name': name, 'qty': qty, 'price': price,
                'pnl': pnl_amt, 'pnl_pct': pnl_pct_val, 'sell_type': _st,
                'reason': reason, 'time': datetime.now().strftime('%H:%M:%S'),
                'success': True
            })
            # ★ 현금 트래커에 매도 금액 추가
            _sell_proceeds = price * qty
            _cash_tracker['amount'] += _sell_proceeds
            _cash_tracker['ts'] = time.time() + 1
            print(f"[SELL] 💰 매도 {qty}주×{price:,} = +{_sell_proceeds:,.0f} | 트래커잔여 {_cash_tracker['amount']:,.0f}")
            # ★ 전량 매도 여부 확인 후에만 auto_tickers 제거
            _remaining_qty = 0
            try:
                _pr = kis_request("GET", "/uapi/domestic-stock/v1/trading/inquire-balance",
                    cfg['app_key'], cfg['app_secret'], mode, token,
                    "TTTC8434R",
                    params={"CANO": cfg['account'], "ACNT_PRDT_CD": cfg.get('account_cd','01'),
                            "AFHR_FLPR_YN":"N","OFL_YN":"","INQR_DVSN":"02","UNPR_DVSN":"01",
                            "FUND_STTL_ICLD_YN":"N","FNCG_AMT_AUTO_RDPT_YN":"N","PRCS_DVSN":"01",
                            "CTX_AREA_FK100":"","CTX_AREA_NK100":""})
                for _p in _pr.get('output1', []):
                    if _p.get('pdno') == ticker:
                        _remaining_qty = int(_p.get('hldg_qty','0') or '0')
                        break
                if _pr.get('rt_cd') == '0':
                    _bal_cache['data'] = _pr
                    _bal_cache['ts'] = time.time()
            except Exception:
                _remaining_qty = qty
            
            if _remaining_qty <= 0:
                if ticker in auto_tickers:
                    auto_tickers.remove(ticker)
                    print(f"[AUTO] {ticker} 전량매도 완료 → auto_tickers 제거")
                _tp1_triggered.discard(ticker)
                _sell_stage.pop(ticker, None)
                _dip_flag.pop(ticker, None)
                auto_avg_count.pop(ticker, None)  # ★ v8.0: 전량매도 → 물타기 횟수 삭제
                _ai_sell_states.pop(ticker, None)
                _sell_plans.pop(ticker, None)
                self._scan_trigger = True
                _auto_status_cache['ts'] = 0  # ★ v8.0: 캐시 즉시 무효화
                print(f"[AUTO] 📡 슬롯 열림 → 즉시 스캔 트리거 설정")
            else:
                print(f"[AUTO] {ticker} 부분매도 성공 ({qty}주), 잔여 {_remaining_qty}주 → auto_tickers 유지")
        except Exception as e:
            trade_log.append({
                "time": datetime.now().isoformat(), "date": today, "mode": "live",
                "type": "SELL_ERROR", "ticker": ticker, "name": name,
                "qty": qty, "reason": reason, "message": str(e)
            })
        finally:
            save_state()
            # ★ 항상 매도 락 해제
            self._sell_locks[ticker].release()

auto_trader = AutoTrader()

# ★★★ v8.0: 서버 시작 시 swing 보유종목 → auto_tickers 이관 ★★★
if swing_tickers:
    _startup_migrated = 0
    for _stk in list(swing_tickers):
        if _stk not in auto_tickers:
            auto_tickers.append(_stk)
            _startup_migrated += 1
    if _startup_migrated:
        print(f"[STARTUP/V8] ★ swing→auto 이관: {_startup_migrated}종목 → 단타 매도엔진 관리")
    swing_tickers.clear()
# ★★★ v8.0: swing 잔재 항상 비움 (swing_tickers 없어도 swing_buy_routes 남아있으면 슬롯 카운트 오류) ★★★
if swing_buy_routes:
    print(f"[STARTUP/V8] swing_buy_routes {len(swing_buy_routes)}건 비움: {list(swing_buy_routes.keys())}")
    swing_buy_routes.clear()
    save_state()
# v2.0 Handler 클래스 → FastAPI 라우트 1:1 변환
# 모든 기존 API 엔드포인트 100% 보존

# ── WebSocket 엔드포인트 ──────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket 실시간 연결 - 매수/매도/브리핑/잔고 push"""
    await websocket.accept()
    connected_clients.add(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    print(f"[WS] ✅ 클라이언트 연결 ({client_ip}) — 총 {len(connected_clients)}개")
    try:
        # 연결 즉시 현재 상태 전송
        now = datetime.now()
        hhmm = now.hour * 100 + now.minute
        await websocket.send_json({
            "event": "connected",
            "data": {
                "version": "3.0",
                "running": auto_trader.running,
                "paused": getattr(auto_trader, 'paused', False),
                "phase": auto_trader._get_market_phase(hhmm) if auto_trader.running else "",
                "auto_tickers": auto_tickers,
                "clients": len(connected_clients),
                "time": now.strftime('%H:%M:%S')
            }
        })
        # 클라이언트 메시지 대기 (keepalive)
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data) if data else {}
            # ping/pong heartbeat
            if msg.get('type') == 'ping':
                await websocket.send_json({"event": "pong", "data": {"time": datetime.now().strftime('%H:%M:%S')}})
            # 클라이언트에서 상태 요청
            elif msg.get('type') == 'status':
                hhmm = datetime.now().hour * 100 + datetime.now().minute
                await websocket.send_json({
                    "event": "status",
                    "data": {
                        "running": auto_trader.running,
                        "paused": getattr(auto_trader, 'paused', False),
                        "phase": auto_trader._get_market_phase(hhmm) if auto_trader.running else "",
                        "auto_tickers": auto_tickers,
                        "clients": len(connected_clients)
                    }
                })
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[WS] ⚠️ 연결 오류: {e}")
    finally:
        connected_clients.discard(websocket)
        print(f"[WS] 🔌 클라이언트 해제 — 총 {len(connected_clients)}개")

# ── 헬퍼: body 파싱 ────────────────────────────────────────────────
async def parse_body(request: Request) -> dict:
    """POST body 파싱 (빈 body 허용)"""
    try:
        return await request.json()
    except Exception:
        return {}

# ── KIS 증권 API ──────────────────────────────────────────────────

@app.post("/api/kis/token")
async def api_kis_token(request: Request):
    body = await parse_body(request)
    try:
        token = kis_get_token(body['app_key'], body['app_secret'], 'live')
        return {'token': token, 'success': True}
    except Exception as e:
        return JSONResponse(status_code=500, content={'error': str(e), 'success': False})

@app.post("/api/kis/balance")
@app.post("/api/balance")
async def api_kis_balance(request: Request):
    body = await parse_body(request)
    try:
        ak = body.get('app_key',''); ask = body.get('app_secret','')
        ac = body.get('account',''); acd = body.get('account_cd','01')
        if not ak: return {'error':'app_key 필요'}
        tok = kis_get_token(ak, ask, 'live')
        _force = body.get('force', False)
        _max_age = 0 if _force else 10  # force=true면 캐시 무시
        if _force:
            _bal_cache['ts'] = 0  # 캐시 무효화
            print(f"[BALANCE] ★ 강제 갱신 요청 (캐시 무효화)")
        bal = get_balance(ak, ask, 'live', tok, ac, acd, max_age=_max_age)
        if _force:
            # ★ 디버그: KIS가 실제로 뭘 반환했는지
            for _p in bal.get('output1', []):
                _q = int(_p.get('hldg_qty', '0') or 0)
                if _q > 0:
                    print(f"[BALANCE] {_p.get('prdt_name','')}({_p.get('pdno','')}) 수량={_q} 평단={_p.get('pchs_avg_pric','')}")
        # ★ v3.0: 증시 데이터 (캐시 우선, 없으면 조회)
        try:
            _mkt_cache = getattr(auto_trader, '_market_cache', {})
            if _mkt_cache:
                bal['_market'] = _mkt_cache
            else:
                _nv = fetch_naver_market_data()
                bal['_market'] = {
                    'kospi': _nv.get('kospi',''),
                    'kospi_chg': _nv.get('kospi_change',''),
                    'kosdaq': _nv.get('kosdaq',''),
                    'kosdaq_chg': _nv.get('kosdaq_change','')
                }
        except: bal['_market'] = {}
        # ★ 히스토리 없으면 파일 → 분봉 API 순서로 복원
        if not getattr(auto_trader, '_market_history', []):
            _today_s = datetime.now().strftime('%Y-%m-%d')
            try:
                with open(f'market_history_{_today_s}.json','r') as _hf:
                    auto_trader._market_history = json.load(_hf)
                    print(f"[CHART] balance에서 파일 복원: {len(auto_trader._market_history)}개")
            except:
                try:
                    auto_trader._market_history = fetch_market_day_chart()
                except:
                    auto_trader._market_history = []
        bal['_market_history'] = getattr(auto_trader, '_market_history', [])
        try:
            _pd = {}
            for _p in bal.get('output1',[]):
                _tk = _p.get('pdno','')
                _qty = int(_p.get('hldg_qty','0') or 0)
                if _qty > 0 and _tk:
                    try:
                        _r = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
                            ak, ask, 'live', tok, "FHKST01010100",
                            params={"FID_COND_MRKT_DIV_CODE":"J","FID_INPUT_ISCD":_tk})
                        _o = _r.get('output',{})
                        _pd[_tk] = {'high':int(_o.get('stck_hgpr','0')or 0),'low':int(_o.get('stck_lwpr','0')or 0),'market':_o.get('rprs_mrkt_kor_name',''),'sector':_o.get('bstp_kor_isnm','')}
                        time.sleep(0.15)
                    except: pass
            bal['_priceDetail'] = _pd
        except: bal['_priceDetail'] = {}
        return bal
    except Exception as e:
        return {'error': str(e), 'rt_cd': '1', 'msg1': str(e)}

@app.post("/api/kis/order")
@app.post("/api/order")
async def api_kis_order(request: Request):
    body = await parse_body(request)
    try:
        ak = body.get('app_key',''); ask = body.get('app_secret','')
        ac = body.get('account',''); acd = body.get('account_cd','01')
        ticker = body.get('ticker','')
        order_type = body.get('order_type','buy')
        qty = str(body.get('qty','1'))
        price = str(body.get('price','0'))
        ord_dvsn = body.get('ord_dvsn','01')
        if not ak or not ticker:
            return {'error':'app_key,ticker 필요','rt_cd':'1'}
        tok = kis_get_token(ak, ask, 'live')
        tr_id = 'TTTC0802U' if order_type == 'buy' else 'TTTC0801U'
        result = kis_request('POST', '/uapi/domestic-stock/v1/trading/order-cash',
            ak, ask, 'live', tok, tr_id,
            body={'CANO': ac, 'ACNT_PRDT_CD': acd,
                  'PDNO': ticker, 'ORD_DVSN': ord_dvsn,
                  'ORD_QTY': qty, 'ORD_UNPR': price})
        
        # ★ v3.0: 수동 매도/매수도 trade_log에 기록 (매도 실적에 반영)
        _success = result.get('rt_cd') == '0'
        _today = datetime.now().strftime('%Y-%m-%d')
        _qty_int = int(qty)
        _price_int = int(float(price)) if float(price) > 0 else 0
        _name = body.get('reason', '') or get_stock_name_naver(ticker) or ticker
        
        if order_type == 'sell' and _success:
            # 평단가 조회
            _avg = 0
            try:
                for _p in (_bal_cache.get('data',{}) or {}).get('output1',[]):
                    if _p.get('pdno') == ticker:
                        _avg = float(_p.get('pchs_avg_pric','0') or 0)
                        _price_int = _price_int or int(float(_p.get('prpr','0') or 0))
                        break
            except: pass
            _pnl = round((_price_int - _avg) * _qty_int) if _avg > 0 else 0
            _pnl_pct = round((_price_int - _avg) / _avg * 100, 2) if _avg > 0 else 0
            # ★ 거래비용 계산
            _cost_rate = float(auto_trader.config.get('trade_cost_rate', 0.3) if auto_trader.running else 0.3) / 100
            _trade_cost = round(_price_int * _qty_int * _cost_rate)
            _pnl_net = _pnl - _trade_cost
            _pnl_pct_net = round(_pnl_net / (_avg * _qty_int) * 100, 2) if _avg > 0 else 0
            trade_log.append({
                "time": datetime.now().isoformat(), "date": _today, "mode": "live",
                "type": "SELL", "ticker": ticker, "name": _name,
                "qty": _qty_int, "price": _price_int, "avg_price": _avg,
                "pnl": _pnl_net, "pnl_pct": _pnl_pct_net,
                "pnl_gross": _pnl, "pnl_pct_gross": _pnl_pct,
                "trade_cost": _trade_cost, "cost_rate": 0.3,
                "reason": "수동매도", "success": True,
                "message": result.get('msg1','')
            })
            # auto_tickers에서 제거
            if ticker in auto_tickers:
                auto_tickers.remove(ticker)
                # ★ 3차 익절 상태 정리
                _sell_stage.pop(ticker, None)
                _dip_flag.pop(ticker, None)
                _tp1_triggered.discard(ticker)
                # ★ 슬롯 열림 → 즉시 스캔 트리거
                if auto_trader.running:
                    auto_trader._scan_trigger = True
                    print(f"[MANUAL] 📡 슬롯 열림 → 즉시 스캔 트리거")
            recently_sold[ticker] = time.time()
            save_state()
            # ★ 텔레그램 매도 알림
            try:
                tg_sell(_name, ticker, _qty_int, _price_int, pnl_pct=_pnl_pct_net, pnl_amt=_pnl_net, sell_type='manual', trade_mode='auto')
            except: pass
            print(f"[MANUAL] 수동매도: {_name}({ticker}) {_qty_int}주 pnl={_pnl:,}")
        elif order_type == 'buy' and _success:
            _bal_cache['ts'] = 0
            _ord_dvsn = body.get('ord_dvsn', '01')
            
            # ★★★ 체결 확인: 2초 후 잔고 체크 ★★★
            time.sleep(2)
            _bal_cache['ts'] = 0  # 캐시 무효화
            try:
                _check_bal = get_balance(ak, ask, 'live', tok, ac, acd, max_age=0)
                _new_qty = 0
                for _bp in _check_bal.get('output1', []):
                    if _bp.get('pdno') == ticker:
                        _new_qty = int(_bp.get('hldg_qty', '0') or 0)
                        _price_int = _price_int or int(float(_bp.get('prpr', '0') or 0))
                        break
            except:
                _new_qty = 0
            
            if _new_qty > 0:
                # ★ 체결 확인됨 → 로그 + 텔레그램
                trade_log.append({
                    "time": datetime.now().isoformat(), "date": _today, "mode": "live",
                    "type": "BUY", "ticker": ticker, "name": _name,
                    "qty": _qty_int, "price": _price_int,
                    "reason": "수동매수", "success": True,
                    "message": f"체결 확인 (보유 {_new_qty}주)"
                })
                if ticker not in auto_tickers:
                    auto_tickers.append(ticker)
                save_state()
                try:
                    tg_buy(_name, ticker, _qty_int, _price_int, reason='수동매수', trade_mode='auto')
                except: pass
                print(f"[MANUAL] ✅ 수동매수 체결: {_name}({ticker}) {_qty_int}주 @₩{_price_int:,} (보유 {_new_qty}주)")
                result['_filled'] = True
                result['_new_qty'] = _new_qty
            else:
                # ★ 미체결 (지정가 대기 중) → 로그/텔레그램 안 보냄
                if ticker not in auto_tickers and _ord_dvsn == '01':
                    auto_tickers.append(ticker)  # 시장가는 곧 체결될 것
                print(f"[MANUAL] ⏳ 수동매수 접수(미체결): {_name}({ticker}) {_qty_int}주 @₩{_price_int:,} (지정가 대기)")
                result['_filled'] = False
                result['_pending'] = True
        
        return result
    except Exception as e:
        return {'error': str(e), 'rt_cd': '1', 'msg1': str(e)}

@app.post("/api/kis/price")
@app.post("/api/price")
async def api_kis_price(request: Request):
    body = await parse_body(request)
    try:
        ak = body.get('app_key',''); ask = body.get('app_secret','')
        ticker = body.get('ticker','')
        if not ticker: return {'error':'ticker 필요'}
        tok = kis_get_token(ak, ask, 'live') if ak else ''
        r = kis_request('GET', '/uapi/domestic-stock/v1/quotations/inquire-price',
            ak, ask, 'live', tok, 'FHKST01010100',
            params={'FID_COND_MRKT_DIV_CODE':'J','FID_INPUT_ISCD':ticker})
        return r
    except Exception as e:
        return {'error': str(e)}

@app.post("/api/kis/index")
async def api_kis_index(request: Request):
    body = await parse_body(request)
    try:
        ak=body.get('app_key',''); ask=body.get('app_secret','')
        iscd=body.get('iscd','0001')
        tok=kis_get_token(ak,ask,'live') if ak else ''
        if not tok: return {'success':False,'error':'토큰없음'}
        data=kis_request('GET','/uapi/domestic-stock/v1/quotations/inquire-index-price',
            ak,ask,'live',tok,'FHPUP02100000',
            params={'FID_COND_MRKT_DIV_CODE':'U','FID_INPUT_ISCD':iscd})
        return {'success':True,'output':data.get('output',{})}
    except Exception as e:
        return {'success':False,'error':str(e)}

@app.post("/api/kis/volume-rank")
async def api_kis_volume_rank(request: Request):
    body = await parse_body(request)
    try:
        ak=body.get('app_key',''); ask=body.get('app_secret','')
        tok=kis_get_token(ak,ask,'live') if ak else ''
        if not tok: return {'success':False,'error':'토큰없음'}
        data=kis_request('GET','/uapi/domestic-stock/v1/quotations/volume-rank',
            ak,ask,'live',tok,'FHPST01710000',
            params={'FID_COND_MRKT_DIV_CODE':'J','FID_COND_SCR_DIV_CODE':'20171',
                    'FID_INPUT_ISCD':'0000','FID_DIV_CLS_CODE':'0',
                    'FID_BLNG_CLS_CODE':'0','FID_TRGT_CLS_CODE':'111111111',
                    'FID_TRGT_EXLS_CLS_CODE':'000000','FID_INPUT_PRICE_1':'',
                    'FID_INPUT_PRICE_2':'','FID_VOL_CNT':'','FID_INPUT_DATE_1':''})
        return {'success':True,'output':data.get('output',[])}
    except Exception as e:
        return {'success':False,'error':str(e)}

# ── v3.0 TIER 3: 호가창 + 체결강도 ───────────────────────────────

@app.post("/api/kis/orderbook")
async def api_kis_orderbook(request: Request):
    """KIS 호가(매수/매도 10단계) + 체결강도 조회"""
    body = await parse_body(request)
    try:
        ak = body.get('app_key',''); ask = body.get('app_secret','')
        ticker = body.get('ticker','')
        if not ak or not ticker:
            return {'success': False, 'error': 'app_key, ticker 필요'}
        tok = kis_get_token(ak, ask, 'live')
        
        # 호가 조회
        hoga = kis_request('GET', '/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn',
            ak, ask, 'live', tok, 'FHKST01010200',
            params={'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': ticker})
        
        output1 = hoga.get('output1', {})
        output2 = hoga.get('output2', {})
        
        # 매도 호가 (1~10단계)
        asks = []
        for i in range(1, 11):
            price = int(output1.get(f'askp{i}', '0') or 0)
            qty = int(output1.get(f'askp_rsqn{i}', '0') or 0)
            if price > 0:
                asks.append({'price': price, 'qty': qty})
        
        # 매수 호가 (1~10단계)
        bids = []
        for i in range(1, 11):
            price = int(output1.get(f'bidp{i}', '0') or 0)
            qty = int(output1.get(f'bidp_rsqn{i}', '0') or 0)
            if price > 0:
                bids.append({'price': price, 'qty': qty})
        
        # 총 매수/매도 잔량
        total_ask_qty = int(output1.get('total_askp_rsqn', '0') or 0)
        total_bid_qty = int(output1.get('total_bidp_rsqn', '0') or 0)
        
        # 체결강도 = 매수체결량 / 매도체결량 * 100
        strength = 0
        if total_ask_qty > 0:
            strength = round(total_bid_qty / total_ask_qty * 100, 1)
        
        # 매수/매도 비율
        total = total_ask_qty + total_bid_qty
        bid_ratio = round(total_bid_qty / max(total, 1) * 100, 1)
        
        return {
            'success': True,
            'ticker': ticker,
            'asks': asks,   # 매도호가 (낮→높)
            'bids': bids,   # 매수호가 (높→낮)
            'total_ask_qty': total_ask_qty,
            'total_bid_qty': total_bid_qty,
            'strength': strength,        # 체결강도 (100 이상=매수우위)
            'bid_ratio': bid_ratio,       # 매수비율 %
            'spread': asks[0]['price'] - bids[0]['price'] if asks and bids else 0,
            'time': datetime.now().strftime('%H:%M:%S')
        }
    except Exception as e:
        return {'success': False, 'error': str(e)}

# ── 자동매매 제어 ─────────────────────────────────────────────────

@app.post("/api/auto/start")
async def api_auto_start(request: Request):
    global auto_rules, ai_config
    body = await parse_body(request)
    ai_config = {
        'provider': body.get('ai_provider', 'openai'),
        'anthropic_key': body.get('anthropic_key', ''),
        'openai_key': body.get('openai_key', ''),
        'dart_key': body.get('dart_key', '')
    }
    config = {
        'app_key': body.get('app_key'),
        'app_secret': body.get('app_secret'),
        'mode': 'live',
        'account': body.get('account'),
        'account_cd': body.get('account_cd', '01'),
        'interval': body.get('interval', 10),
        'max_daily_trades': body.get('max_daily_trades', 20),
        'max_daily_loss': body.get('max_daily_loss', 300000),
        'anthropic_key': body.get('anthropic_key', ''),
        'openai_key': body.get('openai_key', ''),
        'ai_provider': body.get('ai_provider', 'openai'),
        'dart_key': body.get('dart_key', ''),
        'min_cash_ratio': body.get('min_cash_ratio', 30),
        'max_per_stock': body.get('max_per_stock', 20),
        'max_daily_invest': body.get('max_daily_invest', 50),
        'max_positions': body.get('max_positions', 6),
        'daily_loss_limit': body.get('daily_loss_limit', 3),
        'max_buy_amount': body.get('max_buy_amount', 500000),
        'auto_level': body.get('auto_level', 'preset'),
        'exclude_etf': body.get('exclude_etf', True),
        'daily_target': body.get('daily_target', 10),
        'daily_target_amt': float(body.get('daily_target_amt', 0) or 0),
        'daily_target_liquidate': bool(body.get('daily_target_liquidate', False)),
        'daily_loss_amt': float(body.get('daily_loss_amt', 0) or 0),
        'daily_loss_liquidate': bool(body.get('daily_loss_liquidate', False)),
        'target_liq_buf': float(body.get('target_liq_buf', 10) or 10),
        'loss_liq_buf': float(body.get('loss_liq_buf', 10) or 10),
        'force_close': bool(body.get('force_close', False)),
        'force_close_time': int(body.get('force_close_time', 1515)),
        'target_stop': body.get('target_stop', True),
        'session_baseline': float(body.get('session_baseline', 0) or 0),
        'tp1': float(body.get('tp1') or 2.5),
        'tp2': float(body.get('tp2') or 4.5),
        'tp3': float(body.get('tp3') or 7),
        'sl': float(body.get('sl') or body.get('stop_loss') or -4),
        'trailing_pct': float(body.get('trailing_pct', 3)),
        'sector_max_ratio': float(body.get('sector_max_ratio', 40)),
        'trade_cost_rate': float(body.get('trade_cost_rate', 0.3)),
        'ai_sell_max_hold': int(body.get('ai_sell_max_hold', 120)),
        'ai_sell_interval': int(body.get('ai_sell_interval', 3)),
        # ★★★ v6.0: 누락된 설정값 전부 추가 ★★★
        'sector_unique': bool(body.get('sector_unique', True)),
        'max_hold_min': int(body.get('max_hold_min', 0)),  # ★ v8.0: 기본값 0 = 시간제한없음
        'auto_avg_down': bool(body.get('auto_avg_down', True)),
        'auto_avg_pct': float(body.get('auto_avg_pct', -2.5)),
        'avg_pct1': float(body.get('avg_pct1', body.get('auto_avg_pct', -2.5))),
        'avg_pct2': float(body.get('avg_pct2', body.get('auto_avg_pct', -2.5))),
        'avg_pct3': float(body.get('avg_pct3', body.get('auto_avg_pct', -2.5))),
        'avg_r1': int(body.get('avg_r1', 50)),
        'avg_r2': int(body.get('avg_r2', 30)),
        'avg_r3': int(body.get('avg_r3', 20)),
        'surge_threshold': int(body.get('surge_threshold', 70)),
        'cooldown_min': int(body.get('cooldown_min', 10)),
        'tp_mode': body.get('tp_mode', 'full'),
        'tp1_ratio': int(body.get('tp1_ratio', 40)),
        'tp2_ratio': int(body.get('tp2_ratio', 30)),
        'tp3_ratio': int(body.get('tp3_ratio', 30)),
        'scan_intervals': (lambda raw: {
            k: (v.get('min', {'s2':2,'s3':2,'s3b':3}.get(k,3))
                if isinstance(v, dict) else int(v or {'s2':2,'s3':2,'s3b':3}.get(k,3)))
            for k, v in (raw if isinstance(raw, dict) else {}).items()
        } if isinstance(raw, dict) else {'s2':2,'s3':2,'s3b':3})(
            body.get('scan_intervals', {})),
    }
    for _k, _v in pending_cfg.items():
        config[_k] = _v
    pending_cfg.clear()
    try:
        load_state('live')
        _tg = body.get('telegram', {})
        if _tg.get('token') and _tg.get('chat_id'):
            telegram_config['token'] = _tg['token']
            telegram_config['chat_id'] = _tg['chat_id']
            telegram_config['enabled'] = bool(_tg.get('enabled', True))
            telegram_config['on_buy'] = bool(_tg.get('on_buy', True))
            telegram_config['on_sell'] = bool(_tg.get('on_sell', True))
            telegram_config['on_briefing'] = bool(_tg.get('on_briefing', True))
            telegram_config['on_error'] = bool(_tg.get('on_error', True))
            print(f"[TG] 자동매매 시작 → 텔레그램 활성화 (chat_id: {telegram_config['chat_id']})")
            start_tg_poller()
            tg_send(f"🤖 <b>자동매매 시작</b>\n모드: 🔴 실전\n시간: {datetime.now().strftime('%H:%M')}")
        
        # ★ v3.0 근본 FIX: ai_market_scan 룰 없으면 자동 생성 (start 전!)
        _has_scan_rule = any(r.get('type') in ('ai_market_scan', 'ai_autobuy') and r.get('active', True) for r in auto_rules)
        if not _has_scan_rule:
            _default_rule = {
                'id': f"ai_scan_{datetime.now().strftime('%H%M%S')}",
                'type': 'ai_market_scan',
                'active': True,
                'market': 'ALL',
                'max_picks': int(config.get('max_picks', 5)),
                'max_buy_amount': float(config.get('max_buy_amount', 500000)),
                'min_confidence': 80,
                'strategy': 'auto',
                'created': datetime.now().isoformat()
            }
            auto_rules.append(_default_rule)
            save_state()
            print(f"[START] ★ ai_market_scan 룰 자동 생성 (max_picks={_default_rule['max_picks']}, max_buy={_default_rule['max_buy_amount']:,.0f})")
        else:
            print(f"[START] ai_market_scan 룰 {sum(1 for r in auto_rules if r.get('type')=='ai_market_scan')}개 존재")
        
        auto_trader.start(config)
        monitoring['active'] = True
        # ★ v3.0: 시작 이벤트 broadcast
        sync_broadcast('status', {'running': True, 'phase': 'started', 'time': datetime.now().strftime('%H:%M:%S')})
        return {'success': True, 'message': '자동매매 시작'}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={'success': False, 'message': f'시작 오류: {str(e)}'})

@app.post("/api/auto/stop")
async def api_auto_stop(request: Request):
    auto_trader.stop()
    monitoring['active'] = False
    sync_broadcast('status', {'running': False, 'phase': 'stopped', 'time': datetime.now().strftime('%H:%M:%S')})
    return {'success': True, 'message': '자동매매 중지'}

@app.post("/api/auto/pause")
async def api_auto_pause(request: Request):
    if auto_trader.running:
        auto_trader.pause()
        sync_broadcast('status', {'running': True, 'paused': True, 'time': datetime.now().strftime('%H:%M:%S')})
        return {'success': True, 'message': '자동매매 일시정지', 'paused': True}
    return JSONResponse(status_code=400, content={'success': False, 'message': '자동매매가 실행 중이 아닙니다'})

@app.post("/api/auto/resume")
async def api_auto_resume(request: Request):
    if auto_trader.running:
        auto_trader.resume()
        sync_broadcast('status', {'running': True, 'paused': False, 'time': datetime.now().strftime('%H:%M:%S')})
        return {'success': True, 'message': '자동매매 재개', 'paused': False}
    return JSONResponse(status_code=400, content={'success': False, 'message': '자동매매가 실행 중이 아닙니다'})

# ★★★ v6.0: 거래내역 검색 API (디버그용) ★★★
@app.post("/api/trade/search")
async def api_trade_search(request: Request):
    body = await parse_body(request)
    keyword = body.get('keyword', '').strip()
    ticker = body.get('ticker', '').strip()
    results = []
    for l in trade_log:
        if ticker and l.get('ticker') == ticker:
            results.append(l)
        elif keyword and (keyword in l.get('name','') or keyword in l.get('ticker','') or keyword in l.get('message','') or keyword in l.get('reason','')):
            results.append(l)
    return {'count': len(results), 'results': results[-50:]}  # 최근 50건

# ★★★ v6.0: 급등주 흔들기 패턴 분석 API ★★★
@app.post("/api/surge-patterns")
async def api_surge_patterns(request: Request):
    body = await parse_body(request)
    date_from = body.get('from', '')
    date_to = body.get('to', '')
    data = surge_pattern_history
    if date_from:
        data = [p for p in data if p.get('date','') >= date_from]
    if date_to:
        data = [p for p in data if p.get('date','') <= date_to]
    
    # 통계 계산 (★ dip=0 제외 — 장외시간 불완전 데이터)
    stats = {}
    valid_data = [p for p in data if p.get('dip_from_peak', 0) != 0]
    if valid_data:
        dips = [p['dip_from_peak'] for p in valid_data if 'dip_from_peak' in p]
        bounces = [p['bounce_from_dip'] for p in valid_data if 'bounce_from_dip' in p]
        peaks = [p['peak_chg'] for p in valid_data if 'peak_chg' in p]
        shake_dur = [p['shake_duration'] for p in valid_data if 'shake_duration' in p]
        stats = {
            'count': len(data),
            'avg_dip': round(sum(dips)/len(dips), 2) if dips else 0,
            'median_dip': round(sorted(dips)[len(dips)//2], 2) if dips else 0,
            'max_dip': round(min(dips), 2) if dips else 0,
            'min_dip': round(max(dips), 2) if dips else 0,
            'avg_bounce': round(sum(bounces)/len(bounces), 2) if bounces else 0,
            'avg_peak': round(sum(peaks)/len(peaks), 2) if peaks else 0,
            'avg_shake_min': round(sum(shake_dur)/len(shake_dur), 1) if shake_dur else 0,
            'optimal_avg_entry': round(sum(dips)/len(dips) * 0.8, 2) if dips else 0,
            'total_collected': len(data),  # 전체 수집 (불완전 포함)
            # 구간별 분포
            'dip_dist': {
                '0~2%': len([d for d in dips if d >= -2]),
                '2~4%': len([d for d in dips if -4 <= d < -2]),
                '4~6%': len([d for d in dips if -6 <= d < -4]),
                '6~8%': len([d for d in dips if -8 <= d < -6]),
                '8%+': len([d for d in dips if d < -8]),
            }
        }
    return {'success': True, 'stats': stats, 'patterns': data[-100:]}

@app.post("/api/surge-patterns/collect-now")
async def api_surge_collect_now(request: Request):
    """수동 트리거: 분봉 패턴 즉시 수집"""
    if not auto_trader or not auto_trader.config.get('app_key'):
        return {'success': False, 'error': 'KIS 미연결'}
    cfg = auto_trader.config
    try:
        _token = kis_get_token(cfg['app_key'], cfg['app_secret'], 'live')
        collect_today_patterns(cfg['app_key'], cfg['app_secret'], 'live', _token)
        return {'success': True, 'count': len(surge_pattern_history)}
    except Exception as e:
        return {'success': False, 'error': str(e)}

# ★★★ v6.0: 보유종목 수동 평가 API ★★★
# ★★★ v8.0: 단타 수동 추가매수 (물타기 소진 후 수동 매수) ★★★
@app.post("/api/auto/manual-buy")
async def api_auto_manual_buy(request: Request):
    """단타 보유종목 수동 추가매수 — 시장가 즉시 체결"""
    body = await parse_body(request)
    ticker = body.get('ticker', '').strip()
    qty = int(body.get('qty', 0))
    name = body.get('name', ticker)
    
    if not ticker or len(ticker) != 6:
        return {'success': False, 'error': '종목코드 6자리 필요'}
    if qty <= 0:
        return {'success': False, 'error': '수량 1주 이상 필요'}
    
    ak = body.get('app_key', '')
    ase = body.get('app_secret', '')
    account = body.get('account', '')
    account_cd = body.get('account_cd', '01')
    
    if not ak or not ase:
        return {'success': False, 'error': 'KIS API키 필요'}
    
    try:
        token = kis_get_token(ak, ase, 'live')
        result = kis_request("POST", "/uapi/domestic-stock/v1/trading/order-cash",
            ak, ase, 'live', token, "TTTC0802U",
            body={
                "CANO": account[:8] if len(account) >= 8 else account,
                "ACNT_PRDT_CD": account_cd,
                "PDNO": ticker,
                "ORD_DVSN": "01",
                "ORD_QTY": str(qty),
                "ORD_UNPR": "0"
            })
        
        rt_cd = result.get('rt_cd', '')
        msg = result.get('msg1', '')
        
        if rt_cd == '0':
            import time as _t
            _t.sleep(1)
            _price = 0
            try:
                _pr = kis_request("GET", "/uapi/domestic-stock/v1/quotations/inquire-price",
                    ak, ase, 'live', token, "FHKST01010100",
                    params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker})
                _price = int(float(_pr.get('output', {}).get('stck_prpr', '0') or 0))
                if not name or name == ticker:
                    name = _pr.get('output', {}).get('hts_kor_isnm', '').strip() or name
            except: pass
            
            # auto_tickers에 유지 (단타 탭 소속)
            if ticker not in auto_tickers:
                auto_tickers.append(ticker)
            
            trade_log.append({
                "time": datetime.now().isoformat(),
                "date": datetime.now().strftime('%Y-%m-%d'),
                "type": "AVG_DOWN", "ticker": ticker, "name": name,
                "qty": qty, "price": _price or 0,
                "success": True, "trade_mode": "auto",
                "message": f"📥 수동추가매수 {name}({ticker}) {qty}주 @₩{(_price or 0):,}"
            })
            save_state()
            print(f"[AUTO_MANUAL_BUY] ✅ {name}({ticker}) {qty}주 @₩{(_price or 0):,}")
            sync_broadcast('buy', {'ticker': ticker, 'name': name, 'qty': qty, 'price': _price,
                                   'reason': '수동추가매수', 'time': datetime.now().strftime('%H:%M:%S')})
            return {'success': True, 'price': _price, 'qty': qty, 'message': msg}
        else:
            print(f"[AUTO_MANUAL_BUY] ❌ {ticker} 실패: {msg}")
            return {'success': False, 'error': msg}
    except Exception as e:
        return {'success': False, 'error': str(e)}

# ★★★ v6.0: 중장기 수동 추가매수 (물타기 소진 후 수동 매수) ★★★
_auto_status_cache = {'data': None, 'ts': 0, 'log_len': 0}

@app.post("/api/auto/status")
async def api_auto_status(request: Request):
    now = datetime.now()
    # ★★★ v8.0: 전체 응답 3초 캐시 (5000건×6회 순회 → 3초 1회로 감소) ★★★
    _now_ts = time.time()
    if (_auto_status_cache['data'] and 
        (_now_ts - _auto_status_cache['ts']) < 3 and 
        _auto_status_cache['log_len'] == len(trade_log)):
        return _auto_status_cache['data']
    
    hhmm = now.hour * 100 + now.minute
    phase = auto_trader._get_market_phase(hhmm) if auto_trader.running else ""
    interval = auto_trader._get_scan_interval(hhmm) if auto_trader.running else 0
    cfg = auto_trader.config if auto_trader.running else {}
    _today_s = now.strftime('%Y-%m-%d')
    
    # ★ 1회만 순회해서 필요한 값 전부 추출
    _today_pnl = 0
    _daily_stopped = False
    _daily_stop_msg = ''
    for t in trade_log:
        _td = t.get('date', '')
        if _td != _today_s:
            continue
        _tt = t.get('type', '')
        if _tt in ('SELL', 'FORCE_CLOSE', 'AI_SELL') and t.get('success') in (True, 1, 'true'):
            _today_pnl += float(t.get('pnl', 0) or 0)
        if _tt == 'DAILY_STOP':
            _daily_stopped = True
            _daily_stop_msg = t.get('message', '')
    
    result = {
        'running': auto_trader.running,
        'today_pnl': _today_pnl,
        'daily_stopped': _daily_stopped,
        'daily_stop_msg': _daily_stop_msg,
        'paused': getattr(auto_trader, 'paused', False),
        'rules_count': len(auto_rules),
        'log': _build_log_for_client(trade_log),
        'log_total': len(trade_log),
        'market_phase': phase,
        'scan_interval': interval,
        'current_time': now.strftime('%H:%M:%S'),
        'daily_briefing': daily_briefing if daily_briefing.get('date') else None,
        'auto_level': cfg.get('auto_level', 'preset'),
        'daily_target': cfg.get('daily_target', 10),
        'auto_tickers': auto_tickers,
        'api_usage': api_usage,
        'capital_rules': {
            'min_cash_ratio': cfg.get('min_cash_ratio', 30),
            'max_per_stock': cfg.get('max_per_stock', 20),
            'max_positions': cfg.get('max_positions', 6),
            'daily_loss_limit': cfg.get('daily_loss_limit', 3),
        } if cfg else None,
        'balance': None,
        'cash_tracker': _cash_tracker['amount'] if _cash_tracker['amount'] > 0 else None,
        'ws_clients': len(connected_clients),
        'server_version': '5.0',
        'ai_sell_mode': False,  # v8: always manual mode
        'hold_times': _get_hold_times_cached(),
        # ★★★ v8.0 성능: swing 데이터 제거 (매 5초 JSON 경량화) ★★★
        'auto_avg_count': auto_avg_count,
        'avg_detail': _build_avg_detail(),
        'perm_blocked': {k: v for k, v in perm_blocked.items()},
    }
    _auto_status_cache['data'] = result
    _auto_status_cache['ts'] = _now_ts
    _auto_status_cache['log_len'] = len(trade_log)
    return result

def _build_avg_detail():
    """오늘 물타기 상세 내역을 trade_log에서 직접 추출"""
    today = datetime.now().strftime('%Y-%m-%d')
    result = {}
    _found = 0
    _skipped = []
    # ★ 전체 AVG_DOWN 검색 (date 필터 없이 먼저 확인)
    _all_avg = [l for l in trade_log if l.get('type') == 'AVG_DOWN']
    _today_avg = [l for l in _all_avg if (l.get('date') or l.get('time','')[:10] or '') == today]
    
    if _all_avg and not _today_avg:
        # date 필드 형식 문제 디버그
        _sample = _all_avg[-1]
    
    for l in _all_avg:
        _dt = l.get('date') or l.get('time','')[:10] or ''
        _success = l.get('success')
        # ★ success 체크 완화: True, 1, 'true', 'True' 모두 허용
        if not (_success is True or _success == 1 or str(_success).lower() == 'true'):
            _skipped.append(f"{l.get('ticker')} success={_success}({type(_success).__name__})")
            continue
        if _dt != today:
            continue
        tk = l.get('ticker','')
        if not tk: continue
        if tk not in result:
            result[tk] = {'ticker': tk, 'name': l.get('name',''), 'entries': []}
        result[tk]['entries'].append({
            'time': (l.get('time',''))[11:16],
            'fullTime': (l.get('time',''))[5:16].replace('T',' '),
            'qty': int(l.get('qty', 0) or 0),
            'price': float(l.get('price', 0) or 0),
            'round': len(result[tk]['entries']) + 1,
            'message': (l.get('message','') or '').replace('🔄 ','')
        })
        _found += 1
    
    # ★ auto_avg_count에 기록 있지만 trade_log에 없는 종목
    for tk, cnt in auto_avg_count.items():
        if cnt > 0 and tk not in result:
            result[tk] = {'ticker': tk, 'name': ensure_name(tk, ''), 'entries': [],
                          'count_only': cnt}
    
    if _skipped:
        pass
    return result

@app.post("/api/auto/rules")
async def api_auto_rules(request: Request):
    global auto_rules
    body = await parse_body(request)
    action = body.get('action', 'list')
    if action == 'list':
        return {'rules': auto_rules}
    elif action == 'add':
        rule = body.get('rule', {})
        rule['id'] = str(int(time.time() * 1000))
        rule['active'] = True
        auto_rules.append(rule)
        save_state()
        return {'success': True, 'rules': auto_rules}
    elif action == 'remove':
        rid = body.get('id')
        auto_rules = [r for r in auto_rules if r.get('id') != rid]
        save_state()
        return {'success': True, 'rules': auto_rules}
    elif action == 'toggle':
        rid = body.get('id')
        for r in auto_rules:
            if r.get('id') == rid:
                r['active'] = not r.get('active', True)
        save_state()
        return {'success': True, 'rules': auto_rules}
    elif action == 'clear':
        auto_rules = []
        save_state()
        return {'success': True, 'rules': auto_rules}
    return {'rules': auto_rules}

@app.post("/api/auto/update_cfg")
async def api_auto_update_cfg(request: Request):
    body = await parse_body(request)
    allowed = ['max_positions','max_buy_amount','daily_loss_limit','daily_target',
               'cash_ratio','max_per_stock','tp1','tp2','tp3','sl','min_confidence',
               'scan_intervals','max_picks',
               'daily_target_amt','daily_target_liquidate','daily_loss_amt','daily_loss_liquidate',
               'target_liq_buf','loss_liq_buf','trailing_pct',
               'force_close','force_close_time','session_baseline',
               'sector_max_ratio',
               'ai_sell_max_hold','ai_sell_interval',
               'max_hold_min','auto_avg_down','auto_avg_pct','sector_unique',
               'avg_pct1','avg_pct2','avg_pct3',
               'avg_r1','avg_r2','avg_r3','surge_threshold','cooldown_min',
               'tp_mode','tp1_ratio','tp2_ratio','tp3_ratio']
    for k in ('tp1','tp2','tp3','sl','trailing_pct','avg_pct1','avg_pct2','avg_pct3','auto_avg_pct'):
        if k in body:
            try: body[k] = float(body[k])
            except: pass
    for k in ('max_positions','max_buy_amount','ai_sell_max_hold','ai_sell_interval',
              'max_hold_min','avg_r1','avg_r2','avg_r3','sector_max_ratio','surge_threshold','cooldown_min',
              'tp1_ratio','tp2_ratio','tp3_ratio'):
        if k in body:
            try: body[k] = int(body[k])
            except: pass
    if auto_trader and auto_trader.running:
        for k in allowed:
            if k in body:
                auto_trader.config[k] = body[k]
        print(f"[CFG] 실시간 설정 반영: {list(body.keys())}")
        if 'tp_mode' in body:
            _new_tp = body.get('tp_mode','')
            print(f"[CFG] ★ tp_mode = '{_new_tp}'")
            if _new_tp == 'full' and _sell_stage:
                print(f"[CFG] ★ 전량매도 전환 → _sell_stage 초기화: {dict(_sell_stage)}")
                _sell_stage.clear()  # ★ 분할매도 잔량 즉시 정리
    else:
        for k in allowed:
            if k in body:
                pending_cfg[k] = body[k]
        print(f"[CFG] pending 저장: {list(body.keys())}")
    return {'success': True, 'message': '설정 반영 완료'}

# ★★★ 외부 뷰어 동기화 API — 태블릿/폰에서 조회 전용 ★★★
@app.post("/api/viewer/sync")
async def api_viewer_sync(request: Request):
    """원격 뷰어용: 서버 상태 전체를 한 번에 내려줌 (KIS 키 불필요)"""
    now = datetime.now()
    cfg = auto_trader.config if auto_trader.running else {}
    
    # 잔고 캐시
    bal_data = None
    positions = []
    if _bal_cache.get('data'):
        bal = _bal_cache['data']
        out1 = bal.get('output1', [])
        out2 = (bal.get('output2', [{}]) or [{}])[0] or {}
        bal_data = {
            'output1': out1,
            'output2': [out2],
            'tot_evlu_amt': out2.get('tot_evlu_amt', '0'),
            'dnca_tot_amt': out2.get('dnca_tot_amt', '0'),
            'ord_psbl_cash': str(calc_ord_psbl_cash(bal)),
            'evlu_pfls_smtl_amt': out2.get('evlu_pfls_smtl_amt', '0'),
        }
        # 포지션 리스트 (WebSocket broadcast와 동일 구조)
        for _p in out1:
            _tk = _p.get('pdno', '')
            _qty = int(_p.get('hldg_qty', '0') or 0)
            if _qty <= 0:
                continue
            _cur = float(_p.get('prpr', '0') or 0)
            _avg = float(_p.get('pchs_avg_pric', '0') or 0)
            _pnl_pct = round((_cur - _avg) / _avg * 100, 2) if _avg > 0 else 0
            _pnl_amt = round((_cur - _avg) * _qty)
            positions.append({
                'ticker': _tk, 'name': _p.get('prdt_name', _tk),
                'qty': _qty, 'avg_price': round(_avg), 'cur_price': round(_cur),
                'pnl_pct': _pnl_pct, 'pnl_amt': _pnl_amt,
                'peak': peak_prices.get(_tk, round(_cur)),
                'sector': getattr(auto_trader, '_price_detail_cache', {}).get(_tk, {}).get('sector', '') if auto_trader.running else '',
                'auto': _tk in auto_tickers,
            })
    
    # 증시 데이터
    mkt = getattr(auto_trader, '_market_cache', {}) if auto_trader.running else {}
    mkt_history = getattr(auto_trader, '_market_history', []) if auto_trader.running else []
    
    return {
        'viewer': True,
        'running': auto_trader.running,
        'paused': getattr(auto_trader, 'paused', False),
        'balance': bal_data,
        'positions': positions,
        'hold_times': _get_hold_times_cached(),
        'total_eval': int(bal_data['tot_evlu_amt']) if bal_data else 0,
        'cash': int(bal_data['ord_psbl_cash']) if bal_data else 0,
        'total_pnl': int(bal_data['evlu_pfls_smtl_amt']) if bal_data else 0,
        'market': mkt,
        'market_history': mkt_history[-60:],
        'log': _build_log_for_client(trade_log, full=True),  # ★ v8.0: 초기 로드용 전체 로그
        'auto_tickers': list(auto_tickers),
        'swing_tickers': [],  # v8: always empty
        'swing_config': swing_config,
        'swing_avg_count': swing_avg_count,
        'swing_running': swing_running,
        'perm_blocked': {k: v for k, v in perm_blocked.items()},  # ★ 거래정지 종목 표시용
        'daily_briefing': daily_briefing if daily_briefing.get('date') else None,
        'market_phase': auto_trader._get_market_phase(now.hour*100+now.minute) if auto_trader.running else '',
        'config_summary': {
            'preset': cfg.get('preset', ''),
            'max_pos': cfg.get('max_positions', 6),
            'max_buy': cfg.get('max_buy_amount', 300000),
            'tp1': cfg.get('tp1', 3.5), 'tp2': cfg.get('tp2', 5.5),
            'tp3': cfg.get('tp3', 8), 'sl': cfg.get('sl', -3),
            'cash_ratio': cfg.get('cash_ratio', 60),
            'force_close': cfg.get('force_close', False),
            'force_close_time': cfg.get('force_close_time', 1515),
        } if cfg else {},
    }

@app.post("/api/auto/clear_log")
async def api_auto_clear_log(request: Request):
    trade_log.clear()
    save_state()
    return {'success': True, 'message': '거래 로그 초기화 완료'}

@app.post("/api/auto/clear_block")
async def api_auto_clear_block(request: Request):
    today_s = datetime.now().strftime('%Y-%m-%d')
    cleared = 0
    for t in trade_log:
        if t.get('date') == today_s and t.get('type') in ('DAILY_STOP', 'TARGET_HIT'):
            t['type'] = t['type'] + '_VOIDED'
            cleared += 1
    save_state()
    return {'success': True, 'message': f'✅ 오늘 매수 차단 {cleared}건 해제 완료'}

@app.post("/api/auto/clear_perm_blocked")
async def api_auto_clear_perm(request: Request):
    perm_blocked.clear()
    save_state()
    return {'success': True, 'message': '영구차단 목록 초기화 완료'}

@app.post("/api/auto/fix_names")
async def api_auto_fix_names(request: Request):
    body = await parse_body(request)
    try:
        app_key = body.get('app_key','')
        app_secret = body.get('app_secret','')
        token = kis_get_token(app_key, app_secret, 'live') if app_key else ''
        fixed = {}; invalid = {}
        for tkr in list(auto_tickers):
            try:
                info = kis_get_stock_info(app_key, app_secret, tkr, 'live', token)
                real = info.get('output',{}).get('hts_kor_isnm','') if info else ''
                if real: fixed[tkr] = real
                else: invalid[tkr] = '조회불가'
            except: invalid[tkr] = '오류'
        return {'success': True, 'fixed': fixed, 'invalid': invalid}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.post("/api/auto/remove_ticker")
async def api_auto_remove_ticker(request: Request):
    body = await parse_body(request)
    tkr = body.get('ticker','')
    if tkr and tkr in auto_tickers:
        auto_tickers.remove(tkr)
        save_state()
    return {'success': True}

# ── 시장 데이터 ───────────────────────────────────────────────────

@app.post("/api/global/market")
async def api_global_market(request: Request):
    try:
        url = 'https://polling.finance.naver.com/api/realtime/worldMarketIndex'
        req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=5) as res:
            data = json.loads(res.read())
        return {'success': True, 'data': data}
    except Exception as e:
        return {'success': False, 'error': str(e), 'data': []}

@app.post("/api/market/collect")
async def api_market_collect(request: Request):
    body = await parse_body(request)
    try:
        cfg_mc = {
            'app_key': body.get('app_key', ''),
            'app_secret': body.get('app_secret', ''),
            'account': body.get('account', ''),
            'dart_key': body.get('dart_key', '') or ai_config.get('dart_key', '')
        }
        nv_ctx, gl_ctx, sources = collect_market_context(cfg_mc)
        return {'success': True, 'market_ctx': nv_ctx, 'global_ctx': gl_ctx, 'sources': sources}
    except Exception as e:
        return {'success': False, 'error': str(e)}

# ── 네이버 금융 ───────────────────────────────────────────────────

@app.post("/api/naver/price")
async def api_naver_price(request: Request):
    body = await parse_body(request)
    ticker = body.get('ticker','')
    if not ticker or len(ticker) != 6:
        return JSONResponse(status_code=400, content={'error':'ticker 6자리 필요'})
    import re as _re2
    ua = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Referer':'https://finance.naver.com/'}
    result = {'ticker':ticker,'success':False}
    urls = [
        f'https://m.stock.naver.com/api/stock/{ticker}/basic',
        f'https://polling.finance.naver.com/api/realtime/domestic/stock/{ticker}',
        f'https://api.stock.naver.com/stock/{ticker}/basic',
    ]
    for url in urls:
        try:
            req = urllib.request.Request(url, headers=ua)
            with urllib.request.urlopen(req, timeout=8) as r:
                d = json.loads(r.read().decode('utf-8'))
            if not d: continue
            if 'narval' in d: d = d['narval']
            nm = d.get('stockName') or d.get('name','')
            pr = d.get('closePrice') or d.get('stockEndPrice') or d.get('now') or d.get('price') or ''
            if not pr: continue
            result['name'] = nm
            result['price'] = int(str(pr).replace(',',''))
            chg = d.get('compareToPreviousClosePrice') or d.get('change','')
            prev = d.get('previousClosePrice') or ''
            pct = d.get('fluctuationsRatio') or d.get('changeRate','')
            if chg: result['change'] = int(str(chg).replace(',','').replace('+','') or '0')
            if prev: result['prev_close'] = int(str(prev).replace(',',''))
            if pct: result['change_pct'] = str(pct)
            result['volume'] = d.get('accumulatedTradingVolume') or d.get('volume','')
            result['market_cap'] = d.get('marketValue') or d.get('marketCap','')
            result['per'] = d.get('per','')
            result['open'] = d.get('openPrice') or d.get('stockStartPrice','')
            result['high'] = d.get('highPrice') or d.get('stockHighPrice','')
            result['low'] = d.get('lowPrice') or d.get('stockLowPrice','')
            result['market'] = d.get('marketName','')
            result['foreign_ratio'] = d.get('foreignRatio','')
            result['success'] = True
            result['source'] = url
            break
        except Exception:
            continue
    # HTML fallback
    if not result['success']:
        try:
            req = urllib.request.Request(f'https://finance.naver.com/item/main.naver?code={ticker}', headers=ua)
            with urllib.request.urlopen(req, timeout=10) as r:
                html = r.read().decode('euc-kr', errors='replace')
            m = _re2.search(r'<title>\s*:?\s*([^:]+?)\s*:', html)
            if m: result['name'] = m.group(1).strip()
            m = _re2.search(r'no_today.*?blind.*?([[0-9],]+)', html, _re2.DOTALL)
            if m:
                result['price'] = int(m.group(1).replace(',',''))
                result['success'] = True
                result['source'] = 'html'
        except Exception:
            pass
    return result

@app.post("/api/naver/news")
async def api_naver_news(request: Request):
    body = await parse_body(request)
    ticker = body.get('ticker', '')
    news = fetch_naver_news(ticker=ticker)
    return {'news': news}

@app.post("/api/naver/search")
async def api_naver_search(request: Request):
    body = await parse_body(request)
    query = body.get('query', '').strip()
    if not query:
        return JSONResponse(status_code=400, content={'error': 'query required'})
    try:
        ua = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        enc = urllib.parse.quote(query)
        url = f'https://ac.stock.naver.com/ac?q={enc}&target=stock,index,etf,fund,bond,warr'
        req = urllib.request.Request(url, headers=ua)
        with urllib.request.urlopen(req, timeout=6) as r:
            raw = r.read().decode('utf-8')
        data = json.loads(raw)
        results = []
        raw_items = data.get('items') if isinstance(data, dict) else None
        if raw_items and isinstance(raw_items, list):
            for it in raw_items[:8]:
                code = ''; name = ''; market = ''
                if isinstance(it, dict):
                    code = str(it.get('code','') or it.get('ticker','') or it.get('stockCode','')).strip()
                    name = str(it.get('name','') or it.get('stockName','')).strip()
                    market = str(it.get('typeName','') or it.get('typeCode','') or it.get('market','')).strip()
                elif isinstance(it, (list, tuple)) and len(it) >= 2:
                    code = str(it[1]).strip()
                    name = str(it[0]).strip()
                    market = str(it[3]).strip() if len(it) > 3 else ''
                elif isinstance(it, list) and len(it) > 0 and isinstance(it[0], (list, dict)):
                    for sub in it[:8]:
                        if isinstance(sub, dict):
                            _c = str(sub.get('code','') or sub.get('ticker','')).strip()
                            _n = str(sub.get('name','') or sub.get('stockName','')).strip()
                            if _c and len(_c)==6 and _c.isdigit():
                                results.append({'ticker':_c,'name':_n,'market':sub.get('typeName','')})
                    continue
                if code and len(code) == 6 and code.isdigit():
                    results.append({'ticker': code, 'name': name, 'market': market})
        if not results and isinstance(data, list):
            for it in data[:8]:
                if isinstance(it, dict):
                    code = it.get('code','') or it.get('ticker','') or it.get('stockCode','')
                    name = it.get('name','') or it.get('stockName','')
                    if code and len(code) == 6 and code.isdigit():
                        results.append({'ticker': code, 'name': name, 'market': it.get('market','')})
        return {'results': results, 'query': query}
    except Exception as e:
        return {'results': [], 'error': str(e)}

# ── AI 채팅 ───────────────────────────────────────────────────────

@app.post("/api/telegram/config")
async def api_telegram_config(request: Request):
    body = await parse_body(request)
    telegram_config['token'] = body.get('token', '').strip()
    telegram_config['chat_id'] = body.get('chat_id', '').strip()
    telegram_config['enabled'] = bool(body.get('enabled', False))
    telegram_config['on_buy'] = bool(body.get('on_buy', True))
    telegram_config['on_sell'] = bool(body.get('on_sell', True))
    telegram_config['on_briefing'] = bool(body.get('on_briefing', True))
    telegram_config['on_error'] = bool(body.get('on_error', True))
    if body.get('ai_provider'): telegram_config['ai_provider'] = body['ai_provider']
    if body.get('openai_key'): telegram_config['openai_key'] = body['openai_key']
    if body.get('anthropic_key'): telegram_config['anthropic_key'] = body['anthropic_key']
    if body.get('app_key'): telegram_config['app_key'] = body['app_key']
    if body.get('app_secret'): telegram_config['app_secret'] = body['app_secret']
    if body.get('account'): telegram_config['account'] = body['account']
    print(f"[TG] 설정 저장: enabled={telegram_config['enabled']}")
    start_tg_poller()
    return {'success': True, 'config': {k:v for k,v in telegram_config.items() if k!='token'}}

@app.post("/api/telegram/test")
async def api_telegram_test(request: Request):
    body = await parse_body(request)
    _tk = body.get('token', '').strip() or telegram_config.get('token', '')
    _ci = body.get('chat_id', '').strip() or telegram_config.get('chat_id', '')
    if not _tk or not _ci:
        return JSONResponse(status_code=400, content={'error': 'token과 chat_id 필요'})
    try:
        _now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        _msg = urllib.parse.quote(f"✅ 태경 AI 자동매매 v3.0 알림 테스트\n\n연결 성공! 🎉\n시간: {_now}\n\nWebSocket 실시간 통신 활성화!\n매수/매도/브리핑 알림이 이 채팅으로 전송됩니다.")
        _url = f'https://api.telegram.org/bot{_tk}/sendMessage?chat_id={_ci}&text={_msg}&parse_mode=HTML'
        req = urllib.request.Request(_url)
        with urllib.request.urlopen(req, timeout=10) as r:
            result = json.loads(r.read().decode('utf-8'))
        if result.get('ok'):
            telegram_config['token'] = _tk
            telegram_config['chat_id'] = _ci
            telegram_config['enabled'] = True
            start_tg_poller()
            return {'success': True, 'message': '텔레그램 테스트 성공!'}
        else:
            return {'success': False, 'error': str(result)}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.post("/api/telegram/history")
async def api_telegram_history(request: Request):
    return {'history': tg_chat_history[-30:], 'enabled': telegram_config.get('enabled', False)}

# ── 브리핑 히스토리 ───────────────────────────────────────────────

@app.post("/api/briefing/run_closing")
async def api_run_closing(request: Request):
    """마감 브리핑 수동 실행"""
    if daily_briefing.get('closing_done'):
        return {'success': False, 'message': '이미 마감 브리핑 완료됨'}
    if auto_trader.running:
        threading.Thread(target=auto_trader._run_closing_briefing, daemon=True).start()
        return {'success': True, 'message': '마감 브리핑 시작됨'}
    return {'success': False, 'message': '자동매매가 실행 중이 아닙니다'}

@app.post("/api/briefing/history")
async def api_briefing_history(request: Request):
    body = await parse_body(request)
    raw_history = load_briefing_history()
    
    # ★ dict 형식 {날짜: 데이터} → list 형식 [{date, ...}] 변환
    history = []
    if isinstance(raw_history, dict):
        for d, v in raw_history.items():
            if isinstance(v, dict):
                entry = dict(v)
                entry['date'] = d
                history.append(entry)
    elif isinstance(raw_history, list):
        history = [h for h in raw_history if isinstance(h, dict)]
    
    # ★ 오늘 daily_briefing(메모리) 데이터도 포함
    _today_s = datetime.now().strftime('%Y-%m-%d')
    if daily_briefing.get('date') == _today_s or daily_briefing.get('data') or daily_briefing.get('closing'):
        _today_entry = next((h for h in history if h.get('date') == _today_s), None)
        if not _today_entry:
            _today_entry = {'date': _today_s}
            history.append(_today_entry)
        if daily_briefing.get('data'):
            _today_entry['global'] = daily_briefing['data']
        for k in ('morning','midday','noon','afternoon','closing','journal','supplement'):
            if daily_briefing.get(k):
                _today_entry[k] = daily_briefing[k]
    
    date_filter = body.get('date', '')
    if date_filter:
        entry = next((h for h in history if h.get('date') == date_filter), None)
        return {'entry': entry, 'date': date_filter}
    else:
        summaries = []
        for h in sorted(history, key=lambda x: x.get('date',''), reverse=True)[:90]:
            d = h.get('date','')
            cl = h.get('closing', {})
            gb = h.get('global', {})
            tr = cl.get('today_result', {}) if isinstance(cl, dict) else {}
            fa = cl.get('forecast_accuracy', {}) if isinstance(cl, dict) else {}
            tp = cl.get('trade_performance', {}) if isinstance(cl, dict) else {}
            tm = cl.get('tomorrow_outlook', {}) if isinstance(cl, dict) else {}
            ko = gb.get('korea_outlook', {}) if isinstance(gb, dict) else {}
            summaries.append({
                'date': d, 'kospi': tr.get('kospi_change',''),
                'kosdaq': tr.get('kosdaq_change',''), 'direction': ko.get('direction',''),
                'score': fa.get('overall_score',''),
                'buys': tp.get('auto_buys', tp.get('buys',0)),
                'sells': tp.get('auto_sells', tp.get('sells',0)),
                'summary': tr.get('summary','')[:60], 'tomorrow': tm.get('direction',''),
            })
        return {'dates': summaries, 'total': len(history)}

# ── 기타 ──────────────────────────────────────────────────────────

@app.post("/api/test/websearch")
async def api_test_websearch(request: Request):
    body = await parse_body(request)
    try:
        ak = body.get('apiKey','')
        query = body.get('query','한국 주식시장 현황')
        if not ak: return {'success': False, 'error': 'API key 필요'}
        payload = json.dumps({
            'model': 'gpt-4o-mini', 'tools': [{'type':'web_search_preview'}],
            'input': query, 'max_output_tokens': 500
        }).encode()
        req = urllib.request.Request('https://api.openai.com/v1/responses',
            data=payload,
            headers={'Authorization': f'Bearer {ak}', 'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=15) as res:
            result = json.loads(res.read())
        return {'success': True, 'result': result}
    except Exception as e:
        return {'success': False, 'error': str(e)}

# ── HTML 서빙 ─────────────────────────────────────────────────────

@app.get("/")
async def serve_root():
    """루트 → HTML 파일로 리다이렉트"""
    base = os.path.dirname(os.path.abspath(__file__))
    for fname in ['stock-analyzer-v8.html', 'stock-analyzer-v7.html', 'stock-analyzer-v6.html']:
        fpath = os.path.join(base, fname)
        if os.path.exists(fpath):
            return FileResponse(fpath, media_type='text/html')
    return HTMLResponse("<h1>stock-analyzer-v8.html 파일을 같은 폴더에 넣어주세요</h1>")

@app.get("/stock-analyzer-v8.html")
async def serve_html_v8():
    html_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'stock-analyzer-v8.html')
    if os.path.exists(html_file):
        return FileResponse(html_file, media_type='text/html')
    return HTMLResponse("<h1>stock-analyzer-v8.html not found</h1>", status_code=404)

@app.get("/stock-analyzer-v6.html")
async def serve_html_v6():
    """v6 URL 하위호환"""
    html_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'stock-analyzer-v8.html')
    if os.path.exists(html_file):
        return FileResponse(html_file, media_type='text/html')
    return HTMLResponse("<h1>stock-analyzer-v8.html not found</h1>", status_code=404)

# ── v3.0 TIER 3: 모바일 전용 경량 대시보드 ────────────────────────

@app.get("/mobile")
@app.get("/m")
async def serve_mobile():
    """폰 최적화 경량 대시보드 — 같은 Wi-Fi에서 192.168.x.x:8080/mobile"""
    now = datetime.now()
    hhmm = now.hour * 100 + now.minute
    today = now.strftime('%Y-%m-%d')
    
    # 데이터 수집
    running = auto_trader.running
    paused = getattr(auto_trader, 'paused', False)
    phase = auto_trader._get_market_phase(hhmm) if running else "대기"
    
    # 오늘 실현손익
    t_pnl = sum(float(t.get('pnl',0) or 0) for t in trade_log 
               if t.get('date')==today and t.get('type') in ('SELL','FORCE_CLOSE','AI_SELL') and t.get('success'))
    t_buys = len([t for t in trade_log if t.get('date')==today and t.get('type')=='AI_BUY' and t.get('success')])
    t_sells = len([t for t in trade_log if t.get('date')==today and t.get('type') in ('SELL','FORCE_CLOSE') and t.get('success')])
    
    # 보유종목
    positions_html = ""
    total_eval = 0
    cash_amt = 0
    total_pnl_eval = 0
    if _bal_cache['data']:
        out2 = (_bal_cache['data'].get('output2',[{}]) or [{}])[0] or {}
        total_eval = int(out2.get('tot_evlu_amt','0') or 0)
        cash_amt = calc_ord_psbl_cash(_bal_cache['data'])
        total_pnl_eval = int(out2.get('evlu_pfls_smtl_amt','0') or 0)
        
        for p in _bal_cache['data'].get('output1', []):
            qty = int(p.get('hldg_qty','0') or 0)
            if qty <= 0: continue
            nm = p.get('prdt_name','?')
            tk = p.get('pdno','')
            cur = int(p.get('prpr','0') or 0)
            avg = float(p.get('pchs_avg_pric','0') or 0)
            pnl_r = float(p.get('evlu_pfls_rt','0') or 0)
            pnl_a = int(p.get('evlu_pfls_amt','0') or 0)
            c = '#00d68f' if pnl_r >= 0 else '#ff4757'
            sign = '+' if pnl_r >= 0 else ''
            auto_tag = ' [AI]' if tk in auto_tickers else ''
            positions_html += f'''<div style="display:flex;justify-content:space-between;align-items:center;padding:12px 0;border-bottom:1px solid #1e2d48">
                <div><div style="font-weight:700;font-size:15px">{nm}{auto_tag}</div>
                <div style="font-size:12px;color:#8b99b4">{qty}주 | 평단 {avg:,.0f}</div></div>
                <div style="text-align:right"><div style="font-size:18px;font-weight:800;color:{c};font-family:monospace">{sign}{pnl_r:.2f}%</div>
                <div style="font-size:12px;color:{c}">{sign}{pnl_a:,}원</div></div></div>'''
    
    if not positions_html:
        positions_html = '<div style="text-align:center;padding:30px;color:#8b99b4">보유종목 없음</div>'
    
    # 최근 거래 5건
    recent_html = ""
    recent = [t for t in trade_log if t.get('type') in ('AI_BUY','SELL','FORCE_CLOSE') and t.get('success')][-5:]
    for t in reversed(recent):
        is_buy = 'BUY' in t.get('type','')
        emoji = '🟢' if is_buy else '🔴'
        pnl_str = ''
        if not is_buy:
            pp = float(t.get('pnl_pct',0) or 0)
            pnl_str = f" {'+'if pp>=0 else ''}{pp:.1f}%"
        dt = (t.get('time','') or '')[:16].replace('T',' ')
        recent_html += f'<div style="padding:6px 0;font-size:13px;border-bottom:1px solid #1e2d48">{emoji} {t.get("name","?")} {t.get("qty",0)}주{pnl_str} <span style="color:#506080;font-size:11px">{dt}</span></div>'
    
    # 상태 색상
    status_color = '#00d68f' if running and not paused else ('#ffb347' if paused else '#ff4757')
    status_text = '가동중' if running and not paused else ('일시정지' if paused else '중지')
    pnl_color = '#00d68f' if t_pnl >= 0 else '#ff4757'
    pnl_sign = '+' if t_pnl >= 0 else ''
    eval_color = '#00d68f' if total_pnl_eval >= 0 else '#ff4757'
    eval_sign = '+' if total_pnl_eval >= 0 else ''
    
    html = f'''<!DOCTYPE html><html lang="ko"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>TK Mobile</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#05080e;color:#e8ecf4;font-family:-apple-system,system-ui,sans-serif;padding:16px;max-width:480px;margin:0 auto}}
.card{{background:#0f1623;border:1px solid #1e2d48;border-radius:14px;padding:16px;margin-bottom:12px}}
.card-t{{font-size:13px;font-weight:700;margin-bottom:10px;display:flex;align-items:center;gap:6px}}
.stat{{text-align:center;padding:12px 8px}}
.stat-v{{font-size:22px;font-weight:800;font-family:monospace}}
.stat-l{{font-size:11px;color:#8b99b4;margin-top:2px}}
.g2{{display:grid;grid-template-columns:1fr 1fr;gap:8px}}
.g3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}}
.btn{{display:block;width:100%;padding:14px;border:none;border-radius:10px;font-size:15px;font-weight:700;cursor:pointer;margin:6px 0}}
.btn-g{{background:#00d68f22;color:#00d68f;border:1px solid #00d68f44}}
.btn-r{{background:#ff475722;color:#ff4757;border:1px solid #ff475744}}
.btn-a{{background:#ffb34722;color:#ffb347;border:1px solid #ffb34744}}
.mono{{font-family:monospace}}
</style></head><body>
<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">
  <div><div style="font-size:18px;font-weight:800">TK <span style="color:#ff3544">자동매매</span></div>
  <div style="font-size:11px;color:#506080">v3.0 Mobile</div></div>
  <div style="display:flex;align-items:center;gap:6px">
    <div style="width:8px;height:8px;border-radius:50%;background:{status_color}"></div>
    <span style="font-size:12px;color:{status_color};font-weight:600">{status_text}</span>
  </div>
</div>

<div class="card">
  <div class="g2">
    <div class="stat"><div class="stat-v" style="color:{pnl_color}">{pnl_sign}{int(t_pnl):,}</div><div class="stat-l">오늘 실현손익</div></div>
    <div class="stat"><div class="stat-v" style="color:{eval_color}">{eval_sign}{total_pnl_eval:,}</div><div class="stat-l">평가손익</div></div>
  </div>
  <div class="g3" style="margin-top:8px">
    <div class="stat"><div class="stat-v mono" style="font-size:16px">{total_eval:,}</div><div class="stat-l">총평가</div></div>
    <div class="stat"><div class="stat-v mono" style="font-size:16px">{cash_amt:,}</div><div class="stat-l">예수금</div></div>
    <div class="stat"><div class="stat-v mono" style="font-size:16px">{t_buys}/{t_sells}</div><div class="stat-l">매수/매도</div></div>
  </div>
</div>

<div class="card">
  <div class="card-t">📦 보유종목 ({len(auto_tickers)})</div>
  {positions_html}
</div>

<div class="card">
  <div class="card-t">📋 최근 거래</div>
  {recent_html if recent_html else '<div style="text-align:center;padding:20px;color:#506080">거래 없음</div>'}
</div>

<div class="card" style="font-size:11px;color:#506080;text-align:center">
  <div>{phase}</div>
  <div style="margin-top:4px">{now.strftime('%H:%M:%S')} | WS 클라이언트: {len(connected_clients)}</div>
</div>

<div class="g2">
  <button class="btn btn-g" onclick="fetch('/api/auto/resume',{{method:'POST',body:'{{}}',headers:{{'Content-Type':'application/json'}}}}).then(()=>location.reload())">▶ 재개</button>
  <button class="btn btn-a" onclick="fetch('/api/auto/pause',{{method:'POST',body:'{{}}',headers:{{'Content-Type':'application/json'}}}}).then(()=>location.reload())">⏸ 정지</button>
</div>
<button class="btn btn-r" onclick="if(confirm('자동매매를 중단합니까?'))fetch('/api/auto/stop',{{method:'POST',body:'{{}}',headers:{{'Content-Type':'application/json'}}}}).then(()=>location.reload())">⏹ 완전 중단</button>
<button class="btn" style="background:#0f1623;color:#8b99b4;border:1px solid #1e2d48" onclick="location.reload()">↻ 새로고침</button>

<div style="text-align:center;margin-top:16px;font-size:10px;color:#506080">
  같은 Wi-Fi에서만 접속 가능 | 외출 시 텔레그램 사용<br>
  <a href="/" style="color:#2d7ff9">PC 전체 화면으로 이동 →</a>
</div>
</body></html>'''
    
    return HTMLResponse(html)

# ── v3.0 전용: WebSocket 상태 API ────────────────────────────────

@app.get("/api/ws/status")
async def api_ws_status():
    """WebSocket 연결 상태 조회 (디버깅용)"""
    return {
        'connected_clients': len(connected_clients),
        'server_version': '4.0',
        'auto_running': auto_trader.running,
        'auto_paused': getattr(auto_trader, 'paused', False),
        'auto_tickers': auto_tickers,
        'uptime': time.time()
    }

# ── v4.0: 기술적 지표 API ──────────────────────────────────────

@app.post("/api/ta")
async def api_technical_indicators(request: Request):
    """v4.0: 종목 기술적 지표 조회 (RSI/MACD/볼린저/MA/ATR)"""
    body = await parse_body(request)
    ticker = body.get('ticker', '')
    if not ticker:
        return {'success': False, 'error': '종목코드 필요'}
    
    try:
        cfg = getattr(auto_trader, 'config', {}) or {}
        ak = cfg.get('app_key', '')
        ase = cfg.get('app_secret', '')
        if not ak:
            return {'success': False, 'error': 'KIS API 키 미설정'}
        
        token = kis_get_token(ak, ase, 'live')
        ta = get_technical_indicators(ak, ase, 'live', token, ticker)
        if not ta:
            return {'success': False, 'error': '지표 데이터 없음'}
        
        return {'success': True, 'ticker': ticker, 'indicators': ta,
                'context': build_ta_context(ta, ticker)}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.post("/api/orderbook")
async def api_orderbook(request: Request):
    """v4.0: 호가잔량 + 체결강도 조회"""
    body = await parse_body(request)
    ticker = body.get('ticker', '')
    if not ticker:
        return {'success': False, 'error': '종목코드 필요'}
    
    try:
        cfg = getattr(auto_trader, 'config', {}) or {}
        ak = cfg.get('app_key', '')
        ase = cfg.get('app_secret', '')
        if not ak:
            return {'success': False, 'error': 'KIS API 키 미설정'}
        
        token = kis_get_token(ak, ase, 'live')
        ob = fetch_orderbook(ak, ase, 'live', token, ticker)
        if not ob:
            return {'success': False, 'error': '호가 데이터 없음'}
        
        return {'success': True, 'ticker': ticker, 'orderbook': ob,
                'context': build_orderbook_context(ob, ticker)}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.post("/api/investor")
async def api_investor_trend(request: Request):
    """v4.0: 투자자별 수급 동향 (외국인/기관/개인)"""
    body = await parse_body(request)
    ticker = body.get('ticker', '')
    if not ticker:
        return {'success': False, 'error': '종목코드 필요'}
    
    try:
        cfg = getattr(auto_trader, 'config', {}) or {}
        ak = cfg.get('app_key', '')
        ase = cfg.get('app_secret', '')
        if not ak:
            return {'success': False, 'error': 'KIS API 키 미설정'}
        
        token = kis_get_token(ak, ase, 'live')
        inv = fetch_investor_trend(ak, ase, 'live', token, ticker)
        if not inv:
            return {'success': False, 'error': '수급 데이터 없음'}
        
        return {'success': True, 'ticker': ticker, 'investor': inv}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.post("/api/backtest")
async def api_backtest(request: Request):
    """v4.0 Phase 3: 매매 전략 백테스트"""
    body = await parse_body(request)
    days = int(body.get('days', 30))
    try:
        result = run_backtest(days)
        return {'success': True, **result}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.get("/api/sectors")
async def api_sector_flows():
    """v4.0 Phase 3: 업종별 자금 흐름"""
    try:
        sf = fetch_sector_flows()
        return {'success': True, **sf}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.post("/api/mtf")
async def api_mtf(request: Request):
    """v4.0 Phase 3: 멀티타임프레임 분석"""
    body = await parse_body(request)
    ticker = body.get('ticker', '')
    if not ticker:
        return {'success': False, 'error': '종목코드 필요'}
    try:
        cfg = getattr(auto_trader, 'config', {}) or {}
        ak = cfg.get('app_key', '')
        ase = cfg.get('app_secret', '')
        if not ak:
            return {'success': False, 'error': 'KIS API 키 미설정'}
        token = kis_get_token(ak, ase, 'live')
        mtf = calc_mtf_signals(ak, ase, 'live', token, ticker)
        return {'success': True, 'ticker': ticker, 'mtf': mtf,
                'context': build_mtf_context(mtf, ticker)}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.post("/api/sell_analysis")
async def api_sell_analysis(request: Request):
    """v4.0 Phase 3: 보유종목 매도 분석"""
    body = await parse_body(request)
    ticker = body.get('ticker', '')
    if not ticker:
        return {'success': False, 'error': '종목코드 필요'}
    try:
        cfg = getattr(auto_trader, 'config', {}) or {}
        ak = cfg.get('app_key', '')
        ase = cfg.get('app_secret', '')
        if not ak:
            return {'success': False, 'error': 'KIS 미설정'}
        token = kis_get_token(ak, ase, 'live')
        
        # 보유종목에서 정보 추출
        bal = get_balance(ak, ase, 'live', token, cfg.get('account',''), cfg.get('account_cd','01'))
        pos_info = None
        for p in bal.get('output1', []):
            if p.get('pdno') == ticker:
                pos_info = p; break
        if not pos_info:
            return {'success': False, 'error': '보유종목 아님'}
        
        qty = int(pos_info.get('hldg_qty','0') or 0)
        avg_price = float(pos_info.get('pchs_avg_pric','0') or 0)
        cur_price = float(pos_info.get('prpr','0') or 0)
        name = pos_info.get('prdt_name', ticker)
        pnl_pct = ((cur_price - avg_price) / avg_price * 100) if avg_price > 0 else 0
        
        ta = get_technical_indicators(ak, ase, 'live', token, ticker)
        ob = fetch_orderbook(ak, ase, 'live', token, ticker)
        mtf = calc_mtf_signals(ak, ase, 'live', token, ticker)
        
        ctx, urgency = build_sell_analysis_context(
            ticker, name, qty, avg_price, cur_price, pnl_pct, ta, ob, mtf)
        
        return {'success': True, 'ticker': ticker, 'name': name,
                'pnl_pct': round(pnl_pct, 2), 'urgency': urgency,
                'context': ctx, 'ta': ta, 'ob': ob, 'mtf': mtf}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.post("/api/sentiment")
async def api_sentiment(request: Request):
    """v4.0 Phase 4: 종목 뉴스 센티멘트 분석"""
    body = await parse_body(request)
    ticker = body.get('ticker', '')
    if not ticker:
        return {'success': False, 'error': '종목코드 필요'}
    try:
        sentiment = get_stock_sentiment(ticker)
        return {'success': True, 'ticker': ticker, 'sentiment': sentiment}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.post("/api/param_optimize")
async def api_param_optimize(request: Request):
    """v4.0 Phase 4: 파라미터 자동최적화 (수동 실행)"""
    body = await parse_body(request)
    days = int(body.get('days', 14))
    try:
        result = analyze_optimal_params(days)
        return {'success': True, **result}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.post("/api/param_optimize/apply")
async def api_param_optimize_apply(request: Request):
    """v4.0 Phase 4: 최적 파라미터 즉시 적용"""
    try:
        cfg = getattr(auto_trader, 'config', {}) or {}
        if not cfg:
            return {'success': False, 'error': '자동매매 설정 없음'}
        changes = auto_apply_optimal_params(cfg)
        return {'success': True, 'changes': changes or []}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.post("/api/backtest/virtual")
async def api_virtual_backtest(request: Request):
    """v4.0 Phase 5: 종목별 가상매매 백테스트"""
    body = await parse_body(request)
    ticker = body.get('ticker', '')
    if not ticker:
        return {'success': False, 'error': '종목코드 필요'}
    try:
        cfg = getattr(auto_trader, 'config', {}) or {}
        ak = cfg.get('app_key', '')
        ase = cfg.get('app_secret', '')
        if not ak: return {'success': False, 'error': 'KIS 미설정'}
        token = kis_get_token(ak, ase, 'live')
        result = run_virtual_backtest(ak, ase, 'live', token, ticker,
            days=int(body.get('days', 20)),
            tp1=float(body.get('tp1', cfg.get('tp1', 5))),
            sl=float(body.get('sl', cfg.get('sl', -5))),
            trail=float(body.get('trail', cfg.get('trailing_pct', 3))))
        return {'success': True, **result}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.get("/api/kelly")
async def api_kelly():
    """v4.0 Phase 5: 켈리 기준 자금관리 계산"""
    try:
        result = calc_kelly_fraction(30)
        return {'success': True, **result}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.post("/api/correlation")
async def api_correlation(request: Request):
    """v4.0 Phase 5: 보유종목간 상관관계"""
    try:
        cfg = getattr(auto_trader, 'config', {}) or {}
        ak = cfg.get('app_key', '')
        ase = cfg.get('app_secret', '')
        if not ak: return {'success': False, 'error': 'KIS 미설정'}
        token = kis_get_token(ak, ase, 'live')
        tickers = list(auto_tickers)[:10]
        if len(tickers) < 2:
            return {'success': False, 'error': '보유종목 2개 이상 필요'}
        result = calc_stock_correlation(ak, ase, 'live', token, tickers)
        return {'success': True, **result}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.get("/api/journal")
async def api_journal():
    """v4.0 Phase 5: 매매일지 조회"""
    try:
        if os.path.exists(TRADE_JOURNAL_FILE):
            with open(TRADE_JOURNAL_FILE, 'r', encoding='utf-8') as f:
                history = json.load(f)
            dates = sorted(history.keys(), reverse=True)
            return {'success': True, 'journals': {d: history[d] for d in dates[:30]}}
        return {'success': True, 'journals': {}}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.post("/api/journal/generate")
async def api_journal_generate(request: Request):
    """v4.0 Phase 5: 매매일지 수동 생성"""
    try:
        journal = generate_daily_journal()
        return {'success': True, **journal}
    except Exception as e:
        return {'success': False, 'error': str(e)}

@app.get("/api/v4/status")
async def api_v4_status():
    """v4.0: 시스템 상태 + Phase 1 모듈 상태"""
    return {
        'version': '4.0',
        'modules': {
            'technical_indicators': True,
            'orderbook_analysis': True,
            'split_orders': True,
            'atr_dynamic_tpsl': True,
            'dynamic_position_sizing': True,
            'pre_tp1_trailing': True,
            'order_tracking': True,
            'kis_realtime_ws': _kis_ws_connected,
            'parallel_scan': True,
            'investor_trend': True,
            'multi_timeframe': True,
            'backtest': True,
            'sector_rotation': True,
            'ai_sell_analysis': True,
            'news_sentiment': True,
            'auto_param_optimize': True,
            'virtual_backtest': True,
            'kelly_criterion': True,
            'correlation_analysis': True,
            'trade_journal': True,
        },
        'caches': {
            'ta_cache_size': len(_ta_cache),
            'orderbook_cache_size': len(_orderbook_cache),
            'pending_orders': len(_pending_orders),
            'split_buy_tracking': len(_split_buy_tracker),
            'investor_cache_size': len(_investor_cache),
            'ws_prices': len(_kis_ws_prices),
            'ws_subscribed': len(_kis_ws_subscribed),
            'mtf_cache_size': len(_mtf_cache),
            'sector_flow_age': int(time.time() - _sector_flow_cache.get('ts', 0)),
            'sentiment_cache_size': len(_sentiment_cache),
        },
        'auto_running': auto_trader.running,
        'auto_paused': getattr(auto_trader, 'paused', False),
        'auto_tickers': auto_tickers,
    }

# ── v3.0 TIER 2: 매매 성과 분석 API ──────────────────────────────

@app.post("/api/analytics")
async def api_analytics(request: Request):
    """매매 성과 통계: 승률, 평균수익, MDD, 종목별 분석"""
    body = await parse_body(request)
    days = int(body.get('days', 30))
    
    try:
        from collections import defaultdict
        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        # 기간 내 매매 필터
        all_trades = [t for t in trade_log if (t.get('date','') or t.get('time','')[:10]) >= cutoff]
        buys = [t for t in all_trades if t.get('type') in ('AI_BUY','CHAT_BUY') and t.get('success')]
        sells = [t for t in all_trades if t.get('type') in ('SELL','FORCE_CLOSE','AI_SELL','CHAT_SELL') and t.get('success')]
        blocked = [t for t in all_trades if t.get('type') in ('BLOCKED','CAPITAL_BLOCK')]
        
        # 승률
        wins = [t for t in sells if float(t.get('pnl',0) or 0) > 0]
        losses = [t for t in sells if float(t.get('pnl',0) or 0) < 0]
        evens = [t for t in sells if float(t.get('pnl',0) or 0) == 0]
        win_rate = round(len(wins) / max(len(wins) + len(losses), 1) * 100, 1)
        
        # 평균 수익/손실
        win_amts = [float(t.get('pnl',0) or 0) for t in wins]
        loss_amts = [abs(float(t.get('pnl',0) or 0)) for t in losses]
        avg_win = round(sum(win_amts) / max(len(win_amts), 1))
        avg_loss = round(sum(loss_amts) / max(len(loss_amts), 1))
        profit_factor = round(sum(win_amts) / max(sum(loss_amts), 1), 2)
        
        # 총 실현손익
        total_pnl = sum(float(t.get('pnl',0) or 0) for t in sells)
        
        # 일별 손익 → MDD 계산
        daily_pnl = defaultdict(float)
        for t in sells:
            d = t.get('date','') or (t.get('time','') or '')[:10]
            daily_pnl[d] += float(t.get('pnl',0) or 0)
        
        dates_sorted = sorted(daily_pnl.keys())
        cumulative = []
        running = 0
        for d in dates_sorted:
            running += daily_pnl[d]
            cumulative.append({'date': d, 'pnl': round(daily_pnl[d]), 'cumulative': round(running)})
        
        # MDD (최대 낙폭)
        peak_val = 0
        max_dd = 0
        for c in cumulative:
            if c['cumulative'] > peak_val:
                peak_val = c['cumulative']
            dd = peak_val - c['cumulative']
            if dd > max_dd:
                max_dd = dd
        
        # 종목별 집계
        stock_stats = defaultdict(lambda: {'buys':0,'sells':0,'wins':0,'losses':0,
                                           'total_pnl':0,'name':'','pnl_list':[]})
        for t in buys:
            tk = t.get('ticker','')
            if tk:
                stock_stats[tk]['buys'] += 1
                stock_stats[tk]['name'] = t.get('name', tk)
        for t in sells:
            tk = t.get('ticker','')
            pnl = float(t.get('pnl',0) or 0)
            if tk:
                stock_stats[tk]['sells'] += 1
                stock_stats[tk]['total_pnl'] += pnl
                stock_stats[tk]['pnl_list'].append(pnl)
                stock_stats[tk]['name'] = t.get('name', tk)
                if pnl > 0: stock_stats[tk]['wins'] += 1
                elif pnl < 0: stock_stats[tk]['losses'] += 1
        
        # 최고/최악 종목
        top_stocks = sorted(stock_stats.items(), key=lambda x: x[1]['total_pnl'], reverse=True)
        best_stocks = [{'ticker':tk,'name':v['name'],'pnl':round(v['total_pnl']),
                       'trades':v['buys']+v['sells'],'wins':v['wins'],'losses':v['losses']}
                      for tk,v in top_stocks[:5] if v['total_pnl'] > 0]
        worst_stocks = [{'ticker':tk,'name':v['name'],'pnl':round(v['total_pnl']),
                        'trades':v['buys']+v['sells'],'wins':v['wins'],'losses':v['losses']}
                       for tk,v in reversed(top_stocks) if v['total_pnl'] < 0][:5]
        
        # 시간대별 성과
        hour_stats = defaultdict(lambda: {'count':0,'pnl':0})
        for t in sells:
            h = (t.get('time','') or '')[11:13]
            if h:
                hour_stats[h]['count'] += 1
                hour_stats[h]['pnl'] += float(t.get('pnl',0) or 0)
        
        best_hour = max(hour_stats.items(), key=lambda x: x[1]['pnl'])[0] if hour_stats else ''
        worst_hour = min(hour_stats.items(), key=lambda x: x[1]['pnl'])[0] if hour_stats else ''
        
        # 평균 보유시간 추정 (매수→매도 시간차)
        # 활성 거래일수
        active_days = len(set(t.get('date','') for t in buys + sells if t.get('date','')))
        avg_trades_per_day = round((len(buys) + len(sells)) / max(active_days, 1), 1)
        
        return {
            'success': True,
            'period': f'{days}일',
            'summary': {
                'total_pnl': round(total_pnl),
                'win_rate': win_rate,
                'total_buys': len(buys),
                'total_sells': len(sells),
                'total_blocked': len(blocked),
                'wins': len(wins),
                'losses': len(losses),
                'evens': len(evens),
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'profit_factor': profit_factor,
                'max_drawdown': round(max_dd),
                'active_days': active_days,
                'avg_trades_per_day': avg_trades_per_day,
                'best_hour': best_hour,
                'worst_hour': worst_hour,
            },
            'daily': cumulative[-30:],
            'best_stocks': best_stocks,
            'worst_stocks': worst_stocks,
            'all_sells': [{'sell_mode':t.get('sell_mode','manual'),'pnl':float(t.get('pnl',0) or 0)} for t in sells],
        }
    except Exception as e:
        import traceback; traceback.print_exc()
        return {'success': False, 'error': str(e)}

# ============= 서버 시작 =============
if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    # Windows CMD 한글 출력 깨짐 방지
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

    # HTML 파일 존재 확인
    html_found = (os.path.exists('stock-analyzer-v8.html') or 
                  os.path.exists('stock-analyzer-v7.html') or
                  os.path.exists('stock-analyzer-v6.html'))
    if not html_found:
        print("WARNING: stock-analyzer-v8.html not found in this folder!")

    print()
    print("  ========================================")
    print("   AI Auto Trading v8.0                   ")
    print("   태경 AI 자동매매 PRO SYSTEM V8.0       ")
    print("   v7.0 전기능 보존 + v8 업그레이드       ")
    print("  ========================================")
    print(f"   WEB: http://localhost:{PORT}")
    print(f"   WS:  ws://localhost:{PORT}/ws")
    print("   Exit: Ctrl + C")
    print("  ========================================")
    print()

    # 이벤트 루프 참조 저장 (sync_broadcast에서 사용)
    def _on_startup():
        global _event_loop
        _event_loop = asyncio.get_event_loop()
        print(f"[WS] Event loop registered OK")
        # 브라우저 자동 열기
        threading.Timer(1.5, lambda: webbrowser.open(f'http://localhost:{PORT}')).start()

    app.add_event_handler("startup", _on_startup)

    # Graceful shutdown
    import signal
    def _shutdown(sig, frame):
        print("\n  Server shutting down...")
        auto_trader.stop()
        save_state()
        sys.exit(0)
    signal.signal(signal.SIGINT, _shutdown)

    # uvicorn 실행
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info",
                access_log=False)  # access_log 끄면 깔끔
