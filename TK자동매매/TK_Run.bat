@echo off
chcp 65001 > nul
if "%~1"=="" (
  cmd /k ""%~f0" run"
  exit /b
)
title TK자동매매 시스템 (8086)
cd /d "%~dp0"
set SILENT=
if /i "%~1"=="silent" set SILENT=1
echo.
echo  ==========================================
echo    TK자동매매 시스템  (http://127.0.0.1:8086)
echo    한국투자증권 Open API · 모의 → 실전
echo  ==========================================
echo.
set PY=
python --version > nul 2>&1 && set PY=python
if not defined PY ( py --version > nul 2>&1 && set PY=py )
if not defined PY (
  echo  [오류] 파이썬이 설치되어 있지 않습니다. python.org 에서 3.10 이상 설치 (Add to PATH 체크)
  pause
  exit /b
)
%PY% -c "import fastapi, uvicorn, pandas, numpy, websocket, Crypto, pykrx" > nul 2>&1
if errorlevel 1 (
  echo  필요한 부품을 설치합니다 ^(처음 한 번^)...
  %PY% -m pip install -r requirements.txt
)
netstat -ano | findstr ":8086 " | findstr "LISTENING" > nul
if not errorlevel 1 (
  if defined SILENT exit /b
  echo  이미 실행 중입니다. 브라우저만 엽니다.
  start "" http://127.0.0.1:8086
  timeout /t 3 > nul
  exit /b
)
if defined SILENT (
  if not exist logs mkdir logs
  start "TK자동매매" /min cmd /c "%PY% tk_server.py >> logs\tk.log 2>&1"
  exit /b
)
echo  서버를 시작합니다. 잠시 후 브라우저가 열립니다.
echo  ** 이 창을 닫으면 서버가 꺼집니다 — 자동매매 중이면 닫지 마세요 **
echo.
start "" cmd /c "timeout /t 5 > nul & start http://127.0.0.1:8086"
%PY% tk_server.py
echo.
echo  서버가 종료되었습니다.
pause
