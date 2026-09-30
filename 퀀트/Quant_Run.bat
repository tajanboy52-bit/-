@echo off
chcp 65001 > nul
if "%~1"=="" (
  cmd /k ""%~f0" run"
  exit /b
)
title TK Quant - 퀀트 자동매매 (8085)
cd /d "%~dp0"
set SILENT=
if /i "%~1"=="silent" set SILENT=1

echo.
echo  ==========================================
echo    台炅 퀀트 자동매매  TK Quant  (포트 8085)
echo    KIS 모의투자 전용 (실전 주문 없음)
echo    Scout(8082) 데이터를 읽기 전용으로 사용
echo  ==========================================
echo.

set PY=
python --version > nul 2>&1 && set PY=python
if not defined PY ( py --version > nul 2>&1 && set PY=py )
if not defined PY (
  echo  [오류] 파이썬이 설치되어 있지 않습니다.
  pause
  exit /b
)
%PY% -c "import fastapi, uvicorn, pandas, numpy" > nul 2>&1
if errorlevel 1 (
  echo  처음 실행이라 필요한 부품을 설치합니다...
  %PY% -m pip install fastapi uvicorn pandas numpy
)
%PY% -c "import websocket" > nul 2>&1
if errorlevel 1 %PY% -m pip install websocket-client
%PY% -c "import pykrx" > nul 2>&1
if errorlevel 1 %PY% -m pip install "pykrx>=1.2.9"

netstat -ano | findstr ":8085 " | findstr "LISTENING" > nul
if not errorlevel 1 (
  if defined SILENT exit /b
  echo  이미 실행 중입니다. 브라우저만 엽니다.
  start "" http://localhost:8085
  timeout /t 3 > nul
  exit /b
)
if defined SILENT (
  if not exist logs mkdir logs
  start "TK Quant" /min cmd /c "%PY% q_server.py >> logs\quant.log 2>&1"
  exit /b
)
echo  서버를 시작합니다. 잠시 후 브라우저가 열립니다.
echo  ** 이 창을 닫으면 서버가 꺼집니다 (Scout · Danta · Bluechip은 영향 없음) **
echo.
start "" cmd /c "timeout /t 5 > nul & start http://localhost:8085"
%PY% q_server.py
echo.
echo  서버가 종료되었습니다.
pause
