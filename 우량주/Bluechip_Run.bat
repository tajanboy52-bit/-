@echo off
chcp 65001 > nul
if "%~1"=="" (
  cmd /k ""%~f0" run"
  exit /b
)
title TK Bluechip - 우량주 반등 (8084)
cd /d "%~dp0"
set SILENT=
set WAIT=pause
if /i "%~1"=="silent" ( set SILENT=1 & set WAIT=rem )

echo.
echo  ==========================================
echo    台炅 우량주 반등  TK Bluechip  (포트 8084)
echo    우량주 100 · KIS 모의투자 전용 (실전 주문 없음) · 장중 실시간 웹소켓
echo    Scout(8082) · Danta(8083)와 별개 프로그램
echo  ==========================================
echo.

set PY=
python --version > nul 2>&1 && set PY=python
if not defined PY ( py --version > nul 2>&1 && set PY=py )
if not defined PY (
  echo  [오류] 파이썬이 설치되어 있지 않습니다.
  %WAIT%
  exit /b
)

%PY% -c "import fastapi, uvicorn, pandas, numpy" > nul 2>&1
if errorlevel 1 (
  echo  처음 실행이라 필요한 부품을 설치합니다...
  %PY% -m pip install fastapi uvicorn pandas numpy
)
%PY% -c "import pykrx" > nul 2>&1
if errorlevel 1 %PY% -m pip install "pykrx>=1.2.9"
%PY% -c "import websocket" > nul 2>&1
if errorlevel 1 %PY% -m pip install websocket-client

netstat -ano | findstr ":8084 " | findstr "LISTENING" > nul
if not errorlevel 1 (
  if defined SILENT exit /b
  echo  이미 실행 중입니다. 브라우저만 엽니다.
  start "" http://localhost:8084
  timeout /t 3 > nul
  exit /b
)
if defined SILENT (
  if not exist logs mkdir logs
  start "TK Bluechip" /min cmd /c "%PY% bluechip_server.py >> logs\bluechip.log 2>&1"
  exit /b
)

echo  서버를 시작합니다. 잠시 후 브라우저가 열립니다.
echo  ** 이 창을 닫으면 우량주 서버가 꺼집니다 (Scout · Danta는 영향 없음) **
echo.
start "" cmd /c "timeout /t 5 > nul & start http://localhost:8084"
%PY% bluechip_server.py
echo.
echo  서버가 종료되었습니다.
pause
