@echo off
chcp 65001 > nul
if "%~1"=="" goto relaunch
goto start
:relaunch
cmd /k ""%~f0" run"
exit /b

:start
title TK자동매매 시스템 (8086)
cd /d "%~dp0"
set SILENT=
if /i "%~1"=="silent" set SILENT=1
echo.
echo  ==========================================
echo    TK자동매매 시스템  - http://127.0.0.1:8086
echo    한국투자증권 Open API · 모의 → 실전
echo  ==========================================
echo.

rem ── 파이썬 찾기: py → python → 기본 설치 폴더 ──
set PY=
py -3 --version > nul 2>&1
if not errorlevel 1 set PY=py -3
if defined PY goto have_py
python --version > nul 2>&1
if not errorlevel 1 set PY=python
if defined PY goto have_py
for /d %%D in ("%LocalAppData%\Programs\Python\Python3*") do if exist "%%D\python.exe" set PY="%%D\python.exe"
if defined PY goto have_py
echo  [오류] 파이썬을 찾지 못했습니다.
echo         python.org 에서 3.10 이상 설치 - 설치 첫 화면에서 Add python.exe to PATH 체크
echo         설치 뒤 이 창을 닫고 TK_Run.bat 을 다시 실행하세요.
pause
exit /b

:have_py
echo  파이썬: %PY%
%PY% --version

rem ── 필요한 부품 (처음 한 번만 설치) ──
%PY% -c "import fastapi, uvicorn, pandas, numpy, websocket, Crypto, pykrx" > nul 2>&1
if not errorlevel 1 goto parts_ok
echo  필요한 부품을 설치합니다 - 처음 한 번, 몇 분 걸릴 수 있음
%PY% -m pip install -r requirements.txt
%PY% -c "import fastapi, uvicorn, pandas, numpy, websocket, Crypto, pykrx" > nul 2>&1
if not errorlevel 1 goto parts_ok
echo.
echo  [오류] 부품 설치에 실패했습니다. 위 메시지를 캡처해서 보내주세요.
pause
exit /b

:parts_ok
rem ── 이미 실행 중이면 브라우저만 ──
netstat -ano | findstr ":8086 " | findstr "LISTENING" > nul
if errorlevel 1 goto run_server
if defined SILENT exit /b
echo  이미 실행 중입니다. 브라우저만 엽니다.
start "" http://127.0.0.1:8086
timeout /t 3 > nul
exit /b

:run_server
if not defined SILENT goto run_window
if not exist logs mkdir logs
start "TK자동매매" /min cmd /c "%PY% tk_server.py >> logs\tk.log 2>&1"
exit /b

:run_window
echo  서버를 시작합니다. 잠시 후 브라우저가 열립니다.
echo  ** 이 창을 닫으면 서버가 꺼집니다 - 자동매매 중이면 닫지 마세요 **
echo.
start "" cmd /c "timeout /t 5 > nul & start http://127.0.0.1:8086"
%PY% tk_server.py
echo.
echo  서버가 종료되었습니다. 위에 오류가 있으면 캡처해서 보내주세요.
pause
