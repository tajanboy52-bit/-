@echo off
chcp 65001 > nul
rem keep window open on any error: relaunch normal runs inside cmd /k (scheduler "silent" runs are excluded)
if "%~1"=="" (
  cmd /k ""%~f0" run"
  exit /b
)
title TK Stock Scout - 종목추천 엔진
cd /d "%~dp0"
set SILENT=
set WAIT=pause
if /i "%~1"=="silent" ( set SILENT=1 & set WAIT=rem )

echo.
echo  ==========================================
echo    TK Stock Scout 종목추천 엔진
echo    분석 전용 - 주문 기능 없음
echo  ==========================================
echo.

rem ── 파이썬 찾기 (python 또는 py)
set PY=
python --version > nul 2>&1 && set PY=python
if not defined PY ( py --version > nul 2>&1 && set PY=py )
if not defined PY (
  echo  [오류] 파이썬이 설치되어 있지 않습니다.
  echo.
  echo   https://www.python.org/downloads 에서 설치하세요.
  echo   설치 첫 화면에서 "Add python.exe to PATH" 를 꼭 체크하세요.
  echo.
  %WAIT%
  exit /b
)

rem ── 필요한 파일 확인
for %%f in (scout_server.py scout_db.py scout_engines.py scout_strategies.py scout_ext.py scout.html) do (
  if not exist "%%f" (
    echo  [오류] %%f 파일이 없습니다.
    echo   Scout 전체 압축 파일을 이 폴더에 다시 풀어주세요.
    echo.
    %WAIT%
    exit /b
  )
)

rem ── 부품 설치 (처음 한 번만)
%PY% -c "import fastapi, uvicorn, websockets" > nul 2>&1
if errorlevel 1 (
  echo  처음 실행이라 필요한 부품을 설치합니다. 1~2분 걸립니다...
  echo.
  %PY% -m pip install --upgrade pip > nul 2>&1
  %PY% -m pip install fastapi uvicorn websockets
  if errorlevel 1 (
    echo.
    echo  [오류] 설치 실패. 인터넷 연결을 확인하고 다시 실행하세요.
    %WAIT%
    exit /b
  )
  echo.
)

rem ── 연기금 수급 자동 수집용 pykrx (없어도 서버는 동작)
%PY% -m pip show pykrx > nul 2>&1
if errorlevel 1 (
  echo  연기금 수급 수집 부품 pykrx 를 설치합니다...
  %PY% -m pip install "pykrx>=1.2.9" > nul 2>&1
)

rem ── 이미 실행 중인지 확인
netstat -ano | findstr ":8082 " | findstr "LISTENING" > nul
if not errorlevel 1 (
  if defined SILENT exit /b
  echo  이미 실행 중입니다. 브라우저만 엽니다.
  start "" http://localhost:8082
  timeout /t 3 > nul
  exit /b
)
if defined SILENT (
  if not exist logs mkdir logs
  start "TK Stock Scout" /min cmd /c "%PY% scout_server.py >> logs\server.log 2>&1"
  exit /b
)

echo  서버를 시작합니다. 잠시 후 브라우저가 열립니다.
echo  ** 이 검은 창을 닫으면 서버가 꺼집니다 (알림·자동스캔도 멈춤) **
echo  ** 구축·스캔 진행 상황은 이 창에도 표시됩니다 **
echo.
start "" cmd /c "timeout /t 4 > nul & start http://localhost:8082"
%PY% scout_server.py

echo.
echo  서버가 종료되었습니다.
pause
