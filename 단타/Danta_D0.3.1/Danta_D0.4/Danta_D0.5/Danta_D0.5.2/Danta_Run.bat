@echo off
chcp 65001 > nul
if "%~1"=="" (
  cmd /k ""%~f0" run"
  exit /b
)
title TK Danta - 자동단타매매 (8083)
cd /d "%~dp0"
set SILENT=
set WAIT=pause
if /i "%~1"=="silent" ( set SILENT=1 & set WAIT=rem )

echo.
echo  ==========================================
echo    台炅 자동단타매매  TK Danta  (포트 8083)
echo    가상 단타 + 매도 연구 + 1분봉 수집 - 주문 기능 없음
echo    가상추천매매 Scout(8082)와 별개 프로그램
echo  ==========================================
echo.

set PY=
python --version > nul 2>&1 && set PY=python
if not defined PY ( py --version > nul 2>&1 && set PY=py )
if not defined PY (
  echo  [오류] 파이썬이 설치되어 있지 않습니다. https://www.python.org/downloads
  %WAIT%
  exit /b
)

for %%f in (danta_server.py danta_db.py danta_kis.py danta.html) do (
  if not exist "%%f" (
    echo  [오류] %%f 파일이 없습니다. Danta 압축 파일을 이 폴더에 다시 풀어주세요.
    %WAIT%
    exit /b
  )
)

%PY% -c "import fastapi, uvicorn" > nul 2>&1
if errorlevel 1 (
  echo  처음 실행이라 필요한 부품을 설치합니다...
  %PY% -m pip install fastapi uvicorn
)

netstat -ano | findstr ":8083 " | findstr "LISTENING" > nul
if not errorlevel 1 (
  if defined SILENT exit /b
  echo  이미 실행 중입니다. 브라우저만 엽니다.
  start "" http://localhost:8083
  timeout /t 3 > nul
  exit /b
)
if defined SILENT (
  if not exist logs mkdir logs
  start "TK Danta" /min cmd /c "%PY% danta_server.py >> logs\danta.log 2>&1"
  exit /b
)

echo  서버를 시작합니다. 잠시 후 브라우저가 열립니다.
echo  ** 이 창을 닫으면 단타 서버가 꺼집니다 (Scout는 영향 없음) **
echo.
start "" cmd /c "timeout /t 4 > nul & start http://localhost:8083"
%PY% danta_server.py
echo.
echo  서버가 종료되었습니다.
pause
