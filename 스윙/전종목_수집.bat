@echo off
chcp 65001 > nul
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
title Stock Scout - 전종목 3년치 수집
set PY=
python --version > nul 2>&1 && set PY=python
if not defined PY ( py --version > nul 2>&1 && set PY=py )
if not defined PY ( echo  [오류] 파이썬을 찾을 수 없습니다. & pause & exit /b )

for %%f in (scout_allmarket_collect.py scout_flow_collect.py scout_export.py scout_db.py) do (
  if not exist "%%f" ( echo  [오류] %%f 파일이 없습니다. Scout 폴더에 같이 넣어주세요. & pause & exit /b )
)

echo.
echo  ==========================================
echo    전종목 일봉 + 수급 수집 (상장폐지 종목 포함)
echo    가격 2022-09~ / 외국인·기관·연기금 순매수 2023-09~
echo  ==========================================
echo.
echo  KRX 정보데이터시스템 계정이 필요합니다 (data.krx.co.kr 무료 회원가입)
echo.

rem ── pykrx 최신판 필요 (로그인 자동 갱신 기능은 1.2.9 이상)
%PY% -m pip install --upgrade "pykrx>=1.2.9"
if errorlevel 1 ( echo  [오류] pykrx 설치 실패. 인터넷 연결을 확인하세요. & pause & exit /b )

echo.
echo  약 1시간 30분~2시간 걸립니다. 자는 동안 돌려두셔도 됩니다.
echo  Scout 서버는 켜둬도 됩니다.
echo  중간에 창을 닫아도 다시 실행하면 이어서 받습니다.
echo.
%PY% scout_allmarket_collect.py
echo.
pause
