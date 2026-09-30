@echo off
chcp 65001 > nul
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
set PY=
python --version > nul 2>&1 && set PY=python
if not defined PY ( py --version > nul 2>&1 && set PY=py )
if not defined PY ( echo  [오류] 파이썬을 찾을 수 없습니다. & pause & exit /b )

title Stock Scout - 백테스트
echo.
echo  받아둔 3년치 실데이터로 과거 시점마다 추천을 재현하고
echo  매매계획대로 사고팔았다면 어땠는지 검증합니다. 3~10분 걸립니다.
echo.
%PY% scout_backtest.py --save
echo.
echo  결과가 바탕화면 backtest_결과_날짜.txt 에도 저장됐습니다.
pause
