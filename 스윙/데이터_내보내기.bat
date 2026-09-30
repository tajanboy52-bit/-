@echo off
chcp 65001 > nul
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
set PY=
python --version > nul 2>&1 && set PY=python
if not defined PY ( py --version > nul 2>&1 && set PY=py )
if not defined PY ( echo  [오류] 파이썬을 찾을 수 없습니다. & pause & exit /b )

title Stock Scout - 데이터 내보내기
echo.
echo  후보풀 3년치 일봉·수급·종목정보를 압축해서 바탕화면에 저장합니다.
echo  서버가 켜져 있어도 괜찮습니다. 1~2분 걸립니다.
echo.
%PY% scout_export.py
echo.
echo  바탕화면의 scout_data_날짜.zip 파일을 Claude에게 올려주세요.
pause
