@echo off
chcp 65001 > nul
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
title Stock Scout - 가상매매 기록 내보내기
set PY=
python --version > nul 2>&1 && set PY=python
if not defined PY ( py --version > nul 2>&1 && set PY=py )
if not defined PY ( echo  [오류] 파이썬을 찾을 수 없습니다. & pause & exit /b )
echo.
echo  실전 가상매매 기록을 압축해서 바탕화면에 저장합니다.
echo  서버가 켜져 있어도 괜찮습니다.
echo.
%PY% scout_vt_export.py
echo.
pause
