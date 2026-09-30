@echo off
chcp 65001 > nul
cd /d "%~dp0"
title TK자동매매 - 백테스트
set PY=python
python --version > nul 2>&1 || set PY=py
%PY% tk_backtest.py %*
echo.
echo  결과: %APPDATA%\TKAuto\backtest_result.md  → Claude에게 보내 점검
pause
