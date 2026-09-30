@echo off
chcp 65001 > nul
cd /d "%~dp0"
title TK Quant - 백테스트
set PY=python
python --version > nul 2>&1 || set PY=py
echo  Scout 전종목 일봉으로 TK Quant 백테스트 (몇 분) ...
%PY% q_backtest.py %*
echo.
echo  결과: %APPDATA%\TKQuant\backtest_result.md  → Claude에게 보내 점검
pause
