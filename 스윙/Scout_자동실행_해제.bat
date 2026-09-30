@echo off
chcp 65001 > nul
title Stock Scout - 자동 실행 해제
powershell -NoProfile -ExecutionPolicy Bypass -Command "Unregister-ScheduledTask -TaskName 'StockScout' -Confirm:$false; Write-Host ' 해제 완료'"
pause
