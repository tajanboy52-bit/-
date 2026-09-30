@echo off
chcp 65001 > nul
title TK Quant - 자동 실행 해제
powershell -NoProfile -ExecutionPolicy Bypass -Command "Unregister-ScheduledTask -TaskName 'TKQuant' -Confirm:$false; Write-Host ' 해제 완료'"
pause
