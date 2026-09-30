@echo off
chcp 65001 > nul
title TK자동매매 - 자동 실행 해제
powershell -NoProfile -ExecutionPolicy Bypass -Command "Unregister-ScheduledTask -TaskName 'TKAuto' -Confirm:$false; Write-Host ' 해제 완료'"
pause
