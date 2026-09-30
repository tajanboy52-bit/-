@echo off
chcp 65001 > nul
title TK Bluechip - 자동 실행 해제
powershell -NoProfile -ExecutionPolicy Bypass -Command "Unregister-ScheduledTask -TaskName 'TKBluechip' -Confirm:$false; Write-Host ' 해제 완료 (TKBluechip · Scout · Danta 등록은 그대로)'"
pause
