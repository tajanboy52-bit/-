@echo off
chcp 65001 > nul
title TK Danta - 자동 실행 해제
powershell -NoProfile -ExecutionPolicy Bypass -Command "Unregister-ScheduledTask -TaskName 'TKDanta' -Confirm:$false; Write-Host ' 해제 완료 (TKDanta · Scout 등록은 그대로)'"
pause
