@echo off
chcp 65001 > nul
cd /d "%~dp0"
title TK Danta - 자동 실행 등록
echo.
echo  윈도우 작업 스케줄러에 TK Danta(자동단타매매) 자동 실행을 등록합니다.
echo  (로그온 시 + 평일 08:20 / 15:30 / 18:00 · 작업 이름 TKDanta · Scout 등록과 별개)
echo.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0danta_task.ps1"
if errorlevel 1 ( echo. & echo  [오류] 등록 실패. 위 메시지를 캡처해서 보내주세요. )
echo.
pause
