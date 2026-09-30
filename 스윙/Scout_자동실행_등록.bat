@echo off
chcp 65001 > nul
cd /d "%~dp0"
title Stock Scout - 자동 실행 등록
echo.
echo  윈도우 작업 스케줄러에 Scout 자동 실행을 등록합니다.
echo  (로그온 시 + 평일 08:20 / 15:30 / 18:00 · 놓친 실행은 켜질 때 실행)
echo.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scout_task.ps1"
if errorlevel 1 ( echo. & echo  [오류] 등록 실패. 위 메시지를 캡처해서 보내주세요. )
echo.
echo  ※ 윈도우 절전 모드는 따로 "안 함"으로 설정해야 PC가 잠들지 않습니다.
echo.
pause
