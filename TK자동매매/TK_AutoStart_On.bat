@echo off
chcp 65001 > nul
cd /d "%~dp0"
title TK자동매매 - 자동 실행 등록
echo.
echo  윈도우 작업 스케줄러에 TK자동매매 자동 실행을 등록합니다.
echo  (로그온 시 + 평일 07:30 / 15:40 / 18:00 · 작업 이름 TKAuto · 이미 켜져 있으면 그대로)
echo.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0tk_task.ps1"
if errorlevel 1 ( echo. & echo  [오류] 등록 실패. 위 메시지를 캡처해서 보내주세요. )
echo.
echo  ※ 윈도우 설정 → 전원 → 절전 "안 함" 으로 해야 PC가 잠들지 않습니다.
pause
