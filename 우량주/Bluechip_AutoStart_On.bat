@echo off
chcp 65001 > nul
cd /d "%~dp0"
title TK Bluechip - 자동 실행 등록
echo.
echo  윈도우 작업 스케줄러에 TK Bluechip(우량주 반등 · H1 모의투자) 자동 실행을 등록합니다.
echo  (로그온 시 + 평일 08:05 / 15:30 / 18:00 · 작업 이름 TKBluechip · Scout · Danta 등록과 별개)
echo  이미 켜져 있으면 아무것도 하지 않습니다. PC가 재부팅돼도 08:20 장전 점검 · 08:35 주문 전에 자동으로 켜집니다.
echo.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0bluechip_task.ps1"
if errorlevel 1 ( echo. & echo  [오류] 등록 실패. 위 메시지를 캡처해서 보내주세요. )
echo.
echo  ※ 윈도우 절전 모드는 따로 "안 함"으로 설정해야 PC가 잠들지 않습니다.
echo.
pause
