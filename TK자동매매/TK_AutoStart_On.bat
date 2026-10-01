@echo off
chcp 65001 > nul
cd /d "%~dp0"
title TK자동매매 - 자동 실행 등록
echo.
echo  윈도우 작업 스케줄러에 TK자동매매 자동 실행을 등록합니다.
echo  (로그온 시 + 평일 07:30 / 15:40 / 18:00 · 작업 이름 TKAuto · 이미 켜져 있으면 그대로)
echo  평일 07:30에는 PC가 절전 중이어도 깨워서 실행합니다 (절전 해제 타이머).
echo.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0tk_task.ps1"
if errorlevel 1 ( echo. & echo  [오류] 등록 실패. 위 메시지를 캡처해서 보내주세요. )
echo.
echo  ※ 권장 전원 설정
echo    - 절전: 1시간 (낮에는 앱이 PC를 깨어 있게 유지 · 밤 21:30 뒤엔 잠들어도 됨)
echo    - 제어판 → 전원 옵션 → 고급 → 절전 → "절전 해제 타이머 허용" = 사용
echo    - 덮개를 닫아도 아무것도 안 함 (노트북) · 윈도우 업데이트 다시 시작 시간은 장 마감 뒤로
echo    - 완전히 끄기(시스템 종료)를 하면 아침에 못 깨웁니다 → 끄지 말고 절전만
pause
