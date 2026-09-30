$ErrorActionPreference = 'Stop'
$bat = Join-Path $PSScriptRoot 'Bluechip_Run.bat'
if (-not (Test-Path $bat)) { Write-Host '[오류] Bluechip_Run.bat 을 찾을 수 없습니다.'; exit 1 }
$action = New-ScheduledTaskAction -Execute 'cmd.exe' -Argument ('/c ""' + $bat + '" silent"') -WorkingDirectory $PSScriptRoot
$days = 'Monday','Tuesday','Wednesday','Thursday','Friday'
$triggers = @(
  (New-ScheduledTaskTrigger -AtLogOn),
  (New-ScheduledTaskTrigger -Weekly -DaysOfWeek $days -At '08:05'),
  (New-ScheduledTaskTrigger -Weekly -DaysOfWeek $days -At '15:30'),
  (New-ScheduledTaskTrigger -Weekly -DaysOfWeek $days -At '18:00')
)
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Seconds 0) -MultipleInstances IgnoreNew
Register-ScheduledTask -TaskName 'TKBluechip' -Action $action -Trigger $triggers -Settings $settings `
  -Description 'TK Bluechip 우량주 반등 · H1 모의투자 자동 실행 (Scout · Danta와 별개)' -Force | Out-Null
Write-Host ''
Write-Host ' 등록 완료: 작업 이름 TKBluechip (Scout의 StockScout · Danta의 TKDanta 작업과 별개)'
