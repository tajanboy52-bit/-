$ErrorActionPreference = 'Stop'
$bat = Join-Path $PSScriptRoot 'TK_Run.bat'
if (-not (Test-Path $bat)) { Write-Host '[오류] TK_Run.bat 을 찾을 수 없습니다.'; exit 1 }
$action = New-ScheduledTaskAction -Execute 'cmd.exe' -Argument ('/c ""' + $bat + '" silent"') -WorkingDirectory $PSScriptRoot
$days = 'Monday','Tuesday','Wednesday','Thursday','Friday'
$triggers = @(
  (New-ScheduledTaskTrigger -AtLogOn),
  (New-ScheduledTaskTrigger -Weekly -DaysOfWeek $days -At '07:30'),
  (New-ScheduledTaskTrigger -Weekly -DaysOfWeek $days -At '15:40'),
  (New-ScheduledTaskTrigger -Weekly -DaysOfWeek $days -At '18:00')
)
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Seconds 0) -MultipleInstances IgnoreNew
Register-ScheduledTask -TaskName 'TKAuto' -Action $action -Trigger $triggers -Settings $settings `
  -Description 'TK자동매매 시스템 자동 실행' -Force | Out-Null
Write-Host ' 등록 완료: 작업 이름 TKAuto'
