$ErrorActionPreference = 'Stop'
$bat = Join-Path $PSScriptRoot 'Quant_Run.bat'
if (-not (Test-Path $bat)) { Write-Host '[오류] Quant_Run.bat 을 찾을 수 없습니다.'; exit 1 }
$action = New-ScheduledTaskAction -Execute 'cmd.exe' -Argument ('/c ""' + $bat + '" silent"') -WorkingDirectory $PSScriptRoot
$days = 'Monday','Tuesday','Wednesday','Thursday','Friday'
$triggers = @(
  (New-ScheduledTaskTrigger -AtLogOn),
  (New-ScheduledTaskTrigger -Weekly -DaysOfWeek $days -At '08:05'),
  (New-ScheduledTaskTrigger -Weekly -DaysOfWeek $days -At '15:30'),
  (New-ScheduledTaskTrigger -Weekly -DaysOfWeek $days -At '18:30')
)
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Seconds 0) -MultipleInstances IgnoreNew
Register-ScheduledTask -TaskName 'TKQuant' -Action $action -Trigger $triggers -Settings $settings `
  -Description 'TK Quant 퀀트 자동매매 (KIS 모의투자) 자동 실행' -Force | Out-Null
Write-Host ' 등록 완료: 작업 이름 TKQuant'
