$ErrorActionPreference = 'Stop'
$bat = Join-Path $PSScriptRoot 'Scout_실행.bat'
if (-not (Test-Path $bat)) { Write-Host '[오류] Scout_실행.bat 을 찾을 수 없습니다.'; exit 1 }
$action = New-ScheduledTaskAction -Execute 'cmd.exe' -Argument ('/c ""' + $bat + '" silent"') -WorkingDirectory $PSScriptRoot
$days = 'Monday','Tuesday','Wednesday','Thursday','Friday'
$triggers = @(
  (New-ScheduledTaskTrigger -AtLogOn),
  (New-ScheduledTaskTrigger -Weekly -DaysOfWeek $days -At '08:20'),
  (New-ScheduledTaskTrigger -Weekly -DaysOfWeek $days -At '15:30'),
  (New-ScheduledTaskTrigger -Weekly -DaysOfWeek $days -At '18:00')
)
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Seconds 0) -MultipleInstances IgnoreNew
Register-ScheduledTask -TaskName 'StockScout' -Action $action -Trigger $triggers -Settings $settings `
  -Description 'Stock Scout 자동 실행 (로그온 · 평일 08:20 / 15:30 / 18:00 · 놓친 실행은 켜질 때 실행)' -Force | Out-Null
Write-Host ''
Write-Host ' 등록 완료: 작업 이름 StockScout'
Write-Host ' - 로그온할 때, 평일 08:20 / 15:30 / 18:00 에 Scout가 꺼져 있으면 자동으로 켭니다'
Write-Host ' - PC가 꺼져 있어 놓친 시각은 다음에 켜질 때 바로 실행합니다'
Write-Host ' - 이미 켜져 있으면 아무것도 하지 않습니다'
