# PowerShell Script to register daily 08:15 AM Kotak Neo Login Task
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$batFile = Join-Path $scriptDir "schedule_kotak_token.bat"

Write-Host "Registering Daily Kotak Neo Login Task at 08:15 AM IST..." -ForegroundColor Cyan
Write-Host "Target: $batFile"

$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$batFile`""
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday, Tuesday, Wednesday, Thursday, Friday -At 8:15am
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -WakeToRun -StartWhenAvailable

Register-ScheduledTask -TaskName "KotakDailyTokenRefresh815AM" -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null

Write-Host "Successfully registered Windows Task: KotakDailyTokenRefresh815AM (Daily 08:15 AM Weekdays)" -ForegroundColor Green
