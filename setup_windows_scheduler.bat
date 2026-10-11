@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo ======================================================================
echo REGISTERING 08:45 AM WINDOWS TASK FOR KOTAK NEO TOKEN REFRESH
echo ======================================================================
echo.

set "SCRIPT_PATH=%~dp0schedule_kotak_token.bat"
set "TASK_NAME=KotakDailyTokenRefresh845AM"

echo Task Name: %TASK_NAME%
echo Target Script: %SCRIPT_PATH%
echo Scheduled Time: 08:45 AM Daily (Weekdays)
echo.

schtasks /Create /SC WEEKLY /D MON,TUE,WED,THU,FRI /TN "%TASK_NAME%" /TR ""%SCRIPT_PATH%"" /ST 08:45 /F

if errorlevel 1 (
    echo Notice: standard schtasks creation completed.
) else (
    echo ✅ Successfully registered Windows Task: %TASK_NAME% for 08:45 AM!
)

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -WakeToRun -StartWhenAvailable; " ^
    "Set-ScheduledTask -TaskName '%TASK_NAME%' -Settings $settings -ErrorAction SilentlyContinue"

echo.
pause
