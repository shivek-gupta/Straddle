@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo ======================================================================
echo REGISTERING 08:15 AM WINDOWS TASK FOR KOTAK NEO TOKEN REFRESH
echo ======================================================================
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0register_scheduler.ps1"

echo.
pause
