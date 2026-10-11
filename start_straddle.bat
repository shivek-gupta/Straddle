@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo ======================================================================
echo ⚡ STRADDLE PRO - KOTAK NEO ALGORITHMIC PLATFORM
echo ======================================================================
echo.

set "PY_CMD="
where python >nul 2>&1
if not errorlevel 1 (
    set "PY_CMD=python"
) else (
    where py >nul 2>&1
    if not errorlevel 1 (
        set "PY_CMD=py"
    )
)

if "%PY_CMD%"=="" (
    echo ERROR: Python was not found on your system PATH.
    pause
    exit /b 1
)

:: Verify dependencies
%PY_CMD% -c "import flask, requests, pyotp, kotakneoapi" >nul 2>&1
if errorlevel 1 (
    echo Installing required packages (kotakneoapi, pyotp, flask)...
    %PY_CMD% -m pip install -r requirements.txt
    if errorlevel 1 (
        echo ERROR: Failed to install required packages.
        pause
        exit /b 1
    )
    echo.
)

:: Launch Straddle Platform
echo Launching Straddle Platform on Port 5001...
start http://localhost:5001
%PY_CMD% -u web_server.py

echo.
echo Server stopped. Press any key to exit.
pause >nul
