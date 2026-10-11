@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo ======================================================================
echo KOTAK NEO 08:15 AM DAILY AUTOMATED TOKEN REFRESH
echo ======================================================================
echo Current Time: %DATE% %TIME%
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
    echo ERROR: Python was not found on PATH.
    exit /b 1
)

%PY_CMD% -u kotak_auth.py
set RET=%ERRORLEVEL%
echo Completed with exit code %RET%.
exit /b %RET%
