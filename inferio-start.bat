@echo off
setlocal

set VENV=.venv

if not exist %VENV% (
    echo Virtual environment '%VENV%' does not exist. Please create it and install the project and dependencies first.
    exit /b 1
)

call %VENV%\Scripts\activate

echo Launching inferio...

where inferio >nul 2>nul
if %errorlevel%==0 (
    inferio %*
) else (
    python -m inferio %*
)

endlocal