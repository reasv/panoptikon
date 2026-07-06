@echo off
cd /d "%~dp0"
tasklist /FI "IMAGENAME eq gateway.exe" | find /I "gateway.exe" >nul
if not errorlevel 1 (
    echo gateway.exe is already running. Stop it first, then re-run this script.
    exit /b 1
)
cargo build --release -p gateway
if errorlevel 1 (
    echo Build failed, not starting the gateway.
    exit /b 1
)
target\release\gateway.exe --config config\gateway\local.toml
