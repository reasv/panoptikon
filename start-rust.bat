@echo off
cd /d "%~dp0"
if not exist "target\release\gateway.exe" (
    echo target\release\gateway.exe not found.
    echo Build it first with: cargo build --release -p gateway
    exit /b 1
)
target\release\gateway.exe --config config\gateway\local.toml
