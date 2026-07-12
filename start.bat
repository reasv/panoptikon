@echo off
cd /d "%~dp0"
if not exist target\release\panoptikon.exe (
    echo target\release\panoptikon.exe not found.
    echo Build it first with: cargo build --release -p panoptikon
    exit /b 1
)
target\release\panoptikon.exe --config config\server\local.toml %*
