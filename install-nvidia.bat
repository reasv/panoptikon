@echo off
setlocal

set VENV=.venv
set PYTHON_VERSION=3.12

REM Add .local\bin to PATH in case just installed UV (first thing!)
set "PATH=%USERPROFILE%\.local\bin;%PATH%"

REM Check for uv (will print nothing if found, error if not)
where uv >nul 2>nul
if %errorlevel% neq 0 (
    echo UV not found, installing UV...
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    set "PATH=%USERPROFILE%\.local\bin;%PATH%"
)

where uv >nul 2>nul
if %errorlevel% neq 0 (
    echo Failed to install or find UV! Please install UV and try again.
    exit /b 1
)

if not exist "%VENV%\" (
    echo Creating .venv with Python %PYTHON_VERSION%...
    uv venv -p %PYTHON_VERSION%
    if %errorlevel% neq 0 (
        echo Failed to create venv. Make sure UV and Python %PYTHON_VERSION% are installed.
        exit /b 1
    )
) else (
    echo .venv already exists, reusing it.
)

call %VENV%\Scripts\activate

echo Installing dependencies for inference and development...
uv pip install --group inference
uv pip install -e .

echo Installing PyTorch with CUDA support (CUDA 12.4 wheels)...
uv pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu124

echo Installation complete. To run Panoptikon:
echo    start.bat

endlocal