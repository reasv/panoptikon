#!/usr/bin/env bash
set -e

PYTHON_VERSION="3.12"
VENV=".venv"

# Check for uv
if ! command -v uv >/dev/null 2>&1; then
  echo "UV not found. Installing UV..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.cargo/bin:$PATH"
fi

# Ensure uv is picked up if installed to ~/.cargo/bin
if ! command -v uv >/dev/null 2>&1; then
  echo "Failed to install or locate uv. Please install uv and try again."
  exit 1
fi

if [ ! -d "$VENV" ]; then
  echo "Creating .venv with Python $PYTHON_VERSION..."
  uv venv -p "$PYTHON_VERSION"
else
  echo ".venv already exists, reusing it."
fi

echo "Activating virtual environment..."
source "$VENV/bin/activate"

echo "Installing dependencies for inference & development..."
uv pip install --group inference
uv pip install -e .

echo "Installing PyTorch & friends with ROCm 6.2.4 (AMD GPU) support..."
uv pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/rocm6.2.4

echo "✅ Install complete. Make sure the ROCm runtime is present and compatible."
echo "To run Panoptikon:"
echo "    ./start.sh"