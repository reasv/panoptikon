#!/usr/bin/env bash
set -e

PYTHON_VERSION="3.12"
VENV=".venv"

# Ensure relevant bin dirs are in PATH for this script
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"

# Check for uv
if ! command -v uv >/dev/null 2>&1; then
  echo "UV not found. Installing UV..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
  # This covers both .local/bin and .cargo/bin in our PATH already
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "Failed to install or locate uv. Please install uv and try again."
  exit 1
fi

echo "UV found at: $(command -v uv)"

if [ ! -d "$VENV" ]; then
  echo "Creating .venv with Python $PYTHON_VERSION..."
  uv venv -p "$PYTHON_VERSION"
else
  echo ".venv already exists, reusing it."
fi

echo "Activating virtual environment..."
source "$VENV/bin/activate"

echo "Installing dependencies for inference & development (CPU or system GPU as available)..."
uv pip install --group inference
uv pip install -e .

echo
echo "✅ Install complete. To run Panoptikon:"
echo "    ./start.sh"