#!/usr/bin/env bash
set -e

VENV=".venv"

if [ ! -d "$VENV" ]; then
  echo "Virtual environment '$VENV' does not exist. Please create it and install the project and dependencies first."
  exit 1
fi

# Activate the virtual environment
source "$VENV/bin/activate"

echo "Launching Panoptikon..."

# Try to run panoptikon CLI script, fallback to python -m
if command -v panoptikon &>/dev/null; then
  exec panoptikon "$@"
else
  exec python -m panoptikon "$@"
fi