# Use an NVIDIA CUDA base image with Debian
FROM nvidia/cuda:12.6.2-cudnn-runtime-ubuntu24.04

# Set DEBIAN_FRONTEND to noninteractive to avoid timezone configuration prompts
ENV DEBIAN_FRONTEND=noninteractive

# Update package lists and install Python 3, pip, and pipx
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    pipx \
    build-essential \
    python3-dev \
    llvm-14 \
    llvm-14-dev \
    clang-14 \
    && rm -rf /var/lib/apt/lists/*

# Verify that llvm-config is on PATH
RUN which llvm-config || true
RUN llvm-config --version || true

# Verify that Python has _sqlite3 with loadable extensions enabled
RUN python3 -c "import sqlite3; print('SQLite version:', sqlite3.sqlite_version)"
RUN python3 -c "import sqlite3; print('SQLite has loadable extensions:', sqlite3.connect(':memory:').enable_load_extension(True))"

RUN update-alternatives --install /usr/bin/llvm-config llvm-config /usr/bin/llvm-config-14 100

# Then make sure it’s executable by all:
RUN chmod a+rx /usr/bin/llvm-config-14 /usr/bin/llvm-config

# (Optionally confirm it’s set up)
RUN ls -l /usr/bin/llvm-config* && /usr/bin/llvm-config --version

# Ensure pipx is in the PATH for root and appuser
ENV PATH="/root/.local/bin:$PATH"

# Create a directory for the application and adjust permissions for the existing user
RUN mkdir /app && \
    chown -R 1000:1000 /app

# Set the working directory in the container
WORKDIR /app

# Switch to the existing user with UID 1000
USER 1000

# Ensure pipx is in the PATH for the existing user
ENV PATH="/home/ubuntu/.local/bin:$PATH"

# Install pipx and install poetry via pipx for the existing user
RUN pipx install poetry

# Set environment variables for Poetry to enable virtual environments
ENV POETRY_VIRTUALENVS_CREATE=true \
    POETRY_CACHE_DIR=/home/ubuntu/.cache/pypoetry

# Copy the current directory contents into the container
COPY . /app

# Install dependencies using Poetry
RUN poetry install --with inference

ENV ENABLE_CLIENT=false
ENV DISABLE_CLIENT_UPDATE=true

# Expose the port for the application
EXPOSE 6342

# Run the application within the virtual environment
CMD ["poetry", "run", "panoptikon"]
