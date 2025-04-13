# Use an NVIDIA CUDA base image with Debian
FROM nvidia/cuda:12.6.2-cudnn-runtime-ubuntu24.04

# Set DEBIAN_FRONTEND to noninteractive to avoid timezone configuration prompts
ENV DEBIAN_FRONTEND=noninteractive

# Update package lists and install Python 3, curl (for UV), and build tools
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
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

# Set up llvm-config properly
RUN update-alternatives --install /usr/bin/llvm-config llvm-config /usr/bin/llvm-config-14 100
RUN chmod a+rx /usr/bin/llvm-config-14 /usr/bin/llvm-config
RUN ls -l /usr/bin/llvm-config* && /usr/bin/llvm-config --version

# Install UV (https://github.com/astral-sh/uv)
RUN curl -Ls https://astral.sh/uv/install.sh | sh

# Create a directory for the application and adjust permissions for the existing user
RUN mkdir /app && chown -R 1000:1000 /app

# Set the working directory in the container
WORKDIR /app

# Switch to the existing user with UID 1000
USER 1000

# Add UV to PATH (uv installs to ~/.cargo/bin/uv usually, but this handles both possible locations)
ENV PATH="/home/ubuntu/.cargo/bin:/home/ubuntu/.local/bin:$PATH"

# Copy the project into the container
COPY --chown=1000:1000 . /app

# Create virtual environment and install dependencies with CUDA-enabled PyTorch
RUN uv venv && \
    source .venv/bin/activate && \
    uv pip install --group inference

# Optional app config env vars
ENV ENABLE_CLIENT=false
ENV DISABLE_CLIENT_UPDATE=true

# Expose the app port
EXPOSE 6342

# Run the app with UV
CMD ["uv", "run", "panoptikon"]
