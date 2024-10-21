# Use an NVIDIA CUDA base image with Debian
FROM nvidia/cuda:12.6.2-cudnn-runtime-ubuntu24.04

# Set DEBIAN_FRONTEND to noninteractive to avoid timezone configuration prompts
ENV DEBIAN_FRONTEND=noninteractive

# Update package lists and install Python 3, pip, and pipx
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    pipx

# Verify that Python has _sqlite3 with loadable extensions enabled
RUN python3 -c "import sqlite3; print('SQLite version:', sqlite3.sqlite_version)"
RUN python3 -c "import sqlite3; print('SQLite has loadable extensions:', sqlite3.connect(':memory:').enable_load_extension(True))"

# Ensure pipx is in the PATH for root and appuser
ENV PATH="/root/.local/bin:$PATH"

# Create a directory for the application and add a non-root user
RUN mkdir /app && \
    adduser --disabled-password --gecos '' appuser && \
    chown -R appuser /app

# Set the working directory in the container
WORKDIR /app

# Switch to the appuser
USER appuser

# Ensure pipx is in the PATH for appuser
ENV PATH="/home/appuser/.local/bin:$PATH"

# Install pipx for appuser and install poetry via pipx for appuser
RUN pipx install poetry

# Set environment variables for Poetry to enable virtual environments
ENV POETRY_VIRTUALENVS_CREATE=true \
    POETRY_CACHE_DIR=/home/appuser/.cache/pypoetry

# Copy the current directory contents into the container
COPY . /app

# Install dependencies using Poetry as appuser
RUN poetry install --with inference

ENV ENABLE_CLIENT=false
ENV DISABLE_CLIENT_UPDATE=true

# Expose the port for the application
EXPOSE 6342

# Run the application within the virtual environment
CMD ["poetry", "run", "panoptikon"]
