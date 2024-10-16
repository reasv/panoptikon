# Use an NVIDIA CUDA base image with Debian
FROM nvidia/cuda:12.2.0-runtime-ubuntu22.04

# Set DEBIAN_FRONTEND to noninteractive to avoid timezone configuration prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary dependencies for building SQLite, Python, Node.js, and other build tools
RUN apt-get update && \
    apt-get install -y \
    software-properties-common \
    wget \
    curl \
    build-essential \
    libffi-dev \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libncurses5-dev \
    libgdbm-dev \
    libnss3-dev \
    liblzma-dev \
    tk-dev \
    unzip \
    git \
    make \
    gcc \
    pkg-config \
    && apt-get clean

# Install the latest SQLite 3.46.1 from source (autoconf package)
RUN SQLITE_VERSION=3.46.1 && \
    wget https://www.sqlite.org/2024/sqlite-autoconf-3460100.tar.gz && \
    tar -xzf sqlite-autoconf-3460100.tar.gz && \
    cd sqlite-autoconf-3460100 && \
    ./configure --prefix=/usr/local && \
    make -j$(nproc) && \
    make install && \
    ldconfig && \
    cd .. && rm -rf sqlite-autoconf-3460100.tar.gz sqlite-autoconf-3460100

# Set environment variables to help Python find SQLite
ENV CFLAGS="-I/usr/local/include" \
    LDFLAGS="-L/usr/local/lib"

# Install Python 3.12 from source, ensuring it detects the newly installed SQLite
RUN PYTHON_VERSION=3.12.0 && \
    wget https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz && \
    tar -xzf Python-$PYTHON_VERSION.tgz && \
    cd Python-$PYTHON_VERSION && \
    ./configure --enable-optimizations --with-ensurepip=install --enable-loadable-sqlite-extensions && \
    make -j$(nproc) && \
    make altinstall && \
    # Remove existing symbolic links if they exist
    [ -e /usr/bin/python3 ] && rm /usr/bin/python3 || true && \
    [ -e /usr/bin/pip3 ] && rm /usr/bin/pip3 || true && \
    # Create new symbolic links
    ln -s /usr/local/bin/python3.12 /usr/bin/python3 && \
    ln -s /usr/local/bin/pip3.12 /usr/bin/pip3 && \
    cd .. && \
    rm -rf Python-$PYTHON_VERSION.tgz Python-$PYTHON_VERSION

# Verify that Python has _sqlite3 with loadable extensions enabled
RUN python3 -c "import sqlite3; print('SQLite version:', sqlite3.sqlite_version)"
RUN python3 -c "import sqlite3; print('SQLite has loadable extensions:', sqlite3.connect(':memory:').enable_load_extension(True))"

# Upgrade pip and install Poetry globally
RUN pip3 install --upgrade pip && \
    pip3 install poetry

# Install Node.js (version 20+) and NPM
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    npm install -g npm@latest

# Create a directory for the application and add a non-root user
RUN mkdir /app && \
    adduser --disabled-password --gecos '' appuser && \
    chown -R appuser /app

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Clone panoptikon-ui if not already present, to avoid issues with missing repository
RUN if [ ! -d "/app/src/searchui/panoptikon-ui" ]; then \
    git clone https://github.com/reasv/panoptikon-ui.git /app/src/searchui/panoptikon-ui; \
    fi

# Change ownership of app directory to the new user
RUN chown -R appuser /app

# Switch to the app user
USER appuser

# Set environment variables for Poetry to enable virtual environments
ENV POETRY_VIRTUALENVS_CREATE=true \
    POETRY_CACHE_DIR=/home/appuser/.cache/pypoetry \
    PATH="/home/appuser/.local/bin:$PATH"

# Configure Poetry and install dependencies as appuser
RUN poetry install --with inference

ARG RESTRICTED_MODE
ARG INFERENCE_API_URL
ARG PANOPTIKON_API_URL
ENV RESTRICTED_MODE=${RESTRICTED_MODE}
ENV INFERENCE_API_URL=${INFERENCE_API_URL}
ENV PANOPTIKON_API_URL=${PANOPTIKON_API_URL}
# Set up Node.js project and build Next.js application
WORKDIR /app/src/searchui/panoptikon-ui
RUN npm install --include=dev && \
    npx --yes next build

# Expose the port for the application
EXPOSE 6342

WORKDIR /app
# Run the application within the virtual environment
CMD ["poetry", "run", "panoptikon"]
