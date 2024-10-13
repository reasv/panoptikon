# Use an NVIDIA CUDA base image with Debian
FROM nvidia/cuda:12.2.0-runtime-ubuntu22.04

# Install necessary dependencies for Python 3.12, SQLite, and build tools
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
    libsqlite3-dev \
    git \
    make \
    gcc \
    && apt-get clean

# Install Python 3.12 from source
RUN PYTHON_VERSION=3.12.0 && \
    wget https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz && \
    tar -xzf Python-$PYTHON_VERSION.tgz && \
    cd Python-$PYTHON_VERSION && \
    ./configure --enable-optimizations && \
    make -j$(nproc) && \
    make altinstall && \
    [ -e /usr/bin/python3 ] && rm /usr/bin/python3 || true && \
    ln -s /usr/local/bin/python3.12 /usr/bin/python3 && \
    [ -e /usr/bin/pip3 ] && rm /usr/bin/pip3 || true && \
    ln -s /usr/local/bin/pip3.12 /usr/bin/pip3 && \
    cd .. && \
    rm -rf Python-$PYTHON_VERSION* 

# Install the latest SQLite 3.46.1 from source
RUN SQLITE_VERSION=3460100 && \
    wget https://www.sqlite.org/2023/sqlite-autoconf-${SQLITE_VERSION}.tar.gz && \
    tar -xzf sqlite-autoconf-${SQLITE_VERSION}.tar.gz && \
    cd sqlite-autoconf-${SQLITE_VERSION} && \
    ./configure --prefix=/usr/local && \
    make && make install && \
    ldconfig && \
    cd .. && rm -rf sqlite-autoconf-${SQLITE_VERSION}*

# Upgrade pip and install Poetry
RUN pip3 install --upgrade pip && \
    pip3 install poetry

# Create a directory for the application and add a non-root user
RUN mkdir /app && adduser --disabled-password --gecos '' appuser && chown -R appuser /app

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Install dependencies using Poetry
RUN poetry config virtualenvs.create false && poetry install --with inference

# Expose the port for the application
EXPOSE 6342

# Set environment variables
ENV HOST=0.0.0.0
ENV PORT=6342
ENV DATA_FOLDER=data
ENV LOGLEVEL=INFO

# Switch to the app user
USER appuser

# Run the application
CMD ["poetry", "run", "panoptikon"]
