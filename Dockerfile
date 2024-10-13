# Use the Alpine-based SQLite image as the base
FROM keinos/sqlite3:3.46.1

# Install Python 3.12 and dependencies
RUN apk update && \
    apk add --no-cache \
    python3=3.12.0-r0 \
    py3-pip \
    py3-setuptools \
    py3-wheel \
    gcc \
    musl-dev \
    libffi-dev \
    openssl-dev \
    git

# Ensure Python3.12 is the default Python and pip commands
RUN ln -sf python3 /usr/bin/python && \
    ln -sf pip3 /usr/bin/pip

# Install Poetry
RUN pip install --upgrade pip && \
    pip install poetry

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Install the dependencies using Poetry
RUN poetry config virtualenvs.create false && poetry install --with inference

# Expose the port the app runs on
EXPOSE 6342

# Set environment variables
ENV HOST=0.0.0.0
ENV PORT=6342
ENV DATA_FOLDER=data
ENV LOGLEVEL=INFO

# Run the application
CMD ["poetry", "run", "panoptikon"]
