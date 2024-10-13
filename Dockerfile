# Use the Alpine-based SQLite image as the base
FROM keinos/sqlite3:3.46.1

# Temporarily switch to root for package installation
USER root

# Install Python 3.12 and dependencies
RUN apk update && \
    apk add --no-cache \
    python3=3.12.7-r0 \
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

# Create a directory for the virtual environment
ENV VENV_PATH="/opt/poetry_venv"
RUN python3 -m venv $VENV_PATH

# Activate the virtual environment and install Poetry within it
RUN . $VENV_PATH/bin/activate && pip install --upgrade pip && pip install poetry

# Set up environment variables to use Poetry from the virtual environment
ENV PATH="$VENV_PATH/bin:$PATH"

# Create a user with UID 1000 and set permissions
RUN adduser -D -u 1000 appuser && chown -R appuser /app

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Change ownership of app directory to the new user
RUN chown -R appuser /app

# Install dependencies using Poetry
RUN poetry config virtualenvs.create false && poetry install --with inference

# Expose the port the app runs on
EXPOSE 6342

# Set environment variables
ENV HOST=0.0.0.0
ENV PORT=6342
ENV DATA_FOLDER=data
ENV LOGLEVEL=INFO

# Switch to the app user with UID 1000
USER appuser

# Run the application
CMD ["poetry", "run", "panoptikon"]