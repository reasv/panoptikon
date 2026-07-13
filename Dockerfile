# Panoptikon container image: ONE process (the Rust gateway) with TWO
# listeners — a private admin listener (6342, allow-all policy) and a public
# listener (6339, restricted ruleset). This replaces the Python-era
# nginx + two-UI-service compose stack. Distributable compose file:
# deploy/docker-compose.yml.
#
# Build (from a checkout with submodules):  docker build -t panoptikon .
# CUDA variant:  docker build --build-arg ACCELERATOR=cuda -t panoptikon:cuda .
#
# linux/amd64 only: the Python inference lockfile excludes linux/aarch64
# (torch's pinned triton publishes no aarch64 wheels).

# ---- UI build: Next.js standalone bundle, embedded into the binary ----
FROM node:24-trixie-slim AS ui-build
WORKDIR /ui
COPY ui/ ./
ENV BUILD_STANDALONE=true
# Next.js does not copy static assets into the standalone output; the bundle
# is assembled by hand per docs/architecture.md.
RUN npm ci \
    && npm run build \
    && cp -r .next/static .next/standalone/.next/static \
    && if [ -d public ]; then cp -r public .next/standalone/public; fi \
    && test -f .next/standalone/server.js

# ---- Rust build: bundled binary (embeds the Python source set + UI) ----
# Built on the same Ubuntu release as the runtime stage so the binary never
# links glibc symbols newer than what the runtime provides.
FROM ubuntu:24.04 AS rust-build
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential pkg-config libssl-dev curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /src
# .cargo/config.toml sets LIBSQLITE3_FLAGS so the bundled SQLite includes
# the math functions PQL's POW() expressions need.
COPY .cargo/ .cargo/
COPY Cargo.toml Cargo.lock ./
COPY panoptikon/ panoptikon/
COPY python/ python/
COPY config/ config/
COPY --from=ui-build /ui/.next/standalone /ui-bundle
ENV PANOPTIKON_UI_BUNDLE=/ui-bundle
RUN cargo build --release -p panoptikon --features bundled,bundled-ui

# ---- Runtime: Ubuntu LTS + native Node.js + the managed Python venv ----
FROM ubuntu:24.04
# ffmpeg/ffprobe come from apt (wired via [jobs] in docker.toml) instead of
# the venv's pip static-ffmpeg. libgl1/libglib2.0-0t64/libsm6/libxext6/
# libxrender1: OpenCV (EasyOCR) runtime libraries. curl: the healthcheck.
# Node.js 24 (the UI server) via the NodeSource repo.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg libgl1 libglib2.0-0t64 libsm6 libxext6 libxrender1 \
        curl ca-certificates \
    && curl -fsSL https://deb.nodesource.com/setup_24.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Headless Chrome for HTML thumbnails/extraction. google-chrome-stable lands
# at /usr/bin/google-chrome-stable, which the renderer's built-in search order
# finds (files.rs). It runs with --no-sandbox/--disable-dev-shm-usage inside
# the container (config/server/docker.toml [jobs] html_renderer_args), which
# Chromium requires as a non-root user under Docker's default seccomp.
RUN curl -fsSL https://dl.google.com/linux/linux_signing_key.pub \
        -o /usr/share/keyrings/google-chrome.asc \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.asc] https://dl.google.com/linux/chrome/deb/ stable main" \
        > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# The exact uv release `panoptikon setup` pins (setup.rs UV_VERSION): found
# on PATH, it is used as-is and setup skips its own download.
COPY --from=ghcr.io/astral-sh/uv:0.11.28 /uv /usr/local/bin/uv

LABEL org.opencontainers.image.source="https://github.com/reasv/panoptikon" \
      org.opencontainers.image.description="Panoptikon local multimodal media search engine" \
      org.opencontainers.image.licenses="AGPL-3.0-or-later"

COPY --from=rust-build /src/target/release/panoptikon /usr/local/bin/panoptikon

WORKDIR /app
COPY config/server/docker.toml config/server/docker.toml
COPY config/inference/example.toml config/inference/example.toml
# /app/data must exist owned by the runtime user (ubuntu:24.04's built-in
# uid-1000 `ubuntu` user): a named volume mounted there inherits this
# ownership (Docker creates missing mountpoints as root).
RUN mkdir -p data && chown -R ubuntu:ubuntu /app
USER ubuntu
ENV PANOPTIKON_CONFIG_PATH=/app/config/server/docker.toml

# Provision the Python inference environment at build time so first boot is
# fast: extracts the embedded Python source set to /app/runtime and creates
# the venv with the PATH-installed uv. ACCELERATOR: cpu (default) or cuda
# (CUDA 12.8 wheels — run the container with --gpus all). Dropped in the
# same layer: the uv wheel cache (the venv keeps its own copies) and the
# ffmpeg/ffprobe binaries setup's static-ffmpeg prefetch downloads — the
# image wires the apt ffmpeg via [jobs] in docker.toml instead.
ARG ACCELERATOR=cpu
RUN panoptikon setup --accelerator ${ACCELERATOR} \
    && cp /app/runtime/venv/lib/python*/site-packages/pypdfium2_raw/libpdfium.so \
          /app/libpdfium.so \
    && rm -rf /home/ubuntu/.cache/uv \
    && rm -rf /app/runtime/venv/lib/python*/site-packages/static_ffmpeg/bin \
    && mkdir -p /home/ubuntu/.cache

# 6342 private admin, 6339 public restricted (see config/server/docker.toml).
EXPOSE 6342 6339
HEALTHCHECK --interval=30s --timeout=5s --start-period=120s \
    CMD curl -fsS http://127.0.0.1:6342/api/client-config > /dev/null || exit 1
ENTRYPOINT ["panoptikon"]
