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
FROM node:22-bookworm-slim AS ui-build
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
FROM rust:1-bookworm AS rust-build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY panoptikon/ panoptikon/
COPY python/ python/
COPY config/ config/
COPY --from=ui-build /ui/.next/standalone /ui-bundle
ENV PANOPTIKON_UI_BUNDLE=/ui-bundle
RUN cargo build --release -p panoptikon --features bundled,bundled-ui

# ---- Runtime: native Node.js + the managed Python venv ----
FROM node:22-bookworm-slim
# libgl1/libglib2.0-0/libsm6/libxext6/libxrender1: OpenCV (EasyOCR) runtime
# libraries. curl: the container healthcheck.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgl1 libglib2.0-0 libsm6 libxext6 libxrender1 curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

LABEL org.opencontainers.image.source="https://github.com/reasv/panoptikon" \
      org.opencontainers.image.description="Panoptikon local multimodal media search engine" \
      org.opencontainers.image.licenses="AGPL-3.0-or-later"

COPY --from=rust-build /src/target/release/panoptikon /usr/local/bin/panoptikon

WORKDIR /app
COPY config/gateway/docker.toml config/gateway/docker.toml
COPY config/inference/example.toml config/inference/example.toml
# /app/data must exist owned by the runtime user: a named volume mounted
# there inherits this ownership (Docker creates missing mountpoints as root).
RUN mkdir -p data && chown -R node:node /app
USER node
ENV GATEWAY_CONFIG_PATH=/app/config/gateway/docker.toml

# Provision the Python inference environment at build time so first boot is
# fast: downloads uv, extracts the embedded Python source set to
# /app/runtime, creates the venv, and prefetches ffmpeg/ffprobe
# (static-ffmpeg). ACCELERATOR: cpu (default) or cuda (CUDA 12.8 wheels —
# run the container with --gpus all). The uv wheel cache is dropped in the
# same layer (the venv keeps its own copies).
ARG ACCELERATOR=cpu
RUN panoptikon setup --accelerator ${ACCELERATOR} \
    && rm -rf /home/node/.cache/uv && mkdir -p /home/node/.cache

# 6342 private admin, 6339 public restricted (see config/gateway/docker.toml).
EXPOSE 6342 6339
HEALTHCHECK --interval=30s --timeout=5s --start-period=120s \
    CMD curl -fsS http://127.0.0.1:6342/api/client-config > /dev/null || exit 1
ENTRYPOINT ["panoptikon"]
