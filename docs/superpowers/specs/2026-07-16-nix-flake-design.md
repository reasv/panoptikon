# Nix flake design (historical)

Date: 2026-07-16  
**Status: superseded** by [`2026-07-27-nix-contrib-redesign.md`](./2026-07-27-nix-contrib-redesign.md)
and [`contrib/package/nix/README.md`](../../../contrib/package/nix/README.md).

This file began as **dev shells only**. Packaging later lived briefly under a
top-level `nix/` tree before moving to `contrib/`. Keep this note for history;
do not implement from it.

## Original goal (still valid for shells)

Repository-root Nix flake with multi-accelerator **development shells**
(`cpu` / `cuda` / `rocm`) for the Rust server + Python workers + UI tooling.

## What shipped instead (see 2026-07-27)

| Output | Role |
| --- | --- |
| `devShells.default` / `cpu` / `cuda` / `rocm` | Dev shells (`nix develop`) |
| `packages.panoptikon` (+ `-cpu`/`-cuda`/`-rocm`) | Bundled server; version from Cargo.toml |
| `packages.panoptikon-desktop` (+ GPU variants) | Tauri tray + server sidecar (Linux) |
| `nixosModules.default` | `services.panoptikon` (`gpu` option) |
| `checks.*` | Package/desktop smokes + NixOS VMs + alejandra |

**nixpkgs pin:** `nixos-unstable`.

**Runtime contract:** always `--root <writable>`; host tools via PATH /
fontconfig; `UV_PYTHON` + `UV_PYTHON_DOWNLOADS=never` on the package wrap.

## Shared shell packages (unchanged intent)

- `rustc`, `cargo`, `rustfmt`, `clippy`, `pkg-config`, OpenSSL, `alejandra`
- `nodejs_24`, `uv`, `git`, `ffmpeg`, `python312`, `fontconfig`
- Linux: GL/X11 libs, chromium, fonts, WebKitGTK stack for local desktop builds
- CUDA / ROCm extras on respective shells

Shell hook: `UV_PYTHON`, `LD_LIBRARY_PATH`, optional opengl-driver, generate
`config/server/nix-dev.toml` (bare tool names), print next steps. Does **not**
auto-run setup/cargo/npm.

## Systems

- Packages / shells / checks: `x86_64-linux`, `aarch64-linux` only (server wrap and desktop need Linux libs; ROCm shells are x86_64)

## Explicit gaps (still current)

- Inference lock not fully aarch64-linux complete
- UI offline build still patches Inter until panoptikon-ui vendors fonts
- Desktop is a native binary + sidecar, not AppImage/NSIS
