# Nix contrib redesign Implementation Plan

> **Status: implemented.** Live docs: [`contrib/package/nix/README.md`](../contrib/package/nix/README.md).  
> Spec: [`nix-packaging-design.md`](./nix-packaging-design.md).

**Goal:** Packaging under `contrib/`, C2 GPU model, UI pin for `github:` consumers, desktop menu/icons, tests.

**Architecture:** Thin flake; overlay-first packages; version from Cargo.toml; UI via `ui-pin.json` + `importNpmLock` (not monorepo-only, not `inputs.ui`).

## Global constraints (as shipped)

- No `gpu` auto.
- `panoptikon` follows nixpkgs config; `-cpu`/`-cuda`/`-rocm` always force.
- Module: `gpu = null` → `pkgs.panoptikon`; `gpu` set → forced package.
- UI: `ui-pin.json` (`rev` + NAR `hash`); npm via `importNpmLock`; monorepo `ui/` when present in flake source.
- Package version tracks `panoptikon/Cargo.toml`.
- Desktop: install pre-generated `contrib/package/common/share/icons` + `panoptikon-desktop.desktop`.
- VM tests with `autoSetup = false`.

## File map (as shipped)

| Path | Role |
| --- | --- |
| `flake.nix` | Thin wiring |
| `contrib/package/nix/overlay.nix` | Package attrs |
| `contrib/package/nix/panoptikon/` | Server, wrap, ui.nix, **ui-pin.json** |
| `contrib/package/nix/panoptikon-desktop/` | Desktop (version ← panoptikon.version) |
| `contrib/package/nix/rocm-packages.nix` | HIP list |
| `contrib/package/nix/shells.nix` | Dev shells |
| `contrib/package/nix/README.md` | Docs |
| `contrib/nixos/modules/panoptikon.nix` | Service |
| `contrib/nixos/tests/panoptikon.nix` | Parameterized VMs |
| `contrib/package/common/` | Cross-package install payloads (`share/`, …) |
| `scripts/sync-nix-ui-pin.py` | Pin sync / check (pure Python NAR; no nix) |
| `scripts/generate-hicolor-icons.sh` | Regenerate icons from logo SVG |
| `.github/workflows/nix.yml` | CI matrix + weekly lock/pin PR |

## Test matrix (as shipped)

### Package checks (no VM; wrap/layout greps)

| Check | Asserts |
| --- | --- |
| `panoptikon-cli` | `--version`, `--help`, setup flags |
| `panoptikon-install` | share seeds, UV_PYTHON, CPU wrap when config clean |
| `panoptikon-cpu-install` | forced CPU wrap |
| `panoptikon-cuda-install` | ACCELERATOR=cuda, opengl-driver, no /opt/rocm |
| `panoptikon-rocm-install` | ACCELERATOR=rocm, /opt/rocm, current-system HIP |
| `panoptikon-desktop-install` | tray, PATH panoptikon, sidecar, `.desktop`, icons |
| `panoptikon-desktop-cpu/cuda/rocm-install` | sidecar matches forced server variant |

### NixOS VM checks (`autoSetup = false`)

| Check | Config | Asserts |
| --- | --- | --- |
| `panoptikon-nixos` | `gpu = null` | CPU env, seeds, client-config HTTP |
| `panoptikon-nixos-gpu-cpu` | `gpu = "cpu"` | forced cpu package wiring |
| `panoptikon-nixos-gpu-rocm` | `gpu = "rocm"` | KFD, HIP env, rocm wrap |
| `panoptikon-nixos-gpu-cuda` | `gpu = "cuda"` | NVIDIA devices, cuda wrap |

## Tasks (completed)

1. Scaffold contrib + overlay + wrap + ui + server package.
2. Desktop package + overlay matrix (`-cpu`/`-cuda`/`-rocm`).
3. Module with `gpu` + package defaults + asserts.
4. Parameterized NixOS tests + package passthru tests.
5. Thin flake, shells, README; alejandra + CI.
6. UI pin + sync/check (no flake `inputs.ui`).
7. Desktop icons + `.desktop` entry.
