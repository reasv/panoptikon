# Nix packaging redesign (contrib layout, C2 GPU model)

Date: 2026-07-27  
Status: **implemented** — live contract: [`contrib/package/nix/README.md`](../contrib/package/nix/README.md)  
Inspired by: [copyparty](https://github.com/9001/copyparty) (`contrib/package/nix`, thin flake, overlay-first).

## Goals

1. Move packaging into a copyparty-style tree (`contrib/package/nix`, `contrib/nixos`).
2. Thin root `flake.nix` (wire outputs only).
3. GPU package matrix + module `services.panoptikon.gpu` (C2).
4. UI pin that works for `github:` flake consumers (submodule files are not in the store).
5. Package version tracks `panoptikon/Cargo.toml` (no hardcoded version default).
6. Desktop: FreeDesktop `.desktop` + pre-generated hicolor icons under `contrib/package/common/share/`.

## Non-goals

- Prebuilding the managed Python/torch venv in Nix.
- AppImage / signed Desktop installers.
- Full GPU e2e in CI.
- `gpu = "auto"`.
- FlakeHub binary cache (repo-scoped; not useful when packaging lands upstream).
- Submitting to nixpkgs in the initial change (structure remains submittable).

---

## Layout

```
flake.nix / flake.lock
contrib/
  package/nix/
    overlay.nix
    panoptikon/
      default.nix          # server; version from Cargo.toml; UI from pin or monorepo
      ui.nix
      ui-pin.json          # rev + NAR hash (synced from ui submodule)
      wrap.nix
      patch-ui-offline-font.mjs
    panoptikon-desktop/
      default.nix          # version ← panoptikon.version; .desktop + icons
    rocm-packages.nix
    shells.nix
    README.md              # authoritative packaging docs
  common/                  # cross-package install payloads (share/, …)
  nixos/
    modules/panoptikon.nix
    tests/panoptikon.nix   # parameterized by gpu
config/server/nixos.toml
scripts/sync-nix-ui-pin.py
scripts/generate-hicolor-icons.sh
scripts/generate-nix-dev-config.py
.github/workflows/nix.yml         # manual dispatch: verify matrix, lock/pin PR
.github/workflows/nix-verify.yml  # reusable verify matrix (dispatch + release tag)
```

---

## UI source (pin, not flake input)

**Constraint:** Flake sources from `github:` do not include git submodule **file trees**. Flake input URLs must be plain strings (cannot `readFile` a pin into `inputs.ui.url`).

**Design:**

| Source of truth | Downstream |
| --- | --- |
| Monorepo `ui` submodule gitlink | `scripts/sync-nix-ui-pin.py` |
| `contrib/package/nix/panoptikon/ui-pin.json` | package `fetchFromGitHub` (`rev` + NAR `hash`) |
| UI `package-lock.json` | `importNpmLock` (no `npmDepsHash` in pin) |

Package resolution order for UI:

1. Explicit `uiSrc` argument (flake passes monorepo `ui/` when `package.json` exists in the flake source).
2. Else monorepo `src + "/ui"` if present.
3. Else `fetchFromGitHub` using `ui-pin.json` (`rev` / `hash`).

Sync refreshes `rev` + pure-Python NAR `hash` (no nix CLI). `--check` verifies both against the submodule rev and rejects stray pin keys. There are no git hooks: the sync runs once before tagging, and the release workflow's non-blocking `nix-pin-check` job runs `--check --ref HEAD` on the tag.

There is **no** `inputs.ui` on the flake. The **`ui` git submodule is never auto-bumped** by CI.

---

## Package GPU matrix

| Attr | `cudaSupport` | `rocmSupport` | Relation to `nixpkgs.config` |
| --- | --- | --- | --- |
| **`panoptikon`** | config or false | config or false | **Follows** config |
| **`panoptikon-cpu`** | false | false | **Always** CPU |
| **`panoptikon-cuda`** | true | false | **Always** CUDA |
| **`panoptikon-rocm`** | false | true | **Always** ROCm |

Same matrix for desktop; default desktop sidecar is `panoptikon` (config-follow). Forced variants pin the matching server package.

Both config GPU flags true → assert on default packages only.

**Version:** derived from `panoptikon/Cargo.toml` inside `src` when `version` is not passed; desktop defaults to `panoptikon.version`.

---

## Module: `services.panoptikon` (C2)

- `gpu` default **`null`** → `package` default = `pkgs.panoptikon`.
- `gpu = "cpu"|"cuda"|"rocm"` → forced `panoptikon-*` package + env/devices/HIP.
- Both nixpkgs GPU flags only illegal when `gpu` is unset.
- No `accelerator` dual-control option; no silent `package.override` for GPU when `gpu` is null.

---

## Desktop menu entry and icons

- Icons are **generated in-repo** (`./scripts/generate-hicolor-icons.sh` from `static/logo_simple.svg`) and committed under `contrib/package/common/share/icons/hicolor/` (shared by all packaging schemes).
- The Nix package **installs** them into `$out/share/icons/hicolor/…` and a `makeDesktopItem` entry at `$out/share/applications/panoptikon-desktop.desktop`.
- Runtime XDG autostart (`~/.config/autostart/`) remains app-generated when the user enables start-at-login; it is not this menu entry.

---

## Formatting and automation

- **Formatter:** alejandra only via `nix fmt` / flake `formatter` (not a host binary).
- **Pin maintenance is release-time only** (no git hooks, no per-push pin
  check): only tagged releases are installable, so `sync-nix-ui-pin.py` runs
  once before tagging; the pin may drift between releases harmlessly.
- **GitHub Actions** (`.github/workflows/nix.yml`, verify matrix in reusable `nix-verify.yml`):
  - No push, pull-request, or scheduled triggers
  - Plain dispatch: alejandra + **packages** matrix (install/cli) + **nixos** matrix (VMs)
  - Release tag: `release.yml` calls `nix-verify.yml` (read-only, non-gating) on the tagged tree
  - Dispatch with `update`: `nix flake update`, pin sync (to **current** gitlink only), `nix fmt`, **pre-PR light smokes** (pin/alejandra/cli/install), open PR (`GITHUB_TOKEN` does not re-trigger matrix)
  - `cache.nixos.org` only (no Magic Nix Cache); `nix-installer-action@v22`, `checkout@v6`

## Tests

- Package install/cli smokes for CPU and forced GPU wraps.
- Desktop install smokes (sidecar flags, `.desktop`, hicolor icons).
- Parameterized NixOS VMs: `gpu = null` / `cpu` / `rocm` / `cuda` (`autoSetup = false`).
- Flake check: `alejandra`.

---

## Success criteria

1. No top-level `nix/`; contrib layout as above.
2. `github:` consumers build without submodule contents (UI via pin).
3. Package version tracks Cargo.toml as the repo version changes.
4. GPU C2 package/module rules as specified.
5. Pin aligned with the submodule at release tags via the sync script.
6. Desktop package exposes a menu entry and hicolor icons.
