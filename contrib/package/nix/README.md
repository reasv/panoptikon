# Nix packaging for Panoptikon

Layout follows [copyparty](https://github.com/9001/copyparty): packages under
`contrib/package/nix/`, NixOS module under `contrib/nixos/`, thin root
`flake.nix`. Spec: [`docs/nix-packaging-design.md`](../../../docs/nix-packaging-design.md).

## Runtime contract

| Path | Meaning |
| --- | --- |
| `--root <dir>` | chdir base (required outside a checkout) |
| `<root>/config/` | server + inference TOML (module seeds once) |
| `<root>/runtime/{pysrc,ui,venv}/` | embedded Python, UI, managed uv venv |
| `<root>/data/` | DBs / logs |

| Need | Source |
| --- | --- |
| `node` / `ffmpeg` / `uv` / `chromium` / `fc-match` / `python3.12` | package PATH wrap |
| Setup interpreter | `UV_PYTHON` + `UV_PYTHON_DOWNLOADS=never` |
| Label fonts | `FONTCONFIG_FILE` (DejaVu) |
| UI source | `ui-pin.json` → `fetchFromGitHub`; npm via `importNpmLock` (lockfile) |
| Package version | `panoptikon/Cargo.toml` in `src` (no hardcoded default) |
| Default packages | `nixpkgs.config.cudaSupport` / `rocmSupport` (not both) |
| Forced packages | `panoptikon-cpu` / `-cuda` / `-rocm` (ignore config) |
| Service GPU | `services.panoptikon.gpu` (`null` = follow config) |
| Accelerator report | `panoptikon accelerator` (backend always; GPU names optional) |

Do not put `/nix/store/...` tool paths into TOML.

## Packages

```bash
nix build .#panoptikon              # follows nixpkgs GPU config (default CPU)
nix build .#panoptikon-cpu          # always CPU
nix build .#panoptikon-cuda         # always CUDA wrap
nix build .#panoptikon-rocm         # always ROCm wrap
nix build .#panoptikon-desktop      # sidecar = panoptikon (config-follow)
nix build .#panoptikon-desktop-cpu
nix build .#panoptikon-desktop-cuda
nix build .#panoptikon-desktop-rocm
nix develop
```

### Formatting

All `*.nix` files use **[alejandra](https://github.com/kamadorueda/alejandra)** via the
flake **`formatter`** only — always go through nix, not a host `alejandra` binary:

```bash
nix fmt                     # format (flake formatter = alejandra)
nix build .#checks.<system>.alejandra   # CI check
```

### Automated maintenance (GitHub Actions)

Workflow [`.github/workflows/nix.yml`](../../../.github/workflows/nix.yml):

| Trigger | Action |
| --- | --- |
| plain `workflow_dispatch` | flake alejandra format check plus the full package/desktop/NixOS VM smoke matrix |
| `workflow_dispatch` with `update` | `nix flake update`, pin sync, `nix fmt`, **pre-PR smokes** (pin `--check`, alejandra, cli, install), then open PR |

The workflow is **manual dispatch only**: no push, pull-request, or scheduled
runs, by policy. Only tagged releases are installable, so the UI pin and
flake.lock must be correct **at release tags only** (see the release checklist
below). Between releases they may go stale harmlessly — master is not an
installable source. Packaging breakage from core changes surfaces via a manual
dispatch (run one before cutting a release).

CI uses **`cache.nixos.org` only** (no Magic Nix Cache / GHA cache proxy — those
hit rate limits on the full package matrix). Cold matrix builds rebuild the UI
per job (no shared GHA binary cache); first-run wall time can be long. Installer
is pinned (`nix-installer-action@v22`). Package/VM checks run as a **parallel
matrix**.

**Update PR:** nothing runs automatically on the opened PR (no `pull_request`
trigger). The update job therefore runs pin + light package smokes **before**
opening the PR. Dispatch the workflow manually on the PR branch if you need the
full package/VM matrix on the PR tip.

**Never auto-updates the `ui` git submodule** — only maintainers change that
pointer. Automation only rewrites `ui-pin.json` / `flake.lock` to match what is
already recorded.

### Flake consumers (you do **not** need a PR for normal builds)

If your flake only **consumes** panoptikon, **pin a release tag**:

```nix
# Only tagged releases are supported for installation. master is the
# development branch: it is NOT stable, and running it can leave your
# databases in a state the next release cannot migrate cleanly.
inputs.panoptikon.url = "github:reasv/panoptikon/v0.1.8"; # first tag with nix packaging
```

Update by bumping the tag, not by re-locking master. Then:

| Upstream change | What you do |
| --- | --- |
| Server / module / wrap only | `nix flake update` (or rebuild) — **no PR** |
| UI submodule + pin committed correctly | same — pin is **in the tree** you fetch |
| UI `package-lock.json` only (same rev) | **no pin edit** (`importNpmLock`) — rebuild pulls lock from UI source |
| Broken pin (gitlink ≠ pin) | wait for upstream fix, or open a pin-only PR |

`github:` flakes **do not** include monorepo submodule file trees, so the package
uses `ui-pin.json` → `fetchFromGitHub` for the UI. As long as reasv (or your
tracking branch) ships a matching pin, your flake builds without you touching
packaging.

### Release checklist (the only recurring pin maintenance)

The UI pin is **release-time state**. Day-to-day `ui` submodule bumps need no
pin action at all; the pin may drift freely between releases because master
is not an installable source. Once, **before tagging a release**:

```bash
python3 scripts/sync-nix-ui-pin.py          # refresh rev + hash from the ui gitlink
python3 scripts/sync-nix-ui-pin.py --check  # verify
git add contrib/package/nix/panoptikon/ui-pin.json
git commit -m "Sync nix UI pin for release"
```

or dispatch the `nix` workflow with `update` ticked and merge the PR it opens
(that also refreshes `flake.lock`). Then tag. A tag with a stale pin ships a
nix package whose UI does not match the release — that is the only failure
mode this guards.

There is **no npmDepsHash** in the pin (npm → `importNpmLock` on the UI
lockfile). The pin is only `{ rev, hash }` for `fetchFromGitHub`.

### UI pin (submodule → `ui-pin.json`)

Nix flake **input URLs must be plain strings** (no `readFile` of a pin). The
`github:` scheme also cannot fetch monorepo submodules. So the UI is **not** a
flake input; the package pins source in one place:

`contrib/package/nix/panoptikon/ui-pin.json` → `fetchFromGitHub` (`rev` + NAR `hash`)

npm dependencies use **`importNpmLock`** on the UI’s `package-lock.json` (no
`npmDepsHash` in the pin — integrity hashes live in the lockfile).

| Field | Meaning | Needs nix to compute? |
| --- | --- | --- |
| `rev` | panoptikon-ui git commit (from monorepo `ui` gitlink) | no |
| `hash` | NAR SRI of that tree (must match `fetchFromGitHub`) | no (pure Python) |

**Maintainer order:** push the UI commit to `panoptikon-ui` first, then point
the monorepo `ui` gitlink at that **already-public** rev and sync the pin.
`sync-nix-ui-pin.py` hashes the **GitHub archive** (same as `fetchFromGitHub` /
CI). Offline `git archive` is only allowed with `--allow-offline-hash` (do not
push offline-only pins; they can diverge from GitHub).

**Source of truth for the pin:** the monorepo `ui` submodule gitlink
(`git rev-parse HEAD:ui` / staged `:ui`). **Submodule bumps are always manual**
and do not require touching the pin — sync happens once per release (see the
release checklist above).

Do **not** run `git submodule update --remote` from automation or CI.

## NixOS module

```nix
{
  imports = [ inputs.panoptikon.nixosModules.default ];
  nixpkgs.overlays = [ inputs.panoptikon.overlays.default ];
  # CUDA packages need unfree (flake sets allowUnfree; overlay-only consumers must).
  # nixpkgs.config.allowUnfree = true;
  # optional: nixpkgs.config.rocmSupport = true;  # default package becomes ROCm

  services.panoptikon = {
    enable = true;
    host = "127.0.0.1";
    port = 6342;
    # gpu = "rocm";  # force panoptikon-rocm + devices; overrides nixpkgs config
    libraryPaths = [ "/mnt/media" ];
  };
}
```

| `gpu` | Default package |
| --- | --- |
| `null` | `pkgs.panoptikon` (follows config) |
| `"cpu"` / `"cuda"` / `"rocm"` | `pkgs.panoptikon-{cpu,cuda,rocm}` |

Both `cudaSupport` and `rocmSupport` in nixpkgs config is an error only when
`gpu` is unset. Set `gpu` to force a backend. **ROCm** package attrs, shells,
and `gpu = "rocm"` are **x86_64-linux only**.

When `package` is overridden without `gpu`, devices/env follow the **package**
passthru flags (a CPU package stays CPU even if nixpkgs has `cudaSupport`).

### Module options (load-bearing)

| Option | Default | Notes |
| --- | --- | --- |
| `autoSetup` | `true` | preStart `setup --if-needed` (long `TimeoutStartSec`) |
| `setupMustSucceed` | `false` | `false`: soft-fail preStart, skip immediate in-process re-setup, start HTTP; `true`: unit fails until setup works |
| `openFirewall` | `false` | opens TCP `port`; warn if host is non-loopback |
| `rocmOverrideGfx` | `null` | sets `HSA_OVERRIDE_GFX_VERSION` when backend is ROCm |
| `libraryPaths` | `[]` | optional `ReadOnlyPaths` with leading `-` (missing path does not fail start) |
| `host` / `port` | loopback / 6342 | empty host is **not** treated as loopback for firewall warnings |

**Host GPU libraries:** CUDA wrap expects nixpkgs NVIDIA / `/run/opengl-driver`;
ROCm wrap also needs HIP runtime on the host (`environment.systemPackages` via
the module when `gpu`/`effective` is ROCm).

## Packaging purity (local flake builds)

`flake.nix` filters `self` with **root-relative** prefixes (`target/`, `data/`,
`ui/node_modules`, …) and nested `node_modules` / `.venv` / `.next`. Builds still
walk the **worktree** filesystem (not `git ls-files`), so untracked junk outside
the deny list can enter a dirty local evaluation. Keep large artifacts under
ignored/filtered paths.

Server and desktop packages both install `$out/share/panoptikon/` config seeds
(`nixos.toml`, inference example).

## Manual run

```bash
nix build .#panoptikon
ROOT=$(mktemp -d)
mkdir -p "$ROOT"/{config/server,config/inference,data,runtime}
cp result/share/panoptikon/nixos.toml "$ROOT/config/server/default.toml"
./result/bin/panoptikon --root "$ROOT" \
  --config "$ROOT/config/server/default.toml" \
  --disable-update-check
```

## Tests

Package install/cli smokes (no network after build; GPU variants grep wraps):

```bash
nix build .#checks.x86_64-linux.panoptikon-cli
nix build .#checks.x86_64-linux.panoptikon-install
nix build .#checks.x86_64-linux.panoptikon-cpu-install
nix build .#checks.x86_64-linux.panoptikon-cuda-install
nix build .#checks.x86_64-linux.panoptikon-rocm-install
nix build .#checks.x86_64-linux.panoptikon-desktop-install
nix build .#checks.x86_64-linux.panoptikon-desktop-cpu-install
nix build .#checks.x86_64-linux.panoptikon-desktop-cuda-install
nix build .#checks.x86_64-linux.panoptikon-desktop-rocm-install
```

NixOS VMs (`autoSetup = false`; no real GPU):

```bash
nix build .#checks.x86_64-linux.panoptikon-nixos           # gpu=null → CPU
nix build .#checks.x86_64-linux.panoptikon-nixos-gpu-cpu
nix build .#checks.x86_64-linux.panoptikon-nixos-gpu-rocm
nix build .#checks.x86_64-linux.panoptikon-nixos-gpu-cuda
```

## Submitting to nixpkgs

1. `contrib/package/nix/panoptikon` → `pkgs/by-name/pa/panoptikon/`
2. `contrib/nixos/modules/panoptikon.nix` → `nixos/modules/services/web-apps/panoptikon.nix`
3. Tests → `nixos/tests/panoptikon.nix`
4. `src = fetchFromGitHub { fetchSubmodules = true; … }`
5. UI `package-lock.json` changes need no pin edit (`importNpmLock` reads the lock)

## Desktop icons and menu entry

Cross-package install payloads live under **`contrib/package/common/`**
(packaging-scheme agnostic; `share/` is the FHS fragment). Icons are generated
from `static/logo_simple.svg` via `./scripts/generate-hicolor-icons.sh`. The
Nix package only installs them; it does not re-render at build time.

`panoptikon-desktop` installs:

| Path | Content |
| --- | --- |
| `share/applications/panoptikon-desktop.desktop` | App menu entry (`Exec=panoptikon-desktop`, `Icon=panoptikon-desktop`) |
| `share/icons/hicolor/{16…512}x…/apps/panoptikon-desktop.png` | From `contrib/package/common/share/icons/hicolor` |
| `share/icons/hicolor/scalable/apps/panoptikon-desktop.svg` | From `contrib/package/common/share/icons/hicolor` |

This is separate from the runtime XDG autostart entry the app can write under
`~/.config/autostart/` when “start at login” is enabled.

## Gaps

- Inference lock is not Linux aarch64-complete.
- UI offline build still patches fonts until panoptikon-ui vendors Inter.
- Desktop is binary-only (no AppImage).
