# Managed ffmpeg provisioning

Status: designed 2026-08-10, not implemented. Companion to
docs/video-transcoding-design.md (§5 amendment).

## 0. Problem

The transcoder's capability set is currently decided by whichever ffmpeg the
resolver happens to find, and for the default bare-binary and Desktop paths
that is a third party's packaging choice. The first production AVIF export
failed with `Unknown encoder 'libsvtav1'`: the venv's `static-ffmpeg` pypi
package downloads platform zips from `zackees/ffmpeg_bins`, and its win32 zip
is gyan.dev's **"essentials"** build — no SVT-AV1, no guarantee about the next
version either. We do not pin those zips, we do not checksum them, and we do
not choose what is in them.

The `av1` encoder ladder (`hw::av1_software_encoder`, SVT-AV1 → libaom) keeps
the feature working on such builds and STAYS as a safety net — same layering
rule as config migrations vs load-time aliases — but it is not the fix. The
fix is that every deployment channel we control ships or fetches an ffmpeg
**we** pinned, with the full capability set the presets assume.

## 1. Channel audit (2026-08-10)

| Channel | ffmpeg today | SVT-AV1 | Action |
| --- | --- | --- | --- |
| Bare binary (win/mac/linux) | venv `static-ffmpeg` → zackees zips, else PATH | **no** (win32 = gyan essentials 8.0.1) | managed fetch (§2) |
| Desktop | same venv path (`runtime/venv`, setup prefetch) | **no** | inherits managed fetch |
| Docker | apt ffmpeg (Debian), static-ffmpeg bins deleted from image, wired via `[jobs]` in docker.toml | yes (Debian links libsvtav1) | none; optional build-time assert (§6) |
| Nix | nixpkgs `ffmpeg` wrapped into PATH | yes (nixpkgs enables svt-av1) | build-time assert (§6) |
| `[jobs] ffmpeg` override / bare PATH | user's own | unknown | untouched; ladder covers it |

## 2. Decision: Rust-managed, pinned, checksummed fetch

A new `media_tools::provision` module owns ffmpeg+ffprobe acquisition.
Resolution order in `media_tools::resolve` becomes:

1. `[jobs] ffmpeg`/`ffprobe` config override — unchanged, the escape hatch
   (air-gapped hosts, distro packagers, the scoop build).
2. **Managed pinned binaries** — `runtime/ffmpeg/<pin-id>/` if present;
   fetched on demand (§3) when absent.
3. Venv `static-ffmpeg` — legacy grace path so existing installs keep working
   offline and mid-upgrade. Dropped (with the pypi dependency) one release
   after the managed path ships.
4. PATH.

Pins live in code as a per-`(os, arch)` table of `{url, sha256}` — the same
"defaults live in code, not in a frozen file" rule the config system follows.
Re-pinning is a release-checklist item, like the Nix ui-pin sync.

### Build sources (proposed; the one genuinely open choice)

- **Windows x64, Linux x64/arm64**: BtbN GitHub releases (`win64-gpl` zip,
  `linux64/linuxarm64-gpl` tar.xz). Full-featured (SVT-AV1, libwebp, libaom),
  static, and published under immutable dated autobuild tags — pin those, not
  the floating `latest`/`nN.N` tags, whose assets get rebuilt in place. The
  sha256 pin guards against mutation either way.
- **macOS x64/arm64**: martin-riedl.de full builds (per-tool zips, sha256
  published, both architectures, SVT-AV1 included). BtbN does not build mac.
- gyan.dev is out: its full build is 7z-only and its zip is the essentials
  build that caused this.

GPL builds fetched at user runtime are licensing-clean (nothing is
redistributed by us). Download size is comparable to what static-ffmpeg
already pulls today (~100 MB once per install).

## 3. Fetch mechanics

- Trigger points: `panoptikon setup` (replacing `prefetch_static_ffmpeg`) and
  lazily from `resolved()` when the managed dir is absent — the same
  first-use download behavior static-ffmpeg has today, so no new UX.
- Download via the existing reqwest + retry middleware stack, to a temp file
  in `runtime/ffmpeg/`, **sha256-verified before unpack** (sha2 is already a
  dependency), unpacked to `<pin-id>.tmp/`, atomic rename to `<pin-id>/`.
  A file lock (single-flight) serializes concurrent starters; a crashed
  unpack leaves only a `.tmp` dir that the next attempt removes.
- Archives: zip (win/mac; `zip` crate already in-tree) and tar.xz (BtbN
  linux; needs one new dep — `liblzma`/`xz2` — feeding the existing `tar`).
- `<pin-id>` encodes source+version (e.g. `btbn-autobuild-2026-07-xx`), so a
  re-pin is a new directory and never a partial overwrite; stale pin dirs are
  pruned on successful resolve of the current one.
- Any fetch failure logs and falls through to the venv/PATH steps — offline
  hosts degrade to exactly today's behavior, and the `[jobs]` override plus
  pre-seeding `runtime/ffmpeg/<pin-id>/` are the supported air-gapped paths.

## 4. Capability logging, not gating

On first resolve, log the chosen binary's origin (managed / venv / PATH /
override) and whether `libsvtav1`, `libaom-av1`, and `libwebp_anim` are
listed — one `-encoders` spawn, shared with the existing hw/av1 probes. No
startup refusal and no preset hiding: the encoder ladder already turns a
missing SVT into libaom, and a build missing both fails the one affected
preset with ffmpeg's own message. (Preset-list gating on probe results was
considered and rejected: it makes `GET /api/video/presets` depend on a spawn,
and the managed path makes the situation it would handle an override-only
corner.)

## 5. Cache-key interaction

None by design: encoder identity is already in the params hash, so a host
moving from libaom (essentials) to SVT (managed) simply mints new keys, the
same way the hardware h264 slot does. No `TRANSCODER_VERSION` bump.

## 6. Docker and Nix

- Docker: keep apt ffmpeg (it has SVT-AV1 and the image already deletes
  static-ffmpeg's bins). Add a build-time `RUN ffmpeg -encoders | grep -q
  libsvtav1` assert so a base-image regression fails the build, not a user's
  export.
- Nix: assert the wrapped ffmpeg lists `libsvtav1` in the existing wrap
  check (`default.nix` already greps the wrapper for ffmpeg); switch to
  `ffmpeg-full` only if the assert ever fails. The managed fetch must be
  DISABLED under Nix (impure download into the store-adjacent runtime dir);
  the wrap already provides ffmpeg on PATH, which step 4 finds.

## 7. Rollout

1. Ship `provision` + resolver reordering + setup prefetch swap + Docker/Nix
   asserts. Desktop needs its usual sidecar rebuild.
2. One release later: drop `static-ffmpeg` from the venv requirements and
   the probe from the resolver (step 3 in §2), and delete
   `STATIC_FFMPEG_PROBE`.

Open items for review: build-source blessing per platform (§2), the xz dep
choice (§3), and whether Desktop should instead bundle ffmpeg in its
installer (rejected here to keep artifacts small and all channels on one
code path, but it is the one alternative with no first-run download).
