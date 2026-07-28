#!/usr/bin/env bash
# Regenerate FreeDesktop hicolor icons for panoptikon-desktop from
# static/logo_simple.svg. Commit under contrib/package/common/share/icons/.
#
# Requires: rsvg-convert (librsvg)
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
src_svg="$repo_root/static/logo_simple.svg"
out_root="$repo_root/contrib/package/common/share/icons/hicolor"
sizes=(16 22 24 32 48 64 96 128 256 512)

if ! command -v rsvg-convert >/dev/null 2>&1; then
  echo "error: rsvg-convert not found (install librsvg)" >&2
  exit 1
fi

if [[ ! -f $src_svg ]]; then
  echo "error: missing $src_svg" >&2
  exit 1
fi

# logo_simple.svg uses CSS custom properties that librsvg (and many FreeDesktop
# icon loaders) cannot resolve. Bake the file's default gh-palette for both
# PNGs and the shipped scalable SVG.
baked="$(mktemp --suffix=.svg)"
trap 'rm -f "$baked"' EXIT

sed \
  -e 's/var(--fill-color)/#ffffff/g' \
  -e 's/var(--line-color)/#161b22/g' \
  -e 's/var(--background-color)/transparent/g' \
  -e 's/var(--text-color)/#ffffff/g' \
  -e 's/var(--text-border-color)/#ffffff/g' \
  -e 's/var(--wheel-border-width)/0/g' \
  -e 's/var(--light-color)/#ffffff/g' \
  -e 's/var(--dark-color)/#161b22/g' \
  "$src_svg" >"$baked"

install -Dm644 "$baked" "$out_root/scalable/apps/panoptikon-desktop.svg"
echo "wrote $out_root/scalable/apps/panoptikon-desktop.svg"

for size in "${sizes[@]}"; do
  dir="$out_root/${size}x${size}/apps"
  install -d "$dir"
  rsvg-convert -w "$size" -h "$size" "$baked" \
    -o "$dir/panoptikon-desktop.png"
  echo "wrote $dir/panoptikon-desktop.png"
done

echo "done: $out_root"
