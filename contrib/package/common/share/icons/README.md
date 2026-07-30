# FreeDesktop icons (`panoptikon-desktop`)

Hicolor app icons for all packaging schemes. Generated from
[`static/logo_simple.svg`](../../../../../static/logo_simple.svg).

| Path | Content |
| --- | --- |
| `hicolor/{size}x{size}/apps/panoptikon-desktop.png` | Raster sizes 16–512 (`rsvg-convert` of a CSS-var bake) |
| `hicolor/scalable/apps/panoptikon-desktop.svg` | Baked SVG (CSS `var()` expanded for limited SVG loaders) |

Regenerate after changing the source mark:

```bash
./scripts/generate-hicolor-icons.sh
```

Requires `rsvg-convert` (librsvg). Packages only **ship** these files; they do
not re-render at build time.
