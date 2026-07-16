# Panoptikon Desktop icons

These platform icons are generated from the finalized rounded eye render:

`../../../static/render/logo_simple_alt_rounded.png`

Regenerate the complete icon family with Tauri's icon command, then retain the
desktop outputs referenced by `tauri.conf.json`:

```powershell
npx --yes @tauri-apps/cli@2 icon static/render/logo_simple_alt_rounded.png --output panoptikon-desktop/src-tauri/icons
```
