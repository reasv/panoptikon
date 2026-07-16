# Panoptikon Desktop icons

These platform icons are generated from the finalized rounded eye render:

`../../../static/render/logo_simple_alt_rounded.png`

Regenerate the complete icon family with Tauri's icon command, then retain the
desktop outputs referenced by `tauri.conf.json`:

```powershell
npx --yes @tauri-apps/cli@2 icon static/render/logo_simple_alt_rounded.png --output panoptikon-desktop/src-tauri/icons
```

**`icon.ico` must NOT be the Tauri-generated one.** Tauri's icon command
PNG-compresses every ICO frame, and the Windows Start menu icon extractor only
accepts PNG compression for the 256px frame — the shortcut then renders as the
generic blank-window icon (other surfaces like the taskbar and Settings decode
it fine, so the breakage is easy to miss). After running the command above,
rebuild `icon.ico` with BMP-encoded frames:

```powershell
python -c "from PIL import Image; Image.open('static/render/logo_simple_alt_rounded.png').convert('RGBA').save('panoptikon-desktop/src-tauri/icons/icon.ico', format='ICO', sizes=[(16,16),(24,24),(32,32),(48,48),(64,64),(256,256)], bitmap_format='bmp')"
```
