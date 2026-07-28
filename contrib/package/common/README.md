# Common packaging assets

Cross-package, packaging-scheme-agnostic files used by **every** installer
(Nix, and future deb/rpm/AppImage/etc.). Scheme-specific logic lives under
`contrib/package/nix/`, `contrib/nixos/`, and so on; this tree only holds
payloads that install under the same relative paths on the target system.

Layout mirrors a conventional install prefix fragment. Today:

```
common/
  share/                 # → $prefix/share/
    icons/hicolor/…      # FreeDesktop app icons
```

Add further prefix trees here as needed (`share/man`, `share/metainfo`,
`etc/…` only if they are truly shared, etc.) — not under a single scheme’s
directory.

| Repo path | Typical install path |
| --- | --- |
| `share/icons/hicolor/…/apps/panoptikon-desktop.png` | `$prefix/share/icons/hicolor/…/apps/panoptikon-desktop.png` |
| `share/icons/hicolor/scalable/apps/panoptikon-desktop.svg` | `$prefix/share/icons/hicolor/scalable/apps/panoptikon-desktop.svg` |

Brand source for icons: [`static/logo_simple.svg`](../../../static/logo_simple.svg).  
Regenerate icons:

```bash
./scripts/generate-hicolor-icons.sh
```

See also [`share/icons/README.md`](share/icons/README.md).
