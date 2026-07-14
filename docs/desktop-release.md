# Panoptikon Desktop release operations

Desktop versions must match the release tag, the Server and Desktop Cargo
package versions, `tauri.conf.json`, and the UI package version. The release
workflow rejects any mismatch or missing platform artifact.

The updater public key is compiled into
`panoptikon-desktop/src-tauri/tauri.conf.json`. Its password-protected private
key is operational secret material: keep an offline recovery copy outside the
repository and provide CI only these GitHub Actions secrets:

- `TAURI_SIGNING_PRIVATE_KEY`
- `TAURI_SIGNING_PRIVATE_KEY_PASSWORD`

Never commit the private key or password, print them in logs, attach them to a
release, or reuse them for Server checksums. Losing the key prevents existing
Desktop installations from accepting future updates; leaking it requires an
incident response and updater key migration.

For a release, push a canonical `vX.Y.Z` tag. CI builds the UI and bundled
Server natively for each target, stages that exact Server as the Tauri sidecar,
and builds signed Desktop bundles. Verify that the release has three Server
artifacts, three human-facing Desktop installers, three updater payloads plus
signatures, `latest.json`, and `latest-desktop.json`. Test the direct installer
and an update from the preceding release on every supported platform.

Windows packages are per-user NSIS installers. macOS is ad-hoc signed only and
not notarized. Document the expected unknown-publisher/Gatekeeper prompts; do
not describe these packages as Authenticode- or Developer-ID-signed.

## Windows development installer and clean-state testing

For normal source development, run `scripts/run-desktop-dev.ps1`. It builds the
current standalone UI, embeds it in a debug Server sidecar, stages that sidecar,
and launches the unpackaged Tauri application with the isolated
`app.panoptikon.desktop.dev` profile. It does not build or install an NSIS
package. Pass `-SkipUiBuild` for Desktop-shell-only iterations after a current
standalone UI has already been built.

Quit the dev application before rerunning the script if Server/UI sources have
changed, because Windows cannot replace a running sidecar executable.

Run `scripts/build-desktop-dev.ps1` from the repository to build the standalone
UI, bundled Server sidecar, and an unsigned NSIS installer using the mandatory
`app.panoptikon.desktop.dev` Tauri overlay. The development profile uses gateway
port 16342, UI port 16340, and Relay port 17601; browser-facing URLs use
`localhost` while listeners remain explicitly bound to IPv4 loopback.

After quitting Desktop Dev, run `scripts/reset-desktop-dev.ps1` to remove only
its roaming and local application-data roots. The already-installed app will
repeat extraction, environment preparation, and first-database setup on its
next launch. Use
`-WhatIf` to print the guarded targets and `-Force` to skip the RESET prompt.
