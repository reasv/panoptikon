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
