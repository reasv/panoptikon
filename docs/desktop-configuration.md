# Desktop configuration architecture

Panoptikon Desktop has three configuration scopes. They deliberately have
different owners and surfaces:

| Scope | Concrete file | Primary UI | Apply behavior |
| --- | --- | --- | --- |
| Desktop shell | Desktop config `desktop.toml` | Native Desktop control window | Immediate |
| Panoptikon installation | Server root `config/server/desktop.toml` and `.env` references | Native Desktop control window | Save, validate ports, restart Server |
| Index database | `data/index/<database>/config.toml` | Scan page and setup wizard | Immediate / next job as appropriate |
| Inference external inputs | Server root `.env` | “Model credentials and services” page | Next worker spawn; no Server restart |

The native control window owns settings which must remain repairable while the
Server is down. In particular, a port collision cannot be fixed through an HTTP
page hosted on that port. The web frontend owns database-scoped workflows and
model inputs, where it has the necessary database and registry context.

## Editing contract

`panoptikon-config` is the shared file-editing layer. Typed Rust structures
still define defaults and validation; the editable document is retained
separately and receives only the semantic before/after delta.

Every programmatic writer must follow these rules:

1. Parse and validate the current file before changing it.
2. Keep the original editable document (`toml_edit::DocumentMut`).
3. Compare the typed value before the operation with the requested value.
4. Patch only changed paths. Do not materialize unchanged defaults.
5. Preserve comments, whitespace, key/table order, unchanged literal spelling,
   and unknown keys.
6. Commit through a same-directory atomic replacement. Preserve existing file
   permissions; private Desktop and `.env` files use mode `0600` when created
   on Unix.
7. Serialize in-process writers and reject a stale GUI save when its revision
   no longer matches the TOML/`.env` files on disk.

Changed composite values may necessarily receive new formatting, but unrelated
values do not. Array-of-table order is semantic for policies and is never
sorted. New GUI-managed policies are appended without moving hand-written
policies.

The same crate has a line-preserving `.env` editor. It retains comments,
unrelated assignments, ordering, and the original LF/CRLF convention. Duplicate
assignments for a value being edited are collapsed so a later duplicate cannot
silently override the GUI change.

### Environment-backed TOML fields

A quoted whole-value template such as `port = "${PORT:-6342}"` is a reference,
not a literal value owned by the TOML editor. The GUI resolves and displays the
effective value, labels it as environment-backed, and writes `PORT` in the
managed `.env` file. It must not replace the TOML expression with `6342` or the
new value. Embedded expressions such as `"prefix-${NAME}"` are not treated as a
single editable binding and remain advanced, text-only configuration.

Desktop-managed `.env` values take precedence over the inherited environment,
matching worker-spawn behavior. A multi-file save prepares both edits first; if
the `.env` commit fails after the TOML commit, the TOML file is restored.

## Supported installation-wide surface

### Network

- Primary loopback port.
- Optional LAN access on a **separate** named listener and port.
- All databases or an explicit database allowlist, plus the LAN default.

LAN access is represented as endpoint `lan` and policy `desktop_lan`. The
primary `default` endpoint stays on `127.0.0.1`; the LAN endpoint binds
`0.0.0.0`. Physical endpoint matching, rather than the request `Host`, keeps
the local Desktop policy unforgeable from the LAN. `/api/desktop/*` has an
additional policy-layer guard and is available only when the matched policy has
`client.desktop = true`.

Index and user-data database policies use the same allowlist in the simplified
surface so the meaning of “database available on the LAN” is predictable.
This listener has no authentication and grants the normal Panoptikon API over
the selected databases, so it is intended only for trusted networks. It does
not grant Desktop-management endpoints or local Desktop-shell authority.
Tenant prefixes, identity headers, different index/user-data policies, custom
match conditions, or a nonstandard ruleset make the LAN configuration
**custom**. The GUI then explains the condition and leaves that portion
read-only; it never attempts to normalize it.

Before saving a changed listener, Desktop attempts the exact bind. An occupied
port produces an actionable error and the control window remains usable. The
currently bound port is exempt while the managed sidecar is running. If the
sidecar is stopped or failed, even an unchanged port is checked, which covers
the common “another process took 6342” recovery case. Panoptikon does not edit
OS firewall rules.

### Performance and memory

Installation-wide controls:

- inference prewarm master switch;
- lazy prewarm behavior;
- concurrent file loaders;
- intermediate input-data memory budget;
- search embedding-cache size.

Database controls remain on the Scan page:

- `prewarm_embedding_models` prepares Python/model code and reduces import
  latency, but does not load model weights;
- `preload_embedding_models` keeps full embedding models loaded and can consume
  substantial RAM or VRAM;
- per-model extraction batch sizes trade throughput for GPU memory.

The UI must describe these as different memory/latency mechanisms rather than
as interchangeable “preload/prewarm” switches.

### Existing dedicated surfaces

- File-opening commands remain in Desktop because they affect the local host
  and Relay together.
- Declared inference API keys and service values remain on the web page and in
  `.env`; they are hot-read for each worker spawn.
- Folder selection, file types, continuous scanning, schedules, job settings,
  filters, and model preload/prewarm remain per database.
- Desktop enablement, start-at-login, Relay, updates, and diagnostics are shell
  preferences, not Server TOML. “Start Panoptikon automatically” is available
  on the control window's Overview tab as well as from the tray menu; both
  surfaces reflect the operating system's current login-item state.

## Intentionally text-only global settings

The following are valid but not part of the simplified Desktop surface:

- upstream topology, UI build/runtime paths, custom Python and registry paths;
- inference worker deadlines, sweep intervals, eager `always_warm` class lists;
- full rulesets, identities, tenant rewriting, forwarded-header trust, policy
  token keys, and arbitrary extra listeners/policies;
- read-only mode, data-root relocation, scratch paths, custom logging filters;
- explicit ffmpeg/pdfium/browser/font paths and other diagnostic overrides;
- atomic-job and image decompression ceilings.

These settings are deployment, security, or recovery tools whose safe UX needs
more context than a generic form. Users remain free to edit them. The GUI reads
effective supported values, preserves everything else, marks advanced database
configuration, and refuses simplified edits to custom LAN structures.

Accelerator selection is relevant to Desktop but is not an ordinary live
setting: changing it requires rebuilding the managed Python environment. It
belongs in a future dedicated “Inference environment” repair flow that can show
download size, hardware detection, progress, cancellation, and rollback—not in
the generic Server configuration form.
