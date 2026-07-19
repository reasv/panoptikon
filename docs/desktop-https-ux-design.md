# Desktop HTTPS/HTTP2 — UX and topology design

Design for the opt-in TLS feature (client-performance-plan §5.1), settled
2026-07-20. Covers the desktop-side UX and the listener topology. Cert
material and trust-store mechanics are specified in
[client-performance-plan.md](client-performance-plan.md) §5.1.

## Decisions

1. **Two separate controls.** A one-time *certificate install* action and a
   persistent *Use HTTPS* switch. The switch is hidden until a certificate is
   installed and verified, then appears ON.
2. **Parallel listeners, separate port.** The plain-HTTP listener on
   `server.port` (default 6342) always runs, unchanged. TLS is an additional
   listener (default `server.port + 1`, i.e. 6343). The switch controls
   (a) whether the TLS listener is bound at all and (b) which origin every
   desktop surface opens/advertises. HTTP is never replaced.

### Why parallel + separate port is safe (verified browser behavior)

- **Browsers never auto-upgrade these URLs.** Chrome HTTPS-Upgrades and
  Firefox HTTPS-First both exempt dotless hostnames (`localhost`), IP
  literals, and **non-default ports** from automatic http→https upgrading.
  Old `http://localhost:6342` links and tabs keep working verbatim while the
  feature is on, and nothing silently rewrites schemes in either direction.
- **HSTS is banned on all listeners — hard rule.** HSTS is host-scoped, not
  port-scoped: a `Strict-Transport-Security` header served once from
  `https://localhost:6343` would make the browser upgrade
  `http://localhost:6342` → `https://localhost:6342`, which is the plain
  listener — permanently breaking the fallback origin until the HSTS entry
  expires. Never emit HSTS from the gateway's local listeners.
- Scheme changes when toggling the switch break *only* the origin being
  abandoned, and only for already-open tabs / saved links of that scheme.
  This is inherent to any on/off design (origin includes scheme); parallel
  listeners minimize it to exactly the direction the user chose to leave.

## Listener semantics

| Switch | HTTP :6342 | TLS :6343 | Desktop opens |
|--------|-----------|-----------|---------------|
| (not installed) | serving | not bound | `http://localhost:6342` |
| ON | serving | serving (ALPN h2, http/1.1) | `https://localhost:6343` |
| OFF | serving | **not bound** | `http://localhost:6342` |

- Switch OFF is a **kill switch**: the TLS listener stops listening. The
  disable use case is "TLS is misbehaving in my environment" — a
  half-disabled mode that keeps serving TLS would leave the misbehaving
  surface alive. Old `https://` tabs erroring after disable is the expected,
  intended outcome.
- **Degrade, don't fail:** unlike the primary listeners (all-or-nothing bind
  at startup, `main.rs`), a TLS bind or cert-load failure logs, skips the
  TLS listener, and surfaces as a desktop status — it must never prevent
  gateway startup. TLS is an enhancement; the app must always come up on
  HTTP.
- If the switch is ON but the desktop's post-start health check of the
  `https://` origin fails, the desktop opens the HTTP origin instead and
  shows the degraded state (switch stays ON — it records intent; the status
  row records reality).
- Policy identity: the TLS listener shares the `default` endpoint's policy
  identity (same loopback trust surface, scheme-only origin difference). It
  is not a new `[[server.endpoints]]` entry with separate policy config.
  Policy tokens / client-config capabilities must accept both origins.
- Applying a switch change requires a gateway restart; the desktop prompts
  ("apply now = restart gateway"), warning if jobs are running.

## Config sketch

```toml
[server.tls]
enabled = true            # the switch
port = 6343               # written by desktop at setup: server.port + 1
cert_path = "<app-data>/tls/cert.pem"
key_path  = "<app-data>/tls/key.pem"
```

Desktop owns this block. Absent block = feature never set up. Port collision
at bind time falls under degrade-don't-fail; the settings UI lets the user
pick a different port in that case.

## Settings UX — "HTTPS & HTTP/2" section (settings overview tab)

Small section on the main overview tab. States:

**A. Not set up** — one explainer line ("Enables HTTP/2 for faster image
loading. Installs a certificate for localhost — requires administrator
approval.") + button **Set up HTTPS…**

**B. Setup flow** (modal, user-triggered): explains the steps up front
(generate certificate → install into system trust store, with an elevation
prompt → verify), then runs them with per-step progress and per-step
results. Verification = the desktop makes an `https://` request validated
against the **platform trust store** (rustls-platform-verifier), which
proves OS-level trust rather than just listener liveness. On success the
switch appears, ON, with the restart prompt. On partial success (e.g. Linux
NSS steps skipped), show exactly what succeeded/failed and note the
interstitial click-through fallback ("your browser may show a one-time
warning").

**C. Installed + ON** — switch ON; status line
"Active — https://localhost:6343 (HTTP/2)"; cert line "Certificate expires
<date>"; secondary actions: **Test in browser** (opens the https origin),
**Reinstall certificate**, **Remove certificate…**

**D. Installed + OFF** — switch OFF; status "Installed, disabled — using
http://localhost:6342". TLS listener not bound.

**E. Degraded** — switch ON but health check failed: warning banner
"HTTPS is enabled but not responding — opened the HTTP address instead" +
**Repair** (re-runs setup verification path).

**F. Expiring** — ≤60 days to cert expiry (825-day max validity, Apple cap):
inline prompt to regenerate + reinstall (elevation again).

**Remove flow:** confirmation dialog; deletes cert from trust stores
(elevation), optionally deletes local key material, flips the switch off,
reverts the config block. Returns to state A.

## Surfaces governed by the switch

Everywhere the desktop or gateway emits an absolute self-URL must follow the
switch: startup launch URL, tray/menu "Open" actions, any "open in browser"
buttons, and the settings section's own status text. In-app links are
same-origin relative, so an open tab stays consistently on whichever origin
it was opened from. Audit item: confirm no SSR/API surface emits an absolute
`http://localhost:…` self-URL.

## Open items

- Firefox OS-store import of a non-CA (self-signed leaf) anchor — decides
  Plan A vs Plan B cert material (see plan §5.1).
- Linux install tier: how much per-browser detail the setup results UI
  shows.
- Whether the desktop health check should also detect "cert present but
  browser-untrusted" beyond the platform-verifier signal (probably not —
  browser-level trust isn't programmatically observable; Test-in-browser
  covers it).
