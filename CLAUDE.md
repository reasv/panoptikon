# Project status

- The Python version of panoptikon is **deprecated**. All further development happens **only** on the Rust version (the Rust port).
- Any reference to an issue, bug, or possible improvement to panoptikon implicitly refers to the Rust port, unless a message explicitly says otherwise.

# Commit messages

- Keep commit messages short: a single subject line of at most 80 characters.
- Never add a `Co-Authored-By` line or any other trailer to commit messages.

# Config authoring (shipped server TOML)

The shipped server configs (`config/server/*.toml`) are seeded to disk **once**
on first run and are user-owned and never overwritten afterward (bare binary +
Desktop: create-new; Docker: the image seeds the `panoptikon-config` volume
only when it is empty). A value written as a **live line** therefore *freezes*
for existing users — changing its default later never reaches them. The real
defaults layer is the `#[serde(default)]` attributes in
`panoptikon/src/config.rs`, which apply on every upgrade for any key absent
from the user's file.

When adding or changing a setting, classify it:

- **Tunable default** → put the default in `#[serde(default)]` and ship it in
  the TOML only as a **commented example** (`# key = value`), never a live
  line. Absence tracks the code default, so it can be changed centrally later.
- **`${VAR}` env-bridge line** (e.g. `level = "${LOGLEVEL:-INFO}"`) → must stay
  live: the template only expands on lines present in the file. Its fallback
  value still freezes.
- **Deployment identity / profile-specific value** (ports, hosts, the
  rulesets/policies block, and per-profile deviations like
  `trust_forwarded_headers = true` or `inference_local.enabled = true`) → live
  line, and accept that it freezes; there is no serde-default path for these.
  Reaching existing users with a new default for one of these needs a
  version-stamped top-up merge, which does not exist yet.

Per-DB `config.toml` is serialized from `SystemConfig::default()` at creation,
and serde defaults fill absent keys at load, so a newly added key is
functionally correct for existing databases without any migration.
