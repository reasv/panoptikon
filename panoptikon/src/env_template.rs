//! Environment-variable templating for TOML config values.
//!
//! Env vars are not a parallel configuration mechanism: they feed TOML values
//! through `${NAME}` / `${NAME:-default}` placeholders inside *string values*.
//! Substitution operates on the parsed `toml::Value` tree — never on the raw
//! file text — so env values containing TOML-significant characters (Windows
//! paths with backslashes, quotes) can never corrupt the document.
//!
//! Syntax, inside string values only (shell / docker-compose conventions):
//!
//! - `${NAME}` — replaced with the env var's value; a **hard error** naming
//!   the file and the variable when it is unset (fail loudly for secrets).
//!   Set-but-empty is *not* an error: it yields `""`, matching shell.
//! - `${NAME:-default}` — the literal `default` text when the variable is
//!   unset **or set but empty** (shell `:-`). The default runs to the first
//!   `}` (no nesting).
//! - `${NAME-default}` — the default only when the variable is *unset*
//!   (shell `-`); a set-but-empty variable yields `""`.
//! - `$${` — escapes to a literal `${`.
//! - `NAME` matches `[A-Za-z_][A-Za-z0-9_]*`. Anything else after `${` is a
//!   hard error (typos must not pass silently); a `$` not followed by `{` is
//!   literal.
//!
//! Multiple placeholders per string are fine. Substitution is a single pass:
//! placeholder-looking text inside a substituted value is *not* re-expanded.
//! Substitution itself only touches strings, but numeric/boolean/float
//! keys in the *server* config can still be templated by quoting the whole
//! value (`port = "${PORT:-6342}"`): the substituted string is handed to
//! the config crate, whose deserializer coerces it to the target key's
//! type (string-typed keys keep the string verbatim — nothing is ever
//! round-tripped through a number).

use anyhow::{Context, Result, bail};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Mutex, OnceLock},
};

static INHERITED_ENVIRONMENT: OnceLock<HashMap<String, String>> = OnceLock::new();

/// Capture the launch environment before `.env` is loaded into the process.
/// The just-in-time Inferio resolver needs this distinction so removing a
/// Desktop-managed value cannot fall back to a stale startup copy.
pub fn capture_inherited_environment() {
    let _ = INHERITED_ENVIRONMENT.set(std::env::vars().collect());
}

/// Load the root `.env` into the process environment (dotenv convention:
/// variables already set in the inherited environment win). Malformed lines
/// are skipped, not fatal — one stray line must not disable the file.
/// Returns the diagnostics for the caller to log later: this runs before
/// logging init because `.env` may configure logging itself.
pub fn load_process_dotenv() -> Vec<String> {
    let path = Path::new(".env");
    if !path.is_file() {
        return Vec::new();
    }
    let source = match std::fs::read_to_string(path) {
        Ok(source) => source,
        Err(error) => return vec![format!("failed to read {}: {error}", path.display())],
    };
    let parsed = panoptikon_config::parse_dotenv(&source);
    for (key, value) in parsed.values {
        if std::env::var_os(&key).is_none() {
            // SAFETY: startup, at the call site the dotenvy loader used to
            // occupy — nothing reads the environment concurrently yet.
            unsafe { std::env::set_var(&key, &value) };
        }
    }
    parsed
        .diagnostics
        .into_iter()
        .map(|diagnostic| format!("{}: {diagnostic}", path.display()))
        .collect()
}

/// Log each unique `.env` diagnostic once per process: the environment
/// snapshot is re-read per worker spawn and per external-inputs poll, and
/// repeating the same warning for every request would drown the log.
pub fn warn_dotenv_diagnostics(messages: &[String]) {
    static REPORTED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let mut reported = REPORTED.get_or_init(Default::default).lock().unwrap();
    for message in messages {
        if reported.insert(message.clone()) {
            tracing::warn!("{message}");
        }
    }
}

/// A just-in-time environment snapshot used for inference worker creation.
#[derive(Debug, Clone)]
pub struct EnvironmentSnapshot {
    values: HashMap<String, String>,
}

impl EnvironmentSnapshot {
    /// Read the root `.env` now. Ordinary server mode lets the inherited
    /// launch environment win; Desktop-managed mode treats its managed file
    /// as the explicit configuration surface and lets the file win.
    pub fn current(desktop_managed: bool) -> Result<Self> {
        let inherited = INHERITED_ENVIRONMENT
            .get()
            .cloned()
            .unwrap_or_else(|| std::env::vars().collect());
        let mut dotenv = HashMap::new();
        let path = Path::new(".env");
        if path.is_file() {
            let source = std::fs::read_to_string(path)
                .with_context(|| format!("failed to read {}", path.display()))?;
            let parsed = panoptikon_config::parse_dotenv(&source);
            warn_dotenv_diagnostics(
                &parsed
                    .diagnostics
                    .iter()
                    .map(|diagnostic| format!("{}: {diagnostic}", path.display()))
                    .collect::<Vec<_>>(),
            );
            dotenv.extend(parsed.values);
        }
        let mut values;
        if desktop_managed {
            values = inherited;
            values.extend(dotenv);
        } else {
            values = dotenv;
            values.extend(inherited);
        }
        Ok(Self { values })
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    pub fn substitute_json_value(&self, value: &mut serde_json::Value) -> Result<()> {
        walk_json(value, &|name| Ok(self.values.get(name).cloned()))
    }
}

/// Recursively substitute every string value in `value` (arrays and nested
/// tables included). Errors name `source` (the file the value came from) and
/// the offending variable/placeholder.
pub fn substitute_toml_value(value: &mut toml::Value, source: &Path) -> Result<()> {
    walk(value).with_context(|| format!("in config file {}", source.display()))
}

fn walk(value: &mut toml::Value) -> Result<()> {
    match value {
        toml::Value::String(text) => {
            if text.contains('$') {
                *text = substitute_str(text)?;
            }
            Ok(())
        }
        toml::Value::Array(items) => {
            for item in items.iter_mut() {
                walk(item)?;
            }
            Ok(())
        }
        toml::Value::Table(table) => {
            for (_, item) in table.iter_mut() {
                walk(item)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Substitute placeholders in one string. Exposed for tests; config code goes
/// through [`substitute_toml_value`].
pub fn substitute_str(input: &str) -> Result<String> {
    substitute_str_with(input, &lookup)
}

fn substitute_str_with(
    input: &str,
    lookup: &impl Fn(&str) -> Result<Option<String>>,
) -> Result<String> {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(pos) = rest.find('$') {
        out.push_str(&rest[..pos]);
        let tail = &rest[pos..];
        if let Some(after) = tail.strip_prefix("$${") {
            // `$${` -> literal `${`, never re-scanned (single pass).
            out.push_str("${");
            rest = after;
        } else if let Some(inner) = tail.strip_prefix("${") {
            let name_len = leading_var_name_len(inner);
            if name_len == 0 {
                bail!(
                    "malformed substitution '{}': expected a variable name \
                     ([A-Za-z_][A-Za-z0-9_]*) after '${{' (write '$${{' for a literal '${{')",
                    snippet(tail)
                );
            }
            let name = &inner[..name_len];
            let after_name = &inner[name_len..];
            if let Some(after) = after_name.strip_prefix('}') {
                // ${NAME}: unset is a hard error — this form is for secrets
                // and required values, which must fail loudly. Set-but-empty
                // yields "" (shell semantics).
                match lookup(name)? {
                    Some(value) => out.push_str(&value),
                    None => bail!(
                        "environment variable '{name}' is not set (referenced as \
                         '${{{name}}}'; use '${{{name}:-default}}' to provide a fallback)"
                    ),
                }
                rest = after;
            } else if let Some((after_op, empty_uses_default)) = after_name
                .strip_prefix(":-")
                .map(|rest| (rest, true))
                .or_else(|| after_name.strip_prefix('-').map(|rest| (rest, false)))
            {
                // Shell conventions: `:-` substitutes the default when the
                // variable is unset OR set-but-empty; `-` only when unset.
                let Some(end) = after_op.find('}') else {
                    bail!(
                        "malformed substitution '{}': missing closing '}}'",
                        snippet(tail)
                    );
                };
                let default = &after_op[..end];
                match lookup(name)? {
                    Some(value) if !(empty_uses_default && value.is_empty()) => {
                        out.push_str(&value)
                    }
                    _ => out.push_str(default),
                }
                rest = &after_op[end + 1..];
            } else {
                bail!(
                    "malformed substitution '{}': expected '}}', ':-default}}' or \
                     '-default}}' after the variable name",
                    snippet(tail)
                );
            }
        } else {
            // A lone `$` (or `$$` not followed by `{`) is literal.
            out.push('$');
            rest = &tail[1..];
        }
    }
    out.push_str(rest);
    Ok(out)
}

fn walk_json(
    value: &mut serde_json::Value,
    lookup: &impl Fn(&str) -> Result<Option<String>>,
) -> Result<()> {
    match value {
        serde_json::Value::String(text) => {
            if text.contains('$') {
                *text = substitute_str_with(text, lookup)?;
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                walk_json(item, lookup)?;
            }
        }
        serde_json::Value::Object(map) => {
            for item in map.values_mut() {
                walk_json(item, lookup)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Length of the leading `[A-Za-z_][A-Za-z0-9_]*` run in `s`.
fn leading_var_name_len(s: &str) -> usize {
    let mut len = 0;
    for (idx, ch) in s.char_indices() {
        let valid = if idx == 0 {
            ch.is_ascii_alphabetic() || ch == '_'
        } else {
            ch.is_ascii_alphanumeric() || ch == '_'
        };
        if !valid {
            break;
        }
        len = idx + ch.len_utf8();
    }
    len
}

/// Env lookup distinguishing "unset" (None) from non-unicode values (error).
fn lookup(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            bail!("environment variable '{name}' contains non-unicode data")
        }
    }
}

/// A short prefix of the offending text for error messages.
fn snippet(text: &str) -> &str {
    let end = text
        .char_indices()
        .take(24)
        .last()
        .map(|(idx, ch)| idx + ch.len_utf8())
        .unwrap_or(0);
    &text[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Set an env var for the duration of the returned guard. Test-unique
    /// variable names keep parallel tests from racing on process-global env.
    struct EnvVarGuard(&'static str);
    impl EnvVarGuard {
        fn set(name: &'static str, value: &str) -> Self {
            unsafe { std::env::set_var(name, value) };
            Self(name)
        }
    }
    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            unsafe { std::env::remove_var(self.0) };
        }
    }

    /// `${NAME}` substitutes the value; multiple placeholders per string all
    /// expand; text around them is preserved.
    #[test]
    fn basic_substitution_and_multiple_per_string() {
        let _a = EnvVarGuard::set("ET_BASIC_A", "alpha");
        let _b = EnvVarGuard::set("ET_BASIC_B", "beta");
        assert_eq!(substitute_str("${ET_BASIC_A}").unwrap(), "alpha");
        assert_eq!(
            substitute_str("x-${ET_BASIC_A}-y-${ET_BASIC_B}-z").unwrap(),
            "x-alpha-y-beta-z"
        );
    }

    /// `${NAME:-default}` (shell `:-`) uses the env value when set and
    /// non-empty, and the default when unset OR set-but-empty; an empty
    /// default is allowed; defaults may contain any text short of `}`
    /// (including `$`, spaces, colons, backslashes).
    #[test]
    fn colon_dash_default_form() {
        let _a = EnvVarGuard::set("ET_DEF_SET", "real");
        assert_eq!(substitute_str("${ET_DEF_SET:-fallback}").unwrap(), "real");
        assert_eq!(
            substitute_str("${ET_DEF_UNSET_XYZ:-fallback}").unwrap(),
            "fallback"
        );
        let _b = EnvVarGuard::set("ET_DEF_EMPTY", "");
        assert_eq!(
            substitute_str("${ET_DEF_EMPTY:-fallback}").unwrap(),
            "fallback",
            "shell :- treats set-but-empty like unset"
        );
        assert_eq!(substitute_str("${ET_DEF_UNSET_XYZ:-}").unwrap(), "");
        assert_eq!(
            substitute_str("${ET_DEF_UNSET_XYZ:-C:\\data dir:$1}").unwrap(),
            "C:\\data dir:$1"
        );
    }

    /// `${NAME-default}` (shell `-`) uses the default only when the variable
    /// is *unset*: a set-but-empty variable yields "".
    #[test]
    fn dash_default_form() {
        let _a = EnvVarGuard::set("ET_DASH_SET", "real");
        assert_eq!(substitute_str("${ET_DASH_SET-fallback}").unwrap(), "real");
        assert_eq!(
            substitute_str("${ET_DASH_UNSET_XYZ-fallback}").unwrap(),
            "fallback"
        );
        let _b = EnvVarGuard::set("ET_DASH_EMPTY", "");
        assert_eq!(
            substitute_str("[${ET_DASH_EMPTY-fallback}]").unwrap(),
            "[]",
            "shell - keeps a set-but-empty value"
        );
        assert_eq!(substitute_str("${ET_DASH_UNSET_XYZ-}").unwrap(), "");
    }

    /// Bare `${NAME}` with the variable set-but-empty yields "" (shell
    /// semantics: only *unset* is the hard error).
    #[test]
    fn bare_form_empty_value_is_not_an_error() {
        let _a = EnvVarGuard::set("ET_EMPTY", "");
        assert_eq!(substitute_str("${ET_EMPTY}").unwrap(), "");
    }

    /// The `.env` empty-assignment scenario: a line like `LOGLEVEL=` leaves
    /// the variable set to "" — `${LOGLEVEL:-INFO}` must still resolve to
    /// INFO (docker-compose/shell convention).
    #[test]
    fn empty_assignment_resolves_to_colon_dash_default() {
        let _a = EnvVarGuard::set("ET_LOGLEVEL_EMPTY", "");
        assert_eq!(
            substitute_str("${ET_LOGLEVEL_EMPTY:-INFO}").unwrap(),
            "INFO"
        );
    }

    /// `${NAME}` with the variable unset is a hard error naming the variable
    /// and suggesting the default form.
    #[test]
    fn unset_without_default_is_error() {
        let err = substitute_str("${ET_UNSET_NO_DEFAULT_XYZ}").unwrap_err();
        let text = format!("{err:#}");
        assert!(text.contains("ET_UNSET_NO_DEFAULT_XYZ"), "{text}");
        assert!(text.contains(":-"), "suggests the default form: {text}");
    }

    /// `$${` escapes to a literal `${`, and the escaped text is not
    /// re-expanded (single pass). Lone `$` and `$$` (not before `{`) are
    /// literal.
    #[test]
    fn escapes_and_literal_dollars() {
        assert_eq!(substitute_str("$${ET_X}").unwrap(), "${ET_X}");
        assert_eq!(substitute_str("cost: $5").unwrap(), "cost: $5");
        assert_eq!(substitute_str("a$$b").unwrap(), "a$$b");
        assert_eq!(substitute_str("trailing $").unwrap(), "trailing $");
        let _a = EnvVarGuard::set("ET_ESC_MIX", "v");
        assert_eq!(
            substitute_str("$${ET_ESC_MIX} ${ET_ESC_MIX}").unwrap(),
            "${ET_ESC_MIX} v"
        );
    }

    /// Substituted values are not re-scanned: a value containing `${...}`
    /// comes through verbatim (no recursion, no injection via env values).
    #[test]
    fn no_recursive_expansion() {
        let _a = EnvVarGuard::set("ET_RECURSE", "${ET_INNER}");
        assert_eq!(substitute_str("${ET_RECURSE}").unwrap(), "${ET_INNER}");
    }

    /// Malformed placeholders are hard errors: empty/invalid names, a missing
    /// closing brace, and a bare `:` without `-`. Note `${NA-ME}` is NOT
    /// malformed — it is variable `NA` with the `-` (unset-only) default
    /// `ME`.
    #[test]
    fn malformed_placeholders_error() {
        for bad in ["${}", "${1BAD}", "${NAME", "${NAME:x}", "${NAME:-open"] {
            let err = substitute_str(bad).expect_err(bad);
            assert!(
                format!("{err:#}").contains("malformed") || format!("{err:#}").contains("closing"),
                "{bad}: {err:#}"
            );
        }
        assert_eq!(
            substitute_str("${ET_SURELY_UNSET_NA-ME}").unwrap(),
            "ME",
            "name-dash-text parses as the unset-only default form"
        );
    }

    /// Windows env values full of backslashes survive because substitution
    /// happens on parsed values, not raw TOML text.
    #[test]
    fn windows_backslash_values_survive() {
        let _a = EnvVarGuard::set("ET_WIN_PATH", r"C:\Users\test\data folder");
        assert_eq!(
            substitute_str("${ET_WIN_PATH}").unwrap(),
            r"C:\Users\test\data folder"
        );
    }

    /// The tree walker reaches strings nested in tables, arrays, and arrays
    /// of tables; non-string values are untouched.
    #[test]
    fn walks_nested_tables_and_arrays() {
        let _a = EnvVarGuard::set("ET_TREE", "sub");
        let mut value: toml::Value = toml::from_str(
            r#"
top = "${ET_TREE}"
num = 5
flag = true

[table.inner]
key = "a ${ET_TREE} b"
list = ["${ET_TREE}", "plain", "${ET_TREE_MISSING:-d}"]

[[array_of_tables]]
key = "${ET_TREE}"
"#,
        )
        .unwrap();
        substitute_toml_value(&mut value, Path::new("test.toml")).unwrap();
        assert_eq!(value["top"].as_str(), Some("sub"));
        assert_eq!(value["num"].as_integer(), Some(5));
        assert_eq!(value["table"]["inner"]["key"].as_str(), Some("a sub b"));
        let list = value["table"]["inner"]["list"].as_array().unwrap();
        assert_eq!(list[0].as_str(), Some("sub"));
        assert_eq!(list[1].as_str(), Some("plain"));
        assert_eq!(list[2].as_str(), Some("d"));
        assert_eq!(value["array_of_tables"][0]["key"].as_str(), Some("sub"));
    }

    /// Errors from the tree walker name both the file and the variable.
    #[test]
    fn error_names_file_and_variable() {
        let mut value: toml::Value = toml::from_str(
            r#"
[section]
secret = "${ET_MISSING_SECRET_XYZ}"
"#,
        )
        .unwrap();
        let err = substitute_toml_value(&mut value, Path::new("conf/gw.toml")).unwrap_err();
        let text = format!("{err:#}");
        assert!(text.contains("gw.toml"), "names the file: {text}");
        assert!(
            text.contains("ET_MISSING_SECRET_XYZ"),
            "names the variable: {text}"
        );
    }

    /// A parsed-tree substitution round-trips through TOML re-serialization
    /// without corrupting backslash-heavy values (the reason substitution is
    /// not textual).
    #[test]
    fn reserialized_tree_preserves_backslashes() {
        let _a = EnvVarGuard::set("ET_RT_PATH", r"C:\temp\x");
        let mut value: toml::Value = toml::from_str(r#"p = "${ET_RT_PATH}""#).unwrap();
        substitute_toml_value(&mut value, Path::new("t.toml")).unwrap();
        let text = toml::to_string(&value).unwrap();
        let reparsed: toml::Value = toml::from_str(&text).unwrap();
        assert_eq!(reparsed["p"].as_str(), Some(r"C:\temp\x"));
    }
}
