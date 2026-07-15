//! Lossless editors for Panoptikon's user-owned configuration files.
//!
//! Typed configuration remains the source of validation and defaults. This
//! crate owns the other half of the problem: applying the typed before/after
//! delta to the concrete file without reserializing unrelated syntax.

use anyhow::{Context as _, Result, bail};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write as _,
    path::{Path, PathBuf},
    str::FromStr as _,
    sync::atomic::{AtomicU64, Ordering},
};
use toml_edit::{Array, ArrayOfTables, DocumentMut, InlineTable, Item, Table, Value};

/// An editable TOML document which retains comments, whitespace, key order,
/// table order, and the spelling of every unchanged value.
#[derive(Debug, Clone)]
pub struct TomlDocument {
    document: DocumentMut,
}

impl TomlDocument {
    pub fn parse(source: &str) -> Result<Self> {
        Ok(Self {
            document: DocumentMut::from_str(source).context("invalid TOML document")?,
        })
    }

    pub fn from_serializable<T: Serialize>(value: &T) -> Result<Self> {
        let source = toml::to_string_pretty(value).context("failed to serialize TOML value")?;
        Self::parse(&source)
    }

    /// Apply only fields whose serialized values differ between `before` and
    /// `after`. Defaults absent from the concrete file therefore stay absent.
    pub fn patch_serialized<T: Serialize>(&mut self, before: &T, after: &T) -> Result<()> {
        let before = toml::Value::try_from(before).context("failed to serialize old TOML value")?;
        let after = toml::Value::try_from(after).context("failed to serialize new TOML value")?;
        self.patch_values(&before, &after)
    }

    pub fn patch_values(&mut self, before: &toml::Value, after: &toml::Value) -> Result<()> {
        let before = before
            .as_table()
            .context("old TOML document value is not a table")?;
        let after = after
            .as_table()
            .context("new TOML document value is not a table")?;
        patch_table(self.document.as_table_mut(), before, after)
    }

    pub fn write_atomic(&self, path: &Path) -> Result<()> {
        atomic_write(path, self.to_string().as_bytes())
    }

    pub fn write_private_atomic(&self, path: &Path) -> Result<()> {
        atomic_write_with_mode(path, self.to_string().as_bytes(), Some(0o600))
    }
}

impl std::fmt::Display for TomlDocument {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.document.fmt(formatter)
    }
}

fn patch_table(concrete: &mut Table, before: &toml::Table, after: &toml::Table) -> Result<()> {
    for key in before.keys().filter(|key| !after.contains_key(*key)) {
        concrete.remove(key);
    }
    for (key, new_value) in after {
        let old_value = before.get(key);
        if old_value == Some(new_value) {
            continue;
        }
        match (concrete.get_mut(key), old_value) {
            (Some(item), Some(old_value)) => patch_item(item, old_value, new_value)?,
            _ => {
                concrete.insert(key, item_from_toml(new_value)?);
            }
        }
    }
    Ok(())
}

fn patch_item(concrete: &mut Item, before: &toml::Value, after: &toml::Value) -> Result<()> {
    match (concrete, before, after) {
        (Item::Table(table), toml::Value::Table(before), toml::Value::Table(after)) => {
            patch_table(table, before, after)?
        }
        (
            Item::Value(Value::InlineTable(table)),
            toml::Value::Table(before),
            toml::Value::Table(after),
        ) => patch_inline_table(table, before, after)?,
        (
            Item::Value(Value::Array(array)),
            toml::Value::Array(before),
            toml::Value::Array(after),
        ) => patch_array(array, before, after)?,
        (Item::ArrayOfTables(tables), toml::Value::Array(before), toml::Value::Array(after))
            if before.iter().all(toml::Value::is_table)
                && after.iter().all(toml::Value::is_table) =>
        {
            patch_array_of_tables(tables, before, after)?
        }
        (slot, _, after) => replace_item_preserving_decor(slot, item_from_toml(after)?),
    }
    Ok(())
}

fn patch_inline_table(
    concrete: &mut InlineTable,
    before: &toml::Table,
    after: &toml::Table,
) -> Result<()> {
    for key in before.keys().filter(|key| !after.contains_key(*key)) {
        concrete.remove(key);
    }
    for (key, new_value) in after {
        let old_value = before.get(key);
        if old_value == Some(new_value) {
            continue;
        }
        match (concrete.get_mut(key), old_value) {
            (Some(value), Some(old_value)) => patch_value(value, old_value, new_value)?,
            _ => {
                concrete.insert(key, value_from_toml(new_value)?);
            }
        }
    }
    Ok(())
}

fn patch_array(concrete: &mut Array, before: &[toml::Value], after: &[toml::Value]) -> Result<()> {
    let shared = before.len().min(after.len()).min(concrete.len());
    for index in 0..shared {
        if before[index] != after[index] {
            patch_value(
                concrete.get_mut(index).expect("shared array index exists"),
                &before[index],
                &after[index],
            )?;
        }
    }
    while concrete.len() > after.len() {
        concrete.remove(concrete.len() - 1);
    }
    for value in after.iter().skip(concrete.len()) {
        concrete.push(value_from_toml(value)?);
    }
    Ok(())
}

fn patch_value(concrete: &mut Value, before: &toml::Value, after: &toml::Value) -> Result<()> {
    match (concrete, before, after) {
        (Value::InlineTable(table), toml::Value::Table(before), toml::Value::Table(after)) => {
            patch_inline_table(table, before, after)
        }
        (Value::Array(array), toml::Value::Array(before), toml::Value::Array(after)) => {
            patch_array(array, before, after)
        }
        (slot, _, after) => {
            let decor = slot.decor().clone();
            *slot = value_from_toml(after)?;
            *slot.decor_mut() = decor;
            Ok(())
        }
    }
}

fn patch_array_of_tables(
    concrete: &mut ArrayOfTables,
    before: &[toml::Value],
    after: &[toml::Value],
) -> Result<()> {
    let shared = before.len().min(after.len()).min(concrete.len());
    for index in 0..shared {
        if before[index] != after[index] {
            patch_table(
                concrete.get_mut(index).expect("shared table index exists"),
                before[index].as_table().expect("guarded above"),
                after[index].as_table().expect("guarded above"),
            )?;
        }
    }
    while concrete.len() > after.len() {
        concrete.remove(concrete.len() - 1);
    }
    let mut next_position = concrete
        .iter()
        .filter_map(max_table_position)
        .max()
        .unwrap_or(0)
        .saturating_add(1);
    for value in after.iter().skip(concrete.len()) {
        let mut table = table_from_toml(value.as_table().expect("guarded above"))?;
        assign_table_positions(&mut table, &mut next_position);
        concrete.push(table);
    }
    Ok(())
}

fn max_table_position(table: &Table) -> Option<usize> {
    let mut maximum = table.position();
    for (_, item) in table.iter() {
        match item {
            Item::Table(child) => maximum = maximum.max(max_table_position(child)),
            Item::ArrayOfTables(children) => {
                for child in children.iter() {
                    maximum = maximum.max(max_table_position(child));
                }
            }
            _ => {}
        }
    }
    maximum
}

fn assign_table_positions(table: &mut Table, next: &mut usize) {
    table.set_position(*next);
    *next = next.saturating_add(1);
    for (_, item) in table.iter_mut() {
        match item {
            Item::Table(child) => assign_table_positions(child, next),
            Item::ArrayOfTables(children) => {
                for child in children.iter_mut() {
                    assign_table_positions(child, next);
                }
            }
            _ => {}
        }
    }
}

fn replace_item_preserving_decor(slot: &mut Item, mut replacement: Item) {
    match (&*slot, &mut replacement) {
        (Item::Value(old), Item::Value(new)) => *new.decor_mut() = old.decor().clone(),
        (Item::Table(old), Item::Table(new)) => *new.decor_mut() = old.decor().clone(),
        _ => {}
    }
    *slot = replacement;
}

fn item_from_toml(value: &toml::Value) -> Result<Item> {
    const WRAPPER: &str = "__panoptikon_edit_value";
    let mut wrapper = toml::Table::new();
    wrapper.insert(WRAPPER.to_owned(), value.clone());
    let source = toml::to_string(&wrapper).context("failed to render TOML patch value")?;
    let mut document =
        DocumentMut::from_str(&source).context("failed to parse TOML patch value")?;
    document
        .as_table_mut()
        .remove(WRAPPER)
        .context("serialized TOML patch value disappeared")
}

fn value_from_toml(value: &toml::Value) -> Result<Value> {
    item_from_toml(value)?
        .into_value()
        .map_err(|_| anyhow::anyhow!("TOML table cannot be embedded as a value"))
}

fn table_from_toml(value: &toml::Table) -> Result<Table> {
    item_from_toml(&toml::Value::Table(value.clone()))?
        .into_table()
        .map_err(|_| anyhow::anyhow!("TOML value is not a table"))
}

/// A whole-value environment reference such as `${PORT:-6342}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvBinding<'a> {
    pub variable: &'a str,
    pub fallback: Option<&'a str>,
    pub fallback_on_empty: bool,
}

/// Recognize only a single whole-value template. Embedded templates are
/// deliberately not editable as one environment-backed setting.
pub fn whole_value_env_binding(value: &str) -> Option<EnvBinding<'_>> {
    let body = value.strip_prefix("${")?.strip_suffix('}')?;
    let split = body.find([':', '-']);
    let (variable, fallback, fallback_on_empty) = match split {
        None => (body, None, false),
        Some(index) if body[index..].starts_with(":-") => {
            (&body[..index], Some(&body[index + 2..]), true)
        }
        Some(index) if body[index..].starts_with('-') => {
            (&body[..index], Some(&body[index + 1..]), false)
        }
        _ => return None,
    };
    is_env_name(variable).then_some(EnvBinding {
        variable,
        fallback,
        fallback_on_empty,
    })
}

fn is_env_name(value: &str) -> bool {
    let mut chars = value.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && chars.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

/// Comment- and order-preserving `.env` editor used by every managed
/// environment-backed configuration surface.
#[derive(Debug, Clone)]
pub struct DotenvDocument {
    lines: Vec<String>,
    newline: &'static str,
    trailing_newline: bool,
}

impl DotenvDocument {
    pub fn parse(source: &str) -> Self {
        let newline = if source.contains("\r\n") {
            "\r\n"
        } else {
            "\n"
        };
        let trailing_newline = source.ends_with('\n');
        let normalized = source.replace("\r\n", "\n");
        let lines = if normalized.is_empty() {
            Vec::new()
        } else {
            normalized
                .strip_suffix('\n')
                .unwrap_or(&normalized)
                .split('\n')
                .map(str::to_owned)
                .collect()
        };
        Self {
            lines,
            newline,
            trailing_newline,
        }
    }

    pub fn load(path: &Path) -> Result<Self> {
        match fs::read_to_string(path) {
            Ok(source) => Ok(Self::parse(&source)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::parse("")),
            Err(error) => {
                Err(error).with_context(|| format!("failed to read '{}'", path.display()))
            }
        }
    }

    pub fn apply(&mut self, values: &BTreeMap<String, String>, remove: &BTreeSet<String>) {
        let mut written = BTreeSet::new();
        self.lines.retain_mut(|line| {
            let Some(assignment) = dotenv_assignment(line) else {
                return true;
            };
            if remove.contains(assignment.key) {
                return false;
            }
            let Some(value) = values.get(assignment.key) else {
                return true;
            };
            if !written.insert(assignment.key.to_owned()) {
                return false;
            }
            let mut replacement = String::with_capacity(line.len() + value.len());
            replacement.push_str(&line[..=assignment.equals]);
            replacement.push_str(&encode_dotenv_value(value));
            replacement.push_str(assignment.comment_suffix);
            *line = replacement;
            true
        });
        for (key, value) in values {
            if !remove.contains(key) && written.insert(key.clone()) {
                self.lines
                    .push(format!("{key}={}", encode_dotenv_value(value)));
            }
        }
        if !self.lines.is_empty() {
            self.trailing_newline = true;
        }
    }

    pub fn write_private_atomic(&self, path: &Path) -> Result<()> {
        atomic_write_with_mode(path, self.to_string().as_bytes(), Some(0o600))
    }
}

impl std::fmt::Display for DotenvDocument {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.lines.join(self.newline))?;
        if self.trailing_newline && !self.lines.is_empty() {
            formatter.write_str(self.newline)?;
        }
        Ok(())
    }
}

struct DotenvAssignment<'a> {
    key: &'a str,
    equals: usize,
    comment_suffix: &'a str,
}

fn dotenv_assignment(line: &str) -> Option<DotenvAssignment<'_>> {
    let equals = line.find('=')?;
    let prefix = line[..equals].trim_start();
    let prefix = prefix
        .strip_prefix("export ")
        .unwrap_or(prefix)
        .trim_start();
    let key = prefix.trim_end();
    if !is_env_name(key) {
        return None;
    }
    let value = &line[equals + 1..];
    let comment = dotenv_comment_start(value).unwrap_or(value.len());
    let whitespace = value[..comment]
        .char_indices()
        .rev()
        .take_while(|(_, character)| character.is_whitespace())
        .last()
        .map(|(index, _)| index)
        .unwrap_or(comment);
    Some(DotenvAssignment {
        key,
        equals,
        comment_suffix: &value[whitespace..],
    })
}

fn dotenv_comment_start(value: &str) -> Option<usize> {
    let mut quote = None;
    let mut escaped = false;
    for (index, character) in value.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quote == Some('"') {
            escaped = true;
            continue;
        }
        if matches!(character, '\'' | '"') {
            if quote == Some(character) {
                quote = None;
            } else if quote.is_none() {
                quote = Some(character);
            }
            continue;
        }
        if character == '#'
            && quote.is_none()
            && value[..index]
                .chars()
                .next_back()
                .is_some_and(char::is_whitespace)
        {
            return Some(index);
        }
    }
    None
}

fn encode_dotenv_value(value: &str) -> String {
    format!(
        "\"{}\"",
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
            .replace('\r', "\\r")
    )
}

pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    atomic_write_with_mode(path, bytes, None)
}

pub fn atomic_write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    atomic_write_with_mode(path, bytes, Some(0o600))
}

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn atomic_write_with_mode(path: &Path, bytes: &[u8], unix_mode: Option<u32>) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create configuration directory '{}'",
            parent.display()
        )
    })?;
    let (temp, mut file) = create_temp(path)?;
    let result = (|| -> Result<()> {
        file.write_all(bytes)
            .with_context(|| format!("failed to write temporary file '{}'", temp.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to flush temporary file '{}'", temp.display()))?;
        drop(file);

        if let Ok(metadata) = fs::metadata(path) {
            fs::set_permissions(&temp, metadata.permissions())?;
        } else if let Some(mode) = unix_mode {
            set_unix_mode(&temp, mode)?;
        }
        replace_file(&temp, path)?;
        sync_parent(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn create_temp(path: &Path) -> Result<(PathBuf, fs::File)> {
    for _ in 0..100 {
        let temp = unique_temp_path(path);
        match OpenOptions::new().write(true).create_new(true).open(&temp) {
            Ok(file) => return Ok((temp, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to create temporary file '{}'", temp.display())
                });
            }
        }
    }
    bail!(
        "failed to allocate a temporary file beside '{}' after 100 attempts",
        path.display()
    )
}

fn unique_temp_path(path: &Path) -> PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    path.with_file_name(format!(".{name}.{}.{}.tmp", std::process::id(), sequence))
}

#[cfg(unix)]
fn set_unix_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_unix_mode(_path: &Path, _mode: u32) -> Result<()> {
    Ok(())
}

#[cfg(not(windows))]
fn replace_file(temp: &Path, target: &Path) -> Result<()> {
    fs::rename(temp, target)
        .with_context(|| format!("failed to commit configuration '{}'", target.display()))
}

#[cfg(windows)]
fn replace_file(temp: &Path, target: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt as _;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };
    let temp: Vec<u16> = temp.as_os_str().encode_wide().chain(Some(0)).collect();
    let target: Vec<u16> = target.as_os_str().encode_wide().chain(Some(0)).collect();
    let succeeded = unsafe {
        MoveFileExW(
            temp.as_ptr(),
            target.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if succeeded == 0 {
        bail!(
            "failed to commit configuration '{}': {}",
            PathBuf::from(String::from_utf16_lossy(&target[..target.len() - 1])).display(),
            std::io::Error::last_os_error()
        );
    }
    Ok(())
}

#[cfg(unix)]
fn sync_parent(parent: &Path) -> Result<()> {
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent(_parent: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Example {
        server: Server,
        #[serde(default)]
        absent_default: bool,
    }

    #[derive(Serialize)]
    struct Server {
        host: String,
        port: u16,
    }

    #[test]
    fn typed_patch_preserves_comments_order_and_absent_defaults() {
        let source = concat!(
            "# heading\n",
            "future = 42\n\n",
            "[server]\n",
            "host = '127.0.0.1' # keep this spelling\n",
            "# port explanation\n",
            "port = 6342 # keep inline\n",
        );
        let before = Example {
            server: Server {
                host: "127.0.0.1".into(),
                port: 6342,
            },
            absent_default: false,
        };
        let after = Example {
            server: Server {
                host: "127.0.0.1".into(),
                port: 7777,
            },
            absent_default: false,
        };
        let mut document = TomlDocument::parse(source).unwrap();
        document.patch_serialized(&before, &after).unwrap();
        assert_eq!(
            document.to_string(),
            source.replace("port = 6342", "port = 7777")
        );
        assert!(!document.to_string().contains("absent_default"));
    }

    #[test]
    fn whole_value_binding_does_not_claim_embedded_templates() {
        assert_eq!(
            whole_value_env_binding("${PORT:-6342}"),
            Some(EnvBinding {
                variable: "PORT",
                fallback: Some("6342"),
                fallback_on_empty: true,
            })
        );
        assert_eq!(whole_value_env_binding("prefix-${PORT}"), None);
        assert_eq!(whole_value_env_binding("${9BAD}"), None);
    }

    #[test]
    fn dotenv_patch_preserves_comments_order_and_newlines() {
        let mut document = DotenvDocument::parse(
            "# keys\r\nexport TOKEN = old  # account key\r\nOTHER='same'\r\nTOKEN=duplicate\r\n",
        );
        document.apply(
            &BTreeMap::from([
                ("ADDED".into(), "new".into()),
                ("TOKEN".into(), "a\"b".into()),
            ]),
            &BTreeSet::new(),
        );
        assert_eq!(
            document.to_string(),
            "# keys\r\nexport TOKEN =\"a\\\"b\"  # account key\r\nOTHER='same'\r\nADDED=\"new\"\r\n"
        );
    }

    #[test]
    fn atomic_write_replaces_existing_file() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.toml");
        fs::write(&path, "old").unwrap();
        atomic_write(&path, b"new").unwrap();
        assert_eq!(fs::read_to_string(path).unwrap(), "new");
    }
}
