//! Build script for the embedded-resource features (docs/architecture.md
//! "Self-contained releases").
//!
//! - `bundled`: packages the Python source set (`python/inferio_worker`,
//!   `python/inferio`, `pyproject.toml`, `uv.lock` — excluding tests,
//!   `.venv`, and bytecode caches) into `$OUT_DIR/pysrc.tar.gz` for
//!   `include_bytes!` in `src/resources.rs`. The default config TOMLs are
//!   embedded directly with `include_str!` (rustc tracks those itself).
//! - `bundled-ui`: packages the Next.js standalone output directory named
//!   by the `PANOPTIKON_UI_BUNDLE` env var into `$OUT_DIR/ui_bundle.tar.gz`
//!   and sets `cfg(ui_bundle_present)`. When the variable is unset or does
//!   not look like a standalone output (no `server.js`), the cfg stays off
//!   and a `compile_error!` in `src/resources.rs` names the problem.
//!
//! Archives are deterministic (sorted entries, zeroed mtime/uid/gid, fixed
//! modes) so the runtime marker hash only changes when content changes.
//!
//! With neither feature enabled this script only emits cfg/rerun
//! directives — plain dev builds embed nothing.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    // Declared unconditionally so `cfg(ui_bundle_present)` never triggers
    // the unexpected-cfg lint, whichever features are enabled.
    println!("cargo::rustc-check-cfg=cfg(ui_bundle_present)");
    println!("cargo::rerun-if-env-changed=PANOPTIKON_UI_BUNDLE");

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("cargo sets OUT_DIR"));
    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .expect("the panoptikon crate lives one level under the workspace root")
        .to_path_buf();

    if env::var_os("CARGO_FEATURE_BUNDLED").is_some() {
        build_pysrc_archive(&repo_root.join("python"), &out_dir.join("pysrc.tar.gz"));
    }
    if env::var_os("CARGO_FEATURE_BUNDLED_UI").is_some() {
        build_ui_archive(&out_dir.join("ui_bundle.tar.gz"));
    }
}

/// The embedded Python source set: `pyproject.toml` + `uv.lock` at the
/// archive root plus the `inferio_worker` and `inferio` trees (impl classes,
/// built-in registry TOMLs). `tests`, `.venv`, `__pycache__`, and `*.pyc`
/// never ship.
fn build_pysrc_archive(python_dir: &Path, target: &Path) {
    // Watch only the shipped subset: python/ itself also holds the managed
    // .venv (gigabytes, touched constantly at runtime).
    for watched in ["pyproject.toml", "uv.lock", "inferio", "inferio_worker"] {
        println!(
            "cargo::rerun-if-changed={}",
            python_dir.join(watched).display()
        );
    }
    let mut entries: Vec<(String, PathBuf)> = vec![
        ("pyproject.toml".into(), python_dir.join("pyproject.toml")),
        ("uv.lock".into(), python_dir.join("uv.lock")),
    ];
    for dir in ["inferio_worker", "inferio"] {
        collect_python_files(&python_dir.join(dir), dir, &mut entries);
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    write_tar_gz(target, &entries);
}

/// Recursively collect the shippable files under one Python source dir.
fn collect_python_files(dir: &Path, rel: &str, entries: &mut Vec<(String, PathBuf)>) {
    let read = fs::read_dir(dir)
        .unwrap_or_else(|err| panic!("bundled: failed to read '{}': {err}", dir.display()));
    for entry in read {
        let entry = entry
            .unwrap_or_else(|err| panic!("bundled: failed to list '{}': {err}", dir.display()));
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = entry.path();
        // fs::metadata follows symlinks, so a linked file ships by content.
        let meta = fs::metadata(&path)
            .unwrap_or_else(|err| panic!("bundled: failed to stat '{}': {err}", path.display()));
        if meta.is_dir() {
            if matches!(name.as_str(), "__pycache__" | ".venv" | "tests") {
                continue;
            }
            collect_python_files(&path, &format!("{rel}/{name}"), entries);
        } else if meta.is_file() && !name.ends_with(".pyc") {
            entries.push((format!("{rel}/{name}"), path));
        }
    }
}

/// The embedded UI bundle: the entire directory named by
/// `PANOPTIKON_UI_BUNDLE`, which must be a *fully assembled* Next.js
/// standalone output — `server.js` + `node_modules` at the root, with the
/// build's `.next/static` (and `public/`, if any) already copied in.
fn build_ui_archive(target: &Path) {
    let Some(bundle) = env::var_os("PANOPTIKON_UI_BUNDLE") else {
        println!(
            "cargo::warning=feature `bundled-ui` is enabled but PANOPTIKON_UI_BUNDLE is not \
             set; the build will fail (set it to a Next.js standalone output directory)"
        );
        return;
    };
    let bundle = PathBuf::from(bundle);
    // Watch the bundle BEFORE validating it: fixing the directory contents
    // (adding the missing server.js / .next/static) must re-run this script
    // and clear the failure without needing to touch the env var value.
    println!("cargo::rerun-if-changed={}", bundle.display());
    println!(
        "cargo::rerun-if-changed={}",
        bundle.join("server.js").display()
    );
    if !bundle.join("server.js").is_file() {
        println!(
            "cargo::warning=PANOPTIKON_UI_BUNDLE='{}' is not a Next.js standalone output \
             directory (no server.js in it); the build will fail",
            bundle.display()
        );
        return;
    }
    if !bundle.join(".next").join("static").is_dir() {
        // Hard error: a mis-assembled CI bundle (standalone output without
        // the static assets copied in) would ship a UI without JS/CSS.
        panic!(
            "PANOPTIKON_UI_BUNDLE='{}' has no .next/static — a Next.js standalone output \
             is only complete once the build's .next/static (and public/, if any) are \
             copied into it; refusing to embed a broken UI bundle",
            bundle.display()
        );
    }
    let mut entries = Vec::new();
    collect_all_files(&bundle, "", &mut entries);
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    write_tar_gz(target, &entries);
    println!("cargo::rustc-cfg=ui_bundle_present");
}

/// Recursively collect every file under the UI bundle (no excludes: the
/// standalone output is already a curated file set).
fn collect_all_files(dir: &Path, rel: &str, entries: &mut Vec<(String, PathBuf)>) {
    let read = fs::read_dir(dir)
        .unwrap_or_else(|err| panic!("bundled-ui: failed to read '{}': {err}", dir.display()));
    for entry in read {
        let entry = entry
            .unwrap_or_else(|err| panic!("bundled-ui: failed to list '{}': {err}", dir.display()));
        let name = entry.file_name().to_string_lossy().into_owned();
        let child_rel = if rel.is_empty() {
            name.clone()
        } else {
            format!("{rel}/{name}")
        };
        let path = entry.path();
        let meta = fs::metadata(&path)
            .unwrap_or_else(|err| panic!("bundled-ui: failed to stat '{}': {err}", path.display()));
        if meta.is_dir() {
            collect_all_files(&path, &child_rel, entries);
        } else if meta.is_file() {
            entries.push((child_rel, path));
        }
    }
}

/// Write a deterministic tar.gz: entries in the given (sorted) order, mtime
/// 0, uid/gid 0, mode 0644. Parent directories are implicit — the tar crate
/// recreates them on unpack.
fn write_tar_gz(target: &Path, entries: &[(String, PathBuf)]) {
    let file = fs::File::create(target)
        .unwrap_or_else(|err| panic!("failed to create '{}': {err}", target.display()));
    let gz = flate2::write::GzEncoder::new(file, flate2::Compression::default());
    let mut tar = tar::Builder::new(gz);
    for (name, path) in entries {
        let data = fs::read(path)
            .unwrap_or_else(|err| panic!("failed to read '{}': {err}", path.display()));
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_mtime(0);
        header.set_uid(0);
        header.set_gid(0);
        tar.append_data(&mut header, name, data.as_slice())
            .unwrap_or_else(|err| {
                panic!("failed to append '{name}' to '{}': {err}", target.display())
            });
    }
    let gz = tar
        .into_inner()
        .unwrap_or_else(|err| panic!("failed to finish '{}': {err}", target.display()));
    gz.finish()
        .unwrap_or_else(|err| panic!("failed to finish '{}': {err}", target.display()));
}
