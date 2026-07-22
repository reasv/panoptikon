//! Host tool/asset discovery without `/nix/store` paths in config.
//! Order: config → PATH / Nix profiles → fontconfig / XDG → FHS.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Look up an executable by bare name on `PATH`.
pub fn find_on_path(name: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    for dir in env::split_paths(&path) {
        if let Some(found) = executable_in_dir(&dir, name) {
            return Some(found);
        }
    }
    None
}

/// First of `names` on PATH, then profile `bin/` dirs.
pub fn find_executable(names: &[&str]) -> Option<PathBuf> {
    for name in names {
        if let Some(path) = find_on_path(name) {
            return Some(path);
        }
    }
    for prefix in profile_bin_dirs() {
        for name in names {
            if let Some(found) = executable_in_dir(&prefix, name) {
                return Some(found);
            }
        }
    }
    None
}

fn executable_in_dir(dir: &Path, name: &str) -> Option<PathBuf> {
    let candidate = dir.join(name);
    if is_runnable_file(&candidate) {
        return Some(candidate);
    }
    #[cfg(windows)]
    {
        let with_exe = dir.join(format!("{name}.exe"));
        if is_runnable_file(&with_exe) {
            return Some(with_exe);
        }
    }
    None
}

/// Existing runnable file as-is; bare name (single path component) via PATH/profiles.
pub fn resolve_configured_executable(configured: &Path) -> Option<PathBuf> {
    if is_runnable_file(configured) {
        return Some(configured.to_path_buf());
    }
    if configured.components().count() == 1 {
        if let Some(name) = configured.to_str() {
            return find_executable(&[name]);
        }
    }
    None
}

fn first_existing_file(candidates: &[&str]) -> Option<PathBuf> {
    candidates
        .iter()
        .map(PathBuf::from)
        .find(|path| path.is_file())
}

fn profile_bin_dirs() -> Vec<PathBuf> {
    #[cfg(not(unix))]
    {
        return Vec::new();
    }
    #[cfg(unix)]
    {
        let mut dirs = vec![
            PathBuf::from("/run/current-system/sw/bin"),
            PathBuf::from("/nix/var/nix/profiles/default/bin"),
        ];
        if let Some(home) = env::var_os("HOME") {
            let home = PathBuf::from(home);
            dirs.push(home.join(".nix-profile/bin"));
            dirs.push(home.join(".local/state/nix/profile/bin"));
        }
        if let Ok(user) = env::var("USER") {
            dirs.push(PathBuf::from(format!(
                "/etc/profiles/per-user/{user}/bin"
            )));
        }
        dirs
    }
}

fn is_runnable_file(path: &Path) -> bool {
    let Ok(meta) = fs::metadata(path) else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

/// True if `path` spawns and exits successfully with `args` (rejects stub-ld ELFs).
pub fn can_spawn(path: &Path, args: &[&str]) -> bool {
    if !path.is_file() {
        return false;
    }
    Command::new(path)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

const FONT_FILE_CANDIDATES: &[&str] = &[
    "DejaVuSans.ttf",
    "NotoSans.ttf",
    "NotoSans-Regular.ttf",
    "LiberationSans-Regular.ttf",
    "FreeSans.ttf",
    "Arial.ttf",
    "arial.ttf",
];

const FONT_REL_PATHS: &[&str] = &[
    "fonts/truetype/dejavu/DejaVuSans.ttf",
    "fonts/truetype/DejaVuSans.ttf",
    "fonts/TTF/DejaVuSans.ttf",
    "fonts/dejavu/DejaVuSans.ttf",
    "fonts/noto/NotoSans.ttf",
    "fonts/noto/NotoSans-Regular.ttf",
    "fonts/truetype/liberation/LiberationSans-Regular.ttf",
    "fonts/truetype/LiberationSans-Regular.ttf",
    "fonts/truetype/freefont/FreeSans.ttf",
];

/// Thumbnail label font: fc-match → XDG/profile share → FHS. Config first (caller).
pub fn find_label_font() -> Option<PathBuf> {
    if let Some(path) = fontconfig_match_font() {
        return Some(path);
    }
    for root in font_data_roots() {
        for rel in FONT_REL_PATHS {
            let candidate = root.join(rel);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
        let fonts_dir = root.join("fonts");
        if fonts_dir.is_dir() {
            if let Some(path) = find_named_font_under(&fonts_dir, FONT_FILE_CANDIDATES) {
                return Some(path);
            }
        }
    }
    None
}

fn fontconfig_match_font() -> Option<PathBuf> {
    let queries = [
        "sans",
        "DejaVu Sans",
        "Noto Sans",
        "Liberation Sans",
        "FreeSans",
    ];
    let fc = find_executable(&["fc-match"])?;
    for query in queries {
        let Ok(output) = Command::new(&fc)
            .args(["-f", "%{file}\n", query])
            .stdin(std::process::Stdio::null())
            .output()
        else {
            continue;
        };
        if !output.status.success() {
            continue;
        }
        let path = String::from_utf8_lossy(&output.stdout);
        let Some(path) = path.lines().next().map(str::trim).filter(|p| !p.is_empty()) else {
            continue;
        };
        let path = PathBuf::from(path);
        if !path.is_file() {
            continue;
        }
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        if matches!(ext.as_str(), "ttf" | "otf" | "ttc") {
            return Some(path);
        }
    }
    None
}

fn font_data_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(xdg) = env::var_os("XDG_DATA_DIRS") {
        roots.extend(env::split_paths(&xdg));
    }
    if let Some(home) = env::var_os("HOME") {
        let home = PathBuf::from(home);
        roots.push(home.join(".local/share"));
        #[cfg(unix)]
        {
            roots.push(home.join(".nix-profile/share"));
            roots.push(home.join(".local/state/nix/profile/share"));
        }
    }
    #[cfg(unix)]
    {
        roots.push(PathBuf::from("/run/current-system/sw/share"));
        roots.push(PathBuf::from("/nix/var/nix/profiles/default/share"));
        roots.push(PathBuf::from("/usr/share"));
        roots.push(PathBuf::from("/usr/local/share"));
    }
    roots
}

fn find_named_font_under(dir: &Path, names: &[&str]) -> Option<PathBuf> {
    const MAX_DEPTH: u32 = 6;
    const MAX_VISITS: u32 = 4000;
    let mut stack = vec![(dir.to_path_buf(), 0u32)];
    let mut visits = 0u32;
    while let Some((current, depth)) = stack.pop() {
        visits += 1;
        if visits > MAX_VISITS {
            break;
        }
        let Ok(entries) = fs::read_dir(&current) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if names.contains(&name) {
                    return Some(path);
                }
            } else if depth < MAX_DEPTH && path.is_dir() {
                stack.push((path, depth + 1));
            }
        }
    }
    None
}

/// Headless Chromium-family browser for HTML thumbnails.
pub fn find_html_renderer() -> Option<PathBuf> {
    const NAMES: &[&str] = &[
        "chromium",
        "chromium-browser",
        "google-chrome",
        "google-chrome-stable",
        "msedge",
        "microsoft-edge",
    ];
    if let Some(path) = find_executable(NAMES) {
        return Some(path);
    }
    #[cfg(windows)]
    {
        return first_existing_file(&[
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        ]);
    }
    #[cfg(unix)]
    {
        first_existing_file(&[
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
        ])
    }
    #[cfg(not(any(unix, windows)))]
    {
        None
    }
}

/// pypdfium2's `libpdfium` under managed venvs (relative to CWD / `--root`).
pub fn find_pdfium_in_venvs() -> Option<PathBuf> {
    let lib_name = if cfg!(windows) {
        "pdfium.dll"
    } else if cfg!(target_os = "macos") {
        "libpdfium.dylib"
    } else {
        "libpdfium.so"
    };
    let mut site_packages: Vec<PathBuf> = Vec::new();
    for venv in [
        PathBuf::from("python/.venv"),
        PathBuf::from("runtime/venv"),
        PathBuf::from(".venv"),
    ] {
        if cfg!(windows) {
            site_packages.push(venv.join("Lib/site-packages"));
        } else {
            let lib = venv.join("lib");
            let Ok(entries) = fs::read_dir(&lib) else {
                continue;
            };
            for entry in entries.flatten() {
                site_packages.push(entry.path().join("site-packages"));
            }
        }
    }
    for site in site_packages {
        let candidate = site.join("pypdfium2_raw").join(lib_name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_on_path_finds_something_common() {
        #[cfg(unix)]
        {
            assert!(find_on_path("true").is_some() || find_on_path("sh").is_some());
        }
        #[cfg(windows)]
        {
            assert!(find_on_path("cmd").is_some());
        }
    }

    #[test]
    fn can_spawn_rejects_missing() {
        assert!(!can_spawn(Path::new("/nonexistent/binary-xyz"), &["-version"]));
    }

    #[test]
    fn resolve_configured_executable_bare_name() {
        #[cfg(unix)]
        {
            let path = resolve_configured_executable(Path::new("true"))
                .or_else(|| resolve_configured_executable(Path::new("sh")));
            assert!(path.is_some());
        }
        #[cfg(windows)]
        {
            assert!(resolve_configured_executable(Path::new("cmd")).is_some());
        }
        assert!(resolve_configured_executable(Path::new("no-such-panoptikon-tool-xyz")).is_none());
    }

    #[test]
    fn find_label_font_works_when_fontconfig_is_present() {
        if find_executable(&["fc-match"]).is_none() {
            return;
        }
        let path = find_label_font().expect("fontconfig hosts should resolve a sans font");
        assert!(path.is_file(), "{path:?}");
    }
}
