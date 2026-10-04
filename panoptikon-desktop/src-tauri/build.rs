use std::{env, fs};

fn main() {
    println!("cargo:rerun-if-changed=../dist/index.html");
    println!("cargo:rerun-if-changed=../dist/app.js");
    println!("cargo:rerun-if-changed=../dist/styles.css");
    println!("cargo:rerun-if-changed=../dist/launch.html");
    println!("cargo:rerun-if-changed=../dist/launch.js");
    println!("cargo:rerun-if-changed=../dist/launch.css");
    println!("cargo:rerun-if-changed=../dist/update.html");
    println!("cargo:rerun-if-changed=../dist/update.js");
    println!("cargo:rerun-if-changed=../dist/update.css");
    println!("cargo:rerun-if-changed=../dist/spinner_text.svg");
    println!("cargo:rerun-if-changed=../dist/pairing.html");
    println!("cargo:rerun-if-changed=../dist/pairing.js");
    println!("cargo:rerun-if-changed=../dist/pairing.css");
    println!("cargo:rerun-if-changed=../dist/mapping.html");
    println!("cargo:rerun-if-changed=../dist/mapping.js");
    println!("cargo:rerun-if-changed=../dist/mapping.css");
    check_bundled_files();
    tauri_build::build()
}

/// tauri.conf.json bundles the Server sidecar and the PDFium library, both
/// gitignored and staged before a release build. A plain cargo build leaves
/// whichever is missing or empty out of the bundle config (unless TAURI_CONFIG is set);
/// a build with tauri/custom-protocol (`tauri build`) refuses a missing or empty file.
fn check_bundled_files() {
    let target = env::var("TARGET").unwrap();
    let (exe, pdfium) = if target.contains("windows") {
        (".exe", "pdfium.dll")
    } else if target.contains("apple") {
        ("", "libpdfium.dylib")
    } else {
        ("", "libpdfium.so")
    };
    let files = [
        (
            format!("binaries/panoptikon-{target}{exe}"),
            r#""externalBin":null"#,
        ),
        (
            format!("resources/pdfium/{pdfium}"),
            r#""resources":null,"macOS":{"frameworks":null}"#,
        ),
    ];
    let mut unstaged = Vec::new();
    for (file, keys) in &files {
        println!("cargo:rerun-if-changed={file}");
        let staged = fs::metadata(file).is_ok_and(|meta| meta.len() > 0);
        assert!(
            staged || tauri_build::is_dev(),
            "{file} is missing or empty: stage the real file before `tauri build`"
        );
        if !staged {
            unstaged.push(*keys);
        }
    }
    if !unstaged.is_empty() && env::var_os("TAURI_CONFIG").is_none() {
        let config = format!(r#"{{"bundle":{{{}}}}}"#, unstaged.join(","));
        // SAFETY: the build script is single-threaded here.
        unsafe { env::set_var("TAURI_CONFIG", config) };
    }
}
