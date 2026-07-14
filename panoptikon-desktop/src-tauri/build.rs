fn main() {
    println!("cargo:rerun-if-changed=../dist/index.html");
    println!("cargo:rerun-if-changed=../dist/app.js");
    println!("cargo:rerun-if-changed=../dist/styles.css");
    println!("cargo:rerun-if-changed=../dist/launch.html");
    println!("cargo:rerun-if-changed=../dist/launch.js");
    println!("cargo:rerun-if-changed=../dist/launch.css");
    println!("cargo:rerun-if-changed=../dist/spinner_text.svg");
    tauri_build::build()
}
