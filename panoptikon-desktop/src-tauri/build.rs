fn main() {
    println!("cargo:rerun-if-changed=../dist/index.html");
    println!("cargo:rerun-if-changed=../dist/app.js");
    println!("cargo:rerun-if-changed=../dist/styles.css");
    tauri_build::build()
}
