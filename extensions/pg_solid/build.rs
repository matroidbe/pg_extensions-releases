//! Build script for pg_solid.
//!
//! Locates an OpenCASCADE (OCCT) installation, compiles the C++ wrapper against
//! it, and emits the link flags for the toolkits we use.
//!
//! The discovery and toolkit-selection logic lives in `build/occt.rs` so it can
//! be unit-tested (build scripts are not part of cargo's test graph, so tests
//! written here would never run). See that module for the probe order and the
//! OCCT 7.8 DataExchange toolkit rename.

#[path = "build/occt.rs"]
mod occt;

fn main() {
    for var in occt::ENV_VARS {
        println!("cargo:rerun-if-env-changed={var}");
    }
    println!("cargo:rerun-if-changed=src/cpp/occt_wrapper.cpp");
    println!("cargo:rerun-if-changed=build/occt.rs");

    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").ok();
    let occt = occt::Occt::probe(target_arch.as_deref())
        .unwrap_or_else(|| panic!("{}", occt::not_found_message()));

    println!(
        "cargo:warning=pg_solid: building against OCCT {} at {}",
        occt.version_string(),
        occt.include_dir.display()
    );

    // Compile the C++ OCCT wrapper.
    cc::Build::new()
        .cpp(true)
        .file("src/cpp/occt_wrapper.cpp")
        .flag("-std=c++17")
        .include(&occt.include_dir)
        .warnings(false) // OCCT headers produce many warnings
        .compile("occt_wrapper");

    // Tell the linker where the OCCT libraries live. Without this, only
    // installations already on the linker's default search path (i.e. a system
    // prefix) resolve — Homebrew and vendored prefixes would fail to link.
    if let Some(lib_dir) = &occt.lib_dir {
        println!("cargo:rustc-link-search=native={}", lib_dir.display());

        // Bake an rpath for non-system prefixes so the PostgreSQL backend can
        // resolve the OCCT libraries at load time without an ldconfig entry or
        // LD_LIBRARY_PATH. System prefixes are already on the loader's path.
        if !occt::is_system_lib_dir(lib_dir) {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
        }
    }

    for lib in occt::toolkits(occt.version) {
        println!("cargo:rustc-link-lib=dylib={lib}");
    }

    // C++ standard library: libc++ on macOS, libstdc++ elsewhere.
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "macos" {
        println!("cargo:rustc-link-lib=dylib=c++");
    } else {
        println!("cargo:rustc-link-lib=dylib=stdc++");
    }
}
