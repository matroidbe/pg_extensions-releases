fn main() {
    // Allow unresolved PostgreSQL server symbols in the test binary.
    // pgrx extensions are loaded into the PostgreSQL server process which
    // provides symbols like SPI_connect at runtime via dlopen().
    //
    // `--unresolved-symbols` is a GNU ld / lld flag that Apple's ld64 rejects; on
    // macOS the workspace .cargo/config.toml passes `-undefined dynamic_lookup`,
    // which does the same job.
    if std::env::var("CARGO_CFG_TARGET_VENDOR").as_deref() != Ok("apple") {
        println!("cargo:rustc-link-arg=-Wl,--unresolved-symbols=ignore-all");
    }
}
