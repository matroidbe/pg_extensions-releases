fn main() {
    // Allow unresolved PostgreSQL server symbols in the test binary.
    // pgrx extensions are loaded into the PostgreSQL server process which
    // provides symbols like SPI_connect at runtime via dlopen().
    println!("cargo:rustc-link-arg=-Wl,--unresolved-symbols=ignore-all");
}
