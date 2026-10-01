//! Runs the build script's OCCT discovery tests.
//!
//! `build/occt.rs` is compiled by `build.rs`, which cargo does not include in
//! the test graph — unit tests living there would silently never run. Pulling
//! the module in here compiles it into a normal test target so `cargo test`
//! executes its `#[cfg(test)]` block.
//!
//! Everything under test is pure (string and path handling); nothing here needs
//! PostgreSQL or an OCCT installation.

#[path = "../build/occt.rs"]
mod occt;
