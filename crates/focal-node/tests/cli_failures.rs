#![cfg(unix)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::{os::fd::OwnedFd, os::unix::net::UnixStream, process::Command};

#[test]
fn closed_stdout_returns_an_io_error_without_panicking() {
    // A disconnected socket makes the first write fail deterministically,
    // independent of whether parent or child is scheduled first after spawn.
    let (output, reader) = UnixStream::pair().unwrap();
    drop(reader);
    let output: OwnedFd = output.into();
    let result = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["deployment", "schema"])
        .stdout(output)
        .output()
        .unwrap();
    assert_eq!(result.status.code(), Some(1));
    let diagnostic = String::from_utf8_lossy(&result.stderr);
    assert!(diagnostic.contains("focal:"), "{diagnostic}");
    assert!(!diagnostic.contains("panicked"), "{diagnostic}");
}
