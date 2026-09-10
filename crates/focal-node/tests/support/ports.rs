//! Loopback port allocation shared by every real-binary test. A port above
//! the ephemeral range is claimed across processes with an exclusive lock
//! file under the temp directory (one `cargo test` runs many test binaries
//! at once, and a port two of them probed free seconds apart was bound by
//! whichever started second and refused for the other), then probed on both
//! protocols so a listener of an unrelated process is skipped as well. A
//! lock older than half an hour belongs to a run that is over and is
//! reclaimed.
#![allow(clippy::arithmetic_side_effects, clippy::panic)]
use std::{
    fs::{self, OpenOptions},
    io::Write as _,
    net::{TcpListener, UdpSocket},
    path::PathBuf,
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const FIRST: u32 = 24_000;
const COUNT: u32 = 8_000;
const STALE: Duration = Duration::from_secs(30 * 60);

fn locks() -> PathBuf {
    let directory = std::env::temp_dir().join("focal-test-ports");
    let _ = fs::create_dir_all(&directory);
    directory
}

fn claim(port: u16) -> bool {
    let lock = locks().join(port.to_string());
    match OpenOptions::new().write(true).create_new(true).open(&lock) {
        Ok(mut file) => {
            let _ = writeln!(file, "{}", std::process::id());
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let stale = fs::metadata(&lock)
                .and_then(|metadata| metadata.modified())
                .ok()
                .and_then(|modified| SystemTime::now().duration_since(modified).ok())
                .is_some_and(|age| age > STALE);
            if stale {
                let _ = fs::remove_file(&lock);
            }
            false
        }
        Err(_) => false,
    }
}

/// A loopback address no other test process of this machine will pick.
pub fn address() -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let seed = std::process::id().wrapping_mul(2_654_435_761)
        ^ SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.subsec_nanos())
            .unwrap_or(0);
    let _ = NEXT.compare_exchange(0, seed.max(1), Ordering::Relaxed, Ordering::Relaxed);
    for _ in 0..COUNT {
        let port = (FIRST + NEXT.fetch_add(1, Ordering::Relaxed) % COUNT) as u16;
        if !claim(port) {
            continue;
        }
        let free = UdpSocket::bind(("127.0.0.1", port)).is_ok()
            && TcpListener::bind(("127.0.0.1", port)).is_ok();
        if free {
            return format!("127.0.0.1:{port}");
        }
        let _ = fs::remove_file(locks().join(port.to_string()));
    }
    panic!("no free loopback port for the real-binary tests")
}
