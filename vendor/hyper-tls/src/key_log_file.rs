use alloc::vec::Vec;
use core::fmt::{Debug, Formatter};
use std::env::var_os;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use crate::log::warn;
use crate::KeyLog;

// Internal mutable state for KeyLogFile
struct KeyLogFileInner {
    file: Option<File>,
    buf: Vec<u8>,
}

impl KeyLogFileInner {
    fn new(var: Option<OsString>) -> Self {
        let Some(path) = &var else {
            return Self {
                file: None,
                buf: Vec::new(),
            };
        };

        let mut options = OpenOptions::new();
        options.append(true).create(true);
        // Key material is extremely sensitive. On Unix, create with owner-only
        // access so a default umask does not leave the file world-readable.
        #[cfg(unix)]
        options.mode(0o600);

        let file = match options.open(path) {
            Ok(f) => Some(f),
            Err(e) => {
                warn!("unable to create key log file {path:?}: {e}");
                None
            }
        };

        Self {
            file,
            buf: Vec::new(),
        }
    }

    fn try_write(&mut self, label: &str, client_random: &[u8], secret: &[u8]) -> io::Result<()> {
        let Some(file) = &mut self.file else {
            return Ok(());
        };

        self.buf.clear();
        write!(self.buf, "{label} ")?;
        for b in client_random.iter() {
            write!(self.buf, "{b:02x}")?;
        }
        write!(self.buf, " ")?;
        for b in secret.iter() {
            write!(self.buf, "{b:02x}")?;
        }
        writeln!(self.buf)?;
        file.write_all(&self.buf)
    }
}

impl Debug for KeyLogFileInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("KeyLogFileInner")
            // Note: we omit self.buf deliberately as it may contain key data.
            .field("file", &self.file)
            .finish()
    }
}

/// [`KeyLog`] implementation that opens a file whose name is
/// given by the `SSLKEYLOGFILE` environment variable, and writes
/// keys into it.
///
/// If `SSLKEYLOGFILE` is not set, this does nothing.
///
/// If such a file cannot be opened, or cannot be written then
/// this does nothing but logs errors at warning-level.
pub struct KeyLogFile(KeyLogFileInner);

impl KeyLogFile {
    /// Makes a new `KeyLogFile`.  The environment variable is
    /// inspected and the named file is opened during this call.
    #[allow(
        clippy::disallowed_methods,
        reason = "SSLKEYLOGFILE, read only when an owner installs KeyLogFile (rustls; VENDORED.md)"
    )]
    pub fn new() -> Self {
        let var = var_os("SSLKEYLOGFILE");
        Self(KeyLogFileInner::new(var))
    }
}

impl KeyLog for KeyLogFile {
    fn log(&mut self, label: &str, client_random: &[u8], secret: &[u8]) {
        match self.0.try_write(label, client_random, secret) {
            Ok(()) => {}
            Err(e) => {
                warn!("error writing to key log file: {e}");
            }
        }
    }
}

impl Debug for KeyLogFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::{env, format, fs, process};

    use super::*;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn test_env_var_is_not_set() {
        init();
        let mut inner = KeyLogFileInner::new(None);
        assert!(inner.try_write("label", b"random", b"secret").is_ok());
    }

    #[test]
    fn test_env_var_cannot_be_opened() {
        init();
        let mut inner = KeyLogFileInner::new(Some("/dev/does-not-exist".into()));
        assert!(inner.try_write("label", b"random", b"secret").is_ok());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_env_var_cannot_be_written() {
        init();
        let mut inner = KeyLogFileInner::new(Some("/dev/full".into()));
        assert!(inner.try_write("label", b"random", b"secret").is_err());
    }

    #[test]
    fn test_created_file_has_owner_only_permissions() {
        let path = env::temp_dir().join(format!(
            "rustls-keylog-perm-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_file(&path);

        let inner = KeyLogFileInner::new(Some(path.clone().into()));
        assert!(inner.file.is_some(), "key log file should open");

        let mode = fs::metadata(&path).expect("metadata").permissions().mode() & 0o777;
        let _ = fs::remove_file(&path);

        assert_eq!(
            mode, 0o600,
            "SSLKEYLOGFILE must be created with mode 0o600, got {mode:#o}"
        );
    }
}
