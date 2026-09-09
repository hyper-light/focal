//! A lock belongs to one scoped owner, not to the last inherited descriptor.
//! Process creation can duplicate an open file description before close-on-exec
//! runs. Closing only the owner's File would leave its flock held by that copy.
use fs2::FileExt;
use std::{fs::File, io};

/// Deliberately neither Clone nor an owning File conversion: the critical
/// section has exactly one release authority. Accessors borrow its descriptor
/// for marker IO; production callers must not create another lock owner from it.
pub(crate) struct FileLock {
    file: File,
}
impl FileLock {
    pub(crate) fn acquire(file: File) -> io::Result<Self> {
        file.try_lock_exclusive()?;
        // Construct immediately after acquisition so subsequent initialization
        // errors release the lock as well as normal owner completion.
        Ok(Self { file })
    }
    /// Acquire, waiting up to `wait` for another owner's short critical
    /// section to end; a lock still held afterwards is `WouldBlock`.
    pub(crate) fn acquire_within(file: File, wait: std::time::Duration) -> io::Result<Self> {
        let deadline = std::time::Instant::now().checked_add(wait);
        loop {
            match file.try_lock_exclusive() {
                Ok(()) => return Ok(Self { file }),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if deadline.is_none_or(|deadline| std::time::Instant::now() >= deadline) {
                        return Err(error);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
                Err(error) => return Err(error),
            }
        }
    }
    pub(crate) fn file(&self) -> &File {
        &self.file
    }
}
impl Drop for FileLock {
    fn drop(&mut self) {
        loop {
            match FileExt::unlock(&self.file) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                // Drop cannot report IO errors or panic. File's close remains
                // the fallback; acquisition and durability errors retain their
                // existing typed paths and are never replaced here.
                _ => break,
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn failed_initialization_releases_scoped_lock_while_duplicate_stays_open() {
        fn fail_after_acquisition(
            path: &std::path::Path,
            inherited: &mut Option<File>,
        ) -> io::Result<()> {
            let file = File::options().read(true).write(true).open(path)?;
            let lock = FileLock::acquire(file)?;
            *inherited = Some(lock.file().try_clone()?);
            Err(io::Error::other("injected initialization failure"))
        }
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("LOCK");
        File::create(&path).unwrap();
        let mut inherited = None;
        assert!(fail_after_acquisition(&path, &mut inherited).is_err());
        let duplicate = inherited.unwrap();
        duplicate.metadata().unwrap();
        let owner =
            FileLock::acquire(File::options().read(true).write(true).open(&path).unwrap()).unwrap();
        // Refusing a second owner must not unlock the existing owner's lock.
        for _ in 0..2 {
            let other = File::options().read(true).write(true).open(&path).unwrap();
            assert!(
                matches!(FileLock::acquire(other), Err(error) if error.kind() == io::ErrorKind::WouldBlock)
            );
        }
        drop(duplicate);
        let other = File::options().read(true).write(true).open(&path).unwrap();
        assert!(
            matches!(FileLock::acquire(other), Err(error) if error.kind() == io::ErrorKind::WouldBlock)
        );
        drop(owner);
        FileLock::acquire(File::options().read(true).write(true).open(&path).unwrap()).unwrap();
    }
}
