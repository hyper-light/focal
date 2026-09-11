//! Cross-platform private files and directories (decisions 10 and 11, doc 20).
//! A private path is owned by the current user and reachable by no one else.
//! On Unix that is `0700`/`0600` with an owner match; on Windows it is an
//! owner-only DACL set at creation with an owner (SID) match on access. The
//! callers express intent through these functions and never a platform call
//! of their own.
use std::{
    fs::{File, Metadata},
    io,
    path::Path,
};

/// The identity of a file owner: a uid on Unix, a security identifier on
/// Windows. Comparable so callers can require two paths share an owner or an
/// owner matches the current user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owner(OwnerRepr);
#[derive(Debug, Clone, PartialEq, Eq)]
enum OwnerRepr {
    #[cfg(unix)]
    Uid(u32),
    #[cfg(windows)]
    Sid(Vec<u8>),
    #[cfg(not(any(unix, windows)))]
    Unsupported,
}

/// A file's identity on its volume: the same file opened twice compares equal,
/// a replacement does not. `(device, inode)` on Unix; `(volume serial, file
/// index)` on Windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileId {
    pub volume: u64,
    pub index: u64,
}

/// The current process user.
pub fn current_owner() -> io::Result<Owner> {
    #[cfg(unix)]
    {
        Ok(Owner(OwnerRepr::Uid(rustix::process::getuid().as_raw())))
    }
    #[cfg(windows)]
    {
        crate::windows::current_owner().map(|sid| Owner(OwnerRepr::Sid(sid)))
    }
    #[cfg(not(any(unix, windows)))]
    {
        Ok(Owner(OwnerRepr::Unsupported))
    }
}

/// The owner of an existing path (not following a final symlink on Unix).
pub fn owner_at(path: &Path) -> io::Result<Owner> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(Owner(OwnerRepr::Uid(
            std::fs::symlink_metadata(path)?.uid(),
        )))
    }
    #[cfg(windows)]
    {
        crate::windows::owner_at(path).map(|sid| Owner(OwnerRepr::Sid(sid)))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Ok(Owner(OwnerRepr::Unsupported))
    }
}

/// Whether a directory at `path` is private: a real directory (not a symlink)
/// reachable only by its owner. Returns its owner when it is.
pub fn private_dir_owner(path: &Path) -> io::Result<Option<Owner>> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir() || accessible_by_others(&metadata, path)? {
        return Ok(None);
    }
    Ok(Some(owner_from(&metadata, path)?))
}

/// A directory `path` must not be writable by group or other (the containing
/// directory of a socket or a private file). On Windows, that it is owned by
/// the current user.
pub fn dir_not_writable_by_others(path: &Path) -> io::Result<bool> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir() {
        return Ok(false);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let _ = path;
        Ok(metadata.mode() & 0o022 == 0)
    }
    #[cfg(not(unix))]
    {
        Ok(owner_from(&metadata, path)? == current_owner()?)
    }
}

/// A regular file at `path` owned by `owner`, reachable by no one else, with
/// exactly `links` hard links. The private-file check the stores rely on.
pub fn check_private_file(path: &Path, owner: &Owner, links: u64) -> io::Result<bool> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file()
        || owner_from(&metadata, path)? != *owner
        || accessible_by_others(&metadata, path)?
        || hard_link_count(&metadata, path)? != links
    {
        return Ok(false);
    }
    Ok(true)
}

/// The open `file` at `path` is the same regular file the path names, owned by
/// `owner`, private, singly linked. Guards against a swap between the check
/// and the open.
pub fn check_open_private_file(path: &Path, file: &File, owner: &Owner) -> io::Result<bool> {
    check_open_private_file_links(path, file, owner, 1)
}

/// As [check_open_private_file], but requiring exactly `links` hard links — the
/// atomic link-pair publication (temp + target sharing one inode) checks for
/// two.
pub fn check_open_private_file_links(
    path: &Path,
    file: &File,
    owner: &Owner,
    links: u64,
) -> io::Result<bool> {
    if !check_private_file(path, owner, links)? {
        return Ok(false);
    }
    let path_id = file_id_at(path)?;
    let open = file.metadata()?;
    let open_id = file_id(file)?;
    if path_id != open_id
        || !open.is_file()
        || !open_owner(file, owner)?
        || open_accessible_by_others(file, path)?
        || hard_link_count_open(file)? != links
    {
        return Ok(false);
    }
    Ok(true)
}

/// The volume identity of an open file (`(device, inode)` on Unix, `(volume,
/// file index)` on Windows). Two handles or paths to the same file compare
/// equal; a replacement compares unequal.
pub fn file_identity(file: &File) -> io::Result<FileId> {
    file_id(file)
}
/// The volume identity of the file a path names (final symlink not followed).
pub fn path_identity(path: &Path) -> io::Result<FileId> {
    file_id_at(path)
}
/// The number of hard links to the file a path names — two while an atomic
/// link-pair publication (temp linked to target) is mid-flight.
pub fn path_hard_link_count(path: &Path) -> io::Result<u64> {
    let metadata = std::fs::symlink_metadata(path)?;
    hard_link_count(&metadata, path)
}

/// Open a file with owner-only permissions when it is created (`0600` on Unix,
/// an owner-only DACL on Windows).
pub fn open_private(path: &Path, read: bool, write: bool, create: bool) -> io::Result<File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut options = std::fs::OpenOptions::new();
        options.read(read).write(write).create(create).mode(0o600);
        options.open(path)
    }
    #[cfg(windows)]
    {
        crate::windows::open_private(path, read, write, create)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (read, write, create);
        let _ = path;
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }
}

/// Create a *new* owner-only file (`0600` on Unix, an owner-only DACL on
/// Windows), failing with `io::ErrorKind::AlreadyExists` when the path exists.
/// The no-clobber counterpart of [open_private] for atomic publications.
pub fn create_private_new(path: &Path, read: bool, write: bool) -> io::Result<File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(read)
            .write(write)
            .create_new(true)
            .mode(0o600)
            .open(path)
    }
    #[cfg(windows)]
    {
        crate::windows::create_private_new(path, read, write)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (read, write);
        let _ = path;
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }
}

/// Create a directory owner-only (`0700` on Unix, an owner-only DACL on
/// Windows). Errors if it already exists.
pub fn create_dir_private(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new().mode(0o700).create(path)
    }
    #[cfg(windows)]
    {
        crate::windows::create_dir_private(path)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }
}

/// Atomically replace `to` with `from` (both on the same volume).
pub fn atomic_replace(from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        crate::windows::move_replace(from, to)
    }
    #[cfg(not(windows))]
    {
        std::fs::rename(from, to)
    }
}

// ---- Unix-native helpers ---------------------------------------------------

#[cfg(unix)]
fn accessible_by_others(metadata: &Metadata, _path: &Path) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt;
    Ok(metadata.mode() & 0o077 != 0)
}
#[cfg(unix)]
fn owner_from(metadata: &Metadata, _path: &Path) -> io::Result<Owner> {
    use std::os::unix::fs::MetadataExt;
    Ok(Owner(OwnerRepr::Uid(metadata.uid())))
}
#[cfg(unix)]
fn hard_link_count(metadata: &Metadata, _path: &Path) -> io::Result<u64> {
    use std::os::unix::fs::MetadataExt;
    Ok(metadata.nlink())
}
#[cfg(unix)]
fn hard_link_count_open(file: &File) -> io::Result<u64> {
    use std::os::unix::fs::MetadataExt;
    Ok(file.metadata()?.nlink())
}
#[cfg(unix)]
fn file_id(file: &File) -> io::Result<FileId> {
    use std::os::unix::fs::MetadataExt;
    let metadata = file.metadata()?;
    Ok(FileId {
        volume: metadata.dev(),
        index: metadata.ino(),
    })
}
#[cfg(unix)]
fn file_id_at(path: &Path) -> io::Result<FileId> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::symlink_metadata(path)?;
    Ok(FileId {
        volume: metadata.dev(),
        index: metadata.ino(),
    })
}
#[cfg(unix)]
fn open_owner(file: &File, owner: &Owner) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt;
    Ok(Owner(OwnerRepr::Uid(file.metadata()?.uid())) == *owner)
}
#[cfg(unix)]
fn open_accessible_by_others(file: &File, _path: &Path) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt;
    Ok(file.metadata()?.mode() & 0o077 != 0)
}

// ---- Windows delegates ------------------------------------------------------

#[cfg(windows)]
fn accessible_by_others(metadata: &Metadata, path: &Path) -> io::Result<bool> {
    let _ = metadata;
    // A path we own (owner-only DACL at creation) is private; anything else
    // is refused. Owner match is the access check on Windows.
    Ok(owner_at(path)? != current_owner()?)
}
#[cfg(windows)]
fn owner_from(_metadata: &Metadata, path: &Path) -> io::Result<Owner> {
    owner_at(path)
}
#[cfg(windows)]
fn hard_link_count(_metadata: &Metadata, path: &Path) -> io::Result<u64> {
    crate::windows::hard_link_count_at(path)
}
#[cfg(windows)]
fn hard_link_count_open(file: &File) -> io::Result<u64> {
    crate::windows::hard_link_count_open(file)
}
#[cfg(windows)]
fn file_id(file: &File) -> io::Result<FileId> {
    crate::windows::file_id_open(file)
}
#[cfg(windows)]
fn file_id_at(path: &Path) -> io::Result<FileId> {
    crate::windows::file_id_at(path)
}
#[cfg(windows)]
fn open_owner(_file: &File, _owner: &Owner) -> io::Result<bool> {
    // The path owner was already verified by check_private_file, and the
    // file-id comparison in check_open_private_file proves the open handle is
    // that same file, so the open handle's owner needs no separate query.
    Ok(true)
}
#[cfg(windows)]
fn open_accessible_by_others(_file: &File, path: &Path) -> io::Result<bool> {
    Ok(owner_at(path)? != current_owner()?)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn a_private_file_passes_and_a_group_readable_one_fails() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let owner = current_owner().unwrap();
        let path = dir.path().join("f");
        let file = open_private(&path, true, true, true).unwrap();
        assert!(check_open_private_file(&path, &file, &owner).unwrap());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(!check_private_file(&path, &owner, 1).unwrap());
    }

    #[test]
    fn a_private_dir_is_recognised_and_a_loose_one_is_not() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d");
        create_dir_private(&path).unwrap();
        assert!(private_dir_owner(&path).unwrap().is_some());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(private_dir_owner(&path).unwrap().is_none());
    }
}
