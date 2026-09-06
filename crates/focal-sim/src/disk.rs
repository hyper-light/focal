use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DiskError {
    #[error("file does not exist")]
    Missing,
    #[error("disk capacity exhausted")]
    Full,
    #[error("invalid path or operation")]
    Invalid,
    #[error("injected IO failure at operation {0}")]
    Injected(u64),
}

#[derive(Debug, Clone, Default)]
struct Inode {
    live: Vec<u8>,
    durable: Vec<u8>,
}

/// Models file data and directory-entry durability independently. A file sync does
/// not persist its newly created name; a directory sync does not persist file bytes.
#[derive(Debug, Clone)]
pub struct Disk {
    names: BTreeMap<PathBuf, u64>,
    durable_names: BTreeMap<PathBuf, u64>,
    inodes: BTreeMap<u64, Inode>,
    next_inode: u64,
    max_bytes: usize,
    operation: u64,
    fail_before: Option<u64>,
}

impl Disk {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            names: BTreeMap::new(),
            durable_names: BTreeMap::new(),
            inodes: BTreeMap::new(),
            next_inode: 0,
            max_bytes,
            operation: 0,
            fail_before: None,
        }
    }

    pub fn fail_before(&mut self, operation: Option<u64>) {
        self.fail_before = operation;
    }

    fn tick(&mut self) -> Result<(), DiskError> {
        self.operation = self.operation.checked_add(1).ok_or(DiskError::Invalid)?;
        if self.fail_before == Some(self.operation) {
            return Err(DiskError::Injected(self.operation));
        }
        Ok(())
    }

    pub fn create(&mut self, path: impl AsRef<Path>) -> Result<(), DiskError> {
        self.tick()?;
        let path = path.as_ref();
        if self.names.contains_key(path) || path.parent().is_none() {
            return Err(DiskError::Invalid);
        }
        let id = self.next_inode;
        self.next_inode = id.checked_add(1).ok_or(DiskError::Invalid)?;
        self.names.insert(path.to_owned(), id);
        self.inodes.insert(id, Inode::default());
        Ok(())
    }

    pub fn write(
        &mut self,
        path: impl AsRef<Path>,
        offset: usize,
        bytes: &[u8],
    ) -> Result<(), DiskError> {
        self.tick()?;
        let id = *self.names.get(path.as_ref()).ok_or(DiskError::Missing)?;
        let end = offset.checked_add(bytes.len()).ok_or(DiskError::Full)?;
        let old = self.inodes.get(&id).ok_or(DiskError::Missing)?.live.len();
        let total = self
            .inodes
            .values()
            .try_fold(0usize, |sum, i| sum.checked_add(i.live.len()))
            .ok_or(DiskError::Full)?;
        let projected = total
            .checked_add(end.saturating_sub(old))
            .ok_or(DiskError::Full)?;
        if projected > self.max_bytes {
            return Err(DiskError::Full);
        }
        let inode = self.inodes.get_mut(&id).ok_or(DiskError::Missing)?;
        inode
            .live
            .try_reserve(end.saturating_sub(old))
            .map_err(|_| DiskError::Full)?;
        inode.live.resize(old.max(end), 0);
        inode
            .live
            .get_mut(offset..end)
            .ok_or(DiskError::Invalid)?
            .copy_from_slice(bytes);
        Ok(())
    }

    pub fn sync_file(&mut self, path: impl AsRef<Path>) -> Result<(), DiskError> {
        self.tick()?;
        let id = *self.names.get(path.as_ref()).ok_or(DiskError::Missing)?;
        let inode = self.inodes.get_mut(&id).ok_or(DiskError::Missing)?;
        inode.durable.clone_from(&inode.live);
        Ok(())
    }

    pub fn sync_dir(&mut self, path: impl AsRef<Path>) -> Result<(), DiskError> {
        self.tick()?;
        let path = path.as_ref();
        self.durable_names
            .retain(|name, _| name.parent() != Some(path));
        for (name, id) in &self.names {
            if name.parent() == Some(path) {
                self.durable_names.insert(name.clone(), *id);
            }
        }
        Ok(())
    }

    pub fn rename(
        &mut self,
        source: impl AsRef<Path>,
        target: impl AsRef<Path>,
    ) -> Result<(), DiskError> {
        self.tick()?;
        let id = self
            .names
            .remove(source.as_ref())
            .ok_or(DiskError::Missing)?;
        self.names.insert(target.as_ref().to_owned(), id);
        Ok(())
    }

    pub fn read(&self, path: impl AsRef<Path>) -> Result<&[u8], DiskError> {
        let id = self.names.get(path.as_ref()).ok_or(DiskError::Missing)?;
        Ok(&self.inodes.get(id).ok_or(DiskError::Missing)?.live)
    }

    pub fn crash(&mut self) {
        self.names.clone_from(&self.durable_names);
        self.inodes
            .retain(|id, _| self.durable_names.values().any(|v| v == id));
        for inode in self.inodes.values_mut() {
            inode.live.clone_from(&inode.durable);
        }
        self.fail_before = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn syncing_bytes_does_not_publish_the_filename() {
        let mut d = Disk::new(100);
        d.create("/data/log").unwrap();
        d.write("/data/log", 0, b"proof").unwrap();
        d.sync_file("/data/log").unwrap();
        d.crash();
        assert_eq!(d.read("/data/log"), Err(DiskError::Missing));
    }

    #[test]
    fn atomic_manifest_install_requires_both_file_and_directory_sync() {
        let mut d = Disk::new(100);
        d.create("/data/manifest").unwrap();
        d.write("/data/manifest", 0, b"old").unwrap();
        d.sync_file("/data/manifest").unwrap();
        d.sync_dir("/data").unwrap();
        d.create("/data/new").unwrap();
        d.write("/data/new", 0, b"new").unwrap();
        d.sync_file("/data/new").unwrap();
        d.rename("/data/new", "/data/manifest").unwrap();
        let mut before_directory_sync = d.clone();
        before_directory_sync.crash();
        assert_eq!(
            before_directory_sync.read("/data/manifest").unwrap(),
            b"old"
        );
        d.sync_dir("/data").unwrap();
        d.crash();
        assert_eq!(d.read("/data/manifest").unwrap(), b"new");
    }

    #[test]
    fn failed_writes_leave_the_previous_live_and_durable_content() {
        let mut d = Disk::new(5);
        d.create("/data/a").unwrap();
        d.write("/data/a", 0, b"12345").unwrap();
        assert_eq!(d.write("/data/a", 5, b"6"), Err(DiskError::Full));
        assert_eq!(d.read("/data/a").unwrap(), b"12345");
    }

    #[test]
    fn unrepresentable_allocation_returns_full_without_changing_content() {
        let mut d = Disk::new(usize::MAX);
        d.create("/data/a").unwrap();
        d.write("/data/a", 0, b"before").unwrap();
        assert_eq!(
            d.write("/data/a", usize::MAX - 1, b"x"),
            Err(DiskError::Full)
        );
        assert_eq!(d.read("/data/a").unwrap(), b"before");
        d.write("/data/a", 0, b"after!").unwrap();
        assert_eq!(d.read("/data/a").unwrap(), b"after!");
    }
}
