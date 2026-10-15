//! A disk image attached as a device node, for tests of what mantle does on a raw device.
//!
//! hdiutil(1) attaches an image without mounting it (`attach -nomount`) as a `/dev/diskN`
//! node the attaching user owns, so a test reaches a real block device without privilege.

use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

/// An attached image, detached when dropped.
#[derive(Debug)]
pub struct DiskImage {
    node: PathBuf,
}

impl DiskImage {
    /// Creates a blank image of `mib` MiB in `dir`, with no partition map, and attaches it.
    pub fn attach(dir: &Path, mib: u32) -> io::Result<Self> {
        let image = dir.join("device.dmg");
        let created = Command::new("hdiutil")
            .args(["create", "-quiet", "-layout", "NONE", "-size"])
            .arg(format!("{mib}m"))
            .arg(&image)
            .status()?;
        if !created.success() {
            return Err(io::Error::other(format!("hdiutil create: {created}")));
        }
        let attached = Command::new("hdiutil")
            .args(["attach", "-nomount", "-nobrowse"])
            .arg(&image)
            .output()?;
        if !attached.status.success() {
            return Err(io::Error::other(format!(
                "hdiutil attach: {}",
                attached.status
            )));
        }
        // The first word printed is the node: "/dev/disk4" and padding.
        let printed = String::from_utf8_lossy(&attached.stdout);
        let node = printed
            .split_whitespace()
            .next()
            .filter(|n| n.starts_with("/dev/disk"))
            .ok_or_else(|| io::Error::other(format!("hdiutil attach printed {printed:?}")))?;
        Ok(Self {
            node: PathBuf::from(node),
        })
    }

    /// The block node, `/dev/diskN`.
    pub fn node(&self) -> &Path {
        &self.node
    }
}

impl Drop for DiskImage {
    fn drop(&mut self) {
        // Nothing to report to: a node left attached goes when the machine restarts.
        let _ = Command::new("hdiutil")
            .arg("detach")
            .arg("-quiet")
            .arg("-force")
            .arg(&self.node)
            .status();
    }
}
