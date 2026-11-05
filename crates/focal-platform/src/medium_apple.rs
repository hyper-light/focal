//! macOS's description of a medium (mantle `docs/research/02-os-storage-apis.md` §3): the file
//! system, whether it is local, and the device it is mounted from from statfs(2)
//! (`f_fstypename`, `MNT_LOCAL`, `f_mntfromname`), and the device's own description from Disk
//! Arbitration (`diskutil info -plist`): its bus protocol, whether it is solid state, its block
//! size, whether its media is removable, and its media name. Disk Arbitration reads IOKit's
//! registry and needs no privilege; focal calls no Apple framework itself, so it asks the system's
//! own tool, once, for the volume's whole disk: an APFS volume's is its container, which describes
//! the physical store it is synthesized from.
//!
//! The tool answers in a fraction of a second (0.26 to 0.48 s for this description, measured
//! 2026-11-05 on an M-series Mac Studio), so a node starts it and goes on ([`Pending`]): the
//! description completes when the tool has exited and is polled. A node starts it before it opens
//! a socket: std on Apple targets marks a new descriptor close-on-exec only after creating it, so a
//! child spawned meanwhile may hold another thread's new socket for its life.
use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, Stdio};

use super::{FileSystem, Interconnect, MAX_NAME_BYTES, Medium, bounded};

/// statfs(2)'s flag for a file system stored locally (`MNT_LOCAL`, <sys/mount.h>).
const MNT_LOCAL: u32 = 0x0000_1000;
/// The most a pipe holds before its writer waits (`BIG_PIPE_SIZE`, xnu bsd/sys/pipe.h): what the
/// tool can have written when it exits, and the most the probe reads of it. A longer description
/// would leave the tool waiting on the probe, which waits on its exit; it is noted, never read.
const BIG_PIPE_SIZE: u64 = 64 * 1024;
/// Disk Arbitration's command line tool.
const DISKUTIL: &str = "/usr/sbin/diskutil";

/// The tool's run, while it is out.
pub(super) struct Pending {
    child: Child,
}

/// What statfs(2) answers at once, and the tool's run when a device is there to describe.
pub(super) fn start(path: &Path) -> (Medium, Option<Pending>) {
    let mut medium = Medium::unknown(FileSystem::Unknown);
    let stat = match rustix::fs::statfs(path) {
        Ok(stat) => stat,
        Err(error) => {
            medium.note("statfs", error);
            medium.classify();
            return (medium, None);
        }
    };
    let name = text(&stat.f_fstypename);
    medium.file_system = FileSystem::from_name(&name).unwrap_or(FileSystem::Other);
    medium.file_system_name = Some(bounded(&name, MAX_NAME_BYTES));
    if stat.f_flags & MNT_LOCAL == 0 {
        if !medium.file_system.is_network() {
            medium.note("statfs", "the file system is not local");
        }
        medium.interconnect = Interconnect::Network;
        medium.classify();
        return (medium, None);
    }
    let source = text(&stat.f_mntfromname);
    let Some(disk) = whole_disk(&source) else {
        if !medium.file_system.is_memory() {
            medium.note("device", "the file system is mounted from no disk");
        }
        medium.classify();
        return (medium, None);
    };
    medium.device = Some(bounded(disk, MAX_NAME_BYTES));
    let child = Command::new(DISKUTIL)
        .args(["info", "-plist", disk])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn();
    match child {
        Ok(child) => (medium, Some(Pending { child })),
        Err(error) => {
            medium.note("diskutil", error);
            medium.classify();
            (medium, None)
        }
    }
}

/// Whether the tool has answered, its answer read into `medium` when it has: `false` while it is
/// still out.
pub(super) fn poll(medium: &mut Medium, pending: &mut Pending) -> bool {
    match pending.child.try_wait() {
        Ok(None) => false,
        Ok(Some(_)) => {
            answer(medium, pending);
            true
        }
        Err(error) => {
            medium.note("diskutil", error);
            kill(pending);
            medium.classify();
            true
        }
    }
}

/// The tool's answer, waited for: for a caller with nothing else to do (a command line, a test).
/// The tool's exit ends the read, or the probe's bound does, when the tool is killed.
pub(super) fn wait(medium: &mut Medium, mut pending: Pending) {
    answer(medium, &mut pending);
}

/// Ends the tool's run, if it has not ended: a node that stops before the tool answers.
pub(super) fn kill(pending: &mut Pending) {
    if matches!(pending.child.try_wait(), Ok(None)) {
        let _ = pending.child.kill();
    }
    let _ = pending.child.wait();
}

/// Reads the tool's description (at most [`BIG_PIPE_SIZE`] bytes) and what it says into `medium`.
fn answer(medium: &mut Medium, pending: &mut Pending) {
    let mut out = Vec::new();
    let read = match pending.child.stdout.take() {
        Some(stdout) => stdout
            .take(BIG_PIPE_SIZE.saturating_add(1))
            .read_to_end(&mut out),
        None => Ok(0),
    };
    let long = u64::try_from(out.len()).map_or(true, |len| len > BIG_PIPE_SIZE);
    if long {
        let _ = pending.child.kill();
    }
    let status = pending.child.wait();
    match (read, status) {
        (Err(error), _) | (_, Err(error)) => medium.note("diskutil", error),
        (Ok(_), Ok(_)) if long => medium.note("diskutil", "a description longer than a pipe holds"),
        (Ok(_), Ok(status)) if !status.success() => medium.note("diskutil", status),
        (Ok(_), Ok(_)) => match std::str::from_utf8(&out) {
            Ok(xml) => describe(xml, medium),
            Err(error) => medium.note("diskutil", error),
        },
    }
    medium.classify();
}

/// The whole disk a mount's source names: `/dev/disk3s5` is `disk3`'s (BSD names are
/// `disk<N>s<M>`, a slice of whole disk `disk<N>`).
pub(super) fn whole_disk(source: &str) -> Option<&str> {
    let name = source.strip_prefix("/dev/")?;
    let digits = name.strip_prefix("disk")?;
    let end = digits
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(digits.len());
    if end == 0 {
        return None;
    }
    name.get(.."disk".len().saturating_add(end))
}

/// A C string of a statfs(2) field, up to its NUL.
fn text(field: &[std::ffi::c_char]) -> String {
    let bytes: Vec<u8> = field
        .iter()
        .map(|c| {
            let [byte] = c.to_ne_bytes();
            byte
        })
        .take_while(|byte| *byte != 0)
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

/// What the tool's property list says of the device, into `medium`.
pub(super) fn describe(xml: &str, medium: &mut Medium) {
    let mut removable = None;
    for (key, value) in top_level(xml) {
        match (key.as_str(), value) {
            ("BusProtocol", Value::Text(protocol)) => medium.interconnect = interconnect(&protocol),
            ("SolidState", Value::Flag(solid)) => medium.rotational = Some(!solid),
            ("DeviceBlockSize", Value::Number(bytes)) => {
                medium.logical_block = u32::try_from(bytes).ok();
            }
            ("RemovableMedia" | "Removable", Value::Flag(flag)) => {
                removable = Some(removable.unwrap_or(false) || flag);
            }
            ("MediaName", Value::Text(name)) if !name.is_empty() => {
                medium.model = Some(bounded(&name, MAX_NAME_BYTES));
            }
            _ => {}
        }
    }
    medium.removable = removable;
}

/// How a device attaches, from Disk Arbitration's bus protocol: Apple silicon's internal SSD
/// says "Apple Fabric"; an Intel Mac's internal flash "PCI-Express" (NVMe from 2016's models).
fn interconnect(protocol: &str) -> Interconnect {
    match protocol {
        "Apple Fabric" => Interconnect::AppleFabric,
        "PCI-Express" | "PCI" | "NVMe" | "NVM Express" => Interconnect::Nvme,
        "SATA" | "ATA" => Interconnect::Sata,
        "SAS" | "SCSI" | "Fibre Channel Interface" => Interconnect::Scsi,
        "USB" => Interconnect::Usb,
        "Secure Digital" | "SD" => Interconnect::Mmc,
        "Disk Image" | "Virtual Interface" => Interconnect::Virtual,
        _ => Interconnect::Other,
    }
}

/// A value of a property list's top-level dictionary.
enum Value {
    Text(String),
    Number(u64),
    Flag(bool),
}

/// The keys and plain values of an XML property list's top-level dictionary, in order; nested
/// dictionaries and arrays are passed over. Each step consumes a tag, so the scan ends within the
/// text.
fn top_level(xml: &str) -> Vec<(String, Value)> {
    let mut values = Vec::new();
    let mut depth = 0usize;
    let mut key: Option<String> = None;
    let mut rest = xml;
    while let Some(open) = rest.find('<') {
        let after = rest.get(open.saturating_add(1)..).unwrap_or_default();
        let Some(close) = after.find('>') else {
            break;
        };
        let tag = after.get(..close).unwrap_or_default();
        rest = after.get(close.saturating_add(1)..).unwrap_or_default();
        let body = rest
            .get(..rest.find('<').unwrap_or(rest.len()))
            .unwrap_or_default();
        match tag {
            "dict" | "array" => {
                if depth == 1 {
                    key = None;
                }
                depth = depth.saturating_add(1);
            }
            "/dict" | "/array" => depth = depth.saturating_sub(1),
            "dict/" | "array/" if depth == 1 => key = None,
            "key" if depth == 1 => key = Some(unescape(body)),
            "string" | "string/" | "integer" | "true/" | "false/" if depth == 1 => {
                let Some(name) = key.take() else {
                    continue;
                };
                let value = match tag {
                    "string" => Value::Text(unescape(body)),
                    "string/" => Value::Text(String::new()),
                    "integer" => match body.trim().parse() {
                        Ok(number) => Value::Number(number),
                        Err(_) => continue,
                    },
                    flag => Value::Flag(flag == "true/"),
                };
                if values.try_reserve(1).is_err() {
                    break;
                }
                values.push((name, value));
            }
            _ => {}
        }
    }
    values
}

/// XML text with its five predefined entities replaced.
fn unescape(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}
