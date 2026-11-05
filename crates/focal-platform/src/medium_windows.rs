//! Windows' description of a medium (mantle `docs/research/02-os-storage-apis.md` §4), from the
//! answers the audited FFI file gathers (`crate::windows::storage_facts`): the file system's name
//! and the drive's type of the volume holding the path, its sector sizes and whether its device has
//! a seek penalty and takes TRIM, and the device's bus, removable media, product and write cache
//! where Windows answers them to a handle opened for no access.
use std::path::Path;

use super::{FileSystem, Interconnect, MAX_NAME_BYTES, Medium, WriteCache, bounded};

/// `GetDriveTypeW`'s answers (WinBase.h, as windows-sys carries them).
const DRIVE_REMOVABLE: u32 = 2;
const DRIVE_REMOTE: u32 = 4;
const DRIVE_CDROM: u32 = 5;
const DRIVE_RAMDISK: u32 = 6;
/// `FILE_FS_SECTOR_SIZE_INFORMATION` flags (02 §4.4, from Microsoft's Win32 metadata).
const SSINFO_FLAGS_NO_SEEK_PENALTY: u32 = 0x4;
const SSINFO_FLAGS_TRIM_ENABLED: u32 = 0x8;

/// The medium under `path`, as Windows describes it.
pub(super) fn medium(path: &Path) -> Medium {
    let facts = crate::windows::storage_facts(path);
    let mut medium = Medium::unknown(FileSystem::Unknown);
    match facts.file_system {
        Ok(name) => {
            medium.file_system = FileSystem::from_name(&name).unwrap_or(FileSystem::Other);
            medium.file_system_name = Some(bounded(&name, MAX_NAME_BYTES));
        }
        Err(error) => medium.note("GetVolumeInformationW", error),
    }
    match facts.drive_type {
        Ok(DRIVE_REMOTE) => medium.interconnect = Interconnect::Network,
        Ok(DRIVE_RAMDISK) => medium.interconnect = Interconnect::Memory,
        Ok(DRIVE_REMOVABLE) => medium.removable = Some(true),
        Ok(DRIVE_CDROM) => medium.interconnect = Interconnect::Other,
        Ok(_) => {}
        Err(error) => medium.note("GetDriveTypeW", error),
    }
    match facts.sectors {
        Ok(sectors) => {
            medium.logical_block = Some(sectors.logical);
            medium.physical_block = Some(sectors.atomic);
            medium.preferred_write = Some(sectors.performance);
            medium.rotational = Some(sectors.flags & SSINFO_FLAGS_NO_SEEK_PENALTY == 0);
            medium.discard = Some(sectors.flags & SSINFO_FLAGS_TRIM_ENABLED != 0);
        }
        Err(error) => medium.note("FileFsSectorSizeInformation", error),
    }
    match facts.device {
        Ok(device) => {
            if medium.interconnect == Interconnect::Unknown {
                medium.interconnect = interconnect(device.bus);
            }
            medium.removable = Some(medium.removable.unwrap_or(false) || device.removable_media);
            medium.model = device
                .product
                .map(|product| bounded(&product, MAX_NAME_BYTES));
        }
        Err(error) => medium.note("StorageDeviceProperty", error),
    }
    match facts.cache {
        Ok(cache) => {
            use windows_sys::Win32::System::Ioctl::{
                WriteCacheDisabled as DISABLED, WriteCacheEnabled as ENABLED,
                WriteThroughNotSupported as NOT_SUPPORTED, WriteThroughSupported as SUPPORTED,
            };
            medium.write_cache = match cache.enabled {
                ENABLED => WriteCache::WriteBack,
                DISABLED => WriteCache::WriteThrough,
                _ => WriteCache::Unknown,
            };
            medium.fua = match cache.write_through {
                SUPPORTED => Some(true),
                NOT_SUPPORTED => Some(false),
                _ => None,
            };
        }
        Err(error) => medium.note("StorageDeviceWriteCacheProperty", error),
    }
    medium.classify();
    medium
}

/// How a device attaches, from its `STORAGE_BUS_TYPE`.
fn interconnect(bus: i32) -> Interconnect {
    use windows_sys::Win32::Storage::FileSystem::{
        BusTypeAta as ATA, BusTypeAtapi as ATAPI, BusTypeFibre as FIBRE,
        BusTypeFileBackedVirtual as FILE_BACKED, BusTypeMmc as MMC, BusTypeNvme as NVME,
        BusTypeRAID as RAID, BusTypeSas as SAS, BusTypeSata as SATA, BusTypeScsi as SCSI,
        BusTypeSd as SD, BusTypeSpaces as SPACES, BusTypeSsa as SSA, BusTypeUfs as UFS,
        BusTypeUnknown as UNKNOWN, BusTypeUsb as USB, BusTypeVirtual as VIRTUAL,
        BusTypeiScsi as ISCSI,
    };
    match bus {
        NVME => Interconnect::Nvme,
        ATAPI | ATA | SATA => Interconnect::Sata,
        SCSI | SSA | FIBRE | RAID | SAS => Interconnect::Scsi,
        USB => Interconnect::Usb,
        ISCSI => Interconnect::Network,
        SD | MMC | UFS => Interconnect::Mmc,
        VIRTUAL | FILE_BACKED => Interconnect::Virtual,
        SPACES => Interconnect::Composite,
        UNKNOWN => Interconnect::Unknown,
        _ => Interconnect::Other,
    }
}
