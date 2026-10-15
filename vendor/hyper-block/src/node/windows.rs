//! Windows: the capacity of a disk or volume opened by its device path, from
//! IOCTL_DISK_GET_LENGTH_INFO, since such a handle has no file size; and the sector sizes of the
//! volume a file is on, from GetFileInformationByHandleEx's FileStorageInfo class.
#![allow(unsafe_code)]

use std::fs::File;
use std::io;
use std::os::windows::io::AsRawHandle;

use windows_sys::Win32::Storage::FileSystem::{
    FILE_STORAGE_INFO, FileStorageInfo, GetFileInformationByHandleEx,
};
use windows_sys::Win32::System::IO::DeviceIoControl;
use windows_sys::Win32::System::Ioctl::{GET_LENGTH_INFORMATION, IOCTL_DISK_GET_LENGTH_INFO};

/// The sector sizes of the volume `file` is on (FILE_STORAGE_INFO, Windows 8 and Server 2012 on).
pub(crate) fn storage_info(file: &File) -> io::Result<FILE_STORAGE_INFO> {
    let mut info = FILE_STORAGE_INFO {
        LogicalBytesPerSector: 0,
        PhysicalBytesPerSectorForAtomicity: 0,
        PhysicalBytesPerSectorForPerformance: 0,
        FileSystemEffectivePhysicalBytesPerSectorForAtomicity: 0,
        Flags: 0,
        ByteOffsetForSectorAlignment: 0,
        ByteOffsetForPartitionAlignment: 0,
    };
    let size = u32::try_from(std::mem::size_of::<FILE_STORAGE_INFO>())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: the handle is `file`'s and outlives the call; the output is a writable
    // FILE_STORAGE_INFO of `size` bytes, the structure the FileStorageInfo class fills, and the
    // call is synchronous, so the buffer outlives it.
    let ok = unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            FileStorageInfo,
            (&raw mut info).cast(),
            size,
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(info)
}

pub(crate) fn len(file: &File) -> io::Result<u64> {
    let mut info = GET_LENGTH_INFORMATION { Length: 0 };
    let mut returned = 0u32;
    let size = u32::try_from(std::mem::size_of::<GET_LENGTH_INFORMATION>())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: the handle is `file`'s and outlives the call; the output is a writable
    // GET_LENGTH_INFORMATION of `size` bytes; without an OVERLAPPED the call completes before
    // it returns, so the buffer outlives it too.
    let ok = unsafe {
        DeviceIoControl(
            file.as_raw_handle(),
            IOCTL_DISK_GET_LENGTH_INFO,
            std::ptr::null(),
            0,
            (&raw mut info).cast(),
            size,
            &mut returned,
            std::ptr::null_mut(),
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    u64::try_from(info.Length).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "the device reports a negative length",
        )
    })
}
