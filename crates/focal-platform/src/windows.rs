//! The single audited `unsafe` file (decision 11, doc 10). Every call carries
//! a safety argument; nothing else under `crates/` uses `unsafe`
//! (`scripts/check-contracts.py` enforces the boundary). Windows private
//! files and directories are created with an owner-only DACL and verified by
//! owner (SID) on access.
#![allow(unsafe_code)]
use std::{
    ffi::OsStr,
    fs::File,
    io, mem,
    os::windows::{ffi::OsStrExt, io::FromRawHandle},
    path::Path,
    ptr,
};
use windows_sys::Win32::{
    Foundation::{CloseHandle, GetLastError, HANDLE, INVALID_HANDLE_VALUE, LocalFree},
    Security::{
        ACL, ACL_REVISION, AddAccessAllowedAce,
        Authorization::{GetNamedSecurityInfoW, SE_FILE_OBJECT},
        CopySid, GetLengthSid, InitializeAcl, InitializeSecurityDescriptor, IsValidSid,
        OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SECURITY_ATTRIBUTES,
        SECURITY_DESCRIPTOR, SetSecurityDescriptorDacl, TOKEN_QUERY, TOKEN_USER, TokenUser,
    },
    Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, CREATE_NEW, CreateDirectoryW, CreateFileW,
        FILE_ATTRIBUTE_NORMAL, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_SHARE_DELETE, FILE_SHARE_READ,
        FILE_SHARE_WRITE, GetDiskFreeSpaceExW, GetFileInformationByHandle,
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW, OPEN_ALWAYS, OPEN_EXISTING,
    },
    System::{
        SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX},
        Threading::{GetCurrentProcess, OpenProcessToken},
    },
};

fn last_error() -> io::Error {
    // SAFETY: GetLastError reads thread-local state and takes no arguments.
    io::Error::from_raw_os_error(unsafe { GetLastError() } as i32)
}

/// A path as a NUL-terminated UTF-16 sequence.
fn wide(path: &Path) -> Vec<u16> {
    OsStr::new(path)
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}

/// Bytes available to the caller on the volume that holds `path`.
pub(crate) fn available_space(path: &Path) -> Option<u64> {
    let wide = wide(path);
    let mut available: u64 = 0;
    // SAFETY: `wide` is NUL-terminated and outlives the call; `available` is a
    // live aligned u64 written only on success; the two totals we do not need
    // are null. On failure the function returns 0 and leaves `available` at 0.
    let ok = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut available,
            ptr::null_mut(),
            ptr::null_mut(),
        )
    };
    (ok != 0).then_some(available)
}

pub(crate) fn total_space(path: &Path) -> Option<u64> {
    let wide = wide(path);
    let mut total: u64 = 0;
    // SAFETY: `wide` is NUL-terminated and outlives the call; `total` is a
    // live aligned u64 written only on success; the two free counts we do not
    // need are null. On failure the function returns 0 and leaves `total` at 0.
    let ok =
        unsafe { GetDiskFreeSpaceExW(wide.as_ptr(), ptr::null_mut(), &mut total, ptr::null_mut()) };
    (ok != 0).then_some(total)
}

/// The current process user's SID bytes.
/// The machine's physical memory in bytes (`ullTotalPhys`).
pub(crate) fn physical_memory() -> Option<u64> {
    let mut status = MEMORYSTATUSEX {
        dwLength: u32::try_from(mem::size_of::<MEMORYSTATUSEX>()).ok()?,
        ..unsafe_zeroed_status()
    };
    // SAFETY: `status` is a live, writable MEMORYSTATUSEX whose `dwLength`
    // names its own size, as the API requires; the call writes only within
    // it and keeps no pointer past its return.
    let ok = unsafe { GlobalMemoryStatusEx(&mut status) };
    (ok != 0).then_some(status.ullTotalPhys)
}
/// An all-zero MEMORYSTATUSEX: every field is a plain integer, for which
/// zero is a valid value.
fn unsafe_zeroed_status() -> MEMORYSTATUSEX {
    // SAFETY: MEMORYSTATUSEX holds only u32 and u64 fields; the all-zero bit
    // pattern is a valid value of each.
    unsafe { mem::zeroed() }
}

pub(crate) fn current_owner() -> io::Result<Vec<u8>> {
    let mut token: HANDLE = ptr::null_mut();
    // SAFETY: GetCurrentProcess returns the current-process pseudo-handle;
    // OpenProcessToken opens its access token for query into `token`, which we
    // close below. Nonzero return means success.
    let ok = unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) };
    if ok == 0 {
        return Err(last_error());
    }
    let owner = owner_from_token(token);
    // SAFETY: `token` is the handle just opened; closed exactly once.
    unsafe {
        CloseHandle(token);
    }
    owner
}
fn owner_from_token(token: HANDLE) -> io::Result<Vec<u8>> {
    let mut needed: u32 = 0;
    // SAFETY: the pseudo-token is valid for the current process; the first
    // call asks only for the required size (buffer null, size 0), so it fails
    // with ERROR_INSUFFICIENT_BUFFER and writes `needed`.
    unsafe {
        windows_sys::Win32::Security::GetTokenInformation(
            token,
            TokenUser,
            ptr::null_mut(),
            0,
            &mut needed,
        );
    }
    if needed == 0 {
        return Err(last_error());
    }
    let mut buffer = vec![0u8; needed as usize];
    // SAFETY: `buffer` is `needed` bytes, matching the size the first call
    // reported; on success it holds a TOKEN_USER whose SID points inside it.
    let ok = unsafe {
        windows_sys::Win32::Security::GetTokenInformation(
            token,
            TokenUser,
            buffer.as_mut_ptr().cast(),
            needed,
            &mut needed,
        )
    };
    if ok == 0 {
        return Err(last_error());
    }
    // SAFETY: on success `buffer` begins with a TOKEN_USER; its User.Sid points
    // within `buffer`, which outlives the copy below.
    let sid = unsafe { (*buffer.as_ptr().cast::<TOKEN_USER>()).User.Sid };
    copy_sid(sid)
}

/// The owner SID bytes of an existing path.
pub(crate) fn owner_at(path: &Path) -> io::Result<Vec<u8>> {
    let wide = wide(path);
    let mut owner: PSID = ptr::null_mut();
    let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
    // SAFETY: `wide` is NUL-terminated and outlives the call; `owner` and
    // `descriptor` are out-parameters the call fills on success. On success it
    // returns 0 (ERROR_SUCCESS) and `descriptor` must be freed with LocalFree.
    let status = unsafe {
        GetNamedSecurityInfoW(
            wide.as_ptr(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION,
            &mut owner,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut descriptor,
        )
    };
    if status != 0 {
        return Err(io::Error::from_raw_os_error(status as i32));
    }
    let result = copy_sid(owner);
    // SAFETY: `descriptor` was allocated by GetNamedSecurityInfoW and is freed
    // exactly once here; `owner` pointed inside it and is not used afterward.
    unsafe {
        LocalFree(descriptor as *mut _);
    }
    result
}

/// Copy a SID out of borrowed storage into an owned byte vector.
fn copy_sid(sid: PSID) -> io::Result<Vec<u8>> {
    // SAFETY: `sid` is a valid SID pointer from the token or the descriptor.
    if sid.is_null() || unsafe { IsValidSid(sid) } == 0 {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    // SAFETY: `sid` is a valid SID; GetLengthSid returns its byte length.
    let length = unsafe { GetLengthSid(sid) } as usize;
    let mut bytes = vec![0u8; length];
    // SAFETY: `bytes` is `length` bytes, exactly the SID's size; CopySid writes
    // the SID into it. Both pointers are valid for `length` bytes.
    let ok = unsafe { CopySid(length as u32, bytes.as_mut_ptr().cast(), sid) };
    if ok == 0 {
        return Err(last_error());
    }
    Ok(bytes)
}

/// An owner-only DACL for the current user, owned alongside its ACL buffer.
struct OwnerOnlyDacl {
    descriptor: SECURITY_DESCRIPTOR,
    _acl: Vec<u8>,
    _sid: Vec<u8>,
}
impl OwnerOnlyDacl {
    fn new() -> io::Result<Self> {
        let mut sid = current_owner()?;
        // ACL header + one allow ACE (its trailing SidStart overlaps the SID's
        // first DWORD, hence the `- 4`) + the SID bytes.
        let ace_header = mem::size_of::<windows_sys::Win32::Security::ACCESS_ALLOWED_ACE>();
        let acl_size = mem::size_of::<ACL>()
            .checked_add(ace_header)
            .and_then(|size| size.checked_add(sid.len()))
            .and_then(|size| size.checked_sub(mem::size_of::<u32>()))
            .ok_or_else(|| io::Error::other("owner-only ACL size"))?;
        let acl_len =
            u32::try_from(acl_size).map_err(|_| io::Error::other("owner-only ACL size"))?;
        let mut acl = vec![0u8; acl_size];
        // SAFETY: `acl` is `acl_size` bytes; InitializeAcl formats it as an
        // empty ACL of that size at ACL_REVISION.
        let ok = unsafe { InitializeAcl(acl.as_mut_ptr().cast(), acl_len, ACL_REVISION) };
        if ok == 0 {
            return Err(last_error());
        }
        // SAFETY: `acl` is a valid initialized ACL with room for one ACE; `sid`
        // is a valid SID of `sid_len` bytes. AddAccessAllowedAce copies the SID
        // into the ACL, so `acl` does not borrow `sid` afterward.
        let ok = unsafe {
            AddAccessAllowedAce(
                acl.as_mut_ptr().cast(),
                ACL_REVISION,
                windows_sys::Win32::Storage::FileSystem::FILE_ALL_ACCESS,
                sid.as_mut_ptr().cast(),
            )
        };
        if ok == 0 {
            return Err(last_error());
        }
        let mut descriptor: SECURITY_DESCRIPTOR = unsafe { mem::zeroed() };
        // SAFETY: `descriptor` is a live SECURITY_DESCRIPTOR; InitializeSecurity
        // Descriptor formats it at the required revision.
        let ok = unsafe {
            InitializeSecurityDescriptor((&mut descriptor as *mut SECURITY_DESCRIPTOR).cast(), 1)
        };
        if ok == 0 {
            return Err(last_error());
        }
        // SAFETY: `descriptor` is initialized; `acl` is a valid ACL that lives
        // as long as the returned struct (held in `_acl`). The descriptor
        // references it by pointer, so both are kept together.
        let ok = unsafe {
            SetSecurityDescriptorDacl(
                (&mut descriptor as *mut SECURITY_DESCRIPTOR).cast(),
                1,
                acl.as_mut_ptr().cast(),
                0,
            )
        };
        if ok == 0 {
            return Err(last_error());
        }
        Ok(Self {
            descriptor,
            _acl: acl,
            _sid: sid,
        })
    }
    fn attributes(&mut self) -> SECURITY_ATTRIBUTES {
        SECURITY_ATTRIBUTES {
            nLength: mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: (&mut self.descriptor as *mut SECURITY_DESCRIPTOR).cast(),
            bInheritHandle: 0,
        }
    }
}

/// Open a file with an owner-only DACL when created.
pub(crate) fn open_private(path: &Path, read: bool, write: bool, create: bool) -> io::Result<File> {
    open_with(
        path,
        read,
        write,
        if create { OPEN_ALWAYS } else { OPEN_EXISTING },
    )
}

/// Create a new owner-only file. Errors with `AlreadyExists` if it exists.
pub(crate) fn create_private_new(path: &Path, read: bool, write: bool) -> io::Result<File> {
    open_with(path, read, write, CREATE_NEW)
}

fn open_with(path: &Path, read: bool, write: bool, disposition: u32) -> io::Result<File> {
    let wide = wide(path);
    let mut dacl = OwnerOnlyDacl::new()?;
    let attributes = dacl.attributes();
    let mut access = 0u32;
    if read {
        access |= FILE_GENERIC_READ;
    }
    if write {
        access |= FILE_GENERIC_WRITE;
    }
    // SAFETY: `wide` is NUL-terminated; `attributes` (and the DACL/ACL/SID it
    // points to) live across the call in `dacl`. CreateFileW returns an owned
    // handle or INVALID_HANDLE_VALUE. FILE_SHARE_READ|WRITE|DELETE matches std's
    // own share mode, so a held-open file can still be renamed or deleted (the
    // WAL, checkpoint and journal atomic-replace protocols depend on it).
    // CREATE_NEW fails with ERROR_FILE_EXISTS,
    // which the standard library maps to io::ErrorKind::AlreadyExists.
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            access,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            &attributes,
            disposition,
            FILE_ATTRIBUTE_NORMAL,
            ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(last_error());
    }
    // SAFETY: `handle` is a valid, owned file handle CreateFileW returned;
    // File takes sole ownership and closes it on drop.
    Ok(unsafe { File::from_raw_handle(handle as *mut _) })
}

/// Create a directory with an owner-only DACL. Errors if it exists.
pub(crate) fn create_dir_private(path: &Path) -> io::Result<()> {
    let wide = wide(path);
    let mut dacl = OwnerOnlyDacl::new()?;
    let attributes = dacl.attributes();
    // SAFETY: `wide` is NUL-terminated; `attributes` and its DACL live across
    // the call. CreateDirectoryW returns nonzero on success.
    let ok = unsafe { CreateDirectoryW(wide.as_ptr(), &attributes) };
    if ok == 0 {
        return Err(last_error());
    }
    Ok(())
}

/// Atomically replace `to` with `from`, write-through.
pub(crate) fn move_replace(from: &Path, to: &Path) -> io::Result<()> {
    let from = wide(from);
    let to = wide(to);
    // SAFETY: both buffers are NUL-terminated and outlive the call.
    let ok = unsafe {
        MoveFileExW(
            from.as_ptr(),
            to.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if ok == 0 {
        return Err(last_error());
    }
    Ok(())
}

/// Atomically move `from` to `to`, write-through, failing if `to` already
/// exists. Without `MOVEFILE_REPLACE_EXISTING`, `MoveFileExW` returns
/// `ERROR_ALREADY_EXISTS` when the destination exists - the no-clobber
/// publication guarantee - which the standard library maps to
/// `io::ErrorKind::AlreadyExists`.
pub(crate) fn move_create_new(from: &Path, to: &Path) -> io::Result<()> {
    let from = wide(from);
    let to = wide(to);
    // SAFETY: both buffers are NUL-terminated and outlive the call.
    let ok = unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), MOVEFILE_WRITE_THROUGH) };
    if ok == 0 {
        return Err(last_error());
    }
    Ok(())
}

fn info_at(path: &Path) -> io::Result<BY_HANDLE_FILE_INFORMATION> {
    let wide = wide(path);
    // SAFETY: `wide` is NUL-terminated. Open a query-only handle (no access
    // rights requested); FILE_FLAG_BACKUP_SEMANTICS lets it open a directory
    // too, and FILE_FLAG_OPEN_REPARSE_POINT names the path itself, never
    // what a reparse point leads to, as `symlink_metadata` does on Unix: an
    // AF_UNIX socket's file is a reparse point that cannot be followed, so
    // without it a local server had no identity for its socket and left the
    // file behind. The handle is closed below.
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            ptr::null_mut(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(last_error());
    }
    let result = info_from_handle(handle);
    // SAFETY: `handle` is the valid handle just opened; closed exactly once.
    unsafe {
        CloseHandle(handle);
    }
    result
}
fn info_from_handle(handle: HANDLE) -> io::Result<BY_HANDLE_FILE_INFORMATION> {
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { mem::zeroed() };
    // SAFETY: `handle` is a valid open handle; `info` is a live, aligned
    // structure the call fills on success (nonzero return).
    let ok = unsafe { GetFileInformationByHandle(handle, &mut info) };
    if ok == 0 {
        return Err(last_error());
    }
    Ok(info)
}

pub(crate) fn file_id_at(path: &Path) -> io::Result<super::fs::FileId> {
    Ok(id_of(&info_at(path)?))
}
pub(crate) fn file_id_open(file: &File) -> io::Result<super::fs::FileId> {
    use std::os::windows::io::AsRawHandle;
    Ok(id_of(&info_from_handle(file.as_raw_handle() as HANDLE)?))
}
fn id_of(info: &BY_HANDLE_FILE_INFORMATION) -> super::fs::FileId {
    super::fs::FileId {
        volume: u64::from(info.dwVolumeSerialNumber),
        index: (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
    }
}
pub(crate) fn hard_link_count_at(path: &Path) -> io::Result<u64> {
    Ok(u64::from(info_at(path)?.nNumberOfLinks))
}
pub(crate) fn hard_link_count_open(file: &File) -> io::Result<u64> {
    use std::os::windows::io::AsRawHandle;
    Ok(u64::from(
        info_from_handle(file.as_raw_handle() as HANDLE)?.nNumberOfLinks,
    ))
}

/// What Windows says of the volume that holds a path and of the device under it (mantle
/// docs/research/02-os-storage-apis.md §4), each query's answer or why it gave none: the
/// file system's name, the drive's type and the volume's sector sizes and flags, which any caller
/// may ask, and the device's descriptor and write cache, which Windows answers on a handle to the
/// volume opened for no access, where it answers at all (02 §4.3 leaves unprivileged success
/// UNVERIFIED: a refusal is an unknown, never a failure).
pub(crate) struct StorageFacts {
    /// The volume's file system's name (`GetVolumeInformationW`).
    pub(crate) file_system: io::Result<String>,
    /// The volume's drive type (`GetDriveTypeW`: `DRIVE_REMOVABLE` 2, `DRIVE_FIXED` 3,
    /// `DRIVE_REMOTE` 4, `DRIVE_CDROM` 5, `DRIVE_RAMDISK` 6).
    pub(crate) drive_type: io::Result<u32>,
    /// The volume's sectors and flags (`NtQueryVolumeInformationFile`,
    /// `FileFsSectorSizeInformation`), asked through a handle to the path itself.
    pub(crate) sectors: io::Result<SectorFacts>,
    /// The device's descriptor (`IOCTL_STORAGE_QUERY_PROPERTY`, `StorageDeviceProperty`).
    pub(crate) device: io::Result<DeviceFacts>,
    /// The device's write cache (`IOCTL_STORAGE_QUERY_PROPERTY`,
    /// `StorageDeviceWriteCacheProperty`).
    pub(crate) cache: io::Result<CacheFacts>,
}

/// `FILE_FS_SECTOR_SIZE_INFORMATION`'s sizes and flags.
pub(crate) struct SectorFacts {
    pub(crate) logical: u32,
    pub(crate) atomic: u32,
    pub(crate) performance: u32,
    /// `SSINFO_FLAGS_*`: `NO_SEEK_PENALTY` 0x4, `TRIM_ENABLED` 0x8.
    pub(crate) flags: u32,
}

/// `STORAGE_DEVICE_DESCRIPTOR`'s bus, media and identification.
pub(crate) struct DeviceFacts {
    /// `STORAGE_BUS_TYPE`.
    pub(crate) bus: i32,
    pub(crate) removable_media: bool,
    pub(crate) product: Option<String>,
}

/// `STORAGE_WRITE_CACHE_PROPERTY`'s cache and write-through answers.
pub(crate) struct CacheFacts {
    /// `WRITE_CACHE_ENABLE`: `WriteCacheDisabled` 1, `WriteCacheEnabled` 2.
    pub(crate) enabled: i32,
    /// `WRITE_THROUGH`: `WriteThroughNotSupported` 1, `WriteThroughSupported` 2.
    pub(crate) write_through: i32,
}

/// The bytes of a device descriptor read: one memory page (`memory_page_bytes`), which holds the
/// descriptor and its identification strings (an NVMe controller's model is 40 bytes, Identify
/// Controller `MN`); a descriptor longer than a page is read to the page.
const DESCRIPTOR_BYTES: usize = 4096;

pub(crate) fn storage_facts(path: &Path) -> StorageFacts {
    let root = volume_root(path);
    let again = |error: &io::Error| io::Error::new(error.kind(), error.to_string());
    StorageFacts {
        file_system: root
            .as_ref()
            .map_err(again)
            .and_then(|root| file_system_name(root)),
        drive_type: root.as_ref().map_err(again).map(|root| {
            // SAFETY: `root` is a NUL-terminated wide string that outlives the call, which reads
            // it and keeps no pointer to it.
            unsafe { windows_sys::Win32::Storage::FileSystem::GetDriveTypeW(root.as_ptr()) }
        }),
        sectors: sector_facts(path),
        device: root
            .as_ref()
            .map_err(again)
            .and_then(|root| volume_handle(root))
            .and_then(|volume| device_facts(&volume)),
        cache: root
            .as_ref()
            .map_err(again)
            .and_then(|root| volume_handle(root))
            .and_then(|volume| cache_facts(&volume)),
    }
}

/// The root of the volume that holds `path` (`C:\`, or the folder a volume is mounted at), NUL
/// terminated: a prefix of the path, so no longer than it.
fn volume_root(path: &Path) -> io::Result<Vec<u16>> {
    let wide = wide(path);
    let mut root = vec![
        0u16;
        wide.len()
            .max(windows_sys::Win32::Foundation::MAX_PATH as usize)
    ];
    let len =
        u32::try_from(root.len()).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: `wide` is NUL-terminated and `root` is `len` writable u16s; both outlive the call,
    // which writes at most `len` units (its NUL included) and keeps no pointer to either.
    let ok = unsafe {
        windows_sys::Win32::Storage::FileSystem::GetVolumePathNameW(
            wide.as_ptr(),
            root.as_mut_ptr(),
            len,
        )
    };
    if ok == 0 {
        return Err(last_error());
    }
    let end = root
        .iter()
        .position(|unit| *unit == 0)
        .unwrap_or(root.len());
    root.truncate(end);
    root.push(0);
    Ok(root)
}

/// The name of the file system of the volume at `root`.
fn file_system_name(root: &[u16]) -> io::Result<String> {
    // "The maximum buffer size is MAX_PATH+1" (GetVolumeInformationW, lpFileSystemNameBuffer).
    let mut name = [0u16; windows_sys::Win32::Foundation::MAX_PATH as usize + 1];
    let len =
        u32::try_from(name.len()).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: `root` is NUL-terminated (volume_root) and outlives the call; `name` is `len`
    // writable u16s; every other output is null, which the function takes as "not wanted".
    let ok = unsafe {
        windows_sys::Win32::Storage::FileSystem::GetVolumeInformationW(
            root.as_ptr(),
            ptr::null_mut(),
            0,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            name.as_mut_ptr(),
            len,
        )
    };
    if ok == 0 {
        return Err(last_error());
    }
    let end = name
        .iter()
        .position(|unit| *unit == 0)
        .unwrap_or(name.len());
    Ok(String::from_utf16_lossy(
        name.get(..end).unwrap_or_default(),
    ))
}

/// The volume's sectors and flags, through a handle to `path` opened for no access: "No specific
/// access rights are required to query this information" (FILE_FS_SECTOR_SIZE_INFORMATION).
fn sector_facts(path: &Path) -> io::Result<SectorFacts> {
    use std::os::windows::{fs::OpenOptionsExt, io::AsRawHandle};
    let file = std::fs::OpenOptions::new()
        .access_mode(0)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    let mut info =
        windows_sys::Wdk::Storage::FileSystem::FILE_FS_SECTOR_SIZE_INFORMATION::default();
    let mut status_block = windows_sys::Win32::System::IO::IO_STATUS_BLOCK::default();
    let len = u32::try_from(mem::size_of_val(&info))
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: `file` is an open handle for the call's length; `status_block` and `info` are live,
    // writable and of the sizes passed, and the call writes within them and keeps no pointer.
    let status = unsafe {
        windows_sys::Wdk::Storage::FileSystem::NtQueryVolumeInformationFile(
            file.as_raw_handle() as HANDLE,
            &mut status_block,
            (&raw mut info).cast(),
            len,
            windows_sys::Wdk::Storage::FileSystem::FileFsSectorSizeInformation,
        )
    };
    if status < 0 {
        return Err(io::Error::other(format!("NTSTATUS {status:#010x}")));
    }
    Ok(SectorFacts {
        logical: info.LogicalBytesPerSector,
        atomic: info.PhysicalBytesPerSectorForAtomicity,
        performance: info.PhysicalBytesPerSectorForPerformance,
        flags: info.Flags,
    })
}

/// A handle to the volume at `root`, opened for no access: enough to ask the device's properties
/// ("the application can query certain metadata such as file, directory, or device attributes
/// without accessing that file or device", CreateFileW).
fn volume_handle(root: &[u16]) -> io::Result<File> {
    use std::os::windows::{ffi::OsStringExt, fs::OpenOptionsExt};
    // "A reasonable size for the buffer to accommodate the largest possible volume GUID path is
    // 50 characters" (GetVolumeNameForVolumeMountPointW).
    let mut name = [0u16; 50];
    let len =
        u32::try_from(name.len()).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: `root` is NUL-terminated and ends in a backslash, as GetVolumePathNameW returns
    // it; `name` is `len` writable u16s; neither pointer is kept past the call.
    let ok = unsafe {
        windows_sys::Win32::Storage::FileSystem::GetVolumeNameForVolumeMountPointW(
            root.as_ptr(),
            name.as_mut_ptr(),
            len,
        )
    };
    if ok == 0 {
        return Err(last_error());
    }
    // `\\?\Volume{GUID}\` names the volume's root directory; without its last backslash, the
    // volume.
    let end = name
        .iter()
        .position(|unit| *unit == 0)
        .unwrap_or(name.len());
    let volume = name.get(..end).unwrap_or_default();
    let volume = volume.strip_suffix(&[u16::from(b'\\')]).unwrap_or(volume);
    std::fs::OpenOptions::new()
        .access_mode(0)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .open(std::ffi::OsString::from_wide(volume))
}

/// One storage property of the device under `volume` into `out`, `Ok` with the bytes written.
fn query_property(volume: &File, property: i32, out: &mut [u8]) -> io::Result<usize> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::System::Ioctl::{
        IOCTL_STORAGE_QUERY_PROPERTY, PropertyStandardQuery, STORAGE_PROPERTY_QUERY,
    };
    let query = STORAGE_PROPERTY_QUERY {
        PropertyId: property,
        QueryType: PropertyStandardQuery,
        AdditionalParameters: [0],
    };
    let query_len = u32::try_from(mem::size_of_val(&query))
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let out_len =
        u32::try_from(out.len()).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let mut returned: u32 = 0;
    // SAFETY: `volume` is an open handle; `query` is a live STORAGE_PROPERTY_QUERY of the length
    // passed; `out` is `out_len` writable bytes and `returned` a live u32; the call is synchronous
    // (no OVERLAPPED), writes within `out`, and keeps no pointer past its return.
    let ok = unsafe {
        windows_sys::Win32::System::IO::DeviceIoControl(
            volume.as_raw_handle() as HANDLE,
            IOCTL_STORAGE_QUERY_PROPERTY,
            (&raw const query).cast(),
            query_len,
            out.as_mut_ptr().cast(),
            out_len,
            &mut returned,
            ptr::null_mut(),
        )
    };
    if ok == 0 {
        return Err(last_error());
    }
    Ok(usize::try_from(returned).unwrap_or(0).min(out.len()))
}

/// The device's descriptor, read from its bytes: `STORAGE_DEVICE_DESCRIPTOR`'s fields at their
/// offsets, and its product string at the offset it names (zero for none), NUL-terminated within
/// what the device returned.
fn device_facts(volume: &File) -> io::Result<DeviceFacts> {
    use windows_sys::Win32::System::Ioctl::{STORAGE_DEVICE_DESCRIPTOR, StorageDeviceProperty};
    let mut bytes = vec![0u8; DESCRIPTOR_BYTES];
    let returned = query_property(volume, StorageDeviceProperty, &mut bytes)?;
    let bytes = bytes.get(..returned).unwrap_or_default();
    let short = || io::Error::new(io::ErrorKind::InvalidData, "a short device descriptor");
    let word = |offset: usize| -> io::Result<u32> {
        let end = offset.checked_add(4).ok_or_else(short)?;
        let field: [u8; 4] = bytes
            .get(offset..end)
            .ok_or_else(short)?
            .try_into()
            .map_err(|_| short())?;
        Ok(u32::from_le_bytes(field))
    };
    let removable_media = mem::offset_of!(STORAGE_DEVICE_DESCRIPTOR, RemovableMedia);
    let product = mem::offset_of!(STORAGE_DEVICE_DESCRIPTOR, ProductIdOffset);
    let bus = mem::offset_of!(STORAGE_DEVICE_DESCRIPTOR, BusType);
    let text = |offset: u32| -> Option<String> {
        let start = usize::try_from(offset).ok().filter(|start| *start != 0)?;
        let tail = bytes.get(start..)?;
        let end = tail
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(tail.len());
        let text = String::from_utf8_lossy(tail.get(..end)?).trim().to_owned();
        (!text.is_empty()).then_some(text)
    };
    Ok(DeviceFacts {
        bus: i32::from_le_bytes(word(bus)?.to_le_bytes()),
        removable_media: bytes.get(removable_media).copied().ok_or_else(short)? != 0,
        product: text(word(product)?),
    })
}

/// The device's write cache, where it reports one.
fn cache_facts(volume: &File) -> io::Result<CacheFacts> {
    use windows_sys::Win32::System::Ioctl::{
        STORAGE_WRITE_CACHE_PROPERTY, StorageDeviceWriteCacheProperty,
    };
    let mut bytes = [0u8; mem::size_of::<STORAGE_WRITE_CACHE_PROPERTY>()];
    let returned = query_property(volume, StorageDeviceWriteCacheProperty, &mut bytes)?;
    let bytes = bytes.get(..returned).unwrap_or_default();
    let short = || io::Error::new(io::ErrorKind::InvalidData, "a short write cache property");
    let word = |offset: usize| -> io::Result<i32> {
        let end = offset.checked_add(4).ok_or_else(short)?;
        let field: [u8; 4] = bytes
            .get(offset..end)
            .ok_or_else(short)?
            .try_into()
            .map_err(|_| short())?;
        Ok(i32::from_le_bytes(field))
    };
    Ok(CacheFacts {
        enabled: word(mem::offset_of!(
            STORAGE_WRITE_CACHE_PROPERTY,
            WriteCacheEnabled
        ))?,
        write_through: word(mem::offset_of!(
            STORAGE_WRITE_CACHE_PROPERTY,
            WriteThroughSupported
        ))?,
    })
}
