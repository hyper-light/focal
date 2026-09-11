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
        BY_HANDLE_FILE_INFORMATION, CreateDirectoryW, CreateFileW, FILE_ATTRIBUTE_NORMAL,
        FILE_FLAG_BACKUP_SEMANTICS, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_SHARE_READ,
        FILE_SHARE_WRITE, GetDiskFreeSpaceExW, GetFileInformationByHandle,
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW, OPEN_ALWAYS, OPEN_EXISTING,
    },
    System::Threading::{GetCurrentProcess, OpenProcessToken},
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

/// The current process user's SID bytes.
pub(crate) fn current_owner() -> io::Result<Vec<u8>> {
    let mut token: HANDLE = ptr::null_mut();
    // SAFETY: GetCurrentProcess returns the current-process pseudo-handle;
    // OpenProcessToken opens its access token for query into `token`, which we
    // close below. Nonzero return means success.
    let ok = unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) };
    if ok == 0 {
        return Err(last_error());
    }
    let owner = current_owner_from_token(token);
    // SAFETY: `token` is the handle just opened; closed exactly once.
    unsafe {
        CloseHandle(token);
    }
    owner
}
fn current_owner_from_token(token: HANDLE) -> io::Result<Vec<u8>> {
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
        let acl_size = mem::size_of::<ACL>() + ace_header + sid.len() - mem::size_of::<u32>();
        let mut acl = vec![0u8; acl_size];
        // SAFETY: `acl` is `acl_size` bytes; InitializeAcl formats it as an
        // empty ACL of that size at ACL_REVISION.
        let ok = unsafe {
            InitializeAcl(
                acl.as_mut_ptr().cast(),
                acl_size as u32,
                ACL_REVISION as u32,
            )
        };
        if ok == 0 {
            return Err(last_error());
        }
        // SAFETY: `acl` is a valid initialized ACL with room for one ACE; `sid`
        // is a valid SID of `sid_len` bytes. AddAccessAllowedAce copies the SID
        // into the ACL, so `acl` does not borrow `sid` afterward.
        let ok = unsafe {
            AddAccessAllowedAce(
                acl.as_mut_ptr().cast(),
                ACL_REVISION as u32,
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
    let wide = wide(path);
    let mut dacl = OwnerOnlyDacl::new()?;
    let mut attributes = dacl.attributes();
    let mut access = 0u32;
    if read {
        access |= FILE_GENERIC_READ;
    }
    if write {
        access |= FILE_GENERIC_WRITE;
    }
    let disposition = if create { OPEN_ALWAYS } else { OPEN_EXISTING };
    // SAFETY: `wide` is NUL-terminated; `attributes` (and the DACL/ACL/SID it
    // points to) live across the call in `dacl`. CreateFileW returns an owned
    // handle or INVALID_HANDLE_VALUE. FILE_SHARE_READ|WRITE match the locking
    // model the callers already used.
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            access,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            &mut attributes,
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
    let mut attributes = dacl.attributes();
    // SAFETY: `wide` is NUL-terminated; `attributes` and its DACL live across
    // the call. CreateDirectoryW returns nonzero on success.
    let ok = unsafe { CreateDirectoryW(wide.as_ptr(), &mut attributes) };
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

fn info_at(path: &Path) -> io::Result<BY_HANDLE_FILE_INFORMATION> {
    let wide = wide(path);
    // SAFETY: `wide` is NUL-terminated. Open a query-only handle (no access
    // rights requested); FILE_FLAG_BACKUP_SEMANTICS lets it open a directory
    // too. The handle is closed below.
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            0,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            ptr::null_mut(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
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
