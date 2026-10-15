//! A key file's owner-only check on Windows (`docs/seal.md` §3.2): its DACL, read with
//! `GetSecurityInfo`, may allow access only to the file's owner and to LocalSystem (which reads every
//! file regardless). A NULL DACL, which allows everyone everything, an allow entry for any other
//! principal, or an allow entry of a kind this does not read (object and callback entries) is
//! refused. Deny entries take nothing away from the check and are passed.
#![allow(unsafe_code)]

use std::os::windows::io::AsRawHandle as _;

use windows_sys::Win32::Foundation::{HLOCAL, LocalFree};
use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT};
use windows_sys::Win32::Security::{
    ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, ACL_SIZE_INFORMATION, AclSizeInformation,
    DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetAclInformation, IsWellKnownSid,
    OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, WinLocalSystemSid,
};

use crate::SealError;

/// `ACCESS_ALLOWED_ACE_TYPE` (winnt.h).
const ACCESS_ALLOWED: u8 = 0;
/// `ACCESS_DENIED_ACE_TYPE` (winnt.h).
const ACCESS_DENIED: u8 = 1;

/// The refusal a file another principal may read gets, the same as on Unix.
const REFUSED: SealError = SealError::Source("a key file another principal may read");

/// The security descriptor `GetSecurityInfo` allocated, freed with `LocalFree` when dropped.
struct Descriptor(PSECURITY_DESCRIPTOR);

impl Drop for Descriptor {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: the descriptor was allocated by GetSecurityInfo, which documents LocalFree as
            // how it is released, and is freed once, here.
            unsafe { LocalFree(self.0 as HLOCAL) };
        }
    }
}

/// Whether only `file`'s owner (and LocalSystem) may access it.
pub(crate) fn owner_only(file: &std::fs::File) -> Result<(), SealError> {
    let mut owner: PSID = std::ptr::null_mut();
    let mut dacl: *mut ACL = std::ptr::null_mut();
    let mut descriptor: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    // SAFETY: the handle is the open file's, live for this call; the out-pointers are ours, and the
    // owner and DACL they receive point into `descriptor`, which lives until `held` drops.
    let status = unsafe {
        GetSecurityInfo(
            file.as_raw_handle(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &mut owner,
            std::ptr::null_mut(),
            &mut dacl,
            std::ptr::null_mut(),
            &mut descriptor,
        )
    };
    let held = Descriptor(descriptor);
    if status != 0 {
        return Err(SealError::Source(
            "the key file's security could not be read",
        ));
    }
    if dacl.is_null() || owner.is_null() {
        return Err(REFUSED);
    }
    let mut info = ACL_SIZE_INFORMATION {
        AceCount: 0,
        AclBytesInUse: 0,
        AclBytesFree: 0,
    };
    let size = u32::try_from(std::mem::size_of::<ACL_SIZE_INFORMATION>()).map_err(|_| REFUSED)?;
    // SAFETY: `dacl` is the DACL in `held`'s descriptor; `info` is ours and `size` its size.
    let got = unsafe { GetAclInformation(dacl, (&raw mut info).cast(), size, AclSizeInformation) };
    if got == 0 {
        return Err(REFUSED);
    }
    for index in 0..info.AceCount {
        let mut ace: *mut core::ffi::c_void = std::ptr::null_mut();
        // SAFETY: `index` is below the DACL's AceCount; `ace` receives a pointer into the DACL.
        if unsafe { GetAce(dacl, index, &mut ace) } == 0 || ace.is_null() {
            return Err(REFUSED);
        }
        // SAFETY: every ACE begins with an ACE_HEADER (winnt.h); `ace` points at one in the DACL.
        let header = unsafe { std::ptr::read_unaligned(ace.cast::<ACE_HEADER>()) };
        match header.AceType {
            ACCESS_DENIED => {}
            ACCESS_ALLOWED => {
                // SAFETY: an ACCESS_ALLOWED_ACE's SID begins at its SidStart field and runs to the
                // ACE's end, inside the DACL, which `held` keeps alive.
                let sid = unsafe { (&raw mut (*ace.cast::<ACCESS_ALLOWED_ACE>()).SidStart).cast() };
                // SAFETY: both SIDs are valid for the call: one in the DACL, one the owner, both in
                // `held`'s descriptor.
                let allowed = unsafe {
                    EqualSid(sid, owner) != 0 || IsWellKnownSid(sid, WinLocalSystemSid) != 0
                };
                if !allowed {
                    return Err(REFUSED);
                }
            }
            _ => return Err(REFUSED),
        }
    }
    drop(held);
    Ok(())
}
