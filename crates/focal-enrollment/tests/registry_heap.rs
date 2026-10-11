//! A restored registry's charge (`EnrollmentRegistry::charged_bytes`), checked
//! against what its restore asks the allocator for: what the restored
//! registry holds, and the most its restore held at once. Whoever decodes a
//! registry charges this before decoding it, so it must bound both.
//!
//! One test function only: the counters are process-global, so a second
//! concurrent test in the same binary would race them.
#![allow(unsafe_code)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

use focal_enrollment::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// The bookkeeping each allocation is charged by (`focal_memory::ALLOCATOR_OVERHEAD`).
const OVERHEAD: usize = 4 * size_of::<usize>();

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY (test allocator): delegates to the system allocator with the
        // same layout; the accounting is ordinary atomic adds.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let live = LIVE.fetch_add(layout.size() + OVERHEAD, Ordering::Relaxed)
                + layout.size()
                + OVERHEAD;
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() + OVERHEAD, Ordering::Relaxed);
        // SAFETY: `ptr`/`layout` are the pair this allocator returned.
        unsafe { System.dealloc(ptr, layout) };
    }
}
#[global_allocator]
static ALLOC: Counting = Counting;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn invite(
    registry: &mut EnrollmentRegistry,
    authority: &BootstrapAuthority,
    role: EnrollmentRole,
) -> Invitation {
    let draft = registry
        .prepare_invitation(
            authority,
            InviteOptions {
                endpoint: "127.0.0.1:8443".into(),
                server_name: "localhost".into(),
                role,
                expires_at: now() + 600,
            },
            now(),
        )
        .unwrap();
    registry
        .apply_committed(draft.command(), registry.applied_index() + 1)
        .unwrap();
    draft.release(registry).unwrap()
}

fn enroll(
    dir: &tempfile::TempDir,
    registry: &mut EnrollmentRegistry,
    authority: &BootstrapAuthority,
    role: EnrollmentRole,
    name: &str,
) {
    let invitation = invite(registry, authority, role);
    let key = JoinKey::open_or_create(dir.path().join(name), authority.cluster()).unwrap();
    let request = invitation.request(&key, now()).unwrap();
    let JoinPreparation::Commit(command) =
        registry.prepare_join(authority, &request, now()).unwrap()
    else {
        panic!("expected a new enrollment")
    };
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    registry.release(&request, now()).unwrap();
}

/// What restoring `bytes` holds once restored, and the most it held at once
/// while restoring, both over what was held before.
fn restored(bytes: &[u8], cluster: ClusterId) -> (EnrollmentRegistry, usize, usize) {
    let before = LIVE.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let registry =
        EnrollmentRegistry::restore(bytes, cluster, EnrollmentLimits::default()).unwrap();
    let held = LIVE.load(Ordering::Relaxed) - before;
    let peak = PEAK.load(Ordering::Relaxed) - before;
    (registry, held, peak)
}

#[test]
fn a_restored_registry_holds_no_more_than_its_charge() {
    let dir = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        dir.path().join("authority"),
        [7; 16],
        vec!["localhost".into()],
        now(),
    )
    .unwrap();
    let mut registry = EnrollmentRegistry::new(
        authority.cluster(),
        authority.ca_certificate().to_vec(),
        2,
        EnrollmentLimits::default(),
    )
    .unwrap();
    let mut enrolled = 0usize;
    for target in [0usize, 1, 4, 16, 64] {
        while enrolled < target {
            let role = if enrolled % 4 == 3 {
                EnrollmentRole::Client
            } else {
                EnrollmentRole::Node
            };
            enroll(
                &dir,
                &mut registry,
                &authority,
                role,
                &format!("key-{enrolled}"),
            );
            // An invitation still open beside every other enrollment.
            if enrolled.is_multiple_of(2) {
                invite(&mut registry, &authority, EnrollmentRole::Node);
            }
            enrolled += 1;
        }
        let bytes = registry.checkpoint().unwrap();
        let (copy, held, peak) = restored(&bytes, authority.cluster());
        let measured = copy.held_bytes().unwrap();
        let restoring = copy.restore_bytes().unwrap();
        eprintln!(
            "{target} enrollments: {} encoded, committed {}, held {held} of {measured}, \
             peak {peak} of {restoring}",
            bytes.len(),
            copy.charged_bytes(),
        );
        assert_eq!(measured, registry.held_bytes().unwrap());
        assert!(held <= measured, "{target}: held {held} > {measured}");
        assert!(peak <= restoring, "{target}: peak {peak} > {restoring}");
        // Before decoding, the bytes alone bound it.
        let bound = EnrollmentRegistry::restore_bound(bytes.len()).unwrap();
        eprintln!("{target} enrollments: bound before decoding {bound}");
        assert!(restoring <= bound, "{target}: {restoring} > bound {bound}");
    }
}
