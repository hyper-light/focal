//! The memory this process may use: the tightest of the machine's physical
//! memory and the limit of the control group it runs in. A container's
//! limit, not the host's RAM, is what the kernel enforces (an allocation past
//! it is reclaimed against or killed), so it is read first, as the JVM does
//! for `MaxRAMPercentage` (JDK-8146115, cgroup v1 and v2) and .NET for its
//! heap hard limit (75% of the container's limit).

/// The memory available to this process, in bytes: the least of physical
/// memory and every enclosing control group's limit. `None` when neither can
/// be read; the caller then keeps its own bound.
pub fn memory_limit() -> Option<u64> {
    let physical = physical();
    let group = group_limit();
    match (physical, group) {
        (Some(physical), Some(group)) => Some(physical.min(group)),
        (physical, group) => physical.or(group),
    }
}

/// What a process of this machine holds as its own envelope: three quarters of what it may use
/// ([`memory_limit`]), the share .NET's GC takes of a container's limit by default
/// (`GCHeapHardLimitPercent`), the rest left to the allocator, the runtime and the kernel's page
/// cache and socket queues. `None` where the machine's memory cannot be read.
pub fn memory_envelope() -> Option<u64> {
    memory_limit().map(|limit| limit.checked_div(4).unwrap_or(0).saturating_mul(3))
}

/// Turns transparent huge pages off for this process and every process it starts (Linux 3.15's
/// `PR_SET_THP_DISABLE`, kept across `fork` and `execve`), whatever the machine's own setting.
///
/// The allocator asks the kernel to back each arena it maps with huge pages (mimalloc advises
/// `MADV_HUGEPAGE` while its `allow_thp` option holds, the default), and the kernel's default
/// `defrag` policy, `madvise`, makes a page fault in an advised region enter direct reclaim and
/// compaction until a huge page is free (Documentation/admin-guide/mm/transhuge.rst): the thread
/// that touched the page stalls on it. Measured on a bus under load on a busy Linux host: a
/// committed batch's first copy into a new segment spent 70–685 ms on CPU migrating other pages
/// (`__alloc_pages_direct_compact`), 6–14 such appends a node in a 40-second round; with huge
/// pages off there were none, and the round's throughput and p99 were both better. Redis turns
/// them off for itself the same way (`disable-thp`), and mimalloc's switch for it (`allow_thp`
/// at 0) is this call. Elsewhere there is nothing to turn off: no other platform focal ships on
/// gives a process huge pages it did not map as such.
pub fn disable_transparent_huge_pages() -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        rustix::thread::disable_transparent_huge_pages(true).map_err(std::io::Error::from)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(())
    }
}

/// Whether transparent huge pages are off for this process ([`disable_transparent_huge_pages`]),
/// as its kernel says: `None` where the platform has none, or the kernel cannot say.
pub fn transparent_huge_pages_disabled() -> Option<bool> {
    #[cfg(target_os = "linux")]
    {
        rustix::thread::transparent_huge_pages_are_disabled().ok()
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(target_os = "linux")]
fn physical() -> Option<u64> {
    meminfo_total(&read_small("/proc/meminfo")?)
}

#[cfg(target_os = "macos")]
fn physical() -> Option<u64> {
    // `hw.memsize`: the machine's bytes of memory (sysctl(3)).
    sysctl_u64("hw.memsize")
}

/// A number the kernel states under `name` (sysctl(3)), read through the system's own tool: one
/// short-lived process, no `unsafe`.
#[cfg(target_os = "macos")]
pub(crate) fn sysctl_u64(name: &str) -> Option<u64> {
    let output = std::process::Command::new("/usr/sbin/sysctl")
        .args(["-n", name])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    std::str::from_utf8(&output.stdout)
        .ok()?
        .trim()
        .parse()
        .ok()
}

#[cfg(windows)]
fn physical() -> Option<u64> {
    crate::windows::physical_memory()
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn physical() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn group_limit() -> Option<u64> {
    let membership = read_small("/proc/self/cgroup")?;
    // cgroup v2: one unified hierarchy, `0::/path`. A limit anywhere above the
    // group binds it too, so every ancestor's `memory.max` is read.
    if let Some(path) = unified_path(&membership) {
        let mut least: Option<u64> = None;
        for dir in ancestors(path) {
            let file = format!("/sys/fs/cgroup{dir}/memory.max");
            if let Some(limit) = read_small(&file).and_then(|text| v2_limit(&text)) {
                least = Some(least.map_or(limit, |least| least.min(limit)));
            }
        }
        if least.is_some() {
            return least;
        }
    }
    // cgroup v1: the memory controller's own hierarchy. Inside a container
    // its root is the container's group; a value past any real memory means
    // no limit.
    let path = v1_memory_path(&membership).unwrap_or("/");
    let mut least: Option<u64> = None;
    for dir in ancestors(path) {
        let file = format!("/sys/fs/cgroup/memory{dir}/memory.limit_in_bytes");
        if let Some(limit) = read_small(&file).and_then(|text| v1_limit(&text)) {
            least = Some(least.map_or(limit, |least| least.min(limit)));
        }
    }
    least
}

#[cfg(not(target_os = "linux"))]
fn group_limit() -> Option<u64> {
    None
}

/// A small kernel file's text: the files read here are a few lines, and a
/// larger one is not what it is taken for.
#[cfg(target_os = "linux")]
fn read_small(path: &str) -> Option<String> {
    use std::io::Read as _;
    const MAX: u64 = 64 * 1024;
    let mut text = String::new();
    std::fs::File::open(path)
        .ok()?
        .take(MAX)
        .read_to_string(&mut text)
        .ok()?;
    Some(text)
}

/// `MemTotal` of `/proc/meminfo`, in bytes (the file counts kibibytes).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn meminfo_total(text: &str) -> Option<u64> {
    let line = text.lines().find(|line| line.starts_with("MemTotal:"))?;
    let mut fields = line.split_whitespace().skip(1);
    let value: u64 = fields.next()?.parse().ok()?;
    match fields.next() {
        Some("kB") => value.checked_mul(1024),
        None => Some(value),
        Some(_) => None,
    }
}

/// The unified (v2) group's path in `/proc/self/cgroup`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn unified_path(membership: &str) -> Option<&str> {
    membership
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .map(str::trim)
        .filter(|path| path.starts_with('/'))
}

/// The v1 memory controller's group path in `/proc/self/cgroup`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn v1_memory_path(membership: &str) -> Option<&str> {
    membership.lines().find_map(|line| {
        let mut fields = line.splitn(3, ':');
        let _id = fields.next()?;
        let controllers = fields.next()?;
        let path = fields.next()?.trim();
        (controllers.split(',').any(|c| c == "memory") && path.starts_with('/')).then_some(path)
    })
}

/// `path` and every ancestor up to the root, deepest first; at most 64 levels
/// (deeper than any hierarchy a kernel nests).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn ancestors(path: &str) -> impl Iterator<Item = &str> {
    let path = path.trim_end_matches('/');
    let mut next = Some(path);
    std::iter::from_fn(move || {
        let current = next?;
        next = match current.rfind('/') {
            Some(0) if current.len() > 1 => Some(""),
            Some(index) if index > 0 => current.get(..index),
            _ => None,
        };
        Some(current)
    })
    .take(64)
}

/// A v2 `memory.max`: bytes, or `max` for none.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn v2_limit(text: &str) -> Option<u64> {
    let value = text.trim();
    if value == "max" {
        return None;
    }
    value.parse().ok()
}

/// A v1 `memory.limit_in_bytes`: bytes, where a value at or past 2^62 (the
/// kernel's "unlimited", rounded to its page: 9223372036854771712 on 4 KiB
/// pages) is none.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn v1_limit(text: &str) -> Option<u64> {
    let value: u64 = text.trim().parse().ok()?;
    (value < 1 << 62).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meminfo_counts_kibibytes() {
        let text = "MemTotal:       16303428 kB\nMemFree:  1 kB\n";
        assert_eq!(meminfo_total(text), Some(16_303_428 * 1024));
        assert_eq!(meminfo_total("MemFree: 1 kB\n"), None);
        assert_eq!(meminfo_total("MemTotal: x kB\n"), None);
    }

    #[test]
    fn group_paths_are_read_for_either_hierarchy() {
        assert_eq!(
            unified_path("0::/system.slice/focal.service\n"),
            Some("/system.slice/focal.service")
        );
        assert_eq!(unified_path("12:memory:/docker/abc\n"), None);
        let v1 = "12:cpu,cpuacct:/docker/abc\n11:memory:/docker/abc\n0::/\n";
        assert_eq!(v1_memory_path(v1), Some("/docker/abc"));
        assert_eq!(v1_memory_path("0::/\n"), None);
    }

    #[test]
    fn every_ancestor_is_visited_to_the_root() {
        let seen: Vec<&str> = ancestors("/a/b/c").collect();
        assert_eq!(seen, ["/a/b/c", "/a/b", "/a", ""]);
        let root: Vec<&str> = ancestors("/").collect();
        assert_eq!(root, [""]);
    }

    #[test]
    fn limits_read_none_as_none() {
        assert_eq!(v2_limit("max\n"), None);
        assert_eq!(v2_limit("2147483648\n"), Some(2_147_483_648));
        assert_eq!(v1_limit("9223372036854771712\n"), None);
        assert_eq!(v1_limit("2147483648\n"), Some(2_147_483_648));
        assert_eq!(v1_limit("garbage"), None);
    }

    /// The process's huge pages are off once asked, as its kernel reads them back, and off in a
    /// process it starts: the flag is the process's own, not a thread's, and kept across `execve`.
    #[cfg(target_os = "linux")]
    #[test]
    fn huge_pages_turned_off_stay_off_in_the_process_and_its_children() {
        disable_transparent_huge_pages().unwrap();
        assert_eq!(transparent_huge_pages_disabled(), Some(true));
        let other = std::thread::spawn(transparent_huge_pages_disabled)
            .join()
            .unwrap();
        assert_eq!(other, Some(true), "another thread of the process");
        let status = std::process::Command::new("/bin/sh")
            .args(["-c", "cat /proc/self/status"])
            .output()
            .unwrap();
        let status = String::from_utf8(status.stdout).unwrap();
        // `THP_enabled` (Linux 5.0): whether the process may be given huge pages at all.
        if let Some(line) = status.lines().find(|line| line.starts_with("THP_enabled:")) {
            assert_eq!(line.split_whitespace().nth(1), Some("0"), "{line}");
        }
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn no_other_platform_has_huge_pages_to_turn_off() {
        disable_transparent_huge_pages().unwrap();
        assert_eq!(transparent_huge_pages_disabled(), None);
    }

    #[test]
    fn this_machine_reports_its_memory() {
        // Every platform focal ships on can say how much memory it has.
        let limit = memory_limit();
        assert!(limit.is_some_and(|bytes| bytes >= 64 << 20), "{limit:?}");
    }
}
