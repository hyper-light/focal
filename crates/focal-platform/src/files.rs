//! The files a process may hold open at once.

/// Raises this process's soft limit on open files to the most its kernel lets it hold, and says
/// what the limit in force is after ([`open_files_limit`]): the hard limit, which on macOS lets a
/// process hold no more than `kern.maxfilesperproc` whatever it says. A soft limit already there is
/// left as it is, and one the kernel will not raise stays as it was. `None` where a process has no
/// such limit (Windows), or where no bound on it can be read.
///
/// Systems start a process with a low soft limit for programs that use `select(2)`, whose `fd_set`
/// holds a fixed number of descriptors: 256 on macOS, 1,024 on many Linux distributions. focal
/// never calls `select`, and each connection a node holds has a socket of its own
/// (hyper-rt-transport's connected sockets), so a low soft limit, not the node's memory, was what
/// bounded its connections. Go raises the limit the same way for every program it starts, to the
/// hard limit (`src/syscall/rlimit.go`, go.dev/issue/46279), on Darwin no further than
/// `kern.maxfilesperproc` (`src/syscall/rlimit_darwin.go`). The processes focal starts (`sysctl`,
/// `sh`) call no `select` either, so they keep the limit raised.
pub fn raise_open_files() -> Option<u64> {
    #[cfg(unix)]
    {
        use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
        let limit = getrlimit(Resource::Nofile);
        let target = per_process_cap().map_or(limit.maximum, |cap| {
            Some(limit.maximum.map_or(cap, |hard| hard.min(cap)))
        });
        // `None` is unlimited, the most there is.
        let held = limit.current.unwrap_or(u64::MAX);
        if let Some(target) = target
            && held < target
        {
            let _ = setrlimit(
                Resource::Nofile,
                Rlimit {
                    current: Some(target),
                    maximum: limit.maximum,
                },
            );
        }
        open_files_limit()
    }
    #[cfg(not(unix))]
    {
        None
    }
}

/// The most files this process may hold open now: its soft limit, which on macOS lets it hold no
/// more than `kern.maxfilesperproc`. `None` where a process has no such limit (Windows), or where it
/// is unlimited and nothing else bounds it.
pub fn open_files_limit() -> Option<u64> {
    #[cfg(unix)]
    {
        let soft = rustix::process::getrlimit(rustix::process::Resource::Nofile).current;
        match (soft, per_process_cap()) {
            (Some(soft), Some(cap)) => Some(soft.min(cap)),
            (soft, cap) => soft.or(cap),
        }
    }
    #[cfg(not(unix))]
    {
        None
    }
}

/// What the kernel lets one process hold open whatever its limits say: macOS's
/// `kern.maxfilesperproc`. Linux bounds the hard limit itself (`fs.nr_open`), so it has none apart.
#[cfg(unix)]
fn per_process_cap() -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        crate::memory::sysctl_u64("kern.maxfilesperproc")
    }
    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    /// Raised, the limit is the most the kernel lets the process hold and stays so: asked again it
    /// is the same, never lower than before, and the process holds as many files open as it says,
    /// past the 256 macOS starts a process with.
    #[test]
    fn the_open_files_limit_rises_to_the_most_the_kernel_lets_a_process_hold() {
        let before = open_files_limit().unwrap();
        let raised = raise_open_files().unwrap();
        assert!(raised >= before, "{raised} below {before}");
        assert_eq!(open_files_limit(), Some(raised));
        assert_eq!(raise_open_files(), Some(raised), "asked again");
        #[cfg(target_os = "macos")]
        assert!(raised <= crate::memory::sysctl_u64("kern.maxfilesperproc").unwrap());
        // Past macOS's 256, within what a test should open: the process holds that many files at
        // once, the ones it held already among them (`/dev/fd` lists them, and its own read).
        let most = usize::try_from(raised.min(1_024)).unwrap();
        let open = std::fs::read_dir("/dev/fd").unwrap().count();
        let more = most.saturating_sub(open).saturating_sub(1);
        let held: Vec<std::fs::File> = (0..more)
            .map(|_| std::fs::File::open("/dev/null").unwrap())
            .collect();
        assert_eq!(held.len(), more);
    }
}
