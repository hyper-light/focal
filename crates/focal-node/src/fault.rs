//! Crash cuts for qualification (REMAINING §9 A4, doc 06 §3). With the
//! `test-support` feature, `FOCAL_FAULT=<site>:<n>` aborts this process the
//! `n`-th time execution reaches `site`; the sites sit at the durable
//! boundaries a lost reply can straddle. Without the feature every hit is a
//! no-op and the binary contains no hook.

/// Where a cut may fall on the native mutation path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultSite {
    /// The frame was received and admitted syntactically; nothing proposed.
    BeforePropose,
    /// The owner committed and published the outcome; no reply was written.
    AfterCommitBeforeReply,
}
#[cfg(feature = "test-support")]
impl FaultSite {
    const ALL: [Self; 2] = [Self::BeforePropose, Self::AfterCommitBeforeReply];
    fn name(self) -> &'static str {
        match self {
            Self::BeforePropose => "before-propose",
            Self::AfterCommitBeforeReply => "after-commit-before-reply",
        }
    }
    fn index(self) -> usize {
        match self {
            Self::BeforePropose => 0,
            Self::AfterCommitBeforeReply => 1,
        }
    }
}

/// The configured cut, read once from `FOCAL_FAULT`.
#[cfg(feature = "test-support")]
fn configured() -> Option<(FaultSite, u32)> {
    use std::sync::OnceLock;
    static CONFIGURED: OnceLock<Option<(FaultSite, u32)>> = OnceLock::new();
    *CONFIGURED.get_or_init(|| {
        let value = std::env::var("FOCAL_FAULT").ok()?;
        let (site, count) = value.split_once(':')?;
        let site = FaultSite::ALL.into_iter().find(|s| s.name() == site)?;
        let count = count.parse::<u32>().ok().filter(|n| *n > 0)?;
        Some((site, count))
    })
}

/// Arrive at `site`: abort when this is the configured arrival.
pub fn hit(site: FaultSite) {
    #[cfg(feature = "test-support")]
    {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTS: [AtomicU32; 2] = [AtomicU32::new(0), AtomicU32::new(0)];
        let Some((wanted, count)) = configured() else {
            return;
        };
        if wanted != site {
            return;
        }
        let Some(counter) = COUNTS.get(site.index()) else {
            return;
        };
        if counter.fetch_add(1, Ordering::SeqCst).saturating_add(1) == count {
            use std::io::Write as _;
            let _ = writeln!(
                std::io::stderr().lock(),
                "FOCAL_FAULT: aborting at {}",
                site.name()
            );
            std::process::abort();
        }
    }
    #[cfg(not(feature = "test-support"))]
    {
        let _ = site;
    }
}
