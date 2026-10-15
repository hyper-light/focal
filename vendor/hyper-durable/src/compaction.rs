//! When a group's log is compacted (mantle note 32 R22, `docs/durable.md` §6.1): Ongaro's thesis's
//! rule, with slates' wait for the members still taking what was applied.

/// When a group's log is due to be compacted: once the applied entries it holds exceed the image it
/// was last compacted to, times the owner's expansion factor (Ongaro's thesis §5.1.2, "When to
/// snapshot", pp. 54–55: "Servers take a snapshot once the size of the log exceeds the size of the
/// previous snapshot times a configurable expansion factor"). The thesis weighs the previous image
/// because the next one's size is unknown until it is written. The factor trades the disk's
/// bandwidth for its room: at `e`, an image is written for every `e` times its bytes of log, a
/// share of `1 / (1 + e)` of what the group writes, and the disk holds about `2 + e` times the
/// image at the most (the image in force, the log `e` times it, and the image being written; the
/// thesis's example is 4: a fifth of the bandwidth, six times the state). It is the owner's to
/// state, for it knows what its disk and its machine's images cost; slates states one, and zero
/// makes a log due whenever it holds anything applied.
///
/// A leader waits while a member lacks what it applied and the log holds no more than twice the
/// threshold ([`HELD_FOR_FOLLOWERS`]), so that the member is sent the entries it lacks, not an
/// image; past that, it is due.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Compaction {
    /// The times its image's bytes the applied entries a log holds may come to before it is due.
    pub expansion: u64,
}

/// Cited (slates `crates/cluster/src/fold.rs`, measured 2026-09-28): the thresholds' worth of
/// applied entries a leader's log holds while a member still lacks them, the rule's one and one
/// for the member still taking them. A leader that compacted the moment a majority held its entries
/// sent the third voter of three a whole image in place of the round of entries it lacked, at every
/// compaction, so that it never compacted itself. Past it, a member that far behind, or gone, is
/// sent an image and cannot pin the log, which then holds at most twice the expansion in images'
/// worth of applied entries.
pub(crate) const HELD_FOR_FOLLOWERS: u64 = 2;

impl Compaction {
    /// Whether a log that holds `held` bytes of applied entries is due, its last image `image`
    /// bytes, and `lagging` when it leads a member that lacks what it applied. A threshold past
    /// what a `u64` counts saturates: no log reaches it.
    pub(crate) fn due(self, held: u64, image: u64, lagging: bool) -> bool {
        let threshold = image.saturating_mul(self.expansion);
        held > threshold && !(lagging && held <= threshold.saturating_mul(HELD_FOR_FOLLOWERS))
    }
}
