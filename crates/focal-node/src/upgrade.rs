//! The capability level this binary announces and the upgrade fence it is
//! held to ([24](../../../docs/archictecutre/24-placement-execution-and-fleet-control.md)
//! §21; [08](../../../docs/archictecutre/08-stepped-complexity-and-deployment.md)
//! §10). A node reports its level with its load; the founder raises the
//! fence only once every enrolled node reports at least that level; a
//! binary announcing less than the committed fence refuses to serve.
use focal_enrollment::UpgradeFence;

/// The capability level this binary implements. Raised by a release that
/// activates behaviour older binaries cannot follow; the fence gates that
/// behaviour until every node runs such a binary.
pub const CAPABILITY_LEVEL: u32 = 10;
/// The level at which the issuer succeeds itself (24 §11): a credential
/// issued under a successor issuer is presented with the predecessor's
/// endorsement, which a binary below this level cannot verify.
pub const ISSUER_SUCCESSION_LEVEL: u32 = 2;
/// The level at which consensus carries fields beyond raft-rs's (24 §21,
/// 27 §15.9): a refusal's `kept` (R17) and `lost`, which a binary below this
/// level cannot read. A node opens every group it hosts on `Wire::Kept` from
/// its first start at which the fence it knows opens this level
/// (`storage_level::member_wire`): from genesis in a cluster founded at it,
/// and from the next start in one whose fence rose while the node ran, as its
/// storage is converted then.
pub const RAFT_KEPT_LEVEL: u32 = 3;
/// The level at which a node's groups live on hyper-log under hyper-durable's shell (27 §15.8): a
/// node converts its WAL at its first start once the fence opens it, and a cluster founded at it
/// starts there. A binary below this level reads the converted WAL's fence as one it does not know
/// and refuses to open.
pub const STORAGE_LEVEL: u32 = 4;
/// The level at which a session's authority seals by the anchored rule (F12; 26 §4a): a seal
/// names what it seals and applies after the commits its custody took, the owner closes
/// generations itself as its window comes under pressure through a closing seal, and a seal is
/// held for the pipeline to drain. Its records (`FOCALSO2`) are ones a binary below this level
/// cannot read; every binary at it applies both kinds.
pub const SEAL_LEVEL: u32 = 5;
/// The level at which a session's authority retires families in batches among its writes
/// (doc 31 M2): every replica derives a batch's families where its record applies, so no exact
/// prefix is waited for and no write is held. Its records (`FCRETIRE`) are ones a binary below
/// this level cannot read; every binary at it applies both kinds, and below it the authority
/// retires a family at a time (`FOCALRT1`).
pub const RETIRE_LEVEL: u32 = 6;
/// The level at which a session's authority retires to every replica's own cold stream (doc 31
/// M2, A2): its records (`FCRETIRE` version 2) name each family's frame, which every replica
/// appends where the record applies, and its checkpoints carry the streams' section; nothing is
/// sealed under custody. A binary below this level reads neither; every binary at it keeps the
/// streams whatever the fence, so it follows a leader that retires cold.
pub const COLD_LEVEL: u32 = 7;
/// The level at which a session's native engine admits within the wide share of its allowance
/// (doc 31 M3): the allowance less its completion reserve, where below it half. Every binary at
/// the level applies within the whole allowance whatever its share, so it follows a leader of
/// either; one below it holds half, so a leader admits wide only once the fence it knew as the
/// session's hosting was built opens this level.
pub const SHARE_LEVEL: u32 = 8;
/// The level at which a node's content store seals its object files (29 §11): each under its
/// domain's key, named by a keyed hash, which a binary below this level cannot open. A node takes
/// the form from the fence it last recorded (`storage_level::content_form`) as its store is opened
/// at its start: below the level the store keeps the clear layout every earlier binary reads, so a
/// rollback below the fence still finds its content; from it the store seals what it installs and
/// converts what it holds in the clear as it serves, from genesis in a cluster founded there.
pub const CONTENT_LEVEL: u32 = 9;
/// The level at which every store a node keeps beside its log and content is sealed at rest (29
/// §12), beginning with a session's checkpoint seeds: each directory under a key of its own, each
/// chunk sealed whole and named by a keyed hash, which a binary below this level cannot open. A
/// node takes the form from the fence it last recorded (`storage_level::at_rest_form`) as each
/// store is opened: below the level every store keeps the clear layout every earlier binary reads,
/// so a rollback below the fence still finds it; from it each seals what it writes and converts
/// what it holds in the clear as it serves, from genesis in a cluster founded there.
pub const AT_REST_LEVEL: u32 = 10;
/// The environment variable that lowers the announced level for a staged
/// rollout or a rehearsal; it can never raise it.
pub const ANNOUNCED_LEVEL_ENV: &str = "FOCAL_CAPABILITY_LEVEL";

/// The level this process announces: the compiled level, or a lower one
/// the operator set through `FOCAL_CAPABILITY_LEVEL`.
pub fn announced_level() -> u32 {
    #[cfg(test)]
    if let Some(level) = TEST_ANNOUNCED.with(std::cell::Cell::get) {
        return level.min(CAPABILITY_LEVEL);
    }
    std::env::var(ANNOUNCED_LEVEL_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .map_or(CAPABILITY_LEVEL, |level| level.min(CAPABILITY_LEVEL))
}
#[cfg(test)]
thread_local! {
    /// The level a test's thread announces in place of the compiled one: the
    /// operator's staged rollout (`FOCAL_CAPABILITY_LEVEL`) for one test, where
    /// the environment is every test's at once.
    static TEST_ANNOUNCED: std::cell::Cell<Option<u32>> = const { std::cell::Cell::new(None) };
}
/// The level this test's thread announces until the guard is dropped: a node
/// it founds holds this fence from genesis, below the compiled level where
/// the test exercises what a cluster below a later level runs (its groups on
/// focal-log's WAL, below [`STORAGE_LEVEL`]).
#[cfg(test)]
pub(crate) fn announce_for_test(level: u32) -> AnnouncedForTest {
    AnnouncedForTest(TEST_ANNOUNCED.with(|announced| announced.replace(Some(level))))
}
/// What [`announce_for_test`] replaced, put back when the test is done.
#[cfg(test)]
pub(crate) struct AnnouncedForTest(Option<u32>);
#[cfg(test)]
impl Drop for AnnouncedForTest {
    fn drop(&mut self) {
        let before = self.0;
        TEST_ANNOUNCED.with(|announced| announced.set(before));
    }
}
/// The level below the storage level a test founds a WAL node at.
#[cfg(test)]
pub(crate) const BELOW_STORAGE_LEVEL: u32 = STORAGE_LEVEL - 1;
/// Whether a binary announcing `announced` may serve under `fence`.
pub fn admits(fence: UpgradeFence, announced: u32) -> bool {
    fence.level <= announced
}
/// Whether behaviour gated on `level` is open under the committed fence.
pub fn opened(fence: UpgradeFence, level: u32) -> bool {
    fence.level >= level
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn the_fence_admits_at_or_above_its_level_and_opens_at_or_below() {
        let fence = UpgradeFence {
            level: 2,
            activated_at: 1,
            revision: 1,
        };
        assert!(!admits(fence, 1));
        assert!(admits(fence, 2));
        assert!(admits(fence, 3));
        assert!(opened(fence, 1));
        assert!(opened(fence, 2));
        assert!(!opened(fence, 3));
        assert!(admits(UpgradeFence::default(), 0));
        assert!(announced_level() <= CAPABILITY_LEVEL);
    }
}
