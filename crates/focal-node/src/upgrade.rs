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
pub const CAPABILITY_LEVEL: u32 = 3;
/// The level at which the issuer succeeds itself (24 §11): a credential
/// issued under a successor issuer is presented with the predecessor's
/// endorsement, which a binary below this level cannot verify.
pub const ISSUER_SUCCESSION_LEVEL: u32 = 2;
/// The level at which consensus carries fields beyond raft-rs's (24 §21,
/// 27 §15.9): a refusal's `kept` (R17) and `lost`, which a binary below this
/// level cannot read. Every group a node hosts runs `Wire::Kept` once the
/// fence opens it.
pub const RAFT_KEPT_LEVEL: u32 = 3;
/// The environment variable that lowers the announced level for a staged
/// rollout or a rehearsal; it can never raise it.
pub const ANNOUNCED_LEVEL_ENV: &str = "FOCAL_CAPABILITY_LEVEL";

/// The level this process announces: the compiled level, or a lower one
/// the operator set through `FOCAL_CAPABILITY_LEVEL`.
pub fn announced_level() -> u32 {
    std::env::var(ANNOUNCED_LEVEL_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .map_or(CAPABILITY_LEVEL, |level| level.min(CAPABILITY_LEVEL))
}
/// Whether a binary announcing `announced` may serve under `fence`.
pub fn admits(fence: UpgradeFence, announced: u32) -> bool {
    fence.level <= announced
}
/// Whether behaviour gated on `level` is open under the committed fence.
pub fn opened(fence: UpgradeFence, level: u32) -> bool {
    fence.level >= level
}
/// The fields beyond raft-rs's every group of this node carries under the
/// committed fence: `Wire::Kept` (R17's `kept` and `lost`) once it opens
/// `RAFT_KEPT_LEVEL`, which it does only when every enrolled node runs a
/// binary that reads them; `Wire::Frozen` before.
pub fn raft_wire(fence: UpgradeFence) -> focal_consensus::Wire {
    if opened(fence, RAFT_KEPT_LEVEL) {
        focal_consensus::Wire::Kept
    } else {
        focal_consensus::Wire::Frozen
    }
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
    /// R17's fields go on the wire only once the fence opens their level,
    /// which it does only when every enrolled node reads them.
    #[test]
    fn the_wire_carries_kept_and_lost_only_from_their_level() {
        let fence = |level| UpgradeFence {
            level,
            activated_at: 1,
            revision: 1,
        };
        assert_eq!(
            raft_wire(UpgradeFence::default()),
            focal_consensus::Wire::Frozen
        );
        assert_eq!(raft_wire(fence(2)), focal_consensus::Wire::Frozen);
        assert_eq!(
            raft_wire(fence(RAFT_KEPT_LEVEL)),
            focal_consensus::Wire::Kept
        );
        const { assert!(CAPABILITY_LEVEL >= RAFT_KEPT_LEVEL) };
    }
}
