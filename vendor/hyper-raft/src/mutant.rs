//! Planted defects (`docs/sim.md` §4.6), for the tests that show the oracles catch them: each puts
//! back a rule this core keeps. A member plants one only through [`crate::RawNode::plant`], which
//! the `mutants` feature alone compiles and no consumer enables; without it no member holds one,
//! and every check of one is false.

/// A rule taken out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mutant {
    /// A leader commits an index a quorum holds whatever term its entry bears: counting an older
    /// term's replicas (Ongaro's thesis §3.6.2, Figure 3.7).
    OlderTermCommit,
    /// A leader answers a read at its commit before it has committed an entry of its term (the
    /// thesis's §6.4 step 1 taken out).
    ReadBeforeFirstCommit,
    /// A `Ready` that holds a vote leaves at once, before its write of the vote is durable (I1,
    /// `docs/durable.md` §3).
    VoteBeforeDurable,
    /// The fast track counts a member holding the entry beside its log before its log holds an
    /// entry of the leader's term (the first rule of `docs/raft.md` §3, seed 9843).
    FastBesideAnyTerm,
    /// The fast track counts a fast quorum of the configuration in force alone, not of every one a
    /// member of the term may count by (the second rule, seed 54104).
    FastAnyConfiguration,
}
