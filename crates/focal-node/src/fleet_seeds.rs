//! Why a replica's seeded checkpoint waits on its chunks (25 §5), as its owner keeps it for the
//! replica's diagnostics: the last refusal of a pull or an install of one of them, and how many
//! there were, for the checkpoint they were refused for. Its host let go of every refusal, and a
//! restarted follower waited on one chunk for the rest of a run with nothing on the node to say
//! why (fleet33-kill12 k12, 2026-10-29).
use super::*;
use focal_client::admin::AdminSeedRefusal;

/// What a host's service of a seeded checkpoint's chunks was refused with before a chunk reached
/// the replica (`managed_support::service_seed`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SeedRefusal {
    /// This node holds no custody policy for the ledger: it has no peer to ask.
    Policy,
    /// No peer of the ledger's custody policy gave a chunk: the last asked and its answer.
    Pull { peer: u64, error: String },
}
impl SeedRefusal {
    /// What the refusal holds: its own size and its answer's bytes.
    fn bytes(&self) -> usize {
        let held = match self {
            Self::Policy => 0,
            Self::Pull { error, .. } => error.capacity(),
        };
        std::mem::size_of::<Self>().saturating_add(held)
    }
}

/// Where a refusal came: no policy to ask peers by, a pull no peer answered with the chunk, or
/// the replica's install of a chunk it was given.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Policy,
    Pull,
    Install,
}
impl Stage {
    fn name(self) -> &'static str {
        match self {
            Self::Policy => "policy",
            Self::Pull => "pull",
            Self::Install => "install",
        }
    }
}

/// The last refusal, for the checkpoint at `at` (its Raft index and term).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Refused {
    at: (u64, u64),
    refused: u64,
    stage: Stage,
    peer: Option<u64>,
    error: String,
}

/// What an owner keeps of its seeded checkpoint's refusals: the latest alone, with how many came
/// for the same checkpoint; one pending no more takes it with it.
#[derive(Debug, Default)]
pub(super) struct SeedRefusals(Option<Refused>);
impl SeedRefusals {
    fn note(
        &mut self,
        pending: Option<(u64, u64)>,
        stage: Stage,
        peer: Option<u64>,
        error: String,
    ) {
        let Some(at) = pending else {
            self.0 = None;
            return;
        };
        let refused = match &self.0 {
            Some(last) if last.at == at => last.refused.saturating_add(1),
            _ => 1,
        };
        self.0 = Some(Refused {
            at,
            refused,
            stage,
            peer,
            error,
        });
    }
    /// A host's refusal, for the checkpoint pending at `pending`.
    pub(super) fn refused(&mut self, pending: Option<(u64, u64)>, refusal: SeedRefusal) {
        match refusal {
            SeedRefusal::Policy => self.note(
                pending,
                Stage::Policy,
                None,
                "no custody policy for the ledger".into(),
            ),
            SeedRefusal::Pull { peer, error } => self.note(pending, Stage::Pull, Some(peer), error),
        }
    }
    /// The replica's refusal of a chunk it was given, for the checkpoint pending at `pending`.
    pub(super) fn install_refused(&mut self, pending: Option<(u64, u64)>, error: &LedgerError) {
        self.note(pending, Stage::Install, None, error.to_string());
    }
    /// What the diagnostics say of the checkpoint pending at `pending`: nothing for another.
    pub(super) fn current(&self, pending: Option<(u64, u64)>) -> Option<AdminSeedRefusal> {
        let last = self.0.as_ref()?;
        (pending == Some(last.at)).then(|| AdminSeedRefusal {
            refused: last.refused,
            stage: last.stage.name().into(),
            peer: last.peer,
            error: last.error.clone(),
        })
    }
}

impl Owner {
    /// The Raft index and term of the seeded checkpoint this replica waits to install, if any.
    pub(super) fn seed_at(&self) -> Option<(u64, u64)> {
        self.session
            .pending_seed()
            .map(|pending| (pending.index, pending.term))
    }
}

impl ReplicaHost {
    /// Tells the replica why the chunks its seeded checkpoint lacks did not come (25 §5), kept for
    /// its diagnostics as the refusals of their installs are. Refused the queue's room, the next
    /// service tells it again.
    pub(crate) fn seed_refused(&self, refusal: SeedRefusal) -> Result<(), LedgerError> {
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, refusal.bytes())?
            .commit();
        self.sender
            .try_send(Work::SeedRefused(refusal, charge))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The refusals of one checkpoint are counted and the latest kept; another checkpoint's, or
    /// none pending, are not said of it.
    #[test]
    fn the_latest_refusal_of_the_pending_checkpoint_is_kept_and_counted() {
        let mut refusals = SeedRefusals::default();
        let at = Some((12_374, 3));
        assert_eq!(refusals.current(at), None);
        refusals.refused(
            at,
            SeedRefusal::Pull {
                peer: 4,
                error: AccessError::InvalidRequest.to_string(),
            },
        );
        refusals.install_refused(at, &LedgerError::Capacity);
        let current = refusals.current(at).unwrap();
        assert_eq!(current.refused, 2);
        assert_eq!(current.stage, "install");
        assert_eq!(current.peer, None);
        assert_eq!(current.error, LedgerError::Capacity.to_string());
        // A later checkpoint begins again; the one before is not said of it.
        let next = Some((29_902, 3));
        assert_eq!(refusals.current(next), None);
        refusals.refused(next, SeedRefusal::Policy);
        let current = refusals.current(next).unwrap();
        assert_eq!((current.refused, current.stage.as_str()), (1, "policy"));
        assert_eq!(refusals.current(at), None);
        // None pending: nothing is kept.
        refusals.refused(None, SeedRefusal::Policy);
        assert_eq!(refusals.current(next), None);
    }
}
