//! The retention floor (26 §3): the least native prefix every obligation
//! allows history to be reclaimed through. Physical reclamation is per
//! object with the archive (doc 04 §12): a terminal-and-released object,
//! its dependents and their events leave the core together behind a typed
//! continuation once an archive bundle holds them. What this module settles
//! is the floor that bounds any such reclamation and what holds it where it
//! is, so the operator sees why history is retained.
use super::*;

/// What holds the retention floor where it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionBlocker {
    /// A registered consumer still needs history above the floor.
    Cursors,
    /// The archive holds nothing above the floor; reclaiming further would
    /// lose proof.
    Archive,
}
/// The retention floor of one session and every input it was derived from:
/// the floor is the least prefix every obligation allows, never past what
/// is published.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionReport {
    pub published: SessionSeq,
    pub cursors: SessionSeq,
    pub archived: SessionSeq,
    pub floor: SessionSeq,
    pub blocker: RetentionBlocker,
    /// Families retired to the archive through the applied prefix (26 §4).
    pub retired: u64,
    /// A retirement record this authority proposed is still in flight.
    pub retiring: bool,
}
impl RetentionReport {
    pub fn new(published: SessionSeq, cursors: SessionSeq, archived: SessionSeq) -> Self {
        let (floor, blocker) = if archived <= cursors {
            (archived, RetentionBlocker::Archive)
        } else {
            (cursors, RetentionBlocker::Cursors)
        };
        Self {
            published,
            cursors,
            archived,
            floor: floor.min(published),
            blocker,
            retired: 0,
            retiring: false,
        }
    }
    pub fn with_retirement(mut self, retired: u64, retiring: bool) -> Self {
        self.retired = retired;
        self.retiring = retiring;
        self
    }
    /// Whether a family whose last event sits at `through` may leave: the
    /// complete sequences every registered consumer allows to retire reach
    /// it (26 §4); with no consumer, everything published may.
    pub fn allows_family(&self, through: SessionSeq) -> bool {
        through <= self.cursors
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn the_floor_is_the_least_obligation_and_never_past_publication() {
        let report = RetentionReport::new(SessionSeq(100), SessionSeq(80), SessionSeq(50));
        assert_eq!(report.floor, SessionSeq(50));
        assert_eq!(report.blocker, RetentionBlocker::Archive);
        let report = RetentionReport::new(SessionSeq(100), SessionSeq(30), SessionSeq(50));
        assert_eq!(report.floor, SessionSeq(30));
        assert_eq!(report.blocker, RetentionBlocker::Cursors);
        let report = RetentionReport::new(SessionSeq(20), SessionSeq(90), SessionSeq(90));
        assert_eq!(report.floor, SessionSeq(20));
        assert_eq!(report.blocker, RetentionBlocker::Archive);
    }
    #[test]
    fn a_family_may_leave_once_every_consumer_allows_its_last_event_to_retire() {
        let report = RetentionReport::new(SessionSeq(100), SessionSeq(80), SessionSeq(0))
            .with_retirement(3, true);
        assert!(report.allows_family(SessionSeq(80)));
        assert!(!report.allows_family(SessionSeq(81)));
        assert_eq!(report.retired, 3);
        assert!(report.retiring);
        let quiet = RetentionReport::new(SessionSeq(100), SessionSeq(100), SessionSeq(0));
        assert!(quiet.allows_family(SessionSeq(100)));
    }
}
