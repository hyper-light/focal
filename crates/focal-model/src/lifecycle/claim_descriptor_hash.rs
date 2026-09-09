//! Explicit native-only identity framing, shared by typed and value sources.
use super::*;

pub(super) struct Hash(blake3::Hasher);
impl Hash {
    fn field(&mut self, value: &[u8]) {
        self.0.update(&(value.len() as u128).to_be_bytes());
        self.0.update(value);
    }
    pub(super) fn count(&mut self, value: usize) {
        self.field(&(value as u128).to_be_bytes());
    }
    fn code(&mut self, value: u16) {
        self.field(&value.to_be_bytes());
    }
    pub(super) fn new(fields: ClaimFields<'_>) -> Self {
        let mut hash = Self(blake3::Hasher::new_derive_key(
            "focal/native/claim-content/1",
        ));
        hash.field(&fields.ledger.tenant.0);
        hash.field(&fields.ledger.session.0);
        hash.code(fields.schema);
        hash.field(&fields.occurrence.0);
        hash.field(fields.description.as_bytes());
        // Schema-1 hashes are frozen: the policy section exists only from
        // schema 2, where its absence is hashed explicitly.
        if fields.schema >= 2 {
            match fields.policy {
                Some(policy) => {
                    hash.code(1);
                    hash.field(&[u8::from(policy.corrective_allowed)]);
                    hash.field(&policy.max_follow_ups.to_be_bytes());
                    hash.field(&[u8::from(policy.single_issuer)]);
                    hash.code(match policy.escalation {
                        crate::Escalation::None => 0,
                        crate::Escalation::Holder => 1,
                        crate::Escalation::Evaluator => 2,
                    });
                }
                None => hash.code(0),
            }
        }
        hash
    }
    pub(super) fn relation(&mut self, relation: &Relation) {
        self.code(match relation.kind {
            RelationKind::Issuer => 1,
            RelationKind::Subject => 2,
            RelationKind::Evaluator => 3,
            RelationKind::ClaimAction => 4,
            RelationKind::Supersedes => 5,
            RelationKind::DependsOn => 6,
            RelationKind::Awaits => 7,
            RelationKind::CausedBy => 8,
            RelationKind::Refines => 9,
            RelationKind::ConflictsWith => 10,
            RelationKind::DerivedFrom => 11,
            RelationKind::Reviews => 12,
            RelationKind::Amends => 13,
            RelationKind::ContributedBy => 14,
            RelationKind::Invalidates => 15,
        });
        match &relation.target {
            RelationTarget::Participant(value) => {
                self.code(1);
                self.field(&value.0);
            }
            RelationTarget::Object(value) => {
                self.code(2);
                self.field(&value.ledger.tenant.0);
                self.field(&value.ledger.session.0);
                self.code(match value.kind {
                    ObjectKind::Claim => 1,
                    ObjectKind::Testament => 2,
                    ObjectKind::Validation => 3,
                    ObjectKind::Artifact => 4,
                });
                self.field(&value.id.0);
            }
            RelationTarget::Action(value) => {
                self.code(3);
                self.code(match value {
                    ActionType::Work => 1,
                    ActionType::Consultation => 2,
                    ActionType::Challenge => 3,
                    ActionType::Feedback => 4,
                    ActionType::Approval => 5,
                    ActionType::Summon => 6,
                    ActionType::Handoff => 7,
                    ActionType::Evaluation => 8,
                    ActionType::Correction => 9,
                    ActionType::Teardown => 10,
                });
            }
            RelationTarget::Root(value) => {
                self.code(4);
                self.field(&value.0);
            }
            RelationTarget::Evidence(value) => {
                self.code(5);
                self.field(&value.id.0);
                self.field(&value.hash.0);
            }
        }
    }
    pub(super) fn scope(&mut self, scope: ScopeSpec<'_>) {
        self.code(match scope.kind {
            ScopeKind::File => 1,
            ScopeKind::Symbol => 2,
            ScopeKind::Api => 3,
            ScopeKind::TestSurface => 4,
            ScopeKind::Component => 5,
            ScopeKind::UxSurface => 6,
        });
        self.field(scope.key.as_bytes());
    }
    pub(super) fn requirement(&mut self, value: RequirementRef) {
        self.field(&value.id.0);
        self.field(&value.specification.0);
    }
    pub(super) fn slot(&mut self, value: ClaimSlotFields) {
        self.field(&value.slot.to_be_bytes());
        self.field(&value.missing_declaration_index.to_be_bytes());
        self.code(match value.mode {
            ValidationMode::Observe => 1,
            ValidationMode::Required => 2,
        });
        self.count(value.checks);
    }
    pub(super) fn check(&mut self, check: CheckPolicy) {
        self.field(&check.declaration_index.to_be_bytes());
        self.field(&check.validation.0);
        self.code(match check.mode {
            ValidationMode::Observe => 1,
            ValidationMode::Required => 2,
        });
    }
    pub(super) fn finish(mut self, deadline: Option<Deadline>) -> ContentHash {
        match deadline {
            None => self.code(0),
            Some(deadline) => {
                self.code(1);
                self.field(&deadline.timer.0);
                self.field(&deadline.generation.to_be_bytes());
                self.field(&deadline.at.to_be_bytes());
            }
        }
        ContentHash(*self.0.finalize().as_bytes())
    }
}
pub(super) fn intent(id: ClaimId, content: ContentHash) -> ContentHash {
    let mut hash = blake3::Hasher::new_derive_key("focal/native/claim-intent/1");
    hash.update(&id.0);
    hash.update(&content.0);
    ContentHash(*hash.finalize().as_bytes())
}
