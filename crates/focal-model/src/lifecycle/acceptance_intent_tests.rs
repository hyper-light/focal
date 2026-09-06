use super::*;
use crate::{ObjectId, ObjectRevision, SessionId, TenantId};

fn policy() -> AcceptancePolicy {
    let definition = crate::lifecycle::claim::tests::definition(4);
    AcceptancePolicy::memory_fixture(definition.binding, definition.issuer)
}

#[test]
fn acceptance_intent_survives_reconstruction_copy_and_creation_revision_assignment() {
    let original = policy();
    let fingerprint = original.intent_fingerprint();
    assert_eq!(policy().intent_fingerprint(), fingerprint);
    let mut copied = original.try_copy(original.copy_charge().unwrap()).unwrap();
    assert_eq!(copied.intent_fingerprint(), fingerprint);
    copied.claim.revision = ObjectRevision(900);
    assert_eq!(copied.intent_fingerprint(), fingerprint);
    drop(original);
    assert_eq!(copied.intent_fingerprint(), fingerprint);
}

#[test]
fn acceptance_intent_rejects_same_binding_policy_substitution_and_omission() {
    let original = policy();
    let fingerprint = original.intent_fingerprint();
    let mutations: &[fn(&mut AcceptancePolicy)] = &[
        |p| p.claim.ledger.tenant = TenantId::from_u128(90),
        |p| p.claim.ledger.session = SessionId::from_u128(90),
        |p| p.claim.object = ObjectId::from_u128(90),
        |p| p.claim.content = ContentHash([90; 32]),
        |p| p.issuer = ParticipantId::from_u128(90),
        |p| p.declarations[0].definition = DefinitionStamp::fixture(),
        |p| p.declarations[0].binding.revision = ObjectRevision(90),
        |p| p.declarations[0].index = 90,
        |p| p.declarations[0].mode = ValidationMode::Observe,
        |p| p.declarations[0].target = ObligationTarget::Increment,
        |p| p.declarations.reverse(),
        |p| {
            p.declarations.pop();
        },
        |p| p.slots[0].slot = 90,
        |p| p.slots[0].missing_declaration_index = 90,
        |p| p.slots[0].mode = ValidationMode::Observe,
        |p| p.slots[0].checks[0].declaration_index = 90,
        |p| p.slots[0].checks[0].validation = ValidationId::from_u128(90),
        |p| p.slots[0].checks[0].mode = ValidationMode::Observe,
        |p| p.slots[0].checks.clear(),
        |p| p.slots.clear(),
    ];
    for (index, mutation) in mutations.iter().enumerate() {
        let mut changed = original.try_copy(original.copy_charge().unwrap()).unwrap();
        mutation(&mut changed);
        assert_ne!(
            changed.intent_fingerprint(),
            fingerprint,
            "mutation {index}"
        );
    }
    assert_eq!(original.intent_fingerprint(), fingerprint);
}
