use super::*;
use crate::{SessionId, TenantId};
use serde::de::DeserializeOwned;
use std::fmt::Debug;

const LEGACY: &[u8] =
    include_bytes!("../../../focal-core/fixtures/durable-v1-inputs/legacy-inputs.rows");
const MANAGED: &[u8] =
    include_bytes!("../../../focal-core/fixtures/durable-v1-inputs/managed-inputs.rows");

fn exact<T>(value: &T, original: &[u8])
where
    T: V1 + Serialize + DeserializeOwned + PartialEq + Debug,
{
    assert_eq!(postcard::to_stdvec(value).unwrap(), original);
    assert_eq!(postcard::to_stdvec(&Ref(value)).unwrap(), original);
    let (Value(decoded), remaining) = postcard::take_from_bytes::<Value<T>>(original).unwrap();
    assert!(remaining.is_empty());
    assert_eq!(&decoded, value);
    assert_eq!(&postcard::from_bytes::<T>(original).unwrap(), value);
}

fn original_rows<T>(original: &[u8]) -> Vec<T>
where
    T: V1 + Serialize + DeserializeOwned + PartialEq + Debug,
{
    let values: Vec<T> = postcard::from_bytes(original).unwrap();
    exact(&values, original);
    for cut in [0, 1, original.len() / 2, original.len() - 1] {
        assert!(postcard::from_bytes::<Value<Vec<T>>>(&original[..cut]).is_err());
    }
    values
}

#[test]
fn cause_ordinals_and_attestation_booleans_keep_the_original_shapes() {
    exact(
        &Cause::Root(RootCommandId([1; 16])),
        &[&[0][..], &[1; 16]].concat(),
    );
    exact(
        &Cause::Claim(ClaimId([2; 16])),
        &[&[1][..], &[2; 16]].concat(),
    );
    assert!(postcard::from_bytes::<Value<Cause>>(&[2]).is_err());
    assert!(postcard::from_bytes::<Value<Cause>>(&[128]).is_err());

    for durable in [false, true] {
        for schema_valid in [false, true] {
            exact(
                &EvidenceAttestation {
                    descriptor_hash: ContentHash([3; 32]),
                    custody_revision: 300,
                    durable,
                    schema_valid,
                },
                &[
                    &[3; 32][..],
                    &[0xac, 2, u8::from(durable), u8::from(schema_valid)],
                ]
                .concat(),
            );
        }
    }
}

#[test]
fn authority_preserves_exact_ordered_custody_facts_and_historical_counters() {
    let authority = AuthorityContext {
        runtime: true,
        cause: Cause::Claim(ClaimId([2; 16])),
        policy_revision: 128,
        logical_time: u64::MAX,
        evidence: vec![
            EvidenceAttestation {
                descriptor_hash: ContentHash([3; 32]),
                custody_revision: 0,
                durable: false,
                schema_valid: true,
            },
            EvidenceAttestation {
                descriptor_hash: ContentHash([4; 32]),
                custody_revision: 300,
                durable: true,
                schema_valid: false,
            },
        ],
    };
    let original = [
        &[1, 1][..],
        &[2; 16],
        &[0x80, 1],
        &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1],
        &[2],
        &[3; 32],
        &[0, 0, 1],
        &[4; 32],
        &[0xac, 2, 1, 0],
    ]
    .concat();
    exact(&authority, &original);
    for cut in 0..original.len() {
        assert!(postcard::from_bytes::<Value<AuthorityContext>>(&original[..cut]).is_err());
    }
    exact(
        &AuthorityContext {
            runtime: false,
            cause: Cause::Root(RootCommandId::default()),
            policy_revision: 0,
            logical_time: 0,
            evidence: vec![],
        },
        &[0; 21],
    );
}

#[test]
fn managed_identity_decoding_does_not_invent_admission_or_rewrite_zero_values() {
    let stream = RequestStreamIdentity {
        cluster: [0; 16],
        ledger: LedgerId {
            tenant: TenantId::default(),
            session: SessionId::default(),
        },
        principal: ParticipantId::default(),
        slot: 0,
        generation: 0,
    };
    assert!(!stream.is_valid());
    exact(&stream, &[0; 66]);
    let key = ManagedRequestKey {
        stream,
        ordinal: 0,
        id: RequestId::default(),
    };
    assert!(!key.is_valid());
    exact(&key, &[0; 83]);

    let stream = RequestStreamIdentity {
        cluster: [11; 16],
        ledger: LedgerId {
            tenant: TenantId([12; 16]),
            session: SessionId([13; 16]),
        },
        principal: ParticipantId([14; 16]),
        slot: u32::MAX,
        generation: u64::MAX,
    };
    let original = [
        &[11; 16][..],
        &[12; 16],
        &[13; 16],
        &[14; 16],
        &[0xff, 0xff, 0xff, 0xff, 0x0f],
        &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1],
    ]
    .concat();
    exact(&stream, &original);
    let key = ManagedRequestKey {
        stream,
        ordinal: 128,
        id: RequestId([15; 16]),
    };
    exact(&key, &[original.as_slice(), &[0x80, 1], &[15; 16]].concat());
}

#[test]
fn complete_legacy_and_managed_input_rows_match_original_captured_bytes() {
    let legacy = original_rows::<AuthenticatedInput>(LEGACY);
    let managed = original_rows::<ManagedAuthenticatedInput>(MANAGED);
    // Each command has one rich and one alternate/empty historical shape.
    assert_eq!(legacy.len(), 58);
    assert_eq!(managed.len(), 58);

    let mut new_claims = 0;
    let mut new_validations = 0;
    let mut new_artifacts = 0;
    let inspect_claim = |claim: &NewClaim, claims: &mut usize, validations: &mut usize| {
        exact(claim, &postcard::to_stdvec(claim).unwrap());
        *claims += 1;
        for validation in &claim.validations {
            exact(validation, &postcard::to_stdvec(validation).unwrap());
            *validations += 1;
        }
    };
    for input in &legacy {
        // The complete captured input above, not these live shape cross-checks,
        // is the independent historical byte oracle.
        exact(
            &input.authority,
            &postcard::to_stdvec(&input.authority).unwrap(),
        );
        match &input.command {
            Command::GenerateClaim { claim }
            | Command::SupersedeClaim {
                successor: claim, ..
            } => {
                inspect_claim(claim, &mut new_claims, &mut new_validations);
            }
            Command::GenerateClaimBatch { claims } => {
                for claim in claims {
                    inspect_claim(claim, &mut new_claims, &mut new_validations);
                }
            }
            Command::AttachArtifact { artifact, .. }
            | Command::RegisterArtifact { artifact }
            | Command::FailTestamentGeneration {
                error: artifact, ..
            } => {
                exact(artifact, &postcard::to_stdvec(artifact).unwrap());
                new_artifacts += 1;
            }
            _ => {}
        }
    }
    assert!(new_claims > 0);
    assert!(new_validations > 0);
    assert!(new_artifacts > 0);
    for input in &managed {
        exact(&input.key, &postcard::to_stdvec(&input.key).unwrap());
    }
}
