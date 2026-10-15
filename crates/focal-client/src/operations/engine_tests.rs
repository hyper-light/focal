//! The engine selection and the examples it generates: each engine's
//! examples decode through that engine's decoder, match its schema and
//! round-trip unchanged; the native ones carry far-future deadlines and
//! never target `self`; the V1 claim example is exactly what the native
//! decoder refuses (F09); and the offline and online selection rules are
//! the ones every host applies.
use super::tests::assert_shape;
use super::*;
use crate::ClientError;
use focal_model::{ParticipantId, SessionSeq};
use focal_wire::{NativePeerRole, NativeProfile, NativeStanding};
use serde_json::{Value, json};
use std::collections::BTreeSet;

fn bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

#[test]
fn every_engine_example_decodes_through_its_engine_matches_its_schema_and_round_trips() {
    for wire in [WireProfile::V1, WireProfile::Native] {
        assert_eq!(
            application(wire).len(),
            if wire == WireProfile::V1 { 34 } else { 44 }
        );
        for descriptor in application(wire) {
            let name = descriptor.name;
            assert!(std::ptr::eq(
                find_application(wire, name).unwrap(),
                descriptor
            ));
            let value = example(wire, name).unwrap_or_else(|error| panic!("{name}: {error}"));
            let document = decode_application(wire, name, &bytes(&value))
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(document.wire(), wire, "{name}");
            assert_eq!(document.name(), name, "{name}");
            // The V1 enum names its descriptor constant, the native one the
            // catalogue row; both are the released row for this name.
            assert_eq!(document.descriptor().name, descriptor.name, "{name}");
            assert_eq!(document.descriptor().version, descriptor.version, "{name}");
            assert_eq!(document.descriptor().result_kind, descriptor.result_kind);
            let schema = descriptor.input_schema().unwrap();
            assert_shape(&schema, &schema, &value);
            let fields: BTreeSet<_> = value.as_object().unwrap().keys().collect();
            let schema_fields: BTreeSet<_> =
                schema["properties"].as_object().unwrap().keys().collect();
            if wire == WireProfile::Native {
                // The normalized document carries every released field.
                assert_eq!(fields, schema_fields, "{name}");
            } else {
                assert!(fields.is_subset(&schema_fields), "{name}: {fields:?}");
            }
            // Normalization is a fixpoint: decoding the printed example and
            // re-serializing it changes nothing.
            let canonical: Value =
                serde_json::from_slice(&document.canonical_intent().unwrap()).unwrap();
            assert_eq!(canonical["operation"], name, "{name}");
            assert_eq!(canonical["input"], value, "{name}");
            assert_eq!(example(wire, name).unwrap(), value, "{name}");
            // A forged field is refused by the engine's decoder.
            let mut forged = value.clone();
            forged["forged"] = json!(true);
            assert!(
                decode_application(wire, name, &bytes(&forged)).is_err(),
                "{name}"
            );
        }
        assert!(example(wire, "not.a.released.operation").is_err());
        assert!(decode_application(wire, "not.a.released.operation", b"{}").is_err());
    }
}

/// Every `deadline.at` in `value`, and every string-valued top-level `target`.
fn deadlines_and_targets(value: &Value, deadlines: &mut Vec<u64>, targets: &mut Vec<String>) {
    if let Some(object) = value.as_object() {
        if let Some(at) = object
            .get("deadline")
            .and_then(|d| d.get("at"))
            .and_then(Value::as_u64)
        {
            deadlines.push(at);
        }
        if let Some(target) = object.get("target").and_then(Value::as_str) {
            targets.push(target.to_string());
        }
        for child in object.values() {
            deadlines_and_targets(child, deadlines, targets);
        }
    }
    if let Some(array) = value.as_array() {
        for child in array {
            deadlines_and_targets(child, deadlines, targets);
        }
    }
}

#[test]
fn native_examples_carry_far_future_deadlines_and_never_target_self() {
    let mut claims_with_deadlines = 0;
    for descriptor in native_descriptors() {
        let value = example(WireProfile::Native, descriptor.name).unwrap();
        let (mut deadlines, mut targets) = (Vec::new(), Vec::new());
        deadlines_and_targets(&value, &mut deadlines, &mut targets);
        for at in &deadlines {
            assert_eq!(*at, EXAMPLE_DEADLINE_MS, "{}", descriptor.name);
        }
        for target in &targets {
            assert_ne!(target, "self", "{}", descriptor.name);
            assert_eq!(target.len(), 32, "{}: {target}", descriptor.name);
        }
        if descriptor.mutation && value.get("validations").is_some() {
            // Every claim shape declares its deadlines and addresses a participant.
            assert!(!deadlines.is_empty(), "{}", descriptor.name);
            claims_with_deadlines += 1;
            let validations = value["validations"].as_array().unwrap();
            let receipts = validations
                .iter()
                .filter(|v| v["kind"] == "receipt")
                .count();
            assert_eq!(receipts, 1, "{}", descriptor.name);
            assert_eq!(
                validations[0]["deadline"]["at"],
                json!(EXAMPLE_DEADLINE_MS),
                "{}",
                descriptor.name
            );
        }
    }
    // claim.submit, challenge, consult, correct, follow_up.
    assert_eq!(claims_with_deadlines, 5);
    // The claim example is receipt-only and addressed to the illustrative subject.
    let claim = example(WireProfile::Native, "claim.submit").unwrap();
    assert_eq!(claim["target"], example_ids::SUBJECT);
    assert_eq!(claim["validations"].as_array().unwrap().len(), 1);
    assert_eq!(claim["validations"][0]["kind"], "receipt");
    assert_eq!(claim["action"], "work");
    assert_eq!(claim["max_responses"], 4);
    assert_eq!(
        claim["scope_limits"],
        json!({"scopes": 4, "roots": 16, "children": 8})
    );
    // Monitors and peer verbs name the illustrative committed objects.
    let monitor = example(WireProfile::Native, "monitor.register").unwrap();
    assert_eq!(monitor["claim"], example_ids::CLAIM);
    assert_eq!(monitor["roots"][0]["claim"], example_ids::OTHER);
    assert_eq!(monitor["deadline"]["at"], json!(EXAMPLE_DEADLINE_MS));
    let correct = example(WireProfile::Native, "claim.correct").unwrap();
    assert_eq!(correct["challenge"], example_ids::OTHER);
    assert_eq!(
        correct["verdict"],
        format!("{}@{}", example_ids::VERDICT, example_ids::VERDICT_HASH)
    );
    let challenge = example(WireProfile::Native, "claim.challenge").unwrap();
    assert_eq!(
        challenge["artifact"],
        format!("{}@{}", example_ids::ARTIFACT, example_ids::ARTIFACT_HASH)
    );
    assert_eq!(
        challenge["validations"][1]["evaluator"],
        example_ids::EVALUATOR
    );
    assert_eq!(
        challenge["validations"][1]["handlers"][0]["id"],
        example_ids::HANDLER
    );
    assert_eq!(
        challenge["validations"][1]["handlers"][0]["version"],
        example_ids::HANDLER_VERSION
    );
    let testament = example(WireProfile::Native, "testament.submit").unwrap();
    assert_eq!(
        testament["manifest"][0]["artifact"]["id"],
        example_ids::ARTIFACT
    );
    assert_eq!(
        testament["manifest"][0]["artifact"]["hash"],
        example_ids::ARTIFACT_HASH
    );
    assert_eq!(EXAMPLE_DEADLINE_MS, 4_102_444_800_000);
}

#[test]
fn the_v1_claim_example_is_exactly_what_the_native_decoder_refuses() {
    // F09: the V1 example, normalized by the V1 decoder, carries the V1-only
    // `evidence_schemas` field and no per-declaration deadline; the native
    // decoder refuses it by name instead of the owner refusing it later.
    let v1 = example(WireProfile::V1, "claim.submit").unwrap();
    assert_eq!(v1["target"], "self");
    assert!(v1["validations"][0].get("evidence_schemas").is_some());
    let refused = decode_application(WireProfile::Native, "claim.submit", &bytes(&v1)).unwrap_err();
    match refused {
        InputError::Decode(message) => assert!(message.contains("evidence_schemas"), "{message}"),
        other => panic!("{other:?}"),
    }
    // And the native example is not a V1 document either.
    let native = example(WireProfile::Native, "claim.submit").unwrap();
    assert!(decode_application(WireProfile::V1, "claim.submit", &bytes(&native)).is_err());
}

#[test]
fn offline_selection_takes_the_request_then_the_only_catalogue_then_v1() {
    assert_eq!(select_offline(false, "claim.submit"), Ok(WireProfile::V1));
    assert_eq!(
        select_offline(true, "claim.submit"),
        Ok(WireProfile::Native)
    );
    assert_eq!(
        select_offline(false, "claim.challenge"),
        Ok(WireProfile::Native)
    );
    assert_eq!(
        select_offline(true, "claim.challenge"),
        Ok(WireProfile::Native)
    );
    assert_eq!(
        select_offline(false, "claim.submit_batch"),
        Ok(WireProfile::V1)
    );
    let refused = select_offline(true, "claim.submit_batch").unwrap_err();
    assert_eq!(refused, EngineError::NotNative("claim.submit_batch".into()));
    assert_eq!(
        refused.to_string(),
        "claim.submit_batch is not available on the native engine yet; it arrives with the native index families"
    );
    for native in [false, true] {
        assert_eq!(
            select_offline(native, "future.lifecycle"),
            Err(EngineError::Unknown("future.lifecycle".into()))
        );
    }
    // Every name of either catalogue selects one engine without a request,
    // and the union is what discovery completes.
    let mut union = BTreeSet::new();
    for descriptor in application(WireProfile::V1)
        .iter()
        .chain(application(WireProfile::Native))
    {
        union.insert(descriptor.name);
        assert!(select_offline(false, descriptor.name).is_ok());
    }
    // Nineteen names are shared by both catalogues.
    assert_eq!(union.len(), 34 + 44 - 19);
}

fn standing(principal: ParticipantId) -> NativeStanding {
    NativeStanding {
        principal,
        role: NativePeerRole::Actor,
        profile: NativeProfile::AuthoredV1,
        native_sequence: SessionSeq(3),
        logical_time: 9,
    }
}

#[test]
fn the_probe_resolves_answers_the_way_both_hosts_do_and_online_selection_follows_it() {
    let me = ParticipantId::from_u128(3);
    let native = resolve_probe(Ok(Some(standing(me))), false, me).unwrap();
    assert_eq!(native, Engine::Native(standing(me)));
    assert_eq!(native.wire(), WireProfile::Native);
    assert_eq!(native.standing(), Some(&standing(me)));
    assert!(!native.assumed());
    // A standing for another principal is a misconfigured context.
    assert!(matches!(
        resolve_probe(Ok(Some(standing(ParticipantId::from_u128(4)))), true, me),
        Err(ClientError::Configuration)
    ));
    // A legacy answer is V1, known; an unreachable owner is V1 assumed only
    // while no native journal proves the ledger native.
    let answered = resolve_probe(Ok(None), true, me).unwrap();
    assert_eq!(answered, Engine::V1 { assumed: false });
    assert!(!answered.assumed() && answered.standing().is_none());
    let assumed = resolve_probe(Err(ClientError::Transport), false, me).unwrap();
    assert_eq!(assumed, Engine::V1 { assumed: true });
    assert!(assumed.assumed());
    assert!(matches!(
        resolve_probe(Err(ClientError::Transport), true, me),
        Err(ClientError::Transport)
    ));
    assert!(matches!(
        resolve_probe(Err(ClientError::Unauthenticated), false, me),
        Err(ClientError::Unauthenticated)
    ));
    // Online: the probe decides; an explicit request only ever contradicts.
    assert_eq!(select_online(&native, false), Ok(WireProfile::Native));
    assert_eq!(select_online(&native, true), Ok(WireProfile::Native));
    assert_eq!(select_online(&answered, false), Ok(WireProfile::V1));
    assert_eq!(
        select_online(&answered, true),
        Err(EngineError::Contradicted)
    );
    assert_eq!(select_online(&assumed, false), Ok(WireProfile::V1));
    assert_eq!(select_online(&assumed, true), Err(EngineError::Unprobed));
}

#[test]
fn throwaway_identities_are_sequential_nonzero_and_skip_what_the_document_names() {
    let document = decode_application(
        WireProfile::Native,
        "monitor.rebind",
        br#"{"claim":"00000000000000000000000000000001","monitor":"00000000000000000000000000000002","predecessor":"00000000000000000000000000000004","successor":"00000000000000000000000000000003"}"#,
    )
    .unwrap();
    let mut ids = ThrowawayIds::excluding(&document.canonical_intent().unwrap()).unwrap();
    let mut minted = Vec::new();
    for _ in 0..3 {
        minted.push(u128::from_be_bytes(ids.next_id().unwrap()));
    }
    assert_eq!(minted, [5, 6, 7]);
    let mut fresh = ThrowawayIds::excluding(b"{}").unwrap();
    assert_eq!(u128::from_be_bytes(fresh.next_id().unwrap()), 1);
}
