use super::*;
use serde_json::{Value as Json, json};

const ORIGINAL_COMMANDS: &[u8] =
    include_bytes!("../../../focal-core/fixtures/durable-v1-inputs/commands.rows");
const ORIGINAL_TAGS: &[u8] =
    include_bytes!("../../../focal-core/fixtures/durable-v1-inputs/canonical-tags.rows");

fn original_commands() -> Vec<Command> {
    let (commands, rest) = postcard::take_from_bytes(ORIGINAL_COMMANDS).unwrap();
    assert!(rest.is_empty());
    commands
}

#[test]
fn every_command_keeps_original_body_fields_ordinals_and_identity_tags() {
    let commands = original_commands();
    assert_eq!(commands.len(), 58);
    let original_tags: Vec<u16> = postcard::from_bytes(ORIGINAL_TAGS).unwrap();
    assert_eq!(original_tags.len(), commands.len());
    let (Value(frozen), rest): (Value<Vec<Command>>, _) =
        postcard::take_from_bytes(ORIGINAL_COMMANDS).unwrap();
    assert!(rest.is_empty());
    assert_eq!(frozen, commands);
    assert_eq!(
        postcard::to_stdvec(&Ref(&frozen)).unwrap(),
        ORIGINAL_COMMANDS
    );
    for (index, command) in commands.iter().enumerate() {
        let expected = postcard::to_stdvec(command).unwrap();
        let actual = postcard::to_stdvec(&Ref(command)).unwrap();
        assert_eq!(actual, expected);
        let (ordinal, body): (u32, _) = postcard::take_from_bytes(&actual).unwrap();
        assert!(!body.is_empty());
        assert_eq!(u32::from(command_code(command)), ordinal + 1);
        assert_eq!(command_code(command), original_tags[index]);
        assert_eq!(command_code(command), command.code());
        assert_eq!(ordinal as usize, index % 29);
        assert_eq!(usize::from(command_code(command)), index % 29 + 1);
        let (Value(decoded), rest): (Value<Command>, _) =
            postcard::take_from_bytes(&expected).unwrap();
        assert!(rest.is_empty());
        assert_eq!(&decoded, command);
        for length in 0..expected.len() {
            assert!(
                postcard::take_from_bytes::<Value<Command>>(&expected[..length]).is_err(),
                "command {index} truncated at {length}",
            );
        }
    }
}

#[test]
fn new_or_unknown_command_ordinals_cannot_decode_as_v1() {
    for ordinal in [29u32, 30, 127, 128, u32::MAX] {
        let mut bytes = postcard::to_stdvec(&ordinal).unwrap();
        // Complete-looking payload bytes must not make a new tag recognizable.
        bytes.extend_from_slice(&[0; 128]);
        assert!(postcard::take_from_bytes::<Value<Command>>(&bytes).is_err());
    }
}

// Generate typed test perturbations through the original public representation.
// This is deliberately separate from the frozen codec's field declaration.
// Invalid alternatives are discarded by Command's original decoder; every
// original command field must have at least one meaningful accepted change.
// Only the first array element and first four object fields are explored, with
// at most eight retained candidates at each level. Large byte arrays therefore
// cannot produce one whole-array clone for every byte.
fn alternatives(value: &Json) -> Vec<Json> {
    const MAX_ALTERNATIVES: usize = 8;
    match value {
        Json::Null => vec![
            json!(0),
            json!(false),
            json!("changed"),
            json!({"receipt":vec![0u8;16],"epoch":0}),
        ],
        Json::Bool(value) => vec![Json::Bool(!value)],
        Json::Number(value) => {
            let value = value.as_u64().unwrap();
            vec![json!(value ^ 1), json!(0), json!(1), json!(2)]
        }
        Json::String(value) => vec![json!(format!("{value}\0changed"))],
        Json::Array(values) => {
            let mut changed = Vec::new();
            if let Some(value) = values.first() {
                for alternate in alternatives(value) {
                    let mut next = values.clone();
                    next[0] = alternate;
                    changed.push(Json::Array(next));
                }
            }
            if !values.is_empty() && changed.len() < MAX_ALTERNATIVES {
                changed.push(Json::Array(Vec::new()));
            }
            changed
        }
        Json::Object(fields) => {
            let mut changed = Vec::new();
            for (name, value) in fields.iter().take(4) {
                for alternate in alternatives(value) {
                    let mut next = fields.clone();
                    next.insert(name.clone(), alternate);
                    changed.push(Json::Object(next));
                    if changed.len() == MAX_ALTERNATIVES {
                        return changed;
                    }
                }
            }
            changed
        }
    }
}

#[test]
fn changing_each_original_command_field_changes_the_frozen_bytes() {
    for command in original_commands().into_iter().take(29) {
        let original_bytes = postcard::to_stdvec(&Ref(&command)).unwrap();
        let original_json = serde_json::to_value(&command).unwrap();
        let (variant, fields) = original_json.as_object().unwrap().iter().next().unwrap();
        for (field, value) in fields.as_object().unwrap() {
            let mut changed = false;
            for alternative in alternatives(value) {
                let mut proposed = original_json.clone();
                proposed[variant][field] = alternative;
                let Ok(other) = serde_json::from_value::<Command>(proposed) else {
                    continue;
                };
                if other == command {
                    continue;
                }
                let bytes = postcard::to_stdvec(&Ref(&other)).unwrap();
                assert_ne!(bytes, original_bytes, "{variant}.{field}");
                assert_eq!(bytes, postcard::to_stdvec(&other).unwrap());
                let (Value(decoded), rest): (Value<Command>, _) =
                    postcard::take_from_bytes(&bytes).unwrap();
                assert!(rest.is_empty());
                assert_eq!(decoded, other);
                changed = true;
                break;
            }
            assert!(changed, "no typed perturbation exercised {variant}.{field}");
        }
    }
}

#[test]
fn standalone_decoding_leaves_suffix_for_the_enclosing_exact_boundary() {
    // V1 is a nested codec. It consumes precisely this enum, so Core's exact
    // prepared/checkpoint decoder can reject trailing data before publication.
    let command = Command::NegotiateEpoch {
        epoch: RequestEpoch(300),
    };
    let bytes = [0, 0xac, 2, 0xee, 0xff];
    let (Value(decoded), rest): (Value<Command>, _) = postcard::take_from_bytes(&bytes).unwrap();
    assert_eq!(decoded, command);
    assert_eq!(rest, [0xee, 0xff]);
    assert_eq!(postcard::to_stdvec(&Ref(&command)).unwrap(), [0, 0xac, 2]);
}
