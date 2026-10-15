//! The machine-readable schema of the configuration (`config/schema/
//! deployment-v1.json`) and the unknown-key check that names a key by its
//! full path, which serde's field check alone cannot.
use super::ConfigError;

pub const SCHEMA: &str = include_str!("../../../../config/schema/deployment-v1.json");
const MAX_DEPTH: usize = 16;

fn schema() -> Result<serde_json::Value, ConfigError> {
    serde_json::from_str(SCHEMA).map_err(|_| ConfigError::Invalid {
        field: "schema",
        reason: "the embedded configuration schema is not valid JSON",
    })
}
fn parse(yaml: &str) -> Result<serde_json::Value, ConfigError> {
    let options = serde_saphyr::options! {
        budget: serde_saphyr::budget! {max_depth:16,max_events:8192,max_nodes:4096,max_total_scalar_bytes:64*1024,max_aliases:0,max_anchors:0,max_documents:1},
    };
    Ok(serde_saphyr::from_str_with_options(yaml, options)?)
}
/// Every key of `yaml` must be a property of its object in the schema;
/// the first that is not is named by its full dotted path.
pub fn check_unknown_keys(yaml: &str) -> Result<(), ConfigError> {
    if yaml.len() > 64 * 1024 {
        return Err(ConfigError::Invalid {
            field: "configuration",
            reason: "exceeds 64 KiB",
        });
    }
    let value = parse(yaml)?;
    let schema = schema()?;
    walk(&value, &schema)
}
/// An object of the configuration whose keys are being checked: its keys
/// still to visit, the schema's properties for them, whether the schema
/// closes it, and how long the dotted path was where it opened.
struct Open<'a> {
    keys: serde_json::map::Iter<'a>,
    properties: &'a serde_json::Value,
    closed: bool,
    prefix: usize,
}
impl<'a> Open<'a> {
    /// The object `value` checked against `schema`, when it is an object and
    /// the schema names its properties; else nothing to check in it.
    fn of(
        value: &'a serde_json::Value,
        schema: &'a serde_json::Value,
        prefix: usize,
    ) -> Option<Self> {
        let (Some(object), Some(properties)) = (value.as_object(), schema.get("properties")) else {
            return None;
        };
        Some(Self {
            keys: object.iter(),
            properties,
            closed: schema
                .get("additionalProperties")
                .is_some_and(|extra| extra == &serde_json::Value::Bool(false)),
            prefix,
        })
    }
}
/// Walks `value` against `schema` over an explicit stack of the objects open,
/// at most [`MAX_DEPTH`] deep, past which the configuration is refused; the
/// first key a closed object's schema does not name is refused with its path.
fn walk(value: &serde_json::Value, schema: &serde_json::Value) -> Result<(), ConfigError> {
    let mut path = String::new();
    let mut open: Vec<Open<'_>> = Vec::new();
    open.extend(Open::of(value, schema, 0));
    while let Some(frame) = open.last_mut() {
        let Some((key, inner)) = frame.keys.next() else {
            open.pop();
            continue;
        };
        let (properties, closed, prefix) = (frame.properties, frame.closed, frame.prefix);
        path.truncate(prefix);
        if !path.is_empty() {
            path.push('.');
        }
        path.push_str(key);
        match properties.get(key) {
            Some(property) => {
                // `inner` sits one level below the object open last.
                if open.len() > MAX_DEPTH {
                    return Err(ConfigError::Invalid {
                        field: "configuration",
                        reason: "nested too deeply",
                    });
                }
                open.extend(Open::of(inner, property, path.len()));
            }
            None if closed => {
                return Err(ConfigError::UnknownKey { path });
            }
            None => {}
        }
    }
    Ok(())
}
/// The property names the schema declares at `path` (empty for the root).
pub fn properties(path: &[&str]) -> Result<Vec<String>, ConfigError> {
    let schema = schema()?;
    let mut node = &schema;
    for segment in path {
        node = node
            .get("properties")
            .and_then(|properties| properties.get(segment))
            .ok_or(ConfigError::UnknownKey {
                path: path.join("."),
            })?;
    }
    Ok(node
        .get("properties")
        .and_then(|properties| properties.as_object())
        .map(|properties| properties.keys().cloned().collect())
        .unwrap_or_default())
}
