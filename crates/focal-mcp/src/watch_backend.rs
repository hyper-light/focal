use super::*;
use focal_client::watch::{WatchAction, WatchEngine, WatchOptions};
use serde::Deserialize;
use std::time::{Duration, Instant};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Open {
    name: String,
    #[serde(default)]
    claims: Vec<String>,
    #[serde(default)]
    family: Option<String>,
    #[serde(default = "yes")]
    seed: bool,
    #[serde(default = "items")]
    max_items: u32,
    #[serde(default = "bytes")]
    max_bytes: u32,
}
fn yes() -> bool {
    true
}
fn items() -> u32 {
    64
}
fn bytes() -> u32 {
    65536
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Name {
    name: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Inspect {
    #[serde(default)]
    name: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Ack {
    name: String,
    delivery_id: String,
}
fn decode<T: serde::de::DeserializeOwned>(call: &ToolCall) -> Result<T, BackendError> {
    serde_json::from_slice(&bounded_json(&call.arguments)?)
        .map_err(|_| InputError::Invalid("invalid watch arguments").into())
}
impl<T: ClientTransport> Backend<T> {
    pub(super) fn watch<'a>(&'a self, call: &'a ToolCall, cancel: &'a mut Cancel) -> Branch<'a> {
        Box::pin(self.watch_inner(call, cancel))
    }
    async fn watch_inner(
        &self,
        call: &ToolCall,
        cancel: &mut Cancel,
    ) -> Result<(&'static str, OperationOutput), BackendError> {
        let store = self.watches.as_ref().ok_or(BackendError::Configuration)?;
        let mut journal = match call.tool.as_str() {
            "watch.open" => {
                let input: Open = decode(call)?;
                let mut claims = input
                    .claims
                    .iter()
                    .map(|s| parse_id(s).map(ClaimId))
                    .collect::<Result<Vec<_>, _>>()?;
                claims.sort_unstable();
                claims.dedup();
                let family = match input.family.as_deref() {
                    None => None,
                    Some("claim") => Some(ObjectKind::Claim),
                    Some("testament") => Some(ObjectKind::Testament),
                    Some("artifact") => Some(ObjectKind::Artifact),
                    Some("validation") => Some(ObjectKind::Validation),
                    _ => return Err(InputError::Invalid("watch family").into()),
                };
                store.create(
                    &input.name,
                    WatchOptions {
                        engine: if self.has_native() {
                            WatchEngine::Native
                        } else {
                            WatchEngine::Legacy
                        },
                        claims,
                        family,
                        seed: input.seed,
                        max_items: input.max_items,
                        max_bytes: input.max_bytes,
                    },
                )?
            }
            "watch.next" => store.resume(&decode::<Name>(call)?.name)?,
            "watch.acknowledge" => {
                let input: Ack = decode(call)?;
                let mut journal = store.resume(&input.name)?;
                journal.acknowledge(focal_client::input::parse_hash(&input.delivery_id)?)?;
                return Ok((
                    "Consumed",
                    OperationOutput::Watch {
                        status: journal.status(),
                        delivery: None,
                    },
                ));
            }
            "watch.inspect" => {
                let input: Inspect = decode(call)?;
                let Some(name) = input.name else {
                    return Ok((
                        "Watches",
                        OperationOutput::Watches {
                            names: store.names()?,
                        },
                    ));
                };
                let journal = store.resume(&name)?;
                return Ok((
                    "Watch",
                    OperationOutput::Watch {
                        status: journal.status(),
                        delivery: journal.delivery().cloned().map(Box::new),
                    },
                ));
            }
            _ => return Err(BackendError::Configuration),
        };
        let started = Instant::now();
        for _ in 0..64 {
            if cancelled(cancel) {
                return Err(BackendError::Cancelled);
            }
            let action = match journal.next_action(&mut random_id)? {
                WatchAction::Delivery => {
                    return Ok((
                        "Delivery",
                        OperationOutput::Watch {
                            status: journal.status(),
                            delivery: journal.delivery().cloned().map(Box::new),
                        },
                    ));
                }
                WatchAction::Request(action) => action,
            };
            let remaining = Duration::from_secs(30).saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(ClientError::OutcomeUnknown {
                    request: Box::new(action.request),
                }
                .into());
            }
            let request = within(
                remaining,
                async {
                    self.client
                        .request(action.request.clone())
                        .await
                        .map_err(BackendError::Client)
                },
                || {
                    BackendError::Client(ClientError::OutcomeUnknown {
                        request: Box::new(action.request.clone()),
                    })
                },
            );
            let reply = contained(cancellable(cancel, request)).await?;
            journal.accept(action, reply)?;
        }
        Err(focal_client::watch::WatchError::Capacity.into())
    }
}
