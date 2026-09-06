use crate::input::*;
use focal_model::*;
use focal_wire::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestEpochDocument {
    pub epoch: u64,
}
impl RequestEpochDocument {
    pub fn build(self) -> Result<ReconcileQuery, InputError> {
        if self.epoch == 0 {
            return Err(InputError::Invalid("request epoch must be positive"));
        }
        Ok(ReconcileQuery::Epoch {
            epoch: RequestEpoch(self.epoch),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestStatusDocument {
    pub epoch: u64,
    pub request_id: String,
}
impl RequestStatusDocument {
    pub fn build(self) -> Result<ReconcileQuery, InputError> {
        if self.epoch == 0 {
            return Err(InputError::Invalid("request epoch must be positive"));
        }
        Ok(ReconcileQuery::Receipt {
            epoch: RequestEpoch(self.epoch),
            request: RequestId(parse_id(&self.request_id)?),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimIdDocument {
    pub claim: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgressDocument {
    pub claim: String,
    pub receipt: ReceiptDocument,
    pub message: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelDocument {
    pub claim: String,
    pub reason: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcquireReceiptDocument {
    pub claim: String,
    #[serde(default)]
    pub id: Option<String>,
    pub epoch: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeginEvidenceDocument {
    pub claim: String,
    pub receipt: ReceiptDocument,
    #[serde(default)]
    pub id: Option<String>,
}
/// Read prefix from an earlier result, interpreted only in the adapter's
/// selected ledger. This requests a historical view, never route authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrefixDocument {
    pub sequence: u64,
    pub route_epoch: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationPositionDocument {
    pub target_hash: String,
    pub phase: String,
    pub epoch: u64,
    /// None names the run summary; Some(0) names its first immutable verdict.
    #[serde(default)]
    pub attempt: Option<u32>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetDocument {
    pub id: String,
    #[serde(default)]
    pub prefix: Option<PrefixDocument>,
    #[serde(default)]
    pub after: Option<ValidationPositionDocument>,
    #[serde(default = "page_items")]
    pub limit: u32,
}
pub(super) fn page_items() -> u32 {
    64
}
pub(super) fn page_visits() -> u32 {
    1024
}
impl GetDocument {
    pub fn build(
        self,
        kind: ObjectKind,
        context: &BuildContext,
    ) -> Result<ReadRequest, InputError> {
        context.validate()?;
        let id = parse_id(&self.id)?;
        if self.limit == 0 || self.limit > 1024 {
            return Err(InputError::Capacity);
        }
        let consistency = match self.prefix {
            Some(prefix) => {
                if prefix.route_epoch == 0 {
                    return Err(InputError::Invalid("zero read route epoch"));
                }
                ReadConsistency::Exact(ReadToken {
                    ledger: context.ledger,
                    sequence: SessionSeq(prefix.sequence),
                    route_epoch: RouteEpoch(prefix.route_epoch),
                })
            }
            None => ReadConsistency::Linearizable,
        };
        let query = match kind {
            ObjectKind::Validation => {
                let after = self
                    .after
                    .map(|position| {
                        if !matches!(consistency, ReadConsistency::Exact(_)) || position.epoch == 0
                        {
                            return Err(InputError::Invalid(
                                "validation continuation requires a prefix and nonzero epoch",
                            ));
                        }
                        Ok(ValidationResultPosition {
                            run: ValidationRunId {
                                validation: ValidationId(id),
                                target_hash: parse_hash(&position.target_hash)?,
                                phase: parse_validation_phase(&position.phase)?,
                                epoch: position.epoch,
                            },
                            attempt: position.attempt,
                        })
                    })
                    .transpose()?;
                ReadQuery::ValidationResults {
                    id: ValidationId(id),
                    after,
                }
            }
            ObjectKind::Claim | ObjectKind::Testament | ObjectKind::Artifact => {
                if self.after.is_some() {
                    return Err(InputError::Invalid(
                        "only validation results have an attempt cursor",
                    ));
                }
                ReadQuery::Objects(vec![ObjectRef {
                    ledger: context.ledger,
                    kind,
                    id: ObjectId(id),
                }])
            }
        };
        Ok(ReadRequest {
            consistency,
            query,
            max_items: if kind == ObjectKind::Validation {
                self.limit
            } else {
                1
            },
        })
    }
}

/// Optional predicates are conjunctive; absent filters mean one bounded page
/// in the selected ledger. Family-inapplicable fields are rejected on build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListDocument {
    #[serde(default)]
    pub claim: Option<String>,
    #[serde(default)]
    pub testament: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub producer: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub schema_hash: Option<String>,
    #[serde(default)]
    pub evaluator: Option<String>,
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub scopes: Vec<ScopeDocument>,
    /// Typed relation targets: claim:ID, participant:self, action:work, root:ID.
    #[serde(default)]
    pub relations: Vec<ClaimRelationDocument>,
    #[serde(default)]
    pub caused_by: Option<String>,
    #[serde(default)]
    pub inputs: Vec<ObjectReferenceDocument>,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub confidence: Option<String>,
    #[serde(default)]
    pub created_after: Option<u64>,
    #[serde(default)]
    pub created_through: Option<u64>,
    /// Exact hexadecimal encoding of the server's opaque authenticated cursor.
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default = "page_items")]
    pub limit: u32,
    #[serde(default = "page_visits")]
    pub max_visits: u32,
}
impl Default for ListDocument {
    fn default() -> Self {
        Self {
            claim: None,
            testament: None,
            source: None,
            target: None,
            status: None,
            action: None,
            producer: None,
            kind: None,
            schema_hash: None,
            evaluator: None,
            phase: None,
            mode: None,
            scopes: Vec::new(),
            relations: Vec::new(),
            caused_by: None,
            inputs: Vec::new(),
            outcome: None,
            confidence: None,
            created_after: None,
            created_through: None,
            cursor: None,
            limit: page_items(),
            max_visits: page_visits(),
        }
    }
}
impl ListDocument {
    pub fn build(
        self,
        kind: ObjectKind,
        context: &BuildContext,
    ) -> Result<ListRequest, InputError> {
        if self.has_predicates() {
            return Err(InputError::Invalid(
                "extended list filters require build_operation",
            ));
        }
        context.validate()?;
        if self.limit == 0 || self.limit > 1024 || self.max_visits == 0 || self.max_visits > 1024 {
            return Err(InputError::Capacity);
        }
        let mut filter = ListFilter::new(kind);
        filter.claim = self
            .claim
            .as_deref()
            .map(parse_id)
            .transpose()?
            .map(ClaimId);
        filter.testament = self
            .testament
            .as_deref()
            .map(parse_id)
            .transpose()?
            .map(TestamentId);
        filter.source = self
            .source
            .as_deref()
            .map(|v| resolve_participant(v, context))
            .transpose()?;
        filter.target = self
            .target
            .as_deref()
            .map(|v| resolve_participant(v, context))
            .transpose()?;
        filter.producer = self
            .producer
            .as_deref()
            .map(|v| resolve_participant(v, context))
            .transpose()?;
        filter.evaluator = self
            .evaluator
            .as_deref()
            .map(|v| resolve_participant(v, context))
            .transpose()?;
        filter.status = self.status.as_deref().map(parse_status).transpose()?;
        filter.action = self.action.as_deref().map(parse_action).transpose()?;
        filter.schema = self.schema_hash.as_deref().map(parse_hash).transpose()?;
        filter.phase = self
            .phase
            .as_deref()
            .map(parse_validation_phase)
            .transpose()?;
        filter.mode = self
            .mode
            .as_deref()
            .map(parse_validation_mode)
            .transpose()?;
        if let Some(value) = self.kind {
            match kind {
                ObjectKind::Artifact => filter.artifact_kind = Some(value),
                ObjectKind::Validation => {
                    filter.validation_kind = Some(parse_validation_kind(&value)?)
                }
                ObjectKind::Claim | ObjectKind::Testament => {
                    return Err(InputError::Invalid(
                        "kind filter belongs to artifacts or validations",
                    ));
                }
            }
        }
        filter
            .validate()
            .map_err(|_| InputError::Invalid("unsupported or invalid filter for object family"))?;
        Ok(ListRequest {
            filter,
            cursor: self.cursor.as_deref().map(decode_list_cursor).transpose()?,
            max_items: self.limit,
            max_visits: self.max_visits,
        })
    }
}
/// Decode bounded hexadecimal server cursor bytes without altering their
/// opaque scope/prefix authentication. This grants no authority by itself.
pub fn decode_list_cursor(value: &str) -> Result<ListCursor, InputError> {
    if value.is_empty()
        || value.len() > MAX_LIST_CURSOR_BYTES.saturating_mul(2)
        || !value.len().is_multiple_of(2)
    {
        return Err(InputError::Invalid("list cursor length"));
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(value.len() / 2)
        .map_err(|_| InputError::Capacity)?;
    for pair in value.as_bytes().chunks_exact(2) {
        let mut byte = 0u8;
        for digit in pair {
            let digit = match digit {
                b'0'..=b'9' => digit.checked_sub(b'0'),
                b'a'..=b'f' => digit.checked_sub(b'a').and_then(|v| v.checked_add(10)),
                b'A'..=b'F' => digit.checked_sub(b'A').and_then(|v| v.checked_add(10)),
                _ => None,
            }
            .ok_or(InputError::Invalid("list cursor hexadecimal digit"))?;
            byte = byte
                .checked_mul(16)
                .and_then(|v| v.checked_add(digit))
                .ok_or(InputError::Invalid("list cursor byte"))?;
        }
        bytes.push(byte);
    }
    Ok(ListCursor { bytes })
}

/// No filters or historical-prefix claims: summary observes one current ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryDocument {}
