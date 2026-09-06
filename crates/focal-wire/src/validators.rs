//! Recorded external handler contracts, grouped by their immutable requirement.
//! There is no daemon-owned implementation registry or execution capability.
use crate::*;
use focal_model::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidatorRequest {
    pub query: ListRequest,
    pub handler: Option<ValidatorId>,
    pub version: Option<ContentHash>,
    pub agentic: Option<bool>,
    pub evidence_schema: Option<ContentHash>,
}
impl ValidatorRequest {
    pub fn validate(&self, limits: &WireLimits) -> Result<(), AccessError> {
        self.query.filter.validate()?;
        if self.query.filter.kind != ObjectKind::Validation
            || self.handler.is_some_and(|id| id.is_zero())
            || [self.version, self.evidence_schema]
                .into_iter()
                .flatten()
                .any(|hash| hash == ContentHash::default())
            || (self.version.is_some() && self.handler.is_none())
        {
            return Err(AccessError::InvalidRequest);
        }
        if self.query.max_items == 0
            || self.query.max_items > limits.max_items
            || self.query.max_visits == 0
            || self.query.max_visits > limits.max_items
            || self.query.cursor.as_ref().is_some_and(|cursor| {
                cursor.bytes.is_empty() || cursor.bytes.len() > MAX_LIST_CURSOR_BYTES
            })
        {
            return Err(AccessError::Capacity);
        }
        Ok(())
    }
    pub fn matches(&self, requirement: &ValidationContent) -> bool {
        let filter = &self.query.filter;
        filter.kind == ObjectKind::Validation
            && filter.claim.is_none_or(|claim| requirement.claim == claim)
            && filter
                .evaluator
                .is_none_or(|evaluator| requirement.evaluator == evaluator)
            && filter
                .validation_kind
                .is_none_or(|kind| requirement.kind == kind)
            && filter.phase.is_none_or(|phase| requirement.phase == phase)
            && filter.mode.is_none_or(|mode| requirement.mode == mode)
            && self
                .evidence_schema
                .is_none_or(|schema| requirement.evidence_schemas.contains(&schema))
            && requirement
                .handlers
                .iter()
                .any(|handler| self.matches_handler(handler))
    }
    pub fn matches_handler(&self, handler: &HandlerRef) -> bool {
        self.handler.is_none_or(|id| handler.id == id)
            && self
                .version
                .is_none_or(|version| handler.version == version)
            && self
                .agentic
                .is_none_or(|agentic| handler.agentic == agentic)
    }
}

pub fn validator_scope(
    peer: &AuthenticatedPeer,
    ledger: LedgerId,
    request: &ValidatorRequest,
) -> Result<ContentHash, AccessError> {
    let base = list_scope(peer, ledger, &request.query.filter)?;
    let bytes = postcard::to_stdvec(&(
        base,
        request.handler,
        request.version,
        request.agentic,
        request.evidence_schema,
        request.query.max_items,
        request.query.max_visits,
    ))
    .map_err(|_| AccessError::Capacity)?;
    Ok(ContentHash(blake3::derive_key(
        "focal.validator.contract-scope.v1",
        &bytes,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn query() -> ValidatorRequest {
        ValidatorRequest {
            query: ListRequest {
                filter: ListFilter::new(ObjectKind::Validation),
                cursor: None,
                max_items: 8,
                max_visits: 16,
            },
            handler: None,
            version: None,
            agentic: None,
            evidence_schema: None,
        }
    }
    #[test]
    fn response_rejects_valid_requirements_from_other_predicate_bindings() {
        let ledger = LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        };
        let content = ValidationContent {
            ledger,
            schema: SCHEMA_MAJOR,
            claim: ClaimId::from_u128(3),
            kind: ValidationKind::Test,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            description: "recorded contract".into(),
            quality_bar: None,
            evaluator: ParticipantId::from_u128(4),
            handlers: vec![HandlerRef {
                id: ValidatorId::from_u128(5),
                version: ContentHash([6; 32]),
                agentic: false,
            }],
            evidence_schemas: [ContentHash([7; 32])].into_iter().collect(),
            contributed_by: [ParticipantId::from_u128(4)].into_iter().collect(),
            policy_revision: 1,
        };
        let mut selected = query();
        selected.query.filter.claim = Some(content.claim);
        selected.query.filter.evaluator = Some(content.evaluator);
        selected.query.filter.validation_kind = Some(content.kind);
        selected.query.filter.phase = Some(content.phase);
        selected.query.filter.mode = Some(content.mode);
        selected.handler = Some(content.handlers[0].id);
        selected.version = Some(content.handlers[0].version);
        selected.agentic = Some(false);
        selected.evidence_schema = Some(ContentHash([7; 32]));
        selected.validate(&WireLimits::default()).unwrap();
        let request = RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(9),
            operation: Operation::Validators(selected),
        };
        let reply = |content: ValidationContent| {
            let hash = content.content_hash().unwrap();
            request.reply(Response::Validators(ListPage {
                token: ReadToken {
                    ledger,
                    route_epoch: RouteEpoch(1),
                    sequence: SessionSeq(1),
                },
                objects: vec![ReadObject::Validation {
                    id: ValidationId::from_u128(10),
                    value: Validation::new(
                        content,
                        hash,
                        ValidationLifecycle {
                            created: SessionSeq(1),
                            latest_epoch: 0,
                        },
                    ),
                }],
                next: None,
                visited: 1,
            }))
        };
        validate_response(
            &request,
            &reply(content.clone()),
            None,
            &WireLimits::default(),
        )
        .unwrap();
        for predicate in 0..5 {
            let mut forged = content.clone();
            match predicate {
                0 => forged.claim = ClaimId::from_u128(11),
                1 => forged.evaluator = ParticipantId::from_u128(12),
                2 => forged.kind = ValidationKind::Inspection,
                3 => forged.phase = ValidationPhase::Increment,
                _ => forged.mode = ValidationMode::Observe,
            }
            assert!(
                matches!(
                    validate_response(&request, &reply(forged), None, &WireLimits::default()),
                    Err(WireError::InvalidFrame)
                ),
                "base predicate {predicate} must bind the returned requirement"
            );
        }
    }
    #[test]
    fn inspection_appends_wire_tags_and_binds_all_predicates_and_limits() {
        let ledger = LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        };
        let peer = AuthenticatedPeer::local(PeerGrant {
            principal: ParticipantId::from_u128(3),
            tenants: [ledger.tenant].into_iter().collect(),
            role: PeerRole::Actor,
        })
        .unwrap();
        let original = query();
        let scope = validator_scope(&peer, ledger, &original).unwrap();
        for change in 0..7 {
            let mut changed = original.clone();
            match change {
                0 => changed.handler = Some(ValidatorId::from_u128(4)),
                1 => {
                    changed.handler = Some(ValidatorId::from_u128(4));
                    changed.version = Some(ContentHash([5; 32]));
                }
                2 => changed.agentic = Some(false),
                3 => changed.evidence_schema = Some(ContentHash([6; 32])),
                4 => changed.query.max_items = 1,
                5 => changed.query.max_visits = 1,
                _ => changed.query.filter.claim = Some(ClaimId::from_u128(7)),
            }
            assert_ne!(validator_scope(&peer, ledger, &changed).unwrap(), scope);
        }
        let operation = Operation::Validators(original.clone());
        assert_eq!(operation.registered_tag(), 21);
        assert_eq!(postcard::to_stdvec(&operation).unwrap()[0], 20);
        assert!(!operation.is_mutation());
        let envelope = RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(8),
            operation,
        };
        verify_request(peer.clone(), envelope.clone(), &WireLimits::default()).unwrap();
        let page = ListPage {
            token: ReadToken {
                ledger,
                sequence: SessionSeq(0),
                route_epoch: RouteEpoch(1),
            },
            objects: vec![],
            next: None,
            visited: 0,
        };
        assert_eq!(
            postcard::to_stdvec(&Response::Validators(page.clone())).unwrap()[0],
            17
        );
        validate_response(
            &envelope,
            &envelope.reply(Response::Validators(page.clone())),
            Some(peer.principal()),
            &WireLimits::default(),
        )
        .unwrap();
        assert!(
            validate_response(
                &envelope,
                &envelope.reply(Response::Listed(page)),
                Some(peer.principal()),
                &WireLimits::default()
            )
            .is_err()
        );
        for change in 0..5 {
            let mut invalid = original.clone();
            match change {
                0 => invalid.version = Some(ContentHash([5; 32])),
                1 => invalid.handler = Some(ValidatorId::default()),
                2 => invalid.query.filter.kind = ObjectKind::Claim,
                3 => invalid.query.max_visits = 0,
                _ => invalid.query.cursor = Some(ListCursor { bytes: vec![] }),
            }
            assert!(invalid.validate(&WireLimits::default()).is_err());
        }
    }
}
