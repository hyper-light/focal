use focal_graph::{GraphError, GraphKey, GraphObject, GraphSnapshot, GraphValue};
use focal_ledger::Session;
use focal_memory::{MemoryError, ReadBudget};
use focal_model::*;
use focal_wire::*;
use std::{collections::BTreeMap, time::Instant};

/// Fixed-size serving metadata. The graph store separately accounts every pinned
/// page and expires its owning root even if a client keeps an old token forever.
pub(crate) struct ReadViews {
    views: BTreeMap<(ParticipantId, SessionSeq), GraphSnapshot>,
    started: Instant,
    route_epoch: RouteEpoch,
    list_key: Option<zeroize::Zeroizing<[u8; 32]>>,
    traversals: BTreeMap<u64, traversal::SavedTraversal>,
    traversal_nonce: u64,
}
impl ReadViews {
    #[cfg(test)]
    pub(crate) fn traversal_count(&self) -> usize {
        self.traversals.len()
    }
    #[cfg(test)]
    pub(crate) fn expire_test_views(&mut self) {
        self.started = Instant::now() - std::time::Duration::from_secs(31);
    }
    pub fn new() -> Self {
        Self::with_route_epoch(RouteEpoch(1))
    }
    pub fn with_route_epoch(route_epoch: RouteEpoch) -> Self {
        Self {
            views: BTreeMap::new(),
            started: Instant::now(),
            route_epoch,
            list_key: None,
            traversals: BTreeMap::new(),
            traversal_nonce: 0,
        }
    }
    /// Serve at a newer route: every pinned view and saved traversal carried
    /// the old epoch in its token and is dropped; the read clock and the
    /// list key continue, since a clock that ran backwards would stop the
    /// session's read expiry.
    pub(crate) fn set_route_epoch(&mut self, route_epoch: RouteEpoch) {
        if route_epoch == self.route_epoch {
            return;
        }
        self.route_epoch = route_epoch;
        self.views.clear();
        self.traversals.clear();
    }
    /// The per-process key that authenticates list continuations. Minted on
    /// first use; a restart mints another, so cursors never outlive the node
    /// incarnation that issued them.
    pub(crate) fn list_key(&mut self) -> Result<&[u8; 32], AccessError> {
        if self.list_key.is_none() {
            let mut key = zeroize::Zeroizing::new([0; 32]);
            getrandom::fill(key.as_mut()).map_err(|_| AccessError::Unavailable)?;
            self.list_key = Some(key);
        }
        self.list_key.as_deref().ok_or(AccessError::Unavailable)
    }
    pub fn advance(&mut self, session: &mut Session) -> Result<u64, AccessError> {
        let now = u64::try_from(self.started.elapsed().as_millis())
            .map_err(|_| AccessError::Unavailable)?;
        session
            .advance_read_clock(now)
            .map_err(super::host::access)?;
        self.views.retain(|_, view| view.expires_at() > now);
        self.traversals.retain(|_, saved| saved.expires_at() > now);
        Ok(now)
    }
    pub fn read(
        &mut self,
        session: &mut Session,
        principal: ParticipantId,
        read: &ReadRequest,
        request_id: RequestId,
        limits: &WireLimits,
    ) -> Result<ReadPage, AccessError> {
        let now = self.advance(session)?;
        if matches!(read.consistency, ReadConsistency::Linearizable) {
            let mut context = b"focal.client-read\0".to_vec();
            context.extend_from_slice(&request_id.0);
            session
                .read_index(context.clone())
                .map_err(super::host::access)?;
            let mut complete = false;
            for _ in 0..4 {
                let events = session.poll().map_err(super::host::access)?;
                if events.read_barriers.iter().any(|(c, _)| c == &context) {
                    complete = true;
                    break;
                }
            }
            if !complete {
                return Err(AccessError::Unavailable);
            }
        }
        let sequence = match &read.consistency {
            ReadConsistency::Exact(token) => token.sequence,
            ReadConsistency::AtLeast(token) if token.sequence > session.graph_sequence() => {
                return Err(AccessError::Behind {
                    published: session.graph_sequence(),
                });
            }
            _ => session.graph_sequence(),
        };
        let key = (principal, sequence);
        if !self.views.contains_key(&key) {
            if matches!(read.consistency, ReadConsistency::Exact(_)) {
                return Err(AccessError::SnapshotExpired);
            }
            if self.views.len() == 64 {
                return Err(AccessError::Capacity);
            }
            let view = session
                .graph_snapshot(sequence, now, 30_000)
                .map_err(super::host::access)?;
            self.views.insert(key, view);
        }
        let view = self.views.get(&key).ok_or(AccessError::SnapshotExpired)?;
        let mut objects = Vec::new();
        let mut next = None;
        let mut bytes = 256usize;
        let mut push = |key: &GraphKey, value: &GraphValue| -> Result<(), AccessError> {
            let GraphKey::Object(kind, id) = key else {
                return Err(AccessError::Unavailable);
            };
            let GraphValue::Object(object) = value else {
                return Err(AccessError::Unavailable);
            };
            let object = convert(*kind, *id, object)?;
            let size = encode_payload(&object, limits.max_frame_bytes)
                .map_err(|_| AccessError::Capacity)?
                .len();
            bytes = bytes.checked_add(size).ok_or(AccessError::Capacity)?;
            if bytes > limits.max_frame_bytes as usize {
                return Err(AccessError::Capacity);
            }
            objects.push(object);
            Ok(())
        };
        match &read.query {
            ReadQuery::Objects(references) => {
                if references.len() > read.max_items as usize {
                    return Err(AccessError::Capacity);
                }
                for reference in references {
                    let page = view.get(*reference, now).map_err(graph_error)?;
                    for row in page.items() {
                        push(&row.key, &row.value)?;
                    }
                }
            }
            ReadQuery::Scan { after } => {
                let page = view
                    .objects_after(
                        after.map(|key| (key.kind, key.id)),
                        ReadBudget {
                            max_items: read.max_items as usize,
                            max_bytes: (limits.max_frame_bytes as usize)
                                .checked_sub(256)
                                .ok_or(AccessError::Capacity)?,
                            max_edge_visits: read.max_items as usize,
                        },
                        now,
                    )
                    .map_err(graph_error)?;
                for row in page.items() {
                    push(&row.key, &row.value)?;
                }
                if page.continuation.is_some()
                    && let Some(row) = page.items().last()
                    && let GraphKey::Object(kind, id) = row.key
                {
                    next = Some(ObjectKey { kind, id });
                }
            }
            ReadQuery::SeedScan {
                after,
                claims,
                max_bytes,
            } => {
                if *max_bytes < 1024 || *max_bytes > 65536 {
                    return Err(AccessError::InvalidRequest);
                }
                let mut seed_limits = limits.clone();
                seed_limits.max_frame_bytes = (*max_bytes).min(limits.max_frame_bytes);
                (objects, next) =
                    seed::read(view, claims, *after, read.max_items, now, &seed_limits)?;
            }
            ReadQuery::Traverse { roots, depth } => {
                if roots.is_empty() || roots.len() > MAX_TRAVERSAL_ROOTS {
                    return Err(AccessError::InvalidRequest);
                }
                let mut roots = roots.clone();
                roots.sort();
                roots.dedup();
                let query = focal_graph::GraphTraversalQuery {
                    root: *roots.first().ok_or(AccessError::InvalidRequest)?,
                    direction: focal_graph::Direction::Forward,
                    relations: Default::default(),
                    authority_scope: ContentHash::default(),
                };
                let result = view
                    .traverse_roots(
                        &roots,
                        &query,
                        focal_memory::TraversalLimits {
                            max_depth: u32::from(*depth),
                            max_nodes: MAX_TRAVERSAL_NODES as usize,
                            max_edges: MAX_TRAVERSAL_EDGES as usize,
                            max_state_bytes: 1024 * 1024,
                        },
                        ReadBudget {
                            max_items: read.max_items as usize,
                            max_bytes: (limits.max_frame_bytes as usize).saturating_sub(1024),
                            max_edge_visits: limits.max_items as usize,
                        },
                        None,
                        now,
                    )
                    .map_err(graph_error)?;
                // The legacy response cannot communicate truncation/cursors.
                if result.stop != focal_memory::TraversalStop::Complete {
                    return Err(AccessError::Capacity);
                }
                for page in &result.objects {
                    for row in page.items() {
                        push(&row.key, &row.value)?;
                    }
                }
            }
            ReadQuery::ValidationResults { id, after } => {
                if after.is_some() && !matches!(read.consistency, ReadConsistency::Exact(_)) {
                    return Err(AccessError::InvalidRequest);
                }
                if let Some(result) =
                    validation_results::read(view, *id, *after, read.max_items, now, limits)?
                {
                    objects.push(result);
                }
            }
        }
        Ok(ReadPage {
            token: ReadToken {
                ledger: session.ledger(),
                sequence: view.sequence(),
                route_epoch: self.route_epoch,
            },
            objects,
            next,
        })
    }
}

#[path = "lists.rs"]
mod lists;
#[path = "seed_reads.rs"]
mod seed;
#[path = "traversal_reads.rs"]
mod traversal;
#[path = "validation_reads.rs"]
mod validation_results;
pub(crate) use lists::ListReadContext;
fn convert(kind: ObjectKind, id: ObjectId, value: &GraphObject) -> Result<ReadObject, AccessError> {
    Ok(match (kind, value) {
        (ObjectKind::Claim, GraphObject::Claim(value)) => ReadObject::Claim {
            id: ClaimId(id.0),
            value: value.clone(),
        },
        (ObjectKind::Testament, GraphObject::Testament(value)) => ReadObject::Testament {
            id: TestamentId(id.0),
            value: value.clone(),
        },
        (ObjectKind::Validation, GraphObject::Validation(value)) => ReadObject::Validation {
            id: ValidationId(id.0),
            value: value.clone(),
        },
        (ObjectKind::Artifact, GraphObject::Artifact(value)) => ReadObject::Artifact {
            id: ArtifactId(id.0),
            value: value.clone(),
        },
        _ => return Err(AccessError::Unavailable),
    })
}
fn graph_error(error: GraphError) -> AccessError {
    match error {
        GraphError::Memory(
            MemoryError::Capacity { .. }
            | MemoryError::DiskCapacity { .. }
            | MemoryError::AllocationFailed
            | MemoryError::ItemTooLarge { .. },
        ) => AccessError::Capacity,
        _ => AccessError::SnapshotExpired,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::Settings, embedded::EmbeddedNode};
    use focal_ledger::Submission;
    fn submit(node: &mut EmbeddedNode, label: &str, command: Command) {
        let request =
            crate::demo::request(&node.identity, label, node.identity.issuer, command, vec![]);
        assert!(matches!(
            node.session.submit_local(&request).unwrap(),
            Submission::Committed(_)
        ));
    }
    fn status(page: &ReadPage) -> ClaimStatus {
        match &page.objects[0] {
            ReadObject::Claim { value, .. } => value.lifecycle().status,
            _ => panic!("expected claim"),
        }
    }
    #[test]
    fn pinned_reads_survive_publication_and_expire_without_changing_prefix() {
        let root = tempfile::tempdir().unwrap();
        let mut settings = Settings::default();
        settings.node.data_dir = Some(root.path().to_owned());
        let mut node = EmbeddedNode::open(&settings).unwrap();
        submit(
            &mut node,
            "read-epoch",
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
        );
        let id = ClaimId::from_u128(99);
        let claim = crate::demo::claim(&node.identity, id).unwrap();
        submit(&mut node, "read-generate", Command::GenerateClaim { claim });
        let query = ReadQuery::Objects(vec![ObjectRef::claim(node.identity.ledger, id)]);
        let mut views = ReadViews::new();
        let limits = WireLimits::default();
        let principal = node.identity.issuer;
        let first = views
            .read(
                &mut node.session,
                principal,
                &ReadRequest {
                    consistency: ReadConsistency::Linearizable,
                    query: query.clone(),
                    max_items: 1,
                },
                RequestId::from_u128(1),
                &limits,
            )
            .unwrap();
        assert_eq!(status(&first), ClaimStatus::Generated);
        submit(&mut node, "read-post", Command::PostClaim { claim: id });
        let exact = ReadRequest {
            consistency: ReadConsistency::Exact(first.token),
            query: query.clone(),
            max_items: 1,
        };
        assert_eq!(
            views
                .read(
                    &mut node.session,
                    principal,
                    &exact,
                    RequestId::from_u128(2),
                    &limits
                )
                .unwrap(),
            first
        );
        let current = views
            .read(
                &mut node.session,
                principal,
                &ReadRequest {
                    consistency: ReadConsistency::Linearizable,
                    query,
                    max_items: 1,
                },
                RequestId::from_u128(3),
                &limits,
            )
            .unwrap();
        assert_eq!(status(&current), ClaimStatus::Posted);
        assert!(current.token.sequence > first.token.sequence);
        assert_eq!(
            views.read(
                &mut node.session,
                node.identity.worker,
                &exact,
                RequestId::from_u128(4),
                &limits
            ),
            Err(AccessError::SnapshotExpired)
        );
        views.started -= std::time::Duration::from_secs(31);
        assert_eq!(
            views.read(
                &mut node.session,
                principal,
                &exact,
                RequestId::from_u128(5),
                &limits
            ),
            Err(AccessError::SnapshotExpired)
        );
    }
    #[test]
    fn object_scan_pages_are_typed_and_reuse_the_same_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let mut settings = Settings::default();
        settings.node.data_dir = Some(root.path().to_owned());
        let mut node = EmbeddedNode::open(&settings).unwrap();
        let report = crate::demo::run(&mut node).unwrap();
        let mut views = ReadViews::new();
        let limits = WireLimits::default();
        let mut after = None;
        let mut token = None;
        let mut count = 0;
        loop {
            let page = views
                .read(
                    &mut node.session,
                    node.identity.issuer,
                    &ReadRequest {
                        consistency: token
                            .map_or(ReadConsistency::Linearizable, ReadConsistency::Exact),
                        query: ReadQuery::Scan { after },
                        max_items: 1,
                    },
                    RequestId::from_u128(count + 1),
                    &limits,
                )
                .unwrap();
            assert_eq!(page.token.sequence, report.sequence);
            assert_eq!(page.objects.len(), 1);
            count += 1;
            token = Some(page.token);
            after = page.next;
            if after.is_none() {
                break;
            }
            assert!(count < 10);
        }
        assert_eq!(count, 5); // claim, testament, two validations, artifact
    }
}
