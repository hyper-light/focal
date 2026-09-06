use crate::*;
use std::collections::{BTreeSet, VecDeque};

pub type GraphPage = ReadPage<GraphKey, GraphValue>;
pub type GraphScanContinuation = ScanContinuation<GraphKey>;

/// Fixed-size projection; the canonical descriptor hash also covers inline
/// artifact bytes retained by the corresponding session checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactEvidence {
    pub artifact: ArtifactRef,
    pub content: Option<ContentRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphScan {
    Objects(Option<ObjectKind>),
    ByClaim(ClaimId),
    Status(ClaimStatus),
    DeadlinesThrough(u64),
    Required(ClaimId),
    Forward(ObjectRef),
    Reverse(ObjectRef),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Forward,
    Reverse,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphTraversalQuery {
    pub root: ObjectRef,
    pub direction: Direction,
    pub relations: BTreeSet<GraphRelation>,
    /// Binds a serving-layer authority scope; this adapter does not authorize reads.
    pub authority_scope: ContentHash,
}
pub struct GraphSnapshot {
    ledger: LedgerId,
    lease: SnapshotLease<GraphKey, GraphValue>,
    budget: MemoryBudget,
    config: RangeConfig,
}

/// One fixed-size index projection. Object contents remain borrowed inside the
/// snapshot until a caller explicitly materializes a matching result.
pub struct GraphCandidate {
    pub key: GraphKey,
    pub object: Option<ObjectRef>,
    pub bytes: usize,
}
impl GraphSnapshot {
    pub(crate) fn new(
        ledger: LedgerId,
        lease: SnapshotLease<GraphKey, GraphValue>,
        budget: MemoryBudget,
        config: RangeConfig,
    ) -> Self {
        Self {
            ledger,
            lease,
            budget,
            config,
        }
    }
    pub(crate) fn lease(&self) -> &SnapshotLease<GraphKey, GraphValue> {
        &self.lease
    }
    pub fn sequence(&self) -> SessionSeq {
        SessionSeq(self.lease.prefix())
    }
    pub fn ledger(&self) -> LedgerId {
        self.ledger
    }
    pub fn expires_at(&self) -> u64 {
        self.lease.expires_at()
    }
    /// Account retained adapter metadata under the same graph owner.
    pub fn reserve_query(&self, bytes: usize) -> Result<Allocation, GraphError> {
        Ok(self
            .budget
            .reserve(BudgetKind::Query, BudgetLane::Ordinary, bytes)?
            .commit())
    }
    /// Membership is derived from the committed ByClaim index, never inferred
    /// from an artifact's immutable provenance inputs.
    pub fn belongs_to_claim(
        &self,
        reference: ObjectRef,
        claim: ClaimId,
        now: u64,
    ) -> Result<bool, GraphError> {
        self.namespace(reference)?;
        if reference.kind == ObjectKind::Claim {
            return Ok(reference.id.0 == claim.0);
        }
        let key = GraphKey::ByClaim(claim, reference.kind, reference.id);
        let end = GraphKey::Identity(ObjectKind::Claim, ContentHash::default());
        self.lease
            .project_next(&key, false, &end, now, |entry| entry.key == key)
            .map(|found| found.unwrap_or(false))
            .map_err(Into::into)
    }
    pub fn next_candidate(
        &self,
        query: &GraphScan,
        after: Option<&GraphKey>,
        now: u64,
    ) -> Result<Option<GraphCandidate>, GraphError> {
        let query = self.scan_query(query)?;
        let start = query.start.ok_or(GraphError::IndexMismatch)?;
        let end = query.end.ok_or(GraphError::IndexMismatch)?;
        if after.is_some_and(|key| key < &start || key >= &end) {
            return Err(MemoryError::QueryMismatch.into());
        }
        self.lease
            .project_next(
                after.unwrap_or(&start),
                after.is_some(),
                &end,
                now,
                |entry| {
                    let object = match (&entry.key, &entry.value) {
                        (GraphKey::Object(kind, id), GraphValue::Object(_)) => Some(ObjectRef {
                            ledger: self.ledger,
                            kind: *kind,
                            id: *id,
                        }),
                        (_, GraphValue::Reference(reference)) => Some(*reference),
                        (_, GraphValue::Edge(edge)) if edge.relation == GraphRelation::Evidence => {
                            match edge.target {
                                RelationTarget::Object(reference) => Some(reference),
                                _ => None,
                            }
                        }
                        (_, GraphValue::Edge(_)) => None,
                        _ => return Err(GraphError::IndexMismatch),
                    };
                    Ok(GraphCandidate {
                        key: entry.key.clone(),
                        object,
                        bytes: entry
                            .heap_bytes
                            .checked_add(size_of::<Entry<GraphKey, GraphValue>>())
                            .ok_or(GraphError::Overflow)?,
                    })
                },
            )?
            .transpose()
    }
    /// One bounded adjacency lookup. Projection borrows the fixed-prefix row;
    /// only its fixed-size key and edge are copied into traversal state.
    fn next_edge(
        &self,
        query: &GraphScan,
        after: Option<&GraphKey>,
        now: u64,
    ) -> Result<Option<(GraphKey, GraphEdge)>, GraphError> {
        let query = self.scan_query(query)?;
        let start = query.start.ok_or(GraphError::IndexMismatch)?;
        let end = query.end.ok_or(GraphError::IndexMismatch)?;
        if after.is_some_and(|key| key < &start || key >= &end) {
            return Err(MemoryError::QueryMismatch.into());
        }
        self.lease
            .project_next(
                after.unwrap_or(&start),
                after.is_some(),
                &end,
                now,
                |entry| {
                    let GraphValue::Edge(edge) = &entry.value else {
                        return Err(GraphError::IndexMismatch);
                    };
                    Ok((entry.key.clone(), edge.clone()))
                },
            )?
            .transpose()
    }
    /// Retain the original checkpoint for exact page retries. The independent
    /// clone is admitted before copying any frontier/visited metadata.
    pub fn clone_traversal(
        &self,
        value: &GraphTraversalContinuation,
    ) -> Result<GraphTraversalContinuation, GraphError> {
        if value.lease != self.lease.id()
            || value.prefix != self.sequence()
            || value.range != self.lease.range_id()
        {
            return Err(MemoryError::WrongLease.into());
        }
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Query,
                BudgetLane::Ordinary,
                value._allocation.bytes(),
            )?
            .commit();
        let mut queue = VecDeque::new();
        queue
            .try_reserve_exact(value.queue.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        queue.extend(value.queue.iter().cloned());
        let mut roots = Vec::new();
        roots
            .try_reserve_exact(value.roots.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        roots.extend_from_slice(&value.roots);
        Ok(GraphTraversalContinuation {
            range: value.range,
            lease: value.lease,
            prefix: value.prefix,
            query: value.query.clone(),
            roots,
            limits: value.limits,
            queue,
            visited: value.visited.clone(),
            current: value.current.clone(),
            edges: value.edges,
            depth_pruned: value.depth_pruned,
            _allocation: allocation,
        })
    }

    /// Borrow one exact object without cloning its payload or relations. The
    /// callback cannot retain a borrow after the pinned page is released.
    pub fn project_object<R>(
        &self,
        reference: ObjectRef,
        now: u64,
        project: impl FnOnce(&GraphObject, usize) -> R,
    ) -> Result<Option<R>, GraphError> {
        self.namespace(reference)?;
        let key = GraphKey::object(reference);
        let end = next_ref(reference).map_or(
            GraphKey::ByClaim(ClaimId::default(), ObjectKind::Claim, ObjectId::default()),
            GraphKey::object,
        );
        self.lease
            .project_next(&key, false, &end, now, |entry| {
                let GraphValue::Object(object) = &entry.value else {
                    return Err(GraphError::IndexMismatch);
                };
                if entry.key != key {
                    return Err(GraphError::IndexMismatch);
                }
                let bytes = entry
                    .heap_bytes
                    .checked_add(size_of::<Entry<GraphKey, GraphValue>>())
                    .ok_or(GraphError::Overflow)?;
                Ok(project(object, bytes))
            })?
            .transpose()
    }
    pub fn artifact_after(
        &self,
        after: Option<ArtifactId>,
        now: u64,
    ) -> Result<Option<ArtifactEvidence>, GraphError> {
        let query = self.scan_query(&GraphScan::Objects(Some(ObjectKind::Artifact)))?;
        let start = after.map_or_else(
            || GraphKey::Object(ObjectKind::Artifact, ObjectId::default()),
            |id| GraphKey::Object(ObjectKind::Artifact, ObjectId(id.0)),
        );
        let end = query.end.ok_or(GraphError::IndexMismatch)?;
        self.lease
            .project_next(&start, after.is_some(), &end, now, |entry| {
                let (GraphKey::Object(ObjectKind::Artifact, id), GraphValue::Object(object)) =
                    (&entry.key, &entry.value)
                else {
                    return Err(GraphError::IndexMismatch);
                };
                let GraphObject::Artifact(artifact) = object.as_ref() else {
                    return Err(GraphError::IndexMismatch);
                };
                if artifact.content().ledger != self.ledger
                    || artifact.lifecycle().created > self.sequence()
                {
                    return Err(GraphError::Prefix);
                }
                Ok(ArtifactEvidence {
                    artifact: ArtifactRef {
                        id: ArtifactId(id.0),
                        hash: artifact.content_hash(),
                    },
                    content: match &artifact.content().payload {
                        ArtifactPayload::Inline(_) => None,
                        ArtifactPayload::Content(reference) => Some(reference.clone()),
                    },
                })
            })?
            .transpose()
    }
    pub fn get(&self, reference: ObjectRef, now: u64) -> Result<GraphPage, GraphError> {
        self.namespace(reference)?;
        Ok(self.lease.get(&GraphKey::object(reference), now)?)
    }
    pub fn identity(
        &self,
        kind: ObjectKind,
        hash: ContentHash,
        now: u64,
    ) -> Result<GraphPage, GraphError> {
        Ok(self.lease.get(&GraphKey::Identity(kind, hash), now)?)
    }
    pub fn scan(
        &self,
        query: &GraphScan,
        budget: ReadBudget,
        continuation: Option<GraphScanContinuation>,
        now: u64,
    ) -> Result<GraphPage, GraphError> {
        let query = self.scan_query(query)?;
        Ok(self.lease.scan(&query, budget, continuation, now)?)
    }
    /// Stable typed keyset pagination for external cursor envelopes. The caller
    /// must retain this exact snapshot lease and its authority scope across pages.
    pub fn objects_after(
        &self,
        after: Option<(ObjectKind, ObjectId)>,
        budget: ReadBudget,
        now: u64,
    ) -> Result<GraphPage, GraphError> {
        let end = GraphKey::ByClaim(ClaimId::default(), ObjectKind::Claim, ObjectId::default());
        let start = match after {
            None => GraphKey::Object(ObjectKind::Claim, ObjectId::default()),
            Some((kind, id)) => next_ref(ObjectRef {
                ledger: self.ledger,
                kind,
                id,
            })
            .map_or_else(|| end.clone(), GraphKey::object),
        };
        Ok(self.lease.scan(
            &ScanQuery {
                start: Some(start),
                end: Some(end),
                heap_bytes: 0,
            },
            budget,
            None,
            now,
        )?)
    }
    fn namespace(&self, reference: ObjectRef) -> Result<(), GraphError> {
        if reference.ledger != self.ledger {
            Err(GraphError::Prefix)
        } else {
            Ok(())
        }
    }
    fn scan_query(&self, query: &GraphScan) -> Result<ScanQuery<GraphKey>, GraphError> {
        let zero = ObjectId::default();
        let claim = ClaimId::default();
        let relation = GraphRelation::Authored(RelationKind::Issuer);
        let (start, end) = match query {
            GraphScan::Objects(None) => (
                GraphKey::Object(ObjectKind::Claim, zero),
                GraphKey::ByClaim(claim, ObjectKind::Claim, zero),
            ),
            GraphScan::Objects(Some(kind)) => (
                GraphKey::Object(*kind, zero),
                kind.code()
                    .checked_add(1)
                    .and_then(ObjectKind::from_code)
                    .map_or(GraphKey::ByClaim(claim, ObjectKind::Claim, zero), |next| {
                        GraphKey::Object(next, zero)
                    }),
            ),
            GraphScan::ByClaim(id) => (
                GraphKey::ByClaim(*id, ObjectKind::Claim, zero),
                next_id(id.0).map_or(
                    GraphKey::Identity(ObjectKind::Claim, ContentHash::default()),
                    |n| GraphKey::ByClaim(ClaimId(n), ObjectKind::Claim, zero),
                ),
            ),
            GraphScan::Status(status) => (
                GraphKey::Lifecycle(*status, claim),
                status
                    .code()
                    .checked_add(1)
                    .and_then(ClaimStatus::from_code)
                    .map_or(
                        GraphKey::Deadline(0, claim, TimerId::default(), 0),
                        |next| GraphKey::Lifecycle(next, claim),
                    ),
            ),
            GraphScan::DeadlinesThrough(time) => (
                GraphKey::Deadline(0, claim, TimerId::default(), 0),
                time.checked_add(1).map_or(
                    GraphKey::Required(claim, ValidationPhase::Admission, ValidationId::default()),
                    |t| GraphKey::Deadline(t, claim, TimerId::default(), 0),
                ),
            ),
            GraphScan::Required(id) => (
                GraphKey::Required(*id, ValidationPhase::Admission, ValidationId::default()),
                next_id(id.0).map_or(GraphKey::End, |n| {
                    GraphKey::Required(
                        ClaimId(n),
                        ValidationPhase::Admission,
                        ValidationId::default(),
                    )
                }),
            ),
            GraphScan::Forward(source) => {
                self.namespace(*source)?;
                (
                    GraphKey::Forward(
                        *source,
                        RelationTarget::Participant(ParticipantId::default()),
                        relation,
                    ),
                    next_ref(*source).map_or(
                        GraphKey::Reverse(
                            RelationTarget::Participant(ParticipantId::default()),
                            min_ref(self.ledger),
                            relation,
                        ),
                        |next| {
                            GraphKey::Forward(
                                next,
                                RelationTarget::Participant(ParticipantId::default()),
                                relation,
                            )
                        },
                    ),
                )
            }
            GraphScan::Reverse(target) => {
                self.namespace(*target)?;
                (
                    GraphKey::Reverse(
                        RelationTarget::Object(*target),
                        min_ref(self.ledger),
                        relation,
                    ),
                    next_ref(*target).map_or(
                        GraphKey::Reverse(
                            RelationTarget::Action(ActionType::Work),
                            min_ref(self.ledger),
                            relation,
                        ),
                        |next| {
                            GraphKey::Reverse(
                                RelationTarget::Object(next),
                                min_ref(self.ledger),
                                relation,
                            )
                        },
                    ),
                )
            }
        };
        Ok(ScanQuery {
            start: Some(start),
            end: Some(end),
            heap_bytes: 0,
        })
    }
    pub fn traverse(
        &self,
        query: &GraphTraversalQuery,
        limits: TraversalLimits,
        budget: ReadBudget,
        continuation: Option<GraphTraversalContinuation>,
        now: u64,
    ) -> Result<GraphTraversalPage, GraphError> {
        self.traverse_roots(
            std::slice::from_ref(&query.root),
            query,
            limits,
            budget,
            continuation,
            now,
        )
    }
    /// Multiple roots enter one deterministic BFS frontier and share visited state.
    pub fn traverse_roots(
        &self,
        roots: &[ObjectRef],
        query: &GraphTraversalQuery,
        limits: TraversalLimits,
        budget: ReadBudget,
        continuation: Option<GraphTraversalContinuation>,
        now: u64,
    ) -> Result<GraphTraversalPage, GraphError> {
        if roots.is_empty()
            || roots.len() > 32
            || roots.first() != Some(&query.root)
            || roots.windows(2).any(|pair| matches!(pair, [a,b] if a>=b))
        {
            return Err(MemoryError::QueryMismatch.into());
        }
        for root in roots {
            self.namespace(*root)?;
        }
        let limits = TraversalLimits {
            max_depth: limits.max_depth.min(self.config.max_traversal_depth),
            max_nodes: limits.max_nodes.min(self.config.max_traversal_nodes),
            max_edges: limits.max_edges.min(self.config.max_traversal_edges),
            max_state_bytes: limits
                .max_state_bytes
                .min(self.config.max_continuation_bytes),
        };
        let max_items = budget.max_items.min(self.config.max_query_items);
        let max_bytes = budget.max_bytes.min(self.config.max_query_bytes);
        let max_edges = budget.max_edge_visits.min(self.config.max_traversal_edges);
        if max_items == 0
            || max_bytes == 0
            || max_edges == 0
            || limits.max_nodes == 0
            || limits.max_edges == 0
        {
            return Err(MemoryError::InvalidConfiguration("zero graph traversal budget").into());
        }
        let mut cursor = match continuation {
            Some(c) => {
                if c.lease != self.lease.id()
                    || c.prefix != self.sequence()
                    || c.range != self.lease.range_id()
                {
                    return Err(MemoryError::WrongLease.into());
                }
                if c.query != *query || c.roots != roots || !same_limits(c.limits, limits) {
                    return Err(MemoryError::QueryMismatch.into());
                }
                c
            }
            None => {
                let minimum = cursor_charge(roots.len(), query)?;
                if minimum > limits.max_state_bytes {
                    return Err(MemoryError::Capacity {
                        requested: minimum,
                        available: limits.max_state_bytes,
                    }
                    .into());
                }
                let allocation = self
                    .budget
                    .reserve(
                        BudgetKind::Query,
                        BudgetLane::Ordinary,
                        limits.max_state_bytes,
                    )?
                    .commit();
                if roots.len() > limits.max_nodes {
                    return Err(MemoryError::QueryMismatch.into());
                }
                let mut queue = VecDeque::new();
                queue
                    .try_reserve_exact(roots.len())
                    .map_err(|_| MemoryError::AllocationFailed)?;
                for root in roots {
                    queue.push_back(PendingNode::new(*root, 0));
                }
                let mut owned_roots = Vec::new();
                owned_roots
                    .try_reserve_exact(roots.len())
                    .map_err(|_| MemoryError::AllocationFailed)?;
                owned_roots.extend_from_slice(roots);
                GraphTraversalContinuation {
                    range: self.lease.range_id(),
                    lease: self.lease.id(),
                    prefix: self.sequence(),
                    query: query.clone(),
                    roots: owned_roots,
                    limits,
                    queue,
                    visited: roots.iter().copied().collect(),
                    current: None,
                    edges: 0,
                    depth_pruned: false,
                    _allocation: allocation,
                }
            }
        };
        let bytes = size_of::<GraphTraversalPage>()
            .checked_add(
                max_items
                    .checked_mul(size_of::<GraphPage>())
                    .ok_or(GraphError::Overflow)?,
            )
            .ok_or(GraphError::Overflow)?;
        let allocation = self
            .budget
            .reserve(BudgetKind::Query, BudgetLane::Ordinary, bytes)?
            .commit();
        let mut pages = Vec::new();
        pages
            .try_reserve_exact(max_items)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut output_bytes = 0_usize;
        let mut probes = 0;
        let stop = loop {
            if cursor.current.is_none() {
                cursor.current = cursor.queue.pop_front()
            }
            let Some(current) = cursor.current.as_mut() else {
                break if cursor.depth_pruned {
                    TraversalStop::DepthLimit
                } else {
                    TraversalStop::Complete
                };
            };
            if !current.emitted {
                if pages.len() == max_items {
                    break TraversalStop::PageLimit;
                }
                let page = self.get(current.object, now)?;
                if page.is_empty() {
                    return Err(MemoryError::MissingKey.into());
                }
                let bytes = page.items().iter().try_fold(0_usize, |sum, e| {
                    e.heap_bytes
                        .checked_add(size_of::<Entry<GraphKey, GraphValue>>())
                        .and_then(|bytes| sum.checked_add(bytes))
                        .ok_or(GraphError::Overflow)
                })?;
                let total = output_bytes
                    .checked_add(bytes)
                    .ok_or(GraphError::Overflow)?;
                if total > max_bytes {
                    if pages.is_empty() {
                        return Err(MemoryError::ItemTooLarge {
                            bytes: total,
                            limit: max_bytes,
                        }
                        .into());
                    }
                    break TraversalStop::PageLimit;
                }
                pages.push(page);
                output_bytes = total;
                current.emitted = true;
            }
            if probes >= max_edges {
                break TraversalStop::PageLimit;
            }
            if cursor.edges >= limits.max_edges {
                break TraversalStop::EdgeLimit;
            }
            let index = match query.direction {
                Direction::Forward => GraphScan::Forward(current.object),
                Direction::Reverse => GraphScan::Reverse(current.object),
            };
            let edge = self.next_edge(&index, current.after.as_ref(), now)?;
            probes = probes.checked_add(1).ok_or(GraphError::Overflow)?;
            cursor.edges = cursor.edges.checked_add(1).ok_or(GraphError::Overflow)?;
            let Some((key, edge)) = edge else {
                cursor.current = None;
                continue;
            };
            current.after = Some(key);
            if !query.relations.is_empty() && !query.relations.contains(&edge.relation) {
                continue;
            }
            let target = match query.direction {
                Direction::Forward => match edge.target {
                    RelationTarget::Object(object) => object,
                    _ => continue,
                },
                Direction::Reverse => edge.source,
            };
            if cursor.visited.contains(&target) {
                continue;
            }
            if current.depth == limits.max_depth {
                cursor.depth_pruned = true;
                continue;
            }
            if cursor.visited.len() == limits.max_nodes {
                break TraversalStop::NodeLimit;
            }
            if cursor_charge(
                cursor
                    .visited
                    .len()
                    .checked_add(1)
                    .ok_or(GraphError::Overflow)?,
                query,
            )? > limits.max_state_bytes
            {
                break TraversalStop::StateLimit;
            }
            let depth = current.depth.checked_add(1).ok_or(GraphError::Overflow)?;
            cursor
                .queue
                .try_reserve(1)
                .map_err(|_| MemoryError::AllocationFailed)?;
            cursor.visited.insert(target);
            cursor.queue.push_back(PendingNode::new(target, depth));
        };
        let total_edge_visits = cursor.edges;
        Ok(GraphTraversalPage {
            prefix: self.sequence(),
            objects: pages,
            continuation: (stop == TraversalStop::PageLimit).then_some(cursor),
            stop,
            edge_visits: probes,
            total_edge_visits,
            _allocation: allocation,
        })
    }
}
fn next_id(id: [u8; 16]) -> Option<[u8; 16]> {
    u128::from_be_bytes(id)
        .checked_add(1)
        .map(u128::to_be_bytes)
}
fn next_ref(mut r: ObjectRef) -> Option<ObjectRef> {
    if let Some(id) = next_id(r.id.0) {
        r.id = ObjectId(id);
        Some(r)
    } else {
        r.kind = ObjectKind::from_code(r.kind.code().checked_add(1)?)?;
        r.id = ObjectId::default();
        Some(r)
    }
}
fn min_ref(ledger: LedgerId) -> ObjectRef {
    ObjectRef {
        ledger,
        kind: ObjectKind::Claim,
        id: ObjectId::default(),
    }
}
fn same_limits(a: TraversalLimits, b: TraversalLimits) -> bool {
    a.max_depth == b.max_depth
        && a.max_nodes == b.max_nodes
        && a.max_edges == b.max_edges
        && a.max_state_bytes == b.max_state_bytes
}
fn cursor_charge(nodes: usize, query: &GraphTraversalQuery) -> Result<usize, GraphError> {
    let per_node = size_of::<ObjectRef>()
        .saturating_add(size_of::<usize>())
        .saturating_mul(16)
        .saturating_add(size_of::<PendingNode>().saturating_mul(3));
    nodes
        .checked_mul(per_node)
        .and_then(|b| b.checked_add(size_of::<GraphTraversalContinuation>()))
        .and_then(|b| {
            query
                .relations
                .len()
                .checked_mul(64)
                .and_then(|bytes| b.checked_add(bytes))
        })
        .ok_or(GraphError::Overflow)
}
#[derive(Clone)]
struct PendingNode {
    object: ObjectRef,
    depth: u32,
    emitted: bool,
    after: Option<GraphKey>,
}
impl PendingNode {
    fn new(object: ObjectRef, depth: u32) -> Self {
        Self {
            object,
            depth,
            emitted: false,
            after: None,
        }
    }
}
pub struct GraphTraversalContinuation {
    range: RangeId,
    lease: u64,
    prefix: SessionSeq,
    query: GraphTraversalQuery,
    roots: Vec<ObjectRef>,
    limits: TraversalLimits,
    queue: VecDeque<PendingNode>,
    visited: BTreeSet<ObjectRef>,
    current: Option<PendingNode>,
    edges: usize,
    depth_pruned: bool,
    _allocation: Allocation,
}
pub struct GraphTraversalPage {
    pub prefix: SessionSeq,
    /// Accounted point-read pages; each contains exactly one immutable object row.
    pub objects: Vec<GraphPage>,
    pub continuation: Option<GraphTraversalContinuation>,
    pub stop: TraversalStop,
    pub edge_visits: usize,
    pub total_edge_visits: usize,
    _allocation: Allocation,
}
impl GraphTraversalPage {
    pub fn len(&self) -> usize {
        self.objects.len()
    }
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }
}
