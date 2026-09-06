use crate::*;
use std::collections::{BTreeSet, VecDeque};

pub type GraphPage = ReadPage<GraphKey, GraphValue>;
pub type GraphScanContinuation = ScanContinuation<GraphKey>;

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
    pub fn expires_at(&self) -> u64 {
        self.lease.expires_at()
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
        self.namespace(query.root)?;
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
                if c.query != *query || !same_limits(c.limits, limits) {
                    return Err(MemoryError::QueryMismatch.into());
                }
                c
            }
            None => {
                let minimum = cursor_charge(1, query)?;
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
                let mut queue = VecDeque::new();
                queue.push_back(PendingNode::new(query.root, 0));
                GraphTraversalContinuation {
                    range: self.lease.range_id(),
                    lease: self.lease.id(),
                    prefix: self.sequence(),
                    query: query.clone(),
                    limits,
                    queue,
                    visited: BTreeSet::from([query.root]),
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
            let page_budget = ReadBudget {
                max_items: max_edges
                    .saturating_sub(probes)
                    .min(limits.max_edges.saturating_sub(cursor.edges))
                    .min(64),
                max_bytes: self.config.max_query_bytes,
                max_edge_visits: 1,
            };
            let mut neighbors = self.scan(&index, page_budget, current.edges.take(), now)?;
            current.edges = neighbors.continuation.take();
            probes = probes
                .checked_add(neighbors.len())
                .ok_or(GraphError::Overflow)?;
            cursor.edges = cursor
                .edges
                .checked_add(neighbors.len())
                .ok_or(GraphError::Overflow)?;
            let mut cumulative_stop = None;
            for entry in neighbors.items() {
                let GraphValue::Edge(edge) = &entry.value else {
                    return Err(GraphError::IndexMismatch);
                };
                if !query.relations.is_empty() && !query.relations.contains(&edge.relation) {
                    continue;
                }
                let target = match query.direction {
                    Direction::Forward => match &edge.target {
                        RelationTarget::Object(o) => *o,
                        _ => continue,
                    },
                    Direction::Reverse => edge.source,
                };
                if current.depth == limits.max_depth {
                    cursor.depth_pruned = true;
                    continue;
                }
                if cursor.visited.contains(&target) {
                    continue;
                }
                if cursor.visited.len() == limits.max_nodes {
                    cumulative_stop = Some(TraversalStop::NodeLimit);
                    break;
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
                    cumulative_stop = Some(TraversalStop::StateLimit);
                    break;
                }
                cursor.visited.insert(target);
                cursor.queue.push_back(PendingNode::new(
                    target,
                    current.depth.checked_add(1).ok_or(GraphError::Overflow)?,
                ));
            }
            if let Some(stop) = cumulative_stop {
                break stop;
            }
            if current.edges.is_none() {
                cursor.current = None
            }
            // Count even an empty adjacency probe, so isolated graphs cannot bypass work limits.
            if neighbors.is_empty() {
                probes = probes.checked_add(1).ok_or(GraphError::Overflow)?;
                cursor.edges = cursor.edges.checked_add(1).ok_or(GraphError::Overflow)?
            }
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
struct PendingNode {
    object: ObjectRef,
    depth: u32,
    emitted: bool,
    edges: Option<GraphScanContinuation>,
}
impl PendingNode {
    fn new(object: ObjectRef, depth: u32) -> Self {
        Self {
            object,
            depth,
            emitted: false,
            edges: None,
        }
    }
}
pub struct GraphTraversalContinuation {
    range: RangeId,
    lease: u64,
    prefix: SessionSeq,
    query: GraphTraversalQuery,
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
