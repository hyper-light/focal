/// Coordinates of an exact local checkpoint. The descriptor is serializable;
/// the owning snapshot below deliberately has no public constructor/decoder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidencePrefix {
    pub cluster: [u8; 16],
    pub ledger: LedgerId,
    pub group: LogGroupId,
    pub genesis: ContentHash,
    pub node: u64,
    pub sequence: SessionSeq,
    pub index: RaftIndex,
    pub term: RaftTerm,
    pub route: RouteEpoch,
    pub placement_epoch: u64,
    pub membership_epoch: u64,
    pub operation: OperationId,
    pub placement_digest: ContentHash,
    pub checkpoint: ContentHash,
    pub checkpoint_bytes: u64,
    pub artifacts: u64,
}

/// Exact checkpoint bytes and a fixed-prefix artifact projection produced only
/// by successful session checkpoint persistence. Lease expiration fails closed;
/// dropping this export never releases a protected durable consumer's cursor.
pub struct DurableEvidenceSnapshot {
    prefix: EvidencePrefix,
    placement: CommittedPlacement,
    graph: GraphSnapshot,
    /// The native engine's artifact projection, when the session hosts it
    /// (24 §20): every content root the committed rows name at the
    /// checkpoint, in artifact order, taken at the same applied index.
    native: Option<NativeEvidenceProjection>,
    checkpoint: Vec<u8>,
    clock: u64,
    captured_at: std::time::Instant,
    _allocation: Allocation,
}
/// The content roots a native session's committed rows name, projected as
/// the artifact evidence every custody consumer walks: an artifact's payload
/// object under its artifact id, a retired family's archive bundle under
/// the retired claim's id (its own 16-byte identity; an equal artifact id
/// fails closed). Sorted, charged, fixed at capture.
pub struct NativeEvidenceProjection {
    objects: Vec<ArtifactEvidence>,
    _allocation: Allocation,
}
/// The rows one projection page visits.
const NATIVE_PROJECTION_PAGE: usize = 4096;
const NATIVE_PROJECTION_ENTRY_BYTES: usize = 160;
impl DurableEvidenceSnapshot {
    pub fn prefix(&self) -> &EvidencePrefix {
        &self.prefix
    }
    pub fn placement(&self) -> &CommittedPlacement {
        &self.placement
    }
    pub fn checkpoint(&self) -> &[u8] {
        &self.checkpoint
    }
    pub fn clock(&self) -> u64 {
        self.clock
    }
    pub fn expires_at(&self) -> u64 {
        self.graph.expires_at()
    }
    /// The TTL starts at capture, including checkpoint fsync, queue residence
    /// and a caller retaining an unpolled export. This process-local clock is
    /// neither serialized nor part of deterministic domain state.
    pub fn elapsed_clock(&self) -> Result<u64, LedgerError> {
        let elapsed = u64::try_from(self.captured_at.elapsed().as_millis())
            .map_err(|_| LedgerError::Capacity)?;
        let now = self
            .clock
            .checked_add(elapsed)
            .ok_or(LedgerError::Capacity)?;
        if now >= self.expires_at() {
            return Err(GraphError::Memory(MemoryError::LeaseExpired).into());
        }
        Ok(now)
    }
    pub fn artifact_after(
        &self,
        after: Option<ArtifactId>,
        now: u64,
    ) -> Result<Option<focal_graph::ArtifactEvidence>, LedgerError> {
        if let Some(native) = &self.native {
            if now >= self.expires_at() {
                return Err(GraphError::Memory(MemoryError::LeaseExpired).into());
            }
            let start = after.map_or(0, |after| {
                native
                    .objects
                    .partition_point(|entry| entry.artifact.id <= after)
            });
            return Ok(native.objects.get(start).cloned());
        }
        Ok(self.graph.artifact_after(after, now)?)
    }
    /// Consuming the complete snapshot releases its checkpoint buffer and its
    /// reader handle; the small committed placement witness keeps its own charge.
    pub fn into_placement(self) -> CommittedPlacement {
        self.placement
    }
}
impl Session {
    /// Trusted management operation, valid on a fully published follower too.
    /// It performs the existing bounded synchronous checkpoint fsync; ordinary
    /// replication continues to use the separate nonblocking Ready path.
    pub fn checkpoint_evidence(
        &mut self,
        now: u64,
        ttl: u64,
    ) -> Result<DurableEvidenceSnapshot, LedgerError> {
        self.begin_checkpoint_evidence(now, ttl)?;
        if let Err(error) = self.consensus.finish_checkpoint() {
            self.cancel_checkpoint_evidence()?;
            return Err(error.into());
        }
        self.try_finish_checkpoint_evidence()?
            .ok_or(LedgerError::OutcomeUnknown)
    }
    /// Prepare an immutable export while the existing disk owner performs the
    /// checkpoint rewrite. No witness escapes until its exact receipt completes.
    pub fn begin_checkpoint_evidence(&mut self, now: u64, ttl: u64) -> Result<(), LedgerError> {
        self.check()?;
        if self.persistence_pending() {
            return Err(ConsensusError::PersistencePending.into());
        }
        if self.pending_count() != 0 || self.has_ready() {
            return Err(LedgerError::Capacity);
        }
        let stored = self
            .placement_state
            .latest()
            .ok_or(LedgerError::PlacementConflict)?;
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                placement_charge(stored)?,
            )?
            .commit();
        let placement = CommittedPlacement {
            stored: stored.clone(),
            node: self.status().node_id,
            cluster: self.cluster_id(),
            _allocation: allocation,
        };
        let fence = placement.fence();
        let mut prefix = EvidencePrefix {
            cluster: self.cluster_id(),
            ledger: self.ledger,
            group: LogGroupId(self.group_id()),
            genesis: self.placement_genesis()?,
            node: placement.node(),
            sequence: self.sequence(),
            index: RaftIndex(self.applied_raft),
            term: RaftTerm(self.consensus.published_term(self.applied_raft)?),
            route: fence.to_route,
            placement_epoch: fence.placement_epoch,
            membership_epoch: fence.membership_epoch,
            operation: fence.operation,
            placement_digest: fence.placement_digest,
            checkpoint: ContentHash::default(),
            checkpoint_bytes: 0,
            artifacts: u64::try_from(self.core.snapshot().artifacts.len())
                .map_err(|_| LedgerError::Capacity)?,
        };
        // Only an activated native engine has rows to project; a replica
        // that can host one but still serves legacy history exports the
        // graph's projection as before.
        let native = if self.native.is_some() {
            let projection = self.native_evidence_projection()?;
            prefix.artifacts =
                u64::try_from(projection.objects.len()).map_err(|_| LedgerError::Capacity)?;
            Some(projection)
        } else {
            None
        };
        let captured_at = std::time::Instant::now();
        let graph = self.graph.snapshot(now, ttl)?;
        let encoded = match self.encode_checkpoint(true) {
            Ok(Some(encoded)) => encoded,
            outcome => {
                self.graph.release_snapshot(&graph)?;
                return Err(outcome.err().unwrap_or(LedgerError::Corrupt));
            }
        };
        let EncodedCheckpoint {
            bytes,
            retained,
            _scratch,
        } = encoded;
        let (checkpoint, allocation) = retained.ok_or(LedgerError::Corrupt)?;
        prefix.checkpoint = ContentHash(*blake3::hash(&checkpoint).as_bytes());
        prefix.checkpoint_bytes =
            u64::try_from(checkpoint.len()).map_err(|_| LedgerError::Capacity)?;
        if let Err(error) = self.consensus.begin_checkpoint(self.applied_raft, bytes) {
            self.graph.release_snapshot(&graph)?;
            return Err(error.into());
        }
        self.pending_evidence = Some(Box::new(DurableEvidenceSnapshot {
            prefix,
            placement,
            graph,
            native,
            checkpoint,
            clock: now,
            captured_at,
            _allocation: allocation,
        }));
        Ok(())
    }
    /// Project the native engine's committed content roots as artifact
    /// evidence (24 §20): counted first so the projection is charged once,
    /// then collected page by page and sorted by artifact id.
    fn native_evidence_projection(&self) -> Result<NativeEvidenceProjection, LedgerError> {
        let domain = ContentDomainId(self.ledger.tenant.0);
        let mut count = 0usize;
        let mut cursor = None;
        loop {
            let page = self.native_content_roots(cursor, NATIVE_PROJECTION_PAGE)?;
            count = count
                .checked_add(page.roots.len())
                .ok_or(LedgerError::Capacity)?;
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                count
                    .checked_mul(NATIVE_PROJECTION_ENTRY_BYTES)
                    .and_then(|bytes| bytes.checked_add(4096))
                    .ok_or(LedgerError::Capacity)?,
            )?
            .commit();
        let mut objects = Vec::new();
        objects
            .try_reserve_exact(count)
            .map_err(|_| LedgerError::Capacity)?;
        let mut cursor = None;
        loop {
            let page = self.native_content_roots(cursor, NATIVE_PROJECTION_PAGE)?;
            for root in page.roots {
                if objects.len() >= count {
                    return Err(LedgerError::Corrupt);
                }
                let entry = match root {
                    focal_core::native::ContentRoot::Artifact { artifact, pointer }
                    | focal_core::native::ContentRoot::Inline { artifact, pointer } => {
                        if pointer.domain != domain {
                            return Err(LedgerError::Corrupt);
                        }
                        ArtifactEvidence {
                            artifact: ArtifactRef {
                                id: artifact,
                                hash: pointer.root,
                            },
                            content: Some(ContentRef {
                                domain: pointer.domain,
                                root: pointer.root,
                                length: pointer.length,
                                class: pointer.class,
                            }),
                        }
                    }
                    focal_core::native::ContentRoot::Bundle { claim, root, bytes } => {
                        ArtifactEvidence {
                            artifact: ArtifactRef {
                                id: ArtifactId(claim.0),
                                hash: root,
                            },
                            content: Some(ContentRef {
                                domain,
                                root,
                                length: bytes,
                                class: ContentClass::Evidence,
                            }),
                        }
                    }
                };
                objects.push(entry);
            }
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        objects.sort_by(|left, right| left.artifact.id.cmp(&right.artifact.id));
        if objects
            .windows(2)
            .any(|pair| pair.first().map(|e| e.artifact.id) == pair.get(1).map(|e| e.artifact.id))
        {
            return Err(LedgerError::Corrupt);
        }
        Ok(NativeEvidenceProjection {
            objects,
            _allocation: allocation,
        })
    }
    /// Poll without waiting on the writer. Regular try_poll also advances the
    /// same checkpoint gate; either path preserves this exact unpublished export.
    pub fn try_finish_checkpoint_evidence(
        &mut self,
    ) -> Result<Option<DurableEvidenceSnapshot>, LedgerError> {
        self.check()?;
        if self.pending_evidence.is_none() {
            return Ok(None);
        }
        match self.consensus.try_finish_checkpoint() {
            Ok(false) => return Ok(None),
            Ok(true) => {}
            Err(error) => {
                self.cancel_checkpoint_evidence()?;
                return Err(error.into());
            }
        }
        let snapshot = self.pending_evidence.take().ok_or(LedgerError::Corrupt)?;
        let valid = snapshot
            .elapsed_clock()
            .and_then(|now| snapshot.artifact_after(None, now).map(|_| ()));
        if let Err(error) = valid {
            self.release_evidence_lease(&snapshot)?;
            return Err(error);
        }
        Ok(Some(*snapshot))
    }
    /// Drop interest and release the export's owned checkpoint/page lease. An
    /// already admitted rewrite still resolves through try_poll before mutation.
    pub fn cancel_checkpoint_evidence(&mut self) -> Result<(), LedgerError> {
        if let Some(snapshot) = self.pending_evidence.take() {
            self.release_evidence_lease(&snapshot)?;
        }
        self.consensus.cancel_unadmitted_checkpoint();
        Ok(())
    }
    pub fn checkpoint_in_flight(&self) -> bool {
        self.consensus.checkpoint_pending()
    }
    fn release_evidence_lease(
        &mut self,
        snapshot: &DurableEvidenceSnapshot,
    ) -> Result<(), LedgerError> {
        match self.graph.release_snapshot(&snapshot.graph) {
            Ok(()) | Err(GraphError::Memory(MemoryError::LeaseExpired)) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}
