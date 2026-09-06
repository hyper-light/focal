use super::*;
use crate::{
    managed_requests::ManagedRequests,
    managed_store::{ManagedOperationId, ManagedStoreLimits},
    operation_store::{OperationIntent, files::Directory},
    pending::OperationContext,
};
use std::path::Path;

#[derive(Clone, Serialize, Deserialize)]
enum Phase {
    Start,
    Seed {
        cursor: CursorToken,
        token: ReadToken,
        after: Option<ObjectKey>,
    },
    Tail {
        cursor: CursorToken,
    },
}
#[derive(Clone, Serialize, Deserialize)]
enum Pending {
    Managed {
        key: ManagedRequestKey,
        operation: StreamRequest,
    },
    Read(RequestEnvelope),
}
#[derive(Serialize, Deserialize)]
struct State {
    schema: u16,
    context: OperationContext,
    name: String,
    options: WatchOptions,
    consumer: ConsumerId,
    ownership_ready: bool,
    phase: Phase,
    pending: Option<Pending>,
    delivery: Option<WatchDelivery>,
    cleanup: Option<ManagedRequestKey>,
    delivered: u64,
    acknowledged: u64,
    last_ack: Option<ContentHash>,
}
impl State {
    fn validate(&self) -> Result<(), WatchError> {
        if self.delivered.checked_sub(self.acknowledged) != Some(u64::from(self.delivery.is_some()))
            || (self.last_ack.is_some() != (self.acknowledged > 0))
            || self.pending.is_some() && self.delivery.is_some()
            || !self.ownership_ready
                && (self.pending.is_some() || self.cleanup.is_some() || self.delivered != 0)
        {
            return Err(WatchError::Corrupt);
        }
        match self.phase {
            Phase::Start => {}
            Phase::Seed { cursor, token, .. } => {
                self.cursor(cursor)?;
                self.token(token)?;
                if cursor.position.sequence != token.sequence {
                    return Err(WatchError::Corrupt);
                }
            }
            Phase::Tail { cursor } => self.cursor(cursor)?,
        }
        if let Some(key) = self.cleanup {
            self.key(key)?;
        }
        if let Some(pending) = &self.pending {
            if self.cleanup.is_some() {
                return Err(WatchError::Corrupt);
            }
            match pending {
                Pending::Managed { key, operation } => {
                    self.key(*key)?;
                    let expected = match self.phase {
                        Phase::Start => StreamRequest::Open {
                            consumer: self.consumer,
                            filter: self.options.filter(),
                            start: None,
                            seed: self.options.seed,
                            credits: self.options.credits(),
                        },
                        Phase::Seed {
                            cursor,
                            token,
                            after: None,
                        } => StreamRequest::CompleteSeed {
                            cursor,
                            filter: self.options.filter(),
                            snapshot: token.sequence,
                        },
                        Phase::Tail { cursor } => StreamRequest::Poll {
                            cursor,
                            filter: self.options.filter(),
                            acknowledged: Some(cursor),
                            credits: self.options.credits(),
                        },
                        _ => return Err(WatchError::Corrupt),
                    };
                    if operation != &expected {
                        return Err(WatchError::Corrupt);
                    }
                }
                Pending::Read(request) => {
                    let Phase::Seed {
                        token,
                        after: Some(after),
                        ..
                    } = self.phase
                    else {
                        return Err(WatchError::Corrupt);
                    };
                    if request.protocol != focal_wire::PROTOCOL_VERSION
                        || request.ledger != self.context.ledger
                        || request.request_epoch != RequestEpoch(1)
                        || request.request_id.is_zero()
                        || request.route_epoch.0 == 0
                        || request.operation
                            != Operation::Read(ReadRequest {
                                consistency: ReadConsistency::Exact(token),
                                query: ReadQuery::SeedScan {
                                    after: Some(after),
                                    claims: self.options.claims.clone(),
                                    max_bytes: self.options.max_bytes.saturating_sub(512),
                                },
                                max_items: self.options.max_items,
                            })
                    {
                        return Err(WatchError::Corrupt);
                    }
                }
            }
        }
        if let Some(delivery) = &self.delivery {
            if delivery.number != self.delivered
                || postcard::experimental::serialized_size(&delivery.page)
                    .map_err(|_| WatchError::Corrupt)?
                    > MAX_WATCH_PAGE_BYTES
                || delivery.id
                    != ContentHash(
                        *blake3::hash(&encode(&(
                            self.context,
                            &self.name,
                            delivery.number,
                            &delivery.page,
                        ))?)
                        .as_bytes(),
                    )
            {
                return Err(WatchError::Corrupt);
            }
            match &delivery.page {
                WatchPage::Seed { page } => {
                    let Phase::Seed { token, .. } = self.phase else {
                        return Err(WatchError::Corrupt);
                    };
                    if page.token != token {
                        return Err(WatchError::Corrupt);
                    }
                    let request = RequestEnvelope {
                        protocol: 1,
                        ledger: self.context.ledger,
                        route_epoch: token.route_epoch,
                        request_epoch: RequestEpoch(1),
                        request_id: RequestId::from_u128(1),
                        operation: Operation::Read(ReadRequest {
                            consistency: ReadConsistency::Exact(token),
                            query: ReadQuery::SeedScan {
                                after: None,
                                claims: self.options.claims.clone(),
                                max_bytes: self.options.max_bytes,
                            },
                            max_items: self.options.max_items,
                        }),
                    };
                    focal_wire::validate_response(
                        &request,
                        &request.reply(Response::Read(page.clone())),
                        Some(self.context.principal),
                        &WireLimits::default(),
                    )
                    .map_err(|_| WatchError::Corrupt)?;
                }
                WatchPage::Events { page } => {
                    self.token(page.token)?;
                    self.cursor(page.cursor)?;
                    self.cursor(page.acknowledged)?;
                    if page.seed.is_some()
                        || page.acknowledged.scope != page.cursor.scope
                        || page.acknowledged.generation != page.cursor.generation
                        || page.acknowledged.position > page.cursor.position
                        || page.events.len() > (self.options.max_items as usize).saturating_add(1)
                        || page
                            .events
                            .iter()
                            .filter(|event| matches!(event, StreamEvent::Delta { .. }))
                            .count()
                            > self.options.max_items as usize
                        || page
                            .events
                            .iter()
                            .filter(|event| !matches!(event, StreamEvent::Delta { .. }))
                            .count()
                            > 1
                    {
                        return Err(WatchError::Corrupt);
                    }
                    for event in &page.events {
                        let cursor = match event {
                            StreamEvent::Delta { cursor, delta } => {
                                if delta.id.ledger != self.context.ledger
                                    || delta.id.sequence != cursor.position.sequence
                                {
                                    return Err(WatchError::Corrupt);
                                }
                                cursor
                            }
                            StreamEvent::Resolved { cursor }
                            | StreamEvent::Resync { cursor, .. } => cursor,
                        };
                        self.cursor(*cursor)?;
                        if cursor.scope != page.cursor.scope
                            || cursor.generation != page.cursor.generation
                            || cursor.position > page.cursor.position
                        {
                            return Err(WatchError::Corrupt);
                        }
                    }
                }
            }
        }
        Ok(())
    }
    fn key(&self, key: ManagedRequestKey) -> Result<(), WatchError> {
        if !key.is_valid()
            || key.stream.cluster != self.context.cluster
            || key.stream.ledger != self.context.ledger
            || key.stream.principal != self.context.principal
        {
            Err(WatchError::Corrupt)
        } else {
            Ok(())
        }
    }
    fn token(&self, token: ReadToken) -> Result<(), WatchError> {
        if token.ledger != self.context.ledger || token.route_epoch.0 == 0 {
            Err(WatchError::Corrupt)
        } else {
            Ok(())
        }
    }
    fn cursor(&self, cursor: CursorToken) -> Result<(), WatchError> {
        if cursor.key.ledger != self.context.ledger
            || cursor.position.ledger != self.context.ledger
            || cursor.key.consumer != self.consumer
            || cursor.generation == 0
            || cursor.position.sequence.0 == 0 && cursor.position.offset != PositionOffset::Resolved
        {
            Err(WatchError::Corrupt)
        } else {
            Ok(())
        }
    }
}

/// An owned exact request. Keep it until `accept`; dropping it leaves the
/// journal pending and cannot acknowledge any delivered source data.
pub enum WatchAction {
    Request(Box<WatchRequest>),
    Delivery,
}
pub struct WatchRequest {
    pub request: RequestEnvelope,
    maintenance: bool,
}
pub struct WatchJournal {
    directory: Directory,
    record: String,
    state: State,
    requests: ManagedRequests,
    failed: bool,
}
impl WatchJournal {
    pub(super) fn open(
        parent: &Path,
        name: &str,
        context: OperationContext,
        options: WatchOptions,
        create: bool,
    ) -> Result<Self, WatchError> {
        let mut h = blake3::Hasher::new_derive_key("focal.watch.local-name.v1");
        h.update(&encode(&(context, name))?);
        let name_hash = h.finalize();
        let short: String = name_hash.to_hex().chars().take(32).collect();
        let stem = format!("watch-{short}");
        let record = format!("{stem}.watch-owner");
        let (directory, initialized) = Directory::watch(parent, &stem, create)?;
        if !directory.exists(&record)? {
            if initialized || !create {
                return Err(WatchError::Corrupt);
            }
            let mut id = [0; 16];
            for (target, source) in id.iter_mut().zip(name_hash.as_bytes()) {
                *target = *source;
            }
            let state = State {
                schema: 1,
                context,
                name: name.into(),
                options: options.clone(),
                consumer: ConsumerId(id),
                ownership_ready: false,
                phase: Phase::Start,
                pending: None,
                delivery: None,
                cleanup: None,
                delivered: 0,
                acknowledged: 0,
                last_ack: None,
            };
            directory.write(&record, MAGIC, &encode(&state)?, false)?;
        }
        let bytes = directory.read(&record, MAGIC, RECORD_BYTES)?;
        let (state, rest): (State, &[u8]) =
            postcard::take_from_bytes(&bytes).map_err(|_| WatchError::Corrupt)?;
        if !rest.is_empty() || encode(&state)? != bytes {
            return Err(WatchError::Corrupt);
        }
        if state.schema != 1
            || state.context != context
            || state.name != name
            || state.options != options
            || state.consumer.0 == [0; 16]
            || state.acknowledged > state.delivered
        {
            return Err(WatchError::Corrupt);
        }
        state.options.validate()?;
        state.validate()?;
        if !initialized {
            if state.delivered != 0
                || state.pending.is_some()
                || !matches!(state.phase, Phase::Start)
            {
                return Err(WatchError::Corrupt);
            }
            directory.finish_coordinator()?;
        }
        let requests_name = format!("{stem}.requests");
        let limits = ManagedStoreLimits {
            window: 4,
            max_reserved_bytes: 64 * 1024 * 1024,
        };
        let requests = if state.ownership_ready {
            ManagedRequests::open_existing(parent, &requests_name, context, limits)?
        } else {
            ManagedRequests::open(parent, &requests_name, context, limits)?
        };
        let mut this = Self {
            directory,
            record,
            state,
            requests,
            failed: false,
        };
        if !this.state.ownership_ready {
            this.state.ownership_ready = true;
            this.save()?;
        }
        Ok(this)
    }
    pub fn status(&self) -> WatchStatus {
        WatchStatus {
            name: self.state.name.clone(),
            options: self.state.options.clone(),
            delivered: self.state.delivered,
            acknowledged: self.state.acknowledged,
            pending: self.state.pending.is_some(),
            delivery: self.state.delivery.as_ref().map(|d| d.id),
            seeding: matches!(self.state.phase, Phase::Seed { .. })
                || matches!(self.state.phase, Phase::Start) && self.state.options.seed,
        }
    }
    pub fn delivery(&self) -> Option<&WatchDelivery> {
        self.state.delivery.as_ref()
    }
    /// Local synchronization and writes happen on the caller's synchronous
    /// owner, between network waits. The dedicated watch lock serializes its
    /// allocator; no lock on another watch or the catalogue crosses an await.
    pub fn next_action(
        &mut self,
        ids: &mut impl input::IdGenerator,
    ) -> Result<WatchAction, WatchError> {
        self.healthy()?;
        if self.state.delivery.is_some() {
            return Ok(WatchAction::Delivery);
        }
        if let Some(key) = self.state.cleanup {
            self.requests
                .mark_delivered(ManagedOperationId::from_key(key)?)?;
            if let Some(request) = self.requests.maintenance(ids)? {
                return Ok(WatchAction::Request(Box::new(WatchRequest {
                    request,
                    maintenance: true,
                })));
            }
            self.state.cleanup = None;
            self.save()?;
        }
        if let Some(request) = self.requests.maintenance(ids)? {
            return Ok(WatchAction::Request(Box::new(WatchRequest {
                request,
                maintenance: true,
            })));
        }
        if self.state.pending.is_none() {
            let filter = self.state.options.filter();
            let credits = self.state.options.credits();
            let operation = match self.state.phase {
                Phase::Start => Some(StreamRequest::Open {
                    consumer: self.state.consumer,
                    filter,
                    start: None,
                    seed: self.state.options.seed,
                    credits,
                }),
                Phase::Seed {
                    cursor,
                    token,
                    after: None,
                } => Some(StreamRequest::CompleteSeed {
                    cursor,
                    filter,
                    snapshot: token.sequence,
                }),
                Phase::Seed {
                    token,
                    after: Some(after),
                    ..
                } => {
                    let query = ReadQuery::SeedScan {
                        after: Some(after),
                        claims: self.state.options.claims.clone(),
                        max_bytes: self.state.options.max_bytes.saturating_sub(512),
                    };
                    let request = self.envelope(
                        fresh(ids)?,
                        Operation::Read(ReadRequest {
                            consistency: ReadConsistency::Exact(token),
                            query,
                            max_items: self.state.options.max_items,
                        }),
                    );
                    self.state.pending = Some(Pending::Read(request));
                    None
                }
                Phase::Tail { cursor } => Some(StreamRequest::Poll {
                    cursor,
                    filter,
                    acknowledged: Some(cursor),
                    credits,
                }),
            };
            if let Some(operation) = operation {
                let status = self.requests.store()?.status()?;
                let ordinal = status
                    .issued_through
                    .checked_add(1)
                    .ok_or(WatchError::Capacity)?;
                let key = ManagedRequestKey {
                    stream: status.stream,
                    ordinal,
                    id: fresh(ids)?,
                };
                // Persist the expected reservation before touching the allocator.
                self.state.pending = Some(Pending::Managed { key, operation });
            }
            self.save()?;
        }
        let pending = self.state.pending.as_ref().ok_or(WatchError::Corrupt)?;
        let request = match pending {
            Pending::Read(request) => request.clone(),
            Pending::Managed { key, operation } => {
                let store = self.requests.store()?;
                let id = ManagedOperationId::from_key(*key)?;
                let status = store.status()?;
                if status.stream != key.stream {
                    return Err(WatchError::Corrupt);
                }
                if status.issued_through < key.ordinal
                    && (status.issued_through.checked_add(1) != Some(key.ordinal)
                        || store.reserve(key.id)? != id)
                {
                    return Err(WatchError::Corrupt);
                }
                let canonical = encode(operation)?;
                let request = self.envelope(
                    key.id,
                    Operation::Managed {
                        key: *key,
                        operation: ManagedOperation::Cursor(operation.clone()),
                    },
                );
                store
                    .prepare(
                        id,
                        OperationIntent {
                            name: "watch.cursor",
                            version: 1,
                            canonical: &canonical,
                        },
                        |_| Ok(request),
                    )?
                    .request
            }
        };
        Ok(WatchAction::Request(Box::new(WatchRequest {
            request,
            maintenance: false,
        })))
    }
    pub fn accept(
        &mut self,
        action: Box<WatchRequest>,
        response: ResponseEnvelope,
    ) -> Result<(), WatchError> {
        self.healthy()?;
        if action.maintenance {
            return match self.requests.accept_maintenance(&action.request, response) {
                Ok(()) | Err(ManagedRequestsError::Stale) => Ok(()),
                Err(error) => Err(error.into()),
            };
        }
        let mut bound = action.request.clone();
        bound.route_epoch = response.route_epoch;
        focal_wire::validate_response(
            &bound,
            &response,
            Some(self.state.context.principal),
            &WireLimits::default(),
        )
        .map_err(|_| WatchError::InvalidResponse)?;
        if let Response::Error(error) = response.result {
            return Err(WatchError::Remote(error));
        }
        let pending = self
            .state
            .pending
            .as_ref()
            .ok_or(WatchError::InvalidResponse)?;
        let mut cleanup = self.state.cleanup;
        let mut phase = self.state.phase.clone();
        let page = match (pending, response.result) {
            (Pending::Read(saved), Response::Read(page)) if saved == &action.request => {
                WatchPage::Seed { page }
            }
            (Pending::Managed { key, operation }, Response::Managed(reply)) => {
                if !matches!(&action.request.operation,Operation::Managed{key:actual,operation:ManagedOperation::Cursor(actual_op)} if actual==key&&actual_op==operation)
                {
                    return Err(WatchError::InvalidResponse);
                }
                let stream = reply.stream.ok_or(WatchError::InvalidResponse)?;
                self.requests
                    .store()?
                    .record_receipt(ManagedOperationId::from_key(*key)?, &reply.receipt)?;
                cleanup = Some(*key);
                if matches!(operation, StreamRequest::CompleteSeed { .. }) {
                    self.state.cleanup = cleanup;
                    self.state.phase = Phase::Tail {
                        cursor: stream.cursor,
                    };
                    self.state.pending = None;
                    return self.save();
                }
                if let Some(page) = stream.seed {
                    phase = Phase::Seed {
                        cursor: stream.cursor,
                        token: page.token,
                        after: page.next,
                    };
                    WatchPage::Seed { page }
                } else {
                    WatchPage::Events {
                        page: Box::new(stream),
                    }
                }
            }
            _ => return Err(WatchError::InvalidResponse),
        };
        let size =
            postcard::experimental::serialized_size(&page).map_err(|_| WatchError::Capacity)?;
        if size > MAX_WATCH_PAGE_BYTES {
            return Err(WatchError::Capacity);
        }
        let number = self
            .state
            .delivered
            .checked_add(1)
            .ok_or(WatchError::Capacity)?;
        let page = self.select(page);
        let id = ContentHash(
            *blake3::hash(&encode(&(
                self.state.context,
                &self.state.name,
                number,
                &page,
            ))?)
            .as_bytes(),
        );
        self.state.cleanup = cleanup;
        self.state.phase = phase;
        self.state.delivery = Some(WatchDelivery { id, number, page });
        self.state.delivered = number;
        self.state.pending = None;
        self.save()
    }
    /// Call only after the output has been consumed. Exact repeated ACKs are
    /// harmless, including after managed receipt retirement and process restart.
    pub fn acknowledge(&mut self, id: ContentHash) -> Result<(), WatchError> {
        self.healthy()?;
        if self.state.delivery.is_none() && self.state.last_ack == Some(id) {
            return Ok(());
        }
        let delivery = self
            .state
            .delivery
            .as_ref()
            .filter(|d| d.id == id)
            .ok_or(WatchError::DeliveryMismatch)?;
        self.state.phase = match &delivery.page {
            WatchPage::Seed { page } => {
                let Phase::Seed { cursor, .. } = self.state.phase else {
                    return Err(WatchError::Corrupt);
                };
                Phase::Seed {
                    cursor,
                    token: page.token,
                    after: page.next,
                }
            }
            WatchPage::Events { page } => Phase::Tail {
                cursor: page.cursor,
            },
        };
        self.state.acknowledged = delivery.number;
        self.state.last_ack = Some(id);
        self.state.delivery = None;
        self.save()
    }
    fn select(&self, mut page: WatchPage) -> WatchPage {
        let Some(family) = self.state.options.family else {
            return page;
        };
        match &mut page {
            WatchPage::Seed { page } => page.objects.retain(|object| match object {
                focal_wire::ReadObject::Claim { .. } => family == ObjectKind::Claim,
                focal_wire::ReadObject::Testament { .. } => family == ObjectKind::Testament,
                focal_wire::ReadObject::Artifact { .. } => family == ObjectKind::Artifact,
                focal_wire::ReadObject::Validation { .. }
                | focal_wire::ReadObject::ValidationResults { .. } => {
                    family == ObjectKind::Validation
                }
            }),
            WatchPage::Events { page } => page.events.retain(|event| match event {
                StreamEvent::Delta { delta, .. } => match delta.fact {
                    DeltaFact::Artifact(_) => family == ObjectKind::Artifact,
                    DeltaFact::Testament { .. } => family == ObjectKind::Testament,
                    DeltaFact::ValidationScheduled(_) | DeltaFact::Verdict(_) => {
                        family == ObjectKind::Validation
                    }
                    DeltaFact::Status {
                        previous: None,
                        current: ClaimStatus::Generated,
                        ..
                    } => family == ObjectKind::Claim || family == ObjectKind::Validation,
                    DeltaFact::Epoch(_) => false,
                    _ => family == ObjectKind::Claim,
                },
                _ => true,
            }),
        }
        page
    }
    fn envelope(&self, id: RequestId, operation: Operation) -> RequestEnvelope {
        RequestEnvelope {
            protocol: if matches!(operation, Operation::Managed { .. }) {
                focal_wire::MANAGED_PROTOCOL_VERSION
            } else {
                focal_wire::PROTOCOL_VERSION
            },
            ledger: self.state.context.ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: id,
            operation,
        }
    }
    fn healthy(&self) -> Result<(), WatchError> {
        if self.failed {
            Err(WatchError::Failed)
        } else {
            Ok(())
        }
    }
    fn save(&mut self) -> Result<(), WatchError> {
        self.failed = true;
        self.state.validate()?;
        let bytes = encode(&self.state)?;
        self.directory.write(&self.record, MAGIC, &bytes, true)?;
        self.failed = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    fn context() -> OperationContext {
        OperationContext {
            cluster: [1; 16],
            ledger: LedgerId {
                tenant: TenantId::from_u128(1),
                session: SessionId::from_u128(2),
            },
            principal: ParticipantId::from_u128(3),
        }
    }
    #[test]
    fn checksummed_semantic_corruption_and_trailing_bytes_cannot_resume() {
        for trailing in [false, true] {
            let root = tempfile::tempdir().unwrap();
            std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
            let store = WatchStore::open(root.path(), context()).unwrap();
            let mut journal = store.create("bad", WatchOptions::default()).unwrap();
            let mut bytes = if trailing {
                encode(&journal.state).unwrap()
            } else {
                journal.state.delivered = 1;
                encode(&journal.state).unwrap()
            };
            if trailing {
                bytes.push(0);
            }
            journal
                .directory
                .write(&journal.record, MAGIC, &bytes, true)
                .unwrap();
            drop(journal);
            assert!(matches!(store.resume("bad"), Err(WatchError::Corrupt)));
        }
    }
    #[test]
    fn ownership_marker_prevents_recreating_lost_managed_allocator() {
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let store = WatchStore::open(root.path(), context()).unwrap();
        drop(store.create("owned", WatchOptions::default()).unwrap());
        for entry in std::fs::read_dir(root.path()).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy();
            if name.ends_with(".managed-owner") || name.ends_with(".managed-lock") {
                std::fs::remove_file(path).unwrap();
            }
        }
        assert!(matches!(
            store.resume("owned"),
            Err(WatchError::Maintenance(ManagedRequestsError::Missing))
        ));
    }
    #[test]
    fn catalogue_and_per_watch_locks_bound_independent_owners() {
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let store = WatchStore::open(root.path(), context()).unwrap();
        let first = store.create("one", WatchOptions::default()).unwrap();
        assert!(matches!(
            store.resume("one"),
            Err(WatchError::Store(StoreError::Locked))
        ));
        drop(store.create("two", WatchOptions::default()).unwrap());
        drop(first);
        for index in 2..MAX_WATCHES {
            drop(
                store
                    .create(&format!("watch-{index}"), WatchOptions::default())
                    .unwrap(),
            );
        }
        assert_eq!(store.names().unwrap().len(), MAX_WATCHES);
        assert!(matches!(
            store.create("overflow", WatchOptions::default()),
            Err(WatchError::Capacity)
        ));
    }
}
