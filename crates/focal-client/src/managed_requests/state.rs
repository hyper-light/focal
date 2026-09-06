use super::*;
#[derive(Serialize, Deserialize)]
pub(super) struct State {
    schema: u16,
    context: OperationContext,
    limits: ManagedStoreLimits,
    pub phase: Phase,
    pub pending: Option<Box<RequestEnvelope>>,
    pub delivered: Vec<Delivered>,
    pub accepted: Option<(ContentHash, ContentHash)>,
}
#[derive(Serialize, Deserialize)]
pub(super) enum Phase {
    Scan { slot: u32 },
    Observed { slot: u32, generation: u64 },
    Register(Box<Registration>),
    Initialize(Box<Initialization>),
    Ready,
    Exhausted,
}
#[derive(Serialize, Deserialize)]
pub(super) struct Registration {
    pub input: RequestStreamControlInput,
    pub probe: bool,
}
#[derive(Serialize, Deserialize)]
pub(super) struct Initialization {
    pub input: RequestStreamControlInput,
    pub receipt: RequestStreamControlReceipt,
}
#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct Delivered {
    pub key: ManagedRequestKey,
    pub hash: ContentHash,
}
impl State {
    pub fn initial(&self) -> bool {
        matches!(self.phase, Phase::Scan { slot: 0 })
            && self.pending.is_none()
            && self.delivered.is_empty()
            && self.accepted.is_none()
    }
    pub fn new(context: OperationContext, limits: ManagedStoreLimits) -> Self {
        Self {
            schema: 1,
            context,
            limits,
            phase: Phase::Scan { slot: 0 },
            pending: None,
            delivered: Vec::new(),
            accepted: None,
        }
    }
    pub fn validate(
        &self,
        context: OperationContext,
        limits: ManagedStoreLimits,
    ) -> Result<(), ManagedRequestsError> {
        if self.schema != 1 || self.context != context {
            return Err(ManagedRequestsError::Context);
        }
        if self.limits != limits {
            return Err(StoreError::LimitsMismatch.into());
        }
        if self.delivered.len() > limits.window as usize {
            return Err(ManagedRequestsError::Corrupt);
        }
        let mut previous = 0;
        for mark in &self.delivered {
            if !mark.key.is_valid()
                || mark.key.stream.cluster != context.cluster
                || mark.key.stream.ledger != context.ledger
                || mark.key.stream.principal != context.principal
                || mark.key.ordinal <= previous
            {
                return Err(ManagedRequestsError::Corrupt);
            }
            previous = mark.key.ordinal;
        }
        if !matches!(self.phase, Phase::Ready) && !self.delivered.is_empty() {
            return Err(ManagedRequestsError::Corrupt);
        }
        let input = match &self.phase {
            Phase::Register(r) => Some(&r.input),
            Phase::Initialize(i) => Some(&i.input),
            _ => None,
        };
        if let Some(input) = input
            && (input.cluster != context.cluster
                || input.ledger != context.ledger
                || input.principal != context.principal
                || input.id.is_zero()
                || !matches!(input.command,RequestStreamCommand::Register{slot,expected_generation,owner,window} if slot<SCAN && expected_generation<u64::MAX && !owner.is_zero() && window==limits.window))
        {
            return Err(ManagedRequestsError::Corrupt);
        }
        if let Phase::Scan { slot } = self.phase
            && slot >= SCAN
        {
            return Err(ManagedRequestsError::Corrupt);
        }
        if let Phase::Observed { slot, generation } = self.phase
            && (slot >= SCAN || generation == u64::MAX)
        {
            return Err(ManagedRequestsError::Corrupt);
        }
        if let Some(pending) = &self.pending {
            if pending.protocol != focal_wire::MANAGED_PROTOCOL_VERSION
                || pending.ledger != context.ledger
                || pending.request_epoch != RequestEpoch(1)
                || pending.request_id.is_zero()
                || pending.route_epoch.0 == 0
            {
                return Err(ManagedRequestsError::Corrupt);
            }
            let valid = match (&self.phase, &pending.operation) {
                (
                    Phase::Scan { slot },
                    Operation::RequestStreamRead {
                        cluster,
                        query: RequestStreamQuery::Slot { slot: actual },
                    },
                ) => cluster == &context.cluster && slot == actual,
                (
                    Phase::Register(r),
                    Operation::RequestStreamRead {
                        cluster,
                        query: RequestStreamQuery::Slot { slot },
                    },
                ) => {
                    r.probe
                        && cluster == &context.cluster
                        && matches!(r.input.command,RequestStreamCommand::Register{slot:expected,..} if expected==*slot)
                }
                (Phase::Register(r), Operation::RequestStreamControl { cluster, command }) => {
                    !r.probe
                        && cluster == &context.cluster
                        && pending.request_id == r.input.id
                        && command == &r.input.command
                }
                (Phase::Ready, Operation::RequestStreamControl { cluster, .. }) => {
                    cluster == &context.cluster
                }
                _ => false,
            };
            if !valid {
                return Err(ManagedRequestsError::Corrupt);
            }
        }
        Ok(())
    }
}
