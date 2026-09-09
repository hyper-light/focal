use crate::digest::Digest;
use crate::*;
use postcard::ser_flavors::Flavor;

/// Identity encoder version 1: fixed-width big-endian integers and u32 lengths.
/// Sets are held in BTreeSet and emitted in canonical typed order.
#[derive(Debug, thiserror::Error)]
pub enum CanonicalError {
    #[error("canonical v1 field exceeds its u32 length bound")]
    Length,
    #[error("canonical encoding allocation failed")]
    Capacity,
    #[error("command encoding failed: {0}")]
    Codec(#[from] postcard::Error),
}

pub struct CanonicalEncoder {
    bytes: Vec<u8>,
    error: Option<CanonicalError>,
}
impl Default for CanonicalEncoder {
    fn default() -> Self {
        Self::new()
    }
}
impl CanonicalEncoder {
    pub fn new() -> Self {
        Self {
            bytes: Vec::new(),
            error: None,
        }
    }
    pub fn u16(&mut self, x: u16) {
        self.fixed(&x.to_be_bytes())
    }
    pub fn u32(&mut self, x: u32) {
        self.fixed(&x.to_be_bytes())
    }
    pub fn u64(&mut self, x: u64) {
        self.fixed(&x.to_be_bytes())
    }
    pub fn fixed(&mut self, x: &[u8]) {
        if self.error.is_some() {
            return;
        }
        if self.bytes.try_reserve(x.len()).is_err() {
            self.error = Some(CanonicalError::Capacity);
            return;
        }
        self.bytes.extend_from_slice(x)
    }
    pub fn bytes(&mut self, x: &[u8]) {
        self.length(x.len());
        self.fixed(x)
    }
    pub fn string(&mut self, x: &str) {
        self.bytes(x.as_bytes())
    }
    /// Encodes a v1 collection length without truncation. A failed encoder never
    /// emits a usable partial identity; finish returns its first typed error.
    pub fn length(&mut self, length: usize) {
        match u32::try_from(length) {
            Ok(length) => self.u32(length),
            Err(_) if self.error.is_none() => self.error = Some(CanonicalError::Length),
            Err(_) => {}
        }
    }
    pub fn finish(self) -> Result<Vec<u8>, CanonicalError> {
        match self.error {
            Some(error) => Err(error),
            None => Ok(self.bytes),
        }
    }
    fn ledger(&mut self, x: LedgerId) {
        self.fixed(&x.tenant.0);
        self.fixed(&x.session.0)
    }
    fn object_ref(&mut self, x: &ObjectRef) {
        self.ledger(x.ledger);
        self.u16(x.kind.code());
        self.fixed(&x.id.0)
    }
    fn receipt(&mut self, x: ReceiptFence) {
        self.fixed(&x.receipt.0);
        self.u64(x.epoch)
    }
    fn artifacts(&mut self, x: &[ArtifactRef]) {
        self.length(x.len());
        for a in x {
            self.fixed(&a.id.0);
            self.fixed(&a.hash.0)
        }
    }
}
pub trait CanonicalContent {
    fn ledger(&self) -> LedgerId;
    fn schema(&self) -> u16;
    fn kind(&self) -> ObjectKind;
    fn encode_body(&self, e: &mut CanonicalEncoder);
    fn canonical_bytes(&self) -> Result<Vec<u8>, CanonicalError> {
        let mut e = CanonicalEncoder::new();
        e.fixed(b"focal.authored\0");
        e.u16(self.schema());
        e.ledger(self.ledger());
        e.u16(self.kind().code());
        self.encode_body(&mut e);
        e.finish()
    }
    fn content_hash(&self) -> Result<ContentHash, CanonicalError> {
        Ok(ContentHash(
            *blake3::hash(&self.canonical_bytes()?).as_bytes(),
        ))
    }
}
impl CanonicalContent for ClaimContent {
    fn ledger(&self) -> LedgerId {
        self.ledger
    }
    fn schema(&self) -> u16 {
        self.schema
    }
    fn kind(&self) -> ObjectKind {
        ObjectKind::Claim
    }
    fn encode_body(&self, e: &mut CanonicalEncoder) {
        e.fixed(&self.occurrence.0);
        e.string(&self.description);
        e.length(self.relations.len());
        for r in &self.relations {
            e.u16(r.kind.code());
            match &r.target {
                RelationTarget::Participant(p) => {
                    e.u16(1);
                    e.fixed(&p.0)
                }
                RelationTarget::Object(o) => {
                    e.u16(2);
                    e.object_ref(o)
                }
                RelationTarget::Action(a) => {
                    e.u16(3);
                    e.u16(a.code())
                }
                RelationTarget::Root(r) => {
                    e.u16(4);
                    e.fixed(&r.0)
                }
                RelationTarget::Evidence(a) => {
                    e.u16(5);
                    e.fixed(&a.id.0);
                    e.fixed(&a.hash.0)
                }
            }
        }
        e.length(self.scopes.len());
        for s in &self.scopes {
            e.u16(s.kind.code());
            e.string(&s.key)
        }
        // Allocations connecting the generated requirement objects are not authored intent.
        e.length(self.requirements.len());
        for r in &self.requirements {
            e.fixed(&r.specification.0)
        }
        match self.deadline {
            None => e.u16(0),
            Some(d) => {
                e.u16(1);
                e.fixed(&d.timer.0);
                e.u64(d.generation);
                e.u64(d.at)
            }
        }
    }
}
impl CanonicalContent for ValidationContent {
    fn ledger(&self) -> LedgerId {
        self.ledger
    }
    fn schema(&self) -> u16 {
        self.schema
    }
    fn kind(&self) -> ObjectKind {
        ObjectKind::Validation
    }
    fn encode_body(&self, e: &mut CanonicalEncoder) {
        e.fixed(&self.claim.0);
        self.encode_specification(e);
    }
}
impl ValidationContent {
    /// Authored requirement identity excludes runtime allocation of its parent claim.
    pub fn specification_hash(&self) -> Result<ContentHash, CanonicalError> {
        let mut e = CanonicalEncoder::new();
        e.fixed(b"focal.requirement-specification\0");
        e.u16(self.schema);
        e.ledger(self.ledger);
        self.encode_specification(&mut e);
        Ok(ContentHash(*blake3::hash(&e.finish()?).as_bytes()))
    }
    fn encode_specification(&self, e: &mut CanonicalEncoder) {
        e.u16(self.kind.code());
        e.u16(self.phase.code());
        e.u16(self.mode.code());
        e.string(&self.description);
        match &self.quality_bar {
            None => e.u16(0),
            Some(q) => {
                e.u16(1);
                e.string(q)
            }
        }
        e.fixed(&self.evaluator.0);
        e.length(self.handlers.len());
        for h in &self.handlers {
            e.fixed(&h.id.0);
            e.fixed(&h.version.0);
            e.u16(u16::from(h.agentic))
        }
        e.length(self.evidence_schemas.len());
        for s in &self.evidence_schemas {
            e.fixed(&s.0)
        }
        e.length(self.contributed_by.len());
        for p in &self.contributed_by {
            e.fixed(&p.0)
        }
        e.u64(self.policy_revision);
    }
}
impl CanonicalContent for ArtifactContent {
    fn ledger(&self) -> LedgerId {
        self.ledger
    }
    fn schema(&self) -> u16 {
        self.schema
    }
    fn kind(&self) -> ObjectKind {
        ObjectKind::Artifact
    }
    fn encode_body(&self, e: &mut CanonicalEncoder) {
        e.string(&self.kind);
        e.fixed(&self.schema_hash.0);
        e.bytes(&self.metadata);
        match &self.payload {
            ArtifactPayload::Inline(v) => {
                e.u16(1);
                e.bytes(v)
            }
            ArtifactPayload::Content(c) => {
                e.u16(2);
                e.fixed(&c.domain.0);
                e.fixed(&c.root.0);
                e.u64(c.length);
                e.u16(c.class.code())
            }
        }
        e.fixed(&self.producer.0);
        match self.receipt {
            None => e.u16(0),
            Some(r) => {
                e.u16(1);
                e.receipt(r)
            }
        }
        e.length(self.inputs.len());
        for i in &self.inputs {
            e.object_ref(i)
        }
        e.length(self.visibility.len());
        for v in &self.visibility {
            e.string(v)
        }
    }
}
impl CanonicalContent for TestamentContent {
    fn ledger(&self) -> LedgerId {
        self.ledger
    }
    fn schema(&self) -> u16 {
        self.schema
    }
    fn kind(&self) -> ObjectKind {
        ObjectKind::Testament
    }
    fn encode_body(&self, e: &mut CanonicalEncoder) {
        e.fixed(&self.claim.0);
        e.receipt(self.receipt);
        e.fixed(&self.evidence_set.0);
        e.artifacts(&self.artifacts);
        e.string(&self.summary);
        e.u16(self.confidence.code());
        e.u16(self.outcome.code());
    }
}
pub fn manifest_hash(artifacts: &[ArtifactRef]) -> Result<ContentHash, CanonicalError> {
    let mut e = CanonicalEncoder::new();
    e.fixed(b"focal.evidence-manifest\0");
    e.u16(crate::semantics_v1::SCHEMA);
    e.artifacts(artifacts);
    Ok(ContentHash(*blake3::hash(&e.finish()?).as_bytes()))
}
/// Request identity deliberately excludes ingress timestamps and refreshed custody proofs.
/// Postcard v1 field order is a frozen protocol schema; authored identity uses the encoder above.
pub fn command_hash(input: &AuthenticatedInput) -> Result<ContentHash, CanonicalError> {
    command_parts_hash(
        input.ledger,
        input.principal,
        &input.expected_revision,
        &input.command,
    )
}
pub(crate) fn command_parts_hash(
    ledger: LedgerId,
    principal: ParticipantId,
    expected_revision: &Option<ObjectRevision>,
    command: &Command,
) -> Result<ContentHash, CanonicalError> {
    // Legacy admission already accounts for a serialized command. Keep its
    // single-pass encoding: a second traversal regresses rich structured input.
    // Hash the header/body directly, eliminating the old second preimage buffer.
    let bytes = postcard::to_allocvec(&command_body(expected_revision, command))?;
    let mut digest = command_digest(ledger, principal, command, bytes.len())?;
    digest.update(&bytes)?;
    Ok(digest.finalize()?)
}

pub(crate) fn command_parts_hash_streamed(
    ledger: LedgerId,
    principal: ParticipantId,
    expected_revision: &Option<ObjectRevision>,
    command: &Command,
) -> Result<ContentHash, CanonicalError> {
    // Managed ingress promises hashing without a payload-sized allocation.
    // Its size/hash passes share exactly the same frozen body and header as
    // legacy admission; only the already-accounted temporary storage differs.
    let body = command_body(expected_revision, command);
    let length = postcard::experimental::serialized_size(&body)?;
    let digest = command_digest(ledger, principal, command, length)?;
    Ok(postcard::serialize_with_flavor(&body, digest)?)
}

fn command_body<'a>(
    expected_revision: &'a Option<ObjectRevision>,
    command: &'a Command,
) -> (
    durable_v1::Ref<'a, Option<ObjectRevision>>,
    durable_v1::Ref<'a, Command>,
) {
    (durable_v1::Ref(expected_revision), durable_v1::Ref(command))
}

fn command_digest(
    ledger: LedgerId,
    principal: ParticipantId,
    command: &Command,
    length: usize,
) -> Result<Digest, CanonicalError> {
    let length = u32::try_from(length).map_err(|_| CanonicalError::Length)?;
    let mut digest = Digest::new();
    digest.update(b"focal.command\0")?;
    // This existing command family is V1 even when a successor model is added.
    // New persistence/content versions must not rehash an old retry's identity.
    digest.update(&1u16.to_be_bytes())?;
    digest.update(&ledger.tenant.0)?;
    digest.update(&ledger.session.0)?;
    digest.update(&principal.0)?;
    digest.update(&durable_v1::command_code(command).to_be_bytes())?;
    digest.update(&length.to_be_bytes())?;
    Ok(digest)
}

impl From<CanonicalError> for DomainOutcome {
    fn from(error: CanonicalError) -> Self {
        match error {
            CanonicalError::Length | CanonicalError::Capacity => Self::refuse(
                ErrorCode::Capacity,
                "canonical identity exceeds representable capacity",
            ),
            CanonicalError::Codec(_) => {
                Self::refuse(ErrorCode::InvalidSchema, "canonical command codec")
            }
        }
    }
}
