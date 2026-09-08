//! Repeatable value reads for borrowed response construction. Implementations
//! return one fixed-size value per indexed read in bounded constant work; they
//! must not allocate or execute external work. They confer no admission rights.
use super::super::{ContractError, prepare::Scratch};
use super::*;
use focal_model::{ContentHash, WaitPredicate};

const SLOT_READ_WORK: usize = 1 + size_of::<SlotBinding>();
const ARTIFACT_READ_WORK: usize = 1 + size_of::<ArtifactRef>();
const SLOT_COPY_WORK: usize = 2 * SLOT_READ_WORK;
const ARTIFACT_COPY_WORK: usize = 2 * ARTIFACT_READ_WORK;
const ROOT_HASH_WORK: usize = 20 + size_of::<WaitPredicate>();
const ROOT_COPY_WORK: usize = 2 * (1 + size_of::<WaitPredicate>());

#[cfg(test)]
#[path = "response_source_tests.rs"]
mod tests;

pub trait NativeResponseSource {
    fn summary(&self) -> &str;
    fn confidence(&self) -> Confidence;
    fn outcome(&self) -> OutcomeKind;
    fn manifest_len(&self) -> usize;
    fn diagnostic_len(&self) -> usize;
    fn manifest(&self, index: usize) -> Result<Option<SlotBinding>, NativeError>;
    fn diagnostic(&self, index: usize) -> Result<Option<ArtifactRef>, NativeError>;
}
impl<S: NativeResponseSource + ?Sized> NativeResponseSource for &S {
    fn summary(&self) -> &str {
        (**self).summary()
    }
    fn confidence(&self) -> Confidence {
        (**self).confidence()
    }
    fn outcome(&self) -> OutcomeKind {
        (**self).outcome()
    }
    fn manifest_len(&self) -> usize {
        (**self).manifest_len()
    }
    fn diagnostic_len(&self) -> usize {
        (**self).diagnostic_len()
    }
    fn manifest(&self, index: usize) -> Result<Option<SlotBinding>, NativeError> {
        (**self).manifest(index)
    }
    fn diagnostic(&self, index: usize) -> Result<Option<ArtifactRef>, NativeError> {
        (**self).diagnostic(index)
    }
}

impl NativeResponseSource for NativeResponseSpec<'_> {
    fn summary(&self) -> &str {
        self.summary
    }
    fn confidence(&self) -> Confidence {
        self.confidence
    }
    fn outcome(&self) -> OutcomeKind {
        self.outcome
    }
    fn manifest_len(&self) -> usize {
        self.manifest.len()
    }
    fn diagnostic_len(&self) -> usize {
        self.diagnostics.len()
    }
    fn manifest(&self, index: usize) -> Result<Option<SlotBinding>, NativeError> {
        Ok(self.manifest.get(index).copied())
    }
    fn diagnostic(&self, index: usize) -> Result<Option<ArtifactRef>, NativeError> {
        Ok(self.diagnostics.get(index).copied())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeSourceQuote {
    /// Final dynamic buffers, including one native allocation charge per buffer.
    pub bytes: usize,
    pub allocations: usize,
    pub prepare_visits: usize,
    /// Checked response hash pass, or a monitor plan's captured-root check.
    pub hash_visits: usize,
    pub build_visits: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Shape {
    summary: usize,
    manifest: usize,
    diagnostics: usize,
}
#[derive(Clone, Copy)]
struct Header<'a> {
    summary: &'a str,
    confidence: Confidence,
    outcome: OutcomeKind,
    shape: Shape,
}
impl<'a> Header<'a> {
    fn read(source: &'a impl NativeResponseSource) -> Self {
        let summary = source.summary();
        Self {
            summary,
            confidence: source.confidence(),
            outcome: source.outcome(),
            shape: Shape {
                summary: summary.len(),
                manifest: source.manifest_len(),
                diagnostics: source.diagnostic_len(),
            },
        }
    }
}
fn mul(a: usize, b: usize) -> Result<usize, NativeError> {
    a.checked_mul(b).ok_or(NativeError::Capacity("source work"))
}
fn invalid() -> NativeError {
    ContractError::InvalidManifest.into()
}
fn nonempty(n: usize) -> usize {
    usize::from(n != 0)
}

impl Shape {
    fn check(self, limits: NativeLimits) -> Result<(), NativeError> {
        within(self.summary, limits.response_summary_bytes)?;
        within(
            self.manifest,
            super::super::response_budget::work_limit(limits)?,
        )?;
        within(self.diagnostics, limits.diagnostics_per_cycle)
    }
    /// Five metadata reads; two end checks; one fixed-size value read per row.
    /// Hash work is the unchanged preimage: three u64 counts, summary, two tags,
    /// three fields per slot and two per diagnostic, each priced 1 + bytes.
    fn hash_visits(self, copies: usize) -> Result<usize, NativeError> {
        let reads = add(
            7,
            add(
                mul(self.manifest, SLOT_READ_WORK)?,
                mul(self.diagnostics, ARTIFACT_READ_WORK)?,
            )?,
        )?;
        let hashing = add(
            add(32, self.summary)?,
            add(mul(55, self.manifest)?, mul(50, self.diagnostics)?)?,
        )?;
        add(reads, mul(copies, hashing)?)
    }
    fn quote(self) -> Result<NativeSourceQuote, NativeError> {
        let prepare_visits = self.hash_visits(1)?;
        // Capture metadata/end markers, call three fallible reserves, copy and
        // UTF-8-check summary, read/write rows, inspect the three final heaps,
        // then hash the actual owned result with the same single-pass visitor.
        let construction = add(
            add(15, mul(2, self.summary)?)?,
            add(
                mul(self.manifest, SLOT_COPY_WORK)?,
                mul(self.diagnostics, ARTIFACT_COPY_WORK)?,
            )?,
        )?;
        Ok(NativeSourceQuote {
            bytes: charge(self.summary, self.manifest, self.diagnostics)?,
            allocations: add(
                nonempty(self.summary),
                add(nonempty(self.manifest), nonempty(self.diagnostics))?,
            )?,
            prepare_visits,
            hash_visits: self.hash_visits(2)?,
            build_visits: add(construction, prepare_visits)?,
        })
    }
}

fn count(n: usize) -> Result<[u8; 8], NativeError> {
    Ok(u64::try_from(n)
        .map_err(|_| NativeError::Capacity("response intent"))?
        .to_le_bytes())
}
fn fields(
    source: &impl NativeResponseSource,
    header: Header<'_>,
    mut field: impl FnMut(&[u8]),
) -> Result<(), NativeError> {
    field(&count(header.shape.summary)?);
    field(header.summary.as_bytes());
    field(&[match header.confidence {
        Confidence::Hint => 0,
        Confidence::Tentative => 1,
        Confidence::Committed => 2,
        Confidence::Consensus => 3,
    }]);
    field(&[match header.outcome {
        OutcomeKind::Complete => 0,
        OutcomeKind::Partial => 1,
        OutcomeKind::Refused => 2,
        OutcomeKind::Impossible => 3,
        OutcomeKind::Interrupted => 4,
        OutcomeKind::Failed => 5,
    }]);
    field(&count(header.shape.manifest)?);
    for index in 0..header.shape.manifest {
        let slot = source.manifest(index)?.ok_or_else(invalid)?;
        field(&slot.slot.to_le_bytes());
        field(&slot.artifact.id.0);
        field(&slot.artifact.hash.0);
    }
    if source.manifest(header.shape.manifest)?.is_some() {
        return Err(invalid());
    }
    field(&count(header.shape.diagnostics)?);
    for index in 0..header.shape.diagnostics {
        let artifact = source.diagnostic(index)?.ok_or_else(invalid)?;
        field(&artifact.id.0);
        field(&artifact.hash.0);
    }
    if source.diagnostic(header.shape.diagnostics)?.is_some() {
        return Err(invalid());
    }
    Ok(())
}
pub(super) fn hash_into(
    source: &impl NativeResponseSource,
    hash: &mut blake3::Hasher,
) -> Result<(), NativeError> {
    fields(source, Header::read(source), |bytes| {
        hash.update(bytes);
    })
}

/// Unlike the immutable slice plan, a generic source can change through
/// interior mutability. The captured digest binds the actual values returned;
/// construction checks its produced body before releasing any owned input.
#[derive(Debug)]
pub struct NativeResponseSourcePlan<S> {
    source: S,
    shape: Shape,
    fingerprint: ContentHash,
    quote: NativeSourceQuote,
}
impl<S: NativeResponseSource> NativeResponseSourcePlan<S> {
    pub fn prepare(
        source: S,
        limits: NativeLimits,
        max_visits: usize,
    ) -> Result<Self, NativeError> {
        within(5, max_visits)?;
        let header = Header::read(&source);
        header.shape.check(limits)?;
        let quote = header.shape.quote()?;
        within(quote.bytes, limits.preparation_bytes)?;
        within(quote.prepare_visits, max_visits)?;
        let mut hash = blake3::Hasher::new();
        fields(&source, header, |bytes| {
            hash.update(bytes);
        })?;
        let shape = header.shape;
        Ok(Self {
            source,
            shape,
            fingerprint: ContentHash(*hash.finalize().as_bytes()),
            quote,
        })
    }
    pub fn source(&self) -> &S {
        &self.source
    }
    pub(in crate::native) fn summary_bytes(&self) -> usize {
        self.shape.summary
    }
    pub(in crate::native) fn manifest_len(&self) -> usize {
        self.shape.manifest
    }
    pub(in crate::native) fn diagnostic_len(&self) -> usize {
        self.shape.diagnostics
    }
    pub fn quote(&self) -> NativeSourceQuote {
        self.quote
    }
    pub fn fingerprint(&self) -> ContentHash {
        self.fingerprint
    }
    pub fn hash_into(
        &self,
        hash: &mut blake3::Hasher,
        max_visits: usize,
    ) -> Result<(), NativeError> {
        within(self.quote.hash_visits, max_visits)?;
        let header = Header::read(&self.source);
        if header.shape != self.shape {
            return Err(invalid());
        }
        let mut candidate = hash.clone();
        let mut identity = blake3::Hasher::new();
        fields(&self.source, header, |bytes| {
            candidate.update(bytes);
            identity.update(bytes);
        })?;
        if ContentHash(*identity.finalize().as_bytes()) != self.fingerprint {
            return Err(invalid());
        }
        *hash = candidate;
        Ok(())
    }
    pub fn build(
        self,
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<NativeResponseInput, NativeError> {
        within(self.quote.bytes, max_bytes)?;
        within(self.quote.build_visits, max_visits)?;
        let header = Header::read(&self.source);
        if header.shape != self.shape {
            return Err(invalid());
        }
        let input = build(&self.source, header, self.quote.bytes)?;
        let mut hash = blake3::Hasher::new();
        hash_into(&input.as_spec(), &mut hash)?;
        if ContentHash(*hash.finalize().as_bytes()) != self.fingerprint {
            return Err(invalid());
        }
        Ok(input)
    }
}

pub(super) fn build_slice(
    spec: NativeResponseSpec<'_>,
    maximum: usize,
) -> Result<NativeResponseInput, NativeError> {
    build(&spec, Header::read(&spec), maximum)
}
fn build(
    source: &impl NativeResponseSource,
    header: Header<'_>,
    maximum: usize,
) -> Result<NativeResponseInput, NativeError> {
    let mut scratch = Scratch {
        used: 0,
        max: maximum,
    };
    let mut summary = scratch.reserve::<u8>(header.shape.summary)?;
    summary.extend_from_slice(header.summary.as_bytes());
    let summary = String::from_utf8(summary).map_err(|_| ContractError::InvalidManifest)?;
    let mut manifest = scratch.reserve::<SlotBinding>(header.shape.manifest)?;
    for index in 0..header.shape.manifest {
        manifest.push(source.manifest(index)?.ok_or_else(invalid)?);
    }
    if source.manifest(header.shape.manifest)?.is_some() {
        return Err(invalid());
    }
    let mut diagnostics = scratch.reserve::<ArtifactRef>(header.shape.diagnostics)?;
    for index in 0..header.shape.diagnostics {
        diagnostics.push(source.diagnostic(index)?.ok_or_else(invalid)?);
    }
    if source.diagnostic(header.shape.diagnostics)?.is_some() {
        return Err(invalid());
    }
    let input = NativeResponseInput {
        summary,
        confidence: header.confidence,
        outcome: header.outcome,
        manifest,
        diagnostics,
    };
    within(input.heap_charge()?, maximum)?;
    Ok(input)
}

/// Same repeatable, bounded indexed-read contract as NativeResponseSource.
pub trait NativeMonitorSource {
    fn len(&self) -> usize;
    fn root(&self, index: usize) -> Result<Option<WaitPredicate>, NativeError>;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
impl<S: NativeMonitorSource + ?Sized> NativeMonitorSource for &S {
    fn len(&self) -> usize {
        (**self).len()
    }
    fn root(&self, index: usize) -> Result<Option<WaitPredicate>, NativeError> {
        (**self).root(index)
    }
}
impl NativeMonitorSource for [WaitPredicate] {
    fn len(&self) -> usize {
        <[WaitPredicate]>::len(self)
    }
    fn root(&self, index: usize) -> Result<Option<WaitPredicate>, NativeError> {
        Ok(self.get(index).copied())
    }
}

#[derive(Debug)]
pub struct NativeMonitorSourcePlan<S> {
    source: S,
    count: usize,
    fingerprint: ContentHash,
    quote: NativeSourceQuote,
}
fn root_fields(root: WaitPredicate) -> (u8, focal_model::ClaimId) {
    match root {
        WaitPredicate::Satisfied(id) => (0, id),
        WaitPredicate::Terminal(id) => (1, id),
        WaitPredicate::Released(id) => (2, id),
    }
}
fn roots_hash(source: &impl NativeMonitorSource, count: usize) -> Result<ContentHash, NativeError> {
    let mut hash = blake3::Hasher::new();
    hash.update(&self::count(count)?);
    for index in 0..count {
        let (tag, id) = root_fields(source.root(index)?.ok_or_else(invalid)?);
        hash.update(&[tag]);
        hash.update(&id.0);
    }
    if source.root(count)?.is_some() {
        return Err(invalid());
    }
    Ok(ContentHash(*hash.finalize().as_bytes()))
}
impl<S: NativeMonitorSource> NativeMonitorSourcePlan<S> {
    pub fn prepare(
        source: S,
        limits: NativeLimits,
        max_visits: usize,
    ) -> Result<Self, NativeError> {
        within(1, max_visits)?;
        let count = source.len();
        within(count, limits.plan_edges)?;
        // One len, one terminal read, one hash-count field, then one fixed
        // root read plus tag/ID hash fields per element.
        let prepare_visits = add(11, mul(count, ROOT_HASH_WORK)?)?;
        let quote = NativeSourceQuote {
            bytes: array::<WaitPredicate>(count)?,
            allocations: nonempty(count),
            prepare_visits,
            hash_visits: prepare_visits,
            build_visits: add(prepare_visits, add(4, mul(count, ROOT_COPY_WORK)?)?)?,
        };
        within(quote.bytes, limits.preparation_bytes)?;
        within(quote.prepare_visits, max_visits)?;
        let fingerprint = roots_hash(&source, count)?;
        Ok(Self {
            source,
            count,
            fingerprint,
            quote,
        })
    }
    pub fn quote(&self) -> NativeSourceQuote {
        self.quote
    }
    pub fn len(&self) -> usize {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub fn source(&self) -> &S {
        &self.source
    }
    /// Emits precisely the captured source length and one checked terminal
    /// marker. A shared monitor-intent helper still prices and checks iteration.
    pub fn roots(&self) -> impl Iterator<Item = Result<WaitPredicate, NativeError>> + '_ {
        (0..=self.count).filter_map(|index| match self.source.root(index) {
            Ok(Some(root)) => Some(Ok(root)),
            Ok(None) if index == self.count => None,
            Ok(None) => Some(Err(invalid())),
            Err(error) => Some(Err(error)),
        })
    }
    pub fn check(&self, max_visits: usize) -> Result<(), NativeError> {
        within(self.quote.prepare_visits, max_visits)?;
        if self.source.len() != self.count
            || roots_hash(&self.source, self.count)? != self.fingerprint
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn build(
        self,
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<Vec<WaitPredicate>, NativeError> {
        within(self.quote.bytes, max_bytes)?;
        within(self.quote.build_visits, max_visits)?;
        if self.source.len() != self.count {
            return Err(invalid());
        }
        let mut scratch = Scratch {
            used: 0,
            max: self.quote.bytes,
        };
        let mut roots = scratch.reserve(self.count)?;
        for index in 0..self.count {
            roots.push(self.source.root(index)?.ok_or_else(invalid)?);
        }
        if self.source.root(self.count)?.is_some() {
            return Err(invalid());
        }
        if roots_hash(&roots.as_slice(), self.count)? != self.fingerprint {
            return Err(invalid());
        }
        within(array::<WaitPredicate>(roots.capacity())?, self.quote.bytes)?;
        Ok(roots)
    }
}
