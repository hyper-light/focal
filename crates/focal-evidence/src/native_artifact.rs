//! Actual local custody for native descriptors. These capabilities have no
//! decoder or public field constructor. They prove a synced local copy and a
//! configured schema check; they do not assert replicated placement or a verdict.
use super::*;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError};
use focal_model::lifecycle::artifact_descriptor::{
    ArtifactDescriptor, ContentPointer, PayloadSpec,
};
use focal_model::{RequestKey, lifecycle::ContractError};

const STORE_WORKSPACE: usize = 8 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum NativeEvidenceError {
    #[error("native evidence storage: {0}")]
    Content(#[from] ContentError),
    #[error("native evidence memory: {0}")]
    Memory(#[from] MemoryError),
    #[error("native evidence descriptor: {0}")]
    Contract(#[from] ContractError),
    #[error("native evidence schema: {0}")]
    Schema(#[from] crate::BuiltinSchemaError),
    #[error("native evidence does not match the authenticated request")]
    WrongRequest,
    #[error("native evidence schema or byte limit differs from the pinned verification budget")]
    VerificationBudgetChanged,
}

/// Installed by the trusted custody owner. Payload shape verification is not
/// program execution or an acceptance verdict. Custom registries must preserve
/// these byte/workspace bounds and pin the schema identity they implement.
/// `maximum_bytes` must be a bounded, allocation-free lookup with no IO.
pub trait NativeSchemaVerifier {
    fn maximum_bytes(&self, schema: ContentHash) -> Result<usize, crate::BuiltinSchemaError>;
    fn verify(&self, schema: ContentHash, bytes: &[u8]) -> Result<(), crate::BuiltinSchemaError>;
}
pub struct BuiltinNativeSchemas;
impl NativeSchemaVerifier for BuiltinNativeSchemas {
    fn maximum_bytes(&self, schema: ContentHash) -> Result<usize, crate::BuiltinSchemaError> {
        crate::builtin_schema_limit(schema)
    }
    fn verify(&self, schema: ContentHash, bytes: &[u8]) -> Result<(), crate::BuiltinSchemaError> {
        crate::verify_builtin_schema(schema, bytes)
    }
}

/// Checked accounting contract for one immutable schema hash and declared byte
/// maximum. Owners can retain this value before admitting a future report.
/// It reserves no memory and does not identify a verifier implementation/version
/// beyond the trusted registry's immutable schema and maximum contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeVerificationBudget {
    schema: ContentHash,
    maximum: usize,
    peak: usize,
    retained: usize,
}

impl NativeVerificationBudget {
    /// Resolve and bound the schema before any storage read, write or funding
    /// debit. The maximum may be zero for an explicitly empty custom contract.
    /// Registries must keep all parsing scratch within the declared allowance.
    pub fn for_schema(
        schema: ContentHash,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<Self, NativeEvidenceError> {
        if schema.0 == [0; 32] {
            return Err(ContractError::InvalidManifest.into());
        }
        let maximum = schemas.maximum_bytes(schema)?;
        if maximum > MAX_TRANSFER_MANIFEST_BYTES {
            return Err(ContentError::Capacity.into());
        }
        let retained = size_of::<VerifiedNativeArtifact>();
        let peak = add(retained, add(STORE_WORKSPACE, maximum)?)?;
        Ok(Self {
            schema,
            maximum,
            peak,
            retained,
        })
    }

    pub fn schema(self) -> ContentHash {
        self.schema
    }
    pub fn maximum_bytes(self) -> usize {
        self.maximum
    }
    /// Additional store/read/schema workspace plus the retained custody token.
    /// Caller-owned descriptors and their construction are accounted separately.
    pub fn peak_bytes(self) -> usize {
        self.peak
    }
    pub fn retained_bytes(self) -> usize {
        self.retained
    }

    /// Compare an admitted contract with the currently installed declaration.
    /// Growth, shrinkage and schema substitution all refuse. This performs no
    /// allocation or storage IO; the guarded verification entrypoint repeats it
    /// immediately before acquiring funding and uses this checked maximum.
    pub fn check_schema(
        &self,
        schema: ContentHash,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<(), NativeEvidenceError> {
        if self.schema != schema || *self != Self::for_schema(schema, schemas)? {
            return Err(NativeEvidenceError::VerificationBudgetChanged);
        }
        Ok(())
    }
}

/// Retainable proof of a local content tree. Immutable content storage currently
/// never deletes sealed objects. Placement policy must qualify this local copy
/// before a future distributed native ingress promises stronger durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeLocalCustody {
    request: RequestKey,
    descriptor: ContentHash,
    payload: ContentPointer,
}
impl NativeLocalCustody {
    pub fn payload(&self) -> ContentPointer {
        self.payload
    }
    pub fn check(
        &self,
        request: RequestKey,
        artifact: &ArtifactDescriptor,
    ) -> Result<(), NativeEvidenceError> {
        if self.request != request
            || self.descriptor != artifact.intent_fingerprint()
            || request.principal != artifact.producer()
        {
            return Err(NativeEvidenceError::WrongRequest);
        }
        Ok(())
    }
    /// This is a local verified-custody fact, not a global placement revision.
    pub fn local_revision(&self) -> u64 {
        1
    }
}

pub struct VerifiedNativeArtifact {
    custody: NativeLocalCustody,
    allocation: Allocation,
}
impl std::fmt::Debug for VerifiedNativeArtifact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedNativeArtifact")
            .field("custody", &self.custody)
            .finish_non_exhaustive()
    }
}
impl VerifiedNativeArtifact {
    pub fn custody(&self) -> NativeLocalCustody {
        self.custody
    }
    pub fn check(
        &self,
        request: RequestKey,
        descriptor: &ArtifactDescriptor,
    ) -> Result<(), NativeEvidenceError> {
        self.custody.check(request, descriptor)
    }
    pub fn retained_bytes(&self) -> usize {
        self.allocation.bytes()
    }
}

fn add(a: usize, b: usize) -> Result<usize, ContentError> {
    a.checked_add(b).ok_or(ContentError::Capacity)
}
fn pointer(reference: &ContentRef) -> ContentPointer {
    ContentPointer {
        domain: reference.domain,
        root: reference.root,
        length: reference.length,
        class: reference.class,
    }
}
fn reference(pointer: ContentPointer) -> ContentRef {
    ContentRef {
        domain: pointer.domain,
        root: pointer.root,
        length: pointer.length,
        class: pointer.class,
    }
}

impl ContentStore {
    /// Verify and retain actual local evidence before entering native Core.
    /// Inline payloads are sealed into the existing content-tree format too:
    /// an in-memory command alone cannot prove durable custody. Request/actor
    /// identity is pinned independently of the descriptor's content identity.
    pub fn verify_native_artifact(
        &mut self,
        request: RequestKey,
        descriptor: &ArtifactDescriptor,
        inline_domain: ContentDomainId,
        budget: &MemoryBudget,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<VerifiedNativeArtifact, NativeEvidenceError> {
        self.check_native_verification_request(request, descriptor, inline_domain)?;
        let verification = NativeVerificationBudget::for_schema(descriptor.schema_hash(), schemas)?;
        self.verify_native_artifact_quoted(
            request,
            descriptor,
            inline_domain,
            budget,
            schemas,
            &verification,
        )
    }

    /// Verify using the exact schema/maximum contract pinned by the owner.
    /// Registry mismatch refuses before acquiring funding or touching content.
    /// This does not prove that a custom verifier's implementation is unchanged;
    /// the trusted registry must honor the immutable schema it declares.
    pub fn verify_native_artifact_with_budget(
        &mut self,
        request: RequestKey,
        descriptor: &ArtifactDescriptor,
        inline_domain: ContentDomainId,
        budget: &MemoryBudget,
        schemas: &impl NativeSchemaVerifier,
        verification: &NativeVerificationBudget,
    ) -> Result<VerifiedNativeArtifact, NativeEvidenceError> {
        self.check_native_verification_request(request, descriptor, inline_domain)?;
        verification.check_schema(descriptor.schema_hash(), schemas)?;
        self.verify_native_artifact_quoted(
            request,
            descriptor,
            inline_domain,
            budget,
            schemas,
            verification,
        )
    }

    fn check_native_verification_request(
        &self,
        request: RequestKey,
        descriptor: &ArtifactDescriptor,
        inline_domain: ContentDomainId,
    ) -> Result<(), NativeEvidenceError> {
        self.check()?;
        if request.principal != descriptor.producer()
            || request.principal.is_zero()
            || request.id.is_zero()
            || request.epoch.0 == 0
            || inline_domain.is_zero()
        {
            return Err(NativeEvidenceError::WrongRequest);
        }
        Ok(())
    }

    fn verify_native_artifact_quoted(
        &mut self,
        request: RequestKey,
        descriptor: &ArtifactDescriptor,
        inline_domain: ContentDomainId,
        budget: &MemoryBudget,
        schemas: &impl NativeSchemaVerifier,
        verification: &NativeVerificationBudget,
    ) -> Result<VerifiedNativeArtifact, NativeEvidenceError> {
        let maximum = verification.maximum_bytes();
        // Includes complete read/manifest/chunk and bounded schema scratch. The
        // descriptor stays owned by ingress; this capability borrows no payload.
        let mut allocation = budget
            .reserve(
                BudgetKind::Payload,
                BudgetLane::Completion,
                verification.peak_bytes(),
            )?
            .commit();
        let payload = match descriptor.payload() {
            PayloadSpec::Inline(bytes) => {
                if bytes.len() > maximum {
                    return Err(ContentError::Capacity.into());
                }
                schemas.verify(descriptor.schema_hash(), bytes)?;
                pointer(&self.seal_native_inline(inline_domain, bytes)?)
            }
            PayloadSpec::Content(content) => {
                let content_ref = reference(content);
                let bytes = self.read_bytes(&content_ref, maximum)?;
                if bytes.capacity() > maximum {
                    return Err(ContentError::Capacity.into());
                }
                schemas.verify(descriptor.schema_hash(), &bytes)?;
                content
            }
        };
        let custody = NativeLocalCustody {
            request,
            descriptor: descriptor.intent_fingerprint(),
            payload,
        };
        allocation.shrink_to(verification.retained_bytes())?;
        Ok(VerifiedNativeArtifact {
            custody,
            allocation,
        })
    }

    /// Uses the original authenticated manifest encoding, without allocating an
    /// upload identity/tombstone for a complete bounded inline blob. Chunk files
    /// and the manifest are synced through the existing content-store primitives.
    fn seal_native_inline(
        &mut self,
        domain: ContentDomainId,
        bytes: &[u8],
    ) -> Result<ContentRef, ContentError> {
        self.check()?;
        let result = self.seal_native_inline_inner(domain, bytes);
        self.mark_failure(&result);
        result
    }
    fn seal_native_inline_inner(
        &self,
        domain: ContentDomainId,
        bytes: &[u8],
    ) -> Result<ContentRef, ContentError> {
        let length = u64::try_from(bytes.len()).map_err(|_| ContentError::Capacity)?;
        if length > self.limits.max_content_bytes || bytes.len() > MAX_TRANSFER_MANIFEST_BYTES {
            return Err(ContentError::Capacity);
        }
        let count = bytes
            .len()
            .checked_add(
                self.limits
                    .chunk_bytes
                    .checked_sub(1)
                    .ok_or(ContentError::Capacity)?,
            )
            .ok_or(ContentError::Capacity)?
            .checked_div(self.limits.chunk_bytes)
            .ok_or(ContentError::Capacity)?;
        if count
            .checked_mul(40)
            .is_none_or(|n| n > self.limits.max_manifest_bytes)
        {
            return Err(ContentError::Capacity);
        }
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(count)
            .map_err(|_| ContentError::Capacity)?;
        if chunks
            .capacity()
            .checked_mul(std::mem::size_of::<Chunk>())
            .is_none_or(|n| n > STORE_WORKSPACE)
        {
            return Err(ContentError::Capacity);
        }
        for block in bytes.chunks(self.limits.chunk_bytes) {
            let hash = ContentHash(*blake3::hash(block).as_bytes());
            chunks.push(Chunk {
                hash,
                length: u32::try_from(block.len()).map_err(|_| ContentError::Capacity)?,
            });
        }
        let manifest = Manifest {
            schema: 1,
            domain,
            class: ContentClass::Evidence,
            length,
            stream_digest: ContentHash(*blake3::hash(bytes).as_bytes()),
            chunks,
        };
        let encoded_size = postcard::experimental::serialized_size(&manifest)?;
        let size = add(MANIFEST_MAGIC.len(), encoded_size)?;
        if size > self.limits.max_manifest_bytes {
            return Err(ContentError::Capacity);
        }
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(size)
            .map_err(|_| ContentError::Capacity)?;
        if encoded.capacity() > self.limits.max_manifest_bytes {
            return Err(ContentError::Capacity);
        }
        encoded.resize(size, 0);
        encoded
            .get_mut(..MANIFEST_MAGIC.len())
            .ok_or(ContentError::Corrupt)?
            .copy_from_slice(MANIFEST_MAGIC);
        let written = postcard::to_slice(
            &manifest,
            encoded
                .get_mut(MANIFEST_MAGIC.len()..)
                .ok_or(ContentError::Corrupt)?,
        )?;
        if written.len() != encoded_size {
            return Err(ContentError::Corrupt);
        }
        let root = ContentHash(*blake3::hash(&encoded).as_bytes());
        // All deterministic limits and buffer allocations precede disk writes.
        let directory = self.root.join("objects").join(hex(&domain.0));
        durable_directory(&directory)?;
        for (block, chunk) in bytes.chunks(self.limits.chunk_bytes).zip(&manifest.chunks) {
            install_verified_chunk(
                &directory.join(format!("{}.chunk", chunk.hash)),
                block,
                chunk.hash,
            )?;
        }
        atomic_install(&directory.join(format!("{root}.manifest")), &encoded)?;
        Ok(ContentRef {
            domain,
            root,
            length,
            class: ContentClass::Evidence,
        })
    }
}

#[cfg(test)]
#[path = "native_artifact_tests.rs"]
mod tests;
