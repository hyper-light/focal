//! Custody receipts (doc 04 §7, R8 instruction 2): the durable note a
//! coordinator keeps of one verified copy of one content object under one
//! custody scope, taken from the copy's `Durable` reply over its
//! authenticated connection (or from this node's own store). A request or an
//! intent is never a receipt: the note is written only once the copy has
//! verified the whole object. Receipts are named by (ledger, root, node), so
//! an artifact's custody obligation is read by its copies without a scan,
//! and each carries the route epoch and policy revision it was taken at, so
//! a placement change does not inherit an older placement's receipts.
use crate::store::{ContentStore, CustodyRecordKind};
use focal_model::{ContentDomainId, ContentHash, LedgerId, RouteEpoch, SessionId, TenantId};

pub const RECEIPT_MAGIC: [u8; 8] = *b"FCCRCPT1";
/// Magic, ledger, domain, root, length, node, route epoch, policy revision,
/// recorded-at, attestation.
pub const RECEIPT_BYTES: usize = 8 + 32 + 16 + 32 + 8 + 8 + 8 + 8 + 8 + 32;
const BODY_BYTES: usize = RECEIPT_BYTES - 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CustodyReceipt {
    pub ledger: LedgerId,
    pub domain: ContentDomainId,
    pub root: ContentHash,
    pub length: u64,
    /// The copy that verified the object.
    pub node: u64,
    pub route_epoch: RouteEpoch,
    pub policy_revision: u64,
    /// Milliseconds since the Unix epoch on the recorder's clock.
    pub recorded_at: u64,
    pub attestation: ContentHash,
}
impl CustodyReceipt {
    #[allow(
        clippy::too_many_arguments,
        reason = "one receipt is exactly these facts"
    )]
    pub fn new(
        ledger: LedgerId,
        domain: ContentDomainId,
        root: ContentHash,
        length: u64,
        node: u64,
        route_epoch: RouteEpoch,
        policy_revision: u64,
        recorded_at: u64,
    ) -> Self {
        let mut receipt = Self {
            ledger,
            domain,
            root,
            length,
            node,
            route_epoch,
            policy_revision,
            recorded_at,
            attestation: ContentHash([0; 32]),
        };
        let encoded = receipt.encode();
        receipt.attestation = attest(&encoded);
        receipt
    }
    /// The record name a receipt is stored under.
    pub fn name(ledger: LedgerId, root: ContentHash, node: u64) -> ContentHash {
        let mut hasher = blake3::Hasher::new_derive_key("focal.custody.receipt.name.v1");
        hasher.update(&ledger.tenant.0);
        hasher.update(&ledger.session.0);
        hasher.update(&root.0);
        hasher.update(&node.to_le_bytes());
        ContentHash(*hasher.finalize().as_bytes())
    }
    pub fn encode(&self) -> [u8; RECEIPT_BYTES] {
        let mut bytes = [0u8; RECEIPT_BYTES];
        let mut at: usize = 0;
        let mut put = |field: &[u8]| {
            let end = at.saturating_add(field.len());
            if let Some(slot) = bytes.get_mut(at..end) {
                slot.copy_from_slice(field);
            }
            at = end;
        };
        put(&RECEIPT_MAGIC);
        put(&self.ledger.tenant.0);
        put(&self.ledger.session.0);
        put(&self.domain.0);
        put(&self.root.0);
        put(&self.length.to_le_bytes());
        put(&self.node.to_le_bytes());
        put(&self.route_epoch.0.to_le_bytes());
        put(&self.policy_revision.to_le_bytes());
        put(&self.recorded_at.to_le_bytes());
        put(&self.attestation.0);
        bytes
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, crate::ContentError> {
        if bytes.len() != RECEIPT_BYTES || !bytes.starts_with(&RECEIPT_MAGIC) {
            return Err(crate::ContentError::Corrupt);
        }
        let mut at: usize = 8;
        let mut take = |count: usize| -> Result<&[u8], crate::ContentError> {
            let end = at.saturating_add(count);
            let field = bytes.get(at..end).ok_or(crate::ContentError::Corrupt)?;
            at = end;
            Ok(field)
        };
        let array16 = |field: &[u8]| -> Result<[u8; 16], crate::ContentError> {
            field.try_into().map_err(|_| crate::ContentError::Corrupt)
        };
        let array32 = |field: &[u8]| -> Result<[u8; 32], crate::ContentError> {
            field.try_into().map_err(|_| crate::ContentError::Corrupt)
        };
        let u64_of = |field: &[u8]| -> Result<u64, crate::ContentError> {
            Ok(u64::from_le_bytes(
                field.try_into().map_err(|_| crate::ContentError::Corrupt)?,
            ))
        };
        let tenant = TenantId(array16(take(16)?)?);
        let session = SessionId(array16(take(16)?)?);
        let domain = ContentDomainId(array16(take(16)?)?);
        let root = ContentHash(array32(take(32)?)?);
        let length = u64_of(take(8)?)?;
        let node = u64_of(take(8)?)?;
        let route_epoch = RouteEpoch(u64_of(take(8)?)?);
        let policy_revision = u64_of(take(8)?)?;
        let recorded_at = u64_of(take(8)?)?;
        let attestation = ContentHash(array32(take(32)?)?);
        let receipt = Self {
            ledger: LedgerId { tenant, session },
            domain,
            root,
            length,
            node,
            route_epoch,
            policy_revision,
            recorded_at,
            attestation,
        };
        if attest(&receipt.encode()) != attestation || node == 0 {
            return Err(crate::ContentError::Corrupt);
        }
        Ok(receipt)
    }
}
/// The attestation over everything but itself.
fn attest(encoded: &[u8; RECEIPT_BYTES]) -> ContentHash {
    let body = encoded.get(..BODY_BYTES).unwrap_or(&[]);
    ContentHash(
        *blake3::derive_key("focal.custody.receipt.v1", body)
            .first_chunk::<32>()
            .unwrap_or(&[0; 32]),
    )
}

impl ContentStore {
    /// Keep one copy's receipt; the same receipt again is idempotent and a
    /// different one for the same copy replaces it (the newer scope).
    pub fn record_custody_receipt(
        &mut self,
        receipt: &CustodyReceipt,
    ) -> Result<(), crate::ContentError> {
        let name = CustodyReceipt::name(receipt.ledger, receipt.root, receipt.node);
        self.replace_named_custody_record(CustodyRecordKind::Receipt, name, &receipt.encode())
    }
    /// The receipt held for one copy of one object, if any; a corrupt record
    /// is an error, never a receipt.
    pub fn custody_receipt(
        &self,
        ledger: LedgerId,
        root: ContentHash,
        node: u64,
    ) -> Result<Option<CustodyReceipt>, crate::ContentError> {
        let name = CustodyReceipt::name(ledger, root, node);
        self.read_named_custody_record(CustodyRecordKind::Receipt, name, RECEIPT_BYTES)?
            .map(|bytes| CustodyReceipt::decode(&bytes))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StoreLimits;

    fn receipt(node: u64, revision: u64) -> CustodyReceipt {
        CustodyReceipt::new(
            LedgerId {
                tenant: TenantId::from_u128(1),
                session: SessionId::from_u128(2),
            },
            ContentDomainId([1; 16]),
            ContentHash([7; 32]),
            4096,
            node,
            RouteEpoch(3),
            revision,
            1_700_000_000_000,
        )
    }

    #[test]
    fn a_receipt_round_trips_and_every_forgery_is_refused() {
        let original = receipt(2, 5);
        let encoded = original.encode();
        assert_eq!(encoded.len(), RECEIPT_BYTES);
        assert_eq!(CustodyReceipt::decode(&encoded).unwrap(), original);
        for offset in 0..RECEIPT_BYTES {
            let mut forged = encoded;
            forged[offset] ^= 0x01;
            assert!(
                CustodyReceipt::decode(&forged).is_err(),
                "a flip at {offset} decoded"
            );
        }
        assert!(CustodyReceipt::decode(&encoded[..RECEIPT_BYTES - 1]).is_err());
        let mut longer = encoded.to_vec();
        longer.push(0);
        assert!(CustodyReceipt::decode(&longer).is_err());
        // The name binds the ledger, the object and the copy.
        let ledger = original.ledger;
        assert_ne!(
            CustodyReceipt::name(ledger, original.root, 2),
            CustodyReceipt::name(ledger, original.root, 3)
        );
        assert_ne!(
            CustodyReceipt::name(ledger, original.root, 2),
            CustodyReceipt::name(ledger, ContentHash([8; 32]), 2)
        );
    }

    #[test]
    fn a_store_keeps_one_receipt_per_copy_across_reopen_and_refuses_a_corrupt_one() {
        let directory = tempfile::tempdir().unwrap();
        let limits = StoreLimits {
            max_content_bytes: 4096,
            max_staging_bytes: 8192,
            max_uploads: 2,
            chunk_bytes: 4096,
            max_manifest_bytes: 4096,
        };
        let first = receipt(2, 5);
        {
            let mut store = ContentStore::open(directory.path(), limits.clone()).unwrap();
            assert!(
                store
                    .custody_receipt(first.ledger, first.root, 2)
                    .unwrap()
                    .is_none()
            );
            store.record_custody_receipt(&first).unwrap();
            store.record_custody_receipt(&first).unwrap();
            assert_eq!(
                store.custody_receipt(first.ledger, first.root, 2).unwrap(),
                Some(first)
            );
            // A newer scope for the same copy replaces the receipt.
            let newer = receipt(2, 6);
            store.record_custody_receipt(&newer).unwrap();
            assert_eq!(
                store.custody_receipt(first.ledger, first.root, 2).unwrap(),
                Some(newer)
            );
            assert!(
                store
                    .custody_receipt(first.ledger, first.root, 3)
                    .unwrap()
                    .is_none()
            );
        }
        let store = ContentStore::open(directory.path(), limits).unwrap();
        let held = store
            .custody_receipt(first.ledger, first.root, 2)
            .unwrap()
            .unwrap();
        assert_eq!(held.policy_revision, 6);
        let name = CustodyReceipt::name(first.ledger, first.root, 2);
        let path = directory
            .path()
            .join("receipts")
            .join(format!("{name}.record"));
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[40] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();
        assert!(store.custody_receipt(first.ledger, first.root, 2).is_err());
    }
}
