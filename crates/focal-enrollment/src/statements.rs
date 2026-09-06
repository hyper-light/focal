//! Portable proof of possession for bounded directory authority statements.
use crate::*;
use rcgen::{KeyPair, SigningKey};
use serde::{Deserialize, Serialize};
use x509_parser::prelude::{FromDer, X509Certificate};

const DOMAIN: &[u8] = b"focal.directory.node-statement.v1\0";
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedNodeStatement {
    pub cluster: ClusterId,
    pub certificate: Vec<u8>,
    pub statement_hash: Fingerprint,
    pub signature: Vec<u8>,
}
impl CredentialMaterial {
    /// Signing proves this key made a statement; it does not itself prove any
    /// topology, committed prefix or membership fact. The directory verifier
    /// additionally requires installed authority assignments and exact scope.
    pub fn sign_node_statement(
        &self,
        cluster: ClusterId,
        statement: &[u8],
    ) -> Result<SignedNodeStatement, EnrollmentError> {
        if cluster == [0; 16] || statement.len() > MAX_MESSAGE_BYTES {
            return Err(EnrollmentError::Capacity);
        }
        let certificate = self
            .certificate_chain()
            .first()
            .ok_or(EnrollmentError::Invalid)?
            .clone();
        if certificate.len() > 4096 {
            return Err(EnrollmentError::Capacity);
        }
        let statement_hash = hash("focal.directory.statement-payload.v1", statement);
        let message = signed_bytes(cluster, server_fingerprint(&certificate), statement_hash);
        let key = KeyPair::try_from(self.private_key_der().as_slice())?;
        let signature = key.sign(&message)?;
        Ok(SignedNodeStatement {
            cluster,
            certificate,
            statement_hash,
            signature,
        })
    }
}
impl EnrollmentRegistry {
    /// Uses the owning registry's committed certificate/revocation table. The
    /// resulting identity has Node role only; client and bootstrap TLS server
    /// certificates cannot attest infrastructure or log authority statements.
    pub fn verify_node_statement(
        &self,
        proof: &SignedNodeStatement,
        statement: &[u8],
        now: i64,
    ) -> Result<AssignedIdentity, EnrollmentError> {
        if proof.cluster != self.cluster() {
            return Err(EnrollmentError::WrongCluster);
        }
        if statement.len() > MAX_MESSAGE_BYTES
            || proof.certificate.len() > 4096
            || proof.signature.len() > 80
        {
            return Err(EnrollmentError::Capacity);
        }
        if proof.statement_hash != hash("focal.directory.statement-payload.v1", statement) {
            return Err(EnrollmentError::Unauthorized);
        }
        let identity = self.authorize_certificate(&proof.certificate, now)?;
        if identity.role != EnrollmentRole::Node || identity.node_id.is_none() {
            return Err(EnrollmentError::Unauthorized);
        }
        let (rest, certificate) =
            X509Certificate::from_der(&proof.certificate).map_err(|_| EnrollmentError::Invalid)?;
        if !rest.is_empty() {
            return Err(EnrollmentError::Invalid);
        }
        let message = signed_bytes(
            proof.cluster,
            server_fingerprint(&proof.certificate),
            proof.statement_hash,
        );
        ring::signature::UnparsedPublicKey::new(
            &ring::signature::ECDSA_P256_SHA256_ASN1,
            certificate.public_key().subject_public_key.data.as_ref(),
        )
        .verify(&message, &proof.signature)
        .map_err(|_| EnrollmentError::Unauthorized)?;
        Ok(identity)
    }
}
fn signed_bytes(cluster: ClusterId, certificate: Fingerprint, statement: Fingerprint) -> Vec<u8> {
    let mut bytes = DOMAIN.to_vec();
    bytes.extend_from_slice(&cluster);
    bytes.extend_from_slice(&certificate);
    bytes.extend_from_slice(&statement);
    bytes
}
