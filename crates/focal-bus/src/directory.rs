//! Which peer, in which role, a connection's certificate names.
//!
//! The bus admits only identities it was given: each enrolled certificate (DER) names one peer and
//! its role, and a handshake whose certificate the directory does not hold is refused
//! (`Refusal::Identity`) and charged to no identity. A node dialing another checks the name the
//! directory states for it. The directory is bounded by what its owner enrolls, at most `limit`
//! identities, refused past it.

use std::collections::HashMap;

use hyper_transport::{Authenticated, Directory, PeerId};

use crate::BusError;
use crate::wire::Role;

/// The identities a bus endpoint admits.
#[derive(Debug)]
pub struct BusDirectory {
    by_certificate: HashMap<Vec<u8>, (PeerId, Role)>,
    names: HashMap<PeerId, String>,
    limit: usize,
}

impl BusDirectory {
    /// A directory of at most `limit` identities.
    pub fn new(limit: usize) -> Self {
        Self {
            by_certificate: HashMap::new(),
            names: HashMap::new(),
            limit,
        }
    }

    /// Enrolls `certificate` (DER) as `peer` in `role`, dialed by `name` when this side dials it.
    pub fn enroll(
        &mut self,
        certificate: &[u8],
        peer: PeerId,
        role: Role,
        name: &str,
    ) -> Result<(), BusError> {
        if !self.by_certificate.contains_key(certificate) && self.by_certificate.len() >= self.limit
        {
            return Err(BusError::Capacity("identities in the directory"));
        }
        self.by_certificate
            .insert(certificate.to_vec(), (peer, role));
        self.names.insert(peer, name.to_owned());
        Ok(())
    }

    /// Identities enrolled.
    pub fn len(&self) -> usize {
        self.by_certificate.len()
    }

    /// Whether no identity is enrolled.
    pub fn is_empty(&self) -> bool {
        self.by_certificate.is_empty()
    }
}

impl Directory for BusDirectory {
    type Role = Role;

    fn identify(&mut self, authenticated: &Authenticated<'_>) -> Option<(PeerId, Role)> {
        let named = self
            .by_certificate
            .get(authenticated.certificate?)
            .copied()?;
        // A dialed connection is admitted only as the peer dialed.
        match authenticated.dialed {
            Some(dialed) if dialed != named.0 => None,
            _ => Some(named),
        }
    }

    fn server_name(&self, peer: PeerId) -> Option<&str> {
        self.names.get(&peer).map(String::as_str)
    }
}
