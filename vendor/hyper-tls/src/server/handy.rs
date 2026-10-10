use alloc::vec::Vec;
use core::fmt::Debug;

use crate::server::ClientHello;
use crate::{server, sign};

/// Something which never stores sessions.
#[derive(Debug)]
pub struct NoServerSessionStorage {}

impl server::StoresServerSessions for NoServerSessionStorage {
    fn put(&mut self, _id: Vec<u8>, _sec: Vec<u8>) -> bool {
        false
    }
    fn get(&self, _id: &[u8]) -> Option<Vec<u8>> {
        None
    }
    fn take(&mut self, _id: &[u8]) -> Option<Vec<u8>> {
        None
    }
    fn can_cache(&self) -> bool {
        false
    }
}

mod cache {
    use alloc::boxed::Box;
    use alloc::vec::Vec;
    use core::fmt::{Debug, Formatter};

    use crate::{limited_cache, server};

    /// An implementer of `StoresServerSessions` that stores everything
    /// in memory.  If enforces a limit on the number of stored sessions
    /// to bound memory usage.
    ///
    /// It is owned by its [`ServerConfig`](crate::ServerConfig) and changed through `&mut`, so it
    /// needs no lock.
    pub struct ServerSessionMemoryCache {
        cache: limited_cache::LimitedCache<Vec<u8>, Vec<u8>>,
    }

    impl ServerSessionMemoryCache {
        /// Make a new ServerSessionMemoryCache.  `size` is the maximum
        /// number of stored sessions, and may be rounded-up for
        /// efficiency.
        ///
        /// It is boxed, as [`ServerConfig::session_storage`](crate::ServerConfig::session_storage)
        /// holds it.
        pub fn new(size: usize) -> Box<Self> {
            Box::new(Self {
                cache: limited_cache::LimitedCache::new(size),
            })
        }
    }

    impl server::StoresServerSessions for ServerSessionMemoryCache {
        fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> bool {
            self.cache.insert(key, value);
            true
        }

        fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
            self.cache.get(key).cloned()
        }

        fn take(&mut self, key: &[u8]) -> Option<Vec<u8>> {
            self.cache.remove(key)
        }

        fn can_cache(&self) -> bool {
            true
        }
    }

    impl Debug for ServerSessionMemoryCache {
        fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
            f.debug_struct("ServerSessionMemoryCache").finish()
        }
    }

    #[cfg(test)]
    mod tests {
        use std::vec;

        use super::*;
        use crate::server::StoresServerSessions;

        #[test]
        fn test_serversessionmemorycache_accepts_put() {
            let mut c = ServerSessionMemoryCache::new(4);
            assert!(c.put(vec![0x01], vec![0x02]));
        }

        #[test]
        fn test_serversessionmemorycache_persists_put() {
            let mut c = ServerSessionMemoryCache::new(4);
            assert!(c.put(vec![0x01], vec![0x02]));
            assert_eq!(c.get(&[0x01]), Some(vec![0x02]));
            assert_eq!(c.get(&[0x01]), Some(vec![0x02]));
        }

        #[test]
        fn test_serversessionmemorycache_overwrites_put() {
            let mut c = ServerSessionMemoryCache::new(4);
            assert!(c.put(vec![0x01], vec![0x02]));
            assert!(c.put(vec![0x01], vec![0x04]));
            assert_eq!(c.get(&[0x01]), Some(vec![0x04]));
        }

        #[test]
        fn test_serversessionmemorycache_drops_to_maintain_size_invariant() {
            let mut c = ServerSessionMemoryCache::new(2);
            assert!(c.put(vec![0x01], vec![0x02]));
            assert!(c.put(vec![0x03], vec![0x04]));
            assert!(c.put(vec![0x05], vec![0x06]));
            assert!(c.put(vec![0x07], vec![0x08]));
            assert!(c.put(vec![0x09], vec![0x0a]));

            let count = c.get(&[0x01]).iter().count()
                + c.get(&[0x03]).iter().count()
                + c.get(&[0x05]).iter().count()
                + c.get(&[0x07]).iter().count()
                + c.get(&[0x09]).iter().count();

            assert!(count < 5);
        }
    }
}

pub use cache::ServerSessionMemoryCache;

/// Something which never produces tickets.
#[derive(Debug)]
pub(super) struct NeverProducesTickets {}

impl server::ProducesTickets for NeverProducesTickets {
    fn enabled(&self) -> bool {
        false
    }
    fn lifetime(&self) -> u32 {
        0
    }
    fn encrypt(&mut self, _bytes: &[u8]) -> Option<Vec<u8>> {
        None
    }
    fn decrypt(&mut self, _bytes: &[u8]) -> Option<Vec<u8>> {
        None
    }
}

/// An exemplar `ResolvesServerCert` implementation that always resolves to a single
/// [RFC 7250] raw public key.
///
/// [RFC 7250]: https://tools.ietf.org/html/rfc7250
#[derive(Debug)]
pub struct AlwaysResolvesServerRawPublicKeys(sign::CertifiedKey);

impl AlwaysResolvesServerRawPublicKeys {
    /// Create a new `AlwaysResolvesServerRawPublicKeys` instance.
    pub fn new(certified_key: sign::CertifiedKey) -> Self {
        Self(certified_key)
    }
}

impl server::ResolvesServerCert for AlwaysResolvesServerRawPublicKeys {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<&sign::CertifiedKey> {
        Some(&self.0)
    }

    fn only_raw_public_keys(&self) -> bool {
        true
    }
}

mod sni_resolver {
    use alloc::string::{String, ToString};
    use core::fmt::Debug;

    use pki_types::{DnsName, ServerName};

    use crate::error::Error;
    use crate::hash_map::HashMap;
    use crate::server::ClientHello;
    use crate::webpki::{verify_server_name, ParsedCertificate};
    use crate::{server, sign};

    /// Something that resolves do different cert chains/keys based
    /// on client-supplied server name (via SNI).
    #[derive(Debug)]
    pub struct ResolvesServerCertUsingSni {
        by_name: HashMap<String, sign::CertifiedKey>,
    }

    impl ResolvesServerCertUsingSni {
        /// Create a new and empty (i.e., knows no certificates) resolver.
        pub fn new() -> Self {
            Self {
                by_name: HashMap::new(),
            }
        }

        /// Add a new `sign::CertifiedKey` to be used for the given SNI `name`.
        ///
        /// This function fails if `name` is not a valid DNS name, or if
        /// it's not valid for the supplied certificate, or if the certificate
        /// chain is syntactically faulty.
        pub fn add(&mut self, name: &str, ck: sign::CertifiedKey) -> Result<(), Error> {
            let server_name = {
                let checked_name = DnsName::try_from(name)
                    .map_err(|_| Error::General("Bad DNS name".into()))
                    .map(|name| name.to_lowercase_owned())?;
                ServerName::DnsName(checked_name)
            };

            // Check the certificate chain for validity:
            // - it should be non-empty list
            // - the first certificate should be parsable as a x509v3,
            // - the first certificate should quote the given server name
            //   (if provided)
            //
            // These checks are not security-sensitive.  They are the
            // *server* attempting to detect accidental misconfiguration.

            ck.end_entity_cert()
                .and_then(ParsedCertificate::try_from)
                .and_then(|cert| verify_server_name(&cert, &server_name))?;

            if let ServerName::DnsName(name) = server_name {
                self.by_name.insert(name.as_ref().to_string(), ck);
            }
            Ok(())
        }
    }

    impl server::ResolvesServerCert for ResolvesServerCertUsingSni {
        fn resolve(&self, client_hello: ClientHello<'_>) -> Option<&sign::CertifiedKey> {
            if let Some(name) = client_hello.server_name() {
                self.by_name.get(name)
            } else {
                // This kind of resolver requires SNI
                None
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::server::ResolvesServerCert;

        #[test]
        fn test_resolvesservercertusingsni_requires_sni() {
            let rscsni = ResolvesServerCertUsingSni::new();
            assert!(rscsni
                .resolve(ClientHello {
                    server_name: &None,
                    signature_schemes: &[],
                    alpn: None,
                    server_cert_types: None,
                    client_cert_types: None,
                    cipher_suites: &[],
                    certificate_authorities: None,
                    named_groups: None,
                })
                .is_none());
        }

        #[test]
        fn test_resolvesservercertusingsni_handles_unknown_name() {
            let rscsni = ResolvesServerCertUsingSni::new();
            let name = DnsName::try_from("hello.com").unwrap().to_owned();
            assert!(rscsni
                .resolve(ClientHello {
                    server_name: &Some(name),
                    signature_schemes: &[],
                    alpn: None,
                    server_cert_types: None,
                    client_cert_types: None,
                    cipher_suites: &[],
                    certificate_authorities: None,
                    named_groups: None,
                })
                .is_none());
        }
    }
}

pub use sni_resolver::ResolvesServerCertUsingSni;

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use crate::server::{ProducesTickets, StoresServerSessions};

    #[test]
    fn test_noserversessionstorage_drops_put() {
        let mut c = NoServerSessionStorage {};
        assert!(!c.put(vec![0x01], vec![0x02]));
    }

    #[test]
    fn test_noserversessionstorage_denies_gets() {
        let mut c = NoServerSessionStorage {};
        c.put(vec![0x01], vec![0x02]);
        assert_eq!(c.get(&[]), None);
        assert_eq!(c.get(&[0x01]), None);
        assert_eq!(c.get(&[0x02]), None);
    }

    #[test]
    fn test_noserversessionstorage_denies_takes() {
        let mut c = NoServerSessionStorage {};
        assert_eq!(c.take(&[]), None);
        assert_eq!(c.take(&[0x01]), None);
        assert_eq!(c.take(&[0x02]), None);
    }

    #[test]
    fn test_neverproducestickets_does_nothing() {
        let mut npt = NeverProducesTickets {};
        assert!(!npt.enabled());
        assert_eq!(0, npt.lifetime());
        assert_eq!(None, npt.encrypt(&[]));
        assert_eq!(None, npt.decrypt(&[]));
    }
}
