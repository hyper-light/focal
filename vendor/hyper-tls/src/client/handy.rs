use pki_types::ServerName;

use crate::enums::SignatureScheme;
use crate::msgs::persist;
use crate::{client, sign, NamedGroup};

/// An implementer of `ClientSessionStore` which does nothing.
#[derive(Debug)]
pub(super) struct NoClientSessionStorage;

impl client::ClientSessionStore for NoClientSessionStorage {
    fn set_kx_hint(&mut self, _: ServerName<'static>, _: NamedGroup) {}

    fn kx_hint(&self, _: &ServerName<'_>) -> Option<NamedGroup> {
        None
    }

    fn set_tls12_session(&mut self, _: ServerName<'static>, _: persist::Tls12ClientSessionValue) {}

    fn tls12_session(
        &mut self,
        _: &ServerName<'static>,
    ) -> Option<&persist::Tls12ClientSessionValue> {
        None
    }

    fn lent_tls12_session(
        &self,
        _: &ServerName<'static>,
        _: persist::SessionStamp,
    ) -> Option<&persist::Tls12ClientSessionValue> {
        None
    }

    fn current_tls12_session(
        &mut self,
        _: &ServerName<'static>,
        _: persist::SessionStamp,
    ) -> Option<&mut persist::Tls12ClientSessionValue> {
        None
    }

    fn remove_tls12_session(&mut self, _: &ServerName<'_>) {}

    fn insert_tls13_ticket(&mut self, _: ServerName<'static>, _: persist::Tls13ClientSessionValue) {
    }

    fn take_tls13_ticket(
        &mut self,
        _: &ServerName<'_>,
    ) -> Option<persist::Tls13ClientSessionValue> {
        None
    }
}

mod cache {
    use alloc::collections::VecDeque;
    use core::fmt;

    use pki_types::ServerName;

    use crate::msgs::persist;
    use crate::{limited_cache, NamedGroup};

    /// Tickets kept per server: upstream rustls's value, kept unchanged.
    const MAX_TLS13_TICKETS_PER_SERVER: usize = 8;

    struct ServerData {
        kx_hint: Option<NamedGroup>,

        // Zero or one TLS1.2 sessions.
        tls12: Option<Tls12Held>,

        // Up to MAX_TLS13_TICKETS_PER_SERVER TLS1.3 tickets, oldest first.
        tls13: VecDeque<persist::Tls13ClientSessionValue>,
    }

    /// A server's TLS1.2 session, and whether the cache has lent it to a ClientHello.
    struct Tls12Held {
        value: persist::Tls12ClientSessionValue,
        lent: bool,
    }

    impl Default for ServerData {
        fn default() -> Self {
            Self {
                kx_hint: None,
                tls12: None,
                tls13: VecDeque::with_capacity(MAX_TLS13_TICKETS_PER_SERVER),
            }
        }
    }

    /// An implementer of `ClientSessionStore` that stores everything
    /// in memory.
    ///
    /// It enforces a limit on the number of entries to bound memory usage.
    ///
    /// It is owned by its [`ClientConfig`](crate::ClientConfig) and changed through `&mut`, so it
    /// needs no lock.
    ///
    /// A TLS1.2 session it has lent to a ClientHello and then displaces (another session saved
    /// for its server, its removal, or its server's eviction) it keeps findable, so that a
    /// connection still waiting for its server's answer finds the session it offered. It keeps
    /// as many such sessions as its bound on servers, oldest pushed out first: its memory for
    /// TLS1.2 sessions is at most twice its servers'.
    pub struct ClientSessionMemoryCache {
        servers: limited_cache::LimitedCache<ServerName<'static>, ServerData>,
        /// Lent TLS1.2 sessions displaced since, oldest first; at most `max_servers`.
        displaced: VecDeque<persist::Tls12ClientSessionValue>,
        max_servers: usize,
    }

    impl ClientSessionMemoryCache {
        /// Make a new ClientSessionMemoryCache.  `size` is the
        /// maximum number of stored sessions.
        pub fn new(size: usize) -> Self {
            let max_servers = size.saturating_add(MAX_TLS13_TICKETS_PER_SERVER - 1)
                / MAX_TLS13_TICKETS_PER_SERVER;
            Self {
                servers: limited_cache::LimitedCache::new(max_servers),
                displaced: VecDeque::with_capacity(max_servers),
                max_servers,
            }
        }

        /// Keeps `held` findable if it was lent, pushing out the oldest kept session at the bound.
        fn displace(&mut self, held: Option<Tls12Held>) {
            let Some(Tls12Held { value, lent: true }) = held else {
                return;
            };
            if self.max_servers == 0 {
                return;
            }
            if self.displaced.len() >= self.max_servers {
                self.displaced.pop_front();
            }
            self.displaced.push_back(value);
        }

        /// Edits `server_name`'s data, inserting it first if it is new; the TLS1.2 session of a
        /// server the insertion evicted is displaced.
        fn edit(&mut self, server_name: ServerName<'static>, edit: impl FnOnce(&mut ServerData)) {
            let evicted = self
                .servers
                .get_or_insert_default_and_edit(server_name, edit);
            self.displace(evicted.and_then(|data| data.tls12));
        }
    }

    impl super::client::ClientSessionStore for ClientSessionMemoryCache {
        fn set_kx_hint(&mut self, server_name: ServerName<'static>, group: NamedGroup) {
            self.edit(server_name, |data| data.kx_hint = Some(group));
        }

        fn kx_hint(&self, server_name: &ServerName<'_>) -> Option<NamedGroup> {
            self.servers.get(server_name).and_then(|sd| sd.kx_hint)
        }

        fn set_tls12_session(
            &mut self,
            server_name: ServerName<'static>,
            value: persist::Tls12ClientSessionValue,
        ) {
            let mut old = None;
            // The name is moved in: upstream cloned it here, an allocation for every session saved.
            self.edit(server_name, |data| {
                old = data.tls12.replace(Tls12Held { value, lent: false })
            });
            self.displace(old);
        }

        fn tls12_session(
            &mut self,
            server_name: &ServerName<'static>,
        ) -> Option<&persist::Tls12ClientSessionValue> {
            let held = self.servers.get_mut(server_name)?.tls12.as_mut()?;
            held.lent = true;
            Some(&held.value)
        }

        fn lent_tls12_session(
            &self,
            server_name: &ServerName<'static>,
            stamp: persist::SessionStamp,
        ) -> Option<&persist::Tls12ClientSessionValue> {
            self.servers
                .get(server_name)
                .and_then(|data| data.tls12.as_ref())
                .map(|held| &held.value)
                .filter(|value| value.stamp() == stamp)
                .or_else(|| self.displaced.iter().find(|value| value.stamp() == stamp))
        }

        fn current_tls12_session(
            &mut self,
            server_name: &ServerName<'static>,
            stamp: persist::SessionStamp,
        ) -> Option<&mut persist::Tls12ClientSessionValue> {
            self.servers
                .get_mut(server_name)?
                .tls12
                .as_mut()
                .map(|held| &mut held.value)
                .filter(|value| value.stamp() == stamp)
        }

        fn remove_tls12_session(&mut self, server_name: &ServerName<'static>) {
            let old = self
                .servers
                .get_mut(server_name)
                .and_then(|data| data.tls12.take());
            self.displace(old);
        }

        fn insert_tls13_ticket(
            &mut self,
            server_name: ServerName<'static>,
            value: persist::Tls13ClientSessionValue,
        ) {
            // The name is moved in: upstream cloned it here, an allocation for every ticket kept.
            self.edit(server_name, |data| {
                if data.tls13.len() == data.tls13.capacity() {
                    data.tls13.pop_front();
                }
                data.tls13.push_back(value);
            });
        }

        fn take_tls13_ticket(
            &mut self,
            server_name: &ServerName<'static>,
        ) -> Option<persist::Tls13ClientSessionValue> {
            self.servers
                .get_mut(server_name)
                .and_then(|data| data.tls13.pop_back())
        }
    }

    impl fmt::Debug for ClientSessionMemoryCache {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            // Note: we omit self.servers as it may contain sensitive data.
            f.debug_struct("ClientSessionMemoryCache").finish()
        }
    }
}

pub use cache::ClientSessionMemoryCache;

#[derive(Debug)]
pub(super) struct FailResolveClientCert {}

impl client::ResolvesClientCert for FailResolveClientCert {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<&sign::CertifiedKey> {
        None
    }

    fn has_certs(&self) -> bool {
        false
    }
}

/// An exemplar `ResolvesClientCert` implementation that always resolves to a single
/// [RFC 7250] raw public key.
///
/// [RFC 7250]: https://tools.ietf.org/html/rfc7250
#[derive(Debug)]
pub struct AlwaysResolvesClientRawPublicKeys(sign::CertifiedKey);
impl AlwaysResolvesClientRawPublicKeys {
    /// Create a new `AlwaysResolvesClientRawPublicKeys` instance.
    pub fn new(certified_key: sign::CertifiedKey) -> Self {
        Self(certified_key)
    }
}

impl client::ResolvesClientCert for AlwaysResolvesClientRawPublicKeys {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<&sign::CertifiedKey> {
        Some(&self.0)
    }

    fn only_raw_public_keys(&self) -> bool {
        true
    }

    /// Returns true if the resolver is ready to present an identity.
    ///
    /// Even though the function is called `has_certs`, it returns true
    /// although only an RPK (Raw Public Key) is available, not an actual certificate.
    fn has_certs(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[macro_rules_attribute::apply(test_for_each_provider)]
mod tests {
    use std::prelude::v1::*;

    use pki_types::{ServerName, UnixTime};

    use super::provider::cipher_suite;
    use super::{ClientSessionMemoryCache, NoClientSessionStorage};
    use crate::client::ClientSessionStore;
    use crate::identity::Identity;
    use crate::msgs::base::PayloadU16;
    use crate::msgs::enums::NamedGroup;
    use crate::msgs::handshake::CertificateChain;
    use crate::msgs::handshake::SessionId;
    use crate::msgs::persist::Tls13ClientSessionValue;
    use crate::suites::SupportedCipherSuite;

    fn tls12_session() -> crate::msgs::persist::Tls12ClientSessionValue {
        let SupportedCipherSuite::Tls12(suite) =
            cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
        else {
            unreachable!()
        };
        crate::msgs::persist::Tls12ClientSessionValue::new(
            suite,
            SessionId::empty(),
            PayloadU16::empty(),
            &[],
            CertificateChain::default(),
            Identity::fresh(),
            Identity::fresh(),
            UnixTime::now(),
            0,
            true,
        )
    }

    #[test]
    fn test_lent_tls12_session_outlives_its_servers_eviction() {
        // Sixteen sessions: a bound of two servers, of which the cache holds one.
        let mut c = ClientSessionMemoryCache::new(16);
        let a = ServerName::try_from("a.example").unwrap();
        c.set_tls12_session(a.clone(), tls12_session());
        let stamp = c.tls12_session(&a).unwrap().stamp();
        for name in ["b.example", "c.example", "d.example"] {
            c.set_kx_hint(ServerName::try_from(name).unwrap(), NamedGroup::X25519);
        }
        assert!(c.current_tls12_session(&a, stamp).is_none());
        assert_eq!(
            c.lent_tls12_session(&a, stamp).map(|v| v.stamp()),
            Some(stamp)
        );
    }

    #[test]
    fn test_unlent_tls12_session_is_not_kept_once_displaced() {
        let mut c = ClientSessionMemoryCache::new(16);
        let a = ServerName::try_from("a.example").unwrap();
        let first = tls12_session();
        let stamp = first.stamp();
        c.set_tls12_session(a.clone(), first);
        c.set_tls12_session(a.clone(), tls12_session());
        assert!(c.lent_tls12_session(&a, stamp).is_none());
    }

    #[test]
    fn test_current_tls12_session_is_only_the_current_stamp() {
        let mut c = ClientSessionMemoryCache::new(16);
        let a = ServerName::try_from("a.example").unwrap();
        c.set_tls12_session(a.clone(), tls12_session());
        let first = c.tls12_session(&a).unwrap().stamp();
        c.set_tls12_session(a.clone(), tls12_session());
        let second = c.tls12_session(&a).unwrap().stamp();
        assert!(c.current_tls12_session(&a, first).is_none());
        assert!(c.lent_tls12_session(&a, first).is_some());
        assert!(c.current_tls12_session(&a, second).is_some());
    }

    #[test]
    fn test_noclientsessionstorage_does_nothing() {
        let mut c = NoClientSessionStorage {};
        let name = ServerName::try_from("example.com").unwrap();
        let now = UnixTime::now();
        let server_cert_verifier = Identity::fresh();
        let resolves_client_cert = Identity::fresh();

        c.set_kx_hint(name.clone(), NamedGroup::X25519);
        assert_eq!(None, c.kx_hint(&name));

        {
            use crate::msgs::persist::Tls12ClientSessionValue;
            let SupportedCipherSuite::Tls12(tls12_suite) =
                cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
            else {
                unreachable!()
            };

            c.set_tls12_session(
                name.clone(),
                Tls12ClientSessionValue::new(
                    tls12_suite,
                    SessionId::empty(),
                    PayloadU16::empty(),
                    &[],
                    CertificateChain::default(),
                    server_cert_verifier,
                    resolves_client_cert,
                    now,
                    0,
                    true,
                ),
            );
            assert!(c.tls12_session(&name).is_none());
            c.remove_tls12_session(&name);
        }

        let SupportedCipherSuite::Tls13(tls13_suite) = cipher_suite::TLS13_AES_256_GCM_SHA384
        else {
            unreachable!();
        };
        c.insert_tls13_ticket(
            name.clone(),
            Tls13ClientSessionValue::new(
                tls13_suite,
                PayloadU16::empty(),
                &[],
                CertificateChain::default(),
                server_cert_verifier,
                resolves_client_cert,
                now,
                0,
                0,
                0,
            ),
        );
        assert!(c.take_tls13_ticket(&name).is_none());
    }
}
