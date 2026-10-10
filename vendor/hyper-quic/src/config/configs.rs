//! The configurations an endpoint and its connections share, owned by the endpoint
//!
//! quinn-proto shared each configuration among an endpoint and its connections by `Arc`. Here the
//! endpoint owns them in a bounded slab (docs/transport.md §3.1). A connection or a pending
//! incoming attempt holds a generation-checked key, and each call that needs the configuration is
//! lent it by the endpoint.
//!
//! The endpoint counts the connections and incoming attempts on each slot. That count is
//! bookkeeping, not ownership: it rises when a connection or attempt is created and falls on the
//! connection's `Drained` event or when the attempt is accepted, refused, retried or ignored. A
//! configuration that has been superseded (replaced as the server configuration, or retired by the
//! application) is reclaimed once its count reaches zero, so a rotation costs memory only while
//! the connections started under the old configuration live.

use std::num::NonZeroUsize;

use thiserror::Error;

use super::{ClientConfig, ServerConfig};
use crate::crypto::SessionConfig;

/// The number of configuration slots an endpoint has unless configured otherwise
///
/// Derived, not measured: an endpoint holds at most one current configuration per side (server and
/// client) and, during a rotation, the one it replaced, whose connections are still draining.
/// Rotation is driven by certificate renewal, measured in days (Let's Encrypt's short-lived
/// certificates last 6 days; CA/B Forum ballot SC-081 caps lifetimes at 47 days by 2029), far
/// longer than a connection's idle timeout, so a third generation does not overlap the first.
/// Two sides × two generations = 4. An application that keeps more configurations alive at once
/// raises it through [`EndpointConfig::config_slots`](super::EndpointConfig::config_slots).
pub(crate) const DEFAULT_CONFIG_SLOTS: NonZeroUsize = NonZeroUsize::MIN.saturating_add(3);

/// The configurations an endpoint and its connections share
///
/// Owned by an [`Endpoint`](crate::Endpoint); see the module documentation for the lifecycle of a
/// slot.
pub struct Configs {
    slots: Vec<Slot>,
    capacity: NonZeroUsize,
}

struct Slot {
    /// Distinguishes this slot's current occupant from every earlier one
    generation: u32,
    state: SlotState,
}

enum SlotState {
    Vacant,
    /// Boxed: a configuration is large and slots are few and rarely change.
    Occupied(Box<Occupant>),
    /// The generation counter is exhausted; the slot is never reused, so a stale key can never
    /// alias a later occupant
    Spent,
}

struct Occupant {
    config: Shared,
    /// Connections and pending incoming attempts started under this configuration
    users: usize,
    /// Whether the configuration has been replaced or retired, so that it is reclaimed once
    /// `users` reaches zero
    superseded: bool,
}

enum Shared {
    Server(ServerConfig),
    Client(ClientConfig),
}

/// A generation-checked reference to a slot in [`Configs`]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConfigKey {
    index: usize,
    generation: u32,
}

/// A handle to a client configuration owned by an endpoint's [`Configs`]
///
/// Returned by [`Endpoint::insert_client_config`](crate::Endpoint::insert_client_config) and
/// passed to [`Endpoint::connect`](crate::Endpoint::connect). Connections made with the same
/// handle share the configuration, and so its TLS session cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientConfigHandle(pub(crate) ConfigKey);

/// A handle to a server configuration owned by an endpoint's [`Configs`]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServerConfigHandle(pub(crate) ConfigKey);

/// Every configuration slot is occupied by a configuration still in use
///
/// A typed refusal of the new configuration. A superseded configuration's slot is freed when its
/// last connection drains; [`EndpointConfig::config_slots`](super::EndpointConfig::config_slots)
/// sets the bound.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("every configuration slot is in use")]
pub struct ConfigsFull;

impl Configs {
    /// An empty slab of `capacity` slots
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            slots: Vec::new(),
            capacity,
        }
    }

    /// The number of configurations held, current or superseded
    pub fn len(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| matches!(slot.state, SlotState::Occupied(_)))
            .count()
    }

    /// Whether no configuration is held
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The client configuration `handle` refers to, if it is still held
    pub fn client_config(&self, handle: ClientConfigHandle) -> Option<&ClientConfig> {
        match &self.occupant(handle.0)?.config {
            Shared::Client(config) => Some(config),
            Shared::Server(_) => None,
        }
    }

    /// The client configuration `handle` refers to, mutably, if it is still held
    ///
    /// A change applies to connections made with the handle from then on, and to the TLS steps
    /// of connections already made with it.
    pub fn client_config_mut(&mut self, handle: ClientConfigHandle) -> Option<&mut ClientConfig> {
        match &mut self.occupant_mut(handle.0)?.config {
            Shared::Client(config) => Some(config),
            Shared::Server(_) => None,
        }
    }

    /// The server configuration `handle` refers to, if it is still held
    pub fn server_config(&self, handle: ServerConfigHandle) -> Option<&ServerConfig> {
        match &self.occupant(handle.0)?.config {
            Shared::Server(config) => Some(config),
            Shared::Client(_) => None,
        }
    }

    /// The server configuration `handle` refers to, mutably, if it is still held
    #[cfg(test)]
    pub(crate) fn server_config_mut(
        &mut self,
        handle: ServerConfigHandle,
    ) -> Option<&mut ServerConfig> {
        match &mut self.occupant_mut(handle.0)?.config {
            Shared::Server(config) => Some(config),
            Shared::Client(_) => None,
        }
    }

    pub(crate) fn insert_server(
        &mut self,
        config: ServerConfig,
    ) -> Result<ServerConfigHandle, ConfigsFull> {
        self.insert(Shared::Server(config)).map(ServerConfigHandle)
    }

    pub(crate) fn insert_client(
        &mut self,
        config: ClientConfig,
    ) -> Result<ClientConfigHandle, ConfigsFull> {
        self.insert(Shared::Client(config)).map(ClientConfigHandle)
    }

    /// The client configuration `handle` refers to, if new connections may still use it: held
    /// and not retired
    pub(crate) fn client_for_connect(
        &mut self,
        handle: ClientConfigHandle,
    ) -> Option<&mut ClientConfig> {
        let occupant = self.occupant_mut(handle.0)?;
        if occupant.superseded {
            return None;
        }
        match &mut occupant.config {
            Shared::Client(config) => Some(config),
            Shared::Server(_) => None,
        }
    }

    /// The crypto configuration of the slot `key` refers to, lent for one TLS step
    pub(crate) fn session_config(&mut self, key: ConfigKey) -> Option<SessionConfig<'_>> {
        Some(match &mut self.occupant_mut(key)?.config {
            Shared::Client(config) => SessionConfig::Client(&mut *config.crypto),
            Shared::Server(config) => SessionConfig::Server(&mut *config.crypto),
        })
    }

    /// Count one more connection or incoming attempt on the slot `key` refers to
    ///
    /// Returns `false`, counting nothing, when the slot no longer holds that configuration or the
    /// count is at `usize::MAX`.
    pub(crate) fn acquire(&mut self, key: ConfigKey) -> bool {
        let Some(occupant) = self.occupant_mut(key) else {
            return false;
        };
        match occupant.users.checked_add(1) {
            Some(users) => {
                occupant.users = users;
                true
            }
            None => false,
        }
    }

    /// Count one connection or incoming attempt fewer, reclaiming the slot if it was superseded
    /// and this was its last user
    pub(crate) fn release(&mut self, key: ConfigKey) {
        let Some(occupant) = self.occupant_mut(key) else {
            return;
        };
        occupant.users = occupant.users.saturating_sub(1);
        self.reclaim_if_unused(key);
    }

    /// Mark the configuration `key` refers to as replaced, reclaiming its slot now if nothing
    /// uses it
    pub(crate) fn supersede(&mut self, key: ConfigKey) {
        let Some(occupant) = self.occupant_mut(key) else {
            return;
        };
        occupant.superseded = true;
        self.reclaim_if_unused(key);
    }

    fn insert(&mut self, config: Shared) -> Result<ConfigKey, ConfigsFull> {
        let occupant = Occupant {
            config,
            users: 0,
            superseded: false,
        };
        if let Some((index, slot)) = self
            .slots
            .iter_mut()
            .enumerate()
            .find(|(_, slot)| matches!(slot.state, SlotState::Vacant))
        {
            slot.state = SlotState::Occupied(Box::new(occupant));
            return Ok(ConfigKey {
                index,
                generation: slot.generation,
            });
        }
        if self.slots.len() >= self.capacity.get() {
            return Err(ConfigsFull);
        }
        let index = self.slots.len();
        self.slots.push(Slot {
            generation: 0,
            state: SlotState::Occupied(Box::new(occupant)),
        });
        Ok(ConfigKey {
            index,
            generation: 0,
        })
    }

    fn reclaim_if_unused(&mut self, key: ConfigKey) {
        let Some(slot) = self.slots.get_mut(key.index) else {
            return;
        };
        if slot.generation != key.generation {
            return;
        }
        let unused = matches!(
            &slot.state,
            SlotState::Occupied(occupant) if occupant.superseded && occupant.users == 0
        );
        if !unused {
            return;
        }
        match slot.generation.checked_add(1) {
            Some(generation) => {
                slot.generation = generation;
                slot.state = SlotState::Vacant;
            }
            None => slot.state = SlotState::Spent,
        }
    }

    fn occupant(&self, key: ConfigKey) -> Option<&Occupant> {
        let slot = self.slots.get(key.index)?;
        match &slot.state {
            SlotState::Occupied(occupant) if slot.generation == key.generation => Some(occupant),
            _ => None,
        }
    }

    fn occupant_mut(&mut self, key: ConfigKey) -> Option<&mut Occupant> {
        let slot = self.slots.get_mut(key.index)?;
        match &mut slot.state {
            SlotState::Occupied(occupant) if slot.generation == key.generation => Some(occupant),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> ClientConfig {
        crate::tests::util::client_config()
    }

    fn slots(n: usize) -> Configs {
        Configs::new(NonZeroUsize::new(n).unwrap())
    }

    #[test]
    fn full_slab_refuses() {
        let mut configs = slots(2);
        configs.insert_client(client()).unwrap();
        configs.insert_client(client()).unwrap();
        assert_eq!(configs.insert_client(client()).err(), Some(ConfigsFull));
        assert_eq!(configs.len(), 2);
    }

    #[test]
    fn superseded_slot_is_reclaimed_when_its_last_user_releases() {
        let mut configs = slots(1);
        let old = configs.insert_client(client()).unwrap();
        assert!(configs.acquire(old.0));
        configs.supersede(old.0);
        // Still in use: kept, and the slab is still full
        assert!(configs.client_config(old).is_some());
        assert_eq!(configs.insert_client(client()).err(), Some(ConfigsFull));

        configs.release(old.0);
        assert!(configs.client_config(old).is_none());
        let new = configs.insert_client(client()).unwrap();
        // The reused slot does not answer to the old handle
        assert_ne!(new, old);
        assert!(configs.client_config(old).is_none());
        assert!(configs.client_config(new).is_some());
    }

    #[test]
    fn unused_superseded_slot_is_reclaimed_at_once() {
        let mut configs = slots(1);
        let old = configs.insert_client(client()).unwrap();
        configs.supersede(old.0);
        assert!(configs.is_empty());
        assert!(!configs.acquire(old.0));
    }

    #[test]
    fn current_slot_is_kept_without_users() {
        let mut configs = slots(1);
        let current = configs.insert_client(client()).unwrap();
        assert!(configs.acquire(current.0));
        configs.release(current.0);
        assert!(configs.client_config(current).is_some());
    }
}
