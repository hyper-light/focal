//! Storing tokens sent from servers in NEW_TOKEN frames and using them in subsequent connections

use std::collections::{HashMap, VecDeque};

use bytes::Bytes;
use lru_slab::LruSlab;
use tracing::trace;

use crate::token::TokenStore;

/// `TokenStore` implementation that stores up to `N` tokens per server name for up to a
/// limited number of server names, in-memory
#[derive(Debug)]
pub struct TokenMemoryCache(State);

impl TokenMemoryCache {
    /// Construct empty
    pub fn new(max_server_names: u32, max_tokens_per_server: usize) -> Self {
        Self(State::new(max_server_names, max_tokens_per_server))
    }
}

impl TokenStore for TokenMemoryCache {
    fn insert(&mut self, server_name: &str, token: Bytes) {
        trace!(%server_name, "storing token");
        self.0.store(server_name, token)
    }

    fn take(&mut self, server_name: &str) -> Option<Bytes> {
        let token = self.0.take(server_name);
        trace!(%server_name, found=%token.is_some(), "taking token");
        token
    }
}

/// Defaults to a maximum of 256 servers and 2 tokens per server
impl Default for TokenMemoryCache {
    fn default() -> Self {
        Self::new(256, 2)
    }
}

/// Inner state of `TokenMemoryCache`
#[derive(Debug)]
struct State {
    max_server_names: u32,
    max_tokens_per_server: usize,
    // map from server name to index in lru; the map is the name's only owner
    lookup: HashMap<Box<str>, u32>,
    lru: LruSlab<CacheEntry>,
}

impl State {
    fn new(max_server_names: u32, max_tokens_per_server: usize) -> Self {
        Self {
            max_server_names,
            max_tokens_per_server,
            lookup: HashMap::new(),
            lru: LruSlab::default(),
        }
    }

    fn store(&mut self, server_name: &str, token: Bytes) {
        if self.max_server_names == 0 {
            // the rest of this method assumes that we can always insert a new entry so long as
            // we're willing to evict a pre-existing entry. thus, an entry limit of 0 is an edge
            // case we must short-circuit on now.
            return;
        }
        if self.max_tokens_per_server == 0 {
            // similarly to above, the rest of this method assumes that we can always push a new
            // token to a queue so long as we're willing to evict a pre-existing token, so we
            // short-circuit on the edge case of a token limit of 0.
            return;
        }

        if let Some(&slot) = self.lookup.get(server_name) {
            // key already exists, push the new token to its token queue
            let tokens = &mut self.lru.get_mut(slot).tokens;
            if tokens.len() >= self.max_tokens_per_server {
                tokens.pop_front();
            }
            tokens.push_back(token);
            return;
        }

        // key does not yet exist, create a new one, evicting the oldest if necessary
        // max_server_names is > 0, so a full cache has a least recently used entry
        if self.lru.len() >= self.max_server_names
            && let Some(evicted) = self.lru.lru()
        {
            self.lru.remove(evicted);
            // The map is the name's only owner, so the evicted name is found by its slot: a scan
            // of at most `max_server_names` entries, paid only when a new name evicts an old one.
            // Upstream kept a second reference to the name in the entry, through an `Arc<str>`.
            self.lookup.retain(|_, slot| *slot != evicted);
        }

        let slot = self.lru.insert(CacheEntry::new(token));
        self.lookup.insert(Box::from(server_name), slot);
    }

    fn take(&mut self, server_name: &str) -> Option<Bytes> {
        let slab_key = *self.lookup.get(server_name)?;

        // pop from entry's token queue
        let entry = self.lru.get_mut(slab_key);
        // An entry's tokens are never left empty
        let token = entry.tokens.pop_front();

        if entry.tokens.is_empty() {
            // token stack emptied, remove entry
            self.lru.remove(slab_key);
            self.lookup.remove(server_name);
        }

        token
    }
}

/// Cache entry within `TokenMemoryCache`'s LRU slab
#[derive(Debug)]
struct CacheEntry {
    // invariant: tokens is never empty
    tokens: VecDeque<Bytes>,
}

impl CacheEntry {
    /// Construct with a single token
    fn new(token: Bytes) -> Self {
        let mut tokens = VecDeque::new();
        tokens.push_back(token);
        Self { tokens }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use rand::prelude::*;
    use rand_pcg::Pcg32;

    fn new_rng() -> impl Rng {
        Pcg32::new(0xdeadbeefdeadbeef, 0xdeadbeefdeadbeef)
    }

    #[test]
    fn cache_test() {
        let mut rng = new_rng();
        const N: usize = 2;

        for _ in 0..10 {
            let mut cache_1: Vec<(u32, VecDeque<Bytes>)> = Vec::new(); // keep it sorted oldest to newest
            let mut cache_2 = TokenMemoryCache::new(20, 2);

            for i in 0..200 {
                let server_name = rng.random::<u32>() % 10;
                if rng.random_bool(0.666) {
                    // store
                    let token = Bytes::from(vec![i]);
                    println!("STORE {server_name} {token:?}");
                    if let Some((j, _)) = cache_1
                        .iter()
                        .enumerate()
                        .find(|&(_, &(server_name_2, _))| server_name_2 == server_name)
                    {
                        let (_, mut queue) = cache_1.remove(j);
                        queue.push_back(token.clone());
                        if queue.len() > N {
                            queue.pop_front();
                        }
                        cache_1.push((server_name, queue));
                    } else {
                        let mut queue = VecDeque::new();
                        queue.push_back(token.clone());
                        cache_1.push((server_name, queue));
                        if cache_1.len() > 20 {
                            cache_1.remove(0);
                        }
                    }
                    cache_2.insert(&server_name.to_string(), token);
                } else {
                    // take
                    println!("TAKE {server_name}");
                    let expecting = cache_1
                        .iter()
                        .enumerate()
                        .find(|&(_, &(server_name_2, _))| server_name_2 == server_name)
                        .map(|(j, _)| j)
                        .map(|j| {
                            let (_, mut queue) = cache_1.remove(j);
                            let token = queue.pop_front().unwrap();
                            if !queue.is_empty() {
                                cache_1.push((server_name, queue));
                            }
                            token
                        });
                    println!("EXPECTING {expecting:?}");
                    assert_eq!(cache_2.take(&server_name.to_string()), expecting);
                }
            }
        }
    }

    #[test]
    fn zero_max_server_names() {
        // test that this edge case doesn't panic
        let mut cache = TokenMemoryCache::new(0, 2);
        for i in 0..10 {
            cache.insert(&i.to_string(), Bytes::from(vec![i]));
            for j in 0..10 {
                assert!(cache.take(&j.to_string()).is_none());
            }
        }
    }

    #[test]
    fn zero_queue_length() {
        // test that this edge case doesn't panic
        let mut cache = TokenMemoryCache::new(256, 0);
        for i in 0..10 {
            cache.insert(&i.to_string(), Bytes::from(vec![i]));
            for j in 0..10 {
                assert!(cache.take(&j.to_string()).is_none());
            }
        }
    }
}
