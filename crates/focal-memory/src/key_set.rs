//! An ordered set of byte keys held front-coded in small blocks (doc 31 M3).
//!
//! What a unit row of a secondary index holds is its key alone. A row store's entry pays a
//! fixed key slot, a value and a heap count for it: 96 bytes for a native index row whose key is
//! 34. Here a key pays the bytes past the prefix it shares with the key before it and two bytes
//! of lengths, and its share of its block's restart table and directory slot. The keys of one
//! index bucket share their affinity and family, so a 34-byte key takes about 18. This is
//! LevelDB's block format, prefix compression with restart points, and the way Redis keeps a
//! stream's entries in listpacks: contiguous and delta-coded, not a node a key.
//!
//! The set is a tree of four levels: the root, held by the set alone, of mids; a mid of chunks; a
//! chunk of blocks; a block of keys. Mids, chunks and blocks are reference counted and bounded,
//! so a frozen image (a checkpoint's, a read's lease) copies the root, a slot a mid, and shares
//! the rest: a publication edits a mid, a chunk or a block in place where the set alone holds it
//! and copies it where an image still reads it (FASTER's division between in-place and
//! read-copy-update, decided by reference counts, as `RangeStore::publish_sparse` decides it for
//! pages). The root is never shared, so no publication copies it, and what one publication may
//! spend is bounded by its edits alone, whatever the set holds: the bound a write's envelope
//! promises before the write is planned.
//!
//! A publication spends only what its plan reserved ([`KeySet::fund`]), from the write's own
//! source: every block, chunk and mid it makes is charged to that funding, its scratch is covered
//! by what the funding holds while it runs, and the root's growth comes from a reservation of its
//! own, which the root keeps beside its other charges. It checks its edits before it changes anything; past that point a
//! failure is an invariant broken, and the set is poisoned and refuses every publication after it.

use crate::{
    ALLOCATOR_OVERHEAD, Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError, checked_add,
    checked_mul,
};
use std::cmp::Ordering;
use std::sync::Arc;

/// The longest key a set holds.
pub const MAX_KEY: usize = 128;
/// Every this-many entries a block stores a key whole: a search finds its place among them by
/// halving and decodes at most this many entries after it.
const RESTART: usize = 16;
/// A block is cut once its entries pass this many bytes...
const BLOCK_TARGET: usize = 1024;
/// ...or hold this many keys. The key bound bounds what an edit can make a block grow by
/// re-encoding it (its restarts shift), whatever the keys are.
const BLOCK_KEYS: usize = 64;
/// A block an edit leaves with fewer bytes of entries than this (and fewer than half a block's
/// keys) is joined with its neighbour.
const BLOCK_MIN: usize = BLOCK_TARGET / 4;
/// A chunk holds at most this many blocks, a mid this many chunks; one past it is cut evenly.
const CHUNK_BLOCKS: usize = 32;
const MID_CHUNKS: usize = 32;
/// The bytes of a child's first key its parent keeps to search by, zero-padded.
const FENCE: usize = 24;
/// The most bytes one entry takes: its two lengths and a whole key.
const ENTRY_MAX: usize = 2 + MAX_KEY;
/// The most restarts a block holds.
const RESTARTS_MAX: usize = BLOCK_KEYS.div_ceil(RESTART);
/// The most bytes a block's trailer takes: its restart offsets and its two counts.
const TRAILER_MAX: usize = 2 * RESTARTS_MAX + 4;
/// The most bytes of entries a block holds: the cut comes after the entry that passes the target.
const BLOCK_ENTRIES_MAX: usize = BLOCK_TARGET + ENTRY_MAX;
/// What a reference-counted allocation adds to its value: its two counts.
const ARC_COUNTS: usize = 2 * size_of::<usize>();
/// A block's fixed charge: its value behind its reference counts, and two allocations.
const BLOCK_FIXED: usize = size_of::<Block>() + ARC_COUNTS + 2 * ALLOCATOR_OVERHEAD;
/// A node's slot for a child: its fence and its pointer.
const CHILD_SLOT: usize = size_of::<Fence>() + size_of::<Arc<Block>>();
/// A node's fixed charge: its value behind its counts and three allocations (itself and its two
/// vectors). Every node is the same size whatever its children are.
const NODE_FIXED: usize = size_of::<Chunk>() + ARC_COUNTS + 3 * ALLOCATOR_OVERHEAD;
/// A root's fixed charge: its value, its two vectors, and its charges' vector at its least room.
const ROOT_FIXED: usize = size_of::<Root>() + 3 * ALLOCATOR_OVERHEAD + 4 * size_of::<Allocation>();
/// What one growth of a root may add beside its slots: a place in its charges' vector, twice
/// over for the vector's doubling.
const GROWTH_FIXED: usize = 2 * size_of::<Allocation>();

type Fence = [u8; FENCE];
/// A child as its parent places it: its first key's fence and the child.
type Seat<C> = (Fence, Arc<C>);
/// Children below a key and from it on.
type Halves<C> = (Vec<Seat<C>>, Vec<Seat<C>>);

fn fence(key: &[u8]) -> Fence {
    let mut fence = [0u8; FENCE];
    for (target, source) in fence.iter_mut().zip(key) {
        *target = *source;
    }
    fence
}

fn common_prefix(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right)
        .take_while(|(left, right)| left == right)
        .count()
}

fn broken(what: &'static str) -> MemoryError {
    MemoryError::InvalidConfiguration(what)
}

/// A key read from a set, held inline: keys are short.
#[derive(Clone, Copy)]
pub struct SetKey {
    len: u8,
    bytes: [u8; MAX_KEY],
}

impl SetKey {
    /// `key` as a set holds it: one to [`MAX_KEY`] bytes.
    pub fn new(key: &[u8]) -> Result<Self, MemoryError> {
        if key.is_empty() {
            return Err(broken("a set key must hold at least one byte"));
        }
        let mut out = Self::empty();
        out.assign(0, key)
            .ok_or(broken("a set key must hold at most MAX_KEY bytes"))?;
        Ok(out)
    }
    const fn empty() -> Self {
        Self {
            len: 0,
            bytes: [0; MAX_KEY],
        }
    }
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.get(..usize::from(self.len)).unwrap_or(&[])
    }
    /// Keeps this key's first `shared` bytes and appends `suffix`; `None`, unchanged, where
    /// `shared` passes the key or the result passes [`MAX_KEY`].
    fn assign(&mut self, shared: usize, suffix: &[u8]) -> Option<()> {
        if shared > usize::from(self.len) {
            return None;
        }
        let end = shared.checked_add(suffix.len())?;
        let len = u8::try_from(end).ok()?;
        self.bytes.get_mut(shared..end)?.copy_from_slice(suffix);
        self.len = len;
        Some(())
    }
}

impl PartialEq for SetKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}
impl Eq for SetKey {}
impl PartialOrd for SetKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SetKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}
impl std::fmt::Debug for SetKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.as_bytes() {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// One edit of a publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyEdit<'k> {
    /// The set holds the key after it (a key it held already stays).
    Put(&'k [u8]),
    /// The set no longer holds the key, which it held.
    Delete(&'k [u8]),
}

impl<'k> KeyEdit<'k> {
    pub fn key(&self) -> &'k [u8] {
        match self {
            Self::Put(key) | Self::Delete(key) => key,
        }
    }
}

/// An edit a set publishes, in whatever form its caller holds it: its key, and whether it
/// deletes the key (a put otherwise).
pub trait SetEdit {
    fn edit_key(&self) -> &[u8];
    fn deletes(&self) -> bool;
}

impl SetEdit for KeyEdit<'_> {
    fn edit_key(&self) -> &[u8] {
        self.key()
    }
    fn deletes(&self) -> bool {
        matches!(self, Self::Delete(_))
    }
}

/// A row store's change of a key held as bytes: a put of any value is a put of its key.
impl<K: AsRef<[u8]>, V> SetEdit for crate::Change<K, V> {
    fn edit_key(&self) -> &[u8] {
        self.key().as_ref()
    }
    fn deletes(&self) -> bool {
        matches!(self, crate::Change::Delete(_))
    }
}

/// What a set holds, by level.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KeySetStats {
    pub keys: usize,
    pub blocks: usize,
    pub chunks: usize,
    pub mids: usize,
    /// The bytes of the blocks' entries and trailers.
    pub block_bytes: usize,
}

fn block_charge(bytes: usize) -> Result<usize, MemoryError> {
    checked_add(bytes, BLOCK_FIXED)
}

fn node_charge(slots: usize) -> Result<usize, MemoryError> {
    checked_add(checked_mul(slots, CHILD_SLOT)?, NODE_FIXED)
}

fn root_charge(slots: usize) -> Result<usize, MemoryError> {
    checked_add(checked_mul(slots, CHILD_SLOT)?, ROOT_FIXED)
}

/// Where a publication or a build takes its charges from.
enum Source<'f> {
    /// Split from a plan's funding, which [`KeySet::fund`] sized.
    Funding(&'f mut Allocation),
    /// Reserved from a budget in a lane: a build, a split or a join.
    Budget(&'f MemoryBudget, BudgetLane),
}

impl Source<'_> {
    fn take(&mut self, bytes: usize) -> Result<Allocation, MemoryError> {
        match self {
            Self::Funding(funding) => funding.split_off(bytes),
            Self::Budget(budget, lane) => {
                Ok(budget.reserve(BudgetKind::Pages, *lane, bytes)?.commit())
            }
        }
    }
}

/// What a tree level's children answer.
trait Child {
    fn first(&self) -> &[u8];
    fn keys(&self) -> usize;
}

struct Block {
    /// The entries, each `[shared][suffix length][suffix]` with every `RESTART`th sharing
    /// nothing, then the offset of each such restart (`u16`, little-endian), the restart count
    /// and the entry count (`u16` each).
    bytes: Vec<u8>,
    _charge: Allocation,
}

fn read_u16(bytes: &[u8], at: usize) -> Option<usize> {
    let pair = bytes.get(at..at.checked_add(2)?)?;
    let mut word = [0u8; 2];
    word.copy_from_slice(pair);
    Some(usize::from(u16::from_le_bytes(word)))
}

/// The order of the key `head` then `tail` against `key`, the two parts never joined.
fn joined_cmp(head: &[u8], tail: &[u8], key: &[u8]) -> Ordering {
    let (key_head, key_tail) = key.split_at(head.len().min(key.len()));
    match head.cmp(key_head) {
        Ordering::Equal if head.len() > key.len() => Ordering::Greater,
        Ordering::Equal => tail.cmp(key_tail),
        unequal => unequal,
    }
}

/// One entry at `offset`: its shared length, its suffix and the offset after it.
fn entry_at(entries: &[u8], offset: usize) -> Option<(usize, &[u8], usize)> {
    let shared = usize::from(*entries.get(offset)?);
    let len = usize::from(*entries.get(offset.checked_add(1)?)?);
    let start = offset.checked_add(2)?;
    let end = start.checked_add(len)?;
    Some((shared, entries.get(start..end)?, end))
}

impl Block {
    fn count(&self) -> usize {
        self.bytes
            .len()
            .checked_sub(2)
            .and_then(|at| read_u16(&self.bytes, at))
            .unwrap_or(0)
    }
    fn restarts(&self) -> usize {
        self.bytes
            .len()
            .checked_sub(4)
            .and_then(|at| read_u16(&self.bytes, at))
            .unwrap_or(0)
    }
    /// Where the entries end and the restart table begins.
    fn entries_end(&self) -> usize {
        self.bytes
            .len()
            .saturating_sub(4)
            .saturating_sub(self.restarts().saturating_mul(2))
    }
    fn entries(&self) -> &[u8] {
        self.bytes.get(..self.entries_end()).unwrap_or(&[])
    }
    fn restart(&self, index: usize) -> Option<usize> {
        read_u16(
            &self.bytes,
            self.entries_end().checked_add(index.checked_mul(2)?)?,
        )
    }
    fn cursor(&self) -> Cursor<'_> {
        self.cursor_at(0)
    }
    fn cursor_at(&self, offset: usize) -> Cursor<'_> {
        Cursor {
            entries: self.entries(),
            offset,
            key: SetKey::empty(),
            pending: false,
        }
    }
    /// A walk from the entry at `offset`, the key before it `previous` (what the entry's shared
    /// prefix is read from).
    fn cursor_from(&self, offset: usize, previous: SetKey) -> Cursor<'_> {
        Cursor {
            entries: self.entries(),
            offset,
            key: previous,
            pending: false,
        }
    }
    /// Where the first key at or after `key` begins: its entry's offset, its index among the
    /// block's keys, and the key before it (empty before the first); the entries' end, the
    /// block's count and its last key where every key is before `key`. Each entry is compared
    /// as its shared prefix and its suffix, built into a key only once it is passed.
    fn before(&self, key: &[u8]) -> Option<(usize, usize, SetKey)> {
        let entries = self.entries();
        // The last restart whose key is before `key`: restarts hold their keys whole.
        let (mut low, mut high) = (0usize, self.restarts());
        while low < high {
            let middle = low.checked_add(high.checked_sub(low)?.checked_div(2)?)?;
            let before = self
                .restart(middle)
                .and_then(|offset| entry_at(entries, offset))
                .is_some_and(|(_, whole, _)| whole < key);
            if before {
                low = middle.checked_add(1)?;
            } else {
                high = middle;
            }
        }
        let (mut offset, mut index) = match low.checked_sub(1) {
            Some(restart) => (self.restart(restart)?, restart.checked_mul(RESTART)?),
            None => (0, 0),
        };
        let mut previous = SetKey::empty();
        // At most a restart interval's entries lie between the restart and the place.
        while offset < entries.len() {
            let (shared, suffix, next) = entry_at(entries, offset)?;
            let head = previous.as_bytes().get(..shared)?;
            if joined_cmp(head, suffix, key) != Ordering::Less {
                break;
            }
            previous.assign(shared, suffix)?;
            offset = next;
            index = index.checked_add(1)?;
        }
        Some((offset, index, previous))
    }
    /// A cursor whose next key is the first at or after `key` (after it, `exclusive`).
    fn seek(&self, key: &[u8], exclusive: bool) -> Cursor<'_> {
        let entries = self.entries();
        // The last restart whose key is at or before `key`: restarts hold their keys whole.
        let (mut low, mut high) = (0usize, self.restarts());
        while low < high {
            let Some(middle) = high
                .checked_sub(low)
                .and_then(|span| low.checked_add(span / 2))
            else {
                break;
            };
            let at_or_before = self
                .restart(middle)
                .and_then(|offset| entry_at(entries, offset))
                .is_some_and(|(_, whole, _)| whole <= key);
            if at_or_before {
                low = middle.saturating_add(1);
            } else {
                high = middle;
            }
        }
        let start = low
            .checked_sub(1)
            .and_then(|restart| self.restart(restart))
            .unwrap_or(0);
        let mut cursor = self.cursor_at(start);
        // At most a restart interval's entries lie between the restart and the key.
        while cursor.advance() {
            let past = if exclusive {
                cursor.key() > key
            } else {
                cursor.key() >= key
            };
            if past {
                cursor.pending = true;
                break;
            }
        }
        cursor
    }
    fn contains(&self, key: &[u8]) -> bool {
        let mut cursor = self.seek(key, false);
        cursor.advance() && cursor.key() == key
    }
    fn last(&self) -> Option<SetKey> {
        let start = self
            .restarts()
            .checked_sub(1)
            .and_then(|restart| self.restart(restart))
            .unwrap_or(0);
        let mut cursor = self.cursor_at(start);
        let mut any = false;
        while cursor.advance() {
            any = true;
        }
        // The walk's end leaves its buffer at the last key it read.
        any.then_some(cursor.key)
    }
    fn small(&self) -> bool {
        self.entries().len() < BLOCK_MIN && self.count() < BLOCK_KEYS / 2
    }
}

impl Child for Block {
    /// The block's first key: its first entry shares nothing.
    fn first(&self) -> &[u8] {
        entry_at(self.entries(), 0).map_or(&[], |(_, suffix, _)| suffix)
    }
    fn keys(&self) -> usize {
        self.count()
    }
}

/// A walk of one block's keys, each decoded in place into the walk's own buffer and lent from it
/// ([`Cursor::key`]) until the walk moves. Handed out whole, a key was copied a hundred and
/// thirty bytes at a time, four or five times as a block was merged with its edits: on 1-CPU
/// x86 nodes a key set's publication took 160 µs of every claim's 624 on its leader's owner, 58%
/// of it in memcpy, and its followers' membership checks 71 µs more (cluster profile run
/// 38586796329, 1,000 writes a second).
struct Cursor<'b> {
    entries: &'b [u8],
    offset: usize,
    key: SetKey,
    /// Whether `key` is one a seek read past, which the next move gives back first.
    pending: bool,
}

impl Cursor<'_> {
    /// Moves to the next key; false at the block's end, or at an entry that cannot be read.
    fn advance(&mut self) -> bool {
        if self.pending {
            self.pending = false;
            return true;
        }
        if self.offset >= self.entries.len() {
            return false;
        }
        let Some((shared, suffix, next)) = entry_at(self.entries, self.offset) else {
            return false;
        };
        if self.key.assign(shared, suffix).is_none() {
            return false;
        }
        self.offset = next;
        true
    }
    /// The key the walk is at.
    fn key(&self) -> &[u8] {
        self.key.as_bytes()
    }
}

/// Builds blocks: entries front-coded, every `RESTART`th whole, cut at the block bounds.
struct Encoder {
    bytes: Vec<u8>,
    restarts: [u16; RESTARTS_MAX],
    restart_count: usize,
    count: usize,
    last: SetKey,
}

impl Encoder {
    fn new() -> Result<Self, MemoryError> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(BLOCK_ENTRIES_MAX)
            .map_err(|_| MemoryError::AllocationFailed)?;
        Ok(Self {
            bytes,
            restarts: [0; RESTARTS_MAX],
            restart_count: 0,
            count: 0,
            last: SetKey::empty(),
        })
    }
    /// The scratch an encoder holds, which a publication's funding covers while it runs.
    const SCRATCH: usize = BLOCK_ENTRIES_MAX + ALLOCATOR_OVERHEAD;
    fn is_empty(&self) -> bool {
        self.count == 0
    }
    /// Whether the block is cut: its entries passed the byte target or it holds its keys' bound.
    fn full(&self) -> bool {
        self.bytes.len() >= BLOCK_TARGET || self.count >= BLOCK_KEYS
    }
    /// Appends `key`, which must follow the last key pushed.
    fn push(&mut self, key: &[u8]) -> Result<(), MemoryError> {
        if key.is_empty() || key.len() > MAX_KEY {
            return Err(broken("a set key must hold 1 to MAX_KEY bytes"));
        }
        if self.count != 0 && key <= self.last.as_bytes() {
            return Err(broken("a set's keys must ascend"));
        }
        if self.full() {
            return Err(broken("a full block takes no more keys"));
        }
        let restart = self.count.checked_rem(RESTART) == Some(0);
        let shared = if restart {
            let offset = u16::try_from(self.bytes.len())
                .map_err(|_| broken("a block's restart offset passes u16"))?;
            let slot = self
                .restarts
                .get_mut(self.restart_count)
                .ok_or(broken("a block holds no more restarts"))?;
            *slot = offset;
            self.restart_count = checked_add(self.restart_count, 1)?;
            0
        } else {
            common_prefix(self.last.as_bytes(), key)
        };
        let suffix = key
            .get(shared..)
            .ok_or(broken("a shared prefix passes its key"))?;
        let shared_byte = u8::try_from(shared).map_err(|_| broken("a shared prefix passes u8"))?;
        let suffix_byte =
            u8::try_from(suffix.len()).map_err(|_| broken("a key suffix passes u8"))?;
        // Within the capacity reserved up front: a block's entries never pass its bound.
        if checked_add(checked_add(self.bytes.len(), 2)?, suffix.len())? > self.bytes.capacity() {
            return Err(broken("a block's entries pass their bound"));
        }
        self.bytes.push(shared_byte);
        self.bytes.push(suffix_byte);
        self.bytes.extend_from_slice(suffix);
        self.last
            .assign(shared, suffix)
            .ok_or(broken("a set key's suffix passes its key"))?;
        self.count = checked_add(self.count, 1)?;
        Ok(())
    }
    /// The encoder as it stands having pushed `block`'s first `index` keys, which end at
    /// `offset`, the last of them `last`: their entries and their restarts as the block holds
    /// them, copied, not encoded again. Empty encoders only.
    fn resume(
        &mut self,
        block: &Block,
        offset: usize,
        index: usize,
        last: &SetKey,
    ) -> Result<(), MemoryError> {
        if !self.is_empty() {
            return Err(broken("an encoder resumes only empty"));
        }
        let prefix = block
            .entries()
            .get(..offset)
            .ok_or(broken("a block's prefix passes its entries"))?;
        if prefix.len() > self.bytes.capacity() {
            return Err(broken("a block's prefix passes an encoder's bound"));
        }
        let restarts = index.div_ceil(RESTART);
        if restarts > block.restarts() {
            return Err(broken("a block's prefix passes its restarts"));
        }
        for (slot, restart) in self.restarts.iter_mut().zip(0..restarts) {
            *slot = u16::try_from(
                block
                    .restart(restart)
                    .ok_or(broken("a block's restart cannot be read"))?,
            )
            .map_err(|_| broken("a block's restart offset passes u16"))?;
        }
        if restarts > self.restarts.len() {
            return Err(broken("a block's prefix passes an encoder's restarts"));
        }
        self.bytes.extend_from_slice(prefix);
        self.restart_count = restarts;
        self.count = index;
        self.last = *last;
        Ok(())
    }
    /// The bytes the block of what was pushed takes.
    fn block_bytes(&self) -> Result<usize, MemoryError> {
        checked_add(
            self.bytes.len(),
            checked_add(checked_mul(self.restart_count, 2)?, 4)?,
        )
    }
    /// The block of what was pushed, with its first key's fence; the encoder is empty after it.
    fn finish(&mut self, source: &mut Source<'_>) -> Result<Seat<Block>, MemoryError> {
        let len = self.block_bytes()?;
        let charge = source.take(block_charge(len)?)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(len)
            .map_err(|_| MemoryError::AllocationFailed)?;
        bytes.extend_from_slice(&self.bytes);
        for restart in self.restarts.get(..self.restart_count).unwrap_or(&[]) {
            bytes.extend_from_slice(&restart.to_le_bytes());
        }
        let restarts = u16::try_from(self.restart_count)
            .map_err(|_| broken("a block's restart count passes u16"))?;
        let count =
            u16::try_from(self.count).map_err(|_| broken("a block's key count passes u16"))?;
        bytes.extend_from_slice(&restarts.to_le_bytes());
        bytes.extend_from_slice(&count.to_le_bytes());
        let block = Block {
            bytes,
            _charge: charge,
        };
        let first = fence(block.first());
        self.bytes.clear();
        self.restart_count = 0;
        self.count = 0;
        self.last = SetKey::empty();
        Ok((first, Arc::new(block)))
    }
}

/// A chunk (of blocks) or a mid (of chunks): its children's fences and the children, at most
/// its level's bound of them.
struct Node<C> {
    fences: Vec<Fence>,
    children: Vec<Arc<C>>,
    len: usize,
    _charge: Allocation,
}

type Chunk = Node<Block>;
type Mid = Node<Chunk>;

impl<C: Child> Child for Node<C> {
    fn first(&self) -> &[u8] {
        self.children.first().map_or(&[], |child| child.first())
    }
    fn keys(&self) -> usize {
        self.len
    }
}

impl<C: Child> Node<C> {
    /// A node of `children`, charged from `source` for exactly their room.
    fn of(children: &[Seat<C>], source: &mut Source<'_>) -> Result<Self, MemoryError> {
        let charge = source.take(node_charge(children.len())?)?;
        let mut fences = Vec::new();
        fences
            .try_reserve_exact(children.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut arcs = Vec::new();
        arcs.try_reserve_exact(children.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut len = 0usize;
        for (first, child) in children {
            fences.push(*first);
            len = checked_add(len, child.keys())?;
            arcs.push(Arc::clone(child));
        }
        Ok(Self {
            fences,
            children: arcs,
            len,
            _charge: charge,
        })
    }
    /// The same children under a node of its own.
    fn copy(&self, source: &mut Source<'_>) -> Result<Self, MemoryError> {
        let charge = source.take(node_charge(self.children.len())?)?;
        let mut fences = Vec::new();
        fences
            .try_reserve_exact(self.fences.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        fences.extend_from_slice(&self.fences);
        let mut children = Vec::new();
        children
            .try_reserve_exact(self.children.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        children.extend(self.children.iter().map(Arc::clone));
        Ok(Self {
            fences,
            children,
            len: self.len,
            _charge: charge,
        })
    }
    fn child_for(&self, key: &[u8]) -> usize {
        last_at_or_before(&self.fences, key, |index| {
            self.children.get(index).map(|child| child.first())
        })
    }
    /// The children placed as a parent places them.
    fn seats(&self) -> impl Iterator<Item = Seat<C>> + '_ {
        self.fences
            .iter()
            .copied()
            .zip(self.children.iter().map(Arc::clone))
    }
}

/// The position of the last child whose first key is at or before `key`; 0 where none is.
/// A fence orders as its key does wherever fences differ; where they tie, the first key itself
/// decides.
fn last_at_or_before<'a>(
    fences: &[Fence],
    key: &[u8],
    first: impl Fn(usize) -> Option<&'a [u8]>,
) -> usize {
    let probe = fence(key);
    let at_or_before = |index: usize| match fences.get(index).map(|fence| fence.cmp(&probe)) {
        Some(Ordering::Less) => true,
        Some(Ordering::Greater) | None => false,
        Some(Ordering::Equal) => first(index).is_some_and(|first| first <= key),
    };
    let (mut low, mut high) = (0usize, fences.len());
    while low < high {
        let Some(middle) = high
            .checked_sub(low)
            .and_then(|span| low.checked_add(span / 2))
        else {
            break;
        };
        if at_or_before(middle) {
            low = middle.saturating_add(1);
        } else {
            high = middle;
        }
    }
    low.saturating_sub(1)
}

/// The set's top level: its mids, held by the set alone, its vectors' room charged as
/// publications grow it.
struct Root {
    fences: Vec<Fence>,
    mids: Vec<Arc<Mid>>,
    len: usize,
    /// The room the vectors were charged for, in slots.
    room: usize,
    /// That room's charges: the root's own, and each publication's growth, absorbed into one of
    /// the same budget, kind and lane where one is and kept beside them where none is. One a
    /// growth at most, and a growth adds a mid, so they are bounded by the mids the root held.
    charges: Vec<Allocation>,
}

impl Root {
    /// A root of `mids`, its room charged from `source`.
    fn of(mids: &[Seat<Mid>], source: &mut Source<'_>) -> Result<Self, MemoryError> {
        let charge = source.take(root_charge(mids.len())?)?;
        let mut charges = Vec::new();
        charges
            .try_reserve_exact(4)
            .map_err(|_| MemoryError::AllocationFailed)?;
        charges.push(charge);
        let mut fences = Vec::new();
        fences
            .try_reserve_exact(mids.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut arcs = Vec::new();
        arcs.try_reserve_exact(mids.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut len = 0usize;
        for (first, mid) in mids {
            fences.push(*first);
            len = checked_add(len, mid.len)?;
            arcs.push(Arc::clone(mid));
        }
        Ok(Self {
            fences,
            mids: arcs,
            len,
            room: mids.len(),
            charges,
        })
    }
    fn view(&self) -> Tree<'_> {
        Tree {
            fences: &self.fences,
            mids: &self.mids,
            len: self.len,
        }
    }
    /// Replaces the `remove` mids from `at` (none or one), which held `old` keys before their
    /// edit, with `seats`, the vectors grown (and charged from `growth`, absorbed into its lane's
    /// charge) where their room is short.
    fn splice(
        &mut self,
        at: usize,
        remove: usize,
        old: usize,
        seats: Vec<Seat<Mid>>,
        growth: &mut Allocation,
    ) -> Result<(), MemoryError> {
        let end = checked_add(at, remove)?;
        if end > self.mids.len() {
            return Err(broken("a reshaped mid passes its root"));
        }
        let total = checked_add(
            self.mids
                .len()
                .checked_sub(remove)
                .ok_or(broken("a root removes more mids than it holds"))?,
            seats.len(),
        )?;
        if total > self.room {
            let more = total
                .checked_sub(self.mids.len())
                .ok_or(broken("a root's room passes its mids"))?;
            self.fences
                .try_reserve_exact(more)
                .map_err(|_| MemoryError::AllocationFailed)?;
            self.mids
                .try_reserve_exact(more)
                .map_err(|_| MemoryError::AllocationFailed)?;
            let room = self.fences.capacity().max(self.mids.capacity());
            let mut grown = growth.split_off(checked_add(
                checked_mul(
                    room.checked_sub(self.room)
                        .ok_or(broken("a root's room shrank as it grew"))?,
                    CHILD_SLOT,
                )?,
                GROWTH_FIXED,
            )?)?;
            if !self
                .charges
                .iter_mut()
                .any(|charge| charge.absorb(&mut grown).is_ok())
            {
                self.charges
                    .try_reserve(1)
                    .map_err(|_| MemoryError::AllocationFailed)?;
                self.charges.push(grown);
            }
            self.room = room;
        }
        let mut len = self
            .len
            .checked_sub(old)
            .ok_or(broken("a mid holds more than its root"))?;
        for (_, mid) in &seats {
            len = checked_add(len, mid.len)?;
        }
        // Within the room just made: neither splice reallocates.
        self.fences
            .splice(at..end, seats.iter().map(|(first, _)| *first));
        self.mids
            .splice(at..end, seats.into_iter().map(|(_, mid)| mid));
        self.len = len;
        Ok(())
    }
}

/// A read of a root's tree: a set's own, or a frozen image's copy.
#[derive(Clone, Copy)]
struct Tree<'s> {
    fences: &'s [Fence],
    mids: &'s [Arc<Mid>],
    len: usize,
}

impl<'s> Tree<'s> {
    fn mid_for(&self, key: &[u8]) -> usize {
        last_at_or_before(self.fences, key, |index| {
            self.mids.get(index).map(|mid| mid.first())
        })
    }
    /// The mid, chunk and block positions `key` falls in.
    fn locate(&self, key: &[u8]) -> Option<(usize, usize, usize)> {
        let mid_index = self.mid_for(key);
        let mid = self.mids.get(mid_index)?;
        let chunk_index = mid.child_for(key);
        let chunk = mid.children.get(chunk_index)?;
        Some((mid_index, chunk_index, chunk.child_for(key)))
    }
    fn block(&self, (mid, chunk, block): (usize, usize, usize)) -> Option<&'s Block> {
        self.mids
            .get(mid)?
            .children
            .get(chunk)?
            .children
            .get(block)
            .map(|block| &**block)
    }
    fn contains(&self, key: &[u8]) -> bool {
        self.locate(key)
            .and_then(|at| self.block(at))
            .is_some_and(|block| block.contains(key))
    }
    fn iter_from(&self, key: Option<&[u8]>, exclusive: bool) -> KeyIter<'s> {
        let (at, cursor) = match key {
            None => ((0, 0, 0), self.block((0, 0, 0)).map(|block| block.cursor())),
            Some(key) => match self.locate(key) {
                Some(at) => (at, self.block(at).map(|block| block.seek(key, exclusive))),
                None => ((0, 0, 0), None),
            },
        };
        KeyIter {
            mids: self.mids,
            mid: at.0,
            chunk: at.1,
            block: at.2,
            cursor,
        }
    }
    fn last(&self) -> Option<SetKey> {
        self.mids
            .last()
            .and_then(|mid| mid.children.last())
            .and_then(|chunk| chunk.children.last())
            .and_then(|block| block.last())
    }
    fn stats(&self) -> KeySetStats {
        let mut stats = KeySetStats {
            keys: self.len,
            mids: self.mids.len(),
            ..KeySetStats::default()
        };
        for mid in self.mids {
            stats.chunks = stats.chunks.saturating_add(mid.children.len());
            for chunk in &mid.children {
                stats.blocks = stats.blocks.saturating_add(chunk.children.len());
                for block in &chunk.children {
                    stats.block_bytes = stats.block_bytes.saturating_add(block.bytes.len());
                }
            }
        }
        stats
    }
}

/// The keys of a set in order, from where it was asked.
pub struct KeyIter<'s> {
    mids: &'s [Arc<Mid>],
    mid: usize,
    chunk: usize,
    block: usize,
    cursor: Option<Cursor<'s>>,
}

impl Iterator for KeyIter<'_> {
    type Item = SetKey;
    fn next(&mut self) -> Option<SetKey> {
        // Each turn moves to a later block; the blocks are finite.
        loop {
            if let Some(cursor) = self.cursor.as_mut()
                && cursor.advance()
            {
                return Some(cursor.key);
            }
            let mid = self.mids.get(self.mid)?;
            let chunk = mid.children.get(self.chunk)?;
            self.block = self.block.checked_add(1)?;
            if self.block >= chunk.children.len() {
                self.block = 0;
                self.chunk = self.chunk.checked_add(1)?;
                if self.chunk >= mid.children.len() {
                    self.chunk = 0;
                    self.mid = self.mid.checked_add(1)?;
                }
            }
            let block = self
                .mids
                .get(self.mid)?
                .children
                .get(self.chunk)?
                .children
                .get(self.block)?;
            self.cursor = Some(block.cursor());
        }
    }
}

/// A frozen image's copy of a root.
struct FrozenRoot {
    fences: Vec<Fence>,
    mids: Vec<Arc<Mid>>,
    len: usize,
    _charge: Allocation,
}

/// A set frozen as it stood: read-only and sendable, every block charged where it was.
#[derive(Clone)]
pub struct FrozenKeySet {
    root: Arc<FrozenRoot>,
}

impl FrozenKeySet {
    fn tree(&self) -> Tree<'_> {
        Tree {
            fences: &self.root.fences,
            mids: &self.root.mids,
            len: self.root.len,
        }
    }
    pub fn len(&self) -> usize {
        self.root.len
    }
    pub fn is_empty(&self) -> bool {
        self.root.len == 0
    }
    pub fn contains(&self, key: &[u8]) -> bool {
        self.tree().contains(key)
    }
    pub fn iter(&self) -> KeyIter<'_> {
        self.tree().iter_from(None, false)
    }
    /// Keys at or after `key` (after it, `exclusive`).
    pub fn iter_from(&self, key: &[u8], exclusive: bool) -> KeyIter<'_> {
        self.tree().iter_from(Some(key), exclusive)
    }
    pub fn stats(&self) -> KeySetStats {
        self.tree().stats()
    }
}

/// A frozen image's handle that retains nothing: whoever holds the image itself (a lease
/// registry) decides how long it lives, and a read through a handle whose image is gone finds
/// nothing to read.
#[derive(Clone)]
pub struct WeakKeySet {
    root: std::sync::Weak<FrozenRoot>,
}

impl WeakKeySet {
    /// The image, while its holder keeps it.
    pub fn upgrade(&self) -> Option<FrozenKeySet> {
        self.root.upgrade().map(|root| FrozenKeySet { root })
    }
}

impl FrozenKeySet {
    /// A handle on this image that retains nothing.
    pub fn downgrade(&self) -> WeakKeySet {
        WeakKeySet {
            root: Arc::downgrade(&self.root),
        }
    }
}

/// What a publication of a plan's edits may spend, from the write's own source: the funding its
/// blocks, chunks, mids and scratch are charged to, and the root's growth.
pub struct KeySetFunding {
    funding: Allocation,
    growth: Allocation,
}

impl KeySetFunding {
    /// The bytes it holds.
    pub fn bytes(&self) -> usize {
        self.funding.bytes().saturating_add(self.growth.bytes())
    }
}

/// An ordered set of byte keys held front-coded (module docs).
pub struct KeySet {
    root: Root,
    budget: MemoryBudget,
    poisoned: bool,
}

impl std::fmt::Debug for KeySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeySet")
            .field("keys", &self.root.len)
            .field("mids", &self.root.mids.len())
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}

impl KeySet {
    /// An empty set whose root is charged to `budget` in `lane`.
    pub fn new(budget: &MemoryBudget, lane: BudgetLane) -> Result<Self, MemoryError> {
        Ok(Self {
            root: Root::of(&[], &mut Source::Budget(budget, lane))?,
            budget: budget.clone(),
            poisoned: false,
        })
    }
    pub fn budget(&self) -> &MemoryBudget {
        &self.budget
    }
    pub fn len(&self) -> usize {
        self.root.len
    }
    pub fn is_empty(&self) -> bool {
        self.root.len == 0
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }
    pub fn contains(&self, key: &[u8]) -> bool {
        self.root.view().contains(key)
    }
    pub fn iter(&self) -> KeyIter<'_> {
        self.root.view().iter_from(None, false)
    }
    /// Keys at or after `key` (after it, `exclusive`).
    pub fn iter_from(&self, key: &[u8], exclusive: bool) -> KeyIter<'_> {
        self.root.view().iter_from(Some(key), exclusive)
    }
    pub fn stats(&self) -> KeySetStats {
        self.root.view().stats()
    }
    /// The set as it stands, for a reader that outlives later publications: a copy of the root,
    /// a slot a mid, charged to the set's budget in `lane`.
    pub fn freeze(&self, lane: BudgetLane) -> Result<FrozenKeySet, MemoryError> {
        let charge = self
            .budget
            .reserve(BudgetKind::Pages, lane, root_charge(self.root.mids.len())?)?
            .commit();
        let mut fences = Vec::new();
        fences
            .try_reserve_exact(self.root.fences.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        fences.extend_from_slice(&self.root.fences);
        let mut mids = Vec::new();
        mids.try_reserve_exact(self.root.mids.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        mids.extend(self.root.mids.iter().map(Arc::clone));
        Ok(FrozenKeySet {
            root: Arc::new(FrozenRoot {
                fences,
                mids,
                len: self.root.len,
                _charge: charge,
            }),
        })
    }
    /// The most a publication of `edits` edits may spend beside the root's growth, whatever its
    /// keys, whatever the set holds and whatever frozen images share (`quote_for`).
    pub fn quote(edits: usize) -> Result<usize, MemoryError> {
        quote_for(edits, 1)
    }
    /// What `edits` edits divided among `parts` sets' publications may spend beside their roots'
    /// growth, all together: each part's quote, whatever the division.
    pub fn quote_parts(edits: usize, parts: usize) -> Result<usize, MemoryError> {
        quote_for(edits, parts.min(edits))
    }
    /// The most a publication of `edits` edits may grow the root by: two mids an edit, and a
    /// place in its charges an edit (each reshaped mid grows it once at most).
    pub fn growth(edits: usize) -> Result<usize, MemoryError> {
        checked_mul(
            edits,
            checked_add(checked_mul(2, CHILD_SLOT)?, GROWTH_FIXED)?,
        )
    }
    /// What a publication of `edits` edits may spend, reserved from `source` in `lane` (a write's
    /// own budget, within the set's): its quote and the root's growth.
    pub fn fund(
        &self,
        source: &MemoryBudget,
        lane: BudgetLane,
        edits: usize,
    ) -> Result<KeySetFunding, MemoryError> {
        if !source.is_within(&self.budget) {
            return Err(broken("a key set's funding source is outside its budget"));
        }
        let funding = source
            .reserve(BudgetKind::Pages, lane, Self::quote(edits)?)?
            .commit();
        let growth = source
            .reserve(BudgetKind::Pages, lane, Self::growth(edits)?)?
            .commit();
        Ok(KeySetFunding { funding, growth })
    }
    /// Checks `edits` (strictly ascending keys, deletions of keys the set holds) against the set
    /// as it stands, changing nothing.
    pub fn check<E: SetEdit>(&self, edits: &[E]) -> Result<(), MemoryError> {
        self.check_live()?;
        let mut previous: Option<&[u8]> = None;
        for edit in edits {
            let key = edit.edit_key();
            if key.is_empty() || key.len() > MAX_KEY {
                return Err(broken("a set key must hold 1 to MAX_KEY bytes"));
            }
            if previous.is_some_and(|previous| previous >= key) {
                return Err(MemoryError::DuplicateKey);
            }
            if edit.deletes() && !self.contains(key) {
                return Err(MemoryError::MissingKey);
            }
            previous = Some(key);
        }
        Ok(())
    }
    /// Publishes `edits` (strictly ascending keys), spending `funding` (what [`Self::fund`]
    /// reserved for at least as many edits). Refused, nothing changed, where [`Self::check`]
    /// refuses; a failure past it poisons the set.
    pub fn publish<E: SetEdit>(
        &mut self,
        edits: &[E],
        funding: &mut KeySetFunding,
    ) -> Result<(), MemoryError> {
        self.check(edits)?;
        if edits.is_empty() {
            return Ok(());
        }
        if let Err(error) = self.apply(edits, funding) {
            self.poisoned = true;
            return Err(error);
        }
        Ok(())
    }
    fn check_live(&self) -> Result<(), MemoryError> {
        if self.poisoned {
            return Err(broken("key set poisoned by a failed publication"));
        }
        Ok(())
    }
    fn apply<E: SetEdit>(
        &mut self,
        edits: &[E],
        funding: &mut KeySetFunding,
    ) -> Result<(), MemoryError> {
        let KeySetFunding { funding, growth } = funding;
        let mut source = Source::Funding(funding);
        let mut encoder = Encoder::new()?;
        let mut join = Encoder::new()?;
        let root = &mut self.root;
        if root.mids.is_empty() {
            // Every edit is a put: a deletion was checked against what the set holds.
            let blocks = build_blocks(
                edits.iter().map(SetEdit::edit_key),
                &mut encoder,
                &mut source,
            )?;
            let chunks = group(&blocks, CHUNK_BLOCKS, &mut source)?;
            let mids = group(&chunks, MID_CHUNKS, &mut source)?;
            return root.splice(0, 0, 0, mids, growth);
        }
        // Mids an edit cut or emptied, with the keys each held before, applied from the last so
        // earlier positions stand.
        let mut reshapes: Vec<(usize, usize, Vec<Seat<Mid>>)> = Vec::new();
        let mut start = 0usize;
        // Each turn takes the edits of one mid, so the turns are at most the edits.
        while start < edits.len() {
            let key = edits
                .get(start)
                .ok_or(broken("an edit index passes the edits"))?
                .edit_key();
            let index = root.view().mid_for(key);
            let end = group_end(
                edits,
                start,
                root.mids
                    .get(index.saturating_add(1))
                    .map(|mid| mid.first()),
            );
            let group = edits
                .get(start..end)
                .ok_or(broken("an edit group passes the edits"))?;
            let slot = root
                .mids
                .get_mut(index)
                .ok_or(broken("a mid index passes the root"))?;
            let before = slot.len;
            let edited = edit_node(
                slot,
                group,
                MID_CHUNKS,
                &mut source,
                |chunk, group, source| edit_chunk(chunk, group, &mut encoder, &mut join, source),
            )?;
            match edited {
                Edited::InPlace => {
                    let first = fence(slot.first());
                    let after = slot.len;
                    *root
                        .fences
                        .get_mut(index)
                        .ok_or(broken("a root's fences and mids differ"))? = first;
                    root.len = checked_add(
                        root.len
                            .checked_sub(before)
                            .ok_or(broken("a mid holds more than its root"))?,
                        after,
                    )?;
                }
                Edited::Replaced(seats) => {
                    reshapes
                        .try_reserve(1)
                        .map_err(|_| MemoryError::AllocationFailed)?;
                    reshapes.push((index, before, seats));
                }
            }
            start = end;
        }
        for (index, before, seats) in reshapes.into_iter().rev() {
            root.splice(index, 1, before, seats, growth)?;
        }
        Ok(())
    }
    /// The keys below `at` and those from it on, as two sets charged to `lane`: blocks, chunks
    /// and mids wholly on one side are shared, the ones `at` falls in rebuilt.
    pub fn split_at(&self, at: &[u8], lane: BudgetLane) -> Result<(Self, Self), MemoryError> {
        self.check_live()?;
        let mut source = Source::Budget(&self.budget, lane);
        let mut encoder = Encoder::new()?;
        let tree = self.root.view();
        let mut below: Vec<Seat<Mid>> = Vec::new();
        let mut above: Vec<Seat<Mid>> = Vec::new();
        below
            .try_reserve_exact(checked_add(tree.mids.len(), 2)?)
            .map_err(|_| MemoryError::AllocationFailed)?;
        above
            .try_reserve_exact(checked_add(tree.mids.len(), 2)?)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let index = tree.mid_for(at);
        for (position, seat) in self
            .root
            .fences
            .iter()
            .copied()
            .zip(self.root.mids.iter().map(Arc::clone))
            .enumerate()
        {
            match position.cmp(&index) {
                Ordering::Less => below.push(seat),
                Ordering::Greater => above.push(seat),
                Ordering::Equal => {
                    let (low, high) = split_mid(&seat.1, at, &mut encoder, &mut source)?;
                    below.extend(group(&low, MID_CHUNKS, &mut source)?);
                    above.extend(group(&high, MID_CHUNKS, &mut source)?);
                }
            }
        }
        Ok((
            Self {
                root: Root::of(&below, &mut source)?,
                budget: self.budget.clone(),
                poisoned: false,
            },
            Self {
                root: Root::of(&above, &mut source)?,
                budget: self.budget.clone(),
                poisoned: false,
            },
        ))
    }
    /// This set's keys and `right`'s, every one of which follows every one of this set's, as
    /// one set charged to `lane`. Refused where a key of `right` does not follow.
    pub fn join(&self, right: &Self, lane: BudgetLane) -> Result<Self, MemoryError> {
        self.check_live()?;
        right.check_live()?;
        if let (Some(last), Some(first)) = (self.root.view().last(), right.iter().next())
            && last >= first
        {
            return Err(broken("a joined set's keys must follow the set's"));
        }
        let mut mids: Vec<Seat<Mid>> = Vec::new();
        mids.try_reserve_exact(checked_add(self.root.mids.len(), right.root.mids.len())?)
            .map_err(|_| MemoryError::AllocationFailed)?;
        for root in [&self.root, &right.root] {
            mids.extend(
                root.fences
                    .iter()
                    .copied()
                    .zip(root.mids.iter().map(Arc::clone)),
            );
        }
        Ok(Self {
            root: Root::of(&mids, &mut Source::Budget(&self.budget, lane))?,
            budget: self.budget.clone(),
            poisoned: false,
        })
    }
}

/// The most a publication of `edits` edits may spend beside the root's growth.
///
/// Re-encoding never grows a run of keys but by the entries it inserts and its restarts: a
/// deletion shortens its successor's shared prefix by no more than its own entry took, and an
/// insertion lengthens its successor's; each restart stores a whole key. So an edit's block,
/// rebuilt, holds at most the block's entries, the edit's entry and a whole key at each restart
/// of its output (`REBUILT`); a join of a small block with its neighbour, the two blocks and
/// their output's restarts (`JOINED`). Either is cut every `BLOCK_TARGET` bytes or `BLOCK_KEYS`
/// keys, so its blocks are bounded by both. The chunk and the mid an edit lands in are copied
/// where an image shares them, and cut in two where they pass their bound, each with a slot for
/// every child the edit adds; and the publication's scratch is held while it runs.
fn quote_for(edits: usize, parts: usize) -> Result<usize, MemoryError> {
    if edits == 0 {
        return Ok(0);
    }
    let parts = parts.max(1);
    // Restarts an output may hold: its keys over the interval, and one a block it is cut into.
    const OUTPUT_BLOCKS: usize = 8;
    const REBUILT_RESTARTS: usize = (BLOCK_KEYS + 1).div_ceil(RESTART) + OUTPUT_BLOCKS;
    const REBUILT: usize = BLOCK_ENTRIES_MAX + ENTRY_MAX + REBUILT_RESTARTS * MAX_KEY;
    const JOINED_RESTARTS: usize = (BLOCK_KEYS + BLOCK_KEYS / 2).div_ceil(RESTART) + OUTPUT_BLOCKS;
    const JOINED: usize = BLOCK_MIN + BLOCK_ENTRIES_MAX + JOINED_RESTARTS * MAX_KEY;
    // Blocks out of the two, by bytes and by keys, one more each for a cut's remainder.
    const BLOCKS: usize = REBUILT.div_ceil(BLOCK_TARGET)
        + JOINED.div_ceil(BLOCK_TARGET)
        + (BLOCK_KEYS + 1).div_ceil(BLOCK_KEYS)
        + (BLOCK_KEYS + BLOCK_KEYS / 2).div_ceil(BLOCK_KEYS)
        + 2;
    const BLOCK_BYTES: usize = REBUILT + JOINED + BLOCKS * (BLOCK_FIXED + TRAILER_MAX);
    // A chunk copied, and its blocks with the edit's cut into two chunks; a mid likewise.
    const CHUNK_BYTES: usize =
        2 * NODE_FIXED + CHUNK_BLOCKS * CHILD_SLOT + 2 * (CHUNK_BLOCKS + BLOCKS) * CHILD_SLOT;
    const MID_BYTES: usize =
        2 * NODE_FIXED + MID_CHUNKS * CHILD_SLOT + 2 * (MID_CHUNKS + 2) * CHILD_SLOT;
    const PER_EDIT: usize = BLOCK_BYTES + CHUNK_BYTES + MID_BYTES;
    // Scratch: two encoders, and a node's rebuilt lists and the reshapes, each a slot a child
    // and the vectors' allocations.
    const SCRATCH_FIXED: usize = 2 * Encoder::SCRATCH + 8 * ALLOCATOR_OVERHEAD;
    // Every part holds its own scratch; the edits' slots are divided among them.
    let slots = checked_add(
        checked_mul(parts, CHUNK_BLOCKS + MID_CHUNKS)?,
        checked_mul(BLOCKS, edits)?,
    )?;
    const SCRATCH_SLOT: usize =
        2 * size_of::<Seat<Block>>() + size_of::<(usize, usize, Vec<Seat<Mid>>)>() + 1;
    let scratch = checked_add(
        checked_mul(parts, SCRATCH_FIXED)?,
        checked_mul(slots, SCRATCH_SLOT)?,
    )?;
    checked_add(checked_mul(edits, PER_EDIT)?, scratch)
}

/// What an edit made of a child.
enum Edited<C> {
    /// The child, held by its parent alone, was edited where it stands.
    InPlace,
    /// The children that replace it: none where it was emptied, more where it was cut.
    Replaced(Vec<Seat<C>>),
}

/// Where the edits of the child that begins at the current one end: at the first edit at or
/// after the next child's first key, or at the end.
fn group_end<E: SetEdit>(edits: &[E], start: usize, next_first: Option<&[u8]>) -> usize {
    let Some(next_first) = next_first else {
        return edits.len();
    };
    let rest = edits.get(start..).unwrap_or(&[]);
    start.saturating_add(rest.partition_point(|edit| edit.edit_key() < next_first))
}

/// Groups `children` into nodes of at most `bound` children, evenly.
fn group<C: Child>(
    children: &[Seat<C>],
    bound: usize,
    source: &mut Source<'_>,
) -> Result<Vec<Seat<Node<C>>>, MemoryError> {
    let mut nodes = Vec::new();
    if children.is_empty() {
        return Ok(nodes);
    }
    let count = children.len().div_ceil(bound.max(1));
    let per = children.len().div_ceil(count);
    nodes
        .try_reserve_exact(count)
        .map_err(|_| MemoryError::AllocationFailed)?;
    for part in children.chunks(per) {
        let node = Node::of(part, source)?;
        let first = fence(node.first());
        nodes.push((first, Arc::new(node)));
    }
    Ok(nodes)
}

/// The node in `slot` after `edits`, which fall in it: each child they fall in edited by
/// `edit_child`. A node something else holds is copied first, so its children are edited in
/// the copy; where each child stood or became one, the node is edited where it stands, and
/// otherwise replaced by nodes of at most `bound` children of what it became (none where it
/// emptied).
fn edit_node<C: Child, E: SetEdit>(
    slot: &mut Arc<Node<C>>,
    edits: &[E],
    bound: usize,
    source: &mut Source<'_>,
    mut edit_child: impl FnMut(&mut Arc<C>, &[E], &mut Source<'_>) -> Result<Edited<C>, MemoryError>,
) -> Result<Edited<Node<C>>, MemoryError> {
    if Arc::get_mut(slot).is_none() {
        let copy = slot.copy(source)?;
        *slot = Arc::new(copy);
    }
    let node = Arc::get_mut(slot).ok_or(broken("a node is shared after its copy"))?;
    let mut reshapes: Vec<(usize, Vec<Seat<C>>)> = Vec::new();
    let mut start = 0usize;
    // Each turn takes the edits of one child, so the turns are at most the edits.
    while start < edits.len() {
        let key = edits
            .get(start)
            .ok_or(broken("an edit index passes the edits"))?
            .edit_key();
        let index = node.child_for(key);
        let end = group_end(
            edits,
            start,
            node.children
                .get(index.saturating_add(1))
                .map(|child| child.first()),
        );
        let group = edits
            .get(start..end)
            .ok_or(broken("an edit group passes the edits"))?;
        let child = node
            .children
            .get_mut(index)
            .ok_or(broken("a child index passes its node"))?;
        let before = child.keys();
        match edit_child(child, group, source)? {
            Edited::InPlace => {
                let first = fence(child.first());
                let after = child.keys();
                *node
                    .fences
                    .get_mut(index)
                    .ok_or(broken("a node's fences and children differ"))? = first;
                node.len = checked_add(
                    node.len
                        .checked_sub(before)
                        .ok_or(broken("a child holds more than its node"))?,
                    after,
                )?;
            }
            Edited::Replaced(mut seats) if seats.len() == 1 => {
                let (first, replacement) =
                    seats.pop().ok_or(broken("a replacement lost its child"))?;
                let after = replacement.keys();
                *child = replacement;
                *node
                    .fences
                    .get_mut(index)
                    .ok_or(broken("a node's fences and children differ"))? = first;
                node.len = checked_add(
                    node.len
                        .checked_sub(before)
                        .ok_or(broken("a child holds more than its node"))?,
                    after,
                )?;
            }
            Edited::Replaced(seats) => {
                reshapes
                    .try_reserve(1)
                    .map_err(|_| MemoryError::AllocationFailed)?;
                reshapes.push((index, seats));
            }
        }
        start = end;
    }
    if reshapes.is_empty() {
        return Ok(Edited::InPlace);
    }
    let added = reshapes.iter().map(|(_, seats)| seats.len()).sum::<usize>();
    let mut children: Vec<Seat<C>> = Vec::new();
    children
        .try_reserve_exact(checked_add(node.children.len(), added)?)
        .map_err(|_| MemoryError::AllocationFailed)?;
    let mut pending = reshapes.into_iter().peekable();
    for (position, seat) in node.seats().enumerate() {
        if pending.peek().is_some_and(|(index, _)| *index == position) {
            if let Some((_, seats)) = pending.next() {
                children.extend(seats);
            }
            continue;
        }
        children.push(seat);
    }
    Ok(Edited::Replaced(group(&children, bound, source)?))
}

/// One chunk after `edits`, which fall in it: each block an edit lands in rebuilt with its
/// edits, a rebuilt block left small joined with a neighbour, the rest shared. Edited where it
/// stands when the set alone holds it and every rebuilt block became one block that is not
/// small; otherwise replaced by chunks of its blocks.
fn edit_chunk<E: SetEdit>(
    slot: &mut Arc<Chunk>,
    edits: &[E],
    encoder: &mut Encoder,
    join: &mut Encoder,
    source: &mut Source<'_>,
) -> Result<Edited<Chunk>, MemoryError> {
    // Each touched block's position and what it was rebuilt into.
    let mut rebuilt: Vec<(usize, Vec<Seat<Block>>)> = Vec::new();
    let held = slot.children.len();
    {
        let chunk: &Chunk = slot;
        rebuilt
            .try_reserve_exact(edits.len().min(chunk.children.len()))
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut start = 0usize;
        // Each turn takes the edits of one block, so the turns are at most the edits.
        while start < edits.len() {
            let key = edits
                .get(start)
                .ok_or(broken("an edit index passes the edits"))?
                .edit_key();
            let index = chunk.child_for(key);
            let end = group_end(
                edits,
                start,
                chunk
                    .children
                    .get(index.saturating_add(1))
                    .map(|block| block.first()),
            );
            let block = chunk
                .children
                .get(index)
                .ok_or(broken("a block index passes its chunk"))?;
            let group = edits
                .get(start..end)
                .ok_or(broken("an edit group passes the edits"))?;
            let mut out = Vec::new();
            rebuild_block(block, group, encoder, source, &mut out)?;
            if rebuilt.len() >= rebuilt.capacity() {
                return Err(broken("a chunk's rebuilt blocks pass its edits"));
            }
            rebuilt.push((index, out));
            start = end;
        }
    }
    let stands = rebuilt
        .iter()
        .all(|(_, out)| matches!(out.as_slice(), [(_, block)] if !block.small() || held == 1));
    if stands && Arc::get_mut(slot).is_some() {
        let chunk = Arc::get_mut(slot).ok_or(broken("a chunk's holders changed under its edit"))?;
        for (index, mut out) in rebuilt {
            let (first, block) = out.pop().ok_or(broken("a rebuilt block was lost"))?;
            let old = chunk
                .children
                .get_mut(index)
                .ok_or(broken("a block index passes its chunk"))?;
            let removed = old.count();
            let added = block.count();
            *old = block;
            *chunk
                .fences
                .get_mut(index)
                .ok_or(broken("a chunk's fences and blocks differ"))? = first;
            chunk.len = checked_add(
                chunk
                    .len
                    .checked_sub(removed)
                    .ok_or(broken("a block holds more than its chunk"))?,
                added,
            )?;
        }
        return Ok(Edited::InPlace);
    }
    // The chunk's blocks with the rebuilt ones in place, each rebuilt one marked fresh.
    let chunk: &Chunk = slot;
    let added = rebuilt.iter().map(|(_, out)| out.len()).sum::<usize>();
    let room = checked_add(chunk.children.len(), added)?;
    let mut blocks: Vec<Seat<Block>> = Vec::new();
    let mut fresh: Vec<bool> = Vec::new();
    blocks
        .try_reserve_exact(room)
        .map_err(|_| MemoryError::AllocationFailed)?;
    fresh
        .try_reserve_exact(room)
        .map_err(|_| MemoryError::AllocationFailed)?;
    let mut pending = rebuilt.into_iter().peekable();
    for (position, seat) in chunk.seats().enumerate() {
        if pending.peek().is_some_and(|(index, _)| *index == position) {
            if let Some((_, out)) = pending.next() {
                for built in out {
                    blocks.push(built);
                    fresh.push(true);
                }
            }
            continue;
        }
        blocks.push(seat);
        fresh.push(false);
    }
    join_small(&mut blocks, &mut fresh, join, source)?;
    Ok(Edited::Replaced(group(&blocks, CHUNK_BLOCKS, source)?))
}

/// Encodes `keys` (ascending) into blocks cut at the block bounds.
fn build_blocks<'k>(
    keys: impl Iterator<Item = &'k [u8]>,
    encoder: &mut Encoder,
    source: &mut Source<'_>,
) -> Result<Vec<Seat<Block>>, MemoryError> {
    let mut blocks = Vec::new();
    for key in keys {
        if encoder.full() {
            blocks
                .try_reserve(1)
                .map_err(|_| MemoryError::AllocationFailed)?;
            blocks.push(encoder.finish(source)?);
        }
        encoder.push(key)?;
    }
    if !encoder.is_empty() {
        blocks
            .try_reserve(1)
            .map_err(|_| MemoryError::AllocationFailed)?;
        blocks.push(encoder.finish(source)?);
    }
    Ok(blocks)
}

/// Keys lent one at a time in ascending order, each until the next is asked for.
trait LentKeys {
    fn next_key(&mut self) -> Result<Option<&[u8]>, MemoryError>;
}

/// The keys of a block with edits applied, in order, each lent from the block's walk or the edit
/// that puts it.
struct Merged<'b, 'e, E> {
    cursor: Cursor<'b>,
    /// Whether the walk is at a key not yet given.
    held: bool,
    /// Whether the walk's key was given last: the walk moves past it before the next.
    given: bool,
    edits: std::iter::Peekable<std::slice::Iter<'e, E>>,
}

impl<'b, 'e, E: SetEdit> Merged<'b, 'e, E> {
    fn new(block: &'b Block, edits: &'e [E]) -> Self {
        Self::from(block.cursor(), edits)
    }
    /// The keys `cursor` walks with `edits` applied.
    fn from(mut cursor: Cursor<'b>, edits: &'e [E]) -> Self {
        let held = cursor.advance();
        Self {
            cursor,
            held,
            given: false,
            edits: edits.iter().peekable(),
        }
    }
}

impl<E: SetEdit> LentKeys for Merged<'_, '_, E> {
    /// The next key; a deletion of a key the block does not hold is refused.
    fn next_key(&mut self) -> Result<Option<&[u8]>, MemoryError> {
        if self.given {
            self.given = false;
            self.held = self.cursor.advance();
        }
        // Each turn takes a held key or an edit, and returns but where a deletion removes one:
        // the turns are at most the block's keys and its edits.
        loop {
            match (self.held, self.edits.peek().copied()) {
                (false, None) => return Ok(None),
                (true, None) => {
                    self.given = true;
                    return Ok(Some(self.cursor.key()));
                }
                (false, Some(edit)) => {
                    self.edits.next();
                    if edit.deletes() {
                        return Err(MemoryError::MissingKey);
                    }
                    return set_key(edit.edit_key()).map(Some);
                }
                (true, Some(edit)) => match self.cursor.key().cmp(edit.edit_key()) {
                    Ordering::Less => {
                        self.given = true;
                        return Ok(Some(self.cursor.key()));
                    }
                    Ordering::Equal => {
                        self.edits.next();
                        if !edit.deletes() {
                            self.given = true;
                            return Ok(Some(self.cursor.key()));
                        }
                        self.held = self.cursor.advance();
                    }
                    Ordering::Greater => {
                        self.edits.next();
                        if edit.deletes() {
                            return Err(MemoryError::MissingKey);
                        }
                        return set_key(edit.edit_key()).map(Some);
                    }
                },
            }
        }
    }
}

/// Two blocks' keys in turn, the first's then the second's.
struct Joined<'b> {
    first: Cursor<'b>,
    second: Cursor<'b>,
    on_second: bool,
}

impl LentKeys for Joined<'_> {
    fn next_key(&mut self) -> Result<Option<&[u8]>, MemoryError> {
        if !self.on_second {
            if self.first.advance() {
                return Ok(Some(self.first.key()));
            }
            self.on_second = true;
        }
        Ok(self.second.advance().then(|| self.second.key()))
    }
}

/// `key` as a set holds it, checked as [`SetKey::new`] checks it: one to [`MAX_KEY`] bytes.
fn set_key(key: &[u8]) -> Result<&[u8], MemoryError> {
    if key.is_empty() {
        return Err(broken("a set key must hold at least one byte"));
    }
    if key.len() > MAX_KEY {
        return Err(broken("a set key must hold at most MAX_KEY bytes"));
    }
    Ok(key)
}

/// How many keys each block of an output of `count` keys, about `bytes` bytes of entries,
/// holds: the output cut into as few blocks as its bounds allow, evenly, as a B-tree splits a
/// node in halves. Cut greedily, a full block and one more key made a block of one key, and a
/// set published at random kept a third of its blocks' room empty.
fn per_block(count: usize, bytes: usize) -> usize {
    let blocks = count
        .div_ceil(BLOCK_KEYS)
        .max(bytes.div_ceil(BLOCK_TARGET))
        .max(1);
    count.div_ceil(blocks).max(1)
}

/// Encodes `next`'s keys into blocks of `per` keys each, cut sooner where a block's bytes pass
/// its bound, appended to `out`.
fn encode_cut(
    keys: &mut impl LentKeys,
    per: usize,
    encoder: &mut Encoder,
    source: &mut Source<'_>,
    out: &mut Vec<Seat<Block>>,
) -> Result<(), MemoryError> {
    // Each turn takes one key; the keys are the caller's finite output.
    while let Some(key) = keys.next_key()? {
        if encoder.count >= per || encoder.full() {
            out.try_reserve(1)
                .map_err(|_| MemoryError::AllocationFailed)?;
            out.push(encoder.finish(source)?);
        }
        encoder.push(key)?;
    }
    if !encoder.is_empty() {
        out.try_reserve(1)
            .map_err(|_| MemoryError::AllocationFailed)?;
        out.push(encoder.finish(source)?);
    }
    Ok(())
}

/// About the bytes of entries `count` keys take where a block holds `held` keys in `bytes`.
fn scaled(bytes: usize, held: usize, count: usize) -> usize {
    if held == 0 {
        return bytes;
    }
    bytes
        .checked_mul(count)
        .map_or(usize::MAX, |total| total.div_ceil(held))
}

/// `block`'s keys with `edits` applied, encoded and cut evenly into blocks appended to `out`.
fn rebuild_block<E: SetEdit>(
    block: &Block,
    edits: &[E],
    encoder: &mut Encoder,
    source: &mut Source<'_>,
    out: &mut Vec<Seat<Block>>,
) -> Result<(), MemoryError> {
    let held = block.count();
    // The output is counted first, so it is cut evenly: few edits by finding each in the block,
    // a seek reading at most a restart interval's entries; many by merging them with every key
    // the block holds.
    let count = if held > 0 && edits.len().saturating_mul(RESTART) < held {
        let mut count = held;
        for edit in edits {
            let present = block.contains(edit.edit_key());
            if edit.deletes() {
                if !present {
                    return Err(MemoryError::MissingKey);
                }
                count = count.checked_sub(1).ok_or(MemoryError::MissingKey)?;
            } else {
                set_key(edit.edit_key())?;
                if !present {
                    count = checked_add(count, 1)?;
                }
            }
        }
        count
    } else {
        let mut count = 0usize;
        let mut counting = Merged::new(block, edits);
        while counting.next_key()?.is_some() {
            count = checked_add(count, 1)?;
        }
        count
    };
    let bytes = if held == 0 {
        let mut put_bytes = 0usize;
        for edit in edits {
            if !edit.deletes() {
                put_bytes = checked_add(put_bytes, checked_add(2, edit.edit_key().len())?)?;
            }
        }
        put_bytes
    } else {
        scaled(block.entries().len(), held, count)
    };
    let per = per_block(count, bytes);
    // Out as one block, its keys before the first edit stand as the block encodes them: they are
    // copied with their restarts, and only what follows is encoded again, byte for byte what
    // encoding them all would make. A key inserted into a block re-encoded every key the block
    // held, and counted them all first (2,078 ns a published key, `key_set::tests`).
    if held > 0
        && per >= count
        && let Some(first) = edits.first()
    {
        let (offset, index, previous) = block
            .before(first.edit_key())
            .ok_or(broken("a block's entries cannot be read"))?;
        encoder.resume(block, offset, index, &previous)?;
        let mut merged = Merged::from(block.cursor_from(offset, previous), edits);
        return encode_cut(&mut merged, per, encoder, source, out);
    }
    let mut merged = Merged::new(block, edits);
    encode_cut(&mut merged, per, encoder, source, out)
}

/// Joins each fresh block that is small (`Block::small`) with the block after it (or, the last,
/// the one before it), once: the two are re-encoded and cut evenly, and are not fresh after it.
fn join_small(
    blocks: &mut Vec<Seat<Block>>,
    fresh: &mut Vec<bool>,
    encoder: &mut Encoder,
    source: &mut Source<'_>,
) -> Result<(), MemoryError> {
    let mut index = 0usize;
    // Each turn moves past a block or joins two, which are not fresh after it: at most one
    // join a fresh block, so the turns are at most twice the blocks.
    while index < blocks.len() && blocks.len() > 1 {
        let small = fresh.get(index).copied().unwrap_or(false)
            && blocks.get(index).is_some_and(|(_, block)| block.small());
        if !small {
            index = index.saturating_add(1);
            continue;
        }
        let (left, right) = if index.saturating_add(1) < blocks.len() {
            (index, index.saturating_add(1))
        } else {
            (index.saturating_sub(1), index)
        };
        let mut joined: Vec<Seat<Block>> = Vec::new();
        joined
            .try_reserve_exact(4)
            .map_err(|_| MemoryError::AllocationFailed)?;
        {
            let first = &blocks
                .get(left)
                .ok_or(broken("a join passes its blocks"))?
                .1;
            let second = &blocks
                .get(right)
                .ok_or(broken("a join passes its blocks"))?
                .1;
            let count = checked_add(first.count(), second.count())?;
            let bytes = checked_add(first.entries().len(), second.entries().len())?;
            let mut keys = Joined {
                first: first.cursor(),
                second: second.cursor(),
                on_second: false,
            };
            encode_cut(
                &mut keys,
                per_block(count, bytes),
                encoder,
                source,
                &mut joined,
            )?;
        }
        let produced = joined.len();
        blocks
            .try_reserve(produced)
            .map_err(|_| MemoryError::AllocationFailed)?;
        fresh
            .try_reserve(produced)
            .map_err(|_| MemoryError::AllocationFailed)?;
        blocks.splice(left..=right, joined);
        fresh.splice(left..=right, std::iter::repeat_n(false, produced));
        index = left.saturating_add(produced);
    }
    Ok(())
}

/// One mid's chunks below `at` and from `at` on: the chunk `at` falls in divided, the others
/// shared.
fn split_mid(
    mid: &Mid,
    at: &[u8],
    encoder: &mut Encoder,
    source: &mut Source<'_>,
) -> Result<Halves<Chunk>, MemoryError> {
    let mut below = Vec::new();
    let mut above = Vec::new();
    below
        .try_reserve_exact(checked_add(mid.children.len(), 2)?)
        .map_err(|_| MemoryError::AllocationFailed)?;
    above
        .try_reserve_exact(checked_add(mid.children.len(), 2)?)
        .map_err(|_| MemoryError::AllocationFailed)?;
    let index = mid.child_for(at);
    for (position, seat) in mid.seats().enumerate() {
        match position.cmp(&index) {
            Ordering::Less => below.push(seat),
            Ordering::Greater => above.push(seat),
            Ordering::Equal => {
                let (low, high) = split_chunk(&seat.1, at, encoder, source)?;
                below.extend(group(&low, CHUNK_BLOCKS, source)?);
                above.extend(group(&high, CHUNK_BLOCKS, source)?);
            }
        }
    }
    Ok((below, above))
}

/// One chunk's blocks below `at` and from `at` on.
fn split_chunk(
    chunk: &Chunk,
    at: &[u8],
    encoder: &mut Encoder,
    source: &mut Source<'_>,
) -> Result<Halves<Block>, MemoryError> {
    let mut below = Vec::new();
    let mut above = Vec::new();
    below
        .try_reserve_exact(checked_add(chunk.children.len(), 1)?)
        .map_err(|_| MemoryError::AllocationFailed)?;
    above
        .try_reserve_exact(checked_add(chunk.children.len(), 1)?)
        .map_err(|_| MemoryError::AllocationFailed)?;
    let index = chunk.child_for(at);
    for (position, seat) in chunk.seats().enumerate() {
        match position.cmp(&index) {
            Ordering::Less => below.push(seat),
            Ordering::Greater => above.push(seat),
            Ordering::Equal => {
                let mut cursor = seat.1.cursor();
                let mut upper = false;
                while cursor.advance() {
                    if !upper && cursor.key() >= at {
                        if !encoder.is_empty() {
                            below.push(encoder.finish(source)?);
                        }
                        upper = true;
                    }
                    encoder.push(cursor.key())?;
                }
                if !encoder.is_empty() {
                    let built = encoder.finish(source)?;
                    if upper {
                        above.push(built);
                    } else {
                        below.push(built);
                    }
                }
            }
        }
    }
    Ok((below, above))
}

/// Builds a set from ascending keys, as a restore streams them.
pub struct KeySetBuilder {
    budget: MemoryBudget,
    lane: BudgetLane,
    encoder: Encoder,
    blocks: Vec<Seat<Block>>,
    chunks: Vec<Seat<Chunk>>,
    mids: Vec<Seat<Mid>>,
    /// The room `mids` holds while it grows, charged.
    room: Allocation,
}

impl KeySetBuilder {
    pub fn new(budget: &MemoryBudget, lane: BudgetLane) -> Result<Self, MemoryError> {
        let mut blocks = Vec::new();
        blocks
            .try_reserve_exact(CHUNK_BLOCKS)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(MID_CHUNKS)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let room = budget
            .reserve(
                BudgetKind::Pending,
                lane,
                checked_add(
                    Encoder::SCRATCH,
                    checked_add(node_charge(CHUNK_BLOCKS)?, node_charge(MID_CHUNKS)?)?,
                )?,
            )?
            .commit();
        Ok(Self {
            budget: budget.clone(),
            lane,
            encoder: Encoder::new()?,
            blocks,
            chunks,
            mids: Vec::new(),
            room,
        })
    }
    /// Appends `key`, which must follow every key pushed before it.
    pub fn push(&mut self, key: &[u8]) -> Result<(), MemoryError> {
        if self.encoder.full() {
            self.seal_block()?;
        }
        self.encoder.push(key)
    }
    fn seal_block(&mut self) -> Result<(), MemoryError> {
        let block = self
            .encoder
            .finish(&mut Source::Budget(&self.budget, self.lane))?;
        if self.blocks.len() >= CHUNK_BLOCKS {
            self.seal_chunk()?;
        }
        self.blocks.push(block);
        Ok(())
    }
    fn seal_chunk(&mut self) -> Result<(), MemoryError> {
        if self.blocks.is_empty() {
            return Ok(());
        }
        let chunk = Node::of(&self.blocks, &mut Source::Budget(&self.budget, self.lane))?;
        self.blocks.clear();
        if self.chunks.len() >= MID_CHUNKS {
            self.seal_mid()?;
        }
        let first = fence(chunk.first());
        self.chunks.push((first, Arc::new(chunk)));
        Ok(())
    }
    fn seal_mid(&mut self) -> Result<(), MemoryError> {
        if self.chunks.is_empty() {
            return Ok(());
        }
        let mid = Node::of(&self.chunks, &mut Source::Budget(&self.budget, self.lane))?;
        self.chunks.clear();
        if self.mids.len() >= self.mids.capacity() {
            let more = self.mids.capacity().max(8);
            self.room.grow(checked_mul(more, size_of::<Seat<Mid>>())?)?;
            self.mids
                .try_reserve_exact(more)
                .map_err(|_| MemoryError::AllocationFailed)?;
        }
        let first = fence(mid.first());
        self.mids.push((first, Arc::new(mid)));
        Ok(())
    }
    /// The set of every key pushed.
    pub fn finish(mut self) -> Result<KeySet, MemoryError> {
        if !self.encoder.is_empty() {
            self.seal_block()?;
        }
        self.seal_chunk()?;
        self.seal_mid()?;
        let root = Root::of(&self.mids, &mut Source::Budget(&self.budget, self.lane))?;
        Ok(KeySet {
            root,
            budget: self.budget,
            poisoned: false,
        })
    }
}

#[cfg(test)]
#[path = "key_set_tests.rs"]
mod tests;
