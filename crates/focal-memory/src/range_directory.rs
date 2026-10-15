//! Immutable, rank-indexed page directory. Only the edited paths and at most
//! one sibling per level are copied. Keys and values remain in the leaf pages;
//! separators borrow their descendants' minimum keys.
//!
//! A directory node has 16..=32 handles, except the root. Insertion splits 33
//! handles into 16 and 17; deletion borrows or merges before publishing. Each
//! node owns one charged vector and one charged Arc allocation. Small splice
//! descriptions stay on the stack, so there is no unaccounted merge buffer.
//!
//! A node is let go through [`release`], a level at a time: a branch's own
//! drop leaves its children ([`Links::Branches`]), since dropping them there
//! would make a node's drop call itself (CLAUDE.md §1a). Every handle on a
//! node outside a branch is [`Held`] or [`Gathered`], which let go so.

use super::{Page, bounded_push, bounded_vec};
use crate::{
    ALLOCATOR_OVERHEAD, Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError, checked_add,
    checked_mul,
};
use std::mem::ManuallyDrop;
use std::sync::Arc;

const MIN: usize = 16;
const MAX: usize = 32;
// Minimum fanout is a power of two. The smaller root and a transient split get
// two extra levels beyond the number needed to encode any usize page count.
// Heights are checked at construction, so iterator stacks cannot overflow.
const MAX_LEVELS: usize = usize::BITS.div_ceil(MIN.trailing_zeros()).saturating_add(2) as usize;

pub(crate) struct PageDirectory<K, V> {
    root: Held<K, V>,
}

struct Node<K, V> {
    // Owned buffers/children must drop before their accounting allocation.
    links: Links<K, V>,
    /// The order prefix of the node's first key, as [`Page::first`].
    first: u128,
    pages: usize,
    level: usize,
    _allocation: Allocation,
}

enum Links<K, V> {
    Leaves(Vec<Arc<Page<K, V>>>),
    /// Kept from the node's own drop and let go by [`release`].
    Branches(ManuallyDrop<Vec<Arc<Node<K, V>>>>),
}

/// A handle on a node, or on none (an empty directory, a split that did not
/// happen), let go through [`release`] when dropped.
struct Held<K, V>(Option<Arc<Node<K, V>>>);

impl<K, V> Held<K, V> {
    const fn none() -> Self {
        Self(None)
    }

    fn new(node: Node<K, V>) -> Self {
        Self(Some(Arc::new(node)))
    }

    /// Another handle on `node`.
    fn share(node: &Arc<Node<K, V>>) -> Self {
        Self(Some(Arc::clone(node)))
    }

    fn handle(&self) -> Option<&Arc<Node<K, V>>> {
        self.0.as_ref()
    }

    fn node(&self) -> Option<&Node<K, V>> {
        self.0.as_deref()
    }

    /// The node, to be a branch's child, which [`release`] lets go.
    fn into_child(mut self) -> Option<Arc<Node<K, V>>> {
        self.0.take()
    }
}

impl<K, V> Clone for Held<K, V> {
    fn clone(&self) -> Self {
        Self(self.0.as_ref().map(Arc::clone))
    }
}

impl<K, V> Drop for Held<K, V> {
    fn drop(&mut self) {
        if let Some(node) = self.0.take() {
            release(node);
        }
    }
}

/// Handles gathered to be a branch's children, or a level of a directory
/// being built: each let go through [`release`] unless they become children.
struct Gathered<K, V>(Vec<Arc<Node<K, V>>>);

impl<K, V> Gathered<K, V> {
    fn with_capacity(capacity: usize) -> Result<Self, MemoryError> {
        Ok(Self(bounded_vec(capacity)?))
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn get(&self, index: usize) -> Option<&Arc<Node<K, V>>> {
        self.0.get(index)
    }

    /// Takes `held`'s node: refused past `limit` or the capacity, `held`
    /// then let go as it was.
    fn push(&mut self, held: Held<K, V>, limit: usize) -> Result<(), MemoryError> {
        if self.0.len() >= limit || self.0.len() >= self.0.capacity() {
            return Err(MemoryError::Capacity {
                requested: checked_add(self.0.len(), 1)?,
                available: limit,
            });
        }
        if let Some(node) = held.into_child() {
            self.0.push(node);
        }
        Ok(())
    }

    fn pop(&mut self) -> Held<K, V> {
        Held(self.0.pop())
    }

    /// The handles, made a branch's children.
    fn into_children(mut self) -> ManuallyDrop<Vec<Arc<Node<K, V>>>> {
        ManuallyDrop::new(std::mem::take(&mut self.0))
    }
}

impl<K, V> Drop for Gathered<K, V> {
    fn drop(&mut self) {
        for node in std::mem::take(&mut self.0) {
            release(node);
        }
    }
}

/// What [`release`] holds of a branch it let go: the children it has not yet
/// let go, and the branch's charge, given back after their handles' buffer.
type Unwinding<K, V> = (std::vec::IntoIter<Arc<Node<K, V>>>, Allocation);

/// Lets `node` go and, where its handle was the last, its children, theirs
/// and so on: a loop over the branches being let go, one a level and at most
/// [`MAX_LEVELS`] (a branch's children are one level below it, and none is
/// built deeper), each with the children it has left. A node another
/// directory shares only loses a handle here (`Arc::into_inner`, which gives
/// the node to exactly one of the handles let go at once).
fn release<K, V>(node: Arc<Node<K, V>>) {
    // Most handles let go are shared (an edit's copy replacing a node) or a
    // leaf's: those end here, before the unwinding stack is laid out.
    let Some(Node {
        links: Links::Branches(children),
        _allocation: allocation,
        ..
    }) = Arc::into_inner(node)
    else {
        return;
    };
    let mut unwinding: [Option<Unwinding<K, V>>; MAX_LEVELS] = std::array::from_fn(|_| None);
    if let Some(frame) = unwinding.first_mut() {
        *frame = Some((ManuallyDrop::into_inner(children).into_iter(), allocation));
    }
    let mut depth = 1usize;
    let mut next = None;
    loop {
        if let Some(node) = next.take()
            && let Some(Node {
                links: Links::Branches(children),
                _allocation: allocation,
                ..
            }) = Arc::into_inner(node)
        {
            let children = ManuallyDrop::into_inner(children).into_iter();
            // A branch's level is below MAX_LEVELS (`branch`) and each frame is
            // a level below the last, so a frame is always free here.
            if let Some(frame) = unwinding.get_mut(depth) {
                *frame = Some((children, allocation));
                depth = depth.saturating_add(1);
            }
        }
        // The next child of the deepest branch still being let go.
        loop {
            let Some(top) = depth.checked_sub(1) else {
                return;
            };
            let Some(Some((children, _))) = unwinding.get_mut(top) else {
                return;
            };
            if let Some(child) = children.next() {
                next = Some(child);
                break;
            }
            if let Some(frame) = unwinding.get_mut(top) {
                drop(frame.take());
            }
            depth = top;
        }
    }
}

/// One preparation's cumulative issued-allocation bound. Dropped intermediate
/// nodes refund the source, but do not replenish this conservative quote.
pub(super) struct DirectoryBuild<'a> {
    source: BuildSource<'a>,
    lane: BudgetLane,
    remaining: usize,
}

/// Where a build's nodes are charged: reserved from a budget as they are made,
/// or taken from a charge already held (a publication in place spends what its
/// preparation reserved, so it never asks the budget, doc 30 §9).
enum BuildSource<'a> {
    Budget(&'a MemoryBudget),
    Held(&'a mut Allocation),
}

impl<'a> DirectoryBuild<'a> {
    pub(super) fn new(source: &'a MemoryBudget, lane: BudgetLane, byte_bound: usize) -> Self {
        Self {
            source: BuildSource::Budget(source),
            lane,
            remaining: byte_bound,
        }
    }

    /// A build that spends `held`, a `Roots` charge, and nothing past it.
    pub(super) fn held(held: &'a mut Allocation, lane: BudgetLane) -> Self {
        let remaining = held.bytes();
        Self {
            source: BuildSource::Held(held),
            lane,
            remaining,
        }
    }

    fn allocate(&mut self, bytes: usize) -> Result<Allocation, MemoryError> {
        let remaining = self
            .remaining
            .checked_sub(bytes)
            .ok_or(MemoryError::Capacity {
                requested: bytes,
                available: self.remaining,
            })?;
        let allocation = match &mut self.source {
            BuildSource::Budget(source) => source
                .reserve(BudgetKind::Roots, self.lane, bytes)?
                .commit(),
            BuildSource::Held(held) => held.split_off(bytes)?,
        };
        self.remaining = remaining;
        Ok(allocation)
    }

    /// A build's temporary workspace, charged where its nodes are and outside
    /// their bound.
    fn staging(&mut self, bytes: usize) -> Result<Allocation, MemoryError> {
        match &mut self.source {
            BuildSource::Budget(source) => Ok(source
                .reserve(BudgetKind::Roots, self.lane, bytes)?
                .commit()),
            BuildSource::Held(held) => held.split_off(bytes),
        }
    }
}

impl<K, V> PageDirectory<K, V> {
    pub(super) fn new() -> Self {
        Self { root: Held::none() }
    }

    pub(super) fn shared(&self) -> Self {
        Self {
            root: self.root.clone(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.root.node().map_or(0, |node| node.pages)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.root.node().is_none()
    }

    pub(super) fn get(&self, mut index: usize) -> Option<&Arc<Page<K, V>>> {
        let mut node = self.root.node()?;
        if index >= node.pages {
            return None;
        }
        loop {
            match &node.links {
                Links::Leaves(pages) => return pages.get(index),
                Links::Branches(children) => {
                    let (child, offset) = child_at(children, index, false).ok()?;
                    node = children.get(child)?.as_ref();
                    index = offset;
                }
            }
        }
    }

    pub(super) fn last(&self) -> Option<&Arc<Page<K, V>>> {
        self.get(self.len().checked_sub(1)?)
    }

    pub(super) fn iter(&self) -> Iter<'_, K, V> {
        self.from(0)
    }

    pub(super) fn from(&self, index: usize) -> Iter<'_, K, V> {
        Iter::new(self.root.node(), index)
    }

    /// Total additional bytes that may be issued over all directory edits.
    /// Existing/shared directory roots remain separately charged. The caller
    /// supplies the maximum page count at any intermediate point, not just the
    /// final count. This is a conservative sum, not simultaneous retained size.
    ///
    /// Insertion constructs at most two nodes per level plus one new root.
    /// Deletion constructs at most four per level: the recursive replacement,
    /// up to two redistributed siblings, and their parent. Six per level plus
    /// two root nodes also covers replacement and temporary underfull nodes.
    /// There are no heap-allocated temporary splice buffers.
    pub(super) fn edit_bound(edits: usize, max_leaf_count: usize) -> Result<usize, MemoryError> {
        if edits == 0 {
            return Ok(0);
        }
        let levels = Self::levels(max_leaf_count)?;
        let nodes = checked_add(checked_mul(6, levels)?, 2)?;
        checked_mul(edits, checked_mul(nodes, node_charge::<K, V>(MAX)?)?)
    }

    /// What a publication in place of `changed` changes may spend on nodes,
    /// the directory holding at most `max_leaf_count` pages as it publishes
    /// (doc 30 §9). A change's page has its path made the directory's own, a
    /// node a level where a capture still shares it. A put inserts at most two
    /// pages (`insert_owned`: a path a level and its splits, and a root, where
    /// its leaf is full); a delete removes at most one (`remove_owned`: four a
    /// level and two roots). A change is one or the other: a page is removed
    /// only where its group deleted every row, which no put leaves, so a
    /// change pays its path and the larger of the two, never both. Every other
    /// edit is made where it stands.
    pub(super) fn sparse_bound(
        changed: usize,
        max_leaf_count: usize,
    ) -> Result<usize, MemoryError> {
        if changed == 0 {
            return Ok(0);
        }
        let levels = Self::levels(max_leaf_count)?;
        let owned = levels;
        let inserted = checked_mul(2, checked_add(checked_mul(2, levels)?, 1)?)?;
        let removed = checked_add(checked_mul(4, levels)?, 2)?;
        let nodes = checked_add(owned, inserted.max(removed))?;
        checked_mul(changed, checked_mul(nodes, node_charge::<K, V>(MAX)?)?)
    }

    /// The most levels a directory of `max_leaf_count` pages has: internal
    /// occupancy is at least MIN, and an extra level accounts for the smaller
    /// root and for a split during an edit.
    fn levels(max_leaf_count: usize) -> Result<usize, MemoryError> {
        let mut pages = max_leaf_count.max(1);
        let mut levels = 2usize;
        while pages > 1 {
            pages = pages.div_ceil(MIN);
            levels = checked_add(levels, 1)?;
        }
        if levels > MAX_LEVELS {
            return Err(invalid("directory height exceeds supported bound"));
        }
        Ok(levels)
    }

    #[cfg(test)]
    pub(super) fn height(&self) -> usize {
        self.root
            .node()
            .map_or(0, |root| root.level.saturating_add(1))
    }
}

/// Whether a span beginning with prefix `first` and key `start` begins at or
/// before the sought `key`, whose prefix is `probe`: the prefixes decide where
/// they differ, and where they tie the keys do, compared past the prefix
/// (`RangeKey::cmp_past_prefix`). The span's first key is read only where the
/// prefixes tie: a node's is at the end of its leftmost path.
fn at_or_before<'a, K: crate::RangeKey + 'a>(
    first: u128,
    start: impl FnOnce() -> Option<&'a K>,
    probe: u128,
    key: &K,
) -> bool {
    first < probe
        || (first == probe && start().is_some_and(|start| start.cmp_past_prefix(key).is_le()))
}

/// The page a key's span is in, found by [`PageDirectory::locate`]: its rank,
/// the page, and the page after it.
pub(super) struct Located<'a, K, V> {
    pub rank: usize,
    pub page: &'a Arc<Page<K, V>>,
    pub next: Option<&'a Arc<Page<K, V>>>,
}

impl<K: crate::RangeKey, V> PageDirectory<K, V> {
    /// The page whose span holds `key`, found by one descent: what a lookup
    /// reads. `page_index` gives the page's rank, which costs a sum of the
    /// pages left of the path at every level, and `get` finds a page by rank by
    /// walking down again: a lookup by key did both (a 2026-10-11 profile of
    /// the comparison's leader: a quarter of the owner's CPU in the search).
    pub(super) fn page_of(&self, key: &K) -> Option<&Arc<Page<K, V>>> {
        let mut node = self.root.node()?;
        let probe = key.order_prefix();
        loop {
            match &node.links {
                Links::Leaves(pages) => {
                    let local = pages
                        .partition_point(|page| {
                            at_or_before(
                                page.first,
                                || page.entries.first().map(|entry| &entry.key),
                                probe,
                                key,
                            )
                        })
                        .saturating_sub(1);
                    return pages.get(local);
                }
                Links::Branches(children) => {
                    let local = children
                        .partition_point(|child| {
                            at_or_before(child.first, || child.first_key(), probe, key)
                        })
                        .saturating_sub(1);
                    node = children.get(local)?.as_ref();
                }
            }
        }
    }

    /// The rank of the page whose span holds `key` ([`Self::locate`]).
    pub(super) fn page_index(&self, key: &K) -> usize {
        self.locate(key).map_or(0, |located| located.rank)
    }

    /// The page whose span holds `key`, its rank and the page after it, by
    /// one descent: what grouping a batch's changes by page reads. The
    /// prefixes cached beside pages and nodes decide every comparison where
    /// they differ; where they tie, the keys do. The sought key's prefix is
    /// computed once. The page after is the next in the leaf reached, or past
    /// its end the first of the subtree after the path at the deepest level
    /// that has one. The grouping found the page's rank, the page by rank, and
    /// the page after by rank again: three descents a group, each summing the
    /// pages left of its path at every level.
    pub(super) fn locate(&self, key: &K) -> Option<Located<'_, K, V>> {
        let mut node = self.root.node()?;
        let probe = key.order_prefix();
        let mut rank = 0usize;
        let mut after: Option<&Node<K, V>> = None;
        loop {
            match &node.links {
                Links::Leaves(pages) => {
                    let local = pages
                        .partition_point(|page| {
                            at_or_before(
                                page.first,
                                || page.entries.first().map(|entry| &entry.key),
                                probe,
                                key,
                            )
                        })
                        .saturating_sub(1);
                    let next = match local.checked_add(1).and_then(|next| pages.get(next)) {
                        Some(next) => Some(next),
                        None => after.and_then(Node::first_page),
                    };
                    return Some(Located {
                        rank: rank.saturating_add(local),
                        page: pages.get(local)?,
                        next,
                    });
                }
                Links::Branches(children) => {
                    let local = children
                        .partition_point(|child| {
                            at_or_before(child.first, || child.first_key(), probe, key)
                        })
                        .saturating_sub(1);
                    for child in children.iter().take(local) {
                        rank = rank.saturating_add(child.pages);
                    }
                    if let Some(sibling) = local.checked_add(1).and_then(|next| children.get(next))
                    {
                        after = Some(sibling);
                    }
                    node = children.get(local)?;
                }
            }
        }
    }

    pub(super) fn insert(
        &self,
        index: usize,
        page: Arc<Page<K, V>>,
        build: &mut DirectoryBuild<'_>,
    ) -> Result<Self, MemoryError> {
        if index > self.len() {
            return Err(MemoryError::MissingKey);
        }
        self.check_page(index, false, &page)?;
        let Some(root) = self.root.node() else {
            let node = leaf(1, |_| Some(Arc::clone(&page)), |_| true, build)?;
            return Ok(Self { root: node });
        };
        let (left, right) = insert_node(root, index, page, build)?;
        let root = if right.handle().is_none() {
            left
        } else {
            branch(
                2,
                |index| {
                    match index {
                        0 => left.handle(),
                        1 => right.handle(),
                        _ => None,
                    }
                    .map(Held::share)
                },
                |_| true,
                build,
            )?
        };
        Ok(Self { root })
    }

    /// A directory that is this one with the page at `index` replaced: a
    /// shared copy, edited by [`Self::replace_owned`].
    #[cfg(test)]
    pub(super) fn replace(
        &self,
        index: usize,
        page: Arc<Page<K, V>>,
        build: &mut DirectoryBuild<'_>,
    ) -> Result<Self, MemoryError> {
        let mut next = self.shared();
        next.replace_owned(index, page, build)?;
        Ok(next)
    }

    /// Replaces the page at `index`. A node this directory shares with
    /// another is copied the first time an edit passes it ([`owned`]) and
    /// changed where it is after that, so a preparation that edits many
    /// pages copies each node on their paths once (Rodeh 2008, "B-trees,
    /// shadowing, and clones") where it copied every node of every path and
    /// let go of the copies the next edit replaced.
    pub(super) fn replace_owned(
        &mut self,
        index: usize,
        page: Arc<Page<K, V>>,
        build: &mut DirectoryBuild<'_>,
    ) -> Result<(), MemoryError> {
        if index >= self.len() {
            return Err(MemoryError::MissingKey);
        }
        let route = self.route(index, false, &page)?;
        self.own(&route, false, build)?;
        let first = page.first;
        self.apply(&route, false, first, |pages, rank| {
            let slot = pages.get_mut(rank).ok_or(MemoryError::MissingKey)?;
            drop(std::mem::replace(slot, page));
            Ok(())
        })
    }

    /// Inserts `page` at `index` as [`Self::replace_owned`] replaces one,
    /// where the leaf it goes in has room; one that would split, or an empty
    /// directory, takes the edit that copies its path ([`Self::insert`]).
    pub(super) fn insert_owned(
        &mut self,
        index: usize,
        page: Arc<Page<K, V>>,
        build: &mut DirectoryBuild<'_>,
    ) -> Result<(), MemoryError> {
        if index > self.len() {
            return Err(MemoryError::MissingKey);
        }
        let route = match self.root.node() {
            Some(_) => Some(self.route(index, true, &page)?),
            None => None,
        };
        let Some(route) = route.filter(|route| route.width < MAX) else {
            *self = self.insert(index, page, build)?;
            return Ok(());
        };
        // No node counts more pages than the root.
        checked_add(self.len(), 1)?;
        self.own(&route, true, build)?;
        let first = page.first;
        self.apply(&route, true, first, |pages, rank| {
            if rank > pages.len() || pages.len() >= pages.capacity() {
                return Err(invalid("directory leaf has no room for an insert"));
            }
            pages.insert(rank, page);
            Ok(())
        })
    }

    /// Edits the page at `index` where it stands (doc 30 §9): the nodes on its
    /// path become this directory's own ([`Self::own`]), `edit` is given the
    /// page's handle (the page itself is the edit's to make its own or leave),
    /// and each node whose first page this is takes the page's first prefix
    /// as the edit left it. The page count is unchanged; an edit that splits a
    /// page inserts its new pages after it ([`Self::insert_owned`]).
    pub(super) fn edit_page<R>(
        &mut self,
        index: usize,
        build: &mut DirectoryBuild<'_>,
        edit: impl FnOnce(&mut Arc<Page<K, V>>) -> Result<R, MemoryError>,
    ) -> Result<R, MemoryError> {
        // The page is already where it stands among its neighbours, and an
        // edit keeps it in its own span: nothing to check of its order.
        let route = self.route_in_place(index)?;
        self.own(&route, false, build)?;
        let mut handle = self.root.0.as_mut().ok_or(MemoryError::MissingKey)?;
        let mut level = 0usize;
        let (result, first) = loop {
            let node = Arc::get_mut(handle).ok_or(invalid("an edited directory node is shared"))?;
            match &mut node.links {
                Links::Branches(children) => {
                    let child = *route
                        .children
                        .get(level)
                        .filter(|_| level < route.depth)
                        .ok_or(invalid("directory deeper than an edit's route"))?;
                    handle = children.get_mut(child).ok_or(MemoryError::MissingKey)?;
                    level = level.saturating_add(1);
                }
                Links::Leaves(pages) => {
                    let slot = pages.get_mut(route.rank).ok_or(MemoryError::MissingKey)?;
                    let result = edit(slot)?;
                    break (result, slot.first);
                }
            }
        };
        self.apply(&route, false, first, |_, _| Ok(()))?;
        Ok(result)
    }

    /// The handle of the page at `index` where every node on its path is this
    /// directory's own, nothing copied or changed; none where something else
    /// still holds one of them (a frozen image, a lease).
    pub(super) fn page_owned_mut(
        &mut self,
        index: usize,
    ) -> Result<Option<&mut Arc<Page<K, V>>>, MemoryError> {
        let route = self.route_in_place(index)?;
        let mut handle = self.root.0.as_mut().ok_or(MemoryError::MissingKey)?;
        for level in 0..=MAX_LEVELS {
            let Some(node) = Arc::get_mut(handle) else {
                return Ok(None);
            };
            match &mut node.links {
                Links::Branches(children) => {
                    let child = *route
                        .children
                        .get(level)
                        .filter(|_| level < route.depth)
                        .ok_or(invalid("directory deeper than an edit's route"))?;
                    handle = children.get_mut(child).ok_or(MemoryError::MissingKey)?;
                }
                Links::Leaves(pages) => {
                    return pages
                        .get_mut(route.rank)
                        .map(Some)
                        .ok_or(MemoryError::MissingKey);
                }
            }
        }
        Err(invalid("directory deeper than its levels"))
    }

    /// Removes the page at `index`, by the edit that copies its path
    /// ([`Self::remove`]): a removal can merge leaves, and one is rare.
    pub(super) fn remove_owned(
        &mut self,
        index: usize,
        build: &mut DirectoryBuild<'_>,
    ) -> Result<(), MemoryError> {
        *self = self.remove(index, build)?;
        Ok(())
    }

    /// Where an edit of the page at `index` goes (with `insertion`, of a page
    /// put at `index`), read before anything changes, `page` checked against
    /// the pages it will sit between: its neighbours in the leaf reached, or
    /// past the leaf's ends, those beside it in the directory.
    fn route(
        &self,
        index: usize,
        insertion: bool,
        page: &Page<K, V>,
    ) -> Result<Route, MemoryError> {
        let root = self.root.node().ok_or(MemoryError::MissingKey)?;
        let (descent, pages, rank) = Descent::to_leaf(root, index, insertion)?;
        let left = match rank.checked_sub(1) {
            Some(local) => pages.get(local),
            None => index.checked_sub(1).and_then(|index| self.get(index)),
        };
        let (after, next) = if insertion {
            (rank, index)
        } else {
            (checked_add(rank, 1)?, checked_add(index, 1)?)
        };
        let right = match pages.get(after) {
            Some(page) => Some(page),
            None => self.get(next),
        };
        check_between(left.map(AsRef::as_ref), page, right.map(AsRef::as_ref))?;
        Ok(Self::route_of(&descent, rank, pages.len()))
    }

    /// Where the edit of the page at `index` goes, the page already among its
    /// neighbours: an edit where it stands keeps it there ([`Self::edit_page`]).
    fn route_in_place(&self, index: usize) -> Result<Route, MemoryError> {
        let root = self.root.node().ok_or(MemoryError::MissingKey)?;
        let (descent, pages, rank) = Descent::to_leaf(root, index, false)?;
        Ok(Self::route_of(&descent, rank, pages.len()))
    }

    fn route_of(descent: &Descent<'_, K, V>, rank: usize, width: usize) -> Route {
        let mut children = [0usize; MAX_LEVELS];
        for (slot, &(_, child)) in children
            .iter_mut()
            .zip(descent.steps.iter().take(descent.depth))
        {
            *slot = child;
        }
        let mut leftmost_from = descent.depth;
        while let Some(level) = leftmost_from.checked_sub(1) {
            if children.get(level) != Some(&0) {
                break;
            }
            leftmost_from = level;
        }
        Route {
            children,
            depth: descent.depth,
            rank,
            width,
            leftmost_from,
        }
    }

    /// Makes each node on `route` this directory's own ([`owned`]) and, with
    /// `grow`, gives its leaf room for one more page: what may be refused is
    /// refused here, a copy holding what the node it replaced held.
    fn own(
        &mut self,
        route: &Route,
        grow: bool,
        build: &mut DirectoryBuild<'_>,
    ) -> Result<(), MemoryError> {
        let mut handle = self.root.0.as_mut().ok_or(MemoryError::MissingKey)?;
        let mut level = 0usize;
        loop {
            let Node {
                links,
                _allocation: allocation,
                ..
            } = owned(handle, build)?;
            match links {
                Links::Branches(children) => {
                    let child = *route
                        .children
                        .get(level)
                        .filter(|_| level < route.depth)
                        .ok_or(invalid("directory deeper than an edit's route"))?;
                    handle = children.get_mut(child).ok_or(MemoryError::MissingKey)?;
                    level = level.saturating_add(1);
                }
                Links::Leaves(pages) => {
                    if grow && pages.len() >= pages.capacity() {
                        make_room(pages, allocation, build)?;
                    }
                    return Ok(());
                }
            }
        }
    }

    /// Applies an edit along `route`, whose nodes are this directory's own
    /// ([`Self::own`]): where `added`, each node counts one more page; a node
    /// whose first page the edit changes takes `first`; the leaf takes `edit`.
    fn apply(
        &mut self,
        route: &Route,
        added: bool,
        first: u128,
        edit: impl FnOnce(&mut Vec<Arc<Page<K, V>>>, usize) -> Result<(), MemoryError>,
    ) -> Result<(), MemoryError> {
        let mut handle = self.root.0.as_mut().ok_or(MemoryError::MissingKey)?;
        let mut level = 0usize;
        loop {
            let node = Arc::get_mut(handle).ok_or(invalid("an edited directory node is shared"))?;
            if added {
                node.pages = checked_add(node.pages, 1)?;
            }
            if route.rank == 0 && level >= route.leftmost_from {
                node.first = first;
            }
            match &mut node.links {
                Links::Branches(children) => {
                    let child = *route
                        .children
                        .get(level)
                        .filter(|_| level < route.depth)
                        .ok_or(invalid("directory deeper than an edit's route"))?;
                    handle = children.get_mut(child).ok_or(MemoryError::MissingKey)?;
                    level = level.saturating_add(1);
                }
                Links::Leaves(pages) => return edit(pages, route.rank),
            }
        }
    }

    pub(super) fn remove(
        &self,
        index: usize,
        build: &mut DirectoryBuild<'_>,
    ) -> Result<Self, MemoryError> {
        let root = self.root.node().ok_or(MemoryError::MissingKey)?;
        if index >= root.pages {
            return Err(MemoryError::MissingKey);
        }
        let mut root = remove_node(root, index, build)?;
        // A one-child root carries no information and need not be retained.
        loop {
            let only = match root.node().map(|node| &node.links) {
                Some(Links::Branches(children)) if children.len() == 1 => {
                    children.first().map(Held::share)
                }
                _ => None,
            };
            let Some(only) = only else {
                break;
            };
            root = only;
        }
        Ok(Self { root })
    }

    fn check_page(
        &self,
        index: usize,
        replacing: bool,
        page: &Page<K, V>,
    ) -> Result<(), MemoryError> {
        let left = index.checked_sub(1).and_then(|index| self.get(index));
        let next = if replacing {
            checked_add(index, 1)?
        } else {
            index
        };
        check_between(
            left.map(AsRef::as_ref),
            page,
            self.get(next).map(AsRef::as_ref),
        )
    }

    #[cfg(test)]
    pub(super) fn check_shape(&self) -> bool {
        self.root.node().is_none_or(|root| audit(root, true))
    }

    /// The bytes [`Self::from_pages`] may issue for `count` pages: one node
    /// per group of at most MAX handles at every level, each charged at full
    /// width, plus the two transient handle vectors of the widest level.
    pub(super) fn build_bound(count: usize) -> Result<usize, MemoryError> {
        if count == 0 {
            return Ok(0);
        }
        let mut nodes = 0usize;
        let mut width = count;
        let mut levels = 0usize;
        loop {
            let groups = width.div_ceil(MAX).max(1);
            nodes = checked_add(nodes, groups)?;
            levels = checked_add(levels, 1)?;
            if groups == 1 {
                break;
            }
            if levels > MAX_LEVELS {
                return Err(invalid("directory height exceeds supported bound"));
            }
            width = groups;
        }
        let handles = checked_add(
            ALLOCATOR_OVERHEAD,
            checked_mul(count.div_ceil(MAX).max(1), size_of::<Arc<Node<K, V>>>())?,
        )?;
        checked_add(
            checked_mul(nodes, node_charge::<K, V>(MAX)?)?,
            checked_mul(2, handles)?,
        )
    }

    /// Build a directory over `count` ordered pages in one pass: leaves of at
    /// most MAX handles and, below the root, at least MIN, so the result has
    /// the shape a sequence of inserts reaches with one allocation per node
    /// instead of one path copy per page. `item(index)` yields the page at
    /// `index`; a missing page or a disordered neighbour refuses the build.
    pub(super) fn from_pages(
        count: usize,
        mut item: impl FnMut(usize) -> Option<Arc<Page<K, V>>>,
        build: &mut DirectoryBuild<'_>,
    ) -> Result<Self, MemoryError> {
        if count == 0 {
            return Ok(Self::new());
        }
        let groups = count.div_ceil(MAX);
        let _staging = build.staging(checked_mul(
            2,
            checked_add(
                ALLOCATOR_OVERHEAD,
                checked_mul(groups, size_of::<Arc<Node<K, V>>>())?,
            )?,
        )?)?;
        let mut level = Gathered::with_capacity(groups)?;
        let mut offset = 0usize;
        for group in 0..groups {
            let width = group_width(count, groups, group);
            let node = leaf(
                width,
                |index| item(checked_add(offset, index).ok()?),
                |_| true,
                build,
            )?;
            level.push(node, groups)?;
            offset = checked_add(offset, width)?;
        }
        while level.len() > 1 {
            let groups = level.len().div_ceil(MAX);
            let mut next = Gathered::with_capacity(groups)?;
            let mut offset = 0usize;
            for group in 0..groups {
                let width = group_width(level.len(), groups, group);
                let node = branch(
                    width,
                    |index| level.get(checked_add(offset, index).ok()?).map(Held::share),
                    |_| true,
                    build,
                )?;
                next.push(node, groups)?;
                offset = checked_add(offset, width)?;
            }
            level = next;
        }
        Ok(Self { root: level.pop() })
    }
}

/// Divide `total` handles into `groups` nearly equal widths: every group is
/// at least `total / groups`, so with more than one group each holds at
/// least MIN when `total` exceeds MAX.
fn group_width(total: usize, groups: usize, group: usize) -> usize {
    let base = total.checked_div(groups).unwrap_or(0);
    let extra = total.checked_rem(groups).unwrap_or(0);
    base.saturating_add(usize::from(group < extra))
}

impl<K, V> Node<K, V> {
    fn width(&self) -> usize {
        match &self.links {
            Links::Leaves(pages) => pages.len(),
            Links::Branches(children) => children.len(),
        }
    }

    fn first_key(&self) -> Option<&K> {
        Some(&self.first_page()?.entries.first()?.key)
    }

    fn first_page(&self) -> Option<&Arc<Page<K, V>>> {
        let mut node = self;
        loop {
            match &node.links {
                Links::Leaves(pages) => return pages.first(),
                Links::Branches(children) => node = children.first()?,
            }
        }
    }

    fn last_key(&self) -> Option<&K> {
        let mut node = self;
        loop {
            match &node.links {
                Links::Leaves(pages) => return Some(&pages.last()?.entries.last()?.key),
                Links::Branches(children) => node = children.last()?,
            }
        }
    }
}

fn invalid(message: &'static str) -> MemoryError {
    MemoryError::InvalidConfiguration(message)
}

fn node_charge<K, V>(width: usize) -> Result<usize, MemoryError> {
    let node = checked_add(size_of::<Node<K, V>>(), checked_mul(2, size_of::<usize>())?)?;
    let handles = checked_mul(width, size_of::<Arc<Page<K, V>>>())?;
    checked_add(
        checked_add(node, handles)?,
        checked_mul(2, ALLOCATOR_OVERHEAD)?,
    )
}

/// Refuses `page` unless its keys rise strictly, its first after `left`'s
/// last and its last before `right`'s first.
fn check_between<K: crate::RangeKey, V>(
    left: Option<&Page<K, V>>,
    page: &Page<K, V>,
    right: Option<&Page<K, V>>,
) -> Result<(), MemoryError> {
    let first = page
        .entries
        .first()
        .ok_or(invalid("empty directory page"))?;
    let last = page.entries.last().ok_or(invalid("empty directory page"))?;
    let mut previous = None;
    for entry in &page.entries {
        if previous.is_some_and(|key| key >= &entry.key) {
            return Err(MemoryError::InvalidNeighbors);
        }
        previous = Some(&entry.key);
    }
    if left
        .and_then(|page| page.entries.last())
        .is_some_and(|entry| entry.key >= first.key)
        || right
            .and_then(|page| page.entries.first())
            .is_some_and(|entry| entry.key <= last.key)
    {
        return Err(MemoryError::InvalidNeighbors);
    }
    Ok(())
}

/// Where an edit goes, read before anything changes: the child taken at each
/// branch from the root down, the rank within the leaf reached, that leaf's
/// width, and the level from which every child taken was the first (where
/// the rank is 0, the edited page is first in each of those subtrees).
struct Route {
    children: [usize; MAX_LEVELS],
    depth: usize,
    rank: usize,
    width: usize,
    leftmost_from: usize,
}

/// The node `handle` holds, made its directory's own: one no other handle
/// holds is changed where it is; one another directory shares is copied
/// ([`shadow`]) and the copy put in its place.
fn owned<'a, K, V>(
    handle: &'a mut Arc<Node<K, V>>,
    build: &mut DirectoryBuild<'_>,
) -> Result<&'a mut Node<K, V>, MemoryError> {
    if Arc::get_mut(handle).is_none() {
        let copy = shadow(handle, build)?;
        release(std::mem::replace(handle, copy));
    }
    Arc::get_mut(handle).ok_or(invalid("a directory node made its own is shared"))
}

/// A copy of `node` for a directory to change: its pages or children, each
/// handle shared, charged at its width.
fn shadow<K, V>(
    node: &Node<K, V>,
    build: &mut DirectoryBuild<'_>,
) -> Result<Arc<Node<K, V>>, MemoryError> {
    let allocation = build.allocate(node_charge::<K, V>(node.width())?)?;
    let links = match &node.links {
        Links::Leaves(pages) => {
            let mut copy = bounded_vec(pages.len())?;
            copy.extend(pages.iter().map(Arc::clone));
            Links::Leaves(copy)
        }
        Links::Branches(children) => {
            let mut copy = Gathered::with_capacity(children.len())?;
            copy.0.extend(children.iter().map(Arc::clone));
            Links::Branches(copy.into_children())
        }
    };
    Ok(Arc::new(Node {
        links,
        first: node.first,
        pages: node.pages,
        level: node.level,
        _allocation: allocation,
    }))
}

/// Room in a leaf for one more page, its handle charged to the node. A node
/// an earlier write charged under another lane or pool (a publication in
/// place keeps the nodes the directory owns) moves its whole charge to this
/// build, as a copy of it would: its old charge goes back where it came from.
fn make_room<K, V>(
    pages: &mut Vec<Arc<Page<K, V>>>,
    allocation: &mut Allocation,
    build: &mut DirectoryBuild<'_>,
) -> Result<(), MemoryError> {
    let before = pages.capacity();
    let mut handle = build.allocate(size_of::<Arc<Page<K, V>>>())?;
    if allocation.absorb(&mut handle).is_err() {
        let mut whole = build.allocate(allocation.bytes())?;
        whole.absorb(&mut handle)?;
        drop(std::mem::replace(allocation, whole));
    }
    #[cfg(test)]
    let additional = super::preflight_tests::allocation_capacity(1)?;
    #[cfg(not(test))]
    let additional = 1;
    pages
        .try_reserve_exact(additional)
        .map_err(|_| MemoryError::AllocationFailed)?;
    // What is charged is one handle more: a larger grant is refused, as
    // `bounded_vec` refuses one.
    if pages.capacity() > checked_add(before, 1)? {
        return Err(MemoryError::Capacity {
            requested: pages.capacity(),
            available: checked_add(before, 1)?,
        });
    }
    Ok(())
}

/// A node of `count` pages. Their order is checked where `seam` says the
/// sequence is new: everywhere for a node built whole, and only around the
/// replaced span for a splice of a node whose order was checked when it was
/// built (a page replaced in a node of 64 is checked against its two
/// neighbours, not the 63 pairs that did not change).
fn leaf<K: crate::RangeKey, V>(
    count: usize,
    mut item: impl FnMut(usize) -> Option<Arc<Page<K, V>>>,
    seam: impl Fn(usize) -> bool,
    build: &mut DirectoryBuild<'_>,
) -> Result<Held<K, V>, MemoryError> {
    if count == 0 || count > MAX {
        return Err(invalid("invalid directory leaf width"));
    }
    let allocation = build.allocate(node_charge::<K, V>(count)?)?;
    let mut pages: Vec<Arc<Page<K, V>>> = bounded_vec(count)?;
    for index in 0..count {
        let page = item(index).ok_or(invalid("missing directory page"))?;
        let first = page
            .entries
            .first()
            .ok_or(invalid("empty directory page"))?;
        if seam(index)
            && pages
                .last()
                .and_then(|previous| previous.entries.last())
                .is_some_and(|previous| previous.key >= first.key)
        {
            return Err(MemoryError::InvalidNeighbors);
        }
        bounded_push(&mut pages, page, count)?;
    }
    let first = pages.first().map_or(0, |page| page.first);
    Ok(Held::new(Node {
        links: Links::Leaves(pages),
        first,
        pages: count,
        level: 0,
        _allocation: allocation,
    }))
}

/// A node of `count` children, their heights and counts checked; their
/// order where `seam` says the sequence is new, as for [`leaf`].
fn branch<K: crate::RangeKey, V>(
    count: usize,
    mut item: impl FnMut(usize) -> Option<Held<K, V>>,
    seam: impl Fn(usize) -> bool,
    build: &mut DirectoryBuild<'_>,
) -> Result<Held<K, V>, MemoryError> {
    if count == 0 || count > MAX {
        return Err(invalid("invalid directory branch width"));
    }
    let allocation = build.allocate(node_charge::<K, V>(count)?)?;
    let mut children = Gathered::with_capacity(count)?;
    let mut pages = 0usize;
    let mut level = None;
    for index in 0..count {
        let held = item(index).ok_or(invalid("missing directory child"))?;
        let child = held.node().ok_or(invalid("missing directory child"))?;
        let child_level = checked_add(child.level, 1)?;
        if child.pages == 0
            || child_level >= MAX_LEVELS
            || level.is_some_and(|level| child_level != level)
        {
            return Err(invalid("invalid directory child height or count"));
        }
        if seam(index) {
            let first = child.first_key().ok_or(invalid("empty directory child"))?;
            if children
                .0
                .last()
                .and_then(|previous| previous.last_key())
                .is_some_and(|previous| previous >= first)
            {
                return Err(MemoryError::InvalidNeighbors);
            }
        }
        pages = checked_add(pages, child.pages)?;
        level = Some(child_level);
        children.push(held, count)?;
    }
    let first = children.0.first().map_or(0, |child| child.first);
    let level = level.ok_or(invalid("empty directory branch"))?;
    Ok(Held::new(Node {
        links: Links::Branches(children.into_children()),
        first,
        pages,
        level,
        _allocation: allocation,
    }))
}

/// A borrowed sequence with at most two replacement handles, borrowed from
/// their owner. Reading it never copies keys/values or allocates a temporary
/// vector.
struct Splice<'a, T> {
    old: &'a [T],
    at: usize,
    removed: usize,
    inserted: [Option<&'a T>; 2],
    count: usize,
}

impl<'a, T> Splice<'a, T> {
    fn new(
        old: &'a [T],
        at: usize,
        removed: usize,
        first: Option<&'a T>,
        second: Option<&'a T>,
    ) -> Result<Self, MemoryError> {
        if checked_add(at, removed)? > old.len() || (first.is_none() && second.is_some()) {
            return Err(invalid("invalid directory splice"));
        }
        let count = usize::from(first.is_some()).saturating_add(usize::from(second.is_some()));
        Ok(Self {
            old,
            at,
            removed,
            inserted: [first, second],
            count,
        })
    }

    fn len(&self) -> Result<usize, MemoryError> {
        checked_add(self.old.len().saturating_sub(self.removed), self.count)
    }

    /// Whether the pair ending at `index` is new: an inserted item, or the
    /// old item after the span. Every other pair is the old sequence's.
    fn seam(&self, index: usize) -> bool {
        index >= self.at && index <= self.at.saturating_add(self.count)
    }

    fn get(&self, index: usize) -> Option<&'a T> {
        if index < self.at {
            return self.old.get(index);
        }
        let relative = index.checked_sub(self.at)?;
        if relative < self.count {
            return *self.inserted.get(relative)?;
        }
        self.old
            .get(index.checked_sub(self.count)?.checked_add(self.removed)?)
    }
}

/// A node and, if it split, its right half.
type Split<K, V> = (Held<K, V>, Held<K, V>);

fn split_leaves<K: crate::RangeKey, V>(
    items: &Splice<'_, Arc<Page<K, V>>>,
    build: &mut DirectoryBuild<'_>,
) -> Result<Split<K, V>, MemoryError> {
    let count = items.len()?;
    if count <= MAX {
        return Ok((
            leaf(
                count,
                |i| items.get(i).map(Arc::clone),
                |i| items.seam(i),
                build,
            )?,
            Held::none(),
        ));
    }
    if count != checked_add(MAX, 1)? {
        return Err(invalid("directory split width exceeded"));
    }
    let left = leaf(
        MIN,
        |i| items.get(i).map(Arc::clone),
        |i| items.seam(i),
        build,
    )?;
    let right = leaf(
        count.saturating_sub(MIN),
        |i| {
            i.checked_add(MIN)
                .and_then(|i| items.get(i))
                .map(Arc::clone)
        },
        |i| i.checked_add(MIN).is_some_and(|i| items.seam(i)),
        build,
    )?;
    Ok((left, right))
}

fn split_branches<K: crate::RangeKey, V>(
    items: &Splice<'_, Arc<Node<K, V>>>,
    build: &mut DirectoryBuild<'_>,
) -> Result<Split<K, V>, MemoryError> {
    let count = items.len()?;
    if count <= MAX {
        return Ok((
            branch(
                count,
                |i| items.get(i).map(Held::share),
                |i| items.seam(i),
                build,
            )?,
            Held::none(),
        ));
    }
    if count != checked_add(MAX, 1)? {
        return Err(invalid("directory split width exceeded"));
    }
    let left = branch(
        MIN,
        |i| items.get(i).map(Held::share),
        |i| items.seam(i),
        build,
    )?;
    let right = branch(
        count.saturating_sub(MIN),
        |i| {
            i.checked_add(MIN)
                .and_then(|i| items.get(i))
                .map(Held::share)
        },
        |i| i.checked_add(MIN).is_some_and(|i| items.seam(i)),
        build,
    )?;
    Ok((left, right))
}

fn child_at<K, V>(
    children: &[Arc<Node<K, V>>],
    mut rank: usize,
    insertion: bool,
) -> Result<(usize, usize), MemoryError> {
    for (index, child) in children.iter().enumerate() {
        if rank < child.pages || (insertion && rank == child.pages) {
            return Ok((index, rank));
        }
        rank = rank
            .checked_sub(child.pages)
            .ok_or(MemoryError::MissingKey)?;
    }
    Err(MemoryError::MissingKey)
}

/// The branches a walk from a node down to one of its leaves passed, each
/// with the child it took, outermost first: at most [`MAX_LEVELS`], the
/// deepest a directory is built, held in place, so a path is rebuilt upward
/// with no allocation and no recursion.
struct Descent<'a, K, V> {
    steps: [Step<'a, K, V>; MAX_LEVELS],
    depth: usize,
}

/// A branch a walk passed (its children) and the child it took.
type Step<'a, K, V> = (&'a [Arc<Node<K, V>>], usize);

/// The leaf a walk reached: the branches it passed, the leaf's pages and the
/// rank within the leaf.
type Reached<'a, K, V> = (Descent<'a, K, V>, &'a [Arc<Page<K, V>>], usize);

impl<'a, K, V> Descent<'a, K, V> {
    /// Walks from `node` to the leaf holding `rank` (with `insertion`, the
    /// leaf a page at `rank` goes in): the branches passed, the leaf's pages,
    /// and the rank within the leaf.
    fn to_leaf(
        mut node: &'a Node<K, V>,
        mut rank: usize,
        insertion: bool,
    ) -> Result<Reached<'a, K, V>, MemoryError> {
        let mut descent = Self {
            steps: [(&[] as &[Arc<Node<K, V>>], 0); MAX_LEVELS],
            depth: 0,
        };
        loop {
            match &node.links {
                Links::Leaves(pages) => return Ok((descent, pages.as_slice(), rank)),
                Links::Branches(children) => {
                    let (index, within) = child_at(children, rank, insertion)?;
                    let child = children.get(index).ok_or(MemoryError::MissingKey)?;
                    let step = descent
                        .steps
                        .get_mut(descent.depth)
                        .ok_or(invalid("directory deeper than its levels"))?;
                    *step = (children.as_slice(), index);
                    descent.depth = descent.depth.saturating_add(1);
                    node = child;
                    rank = within;
                }
            }
        }
    }

    /// The branches passed, innermost first: the order a path is rebuilt in.
    fn upward(&self) -> impl Iterator<Item = &(&'a [Arc<Node<K, V>>], usize)> {
        self.steps.iter().take(self.depth).rev()
    }
}

fn insert_node<K: crate::RangeKey, V>(
    node: &Node<K, V>,
    rank: usize,
    page: Arc<Page<K, V>>,
    build: &mut DirectoryBuild<'_>,
) -> Result<Split<K, V>, MemoryError> {
    let (descent, pages, rank) = Descent::to_leaf(node, rank, true)?;
    let mut split = split_leaves(&Splice::new(pages, rank, 0, Some(&page), None)?, build)?;
    for &(children, index) in descent.upward() {
        let (left, right) = split;
        split = split_branches(
            &Splice::new(children, index, 1, left.handle(), right.handle())?,
            build,
        )?;
    }
    Ok(split)
}

fn remove_node<K: crate::RangeKey, V>(
    node: &Node<K, V>,
    rank: usize,
    build: &mut DirectoryBuild<'_>,
) -> Result<Held<K, V>, MemoryError> {
    let (descent, pages, rank) = Descent::to_leaf(node, rank, false)?;
    let items = Splice::new(pages, rank, 1, None, None)?;
    let mut node = if items.len()? == 0 {
        Held::none()
    } else {
        leaf(
            items.len()?,
            |i| items.get(i).map(Arc::clone),
            |i| items.seam(i),
            build,
        )?
    };
    for &(children, index) in descent.upward() {
        let (at, removed, first, second) = match node.node() {
            Some(child) if child.width() < MIN && children.len() > 1 => {
                repair(children, index, child, build)?
            }
            _ => (
                index,
                1,
                std::mem::replace(&mut node, Held::none()),
                Held::none(),
            ),
        };
        node = {
            let items = Splice::new(children, at, removed, first.handle(), second.handle())?;
            if items.len()? == 0 {
                Held::none()
            } else {
                branch(
                    items.len()?,
                    |i| items.get(i).map(Held::share),
                    |i| items.seam(i),
                    build,
                )?
            }
        };
    }
    Ok(node)
}

/// What replaces an underfull child and the sibling it is evened with: where
/// the replaced span starts, its length, and the nodes that take its place.
type Repaired<K, V> = (usize, usize, Held<K, V>, Held<K, V>);

fn repair<K: crate::RangeKey, V>(
    children: &[Arc<Node<K, V>>],
    index: usize,
    child: &Node<K, V>,
    build: &mut DirectoryBuild<'_>,
) -> Result<Repaired<K, V>, MemoryError> {
    let left = index.checked_sub(1).and_then(|i| children.get(i));
    let right = index.checked_add(1).and_then(|i| children.get(i));
    if let Some(left) = left.filter(|left| left.width() > MIN) {
        let (first, second) = redistribute(left, child, left.width().saturating_sub(1), build)?;
        return Ok((index.saturating_sub(1), 2, first, second));
    }
    if let Some(right) = right.filter(|right| right.width() > MIN) {
        let (first, second) = redistribute(child, right, checked_add(child.width(), 1)?, build)?;
        return Ok((index, 2, first, second));
    }
    if let Some(left) = left {
        let width = checked_add(left.width(), child.width())?;
        let (first, second) = redistribute(left, child, width, build)?;
        return Ok((index.saturating_sub(1), 2, first, second));
    }
    let right = right.ok_or(invalid("missing directory rebalance sibling"))?;
    let width = checked_add(child.width(), right.width())?;
    let (first, second) = redistribute(child, right, width, build)?;
    Ok((index, 2, first, second))
}

fn joined<'a, T>(left: &'a [T], right: &'a [T], index: usize) -> Option<&'a T> {
    if index < left.len() {
        left.get(index)
    } else {
        right.get(index.checked_sub(left.len())?)
    }
}

fn redistribute<K: crate::RangeKey, V>(
    left: &Node<K, V>,
    right: &Node<K, V>,
    first_count: usize,
    build: &mut DirectoryBuild<'_>,
) -> Result<Split<K, V>, MemoryError> {
    if left.level != right.level {
        return Err(invalid("directory sibling heights differ"));
    }
    let total = checked_add(left.width(), right.width())?;
    let second_count = total
        .checked_sub(first_count)
        .ok_or(invalid("invalid rebalance split"))?;
    if !(MIN..=MAX).contains(&first_count)
        || (second_count != 0 && !(MIN..=MAX).contains(&second_count))
    {
        return Err(invalid("invalid directory rebalance occupancy"));
    }
    match (&left.links, &right.links) {
        (Links::Leaves(left), Links::Leaves(right)) => {
            let first = leaf(
                first_count,
                |i| joined(left, right, i).map(Arc::clone),
                |_| true,
                build,
            )?;
            let second = if second_count == 0 {
                Held::none()
            } else {
                leaf(
                    second_count,
                    |i| {
                        i.checked_add(first_count)
                            .and_then(|i| joined(left, right, i))
                            .map(Arc::clone)
                    },
                    |_| true,
                    build,
                )?
            };
            Ok((first, second))
        }
        (Links::Branches(left), Links::Branches(right)) => {
            let (left, right) = (left.as_slice(), right.as_slice());
            let first = branch(
                first_count,
                |i| joined(left, right, i).map(Held::share),
                |_| true,
                build,
            )?;
            let second = if second_count == 0 {
                Held::none()
            } else {
                branch(
                    second_count,
                    |i| {
                        i.checked_add(first_count)
                            .and_then(|i| joined(left, right, i))
                            .map(Held::share)
                    },
                    |_| true,
                    build,
                )?
            };
            Ok((first, second))
        }
        _ => Err(invalid("directory sibling kinds differ")),
    }
}

struct Frame<'a, K, V> {
    children: &'a [Arc<Node<K, V>>],
    next: usize,
}

impl<K, V> Copy for Frame<'_, K, V> {}
impl<K, V> Clone for Frame<'_, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

pub(crate) struct Iter<'a, K, V> {
    stack: [Option<Frame<'a, K, V>>; MAX_LEVELS],
    depth: usize,
    pages: &'a [Arc<Page<K, V>>],
    offset: usize,
}

impl<'a, K, V> Iter<'a, K, V> {
    fn new(root: Option<&'a Node<K, V>>, index: usize) -> Self {
        let mut result = Self {
            stack: [None; MAX_LEVELS],
            depth: 0,
            pages: &[],
            offset: 0,
        };
        if let Some(root) = root.filter(|root| index < root.pages) {
            result.descend(root, index);
        }
        result
    }

    fn descend(&mut self, mut node: &'a Node<K, V>, mut rank: usize) {
        loop {
            match &node.links {
                Links::Leaves(pages) => {
                    self.pages = pages;
                    self.offset = rank;
                    return;
                }
                Links::Branches(children) => {
                    let Ok((index, offset)) = child_at(children, rank, false) else {
                        return;
                    };
                    let Some(frame) = self.stack.get_mut(self.depth) else {
                        return;
                    };
                    let Some(child) = children.get(index) else {
                        return;
                    };
                    *frame = Some(Frame {
                        children,
                        next: index.saturating_add(1),
                    });
                    self.depth = self.depth.saturating_add(1);
                    node = child;
                    rank = offset;
                }
            }
        }
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = &'a Arc<Page<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(page) = self.pages.get(self.offset) {
                self.offset = self.offset.saturating_add(1);
                return Some(page);
            }
            self.pages = &[];
            self.offset = 0;
            loop {
                let depth = self.depth.checked_sub(1)?;
                let frame = self.stack.get_mut(depth)?.as_mut()?;
                if let Some(child) = frame.children.get(frame.next) {
                    frame.next = frame.next.saturating_add(1);
                    self.descend(child, 0);
                    break;
                }
                *self.stack.get_mut(depth)? = None;
                self.depth = depth;
            }
        }
    }
}

impl<'a, K, V> IntoIterator for &'a PageDirectory<K, V> {
    type Item = &'a Arc<Page<K, V>>;
    type IntoIter = Iter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
fn audit<K: crate::RangeKey, V>(node: &Node<K, V>, root: bool) -> bool {
    let minimum = if root {
        match node.links {
            Links::Leaves(_) => 1,
            Links::Branches(_) => 2,
        }
    } else {
        MIN
    };
    if !(minimum..=MAX).contains(&node.width()) {
        return false;
    }
    // The prefix a node caches is its first page's, and a page's is its first key's.
    if !node.first_page().is_some_and(|page| {
        page.first == node.first
            && page
                .entries
                .first()
                .is_some_and(|entry| entry.key.order_prefix() == page.first)
    }) {
        return false;
    }
    match &node.links {
        Links::Leaves(pages) => {
            node.level == 0
                && node.pages == pages.len()
                && pages.iter().all(|page| {
                    !page.entries.is_empty()
                        && page.entries.windows(2).all(|entries| {
                            entries
                                .first()
                                .zip(entries.last())
                                .is_some_and(|(a, b)| a.key < b.key)
                        })
                })
                && pages.windows(2).all(|pages| {
                    pages
                        .first()
                        .and_then(|p| p.entries.last())
                        .zip(pages.last().and_then(|p| p.entries.first()))
                        .is_some_and(|(a, b)| a.key < b.key)
                })
        }
        Links::Branches(children) => {
            children
                .iter()
                .all(|child| child.level.checked_add(1) == Some(node.level) && audit(child, false))
                && children
                    .iter()
                    .try_fold(0usize, |n, child| n.checked_add(child.pages))
                    == Some(node.pages)
                && children.windows(2).all(|children| {
                    children
                        .first()
                        .and_then(|n| n.last_key())
                        .zip(children.last().and_then(|n| n.first_key()))
                        .is_some_and(|(a, b)| a < b)
                })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Change, Entry, RangeConfig, RangeId, RangeStore};

    fn multilevel(budget: &MemoryBudget) -> RangeStore<u64, u64> {
        let mut store = RangeStore::new(
            RangeId(790),
            0,
            RangeConfig {
                page_entries: 1,
                max_batch_entries: 2048,
                ..RangeConfig::default()
            },
            budget.clone(),
        )
        .unwrap();
        store
            .apply_batch(
                1,
                (0..1200u64)
                    .map(|index| Change::Put(Entry::new(index * 2, index, 0)))
                    .collect(),
                BudgetLane::Ordinary,
            )
            .unwrap();
        assert!(store.root.pages.height() >= 3);
        store
    }

    fn children(node: &Node<u64, u64>) -> &[Arc<Node<u64, u64>>] {
        match &node.links {
            Links::Branches(children) => children.as_slice(),
            Links::Leaves(_) => panic!("expected a branch in multilevel fixture"),
        }
    }

    #[test]
    fn sparse_edit_shares_untouched_internal_subtrees_at_multiple_levels() {
        let budget = MemoryBudget::new(64 * 1024 * 1024, 1024 * 1024).unwrap();
        let store = multilevel(&budget);
        let next = store
            .prepare_batch_with(
                2,
                vec![Change::Put(Entry::new(0, 9999, 0))],
                BudgetLane::Ordinary,
                |_| panic!("singleton replacement cannot copy an old value"),
            )
            .unwrap();
        let old_root = store.root.pages.root.handle().unwrap();
        let next_root = next.root.pages.root.handle().unwrap();
        assert!(!Arc::ptr_eq(old_root, next_root));
        let old = children(old_root);
        let new = children(next_root);
        assert_eq!(old.len(), new.len());
        assert!(!Arc::ptr_eq(old.first().unwrap(), new.first().unwrap()));
        for (old, new) in old.iter().zip(new).skip(1) {
            assert!(Arc::ptr_eq(old, new), "untouched root child was copied");
        }
        let old = children(old.first().unwrap());
        let new = children(new.first().unwrap());
        assert!(!Arc::ptr_eq(old.first().unwrap(), new.first().unwrap()));
        for (old, new) in old.iter().zip(new).skip(1) {
            assert!(Arc::ptr_eq(old, new), "untouched descendant was copied");
        }
        assert_eq!(store.get(&0), Some(&0));
        assert_eq!(next.get(&0), Some(&9999));
        assert!(store.root.pages.check_shape());
        assert!(next.root.pages.check_shape());
        drop(next);
        drop(store);
        assert_eq!(budget.stats().used, 0);
    }

    #[test]
    fn one_byte_short_cumulative_node_bound_refunds_entire_unpublished_path() {
        let budget = MemoryBudget::new(64 * 1024 * 1024, 1024 * 1024).unwrap();
        let store = multilevel(&budget);
        let original = &store.root.pages;
        let before = budget.stats();
        let mut node = original.root.node().unwrap();
        let mut exact = 0;
        loop {
            exact += node_charge::<u64, u64>(node.width()).unwrap();
            match &node.links {
                Links::Leaves(_) => break,
                Links::Branches(children) => node = children.first().unwrap(),
            }
        }
        assert!(PageDirectory::<u64, u64>::edit_bound(1, original.len()).unwrap() >= exact);
        let replacement = Arc::clone(original.get(0).unwrap());
        let mut short = DirectoryBuild::new(&budget, BudgetLane::Completion, exact - 1);
        assert!(matches!(
            original.replace(0, replacement, &mut short),
            Err(MemoryError::Capacity { .. })
        ));
        assert_eq!(budget.stats().used, before.used);
        assert_eq!(budget.stats().by_kind, before.by_kind);
        assert!(original.check_shape());
        assert_eq!(store.get(&0), Some(&0));

        let mut admitted = DirectoryBuild::new(&budget, BudgetLane::Completion, exact);
        let next = original
            .replace(0, Arc::clone(original.get(0).unwrap()), &mut admitted)
            .unwrap();
        assert_eq!(admitted.remaining, 0);
        assert_eq!(budget.stats().used - before.used, exact);
        assert!(next.check_shape());
        drop(next);
        assert_eq!(budget.stats().used, before.used);
        drop(store);
        assert_eq!(budget.stats().used, 0);
    }

    /// A page of one entry, its key `key`, charged to `budget`.
    fn page(budget: &MemoryBudget, key: u64) -> Arc<Page<u64, u64>> {
        let allocation = budget
            .reserve(BudgetKind::Pages, BudgetLane::Completion, 64)
            .unwrap()
            .commit();
        Arc::new(Page::new(vec![Entry::new(key, key, 0)], allocation))
    }

    /// A directory over `count` pages whose keys are 0, 2, 4 and on.
    fn built(budget: &MemoryBudget, count: usize) -> PageDirectory<u64, u64> {
        let bound = PageDirectory::<u64, u64>::build_bound(count).unwrap();
        let mut build = DirectoryBuild::new(budget, BudgetLane::Completion, bound);
        PageDirectory::from_pages(
            count,
            |index| Some(page(budget, u64::try_from(index).ok()? * 2)),
            &mut build,
        )
        .unwrap()
    }

    /// The nodes from the root to the page at `index`, each with its width.
    fn path(
        directory: &PageDirectory<u64, u64>,
        mut index: usize,
    ) -> Vec<(*const Node<u64, u64>, usize)> {
        let mut path = Vec::new();
        let mut node = directory.root.handle().unwrap();
        loop {
            path.push((Arc::as_ptr(node), node.width()));
            match &node.links {
                Links::Leaves(_) => return path,
                Links::Branches(children) => {
                    let (child, within) = child_at(children, index, false).unwrap();
                    node = &children[child];
                    index = within;
                }
            }
        }
    }

    /// Edits of many pages copy each node on their paths once: replacing a
    /// page in each of several leaves issues the charge of the union of their
    /// paths, each node at its width, where copying every path for every edit
    /// issued the sum. The edited directory holds the base's pages but those
    /// replaced, and the base keeps every page it had.
    #[test]
    fn edits_of_many_pages_copy_each_node_on_their_paths_once() {
        let budget = MemoryBudget::new(64 * 1024 * 1024, 1024 * 1024).unwrap();
        let base = built(&budget, MAX * MAX + 7);
        assert_eq!(base.height(), 3);
        let before: Vec<_> = base.iter().map(Arc::as_ptr).collect();
        // Leaves under both of the root's children, two pages in one leaf
        // twice, and the first page, the first of every node above it.
        let ranks = [0, 40, 41, 600, 1000, 1030];
        let mut union = std::collections::BTreeMap::new();
        let mut summed = 0;
        for &rank in &ranks {
            for (node, width) in path(&base, rank) {
                summed += node_charge::<u64, u64>(width).unwrap();
                union.insert(node, width);
            }
        }
        let once: usize = union
            .values()
            .map(|width| node_charge::<u64, u64>(*width).unwrap())
            .sum();
        assert_eq!(union.len(), 7);
        assert!(once * 2 < summed);
        let mut edited = base.shared();
        let mut build = DirectoryBuild::new(&budget, BudgetLane::Completion, usize::MAX);
        let mut replaced = Vec::new();
        for &rank in &ranks {
            // Odd keys: each new page sorts between the old one's neighbours.
            let page = page(&budget, u64::try_from(rank).unwrap() * 2 + 1);
            replaced.push(Arc::as_ptr(&page));
            edited.replace_owned(rank, page, &mut build).unwrap();
        }
        assert_eq!(usize::MAX - build.remaining, once);
        assert!(edited.check_shape());
        assert!(base.check_shape());
        assert_eq!(base.iter().map(Arc::as_ptr).collect::<Vec<_>>(), before);
        for (rank, page) in edited.iter().enumerate() {
            match ranks.iter().position(|edit| *edit == rank) {
                Some(edit) => assert_eq!(Arc::as_ptr(page), replaced[edit], "rank {rank}"),
                None => assert_eq!(Arc::as_ptr(page), before[rank], "rank {rank}"),
            }
        }
        drop(edited);
        drop(base);
        assert_eq!(budget.stats().used, 0);
    }

    /// An edit made in place that is refused at any byte short of what it
    /// needs leaves its directory holding every page it held, in order and
    /// in shape: a node copied before the refusal holds what the one it
    /// replaced held. Dropping the directory gives back all it took. Two
    /// edits run in each directory, so the second meets nodes the first made
    /// its own: a replace, and an insert into a leaf with room, which grows
    /// the leaf in place.
    #[test]
    fn an_edit_in_place_refused_at_any_point_keeps_every_page_and_gives_back_all_it_took() {
        let budget = MemoryBudget::new(64 * 1024 * 1024, 1024 * 1024).unwrap();
        // Two leaves of 31 under a root: room in each.
        let base = built(&budget, 2 * MAX - 2);
        assert_eq!(base.height(), 2);
        let keys = |directory: &PageDirectory<u64, u64>| -> Vec<u64> {
            directory.iter().map(|page| page.entries[0].key).collect()
        };
        let original = keys(&base);
        let before = budget.stats();
        type InPlace<'a> = dyn Fn(&mut PageDirectory<u64, u64>, &mut DirectoryBuild<'_>) -> Result<(), MemoryError>
            + 'a;
        let mut replaced = original.clone();
        replaced[3] = 7;
        let mut inserted = original.clone();
        inserted.insert(3, 5);
        // Each edit's name, its two steps, and the keys after the first and
        // after both.
        type Case<'a> = (
            &'a str,
            &'a InPlace<'a>,
            &'a InPlace<'a>,
            Vec<u64>,
            Vec<u64>,
        );
        let edits: [Case<'_>; 2] = [
            (
                "replace",
                &|directory, build| directory.replace_owned(3, page(&budget, 7), build),
                &|directory, build| directory.replace_owned(4, page(&budget, 9), build),
                replaced.clone(),
                {
                    let mut both = replaced;
                    both[4] = 9;
                    both
                },
            ),
            (
                "insert",
                &|directory, build| directory.insert_owned(3, page(&budget, 5), build),
                // After the first: 0, 2, 4, 5, 6, 8, 10 and on; 9 goes at 6.
                &|directory, build| directory.insert_owned(6, page(&budget, 9), build),
                inserted.clone(),
                {
                    let mut both = inserted;
                    both.insert(6, 9);
                    both
                },
            ),
        ];
        for (name, first, second, halfway, done) in edits {
            let mut refused = 0;
            loop {
                let mut directory = base.shared();
                let mut build = DirectoryBuild::new(&budget, BudgetLane::Completion, refused);
                let result = first(&mut directory, &mut build)
                    .and_then(|()| second(&mut directory, &mut build));
                assert!(directory.check_shape(), "{name} at {refused} bytes");
                let now = keys(&directory);
                drop(directory);
                assert_eq!(
                    budget.stats().used,
                    before.used,
                    "{name} at {refused} bytes"
                );
                if result.is_ok() {
                    assert_eq!(now, done, "{name}");
                    break;
                }
                assert!(
                    now == original || now == halfway,
                    "{name} refused at {refused} bytes holds {now:?}"
                );
                refused += 1;
                assert!(
                    refused < 1 << 16,
                    "{name} is refused past any path's charge"
                );
            }
            assert!(refused > 0, "{name} needs room to build");
        }
        assert_eq!(keys(&base), original);
        drop(base);
        assert_eq!(budget.stats().used, 0);
    }

    /// One descent by key finds a key's page, the page's rank and the page
    /// after it, at every page of a three-level directory: on a page's first
    /// key, between two pages' keys, and past the last page, where there is
    /// none after.
    #[test]
    fn locating_a_key_finds_its_page_its_rank_and_the_page_after() {
        let budget = MemoryBudget::new(64 * 1024 * 1024, 1024 * 1024).unwrap();
        let directory = built(&budget, MAX * MAX + 7);
        assert_eq!(directory.height(), 3);
        for rank in 0..directory.len() {
            let key = u64::try_from(rank).unwrap() * 2;
            for sought in [key, key + 1] {
                let located = directory.locate(&sought).unwrap();
                assert_eq!(located.rank, rank, "key {sought}");
                assert!(Arc::ptr_eq(located.page, directory.get(rank).unwrap()));
                match directory.get(rank + 1) {
                    Some(next) => assert!(Arc::ptr_eq(located.next.unwrap(), next), "key {sought}"),
                    None => assert!(located.next.is_none(), "key {sought}"),
                }
                assert_eq!(directory.page_index(&sought), rank);
            }
        }
        assert!(PageDirectory::<u64, u64>::new().locate(&0).is_none());
        drop(directory);
        assert_eq!(budget.stats().used, 0);
    }

    /// An edit of a directory, made within the build it is given.
    type Edit<'a> =
        dyn Fn(&mut DirectoryBuild<'_>) -> Result<PageDirectory<u64, u64>, MemoryError> + 'a;

    /// Every point an edit can be refused at gives back all it took, through
    /// the handles that let go of the nodes it built ([`Held`], [`Gathered`],
    /// [`release`]): an insert that splits a full leaf and its full parent up
    /// to a new root, a removal that evens an underfull leaf with its
    /// sibling, and one that merges two and collapses the root, each refused
    /// at every byte short of what it needs; admitted, what it built goes
    /// when it is dropped.
    #[test]
    fn an_edit_refused_at_any_point_leaves_no_charge_behind() {
        let budget = MemoryBudget::new(64 * 1024 * 1024, 1024 * 1024).unwrap();
        let full = built(&budget, MAX * MAX);
        assert_eq!(full.height(), 2);
        let evened = built(&budget, MAX + 1);
        let mut merged = built(&budget, MAX + 1);
        let mut admitted = DirectoryBuild::new(&budget, BudgetLane::Completion, usize::MAX);
        // [17, 16] less one of the left's pages: [16, 16].
        merged = merged.remove(0, &mut admitted).unwrap();
        let edits: [(&str, &Edit<'_>); 3] = [
            ("split to a new root", &|build| {
                full.insert(1, page(&budget, 1), build)
            }),
            ("evened with a sibling", &|build| evened.remove(MAX, build)),
            ("merged and collapsed", &|build| {
                merged.remove(MAX - 1, build)
            }),
        ];
        let before = budget.stats();
        for (name, edit) in edits {
            let mut refused = 0;
            let next = loop {
                let mut build = DirectoryBuild::new(&budget, BudgetLane::Completion, refused);
                match edit(&mut build) {
                    Ok(next) => break next,
                    Err(_) => {
                        assert_eq!(budget.stats(), before, "{name}: refused at {refused} bytes");
                        refused += 1;
                    }
                }
            };
            assert!(refused > 0, "{name} needs room to build");
            assert!(next.check_shape(), "{name}");
            drop(next);
            assert_eq!(budget.stats(), before, "{name}: admitted, then dropped");
        }
        assert_eq!(full.height(), 2);
        drop((full, evened, merged));
        assert_eq!(budget.stats().used, 0);
    }
}
