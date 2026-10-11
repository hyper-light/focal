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
    let mut unwinding: [Option<Unwinding<K, V>>; MAX_LEVELS] = std::array::from_fn(|_| None);
    let mut depth = 0usize;
    let mut next = Some(node);
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
    source: &'a MemoryBudget,
    lane: BudgetLane,
    remaining: usize,
}

impl<'a> DirectoryBuild<'a> {
    pub(super) fn new(source: &'a MemoryBudget, lane: BudgetLane, byte_bound: usize) -> Self {
        Self {
            source,
            lane,
            remaining: byte_bound,
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
        let allocation = self
            .source
            .reserve(BudgetKind::Roots, self.lane, bytes)?
            .commit();
        self.remaining = remaining;
        Ok(allocation)
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
        // Internal occupancy is at least MIN. An extra level accounts for the
        // smaller root and for a split during an edit.
        let mut pages = max_leaf_count.max(1);
        let mut levels = 2usize;
        while pages > 1 {
            pages = pages.div_ceil(MIN);
            levels = checked_add(levels, 1)?;
        }
        if levels > MAX_LEVELS {
            return Err(invalid("directory height exceeds supported bound"));
        }
        let nodes = checked_add(checked_mul(6, levels)?, 2)?;
        checked_mul(edits, checked_mul(nodes, node_charge::<K, V>(MAX)?)?)
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
/// (`RangeKey::cmp_past_prefix`).
fn at_or_before<K: crate::RangeKey>(first: u128, start: Option<&K>, probe: u128, key: &K) -> bool {
    first < probe
        || (first == probe && start.is_some_and(|start| start.cmp_past_prefix(key).is_le()))
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
                                page.entries.first().map(|entry| &entry.key),
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
                            at_or_before(child.first, child.first_key(), probe, key)
                        })
                        .saturating_sub(1);
                    node = children.get(local)?.as_ref();
                }
            }
        }
    }

    /// The page whose span holds `key`. The prefixes cached beside pages and
    /// nodes decide every comparison where they differ; where they tie, the
    /// keys do. The sought key's prefix is computed once.
    pub(super) fn page_index(&self, key: &K) -> usize {
        let Some(mut node) = self.root.node() else {
            return 0;
        };
        let probe = key.order_prefix();
        let mut rank = 0usize;
        loop {
            match &node.links {
                Links::Leaves(pages) => {
                    let local = pages
                        .partition_point(|page| {
                            at_or_before(
                                page.first,
                                page.entries.first().map(|entry| &entry.key),
                                probe,
                                key,
                            )
                        })
                        .saturating_sub(1);
                    return rank.saturating_add(local);
                }
                Links::Branches(children) => {
                    let local = children
                        .partition_point(|child| {
                            at_or_before(child.first, child.first_key(), probe, key)
                        })
                        .saturating_sub(1);
                    for child in children.iter().take(local) {
                        rank = rank.saturating_add(child.pages);
                    }
                    let Some(child) = children.get(local) else {
                        return rank;
                    };
                    node = child;
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

    pub(super) fn replace(
        &self,
        index: usize,
        page: Arc<Page<K, V>>,
        build: &mut DirectoryBuild<'_>,
    ) -> Result<Self, MemoryError> {
        if index >= self.len() {
            return Err(MemoryError::MissingKey);
        }
        self.check_page(index, true, &page)?;
        let root = self.root.node().ok_or(MemoryError::MissingKey)?;
        Ok(Self {
            root: replace_node(root, index, page, build)?,
        })
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
        if index
            .checked_sub(1)
            .and_then(|index| self.get(index))
            .and_then(|page| page.entries.last())
            .is_some_and(|entry| entry.key >= first.key)
        {
            return Err(MemoryError::InvalidNeighbors);
        }
        let next = if replacing {
            checked_add(index, 1)?
        } else {
            index
        };
        if self
            .get(next)
            .and_then(|page| page.entries.first())
            .is_some_and(|entry| entry.key <= last.key)
        {
            return Err(MemoryError::InvalidNeighbors);
        }
        Ok(())
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
        let _staging = build.source.reserve(
            BudgetKind::Roots,
            build.lane,
            checked_mul(
                2,
                checked_add(
                    ALLOCATOR_OVERHEAD,
                    checked_mul(groups, size_of::<Arc<Node<K, V>>>())?,
                )?,
            )?,
        )?;
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
        let mut node = self;
        loop {
            match &node.links {
                Links::Leaves(pages) => return Some(&pages.first()?.entries.first()?.key),
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

fn replace_node<K: crate::RangeKey, V>(
    node: &Node<K, V>,
    rank: usize,
    page: Arc<Page<K, V>>,
    build: &mut DirectoryBuild<'_>,
) -> Result<Held<K, V>, MemoryError> {
    let (descent, pages, rank) = Descent::to_leaf(node, rank, false)?;
    let items = Splice::new(pages, rank, 1, Some(&page), None)?;
    let mut node = leaf(
        items.len()?,
        |i| items.get(i).map(Arc::clone),
        |i| items.seam(i),
        build,
    )?;
    for &(children, index) in descent.upward() {
        node = {
            let items = Splice::new(children, index, 1, node.handle(), None)?;
            branch(
                items.len()?,
                |i| items.get(i).map(Held::share),
                |i| items.seam(i),
                build,
            )?
        };
    }
    Ok(node)
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
