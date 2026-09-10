//! Immutable, rank-indexed page directory. Only the edited paths and at most
//! one sibling per level are copied. Keys and values remain in the leaf pages;
//! separators borrow their descendants' minimum keys.
//!
//! A directory node has 16..=32 handles, except the root. Insertion splits 33
//! handles into 16 and 17; deletion borrows or merges before publishing. Each
//! node owns one charged vector and one charged Arc allocation. Small splice
//! descriptions stay on the stack, so there is no unaccounted merge buffer.

use super::{Page, bounded_push, bounded_vec};
use crate::{
    ALLOCATOR_OVERHEAD, Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError, checked_add,
    checked_mul,
};
use std::sync::Arc;

const MIN: usize = 16;
const MAX: usize = 32;
// Minimum fanout is a power of two. The smaller root and a transient split get
// two extra levels beyond the number needed to encode any usize page count.
// Heights are checked at construction, so iterator stacks cannot overflow.
const MAX_LEVELS: usize = usize::BITS.div_ceil(MIN.trailing_zeros()).saturating_add(2) as usize;

pub(crate) struct PageDirectory<K, V> {
    root: Option<Arc<Node<K, V>>>,
}

struct Node<K, V> {
    // Owned buffers/children must drop before their accounting allocation.
    links: Links<K, V>,
    pages: usize,
    level: usize,
    _allocation: Allocation,
}

enum Links<K, V> {
    Leaves(Vec<Arc<Page<K, V>>>),
    Branches(Vec<Arc<Node<K, V>>>),
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
        Self { root: None }
    }

    pub(super) fn shared(&self) -> Self {
        Self {
            root: self.root.as_ref().map(Arc::clone),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.root.as_ref().map_or(0, |node| node.pages)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    pub(super) fn get(&self, mut index: usize) -> Option<&Arc<Page<K, V>>> {
        let mut node = self.root.as_deref()?;
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
        Iter::new(self.root.as_deref(), index)
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
            .as_ref()
            .map_or(0, |root| root.level.saturating_add(1))
    }
}

impl<K: Ord, V> PageDirectory<K, V> {
    pub(super) fn page_index(&self, key: &K) -> usize {
        let Some(mut node) = self.root.as_deref() else {
            return 0;
        };
        let mut rank = 0usize;
        loop {
            match &node.links {
                Links::Leaves(pages) => {
                    let local = pages
                        .partition_point(|page| {
                            page.entries.first().is_some_and(|entry| &entry.key <= key)
                        })
                        .saturating_sub(1);
                    return rank.saturating_add(local);
                }
                Links::Branches(children) => {
                    let local = children
                        .partition_point(|child| {
                            child.first_key().is_some_and(|first| first <= key)
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
        let Some(root) = &self.root else {
            let node = leaf(1, |_| Some(Arc::clone(&page)), build)?;
            return Ok(Self { root: Some(node) });
        };
        let (left, right) = insert_node(root, index, page, build)?;
        let root = match right {
            None => left,
            Some(right) => branch(
                2,
                |index| match index {
                    0 => Some(Arc::clone(&left)),
                    1 => Some(Arc::clone(&right)),
                    _ => None,
                },
                build,
            )?,
        };
        Ok(Self { root: Some(root) })
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
        let root = self.root.as_ref().ok_or(MemoryError::MissingKey)?;
        Ok(Self {
            root: Some(replace_node(root, index, page, build)?),
        })
    }

    pub(super) fn remove(
        &self,
        index: usize,
        build: &mut DirectoryBuild<'_>,
    ) -> Result<Self, MemoryError> {
        let root = self.root.as_ref().ok_or(MemoryError::MissingKey)?;
        if index >= root.pages {
            return Err(MemoryError::MissingKey);
        }
        let mut root = remove_node(root, index, build)?;
        // A one-child root carries no information and need not be retained.
        while let Some(node) = &root {
            let Links::Branches(children) = &node.links else {
                break;
            };
            if children.len() != 1 {
                break;
            }
            root = children.first().map(Arc::clone);
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
        self.root.as_ref().is_none_or(|root| audit(root, true))
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
            return Ok(Self { root: None });
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
        let mut level: Vec<Arc<Node<K, V>>> = bounded_vec(groups)?;
        let mut offset = 0usize;
        for group in 0..groups {
            let width = group_width(count, groups, group);
            let node = leaf(width, |index| item(checked_add(offset, index).ok()?), build)?;
            bounded_push(&mut level, node, groups)?;
            offset = checked_add(offset, width)?;
        }
        while level.len() > 1 {
            let groups = level.len().div_ceil(MAX);
            let mut next: Vec<Arc<Node<K, V>>> = bounded_vec(groups)?;
            let mut offset = 0usize;
            for group in 0..groups {
                let width = group_width(level.len(), groups, group);
                let node = branch(
                    width,
                    |index| level.get(checked_add(offset, index).ok()?).map(Arc::clone),
                    build,
                )?;
                bounded_push(&mut next, node, groups)?;
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

fn leaf<K: Ord, V>(
    count: usize,
    mut item: impl FnMut(usize) -> Option<Arc<Page<K, V>>>,
    build: &mut DirectoryBuild<'_>,
) -> Result<Arc<Node<K, V>>, MemoryError> {
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
        if pages
            .last()
            .and_then(|previous| previous.entries.last())
            .is_some_and(|previous| previous.key >= first.key)
        {
            return Err(MemoryError::InvalidNeighbors);
        }
        bounded_push(&mut pages, page, count)?;
    }
    Ok(Arc::new(Node {
        links: Links::Leaves(pages),
        pages: count,
        level: 0,
        _allocation: allocation,
    }))
}

fn branch<K: Ord, V>(
    count: usize,
    mut item: impl FnMut(usize) -> Option<Arc<Node<K, V>>>,
    build: &mut DirectoryBuild<'_>,
) -> Result<Arc<Node<K, V>>, MemoryError> {
    if count == 0 || count > MAX {
        return Err(invalid("invalid directory branch width"));
    }
    let allocation = build.allocate(node_charge::<K, V>(count)?)?;
    let mut children: Vec<Arc<Node<K, V>>> = bounded_vec(count)?;
    let mut pages = 0usize;
    let mut level = None;
    for index in 0..count {
        let child = item(index).ok_or(invalid("missing directory child"))?;
        let child_level = checked_add(child.level, 1)?;
        if child.pages == 0
            || child_level >= MAX_LEVELS
            || level.is_some_and(|level| child_level != level)
        {
            return Err(invalid("invalid directory child height or count"));
        }
        let first = child.first_key().ok_or(invalid("empty directory child"))?;
        if children
            .last()
            .and_then(|previous| previous.last_key())
            .is_some_and(|previous| previous >= first)
        {
            return Err(MemoryError::InvalidNeighbors);
        }
        pages = checked_add(pages, child.pages)?;
        level = Some(child_level);
        bounded_push(&mut children, child, count)?;
    }
    Ok(Arc::new(Node {
        links: Links::Branches(children),
        pages,
        level: level.ok_or(invalid("empty directory branch"))?,
        _allocation: allocation,
    }))
}

/// A borrowed sequence with at most two owned replacement handles. Reading it
/// never copies keys/values or allocates a temporary vector.
struct Splice<'a, T> {
    old: &'a [T],
    at: usize,
    removed: usize,
    inserted: [Option<T>; 2],
    count: usize,
}

impl<'a, T> Splice<'a, T> {
    fn new(
        old: &'a [T],
        at: usize,
        removed: usize,
        first: Option<T>,
        second: Option<T>,
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

    fn get(&self, index: usize) -> Option<&T> {
        if index < self.at {
            return self.old.get(index);
        }
        let relative = index.checked_sub(self.at)?;
        if relative < self.count {
            return self.inserted.get(relative)?.as_ref();
        }
        self.old
            .get(index.checked_sub(self.count)?.checked_add(self.removed)?)
    }
}

type Split<K, V> = (Arc<Node<K, V>>, Option<Arc<Node<K, V>>>);

fn split_leaves<K: Ord, V>(
    items: &Splice<'_, Arc<Page<K, V>>>,
    build: &mut DirectoryBuild<'_>,
) -> Result<Split<K, V>, MemoryError> {
    let count = items.len()?;
    if count <= MAX {
        return Ok((leaf(count, |i| items.get(i).map(Arc::clone), build)?, None));
    }
    if count != checked_add(MAX, 1)? {
        return Err(invalid("directory split width exceeded"));
    }
    let left = leaf(MIN, |i| items.get(i).map(Arc::clone), build)?;
    let right = leaf(
        count.saturating_sub(MIN),
        |i| {
            i.checked_add(MIN)
                .and_then(|i| items.get(i))
                .map(Arc::clone)
        },
        build,
    )?;
    Ok((left, Some(right)))
}

fn split_branches<K: Ord, V>(
    items: &Splice<'_, Arc<Node<K, V>>>,
    build: &mut DirectoryBuild<'_>,
) -> Result<Split<K, V>, MemoryError> {
    let count = items.len()?;
    if count <= MAX {
        return Ok((
            branch(count, |i| items.get(i).map(Arc::clone), build)?,
            None,
        ));
    }
    if count != checked_add(MAX, 1)? {
        return Err(invalid("directory split width exceeded"));
    }
    let left = branch(MIN, |i| items.get(i).map(Arc::clone), build)?;
    let right = branch(
        count.saturating_sub(MIN),
        |i| {
            i.checked_add(MIN)
                .and_then(|i| items.get(i))
                .map(Arc::clone)
        },
        build,
    )?;
    Ok((left, Some(right)))
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

fn insert_node<K: Ord, V>(
    node: &Node<K, V>,
    rank: usize,
    page: Arc<Page<K, V>>,
    build: &mut DirectoryBuild<'_>,
) -> Result<Split<K, V>, MemoryError> {
    match &node.links {
        Links::Leaves(pages) => {
            split_leaves(&Splice::new(pages, rank, 0, Some(page), None)?, build)
        }
        Links::Branches(children) => {
            let (index, rank) = child_at(children, rank, true)?;
            let child = children.get(index).ok_or(MemoryError::MissingKey)?;
            let (left, right) = insert_node(child, rank, page, build)?;
            split_branches(&Splice::new(children, index, 1, Some(left), right)?, build)
        }
    }
}

fn replace_node<K: Ord, V>(
    node: &Node<K, V>,
    rank: usize,
    page: Arc<Page<K, V>>,
    build: &mut DirectoryBuild<'_>,
) -> Result<Arc<Node<K, V>>, MemoryError> {
    match &node.links {
        Links::Leaves(pages) => {
            let items = Splice::new(pages, rank, 1, Some(page), None)?;
            leaf(items.len()?, |i| items.get(i).map(Arc::clone), build)
        }
        Links::Branches(children) => {
            let (index, rank) = child_at(children, rank, false)?;
            let child = children.get(index).ok_or(MemoryError::MissingKey)?;
            let child = replace_node(child, rank, page, build)?;
            let items = Splice::new(children, index, 1, Some(child), None)?;
            branch(items.len()?, |i| items.get(i).map(Arc::clone), build)
        }
    }
}

fn remove_node<K: Ord, V>(
    node: &Node<K, V>,
    rank: usize,
    build: &mut DirectoryBuild<'_>,
) -> Result<Option<Arc<Node<K, V>>>, MemoryError> {
    match &node.links {
        Links::Leaves(pages) => {
            let items = Splice::new(pages, rank, 1, None, None)?;
            if items.len()? == 0 {
                return Ok(None);
            }
            Ok(Some(leaf(
                items.len()?,
                |i| items.get(i).map(Arc::clone),
                build,
            )?))
        }
        Links::Branches(children) => {
            let (index, rank) = child_at(children, rank, false)?;
            let old = children.get(index).ok_or(MemoryError::MissingKey)?;
            let child = remove_node(old, rank, build)?;
            let items = match child {
                Some(child) if child.width() < MIN && children.len() > 1 => {
                    repair(children, index, child, build)?
                }
                child => Splice::new(children, index, 1, child, None)?,
            };
            if items.len()? == 0 {
                return Ok(None);
            }
            Ok(Some(branch(
                items.len()?,
                |i| items.get(i).map(Arc::clone),
                build,
            )?))
        }
    }
}

fn repair<'a, K: Ord, V>(
    children: &'a [Arc<Node<K, V>>],
    index: usize,
    child: Arc<Node<K, V>>,
    build: &mut DirectoryBuild<'_>,
) -> Result<Splice<'a, Arc<Node<K, V>>>, MemoryError> {
    let left = index.checked_sub(1).and_then(|i| children.get(i));
    let right = index.checked_add(1).and_then(|i| children.get(i));
    if let Some(left) = left.filter(|left| left.width() > MIN) {
        let (first, second) = redistribute(left, &child, left.width().saturating_sub(1), build)?;
        return Splice::new(children, index.saturating_sub(1), 2, Some(first), second);
    }
    if let Some(right) = right.filter(|right| right.width() > MIN) {
        let (first, second) = redistribute(&child, right, checked_add(child.width(), 1)?, build)?;
        return Splice::new(children, index, 2, Some(first), second);
    }
    if let Some(left) = left {
        let width = checked_add(left.width(), child.width())?;
        let (first, second) = redistribute(left, &child, width, build)?;
        return Splice::new(children, index.saturating_sub(1), 2, Some(first), second);
    }
    let right = right.ok_or(invalid("missing directory rebalance sibling"))?;
    let width = checked_add(child.width(), right.width())?;
    let (first, second) = redistribute(&child, right, width, build)?;
    Splice::new(children, index, 2, Some(first), second)
}

fn joined<'a, T>(left: &'a [T], right: &'a [T], index: usize) -> Option<&'a T> {
    if index < left.len() {
        left.get(index)
    } else {
        right.get(index.checked_sub(left.len())?)
    }
}

fn redistribute<K: Ord, V>(
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
                build,
            )?;
            let second = if second_count == 0 {
                None
            } else {
                Some(leaf(
                    second_count,
                    |i| {
                        i.checked_add(first_count)
                            .and_then(|i| joined(left, right, i))
                            .map(Arc::clone)
                    },
                    build,
                )?)
            };
            Ok((first, second))
        }
        (Links::Branches(left), Links::Branches(right)) => {
            let first = branch(
                first_count,
                |i| joined(left, right, i).map(Arc::clone),
                build,
            )?;
            let second = if second_count == 0 {
                None
            } else {
                Some(branch(
                    second_count,
                    |i| {
                        i.checked_add(first_count)
                            .and_then(|i| joined(left, right, i))
                            .map(Arc::clone)
                    },
                    build,
                )?)
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
fn audit<K: Ord, V>(node: &Node<K, V>, root: bool) -> bool {
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
            Links::Branches(children) => children,
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
        let old_root = store.root.pages.root.as_ref().unwrap();
        let next_root = next.root.pages.root.as_ref().unwrap();
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
        let mut node = original.root.as_deref().unwrap();
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
}
