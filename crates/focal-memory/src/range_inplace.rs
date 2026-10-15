//! A candidate's changes alone, published where the rows are (doc 30 §9).
//!
//! A [`PreparedRange`] builds every page its batch touches beside the
//! committed ones and holds the committed root, so a page it changes is copied
//! whole, its unchanged neighbours with it, at every write. A [`SparseRange`]
//! holds its checked changes and what its publication may spend, nothing of
//! the store's: [`RangeStore::publish_sparse`] merges them into each page where
//! it stands when the store alone holds the page, and copies the page only
//! where something else still reads it (a capture's frozen root, a lease):
//! FASTER's division between rows updated in place and rows read-copy-updated,
//! decided by reference counts (Chandramouli et al., SIGMOD 2018), and the
//! copy-on-update of Cao et al. (SIGMOD 2011) confined to captures.
//!
//! A group's leaves follow the cut rule every builder shares
//! (`layout::Cutter`), so a publication in place lays rows out exactly as a
//! rebuild does. Groups are taken from the end of the batch, and a group's new
//! leaves from the end of its page: a page the store alone holds keeps its
//! first leaf where it stands, the rows past it moved off its end into leaves
//! of exactly their size; a page something else reads is copied leaf by leaf.
//! Every leaf holds exactly its rows, as a rebuilt one does: a new leaf is
//! built to its size, and an edited page grows by its inserts and gives back
//! what its deletions and the rows moved off it freed. Room held past a page's
//! rows is memory the budget's allowance cannot give to rows: at 4 KiB pages,
//! growth by a quarter left a fifth of the slots empty (doc 30 §9.1).
//!
//! Publication spends only what [`RangeStore::plan_sparse`] reserved: every
//! page it grows, splits or copies, every directory node and root it makes,
//! and its groups' staging are charged to reservations the plan took, so a
//! publication is never refused by the budget. It checks the candidate
//! against the store before it changes anything; past that point a failure is
//! an invariant broken, and the store is poisoned and refuses every plan and
//! publication after it.

use super::{
    ALLOCATOR_OVERHEAD, Allocation, BudgetKind, BudgetLane, Change, DirectoryBuild, Entry,
    MemoryBudget, MemoryError, Page, PageDirectory, RangeId, RangeStore, Rewrite, Root,
    bounded_push, bounded_vec, checked_add, checked_mul, groups, layout, merge_layout_charge,
    page_charge, root_charge, visit_merged,
};
use std::sync::Arc;

/// A candidate's checked changes and what their publication may spend.
pub struct SparseRange<K, V> {
    owner: crate::OwnerId,
    range: RangeId,
    base_prefix: u64,
    prefix: u64,
    /// The identity of the state it was planned on (the store's, or the one
    /// its predecessor makes), and of the state it makes: a candidate planned
    /// on a fork at the same prefix names another state and is refused.
    base_state: u64,
    state: u64,
    /// The most pages the store holds once this publishes: its base's, and
    /// two more a change (`PageDirectory::sparse_bound`).
    leaves: usize,
    /// Strictly ordered by key, each checked against the base state.
    changes: Vec<Change<K, V>>,
    /// The entries the store holds once this is published.
    len: usize,
    /// The heap its puts carry, and its deletions and the heap of the entries
    /// they name: what a write envelope bounds (`RangeWriteEnvelope::check_sparse`).
    incoming_heap: usize,
    deleted: usize,
    deleted_heap: usize,
    lane: BudgetLane,
    /// Pages its publication may grow, split or copy (`Pages`).
    pages: Allocation,
    /// Directory nodes and a root its publication may make (`Roots`).
    nodes: Allocation,
    /// A page group's merge staging while it is laid out (`Pending`): what a
    /// rebuild's group stages (`merge_layout_charge`), held from the plan so
    /// the publication never asks the budget.
    staging: Allocation,
    /// The batch itself: its vector and its rows' heap (`Pending`).
    _input: Allocation,
}

impl<K: crate::RangeKey, V> SparseRange<K, V> {
    pub fn id(&self) -> RangeId {
        self.range
    }
    /// The prefix of the state it was planned on.
    pub fn base_prefix(&self) -> u64 {
        self.base_prefix
    }
    pub fn prefix(&self) -> u64 {
        self.prefix
    }
    /// The entries the store holds once this is published.
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    /// Its changes, strictly ordered by key.
    pub fn changes(&self) -> &[Change<K, V>] {
        &self.changes
    }
    /// This candidate's own change of `key`: `Some(Some(value))` where it puts
    /// one, `Some(None)` where it deletes the key, `None` where it leaves it.
    pub fn get(&self, key: &K) -> Option<Option<&V>> {
        self.change(key)
            .map(|change| change.map(|entry| &entry.value))
    }
    /// [`Self::get`], the put's whole entry.
    pub fn change(&self, key: &K) -> Option<Option<&Entry<K, V>>> {
        match self.changes.get(self.position(key)?)? {
            Change::Put(entry) => Some(Some(entry)),
            Change::Delete(_) => Some(None),
        }
    }
    /// The index of its change of `key` among its changes, if it has one.
    pub fn position(&self, key: &K) -> Option<usize> {
        let probe = key.order_prefix();
        self.changes
            .binary_search_by(|change| {
                change
                    .key()
                    .order_prefix()
                    .cmp(&probe)
                    .then_with(|| change.key().cmp_past_prefix(key))
            })
            .ok()
    }
    /// What its batch itself is charged: its vector and its rows' heap.
    pub fn input_bytes(&self) -> usize {
        self._input.bytes()
    }
    /// What it holds for its groups' merge staging.
    pub fn staging_bytes(&self) -> usize {
        self.staging.bytes()
    }
    /// Whether this candidate was planned on the state `previous` makes, at
    /// its prefix, for the same store.
    pub fn follows(&self, previous: &Self) -> bool {
        self.owner == previous.owner
            && self.range == previous.range
            && self.base_prefix == previous.prefix
            && self.base_state == previous.state
    }
    /// What its publication may spend, pages and nodes together.
    pub fn reserved_bytes(&self) -> usize {
        self.pages.bytes().saturating_add(self.nodes.bytes())
    }
    pub(crate) fn owner(&self) -> crate::OwnerId {
        self.owner
    }
    /// (changed keys, input capacity, incoming heap, deletions, deleted heap)
    pub(crate) fn shape(&self) -> (usize, usize, usize, usize, usize) {
        (
            self.changes.len(),
            self.changes.capacity(),
            self.incoming_heap,
            self.deleted,
            self.deleted_heap,
        )
    }
    /// (input, pages, nodes, staging) reserved
    pub(crate) fn charges(&self) -> (usize, usize, usize, usize) {
        (
            self._input.bytes(),
            self.pages.bytes(),
            self.nodes.bytes(),
            self.staging.bytes(),
        )
    }
}

/// A refused publication: refused before anything changed, the candidate is
/// returned whole; failed past that point, the store is poisoned and the
/// candidate is gone.
pub struct SparseRefusal<K, V> {
    error: MemoryError,
    candidate: Option<SparseRange<K, V>>,
}

impl<K, V> SparseRefusal<K, V> {
    pub fn error(&self) -> &MemoryError {
        &self.error
    }
    /// Whether the failure poisoned the store.
    pub fn poisoned(&self) -> bool {
        self.candidate.is_none()
    }
    /// The error, and the candidate where it was refused whole.
    pub fn into_parts(self) -> (MemoryError, Option<SparseRange<K, V>>) {
        (self.error, self.candidate)
    }
    fn refused(error: MemoryError, candidate: SparseRange<K, V>) -> Self {
        Self {
            error,
            candidate: Some(candidate),
        }
    }
}

/// The state a candidate is planned on: the store's, or the one an unpublished
/// predecessor makes.
pub enum SparseBase<'a, K, V> {
    Committed,
    After(&'a SparseRange<K, V>),
}

impl<K: crate::RangeKey + Clone, V> RangeStore<K, V> {
    /// Plan `changes` at the prefix after `base`'s, `present` giving the heap
    /// of the entry the base state holds at a key, if any: the store's
    /// committed root with its unpublished predecessors' changes over it,
    /// which its caller holds (doc 30 §9). Checks
    /// the batch as [`Self::plan_batch`] does (the next prefix, at most
    /// `max_entries` changes, no key twice, each entry within the leaf bound,
    /// no key deleted that the base does not hold) and reserves what its
    /// publication may spend, by the bounds a write envelope states for a
    /// write of its shape ([`Self::future_write_envelope`]): three new leaves'
    /// headers, a leaf's payload and an entry a change, its incoming heap, and
    /// the directory edits and root those may take. A page merged in place
    /// spends a fraction of that; one copied because a capture still reads it
    /// spends at most a leaf. Everything is reserved from `source`, the
    /// store's budget or a pool funded from it.
    #[allow(clippy::too_many_arguments)]
    pub fn plan_sparse(
        &self,
        source: &MemoryBudget,
        base: SparseBase<'_, K, V>,
        prefix: u64,
        changes: Vec<Change<K, V>>,
        lane: BudgetLane,
        max_entries: usize,
        present: impl FnMut(&K) -> Option<usize>,
    ) -> Result<SparseRange<K, V>, MemoryError> {
        self.plan_sparse_from(
            source,
            None,
            base,
            prefix,
            changes,
            lane,
            max_entries,
            present,
        )
    }

    /// [`Self::plan_sparse`], the changes funded by the caller: what they
    /// take (their vector and their heap) is split from `input`, which the
    /// caller reserved with them (a replayed record's), never charged twice.
    /// Refused, `input` untouched, where it holds less.
    #[allow(clippy::too_many_arguments)]
    pub fn plan_sparse_funded(
        &self,
        source: &MemoryBudget,
        input: &mut Allocation,
        base: SparseBase<'_, K, V>,
        prefix: u64,
        changes: Vec<Change<K, V>>,
        lane: BudgetLane,
        max_entries: usize,
        present: impl FnMut(&K) -> Option<usize>,
    ) -> Result<SparseRange<K, V>, MemoryError> {
        self.plan_sparse_from(
            source,
            Some(input),
            base,
            prefix,
            changes,
            lane,
            max_entries,
            present,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_sparse_from(
        &self,
        source: &MemoryBudget,
        funding: Option<&mut Allocation>,
        base: SparseBase<'_, K, V>,
        prefix: u64,
        mut changes: Vec<Change<K, V>>,
        lane: BudgetLane,
        max_entries: usize,
        mut present: impl FnMut(&K) -> Option<usize>,
    ) -> Result<SparseRange<K, V>, MemoryError> {
        let (base_prefix, base_len, base_state, base_leaves) = match base {
            SparseBase::Committed => (self.prefix(), self.len(), self.state, self.root.pages.len()),
            SparseBase::After(previous) => {
                if previous.range != self.id() || previous.owner != self.root.owner {
                    return Err(MemoryError::WrongRange);
                }
                (
                    previous.prefix,
                    previous.len,
                    previous.state,
                    previous.leaves,
                )
            }
        };
        self.check_live()?;
        if !source.is_within(&self.budget) {
            return Err(MemoryError::InvalidConfiguration(
                "a range's allocation source is outside its budget",
            ));
        }
        if base_prefix < self.prefix() {
            return Err(MemoryError::StalePreparation {
                prepared_at: base_prefix,
                current_prefix: self.prefix(),
            });
        }
        let expected = base_prefix
            .checked_add(1)
            .ok_or(MemoryError::CounterExhausted("published prefix"))?;
        if prefix != expected {
            return Err(MemoryError::PrefixMismatch {
                expected,
                actual: prefix,
            });
        }
        if changes.len() > max_entries {
            return Err(MemoryError::Capacity {
                requested: changes.len(),
                available: max_entries,
            });
        }
        changes.sort_unstable_by(|left, right| left.key().cmp(right.key()));
        if changes.windows(2).any(|pair| {
            pair.first()
                .zip(pair.last())
                .is_some_and(|(left, right)| left.key() == right.key())
        }) {
            return Err(MemoryError::DuplicateKey);
        }
        let mut len = base_len;
        let mut input = if changes.capacity() == 0 {
            0
        } else {
            checked_add(
                ALLOCATOR_OVERHEAD,
                checked_mul(changes.capacity(), size_of::<Change<K, V>>())?,
            )?
        };
        let (mut incoming_heap, mut deleted, mut deleted_heap) = (0usize, 0usize, 0usize);
        for change in &changes {
            let held = present(change.key());
            match change {
                Change::Put(entry) => {
                    layout::check_entry::<K, V>(entry.heap_bytes, self.config)?;
                    incoming_heap = checked_add(incoming_heap, entry.heap_bytes)?;
                    if held.is_none() {
                        len = checked_add(len, 1)?;
                    }
                }
                Change::Delete(_) => {
                    let heap = held.ok_or(MemoryError::MissingKey)?;
                    deleted = checked_add(deleted, 1)?;
                    deleted_heap = checked_add(deleted_heap, heap)?;
                    len = len.checked_sub(1).ok_or(MemoryError::MissingKey)?;
                }
            }
        }
        input = checked_add(input, incoming_heap)?;
        let leaves = checked_add(base_leaves, checked_mul(2, changes.len())?)?;
        let (pages, nodes, staging) = self.sparse_quote(changes.len(), incoming_heap, leaves)?;
        if let Some(funding) = funding.as_ref()
            && funding.bytes() < input
        {
            return Err(MemoryError::Capacity {
                requested: input,
                available: funding.bytes(),
            });
        }
        let pages = source.reserve(BudgetKind::Pages, lane, pages)?.commit();
        let nodes = source.reserve(BudgetKind::Roots, lane, nodes)?.commit();
        let staging = source.reserve(BudgetKind::Pending, lane, staging)?.commit();
        let input = match funding {
            Some(funding) => funding.split_off(input)?,
            None => source.reserve(BudgetKind::Pending, lane, input)?.commit(),
        };
        Ok(SparseRange {
            owner: self.root.owner,
            range: self.id(),
            base_prefix,
            prefix,
            base_state,
            state: self.fresh_state(),
            leaves,
            changes,
            len,
            incoming_heap,
            deleted,
            deleted_heap,
            lane,
            pages,
            nodes,
            staging,
            _input: input,
        })
    }

    /// The page, node and staging bytes a publication of `changed` keys
    /// carrying `incoming_heap` may spend: a write envelope's
    /// `new_pages_bytes`, `directory_bytes` for one member and
    /// `merge_pending_bytes`, so a sparse plan fits any envelope its shape
    /// fits.
    fn sparse_quote(
        &self,
        changed: usize,
        incoming_heap: usize,
        leaves: usize,
    ) -> Result<(usize, usize, usize), MemoryError> {
        let owner_limit = self.budget.limit();
        let page_header = page_charge::<K, V>(0, 0)?;
        let minimum_page = page_charge::<K, V>(1, 0)?;
        let entry_inline = size_of::<Entry<K, V>>();
        let max_base_pages =
            owner_limit
                .checked_div(minimum_page)
                .ok_or(MemoryError::InvalidConfiguration(
                    "a directory page must have a nonzero charge",
                ))?;
        let ordinary_payload = self
            .config
            .page_bytes
            .min(owner_limit)
            .saturating_sub(page_header);
        let max_new_pages = checked_mul(3, changed)?;
        let pages = checked_add(
            checked_mul(max_new_pages, page_header)?,
            checked_add(
                checked_mul(changed, ordinary_payload)?,
                checked_add(checked_mul(changed, entry_inline)?, incoming_heap)?,
            )?,
        )?;
        // Nodes for the directory as it can stand when this publishes: its
        // base's pages and two more a change, never more than the owner's
        // memory holds (a write envelope's bound).
        let nodes = checked_add(
            root_charge::<K, V>(0)?,
            PageDirectory::<K, V>::sparse_bound(
                changed,
                leaves.min(checked_add(max_base_pages, max_new_pages)?),
            )?,
        )?;
        // A group stages its page's rows and its changes; a page holds no more
        // rows than a leaf of ordinary payload, or one oversized row.
        let old_rows = self
            .config
            .page_entries
            .min(ordinary_payload.checked_div(entry_inline).ok_or(
                MemoryError::InvalidConfiguration("a range entry must have a nonzero charge"),
            )?)
            .max(usize::from(max_base_pages != 0));
        let staging = if changed == 0 {
            0
        } else {
            merge_layout_charge::<K, V>(checked_add(old_rows, changed)?, self.partition.is_some())?
        };
        Ok((pages, nodes, staging))
    }

    /// Publish `sparse` after durable commitment, in order (its base is the
    /// store's prefix). Each page is merged where it stands when the store
    /// alone holds it and copied (`copy` for each retained value) where
    /// something else still reads it; splits move entries, never copy them.
    /// Everything it spends comes from what its plan reserved.
    #[allow(clippy::result_large_err)] // A refused candidate is returned whole, never dropped.
    pub fn publish_sparse<F>(
        &mut self,
        sparse: SparseRange<K, V>,
        mut copy: F,
    ) -> Result<(), SparseRefusal<K, V>>
    where
        F: FnMut(&V) -> Result<V, MemoryError>,
    {
        if let Err(error) = self.check_sparse(&sparse) {
            return Err(SparseRefusal::refused(error, sparse));
        }
        // From here the store changes: a failure is an invariant broken.
        let result = self.apply_sparse(sparse, &mut copy);
        if let Err(error) = result {
            self.poisoned = true;
            return Err(SparseRefusal {
                error,
                candidate: None,
            });
        }
        Ok(())
    }

    /// Whether `sparse` publishes onto this store as it stands: planned for
    /// this store and its owner, on its current prefix, the store unpoisoned.
    /// [`Self::publish_sparse`] makes the same check; a group checks every
    /// member's candidate before it publishes any.
    pub fn check_sparse(&self, sparse: &SparseRange<K, V>) -> Result<(), MemoryError> {
        self.check_live()?;
        if sparse.range != self.id() || sparse.owner != self.root.owner {
            return Err(MemoryError::WrongRange);
        }
        if sparse.base_prefix != self.prefix() {
            return Err(MemoryError::StalePreparation {
                prepared_at: sparse.base_prefix,
                current_prefix: self.prefix(),
            });
        }
        if sparse.base_state != self.state {
            // Planned on another state at this prefix: a fork.
            return Err(MemoryError::WrongRange);
        }
        Ok(())
    }

    fn check_live(&self) -> Result<(), MemoryError> {
        if self.poisoned {
            return Err(MemoryError::InvalidConfiguration(
                "range store poisoned by a failed publication",
            ));
        }
        Ok(())
    }

    /// Visits each row [`Self::publish_sparse`] copies when something else
    /// still holds the pages `sparse` edits: every row of an edited page that
    /// its changes neither replace nor delete, its pages grouped as the
    /// publication groups them (a page it leaves as it is copies nothing). A
    /// caller that meters what a publication may do pays for these here,
    /// before anything changes: a publication is never refused once begun.
    /// `sparse` must publish onto this store as it stands.
    pub fn visit_retained(
        &self,
        sparse: &SparseRange<K, V>,
        mut visit: impl FnMut(&V) -> Result<(), MemoryError>,
    ) -> Result<(), MemoryError> {
        self.check_sparse(sparse)?;
        let shape = Shape::<K>::new::<V>(self.config, self.partition)?;
        // Groups from the end, as the publication takes them: its edits lie
        // past every group left, so the groups read here off the root as it
        // stands are the ones it edits.
        let mut end = sparse.changes.len();
        loop {
            let remaining = sparse.changes.get(..end).ok_or(MemoryError::MissingKey)?;
            let Some(group) = groups::last(&self.root, remaining)? else {
                return Ok(());
            };
            let floor = end
                .checked_sub(group.count)
                .ok_or(MemoryError::MissingKey)?;
            let changes = remaining.get(floor..).ok_or(MemoryError::MissingKey)?;
            if let Some(page) = group.page
                && layout::reusable_page(page, changes, shape.config, shape.partition)?.is_none()
            {
                retained(page, changes, &mut visit)?;
            }
            end = floor;
        }
    }

    fn apply_sparse<F>(
        &mut self,
        sparse: SparseRange<K, V>,
        copy: &mut F,
    ) -> Result<(), MemoryError>
    where
        F: FnMut(&V) -> Result<V, MemoryError>,
    {
        let SparseRange {
            prefix,
            state,
            mut changes,
            len,
            lane,
            mut pages,
            mut nodes,
            mut staging,
            _input,
            ..
        } = sparse;
        self.own_root(&mut nodes)?;
        let shape = Shape::<K>::new::<V>(self.config, self.partition)?;
        let root = Arc::get_mut(&mut self.root).ok_or(MemoryError::WrongRange)?;
        let mut build = DirectoryBuild::held(&mut nodes, lane);
        // Groups are taken from the end: each takes its changes off the end of
        // the batch, and the pages it edits or adds lie past every group left.
        while let Some(group) = groups::last(root, &changes)? {
            let (rank, count) = (group.rank, group.count);
            let floor = changes
                .len()
                .checked_sub(count)
                .ok_or(MemoryError::MissingKey)?;
            let held = group.page.map_or(0, |page| page.entries.len());
            // The group's descriptors, cuts and staged pages live under this.
            let mut hold = staging.split_off(merge_layout_charge::<K, V>(
                checked_add(held, count)?,
                shape.partition.is_some(),
            )?)?;
            let step = match group.page {
                None => Step::Fresh,
                Some(page) => {
                    let group = changes.get(floor..).ok_or(MemoryError::MissingKey)?;
                    // An unchanged isolated page (an oversized singleton, or a
                    // partition's page the changes only border) stays as it
                    // is, whoever holds it, as a rebuild keeps it: the changes
                    // before it and after it go in pages of their own. Its
                    // rows never move, so an oversized row is never charged
                    // to a write that does not change it.
                    match layout::reusable_page(page, group, shape.config, shape.partition)? {
                        Some(split) => Step::Around(split),
                        None => Step::Merge(shape.plan(page, group)?),
                    }
                }
            };
            match step {
                // An empty store: the group is every change, all puts.
                Step::Fresh => {
                    let leaves = shape.fresh(&mut changes, floor, &mut pages)?;
                    insert_all(&mut root.pages, 0, leaves, &mut build)?;
                }
                Step::Around(split) => {
                    let after = checked_add(floor, split)?;
                    let leaves = shape.fresh(&mut changes, after, &mut pages)?;
                    insert_all(&mut root.pages, checked_add(rank, 1)?, leaves, &mut build)?;
                    let leaves = shape.fresh(&mut changes, floor, &mut pages)?;
                    insert_all(&mut root.pages, rank, leaves, &mut build)?;
                }
                Step::Merge(Plan::Gone) => {
                    // Every row deleted; the plan found each.
                    changes.truncate(floor);
                    root.pages.remove_owned(rank, &mut build)?;
                }
                Step::Merge(plan) => {
                    let edited = root.pages.edit_page(rank, &mut build, |slot| {
                        match Arc::get_mut(slot) {
                            Some(page) => shape
                                .in_place(page, &mut changes, floor, &plan, &mut pages)
                                .map(Some),
                            // Something else still reads this page: it is copied.
                            None => Ok(None),
                        }
                    })?;
                    match edited {
                        Some(tails) => {
                            insert_all(&mut root.pages, checked_add(rank, 1)?, tails, &mut build)?;
                        }
                        None => {
                            let old = root
                                .pages
                                .get(rank)
                                .map(Arc::clone)
                                .ok_or(MemoryError::MissingKey)?;
                            let mut leaves = shape
                                .copied(&old, &mut changes, floor, &plan, copy, &mut pages)?
                                .into_iter();
                            let first = leaves.next().ok_or(MemoryError::MissingKey)?;
                            root.pages.replace_owned(rank, first, &mut build)?;
                            insert_all(&mut root.pages, checked_add(rank, 1)?, leaves, &mut build)?;
                            drop(old);
                        }
                    }
                }
            }
            staging.absorb(&mut hold)?;
        }
        root.len = len;
        root.prefix = prefix;
        self.state = state;
        Ok(())
    }

    /// Replaces the value of `key`'s row where it stands with what `rewrite`
    /// makes of it (a row of no more heap than it held), letting go of its
    /// page's charge by the difference: the same key in the same page, and
    /// nothing else changed, so the store's prefix and state stay its own and
    /// a plan made on it publishes as it would have. Where a frozen image or a
    /// lease still reads the page or a node above it, nothing is rewritten
    /// ([`Rewrite::Shared`]); what a publication just wrote is the store's
    /// own. A value moved off the heap (a row spilled to a log) is what this
    /// is for.
    pub fn rewrite<F>(&mut self, key: &K, rewrite: F) -> Result<Rewrite, MemoryError>
    where
        F: FnOnce(&V) -> Result<Option<(V, usize)>, MemoryError>,
    {
        self.check_live()?;
        let Some(root) = Arc::get_mut(&mut self.root) else {
            return Ok(Rewrite::Shared);
        };
        if root.len == 0 {
            return Ok(Rewrite::Absent);
        }
        let index = root.page_index(key);
        let Some(slot) = root.pages.page_owned_mut(index)? else {
            return Ok(Rewrite::Shared);
        };
        let Some(page) = Arc::get_mut(slot) else {
            return Ok(Rewrite::Shared);
        };
        page.rewrite(key, rewrite)
    }

    /// Makes the store's root its own: a root something else holds (a frozen
    /// capture, a lease) is copied, its directory shared until an edit copies
    /// the nodes on its path.
    fn own_root(&mut self, nodes: &mut Allocation) -> Result<(), MemoryError> {
        if Arc::get_mut(&mut self.root).is_some() {
            return Ok(());
        }
        let allocation = nodes.split_off(root_charge::<K, V>(0)?)?;
        let root = Root {
            owner: self.root.owner,
            range: self.root.range,
            prefix: self.root.prefix,
            pages: self.root.pages.shared(),
            len: self.root.len,
            _allocation: allocation,
        };
        self.root = Arc::new(root);
        Ok(())
    }
}

/// Inserts more rows than this into a page by one pass that moves its rows
/// into a vector of the leaf's size; fewer, each where it goes.
const IN_PLACE_INSERTS: usize = 4;

/// What a publication does with a page group: lay an empty store's changes
/// out fresh, put them in pages of their own around a page that stays as it
/// is, or merge them with the page's rows.
enum Step {
    Fresh,
    Around(usize),
    Merge(Plan),
}

/// How a page group is laid out, read before the page changes: every row
/// deleted, one leaf of `len` rows, or leaves of these lengths in order (the
/// cut rule, `layout::Cutter`).
enum Plan {
    Gone,
    One { len: usize, held: usize },
    Many { lengths: Vec<usize>, held: usize },
}

/// The store's leaf rule, as a publication reads it.
struct Shape<K> {
    config: super::RangeConfig,
    partition: Option<fn(&K) -> u64>,
    cutter: layout::Cutter,
}

impl<K: crate::RangeKey + Clone> Shape<K> {
    fn new<V>(
        config: super::RangeConfig,
        partition: Option<fn(&K) -> u64>,
    ) -> Result<Self, MemoryError> {
        Ok(Self {
            config,
            partition,
            cutter: layout::Cutter::new::<K, V>(config)?,
        })
    }

    /// How `group`, the changes that go in `page`, lay out with its rows:
    /// one leaf where the rows left fit one (one partition, both bounds), so
    /// an edit costs its changes; otherwise the cut rule over the merged rows.
    fn plan<V>(&self, page: &Page<K, V>, group: &[Change<K, V>]) -> Result<Plan, MemoryError> {
        // The heap the page's rows hold now, summed once for the plan and the merge.
        let held = heap_of(&page.entries)?;
        let mut heap = held;
        let label = self
            .partition
            .and_then(|classify| page.entries.first().map(|entry| classify(&entry.key)));
        let (mut inserts, mut deletes, mut one_partition) = (0usize, 0usize, true);
        for change in group {
            let found = search(&page.entries, change.key());
            match (change, found) {
                (Change::Put(entry), Ok(at)) => {
                    let held = page.entries.get(at).ok_or(MemoryError::MissingKey)?;
                    heap = checked_add(heap, entry.heap_bytes)?
                        .checked_sub(held.heap_bytes)
                        .ok_or(MemoryError::MissingKey)?;
                }
                (Change::Put(entry), Err(_)) => {
                    inserts = checked_add(inserts, 1)?;
                    heap = checked_add(heap, entry.heap_bytes)?;
                    if let (Some(classify), Some(label)) = (self.partition, label) {
                        one_partition &= classify(&entry.key) == label;
                    }
                }
                (Change::Delete(_), Ok(at)) => {
                    let held = page.entries.get(at).ok_or(MemoryError::MissingKey)?;
                    deletes = checked_add(deletes, 1)?;
                    heap = heap
                        .checked_sub(held.heap_bytes)
                        .ok_or(MemoryError::MissingKey)?;
                }
                (Change::Delete(_), Err(_)) => return Err(MemoryError::MissingKey),
            }
        }
        let len = checked_add(page.entries.len(), inserts)?
            .checked_sub(deletes)
            .ok_or(MemoryError::MissingKey)?;
        if len == 0 {
            return Ok(Plan::Gone);
        }
        if one_partition
            && len <= self.config.page_entries
            && (len == 1 || page_charge::<K, V>(len, heap)? <= self.config.page_bytes)
        {
            return Ok(Plan::One { len, held });
        }
        self.lengths(&page.entries, group)
            .map(|lengths| Plan::Many { lengths, held })
    }

    /// The lengths of the leaves `old` and `group` merge into, in order.
    fn lengths<V>(
        &self,
        old: &[Entry<K, V>],
        group: &[Change<K, V>],
    ) -> Result<Vec<usize>, MemoryError> {
        match self.partition {
            Some(classify) => self.lengths_by(old, group, classify),
            None => self.lengths_by(old, group, |_: &K| ()),
        }
    }

    fn lengths_by<V, P: Copy + Eq>(
        &self,
        old: &[Entry<K, V>],
        group: &[Change<K, V>],
        classify: impl Fn(&K) -> P,
    ) -> Result<Vec<usize>, MemoryError> {
        let capacity = checked_add(old.len(), group.len())?;
        let mut merged = bounded_vec(capacity)?;
        visit_merged(
            old,
            group,
            |key, entry| bounded_push(&mut merged, (entry, classify(key)), capacity),
            &mut |_, _| Ok(()),
        )?;
        let bound = merged.len();
        let mut scratch = bounded_vec(bound)?;
        let mut lengths = bounded_vec(bound)?;
        self.cutter.cut(&merged, &mut scratch, &mut |span| {
            bounded_push(&mut lengths, span.len, bound)
        })?;
        Ok(lengths)
    }

    /// The changes from `floor`, all puts and of no page, in leaves of their
    /// own, in order.
    fn fresh<V>(
        &self,
        changes: &mut Vec<Change<K, V>>,
        floor: usize,
        funding: &mut Allocation,
    ) -> Result<Vec<Arc<Page<K, V>>>, MemoryError> {
        let lengths = self.lengths(&[], changes.get(floor..).ok_or(MemoryError::MissingKey)?)?;
        build_from_end(&lengths, &mut NoRows, changes, floor, funding)
    }

    /// `old`, a page something else still reads, merged with the changes from
    /// `floor` into new leaves laid out by `plan`, each retained value copied
    /// with `copy`, in order.
    fn copied<V, F>(
        &self,
        old: &Page<K, V>,
        changes: &mut Vec<Change<K, V>>,
        floor: usize,
        plan: &Plan,
        copy: &mut F,
        funding: &mut Allocation,
    ) -> Result<Vec<Arc<Page<K, V>>>, MemoryError>
    where
        F: FnMut(&V) -> Result<V, MemoryError>,
    {
        let one;
        let lengths: &[usize] = match plan {
            Plan::Gone => &[],
            Plan::One { len, .. } => {
                one = [*len];
                &one
            }
            Plan::Many { lengths, .. } => lengths,
        };
        let mut rows = Copied {
            rows: &old.entries,
            end: old.entries.len(),
            copy,
        };
        build_from_end(lengths, &mut rows, changes, floor, funding)
    }

    /// Merges the changes from `floor` into `page` where it stands, laid out
    /// by `plan`: the leaves past its first are built from its end, their rows
    /// moved off the page, and handed back in order; the first stays the page.
    fn in_place<V>(
        &self,
        page: &mut Page<K, V>,
        changes: &mut Vec<Change<K, V>>,
        floor: usize,
        plan: &Plan,
        funding: &mut Allocation,
    ) -> Result<Vec<Arc<Page<K, V>>>, MemoryError> {
        let (first, rest, mut heap): (usize, &[usize], usize) = match plan {
            Plan::Gone => {
                return Err(MemoryError::InvalidConfiguration(
                    "a page left empty is removed, not edited",
                ));
            }
            Plan::One { len, held } => (*len, &[], *held),
            Plan::Many { lengths, held } => match lengths.split_first() {
                Some((first, rest)) => (*first, rest, *held),
                None => return Err(MemoryError::MissingKey),
            },
        };
        // The page keeps its charge until its own merge is done: the rows
        // moved off it go with their leaves, charged as each is built.
        let mut tails = bounded_vec(rest.len())?;
        for len in rest.iter().rev() {
            let leaf = leaf_from_end(*len, &mut Moved(&mut page.entries), changes, floor)?;
            heap = heap
                .checked_sub(checked_add(leaf.taken, leaf.dropped)?)
                .ok_or(MemoryError::MissingKey)?;
            let allocation =
                funding.split_off(page_charge::<K, V>(leaf.entries.capacity(), leaf.heap)?)?;
            bounded_push(
                &mut tails,
                Arc::new(Page::new(leaf.entries, allocation)),
                rest.len(),
            )?;
        }
        tails.reverse();
        self.merge_rest(page, changes, floor, &mut heap, funding)?;
        let len = page.entries.len();
        if len != first {
            return Err(MemoryError::InvalidConfiguration(
                "an in-place merge left other than its leaf",
            ));
        }
        // The page holds exactly its rows: what its deletions and the rows
        // moved off it freed goes back.
        if page.entries.capacity() > len {
            page.entries.shrink_to_fit();
        }
        page.first = page
            .entries
            .first()
            .map_or(0, |entry| entry.key.order_prefix());
        charge_to(page, heap, funding)?;
        Ok(tails)
    }

    /// Merges the changes from `floor` into `page` where it stands: deletions
    /// first, so the page never holds more rows than its leaf; up to
    /// [`IN_PLACE_INSERTS`] inserts each where it goes, more by one pass that
    /// moves the rows into a vector of the leaf's size. A page that grows
    /// grows by its inserts alone. `heap` follows the rows.
    fn merge_rest<V>(
        &self,
        page: &mut Page<K, V>,
        changes: &mut Vec<Change<K, V>>,
        floor: usize,
        heap: &mut usize,
        funding: &mut Allocation,
    ) -> Result<(), MemoryError> {
        let group = changes.get(floor..).ok_or(MemoryError::MissingKey)?;
        let mut inserts = 0usize;
        for change in group {
            match change {
                Change::Delete(key) => {
                    let at = search(&page.entries, key).map_err(|_| MemoryError::MissingKey)?;
                    if at >= page.entries.len() {
                        return Err(MemoryError::MissingKey);
                    }
                    let gone = page.entries.remove(at);
                    *heap = heap
                        .checked_sub(gone.heap_bytes)
                        .ok_or(MemoryError::MissingKey)?;
                }
                Change::Put(entry) => {
                    if search(&page.entries, &entry.key).is_err() {
                        inserts = checked_add(inserts, 1)?;
                    }
                }
            }
        }
        let wanted = checked_add(page.entries.len(), inserts)?;
        if inserts <= IN_PLACE_INSERTS {
            if wanted > page.entries.capacity() {
                page.entries
                    .try_reserve_exact(wanted.saturating_sub(page.entries.len()))
                    .map_err(|_| MemoryError::AllocationFailed)?;
            }
            for change in changes.drain(floor..) {
                let Change::Put(entry) = change else {
                    continue;
                };
                match search(&page.entries, &entry.key) {
                    Ok(at) => {
                        let slot = page.entries.get_mut(at).ok_or(MemoryError::MissingKey)?;
                        *heap = checked_add(*heap, entry.heap_bytes)?
                            .checked_sub(slot.heap_bytes)
                            .ok_or(MemoryError::MissingKey)?;
                        drop(std::mem::replace(slot, entry));
                    }
                    Err(at) => {
                        if at > page.entries.len() || page.entries.len() >= page.entries.capacity()
                        {
                            return Err(MemoryError::InvalidConfiguration(
                                "an in-place merge outgrew its page",
                            ));
                        }
                        *heap = checked_add(*heap, entry.heap_bytes)?;
                        page.entries.insert(at, entry);
                    }
                }
            }
            return Ok(());
        }
        // The new vector is charged while the old one is moved into it.
        let mut transient = funding.split_off(staging_bytes::<K, V>(wanted)?)?;
        let fresh = bounded_vec(wanted)?;
        let mut old = std::mem::replace(&mut page.entries, fresh)
            .into_iter()
            .peekable();
        for change in changes.drain(floor..) {
            let Change::Put(entry) = change else {
                continue;
            };
            while let Some(row) = old.next_if(|row| row.key < entry.key) {
                bounded_push(&mut page.entries, row, wanted)?;
            }
            if old.peek().is_some_and(|row| row.key == entry.key)
                && let Some(gone) = old.next()
            {
                *heap = heap
                    .checked_sub(gone.heap_bytes)
                    .ok_or(MemoryError::MissingKey)?;
            }
            *heap = checked_add(*heap, entry.heap_bytes)?;
            bounded_push(&mut page.entries, entry, wanted)?;
        }
        for row in old {
            bounded_push(&mut page.entries, row, wanted)?;
        }
        funding.absorb(&mut transient)
    }
}

/// The rows a page group's leaves are built from, taken from the end.
trait Rows<K, V> {
    fn last_key(&self) -> Option<&K>;
    /// The last row, for a leaf.
    fn take_last(&mut self) -> Result<Entry<K, V>, MemoryError>;
    /// The last row, which a change replaced or deleted; its heap.
    fn drop_last(&mut self) -> Result<usize, MemoryError>;
}

/// A page the store alone holds: its rows are moved off its end.
struct Moved<'a, K, V>(&'a mut Vec<Entry<K, V>>);

impl<K, V> Rows<K, V> for Moved<'_, K, V> {
    fn last_key(&self) -> Option<&K> {
        self.0.last().map(|row| &row.key)
    }
    fn take_last(&mut self) -> Result<Entry<K, V>, MemoryError> {
        self.0.pop().ok_or(MemoryError::MissingKey)
    }
    fn drop_last(&mut self) -> Result<usize, MemoryError> {
        self.0
            .pop()
            .map(|row| row.heap_bytes)
            .ok_or(MemoryError::MissingKey)
    }
}

/// A page something else still reads: its rows are copied, it is left as it is.
struct Copied<'a, K, V, F> {
    rows: &'a [Entry<K, V>],
    end: usize,
    copy: &'a mut F,
}

impl<K: Clone, V, F> Rows<K, V> for Copied<'_, K, V, F>
where
    F: FnMut(&V) -> Result<V, MemoryError>,
{
    fn last_key(&self) -> Option<&K> {
        self.end
            .checked_sub(1)
            .and_then(|at| self.rows.get(at))
            .map(|row| &row.key)
    }
    fn take_last(&mut self) -> Result<Entry<K, V>, MemoryError> {
        let at = self.end.checked_sub(1).ok_or(MemoryError::MissingKey)?;
        let row = self.rows.get(at).ok_or(MemoryError::MissingKey)?;
        let entry = Entry {
            key: row.key.clone(),
            value: (self.copy)(&row.value)?,
            heap_bytes: row.heap_bytes,
        };
        self.end = at;
        Ok(entry)
    }
    fn drop_last(&mut self) -> Result<usize, MemoryError> {
        let at = self.end.checked_sub(1).ok_or(MemoryError::MissingKey)?;
        let heap = self.rows.get(at).ok_or(MemoryError::MissingKey)?.heap_bytes;
        self.end = at;
        Ok(heap)
    }
}

/// No page: a run of changes alone.
struct NoRows;

impl<K, V> Rows<K, V> for NoRows {
    fn last_key(&self) -> Option<&K> {
        None
    }
    fn take_last(&mut self) -> Result<Entry<K, V>, MemoryError> {
        Err(MemoryError::MissingKey)
    }
    fn drop_last(&mut self) -> Result<usize, MemoryError> {
        Err(MemoryError::MissingKey)
    }
}

/// A leaf built from a group's end: its rows, their heap, and the heap of the
/// old rows it took and of those its changes dropped.
struct Leaf<K, V> {
    entries: Vec<Entry<K, V>>,
    heap: usize,
    taken: usize,
    dropped: usize,
}

/// The leaf of the last `len` rows a group leaves: `rows`' last merged with
/// the last of the changes from `floor`, a change replacing or deleting the
/// row of its key, in a vector of exactly `len`.
fn leaf_from_end<K: crate::RangeKey, V>(
    len: usize,
    rows: &mut impl Rows<K, V>,
    changes: &mut Vec<Change<K, V>>,
    floor: usize,
) -> Result<Leaf<K, V>, MemoryError> {
    enum Next {
        Row,
        Change { matched: bool },
    }
    let mut entries = bounded_vec(len)?;
    let (mut heap, mut taken, mut dropped) = (0usize, 0usize, 0usize);
    while entries.len() < len {
        let change = changes.last().filter(|_| changes.len() > floor);
        let next = match (rows.last_key(), change) {
            (Some(row), Some(change)) => match row.cmp(change.key()) {
                std::cmp::Ordering::Greater => Next::Row,
                std::cmp::Ordering::Equal => Next::Change { matched: true },
                std::cmp::Ordering::Less => Next::Change { matched: false },
            },
            (Some(_), None) => Next::Row,
            (None, Some(_)) => Next::Change { matched: false },
            (None, None) => return Err(MemoryError::MissingKey),
        };
        match next {
            Next::Row => {
                let row = rows.take_last()?;
                taken = checked_add(taken, row.heap_bytes)?;
                heap = checked_add(heap, row.heap_bytes)?;
                bounded_push(&mut entries, row, len)?;
            }
            Next::Change { matched } => {
                if matched {
                    dropped = checked_add(dropped, rows.drop_last()?)?;
                }
                match changes.pop().ok_or(MemoryError::MissingKey)? {
                    Change::Put(entry) => {
                        heap = checked_add(heap, entry.heap_bytes)?;
                        bounded_push(&mut entries, entry, len)?;
                    }
                    Change::Delete(_) if matched => {}
                    Change::Delete(_) => return Err(MemoryError::MissingKey),
                }
            }
        }
    }
    entries.reverse();
    Ok(Leaf {
        entries,
        heap,
        taken,
        dropped,
    })
}

/// Builds a group's leaves of `lengths` from its end, each charged to
/// `funding`, and hands them back in order. What the group has left past its
/// first leaf are deletions of the rows before it, each with its row.
fn build_from_end<K: crate::RangeKey, V>(
    lengths: &[usize],
    rows: &mut impl Rows<K, V>,
    changes: &mut Vec<Change<K, V>>,
    floor: usize,
    funding: &mut Allocation,
) -> Result<Vec<Arc<Page<K, V>>>, MemoryError> {
    let mut leaves = bounded_vec(lengths.len())?;
    for len in lengths.iter().rev() {
        let leaf = leaf_from_end(*len, rows, changes, floor)?;
        let allocation =
            funding.split_off(page_charge::<K, V>(leaf.entries.capacity(), leaf.heap)?)?;
        bounded_push(
            &mut leaves,
            Arc::new(Page::new(leaf.entries, allocation)),
            lengths.len(),
        )?;
    }
    while changes.len() > floor {
        let matched = match (rows.last_key(), changes.last()) {
            (Some(row), Some(Change::Delete(key))) => row == key,
            _ => false,
        };
        if !matched {
            return Err(MemoryError::MissingKey);
        }
        rows.drop_last()?;
        changes.pop();
    }
    if rows.last_key().is_some() {
        return Err(MemoryError::MissingKey);
    }
    leaves.reverse();
    Ok(leaves)
}

/// Inserts `leaves` in order from `at`.
fn insert_all<K: crate::RangeKey, V>(
    pages: &mut PageDirectory<K, V>,
    at: usize,
    leaves: impl IntoIterator<Item = Arc<Page<K, V>>>,
    build: &mut DirectoryBuild<'_>,
) -> Result<(), MemoryError> {
    let mut at = at;
    for leaf in leaves {
        pages.insert_owned(at, leaf, build)?;
        at = checked_add(at, 1)?;
    }
    Ok(())
}

/// Each row of `page` that `group`, its changes in key order, neither
/// replaces nor deletes.
fn retained<K: crate::RangeKey, V>(
    page: &Page<K, V>,
    group: &[Change<K, V>],
    visit: &mut impl FnMut(&V) -> Result<(), MemoryError>,
) -> Result<(), MemoryError> {
    let mut changes = group.iter().peekable();
    for row in &page.entries {
        while changes.next_if(|change| change.key() < &row.key).is_some() {}
        if changes.next_if(|change| change.key() == &row.key).is_none() {
            visit(&row.value)?;
        }
    }
    Ok(())
}

/// Where `key` is in `entries`, or would go: its prefix once, each entry's
/// against it, past it only where they tie.
fn search<K: crate::RangeKey, V>(entries: &[Entry<K, V>], key: &K) -> Result<usize, usize> {
    let probe = key.order_prefix();
    entries.binary_search_by(|entry| {
        entry
            .key
            .order_prefix()
            .cmp(&probe)
            .then_with(|| entry.key.cmp_past_prefix(key))
    })
}

/// Makes a page's charge what its entries and their capacity take, from
/// `funding`. The charge it held goes back to `funding` first, so an edit
/// needs only what it grew by, where both are of one owner (budget, kind and
/// lane). A page an earlier write charged elsewhere (another lane, another
/// pool) gives its charge back there and is this write's to hold from now:
/// it is held to its rows first, so this write pays for the rows it keeps,
/// never for room the rows its merge moved away left behind.
fn charge_to<K, V>(
    page: &mut Page<K, V>,
    heap: usize,
    funding: &mut Allocation,
) -> Result<(), MemoryError> {
    let mut held = std::mem::replace(&mut page._allocation, funding.split_off(0)?);
    if funding.absorb(&mut held).is_err() {
        drop(held);
        page.entries.shrink_to_fit();
    }
    let wanted = page_charge::<K, V>(page.entries.capacity(), heap)?;
    page._allocation = funding.split_off(wanted)?;
    Ok(())
}

/// What a transient vector of `len` entries takes.
fn staging_bytes<K, V>(len: usize) -> Result<usize, MemoryError> {
    checked_add(
        ALLOCATOR_OVERHEAD,
        checked_mul(len, size_of::<Entry<K, V>>())?,
    )
}

/// The heap `entries` hold, summed.
fn heap_of<K, V>(entries: &[Entry<K, V>]) -> Result<usize, MemoryError> {
    entries
        .iter()
        .try_fold(0usize, |sum, entry| checked_add(sum, entry.heap_bytes))
}
