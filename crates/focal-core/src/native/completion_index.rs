//! Uniquely owned completion-grant AVL index. Integer links identify stable
//! physical slots: rotations and successor transplantation never exchange values.
//! Parent links support a constant-storage cursor and bounded upward repairs.
//! Only geometric slot-buffer growth allocates; its journal retains the original
//! empty buffer and permit for exact tail rollback, including older removals.

use super::prepare::{array, within};
use super::{ContractError, EvaluationKey, NativeError};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError, OwnerId};

#[cfg(test)]
#[path = "completion_index_tests.rs"]
mod tests;

// An AVL tree with at most usize::MAX elements has height below twice the
// machine word width. A bad private link cannot create an unbounded traversal.
const WALK_LIMIT: u32 = usize::BITS.saturating_mul(2).saturating_add(2);

#[derive(Debug)]
struct Node<T, K> {
    key: K,
    value: T,
    weight: usize,
    maximum: usize,
    height: u16,
    parent: Option<usize>,
    left: Option<usize>,
    right: Option<usize>,
}

#[derive(Debug)]
struct Slot<T, K> {
    node: Option<Node<T, K>>,
    // Doubly linked vacancies allow growth rollback to remove only its own
    // trailing vacant slots, without scanning older live grants or vacancies.
    free_previous: Option<usize>,
    free_next: Option<usize>,
}

#[derive(Debug)]
pub(super) struct IndexGrowth<T, K = EvaluationKey> {
    slots: Vec<Slot<T, K>>,
    allocation: Option<Allocation>,
    owner: OwnerId,
    old_len: usize,
    replacement_capacity: usize,
}

#[derive(Debug)]
pub(super) struct CompletionIndex<T, K = EvaluationKey> {
    // Drop values/buffers before returning the matching accounting allowance.
    slots: Vec<Slot<T, K>>,
    allocation: Option<Allocation>,
    root: Option<usize>,
    free: Option<usize>,
    len: usize,
    owner: Option<OwnerId>,
    poisoned: std::cell::Cell<bool>,
    #[cfg(test)]
    touches: std::cell::Cell<usize>,
}

impl<T, K: Copy + Ord> CompletionIndex<T, K> {
    pub(super) fn new() -> Self {
        Self {
            slots: Vec::new(),
            allocation: None,
            root: None,
            free: None,
            len: 0,
            owner: None,
            poisoned: std::cell::Cell::new(false),
            #[cfg(test)]
            touches: std::cell::Cell::new(0),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }
    #[cfg(test)]
    pub(super) fn capacity(&self) -> usize {
        self.slots.capacity()
    }

    pub(super) fn check_health(&self) -> Result<(), NativeError> {
        if self.root.is_none() != (self.len == 0)
            || self
                .root
                .is_some_and(|root| self.node(root).is_none_or(|node| node.parent.is_some()))
        {
            self.poison();
        }
        if self.poisoned.get() {
            Err(ContractError::InvalidCut.into())
        } else {
            Ok(())
        }
    }
    fn poison(&self) {
        self.poisoned.set(true);
    }

    fn touch(&self) {
        #[cfg(test)]
        self.touches.set(self.touches.get().saturating_add(1));
    }
    fn node(&self, index: usize) -> Option<&Node<T, K>> {
        self.touch();
        let node = self.slots.get(index).and_then(|slot| slot.node.as_ref());
        if node.is_none() {
            self.poison();
        }
        node
    }
    fn node_mut(&mut self, index: usize) -> Option<&mut Node<T, K>> {
        self.touch();
        let node = self
            .slots
            .get_mut(index)
            .and_then(|slot| slot.node.as_mut());
        if node.is_none() {
            self.poisoned.set(true);
        }
        node
    }
    fn height(&self, index: Option<usize>) -> u16 {
        index
            .and_then(|index| self.node(index))
            .map_or(0, |node| node.height)
    }
    fn subtree_maximum(&self, index: Option<usize>) -> usize {
        index
            .and_then(|index| self.node(index))
            .map_or(0, |node| node.maximum)
    }
    pub(super) fn maximum(&self) -> usize {
        if self.check_health().is_err() {
            return 0;
        }
        self.subtree_maximum(self.root)
    }

    /// Returns a matching node, or the final parent of an absent key.
    fn locate(&self, key: K) -> Result<(Option<usize>, Option<usize>), NativeError> {
        self.check_health()?;
        let mut current = self.root;
        let mut parent = None;
        for _ in 0..WALK_LIMIT {
            let Some(index) = current else {
                return Ok((None, parent));
            };
            let node = self.node(index).ok_or(ContractError::InvalidCut)?;
            match key.cmp(&node.key) {
                std::cmp::Ordering::Equal => return Ok((Some(index), parent)),
                std::cmp::Ordering::Less => current = node.left,
                std::cmp::Ordering::Greater => current = node.right,
            }
            parent = Some(index);
        }
        self.poison();
        Err(ContractError::InvalidCut.into())
    }

    pub(super) fn get(&self, key: K) -> Option<&T> {
        let (index, _) = self.locate(key).ok()?;
        self.node(index?).map(|node| &node.value)
    }

    /// Charge both buffers during growth. The old emptied buffer belongs to the
    /// candidate journal until its commit or tail rollback; values move once.
    pub(super) fn grow(
        &mut self,
        source: &MemoryBudget,
        limit: usize,
    ) -> Result<Option<IndexGrowth<T, K>>, NativeError> {
        self.check_health()?;
        if self.free.is_some() || self.slots.len() < self.slots.capacity() {
            return Ok(None);
        }
        let minimum = self
            .slots
            .len()
            .checked_add(1)
            .ok_or(NativeError::Capacity("completion slots"))?;
        within(minimum, limit)?;
        let capacity = self
            .slots
            .capacity()
            .checked_mul(2)
            .unwrap_or(limit)
            .max(1)
            .min(limit);
        let owner = match self.owner {
            Some(owner) => owner,
            None => OwnerId::new()?,
        };
        let bytes = array::<Slot<T, K>>(capacity)?;
        let reservation = source.reserve(BudgetKind::Index, BudgetLane::Ordinary, bytes)?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(capacity)
            .map_err(|_| MemoryError::AllocationFailed)?;
        within(array::<Slot<T, K>>(slots.capacity())?, bytes)?;
        let old_len = self.slots.len();
        slots.append(&mut self.slots);
        let old = std::mem::replace(&mut self.slots, slots);
        let allocation = self.allocation.replace(reservation.commit());
        self.owner = Some(owner);
        Ok(Some(IndexGrowth {
            slots: old,
            allocation,
            owner,
            old_len,
            replacement_capacity: self.slots.capacity(),
        }))
    }

    /// No allocation. Duplicate keys and insufficient prepared capacity refuse
    /// before modifying the tree or invoking any caller mutation.
    pub(super) fn insert(&mut self, key: K, value: T, workspace: usize) -> Result<(), NativeError> {
        let (found, parent) = self.locate(key)?;
        if found.is_some() {
            return Err(ContractError::ContentConflict.into());
        }
        let len = self
            .len
            .checked_add(1)
            .ok_or(NativeError::Capacity("completion slots"))?;
        let index = if let Some(free) = self.free {
            if self.slots.get(free).is_none_or(|slot| slot.node.is_some()) {
                self.poison();
                return Err(ContractError::InvalidCut.into());
            }
            free
        } else {
            if self.slots.len() == self.slots.capacity() {
                return Err(NativeError::Capacity("completion index requires growth"));
            }
            self.slots.len()
        };
        let node = Node {
            key,
            value,
            weight: workspace,
            maximum: workspace,
            height: 1,
            parent,
            left: None,
            right: None,
        };
        if index == self.slots.len() {
            self.slots.push(Slot {
                node: Some(node),
                free_previous: None,
                free_next: None,
            });
        } else {
            self.unlink_free(index);
            self.check_health()?;
            if let Some(slot) = self.slots.get_mut(index) {
                slot.node = Some(node);
            }
        }
        if let Some(parent) = parent {
            if let Some(node) = self.node_mut(parent) {
                if key < node.key {
                    node.left = Some(index);
                } else {
                    node.right = Some(index);
                }
            }
        } else {
            self.root = Some(index);
        }
        self.len = len;
        self.rebalance(parent);
        self.check_health()
    }

    /// Keys live in the private node; the caller can mutate only the owned value.
    /// The precomputed replacement weight repairs cached maxima up the same path.
    pub(super) fn replace_weight(
        &mut self,
        key: K,
        workspace: usize,
        mutate: impl FnOnce(&mut T),
    ) -> Result<(), NativeError> {
        let index = self.locate(key)?.0.ok_or(ContractError::StaleEvaluation)?;
        let node = self.node_mut(index).ok_or(ContractError::InvalidCut)?;
        mutate(&mut node.value);
        node.weight = workspace;
        let mut current = Some(index);
        for _ in 0..WALK_LIMIT {
            let Some(index) = current else {
                break;
            };
            self.refresh(index);
            current = self.node(index).and_then(|node| node.parent);
        }
        if current.is_some() {
            self.poison();
        }
        self.check_health()
    }

    fn refresh(&mut self, index: usize) {
        let Some(node) = self.node(index) else {
            return;
        };
        let (left, right, weight) = (node.left, node.right, node.weight);
        let height = self.height(left).max(self.height(right)).saturating_add(1);
        let maximum = weight
            .max(self.subtree_maximum(left))
            .max(self.subtree_maximum(right));
        if let Some(node) = self.node_mut(index) {
            node.height = height;
            node.maximum = maximum;
        }
    }

    fn parent_link(&mut self, parent: Option<usize>, old: usize, new: Option<usize>) {
        if let Some(parent) = parent {
            if let Some(node) = self.node_mut(parent) {
                if node.left == Some(old) {
                    node.left = new;
                } else if node.right == Some(old) {
                    node.right = new;
                } else {
                    self.poisoned.set(true);
                }
            }
        } else {
            if self.root != Some(old) {
                self.poison();
            }
            self.root = new;
        }
        if let Some(new) = new
            && let Some(node) = self.node_mut(new)
        {
            node.parent = parent;
        }
    }

    fn rotate_left(&mut self, root: usize) -> usize {
        let Some(node) = self.node(root) else {
            return root;
        };
        let (parent, Some(right)) = (node.parent, node.right) else {
            self.poison();
            return root;
        };
        let Some(child) = self.node(right) else {
            return root;
        };
        let middle = child.left;
        if let Some(node) = self.node_mut(root) {
            node.right = middle;
            node.parent = Some(right);
        }
        if let Some(middle) = middle
            && let Some(node) = self.node_mut(middle)
        {
            node.parent = Some(root);
        }
        self.parent_link(parent, root, Some(right));
        if let Some(node) = self.node_mut(right) {
            node.left = Some(root);
        }
        self.refresh(root);
        self.refresh(right);
        right
    }

    fn rotate_right(&mut self, root: usize) -> usize {
        let Some(node) = self.node(root) else {
            return root;
        };
        let (parent, Some(left)) = (node.parent, node.left) else {
            self.poison();
            return root;
        };
        let Some(child) = self.node(left) else {
            return root;
        };
        let middle = child.right;
        if let Some(node) = self.node_mut(root) {
            node.left = middle;
            node.parent = Some(left);
        }
        if let Some(middle) = middle
            && let Some(node) = self.node_mut(middle)
        {
            node.parent = Some(root);
        }
        self.parent_link(parent, root, Some(left));
        if let Some(node) = self.node_mut(left) {
            node.right = Some(root);
        }
        self.refresh(root);
        self.refresh(left);
        left
    }

    fn rebalance(&mut self, mut current: Option<usize>) {
        for _ in 0..WALK_LIMIT {
            let Some(index) = current else {
                break;
            };
            self.refresh(index);
            let Some(node) = self.node(index) else {
                break;
            };
            let (left, right) = (node.left, node.right);
            let left_height = self.height(left);
            let right_height = self.height(right);
            let top = if left_height > right_height.saturating_add(1) {
                if let Some(left) = left
                    && let Some(node) = self.node(left)
                    && self.height(node.right) > self.height(node.left)
                {
                    self.rotate_left(left);
                }
                self.rotate_right(index)
            } else if right_height > left_height.saturating_add(1) {
                if let Some(right) = right
                    && let Some(node) = self.node(right)
                    && self.height(node.left) > self.height(node.right)
                {
                    self.rotate_right(right);
                }
                self.rotate_left(index)
            } else {
                index
            };
            current = self.node(top).and_then(|node| node.parent);
        }
        if current.is_some() {
            self.poison();
        }
    }

    fn minimum_index(&self, mut current: usize) -> Option<usize> {
        for _ in 0..WALK_LIMIT {
            let node = self.node(current)?;
            if let Some(left) = node.left {
                current = left;
            } else {
                return Some(current);
            }
        }
        self.poison();
        None
    }

    /// Removal transplants a successor's links into the removed node's position;
    /// the successor and every other surviving value keep their physical slot.
    pub(super) fn remove(&mut self, key: K) -> Result<T, NativeError> {
        let index = self.locate(key)?.0.ok_or(ContractError::StaleEvaluation)?;
        let node = self.node(index).ok_or(ContractError::InvalidCut)?;
        let (parent, left, right) = (node.parent, node.left, node.right);
        let len = self.len.checked_sub(1).ok_or(ContractError::InvalidCut)?;
        let successor = if let (Some(_), Some(right)) = (left, right) {
            let successor = self.minimum_index(right).ok_or(ContractError::InvalidCut)?;
            let node = self.node(successor).ok_or(ContractError::InvalidCut)?;
            Some((successor, node.parent, node.right))
        } else {
            None
        };
        // All references and arithmetic are checked before consuming the value.
        let removed = self
            .slots
            .get_mut(index)
            .and_then(|slot| slot.node.take())
            .ok_or(ContractError::InvalidCut)?;
        let repair = if let Some((successor, successor_parent, successor_right)) = successor {
            if successor_parent != Some(index) {
                self.parent_link(successor_parent, successor, successor_right);
                if let Some(node) = self.node_mut(successor) {
                    node.right = right;
                }
                if let Some(right) = right
                    && let Some(node) = self.node_mut(right)
                {
                    node.parent = Some(successor);
                }
            }
            self.parent_link(parent, index, Some(successor));
            if let Some(node) = self.node_mut(successor) {
                node.left = left;
            }
            if let Some(left) = left
                && let Some(node) = self.node_mut(left)
            {
                node.parent = Some(successor);
            }
            if successor_parent == Some(index) {
                Some(successor)
            } else {
                successor_parent
            }
        } else {
            self.parent_link(parent, index, left.or(right));
            parent
        };
        self.link_free(index);
        self.len = len;
        self.rebalance(repair);
        self.check_health()?;
        Ok(removed.value)
    }

    fn link_free(&mut self, index: usize) {
        let next = self.free;
        if next == Some(index)
            || self.slots.get(index).is_none_or(|slot| slot.node.is_some())
            || next.is_some_and(|next| {
                self.slots
                    .get(next)
                    .is_none_or(|slot| slot.node.is_some() || slot.free_previous.is_some())
            })
        {
            self.poison();
            return;
        }
        if let Some(slot) = self.slots.get_mut(index) {
            slot.free_previous = None;
            slot.free_next = next;
        }
        if let Some(next) = next
            && let Some(slot) = self.slots.get_mut(next)
        {
            slot.free_previous = Some(index);
        }
        self.free = Some(index);
    }

    fn unlink_free(&mut self, index: usize) {
        let Some(slot) = self.slots.get(index) else {
            self.poison();
            return;
        };
        let (previous, next) = (slot.free_previous, slot.free_next);
        if slot.node.is_some()
            || previous == Some(index)
            || next == Some(index)
            || (previous.is_some() && previous == next)
            || previous.map_or(self.free != Some(index), |previous| {
                self.slots
                    .get(previous)
                    .is_none_or(|slot| slot.node.is_some() || slot.free_next != Some(index))
            })
            || next.is_some_and(|next| {
                self.slots
                    .get(next)
                    .is_none_or(|slot| slot.node.is_some() || slot.free_previous != Some(index))
            })
        {
            self.poison();
            return;
        }
        if let Some(previous) = previous {
            if let Some(slot) = self.slots.get_mut(previous) {
                slot.free_next = next;
            }
        } else {
            self.free = next;
        }
        if let Some(next) = next
            && let Some(slot) = self.slots.get_mut(next)
        {
            slot.free_previous = previous;
        }
        if let Some(slot) = self.slots.get_mut(index) {
            slot.free_previous = None;
            slot.free_next = None;
        }
    }

    fn check_growth(
        &self,
        growth: &IndexGrowth<T, K>,
        removed: Option<K>,
    ) -> Result<(), NativeError> {
        self.check_health()?;
        if self.owner != Some(growth.owner)
            || !growth.slots.is_empty()
            || self.slots.capacity() != growth.replacement_capacity
            || growth.old_len > self.slots.len()
            || growth.old_len > growth.slots.capacity()
        {
            return Err(ContractError::InvalidCut.into());
        }
        for slot in self.slots.iter().skip(growth.old_len) {
            if let Some(node) = &slot.node
                && Some(node.key) != removed
            {
                return Err(ContractError::InvalidCut.into());
            }
        }
        Ok(())
    }

    pub(super) fn check_remove_restore_growth(
        &self,
        growth: &IndexGrowth<T, K>,
        removed_key: K,
    ) -> Result<(), NativeError> {
        self.locate(removed_key)?
            .0
            .ok_or(ContractError::StaleEvaluation)?;
        self.check_growth(growth, Some(removed_key))
    }

    pub(super) fn check_restore_growth(
        &self,
        growth: &IndexGrowth<T, K>,
    ) -> Result<(), NativeError> {
        self.check_growth(growth, None)
    }

    pub(super) fn restore_growth(
        &mut self,
        mut growth: IndexGrowth<T, K>,
    ) -> Result<(), NativeError> {
        self.check_restore_growth(&growth)?;
        // Only the new suffix is inspected/unlinked. Committed removals within
        // the original capacity retain their actual free-list and tree links.
        for index in growth.old_len..self.slots.len() {
            self.unlink_free(index);
            self.check_health()?;
        }
        self.slots.truncate(growth.old_len);
        growth.slots.append(&mut self.slots);
        self.slots = growth.slots;
        self.allocation = growth.allocation;
        self.check_health()
    }

    fn lower_bound(&self, key: K) -> Option<usize> {
        self.check_health().ok()?;
        let mut current = self.root;
        let mut candidate = None;
        for _ in 0..WALK_LIMIT {
            let Some(index) = current else {
                break;
            };
            let node = self.node(index)?;
            if node.key < key {
                current = node.right;
            } else {
                candidate = Some(index);
                current = node.left;
            }
        }
        if current.is_some() {
            self.poison();
            return None;
        }
        candidate
    }

    fn successor(&self, index: usize) -> Option<usize> {
        let node = self.node(index)?;
        if let Some(right) = node.right {
            return self.minimum_index(right);
        }
        let mut child = index;
        let mut parent = node.parent;
        for _ in 0..WALK_LIMIT {
            let index = parent?;
            let node = self.node(index)?;
            if node.left == Some(child) {
                return Some(index);
            }
            if node.right != Some(child) {
                self.poison();
                return None;
            }
            child = index;
            parent = node.parent;
        }
        self.poison();
        None
    }

    pub(super) fn iter_from(&self, lower: K) -> Cursor<'_, T, K> {
        Cursor {
            index: self,
            next: self.lower_bound(lower),
            remaining: self.len,
            previous: None,
        }
    }
}

pub(super) struct Cursor<'a, T, K = EvaluationKey> {
    index: &'a CompletionIndex<T, K>,
    next: Option<usize>,
    remaining: usize,
    previous: Option<K>,
}
impl<'a, T, K: Copy + Ord> Iterator for Cursor<'a, T, K> {
    type Item = (K, &'a T);
    fn next(&mut self) -> Option<Self::Item> {
        self.index.check_health().ok()?;
        let current = self.next?;
        let node = self.index.node(current)?;
        if self.remaining == 0 || self.previous.is_some_and(|previous| previous >= node.key) {
            self.index.poison();
            self.next = None;
            return None;
        }
        self.remaining = self.remaining.saturating_sub(1);
        self.previous = Some(node.key);
        self.next = self.index.successor(current);
        self.index.check_health().ok()?;
        Some((node.key, &node.value))
    }
}

#[cfg(test)]
impl<T, K: Copy + Ord> CompletionIndex<T, K> {
    pub(super) fn slot_charge(capacity: usize) -> Result<usize, NativeError> {
        array::<Slot<T, K>>(capacity)
    }
    pub(super) fn slot(&self, key: K) -> Option<usize> {
        self.locate(key).ok()?.0
    }
    pub(super) fn reset_visits(&self) {
        self.touches.set(0);
    }
    pub(super) fn visits(&self) -> usize {
        self.touches.get()
    }

    pub(super) fn validate(&self) -> Result<(), NativeError> {
        self.check_health()?;
        use std::collections::BTreeSet;
        let bad = || NativeError::Contract(ContractError::InvalidCut);
        let mut seen = BTreeSet::new();
        let mut work = Vec::new();
        if let Some(root) = self.root {
            work.push((root, None, None, None));
        }
        while let Some((index, parent, minimum, maximum)) = work.pop() {
            if !seen.insert(index) {
                return Err(bad());
            }
            let node = self.node(index).ok_or_else(bad)?;
            if node.parent != parent
                || minimum.is_some_and(|key| node.key <= key)
                || maximum.is_some_and(|key| node.key >= key)
                || self.height(node.left).abs_diff(self.height(node.right)) > 1
                || node.height
                    != self
                        .height(node.left)
                        .max(self.height(node.right))
                        .saturating_add(1)
                || node.maximum
                    != node
                        .weight
                        .max(self.subtree_maximum(node.left))
                        .max(self.subtree_maximum(node.right))
            {
                return Err(bad());
            }
            if let Some(left) = node.left {
                work.push((left, Some(index), minimum, Some(node.key)));
            }
            if let Some(right) = node.right {
                work.push((right, Some(index), Some(node.key), maximum));
            }
        }
        if seen.len() != self.len {
            return Err(bad());
        }
        let mut free = self.free;
        let mut previous = None;
        while let Some(index) = free {
            if !seen.insert(index) {
                return Err(bad());
            }
            let slot = self.slots.get(index).ok_or_else(bad)?;
            if slot.node.is_some() || slot.free_previous != previous {
                return Err(bad());
            }
            previous = Some(index);
            free = slot.free_next;
        }
        if seen.len() != self.slots.len() {
            return Err(bad());
        }
        Ok(())
    }
}
