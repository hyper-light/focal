use super::*;
use crate::lifecycle::memory as bytes;

impl Registry {
    pub fn copy_heap_allocations(&self) -> Result<usize, ContractError> {
        let mut count = bytes::add(
            bytes::allocation::<Scope>(self.scopes.len()),
            bytes::allocation::<OwnedChild>(self.children.len()),
        )?;
        for scope in &self.scopes {
            count = bytes::add(count, bytes::allocation::<WaitPredicate>(scope.roots.len()))?;
        }
        Ok(count)
    }
    pub fn heap_allocations(&self) -> Result<usize, ContractError> {
        let mut count = bytes::add(
            bytes::allocation::<Scope>(self.scopes.capacity()),
            bytes::allocation::<OwnedChild>(self.children.capacity()),
        )?;
        for scope in &self.scopes {
            count = bytes::add(
                count,
                bytes::allocation::<WaitPredicate>(scope.roots.capacity()),
            )?;
        }
        Ok(count)
    }
    pub fn copy_heap_bytes(&self) -> Result<usize, ContractError> {
        let mut size = bytes::add(
            bytes::array::<Scope>(self.scopes.len())?,
            bytes::array::<OwnedChild>(self.children.len())?,
        )?;
        for scope in &self.scopes {
            size = bytes::add(size, bytes::array::<WaitPredicate>(scope.roots.len())?)?;
        }
        Ok(size)
    }
    pub fn retained_heap_bytes(&self) -> Result<usize, ContractError> {
        let mut size = bytes::add(
            bytes::array::<Scope>(self.scopes.capacity())?,
            bytes::array::<OwnedChild>(self.children.capacity())?,
        )?;
        for scope in &self.scopes {
            size = bytes::add(size, bytes::array::<WaitPredicate>(scope.roots.capacity())?)?;
        }
        Ok(size)
    }
    pub fn copy_charge(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.copy_heap_bytes()?)
    }
    pub fn retained_bytes(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.retained_heap_bytes()?)
    }
    pub fn try_copy(&self, max_bytes: usize) -> Result<Self, ContractError> {
        bytes::fits(self.copy_charge()?, max_bytes)?;
        let mut scopes = bytes::reserve(self.scopes.len())?;
        for scope in &self.scopes {
            scopes.push(Scope {
                id: scope.id,
                roots: bytes::copy(&scope.roots)?,
                deadline: scope.deadline,
                registered: scope.registered,
                disposition: scope.disposition,
                last_rebinding: scope.last_rebinding,
            });
        }
        let copied = Self {
            owner: self.owner,
            limits: self.limits,
            scopes,
            children: bytes::copy(&self.children)?,
            released: self.released,
            last_cut: self.last_cut,
        };
        bytes::fits(copied.retained_bytes()?, max_bytes)?;
        Ok(copied)
    }
}

#[cfg(test)]
impl Registry {
    pub(in crate::lifecycle) fn memory_fixture(owner: Binding) -> Self {
        use crate::{ContentHash, ObjectId, ObjectRevision, TimerId};
        let cut = ClaimCut {
            position: SessionSeq(8),
            cause: ContentHash([8; 32]),
        };
        let mut roots = Vec::with_capacity(16);
        roots.extend([
            WaitPredicate::Satisfied(ClaimId::from_u128(90)),
            WaitPredicate::Terminal(ClaimId::from_u128(91)),
            WaitPredicate::Released(ClaimId::from_u128(92)),
        ]);
        let mut scopes = Vec::with_capacity(8);
        scopes.push(Scope {
            id: MonitorId::from_u128(80),
            roots,
            deadline: Deadline {
                timer: TimerId::from_u128(81),
                generation: 3,
                at: 999,
            },
            registered: SessionSeq(4),
            disposition: Some(MonitorDisposition::Released(cut)),
            last_rebinding: Some(Rebinding {
                predecessor: ClaimId::from_u128(91),
                successor: ClaimId::from_u128(92),
                cut,
            }),
        });
        let mut children = Vec::with_capacity(8);
        children.push(OwnedChild {
            binding: Binding {
                object: ObjectId::from_u128(93),
                revision: ObjectRevision(1),
                ..owner
            },
            registered: SessionSeq(3),
        });
        Self {
            owner,
            limits: ScopeLimits {
                scopes: 8,
                roots: 16,
                children: 8,
            },
            scopes,
            children,
            released: Some(cut),
            last_cut: cut.position,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scope_copy_preserves_release_rebinding_and_owned_children_without_shared_buffers() {
        let binding = crate::lifecycle::claim::tests::definition(4).binding;
        let mut original = Registry::memory_fixture(binding);
        let before = original.clone();
        let charge = original.copy_charge().unwrap();
        let copied = original.try_copy(charge).unwrap();
        assert_eq!(copied, before);
        assert_eq!(original.heap_allocations().unwrap(), 3);
        assert_eq!(copied.heap_allocations().unwrap(), 3);
        assert!(copied.retained_heap_bytes().unwrap() < original.retained_heap_bytes().unwrap());
        original.scopes[0].roots.clear();
        original.children.clear();
        drop(original);
        assert_eq!(copied.iter().next().unwrap().roots().len(), 3);
        assert_eq!(copied.children().len(), 1);
        assert!(copied.released());
        assert_eq!(
            copied
                .iter()
                .next()
                .unwrap()
                .last_rebinding()
                .unwrap()
                .successor,
            ClaimId::from_u128(92)
        );
        for after in 0..3 {
            assert_eq!(
                bytes::fail_after(after, || copied.try_copy(charge)),
                Err(ContractError::Capacity)
            );
            assert_eq!(copied, before);
        }
    }
}
