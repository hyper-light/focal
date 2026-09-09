use super::*;

fn relation(value: &Relation) -> Result<Relation, ContractError> {
    Ok(copy_relation(value))
}
fn scope(value: &AuthoredScope) -> Result<ScopeSpec<'_>, ContractError> {
    Ok(ScopeSpec {
        kind: value.kind,
        key: &value.key,
    })
}
fn slot(value: &AuthoredSlot) -> Result<SlotPolicy<'_>, ContractError> {
    Ok(SlotPolicy {
        slot: value.slot,
        missing_declaration_index: value.missing_declaration_index,
        mode: value.mode,
        checks: &value.checks,
    })
}

impl ClaimSlotSource for SlotPolicy<'_> {
    type Checks<'s>
        = std::iter::Map<
        std::iter::Copied<std::slice::Iter<'s, CheckPolicy>>,
        fn(CheckPolicy) -> Result<CheckPolicy, ContractError>,
    >
    where
        Self: 's;
    fn fields(&self) -> ClaimSlotFields {
        ClaimSlotFields {
            slot: self.slot,
            missing_declaration_index: self.missing_declaration_index,
            mode: self.mode,
            checks: self.checks.len(),
        }
    }
    fn checks(&self) -> Self::Checks<'_> {
        self.checks.iter().copied().map(Ok)
    }
}

impl<'a> ClaimSource<'a> for ClaimSpec<'a> {
    type Relations<'s>
        = std::iter::Map<
        std::slice::Iter<'a, Relation>,
        fn(&Relation) -> Result<Relation, ContractError>,
    >
    where
        Self: 's;
    type Scopes<'s>
        = std::iter::Map<
        std::iter::Copied<std::slice::Iter<'a, ScopeSpec<'a>>>,
        fn(ScopeSpec<'a>) -> Result<ScopeSpec<'a>, ContractError>,
    >
    where
        Self: 's;
    type Requirements<'s>
        = std::iter::Map<
        std::iter::Copied<std::slice::Iter<'a, RequirementRef>>,
        fn(RequirementRef) -> Result<RequirementRef, ContractError>,
    >
    where
        Self: 's;
    type Slot<'s>
        = SlotPolicy<'a>
    where
        Self: 's;
    type Slots<'s>
        = std::iter::Map<
        std::iter::Copied<std::slice::Iter<'a, SlotPolicy<'a>>>,
        fn(SlotPolicy<'a>) -> Result<SlotPolicy<'a>, ContractError>,
    >
    where
        Self: 's;
    fn fields(&self) -> ClaimFields<'a> {
        ClaimFields {
            ledger: self.ledger,
            id: self.id,
            schema: self.schema,
            occurrence: self.occurrence,
            description: self.description,
            deadline: self.deadline,
            policy: self.policy,
        }
    }
    fn relation_count(&self) -> usize {
        self.relations.len()
    }
    fn scope_count(&self) -> usize {
        self.scopes.len()
    }
    fn requirement_count(&self) -> usize {
        self.requirements.len()
    }
    fn slot_count(&self) -> usize {
        self.slots.len()
    }
    fn relations(&self) -> Self::Relations<'_> {
        self.relations.iter().map(relation)
    }
    fn scopes(&self) -> Self::Scopes<'_> {
        self.scopes.iter().copied().map(Ok)
    }
    fn requirements(&self) -> Self::Requirements<'_> {
        self.requirements.iter().copied().map(Ok)
    }
    fn slots(&self) -> Self::Slots<'_> {
        self.slots.iter().copied().map(Ok)
    }
}

pub(super) struct OwnedSource<'a>(pub(super) &'a ClaimDescriptor);
impl<'a> ClaimSource<'a> for OwnedSource<'a> {
    type Relations<'s>
        = std::iter::Map<
        std::slice::Iter<'a, Relation>,
        fn(&Relation) -> Result<Relation, ContractError>,
    >
    where
        Self: 's;
    type Scopes<'s>
        = std::iter::Map<
        std::slice::Iter<'a, AuthoredScope>,
        fn(&'a AuthoredScope) -> Result<ScopeSpec<'a>, ContractError>,
    >
    where
        Self: 's;
    type Requirements<'s>
        = std::iter::Map<
        std::iter::Copied<std::slice::Iter<'a, RequirementRef>>,
        fn(RequirementRef) -> Result<RequirementRef, ContractError>,
    >
    where
        Self: 's;
    type Slot<'s>
        = SlotPolicy<'a>
    where
        Self: 's;
    type Slots<'s>
        = std::iter::Map<
        std::slice::Iter<'a, AuthoredSlot>,
        fn(&'a AuthoredSlot) -> Result<SlotPolicy<'a>, ContractError>,
    >
    where
        Self: 's;
    fn fields(&self) -> ClaimFields<'a> {
        ClaimFields {
            ledger: self.0.ledger,
            id: self.0.id,
            schema: self.0.schema,
            occurrence: self.0.occurrence,
            description: &self.0.description,
            deadline: self.0.deadline,
            policy: self.0.policy,
        }
    }
    fn relation_count(&self) -> usize {
        self.0.relations.len()
    }
    fn scope_count(&self) -> usize {
        self.0.scopes.len()
    }
    fn requirement_count(&self) -> usize {
        self.0.requirements.len()
    }
    fn slot_count(&self) -> usize {
        self.0.slots.len()
    }
    fn relations(&self) -> Self::Relations<'_> {
        self.0.relations.iter().map(relation)
    }
    fn scopes(&self) -> Self::Scopes<'_> {
        self.0.scopes.iter().map(scope)
    }
    fn requirements(&self) -> Self::Requirements<'_> {
        self.0.requirements.iter().copied().map(Ok)
    }
    fn slots(&self) -> Self::Slots<'_> {
        self.0.slots.iter().map(slot)
    }
}
