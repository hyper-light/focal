use super::*;
use crate::native::layout::PackedKey;
use focal_memory::{Entry, RangeHydrationLookup, RangeHydrationSource};

struct Objects<'a, 'root, 'bytes, C> {
    dependencies: &'a RangeHydrationLookup<'root, PackedKey, Row>,
    shared: &'a Shared<'a, 'bytes, C>,
}
impl<C> read_dispatch::Objects for Objects<'_, '_, '_, C> {
    fn ledger(&self) -> LedgerId {
        self.shared.header.ledger
    }
    fn prefix(&self) -> SessionSeq {
        self.shared.header.prefix
    }
    fn get(&self, key: Key, meter: &Meter) -> Result<Option<&Row>, NativeError> {
        meter
            .charge(read_index::lookup_work()?)
            .map_err(read_evidence::codec)?;
        Ok(self.dependencies.get(&PackedKey::probe(&key)))
    }
    fn claim_dependency(
        &self,
        id: ClaimId,
        meter: &Meter,
    ) -> Result<read_dispatch::ClaimDependency<'_>, NativeError> {
        self.shared
            .index
            .claim(id, meter)
            .map(read_dispatch::ClaimDependency::Raw)
    }
    fn artifact_origin(
        &self,
        id: ArtifactId,
        meter: &Meter,
    ) -> Result<read_dispatch::ArtifactOrigin, NativeError> {
        self.shared.index.artifact(id, meter)
    }
}
fn context<'a, O, C>(
    objects: &'a O,
    shared: &'a Shared<'_, '_, C>,
) -> read_dispatch::Context<'a, O, C> {
    read_dispatch::Context {
        objects,
        custody: shared.custody,
        workspace: shared.budget,
        workspace_lane: BudgetLane::Committed,
        parsing: &shared.meters.parsing,
        source: &shared.meters.source,
        model: &shared.meters.model,
        lookup: &shared.meters.lookup,
        limits: shared.limits,
    }
}
pub(super) struct Source<'a, 'bytes, C> {
    row: EncodedRow<'bytes>,
    /// The row's key as the store holds it (doc 31 M3), packed once as the row is read.
    key: PackedKey,
    shared: &'a Shared<'a, 'bytes, C>,
}
pub(super) struct Plan<'p, 'a, 'bytes, C> {
    source: &'p Source<'a, 'bytes, C>,
    dependencies: RangeHydrationLookup<'p, PackedKey, Row>,
    quote: read_dispatch::Quote,
}
impl<'s, 'bytes, C: read_evidence::Custody> RangeHydrationSource<PackedKey, Row>
    for Source<'s, 'bytes, C>
{
    type Plan<'a>
        = Plan<'a, 's, 'bytes, C>
    where
        Self: 'a;
    fn key(&self) -> &PackedKey {
        &self.key
    }
    fn prepare<'a>(
        &'a self,
        dependencies: RangeHydrationLookup<'a, PackedKey, Row>,
    ) -> Result<Entry<PackedKey, Self::Plan<'a>>, MemoryError>
    where
        PackedKey: 'a,
        Row: 'a,
    {
        let objects = Objects {
            dependencies: &dependencies,
            shared: self.shared,
        };
        let quote = read_dispatch::prepare(&self.row, &context(&objects, self.shared))
            .map_err(|error| self.shared.refuse(error))?;
        Ok(Entry::new(
            self.key.clone(),
            Plan {
                source: self,
                dependencies,
                quote,
            },
            // A wide row's box is charged to its entry (`Row`): the key's
            // family names the box it may take; and so is a wide key's heap.
            quote
                .heap_bytes
                .checked_add(self.row.key.boxed_heap())
                .and_then(|bytes| bytes.checked_add(self.key.heap_bytes()))
                .ok_or(MemoryError::AllocationFailed)?,
        ))
    }
    fn build<'a>(plan: Self::Plan<'a>, allowance: usize) -> Result<(Row, usize), MemoryError>
    where
        Self: 'a,
    {
        let shared = plan.source.shared;
        let objects = Objects {
            dependencies: &plan.dependencies,
            shared,
        };
        read_dispatch::with_build(
            &plan.source.row,
            &context(&objects, shared),
            plan.quote,
            allowance,
            |row, actual| {
                let boxed = row.boxed_heap();
                Ok((row, crate::native::prepare::add(actual, boxed)?))
            },
        )
        .map_err(|error| shared.refuse(error))
    }
}

pub(super) struct Sources<'a, 'bytes, C> {
    rows: RecordRows<'bytes>,
    phase: usize,
    shared: &'a Shared<'a, 'bytes, C>,
    stopped: bool,
}
impl<'a, 'bytes, C> Sources<'a, 'bytes, C> {
    pub(super) fn new(
        rows: RecordRows<'bytes>,
        phase: usize,
        shared: &'a Shared<'a, 'bytes, C>,
    ) -> Self {
        Self {
            rows,
            phase,
            shared,
            stopped: false,
        }
    }
}
impl<'a, 'bytes, C> Iterator for Sources<'a, 'bytes, C> {
    type Item = Result<Source<'a, 'bytes, C>, MemoryError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped {
            return None;
        }
        loop {
            let Some(next) = self.rows.next() else {
                self.stopped = true;
                return None;
            };
            let selected = (|| {
                let row = next.map_err(read_evidence::codec)?;
                self.shared
                    .meters
                    .lookup
                    .charge(1)
                    .map_err(read_evidence::codec)?;
                if read_index::phase(row.key)? != self.phase {
                    return Ok(None);
                }
                let key = PackedKey::pack(&row.key)?;
                if crate::native::ranges::indexed(&key) {
                    // Its member's key set holds it (doc 31 M3): checked as
                    // every fixed row is, its key given to the set in order.
                    read_dispatch::index_row(
                        &row,
                        &self.shared.meters.parsing,
                        &self.shared.meters.model,
                        self.shared.header.ledger,
                    )?;
                    self.shared
                        .keys
                        .try_borrow_mut()
                        .map_err(|_| {
                            MemoryError::InvalidConfiguration("index keys borrowed twice")
                        })?
                        .as_mut()
                        .ok_or(MemoryError::InvalidConfiguration(
                            "index keys finished early",
                        ))?
                        .push(key.as_bytes())?;
                    return Ok(None);
                }
                Ok(Some(Source {
                    row,
                    key,
                    shared: self.shared,
                }))
            })();
            match selected {
                Ok(Some(row)) => return Some(Ok(row)),
                Ok(None) => {}
                Err(error) => {
                    self.stopped = true;
                    return Some(Err(self.shared.refuse(error)));
                }
            }
        }
    }
}
