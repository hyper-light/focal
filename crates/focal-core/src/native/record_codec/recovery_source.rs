use super::*;
use focal_memory::{Entry, RangeHydrationLookup, RangeHydrationSource};

struct Objects<'a, 'root, 'bytes, C> {
    dependencies: &'a RangeHydrationLookup<'root, Key, Row>,
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
        Ok(self.dependencies.get(&key))
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
        workspace_lane: BudgetLane::Completion,
        parsing: &shared.meters.parsing,
        source: &shared.meters.source,
        model: &shared.meters.model,
        lookup: &shared.meters.lookup,
        limits: shared.limits,
    }
}
pub(super) struct Source<'a, 'bytes, C> {
    row: EncodedRow<'bytes>,
    shared: &'a Shared<'a, 'bytes, C>,
}
pub(super) struct Plan<'p, 'a, 'bytes, C> {
    source: &'p Source<'a, 'bytes, C>,
    dependencies: RangeHydrationLookup<'p, Key, Row>,
    quote: read_dispatch::Quote,
}
impl<'s, 'bytes, C: read_evidence::Custody> RangeHydrationSource<Key, Row>
    for Source<'s, 'bytes, C>
{
    type Plan<'a>
        = Plan<'a, 's, 'bytes, C>
    where
        Self: 'a;
    fn key(&self) -> &Key {
        &self.row.key
    }
    fn prepare<'a>(
        &'a self,
        dependencies: RangeHydrationLookup<'a, Key, Row>,
    ) -> Result<Entry<Key, Self::Plan<'a>>, MemoryError>
    where
        Key: 'a,
        Row: 'a,
    {
        let objects = Objects {
            dependencies: &dependencies,
            shared: self.shared,
        };
        let quote = read_dispatch::prepare(&self.row, &context(&objects, self.shared))
            .map_err(|error| self.shared.refuse(error))?;
        Ok(Entry::new(
            self.row.key,
            Plan {
                source: self,
                dependencies,
                quote,
            },
            quote.heap_bytes,
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
            |row, actual| Ok((row, actual)),
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
                Ok(Some(Source {
                    row,
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
