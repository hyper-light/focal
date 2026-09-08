//! Complete borrowed monitor registry snapshots. The model restores dispositions
//! from recorded cuts; recovery never repeats discovery against today's graph.
use super::{
    bytes::{Cursor, Error},
    read_fields as fields,
    read_source::{Meter, Span, Values},
};
use focal_model::lifecycle::{ContractError, scope};
use focal_model::{ClaimId, MonitorId, WaitPredicate};

#[derive(Clone, Copy)]
pub(super) struct Registry<'a> {
    pub(super) fields: scope::RegistrySnapshotV1,
    scopes: Span<'a>,
    children: Span<'a>,
}
#[derive(Clone, Copy)]
pub(super) struct Scope<'a> {
    fields: scope::ScopeSnapshotV1,
    roots: Span<'a>,
}
pub(super) fn predicate(c: &mut Cursor<'_>) -> Result<WaitPredicate, Error> {
    let tag = c.u8()?;
    let id = ClaimId(c.fixed()?);
    match tag {
        0 => Ok(WaitPredicate::Satisfied(id)),
        1 => Ok(WaitPredicate::Terminal(id)),
        2 => Ok(WaitPredicate::Released(id)),
        _ => Err(Error::InvalidTag("wait predicate")),
    }
}
fn child(c: &mut Cursor<'_>) -> Result<scope::OwnedChildSnapshotV1, Error> {
    Ok(scope::OwnedChildSnapshotV1 {
        binding: fields::binding(c)?,
        registered: fields::sequence(c)?,
    })
}
fn scope<'a>(c: &mut Cursor<'a>) -> Result<Scope<'a>, Error> {
    let id = MonitorId(c.fixed()?);
    let deadline = fields::deadline(c)?;
    let registered = fields::sequence(c)?;
    let disposition = fields::optional(c, |c| match c.u8()? {
        0 => fields::claim_cut(c).map(scope::MonitorDisposition::Released),
        1 => fields::cancellation(c).map(scope::MonitorDisposition::Cancelled),
        _ => Err(Error::InvalidTag("monitor disposition")),
    })?;
    let last_rebinding = fields::optional(c, fields::rebinding)?;
    let roots = Span::read_fixed(c, 17)?;
    Ok(Scope {
        fields: scope::ScopeSnapshotV1 {
            id,
            roots: roots.count,
            deadline,
            registered,
            disposition,
            last_rebinding,
        },
        roots,
    })
}
impl<'a> Registry<'a> {
    pub(super) fn read(c: &mut Cursor<'a>) -> Result<Self, Error> {
        let owner = fields::binding(c)?;
        let limits = fields::scope_limits(c)?;
        let released = fields::optional(c, fields::claim_cut)?;
        let last_cut = fields::sequence(c)?;
        let scopes = Span::read_with(c, scope)?;
        let children = Span::read_fixed(c, 96)?;
        Ok(Self {
            fields: scope::RegistrySnapshotV1 {
                owner,
                limits,
                scopes: scopes.count,
                children: children.count,
                released,
                last_cut,
            },
            scopes,
            children,
        })
    }
    pub(super) fn source<'m>(&self, meter: &'m Meter) -> RegistrySource<'m, 'a> {
        RegistrySource { raw: *self, meter }
    }
}
pub(super) struct RegistrySource<'m, 'a> {
    raw: Registry<'a>,
    meter: &'m Meter,
}
pub(super) struct ScopeSource<'m, 'a> {
    raw: Scope<'a>,
    meter: &'m Meter,
}
pub(super) struct Scopes<'m, 'a> {
    values: Values<'m, 'a, Scope<'a>>,
    meter: &'m Meter,
}
impl<'m, 'a> Iterator for Scopes<'m, 'a> {
    type Item = Result<ScopeSource<'m, 'a>, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.values.next().map(|raw| {
            raw.map(|raw| ScopeSource {
                raw,
                meter: self.meter,
            })
        })
    }
}
impl scope::ScopeSnapshotSource for ScopeSource<'_, '_> {
    fn fields(&self) -> scope::ScopeSnapshotV1 {
        self.raw.fields
    }
    type Roots<'a>
        = Values<'a, 'a, WaitPredicate>
    where
        Self: 'a;
    fn roots(&self) -> Self::Roots<'_> {
        Values::new(self.raw.roots, self.meter, predicate)
    }
}
impl scope::RegistrySnapshotSource for RegistrySource<'_, '_> {
    fn fields(&self) -> scope::RegistrySnapshotV1 {
        self.raw.fields
    }
    type Scope<'a>
        = ScopeSource<'a, 'a>
    where
        Self: 'a;
    type Scopes<'a>
        = Scopes<'a, 'a>
    where
        Self: 'a;
    type Children<'a>
        = Values<'a, 'a, scope::OwnedChildSnapshotV1>
    where
        Self: 'a;
    fn scopes(&self) -> Self::Scopes<'_> {
        Scopes {
            values: Values::new(self.raw.scopes, self.meter, scope),
            meter: self.meter,
        }
    }
    fn children(&self) -> Self::Children<'_> {
        Values::new(self.raw.children, self.meter, child)
    }
}
