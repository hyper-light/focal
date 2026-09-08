//! Independent cumulative allowances prevent nested cohort callbacks from
//! replenishing descriptor parsing/checking work.
use super::*;
use crate::native::input_codec::source_bytes::Meter;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AuthoredCreationWork {
    pub parse: usize,
    pub source: usize,
    pub descriptor: usize,
    pub acceptance: usize,
    pub native: usize,
}
impl AuthoredCreationWork {
    pub(super) fn add(self, other: Self) -> Result<Self, DecodeError> {
        Ok(Self {
            parse: add(self.parse, other.parse)?,
            source: add(self.source, other.source)?,
            descriptor: add(self.descriptor, other.descriptor)?,
            acceptance: add(self.acceptance, other.acceptance)?,
            native: add(self.native, other.native)?,
        })
    }
    pub(super) fn subtract(self, other: Self) -> Result<Self, DecodeError> {
        Ok(Self {
            parse: subtract(self.parse, other.parse)?,
            source: subtract(self.source, other.source)?,
            descriptor: subtract(self.descriptor, other.descriptor)?,
            acceptance: subtract(self.acceptance, other.acceptance)?,
            native: subtract(self.native, other.native)?,
        })
    }
    pub(super) fn fits(self, maximum: Self) -> Result<(), DecodeError> {
        maximum.subtract(self)?;
        Ok(())
    }
}
pub(super) fn add(a: usize, b: usize) -> Result<usize, DecodeError> {
    a.checked_add(b).ok_or(CodecError::Capacity.into())
}
pub(super) fn subtract(a: usize, b: usize) -> Result<usize, DecodeError> {
    a.checked_sub(b).ok_or(CodecError::Capacity.into())
}
pub(super) fn multiply(a: usize, b: usize) -> Result<usize, DecodeError> {
    a.checked_mul(b).ok_or(CodecError::Capacity.into())
}

pub(super) struct Budget {
    pub(super) parse: Meter,
    pub(super) source: Meter,
    pub(super) descriptor: Meter,
    pub(super) acceptance: Meter,
    pub(super) native: Meter,
}
impl Budget {
    pub(super) fn new(work: AuthoredCreationWork) -> Self {
        Self {
            parse: Meter::new(work.parse),
            source: Meter::new(work.source),
            descriptor: Meter::new(work.descriptor),
            acceptance: Meter::new(work.acceptance),
            native: Meter::new(work.native),
        }
    }
    pub(super) fn remaining(&self) -> AuthoredCreationWork {
        AuthoredCreationWork {
            parse: self.parse.remaining(),
            source: self.source.remaining(),
            descriptor: self.descriptor.remaining(),
            acceptance: self.acceptance.remaining(),
            native: self.native.remaining(),
        }
    }
    pub(super) fn prepared(&self, quote: BodyConstructionQuote) -> Result<(), DecodeError> {
        self.source.charge(quote.source_inspection_visits)?;
        self.descriptor.charge(quote.model_inspection_visits)?;
        Ok(())
    }
    pub(super) fn built(&self, quote: BodyConstructionQuote) -> Result<(), DecodeError> {
        self.source.charge(quote.source_build_visits)?;
        self.descriptor.charge(quote.model_build_visits)?;
        Ok(())
    }
}

pub(super) struct Reader<'b, 'a> {
    pub(super) tail: &'a [u8],
    pub(super) budget: &'b Budget,
}
impl<'b, 'a> Reader<'b, 'a> {
    pub(super) fn read<T>(
        &mut self,
        f: impl FnOnce(&mut Cursor<'a>) -> Result<T, DecodeError>,
    ) -> Result<T, DecodeError> {
        let mut cursor = Cursor::new(self.tail, self.tail.len(), self.budget.parse.remaining())?;
        let value = f(&mut cursor)?;
        self.budget.parse.charge(cursor.visits_used())?;
        self.tail = self
            .tail
            .get(cursor.offset()..)
            .ok_or(CodecError::Truncated)?;
        Ok(value)
    }
    pub(super) fn start(
        bytes: &'a [u8],
        budget: &'b Budget,
        max_claims: usize,
    ) -> Result<(Self, usize), DecodeError> {
        let mut reader = Self {
            tail: bytes,
            budget,
        };
        let count = reader.read(|cursor| {
            cursor.take(85)?;
            Ok(cursor.count(max_claims)?)
        })?;
        if count == 0 {
            return Err(ContractError::InvalidPolicy.into());
        }
        Ok((reader, count))
    }
    pub(super) fn validation(&mut self) -> Result<ValidationBodyInput<'a>, DecodeError> {
        self.read(ValidationBodyInput::read)
    }
    pub(super) fn group(&mut self, max_declarations: usize) -> Result<Group<'a>, DecodeError> {
        let source_limit = self.budget.source.remaining();
        let claim = self.read(|cursor| ClaimBodyInput::read(cursor, source_limit))?;
        self.budget
            .source
            .charge(claim.parse_quote().source_visits)?;
        let count = self.read(|cursor| Ok(cursor.count(max_declarations)?))?;
        let beginning = self.tail;
        for _ in 0..count {
            self.validation()?;
        }
        let length = subtract(beginning.len(), self.tail.len())?;
        let declarations = beginning.get(..length).ok_or(CodecError::Truncated)?;
        let (max_responses, scope_limits, owner) = self.read(|cursor| {
            let max_responses = cursor.u32()?;
            let scope_limits = scope::ScopeLimits {
                scopes: usize::try_from(cursor.u32()?).map_err(|_| CodecError::Capacity)?,
                roots: usize::try_from(cursor.u32()?).map_err(|_| CodecError::Capacity)?,
                children: usize::try_from(cursor.u32()?).map_err(|_| CodecError::Capacity)?,
            };
            let owner = match cursor.u8()? {
                0 => None,
                1 => Some(creation::Owner {
                    expected: fixed::binding(cursor)?,
                    receipt: fixed::optional_receipt(cursor)?,
                }),
                _ => return Err(CodecError::InvalidTag("owner").into()),
            };
            Ok((max_responses, scope_limits, owner))
        })?;
        Ok(Group {
            claim,
            declarations,
            count,
            max_responses,
            scope_limits,
            owner,
        })
    }
    pub(super) fn finish(self) -> Result<(), DecodeError> {
        if self.tail.is_empty() {
            Ok(())
        } else {
            Err(CodecError::TrailingBytes.into())
        }
    }
}

pub(super) struct Group<'a> {
    pub(super) claim: ClaimBodyInput<'a>,
    pub(super) declarations: &'a [u8],
    pub(super) count: usize,
    pub(super) max_responses: u32,
    pub(super) scope_limits: scope::ScopeLimits,
    pub(super) owner: Option<creation::Owner>,
}
