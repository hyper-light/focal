//! Borrowed recovery collections with one cumulative allowance across callbacks.
//! Plans retain byte spans, never an uncharged array of decoded model objects.
use super::{CodecError, bytes::Cursor};
use focal_model::lifecycle::ContractError;
use std::cell::Cell;

pub(super) struct Meter {
    remaining: Cell<usize>,
}
impl Meter {
    pub(super) fn new(visits: usize) -> Self {
        Self { remaining: Cell::new(visits) }
    }
    pub(super) fn remaining(&self) -> usize { self.remaining.get() }
    pub(super) fn charge(&self, amount: usize) -> Result<(), CodecError> {
        self.remaining.set(self.remaining.get().checked_sub(amount).ok_or(CodecError::Capacity)?);
        Ok(())
    }
    /// The local cursor receives only the remaining shared allowance. Reconcile
    /// consumed work on success and refusal before another callback can run.
    pub(super) fn read<'a, T>(
        &self,
        bytes: &'a [u8],
        read: impl FnOnce(&mut Cursor<'a>) -> Result<T, CodecError>,
    ) -> Result<(T, usize), CodecError> {
        let mut cursor = Cursor::new(bytes, bytes.len(), self.remaining())?;
        let value = read(&mut cursor);
        self.charge(cursor.visits_used())?;
        value.map(|value| (value, cursor.offset()))
    }
    /// Model plans report successful inspection work. A refused inspection has
    /// no usable quote, so consume its entire allowance before returning failure.
    pub(super) fn model<T>(
        &self,
        prepare: impl FnOnce(usize) -> Result<(T, usize), ContractError>,
    ) -> Result<T, ContractError> {
        let available = self.remaining();
        match prepare(available) {
            Ok((value, used)) => { self.charge(used).map_err(model_error)?; Ok(value) }
            Err(error) => { self.charge(available).map_err(model_error)?; Err(error) }
        }
    }
    pub(super) fn budget<T>(
        &self,
        action: impl FnOnce(&mut focal_model::lifecycle::graph::VisitBudget) -> Result<T, ContractError>,
    ) -> Result<T, ContractError> {
        let available = self.remaining();
        let mut visits = focal_model::lifecycle::graph::VisitBudget::new(available);
        let result = action(&mut visits);
        self.charge(available.checked_sub(visits.remaining()).ok_or(ContractError::Capacity)?)
            .map_err(model_error)?;
        result
    }
}

pub(super) fn model_error(error: CodecError) -> ContractError {
    match error {
        CodecError::Capacity => ContractError::Capacity,
        _ => ContractError::InvalidManifest,
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Span<'a> {
    pub(super) bytes: &'a [u8],
    pub(super) count: usize,
}
impl<'a> Span<'a> {
    pub(super) fn read_fixed(cursor: &mut Cursor<'a>, width: usize) -> Result<Self, CodecError> {
        let count = cursor.count(cursor.remaining())?;
        let length = count.checked_mul(width).ok_or(CodecError::Capacity)?;
        Ok(Self { count, bytes: cursor.take(length)? })
    }
    pub(super) fn read_with<T>(
        cursor: &mut Cursor<'a>,
        read: fn(&mut Cursor<'a>) -> Result<T, CodecError>,
    ) -> Result<Self, CodecError> {
        let count = cursor.count(cursor.remaining())?;
        let tail = cursor.unread();
        let start = cursor.offset();
        cursor.visit(count.checked_add(1).ok_or(CodecError::Capacity)?)?;
        for _ in 0..count { read(cursor)?; }
        let length = cursor.offset().checked_sub(start).ok_or(CodecError::Capacity)?;
        Ok(Self { count, bytes: tail.get(..length).ok_or(CodecError::Truncated)? })
    }
}

pub(super) struct Values<'m, 'a, T> {
    span: Span<'a>,
    meter: &'m Meter,
    read: fn(&mut Cursor<'a>) -> Result<T, CodecError>,
    finished: bool,
}
impl<'m, 'a, T> Values<'m, 'a, T> {
    pub(super) fn new(span: Span<'a>, meter: &'m Meter, read: fn(&mut Cursor<'a>) -> Result<T, CodecError>) -> Self {
        Self { span, meter, read, finished: false }
    }
}
impl<T> Iterator for Values<'_, '_, T> {
    type Item = Result<T, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished { return None; }
        let value = (|| {
            self.meter.charge(1)?;
            if self.span.count == 0 {
                self.finished = true;
                if !self.span.bytes.is_empty() { return Err(CodecError::TrailingBytes); }
                return Ok(None);
            }
            let (value, offset) = self.meter.read(self.span.bytes, self.read)?;
            self.span.bytes = self.span.bytes.get(offset..).ok_or(CodecError::Truncated)?;
            self.span.count = self.span.count.checked_sub(1).ok_or(CodecError::Capacity)?;
            Ok(Some(value))
        })();
        match value {
            Ok(Some(value)) => Some(Ok(value)),
            Ok(None) => None,
            Err(error) => { self.finished = true; Some(Err(model_error(error))) }
        }
    }
}
impl<T> std::iter::FusedIterator for Values<'_, '_, T> {}
