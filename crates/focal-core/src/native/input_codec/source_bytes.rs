//! Sequential byte reads sharing one allowance across nested model callbacks.
//! The meter is borrowed from the exclusive input view; no iterator can reset it.
use super::bytes::Cursor;
use super::{CodecError, ContractError};
use std::cell::Cell;

#[derive(Debug)]
pub(super) struct Meter {
    remaining: Cell<usize>,
}
impl Meter {
    pub(super) fn new(visits: usize) -> Self {
        Self {
            remaining: Cell::new(visits),
        }
    }
    pub(super) fn reset(&mut self, visits: usize) {
        self.remaining.set(visits);
    }
    pub(super) fn remaining(&self) -> usize {
        self.remaining.get()
    }
    pub(super) fn charge(&self, amount: usize) -> Result<(), CodecError> {
        self.remaining.set(
            self.remaining
                .get()
                .checked_sub(amount)
                .ok_or(CodecError::Capacity)?,
        );
        Ok(())
    }
}
pub(super) fn model_error(error: CodecError) -> ContractError {
    match error {
        CodecError::Capacity => ContractError::Capacity,
        _ => ContractError::InvalidManifest,
    }
}

/// The inner cursor retains its own overflow checks; all work is prepaid from
/// the shared meter before a read or UTF-8 scan can execute.
pub(super) struct SourceCursor<'m, 'a> {
    inner: Cursor<'a>,
    meter: &'m Meter,
}
impl<'m, 'a> SourceCursor<'m, 'a> {
    pub(super) fn new(bytes: &'a [u8], meter: &'m Meter) -> Result<Self, CodecError> {
        Ok(Self {
            inner: Cursor::new(bytes, bytes.len(), usize::MAX)?,
            meter,
        })
    }
    pub(super) fn offset(&self) -> usize {
        self.inner.offset()
    }
    pub(super) fn remaining(&self) -> usize {
        self.inner.remaining()
    }
    pub(super) fn meter(&self) -> &'m Meter {
        self.meter
    }
    pub(super) fn take(&mut self, length: usize) -> Result<&'a [u8], CodecError> {
        self.meter
            .charge(length.checked_add(1).ok_or(CodecError::Capacity)?)?;
        self.inner.take(length)
    }
    pub(super) fn fixed<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        self.take(N)?.try_into().map_err(|_| CodecError::Truncated)
    }
    pub(super) fn u8(&mut self) -> Result<u8, CodecError> {
        let [value] = self.fixed()?;
        Ok(value)
    }
    pub(super) fn u16(&mut self) -> Result<u16, CodecError> {
        Ok(u16::from_le_bytes(self.fixed()?))
    }
    pub(super) fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_le_bytes(self.fixed()?))
    }
    pub(super) fn count(&mut self, maximum: usize) -> Result<usize, CodecError> {
        let value = self.u32()?;
        self.meter.charge(1)?;
        let value = usize::try_from(value).map_err(|_| CodecError::Capacity)?;
        if value > maximum {
            return Err(CodecError::Capacity);
        }
        Ok(value)
    }
    pub(super) fn text(&mut self, maximum: usize) -> Result<&'a str, CodecError> {
        let length = self.count(maximum)?;
        let bytes = self.take(length)?;
        self.meter
            .charge(length.checked_add(1).ok_or(CodecError::Capacity)?)?;
        std::str::from_utf8(bytes).map_err(|_| CodecError::InvalidUtf8)
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Span<'a> {
    pub(super) bytes: &'a [u8],
    pub(super) count: usize,
}
impl<'a> Span<'a> {
    pub(super) const EMPTY: Self = Self {
        bytes: &[],
        count: 0,
    };
    pub(super) fn read(cursor: &mut Cursor<'a>, width: usize) -> Result<Self, CodecError> {
        let count = cursor.count(cursor.remaining())?;
        let bytes = cursor.take(count.checked_mul(width).ok_or(CodecError::Capacity)?)?;
        Ok(Self { bytes, count })
    }
}

pub(super) struct Values<'m, 'a, T> {
    tail: &'a [u8],
    left: usize,
    meter: &'m Meter,
    read: fn(&mut SourceCursor<'m, 'a>) -> Result<T, CodecError>,
}
impl<'m, 'a, T> Values<'m, 'a, T> {
    pub(super) fn new(
        span: Span<'a>,
        meter: &'m Meter,
        read: fn(&mut SourceCursor<'m, 'a>) -> Result<T, CodecError>,
    ) -> Self {
        Self {
            tail: span.bytes,
            left: span.count,
            meter,
            read,
        }
    }
}
impl<T> Iterator for Values<'_, '_, T> {
    type Item = Result<T, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return if self.tail.is_empty() {
                None
            } else {
                self.tail = &[];
                Some(Err(ContractError::InvalidManifest))
            };
        }
        let value = (|| {
            let mut cursor = SourceCursor::new(self.tail, self.meter)?;
            let value = (self.read)(&mut cursor)?;
            self.tail = self
                .tail
                .get(cursor.offset()..)
                .ok_or(CodecError::Truncated)?;
            self.left = self.left.checked_sub(1).ok_or(CodecError::Capacity)?;
            Ok(value)
        })();
        if value.is_err() {
            self.left = 0;
            self.tail = &[];
        }
        Some(value.map_err(model_error))
    }
}
