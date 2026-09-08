//! Allocation-free byte cursors and encoding sinks. Integer fields are little
//! endian; count and UTF-8 fields carry a u32 prefix. No native authority or
//! enum interpretation is supplied by these primitives.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Truncated,
    TrailingBytes,
    InvalidTag(&'static str),
    InvalidUtf8,
    Capacity,
    Allocation,
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("truncated native input"),
            Self::TrailingBytes => formatter.write_str("trailing native input bytes"),
            Self::InvalidTag(field) => write!(formatter, "invalid native {field} tag"),
            Self::InvalidUtf8 => formatter.write_str("invalid native UTF-8 text"),
            Self::Capacity => formatter.write_str("native codec capacity exceeded"),
            Self::Allocation => formatter.write_str("native codec allocation failed"),
        }
    }
}
impl std::error::Error for Error {}

fn add(left: usize, right: usize) -> Result<usize, Error> {
    left.checked_add(right).ok_or(Error::Capacity)
}

#[derive(Debug)]
struct Visits {
    used: usize,
    maximum: usize,
}
impl Visits {
    fn new(maximum: usize) -> Self {
        Self { used: 0, maximum }
    }
    fn after(&self, charge: usize) -> Result<usize, Error> {
        let used = add(self.used, charge)?;
        if used > self.maximum {
            return Err(Error::Capacity);
        }
        Ok(used)
    }
    fn charge(&mut self, charge: usize) -> Result<(), Error> {
        self.used = self.after(charge)?;
        Ok(())
    }
}

pub(in crate::native) struct Cursor<'a> {
    // Only the unread tail is retained; remaining needs no subtraction or clamp.
    bytes: &'a [u8],
    offset: usize,
    visits: Visits,
}

impl<'a> Cursor<'a> {
    pub(in crate::native) fn new(
        bytes: &'a [u8],
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<Self, Error> {
        if bytes.len() > max_bytes {
            return Err(Error::Capacity);
        }
        Ok(Self {
            bytes,
            offset: 0,
            visits: Visits::new(max_visits),
        })
    }
    pub(in crate::native) fn offset(&self) -> usize {
        self.offset
    }
    pub(in crate::native) fn remaining(&self) -> usize {
        self.bytes.len()
    }
    pub(in crate::native) fn unread(&self) -> &'a [u8] {
        self.bytes
    }
    pub(in crate::native) fn visits_used(&self) -> usize {
        self.visits.used
    }

    /// Debit non-read work before comparisons or integrity verification. This
    /// shares the cursor's allowance rather than opening a nested fresh meter.
    pub(in crate::native) fn visit(&mut self, amount: usize) -> Result<(), Error> {
        self.visits.charge(amount)
    }

    /// Each primitive read costs one operation plus its byte count. Empty
    /// reads still cost one. Refusal leaves offset and visits unchanged.
    pub(in crate::native) fn take(&mut self, len: usize) -> Result<&'a [u8], Error> {
        let end = add(self.offset, len)?;
        let used = self.visits.after(add(1, len)?)?;
        let (bytes, remaining) = self.bytes.split_at_checked(len).ok_or(Error::Truncated)?;
        self.bytes = remaining;
        self.offset = end;
        self.visits.used = used;
        Ok(bytes)
    }
    pub(in crate::native) fn fixed<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        self.take(N)?.try_into().map_err(|_| Error::Truncated)
    }
    pub(in crate::native) fn u8(&mut self) -> Result<u8, Error> {
        let [value] = self.fixed::<1>()?;
        Ok(value)
    }
    pub(in crate::native) fn u16(&mut self) -> Result<u16, Error> {
        Ok(u16::from_le_bytes(self.fixed()?))
    }
    pub(in crate::native) fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(self.fixed()?))
    }
    pub(in crate::native) fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(self.fixed()?))
    }
    /// Count validation is one additional operation after reading the prefix.
    /// Composite reads preserve any successfully consumed prefix on refusal.
    pub(in crate::native) fn count(&mut self, maximum: usize) -> Result<usize, Error> {
        let count = self.u32()?;
        self.visits.charge(1)?;
        let count = usize::try_from(count).map_err(|_| Error::Capacity)?;
        if count > maximum {
            return Err(Error::Capacity);
        }
        Ok(count)
    }
    /// Taking bytes and validating UTF-8 are two distinct linear operations.
    /// The returned string borrows the input; no String or scratch array exists.
    pub(in crate::native) fn text(&mut self, maximum: usize) -> Result<&'a str, Error> {
        let len = self.count(maximum)?;
        let bytes = self.take(len)?;
        self.visits.charge(add(1, len)?)?;
        std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)
    }
    /// End checking, like offset/remaining, only inspects cursor metadata.
    pub(in crate::native) fn finish(self) -> Result<(), Error> {
        if !self.bytes.is_empty() {
            return Err(Error::TrailingBytes);
        }
        Ok(())
    }
}

pub(in crate::native) trait Sink {
    fn write(&mut self, bytes: &[u8]) -> Result<(), Error>;
    /// Debit explicitly quoted non-byte work without changing the output.
    /// The caller supplies the complete charge; zero has no implicit overhead.
    fn visit(&mut self, amount: usize) -> Result<(), Error>;
}

pub(in crate::native) struct CountingSink {
    len: usize,
    maximum: usize,
    visits: Visits,
}
impl CountingSink {
    pub(in crate::native) fn new(max_bytes: usize, max_visits: usize) -> Self {
        Self {
            len: 0,
            maximum: max_bytes,
            visits: Visits::new(max_visits),
        }
    }
    pub(in crate::native) fn len(&self) -> usize {
        self.len
    }
    pub(in crate::native) fn visits_used(&self) -> usize {
        self.visits.used
    }
}
impl Sink for CountingSink {
    fn visit(&mut self, amount: usize) -> Result<(), Error> {
        self.visits.charge(amount)
    }
    fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let len = add(self.len, bytes.len())?;
        let used = self.visits.after(add(1, bytes.len())?)?;
        if len > self.maximum {
            return Err(Error::Capacity);
        }
        self.len = len;
        self.visits.used = used;
        Ok(())
    }
}

pub(in crate::native) struct SliceSink<'a> {
    bytes: &'a mut [u8],
    written: usize,
    visits: Visits,
}
impl<'a> SliceSink<'a> {
    pub(in crate::native) fn new(bytes: &'a mut [u8], max_visits: usize) -> Self {
        Self {
            bytes,
            written: 0,
            visits: Visits::new(max_visits),
        }
    }
    pub(in crate::native) fn len(&self) -> usize {
        self.written
    }
    pub(in crate::native) fn visits_used(&self) -> usize {
        self.visits.used
    }
    /// Exact fill is a metadata check and adds no write visits. Counting and
    /// slice encoders therefore have identical costs for the same write stream.
    pub(in crate::native) fn finish(self) -> Result<(), Error> {
        if self.written != self.bytes.len() {
            return Err(Error::Truncated);
        }
        Ok(())
    }
}
impl Sink for SliceSink<'_> {
    fn visit(&mut self, amount: usize) -> Result<(), Error> {
        self.visits.charge(amount)
    }
    fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let end = add(self.written, bytes.len())?;
        let used = self.visits.after(add(1, bytes.len())?)?;
        let target = self
            .bytes
            .get_mut(self.written..end)
            .ok_or(Error::Capacity)?;
        // The checked range has exactly bytes.len() elements.
        target.copy_from_slice(bytes);
        self.written = end;
        self.visits.used = used;
        Ok(())
    }
}

pub(in crate::native) fn write_u8<S: Sink + ?Sized>(sink: &mut S, value: u8) -> Result<(), Error> {
    sink.write(&[value])
}
pub(in crate::native) fn write_u16<S: Sink + ?Sized>(
    sink: &mut S,
    value: u16,
) -> Result<(), Error> {
    sink.write(&value.to_le_bytes())
}
pub(in crate::native) fn write_u32<S: Sink + ?Sized>(
    sink: &mut S,
    value: u32,
) -> Result<(), Error> {
    sink.write(&value.to_le_bytes())
}
pub(in crate::native) fn write_u64<S: Sink + ?Sized>(
    sink: &mut S,
    value: u64,
) -> Result<(), Error> {
    sink.write(&value.to_le_bytes())
}
pub(in crate::native) fn write_count<S: Sink + ?Sized>(
    sink: &mut S,
    value: usize,
) -> Result<(), Error> {
    write_u32(sink, u32::try_from(value).map_err(|_| Error::Capacity)?)
}
pub(in crate::native) fn write_text<S: Sink + ?Sized>(
    sink: &mut S,
    value: &str,
) -> Result<(), Error> {
    write_count(sink, value.len())?;
    write_raw(sink, value.as_bytes())
}
/// Raw fields have no implicit length prefix. Use write_count/write_text where
/// the schema requires one; fixed-width IDs and hashes use raw bytes directly.
pub(in crate::native) fn write_raw<S: Sink + ?Sized>(
    sink: &mut S,
    value: &[u8],
) -> Result<(), Error> {
    sink.write(value)
}

#[cfg(test)]
#[path = "bytes_tests.rs"]
mod tests;
