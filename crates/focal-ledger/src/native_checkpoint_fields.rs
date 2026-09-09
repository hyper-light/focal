use super::*;

pub(super) fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b).ok_or(Error::Capacity)
}
pub(super) fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b).ok_or(Error::Capacity)
}
pub(super) fn profile(value: NativeContentProfile) -> u8 {
    match value {
        NativeContentProfile::ProjectionOnly => 0,
        NativeContentProfile::AuthoredV1 => 1,
    }
}

pub(super) struct Sink<'a, F, E> {
    maximum: usize,
    available: usize,
    remaining: usize,
    pub(super) length: usize,
    pub(super) hash: blake3::Hasher,
    output: &'a mut F,
    pub(super) error: Option<E>,
}
impl<'a, F, E> Sink<'a, F, E>
where
    F: FnMut(&[u8]) -> Result<(), E>,
{
    pub(super) fn new(maximum: usize, visits: usize, output: &'a mut F) -> Self {
        Self {
            maximum,
            available: visits,
            remaining: visits,
            length: 0,
            hash: blake3::Hasher::new_derive_key(HASH_DOMAIN),
            output,
            error: None,
        }
    }
    pub(super) fn used(&self) -> usize {
        self.available.saturating_sub(self.remaining)
    }
    pub(super) fn visit(&mut self, amount: usize) -> Result<(), Error> {
        self.remaining = self.remaining.checked_sub(amount).ok_or(Error::Capacity)?;
        Ok(())
    }
    fn emit(&mut self, bytes: &[u8], hash: bool) -> Result<(), Error> {
        let next = add(self.length, bytes.len())?;
        if next > self.maximum {
            return Err(Error::Capacity);
        }
        self.visit(add(mul(bytes.len(), if hash { 2 } else { 1 })?, 1)?)?;
        if hash {
            self.hash.update(bytes);
        }
        match (self.output)(bytes) {
            Ok(()) => {
                self.length = next;
                Ok(())
            }
            Err(error) => {
                self.error = Some(error);
                Err(Error::Output)
            }
        }
    }
    pub(super) fn raw(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.emit(bytes, true)
    }
    pub(super) fn trailer(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.emit(bytes, false)
    }
    pub(super) fn u8(&mut self, value: u8) -> Result<(), Error> {
        self.raw(&[value])
    }
    pub(super) fn u16(&mut self, value: u16) -> Result<(), Error> {
        self.raw(&value.to_le_bytes())
    }
    pub(super) fn u32(&mut self, value: u32) -> Result<(), Error> {
        self.raw(&value.to_le_bytes())
    }
    pub(super) fn u64(&mut self, value: u64) -> Result<(), Error> {
        self.raw(&value.to_le_bytes())
    }
}

pub(super) trait Configuration {
    fn count(&self, part: usize) -> Result<usize, Error>;
    fn member(&self, part: usize, index: usize) -> Result<u64, Error>;
    fn auto_leave(&self) -> bool;
}
impl Configuration for MembershipConfiguration {
    fn count(&self, part: usize) -> Result<usize, Error> {
        Ok(members(self, part)?.len())
    }
    fn member(&self, part: usize, index: usize) -> Result<u64, Error> {
        members(self, part)?
            .get(index)
            .copied()
            .ok_or(Error::Invalid("membership index"))
    }
    fn auto_leave(&self) -> bool {
        self.auto_leave
    }
}
fn members(value: &MembershipConfiguration, part: usize) -> Result<&[u64], Error> {
    match part {
        0 => Ok(&value.voters),
        1 => Ok(&value.learners),
        2 => Ok(&value.voters_outgoing),
        3 => Ok(&value.learners_next),
        _ => Err(Error::Invalid("membership part")),
    }
}
fn contains(value: &impl Configuration, part: usize, sought: u64) -> Result<bool, Error> {
    let mut low = 0usize;
    let mut high = value.count(part)?;
    while low < high {
        let middle = add(low, high.checked_sub(low).ok_or(Error::Capacity)? / 2)?;
        let member = value.member(part, middle)?;
        if member == sought {
            return Ok(true);
        }
        if member < sought {
            low = add(middle, 1)?;
        } else {
            high = middle;
        }
    }
    Ok(false)
}

/// Equivalent canonical configuration constraints to consensus validation,
/// applied directly to borrowed sorted lists without Vec/BTreeSet clones.
pub(super) fn check_configuration(
    value: &impl Configuration,
    limit: usize,
    mut charge: impl FnMut(usize) -> Result<(), Error>,
) -> Result<(), Error> {
    charge(5)?;
    let mut count = 0usize;
    for part in 0..4 {
        let size = value.count(part)?;
        if size > 1024 {
            return Err(Error::Capacity);
        }
        count = add(count, size)?;
    }
    if count > limit || count > 2048 {
        return Err(Error::Capacity);
    }
    // Includes sorted scans, terminal probes and each bounded membership seek.
    charge(mul(add(count, 5)?, 1024)?)?;
    if value.count(0)? == 0
        || (value.count(2)? == 0 && (value.auto_leave() || value.count(3)? != 0))
    {
        return Err(Error::Invalid("membership configuration"));
    }
    for part in 0..4 {
        let mut previous = 0u64;
        for index in 0..value.count(part)? {
            let member = value.member(part, index)?;
            if member <= previous {
                return Err(Error::Invalid("canonical membership"));
            }
            previous = member;
            if (part == 1 && (contains(value, 0, member)? || contains(value, 2, member)?))
                || (part == 3
                    && (!contains(value, 2, member)?
                        || contains(value, 0, member)?
                        || contains(value, 1, member)?))
            {
                return Err(Error::Invalid("membership overlap"));
            }
        }
    }
    Ok(())
}
pub(super) fn write_configuration<F, E>(
    sink: &mut Sink<'_, F, E>,
    value: &MembershipConfiguration,
) -> Result<(), Error>
where
    F: FnMut(&[u8]) -> Result<(), E>,
{
    sink.visit(5)?;
    for part in 0..4 {
        let members = members(value, part)?;
        sink.u32(u32::try_from(members.len()).map_err(|_| Error::Capacity)?)?;
        sink.visit(add(members.len(), 1)?)?;
        for member in members {
            sink.u64(*member)?;
        }
    }
    sink.u8(u8::from(value.auto_leave))
}

pub(super) struct Cursor<'a> {
    pub(super) bytes: &'a [u8],
    offset: usize,
    pub(super) remaining: usize,
}
impl<'a> Cursor<'a> {
    pub(super) fn new(bytes: &'a [u8], remaining: usize) -> Self {
        Self {
            bytes,
            offset: 0,
            remaining,
        }
    }
    pub(super) fn charge(&mut self, work: usize) -> Result<(), Error> {
        self.remaining = self.remaining.checked_sub(work).ok_or(Error::Capacity)?;
        Ok(())
    }
    pub(super) fn take(&mut self, length: usize) -> Result<&'a [u8], Error> {
        self.charge(add(length, 1)?)?;
        let end = add(self.offset, length)?;
        let value = self.bytes.get(self.offset..end).ok_or(Error::Truncated)?;
        self.offset = end;
        Ok(value)
    }
    pub(super) fn fixed<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        self.take(N)?.try_into().map_err(|_| Error::Truncated)
    }
    pub(super) fn u8(&mut self) -> Result<u8, Error> {
        self.fixed::<1>().map(u8::from_le_bytes)
    }
    pub(super) fn u16(&mut self) -> Result<u16, Error> {
        self.fixed().map(u16::from_le_bytes)
    }
    pub(super) fn u32(&mut self) -> Result<u32, Error> {
        self.fixed().map(u32::from_le_bytes)
    }
    pub(super) fn u64(&mut self) -> Result<u64, Error> {
        self.fixed().map(u64::from_le_bytes)
    }
    pub(super) fn boolean(&mut self) -> Result<bool, Error> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::Invalid("boolean")),
        }
    }
    pub(super) fn finish(&mut self) -> Result<(), Error> {
        self.charge(1)?;
        if self.offset != self.bytes.len() {
            return Err(Error::Invalid("trailing bytes"));
        }
        Ok(())
    }
}
