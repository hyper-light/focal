use crate::ProtocolError;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use serde::Serialize;
use std::io::{self, Write};

pub struct EncodedFrame {
    bytes: Vec<u8>,
    _allocation: Allocation,
}
impl EncodedFrame {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}
pub struct InputFrame {
    bytes: Vec<u8>,
    _allocation: Allocation,
}
impl InputFrame {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}
impl std::fmt::Debug for EncodedFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedFrame")
            .field("bytes", &self.bytes.len())
            .finish()
    }
}
impl std::fmt::Debug for InputFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InputFrame")
            .field("bytes", &self.bytes.len())
            .finish()
    }
}

/// Consumes at most one line per call. The host retains and retries the unused suffix.
pub struct FrameDecoder {
    bytes: Vec<u8>,
    limit: usize,
    budget: MemoryBudget,
    allocation: Option<Allocation>,
    closed: bool,
}
impl FrameDecoder {
    pub fn new(max_bytes: usize, budget: MemoryBudget) -> Result<Self, ProtocolError> {
        if max_bytes == 0 {
            return Err(ProtocolError::Limits);
        }
        Ok(Self {
            bytes: Vec::new(),
            limit: max_bytes,
            budget,
            allocation: None,
            closed: false,
        })
    }
    pub fn push(&mut self, input: &[u8]) -> Result<(usize, Option<InputFrame>), ProtocolError> {
        if self.closed {
            return Err(ProtocolError::Closed);
        }
        if input.is_empty() {
            return Ok((0, None));
        }
        if self.allocation.is_none() {
            self.allocation = Some(
                self.budget
                    .reserve(BudgetKind::Control, BudgetLane::Completion, self.limit)?
                    .commit(),
            );
            self.bytes
                .try_reserve_exact(self.limit)
                .map_err(|_| ProtocolError::Capacity)?;
        }
        let mut consumed = 0usize;
        for byte in input {
            consumed = consumed.checked_add(1).ok_or(ProtocolError::Capacity)?;
            if *byte == b'\n' {
                if self.bytes.last() == Some(&b'\r') {
                    self.bytes.pop();
                }
                let bytes = std::mem::take(&mut self.bytes);
                let allocation = self.allocation.take().ok_or(ProtocolError::Closed)?;
                return Ok((
                    consumed,
                    Some(InputFrame {
                        bytes,
                        _allocation: allocation,
                    }),
                ));
            }
            if self.bytes.len() >= self.limit {
                self.closed = true;
                self.bytes = Vec::new();
                self.allocation.take();
                return Err(ProtocolError::Frame);
            }
            self.bytes.push(*byte);
        }
        Ok((consumed, None))
    }
    /// EOF never promotes an unterminated JSON fragment into a protocol message.
    pub fn finish(&mut self) -> Result<(), ProtocolError> {
        self.closed = true;
        let partial = !self.bytes.is_empty();
        self.bytes = Vec::new();
        self.allocation.take();
        if partial {
            Err(ProtocolError::Frame)
        } else {
            Ok(())
        }
    }
}

pub(crate) struct BoundedWriter {
    pub bytes: Vec<u8>,
    pub limit: usize,
}
impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("capacity"))?;
        if next > self.limit {
            return Err(io::Error::other("capacity"));
        }
        if next > self.bytes.capacity() {
            let target = self
                .bytes
                .capacity()
                .checked_mul(2)
                .unwrap_or(self.limit)
                .max(256)
                .max(next)
                .min(self.limit);
            let additional = target
                .checked_sub(self.bytes.len())
                .ok_or_else(|| io::Error::other("capacity"))?;
            self.bytes
                .try_reserve_exact(additional)
                .map_err(|_| io::Error::other("capacity"))?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(crate) fn encode<T: Serialize>(
    value: &T,
    limit: usize,
    budget: &MemoryBudget,
) -> Result<EncodedFrame, ProtocolError> {
    let mut allocation = budget
        .reserve(BudgetKind::Control, BudgetLane::Completion, limit)?
        .commit();
    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        limit,
    };
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        serde_json::to_writer(&mut writer, value)
    }))
    .map_err(|_| ProtocolError::Dependency)?
    .map_err(|_| ProtocolError::Encode)?;
    writer.write_all(b"\n").map_err(|_| ProtocolError::Encode)?;
    allocation.shrink_to(writer.bytes.capacity())?;
    Ok(EncodedFrame {
        bytes: writer.bytes,
        _allocation: allocation,
    })
}
