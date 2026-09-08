//! Full structural traversal without allocation. This proves framing, closed
//! tags, lengths, UTF-8 and resource bounds only. It deliberately supplies no
//! semantic intent, NativeInput, custody proof or authority/funding capability.
use super::bytes::{Cursor, Error};
use super::*;
use focal_model::{RequestEpoch, RequestId, SessionId, TenantId};

#[derive(Debug, Clone, Copy)]
pub struct InspectionLimits {
    pub bytes: usize,
    pub visits: usize,
    /// Shared across every array in the frame, including nested declarations.
    pub items: usize,
    /// Shared across all UTF-8 fields, including empty descriptions and labels.
    pub text_bytes: usize,
    /// Shared across opaque metadata and inline artifact payloads.
    pub blob_bytes: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    Request { command: u8 },
    EvaluationDeadline,
    ClaimDeadline,
    MonitorDeadline,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputHeader {
    pub ledger: LedgerId,
    pub profile: NativeContentProfile,
    pub kind: FrameKind,
    pub request: Option<RequestKey>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InspectionQuote {
    pub bytes: usize,
    pub visits: usize,
    pub items: usize,
    pub text_bytes: usize,
    pub blob_bytes: usize,
}
#[derive(Debug)]
pub struct StructuralInput<'a> {
    bytes: &'a [u8],
    header: InputHeader,
    quote: InspectionQuote,
}
impl<'a> StructuralInput<'a> {
    pub fn inspect(bytes: &'a [u8], limits: InspectionLimits) -> Result<Self, Error> {
        Self::inspect_with_header(bytes, limits, |_| Ok(()))
    }
    pub(super) fn inspect_with_header<E: From<Error>>(
        bytes: &'a [u8],
        limits: InspectionLimits,
        check_header: impl FnOnce(InputHeader) -> Result<(), E>,
    ) -> Result<Self, E> {
        let mut scan = Scan {
            cursor: Cursor::new(bytes, limits.bytes, limits.visits)?,
            items: limits.items,
            text: limits.text_bytes,
            blobs: limits.blob_bytes,
        };
        if scan.cursor.fixed::<8>()? != MAGIC || scan.cursor.u16()? != VERSION {
            return Err(Error::InvalidTag("input format").into());
        }
        let profile = match scan.cursor.u8()? {
            0 => NativeContentProfile::ProjectionOnly,
            1 => NativeContentProfile::AuthoredV1,
            _ => return Err(Error::InvalidTag("content profile").into()),
        };
        let namespace = scan.closed8(3, "input namespace")?;
        let ledger = LedgerId {
            tenant: TenantId(scan.cursor.fixed()?),
            session: SessionId(scan.cursor.fixed()?),
        };
        let (kind, request) = match namespace {
            0 => {
                let request = RequestKey {
                    principal: ParticipantId(scan.cursor.fixed()?),
                    epoch: RequestEpoch(scan.cursor.u64()?),
                    id: RequestId(scan.cursor.fixed()?),
                };
                let command = scan.closed8(27, "command")?;
                if matches!(
                    (profile, command),
                    (NativeContentProfile::ProjectionOnly, 27)
                        | (NativeContentProfile::AuthoredV1, 0)
                ) {
                    return Err(Error::InvalidTag("creation profile").into());
                }
                (FrameKind::Request { command }, Some(request))
            }
            1 => (FrameKind::EvaluationDeadline, None),
            2 => (FrameKind::ClaimDeadline, None),
            3 => (FrameKind::MonitorDeadline, None),
            _ => return Err(Error::InvalidTag("input namespace").into()),
        };
        let header = InputHeader {
            ledger,
            profile,
            kind,
            request,
        };
        check_header(header)?;
        match kind {
            FrameKind::Request { command } => scan.command(command)?,
            FrameKind::EvaluationDeadline => {
                scan.evaluation()?;
                scan.deadline()?;
            }
            FrameKind::ClaimDeadline => {
                scan.id()?;
                scan.deadline()?;
            }
            FrameKind::MonitorDeadline => {
                scan.id()?;
                scan.id()?;
                scan.deadline()?;
            }
        };
        if scan.cursor.remaining() != 0 {
            return Err(Error::TrailingBytes.into());
        }
        let quote = InspectionQuote {
            bytes: scan.cursor.offset(),
            visits: scan.cursor.visits_used(),
            items: limits
                .items
                .checked_sub(scan.items)
                .ok_or(Error::Capacity)?,
            text_bytes: limits
                .text_bytes
                .checked_sub(scan.text)
                .ok_or(Error::Capacity)?,
            blob_bytes: limits
                .blob_bytes
                .checked_sub(scan.blobs)
                .ok_or(Error::Capacity)?,
        };
        scan.cursor.finish()?;
        Ok(Self {
            bytes,
            header,
            quote,
        })
    }
    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
    pub fn header(&self) -> InputHeader {
        self.header
    }
    pub fn quote(&self) -> InspectionQuote {
        self.quote
    }
}

struct Scan<'a> {
    cursor: Cursor<'a>,
    items: usize,
    text: usize,
    blobs: usize,
}
impl Scan<'_> {
    fn closed8(&mut self, max: u8, field: &'static str) -> Result<u8, Error> {
        let value = self.cursor.u8()?;
        if value > max {
            return Err(Error::InvalidTag(field));
        }
        Ok(value)
    }
    fn closed16(&mut self, min: u16, max: u16, field: &'static str) -> Result<u16, Error> {
        let value = self.cursor.u16()?;
        if value < min || value > max {
            return Err(Error::InvalidTag(field));
        }
        Ok(value)
    }
    fn option(&mut self) -> Result<bool, Error> {
        Ok(self.closed8(1, "option")? == 1)
    }
    fn id(&mut self) -> Result<(), Error> {
        self.cursor.take(16)?;
        Ok(())
    }
    fn hash(&mut self) -> Result<(), Error> {
        self.cursor.take(32)?;
        Ok(())
    }
    fn ledger(&mut self) -> Result<(), Error> {
        self.id()?;
        self.id()
    }
    fn binding(&mut self) -> Result<(), Error> {
        self.ledger()?;
        self.id()?;
        self.hash()?;
        self.cursor.u64()?;
        Ok(())
    }
    fn deadline(&mut self) -> Result<(), Error> {
        self.id()?;
        self.cursor.u64()?;
        self.cursor.u64()?;
        Ok(())
    }
    fn optional_deadline(&mut self) -> Result<(), Error> {
        if self.option()? {
            self.deadline()?;
        }
        Ok(())
    }
    fn receipt(&mut self) -> Result<(), Error> {
        self.id()?;
        self.cursor.u64()?;
        Ok(())
    }
    fn optional_receipt(&mut self) -> Result<(), Error> {
        if self.option()? {
            self.receipt()?;
        }
        Ok(())
    }
    fn artifact_ref(&mut self) -> Result<(), Error> {
        self.id()?;
        self.hash()
    }
    fn owner(&mut self) -> Result<(), Error> {
        if self.option()? {
            self.binding()?;
            self.optional_receipt()?;
        }
        Ok(())
    }
    fn capacities(&mut self) -> Result<(), Error> {
        self.cursor.u32()?;
        self.cursor.u32()?;
        self.cursor.u32()?;
        Ok(())
    }
    fn count(&mut self) -> Result<usize, Error> {
        let count = self.cursor.count(self.items)?;
        self.items = self.items.checked_sub(count).ok_or(Error::Capacity)?;
        Ok(count)
    }
    fn text(&mut self) -> Result<(), Error> {
        let value = self.cursor.text(self.text)?;
        self.text = self.text.checked_sub(value.len()).ok_or(Error::Capacity)?;
        Ok(())
    }
    fn blob(&mut self) -> Result<(), Error> {
        let count = self.cursor.count(self.blobs)?;
        self.cursor.take(count)?;
        self.blobs = self.blobs.checked_sub(count).ok_or(Error::Capacity)?;
        Ok(())
    }
    fn object_ref(&mut self) -> Result<(), Error> {
        self.ledger()?;
        self.closed16(1, 4, "object family")?;
        self.id()
    }
    fn mode(&mut self) -> Result<(), Error> {
        self.closed8(1, "validation mode")?;
        Ok(())
    }
    fn attempt(&mut self) -> Result<(), Error> {
        self.closed8(3, "attempt phase")?;
        self.cursor.u32()?;
        self.id()?;
        self.hash()?;
        self.id()?;
        self.hash()
    }
    fn target(&mut self) -> Result<(), Error> {
        match self.closed8(4, "validation target")? {
            0 => {
                self.binding()?;
                self.cursor.u32()?;
                self.binding()
            }
            1 => {
                self.binding()?;
                self.cursor.u32()?;
                Ok(())
            }
            2 | 3 => self.binding(),
            4 => {
                self.binding()?;
                self.binding()
            }
            _ => Err(Error::InvalidTag("validation target")),
        }
    }
    fn evaluation(&mut self) -> Result<(), Error> {
        self.id()?;
        self.id()?;
        self.cursor.u64()?;
        match self.closed8(4, "evaluation target")? {
            0 => Ok(()),
            1 | 4 => self.id(),
            2 => {
                self.id()?;
                self.cursor.u32()?;
                self.id()
            }
            3 => {
                self.id()?;
                self.cursor.u32()?;
                Ok(())
            }
            _ => Err(Error::InvalidTag("evaluation target")),
        }
    }
    fn report(&mut self) -> Result<(), Error> {
        self.cursor.u64()?;
        self.attempt()?;
        self.closed8(3, "verdict")?;
        self.artifact_ref()
    }
    fn slots(&mut self) -> Result<(), Error> {
        for _ in 0..self.count()? {
            self.cursor.u32()?;
            self.cursor.u32()?;
            self.mode()?;
            for _ in 0..self.count()? {
                self.cursor.u32()?;
                self.id()?;
                self.mode()?;
            }
        }
        Ok(())
    }
    fn claim(&mut self) -> Result<(), Error> {
        self.ledger()?;
        self.id()?;
        self.closed16(1, 1, "claim schema")?;
        self.id()?;
        self.text()?;
        for _ in 0..self.count()? {
            self.closed16(1, 15, "relation kind")?;
            match self.closed8(3, "relation target")? {
                0 | 3 => self.id()?,
                1 => self.object_ref()?,
                2 => {
                    self.closed16(1, 10, "claim action")?;
                }
                _ => return Err(Error::InvalidTag("relation target")),
            }
        }
        for _ in 0..self.count()? {
            self.closed16(1, 6, "scope kind")?;
            self.text()?;
        }
        for _ in 0..self.count()? {
            self.id()?;
            self.hash()?;
        }
        self.slots()?;
        self.optional_deadline()
    }
    fn phase_policy(&mut self) -> Result<(), Error> {
        self.id()?;
        self.hash()?;
        if self.option()? {
            self.hash()?;
        }
        for _ in 0..self.count()? {
            self.id()?;
            self.hash()?;
            self.closed8(1, "agentic flag")?;
            self.cursor.u32()?;
            self.hash()?;
            self.hash()?;
        }
        Ok(())
    }
    fn declaration_fields(&mut self) -> Result<(), Error> {
        self.id()?;
        self.id()?;
        self.cursor.u32()?;
        self.closed16(1, 7, "validation kind")?;
        self.closed16(1, 3, "declared phase")?;
        self.mode()?;
        if self.closed8(3, "target declaration")? == 0 {
            self.cursor.u32()?;
            self.text()?;
        }
        match self.closed8(2, "validation program")? {
            0 => {}
            1 => {
                self.phase_policy()?;
                if self.option()? {
                    self.phase_policy()?;
                }
            }
            2 => self.phase_policy()?,
            _ => return Err(Error::InvalidTag("validation program")),
        }
        self.deadline()
    }
    fn declaration(&mut self) -> Result<(), Error> {
        self.binding()?;
        self.declaration_fields()
    }
    fn validation(&mut self) -> Result<(), Error> {
        self.ledger()?;
        self.id()?;
        self.closed16(1, 1, "validation schema")?;
        self.declaration_fields()?;
        self.text()?;
        if self.option()? {
            self.text()?;
        }
        for _ in 0..self.count()? {
            self.id()?;
        }
        self.cursor.u64()?;
        Ok(())
    }
    fn projection(&mut self) -> Result<(), Error> {
        self.binding()?;
        self.id()?;
        self.id()?;
        self.optional_deadline()?;
        self.cursor.u32()?;
        for _ in 0..self.count()? {
            self.closed8(1, "graph edge")?;
            self.id()?;
        }
        self.binding()?;
        self.closed8(1, "cause")?;
        self.id()?;
        for _ in 0..self.count()? {
            self.closed8(1, "correction")?;
            self.object_ref()?;
        }
        self.binding()?;
        self.id()?;
        self.slots()?;
        self.capacities()?;
        self.owner()
    }
    fn artifact(&mut self) -> Result<(), Error> {
        self.ledger()?;
        self.id()?;
        self.closed16(1, 1, "artifact schema")?;
        self.text()?;
        self.hash()?;
        self.blob()?;
        if self.closed8(1, "artifact payload")? == 0 {
            self.blob()?;
        } else {
            self.id()?;
            self.hash()?;
            self.cursor.u64()?;
            self.closed16(1, 3, "content class")?;
        }
        self.id()?;
        self.optional_receipt()?;
        if self.option()? {
            self.id()?;
            self.id()?;
            self.target()?;
            self.cursor.u64()?;
            self.attempt()?;
            self.closed8(3, "result verdict")?;
        }
        if self.option()? {
            self.id()?;
            self.cursor.u32()?;
            match self.closed8(2, "work role")? {
                0 => {
                    self.cursor.u32()?;
                }
                1 => {
                    self.closed8(3, "work failure")?;
                }
                2 => {
                    self.artifact_ref()?;
                    self.closed8(3, "receipt failure")?;
                }
                _ => return Err(Error::InvalidTag("work role")),
            }
        }
        for _ in 0..self.count()? {
            self.object_ref()?;
        }
        for _ in 0..self.count()? {
            self.text()?;
        }
        Ok(())
    }
    fn command(&mut self, tag: u8) -> Result<(), Error> {
        match tag {
            0 => {
                for _ in 0..self.count()? {
                    self.projection()?;
                }
                for _ in 0..self.count()? {
                    self.declaration()?;
                }
                Ok(())
            }
            1 | 2 | 16 | 21 | 23 => self.binding(),
            3 | 14 | 18 => {
                self.binding()?;
                self.evaluation()?;
                self.binding()
            }
            4 | 15 | 19 => {
                self.binding()?;
                self.evaluation()?;
                self.binding()?;
                self.report()?;
                self.artifact()
            }
            5 | 20 => {
                self.binding()?;
                self.id()
            }
            6 => {
                self.binding()?;
                self.cursor.u32()?;
                self.artifact()
            }
            7 => {
                self.binding()?;
                self.closed8(3, "work failure")?;
                self.artifact()
            }
            8 | 10 | 11 | 17 => {
                self.binding()?;
                self.binding()
            }
            9 => {
                self.binding()?;
                self.binding()?;
                self.text()?;
                self.closed8(3, "confidence")?;
                self.closed8(5, "response outcome")?;
                for _ in 0..self.count()? {
                    self.cursor.u32()?;
                    self.artifact_ref()?;
                }
                for _ in 0..self.count()? {
                    self.artifact_ref()?;
                }
                Ok(())
            }
            12 => {
                self.binding()?;
                self.cursor.u32()?;
                self.artifact_ref()
            }
            13 => {
                self.binding()?;
                self.binding()?;
                self.closed8(3, "receipt failure")?;
                self.artifact()
            }
            22 => {
                self.binding()?;
                self.receipt()?;
                self.id()?;
                self.id()
            }
            24 => {
                self.binding()?;
                self.optional_receipt()?;
                self.id()?;
                for _ in 0..self.count()? {
                    self.closed8(2, "wait predicate")?;
                    self.id()?;
                }
                self.deadline()
            }
            25 => {
                self.binding()?;
                self.optional_receipt()?;
                self.id()?;
                self.binding()?;
                self.binding()
            }
            26 => {
                self.binding()?;
                self.optional_receipt()?;
                self.id()
            }
            27 => {
                for _ in 0..self.count()? {
                    self.claim()?;
                    for _ in 0..self.count()? {
                        self.validation()?;
                    }
                    self.cursor.u32()?;
                    self.capacities()?;
                    self.owner()?;
                }
                Ok(())
            }
            _ => Err(Error::InvalidTag("command")),
        }
    }
}
