/// One continuous stream sequence line per ledger (23 §6). Cursor positions,
/// delta identities and retention floors are `SessionSeq` values on this line:
/// the legacy prefix `0..=N` (the V1 domain sequence, sealed at activation)
/// followed by the native records. A genesis ledger has `N = 0` and native
/// record `s` is stream sequence `s`; an imported ledger's native sequence one
/// is the image of the legacy prefix (23 §5) and native record `s > 1` is
/// stream sequence `N + s - 1`. The legacy tail keeps its retained schema-1
/// deltas; every later delta is derived on demand from the committed native
/// events (`Key::Event` rows, 22 §3), which are retained with the prefix, so
/// the native part of the line never expires before the retention floor does.
impl Session {
    /// The sealed legacy domain prefix; zero on a genesis ledger.
    fn legacy_prefix(&self) -> SessionSeq {
        self.core.sequence()
    }
    /// The committed native prefix that serves this replica's stream, once
    /// activation is committed and the engine is hosted here.
    fn native_stream_core(&self) -> Option<&Core<NativeState>> {
        if !self.activation.is_native() {
            return None;
        }
        self.native
            .as_deref()
            .and_then(|engine| engine.committed_core().ok())
    }
    /// The stream sequence of a native record.
    pub fn stream_sequence_of(&self, native: SessionSeq) -> Result<SessionSeq, LedgerError> {
        stream_line(self.activation, self.legacy_prefix(), native)
    }
    /// The native record at a stream sequence past the legacy prefix.
    fn native_sequence_of(&self, stream: SessionSeq) -> Option<SessionSeq> {
        let legacy = self.legacy_prefix();
        if stream <= legacy {
            return None;
        }
        match self.activation {
            LedgerActivation::Native {
                kind: ActivationKind::Genesis,
                ..
            } => Some(stream),
            LedgerActivation::Native {
                kind: ActivationKind::Imported,
                ..
            } => stream
                .0
                .checked_sub(legacy.0)
                .and_then(|offset| offset.checked_add(1))
                .map(SessionSeq),
            LedgerActivation::V1 => None,
        }
    }
    /// The published end of the stream line: the legacy prefix on a legacy
    /// ledger, the committed native prefix's stream position on a native one.
    pub fn stream_published(&self) -> SessionSeq {
        self.native
            .as_deref()
            .filter(|_| self.activation.is_native())
            .and_then(|engine| engine.sequence().ok())
            .and_then(|native| self.stream_sequence_of(native).ok())
            .unwrap_or_else(|| self.sequence())
    }
    /// The native record position of a retained delta cursor past the legacy
    /// prefix, when that exact event exists.
    fn native_delta_exists(&self, position: Position, ordinal: u32) -> bool {
        self.native_stream_core()
            .zip(self.native_sequence_of(position.sequence))
            .is_some_and(|(core, native)| core.native_event(native, ordinal).is_some())
    }
    /// Replay retained legacy deltas and then derived native deltas after a
    /// validated position, within the work limits (see `DeltaSource`).
    fn replay_deltas(
        &self,
        after: Position,
        limit: ReplayLimit,
        visit: &mut dyn FnMut(&Delta) -> Result<(), StreamError>,
    ) -> Result<Position, StreamError> {
        if self.failed {
            return Err(StreamError::SourceUnavailable);
        }
        self.validate_replay_position(after)?;
        if limit.max_items == 0 || limit.max_bytes == 0 || limit.max_sequences == 0 {
            return Err(StreamError::Invalid("zero replay budget"));
        }
        let through = SessionSeq(
            after
                .sequence
                .0
                .saturating_add(limit.max_sequences)
                .min(self.stream_published().0),
        );
        let mut returned = after;
        let mut items = 0usize;
        let mut bytes = 0usize;
        // One more delta of `size` bytes fits the work limits; a first delta
        // that cannot fit is a capacity refusal, never a silent skip.
        let mut admit = |size: usize| -> Result<bool, StreamError> {
            if items >= limit.max_items || bytes.saturating_add(size) > limit.max_bytes {
                if items == 0 {
                    return Err(StreamError::Capacity);
                }
                return Ok(false);
            }
            bytes = bytes.checked_add(size).ok_or(StreamError::Capacity)?;
            items = items.checked_add(1).ok_or(StreamError::Capacity)?;
            Ok(true)
        };
        let start = self
            .deltas
            .partition_point(|d| Position::after_delta(d.delta.id) <= after);
        for delta in self.deltas.range(start..) {
            if delta.delta.id.sequence > through {
                break;
            }
            if !admit(delta.bytes)? {
                return Ok(returned);
            }
            visit(&delta.delta)?;
            returned = Position::after_delta(delta.delta.id);
        }
        let legacy = self.legacy_prefix();
        if let Some(core) = self.native_stream_core() {
            let (mut stream, mut ordinal) = if returned.sequence > legacy {
                match returned.offset {
                    PositionOffset::Delta(ordinal) => (
                        returned.sequence,
                        ordinal.checked_add(1).ok_or(StreamError::Capacity)?,
                    ),
                    PositionOffset::Resolved => (
                        SessionSeq(
                            returned
                                .sequence
                                .0
                                .checked_add(1)
                                .ok_or(StreamError::Capacity)?,
                        ),
                        0,
                    ),
                }
            } else {
                (
                    SessionSeq(legacy.0.checked_add(1).ok_or(StreamError::Capacity)?),
                    0,
                )
            };
            while stream <= through {
                let Some(native) = self.native_sequence_of(stream) else {
                    return Err(StreamError::SourceViolation(
                        "stream sequence past the legacy prefix has no native record",
                    ));
                };
                match core.native_event(native, ordinal) {
                    Some(event) => {
                        let delta = focal_core::native::native_delta(self.ledger, stream, event);
                        let size = postcard::experimental::serialized_size(&delta)
                            .map_err(|_| StreamError::Codec)?;
                        if !admit(size)? {
                            return Ok(returned);
                        }
                        visit(&delta)?;
                        returned = Position::after_delta(delta.id);
                        ordinal = ordinal.checked_add(1).ok_or(StreamError::Capacity)?;
                    }
                    None => {
                        stream = SessionSeq(stream.0.checked_add(1).ok_or(StreamError::Capacity)?);
                        ordinal = 0;
                    }
                }
            }
        }
        Ok(Position::resolved(self.ledger, through))
    }
}
/// The stream sequence of native record `native` on a ledger whose sealed
/// legacy prefix is `legacy` (23 §6).
pub(crate) fn stream_line(
    activation: LedgerActivation,
    legacy: SessionSeq,
    native: SessionSeq,
) -> Result<SessionSeq, LedgerError> {
    match activation {
        LedgerActivation::Native {
            kind: ActivationKind::Genesis,
            ..
        } => Ok(native),
        LedgerActivation::Native {
            kind: ActivationKind::Imported,
            ..
        } => Ok(SessionSeq(
            legacy
                .0
                .checked_add(native.0.saturating_sub(1))
                .ok_or(LedgerError::Corrupt)?,
        )),
        LedgerActivation::V1 => Err(LedgerError::NativeUnsupported),
    }
}
