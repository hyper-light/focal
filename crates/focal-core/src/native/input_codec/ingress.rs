//! One bounded path from a complete borrowed actor frame to managed admission.
//! Receive-buffer funding and authenticated transport remain outside this codec.
use super::*;
use focal_model::lifecycle::{
    aggregation, artifact_descriptor, claim_descriptor, validation_descriptor,
};

#[cfg(test)]
#[path = "ingress_tests.rs"]
mod tests;

/// Separate cumulative work domains include initial inspection, preparation and
/// final construction. Raw artifact source callbacks also spend this source
/// allowance during the owner's borrowed authority checks. These are internal
/// node limits, not participant-selected execution or policy authority.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DecodeWork {
    pub parse: usize,
    pub source: usize,
    pub model: usize,
    pub acceptance: usize,
    pub native: usize,
}
impl DecodeWork {
    pub fn total(self) -> Result<usize, DecodeError> {
        [
            self.parse,
            self.source,
            self.model,
            self.acceptance,
            self.native,
        ]
        .into_iter()
        .try_fold(0usize, |sum, value| {
            sum.checked_add(value).ok_or(CodecError::Capacity.into())
        })
    }
    pub(super) fn subtract(self, used: Self) -> Result<Self, DecodeError> {
        fn sub(a: usize, b: usize) -> Result<usize, DecodeError> {
            a.checked_sub(b).ok_or(CodecError::Capacity.into())
        }
        Ok(Self {
            parse: sub(self.parse, used.parse)?,
            source: sub(self.source, used.source)?,
            model: sub(self.model, used.model)?,
            acceptance: sub(self.acceptance, used.acceptance)?,
            native: sub(self.native, used.native)?,
        })
    }
    pub(super) fn covers(self, used: Self) -> Result<(), DecodeError> {
        self.subtract(used).map(|_| ())
    }
    fn authored(self) -> AuthoredCreationWork {
        AuthoredCreationWork {
            parse: self.parse,
            source: self.source,
            descriptor: self.model,
            acceptance: self.acceptance,
            native: self.native,
        }
    }
    fn projection(self) -> LegacyCreationWork {
        LegacyCreationWork {
            parsing: self.parse,
            source: self.source,
            declarations: self.model,
            acceptance: self.acceptance,
            structure: self.native,
        }
    }
}
impl From<AuthoredCreationWork> for DecodeWork {
    fn from(work: AuthoredCreationWork) -> Self {
        Self {
            parse: work.parse,
            source: work.source,
            model: work.descriptor,
            acceptance: work.acceptance,
            native: work.native,
        }
    }
}
impl From<LegacyCreationWork> for DecodeWork {
    fn from(work: LegacyCreationWork) -> Self {
        Self {
            parse: work.parsing,
            source: work.source,
            model: work.declarations,
            acceptance: work.acceptance,
            native: work.structure,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeQuote {
    pub inspection: InspectionQuote,
    /// Includes the initial structural scan as well as typed preparation.
    pub preparation: DecodeWork,
    pub construction: DecodeWork,
    pub input_bytes: usize,
}

/// Semantic dimensions can be made stricter by an embedding. `for_native`
/// derives a baseline from existing owner and frame limits without introducing
/// mandatory per-command configuration in the user interface.
#[derive(Debug, Clone, Copy)]
pub struct NativeDecodeLimits {
    pub frame_bytes: usize,
    pub items: usize,
    pub text_bytes: usize,
    pub blob_bytes: usize,
    pub artifact: artifact_descriptor::Limits,
    pub claim: claim_descriptor::Limits,
    pub validation: validation_descriptor::Limits,
    pub acceptance: aggregation::Limits,
    pub work: DecodeWork,
}
impl NativeDecodeLimits {
    pub fn for_native(
        native: NativeLimits,
        frame_bytes: usize,
        work: DecodeWork,
    ) -> Result<Self, DecodeError> {
        work.total()?;
        let edges = native.plan_edges;
        let attempts = u32::try_from(edges).map_err(|_| CodecError::Capacity)?;
        let declaration = validation::Limits {
            handlers: attempts,
            attempts,
            slot_bytes: frame_bytes,
        };
        Ok(Self {
            frame_bytes,
            items: frame_bytes,
            text_bytes: frame_bytes,
            blob_bytes: frame_bytes,
            artifact: artifact_descriptor::Limits {
                kind_bytes: frame_bytes,
                metadata_bytes: frame_bytes,
                inline_bytes: frame_bytes,
                inputs: edges,
                visibility_labels: edges,
                visibility_label_bytes: frame_bytes,
                construction_bytes: native.preparation_bytes,
            },
            claim: claim_descriptor::Limits {
                description_bytes: frame_bytes,
                relations: edges,
                scopes: edges,
                scope_key_bytes: frame_bytes,
                requirements: edges,
                slots: edges,
                checks: edges,
                construction_bytes: native.preparation_bytes,
            },
            validation: validation_descriptor::Limits {
                declaration,
                description_bytes: frame_bytes,
                quality_bar_bytes: frame_bytes,
                contributors: edges,
                construction_bytes: native.preparation_bytes,
            },
            acceptance: aggregation::Limits {
                max_slots: edges,
                max_checks: edges,
                max_results: native.results,
                max_updates: edges,
            },
            work,
        })
    }

    /// The callback runs while any local artifact source is still borrowed.
    /// It cannot return a plan that borrows that local source. No typed scratch
    /// arrays or final input buffers are allocated by this dispatch function.
    /// Final construction limits travel inside the opaque request and are checked
    /// only for a fresh request, preserving exact retries with no build headroom.
    pub fn with_request<'a, T>(
        self,
        native: NativeLimits,
        bytes: &'a [u8],
        use_request: impl for<'s> FnOnce(DecodedRequest<'s, 'a>, DecodeQuote) -> T,
    ) -> Result<T, DecodeError> {
        self.with_header_check(native, bytes, |_| Ok(()), use_request)
    }

    pub(in crate::native) fn with_header_check<'a, T>(
        self,
        native: NativeLimits,
        bytes: &'a [u8],
        check_header: impl FnOnce(InputHeader) -> Result<(), DecodeError>,
        use_request: impl for<'s> FnOnce(DecodedRequest<'s, 'a>, DecodeQuote) -> T,
    ) -> Result<T, DecodeError> {
        self.work.total()?;
        let frame = StructuralInput::inspect_with_header(
            bytes,
            InspectionLimits {
                bytes: self.frame_bytes,
                visits: self.work.parse,
                items: self.items,
                text_bytes: self.text_bytes,
                blob_bytes: self.blob_bytes,
            },
            check_header,
        )?;
        let inspection = frame.quote();
        let mut remaining = self.work.subtract(DecodeWork {
            parse: inspection.visits,
            ..DecodeWork::default()
        })?;
        let FrameKind::Request { command } = frame.header().kind else {
            return Err(ContractError::WrongActor.into());
        };
        match command {
            4 | 6 | 7 | 13 | 15 | 19 => {
                let mut body = frame
                    .artifact_input(remaining.parse)?
                    .ok_or(CodecError::InvalidTag("artifact command"))?;
                remaining = remaining.subtract(DecodeWork {
                    parse: body.parse_visits(),
                    ..DecodeWork::default()
                })?;
                let plan = body.prepare_for_frame(
                    native,
                    self.artifact,
                    remaining.model,
                    remaining.source,
                    remaining.native,
                )?;
                let quote = plan.quote();
                remaining = remaining.subtract(DecodeWork {
                    source: quote.source_inspection_visits,
                    model: quote.model_inspection_visits,
                    native: quote.native_inspection_visits,
                    ..DecodeWork::default()
                })?;
                self.finish(plan.into(), inspection, remaining, use_request)
            }
            9 => {
                let plan = frame
                    .prepare_response(native, remaining.native)?
                    .ok_or(CodecError::InvalidTag("response command"))?;
                remaining = remaining.subtract(DecodeWork {
                    native: plan.quote().prepare_visits,
                    ..DecodeWork::default()
                })?;
                self.finish(plan.into(), inspection, remaining, use_request)
            }
            24 => {
                let plan = frame
                    .prepare_monitor(native, remaining.native)?
                    .ok_or(CodecError::InvalidTag("monitor command"))?;
                remaining = remaining.subtract(DecodeWork {
                    native: plan.quote().prepare_visits,
                    ..DecodeWork::default()
                })?;
                self.finish(plan.into(), inspection, remaining, use_request)
            }
            0 => {
                let plan = frame
                    .prepare_legacy_creation(
                        native,
                        LegacyCreationLimits {
                            declaration: self.validation.declaration,
                            acceptance: self.acceptance,
                            bytes: native.preparation_bytes,
                            work: remaining.projection(),
                        },
                    )?
                    .ok_or(CodecError::InvalidTag("projection creation"))?;
                remaining = remaining.subtract(plan.quote().preparation.into())?;
                self.finish(plan.into(), inspection, remaining, use_request)
            }
            27 => {
                let plan = frame
                    .prepare_authored_creation(
                        native,
                        AuthoredCreationLimits {
                            claim: self.claim,
                            validation: self.validation,
                            acceptance: self.acceptance,
                            work: remaining.authored(),
                        },
                    )?
                    .ok_or(CodecError::InvalidTag("authored creation"))?;
                remaining = remaining.subtract(plan.quote().preparation.into())?;
                self.finish(plan.into(), inspection, remaining, use_request)
            }
            _ => {
                let plan = frame
                    .decode_fixed(remaining.parse)?
                    .ok_or(CodecError::InvalidTag("fixed command"))?;
                // Both fixed passes read exactly the same primitive boundaries.
                // Price bounded native request hashing before conversion does it.
                remaining = remaining.subtract(DecodeWork {
                    parse: inspection.visits,
                    native: 4096,
                    ..DecodeWork::default()
                })?;
                self.finish(plan.try_into()?, inspection, remaining, use_request)
            }
        }
    }

    fn finish<'s, 'a, T>(
        self,
        request: DecodedRequest<'s, 'a>,
        inspection: InspectionQuote,
        remaining: DecodeWork,
        use_request: impl FnOnce(DecodedRequest<'s, 'a>, DecodeQuote) -> T,
    ) -> Result<T, DecodeError> {
        let quote = DecodeQuote {
            inspection,
            preparation: self.work.subtract(remaining)?,
            construction: request.construction_work(),
            input_bytes: request.construction_bytes(),
        };
        Ok(use_request(request.with_remaining_work(remaining), quote))
    }
}
