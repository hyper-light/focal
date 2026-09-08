//! Complete command plans consumed by the exclusive owner. Conversion keeps
//! variable content borrowed; caller-supplied identity summaries are excluded.
use super::*;
use crate::native::prepare::{add, within};

#[derive(Debug)]
pub struct DecodedRequest<'s, 'a> {
    pub(in crate::native) plan: Plan<'s, 'a>,
    remaining_work: Option<DecodeWork>,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // No allocation before owner admission.
pub(in crate::native) enum Plan<'s, 'a> {
    Fixed {
        header: InputHeader,
        input: NativeInput,
        intent: ContentHash,
    },
    Artifact(ArtifactFramePlan<'s, 'a>),
    Response(ResponseFramePlan<'a>),
    Monitor(MonitorFramePlan<'a>),
    Projection(LegacyCreationPlan<'a>),
    Authored(AuthoredFramePlan<'a>),
}

impl<'s, 'a> From<ArtifactFramePlan<'s, 'a>> for DecodedRequest<'s, 'a> {
    fn from(plan: ArtifactFramePlan<'s, 'a>) -> Self {
        Self {
            plan: Plan::Artifact(plan),
            remaining_work: None,
        }
    }
}
impl<'a> From<ResponseFramePlan<'a>> for DecodedRequest<'_, 'a> {
    fn from(plan: ResponseFramePlan<'a>) -> Self {
        Self {
            plan: Plan::Response(plan),
            remaining_work: None,
        }
    }
}
impl<'a> From<MonitorFramePlan<'a>> for DecodedRequest<'_, 'a> {
    fn from(plan: MonitorFramePlan<'a>) -> Self {
        Self {
            plan: Plan::Monitor(plan),
            remaining_work: None,
        }
    }
}
impl<'a> From<LegacyCreationPlan<'a>> for DecodedRequest<'_, 'a> {
    fn from(plan: LegacyCreationPlan<'a>) -> Self {
        Self {
            plan: Plan::Projection(plan),
            remaining_work: None,
        }
    }
}
impl<'a> From<AuthoredFramePlan<'a>> for DecodedRequest<'_, 'a> {
    fn from(plan: AuthoredFramePlan<'a>) -> Self {
        Self {
            plan: Plan::Authored(plan),
            remaining_work: None,
        }
    }
}
impl TryFrom<FixedFrame> for DecodedRequest<'_, '_> {
    type Error = DecodeError;
    fn try_from(frame: FixedFrame) -> Result<Self, Self::Error> {
        let FixedFrame::Request {
            ledger,
            profile,
            input,
        } = frame
        else {
            return Err(ContractError::WrongActor.into());
        };
        let tag = super::encode::tag(&input.command);
        if matches!(tag, 0 | 4 | 6 | 7 | 9 | 13 | 15 | 19 | 24 | 27) {
            return Err(ContractError::InvalidPolicy.into());
        }
        let intent = crate::native::intent::fingerprint(ledger, &input)?;
        Ok(Self {
            plan: Plan::Fixed {
                header: InputHeader {
                    ledger,
                    profile,
                    kind: FrameKind::Request { command: tag },
                    request: Some(input.request),
                },
                input,
                intent,
            },
            remaining_work: None,
        })
    }
}

impl DecodedRequest<'_, '_> {
    pub(super) fn with_remaining_work(mut self, remaining: DecodeWork) -> Self {
        self.remaining_work = Some(remaining);
        self
    }

    pub fn construction_work(&self) -> DecodeWork {
        match &self.plan {
            Plan::Fixed { .. } => DecodeWork::default(),
            Plan::Artifact(plan) => {
                let q = plan.quote();
                DecodeWork {
                    source: q.source_build_visits,
                    model: q.model_build_visits,
                    native: q.native_build_visits,
                    ..DecodeWork::default()
                }
            }
            Plan::Response(plan) => DecodeWork {
                native: plan.quote().build_visits,
                ..DecodeWork::default()
            },
            Plan::Monitor(plan) => DecodeWork {
                native: plan.quote().build_visits,
                ..DecodeWork::default()
            },
            Plan::Projection(plan) => plan.quote().construction.into(),
            Plan::Authored(plan) => plan.quote().construction.into(),
        }
    }
    pub fn header(&self) -> InputHeader {
        match &self.plan {
            Plan::Fixed { header, .. } => *header,
            Plan::Artifact(plan) => plan.header(),
            Plan::Response(plan) => plan.header(),
            Plan::Monitor(plan) => plan.header(),
            Plan::Projection(plan) => plan.header(),
            Plan::Authored(plan) => plan.header(),
        }
    }
    pub fn intent_fingerprint(&self) -> ContentHash {
        match &self.plan {
            Plan::Fixed { intent, .. } => *intent,
            Plan::Artifact(plan) => plan.intent(),
            Plan::Response(plan) => plan.intent(),
            Plan::Monitor(plan) => plan.intent(),
            Plan::Projection(plan) => plan.intent_fingerprint(),
            Plan::Authored(plan) => plan.intent_fingerprint(),
        }
    }
    /// Required construction allowance. Initial parsing/model preparation and
    /// effective-state authorization have their own bounded checks.
    pub fn construction_visits(&self) -> Result<usize, NativeError> {
        let work = self.construction_work();
        [
            work.parse,
            work.source,
            work.model,
            work.acceptance,
            work.native,
        ]
        .into_iter()
        .try_fold(0, add)
    }
    pub fn construction_bytes(&self) -> usize {
        match &self.plan {
            Plan::Fixed { .. } => 0,
            Plan::Artifact(plan) => plan.quote().bytes,
            Plan::Response(plan) => plan.quote().bytes,
            Plan::Monitor(plan) => plan.quote().bytes,
            Plan::Projection(plan) => plan.quote().bytes,
            Plan::Authored(plan) => plan.quote().bytes,
        }
    }
    pub(in crate::native) fn check_build(
        &self,
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<(), DecodeError> {
        within(self.construction_bytes(), max_bytes)?;
        within(self.construction_visits()?, max_visits)?;
        if let Some(remaining) = self.remaining_work {
            remaining.covers(self.construction_work())?;
        }
        if let Plan::Artifact(plan) = &self.plan {
            plan.check_build_capacity()?;
        }
        Ok(())
    }
    pub(in crate::native) fn build(self) -> Result<NativeInput, DecodeError> {
        self.check_build(self.construction_bytes(), self.construction_visits()?)?;
        match self.plan {
            Plan::Fixed { input, .. } => Ok(input),
            Plan::Artifact(plan) => {
                let q = plan.quote();
                plan.build(q.bytes, q.model_build_visits, q.native_build_visits)
            }
            Plan::Response(plan) => {
                let q = plan.quote();
                plan.build(q.bytes, q.build_visits)
            }
            Plan::Monitor(plan) => {
                let q = plan.quote();
                plan.build(q.bytes, q.build_visits)
            }
            Plan::Projection(plan) => {
                let q = plan.quote();
                plan.build(q.bytes, q.construction)
            }
            Plan::Authored(plan) => {
                let q = plan.quote();
                plan.build(q.bytes, q.construction)
            }
        }
    }
}
