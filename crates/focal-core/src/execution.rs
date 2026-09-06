//! Select domain rules from a prepared entry's contract. This is local dispatch,
//! not a persisted profile field or a process-wide default. A successor must own
//! distinct rules; it cannot change what an existing schema-one entry executes.
use crate::*;
use focal_model::durable_v1::Ref;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Version {
    V1,
}

type Output<'a> = (
    access::WriteState<'a>,
    CommandResult,
    Vec<Delta>,
    Vec<EffectIntent>,
);

impl Version {
    pub(crate) fn from_schema(schema: u16) -> Result<Self, CoreError> {
        match schema {
            semantics_v1::SCHEMA => Ok(Self::V1),
            _ => Err(CoreError::UnsupportedSchema(schema)),
        }
    }

    pub(crate) const fn schema(self) -> u16 {
        match self {
            Self::V1 => semantics_v1::SCHEMA,
        }
    }

    pub(crate) fn claim_id(self, command: &Command) -> Option<ClaimId> {
        match self {
            Self::V1 => semantics_v1::claim_id(command),
        }
    }

    pub(crate) fn managed_key_valid(self, key: &ManagedRequestKey) -> bool {
        match self {
            Self::V1 => semantics_v1::request_key_valid(key),
        }
    }

    pub(crate) fn legacy_input_size(
        self,
        input: &AuthenticatedInput,
    ) -> Result<usize, postcard::Error> {
        match self {
            Self::V1 => postcard::experimental::serialized_size(&Ref(input)),
        }
    }

    pub(crate) fn managed_input_size(
        self,
        input: &ManagedAuthenticatedInput,
    ) -> Result<usize, postcard::Error> {
        match self {
            Self::V1 => postcard::experimental::serialized_size(&Ref(input)),
        }
    }

    pub(crate) fn legacy_hash(
        self,
        input: &AuthenticatedInput,
    ) -> Result<ContentHash, CanonicalError> {
        match self {
            Self::V1 => command_hash(input),
        }
    }

    pub(crate) fn managed_hash(
        self,
        input: &ManagedAuthenticatedInput,
    ) -> Result<ContentHash, CanonicalError> {
        match self {
            Self::V1 => managed_command_hash(input),
        }
    }

    pub(crate) fn execute_on<'a>(
        self,
        state: access::WriteState<'a>,
        limits: &'a Limits,
        input: &'a AuthenticatedInput,
        sequence: SessionSeq,
    ) -> Result<Output<'a>, DomainOutcome> {
        match self {
            Self::V1 => execution_v1::execute_on(state, limits, input, sequence),
        }
    }

    pub(crate) fn execute_managed_on<'a>(
        self,
        state: access::WriteState<'a>,
        limits: &'a Limits,
        input: &'a ManagedAuthenticatedInput,
        sequence: SessionSeq,
    ) -> Result<Output<'a>, DomainOutcome> {
        match self {
            Self::V1 => execution_v1::execute_managed_on(state, limits, input, sequence),
        }
    }
}
