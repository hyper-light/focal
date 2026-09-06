//! Bounded human documents. Builders return an expanded command; the caller must
//! persist it before sending. Context is a client hint checked again by ingress,
//! never a grant to impersonate an actor or manufacture server authority.
mod build;
mod decode;
mod documents;
mod vocabulary;
pub use decode::{InputFormat, MAX_INPUT_BYTES, parse_document};
pub use documents::*;
use focal_model::*;
pub use vocabulary::*;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InputError {
    #[error("authored input exceeds its bounded budget")]
    Capacity,
    #[error("invalid authored input: {0}")]
    Invalid(&'static str),
    #[error("cannot decode authored input: {0}")]
    Decode(String),
    #[error("identity generation failed")]
    Identity,
}

#[derive(Debug, Clone, Copy)]
pub struct BuildContext {
    pub ledger: LedgerId,
    pub actor: ParticipantId,
    pub root: RootCommandId,
    pub policy_revision: u64,
}
impl BuildContext {
    pub fn validate(&self) -> Result<(), InputError> {
        if self.ledger.tenant.is_zero()
            || self.ledger.session.is_zero()
            || self.actor.is_zero()
            || self.root.is_zero()
            || self.policy_revision == 0
        {
            return Err(InputError::Invalid("context identity or policy"));
        }
        Ok(())
    }
}

/// Inject an OS-random generator in the application, a deterministic fallible
/// generator in tests. Failed generation never sends or persists a mutation.
pub trait IdGenerator {
    fn next_id(&mut self) -> Result<[u8; 16], InputError>;
}
impl<F: FnMut() -> Result<[u8; 16], InputError>> IdGenerator for F {
    fn next_id(&mut self) -> Result<[u8; 16], InputError> {
        self()
    }
}

/// Exact 32-digit hexadecimal ID; uppercase is accepted and normalizes to the
/// same typed value. Whitespace, separators and zero are rejected.
pub fn parse_id(value: &str) -> Result<[u8; 16], InputError> {
    let bytes = parse_hex::<16>(value)?;
    if bytes.iter().all(|byte| *byte == 0) {
        return Err(InputError::Invalid("zero identity"));
    }
    Ok(bytes)
}
/// Exact 64-digit hexadecimal content/schema/version hash, never a free-form name.
pub fn parse_hash(value: &str) -> Result<ContentHash, InputError> {
    let bytes = parse_hex::<32>(value)?;
    if bytes.iter().all(|byte| *byte == 0) {
        return Err(InputError::Invalid("zero hash"));
    }
    Ok(ContentHash(bytes))
}
/// The only participant alias is the selected authenticated actor. It never
/// resolves an object ID, changes issuer authority, or bypasses self-work rules.
pub fn resolve_participant(
    value: &str,
    context: &BuildContext,
) -> Result<ParticipantId, InputError> {
    context.validate()?;
    if value == "self" {
        Ok(context.actor)
    } else {
        Ok(ParticipantId(parse_id(value)?))
    }
}
fn parse_hex<const N: usize>(value: &str) -> Result<[u8; N], InputError> {
    if N.checked_mul(2) != Some(value.len()) {
        return Err(InputError::Invalid("hexadecimal width"));
    }
    let mut result = [0; N];
    for (output, pair) in result.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let mut digits = pair.iter();
        let hi = digits
            .next()
            .copied()
            .and_then(hex_digit)
            .ok_or(InputError::Invalid("hexadecimal digit"))?;
        let lo = digits
            .next()
            .copied()
            .and_then(hex_digit)
            .ok_or(InputError::Invalid("hexadecimal digit"))?;
        *output = (hi << 4) | lo;
    }
    Ok(result)
}
fn hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => value.checked_sub(b'0'),
        b'a'..=b'f' => value.checked_sub(b'a')?.checked_add(10),
        b'A'..=b'F' => value.checked_sub(b'A')?.checked_add(10),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
