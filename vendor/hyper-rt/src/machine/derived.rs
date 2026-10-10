//! A value with its provenance.
//!
//! R3 says every parameter is measured from the machine or the data and derived by a stated
//! algorithm. In code that means a tunable is never a bare number: it is a [`Derived`] carrying the
//! formula that produced it and the names of the measured anchors it used, so the boot log can
//! print `value = formula(anchors)` for every constant and a reader can check the arithmetic.
//! The `derived!` macro is the one form the literal check (`cargo xtask literals`) accepts for a
//! numeric literal in shipped code.

/// A value together with the formula and anchors that produced it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Derived<T> {
    /// The derived value.
    pub value: T,
    /// The formula, in plain English or arithmetic, exactly as the design states it.
    pub formula: &'static str,
    /// The measured quantities the formula consumed, by their profile field names.
    pub anchors: &'static [&'static str],
}

impl<T> Derived<T> {
    /// Builds a derived value; prefer the `derived!` macro at call sites.
    pub const fn new(value: T, formula: &'static str, anchors: &'static [&'static str]) -> Self {
        Self {
            value,
            formula,
            anchors,
        }
    }
}

impl<T: Copy> Derived<T> {
    /// The value alone, for arithmetic.
    pub const fn get(&self) -> T {
        self.value
    }
}

/// Marks a value as derived: `derived!(expr, "formula", ["anchor", ...])`.
///
/// The expression may contain numeric literals; the literal check accepts them because the
/// derivation is written next to them. Use it for every constant that would otherwise be a bare
/// number, including shape constants ratified in the gap ledger (then the anchor names the ledger
/// entry).
#[macro_export]
macro_rules! derived {
  ($value:expr, $formula:literal, [$($anchor:literal),* $(,)?]) => {
    $crate::machine::Derived::new($value, $formula, &[$($anchor),*])
  };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_derived_value_carries_its_formula_and_anchors() {
        let d: Derived<u64> = derived!(4096 * 4, "page size × 4", ["page.base"]);
        assert_eq!(d.get(), 16384);
        assert_eq!(d.formula, "page size × 4");
        assert_eq!(d.anchors, &["page.base"]);
    }
}
