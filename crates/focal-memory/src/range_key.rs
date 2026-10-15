//! The key a range store orders its rows by, and the integer prefix of that
//! order a store caches where it searches.

/// A key a range store holds. Its order may begin with a 128-bit prefix the
/// store caches beside each page and directory node: a search computes the
/// sought key's prefix once and compares integers, comparing whole keys only
/// where two prefixes tie. The prefix must agree with the order: a key whose
/// prefix is less than another's is less than it. The default prefix is the
/// same for every key, so a key that names none is compared whole, as before.
pub trait RangeKey: Ord {
    fn order_prefix(&self) -> u128 {
        0
    }
    /// The order of this key and `other`, whose prefixes are equal: what is
    /// left of the order past the prefix. A key whose prefix is a field of its
    /// order compares the rest here without reading that field again (a search
    /// compares whole keys only where prefixes tie, and there they tie by
    /// construction). It must agree with `cmp` wherever the prefixes are equal;
    /// the default is `cmp` itself.
    fn cmp_past_prefix(&self, other: &Self) -> std::cmp::Ordering {
        self.cmp(other)
    }
}

macro_rules! whole {
    ($($t:ty),*) => { $(impl RangeKey for $t {})* };
}
whole!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, char, bool, String
);
impl<T: Ord> RangeKey for Vec<T> {}
impl<A: Ord, B: Ord> RangeKey for (A, B) {}
impl<A: Ord, B: Ord, C: Ord> RangeKey for (A, B, C) {}
impl<T: Ord, const N: usize> RangeKey for [T; N] {}
