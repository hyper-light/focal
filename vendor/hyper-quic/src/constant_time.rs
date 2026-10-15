// This function is non-inline to prevent the optimizer from looking inside it.
#[inline(never)]
fn constant_time_ne(a: &[u8], b: &[u8]) -> u8 {
    // The caller compares lengths first (`eq`); zipping needs no bounds checks.
    let mut tmp = 0;
    for (x, y) in a.iter().zip(b) {
        tmp |= x ^ y;
    }
    tmp // The compare with 0 must happen outside this function.
}

/// Compares byte strings in constant time.
pub(crate) fn eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && constant_time_ne(a, b) == 0
}
