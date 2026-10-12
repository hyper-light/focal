use super::*;

const MEMORY: &str = "refused: NativeRefusal { kind: Capacity, detail: \"native owner memory: Capacity { requested: 2728, available: 2426 }\" }";
const MEMORY_AGAIN: &str = "refused: NativeRefusal { kind: Capacity, detail: \"native owner memory: Capacity { requested: 360, available: 140 }\" }";

/// Refusals that name their own figures are one reason: a full session's
/// refusals each named their request and its room, and every one took a
/// place of its own until the places ran out.
#[test]
fn refusals_of_one_kind_are_counted_together_whatever_their_figures() {
    let mut refusals = Vec::new();
    note(&mut refusals, MEMORY.into());
    note(&mut refusals, MEMORY_AGAIN.into());
    note(&mut refusals, "access: Capacity".into());
    assert_eq!(
        refusals,
        vec![
            (
                "refused: NativeRefusal { kind: Capacity, detail: \"native owner memory: Capacity { requested: #, available: # }\" }".to_owned(),
                2
            ),
            ("access: Capacity".to_owned(), 1),
        ]
    );
}

/// Past the kept reasons the rest are counted under one, so the reasons
/// always sum to the refusals they name: the comparison's reports at 1,000/s
/// named 5,300 of 24,983.
#[test]
fn reasons_past_the_kept_ones_are_counted_never_dropped() {
    let mut refusals = Vec::new();
    let mut noted = 0u64;
    for kind in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"] {
        for _ in 0..3 {
            note(&mut refusals, format!("reason {kind}"));
            noted += 1;
        }
    }
    assert_eq!(refusals.len(), MAX_REFUSALS);
    assert_eq!(
        refusals.last().map(|(reason, _)| reason.as_str()),
        Some(OTHER_REASONS)
    );
    assert_eq!(refusals.iter().map(|(_, count)| count).sum::<u64>(), noted);
    // Workers' reasons merged into the run's, past its places as well.
    let mut run = Vec::new();
    for (reason, count) in &refusals {
        note_count(&mut run, reason.clone(), *count);
    }
    for (reason, count) in &refusals {
        note_count(&mut run, reason.clone(), *count);
    }
    assert_eq!(run.len(), MAX_REFUSALS);
    assert_eq!(run.iter().map(|(_, count)| count).sum::<u64>(), 2 * noted);
}
