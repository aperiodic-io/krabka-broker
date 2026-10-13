use {super::retention_delete_target, creusot_std::prelude::*};

/// Extend a deletion floor through one completed whole-range delete.
/// A gap, malformed coordinates or an unrepresentable successor closes the
/// contiguous run permanently. Completion and truthful metadata are host facts.
#[ensures(current@ <= result.0@)]
#[ensures(result.1 == (contiguous && current@ >= 0 && start@ >= 0
    && start@ <= end@ && start@ <= current@ && end@ < i64::MAX@))]
#[ensures(if result.1 { result.0@ == current@.max(end@ + 1) } else { result.0 == current })]
#[ensures(forall<offset: Int> current@ <= offset && offset < result.0@
    ==> start@ <= offset && offset <= end@)]
#[must_use]
pub fn remote_retention_floor_step(
    current: i64,
    contiguous: bool,
    start: i64,
    end: i64,
) -> (i64, bool) {
    if !contiguous || current < 0 || start < 0 || start > end || start > current {
        return (current, false);
    }
    match retention_delete_target(Some(end)) {
        Some(target) => (current.max(target), true),
        None => (current, false),
    }
}
