//! When the scheduler injects a group next.
//!
//! The scheduler reads one due time per group and never a clock of its own, so
//! the two functions that set and read that due time are pure and sit in their
//! own file.

use {super::GroupEntry, krabka_units::convert::TimeExt as _};

/// Set when the scheduler should inject next.
///
/// A group with no interval injects only on demand, so it gets no due time.
pub(crate) fn schedule_next(entry: &mut GroupEntry, now_ms: i64) {
    entry.next_due_ms = entry
        .definition
        .interval
        .map(|interval| now_ms.saturating_add(interval.millis_i64().max(0)));
}

/// Whether the scheduler should inject this group now.
pub(crate) fn is_due(entry: &GroupEntry, now_ms: i64) -> bool {
    entry.next_due_ms.is_some_and(|due| now_ms >= due)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::barrier::state::GroupSpec,
        assert2::{assert, check},
        krabka_units::millis,
    };

    #[test]
    fn a_group_with_an_interval_gets_a_due_time() {
        let mut entry = GroupEntry::from_spec(
            GroupSpec {
                topics: vec!["orders".to_owned()],
                interval: Some(millis(5_000)),
                retained_cuts: 4,
            },
            0,
        );
        assert!(!is_due(&entry, 1_000));

        schedule_next(&mut entry, 1_000);
        assert!(entry.next_due_ms == Some(6_000));
        check!(!is_due(&entry, 5_999));
        check!(is_due(&entry, 6_000));
        check!(is_due(&entry, 9_999));
    }

    #[test]
    fn a_group_with_no_interval_is_never_due() {
        let mut entry = GroupEntry::from_spec(
            GroupSpec {
                topics: vec!["orders".to_owned()],
                interval: None,
                retained_cuts: 4,
            },
            0,
        );
        schedule_next(&mut entry, 1_000);
        assert!(entry.next_due_ms.is_none());
        assert!(!is_due(&entry, i64::MAX));
    }
}
