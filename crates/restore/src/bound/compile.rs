//! Turning the operator's bound flags into a compiled `Predicates` set, and
//! the one flag combination that is rejected outright.
//!
//! `Predicates::from_args` lives here because compiling the flags is a
//! separate concern from evaluating them: it groups `--to-offset` and
//! `--exclude-offset` per partition, and it proves, from the flags alone,
//! whether the exclude ranges swallow the whole keep window of a partition.
//! `fully_covers` is the interval arithmetic behind that proof.

use {
    super::Predicates,
    crate::{
        args::{PartitionRef, RestoreArgs},
        error::RestoreError,
    },
    krabka_ids::Offset,
    std::collections::HashMap,
};

impl Predicates {
    /// Compile the bound flags into a predicate set.
    ///
    /// # Errors
    ///
    /// Returns [`RestoreError::InvalidArgument`] when a partition's
    /// `--exclude-offset` ranges, merged, cover every offset from `0` through
    /// that partition's `--to-offset` bound: nothing between `0` and the
    /// bound can survive, so the bound keeps zero records for that partition.
    /// That is the one flag combination this function can prove empties a
    /// partition without reading the archive, because both windows are named
    /// entirely by the flags. Two related cases are deliberately not
    /// flagged here. A negative `--to-offset`, which would exclude a whole
    /// partition outright, is already rejected by [`RestoreArgs`]'s own
    /// parser and never reaches this function. A `--exclude-key` or
    /// `--exclude-header` pattern that matches every possible byte string,
    /// such as an empty pattern or `.*`, is not detected either: proving a
    /// regex is universal is not a check this function attempts, a partial
    /// heuristic would catch some universal patterns and not others, and
    /// unlike an offset window, a key predicate never empties a partition on
    /// its own anyway, because a keyless record still survives it.
    pub fn from_args(args: &RestoreArgs) -> Result<Self, RestoreError> {
        let mut to_offset = HashMap::with_capacity(args.to_offset.len());
        for bound in &args.to_offset {
            to_offset.insert(bound.partition.clone(), bound.last_offset);
        }

        let mut exclude_offset: HashMap<PartitionRef, Vec<(Offset, Offset)>> = HashMap::new();
        for range in &args.exclude_offset {
            exclude_offset
                .entry(range.partition.clone())
                .or_default()
                .push((range.start, range.end_exclusive));
        }

        for (partition, &last_offset) in &to_offset {
            let fully_excluded = exclude_offset
                .get(partition)
                .is_some_and(|ranges| fully_covers(ranges, last_offset));
            if fully_excluded {
                return Err(RestoreError::InvalidArgument(format!(
                    "--exclude-offset excludes every offset that --to-offset keeps in {partition}"
                )));
            }
        }

        Ok(Self {
            to_offset,
            to_timestamp: args.to_timestamp,
            exclude_key: args.exclude_key.clone(),
            exclude_header: args.exclude_header.clone(),
            exclude_producer_id: args.exclude_producer_id.iter().copied().collect(),
            exclude_offset,
        })
    }
}

/// Whether `ranges`, merged, cover every offset in `0..=last_offset`.
///
/// This is the arithmetic behind the "can never keep a record" check in
/// [`Predicates::from_args`]: a partition's whole possible keep window, from
/// offset zero through its `--to-offset` bound, counts as covered only when
/// the exclude ranges leave no gap in it.
fn fully_covers(ranges: &[(Offset, Offset)], last_offset: Offset) -> bool {
    let mut sorted = ranges.to_vec();
    sorted.sort_unstable_by_key(|&(start, _)| start);

    let mut covered_through = Offset::ZERO;
    for (start, end_exclusive) in sorted {
        if start > covered_through {
            return false;
        }
        if end_exclusive > covered_through {
            covered_through = end_exclusive;
        }
        if covered_through > last_offset {
            return true;
        }
    }
    covered_through > last_offset
}

#[cfg(test)]
mod tests {
    use {super::*, crate::bound::test_support::args_from, assert2::check};

    #[test]
    fn exclude_offset_fully_covering_the_to_offset_window_is_rejected() {
        let result = Predicates::from_args(&args_from(&[
            "--to-offset",
            "orders:0=5",
            "--exclude-offset",
            "orders:0=0..6",
        ]));

        check!(matches!(result, Err(RestoreError::InvalidArgument(_))));
    }

    #[test]
    fn exclude_offset_covering_the_window_through_several_ranges_is_rejected() {
        let result = Predicates::from_args(&args_from(&[
            "--to-offset",
            "orders:0=5",
            "--exclude-offset",
            "orders:0=0..3",
            "--exclude-offset",
            "orders:0=3..6",
        ]));

        check!(matches!(result, Err(RestoreError::InvalidArgument(_))));
    }

    #[test]
    fn exclude_offset_leaving_any_gap_in_the_window_is_accepted() {
        let result = Predicates::from_args(&args_from(&[
            "--to-offset",
            "orders:0=5",
            "--exclude-offset",
            "orders:0=0..3",
        ]));

        check!(result.is_ok());
    }

    #[test]
    fn exclude_offset_covering_an_unrelated_partition_does_not_reject() {
        let result = Predicates::from_args(&args_from(&[
            "--to-offset",
            "orders:0=5",
            "--exclude-offset",
            "orders:1=0..100",
        ]));

        check!(result.is_ok());
    }

    #[test]
    fn no_to_offset_bound_means_no_coverage_check_at_all() {
        let result =
            Predicates::from_args(&args_from(&["--exclude-offset", "orders:0=0..1000000"]));

        check!(result.is_ok());
    }
}
