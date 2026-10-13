//! The retention window over the cuts one group keeps.
//!
//! Publishing a cut retires the epochs that fall out of the window, and the
//! coordinator tombstones exactly what this function names. The rule is one
//! pure comparison over the held epochs, so it carries its table of cases in
//! its own file.

/// The epochs that leave the retention window when `published` is published.
///
/// The group keeps its last `retained_cuts` cuts, so every held epoch at or
/// below `published - retained_cuts` falls off. `held` is the set of epochs
/// the group still carries, and it does not hold `published` yet. A group edit
/// that reduced `retained_cuts` drops more than one epoch at once. A
/// `retained_cuts` that is not positive drops nothing, and
/// [`crate::barrier::coordinator::validate_spec`] rejects such a value.
pub(crate) fn expired_cut_epochs(published: i64, retained_cuts: i32, held: &[i64]) -> Vec<i64> {
    held.iter()
        .copied()
        .filter(|epoch| krabka_verified::barrier_cut_expired(published, retained_cuts, *epoch))
        .collect()
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    struct RetentionCase {
        name: &'static str,
        published: i64,
        retained_cuts: i32,
        held: &'static [i64],
        expired: &'static [i64],
    }

    #[test]
    fn the_retention_window_drops_the_epochs_below_it() {
        const ALL: &[i64] = &[1, 2, 3, 4, 5, 6, 7, 8];
        let cases = [
            RetentionCase {
                name: "nothing held yet",
                published: 1,
                retained_cuts: 1,
                held: &[],
                expired: &[],
            },
            RetentionCase {
                name: "one cut retained",
                published: 2,
                retained_cuts: 1,
                held: &[1],
                expired: &[1],
            },
            RetentionCase {
                name: "three cuts retained",
                published: 4,
                retained_cuts: 3,
                held: &[1, 2, 3],
                expired: &[1],
            },
            RetentionCase {
                name: "window not full",
                published: 3,
                retained_cuts: 3,
                held: &[1, 2],
                expired: &[],
            },
            RetentionCase {
                name: "default window",
                published: 10,
                retained_cuts: 32,
                held: ALL,
                expired: &[],
            },
            RetentionCase {
                name: "a reduced window drops several",
                published: 9,
                retained_cuts: 2,
                held: ALL,
                expired: &[1, 2, 3, 4, 5, 6, 7],
            },
            RetentionCase {
                name: "no retention drops nothing",
                published: 9,
                retained_cuts: 0,
                held: ALL,
                expired: &[],
            },
            RetentionCase {
                name: "negative retention drops nothing",
                published: 9,
                retained_cuts: -1,
                held: ALL,
                expired: &[],
            },
            RetentionCase {
                name: "cutoff underflow drops nothing",
                published: i64::MIN,
                retained_cuts: 1,
                held: &[i64::MIN],
                expired: &[],
            },
            RetentionCase {
                name: "exact minimum cutoff",
                published: i64::MIN + 1,
                retained_cuts: 1,
                held: &[i64::MIN, i64::MIN + 1],
                expired: &[i64::MIN],
            },
        ];
        for case in cases {
            check!(
                expired_cut_epochs(case.published, case.retained_cuts, case.held) == case.expired,
                "{}",
                case.name
            );
        }
    }
}
