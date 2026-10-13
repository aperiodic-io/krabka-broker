use {super::*, assert2::check, proptest::prelude::*, std::iter};

/// Places `rf` replicas one per site in turn and reports what each site
/// holds. This is an independent implementation, and not the ceiling
/// formula again. Both oracles below read it.
fn round_robin_buckets(rf: i64, sites: i64) -> Vec<i64> {
    let site_count = usize::try_from(sites).expect("site count fits in usize");
    let mut buckets: Vec<i64> = iter::repeat_n(0, site_count).collect();
    let mut next = 0usize;
    let mut placed = 0i64;
    while placed < rf {
        buckets[next] += 1;
        next = (next + 1) % site_count;
        placed += 1;
    }
    buckets
}

/// What the site holding the most replicas holds.
fn largest_site_oracle(rf: i64, sites: i64) -> i64 {
    round_robin_buckets(rf, sites)
        .into_iter()
        .max()
        .expect("at least one site")
}

/// The count that remains after the loss of the site that holds the most
/// replicas.
fn round_robin_oracle(rf: i64, sites: i64) -> i64 { rf - largest_site_oracle(rf, sites) }

/// Sums the slice and checks every site with an iterator chain.
fn quorum_oracle(voters_per_site: &[i64]) -> bool {
    let total: i64 = voters_per_site.iter().sum();
    voters_per_site
        .iter()
        .copied()
        .all(|voters| 2 * (total - voters) > total)
}

mod survivors_match_round_robin_placement;
