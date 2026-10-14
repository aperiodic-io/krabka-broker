use super::*;

proptest! {
    #[test]
    fn survivors_match_round_robin_placement(rf in 1i64..64, sites in 1i64..8) {
        prop_assert_eq!(site_loss_survivors(rf, sites), round_robin_oracle(rf, sites));
    }

    #[test]
    fn safe_min_insync_stays_inside_the_surviving_replicas(
        rf in 1i64..64,
        sites in 1i64..8,
        min_insync in 0i64..64,
    ) {
        let buckets = round_robin_buckets(rf, sites);
        let survivors = rf - buckets.iter().copied().max().expect("a site");
        // Stated as the two properties themselves rather than as the
        // bounds: no single site can hold a full in-sync set, and a site
        // loss leaves one.
        let one_site_could_hold_the_whole_isr =
            buckets.iter().any(|&held| held >= min_insync);
        prop_assert_eq!(
            min_insync_is_site_loss_safe(rf, sites, min_insync),
            !one_site_could_hold_the_whole_isr && min_insync <= survivors
        );
    }

    #[test]
    fn quorum_matches_iterator_oracle(
        voters_per_site in proptest::collection::vec(0i64..8, 0..7),
    ) {
        prop_assert_eq!(
            quorum_survives_any_single_site_loss(&voters_per_site),
            quorum_oracle(&voters_per_site)
        );
    }
}

#[yare::parameterized(
    three_sites = { "three replicas over three sites", 3, 3, 2 },
    two_sites = { "three replicas over two sites", 3, 2, 1 },
    five_over_three = { "five replicas over three sites", 5, 3, 3 },
    one_site = { "one site holds every replica", 4, 1, 0 },
)]
fn site_loss_takes_away_the_largest_site(name: &str, rf: i64, sites: i64, expected: i64) {
    check!(site_loss_survivors(rf, sites) == expected, "case {name}");
}

#[yare::parameterized(
    one_replica = { "one replica is not durable", 1, false },
    two_replicas = { "two is the only safe value", 2, true },
    three_replicas = { "three cannot survive a site loss", 3, false },
)]
fn three_sites_pin_min_insync_replicas_to_two(name: &str, min_insync: i64, expected: bool) {
    check!(
        min_insync_is_site_loss_safe(3, 3, min_insync) == expected,
        "case {name}"
    );
}

/// A replication factor that puts two replicas in one site has no safe
/// `min.insync.replicas` over three sites, and the reason is the lower
/// bound rather than the upper one.
///
/// Four replicas over three sites land 2-1-1. A `min.insync.replicas` of 2
/// is then satisfiable inside the two-replica site alone, so that site can
/// hold every copy of an acknowledged write. Raising it to 3 fixes the
/// placement problem and breaks availability instead: the loss of the
/// two-replica site leaves 2. Nothing in between exists, so the profile
/// takes one replica per site and no more.
#[yare::parameterized(
    fits_doubled_site = { "a whole in-sync set fits in the doubled site", 4, 3, 2, false },
    site_loss_too_small = { "raising it past the doubled site loses a site loss", 4, 3, 3, false },
    two_site_shape = { "three over two sites is the same shape", 3, 2, 2, false },
    both_bounds = { "six over three sites has room for both bounds", 6, 3, 3, true },
    inside_survivors = { "and one more, still inside the survivors", 6, 3, 4, true },
    beyond_survivors = { "but not five, which a site loss cannot leave", 6, 3, 5, false },
)]
fn a_site_holding_two_replicas_has_no_safe_min_insync(
    name: &str,
    rf: i64,
    sites: i64,
    min_insync: i64,
    expected: bool,
) {
    check!(
        min_insync_is_site_loss_safe(rf, sites, min_insync) == expected,
        "case {name}"
    );
}

#[yare::parameterized(
    one_replica = { "one replica is not durable", 1, false },
    two_replicas = { "two is more than the surviving replicas", 2, false },
    three_replicas = { "three cannot survive a site loss", 3, false },
)]
fn two_sites_leave_no_safe_min_insync_replicas(name: &str, min_insync: i64, expected: bool) {
    check!(
        min_insync_is_site_loss_safe(3, 2, min_insync) == expected,
        "case {name}"
    );
}

#[yare::parameterized(
    three_sites = { "one voter in each of three sites", &[1, 1, 1][..], true },
    two_sites = { "two sites lose the quorum either way", &[1, 1][..], false },
    five_voters = { "five voters over three sites", &[2, 2, 1][..], true },
    one_site_majority = { "one site holds three of five voters", &[3, 1, 1][..], false },
    no_sites = { "no sites at all", &[][..], true },
)]
fn quorum_needs_a_voter_in_a_third_site(name: &str, voters_per_site: &[i64], expected: bool) {
    check!(
        quorum_survives_any_single_site_loss(voters_per_site) == expected,
        "case {name}"
    );
}
