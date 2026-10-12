//! Tests for the stretch-cluster replica placement: Kafka's striped placement
//! for a cluster without a site, the random start of both placements, the
//! site spread and its rotation, the preferred-site and witness rules for
//! `replicas[0]`, the last-resort use of fenced brokers, and the requests that
//! must return no assignment at all.
//!
//! They sit in their own file because checking the rules takes about as many
//! lines as stating them. The random start makes an exact replica list depend
//! on the seed, so most tests run a range of seeds and check what holds for
//! every one of them.

use std::collections::BTreeSet;

use assert2::assert;

use super::*;

/// The seeds that a test runs the placement with.
const SEEDS: std::ops::Range<u64> = 0..64;

fn broker(node_id: u64, site: Option<&str>, is_witness: bool) -> SiteBrokerView {
    SiteBrokerView {
        node_id: NodeId(node_id),
        site: site.map(str::to_string),
        is_witness,
        fenced: false,
    }
}

fn replica(node_id: u64, site: &str) -> SiteBrokerView {
    broker(node_id, Some(site), false)
}

fn witness(node_id: u64, site: &str) -> SiteBrokerView {
    broker(node_id, Some(site), true)
}

fn fenced(mut view: SiteBrokerView) -> SiteBrokerView {
    view.fenced = true;
    view
}

fn plain(node_ids: &[u64]) -> Vec<SiteBrokerView> {
    node_ids
        .iter()
        .map(|node_id| broker(*node_id, None, false))
        .collect()
}

/// The placement that `seed` gives.
fn place(
    brokers: &[SiteBrokerView],
    partitions: i32,
    replication_factor: i16,
    preferred_site: Option<&str>,
    seed: u64,
) -> Vec<Vec<NodeId>> {
    stretch_replicas(
        brokers,
        partitions,
        replication_factor,
        preferred_site,
        &mut PlacementRng::seeded(seed),
    )
}

// Two brokers in each of the sites "a", "b", and "c", out of node-id order
// so every test also covers the sort.
fn six_brokers() -> Vec<SiteBrokerView> {
    vec![
        replica(5, "c"),
        replica(2, "a"),
        replica(6, "c"),
        replica(3, "b"),
        replica(1, "a"),
        replica(4, "b"),
    ]
}

fn site_of(brokers: &[SiteBrokerView], node_id: NodeId) -> &str {
    brokers
        .iter()
        .find(|broker| broker.node_id == node_id)
        .and_then(|broker| broker.site.as_deref())
        .expect("the placement returns a known broker with a site")
}

fn sites_of(brokers: &[SiteBrokerView], replicas: &[NodeId]) -> Vec<String> {
    let mut sites = replicas
        .iter()
        .map(|node_id| site_of(brokers, *node_id).to_string())
        .collect::<Vec<_>>();
    sites.sort();
    sites
}

fn six_partitions(brokers: &[SiteBrokerView], seed: u64) -> Vec<Vec<NodeId>> {
    let placement = place(brokers, 6, 3, None, seed);
    assert!(placement.len() == 6);
    placement
}

fn distinct(replicas: &[NodeId]) -> Vec<NodeId> {
    let mut ids = replicas.to_vec();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// The first replica of each partition.
fn leaders(placement: &[Vec<NodeId>]) -> Vec<NodeId> {
    placement.iter().map(|replicas| replicas[0]).collect()
}

#[test]
fn a_cluster_without_a_site_stripes_from_a_random_start() {
    let brokers = plain(&[3, 1, 2]);
    let ring = [NodeId(1), NodeId(2), NodeId(3)];

    for seed in SEEDS {
        for (partitions, replication_factor) in [(1, 1), (3, 1), (3, 2), (4, 3), (3, 3)] {
            let placement = place(&brokers, partitions, replication_factor, None, seed);

            assert!(
                placement.len() == usize::try_from(partitions).unwrap(),
                "seed {seed}"
            );
            // The first `n` partitions of a round move one broker further
            // round the id-ordered ring each, and each replica list is the
            // ring from its first broker on.
            let start = ring.iter().position(|id| *id == placement[0][0]).unwrap();
            for (partition, replicas) in placement.iter().take(ring.len()).enumerate() {
                let want = (0..usize::try_from(replication_factor).unwrap())
                    .map(|step| ring[(start + partition + step) % ring.len()])
                    .collect::<Vec<_>>();
                assert!(
                    *replicas == want,
                    "seed {seed}, partitions {partitions}, rf {replication_factor}"
                );
            }
        }
    }
}

#[test]
fn the_first_partition_of_a_cluster_without_a_site_starts_anywhere() {
    let brokers = plain(&[1, 2, 3, 4, 5]);

    let firsts = SEEDS
        .map(|seed| place(&brokers, 1, 1, None, seed)[0][0])
        .collect::<BTreeSet<_>>();

    // With a start at broker 1 for every topic, brokers 2 to 5 would never
    // host a single-partition topic.
    assert!(firsts == (1..=5).map(NodeId).collect::<BTreeSet<_>>());
}

#[test]
fn a_cluster_without_a_site_reshuffles_after_each_round_and_stays_balanced() {
    let brokers = plain(&[1, 2, 3]);

    for seed in SEEDS {
        let placement = place(&brokers, 6, 1, None, seed);

        // Two rounds over three brokers: each broker leads twice, whatever
        // order the shuffle after the first round chose.
        let mut led = leaders(&placement);
        led.sort_unstable();
        assert!(
            led == vec![
                NodeId(1),
                NodeId(1),
                NodeId(2),
                NodeId(2),
                NodeId(3),
                NodeId(3)
            ],
            "seed {seed}"
        );
    }
    // The second round is not a copy of the first for every seed.
    let repeats = SEEDS
        .filter(|seed| {
            let led = leaders(&place(&brokers, 6, 1, None, *seed));
            led[..3] == led[3..]
        })
        .count();
    assert!(repeats < SEEDS.count());
}

#[test]
fn the_input_order_is_not_part_of_the_placement() {
    let brokers = plain(&[3, 1, 2]);
    let sorted = plain(&[1, 2, 3]);

    for seed in SEEDS {
        assert!(place(&brokers, 7, 2, None, seed) == place(&sorted, 7, 2, None, seed));
    }
}

#[test]
fn a_fenced_broker_is_a_last_resort_and_never_the_leader() {
    let brokers = vec![
        broker(1, None, false),
        fenced(broker(2, None, false)),
        broker(3, None, false),
    ];

    for seed in SEEDS {
        // Replication factor 3 has to use the fenced broker, and puts it last.
        for replicas in place(&brokers, 5, 3, None, seed) {
            assert!(
                replicas.len() == 3 && replicas[2] == NodeId(2),
                "seed {seed}"
            );
        }
        // Replication factor 2 finds enough unfenced brokers and leaves it out.
        for replicas in place(&brokers, 5, 2, None, seed) {
            assert!(!replicas.contains(&NodeId(2)), "seed {seed}");
        }
        // No partition leads on it, even at replication factor 1.
        for replicas in place(&brokers, 5, 1, None, seed) {
            assert!(replicas == vec![NodeId(1)] || replicas == vec![NodeId(3)]);
        }
    }
}

#[test]
fn fenced_brokers_are_counted_and_refused_as_kafkas_placer_does() {
    let three = vec![
        broker(1, None, false),
        fenced(broker(2, None, false)),
        fenced(broker(3, None, false)),
    ];
    let all_fenced = vec![
        fenced(broker(1, None, false)),
        fenced(broker(2, None, false)),
    ];
    let cases = [
        // (label, brokers, replication factor, placed, reason)
        (
            "the replication factor is the broker count, fenced included",
            &three,
            3,
            true,
            String::new(),
        ),
        (
            "the replication factor exceeds the broker count",
            &three,
            4,
            false,
            "The target replication factor of 4 cannot be reached because only 3 broker(s) are \
             registered or some brokers have all their log directories cordoned."
                .to_owned(),
        ),
        (
            "every broker is fenced",
            &all_fenced,
            1,
            false,
            "All brokers are currently fenced, or have all their log directories cordoned."
                .to_owned(),
        ),
        (
            "every broker is fenced and the replication factor is too high",
            &all_fenced,
            3,
            false,
            "All brokers are currently fenced, or have all their log directories cordoned."
                .to_owned(),
        ),
        (
            "no broker at all",
            &Vec::new(),
            1,
            false,
            "All brokers are currently fenced, or have all their log directories cordoned."
                .to_owned(),
        ),
    ];

    for (label, brokers, replication_factor, placed, reason) in cases {
        let placement = place(brokers, 2, replication_factor, None, 0);

        assert!(!placement.is_empty() == placed, "{label}");
        if !placed {
            assert!(
                placement_failure_reason(replication_factor, brokers) == reason,
                "{label}"
            );
        }
    }
}

#[test]
fn one_broker_per_site_rotates_the_replica_list_from_a_random_site() {
    let brokers = vec![replica(1, "a"), replica(2, "b"), replica(3, "c")];

    for seed in SEEDS {
        let placement = place(&brokers, 4, 3, None, seed);

        // Every list holds all three brokers, the leader moves one site on
        // with each partition, and it comes round again after three.
        let led = leaders(&placement);
        assert!(distinct(&led[..3]).len() == 3, "seed {seed}");
        assert!(led[3] == led[0], "seed {seed}");
        for replicas in &placement {
            assert!(
                sites_of(&brokers, replicas) == vec!["a", "b", "c"],
                "seed {seed}"
            );
        }
    }
}

#[test]
fn the_first_partition_of_a_site_cluster_starts_at_any_site_and_broker() {
    let brokers = six_brokers();

    let firsts = SEEDS
        .map(|seed| place(&brokers, 1, 3, None, seed)[0][0])
        .collect::<BTreeSet<_>>();

    assert!(firsts == (1..=6).map(NodeId).collect::<BTreeSet<_>>());
}

#[test]
fn three_sites_hold_one_replica_each() {
    let brokers = vec![replica(1, "a"), replica(2, "b"), replica(3, "c")];

    for seed in SEEDS {
        let placement = place(&brokers, 7, 3, None, seed);

        assert!(placement.len() == 7);
        for replicas in &placement {
            assert!(sites_of(&brokers, replicas) == vec!["a", "b", "c"]);
        }
    }
}

#[test]
fn the_preferred_site_leads_every_partition() {
    let brokers = six_brokers();

    for seed in SEEDS {
        let placement = place(&brokers, 9, 3, Some("b"), seed);

        assert!(placement.len() == 9);
        for replicas in &placement {
            assert!(site_of(&brokers, replicas[0]) == "b");
            assert!(sites_of(&brokers, replicas) == vec!["a", "b", "c"]);
        }
    }
}

#[test]
fn the_witness_replicates_but_never_leads() {
    let brokers = vec![replica(1, "a"), replica(2, "b"), witness(3, "w")];

    for seed in SEEDS {
        let placement = six_partitions(&brokers, seed);

        for replicas in &placement {
            assert!(replicas.contains(&NodeId(3)));
            assert!(replicas[0] != NodeId(3));
        }
    }
}

#[test]
fn a_preferred_witness_site_still_leads_on_a_non_witness() {
    let brokers = vec![replica(1, "a"), replica(2, "b"), witness(3, "w")];

    for seed in SEEDS {
        let placement = place(&brokers, 6, 3, Some("w"), seed);

        assert!(placement.len() == 6);
        for replicas in &placement {
            assert!(site_of(&brokers, replicas[0]) != "w");
            assert!(sites_of(&brokers, replicas) == vec!["a", "b", "w"]);
        }
    }
}

#[test]
fn the_partitions_spread_over_the_brokers_of_a_site() {
    let brokers = six_brokers();

    for seed in SEEDS {
        let placement = place(&brokers, 12, 3, None, seed);

        assert!(placement.len() == 12);
        for replicas in &placement {
            assert!(sites_of(&brokers, replicas) == vec!["a", "b", "c"]);
        }
        // Both brokers of every site take a share of the partitions.
        let used = distinct(&placement.concat());
        assert!(
            used == (1..=6).map(NodeId).collect::<Vec<_>>(),
            "seed {seed}"
        );
    }
}

#[test]
fn a_replication_factor_above_the_site_count_balances_the_sites() {
    let brokers = six_brokers();

    for seed in SEEDS {
        let placement = place(&brokers, 6, 5, None, seed);

        assert!(placement.len() == 6);
        for replicas in &placement {
            assert!(distinct(replicas).len() == 5);
            // Five replicas over three sites: no site holds a third one.
            let mut per_site = ["a", "b", "c"]
                .iter()
                .map(|site| {
                    replicas
                        .iter()
                        .filter(|node_id| site_of(&brokers, **node_id) == *site)
                        .count()
                })
                .collect::<Vec<_>>();
            per_site.sort_unstable();
            assert!(per_site == vec![1, 2, 2]);
        }
    }
}

#[test]
fn a_fenced_broker_of_a_site_is_taken_after_its_unfenced_brokers() {
    // Site "a" holds brokers 1 and 4, and broker 4 is fenced. Sites "b" and
    // "c" hold one broker each.
    let brokers = vec![
        replica(1, "a"),
        fenced(replica(4, "a")),
        replica(2, "b"),
        replica(3, "c"),
    ];

    for seed in SEEDS {
        // One replica per site: the unfenced broker of "a" serves, and the
        // fenced one is not used.
        for replicas in place(&brokers, 6, 3, None, seed) {
            assert!(!replicas.contains(&NodeId(4)), "seed {seed}");
        }
        // Replication factor 4 uses every broker, and no partition leads on
        // the fenced one.
        for replicas in place(&brokers, 6, 4, None, seed) {
            assert!(distinct(&replicas).len() == 4 && replicas[0] != NodeId(4));
        }
    }
}

#[test]
fn a_site_of_fenced_brokers_still_holds_a_replica() {
    let brokers = vec![fenced(replica(1, "a")), replica(2, "b"), replica(3, "c")];

    for seed in SEEDS {
        let placement = six_partitions(&brokers, seed);

        for replicas in &placement {
            assert!(sites_of(&brokers, replicas) == vec!["a", "b", "c"]);
            assert!(replicas[0] != NodeId(1), "seed {seed}");
        }
    }
}

#[test]
fn an_impossible_replication_factor_returns_no_assignment() {
    let brokers = vec![replica(1, "a"), replica(2, "b"), replica(3, "c")];

    for replication_factor in [-1_i16, 0, 4, 100] {
        assert!(place(&brokers, 3, replication_factor, None, 0).is_empty());
    }
}

#[test]
fn a_cluster_that_cannot_lead_a_partition_returns_no_assignment() {
    let witnesses = vec![witness(1, "a"), witness(2, "b"), witness(3, "c")];
    // The only broker that may lead is fenced.
    let no_unfenced_leader = vec![witness(1, "a"), witness(2, "b"), fenced(replica(3, "c"))];

    assert!(place(&witnesses, 3, 3, None, 0).is_empty());
    assert!(place(&no_unfenced_leader, 3, 3, None, 0).is_empty());
}

#[test]
fn an_ordinary_mixed_rack_cluster_places_unracked_brokers_as_one_rack() {
    let brokers = vec![replica(1, "a"), replica(2, "b"), broker(3, None, false)];

    for seed in SEEDS {
        let placement = place(&brokers, 3, 3, None, seed);

        assert!(placement.len() == 3);
        for replicas in &placement {
            assert!(distinct(replicas).len() == 3);
        }
        assert!(distinct(&leaders(&placement)).len() == 3, "seed {seed}");
    }
}

#[test]
fn an_explicit_stretch_cluster_still_rejects_unracked_capacity() {
    let brokers = vec![replica(1, "a"), replica(2, "b"), broker(3, None, false)];

    assert!(place(&brokers, 3, 3, Some("a"), 0).is_empty());
}

#[test]
fn a_seed_fixes_the_placement() {
    let brokers = six_brokers();

    let placement = place(&brokers, 7, 3, Some("c"), 5);

    assert!(place(&brokers, 7, 3, Some("c"), 5) == placement);
    // The input order is not part of the result: the code sorts by node id.
    let reversed = brokers.iter().rev().cloned().collect::<Vec<_>>();
    assert!(place(&reversed, 7, 3, Some("c"), 5) == placement);
    // A preferred site of two brokers still gives both a share of the
    // partitions, and the start decides which one comes first.
    let firsts = SEEDS
        .map(|seed| place(&brokers, 1, 3, Some("c"), seed)[0][0])
        .collect::<BTreeSet<_>>();
    assert!(firsts == BTreeSet::from([NodeId(5), NodeId(6)]));
}

#[test]
fn the_entropy_seed_is_not_a_constant() {
    let seeds = (0..8)
        .map(|_| PlacementRng::from_entropy().next_u64())
        .collect::<BTreeSet<_>>();

    assert!(seeds.len() > 1);
}
