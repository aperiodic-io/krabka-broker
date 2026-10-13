//! Unit tests of the `CreatePartitions` replica placement: the automatic
//! site-aware placement of the new partitions, and the validation of an
//! explicit `assignments` list.

use {
    super::*,
    crate::handlers::{create_partitions::test_support::assn, test_support::sorted_replica_sites},
    assert2::assert,
    std::collections::BTreeSet,
};

/// The seeds that a test runs the placement with.
const SEEDS: std::ops::Range<u64> = 0..64;

/// Brokers that declare no site. The placement of such a cluster is Kafka's
/// striped placement.
fn plain_brokers(node_ids: &[u64]) -> Vec<SiteBrokerView> {
    node_ids
        .iter()
        .map(|node_id| SiteBrokerView {
            node_id: NodeId(*node_id),
            site: None,
            is_witness: false,
            fenced: false,
        })
        .collect()
}

/// Brokers with a site each. A broker whose id is in `witnesses` also
/// carries the witness role.
fn site_brokers(brokers: &[(u64, &str)], witnesses: &[u64]) -> Vec<SiteBrokerView> {
    brokers
        .iter()
        .map(|(node_id, site)| SiteBrokerView {
            node_id: NodeId(*node_id),
            site: Some((*site).to_string()),
            is_witness: witnesses.contains(node_id),
            fenced: false,
        })
        .collect()
}

fn site_of(brokers: &[(u64, &str)], node_id: NodeId) -> String {
    brokers
        .iter()
        .find(|(id, _)| NodeId(*id) == node_id)
        .map(|(_, site)| (*site).to_string())
        .expect("the placement returns a broker that declared a site")
}

/// The sites of one replica list, sorted, so the caller can compare the
/// spread without depending on the replica order.
fn sites_of(brokers: &[(u64, &str)], replicas: &[NodeId]) -> Vec<String> {
    sorted_replica_sites(replicas, |node| site_of(brokers, node))
}

/// The automatic placement of `new_partitions` partitions that `seed` gives.
fn automatic(
    brokers: &[SiteBrokerView],
    new_partitions: usize,
    rf: i16,
    preferred_site: Option<&str>,
    seed: u64,
) -> Result<Vec<Vec<NodeId>>, (i16, String)> {
    resolve_new_partition_assignments(
        None,
        brokers,
        new_partitions,
        rf,
        preferred_site,
        &mut PlacementRng::seeded(seed),
    )
}

/// The explicit assignments `provided`, resolved.
fn explicit(
    provided: &[CreatePartitionsAssignment],
    brokers: &[SiteBrokerView],
    rf: i16,
    preferred_site: Option<&str>,
) -> Result<Vec<Vec<NodeId>>, (i16, String)> {
    resolve_new_partition_assignments(
        Some(&provided.to_vec()),
        brokers,
        provided.len(),
        rf,
        preferred_site,
        &mut PlacementRng::seeded(0),
    )
}

#[test]
fn a_cluster_without_sites_stripes_the_new_partitions() {
    let brokers = plain_brokers(&[0, 1, 2]);

    for seed in SEEDS {
        let out = automatic(&brokers, 3, 2, None, seed).expect("striped placement should succeed");

        assert!(out.len() == 3);
        for r in &out {
            assert!(r.len() == 2, "each replica list must be rf=2");
            assert!(r[0] != r[1]);
            for b in r {
                assert!(brokers.iter().any(|known| known.node_id == *b));
            }
        }
    }
}

#[test]
fn a_mixed_rack_cluster_keeps_the_unracked_broker_placeable() {
    let brokers = vec![
        SiteBrokerView {
            node_id: NodeId(1),
            site: Some("a".into()),
            is_witness: false,
            fenced: false,
        },
        SiteBrokerView {
            node_id: NodeId(2),
            site: Some("b".into()),
            is_witness: false,
            fenced: false,
        },
        SiteBrokerView {
            node_id: NodeId(3),
            site: None,
            is_witness: false,
            fenced: false,
        },
    ];

    let assignments = automatic(&brokers, 3, 3, None, 0).expect("mixed-rack automatic placement");

    assert!(
        assignments
            .iter()
            .all(|replicas| replicas.contains(&NodeId(3)))
    );
}

/// Kafka's `createPartitions` places `additional` partitions with a placer
/// that starts at a random broker and ignores the index of the first new
/// partition. The new partitions of a topic that has two thus lead anywhere,
/// and do not continue the rotation of the two it has.
#[test]
fn new_partitions_start_at_a_fresh_random_broker() {
    let brokers = plain_brokers(&[0, 1, 2]);

    let firsts = SEEDS
        .map(|seed| automatic(&brokers, 2, 1, None, seed).expect("striped placement")[0][0])
        .collect::<BTreeSet<_>>();

    assert!(firsts == BTreeSet::from([NodeId(0), NodeId(1), NodeId(2)]));
}

#[test]
fn three_sites_hold_one_replica_of_every_new_partition() {
    const SITES: [(u64, &str); 3] = [(1, "a"), (2, "b"), (3, "c")];
    let brokers = site_brokers(&SITES, &[]);

    for seed in SEEDS {
        let new_tail =
            automatic(&brokers, 4, 3, None, seed).expect("site placement should succeed");

        assert!(new_tail.len() == 4);
        for replicas in &new_tail {
            assert!(sites_of(&SITES, replicas) == vec!["a", "b", "c"]);
        }
        // The leader moves one site on with each partition.
        assert!(new_tail[3][0] == new_tail[0][0]);
        assert!(new_tail[1][0] != new_tail[0][0]);
    }
}

#[test]
fn the_preferred_site_leads_every_new_partition() {
    const SITES: [(u64, &str); 6] = [(1, "a"), (2, "b"), (3, "c"), (4, "a"), (5, "b"), (6, "c")];
    let brokers = site_brokers(&SITES, &[]);

    for seed in SEEDS {
        // The topic already has two partitions and grows to six.
        let new_tail =
            automatic(&brokers, 4, 3, Some("b"), seed).expect("site placement should succeed");

        let leader_sites = new_tail
            .iter()
            .map(|replicas| site_of(&SITES, replicas[0]))
            .collect::<Vec<_>>();
        assert!(leader_sites == vec!["b"; 4]);
        let spread = new_tail
            .iter()
            .map(|replicas| sites_of(&SITES, replicas))
            .collect::<Vec<_>>();
        assert!(spread == vec![vec!["a", "b", "c"]; 4]);
    }
}

#[test]
fn a_witness_replicates_new_partitions_but_leads_none() {
    const SITES: [(u64, &str); 3] = [(1, "a"), (2, "b"), (3, "w")];
    let brokers = site_brokers(&SITES, &[3]);

    for seed in SEEDS {
        let new_tail =
            automatic(&brokers, 6, 3, None, seed).expect("site placement should succeed");

        assert!(
            new_tail
                .iter()
                .all(|replicas| replicas.contains(&NodeId(3)) && replicas[0] != NodeId(3))
        );
    }
}

#[test]
fn a_fenced_broker_takes_a_new_partition_only_as_a_last_resort() {
    let mut brokers = plain_brokers(&[0, 1, 2]);
    brokers[1].fenced = true;

    for seed in SEEDS {
        // Kafka's placer counts the fenced broker: rf=3 on three brokers
        // places, with the fenced broker last and never leading.
        let full = automatic(&brokers, 3, 3, None, seed).expect("rf=3 fits three brokers");
        assert!(
            full.iter()
                .all(|replicas| replicas.len() == 3 && replicas[2] == NodeId(1))
        );
        // rf=2 has enough unfenced brokers and leaves it out.
        let partial = automatic(&brokers, 3, 2, None, seed).expect("rf=2 fits two brokers");
        assert!(
            partial
                .iter()
                .all(|replicas| !replicas.contains(&NodeId(1)))
        );
    }
}

#[test]
fn a_manual_assignment_overrides_the_site_placement() {
    const SITES: [(u64, &str); 3] = [(1, "a"), (2, "b"), (3, "c")];
    let brokers = site_brokers(&SITES, &[]);
    let provided = vec![assn(&[2, 3])];

    let manual = explicit(&provided, &brokers, 2, Some("a"))
        .expect("explicit assignments should pass validation");

    assert!(manual == vec![vec![NodeId(2), NodeId(3)]]);
    // The automatic placement of the same cluster leads in site `a`, so
    // the manual list really did override it.
    let placed = automatic(&brokers, 1, 2, Some("a"), 0).expect("site placement should succeed");
    assert!(site_of(&SITES, placed[0][0]) == "a");
}

#[test]
fn a_cluster_of_witnesses_returns_invalid_rf() {
    const SITES: [(u64, &str); 3] = [(1, "a"), (2, "b"), (3, "c")];
    let brokers = site_brokers(&SITES, &[1, 2, 3]);

    let err = automatic(&brokers, 1, 3, None, 0)
        .expect_err("a cluster that can lead no partition must fail");

    assert!(err.0 == codes::INVALID_REPLICATION_FACTOR);
}

#[test]
fn rf_exceeds_broker_count_returns_invalid_rf() {
    let brokers = plain_brokers(&[0, 1]);
    let err = automatic(&brokers, 1, 3, None, 0).expect_err("rf=3 against 2 brokers must fail");
    assert!(err.0 == codes::INVALID_REPLICATION_FACTOR);
}

#[test]
fn honored_assignments_pass_through_verbatim() {
    let brokers = plain_brokers(&[0, 1, 2, 3]);
    let provided = vec![assn(&[3, 1]), assn(&[2, 0]), assn(&[1, 3])];
    let out = explicit(&provided, &brokers, 2, None)
        .expect("explicit assignments should pass validation");
    assert!(
        out == vec![
            vec![NodeId(3), NodeId(1)],
            vec![NodeId(2), NodeId(0)],
            vec![NodeId(1), NodeId(3)],
        ]
    );
}

/// Kafka's `validateManualPartitionAssignment` refuses an explicit list with
/// these messages, on brokers 0 to 2 and a topic of replication factor 2.
#[test]
fn invalid_explicit_assignments_answer_kafkas_messages() {
    let brokers = plain_brokers(&[0, 1, 2]);
    let cases: [(&[&[i32]], &str); 5] = [
        (
            &[&[0, 1, 2]],
            "The manual partition assignment includes a partition with 3 replica(s), but this \
             is not consistent with previous partitions, which have 2 replica(s).",
        ),
        (
            &[&[1, 1]],
            "The manual partition assignment includes the broker 1 more than once.",
        ),
        (
            &[&[0, 9]],
            "The manual partition assignment includes broker 9, but no such broker is \
             registered.",
        ),
        (
            &[&[0, -1]],
            "The manual partition assignment includes broker -1, but no such broker is \
             registered.",
        ),
        (
            &[&[0, 1], &[]],
            "The manual partition assignment includes an empty replica list.",
        ),
    ];

    let (actual, expected): (Vec<_>, Vec<_>) = cases
        .into_iter()
        .map(|(lists, message)| {
            let provided: Vec<CreatePartitionsAssignment> =
                lists.iter().map(|list| assn(list)).collect();
            (
                explicit(&provided, &brokers, 2, None),
                Err((codes::INVALID_REPLICA_ASSIGNMENT, message.to_owned())),
            )
        })
        .unzip();
    assert!(actual == expected);
}

/// An automatic placement that cannot put `rf` replicas on the brokers
/// answers the message of Kafka's placer. `createPartitions` lets that
/// message through as it is, where `createTopic` wraps it in "Unable to
/// replicate the partition", so the row carries no prefix. Fenced brokers
/// count toward the total, and a cluster with none unfenced says so.
#[test]
fn an_unplaceable_rf_answers_the_placers_message() {
    let brokers = plain_brokers(&[0, 1]);
    let mut fenced = plain_brokers(&[0, 1]);
    for broker in &mut fenced {
        broker.fenced = true;
    }

    let too_many = automatic(&brokers, 1, 3, None, 0);
    let none_unfenced = automatic(&fenced, 1, 1, None, 0);

    assert!(
        too_many
            == Err((
                codes::INVALID_REPLICATION_FACTOR,
                "The target replication factor of 3 cannot be reached because only 2 broker(s) \
                 are registered or some brokers have all their log directories cordoned."
                    .to_owned(),
            ))
    );
    assert!(
        none_unfenced
            == Err((
                codes::INVALID_REPLICATION_FACTOR,
                "All brokers are currently fenced, or have all their log directories cordoned."
                    .to_owned(),
            ))
    );
}
