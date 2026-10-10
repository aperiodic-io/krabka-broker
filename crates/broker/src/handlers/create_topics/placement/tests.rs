//! Unit tests of the replica placement: the striped baseline, the site-aware
//! spread, the fenced and shutting-down brokers, and the validation of an
//! explicit assignment list.

use assert2::assert;
use krabka_metadata::MetadataRecord;
use krabka_protocol::owned::create_topics_request::{CreatableReplicaAssignment, CreatableTopic};
use krabka_raft::NodeId;

use super::{
    InitialLeadership, PlacementRng, automatic_leaderships, automatic_placement_exclusions, codes,
    inactive_brokers, manual_leaderships, manual_replicas, placement_failure_message,
    resolve_assignments, site_broker_views,
};
use crate::config_keys::resolve_preferred_leader_site;

/// The seeds that a test runs the placement with.
const SEEDS: std::ops::Range<u64> = 0..32;

/// One broker in each of the sites `a`, `b`, and `c`.
const THREE_SITES: [(u64, Option<&str>); 3] = [(1, Some("a")), (2, Some("b")), (3, Some("c"))];

/// Two brokers in each of the sites `a`, `b`, and `c`.
const SIX_BROKERS: [(u64, Option<&str>); 6] = [
    (1, Some("a")),
    (2, Some("b")),
    (3, Some("c")),
    (4, Some("a")),
    (5, Some("b")),
    (6, Some("c")),
];

/// A metadata image that registers `brokers` with their racks, marks
/// `witnesses` with the witness role, and pins `preferred_site` as the
/// cluster-wide default.
fn stretch_image(
    brokers: &[(u64, Option<&str>)],
    witnesses: &[u64],
    preferred_site: Option<&str>,
) -> krabka_metadata::MetadataImage {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    for (node_id, rack) in brokers {
        image.apply(&MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                incarnation_id: uuid::Uuid::from_u128(u128::from(*node_id)),
                rack: rack.map(str::to_string),
                ..crate::test_support::broker_registration(krabka_raft::NodeId(*node_id))
            },
        ));
    }
    for node_id in witnesses {
        image.apply(&MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: NodeId(*node_id),
                config_name: crate::config_keys::BROKER_WITNESS.into(),
                config_value: Some(crate::config_keys::WITNESS_TRUE.into()),
            },
        ));
    }
    if let Some(site) = preferred_site {
        image.apply(&MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: crate::config_keys::STRETCH_PREFERRED_LEADER_SITE.into(),
                config_value: Some(site.into()),
            },
        ));
    }
    image
}

/// A topic request that asks for automatic placement.
fn auto_topic(partitions: i32, rf: i16) -> CreatableTopic {
    CreatableTopic {
        name: "orders".into(),
        num_partitions: partitions,
        replication_factor: rf,
        ..Default::default()
    }
}

/// The `(node id, site, witness)` triple of each view, in list order.
fn view_rows(views: &[super::SiteBrokerView]) -> Vec<(NodeId, Option<String>, bool)> {
    views
        .iter()
        .map(|view| (view.node_id, view.site.clone(), view.is_witness))
        .collect()
}

fn site_of(brokers: &[(u64, Option<&str>)], node_id: NodeId) -> String {
    brokers
        .iter()
        .find(|(id, _)| NodeId(*id) == node_id)
        .and_then(|(_, rack)| *rack)
        .expect("the placement returns a broker that declared a site")
        .to_string()
}

/// The sites of one replica list, sorted, so the caller can compare the
/// spread without depending on the replica order.
fn sites_of(brokers: &[(u64, Option<&str>)], replicas: &[NodeId]) -> Vec<String> {
    crate::handlers::test_support::sorted_replica_sites(replicas, |node| site_of(brokers, node))
}

fn broker_views(
    image: &krabka_metadata::MetadataImage,
    local_broker: Option<NodeId>,
) -> Vec<super::SiteBrokerView> {
    site_broker_views(
        image,
        local_broker,
        &std::collections::HashSet::new(),
        &std::collections::HashSet::new(),
    )
}

/// The automatic placement that `seed` gives, or the code and message of a
/// refusal.
fn assign(
    topic: &CreatableTopic,
    views: &[super::SiteBrokerView],
    preferred_site: Option<&str>,
    seed: u64,
) -> Result<Vec<Vec<NodeId>>, (i16, String)> {
    resolve_assignments(
        topic,
        views,
        preferred_site,
        &mut PlacementRng::seeded(seed),
    )
}

/// Marks `node_id` as in controlled shutdown in `image`.
fn enter_controlled_shutdown(image: &mut krabka_metadata::MetadataImage, node_id: u64) {
    let mut registration = image.broker(NodeId(node_id)).expect("registered").clone();
    registration.in_controlled_shutdown = true;
    image.apply(&MetadataRecord::V1BrokerRegistration(registration));
}

#[test]
fn manual_assignments_preserve_partition_order_and_validate_brokers() {
    let topic = CreatableTopic {
        num_partitions: -1,
        replication_factor: -1,
        assignments: vec![
            CreatableReplicaAssignment {
                partition_index: 1,
                broker_ids: vec![2, 1],
                ..Default::default()
            },
            CreatableReplicaAssignment {
                partition_index: 0,
                broker_ids: vec![1, 2],
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let assignments =
        manual_replicas(&topic, &[NodeId(1), NodeId(2)]).expect("valid manual assignments");
    assert!(assignments == vec![vec![NodeId(1), NodeId(2)], vec![NodeId(2), NodeId(1)]]);
}

/// Kafka's `createTopic` and `validateManualPartitionAssignment` refuse a
/// manual assignment with these codes and messages, on brokers 1 and 2.
#[test]
fn invalid_manual_assignments_answer_kafkas_codes_and_messages() {
    fn topic(
        num_partitions: i32,
        replication_factor: i16,
        lists: &[(i32, &[i32])],
    ) -> CreatableTopic {
        CreatableTopic {
            num_partitions,
            replication_factor,
            assignments: lists
                .iter()
                .map(|(partition_index, broker_ids)| CreatableReplicaAssignment {
                    partition_index: *partition_index,
                    broker_ids: broker_ids.to_vec(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
    let cases = [
        (
            topic(-1, 2, &[(0, &[1, 2])]),
            codes::INVALID_REQUEST,
            "A manual partition assignment was specified, but replication factor was not set to \
             -1.",
        ),
        (
            topic(1, -1, &[(0, &[1, 2])]),
            codes::INVALID_REQUEST,
            "A manual partition assignment was specified, but numPartitions was not set to -1.",
        ),
        (
            topic(-1, -1, &[(0, &[1, 2]), (0, &[2, 1])]),
            codes::INVALID_REPLICA_ASSIGNMENT,
            "Found multiple manual partition assignments for partition 0",
        ),
        (
            topic(-1, -1, &[(0, &[])]),
            codes::INVALID_REPLICA_ASSIGNMENT,
            "The manual partition assignment includes an empty replica list.",
        ),
        (
            topic(-1, -1, &[(0, &[9, 1, 7])]),
            codes::INVALID_REPLICA_ASSIGNMENT,
            "The manual partition assignment includes broker 7, but no such broker is registered.",
        ),
        (
            topic(-1, -1, &[(0, &[2, 1, 2])]),
            codes::INVALID_REPLICA_ASSIGNMENT,
            "The manual partition assignment includes the broker 2 more than once.",
        ),
        (
            topic(-1, -1, &[(0, &[1, 2]), (1, &[1])]),
            codes::INVALID_REPLICA_ASSIGNMENT,
            "The manual partition assignment includes a partition with 1 replica(s), but this is \
             not consistent with previous partitions, which have 2 replica(s).",
        ),
        (
            topic(-1, -1, &[(0, &[1]), (2, &[2])]),
            codes::INVALID_REPLICA_ASSIGNMENT,
            "partitions should be a consecutive 0-based integer sequence",
        ),
    ];

    let (actual, expected): (Vec<_>, Vec<_>) = cases
        .into_iter()
        .map(|(topic, code, message)| {
            (
                manual_replicas(&topic, &[NodeId(1), NodeId(2)]),
                Err((code, message.to_owned())),
            )
        })
        .unzip();
    assert!(actual == expected);
}

#[test]
fn site_broker_views_read_the_rack_and_the_witness_role() {
    let image = stretch_image(&[(3, Some("c")), (1, Some("a")), (2, None)], &[3], None);

    let views = broker_views(&image, Some(NodeId(9)));

    // The views come back in node-id order, whatever order the image
    // holds them in.
    let expected = vec![
        (NodeId(1), Some("a".to_string()), false),
        (NodeId(2), None, false),
        (NodeId(3), Some("c".to_string()), true),
    ];
    assert!(view_rows(&views) == expected);
}

/// Kafka's `UsableBrokerIterator` hands the placer a fenced broker, tagged as
/// fenced, and `StripedReplicaPlacer` takes it as a last resort. A fenced
/// broker thus stays a candidate, unlike one in controlled shutdown.
#[test]
fn a_fenced_broker_is_a_tagged_last_resort_candidate() {
    let mut image = stretch_image(&[(1, None), (2, None), (3, None)], &[], None);
    let fenced = std::collections::HashSet::from([2]);
    let views = site_broker_views(
        &image,
        Some(NodeId(1)),
        &automatic_placement_exclusions(&image, &fenced),
        &fenced,
    );

    assert!(
        views
            .iter()
            .map(|view| (view.node_id, view.fenced))
            .collect::<Vec<_>>()
            == vec![(NodeId(1), false), (NodeId(2), true), (NodeId(3), false)]
    );
    for seed in SEEDS {
        // The replication factor is the broker count, fenced included, so
        // the topic is placed, and the fenced broker comes last.
        let assignments = assign(&auto_topic(3, 3), &views, None, seed).expect("placement");
        assert!(
            assignments
                .iter()
                .all(|replicas| replicas.len() == 3 && replicas[2] == NodeId(2)),
            "seed {seed}"
        );
    }

    // A broker in controlled shutdown is not a candidate at all, so the same
    // replication factor now exceeds the broker count.
    enter_controlled_shutdown(&mut image, 3);
    let views = site_broker_views(
        &image,
        Some(NodeId(1)),
        &automatic_placement_exclusions(&image, &fenced),
        &fenced,
    );
    assert!(
        views.iter().map(|view| view.node_id).collect::<Vec<_>>() == vec![NodeId(1), NodeId(2)]
    );
    assert!(assign(&auto_topic(1, 3), &views, None, 0) == Ok(Vec::new()));
    assert!(
        placement_failure_message(3, &views)
            == "Unable to replicate the partition 3 time(s): The target replication factor of 3 \
                cannot be reached because only 2 broker(s) are registered or some brokers have \
                all their log directories cordoned."
    );
}

/// A fenced broker is out of controlled shutdown. Kafka's
/// `BrokerHeartbeatManager.touch` clears the controlled-shutdown offset of a
/// broker it fences, so `UsableBrokerIterator` hands the broker to the placer
/// again, as a fenced last resort. The registration keeps
/// `InControlledShutdown` until the broker registers again, so only the fence
/// says that the controlled shutdown is over.
///
/// The second row is a broker after a clean stop, the state that
/// `ShareConsumerTest.test_broker_failure` leaves broker 1 in. Without it,
/// `__share_group_state` at replication factor 3 could not be created on the
/// two brokers left, and the share group never initialized its partitions.
#[test]
fn a_fenced_broker_is_out_of_controlled_shutdown() {
    struct Case {
        what: &'static str,
        /// The fence on the registration of broker 3, which is in controlled
        /// shutdown.
        registration_fenced: bool,
        /// What the controller's heartbeat registry holds unavailable.
        registry_unavailable: &'static [u64],
        /// `(node id, fenced)` of each view.
        views: Vec<(NodeId, bool)>,
        /// The sorted replicas and ISR of an automatic partition at
        /// replication factor 3, or the `CreateTopics` refusal.
        created: Result<(Vec<NodeId>, Vec<NodeId>), String>,
    }
    impl Case {
        fn fenced_last_resort(
            what: &'static str,
            registration_fenced: bool,
            registry_unavailable: &'static [u64],
        ) -> Self {
            Self {
                what,
                registration_fenced,
                registry_unavailable,
                views: vec![(NodeId(1), false), (NodeId(2), false), (NodeId(3), true)],
                created: Ok((
                    vec![NodeId(1), NodeId(2), NodeId(3)],
                    vec![NodeId(1), NodeId(2)],
                )),
            }
        }
    }
    let cases = [
        Case {
            what: "still in controlled shutdown",
            registration_fenced: false,
            registry_unavailable: &[],
            views: vec![(NodeId(1), false), (NodeId(2), false)],
            created: Err(
                "Unable to replicate the partition 3 time(s): The target replication \
                          factor of 3 cannot be reached because only 2 broker(s) are \
                          registered or some brokers have all their log directories cordoned."
                    .to_owned(),
            ),
        },
        Case::fenced_last_resort("stopped after its controlled shutdown", true, &[]),
        Case::fenced_last_resort(
            "session expired on the controller before the fence is replicated",
            false,
            &[3],
        ),
    ];
    for case in cases {
        let mut image = stretch_image(&[(1, None), (2, None), (3, None)], &[], None);
        let mut registration = image.broker(NodeId(3)).expect("registered").clone();
        registration.in_controlled_shutdown = true;
        registration.fenced = case.registration_fenced;
        image.apply(&MetadataRecord::V1BrokerRegistration(registration));
        // `unavailable_brokers`: the replicated fence, with the controller's
        // registry on top.
        let mut unavailable = crate::heartbeat::fencing::fenced_node_ids(&image);
        unavailable.extend(case.registry_unavailable);

        let views = site_broker_views(
            &image,
            Some(NodeId(1)),
            &automatic_placement_exclusions(&image, &unavailable),
            &unavailable,
        );
        let created = match assign(&auto_topic(1, 3), &views, None, 0) {
            Ok(assignments) if assignments.is_empty() => Err(placement_failure_message(3, &views)),
            Ok(assignments) => {
                let leaderships =
                    automatic_leaderships(&assignments, &inactive_brokers(&image, &unavailable));
                let mut replicas = assignments[0].clone();
                replicas.sort_unstable();
                let mut isr = leaderships[0].isr.clone();
                isr.sort_unstable();
                Ok((replicas, isr))
            }
            Err((code, message)) => panic!("{}: refused with {code}: {message}", case.what),
        };

        assert!(
            views
                .iter()
                .map(|view| (view.node_id, view.fenced))
                .collect::<Vec<_>>()
                == case.views,
            "{}",
            case.what
        );
        assert!(created == case.created, "{}", case.what);
    }
}

/// Kafka's `createTopic` builds the ISR from the replicas that pass
/// `isActive`, and makes its first member the leader. A fenced replica stays
/// in the replica list and out of the ISR.
#[test]
fn the_isr_leaves_out_fenced_and_shutting_down_brokers() {
    let mut image = stretch_image(&[(1, None), (2, None), (3, None)], &[], None);
    enter_controlled_shutdown(&mut image, 3);
    let fenced = std::collections::HashSet::from([2]);
    let inactive = inactive_brokers(&image, &fenced);
    let replicas = vec![vec![NodeId(1), NodeId(2), NodeId(3)]];

    assert!(inactive == std::collections::HashSet::from([2, 3]));
    assert!(
        automatic_leaderships(&replicas, &inactive)
            == vec![InitialLeadership {
                leader: NodeId(1),
                isr: vec![NodeId(1)],
            }]
    );
    // A manual assignment follows the same rule, and refuses a list of
    // brokers that are all inactive.
    let witnesses = std::collections::HashSet::new();
    assert!(
        manual_leaderships(
            &[vec![NodeId(3), NodeId(2), NodeId(1)]],
            &inactive,
            &witnesses,
            0
        ) == Ok(vec![InitialLeadership {
            leader: NodeId(1),
            isr: vec![NodeId(1)],
        }])
    );
    assert!(
        manual_leaderships(&[vec![NodeId(3), NodeId(2)]], &inactive, &witnesses, 1)
            == Err(
                "All brokers specified in the manual partition assignment for partition 1 \
                    are fenced or in controlled shutdown."
                    .to_owned()
            )
    );
}

#[test]
fn an_image_without_a_registration_places_on_this_broker_alone() {
    let image = stretch_image(&[], &[], None);

    let views = broker_views(&image, Some(NodeId(7)));

    assert!(view_rows(&views) == vec![(NodeId(7), None, false)]);
}

/// A node whose `process.roles` exclude `broker` hosts no replica, and it
/// never self-registers, so the empty-image fallback must not name it. The
/// list stays empty, the automatic placement cannot satisfy any replication
/// factor, and the handler reports `INVALID_REPLICATION_FACTOR` -- what a
/// Kafka controller with no registered broker returns.
#[test]
fn a_controller_only_node_is_not_its_own_placement_fallback() {
    let image = stretch_image(&[], &[], None);

    let views = broker_views(&image, None);

    assert!(views.is_empty());
    assert!(assign(&auto_topic(1, 1), &views, None, 0) == Ok(Vec::new()));
}

#[test]
fn three_sites_hold_one_replica_of_every_partition() {
    let image = stretch_image(&THREE_SITES, &[], None);
    let views = broker_views(&image, Some(NodeId(1)));

    for seed in SEEDS {
        let assignments =
            assign(&auto_topic(4, 3), &views, None, seed).expect("automatic placement");

        // Every list holds all three brokers, one for each site, and the
        // leader rotates over the sites, from a random one.
        assert!(
            assignments
                .iter()
                .all(|replicas| sites_of(&THREE_SITES, replicas) == vec!["a", "b", "c"]),
            "seed {seed}"
        );
        let leaders = assignments
            .iter()
            .map(|replicas| replicas[0])
            .collect::<Vec<_>>();
        assert!(
            leaders[..3]
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == 3
                && leaders[3] == leaders[0],
            "seed {seed}"
        );
    }
}

#[test]
fn the_preferred_site_leads_every_partition() {
    let image = stretch_image(&SIX_BROKERS, &[], Some("b"));
    let views = broker_views(&image, Some(NodeId(1)));

    for seed in SEEDS {
        let assignments = assign(
            &auto_topic(6, 3),
            &views,
            resolve_preferred_leader_site(&image),
            seed,
        )
        .expect("automatic placement");

        let leader_sites = assignments
            .iter()
            .map(|replicas| site_of(&SIX_BROKERS, replicas[0]))
            .collect::<Vec<_>>();
        assert!(leader_sites == vec!["b"; 6]);
        let spread = assignments
            .iter()
            .map(|replicas| sites_of(&SIX_BROKERS, replicas))
            .collect::<Vec<_>>();
        assert!(spread == vec![vec!["a", "b", "c"]; 6]);
    }
}

#[test]
fn a_witness_replicates_but_leads_no_partition() {
    let brokers = [(1, Some("a")), (2, Some("b")), (3, Some("w"))];
    let image = stretch_image(&brokers, &[3], None);
    let views = broker_views(&image, Some(NodeId(1)));

    for seed in SEEDS {
        let assignments =
            assign(&auto_topic(6, 3), &views, None, seed).expect("automatic placement");

        // The witness takes a replica of every partition, and leadership
        // rotates over the two brokers that serve clients.
        let holds_witness = assignments
            .iter()
            .map(|replicas| replicas.contains(&NodeId(3)))
            .collect::<Vec<_>>();
        assert!(holds_witness == vec![true; 6]);
        let leaders = assignments
            .iter()
            .map(|replicas| replicas[0])
            .collect::<Vec<_>>();
        assert!(
            leaders[0] != leaders[1]
                && leaders.iter().all(|leader| *leader != NodeId(3))
                && leaders[..2] == leaders[2..4]
                && leaders[..2] == leaders[4..],
            "seed {seed}"
        );
    }
}

/// Kafka's `StripedReplicaPlacer` starts each topic at a random broker, so
/// with the defaults `num.partitions=1` and RF 1 the topics of a cluster
/// without racks spread over every broker instead of stacking on the lowest
/// id.
#[test]
fn a_cluster_without_racks_starts_each_topic_at_a_random_broker() {
    let image = stretch_image(&[(1, None), (2, None), (3, None)], &[], None);
    let views = broker_views(&image, Some(NodeId(1)));

    let leaders = (0..64)
        .map(|seed| {
            assign(&auto_topic(1, 1), &views, None, seed).expect("automatic placement")[0][0]
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert!(leaders == std::collections::BTreeSet::from([NodeId(1), NodeId(2), NodeId(3)]));

    for (partitions, rf) in [(1, 1), (3, 1), (3, 2), (4, 3), (5, 2)] {
        let assignments =
            assign(&auto_topic(partitions, rf), &views, None, 7).expect("automatic placement");

        assert!(assignments.len() == usize::try_from(partitions).unwrap());
        for replicas in &assignments {
            let mut distinct = replicas.clone();
            distinct.sort_unstable();
            distinct.dedup();
            assert!(
                replicas.len() == usize::try_from(rf).unwrap() && distinct.len() == replicas.len(),
                "partitions {partitions}, rf {rf}"
            );
        }
    }
}

#[test]
fn a_mixed_rack_cluster_keeps_the_unracked_broker_placeable() {
    let image = stretch_image(&[(1, Some("a")), (2, Some("b")), (3, None)], &[], None);
    let views = broker_views(&image, Some(NodeId(1)));

    let assignments =
        assign(&auto_topic(3, 3), &views, None, 0).expect("mixed-rack automatic placement");

    assert!(
        assignments
            .iter()
            .all(|replicas| replicas.contains(&NodeId(3)))
    );
}

#[test]
fn a_manual_assignment_overrides_the_site_placement() {
    let image = stretch_image(&THREE_SITES, &[], Some("c"));
    let views = broker_views(&image, Some(NodeId(1)));
    let preferred_site = resolve_preferred_leader_site(&image);
    let manual = CreatableTopic {
        name: "orders".into(),
        num_partitions: -1,
        replication_factor: -1,
        assignments: vec![
            CreatableReplicaAssignment {
                partition_index: 0,
                broker_ids: vec![2, 1],
                ..Default::default()
            },
            CreatableReplicaAssignment {
                partition_index: 1,
                broker_ids: vec![1, 3],
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let assignments = assign(&manual, &views, preferred_site, 0).expect("manual assignments");

    assert!(assignments == vec![vec![NodeId(2), NodeId(1)], vec![NodeId(1), NodeId(3)]]);
    // The automatic placement of the same cluster leads in site `c`, so
    // the manual lists really did override it.
    let automatic =
        assign(&auto_topic(2, 2), &views, preferred_site, 0).expect("automatic placement");
    assert!(automatic.iter().all(|replicas| replicas[0] == NodeId(3)));
}

#[test]
fn an_impossible_request_gives_no_assignment() {
    // The empty outer vec is what makes the handler report
    // INVALID_REPLICATION_FACTOR.
    let image = stretch_image(&THREE_SITES, &[], None);
    let views = broker_views(&image, Some(NodeId(1)));

    let too_many = assign(&auto_topic(1, 4), &views, None, 0).expect("no error code");

    assert!(too_many.is_empty());

    // A cluster of witnesses can lead no partition at all.
    let witnesses_only = stretch_image(&THREE_SITES, &[1, 2, 3], None);
    let views = broker_views(&witnesses_only, Some(NodeId(1)));

    let unleadable = assign(&auto_topic(1, 3), &views, None, 0).expect("no error code");

    assert!(unleadable.is_empty());
}

/// KIP-1066, as a live `apache/kafka:4.3.1` answers it: the automatic placement
/// leaves out a broker whose log directories are all cordoned, and with every
/// broker so cordoned the placement cannot be met, which the handler reports
/// as `INVALID_REPLICATION_FACTOR` with "All brokers are currently fenced, or
/// have all their log directories cordoned.". A broker with one uncordoned
/// directory, or one that has not reported yet, stays a candidate, and an
/// unavailable broker is left out as before.
#[test]
fn fully_cordoned_brokers_are_not_automatic_placement_candidates() {
    let dir = |n: u128| uuid::Uuid::from_u128(n);
    let mut image = stretch_image(&[(1, None), (2, None), (3, None), (4, None)], &[], None);
    for (node, cordoned) in [
        (1, Some(vec![dir(10), dir(11)])),
        (2, Some(vec![dir(20)])),
        (3, None),
        (4, Some(vec![])),
    ] {
        let mut registration = image.broker(NodeId(node)).expect("registered").clone();
        registration.log_dirs = vec![dir(u128::from(node) * 10), dir(u128::from(node) * 10 + 1)];
        registration.cordoned_log_dirs = cordoned;
        image.apply(&MetadataRecord::V1BrokerRegistration(registration));
    }
    let cases = [
        (
            "no broker in controlled shutdown",
            None,
            vec![NodeId(2), NodeId(3), NodeId(4)],
        ),
        (
            "broker 4 in controlled shutdown",
            Some(4),
            vec![NodeId(2), NodeId(3)],
        ),
    ];
    for (label, shutting_down, want) in cases {
        let mut image = image.clone();
        if let Some(node_id) = shutting_down {
            enter_controlled_shutdown(&mut image, node_id);
        }
        let excluded = automatic_placement_exclusions(&image, &std::collections::HashSet::new());
        let views = site_broker_views(
            &image,
            Some(NodeId(1)),
            &excluded,
            &std::collections::HashSet::new(),
        );
        assert!(
            views.iter().map(|view| view.node_id).collect::<Vec<_>>() == want,
            "{label}"
        );
    }

    let mut all_cordoned = stretch_image(&[(1, None)], &[], None);
    let mut registration = all_cordoned.broker(NodeId(1)).expect("registered").clone();
    registration.log_dirs = vec![dir(10)];
    registration.cordoned_log_dirs = Some(vec![dir(10)]);
    all_cordoned.apply(&MetadataRecord::V1BrokerRegistration(registration));
    let excluded = automatic_placement_exclusions(&all_cordoned, &std::collections::HashSet::new());
    let views = site_broker_views(
        &all_cordoned,
        Some(NodeId(1)),
        &excluded,
        &std::collections::HashSet::new(),
    );
    assert!(views.is_empty());
    assert!(assign(&auto_topic(1, 1), &views, None, 0) == Ok(Vec::new()));
    assert!(
        placement_failure_message(1, &views)
            == "Unable to replicate the partition 1 time(s): All brokers are currently fenced, \
                or have all their log directories cordoned."
    );
}
