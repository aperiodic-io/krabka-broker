//! Table-driven coverage of the offline-replica projection: an unregistered
//! broker, a fenced broker, a broker without the request's listener, a replica
//! on a directory the registration no longer lists, a registration left with
//! no online directory at all, and the two "online" sentinels.

use {
    super::*,
    crate::test_support::{broker_registration, in_sync_partition},
    assert2::assert,
    krabka_metadata::{BrokerEndpoint, MetadataRecord, TopicRecord},
    uuid::Uuid,
};

/// The listener that the requests in these tests arrive on, and that every
/// registration lists unless a test says otherwise.
const LISTENER: &str = "PLAINTEXT";

fn dir(n: u128) -> Uuid { Uuid::from_u128(n) }

fn registration(node_id: NodeId, log_dirs: Vec<Uuid>) -> MetadataRecord {
    registration_on(node_id, log_dirs, LISTENER)
}

/// A registration whose only endpoint is on `listener`.
fn registration_on(node_id: NodeId, log_dirs: Vec<Uuid>, listener: &str) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        incarnation_id: Uuid::from_u128(u128::from(node_id.0)),
        host: format!("broker-{}", node_id.0),
        endpoints: vec![BrokerEndpoint {
            name: listener.to_owned(),
            host: format!("broker-{}", node_id.0),
            port: 9092,
            protocol: krabka_security::ListenerProtocol::Plaintext,
        }],
        log_dirs,
        ..broker_registration(krabka_raft::NodeId(node_id.0))
    })
}

fn partition(replicas: &[NodeId], directories: &[Uuid]) -> PartitionRecord {
    in_sync_partition()
        .leader_epoch(3)
        .replicas(replicas.to_vec())
        .directories(directories.to_vec())
        .call()
}

/// A broker registered with the log directories it reports online.
struct Registered {
    node_id: NodeId,
    online_dirs: Vec<Uuid>,
}

impl Registered {
    fn new(node_id: NodeId, online_dirs: Vec<Uuid>) -> Self {
        Self {
            node_id,
            online_dirs,
        }
    }
}

/// Image with `t-0` on `replicas`/`directories` and a registration for every
/// broker in `registrations`.
fn image(registrations: &[Registered], replicas: &[NodeId], directories: &[Uuid]) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    img.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: "t".into(),
        topic_id: Uuid::from_u128(0x70b1c),
        partitions: 1,
        replication_factor: i16::try_from(replicas.len()).unwrap(),
    }));
    for Registered {
        node_id,
        online_dirs,
    } in registrations
    {
        img.apply(&registration(*node_id, online_dirs.clone()));
    }
    img.apply(&MetadataRecord::V1Partition(partition(
        replicas,
        directories,
    )));
    img
}

#[derive(krabka_macros::FieldDefaults)]
struct ReplicaCaseSetup {
    #[default("healthy replicas")]
    name: &'static str,
    #[default(vec![Registered { node_id: NodeId(1), online_dirs: vec![dir(0x600d)] }, Registered { node_id: NodeId(2), online_dirs: vec![dir(0x600d)] }])]
    registrations: Vec<Registered>,
    #[default(vec![NodeId(1), NodeId(2)])]
    replicas: Vec<NodeId>,
    #[default(vec![dir(0x600d), dir(0x600d)])]
    directories: Vec<Uuid>,
    unavailable: Vec<NodeId>,
}

struct Case<E> {
    setup: ReplicaCaseSetup,
    expected: E,
}

#[bon::builder]
fn replica_case<E>(
    expected: E,
    #[builder(default = "healthy replicas")] name: &'static str,
    #[builder(default = vec ! [Registered :: new (NodeId (1) , vec ! [dir (0x600d)]) , Registered :: new (NodeId (2) , vec ! [dir (0x600d)])])]
    registrations: Vec<Registered>,
    #[builder(default = vec ! [NodeId (1) , NodeId (2)])] replicas: Vec<NodeId>,
    #[builder(default = vec ! [dir (0x600d) , dir (0x600d)])] directories: Vec<Uuid>,
    #[builder(default)] unavailable: Vec<NodeId>,
) -> Case<E> {
    let setup = ReplicaCaseSetup {
        name,
        registrations,
        replicas,
        directories,
        unavailable,
    };
    Case { setup, expected }
}

impl<E> Case<E> {
    fn image(&self) -> MetadataImage {
        image(
            &self.setup.registrations,
            &self.setup.replicas,
            &self.setup.directories,
        )
    }

    fn unavailable(&self) -> HashSet<u64> { self.setup.unavailable.iter().map(|id| id.0).collect() }
}

/// Keep the projection cases independent while sharing their image/lookup/assertion driver.
fn check_replica_cases<E: std::fmt::Debug + PartialEq>(
    cases: Vec<Case<E>>,
    project: impl Fn(&MetadataImage, &PartitionRecord, &HashSet<u64>, &str) -> E,
) {
    for case in cases {
        let img = case.image();
        let record = img.partition("t", 0).expect("partition in image");
        let unavailable: HashSet<u64> = case.unavailable();
        let actual = project(&img, record, &unavailable, LISTENER);
        assert!(actual == case.expected, "case {}", case.setup.name);
    }
}

#[test]
fn offline_replicas_matches_kafka_replica_state_rules() {
    let (good, bad) = (dir(0x600d), dir(0xbad));
    let first_broker_without_dirs = || {
        vec![
            Registered::new(NodeId(1), vec![]),
            Registered::new(NodeId(2), vec![good]),
        ]
    };
    let cases = vec![
        replica_case()
            .expected(vec![])
            .name("every replica registered, online dir, unfenced")
            .registrations(vec![
                Registered::new(NodeId(1), vec![good, bad]),
                Registered::new(NodeId(2), vec![good]),
            ])
            .call(),
        replica_case()
            .expected(vec![1])
            .name("replica on a dir the registration no longer lists")
            .directories(vec![bad, good])
            .call(),
        replica_case()
            .expected(vec![2])
            .name("fenced broker")
            .unavailable(vec![NodeId(2)])
            .call(),
        replica_case()
            .expected(vec![2])
            .name("unregistered broker")
            .registrations(vec![Registered::new(NodeId(1), vec![good])])
            .call(),
        replica_case()
            .expected(vec![])
            .name("unassigned directory id is online")
            .directories(vec![Uuid::nil(), Uuid::nil()])
            .call(),
        replica_case()
            .expected(vec![1])
            .name("registration whose last online dir was retired offlines its replicas")
            .registrations(first_broker_without_dirs())
            .directories(vec![bad, good])
            .call(),
        replica_case()
            .expected(vec![])
            .name("registration with no online dir keeps an unassigned replica online")
            .registrations(first_broker_without_dirs())
            .directories(vec![Uuid::nil(), good])
            .call(),
        replica_case()
            .expected(vec![])
            .name("missing directory slot is online")
            .directories(vec![])
            .call(),
        replica_case()
            .expected(vec![2, 1])
            .name("offline dir and fenced peer are both reported, in replica order")
            .replicas(vec![NodeId(2), NodeId(1)])
            .directories(vec![good, bad])
            .unavailable(vec![NodeId(2)])
            .call(),
    ];

    check_replica_cases(cases, offline_replicas);
}

/// A replica on a directory its broker no longer lists online neither leads
/// nor stays in the reported ISR -- and nothing else changes.
///
/// The first row is the one `kafka-topics --describe
/// --unavailable-partitions` opened this gap on: a sole replica on a failed
/// log directory. Apache Kafka 4.3.1 answers that shape `Leader: none
/// Replicas: 1 Isr:`, and it is the `Leader: none` and the empty ISR, not the
/// offline list, that the tool's two health filters read.
///
/// The last two rows are the boundary. A fenced broker and an unregistered
/// one are both reported offline, and both keep their ISR seat here, because
/// Kafka's `KRaftMetadataCache` passes the image's ISR through and krabka's
/// controller is able to shrink it for those two edges itself.
#[test]
fn a_replica_on_a_dead_log_dir_neither_leads_nor_stays_in_the_isr() {
    let (good, bad) = (dir(0x600d), dir(0xbad));
    let cases = vec![
        replica_case()
            .expected(availability(NO_LEADER_ID, vec![], vec![1]))
            .name("sole replica on a failed log dir")
            .registrations(vec![Registered::new(NodeId(1), vec![good])])
            .replicas(vec![NodeId(1)])
            .directories(vec![bad])
            .call(),
        replica_case()
            .expected(availability(NO_LEADER_ID, vec![2], vec![1]))
            .name("leader on a failed log dir, follower healthy")
            .directories(vec![bad, good])
            .call(),
        replica_case()
            .expected(availability(1, vec![1], vec![]))
            .name("sole replica on a directory nobody has assigned yet")
            .registrations(vec![Registered::new(NodeId(1), vec![good])])
            .replicas(vec![NodeId(1)])
            .directories(vec![Uuid::nil()])
            .call(),
        replica_case()
            .expected(availability(1, vec![1, 2], vec![2]))
            .name("fenced follower keeps its ISR seat")
            .unavailable(vec![NodeId(2)])
            .call(),
        replica_case()
            .expected(availability(1, vec![1, 2], vec![2]))
            .name("unregistered follower keeps its ISR seat")
            .registrations(vec![Registered::new(NodeId(1), vec![good])])
            .call(),
        replica_case()
            .expected(availability(1, vec![1, 2], vec![]))
            .name("healthy partition is untouched")
            .call(),
    ];

    check_replica_cases(cases, partition_availability);
}

/// Kafka's `KRaftMetadataCache` finds a leader's endpoint with
/// `getAliveEndpoint`, which is empty for a broker with no registration and
/// for one with no endpoint on the request's listener, and it reports a
/// replica on such a broker offline (`isReplicaOffline`). The leader is then
/// `-1`, and the ISR and the replica list are left as they are.
#[test]
fn a_broker_without_the_request_listener_is_offline_and_cannot_be_the_leader() {
    let good = dir(0x600d);
    // Node 1 lists `PLAINTEXT`, node 2 lists only `EXTERNAL`, and node 3 has
    // no registration.
    let mut img = image(
        &[
            Registered::new(NodeId(1), vec![good]),
            Registered::new(NodeId(2), vec![good]),
        ],
        &[NodeId(1), NodeId(2)],
        &[good, good],
    );
    img.apply(&registration_on(NodeId(2), vec![good], "EXTERNAL"));
    let cases = [
        (
            "the leader lists the listener and the follower does not",
            vec![1, 2],
            availability(1, vec![1, 2], vec![2]),
        ),
        (
            "the leader does not list the listener",
            vec![2, 1],
            availability(NO_LEADER_ID, vec![2, 1], vec![2]),
        ),
        (
            "the leader has no registration",
            vec![3, 1],
            availability(NO_LEADER_ID, vec![3, 1], vec![3]),
        ),
    ];

    for (name, replicas, expected) in cases {
        let mut img = img.clone();
        img.apply(&MetadataRecord::V1Partition(partition(
            &replicas.iter().copied().map(NodeId).collect::<Vec<_>>(),
            &[good, good],
        )));
        let record = img.partition("t", 0).expect("partition in image");

        let actual = partition_availability(&img, record, &HashSet::new(), LISTENER);

        assert!(actual == expected, "case {name}");
    }
    // The same partition on the other listener: node 2 is the one in place.
    let record = img.partition("t", 0).expect("partition in image");
    assert!(
        partition_availability(&img, record, &HashSet::new(), "EXTERNAL")
            == availability(NO_LEADER_ID, vec![1, 2], vec![1])
    );
}

/// One election case: the nodes reported unavailable and the nodes electable.
struct ElectableCase<'a> {
    name: &'a str,
    unavailable: Vec<u64>,
    expected: Vec<u64>,
}

/// The set an operator election may elect from, which every node computes the
/// same way so that a rotating `controllerId` cannot change the answer.
///
/// A registration is what makes a broker electable and the unavailable set is
/// what takes it back, including for a broker whose heartbeat this node has
/// never seen -- the case that made `ElectLeaders` depend on where it landed.
#[test]
fn electable_is_the_registered_brokers_the_unavailable_set_does_not_name() {
    let good = dir(0x600d);
    let img = image(
        &[
            Registered::new(NodeId(1), vec![good]),
            Registered::new(NodeId(2), vec![good]),
            Registered::new(NodeId(3), vec![good]),
        ],
        &[NodeId(1), NodeId(2)],
        &[good, good],
    );

    let cases: [ElectableCase<'_>; 4] = [
        ElectableCase {
            name: "nothing unavailable",
            unavailable: vec![],
            expected: vec![1, 2, 3],
        },
        ElectableCase {
            name: "one fenced broker",
            unavailable: vec![2],
            expected: vec![1, 3],
        },
        ElectableCase {
            name: "every broker unavailable",
            unavailable: vec![1, 2, 3],
            expected: vec![],
        },
        ElectableCase {
            name: "an unavailable broker that never registered",
            unavailable: vec![9],
            expected: vec![1, 2, 3],
        },
    ];

    for ElectableCase {
        name,
        unavailable,
        expected,
    } in cases
    {
        let actual = electable(&img, &unavailable.into_iter().collect());

        assert!(actual == expected.into_iter().collect(), "case {name}");
    }
}

/// A broker the image does not carry a registration for is not electable, even
/// though nothing reports it unavailable.
#[test]
fn an_unregistered_broker_is_never_electable() {
    let good = dir(0x600d);
    let img = image(
        &[Registered::new(NodeId(1), vec![good])],
        &[NodeId(1), NodeId(2)],
        &[good, good],
    );

    assert!(electable(&img, &HashSet::new()) == HashSet::from([1]));
}

/// One leaderless case: the replicas, the last-known ELR and the availability
/// that both APIs project.
struct LeaderlessCase<'a> {
    name: &'a str,
    replicas: Vec<u64>,
    last_known: Vec<u64>,
    expected: PartitionAvailability,
}

/// A partition the controller left without a leader keeps its last leader in
/// its record and publishes a one-member last-known ELR that names it (see
/// `crate::elr::state::is_leaderless`). Both APIs project it the way Kafka's
/// `leader = -1` reads: no leader, and the last leader out of the ISR. The
/// eligible set beside it does not change the answer, and a last-known ELR
/// that does not name the leader is not a marker.
#[test]
fn a_leaderless_partition_projects_no_leader_and_an_isr_without_its_last_leader() {
    let good = dir(0x600d);
    let cases: [LeaderlessCase<'_>; 5] = [
        LeaderlessCase {
            name: "sole replica, last-known ELR names it",
            replicas: vec![1],
            last_known: vec![1],
            expected: availability(NO_LEADER_ID, vec![], vec![]),
        },
        LeaderlessCase {
            name: "an ISR member other than the last leader stays",
            replicas: vec![1, 2],
            last_known: vec![1],
            expected: availability(NO_LEADER_ID, vec![2], vec![]),
        },
        LeaderlessCase {
            name: "no last-known ELR",
            replicas: vec![1, 2],
            last_known: vec![],
            expected: availability(1, vec![1, 2], vec![]),
        },
        LeaderlessCase {
            name: "a last-known ELR that names another replica is no marker",
            replicas: vec![1, 2],
            last_known: vec![2],
            expected: availability(1, vec![1, 2], vec![]),
        },
        LeaderlessCase {
            name: "a multi-member last-known ELR is no marker",
            replicas: vec![1, 2],
            last_known: vec![1, 2],
            expected: availability(1, vec![1, 2], vec![]),
        },
    ];
    for LeaderlessCase {
        name,
        replicas,
        last_known,
        expected,
    } in cases
    {
        let mut img = image(
            &[
                Registered::new(NodeId(1), vec![good]),
                Registered::new(NodeId(2), vec![good]),
            ],
            &replicas.iter().copied().map(NodeId).collect::<Vec<_>>(),
            &vec![good; replicas.len()],
        );
        img.apply(&MetadataRecord::V1PartitionElr(
            krabka_metadata::PartitionElrRecord {
                topic: "t".into(),
                partition: 0,
                eligible_leader_replicas: vec![NodeId(2)],
                last_known_elr: last_known.into_iter().map(NodeId).collect(),
            },
        ));
        let record = img.partition("t", 0).expect("partition in image");

        assert!(
            partition_availability(&img, record, &HashSet::new(), LISTENER) == expected,
            "case {name}"
        );
    }
}

fn availability(
    leader_id: i32,
    isr_nodes: Vec<i32>,
    offline_replicas: Vec<i32>,
) -> PartitionAvailability {
    PartitionAvailability {
        leader_id,
        isr_nodes,
        offline_replicas,
    }
}
