//! End-to-end test for the `BrokerHeartbeat` wire handler against a live
//! broker, plus the request builder and the leader wait it needs.
//!
//! It pins the response shape a controller leader returns for a registered,
//! caught-up broker.

use std::sync::Arc;

use assert2::{assert, check};
use krabka_protocol::{
    owned::broker_heartbeat_response::BrokerHeartbeatResponse,
    primitives::uuid::Uuid as ProtocolUuid,
};

use super::*;
use crate::{
    broker::Broker,
    codes,
    test_support::{peer, start_broker_with_authorizer as start_broker, test_ctx},
};

fn request(
    broker_epoch: i64,
    current_metadata_offset: i64,
    offline_log_dirs: Vec<uuid::Uuid>,
) -> BrokerHeartbeatRequest {
    BrokerHeartbeatRequest {
        broker_id: 1,
        broker_epoch,
        current_metadata_offset,
        want_fence: false,
        want_shut_down: false,
        offline_log_dirs: offline_log_dirs
            .into_iter()
            .map(|u| ProtocolUuid(u.into_bytes()))
            .collect(),
        cordoned_log_dirs: None,
        ..Default::default()
    }
}

crate::test_support::context_helper!(client_id = "broker-heartbeat-test");

/// Every heartbeat is authorized against its own principal, on the
/// controller listener too, as Kafka's
/// `ControllerApis.handleBrokerHeartBeatRequest` does (#684). A principal
/// without `ClusterAction` gets `CLUSTER_AUTHORIZATION_FAILED`, and one with
/// it gets the heartbeat answer.
#[tokio::test]
async fn every_heartbeat_needs_cluster_action() {
    broker_fixture!(
        (broker_handle, _dir, broker),
        start_broker(Arc::new(crate::test_support::GrantsInPrincipalName)),
        controller_leader
    );
    let peer = peer();
    let version = krabka_protocol::owned::broker_heartbeat_request::MAX_VERSION;
    let broker_epoch = broker
        .controller
        .current_image()
        .broker_epoch(NodeId(1))
        .expect("broker registration should be applied");
    let req = request(broker_epoch, broker_epoch, vec![]);

    let cases = [
        ("none", codes::CLUSTER_AUTHORIZATION_FAILED),
        (
            "Cluster:Alter+Cluster:Describe",
            codes::CLUSTER_AUTHORIZATION_FAILED,
        ),
        ("Cluster:ClusterAction", codes::NONE),
    ];
    for (name, error_code) in cases {
        let principal = crate::test_support::principal(name);
        let ctx = test_context(&principal, &peer);
        let response = handle(&broker, req.clone(), version, &ctx)
            .await
            .expect("BrokerHeartbeat handler");
        assert!(response.error_code == error_code, "{name}: {response:?}");
    }

    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_leader_success_preserves_response_shape() {
    broker_fixture!(
        (broker_handle, _dir, broker),
        allow_all,
        context(ctx, "ANONYMOUS"),
        controller_leader
    );
    let version = krabka_protocol::owned::broker_heartbeat_request::MAX_VERSION;
    let image = broker.controller.current_image();
    let broker_epoch = image
        .broker_epoch(NodeId(1))
        .expect("broker registration should be applied");
    let req = request(broker_epoch, broker_epoch, vec![]);

    let resp = handle(&broker, req, version, &ctx)
        .await
        .expect("BrokerHeartbeat handler");

    let expected = unthrottled_wire!(BrokerHeartbeatResponse {
        error_code: codes::NONE,
        is_caught_up: true,
        is_fenced: false,
        should_shut_down: false,
    });
    assert!(resp == expected, "{resp:?}");

    broker_handle.shutdown().await;
}

/// What one heartbeat was answered with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Answer {
    error_code: i16,
    is_caught_up: bool,
    is_fenced: bool,
    should_shut_down: bool,
}

const fn answer(is_fenced: bool, should_shut_down: bool) -> Answer {
    Answer {
        error_code: codes::NONE,
        is_caught_up: true,
        is_fenced,
        should_shut_down,
    }
}

/// A new registration of `node`: fenced, as Kafka's `RegisterBrokerRecord`
/// defaults `Fenced` to true, and at no epoch yet, for the controller to stamp
/// the offset it commits at.
fn new_registration(node: u64) -> krabka_metadata::MetadataRecord {
    krabka_metadata::MetadataRecord::V1BrokerRegistration(
        krabka_metadata::BrokerRegistrationRecord {
            fenced: true,
            broker_epoch: -1,
            incarnation_id: uuid::Uuid::from_u128(u128::from(node)),
            port: 19_090 + u16::try_from(node).expect("a small node id"),
            log_dirs: vec![uuid::Uuid::from_u128(1000 + u128::from(node))],
            ..crate::test_support::broker_registration(krabka_raft::NodeId(node))
        },
    )
}

/// A controller, node 1, with brokers 2 and 3 registered and one topic `t`:
/// partition 0 led by 2 with the ISR `[2, 3, 1]`, partition 1 led by 3 with
/// the ISR `[3, 2]`.
struct Cluster {
    handle: crate::BrokerHandle,
    _dir: tempfile::TempDir,
    broker: Arc<Broker>,
}

impl Cluster {
    async fn start() -> Self {
        use krabka_metadata::{LeaderEpoch, MetadataRecord, PartitionRecord, TopicRecord};

        broker_fixture!((handle, dir, broker), allow_all, controller_leader);
        let partition = |index: i32, leader: u64, isr: &[u64]| {
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "t".into(),
                partition: index,
                leader: NodeId(leader),
                replicas: isr.iter().copied().map(NodeId).collect(),
                isr: isr.iter().copied().map(NodeId).collect(),
                leader_epoch: LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![uuid::Uuid::nil(); isr.len()],
                partition_epoch: 0,
            })
        };
        broker
            .controller
            .submit_change(vec![
                new_registration(2),
                new_registration(3),
                MetadataRecord::V1Topic(TopicRecord {
                    name: "t".into(),
                    topic_id: uuid::Uuid::from_u128(0x7),
                    partitions: 2,
                    replication_factor: 2,
                }),
                partition(0, 2, &[2, 3, 1]),
                partition(1, 3, &[3, 2]),
            ])
            .await
            .expect("seed the cluster");
        Self {
            handle,
            _dir: dir,
            broker,
        }
    }

    fn epoch(&self, node: u64) -> i64 {
        self.broker
            .controller
            .current_image()
            .broker_epoch(NodeId(node))
            .expect("a registered broker")
    }

    async fn heartbeat(&self, broker_id: i32, epoch: i64, offset: i64, shut_down: bool) -> Answer {
        test_ctx!(ctx, "ANONYMOUS");
        let version = krabka_protocol::owned::broker_heartbeat_request::MAX_VERSION;
        let req = BrokerHeartbeatRequest {
            broker_id,
            broker_epoch: epoch,
            current_metadata_offset: offset,
            want_shut_down: shut_down,
            ..Default::default()
        };
        let response = handle(&self.broker, req, version, &ctx)
            .await
            .expect("BrokerHeartbeat handler");
        Answer {
            error_code: response.error_code,
            is_caught_up: response.is_caught_up,
            is_fenced: response.is_fenced,
            should_shut_down: response.should_shut_down,
        }
    }

    /// The leader and ISR of partition `index` of `t`.
    fn leader_and_isr(&self, index: i32) -> (u64, Vec<u64>) {
        let image = self.broker.controller.current_image();
        let partition = image.partition("t", index).expect("a seeded partition");
        (
            partition.leader.0,
            partition.isr.iter().map(|node| node.0).collect(),
        )
    }

    fn applied(&self) -> i64 {
        self.broker.controller.current_metadata_offset()
    }

    /// The `(fenced, in_controlled_shutdown)` flags of `node`'s registration,
    /// and whether it still holds the epoch it registered at.
    fn registration(&self, node: u64, epoch: i64) -> (bool, bool, bool) {
        let image = self.broker.controller.current_image();
        let registration = image.broker(NodeId(node)).expect("a registered broker");
        (
            registration.fenced,
            registration.in_controlled_shutdown,
            registration.broker_epoch == epoch,
        )
    }
}

/// krabka-io/krabka-broker#824: `BrokerHeartbeat` follows Kafka's
/// `ReplicationControlManager.processBrokerHeartbeat`.
///
/// A heartbeat from a broker id with no registration, or with a stale epoch,
/// is `STALE_BROKER_EPOCH` (`ClusterControlManager.checkBrokerEpoch`). A
/// broker that asks to shut down while it leads a partition enters controlled
/// shutdown: it leaves every ISR, not only the ones it leads, and its
/// leaderships move. It may stop only once every active broker reports a
/// metadata offset past the records that did that
/// (`BrokerHeartbeatManager.calculateNextBrokerState`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_controlled_shutdown_drains_the_isrs_and_waits_for_active_brokers() {
    let cluster = Cluster::start().await;
    let epoch_2 = cluster.epoch(2);
    let epoch_3 = cluster.epoch(3);
    let mut answers = Vec::new();
    let mut expected = Vec::new();
    // Broker 2's registration after each step: Kafka's
    // `BrokerRegistrationChangeRecord`s move its fence and its controlled
    // shutdown, at the epoch it registered at.
    let mut registrations = vec![("registered", cluster.registration(2, epoch_2))];
    let mut expected_registrations = vec![("registered", (true, false, true))];

    let stale = Answer {
        error_code: codes::STALE_BROKER_EPOCH,
        ..success_response_default()
    };
    expected.push(("a broker id with no registration", stale));
    answers.push((
        "a broker id with no registration",
        cluster
            .heartbeat(9, epoch_2, cluster.applied(), false)
            .await,
    ));
    expected.push(("a stale broker epoch", stale));
    answers.push((
        "a stale broker epoch",
        cluster
            .heartbeat(2, epoch_2 - 1, cluster.applied(), false)
            .await,
    ));

    expected.push(("node 1 unfences", answer(false, false)));
    answers.push((
        "node 1 unfences",
        cluster
            .heartbeat(1, cluster.epoch(1), cluster.applied(), false)
            .await,
    ));
    expected.push(("broker 3 unfences", answer(false, false)));
    answers.push((
        "broker 3 unfences",
        cluster
            .heartbeat(3, epoch_3, cluster.applied(), false)
            .await,
    ));
    expected.push(("broker 2 unfences", answer(false, false)));
    answers.push((
        "broker 2 unfences",
        cluster
            .heartbeat(2, epoch_2, cluster.applied(), false)
            .await,
    ));
    registrations.push(("unfenced", cluster.registration(2, epoch_2)));
    expected_registrations.push(("unfenced", (false, false, true)));

    expected.push(("broker 2 asks to shut down", answer(false, false)));
    answers.push((
        "broker 2 asks to shut down",
        cluster.heartbeat(2, epoch_2, cluster.applied(), true).await,
    ));
    registrations.push(("in controlled shutdown", cluster.registration(2, epoch_2)));
    expected_registrations.push(("in controlled shutdown", (false, true, true)));
    let drained = cluster.applied();
    let (leader_0, isr_0) = cluster.leader_and_isr(0);
    let (leader_1, isr_1) = cluster.leader_and_isr(1);
    check!(
        leader_0 != 2 && !isr_0.contains(&2),
        "partition 0: {leader_0} {isr_0:?}"
    );
    check!(
        (leader_1, isr_1) == (3, vec![3]),
        "broker 2 left the ISR it only follows"
    );

    // Broker 3 is still behind the drain, and node 1, the controller's own
    // broker, has caught up to it.
    expected.push(("node 1 at the drain", answer(false, false)));
    answers.push((
        "node 1 at the drain",
        cluster.heartbeat(1, cluster.epoch(1), drained, false).await,
    ));
    expected.push(("broker 3 behind the drain", answer(false, false)));
    answers.push((
        "broker 3 behind the drain",
        cluster.heartbeat(3, epoch_3, epoch_3, false).await,
    ));
    expected.push(("broker 2 waits for broker 3", answer(false, false)));
    answers.push((
        "broker 2 waits for broker 3",
        cluster.heartbeat(2, epoch_2, cluster.applied(), true).await,
    ));

    expected.push(("broker 3 at the drain", answer(false, false)));
    answers.push((
        "broker 3 at the drain",
        cluster
            .heartbeat(3, epoch_3, cluster.applied(), false)
            .await,
    ));
    expected.push(("broker 2 may shut down", answer(true, true)));
    answers.push((
        "broker 2 may shut down",
        cluster.heartbeat(2, epoch_2, cluster.applied(), true).await,
    ));
    registrations.push(("shut down", cluster.registration(2, epoch_2)));
    expected_registrations.push(("shut down", (true, true, true)));

    check!(answers == expected);
    check!(registrations == expected_registrations);
    cluster.handle.shutdown().await;
}

/// Codex review of krabka-io/krabka-broker#1166: a broker that crashes in
/// controlled shutdown and registers again, without passing through
/// `BrokerRegistration`'s `replace_incarnation`, leaves the controller's
/// registry in controlled shutdown. Its new registration is fenced, and Kafka's
/// `BrokerHeartbeatManager.register` takes a fenced broker out of controlled
/// shutdown, so its first caught-up heartbeat unfences it rather than holding
/// it in controlled shutdown or telling it to shut down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_from_controlled_shutdown_starts_fenced() {
    let cluster = Cluster::start().await;
    let epoch_2 = cluster.epoch(2);
    let mut answers = Vec::new();
    for node in [1, 3, 2] {
        answers.push((
            "unfences",
            cluster
                .heartbeat(
                    i32::try_from(node).expect("a small node id"),
                    cluster.epoch(node),
                    cluster.applied(),
                    false,
                )
                .await,
        ));
    }
    answers.push((
        "enters controlled shutdown",
        cluster.heartbeat(2, epoch_2, cluster.applied(), true).await,
    ));
    // Broker 2's self-registration after the crash.
    cluster
        .broker
        .controller
        .submit_change(vec![new_registration(2)])
        .await
        .expect("broker 2 registers again");
    let restarted = cluster.epoch(2);
    let registered = cluster.registration(2, restarted);
    answers.push((
        "the restart unfences",
        cluster
            .heartbeat(2, restarted, cluster.applied(), false)
            .await,
    ));

    check!(
        answers
            == vec![
                ("unfences", answer(false, false)),
                ("unfences", answer(false, false)),
                ("unfences", answer(false, false)),
                ("enters controlled shutdown", answer(false, false)),
                ("the restart unfences", answer(false, false)),
            ]
    );
    check!(restarted > epoch_2);
    check!(
        [registered, cluster.registration(2, restarted)]
            == [(true, false, true), (false, false, true)]
    );
    cluster.handle.shutdown().await;
}

/// `handleBrokerUnfenced`: the heartbeat that unfences a broker also elects
/// again for every partition that has no leader, with that broker as an
/// acceptable leader (`generateLeaderAndIsrUpdates` over
/// `partitionsWithNoLeader`).
///
/// Partition 1 is the state an unclean restart leaves for the only ISR member:
/// broker 2 is the last leader, nothing is eligible, and the last-known ELR
/// names it (`PartitionChangeBuilder.canElectLastKnownLeader`). Its unfence
/// gives it the partition back as an unclean leader -- singleton ISR, a new
/// leader epoch, `RECOVERING` -- and clears both sets, all in the records of
/// the heartbeat. Partition 0, which has a leader, is left alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unfencing_broker_takes_back_a_partition_with_no_leader() {
    use krabka_metadata::{LeaderEpoch, MetadataRecord, PartitionElrRecord, PartitionRecord};

    let cluster = Cluster::start().await;
    crate::test_support::finalize_elr_version_on(&cluster.broker).await;
    let epoch_2 = cluster.epoch(2);
    let no_leader = PartitionRecord {
        isr: vec![NodeId(2)],
        leader_epoch: LeaderEpoch(4),
        directories: vec![uuid::Uuid::nil(); 2],
        ..crate::handlers::test_support::replicated_partition(
            crate::handlers::test_support::ReplicatedPartitionSetup {
                topic: "t",
                partition: krabka_ids::PartitionIndex(1),
                leader: NodeId(2),
                replicas: &[NodeId(2), NodeId(3)],
            },
        )
    };
    cluster
        .broker
        .controller
        .submit_change(vec![
            MetadataRecord::V1Partition(no_leader.clone()),
            MetadataRecord::V1PartitionElr(PartitionElrRecord {
                topic: "t".into(),
                partition: 1,
                eligible_leader_replicas: vec![],
                last_known_elr: vec![NodeId(2)],
            }),
        ])
        .await
        .expect("seed a partition with no leader");
    let before = cluster.broker.controller.current_image();
    check!(crate::elr::state::is_leaderless(
        &before,
        before.partition("t", 1).expect("partition 1")
    ));

    let answer_2 = cluster
        .heartbeat(2, epoch_2, cluster.applied(), false)
        .await;

    check!(answer_2 == answer(false, false));
    let image = cluster.broker.controller.current_image();
    let elected = image.partition("t", 1).expect("partition 1");
    check!(
        *elected
            == PartitionRecord {
                leader: NodeId(2),
                isr: vec![NodeId(2)],
                leader_epoch: LeaderEpoch(5),
                partition_epoch: elected.partition_epoch,
                ..no_leader
            }
    );
    check!(elected.partition_epoch > 0);
    check!(
        crate::elr::TopicElr::of_topic(&image, "t").partition(1)
            == crate::elr::state::PartitionElr::default()
    );
    check!(image.leader_recovery_state("t", 1) == krabka_metadata::LeaderRecoveryState::Recovering);
    check!(cluster.leader_and_isr(0) == (2, vec![2, 3, 1]));
    cluster.handle.shutdown().await;
}

/// The response fields an error answer leaves at their schema defaults.
fn success_response_default() -> Answer {
    let defaults = BrokerHeartbeatResponse::default();
    Answer {
        error_code: defaults.error_code,
        is_caught_up: defaults.is_caught_up,
        is_fenced: defaults.is_fenced,
        should_shut_down: defaults.should_shut_down,
    }
}

/// KIP-1066: `processBrokerHeartbeat` stores a heartbeat's cordoned directories
/// on the registration only from `metadata.version` `4.3-IV0`, only once the
/// broker sends a set, and only when the set changed.
#[test]
fn a_heartbeat_stores_its_cordoned_dirs_from_4_3_iv0() {
    let dir = |n: u128| uuid::Uuid::from_u128(n);
    let registration = krabka_metadata::BrokerRegistrationRecord {
        cordoned_log_dirs: Some(vec![dir(1)]),
        broker_epoch: 5,
        incarnation_id: dir(2),
        log_dirs: vec![dir(1), dir(2)],
        ..crate::test_support::broker_registration(krabka_raft::NodeId(2))
    };
    let image_at = |level: i16| {
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        image.apply(&krabka_metadata::MetadataRecord::V1BrokerRegistration(
            registration.clone(),
        ));
        image.apply(&krabka_metadata::MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: crate::features::METADATA_VERSION.into(),
                level,
            },
        ));
        image
    };
    let heartbeat = |cordoned: Option<&[u128]>| BrokerHeartbeatRequest {
        broker_id: 2,
        broker_epoch: 5,
        cordoned_log_dirs: cordoned.map(|ids| {
            ids.iter()
                .map(|id| ProtocolUuid(*dir(*id).as_bytes()))
                .collect()
        }),
        ..Default::default()
    };
    let stored = |ids: &[u128]| {
        Some(krabka_metadata::MetadataRecord::V1BrokerRegistrationChange(
            krabka_metadata::BrokerRegistrationChangeRecord {
                node_id: NodeId(2),
                broker_epoch: 5,
                fenced: krabka_metadata::FencingChange::None,
                in_controlled_shutdown: false,
                log_dirs: vec![],
                cordoned_log_dirs: Some(ids.iter().copied().map(dir).collect()),
            },
        ))
    };
    let cases: [(&str, i16, Option<&[u128]>, _); 5] = [
        (
            "every directory cordoned",
            30,
            Some(&[1, 2]),
            stored(&[1, 2]),
        ),
        ("uncordoned", 30, Some(&[]), stored(&[])),
        ("unchanged", 30, Some(&[1]), None),
        ("not caught up yet", 30, None, None),
        ("below 4.3-IV0", 29, Some(&[1, 2]), None),
    ];
    for (label, level, cordoned, want) in cases {
        check!(
            cordoned_dirs_change(&image_at(level), NodeId(2), &heartbeat(cordoned)) == want,
            "{label}"
        );
    }
}

/// krabka-io/krabka-broker#1009: the records each heartbeat transition
/// writes, as `ReplicationControlManager.processBrokerHeartbeat` writes them.
/// `handleBrokerFenced` (for `FENCED` and `SHUTDOWN_NOW`) puts the partition
/// changes before a `BrokerRegistrationChangeRecord` with `Fenced = 1`;
/// `handleBrokerUnfenced` writes `Fenced = -1`, and
/// `handleBrokerInControlledShutdown` writes `InControlledShutdown = 1` ahead
/// of the partition changes, and not at all when the registration already
/// holds it. Every change names the broker's registered epoch.
#[test]
fn each_transition_writes_kafkas_registration_change() {
    use BrokerControlState::{ControlledShutdown, Fenced, ShutdownNow, Unfenced};
    use krabka_metadata::{
        BrokerRegistrationChangeRecord, FencingChange, MetadataImage, MetadataRecord, TopicRecord,
    };

    let registered = |fenced: bool, in_controlled_shutdown: bool| {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        let MetadataRecord::V1BrokerRegistration(registration) = new_registration(2) else {
            unreachable!("new_registration builds a registration");
        };
        image.apply(&MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                broker_epoch: 40,
                fenced,
                in_controlled_shutdown,
                ..registration
            },
        ));
        image
    };
    let change = |fenced: FencingChange, in_controlled_shutdown: bool| {
        MetadataRecord::V1BrokerRegistrationChange(BrokerRegistrationChangeRecord {
            fenced,
            in_controlled_shutdown,
            ..BrokerRegistrationChangeRecord::no_change(NodeId(2), 40)
        })
    };
    // Stands in for the partition changes that take broker 2 out of its ISRs,
    // which the handler passes in for a fence, a shutdown and a controlled
    // shutdown.
    let leave = MetadataRecord::V1Topic(TopicRecord {
        name: "leave".into(),
        topic_id: uuid::Uuid::from_u128(9),
        partitions: 1,
        replication_factor: 1,
    });
    // Stands in for the elections an unfence makes possible, which the
    // handler passes in for that transition alone.
    let elect = MetadataRecord::V1Topic(TopicRecord {
        name: "elect".into(),
        topic_id: uuid::Uuid::from_u128(10),
        partitions: 1,
        replication_factor: 1,
    });

    let cases = [
        (
            "fenced -> unfenced",
            registered(true, false),
            Fenced,
            Unfenced,
            vec![],
            vec![change(FencingChange::Unfence, false)],
        ),
        // `handleBrokerUnfenced` writes the registration change first and
        // then the elections over `partitionsWithNoLeader`.
        (
            "fenced -> unfenced elects for the partitions with no leader",
            registered(true, false),
            Fenced,
            Unfenced,
            vec![elect.clone()],
            vec![change(FencingChange::Unfence, false), elect.clone()],
        ),
        (
            "fenced stays fenced",
            registered(true, false),
            Fenced,
            Fenced,
            vec![leave.clone()],
            vec![leave.clone()],
        ),
        (
            "fenced -> shutdown now",
            registered(true, false),
            Fenced,
            ShutdownNow,
            vec![leave.clone()],
            vec![leave.clone()],
        ),
        (
            "unfenced -> fenced",
            registered(false, false),
            Unfenced,
            Fenced,
            vec![leave.clone()],
            vec![leave.clone(), change(FencingChange::Fence, false)],
        ),
        (
            "unfenced -> shutdown now",
            registered(false, false),
            Unfenced,
            ShutdownNow,
            vec![leave.clone()],
            vec![leave.clone(), change(FencingChange::Fence, false)],
        ),
        (
            "unfenced -> controlled shutdown",
            registered(false, false),
            Unfenced,
            ControlledShutdown,
            vec![leave.clone()],
            vec![change(FencingChange::None, true), leave.clone()],
        ),
        (
            "controlled shutdown already on the registration",
            registered(false, true),
            Unfenced,
            ControlledShutdown,
            vec![leave.clone()],
            vec![leave.clone()],
        ),
        (
            "controlled shutdown -> shutdown now",
            registered(false, true),
            ControlledShutdown,
            ShutdownNow,
            vec![leave.clone()],
            vec![leave.clone(), change(FencingChange::Fence, false)],
        ),
    ];
    for (what, image, current, next, leaving, want) in cases {
        check!(
            transition_records(&image, NodeId(2), current, next, leaving) == want,
            "{what}"
        );
    }
}
