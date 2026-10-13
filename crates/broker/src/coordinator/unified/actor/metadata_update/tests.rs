//! Tests for the metadata refresh that a metadata update asks of a consumer
//! group, through the group actor's mailbox.

use {
    crate::coordinator::unified::{
        GroupCoordinator,
        actor::{
            GroupActorHandle, GroupActorMessage,
            test_support::{
                decode_assignment,
                rpc::{self, consumer_request_as_client as heartbeat},
                seed_and_upgrade,
            },
        },
        config::{ConsumerGroupMigrationPolicy, NextGenConfig},
        offsets_log::fake::InMemoryOffsetsLog,
        reconciler::ReconcileInput,
        share::config::ShareGroupConfig,
        streams::config::StreamsGroupConfig,
        test_support::{
            SwitchableMetadata, assignment_interval_fixture, consumer_join_request,
            make_coord_with_metadata, proto_uuid, snapshot_of,
        },
    },
    assert2::{assert, check},
    krabka_protocol::owned::{
        common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions as AssignedTopic,
        consumer_group_heartbeat_request::{
            ConsumerGroupHeartbeatRequest, TopicPartitions as OwnedTopic,
        },
        consumer_group_heartbeat_response::{Assignment, ConsumerGroupHeartbeatResponse},
        consumer_protocol_assignment::{ConsumerProtocolAssignment, TopicPartition},
    },
    std::sync::Arc,
};

async fn metadata_update(handle: &GroupActorHandle, topics: &[&str]) {
    handle
        .tx
        .send(GroupActorMessage::MetadataUpdate {
            topics: topics.iter().map(|topic| (*topic).to_string()).collect(),
        })
        .await
        .unwrap();
}

/// The first heartbeat of member `m1`, which subscribes to `orders`.
fn join() -> ConsumerGroupHeartbeatRequest { consumer_join_request().topics(&["orders"]).call() }

/// A later heartbeat of `m1`, which sends only what changed, as Kafka's
/// consumer does: the partitions it owns when it acknowledges them, else
/// nothing.
fn keepalive(member_epoch: i32, owned: Option<Vec<OwnedTopic>>) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: "m1".into(),
        member_epoch,
        rebalance_timeout_ms: -1,
        topic_partitions: owned,
        ..Default::default()
    }
}

/// Acknowledges the partitions of `joined`, so that `m1` owns them.
async fn acknowledge(handle: &GroupActorHandle, joined: &ConsumerGroupHeartbeatResponse) {
    let owned: Vec<OwnedTopic> = joined
        .assignment
        .iter()
        .flat_map(|assignment| &assignment.topic_partitions)
        .map(|topic| OwnedTopic {
            topic_id: topic.topic_id,
            partitions: topic.partitions.clone(),
            ..Default::default()
        })
        .collect();
    if !owned.is_empty() {
        let acknowledged = heartbeat(handle, keepalive(joined.member_epoch, Some(owned))).await;
        assert!(acknowledged.error_code == 0, "{acknowledged:?}");
    }
}

/// The partitions of a topic that is named by the byte its topic id repeats.
struct TopicByte {
    byte: u8,
    partitions: Vec<i32>,
}

/// The answer to `m1` at `member_epoch`. `assignment` names each topic by the
/// byte that its topic id repeats.
fn answer(member_epoch: i32, assignment: Option<Vec<TopicByte>>) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        member_id: Some("m1".into()),
        member_epoch,
        heartbeat_interval_ms: 5_000,
        assignment: assignment.map(|topics| Assignment {
            topic_partitions: topics
                .into_iter()
                .map(|TopicByte { byte, partitions }| AssignedTopic {
                    topic_id: proto_uuid(byte),
                    partitions,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

struct Row {
    name: &'static str,
    /// The metadata when `m1` joins.
    before: ReconcileInput,
    /// The metadata when the update arrives.
    after: ReconcileInput,
    /// The topics that the update names.
    update: &'static [&'static str],
    /// The answer to the first heartbeat after the update.
    expected: ConsumerGroupHeartbeatResponse,
}

fn grows_to_three(name: &'static str, before: ReconcileInput) -> Row {
    Row {
        name,
        before,
        after: snapshot_of(&[("orders", 1, 3)]),
        update: &["orders"],
        expected: answer(
            3,
            Some(vec![TopicByte {
                byte: 1,
                partitions: vec![0, 1, 2],
            }]),
        ),
    }
}

/// Kafka's `GroupMetadataManager.onMetadataUpdate` requests a metadata
/// refresh of every group that subscribes to a created, changed or deleted
/// topic. The next heartbeat of the group computes the metadata hash again
/// (`hasMetadataExpired`, `updateSubscriptionMetadata`), and a new hash bumps
/// the group epoch and brings a new target assignment. No clock moves between
/// the heartbeats: Kafka 4.3.1 has no refresh interval to wait out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_heartbeat_after_a_metadata_update_refreshes_the_assignment() {
    let rows = [
        grows_to_three("the subscribed topic is created", snapshot_of(&[])),
        grows_to_three(
            "the subscribed topic grows",
            snapshot_of(&[("orders", 1, 1)]),
        ),
        // The target loses the partitions. Kafka's `CurrentAssignmentBuilder`
        // keeps the member at epoch 2 until it acknowledges the revocation, and
        // its heartbeat answer already carries the smaller assignment.
        Row {
            name: "the subscribed topic is deleted",
            before: snapshot_of(&[("orders", 1, 2)]),
            after: snapshot_of(&[]),
            update: &["orders"],
            expected: answer(2, Some(vec![])),
        },
        // The update names `payments` only. The group does not read the
        // metadata again, so it does not see that `orders` grew in the same
        // image either. Kafka also refreshes only the groups that
        // `groupsSubscribedToTopic` names.
        Row {
            name: "only a topic that the group does not subscribe to changes",
            before: snapshot_of(&[("orders", 1, 1)]),
            after: snapshot_of(&[("orders", 1, 2), ("payments", 2, 1)]),
            update: &["payments"],
            expected: answer(2, None),
        },
        // A new partition leader changes the topic but not its hash, and Kafka
        // bumps the group epoch only for a new hash.
        Row {
            name: "the subscribed topic changes and keeps its hash",
            before: snapshot_of(&[("orders", 1, 2)]),
            after: snapshot_of(&[("orders", 1, 2)]),
            update: &["orders"],
            expected: answer(2, None),
        },
    ];

    let mut answers = Vec::new();
    let mut expected = Vec::new();
    for row in rows {
        let metadata = SwitchableMetadata::new(row.before);
        let coordinator = make_coord_with_metadata(metadata.clone());
        let handle = coordinator.get_or_create_consumer("g");
        let joined = heartbeat(&handle, join()).await;
        acknowledge(&handle, &joined).await;

        metadata.set(row.after);
        metadata_update(&handle, row.update).await;
        let refreshed = heartbeat(&handle, keepalive(joined.member_epoch, None)).await;

        answers.push((row.name, refreshed));
        expected.push((row.name, row.expected));
    }
    check!(answers == expected);
}

/// Kafka refreshes a loaded group's metadata at its first heartbeat and
/// compares the hash with the `MetadataHash` that the group's last
/// `ConsumerGroupMetadataValue` stored. An unchanged topic keeps the epoch
/// and the target, and a topic that grew while no coordinator held the
/// group, or after the load, bumps the epoch and reaches the member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_loaded_group_refreshes_its_metadata_at_the_first_heartbeat() {
    // (name, the metadata when the group loads, a change after the load, the
    // answer to the first heartbeat)
    let rows = [
        (
            "the topic is as it was",
            snapshot_of(&[("orders", 1, 2)]),
            None,
            answer(2, None),
        ),
        (
            "the topic grew while no coordinator held the group",
            snapshot_of(&[("orders", 1, 3)]),
            None,
            answer(
                3,
                Some(vec![TopicByte {
                    byte: 1,
                    partitions: vec![0, 1, 2],
                }]),
            ),
        ),
        (
            "the topic grew after the load",
            snapshot_of(&[("orders", 1, 2)]),
            Some(snapshot_of(&[("orders", 1, 3)])),
            answer(
                3,
                Some(vec![TopicByte {
                    byte: 1,
                    partitions: vec![0, 1, 2],
                }]),
            ),
        ),
    ];

    let mut answers = Vec::new();
    let mut expected = Vec::new();
    for (name, at_load, after_load, wanted) in rows {
        let metadata = SwitchableMetadata::new(snapshot_of(&[("orders", 1, 2)]));
        let previous = make_coord_with_metadata(metadata.clone());
        let previous_handle = previous.get_or_create_consumer("g");
        let joined = heartbeat(&previous_handle, join()).await;
        acknowledge(&previous_handle, &joined).await;
        let seed = previous.cached_seed("g").expect("the group's records");

        metadata.set(at_load);
        let loaded = make_coord_with_metadata(metadata.clone());
        let handle = loaded.get_or_create_consumer("g");
        handle.tx.send(GroupActorMessage::Seed(seed)).await.unwrap();
        // The actor reads the metadata when it applies the seed, so wait for
        // that before the metadata changes.
        let described = rpc::begin(&handle, |reply| GroupActorMessage::Describe { reply }).await;
        described.await.unwrap();
        if let Some(after) = after_load {
            metadata.set(after);
            metadata_update(&handle, &["orders"]).await;
        }
        let refreshed = heartbeat(&handle, keepalive(joined.member_epoch, None)).await;

        answers.push((name, refreshed));
        expected.push((name, wanted));
    }
    check!(answers == expected);
}

/// Kafka's `classicGroupJoinToConsumerGroup` refreshes the metadata as the
/// consumer heartbeat does, so a classic member that a consumer group hosts
/// gets the partitions of a created topic when it joins again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hosted_classic_member_gets_a_created_topic_when_it_joins_again() {
    let metadata = SwitchableMetadata::new(snapshot_of(&[]));
    let coordinator = Arc::new(GroupCoordinator::new(
        NextGenConfig {
            migration_policy: ConsumerGroupMigrationPolicy::Upgrade,
            ..NextGenConfig::assigning_at_once()
        },
        ShareGroupConfig::assigning_at_once(),
        metadata.clone(),
        Arc::new(InMemoryOffsetsLog::default()),
        StreamsGroupConfig::default(),
    ));
    let handle = seed_and_upgrade(&coordinator, "orders").await;

    metadata.set(snapshot_of(&[("orders", 1, 2)]));
    metadata_update(&handle, &["orders"]).await;
    let joined = rpc::classic_join(&handle, "m-classic", "orders").await;
    let synced = rpc::classic_sync(&handle, "m-classic", joined.generation_id).await;

    check!(synced.error_code == 0);
    check!(
        decode_assignment(&synced.assignment)
            == ConsumerProtocolAssignment {
                assigned_partitions: vec![TopicPartition {
                    topic: "orders".into(),
                    partitions: vec![0, 1],
                    ..Default::default()
                }],
                ..Default::default()
            }
    );
}

/// The `MetadataHash` that a consumer group writes is Kafka's: hash4j 0.22.0
/// gives `computeGroupHash` of topic `orders`, id `0101..01-0101..01`, two
/// partitions and no racks, as the golden value below, and a group that
/// subscribes to no existing topic writes 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_written_metadata_hash_is_kafkas() {
    // (case, the metadata, the hash of the group's epoch record)
    let rows = [
        (
            "orders exists",
            snapshot_of(&[("orders", 1, 2)]),
            2_418_189_869_542_540_743,
        ),
        ("orders does not exist", snapshot_of(&[]), 0),
    ];
    let mut written = Vec::new();
    let mut expected = Vec::new();
    for (case, metadata, hash) in rows {
        let coordinator = make_coord_with_metadata(SwitchableMetadata::new(metadata));
        let handle = coordinator.get_or_create_consumer("g");
        heartbeat(&handle, join()).await;
        let seed = coordinator.cached_seed("g").expect("the group's records");
        written.push((case, seed.group_epoch, seed.metadata_hash));
        expected.push((case, 2, hash));
    }
    check!(written == expected);
}

/// KIP-1263: a consumer group replays the `AssignmentTimestamp` of its target
/// assignment metadata record, and Kafka's `canComputeNextTargetAssignment`
/// runs the assignment interval from it, so a coordinator failover does not
/// cut the interval short. An unknown time (0) or an elapsed interval lets the
/// next assignment run, and the group writes the time it finished.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_replayed_assignment_timestamp_holds_the_interval() {
    use crate::coordinator::unified::{GroupSeed, wall_clock_ms};

    let (rows, mut cases) = assignment_interval_fixture();

    for (case, ago, wanted) in rows {
        let coordinator = Arc::new(GroupCoordinator::new(
            NextGenConfig {
                assignment_interval: std::time::Duration::from_mins(1),
                ..NextGenConfig::assigning_at_once()
            },
            ShareGroupConfig::assigning_at_once(),
            SwitchableMetadata::new(snapshot_of(&[("orders", 1, 2)])),
            Arc::new(InMemoryOffsetsLog::default()),
            StreamsGroupConfig::default(),
        ));
        let handle = coordinator.get_or_create_consumer("g");
        let stored = ago.map_or(0, |ago| wall_clock_ms() - ago);
        handle
            .tx
            .send(GroupActorMessage::Seed(GroupSeed {
                group_epoch: 2,
                target_epoch: 2,
                assignment_timestamp_ms: stored,
                ..GroupSeed::default()
            }))
            .await
            .unwrap();
        let before = wall_clock_ms();
        let joined = heartbeat(&handle, join()).await;
        let after = wall_clock_ms();
        let written = coordinator
            .cached_seed("g")
            .unwrap()
            .assignment_timestamp_ms;
        cases.record(case, joined.member_epoch, (before, after), written, wanted);
    }
    cases.check();
}
