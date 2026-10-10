//! Handler tests for the leader and replica checks on a `Fetch` read.
//!
//! Kafka's `ReplicaManager.readFromLog` reads a partition through
//! `Partition.localLogWithEpochOrThrow(currentLeaderEpoch,
//! fetchParams.fetchOnlyLeader)`. `FetchParams.fetchOnlyLeader` is true for a
//! follower fetch, and for a consumer fetch without client metadata, which
//! `KafkaApis.handleFetchRequest` builds only from v11. A broker that does not
//! lead the partition then answers `NOT_LEADER_OR_FOLLOWER`.
//!
//! For a follower fetch, `Partition.followerReplicaOrThrow` also refuses a
//! replica id that is not a follower in the assignment. It answers
//! `UNKNOWN_LEADER_EPOCH` when the request carries a leader epoch and
//! `NOT_LEADER_OR_FOLLOWER` when it does not, and it moves no follower state.
//!
//! The broker under test is node 1. Each case creates its own topic, with
//! replicas 1 and 2, led by node 1 or by node 2.

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use krabka_ids::{ApiVersion, LeaderEpoch};
use krabka_log::Offset;
use krabka_metadata::NodeId;
use krabka_protocol::{
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::{FetchResponse, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData},
    },
    records::RecordsPayload,
};

use super::test_support::FetchReplicaId;
use crate::{
    broker::BrokerHandle,
    codes,
    fetch_session::INVALID_SESSION_ID,
    partition::Partition,
    test_support::{KafkaErrorCode, start_broker_no_audit_with},
};

/// The node id of the broker under test.
const THIS_NODE: NodeId = NodeId(1);

/// The other replica of every test partition.
const OTHER_NODE: NodeId = NodeId(2);

/// The log end offset after the two records each case appends.
const LOG_END: Offset = Offset(2);

/// Who sends the fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sender {
    /// A follower fetch with this replica id, and this current leader epoch.
    Follower {
        replica_id: FetchReplicaId,
        leader_epoch: LeaderEpoch,
    },
    /// A consumer fetch, with replica id -1 and no rack.
    Consumer,
}

/// What a case expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    /// The follower reads at the log end, and the high watermark follows it
    /// after the read. The row reports the high watermark from before.
    FollowerRead,
    /// The consumer reads up to the high watermark, which is still 0.
    ConsumerRead,
    /// The row is refused with this code, and names this current leader.
    Refused(KafkaErrorCode, Option<NodeId>),
}

#[derive(Debug, Clone, Copy)]
struct Case {
    version: ApiVersion,
    sender: Sender,
    leader: NodeId,
    expect: Expect,
}

/// The observable result of one case.
#[derive(Debug, PartialEq)]
struct Outcome {
    case: String,
    response: FetchResponse,
    high_watermark: Offset,
    tracked_followers: Vec<u64>,
}

async fn start() -> (BrokerHandle, tempfile::TempDir) {
    start_broker_no_audit_with(|cfg| {
        // Node 2 never fetches. Keep it in the ISR for the whole test.
        cfg.replica_lag_time_max = krabka_units::secs(600);
    })
    .await
}

/// Create `topic` with replicas 1 and 2, led by `leader`, wait until this
/// broker holds the partition in that role, and append two records.
async fn partition(
    broker: &BrokerHandle,
    setup: crate::handlers::test_support::ReplicatedTopicSetup<'_>,
) -> Arc<Partition> {
    let topic = setup.topic;
    let leader = setup.leader;
    crate::handlers::test_support::seed_partition_replicas(broker, setup).await;

    wait_for_local_partition!(
        (shared, partition),
        broker,
        topic,
        partition,
        partition
            .current_leader
            .load(std::sync::atomic::Ordering::Acquire)
            == leader.0
            && (leader != THIS_NODE
                || partition
                    .replica_state
                    .lock()
                    .await
                    .isr
                    .contains(&krabka_raft::NodeId(OTHER_NODE.0))),
        "the broker holds the partition in its role"
    );

    let mut batch = crate::handlers::test_support::default_records_batch(&[b"first", b"second"]);
    crate::test_support::append_partition_batch(&partition, &mut batch);
    partition
}

/// A sessionless one-row fetch of partition 0 of `topic`. A follower fetches
/// from the log end offset, which moves the high watermark when the leader
/// records its progress. A consumer fetches from offset 0.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct LeaderFetchSetup<'a> {
    #[default(ApiVersion(12))]
    version: ApiVersion,
    #[default(Sender::Consumer)]
    sender: Sender,
    #[default("orders")]
    topic: &'a str,
}

fn request(setup: LeaderFetchSetup<'_>) -> FetchRequest {
    let LeaderFetchSetup {
        version,
        sender,
        topic,
    } = setup;
    let (replica_id, current_leader_epoch, fetch_offset) = match sender {
        Sender::Follower {
            replica_id,
            leader_epoch,
        } => (replica_id.0, leader_epoch.0, LOG_END),
        Sender::Consumer => (-1, -1, Offset(0)),
    };
    FetchRequest {
        topics: vec![FetchTopic {
            topic: topic.to_owned(),
            partitions: vec![FetchPartition {
                current_leader_epoch,
                ..super::test_support::request_partition(fetch_offset)
            }],
            ..Default::default()
        }],
        ..super::test_support::sessionless_request(super::test_support::SessionlessFetchSetup {
            version,
            replica: super::test_support::FetchReplicaId(replica_id),
        })
    }
}

async fn fetch(
    broker: &BrokerHandle,
    version: ApiVersion,
    request: &FetchRequest,
) -> FetchResponse {
    super::test_support::fetch_wire(
        broker,
        request,
        super::test_support::FetchWireSetup {
            version,
            user: "client",
            client_id: "fetch-client",
        },
    )
    .await
}

fn expected(case: Case, label: String, topic: &str) -> Outcome {
    let (row, high_watermark, tracked_followers) = match case.expect {
        Expect::FollowerRead => (
            super::test_support::expected_read(super::test_support::ExpectedReadSetup {
                watermark: Offset(0),
                records: RecordsPayload::Legacy(Bytes::new()),
            }),
            LOG_END,
            vec![OTHER_NODE.0],
        ),
        Expect::ConsumerRead => (
            super::test_support::expected_read(super::test_support::ExpectedReadSetup {
                watermark: Offset(0),
                records: RecordsPayload::Legacy(Bytes::new()),
            }),
            Offset(0),
            if case.leader == THIS_NODE {
                vec![OTHER_NODE.0]
            } else {
                Vec::new()
            },
        ),
        Expect::Refused(error_code, current_leader) => (
            PartitionData {
                partition_index: 0,
                error_code: error_code.0,
                high_watermark: -1,
                last_stable_offset: -1,
                log_start_offset: -1,
                aborted_transactions: None,
                preferred_read_replica: -1,
                records: Some(RecordsPayload::Legacy(Bytes::new())),
                // `CurrentLeader` is a tagged field from v12, but Kafka
                // fills it only from v16.
                current_leader: current_leader.filter(|_| case.version.0 >= 16).map_or_else(
                    LeaderIdAndEpoch::default,
                    |leader| LeaderIdAndEpoch {
                        leader_id: i32::try_from(leader.0).expect("small node id"),
                        leader_epoch: 0,
                        ..Default::default()
                    },
                ),
                ..Default::default()
            },
            Offset(0),
            if case.leader == THIS_NODE {
                vec![OTHER_NODE.0]
            } else {
                Vec::new()
            },
        ),
    };
    Outcome {
        case: label,
        response: FetchResponse {
            error_code: codes::NONE,
            session_id: INVALID_SESSION_ID,
            responses: vec![FetchableTopicResponse {
                topic: topic.to_owned(),
                partitions: vec![row],
                ..Default::default()
            }],
            ..Default::default()
        },
        high_watermark,
        tracked_followers,
    }
}

#[tokio::test]
async fn a_read_needs_the_leader_and_an_assigned_follower() {
    let assigned = Sender::Follower {
        replica_id: FetchReplicaId(2),
        leader_epoch: LeaderEpoch(-1),
    };
    let unassigned_without_epoch = Sender::Follower {
        replica_id: FetchReplicaId(3),
        leader_epoch: LeaderEpoch(-1),
    };
    let unassigned_with_epoch = Sender::Follower {
        replica_id: FetchReplicaId(3),
        leader_epoch: LeaderEpoch(0),
    };
    let cases = [
        Case {
            version: ApiVersion(12),
            sender: assigned,
            leader: THIS_NODE,
            expect: Expect::FollowerRead,
        },
        Case {
            version: ApiVersion(12),
            sender: assigned,
            leader: OTHER_NODE,
            expect: Expect::Refused(
                KafkaErrorCode(codes::NOT_LEADER_OR_FOLLOWER),
                Some(OTHER_NODE),
            ),
        },
        Case {
            version: ApiVersion(12),
            sender: unassigned_without_epoch,
            leader: THIS_NODE,
            expect: Expect::Refused(
                KafkaErrorCode(codes::NOT_LEADER_OR_FOLLOWER),
                Some(THIS_NODE),
            ),
        },
        Case {
            version: ApiVersion(12),
            sender: unassigned_with_epoch,
            leader: THIS_NODE,
            expect: Expect::Refused(KafkaErrorCode(codes::UNKNOWN_LEADER_EPOCH), None),
        },
        Case {
            version: ApiVersion(10),
            sender: Sender::Consumer,
            leader: THIS_NODE,
            expect: Expect::ConsumerRead,
        },
        Case {
            version: ApiVersion(10),
            sender: Sender::Consumer,
            leader: OTHER_NODE,
            expect: Expect::Refused(
                KafkaErrorCode(codes::NOT_LEADER_OR_FOLLOWER),
                Some(OTHER_NODE),
            ),
        },
        Case {
            version: ApiVersion(11),
            sender: Sender::Consumer,
            leader: OTHER_NODE,
            expect: Expect::ConsumerRead,
        },
    ];

    let (broker, _dir) = start().await;
    topic_case_outcomes!(
        (actual, want),
        (index, case, label, name),
        "leader-read",
        cases,
        {
            let partition = partition(
                &broker,
                crate::handlers::test_support::ReplicatedTopicSetup {
                    topic: &name,
                    topic_id: uuid::Uuid::from_u128(index + 1),
                    leader: case.leader,
                    ..Default::default()
                },
            )
            .await;

            let response = fetch(
                &broker,
                case.version,
                &request(LeaderFetchSetup {
                    version: case.version,
                    sender: case.sender,
                    topic: &name,
                }),
            )
            .await;
            let mut tracked_followers: Vec<u64> = partition
                .replica_state
                .lock()
                .await
                .per_follower
                .keys()
                .map(|node| node.0)
                .collect();
            tracked_followers.sort_unstable();
            actual.push(Outcome {
                case: label.clone(),
                response,
                high_watermark: partition.high_watermark().await,
                tracked_followers,
            });
            want.push(expected(case, label, &name));
        }
    );
    broker.shutdown().await;

    assert!(actual == want);
}
