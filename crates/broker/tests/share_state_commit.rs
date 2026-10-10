//! A share coordinator answers a share-state request only once the records of
//! its `__share_group_state` partition are committed.
//!
//! Kafka's share coordinator is a `CoordinatorRuntime` shard. The runtime
//! completes a write when the high watermark of the partition passes it, and
//! it fails the write with a timeout after `share.coordinator.write.timeout.ms`.
//! `CoordinatorOperationExceptionHelper` answers that timeout as
//! `COORDINATOR_NOT_AVAILABLE`. A coordinator that answered after the local
//! append only could acknowledge state that the next leader of the partition
//! never gets. A share-partition leader then loses or delivers again the
//! records that the state covered.

use std::time::Duration;

use assert2::assert;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use krabka_broker::{BrokerConfig, BrokerHandle};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        delete_share_group_state_request::{
            DeleteShareGroupStateRequest, DeleteStateData, PartitionData as DeletePart,
        },
        delete_share_group_state_response::{
            DeleteShareGroupStateResponse, DeleteStateResult, PartitionResult as DeleteRow,
        },
        initialize_share_group_state_request::{
            InitializeShareGroupStateRequest, InitializeStateData, PartitionData as InitPart,
        },
        initialize_share_group_state_response::{
            InitializeShareGroupStateResponse, InitializeStateResult, PartitionResult as InitRow,
        },
        read_share_group_state_request::{
            PartitionData as ReadPart, ReadShareGroupStateRequest, ReadStateData,
        },
        read_share_group_state_response::{
            PartitionResult as ReadRow, ReadShareGroupStateResponse, ReadStateResult,
        },
        write_share_group_state_request::{
            PartitionData as WritePart, StateBatch, WriteShareGroupStateRequest, WriteStateData,
        },
        write_share_group_state_response::{
            PartitionResult as WriteRow, WriteShareGroupStateResponse, WriteStateResult,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};
use tempfile::TempDir;

use crate::support::{
    client::connect_owned,
    topics::{creatable_topic, create_topic_request},
};

mod support;

const GROUP: &str = "share-commit";
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;

/// The share coordinator's `share.coordinator.write.timeout.ms` in this
/// suite: short, so that each write that cannot commit answers quickly.
const WRITE_TIMEOUT: Duration = Duration::from_secs(1);

/// Kafka's message of a coordinator write that timed out, after the prefix
/// of the operation.
const TIMED_OUT: &str = "The request timed out.";

type Cluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;

/// Three brokers with one `__share_group_state` partition, replicated on all
/// of them. A stopped broker stays in the ISR: the leader does not shrink it
/// on lag, and the controller does not fence the broker, for longer than the
/// test runs.
async fn start_three() -> Cluster {
    support::fixed_internal_isr_cluster(|config| {
        config.share_coordinator.write_timeout = WRITE_TIMEOUT;
    })
    .await
}

async fn client(handle: &BrokerHandle) -> Client {
    connect_owned(
        handle.listen_addr().to_string(),
        "share-state-commit",
        "client",
    )
    .await
}

/// Creates the one-partition data topic `orders` and returns its topic id.
/// The share coordinator refuses the state of a topic partition that the
/// metadata image does not hold.
async fn create_orders(client: &Client) -> uuid::Uuid {
    let created = client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup::default(),
        )))
        .await
        .expect("CreateTopics");
    assert!(created.topics[0].error_code == 0, "{created:?}");
    uuid::Uuid::from_bytes(created.topics[0].topic_id.0)
}

fn wire(topic_id: uuid::Uuid) -> WireUuid {
    WireUuid(*topic_id.as_bytes())
}

fn position_of(cluster: &Cluster, node_id: u64) -> usize {
    cluster
        .iter()
        .position(|(handle, _, _)| handle.node_id() == node_id)
        .expect("a cluster member")
}

/// One share-state request on `(GROUP, orders, 0)`.
#[derive(Debug, Clone, Copy)]
enum Rpc {
    Initialize { state_epoch: i32 },
    Write { start_offset: i64 },
    Read,
    Delete,
}

/// The whole response of an [`Rpc`].
#[derive(Debug, PartialEq)]
enum Answer {
    Initialize(InitializeShareGroupStateResponse),
    Write(WriteShareGroupStateResponse),
    Read(ReadShareGroupStateResponse),
    Delete(DeleteShareGroupStateResponse),
}

impl Rpc {
    async fn send(self, client: &Client, topic_id: uuid::Uuid) -> Answer {
        let topic_id = wire(topic_id);
        match self {
            Self::Initialize { state_epoch } => Answer::Initialize(
                client
                    .send(InitializeShareGroupStateRequest {
                        group_id: GROUP.into(),
                        topics: vec![InitializeStateData {
                            topic_id,
                            partitions: vec![InitPart {
                                partition: 0,
                                state_epoch,
                                start_offset: 0,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .await
                    .expect("InitializeShareGroupState"),
            ),
            Self::Write { start_offset } => Answer::Write(
                client
                    .send(WriteShareGroupStateRequest {
                        group_id: GROUP.into(),
                        topics: vec![WriteStateData {
                            topic_id,
                            partitions: vec![WritePart {
                                partition: 0,
                                state_epoch: 0,
                                leader_epoch: 0,
                                start_offset,
                                delivery_complete_count: 0,
                                state_batches: vec![StateBatch {
                                    first_offset: start_offset,
                                    last_offset: start_offset + 9,
                                    delivery_state: 2,
                                    delivery_count: 1,
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .await
                    .expect("WriteShareGroupState"),
            ),
            Self::Read => Answer::Read(
                client
                    .send(ReadShareGroupStateRequest {
                        group_id: GROUP.into(),
                        topics: vec![ReadStateData {
                            topic_id,
                            partitions: vec![ReadPart {
                                partition: 0,
                                leader_epoch: 0,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .await
                    .expect("ReadShareGroupState"),
            ),
            Self::Delete => Answer::Delete(
                client
                    .send(DeleteShareGroupStateRequest {
                        group_id: GROUP.into(),
                        topics: vec![DeleteStateData {
                            topic_id,
                            partitions: vec![DeletePart {
                                partition: 0,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .await
                    .expect("DeleteShareGroupState"),
            ),
        }
    }

    /// The answer with `error_code` and, for an error, Kafka's message: the
    /// prefix of the operation and `message`.
    fn answer(self, topic_id: uuid::Uuid, error_code: i16, message: &str) -> Answer {
        let topic_id = wire(topic_id);
        let error = |operation: &str| {
            (error_code != 0).then(|| format!("Unable to {operation} share group state: {message}"))
        };
        match self {
            Self::Initialize { .. } => Answer::Initialize(InitializeShareGroupStateResponse {
                results: vec![InitializeStateResult {
                    topic_id,
                    partitions: vec![InitRow {
                        partition: 0,
                        error_code,
                        error_message: error("initialize"),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            Self::Write { .. } => Answer::Write(WriteShareGroupStateResponse {
                results: vec![WriteStateResult {
                    topic_id,
                    partitions: vec![WriteRow {
                        partition: 0,
                        error_code,
                        error_message: error("write"),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            Self::Read => Answer::Read(ReadShareGroupStateResponse {
                results: vec![ReadStateResult {
                    topic_id,
                    partitions: vec![ReadRow {
                        partition: 0,
                        error_code,
                        error_message: error("read"),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            Self::Delete => Answer::Delete(DeleteShareGroupStateResponse {
                results: vec![DeleteStateResult {
                    topic_id,
                    partitions: vec![DeleteRow {
                        partition: 0,
                        error_code,
                        error_message: error("delete"),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        }
    }
}

/// Initializes `(GROUP, orders, 0)` on `client`'s broker, and retries while
/// the share coordinator loads the state partition, as Kafka's persister
/// retries it.
async fn initialize_when_loaded(client: &Client, topic_id: uuid::Uuid) -> Answer {
    let rpc = Rpc::Initialize { state_epoch: 0 };
    for _ in 0..100 {
        let answer = rpc.send(client, topic_id).await;
        let Answer::Initialize(response) = &answer else {
            unreachable!("an Initialize answers an Initialize");
        };
        let code = response.results[0].partitions[0].error_code;
        if ![
            COORDINATOR_LOAD_IN_PROGRESS,
            COORDINATOR_NOT_AVAILABLE,
            NOT_COORDINATOR,
        ]
        .contains(&code)
        {
            return answer;
        }
        // intentional: the client has no awaiter for the load of the state
        // partition. It retries, as Kafka's persister does.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the share coordinator did not load __share_group_state-0");
}

/// While a follower of `__share_group_state-0` is down but still in its ISR,
/// no share-state record commits. The coordinator then answers no request
/// with success: each one times out as Kafka's coordinator write does, and
/// the share-partition leader gets `COORDINATOR_NOT_AVAILABLE`, which it
/// retries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_share_state_request_is_answered_only_when_committed() {
    let mut cluster = start_three().await;
    let lookup = client(&cluster[0].0).await;
    let topic_id = create_orders(&lookup).await;
    let key = format!("{GROUP}:{}:0", URL_SAFE_NO_PAD.encode(topic_id.as_bytes()));
    let coordinator = support::find_coordinator(&lookup, support::KEY_TYPE_SHARE, &key)
        .await
        .node_id;
    lookup.close();
    let coordinator = u64::try_from(coordinator).expect("a node id");

    // Every replica is up: the records commit, and the coordinator answers.
    let member = client(&cluster[position_of(&cluster, coordinator)].0).await;
    let initialized = initialize_when_loaded(&member, topic_id).await;
    assert!(initialized == Rpc::Initialize { state_epoch: 0 }.answer(topic_id, 0, ""));
    let written = Rpc::Write { start_offset: 0 }.send(&member, topic_id).await;
    assert!(written == Rpc::Write { start_offset: 0 }.answer(topic_id, 0, ""));

    // Stop a follower of the state partition. It stays in the ISR, so the
    // high watermark stops below every later record.
    let stopped_dir = crate::support::share::crash_follower(&mut cluster, coordinator).await;

    let rows = [
        Rpc::Write { start_offset: 10 },
        Rpc::Read,
        Rpc::Initialize { state_epoch: 1 },
        Rpc::Delete,
    ];
    for rpc in rows {
        let answer = rpc.send(&member, topic_id).await;
        assert!(
            answer == rpc.answer(topic_id, COORDINATOR_NOT_AVAILABLE, TIMED_OUT),
            "{rpc:?}"
        );
    }
    member.close();

    crate::support::shutdown_cluster(cluster).await;
    drop(stopped_dir);
}
