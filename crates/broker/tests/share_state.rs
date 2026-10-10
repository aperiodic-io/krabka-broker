//! End-to-end integration tests for the KIP-932 share coordinator (persister).
//! The tests run against an in-process Krabka broker with `krabka-client-core`.
//!
//! The typed client works because `ApiVersions` advertises `api_keys` 83-87.
//! Each `*ShareGroupState*Request` impls `ProtocolRequest`, so
//! `client.send(req)` exercises the real wire path. That path includes version
//! negotiation through `ApiVersions`.
//!
//! Timing note: the raw persister RPC handlers do NOT create
//! `__share_group_state`. `FindCoordinator(SHARE)` does. The first lookup asks
//! for the topic and answers `COORDINATOR_NOT_AVAILABLE`, and a client retries
//! it. After the broker creates the topic, it materializes and leads the
//! topic's partitions asynchronously in the replicator supervisor. So the first
//! `Initialize` may briefly return a coordinator-not-ready code. The `*_ready`
//! helpers retry, exactly as a real client would.

mod support;

use std::{sync::Arc, time::Duration};

use assert2::{assert, check};
use krabka_broker::{BootstrapMode, Broker, BrokerConfig};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    delete_share_group_state_request::{
        DeleteShareGroupStateRequest, DeleteStateData, PartitionData as DeletePart,
    },
    find_coordinator_request::FindCoordinatorRequest,
    find_coordinator_response::Coordinator,
    initialize_share_group_state_request::{
        InitializeShareGroupStateRequest, InitializeStateData, PartitionData as InitPart,
    },
    read_share_group_state_request::{
        PartitionData as ReadPart, ReadShareGroupStateRequest, ReadStateData,
    },
    read_share_group_state_summary_request::{
        PartitionData as SummaryPart, ReadShareGroupStateSummaryRequest, ReadStateSummaryData,
    },
    write_share_group_state_request::{
        PartitionData as WritePart, StateBatch, WriteShareGroupStateRequest, WriteStateData,
    },
};

use crate::support::{client::connect_client, discovery::coordinator_lookup_request};

const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;
const FENCED_STATE_EPOCH: i16 = 124;
const INVALID_REQUEST: i16 = 42;

fn not_ready(code: i16) -> bool {
    code == COORDINATOR_LOAD_IN_PROGRESS
        || code == COORDINATOR_NOT_AVAILABLE
        || code == NOT_COORDINATOR
}

use crate::support::boot_single as boot;

async fn connect(bootstrap: &str) -> Arc<Client> {
    Arc::new(connect_client(bootstrap, Some("c1")).await)
}

/// Create a one-partition data topic and return its topic id. The share
/// coordinator refuses a read or a write of a topic partition that the
/// metadata image does not hold.
async fn create_topic(client: &Client, name: &str) -> uuid::Uuid {
    let id = crate::support::client::create_topic_with(
        client,
        crate::support::topics::CreateTopicSetup {
            topic: name,
            ..Default::default()
        },
    )
    .await;
    uuid::Uuid::from_bytes(id.0)
}

use crate::support::share::{coordinator_key as share_coordinator_key, wire};

/// Sends one `FindCoordinator(SHARE)` for `key` and returns its row. A lookup
/// before `__share_group_state` exists asks for the topic.
async fn find_share_once(client: &Client, key: &str) -> Coordinator {
    let resp = client
        .send(coordinator_lookup_request(
            FindCoordinatorRequest::default().key,
            support::KEY_TYPE_SHARE,
            vec![key.to_string()],
        ))
        .await
        .expect("FindCoordinator(SHARE)");
    let [row] = resp.coordinators.as_slice() else {
        panic!("one coordinator row: {resp:?}");
    };
    row.clone()
}

/// Initialize one (group, topic, partition). This helper retries while the
/// coordinator still materializes the state partition. Returns the final
/// `error_code`.
async fn initialize_ready(
    client: &Client,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
    state_epoch: i32,
    start_offset: i64,
) -> i16 {
    for _ in 0..40 {
        let resp = client
            .send(InitializeShareGroupStateRequest {
                group_id: group.into(),
                topics: vec![InitializeStateData {
                    topic_id: wire(tid),
                    partitions: vec![InitPart {
                        partition,
                        state_epoch,
                        start_offset,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("InitializeShareGroupState");
        let code = resp.results[0].partitions[0].error_code;
        if !not_ready(code) {
            return code;
        }
        // intentional: bounded retry of the Initialize RPC while the share
        // coordinator is still asynchronously loading/materializing the
        // __share_group_state partition. This helper holds only a `Client`
        // (no `BrokerHandle`), and the awaited condition is coordinator LOAD
        // state (COORDINATOR_LOAD_IN_PROGRESS) — not share-partition state, so
        // the wait_until_share_* awaiters don't apply (they observe SPSO /
        // delivery / acquired state that this very Initialize creates).
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("share coordinator never became ready for {group}:{tid}:{partition}");
}

struct StateWrite<'a> {
    group: &'a str,
    topic_id: uuid::Uuid,
    partition: i32,
    state_epoch: i32,
    leader_epoch: i32,
    start_offset: i64,
    delivery_complete_count: i32,
    batches: Vec<StateBatch>,
}

async fn write_state(client: &Client, write: StateWrite<'_>) -> i16 {
    let resp = client
        .send(WriteShareGroupStateRequest {
            group_id: write.group.into(),
            topics: vec![WriteStateData {
                topic_id: wire(write.topic_id),
                partitions: vec![WritePart {
                    partition: write.partition,
                    state_epoch: write.state_epoch,
                    leader_epoch: write.leader_epoch,
                    start_offset: write.start_offset,
                    delivery_complete_count: write.delivery_complete_count,
                    state_batches: write.batches,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("WriteShareGroupState");
    resp.results[0].partitions[0].error_code
}

async fn read_summary(
    client: &Client,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
) -> krabka_protocol::owned::read_share_group_state_summary_response::PartitionResult {
    let resp = client
        .send(ReadShareGroupStateSummaryRequest {
            group_id: group.into(),
            topics: vec![ReadStateSummaryData {
                topic_id: wire(tid),
                partitions: vec![SummaryPart {
                    partition,
                    leader_epoch: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("ReadShareGroupStateSummary");
    resp.results[0].partitions[0].clone()
}

async fn read_state(
    client: &Client,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
) -> krabka_protocol::owned::read_share_group_state_response::PartitionResult {
    let resp = client
        .send(ReadShareGroupStateRequest {
            group_id: group.into(),
            topics: vec![ReadStateData {
                topic_id: wire(tid),
                partitions: vec![ReadPart {
                    partition,
                    leader_epoch: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("ReadShareGroupState");
    resp.results[0].partitions[0].clone()
}

/// `FindCoordinator(SHARE)` bootstraps `__share_group_state` and routes the
/// `(group, topic, partition)` key to a real broker (this single node).
///
/// The first lookup finds no topic. It asks for the topic and answers
/// `COORDINATOR_NOT_AVAILABLE` with no node, as Kafka's
/// `KafkaApis.getCoordinator` does. A retried lookup names this broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn find_coordinator_share_creates_topic_and_returns_broker() {
    let (broker, bootstrap, _d) = boot().await;
    let client = connect(&bootstrap).await;

    let tid = uuid::Uuid::from_bytes([3u8; 16]);
    let key = share_coordinator_key("g1", tid, 0);
    let first = find_share_once(&client, &key).await;
    check!(
        first
            == Coordinator {
                key: key.clone(),
                node_id: -1,
                host: String::new(),
                port: -1,
                error_code: COORDINATOR_NOT_AVAILABLE,
                error_message: None,
                ..Default::default()
            }
    );

    let retried = support::find_coordinator(&client, support::KEY_TYPE_SHARE, &key).await;
    let addr = broker.listen_addr();
    check!(
        retried
            == Coordinator {
                key,
                node_id: i32::try_from(broker.node_id()).unwrap(),
                host: addr.ip().to_string(),
                port: i32::from(addr.port()),
                error_code: 0,
                error_message: None,
                ..Default::default()
            }
    );
}

/// Initialize -> Write -> Read -> `ReadSummary` -> Delete round-trips over the wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persister_round_trip() {
    let (_b, bootstrap, _d) = boot().await;
    let client = connect(&bootstrap).await;
    let tid = create_topic(&client, "round-trip").await;

    // Bootstrap __share_group_state, then initialize (retrying until led).
    initialize_partition(&client, "g1", tid, 0, (0, 0)).await;

    // Write an in-flight batch above the new SPSO (5).
    let w = write_state(
        &client,
        StateWrite {
            group: "g1",
            topic_id: tid,
            partition: 0,
            state_epoch: 0,
            leader_epoch: 0,
            start_offset: 5,
            delivery_complete_count: 2,
            batches: vec![StateBatch {
                first_offset: 5,
                last_offset: 9,
                delivery_state: 2,
                delivery_count: 1,
                ..Default::default()
            }],
        },
    )
    .await;
    assert!(w == 0, "write error: {w}");

    // Read full state.
    let r = read_state(&client, "g1", tid, 0).await;
    check!(r.error_code == 0, "read error: {}", r.error_code);
    check!(
        r.start_offset == 5,
        "SPSO must be 5, got {}",
        r.start_offset
    );
    check!(
        r.state_batches
            .iter()
            .any(|b| b.first_offset == 5 && b.last_offset == 9),
        "written batch must be present: {:?}",
        r.state_batches
    );

    // Summary matches.
    let s = read_summary(&client, "g1", tid, 0).await;
    check!(s.error_code == 0, "summary error: {}", s.error_code);
    check!(
        s.start_offset == 5,
        "summary SPSO 5, got {}",
        s.start_offset
    );
    check!(
        s.state_epoch == 0,
        "summary state_epoch 0, got {}",
        s.state_epoch
    );
    check!(
        s.delivery_complete_count == 2,
        "summary DCC 2, got {}",
        s.delivery_complete_count
    );

    // Delete, then a fresh read is refused: the key has no state.
    let del = client
        .send(DeleteShareGroupStateRequest {
            group_id: "g1".into(),
            topics: vec![DeleteStateData {
                topic_id: wire(tid),
                partitions: vec![DeletePart {
                    partition: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DeleteShareGroupState");
    assert!(
        del.results[0].partitions[0].error_code == 0,
        "delete error: {}",
        del.results[0].partitions[0].error_code
    );

    let after = read_state(&client, "g1", tid, 0).await;
    assert!(
        after
            == krabka_protocol::owned::read_share_group_state_response::PartitionResult {
                partition: 0,
                error_code: INVALID_REQUEST,
                error_message: Some(
                    "Read operation on uninitialized share partition not allowed.".to_owned()
                ),
                ..Default::default()
            }
    );
}

/// The broker fences a write that carries a `state_epoch` below the durable
/// one (124).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_fences_stale_state_epoch() {
    let (_b, bootstrap, _d) = boot().await;
    let client = connect(&bootstrap).await;
    let tid = create_topic(&client, "stale-state-epoch").await;

    support::find_coordinator(
        &client,
        support::KEY_TYPE_SHARE,
        &share_coordinator_key("g1", tid, 0),
    )
    .await;
    let init = initialize_ready(&client, "g1", tid, 0, 5, 0).await; // state_epoch 5
    assert!(init == 0, "initialize error: {init}");

    // Write at state_epoch 0 (< durable 5) -> fenced.
    let w = write_state(
        &client,
        StateWrite {
            group: "g1",
            topic_id: tid,
            partition: 0,
            state_epoch: 0,
            leader_epoch: 0,
            start_offset: 0,
            delivery_complete_count: 0,
            batches: vec![],
        },
    )
    .await;
    assert!(
        w == FENCED_STATE_EPOCH,
        "stale state_epoch must fence with 124, got {w}"
    );
}

/// Persisted share state survives a broker restart. Recovery replays
/// `__share_group_state`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn state_survives_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let tid = {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir.clone()))
            .await
            .unwrap();
        let client = connect(&broker.listen_addr().to_string()).await;
        let tid = create_topic(&client, "survives-restart").await;

        initialize_partition(&client, "g1", tid, 0, (0, 0)).await;
        let w = write_state(
            &client,
            StateWrite {
                group: "g1",
                topic_id: tid,
                partition: 0,
                state_epoch: 0,
                leader_epoch: 0,
                start_offset: 7,
                delivery_complete_count: 3,
                batches: vec![],
            },
        )
        .await;
        assert!(w == 0, "write error: {w}");

        broker.wait_until_share_spso("g1", tid, 0, 7).await;
        broker.shutdown().await;
        tid
    };

    {
        let mut cfg = BrokerConfig::for_tests(log_dir);
        cfg.bootstrap_mode = BootstrapMode::Rejoin;
        let broker = Broker::start(cfg).await.unwrap();
        let client = connect(&broker.listen_addr().to_string()).await;

        // Recovered coordinator may still be materializing the led partition;
        // await until the persisted SPSO is visible, then assert the wire value.
        broker.wait_until_share_spso("g1", tid, 0, 7).await;
        let start_offset = read_summary(&client, "g1", tid, 0).await.start_offset;
        assert!(
            start_offset == 7,
            "recovered SPSO must be 7, got {start_offset}"
        );
    }
}

/// Resolve and initialize a share partition before its first write.
async fn initialize_partition(
    client: &Client,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
    (state_epoch, start_offset): (i32, i64),
) {
    support::find_coordinator(
        client,
        support::KEY_TYPE_SHARE,
        &share_coordinator_key(group, tid, partition),
    )
    .await;
    let init = initialize_ready(client, group, tid, partition, state_epoch, start_offset).await;
    assert!(init == 0, "initialize error: {init}");
}
