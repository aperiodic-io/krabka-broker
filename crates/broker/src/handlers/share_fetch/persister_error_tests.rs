//! Handler tests for how `ShareFetch` and `ShareAcknowledge` answer when the
//! share-state persister fails.
//!
//! Kafka's `SharePartition.maybeInitialize` fails the share partition when
//! `ReadShareGroupState` fails, and `SharePartitionManager` removes it from
//! its cache, so the next request reads again. A failed
//! `WriteShareGroupState` rolls the acknowledgement back
//! (`SharePartition.rollbackOrProcessStateUpdates`), and the client gets the
//! error that `SharePartition.fetchPersisterError` maps. A fenced state epoch
//! maps to `NOT_LEADER_OR_FOLLOWER` and drops the partition from the cache.

use {
    crate::{
        broker::BrokerHandle,
        codes,
        error::BrokerError,
        handlers::test_support::{
            AcknowledgementCode, AcknowledgementPartitionSetup, acknowledge_batches_request,
            create_topic as create_topic_from_test_support, metadata_topic_record, produce_records,
            send_acknowledgements, share_fetch_partition_rows, share_fetch_wire,
            single_replica_partition,
        },
        share_coordinator::bootstrap::TOPIC,
        test_support::initialize_share_state,
    },
    assert2::assert,
    krabka_log::Offset,
    krabka_metadata::{MetadataRecord, NodeId},
    krabka_protocol::{
        owned::{
            share_fetch_request::{
                AcknowledgementBatch as FetchAcknowledgeBatch, FetchPartition, FetchTopic,
                ShareFetchRequest,
            },
            share_fetch_response::PartitionData,
        },
        primitives::uuid::Uuid as WireUuid,
    },
};

/// The acknowledge type `Accept`.
const ACCEPT: i8 = 1;

/// The API that carries the acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Api {
    ShareAcknowledge,
    /// An acknowledgement piggybacked on a `ShareFetch`.
    ShareFetch,
}

use crate::handlers::test_support::start_allow_all_no_audit as start;

async fn create_topic(broker: &BrokerHandle, name: &str) -> WireUuid {
    create_topic_from_test_support()
        .broker(broker)
        .client_id("share-persister-error-test")
        .name(name)
        .call()
        .await
}

/// Appends one batch of two records to partition 0 of `topic`.
async fn produce_two_records(broker: &BrokerHandle, topic: &str) {
    produce_records()
        .broker(broker)
        .topic(topic)
        .count(2)
        .call()
        .await;
}

/// Sends a `ShareFetch` for partition 0 of `topic_id`. With `accept`, the row
/// accepts that offset range.
async fn share_fetch(
    broker: &BrokerHandle,
    group: &str,
    epoch: i32,
    topic_id: WireUuid,
    accept: Option<(i64, i64)>,
) -> PartitionData {
    let request = ShareFetchRequest {
        group_id: Some(group.into()),
        member_id: Some("member".into()),
        share_session_epoch: epoch,
        max_wait_ms: 0,
        max_bytes: 1 << 20,
        max_records: 500,
        batch_size: 500,
        topics: vec![FetchTopic {
            topic_id,
            partitions: vec![FetchPartition {
                partition_index: 0,
                acknowledgement_batches: accept
                    .map(|(first_offset, last_offset)| FetchAcknowledgeBatch {
                        first_offset,
                        last_offset,
                        acknowledge_types: vec![ACCEPT],
                        ..Default::default()
                    })
                    .into_iter()
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let response = share_fetch_wire()
        .broker(broker)
        .request(&request)
        .call()
        .await;
    share_fetch_partition_rows(response)
        .into_iter()
        .next()
        .unwrap_or_default()
}

/// Sends a `ShareAcknowledge` that accepts `[first, last]` on partition 0 of
/// `topic_id`, and returns the partition error.
async fn share_acknowledge(
    broker: &BrokerHandle,
    group: &str,
    epoch: i32,
    topic_id: WireUuid,
    (first_offset, last_offset): (i64, i64),
) -> i16 {
    let request = acknowledge_batches_request()
        .group(group)
        .epoch(epoch)
        .topic_id(topic_id)
        .partition(AcknowledgementPartitionSetup::single_batch(
            krabka_log::Offset(first_offset)..=krabka_log::Offset(last_offset),
            AcknowledgementCode(ACCEPT),
        ))
        .call();
    let response = send_acknowledgements()
        .broker(broker)
        .request(&request)
        .call()
        .await;

    response.responses[0].partitions[0].error_code
}

use crate::handlers::test_support::acquired_share_records as acquired;

fn topic_uuid(topic_id: WireUuid) -> uuid::Uuid { uuid::Uuid::from_bytes(topic_id.0) }

async fn initialize_state(broker: &BrokerHandle, group: &str, topic_id: WireUuid) {
    initialize_share_state(broker, group, topic_uuid(topic_id), 0).await;
}

/// What a client and the partition cache show after an acknowledgement
/// whose state write the coordinator fenced.
#[derive(Debug, PartialEq, Eq)]
struct FencedWrite {
    /// The acknowledge error: the `ShareAcknowledge` partition error, or the
    /// `ShareFetch` acknowledge error.
    acknowledge_error: i16,
    /// The `ShareFetch` partition error of the acknowledging request, or
    /// `NONE` for a `ShareAcknowledge`.
    fetch_error: i16,
    /// Whether the broker still caches the share partition.
    cached: bool,
    /// The offsets that the next `ShareFetch` acquires.
    next_acquired: Vec<(i64, i64)>,
}

/// The coordinator raises the state epoch while a member holds records, as a
/// new registration of the share partition does. The acknowledgement then
/// carries a stale epoch and the coordinator answers `FENCED_STATE_EPOCH`.
///
/// Kafka answers `NOT_LEADER_OR_FOLLOWER`, keeps the records unacknowledged,
/// and drops the partition from the cache. The next fetch reads the state
/// again and acquires the records again.
#[tokio::test]
async fn a_fenced_state_write_fails_the_acknowledgement_and_drops_the_partition() {
    let (broker, _dir) = start().await;
    let shared = broker.broker_arc_for_test();

    let mut actual = Vec::new();
    for api in [Api::ShareAcknowledge, Api::ShareFetch] {
        let name = format!("fenced-{api:?}");
        let topic_id = create_topic(&broker, &name).await;
        initialize_state(&broker, &name, topic_id).await;
        let opened = share_fetch(&broker, &name, 0, topic_id, None).await;
        assert!(opened.error_code == codes::NONE, "{opened:?}");
        produce_two_records(&broker, &name).await;
        let fetched = share_fetch(&broker, &name, 1, topic_id, None).await;
        assert!(acquired(&fetched) == vec![(0, 1)], "{fetched:?}");

        let state_epoch = shared
            .share_partition_leaders
            .peek_for_test(&name, topic_uuid(topic_id), 0)
            .expect("the fetch cached the share partition")
            .lock()
            .await
            .state_epoch;
        shared
            .share_coordinator
            .initialize(
                &shared.controller.current_image(),
                &name,
                topic_uuid(topic_id),
                0,
                state_epoch + 1,
                Offset(0),
            )
            .await
            .expect("raise the state epoch");

        let (acknowledge_error, fetch_error) = match api {
            Api::ShareAcknowledge => (
                share_acknowledge(&broker, &name, 2, topic_id, (0, 1)).await,
                codes::NONE,
            ),
            Api::ShareFetch => {
                let row = share_fetch(&broker, &name, 2, topic_id, Some((0, 1))).await;
                (row.acknowledge_error_code, row.error_code)
            }
        };
        let cached = shared
            .share_partition_leaders
            .peek_for_test(&name, topic_uuid(topic_id), 0)
            .is_some();
        let next = share_fetch(&broker, &name, 3, topic_id, None).await;

        actual.push((api, FencedWrite {
            acknowledge_error,
            fetch_error,
            cached,
            next_acquired: acquired(&next),
        }));
    }

    let expected = vec![
        (Api::ShareAcknowledge, FencedWrite {
            acknowledge_error: codes::NOT_LEADER_OR_FOLLOWER,
            fetch_error: codes::NONE,
            cached: false,
            next_acquired: vec![(0, 1)],
        }),
        (Api::ShareFetch, FencedWrite {
            acknowledge_error: codes::NOT_LEADER_OR_FOLLOWER,
            fetch_error: codes::NOT_LEADER_OR_FOLLOWER,
            cached: false,
            next_acquired: vec![(0, 1)],
        }),
    ];
    assert!(actual == expected);
    broker.shutdown().await;
}

/// Points every `__share_group_state` partition at a broker that is not
/// registered, so no share coordinator can serve a state read.
async fn state_topic_led_by_an_unknown_broker(broker: &BrokerHandle) {
    let shared = broker.broker_arc_for_test();
    let partitions = shared.config.share_coordinator.state_topic_num_partitions;
    let mut records = vec![MetadataRecord::V1Topic(metadata_topic_record(
        TOPIC,
        uuid::Uuid::new_v4(),
        partitions,
        1,
    ))];
    records.extend((0..partitions).map(|partition| {
        MetadataRecord::V1Partition(single_replica_partition(
            TOPIC,
            krabka_ids::PartitionIndex(partition),
            NodeId(99),
        ))
    }));
    shared
        .controller
        .submit_change(records)
        .await
        .expect("seed the share-state topic");
}

/// A state read that no coordinator can serve fails the partition with
/// `COORDINATOR_NOT_AVAILABLE`, which is what Kafka's `fetchPersisterError`
/// gives for the coordinator errors. The broker caches nothing, so a later
/// request reads again. The partition never starts from a guessed offset. An
/// acknowledgement for the partition then finds nothing cached and answers
/// `UNKNOWN_TOPIC_OR_PARTITION`, as Kafka's `SharePartitionManager.acknowledge`
/// does, without reading the state.
#[tokio::test]
async fn a_failed_state_read_fails_the_partition_and_caches_nothing() {
    let (broker, _dir) = start().await;
    state_topic_led_by_an_unknown_broker(&broker).await;
    let topic_id = create_topic(&broker, "unreadable").await;
    produce_two_records(&broker, "unreadable").await;
    let shared = broker.broker_arc_for_test();

    let fetch = share_fetch(&broker, "fetch-group", 0, topic_id, None).await;
    let fetch_cached = shared
        .share_partition_leaders
        .peek_for_test("fetch-group", topic_uuid(topic_id), 0)
        .is_some();
    let _ = share_fetch(&broker, "acknowledge-group", 0, topic_id, None).await;
    let acknowledge = share_acknowledge(&broker, "acknowledge-group", 1, topic_id, (0, 1)).await;
    let acknowledge_cached = shared
        .share_partition_leaders
        .peek_for_test("acknowledge-group", topic_uuid(topic_id), 0)
        .is_some();

    assert!(
        (
            (fetch.error_code, acquired(&fetch), fetch_cached),
            (acknowledge, acknowledge_cached)
        ) == (
            (codes::COORDINATOR_NOT_AVAILABLE, Vec::new(), false),
            (codes::UNKNOWN_TOPIC_OR_PARTITION, false)
        )
    );
    broker.shutdown().await;
}

/// The share coordinator refuses a read of a key that the group coordinator
/// has not initialized with `INVALID_REQUEST`, as Kafka's
/// `ShareCoordinatorShard.maybeGetReadStateError` does. The persister reports
/// that code as a share-state partition error, and Kafka's
/// `fetchPersisterError` maps it to `UNKNOWN_SERVER_ERROR`. The partition does
/// not start from `share.auto.offset.reset`, and the broker caches nothing.
/// Once the group coordinator initializes the key, the next fetch reads it.
#[tokio::test]
async fn a_read_of_an_uninitialized_state_fails_until_the_state_is_initialized() {
    const GROUP: &str = "uninitialized-group";
    let (broker, _dir) = start().await;
    let topic_id = create_topic(&broker, "uninitialized").await;
    let shared = broker.broker_arc_for_test();
    let persister = shared
        .group_coordinator
        .share_persister()
        .cloned()
        .expect("share persister");
    let cached = || {
        shared
            .share_partition_leaders
            .peek_for_test(GROUP, topic_uuid(topic_id), 0)
            .is_some()
    };

    let read_code = match persister
        .read_state(GROUP, topic_uuid(topic_id), 0, 0)
        .await
    {
        Err(BrokerError::SharePartitionState { code, .. }) => Some(code),
        _ => None,
    };
    let refused = share_fetch(&broker, GROUP, 0, topic_id, None).await;
    let refused_cached = cached();
    initialize_state(&broker, GROUP, topic_id).await;
    let opened = share_fetch(&broker, GROUP, 1, topic_id, None).await;
    let opened_cached = cached();
    produce_two_records(&broker, "uninitialized").await;
    let fetched = share_fetch(&broker, GROUP, 2, topic_id, None).await;

    assert!(
        (
            read_code,
            (refused.error_code, refused_cached),
            (opened.error_code, opened_cached),
            (fetched.error_code, acquired(&fetched))
        ) == (
            Some(codes::INVALID_REQUEST),
            (codes::UNKNOWN_SERVER_ERROR, false),
            (codes::NONE, true),
            (codes::NONE, vec![(0, 1)])
        )
    );
    broker.shutdown().await;
}
