//! A tiered partition that stops taking writes leaves local disk through its
//! last record, over the wire.
//!
//! Kafka's `ShareConsumerDLQTieredStorageTest` produces ten records to a
//! tiered topic and then waits for `ListOffsets(EARLIEST_LOCAL)` to reach the
//! last of them. Every record sits in the active segment, and `segment.bytes`
//! and `segment.ms` are far from rolling it. Kafka's retention check rolls it
//! once it breaches `local.retention.ms`. The copy then uploads the sealed
//! records, and the next check drops them from local disk.

use std::time::{Duration, Instant};

use assert2::{assert, check};
use krabka_broker::BrokerHandle;
use krabka_client_core::Client;
use krabka_protocol::owned::list_offsets_request::ListOffsetsRequest;

use crate::{
    rlmm_cluster::{await_activation, build_client, start_broker_with_topic_rlmm},
    rlmm_round_trip::remote_log_files,
    run_broker_test,
    support::{
        offsets::{list_offset_partition, single_partition_list_offsets},
        topics::topic_configs,
    },
    topic_fixture::create_configured_topic,
};

const TOPIC: &str = "tiered-idle-partition-itest";
/// The ten records Kafka's test produces.
const RECORDS: i64 = 10;
/// Kafka's `ListOffsetsRequest.EARLIEST_TIMESTAMP`.
const EARLIEST_TIMESTAMP: i64 = -2;
/// Kafka's `ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP` (KIP-405).
const EARLIEST_LOCAL_TIMESTAMP: i64 = -4;

#[test]
fn an_idle_tiered_partition_leaves_local_disk_through_its_last_record() {
    run_broker_test(an_idle_tiered_partition_leaves_local_disk_through_its_last_record_case());
}

async fn an_idle_tiered_partition_leaves_local_disk_through_its_last_record_case() {
    let (broker, _log_dir, remote_dir) = start_broker_with_topic_rlmm().await;
    await_activation(&broker).await;
    let client = build_client(&broker).await;
    create_idle_tiered_topic(&client).await;
    await_idle_tiered_config(&broker).await;

    broker
        .produce_records_for_test(TOPIC, 0, RECORDS.try_into().unwrap())
        .await
        .expect("produce records");

    // intentional: the local log start moves on the remote-log-manager's
    // timer and has no metric, so poll the wire answer (bounded loop).
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let earliest_local = list_offset(&client, EARLIEST_LOCAL_TIMESTAMP).await;
        if earliest_local == RECORDS {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "the local log start stayed at {earliest_local}, short of the log end {RECORDS}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The records left local disk, and the remote tier still answers for
    // every one of them.
    check!(list_offset(&client, EARLIEST_TIMESTAMP).await == 0);
    check!(!remote_log_files(remote_dir.path()).is_empty());

    drop(client);
    broker.shutdown().await;
}

// A tiered topic whose records outlive `local.retention.ms` at once:
// `produce_records_for_test` stamps no record timestamp, so every segment's
// newest timestamp is 0. `retention.ms=-1` keeps the remote tier from
// deleting them for the same reason.
async fn create_idle_tiered_topic(client: &Client) {
    create_configured_topic(
        client,
        TOPIC,
        topic_configs([
            ("remote.storage.enable", "true"),
            ("local.retention.ms", "1000"),
            ("retention.ms", "-1"),
        ]),
    )
    .await;
}

// intentional: the reconcile loop applies the topic config to the partition's
// `LogConfig` with no awaiter or metric, so poll it (bounded loop).
async fn await_idle_tiered_config(broker: &BrokerHandle) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if broker
            .partition_log_config_for_test(TOPIC, 0)
            .is_some_and(|config| {
                config.remote_storage_enable
                    && config.local_retention == Some(krabka_units::millis(1_000))
            })
        {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "tiered-storage topic config never propagated; saw {:?}",
            broker.partition_log_config_for_test(TOPIC, 0)
        );
        tokio::task::yield_now().await;
    }
}

async fn list_offset(client: &Client, timestamp: i64) -> i64 {
    let mut resp = client
        .send(ListOffsetsRequest {
            replica_id: -1,
            timeout_ms: 5_000,
            ..single_partition_list_offsets(TOPIC, list_offset_partition(0, timestamp))
        })
        .await
        .expect("ListOffsets");
    let partition = resp.topics.remove(0).partitions.remove(0);
    assert!(
        partition.error_code == 0,
        "ListOffsets({timestamp}) failed with error code {}",
        partition.error_code
    );
    partition.offset
}
