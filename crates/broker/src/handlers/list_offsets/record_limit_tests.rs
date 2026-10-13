//! Kafka trunk's `max.decompressed.message.bytes` on a by-timestamp
//! `ListOffsets`.
//!
//! `UnifiedLog.fetchOffsetByTimestamp` decompresses the batch a timestamp or
//! `MAX_TIMESTAMP` lookup lands in, and a record above the topic's limit throws
//! `InvalidRecordException`, which `Errors.forException` answers as
//! `INVALID_RECORD`. Kafka 4.3.1 has no such key, so a broker that does not
//! serve trunk's keys answers every lookup as it always did.
//!
//! The records go straight into the partition's log, past `Produce`'s own
//! check, the way a record that predates a lowered limit is already there.

use {
    super::{
        sentinels::{MAX_TIMESTAMP, UNKNOWN_EPOCH, UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP},
        test_support::{client_for, create_topic, list_one},
    },
    crate::{
        api_catalog::UnstableApiVersions, broker::BrokerHandle, codes,
        test_support::start_broker_no_audit_with,
    },
    assert2::assert,
    bytes::Bytes,
    krabka_compression::CompressionType,
    krabka_ids::PartitionIndex,
    krabka_metadata::{BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataRecord},
    krabka_protocol::{
        owned::list_offsets_response::ListOffsetsPartitionResponse,
        records::{Attributes, Record, RecordBatch},
    },
    std::sync::atomic::Ordering,
};

const TOPIC: &str = "list-offsets-record-limit";
const KEY: &str = "max.decompressed.message.bytes";
/// A `log.retention.ms` that rides with the limit, so a test can tell the
/// partition has taken the broker configs even where the limit stays unset.
const RETENTION_KEY: &str = "log.retention.ms";

/// A broker with one topic holding two gzip batches: a small record stamped
/// 1000 at offset 0, and a record with a 1000-byte value stamped 2000 at
/// offset 1.
async fn broker_with_records(
    unstable: UnstableApiVersions,
) -> (BrokerHandle, krabka_client_core::Client, tempfile::TempDir) {
    let (broker, dir) = start_broker_no_audit_with(move |config| {
        config.features.unstable_api_versions = unstable;
    })
    .await;
    let client = client_for(&broker).await;
    create_topic(&client, TOPIC, Vec::new()).await;
    broker.wait_until_partition_present(TOPIC, 0).await;
    let shared = broker.broker_arc_for_test();
    let partition = shared
        .partitions
        .get(TOPIC, PartitionIndex(0))
        .expect("partition");
    for (timestamp, value_len) in [(1_000, 10), (2_000, 1_000)] {
        let batch = RecordBatch {
            partition_leader_epoch: partition.current_leader_epoch.load(Ordering::Acquire),
            base_timestamp: timestamp,
            max_timestamp: timestamp,
            attributes: Attributes::default().with_compression(CompressionType::Gzip),
            records: vec![Record {
                value: Some(Bytes::from(vec![7_u8; value_len])),
                ..Default::default()
            }],
            ..Default::default()
        };
        partition.produce_batch(batch).await.expect("append");
    }
    // A lookup is answered up to the high watermark.
    partition.replica_state.lock().await.hw = krabka_log::Offset(2);
    (broker, client, dir)
}

/// Store the cluster-wide `max.decompressed.message.bytes`, and wait until the
/// partition's log has taken the `log.retention.ms` of `generation`, which is
/// stored with it in one metadata change. The wait is on the retention, not on
/// the limit, so it also ends on a broker that leaves the limit unset.
async fn set_cluster_limit(broker: &BrokerHandle, limit: &str, generation: u32) {
    let retention_ms: u32 = 86_400_000 + generation;
    let records = [
        (KEY, limit.to_owned()),
        (RETENTION_KEY, retention_ms.to_string()),
    ]
    .into_iter()
    .map(|(name, value)| {
        MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
            config_name: name.to_owned(),
            config_value: Some(value),
        })
    })
    .collect::<Vec<_>>();
    let retention = krabka_units::millis(retention_ms);
    broker
        .broker_arc_for_test()
        .controller
        .submit_change(records)
        .await
        .expect("submit the broker configs");
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if broker
                .partition_log_config_for_test(TOPIC, 0)
                .is_some_and(|config| config.retention == Some(retention))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the broker configs reached the partition log");
}

fn found(offset: i64, timestamp: i64) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse {
        partition_index: 0,
        error_code: codes::NONE,
        timestamp,
        offset,
        leader_epoch: 0,
        ..Default::default()
    }
}

fn refused() -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse {
        partition_index: 0,
        error_code: codes::INVALID_RECORD,
        timestamp: UNKNOWN_TIMESTAMP,
        offset: UNKNOWN_OFFSET,
        leader_epoch: UNKNOWN_EPOCH,
        ..Default::default()
    }
}

/// A limit under the size of the stored record fails the lookups that have to
/// read it and no other, and raising it answers them again.
#[tokio::test]
async fn a_trunk_broker_refuses_a_lookup_that_reads_an_oversized_record() {
    let (broker, client, _dir) = broker_with_records(UnstableApiVersions::Enabled).await;

    // No limit is set, so nothing is refused.
    assert!(list_one(&client, TOPIC, 1_500).await == found(1, 2_000));

    set_cluster_limit(&broker, "100", 1).await;
    for (name, timestamp, expected) in [
        // The small batch is the match, and the oversized one is never read.
        ("a match before the oversized batch", 1_000, found(0, 1_000)),
        // The first batch tops out at 1000, so the oversized batch is the match.
        ("the match is oversized", 1_500, refused()),
        // The maximum is in the oversized batch.
        ("MAX_TIMESTAMP", MAX_TIMESTAMP, refused()),
    ] {
        assert!(
            list_one(&client, TOPIC, timestamp).await == expected,
            "{name}"
        );
    }

    // The record body is 1007 bytes, so a limit of exactly that admits it.
    set_cluster_limit(&broker, "1007", 2).await;
    check_large_record_lookups(&client).await;

    drop(client);
    broker.shutdown().await;
}

/// Kafka 4.3.1 has no `max.decompressed.message.bytes`: a broker that does not
/// serve trunk's keys stores the broker config as an unknown key and reads none
/// of it, so a lookup answers as it always did.
#[tokio::test]
async fn a_default_broker_ignores_the_limit_on_a_lookup() {
    let (broker, client, _dir) = broker_with_records(UnstableApiVersions::Disabled).await;

    set_cluster_limit(&broker, "100", 1).await;

    check_large_record_lookups(&client).await;
    assert!(
        broker
            .partition_log_config_for_test(TOPIC, 0)
            .expect("the partition is local")
            .max_decompressed_record
            == None
    );

    drop(client);
    broker.shutdown().await;
}

async fn check_large_record_lookups(client: &krabka_client_core::Client) {
    for timestamp in [1_500, MAX_TIMESTAMP] {
        assert!(list_one(client, TOPIC, timestamp).await == found(1, 2_000));
    }
}
