//! The lookup for a positive request timestamp.
//!
//! KIP-405 puts the oldest records in the remote tier, so a by-timestamp
//! lookup asks the remote tier first, then compares overlapping local records
//! so an upload gap cannot hide an earlier match.

use std::{future::Future, time::Duration};

use krabka_remote_storage::RemoteStorageError;
use krabka_units::prelude::ByteSizeExt;

use super::{
    remote::await_remote,
    sentinels::{UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP},
};
use crate::{broker::Broker, codes, partition::Partition};

/// Resolve a positive request timestamp to `(offset, record timestamp)`, or to
/// the error code the partition's row carries.
///
/// A compressed record the scan has to decompress that is above the topic's
/// Kafka trunk `max.decompressed.message.bytes` fails the lookup, on the
/// remote tier and on the local log alike, with `INVALID_RECORD`: Kafka's
/// `InvalidRecordException` reaches the response through
/// `Errors.forException`. Under Kafka 4.3.1 the limit is unset and no lookup
/// reads it.
pub(super) async fn resolve_timestamp_offset(
    broker: &Broker,
    partition: &Partition,
    topic_name: &str,
    partition_index: i32,
    topic_id: Option<uuid::Uuid>,
    timestamp: i64,
    remote_timeout: Duration,
) -> Result<(i64, i64), i16> {
    let minimum = partition
        .log
        .lock()
        .expect("log mutex poisoned")
        .log_start_offset()
        .0;
    let remote = broker
        .remote_reader
        .as_ref()
        .zip(topic_id)
        .map(|(reader, id)| {
            let topic_partition = krabka_remote_storage::TopicIdPartition::new(
                id,
                topic_name.to_string(),
                partition_index,
            );
            move |max_record_body| async move {
                reader
                    .offset_for_timestamp(&topic_partition, timestamp, minimum, max_record_body)
                    .await
            }
        });
    lookup_timestamp(
        (partition, topic_name, partition_index),
        timestamp,
        remote_timeout,
        remote,
    )
    .await
}

/// [`resolve_timestamp_offset`] with the remote tier's read handed in, so a test
/// can answer it without a remote segment store.
///
/// `remote` is `None` for a broker with no remote tier. Otherwise it reads the
/// tier for the partition, given the topic's decompressed-record limit in
/// bytes, which this function takes from the partition's log configuration.
async fn lookup_timestamp<Read, Answer>(
    (partition, topic_name, partition_index): (&Partition, &str, i32),
    timestamp: i64,
    remote_timeout: Duration,
    remote: Option<Read>,
) -> Result<(i64, i64), i16>
where
    Read: FnOnce(Option<usize>) -> Answer,
    Answer: Future<Output = Result<Option<(i64, i64)>, RemoteStorageError>>,
{
    let mut remote_found = None;
    if let Some(read) = remote {
        let max_record_body = partition
            .log
            .lock()
            .expect("log mutex poisoned")
            .config_snapshot()
            .max_decompressed_record
            .map(ByteSizeExt::bytes_usize);
        match await_remote(remote_timeout, read(max_record_body)).await {
            None => return Err(codes::REQUEST_TIMED_OUT),
            Some(Ok(Some(offset_and_timestamp))) => remote_found = Some(offset_and_timestamp),
            Some(Ok(None)) => {}
            Some(Err(error @ RemoteStorageError::RecordTooLarge { .. })) => {
                tracing::warn!(topic = topic_name, partition = partition_index,
                    error = %error, "list_offsets: remote offset_for_timestamp refused a record");
                return Err(codes::INVALID_RECORD);
            }
            Some(Err(error)) => tracing::warn!(topic = topic_name, partition = partition_index,
                error = %error, "list_offsets: remote offset_for_timestamp failed"),
        }
    }
    let log = partition.log.lock().expect("log mutex poisoned");
    if let Some(found) = remote_found
        && found.0 < log.local_log_start_offset().0
    {
        // No local record can precede this remote hit. Overlapping tiers
        // need both candidates: failed copies can leave holes in the tier.
        return Ok(found);
    }
    let found = log.offset_for_timestamp_checked(timestamp);
    match found {
        Ok(found) => Ok(krabka_verified::list_offsets::earliest_timestamp_candidate(
            remote_found,
            found.map(|(offset, matched)| (offset.0, matched)),
        )
        .unwrap_or((UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP))),
        Err(error) => {
            tracing::warn!(topic = topic_name, partition = partition_index,
                error = %error, "list_offsets: offset_for_timestamp refused a record");
            Err(codes::from_broker_error(&error.into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::BytesMut;
    use krabka_protocol::owned::list_offsets_response::ListOffsetsPartitionResponse;
    use krabka_remote_storage::{
        LogSegmentData, RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata,
        RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, TopicIdPartition,
    };

    use super::*;
    use crate::{codes, handlers::list_offsets::test_support::list_one};

    /// The remote tier answers first, and what it answers decides the row: a
    /// hit competes with any earlier local match; a miss or a failure reads the local log,
    /// and a record it refused for `max.decompressed.message.bytes` is the
    /// row's `INVALID_RECORD`, as `InvalidRecordException` is in Kafka. The
    /// local log holds a match for the timestamp, so a refusal that fell
    /// through would come back as that match.
    #[tokio::test]
    async fn a_record_the_remote_tier_refuses_answers_invalid_record_and_not_the_local_match() {
        use std::sync::{Arc, Mutex};

        use krabka_protocol::records::{Record, RecordBatch};

        let (partition, _dir) =
            crate::partition::test_support::test_partition(Arc::new(tokio::sync::Notify::new()));
        {
            let mut log = partition.log.lock().expect("partition log lock");
            log.append(&mut RecordBatch {
                base_timestamp: 1_000,
                max_timestamp: 1_000,
                records: vec![Record {
                    value: Some(bytes::Bytes::from_static(b"local")),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .expect("append the local record");
            let config = krabka_log::LogConfig {
                max_decompressed_record: Some(krabka_units::bytes(100)),
                ..log.config_snapshot()
            };
            log.set_config(config);
        }
        let local = Ok((0, 1_000));

        for (name, remote, expected) in [
            (
                "the remote tier refuses a record",
                Some(Err(RemoteStorageError::RecordTooLarge {
                    size: 1_007,
                    limit: 100,
                })),
                Err(codes::INVALID_RECORD),
            ),
            (
                "any other remote failure falls back to the local log",
                Some(Err(RemoteStorageError::Io(std::io::Error::other("boom")))),
                local,
            ),
            (
                "a remote miss falls back to the local log",
                Some(Ok(None)),
                local,
            ),
            (
                "a later remote hit cannot hide the earlier local match",
                Some(Ok(Some((7, 7_000)))),
                local,
            ),
            ("no remote tier reads the local log", None, local),
        ] {
            let limits = Mutex::new(Vec::new());
            let remote = remote.map(|answer| {
                |limit| {
                    limits.lock().expect("limits lock").push(limit);
                    std::future::ready(answer)
                }
            });
            let had_remote = remote.is_some();

            let got =
                lookup_timestamp((&partition, "t", 0), 500, Duration::from_secs(5), remote).await;

            assert!(got == expected, "{name}");
            // The reader is handed the topic's limit in bytes.
            assert!(
                *limits.lock().expect("limits lock")
                    == if had_remote { vec![Some(100)] } else { vec![] },
                "{name}"
            );
        }
    }

    use std::ops::RangeInclusive;

    use krabka_ids::Offset;
    use krabka_units::{ByteSize, bytes};

    use crate::test_support::UnixMillis;

    #[derive(krabka_macros::FieldDefaults)]
    struct TimestampSegmentSetup {
        #[default(TopicIdPartition::new(uuid::Uuid::from_u128(1), "orders", 0))]
        topic_partition: TopicIdPartition,
        #[default(Offset(0)..=Offset(6))]
        bounds: RangeInclusive<Offset>,
        #[default(bytes(64))]
        size: ByteSize,
        #[default(UnixMillis(2_400))]
        event: UnixMillis,
    }

    fn copy_timestamp_segment(
        reader: &crate::remote_reader::RemoteReader,
        data: &LogSegmentData,
        setup: TimestampSegmentSetup,
    ) -> RemoteLogSegmentId {
        let TimestampSegmentSetup {
            topic_partition,
            bounds,
            size,
            event,
        } = setup;
        let id = RemoteLogSegmentId::new(topic_partition, uuid::Uuid::new_v4());
        let metadata = RemoteLogSegmentMetadata::new(
            id.clone(),
            bounds.start().0,
            bounds.end().0,
            2_400,
            1,
            event.0,
            RemoteLogSegmentDetails::new(
                i32::try_from(size.bytes_usize()).expect("segment size"),
                RemoteLogSegmentState::CopySegmentStarted,
                maplit::btreemap! {krabka_ids::LeaderEpoch(0) => bounds.start().0},
            ),
        )
        .expect("segment metadata");
        reader
            .rlmm
            .add_remote_log_segment_metadata(metadata.clone())
            .expect("add segment");
        reader
            .rsm
            .copy_log_segment_data(&metadata, data)
            .expect("copy segment");
        reader
            .rlmm
            .update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
                remote_log_segment_id: id.clone(),
                event_timestamp_ms: event.0,
                custom_metadata: None,
                state: RemoteLogSegmentState::CopySegmentFinished,
                broker_id: 1,
            })
            .expect("finish segment");
        id
    }

    #[tokio::test]
    async fn positive_timestamp_wire_response_returns_exact_remote_record() {
        use bytes::Bytes;
        use krabka_protocol::records::{Record, RecordBatch};

        const TOPIC: &str = "list-offsets-remote-timestamp";

        let (broker, client, _dirs) =
            crate::handlers::list_offsets::test_support::remote_topic(TOPIC, vec![]).await;

        let broker_arc = broker.broker_arc_for_test();
        let partition = broker_arc
            .partitions
            .get(TOPIC, krabka_ids::PartitionIndex(0))
            .expect("partition");
        let source_dir = tempfile::tempdir().expect("source tempdir");
        let batches = [
            (0, &[1_000, 1_100][..]),
            (2, &[1_600, 1_700][..]),
            (4, &[2_000, 2_200, 2_400][..]),
        ];
        let mut log_bytes = BytesMut::new();
        let mut last_position = 0;
        for (base_offset, timestamps) in batches {
            if base_offset == 4 {
                last_position = u32::try_from(log_bytes.len()).expect("segment position");
            }
            let base_timestamp = timestamps[0];
            let batch = RecordBatch {
                base_offset,
                last_offset_delta: i32::try_from(timestamps.len() - 1).expect("record count"),
                base_timestamp,
                max_timestamp: *timestamps.iter().max().expect("timestamps"),
                records: timestamps
                    .iter()
                    .enumerate()
                    .map(|(offset_delta, timestamp)| Record {
                        timestamp_delta: *timestamp - base_timestamp,
                        offset_delta: i32::try_from(offset_delta).expect("offset delta"),
                        value: Some(Bytes::from_static(b"value")),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };
            batch.encode(&mut log_bytes).expect("encode batch");
            assert!(
                partition
                    .produce_batch(batch)
                    .await
                    .expect("local append")
                    .0
                    == base_offset
            );
        }
        let log_path = source_dir.path().join("segment.log");
        let offset_index_path = source_dir.path().join("segment.index");
        let time_index_path = source_dir.path().join("segment.timeindex");
        std::fs::write(&log_path, &log_bytes).expect("write log");
        std::fs::write(
            &offset_index_path,
            [
                0_u32.to_be_bytes(),
                0_u32.to_be_bytes(),
                4_u32.to_be_bytes(),
                last_position.to_be_bytes(),
            ]
            .concat(),
        )
        .expect("write offset index");
        let mut time_index = Vec::new();
        time_index.extend_from_slice(&1_100_i64.to_be_bytes());
        time_index.extend_from_slice(&0_u32.to_be_bytes());
        time_index.extend_from_slice(&2_400_i64.to_be_bytes());
        time_index.extend_from_slice(&4_u32.to_be_bytes());
        std::fs::write(&time_index_path, time_index).expect("write time index");

        let topic_id = broker_arc
            .controller
            .current_image()
            .topic(TOPIC)
            .expect("topic metadata")
            .topic_id;
        let topic_partition = TopicIdPartition::new(topic_id, TOPIC, 0);
        let reader = broker_arc.remote_reader.as_ref().expect("remote reader");
        let segment_id = copy_timestamp_segment(
            reader,
            &LogSegmentData {
                log_segment: log_path.clone(),
                offset_index: offset_index_path.clone(),
                time_index: time_index_path.clone(),
                transaction_index: None,
                producer_snapshot_index: None,
                leader_epoch_index: Bytes::from_static(b"0\n1\n0 0\n"),
            },
            TimestampSegmentSetup {
                topic_partition: topic_partition.clone(),
                size: ByteSize::from_bytes(u64::try_from(log_bytes.len()).expect("segment size")),
                ..Default::default()
            },
        );

        // Publish the exclusive committed end of the seven produced records.
        partition.replica_state.lock().await.hw = krabka_log::Offset(7);

        assert!(
            list_one(&client, TOPIC, 1_500).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: 1_600,
                    offset: 2,
                    // The partition's leader recorded epoch 0 at promotion,
                    // and the remote segment's epoch index agrees: epoch 0
                    // covers offset 2.
                    leader_epoch: 0,
                    ..Default::default()
                }
        );

        // A failed earlier copy can leave only a later segment finished.
        // Local records still cover the gap; the remote hit at 4 must not
        // hide the earlier local match at 2.
        reader
            .rlmm
            .update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
                remote_log_segment_id: segment_id,
                event_timestamp_ms: 2_500,
                custom_metadata: None,
                state: RemoteLogSegmentState::DeleteSegmentStarted,
                broker_id: 1,
            })
            .expect("hide the full remote copy");
        let suffix = &log_bytes[usize::try_from(last_position).expect("position")..];
        std::fs::write(&log_path, suffix).expect("write suffix");
        std::fs::write(&offset_index_path, [0_u8; 8]).expect("write suffix offset index");
        std::fs::write(
            &time_index_path,
            [
                2_400_i64.to_be_bytes().as_slice(),
                0_u32.to_be_bytes().as_slice(),
            ]
            .concat(),
        )
        .expect("write suffix time index");
        copy_timestamp_segment(
            reader,
            &LogSegmentData {
                log_segment: log_path,
                offset_index: offset_index_path,
                time_index: time_index_path,
                transaction_index: None,
                producer_snapshot_index: None,
                leader_epoch_index: Bytes::from_static(b"0\n1\n0 4\n"),
            },
            TimestampSegmentSetup {
                topic_partition,
                bounds: Offset(4)..=Offset(6),
                size: ByteSize::from_bytes(u64::try_from(suffix.len()).expect("segment size")),
                event: UnixMillis(2_500),
            },
        );
        let response = list_one(&client, TOPIC, 1_500).await;
        assert!(
            response.error_code == codes::NONE
                && response.offset == 2
                && response.timestamp == 1_600,
            "{response:?}"
        );

        for (floor, expected_timestamp) in [(4, 2_000), (5, 2_200)] {
            partition
                .log
                .lock()
                .expect("log mutex")
                .set_log_start_offset(krabka_ids::Offset(floor))
                .expect("logical trim");
            let response = list_one(&client, TOPIC, 1_500).await;
            assert!(
                response.error_code == codes::NONE
                    && response.offset == floor
                    && response.timestamp == expected_timestamp,
                "{response:?}"
            );
        }

        drop(client);
        broker.shutdown().await;
    }
}
