//! Promotion of a WAL follower to the canonical partition log. When this broker
//! takes over a diskless shard it copies its own checkpointed follower prefix
//! into the partition log, after it has checked that the two agree byte for byte
//! on the range they share.

use std::path::PathBuf;

use krabka_ids::Offset;
use krabka_log::{Log, LogConfig};
use krabka_raft::NodeId;
use krabka_verified::broker::DeleteRecordsTrimApplication;

use super::log::{FollowerLog, voter_dir};
use crate::{
    BrokerError,
    wal::quorum::{
        engine::{read_batches_exact, read_log_batches_covering},
        registry::ShardId,
    },
};

/// Copy this broker's checkpointed follower prefix into a newly promoted
/// partition log. The follower retains the reconciled logical floor durably
/// before physical rebasing, so a restart can retry from the same source.
pub(crate) fn hydrate_on_promotion(
    log_dirs: &[PathBuf],
    topic: &str,
    shard: ShardId,
    node_id: NodeId,
    storage: &LogConfig,
    destination: &mut Log,
) -> Result<Option<Offset>, BrokerError> {
    let Some(dir) = log_dirs
        .iter()
        .map(|root| voter_dir(root, topic, shard, node_id))
        .find(|candidate| candidate.exists())
    else {
        return Ok(None);
    };
    let follower = FollowerLog::open_at(dir, storage)?;
    let source_start = follower.start_offset();
    let source_end = follower.end_offset();
    let destination_start = destination.log_start_offset();
    let destination_end = destination.log_end_offset();

    let floor = match krabka_verified::broker::delete_records_trim_application(
        0,
        source_start.0,
        destination_start.0,
    ) {
        DeleteRecordsTrimApplication::RejectMalformed => {
            return Err(BrokerError::Replication(
                "promoted WAL has a malformed logical floor".into(),
            ));
        }
        DeleteRecordsTrimApplication::TrimWal { frontier }
        | DeleteRecordsTrimApplication::TrimLocal { frontier }
        | DeleteRecordsTrimApplication::Complete { frontier } => Offset(frontier),
    };

    if destination_start == destination_end && destination_end < source_end {
        // Preserve the logical floor in the durable source before rebasing the
        // empty destination. A restart can then recover it after any partial copy.
        follower.trim_to_blocking(floor)?;
        let copy_start = follower.start_offset();
        let batches = read_log_batches_covering(&follower.log.lock(), copy_start, source_end)?;
        let physical_start = batches
            .first()
            .map_or(source_start, |batch| batch.base_offset);
        destination.reset_to(physical_start)?;
        for batch in batches {
            destination.append_verbatim_at(&batch.verbatim, batch.base_offset)?;
        }
    } else {
        let overlap_start = source_start.max(destination_start);
        let overlap_end = source_end.min(destination_end);
        if overlap_start < overlap_end {
            let source =
                read_log_batches_covering(&follower.log.lock(), overlap_start, overlap_end)?;
            let current = read_log_batches_covering(destination, overlap_start, overlap_end)?;
            if source.len() != current.len()
                || source.iter().zip(&current).any(|(source, current)| {
                    !krabka_verified::wal::wal_batch_equal(
                        (
                            source.base_offset.0,
                            source.last_offset.0,
                            &source.verbatim.bytes,
                        ),
                        (
                            current.base_offset.0,
                            current.last_offset.0,
                            &current.verbatim.bytes,
                        ),
                    )
                })
            {
                return Err(BrokerError::Replication(format!(
                    "promoted WAL follower diverges from canonical log in {}..{}",
                    overlap_start.0, overlap_end.0
                )));
            }
        } else if destination_end < source_start {
            return Err(BrokerError::Replication(format!(
                "promoted WAL follower starts at {}, after canonical LEO {}",
                source_start.0, destination_end.0
            )));
        }
    }

    if destination.log_end_offset() < source_end {
        let batches = read_batches_exact(&follower.log, destination.log_end_offset(), source_end)?;
        for batch in batches {
            destination.append_verbatim_at(&batch.verbatim, batch.base_offset)?;
        }
    }
    if destination.log_end_offset() < source_end {
        return Err(BrokerError::Replication(format!(
            "promoted WAL hydration ended at {}, before durable follower LEO {}",
            destination.log_end_offset().0,
            source_end.0
        )));
    }
    destination.trim_to_offset(floor)?;
    destination.sync()?;
    Ok(Some(source_end))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;
    use krabka_ids::PartitionIndex;
    use krabka_protocol::records::{Record, RecordBatch};
    use krabka_units::mebibytes;

    use super::*;
    use crate::{
        log_dir::partition_dir,
        wal::quorum::{
            engine::{read_log_batches_exact, split_batches},
            follower::checkpoint::{DURABLE_OFFSET_FILE, DurableRange, write_durable_offset},
        },
    };

    macro_rules! follower_fixture {
        ($root:ident, $shard:ident, $directory:ident, $source:ident; $topic_id:expr) => {
            let $root = tempfile::tempdir().unwrap();
            let $shard = ShardId {
                topic_id: uuid::Uuid::from_u128($topic_id),
                partition: PartitionIndex(0),
            };
            let $directory = voter_dir($root.path(), "diskless", $shard, NodeId(2));
            let mut $source = Log::open(&$directory, LogConfig::default()).unwrap();
        };
    }

    macro_rules! checkpoint_follower {
        ($directory:ident, $start:expr, $end:expr) => {
            write_durable_offset(
                &$directory.join(DURABLE_OFFSET_FILE),
                DurableRange {
                    start: $start,
                    end: $end,
                },
            )
            .unwrap()
        };
    }

    macro_rules! hydrate_follower {
        ($root:ident, $shard:ident, $destination:ident) => {
            hydrate_on_promotion(
                &[$root.path().to_path_buf()],
                "diskless",
                $shard,
                NodeId(2),
                &LogConfig::default(),
                &mut $destination,
            )
        };
    }

    #[test]
    fn promotion_preserves_control_marker_state_before_and_after_reopen() {
        use krabka_ids::ProducerId;
        use krabka_protocol::records::Attributes;

        for marker_type in [0i16, 1, 1000, 23] {
            follower_fixture!(root, shard, follower_dir, source; 106);
            let mut data = RecordBatch {
                producer_id: 7,
                producer_epoch: 0,
                base_sequence: 0,
                attributes: Attributes::default().with_transactional(true),
                records: vec![Record::default()],
                ..RecordBatch::default()
            };
            source.append(&mut data).unwrap();
            let mut key = Vec::from(0i16.to_be_bytes());
            key.extend_from_slice(&marker_type.to_be_bytes());
            let mut marker = RecordBatch {
                producer_id: 7,
                producer_epoch: 0,
                base_sequence: if marker_type == 1000 { 77 } else { -1 },
                attributes: Attributes::default()
                    .with_control(true)
                    .with_transactional(marker_type != 1000),
                records: vec![Record {
                    key: Some(Bytes::from(key)),
                    value: Some(Bytes::from_static(&[0, 0, 0, 0, 0, 17])),
                    ..Record::default()
                }],
                ..RecordBatch::default()
            };
            source.append(&mut marker).unwrap();
            source
                .append(&mut RecordBatch {
                    records: vec![Record::default()],
                    ..RecordBatch::default()
                })
                .unwrap();
            source.sync().unwrap();
            let end = source.log_end_offset();
            let expected_lso = source.last_stable_offset(end);
            let expected_producers = source.producer_state_snapshot();
            let expected_marker = source.transaction_marker_state(ProducerId(7));
            let expected_aborts = source.aborted_in_range(Offset(0), end);
            let expected_bytes = source.read_raw(Offset(0), end, mebibytes(1)).unwrap().bytes;
            checkpoint_follower!(follower_dir, Offset(0), end);
            drop(source);
            let destination_dir = partition_dir(root.path(), "diskless", 0);
            let mut destination = Log::open(&destination_dir, LogConfig::default()).unwrap();
            for _ in 0..2 {
                assert!(hydrate_follower!(root, shard, destination).unwrap() == Some(end));
                assert!(
                    destination.last_stable_offset(end) == expected_lso,
                    "marker type {marker_type}"
                );
                assert!(destination.producer_state_snapshot() == expected_producers);
                assert!(destination.transaction_marker_state(ProducerId(7)) == expected_marker);
                assert!(destination.aborted_in_range(Offset(0), end) == expected_aborts);
                assert!(
                    destination
                        .read_raw(Offset(0), end, mebibytes(1))
                        .unwrap()
                        .bytes
                        == expected_bytes
                );
                drop(destination);
                destination = Log::open(&destination_dir, LogConfig::default()).unwrap();
            }
        }
    }

    #[test]
    fn promotion_preserves_interior_floors_and_whole_batches_through_retry() {
        #[derive(Debug)]
        struct InterruptedCopy;
        impl krabka_log::LogIo for InterruptedCopy {
            fn write(&self, _file: &std::fs::File, _buf: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("promotion copy interrupted"))
            }
            fn write_vectored(
                &self,
                _file: &std::fs::File,
                _buf: &[std::io::IoSlice<'_>],
            ) -> std::io::Result<usize> {
                Err(std::io::Error::other("promotion copy interrupted"))
            }
        }
        for (copied_batches, destination_floor) in [(0, 0), (0, 2), (0, 4), (1, 0), (1, 2), (2, 0)]
        {
            follower_fixture!(root, shard, follower_dir, follower; 105);
            for _ in 0..2 {
                let mut batch = RecordBatch {
                    last_offset_delta: 2,
                    records: (0..3)
                        .map(|offset_delta| Record {
                            offset_delta,
                            ..Record::default()
                        })
                        .collect(),
                    ..RecordBatch::default()
                };
                follower.append(&mut batch).unwrap();
            }
            let full_bytes = follower
                .read_raw(Offset(0), Offset(6), mebibytes(1))
                .unwrap()
                .bytes;
            let destination_dir = partition_dir(root.path(), "diskless", 0);
            let mut destination = Log::open(&destination_dir, LogConfig::default()).unwrap();
            for batch in
                read_log_batches_exact(&follower, Offset(0), Offset(copied_batches * 3)).unwrap()
            {
                destination
                    .append_verbatim_at(&batch.verbatim, batch.base_offset)
                    .unwrap();
            }
            if copied_batches == 0 {
                destination.reset_to(Offset(destination_floor)).unwrap();
            } else {
                destination
                    .trim_to_offset(Offset(destination_floor))
                    .unwrap();
            }
            follower.trim_to_offset(Offset(1)).unwrap();
            follower.sync().unwrap();
            checkpoint_follower!(follower_dir, Offset(1), Offset(6));
            drop(follower);

            if copied_batches == 0 && destination_floor > 1 {
                destination.test_set_io(std::sync::Arc::new(InterruptedCopy));
                let error = hydrate_follower!(root, shard, destination).unwrap_err();
                assert!(error.to_string().contains("promotion copy interrupted"));
                assert!(
                    std::fs::read_to_string(follower_dir.join(DURABLE_OFFSET_FILE)).unwrap()
                        == format!("0\n{destination_floor} 6\n")
                );
                drop(destination);
                destination = Log::open(&destination_dir, LogConfig::default()).unwrap();
            }

            for _ in 0..2 {
                assert!(hydrate_follower!(root, shard, destination).unwrap() == Some(Offset(6)));
                let floor = Offset(destination_floor.max(1));
                assert!(destination.log_start_offset() == floor);
                assert!(destination.log_end_offset() == Offset(6));
                let expected = if destination_floor >= 3 {
                    split_batches(&full_bytes).unwrap()[1]
                        .verbatim
                        .bytes
                        .clone()
                } else {
                    full_bytes.clone()
                };
                assert!(
                    destination
                        .read_raw(floor, Offset(6), mebibytes(1))
                        .unwrap()
                        .bytes
                        == expected
                );
                drop(destination);
                destination = Log::open(&destination_dir, LogConfig::default()).unwrap();
            }
        }
    }

    #[test]
    fn promotion_hydrates_exact_checkpointed_bytes_without_regression() {
        follower_fixture!(root, shard, follower_dir, follower; 101);
        let mut durable = RecordBatch {
            records: vec![
                Record {
                    value: Some(Bytes::from_static(b"a")),
                    ..Record::default()
                },
                Record {
                    offset_delta: 1,
                    value: Some(Bytes::from_static(b"b")),
                    ..Record::default()
                },
            ],
            last_offset_delta: 1,
            ..RecordBatch::default()
        };
        follower.append(&mut durable).unwrap();
        follower.sync().unwrap();
        checkpoint_follower!(follower_dir, Offset(0), Offset(2));
        let mut uncertain = RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"uncertain")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        };
        follower.append(&mut uncertain).unwrap();
        follower.sync().unwrap();
        drop(follower);

        let destination_dir = partition_dir(root.path(), "diskless", 0);
        let mut destination = Log::open(&destination_dir, LogConfig::default()).unwrap();
        assert!(hydrate_follower!(root, shard, destination).unwrap() == Some(Offset(2)));
        assert!(destination.log_end_offset() == Offset(2));
        let source = Log::open(&follower_dir, LogConfig::default()).unwrap();
        assert!(source.log_end_offset() == Offset(2));
        assert!(
            source
                .read_raw(Offset(0), Offset(2), mebibytes(1))
                .unwrap()
                .bytes
                == destination
                    .read_raw(Offset(0), Offset(2), mebibytes(1))
                    .unwrap()
                    .bytes
        );

        let mut newer = RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"newer")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        };
        destination.append(&mut newer).unwrap();
        destination.sync().unwrap();
        assert!(hydrate_follower!(root, shard, destination).unwrap() == Some(Offset(2)));
        assert!(destination.log_end_offset() == Offset(3));
        assert!(follower_dir.exists());
    }

    #[test]
    fn promotion_rejects_equal_length_different_bytes_without_mutating_destination() {
        let root = tempfile::tempdir().unwrap();
        let shard = ShardId {
            topic_id: uuid::Uuid::from_u128(103),
            partition: PartitionIndex(0),
        };
        let config = LogConfig::default();
        let follower_dir = voter_dir(root.path(), "diskless", shard, NodeId(2));
        let mut follower = Log::open(&follower_dir, config.clone()).unwrap();
        let mut durable = RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"source")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        };
        follower.append(&mut durable).unwrap();
        follower.sync().unwrap();
        checkpoint_follower!(follower_dir, Offset(0), Offset(1));
        let source_bytes = follower
            .read_raw(Offset(0), Offset(1), mebibytes(1))
            .unwrap()
            .bytes;
        drop(follower);
        let destination_dir = partition_dir(root.path(), "diskless", 0);
        let mut destination = Log::open(&destination_dir, config.clone()).unwrap();
        let mut different = RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"target")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        };
        destination.append(&mut different).unwrap();
        destination.sync().unwrap();
        let before = destination
            .read_raw(Offset(0), Offset(1), mebibytes(1))
            .unwrap()
            .bytes;
        assert!(before.len() == source_bytes.len() && before != source_bytes);
        let error = hydrate_on_promotion(
            &[root.path().to_path_buf()],
            "diskless",
            shard,
            NodeId(2),
            &config,
            &mut destination,
        )
        .unwrap_err();
        assert!(error.to_string().contains("diverges from canonical log"));
        assert!(
            destination.log_start_offset() == Offset(0)
                && destination.log_end_offset() == Offset(1)
        );
        assert!(
            destination
                .read_raw(Offset(0), Offset(1), mebibytes(1))
                .unwrap()
                .bytes
                == before
        );
        assert!(follower_dir.exists());
    }

    #[test]
    fn promotion_retries_after_reopening_a_partial_destination() {
        follower_fixture!(root, shard, follower_dir, follower; 102);
        let mut first = RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"first")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        };
        let mut second = RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"second")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        };
        follower.append(&mut first).unwrap();
        follower.append(&mut second).unwrap();
        follower.sync().unwrap();
        checkpoint_follower!(follower_dir, Offset(0), Offset(2));

        let destination_dir = partition_dir(root.path(), "diskless", 0);
        {
            let mut partial = Log::open(&destination_dir, LogConfig::default()).unwrap();
            let prefix = read_log_batches_exact(&follower, Offset(0), Offset(1)).unwrap();
            partial
                .append_verbatim_at(&prefix[0].verbatim, prefix[0].base_offset)
                .unwrap();
            partial.sync().unwrap();
        }

        // Model a process restart after only the first durable batch was
        // adopted. Reopening the canonical directory and retrying hydration
        // must retain the exact prefix and append the missing durable tail.
        let mut reopened = Log::open(&destination_dir, LogConfig::default()).unwrap();
        assert!(hydrate_follower!(root, shard, reopened).unwrap() == Some(Offset(2)));
        assert!(reopened.log_end_offset() == Offset(2));
        assert!(
            follower
                .read_raw(Offset(0), Offset(2), mebibytes(1))
                .unwrap()
                .bytes
                == reopened
                    .read_raw(Offset(0), Offset(2), mebibytes(1))
                    .unwrap()
                    .bytes
        );
    }
}
