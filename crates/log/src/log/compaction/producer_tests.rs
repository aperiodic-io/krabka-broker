//! What a compaction pass keeps for the producers of the log.
//!
//! Kafka's `Cleaner.cleanSegments` reads the last record of each active
//! producer from the producer state of the log
//! (`UnifiedLog.lastRecordsOfActiveProducers`), and `Cleaner.cleanInto` keeps
//! that record, as an empty batch when compaction removes all of its records.
//! Every append path updates the producer state, so a leader, a follower and a
//! follower that appends verbatim keep the same batches.

use {
    super::*,
    crate::{
        CleanupPolicy,
        config::LogConfig,
        log::test_support::{
            AppendPath as Path, append_path as append, commit_marker, compaction_ctx,
            set_segment_size, tiny_segments,
        },
    },
    krabka_protocol::records::RecordBatch,
    krabka_units::prelude::{Time, mebibytes},
    tempfile::tempdir,
};

krabka_macros::compacted_batch!(Kept);

krabka_macros::compaction_record_fixture!(record);

/// Two producer writes, then three ordinary writes that supersede their keys.
fn overwritten_producer_records(epoch: i16, sequence: i32) -> Vec<RecordBatch> {
    vec![
        record(Some((7, 0, 0)), false, ("a", "p-a"), 0),
        record(Some((7, epoch, sequence)), false, ("b", "p-b"), 0),
        record(None, false, ("a", "a-2"), 0),
        record(None, false, ("b", "b-3"), 0),
        record(None, false, ("c", "c-4"), 0),
    ]
}

/// Relocate the explicitly selected, unchanged batches to their expected offsets.
fn whole_batches(batches: &[RecordBatch], offsets: std::ops::Range<usize>) -> Vec<Kept> {
    offsets
        .map(|offset| Kept::whole(i64::try_from(offset).unwrap(), &batches[offset]))
        .collect()
}

fn header_and_whole(
    batches: &[RecordBatch],
    header: usize,
    whole: std::ops::Range<usize>,
) -> Vec<Kept> {
    std::iter::once(Kept::header(
        i64::try_from(header).unwrap(),
        &batches[header],
    ))
    .chain(whole_batches(batches, whole))
    .collect()
}

/// Kafka's cleaner keeps the last record of each producer in the producer
/// state of the log. That record is the last data batch of the producer at
/// its epoch, or a marker at its epoch when a marker at a new epoch has
/// cleared its batches. Kafka keeps no batch of a producer that
/// `removeExpiredProducers` removed. Each case ends with a batch that stays in
/// the active segment, which no pass rewrites, and the three passes take a
/// commit marker past its delete horizon.
#[test]
fn compaction_keeps_the_last_record_of_each_producer_in_the_log() {
    let superseded = overwritten_producer_records(0, 1);
    let new_epoch = overwritten_producer_records(1, 0);
    // Transaction version 1 commits at the epoch of the transaction, and
    // transaction version 2 at the next epoch.
    let same_epoch_commit = vec![
        record(Some((9, 2, 0)), true, ("a", "t-a"), 0),
        commit_marker(9, 2),
        record(None, false, ("a", "a-2"), 0),
        record(None, false, ("c", "c-3"), 0),
    ];
    let next_epoch_commit = vec![
        record(Some((10, 2, 0)), true, ("a", "t-a"), 0),
        commit_marker(10, 3),
        record(None, false, ("a", "a-2"), 0),
        record(None, false, ("c", "c-3"), 0),
    ];
    // (case, batches, whether the producers expire first, the kept batches)
    let cases: Vec<(&str, &Vec<RecordBatch>, bool, Vec<Kept>)> = vec![
        (
            "the last batch of an idempotent producer",
            &superseded,
            false,
            header_and_whole(&superseded, 1, 2..5),
        ),
        (
            "an expired idempotent producer",
            &superseded,
            true,
            whole_batches(&superseded, 2..5),
        ),
        (
            "the last batch at the new epoch of a producer",
            &new_epoch,
            false,
            header_and_whole(&new_epoch, 1, 2..5),
        ),
        // The data batch is the last record of the producer, so every pass
        // meets a batch of the transaction and the marker stays.
        (
            "a commit marker at the epoch of the transaction",
            &same_epoch_commit,
            false,
            header_and_whole(&same_epoch_commit, 0, 1..4),
        ),
        // The marker is the last record of the producer: it holds the epoch
        // that fences the producer.
        (
            "a commit marker at the next epoch",
            &next_epoch_commit,
            false,
            header_and_whole(&next_epoch_commit, 1, 2..4),
        ),
        (
            "a commit marker of an expired producer",
            &next_epoch_commit,
            true,
            whole_batches(&next_epoch_commit, 2..4),
        ),
    ];
    for (name, batches, expired, want) in cases {
        for path in [Path::Leader, Path::Follower, Path::Verbatim] {
            let dir = tempdir().unwrap();
            // One batch for each segment, and no delete retention, so that the
            // third pass finds the delete horizon of a marker passed.
            let mut log = Log::open(dir.path(), LogConfig {
                cleanup_policy: CleanupPolicy::Compact,
                delete_retention: Time::ZERO,
                ..tiny_segments()
            })
            .unwrap();
            for batch in batches.iter().cloned() {
                append(&mut log, path, batch);
            }
            // The pass groups by `segment.bytes`; widen it so that a pass
            // merges the sealed segments.
            set_segment_size(&mut log, mebibytes(1));
            if expired {
                log.remove_expired_producers(i64::MAX, 1);
            }

            for _ in 0..3 {
                log.compact(&compaction_ctx()).unwrap();
            }

            let kept: Vec<Kept> = log
                .read(Offset(0), mebibytes(1))
                .unwrap()
                .batches
                .iter()
                .map(Kept::of)
                .collect();
            assert2::check!(kept == want, "{name}, {path:?}");
        }
    }
}
