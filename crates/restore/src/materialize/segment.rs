//! Writing one verified segment into its target partition, under the bound.
//!
//! This module owns the log side of a restore: it opens the target partition's
//! [`Log`], aligns its end offset to the segment's base offset, walks the
//! archived batches in order, and appends what
//! [`prepare_batch`](super::prepare::prepare_batch) prepared for each one. It
//! also defines [`SegmentOutcome`], the per-segment tally the report renders.
//! Under `--dry-run` it does the same work and opens no log at all.

use krabka_ids::Offset;
use krabka_log::{Log, LogConfig, name};
use krabka_protocol::records::RecordBatchBorrowed;
use krabka_remote_storage::TopicIdPartition;
use serde::Serialize;
use uuid::Uuid;

use super::prepare::{BatchTally, PreparedBatch, prepare_batch};
use crate::{
    args::{PartitionRef, RestoreArgs},
    bound::Predicates,
    error::RestoreError,
    verify::VerifiedSegment,
};

/// What writing one segment into the target produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SegmentOutcome {
    /// The per-copy segment id in the archive.
    pub segment_id: Uuid,
    /// First offset the written segment holds.
    pub base_offset: Offset,
    /// Last offset the written segment holds.
    pub end_offset: Offset,
    /// Batches written unchanged.
    pub batches_kept: u64,
    /// Batches re-encoded because the bound dropped some of their records.
    pub batches_rewritten: u64,
    /// Batches written as a bare header because the bound excluded every
    /// record. The offset range stays claimed; see
    /// [`crate::bound::BatchDecision::Empty`].
    pub batches_emptied: u64,
    /// Records written.
    pub records_kept: u64,
    /// Records the bound dropped.
    pub records_dropped: u64,
    /// Bytes written into the target segment.
    pub bytes_written: u64,
}

/// Write one verified segment into its target partition, under the bound.
///
/// # Errors
///
/// Returns [`RestoreError::Records`] when a batch will not re-encode, and
/// [`RestoreError::Log`] or [`RestoreError::Io`] when the target rejects the
/// write.
pub async fn write_segment(
    args: &RestoreArgs,
    partition: &TopicIdPartition,
    segment: &VerifiedSegment,
    predicates: &Predicates,
) -> Result<SegmentOutcome, RestoreError> {
    let partition_ref = PartitionRef {
        topic: partition.topic.clone(),
        partition: partition.partition,
    };
    let mut outcome = SegmentOutcome {
        segment_id: segment.facts.segment_id,
        base_offset: segment.facts.base_offset,
        end_offset: segment.facts.end_offset,
        batches_kept: 0,
        batches_rewritten: 0,
        batches_emptied: 0,
        records_kept: 0,
        records_dropped: 0,
        bytes_written: 0,
    };

    // Every batch in this segment starts past the keep bound: an earlier
    // segment already ended the partition's restored history, so nothing
    // here survives and the target log is never opened for it.
    if predicates.batch_past_offset_bound(&partition_ref, segment.facts.base_offset) {
        return Ok(outcome);
    }

    let mut log = if args.dry_run {
        None
    } else {
        let dir = name::partition_dir(&args.target.log_dir, &partition.topic, partition.partition);
        std::fs::create_dir_all(&dir)?;
        Some(Log::open(&dir, LogConfig::default())?)
    };
    if let Some(log) = log.as_mut() {
        align_log_to_segment(log, segment, partition)?;
    }

    let raw: &[u8] = &segment.log;
    let mut pos = 0usize;
    while pos < raw.len() {
        let mut cursor = &raw[pos..];
        let remaining_before = cursor.len();
        // `<_>::default()` stands in for `RecordDecompressionPolicy::default()`.
        // `krabka-compression`, which defines that type, is only a transitive
        // dependency here (through `krabka-protocol`), so this crate has no
        // path to name it; the default policy this crate would otherwise ask
        // for by name is exactly what a bare `decode_borrow_with_policy`
        // caller already gets through inference here.
        let batch = RecordBatchBorrowed::decode_borrow_with_policy(&mut cursor, <_>::default())?;
        let consumed = remaining_before - cursor.len();
        let header = batch.header();
        let batch_base_offset = Offset(header.base_offset.get());

        if predicates.batch_past_offset_bound(&partition_ref, batch_base_offset) {
            break;
        }

        let batch_bytes = segment.log.slice(pos..pos + consumed);
        let records_in_batch = nonneg_u64(header.records_count.get());

        let (mut prepared, tally) = prepare_batch(
            &partition_ref,
            predicates,
            &batch,
            batch_bytes,
            records_in_batch,
        )?;

        let last_offset_delta = prepared.last_offset_delta();
        outcome.bytes_written += bytes_len(prepared.encoded_len());
        outcome.end_offset = batch_base_offset + i64::from(last_offset_delta);

        if let Some(log) = log.as_mut() {
            match &mut prepared {
                PreparedBatch::Verbatim(verbatim) => {
                    log.append_verbatim_at(verbatim, batch_base_offset)?;
                }
                PreparedBatch::Owned { batch, .. } => {
                    log.append_at(batch, batch_base_offset)?;
                }
            }
        }

        match tally {
            BatchTally::Kept => {
                outcome.batches_kept += 1;
                outcome.records_kept += records_in_batch;
            }
            BatchTally::Rewritten { kept, dropped } => {
                outcome.batches_rewritten += 1;
                outcome.records_kept += kept;
                outcome.records_dropped += dropped;
            }
            BatchTally::Emptied => {
                outcome.batches_emptied += 1;
                outcome.records_dropped += records_in_batch;
            }
        }

        pos += consumed;
    }

    if let Some(log) = log.as_mut() {
        log.sync()?;
    }

    Ok(outcome)
}

/// Align `log`'s end offset to `segment`'s declared base offset before any of its batches are appended.
///
/// `Log::append_at`/`Log::append_verbatim_at` take an offset at or above `log.log_end_offset()`, as Kafka's `appendAsFollower` does for a compacted leader's holes, but a restore has no hole to explain: a segment that starts past the end of what was written means an earlier segment is missing, so this function insists on an aligned log. Segments arrive in base-offset order within a partition, so the common case is already aligned. The one legitimate exception is the very first segment written into a brand-new, still-empty target log, when the archive's first surviving base offset is not zero: `Log::open` always starts a fresh directory's active segment at offset zero, so that case needs one `reset_to` to slide the empty log's base up to match. Anything else — a log that already holds data whose end offset does not match this segment's base offset — is a sequencing bug upstream, not a case this function may paper over by resetting: `reset_to` is destructive, and calling it here would discard whatever an earlier call already wrote for this partition.
///
/// # Errors
///
/// Returns [`RestoreError::InvalidArgument`] when `log` already holds data that does not end where `segment` begins.
fn align_log_to_segment(
    log: &mut Log,
    segment: &VerifiedSegment,
    partition: &TopicIdPartition,
) -> Result<(), RestoreError> {
    let leo = log.log_end_offset();
    if leo == segment.facts.base_offset {
        return Ok(());
    }
    if leo == Offset(0) {
        log.reset_to(segment.facts.base_offset)?;
        return Ok(());
    }
    Err(RestoreError::InvalidArgument(format!(
        "segment {} of {}-{} starts at offset {}, but the target log already ends at {leo}; \
         segments must be written in contiguous base-offset order",
        segment.facts.segment_id, partition.topic, partition.partition, segment.facts.base_offset,
    )))
}

/// Convert a header's `records_count` (or any small non-negative `i32` count) to `u64`, treating a corrupt-but-CRC-valid negative count as zero rather than panicking. [`crate::verify::verify_segment`] has already checked every batch's CRC by the time this module sees it.
fn nonneg_u64(count: i32) -> u64 {
    u64::try_from(count.max(0)).unwrap_or(0)
}

/// Convert a byte length to `u64` for [`SegmentOutcome::bytes_written`].
fn bytes_len(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    mod retry;

    use assert2::check;
    use krabka_protocol::records::{Attributes, RecordBatch};

    use super::*;
    use crate::materialize::test_support::{
        args_from, batch, materialization_context, record, record_with_key, reopen_orders,
        topic_id_partition, verified_segment,
    };

    // Preserve each archived batch's span even when filtering empties it.
    fn read_restored_pair(log: &Log) -> krabka_log::ReadOutput {
        let read = log
            .read(Offset(0), LogConfig::default().segment_size)
            .expect("read back");
        check!(read.batches.len() == 2);
        check!(read.batches[0].base_offset == 0);
        check!(read.batches[0].last_offset_delta == 2);
        read
    }

    #[tokio::test]
    async fn keep_only_batches_round_trip_verbatim_at_their_original_offsets() {
        let (_target, args, partition, predicates) = materialization_context(&[]);

        let batches = vec![
            batch(0, vec![record(0, "a"), record(1, "b"), record(2, "c")]),
            batch(3, vec![record(0, "d"), record(1, "e")]),
        ];
        let segment = verified_segment(0, &batches);

        let outcome = write_segment(&args, &partition, &segment, &predicates)
            .await
            .expect("write_segment");

        check!(outcome.batches_kept == 2);
        check!(outcome.batches_rewritten == 0);
        check!(outcome.batches_emptied == 0);
        check!(outcome.records_kept == 5);
        check!(outcome.records_dropped == 0);
        check!(outcome.end_offset == Offset(4));

        let log = reopen_orders(&args);
        let read = log
            .read(Offset(0), LogConfig::default().segment_size)
            .expect("read back");
        check!(read.batches == batches);
    }

    #[tokio::test]
    async fn an_emptied_batch_becomes_a_bare_header_and_the_next_batch_still_appends() {
        let (_target, args, partition, predicates) =
            materialization_context(&["--exclude-key", "^drop$"]);

        let excluded = vec![
            record_with_key(0, "drop"),
            record_with_key(1, "drop"),
            record_with_key(2, "drop"),
        ];
        let batch_a = batch(0, excluded);
        let batch_b = batch(3, vec![record(0, "kept")]);
        let segment = verified_segment(0, &[batch_a, batch_b]);

        let outcome = write_segment(&args, &partition, &segment, &predicates)
            .await
            .expect("write_segment");

        check!(outcome.batches_emptied == 1);
        check!(outcome.batches_kept == 1);
        check!(outcome.records_dropped == 3);
        check!(outcome.records_kept == 1);
        check!(outcome.end_offset == Offset(3));

        let log = reopen_orders(&args);
        // The bare header's full archived span (offsets 0..=2) is still
        // claimed, so log_end_offset already accounts for batch_b at offset 3.
        check!(log.log_end_offset() == Offset(4));

        let read = read_restored_pair(&log);
        check!(read.batches[0].records.is_empty());
        check!(read.batches[1].base_offset == 3);
    }

    #[tokio::test]
    async fn a_filtered_batch_keeps_the_survivors_original_absolute_offsets() {
        let (_target, args, partition, predicates) =
            materialization_context(&["--exclude-key", "^drop$"]);

        let batch_a = batch(
            0,
            vec![
                record(0, "keep0"),
                record_with_key(1, "drop"),
                record(2, "keep2"),
            ],
        );
        let segment = verified_segment(0, std::slice::from_ref(&batch_a));

        let outcome = write_segment(&args, &partition, &segment, &predicates)
            .await
            .expect("write_segment");

        check!(outcome.batches_rewritten == 1);
        check!(outcome.batches_kept == 0);
        check!(outcome.batches_emptied == 0);
        check!(outcome.records_kept == 2);
        check!(outcome.records_dropped == 1);
        check!(outcome.end_offset == Offset(2));

        let log = reopen_orders(&args);
        let read = log
            .read(Offset(0), LogConfig::default().segment_size)
            .expect("read back");
        let expected = RecordBatch {
            last_offset_delta: 2,
            records: vec![record(0, "keep0"), record(2, "keep2")],
            ..batch_a
        };
        check!(read.batches == vec![expected]);
    }

    #[tokio::test]
    async fn a_filtered_batch_that_drops_its_trailing_record_does_not_strand_the_next_batch() {
        // The archive is contiguous by construction: `batch_b`'s base offset
        // (3) is exactly where `batch_a`'s ORIGINAL span (offsets 0..=2)
        // ends. If `write_segment` used `filter_batch`'s recomputed
        // `last_offset_delta` (which shrinks to the highest SURVIVING
        // delta -- 1, once offset 2 is excluded) instead of the archived
        // value, `batch_a` would claim only offsets 0..=1 and
        // `log.log_end_offset()` after it would be 2, not 3, so the offset
        // space of the archive would shrink. This is the regression a Filter
        // decision that happens to exclude a batch's LAST record must not
        // reintroduce.
        let (_target, args, partition, predicates) =
            materialization_context(&["--exclude-key", "^drop$"]);

        let batch_a = batch(0, vec![record(0, "keep0"), record_with_key(2, "drop")]);
        let batch_b = batch(3, vec![record(0, "keep3")]);
        let segment = verified_segment(0, &[batch_a.clone(), batch_b.clone()]);

        let outcome = write_segment(&args, &partition, &segment, &predicates)
            .await
            .expect("write_segment must not fail with an offset mismatch");

        check!(outcome.batches_rewritten == 1);
        check!(outcome.batches_kept == 1);
        check!(outcome.records_kept == 2);
        check!(outcome.records_dropped == 1);
        check!(outcome.end_offset == Offset(3));

        let log = reopen_orders(&args);
        let read = read_restored_pair(&log);
        check!(read.batches[0].records == vec![record(0, "keep0")]);
        check!(read.batches[1].base_offset == 3);
        check!(read.batches[1].records == vec![record(0, "keep3")]);
    }

    #[tokio::test]
    async fn a_second_segment_continues_without_resetting_the_first() {
        let (_target, args, partition, predicates) = materialization_context(&[]);

        let batch_1 = batch(0, vec![record(0, "a"), record(1, "b")]);
        let segment_1 = verified_segment(0, std::slice::from_ref(&batch_1));
        let outcome_1 = write_segment(&args, &partition, &segment_1, &predicates)
            .await
            .expect("segment 1");
        check!(outcome_1.end_offset == Offset(1));

        let batch_2 = batch(2, vec![record(0, "c")]);
        let segment_2 = verified_segment(2, std::slice::from_ref(&batch_2));
        let outcome_2 = write_segment(&args, &partition, &segment_2, &predicates)
            .await
            .expect("segment 2");
        check!(outcome_2.end_offset == Offset(2));

        let log = reopen_orders(&args);
        let read = log
            .read(Offset(0), LogConfig::default().segment_size)
            .expect("read back");
        // The first segment's data survives the second call: no destructive
        // reset happened in between.
        check!(read.batches == vec![batch_1, batch_2]);
    }

    #[tokio::test]
    async fn the_first_segment_resets_an_empty_log_to_a_nonzero_base() {
        let (_target, args, partition, predicates) = materialization_context(&[]);

        let batches = vec![batch(1000, vec![record(0, "x"), record(1, "y")])];
        let segment = verified_segment(1000, &batches);

        let outcome = write_segment(&args, &partition, &segment, &predicates)
            .await
            .expect("write_segment");
        check!(outcome.base_offset == Offset(1000));
        check!(outcome.end_offset == Offset(1001));

        let log = reopen_orders(&args);
        check!(log.log_start_offset() == Offset(1000));
        check!(log.log_end_offset() == Offset(1002));
        let read = log
            .read(Offset(1000), LogConfig::default().segment_size)
            .expect("read back");
        check!(read.batches == batches);
    }

    #[tokio::test]
    async fn dry_run_matches_a_real_run_but_writes_nothing() {
        let mut compressed = batch(0, vec![record(0, "a"), record_with_key(1, "drop")]);
        // Kafka compression attribute 1 is gzip. A compressed rewrite's
        // encoded size cannot be predicted by `RecordBatch::encoded_len`, so
        // this pins the pre-encode path used by both real and dry runs.
        compressed.attributes = Attributes(1);
        let batches = vec![compressed, batch(2, vec![record(0, "c")])];
        let partition = topic_id_partition("orders", 0);
        // One shared fixture: `SegmentOutcome::segment_id` echoes
        // `segment.facts.segment_id`, and that id is random per fixture, so
        // both runs must see the exact same `VerifiedSegment` for their
        // outcomes to compare equal on that field too.
        let segment = verified_segment(0, &batches);

        let real_target = tempfile::tempdir().expect("tempdir");
        let real_args = args_from(&["--exclude-key", "^drop$"], real_target.path());
        let real_predicates = Predicates::from_args(&real_args).expect("predicates");
        let real_outcome = write_segment(&real_args, &partition, &segment, &real_predicates)
            .await
            .expect("real write");
        let real_dir = name::partition_dir(&real_args.target.log_dir, "orders", 0);
        let log_bytes: u64 = std::fs::read_dir(real_dir)
            .expect("target partition")
            .map(|entry| entry.expect("directory entry").path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "log"))
            .map(|path| std::fs::metadata(path).expect("log metadata").len())
            .sum();
        check!(real_outcome.bytes_written == log_bytes);

        let dry_target = tempfile::tempdir().expect("tempdir");
        let mut dry_args = args_from(&["--exclude-key", "^drop$"], dry_target.path());
        dry_args.dry_run = true;
        let dry_predicates = Predicates::from_args(&dry_args).expect("predicates");
        let dry_outcome = write_segment(&dry_args, &partition, &segment, &dry_predicates)
            .await
            .expect("dry write");

        check!(dry_outcome == real_outcome);
        check!(!dry_target.path().join("orders-0").exists());
        check!(real_target.path().join("orders-0").exists());
    }
}
