//! The second compaction pass. It streams the sealed segments into `.cleaned`
//! files, applying the KIP-534 retain decision to every record, and owns the
//! rewrite's input and output types. [`atomic_swap`] later renames the
//! `.cleaned` files to `.swap`, which is the commit point that recovery honours.

use {
    super::{
        BatchMeta, CleanedTransactionMetadata, RecordMeta, RetainDecision, TxnDataState,
        batch_reader::read_all_batches, retain_decision,
    },
    crate::{
        error::LogError,
        io::{IoTarget, LogIo, write_all},
        record_limit::check_records_read,
        recovery::swap::cleaned_path,
        segment::Segment,
        txn_index::TxnIndex,
    },
    bytes::{Bytes, BytesMut},
    krabka_compression::CompressionType,
    krabka_ids::{Offset, ProducerId},
    krabka_protocol::records::{RecordBatch, TimestampType},
    krabka_units::prelude::{ByteSize, Time, TimeExt},
    std::{
        collections::HashMap,
        fs::{File, OpenOptions},
        path::{Path, PathBuf},
    },
    tracing::instrument,
};

#[cfg(test)]
mod record_limit_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod transaction_tests;

/// Encode one retained batch and advance the inclusive output frontier after its write.
fn write_rewritten_batch(
    io: &dyn LogIo,
    file: &File,
    batch: &RecordBatch,
    previous_last: Offset,
) -> Result<Offset, LogError> {
    let mut bytes = BytesMut::with_capacity(batch.encoded_len());
    batch.encode(&mut bytes)?;
    write_all(io, IoTarget::CompactionSwap, file, &bytes)?;
    Ok(previous_last.max(Offset(
        batch.base_offset + i64::from(batch.last_offset_delta),
    )))
}

/// Kafka's `RecordBatch.NO_TIMESTAMP`: the base timestamp of a batch that has
/// no records.
const NO_TIMESTAMP: i64 = -1;

/// Result of [`rewrite_segments`]: paths to the `.cleaned` files that
/// [`atomic_swap`] should promote through `.swap` to their final names.
pub struct RewriteOutput {
    pub log_swap: PathBuf,
    pub index_swap: PathBuf,
    pub timeindex_swap: PathBuf,
    /// `base_offset` of the new segment. It equals the lowest input segment.
    pub new_base_offset: Offset,
    /// Highest absolute offset of any surviving record.
    #[cfg(test)]
    pub new_last_offset: Offset,
    /// Path to the rewritten `.txnindex`. The rewrite writes this file only
    /// when it keeps the abort marker of one or more aborted transactions. It
    /// is `None` when no aborted transaction survives.
    pub txnindex_swap: Option<PathBuf>,
}

/// Time-based retention inputs used while rewriting compacted segments.
#[derive(Debug, Clone, Copy)]
pub struct RewriteRetention {
    /// Current wall-clock time in milliseconds. An instant, so it stays raw.
    pub now_ms: i64,
    /// How long a tombstone remains eligible for reads before deletion.
    pub delete_retention: Time,
}

/// What the log's producer state says about an active producer's last record:
/// Kafka's `LastRecord`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerLastRecord {
    /// The last offset of the producer's last *data* batch, or `None` when it
    /// has written no data batch on the partition, only transaction markers.
    pub last_data_offset: Option<Offset>,
    /// The producer's current epoch, which fences a zombie.
    pub producer_epoch: i16,
}

/// The last absolute offset `batch` spans.
fn last_offset_of(batch: &RecordBatch) -> i64 {
    batch.base_offset + i64::from(batch.last_offset_delta)
}

/// What one cleaning round knows about the log beyond the segments a
/// [`rewrite_segments`] call gets. A round can rewrite several size-bounded
/// groups, one call each, and both facts are about the whole round.
#[derive(Debug, Clone, Copy)]
pub struct CleaningRound<'a> {
    /// Kafka's `lastRecordsOfActiveProducers`: each active producer's last
    /// record. A producer that is not in the map is not active.
    pub active_producers: &'a HashMap<ProducerId, ProducerLastRecord>,
    /// Kafka's `upperBoundOffsetOfCleaningRound`: the offset after the last
    /// batch the round rewrites.
    pub upper_bound: Offset,
    /// Kafka trunk's `max.decompressed.message.bytes`, the `maxRecordBodySize`
    /// `Cleaner.cleanInto` hands to `MemoryRecords.filterTo`. `None` is no limit.
    pub max_decompressed_record: Option<ByteSize>,
}

impl CleaningRound<'_> {
    /// Kafka's `isBatchLastRecordOfProducer` in `Cleaner.cleanInto`: whether
    /// `batch` is the record that keeps its producer's state alive. It is the
    /// producer's last data batch, or, for a producer that wrote only
    /// transaction markers, a marker of its current epoch.
    fn is_last_record_of_producer(&self, batch: &RecordBatch) -> bool {
        let Some(last) = self.active_producers.get(&ProducerId(batch.producer_id)) else {
            return false;
        };
        match last.last_data_offset {
            Some(last_data_offset) => last_offset_of(batch) == last_data_offset.0,
            None => {
                batch.attributes.is_control_batch() && batch.producer_epoch == last.producer_epoch
            }
        }
    }

    /// Kafka's `batch.nextOffset() == upperBoundOffsetOfCleaningRound`: the
    /// last batch of the round, kept even when empty so that the last offset is
    /// not lost.
    fn is_last_batch_of_round(&self, batch: &RecordBatch) -> bool {
        last_offset_of(batch) + 1 == self.upper_bound.0
    }
}

/// Stream `segments`, oldest to newest, into new `.cleaned` files and apply the
/// KIP-534 per-record [`retain_decision`].
///
/// For each record the decision is:
///   - `Keep` → write it through.
///   - `SetHorizon(h)` → write it through, and stamp the output batch with
///     delete horizon `h` (bit 6 set, `base_timestamp = h`).
///   - `Delete` → drop it.
///
/// Records keep their **absolute** offsets. The output `RecordBatch`es can
/// therefore hold gaps in their `offset_delta` values where superseded records
/// used to live. This matches Kafka's on-disk format for compacted topics.
///
/// Every record of a batch that belongs to an aborted transaction is dropped,
/// and a transaction marker follows Kafka's `CleanedTransactionMetadata`: it
/// ages out through its delete horizon once the walk has met no batch of its
/// transaction. The caller adds the aborted transactions of the range to
/// `txn_meta` first, and passes the same `txn_meta` to every group of one pass
/// in offset order, because a transaction can span groups. The output's
/// `.txnindex` holds the aborted transactions whose marker this group kept.
///
/// `RETAIN_EMPTY`: this function normally skips a batch that ends up with no
/// kept records. It writes such a batch again as a bare header with no records
/// in two cases, both Kafka's `Cleaner.cleanInto`: when the batch is the
/// record that keeps an active producer's state alive
/// ([`CleaningRound::active_producers`]: the producer's last data batch, or a
/// marker of its current epoch when it wrote no data batch), and when it is the
/// last batch of the whole round ([`CleaningRound::upper_bound`]), not of each
/// output group. The producer sequence, the producer epoch, and the log-end
/// offset therefore survive.
///
/// This function writes the `.cleaned` files to the segments' shared directory.
/// The caller must fsync them and promote them through [`atomic_swap`].
#[instrument(
    level = "info",
    skip_all,
    fields(
        dir = %dir.display(),
        segments = segments.len(),
        new_base = tracing::field::Empty,
        new_last_offset = tracing::field::Empty,
    ),
    err,
)]
pub fn rewrite_segments(
    io: &dyn LogIo,
    dir: &Path,
    segments: &[&Segment],
    offset_map: &HashMap<Bytes, Offset>,
    txn_meta: &mut CleanedTransactionMetadata,
    retention: RewriteRetention,
    round: CleaningRound<'_>,
) -> Result<RewriteOutput, LogError> {
    // The Creusot-verified retain kernel is stated over integer milliseconds,
    // and the horizon it computes is stamped into an on-disk `base_timestamp`,
    // so the extent crosses to a raw count once, here, truncating rather than
    // rounding so a stamped horizon can never land a millisecond late.
    let delete_retention_ms = retention.delete_retention.millis_i64_trunc();

    let first = segments
        .first()
        .ok_or_else(|| LogError::Io(std::io::Error::other("rewrite_segments: empty input")))?;
    let new_base = first.base_offset();
    tracing::Span::current().record("new_base", new_base.0);

    let log_swap = swap_path(dir, new_base.0, "log");
    let index_swap = swap_path(dir, new_base.0, "index");
    let timeindex_swap = swap_path(dir, new_base.0, "timeindex");

    // Truncate (or create) all three .cleaned files. We rewrite the .log
    // file proper here; for the index sidecars we write empty files
    // and let Segment::open populate them via tail-scan in the recovery
    // promotion path. (Sparse indexes are derivable from the .log; an
    // empty index is correct and small.)
    let log_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&log_swap)?;
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&index_swap)?;
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&timeindex_swap)?;

    let mut all_batches: Vec<RecordBatch> = Vec::new();
    for seg in segments {
        all_batches.extend(read_all_batches(seg)?);
    }

    let mut last_kept_offset = new_base - 1;

    for batch in &all_batches {
        let is_control = batch.attributes.is_control_batch();
        let producer_id = ProducerId(batch.producer_id);
        // The pass reads the batch before it filters its records, as Kafka's
        // `Cleaner.shouldDiscardBatch` does: a transaction whose records all
        // die in this pass still holds its marker until the next one.
        let (txn, aborted) = if is_control {
            let discardable = txn_meta.on_control_batch_read(batch)?;
            let state = if producer_id.get() < 0 {
                TxnDataState::NotTransactional
            } else if discardable {
                TxnDataState::DataFullyGone
            } else {
                TxnDataState::DataSurvives
            };
            (state, false)
        } else {
            (
                TxnDataState::NotTransactional,
                txn_meta.on_batch_read(batch),
            )
        };
        // `MemoryRecords.filterTo` decompresses every batch but one it deletes
        // outright, which is an aborted batch that is neither the last record
        // of an active producer nor the last batch of the round, and holds the
        // batch it decompresses to `max.decompressed.message.bytes`.
        if !aborted
            || (producer_id.get() >= 0 && round.is_last_record_of_producer(batch))
            || round.is_last_batch_of_round(batch)
        {
            check_records_read(batch, batch.records.len(), round.max_decompressed_record)?;
        }
        let batch_meta = BatchMeta {
            is_control,
            producer_id,
            existing_horizon: batch.delete_horizon_ms(),
        };

        let mut kept: Vec<krabka_protocol::records::Record> =
            Vec::with_capacity(batch.records.len());
        // Stamp the output batch with a delete horizon if any record's
        // decision asks for it (stamp once per batch).
        let mut stamp_horizon: Option<i64> = None;
        for record in &batch.records {
            let absolute = Offset(batch.base_offset + i64::from(record.offset_delta));
            let is_newest_for_key = record
                .key
                .as_ref()
                .is_some_and(|k| offset_map.get(k.as_ref()).copied() == Some(absolute));
            let rec_meta = RecordMeta {
                has_key: record.key.is_some(),
                has_value: record.value.is_some(),
            };
            // Every record of an aborted transaction goes, whether or not it
            // is the newest for its key: Kafka's `discardBatchRecords`.
            let decision = if aborted {
                RetainDecision::Delete
            } else {
                retain_decision(
                    rec_meta,
                    batch_meta,
                    is_newest_for_key,
                    txn,
                    retention.now_ms,
                    delete_retention_ms,
                )
            };
            match decision {
                RetainDecision::Keep => kept.push(record.clone()),
                RetainDecision::SetHorizon(h) => {
                    kept.push(record.clone());
                    stamp_horizon = Some(h);
                }
                RetainDecision::Delete => {}
            }
        }

        if kept.is_empty() {
            // RETAIN_EMPTY: re-emit a bare header for an emptied batch that
            // keeps a producer's state alive, or that is the last batch of
            // the round, so the producer sequence and epoch and the log-end
            // offset survive.
            let keeps_producer_state =
                batch.producer_id >= 0 && round.is_last_record_of_producer(batch);
            if !(keeps_producer_state || round.is_last_batch_of_round(batch)) {
                continue;
            }
            let out_batch = bare_header(batch);
            last_kept_offset = write_rewritten_batch(io, &log_file, &out_batch, last_kept_offset)?;
            continue;
        }

        // Kafka's `MemoryRecords.buildRetainedRecordsInto` keeps the original
        // batch's base offset and last offset (`overrideLastOffset`), so the
        // producer's last sequence (`base_sequence + last_offset_delta`) and
        // the `RETAIN_EMPTY` comparisons of the next pass survive a batch that
        // loses its tail records. The max timestamp is the retained records'
        // under CreateTime (`MemoryRecordsBuilder.recordWritten`), and the
        // batch's own under LogAppendTime (`writeDefaultBatchHeader`). The
        // timestamps are absolute here, whether or not a delete horizon
        // already re-based the batch.
        let max_timestamp = match batch.attributes.timestamp_type() {
            TimestampType::LogAppendTime => batch.max_timestamp,
            TimestampType::CreateTime => kept
                .iter()
                .map(|r| batch.base_timestamp.saturating_add(r.timestamp_delta))
                .max()
                .expect("kept non-empty"),
        };
        let mut out_batch = RecordBatch {
            max_timestamp,
            records: kept,
            ..batch.clone()
        };
        // Stamp the delete horizon once, after the kept batch is built. This
        // rewrites each kept record's timestamp_delta so absolute timestamps
        // are preserved (see `RecordBatch::with_delete_horizon`).
        if let Some(h) = stamp_horizon {
            out_batch = out_batch.with_delete_horizon(h);
        }

        last_kept_offset = write_rewritten_batch(io, &log_file, &out_batch, last_kept_offset)?;
    }
    io.sync_file(IoTarget::CompactionSwap, &log_file)?;

    // Rebuild the `.txnindex` from the aborted transactions the walk kept an
    // abort marker for, as Kafka's `CleanedTransactionMetadata` appends them
    // to the cleaned index: a transaction whose batches the pass no longer
    // meets has its entry dropped together with its marker.
    //
    // An entry lands in the output group that holds its abort marker. A
    // compaction pass can rewrite the consumed range into several output
    // segments (`Log::group_segments_by_size`), and the walk closes an
    // aborted transaction where it reads the marker, so no entry is duplicated
    // across outputs. A read-committed fetch that scans several of a
    // multi-segment pass's outputs would otherwise see the same aborted
    // transaction once per output and inflate its response.
    let retained = txn_meta.take_cleaned_index();
    let txnindex_swap = if retained.is_empty() {
        None
    } else {
        let path = swap_path(dir, new_base.0, "txnindex");
        // Truncate any stale .cleaned file, then append the retained entries.
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        let mut idx = TxnIndex::open(path.clone())?;
        for entry in retained {
            idx.append(entry)?;
        }
        Some(path)
    };

    tracing::Span::current().record("new_last_offset", last_kept_offset.0);
    Ok(RewriteOutput {
        log_swap,
        index_swap,
        timeindex_swap,
        new_base_offset: new_base,
        #[cfg(test)]
        new_last_offset: last_kept_offset,
        txnindex_swap,
    })
}

/// The bare header a `RETAIN_EMPTY` batch leaves in the output, Kafka's
/// `DefaultRecordBatch.writeEmptyHeader`: it has no base timestamp, no
/// compression and no delete horizon, whatever the emptied batch had. The
/// batch's offsets, max timestamp, producer state, leader epoch and its
/// transactional, control and timestamp-type bits carry over.
fn bare_header(batch: &RecordBatch) -> RecordBatch {
    RecordBatch {
        base_offset: batch.base_offset,
        last_offset_delta: batch.last_offset_delta,
        max_timestamp: batch.max_timestamp,
        base_timestamp: NO_TIMESTAMP,
        attributes: batch
            .attributes
            .with_compression(CompressionType::None)
            .with_delete_horizon(false),
        producer_id: batch.producer_id,
        producer_epoch: batch.producer_epoch,
        base_sequence: batch.base_sequence,
        partition_leader_epoch: batch.partition_leader_epoch,
        records: vec![],
    }
}

fn swap_path(dir: &Path, base_offset: i64, ext: &str) -> PathBuf {
    cleaned_path(dir, base_offset, ext)
}
