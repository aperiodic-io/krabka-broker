//! Grouped record-batch appends and the blocking-pool hop that runs them.
//!
//! Both the leader path, which lets the log stamp the base offset, and the
//! diskless path, which appends at an externally assigned base offset, share
//! the one-lock-per-group discipline, so they stay together in one module.

use {
    super::storage::{lock_log, storage_failure_error},
    crate::{
        blocking::run_blocking_catching,
        error::BrokerError,
        partition::{AppendedBatch, ProduceData, ProducerAppendCheck},
    },
    krabka_compression::CompressionType,
    krabka_log::{Log, Offset},
    krabka_protocol::records::RecordBatch,
    krabka_units::prelude::{ByteSizeExt as _, mebibytes},
    std::sync::{Arc, Mutex},
};

type CheckedBatches = (Vec<ProduceData>, Vec<Option<ProducerAppendCheck>>);
type AppendOutcome = (
    Vec<Result<AppendedBatch, BrokerError>>,
    Offset,
    Vec<krabka_log::ProducerSnapshotEntry>,
);

/// Hold the group's log lock after releasing any replicated transactions.
fn prepare_log(
    log: &Mutex<Log>,
    high_watermark: Option<Offset>,
) -> (std::sync::MutexGuard<'_, Log>, Option<CompressionType>) {
    let mut guard = lock_log(log);
    if let Some(high_watermark) = high_watermark {
        guard.release_replicated_transactions(high_watermark);
    }
    let target = guard.config_snapshot().compression_type;
    (guard, target)
}

/// Append owned bytes at an assigned offset, or let the leader stamp them.
fn append_owned(
    log: &mut Log,
    batch: &mut RecordBatch,
    base: Option<Offset>,
) -> Result<AppendedBatch, krabka_log::LogError> {
    match base {
        Some(base) => log.append_at(batch, base).map(|()| at_offset(base)),
        None => log.append(batch).map(appended),
    }
}

/// Dispatch one accepted job without changing control-batch compression or stamps.
fn append_data(
    log: &mut Log,
    mut data: ProduceData,
    target: Option<CompressionType>,
    base: Option<Offset>,
) -> Result<AppendedBatch, BrokerError> {
    data.apply_target_compression(target);
    let result = match data {
        ProduceData::Verbatim(batch) => match base {
            Some(base) => log.append_verbatim_at(&batch, base).map(at_offset),
            None => log.append_verbatim(&batch).map(appended),
        },
        ProduceData::Owned(mut batch) | ProduceData::OwnedControl(mut batch) => {
            append_owned(log, &mut batch, base)
        }
        ProduceData::OwnedCommitMarker {
            mut batch,
            commit_stamp,
        } => match base {
            Some(base) => log
                .append_at_with_commit_stamp(&mut batch, base, commit_stamp)
                .map(|()| at_offset(base)),
            None => log
                .append_with_commit_stamp(&mut batch, commit_stamp)
                .map(at_offset),
        },
    };
    result.map_err(BrokerError::from)
}

/// Mirror successful control appends while the group's original log lock is held.
fn capture_control_entry(
    log: &Log,
    appended: bool,
    producer: Option<krabka_log::ProducerId>,
    entries: &mut Vec<krabka_log::ProducerSnapshotEntry>,
) {
    if appended
        && let Some(producer) = producer
        && let Some(entry) = log.producer_state_entry(producer)
    {
        entries.push(entry);
    }
}

/// The writer's answer for a batch the log both placed and, when the partition
/// asks for it, stamped.
fn appended((base_offset, log_append_time_ms): (Offset, Option<i64>)) -> AppendedBatch {
    AppendedBatch {
        base_offset,
        log_append_time_ms,
    }
}

/// The writer's answer for an append at a caller-assigned offset.
///
/// These appends carry no log-append stamp. Kafka stamps under
/// `validateAndAssignOffsets`, which is the offset-assigning leader append
/// alone: a follower stores the leader's already-stamped bytes, and re-stamping
/// them would give one offset two different encodings across replicas.
fn at_offset(base_offset: Offset) -> AppendedBatch {
    AppendedBatch {
        base_offset,
        log_append_time_ms: None,
    }
}

/// The refusal for a client batch whose producer transaction check fails, or
/// `None` when the batch may append. The check runs under the append's own
/// lock, so no marker can land between the check and the append.
fn refuse_transactional_append(
    log: &Log,
    check: Option<&Option<ProducerAppendCheck>>,
) -> Option<BrokerError> {
    let check = check.copied().flatten()?;
    log.check_transactional_append(check.batch, check.guard)
        .err()
        .map(BrokerError::TransactionAppend)
}

/// Append a whole group of produce jobs under a single lock acquisition.
///
/// The function returns the per-job results, a base offset or an error, in
/// input order. It also returns the log-end offset after the append, for the
/// group's HW recompute, and the log's producer entry for every control batch
/// that appended, for the produce-path tracker's mirror.
///
/// The mirror entries are read here, under the one lock this function already
/// holds, rather than by a second, later lock acquisition. A `std::sync::
/// Mutex` lock taken directly on the calling async task blocks its worker
/// thread for as long as whichever task holds the lock, and does not yield it
/// back to the runtime the way an uncontended `.await` would. This function
/// itself already runs off the async worker pool (see
/// [`run_produce_append_batch`]), so a second, separate acquisition later, on
/// the worker pool, could block a worker thread behind a concurrent
/// `spawn_blocking` log operation such as a diskless flush's trim, starving
/// whatever else that thread was due to run -- including, on a freshly
/// promoted leader catching up on replicated markers across many partitions
/// at once, this broker's own heartbeat-sending task, past the controller's
/// liveness timeout.
///
/// Ordinary verbatim jobs share bounded Direct I/O writes when the log permits
/// them; other verbatim jobs use `append_verbatim`. The function recompresses
/// owned jobs to the topic's configured codec, which it reads once under the
/// same lock. Control jobs skip that rewrite, because Kafka never compresses a
/// control batch that arrived uncompressed. Sequential appends stamp
/// sequential base offsets, so the function keeps the order across the group.
fn append_produce_batch(
    log: &Mutex<Log>,
    high_watermark: Option<Offset>,
    (datas, checks): CheckedBatches,
) -> AppendOutcome {
    let (mut guard, target) = prepare_log(log, high_watermark);
    if let Some(results) = try_append_direct_group(&mut guard, &datas, &checks) {
        return (results, guard.log_end_offset(), Vec::new());
    }
    let mut results = Vec::with_capacity(datas.len());
    let mut control_entries = Vec::new();
    for (index, data) in datas.into_iter().enumerate() {
        let control_producer = data.control_producer_id();
        if let Some(refused) = refuse_transactional_append(&guard, checks.get(index)) {
            results.push(Err(refused));
            continue;
        }
        let r = append_data(&mut guard, data, target, None);
        capture_control_entry(&guard, r.is_ok(), control_producer, &mut control_entries);
        results.push(r);
    }
    // Read the post-append LEO once under the same lock so the HW recompute
    // reflects the whole group.
    let leo = guard.log_end_offset();
    (results, leo, control_entries)
}

/// Combine ordinary verbatim jobs before any acknowledgment is sent.
fn try_append_direct_group(
    log: &mut Log,
    datas: &[ProduceData],
    checks: &[Option<ProducerAppendCheck>],
) -> Option<Vec<Result<AppendedBatch, BrokerError>>> {
    if checks.iter().any(Option::is_some)
        || log.config_snapshot().segment_allocation != krabka_log::SegmentAllocation::Preallocate
    {
        return None;
    }
    let batches = datas
        .iter()
        .map(|data| match data {
            ProduceData::Verbatim(batch) => Some(batch),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    let mut remaining = batches.as_slice();
    let mut results = Vec::with_capacity(datas.len());
    while !remaining.is_empty() {
        let count = direct_group_len(remaining);
        if let Some(result) = log.try_append_verbatim_group(&remaining[..count]) {
            results.extend(direct_group_results(result, count));
            remaining = &remaining[count..];
        } else {
            // A roll or an empty segment needs one ordinary append before
            // the following batches can share its Direct I/O write.
            results.push(
                log.append_verbatim(remaining[0])
                    .map(appended)
                    .map_err(Into::into),
            );
            remaining = &remaining[1..];
        }
    }
    Some(results)
}

/// Limit each attempt by both byte size and the work spent on a refused group.
fn direct_group_len(batches: &[&krabka_log::VerbatimBatch]) -> usize {
    let mut bytes = 0;
    batches
        .iter()
        .take(64)
        .take_while(|batch| {
            bytes += batch.bytes.len();
            bytes <= mebibytes(1).bytes_usize()
        })
        .count()
        .max(1)
}

fn direct_group_results(
    result: Result<Vec<Offset>, krabka_log::LogError>,
    count: usize,
) -> Vec<Result<AppendedBatch, BrokerError>> {
    match result {
        Ok(offsets) => offsets
            .into_iter()
            .map(|base| Ok(at_offset(base)))
            .collect(),
        Err(error) => {
            let mut failures = Vec::with_capacity(count);
            for _ in 1..count {
                let failure = match &error {
                    krabka_log::LogError::Io(io) => {
                        krabka_log::LogError::Io(std::io::Error::new(io.kind(), io.to_string()))
                    }
                    other => krabka_log::LogError::Corrupt(other.to_string()),
                };
                failures.push(Err(failure.into()));
            }
            failures.insert(0, Err(error.into()));
            failures
        }
    }
}

fn append_produce_batch_at(
    log: &Mutex<Log>,
    base: Offset,
    high_watermark: Option<Offset>,
    (datas, checks): CheckedBatches,
) -> AppendOutcome {
    let (mut guard, target) = prepare_log(log, high_watermark);
    let mut next = base;
    let mut results = Vec::with_capacity(datas.len());
    let mut control_entries = Vec::new();
    for (index, data) in datas.into_iter().enumerate() {
        let count = i64::from(data.record_count());
        let control_producer = data.control_producer_id();
        let result = if let Some(refused) = refuse_transactional_append(&guard, checks.get(index)) {
            Err(refused)
        } else {
            append_data(&mut guard, data, target, Some(next))
        };
        capture_control_entry(
            &guard,
            result.is_ok(),
            control_producer,
            &mut control_entries,
        );
        next = Offset(next.0 + count);
        guard.reconcile_next_offset(next);
        results.push(result);
    }
    let leo = guard.log_end_offset();
    (results, leo, control_entries)
}

/// Run [`append_produce_batch`] away from normal async polling.
///
/// On the broker's multi-thread runtime, `block_in_place` avoids the per-batch
/// `spawn_blocking` scheduling hop. Tokio can still hand the worker's other
/// tasks to a replacement thread. Current-thread test runtimes keep the
/// `spawn_blocking` fallback because `block_in_place` is illegal there, and
/// `wasm32-wasip1` appends inline; [`crate::blocking`] makes that choice. The
/// writer loop is still the single serializer for this partition, so the append
/// order does not change.
pub(crate) async fn run_produce_append_batch(
    log: Arc<Mutex<Log>>,
    high_watermark: Option<Offset>,
    datas: CheckedBatches,
) -> Result<AppendOutcome, BrokerError> {
    run_blocking_catching(move || append_produce_batch(&log, high_watermark, datas))
        .await
        .map_err(|error| storage_failure_error("append task panicked", error))
}

pub(crate) async fn run_produce_append_batch_at(
    log: Arc<Mutex<Log>>,
    base: Offset,
    high_watermark: Option<Offset>,
    datas: CheckedBatches,
) -> Result<AppendOutcome, BrokerError> {
    run_blocking_catching(move || append_produce_batch_at(&log, base, high_watermark, datas))
        .await
        .map_err(|error| storage_failure_error("append task panicked", error))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            error::BrokerError,
            partition_writer::test_support::sample_batch,
            test_support::{EmptyPayloadBatchSetup, RecordCount},
        },
        assert2::{assert, check},
        krabka_log::{LogConfig, LogIo},
        tempfile::tempdir,
    };

    #[derive(Debug)]
    struct FailNthWrite {
        next: std::sync::atomic::AtomicUsize,
        fail_at: usize,
    }

    impl FailNthWrite {
        fn check_write(&self) -> std::io::Result<()> {
            let call = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            if call == self.fail_at {
                Err(std::io::ErrorKind::StorageFull.into())
            } else {
                Ok(())
            }
        }
    }

    impl LogIo for FailNthWrite {
        fn write(&self, file: &std::fs::File, buf: &[u8]) -> std::io::Result<usize> {
            use std::io::Write;

            self.check_write()?;
            (&*file).write(buf)
        }

        fn open_direct(&self, path: &std::path::Path) -> std::io::Result<krabka_log::DirectFile> {
            Ok(krabka_log::DirectFile {
                file: std::fs::File::options().read(true).write(true).open(path)?,
                mem_align: 1,
                block: 1,
            })
        }

        fn write_direct(
            &self,
            file: &std::fs::File,
            buf: &[u8],
            offset: u64,
        ) -> std::io::Result<usize> {
            use std::io::{Seek, SeekFrom, Write};

            self.check_write()?;
            let mut file = file;
            file.seek(SeekFrom::Start(offset))?;
            file.write(buf)
        }
    }

    fn lz4_log(path: &std::path::Path) -> Mutex<Log> {
        Mutex::new(
            Log::open(path, LogConfig {
                compression_type: Some(CompressionType::Lz4),
                ..LogConfig::default()
            })
            .expect("open log"),
        )
    }

    fn failing_log(path: &std::path::Path) -> Mutex<Log> {
        let mut log = Log::open(path, LogConfig::default()).expect("open log");
        log.test_set_io(Arc::new(FailNthWrite {
            next: std::sync::atomic::AtomicUsize::new(0),
            fail_at: 2,
        }));
        Mutex::new(log)
    }

    fn assert_partial_append(results: &[Result<AppendedBatch, BrokerError>], leo: Offset) {
        assert!(results[0].as_ref().unwrap().base_offset == Offset(0));
        assert!(matches!(
            &results[1],
            Err(crate::error::BrokerError::Log(krabka_log::LogError::Io(error)))
                if error.kind() == std::io::ErrorKind::StorageFull
        ));
        assert!(leo == Offset(1));
    }

    fn two_single_record_batches() -> Vec<ProduceData> {
        std::iter::repeat_with(|| {
            ProduceData::Owned(sample_batch(EmptyPayloadBatchSetup::default()))
        })
        .take(2)
        .collect()
    }

    #[test]
    fn verbatim_produce_groups_share_a_write_and_ack_only_written_offsets() {
        for (jobs, payload, seeded, append_time, fail_at, writes, failed) in [
            (8, 0, true, false, usize::MAX, 1, 0),
            (8, 0, true, false, 1, 1, 8),
            (8, 0, false, false, usize::MAX, 2, 0),
            (1_024, 2_048, true, false, usize::MAX, 16, 0),
            (1_024, 2_048, true, false, 2, 16, 64),
            (16, 128 * 1_024, true, false, usize::MAX, 3, 0),
            (8, 0, true, true, usize::MAX, 8, 0),
        ] {
            let dir = tempdir().unwrap();
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            if seeded {
                log.append(&mut sample_batch(EmptyPayloadBatchSetup::default()))
                    .unwrap();
            }
            let disk = Arc::new(FailNthWrite {
                next: std::sync::atomic::AtomicUsize::new(0),
                fail_at,
            });
            log.test_set_io(disk.clone());
            log.set_config(LogConfig {
                segment_allocation: krabka_log::SegmentAllocation::Preallocate,
                segment_size: mebibytes(16),
                message_timestamp_type: if append_time {
                    krabka_protocol::records::TimestampType::LogAppendTime
                } else {
                    krabka_protocol::records::TimestampType::CreateTime
                },
                ..LogConfig::default()
            });
            let mut bytes = bytes::BytesMut::new();
            let mut sample = sample_batch(EmptyPayloadBatchSetup::default());
            sample.records[0].value = Some(bytes::Bytes::from(vec![7; payload]));
            sample.encode(&mut bytes).unwrap();
            let bytes = bytes.freeze();
            let validated = krabka_protocol::records::validate_one_v2_batch(&bytes).unwrap();
            let batch = krabka_log::VerbatimBatch::from_header(validated.header, bytes.clone());
            let log = Mutex::new(log);
            let datas = (0..jobs)
                .map(|_| ProduceData::Verbatim(batch.clone()))
                .collect();
            let (results, leo, controls) = append_produce_batch(&log, None, (datas, Vec::new()));
            assert!(controls.is_empty());
            assert!(disk.next.load(std::sync::atomic::Ordering::Relaxed) == writes);
            let failures: Vec<_> = results.iter().filter(|result| result.is_err()).collect();
            assert!(failures.len() == failed);
            assert!(failures.iter().all(|result| matches!(result,
                    Err(crate::error::BrokerError::Log(krabka_log::LogError::Io(error)))
                    if error.kind() == std::io::ErrorKind::StorageFull)));
            let base = i64::from(seeded);
            let end = base + i64::try_from(jobs - failed).unwrap();
            for result in results.iter().flatten() {
                assert!(result.log_append_time_ms.is_some() == append_time);
            }
            let assigned: Vec<_> = results
                .into_iter()
                .filter_map(Result::ok)
                .map(|result| result.base_offset)
                .collect();
            assert!(assigned == (base..end).map(Offset).collect::<Vec<_>>());
            assert!(leo == Offset(end));
            assert!(
                lock_log(&log)
                    .read(Offset(0), mebibytes(16))
                    .unwrap()
                    .batches
                    .len()
                    == usize::try_from(leo.0).unwrap()
            );
        }
    }

    #[test]
    fn grouped_produce_surfaces_nth_log_write_failure_without_advancing_leo() {
        let dir = tempdir().expect("tempdir");
        let log = failing_log(dir.path());

        let (results, leo, _) =
            append_produce_batch(&log, None, (two_single_record_batches(), Vec::new()));

        assert_partial_append(&results, leo);
        assert!(log.lock().unwrap().log_end_offset() == Offset(1));
    }

    #[test]
    fn diskless_group_reanchors_after_partial_append_failure() {
        let dir = tempdir().expect("tempdir");
        let log = failing_log(dir.path());

        let (results, leo, _) = append_produce_batch_at(
            &log,
            Offset(0),
            None,
            (two_single_record_batches(), Vec::new()),
        );

        assert_partial_append(&results, leo);

        let (results, leo, _) = append_produce_batch_at(
            &log,
            Offset(2),
            None,
            (
                vec![ProduceData::Owned(sample_batch(
                    EmptyPayloadBatchSetup::default(),
                ))],
                Vec::new(),
            ),
        );

        assert!(results[0].as_ref().unwrap().base_offset == Offset(2));
        assert!(leo == Offset(3));
    }

    #[test]
    fn append_owned_batch_recompresses_to_configured_log_codec() {
        let dir = tempdir().expect("tempdir");
        let log = lz4_log(dir.path());

        let original = sample_batch(EmptyPayloadBatchSetup {
            count: RecordCount(2),
        });
        assert!(original.attributes.compression() == CompressionType::None);

        let (results, leo, _) =
            append_produce_batch(&log, None, (vec![ProduceData::Owned(original)], Vec::new()));
        assert!(results.len() == 1);
        let assigned = results.into_iter().next().unwrap().expect("append ok");
        assert!(assigned.base_offset == 0);
        assert!(leo == 2);

        let batch = one_appended_batch(&log);
        check!(batch.attributes.compression() == CompressionType::Lz4);
        check!(batch.records.len() == 2);
    }

    /// A control batch that arrives uncompressed stays uncompressed, whatever
    /// the topic's `compression.type` says. Kafka never compresses one, and a
    /// control batch holds one small record, so the rewrite would both diverge
    /// from Kafka and buy nothing.
    /// A transactional COMMIT control batch for producer 7 at epoch 3, built
    /// from the batch `sample_batch` gives: its one record carries the
    /// end-transaction marker Kafka writes, a 4-byte key (version 0, type 1)
    /// and a 6-byte value (version 0, coordinator epoch 17), so the append
    /// path's control-record check accepts it.
    fn commit_marker_batch() -> krabka_protocol::records::RecordBatch {
        let mut marker = sample_batch(EmptyPayloadBatchSetup::default());
        marker.attributes = marker
            .attributes
            .with_transactional(true)
            .with_control(true);
        marker.producer_id = 7;
        marker.producer_epoch = 3;
        for record in &mut marker.records {
            record.key = Some(bytes::Bytes::from_static(&[0, 0, 0, 1]));
            record.value = Some(bytes::Bytes::from_static(&[0, 0, 0, 0, 0, 17]));
        }
        marker
    }

    fn one_appended_batch(log: &Mutex<Log>) -> krabka_protocol::records::RecordBatch {
        let read = log
            .lock()
            .unwrap()
            .read(Offset(0), krabka_units::mebibytes(10))
            .unwrap();
        assert!(read.batches.len() == 1);
        read.batches.into_iter().next().unwrap()
    }

    fn check_control_append(
        log: &Mutex<Log>,
        control_entries: &[krabka_log::ProducerSnapshotEntry],
    ) {
        assert!(control_entries.len() == 1);
        check!(control_entries[0].producer_id == krabka_log::ProducerId(7));
        check!(control_entries[0].producer_epoch == 3);
        let batch = one_appended_batch(log);
        check!(batch.attributes.compression() == CompressionType::None);
        check!(batch.attributes.is_control_batch());
    }

    #[test]
    fn append_control_batch_keeps_its_own_compression() {
        let dir = tempdir().expect("tempdir");
        let log = lz4_log(dir.path());

        let marker = commit_marker_batch();
        assert!(marker.attributes.compression() == CompressionType::None);

        let (results, _, control_entries) = append_produce_batch(
            &log,
            None,
            (vec![ProduceData::OwnedControl(marker)], Vec::new()),
        );
        let assigned = results.into_iter().next().unwrap().expect("append ok");
        assert!(assigned.base_offset == 0);

        // The mirror entry comes from this same lock acquisition, not a
        // second one: this is what keeps a produce-path mirror from ever
        // blocking a worker thread behind a concurrent blocking-pool log
        // operation.
        check_control_append(&log, &control_entries);
    }

    /// A group that carries a transaction marker first releases every
    /// complete transaction whose marker the given high watermark passed, so
    /// a partition that no reader fetches from does not keep them forever.
    #[test]
    fn a_marker_group_releases_transactions_below_the_high_watermark() {
        fn data(producer_id: i64) -> ProduceData {
            let mut batch = sample_batch(EmptyPayloadBatchSetup::default());
            batch.producer_id = producer_id;
            batch.attributes = batch.attributes.with_transactional(true);
            ProduceData::Owned(batch)
        }
        fn commit(producer_id: i64) -> ProduceData {
            let mut batch = sample_batch(EmptyPayloadBatchSetup::default());
            batch.producer_id = producer_id;
            batch.attributes = batch.attributes.with_transactional(true).with_control(true);
            batch.records[0].key = Some(bytes::Bytes::from_static(&[0, 0, 0, 1]));
            batch.records[0].value = Some(bytes::Bytes::from_static(&[0, 0, 0, 0, 0, 0]));
            ProduceData::OwnedControl(batch)
        }
        for (name, high_watermark, want_first_unstable) in [
            ("no high watermark", None, Offset(0)),
            (
                "high watermark at the first marker",
                Some(Offset(1)),
                Offset(0),
            ),
            (
                "high watermark past the first marker",
                Some(Offset(2)),
                Offset(2),
            ),
        ] {
            for at_offset in [false, true] {
                let dir = tempdir().expect("tempdir");
                let log =
                    Mutex::new(Log::open(dir.path(), LogConfig::default()).expect("open log"));
                let append = |high_watermark, datas: Vec<ProduceData>| {
                    let base = lock_log(&log).log_end_offset();
                    let (results, _, _) = if at_offset {
                        append_produce_batch_at(&log, base, high_watermark, (datas, Vec::new()))
                    } else {
                        append_produce_batch(&log, high_watermark, (datas, Vec::new()))
                    };
                    assert!(results.iter().all(Result::is_ok), "{name}");
                };
                append(None, vec![data(7), commit(7)]); // offsets 0 and 1
                append(None, vec![data(8)]); // offset 2
                append(high_watermark, vec![commit(8)]); // offset 3

                check!(
                    lock_log(&log).lso() == want_first_unstable,
                    "{name}, at offset {at_offset}"
                );
            }
        }
    }

    /// The follower path makes the same promise as the leader path. A
    /// replicated control batch must land byte-for-byte as the leader wrote
    /// it, or the two logs diverge.
    #[test]
    fn append_control_batch_at_offset_keeps_its_own_compression() {
        let dir = tempdir().expect("tempdir");
        let log = lz4_log(dir.path());

        let marker = commit_marker_batch();

        let (results, _, control_entries) = append_produce_batch_at(
            &log,
            Offset(0),
            None,
            (vec![ProduceData::OwnedControl(marker)], Vec::new()),
        );
        assert!(
            results
                .into_iter()
                .next()
                .unwrap()
                .expect("append ok")
                .base_offset
                == 0
        );
        check_control_append(&log, &control_entries);
    }
}
