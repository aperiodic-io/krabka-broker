//! The log reads behind a `ShareFetch` response, and the assembly of their
//! bytes into one partition row.
//!
//! An acquire pass hands this module a partition's acquisition state. The
//! module reads the log first, locks only the offsets inside the bytes that
//! the read returned, and gives back those batch bytes plus the
//! `acquired_records` rows that describe them. The same log-scan shape
//! answers the two questions the pass asks before it acquires: which offsets
//! hold control batches or aborted transactional data, and which offsets
//! KFC-1 scheduled delivery has not released yet.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use krabka_log::{Offset, RawRead};
use krabka_protocol::{
    owned::share_fetch_response::{AcquiredRecords, PartitionData},
    records::{Attributes, HEADER_LEN, RecordBatchHeader, RecordsPayload},
};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use zerocopy::FromBytes as _;

use super::tiered::TieredSource;
use crate::{
    error::BrokerError,
    remote_reader::RemoteReader,
    share_partition::state::{AcquireShape, AcquiredRange, AcquisitionState},
};

/// What one acquire step may take from a share partition.
pub(super) struct AcquireRequest<'a> {
    pub(super) member: &'a str,
    /// The records that the request can still take, across its partitions.
    pub(super) max_records: i32,
    /// The byte budget of the log read.
    pub(super) max_bytes: i32,
    /// Whether the read may exceed `max_bytes` by the one batch it starts
    /// with: Kafka's `minOneMessage`, which only the first partition of a
    /// response that returns records gets.
    pub(super) min_one_batch: bool,
    /// The exclusive end of the readable window: the high watermark, or the
    /// last stable offset under `read_committed`.
    pub(super) upper: Offset,
    pub(super) now: Instant,
    pub(super) lock_duration: Duration,
    pub(super) max_attempts: i16,
    /// How the request shapes what it acquires.
    pub(super) mode: AcquireMode,
    /// The remote tier of a tiered partition (KIP-405), which serves the
    /// offsets below the local log start.
    pub(super) tier: Option<TieredSource<'a>>,
}

/// Kafka's `ShareAcquireMode` with the request's `BatchSize`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AcquireMode {
    /// `batch_optimized` (0, and the only mode before v2): `MaxRecords` is a
    /// soft limit that rounds up to a log batch end, and new records come
    /// back in rows of about `batch_size` records on log batch boundaries.
    BatchOptimized { batch_size: i32 },
    /// `record_limit` (1): at most `MaxRecords` records, in one row per run.
    RecordLimit,
}

impl AcquireMode {
    /// The mode of a request: its `ShareAcquireMode` and `BatchSize`.
    pub(crate) fn of(share_acquire_mode: i8, batch_size: i32) -> Self {
        if share_acquire_mode == 1 {
            Self::RecordLimit
        } else {
            Self::BatchOptimized { batch_size }
        }
    }
}

/// The byte budget of one partition's log read.
///
/// The per-partition cap is absent at supported protocol versions and decodes
/// to zero, so the read falls back to the request-wide byte budget.
pub(super) fn read_budget(partition_max_bytes: i32, request_max_bytes: i32) -> i32 {
    if partition_max_bytes > 0 {
        partition_max_bytes
    } else {
        request_max_bytes
    }
}

/// Reads the log, then acquires records only inside the bytes that the read
/// returned, and fills `out` with those bytes and the acquired rows.
///
/// Kafka reads first and acquires second. `SharePartition.acquire` bounds the
/// acquisition by the last batch of the fetched records, so every offset in
/// `acquired_records` has its record in `records`. The Java share consumer
/// treats an acquired offset with no record as a gap and acknowledges it as
/// `Gap`, which archives it. An acquisition that ran past the byte budget
/// would therefore lose the records that the read cut off.
///
/// The read starts at the first offset that the state can hand out. The
/// response carries each read batch that holds an acquired offset, and no
/// other batch. It returns the number of offsets that it acquired.
///
/// An offset that only the remote tier holds is read from the tier, as Kafka's
/// `DelayedShareFetch` reads it through `RemoteLogManager.asyncRead`. The
/// control batches, the aborted transactional data and the not-yet-due
/// batches of that read are taken out of the window here, because the scans
/// that do so for the local log cannot read the tier.
pub(super) async fn acquire_read_records(
    out: &mut PartitionData,
    partition: &Arc<crate::partition::Partition>,
    state: &mut AcquisitionState,
    request: &AcquireRequest<'_>,
) -> Result<i64, BrokerError> {
    let Some(from) = state.first_acquirable_offset(request.max_attempts) else {
        return Ok(0);
    };
    let read = match &request.tier {
        // An offset below an established log start stays with the local
        // read, whose `OffsetTooLow` moves the share-partition start offset
        // past it.
        Some(tier) if RemoteReader::serves(partition, from) => {
            let Some(read) = tier.read(partition, from, request.max_bytes).await? else {
                return Ok(0);
            };
            for (first, last) in &read.unreadable {
                state.archive_internal(*first, *last);
            }
            for (first, last) in &read.not_due {
                state.defer_internal(*first, *last);
            }
            read.bytes
        }
        _ => {
            let Some(read) = read_raw(partition, from, request.upper, request.max_bytes).await?
            else {
                return Ok(0);
            };
            read.bytes
        }
    };
    let read_bytes = batches_within(&read, request.max_bytes, request.min_one_batch)?;
    let bounds = batch_bounds(&read_bytes)?;
    let Some(&(_, read_last)) = bounds.last() else {
        return Ok(0);
    };
    let last = Offset(read_last).min(request.upper - 1);
    let ends: Vec<Offset> = bounds.iter().map(|(_, last)| Offset(*last)).collect();
    let acquired = state.acquire_shaped(
        request.member,
        AcquireShape {
            max_records: request.max_records,
            batch_ends: match request.mode {
                AcquireMode::BatchOptimized { .. } => Some(&ends),
                AcquireMode::RecordLimit => None,
            },
        },
        last,
        (request.now, request.lock_duration),
        request.max_attempts,
    );
    if acquired.is_empty() {
        return Ok(0);
    }
    let records = batches_holding(&read_bytes, &acquired)?;
    if !records.is_empty() {
        out.records = Some(RecordsPayload::Raw(records));
    }
    let bases: Vec<i64> = bounds.iter().map(|(base, _)| *base).collect();
    out.acquired_records = acquired
        .iter()
        .flat_map(|range| rows_of(range, request.mode, &bases))
        .collect();
    Ok(acquired
        .iter()
        .map(|range| range.last.0 - range.first.0 + 1)
        .sum())
}

/// The `AcquiredRecords` rows of one acquired run.
///
/// Kafka's `SharePartition.createBatches`: in `batch_optimized` mode a run
/// of new records (delivery count 1) longer than `batch_size` is split into
/// rows on log batch boundaries, a new row starting at the first batch base
/// at least `batch_size` records past the start of the current one. Every
/// other run is one row.
fn rows_of(range: &AcquiredRange, mode: AcquireMode, bases: &[i64]) -> Vec<AcquiredRecords> {
    let row = |first_offset, last_offset| AcquiredRecords {
        first_offset,
        last_offset,
        delivery_count: range.delivery_count,
        ..Default::default()
    };
    let (first, last) = (range.first.0, range.last.0);
    let AcquireMode::BatchOptimized { batch_size } = mode else {
        return vec![row(first, last)];
    };
    if range.delivery_count != 1 || batch_size <= 0 || last - first < i64::from(batch_size) {
        return vec![row(first, last)];
    }
    let mut rows = Vec::new();
    let mut current = first;
    for base in bases
        .iter()
        .copied()
        .filter(|base| *base > first && *base <= last)
    {
        if base - current >= i64::from(batch_size) {
            rows.push(row(current, base - 1));
            current = base;
        }
    }
    rows.push(row(current, last));
    rows
}

/// One v2 batch of a read: where its bytes sit, the offsets it holds, and the
/// header fields that decide whether a share consumer may get it.
pub(super) struct BatchSpan {
    bytes: std::ops::Range<usize>,
    pub(super) base: i64,
    pub(super) last: i64,
    pub(super) attributes: Attributes,
    pub(super) producer_id: i64,
    pub(super) max_timestamp: i64,
}

/// Every v2 batch in `bytes`, in the order that the bytes hold them. It reads
/// only the batch headers.
pub(super) fn batch_spans(bytes: &Bytes) -> Result<Vec<BatchSpan>, BrokerError> {
    let mut spans = Vec::new();
    let mut at = 0_usize;
    while at < bytes.len() {
        let header = bytes
            .get(at..at + HEADER_LEN)
            .and_then(|raw| RecordBatchHeader::ref_from_bytes(raw).ok())
            .ok_or_else(|| corrupt_read("a truncated record batch header"))?;
        let length = usize::try_from(header.batch_length.get())
            .ok()
            .map(|length| length + LOG_OVERHEAD)
            .filter(|length| *length >= HEADER_LEN && at + length <= bytes.len())
            .ok_or_else(|| corrupt_read("a record batch length outside the read"))?;
        let base = header.base_offset.get();
        spans.push(BatchSpan {
            bytes: at..at + length,
            base,
            last: base + i64::from(header.last_offset_delta.get()),
            attributes: Attributes(header.attributes.get()),
            producer_id: header.producer_id.get(),
            max_timestamp: header.max_timestamp.get(),
        });
        at += length;
    }
    Ok(spans)
}

/// The `(base_offset, last_offset)` of every v2 batch in `bytes`, in log
/// order. It reads only the batch headers.
fn batch_bounds(bytes: &Bytes) -> Result<Vec<(i64, i64)>, BrokerError> {
    Ok(batch_spans(bytes)?
        .into_iter()
        .map(|span| (span.base, span.last))
        .collect())
}

/// The byte ranges of `bytes`, joined into one buffer. It copies nothing
/// when one range covers them.
fn gather(bytes: &Bytes, ranges: &[std::ops::Range<usize>]) -> Bytes {
    // Adjacent ranges read as one run.
    let mut runs: Vec<std::ops::Range<usize>> = Vec::new();
    for range in ranges {
        match runs.last_mut() {
            Some(run) if run.end == range.start => run.end = range.end,
            _ => runs.push(range.clone()),
        }
    }
    match runs.as_slice() {
        [] => Bytes::new(),
        [run] => bytes.slice(run.clone()),
        runs => {
            let mut blob = BytesMut::with_capacity(runs.iter().map(ExactSizeIterator::len).sum());
            for run in runs {
                blob.extend_from_slice(&bytes[run.clone()]);
            }
            blob.freeze()
        }
    }
}

/// Returns the batches of `bytes` that hold at least one offset of
/// `acquired`, in log order.
///
/// It walks the v2 batch headers and decodes no record. When every batch
/// qualifies, it returns `bytes` without a copy.
fn batches_holding(bytes: &Bytes, acquired: &[AcquiredRange]) -> Result<Bytes, BrokerError> {
    let kept: Vec<_> = batch_spans(bytes)?
        .into_iter()
        .filter(|span| {
            acquired
                .iter()
                .any(|range| range.first.0 <= span.last && span.base <= range.last.0)
        })
        .map(|span| span.bytes)
        .collect();
    Ok(gather(bytes, &kept))
}

/// The leading whole batches of `bytes` that fit in `max_bytes`.
///
/// `Log::read_raw` can return one batch past the budget: a read that crosses
/// a segment boundary takes the next segment's first batch whole. Only
/// Kafka's `minOneMessage` lets a read exceed its budget, and only by the
/// first batch when no batch fits (`LogSegment.read` raises the size to that
/// one batch, and `LocalLog.read` reads a single segment). So with
/// `min_one_batch` the result is the batches that fit, or the first batch
/// alone when none does; without it, only the batches that fit, so a later
/// partition whose first batch does not fit returns no records.
fn batches_within(
    bytes: &Bytes,
    max_bytes: i32,
    min_one_batch: bool,
) -> Result<Bytes, BrokerError> {
    let budget = usize::try_from(max_bytes.max(0)).unwrap_or(usize::MAX);
    let spans = batch_spans(bytes)?;
    let mut kept: Vec<_> = spans
        .iter()
        .map(|span| span.bytes.clone())
        .take_while(|range| range.end <= budget)
        .collect();
    if kept.is_empty() && min_one_batch {
        kept.extend(spans.into_iter().next().map(|span| span.bytes));
    }
    Ok(gather(bytes, &kept))
}

/// Joins the batches that a later acquire pass read onto the batches that
/// the earlier passes of the same response already carry, and returns the
/// joined records with the number of bytes that the join added.
///
/// Two passes can read the same batch: a pass that acquires part of a batch
/// leaves the rest of it to a later pass, and the later read starts inside
/// that batch, so `Log::read_raw` returns the whole batch again. Kafka reads
/// the log once per response, so its records never hold a batch twice. This
/// keeps one copy of each batch, by base offset, in log order.
pub(super) fn merge_batches(before: &Bytes, added: &Bytes) -> Result<(Bytes, i64), BrokerError> {
    let held = batch_spans(before)?;
    let fresh: Vec<BatchSpan> = batch_spans(added)?
        .into_iter()
        .filter(|span| held.iter().all(|kept| kept.base != span.base))
        .collect();
    let added_len =
        i64::try_from(fresh.iter().map(|span| span.bytes.len()).sum::<usize>()).unwrap_or(i64::MAX);
    if fresh.is_empty() {
        return Ok((before.clone(), 0));
    }
    if held.is_empty() {
        return Ok((
            gather(
                added,
                &fresh.into_iter().map(|span| span.bytes).collect::<Vec<_>>(),
            ),
            added_len,
        ));
    }
    let mut all: Vec<(i64, &Bytes, std::ops::Range<usize>)> = held
        .into_iter()
        .map(|span| (span.base, before, span.bytes))
        .chain(fresh.into_iter().map(|span| (span.base, added, span.bytes)))
        .collect();
    all.sort_by_key(|(base, _, _)| *base);
    let mut blob = BytesMut::with_capacity(all.iter().map(|(_, _, range)| range.len()).sum());
    for (_, source, range) in all {
        blob.extend_from_slice(&source[range]);
    }
    Ok((blob.freeze(), added_len))
}

/// The bytes of a v2 batch in front of its `batch_length` field: the base
/// offset (8) and the length itself (4).
const LOG_OVERHEAD: usize = 12;

/// A read whose bytes are not a run of whole record batches. Kafka reports
/// such a batch as `CORRUPT_MESSAGE`, which [`LogError::Records`] maps to.
fn corrupt_read(what: &str) -> BrokerError {
    BrokerError::Log(krabka_log::LogError::Records(
        krabka_protocol::records::RecordsError::RecordParse(format!(
            "share-fetch read returned {what}"
        )),
    ))
}

/// Reads the verbatim on-disk batch bytes for `[fetch_offset, limit_offset)`
/// through `Log::read_raw`, off the reactor thread. It returns `None` when it
/// read nothing.
async fn read_raw(
    part: &crate::partition::Partition,
    fetch_offset: Offset,
    limit_offset: Offset,
    max_bytes: i32,
) -> Result<Option<RawRead>, BrokerError> {
    if limit_offset <= fetch_offset {
        return Ok(None);
    }
    let read_max = ByteSize::from_bytes_i64(i64::from(max_bytes.max(0)));
    let log = part.log.clone();
    let join = crate::blocking::spawn_blocking(move || {
        let log = log.lock().expect("log mutex poisoned");
        log.read_raw(fetch_offset, limit_offset, read_max)
    });
    let raw = match join.await {
        Ok(res) => res?,
        Err(join_err) => {
            return Err(BrokerError::Io(std::io::Error::other(format!(
                "share-fetch read task panicked: {join_err}"
            ))));
        }
    };
    Ok((raw.total > 0).then_some(raw))
}

/// The aborted transactions of a window, by producer, as `(first offset,
/// abort marker offset)` pairs.
#[derive(Debug, Default)]
pub(super) struct AbortedRanges(HashMap<i64, Vec<(i64, i64)>>);

impl AbortedRanges {
    /// Records an aborted transaction of `producer_id` that starts at `first`
    /// and ends at its abort marker, `marker`.
    pub(super) fn add(&mut self, producer_id: i64, first: i64, marker: i64) {
        self.0.entry(producer_id).or_default().push((first, marker));
    }

    /// Whether a share consumer must never get the batch of `producer_id`
    /// with `attributes` that holds the offsets `[base, last]`: it is a
    /// control batch, or transactional data of a transaction this holds,
    /// which ends after the batch.
    pub(super) fn excludes(
        &self,
        attributes: Attributes,
        producer_id: i64,
        (base, last): (i64, i64),
    ) -> bool {
        attributes.is_control_batch()
            || (attributes.is_transactional()
                && self.0.get(&producer_id).is_some_and(|txns| {
                    txns.iter()
                        .any(|&(first, marker)| first <= base && last <= marker)
                }))
    }
}

/// The byte budget, in bytes, of one log read while
/// [`unreadable_batch_ranges`] walks a window.
const UNREADABLE_SCAN_CHUNK_BYTES: u64 = 1 << 20;

/// Returns the offset ranges in `[start, end)` that a share consumer must
/// never get: every control batch, and under `read_committed` every data
/// batch of an aborted transaction.
///
/// Share acquisition state is offset-based and therefore materializes log
/// control markers along with data unless the handler explicitly archives
/// them. The decoded log read keeps this classification out of the raw-byte
/// response path.
///
/// A `ShareFetch` response has no aborted-transactions field, so the share
/// consumer cannot drop an aborted batch itself. Kafka's
/// `SharePartition.filterAbortedTransactionalAcquiredRecords` archives those
/// batches for a `read_committed` share group. A batch is aborted when it is
/// transactional and its producer has an aborted transaction whose range, up
/// to the abort marker, holds the batch.
pub(super) async fn unreadable_batch_ranges(
    part: &crate::partition::Partition,
    start: Offset,
    end: Offset,
    read_committed: bool,
) -> Result<Vec<(Offset, Offset)>, BrokerError> {
    if end <= start {
        return Ok(Vec::new());
    }
    scan_log(part, "control", move |log| {
        let mut aborted = AbortedRanges::default();
        if read_committed {
            for txn in log.aborted_in_range(start, end) {
                aborted.add(txn.producer_id.get(), txn.start_offset.0, txn.last_offset.0);
            }
        }
        // Read the window in bounded chunks, and stop at `end`, so a window
        // far behind the log end does not decode the rest of the log.
        let mut ranges = Vec::new();
        let mut cursor = start;
        while cursor < end {
            let read = log.read(cursor, ByteSize::from_bytes(UNREADABLE_SCAN_CHUNK_BYTES))?;
            let Some(last_batch) = read.batches.last() else {
                break;
            };
            let next = Offset(last_batch.base_offset + i64::from(last_batch.last_offset_delta) + 1);
            for batch in &read.batches {
                let last = batch.base_offset + i64::from(batch.last_offset_delta);
                if batch.base_offset >= end.0 {
                    break;
                }
                let unreadable = aborted.excludes(
                    batch.attributes,
                    batch.producer_id,
                    (batch.base_offset, last),
                );
                let first = Offset(batch.base_offset).max(start);
                let last = Offset(last).min(end - 1);
                if unreadable && first <= last {
                    ranges.push((first, last));
                }
            }
            if next <= cursor {
                break;
            }
            cursor = next;
        }
        Ok::<_, krabka_log::LogError>(ranges)
    })
    .await?
    .map_err(BrokerError::from)
}

/// Returns the offset ranges in `[start, end)` that KFC-1 scheduled delivery
/// has not released yet, as of `now_ms`.
///
/// A share group is the one reader that may take a due record from behind a
/// waiting one, so it needs every gap in the window and not the leading active
/// prefix that caps a classic `Fetch`. The ranges come back batch-aligned and
/// coalesced, which suits an acquisition state that is offset-based and a read
/// path that is batch-granular.
///
/// `now_ms` is the partition's own delivery clock, so an append, the delivery
/// scheduler, and this pass all decide against one timeline. A topic that
/// delivers immediately answers with nothing before it reads a batch header,
/// so the ordinary case costs one call and no I/O.
pub(super) async fn pending_activation_ranges(
    part: &crate::partition::Partition,
    start: Offset,
    end: Offset,
    now_ms: i64,
) -> Result<Vec<(Offset, Offset)>, BrokerError> {
    if end <= start {
        return Ok(Vec::new());
    }
    scan_log(part, "activation", move |log| {
        log.pending_activation_ranges(start, end - 1, now_ms)
    })
    .await
}

/// Run a range scan under the log lock and retain each caller's panic label.
async fn scan_log<T: Send + 'static>(
    part: &crate::partition::Partition,
    kind: &'static str,
    read: impl FnOnce(&krabka_log::Log) -> T + Send + 'static,
) -> Result<T, BrokerError> {
    let log = part.log.clone();
    crate::blocking::spawn_blocking(move || {
        let log = log.lock().expect("log mutex poisoned");
        read(&log)
    })
    .await
    .map_err(|join_err| {
        BrokerError::Io(std::io::Error::other(format!(
            "share-fetch {kind} scan panicked: {join_err}"
        )))
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_log::DeliveryPolicy;
    use qubit_clock::{FixedWallClock, WallClock};

    use super::*;
    use crate::delivery::test_support::{NOW_MS, scheduled_partition, wall_at};

    #[tokio::test]
    async fn only_the_batches_that_are_not_due_are_reported_as_pending() {
        // Three two-record batches: due, not due, due. The middle one is the
        // case a classic Fetch cannot serve around.
        let activations = [NOW_MS - 60_000, NOW_MS + 60_000, NOW_MS - 60_000];
        let clock: Arc<dyn WallClock> = Arc::new(FixedWallClock::new(wall_at(NOW_MS)));

        let dir = tempfile::tempdir().expect("a log root");
        let scheduled = scheduled_partition(
            &dir,
            &clock,
            crate::delivery::test_support::ScheduleSetup {
                activations: activations
                    .iter()
                    .copied()
                    .map(crate::test_support::UnixMillis)
                    .collect(),
                leader: krabka_ids::NodeId(0),
                ..Default::default()
            },
        );
        let ranges = pending_activation_ranges(&scheduled, Offset(0), Offset(6), NOW_MS)
            .await
            .expect("scan the schedule");
        assert!(ranges == vec![(Offset(2), Offset(3))]);

        // The window bound is exclusive, so a window that stops below the
        // waiting batch reports nothing.
        let clipped = pending_activation_ranges(&scheduled, Offset(0), Offset(2), NOW_MS)
            .await
            .expect("scan the schedule");
        assert!(clipped == Vec::new());

        // An immediate topic answers with nothing whatever its timestamps say.
        let immediate_dir = tempfile::tempdir().expect("a log root");
        let immediate = scheduled_partition(
            &immediate_dir,
            &clock,
            crate::delivery::test_support::ScheduleSetup {
                topic: "immediate",
                policy: DeliveryPolicy::Immediate,
                activations: activations
                    .iter()
                    .copied()
                    .map(crate::test_support::UnixMillis)
                    .collect(),
                leader: krabka_ids::NodeId(0),
            },
        );
        let none = pending_activation_ranges(&immediate, Offset(0), Offset(6), NOW_MS)
            .await
            .expect("scan the schedule");
        assert!(none == Vec::new());
    }

    // Kafka's `SharePartition.filterAbortedTransactionalAcquiredRecords` with
    // the control batches beside them: what a share consumer must never get.
    // Producer 7 aborted a transaction that runs from offset 10 to its marker
    // at offset 20. Each case is `(label, attributes, producer, batch range,
    // excluded)`.
    #[test]
    fn control_batches_and_aborted_data_are_excluded() {
        let mut aborted = AbortedRanges::default();
        aborted.add(7, 10, 20);
        let transactional = Attributes::default().with_transactional(true);
        let cases = [
            (
                "a control batch",
                transactional.with_control(true),
                7,
                (20, 20),
                true,
            ),
            ("plain data", Attributes::default(), 7, (12, 14), false),
            (
                "data inside the aborted transaction",
                transactional,
                7,
                (12, 14),
                true,
            ),
            (
                "data past the abort marker",
                transactional,
                7,
                (21, 22),
                false,
            ),
            ("another producer's data", transactional, 8, (12, 14), false),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (label, attributes, producer_id, range, excluded) in cases {
            actual.push((label, aborted.excludes(attributes, producer_id, range)));
            expected.push((label, excluded));
        }
        assert!(actual == expected);
    }

    /// Three one-record batches of equal size at offsets 0, 1 and 2, whole
    /// and one by one.
    fn three_batches() -> (Bytes, Vec<Bytes>) {
        let batches: Vec<Bytes> = (0..3)
            .map(|base| {
                let mut one = BytesMut::new();
                krabka_protocol::records::RecordBatch {
                    base_offset: base,
                    records: vec![krabka_protocol::records::Record {
                        value: Some(Bytes::from(vec![0_u8; 32])),
                        ..Default::default()
                    }],
                    ..Default::default()
                }
                .encode(&mut one)
                .expect("encode a batch");
                one.freeze()
            })
            .collect();
        (batches.concat().into(), batches)
    }

    /// Kafka's `minOneMessage` lets a read exceed its budget only by its
    /// first batch, and only when no batch fits: a read that crossed a
    /// segment boundary and brought back a later batch past the budget keeps
    /// just the batches that fit.
    #[test]
    fn only_the_first_batch_may_exceed_the_budget() {
        let (read, batches) = three_batches();
        let size = i32::try_from(batches[0].len()).expect("a small batch");
        let first = batches[0].clone();
        let first_two: Bytes = [batches[0].clone(), batches[1].clone()].concat().into();
        // (budget, min_one_batch, kept)
        let cases = [
            (size, true, first.clone()),
            (size + size / 2, true, first.clone()),
            (size / 2, true, first.clone()),
            (0, true, first.clone()),
            (2 * size, true, first_two.clone()),
            (size / 2, false, Bytes::new()),
            (size + size / 2, false, first),
            (3 * size, false, read.clone()),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (budget, min_one_batch, want) in cases {
            let kept = batches_within(&read, budget, min_one_batch).expect("whole batches");
            actual.push((budget, min_one_batch, kept));
            expected.push((budget, min_one_batch, want));
        }
        assert!(actual == expected);
    }

    /// Kafka's `createBatches` over log batches of five records at 0, 5, 10
    /// and 15.
    #[test]
    fn new_records_split_into_rows_on_batch_boundaries() {
        let bases = [0, 5, 10, 15];
        let range = |first, last, delivery_count| AcquiredRange {
            first: Offset(first),
            last: Offset(last),
            delivery_count,
        };
        let rows = [
            (
                range(0, 19, 1),
                AcquireMode::BatchOptimized { batch_size: 10 },
                vec![(0, 9), (10, 19)],
            ),
            (
                range(0, 19, 1),
                AcquireMode::BatchOptimized { batch_size: 7 },
                vec![(0, 9), (10, 19)],
            ),
            (
                range(0, 19, 1),
                AcquireMode::BatchOptimized { batch_size: 20 },
                vec![(0, 19)],
            ),
            (range(0, 19, 1), AcquireMode::RecordLimit, vec![(0, 19)]),
            // A redelivery keeps its one row.
            (
                range(0, 19, 2),
                AcquireMode::BatchOptimized { batch_size: 5 },
                vec![(0, 19)],
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (range, mode, want) in rows {
            let got: Vec<_> = rows_of(&range, mode, &bases)
                .into_iter()
                .map(|row| (row.first_offset, row.last_offset))
                .collect();
            actual.push((mode, range.delivery_count, got));
            expected.push((mode, range.delivery_count, want));
        }
        assert2::assert!(actual == expected);
    }
}
