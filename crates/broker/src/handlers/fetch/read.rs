//! The local log read: the plan a fetch derives under the partition's log
//! mutex, the blocking seek-and-read that serves it, and the response
//! fields the served bytes fill in.

use {
    super::{FetchWatermarks, VisibilityWindow, compute_visibility_window, plan::refused_read},
    crate::{
        blocking::run_blocking_catching, codes, diskless::hot_tail::HotTailCache,
        error::BrokerError, log_dir_status::LogDirRegistry, partition::Partition,
    },
    krabka_log::{Log, LogError, Offset},
    krabka_protocol::{
        owned::fetch_response::{AbortedTransaction, PartitionData},
        records::RecordsPayload,
    },
    krabka_units::{ByteSize, convert::ByteSizeExt as _},
    std::sync::{Arc, Mutex},
};

/// Hold the partition's log mutex for a short time to read the offsets, and
/// optionally the verbatim on-disk batch bytes through `Log::read_raw`.
///
/// The read fills `out` in place with a `RecordsPayload::Raw`. It returns the
/// byte-size estimate of the records it put in `out`, or 0 when it put none.
///
/// When `read_committed` is `true`, on a consumer fetch with
/// `isolation_level=1`:
/// - the raw bytes are clamped at `min(lso, hw)`, so
///   `base_offset < min(lso, hw)`
/// - there is NO server-side batch filtering. Aborted batches and control
///   batches stay in the byte stream, and the consumer drops them on the
///   client side with the list below
/// - `out.aborted_transactions` comes from the partition's `.txnindex` files
///
/// When `is_follower_fetch` is `true`:
/// - the raw bytes go up to LEO, with no HW clamp
///
/// When `read_committed` is `false` and `is_follower_fetch` is `false`, on a
/// consumer fetch in `read_uncommitted`:
/// - the raw bytes are clamped at HW, so `base_offset < hw`
/// - `out.aborted_transactions` is `None`
///
/// In every case `out.high_watermark` is `hw` and `out.last_stable_offset` is
/// `min(lso, hw)`, the values Kafka's `Partition.readRecords` reports to every
/// fetcher. A follower therefore adopts the leader's committed bound, never
/// its log end.
enum ReadPlan {
    OffsetOutOfRange,
    Empty,
    Read {
        limit_offset: Offset,
        effective_lso: Offset,
        read_committed_aborts: bool,
    },
}

pub(super) struct ReadRequest {
    pub(super) topic_id: Option<uuid::Uuid>,
    pub(super) hot_tail: Option<Arc<HotTailCache>>,
    pub(super) fetch_offset: Offset,
    pub(super) max_bytes: i32,
    pub(super) read_committed: bool,
    pub(super) is_follower_fetch: bool,
    pub(super) sendfile_capable: bool,
    pub(super) sendfile_min_bytes: usize,
}

/// Read one partition's local log into `out`, and return the bytes it served.
///
/// A failed read never fails the fetch. Kafka's `ReplicaManager.readFromLog`
/// catches whatever one partition's read throws and answers that partition's
/// row with the error, so the other partitions of the request are still
/// served; [`settle_failed_read`] is that catch.
pub(super) async fn do_read(
    part: &Partition,
    request: ReadRequest,
    log_dir_status: &LogDirRegistry,
    out: &mut PartitionData,
) -> usize {
    match read_local_log(part, request, out).await {
        Ok(bytes) => bytes,
        Err(error) => {
            *out = settle_failed_read(part, out.partition_index, &error, log_dir_status);
            0
        }
    }
}

/// The row of a read that failed, and the log-dir bookkeeping that goes with it.
///
/// The catch arms of Kafka's `ReplicaManager.readFromLog.read`:
///   * an `IOException`, which `LocalLog.maybeHandleIOException` has already
///     turned into `KafkaStorageException` after it took the dir offline, is
///     `KAFKA_STORAGE_ERROR`. The dir goes offline here as well, so the next
///     fetch reaches the offline-dir refusal and the controller moves
///     leadership away, instead of the same read failing on every retry;
///   * `OffsetOutOfRangeException` is `OFFSET_OUT_OF_RANGE`. The log raises
///     [`LogError::OffsetTooLow`] when retention or `DeleteRecords` moved the
///     log start between `plan_read` and the read, and
///     [`LogError::OffsetBelowLocalStart`] when tiering did. The row has the
///     shape `plan_read` gives the same answer, so the cold-tier fallback
///     treats both alike;
///   * `CorruptRecordException` is `CORRUPT_MESSAGE`;
///   * anything else is the generic row `Errors.forException` gives it.
fn settle_failed_read(
    part: &Partition,
    partition_index: i32,
    error: &BrokerError,
    log_dir_status: &LogDirRegistry,
) -> PartitionData {
    if let BrokerError::Log(LogError::Io(io_error)) = error {
        log_dir_status.mark_offline(
            &part.log_dir.load(),
            &format!("partition read failed: {io_error}"),
        );
    }
    let code = match error {
        BrokerError::Log(
            LogError::OffsetTooLow { .. } | LogError::OffsetBelowLocalStart { .. },
        ) => codes::OFFSET_OUT_OF_RANGE,
        BrokerError::Log(LogError::CrcMismatch { .. } | LogError::Corrupt(_)) => {
            codes::CORRUPT_MESSAGE
        }
        other => codes::from_broker_error(other),
    };
    if code != codes::OFFSET_OUT_OF_RANGE {
        tracing::error!(
            topic = %part.topic,
            partition = part.index.0,
            error_code = code,
            %error,
            "fetch: local log read failed",
        );
    }
    refused_read(partition_index, code)
}

async fn read_local_log(
    part: &Partition,
    request: ReadRequest,
    out: &mut PartitionData,
) -> Result<usize, BrokerError> {
    let ReadRequest {
        topic_id,
        hot_tail,
        fetch_offset,
        max_bytes,
        read_committed,
        is_follower_fetch,
        sendfile_capable,
        sendfile_min_bytes,
    } = request;
    let hw = part.high_watermark().await;
    let PlannedWindow {
        log_start,
        window: w,
        plan,
    } = plan_read(
        part,
        fetch_offset,
        hw,
        read_committed,
        is_follower_fetch,
        out,
    );
    // Log mutex released here.

    // The cache answers with whole batches, so it takes the window's limit and
    // serves only a batch that ends below it. The high watermark is
    // batch-aligned by construction and never cut one short, but the delivery
    // watermark of KFC-1 is a second cap on the same window, and a cache that
    // ignored it would hand out a batch the log read path would have held back.
    if part.diskless
        && !read_committed
        && let ReadPlan::Read { limit_offset, .. } = &plan
        && let (Some(topic_id), Some(hot_tail)) = (topic_id, hot_tail.as_ref())
        && let Some(bytes) = hot_tail.get(
            topic_id,
            part.index,
            fetch_offset.0,
            limit_offset.0,
            usize::try_from(max_bytes.max(0)).unwrap_or(0),
        )
    {
        return Ok(finish_read(
            out,
            &w,
            log_start,
            read_committed,
            is_follower_fetch,
            Vec::new(),
            Some(RecordsPayload::Raw(bytes)),
        ));
    }

    let ServedRecords {
        records,
        aborted: aborted_txns,
    } = match plan {
        ReadPlan::OffsetOutOfRange => return Ok(0),
        ReadPlan::Empty => ServedRecords {
            records: None,
            aborted: Vec::new(),
        },
        ReadPlan::Read {
            limit_offset,
            effective_lso,
            read_committed_aborts,
        } => {
            let read_max = ByteSize::from_bytes_i64(i64::from(max_bytes.max(0)));
            run_blocking_read(&part.log, &BlockingRead {
                fetch_offset,
                limit_offset,
                effective_lso,
                read_max,
                read_committed_aborts,
                sendfile_capable,
                sendfile_min_bytes,
            })
            .await?
        }
    };

    Ok(finish_read(
        out,
        &w,
        log_start,
        read_committed,
        is_follower_fetch,
        aborted_txns,
        records,
    ))
}

/// What a blocking read served: the records, or none for an empty window, and
/// the aborted transactions that overlap them.
#[derive(Debug, PartialEq)]
struct ServedRecords {
    records: Option<RecordsPayload>,
    aborted: Vec<AbortedTransaction>,
}

/// The records a blocking read copied or described, and the aborted
/// transactions that overlap them.
#[derive(Debug, PartialEq)]
struct RawServed {
    records: RecordsPayload,
    aborted: Vec<AbortedTransaction>,
}

/// Everything the blocking half of one partition's read needs, decided by
/// `plan_read` while it held the log mutex and carried across the point where
/// that mutex was released.
struct BlockingRead {
    fetch_offset: Offset,
    limit_offset: Offset,
    effective_lso: Offset,
    read_max: ByteSize,
    read_committed_aborts: bool,
    sendfile_capable: bool,
    sendfile_min_bytes: usize,
}

/// Run the blocking seek-and-read away from normal async polling.
///
/// A fetch reads its partitions one after another, so whatever this hand-off
/// costs, a consumer subscribed to 200 partitions pays 200 times before a
/// single byte goes out. `bench_fetch_handoff` priced the three ways of making
/// it (microseconds per fetch, one 1 KiB record per partition, fastest of
/// eight runs of the suite's own fastest-of-five):
///
/// | partitions | per-partition `spawn_blocking` | one batched `spawn_blocking` | `block_in_place` |
/// |-----------:|-------------------------------:|-----------------------------:|-----------------:|
/// |          1 |                            5.4 |                          5.5 |              0.6 |
/// |         16 |                           95.0 |                         14.9 |              9.7 |
/// |        200 |                         1303.5 |                        127.8 |            123.3 |
///
/// A task allocation, a queue push, a pool wakeup and a `JoinHandle` await per
/// partition dominate a warm read: dropping them is worth 9x at one partition
/// and 11x at two hundred. Batching the whole pending set into one hand-off
/// buys the same order of magnitude and no more, and never beats keeping the
/// read in place: it is 9x behind at one partition, 1.5x behind at sixteen and
/// 4% behind at two hundred -- while it would additionally cost the
/// per-partition remote-tier and diskless fallbacks, which are async and
/// cannot run inside one blocking closure. So the read side lands where the
/// append side did in [`crate::partition_writer`]: `block_in_place`, called
/// once per partition, which is what the arm above measures. Only a call that
/// still holds a worker's core pays for a hand-off, and after the first read
/// in a fetch the calling thread usually does not hold one -- the replacement
/// took it while that read ran. The 200-partition column prices the whole
/// sequence either way.
///
/// What it costs is a thread. `block_in_place` parks the one it runs on and
/// hands that worker's core, with the tasks queued on it, to a replacement
/// taken from the same blocking pool `spawn_blocking` draws on. So a fetch
/// reading 200 partitions holds a thread for the whole read rather than
/// releasing it between partitions, and the broker's `#[tokio::main]` is
/// multi-threaded, so there is a replacement to take the core.
///
/// That pool is the bound, 512 threads by default. While it is saturated the
/// replacement is queued instead of started, and the handed-off core waits for
/// a thread rather than resuming: a read that would merely have queued under
/// `spawn_blocking` costs a unit of runtime parallelism instead. Three things
/// keep that bounded. There is one core per worker thread and a call can only
/// give away the core it holds, so no more than `worker_threads` are ever in
/// flight. The tasks on a handed-off core stay stealable through the
/// scheduler's remote handles, so they are delayed rather than stranded. And
/// reads do not queue on this path, so the wait ends when any in-flight read
/// finishes rather than behind a backlog of them. Bounding it further is one
/// broker-wide admission decision over blocking work rather than a
/// per-call-site one: appends in [`crate::partition_writer`], WAL fsync and
/// trim, and quorum replica IO all draw on the same pool the same way.
///
/// `block_in_place` is also flatly illegal on a current-thread runtime, where
/// it panics rather than degrading, so the current-thread runtimes that
/// `#[tokio::test]` builds keep the `spawn_blocking` path, and
/// `wasm32-wasip1`, which has no blocking pool, reads inline.
/// [`crate::blocking`] makes that choice.
async fn run_blocking_read(
    log: &Arc<Mutex<Log>>,
    read: &BlockingRead,
) -> Result<ServedRecords, BrokerError> {
    let log = Arc::clone(log);
    let read = BlockingRead { ..*read };
    let served = run_blocking_catching(move || read_records(&log, &read))
        .await
        .map_err(|error| read_task_panicked(&error))?;
    let RawServed { records, aborted } = served?;
    let records = (records.payload_len() > 0).then_some(records);
    Ok(ServedRecords { records, aborted })
}

fn read_task_panicked(cause: &impl std::fmt::Display) -> BrokerError {
    BrokerError::Io(std::io::Error::other(format!(
        "fetch read task panicked: {cause}"
    )))
}

/// The blocking body of one partition's read: take the log mutex, seek, and
/// either describe or copy the records run out of it.
fn read_records(log: &Mutex<Log>, read: &BlockingRead) -> Result<RawServed, BrokerError> {
    let &BlockingRead {
        fetch_offset,
        limit_offset,
        effective_lso,
        read_max,
        read_committed_aborts,
        sendfile_capable,
        sendfile_min_bytes,
    } = read;
    let log = log.lock().expect("log mutex poisoned");

    // Zero-copy (Increments D + E): on a plaintext connection
    // (SENDFILE alias: Linux + Apple + FreeBSD/DragonFly), describe
    // the records run with a cheap header-only walk (`read_raw_desc`)
    // instead of `pread`ing the payload. If the run is large enough
    // to amortize the sendfile syscall, return file-backed regions
    // for the `sendfile` drain; otherwise fall back to the byte-copy
    // `read_raw` path (small/fragmented fetches stay on the vectored
    // path). The descriptor is captured here under the log lock so
    // retention can't truncate the region out from under the later
    // async send (the `Arc<File>` pins the inode).
    // Set only on the raw-byte path below, which is the only path
    // `read_committed_aborts` ever takes (the sendfile path is skipped
    // whenever it is set — see the comment there). It is the offset just
    // past the last batch this call actually served, which bounds the
    // aborted-transaction scan below to what the response carries.
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    let mut served_upper_bound: Option<Offset> = None;
    // Every read on the targets without sendfile takes the raw-byte path,
    // which assigns this exactly once.
    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    )))]
    let served_upper_bound: Option<Offset>;
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    let records: RecordsPayload = {
        let mut chosen: Option<RecordsPayload> = None;
        // READ_COMMITTED responses also carry aborted-transaction
        // metadata and are consumed by ordinary Kafka clients as one
        // framed response. Keep those on the raw-byte encoder; the
        // file-region writer can otherwise detach the records payload
        // from the metadata frame and leave a fresh stable-topic reader
        // with HW/LSO but no decoded batches.
        if sendfile_capable && !read_committed_aborts {
            let desc = log.read_raw_desc(fetch_offset, limit_offset, read_max)?;
            if should_use_sendfile(desc.total, !desc.regions.is_empty(), sendfile_min_bytes) {
                chosen = Some(RecordsPayload::FileRegions(desc.regions));
            }
        }
        if let Some(p) = chosen {
            p
        } else {
            let raw = log.read_raw(fetch_offset, limit_offset, read_max)?;
            served_upper_bound = raw.last_offset.map(|o| o + 1);
            RecordsPayload::Raw(raw.bytes)
        }
    };
    // Windows fallback: no safe `sendfile`/`TransmitFile`, so always
    // `read_raw` + copy (the Increment C vectored path drains it).
    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    )))]
    let records: RecordsPayload = {
        // Both sendfile inputs are consumed only by the platform
        // branch above, so discard them here or `-D warnings` fails
        // the build on this target.
        let _ = (sendfile_capable, sendfile_min_bytes);
        let raw = log.read_raw(fetch_offset, limit_offset, read_max)?;
        served_upper_bound = raw.last_offset.map(|o| o + 1);
        RecordsPayload::Raw(raw.bytes)
    };

    // read_committed does NO server-side batch filtering: verbatim
    // bytes (including aborted/control batches) are returned and the
    // consumer drops them client-side via `aborted_transactions`,
    // matching Apache Kafka's behavior. Skip the Vec allocation
    // entirely when there are no aborted txns in range.
    //
    // The scan is bounded by the offset just past the bytes this call
    // actually served, not by `effective_lso`: `partition_max_bytes` (or
    // KFC-1's delivery watermark) can clip the read well short of the LSO,
    // and Kafka's `LocalLog.addAbortedTransactions` bounds the same scan by
    // `fetchUpperBoundOffset`, the position the read stopped at, for
    // exactly that reason. A read that served nothing scans nothing.
    let aborted = if read_committed_aborts {
        let upper_bound =
            served_upper_bound.map_or(fetch_offset, |served| served.min(effective_lso));
        let mut it = log.aborted_in_range(fetch_offset, upper_bound).into_iter();
        if let Some(first) = it.next() {
            let mut v = vec![AbortedTransaction {
                // Unwrap the log-layer `ProducerId` into the wire `i64` field.
                producer_id: first.producer_id.get(),
                // Unwrap the log-layer `Offset` into the wire `i64` field.
                first_offset: first.start_offset.0,
                ..Default::default()
            }];
            v.extend(it.map(|e| AbortedTransaction {
                producer_id: e.producer_id.get(),
                first_offset: e.start_offset.0,
                ..Default::default()
            }));
            v
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    Ok::<_, BrokerError>(RawServed { records, aborted })
}

fn finish_read(
    response: &mut PartitionData,
    window: &VisibilityWindow,
    log_start: Offset,
    read_committed: bool,
    follower_fetch: bool,
    aborted: Vec<AbortedTransaction>,
    records: Option<RecordsPayload>,
) -> usize {
    response.error_code = codes::NONE;
    response.high_watermark = window.response_hw.0;
    response.log_start_offset = log_start.0;
    response.last_stable_offset = window.response_lso.0;
    if read_committed && !follower_fetch {
        response.aborted_transactions = Some(aborted);
    }
    let bytes = records.as_ref().map_or(0, RecordsPayload::payload_len);
    response.records = records;
    bytes
}

/// The bounds a partition row reports beside records a cold tier served.
///
/// A local `OFFSET_OUT_OF_RANGE` row carries Kafka's all -1 offsets (see
/// `plan_read`), so a read that goes on to serve the offset from another tier
/// has to put the live bounds back. They are the values the local read path
/// reports: the global log start, the high watermark, and the last stable
/// offset capped at it. Kafka's `ReplicaManager.handleOffsetOutOfRangeError`
/// reads the same three off the `UnifiedLog` for the remote band.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LiveOffsets {
    pub(crate) log_start: Offset,
    pub(crate) high_watermark: Offset,
    pub(crate) last_stable_offset: Offset,
}

impl LiveOffsets {
    /// Reads the bounds off `part` now.
    pub(crate) async fn of(part: &Partition) -> Self {
        let high_watermark = part.high_watermark().await;
        let mut log = part.log.lock().expect("log mutex poisoned");
        Self {
            log_start: log.log_start_offset(),
            high_watermark,
            last_stable_offset: log.last_stable_offset(high_watermark),
        }
    }

    /// Writes all three bounds into `response`.
    pub(crate) fn report(self, response: &mut PartitionData) {
        response.log_start_offset = self.log_start.0;
        response.high_watermark = self.high_watermark.0;
        response.last_stable_offset = self.last_stable_offset.0;
    }
}

/// KFC-1's delivery watermark for this fetch, capped at the high watermark.
///
/// The value is recomputed here, under the log mutex the fetch already holds,
/// rather than read from the partition's lock-free mirror. The mirror is only
/// as fresh as the last append or the last scheduler tick, and a fetch that
/// caps itself on a stale value either holds back a batch that has come due or,
/// after a truncation, serves one that has not. The delivery scheduler exists
/// for liveness; this call is what makes a fetch correct.
///
/// The clamp keeps the value inside the window it belongs to: the window may
/// never expose beyond the high watermark. The verified kernel takes the same
/// minimum itself and has no precondition, so a value outside that range can
/// only narrow what a consumer reads.
///
/// A follower fetch is never gated, so it does no work here. A scheduled record
/// replicates, and counts toward the ISR and the high watermark, long before
/// any consumer may see it. The kernel ignores the value for a follower, and
/// the high watermark is simply the neutral one.
///
/// A topic that delivers immediately answers the log end offset before it reads
/// a single batch header, so the clamp gives the high watermark straight back
/// and the read path reaches `sendfile` exactly where it does today.
///
/// `now_ms` comes from the partition's own delivery clock rather than from the
/// system clock directly, so an append, the scheduler and a fetch all read one
/// timeline. That is what lets a test drive a manual clock across an activation
/// boundary and assert what a fetch returns on each side of it.
fn deliverable_offset(
    log: &mut Log,
    high_watermark: Offset,
    follower_fetch: bool,
    now_ms: i64,
) -> Offset {
    if follower_fetch {
        return high_watermark;
    }
    log.advance_delivery_watermark(now_ms)
        .watermark
        .min(high_watermark)
}

/// What `plan_read` decided while it held the log mutex: the log start the
/// response reports, the visibility window, and the read to run.
struct PlannedWindow {
    log_start: Offset,
    window: VisibilityWindow,
    plan: ReadPlan,
}

fn plan_read(
    partition: &Partition,
    fetch_offset: Offset,
    high_watermark: Offset,
    read_committed: bool,
    follower_fetch: bool,
    response: &mut PartitionData,
) -> PlannedWindow {
    let mut log = partition.log.lock().expect("log mutex poisoned");
    let log_start = log.log_start_offset();
    // KIP-405 keeps two floors, and this one is the floor of the *local* read:
    // on a tiered partition the offsets in `[log_start, local_log_start)` are
    // in the remote tier, so the window has to call them out of range for the
    // fetch to fall through to `try_remote_read`. The two are equal on every
    // other topic. The response still reports the global `log_start` below,
    // which is the value Kafka puts in `logStartOffset`.
    let local_log_start = log.local_log_start_offset();
    let log_end = log.log_end_offset();
    let deliverable = deliverable_offset(
        &mut log,
        high_watermark,
        follower_fetch,
        partition.delivery.now_ms(),
    );
    let window = compute_visibility_window(
        follower_fetch,
        read_committed,
        FetchWatermarks {
            log_start: local_log_start,
            hw: high_watermark,
            lso: log.last_stable_offset(high_watermark),
            log_end,
            deliverable,
        },
        fetch_offset,
    );
    let plan = if window.out_of_range {
        // Kafka's `Partition.readRecords` throws `OffsetOutOfRangeException`
        // here, and outside the remote band the non-tiered branch of
        // `ReplicaManager.handleOffsetOutOfRangeError` answers it with
        // `LogReadResult(Errors)`: every offset -1 and no records, not the
        // live bounds. A tiered partition's remote band is answered afresh by
        // `try_remote_read`, which writes the live bounds itself on the
        // branches where Kafka reports them.
        *response = refused_read(response.partition_index, codes::OFFSET_OUT_OF_RANGE);
        ReadPlan::OffsetOutOfRange
    } else if window.empty {
        ReadPlan::Empty
    } else {
        ReadPlan::Read {
            limit_offset: window.limit_offset,
            effective_lso: window.effective_lso,
            read_committed_aborts: window.read_committed_aborts,
        }
    };
    PlannedWindow {
        log_start,
        window,
        plan,
    }
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn should_use_sendfile(total_bytes: usize, has_regions: bool, minimum_bytes: usize) -> bool {
    total_bytes >= minimum_bytes && has_regions
}

#[cfg(test)]
mod tests {
    use {
        super::{RawServed, ServedRecords, read_task_panicked},
        crate::{
            blocking::inline_blocking_on_this_thread,
            codes::{
                CORRUPT_MESSAGE, KAFKA_STORAGE_ERROR, NONE, OFFSET_OUT_OF_RANGE,
                UNKNOWN_SERVER_ERROR,
            },
            delivery::test_support::batch_at,
            error::BrokerError,
            handlers::test_support::default_records_batch,
            log_dir_status::LogDirRegistry,
            partition::{Partition, test_support::test_partition},
            time_util::now_ms,
        },
        assert2::assert,
        bytes::Bytes,
        krabka_ids::ProducerId,
        krabka_log::{DeliveryPolicy, Log, LogConfig, LogError, Offset},
        krabka_protocol::{
            owned::fetch_response::{AbortedTransaction, PartitionData},
            records::{Attributes, Record, RecordBatch, RecordsPayload},
        },
        krabka_units::prelude::{ByteSizeExt as _, mebibytes},
        std::sync::{Arc, Mutex},
    };

    /// The read budget for the hand-off test: larger than the log it reads, so
    /// the served bytes are the whole batch under either runtime flavor.
    const UNBOUNDED: krabka_units::ByteSize = mebibytes(1);

    /// The producer that opens and aborts the transaction the
    /// `read_committed` case reads back.
    const PID: i64 = 1000;

    async fn assert_zero_byte_read(
        partition: &Partition,
        request: super::ReadRequest,
        out: &mut PartitionData,
        label: &str,
    ) {
        let bytes = super::do_read(partition, request, &LogDirRegistry::default(), out).await;
        assert!(bytes == 0, "{label}");
    }

    /// A log and the temporary directory that holds its segments.
    struct LogInDir {
        dir: tempfile::TempDir,
        log: Log,
    }

    fn empty_log() -> LogInDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        LogInDir { dir, log }
    }

    fn aborted_transaction_log() -> LogInDir {
        let LogInDir { dir, mut log } = empty_log();
        log.append(&mut transactional_batch(ProducerId(PID)))
            .expect("append the transaction's one data batch");
        log.append(&mut abort_marker(ProducerId(PID)))
            .expect("append the abort marker that closes it");
        LogInDir { dir, log }
    }

    /// A shared log with the raw bytes of its whole range and the end of it.
    struct RawLog {
        log: Mutex<Log>,
        raw: Bytes,
        limit: Offset,
    }

    fn locked_raw_log(log: Log) -> RawLog {
        let limit = log.log_end_offset();
        let raw = log
            .read_raw(Offset(0), limit, UNBOUNDED)
            .expect("the test log holds the range it is asked for")
            .bytes;
        RawLog {
            log: Mutex::new(log),
            raw,
            limit,
        }
    }

    /// One batch of two records in a fresh log directory. The directory is
    /// returned because dropping it deletes the segments underneath the log.
    fn two_record_log() -> LogInDir {
        let LogInDir { dir, mut log } = empty_log();
        log.append(&mut default_records_batch(&[b"first", b"second"]))
            .expect("append the batch under test");
        LogInDir { dir, log }
    }

    /// The plain read the cases below vary: everything from offset 0 to
    /// `limit`, no byte budget in the way, no sendfile and no `read_committed`
    /// bookkeeping.
    fn whole_log_read(limit: Offset) -> super::BlockingRead {
        super::BlockingRead {
            fetch_offset: Offset(0),
            limit_offset: limit,
            effective_lso: limit,
            read_max: UNBOUNDED,
            read_committed_aborts: false,
            sendfile_capable: false,
            sendfile_min_bytes: 0,
        }
    }

    /// [`super::run_blocking_read`] picks its hand-off from the runtime
    /// flavor: `block_in_place` on the multi-threaded runtime the broker runs
    /// on, `spawn_blocking` on a current-thread runtime, where
    /// `block_in_place` panics outright rather than degrading, and the calling
    /// thread on `wasm32-wasip1`. Every arm has to serve exactly the bytes a
    /// direct read serves.
    #[test]
    fn every_placement_serves_the_same_bytes() {
        let LogInDir { dir: _dir, log } = two_record_log();
        let limit = log.log_end_offset();
        let log = Arc::new(Mutex::new(log));
        let read = whole_log_read(limit);
        let expected = ServedRecords {
            records: Some(RecordsPayload::Raw(
                log.lock()
                    .expect("log mutex poisoned")
                    .read_raw(Offset(0), limit, UNBOUNDED)
                    .expect("the test log holds the range it is asked for")
                    .bytes,
            )),
            aborted: Vec::new(),
        };

        let served = |runtime: tokio::runtime::Runtime| {
            runtime
                .block_on(super::run_blocking_read(&log, &read))
                .expect("the blocking read succeeds")
        };
        let current_thread = served(
            tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("build a current-thread runtime"),
        );
        let multi_thread = served(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .build()
                .expect("build a multi-threaded runtime"),
        );
        let inline = {
            let _inline = inline_blocking_on_this_thread();
            served(
                tokio::runtime::Builder::new_current_thread()
                    .build()
                    .expect("build a current-thread runtime"),
            )
        };

        assert!(current_thread == expected);
        assert!(multi_thread == expected);
        assert!(inline == expected);
    }

    #[test]
    fn sendfile_eligibility_honors_nondefault_threshold() {
        assert!(super::should_use_sendfile(64, true, 64));
        assert!(!super::should_use_sendfile(63, true, 64));
        assert!(!super::should_use_sendfile(64, false, 64));
    }

    krabka_macros::sendfile_platform! {
    /// On a plaintext connection [`super::read_records`] describes the records
    /// run for the `sendfile` drain instead of `pread`ing it, but only once
    /// the run is long enough to pay for the syscall. Below the threshold it
    /// falls back to the byte copy, and both answers carry the same bytes.
    #[test]
    fn a_sendfile_capable_read_describes_the_run_only_above_the_threshold() {
        let LogInDir { dir: _dir, log } = two_record_log();
        let RawLog { log, raw, limit } = locked_raw_log(log);

        let described = super::read_records(
            &log,
            &super::BlockingRead {
                sendfile_capable: true,
                ..whole_log_read(limit)
            },
        )
        .expect("the blocking read succeeds");
        let copied = super::read_records(
            &log,
            &super::BlockingRead {
                sendfile_capable: true,
                sendfile_min_bytes: raw.len() + 1,
                ..whole_log_read(limit)
            },
        )
        .expect("the blocking read succeeds");

        assert!(let RecordsPayload::FileRegions(_) = &described.records);
        assert!(described.records.payload_len() == raw.len());
        assert!(described.aborted.is_empty());
        assert!(
            copied
                == RawServed {
                    records: RecordsPayload::Raw(raw),
                    aborted: Vec::new(),
                }
        );
    }
    }

    /// A `read_committed` fetch does no server-side filtering: it serves the
    /// verbatim bytes, aborted batches included, and reports the aborted
    /// ranges beside them so the consumer can drop those client-side. The same
    /// read without the flag reports none, and neither answer moves a byte.
    #[test]
    fn read_committed_reports_the_aborted_ranges_beside_the_same_bytes() {
        let LogInDir { dir: _dir, log } = aborted_transaction_log();
        let RawLog { log, raw, limit } = locked_raw_log(log);

        let read_committed = super::read_records(&log, &super::BlockingRead {
            read_committed_aborts: true,
            ..whole_log_read(limit)
        })
        .expect("the blocking read succeeds");
        let read_uncommitted =
            super::read_records(&log, &whole_log_read(limit)).expect("the blocking read succeeds");

        assert!(
            read_committed
                == RawServed {
                    records: RecordsPayload::Raw(raw.clone()),
                    aborted: vec![AbortedTransaction {
                        producer_id: PID,
                        first_offset: 0,
                        ..AbortedTransaction::default()
                    }],
                }
        );
        assert!(
            read_uncommitted
                == RawServed {
                    records: RecordsPayload::Raw(raw),
                    aborted: Vec::new(),
                }
        );
    }

    /// `partition_max_bytes` can clip a read well short of the last stable
    /// offset. The aborted-transaction scan has to stop at the bytes the
    /// response actually carries, not at the full LSO, or a response with one
    /// batch can carry the aborted list of a much longer range (#874).
    #[test]
    fn read_committed_aborted_scan_stops_at_the_bytes_actually_served() {
        const OTHER_PID: i64 = PID + 1;

        let LogInDir { dir: _dir, mut log } = aborted_transaction_log();
        let clip_at = log.log_end_offset();
        log.append(&mut transactional_batch(ProducerId(OTHER_PID)))
            .expect("append the second transaction's data");
        log.append(&mut abort_marker(ProducerId(OTHER_PID)))
            .expect("append the second transaction's abort marker");
        let full_end = log.log_end_offset();

        // The exact byte length of the first transaction's two batches: a
        // budget of this many bytes should serve exactly them, whatever
        // `limit_offset` or `effective_lso` ask for beyond that.
        let clip = log
            .read_raw(Offset(0), clip_at, UNBOUNDED)
            .expect("read the first transaction alone");

        let log = Mutex::new(log);
        let RawServed { records, aborted } = super::read_records(&log, &super::BlockingRead {
            fetch_offset: Offset(0),
            limit_offset: full_end,
            // Spans both transactions; only the byte budget below clips
            // the read to the first one.
            effective_lso: full_end,
            read_max: krabka_units::ByteSize::from_bytes(
                u64::try_from(clip.total).unwrap_or(u64::MAX),
            ),
            read_committed_aborts: true,
            sendfile_capable: false,
            sendfile_min_bytes: 0,
        })
        .expect("the blocking read succeeds");

        assert!(records == RecordsPayload::Raw(clip.bytes));
        assert!(
            aborted
                == vec![AbortedTransaction {
                    producer_id: PID,
                    first_offset: 0,
                    ..AbortedTransaction::default()
                }],
            "the second transaction's abort must not appear: its batches were never served"
        );
    }

    /// One transactional data record from `pid`, the batch that opens the
    /// transaction on this partition. `Log::append` rewrites the offsets.
    fn transactional_batch(pid: ProducerId) -> RecordBatch {
        RecordBatch {
            producer_id: pid.0,
            attributes: Attributes::default().with_transactional(true),
            records: vec![Record {
                offset_delta: 0,
                value: Some(Bytes::from_static(b"in a transaction")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        }
    }

    /// `pid`'s abort control batch: a control record whose 4-byte key is
    /// (version=0: i16, `marker_type`=0: i16) big-endian, with the
    /// coordinator epoch in its value. Appending it writes the transaction's
    /// offset range into the partition's `.txnindex`.
    fn abort_marker(pid: ProducerId) -> RecordBatch {
        let mut key = [0u8; 4];
        key[2..4].copy_from_slice(&0i16.to_be_bytes());
        let mut value = [0u8; 6];
        value[2..6].copy_from_slice(&17i32.to_be_bytes());
        RecordBatch {
            producer_id: pid.0,
            attributes: Attributes::default()
                .with_transactional(true)
                .with_control(true),
            records: vec![Record {
                offset_delta: 0,
                key: Some(Bytes::copy_from_slice(&key)),
                value: Some(Bytes::copy_from_slice(&value)),
                ..Record::default()
            }],
            ..RecordBatch::default()
        }
    }

    /// A consumer's `read_uncommitted` request for everything from
    /// `fetch_offset`, with a byte budget larger than the log.
    fn consumer_request(fetch_offset: i64) -> super::ReadRequest {
        super::ReadRequest {
            topic_id: None,
            hot_tail: None,
            fetch_offset: Offset(fetch_offset),
            max_bytes: 1 << 20,
            read_committed: false,
            is_follower_fetch: false,
            sendfile_capable: false,
            sendfile_min_bytes: 0,
        }
    }

    fn expected_offset_out_of_range() -> PartitionData {
        PartitionData {
            error_code: OFFSET_OUT_OF_RANGE,
            high_watermark: -1,
            last_stable_offset: -1,
            log_start_offset: -1,
            aborted_transactions: None,
            preferred_read_replica: -1,
            records: Some(RecordsPayload::Raw(Bytes::new())),
            ..PartitionData::default()
        }
    }

    /// [`super::do_read`] hands the bounds its plan decided to the blocking
    /// read and fills the response from what came back. The three cases are
    /// the three shapes the plan has: a range to serve, nothing to serve
    /// because the fetch sits at the high watermark, and an offset outside the
    /// log, which reports `OFFSET_OUT_OF_RANGE` and serves nothing.
    #[tokio::test]
    async fn do_read_serves_the_window_its_plan_decided() {
        let (partition, _dir) = test_partition(Arc::new(tokio::sync::Notify::new()));
        let (limit, records) = {
            let mut log = partition.log.lock().expect("log mutex poisoned");
            log.append(&mut default_records_batch(&[b"first", b"second"]))
                .expect("append the batch under test");
            let limit = log.log_end_offset();
            let records = log
                .read_raw(Offset(0), limit, UNBOUNDED)
                .expect("the test log holds the range it is asked for")
                .bytes;
            (limit, records)
        };
        partition.replica_state.lock().await.hw = limit;

        let served = PartitionData {
            error_code: NONE,
            high_watermark: limit.0,
            last_stable_offset: limit.0,
            log_start_offset: 0,
            aborted_transactions: None,
            records: Some(RecordsPayload::Raw(records.clone())),
            ..PartitionData::default()
        };
        let cases = [
            ("the whole log", 0, records.len(), served.clone()),
            ("nothing left to serve", limit.0, 0, PartitionData {
                records: None,
                ..served.clone()
            }),
            (
                "past the log end",
                limit.0 + 1,
                0,
                expected_offset_out_of_range(),
            ),
        ];

        for (name, fetch_offset, expected_bytes, expected) in cases {
            // Match the row initialized by the production planner: reads
            // outside committed consumer isolation leave this list null.
            let mut out = PartitionData {
                aborted_transactions: None,
                ..PartitionData::default()
            };
            let bytes = super::do_read(
                &partition,
                consumer_request(fetch_offset),
                &LogDirRegistry::default(),
                &mut out,
            )
            .await;
            assert!(bytes == expected_bytes, "{name}");
            assert!(out == expected, "{name}");
        }
    }

    #[tokio::test]
    async fn do_read_distinguishes_log_end_from_offsets_above_it_for_every_fetcher() {
        let (partition, _dir) = test_partition(Arc::new(tokio::sync::Notify::new()));
        {
            let mut log = partition.log.lock().expect("log mutex poisoned");
            log.append(&mut default_records_batch(&[b"first", b"second"]))
                .expect("append two records");
        }
        // LEO is 2 while HW and LSO trail it at 1.
        partition.replica_state.lock().await.hw = Offset(1);
        for (follower, committed) in [(false, false), (false, true), (true, false), (true, true)] {
            for (offset, expected) in [
                (2, PartitionData {
                    high_watermark: 1,
                    last_stable_offset: 1,
                    log_start_offset: 0,
                    aborted_transactions: (committed && !follower).then(Vec::new),
                    ..PartitionData::default()
                }),
                (3, expected_offset_out_of_range()),
            ] {
                let request = super::ReadRequest {
                    is_follower_fetch: follower,
                    read_committed: committed,
                    ..consumer_request(offset)
                };
                let mut out = PartitionData {
                    aborted_transactions: None,
                    ..PartitionData::default()
                };
                assert_zero_byte_read(
                    &partition,
                    request,
                    &mut out,
                    &format!("follower={follower}, committed={committed}, offset={offset}"),
                )
                .await;
                assert!(
                    out == expected,
                    "follower={follower}, committed={committed}, offset={offset}"
                );
            }
        }
    }

    macro_rules! before_and_transaction {
        ($log:ident, $partition:ident, $transaction_label:literal) => {
            let mut $log = $partition.log.lock().expect("log mutex poisoned");
            $log.append(&mut crate::handlers::test_support::default_records_batch(
                &[b"before"],
            ))
            .expect("append the record before the transaction"); // offset 0
            $log.append(&mut transactional_batch(ProducerId(PID)))
                .expect($transaction_label); // offset 1
        };
    }

    /// A `read_committed` consumer sees nothing of a committed transaction
    /// until the high watermark passes its commit marker. Kafka keeps the
    /// transaction in `unreplicatedTxns` until then, so the last stable offset
    /// stays at the transaction's first offset: a leader change can still
    /// truncate the marker away and decide the outcome again.
    #[tokio::test]
    async fn read_committed_waits_for_the_high_watermark_to_pass_the_commit_marker() {
        let (partition, _dir) = test_partition(Arc::new(tokio::sync::Notify::new()));
        let transaction = {
            before_and_transaction!(log, partition, "append the transaction's data");
            let mut commit = abort_marker(ProducerId(PID));
            commit.records[0].key = Some(Bytes::from_static(&[0, 0, 0, 1]));
            log.append(&mut commit).expect("append the commit marker"); // offset 2
            log.read_raw(Offset(1), Offset(3), UNBOUNDED)
                .expect("the test log holds the transaction")
                .bytes
        };
        let held = |high_watermark: i64| PartitionData {
            error_code: NONE,
            high_watermark,
            last_stable_offset: 1,
            log_start_offset: 0,
            aborted_transactions: Some(Vec::new()),
            records: None,
            ..PartitionData::default()
        };
        let cases = [
            ("high watermark inside the transaction", 1, held(1)),
            ("high watermark at the marker", 2, held(2)),
            ("high watermark past the marker", 3, PartitionData {
                last_stable_offset: 3,
                records: Some(RecordsPayload::Raw(transaction)),
                ..held(3)
            }),
        ];

        for (name, high_watermark, expected) in cases {
            partition.replica_state.lock().await.hw = Offset(high_watermark);
            let mut out = PartitionData::default();
            super::do_read(
                &partition,
                super::ReadRequest {
                    read_committed: true,
                    ..consumer_request(1)
                },
                &LogDirRegistry::default(),
                &mut out,
            )
            .await;
            assert!(out == expected, "{name}");
        }
    }

    /// A fetch below the log start reports an error and serves nothing. A
    /// consumer resuming from an
    /// offset retention has already deleted lands. Kafka answers it with
    /// `LogReadResult(Errors)` on a partition with no remote tier, so every
    /// offset in the row is -1 and the records are empty, whether the reader
    /// is a consumer or a follower: the consumer resets through
    /// `auto.offset.reset` and a follower through `ListOffsets`, and neither
    /// reads a bound out of this row. The whole row is replaced, so a stale
    /// value the caller left in it cannot leak out either.
    #[tokio::test]
    async fn do_read_reports_a_fetch_below_the_log_start_out_of_range() {
        let RetainedPartition {
            partition,
            dir: _dir,
            limit,
        } = retained_partition();
        partition.replica_state.lock().await.hw = limit;

        let refused = PartitionData {
            partition_index: 3,
            ..super::refused_read(3, OFFSET_OUT_OF_RANGE)
        };
        let cases = [
            ("read_uncommitted consumer", consumer_request(0)),
            ("read_committed consumer", super::ReadRequest {
                read_committed: true,
                ..consumer_request(0)
            }),
            ("follower", super::ReadRequest {
                is_follower_fetch: true,
                ..consumer_request(0)
            }),
        ];
        for (name, request) in cases {
            let mut out = PartitionData {
                partition_index: 3,
                high_watermark: 77,
                last_stable_offset: 77,
                log_start_offset: 7,
                aborted_transactions: Some(Vec::new()),
                ..PartitionData::default()
            };
            assert_zero_byte_read(&partition, request, &mut out, name).await;
            assert!(out == refused, "{name}");
        }
        assert!(
            refused
                == PartitionData {
                    partition_index: 3,
                    error_code: crate::codes::OFFSET_OUT_OF_RANGE,
                    high_watermark: -1,
                    last_stable_offset: -1,
                    log_start_offset: -1,
                    preferred_read_replica: -1,
                    aborted_transactions: None,
                    records: Some(RecordsPayload::Raw(Bytes::new())),
                    ..PartitionData::default()
                },
            "the refused row is Kafka's LogReadResult(Errors)"
        );
    }

    /// [`super::LiveOffsets`] reports what a successful local read reports for
    /// the same partition: a cold tier that serves an offset the local log
    /// refused must put back exactly the bounds the local path would have
    /// written, including the last stable offset held at an open
    /// transaction.
    #[tokio::test]
    async fn live_offsets_report_the_bounds_a_local_read_reports() {
        let (partition, _dir) = test_partition(Arc::new(tokio::sync::Notify::new()));
        {
            before_and_transaction!(log, partition, "append the open transaction");
        }
        partition.replica_state.lock().await.hw = Offset(2);

        let mut local = PartitionData::default();
        super::do_read(
            &partition,
            consumer_request(0),
            &LogDirRegistry::default(),
            &mut local,
        )
        .await;
        let mut reported = PartitionData::default();
        super::LiveOffsets::of(&partition)
            .await
            .report(&mut reported);

        assert!(
            super::LiveOffsets::of(&partition).await
                == super::LiveOffsets {
                    log_start: Offset(0),
                    high_watermark: Offset(2),
                    last_stable_offset: Offset(1),
                }
        );
        assert!(
            (
                reported.log_start_offset,
                reported.high_watermark,
                reported.last_stable_offset
            ) == (
                local.log_start_offset,
                local.high_watermark,
                local.last_stable_offset
            )
        );
    }

    /// A log holding one batch per entry of `activations`, two records each.
    fn scheduled_log(policy: DeliveryPolicy, activations: &[i64]) -> LogInDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = LogConfig {
            delivery_policy: policy,
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).expect("open log");
        for activation_ms in activations {
            log.append(&mut batch_at(*activation_ms))
                .expect("append a batch");
        }
        LogInDir { dir, log }
    }

    /// A read that throws answers its own partition's row, the one Kafka's
    /// `ReplicaManager.readFromLog.read` gives that exception, and an I/O
    /// failure takes the partition's log dir offline so the next fetch is
    /// refused at the gate instead of failing the same read again. Only the
    /// `IOException` flips the dir: retention racing a read and a panic in
    /// the read task are not a disk fault.
    #[tokio::test]
    async fn a_failed_read_answers_the_row_kafka_gives_its_error() {
        struct Case {
            name: &'static str,
            error: BrokerError,
            error_code: i16,
            dir_offline: bool,
        }
        let cases = [
            Case {
                name: "an I/O failure",
                error: BrokerError::Log(LogError::Io(std::io::Error::other("EIO"))),
                error_code: KAFKA_STORAGE_ERROR,
                dir_offline: true,
            },
            Case {
                name: "retention moved the log start past the fetch offset",
                error: BrokerError::Log(LogError::OffsetTooLow {
                    requested: Offset(0),
                    log_start: Offset(5),
                }),
                error_code: OFFSET_OUT_OF_RANGE,
                dir_offline: false,
            },
            Case {
                name: "tiering moved the local log start past the fetch offset",
                error: BrokerError::Log(LogError::OffsetBelowLocalStart {
                    requested: Offset(0),
                    local_log_start: Offset(5),
                }),
                error_code: OFFSET_OUT_OF_RANGE,
                dir_offline: false,
            },
            Case {
                name: "a corrupt batch",
                error: BrokerError::Log(LogError::Corrupt("record batch header".into())),
                error_code: CORRUPT_MESSAGE,
                dir_offline: false,
            },
            Case {
                name: "a panic in the read task",
                error: read_task_panicked(&"boom"),
                error_code: UNKNOWN_SERVER_ERROR,
                dir_offline: false,
            },
        ];
        for case in cases {
            let (partition, dir) = test_partition(Arc::new(tokio::sync::Notify::new()));
            let log_dirs = LogDirRegistry::probe(&[dir.path().to_path_buf()]);

            let row = super::settle_failed_read(&partition, 4, &case.error, &log_dirs);

            assert!(
                row == super::refused_read(4, case.error_code),
                "{}",
                case.name
            );
            assert!(
                log_dirs.is_offline(dir.path()) == case.dir_offline,
                "{}",
                case.name
            );
        }
    }

    /// Retention or `DeleteRecords` can move the log start between `plan_read`
    /// and the second log lock. The read then fails with `OffsetTooLow`, and
    /// that is `OFFSET_OUT_OF_RANGE` for the one row, with the dir still
    /// online, and not an error that fails the whole fetch.
    #[tokio::test]
    async fn a_read_that_loses_the_race_with_retention_is_out_of_range() {
        let RetainedPartition {
            partition,
            dir,
            limit,
        } = retained_partition();
        let log_dirs = LogDirRegistry::probe(&[dir.path().to_path_buf()]);

        let error = super::read_records(&partition.log, &whole_log_read(limit))
            .expect_err("the fetch offset is below the log start");
        let row = super::settle_failed_read(&partition, 0, &error, &log_dirs);

        assert!(row == super::refused_read(0, crate::codes::OFFSET_OUT_OF_RANGE));
        assert!(!log_dirs.is_offline(dir.path()));
    }

    #[test]
    fn a_consumer_is_capped_at_the_delivery_watermark_and_a_follower_is_not() {
        let now_ms = now_ms();
        // Two batches of two records: [0, 1] is due, [2, 3] is an hour out.
        let LogInDir { dir: _dir, mut log } = scheduled_log(DeliveryPolicy::Scheduled, &[
            now_ms - 3_600_000,
            now_ms + 3_600_000,
        ]);

        assert!(super::deliverable_offset(&mut log, Offset(4), false, now_ms) == Offset(2));
        // Replication is not gated: the follower reads the whole log.
        assert!(super::deliverable_offset(&mut log, Offset(4), true, now_ms) == Offset(4));
    }

    #[test]
    fn an_immediate_topic_is_capped_at_the_high_watermark_alone() {
        let now_ms = now_ms();
        let LogInDir { dir: _dir, mut log } = scheduled_log(DeliveryPolicy::Immediate, &[
            now_ms + 3_600_000,
            now_ms + 7_200_000,
        ]);

        // Nothing is held back by time, and the high watermark still caps the
        // window below the log end.
        assert!(super::deliverable_offset(&mut log, Offset(3), false, now_ms) == Offset(3));
        assert!(super::deliverable_offset(&mut log, Offset(4), false, now_ms) == Offset(4));
    }
    /// A partition whose whole log retention trimmed away, with its directory and
    /// the offset it trimmed to.
    struct RetainedPartition {
        partition: Partition,
        dir: tempfile::TempDir,
        limit: krabka_log::Offset,
    }

    fn retained_partition() -> RetainedPartition {
        let (partition, dir) = test_partition(Arc::new(tokio::sync::Notify::new()));
        let limit = {
            let mut log = partition.log.lock().expect("log mutex poisoned");
            log.append(&mut RecordBatch {
                records: vec![Record {
                    offset_delta: 0,
                    value: Some(Bytes::from_static(b"deleted by retention")),
                    ..Record::default()
                }],
                ..RecordBatch::default()
            })
            .expect("append the batch retention then deletes");
            let limit = log.log_end_offset();
            log.trim_to_offset(limit)
                .expect("trim the whole log away from under the fetch");
            limit
        };
        RetainedPartition {
            partition,
            dir,
            limit,
        }
    }
}
