//! KIP-405 hand-off to the remote tier for an offset the local log no
//! longer holds, and the KFC-1 activation check that keeps a batch the
//! tier returns from going out before it is due.

use krabka_log::{DeliveryPolicy, LeaderEpoch, Offset};
use krabka_protocol::{
    Encode,
    owned::fetch_response::{AbortedTransaction, PartitionData},
    primitives::uuid::Uuid as WireUuid,
    records::RecordBatch,
};
use krabka_units::convert::TimeExt as _;

use super::{
    plan::{PendingRead, refused_read},
    read::LiveOffsets,
};
use crate::{
    broker::Broker,
    codes,
    metrics::{BrokerMetrics, RemoteTierPath},
    partition::Partition,
};

/// Whether a batch the remote tier returned may go out to a consumer now.
///
/// The local log no longer holds this batch, so the partition's delivery
/// watermark says nothing about it: that watermark is derived from the records
/// the log still has, and it is clamped to at or above the log start. The
/// evidence that survived the copy is the batch's own `max_timestamp`, which is
/// the activation time KFC-1 defines, and this applies the log's own rule to
/// it. A batch is active once its activation time plus the declared clock bound
/// is at or before the broker's clock reading, so delivery is never early.
///
/// A topic that delivers immediately answers `true` without reading the
/// timestamp.
fn remote_batch_is_deliverable(
    policy: DeliveryPolicy,
    uncertainty_ms: i64,
    max_timestamp: i64,
    now_ms: i64,
) -> bool {
    krabka_log::batch_is_deliverable(policy, uncertainty_ms, max_timestamp, now_ms)
}

/// Records how long a cold-tier read held its reader slot, on whichever of
/// `try_remote_read`'s many exits the read leaves by.
///
/// A guard rather than a call at the end because the read returns early on a
/// held-back batch, on an aborted-transaction failure and on every remote
/// error, and a slot the reader held is a slot the pool could not hand out
/// however the read ended.
struct ReadTimer<'metrics> {
    metrics: &'metrics BrokerMetrics,
    started: std::time::Instant,
}

impl<'metrics> ReadTimer<'metrics> {
    fn started(metrics: &'metrics BrokerMetrics) -> Self {
        Self {
            metrics,
            started: std::time::Instant::now(),
        }
    }
}

impl Drop for ReadTimer<'_> {
    fn drop(&mut self) {
        self.metrics
            .observe_remote_reader_fetch(self.started.elapsed());
    }
}

/// Answers one partition of a `Fetch` the cold tier could not serve because
/// the tier itself failed, rather than because the offset does not exist.
///
/// Kafka's `ReplicaManager.processRemoteFetches` turns whatever the remote
/// read threw into a `LogReadResult` carrying that exception, and
/// `Errors.forException` has no mapping for a `RemoteStorageException` or for
/// the executor's `RejectedExecutionException`, so the partition goes out as
/// `UNKNOWN_SERVER_ERROR`. That matters: a client retries
/// `UNKNOWN_SERVER_ERROR` and keeps its position, where
/// `OFFSET_OUT_OF_RANGE` sends it to `auto.offset.reset` -- which for `latest`
/// silently skips the history it asked for and for `none` kills the consumer.
/// A transient brownout of the object store, or an RLMM still catching up
/// after a restart, must not cost a consumer its offset.
///
/// The row is Kafka's `LogReadResult(Errors)`, which is what
/// `DelayedRemoteFetch.onComplete` builds for a remote read that ended in an
/// exception: every offset -1, empty records and no aborted-transaction list,
/// exactly the local path's `refused_read`. The rest of the request is
/// unaffected.
///
/// Returns zero served bytes, because the partition carries an error and no
/// records.
fn fail_partition(p: &mut PendingRead) -> usize {
    p.out = refused_read(p.partition_index, codes::UNKNOWN_SERVER_ERROR);
    0
}

/// Answers one partition whose cold read the remote tier failed, and counts
/// the failure where KIP-405 counts it.
///
/// Every failure the tier raises is answered the same way, by
/// [`fail_partition`]: none of them is evidence that the offset does not
/// exist. `NotReady` is the metadata partition still catching up after a
/// restart, and it is the one failure that is not
/// `RemoteFetchErrorsPerSec` -- it is expected churn during catch-up, so it
/// logs at debug and `failed_fetch_requests` is what counts it.
fn fail_remote_read(
    metrics: &BrokerMetrics,
    p: &mut PendingRead,
    error: &krabka_remote_storage::RemoteStorageError,
) -> usize {
    if let krabka_remote_storage::RemoteStorageError::NotReady { partition } = error {
        tracing::debug!(
            topic = %p.topic_name,
            partition = p.partition_index,
            offset = p.fetch_offset,
            metadata_partition = partition,
            "remote-reader: metadata partition not yet caught up; \
             failing the partition for client retry"
        );
    } else {
        metrics.record_remote_error(RemoteTierPath::Fetch, &p.topic_name);
        tracing::warn!(
            topic = %p.topic_name,
            partition = p.partition_index,
            offset = p.fetch_offset,
            error = %error,
            "remote-reader: the cold-tier read failed; failing the partition"
        );
    }
    fail_partition(p)
}

/// Answers one partition of a `Fetch` that the bounded reader pool refused.
///
/// The refusal is a server-side failure like any other remote-read failure
/// (see [`fail_partition`]) -- which is the whole point of the cap: the local
/// paths keep their resources instead of queueing behind the cold tier.
fn reject_saturated(p: &mut PendingRead) -> usize {
    tracing::warn!(
        topic = %p.topic_name,
        partition = p.partition_index,
        offset = p.fetch_offset,
        "remote-reader: reader pool saturated; refusing the cold-tier read"
    );
    fail_partition(p)
}

/// Answers a follower that fetched into the band the leader keeps only in the
/// remote tier.
///
/// KIP-405 routes a follower and a consumer differently over the same offsets.
/// A consumer is served out of the archive, one batch per fetch. A follower is
/// not: serving it would copy every archived byte back down the replication
/// path onto the new replica's disk, at replication rate, through the leader's
/// bounded reader pool -- which fills the disk tiering was meant to spare,
/// starves the consumer cold reads sharing that pool, and leaves every replica
/// holding the whole archive anyway. So the leader answers
/// `OFFSET_MOVED_TO_TIERED_STORAGE` (109) and the follower restarts its log at
/// the leader's local log start, which is what Kafka's
/// `ReplicaManager.handleOffsetOutOfRangeError` does for
/// `params.isFromFollower`.
///
/// The row is Kafka's `ReplicaManager.createLogReadResult(highWatermark,
/// leaderLogStartOffset, leaderLogEndOffset, e)`: the live high watermark and
/// global log start, which tell the follower which band it asked into, and
/// otherwise the refused row -- empty records, no aborted-transaction list,
/// and a last stable offset of -1, because that constructor passes
/// `OptionalLong.empty()` and `KafkaApis` sends an absent LSO as -1.
///
/// Returns zero served bytes: the partition carries an error and no records.
fn moved_to_tiered_storage(p: &mut PendingRead, live: LiveOffsets, local_log_start: i64) -> usize {
    tracing::debug!(
        topic = %p.topic_name,
        partition = p.partition_index,
        offset = p.fetch_offset,
        local_log_start,
        "remote-reader: follower fetched below the local log start; \
         answering OFFSET_MOVED_TO_TIERED_STORAGE"
    );
    p.out = PartitionData {
        high_watermark: live.high_watermark.0,
        log_start_offset: live.log_start.0,
        ..refused_read(p.partition_index, codes::OFFSET_MOVED_TO_TIERED_STORAGE)
    };
    0
}

/// KIP-405: try to serve `p`'s requested offset from the remote tier when the
/// local log returned `OFFSET_OUT_OF_RANGE` and the topic has
/// `remote.storage.enable=true`.
///
/// On success the function replaces the partition's error and records,
/// reports the live bounds beside them (see [`LiveOffsets`]), and returns the
/// encoded batch size. On a genuine miss -- an offset below the established
/// log start, or one no remote segment holds -- and for a non-tiered topic, it
/// leaves `p.out` untouched and returns `None`, so the `OFFSET_OUT_OF_RANGE`
/// row the local read set, with Kafka's all -1 offsets, stands. That is
/// Kafka's answer to both: the non-tiered branch of
/// `ReplicaManager.handleOffsetOutOfRangeError` for the first, and
/// `DelayedRemoteFetch.onComplete` turning `RemoteLogManager.read`'s
/// `OffsetOutOfRangeException` into `LogReadResult(Errors)` for the second.
/// When the tier itself fails, the partition is answered
/// `UNKNOWN_SERVER_ERROR` instead: see [`fail_partition`].
///
/// A *follower* never reads the tier at all: it is answered
/// `OFFSET_MOVED_TO_TIERED_STORAGE` and rebuilds its log at the leader's local
/// log start. See [`moved_to_tiered_storage`].
///
/// A consumer read of a scheduled topic is capped here as well. The remote path
/// serves whole batches with no offset limit, and it is the one read path the
/// local delivery watermark cannot bound, so it checks the batch's own
/// activation time instead. See [`remote_batch_is_deliverable`].
///
/// The whole cold read -- the batch and, for a read-committed fetch, the
/// remote tail's aborted-transaction indexes -- runs under one permit from the
/// reader's bounded pool, exactly as Kafka runs one `RemoteLogReader` task per
/// remote fetch. A read that arrives with the pool's pending queue already
/// full is refused: see [`reject_saturated`].
pub(super) async fn try_remote_read(
    broker: &Broker,
    p: &mut PendingRead,
    part: &Partition,
) -> Option<usize> {
    let reader = broker.remote_reader.clone()?;
    let (
        remote_storage_enable,
        delivery_policy,
        delivery_uncertainty_ms,
        log_start,
        local_log_start,
    ) = {
        let log = part.log.lock().expect("log mutex poisoned");
        let config = log.config_snapshot();
        (
            config.remote_storage_enable,
            config.delivery_policy,
            config.delivery_clock_uncertainty.millis_i64_trunc(),
            log.established_log_start(),
            log.local_log_start_offset(),
        )
    };
    if !remote_storage_enable {
        return None;
    }
    // KIP-405's `RemoteFetchRequestsPerSec`. Counted once this partition is
    // known to be tiered, so a cluster with no tiered topic materialises no
    // series, and before the reader pool can refuse the read, so a refusal
    // still shows up as an attempt.
    broker
        .metrics
        .record_remote_request(RemoteTierPath::Fetch, &p.topic_name);
    // KIP-405: the remote tier serves `[log_start, local_log_start)` and
    // nothing below. An offset under the global floor was deleted by
    // `DeleteRecords` or by remote retention, so it stays
    // `OFFSET_OUT_OF_RANGE` even while the RLMM still lists the segment that
    // held it. Kafka's `ReplicaManager.handleOffsetOutOfRangeError` builds a
    // `DelayedRemoteFetch` under the same condition.
    //
    // Only a floor this process moved may refuse a read. A floor inferred at
    // `Log::open` from the segments left on disk sits above everything the
    // archive holds on a partition whose local segments were evicted, and
    // refusing against it would hide the whole tier after every restart. See
    // `Log::established_log_start`.
    if matches!(log_start, Some(floor) if p.fetch_offset < floor.0) {
        return None;
    }
    // Kafka reads the bounds it reports for the remote band here, once the
    // offset is known to lie above the global floor.
    let live = LiveOffsets::of(part).await;
    // KIP-405: a follower in the remote band is redirected, never served.
    // Checked after the floor above, because an offset below the global floor
    // is gone from every tier and stays `OFFSET_OUT_OF_RANGE` for a follower
    // exactly as it does for a consumer -- Kafka guards the same way, with
    // `logStartOffset <= offset` before `offset < localLogStartOffset`.
    if p.is_follower_fetch && p.fetch_offset < local_log_start.0 {
        return Some(moved_to_tiered_storage(p, live, local_log_start.0));
    }
    if p.topic_id == WireUuid::ZERO {
        // Without a topic_id we can't build `TopicIdPartition` keyed the
        // same way the RLMM stores entries (Kafka's equality is by id +
        // partition).
        return None;
    }
    let topic_id = uuid::Uuid::from_bytes(p.topic_id.0);
    let tp = krabka_remote_storage::TopicIdPartition::new(
        topic_id,
        p.topic_name.clone(),
        p.partition_index,
    );
    // Atomic stores the raw epoch; wrap into `LeaderEpoch` for the
    // remote-reader / RLMM seam that follows.
    let current_leader_epoch = LeaderEpoch(
        part.current_leader_epoch
            .load(std::sync::atomic::Ordering::Acquire),
    );
    // Resolve the leader epoch that *owned* the requested fetch offset from
    // the local leader-epoch checkpoint (Kafka's `epochForOffset`).  The
    // checkpoint is only appended-to / truncated-from-end (never pruned from
    // the start on local eviction), so tiered offsets that are no longer
    // stored locally still resolve to their copy-time epoch.  Fall back to
    // the current leader epoch when the checkpoint has no entries (empty /
    // fresh log) so behavior is at least as good as before.
    let leader_epoch = {
        let log = part.log.lock().expect("log mutex poisoned");
        log.epoch_checkpoint()
            .epoch_for_offset(Offset(p.fetch_offset))
            .unwrap_or(current_leader_epoch)
    };
    let max_bytes = usize::try_from(p.max_bytes.max(0)).unwrap_or(0);

    // KIP-405's `remote.log.reader.threads` / `.max.pending.tasks` cap. The
    // permit is held for the batch read and the `.txnindex` read below, so one
    // cold fetch occupies one reader slot however many objects it touches.
    let Ok(_permit) = reader.pool.acquire().await else {
        return Some(reject_saturated(p));
    };
    // Kafka's `RemoteLogReaderFetchRateAndTimeMs` measures the reader task,
    // which is exactly the span this permit covers.
    let _read_timer = ReadTimer::started(&broker.metrics);

    match reader
        .fetch_batch(&tp, leader_epoch, p.fetch_offset, max_bytes)
        .await
    {
        Ok(Some(batch)) => {
            // A follower is never gated: it replicates a scheduled record, and
            // counts it toward the ISR, before any consumer may see it.
            if !p.is_follower_fetch
                && !remote_batch_is_deliverable(
                    delivery_policy,
                    delivery_uncertainty_ms,
                    batch.max_timestamp,
                    part.delivery.now_ms(),
                )
            {
                // Answer as the local path answers a batch that is not due: an
                // empty partition and no error. `OFFSET_OUT_OF_RANGE` would
                // send the consumer to its reset policy and lose the record it
                // is waiting for, and the batch is due later, not never.
                tracing::debug!(
                    topic = %p.topic_name,
                    partition = p.partition_index,
                    offset = p.fetch_offset,
                    max_timestamp = batch.max_timestamp,
                    "remote-reader: batch is not due yet; holding it back"
                );
                p.out.error_code = codes::NONE;
                live.report(&mut p.out);
                p.out.records = None;
                if p.read_committed {
                    p.out.aborted_transactions = Some(Vec::new());
                }
                return Some(0);
            }
            let bytes_est = <RecordBatch as Encode>::encoded_len(&batch, 0);

            // KIP-405 read-committed: surface the aborted-transaction list
            // from remote and local `.txnindex` files, including later marker
            // segments, so the consumer drops aborted records client-side.
            // Select against this batch's returned window, then deduplicate
            // wire rows shared by both tiers. `Some(empty)` is the correct
            // read-committed signal (read-uncommitted leaves it `None`).
            if p.read_committed && !p.is_follower_fetch {
                let Some(batch_last_offset) = batch
                    .base_offset
                    .checked_add(i64::from(batch.last_offset_delta))
                else {
                    tracing::warn!(
                        topic = %p.topic_name,
                        partition = p.partition_index,
                        offset = p.fetch_offset,
                        "remote-reader: batch offset overflow; failing the partition"
                    );
                    return Some(fail_partition(p));
                };
                let Some(batch_end) = batch_last_offset.checked_add(1) else {
                    return Some(fail_partition(p));
                };
                let aborts = match reader
                    .aborted_transactions(&tp, leader_epoch, p.fetch_offset, batch_last_offset)
                    .await
                {
                    Ok(aborts) => aborts,
                    Err(e) => return Some(fail_remote_read(&broker.metrics, p, &e)),
                };
                let local = part
                    .log
                    .lock()
                    .expect("log mutex poisoned")
                    .aborted_in_range(Offset(p.fetch_offset), Offset(batch_end));
                let rows: Vec<_> = aborts
                    .into_iter()
                    .map(|entry| (entry.producer_id, entry.start_offset))
                    .chain(
                        local
                            .into_iter()
                            .map(|entry| (entry.producer_id.get(), entry.start_offset.0)),
                    )
                    .collect();
                p.out.aborted_transactions = Some(
                    krabka_verified::transaction::unique_aborted_transaction_rows(&rows)
                        .into_iter()
                        .map(|(producer_id, first_offset)| AbortedTransaction {
                            producer_id,
                            first_offset,
                            ..Default::default()
                        })
                        .collect(),
                );
            }

            // `DelayedRemoteFetch.onComplete`: the remote records beside the
            // bounds the local read result carried for the remote band.
            p.out.error_code = codes::NONE;
            live.report(&mut p.out);
            p.out.records = Some(batch.into());
            // KIP-405's `RemoteFetchBytesPerSec`: what the tier actually
            // served, which is the batch that is about to go out.
            broker.metrics.record_remote_bytes(
                RemoteTierPath::Fetch,
                &p.topic_name,
                u64::try_from(bytes_est).unwrap_or(0),
            );
            Some(bytes_est)
        }
        Ok(None) => None,
        Err(e) => Some(fail_remote_read(&broker.metrics, p, &e)),
    }
}

#[cfg(test)]
mod tests {
    mod aborts;

    use assert2::{assert, check};
    use krabka_log::{DeliveryPolicy, Log, LogConfig};
    use krabka_units::prelude::millis;

    use crate::{
        broker::{Broker, BrokerHandle, spawn_partition},
        codes::OFFSET_OUT_OF_RANGE,
        config::{BrokerConfig, RemoteStorageBackend, RlmmKind},
        delivery::test_support::batch_at,
        metrics::BrokerMetrics,
        partition::Partition,
    };

    /// KIP-405 splits the floors, and only the global one bounds the remote
    /// tier: an offset a `DeleteRecords` deleted is gone from every tier, so
    /// the fetch stays `OFFSET_OUT_OF_RANGE` even while the RLMM still lists
    /// the segment that held it. Kafka's
    /// `ReplicaManager.handleOffsetOutOfRangeError` builds its
    /// `DelayedRemoteFetch` under the same `logStartOffset <= fetchOffset`
    /// condition.
    ///
    /// The two cases run against one tiered partition in one order, so the
    /// second is measured against a tier that has just been shown to answer.
    /// `try_remote_read` reaching `fetch_batch` in the second case would
    /// therefore have served the same batch it served in the first; an
    /// untouched `p.out` is what says it never got there.
    /// The topic every tiered-read case below uses.
    const TIERED_TOPIC: &str = "tiered-delete-records";

    /// KIP-405's `remote.log.reader.max.pending.tasks`: a cold read that
    /// arrives with the reader pool's queue already full is answered with an
    /// error for that partition rather than parked behind the running reads.
    /// Kafka's executor throws `RejectedExecutionException`, which
    /// `Errors.forException` does not map, and `DelayedRemoteFetch` answers
    /// the exception with `LogReadResult(Errors)`: `UNKNOWN_SERVER_ERROR`, every
    /// offset -1, empty records and no aborted-transaction list, whatever the
    /// isolation level.
    #[test]
    fn a_refused_cold_read_answers_the_partition_with_an_error_and_no_records() {
        for read_committed in [false, true] {
            let mut pending = super::PendingRead {
                read_committed,
                ..out_of_range_at_unattached(7)
            };

            let served = super::reject_saturated(&mut pending);

            check!(served == 0);
            check!(pending.out == super::refused_read(0, crate::codes::UNKNOWN_SERVER_ERROR));
        }
    }

    /// The topic id every tiered-read case below uses.
    fn tiered_topic_id() -> uuid::Uuid {
        uuid::Uuid::from_u128(0x7157)
    }

    /// A partition whose sealed segments are all in the archive and none of
    /// them on local disk any more: exactly the shape the remote read path
    /// exists for.
    ///
    /// `reopen` drops the log after the eviction and opens it again from the
    /// same directory, which is what a broker restart does. Nothing durable
    /// carries the global floor across that yet, so the reopened log infers
    /// one from the segments that survived -- and on this partition that
    /// inference sits above the whole archive.
    async fn tiered_partition(
        broker: &std::sync::Arc<Broker>,
        dir: &std::path::Path,
        reopen: bool,
    ) -> std::sync::Arc<Partition> {
        use krabka_ids::{LeaderEpoch, PartitionIndex};

        use crate::remote_log_manager::{ArchiveMode, copy_eligible, test_support::tier};

        /// A batch wide enough that a 256-byte segment rolls every few
        /// appends, so the log seals segments the copy pass can tier.
        fn tiered_batch() -> krabka_protocol::records::RecordBatch {
            let mut batch = krabka_protocol::records::RecordBatch {
                last_offset_delta: 1,
                ..Default::default()
            };
            for offset_delta in 0..2 {
                batch.records.push(krabka_protocol::records::Record {
                    offset_delta,
                    value: Some(bytes::Bytes::from(vec![b'x'; 64])),
                    ..Default::default()
                });
            }
            batch
        }

        let config = LogConfig {
            segment_size: krabka_units::bytes(256),
            remote_storage_enable: true,
            ..LogConfig::default()
        };
        let part_dir = dir.join(format!("{TIERED_TOPIC}-0"));
        std::fs::create_dir_all(&part_dir).expect("partition dir");
        let mut log = Log::open(&part_dir, config.clone()).expect("open partition log");
        for _ in 0..12 {
            let mut batch = tiered_batch();
            log.append(&mut batch).expect("append");
        }
        log.sync().expect("flush sealed segments before archiving");
        let exports = log.tierable_segments();
        assert!(exports.len() >= 2, "the test needs sealed segments to tier");
        let tiered_through = exports.last().expect("sealed segments").last_offset + 1;

        let tp = krabka_remote_storage::TopicIdPartition::new(tiered_topic_id(), TIERED_TOPIC, 0);
        let reader = broker.remote_reader.clone().expect("remote reader");
        let copied = copy_eligible(
            &tier(ArchiveMode::Mutable, &reader.rsm, &reader.rlmm),
            &tp,
            1,
            LeaderEpoch(0),
            exports,
        )
        .await;
        assert!(copied > 0, "the copy pass should have tiered every segment");

        log.delete_local_segments_through(tiered_through)
            .expect("evict the copied segments");
        if reopen {
            drop(log);
            log = Log::open(&part_dir, config).expect("reopen partition log");
            assert!(
                log.log_start_offset() == tiered_through,
                "the reopened log infers its floor from what survived"
            );
        }
        spawn_partition(
            TIERED_TOPIC.to_string(),
            PartitionIndex(0),
            dir.to_path_buf(),
            log,
            broker.log_dir_status.clone(),
            broker.producer_state.clone(),
            false,
        )
    }

    /// A broker with a local archive and an in-memory RLMM, and the
    /// directories both live in.
    async fn tiered_broker() -> (BrokerHandle, tempfile::TempDir, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let remote_dir = tempfile::tempdir().expect("remote tempdir");
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        config.remote_storage_backend = Some(RemoteStorageBackend::Local {
            dir: remote_dir.path().to_path_buf(),
        });
        config.remote_log_metadata = RlmmKind::InMemory;
        let handle = Broker::start(config).await.expect("start broker");
        (handle, dir, remote_dir)
    }

    /// The high watermark `tiered_partition` installs: the end of the 24
    /// records it appends, so every bound the remote path reports is distinct
    /// from the -1 of a refused row.
    const TIERED_HIGH_WATERMARK: i64 = 24;

    /// Installs [`TIERED_HIGH_WATERMARK`] on `part`, as the leader's ISR would
    /// once every replica had caught up.
    async fn commit_everything(part: &Partition) {
        part.replica_state.lock().await.hw = krabka_log::Offset(TIERED_HIGH_WATERMARK);
    }

    /// The row the remote path reports beside records it served: the live
    /// bounds, which on `tiered_partition` are the global log start, and the
    /// high watermark doubling as the last stable offset because the partition
    /// holds no transaction.
    fn served_bounds(log_start: i64) -> (i64, i64, i64) {
        (log_start, TIERED_HIGH_WATERMARK, TIERED_HIGH_WATERMARK)
    }

    /// The three bounds `out` reports, in [`served_bounds`] order.
    fn bounds(out: &krabka_protocol::owned::fetch_response::PartitionData) -> (i64, i64, i64) {
        (
            out.log_start_offset,
            out.high_watermark,
            out.last_stable_offset,
        )
    }

    /// A `PendingRead` for offset 0 that the local log has already answered
    /// `OFFSET_OUT_OF_RANGE`, with the all -1 row `do_read` leaves behind.
    fn out_of_range_at(part: &std::sync::Arc<Partition>, fetch_offset: i64) -> super::PendingRead {
        use krabka_protocol::primitives::uuid::Uuid as WireUuid;

        use crate::codes;

        super::PendingRead {
            topic_name: TIERED_TOPIC.into(),
            topic_id: WireUuid(tiered_topic_id().into_bytes()),
            partition_index: 0,
            current_leader_epoch: 0,
            last_fetched_epoch: -1,
            fetch_offset,
            max_bytes: 1_048_576,
            read_committed: false,
            is_follower_fetch: false,
            fetch_only_leader: false,
            follower_position: None,
            partition: Some(std::sync::Arc::clone(part)),
            out: super::refused_read(0, codes::OFFSET_OUT_OF_RANGE),
            cpu_micros: 0,
        }
    }

    macro_rules! committed_tiered_fixture {
        (($handle:ident, $dir:ident, $remote_dir:ident, $broker:ident, $part:ident)) => {
            let ($handle, $dir, $remote_dir) = tiered_broker().await;
            let $broker = $handle.broker_arc_for_test();
            let $part = tiered_partition(&$broker, $dir.path(), false).await;
            commit_everything(&$part).await;
        };
    }

    #[tokio::test]
    async fn a_fetch_below_the_global_log_start_never_reaches_the_remote_tier() {
        use krabka_log::Offset;

        use crate::codes;

        committed_tiered_fixture!((broker_handle, dir, _remote_dir, broker, part));

        let out_of_range = || out_of_range_at(&part, 0);
        // The floor has not moved, so offset 0 is the remote tier's to serve.
        let mut served = out_of_range();
        let bytes_served = super::try_remote_read(&broker, &mut served, &part).await;
        check!(bytes_served.is_some_and(|n| n > 0), "the tier answers");
        check!(served.out.error_code == codes::NONE);
        check!(bounds(&served.out) == served_bounds(0));

        // `DeleteRecords` to offset 5. The remote segment that holds offset 0
        // is still listed in the RLMM and still in the archive, and it must
        // stop being reachable all the same.
        part.log
            .lock()
            .expect("log mutex poisoned")
            .set_log_start_offset(Offset(5))
            .expect("move the log start");

        let mut refused = out_of_range();
        let bytes_served = super::try_remote_read(&broker, &mut refused, &part).await;
        check!(bytes_served == None, "no tier answers below the floor");
        check!(refused.out == super::refused_read(0, codes::OFFSET_OUT_OF_RANGE));

        // The floor itself still reads: the band above it is the tier's, and
        // the row reports the floor that moved.
        let mut at_floor = out_of_range_at(&part, 5);
        let bytes_served = super::try_remote_read(&broker, &mut at_floor, &part).await;
        check!(bytes_served.is_some_and(|n| n > 0), "the floor is readable");
        check!(bounds(&at_floor.out) == served_bounds(5));

        broker_handle.shutdown().await;
    }

    /// KIP-405 routes a follower and a consumer differently over the same
    /// band of offsets, and this is the case that says so.
    ///
    /// The consumer is served the archived batch, one fetch at a time. The
    /// follower is not served at all: it is answered
    /// `OFFSET_MOVED_TO_TIERED_STORAGE` with no records, so the archive is
    /// never re-materialised on the new replica's disk. Kafka's
    /// `ReplicaManager.handleOffsetOutOfRangeError` splits on
    /// `params.isFromFollower` in exactly the same place.
    ///
    /// The two floors still bound the follower the way they bound a consumer.
    /// An offset a `DeleteRecords` deleted is gone from every tier, so it stays
    /// `OFFSET_OUT_OF_RANGE` rather than sending the follower to a band that no
    /// longer exists; and an offset above the leader's log is not in the tier
    /// either, so it stays `OFFSET_OUT_OF_RANGE` and the follower truncates
    /// instead of restarting.
    ///
    /// The redirect carries Kafka's `createLogReadResult` row: the live high
    /// watermark and log start, a last stable offset of -1, and empty records.
    /// The consumer's served row carries all three live bounds, as
    /// `DelayedRemoteFetch.onComplete` copies them off the local read result.
    #[tokio::test]
    async fn a_follower_below_the_local_log_start_is_redirected_and_a_consumer_is_served() {
        use krabka_log::Offset;
        use krabka_protocol::owned::fetch_response::PartitionData;

        use crate::codes;

        committed_tiered_fixture!((broker_handle, dir, _remote_dir, broker, part));

        let follower_at = |fetch_offset| super::PendingRead {
            is_follower_fetch: true,
            ..out_of_range_at(&part, fetch_offset)
        };

        let mut follower = follower_at(0);
        let served = super::try_remote_read(&broker, &mut follower, &part).await;
        check!(
            served == Some(0),
            "the follower is served no archived bytes"
        );
        check!(
            follower.out
                == PartitionData {
                    high_watermark: TIERED_HIGH_WATERMARK,
                    log_start_offset: 0,
                    ..super::refused_read(0, codes::OFFSET_MOVED_TO_TIERED_STORAGE)
                }
        );

        // A consumer at the very same offset still reads the archive.
        let mut consumer = out_of_range_at(&part, 0);
        let served = super::try_remote_read(&broker, &mut consumer, &part).await;
        check!(
            served.is_some_and(|n| n > 0),
            "the tier answers the consumer"
        );
        check!(consumer.out.error_code == codes::NONE);
        check!(bounds(&consumer.out) == served_bounds(0));
        check!(consumer.out.aborted_transactions == None);
        check!(
            consumer
                .out
                .records
                .as_ref()
                .is_some_and(|records| records.payload_len() > 0)
        );

        // Above the leader's log there is no tiered band to redirect into.
        let mut ahead = follower_at(10_000);
        let served = super::try_remote_read(&broker, &mut ahead, &part).await;
        check!(served == None, "no segment holds offset 10_000");
        check!(ahead.out == super::refused_read(0, codes::OFFSET_OUT_OF_RANGE));

        // Below the global floor the offset is gone from every tier.
        part.log
            .lock()
            .expect("log mutex poisoned")
            .set_log_start_offset(Offset(5))
            .expect("move the log start");
        let mut deleted = follower_at(0);
        let served = super::try_remote_read(&broker, &mut deleted, &part).await;
        check!(served == None, "nothing serves an offset below the floor");
        check!(deleted.out == super::refused_read(0, codes::OFFSET_OUT_OF_RANGE));

        broker_handle.shutdown().await;
    }

    /// A restart must not hide the archive.
    ///
    /// `Log::open` reads a global floor off the segments that survived on
    /// disk, and on a tiered partition whose local segments were evicted that
    /// reading sits above every offset the archive holds. Refusing a remote
    /// read against it would answer `OFFSET_OUT_OF_RANGE` for the whole tier
    /// after every restart, so only a floor this process moved refuses one.
    #[tokio::test]
    async fn a_restart_does_not_put_the_archive_out_of_range() {
        use crate::codes;

        let (broker_handle, dir, _remote_dir) = tiered_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let part = tiered_partition(&broker, dir.path(), true).await;

        let mut served = out_of_range_at(&part, 0);
        let bytes_served = super::try_remote_read(&broker, &mut served, &part).await;
        check!(
            bytes_served.is_some_and(|n| n > 0),
            "the tier still answers offset 0 after a restart"
        );
        check!(served.out.error_code == codes::NONE);

        broker_handle.shutdown().await;
    }

    /// A `PendingRead` with no partition attached, for the cases that never
    /// reach the log.
    fn out_of_range_at_unattached(fetch_offset: i64) -> super::PendingRead {
        super::PendingRead {
            topic_name: TIERED_TOPIC.into(),
            topic_id: krabka_protocol::primitives::uuid::Uuid::ZERO,
            partition_index: 0,
            current_leader_epoch: 0,
            last_fetched_epoch: -1,
            fetch_offset,
            max_bytes: 1024,
            read_committed: false,
            is_follower_fetch: false,
            fetch_only_leader: false,
            follower_position: None,
            partition: None,
            out: krabka_protocol::owned::fetch_response::PartitionData {
                error_code: OFFSET_OUT_OF_RANGE,
                ..Default::default()
            },
            cpu_micros: 0,
        }
    }

    /// A failed cold read is a server-side failure, not a statement that the
    /// offset does not exist. `OFFSET_OUT_OF_RANGE` sends a consumer to its
    /// `auto.offset.reset` -- `latest` silently skips the history it asked
    /// for, `none` kills it -- where `UNKNOWN_SERVER_ERROR` is retried with
    /// the consumer's position intact, which is what Kafka answers when
    /// `Errors.forException` cannot map the `RemoteStorageException` the
    /// remote read threw.
    ///
    /// Every failure the tier can raise is answered the same way, including
    /// the RLMM's `NotReady`: a metadata partition that has not caught up
    /// after a restart knows nothing about the offset either way. The row is
    /// `DelayedRemoteFetch.onComplete`'s `LogReadResult(Errors)` for a remote
    /// read that threw: every offset -1, empty records, and no
    /// aborted-transaction list for either isolation level. Whatever the row
    /// held before is replaced, so a stale value cannot leak out.
    #[test]
    fn every_failed_cold_read_answers_the_partition_with_unknown_server_error() {
        use krabka_remote_storage::RemoteStorageError;

        let metrics = BrokerMetrics::new();
        for error in [
            RemoteStorageError::NotReady { partition: 3 },
            RemoteStorageError::Backend("object store brownout".into()),
            RemoteStorageError::Io(std::io::Error::other("connection reset by peer")),
        ] {
            for read_committed in [false, true] {
                let mut pending = super::PendingRead {
                    read_committed,
                    out: krabka_protocol::owned::fetch_response::PartitionData {
                        partition_index: 0,
                        error_code: OFFSET_OUT_OF_RANGE,
                        high_watermark: 900,
                        last_stable_offset: 900,
                        log_start_offset: 100,
                        records: Some(krabka_protocol::records::RecordsPayload::Raw(
                            bytes::Bytes::from_static(b"stale"),
                        )),
                        ..Default::default()
                    },
                    ..out_of_range_at_unattached(7)
                };

                let served = super::fail_remote_read(&metrics, &mut pending, &error);

                check!(served == 0, "{error}");
                check!(
                    pending.out
                        == krabka_protocol::owned::fetch_response::PartitionData {
                            partition_index: 0,
                            error_code: crate::codes::UNKNOWN_SERVER_ERROR,
                            high_watermark: -1,
                            last_stable_offset: -1,
                            log_start_offset: -1,
                            preferred_read_replica: -1,
                            records: Some(krabka_protocol::records::RecordsPayload::Raw(
                                bytes::Bytes::new(),
                            )),
                            aborted_transactions: None,
                            ..Default::default()
                        },
                    "{error}"
                );
            }
        }
    }

    /// The whole point of the mapping, end to end: an archive the broker
    /// cannot read answers the partition `UNKNOWN_SERVER_ERROR` and no
    /// records, and an offset the archive genuinely does not hold keeps the
    /// `OFFSET_OUT_OF_RANGE` the local read set.
    #[tokio::test]
    async fn a_broken_archive_fails_the_partition_and_a_real_miss_stays_out_of_range() {
        use crate::codes;

        let (broker_handle, dir, remote_dir) = tiered_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let part = tiered_partition(&broker, dir.path(), false).await;

        // An offset above everything the archive holds: the RLMM is caught up
        // and answers, and no segment covers it. That is a genuine miss.
        let mut missing = out_of_range_at(&part, 10_000);
        let bytes_served = super::try_remote_read(&broker, &mut missing, &part).await;
        check!(bytes_served == None, "no segment holds offset 10_000");
        check!(missing.out == super::refused_read(0, codes::OFFSET_OUT_OF_RANGE));

        // Now take the archive away under the reader, leaving the RLMM still
        // listing every segment: exactly an object-store brownout.
        std::fs::remove_dir_all(remote_dir.path()).expect("wipe the archive");

        let mut failed = out_of_range_at(&part, 0);
        let bytes_served = super::try_remote_read(&broker, &mut failed, &part).await;
        check!(bytes_served == Some(0), "a failed read serves no bytes");
        check!(failed.out == super::refused_read(0, codes::UNKNOWN_SERVER_ERROR));

        broker_handle.shutdown().await;
    }

    #[test]
    fn a_remote_batch_is_held_back_until_its_activation_time_plus_the_bound() {
        // Immediate delivery reads no timestamp at all.
        assert!(super::remote_batch_is_deliverable(
            DeliveryPolicy::Immediate,
            250,
            10_000,
            0
        ));
        // Scheduled: due at 10_000, and the 250 ms clock bound is added to it.
        assert!(!super::remote_batch_is_deliverable(
            DeliveryPolicy::Scheduled,
            250,
            10_000,
            10_249
        ));
        assert!(super::remote_batch_is_deliverable(
            DeliveryPolicy::Scheduled,
            250,
            10_000,
            10_250
        ));
        // The bound is added to the activation time, never subtracted from it,
        // so a clock at the far end of its own uncertainty still never delivers
        // early.
        assert!(!super::remote_batch_is_deliverable(
            DeliveryPolicy::Scheduled,
            250,
            10_000,
            10_000
        ));
        // A saturating subtraction keeps the far end of the clock range safe.
        assert!(!super::remote_batch_is_deliverable(
            DeliveryPolicy::Scheduled,
            250,
            10_000,
            i64::MIN
        ));
    }

    #[test]
    fn local_and_remote_paths_agree_for_the_same_batch_and_clock() {
        for (activation_ms, now_ms) in [(10_000, 10_249), (10_000, 10_250), (i64::MAX, i64::MAX)] {
            let dir = tempfile::tempdir().expect("tempdir");
            let config = LogConfig {
                delivery_policy: DeliveryPolicy::Scheduled,
                delivery_clock_uncertainty: millis(250),
                ..LogConfig::default()
            };
            let mut log = Log::open(dir.path(), config).expect("open log");
            let mut batch = batch_at(activation_ms);
            log.append(&mut batch).expect("append scheduled batch");
            let local_visible =
                log.advance_delivery_watermark(now_ms).watermark == log.log_end_offset();
            let remote_visible = super::remote_batch_is_deliverable(
                DeliveryPolicy::Scheduled,
                250,
                activation_ms,
                now_ms,
            );

            assert!(local_visible == remote_visible);
        }
    }
}
