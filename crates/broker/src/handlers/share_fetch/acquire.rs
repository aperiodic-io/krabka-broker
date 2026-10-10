//! The acquire passes that KIP-932 runs over the pending partitions: apply the
//! piggybacked acknowledgements, expire stale locks, materialize newly
//! produced records, and lock a batch of `Available` ones for this member.
//!
//! This is the stage that owns the per-partition
//! [`AcquisitionState`](crate::share_partition::state::AcquisitionState) locks,
//! the request-wide record budget, and the retry pass behind the long poll.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use krabka_log::{LogError, Offset};
use krabka_protocol::{owned::share_fetch_response::PartitionData, records::RecordsPayload};

use super::{
    acknowledge::{AckApplication, apply_acknowledgements},
    long_poll::{LongPollOutcome, arm_waits, long_poll},
    pending::PendingPartition,
    records::{
        AcquireMode, AcquireRequest, acquire_read_records, merge_batches,
        pending_activation_ranges, read_budget, unreadable_batch_ranges,
    },
    tiered::TieredSource,
};
use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    share_partition::{
        group_settings::GroupShareSettings, manager::persistence::fences_the_partition,
    },
};

/// KFC-1: the most not-yet-due records an acquire pass leaves in one share
/// partition's window.
///
/// A deferred run is not in flight, so it does not spend the record lock
/// limit, `share_group_partition_max_record_locks`, and materialization walks
/// past it to reach the due records behind it. That is the point of the whole path, and
/// it needs a second bound of its own: without one, a single far-future batch
/// at the head of the window pulls the rest of the log in behind it, and every
/// later pass re-walks all of it.
///
/// This should be the broker config `share_delivery_max_deferred_records`. It
/// is a constant here because the runtime settings it belongs beside live in
/// `file_config.rs` and in
/// [`ShareGroupConfig`](crate::coordinator::unified::share::config::ShareGroupConfig).
const SHARE_DELIVERY_MAX_DEFERRED_RECORDS: i64 = 1_000;

#[derive(Clone, Copy)]
pub(super) struct AcquireContext<'a> {
    pub(super) broker: &'a Broker,
    pub(super) manager: &'a Arc<crate::share_partition::manager::SharePartitionLeaderManager>,
    pub(super) group: &'a str,
    pub(super) member: &'a str,
    pub(super) max_records: i32,
    /// The byte budget of the whole response, split across the partitions.
    pub(super) max_bytes: i32,
    /// The record bytes the response waits for, up to `max_bytes`.
    pub(super) min_bytes: i32,
    /// `ShareAcquireMode` and `BatchSize`.
    pub(super) mode: AcquireMode,
    pub(super) renewal: super::acknowledge::Renewal,
    /// The group's share settings: lock duration, delivery count limit,
    /// record lock limit and isolation level.
    pub(super) settings: GroupShareSettings,
}

/// What the passes of one request have taken so far.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Spent {
    records: i64,
    bytes: i64,
}

/// Runs acquire passes until the response holds `min_bytes` of records, the
/// record or byte budget is spent, or `max_wait_ms` runs out.
///
/// This is Kafka's `DelayedShareFetch`: the request waits in purgatory until
/// `isMinBytesSatisfied` or the wait ends. Kafka measures the bytes that the
/// log holds past each fetch offset; this measures the bytes it acquired,
/// which is the same for records that no other member takes first. A
/// `min_bytes` of 0 is satisfied at once. Each later pass adds to what the
/// earlier ones acquired.
pub(super) async fn acquire_records(
    context: &AcquireContext<'_>,
    pending: &mut [PendingPartition],
    max_wait_ms: i32,
) {
    let wait = Duration::from_millis(u64::try_from(max_wait_ms).unwrap_or(0));
    let deadline = Instant::now() + wait;
    let min_bytes = i64::from(context.min_bytes.min(context.max_bytes).max(0));
    let mut spent = Spent::default();
    let mut first = true;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        // Arm the long poll's waiters before the pass, so a record produced
        // while the pass runs still wakes the park that follows it.
        let waits = if left.is_zero() {
            Vec::new()
        } else {
            arm_waits(context.broker, pending)
        };
        spent = acquire_pass(context, pending, first, spent).await;
        first = false;
        let done = spent.bytes >= min_bytes
            || spent.records >= i64::from(context.max_records)
            || spent.bytes >= i64::from(context.max_bytes);
        if done || left.is_zero() {
            return;
        }
        let left_ms = i32::try_from(
            deadline
                .saturating_duration_since(Instant::now())
                .as_millis(),
        )
        .unwrap_or(i32::MAX);
        if long_poll(waits, left_ms).await == LongPollOutcome::NoPartitions {
            return;
        }
    }
}

/// Kafka's `PartitionMaxBytesStrategy.uniformPartitionMaxBytes`: the byte
/// budget of the partition at `index` of `partitions`.
///
/// An even share each, with the remainder on one partition. A budget smaller
/// than the partition count gives one byte to as many partitions as it
/// covers and none to the rest. Kafka picks the partitions for the remainder
/// and for the single bytes at random; this picks the first ones, which the
/// round-robin rotation of the partitions spreads over the requests.
fn uniform_share(budget: i64, partitions: usize, index: usize) -> i32 {
    let (Ok(count), Ok(at)) = (i64::try_from(partitions), i64::try_from(index)) else {
        return 0;
    };
    if count == 0 || budget <= 0 {
        return 0;
    }
    let share = if budget >= count {
        budget / count + if at == 0 { budget % count } else { 0 }
    } else {
        i64::from(at < budget)
    };
    i32::try_from(share).unwrap_or(i32::MAX)
}

/// Pulls freshly produced records into the acquisition window, unless the
/// schedule already holds back [`SHARE_DELIVERY_MAX_DEFERRED_RECORDS`] of them.
///
/// The previous pass's deferral still stands when this runs, which is why the
/// count means something here and would read zero after `promote_deferred`. It
/// is one pass out of date, and that only makes the bound conservative.
fn materialize_within_deferral_bound(
    state: &mut crate::share_partition::state::AcquisitionState,
    upper: Offset,
    max_record_locks: i32,
) {
    if state.deferred_records() < SHARE_DELIVERY_MAX_DEFERRED_RECORDS {
        state.materialize(upper, max_record_locks);
    }
}

/// Materializes the window and archives the offsets that no share consumer
/// may get, until the window holds an `Available` record or cannot grow.
///
/// Transaction markers occupy log offsets but are broker metadata, not user
/// records. They are archived before acquisition so their encoded coordinator
/// epoch can never appear in a `ShareFetch` response. Under `read_committed`,
/// the data of aborted transactions is archived too, as Kafka's
/// `SharePartition` does: the share consumer has no aborted transaction list
/// to filter them with.
///
/// One materialization adds at most the room the record lock limit leaves. When all of them
/// are archived, the window grows again in the same pass, so a large aborted
/// transaction does not turn into a run of empty fetches. The first scan
/// covers the whole window, and each later scan covers only the offsets that
/// the last materialization added.
async fn grow_readable_window(
    state: &mut crate::share_partition::state::AcquisitionState,
    partition: &crate::partition::Partition,
    upper: Offset,
    max_record_locks: i32,
    read_committed: bool,
) -> Result<(), BrokerError> {
    // The scan floor must never sit below the first offset the local log
    // holds. An `Acquired` batch below a moved log start keeps
    // `state.start_offset` (the SPSO) pinned there until its lock expires
    // (see `AcquisitionState::advance_past_log_start`), but the log itself no
    // longer has that range to read. Without this clamp, every pass would
    // re-scan the now-deleted prefix and hit `LogError::OffsetTooLow` again,
    // even though `Available` offsets exist at or above the log start. On a
    // tiered partition (KIP-405) the offsets below the local log start are in
    // the remote tier, and the acquire step classifies them from the batches
    // it reads there.
    let log_start = partition
        .log
        .lock()
        .expect("log mutex poisoned")
        .local_log_start_offset();
    let mut scan_from = state.start_offset.max(log_start);
    loop {
        let end_before = state.end_offset;
        materialize_within_deferral_bound(state, upper, max_record_locks);
        let scan_start = scan_from.max(state.start_offset).max(log_start);
        for (first, last) in
            unreadable_batch_ranges(partition, scan_start, state.end_offset, read_committed).await?
        {
            state.archive_internal(first, last);
        }
        if state.end_offset == end_before || state.has_available() {
            return Ok(());
        }
        scan_from = state.end_offset;
    }
}

fn remaining_record_budget(max_records: i32, acquired: i64) -> i32 {
    max_records
        .saturating_sub(i32::try_from(acquired).unwrap_or(i32::MAX))
        .max(0)
}

/// The read-only inputs [`grow_and_acquire`] needs, gathered into one value so
/// the function itself takes few enough arguments for `clippy::pedantic`.
struct GrowAndAcquireArgs<'a> {
    part: &'a Arc<crate::partition::Partition>,
    upper: Offset,
    settings: &'a GroupShareSettings,
    member: &'a str,
    max_bytes: i32,
    min_one_batch: bool,
    remaining_records: i32,
    mode: AcquireMode,
    now: Instant,
    tier: Option<TieredSource<'a>>,
}

/// Grows the readable window, promotes due deferrals, and acquires records
/// for one partition, in that order.
///
/// This is everything an acquire pass does with a partition's log once its
/// acknowledgements are applied and its exhausted records are archived. It is
/// split out so the caller can catch [`LogError::OffsetTooLow`] around the
/// whole sequence: any of these steps can read at the share-partition start
/// offset, and that offset can sit below the log's start offset.
async fn grow_and_acquire(
    st: &mut crate::share_partition::state::AcquisitionState,
    out: &mut PartitionData,
    args: GrowAndAcquireArgs<'_>,
) -> Result<i64, BrokerError> {
    let GrowAndAcquireArgs {
        part,
        upper,
        settings,
        member,
        max_bytes,
        min_one_batch,
        remaining_records,
        mode,
        now,
        tier,
    } = args;
    grow_readable_window(
        st,
        part,
        upper,
        settings.max_record_locks,
        settings.read_committed,
    )
    .await?;
    // KFC-1: re-derive the deferral from the log and this partition's own
    // clock on every pass, exactly as the control-batch ranges above are.
    // Dropping it first is what keeps a batch that has since come due from
    // staying held back by an older clock reading.
    st.promote_deferred();
    let deferred =
        pending_activation_ranges(part, st.start_offset, st.end_offset, part.delivery.now_ms())
            .await?;
    for (first, last) in deferred {
        st.defer_internal(first, last);
    }
    if remaining_records <= 0 {
        return Ok(0);
    }
    let request = AcquireRequest {
        member,
        max_records: remaining_records,
        max_bytes,
        min_one_batch,
        upper,
        now,
        lock_duration: settings.record_lock_duration,
        max_attempts: settings.delivery_count_limit,
        mode,
        tier,
    };
    acquire_read_records(out, part, st, &request).await
}

/// True when `err` is the log answering that a read started below its log
/// start offset: `Log::check_locally_readable`'s [`LogError::OffsetTooLow`].
///
/// This is the retention/`DeleteRecords`/old-earliest-reset race the SPSO
/// cannot see coming: the log start offset moved past it between one acquire
/// pass and the next. It is not a partition-fencing error and not a corrupt
/// read, so the caller recovers from it in place of failing the request.
fn log_start_moved_past_spso(err: &BrokerError) -> bool {
    matches!(err, BrokerError::Log(LogError::OffsetTooLow { .. }))
}

/// Runs one acquire pass over the pending partitions that this broker can
/// lead.
///
/// When `apply_acks` is true, this function applies the piggybacked
/// acknowledgement batches first, and sets `acknowledge_error_code`. A batch
/// offset of type Renew renews its acquisition lock, per KIP-1222.
///
/// Under a `ReadCommitted` isolation level, this function clamps the
/// materialize and read window to the partition's last stable offset, so it
/// never acquires an uncommitted record, and it archives the data batches of
/// aborted transactions in the window. It returns the total number of
/// offsets that it acquired across all partitions in this pass.
///
/// On a KFC-1 scheduled topic it also re-derives which ranges of the window
/// are not due yet and marks them `Deferred`, so acquisition steps over them.
/// The derivation is thrown away and redone on each pass, so nothing outlives
/// the clock reading that produced it.
async fn acquire_pass(
    context: &AcquireContext<'_>,
    pending: &mut [PendingPartition],
    apply_acks: bool,
    spent: Spent,
) -> Spent {
    let &AcquireContext {
        broker,
        manager: mgr,
        group,
        member,
        max_records,
        max_bytes,
        mode,
        renewal,
        settings,
        ..
    } = context;
    let now = Instant::now();
    let read_committed = settings.read_committed;
    let mut total = spent;
    let fetching = pending.iter().filter(|p| p.leadable && p.fetchable).count();
    let byte_budget = i64::from(max_bytes) - spent.bytes;
    let mut fetch_index = 0_usize;

    for p in pending.iter_mut() {
        if !p.leadable {
            continue;
        }

        let mut has_acks = apply_acks && !p.ack_batches.is_empty();
        // Kafka's `SharePartitionManager.acknowledge` runs before the fetch,
        // and answers UNKNOWN_TOPIC_OR_PARTITION for a share partition that no
        // earlier fetch on this broker loaded.
        if has_acks && mgr.cached(group, p.topic_id, p.partition_index).is_none() {
            p.out.acknowledge_error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
            has_acks = false;
        }
        // A failed state read fails the partition and caches nothing, as
        // Kafka's `SharePartitionManager.handleInitializationException` does.
        let cell = match mgr.get_or_load(group, p.topic_id, p.partition_index).await {
            Ok(cell) => cell,
            Err(code) => {
                fail_partition(p, has_acks, code);
                continue;
            }
        };
        let mut st = cell.lock().await;
        st.set_dlq_enabled(settings.dlq_enabled);

        // Apply piggybacked acknowledgements (first pass only), all or
        // nothing. The type Renew renews the lock of its offsets, and the
        // other types take their normal transition. The change is durable
        // before the acquisition runs, or it is rolled back and the write
        // error becomes the acknowledge error.
        if has_acks {
            let application = AckApplication {
                member,
                now,
                renewal,
                max_attempts: settings.delivery_count_limit,
            };
            let batches = p
                .ack_batches
                .iter()
                .map(|(first, last, types)| (*first, *last, types.as_slice()));
            let code = mgr
                .apply_durably(group, p.topic_id, p.partition_index, &cell, &mut st, |st| {
                    apply_acknowledgements(st, &application, batches)
                })
                .await;
            p.out.acknowledge_error_code = code;
            if fences_the_partition(code) {
                fail_partition(p, true, code);
                continue;
            }
        }

        if !p.fetchable {
            // Best-effort: a failed write keeps the state dirty for a retry.
            let _ = mgr
                .persist_if_dirty(group, p.topic_id, p.partition_index, Some(&cell), &mut st)
                .await;
            continue;
        }

        // Expire stale locks, materialize freshly produced records, acquire.
        st.expire_locks(now, settings.delivery_count_limit);
        let part = p.topic_name.as_deref().and_then(|name| {
            broker
                .partitions
                .get(name, krabka_ids::PartitionIndex(p.partition_index))
        });
        let Some(part) = part else {
            // Lost the partition between the leadership check and here.
            p.out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
            p.leadable = false;
            // Best-effort: a failed write keeps the state dirty for a retry.
            let _ = mgr
                .persist_if_dirty(group, p.topic_id, p.partition_index, Some(&cell), &mut st)
                .await;
            continue;
        };
        let hwm = part.high_watermark().await;
        // Under read_committed, never surface records past the last stable
        // offset: clamp the materialize/read window to `min(lso, hwm)` so no
        // record from an OPEN transaction can be acquired.
        //
        // KFC-1 puts no delivery watermark here on purpose. That cap is what
        // holds a classic group to offset order, and a share group is exactly
        // the reader that does not need it: the window runs to the high
        // watermark and the deferral marks below hold the waiting records
        // back one range at a time.
        let upper = if read_committed {
            part.last_stable_offset(hwm)
        } else {
            hwm
        };
        // A released or expired record at the delivery limit is archived
        // first, so it cannot hold the window shut.
        st.archive_exhausted(settings.delivery_count_limit);
        let share = uniform_share(byte_budget, fetching, fetch_index);
        fetch_index += 1;
        // A partition that the byte budget does not reach acquires nothing
        // this pass.
        let remaining_records = if share > 0 {
            remaining_record_budget(max_records, total.records)
        } else {
            0
        };
        // Kafka's `ReplicaManager.readFromLog`: each read is capped at what
        // the response has left, and only a read made while the response
        // still holds no record may exceed its cap, by the one batch that it
        // starts with. Without that, each partition whose next batch is
        // larger than its share would add one oversized batch.
        let response_left = i64::from(max_bytes).saturating_sub(total.bytes).max(0);
        let read_max_bytes = read_budget(p.partition_max_bytes, share)
            .min(i32::try_from(response_left).unwrap_or(i32::MAX));
        let min_one_batch = total.bytes == 0;
        let tier = broker
            .remote_reader
            .as_deref()
            .zip(p.topic_name.as_ref())
            .map(|(reader, topic)| TieredSource {
                reader,
                metrics: &broker.metrics,
                tp: krabka_remote_storage::TopicIdPartition::new(
                    p.topic_id,
                    topic.clone(),
                    p.partition_index,
                ),
                read_committed,
            });
        let grow_and_acquire_args = || GrowAndAcquireArgs {
            part: &part,
            upper,
            settings: &settings,
            member,
            max_bytes: read_max_bytes,
            min_one_batch,
            remaining_records,
            mode,
            now,
            tier: tier.clone(),
        };
        // This pass's records land in `fresh`, then join what earlier passes
        // put in the row.
        let mut fresh = PartitionData::default();
        let mut outcome = grow_and_acquire(&mut st, &mut fresh, grow_and_acquire_args()).await;
        if outcome.as_ref().is_err_and(log_start_moved_past_spso) {
            // Kafka's `ShareFetchUtils.processFetchResponse` /
            // `SharePartition.updateCacheAndOffsets`: the log start offset
            // moved past the SPSO. Archive the Available/Deferred records
            // below it, move the SPSO (and the SPEO, if the window had not
            // grown that far). An Acquired record below the new start
            // stays locked until it times out.
            st.advance_past_log_start(part.log_start_offset());
            fresh = PartitionData::default();
            // Retry once in place, now that the SPSO/scan floor is
            // repaired: without this, a repair that leaves readable
            // records at the new log start would still report 0
            // acquired, and the caller (which only long-polls when the
            // WHOLE pass acquires nothing) would park for the full
            // max_wait_ms even though a retry right now would already
            // find them.
            outcome = grow_and_acquire(&mut st, &mut fresh, grow_and_acquire_args()).await;
            if outcome.as_ref().is_err_and(log_start_moved_past_spso) {
                // The log start moved again between the two attempts.
                // Report the partition as caught up with no records
                // rather than failing the whole request.
                st.advance_past_log_start(part.log_start_offset());
                fresh = PartitionData::default();
                outcome = Ok(0);
            }
        }
        let acquired_count = match outcome {
            Ok(count) => count,
            Err(err) => {
                // Kafka's `ShareFetchUtils.processFetchResponse` turns a read
                // error of one partition into that partition's error code with
                // no records, and the other partitions of the request still
                // return theirs. A record that this pass locked before the
                // failure stays locked until its lock times out.
                let code = read_failure_code(&err);
                tracing::warn!(
                    group,
                    topic_id = %p.topic_id,
                    partition = p.partition_index,
                    error = %err,
                    code,
                    "share-partition log read failed"
                );
                // Best-effort: a failed write keeps the state dirty for a retry.
                let _ = mgr
                    .persist_if_dirty(group, p.topic_id, p.partition_index, Some(&cell), &mut st)
                    .await;
                fail_partition(p, false, code);
                continue;
            }
        };

        p.out.error_code = codes::NONE;
        // The acquisition itself is not durable state (an acquired record
        // persists as available), so a failed write keeps the state dirty for
        // a retry. A fenced write drops the cell, and the records acquired on
        // it must not reach the client.
        match mgr
            .persist_if_dirty(group, p.topic_id, p.partition_index, Some(&cell), &mut st)
            .await
        {
            Err(code) if fences_the_partition(code) => fail_partition(p, false, code),
            _ => match append_records(&mut p.out, fresh) {
                Ok(bytes) => {
                    total.records += acquired_count;
                    total.bytes += bytes;
                }
                Err(err) => fail_partition(p, false, read_failure_code(&err)),
            },
        }
    }
    total
}

/// The error code of a partition whose log read failed, as Kafka's
/// `Errors.forException` maps what `ReplicaManager.readFromLog` catches: an
/// I/O fault is `KAFKA_STORAGE_ERROR`, a batch that does not decode or check
/// is `CORRUPT_MESSAGE`, and anything else is `UNKNOWN_SERVER_ERROR`.
fn read_failure_code(err: &BrokerError) -> i16 {
    match err {
        BrokerError::Log(
            LogError::CrcMismatch { .. }
            | LogError::PartialBatch { .. }
            | LogError::Records(_)
            | LogError::Corrupt(_),
        ) => codes::CORRUPT_MESSAGE,
        BrokerError::Log(LogError::Io(_)) => codes::KAFKA_STORAGE_ERROR,
        other => codes::from_broker_error(other),
    }
}

/// Adds the records and the acquired rows of one pass to a partition row, and
/// returns the record bytes it added.
///
/// A batch that an earlier pass already put in the row is not added again:
/// see [`merge_batches`].
fn append_records(out: &mut PartitionData, fresh: PartitionData) -> Result<i64, BrokerError> {
    out.acquired_records.extend(fresh.acquired_records);
    let Some(RecordsPayload::Raw(added)) = fresh.records else {
        return Ok(0);
    };
    let (joined, added_len) = match out.records.take() {
        Some(RecordsPayload::Raw(before)) if !before.is_empty() => merge_batches(&before, &added)?,
        _ => {
            let added_len = i64::try_from(added.len()).unwrap_or(i64::MAX);
            (added, added_len)
        }
    };
    out.records = Some(RecordsPayload::Raw(joined));
    Ok(added_len)
}

/// Fails one partition row with a share-partition error, and leaves it out of
/// any later acquire pass.
///
/// The fetch error goes on a row of the share session, and the acknowledge
/// error on a row that carried acknowledgements.
fn fail_partition(p: &mut PendingPartition, has_acks: bool, code: i16) {
    p.out.records = None;
    p.out.acquired_records.clear();
    if p.fetchable {
        p.out.error_code = code;
    }
    if has_acks {
        p.out.acknowledge_error_code = code;
    }
    p.leadable = false;
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn materialization_stops_at_the_deferred_record_bound() {
        let bound = SHARE_DELIVERY_MAX_DEFERRED_RECORDS;
        let inflight = i32::try_from(bound).expect("the bound fits an inflight budget");
        for (deferred, expected_end) in [(bound - 1, bound + 9), (bound, bound)] {
            let mut state = crate::share_partition::state::AcquisitionState::new(Offset(0));
            state.materialize(Offset(deferred), inflight);
            state.defer_internal(Offset(0), Offset(deferred - 1));
            assert!(state.deferred_records() == deferred);

            materialize_within_deferral_bound(&mut state, Offset(deferred + 10), 10);

            assert!(
                state.end_offset == Offset(expected_end),
                "deferred={deferred}"
            );
        }
    }

    /// A transactional batch at offsets 0-2 from producer 1000, its end marker
    /// at offset 3, and a plain record at offset 4.
    fn transaction_then_record(commit: bool) -> Vec<krabka_protocol::records::RecordBatch> {
        use bytes::Bytes;
        use krabka_protocol::records::{Attributes, Record, RecordBatch};

        let transactional = Attributes::default().with_transactional(true);
        let value = |v: &'static [u8]| Record {
            value: Some(Bytes::from_static(v)),
            ..Record::default()
        };
        // Control key: version 0, marker type (0 abort, 1 commit). Control
        // value: version 0, coordinator epoch 0.
        let marker_key = [0, 0, 0, u8::from(commit)];
        vec![
            RecordBatch {
                last_offset_delta: 2,
                producer_id: 1000,
                attributes: transactional,
                records: (0..3)
                    .map(|offset_delta| Record {
                        offset_delta,
                        ..value(b"txn")
                    })
                    .collect(),
                ..RecordBatch::default()
            },
            RecordBatch {
                producer_id: 1000,
                attributes: transactional.with_control(true),
                records: vec![Record {
                    key: Some(Bytes::copy_from_slice(&marker_key)),
                    value: Some(Bytes::from_static(&[0, 0, 0, 0, 0, 0])),
                    ..Record::default()
                }],
                ..RecordBatch::default()
            },
            RecordBatch {
                records: vec![value(b"plain")],
                ..RecordBatch::default()
            },
        ]
    }

    /// One pass reaches the first readable record behind a transaction that
    /// fills whole materialization windows with offsets it archives.
    #[tokio::test]
    async fn the_window_grows_past_offsets_that_are_all_archived() {
        use std::sync::Arc;

        use qubit_clock::{FixedWallClock, WallClock};

        use crate::delivery::test_support::{NOW_MS, partition_with_batches, wall_at};

        // (commit, read_committed, max_inflight, acquired)
        let cases = [
            (false, true, 2, vec![(Offset(4), Offset(4))]),
            (false, true, 100, vec![(Offset(4), Offset(4))]),
            (false, false, 2, vec![(Offset(0), Offset(1))]),
            (true, true, 2, vec![(Offset(0), Offset(1))]),
        ];
        let clock: Arc<dyn WallClock> = Arc::new(FixedWallClock::new(wall_at(NOW_MS)));
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (commit, read_committed, max_inflight, acquired) in cases {
            let dir = tempfile::tempdir().expect("a log root");
            let partition = partition_with_batches(
                &dir,
                &clock,
                crate::delivery::test_support::DeliveryPartitionSetup {
                    topic: "txn",
                    batches: transaction_then_record(commit),
                    leader: krabka_ids::NodeId(0),
                    ..Default::default()
                },
            );
            let mut state = crate::share_partition::state::AcquisitionState::new(Offset(0));

            grow_readable_window(
                &mut state,
                &partition,
                Offset(5),
                max_inflight,
                read_committed,
            )
            .await
            .expect("grow the window");
            let got: Vec<_> = state
                .acquire(
                    "m",
                    500,
                    Offset(4),
                    std::time::Instant::now(),
                    std::time::Duration::from_secs(30),
                    5,
                )
                .into_iter()
                .map(|range| (range.first, range.last))
                .collect();

            let row = (commit, read_committed, max_inflight);
            actual.push((row, got));
            expected.push((row, acquired));
        }
        assert!(actual == expected);
    }

    /// Kafka's `uniformPartitionMaxBytes` with the random picks fixed to the
    /// first partitions.
    #[test]
    fn the_byte_budget_splits_evenly_across_the_partitions() {
        let shares = |budget, partitions| {
            (0..partitions)
                .map(|index| uniform_share(budget, partitions, index))
                .collect::<Vec<_>>()
        };
        check!(shares(1 << 20, 1) == vec![1 << 20]);
        check!(shares(10, 3) == vec![4, 3, 3]);
        check!(shares(2, 3) == vec![1, 1, 0]);
        check!(shares(0, 2) == vec![0, 0]);
    }

    /// Kafka's `Errors.forException` for what `ReplicaManager.readFromLog`
    /// catches.
    #[test]
    fn a_failed_log_read_maps_to_the_partition_error_kafka_gives() {
        let io = || std::io::Error::other("disk");
        let cases = [
            (
                BrokerError::Log(LogError::Io(io())),
                codes::KAFKA_STORAGE_ERROR,
            ),
            (
                BrokerError::Log(LogError::Corrupt("txn index".into())),
                codes::CORRUPT_MESSAGE,
            ),
            (
                BrokerError::Log(LogError::Records(
                    krabka_protocol::records::RecordsError::BodyTooShort { needed: 1 },
                )),
                codes::CORRUPT_MESSAGE,
            ),
            (BrokerError::Io(io()), codes::UNKNOWN_SERVER_ERROR),
        ];
        let (got, want): (Vec<_>, Vec<_>) = cases
            .iter()
            .map(|(err, code)| (read_failure_code(err), *code))
            .unzip();
        assert!(got == want);
    }

    #[test]
    fn remaining_record_budget_is_request_wide_and_saturating() {
        check!(remaining_record_budget(500, 300) == 200);
        check!(remaining_record_budget(500, 500) == 0);
        check!(remaining_record_budget(500, i64::MAX) == 0);
    }
}
