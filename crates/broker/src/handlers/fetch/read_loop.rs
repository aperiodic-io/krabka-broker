//! The read loop: every planned partition is read once, and a fetch that did
//! not reach `min_bytes` parks on the partitions' notifiers, reading a
//! partition again only when its own notifier fires, until the accumulated
//! bytes clear the floor, an epoch check fails, or `max_wait_ms` runs out.

use {
    super::{
        plan::{
            PendingRead, ReadRole, TopicPartitionRef, apply_epoch_checks, leader_refusal,
            record_follower_position, required_leader,
        },
        read::{LiveOffsets, ReadRequest, do_read},
        remote::try_remote_read,
        request::EffectivePartition,
        response::{GroupedResponses, group_into_topic_responses},
    },
    crate::{
        broker::Broker, codes, diskless::read::try_diskless_read, metrics::RequestPhases,
        partition::Partition,
    },
    krabka_log::Offset,
    krabka_protocol::owned::fetch_response::PartitionData,
    krabka_units::convert::ByteSizeExt as _,
    std::{sync::Arc, time::Duration},
    tokio::sync::Notify,
};

/// The initial pass and a long-poll re-read use the same planned read inputs.
fn planned_read_request(
    broker: &Broker,
    read: &PendingRead,
    max_bytes: i32,
    sendfile_capable: bool,
) -> ReadRequest {
    ReadRequest {
        topic_id: Some(uuid::Uuid::from_bytes(read.topic_id.0)),
        hot_tail: Some(broker.hot_tail.clone()),
        fetch_offset: Offset(read.fetch_offset),
        max_bytes,
        read_committed: read.read_committed,
        is_follower_fetch: read.is_follower_fetch,
        sendfile_capable,
        sendfile_min_bytes: broker.config.sendfile_min.bytes_usize(),
    }
}

type WaitFut = std::pin::Pin<Box<dyn std::future::Future<Output = Woken> + Send>>;

/// The waiter that fired: which pending read it belongs to, and the notifier
/// it parked on, so the poll loop can arm a replacement on the same notifier
/// before it reads.
struct Woken {
    pending: usize,
    notify: Arc<Notify>,
}

/// What a fetch that has already read once needs in order to decide whether to
/// keep parking.
struct LongPollState {
    /// The `min_bytes` floor of the request, clamped at zero. Kafka treats
    /// `fetch.min.bytes` as a contract and not as a hint: the fetch is held
    /// until this many bytes are readable across its partitions or the wait
    /// expires.
    min_bytes: usize,
    max_wait_ms: i32,
    sendfile_capable: bool,
    /// Bytes the latest read of each pending entry produced, indexed like
    /// `pending`. A re-read replaces its entry rather than adding to it,
    /// because the re-read replaces the records too.
    bytes: Vec<usize>,
    /// `true` where the cold tier, and not the local log, answered the entry.
    cold_served: Vec<bool>,
    /// What is left of the whole response's byte budget: `min(request
    /// max_bytes, fetch.max.bytes)`, run down as partitions are read.
    ///
    /// Kafka's `ReplicaManager.readFromLog` carries this down the partition
    /// loop as `limitBytes`; this field is the same running total, updated
    /// after every partition read in both the first pass and a long-poll
    /// re-read. A partition whose own budget is already spent still gets a
    /// real read at `max_bytes = 0` rather than being skipped -- its offsets
    /// and watermarks are still live -- and this broker's log layer
    /// guarantees that read at least one batch. The caller keeps an oversized
    /// batch only while the response has no records, so the progress exception
    /// applies to the first nonempty partition alone.
    remaining_response_bytes: usize,
    /// Original response budget, used to recompute the remaining budget when
    /// a long-poll read replaces an earlier oversized first batch.
    response_max_bytes: usize,
}

#[derive(Clone, Copy)]
struct ReadAccounting {
    local_bytes: usize,
    cold_bytes: usize,
    budget: i32,
    allow_first_batch_overrun: bool,
}

impl LongPollState {
    fn total(&self) -> usize { self.bytes.iter().sum() }

    /// The `max_bytes` to give this partition's read: whatever
    /// `partition_max_bytes` asked for, capped at what the response has left.
    fn partition_read_budget(&self, requested_max_bytes: i32) -> i32 {
        let requested = usize::try_from(requested_max_bytes.max(0)).unwrap_or(usize::MAX);
        i32::try_from(requested.min(self.remaining_response_bytes)).unwrap_or(i32::MAX)
    }

    /// Charges `served` bytes against the response budget. Saturates at
    /// zero: the anti-stall guarantee below this call can still hand back
    /// more than was asked for, and that overrun must not wrap the budget
    /// back up.
    fn charge(&mut self, served: usize) {
        self.remaining_response_bytes = self.remaining_response_bytes.saturating_sub(served);
    }

    fn record_read(&mut self, index: usize, read: &mut PendingRead, result: ReadAccounting) {
        self.cold_served[index] = result.cold_bytes > 0;
        self.bytes[index] = result.local_bytes + result.cold_bytes;
        if !result.allow_first_batch_overrun
            && self.bytes[index] > usize::try_from(result.budget.max(0)).unwrap_or(0)
        {
            discard_to_metadata_only(read);
            self.bytes[index] = 0;
            self.cold_served[index] = false;
        }
        self.charge(self.bytes[index]);
    }
}

/// Arms one waiter on `notify` and tags it with the pending entry it belongs
/// to.
///
/// The waiter registers here rather than on its first poll. Every producer
/// path signals with `notify_waiters`, which wakes only the waiters already
/// registered and leaves no permit behind, so a waiter armed after the read
/// pass would miss an append that landed during it and would then sleep out
/// the whole `max_wait_ms`.
fn arm_wait(pending: usize, notify: Arc<Notify>) -> WaitFut {
    let mut notified = Box::pin(Arc::clone(&notify).notified_owned());
    notified.as_mut().enable();
    Box::pin(async move {
        notified.await;
        Woken { pending, notify }
    })
}

/// Arms every planned partition's waiters, before any of them is read.
///
/// A fetch that then finds enough bytes on the first pass drops the whole set
/// unused, so a wake set costs one waiter-list insertion and one removal per
/// notifier whether or not the fetch parks. That is the price of the guarantee
/// -- registering after the read is what loses an append -- and it is small
/// beside the per-partition log read the same pass makes. Only a fetch that
/// asked to wait pays it at all: `max_wait_ms == 0` arms nothing.
fn arm_waits(pending: &[PendingRead]) -> Vec<WaitFut> {
    let mut waits = Vec::new();
    for (index, read) in pending.iter().enumerate() {
        let Some(part) = read.partition.as_ref() else {
            continue;
        };
        waits.push(arm_wait(index, part.append_notify.clone()));
        // KIP-392: a consumer reading from a follower becomes unblocked
        // when the follower's HW advances (via set_follower_hw), not only
        // on raw append. Follower (inter-broker) fetches don't need this.
        //
        // KFC-1: a consumer parked exactly at the delivery watermark is
        // waiting for time to pass and not for bytes to arrive. Nothing
        // appends and the HW does not move when the batch it wants comes
        // due, so the delivery advance is its own wake. Without it the
        // consumer sleeps out its whole long poll in the one case the
        // feature exists for.
        if !read.is_follower_fetch {
            waits.push(arm_wait(index, part.hw_advance_notify.clone()));
            waits.push(arm_wait(index, part.delivery.advance_notify.clone()));
        }
    }
    waits
}

/// Arms the leadership waiter of every follower read, after the first pass
/// records the follower positions.
///
/// A leadership change fires `hw_advance_notify`. A fetch that only the leader
/// may serve wakes on it, so a parked follower fetch answers
/// `NOT_LEADER_OR_FOLLOWER` at once, as Kafka's `DelayedFetch` completes on a
/// leader change.
///
/// This function arms the waiter only after the first pass. The position that
/// the pass records fires the same notifier when it moves the high watermark.
/// A waiter armed before the pass would wake its own fetch once for each
/// partition, and each wake revalidates every partition of the fetch. The
/// revalidation at the start of the long poll catches a leadership change
/// that lands before this arm.
fn arm_leadership_waits(pending: &[PendingRead]) -> Vec<WaitFut> {
    pending
        .iter()
        .enumerate()
        .filter(|(_, read)| read.is_follower_fetch && read.fetch_only_leader)
        .filter_map(|(index, read)| {
            let part = read.partition.as_ref()?;
            Some(arm_wait(index, part.hw_advance_notify.clone()))
        })
        .collect()
}

/// Reports the live bounds on every follower row the first pass read
/// locally, right before the fetch parks.
///
/// A follower row that the first pass answers carries the high watermark from
/// before the leader recorded its fetch offset, as Kafka's first read does.
/// The leader answers a fetch that parks later, and Kafka then reads every
/// partition again, on a wake or on expiry. This function reads the bounds
/// after [`arm_leadership_waits`] arms its waiters. So a later move of the
/// high watermark wakes the fetch, and the fetch reads the row again.
async fn refresh_follower_bounds(pending: &mut [PendingRead], state: &LongPollState) {
    for (index, read) in pending.iter_mut().enumerate() {
        if !read.is_follower_fetch || read.out.error_code != codes::NONE || state.cold_served[index]
        {
            continue;
        }
        let Some(part) = read.partition.clone() else {
            continue;
        };
        LiveOffsets::of(&part).await.report(&mut read.out);
    }
}

/// Read every planned partition once, then long-poll until the fetch reaches
/// `min_bytes` or runs out of wait.
///
/// The loop is sequential, so the cost of getting one partition's blocking
/// read off the reactor is paid once per partition before any bytes go out: a
/// consumer subscribed to 200 partitions pays it 200 times. `do_read` makes
/// that hand-off with `block_in_place` rather than a `spawn_blocking` per
/// partition, which `bench_fetch_handoff` measured at a tenth of the cost;
/// [`super::read::run_blocking_read`] carries the numbers and the trade.
///
/// The per-partition step stays a step, because the cold-tier fallback a
/// partition falls through to -- [`serve_from_cold_tier`], covering the
/// remote tier and diskless -- is async and cannot run inside one blocking
/// closure covering the whole pending set.
pub(super) async fn execute_pending_reads(
    broker: &Broker,
    mut pending: Vec<PendingRead>,
    min_bytes: i32,
    response_max_bytes: usize,
    max_wait_ms: i32,
    sendfile_capable: bool,
    phases: &RequestPhases,
) -> GroupedResponses {
    let mut state = LongPollState {
        // Kafka's `fetchMinBytes = min(minBytes, fetchMaxBytes)`: a floor the
        // response's own cap could never clear is not a floor at all.
        min_bytes: usize::try_from(min_bytes.max(0))
            .unwrap_or(0)
            .min(response_max_bytes),
        max_wait_ms,
        sendfile_capable,
        bytes: vec![0; pending.len()],
        cold_served: vec![false; pending.len()],
        remaining_response_bytes: response_max_bytes,
        response_max_bytes,
    };
    // Arm the long poll's waiters before the first read pass, so that an
    // append landing between a partition's read and the park cannot be lost.
    // Kafka closes the same window by re-running `tryComplete` right after it
    // registers the watch (`tryCompleteElseWatch`).
    let waits = if max_wait_ms > 0 {
        arm_waits(&pending)
    } else {
        Vec::new()
    };
    for (index, read) in pending.iter_mut().enumerate() {
        let Some(partition) = read.partition.clone() else {
            continue;
        };
        // Check the leader again right before the first read, so a leadership
        // change after planning does not serve a follower fetch or an old
        // consumer fetch from this replica.
        if let Some(refused) = leader_refusal(
            &broker.controller.current_image(),
            TopicPartitionRef {
                topic: &read.topic_name,
                partition_index: read.partition_index,
            },
            &partition,
            required_leader(read.fetch_only_leader, broker.config.node_id, &partition),
        ) {
            read.out = refused;
            continue;
        }
        // Cap this partition's read at what the response has left. The
        // original per-partition request stays in `read.max_bytes`, which a
        // later long-poll re-read of this same partition still consults;
        // only the current pass's own budget is capped here.
        let requested_max_bytes = read.max_bytes;
        let budget = state.partition_read_budget(requested_max_bytes);
        let min_one_batch = state.total() == 0;
        let started = std::time::Instant::now();
        // Kafka's `Partition.readRecords` takes the log end offset with the
        // high watermark before it reads. The follower update after the read
        // compares the fetch offset with that log end offset.
        let follower_update = read
            .follower_position
            .map(|position| (position, partition.log_end_offset()));
        let local_bytes = do_read(
            &partition,
            planned_read_request(broker, read, budget, sendfile_capable),
            &broker.log_dir_status,
            &mut read.out,
        )
        .await;
        read.cpu_micros = read
            .cpu_micros
            .saturating_add(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
        // The same interval, on the request's local phase. The two accounts
        // differ in what they roll up to: `cpu_micros` is per partition and
        // feeds the rebalancer, while the phase is per request and is the
        // Fetch half of `request_local_duration_seconds`.
        phases.add_local(started.elapsed());
        // `serve_from_cold_tier` reads its budget off `read.max_bytes`
        // itself, so this pass's capped budget is loaned to it here and
        // given back immediately after: the field's steady-state value stays
        // the original per-partition request, which is what a later
        // long-poll re-read of this partition (or an epoch recheck) must
        // still see.
        read.max_bytes = budget;
        let cold = serve_from_cold_tier(broker, read, &partition, phases).await;
        read.max_bytes = requested_max_bytes;
        state.record_read(index, read, ReadAccounting {
            local_bytes,
            cold_bytes: cold,
            budget,
            allow_first_batch_overrun: min_one_batch,
        });
        // Kafka's `Partition.fetchRecords` records the follower's position
        // only after its read succeeded, so the row this read just filled
        // reports the high watermark from before that position counted.
        if let Some((position, leader_log_end)) = follower_update
            && read.out.error_code == codes::NONE
        {
            record_follower_position(&partition, position, read.fetch_offset, leader_log_end).await;
        }
    }
    // KIP-392: a partition that named a preferred read replica was never
    // read locally and never will be while it keeps naming one, so parking
    // on it cannot help. Kafka's `ReplicaManager.fetchMessages` folds
    // `hasPreferredReadReplica` into its own completion check for exactly
    // this reason and answers the whole fetch at once rather than waiting
    // out `max_wait_ms` for bytes this broker cannot supply.
    let has_preferred_read_replica = pending
        .iter()
        .any(|read| read.out.preferred_read_replica >= 0);
    if state.total() < state.min_bytes && max_wait_ms > 0 && !has_preferred_read_replica {
        let mut waits = waits;
        waits.extend(arm_leadership_waits(&pending));
        refresh_follower_bounds(&mut pending, &state).await;
        long_poll_then_reread(broker, &mut pending, waits, &mut state, phases).await;
    }
    group_into_topic_responses(pending)
}

/// Park on the armed waiters until the fetch has `min_bytes` to answer with,
/// an epoch check fences one of its partitions, or `max_wait_ms` expires.
///
/// Each wake reads only the partition whose notifier fired: the rest keep the
/// records their last read gave them, which is what lets the wait accumulate
/// across several appends instead of restarting on each one. The single
/// deadline is the whole request's, so a fetch that wakes ten times still
/// answers within its `max_wait_ms`.
// cargo-mutants: long-poll serve-loop glue -- parks on partition append/HW
// notifiers, then replays `do_read` for the partition that woke it. The live
// fetch integration suite covers notifier-driven re-reads; the focused unit
// tests below cover the `min_bytes` floor, the append that lands before the
// park, and epoch revalidation after the wait.
#[cfg_attr(test, mutants::skip)]
async fn long_poll_then_reread(
    broker: &Broker,
    pending: &mut [PendingRead],
    mut waits: Vec<WaitFut>,
    state: &mut LongPollState,
    phases: &RequestPhases,
) {
    let max_wait = Duration::from_millis(u64::from(u32::try_from(state.max_wait_ms).unwrap_or(0)));
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        if revalidate_epochs(broker, pending).await
            || has_read_error(pending)
            || state.total() >= state.min_bytes
        {
            return;
        }
        if waits.is_empty() {
            return;
        }
        // The park is the Fetch remote phase: this broker has read everything
        // it holds and is waiting for someone else to append, so the time
        // belongs beside the Produce `acks=all` gate and not beside the local
        // read.
        let parked = std::time::Instant::now();
        let outcome =
            tokio::time::timeout_at(deadline, futures_util::future::select_all(waits)).await;
        phases.add_remote(parked.elapsed());
        let Ok((woken, _fired, rest)) = outcome else {
            // The deadline passed. Nothing more will be read, but the loop
            // head still revalidates the epochs before the response goes out.
            waits = Vec::new();
            continue;
        };
        waits = rest;
        // Arm the replacement before the read and not after it, for the reason
        // `arm_wait` gives: an append landing while this read runs would
        // otherwise leave nothing registered to catch it.
        waits.push(arm_wait(woken.pending, woken.notify));
        reread_woken(broker, pending, woken.pending, state, phases).await;
    }
}

/// Re-run the leader-epoch and divergence checks the plan applied before the
/// first read, and replace the response of every partition they now fence.
///
/// This is the half of Kafka's `DelayedFetch.tryComplete` that costs no I/O:
/// an epoch that moved while the fetch was parked completes the fetch whatever
/// the accumulated byte count is. A partition that still passes keeps the
/// records its last read gave it.
async fn revalidate_epochs(broker: &Broker, pending: &mut [PendingRead]) -> bool {
    let image = broker.controller.current_image();
    let mut fenced = false;
    for read in pending.iter_mut() {
        let Some(part) = read.partition.clone() else {
            continue;
        };
        let request = EffectivePartition {
            partition: read.partition_index,
            current_leader_epoch: read.current_leader_epoch,
            last_fetched_epoch: read.last_fetched_epoch,
            fetch_offset: read.fetch_offset,
            // The epoch recheck reads no log start.
            log_start_offset: -1,
            partition_max_bytes: read.max_bytes,
        };
        // A fenced or diverging row is a `LogReadResult` of Kafka's read path,
        // so its aborted transactions are null (`FetchDataInfo.empty`), not
        // the generated default's empty list.
        let mut fresh = PartitionData {
            partition_index: read.partition_index,
            aborted_transactions: None,
            ..Default::default()
        };
        if apply_epoch_checks(
            &image,
            &read.topic_name,
            read.partition_index,
            &request,
            ReadRole {
                partition: &part,
                required_leader: required_leader(
                    read.fetch_only_leader,
                    broker.config.node_id,
                    &part,
                ),
                // Kafka's `DelayedFetch.tryComplete` checks the leader again
                // and does not check the replica id again.
                assigned_follower: true,
                log_dir_offline: broker.log_dir_status.is_offline(&part.log_dir.load()),
            },
            &mut fresh,
        )
        .await
        {
            read.out = fresh;
            fenced = true;
        }
    }
    fenced
}

/// Whether any partition's latest read ended in an error.
///
/// This is Kafka's `errorReadingData`: `ReplicaManager.fetchMessages` never
/// parks a fetch whose first read pass produced an error, and
/// `DelayedFetch.tryComplete` force-completes a parked one as soon as a
/// partition's offset falls off the log. Retention or truncation moving a
/// requested offset out of range while the fetch waits is exactly that case,
/// and the re-read that discovers it contributes zero bytes -- so without this
/// the fetch would sit on its `min_bytes` floor until `max_wait_ms` expired
/// with the error already in hand.
///
/// A partition the cold tier answered has had its `OFFSET_OUT_OF_RANGE`
/// cleared by `serve_from_cold_tier`, so a tiered read still accumulates and
/// keeps waiting.
fn has_read_error(pending: &[PendingRead]) -> bool {
    pending
        .iter()
        .any(|read| read.out.error_code != codes::NONE)
}

/// Read the one partition whose notifier fired, replacing both its response
/// and its contribution to the accumulated byte count.
async fn reread_woken(
    broker: &Broker,
    pending: &mut [PendingRead],
    index: usize,
    state: &mut LongPollState,
    phases: &RequestPhases,
) {
    let Some(read) = pending.get_mut(index) else {
        return;
    };
    let Some(part) = read.partition.clone() else {
        return;
    };
    // A partition the cold tier already answered keeps that answer. The local
    // log does not hold the offset -- that is what sent it to the tier in the
    // first place -- so a re-read would trade a served batch for another
    // object-store round trip.
    if state.cold_served[index] {
        return;
    }
    // `do_read` fills `aborted_transactions` only for a `read_committed`
    // consumer. Every other re-read leaves it null, as Kafka's `LocalLog.read`
    // does outside `TXN_COMMITTED` isolation.
    read.out = PartitionData {
        partition_index: read.partition_index,
        aborted_transactions: None,
        ..Default::default()
    };
    // A re-read replaces this entry's bytes rather than adding to them, so
    // its old contribution is first given back to the response budget: the
    // budget tracks what the response currently holds, not a cumulative
    // total of every read this partition has ever produced.
    state.bytes[index] = 0;
    state.remaining_response_bytes = state.response_max_bytes.saturating_sub(state.total());
    let requested_max_bytes = read.max_bytes;
    let read_budget = state.partition_read_budget(requested_max_bytes);
    let min_one_batch = state.total() == 0;
    // Time the re-read so its duration accumulates into the same
    // per-partition CPU counter as the first pass (wall-clock delta;
    // see the first-pass comment for why this replaces TaskMonitor).
    let read_start = std::time::Instant::now();
    let bytes = do_read(
        &part,
        planned_read_request(broker, read, read_budget, state.sendfile_capable),
        &broker.log_dir_status,
        &mut read.out,
    )
    .await;
    let micros = u64::try_from(read_start.elapsed().as_micros()).unwrap_or(u64::MAX);
    read.cpu_micros = read.cpu_micros.saturating_add(micros);
    // The re-read is local work like the first pass, so it accumulates on
    // the same phase.
    phases.add_local(read_start.elapsed());

    // The partition may have aged past this offset while the fetch was parked,
    // in which case the cold tier is where the records now are. Loan the
    // capped budget to it the same way the first pass does, and give the
    // original per-partition request back afterward.
    read.max_bytes = read_budget;
    let cold = serve_from_cold_tier(broker, read, &part, phases).await;
    read.max_bytes = requested_max_bytes;
    state.record_read(index, read, ReadAccounting {
        local_bytes: bytes,
        cold_bytes: cold,
        budget: read_budget,
        allow_first_batch_overrun: min_one_batch,
    });
}

/// Serve `read`'s offset out of the cold tier when the local log no longer
/// holds it, charging the object-store round trip to the request's remote
/// phase.
///
/// A KIP-405 tiered read and a diskless WAL cold read are both a network round
/// trip to an object store rather than work on this broker's own log, so they
/// belong beside the long poll and the Produce `acks=all` gate and not beside
/// `do_read`. Kafka accounts them the same way: a fetch that misses the local
/// log becomes a `DelayedRemoteFetch` in the purgatory, and the purgatory wait
/// is what `RequestMetrics.RemoteTimeMs` measures.
///
/// Returns the bytes the cold tier served, and zero when the local log already
/// answered, when no tier holds the offset, or when the tier failed the read
/// -- a failure the remote path has already answered with
/// `UNKNOWN_SERVER_ERROR`, so the `OFFSET_OUT_OF_RANGE` gate above is what
/// keeps the failed partition from being handed to a second tier. A read the
/// local log answered charges nothing at all: the clock is only read once the
/// fallback is entered, so a cluster with no tiered or diskless topic sees an
/// unchanged remote phase.
/// Turns a read the response budget had no room for into the row an empty
/// read produces: the watermarks and log start the read just filled in, no
/// records, and the aborted-transaction list an empty read carries.
///
/// Kafka reads nothing for such a partition (`maxBytes` 0, no
/// `minOneMessage`), so the aborted-transaction range it looks up ends at the
/// fetch offset and holds nothing. The local read here did run, because
/// `Log::read_raw` serves a batch even at a zero budget, and its aborted
/// transactions describe records this row no longer carries. They are
/// replaced with what `do_read` reports for an empty read: an empty list for
/// a `read_committed` consumer, and `None` otherwise.
fn discard_to_metadata_only(read: &mut PendingRead) {
    read.out.records = None;
    read.out.aborted_transactions = (read.read_committed && !read.is_follower_fetch).then(Vec::new);
}

async fn serve_from_cold_tier(
    broker: &Broker,
    read: &mut PendingRead,
    part: &Partition,
    phases: &RequestPhases,
) -> usize {
    if read.out.error_code != codes::OFFSET_OUT_OF_RANGE {
        return 0;
    }
    let started = std::time::Instant::now();
    let served = match try_remote_read(broker, read, part).await {
        Some(remote_bytes) => remote_bytes,
        None => try_diskless_read(broker, read, part).await.unwrap_or(0),
    };
    phases.add_remote(started.elapsed());
    served
}

#[cfg(test)]
mod tests {
    use {
        super::super::{plan::FetchMode, response::GroupedResponses},
        crate::{
            broker::{Broker, spawn_partition},
            codes::{NONE, OFFSET_OUT_OF_RANGE},
            diskless::wal_index::WalFlushRecord,
            handlers::{fetch::test_support::append_epoch_batches, test_support::partition},
            metrics::RequestPhases,
            partition::{Partition, test_support::test_partition},
            producer_state::ProducerState,
            test_support::{StandalonePartitionSetup, StorageMode},
        },
        assert2::assert,
        bytes::{Bytes, BytesMut},
        krabka_ids::Offset,
        krabka_log::{Log, LogConfig},
        krabka_protocol::{
            primitives::uuid::Uuid as WireUuid,
            records::{Record, RecordBatch, RecordsPayload},
        },
        object_store::{ObjectStoreExt as _, PutPayload, path::Path},
    };

    fn wide_partition_fetch_request() -> super::EffectivePartition {
        super::EffectivePartition {
            partition: 0,
            current_leader_epoch: 0,
            last_fetched_epoch: -1,
            fetch_offset: 0,
            log_start_offset: -1,
            partition_max_bytes: 1 << 20,
        }
    }

    /// Table over a run of per-partition reads (each partition's own
    /// `partition_max_bytes` and the bytes its read actually served) against
    /// the response budget, to the `max_bytes` each successive partition's
    /// read is given and what is left of the budget afterward (#869). The
    /// first partition may still exceed the budget -- this broker's log layer
    /// guarantees at least one batch -- but every later partition's own cap
    /// is bounded by what the response has left, however large its own
    /// `partition_max_bytes` asked for.
    #[test]
    fn partition_read_budget_runs_down_across_a_response() {
        struct Step {
            partition_max_bytes: i32,
            /// What this partition's read is given, computed before it runs.
            want_given: i32,
            /// Bytes the read actually serves, which charges the budget.
            served: usize,
        }
        struct Case {
            name: &'static str,
            response_budget: usize,
            steps: Vec<Step>,
            want_remaining: usize,
        }

        let cases = [
            Case {
                name: "each partition's own cap already fits under the budget",
                response_budget: 10_000,
                steps: vec![
                    Step {
                        partition_max_bytes: 1_000,
                        want_given: 1_000,
                        served: 900,
                    },
                    Step {
                        partition_max_bytes: 1_000,
                        want_given: 1_000,
                        served: 800,
                    },
                ],
                want_remaining: 10_000 - 900 - 800,
            },
            Case {
                name: "the first partition alone is over the whole budget",
                response_budget: 100,
                steps: vec![Step {
                    partition_max_bytes: 10_000,
                    want_given: 100,
                    served: 4_096,
                }],
                want_remaining: 0,
            },
            Case {
                name: "a later partition's own cap is capped by what is left, \
                       however large it asked for",
                response_budget: 1_000,
                steps: vec![
                    Step {
                        partition_max_bytes: 10_000,
                        want_given: 1_000,
                        served: 700,
                    },
                    Step {
                        partition_max_bytes: 10_000,
                        want_given: 300,
                        served: 300,
                    },
                ],
                want_remaining: 0,
            },
            Case {
                name: "a partition whose budget is already spent still reads, at zero",
                response_budget: 500,
                steps: vec![
                    Step {
                        partition_max_bytes: 10_000,
                        want_given: 500,
                        served: 500,
                    },
                    Step {
                        partition_max_bytes: 10_000,
                        want_given: 0,
                        served: 0,
                    },
                ],
                want_remaining: 0,
            },
        ];

        for case in cases {
            let mut state = super::LongPollState {
                min_bytes: 0,
                max_wait_ms: 0,
                sendfile_capable: false,
                bytes: Vec::new(),
                cold_served: Vec::new(),
                remaining_response_bytes: case.response_budget,
                response_max_bytes: case.response_budget,
            };
            for (i, step) in case.steps.iter().enumerate() {
                let given = state.partition_read_budget(step.partition_max_bytes);
                assert!(
                    given == step.want_given,
                    "{}: step {i}: given {given}, want {}",
                    case.name,
                    step.want_given
                );
                state.charge(step.served);
            }
            assert!(
                state.remaining_response_bytes == case.want_remaining,
                "{}: remaining {}, want {}",
                case.name,
                state.remaining_response_bytes,
                case.want_remaining
            );
        }
    }

    fn open_fixture_log(dir: &std::path::Path, topic: &str) -> Log {
        let part_dir = dir.join(format!("{topic}-0"));
        std::fs::create_dir_all(&part_dir).expect("partition dir");
        Log::open(&part_dir, LogConfig::default()).expect("open partition log")
    }

    fn spawn_fixture_partition(
        broker: &Broker,
        dir: &std::path::Path,
        log: Log,
        setup: StandalonePartitionSetup<'_>,
    ) -> std::sync::Arc<Partition> {
        spawn_partition(
            setup.topic.to_owned(),
            setup.partition,
            dir.to_path_buf(),
            log,
            broker.log_dir_status.clone(),
            broker.producer_state.clone(),
            setup.storage == StorageMode::Diskless,
        )
    }

    /// A local partition read planned against `Broker`'s "leader with no
    /// replication target installed" default, for a topic that already holds
    /// one appended batch at offset 0.
    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct NonemptyPartitionSetup<'a> {
        #[default("orders")]
        topic: &'a str,
        #[default(b"v")]
        payload: &'static [u8],
    }

    #[bon::builder]
    async fn nonempty_local_partition(
        broker: &Broker,
        dir: &std::path::Path,
        #[builder(default = "orders")] topic: &'_ str,
        #[builder(default = b"v")] payload: &'static [u8],
    ) -> std::sync::Arc<Partition> {
        let setup = NonemptyPartitionSetup { topic, payload };
        let NonemptyPartitionSetup { topic, payload } = setup;
        let part = spawn_fixture_partition(
            broker,
            dir,
            open_fixture_log(dir, topic),
            StandalonePartitionSetup {
                topic,
                ..Default::default()
            },
        );
        part.install_replication_target(None, broker.config.node_id.0, 0)
            .await;
        part.produce_batch(sized_batch(payload))
            .await
            .expect("append the partition's one batch");
        part
    }

    /// A fetch over several nonempty partitions where the first one's single
    /// batch already exceeds the whole response budget: `Log::read_raw`
    /// guarantees at least one complete batch however small a budget it is
    /// given, so the first partition's read can run over on its own. Kafka
    /// spends that progress guarantee on exactly one partition per response;
    /// a second nonempty partition read once the budget is gone gets a
    /// metadata-only row -- its watermarks, no records -- rather than a
    /// second oversized batch, or a fetch over many nonempty partitions would
    /// exceed `max_bytes` by an amount proportional to the partition count
    /// (PR #1135, finding 1).
    #[tokio::test]
    async fn only_the_first_nonempty_partition_gets_the_over_budget_batch() {
        const TOPIC_A: &str = "budget-progress-a";
        const TOPIC_B: &str = "budget-progress-b";
        const PAYLOAD: &[u8; 512] = &[b'x'; 512];

        broker_fixture!(
            (broker_handle, dir, broker),
            crate::handlers::test_support::start_broker()
        );

        let part_a = nonempty_local_partition()
            .broker(&broker)
            .dir(dir.path())
            .topic(TOPIC_A)
            .payload(PAYLOAD)
            .call()
            .await;
        let part_b = nonempty_local_partition()
            .broker(&broker)
            .dir(dir.path())
            .topic(TOPIC_B)
            .payload(PAYLOAD)
            .call()
            .await;

        let request = wide_partition_fetch_request();
        let pending = vec![
            planned_follower(TOPIC_A, &part_a, &request, crate::codes::NONE),
            planned_follower(TOPIC_B, &part_b, &request, crate::codes::NONE),
        ];
        // A response budget far smaller than either partition's one batch:
        // the first partition's read still serves it whole, which alone
        // spends the entire budget before the second partition is read.
        let topics = read_with_tiny_budget(&broker, pending).await;

        let served_a = &topics[0].partitions[0];
        let served_b = &topics[1].partitions[0];
        assert!(served_base_offsets(served_a) == vec![0]);
        assert!(served_a.high_watermark == 1);
        // The second partition still has data of its own, but the response
        // budget was already spent on the first: it gets its watermarks and
        // no records, not a second oversized batch.
        assert!(served_b.records.is_none());
        assert!(served_b.high_watermark == 1);
        assert!(served_b.error_code == crate::codes::NONE);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn a_fitting_first_batch_spends_the_exception_for_initial_and_replacement_reads() {
        broker_fixture!(
            (broker_handle, dir, broker),
            crate::handlers::test_support::start_broker()
        );
        let first = nonempty_local_partition()
            .broker(&broker)
            .dir(dir.path())
            .topic("budget-fitting-first")
            .payload(b"small")
            .call()
            .await;
        let second = nonempty_local_partition()
            .broker(&broker)
            .dir(dir.path())
            .topic("budget-oversized-second")
            .payload(&[b'x'; 512])
            .call()
            .await;
        let request = wide_partition_fetch_request();
        let pending = || {
            vec![
                planned_follower("budget-fitting-first", &first, &request, NONE),
                planned_follower("budget-oversized-second", &second, &request, NONE),
            ]
        };
        let phases = RequestPhases::default();
        let GroupedResponses {
            responses: topics, ..
        } = super::execute_pending_reads(&broker, pending(), 0, 150, 0, false, &phases).await;
        let first_bytes = topics[0].partitions[0]
            .records
            .as_ref()
            .unwrap()
            .payload_len();
        assert!(first_bytes > 0 && first_bytes < 150);
        assert!(topics[1].partitions[0].records.is_none());
        let mut pending = pending();
        let mut state = super::LongPollState {
            min_bytes: 150,
            max_wait_ms: 0,
            sendfile_capable: false,
            bytes: vec![first_bytes, 0],
            cold_served: vec![false; 2],
            remaining_response_bytes: 150 - first_bytes,
            response_max_bytes: 150,
        };
        super::reread_woken(&broker, &mut pending, 1, &mut state, &phases).await;
        assert!(pending[1].out.records.is_none());
        assert!(state.total() == first_bytes);
        assert!(state.remaining_response_bytes == 150 - first_bytes);
        broker_handle.shutdown().await;
    }

    /// A local partition and the temporary directory that holds its log.
    struct ConsumerPartition {
        partition: std::sync::Arc<Partition>,
        dir: tempfile::TempDir,
    }

    /// A local partition whose log holds `batches`, with its high watermark
    /// at the log end, read as a consumer (`fetch_only_leader` off).
    async fn consumer_partition(mut batches: Vec<RecordBatch>) -> ConsumerPartition {
        let (part, dir) = test_partition(std::sync::Arc::new(tokio::sync::Notify::new()));
        let end = {
            let mut log = part.log.lock().expect("log mutex poisoned");
            for batch in &mut batches {
                log.append(batch).expect("append a batch under test");
            }
            log.log_end_offset()
        };
        part.replica_state.lock().await.hw = end;
        ConsumerPartition {
            partition: std::sync::Arc::new(part),
            dir,
        }
    }

    /// A consumer read of `topic`'s partition 0 from offset 0.
    fn consumer_read(
        topic: &str,
        part: &std::sync::Arc<Partition>,
        read_committed: bool,
    ) -> super::PendingRead {
        super::PendingRead {
            topic_name: topic.into(),
            topic_id: WireUuid::ZERO,
            partition_index: 0,
            current_leader_epoch: -1,
            last_fetched_epoch: -1,
            fetch_offset: 0,
            max_bytes: 1 << 20,
            read_committed,
            is_follower_fetch: false,
            fetch_only_leader: false,
            follower_position: None,
            partition: Some(std::sync::Arc::clone(part)),
            out: super::PartitionData {
                partition_index: 0,
                ..Default::default()
            },
            cpu_micros: 0,
        }
    }

    /// A transaction from `PRODUCER_ID` that one data batch opens and an
    /// abort marker closes, so a `read_committed` read over it reports the
    /// aborted range starting at offset 0.
    fn aborted_transaction() -> Vec<RecordBatch> {
        use krabka_protocol::records::Attributes;

        const PRODUCER_ID: i64 = 1000;
        // Control record key: version 0, marker type 0 (abort), big-endian.
        let abort_key = [0u8; 4];
        let mut abort_value = [0u8; 6];
        abort_value[2..6].copy_from_slice(&17i32.to_be_bytes());
        vec![
            RecordBatch {
                producer_id: PRODUCER_ID,
                attributes: Attributes::default().with_transactional(true),
                records: vec![Record {
                    value: Some(Bytes::from_static(b"in an aborted transaction")),
                    ..Default::default()
                }],
                ..Default::default()
            },
            RecordBatch {
                producer_id: PRODUCER_ID,
                attributes: Attributes::default()
                    .with_transactional(true)
                    .with_control(true),
                records: vec![Record {
                    key: Some(Bytes::copy_from_slice(&abort_key)),
                    value: Some(Bytes::copy_from_slice(&abort_value)),
                    ..Default::default()
                }],
                ..Default::default()
            },
        ]
    }

    /// Independent null-versus-empty aborted-transaction expectations for both isolation modes.
    macro_rules! aborted_isolation_cases {
        () => {
            [
                ("read_uncommitted", false, None),
                ("read_committed", true, Some(Vec::new())),
            ]
        };
    }

    /// A partition the response budget has no room for gets the row an
    /// empty read produces, including the aborted-transaction list: Kafka
    /// reads nothing for it, so the aborted range it looks up ends at the
    /// fetch offset. The local read behind it did run and did find an
    /// aborted transaction in range, and that list must not survive with
    /// the records it described gone. A `read_committed` consumer gets the
    /// empty list `do_read` reports for an empty read; a `read_uncommitted`
    /// one gets `None`.
    #[tokio::test]
    async fn a_metadata_only_row_carries_an_empty_reads_aborted_transactions() {
        let cases = aborted_isolation_cases!();

        broker_fixture!(
            (broker_handle, _dir, broker),
            crate::handlers::test_support::start_broker()
        );

        for (name, read_committed, want_aborted) in cases {
            // The first partition spends the response's one progress
            // exception; the second is read at a zero budget after it.
            let ConsumerPartition {
                partition: spender,
                dir: _spender_dir,
            } = consumer_partition(vec![sized_batch(&[b'x'; 512])]).await;
            let ConsumerPartition {
                partition: aborted,
                dir: _aborted_dir,
            } = consumer_partition(aborted_transaction()).await;
            let pending = vec![
                consumer_read("spender", &spender, read_committed),
                consumer_read("aborted", &aborted, read_committed),
            ];

            let topics = read_with_tiny_budget(&broker, pending).await;

            assert!(
                served_base_offsets(&topics[0].partitions[0]) == vec![0],
                "{name}"
            );
            assert!(
                topics[1].partitions[0]
                    == super::PartitionData {
                        partition_index: 0,
                        error_code: crate::codes::NONE,
                        high_watermark: 2,
                        last_stable_offset: 2,
                        log_start_offset: 0,
                        aborted_transactions: want_aborted,
                        records: None,
                        ..Default::default()
                    },
                "{name}"
            );
        }
        broker_handle.shutdown().await;
    }

    /// A partition whose local log holds one record and then has it trimmed
    /// away, so a fetch at offset 0 falls straight through to the cold tier
    /// -- the shape [`serve_from_cold_tier`] exists for.
    fn evicted_diskless_partition(
        broker: &Broker,
        dir: &std::path::Path,
        topic: &str,
    ) -> std::sync::Arc<Partition> {
        let mut log = open_fixture_log(dir, topic);
        log.append(&mut RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"evicted")),
                ..Default::default()
            }],
            ..Default::default()
        })
        .expect("append the batch retention then evicts");
        let limit = log.log_end_offset();
        log.trim_to_offset(limit)
            .expect("trim the whole log away from under the fetch");
        spawn_fixture_partition(broker, dir, log, StandalonePartitionSetup {
            topic,
            storage: StorageMode::Diskless,
            ..Default::default()
        })
    }

    /// One encoded batch of a single record.
    fn encoded_batch(base_offset: Offset, payload: Vec<u8>) -> Bytes {
        let mut buf = BytesMut::new();
        RecordBatch {
            base_offset: base_offset.0,
            records: vec![Record {
                value: Some(Bytes::from(payload)),
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode(&mut buf)
        .expect("encode a diskless WAL batch");
        buf.freeze()
    }

    /// KIP-405/diskless cold reads are bound by the response's remaining
    /// byte budget too. A cold first batch spends the first-nonempty exception
    /// whether it fits or exceeds the response cap. The second partition's
    /// guaranteed first batch cannot escape the remaining zero or one byte
    /// budget (PR #1135, finding 2).
    #[tokio::test]
    async fn cold_tier_reads_are_capped_by_the_response_budget_left_after_an_earlier_partition() {
        const TOPIC_A: &str = "budget-cold-a";
        const TOPIC_B: &str = "budget-cold-b";

        let object_dir = tempfile::tempdir().expect("object tempdir");
        broker_fixture!(
            (broker_handle, dir, broker),
            local_remote_storage(object_dir)
        );

        let topic_a = uuid::Uuid::from_u128(0xA0);
        let topic_b = uuid::Uuid::from_u128(0xB0);
        let part_a = evicted_diskless_partition(&broker, dir.path(), TOPIC_A);
        let part_b = evicted_diskless_partition(&broker, dir.path(), TOPIC_B);

        // Partition A: one cold batch big enough to spend the whole response
        // budget by itself.
        let batch_a = encoded_batch(Offset(0), vec![b'a'; 300]);
        let read_handle = broker.diskless_read.as_ref().expect("diskless read handle");
        read_handle
            .object_store()
            .put(
                &Path::from("diskless-wal/a"),
                PutPayload::from(batch_a.clone()),
            )
            .await
            .expect("put partition a's cold run");
        read_handle.index.lock().await.apply(&WalFlushRecord {
            object_key: "diskless-wal/a".into(),
            format_version: WalFlushRecord::FORMAT_VERSION,
            entries: vec![crate::diskless::wal_index::WalIndexEntry {
                topic_id: topic_a,
                partition: 0,
                first_offset: 0,
                last_offset: 0,
                byte_start: 0,
                byte_len: u32::try_from(batch_a.len()).expect("small run"),
                max_timestamp_ms: 0,
            }],
        });

        // Partition B: two adjacent cold batches in one object. A read given
        // the full, uncapped per-partition request would extend across both;
        // a read capped to the near-zero budget left after A must stop after
        // the first.
        let first = encoded_batch(Offset(0), b"first".to_vec());
        let second = encoded_batch(Offset(1), vec![b'b'; 300]);
        let mut combined = BytesMut::new();
        combined.extend_from_slice(&first);
        combined.extend_from_slice(&second);
        let combined = combined.freeze();
        read_handle
            .object_store()
            .put(&Path::from("diskless-wal/b"), PutPayload::from(combined))
            .await
            .expect("put partition b's cold run");
        read_handle.index.lock().await.apply(&WalFlushRecord {
            object_key: "diskless-wal/b".into(),
            format_version: WalFlushRecord::FORMAT_VERSION,
            entries: vec![
                crate::diskless::wal_index::WalIndexEntry {
                    topic_id: topic_b,
                    partition: 0,
                    first_offset: 0,
                    last_offset: 0,
                    byte_start: 0,
                    byte_len: u32::try_from(first.len()).expect("small run"),
                    max_timestamp_ms: 0,
                },
                crate::diskless::wal_index::WalIndexEntry {
                    topic_id: topic_b,
                    partition: 0,
                    first_offset: 1,
                    last_offset: 1,
                    byte_start: u64::try_from(first.len()).expect("small run"),
                    byte_len: u32::try_from(second.len()).expect("small run"),
                    max_timestamp_ms: 0,
                },
            ],
        });

        let request_for = |topic_id: WireUuid| super::PendingRead {
            topic_name: String::new(),
            topic_id,
            partition_index: 0,
            current_leader_epoch: 0,
            last_fetched_epoch: -1,
            fetch_offset: 0,
            max_bytes: 1 << 20,
            read_committed: false,
            is_follower_fetch: false,
            fetch_only_leader: false,
            follower_position: None,
            partition: None,
            out: super::PartitionData {
                partition_index: 0,
                ..Default::default()
            },
            cpu_micros: 0,
        };
        let pending = || {
            vec![
                super::PendingRead {
                    topic_name: TOPIC_A.into(),
                    topic_id: WireUuid(topic_a.into_bytes()),
                    partition: Some(std::sync::Arc::clone(&part_a)),
                    ..request_for(WireUuid(topic_a.into_bytes()))
                },
                super::PendingRead {
                    topic_name: TOPIC_B.into(),
                    topic_id: WireUuid(topic_b.into_bytes()),
                    partition: Some(std::sync::Arc::clone(&part_b)),
                    ..request_for(WireUuid(topic_b.into_bytes()))
                },
            ]
        };

        let phases = RequestPhases::default();
        // Both an oversized first batch and a fitting first batch spend the
        // response's first-nonempty exception. B cannot use it for the whole
        // batch its WAL source returns beyond the remaining byte budget.
        for budget in [1, batch_a.len() + 1] {
            let GroupedResponses {
                responses: topics, ..
            } = super::execute_pending_reads(&broker, pending(), 0, budget, 0, false, &phases)
                .await;
            let served_a = &topics[0].partitions[0];
            let served_b = &topics[1].partitions[0];
            assert!(served_a.error_code == NONE);
            assert!(
                served_a.records.as_ref().map(RecordsPayload::payload_len) == Some(batch_a.len())
            );
            assert!(served_b.error_code == NONE);
            assert!(served_b.records.is_none(), "response budget {budget}");
        }
        broker_handle.shutdown().await;
    }

    /// A cold read is a round trip to an object store, so it belongs to the
    /// remote phase. Before this was charged, a tiered or diskless fetch could
    /// spend its whole latency in the object store while both phase histograms
    /// stayed near zero and the time fell into the unnamed remainder the
    /// rustdoc describes as decode, authorization and encode.
    #[tokio::test]
    async fn cold_tier_fallback_charges_the_object_store_read_to_the_remote_phase() {
        let object_dir = tempfile::tempdir().expect("object tempdir");
        broker_fixture!(
            (broker_handle, dir, broker),
            local_remote_storage(object_dir)
        );

        let topic_id = uuid::Uuid::from_u128(0xC01D);
        let mut flushed = BytesMut::new();
        RecordBatch {
            base_offset: 0,
            records: vec![Record {
                value: Some(Bytes::from_static(b"cold")),
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode(&mut flushed)
        .expect("encode flushed batch");
        let flushed = flushed.freeze();
        let read_handle = broker.diskless_read.as_ref().expect("diskless read handle");
        read_handle
            .object_store()
            .put(
                &Path::from("diskless-wal/cold"),
                PutPayload::from(flushed.clone()),
            )
            .await
            .expect("put flushed run");
        read_handle.index.lock().await.apply(&WalFlushRecord {
            object_key: "diskless-wal/cold".into(),
            format_version: WalFlushRecord::FORMAT_VERSION,
            entries: vec![crate::diskless::wal_index::WalIndexEntry {
                topic_id,
                partition: 0,
                first_offset: 0,
                last_offset: 0,
                byte_start: 0,
                byte_len: u32::try_from(flushed.len()).expect("small run"),
                max_timestamp_ms: 0,
            }],
        });

        let part = partition(&broker, dir.path(), StandalonePartitionSetup {
            topic: "cold",
            storage: StorageMode::Diskless,
            ..Default::default()
        });
        let mut pending = super::PendingRead {
            topic_name: "cold".into(),
            topic_id: WireUuid(topic_id.into_bytes()),
            partition_index: 0,
            current_leader_epoch: 0,
            last_fetched_epoch: -1,
            fetch_offset: 0,
            max_bytes: i32::try_from(flushed.len()).expect("small run"),
            read_committed: false,
            is_follower_fetch: false,
            fetch_only_leader: false,
            follower_position: None,
            partition: Some(std::sync::Arc::clone(&part)),
            out: super::PartitionData {
                error_code: OFFSET_OUT_OF_RANGE,
                high_watermark: 1,
                log_start_offset: 1,
                ..Default::default()
            },
            cpu_micros: 0,
        };

        let phases = RequestPhases::default();
        let served = super::serve_from_cold_tier(&broker, &mut pending, &part, &phases).await;

        assert!(served == flushed.len());
        assert!(pending.out.error_code == crate::codes::NONE);
        assert!(phases.remote_seconds() > 0.0);
        // The object-store trip is charged to exactly one phase: the local
        // phase belongs to `do_read`, which this call does not make.
        assert!(phases.local_seconds() < 1.0e-9);

        // A partition the local log answered never enters the fallback, so a
        // cluster with no cold tier sees an unchanged remote phase.
        pending.out.error_code = NONE;
        let local_only = RequestPhases::default();
        let served = super::serve_from_cold_tier(&broker, &mut pending, &part, &local_only).await;

        assert!(served == 0);
        assert!(local_only.remote_seconds() < 1.0e-9);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn long_poll_reread_rechecks_follower_epoch() {
        const TOPIC: &str = "long-poll-epoch";

        // A follower fetch reads only from the leader.
        broker_fixture!(
            (broker_handle, dir, broker, part),
            local_follower_partition(TOPIC)
        );
        let mut pending = [follower_read(TOPIC, &part, NONE)];

        part.install_leader_change(1, 1).await;
        part.produce_batch(RecordBatch {
            partition_leader_epoch: 1,
            records: vec![Record {
                value: Some(Bytes::from_static(b"new-epoch")),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("append new-epoch record");

        let phases = RequestPhases::default();
        let mut state = state_for().pending(&pending).call();
        super::long_poll_then_reread(&broker, &mut pending, Vec::new(), &mut state, &phases).await;

        assert!(pending[0].out.error_code == crate::codes::FENCED_LEADER_EPOCH);
        // A fenced-epoch row never touches the log, so it carries the -1
        // sentinels of a refused read (#872/#873): `records` is present but
        // empty, not null.
        assert!(
            pending[0].out.records
                == Some(krabka_protocol::records::RecordsPayload::Raw(Bytes::new()))
        );
        broker_handle.shutdown().await;
    }

    /// Retention or truncation moving a requested offset off the log while
    /// the fetch is parked is Kafka's `errorReadingData`: `fetchMessages`
    /// never parks on it and `DelayedFetch.tryComplete` force-completes for
    /// it, whatever the accumulated byte count is. A fetch that only ever
    /// checked its epochs and its `min_bytes` floor would instead hold the
    /// error until `max_wait_ms` expired.
    #[tokio::test]
    async fn a_partition_error_completes_the_long_poll() {
        const TOPIC: &str = "long-poll-error";

        // A follower fetch reads only from the leader.
        broker_fixture!(
            (broker_handle, dir, broker, part),
            local_follower_partition(TOPIC)
        );
        let mut pending = [follower_read(TOPIC, &part, OFFSET_OUT_OF_RANGE)];

        // Nothing will ever append, and the floor is unreachable: only the
        // error can end this wait before its 30-second deadline.
        let waits = super::arm_waits(&pending);
        check_blocked_poll_completes()
            .broker(&broker)
            .pending(&mut pending)
            .waits(waits)
            .failure("the error completes the long poll")
            .call()
            .await;

        assert!(pending[0].out.error_code == crate::codes::OFFSET_OUT_OF_RANGE);
        broker_handle.shutdown().await;
    }

    /// Kafka's `DelayedFetch.tryComplete` completes a parked follower fetch
    /// when the partition's leader changes. A follower fetch parked below its
    /// `min_bytes` floor on a partition that then moves to another leader
    /// answers `NOT_LEADER_OR_FOLLOWER` at once, not after `max_wait_ms`.
    #[tokio::test]
    async fn a_leader_change_completes_a_parked_follower_fetch() {
        const TOPIC: &str = "long-poll-leader-change";

        broker_fixture!((broker_handle, dir, broker, part), local_partition(TOPIC));
        let node_id = broker.config.node_id.0;
        part.install_replication_target(None, node_id, 0).await;
        let request = super::EffectivePartition {
            partition: 0,
            current_leader_epoch: -1,
            last_fetched_epoch: -1,
            fetch_offset: 0,
            log_start_offset: -1,
            partition_max_bytes: 1024,
        };
        let mut pending = [planned_follower(TOPIC, &part, &request, NONE)];

        // What `execute_pending_reads` arms around its first pass: the append
        // waiters before it, the leadership waiters after it.
        let mut waits = super::arm_waits(&pending);
        waits.extend(super::arm_leadership_waits(&pending));
        let demoted = std::sync::Arc::clone(&part);
        let demote = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            demoted
                .install_replication_target(None, node_id + 1, 0)
                .await;
        });
        check_blocked_poll_completes()
            .broker(&broker)
            .pending(&mut pending)
            .waits(waits)
            .failure("the leader change completes the long poll")
            .call()
            .await;
        demote.await.expect("demote task");

        assert!(
            pending[0].out
                == super::super::plan::refused_read(0, crate::codes::NOT_LEADER_OR_FOLLOWER)
        );
        broker_handle.shutdown().await;
    }

    /// A long-poll re-read is a fresh pass through Kafka's `LocalLog.read`,
    /// which returns an aborted-transaction list only under `TXN_COMMITTED`
    /// isolation. A `read_uncommitted` re-read carries null, not the
    /// generated default's empty list, and a `read_committed` one carries
    /// the (here empty) list its read found.
    #[tokio::test]
    async fn a_reread_carries_aborted_transactions_only_for_read_committed() {
        broker_fixture!(
            (broker_handle, _dir, broker),
            crate::handlers::test_support::start_broker()
        );

        for (name, read_committed, want_aborted) in aborted_isolation_cases!() {
            let ConsumerPartition {
                partition: part,
                dir: _part_dir,
            } = consumer_partition(Vec::new()).await;
            let mut pending = [consumer_read("reread", &part, read_committed)];
            let phases = RequestPhases::default();
            let mut state = state_for().pending(&pending).call();
            super::reread_woken(&broker, &mut pending, 0, &mut state, &phases).await;

            assert!(
                pending[0].out
                    == super::PartitionData {
                        partition_index: 0,
                        error_code: crate::codes::NONE,
                        high_watermark: 0,
                        last_stable_offset: 0,
                        log_start_offset: 0,
                        aborted_transactions: want_aborted,
                        records: None,
                        ..Default::default()
                    },
                "{name}"
            );
        }
        broker_handle.shutdown().await;
    }

    /// An epoch recheck that finds the fetch diverging replaces the row with
    /// Kafka's diverging-epoch answer, which `Partition.readRecords` builds
    /// from `FetchDataInfo.empty`: the live bounds, the diverging epoch, and
    /// null aborted transactions, not the generated default's empty list.
    #[tokio::test]
    async fn a_diverging_recheck_carries_null_aborted_transactions() {
        const TOPIC: &str = "recheck-diverge";

        broker_fixture!((broker_handle, dir, broker, part), local_partition(TOPIC));
        {
            let mut log = part.log.lock().expect("log mutex poisoned");
            append_epoch_batches(&mut log, &[0, 0, 1, 1]);
        }
        // Epoch 3 starts at offset 4, so epoch 2, which this log never saw,
        // ends there.
        part.install_local_leadership(&ProducerState::new(), None, 1, 3)
            .await
            .expect("promote");
        let mut pending = [super::PendingRead {
            last_fetched_epoch: 2,
            fetch_offset: 6,
            ..consumer_read(TOPIC, &part, false)
        }];

        assert!(super::revalidate_epochs(&broker, &mut pending).await);
        assert!(
            pending[0].out
                == super::PartitionData {
                    partition_index: 0,
                    error_code: crate::codes::NONE,
                    high_watermark: 0,
                    last_stable_offset: 0,
                    log_start_offset: 0,
                    aborted_transactions: None,
                    diverging_epoch: krabka_protocol::owned::fetch_response::EpochEndOffset {
                        epoch: 1,
                        end_offset: 4,
                        ..Default::default()
                    },
                    ..Default::default()
                }
        );
        broker_handle.shutdown().await;
    }

    #[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
    struct FetchMinimumBytes(usize);
    #[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
    struct FetchWaitMillis(i32);
    #[derive(Clone, Copy, Default)]
    struct PollStateSetup {
        minimum: FetchMinimumBytes,
        wait: FetchWaitMillis,
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct BlockedPollSetup<'a> {
        #[default(FetchMinimumBytes(4096))]
        minimum: FetchMinimumBytes,
        #[default(FetchWaitMillis(30_000))]
        wait: FetchWaitMillis,
        #[default("the long poll completes promptly")]
        failure: &'a str,
    }

    #[bon::builder]
    async fn check_blocked_poll_completes(
        broker: &Broker,
        pending: &mut [super::PendingRead],
        waits: Vec<super::WaitFut>,
        #[builder(into, default = FetchMinimumBytes (4096))] minimum: FetchMinimumBytes,
        #[builder(into, default = FetchWaitMillis (30_000))] wait: FetchWaitMillis,
        #[builder(default = "the long poll completes promptly")] failure: &'_ str,
    ) {
        let setup = BlockedPollSetup {
            minimum,
            wait,
            failure,
        };
        let phases = RequestPhases::default();
        let mut state = state_for()
            .pending(pending)
            .minimum(setup.minimum)
            .wait(setup.wait)
            .call();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            super::long_poll_then_reread(broker, pending, waits, &mut state, &phases),
        )
        .await
        .expect(setup.failure);
    }

    /// The accumulator a fetch carries into the long poll, for a pending set
    /// that has just been read and produced `bytes` bytes in total.
    #[bon::builder]
    fn state_for(
        pending: &[super::PendingRead],
        #[builder(into, default)] minimum: FetchMinimumBytes,
        #[builder(into, default)] wait: FetchWaitMillis,
    ) -> super::LongPollState {
        let setup = PollStateSetup { minimum, wait };
        super::LongPollState {
            min_bytes: setup.minimum.0,
            max_wait_ms: setup.wait.0,
            sendfile_capable: false,
            bytes: vec![0; pending.len()],
            cold_served: vec![false; pending.len()],
            remaining_response_bytes: usize::MAX,
            response_max_bytes: usize::MAX,
        }
    }

    fn sized_batch(payload: &'static [u8]) -> RecordBatch {
        RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(payload)),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// The base offsets of the batches a fetch answered with, in order.
    fn served_base_offsets(out: &super::PartitionData) -> Vec<i64> {
        let Some(krabka_protocol::records::RecordsPayload::Raw(raw)) = out.records.as_ref() else {
            return Vec::new();
        };
        let krabka_protocol::records::RecordsPayload::V2(batches) =
            krabka_protocol::records::RecordsPayload::from_bytes(raw.clone())
                .expect("decode served")
        else {
            return Vec::new();
        };
        batches.iter().map(|batch| batch.base_offset).collect()
    }

    /// `fetch.min.bytes` is a floor and not a hint. A wake that does not carry
    /// enough bytes parks again, and the response the fetch finally sends
    /// holds every append that arrived up to the one that cleared the floor.
    #[tokio::test]
    async fn min_bytes_holds_the_long_poll_until_three_appends_add_up() {
        const TOPIC: &str = "min-bytes-floor";
        const PAYLOAD: &[u8; 64] = &[b'x'; 64];

        // A follower fetch reads only from the leader.
        broker_fixture!(
            (broker_handle, dir, broker, part),
            local_follower_partition(TOPIC)
        );

        // One batch's worth of bytes, so the floor can be set between two
        // appends and three.
        let mut sized = BytesMut::new();
        sized_batch(PAYLOAD)
            .encode(&mut sized)
            .expect("encode one batch");
        let one_batch = sized.len();
        let min_bytes = i32::try_from(one_batch * 2 + 1).expect("small floor");

        let producer = std::sync::Arc::clone(&part);
        let appends = tokio::spawn(async move {
            for _ in 0..3 {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                producer
                    .produce_batch(sized_batch(PAYLOAD))
                    .await
                    .expect("append");
            }
        });

        let request = super::EffectivePartition {
            partition: 0,
            current_leader_epoch: 0,
            last_fetched_epoch: -1,
            fetch_offset: 0,
            log_start_offset: -1,
            partition_max_bytes: i32::try_from(one_batch * 8).expect("small budget"),
        };
        let pending = vec![planned_follower(TOPIC, &part, &request, crate::codes::NONE)];
        let phases = RequestPhases::default();
        let GroupedResponses {
            responses: topics, ..
        } = super::execute_pending_reads(
            &broker,
            pending,
            min_bytes,
            usize::MAX,
            30_000,
            false,
            &phases,
        )
        .await;

        appends.await.expect("producer task");
        let served = &topics[0].partitions[0];
        assert!(served_base_offsets(served) == vec![0, 1, 2]);
        broker_handle.shutdown().await;
    }

    /// An append that lands after the read pass and before the park is not
    /// lost: the waiters are armed before anything is read, so the producer's
    /// `notify_waiters` -- which leaves no permit behind for a waiter that
    /// registers later -- still has someone to wake.
    ///
    /// Without the pre-armed waiters this fetch sleeps out its whole
    /// `max_wait_ms`, which the test's own timeout stands in for.
    #[tokio::test]
    async fn an_append_that_lands_before_the_park_is_not_missed() {
        const TOPIC: &str = "append-before-park";

        // A follower fetch reads only from the leader.
        broker_fixture!(
            (broker_handle, dir, broker, part),
            local_follower_partition(TOPIC)
        );

        let mut pending = [follower_read(TOPIC, &part, NONE)];

        // What `execute_pending_reads` does in this order: arm the waiters,
        // read (the log is empty, so the read finds nothing), then park.
        let waits = super::arm_waits(&pending);
        let mut state = state_for().pending(&pending).minimum(1).wait(60_000).call();

        // The race the arming closes: the append lands between the read and
        // the park.
        part.produce_batch(sized_batch(b"raced"))
            .await
            .expect("append");

        let phases = RequestPhases::default();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            super::long_poll_then_reread(&broker, &mut pending, waits, &mut state, &phases),
        )
        .await
        .expect("the fetch answers on the append it raced");

        assert!(served_base_offsets(&pending[0].out) == vec![0]);
        broker_handle.shutdown().await;
    }
    /// Read `pending` under an 8 byte response budget and return the topics.
    async fn read_with_tiny_budget(
        broker: &Broker,
        pending: Vec<super::PendingRead>,
    ) -> Vec<krabka_protocol::owned::fetch_response::FetchableTopicResponse> {
        let phases = RequestPhases::default();
        let GroupedResponses { responses, .. } =
            super::execute_pending_reads(broker, pending, 0, 8, 0, false, &phases).await;
        responses
    }

    fn planned_follower(
        topic: &str,
        part: &std::sync::Arc<Partition>,
        request: &super::EffectivePartition,
        error_code: i16,
    ) -> super::PendingRead {
        super::PendingRead::planned(
            topic,
            WireUuid::ZERO,
            request,
            FetchMode {
                read_committed: false,
                is_follower_fetch: true,
            },
            Some(std::sync::Arc::clone(part)),
            super::PartitionData {
                partition_index: 0,
                error_code,
                ..Default::default()
            },
        )
    }

    fn follower_read(
        topic: &str,
        part: &std::sync::Arc<Partition>,
        error_code: i16,
    ) -> super::PendingRead {
        planned_follower(
            topic,
            part,
            &super::EffectivePartition {
                partition: 0,
                current_leader_epoch: 0,
                last_fetched_epoch: -1,
                fetch_offset: 0,
                log_start_offset: -1,
                partition_max_bytes: 1024,
            },
            error_code,
        )
    }
}
