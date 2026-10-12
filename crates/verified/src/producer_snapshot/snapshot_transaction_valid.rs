use creusot_std::prelude::*;

use super::ProducerReloadRange;
#[cfg(creusot)]
use super::ProducerSnapshotEntryFacts;

open_logic! {
/// Kafka's `truncateAndReload` keeps a snapshot exactly when
/// `logStartOffset < offset <= logEndOffset`. A snapshot at the log start
/// describes no record that is still in the log, so Kafka deletes it too.
pub fn kafka_reload_keeps(offset: Int, log_start: Int, log_end: Int) -> bool {
    pearlite! { log_start < offset && offset <= log_end }
}
}

open_logic! {
/// Some local segment starts at `offset`.
pub fn is_segment_base(bases: Seq<i64>, offset: Int) -> bool {
    pearlite! { exists<j: Int> 0 <= j && j < bases.len() && bases[j]@ == offset }
}
}

open_logic! {
/// Kafka's `removeStraySnapshots(segmentBaseOffsets)` deletes a snapshot
/// exactly when no segment starts at its offset, unless it is the newest
/// snapshot and lies above every segment base. That one survivor is the
/// snapshot a clean shutdown writes at the log end.
///
/// The method walks the snapshots in offset order, deleting each stray one
/// as soon as a later stray one turns up, then deletes the last stray one if
/// it lies below the greatest segment base. What is left is the rule above.
pub fn kafka_stray_removed(snapshots: Seq<i64>, index: Int, bases: Seq<i64>) -> bool {
    pearlite! {
        !is_segment_base(bases, snapshots[index]@)
            && !((forall<j: Int> 0 <= j && j < snapshots.len()
                    ==> snapshots[j]@ <= snapshots[index]@)
                && (forall<j: Int> 0 <= j && j < bases.len()
                    ==> bases[j]@ < snapshots[index]@))
    }
}
}

open_logic! {
/// Kafka's replay cursor: `max(segment.baseOffset, mapEndOffset,
/// logStartOffset)` for the first local segment the replay reads, where
/// `mapEndOffset` is the loaded snapshot's offset, or the log start when no
/// snapshot loads. A loaded snapshot always lies above the log start.
pub fn kafka_replay_start(range: ProducerReloadRange, snapshot: Option<i64>) -> Int {
    pearlite! {
        match snapshot {
            Some(offset) => if offset@ >= range.local_start@ { offset@ } else { range.local_start@ },
            None => if range.log_start@ >= range.local_start@ {
                range.log_start@
            } else {
                range.local_start@
            },
        }
    }
}
}

/// Pick the log start a producer-state reload runs against, as Kafka's
/// `LogLoader.load` does.
///
/// With remote storage enabled (KIP-405), it is the checkpointed log start,
/// which may sit below every local segment. Kafka reads a missing checkpoint
/// as 0, so `established` is `None` answers 0. Without remote storage, it is
/// the greater of the checkpoint and the oldest local segment's base, which
/// is `local_log_start`.
#[ensures(result == if remote_storage_enable {
    match established {
        Some(start) => start,
        None => 0i64,
    }
} else {
    local_log_start
})]
#[must_use]
pub const fn producer_snapshot_reload_log_start(
    remote_storage_enable: bool,
    established: Option<i64>,
    local_log_start: i64,
) -> i64 {
    if remote_storage_enable {
        match established {
            Some(start) => start,
            None => 0,
        }
    } else {
        local_log_start
    }
}

/// Keep a snapshot through a reload exactly when Kafka's `truncateAndReload`
/// keeps it: when it lies in `(log_start, log_end]`.
#[ensures(result == kafka_reload_keeps(offset@, range.log_start@, range.log_end@))]
#[must_use]
pub const fn producer_snapshot_reload_keeps(offset: i64, range: ProducerReloadRange) -> bool {
    range.log_start < offset && offset <= range.log_end
}

/// Delete the snapshot at `snapshots[index]` exactly when Kafka's
/// `removeStraySnapshots` deletes it, given every local segment's base
/// offset. The snapshot offsets must be distinct; their order is irrelevant.
///
/// The proof assumes `index < snapshots.len()`. Other inputs are outside
/// that contract.
///
/// # Panics
///
/// Panics when `index` is outside `snapshots`.
#[requires(index@ < snapshots@.len())]
#[ensures(result == kafka_stray_removed(snapshots@, index@, segment_bases@))]
#[must_use]
pub fn producer_snapshot_stray(snapshots: &[i64], index: usize, segment_bases: &[i64]) -> bool {
    let offset = snapshots[index];
    let mut is_base = false;
    let mut above_every_base = true;
    let mut i = 0usize;
    #[invariant(i@ <= segment_bases@.len())]
    #[invariant(is_base == exists<j: Int> 0 <= j && j < i@ && segment_bases@[j]@ == offset@)]
    #[invariant(above_every_base == forall<j: Int> 0 <= j && j < i@
        ==> segment_bases@[j]@ < offset@)]
    #[variant(segment_bases@.len() - i@)]
    while i < segment_bases.len() {
        if segment_bases[i] == offset {
            is_base = true;
        }
        if segment_bases[i] >= offset {
            above_every_base = false;
        }
        i += 1;
    }
    let mut newest = true;
    let mut k = 0usize;
    #[invariant(k@ <= snapshots@.len())]
    #[invariant(newest == forall<j: Int> 0 <= j && j < k@ ==> snapshots@[j]@ <= offset@)]
    #[variant(snapshots@.len() - k@)]
    while k < snapshots.len() {
        if snapshots[k] > offset {
            newest = false;
        }
        k += 1;
    }
    !(is_base || (newest && above_every_base))
}

/// Return the index of the newest snapshot a reload keeps, as Kafka's
/// `loadFromSnapshot` loads `latestSnapshotFile()` after `truncateAndReload`
/// deleted every snapshot outside `(log_start, log_end]`. Input order is
/// irrelevant.
#[ensures(result == None ==>
    forall<i: Int> 0 <= i && i < offsets@.len()
        ==> !kafka_reload_keeps(offsets@[i]@, range.log_start@, range.log_end@))]
#[ensures(forall<selected: usize> result == Some(selected) ==>
    selected@ < offsets@.len()
        && kafka_reload_keeps(offsets@[selected@]@, range.log_start@, range.log_end@)
        && (forall<i: Int> 0 <= i && i < offsets@.len()
            && kafka_reload_keeps(offsets@[i]@, range.log_start@, range.log_end@)
            ==> offsets@[i]@ <= offsets@[selected@]@))]
#[must_use]
pub fn producer_snapshot_latest_index(
    offsets: &[i64],
    range: ProducerReloadRange,
) -> Option<usize> {
    let mut best: Option<usize> = None;
    let mut i = 0usize;
    #[invariant(i@ <= offsets@.len())]
    #[invariant(match best {
        None => forall<j: Int> 0 <= j && j < i@
            ==> !kafka_reload_keeps(offsets@[j]@, range.log_start@, range.log_end@),
        Some(selected) => selected@ < i@
            && kafka_reload_keeps(offsets@[selected@]@, range.log_start@, range.log_end@)
            && (forall<j: Int> 0 <= j && j < i@
                && kafka_reload_keeps(offsets@[j]@, range.log_start@, range.log_end@)
                ==> offsets@[j]@ <= offsets@[selected@]@),
    })]
    #[variant(offsets@.len() - i@)]
    while i < offsets.len() {
        if producer_snapshot_reload_keeps(offsets[i], range) {
            match best {
                None => best = Some(i),
                Some(selected) if offsets[i] > offsets[selected] => best = Some(i),
                Some(_) => {}
            }
        }
        i += 1;
    }
    best
}

open_logic! {
/// The entry's last-record fields are either the no-record sentinel -- a
/// producer that has only written transaction markers, which Kafka writes as
/// `(-1, -1, 0)` -- or a real batch that ends before the snapshot's offset,
/// with room below its last offset for the offsets its delta spans.
pub fn snapshot_last_record_valid(snapshot_offset: Int, entry: ProducerSnapshotEntryFacts) -> bool {
    pearlite! {
        (entry.last_offset@ == -1 && entry.last_sequence@ == -1 && entry.offset_delta@ == 0)
            || (entry.last_offset@ >= 0
                && entry.last_sequence@ >= 0
                && entry.offset_delta@ >= 0
                && entry.last_offset@ >= entry.offset_delta@
                && entry.last_offset@ < snapshot_offset)
    }
}
}

open_logic! {
/// The entry's open transaction is either absent (`-1`), or starts before
/// the snapshot's offset and no later than the producer's last record.
pub fn snapshot_transaction_valid(snapshot_offset: Int, entry: ProducerSnapshotEntryFacts) -> bool {
    pearlite! {
        entry.current_txn_first_offset@ == -1
            || (entry.current_txn_first_offset@ >= 0
                && entry.current_txn_first_offset@ < snapshot_offset
                && entry.current_txn_first_offset@ <= entry.last_offset@)
    }
}
}
