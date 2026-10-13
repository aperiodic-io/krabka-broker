#[cfg(creusot)]
use super::{
    kafka_reload_keeps, kafka_replay_start, snapshot_last_record_valid, snapshot_transaction_valid,
};
use {
    super::{ProducerReloadRange, ProducerSnapshotEntryFacts, producer_snapshot_reload_keeps},
    creusot_std::prelude::*,
};

open_logic! {
/// What a snapshot at `snapshot_offset` can truthfully say about one
/// producer: a real producer identity, a coordinator epoch that is real or
/// Kafka's `-1`, and last-record and transaction fields that describe only
/// records before the snapshot's offset.
pub fn snapshot_entry_valid_model(snapshot_offset: Int, entry: ProducerSnapshotEntryFacts) -> bool {
    pearlite! {
        snapshot_offset >= 0
            && entry.producer_id@ >= 0
            && entry.producer_epoch@ >= 0
            && entry.coordinator_epoch@ >= -1
            && snapshot_last_record_valid(snapshot_offset, entry)
            && snapshot_transaction_valid(snapshot_offset, entry)
    }
}
}

/// Validate one decoded producer entry against its snapshot's exclusive
/// offset boundary: the result is `snapshot_entry_valid_model`.
#[ensures(result == snapshot_entry_valid_model(snapshot_offset@, entry))]
#[must_use]
pub fn producer_snapshot_entry_valid(
    snapshot_offset: i64,
    entry: ProducerSnapshotEntryFacts,
) -> bool {
    let last_record_valid =
        (entry.last_offset == -1 && entry.last_sequence == -1 && entry.offset_delta == 0)
            || (entry.last_offset >= 0
                && entry.last_sequence >= 0
                && entry.offset_delta >= 0
                && entry.last_offset >= i64::from(entry.offset_delta)
                && entry.last_offset < snapshot_offset);
    let transaction_valid = entry.current_txn_first_offset == -1
        || (entry.current_txn_first_offset >= 0
            && entry.current_txn_first_offset < snapshot_offset
            && entry.current_txn_first_offset <= entry.last_offset);
    snapshot_offset >= 0
        && entry.producer_id >= 0
        && entry.producer_epoch >= 0
        && entry.coordinator_epoch >= -1
        && last_record_valid
        && transaction_valid
}

/// Select the replay cursor after a reload loaded `snapshot`, or none.
///
/// Kafka's `UnifiedLog.rebuildProducerState` replays each local segment from
/// `max(segment.baseOffset, mapEndOffset, logStartOffset)` (see
/// `kafka_replay_start`). The range must be nonnegative and ordered, with
/// the local start at or below the log end, and a loaded snapshot must be one
/// the reload keeps; anything else is rejected as corrupt.
///
/// The cursor can land inside a batch when a trim did. Kafka's
/// `LogSegment.read(startOffset, ..)` then starts from the batch that holds
/// the cursor, and replaying that whole batch is the host's job.
#[ensures((result != None) == (range.log_start@ >= 0
    && range.log_start@ <= range.log_end@
    && range.local_start@ >= 0
    && range.local_start@ <= range.log_end@
    && match snapshot {
        Some(offset) => kafka_reload_keeps(offset@, range.log_start@, range.log_end@),
        None => true,
    }))]
#[ensures(forall<start: i64> result == Some(start) ==> start@ == kafka_replay_start(range, snapshot))]
#[must_use]
pub fn producer_snapshot_replay_start(
    range: ProducerReloadRange,
    snapshot: Option<i64>,
) -> Option<i64> {
    if range.log_start < 0
        || range.log_end < range.log_start
        || range.local_start < 0
        || range.log_end < range.local_start
    {
        return None;
    }
    let map_end = match snapshot {
        Some(offset) if producer_snapshot_reload_keeps(offset, range) => offset,
        Some(_) => return None,
        None => range.log_start,
    };
    Some(map_end.max(range.local_start))
}
