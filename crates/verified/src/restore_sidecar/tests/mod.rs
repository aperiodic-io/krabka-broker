use {
    super::{
        RestoreAbortedTxn, RestoreSegmentExtent, restore_index_frontier,
        restore_leader_epoch_entry_valid, restore_offset_index_entry_valid,
        restore_producer_ids_strict, restore_time_index_entry_valid, restore_txn_index_entry_valid,
    },
    assert2::check,
};

const SEGMENT: RestoreSegmentExtent = RestoreSegmentExtent {
    base_offset: 100,
    last_offset: 110,
};

const fn txn(producer_id: i64, start_offset: i64, last_offset: i64) -> RestoreAbortedTxn {
    RestoreAbortedTxn {
        producer_id,
        start_offset,
        last_offset,
    }
}

/// Walk a whole index the way the host does, threading `last_offset`.
fn txn_index_valid(entries: &[RestoreAbortedTxn], segment: RestoreSegmentExtent) -> bool {
    let mut previous_last = None;
    for &entry in entries {
        if !restore_txn_index_entry_valid(previous_last, entry, segment) {
            return false;
        }
        previous_last = Some(entry.last_offset);
    }
    true
}

mod index_frontier_is_the_u32_relative_span;
