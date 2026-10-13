//! One compaction pass over the abstract log. The pass is generic over the
//! [`Cleaner`] so that the same checker runs against the production cores and
//! against the deliberately-broken legacy pair. It asserts nothing: the rules a
//! pass must obey live in `invariants.rs` and are checked as `always`
//! properties over the pass's input and output.

use {
    super::{
        DELETE_RETENTION_MS,
        state::{Cleaner, Entry, EntryKind},
    },
    crate::compact::{
        RetainDecision, TxnDataState, data_retention_metadata, marker_retention_metadata,
    },
};

/// Run one compaction pass of `cleaner` over `log` at `clock` and return the
/// next log. This mirrors the production rewrite path: build the dedup map
/// with the cleaner's filter, derive each marker's transaction state, and then
/// apply the cleaner's retain decision to each entry in offset order.
pub(super) fn compact_pass(log: &[Entry], clock: i64, cleaner: Cleaner) -> Vec<Entry> {
    let offset_map = cleaner.offset_map(log);
    let marker_states = Cleaner::marker_states(log);

    let mut next: Vec<Entry> = Vec::with_capacity(log.len());
    for (idx, entry) in log.iter().enumerate() {
        let is_newest = Cleaner::is_newest(log, &offset_map, idx);
        let (metadata, txn) = match entry.kind {
            EntryKind::Data { value } => (
                data_retention_metadata()
                    .has_key(entry.key.is_some())
                    .has_value(value.is_some())
                    .maybe_existing_horizon(entry.horizon)
                    .call(),
                TxnDataState::NotTransactional,
            ),
            // A control record carries the control-type key and no value.
            EntryKind::Marker { producer_id, .. } => (
                marker_retention_metadata()
                    .producer_id(i64::from(producer_id))
                    .maybe_existing_horizon(entry.horizon)
                    .call(),
                marker_states[&idx],
            ),
        };

        match (cleaner.retain)(
            metadata.record,
            metadata.batch,
            is_newest,
            txn,
            clock,
            DELETE_RETENTION_MS,
        ) {
            RetainDecision::Keep => next.push(entry.clone()),
            RetainDecision::SetHorizon(h) => next.push(Entry {
                horizon: Some(h),
                ..entry.clone()
            }),
            RetainDecision::Delete => {}
        }
    }
    next
}
