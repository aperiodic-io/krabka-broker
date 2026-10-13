#[cfg(creusot)]
use super::restore_selection::{restore_source_selected, selection_input_valid};
use {
    super::{
        ProducerDecision, ProducerSnapshotEntryFacts, RestoreBatchFrame, RestoreExclusions,
        RestoreRecordDeltas, increment_sequence, reloaded_snapshot_preserves_last_batch_retry,
        restore_selection_respects_batch_extent,
    },
    crate::restore::{
        RestoreLayout, RestoreProducer, restore_rewritten_batch_header, restore_rewritten_record,
    },
    creusot_std::prelude::*,
};

/// Filtering an admitted idempotent data batch preserves original survivor
/// identity and the full producer sequence/offset span, including an empty
/// rewrite. Snapshot reconstruction then recognizes the original retry and
/// returns the original acknowledgement frontier. This projects one last
/// non-transactional batch: encoding, snapshot publication, complete replay,
/// PID routing and installation remain host obligations.
#[requires(frame.base_offset@ >= 0 && selection_input_valid(frame, records@))]
#[requires(records@.len() <= i32::MAX@)]
#[requires(producer.0@ >= 0 && producer.1@ >= 0 && producer.2@ >= 0)]
#[requires(forall<i: Int> 0 <= i && i < records@.len()
    ==> crate::restore::kafka_record_timestamp(frame, records@[i].0) <= frame.max_timestamp@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < records@.len()
    ==> records@[i].0.offset_delta@ < records@[j].0.offset_delta@)]
#[ensures(result.1@ == frame.base_offset@ + frame.last_offset_delta@ + 1)]
#[ensures(result.2 == producer.2 && result.3 == ProducerDecision::Duplicate { retained: 4usize })]
#[ensures(result.0@.len() <= records@.len()
    && (forall<i: Int> 0 <= i && i < result.0@.len() ==> result.0@[i].0@ < records@.len()
        && result.0@[i].1@ == frame.base_offset@ + records@[result.0@[i].0@].0.offset_delta@
        && result.0@[i].2@ == crate::restore::kafka_record_timestamp(frame, records@[result.0@[i].0@].0)
        && frame.base_offset@ <= result.0@[i].1@ && result.0@[i].1@ < result.1@
        && result.0@[i].2@ <= frame.max_timestamp@)
    && (forall<i: Int, j: Int> 0 <= i && i < j && j < result.0@.len() ==> result.0@[i].0@ < result.0@[j].0@)
    && (forall<i: Int> 0 <= i && i < records@.len() ==>
        (exists<j: Int> 0 <= j && j < result.0@.len() && result.0@[j].0@ == i)
            == restore_source_selected(frame, records@[i], bounds.0, bounds.1)))]
pub(super) fn filtered_restore_preserves_producer_retry(
    frame: RestoreBatchFrame,
    records: &[(RestoreRecordDeltas, RestoreExclusions)],
    bounds: (Option<i64>, Option<i64>),
    producer: (i64, i16, i32),
) -> (Vec<(usize, i64, i64)>, i64, i32, ProducerDecision) {
    let (_, _, selected) =
        restore_selection_respects_batch_extent(frame, records, bounds.0, bounds.1)
            .expect("admitted source coordinates");
    // The admitted length makes this bounded remainder an identity, using
    // the same checked cast range as the producer sequence kernels.
    let retained_count = (selected.len() as u64 % 0x8000_0000) as i32;
    let end = restore_rewritten_batch_header(
        RestoreLayout {
            base_offset: frame.base_offset,
            last_offset_delta: frame.last_offset_delta,
            records_count: retained_count,
        },
        RestoreProducer {
            control: false,
            transactional: false,
            producer_id: producer.0,
            producer_epoch: producer.1,
            base_sequence: producer.2,
        },
    )
    .expect("legal data rewrite");
    let mut retained: Vec<(usize, i64, i64)> = Vec::new();
    let mut i = 0usize;
    let mut previous = None;
    #[invariant(i@ <= selected@.len() && retained@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        retained@[j].0 == selected@[j]
        && retained@[j].1@ == frame.base_offset@ + records@[selected@[j]@].0.offset_delta@
        && retained@[j].2@ == crate::restore::kafka_record_timestamp(frame, records@[selected@[j]@].0))]
    #[invariant(previous == if i@ == 0 { None } else { Some(records@[selected@[i@ - 1]@].0.offset_delta) })]
    #[variant(selected@.len() - i@)]
    while i < selected.len() {
        let record = records[selected[i]].0;
        let coordinates =
            restore_rewritten_record(previous, frame, record).expect("legal survivor");
        proof_assert!(coordinates.0@ == frame.base_offset@ + record.offset_delta@
            && coordinates.1@ == crate::restore::kafka_record_timestamp(frame, record));
        retained.push((selected[i], coordinates.0, coordinates.1));
        previous = Some(record.offset_delta);
        i += 1;
    }
    let entry = ProducerSnapshotEntryFacts {
        producer_id: producer.0,
        producer_epoch: producer.1,
        last_sequence: increment_sequence(producer.2, frame.last_offset_delta),
        last_offset: end - 1,
        offset_delta: frame.last_offset_delta,
        coordinator_epoch: -1,
        current_txn_first_offset: -1,
    };
    let (_base, first, frontier, retry, _) =
        reloaded_snapshot_preserves_last_batch_retry(end, entry, producer.1)
            .expect("faithful rewritten last batch");
    proof_assert!(_base == frame.base_offset && first == producer.2 && frontier == end);
    (retained, frontier, first, retry)
}
