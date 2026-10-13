use {
    super::tiered_timestamp_lookup_preserves_first,
    crate::{
        list_offsets::ListOffsetsSelectionDecision,
        timestamp::{timestamp_record_coordinates, timestamp_record_time},
    },
    creusot_std::prelude::*,
};

/// Batch timestamp type, checked record coordinates, retention and exclusive
/// visibility preserve the first record's actual wire timestamp. Append-time
/// lookup ignores producer-time overflow; `CreateTime` requires valid sums.
/// Decoding, timestamp-type projection, and complete batch enumeration are
/// host obligations. This trace contains one complete decoded data batch.
#[requires(base@ >= 0 && request.0@ >= 0 && request.1@ >= 0 && request.2@ >= 0 && epoch@ >= -1)]
#[requires(forall<i: Int> 0 <= i && i < records@.len()
    ==> records@[i].0@ >= 0 && base@ + records@[i].0@ <= i64::MAX@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < records@.len()
    ==> records@[i].0@ < records@[j].0@)]
#[requires(append_time != None || (forall<i: Int> 0 <= i && i < records@.len()
    ==> i64::MIN@ <= batch_time@ + records@[i].1@ && batch_time@ + records@[i].1@ <= i64::MAX@))]
#[ensures(match result {
    ListOffsetsSelectionDecision::RejectMalformed => false,
    ListOffsetsSelectionDecision::Resolved { offset, timestamp, leader_epoch } =>
        request.0@ <= offset@ && offset@ < request.2@ && timestamp@ >= request.1@ && leader_epoch == epoch
        && (exists<i: Int> 0 <= i && i < records@.len()
            && offset@ == base@ + records@[i].0@
            && timestamp@ == match append_time { Some(stamp) => stamp@, None => batch_time@ + records@[i].1@ })
        && (forall<i: Int> 0 <= i && i < records@.len()
            && base@ + records@[i].0@ >= request.0@
            && (match append_time { Some(stamp) => stamp@, None => batch_time@ + records@[i].1@ }) >= request.1@
            ==> offset@ <= base@ + records@[i].0@),
    ListOffsetsSelectionDecision::Unknown =>
        forall<i: Int> 0 <= i && i < records@.len()
            ==> base@ + records@[i].0@ < request.0@ || base@ + records@[i].0@ >= request.2@
                || (match append_time { Some(stamp) => stamp@, None => batch_time@ + records@[i].1@ }) < request.1@,
})]
pub(super) fn typed_timestamp_records_preserve_visibility(
    records: &[(i32, i64)],
    base: i64,
    batch_time: i64,
    append_time: Option<i64>,
    request: (i64, i64, i64), // logical floor, target timestamp, exclusive visibility bound
    epoch: i32,
) -> ListOffsetsSelectionDecision {
    let (minimum, target, bound) = request;
    let mut decoded: Vec<(i64, i64)> = Vec::new();
    let mut index = 0usize;
    #[invariant(index@ <= records@.len() && decoded@.len() == index@)]
    #[invariant(forall<i: Int> 0 <= i && i < index@
        ==> decoded@[i].0@ == base@ + records@[i].0@
            && decoded@[i].1@ == match append_time { Some(stamp) => stamp@, None => batch_time@ + records@[i].1@ })]
    #[variant(records@.len() - index@)]
    while index < records.len() {
        let (offset_delta, timestamp_delta) = records[index];
        let timestamp = timestamp_record_time(batch_time, timestamp_delta, append_time)
            .expect("valid effective timestamp");
        decoded.push(
            timestamp_record_coordinates(base, offset_delta, timestamp, 0)
                .expect("valid absolute offset"),
        );
        index += 1;
    }
    tiered_timestamp_lookup_preserves_first(&[], &decoded, target, minimum, bound, epoch)
}
