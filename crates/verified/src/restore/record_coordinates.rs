use {
    super::{
        RestoreBatchFrame, RestoreExclusions, RestoreFilterDecision, RestoreReconcileDecision,
        RestoreRecordDeltas, RestoreSnapshotState, RestoreTimestampType,
    },
    creusot_std::prelude::*,
};

open_logic! {
/// Exclusion predicates are OR-combined: any one match drops the record.
pub fn restore_excluded(exclusions: RestoreExclusions) -> bool {
    pearlite! {
        exclusions.producer
            || exclusions.offset
            || exclusions.content.key
            || exclusions.content.header
    }
}
}

/// Keep one record exactly when it is inside both bounds and no exclusion
/// predicate matches. `--to-offset` is inclusive; `--to-timestamp` is
/// exclusive.
#[ensures(result == (
    match offset_bound { Some(bound) => offset@ <= bound@, None => true }
        && match timestamp_bound { Some(bound) => timestamp@ < bound@, None => true }
        && !restore_excluded(exclusions)
))]
#[must_use]
pub fn restore_record_selected(
    offset: i64,
    offset_bound: Option<i64>,
    timestamp: i64,
    timestamp_bound: Option<i64>,
    exclusions: RestoreExclusions,
) -> bool {
    (match offset_bound {
        Some(bound) => offset <= bound,
        None => true,
    }) && (match timestamp_bound {
        Some(bound) => timestamp < bound,
        None => true,
    }) && !(exclusions.producer
        || exclusions.offset
        || exclusions.content.key
        || exclusions.content.header)
}

/// Fold whether the record walk saw at least one keep and one drop.
#[ensures(match result {
    RestoreFilterDecision::Keep => !saw_drop,
    RestoreFilterDecision::Empty => saw_drop && !saw_keep,
    RestoreFilterDecision::Filter => saw_keep && saw_drop,
})]
#[must_use]
pub fn restore_batch_filter_decision(saw_keep: bool, saw_drop: bool) -> RestoreFilterDecision {
    if saw_keep && saw_drop {
        RestoreFilterDecision::Filter
    } else if saw_drop {
        RestoreFilterDecision::Empty
    } else {
        RestoreFilterDecision::Keep
    }
}

/// A sorted batch stream may stop exactly when the next batch base is past an
/// inclusive offset bound.
#[ensures(result == match offset_bound {
    Some(bound) => batch_base@ > bound@,
    None => false,
})]
#[must_use]
pub fn restore_batch_past_offset_bound(batch_base: i64, offset_bound: Option<i64>) -> bool {
    match offset_bound {
        Some(bound) => batch_base > bound,
        None => false,
    }
}

/// Reconcile one archive-scan observation with one snapshot lifecycle state.
///
/// A live segment must have been scanned. A segment whose deletion started is
/// excluded whether or not its objects remain. A scanned segment that the
/// snapshot omits, or reports as fully deleted, disagrees; an unscanned one
/// is simply excluded.
#[ensures((result == RestoreReconcileDecision::Keep)
    == (scanned && snapshot_state == RestoreSnapshotState::Live))]
#[ensures((result == RestoreReconcileDecision::Exclude)
    == ((!scanned && snapshot_state != RestoreSnapshotState::Live)
        || (scanned && snapshot_state == RestoreSnapshotState::DeleteStarted)))]
#[ensures((result == RestoreReconcileDecision::Disagree)
    == ((!scanned && snapshot_state == RestoreSnapshotState::Live)
        || (scanned && (snapshot_state == RestoreSnapshotState::Missing
            || snapshot_state == RestoreSnapshotState::DeleteFinished))))]
#[must_use]
pub fn restore_archive_reconcile(
    scanned: bool,
    snapshot_state: RestoreSnapshotState,
) -> RestoreReconcileDecision {
    match snapshot_state {
        RestoreSnapshotState::Live => {
            if scanned {
                RestoreReconcileDecision::Keep
            } else {
                RestoreReconcileDecision::Disagree
            }
        }
        RestoreSnapshotState::DeleteStarted => RestoreReconcileDecision::Exclude,
        RestoreSnapshotState::Missing | RestoreSnapshotState::DeleteFinished => {
            if scanned {
                RestoreReconcileDecision::Disagree
            } else {
                RestoreReconcileDecision::Exclude
            }
        }
    }
}

/// Validate one archived batch and return its exclusive next offset.
///
/// The caller supplies the minimum base offset the next batch may carry. Kafka
/// compaction preserves absolute offsets and may leave gaps, so a later base is
/// valid; overlap/regression, a negative span, and an exclusive end outside
/// `i64` fail closed.
#[ensures(match result {
    Some(next) => last_delta@ >= 0
        && base@ >= minimum_base@
        && next@ == base@ + last_delta@ + 1
        && next@ > base@
        && next@ <= i64::MAX@,
    None => last_delta@ < 0
        || base@ < minimum_base@
        || base@ + last_delta@ + 1 > i64::MAX@,
})]
#[must_use]
pub fn restore_batch_step(minimum_base: i64, base: i64, last_delta: i32) -> Option<i64> {
    if last_delta < 0 || base < minimum_base {
        return None;
    }

    let delta = i64::from(last_delta);
    if base > i64::MAX - delta - 1 {
        None
    } else {
        Some(base + delta + 1)
    }
}

open_logic! {
/// The timestamp Kafka reports for a record: `baseTimestamp + timestampDelta`
/// under `CreateTime`, and the batch `maxTimestamp` under `LogAppendTime`,
/// whose record deltas stay as the producer wrote them
/// (`DefaultRecord.readFrom(..., logAppendTime)`,
/// `DefaultRecordBatch.RecordIterator`).
pub fn kafka_record_timestamp(frame: RestoreBatchFrame, record: RestoreRecordDeltas) -> Int {
    pearlite! {
        match frame.timestamp_type {
            RestoreTimestampType::CreateTime => frame.base_timestamp@ + record.timestamp_delta@,
            RestoreTimestampType::LogAppendTime => frame.max_timestamp@,
        }
    }
}
}

open_logic! {
/// A record lies inside its batch's declared offset span, and both its
/// absolute offset and its Kafka timestamp are representable as `i64`.
pub fn restore_record_placeable(frame: RestoreBatchFrame, record: RestoreRecordDeltas) -> bool {
    pearlite! {
        frame.last_offset_delta@ >= 0
            && record.offset_delta@ >= 0
            && record.offset_delta@ <= frame.last_offset_delta@
            && frame.base_offset@ + record.offset_delta@ <= i64::MAX@
            && kafka_record_timestamp(frame, record) >= i64::MIN@
            && kafka_record_timestamp(frame, record) <= i64::MAX@
    }
}
}

/// Compute one record's absolute offset and Kafka timestamp within its batch.
///
/// Malformed ranges and either kind of overflow fail closed. Under
/// `LogAppendTime` the producer's `base_timestamp + timestamp_delta` is never
/// read, as in Kafka, so it cannot make a legal batch fail.
#[ensures(match result {
    Some((offset, timestamp)) => restore_record_placeable(frame, record)
        && offset@ == frame.base_offset@ + record.offset_delta@
        && timestamp@ == kafka_record_timestamp(frame, record),
    None => !restore_record_placeable(frame, record),
})]
#[must_use]
pub fn restore_record_coordinates(
    frame: RestoreBatchFrame,
    record: RestoreRecordDeltas,
) -> Option<(i64, i64)> {
    if frame.last_offset_delta < 0
        || record.offset_delta < 0
        || record.offset_delta > frame.last_offset_delta
    {
        return None;
    }
    let offset = frame
        .base_offset
        .checked_add(i64::from(record.offset_delta))?;
    let timestamp = match frame.timestamp_type {
        RestoreTimestampType::CreateTime => {
            frame.base_timestamp.checked_add(record.timestamp_delta)?
        }
        RestoreTimestampType::LogAppendTime => frame.max_timestamp,
    };
    Some((offset, timestamp))
}
