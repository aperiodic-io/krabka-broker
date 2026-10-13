#[cfg(creusot)]
use super::{kafka_record_timestamp, restore_record_placeable};
use {
    super::{
        RestoreBatchFrame, RestoreLayout, RestoreProducer, RestoreRecordDeltas,
        restore_record_coordinates,
    },
    creusot_std::prelude::*,
};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Kafka's legal producer identities (`RecordBatch.NO_PRODUCER_ID`,
/// `NO_PRODUCER_EPOCH`, `NO_SEQUENCE` are all `-1`).
///
/// A data batch is either non-idempotent, with every identity field at its
/// `-1` sentinel and no transactional bit, or idempotent, with a nonnegative
/// producer id, epoch and base sequence. A transaction marker is a
/// transactional control batch with a nonnegative producer and epoch and the
/// `-1` sequence (`EndTransactionMarker` batches carry no sequence). A
/// non-transactional control batch carries the non-idempotent sentinels.
pub fn legal_producer(producer: RestoreProducer) -> bool {
    pearlite! {
        if producer.control {
            non_idempotent_producer(producer)
                || (producer.transactional
                    && producer.producer_id@ >= 0
                    && producer.producer_epoch@ >= 0
                    && producer.base_sequence@ == -1)
        } else {
            non_idempotent_producer(producer)
                || (producer.producer_id@ >= 0
                    && producer.producer_epoch@ >= 0
                    && producer.base_sequence@ >= 0)
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Every identity field at its `-1` sentinel and no transactional bit.
pub fn non_idempotent_producer(producer: RestoreProducer) -> bool {
    pearlite! {
        !producer.transactional
            && producer.producer_id@ == -1
            && producer.producer_epoch@ == -1
            && producer.base_sequence@ == -1
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// A batch header's offset layout is legal: nonnegative base, span and
/// record count, an exclusive end that fits `i64`, and for a control batch
/// Kafka's single-offset span holding at most its one marker. A control batch
/// may hold zero records because Kafka's `LogCleaner` (and krabka's
/// compaction) keeps a producer's last batch as an empty header
/// (`BatchRetention.RETAIN_EMPTY`) once the marker record itself is discarded.
pub fn legal_header(layout: RestoreLayout, control: bool) -> bool {
    pearlite! {
        layout.base_offset@ >= 0
            && layout.last_offset_delta@ >= 0
            && layout.records_count@ >= 0
            && (control ==> layout.last_offset_delta@ == 0 && layout.records_count@ <= 1)
            && layout.base_offset@ + layout.last_offset_delta@ + 1 <= i64::MAX@
    }
}

/// Admit a batch header synthesized or re-encoded by restore and return its
/// exclusive offset frontier.
#[ensures(match result {
    Some(frontier) => legal_header(layout, producer.control)
        && legal_producer(producer)
        && frontier@ == layout.base_offset@ + layout.last_offset_delta@ + 1,
    None => !(legal_header(layout, producer.control) && legal_producer(producer)),
})]
#[must_use]
pub fn restore_rewritten_batch_header(
    layout: RestoreLayout,
    producer: RestoreProducer,
) -> Option<i64> {
    let non_idempotent = !producer.transactional
        && producer.producer_id == -1
        && producer.producer_epoch == -1
        && producer.base_sequence == -1;
    let producer_legal = if producer.control {
        non_idempotent
            || (producer.transactional
                && producer.producer_id >= 0
                && producer.producer_epoch >= 0
                && producer.base_sequence == -1)
    } else {
        non_idempotent
            || (producer.producer_id >= 0
                && producer.producer_epoch >= 0
                && producer.base_sequence >= 0)
    };
    if !producer_legal
        || layout.base_offset < 0
        || layout.last_offset_delta < 0
        || layout.records_count < 0
        || (producer.control && (layout.last_offset_delta != 0 || layout.records_count > 1))
    {
        return None;
    }

    layout
        .base_offset
        .checked_add(i64::from(layout.last_offset_delta))?
        .checked_add(1)
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// One retained record is legal in a rewritten batch: it is placeable, its
/// offset delta strictly follows the previous retained record's, and its
/// Kafka timestamp does not exceed the preserved archived `max_timestamp`.
/// The bound may be loose when the record that set it was filtered out;
/// under `LogAppendTime` it holds with equality.
pub fn restore_rewrite_record_legal(
    previous_offset_delta: Option<i32>,
    frame: RestoreBatchFrame,
    record: RestoreRecordDeltas,
) -> bool {
    pearlite! {
        restore_record_placeable(frame, record)
            && match previous_offset_delta {
                Some(previous) => previous@ < record.offset_delta@,
                None => true,
            }
            && kafka_record_timestamp(frame, record) <= frame.max_timestamp@
    }
}

/// Validate one retained record against the synthesized header and return its
/// absolute offset and Kafka timestamp.
#[ensures(match result {
    Some((offset, timestamp)) => restore_rewrite_record_legal(previous_offset_delta, frame, record)
        && offset@ == frame.base_offset@ + record.offset_delta@
        && timestamp@ == kafka_record_timestamp(frame, record),
    None => !restore_rewrite_record_legal(previous_offset_delta, frame, record),
})]
#[must_use]
pub fn restore_rewritten_record(
    previous_offset_delta: Option<i32>,
    frame: RestoreBatchFrame,
    record: RestoreRecordDeltas,
) -> Option<(i64, i64)> {
    if previous_offset_delta.is_some_and(|previous| previous >= record.offset_delta) {
        return None;
    }
    let coordinates = restore_record_coordinates(frame, record)?;
    if coordinates.1 > frame.max_timestamp {
        None
    } else {
        Some(coordinates)
    }
}
