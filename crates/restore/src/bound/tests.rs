//! Unit tests for the decisions `Predicates` makes about one archived batch
//! and the records inside it, driven through the shared `decide` harness.

use assert2::check;
use bytes::Bytes;
use krabka_protocol::records::{Record, TimestampType};

use super::{
    test_support::{
        BASE_TIMESTAMP, batch, check_decision, check_record_parse_error, decide, header,
        keyed_records, partition, predicates, record, timestamped_records,
    },
    *,
};

#[test]
fn no_predicates_keep_everything() {
    let predicates = predicates(&[]);
    let orders_0 = partition("orders", 0);
    let owned = batch(1, keyed_records(&[Some(b"k0"), None]));

    check_decision(
        &predicates,
        &orders_0,
        &owned,
        BatchDecision::Keep,
        &[RecordDecision::Keep, RecordDecision::Keep],
    );
}

#[test]
fn to_offset_bound_is_inclusive_at_the_named_offset() {
    let predicates = predicates(&["--to-offset", "orders:0=42"]);

    check!(predicates.offset_bound(&partition("orders", 0)) == Some(Offset(42)));
    check!(predicates.offset_bound(&partition("orders", 1)).is_none());
    check!(predicates.offset_bound(&partition("other", 0)).is_none());
    check!(!predicates.batch_past_offset_bound(&partition("orders", 0), Offset(42)));
    check!(predicates.batch_past_offset_bound(&partition("orders", 0), Offset(43)));
    check!(!predicates.batch_past_offset_bound(&partition("orders", 1), Offset(i64::MAX)));
}

#[test]
fn to_offset_filters_a_batch_that_straddles_the_inclusive_bound() {
    let predicates = predicates(&["--to-offset", "orders:0=1001"]);
    let orders_0 = partition("orders", 0);
    let owned = batch(1, vec![record(0), record(1), record(2)]);

    check_decision(
        &predicates,
        &orders_0,
        &owned,
        BatchDecision::Filter,
        &[
            RecordDecision::Keep,
            RecordDecision::Keep,
            RecordDecision::Drop,
        ],
    );

    let (other_decision, other_records) = decide(&predicates, &partition("orders", 1), &owned);
    check!(other_decision == BatchDecision::Keep);
    check!(other_records == [RecordDecision::Keep; 3]);
}

#[test]
fn exclude_key_filters_only_matching_records() {
    let predicates = predicates(&["--exclude-key", "^alpha"]);
    let orders_0 = partition("orders", 0);
    let owned = batch(
        1,
        keyed_records(&[Some(b"alpha-1"), Some(b"beta-1"), Some(b"alpha-2")]),
    );

    check_decision(
        &predicates,
        &orders_0,
        &owned,
        BatchDecision::Filter,
        &[
            RecordDecision::Drop,
            RecordDecision::Keep,
            RecordDecision::Drop,
        ],
    );
}

fn check_two_key_records(
    pattern: &str,
    expected_batch: BatchDecision,
    expected_record: RecordDecision,
) {
    let predicates = predicates(&["--exclude-key", pattern]);
    let owned = batch(1, keyed_records(&[Some(b"k1"), Some(b"k2")]));
    check_decision(
        &predicates,
        &partition("orders", 0),
        &owned,
        expected_batch,
        &[expected_record; 2],
    );
}

#[test]
fn exclude_key_matching_every_record_empties_the_batch() {
    check_two_key_records("^k", BatchDecision::Empty, RecordDecision::Drop);
}

#[test]
fn exclude_key_matching_nothing_keeps_the_batch() {
    check_two_key_records("^zzz", BatchDecision::Keep, RecordDecision::Keep);
}

#[test]
fn a_keyless_record_never_matches_an_exclude_key_pattern_even_dot_star() {
    let predicates = predicates(&["--exclude-key", ".*"]);
    let orders_0 = partition("orders", 0);
    let owned = batch(1, keyed_records(&[None, Some(b"anything")]));

    check_decision(
        &predicates,
        &orders_0,
        &owned,
        BatchDecision::Filter,
        &[RecordDecision::Keep, RecordDecision::Drop],
    );
}

#[test]
fn exclude_header_matches_on_name_and_value_not_name_alone() {
    let predicates = predicates(&["--exclude-header", "trace=^bad"]);
    let orders_0 = partition("orders", 0);
    let owned = batch(
        1,
        vec![
            Record {
                headers: vec![header("trace", b"bad-1")],
                ..record(0)
            },
            Record {
                headers: vec![header("trace", b"good-1")],
                ..record(1)
            },
            Record {
                headers: vec![header("other", b"bad-1")],
                ..record(2)
            },
        ],
    );

    check_decision(
        &predicates,
        &orders_0,
        &owned,
        BatchDecision::Filter,
        &[
            RecordDecision::Drop,
            RecordDecision::Keep,
            RecordDecision::Keep,
        ],
    );
}

#[test]
fn exclude_producer_id_drops_every_record_from_that_producer_and_no_other() {
    let predicates = predicates(&["--exclude-producer-id", "7"]);
    let orders_0 = partition("orders", 0);

    let named = batch(7, vec![record(0), record(1)]);
    let (batch_decision, records) = decide(&predicates, &orders_0, &named);
    check!(batch_decision == BatchDecision::Empty);
    check!(records == [RecordDecision::Drop, RecordDecision::Drop]);

    let other = batch(8, vec![record(0), record(1)]);
    let (batch_decision, records) = decide(&predicates, &orders_0, &other);
    check!(batch_decision == BatchDecision::Keep);
    check!(records == [RecordDecision::Keep, RecordDecision::Keep]);
}

#[test]
fn exclude_offset_range_is_half_open() {
    // BASE_OFFSET is 1_000, so offset_delta N is absolute offset 1_000+N.
    // The range 1001..1003 must drop 1001 (inclusive start) and 1002, and
    // keep 1000 and 1003 (exclusive end).
    let predicates = predicates(&["--exclude-offset", "orders:0=1001..1003"]);
    let orders_0 = partition("orders", 0);
    let owned = batch(1, vec![record(0), record(1), record(2), record(3)]);

    check_decision(
        &predicates,
        &orders_0,
        &owned,
        BatchDecision::Filter,
        &[
            RecordDecision::Keep,
            RecordDecision::Drop,
            RecordDecision::Drop,
            RecordDecision::Keep,
        ],
    );
}

#[test]
fn exclude_offset_only_applies_to_its_named_partition() {
    let predicates = predicates(&["--exclude-offset", "orders:0=1001..1003"]);
    let orders_1 = partition("orders", 1);
    let owned = batch(1, vec![record(1)]);

    let (batch_decision, records) = decide(&predicates, &orders_1, &owned);

    check!(batch_decision == BatchDecision::Keep);
    check!(records == [RecordDecision::Keep]);
}

fn check_timestamp_records(
    timestamps: &[i64],
    expected_batch: BatchDecision,
    expected_records: &[RecordDecision],
) {
    let bound = BASE_TIMESTAMP + 100;
    let predicates = predicates(&["--to-timestamp", &bound.to_string()]);
    let owned = batch(1, timestamped_records(timestamps));
    check_decision(
        &predicates,
        &partition("orders", 0),
        &owned,
        expected_batch,
        expected_records,
    );
}

#[test]
fn to_timestamp_entirely_before_the_bound_keeps_the_batch() {
    check_timestamp_records(
        &[0, 50],
        BatchDecision::Keep,
        &[RecordDecision::Keep, RecordDecision::Keep],
    );
}

#[test]
fn to_timestamp_entirely_at_or_after_the_bound_empties_the_batch() {
    check_timestamp_records(
        &[100, 200],
        BatchDecision::Empty,
        &[RecordDecision::Drop, RecordDecision::Drop],
    );
}

#[test]
fn to_timestamp_straddling_the_bound_filters_the_right_split() {
    check_timestamp_records(
        &[0, 100, 150],
        BatchDecision::Filter,
        &[
            RecordDecision::Keep,
            RecordDecision::Drop,
            RecordDecision::Drop,
        ],
    );
}

#[test]
fn to_timestamp_judges_log_append_time_records_by_the_batch_max_timestamp() {
    let orders_0 = partition("orders", 0);
    let mut owned = batch(1, timestamped_records(&[0, 50]));
    owned.attributes = owned
        .attributes
        .with_timestamp_type(TimestampType::LogAppendTime);
    owned.base_timestamp = BASE_TIMESTAMP + 10_000;
    owned.max_timestamp = BASE_TIMESTAMP;

    for (name, bound, expected_batch, expected_record) in [
        (
            "append time before the bound",
            BASE_TIMESTAMP + 1,
            BatchDecision::Keep,
            RecordDecision::Keep,
        ),
        (
            "append time at the bound",
            BASE_TIMESTAMP,
            BatchDecision::Empty,
            RecordDecision::Drop,
        ),
    ] {
        let predicates = predicates(&["--to-timestamp", &bound.to_string()]);
        let (batch_decision, records) = decide(&predicates, &orders_0, &owned);
        check!(batch_decision == expected_batch, "{name}");
        check!(records == [expected_record, expected_record], "{name}");
    }
}

#[test]
fn predicates_that_both_match_one_record_still_drop_it_once() {
    let predicates = predicates(&["--exclude-key", "^bad", "--exclude-producer-id", "9"]);
    let orders_0 = partition("orders", 0);
    let owned = batch(9, keyed_records(&[Some(b"bad-1")]));

    check_decision(
        &predicates,
        &orders_0,
        &owned,
        BatchDecision::Empty,
        &[RecordDecision::Drop],
    );
}

#[test]
fn non_utf8_key_bytes_never_match_and_do_not_panic() {
    let predicates = predicates(&["--exclude-key", ".*"]);
    let orders_0 = partition("orders", 0);
    let invalid_utf8: &[u8] = &[0xFF, 0xFE, 0xFD];
    let owned = batch(
        1,
        vec![Record {
            key: Some(Bytes::copy_from_slice(invalid_utf8)),
            ..record(0)
        }],
    );

    check_decision(
        &predicates,
        &orders_0,
        &owned,
        BatchDecision::Keep,
        &[RecordDecision::Keep],
    );
}

#[test]
fn record_offset_outside_the_declared_batch_is_an_integrity_error() {
    let predicates = predicates(&["--exclude-key", "never"]);
    let orders_0 = partition("orders", 0);
    let mut owned = batch(1, vec![record(1)]);
    owned.last_offset_delta = 0;

    check_record_parse_error(&predicates, &orders_0, &owned);
}

#[test]
fn record_offset_overflow_is_an_integrity_error() {
    let predicates = predicates(&["--exclude-key", "never"]);
    let orders_0 = partition("orders", 0);
    let mut owned = batch(1, vec![record(1)]);
    owned.base_offset = i64::MAX;

    check_record_parse_error(&predicates, &orders_0, &owned);
}

#[test]
fn record_timestamp_overflow_is_an_integrity_error() {
    let predicates = predicates(&["--to-timestamp", "0"]);
    let orders_0 = partition("orders", 0);
    let mut owned = batch(1, timestamped_records(&[1]));
    owned.base_timestamp = i64::MAX;
    owned.max_timestamp = i64::MAX;

    check_record_parse_error(&predicates, &orders_0, &owned);
}
