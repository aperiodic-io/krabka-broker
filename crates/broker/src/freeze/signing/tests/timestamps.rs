//! Tests for the two rules that `set_at_ms` passes.
//!
//! The skew window is symmetric around this broker's clock and takes its width
//! from the configuration, and a record that replaces a live entry must carry
//! a newer stamp than the entry it replaces. Together they are what stops an
//! old signed thaw from being replayed.

use {
    super::{ALICE, CLUSTER_ID, NOW_MS, check_against, record, signed, trust},
    crate::freeze::signing::{FreezeSignatureCheck, verify_freeze_signature},
    assert2::check,
    krabka_metadata::TopicFreezeRecord,
    krabka_units::{Time, convert::TimeExt as _, minutes, secs},
};

#[yare::parameterized(
    exactly_past_edge = { NOW_MS - 300_000, true },
    exactly_future_edge = { NOW_MS + 300_000, true },
    one_millisecond_past_edge = { NOW_MS - 300_001, false },
    one_millisecond_after_edge = { NOW_MS + 300_001, false },
    same_moment = { NOW_MS, true },
)]
fn timestamp_skew_boundary_is_checked(set_at_ms: i64, expected: bool) {
    let trust = trust();
    let window = minutes(5);
    let signed_record = signed(&trust.alice, CLUSTER_ID, &TopicFreezeRecord {
        set_at_ms,
        ..record()
    });
    let check = FreezeSignatureCheck {
        max_skew: window,
        ..check_against(&trust, ALICE)
    };
    assert2::check!(verify_freeze_signature(&check, &signed_record).is_ok() == expected);
}

#[yare::parameterized(
    wider_than_offset = { minutes(1), true },
    narrower_than_offset = { secs(10), false },
    zero_width = { secs(0), false },
)]
fn skew_window_uses_configured_width(max_skew: Time, expected: bool) {
    let trust = trust();
    let record = signed(&trust.alice, CLUSTER_ID, &TopicFreezeRecord {
        set_at_ms: NOW_MS - 30_000,
        ..record()
    });

    let check = FreezeSignatureCheck {
        max_skew,
        ..check_against(&trust, ALICE)
    };
    assert2::check!(verify_freeze_signature(&check, &record).is_ok() == expected);
}

#[test]
fn a_newer_timestamp_replaces_a_live_entry() {
    let trust = trust();
    let replaced = TopicFreezeRecord {
        set_at_ms: NOW_MS - 1,
        ..record()
    };
    let record = signed(&trust.alice, CLUSTER_ID, &record());
    let check = FreezeSignatureCheck {
        replaces: Some(&replaced),
        ..check_against(&trust, ALICE)
    };

    check!(verify_freeze_signature(&check, &record) == Ok(()));
}

#[test]
fn opposite_machine_extremes_never_wrap_into_the_skew_window() {
    let trust = trust();
    for (label, now_ms, set_at_ms) in [
        ("past extreme", i64::MAX, i64::MIN),
        ("future extreme", i64::MIN, i64::MAX),
    ] {
        let record = signed(&trust.alice, CLUSTER_ID, &TopicFreezeRecord {
            set_at_ms,
            ..record()
        });
        let check = FreezeSignatureCheck {
            now_ms,
            max_skew: Time::from_millis(i64::MAX),
            ..check_against(&trust, ALICE)
        };
        check!(
            verify_freeze_signature(&check, &record)
                == Err(crate::freeze::signing::SignatureRefusal::TimestampOutsideSkewWindow),
            "{label}"
        );
    }
}
