//! Tests for the approver set fingerprint.
//!
//! The fingerprint ignores the order of the configured list and the repeats in
//! it, and it changes when the membership changes.

use {crate::operator_keys::approver_set_fingerprint, assert2::check};

#[test]
fn approver_set_fingerprint_ignores_order_and_tracks_membership() {
    let base = ["User:alice", "User:bob", "User:carol"].map(str::to_owned);
    let baseline = approver_set_fingerprint(&base);

    for (name, approvers, expected_equal) in [
        (
            "the same set",
            vec!["User:alice", "User:bob", "User:carol"],
            true,
        ),
        (
            "reversed",
            vec!["User:carol", "User:bob", "User:alice"],
            true,
        ),
        (
            "shuffled with a repeat",
            vec!["User:bob", "User:carol", "User:alice", "User:bob"],
            true,
        ),
        (
            "one member added",
            vec!["User:alice", "User:bob", "User:carol", "User:dave"],
            false,
        ),
        ("one member removed", vec!["User:alice", "User:bob"], false),
        (
            "one member renamed",
            vec!["User:alice", "User:bob", "User:carla"],
            false,
        ),
        (
            "the same characters split differently",
            vec!["User:ali", "ceUser:bob", "User:carol"],
            false,
        ),
        ("empty", vec![], false),
    ] {
        let candidate: Vec<String> = approvers.into_iter().map(str::to_owned).collect();
        check!(
            (approver_set_fingerprint(&candidate) == baseline) == expected_equal,
            "case {name}"
        );
    }
    check!(baseline.len() == 64);
    check!(baseline.chars().all(|c| c.is_ascii_hexdigit()));
}
