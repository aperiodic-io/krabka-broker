//! Tests for the exit-code vocabulary: the numbers themselves, and the failure
//! kinds that choose one.

use {
    crate::{
        EXIT_BAD_SIGNATURE, EXIT_MISMATCH, EXIT_NO_APPROVAL, EXIT_REFUSED, EXIT_UNREACHABLE,
        failure::Failure,
    },
    assert2::check,
};

/// Every exit code is a distinct number, because a runbook that reads two
/// meanings out of one number is a runbook that does the wrong thing.
#[test]
fn the_exit_codes_are_distinct() {
    let codes = [
        ("refused", EXIT_REFUSED),
        ("unreachable", EXIT_UNREACHABLE),
        ("mismatch", EXIT_MISMATCH),
        ("no approval", EXIT_NO_APPROVAL),
        ("bad signature", EXIT_BAD_SIGNATURE),
    ];
    for (index, (left_name, left)) in codes.iter().enumerate() {
        for (right_name, right) in &codes[index + 1..] {
            check!(left != right, "{left_name} and {right_name}");
        }
    }
}

/// The three codes `krabka-barrier` also ships keep the numbers it gives
/// them, so one runbook can branch on both tools.
#[test]
fn the_shared_exit_codes_keep_the_barrier_meanings() {
    check!(EXIT_REFUSED == 1);
    check!(EXIT_UNREACHABLE == 2);
    check!(EXIT_MISMATCH == 3);
}

/// A transport failure says that nothing is known about the outcome, which
/// is the difference between it and a refusal.
#[yare::parameterized(
    transport = { Failure::Transport("gone".to_owned()), EXIT_UNREACHABLE },
    refusal = { Failure::Refused("no".to_owned()), EXIT_REFUSED },
    signature = { Failure::Signature("bad key".to_owned()), EXIT_BAD_SIGNATURE },
)]
fn a_failure_reports_its_own_exit_code(failure: Failure, expected: i32) {
    check!(failure.exit_code() == expected);
    check!(!failure.message().is_empty());
}
