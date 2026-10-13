use super::*;

#[test]
fn skew_window_is_symmetric_and_overflow_safe() {
    assert2::check!(freeze_timestamp_in_window(i64::MAX, i64::MAX, i64::MAX));
    assert2::check!(!freeze_timestamp_in_window(i64::MIN, i64::MAX, i64::MAX));
    assert2::check!(!freeze_timestamp_in_window(i64::MAX, i64::MIN, i64::MAX));
    assert2::check!(!freeze_timestamp_in_window(0, 0, -1));
}

#[test]
fn signature_rules_return_the_first_failure() {
    let valid = FreezeSignatureFacts {
        identity: FreezeIdentityState::Bound,
        set_at_ms: 100,
        now_ms: 100,
        max_skew_ms: 10,
        replaces: true,
        replaced_set_at_ms: 99,
        signature_valid: true,
    };
    for (facts, expected) in [
        (
            FreezeSignatureFacts {
                identity: FreezeIdentityState::UnknownKey,
                ..valid
            },
            FreezeSignatureDecision::UnknownKey,
        ),
        (
            FreezeSignatureFacts {
                identity: FreezeIdentityState::WrongKeyPrincipal,
                ..valid
            },
            FreezeSignatureDecision::AuthorIsNotKeyPrincipal,
        ),
        (
            FreezeSignatureFacts {
                identity: FreezeIdentityState::WrongConnectionPrincipal,
                ..valid
            },
            FreezeSignatureDecision::AuthorIsNotConnectionPrincipal,
        ),
        (
            FreezeSignatureFacts {
                set_at_ms: 111,
                ..valid
            },
            FreezeSignatureDecision::TimestampOutsideSkewWindow,
        ),
        (
            FreezeSignatureFacts {
                set_at_ms: 99,
                ..valid
            },
            FreezeSignatureDecision::TimestampNotNewer,
        ),
        (
            FreezeSignatureFacts {
                signature_valid: false,
                ..valid
            },
            FreezeSignatureDecision::SignatureInvalid,
        ),
        (valid, FreezeSignatureDecision::Admit),
    ] {
        assert2::check!(freeze_signature_decision(facts) == expected);
    }
}

#[test]
fn scope_precedence_is_literal_then_longest_prefix() {
    assert2::check!(
        freeze_scope_decision(FreezeScopeRank::NoMatch, FreezeScopeRank::Prefix {
            length: 3
        },) == FreezeScopeDecision::Replace
    );
    assert2::check!(
        freeze_scope_decision(
            FreezeScopeRank::Prefix { length: 3 },
            FreezeScopeRank::Prefix { length: 4 },
        ) == FreezeScopeDecision::Replace
    );
    assert2::check!(
        freeze_scope_decision(
            FreezeScopeRank::Prefix { length: 4 },
            FreezeScopeRank::Prefix { length: 3 },
        ) == FreezeScopeDecision::Keep
    );
    assert2::check!(
        freeze_scope_decision(
            FreezeScopeRank::Prefix { length: 4 },
            FreezeScopeRank::Prefix { length: 4 },
        ) == FreezeScopeDecision::Keep
    );
    assert2::check!(
        freeze_scope_decision(
            FreezeScopeRank::Prefix { length: 4 },
            FreezeScopeRank::Literal
        ) == FreezeScopeDecision::Replace
    );
    assert2::check!(
        freeze_scope_decision(FreezeScopeRank::Literal, FreezeScopeRank::Prefix {
            length: 5
        }) == FreezeScopeDecision::Keep
    );
}

#[test]
fn timestamp_window_boundaries() {
    assert2::check!(freeze_timestamp_in_window(100, 100, 0));
    assert2::check!(!freeze_timestamp_in_window(100, 100, -1));
    assert2::check!(!freeze_timestamp_in_window(101, 100, 0));
}

#[test]
fn mutation_inventory_ranks_authorization_then_freeze() {
    use {
        FreezeMutationDecision::{Admit, AuthorizationDenied, Frozen},
        FreezeMutationKind::{
            BarrierMarker, Compaction, DeleteRecords, DeleteTopic, OffsetCommit, Produce,
            ReassignmentAlter, ReassignmentCompletion, Replication, Retention, TieringCopy,
            TransactionCompletion, TransactionEnlistment,
        },
    };

    let refused = [
        Produce,
        TransactionEnlistment,
        DeleteRecords,
        DeleteTopic,
        ReassignmentAlter,
        Compaction,
        Retention,
    ];
    let allowed = [
        TransactionCompletion,
        ReassignmentCompletion,
        OffsetCommit,
        Replication,
        BarrierMarker,
        TieringCopy,
    ];

    for kind in refused {
        assert2::check!(freeze_mutation_decision(false, true, kind) == AuthorizationDenied);
        assert2::check!(freeze_mutation_decision(true, true, kind) == Frozen);
        assert2::check!(freeze_mutation_decision(true, false, kind) == Admit);
    }
    for kind in allowed {
        assert2::check!(freeze_mutation_decision(false, true, kind) == AuthorizationDenied);
        assert2::check!(freeze_mutation_decision(true, true, kind) == Admit);
        assert2::check!(freeze_mutation_decision(true, false, kind) == Admit);
    }
}

#[test]
fn replacements_require_a_live_thaw_target_a_newer_stamp_and_no_tail() {
    let valid = FreezeReplacementFacts {
        stored: FreezeStoredState::Present { set_at_ms: 9 },
        incoming_frozen: true,
        incoming_set_at_ms: 10,
        uncommitted_tail: false,
    };
    for (facts, expected) in [
        (
            FreezeReplacementFacts {
                stored: FreezeStoredState::Missing,
                incoming_frozen: false,
                ..valid
            },
            FreezeReplacementDecision::Missing,
        ),
        (
            FreezeReplacementFacts {
                incoming_set_at_ms: 9,
                ..valid
            },
            FreezeReplacementDecision::Stale,
        ),
        (
            FreezeReplacementFacts {
                uncommitted_tail: true,
                ..valid
            },
            FreezeReplacementDecision::InFlight,
        ),
        (valid, FreezeReplacementDecision::Append),
        (
            FreezeReplacementFacts {
                stored: FreezeStoredState::Missing,
                incoming_frozen: true,
                ..valid
            },
            FreezeReplacementDecision::Append,
        ),
        // A permanent refusal outranks the transient tail, so the
        // controller never retries a record that can never append.
        (
            FreezeReplacementFacts {
                stored: FreezeStoredState::Missing,
                incoming_frozen: false,
                uncommitted_tail: true,
                ..valid
            },
            FreezeReplacementDecision::Missing,
        ),
        (
            FreezeReplacementFacts {
                incoming_set_at_ms: 8,
                uncommitted_tail: true,
                ..valid
            },
            FreezeReplacementDecision::Stale,
        ),
        (
            FreezeReplacementFacts {
                stored: FreezeStoredState::Missing,
                uncommitted_tail: true,
                ..valid
            },
            FreezeReplacementDecision::InFlight,
        ),
    ] {
        assert2::check!(freeze_replacement_decision(facts) == expected);
    }
}
