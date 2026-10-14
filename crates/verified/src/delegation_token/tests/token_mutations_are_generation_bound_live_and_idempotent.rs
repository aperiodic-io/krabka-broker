use super::*;

const RENEW_EXPECTED: TokenMutationFacts = TokenMutationFacts {
    kind: TokenMutationKind::Renew,
    state: TokenMutationState::Expected,
    now_ms: 100,
    expected_expiry_ms: 150,
    incoming_expiry_ms: 175,
    max_timestamp_ms: 200,
    uncommitted_tail: false,
};

const EXPIRE_EXPECTED: TokenMutationFacts = TokenMutationFacts {
    kind: TokenMutationKind::Expire,
    ..RENEW_EXPECTED
};

#[yare::parameterized(
    renew_append_current = { RENEW_EXPECTED, TokenMutationDecision::Append },
    renew_append_earlier_deadline = { TokenMutationFacts {
        now_ms: 0, expected_expiry_ms: 50, incoming_expiry_ms: 75, max_timestamp_ms: 100,
        ..RENEW_EXPECTED
    }, TokenMutationDecision::Append },
    renew_may_shorten_expiry = { TokenMutationFacts {
        incoming_expiry_ms: 149, ..RENEW_EXPECTED
    }, TokenMutationDecision::Append },
    renew_deadline_equal_to_now = { TokenMutationFacts {
        incoming_expiry_ms: 100, ..RENEW_EXPECTED
    }, TokenMutationDecision::Append },
    renew_expected_deadline_equal_to_now = { TokenMutationFacts {
        expected_expiry_ms: 100, ..RENEW_EXPECTED
    }, TokenMutationDecision::Append },
    renew_max_timestamp_equal_to_expiry = { TokenMutationFacts {
        expected_expiry_ms: 100, incoming_expiry_ms: 110, max_timestamp_ms: 110,
        ..RENEW_EXPECTED
    }, TokenMutationDecision::Append },
    renew_retry_at_expected_expiry = { TokenMutationFacts {
        incoming_expiry_ms: 150, ..RENEW_EXPECTED
    }, TokenMutationDecision::Retry },
    renew_retry_at_now = { TokenMutationFacts {
        expected_expiry_ms: 100, incoming_expiry_ms: 100, max_timestamp_ms: 100,
        ..RENEW_EXPECTED
    }, TokenMutationDecision::Retry },
    renew_rejects_negative_now = { TokenMutationFacts {
        now_ms: -1, ..RENEW_EXPECTED
    }, TokenMutationDecision::Reject },
    renew_rejects_stale_state = { TokenMutationFacts {
        state: TokenMutationState::Stale, ..RENEW_EXPECTED
    }, TokenMutationDecision::Reject },
    renew_rejects_deadline_before_now = { TokenMutationFacts {
        incoming_expiry_ms: 99, ..RENEW_EXPECTED
    }, TokenMutationDecision::Reject },
    renew_rejects_deadline_after_max_timestamp = { TokenMutationFacts {
        incoming_expiry_ms: i64::MAX, ..RENEW_EXPECTED
    }, TokenMutationDecision::Reject },
    renew_rejects_expired_expected_state = { TokenMutationFacts {
        expected_expiry_ms: 99, ..RENEW_EXPECTED
    }, TokenMutationDecision::Reject },
    renew_rejects_expiry_before_max_timestamp = { TokenMutationFacts {
        max_timestamp_ms: 99, expected_expiry_ms: 99, incoming_expiry_ms: 99,
        ..RENEW_EXPECTED
    }, TokenMutationDecision::Reject },
    renew_rejects_expected_expiry_after_max = { TokenMutationFacts {
        expected_expiry_ms: 201, max_timestamp_ms: 200, ..RENEW_EXPECTED
    }, TokenMutationDecision::Reject },
    renew_rejects_uncommitted_tail = { TokenMutationFacts {
        uncommitted_tail: true, ..RENEW_EXPECTED
    }, TokenMutationDecision::Reject },
    delete_missing_retries = { TokenMutationFacts {
        kind: TokenMutationKind::Delete, state: TokenMutationState::Missing, ..RENEW_EXPECTED
    }, TokenMutationDecision::Retry },
    delete_expected_appends = { TokenMutationFacts {
        kind: TokenMutationKind::Delete, expected_expiry_ms: 100, ..RENEW_EXPECTED
    }, TokenMutationDecision::Append },
    expire_append_current = { EXPIRE_EXPECTED, TokenMutationDecision::Append },
    expire_append_earlier_deadline = { TokenMutationFacts {
        now_ms: 0, expected_expiry_ms: 50, incoming_expiry_ms: 0, max_timestamp_ms: 100,
        ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Append },
    expire_deadline_equal_to_now = { TokenMutationFacts {
        expected_expiry_ms: 100, ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Append },
    expire_max_timestamp_equal_to_expiry = { TokenMutationFacts {
        expected_expiry_ms: 100, incoming_expiry_ms: 100, max_timestamp_ms: 100,
        ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Append },
    expire_rejects_negative_now = { TokenMutationFacts {
        now_ms: -1, ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Reject },
    expire_rejects_expired_expected_state = { TokenMutationFacts {
        expected_expiry_ms: 99, ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Reject },
    expire_rejects_max_before_expected = { TokenMutationFacts {
        max_timestamp_ms: 99, ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Reject },
    expire_rejects_expected_expiry_after_max = { TokenMutationFacts {
        expected_expiry_ms: 201, max_timestamp_ms: 200, ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Reject },
    expire_rejects_negative_incoming_expiry = { TokenMutationFacts {
        incoming_expiry_ms: -1, ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Reject },
    expire_rejects_incoming_expiry_after_max = { TokenMutationFacts {
        incoming_expiry_ms: 201, max_timestamp_ms: 200, ..EXPIRE_EXPECTED
    }, TokenMutationDecision::Reject },
)]
fn token_mutations_are_generation_bound_live_and_idempotent(
    facts: TokenMutationFacts,
    expected: TokenMutationDecision,
) {
    check!(token_mutation_decision(facts) == expected, "{facts:?}");
}

/// `DelegationTokenControlManager.expireDelegationToken`: a negative period
/// deletes before the expiry check, and a live token gets
/// `min(max, now + period)` with a saturating sum.
#[yare::parameterized(
    zero_period_at_zero = { 0, 0, 150, 200, TokenExpireDecision::Update(0) },
    zero_period = { 100, 0, 150, 200, TokenExpireDecision::Update(100) },
    positive_period = { 100, 25, 150, 200, TokenExpireDecision::Update(125) },
    period_capped_at_max = { 100, 500, 150, 200, TokenExpireDecision::Update(200) },
    near_overflow_is_capped = { 100, i64::MAX - 100, 150, 200, TokenExpireDecision::Update(200) },
    saturating_overflow = { 100, i64::MAX, 150, i64::MAX, TokenExpireDecision::Update(i64::MAX) },
    zero_period_at_expiry = { 100, 0, 100, 200, TokenExpireDecision::Update(100) },
    zero_period_at_max = { 100, 0, 100, 100, TokenExpireDecision::Update(100) },
    negative_period_deletes_live = { 100, -1, 150, 200, TokenExpireDecision::Delete },
    negative_period_deletes_expired = { 100, -1, 50, 60, TokenExpireDecision::Delete },
    minimum_period_deletes = { 100, i64::MIN, 150, 200, TokenExpireDecision::Delete },
    current_expiry_is_past = { 100, 0, 99, 200, TokenExpireDecision::Expired },
    max_timestamp_is_past = { 100, 0, 150, 99, TokenExpireDecision::Expired },
)]
fn expire_matches_kafka_delete_expiry_and_saturation(
    now: i64,
    period: i64,
    current: i64,
    max: i64,
    expected: TokenExpireDecision,
) {
    check!(expire_token_deadline(now, period, current, max) == expected);
}

#[test]
fn active_tokens_require_both_live_ordered_deadlines() {
    check!(token_is_active(0, 50, 100));
    check!(!token_is_active(-1, 50, 100));
    check!(token_is_active(100, 150, 200));
    check!(!token_is_active(100, 100, 200));
    check!(!token_is_active(100, 150, 100));
    check!(expire_token_deadline(100, 50, 200, 200) == TokenExpireDecision::Update(150));
    check!(renew_token_expiry(100, 50, 50, 200, 200) == TokenRenewDecision::Renew(150));
    let renew_at_max = TokenMutationFacts {
        state: TokenMutationState::Expected,
        kind: TokenMutationKind::Renew,
        now_ms: 100,
        expected_expiry_ms: 200,
        incoming_expiry_ms: 200,
        max_timestamp_ms: 200,
        uncommitted_tail: false,
    };
    check!(token_mutation_decision(renew_at_max) == TokenMutationDecision::Retry);
    let renew_over_max = TokenMutationFacts {
        expected_expiry_ms: 250,
        incoming_expiry_ms: 250,
        ..renew_at_max
    };
    check!(token_mutation_decision(renew_over_max) == TokenMutationDecision::Reject);
    check!(!token_is_active(100, 201, 200));
}
