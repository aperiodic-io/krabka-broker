use super::*;

#[yare::parameterized(
    no_filter_even_owner = { false, true, false, false, false, false },
    no_filter_even_requester = { false, false, true, false, false, false },
    owner_with_filter = { true, true, false, false, false, true },
    requester_with_filter = { true, false, true, false, false, true },
    renewer_with_filter = { true, false, false, true, false, true },
    acl_with_filter = { true, false, false, false, true, true },
    unrelated_with_filter = { true, false, false, false, false, false },
)]
fn token_visibility_requires_owner_filter_plus_a_relationship(
    filter: bool,
    owner: bool,
    requester: bool,
    renewer: bool,
    acl: bool,
    visible: bool,
) {
    check!(token_describe_visible(filter, owner, requester, renewer, acl) == visible);
}

#[yare::parameterized(
    regular_credential_wins = { true, false, true, false, ScramCredentialSource::Regular },
    active_token_fallback = { false, true, true, true, ScramCredentialSource::DelegationToken },
    expired_token_fallback = {
        false,
        true,
        true,
        false,
        ScramCredentialSource::ExpiredDelegationToken,
    },
    non_token_mechanism = { false, false, true, true, ScramCredentialSource::Unknown },
    no_token_credential = { false, true, false, true, ScramCredentialSource::Unknown },
)]
fn scram_source_prefers_regular_and_limits_token_fallback(
    regular: bool,
    token_mechanism: bool,
    token: bool,
    active: bool,
    expected: ScramCredentialSource,
) {
    check!(scram_credential_source(regular, token_mechanism, token, active) == expected);
}

#[yare::parameterized(
    create = { TokenApi::Create },
    renew = { TokenApi::Renew },
    expire = { TokenApi::Expire },
    describe = { TokenApi::Describe },
)]
fn token_api_admission_requires_identity_and_blocks_every_token_authed_call(api: TokenApi) {
    check!(token_api_admission(false, false, api) == TokenApiAdmission::Reject);
    check!(token_api_admission(false, true, api) == TokenApiAdmission::Reject);
    check!(token_api_admission(true, false, api) == TokenApiAdmission::Allow);
    // KafkaApis.allowTokenRequests refuses every delegation-token API,
    // Describe included, to a token-authenticated caller.
    check!(token_api_admission(true, true, api) == TokenApiAdmission::Reject);
}

/// `DelegationTokenControlManager.createDelegationToken`: a request of 0
/// or less takes the configured lifetime, a positive one the smaller of
/// the two, and both sums saturate at `i64::MAX`.
#[yare::parameterized(
    negative_request_uses_ceiling = { 0, -1, 1_000, 100, created(1_000, 100) },
    later_time_adds_deadlines = { 100, -1, 1_000, 100, created(1_100, 200) },
    zero_request_uses_ceiling = { 100, 0, 1_000, 100, created(1_100, 200) },
    other_negative_request_uses_ceiling = { 100, -2, 1_000, 100, created(1_100, 200) },
    minimum_request_uses_ceiling = { 100, i64::MIN, 1_000, 100, created(1_100, 200) },
    positive_request_caps_lifetime = { 100, 50, 1_000, 100, created(150, 150) },
    oversized_request_uses_ceiling = { 100, 5_000, 1_000, 100, created(1_100, 200) },
    renewal_period_can_extend_expiry = { 100, -1, 100, 101, created(200, 200) },
    maximum_expiry_saturates = { 100, -1, i64::MAX - 100, 100, created(i64::MAX, 200) },
    both_deadlines_saturate = { i64::MAX, -1, 1, 1, created(i64::MAX, i64::MAX) },
    only_expiry_saturates = { 1, -1, i64::MAX, 1, created(i64::MAX, 2) },
    expiry_and_renewal_saturate = { 1, -1, i64::MAX, i64::MAX, created(i64::MAX, i64::MAX) },
    zero_ceiling_is_invalid = { 100, -1, 0, 100, TokenCreateDecision::Invalid },
    zero_renewal_is_invalid = { 100, -1, 1_000, 0, TokenCreateDecision::Invalid },
    negative_ceiling_is_invalid = { 100, -1, -1, 100, TokenCreateDecision::Invalid },
)]
fn create_matches_kafka_lifetime_and_saturation(
    now: i64,
    requested: i64,
    ceiling: i64,
    renew: i64,
    expected: TokenCreateDecision,
) {
    check!(create_token_deadlines(now, requested, ceiling, renew) == expected);
}

/// `DelegationTokenControlManager.renewDelegationToken`: `Expired` only
/// for a deadline strictly before `now`; the expiry is
/// `min(max, now + min(default, period))` for a positive period and
/// `min(max, now + default)` otherwise, even when that shortens it.
#[yare::parameterized(
    requested_shorter_period = { 100, 25, 50, 150, 1_000, TokenRenewDecision::Renew(125) },
    requested_longer_period = { 100, 75, 50, 150, 1_000, TokenRenewDecision::Renew(150) },
    requested_much_longer_period = { 100, 500, 50, 150, 1_000, TokenRenewDecision::Renew(150) },
    negative_period_uses_default = { 100, -1, 50, 150, 1_000, TokenRenewDecision::Renew(150) },
    zero_period_uses_default = { 100, 0, 50, 150, 1_000, TokenRenewDecision::Renew(150) },
    other_negative_period_uses_default = {
        100,
        -5,
        50,
        150,
        1_000,
        TokenRenewDecision::Renew(150),
    },
    short_period_can_shorten_expiry = { 100, 10, 50, 900, 1_000, TokenRenewDecision::Renew(110) },
    capped_by_maximum = { 100, 500, 1_000, 150, 200, TokenRenewDecision::Renew(200) },
    saturated_expiry_capped_by_maximum = {
        100,
        i64::MAX,
        i64::MAX,
        150,
        200,
        TokenRenewDecision::Renew(200),
    },
    saturated_expiry = {
        1,
        i64::MAX,
        i64::MAX,
        150,
        i64::MAX,
        TokenRenewDecision::Renew(i64::MAX),
    },
    deadline_equal_to_now_is_live = { 100, 25, 50, 100, 1_000, TokenRenewDecision::Renew(125) },
    max_equal_to_now_caps_renewal = { 100, 25, 50, 100, 100, TokenRenewDecision::Renew(100) },
    expired_deadline = { 100, 25, 50, 99, 1_000, TokenRenewDecision::Expired },
    max_before_now_is_expired = { 100, 25, 50, 150, 99, TokenRenewDecision::Expired },
    zero_default_cannot_renew_expired = { 100, 25, 0, 99, 1_000, TokenRenewDecision::Expired },
    zero_default_is_invalid_while_live = { 100, 25, 0, 150, 1_000, TokenRenewDecision::Invalid },
)]
fn renew_matches_kafka_period_cap_and_expiry(
    now: i64,
    requested: i64,
    default: i64,
    current: i64,
    max: i64,
    expected: TokenRenewDecision,
) {
    check!(renew_token_expiry(now, requested, default, current, max) == expected);
}
