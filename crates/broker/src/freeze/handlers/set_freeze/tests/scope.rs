//! Tests for the rules that read the request alone.
//!
//! The pattern-type byte, the scope it names, the registry ceiling that a new
//! entry meets, and the registry record that the pair becomes.

use {
    super::{
        super::checks::{check_limit, check_scope, live_entry, prepare, record_of},
        ALICE, ALICE_KEY, AliceKeyConfiguration, PROPOSAL, alice_key_configuration, freeze_fixture,
        freeze_request, image, with_alice_freeze_env,
    },
    crate::{
        codes,
        config::{BrokerConfig, FreezeConfig},
        time_util::now_ms,
    },
    assert2::check,
    krabka_metadata::{PatternType, TopicFreezeRecord},
    krabka_protocol::{
        krabka::freeze::{PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED, SetTopicFreezeRequest},
        primitives::uuid::Uuid as ProtocolUuid,
    },
};

#[yare::parameterized(
    topic_name = { PatternType::Literal, "orders", None },
    namespace_prefix = { PatternType::Prefixed, "tenant-a.", None },
    empty_literal_scope = {
        PatternType::Literal,
        "",
        Some(codes::FREEZE_SCOPE_INVALID),
    },
    empty_prefix_scope = {
        PatternType::Prefixed,
        "",
        Some(codes::FREEZE_SCOPE_INVALID),
    },
    literal_internal_topic = {
        PatternType::Literal,
        "__consumer_offsets",
        Some(codes::FREEZE_SCOPE_INVALID),
    },
    prefix_reaching_internal_namespace = {
        PatternType::Prefixed,
        "_",
        Some(codes::FREEZE_SCOPE_INVALID),
    },
    prefix_inside_internal_namespace = {
        PatternType::Prefixed,
        "__con",
        Some(codes::FREEZE_SCOPE_INVALID),
    },
)]
fn unusable_scopes_are_refused_as_invalid(
    pattern_type: PatternType,
    scope: &str,
    expected: Option<i16>,
) {
    let outcome = check_scope(pattern_type, scope);
    check!(outcome.err().map(|r| r.code) == expected);
}

#[yare::parameterized(
    room_for_another_entry = { 3, None },
    registry_full = { 2, Some(codes::FREEZE_LIMIT_EXCEEDED) },
    lowered_ceiling = { 1, Some(codes::FREEZE_LIMIT_EXCEEDED) },
)]
fn the_registry_ceiling_refuses_one_entry_past_it(max_entries: usize, expected: Option<i16>) {
    let image = image(&[
        ("orders", PatternType::Literal),
        ("payments", PatternType::Literal),
    ]);

    let outcome = check_limit(&image, max_entries);
    check!(outcome.err().map(|r| r.code) == expected);
}

#[yare::parameterized(
    same_scope_replaces = { "orders", None },
    second_scope_exceeds_ceiling = {
        "payments",
        Some(codes::FREEZE_LIMIT_EXCEEDED),
    },
)]
fn a_freeze_that_replaces_a_live_entry_does_not_meet_the_ceiling(
    scope: &str,
    expected: Option<i16>,
) {
    let AliceKeyConfiguration {
        dir: _dir, config, ..
    } = alice_key_configuration();
    let config = BrokerConfig {
        freeze: FreezeConfig {
            max_entries: 1,
            ..FreezeConfig::default()
        },
        ..config
    };
    let fixture = freeze_fixture(image(&[("orders", PatternType::Literal)]));
    let ctx = fixture.context();
    let env = fixture.environment(&config, &ctx);

    let outcome = prepare(&env, &freeze_request(scope, PATTERN_TYPE_LITERAL));
    check!(outcome.err().map(|r| r.code) == expected);
}

#[yare::parameterized(
    literal_entry = {
        PatternType::Literal,
        "orders",
        Some(PatternType::Literal),
    },
    prefixed_entry_same_name = {
        PatternType::Prefixed,
        "orders",
        Some(PatternType::Prefixed),
    },
    prefixed_entry = {
        PatternType::Prefixed,
        "tenant-a.",
        Some(PatternType::Prefixed),
    },
    literal_entry_covered_by_prefix = { PatternType::Literal, "tenant-a.", None },
    absent_scope = { PatternType::Literal, "absent", None },
    exact_lookup_not_longer_name = { PatternType::Prefixed, "tenant-a.orders", None },
)]
fn a_live_entry_is_found_by_its_scope_and_its_pattern_type_together(
    pattern_type: PatternType,
    scope: &str,
    expected: Option<PatternType>,
) {
    let image = image(&[
        ("orders", PatternType::Literal),
        ("orders", PatternType::Prefixed),
        ("tenant-a.", PatternType::Prefixed),
    ]);

    check!(live_entry(&image, pattern_type, scope).map(|entry| entry.pattern_type) == expected);
}

#[test]
fn the_record_takes_the_authenticated_principal_as_its_author() {
    let req = SetTopicFreezeRequest {
        scope: "orders".to_owned(),
        pattern_type: PATTERN_TYPE_LITERAL,
        frozen: true,
        reason: "DR cutover".to_owned(),
        proposal_id: ProtocolUuid(PROPOSAL.into_bytes()),
        set_at_ms: 1_770_000_000_000,
        key_id: ALICE_KEY.to_owned(),
        signature: vec![0xAB; 64],
        ..SetTopicFreezeRequest::default()
    };

    let expected = TopicFreezeRecord {
        scope: "orders".to_owned(),
        pattern_type: PatternType::Literal,
        frozen: true,
        reason: "DR cutover".to_owned(),
        set_by: ALICE.to_owned(),
        set_at_ms: 1_770_000_000_000,
        proposal_id: PROPOSAL,
        key_id: ALICE_KEY.to_owned(),
        signature: vec![0xAB; 64],
    };
    check!(record_of(&req, PatternType::Literal, ALICE) == expected);
}

#[test]
fn an_unsigned_record_takes_the_brokers_clock() {
    let req = SetTopicFreezeRequest {
        // A client that sends a timestamp in the far future cannot park an
        // entry where no later record replaces it.
        set_at_ms: i64::MAX,
        ..freeze_request("orders", PATTERN_TYPE_LITERAL)
    };

    let before = now_ms();
    let record = record_of(&req, PatternType::Literal, ALICE);

    check!(record.set_at_ms >= before);
    check!(record.set_at_ms < i64::MAX);
    check!(record.key_id.is_empty());
    check!(record.signature.is_empty());
}

#[test]
fn a_pattern_type_byte_that_names_no_scope_kind_is_an_invalid_request() {
    struct PatternCase {
        label: &'static str,
        byte: i8,
        error: Option<i16>,
    }
    with_alice_freeze_env(
        || image(&[]),
        |_fixture, env| {
            for case in [
                PatternCase {
                    label: "literal",
                    byte: PATTERN_TYPE_LITERAL,
                    error: None,
                },
                PatternCase {
                    label: "prefixed",
                    byte: PATTERN_TYPE_PREFIXED,
                    error: None,
                },
                PatternCase {
                    label: "any",
                    byte: 1,
                    error: Some(codes::INVALID_REQUEST),
                },
                PatternCase {
                    label: "unknown",
                    byte: 0,
                    error: Some(codes::INVALID_REQUEST),
                },
                PatternCase {
                    label: "match",
                    byte: 2,
                    error: Some(codes::INVALID_REQUEST),
                },
                PatternCase {
                    label: "a byte no build knows",
                    byte: 9,
                    error: Some(codes::INVALID_REQUEST),
                },
            ] {
                let outcome = prepare(env, &freeze_request("orders", case.byte));
                check!(
                    outcome.err().map(|r| r.code) == case.error,
                    "{}",
                    case.label
                );
            }
        },
    );
}
