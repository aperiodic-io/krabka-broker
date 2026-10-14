use super::*;

/// Kafka `AclAuthorizer` precedence scenarios.
#[yare::parameterized(
    no_acl_default_denies = { facts(), AclDecision::DenyDefault },
    matching_allow = { AclFacts { saw_allow: true, ..facts() }, AclDecision::AllowAcl },
    matching_deny = { AclFacts { saw_deny: true, ..facts() }, AclDecision::DenyExplicit },
    deny_precedes_allow = {
        AclFacts { saw_allow: true, saw_deny: true, ..facts() },
        AclDecision::DenyExplicit,
    },
    superuser_bypasses_deny = {
        AclFacts { super_user: true, saw_deny: true, ..facts() },
        AclDecision::AllowSuperuser,
    },
    allow_default_without_acl = {
        AclFacts { default_decision: AclDefault::Allow, ..facts() },
        AclDecision::AllowNoAcl,
    },
    allow_default_does_not_override_deny = {
        AclFacts { saw_deny: true, default_decision: AclDefault::Allow, ..facts() },
        AclDecision::DenyExplicit,
    },
    explicit_allow_is_acl_grant = {
        AclFacts { saw_allow: true, default_decision: AclDefault::Allow, ..facts() },
        AclDecision::AllowAcl,
    },
)]
fn acl_precedence_matches_kafka_scenarios(facts: AclFacts, expected: AclDecision) {
    check!(acl_decision(facts) == expected);
}

/// Kafka principal and host matching: `User:*` and host `*` are wildcards.
#[yare::parameterized(
    exact_principal = { false, true, true },
    wildcard_principal = { true, false, true },
    different_principal = { false, false, false },
    wildcard_host = { true, false, true },
)]
fn identity_wildcards_match_kafka_scenarios(wildcard: bool, exact: bool, expected: bool) {
    check!(acl_identity_match(wildcard, exact) == expected);
}

/// Kafka LITERAL, wildcard, and PREFIXED resource patterns against the
/// requested topic `orders`.
#[yare::parameterized(
    literal_exact = {
        AclPatternKind::Literal,
        AclResourceFacts {
            resource_type: AclResourceTypeMatch::Same,
            exact_name: true,
            wildcard_name: false,
            name_has_prefix: true,
        },
        true,
    },
    literal_wildcard = {
        AclPatternKind::Literal,
        AclResourceFacts {
            resource_type: AclResourceTypeMatch::Same,
            exact_name: false,
            wildcard_name: true,
            name_has_prefix: false,
        },
        true,
    },
    literal_prefix_only = {
        AclPatternKind::Literal,
        AclResourceFacts {
            resource_type: AclResourceTypeMatch::Same,
            exact_name: false,
            wildcard_name: false,
            name_has_prefix: true,
        },
        false,
    },
    prefixed_prefix = {
        AclPatternKind::Prefixed,
        AclResourceFacts {
            resource_type: AclResourceTypeMatch::Same,
            exact_name: false,
            wildcard_name: false,
            name_has_prefix: true,
        },
        true,
    },
    prefixed_exact = {
        AclPatternKind::Prefixed,
        AclResourceFacts {
            resource_type: AclResourceTypeMatch::Same,
            exact_name: true,
            wildcard_name: false,
            name_has_prefix: true,
        },
        true,
    },
    prefixed_no_match = {
        AclPatternKind::Prefixed,
        AclResourceFacts {
            resource_type: AclResourceTypeMatch::Same,
            exact_name: false,
            wildcard_name: false,
            name_has_prefix: false,
        },
        false,
    },
    prefixed_wildcard_is_not_special = {
        AclPatternKind::Prefixed,
        AclResourceFacts {
            resource_type: AclResourceTypeMatch::Same,
            exact_name: false,
            wildcard_name: true,
            name_has_prefix: false,
        },
        false,
    },
    literal_wrong_type = {
        AclPatternKind::Literal,
        AclResourceFacts {
            resource_type: AclResourceTypeMatch::Different,
            exact_name: true,
            wildcard_name: false,
            name_has_prefix: true,
        },
        false,
    },
    prefixed_wrong_type = {
        AclPatternKind::Prefixed,
        AclResourceFacts {
            resource_type: AclResourceTypeMatch::Different,
            exact_name: false,
            wildcard_name: false,
            name_has_prefix: true,
        },
        false,
    },
)]
fn resource_patterns_match_kafka_scenarios(
    pattern: AclPatternKind,
    facts: AclResourceFacts,
    expected: bool,
) {
    check!(acl_resource_match(pattern, facts) == expected);
}

/// Kafka's operation-implication table (`AclAuthorizer` /
/// `AclEntry.supportedOperations`) for stored ALLOW and DENY ACLs.
#[yare::parameterized(
    same_operation_allow = { AclOperationKind::Read, AclOperationKind::Read, true, true },
    same_operation_deny = { AclOperationKind::Read, AclOperationKind::Read, false, true },
    all_cluster_action = { AclOperationKind::All, AclOperationKind::ClusterAction, true, true },
    all_describe_deny = { AclOperationKind::All, AclOperationKind::Describe, false, true },
    read_implies_describe = { AclOperationKind::Read, AclOperationKind::Describe, true, true },
    write_implies_describe = { AclOperationKind::Write, AclOperationKind::Describe, true, true },
    delete_implies_describe = { AclOperationKind::Delete, AclOperationKind::Describe, true, true },
    alter_implies_describe = { AclOperationKind::Alter, AclOperationKind::Describe, true, true },
    alter_configs_implies_describe_configs = {
        AclOperationKind::AlterConfigs,
        AclOperationKind::DescribeConfigs,
        true,
        true,
    },
    deny_does_not_gain_describe = {
        AclOperationKind::Read,
        AclOperationKind::Describe,
        false,
        false,
    },
    deny_does_not_gain_describe_configs = {
        AclOperationKind::AlterConfigs,
        AclOperationKind::DescribeConfigs,
        false,
        false,
    },
    describe_does_not_imply_read = {
        AclOperationKind::Describe,
        AclOperationKind::Read,
        true,
        false,
    },
    describe_configs_does_not_imply_alter_configs = {
        AclOperationKind::DescribeConfigs,
        AclOperationKind::AlterConfigs,
        true,
        false,
    },
    create_does_not_imply_describe = {
        AclOperationKind::Create,
        AclOperationKind::Describe,
        true,
        false,
    },
    write_does_not_imply_read = { AclOperationKind::Write, AclOperationKind::Read, true, false },
    alter_does_not_imply_alter_configs = {
        AclOperationKind::Alter,
        AclOperationKind::AlterConfigs,
        true,
        false,
    },
)]
fn operation_implications_match_kafka_scenarios(
    stored: AclOperationKind,
    requested: AclOperationKind,
    is_allow: bool,
    expected: bool,
) {
    check!(acl_operation_match(stored, requested, is_allow) == expected);
}

#[yare::parameterized(
    pre_handshake_sasl_handshake = { RequestAuthState::PreHandshake, 17, true },
    pre_handshake_sasl_authenticate = { RequestAuthState::PreHandshake, 18, true },
    pre_handshake_token_auth = { RequestAuthState::PreHandshake, 36, false },
    pre_handshake_negative = { RequestAuthState::PreHandshake, -1, false },
    pre_handshake_unknown = { RequestAuthState::PreHandshake, 0, false },
    exchanging_token_auth = { RequestAuthState::Exchanging, 36, true },
    exchanging_sasl_handshake = { RequestAuthState::Exchanging, 17, false },
    exchanging_sasl_authenticate = { RequestAuthState::Exchanging, 18, false },
    exchanging_other = { RequestAuthState::Exchanging, i16::MAX, false },
    failed_token_auth = { RequestAuthState::Failed, 36, false },
    failed_sasl_handshake = { RequestAuthState::Failed, 17, false },
    failed_sasl_authenticate = { RequestAuthState::Failed, 18, false },
    authenticated_zero = { RequestAuthState::Authenticated, 0, true },
    authenticated_maximum = { RequestAuthState::Authenticated, i16::MAX, true },
)]
fn request_auth_admission_truth_table(state: RequestAuthState, api_key: i16, allowed: bool) {
    check!(request_auth_admission(state, api_key) == allowed);
}
