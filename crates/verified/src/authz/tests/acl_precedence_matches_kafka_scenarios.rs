use super::*;

/// Kafka `AclAuthorizer` precedence scenarios.
#[test]
fn acl_precedence_matches_kafka_scenarios() {
    use AclDecision::{AllowAcl, AllowNoAcl, AllowSuperuser, DenyDefault, DenyExplicit};

    for (scenario, facts, expected) in [
        ("no ACL and the default config denies", facts(), DenyDefault),
        (
            "a matching ALLOW allows",
            AclFacts {
                saw_allow: true,
                ..facts()
            },
            AllowAcl,
        ),
        (
            "a matching DENY denies",
            AclFacts {
                saw_deny: true,
                ..facts()
            },
            DenyExplicit,
        ),
        (
            "DENY takes precedence over a matching ALLOW",
            AclFacts {
                saw_allow: true,
                saw_deny: true,
                ..facts()
            },
            DenyExplicit,
        ),
        (
            "a super user bypasses a matching DENY",
            AclFacts {
                super_user: true,
                saw_deny: true,
                ..facts()
            },
            AllowSuperuser,
        ),
        (
            "allow.everyone.if.no.acl.found allows a resource with no ACL",
            AclFacts {
                default_decision: AclDefault::Allow,
                ..facts()
            },
            AllowNoAcl,
        ),
        (
            "allow.everyone.if.no.acl.found does not override a DENY",
            AclFacts {
                saw_deny: true,
                default_decision: AclDefault::Allow,
                ..facts()
            },
            DenyExplicit,
        ),
        (
            "an explicit ALLOW is reported as an ACL grant, not the default",
            AclFacts {
                saw_allow: true,
                default_decision: AclDefault::Allow,
                ..facts()
            },
            AllowAcl,
        ),
    ] {
        check!(acl_decision(facts) == expected, "{scenario}");
    }
}

/// Kafka principal and host matching: `User:*` and host `*` are wildcards.
#[test]
fn identity_wildcards_match_kafka_scenarios() {
    // (scenario, stored is the wildcard, stored equals the request, expected)
    for (scenario, wildcard, exact, expected) in [
        ("User:alice ACL, request from User:alice", false, true, true),
        ("User:* ACL, request from User:alice", true, false, true),
        ("User:bob ACL, request from User:alice", false, false, false),
        ("host * ACL, request from any host", true, false, true),
    ] {
        check!(
            acl_identity_match(wildcard, exact) == expected,
            "{scenario}"
        );
    }
}

/// Kafka LITERAL, wildcard, and PREFIXED resource patterns against the
/// requested topic `orders`.
#[test]
fn resource_patterns_match_kafka_scenarios() {
    use {
        AclPatternKind::{Literal, Prefixed},
        AclResourceTypeMatch::{Different, Same},
    };

    let topic = |exact_name, wildcard_name, name_has_prefix| AclResourceFacts {
        resource_type: Same,
        exact_name,
        wildcard_name,
        name_has_prefix,
    };
    for (scenario, pattern, facts, expected) in [
        ("LITERAL orders", Literal, topic(true, false, true), true),
        ("LITERAL *", Literal, topic(false, true, false), true),
        (
            "LITERAL ord is not a prefix",
            Literal,
            topic(false, false, true),
            false,
        ),
        ("PREFIXED ord", Prefixed, topic(false, false, true), true),
        ("PREFIXED orders", Prefixed, topic(true, false, true), true),
        ("PREFIXED pay", Prefixed, topic(false, false, false), false),
        (
            "PREFIXED * is not a wildcard",
            Prefixed,
            topic(false, true, false),
            false,
        ),
        (
            "LITERAL orders on a GROUP",
            Literal,
            AclResourceFacts {
                resource_type: Different,
                ..topic(true, false, true)
            },
            false,
        ),
        (
            "PREFIXED ord on a GROUP",
            Prefixed,
            AclResourceFacts {
                resource_type: Different,
                ..topic(false, false, true)
            },
            false,
        ),
    ] {
        check!(acl_resource_match(pattern, facts) == expected, "{scenario}");
    }
}

/// Kafka's operation-implication table (`AclAuthorizer` /
/// `AclEntry.supportedOperations`) for stored ALLOW and DENY ACLs.
#[test]
fn operation_implications_match_kafka_scenarios() {
    use AclOperationKind::{
        All, Alter, AlterConfigs, ClusterAction, Create, Delete, Describe, DescribeConfigs, Read,
        Write,
    };

    // (stored, requested, stored ACL is ALLOW, expected)
    for (stored, requested, is_allow, expected) in [
        (Read, Read, true, true),
        (Read, Read, false, true),
        (All, ClusterAction, true, true),
        (All, Describe, false, true),
        // ALLOW Read, Write, Delete, and Alter each imply Describe.
        (Read, Describe, true, true),
        (Write, Describe, true, true),
        (Delete, Describe, true, true),
        (Alter, Describe, true, true),
        (AlterConfigs, DescribeConfigs, true, true),
        // A DENY never gains the implied operations.
        (Read, Describe, false, false),
        (AlterConfigs, DescribeConfigs, false, false),
        // The implications are one-way and do not chain further.
        (Describe, Read, true, false),
        (DescribeConfigs, AlterConfigs, true, false),
        (Create, Describe, true, false),
        (Write, Read, true, false),
        (Alter, AlterConfigs, true, false),
    ] {
        check!(acl_operation_match(stored, requested, is_allow) == expected);
    }
}

#[test]
fn request_auth_admission_truth_table() {
    use RequestAuthState::{Authenticated, Exchanging, Failed, PreHandshake};

    for (state, api_key, allowed) in [
        (PreHandshake, 17, true),
        (PreHandshake, 18, true),
        (PreHandshake, 36, false),
        (PreHandshake, -1, false),
        (PreHandshake, 0, false),
        (Exchanging, 36, true),
        (Exchanging, 17, false),
        (Exchanging, 18, false),
        (Exchanging, i16::MAX, false),
        (Failed, 36, false),
        (Failed, 17, false),
        (Failed, 18, false),
        (Authenticated, 0, true),
        (Authenticated, i16::MAX, true),
    ] {
        check!(request_auth_admission(state, api_key) == allowed);
    }
}
