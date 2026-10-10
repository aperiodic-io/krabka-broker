//! Decision-order tests for [`SimpleAclAuthorizer`]: super-user bypass,
//! deny-wins, resource-pattern matching, and the default-deny fallback.
//!
//! The tests that cover which stored entries apply at all live beside those
//! predicates in [`super::matching`].

use krabka_metadata::{AclOperation, MetadataRecord, PatternType};
use krabka_security::Principal;

use super::*;
use crate::{
    authorize_topics,
    simple::test_support::{
        AclSetup, AliceAuthorizer, QuerySetup, acl_image, addr, alice, check_resource_type_access,
        check_topic_access, img, no_super, one_super, req, topic_acl,
    },
};

#[test]
fn empty_image_with_no_super_users_defaults_to_deny() {
    // There is no compat shim that returns Allow in this case —
    // `SimpleAclAuthorizer` is default-deny when nothing matches.
    // Operators who want "allow everything" should configure
    // `AllowAllAuthorizer` explicitly.
    let img = img();
    check_topic_access(&img, "foo", AclOperation::Read, AuthorizationResult::Deny);
}

#[test]
fn super_user_bypass_grants_everything_even_with_acls() {
    let mut img = img();
    // A DENY ACL that would otherwise reject.
    img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(AclSetup {
        permission: PermissionType::Deny,
        ..Default::default()
    })));
    let a = alice();
    let h = addr();
    let auth = SimpleAclAuthorizer::new(one_super("alice"));
    assert2::assert!(
        auth.authorize(&img, &req(&a, &h, QuerySetup::default())) == AuthorizationResult::Allow
    );
}

#[test]
fn deny_by_default_when_super_user_set_but_principal_mismatches() {
    let mut img = img();
    // An ACL exists but doesn't match alice.
    img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(AclSetup {
        principal: "User:bob",
        ..Default::default()
    })));
    let a = alice();
    let h = addr();
    let auth = SimpleAclAuthorizer::new(one_super("admin"));
    assert2::assert!(
        auth.authorize(&img, &req(&a, &h, QuerySetup::default())) == AuthorizationResult::Deny
    );
}

#[test]
fn literal_allow_matches_exact_name() {
    let img = acl_image([topic_acl(AclSetup::default())]);
    let caller = AliceAuthorizer::default();
    for (_name, resource, expected) in [
        ("exact match", "foo", AuthorizationResult::Allow),
        ("literal mismatch", "foobar", AuthorizationResult::Deny),
    ] {
        assert2::assert!(caller.authorize(&img, resource, AclOperation::Read) == expected);
    }
}

#[test]
fn prefixed_allow_matches_prefix() {
    let img = acl_image([topic_acl(AclSetup {
        pattern: PatternType::Prefixed,
        name: "team-",
        ..Default::default()
    })]);
    let caller = AliceAuthorizer::default();
    for (_name, resource, expected) in [
        ("prefix match", "team-foo", AuthorizationResult::Allow),
        ("prefix mismatch", "other", AuthorizationResult::Deny),
    ] {
        assert2::assert!(caller.authorize(&img, resource, AclOperation::Read) == expected);
    }
}

#[test]
fn deny_wins_over_allow() {
    let img = acl_image([
        topic_acl(AclSetup::default()),
        topic_acl(AclSetup {
            permission: PermissionType::Deny,
            ..Default::default()
        }),
    ]);
    check_topic_access(&img, "foo", AclOperation::Read, AuthorizationResult::Deny);
}

#[test]
fn authorize_topics_batch_returns_per_topic_decisions() {
    let img = acl_image([
        topic_acl(AclSetup {
            name: "t1",
            ..Default::default()
        }),
        topic_acl(AclSetup {
            permission: PermissionType::Deny,
            name: "t2",
            ..Default::default()
        }),
    ]);
    let caller = AliceAuthorizer::default();
    let map = authorize_topics(
        &caller.authorizer,
        &img,
        &caller.principal,
        &caller.host,
        AclOperation::Read,
        ["t1", "t2", "t3"],
    );
    let actual = ["t1", "t2", "t3"].map(|topic| map.get(topic).copied());
    // t3: no matching ACL → Deny by default (default-deny; there is
    // no shim that allows in this case).
    assert2::assert!(
        actual
            == [
                Some(AuthorizationResult::Allow),
                Some(AuthorizationResult::Deny),
                Some(AuthorizationResult::Deny),
            ]
    );
}

#[test]
fn multi_super_user_all_bypass() {
    let img = img();
    let h = addr();
    let supers = {
        let mut s = HashSet::new();
        s.insert("admin".to_string());
        s.insert("ops-bot".to_string());
        s
    };
    let admin = Principal {
        name: "admin".into(),
        auth_method: krabka_security::AuthMethod::SaslPlain,
        groups: vec![],
    };
    let ops = Principal {
        name: "ops-bot".into(),
        auth_method: krabka_security::AuthMethod::SaslPlain,
        groups: vec![],
    };
    let alice = alice();
    let auth = SimpleAclAuthorizer::new(supers);
    let actual = [&admin, &ops, &alice].map(|principal| {
        auth.authorize(
            &img,
            &req(
                principal,
                &h,
                QuerySetup {
                    op: AclOperation::Write,
                    ..Default::default()
                },
            ),
        )
    });
    // alice is not a super-user and the image has no matching ACL,
    // so default-deny applies (no compat shim).
    assert2::assert!(
        actual
            == [
                AuthorizationResult::Allow,
                AuthorizationResult::Allow,
                AuthorizationResult::Deny,
            ]
    );
}

#[test]
fn simple_acl_authorizer_is_configured() {
    let auth = SimpleAclAuthorizer::new(no_super());
    assert2::assert!(auth.is_configured());
}

/// `authorize_by_resource_type` answers "does this principal have an
/// ALLOW-not-covered-by-DENY grant for `operation` on any resource of
/// `resource_type`", without naming one resource up front.
mod authorize_by_resource_type {
    use krabka_metadata::{AclOperation, MetadataRecord, PatternType, ResourceType};

    use super::*;

    #[test]
    fn no_acls_at_all_denies() {
        let img = img();
        let caller = AliceAuthorizer::default();
        caller.check_resource_type(
            &img,
            ResourceType::Topic,
            AclOperation::Write,
            AuthorizationResult::Deny,
        );
    }

    #[test]
    fn literal_allow_on_one_resource_allows() {
        let img = acl_image([topic_acl(AclSetup {
            op: AclOperation::Write,
            name: "orders",
            ..Default::default()
        })]);
        check_resource_type_access(
            &img,
            ResourceType::Topic,
            AclOperation::Write,
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn prefixed_allow_allows() {
        let img = acl_image([topic_acl(AclSetup {
            op: AclOperation::Write,
            pattern: PatternType::Prefixed,
            name: "ord",
            ..Default::default()
        })]);
        check_resource_type_access(
            &img,
            ResourceType::Topic,
            AclOperation::Write,
            AuthorizationResult::Allow,
        );
    }

    fn allow_orders_with_deny(denied_resource: &str, expected: AuthorizationResult) {
        let img = acl_image([
            topic_acl(AclSetup {
                op: AclOperation::Write,
                name: "orders",
                ..Default::default()
            }),
            topic_acl(AclSetup {
                permission: PermissionType::Deny,
                op: AclOperation::Write,
                name: denied_resource,
                ..Default::default()
            }),
        ]);
        check_resource_type_access(&img, ResourceType::Topic, AclOperation::Write, expected);
    }

    #[test]
    fn allow_fully_covered_by_a_broader_deny_denies() {
        allow_orders_with_deny("*", AuthorizationResult::Deny);
    }

    #[test]
    fn allow_on_one_resource_with_deny_on_a_different_resource_still_allows() {
        allow_orders_with_deny("payments", AuthorizationResult::Allow);
    }

    /// A literal DENY on the bare prefix string must not shadow a prefixed
    /// ALLOW grant: "orders", "order-events", and every other resource
    /// strictly under the "ord" prefix are still allowed, even though the
    /// prefix string itself, taken as a literal resource name, is denied.
    #[test]
    fn literal_deny_on_the_bare_prefix_string_does_not_shadow_the_prefixed_allow() {
        let img = acl_image([
            topic_acl(AclSetup {
                op: AclOperation::Write,
                pattern: PatternType::Prefixed,
                name: "ord",
                ..Default::default()
            }),
            topic_acl(AclSetup {
                permission: PermissionType::Deny,
                op: AclOperation::Write,
                name: "ord",
                ..Default::default()
            }),
        ]);
        check_resource_type_access(
            &img,
            ResourceType::Topic,
            AclOperation::Write,
            AuthorizationResult::Allow,
        );
    }

    /// Kafka's default `authorizeByResourceType` first authorizes the
    /// hard-coded topic name `hardcode`. Under
    /// `allow.everyone.if.no.acl.found` that probe is allowed whenever no
    /// topic ACL covers the name, so a principal with no topic ACL gets in
    /// even though it has no ALLOW entry for the scan to find. This is what
    /// admits an idempotent producer once a cluster ACL exists.
    #[test]
    fn hardcoded_name_probe_honours_allow_everyone_if_no_acl_found() {
        // (stored entries, allow.everyone.if.no.acl.found, result)
        let other_topic = |pattern, name| {
            topic_acl(AclSetup {
                op: AclOperation::Write,
                principal: "User:bob",
                pattern,
                name,
                ..Default::default()
            })
        };
        let cases = [
            (
                "no acls, flag off",
                vec![],
                false,
                AuthorizationResult::Deny,
            ),
            ("no acls, flag on", vec![], true, AuthorizationResult::Allow),
            (
                "acl on another topic, flag off",
                vec![other_topic(PatternType::Literal, "orders")],
                false,
                AuthorizationResult::Deny,
            ),
            (
                "acl on another topic, flag on",
                vec![other_topic(PatternType::Literal, "orders")],
                true,
                AuthorizationResult::Allow,
            ),
            (
                "literal acl on hardcode, flag on",
                vec![other_topic(PatternType::Literal, "hardcode")],
                true,
                AuthorizationResult::Deny,
            ),
            (
                "prefixed acl covering hardcode, flag on",
                vec![other_topic(PatternType::Prefixed, "hard")],
                true,
                AuthorizationResult::Deny,
            ),
            (
                "wildcard acl, flag on",
                vec![other_topic(PatternType::Literal, "*")],
                true,
                AuthorizationResult::Deny,
            ),
        ];
        for (label, entries, flag, expected) in cases {
            let mut img = img();
            for entry in entries {
                img.apply(&MetadataRecord::V1AccessControlEntry(entry));
            }
            let a = alice();
            let h = addr();
            let auth =
                SimpleAclAuthorizer::new(no_super()).with_allow_everyone_if_no_acl_found(flag);
            assert2::check!(
                auth.authorize_by_resource_type(
                    &img,
                    &a,
                    &h,
                    ResourceType::Topic,
                    AclOperation::Write
                ) == expected,
                "{label}"
            );
        }
    }
}

/// #650: `allow.everyone.if.no.acl.found` (default `false`) allows a
/// request only when NO ACL at all applies to the resource -- by resource
/// type, name, and LITERAL/PREFIXED/wildcard pattern, regardless of
/// principal, host, operation, or permission type. If at least one ACL
/// applies to the resource and none of them matches the request, the
/// request is still denied.
#[test]
fn allow_everyone_if_no_acl_found_applies_only_when_no_acl_touches_the_resource() {
    enum Case {
        /// No ACL at all on the image.
        NoAcls,
        /// An ACL exists for the resource, but for a different principal
        /// (so it "applies to the resource" without matching the request).
        AclForResourceOtherPrincipal,
        /// An ACL exists for a completely different resource.
        AclForOtherResource,
        /// A matching ALLOW ACL exists.
        MatchingAllow,
        /// A matching DENY ACL exists.
        MatchingDeny,
    }

    let cases = [
        (
            "no acls at all + flag off",
            Case::NoAcls,
            false,
            AuthorizationResult::Deny,
        ),
        (
            "no acls at all + flag on",
            Case::NoAcls,
            true,
            AuthorizationResult::Allow,
        ),
        (
            "acl on resource, principal mismatch + flag off",
            Case::AclForResourceOtherPrincipal,
            false,
            AuthorizationResult::Deny,
        ),
        (
            "acl on resource, principal mismatch + flag on",
            Case::AclForResourceOtherPrincipal,
            true,
            AuthorizationResult::Deny,
        ),
        (
            "acl on a different resource + flag off",
            Case::AclForOtherResource,
            false,
            AuthorizationResult::Deny,
        ),
        (
            "acl on a different resource + flag on",
            Case::AclForOtherResource,
            true,
            AuthorizationResult::Allow,
        ),
        (
            "matching allow acl + flag off",
            Case::MatchingAllow,
            false,
            AuthorizationResult::Allow,
        ),
        (
            "matching allow acl + flag on",
            Case::MatchingAllow,
            true,
            AuthorizationResult::Allow,
        ),
        (
            "matching deny acl + flag off",
            Case::MatchingDeny,
            false,
            AuthorizationResult::Deny,
        ),
        (
            "matching deny acl + flag on",
            Case::MatchingDeny,
            true,
            AuthorizationResult::Deny,
        ),
    ];

    for (name, case, flag, expected) in cases {
        let mut img = img();
        match case {
            Case::NoAcls => {}
            Case::AclForResourceOtherPrincipal => {
                img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(AclSetup {
                    principal: "User:bob",
                    ..Default::default()
                })));
            }
            Case::AclForOtherResource => {
                img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(AclSetup {
                    name: "other",
                    ..Default::default()
                })));
            }
            Case::MatchingAllow => {
                img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
                    AclSetup::default(),
                )));
            }
            Case::MatchingDeny => {
                img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(AclSetup {
                    permission: PermissionType::Deny,
                    ..Default::default()
                })));
            }
        }
        let a = alice();
        let h = addr();
        let auth = SimpleAclAuthorizer::new(no_super()).with_allow_everyone_if_no_acl_found(flag);
        assert2::assert!(
            auth.authorize(&img, &req(&a, &h, QuerySetup::default())) == expected,
            "{name}"
        );
    }
}

/// The super-user bypass still wins even with
/// `allow.everyone.if.no.acl.found` set, and it does not need the flag to
/// grant access.
#[test]
fn allow_everyone_if_no_acl_found_does_not_change_super_user_bypass() {
    let img = acl_image([topic_acl(AclSetup {
        permission: PermissionType::Deny,
        ..Default::default()
    })]);
    let a = alice();
    let h = addr();
    let auth =
        SimpleAclAuthorizer::new(one_super("alice")).with_allow_everyone_if_no_acl_found(false);
    assert2::assert!(
        auth.authorize(&img, &req(&a, &h, QuerySetup::default())) == AuthorizationResult::Allow
    );
}
