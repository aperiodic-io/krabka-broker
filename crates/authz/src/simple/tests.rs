//! Decision-order tests for [`SimpleAclAuthorizer`]: super-user bypass,
//! deny-wins, resource-pattern matching, and the default-deny fallback.
//!
//! The tests that cover which stored entries apply at all live beside those
//! predicates in [`super::matching`].

use {
    super::*,
    crate::{
        authorize_topics,
        simple::test_support::{
            AclSetup, AliceAuthorizer, acl_image, addr, alice, check_resource_type_access,
            check_topic_access, img, no_super, one_super, req, topic_acl,
        },
    },
    krabka_metadata::{AclOperation, MetadataRecord, PatternType},
    krabka_security::Principal,
};

fn prefixed_topic_write_allowed_when(denial: AclSetup) {
    let image = acl_image([
        topic_acl()
            .op(AclOperation::Write)
            .pattern(PatternType::Prefixed)
            .name("ord")
            .call(),
        topic_acl()
            .permission(denial.permission)
            .op(denial.op)
            .principal(denial.principal)
            .host(denial.host)
            .rt(denial.rt)
            .pattern(denial.pattern)
            .name(denial.name)
            .call(),
    ]);
    check_resource_type_access(
        &image,
        ResourceType::Topic,
        AclOperation::Write,
        AuthorizationResult::Allow,
    );
}

fn resource_auth_context(
    entries: Vec<krabka_metadata::AclEntry>,
    flag: bool,
) -> (krabka_metadata::MetadataImage, SimpleAclAuthorizer) {
    let mut image = img();
    for entry in entries {
        image.apply(&MetadataRecord::V1AccessControlEntry(entry));
    }
    let authorizer = SimpleAclAuthorizer::new(no_super()).with_allow_everyone_if_no_acl_found(flag);
    (image, authorizer)
}

fn check_resource_auth_context(
    entries: Vec<krabka_metadata::AclEntry>,
    flag: bool,
    expected: AuthorizationResult,
    authorize: impl FnOnce(
        &krabka_metadata::MetadataImage,
        &SimpleAclAuthorizer,
        &Principal,
        &std::net::SocketAddr,
    ) -> AuthorizationResult,
) {
    let (image, authorizer) = resource_auth_context(entries, flag);
    let principal = alice();
    let host = addr();
    assert2::assert!(authorize(&image, &authorizer, &principal, &host) == expected);
}

#[test]
fn empty_image_with_no_super_users_defaults_to_deny() {
    // There is no compat shim that returns Allow in this case —
    // `SimpleAclAuthorizer` is default-deny when nothing matches.
    // Operators who want "allow everything" should configure
    // `AllowAllAuthorizer` explicitly.
    let img = img();
    check_topic_access(&img, "foo", AclOperation::Read, AuthorizationResult::Deny);
}

#[yare::parameterized(
    default_flag = { None::<bool> },
    allow_everyone_disabled = { Some(false) },
)]
fn super_user_bypass_grants_everything_even_with_acls(allow_everyone: Option<bool>) {
    let mut img = img();
    // A DENY ACL that would otherwise reject.
    img.apply(&MetadataRecord::V1AccessControlEntry(
        topic_acl().permission(PermissionType::Deny).call(),
    ));
    let a = alice();
    let h = addr();
    let auth = SimpleAclAuthorizer::new(one_super("alice"));
    let auth = allow_everyone.map_or(auth, |flag| {
        SimpleAclAuthorizer::new(one_super("alice")).with_allow_everyone_if_no_acl_found(flag)
    });
    assert2::assert!(
        auth.authorize(&img, &req().p(&a).host(&h).call()) == AuthorizationResult::Allow
    );
}

#[test]
fn deny_by_default_when_super_user_set_but_principal_mismatches() {
    let mut img = img();
    // An ACL exists but doesn't match alice.
    img.apply(&MetadataRecord::V1AccessControlEntry(
        topic_acl().principal("User:bob").call(),
    ));
    let a = alice();
    let h = addr();
    let auth = SimpleAclAuthorizer::new(one_super("admin"));
    assert2::assert!(
        auth.authorize(&img, &req().p(&a).host(&h).call()) == AuthorizationResult::Deny
    );
}

#[yare::parameterized(
    literal_match = { PatternType::Literal, "foo", "foo", AuthorizationResult::Allow },
    literal_mismatch = { PatternType::Literal, "foo", "foobar", AuthorizationResult::Deny },
    prefix_match = { PatternType::Prefixed, "team-", "team-foo", AuthorizationResult::Allow },
    prefix_mismatch = { PatternType::Prefixed, "team-", "other", AuthorizationResult::Deny },
)]
fn allow_pattern_matches_only_matching_name(
    pattern: PatternType,
    acl_name: &str,
    resource: &str,
    expected: AuthorizationResult,
) {
    let img = acl_image([topic_acl().pattern(pattern).name(acl_name).call()]);
    let caller = AliceAuthorizer::default();
    assert2::assert!(caller.authorize(&img, resource, AclOperation::Read) == expected);
}

#[test]
fn deny_wins_over_allow() {
    let img = acl_image([
        topic_acl().call(),
        topic_acl().permission(PermissionType::Deny).call(),
    ]);
    check_topic_access(&img, "foo", AclOperation::Read, AuthorizationResult::Deny);
}

#[test]
fn authorize_topics_batch_returns_per_topic_decisions() {
    let img = acl_image([
        topic_acl().name("t1").call(),
        topic_acl()
            .permission(PermissionType::Deny)
            .name("t2")
            .call(),
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
            &req().p(principal).host(&h).op(AclOperation::Write).call(),
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
    use {
        super::*,
        krabka_metadata::{AclOperation, PatternType, ResourceType},
    };

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
        let img = acl_image([topic_acl().op(AclOperation::Write).name("orders").call()]);
        check_resource_type_access(
            &img,
            ResourceType::Topic,
            AclOperation::Write,
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn prefixed_allow_allows() {
        let img = acl_image([topic_acl()
            .op(AclOperation::Write)
            .pattern(PatternType::Prefixed)
            .name("ord")
            .call()]);
        check_resource_type_access(
            &img,
            ResourceType::Topic,
            AclOperation::Write,
            AuthorizationResult::Allow,
        );
    }

    fn allow_orders_with_deny(denied_resource: &str, expected: AuthorizationResult) {
        let img = acl_image([
            topic_acl().op(AclOperation::Write).name("orders").call(),
            topic_acl()
                .permission(PermissionType::Deny)
                .op(AclOperation::Write)
                .name(denied_resource)
                .call(),
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
        prefixed_topic_write_allowed_when(AclSetup {
            permission: PermissionType::Deny,
            op: AclOperation::Write,
            name: "ord",
            ..Default::default()
        });
    }

    #[yare::parameterized(
        literal_probe_collision = {
            vec![(PatternType::Literal, "ord"), (PatternType::Literal, "ord\u{10ffff}")],
            "orders",
            AuthorizationResult::Allow,
        },
        narrower_prefix_probe_collision = {
            vec![
                (PatternType::Literal, "ord"),
                (PatternType::Prefixed, "ord\u{10ffff}"),
                (PatternType::Prefixed, "order"),
                (PatternType::Prefixed, "ord-"),
            ],
            "ordinary",
            AuthorizationResult::Allow,
        },
        same_prefix_denies = {
            vec![(PatternType::Prefixed, "ord")],
            "orders",
            AuthorizationResult::Deny,
        },
        broader_prefix_denies = {
            vec![(PatternType::Prefixed, "or")],
            "orders",
            AuthorizationResult::Deny,
        },
        wildcard_denies = {
            vec![(PatternType::Literal, "*")],
            "orders",
            AuthorizationResult::Deny,
        },
    )]
    fn prefix_grants_survive_finite_literal_and_narrower_prefix_exclusions(
        exclusions: Vec<(PatternType, &'static str)>,
        witness: &'static str,
        expected: AuthorizationResult,
    ) {
        let grant = topic_acl()
            .op(AclOperation::Write)
            .pattern(PatternType::Prefixed)
            .name("ord")
            .call();
        let denies = exclusions.into_iter().map(|(pattern, name)| {
            topic_acl()
                .permission(PermissionType::Deny)
                .op(AclOperation::Write)
                .pattern(pattern)
                .name(name)
                .call()
        });
        let image = acl_image(std::iter::once(grant).chain(denies));
        let caller = AliceAuthorizer::default();
        assert2::check!(caller.authorize(&image, witness, AclOperation::Write) == expected);
        assert2::check!(
            caller.authorize_by_resource_type(&image, ResourceType::Topic, AclOperation::Write)
                == expected
        );
    }

    #[test]
    fn many_finite_exclusions_cannot_hide_a_remaining_prefix_grant() {
        for count in [1, 32, 256] {
            let mut entries = vec![
                topic_acl()
                    .op(AclOperation::Write)
                    .pattern(PatternType::Prefixed)
                    .name("ord")
                    .call(),
            ];
            for index in 0..count {
                for pattern in [PatternType::Literal, PatternType::Prefixed] {
                    entries.push(
                        topic_acl()
                            .permission(PermissionType::Deny)
                            .op(AclOperation::Write)
                            .pattern(pattern)
                            .name(&format!("ord-{index}"))
                            .call(),
                    );
                }
            }
            entries.push(
                topic_acl()
                    .permission(PermissionType::Deny)
                    .op(AclOperation::Write)
                    .name("ord")
                    .call(),
            );
            let image = acl_image(entries);
            let caller = AliceAuthorizer::default();
            caller.check(
                &image,
                "orders",
                AclOperation::Write,
                AuthorizationResult::Allow,
            );
            caller.check_resource_type(
                &image,
                ResourceType::Topic,
                AclOperation::Write,
                AuthorizationResult::Allow,
            );
        }
    }

    #[yare::parameterized(
        literal_denies_same_name = {
            "orders",
            PatternType::Literal,
            "orders",
            AuthorizationResult::Deny,
        },
        prefix_denies_within_range = {
            "orders",
            PatternType::Prefixed,
            "ord",
            AuthorizationResult::Deny,
        },
        prefix_denies_outside_range = {
            "orders",
            PatternType::Prefixed,
            "orders-",
            AuthorizationResult::Allow,
        },
        wildcard_grant_literal_deny = {
            "*",
            PatternType::Literal,
            "hardcode",
            AuthorizationResult::Allow,
        },
        wildcard_grant_prefix_deny = {
            "*",
            PatternType::Prefixed,
            "hard",
            AuthorizationResult::Allow,
        },
        wildcard_denies_wildcard = { "*", PatternType::Literal, "*", AuthorizationResult::Deny },
    )]
    fn literal_and_wildcard_grants_follow_kafka_pattern_dominance(
        grant: &'static str,
        denied_pattern: PatternType,
        denied_name: &'static str,
        expected: AuthorizationResult,
    ) {
        let image = acl_image([
            topic_acl().op(AclOperation::Write).name(grant).call(),
            topic_acl()
                .permission(PermissionType::Deny)
                .op(AclOperation::Write)
                .pattern(denied_pattern)
                .name(denied_name)
                .call(),
        ]);
        check_resource_type_access(&image, ResourceType::Topic, AclOperation::Write, expected);
    }

    #[yare::parameterized(
        different_principal = {
            AclSetup { principal: "User:bob", op: AclOperation::Write, ..Default::default() },
        },
        different_host = {
            AclSetup { host: "192.0.2.1", op: AclOperation::Write, ..Default::default() },
        },
        different_operation = { AclSetup { op: AclOperation::Read, ..Default::default() } },
        different_resource_type = {
            AclSetup { rt: ResourceType::Group, op: AclOperation::Write, ..Default::default() },
        },
    )]
    fn resource_type_denials_match_the_caller_host_operation_and_type(excluded: AclSetup) {
        prefixed_topic_write_allowed_when(AclSetup {
            permission: PermissionType::Deny,
            name: "*",
            ..excluded
        });
    }

    /// Kafka's default `authorizeByResourceType` first authorizes the
    /// hard-coded topic name `hardcode`. Under
    /// `allow.everyone.if.no.acl.found` that probe is allowed whenever no
    /// topic ACL covers the name, so a principal with no topic ACL gets in
    /// even though it has no ALLOW entry for the scan to find. This is what
    /// admits an idempotent producer once a cluster ACL exists.
    #[yare::parameterized(
        no_acl_flag_off = { vec![], false, AuthorizationResult::Deny },
        no_acl_flag_on = { vec![], true, AuthorizationResult::Allow },
        other_topic_flag_off = {
            vec![
                topic_acl()
                    .op(AclOperation::Write)
                    .principal("User:bob")
                    .pattern(PatternType::Literal)
                    .name("orders")
                    .call(),
            ],
            false,
            AuthorizationResult::Deny,
        },
        other_topic_flag_on = {
            vec![
                topic_acl()
                    .op(AclOperation::Write)
                    .principal("User:bob")
                    .pattern(PatternType::Literal)
                    .name("orders")
                    .call(),
            ],
            true,
            AuthorizationResult::Allow,
        },
        literal_hardcode = {
            vec![
                topic_acl()
                    .op(AclOperation::Write)
                    .principal("User:bob")
                    .pattern(PatternType::Literal)
                    .name("hardcode")
                    .call(),
            ],
            true,
            AuthorizationResult::Deny,
        },
        prefix_covers_hardcode = {
            vec![
                topic_acl()
                    .op(AclOperation::Write)
                    .principal("User:bob")
                    .pattern(PatternType::Prefixed)
                    .name("hard")
                    .call(),
            ],
            true,
            AuthorizationResult::Deny,
        },
        wildcard = {
            vec![
                topic_acl()
                    .op(AclOperation::Write)
                    .principal("User:bob")
                    .pattern(PatternType::Literal)
                    .name("*")
                    .call(),
            ],
            true,
            AuthorizationResult::Deny,
        },
    )]
    fn hardcoded_name_probe_honours_allow_everyone_if_no_acl_found(
        entries: Vec<krabka_metadata::AclEntry>,
        flag: bool,
        expected: AuthorizationResult,
    ) {
        check_resource_auth_context(
            entries,
            flag,
            expected,
            |image, authorizer, principal, host| {
                authorizer.authorize_by_resource_type(
                    image,
                    principal,
                    host,
                    ResourceType::Topic,
                    AclOperation::Write,
                )
            },
        );
    }
}

/// #650: `allow.everyone.if.no.acl.found` (default `false`) allows a
/// request only when NO ACL at all applies to the resource -- by resource
/// type, name, and LITERAL/PREFIXED/wildcard pattern, regardless of
/// principal, host, operation, or permission type. If at least one ACL
/// applies to the resource and none of them matches the request, the
/// request is still denied.
#[yare::parameterized(
    no_acl_flag_off = { vec![], false, AuthorizationResult::Deny },
    no_acl_flag_on = { vec![], true, AuthorizationResult::Allow },
    resource_acl_other_principal_flag_off = {
        vec![topic_acl().principal("User:bob").call()],
        false,
        AuthorizationResult::Deny,
    },
    resource_acl_other_principal_flag_on = {
        vec![topic_acl().principal("User:bob").call()],
        true,
        AuthorizationResult::Deny,
    },
    other_resource_flag_off = {
        vec![topic_acl().name("other").call()],
        false,
        AuthorizationResult::Deny,
    },
    other_resource_flag_on = {
        vec![topic_acl().name("other").call()],
        true,
        AuthorizationResult::Allow,
    },
    matching_allow_flag_off = { vec![topic_acl().call()], false, AuthorizationResult::Allow },
    matching_allow_flag_on = { vec![topic_acl().call()], true, AuthorizationResult::Allow },
    matching_deny_flag_off = {
        vec![topic_acl().permission(PermissionType::Deny).call()],
        false,
        AuthorizationResult::Deny,
    },
    matching_deny_flag_on = {
        vec![topic_acl().permission(PermissionType::Deny).call()],
        true,
        AuthorizationResult::Deny,
    },
)]
fn allow_everyone_if_no_acl_found_applies_only_when_no_acl_touches_the_resource(
    entries: Vec<krabka_metadata::AclEntry>,
    flag: bool,
    expected: AuthorizationResult,
) {
    check_resource_auth_context(
        entries,
        flag,
        expected,
        |image, authorizer, principal, host| {
            authorizer.authorize(image, &req().p(principal).host(host).call())
        },
    );
}
