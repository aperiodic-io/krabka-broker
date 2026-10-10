use assert2::check;

use super::*;

const TOPIC: i8 = 2;
const LITERAL: i8 = 3;
const PREFIXED: i8 = 4;
const READ: i8 = 3;
const ALLOW: i8 = 3;

fn wire(resource_name: Option<&str>, pattern_type: i8) -> WireAclBindingFilter<'_> {
    WireAclBindingFilter {
        resource_type: TOPIC,
        resource_name,
        pattern_type,
        principal: None,
        host: None,
        operation: WIRE_ANY,
        permission_type: WIRE_ANY,
    }
}

fn topic_acl(name: &str, pattern_type: PatternType) -> AclEntry {
    AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: name.into(),
        pattern_type,
        principal: "User:alice".into(),
        host: "*".into(),
        operation: AclOperation::Read,
        permission_type: PermissionType::Allow,
    }
}

/// Edits one axis of a filter in a test table row.
type FilterEdit = fn(&mut AclBindingFilter);

fn any_filter() -> AclBindingFilter {
    AclBindingFilter {
        resource_type: AxisFilter::Any,
        resource_name: None,
        pattern_type: PatternTypeFilter::Any,
        principal: None,
        host: None,
        operation: AxisFilter::Any,
        permission_type: AxisFilter::Any,
    }
}

#[test]
fn from_wire_decodes_every_axis() {
    let cases = [
        (
            "all concrete",
            WireAclBindingFilter {
                resource_type: TOPIC,
                resource_name: Some("orders"),
                pattern_type: LITERAL,
                principal: Some("User:alice"),
                host: Some("*"),
                operation: READ,
                permission_type: ALLOW,
            },
            Ok(AclBindingFilter {
                resource_type: AxisFilter::Exact(ResourceType::Topic),
                resource_name: Some("orders".into()),
                pattern_type: PatternTypeFilter::Exact(PatternType::Literal),
                principal: Some("User:alice".into()),
                host: Some("*".into()),
                operation: AxisFilter::Exact(AclOperation::Read),
                permission_type: AxisFilter::Exact(PermissionType::Allow),
            }),
        ),
        (
            "ANY and MATCH keep empty strings, which are not null",
            WireAclBindingFilter {
                resource_type: WIRE_ANY,
                resource_name: Some(""),
                pattern_type: WIRE_PATTERN_MATCH,
                principal: Some(""),
                host: Some(""),
                operation: WIRE_ANY,
                permission_type: WIRE_ANY,
            },
            Ok(AclBindingFilter {
                resource_type: AxisFilter::Any,
                resource_name: Some(String::new()),
                pattern_type: PatternTypeFilter::Match,
                principal: Some(String::new()),
                host: Some(String::new()),
                operation: AxisFilter::Any,
                permission_type: AxisFilter::Any,
            }),
        ),
        (
            "KIP-373 values decode to the stored USER resource and token operations",
            WireAclBindingFilter {
                resource_type: 7,
                resource_name: None,
                pattern_type: PREFIXED,
                principal: None,
                host: None,
                operation: 13,
                permission_type: WIRE_ANY,
            },
            Ok(AclBindingFilter {
                resource_type: AxisFilter::Exact(ResourceType::User),
                resource_name: None,
                pattern_type: PatternTypeFilter::Exact(PatternType::Prefixed),
                principal: None,
                host: None,
                operation: AxisFilter::Exact(AclOperation::CreateTokens),
                permission_type: AxisFilter::Any,
            }),
        ),
        (
            "bytes Kafka does not define become UNKNOWN",
            WireAclBindingFilter {
                resource_type: 8,
                resource_name: None,
                pattern_type: 5,
                principal: None,
                host: None,
                operation: 16,
                permission_type: 4,
            },
            Ok(AclBindingFilter {
                resource_type: AxisFilter::Unknown,
                resource_name: None,
                pattern_type: PatternTypeFilter::Unknown,
                principal: None,
                host: None,
                operation: AxisFilter::Unknown,
                permission_type: AxisFilter::Unknown,
            }),
        ),
    ];
    for (name, wire, want) in cases {
        check!(AclBindingFilter::from_wire(wire) == want, "{name}");
    }
}

#[test]
fn from_wire_refuses_unknown_on_any_axis() {
    type Corrupt = fn(&mut WireAclBindingFilter<'_>);
    let cases: [(&str, Corrupt); 4] = [
        ("resource_type", |w| w.resource_type = WIRE_UNKNOWN),
        ("pattern_type", |w| w.pattern_type = WIRE_UNKNOWN),
        ("operation", |w| w.operation = WIRE_UNKNOWN),
        ("permission_type", |w| w.permission_type = WIRE_UNKNOWN),
    ];
    for (axis, corrupt) in cases {
        let mut w = wire(Some("orders"), LITERAL);
        corrupt(&mut w);
        check!(
            AclBindingFilter::from_wire(w) == Err(UnknownElement),
            "axis {axis}"
        );
    }
}

/// The table from issue #770: Kafka's `ResourcePatternFilter.matches` over
/// six seeded topic patterns.
#[test]
fn resource_pattern_matching_follows_kafka() {
    let seeded = crate::handlers::acl_wire::PATTERN_MATCHING_FIXTURE;
    let all: &[&str] = &["foo", "*", "f", "fo", "bar", "food"];
    let cases: [(i8, Option<&str>, &[&str]); 13] = [
        (WIRE_ANY, None, all),
        (WIRE_ANY, Some(""), &[]),
        (WIRE_ANY, Some("foo"), &["foo"]),
        (WIRE_ANY, Some("*"), &["*"]),
        (WIRE_PATTERN_MATCH, None, all),
        // An empty name still applies to the literal wildcard.
        (WIRE_PATTERN_MATCH, Some(""), &["*"]),
        (WIRE_PATTERN_MATCH, Some("foo"), &["foo", "*", "f", "fo"]),
        (WIRE_PATTERN_MATCH, Some("food"), &["*", "f", "fo", "food"]),
        (LITERAL, None, &["foo", "*", "food"]),
        (LITERAL, Some(""), &[]),
        (LITERAL, Some("foo"), &["foo"]),
        (PREFIXED, None, &["f", "fo", "bar"]),
        (PREFIXED, Some("foo"), &[]),
    ];
    for (pattern_type, name, want) in cases {
        let filter = AclBindingFilter::from_wire(wire(name, pattern_type)).expect("filter");
        let got: Vec<&str> = seeded
            .iter()
            .filter(|(n, pt)| filter.matches(&topic_acl(n, *pt)))
            .map(|(n, _)| *n)
            .collect();
        check!(got == want, "pattern {pattern_type} name {name:?}");
    }
}

#[test]
fn entry_matching_takes_only_null_as_any() {
    let entry = topic_acl("orders", PatternType::Literal);
    let cases: [(&str, FilterEdit, bool); 9] = [
        ("everything null", |_| {}, true),
        (
            "empty principal",
            |f| f.principal = Some(String::new()),
            false,
        ),
        ("empty host", |f| f.host = Some(String::new()), false),
        (
            "principal",
            |f| f.principal = Some("User:alice".into()),
            true,
        ),
        (
            "other principal",
            |f| f.principal = Some("User:bob".into()),
            false,
        ),
        ("host", |f| f.host = Some("*".into()), true),
        (
            "operation",
            |f| f.operation = AxisFilter::Exact(AclOperation::Write),
            false,
        ),
        (
            "permission",
            |f| f.permission_type = AxisFilter::Exact(PermissionType::Deny),
            false,
        ),
        (
            "resource type",
            |f| f.resource_type = AxisFilter::Exact(ResourceType::Group),
            false,
        ),
    ];
    for (name, edit, want) in cases {
        let mut filter = any_filter();
        edit(&mut filter);
        check!(filter.matches(&entry) == want, "{name}");
    }
}

/// KIP-1276: `AccessControlEntryFilter.matches` compares a stored CIDR host
/// as text, so only the same literal finds it. An address inside the range,
/// or the same range written another way, does not.
#[test]
fn host_filter_compares_a_cidr_host_as_text() {
    let entry = AclEntry {
        host: "10.0.0.0/8".into(),
        ..topic_acl("orders", PatternType::Literal)
    };
    let cases = [
        ("10.0.0.0/8", true),
        ("10.1.2.3", false),
        ("10.0.0.0/08", false),
        ("*", false),
    ];
    for (host, want) in cases {
        let mut filter = any_filter();
        filter.host = Some(host.into());
        check!(filter.matches(&entry) == want, "host filter {host:?}");
    }
}

/// KIP-373: an exact `USER` / `CREATE_TOKENS` filter finds the binding that
/// `CreateAcls` stored, so `DescribeAcls` lists it and `DeleteAcls` revokes it.
#[test]
fn exact_user_token_filter_matches_the_stored_binding() {
    let entry = crate::test_support::allow_acl(crate::test_support::AllowAclSetup {
        resource_type: ResourceType::User,
        resource_name: "User:bob",
        operation: AclOperation::CreateTokens,
        ..Default::default()
    });
    let filter = AclBindingFilter::from_wire(WireAclBindingFilter {
        resource_type: 7,
        resource_name: Some("User:bob"),
        pattern_type: LITERAL,
        principal: None,
        host: None,
        operation: 13,
        permission_type: WIRE_ANY,
    })
    .unwrap();
    check!(filter.matches(&entry));
}

#[test]
fn unknown_axes_match_nothing() {
    let entry = topic_acl("orders", PatternType::Literal);
    let cases: [(&str, FilterEdit); 3] = [
        ("unknown resource type", |f| {
            f.resource_type = AxisFilter::Unknown;
        }),
        ("unknown pattern type", |f| {
            f.pattern_type = PatternTypeFilter::Unknown;
        }),
        ("unknown permission", |f| {
            f.permission_type = AxisFilter::Unknown;
        }),
    ];
    for (name, edit) in cases {
        let mut filter = any_filter();
        edit(&mut filter);
        check!(!filter.matches(&entry), "{name}");
    }
}

#[test]
fn unknown_message_names_the_failing_half_of_the_filter() {
    let cases: [(&str, FilterEdit, Option<&str>); 6] = [
        ("valid", |_| {}, None),
        (
            "resource type",
            |f| f.resource_type = AxisFilter::Unknown,
            Some(UNKNOWN_PATTERN_FILTER_MESSAGE),
        ),
        (
            "pattern type",
            |f| f.pattern_type = PatternTypeFilter::Unknown,
            Some(UNKNOWN_PATTERN_FILTER_MESSAGE),
        ),
        (
            "operation",
            |f| f.operation = AxisFilter::Unknown,
            Some(UNKNOWN_ENTRY_FILTER_MESSAGE),
        ),
        (
            "permission",
            |f| f.permission_type = AxisFilter::Unknown,
            Some(UNKNOWN_ENTRY_FILTER_MESSAGE),
        ),
        (
            "pattern half wins",
            |f| {
                f.pattern_type = PatternTypeFilter::Unknown;
                f.operation = AxisFilter::Unknown;
            },
            Some(UNKNOWN_PATTERN_FILTER_MESSAGE),
        ),
    ];
    for (name, edit, want) in cases {
        let mut filter = any_filter();
        edit(&mut filter);
        check!(filter.unknown_message() == want, "{name}");
    }
}
