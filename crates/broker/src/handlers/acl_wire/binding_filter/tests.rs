use {
    super::*,
    crate::{handlers::acl_wire::PATTERN_MATCHING_FIXTURE, test_support::allow_acl},
    assert2::check,
};

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

/// A wire-filter corruption that must make [`AclBindingFilter::from_wire`] refuse.
struct CorruptCase {
    axis: &'static str,
    corrupt: fn(&mut WireAclBindingFilter<'_>),
}

/// A filter edit with the name that labels a failed case.
struct EditCase {
    name: &'static str,
    edit: FilterEdit,
}

/// A filter edit and whether the edited filter still matches the entry.
struct EntryMatchCase {
    name: &'static str,
    edit: FilterEdit,
    want: bool,
}

/// A filter edit and the refusal message it must produce, if any.
struct UnknownMessageCase {
    name: &'static str,
    edit: FilterEdit,
    want: Option<&'static str>,
}

/// A wire pattern type and resource name, with the seeded names the filter must match.
struct PatternMatchCase {
    wire_pattern_type: i8,
    name: Option<&'static str>,
    want: &'static [&'static str],
}

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
    let cases: [CorruptCase; 4] = [
        CorruptCase {
            axis: "resource_type",
            corrupt: |w| w.resource_type = WIRE_UNKNOWN,
        },
        CorruptCase {
            axis: "pattern_type",
            corrupt: |w| w.pattern_type = WIRE_UNKNOWN,
        },
        CorruptCase {
            axis: "operation",
            corrupt: |w| w.operation = WIRE_UNKNOWN,
        },
        CorruptCase {
            axis: "permission_type",
            corrupt: |w| w.permission_type = WIRE_UNKNOWN,
        },
    ];
    for CorruptCase { axis, corrupt } in cases {
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
    let seeded = PATTERN_MATCHING_FIXTURE;
    let all: &[&str] = &["foo", "*", "f", "fo", "bar", "food"];
    let cases: [PatternMatchCase; 13] = [
        PatternMatchCase {
            wire_pattern_type: WIRE_ANY,
            name: None,
            want: all,
        },
        PatternMatchCase {
            wire_pattern_type: WIRE_ANY,
            name: Some(""),
            want: &[],
        },
        PatternMatchCase {
            wire_pattern_type: WIRE_ANY,
            name: Some("foo"),
            want: &["foo"],
        },
        PatternMatchCase {
            wire_pattern_type: WIRE_ANY,
            name: Some("*"),
            want: &["*"],
        },
        PatternMatchCase {
            wire_pattern_type: WIRE_PATTERN_MATCH,
            name: None,
            want: all,
        },
        // An empty name still applies to the literal wildcard.
        PatternMatchCase {
            wire_pattern_type: WIRE_PATTERN_MATCH,
            name: Some(""),
            want: &["*"],
        },
        PatternMatchCase {
            wire_pattern_type: WIRE_PATTERN_MATCH,
            name: Some("foo"),
            want: &["foo", "*", "f", "fo"],
        },
        PatternMatchCase {
            wire_pattern_type: WIRE_PATTERN_MATCH,
            name: Some("food"),
            want: &["*", "f", "fo", "food"],
        },
        PatternMatchCase {
            wire_pattern_type: LITERAL,
            name: None,
            want: &["foo", "*", "food"],
        },
        PatternMatchCase {
            wire_pattern_type: LITERAL,
            name: Some(""),
            want: &[],
        },
        PatternMatchCase {
            wire_pattern_type: LITERAL,
            name: Some("foo"),
            want: &["foo"],
        },
        PatternMatchCase {
            wire_pattern_type: PREFIXED,
            name: None,
            want: &["f", "fo", "bar"],
        },
        PatternMatchCase {
            wire_pattern_type: PREFIXED,
            name: Some("foo"),
            want: &[],
        },
    ];
    for PatternMatchCase {
        wire_pattern_type: pattern_type,
        name,
        want,
    } in cases
    {
        let filter = AclBindingFilter::from_wire(wire(name, pattern_type)).expect("filter");
        let got: Vec<&str> = seeded
            .iter()
            .filter(|seed| filter.matches(&topic_acl(seed.name, seed.pattern_type)))
            .map(|seed| seed.name)
            .collect();
        check!(got == want, "pattern {pattern_type} name {name:?}");
    }
}

#[test]
fn entry_matching_takes_only_null_as_any() {
    let entry = topic_acl("orders", PatternType::Literal);
    let cases: [EntryMatchCase; 9] = [
        EntryMatchCase {
            name: "everything null",
            edit: |_| {},
            want: true,
        },
        EntryMatchCase {
            name: "empty principal",
            edit: |f| f.principal = Some(String::new()),
            want: false,
        },
        EntryMatchCase {
            name: "empty host",
            edit: |f| f.host = Some(String::new()),
            want: false,
        },
        EntryMatchCase {
            name: "principal",
            edit: |f| f.principal = Some("User:alice".into()),
            want: true,
        },
        EntryMatchCase {
            name: "other principal",
            edit: |f| f.principal = Some("User:bob".into()),
            want: false,
        },
        EntryMatchCase {
            name: "host",
            edit: |f| f.host = Some("*".into()),
            want: true,
        },
        EntryMatchCase {
            name: "operation",
            edit: |f| f.operation = AxisFilter::Exact(AclOperation::Write),
            want: false,
        },
        EntryMatchCase {
            name: "permission",
            edit: |f| f.permission_type = AxisFilter::Exact(PermissionType::Deny),
            want: false,
        },
        EntryMatchCase {
            name: "resource type",
            edit: |f| f.resource_type = AxisFilter::Exact(ResourceType::Group),
            want: false,
        },
    ];
    for EntryMatchCase { name, edit, want } in cases {
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
    let entry = allow_acl()
        .resource_type(ResourceType::User)
        .resource_name("User:bob")
        .operation(AclOperation::CreateTokens)
        .call();
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
    let cases: [EditCase; 3] = [
        EditCase {
            name: "unknown resource type",
            edit: |f| {
                f.resource_type = AxisFilter::Unknown;
            },
        },
        EditCase {
            name: "unknown pattern type",
            edit: |f| {
                f.pattern_type = PatternTypeFilter::Unknown;
            },
        },
        EditCase {
            name: "unknown permission",
            edit: |f| {
                f.permission_type = AxisFilter::Unknown;
            },
        },
    ];
    for EditCase { name, edit } in cases {
        let mut filter = any_filter();
        edit(&mut filter);
        check!(!filter.matches(&entry), "{name}");
    }
}

#[test]
fn unknown_message_names_the_failing_half_of_the_filter() {
    let cases: [UnknownMessageCase; 6] = [
        UnknownMessageCase {
            name: "valid",
            edit: |_| {},
            want: None,
        },
        UnknownMessageCase {
            name: "resource type",
            edit: |f| f.resource_type = AxisFilter::Unknown,
            want: Some(UNKNOWN_PATTERN_FILTER_MESSAGE),
        },
        UnknownMessageCase {
            name: "pattern type",
            edit: |f| f.pattern_type = PatternTypeFilter::Unknown,
            want: Some(UNKNOWN_PATTERN_FILTER_MESSAGE),
        },
        UnknownMessageCase {
            name: "operation",
            edit: |f| f.operation = AxisFilter::Unknown,
            want: Some(UNKNOWN_ENTRY_FILTER_MESSAGE),
        },
        UnknownMessageCase {
            name: "permission",
            edit: |f| f.permission_type = AxisFilter::Unknown,
            want: Some(UNKNOWN_ENTRY_FILTER_MESSAGE),
        },
        UnknownMessageCase {
            name: "pattern half wins",
            edit: |f| {
                f.pattern_type = PatternTypeFilter::Unknown;
                f.operation = AxisFilter::Unknown;
            },
            want: Some(UNKNOWN_PATTERN_FILTER_MESSAGE),
        },
    ];
    for UnknownMessageCase { name, edit, want } in cases {
        let mut filter = any_filter();
        edit(&mut filter);
        check!(filter.unknown_message() == want, "{name}");
    }
}
