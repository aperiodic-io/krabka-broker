//! What `#[derive(EnumStr)]` names, parses, lists and encodes.

use assert2::assert;
use krabka_macros::EnumStr;
use prometheus_client::{
    encoding::{EncodeLabelSet, text::encode},
    metrics::{counter::Counter, family::Family},
    registry::Registry,
};

/// Snake-cased names, one override, one alias, a renamed parser, `ALL` and a
/// label value.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, EnumStr)]
#[enum_str(case = "snake_case", parse = from_name, all, label_value)]
enum Phase {
    Attempted,
    ApiActivity,
    #[enum_str(name = "compact,delete", alias = "compact-delete")]
    CompactAndDelete,
    #[enum_str(name = "", alias("none", "off"))]
    NoCleanup,
}

/// Unchanged variant names and a renamed getter over variants with fields,
/// so the getter takes `&self`.
#[derive(Debug, EnumStr)]
#[enum_str(as_str = name)]
enum Role {
    Leader {
        #[expect(dead_code, reason = "only the variant's name is read")]
        epoch: i32,
    },
    Voted(#[expect(dead_code, reason = "only the variant's name is read")] u32),
    Resigned,
}

#[derive(EnumStr)]
#[enum_str(label_value)]
enum GenericEvent<T>
where
    T: Clone,
{
    Value(T),
    Missing,
}

#[test]
fn generic_payload_preserves_the_type_arguments_and_where_clause() {
    let value = GenericEvent::Value(String::from("payload"));
    assert!(value.as_str() == "Value");
    assert!(GenericEvent::<String>::Missing.as_str() == "Missing");
    let GenericEvent::Value(payload) = value else {
        panic!("value variant")
    };
    assert!(payload == "payload");
}

/// The remaining cases, and a bare `parse`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, EnumStr)]
#[enum_str(case = "UPPERCASE", parse)]
enum Upper {
    Warn,
    FatalError,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, EnumStr)]
#[enum_str(case = "kebab-case", parse)]
enum Kebab {
    HighlyAvailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, EnumStr)]
#[enum_str(case = "lowercase", parse)]
enum Lower {
    HighlyAvailable,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PhaseLabel {
    phase: Phase,
}

#[test]
fn as_str_cases_the_variant_name_unless_the_variant_names_itself() {
    assert!(Phase::ALL.map(Phase::as_str) == ["attempted", "api_activity", "compact,delete", ""]);
    assert!([Upper::Warn, Upper::FatalError].map(Upper::as_str) == ["WARN", "FATALERROR"]);
    assert!(Kebab::HighlyAvailable.as_str() == "highly-available");
    assert!(Lower::HighlyAvailable.as_str() == "highlyavailable");
}

#[test]
fn as_str_takes_self_by_reference_when_a_variant_has_fields() {
    let roles = [Role::Leader { epoch: 3 }, Role::Voted(1), Role::Resigned];
    assert!(roles.iter().map(Role::name).collect::<Vec<_>>() == ["Leader", "Voted", "Resigned"]);
}

#[test]
fn parse_accepts_each_name_and_alias_and_nothing_else() {
    let cases = [
        ("attempted", Some(Phase::Attempted)),
        ("api_activity", Some(Phase::ApiActivity)),
        ("compact,delete", Some(Phase::CompactAndDelete)),
        ("compact-delete", Some(Phase::CompactAndDelete)),
        ("", Some(Phase::NoCleanup)),
        ("none", Some(Phase::NoCleanup)),
        ("off", Some(Phase::NoCleanup)),
        ("Attempted", None),
        ("ApiActivity", None),
    ];
    for (text, want) in cases {
        assert!(Phase::from_name(text) == want, "{text:?}");
    }
    assert!(Upper::parse("FATALERROR") == Some(Upper::FatalError));
    assert!(Upper::parse("warn") == None);
    assert!(Kebab::parse("highly-available") == Some(Kebab::HighlyAvailable));
    assert!(Lower::parse("highlyavailable") == Some(Lower::HighlyAvailable));
}

#[test]
fn all_lists_every_variant_in_declaration_order() {
    assert!(
        Phase::ALL
            == [
                Phase::Attempted,
                Phase::ApiActivity,
                Phase::CompactAndDelete,
                Phase::NoCleanup
            ]
    );
}

#[test]
fn label_value_encodes_the_text() {
    let family = Family::<PhaseLabel, Counter>::default();
    family
        .get_or_create(&PhaseLabel {
            phase: Phase::CompactAndDelete,
        })
        .inc();
    let mut registry = Registry::default();
    registry.register("phases", "Phases seen", family);
    let mut text = String::new();
    encode(&mut text, &registry).unwrap();
    assert!(
        text == "# HELP phases Phases seen.\n# TYPE phases counter\n\
                 phases_total{phase=\"compact,delete\"} 1\n# EOF\n"
    );
}
