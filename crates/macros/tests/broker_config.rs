//! The single struct generator retains configuration fields, attributes and derives.

use assert2::assert;

mod declared {
    krabka_macros::broker_config! {
        /// A generic configuration with an optional field excluded at compile time.
        pub struct Config<T: Clone + std::fmt::Debug> {
            /// The first field stays first in derived Debug output.
            pub enabled: bool,
            #[cfg(any())]
            pub absent: ThisTypeMustNotBeResolved,
            pub value: T,
        }
    }

    krabka_macros::broker_config! {
        pub struct Qualified {
            pub root: Option<std::path::PathBuf>,
            pub nested: Option<std::collections::BTreeMap<String, Vec<std::path::PathBuf>>>,
        }
    }

    krabka_macros::broker_config! {
        pub struct IndependentSettings {
            pub audit_enabled: bool,
            pub auto_join: bool,
            pub delete_topic_enable: bool,
            pub client_metrics_enable: bool,
        }
    }
}

#[test]
fn accepts_qualified_types_nested_inside_generic_field_types() {
    let original = declared::Qualified {
        root: Some(std::path::PathBuf::from("broker")),
        nested: Some(std::collections::BTreeMap::from([(
            "logs".to_owned(),
            vec![std::path::PathBuf::from("data")],
        )])),
    };
    let cloned = original.clone();
    assert!(cloned.root == original.root);
    assert!(cloned.nested == original.nested);
}

#[test]
fn constructs_independent_boolean_settings_without_changing_the_field_types() {
    let settings = declared::IndependentSettings {
        audit_enabled: true,
        auto_join: false,
        delete_topic_enable: true,
        client_metrics_enable: false,
    };
    assert!(settings.audit_enabled && settings.delete_topic_enable);
    assert!(!settings.auto_join && !settings.client_metrics_enable);
    assert!(
        format!("{settings:?}")
            == "IndependentSettings { audit_enabled: true, auto_join: false, delete_topic_enable: true, client_metrics_enable: false }"
    );
}

#[test]
fn preserves_public_fields_generics_conditional_fields_and_derive_order() {
    let original = declared::Config {
        enabled: true,
        value: "broker".to_owned(),
    };
    let cloned = original.clone();
    assert!(cloned.enabled);
    assert!(cloned.value == original.value);
    assert!(format!("{cloned:?}") == "Config { enabled: true, value: \"broker\" }");
}
