//! The three KFC-7 schema-validation keys and the produce-path gate they
//! resolve to.

use crate::schema_validation::SchemaGate;

/// KFC-7: when `true`, the produce path validates the schema of every record
/// key on this topic. A record that fails the check is rejected with
/// `INVALID_RECORD` (87). Default `false`. This key does not reach
/// [`krabka_log::LogConfig`].
pub(crate) const SCHEMA_VALIDATION_KEY: &str = "schema.validation.key";

/// KFC-7: when `true`, the produce path validates the schema of every record
/// value on this topic. It is the same check that [`SCHEMA_VALIDATION_KEY`]
/// asks for, on the other half of the record. Default `false`.
pub(crate) const SCHEMA_VALIDATION_VALUE: &str = "schema.validation.value";

/// KFC-7: how much of a record the schema check reads. `id`, the default,
/// reads the five-byte Confluent header alone. `full` also decodes the body
/// against the schema that the header names. This key alone turns nothing on:
/// a topic that sets the mode and leaves both booleans `false` runs no check.
pub(crate) const SCHEMA_VALIDATION_MODE: &str = "schema.validation.mode";
pub(crate) const SCHEMA_VALIDATION_MODE_ID: &str = "id";
pub(crate) const SCHEMA_VALIDATION_MODE_FULL: &str = "full";

/// Resolve the KFC-7 schema-validation gate for `topic`. `None` means the
/// topic asks for no check, and no schema-validation code then runs on its
/// produce path. A missing or unparseable value resolves to its default:
/// `false` for the two booleans and `id` for the mode. This matches the
/// permissive runtime behavior of the other Produce-side topic config reads.
///
/// `schema.validation.mode` alone does not turn the check on, so a topic that
/// sets only the mode still resolves to `None`.
#[must_use]
pub(crate) fn resolve_schema_validation(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> Option<SchemaGate> {
    use crate::schema_validation::{SchemaGate, ValidationMode};

    let configs = image.topic_config(topic);
    let read = |key: &str| {
        configs
            .and_then(|configs| configs.get(key))
            .map(String::as_str)
    };
    let gate = SchemaGate {
        key: read(SCHEMA_VALIDATION_KEY).and_then(super::parse::bool_value) == Some(true),
        value: read(SCHEMA_VALIDATION_VALUE).and_then(super::parse::bool_value) == Some(true),
        mode: match read(SCHEMA_VALIDATION_MODE).map(super::parse::java_trim) {
            Some(SCHEMA_VALIDATION_MODE_FULL) => ValidationMode::Full,
            _ => ValidationMode::Id,
        },
    };
    gate.is_active().then_some(gate)
}

#[cfg(test)]
mod tests {
    use {
        super::{
            super::{
                log_config::apply_to_log_config,
                validation::{is_recognized, validate_topic_config},
            },
            *,
        },
        crate::config_keys::test_support::topic_config_record,
        assert2::{assert, check},
        krabka_log::LogConfig,
    };

    /// A metadata image whose topic `t` carries exactly `overrides`.
    fn image_with_topic_config(overrides: &[(&str, &str)]) -> krabka_metadata::MetadataImage {
        use {
            krabka_metadata::{MetadataImage, MetadataRecord},
            uuid::Uuid,
        };

        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1TopicConfig(
            topic_config_record().overrides(overrides).call(),
        ));
        image
    }

    #[test]
    fn validate_schema_validation_booleans_accept_bools_only() {
        let cases = [
            (SCHEMA_VALIDATION_KEY, "true", true),
            (SCHEMA_VALIDATION_KEY, "false", true),
            (SCHEMA_VALIDATION_KEY, "yes", false),
            (SCHEMA_VALIDATION_KEY, "True", true),
            (SCHEMA_VALIDATION_KEY, "", false),
            (SCHEMA_VALIDATION_VALUE, "true", true),
            (SCHEMA_VALIDATION_VALUE, "false", true),
            (SCHEMA_VALIDATION_VALUE, "1", false),
            (SCHEMA_VALIDATION_VALUE, "", false),
        ];
        for (key, value, want_ok) in cases {
            check!(
                validate_topic_config(key, value).is_ok() == want_ok,
                "{key}={value}"
            );
        }
    }

    #[test]
    fn validate_schema_validation_mode_accepts_the_two_modes_only() {
        let cases = [
            (SCHEMA_VALIDATION_MODE_ID, true),
            (SCHEMA_VALIDATION_MODE_FULL, true),
            ("Full", false),
            (" full ", true),
            ("body", false),
            ("", false),
        ];
        for (value, want_ok) in cases {
            check!(
                validate_topic_config(SCHEMA_VALIDATION_MODE, value).is_ok() == want_ok,
                "schema.validation.mode={value}"
            );
        }
    }

    #[test]
    fn schema_validation_mode_rejection_names_both_modes() {
        let error = validate_topic_config(SCHEMA_VALIDATION_MODE, "body").unwrap_err();
        assert!(
            error
                == "Invalid value body for configuration schema.validation.mode: String must be \
                    one of: id, full"
        );
    }

    #[test]
    fn is_recognized_includes_schema_validation_keys() {
        assert!(is_recognized(SCHEMA_VALIDATION_KEY));
        assert!(is_recognized(SCHEMA_VALIDATION_VALUE));
        assert!(is_recognized(SCHEMA_VALIDATION_MODE));
    }

    #[test]
    fn apply_leaves_log_config_alone_for_the_schema_validation_keys() {
        // All three keys are enforced on the produce path, so none of them may
        // reach the log's own config.
        let overrides = maplit::btreemap! {
        SCHEMA_VALIDATION_KEY.to_string() => "true".to_string(),
        SCHEMA_VALIDATION_VALUE.to_string() => "true".to_string(),
        SCHEMA_VALIDATION_MODE.to_string() => SCHEMA_VALIDATION_MODE_FULL.to_string()};
        assert!(apply_to_log_config(&overrides, &LogConfig::default()) == LogConfig::default());
    }

    #[test]
    fn resolve_schema_validation_reads_the_three_keys() {
        use crate::schema_validation::{SchemaGate, ValidationMode};

        let cases = [
            // Neither boolean is set, so the topic has no gate.
            (Vec::new(), None),
            (
                vec![
                    (SCHEMA_VALIDATION_KEY, "false"),
                    (SCHEMA_VALIDATION_VALUE, "false"),
                ],
                None,
            ),
            // The mode alone does not turn the check on.
            (
                vec![(SCHEMA_VALIDATION_MODE, SCHEMA_VALIDATION_MODE_FULL)],
                None,
            ),
            (
                vec![
                    (SCHEMA_VALIDATION_KEY, "false"),
                    (SCHEMA_VALIDATION_VALUE, "false"),
                    (SCHEMA_VALIDATION_MODE, SCHEMA_VALIDATION_MODE_FULL),
                ],
                None,
            ),
            // Either boolean alone gives a gate, and the mode defaults to `id`.
            (
                vec![(SCHEMA_VALIDATION_KEY, "true")],
                Some(SchemaGate {
                    key: true,
                    value: false,
                    mode: ValidationMode::Id,
                }),
            ),
            (
                vec![(SCHEMA_VALIDATION_VALUE, "true")],
                Some(SchemaGate {
                    key: false,
                    value: true,
                    mode: ValidationMode::Id,
                }),
            ),
            (
                vec![
                    (SCHEMA_VALIDATION_VALUE, "true"),
                    (SCHEMA_VALIDATION_MODE, SCHEMA_VALIDATION_MODE_ID),
                ],
                Some(SchemaGate {
                    key: false,
                    value: true,
                    mode: ValidationMode::Id,
                }),
            ),
            (
                vec![
                    (SCHEMA_VALIDATION_KEY, "true"),
                    (SCHEMA_VALIDATION_MODE, SCHEMA_VALIDATION_MODE_FULL),
                ],
                Some(SchemaGate {
                    key: true,
                    value: false,
                    mode: ValidationMode::Full,
                }),
            ),
            (
                vec![
                    (SCHEMA_VALIDATION_KEY, "true"),
                    (SCHEMA_VALIDATION_VALUE, "true"),
                    (SCHEMA_VALIDATION_MODE, SCHEMA_VALIDATION_MODE_FULL),
                ],
                Some(SchemaGate {
                    key: true,
                    value: true,
                    mode: ValidationMode::Full,
                }),
            ),
        ];
        for (overrides, want) in cases {
            let image = image_with_topic_config(&overrides);
            check!(
                resolve_schema_validation(&image, "t") == want,
                "{overrides:?}"
            );
        }
    }

    #[test]
    fn a_topic_with_no_config_at_all_has_no_schema_validation_gate() {
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        assert!(resolve_schema_validation(&image, "t").is_none());
    }

    #[test]
    fn corrupt_schema_validation_settings_resolve_to_their_defaults() {
        use crate::schema_validation::{SchemaGate, ValidationMode};

        // A corrupt boolean resolves to `false`, which leaves no gate.
        for value in ["yes", "1", ""] {
            let image = image_with_topic_config(&[
                (SCHEMA_VALIDATION_KEY, value),
                (SCHEMA_VALIDATION_VALUE, value),
            ]);
            check!(
                resolve_schema_validation(&image, "t").is_none(),
                "schema.validation.key=schema.validation.value={value}"
            );
        }

        // A corrupt mode resolves to `id`, and the gate stays on.
        for value in ["Full", "body", ""] {
            let image = image_with_topic_config(&[
                (SCHEMA_VALIDATION_VALUE, "true"),
                (SCHEMA_VALIDATION_MODE, value),
            ]);
            check!(
                resolve_schema_validation(&image, "t")
                    == Some(SchemaGate {
                        key: false,
                        value: true,
                        mode: ValidationMode::Id,
                    }),
                "schema.validation.mode={value}"
            );
        }
    }
}
