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
                test_support::{TopicOverride, topic_config_record},
                validation::{is_recognized, validate_topic_config},
            },
            *,
        },
        assert2::{assert, check},
        krabka_log::LogConfig,
        krabka_metadata::{MetadataImage, MetadataRecord},
        uuid::Uuid,
    };

    /// A metadata image whose topic `t` carries exactly `overrides`.
    fn image_with_topic_config(overrides: &[TopicOverride<'_>]) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1TopicConfig(topic_config_record(
            overrides,
        )));
        image
    }

    #[yare::parameterized(
        key_true = { SCHEMA_VALIDATION_KEY, "true", true },
        key_false = { SCHEMA_VALIDATION_KEY, "false", true },
        key_yes = { SCHEMA_VALIDATION_KEY, "yes", false },
        key_uppercase_true = { SCHEMA_VALIDATION_KEY, "True", true },
        key_empty = { SCHEMA_VALIDATION_KEY, "", false },
        value_true = { SCHEMA_VALIDATION_VALUE, "true", true },
        value_false = { SCHEMA_VALIDATION_VALUE, "false", true },
        value_numeric_one = { SCHEMA_VALIDATION_VALUE, "1", false },
        value_empty = { SCHEMA_VALIDATION_VALUE, "", false },
    )]
    fn validate_schema_validation_booleans_accept_bools_only(
        key: &str,
        value: &str,
        want_ok: bool,
    ) {
        check!(
            validate_topic_config(key, value).is_ok() == want_ok,
            "{key}={value}"
        );
    }

    #[yare::parameterized(
        id = { SCHEMA_VALIDATION_MODE_ID, true },
        full = { SCHEMA_VALIDATION_MODE_FULL, true },
        uppercase = { "Full", false },
        trimmed_full = { " full ", true },
        body = { "body", false },
        empty = { "", false },
    )]
    fn validate_schema_validation_mode_accepts_the_two_modes_only(value: &str, want_ok: bool) {
        check!(
            validate_topic_config(SCHEMA_VALIDATION_MODE, value).is_ok() == want_ok,
            "schema.validation.mode={value}"
        );
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
                    TopicOverride {
                        key: SCHEMA_VALIDATION_KEY,
                        value: "false",
                    },
                    TopicOverride {
                        key: SCHEMA_VALIDATION_VALUE,
                        value: "false",
                    },
                ],
                None,
            ),
            // The mode alone does not turn the check on.
            (
                vec![TopicOverride {
                    key: SCHEMA_VALIDATION_MODE,
                    value: SCHEMA_VALIDATION_MODE_FULL,
                }],
                None,
            ),
            (
                vec![
                    TopicOverride {
                        key: SCHEMA_VALIDATION_KEY,
                        value: "false",
                    },
                    TopicOverride {
                        key: SCHEMA_VALIDATION_VALUE,
                        value: "false",
                    },
                    TopicOverride {
                        key: SCHEMA_VALIDATION_MODE,
                        value: SCHEMA_VALIDATION_MODE_FULL,
                    },
                ],
                None,
            ),
            // Either boolean alone gives a gate, and the mode defaults to `id`.
            (
                vec![TopicOverride {
                    key: SCHEMA_VALIDATION_KEY,
                    value: "true",
                }],
                Some(SchemaGate {
                    key: true,
                    value: false,
                    mode: ValidationMode::Id,
                }),
            ),
            (
                vec![TopicOverride {
                    key: SCHEMA_VALIDATION_VALUE,
                    value: "true",
                }],
                Some(SchemaGate {
                    key: false,
                    value: true,
                    mode: ValidationMode::Id,
                }),
            ),
            (
                vec![
                    TopicOverride {
                        key: SCHEMA_VALIDATION_VALUE,
                        value: "true",
                    },
                    TopicOverride {
                        key: SCHEMA_VALIDATION_MODE,
                        value: SCHEMA_VALIDATION_MODE_ID,
                    },
                ],
                Some(SchemaGate {
                    key: false,
                    value: true,
                    mode: ValidationMode::Id,
                }),
            ),
            (
                vec![
                    TopicOverride {
                        key: SCHEMA_VALIDATION_KEY,
                        value: "true",
                    },
                    TopicOverride {
                        key: SCHEMA_VALIDATION_MODE,
                        value: SCHEMA_VALIDATION_MODE_FULL,
                    },
                ],
                Some(SchemaGate {
                    key: true,
                    value: false,
                    mode: ValidationMode::Full,
                }),
            ),
            (
                vec![
                    TopicOverride {
                        key: SCHEMA_VALIDATION_KEY,
                        value: "true",
                    },
                    TopicOverride {
                        key: SCHEMA_VALIDATION_VALUE,
                        value: "true",
                    },
                    TopicOverride {
                        key: SCHEMA_VALIDATION_MODE,
                        value: SCHEMA_VALIDATION_MODE_FULL,
                    },
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

    /// A corrupt boolean resolves to `false`, which leaves no gate.
    #[yare::parameterized(
        yes = { "yes" },
        numeric_one = { "1" },
        empty = { "" },
    )]
    fn corrupt_schema_validation_boolean_resolves_to_false(value: &str) {
        let image = image_with_topic_config(&[
            TopicOverride {
                key: SCHEMA_VALIDATION_KEY,
                value,
            },
            TopicOverride {
                key: SCHEMA_VALIDATION_VALUE,
                value,
            },
        ]);
        check!(
            resolve_schema_validation(&image, "t").is_none(),
            "schema.validation.key=schema.validation.value={value}"
        );
    }

    /// A corrupt mode resolves to `id`, and the gate stays on.
    #[yare::parameterized(
        uppercase_full = { "Full" },
        body = { "body" },
        empty = { "" },
    )]
    fn corrupt_schema_validation_mode_resolves_to_id(value: &str) {
        use crate::schema_validation::{SchemaGate, ValidationMode};

        let image = image_with_topic_config(&[
            TopicOverride {
                key: SCHEMA_VALIDATION_VALUE,
                value: "true",
            },
            TopicOverride {
                key: SCHEMA_VALIDATION_MODE,
                value,
            },
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
