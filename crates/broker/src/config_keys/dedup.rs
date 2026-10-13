//! The two KFC-10 produce-time deduplication keys and the settings they
//! resolve to.
//!
//! A topic with a dedup window drops, on the leader at produce time, a record
//! whose dedup identity it already admitted inside the window. Neither key
//! reaches `Log.config`: the produce path and the replica supervisor read them
//! through [`resolve_dedup_topic_config`].

use {
    super::parse::{invalid_value, java_trim, long_value, parse_long},
    krabka_metadata::MetadataImage,
    std::collections::BTreeMap,
};

/// KFC-10: the deduplication window in milliseconds. A record whose identity
/// the leader already admitted within the window is dropped. `-1`, the
/// default, turns deduplication off. `0` and values below `-1` are refused.
pub(crate) const DEDUP_WINDOW_MS: &str = "dedup.window.ms";

/// KFC-10 sentinel for [`DEDUP_WINDOW_MS`]: `-1` turns deduplication off.
pub(crate) const DEDUP_WINDOW_DISABLED: i64 = -1;

/// KFC-10: the name of the record header whose value is the dedup identity.
/// Unset, the default, uses the record key. A set value must not be empty
/// after Kafka's trim.
pub(crate) const DEDUP_KEY_HEADER: &str = "dedup.key.header";

/// The dedup settings that a topic's stored config asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DedupTopicConfig {
    /// The window in milliseconds. It is always 1 or more.
    pub(crate) window_ms: i64,
    /// The header whose value is the dedup identity. `None` uses the record
    /// key.
    pub(crate) key_header: Option<String>,
}

/// Resolve the dedup settings from a topic's stored override map. `None`
/// means that deduplication is off.
///
/// The read is permissive, like the other produce-side topic config reads. A
/// missing, unparseable or out-of-range window turns deduplication off. An
/// empty header name falls back to key mode.
#[must_use]
pub(crate) fn dedup_topic_config(overrides: &BTreeMap<String, String>) -> Option<DedupTopicConfig> {
    let window_ms = overrides
        .get(DEDUP_WINDOW_MS)
        .and_then(|value| long_value(value))
        .filter(|millis| *millis >= 1)?;
    let key_header = overrides
        .get(DEDUP_KEY_HEADER)
        .map(|value| java_trim(value))
        .filter(|name| !name.is_empty())
        .map(str::to_owned);
    Some(DedupTopicConfig {
        window_ms,
        key_header,
    })
}

/// Resolve the dedup settings of `topic` from the metadata image. `None`
/// means that deduplication is off, also for a topic with no stored config.
#[must_use]
pub(crate) fn resolve_dedup_topic_config(
    image: &MetadataImage,
    topic: &str,
) -> Option<DedupTopicConfig> {
    image.topic_config(topic).and_then(dedup_topic_config)
}

/// The value check of [`DEDUP_WINDOW_MS`]: a `LONG` that is `-1` or 1 or
/// more. Kafka has no validator of this shape, so the key owns it.
pub(super) fn parse_dedup_window(value: &str) -> Result<i64, String> {
    let parsed = parse_long(DEDUP_WINDOW_MS, value)?;
    if parsed == DEDUP_WINDOW_DISABLED || parsed >= 1 {
        return Ok(parsed);
    }
    Err(invalid_value(
        DEDUP_WINDOW_MS,
        parsed,
        &format!("Value must be at least 1, or {DEDUP_WINDOW_DISABLED} to disable deduplication"),
    ))
}

/// The value check of [`DEDUP_KEY_HEADER`]: a `STRING`, trimmed as Kafka's
/// `ConfigDef.parseType` trims one, that is not empty. An empty name cannot
/// match a header, so the key refuses it instead of storing a dead setting.
pub(super) fn parse_dedup_key_header(value: &str) -> Result<&str, String> {
    let name = java_trim(value);
    if name.is_empty() {
        // Kafka's validators print the parsed value, which is the trimmed one.
        return Err(invalid_value(
            DEDUP_KEY_HEADER,
            name,
            "Header name must not be empty",
        ));
    }
    Ok(name)
}

#[cfg(test)]
mod tests {
    use {
        super::{
            super::{
                DISKLESS,
                log_config::apply_to_log_config,
                validation::{
                    canonical_topic_config, is_recognized, validate_topic_config,
                    validate_topic_config_map,
                },
            },
            *,
        },
        crate::api_catalog::UnstableApiVersions,
        assert2::{assert, check},
        krabka_log::LogConfig,
        krabka_metadata::{MetadataRecord, TopicConfigRecord},
        uuid::Uuid,
    };

    /// A config key and its value.
    #[derive(Debug)]
    struct ConfigPair<'a> {
        key: &'a str,
        value: &'a str,
    }

    fn map(pairs: &[ConfigPair<'_>]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|ConfigPair { key, value }| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn window(value: &str) -> ConfigPair<'_> {
        ConfigPair {
            key: DEDUP_WINDOW_MS,
            value,
        }
    }

    fn header(value: &str) -> ConfigPair<'_> {
        ConfigPair {
            key: DEDUP_KEY_HEADER,
            value,
        }
    }

    #[test]
    fn both_keys_are_recognized() {
        check!(is_recognized(DEDUP_WINDOW_MS));
        check!(is_recognized(DEDUP_KEY_HEADER));
    }

    #[test]
    fn the_window_stores_its_canonical_long_or_is_refused() {
        let cases = [
            ("-1", Ok("-1")),
            ("1", Ok("1")),
            (" 60000 ", Ok("60000")),
            ("9223372036854775807", Ok("9223372036854775807")),
            (
                "0",
                Err(
                    "Invalid value 0 for configuration dedup.window.ms: Value must be at least \
                     1, or -1 to disable deduplication",
                ),
            ),
            (
                "-2",
                Err(
                    "Invalid value -2 for configuration dedup.window.ms: Value must be at least \
                     1, or -1 to disable deduplication",
                ),
            ),
            ("-9223372036854775808", Err("")),
            ("soon", Err("")),
            ("", Err("")),
            ("9223372036854775808", Err("")),
        ];
        for (value, want) in cases {
            let got = canonical_topic_config(DEDUP_WINDOW_MS, value, UnstableApiVersions::Disabled);
            match want {
                Ok(canonical) => {
                    check!(got == Ok(canonical.to_owned()), "{value:?}");
                }
                // An empty expectation only asks for a refusal: the parse
                // failures carry the generic `LONG` text that `parse` owns.
                Err("") => {
                    check!(got.is_err(), "{value:?}");
                }
                Err(message) => {
                    check!(got == Err(message.to_owned()), "{value:?}");
                }
            }
        }
    }

    #[test]
    fn the_header_stores_its_trimmed_name_or_is_refused_when_empty() {
        let cases = [
            ("dedup-id", Ok("dedup-id")),
            (" Idempotency-Key ", Ok("Idempotency-Key")),
            ("ключ", Ok("ключ")),
            ("a b", Ok("a b")),
            ("", Err(())),
            ("   ", Err(())),
        ];
        for (value, want) in cases {
            let got =
                canonical_topic_config(DEDUP_KEY_HEADER, value, UnstableApiVersions::Disabled);
            check!(
                got.as_deref().map_err(drop) == want,
                "{value:?} gave {got:?}"
            );
        }
        let error = validate_topic_config(DEDUP_KEY_HEADER, " ").unwrap_err();
        assert!(
            error
                == "Invalid value  for configuration dedup.key.header: Header name must not \
                      be empty"
        );
    }

    #[test]
    fn apply_leaves_log_config_alone_for_the_dedup_keys() {
        // Both keys are enforced on the produce path, so neither may reach the
        // log's own config.
        let overrides = map(&[window("60000"), header("dedup-id")]);
        assert!(apply_to_log_config(&overrides, &LogConfig::default()) == LogConfig::default());
    }

    #[test]
    fn a_diskless_topic_refuses_a_dedup_window_but_not_an_inert_header() {
        let diskless = || ConfigPair {
            key: DISKLESS,
            value: "true",
        };
        for (label, pairs) in [
            ("diskless with a header and no window", vec![
                diskless(),
                header("dedup-id"),
            ]),
            ("a window and a header", vec![
                window("60000"),
                header("dedup-id"),
            ]),
            ("a header with the window off", vec![header("dedup-id")]),
        ] {
            check!(validate_topic_config_map(&map(&pairs)) == Ok(()), "{label}");
        }

        let error = validate_topic_config_map(&map(&[diskless(), window("60000")])).unwrap_err();
        assert!(
            error
                == "dedup.window.ms is not supported on diskless topics: krabka.diskless=true \
                    cannot be combined with a dedup.window.ms other than -1, because \
                    produce-time deduplication runs only on the local-log produce path"
        );
    }

    #[test]
    fn dedup_topic_config_resolves_the_stored_map() {
        let cases = [
            ("no keys", vec![], None),
            ("the window off", vec![window("-1")], None),
            ("a header without a window", vec![header("dedup-id")], None),
            (
                "a header with the window off",
                vec![window("-1"), header("dedup-id")],
                None,
            ),
            (
                "a window in key mode",
                vec![window("60000")],
                Some(DedupTopicConfig {
                    window_ms: 60_000,
                    key_header: None,
                }),
            ),
            (
                "a window in header mode",
                vec![window("1"), header("dedup-id")],
                Some(DedupTopicConfig {
                    window_ms: 1,
                    key_header: Some("dedup-id".to_owned()),
                }),
            ),
            (
                "an untrimmed value written before canonical storage",
                vec![window(" 5000 "), header(" dedup-id ")],
                Some(DedupTopicConfig {
                    window_ms: 5000,
                    key_header: Some("dedup-id".to_owned()),
                }),
            ),
            // Corrupt stored values resolve permissively: a bad window turns
            // dedup off, and a bad header falls back to key mode.
            ("a corrupt window", vec![window("soon"), header("h")], None),
            ("a zero window", vec![window("0")], None),
            ("a window below -1", vec![window("-5")], None),
            ("an empty window", vec![window("")], None),
            (
                "an empty header",
                vec![window("60000"), header("")],
                Some(DedupTopicConfig {
                    window_ms: 60_000,
                    key_header: None,
                }),
            ),
            (
                "a blank header",
                vec![window("60000"), header("  ")],
                Some(DedupTopicConfig {
                    window_ms: 60_000,
                    key_header: None,
                }),
            ),
        ];
        for (label, pairs, want) in cases {
            check!(dedup_topic_config(&map(&pairs)) == want, "{label}");
        }
    }

    #[test]
    fn resolve_dedup_topic_config_reads_the_topic_from_the_image() {
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: map(&[window("60000"), header("dedup-id")]),
        }));

        check!(
            resolve_dedup_topic_config(&image, "t")
                == Some(DedupTopicConfig {
                    window_ms: 60_000,
                    key_header: Some("dedup-id".to_owned()),
                })
        );
        check!(resolve_dedup_topic_config(&image, "other").is_none());
        check!(resolve_dedup_topic_config(&MetadataImage::new(Uuid::nil()), "t").is_none());
    }
}
