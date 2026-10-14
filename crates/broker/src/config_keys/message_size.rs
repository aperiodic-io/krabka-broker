//! Kafka's `max.message.bytes` topic key: the per-topic lookup the produce
//! path reads, with the broker-wide `message.max.bytes` behind it.
//!
//! The value check lives in [`super::registry`] with every other key's, as an
//! `INT` row with `atLeast(0)`: `0` is legal, `-1` is not, and a value past
//! `i32::MAX` is refused as "not a 32-bit integer" rather than as an
//! out-of-range one, which is what Kafka's own `ConfigDef` does.

use {
    super::MAX_MESSAGE_BYTES,
    krabka_units::{ByteSize, convert::ByteSizeExt as _},
};

/// Resolve the largest record batch a topic accepts.
///
/// `broker_default` is the broker's `message.max.bytes`, which is what Kafka
/// reports as the `DEFAULT_CONFIG` synonym of an unset `max.message.bytes`.
/// A corrupt stored value falls back to it too, matching the other
/// produce-side config reads: `AlterConfigs` already refused the value, so a
/// string here that does not parse means a damaged metadata image and not an
/// operator's intent.
#[must_use]
pub(crate) fn resolve_max_message_bytes(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    broker_default: ByteSize,
) -> ByteSize {
    image
        .topic_config(topic)
        .and_then(|configs| configs.get(MAX_MESSAGE_BYTES))
        .and_then(|value| super::parse::int_value(value))
        .and_then(|value| u64::try_from(value).ok())
        .map_or(broker_default, ByteSize::from_bytes)
}

#[cfg(test)]
mod tests {
    use {
        super::{super::validation::validate_topic_config, *},
        assert2::assert,
        krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord},
        krabka_units::bytes,
        std::collections::BTreeMap,
        uuid::Uuid,
    };

    #[yare::parameterized(
        zero = { "0", true },
        positive = { "1048588", true },
        i32_max = { "2147483647", true },
        negative = { "-1", false },
        above_i32_max = { "2147483648", false },
        non_integer = { "abc", false },
        empty = { "", false },
    )]
    fn max_message_bytes_matches_kafkas_int_at_least_zero(value: &str, accepted: bool) {
        assert2::assert!(
            validate_topic_config(MAX_MESSAGE_BYTES, value).is_ok() == accepted,
            "max.message.bytes={value}"
        );
    }

    fn image_with(value: Option<&str>) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        if let Some(value) = value {
            let mut overrides = BTreeMap::new();
            overrides.insert(MAX_MESSAGE_BYTES.to_owned(), value.to_owned());
            image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "t".to_owned(),
                overrides,
            }));
        }
        image
    }

    #[yare::parameterized(
        no_override = { None, bytes(1_048_588) },
        positive_override = { Some("2048"), bytes(2048) },
        zero_override = { Some("0"), bytes(0) },
        corrupt_override = { Some("not-a-number"), bytes(1_048_588) },
        negative_override = { Some("-5"), bytes(1_048_588) },
    )]
    fn resolve_max_message_bytes_prefers_the_override_and_falls_back_otherwise(
        stored: Option<&str>,
        want: krabka_units::ByteSize,
    ) {
        let default = bytes(1_048_588);

        assert2::assert!(
            resolve_max_message_bytes(&image_with(stored), "t", default) == want,
            "stored {stored:?}"
        );
    }

    #[test]
    fn resolve_max_message_bytes_falls_back_on_a_topic_with_no_config_at_all() {
        let default = bytes(4096);
        assert!(resolve_max_message_bytes(&image_with(None), "ghost", default) == default);
    }
}
