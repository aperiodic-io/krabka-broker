//! The three KFC-1 scheduled-delivery keys and the produce-path resolver for
//! the one of them that never reaches `Log.config`.

use krabka_units::{Time, convert::wire::opt_time_from_millis_i64};

/// KFC-1: per-topic delivery mode. `immediate`, the default, is Kafka's
/// behavior. `scheduled` makes each batch's `max_timestamp` its delivery time,
/// so a record stays invisible to consumers until it comes due. This is the
/// one delivery key that reaches [`krabka_log::LogConfig::delivery_policy`].
pub(crate) const DELIVERY_MODE: &str = "delivery.mode";
pub(crate) const DELIVERY_MODE_IMMEDIATE: &str = "immediate";
pub(crate) const DELIVERY_MODE_SCHEDULED: &str = "scheduled";

/// KFC-1: the largest delay the produce path accepts, measured forward from
/// produce time. `-1` removes the limit. A batch scheduled further ahead is
/// rejected with `INVALID_TIMESTAMP` (32).
pub(crate) const DELIVERY_MAX_DELAY_MS: &str = "delivery.max.delay.ms";
/// Default `delivery.max.delay.ms`: 7 days.
pub(crate) const DEFAULT_DELIVERY_MAX_DELAY_MS: i64 = 604_800_000;

/// KFC-1: when `true`, the log refuses a batch whose delivery time is before
/// the largest delivery time already in the partition. It turns a silently
/// stalled schedule into an `INVALID_TIMESTAMP` (32) at the producer that
/// caused it. Default `false`. It reaches
/// [`krabka_log::LogConfig::schedule_order`], because only the log's own
/// lock makes the test and the append it guards one critical section.
pub(crate) const DELIVERY_SCHEDULE_MONOTONIC: &str = "delivery.schedule.monotonic";

/// KFC-1 sentinel for `delivery.max.delay.ms`: `-1` means no bound on how far
/// ahead a batch may be scheduled, and is the lowest legal value.
pub(super) const DELIVERY_MAX_DELAY_UNLIMITED: i64 = -1;

/// Resolve `delivery.max.delay.ms` for `topic`: the largest delay the produce
/// path accepts, measured forward from produce time. `None` is the `-1`
/// sentinel and removes the bound. A missing or unparseable value resolves to
/// the 7-day default, which matches the permissive runtime behavior of the
/// other Produce-side topic config reads.
#[must_use]
pub(crate) fn resolve_delivery_max_delay(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> Option<Time> {
    let millis = image
        .topic_config(topic)
        .and_then(|configs| configs.get(DELIVERY_MAX_DELAY_MS))
        .and_then(|value| super::parse::long_value(value))
        .filter(|millis| *millis >= DELIVERY_MAX_DELAY_UNLIMITED)
        .unwrap_or(DEFAULT_DELIVERY_MAX_DELAY_MS);
    opt_time_from_millis_i64(millis)
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
        assert2::assert,
        krabka_log::LogConfig,
        krabka_units::millis,
        std::collections::BTreeMap,
    };

    #[yare::parameterized(
        immediate = { DELIVERY_MODE_IMMEDIATE, true },
        scheduled = { DELIVERY_MODE_SCHEDULED, true },
        unknown = { "later", false },
        empty = { "", false },
    )]
    fn validate_delivery_mode_accepts_the_two_modes_only(value: &str, want_ok: bool) {
        assert2::assert!(
            validate_topic_config(DELIVERY_MODE, value).is_ok() == want_ok,
            "delivery.mode={value}"
        );
    }

    #[yare::parameterized(
        zero = { "0", true },
        default = { "604800000", true },
        unlimited = { "-1", true },
        below_unlimited = { "-2", false },
        non_integer = { "soon", false },
    )]
    fn validate_delivery_max_delay_ms_boundary_cases(value: &str, want_ok: bool) {
        assert2::assert!(
            validate_topic_config(DELIVERY_MAX_DELAY_MS, value).is_ok() == want_ok,
            "delivery.max.delay.ms={value}"
        );
    }

    #[yare::parameterized(
        true_value = { "true", true },
        false_value = { "false", true },
        non_boolean = { "yes", false },
        empty = { "", false },
    )]
    fn validate_delivery_schedule_monotonic_accepts_bools_only(value: &str, want_ok: bool) {
        assert2::assert!(
            validate_topic_config(DELIVERY_SCHEDULE_MONOTONIC, value).is_ok() == want_ok,
            "delivery.schedule.monotonic={value}"
        );
    }

    #[test]
    fn is_recognized_includes_delivery_keys() {
        assert!(is_recognized(DELIVERY_MODE));
        assert!(is_recognized(DELIVERY_MAX_DELAY_MS));
        assert!(is_recognized(DELIVERY_SCHEDULE_MONOTONIC));
    }

    #[test]
    fn apply_delivery_mode_propagates_both_ways() {
        let mut scheduled = BTreeMap::new();
        scheduled.insert(DELIVERY_MODE.into(), DELIVERY_MODE_SCHEDULED.into());
        assert!(
            apply_to_log_config(&scheduled, &LogConfig::default())
                == LogConfig {
                    delivery_policy: krabka_log::DeliveryPolicy::Scheduled,
                    ..LogConfig::default()
                }
        );

        let base = LogConfig {
            delivery_policy: krabka_log::DeliveryPolicy::Scheduled,
            ..LogConfig::default()
        };
        let mut immediate = BTreeMap::new();
        immediate.insert(DELIVERY_MODE.into(), DELIVERY_MODE_IMMEDIATE.into());
        assert!(apply_to_log_config(&immediate, &base) == LogConfig::default());
    }

    #[test]
    fn apply_carries_schedule_monotonic_and_leaves_the_produce_side_key_alone() {
        // `delivery.max.delay.ms` is enforced on the produce path against the
        // broker's clock, so it may not move any of the log's own settings.
        // `delivery.schedule.monotonic` is enforced by the log itself, so it
        // has to reach the log's config and nothing else.
        let mut overrides = BTreeMap::new();
        overrides.insert(DELIVERY_MAX_DELAY_MS.into(), "1000".into());
        assert!(apply_to_log_config(&overrides, &LogConfig::default()) == LogConfig::default());

        overrides.insert(DELIVERY_SCHEDULE_MONOTONIC.into(), "true".into());
        assert!(
            apply_to_log_config(&overrides, &LogConfig::default())
                == LogConfig {
                    schedule_order: krabka_log::ScheduleOrder::Monotonic,
                    ..LogConfig::default()
                }
        );
    }

    #[yare::parameterized(
        no_override = { None, Some(604_800_000), false },
        valid_override = { Some("90000"), Some(90_000), false },
        disabled_override = { Some("-1"), None, false },
        malformed_word = { Some("soon"), Some(604_800_000), true },
        negative_override = { Some("-5"), Some(604_800_000), true },
        empty_override = { Some(""), Some(604_800_000), true },
    )]
    fn delivery_max_delay_resolves_overrides_and_falls_back_for_invalid_values(
        value: Option<&str>,
        expected_millis: Option<u32>,
        check_schedule_default: bool,
    ) {
        use {
            krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord},
            uuid::Uuid,
        };

        let mut overrides = maplit::btreemap! {};
        if let Some(value) = value {
            overrides.insert(DELIVERY_MAX_DELAY_MS.to_owned(), value.to_owned());
            if check_schedule_default {
                overrides.insert(DELIVERY_SCHEDULE_MONOTONIC.to_owned(), value.to_owned());
            }
        }
        let mut image = MetadataImage::new(Uuid::nil());
        if !overrides.is_empty() {
            image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "t".into(),
                overrides: overrides.clone(),
            }));
        }

        assert2::assert!(
            resolve_delivery_max_delay(&image, "t") == expected_millis.map(millis),
            "delivery.max.delay.ms={value:?}"
        );
        if let (true, Some(value)) = (check_schedule_default, value) {
            // A corrupt `delivery.schedule.monotonic` is not "true", so the
            // applier leaves the log's default in place.
            assert2::assert!(
                apply_to_log_config(
                    &maplit::btreemap! {
                        DELIVERY_SCHEDULE_MONOTONIC.to_owned() => value.to_owned()
                    },
                    &LogConfig::default()
                ) == LogConfig::default(),
                "delivery.schedule.monotonic={value:?}"
            );
        }
    }
}
