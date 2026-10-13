//! Krabka's `krabka.diskless` topic key: the per-topic opt-in to the diskless
//! WAL data path, its value check, the two exclusivity rules it carries, and
//! the immutability rule both alter paths enforce.

use {
    super::{
        REMOTE_STORAGE_ENABLE,
        delivery::{DELIVERY_MODE, DELIVERY_MODE_SCHEDULED},
    },
    std::collections::BTreeMap,
};

/// Krabka extension: per-topic opt-in to the diskless data path.
///
/// `true` routes every partition of the topic through the quorum-replicated
/// WAL, the object-store flusher, and the cold-read path instead of the plain
/// local log. The value is a `BOOLEAN` read the way Kafka reads one, so `TRUE`
/// turns the path on too. Any other value, and an absent key, leave the topic
/// on the local-log path.
pub(crate) const DISKLESS: &str = "krabka.diskless";

/// Resolve a topic's diskless flag from its stored override map.
///
/// This is read once, when a partition is opened
/// ([`crate::replicator_supervisor::materialize_partition`]), and it decides
/// which data path the partition's runtime is built for. Nothing re-reads it
/// afterwards, which is why [`validate_diskless_unchanged`] pins the value for
/// the life of the topic.
#[must_use]
pub(crate) fn resolve_diskless(config: Option<&BTreeMap<String, String>>) -> bool {
    config
        .and_then(|config| config.get(DISKLESS))
        .and_then(|value| super::parse::bool_value(value))
        == Some(true)
}

/// Reject an alter that would change a topic's diskless flag.
///
/// A partition reads [`DISKLESS`] once, at open, and builds either the WAL
/// runtime or the local-log runtime from it. Flipping the stored value
/// afterwards would leave every already-open partition on the old path and
/// every partition opened after the next restart on the new one, over the same
/// log directory. The flag is therefore fixed when the topic is created.
///
/// `current` is the topic's stored override map and `next` is the map the
/// alter would commit. Both paths compare the *resolved* flag, so dropping an
/// explicit `krabka.diskless=false` is as legal as leaving it in place, and an
/// `AlterConfigs` replacement that drops `krabka.diskless=true` is refused the
/// same way an explicit flip is.
pub(crate) fn validate_diskless_unchanged(
    current: Option<&BTreeMap<String, String>>,
    next: &BTreeMap<String, String>,
) -> Result<(), String> {
    let before = resolve_diskless(current);
    let after = resolve_diskless(Some(next));
    if before == after {
        return Ok(());
    }
    Err(format!(
        "{DISKLESS} is fixed when the topic is created and cannot be changed to `{after}`: a \
         partition reads it once, when it is opened, to build either the diskless WAL runtime or \
         the local-log runtime; create a new topic with the data path you want and reproduce into \
         it"
    ))
}

/// The two exclusivity rules the diskless data path carries, checked over a
/// topic's whole override map.
///
/// The first is KIP-405 tiered storage. A diskless partition already keeps its
/// records in the object store: the WAL flusher writes them there and the
/// cold-read path serves them back, and the local log is a trimmed projection
/// of that. Tiered storage is a second, independent uploader over the same
/// local log, with its own local-retention deletion. Turning both on would give
/// one partition two object-store copies and let tiered local retention delete
/// segments the diskless trim frontier still accounts for.
///
/// The second is KFC-1 scheduled delivery. The flusher copies through the
/// partition's durability high watermark rather than its delivery watermark, so
/// a batch that is not yet due can reach an object-store run and be trimmed
/// locally. The cold-read path
/// ([`crate::diskless::read::try_diskless_read`]) then serves that run's raw
/// bytes back with neither the local path's delivery-watermark cap nor the
/// tiered path's per-batch activation check, so a scheduled record would be
/// delivered before its own time. Until the cold path carries a delivery gate,
/// the pair is refused rather than silently breaking the one guarantee
/// scheduled delivery exists to give.
pub(super) fn validate_diskless_combination(
    overrides: &BTreeMap<String, String>,
) -> Result<(), String> {
    if !resolve_diskless(Some(overrides)) {
        return Ok(());
    }
    if super::validation::flag(overrides, REMOTE_STORAGE_ENABLE) {
        return Err(format!(
            "{DISKLESS}=true cannot be combined with {REMOTE_STORAGE_ENABLE}=true: a diskless \
             partition already keeps its records in the object store through the WAL flusher, and \
             tiered storage is a second uploader over the same local log whose local retention \
             would delete segments the diskless trim frontier still accounts for"
        ));
    }
    if overrides
        .get(DELIVERY_MODE)
        .is_some_and(|mode| mode == DELIVERY_MODE_SCHEDULED)
    {
        return Err(format!(
            "{DISKLESS}=true cannot be combined with {DELIVERY_MODE}={DELIVERY_MODE_SCHEDULED}: \
             the diskless flusher copies through the durability high watermark rather than the \
             delivery watermark, so a batch that is not yet due reaches an object-store run and is \
             trimmed locally, and the cold-read path serves that run back with no delivery gate, \
             which would deliver a scheduled record before its own time"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::{
            super::validation::{is_recognized, validate_topic_config, validate_topic_config_map},
            *,
        },
        assert2::{assert, check},
    };

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn resolve_diskless_reads_a_kafka_boolean() {
        for (label, config, expected) in [
            ("no override map at all", None, false),
            ("an override map without the key", Some(map(&[])), false),
            (
                "the key set to false",
                Some(map(&[(DISKLESS, "false")])),
                false,
            ),
            (
                "the key set in upper case",
                Some(map(&[(DISKLESS, " TRUE ")])),
                true,
            ),
            (
                "the key set to true",
                Some(map(&[(DISKLESS, "true")])),
                true,
            ),
        ] {
            check!(resolve_diskless(config.as_ref()) == expected, "{label}");
        }
    }

    #[test]
    fn the_key_is_whitelisted_and_takes_only_booleans() {
        assert!(is_recognized(DISKLESS));
        for (value, want_ok) in [
            ("true", true),
            ("false", true),
            ("TRUE", true),
            ("1", false),
            ("", false),
        ] {
            check!(
                validate_topic_config(DISKLESS, value).is_ok() == want_ok,
                "{DISKLESS}={value}"
            );
        }
        let error = validate_topic_config(DISKLESS, "yes").unwrap_err();
        assert!(error.contains(DISKLESS), "got: {error}");
    }

    #[derive(Clone, Copy)]
    enum DisklessConflict {
        TieredStorage,
        ScheduledDelivery,
    }

    type DisklessValidCases = Vec<(&'static str, BTreeMap<String, String>)>;

    #[yare::parameterized(
        tiered_storage = { DisklessConflict::TieredStorage },
        scheduled_delivery = { DisklessConflict::ScheduledDelivery },
    )]
    fn diskless_excludes_conflicting_topic_features(conflict: DisklessConflict) {
        let (key, conflicting_value, valid_cases): (&str, &str, DisklessValidCases) = match conflict
        {
            DisklessConflict::TieredStorage => (REMOTE_STORAGE_ENABLE, "true", vec![
                (
                    "diskless with tiering explicitly off",
                    map(&[(DISKLESS, "true"), (REMOTE_STORAGE_ENABLE, "false")]),
                ),
                (
                    "tiering with diskless explicitly off",
                    map(&[(DISKLESS, "false"), (REMOTE_STORAGE_ENABLE, "true")]),
                ),
                ("diskless alone", map(&[(DISKLESS, "true")])),
                ("tiering alone", map(&[(REMOTE_STORAGE_ENABLE, "true")])),
            ]),
            DisklessConflict::ScheduledDelivery => (DELIVERY_MODE, DELIVERY_MODE_SCHEDULED, vec![
                (
                    "an immediate diskless topic",
                    map(&[(DISKLESS, "true"), (DELIVERY_MODE, "immediate")]),
                ),
                (
                    "a scheduled local-log topic",
                    map(&[
                        (DISKLESS, "false"),
                        (DELIVERY_MODE, DELIVERY_MODE_SCHEDULED),
                    ]),
                ),
            ]),
        };
        for (label, overrides) in valid_cases {
            assert2::check!(validate_topic_config_map(&overrides).is_ok(), "{label}");
        }

        let overrides = map(&[(DISKLESS, "true"), (key, conflicting_value)]);
        let error = validate_topic_config_map(&overrides).unwrap_err();
        assert2::check!(error.contains(DISKLESS), "got: {error}");
        assert2::check!(error.contains(key), "got: {error}");
    }

    #[test]
    fn only_a_change_of_the_resolved_flag_is_refused() {
        for (label, current, next, want_ok) in [
            (
                "the same value restated",
                Some(map(&[(DISKLESS, "true")])),
                map(&[(DISKLESS, "true")]),
                true,
            ),
            (
                "an ordinary key added to a diskless topic",
                Some(map(&[(DISKLESS, "true")])),
                map(&[(DISKLESS, "true"), ("retention.ms", "60000")]),
                true,
            ),
            (
                "an explicit false dropped, which resolves the same way",
                Some(map(&[(DISKLESS, "false")])),
                map(&[]),
                true,
            ),
            (
                "turning the flag off on a diskless topic",
                Some(map(&[(DISKLESS, "true")])),
                map(&[(DISKLESS, "false")]),
                false,
            ),
            (
                "dropping the key from a diskless topic, as a replacement does",
                Some(map(&[(DISKLESS, "true")])),
                map(&[]),
                false,
            ),
            (
                "turning the flag on for a local-log topic",
                Some(map(&[])),
                map(&[(DISKLESS, "true")]),
                false,
            ),
            (
                "turning the flag on for a topic with no overrides at all",
                None,
                map(&[(DISKLESS, "true")]),
                false,
            ),
        ] {
            check!(
                validate_diskless_unchanged(current.as_ref(), &next).is_ok() == want_ok,
                "{label}"
            );
        }
    }

    #[test]
    fn the_refusal_names_the_key_and_says_it_is_fixed_at_creation() {
        let error = validate_diskless_unchanged(None, &map(&[(DISKLESS, "true")])).unwrap_err();

        assert!(error.contains(DISKLESS), "got: {error}");
        assert!(
            error.contains("fixed when the topic is created"),
            "got: {error}"
        );
    }
}
