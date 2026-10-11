//! Kafka's guard against replaying a feature level this controller does not
//! support.
//!
//! `FeatureControlManager.replay(FeatureLevelRecord)` looks the feature up in
//! the controller's own supported features and throws when the record's level
//! lies outside that range:
//!
//! ```text
//! Tried to apply FeatureLevelRecord ..., but this controller only supports versions <range>
//! ```
//!
//! The exception is fatal, so a controller never runs at a level that it did
//! not advertise. That is what protects a cluster whose log finalized an
//! unstable `metadata.version` from a node that started with
//! `unstable.feature.versions.enable` off. A feature the controller does not
//! know is supported at level 0 only, as `QuorumFeatures.localSupportedFeature`
//! answers `DISABLED`, the range `0-0`, for it: a log that finalized a feature
//! from a newer release at any level above 0 is refused, and the message names
//! the range as `0`.
//!
//! Kafka's fatal fault halts the process (`ProcessTerminatingFaultHandler`,
//! exit status 1). A library does not halt its host, so the controller
//! reports the fault and the embedder halts. A controller that starts over such
//! a log refuses to start with a [`RaftError::FatalFault`](crate::RaftError),
//! and one that applies such a level from its leader stops itself: it publishes the
//! refusal on [`ControllerHandle::watch_fatal`](crate::ControllerHandle), then
//! cancels the listener and the engine, and every later submit fails with
//! [`RaftError::Shutdown`](crate::RaftError). A combined broker turns that
//! signal into a failed `Broker::start` or, once started, into its
//! self-shutdown, and `krabka-broker` exits non-zero with the refusal, so the
//! process never runs on without its controller.
//!
//! The engine applies a record and publishes the resulting image, so the guard
//! reads the finalized levels of a published image: the one the engine
//! recovered at start-up, and each one it publishes afterwards. The image keeps
//! the last level of each feature, so a record that a later record superseded
//! is not named. Kafka would name it, and refuses the same logs.

use krabka_metadata::MetadataImage;

use crate::{config::UnstableFeatureVersions, supported_feature_range};

/// Kafka's `VersionRange.toString()`: a single level as itself, and a span as
/// `min-max`.
fn range_text(min: i16, max: i16) -> String {
    if min == max {
        min.to_string()
    } else {
        format!("{min}-{max}")
    }
}

/// Kafka's `QuorumFeatures.DISABLED`, the range of a feature that the
/// controller does not support.
const UNKNOWN_FEATURE_RANGE: (i16, i16) = (0, 0);

/// The refusal for the first finalized feature of `image` whose level this
/// controller does not support under `unstable`, or `None` when it supports
/// them all.
pub(super) fn unsupported_feature_level(
    image: &MetadataImage,
    unstable: UnstableFeatureVersions,
) -> Option<String> {
    image.finalized_features().iter().find_map(|(name, level)| {
        // `QuorumFeatures.localSupportedFeature`: a feature this controller
        // does not know is `DISABLED`, supported at level 0 only.
        let (min, max) = krabka_metadata::feature(name).map_or(UNKNOWN_FEATURE_RANGE, |feature| {
            supported_feature_range(feature, unstable)
        });
        (!(min..=max).contains(level)).then(|| {
            format!(
                "Tried to apply FeatureLevelRecord FeatureLevelRecord(name='{name}', \
                 featureLevel={level}), but this controller only supports versions {}",
                range_text(min, max)
            )
        })
    })
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
    use uuid::Uuid;

    use super::*;

    /// An image that finalized each of `levels`.
    fn image_at(levels: &[(&str, i16)]) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        for (name, level) in levels {
            image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: (*name).into(),
                level: *level,
            }));
        }
        image
    }

    fn refusal(name: &str, level: i16, range: &str) -> String {
        format!(
            "Tried to apply FeatureLevelRecord FeatureLevelRecord(name='{name}', \
             featureLevel={level}), but this controller only supports versions {range}"
        )
    }

    /// Kafka 4.3's latest production `metadata.version` is `4.3-IV0` (30), and
    /// `4.4-IV2` (33) is one of trunk's unstable levels.
    #[test]
    fn a_level_outside_the_supported_range_is_refused_with_kafkas_message() {
        use UnstableFeatureVersions::{Disabled, Enabled};

        for (what, levels, unstable, expected) in [
            (
                "the latest production level",
                vec![("metadata.version", 30)],
                Disabled,
                None,
            ),
            (
                "an unstable level with the flag off",
                vec![("metadata.version", 33)],
                Disabled,
                Some(refusal("metadata.version", 33, "7-30")),
            ),
            (
                "an unstable level with the flag on",
                vec![("metadata.version", 33)],
                Enabled,
                None,
            ),
            (
                "a feature above its range",
                vec![("share.version", 5)],
                Enabled,
                Some(refusal("share.version", 5, "0-2")),
            ),
            (
                "a level below the range",
                vec![("metadata.version", 3)],
                Enabled,
                Some(refusal("metadata.version", 3, "7-34")),
            ),
            (
                "the first of two refused features",
                vec![("metadata.version", 33), ("share.version", 5)],
                Disabled,
                Some(refusal("metadata.version", 33, "7-30")),
            ),
            (
                "a feature this controller does not know",
                vec![("some.future.feature", 9)],
                Disabled,
                Some(refusal("some.future.feature", 9, "0")),
            ),
            (
                "a feature this controller does not know, with the flag on",
                vec![("some.future.feature", 1)],
                Enabled,
                Some(refusal("some.future.feature", 1, "0")),
            ),
            (
                "a known feature at a level it supports beside an unknown one",
                vec![("metadata.version", 30), ("some.future.feature", 2)],
                Disabled,
                Some(refusal("some.future.feature", 2, "0")),
            ),
            ("no finalized feature", vec![], Disabled, None),
        ] {
            check!(
                unsupported_feature_level(&image_at(&levels), unstable) == expected,
                "{what}"
            );
        }
    }
}
