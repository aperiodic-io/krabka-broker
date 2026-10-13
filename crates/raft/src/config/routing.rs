//! The request-routing seams the broker crate hangs on a controller.
//!
//! `RaftShardRouter` claims KIP-595 traffic addressed to a non-metadata quorum
//! shard before metadata dispatch sees it, and `ControllerAdminRouter` carries
//! the KIP-919 Admin surface, so a request the controller listener accepts is
//! served by the broker's own handler registry rather than by a second
//! implementation of the same semantics. Both are hooks the broker installs on
//! `ControllerConfig` rather than settings an operator writes down, which is
//! why they sit apart from the configuration itself.

use {
    crate::error::RaftError,
    bytes::Bytes,
    std::{future::Future, net::SocketAddr, pin::Pin},
};

/// Optional router for KIP-595 traffic addressed to non-metadata quorum shards.
pub type ShardRouteFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<Bytes>, RaftError>> + Send + 'a>>;

/// Classifies and serves shard-addressed KIP-595 requests before metadata dispatch.
pub trait RaftShardRouter: Send + Sync {
    fn route(
        &self,
        api_key: i16,
        body: Bytes,
        principal: Option<&krabka_security::Principal>,
    ) -> ShardRouteFuture<'_>;
}

/// Whether a listener advertises and accepts what Kafka trunk serves beyond
/// the latest Kafka release.
///
/// This is Kafka's internal `unstable.api.versions.enable` broker config.
/// Kafka's `ApiKeys.toApiVersion` advertises `latestVersion(false)` unless it
/// is set, and `ApiKeys.isVersionEnabled` refuses the unstable version on
/// receive, which closes the connection. The default is
/// [`Disabled`][Self::Disabled], as it is in Kafka.
///
/// krabka vendors Kafka trunk's schemas, so while the setting is
/// [`Disabled`][Self::Disabled] a listener also answers exactly as a Kafka
/// 4.3.1 broker does: each API is capped at the version that release
/// advertises, and an api key that release does not have is neither
/// advertised nor accepted. [`Enabled`][Self::Enabled] serves what krabka
/// implements from trunk.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum UnstableApiVersions {
    /// Advertise and accept exactly Kafka 4.3.1's API versions.
    #[default]
    Disabled,
    /// Advertise and accept each API up to the highest version it decodes.
    Enabled,
}

impl From<bool> for UnstableApiVersions {
    fn from(enabled: bool) -> Self {
        if enabled {
            Self::Enabled
        } else {
            Self::Disabled
        }
    }
}

/// Whether a node supports feature levels past the latest production ones.
///
/// This is Kafka's internal `unstable.feature.versions.enable` broker config.
/// Kafka's `BrokerFeatures.defaultSupportedFeatures` and
/// `QuorumFeatures.defaultSupportedFeatureMap` cap every feature at its
/// `latestProduction` level unless it is set, and `kafka-storage format`
/// refuses an unstable `metadata.version`. In Kafka 4.3.1 the one feature that
/// has such levels is `metadata.version`, whose latest production level is
/// `4.3-IV0` (30); krabka also knows trunk's `4.4-IV0` to `4.5-IV0` (31-34).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum UnstableFeatureVersions {
    /// Support each feature up to its latest production level.
    #[default]
    Disabled,
    /// Support each feature up to the highest level krabka knows.
    Enabled,
}

impl From<bool> for UnstableFeatureVersions {
    fn from(enabled: bool) -> Self {
        if enabled {
            Self::Enabled
        } else {
            Self::Disabled
        }
    }
}

/// Kafka 4.3's `MetadataVersion.LATEST_PRODUCTION`, `4.3-IV0`: the highest
/// `metadata.version` a node supports while [`UnstableFeatureVersions`] is
/// [`Disabled`][UnstableFeatureVersions::Disabled], and the level a cluster
/// bootstraps at when no release is named.
pub const LATEST_PRODUCTION_METADATA_VERSION: i16 =
    krabka_metadata::metadata_version::CORDONED_LOG_DIRS_MIN_LEVEL;

/// The latest production level of each feature that has levels past it, as
/// Kafka's `Feature.latestProduction` gives it: what a node supports unless
/// [`UnstableFeatureVersions`] is [`Enabled`][UnstableFeatureVersions::Enabled].
/// A feature that is not listed has no such level.
///
/// In Kafka 4.3.1 that is `metadata.version`, whose trunk levels are `4.4-IV0`
/// and above, and `share.version`, whose level 2 is trunk's `SV_2`
/// (KIP-1191).
const LATEST_PRODUCTION_LEVELS: [ProductionLevel; 2] = [
    ProductionLevel {
        feature: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE,
        level: LATEST_PRODUCTION_METADATA_VERSION,
    },
    ProductionLevel {
        feature: krabka_metadata::metadata_version::SHARE_VERSION_FEATURE,
        level: 1,
    },
];

/// The latest production level of one feature.
struct ProductionLevel {
    feature: &'static str,
    level: i16,
}

/// The supported range of `feature` under `unstable`: the `krabka_metadata`
/// registry's range, which lists the levels of Kafka trunk, capped at the
/// feature's latest production level (see `LATEST_PRODUCTION_LEVELS`)
/// unless unstable feature versions are enabled. A default node therefore
/// advertises the ranges of Kafka 4.3.1.
#[must_use]
pub fn supported_feature_range(
    feature: &dyn krabka_metadata::Feature,
    unstable: UnstableFeatureVersions,
) -> (i16, i16) {
    let (min, max) = feature.supported_range();
    match LATEST_PRODUCTION_LEVELS
        .iter()
        .find(|production| production.feature == feature.name())
    {
        Some(production) if unstable == UnstableFeatureVersions::Disabled => {
            (min, max.min(production.level))
        }
        _ => (min, max),
    }
}

/// The feature ranges a node registers with the controller under `unstable`,
/// Kafka's `BrokerFeatures.createDefault(unstableFeatureVersionsEnabled)`:
/// [`krabka_metadata::supported_feature_ranges`] with each registry feature
/// capped by [`supported_feature_range`].
#[must_use]
pub fn supported_feature_ranges(
    unstable: UnstableFeatureVersions,
) -> std::collections::BTreeMap<String, (i16, i16)> {
    let mut ranges = krabka_metadata::supported_feature_ranges();
    for feature in krabka_metadata::feature_registry() {
        ranges.insert(
            feature.name().to_owned(),
            supported_feature_range(*feature, unstable),
        );
    }
    ranges
}

/// One Kafka API version range served by a controller-listener Admin router.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControllerApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    /// The highest version the listener decodes, the generated `MAX_VERSION`.
    pub max_version: i16,
    /// The highest version a Kafka 4.3.1 listener advertises by default, or
    /// `None` when that release has no such api key. It is below
    /// `max_version` for a version Kafka trunk added, or one 4.3.1 marks
    /// `latestVersionUnstable`.
    pub released_max: Option<i16>,
    pub flexible_min: i16,
}

impl ControllerApiVersion {
    /// The highest version the listener advertises and accepts under
    /// `unstable`, or `None` when it neither advertises nor accepts the api
    /// key at all.
    #[must_use]
    pub const fn enabled_max(self, unstable: UnstableApiVersions) -> Option<i16> {
        match unstable {
            UnstableApiVersions::Enabled => Some(self.max_version),
            UnstableApiVersions::Disabled => self.released_max,
        }
    }

    /// Whether `version` is one that `unstable` disables: inside the
    /// decodable range, above the enabled maximum. Kafka's
    /// `Processor.parseRequestHeader` closes the connection on such a request,
    /// and on any request for an api key it does not know.
    #[must_use]
    pub const fn is_disabled_version(self, version: i16, unstable: UnstableApiVersions) -> bool {
        let above_enabled = match self.enabled_max(unstable) {
            Some(max) => version > max,
            None => true,
        };
        above_enabled && version <= self.max_version
    }
}

/// Authenticated request handed from the controller listener to the broker's
/// existing Admin handler registry.
#[derive(Clone, Debug)]
pub struct ControllerAdminRequest {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub body: Bytes,
    pub peer: SocketAddr,
    pub principal: Option<krabka_security::Principal>,
    pub authenticated_via_token: bool,
}

/// Encoded Kafka response body plus its response-header shape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControllerAdminResponse {
    pub body: Bytes,
    pub flexible: bool,
}

pub type ControllerAdminRouteFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ControllerAdminResponse>, RaftError>> + Send + 'a>>;

/// Optional KIP-919 Admin RPC surface attached by the broker crate.
pub trait ControllerAdminRouter: Send + Sync {
    fn api_versions(&self) -> &[ControllerApiVersion];
    fn route(&self, request: ControllerAdminRequest) -> ControllerAdminRouteFuture<'_>;
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    /// A registry feature with a chosen name and range.
    struct Listed {
        name: &'static str,
        min: i16,
        max: i16,
    }

    impl krabka_metadata::Feature for Listed {
        fn name(&self) -> &'static str { self.name }

        fn supported_range(&self) -> (i16, i16) { (self.min, self.max) }

        fn default_level(&self, _bootstrap_mv: i16) -> i16 { self.min }
    }

    /// The range each node advertises: Kafka 4.3.1's by default, and the
    /// registry's, which is trunk's, under `unstable.feature.versions.enable`.
    /// `share.version` 2 is trunk's `SV_2` (KIP-1191).
    #[test]
    fn a_default_node_advertises_the_latest_production_ranges() {
        use {
            UnstableFeatureVersions::{Disabled, Enabled},
            krabka_metadata::metadata_version::{METADATA_VERSION_FEATURE, SHARE_VERSION_FEATURE},
        };

        // (feature, registry range, unstable, expected range)
        let rows = [
            (SHARE_VERSION_FEATURE, (0, 1), Disabled, (0, 1)),
            (SHARE_VERSION_FEATURE, (0, 1), Enabled, (0, 1)),
            (SHARE_VERSION_FEATURE, (0, 2), Disabled, (0, 1)),
            (SHARE_VERSION_FEATURE, (0, 2), Enabled, (0, 2)),
            (METADATA_VERSION_FEATURE, (7, 33), Disabled, (7, 30)),
            (METADATA_VERSION_FEATURE, (7, 33), Enabled, (7, 33)),
            ("group.version", (0, 1), Disabled, (0, 1)),
            ("group.version", (0, 1), Enabled, (0, 1)),
        ];
        let actual: Vec<_> = rows
            .iter()
            .map(|&(name, range, unstable, _)| {
                (
                    name,
                    unstable,
                    supported_feature_range(
                        &Listed {
                            name,
                            min: range.0,
                            max: range.1,
                        },
                        unstable,
                    ),
                )
            })
            .collect();
        let expected: Vec<_> = rows
            .iter()
            .map(|&(name, _, unstable, want)| (name, unstable, want))
            .collect();
        assert!(actual == expected);
    }

    /// The whole feature map a node registers with, as Kafka's
    /// `BrokerFeatures.createDefault` builds it: every registry feature,
    /// `krabka.version` among them, plus the registration-only KIP-1155
    /// capability.
    #[test]
    fn registration_advertises_every_feature_with_krabka_version() {
        use {
            UnstableFeatureVersions::{Disabled, Enabled},
            krabka_metadata::metadata_version::{METADATA_VERSION_MAX, METADATA_VERSION_MIN},
            std::collections::BTreeMap,
        };

        let expected = |metadata_max: i16, share_max: i16| {
            BTreeMap::from(
                [
                    ("eligible.leader.replicas.version", (0, 1)),
                    ("group.version", (0, 1)),
                    ("krabka.metadata.downgrade", (1, 1)),
                    ("krabka.version", (0, 1)),
                    ("kraft.version", (0, 1)),
                    ("metadata.version", (METADATA_VERSION_MIN, metadata_max)),
                    ("share.version", (0, share_max)),
                    ("streams.version", (0, 1)),
                    ("transaction.version", (0, 2)),
                ]
                .map(|(name, range)| (name.to_owned(), range)),
            )
        };
        let actual = [Disabled, Enabled].map(supported_feature_ranges);
        assert!(
            actual
                == [
                    expected(LATEST_PRODUCTION_METADATA_VERSION, 1),
                    expected(METADATA_VERSION_MAX, 2),
                ]
        );
    }
}
