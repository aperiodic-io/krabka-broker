//! Gateway-side ACL snapshot: a flat `Vec<AclEntry>` from `describe_acls`.
//!
//! The snapshot implements [`AclSource`] with EXACTLY the broker's matching
//! semantics.

use krabka_metadata::{AclEntry, PatternType, ResourceType};

use crate::{AclEntries, AclSource};

/// Immutable ACL snapshot. Each refresh rebuilds it wholesale.
#[derive(Debug, Clone, Default)]
pub struct AclCache {
    entries: Vec<AclEntry>,
    cidr_hosts_supported: bool,
}

impl AclCache {
    /// A snapshot of `entries` that compares every host as text, as Kafka
    /// 4.3.1 does. Say that the cluster reads a range from a host containing
    /// `/` with [`Self::with_cidr_hosts_supported`].
    #[must_use]
    pub fn new(entries: Vec<AclEntry>) -> Self {
        Self {
            entries,
            cidr_hosts_supported: false,
        }
    }

    /// Sets whether a stored host containing `/` is a CIDR range (KIP-1276),
    /// which it is once the broker the entries came from has a
    /// `metadata.version` of 4.4-IV1 or higher. A gateway that reads a range
    /// where that broker compares text would authorize a peer the broker
    /// denies, and the other way round.
    #[must_use]
    pub fn with_cidr_hosts_supported(mut self, supported: bool) -> Self {
        self.cidr_hosts_supported = supported;
        self
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl AclSource for AclCache {
    fn matching_acls<'a>(&'a self, rt: ResourceType, name: &'a str) -> AclEntries<'a> {
        // MUST mirror MetadataImage::matching_acls: same resource_type, and
        // (LITERAL == name) || (LITERAL == "*") || (PREFIXED && name.starts_with(resource_name)).
        Box::new(self.entries.iter().filter(move |e| {
            e.resource_type == rt
                && match e.pattern_type {
                    PatternType::Literal => e.resource_name == name || e.resource_name == "*",
                    PatternType::Prefixed => name.starts_with(e.resource_name.as_str()),
                }
        }))
    }

    fn acls_of_type(&self, rt: ResourceType) -> AclEntries<'_> {
        Box::new(self.entries.iter().filter(move |e| e.resource_type == rt))
    }

    fn cidr_hosts_supported(&self) -> bool {
        self.cidr_hosts_supported
    }
}

#[cfg(test)]
mod tests {

    use krabka_metadata::{AclOperation, MetadataImage, MetadataRecord, ResourceType};
    use uuid::Uuid;

    use super::*;
    use crate::test_support::{AclSetup, acl as entry};

    /// A stable, comparable identity for an `AclEntry`.
    ///
    /// A test can then compare the `matching_acls` output of the two sources as
    /// sets, in any order. The ACL enums derive `Debug` but not `Ord`, so this
    /// function keys on the debug rendering of the identifying fields. That
    /// rendering is a `String`, which is `Ord`.
    fn key(e: &AclEntry) -> String {
        format!(
            "{:?}|{:?}|{}|{:?}",
            e.resource_type, e.pattern_type, e.resource_name, e.operation
        )
    }

    fn sorted_keys(it: AclEntries<'_>) -> Vec<String> {
        let mut v: Vec<_> = it.map(key).collect();
        v.sort();
        v
    }

    /// Cross-validation guard for the two matching implementations.
    ///
    /// The test builds the same `AclEntry` set into BOTH a `MetadataImage`,
    /// with `apply`, and an `AclCache`. The two must give the SAME matching set
    /// for every probe. This protects against drift between the broker's image
    /// matching and the gateway cache's reimplementation.
    #[test]
    fn cache_matches_image_for_every_probe() {
        // A representative ACL set: literal exact, the literal "*" wildcard,
        // a prefixed entry, an unrelated topic, and a different resource type.
        let entries = vec![
            entry(AclSetup::default()),
            entry(AclSetup {
                op: AclOperation::Write,
                name: "*",
                ..Default::default()
            }),
            entry(AclSetup {
                pattern: PatternType::Prefixed,
                name: "team-",
                ..Default::default()
            }),
            entry(AclSetup {
                name: "bar",
                ..Default::default()
            }),
            entry(AclSetup {
                rt: ResourceType::Group,
                name: "cg-1",
                ..Default::default()
            }),
            entry(AclSetup {
                rt: ResourceType::Group,
                pattern: PatternType::Prefixed,
                name: "app-",
                ..Default::default()
            }),
        ];

        // Build the same set into a MetadataImage (broker side) ...
        let mut image = MetadataImage::new(Uuid::nil());
        for e in &entries {
            image.apply(&MetadataRecord::V1AccessControlEntry(e.clone()));
        }
        // ... and into an AclCache (gateway side).
        let cache = AclCache::new(entries.clone());

        // Probes covering every matching code path.
        let probes: &[(ResourceType, &str)] = &[
            (ResourceType::Topic, "foo"),      // literal exact hit (+ "*" wildcard)
            (ResourceType::Topic, "*"),        // querying the wildcard resource itself
            (ResourceType::Topic, "team-foo"), // prefixed hit (+ "*" wildcard)
            (ResourceType::Topic, "team-"),    // prefix boundary (starts_with itself)
            (ResourceType::Topic, "nomatch"),  // only the "*" wildcard matches
            (ResourceType::Group, "cg-1"),     // literal hit, no wildcard for Group
            (ResourceType::Group, "app-svc"),  // prefixed hit on Group
            (ResourceType::Group, "other"),    // prefixed miss, no wildcard → empty
            (ResourceType::Cluster, "kafka-cluster"), // wrong type → empty in both
        ];

        for &(rt, name) in probes {
            let from_image = sorted_keys(AclSource::matching_acls(&image, rt, name));
            let from_cache = sorted_keys(AclSource::matching_acls(&cache, rt, name));
            assert2::assert!(from_image == from_cache);
        }
    }

    /// A host containing `/` is a CIDR range only where the cluster's
    /// `metadata.version` has reached 4.4-IV1 (KIP-1276), and a snapshot of its
    /// ACLs must decide as the cluster does. Each row builds the same range
    /// ALLOW into an image at a `metadata.version` and into a cache told what
    /// that image supports, and both must give the answer of the row: text
    /// comparison below the level, where the range applies to no peer, and a
    /// range at or above it. A cache that is not told compares as text.
    #[test]
    fn a_cache_reads_a_slash_host_as_the_cluster_does() {
        use krabka_metadata::{
            FeatureLevelRecord,
            metadata_version::{CIDR_ACL_MIN_LEVEL, METADATA_VERSION_FEATURE},
        };
        use krabka_security::{AuthMethod, Principal};

        use crate::{AuthorizationRequest, AuthorizationResult, Authorizer, SimpleAclAuthorizer};

        let range_allow = AclEntry {
            host: "10.0.0.0/8".into(),
            ..entry(AclSetup::default())
        };
        let alice = Principal {
            name: "alice".into(),
            auth_method: AuthMethod::SaslPlain,
            groups: vec![],
        };
        let peer = "10.1.2.3:5000".parse().unwrap();
        let request = AuthorizationRequest {
            principal: &alice,
            host: &peer,
            resource_type: ResourceType::Topic,
            resource_name: "foo",
            operation: AclOperation::Read,
        };
        let auth = SimpleAclAuthorizer::new(std::collections::HashSet::new());
        // (label, metadata.version of the cluster, whether the cache is told)
        let cases = [
            (
                "an unfinalized version is below the level",
                None,
                false,
                AuthorizationResult::Deny,
            ),
            (
                "a level below the floor",
                Some(CIDR_ACL_MIN_LEVEL - 1),
                false,
                AuthorizationResult::Deny,
            ),
            (
                "the floor",
                Some(CIDR_ACL_MIN_LEVEL),
                true,
                AuthorizationResult::Allow,
            ),
        ];
        for (label, level, told, expected) in cases {
            let mut image = MetadataImage::new(Uuid::nil());
            if let Some(level) = level {
                image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                    name: METADATA_VERSION_FEATURE.into(),
                    level,
                }));
            }
            image.apply(&MetadataRecord::V1AccessControlEntry(range_allow.clone()));
            let cache = AclCache::new(vec![range_allow.clone()]).with_cidr_hosts_supported(told);

            assert2::assert!(
                (
                    auth.authorize(&image, &request),
                    auth.authorize(&cache, &request)
                ) == (expected, expected),
                "{label}"
            );
        }
        // Not told: text, as Kafka 4.3.1 compares.
        assert2::assert!(
            auth.authorize(&AclCache::new(vec![range_allow]), &request)
                == AuthorizationResult::Deny
        );
    }

    #[test]
    fn len_and_is_empty_track_entries() {
        let empty = AclCache::default();
        assert2::assert!(empty.is_empty());
        assert2::assert!(empty.len() == 0);

        let cache = AclCache::new(vec![entry(AclSetup::default())]);
        assert2::assert!(!cache.is_empty());
        assert2::assert!(cache.len() == 1);
    }
}
