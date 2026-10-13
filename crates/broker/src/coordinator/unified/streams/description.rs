//! KIP-1331 topology descriptions: the broker side of the push that a Streams
//! client makes with `StreamsGroupTopologyDescriptionUpdate` (93).
//!
//! Kafka stores a pushed description through the plugin that
//! `group.streams.topology.description.plugin.class` names. While one is
//! configured, a heartbeat at version 1 asks one member for a push by setting
//! `TopologyDescriptionRequired`, and `StreamsGroupDescribe` v1 serves the
//! stored description. Without one, Kafka never asks, refuses a push with
//! `UNSUPPORTED_VERSION`, and describes every group as `NOT_STORED`.
//!
//! krabka cannot load a JVM class. It builds in the plugin Kafka ships,
//! `org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin`, which
//! holds the latest description of each group in memory, and it refuses any
//! other class name at startup, as Kafka refuses a class it cannot load. The
//! group's streams actor holds that description, so it is lost when the broker
//! restarts or another broker takes over the group, which is the limit Kafka
//! documents for the reference plugin.
//!
//! [`TopologyDescription`] is Kafka's `StreamsGroupTopologyDescription`, the
//! form a plugin stores, with the conversions from the push and to the
//! describe response. [`SolicitationBackoff`] is Kafka's
//! `StreamsGroupTopologyDescriptionBackoff`, which keeps the members of a
//! group from all being asked for the same description.

mod backoff;
mod model;

pub use self::{
    backoff::{Jitter, SolicitationBackoff},
    model::{GlobalStore, Node, NodeKind, Subtopology, TopologyDescription},
};

/// KIP-1331's record of what the topology description plugin holds for a
/// group: Kafka trunk's `StreamsGroup.storedDescriptionTopologyEpoch` and
/// `failedDescriptionTopologyEpoch`.
///
/// Kafka trunk persists both as tags 2 and 3 of `StreamsGroupMetadataValue`,
/// and moves them away from [`Self::NONE`] only through its plugin, so it
/// writes the tags only while `group.streams.topology.description.plugin.class`
/// is set. Kafka 4.3.1 has neither the tags nor KIP-1331. The broker writes
/// the tags under the same condition, and so writes the 4.3.1 record while no
/// plugin is configured; it reads them back in either case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_macros::FieldDefaults)]
pub struct DescriptionEpochs {
    /// The topology epoch whose description the plugin holds,
    /// [`Self::NONE`] when it holds none, or [`Self::UNCERTAIN`] when a plugin
    /// operation may not have completed.
    #[default(Self::NONE)]
    pub stored: i32,
    /// The topology epoch whose description the plugin rejected for good, or
    /// [`Self::NONE`].
    #[default(Self::NONE)]
    pub failed: i32,
}

impl DescriptionEpochs {
    /// Kafka's `STORED_TOPOLOGY_EPOCH_NONE`.
    pub const NONE: i32 = -1;
    /// Kafka's `STORED_TOPOLOGY_EPOCH_UNCERTAIN`: the plugin may or may not
    /// hold a description.
    pub const UNCERTAIN: i32 = -2;

    /// Whether the plugin holds the description of `topology_epoch`: Kafka's
    /// `isReliablyStoredTopologyEpoch` and an equal epoch.
    #[must_use]
    pub fn holds(self, topology_epoch: i32) -> bool {
        self.stored >= 0 && self.stored == topology_epoch
    }
}

/// Kafka's `GroupCoordinatorConfig.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_PLUGIN_CLASS_CONFIG`.
pub const PLUGIN_CLASS_CONFIG: &str = "group.streams.topology.description.plugin.class";

/// The class of Kafka's reference plugin, the one plugin krabka builds in.
pub const IN_MEMORY_PLUGIN_CLASS: &str =
    "org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin";

/// The topology description plugin of the broker:
/// `group.streams.topology.description.plugin.class`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TopologyDescriptionPlugin {
    /// No plugin, Kafka's default: the broker never asks for a description
    /// and stores none.
    #[default]
    None,
    /// Kafka's `InMemoryTopologyDescriptionPlugin`: the latest description of
    /// each group, held in memory.
    InMemory,
}

impl TopologyDescriptionPlugin {
    /// The plugin that a `group.streams.topology.description.plugin.class`
    /// value names.
    ///
    /// # Errors
    ///
    /// Returns a message for a class other than Kafka's in-memory plugin,
    /// which krabka cannot load. Kafka also fails to start on a class it
    /// cannot load, an empty name included.
    pub fn from_class_name(value: &str) -> Result<Self, String> {
        match value.trim() {
            IN_MEMORY_PLUGIN_CLASS => Ok(Self::InMemory),
            other => Err(format!(
                "server_properties `{PLUGIN_CLASS_CONFIG}` names `{other}`, which krabka cannot \
                 load: the only topology description plugin it builds in is \
                 `{IN_MEMORY_PLUGIN_CLASS}`"
            )),
        }
    }

    /// Whether a plugin is configured: Kafka's `isPluginConfigured`.
    #[must_use]
    pub fn is_configured(self) -> bool { self != Self::None }
}

/// The description that the in-memory plugin holds for one group, with the
/// topology epoch it was pushed at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredDescription {
    pub topology_epoch: i32,
    pub description: TopologyDescription,
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    /// The in-memory class loads, surrounding blanks and all, as Kafka's
    /// `ConfigDef` trims a class name. Every other name, an empty one
    /// included, is a class krabka cannot load.
    #[test]
    fn only_the_in_memory_class_names_a_plugin() {
        let refused = |name: &str| {
            Err(format!(
                "server_properties `group.streams.topology.description.plugin.class` names \
                 `{name}`, which krabka cannot load: the only topology description plugin it \
                 builds in is `org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin`"
            ))
        };
        let rows = [
            (
                "org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin",
                Ok(TopologyDescriptionPlugin::InMemory),
            ),
            (
                " org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin\t",
                Ok(TopologyDescriptionPlugin::InMemory),
            ),
            (
                "com.example.JdbcTopologyStore",
                refused("com.example.JdbcTopologyStore"),
            ),
            ("", refused("")),
        ];
        for (value, want) in rows {
            assert!(
                TopologyDescriptionPlugin::from_class_name(value) == want,
                "{value:?}"
            );
        }
    }
}
