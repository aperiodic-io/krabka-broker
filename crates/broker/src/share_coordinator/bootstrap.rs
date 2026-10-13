//! The shape of the `__share_group_state` internal topic (KIP-932).
//!
//! The first `FindCoordinator(SHARE)`, or the first call of the share-state
//! persister, creates the topic with its configured partition count and
//! replication factor (`crate::auto_topic_creation`), as
//! Kafka's `KafkaApis.getCoordinator` does.

use {
    crate::share_coordinator::config::ShareCoordinatorConfig,
    krabka_units::convert::ByteSizeExt as _, std::collections::BTreeMap,
};

pub const TOPIC: &str = "__share_group_state";

/// The topic configs Kafka writes when it creates `__share_group_state`
/// (`ShareCoordinatorService.shareGroupStateTopicConfigs`, as KIP-932
/// defines them).
pub(crate) fn topic_configs(config: &ShareCoordinatorConfig) -> BTreeMap<String, String> {
    use crate::config_keys::{
        CLEANUP_POLICY, COMPRESSION_TYPE, MIN_INSYNC_REPLICAS, RETENTION_MS, SEGMENT_BYTES,
    };
    BTreeMap::from([
        (CLEANUP_POLICY.to_owned(), "delete".to_owned()),
        (COMPRESSION_TYPE.to_owned(), "producer".to_owned()),
        (
            SEGMENT_BYTES.to_owned(),
            config.state_topic_segment_bytes.bytes_u64().to_string(),
        ),
        (
            MIN_INSYNC_REPLICAS.to_owned(),
            config.state_topic_min_isr.to_string(),
        ),
        (RETENTION_MS.to_owned(), "-1".to_owned()),
    ])
}
