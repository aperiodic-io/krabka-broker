//! The metadata records that a `CreateTopics` row commits: one `TopicRecord`,
//! one `PartitionRecord` per partition, and the `TopicConfigRecord` that
//! carries the config overrides the request asked for.

use {
    krabka_metadata::{MetadataRecord, TopicConfigRecord, TopicRecord},
    krabka_protocol::owned::create_topics_request::CreatableTopic,
    uuid::Uuid,
};

/// Kafka's `computeConfigChanges` refusal of a config with a null value: the
/// `INVALID_CONFIG` message that names every such config, in request order.
pub(super) fn null_config_error(request: &CreatableTopic) -> Option<String> {
    let nulls: Vec<&str> = request
        .configs
        .iter()
        .filter(|config| config.value.is_none())
        .map(|config| config.name.as_str())
        .collect();
    (!nulls.is_empty()).then(|| {
        format!(
            "Null value not supported for topic configs: {}",
            nulls.join(",")
        )
    })
}

/// A topic's config overrides, as `CreateTopics` carries them. The handler
/// refuses a config with a null value first ([`null_config_error`]), so every
/// config here has a value.
pub(super) fn topic_config_overrides(
    request: &CreatableTopic,
) -> std::collections::BTreeMap<String, String> {
    request
        .configs
        .iter()
        .filter_map(|config| {
            config
                .value
                .as_ref()
                .map(|value| (config.name.clone(), value.clone()))
        })
        .collect()
}

pub(super) fn topic_records(
    request: &CreatableTopic,
    topic_id: Uuid,
    assignments: &[Vec<krabka_raft::NodeId>],
    leaderships: &[super::InitialLeadership],
    overrides: &std::collections::BTreeMap<String, String>,
) -> Vec<MetadataRecord> {
    let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
        name: request.name.clone(),
        topic_id,
        partitions: i32::try_from(assignments.len()).unwrap_or(i32::MAX),
        replication_factor: assignments
            .first()
            .and_then(|replicas| i16::try_from(replicas.len()).ok())
            .unwrap_or(-1),
    })];
    // Kafka's `buildPartitionRegistration`: the ISR holds only the replicas
    // that were active, and one of them leads.
    records.extend(assignments.iter().zip(leaderships).enumerate().map(
        |(index, (replicas, leadership))| {
            leadership.partition_record(&request.name, i32::try_from(index).unwrap_or(0), replicas)
        },
    ));
    if !overrides.is_empty() {
        records.push(MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: request.name.clone(),
            overrides: overrides.clone(),
        }));
    }
    records
}
