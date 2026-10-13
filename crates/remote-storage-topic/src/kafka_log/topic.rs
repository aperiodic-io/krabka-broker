//! Provisioning of the internal metadata topic.
//!
//! One admin round-trip at startup reuses an existing topic, whose real
//! partition count and id then win over the configured values, or creates an
//! absent one with the configured cleanup policy, `retention.ms=-1` and the
//! configured `min.insync.replicas`. The same
//! round-trip resolves the topic `Uuid`, which the manual fetch path needs
//! because Fetch v13 and later carry `topic_id` and not the name.

use {
    crate::{error::MetadataLogError, kafka_log::config::KafkaMetadataLogConfig},
    krabka_client_admin::{
        AdminClient, AlterConfigOp, ConfigResource, CreateTopicSpec,
        IncrementalAlterConfigsOptions, TopicMutationOptions,
    },
    krabka_client_core::ConnectionOptions,
    krabka_protocol::primitives::uuid::Uuid as WireUuid,
    std::collections::BTreeMap,
    tracing::{debug, instrument, warn},
};

/// The partition count and id of the metadata topic.
pub(super) struct TopicInfo {
    pub partition_count: i32,
    pub topic_id: WireUuid,
}

/// Provision the topic if it is missing and return its [`TopicInfo`].
///
/// An existing topic's count and id win. This function re-reads a
/// freshly-created topic's id with a second metadata round-trip, because the
/// `CreateTopics` outcome does not reliably carry it.
#[instrument(skip_all, fields(topic = %cfg.topic), err)]
pub(super) async fn ensure_topic(
    cfg: &KafkaMetadataLogConfig,
) -> Result<TopicInfo, MetadataLogError> {
    let mut admin = AdminClient::connect_with_options(
        std::slice::from_ref(&cfg.bootstrap),
        ConnectionOptions {
            client_id: format!("{}-admin", cfg.client_id),
            dispatch_queue_capacity: cfg.dispatch_queue_capacity,
            frame_max: cfg.frame_max,
            security: cfg.security.clone().map(Box::new),
            ..ConnectionOptions::default()
        },
    )
    .await
    .map_err(|e| MetadataLogError::Other(format!("admin connect failed: {e}")))?;

    let topic_ref = cfg.topic.as_str();
    let meta = admin
        .metadata(&[topic_ref])
        .await
        .map_err(|e| MetadataLogError::Other(format!("metadata failed: {e}")))?;

    if let Some(entry) = meta.topics.iter().find(|t| t.name == cfg.topic)
        && entry.error.is_none()
        && entry.partition_count > 0
    {
        if cfg.compacted {
            ensure_compacted(&mut admin, &cfg.topic).await?;
        }
        debug!(
            topic = %cfg.topic,
            partition_count = entry.partition_count,
            "metadata topic already exists; reusing"
        );
        let topic_id = entry.topic_id.map_or(WireUuid::ZERO, to_wire_uuid);
        warn_if_zero_topic_id(&cfg.topic, topic_id);
        return Ok(TopicInfo {
            partition_count: entry.partition_count,
            topic_id,
        });
    }

    if !cfg.provision_topic {
        return Err(MetadataLogError::Other(format!(
            "metadata topic {} does not exist",
            cfg.topic
        )));
    }

    let spec = CreateTopicSpec {
        name: cfg.topic.clone(),
        partitions: cfg.num_partitions,
        replicas: cfg.replication,
        configs: creation_configs(cfg),
        replica_assignments: BTreeMap::new(),
    };
    let outcomes = admin
        .create_topics(
            &[spec],
            TopicMutationOptions::with_timeout(cfg.topic_create_timeout),
        )
        .await
        .map_err(|e| MetadataLogError::Other(format!("create_topics failed: {e}")))?;
    let outcome = outcomes
        .into_iter()
        .find(|o| o.name == cfg.topic)
        .ok_or_else(|| MetadataLogError::Other("create_topics returned no outcome".into()))?;
    if let Some(err) = outcome.error {
        return Err(MetadataLogError::Other(format!(
            "create_topics for {} failed: {err:?}",
            cfg.topic
        )));
    }
    debug!(
        topic = %cfg.topic,
        partition_count = cfg.num_partitions,
        "metadata topic created"
    );

    // Re-read metadata to learn the freshly-assigned topic id.
    let topic_id = if let Some(id) = outcome.topic_id {
        to_wire_uuid(id)
    } else {
        let meta = admin
            .metadata(&[topic_ref])
            .await
            .map_err(|e| MetadataLogError::Other(format!("metadata (post-create) failed: {e}")))?;
        meta.topics
            .iter()
            .find(|t| t.name == cfg.topic)
            .and_then(|t| t.topic_id)
            .map_or(WireUuid::ZERO, to_wire_uuid)
    };
    warn_if_zero_topic_id(&cfg.topic, topic_id);
    Ok(TopicInfo {
        partition_count: cfg.num_partitions,
        topic_id,
    })
}

/// Topic configs a freshly created metadata topic carries. Kafka's
/// `TopicBasedRemoteLogMetadataManager.newRemoteLogMetadataTopic` also sets
/// `min.insync.replicas` (`remote.log.metadata.topic.min.isr`, default 2), so
/// an `acks=all` event lands on that many replicas rather than on the leader
/// alone.
fn creation_configs(cfg: &KafkaMetadataLogConfig) -> BTreeMap<String, String> {
    let mut configs = BTreeMap::from([
        (
            "cleanup.policy".to_string(),
            if cfg.compacted { "compact" } else { "delete" }.to_string(),
        ),
        ("retention.ms".to_string(), "-1".to_string()),
    ]);
    if let Some(min_isr) = cfg.min_isr {
        configs.insert("min.insync.replicas".to_string(), min_isr.to_string());
    }
    configs
}

async fn ensure_compacted(admin: &mut AdminClient, topic: &str) -> Result<(), MetadataLogError> {
    let outcomes = admin
        .incremental_alter_configs(
            &BTreeMap::from([(ConfigResource::topic(topic), vec![AlterConfigOp::set(
                "cleanup.policy",
                "compact",
            )])]),
            IncrementalAlterConfigsOptions::default(),
        )
        .await
        .map_err(|error| {
            MetadataLogError::Other(format!("set cleanup.policy=compact failed: {error}"))
        })?;
    if let Some(error) = outcomes.into_values().find_map(Result::err) {
        return Err(MetadataLogError::Other(format!(
            "set cleanup.policy=compact for {topic} failed: {error:?}"
        )));
    }
    Ok(())
}

/// A zero topic id makes every Fetch v≥13 fail, because Fetch carries
/// `topic_id` and not the name. The metadata consumer then spins with no
/// progress. This function warns loudly, so an operator can diagnose the
/// misconfiguration instead of seeing a silent hang.
fn warn_if_zero_topic_id(topic: &str, topic_id: WireUuid) {
    if topic_id == WireUuid::ZERO {
        warn!(
            topic = %topic,
            "metadata topic resolved to a zero topic_id; Fetch v>=13 will fail \
             and the consumer will make no progress"
        );
    }
}

/// Convert the admin client's `uuid::Uuid` to the wire `Uuid` Fetch
/// requires.
fn to_wire_uuid(u: uuid::Uuid) -> WireUuid { WireUuid(*u.as_bytes()) }

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn creation_configs_follow_kafkas_metadata_topic() {
        let cases = [
            (
                "defaults match Kafka: delete policy, infinite retention, min.isr 2",
                KafkaMetadataLogConfig::new("127.0.0.1:9092"),
                [
                    ("cleanup.policy", "delete"),
                    ("min.insync.replicas", "2"),
                    ("retention.ms", "-1"),
                ],
            ),
            (
                "an override reaches the topic",
                KafkaMetadataLogConfig {
                    min_isr: Some(1),
                    ..KafkaMetadataLogConfig::new("127.0.0.1:9092")
                },
                [
                    ("cleanup.policy", "delete"),
                    ("min.insync.replicas", "1"),
                    ("retention.ms", "-1"),
                ],
            ),
            (
                "a compacted topic keeps min.isr",
                KafkaMetadataLogConfig {
                    compacted: true,
                    ..KafkaMetadataLogConfig::new("127.0.0.1:9092")
                },
                [
                    ("cleanup.policy", "compact"),
                    ("min.insync.replicas", "2"),
                    ("retention.ms", "-1"),
                ],
            ),
        ];
        for (name, cfg, expected) in cases {
            let expected = expected
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<_, _>>();
            assert!(creation_configs(&cfg) == expected, "case {name}");
        }
    }

    #[test]
    fn unset_min_isr_leaves_the_cluster_default() {
        let cfg = KafkaMetadataLogConfig {
            min_isr: None,
            ..KafkaMetadataLogConfig::new("127.0.0.1:9092")
        };
        assert!(!creation_configs(&cfg).contains_key("min.insync.replicas"));
    }
}
