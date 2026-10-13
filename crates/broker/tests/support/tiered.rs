//! Ordered tiered-topic configuration shared by the tiered-storage suites.
use {
    crate::support::{
        client::{BatchProduceSetup, produce_batch},
        produce::ProduceTimeoutMillis,
        records::batch_from_records,
        topics::{
            AssignedTopicSetup, ConfiguredTopicSetup, CreateTopicRequestSetup,
            CreateTopicsTimeoutMillis, assigned_topic, creatable_topic, create_topic_request,
            create_topic_request_with_setup, topic_configs,
        },
    },
    krabka_protocol::owned::create_topics_request::CreatableTopicConfig,
};

pub fn tiered_configs(segment_bytes: Option<&str>) -> Vec<CreatableTopicConfig> {
    topic_configs(
        std::iter::once(("remote.storage.enable", "true"))
            .chain(segment_bytes.map(|value| ("internal.segment.bytes", value)))
            .chain([
                ("local.retention.bytes", "1"),
                ("retention.bytes", "-1"),
                ("retention.ms", "-1"),
            ]),
    )
}

/// Create one tiered topic with caller-ordered configs and the suite's wire timeout.
pub async fn create_configured_topic(
    client: &krabka_client_core::Client,
    topic: &str,
    configs: Vec<CreatableTopicConfig>,
) {
    let response = client
        .send(create_topic_request(
            krabka_protocol::owned::create_topics_request::CreatableTopic {
                configs,
                ..creatable_topic(ConfiguredTopicSetup {
                    name: (topic).into(),
                    ..Default::default()
                })
            },
        ))
        .await
        .expect("CreateTopics");
    assert2::assert!(
        response.topics[0].error_code == 0,
        "CreateTopics failed: {:?}",
        response.topics[0].error_message
    );
}

#[derive(krabka_macros::FieldDefaults)]
pub struct AssignedTieredTopicSetup<'a> {
    pub topic: AssignedTopicSetup,
    pub segment_bytes: Option<&'a str>,
}

/// Pin one tiered partition to the caller's ordered replica list.
pub async fn create_assigned_topic(
    client: &krabka_client_core::Client,
    setup: AssignedTieredTopicSetup<'_>,
) {
    let response = client
        .send(create_topic_request_with_setup(
            krabka_protocol::owned::create_topics_request::CreatableTopic {
                configs: tiered_configs(setup.segment_bytes),
                ..assigned_topic(setup.topic)
            },
            CreateTopicRequestSetup {
                timeout: CreateTopicsTimeoutMillis(10_000),
            },
        ))
        .await
        .expect("CreateTopics");
    assert2::assert!(
        response.topics[0].error_code == 0,
        "CreateTopics failed: {response:?}"
    );
}

/// Poll each replica's local configuration with the suite's 30s/200ms bound.
pub async fn await_tiered_replicas(
    brokers: &[&krabka_broker::BrokerHandle],
    topic: &str,
    segment_size: Option<krabka_units::ByteSize>,
    failure: &str,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let ready = |broker: &&krabka_broker::BrokerHandle| {
            broker
                .partition_log_config_for_test(topic, 0)
                .is_some_and(|config| {
                    config.remote_storage_enable
                        && segment_size.is_none_or(|size| config.segment_size == size)
                        && config.local_retention_size == Some(krabka_units::bytes(1))
                })
        };
        if brokers.iter().all(ready) {
            return;
        }
        assert2::assert!(std::time::Instant::now() <= deadline, "{failure}");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

#[derive(Clone, Copy)]
pub struct TieredRecordCount(pub usize);

#[derive(Clone, Copy, derive_more::Display)]
pub struct TieredRecordIndex(pub usize);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct TieredRecordsSetup<'a> {
    #[default("orders")]
    pub topic: &'a str,
    pub topic_id: krabka_protocol::primitives::uuid::Uuid,
    #[default(TieredRecordCount(1))]
    pub count: TieredRecordCount,
}

/// Produce one acknowledged single-record request for every caller-generated value.
pub async fn produce_records(
    client: &krabka_client_core::Client,
    value: impl Fn(TieredRecordIndex) -> bytes::Bytes,
    setup: TieredRecordsSetup<'_>,
) {
    for index in 0..setup.count.0 {
        let batch = batch_from_records(vec![krabka_protocol::records::Record {
            value: Some(value(TieredRecordIndex(index))),
            ..Default::default()
        }]);
        let response = produce_batch(client, batch, BatchProduceSetup {
            topic: setup.topic,
            topic_id: setup.topic_id,
            timeout: ProduceTimeoutMillis(10_000),
            ..Default::default()
        })
        .await;
        assert2::assert!(response.error_code == 0, "Produce failed: {response:?}");
    }
}
