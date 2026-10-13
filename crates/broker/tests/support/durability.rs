//! Exact single-partition requests for replication and durability scenarios.

use {
    super::topic_id_for,
    crate::support::{
        client::{BatchProduceSetup, connect_client},
        produce::{ProduceAcknowledgements, ProduceTimeoutMillis},
        records::value_record,
        registered_cluster,
        topics::{TopicReplicationFactor, create_topic_request},
    },
    assert2::assert,
    bytes::Bytes,
    krabka_broker::BrokerHandle,
    krabka_protocol::records::RecordBatch,
    std::time::{Duration, Instant},
};

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
pub struct BrokerCount(pub u64);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct ReplicatedTopicSetup<'a> {
    #[default(BrokerCount(3))]
    pub brokers: BrokerCount,
    #[default("t")]
    pub topic: &'a str,
    #[default(crate::support::topics::TopicReplicationFactor(3))]
    pub replicas: TopicReplicationFactor,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct TopicReplicasSetup<'a> {
    #[default("t")]
    pub topic: &'a str,
    #[default(crate::support::topics::TopicReplicationFactor(1))]
    pub replicas: TopicReplicationFactor,
}

/// Boot registered brokers and create a topic on the first `rf` replicas.
///
/// # Panics
/// Panics if the brokers or the replicated topic cannot be created.
#[bon::builder]
pub async fn replicated_topic_fixture(
    #[builder(into, default = BrokerCount (3))] brokers: BrokerCount,
    #[builder(default = "t")] topic: &'_ str,
    #[builder(default = crate :: support :: topics :: TopicReplicationFactor (3))]
    replicas: TopicReplicationFactor,
) -> (
    Vec<(BrokerHandle, krabka_broker::BrokerConfig, tempfile::TempDir)>,
    String,
) {
    let setup = ReplicatedTopicSetup {
        brokers,
        topic,
        replicas,
    };
    let cluster = registered_cluster(setup.brokers.0).await;
    let bootstrap = cluster[0].1.listen_addr.to_string();
    create_topic_on_replicas()
        .broker(&cluster[0].0)
        .bootstrap(&bootstrap)
        .topic(setup.topic)
        .replicas(setup.replicas)
        .call()
        .await;
    (cluster, bootstrap)
}

pub fn record_batch_with_values(values: &[&str]) -> RecordBatch {
    let mut batch = RecordBatch {
        last_offset_delta: (i32::try_from(values.len()).unwrap() - 1).max(0),
        max_timestamp: i64::try_from(values.len()).unwrap(),
        ..RecordBatch::default()
    };
    for (i, v) in values.iter().enumerate() {
        batch.records.push(value_record(
            i32::try_from(i).unwrap(),
            Some(Bytes::from(v.to_string())),
        ));
    }
    batch
}

#[bon::builder]
pub async fn create_topic_on_replicas(
    broker: &BrokerHandle,
    bootstrap: &str,
    #[builder(default = "t")] topic: &'_ str,
    #[builder(default = crate :: support :: topics :: TopicReplicationFactor (1))]
    replicas: TopicReplicationFactor,
) {
    let setup = TopicReplicasSetup { topic, replicas };
    let client = connect_client(bootstrap.to_string(), None).await;
    let replicas: Vec<i32> = (1..=i32::from(setup.replicas.0)).collect();
    let resp = client
        .send(create_topic_request(super::topic_on(setup.topic, &[
            &replicas,
        ])))
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics failed: {resp:?}"
    );
    // CreateTopics ack means the controller's quorum committed the
    // metadata record, but the supervisor's reconcile loop materializes
    // the partition locally asynchronously. Wait until it appears so
    // subsequent Produce/Fetch don't race the materialization.
    broker.wait_until_partition_present(setup.topic, 0).await;
}

pub async fn produce_batch(
    bootstrap: &str,
    batch: RecordBatch,
    mut setup: super::client::BatchProduceSetup<'_>,
) -> Result<i64, i16> {
    let client = connect_client(bootstrap.to_string(), None).await;
    setup.topic_id = topic_id_for(&client, setup.topic).await;
    let pr = super::client::produce_batch()
        .client(&client)
        .batch(batch)
        .topic(setup.topic)
        .topic_id(setup.topic_id)
        .acknowledgements(setup.acknowledgements)
        .timeout(setup.timeout)
        .call()
        .await;
    if pr.error_code == 0 {
        Ok(pr.base_offset)
    } else {
        Err(pr.error_code)
    }
}

pub async fn produce_acks(
    bootstrap: &str,
    values: &[&str],
    setup: super::client::BatchProduceSetup<'_>,
) -> Result<i64, i16> {
    produce_batch(bootstrap, record_batch_with_values(values), setup).await
}

/// Time the acks-all request that waits for ISR shrink after a follower stops.
#[bon::builder]
pub async fn produce_acks_after_isr_shrink(
    bootstrap: &str,
    topic: &str,
    values: &[&str],
    #[builder(default = ProduceTimeoutMillis(10_000))] timeout: ProduceTimeoutMillis,
) -> (i64, Duration) {
    let start = Instant::now();
    let offset = produce_acks(bootstrap, values, BatchProduceSetup {
        topic,
        acknowledgements: ProduceAcknowledgements::AllReplicas,
        timeout,
        ..Default::default()
    })
    .await
    .expect("acks-all succeeds after ISR shrink");
    (offset, start.elapsed())
}
