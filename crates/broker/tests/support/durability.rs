//! Exact single-partition requests for replication and durability scenarios.

use {
    super::topic_id_for,
    crate::support::{
        client::connect_client,
        records::value_record,
        registered_cluster,
        topics::{TopicReplicationFactor, create_topic_request},
    },
    assert2::assert,
    bytes::Bytes,
    krabka_broker::BrokerHandle,
    krabka_protocol::records::RecordBatch,
};

#[derive(Clone, Copy)]
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
pub async fn replicated_topic_fixture(
    setup: ReplicatedTopicSetup<'_>,
) -> (
    Vec<(BrokerHandle, krabka_broker::BrokerConfig, tempfile::TempDir)>,
    String,
) {
    let cluster = registered_cluster(setup.brokers.0).await;
    let bootstrap = cluster[0].1.listen_addr.to_string();
    create_topic_on_replicas(&cluster[0].0, &bootstrap, TopicReplicasSetup {
        topic: setup.topic,
        replicas: setup.replicas,
    })
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

pub async fn create_topic_on_replicas(
    broker: &BrokerHandle,
    bootstrap: &str,
    setup: TopicReplicasSetup<'_>,
) {
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
    let pr = super::client::produce_batch(&client, batch, setup).await;
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
