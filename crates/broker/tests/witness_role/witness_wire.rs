//! The client-facing requests the witness tests send and the one partition
//! view they compare against.
//!
//! `CreateTopics`, the `acks=all` `Produce`, and the rack-carrying consumer
//! `Fetch` are shared by more than one test, and `PartitionView` is the
//! whole-struct shape of the `Metadata` answer that the ISR assertions compare
//! in one piece. Keeping them together puts every wire shape this suite pins
//! down in one file.

use {
    crate::{
        TOPIC,
        support::{
            client::{create_replicated_topic, value_batch},
            discovery::{metadata_first_partition, named_topic_metadata},
            fetch::fetch_topic_row,
            produce::{ProduceTimeoutMillis, SinglePartitionProduceSetup, batch_request},
        },
    },
    krabka_client_core::Client,
    krabka_protocol::{
        owned::{
            fetch_request::{FetchPartition, FetchRequest},
            produce_request::ProduceRequest,
            produce_response::ProduceResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::collections::BTreeSet,
};

/// Create `TOPIC` with one partition and rf=3, and return its id.
pub(crate) async fn create_topic(client: &Client) -> WireUuid {
    create_replicated_topic(client, TOPIC).await
}

fn produce_request(topic_id: WireUuid, n: i32) -> ProduceRequest {
    batch_request(
        value_batch().records(n).call(),
        SinglePartitionProduceSetup {
            topic: (TOPIC).into(),
            topic_id,
            timeout: ProduceTimeoutMillis(10_000),
            ..SinglePartitionProduceSetup::replicated()
        },
    )
}

/// The whole response to an `acks=all` produce of `n` records. The KIP-951
/// assertions read `node_endpoints` off it, which the partition-level
/// [`produce_error`] cannot carry.
pub(crate) async fn produce_response(
    client: &Client,
    topic_id: WireUuid,
    n: i32,
) -> ProduceResponse {
    client
        .send(produce_request(topic_id, n))
        .await
        .expect("Produce round-trip")
}

/// The partition-level error code of an `acks=all` produce of `n` records.
pub(crate) async fn produce_error(client: &Client, topic_id: WireUuid, n: i32) -> i16 {
    produce_response(client, topic_id, n).await.responses[0].partition_responses[0].error_code
}

/// A consumer `Fetch` (`replica_id` = -1) carrying `rack`.
pub(crate) fn consumer_fetch(topic_id: WireUuid, rack: &str) -> FetchRequest {
    FetchRequest {
        replica_id: -1,
        max_wait_ms: 800,
        min_bytes: 0,
        max_bytes: 10_485_760,
        session_id: 0,
        session_epoch: -1, // sessionless full fetch
        rack_id: rack.to_string(),
        topics: vec![fetch_topic_row(TOPIC, topic_id, vec![FetchPartition {
            partition: 0,
            fetch_offset: 0,
            current_leader_epoch: -1,
            partition_max_bytes: 1_048_576,
            ..Default::default()
        }])],
        ..Default::default()
    }
}

/// The metadata a Kafka admin tool renders for one partition, as one value.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PartitionView {
    pub(crate) error_code: i16,
    pub(crate) partition_index: i32,
    pub(crate) leader_id: i32,
    pub(crate) replica_nodes: Vec<i32>,
    pub(crate) isr_nodes: BTreeSet<i32>,
    pub(crate) offline_replicas: Vec<i32>,
}

pub(crate) async fn partition_view(client: &Client) -> PartitionView {
    let resp = client
        .send(named_topic_metadata(TOPIC))
        .await
        .expect("Metadata for the topic");
    let partition = metadata_first_partition(&resp, TOPIC).expect("the topic has partition 0");
    // Placement starts at a random site, as Kafka's does, so the order of the
    // replicas after the first one, the preferred leader, is not fixed.
    let mut replica_nodes = partition.replica_nodes.clone();
    if let Some((_, followers)) = replica_nodes.split_first_mut() {
        followers.sort_unstable();
    }
    PartitionView {
        error_code: partition.error_code,
        partition_index: partition.partition_index,
        leader_id: partition.leader_id,
        replica_nodes,
        isr_nodes: partition.isr_nodes.iter().copied().collect(),
        offline_replicas: partition.offline_replicas.clone(),
    }
}
