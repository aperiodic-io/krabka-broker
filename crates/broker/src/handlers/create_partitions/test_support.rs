//! Fixture builders shared by the tests of the `CreatePartitions` handler and
//! of its submodules: the request shapes that the tests send, and the
//! metadata records that seed a topic and a controller-mutation quota into a
//! running broker.

use krabka_metadata::{MetadataRecord, TopicRecord};
use krabka_protocol::owned::create_partitions_request::{
    CreatePartitionsAssignment, CreatePartitionsRequest, CreatePartitionsTopic,
};
use krabka_raft::NodeId;

use crate::{
    broker::BrokerHandle,
    handlers::test_support::{
        TopicPartitionCount, TopicReplicationFactor,
        seed_controller_quota as seed_controller_quota_fixture, single_replica_partition,
    },
};

pub const VERSION: i16 = 3;

pub fn assn(broker_ids: &[i32]) -> CreatePartitionsAssignment {
    CreatePartitionsAssignment {
        broker_ids: broker_ids.to_vec(),
        ..Default::default()
    }
}

#[derive(krabka_macros::FieldDefaults)]
pub struct PartitionTopicSetup<'a> {
    #[default("t")]
    pub name: &'a str,
    #[default(crate::handlers::test_support::TopicPartitionCount(3))]
    pub count: TopicPartitionCount,
    pub assignments: Option<Vec<CreatePartitionsAssignment>>,
}

pub fn topic_req(setup: PartitionTopicSetup<'_>) -> CreatePartitionsTopic {
    CreatePartitionsTopic {
        name: setup.name.into(),
        count: setup.count.0,
        assignments: setup.assignments,
        ..Default::default()
    }
}

#[derive(Clone, Copy, Default)]
pub enum PartitionMutation {
    #[default]
    Apply,
    Validate,
}

pub fn request(
    topics: Vec<CreatePartitionsTopic>,
    mutation: PartitionMutation,
) -> CreatePartitionsRequest {
    CreatePartitionsRequest {
        topics,
        timeout_ms: 5_000,
        validate_only: matches!(mutation, PartitionMutation::Validate),
        ..Default::default()
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct SeedTopicSetup<'a> {
    #[default("t")]
    pub name: &'a str,
    #[default(crate::handlers::test_support::TopicPartitionCount(2))]
    pub partitions: TopicPartitionCount,
    #[default(crate::handlers::test_support::TopicReplicationFactor(1))]
    pub rf: TopicReplicationFactor,
}

impl<'a> SeedTopicSetup<'a> {
    pub fn single_partition(name: &'a str) -> Self {
        Self {
            name,
            partitions: TopicPartitionCount(1),
            ..Default::default()
        }
    }
}

pub async fn seed_topic(handle: &BrokerHandle, setup: SeedTopicSetup<'_>) {
    let SeedTopicSetup {
        name,
        partitions,
        rf,
    } = setup;
    let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
        name: name.into(),
        topic_id: uuid::Uuid::new_v4(),
        partitions: partitions.0,
        replication_factor: rf.0,
    })];
    for partition in 0..partitions.0 {
        records.push(MetadataRecord::V1Partition(single_replica_partition(
            name,
            krabka_ids::PartitionIndex(partition),
            NodeId(handle.node_id()),
        )));
    }
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(records)
        .await
        .expect("seed topic");
}

pub async fn seed_controller_quota(handle: &BrokerHandle, rate: f64) {
    seed_controller_quota_fixture(handle, rate).await;
}

/// Independent complete wire rows expected by the handler tests.
pub fn expected_result(
    name: &str,
    error_code: i16,
    error_message: Option<String>,
) -> krabka_protocol::owned::create_partitions_response::CreatePartitionsTopicResult {
    tagged_wire!(
        krabka_protocol::owned::create_partitions_response::CreatePartitionsTopicResult {
            name: name.into(),
            error_code,
            error_message,
        }
    )
}

pub fn expected_response(
    results: Vec<krabka_protocol::owned::create_partitions_response::CreatePartitionsTopicResult>,
) -> krabka_protocol::owned::create_partitions_response::CreatePartitionsResponse {
    unthrottled_wire!(
        krabka_protocol::owned::create_partitions_response::CreatePartitionsResponse { results }
    )
}
