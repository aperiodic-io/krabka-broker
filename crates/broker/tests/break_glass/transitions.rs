//! The five privileged transitions the two-person rule gates, each in the
//! shape a Kafka request makes it.
//!
//! Every driver answers the error code of the one row its request names, so a
//! case can compare a refusal against a completion without unpacking a
//! response. The two metric readers alongside them are how a case tells a
//! counted refusal apart from one the broker never saw.

use {
    crate::{
        support::offsets::{
            delete_records_partition, delete_records_request, delete_records_topic,
        },
        topics::delete_topic,
    },
    krabka_broker::{
        BrokerHandle, codes,
        metrics::{BreakGlassAction as ActionLabel, BreakGlassActionLabel},
    },
    krabka_client_core::Client,
    krabka_metadata::BreakGlassAction as GatedAction,
    krabka_protocol::owned::{
        alter_partition_reassignments_request::{
            AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
        },
        elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
        unregister_broker_request::UnregisterBrokerRequest,
    },
};

/// The one broker id a single-node cluster registers.
pub(super) const BROKER_ID: i32 = 1;

/// One privileged transition, in the shape a request makes it.
///
/// The enum exists so a table can name a transition without carrying a boxed
/// future per row. Each variant answers the error code the broker returned for
/// the one row that the request names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Transition {
    /// `ElectLeaders` with the unclean election type. Preferred election is not
    /// gated.
    UncleanElection,
    /// `UnregisterBroker` on this cluster's one broker.
    Unregister,
    /// `AlterPartitionReassignments` with a null replica list, which is the
    /// cancel. A start is not gated, and a completion is not a cancel.
    CancelReassignment,
    /// `DeleteTopics`.
    DeleteTopic,
    /// `DeleteRecords` at offset zero.
    TrimRecords,
}

impl Transition {
    /// The break-glass target a proposal must name to authorize this
    /// transition on `topic`.
    pub(super) fn target(self, topic: &str) -> String {
        match self {
            Transition::UncleanElection
            | Transition::CancelReassignment
            | Transition::TrimRecords => format!("{topic}-0"),
            Transition::Unregister => BROKER_ID.to_string(),
            Transition::DeleteTopic => topic.to_owned(),
        }
    }

    /// Send the request, and answer the error code of the row it names.
    pub(super) async fn run(self, client: &Client, topic: &str) -> i16 {
        match self {
            Transition::UncleanElection => unclean_election(client, topic).await,
            Transition::Unregister => unregister(client, BROKER_ID).await,
            Transition::CancelReassignment => cancel_reassignment(client, topic).await,
            Transition::DeleteTopic => delete_topic(client, topic).await,
            Transition::TrimRecords => trim_records(client, topic).await,
        }
    }
}

/// `ElectLeaders(UNCLEAN)` on partition 0 of `topic`.
async fn unclean_election(client: &Client, topic: &str) -> i16 {
    let response = client
        .send(ElectLeadersRequest {
            election_type: 1,
            topic_partitions: Some(vec![TopicPartitions {
                topic: topic.to_owned(),
                partitions: vec![0],
                ..TopicPartitions::default()
            }]),
            timeout_ms: 10_000,
            ..ElectLeadersRequest::default()
        })
        .await
        .expect("ElectLeaders");
    response
        .replica_election_results
        .first()
        .and_then(|result| result.partition_result.first())
        .map_or(codes::UNKNOWN_SERVER_ERROR, |row| row.error_code)
}

/// `UnregisterBroker(broker_id)`.
pub(super) async fn unregister(client: &Client, broker_id: i32) -> i16 {
    client
        .send(UnregisterBrokerRequest {
            broker_id,
            ..UnregisterBrokerRequest::default()
        })
        .await
        .expect("UnregisterBroker")
        .error_code
}

/// `AlterPartitionReassignments` with a null replica list: the cancel.
async fn cancel_reassignment(client: &Client, topic: &str) -> i16 {
    let response = client
        .send(AlterPartitionReassignmentsRequest {
            timeout_ms: 10_000,
            topics: vec![ReassignableTopic {
                name: topic.to_owned(),
                partitions: vec![ReassignablePartition {
                    partition_index: 0,
                    replicas: None,
                    ..ReassignablePartition::default()
                }],
                ..ReassignableTopic::default()
            }],
            ..AlterPartitionReassignmentsRequest::default()
        })
        .await
        .expect("AlterPartitionReassignments");
    response
        .responses
        .first()
        .and_then(|row| row.partitions.first())
        .map_or(codes::UNKNOWN_SERVER_ERROR, |row| row.error_code)
}

/// `DeleteRecords` at offset zero on partition 0 of `topic`.
async fn trim_records(client: &Client, topic: &str) -> i16 {
    let response = client
        .send(delete_records_request(
            vec![delete_records_topic(topic.to_owned(), vec![
                delete_records_partition(0, 0),
            ])],
            10_000,
        ))
        .await
        .expect("DeleteRecords");
    response
        .topics
        .first()
        .and_then(|row| row.partitions.first())
        .map_or(codes::UNKNOWN_SERVER_ERROR, |row| row.error_code)
}

/// How many gated transitions this broker refused for `action`.
pub(super) fn refusals(broker: &BrokerHandle, action: GatedAction) -> u64 {
    broker
        .metrics()
        .break_glass_refusals
        .get_or_create(&BreakGlassActionLabel {
            action: ActionLabel(action),
        })
        .get()
}

/// How many privileged transitions this broker ran with no approval at all.
pub(super) fn bypassed(broker: &BrokerHandle, action: GatedAction) -> u64 {
    broker
        .metrics()
        .break_glass_bypassed
        .get_or_create(&BreakGlassActionLabel {
            action: ActionLabel(action),
        })
        .get()
}
