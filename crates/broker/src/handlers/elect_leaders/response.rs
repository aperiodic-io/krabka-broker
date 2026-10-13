//! The wire shape of an `ElectLeaders` answer: the error code and text each
//! election failure carries, and the refusal of a whole request.
//!
//! A refusal of the whole request (authorization, an unknown election type)
//! answers the way Kafka's `ElectLeadersRequest.getErrorResponse` does: the
//! top-level `error_code` (v1+) carries the refusal, and so does every
//! partition row the client named. A request with `topic_partitions = null`
//! named no row, so its refusal carries none.

use {
    crate::{codes, leader_election::ElectError},
    krabka_metadata::MetadataImage,
    krabka_protocol::owned::{
        elect_leaders_request::ElectLeadersRequest,
        elect_leaders_response::{ElectLeadersResponse, PartitionResult, ReplicaElectionResult},
    },
};

/// Kafka's default `Errors.ELECTION_NOT_NEEDED` message, which
/// `new ApiError(Errors.ELECTION_NOT_NEEDED)` puts on the row.
const ELECTION_NOT_NEEDED_MESSAGE: &str = "Leader election not needed for topic partition.";

/// Kafka's default `Errors.PREFERRED_LEADER_NOT_AVAILABLE` message.
const PREFERRED_LEADER_NOT_AVAILABLE_MESSAGE: &str = "The preferred leader was not available.";

/// Kafka's default `Errors.ELIGIBLE_LEADERS_NOT_AVAILABLE` message.
const ELIGIBLE_LEADERS_NOT_AVAILABLE_MESSAGE: &str =
    "Eligible topic partition leaders are not available.";

/// The code and message of the row for one partition whose election failed.
///
/// The messages follow `ReplicationControlManager.electLeader`: a missing
/// topic and a missing partition read differently, and every other refusal
/// carries the default text of its `Errors` constant.
pub(super) fn elect_error_to_wire(
    err: ElectError,
    image: &MetadataImage,
    topic: &str,
    partition: i32,
) -> (i16, String) {
    match err {
        ElectError::UnknownTopicOrPartition => {
            let message = if image.topic(topic).is_none() {
                format!("No such topic as {topic}")
            } else {
                format!("No such partition as {topic}-{partition}")
            };
            (codes::UNKNOWN_TOPIC_OR_PARTITION, message)
        }
        ElectError::PreferredAlreadyLeader | ElectError::ElectionNotNeeded => (
            codes::ELECTION_NOT_NEEDED,
            ELECTION_NOT_NEEDED_MESSAGE.to_owned(),
        ),
        ElectError::PreferredNotInIsr
        | ElectError::PreferredNotAlive
        | ElectError::PreferredIsWitness => (
            codes::PREFERRED_LEADER_NOT_AVAILABLE,
            PREFERRED_LEADER_NOT_AVAILABLE_MESSAGE.to_owned(),
        ),
        ElectError::NoEligibleReplica => (
            codes::ELIGIBLE_LEADERS_NOT_AVAILABLE,
            ELIGIBLE_LEADERS_NOT_AVAILABLE_MESSAGE.to_owned(),
        ),
        ElectError::EpochExhausted => (
            codes::INVALID_REQUEST,
            "partition metadata epoch is exhausted".to_owned(),
        ),
    }
}

/// The response to a request refused as a whole.
///
/// Mirrors `ElectLeadersRequest.getErrorResponse`: the top-level code is the
/// refusal, and every named partition row carries the same code and message.
/// v0 has no top-level field, so the encoder drops it and the rows alone carry
/// the refusal.
pub(super) fn whole_request_error(
    req: &ElectLeadersRequest,
    code: i16,
    msg: &str,
) -> ElectLeadersResponse {
    let results: Vec<ReplicaElectionResult> = req
        .topic_partitions
        .iter()
        .flatten()
        .map(|tp| ReplicaElectionResult {
            topic: tp.topic.clone(),
            partition_result: tp
                .partitions
                .iter()
                .map(|&p| PartitionResult {
                    partition_id: p,
                    error_code: code,
                    error_message: Some(msg.into()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    ElectLeadersResponse {
        throttle_time_ms: 0,
        error_code: code,
        replica_election_results: results,
        ..Default::default()
    }
}
