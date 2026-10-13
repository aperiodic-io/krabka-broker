//! Per-request authorization on the controller listener.
//!
//! Kafka's `ControllerApis` authorizes every request with the cluster
//! operation that its api needs, and a denial becomes the
//! `getErrorResponse` of that api with `CLUSTER_AUTHORIZATION_FAILED`. This
//! module holds that table for the apis that the controller listener answers
//! itself, and the refusal body of each one.
//!
//! The apis that the KIP-919 Admin router sends to a broker handler are not
//! in the table: each of those handlers checks its own operation. The
//! exception is `AssignReplicasToDirs`, whose broker handler takes no
//! principal, so the table holds it. `ApiVersions` needs no grant.

use {
    crate::{
        ClusterOperation,
        error::RaftError,
        kraft::transport::{PeerApi, api_key},
        wire::{
            API_KEY_DELEGATION_TOKEN_MUTATION, API_KEY_METADATA_FETCH, API_KEY_SUBMIT_CHANGE,
            KrabkaMetadataFetchResponse, KrabkaSubmitChangeResponse,
            PRIVATE_CLUSTER_AUTHORIZATION_FAILED,
        },
    },
    bytes::{Bytes, BytesMut},
    krabka_protocol::{Decode, Encode, owned},
};

/// Kafka's `CLUSTER_AUTHORIZATION_FAILED`.
const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;

/// Kafka's message for `CLUSTER_AUTHORIZATION_FAILED`, which the apis with an
/// error message field carry.
const CLUSTER_AUTHORIZATION_FAILED_MESSAGE: &str = "Cluster authorization failed.";

/// Kafka's `FetchResponse.INVALID_HIGH_WATERMARK`.
const INVALID_HIGH_WATERMARK: i64 = -1;

/// The first `Fetch` version whose error response has no partition rows.
const FETCH_TOP_LEVEL_ERROR_ONLY_VERSION: i16 = 13;

/// The cluster operation that `api_key` needs on the controller listener, or
/// `None` when the listener does not authorize it itself.
///
/// The operations are Kafka's, from `ControllerApis`: `handleFetch`,
/// `handleFetchSnapshot`, `handleVote`, `handleBeginQuorumEpoch`,
/// `handleEndQuorumEpoch`, `handleControllerRegistration`,
/// `handleUpdateRaftVoter` and `handleAssignReplicasToDirs` need
/// `CLUSTER_ACTION`. `handleAddRaftVoter`, `handleRemoveRaftVoter` and
/// `handleDescribeCluster` need `ALTER`. `handleDescribeQuorum` needs
/// `DESCRIBE`. The krabka-private metadata apis have no Kafka counterpart.
/// They read or write the metadata log for another node, so they need
/// `CLUSTER_ACTION`.
pub(super) const fn required_operation(api_key: i16) -> Option<ClusterOperation> {
    match api_key {
        peer if PeerApi::from_api_key(peer).is_some() => Some(ClusterOperation::ClusterAction),
        owned::controller_registration_request::API_KEY
        | owned::update_raft_voter_request::API_KEY
        | owned::assign_replicas_to_dirs_request::API_KEY
        | API_KEY_SUBMIT_CHANGE
        | API_KEY_METADATA_FETCH
        | API_KEY_DELEGATION_TOKEN_MUTATION => Some(ClusterOperation::ClusterAction),
        owned::add_raft_voter_request::API_KEY
        | owned::remove_raft_voter_request::API_KEY
        | owned::describe_cluster_request::API_KEY => Some(ClusterOperation::Alter),
        owned::describe_quorum_request::API_KEY => Some(ClusterOperation::Describe),
        _ => None,
    }
}

/// The refusal body of a request that [`required_operation`] names and the
/// connection principal does not hold.
///
/// Each Kafka api answers in the shape of its own `getErrorResponse`. The
/// krabka-private apis answer their own response with
/// [`PRIVATE_CLUSTER_AUTHORIZATION_FAILED`].
///
/// # Errors
///
/// Returns an error when a `Fetch` body does not decode, or when the
/// response does not encode at `version`.
pub(super) fn refusal(api_key: i16, version: i16, body: &[u8]) -> Result<Bytes, RaftError> {
    let mut out = BytesMut::new();
    let message = || Some(CLUSTER_AUTHORIZATION_FAILED_MESSAGE.to_string());
    // A Kafka response whose `getErrorResponse` sets only `ErrorCode`, and
    // `ErrorMessage` when the api has one.
    macro_rules! refuse {
        ($module:ident::$response:ident $(, $message:ident)?) => {
            owned::$module::$response {
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                $($message: message(),)?
                ..Default::default()
            }
            .encode(&mut out, version)?
        };
    }
    match api_key {
        api_key::FETCH => fetch_refusal(version, body)?.encode(&mut out, version)?,
        api_key::VOTE => refuse!(vote_response::VoteResponse),
        api_key::BEGIN_QUORUM_EPOCH => {
            refuse!(begin_quorum_epoch_response::BeginQuorumEpochResponse);
        }
        api_key::END_QUORUM_EPOCH => refuse!(end_quorum_epoch_response::EndQuorumEpochResponse),
        api_key::FETCH_SNAPSHOT => refuse!(fetch_snapshot_response::FetchSnapshotResponse),
        owned::assign_replicas_to_dirs_request::API_KEY => {
            refuse!(assign_replicas_to_dirs_response::AssignReplicasToDirsResponse);
        }
        owned::controller_registration_request::API_KEY => refuse!(
            controller_registration_response::ControllerRegistrationResponse,
            error_message
        ),
        owned::update_raft_voter_request::API_KEY => {
            refuse!(update_raft_voter_response::UpdateRaftVoterResponse);
        }
        owned::add_raft_voter_request::API_KEY => {
            refuse!(add_raft_voter_response::AddRaftVoterResponse, error_message);
        }
        owned::remove_raft_voter_request::API_KEY => refuse!(
            remove_raft_voter_response::RemoveRaftVoterResponse,
            error_message
        ),
        owned::describe_cluster_request::API_KEY => refuse!(
            describe_cluster_response::DescribeClusterResponse,
            error_message
        ),
        owned::describe_quorum_request::API_KEY => refuse!(
            describe_quorum_response::DescribeQuorumResponse,
            error_message
        ),
        API_KEY_SUBMIT_CHANGE | API_KEY_DELEGATION_TOKEN_MUTATION => {
            let mut private = Vec::new();
            KrabkaSubmitChangeResponse {
                error_code: PRIVATE_CLUSTER_AUTHORIZATION_FAILED,
                leader_hint: -1,
                result: Bytes::new(),
            }
            .encode_v0(&mut private)?;
            return Ok(Bytes::from(private));
        }
        API_KEY_METADATA_FETCH => {
            let mut private = Vec::new();
            KrabkaMetadataFetchResponse {
                error_code: PRIVATE_CLUSTER_AUTHORIZATION_FAILED,
                leader_hint: -1,
                leader_epoch: -1,
                log_start_offset: -1,
                high_watermark: -1,
                quorum_high_watermark: -1,
                snapshot_id: None,
                records: Bytes::new(),
            }
            .encode_v0(&mut private)?;
            return Ok(Bytes::from(private));
        }
        _ => {
            return Err(RaftError::Protocol(
                krabka_protocol::ProtocolError::InvalidValue(
                    "no controller-listener refusal for this api key",
                ),
            ));
        }
    }
    Ok(out.freeze())
}

/// Kafka's `FetchRequest.getErrorResponse`: the top-level error and the
/// session id of the request. Before v13 every requested partition also
/// carries the error, with an invalid high watermark and no records.
fn fetch_refusal(
    version: i16,
    body: &[u8],
) -> Result<owned::fetch_response::FetchResponse, RaftError> {
    use owned::fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData};

    let mut cursor = body;
    let request = owned::fetch_request::FetchRequest::decode(&mut cursor, version)?;
    let responses = if version < FETCH_TOP_LEVEL_ERROR_ONLY_VERSION {
        request
            .topics
            .iter()
            .map(|topic| FetchableTopicResponse {
                topic: topic.topic.clone(),
                topic_id: topic.topic_id,
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| PartitionData {
                        partition_index: partition.partition,
                        error_code: CLUSTER_AUTHORIZATION_FAILED,
                        high_watermark: INVALID_HIGH_WATERMARK,
                        ..Default::default()
                    })
                    .collect(),
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            })
            .collect()
    } else {
        Vec::new()
    };
    Ok(FetchResponse {
        error_code: CLUSTER_AUTHORIZATION_FAILED,
        session_id: request.session_id,
        responses,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests;
