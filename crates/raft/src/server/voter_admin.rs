//! The KIP-853 voter-reconfiguration handlers on the controller listener:
//! `AddRaftVoter`, `RemoveRaftVoter` and `UpdateRaftVoter`.
//!
//! The request checks, their order and their codes are shared with the broker
//! listener in [`crate::voter_requests`]. This module decodes the request, runs
//! those checks, probes an `AddRaftVoter` candidate, applies the change and
//! encodes the response.

#[cfg(test)]
use crate::voter_requests::valid_wire_listeners;
use {
    crate::{
        NodeId,
        error::RaftError,
        kraft::KraftController,
        reconfig::{AddVoter, RemoveVoter, UpdateVoter, VoterChange},
        voter_requests::{
            Refusal, VoterOperation, add_voter_refusal, candidate_kraft_version_refusal,
            candidate_unavailable_refusal, reconfiguration_refusal, remove_voter_refusal,
            update_voter_current_leader, update_voter_refusal,
        },
    },
    bytes::{Bytes, BytesMut},
};

macro_rules! voter_endpoints {
    ($listeners:expr) => {
        $listeners
            .into_iter()
            .map(|listener| krabka_metadata::VoterEndpoint {
                name: listener.name,
                host: listener.host,
                port: listener.port,
            })
    };
}

#[cfg(test)]
mod tests;

/// Asks the candidate for its `ApiVersions`, as `AddVoterHandler` does, and
/// refuses it when it cannot answer or does not support the finalized
/// `kraft.version`.
async fn probe_voter_candidate(
    request: &krabka_protocol::owned::add_raft_voter_request::AddRaftVoterRequest,
    finalized_version: u16,
    engine: &KraftController,
) -> Result<(), Refusal> {
    let endpoint = request
        .listeners
        .iter()
        .find(|listener| listener.name.eq_ignore_ascii_case("CONTROLLER"))
        .or_else(|| request.listeners.first())
        .expect("validated non-empty listeners");
    let address = format!("{}:{}", endpoint.host, endpoint.port);
    let supported = engine
        .probe_kraft_version(&address, finalized_version)
        .await
        .map_err(|error| {
            candidate_unavailable_refusal(
                request.voter_id,
                request.voter_directory_id,
                &error.to_string(),
            )
        })?;
    if supported {
        Ok(())
    } else {
        Err(candidate_kraft_version_refusal(
            request.voter_id,
            request.voter_directory_id,
            finalized_version,
        ))
    }
}

/// The voter a checked request names. The checks refused a negative id.
fn requested_voter(
    voter_id: i32,
    voter_directory_id: krabka_protocol::primitives::uuid::Uuid,
    endpoints: impl IntoIterator<Item = krabka_metadata::VoterEndpoint>,
    kraft_version: krabka_metadata::KRaftVersionRange,
) -> krabka_metadata::Voter {
    krabka_metadata::Voter {
        id: NodeId(u64::try_from(voter_id).unwrap_or_default()),
        directory_id: uuid::Uuid::from_bytes(voter_directory_id.0),
        endpoints: endpoints.into_iter().collect(),
        kraft_version,
    }
}

pub(super) fn add_voter_ack_when_committed(version: i16, request_ack: bool) -> bool {
    version == 0 || request_ack
}

pub(super) async fn add_raft_voter_response(
    version: i16,
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            add_raft_voter_request::AddRaftVoterRequest,
            add_raft_voter_response::AddRaftVoterResponse,
        },
    };

    let request = AddRaftVoterRequest::decode(&mut &body[..], version)?;
    let image = engine.current_image();
    let quorum = engine.quorum_state().await?;
    let (voter_id, directory_id) = (request.voter_id, request.voter_directory_id);
    let ack_when_committed = add_voter_ack_when_committed(version, request.ack_when_committed);
    let refusal = match add_voter_refusal(&request, &image.cluster_id().to_string(), &quorum) {
        Some(refusal) => Some(refusal),
        // `AddVoterHandler` answers from the leader's own state (a pending
        // change, the high watermark, `kraft.version`, an uncommitted voters
        // record, a duplicate id) before it sends the candidate anything, so
        // a retried or refused add never probes an unreachable candidate.
        None => match reconfiguration_refusal(
            engine
                .reconfigure(VoterChange::CheckAdd(AddVoter {
                    voter: requested_voter(
                        voter_id,
                        directory_id,
                        [],
                        krabka_metadata::KRaftVersionRange::default(),
                    ),
                    ack_when_committed,
                }))
                .await,
            VoterOperation::Add,
            voter_id,
            directory_id,
        ) {
            (0, _) => probe_voter_candidate(&request, image.kraft_version(), engine)
                .await
                .err(),
            refusal => Some(refusal),
        },
    };
    let (error_code, error_message) = if let Some(refusal) = refusal {
        refusal
    } else {
        let voter = requested_voter(
            voter_id,
            directory_id,
            voter_endpoints!(request.listeners),
            krabka_metadata::KRaftVersionRange::default(),
        );
        reconfiguration_refusal(
            engine
                .reconfigure(VoterChange::Add(AddVoter {
                    voter,
                    ack_when_committed,
                }))
                .await,
            VoterOperation::Add,
            voter_id,
            directory_id,
        )
    };
    let mut output = BytesMut::new();
    AddRaftVoterResponse {
        error_code,
        error_message,
        ..Default::default()
    }
    .encode(&mut output, version)?;
    Ok(output.freeze())
}

pub(super) async fn remove_raft_voter_response(
    version: i16,
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            remove_raft_voter_request::RemoveRaftVoterRequest,
            remove_raft_voter_response::RemoveRaftVoterResponse,
        },
    };

    let request = RemoveRaftVoterRequest::decode(&mut &body[..], version)?;
    let cluster_id = engine.current_image().cluster_id().to_string();
    let quorum = engine.quorum_state().await?;
    let (error_code, error_message) =
        if let Some(refusal) = remove_voter_refusal(&request, &cluster_id, &quorum) {
            refusal
        } else {
            reconfiguration_refusal(
                engine
                    .reconfigure(VoterChange::Remove(RemoveVoter {
                        id: NodeId(u64::try_from(request.voter_id).unwrap_or_default()),
                        directory_id: uuid::Uuid::from_bytes(request.voter_directory_id.0),
                    }))
                    .await,
                VoterOperation::Remove,
                request.voter_id,
                request.voter_directory_id,
            )
        };
    let mut output = BytesMut::new();
    RemoveRaftVoterResponse {
        error_code,
        error_message,
        ..Default::default()
    }
    .encode(&mut output, version)?;
    Ok(output.freeze())
}

pub(super) async fn update_raft_voter_response(
    version: i16,
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            update_raft_voter_request::UpdateRaftVoterRequest,
            update_raft_voter_response::UpdateRaftVoterResponse,
        },
    };

    let request = UpdateRaftVoterRequest::decode(&mut &body[..], version)?;
    let cluster_id = engine.current_image().cluster_id().to_string();
    let quorum = engine.quorum_state().await?;
    let error_code = if let Some(code) = update_voter_refusal(&request, &cluster_id, &quorum) {
        code
    } else {
        let feature = &request.k_raft_version_feature;
        let kraft_version = krabka_metadata::KRaftVersionRange {
            min: u16::try_from(feature.min_supported_version).unwrap_or_default(),
            max: u16::try_from(feature.max_supported_version).unwrap_or_default(),
        };
        let (voter_id, directory_id) = (request.voter_id, request.voter_directory_id);
        let voter = requested_voter(
            voter_id,
            directory_id,
            voter_endpoints!(request.listeners),
            kraft_version,
        );
        reconfiguration_refusal(
            engine
                .reconfigure(VoterChange::Update(UpdateVoter { voter }))
                .await,
            VoterOperation::Update,
            voter_id,
            directory_id,
        )
        .0
    };
    // Kafka's `RaftUtil.updateVoterResponse` fills the leader in every answer,
    // so read the quorum again after the change.
    let quorum = engine.quorum_state().await?;
    let mut output = BytesMut::new();
    UpdateRaftVoterResponse {
        error_code,
        current_leader: update_voter_current_leader(&quorum),
        ..Default::default()
    }
    .encode(&mut output, version)?;
    Ok(output.freeze())
}
