//! `RemoveRaftVoter` (`api_key=81`, KIP-853).
//!
//! This admin RPC drops a voter from the controller-raft voter set. It refuses
//! to remove the last voter.
//!
//! ## ACL
//!
//! The handler needs `Alter` on `Cluster("kafka-cluster")`. On Deny, the whole
//! response carries `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
//!
//! After the ACL gate, the request runs the controller listener's own checks
//! in `krabka_raft::voter_requests`, in the order of Kafka's
//! `KafkaRaftClient.handleRemoveVoterRequest`: a foreign cluster id is
//! `INCONSISTENT_CLUSTER_ID (104)`, a node that is not the leader answers
//! `NOT_LEADER_OR_FOLLOWER (6)`, and an invalid voter key is
//! `INVALID_REQUEST (42)`.

use krabka_protocol::owned::{
    remove_raft_voter_request::RemoveRaftVoterRequest,
    remove_raft_voter_response::RemoveRaftVoterResponse,
};
use krabka_raft::{reconfig::RemoveVoter, voter_requests};

use crate::{codes, handlers::raft_voter::respond};

crate::handlers::raft_voter::leader_handler!(
    (broker, version, req_bytes, ctx),
    (
        RemoveRaftVoterRequest,
        RemoveRaftVoterResponse,
        81,
        "remove-raft-voter denied"
    ),
    (req, image, quorum),
    {
        if let Some((error_code, error_message)) =
            voter_requests::remove_voter_refusal(&req, &image.cluster_id().to_string(), &quorum)
        {
            return respond::<RemoveRaftVoterResponse>(version, error_code, error_message);
        }

        let id = u64::try_from(req.voter_id).unwrap_or_default();
        let (error_code, error_message) = voter_requests::reconfiguration_refusal(
            broker
                .controller
                .remove_voter(RemoveVoter {
                    id: krabka_raft::NodeId(id),
                    directory_id: uuid::Uuid::from_bytes(req.voter_directory_id.0),
                })
                .await,
            voter_requests::VoterOperation::Remove,
            req.voter_id,
            req.voter_directory_id,
        );

        if error_code == codes::NONE {
            crate::handlers::audit_admin_success(
                broker.audit_log.as_ref(),
                ctx,
                "RemoveRaftVoter",
                vec![crate::handlers::audit_resource("RaftVoter", id.to_string())],
            );
        }

        respond::<RemoveRaftVoterResponse>(version, error_code, error_message)
    }
);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_protocol::{owned::remove_raft_voter_response, primitives::uuid::Uuid as ProtoUuid};

    use crate::test_support::{DenyAll, round_trip_response};

    fn request(voter_id: i32) -> RemoveRaftVoterRequest {
        RemoveRaftVoterRequest {
            cluster_id: Some("cluster".into()),
            voter_id,
            voter_directory_id: ProtoUuid([3; 16]),
            ..Default::default()
        }
    }

    crate::handlers::raft_voter::test_dispatch!(
        81,
        RemoveRaftVoterRequest,
        RemoveRaftVoterResponse
    );

    use super::*;
    use crate::test_support::{start_broker_with_authorizer as start_broker, test_ctx};

    /// Decode and encode round trip at the minimum and maximum versions.
    #[test]
    fn response_round_trips_at_min_and_max_versions() {
        use krabka_protocol::owned::remove_raft_voter_response::RemoveRaftVoterResponse;
        for version in [
            remove_raft_voter_response::MIN_VERSION,
            remove_raft_voter_response::MAX_VERSION,
        ] {
            let resp = RemoveRaftVoterResponse {
                error_code: codes::INVALID_REQUEST,
                error_message: Some("cannot remove the last voter".into()),
                ..Default::default()
            };
            let (decoded, consumed) = round_trip_response(&resp, krabka_ids::ApiVersion(version));
            assert!(
                (
                    decoded.error_code,
                    decoded.error_message.as_deref(),
                    consumed,
                ) == (
                    codes::INVALID_REQUEST,
                    Some("cannot remove the last voter"),
                    true,
                ),
                "all bytes consumed at v{version}"
            );
        }
    }

    #[tokio::test]
    async fn handle_denies_cluster_alter_without_calling_reconfig() {
        let version = krabka_protocol::owned::remove_raft_voter_response::MAX_VERSION;
        crate::handlers::raft_voter::check_denied_reconfiguration!(
            (broker_handle, _dir, broker, ctx, resp),
            version,
            "remove-raft-voter denied"
        );
    }

    #[tokio::test]
    async fn handle_rejects_negative_voter_id_before_reconfig() {
        let version = krabka_protocol::owned::remove_raft_voter_response::MAX_VERSION;
        crate::handlers::raft_voter::check_invalid_voter!(
            (broker_handle, _dir, broker, ctx, request, resp),
            version,
            RemoveRaftVoterResponse,
            "Remove voter request didn't include a valid voter"
        );
    }
}
