//! `AddRaftVoter` (`api_key=80`, KIP-853). Admin RPC that promotes a
//! caught-up observer into the controller-raft voter set.
//!
//! ## ACL
//!
//! `Alter` on `Cluster("kafka-cluster")`. Deny → whole-response
//! `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
//!
//! ## Request checks and error codes
//!
//! After the ACL gate, the request runs the controller listener's own checks
//! in `krabka_raft::voter_requests`, in the order of Kafka's
//! `KafkaRaftClient.handleAddVoterRequest` and `AddVoterHandler`: a foreign
//! cluster id is `INCONSISTENT_CLUSTER_ID (104)`, a node that is not the leader
//! answers `NOT_LEADER_OR_FOLLOWER (6)`, an invalid voter key or listener set
//! is `INVALID_REQUEST (42)`, a candidate whose `kraft.version` range does not
//! cover the finalized version is `INVALID_REQUEST (42)`, and a candidate that
//! is not caught up is `REQUEST_TIMED_OUT (7)`.

#[cfg(test)]
use crate::handlers::raft_voter::add_voter_request;
use {
    crate::{
        broker::Broker,
        codes,
        handlers::{
            ApiVersion, DispatchResult, RequestContext, audit_admin_success, audit_resource,
            cluster_alter_denied,
            raft_voter::{Refusals, respond},
        },
    },
    krabka_metadata::{Voter, VoterEndpoint},
    krabka_protocol::owned::{
        add_raft_voter_request::AddRaftVoterRequest, add_raft_voter_response::AddRaftVoterResponse,
        api_versions_request::ApiVersionsRequest,
    },
    krabka_raft::{reconfig::AddVoter, voter_requests},
};

#[krabka_macros::voter_admission(
    request = AddRaftVoterRequest,
    api = 80,
    gate = cluster_alter_denied,
    refusals = Refusals::<AddRaftVoterResponse>::leader_only("add-raft-voter denied"),
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: ApiVersion,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> DispatchResult {
    let (voter_id, directory_id) = (req.voter_id, req.voter_directory_id);
    let id = u64::try_from(voter_id).unwrap_or_default();
    let voter = Voter {
        id: krabka_raft::NodeId(id),
        directory_id: uuid::Uuid::from_bytes(directory_id.0),
        endpoints: req
            .listeners
            .iter()
            .map(|l| VoterEndpoint {
                name: l.name.clone(),
                host: l.host.clone(),
                port: l.port,
            })
            .collect(),
        kraft_version: krabka_metadata::KRaftVersionRange::default(),
    };
    let add = AddVoter {
        voter,
        ack_when_committed: version == 0 || req.ack_when_committed,
    };
    let refusal =
        match voter_requests::add_voter_refusal(&req, &image.cluster_id().to_string(), &quorum) {
            Some(refusal) => Some(refusal),
            // `AddVoterHandler` answers from the leader's own state (a pending
            // change, the high watermark, `kraft.version`, an uncommitted voters
            // record, a duplicate id) before it sends the candidate anything, so
            // a retried or refused add never probes an unreachable candidate.
            None => match voter_requests::reconfiguration_refusal(
                broker.controller.check_add_voter(add.clone()).await,
                voter_requests::VoterOperation::Add,
                voter_id,
                directory_id,
            ) {
                (codes::NONE, _) => probe_candidate(broker, &req, image.kraft_version())
                    .await
                    .err(),
                refusal => Some(refusal),
            },
        };
    if let Some((error_code, error_message)) = refusal {
        return respond::<AddRaftVoterResponse>(version, error_code, error_message);
    }

    let (error_code, error_message) = voter_requests::reconfiguration_refusal(
        broker.controller.add_voter(add).await,
        voter_requests::VoterOperation::Add,
        voter_id,
        directory_id,
    );

    if error_code == codes::NONE {
        audit_admin_success(broker.audit_log.as_ref(), ctx, "AddRaftVoter", vec![
            audit_resource("RaftVoter", id.to_string()),
        ]);
    }

    respond::<AddRaftVoterResponse>(version, error_code, error_message)
}

/// Asks the candidate for its `ApiVersions` over the controller listener, as
/// `AddVoterHandler` does, and refuses it when it cannot answer or does not
/// support the finalized `kraft.version`.
async fn probe_candidate(
    broker: &Broker,
    req: &AddRaftVoterRequest,
    finalized_version: u16,
) -> Result<(), voter_requests::Refusal> {
    let unavailable = |error: String| {
        voter_requests::candidate_unavailable_refusal(req.voter_id, req.voter_directory_id, &error)
    };
    let endpoint = req
        .listeners
        .iter()
        .find(|listener| listener.name.eq_ignore_ascii_case("CONTROLLER"))
        .or_else(|| req.listeners.first())
        .expect("validated non-empty listeners");
    let server_name = broker
        .config
        .controller_server_name
        .as_deref()
        .unwrap_or(&endpoint.host);
    let connection = broker
        .inter_broker_client
        .connect_as_connection(
            &endpoint.host,
            endpoint.port,
            broker.config.controller_listener_protocol,
            server_name,
            krabka_client_core::ConnectionOptions {
                client_id: "krabka-voter-probe".into(),
                ..Default::default()
            },
        )
        .await
        .map_err(|error| unavailable(error.to_string()))?;
    let request = ApiVersionsRequest {
        client_software_name: "krabka".into(),
        client_software_version: env!("CARGO_PKG_VERSION").into(),
        ..Default::default()
    };
    let response = connection
        .send(request)
        .await
        .map_err(|error| unavailable(error.to_string()));
    connection.close();
    let response = response?;
    let supported = response
        .supported_features
        .iter()
        .find(|feature| feature.name == "kraft.version")
        .is_some_and(|feature| {
            i16::try_from(finalized_version).is_ok_and(|version| {
                feature.min_version <= version && version <= feature.max_version
            })
        });
    if !supported {
        return Err(voter_requests::candidate_kraft_version_refusal(
            req.voter_id,
            req.voter_directory_id,
            finalized_version,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use {
        crate::test_support::round_trip_response, assert2::assert,
        krabka_protocol::owned::add_raft_voter_response,
    };

    fn request(voter_id: i32) -> AddRaftVoterRequest {
        add_voter_request().voter_id(voter_id).call()
    }

    krabka_macros::wire_codec_helpers! { test_context ["admin-client"]; }
    krabka_macros::wire_test_driver! {
        broker answer { request: AddRaftVoterRequest, response: AddRaftVoterResponse, api: 80 }
    }
    use {
        super::*,
        crate::{
            handlers::{
                raft_voter::{VoterCase, check_voter_case},
                test_support::voter_cluster_id,
            },
            test_support::start_broker_with_authorizer as start_broker,
        },
    };

    /// Decode→encode round-trip at min and max versions. Guards against
    /// the response failing to encode at either end of the version range
    /// the schema declares.
    #[test]
    fn response_round_trips_at_min_and_max_versions() {
        use krabka_protocol::owned::add_raft_voter_response::AddRaftVoterResponse;
        for version in [
            add_raft_voter_response::MIN_VERSION,
            add_raft_voter_response::MAX_VERSION,
        ] {
            let resp = AddRaftVoterResponse {
                error_code: codes::NOT_LEADER_OR_FOLLOWER,
                error_message: Some("not the raft leader".into()),
                ..Default::default()
            };
            let (decoded, consumed) = round_trip_response(&resp, krabka_ids::ApiVersion(version));
            assert!(decoded.error_code == codes::NOT_LEADER_OR_FOLLOWER);
            assert!(consumed, "all bytes consumed at v{version}");
        }
    }

    #[tokio::test]
    async fn handle_denies_cluster_alter_without_calling_reconfig() {
        let version = 1;
        check_voter_case(
            VoterCase::Denied,
            80,
            version,
            |_| request(2),
            || AddRaftVoterResponse {
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some("add-raft-voter denied".into()),
                ..Default::default()
            },
        )
        .await;
    }

    #[tokio::test]
    async fn handle_rejects_negative_voter_id_before_reconfig() {
        let version = 1;
        check_voter_case(
            VoterCase::Invalid,
            80,
            version,
            |broker| {
                let mut request = request(-7);
                request.cluster_id = Some(voter_cluster_id(broker));
                request
            },
            || AddRaftVoterResponse {
                error_code: codes::INVALID_REQUEST,
                error_message: Some("Add voter request didn't include a valid voter".into()),
                ..Default::default()
            },
        )
        .await;
    }

    #[tokio::test]
    async fn handle_reports_reconfig_error_from_controller() {
        let version = 1;
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [start_broker] allow_all;
            user ["admin"];
        };
        let mut request = request(2);
        request.cluster_id = Some(voter_cluster_id(&broker));
        let resp = answer(&broker, version, &request, &ctx).await;

        assert!(
            resp == AddRaftVoterResponse {
                error_code: codes::UNSUPPORTED_VERSION,
                error_message: Some(
                    "Cluster doesn't support adding voter because the kraft.version feature \
                     is 0"
                        .into()
                ),
                ..Default::default()
            }
        );
        broker_handle.shutdown().await;
    }
}
