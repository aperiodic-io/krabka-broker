//! `UpdateRaftVoter` (`api_key=82`, KIP-853). Admin RPC that rewrites an
//! existing voter's listeners and its supported `kraft.version` range.
//!
//! ## ACL
//!
//! `ClusterAction` on `Cluster("kafka-cluster")`, matching
//! `ControllerApis.handleUpdateRaftVoter`, which calls
//! `authorizeClusterOperation(request, CLUSTER_ACTION)`. This differs from
//! `AddRaftVoter` and `RemoveRaftVoter`, which both check `Alter`: a
//! controller sends `UpdateRaftVoter` for itself to update its own endpoints
//! (KIP-853), and its principal usually holds `ClusterAction` rather than
//! `Alter`. Deny → whole-response `error_code =
//! CLUSTER_AUTHORIZATION_FAILED (31)`.
//!
//! After the ACL gate, the request runs the controller listener's own checks
//! in `krabka_raft::voter_requests`, which follow
//! `KafkaRaftClient.handleUpdateVoterRequest` and `UpdateVoterHandler`: a
//! cluster id that names another cluster is `INCONSISTENT_CLUSTER_ID (104)`, a
//! leader epoch on either side of the quorum's is `FENCED_LEADER_EPOCH (74)` or
//! `UNKNOWN_LEADER_EPOCH (75)`, a node that is not the leader answers
//! `NOT_LEADER_OR_FOLLOWER (6)`, and everything else malformed, including
//! listeners without the leader's controller listener name, is
//! `INVALID_REQUEST (42)`. Every answer after the ACL gate names the leader in
//! `CurrentLeader`.
//!
//! A request that carries no cluster id at all passes the first check, because
//! `KafkaRaftClient.hasValidClusterId` returns true for a null cluster id. The
//! add and remove paths already read it that way.

#[cfg(test)]
use crate::handlers::raft_voter::add_voter_request;
use {
    crate::{
        codes,
        handlers::{
            ErrorCodeResponse as _, cluster_action_denied,
            raft_voter::{Admitted, Refusals},
        },
    },
    krabka_metadata::{Voter, VoterEndpoint},
    krabka_protocol::owned::{
        update_raft_voter_request::UpdateRaftVoterRequest,
        update_raft_voter_response::UpdateRaftVoterResponse,
    },
    krabka_raft::{reconfig::UpdateVoter, voter_requests},
};

crate::handlers::raft_voter::handler!(broker, version, req_bytes, ctx, {
    let Admitted { req, image, quorum } = crate::handlers::raft_voter::admit!(
        UpdateRaftVoterRequest,
        (broker, version, req_bytes, ctx),
        82,
        cluster_action_denied,
        Refusals {
            denied: UpdateRaftVoterResponse::error(codes::CLUSTER_AUTHORIZATION_FAILED),
            not_leader: UpdateRaftVoterResponse::error(voter_requests::NOT_LEADER_OR_FOLLOWER),
        }
    );
    let error_code = if let Some(code) =
        voter_requests::update_voter_refusal(&req, &image.cluster_id().to_string(), &quorum)
    {
        code
    } else {
        let kraft_version = krabka_metadata::KRaftVersionRange {
            min: req
                .k_raft_version_feature
                .min_supported_version
                .max(0)
                .cast_unsigned(),
            max: req
                .k_raft_version_feature
                .max_supported_version
                .max(0)
                .cast_unsigned(),
        };
        let (voter_id, directory_id) = (req.voter_id, req.voter_directory_id);
        let voter = Voter {
            id: krabka_raft::NodeId(u64::try_from(voter_id).unwrap_or_default()),
            directory_id: uuid::Uuid::from_bytes(directory_id.0),
            endpoints: req
                .listeners
                .into_iter()
                .map(|l| VoterEndpoint {
                    name: l.name,
                    host: l.host,
                    port: l.port,
                })
                .collect(),
            kraft_version,
        };
        voter_requests::reconfiguration_refusal(
            broker.controller.update_voter(UpdateVoter { voter }).await,
            voter_requests::VoterOperation::Update,
            voter_id,
            directory_id,
        )
        .0
    };

    // Kafka's `RaftUtil.updateVoterResponse` names the leader in every answer.
    let quorum = broker.controller.quorum_snapshot().unwrap_or(quorum);
    crate::handlers::encode_response(
        &UpdateRaftVoterResponse {
            error_code,
            current_leader: voter_requests::update_voter_current_leader(&quorum),
            ..Default::default()
        },
        version,
    )
});

#[cfg(test)]
mod tests {
    use {
        crate::{
            handlers::{
                add_raft_voter::handle as add_raft_voter_handle, encode_response,
                remove_raft_voter::handle,
            },
            test_support::{
                DenyAll, decode_response as decode_other_response,
                encode_request as encode_other_request,
            },
        },
        assert2::assert,
        krabka_protocol::{
            Decode as _,
            owned::update_raft_voter_request::{KRaftVersionFeature, Listener},
            primitives::uuid::Uuid as ProtoUuid,
        },
        std::sync::Arc,
    };

    fn request(voter_id: i32) -> UpdateRaftVoterRequest {
        UpdateRaftVoterRequest {
            cluster_id: Some("cluster".into()),
            current_leader_epoch: 1,
            voter_id,
            voter_directory_id: ProtoUuid([4; 16]),
            listeners: vec![Listener {
                name: "CONTROLLER".into(),
                host: "127.0.0.1".into(),
                port: 9093,
                ..Default::default()
            }],
            k_raft_version_feature: KRaftVersionFeature {
                min_supported_version: 1,
                max_supported_version: 1,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Applies one malformation to an otherwise well-formed request.
    type Mutate = fn(&mut UpdateRaftVoterRequest);

    crate::test_support::codec_helpers!(UpdateRaftVoterRequest, UpdateRaftVoterResponse);

    crate::handlers::raft_voter::test_dispatch!(
        82,
        UpdateRaftVoterRequest,
        UpdateRaftVoterResponse
    );

    use {
        super::*,
        crate::test_support::{start_broker_with_authorizer as start_broker, test_ctx},
    };

    /// Decode and encode round-trip at the min and max versions.
    #[test]
    fn response_round_trips_at_min_and_max_versions() {
        use krabka_protocol::owned::update_raft_voter_response::{self, UpdateRaftVoterResponse};
        for version in [
            update_raft_voter_response::MIN_VERSION,
            update_raft_voter_response::MAX_VERSION,
        ] {
            let resp = UpdateRaftVoterResponse {
                error_code: codes::INVALID_REQUEST,
                ..Default::default()
            };
            let bytes = encode_response(&resp, version).expect("encode");
            let mut cur: &[u8] = &bytes;
            let decoded = UpdateRaftVoterResponse::decode(&mut cur, version).expect("decode");
            assert!(decoded.error_code == codes::INVALID_REQUEST);
            assert!(cur.is_empty(), "all bytes consumed at v{version}");
        }
    }

    #[tokio::test]
    async fn handle_denies_cluster_alter_without_calling_reconfig() {
        let version = 0;
        crate::handlers::raft_voter::check_denied_reconfiguration!(
            (broker_handle, _dir, broker, ctx, resp),
            version
        );
    }

    #[tokio::test]
    async fn handle_rejects_negative_voter_id_before_reconfig() {
        let version = 0;
        broker_fixture!(
            (broker_handle, _dir, broker),
            allow_all,
            context(ctx, "admin")
        );
        let mut request = request(-7);
        stamp_voter_request!(request, broker, leader_epoch);
        let resp = answer(&broker, version, &request, &ctx).await;

        assert!(resp.error_code == codes::INVALID_REQUEST);
        broker_handle.shutdown().await;
    }

    /// Each rejected field carries the code that
    /// `KafkaRaftClient.handleUpdateVoterRequest` carries for it. None of
    /// them is voter-specific: KIP-853 adds no "invalid voter update" code.
    #[tokio::test]
    async fn handle_reports_the_kafka_code_for_each_rejected_field() {
        let version = 0;
        broker_fixture!(
            (broker_handle, _dir, broker),
            allow_all,
            context(ctx, "admin")
        );
        let cluster_id = broker.controller.current_image().cluster_id().to_string();
        let epoch = i32::try_from(broker.controller.quorum_state().current_term)
            .expect("the test quorum's term fits an i32");
        let well_formed = || {
            let mut req = request(2);
            req.cluster_id = Some(cluster_id.clone());
            req.current_leader_epoch = epoch;
            req
        };

        let cases: [(&str, Mutate, i16); 7] = [
            (
                "another cluster's id",
                |req| req.cluster_id = Some("not-this-cluster".into()),
                codes::INCONSISTENT_CLUSTER_ID,
            ),
            (
                "an epoch the quorum has left behind",
                |req| req.current_leader_epoch -= 1,
                codes::FENCED_LEADER_EPOCH,
            ),
            (
                "an epoch ahead of the quorum's",
                |req| req.current_leader_epoch += 1,
                codes::UNKNOWN_LEADER_EPOCH,
            ),
            (
                "a zero voter directory id",
                |req| req.voter_directory_id = ProtoUuid([0; 16]),
                codes::INVALID_REQUEST,
            ),
            (
                "no listeners at all",
                |req| req.listeners.clear(),
                codes::INVALID_REQUEST,
            ),
            (
                "an inverted kraft.version range",
                |req| req.k_raft_version_feature.min_supported_version = 2,
                codes::INVALID_REQUEST,
            ),
            (
                "listeners without the leader's controller listener",
                |req| req.listeners[0].name = "PLAINTEXT".into(),
                codes::INVALID_REQUEST,
            ),
        ];
        let leader_id = i32::try_from(broker.config.node_id.0).expect("node id fits an i32");

        for (what, mutate, want) in cases {
            let mut req = well_formed();
            mutate(&mut req);
            let resp = answer(&broker, version, &req, &ctx).await;
            assert!(resp.error_code == want, "{what}");
            // Every refusal names the leader, as `RaftUtil.updateVoterResponse`
            // fills it.
            assert!(
                (
                    resp.current_leader.leader_id,
                    resp.current_leader.leader_epoch,
                ) == (leader_id, epoch),
                "{what}"
            );
        }
        broker_handle.shutdown().await;
    }

    /// `KafkaRaftClient.hasValidClusterId` answers true for a request that
    /// carries no cluster id, so an absent one is not an inconsistent one: the
    /// request runs the rest of the checks and reaches the voter set, which
    /// holds no voter 2.
    #[tokio::test]
    async fn handle_accepts_a_request_that_names_no_cluster() {
        let version = 0;
        broker_fixture!(
            (broker_handle, _dir, broker),
            allow_all,
            context(ctx, "admin")
        );
        let mut named = request(2);
        // Kafka's `UpdateVoterHandler` checks the voter's kraft.version range
        // against the cluster's before it looks the voter up, so the range
        // covers the cluster's version for the lookup to be reached.
        named.k_raft_version_feature.min_supported_version = 0;
        named.cluster_id = Some(broker.controller.current_image().cluster_id().to_string());
        named.current_leader_epoch =
            i32::try_from(broker.controller.quorum_state().current_term).unwrap_or(i32::MAX);
        let mut anonymous = named.clone();
        anonymous.cluster_id = None;

        let mut codes_seen = Vec::new();
        for req in [named, anonymous] {
            let req_bytes = encode_request(&req, version);
            let resp = super::handle(&broker, version, &req_bytes, &ctx)
                .await
                .expect("handle");
            codes_seen.push(decode_response(&resp, version).error_code);
        }

        assert!(codes_seen == vec![codes::VOTER_NOT_FOUND, codes::VOTER_NOT_FOUND]);
        broker_handle.shutdown().await;
    }

    /// `UpdateRaftVoter` checks `ClusterAction`, matching Kafka's
    /// `ControllerApis.handleUpdateRaftVoter`. `AddRaftVoter` and
    /// `RemoveRaftVoter` keep checking `Alter` (#688): a principal with only
    /// the operation each api actually needs gets past the ACL gate, and a
    /// principal with only the other one does not.
    #[tokio::test]
    async fn each_raft_voter_api_checks_its_own_cluster_operation() {
        use {
            assert2::check,
            krabka_metadata::AclOperation,
            krabka_protocol::owned::{
                add_raft_voter_response::AddRaftVoterResponse,
                remove_raft_voter_request::RemoveRaftVoterRequest,
                remove_raft_voter_response::RemoveRaftVoterResponse,
            },
        };

        /// Authorizer that allows exactly one cluster operation and denies
        /// every other one, including on other resources.
        #[derive(Debug)]
        struct GrantOnly(AclOperation);

        test_authorizer!(GrantOnly, (self, _source, req), {
            if req.operation == self.0 {
                crate::authorizer::AuthorizationResult::Allow
            } else {
                crate::authorizer::AuthorizationResult::Deny
            }
        });

        enum Api {
            Update,
            Add,
            Remove,
        }

        let version = 0;
        let cases: [(&str, Api, AclOperation, bool); 5] = [
            (
                "UpdateRaftVoter",
                Api::Update,
                AclOperation::ClusterAction,
                false,
            ),
            ("UpdateRaftVoter", Api::Update, AclOperation::Alter, true),
            ("AddRaftVoter", Api::Add, AclOperation::Alter, false),
            ("AddRaftVoter", Api::Add, AclOperation::ClusterAction, true),
            ("RemoveRaftVoter", Api::Remove, AclOperation::Alter, false),
        ];

        for (api_name, api, grant, want_cluster_authorization_failed) in cases {
            broker_fixture!(
                (broker_handle, _dir, broker),
                start_broker(Arc::new(GrantOnly(grant)))
            );
            test_ctx!(ctx, "alice");

            let error_code = match api {
                Api::Update => {
                    let req_bytes = encode_request(&request(2), version);
                    let resp = super::handle(&broker, version, &req_bytes, &ctx)
                        .await
                        .expect("handle");
                    decode_response(&resp, version).error_code
                }
                Api::Add => {
                    let req = add_voter_request().voter_id(2).call();
                    let req_bytes = encode_other_request(&req, version);
                    let resp = add_raft_voter_handle(&broker, version, &req_bytes, &ctx)
                        .await
                        .expect("handle");
                    decode_other_response::<AddRaftVoterResponse>(&resp, version).error_code
                }
                Api::Remove => {
                    let req = RemoveRaftVoterRequest {
                        cluster_id: Some("cluster".into()),
                        voter_id: 2,
                        voter_directory_id: ProtoUuid([3; 16]),
                        ..Default::default()
                    };
                    let req_bytes = encode_other_request(&req, version);
                    let resp = handle(&broker, version, &req_bytes, &ctx)
                        .await
                        .expect("handle");
                    decode_other_response::<RemoveRaftVoterResponse>(&resp, version).error_code
                }
            };

            check!(
                (error_code == codes::CLUSTER_AUTHORIZATION_FAILED)
                    == want_cluster_authorization_failed,
                "{api_name} with {grant:?} only"
            );
            broker_handle.shutdown().await;
        }
    }

    #[tokio::test]
    async fn handle_reports_reconfig_error_from_controller() {
        let version = 0;
        broker_fixture!(
            (broker_handle, _dir, broker),
            allow_all,
            context(ctx, "admin")
        );
        let mut request = request(2);
        // The range covers the cluster's kraft.version, so Kafka's handler
        // reaches the voter lookup.
        request.k_raft_version_feature.min_supported_version = 0;
        stamp_voter_request!(request, broker, leader_epoch);
        let resp = answer(&broker, version, &request, &ctx).await;

        assert!(resp.error_code == codes::VOTER_NOT_FOUND);
        broker_handle.shutdown().await;
    }
}
