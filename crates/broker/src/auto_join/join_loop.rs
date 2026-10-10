//! The join loop proper: retry `AddRaftVoter` against the bootstrap servers
//! until this node's own identity appears in the committed voter set.
//!
//! The loop also repairs a stale registration: if the committed voter set
//! already names this node under a different directory identity, it sends
//! `RemoveRaftVoter` for the stale entry first, then re-joins on a later
//! iteration.

use krabka_protocol::owned::remove_raft_voter_request::RemoveRaftVoterRequest;
use krabka_units::convert::TimeExt as _;

use super::{
    AutoJoinParams,
    outcome::{JoinOutcome, log_join_outcome},
    request::{advertised_controller_listener, build_add_raft_voter_request, join_target},
    rpc::{send_add_raft_voter, send_remove_raft_voter},
};

/// Drive the auto-join loop. Returns immediately (without touching the
/// network) when `auto_join` is disabled. Otherwise loops until this broker
/// appears in the committed voter set. Each request goes to the current leader,
/// or rotates across `bootstrap_servers` while no leader is known.
/// Intended to be spawned as a detached background task during `Broker::start`.
pub(crate) async fn run(params: AutoJoinParams) {
    if !params.auto_join {
        return;
    }

    let self_id = params.node_id;
    let bootstrap_servers = params.bootstrap_servers;
    if bootstrap_servers.is_empty() {
        tracing::warn!(
            node_id = self_id.0,
            "auto_join enabled but bootstrap_servers is empty; cannot discover a leader"
        );
        return;
    }

    // Self's voter identity, advertising the REAL bound controller endpoint
    // (resolved port, not the possibly-zero configured port) so the leader's
    // add_learner can dial us back.
    let bound = params.controller.controller_bound_addr();
    let Ok(voter_id) = i32::try_from(self_id.0) else {
        tracing::error!(node_id = self_id.0, "node_id exceeds i32; cannot auto-join");
        return;
    };
    let directory_id = krabka_protocol::primitives::uuid::Uuid(*params.directory_id.as_bytes());
    let listener = advertised_controller_listener(params.advertised_controller.as_deref(), bound);

    let protocol = params.listener_protocol;
    let server_name = params.inter_broker_server_name;
    let retry_backoff = params.retry_backoff;
    let Ok(voter_request_timeout_ms) = i32::try_from(params.voter_request_timeout.millis_i64())
    else {
        tracing::error!(
            timeout = ?params.voter_request_timeout,
            "auto-join voter request timeout exceeds Kafka wire limit"
        );
        return;
    };
    let client = params.inter_broker_client;
    let controller = params.controller;
    let cluster_id = params.cluster_id;

    let mut next_server = 0usize;
    loop {
        // Terminate as soon as the committed voter set includes us.
        if let Some(existing) = controller.current_image().voters().get(self_id)
            && existing.directory_id == params.directory_id
        {
            tracing::info!(node_id = self_id.0, "auto-join complete; node is a voter");
            return;
        }

        let (target, from_bootstrap) =
            join_target(&controller.quorum_state(), &bootstrap_servers, next_server);
        if from_bootstrap {
            next_server = next_server.wrapping_add(1);
        }
        let target = target.as_str();

        if let Some(existing) = controller.current_image().voters().get(self_id)
            && existing.directory_id != params.directory_id
        {
            let req = build_remove_raft_voter_request(cluster_id, voter_id, existing.directory_id);
            if let Err(error) =
                send_remove_raft_voter(&client, protocol, &server_name, target, &req).await
            {
                tracing::debug!(node_id = self_id.0, server = %target, %error, "auto-join: stale voter removal failed");
            }
            tokio::time::sleep(retry_backoff.to_std()).await;
            continue;
        }

        let req = build_add_raft_voter_request(
            cluster_id,
            voter_id,
            directory_id,
            listener.clone(),
            voter_request_timeout_ms,
        );

        match send_add_raft_voter(&client, protocol, &server_name, target, &req).await {
            Ok(resp) => {
                let _: JoinOutcome = log_join_outcome(self_id, target, &resp);
            }
            Err(e) => {
                tracing::debug!(
                    node_id = self_id.0,
                    server = %target,
                    error = %e,
                    "auto-join: dial/RPC failed; trying next bootstrap server"
                );
            }
        }

        tokio::time::sleep(retry_backoff.to_std()).await;
    }
}

pub(crate) fn build_remove_raft_voter_request(
    cluster_id: Option<uuid::Uuid>,
    voter_id: i32,
    stale_directory_id: uuid::Uuid,
) -> RemoveRaftVoterRequest {
    RemoveRaftVoterRequest {
        cluster_id: cluster_id.map(|id| id.to_string()),
        voter_id,
        voter_directory_id: krabka_protocol::primitives::uuid::Uuid(*stale_directory_id.as_bytes()),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use krabka_metadata::{
        KRaftVersionRange, MetadataImage, MetadataRecord, Voter, VoterEndpoint, VoterSet,
        VotersRecord,
    };
    use krabka_raft::NodeId;
    use krabka_units::{millis, secs};

    use super::*;
    use crate::test_support::FakeMetadataSource;

    fn image_with_voter(node_id: NodeId) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Voters(VotersRecord {
            voters: VoterSet::from_voters([Voter {
                id: node_id,
                directory_id: uuid::Uuid::from_u128(node_id.0.into()),
                endpoints: vec![VoterEndpoint {
                    name: "CONTROLLER".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 19093,
                }],
                kraft_version: KRaftVersionRange::default(),
            }]),
        }));
        image
    }

    /// `run` returns immediately when `auto_join` is disabled — no panic, no
    /// network dial. Build params with a real controller + inter-broker client
    /// but `auto_join = false`, and a deliberately bogus bootstrap server. If
    /// `run` honoured the flag it never dials; if it regressed and dialed, the
    /// loop would spin against the unreachable address and the timeout would
    /// fire (failing the test).
    fn voter_params(source: Arc<dyn crate::metadata_source::MetadataSource>) -> AutoJoinParams {
        AutoJoinParams {
            retry_backoff: millis(7),
            voter_request_timeout: secs(30),
            node_id: NodeId(7),
            directory_id: uuid::Uuid::from_u128(7),
            ..crate::auto_join::test_support::params(source, vec!["127.0.0.1:1".to_string()])
        }
    }

    #[tokio::test]
    async fn run_returns_immediately_when_auto_join_disabled() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let config = crate::BrokerConfig::for_tests(tempdir.path().to_path_buf());
        let handle = crate::Broker::start(config).await.expect("broker start");
        let broker = handle.broker_arc_for_test();

        let params = AutoJoinParams {
            auto_join: false,
            node_id: krabka_raft::NodeId(999),
            directory_id: uuid::Uuid::from_u128(1),
            inter_broker_client: broker.inter_broker_client_for_test(),
            // The fixture's bootstrap is unreachable if the disabled flag regresses.
            ..voter_params(broker.controller_for_test())
        };

        tokio::time::timeout(Duration::from_secs(2), run(params))
            .await
            .expect("run() returned immediately for auto_join=false");

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn run_with_auto_join_true_checks_current_voter_set_before_returning() {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_with_voter(NodeId(7)))
                .controller_bound_addr("127.0.0.1:19093".parse().expect("bound controller addr"))
                .build(),
        );
        let params = voter_params(source.clone());

        tokio::time::timeout(Duration::from_secs(2), run(params))
            .await
            .expect("already-voter auto join returns without dialing");

        assert2::assert!((source.controller_bound_addr_calls()) == (1));
        assert2::assert!((source.current_image_calls()) == (1));
    }

    #[test]
    fn build_remove_raft_voter_request_carries_identity() {
        let cluster = uuid::Uuid::from_u128(1234);
        let stale = uuid::Uuid::from_u128(5678);
        let req = build_remove_raft_voter_request(Some(cluster), 42, stale);
        assert2::assert!(req.cluster_id == Some(cluster.to_string()));
        assert2::assert!(req.voter_id == 42);
        assert2::assert!(
            req.voter_directory_id == krabka_protocol::primitives::uuid::Uuid(*stale.as_bytes())
        );

        let req_no_cluster = build_remove_raft_voter_request(None, 42, stale);
        assert2::assert!(req_no_cluster.cluster_id.is_none());
        assert2::assert!(req_no_cluster.voter_id == 42);
        assert2::assert!(
            req_no_cluster.voter_directory_id
                == krabka_protocol::primitives::uuid::Uuid(*stale.as_bytes())
        );
    }

    #[tokio::test]
    async fn run_attempts_stale_voter_removal_when_directory_id_differs() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");

        // The fake controller hands over the api key of the request that
        // follows ApiVersions. The test waits for it rather than for a fixed
        // window, so a slow runner cannot end the join loop before the
        // RemoveRaftVoter frame has been read.
        let (key_tx, key_rx) = tokio::sync::oneshot::channel::<i16>();
        tokio::spawn(async move {
            use crate::auto_join::test_support::{negotiate_versions, read_frame};
            if let Ok((mut socket, _)) = listener.accept().await {
                let apis = [
                    (
                        krabka_ids::ApiKey(
                            krabka_protocol::owned::remove_raft_voter_request::API_KEY,
                        ),
                        krabka_ids::ApiVersion(
                            krabka_protocol::owned::remove_raft_voter_request::MAX_VERSION,
                        ),
                    ),
                    (
                        krabka_ids::ApiKey(krabka_protocol::owned::add_raft_voter_request::API_KEY),
                        krabka_ids::ApiVersion(
                            krabka_protocol::owned::add_raft_voter_request::MAX_VERSION,
                        ),
                    ),
                ];
                if negotiate_versions(&mut socket, &apis).await.is_ok()
                    && let Ok(frame) = read_frame(&mut socket).await
                    && frame.len() >= 2
                {
                    let _ = key_tx.send(i16::from_be_bytes([frame[0], frame[1]]));
                }
            }
        });

        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_with_voter(NodeId(7)))
                .controller_bound_addr(addr)
                .build(),
        );

        let params = AutoJoinParams {
            voter_request_timeout: secs(1),
            directory_id: uuid::Uuid::from_u128(42), // differs from image_with_voter's 7
            bootstrap_servers: vec![addr.to_string()],
            ..voter_params(source.clone())
        };

        let join = tokio::spawn(run(params));
        let key = tokio::time::timeout(Duration::from_secs(10), key_rx)
            .await
            .expect("the join loop sends a request after ApiVersions within 10s")
            .expect("the fake controller reads that request");
        join.abort();
        assert2::assert!(key == krabka_protocol::owned::remove_raft_voter_request::API_KEY);
    }
}
