//! The `UpdateRaftVoter` advertisement loop.
//!
//! This is the second, independent background task of the module: it tells the
//! current leader where this controller actually listens, at startup and again
//! after every leader change. It is separate from the join loop because it
//! keeps running for the life of the broker, whereas the join loop stops as
//! soon as the node is a voter.

use krabka_protocol::owned::update_raft_voter_request::{
    KRaftVersionFeature, Listener as UpdateListener, UpdateRaftVoterRequest,
};
use krabka_units::convert::TimeExt as _;

use super::{
    AutoJoinParams,
    request::{advertised_controller_listener, select_bootstrap_server},
    rpc::send_update_voter,
};
use crate::codes;

/// Advertise this controller at startup and after each leader change. The
/// leader accepts this at both `kraft.version` levels; level zero keeps the
/// data in memory for upgrade preflight, while level one persists it.
pub(crate) async fn run_voter_updates(params: AutoJoinParams) {
    let Ok(voter_id) = i32::try_from(params.node_id.0) else {
        tracing::error!(
            node_id = params.node_id.0,
            "node_id exceeds i32; cannot update voter"
        );
        return;
    };
    let listener = advertised_controller_listener(
        params.advertised_controller.as_deref(),
        params.controller.controller_bound_addr(),
    );
    let mut last_updated = None;
    let mut next_server = 0usize;
    loop {
        let quorum = params.controller.quorum_state();
        let leader = quorum.current_leader;
        let epoch = i32::try_from(quorum.current_term).unwrap_or(i32::MAX);
        if leader.is_some() && last_updated != Some((leader, epoch)) {
            // If our advertised listener and directory ID already match the committed
            // voter record, skip sending an update RPC.
            if let Some(my_voter) = quorum.voter_nodes.get(&params.node_id)
                && voter_already_up_to_date(my_voter, params.directory_id, &listener)
            {
                last_updated = Some((leader, epoch));
                tokio::time::sleep(params.retry_backoff.to_std()).await;
                continue;
            }

            // Resolve target controller: try the known leader's endpoint first,
            // falling back to bootstrap_servers only when unmapped.
            let target_str = resolve_target_controller(
                leader,
                &quorum.voter_nodes,
                &params.bootstrap_servers,
                &mut next_server,
            );

            let Some(target) = target_str else {
                tracing::warn!(
                    node_id = params.node_id.0,
                    ?leader,
                    "no target controller endpoint or bootstrap server available for UpdateVoter"
                );
                tokio::time::sleep(params.retry_backoff.to_std()).await;
                continue;
            };
            let request = build_update_raft_voter_request(
                params.cluster_id,
                epoch,
                voter_id,
                params.directory_id,
                &listener,
            );
            match send_update_voter(
                &params.inter_broker_client,
                params.listener_protocol,
                &params.inter_broker_server_name,
                &target,
                &request,
            )
            .await
            {
                Ok(response) if response.error_code == codes::NONE => {
                    last_updated = Some((leader, epoch));
                }
                Ok(response) => tracing::debug!(
                    node_id = params.node_id.0,
                    server = %target,
                    error_code = response.error_code,
                    "UpdateVoter was not acknowledged; retrying"
                ),
                Err(error) => tracing::debug!(
                    node_id = params.node_id.0,
                    server = %target,
                    %error,
                    "UpdateVoter failed; retrying"
                ),
            }
        }
        tokio::time::sleep(params.retry_backoff.to_std()).await;
    }
}

pub(crate) fn voter_already_up_to_date(
    my_voter: &krabka_raft::Node,
    directory_id: uuid::Uuid,
    listener: &krabka_protocol::owned::add_raft_voter_request::Listener,
) -> bool {
    let matches_dir = my_voter.directory_id == directory_id;
    let matches_listener = my_voter
        .endpoints
        .iter()
        .any(|ep| ep.host == listener.host && ep.port == listener.port);
    matches_dir && matches_listener
}

pub(crate) fn resolve_target_controller(
    leader: Option<krabka_raft::NodeId>,
    voter_nodes: &std::collections::BTreeMap<krabka_raft::NodeId, krabka_raft::Node>,
    bootstrap_servers: &[String],
    next_server: &mut usize,
) -> Option<String> {
    if let Some(leader_id) = leader
        && let Some(leader_node) = voter_nodes.get(&leader_id)
        && let Some(ep) = leader_node
            .endpoints
            .iter()
            .find(|e| e.name.eq_ignore_ascii_case("CONTROLLER"))
            .or_else(|| leader_node.endpoints.first())
    {
        Some(format!("{}:{}", ep.host, ep.port))
    } else if !bootstrap_servers.is_empty() {
        let s = select_bootstrap_server(bootstrap_servers, *next_server);
        *next_server = next_server.wrapping_add(1);
        Some(s.to_string())
    } else {
        None
    }
}

pub(crate) fn build_update_raft_voter_request(
    cluster_id: Option<uuid::Uuid>,
    epoch: i32,
    voter_id: i32,
    directory_id: uuid::Uuid,
    listener: &krabka_protocol::owned::add_raft_voter_request::Listener,
) -> UpdateRaftVoterRequest {
    UpdateRaftVoterRequest {
        cluster_id: cluster_id.map(|id| id.to_string()),
        current_leader_epoch: epoch,
        voter_id,
        voter_directory_id: krabka_protocol::primitives::uuid::Uuid(*directory_id.as_bytes()),
        listeners: vec![UpdateListener {
            name: listener.name.clone(),
            host: listener.host.clone(),
            port: listener.port,
            ..Default::default()
        }],
        k_raft_version_feature: KRaftVersionFeature {
            min_supported_version: 0,
            max_supported_version: 1,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use krabka_metadata::{KRaftVersionRange, VoterEndpoint};
    use krabka_raft::{Node, NodeId};

    use super::*;
    use crate::{
        auto_join::test_support::{
            ResponseHeaderMode, ResponseWireSetup, params, voter_update_params,
        },
        test_support::FakeMetadataSource,
    };

    #[test]
    fn build_update_raft_voter_request_carries_identity_and_endpoints() {
        let cluster = uuid::Uuid::from_u128(1234);
        let dir = uuid::Uuid::from_u128(5678);
        let listener = krabka_protocol::owned::add_raft_voter_request::Listener {
            name: "CONTROLLER".to_string(),
            host: "10.0.0.1".to_string(),
            port: 9093,
            ..Default::default()
        };
        let req = build_update_raft_voter_request(Some(cluster), 5, 42, dir, &listener);
        assert2::assert!(req.cluster_id == Some(cluster.to_string()));
        assert2::assert!(req.current_leader_epoch == 5);
        assert2::assert!(req.voter_id == 42);
        assert2::assert!(
            req.voter_directory_id == krabka_protocol::primitives::uuid::Uuid(*dir.as_bytes())
        );
        assert2::assert!(req.listeners.len() == 1);
        assert2::assert!(req.listeners[0].name == "CONTROLLER");
        assert2::assert!(req.listeners[0].host == "10.0.0.1");
        assert2::assert!(req.listeners[0].port == 9093);
        assert2::assert!(req.k_raft_version_feature.min_supported_version == 0);
        assert2::assert!(req.k_raft_version_feature.max_supported_version == 1);

        let req_none = build_update_raft_voter_request(None, 5, 42, dir, &listener);
        assert2::assert!(req_none.cluster_id.is_none());
    }

    #[test]
    fn voter_already_up_to_date_requires_matching_directory_and_listener() {
        let dir = uuid::Uuid::from_u128(100);
        let other_dir = uuid::Uuid::from_u128(200);
        let node = Node {
            directory_id: dir,
            endpoints: vec![VoterEndpoint {
                name: "CONTROLLER".to_string(),
                host: "127.0.0.1".to_string(),
                port: 9093,
            }],
            kraft_version: KRaftVersionRange::default(),
        };

        let matching_listener = krabka_protocol::owned::add_raft_voter_request::Listener {
            name: "CONTROLLER".to_string(),
            host: "127.0.0.1".to_string(),
            port: 9093,
            ..Default::default()
        };
        assert2::assert!(voter_already_up_to_date(&node, dir, &matching_listener));

        // Differing directory ID
        assert2::assert!(!voter_already_up_to_date(
            &node,
            other_dir,
            &matching_listener
        ));

        // Differing host
        let wrong_host = krabka_protocol::owned::add_raft_voter_request::Listener {
            name: "CONTROLLER".to_string(),
            host: "127.0.0.2".to_string(),
            port: 9093,
            ..Default::default()
        };
        assert2::assert!(!voter_already_up_to_date(&node, dir, &wrong_host));

        // Differing port
        let wrong_port = krabka_protocol::owned::add_raft_voter_request::Listener {
            name: "CONTROLLER".to_string(),
            host: "127.0.0.1".to_string(),
            port: 9094,
            ..Default::default()
        };
        assert2::assert!(!voter_already_up_to_date(&node, dir, &wrong_port));

        // Empty endpoints
        let empty_node = Node {
            directory_id: dir,
            endpoints: vec![],
            kraft_version: KRaftVersionRange::default(),
        };
        assert2::assert!(!voter_already_up_to_date(
            &empty_node,
            dir,
            &matching_listener
        ));
    }

    #[test]
    fn resolve_target_controller_finds_leader_or_bootstrap() {
        let mut voter_nodes = std::collections::BTreeMap::new();
        voter_nodes.insert(
            NodeId(1),
            Node {
                directory_id: uuid::Uuid::from_u128(1),
                endpoints: vec![
                    VoterEndpoint {
                        name: "OTHER".to_string(),
                        host: "127.0.0.1".to_string(),
                        port: 8080,
                    },
                    VoterEndpoint {
                        name: "CONTROLLER".to_string(),
                        host: "127.0.0.1".to_string(),
                        port: 9093,
                    },
                ],
                kraft_version: KRaftVersionRange::default(),
            },
        );
        voter_nodes.insert(
            NodeId(2),
            Node {
                directory_id: uuid::Uuid::from_u128(2),
                endpoints: vec![VoterEndpoint {
                    name: "INTERNAL".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 7070,
                }],
                kraft_version: KRaftVersionRange::default(),
            },
        );

        let mut next = 0;
        let bootstraps = vec!["127.0.0.1:9092".to_string()];

        // Prefers CONTROLLER endpoint
        assert2::assert!(
            resolve_target_controller(Some(NodeId(1)), &voter_nodes, &bootstraps, &mut next)
                == Some("127.0.0.1:9093".to_string())
        );

        // Falls back to first endpoint if no CONTROLLER
        assert2::assert!(
            resolve_target_controller(Some(NodeId(2)), &voter_nodes, &bootstraps, &mut next)
                == Some("127.0.0.1:7070".to_string())
        );

        // Unknown leader falls back to bootstrap server and advances index
        assert2::assert!(
            resolve_target_controller(Some(NodeId(99)), &voter_nodes, &bootstraps, &mut next)
                == Some("127.0.0.1:9092".to_string())
        );
        assert2::assert!(next == 1);

        // Unknown leader with empty bootstrap returns None
        let empty_bootstraps: Vec<String> = vec![];
        assert2::assert!(
            resolve_target_controller(Some(NodeId(99)), &voter_nodes, &empty_bootstraps, &mut next)
                .is_none()
        );
    }

    #[tokio::test]
    async fn run_voter_updates_stops_when_node_id_overflows() {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .controller_bound_addr("127.0.0.1:9093".parse().expect("addr"))
                .build(),
        );
        let params = AutoJoinParams {
            node_id: NodeId(u64::MAX),
            directory_id: uuid::Uuid::from_u128(1),
            ..params(source, vec![])
        };

        tokio::time::timeout(Duration::from_millis(100), run_voter_updates(params))
            .await
            .expect("returns immediately on overflow");
    }

    async fn spawn_mock_update_server(
        response_code: i16,
    ) -> (std::net::SocketAddr, Arc<std::sync::atomic::AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count_clone = count.clone();

        tokio::spawn(async move {
            use krabka_protocol::owned::{
                update_raft_voter_request as req,
                update_raft_voter_response::UpdateRaftVoterResponse,
            };

            use crate::auto_join::test_support::{negotiate_versions, read_frame, write_response};
            while let Ok((mut socket, _)) = listener.accept().await {
                if negotiate_versions(
                    &mut socket,
                    &[(
                        krabka_ids::ApiKey(req::API_KEY),
                        krabka_ids::ApiVersion(req::MAX_VERSION),
                    )],
                )
                .await
                .is_ok()
                    && let Ok(frame) = read_frame(&mut socket).await
                    && frame.len() >= 8
                    && i16::from_be_bytes([frame[0], frame[1]]) == req::API_KEY
                {
                    count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let response = UpdateRaftVoterResponse {
                        error_code: response_code,
                        ..Default::default()
                    };
                    let _ = write_response(
                        &mut socket,
                        &frame,
                        &response,
                        ResponseWireSetup {
                            version: krabka_ids::ApiVersion(req::MAX_VERSION),
                            header: ResponseHeaderMode::Flexible,
                        },
                    )
                    .await;
                }
            }
        });

        (addr, count)
    }

    #[tokio::test]
    async fn run_voter_updates_advertises_to_leader_and_records_success() {
        let (addr, count) = spawn_mock_update_server(codes::NONE).await;

        let params = voter_update_params(addr, Some(NodeId(1)));

        let _ = tokio::time::timeout(Duration::from_millis(200), run_voter_updates(params)).await;

        assert2::assert!(count.load(std::sync::atomic::Ordering::Relaxed) == 1);
    }

    #[tokio::test]
    async fn run_voter_updates_retries_on_error_response() {
        let (addr, count) = spawn_mock_update_server(codes::UNKNOWN_SERVER_ERROR).await;

        let params = voter_update_params(addr, Some(NodeId(1)));

        let _ = tokio::time::timeout(Duration::from_millis(150), run_voter_updates(params)).await;

        assert2::assert!(count.load(std::sync::atomic::Ordering::Relaxed) >= 2);
    }

    #[tokio::test]
    async fn run_voter_updates_does_not_advertise_when_leader_is_none() {
        let (addr, count) = spawn_mock_update_server(codes::NONE).await;

        let params = voter_update_params(addr, None);

        let _ = tokio::time::timeout(Duration::from_millis(50), run_voter_updates(params)).await;

        assert2::assert!(count.load(std::sync::atomic::Ordering::Relaxed) == 0);
    }
}
