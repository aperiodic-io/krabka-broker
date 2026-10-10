//! KIP-858: report which log-directory UUID hosts each local replica by
//! sending `AssignReplicasToDirs` (api key 73) to the controller leader.
//!
//! This module exports two entry points:
//!
//! - [`build_request`], a pure grouping builder that a unit test can drive
//!   with no network.
//! - [`send_assignments`], an async sender that mirrors the pattern in
//!   `isr_maintenance::send_alter_partition`.

use std::{collections::BTreeMap, sync::Arc};

use krabka_protocol::owned::assign_replicas_to_dirs_request::{
    AssignReplicasToDirsRequest, DirectoryData, PartitionData, TopicData,
};

/// Kafka sentinel for "broker epoch unknown" in `AssignReplicasToDirs`, sent
/// only when this broker's own epoch is not yet known (for example, before
/// registration has landed in the image). `check_broker_epoch` treats it as
/// "skip the fence", the same KIP-903 convention `AlterPartition`'s ISR
/// eligibility check uses.
pub(crate) const UNKNOWN_BROKER_EPOCH: i64 = -1;

/// Groups flat `(topic_id, partition, dir_uuid)` assignments into the nested
/// `AssignReplicasToDirs` wire shape, which is `directories[]`, then
/// `topics[]`, then `partitions[]`.
///
/// The grouping is deterministic, because it uses a `BTreeMap` keyed by the
/// 16-byte UUID representation. The request is therefore stable across calls,
/// which matters for unit tests.
///
/// `broker_epoch` should be this broker's own current epoch, read from the
/// image at the call site (`image.broker_epoch(NodeId(broker_id))`), so the
/// controller's `check_broker_epoch` fence actually has something to check
/// against a stale or restarted broker. Pass `UNKNOWN_BROKER_EPOCH` only when
/// that lookup comes back empty.
pub(crate) fn build_request(
    broker_id: i32,
    broker_epoch: i64,
    assignments: &[(uuid::Uuid, i32, uuid::Uuid)], // (topic_id, partition, dir_uuid)
) -> AssignReplicasToDirsRequest {
    // dir_uuid → topic_id → [partition_index]
    let mut by_dir: BTreeMap<[u8; 16], BTreeMap<[u8; 16], Vec<i32>>> = BTreeMap::new();

    for (topic_id, partition, dir_uuid) in assignments {
        by_dir
            .entry(*dir_uuid.as_bytes())
            .or_default()
            .entry(*topic_id.as_bytes())
            .or_default()
            .push(*partition);
    }

    let directories: Vec<DirectoryData> = by_dir
        .into_iter()
        .map(|(dir_bytes, topics_map)| {
            let topics: Vec<TopicData> = topics_map
                .into_iter()
                .map(|(topic_bytes, mut partitions)| {
                    partitions.sort_unstable();
                    TopicData {
                        topic_id: krabka_protocol::primitives::uuid::Uuid(topic_bytes),
                        partitions: partitions
                            .into_iter()
                            .map(|p| PartitionData {
                                partition_index: p,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }
                })
                .collect();
            DirectoryData {
                id: krabka_protocol::primitives::uuid::Uuid(dir_bytes),
                topics,
                ..Default::default()
            }
        })
        .collect();

    AssignReplicasToDirsRequest {
        broker_id,
        broker_epoch,
        directories,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    }
}

/// Sends an `AssignReplicasToDirs` report to the controller leader.
///
/// KIP-919 carries api key 73 on the controller's CONTROLLER listener, so the
/// report is addressed the same way `BrokerHeartbeat` is: resolve the leader's
/// controller endpoint with [`crate::controller_endpoint::leader_endpoint`],
/// then dial it through `dialer`, which runs whatever TLS and SASL that
/// listener is configured for.
///
/// Neither half is optional. A controller-only node never self-registers as a
/// broker, so `image.broker(leader)` is empty for it and a broker-only node
/// would give up before sending anything. A bare
/// `krabka_client_core::Client` would send plaintext Kafka frames at a
/// controller listener configured for SSL, `SASL_SSL`, or `SASL_PLAINTEXT`
/// and fail every reconcile.
///
/// It returns `Err` in these cases:
/// - the image holds no controller leader
/// - neither the voter set nor the configured quorum names the leader's
///   controller endpoint
/// - the connection failed
/// - the send or the receive failed
/// - the response carries a non-zero `error_code`
pub(crate) async fn send_assignments(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    dialer: &crate::controller_endpoint::ControllerDialer,
    client_id: &str,
    req: AssignReplicasToDirsRequest,
) -> Result<(), String> {
    let leader_id = *controller.watch_leader().borrow();
    let Some(leader_id) = leader_id else {
        return Err("no controller leader".into());
    };
    let image = controller.current_image();
    let Some((host, port)) =
        crate::controller_endpoint::leader_endpoint(&image, &dialer.quorum_voters, leader_id)
    else {
        return Err("controller leader has no known controller endpoint".into());
    };

    let connection = dialer
        .outbound_client
        .connect_as_connection(
            &host,
            port,
            dialer.listener_protocol,
            &dialer.server_name,
            assign_dirs_connection_options(client_id),
        )
        .await
        .map_err(|e| format!("connect: {e}"))?;

    let resp = connection
        .send(req)
        .await
        .map_err(|e| format!("send: {e}"))?;
    connection.close();
    let current_leader = *controller.watch_leader().borrow();
    validate_assign_response(resp.error_code, leader_id, current_leader)
}

pub(crate) fn assign_dirs_connection_options(
    client_id: &str,
) -> krabka_client_core::ConnectionOptions {
    krabka_client_core::ConnectionOptions {
        client_id: client_id.to_string(),
        ..Default::default()
    }
}

fn validate_assign_response(
    error_code: i16,
    sent_controller: krabka_raft::NodeId,
    current_controller: Option<krabka_raft::NodeId>,
) -> Result<(), String> {
    match krabka_verified::directory_response_decision(
        error_code == 0,
        sent_controller.0,
        current_controller.map(|node| node.0),
    ) {
        krabka_verified::DirectoryResponseDecision::ControllerError => Err(format!(
            "AssignReplicasToDirs rejected by controller: error_code={error_code}"
        )),
        krabka_verified::DirectoryResponseDecision::StaleController => {
            Err("controller leader changed while assignment report was in flight".into())
        }
        krabka_verified::DirectoryResponseDecision::Accept => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use assert2::{assert, check};
    use krabka_metadata::MetadataImage;
    use krabka_protocol::{Decode, Encode};
    use krabka_raft::NodeId;
    use uuid::Uuid;

    use super::*;
    use crate::test_support::FakeMetadataSource;

    #[test]
    fn build_request_groups_correctly() {
        // Assignments:
        //   (tA, 0, dX)  →  dir dX, topic tA, partition 0
        //   (tA, 1, dX)  →  dir dX, topic tA, partition 1
        //   (tB, 0, dY)  →  dir dY, topic tB, partition 0
        let ta = Uuid::from_u128(0xAAAA);
        let tb = Uuid::from_u128(0xBBBB);
        let dx = Uuid::from_u128(0xDDDD);
        let dy = Uuid::from_u128(0xEEEE);

        let assignments = [(ta, 0i32, dx), (ta, 1i32, dx), (tb, 0i32, dy)];

        let req = build_request(7, 41, &assignments);

        // broker_id, epoch, and two directories.
        check!(req.broker_id == 7);
        check!(req.broker_epoch == 41);
        check!(req.directories.len() == 2);

        // Find dir dX and dY by their UUID bytes.
        let dir_x = req
            .directories
            .iter()
            .find(|d| d.id.0 == *dx.as_bytes())
            .expect("dir dX missing");
        let dir_y = req
            .directories
            .iter()
            .find(|d| d.id.0 == *dy.as_bytes())
            .expect("dir dY missing");

        // dX should have exactly one topic (tA) with two partitions [0, 1].
        assert!(dir_x.topics.len() == 1);
        let topic_a = dir_x
            .topics
            .iter()
            .find(|t| t.topic_id.0 == *ta.as_bytes())
            .expect("topic tA in dX missing");
        let mut part_indices: Vec<i32> = topic_a
            .partitions
            .iter()
            .map(|p| p.partition_index)
            .collect();
        part_indices.sort_unstable();
        assert!(part_indices == vec![0, 1]);

        // dY should have exactly one topic (tB) with one partition [0].
        assert!(dir_y.topics.len() == 1);
        let topic_b = dir_y
            .topics
            .iter()
            .find(|t| t.topic_id.0 == *tb.as_bytes())
            .expect("topic tB in dY missing");
        assert!(topic_b.partitions.len() == 1);
        assert!(topic_b.partitions[0].partition_index == 0);
    }

    #[test]
    fn build_request_empty_assignments() {
        let req = build_request(1, UNKNOWN_BROKER_EPOCH, &[]);
        check!(req.broker_id == 1);
        check!(req.broker_epoch == -1);
        check!(req.directories.is_empty());
    }

    #[test]
    fn build_request_round_trips_known_and_unknown_broker_epochs() {
        for epoch in [9, UNKNOWN_BROKER_EPOCH] {
            let req = build_request(3, epoch, &[]);
            let mut bytes = bytes::BytesMut::new();
            req.encode(&mut bytes, 0).expect("encode request");
            let decoded = AssignReplicasToDirsRequest::decode(&mut bytes.freeze(), 0)
                .expect("decode request");
            assert!(decoded.broker_id == 3);
            assert!(decoded.broker_epoch == epoch, "{epoch}");
        }
    }

    use crate::test_support::plaintext_controller_dialer_with_voters as plaintext_dialer;

    /// What a broker-only node sees of a controller-only leader: a voter with a
    /// CONTROLLER endpoint, and no broker registration anywhere in the image.
    /// `register_broker` skips a node whose `process.roles` exclude `broker`,
    /// so `image.broker(leader)` stays empty however long the cluster runs.
    fn voter_only_image(leader: NodeId, controller_addr: SocketAddr) -> MetadataImage {
        crate::test_support::voter_image(crate::test_support::VoterImageSetup {
            node: leader,
            endpoints: vec![krabka_metadata::VoterEndpoint {
                name: "CONTROLLER".to_owned(),
                host: controller_addr.ip().to_string(),
                port: controller_addr.port(),
            }],
        })
    }

    /// Boot a single-node broker that leads the metadata quorum, and hand back
    /// the address of the controller listener KIP-919 puts api key 73 on.
    async fn start_controller() -> (crate::BrokerHandle, SocketAddr, tempfile::TempDir) {
        crate::test_support::start_controller().await
    }

    #[tokio::test]
    async fn send_assignments_rejects_bad_controller_leader() {
        let cases = [
            // No controller leader elected at all.
            (None, "no controller leader"),
            // A leader that neither the voter set nor the configured quorum
            // carries an endpoint for.
            (
                Some(NodeId(42)),
                "controller leader has no known controller endpoint",
            ),
        ];
        for (leader, expected) in cases {
            let source: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(
                FakeMetadataSource::builder()
                    .image(MetadataImage::new(uuid::Uuid::nil()))
                    .leader(leader)
                    .build(),
            );

            let err = send_assignments(
                &source,
                &plaintext_dialer(Vec::new()),
                "assign-test",
                build_request(1, UNKNOWN_BROKER_EPOCH, &[]),
            )
            .await
            .expect_err("bad controller leader must fail");

            assert!(err == expected, "case: leader={leader:?}");
        }
    }

    fn voter_only_source(
        controller_addr: std::net::SocketAddr,
    ) -> Arc<dyn crate::metadata_source::MetadataSource> {
        Arc::new(
            FakeMetadataSource::builder()
                .image(voter_only_image(NodeId(1), controller_addr))
                .leader(Some(NodeId(1)))
                .build(),
        )
    }

    /// KIP-919 carries `AssignReplicasToDirs` on the controller listener, and a
    /// controller-only node publishes no broker registration at all. Resolving
    /// the leader through `image.broker()` would therefore strand every
    /// broker-only node: the report would never leave, and no partition would
    /// ever record which log dir hosts its replica.
    ///
    /// The voter set is the source, so a leader that is only ever a controller
    /// is still reachable, and the report round-trips.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_assignments_reaches_a_leader_registered_only_as_a_controller() {
        let (broker, controller_addr, _dir) = start_controller().await;
        let source = voter_only_source(controller_addr);

        let sent = send_assignments(
            &source,
            &plaintext_dialer(Vec::new()),
            "assign-test",
            build_request(1, UNKNOWN_BROKER_EPOCH, &[]),
        )
        .await;

        assert!(sent == Ok(()));
        broker.shutdown().await;
    }

    /// The report travels the controller listener's own channel. A bare
    /// `krabka_client_core::Client` would put plaintext Kafka frames on a
    /// listener configured for SSL, `SASL_SSL`, or `SASL_PLAINTEXT` and fail
    /// every reconcile.
    ///
    /// An SSL controller listener with no client TLS configured is the
    /// observable end of that: the dialer refuses before a byte is written,
    /// where a plaintext client would have connected and succeeded.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_assignments_dials_with_the_controller_listener_security() {
        let (broker, controller_addr, _dir) = start_controller().await;
        let source = voter_only_source(controller_addr);
        let mut dialer = plaintext_dialer(Vec::new());
        dialer.listener_protocol = krabka_security::ListenerProtocol::Ssl;

        let err = send_assignments(
            &source,
            &dialer,
            "assign-test",
            build_request(1, UNKNOWN_BROKER_EPOCH, &[]),
        )
        .await
        .expect_err("an SSL controller listener needs the configured TLS client");

        assert!(err == "connect: config: TLS listener without TlsConnector");
        broker.shutdown().await;
    }

    #[test]
    fn validate_assign_response_rejects_controller_error() {
        assert!(validate_assign_response(0, NodeId(7), Some(NodeId(7))).is_ok());
        let err = validate_assign_response(42, NodeId(7), Some(NodeId(7)))
            .expect_err("non-zero error_code must fail");
        assert!(err.contains("error_code=42"));
    }

    #[test]
    fn validate_assign_response_rejects_stale_controller_and_is_retryable() {
        for current in [None, Some(NodeId(8))] {
            for _ in 0..2 {
                let err = validate_assign_response(0, NodeId(7), current)
                    .expect_err("stale controller success must be retried");
                assert!(err.contains("leader changed"));
            }
        }
    }

    #[test]
    fn assign_dirs_connection_options_sets_client_id() {
        let opts = assign_dirs_connection_options("test-client");
        assert2::assert!(opts.client_id == "test-client");
    }
}
