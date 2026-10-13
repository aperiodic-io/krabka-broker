//! `GetReplicaLogInfo` (`api_key` 1020, KIP-966). This is an inter-broker RPC.
//! The controller asks this broker for the log-end-offset and the last-written
//! leader epoch of the partitions that it hosts, which drives offset-aware
//! unclean recovery. The handler table serves it on the inter-broker listener.
//!
//! For each requested partition that this broker hosts locally, the handler
//! answers with the local LEO and the cached leader epoch. A partition that
//! this broker does not host gets `REPLICA_NOT_AVAILABLE (9)` with the
//! sentinel offset `-1`. That matches the JVM behaviour for a replica that the
//! broker is not a member of.

use {
    crate::{
        codes,
        error::BrokerError,
        handlers::{cluster_action_denied, encode_response},
    },
    bytes::Bytes,
    krabka_protocol::{
        Decode,
        owned::{
            get_replica_log_info_request::GetReplicaLogInfoRequest,
            get_replica_log_info_response::{
                GetReplicaLogInfoResponse, PartitionLogInfo, TopicPartitionLogInfo,
            },
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::sync::atomic::Ordering,
};

wire_handler! {
    (broker, version, req_bytes, ctx), {
        let image = broker.controller.current_image();

        // ── ACL preamble ────────────────────────────────────────────
        // Inter-broker control-plane RPC: `ClusterAction` on
        // `Cluster("kafka-cluster")`. The response has no top-level error
        // field (it's a list of per-partition log-info rows), so on Deny we
        // stamp `CLUSTER_AUTHORIZATION_FAILED (31)` on every requested
        // partition — mirroring `alter_replica_log_dirs`' cluster-deny path.
        if cluster_action_denied(broker.config.authorizer.as_ref(), &image, ctx) {
            return respond(version, req_bytes, |_, partition| denied_row(partition));
        }

        respond(version, req_bytes, |topic_id, partition| {
            let hosted = image
                .topic_name_by_id(&uuid::Uuid::from_bytes(topic_id.0))
                .and_then(|name| {
                    broker
                        .partitions
                        .get(name, krabka_ids::PartitionIndex(partition))
                });
            match hosted {
                Some(part) => {
                    let epoch = part.current_leader_epoch.load(Ordering::Acquire);
                    PartitionLogInfo {
                        partition,
                        last_written_leader_epoch: epoch,
                        current_leader_epoch: epoch,
                        // Unwrap the `Offset` into the wire `i64` field.
                        log_end_offset: part.log_end_offset().0,
                        error_code: codes::NONE,
                        error_message: None,
                        ..Default::default()
                    }
                }
                None => unanswered(
                    partition,
                    codes::REPLICA_NOT_AVAILABLE,
                    "partition not hosted locally",
                ),
            }
        })
    }
}

/// Answers every requested `(topic_id, partition)` with the row `row` gives
/// it, in request order. A request that does not decode answers with an
/// empty list.
fn respond(
    version: i16,
    req_bytes: &[u8],
    mut row: impl FnMut(WireUuid, i32) -> PartitionLogInfo,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let topic_partitions = GetReplicaLogInfoRequest::decode(&mut cur, version)
        .map(|req| req.topic_partitions)
        .unwrap_or_default();
    let topic_partition_log_info_list = topic_partitions
        .into_iter()
        .map(|tp| TopicPartitionLogInfo {
            topic_id: tp.topic_id,
            partition_log_info: tp
                .partitions
                .into_iter()
                .map(|partition| row(tp.topic_id, partition))
                .collect(),
            ..Default::default()
        })
        .collect();
    let resp = GetReplicaLogInfoResponse {
        broker_epoch: 0,
        topic_partition_log_info_list,
        ..Default::default()
    };
    encode_response(&resp, version)
}

/// The row a Deny gives every requested partition: the response carries no
/// top-level error code, so the per-partition stamp reports the authorization
/// failure. This mirrors `alter_replica_log_dirs`.
fn denied_row(partition: i32) -> PartitionLogInfo {
    unanswered(
        partition,
        codes::CLUSTER_AUTHORIZATION_FAILED,
        "cluster authorization failed",
    )
}

/// A partition row with sentinel offsets and epochs, refused with `error_code`.
fn unanswered(partition: i32, error_code: i16, error_message: &str) -> PartitionLogInfo {
    PartitionLogInfo {
        partition,
        last_written_leader_epoch: -1,
        current_leader_epoch: -1,
        log_end_offset: -1,
        error_code,
        error_message: Some(error_message.into()),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            log_dir::partition_dir,
            test_support::{encode_request, peer, spawn_standalone_partition},
        },
        assert2::assert,
    };

    /// A locally hosted partition answers with its cached
    /// `current_leader_epoch` and `last_written_leader_epoch`. A non-zero
    /// epoch pins the struct field against the deletion mutant, which would
    /// set it to the default 0.
    #[tokio::test]
    async fn hosted_partition_reports_current_leader_epoch() {
        use {
            crate::test_support::{peer, principal},
            krabka_metadata::{MetadataRecord, NodeId, PartitionRecord, TopicRecord},
            krabka_protocol::owned::{
                get_replica_log_info_request::{self, GetReplicaLogInfoRequest, TopicPartitions},
                get_replica_log_info_response::GetReplicaLogInfoResponse,
            },
            std::sync::{Arc, atomic::Ordering},
        };

        let topic_uuid = uuid::Uuid::from_u128(0xABCD);
        broker_fixture!(
            (broker_handle, dir, broker),
            crate::test_support::start_broker_with_authorizer_no_audit(Arc::new(
                crate::authorizer::AllowAllAuthorizer
            ),)
        );

        // Seed the topic so the handler resolves topic_id → name.
        broker
            .controller
            .submit_change(vec![
                MetadataRecord::V1Topic(TopicRecord {
                    name: "orders".into(),
                    topic_id: topic_uuid,
                    partitions: 1,
                    replication_factor: 1,
                }),
                MetadataRecord::V1Partition(PartitionRecord {
                    directories: vec![uuid::Uuid::nil()],
                    partition_epoch: 1,
                    ..crate::handlers::test_support::single_replica_partition(
                        "orders",
                        krabka_ids::PartitionIndex(0),
                        NodeId(1),
                    )
                }),
            ])
            .await
            .expect("seed topic");

        // Materialize a local replica and force a non-zero leader epoch.
        let part_dir = partition_dir(dir.path(), "orders", 0);
        std::fs::create_dir_all(&part_dir).unwrap();
        let log = krabka_log::Log::open(&part_dir, krabka_log::LogConfig::default()).unwrap();
        let part = spawn_standalone_partition()
            .log_dir(dir.path())
            .log(log)
            .call();
        part.current_leader_epoch.store(11, Ordering::Release);
        broker
            .partitions
            .insert("orders".into(), krabka_ids::PartitionIndex(0), part);

        let version = get_replica_log_info_request::MAX_VERSION;
        let req = GetReplicaLogInfoRequest {
            topic_partitions: vec![TopicPartitions {
                topic_id: krabka_protocol::primitives::uuid::Uuid(*topic_uuid.as_bytes()),
                partitions: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        };
        let req_buf = encode_request(&req, version);

        request_identity!(
            (p, peer, ctx),
            principal("admin"),
            client_id = "inter-broker"
        );
        let bytes = handle(&broker, version, &req_buf, &ctx).expect("handle");
        let mut cur: &[u8] = &bytes;
        let resp = GetReplicaLogInfoResponse::decode(&mut cur, version).unwrap();

        let row = &resp.topic_partition_log_info_list[0].partition_log_info[0];
        assert!(row.error_code == codes::NONE);
        assert!(
            row.current_leader_epoch == 11,
            "hosted partition must report its current_leader_epoch (11), got {}",
            row.current_leader_epoch
        );
        assert!(row.last_written_leader_epoch == 11);
        broker_handle.shutdown().await;
    }

    /// With empty ACLs and no super-users, the authorizer denies
    /// `ClusterAction` to every principal, so the denied response carries
    /// `CLUSTER_AUTHORIZATION_FAILED`.
    #[test]
    fn cluster_action_denied_yields_cluster_authorization_failed() {
        use krabka_protocol::owned::{
            get_replica_log_info_request::{self, GetReplicaLogInfoRequest, TopicPartitions},
            get_replica_log_info_response::GetReplicaLogInfoResponse,
        };

        empty_acl_fixture!(
            (authorizer, image),
            (principal, peer, ctx),
            crate::test_support::principal("ANONYMOUS"),
            client_id = "client-a",
            connection_id = "connection-a"
        );

        assert!(cluster_action_denied(&authorizer, &image, &ctx));

        let version = get_replica_log_info_request::MAX_VERSION;
        let req = GetReplicaLogInfoRequest {
            topic_partitions: vec![TopicPartitions {
                topic_id: krabka_protocol::primitives::uuid::Uuid([7u8; 16]),
                partitions: vec![0, 1],
                ..Default::default()
            }],
            ..Default::default()
        };
        let req_buf = encode_request(&req, version);

        let bytes =
            respond(version, &req_buf, |_, partition| denied_row(partition)).expect("encode resp");
        let mut cur: &[u8] = &bytes;
        let resp = GetReplicaLogInfoResponse::decode(&mut cur, version).unwrap();
        let codes_seen: Vec<i16> = resp
            .topic_partition_log_info_list
            .iter()
            .flat_map(|t| t.partition_log_info.iter().map(|p| p.error_code))
            .collect();
        assert!(!codes_seen.is_empty());
        assert!(
            codes_seen
                .iter()
                .all(|&c| c == codes::CLUSTER_AUTHORIZATION_FAILED)
        );
    }
}
