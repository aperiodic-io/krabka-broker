//! `FetchSnapshot` (`api_key=59`, KIP-630). Serves a byte range of the
//! controller's `__cluster_metadata` snapshot to a replica catching up.
//!
//! Krabka runs a single raft log, the controller quorum, and snapshots its
//! `MetadataImage`. A replica fetches the snapshot one page at a time as it
//! advances `position`. Each response carries the requested byte range
//! verbatim, plus the snapshot's `(end_offset, epoch)` id and total `size`.
//!
//! The broker listener does not answer the request itself. It hands the body to
//! the controller's engine, which is what the controller listener does too, so
//! both listeners give one answer to one request: Kafka's
//! `KafkaRaftClient.handleFetchSnapshotRequest`. The engine serves exactly the
//! snapshot the request names and answers `SNAPSHOT_NOT_FOUND` for any other,
//! `UNKNOWN_TOPIC_OR_PARTITION` for another topic or partition,
//! `FENCED_LEADER_EPOCH`, `UNKNOWN_LEADER_EPOCH` or `NOT_LEADER_OR_FOLLOWER`
//! for a `CurrentLeaderEpoch` that this node cannot serve, and
//! `POSITION_OUT_OF_RANGE` for a position outside the snapshot. It accepts the
//! cluster id in Kafka's base64 `Uuid` form as well as the hyphenated one, and
//! fills `CurrentLeader` and the leader's endpoint into the response.
//!
//! The returned bytes are *unaligned*. A snapshot is many concatenated
//! record batches, and a paged byte range is by design not batch-aligned.
//!
//! A broker-only observer runs no engine and keeps no metadata log to serve.
//! It answers `SNAPSHOT_NOT_FOUND` for the metadata partition and
//! `UNKNOWN_TOPIC_OR_PARTITION` for any other.

use bytes::Bytes;
use krabka_protocol::owned::{
    fetch_snapshot_request::FetchSnapshotRequest,
    fetch_snapshot_response::{FetchSnapshotResponse, PartitionSnapshot, TopicSnapshot},
};

use crate::codes;

/// The single Kafka-side topic name that represents the `KRaft` metadata
/// log. It mirrors
/// `org.apache.kafka.common.Topic.CLUSTER_METADATA_TOPIC_NAME`.
const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";

wire_handler! {
    /// Checks `ClusterAction` on the cluster, then serves the byte range.
    ///
    /// The snapshot is the serialized metadata image, with every ACL, config,
    /// SCRAM credential and delegation token record. Kafka's
    /// `ControllerApis.handleFetchSnapshot` calls
    /// `authorizeClusterOperation(request, CLUSTER_ACTION)` first, and a denial
    /// becomes `FetchSnapshotRequest.getErrorResponse`: a top-level
    /// `CLUSTER_AUTHORIZATION_FAILED` and no topics.
    async (broker, version, req_bytes, ctx), {
        let req = crate::handlers::decode_request::<FetchSnapshotRequest>(req_bytes, version)?;
        if crate::handlers::cluster_action_denied(
            broker.config.authorizer.as_ref(),
            &broker.controller.current_image(),
            ctx,
        ) {
            return crate::handlers::encode_response(
                &FetchSnapshotResponse {
                    error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                    ..Default::default()
                },
                version,
            );
        }
        match broker
            .controller
            .fetch_snapshot(version, Bytes::copy_from_slice(req_bytes))
            .await
        {
            Some(answer) => Ok(answer?),
            None => crate::handlers::encode_response(&without_metadata_log(&req), version),
        }
    }
}

/// The answer of a node that keeps no metadata log: no snapshot of the
/// metadata partition to serve, and no such partition for anything else.
fn without_metadata_log(req: &FetchSnapshotRequest) -> FetchSnapshotResponse {
    let topics = req
        .topics
        .iter()
        .map(|topic| TopicSnapshot {
            name: topic.name.clone(),
            partitions: topic
                .partitions
                .iter()
                .map(|part| PartitionSnapshot {
                    index: part.partition,
                    error_code: if topic.name == CLUSTER_METADATA_TOPIC && part.partition == 0 {
                        codes::SNAPSHOT_NOT_FOUND
                    } else {
                        codes::UNKNOWN_TOPIC_OR_PARTITION
                    },
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    FetchSnapshotResponse {
        topics,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use krabka_protocol::owned::{
        fetch_snapshot_request::{
            self, PartitionSnapshot as ReqPartition, SnapshotId as ReqSnapshotId,
            TopicSnapshot as ReqTopic,
        },
        fetch_snapshot_response::{self, LeaderIdAndEpoch},
    };
    use krabka_raft::SnapshotRange;

    use super::*;
    use crate::{
        authorizer::AllowAllAuthorizer,
        test_support::{
            GrantsInPrincipalName, decode_response, encode_request, peer,
            start_broker_no_audit_with,
        },
    };

    /// A request for partition 0 of the metadata topic, at the given id,
    /// leader epoch and position.
    fn request(
        snapshot_id: (i64, i32),
        current_leader_epoch: i32,
        position: i64,
    ) -> FetchSnapshotRequest {
        FetchSnapshotRequest {
            replica_id: -1,
            max_bytes: 16,
            topics: vec![ReqTopic {
                name: CLUSTER_METADATA_TOPIC.into(),
                partitions: vec![ReqPartition {
                    partition: 0,
                    current_leader_epoch,
                    snapshot_id: ReqSnapshotId {
                        end_offset: snapshot_id.0,
                        epoch: snapshot_id.1,
                        ..Default::default()
                    },
                    position,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// One request that differs from the one naming the served snapshot, and
    /// what Kafka's raft client answers to it.
    struct Case {
        what: &'static str,
        /// Edits the request that names the served snapshot at the leader's
        /// epoch, from its first byte.
        edit: Box<dyn Fn(&mut FetchSnapshotRequest)>,
        top_level: i16,
        partition_code: i16,
        /// Whether the partition answer names the leader. Kafka's
        /// `addQuorumLeader` fills `CurrentLeader` in every one but the
        /// unknown-topic answer.
        names_leader: bool,
    }

    fn case(
        what: &'static str,
        partition_code: i16,
        names_leader: bool,
        edit: impl Fn(&mut FetchSnapshotRequest) + 'static,
    ) -> Case {
        Case {
            what,
            edit: Box::new(edit),
            top_level: codes::NONE,
            partition_code,
            names_leader,
        }
    }

    /// A broker listener serves `FetchSnapshot` only to a principal with
    /// `ClusterAction` on the cluster (#682). Kafka's
    /// `ControllerApis.handleFetchSnapshot` authorizes first, and a denial is
    /// `FetchSnapshotRequest.getErrorResponse`: a top-level
    /// `CLUSTER_AUTHORIZATION_FAILED` and no snapshot bytes.
    macro_rules! snapshot_wire {
        (($user:ident, $context:ident, $bytes:ident), $grants:expr, $broker:ident, $version:expr, $address:ident) => {
            let $user = crate::test_support::principal($grants);
            let $context = crate::test_support::request_context(&$user, &$address, "snapshot-test");
            let $bytes = crate::test_support::dispatch_context(
                &$broker,
                fetch_snapshot_request::API_KEY,
                $version,
                &crate::test_support::encode_request(&request((0, 0), 0, 0), $version),
                &$context,
            )
            .await;
        };
    }

    #[tokio::test]
    async fn fetch_snapshot_needs_cluster_action() {
        let (handle, _dir) = start_broker_no_audit_with(|config| {
            config.authorizer = Arc::new(GrantsInPrincipalName);
        })
        .await;
        let broker = handle.broker_arc_for_test();
        let refused = FetchSnapshotResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            ..Default::default()
        };

        let address = peer();
        for version in [
            fetch_snapshot_response::MIN_VERSION,
            fetch_snapshot_response::MAX_VERSION,
        ] {
            for grants in ["none", "Cluster:Describe+Cluster:Alter+Topic:Read"] {
                snapshot_wire!((user, ctx, bytes), grants, broker, version, address);
                check!(
                    crate::test_support::decode_response::<FetchSnapshotResponse>(&bytes, version)
                        == refused,
                    "v{version} {grants}"
                );
            }
            // With ClusterAction the request reaches the engine, which answers
            // a partition of its own.
            snapshot_wire!(
                (user, ctx, bytes),
                "Cluster:ClusterAction",
                broker,
                version,
                address
            );
            let served = decode_response::<FetchSnapshotResponse>(&bytes, version);
            check!(served.error_code == codes::NONE, "v{version}");
            check!(served.topics.len() == 1, "v{version}");
        }
        handle.shutdown().await;
    }

    /// The broker listener answers as Kafka's `handleFetchSnapshotRequest`
    /// does: exactly the named snapshot, and the refusals of its order.
    #[tokio::test]
    async fn the_broker_listener_answers_as_kafkas_raft_client_does() {
        let version = fetch_snapshot_response::MAX_VERSION;
        let (handle, _dir) = start_broker_no_audit_with(|config| {
            config.authorizer = Arc::new(AllowAllAuthorizer);
        })
        .await;
        let broker = handle.broker_arc_for_test();
        handle
            .trigger_snapshot_for_test()
            .await
            .expect("trigger metadata snapshot");
        // The trigger only schedules the snapshot, so wait for its file.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let (snapshot, size, first_page) = loop {
            if let SnapshotRange::Slice(slice) = broker.controller.read_snapshot_range(0, 16) {
                break (
                    (slice.end_offset, slice.epoch),
                    slice.total_size,
                    slice.bytes,
                );
            }
            assert!(
                std::time::Instant::now() <= deadline,
                "no metadata snapshot within 30s"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        };
        let leader_epoch = i32::try_from(broker.controller.quorum_state().current_term)
            .expect("the leader epoch is on the wire as an int32");
        let leader = LeaderIdAndEpoch {
            leader_id: i32::try_from(broker.config.node_id.0).unwrap(),
            leader_epoch,
            ..Default::default()
        };
        let cluster_id = broker.controller.current_image().cluster_id();

        let cases = [
            case("the named snapshot", codes::NONE, true, |_| {}),
            case(
                "another end offset",
                codes::SNAPSHOT_NOT_FOUND,
                true,
                |req| {
                    req.topics[0].partitions[0].snapshot_id.end_offset += 1;
                },
            ),
            case("another epoch", codes::SNAPSHOT_NOT_FOUND, true, |req| {
                req.topics[0].partitions[0].snapshot_id.epoch += 1;
            }),
            case(
                "another topic",
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                false,
                |req| {
                    req.topics[0].name = "not-metadata".into();
                },
            ),
            case(
                "another partition",
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                false,
                |req| {
                    req.topics[0].partitions[0].partition = 1;
                },
            ),
            case(
                "the end of the snapshot",
                codes::POSITION_OUT_OF_RANGE,
                true,
                move |req| {
                    req.topics[0].partitions[0].position = size;
                },
            ),
            case(
                "a negative position",
                codes::POSITION_OUT_OF_RANGE,
                true,
                |req| {
                    req.topics[0].partitions[0].position = -1;
                },
            ),
            case(
                "a stale leader epoch",
                codes::FENCED_LEADER_EPOCH,
                true,
                |req| {
                    req.topics[0].partitions[0].current_leader_epoch -= 1;
                },
            ),
            case(
                "a leader epoch this node has not reached",
                codes::UNKNOWN_LEADER_EPOCH,
                true,
                |req| req.topics[0].partitions[0].current_leader_epoch += 1,
            ),
            case("the cluster id in base64", codes::NONE, true, move |req| {
                req.cluster_id = Some(URL_SAFE_NO_PAD.encode(cluster_id.as_bytes()));
            }),
            case("the cluster id hyphenated", codes::NONE, true, move |req| {
                req.cluster_id = Some(cluster_id.to_string());
            }),
            Case {
                top_level: codes::INCONSISTENT_CLUSTER_ID,
                ..case("another cluster id", codes::NONE, false, |req| {
                    req.cluster_id = Some("another-cluster".into());
                })
            },
        ];

        request_identity!(
            (principal, address, ctx),
            crate::test_support::principal("admin"),
            client_id = "snapshot-test",
            address = crate::test_support::peer()
        );
        for Case {
            what,
            edit,
            top_level,
            partition_code,
            names_leader,
        } in cases
        {
            let mut req = request(snapshot, leader_epoch, 0);
            edit(&mut req);
            let bytes = super::handle(&broker, version, &encode_request(&req, version), &ctx)
                .await
                .expect("handle");
            let resp = decode_response::<FetchSnapshotResponse>(&bytes, version);

            check!(resp.error_code == top_level, "{what}: top-level code");
            if top_level != codes::NONE {
                check!(resp.topics.is_empty(), "{what}: no topic rows");
                continue;
            }
            let [topic] = resp.topics.as_slice() else {
                panic!("{what}: one topic row, got {resp:?}");
            };
            let [part] = topic.partitions.as_slice() else {
                panic!("{what}: one partition row, got {resp:?}");
            };
            check!(topic.name == req.topics[0].name, "{what}: topic");
            check!(
                part.index == req.topics[0].partitions[0].partition,
                "{what}"
            );
            check!(part.error_code == partition_code, "{what}: partition code");
            check!(
                part.current_leader
                    == if names_leader {
                        leader.clone()
                    } else {
                        LeaderIdAndEpoch::default()
                    },
                "{what}: current leader"
            );
            if partition_code == codes::NONE {
                let mut page = bytes::BytesMut::new();
                part.unaligned_records.encode_to(&mut page).unwrap();
                check!(part.snapshot_id.end_offset == snapshot.0, "{what}");
                check!(part.snapshot_id.epoch == snapshot.1, "{what}");
                check!(part.size == size, "{what}");
                check!(part.position == 0, "{what}");
                check!(page.freeze() == first_page, "{what}: the first page");
            }
        }
        handle.shutdown().await;
    }

    /// A broker-only observer keeps no metadata log, so it answers without
    /// asking an engine.
    #[test]
    fn a_node_without_a_metadata_log_has_no_snapshot_and_no_other_partition() {
        let mut req = request((7, 1), 1, 0);
        req.topics.push(ReqTopic {
            name: "not-metadata".into(),
            partitions: vec![ReqPartition {
                partition: 0,
                ..Default::default()
            }],
            ..Default::default()
        });

        let resp = without_metadata_log(&req);

        let expected = FetchSnapshotResponse {
            topics: vec![
                TopicSnapshot {
                    name: CLUSTER_METADATA_TOPIC.into(),
                    partitions: vec![PartitionSnapshot {
                        index: 0,
                        error_code: codes::SNAPSHOT_NOT_FOUND,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                TopicSnapshot {
                    name: "not-metadata".into(),
                    partitions: vec![PartitionSnapshot {
                        index: 0,
                        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert!(resp == expected);
    }
}
