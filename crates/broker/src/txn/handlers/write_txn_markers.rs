//! `WriteTxnMarkers` (`api_key=27`). Receives a fan-out from the transaction
//! coordinator (`EndTxn`) and appends control-marker batches to each
//! partition this broker leads.
//!
//! ## Flow
//!
//! For each marker entry in the request:
//! 1. Determine commit or abort from `transaction_result`.
//! 2. For each (topic, partition) named in the marker, as Kafka's
//!    `KafkaApis.handleWriteTxnMarkersRequest` does:
//!    - A partition this broker does not host, or hosts in an offline log
//!      directory, answers `UNKNOWN_TOPIC_OR_PARTITION`. Kafka's
//!      `ReplicaManager.onlinePartition` finds no online partition for both.
//!    - A partition this broker hosts but does not lead answers
//!      `NOT_LEADER_OR_FOLLOWER`, and nothing is appended. Kafka appends with
//!      `AppendOrigin.COORDINATOR`, and the leader append refuses a follower.
//!    - A partition whose ISR is smaller than `min.insync.replicas` answers
//!      `NOT_ENOUGH_REPLICAS`, and nothing is appended.
//!    - Otherwise the handler appends the marker batch.
//! 3. Wait until every appended marker commits, under one deadline of
//!    Kafka's default `request.timeout.ms`. Kafka appends the markers with
//!    `requiredAcks=-1` and answers a partition when its `DelayedProduce`
//!    completes: `NONE` when the high watermark covers the marker,
//!    `NOT_LEADER_OR_FOLLOWER` when the broker stops leading the partition
//!    first, and `REQUEST_TIMED_OUT` at the deadline.
//! 4. Return a nested per-producer → per-topic → per-partition response.
//!    Kafka keys the results by producer id, so two marker entries for one
//!    producer come back as one result.
//!
//! Wire format: v1 flexible with tagged fields, and v2 flexible with
//! `transaction_version`.

use futures_util::future::BoxFuture;
use krabka_ids::PartitionIndex;
use krabka_protocol::owned::{
    write_txn_markers_request::WriteTxnMarkersRequest,
    write_txn_markers_response::{
        WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
        WriteTxnMarkersResponse,
    },
};

mod leader;
mod materialize;
mod offsets;

#[cfg(test)]
pub(crate) mod test_support;

pub(crate) use self::{
    leader::{MARKER_COMMIT_TIMEOUT, append_marker_as_leader},
    materialize::MarkerAppend,
    offsets::CommittedOffsets,
};
use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::{RequestContext, cluster_action_denied, cluster_alter_denied},
    txn::marker::MarkerType,
};

/// Authorizes the request, then writes the markers.
///
/// Kafka's `KafkaApis.handleWriteTxnMarkersRequest` allows a principal that
/// holds `Alter` or `ClusterAction` on the cluster. Any other principal gets
/// `WriteTxnMarkersRequest.getErrorResponse` with
/// `CLUSTER_AUTHORIZATION_FAILED`: that code on every requested partition,
/// and no marker is written.
pub(crate) async fn handle(
    broker: &Broker,
    req: WriteTxnMarkersRequest,
    _version: i16,
    ctx: &RequestContext<'_>,
) -> Result<WriteTxnMarkersResponse, BrokerError> {
    let authorizer = broker.config.authorizer.as_ref();
    let image = broker.controller.current_image();
    let resp = if cluster_alter_denied(authorizer, &image, ctx)
        && cluster_action_denied(authorizer, &image, ctx)
    {
        cluster_authorization_failed(&req)
    } else {
        serve(broker, req).await
    };
    Ok(resp)
}

/// The response Kafka's `WriteTxnMarkersRequest.getErrorResponse` builds for
/// `CLUSTER_AUTHORIZATION_FAILED`: every requested partition of every marker
/// carries the code.
fn cluster_authorization_failed(req: &WriteTxnMarkersRequest) -> WriteTxnMarkersResponse {
    WriteTxnMarkersResponse {
        markers: req
            .markers
            .iter()
            .map(|marker| WritableTxnMarkerResult {
                producer_id: marker.producer_id,
                topics: marker
                    .topics
                    .iter()
                    .map(|topic| WritableTxnMarkerTopicResult {
                        name: topic.name.clone(),
                        partitions: topic
                            .partition_indexes
                            .iter()
                            .map(|&partition_index| WritableTxnMarkerPartitionResult {
                                partition_index,
                                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn serve(
    broker: &Broker,
    req: WriteTxnMarkersRequest,
) -> BoxFuture<'static, WriteTxnMarkersResponse> {
    let partitions = broker.partitions.clone();
    let group_coordinator = broker.group_coordinator.clone();
    let log_dir_status = broker.log_dir_status.clone();
    let node_id = broker.config.node_id;
    Box::pin(async move {
        // Kafka appends every marker of the request first and then waits for
        // all of them under one `DelayedProduce`, so one timeout bounds the
        // whole request. The rows keep the order of the request.
        let mut rows = Vec::new();
        for marker_entry in &req.markers {
            let marker_type = if marker_entry.transaction_result {
                MarkerType::Commit
            } else {
                MarkerType::Abort
            };
            // Wrap the wire `i64` into `ProducerId` for the marker builder;
            // unwrapped again below for the raw-`i64` response field.
            let pid = krabka_log::ProducerId(marker_entry.producer_id);
            let marker = MarkerAppend {
                producer_id: pid,
                producer_epoch: marker_entry.producer_epoch,
                marker_type,
                coordinator_epoch: marker_entry.coordinator_epoch,
                commit_stamp: None,
                transaction_version: i16::from(marker_entry.transaction_version),
            };

            for topic in &marker_entry.topics {
                for &p in &topic.partition_indexes {
                    let row = match partitions.get(&topic.name, PartitionIndex(p)) {
                        Some(part) if !log_dir_status.is_offline(&part.log_dir.load()) => {
                            append_marker_as_leader(
                                &part,
                                node_id,
                                Some(&group_coordinator),
                                &topic.name,
                                marker,
                            )
                            .await
                            .map_err(|error| marker_error_code(&topic.name, p, &error))
                        }
                        _ => {
                            tracing::debug!(
                                topic = %topic.name,
                                partition = p,
                                "WriteTxnMarkers: partition not online here; returning UNKNOWN_TOPIC_OR_PARTITION"
                            );
                            Err(codes::UNKNOWN_TOPIC_OR_PARTITION)
                        }
                    };
                    rows.push((pid.get(), topic.name.clone(), p, row));
                }
            }
        }

        let deadline = std::time::Instant::now() + MARKER_COMMIT_TIMEOUT;
        let answered = futures_util::future::join_all(rows.into_iter().map(
            |(producer_id, topic, partition, row)| async move {
                let code = match row {
                    Ok(pending) => match pending.committed(deadline).await {
                        Ok(()) => codes::NONE,
                        Err(error) => marker_error_code(&topic, partition, &error),
                    },
                    Err(code) => code,
                };
                (producer_id, topic, partition, code)
            },
        ))
        .await;
        let mut marker_results = MarkerResults::default();
        for (producer_id, topic, partition, code) in answered {
            marker_results.record(producer_id, &topic, partition, code);
        }

        WriteTxnMarkersResponse {
            markers: marker_results.markers,
            ..Default::default()
        }
    })
}

/// The response code for a marker that failed or did not commit.
///
/// A refusal carries the code Kafka answers for it. A log failure is Kafka's
/// `KafkaStorageException`, which the transaction coordinator retries
/// (`TransactionMarkerRequestCompletionHandler`). Every other failure keeps
/// its broker-wide code.
fn marker_error_code(topic: &str, partition: i32, error: &BrokerError) -> i16 {
    if let BrokerError::MarkerWriteRefused { code, .. } = error {
        tracing::debug!(topic, partition, %error, "WriteTxnMarkers: marker refused");
        return *code;
    }
    tracing::warn!(topic, partition, %error, "WriteTxnMarkers: marker append failed");
    match error {
        BrokerError::Log(_) | BrokerError::Io(_) => codes::KAFKA_STORAGE_ERROR,
        other => codes::from_broker_error(other),
    }
}

/// The response under construction, in Kafka's shape: one result per
/// producer id, one topic row per topic name, and one row per partition. The
/// index maps keep each insert constant time, so a request with many
/// partitions builds its response in one linear pass.
#[derive(Default)]
struct MarkerResults {
    markers: Vec<WritableTxnMarkerResult>,
    producers: std::collections::HashMap<i64, usize>,
    topics: std::collections::HashMap<(i64, String), usize>,
    partitions: std::collections::HashMap<(i64, String, i32), usize>,
}

impl MarkerResults {
    /// Put one partition's code into the response. A repeated partition keeps
    /// the last code, as Kafka's map does.
    fn record(&mut self, producer_id: i64, topic: &str, partition_index: i32, error_code: i16) {
        let marker = *self.producers.entry(producer_id).or_insert_with(|| {
            self.markers.push(WritableTxnMarkerResult {
                producer_id,
                ..Default::default()
            });
            self.markers.len() - 1
        });
        let topics = &mut self.markers[marker].topics;
        let topic_row = *self
            .topics
            .entry((producer_id, topic.to_owned()))
            .or_insert_with(|| {
                topics.push(WritableTxnMarkerTopicResult {
                    name: topic.to_owned(),
                    ..Default::default()
                });
                topics.len() - 1
            });
        let partitions = &mut topics[topic_row].partitions;
        match self
            .partitions
            .entry((producer_id, topic.to_owned(), partition_index))
        {
            std::collections::hash_map::Entry::Occupied(row) => {
                partitions[*row.get()].error_code = error_code;
            }
            std::collections::hash_map::Entry::Vacant(row) => {
                row.insert(partitions.len());
                partitions.push(WritableTxnMarkerPartitionResult {
                    partition_index,
                    error_code,
                    ..Default::default()
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::{
        UnknownTaggedFields,
        owned::write_txn_markers_request::{WritableTxnMarker, WritableTxnMarkerTopic},
    };

    use super::*;
    use crate::{
        coordinator::{
            bootstrap::OFFSETS_TOPIC,
            persistence::OffsetCommitValue,
            unified::actor::{GroupActorMessage, test_support::rpc},
        },
        txn::handlers::write_txn_markers::test_support::{open_partition, start_broker},
    };

    fn check_orders_marker(response: &WriteTxnMarkersResponse, expected_code: i16, label: &str) {
        assert!(
            response
                == &WriteTxnMarkersResponse {
                    markers: vec![result(91, "orders", &[(1, expected_code)])],
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            "{label}"
        );
    }

    #[derive(Clone, Copy)]
    struct PendingOffsetsSetup<'a> {
        group_id: &'a str,
        topic: &'a str,
        partition: krabka_ids::PartitionIndex,
    }

    impl Default for PendingOffsetsSetup<'_> {
        fn default() -> Self {
            Self {
                group_id: "marker-materialization-group",
                topic: "orders",
                partition: krabka_ids::PartitionIndex(2),
            }
        }
    }

    async fn mark_pending_offsets(
        broker: &Broker,
        setup: PendingOffsetsSetup<'_>,
    ) -> std::sync::Arc<crate::coordinator::unified::actor::GroupActorHandle> {
        let PendingOffsetsSetup {
            group_id,
            topic,
            partition,
        } = setup;
        let handle = broker.group_coordinator.get_or_create_group(
            group_id,
            crate::coordinator::unified::actor::GroupKindTag::Classic,
        );
        let ack = rpc::begin(&handle, |reply| GroupActorMessage::AddPendingTxnOffsets {
            producer_id: 91,
            written_at: 0,
            keys: vec![(topic.to_string(), partition.0)],
            reply,
        })
        .await;
        ack.await.expect("AddPendingTxnOffsets ack");
        handle
    }

    async fn commit_offsets_marker(broker: &Broker, partition: i32) {
        let response = handle_allowed(broker, offsets_marker(partition, true))
            .await
            .expect("commit marker");
        assert!(response.markers[0].topics[0].partitions[0].error_code == codes::NONE);
    }

    const VERSION: i16 = 2;

    /// Serves a request as a principal that the default `AllowAllAuthorizer`
    /// allows.
    async fn handle_allowed(
        broker: &Broker,
        req: WriteTxnMarkersRequest,
    ) -> Result<WriteTxnMarkersResponse, BrokerError> {
        let user = crate::test_support::principal("ANONYMOUS");
        let address = crate::test_support::peer();
        let ctx = crate::test_support::request_context(&user, &address, "write-txn-markers-test");
        super::handle(broker, req, VERSION, &ctx).await
    }

    fn marker(producer_id: i64, topic: &str, partitions: Vec<i32>) -> WritableTxnMarker {
        WritableTxnMarker {
            producer_id,
            producer_epoch: 4,
            transaction_result: true,
            transaction_version: 1,
            topics: vec![WritableTxnMarkerTopic {
                name: topic.into(),
                partition_indexes: partitions,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn result(producer_id: i64, topic: &str, rows: &[(i32, i16)]) -> WritableTxnMarkerResult {
        WritableTxnMarkerResult {
            producer_id,
            topics: vec![WritableTxnMarkerTopicResult {
                name: topic.into(),
                partitions: rows
                    .iter()
                    .map(
                        |&(partition_index, error_code)| WritableTxnMarkerPartitionResult {
                            partition_index,
                            error_code,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                    )
                    .collect(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }
    }

    /// Kafka's `KafkaApis.handleWriteTxnMarkersRequest` answers
    /// `UNKNOWN_TOPIC_OR_PARTITION` for a partition that is not online on this
    /// broker, and appends every other marker with `AppendOrigin.COORDINATOR`,
    /// which refuses a follower with `NOT_LEADER_OR_FOLLOWER`.
    #[tokio::test]
    async fn a_marker_appends_only_to_an_online_partition_this_broker_leads() {
        enum Hosting {
            Led,
            Followed,
            NotHosted,
            LedInOfflineLogDir,
        }
        let cases = [
            ("hosted and led", Hosting::Led, codes::NONE, 1),
            (
                "hosted, led elsewhere",
                Hosting::Followed,
                codes::NOT_LEADER_OR_FOLLOWER,
                0,
            ),
            (
                "not hosted",
                Hosting::NotHosted,
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                0,
            ),
            (
                "led, log directory offline",
                Hosting::LedInOfflineLogDir,
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                0,
            ),
        ];
        for (name, hosting, expected_code, expected_log_end) in cases {
            let (broker_handle, dir) = start_broker().await;
            let broker = broker_handle.broker_arc_for_test();
            let node_id = broker.config.node_id.0;
            let log_dir = dir.path().join("markers");
            let part = match hosting {
                Hosting::NotHosted => None,
                Hosting::Led | Hosting::Followed | Hosting::LedInOfflineLogDir => {
                    let part = open_partition(
                        &broker,
                        &log_dir,
                        crate::test_support::StandalonePartitionSetup {
                            partition: krabka_ids::PartitionIndex(1),
                            ..Default::default()
                        },
                    );
                    let leader = if matches!(hosting, Hosting::Followed) {
                        node_id + 1
                    } else {
                        node_id
                    };
                    part.install_replication_target(None, leader, 3).await;
                    if matches!(hosting, Hosting::LedInOfflineLogDir) {
                        broker.log_dir_status.mark_offline(&log_dir, "test");
                    }
                    Some(part)
                }
            };

            let bytes = handle_allowed(
                &broker,
                WriteTxnMarkersRequest {
                    markers: vec![marker(91, "orders", vec![1])],
                    ..Default::default()
                },
            )
            .await
            .expect("handle");

            check_orders_marker(&bytes, expected_code, name);
            if let Some(part) = part {
                assert!(part.log_end_offset().0 == expected_log_end, "{name}");
            }
            broker_handle.shutdown().await;
        }
    }

    /// Kafka's `handleWriteTxnMarkersRequest` appends with `requiredAcks=-1`
    /// and answers a partition when its `DelayedProduce` completes: `NONE`
    /// once the high watermark covers the marker, `NOT_LEADER_OR_FOLLOWER`
    /// when the broker stops leading the partition first. The leader append
    /// refuses a marker with `NOT_ENOUGH_REPLICAS` while the ISR is below
    /// `min.insync.replicas`, and appends nothing.
    #[tokio::test]
    async fn a_marker_is_answered_only_once_committed() {
        enum Change {
            FollowerCatchesUp,
            LeaderMoves,
            UnderMinIsr,
        }
        let cases = [
            (
                "the follower catches up",
                Change::FollowerCatchesUp,
                codes::NONE,
                1,
            ),
            (
                "another broker takes the partition first",
                Change::LeaderMoves,
                codes::NOT_LEADER_OR_FOLLOWER,
                1,
            ),
            (
                "ISR below min.insync.replicas",
                Change::UnderMinIsr,
                codes::NOT_ENOUGH_REPLICAS,
                0,
            ),
        ];
        for (name, change, expected_code, expected_log_end) in cases {
            let (broker_handle, dir) = start_broker().await;
            let broker = broker_handle.broker_arc_for_test();
            let node_id = broker.config.node_id;
            let follower = krabka_metadata::NodeId(node_id.0 + 1);
            let part = open_partition(
                &broker,
                &dir.path().join("markers"),
                crate::test_support::StandalonePartitionSetup {
                    partition: krabka_ids::PartitionIndex(1),
                    ..Default::default()
                },
            );
            part.install_replication_target(None, node_id.0, 3).await;
            // The follower has not fetched, so it holds the high watermark at
            // zero.
            part.install_isr(&[node_id, follower], &[node_id, follower], node_id)
                .await;
            if matches!(change, Change::UnderMinIsr) {
                part.install_isr(&[node_id], &[node_id, follower], node_id)
                    .await;
                part.replica_state
                    .lock()
                    .await
                    .set_policy(crate::replica_state::LeaderPolicy {
                        effective_min_isr: 2,
                        replica_lag_time_max: std::time::Duration::from_secs(30),
                        brokers: std::collections::HashMap::new(),
                    });
            }

            let request = WriteTxnMarkersRequest {
                markers: vec![marker(91, "orders", vec![1])],
                ..Default::default()
            };
            let answer = tokio::spawn({
                let broker = std::sync::Arc::clone(&broker);
                async move { handle_allowed(&broker, request).await }
            });
            if !matches!(change, Change::UnderMinIsr) {
                loop {
                    let appended = part.append_notify.notified();
                    tokio::pin!(appended);
                    appended.as_mut().enable();
                    if part.log_end_offset().0 == 1 {
                        break;
                    }
                    appended.await;
                }
                // intentional: the handler would answer within this time if
                // it answered at the local append.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                assert!(!answer.is_finished(), "{name}: answered before the commit");
                if matches!(change, Change::LeaderMoves) {
                    part.install_replication_target(None, follower.0, 4).await;
                } else {
                    part.replica_state.lock().await.hw = krabka_log::Offset(1);
                    part.hw_advance_notify.notify_waiters();
                }
            }
            let bytes = answer.await.expect("the handler task").expect("handle");

            check_orders_marker(&bytes, expected_code, name);
            assert!(part.log_end_offset().0 == expected_log_end, "{name}");
            broker_handle.shutdown().await;
        }
    }

    /// Kafka keys the results by producer id, so two marker entries for one
    /// producer come back as one result.
    #[tokio::test]
    async fn marker_entries_for_one_producer_share_one_result() {
        let (broker_handle, dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let node_id = broker.config.node_id.0;
        for partition in [1, 2] {
            open_partition(
                &broker,
                dir.path(),
                crate::test_support::StandalonePartitionSetup {
                    partition: krabka_ids::PartitionIndex(partition),
                    ..Default::default()
                },
            )
            .install_replication_target(None, node_id, 3)
            .await;
        }

        let bytes = handle_allowed(
            &broker,
            WriteTxnMarkersRequest {
                markers: vec![
                    marker(91, "orders", vec![1]),
                    marker(92, "orders", vec![1]),
                    marker(91, "orders", vec![2]),
                ],
                ..Default::default()
            },
        )
        .await
        .expect("handle");

        assert!(
            bytes
                == WriteTxnMarkersResponse {
                    markers: vec![
                        result(91, "orders", &[(1, codes::NONE), (2, codes::NONE)]),
                        result(92, "orders", &[(1, codes::NONE)]),
                    ],
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn committed_offsets_are_published_by_the_offsets_partition_marker() {
        use krabka_log::Offset;

        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let group_id = "marker-materialization-group";
        let (offsets_partition, part) =
            super::test_support::local_offsets_partition_with_index(&broker, group_id);
        let value = OffsetCommitValue {
            offset: Offset(42),
            leader_epoch: 3,
            metadata: "txn".into(),
            // A real commit timestamp: an epoch-relative one is older than
            // `offsets.retention.minutes`, so the KIP-211 sweep would
            // correctly reap the offset this test is about.
            commit_timestamp_ms: crate::time_util::now_ms(),
            expire_timestamp_ms: None,
            topic_id: None,
        };
        part.produce_batch(super::test_support::transactional_offset_batch(
            (krabka_log::ProducerId(91), 4),
            (group_id, "orders", 2),
            &value,
        ))
        .await
        .expect("append transactional offset");

        commit_offsets_marker(&broker, offsets_partition).await;

        let handle = broker
            .group_coordinator
            .find(group_id)
            .expect("offset home actor");
        let committed = rpc::fetch_offsets(&handle).await.committed;
        let entry = committed
            .get(&("orders".to_string(), 2))
            .expect("committed offset visible");
        assert!(entry.offset == 42);
        assert!(entry.leader_epoch == 3);
        assert!(entry.metadata == "txn");
        broker_handle.shutdown().await;
    }

    /// An abort marker publishes nothing, so a group whose actor has already
    /// exited has nothing left to resolve: its KIP-447 pending marks died with
    /// it. The marker is durable by the time the coordinator is consulted, so
    /// reporting a failure would only make the transaction coordinator retry a
    /// marker that has already landed.
    #[tokio::test]
    async fn abort_marker_succeeds_when_the_groups_actor_has_exited() {
        use krabka_log::Offset;

        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let group_id = "abort-after-actor-exit";
        let (offsets_partition, part) =
            super::test_support::local_offsets_partition_with_index(&broker, group_id);
        part.produce_batch(super::test_support::transactional_offset_batch(
            (krabka_log::ProducerId(91), 4),
            (group_id, "orders", 2),
            &OffsetCommitValue {
                offset: Offset(42),
                leader_epoch: 3,
                metadata: "txn".into(),
                commit_timestamp_ms: 123,
                expire_timestamp_ms: None,
                topic_id: None,
            },
        ))
        .await
        .expect("append transactional offset");

        // The actor takes the transaction's pending marks and then exits,
        // leaving a closed handle behind in the registry.
        let handle = mark_pending_offsets(
            &broker,
            PendingOffsetsSetup {
                group_id,
                ..Default::default()
            },
        )
        .await;
        rpc::shutdown(&handle).await;
        for _ in 0..1000 {
            if handle.tx.is_closed() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert!(handle.tx.is_closed());

        let req = offsets_marker(offsets_partition, false);
        let response = handle_allowed(&broker, req).await.expect("abort marker");
        assert!(
            response
                == WriteTxnMarkersResponse {
                    markers: vec![WritableTxnMarkerResult {
                        producer_id: 91,
                        topics: vec![WritableTxnMarkerTopicResult {
                            name: OFFSETS_TOPIC.into(),
                            partitions: vec![WritableTxnMarkerPartitionResult {
                                partition_index: offsets_partition,
                                error_code: codes::NONE,
                                unknown_tagged_fields: UnknownTaggedFields::default(),
                            }],
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        }],
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }
        );
        broker_handle.shutdown().await;
    }

    /// One transaction can carry offset commits for several groups on the same
    /// offsets partition, and its marker has to resolve every one of them. The
    /// marker is durable before the coordinator is consulted, and it ended the
    /// log's pending transaction, so a group the resolution skips keeps its
    /// KIP-447 marks and loses its offsets for good: no marker retry can
    /// rediscover them.
    #[tokio::test]
    async fn a_commit_marker_resolves_every_group_in_the_transaction() {
        use krabka_log::Offset;
        use krabka_protocol::records::{Attributes, Record, RecordBatch};

        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let first = "marker-two-groups-a";
        let second = "marker-two-groups-b";
        // Both groups' records go into one batch on one partition, which is
        // what a single marker resolves; the group ids come off the record
        // keys, not from the partition.
        let offsets_partition = crate::coordinator::partitioner::partition_for_group(
            &broker.controller.current_image(),
            first,
        );
        let part = broker
            .partitions
            .get(OFFSETS_TOPIC, PartitionIndex(offsets_partition))
            .expect("local offsets partition");
        let row = |group_id, topic, partition, offset, delta| Record {
            offset_delta: delta,
            key: Some(OffsetCommitValue::encode_key(group_id, topic, partition).unwrap()),
            value: Some(
                OffsetCommitValue {
                    offset: Offset(offset),
                    leader_epoch: 3,
                    metadata: "txn".into(),
                    commit_timestamp_ms: 123,
                    expire_timestamp_ms: None,
                    topic_id: None,
                }
                .encode_value(),
            ),
            ..Default::default()
        };
        part.produce_batch(RecordBatch {
            producer_id: 91,
            producer_epoch: 4,
            base_sequence: 0,
            attributes: Attributes::default().with_transactional(true),
            last_offset_delta: 1,
            records: vec![
                row(first, "orders", 2, 42, 0),
                row(second, "payments", 5, 7, 1),
            ],
            ..RecordBatch::default()
        })
        .await
        .expect("append transactional offsets");

        // Both groups hold the transaction's pending marks, the way
        // `TxnOffsetCommit` leaves them.
        for (group_id, topic, partition) in [(first, "orders", 2), (second, "payments", 5)] {
            mark_pending_offsets(
                &broker,
                PendingOffsetsSetup {
                    group_id,
                    topic,
                    partition: krabka_ids::PartitionIndex(partition),
                },
            )
            .await;
        }

        commit_offsets_marker(&broker, offsets_partition).await;

        for (group_id, topic, partition, offset) in
            [(first, "orders", 2, 42), (second, "payments", 5, 7)]
        {
            let handle = broker
                .group_coordinator
                .find(group_id)
                .expect("offset home actor");
            let offsets = rpc::fetch_offsets(&handle).await;
            assert!(
                offsets
                    .committed
                    .get(&(topic.to_string(), partition))
                    .map(|entry| entry.offset)
                    == Some(krabka_log::Offset(offset))
            );
            assert!(offsets.pending_txn.is_empty());
        }
        broker_handle.shutdown().await;
    }
    fn offsets_marker(offsets_partition: i32, transaction_result: bool) -> WriteTxnMarkersRequest {
        use crate::coordinator::bootstrap::OFFSETS_TOPIC;
        WriteTxnMarkersRequest {
            markers: vec![WritableTxnMarker {
                producer_id: 91,
                producer_epoch: 4,
                transaction_result,
                transaction_version: 1,
                topics: vec![WritableTxnMarkerTopic {
                    name: OFFSETS_TOPIC.into(),
                    partition_indexes: vec![offsets_partition],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }
}
