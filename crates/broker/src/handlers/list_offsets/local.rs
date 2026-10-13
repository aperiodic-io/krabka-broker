//! The offsets that come from a partition's own log rather than from the
//! remote tier: the `LATEST` sentinel's answer, and the leader epoch that
//! belongs with a resolved offset.
//!
//! KFC-1 makes `LATEST` depend on the topic's [`DeliveryPolicy`], which is why
//! the sentinel needs a function of its own rather than the log end offset the
//! rest of the handler reads.

use {crate::partition::Partition, krabka_log::DeliveryPolicy};

/// The offset the `LATEST` sentinel reports for one partition, before the
/// fetchable bound is applied to it.
///
/// A topic that delivers immediately answers the local log end offset. A topic
/// that schedules delivery (KFC-1) answers its delivery watermark instead,
/// because a consumer that seeks to end takes this value as its position. The
/// log end offset would step that consumer over every record that has not come
/// due, and a classic consumer's position is one offset per partition, so those
/// records would be unreachable for it forever. The watermark instead lands it
/// on the first record that still waits, and it receives that record when the
/// record activates.
///
/// The watermark is recomputed under the log mutex rather than read from the
/// partition's lock-free mirror. The mirror is only as fresh as the last
/// scheduler sweep left it, and that scheduler is a liveness aid that may lag
/// or die without making a fetch wrong. A `LATEST` taken from the mirror could
/// therefore sit behind what a fetch at the same instant serves, and hand a
/// seek-to-end consumer records it asked to skip. The recompute answers from
/// the rule and the clock reading that the fetch cap uses.
///
/// A topic that delivers immediately never reaches the recompute, so it pays
/// no extra lock and no walk here. `None` means the topic stopped scheduling
/// delivery between the config snapshot and the recompute, where the log end
/// offset is again the right answer.
///
/// Kafka has no counterpart to this function: it answers LATEST with
/// `lastFetchableOffset` outright. The delivery watermark is the one cap KFC-1
/// adds, and [`resolve_partition`](super::resolve::resolve_partition) takes the
/// lower of the two, so on every topic that does not schedule delivery the
/// answer is Kafka's.
///
/// `Some` confirms the recompute still observed scheduled delivery. Archive
/// resolution uses that fact too, so an immediate-mode transition cannot keep
/// a scheduled cap after the local recompute has discarded it.
pub(super) fn scheduled_latest_offset(
    partition: &Partition,
    policy: DeliveryPolicy,
) -> Option<i64> {
    if policy != DeliveryPolicy::Scheduled {
        return None;
    }
    partition
        .delivery
        .publish_now(&partition.log)
        .map(|delivery| delivery.watermark.0)
}

pub(super) fn leader_epoch_for_offset(partition: &Partition, offset: i64) -> i32 {
    let log = partition.log.lock().expect("log mutex poisoned");
    log.epoch_checkpoint()
        .epoch_for_offset(krabka_log::Offset(offset))
        .map_or(-1, |epoch| epoch.0)
}

/// The start offset of the partition's live leader epoch, from the epoch
/// checkpoint this node's own log keeps. `None` when the checkpoint holds no
/// entry for that epoch, which a fresh promotion whose log prepare has not
/// yet appended one leaves in that state.
///
/// KIP-207 reads this to decide whether the high watermark has caught up to
/// the epoch it is supposed to bound: see
/// [`resolve_partition`](super::resolve::resolve_partition).
pub(super) fn epoch_start_offset(partition: &Partition) -> Option<i64> {
    let current = partition
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    let log = partition.log.lock().expect("log mutex poisoned");
    log.epoch_checkpoint()
        .entries()
        .iter()
        .find(|entry| entry.epoch.0 == current)
        .map(|entry| entry.start_offset.0)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            broker::{Broker, BrokerHandle},
            codes,
            delivery::test_support::{BOUND_MS, NOW_MS, register, scheduled_partition, wall_at},
            handlers::{
                RequestContext,
                list_offsets::{
                    handle,
                    sentinels::{
                        EARLIEST_LOCAL_TIMESTAMP, EARLIEST_PENDING_UPLOAD_TIMESTAMP,
                        EARLIEST_TIMESTAMP, LATEST_TIERED_TIMESTAMP, LATEST_TIMESTAMP,
                        MAX_TIMESTAMP, UNKNOWN_TIMESTAMP,
                    },
                    test_support::{decode_response, encode_request, test_context},
                },
                test_support::replicated_partition,
            },
            test_support::{
                UnixMillis, start_broker_with_authorizer_no_audit as start_broker, test_ctx,
            },
        },
        assert2::{assert, check},
        krabka_protocol::owned::{
            list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
            list_offsets_response::ListOffsetsPartitionResponse,
        },
        std::sync::Arc,
    };

    // Activation time of a batch that has long since come due.
    const DELIVERED_MS: i64 = 1_700_000_000_000;
    // Activation time of a batch that still waits. It sits far enough ahead
    // that every clock this test can read calls it pending, so the schedule
    // holds without a manual timeline.
    const PENDING_MS: i64 = 4_100_000_000_000;
    // One batch that has come due and one that has not, two records each. The
    // log end offset is therefore 4, and a scheduled topic's delivery
    // watermark is 2.
    const ACTIVATIONS: [i64; 2] = [DELIVERED_MS, PENDING_MS];

    // Put a partition holding both activations under `topic` on this broker,
    // and tell the metadata image this broker leads it. `ListOffsets` now
    // answers from what it holds locally only when the image agrees this
    // node leads the partition, so a fixture that skips the image is refused
    // `NOT_LEADER_OR_FOLLOWER`/`UNKNOWN_TOPIC_OR_PARTITION` before it ever
    // reaches the delivery-watermark logic under test.
    async fn register_delivery_partition(
        broker_handle: &BrokerHandle,
        broker: &Broker,
        logs: &tempfile::TempDir,
        topic: &str,
        policy: DeliveryPolicy,
    ) {
        let clock: Arc<dyn qubit_clock::WallClock> = Arc::new(qubit_clock::StdWallClock::new());
        let partition = scheduled_partition()
            .dir(logs)
            .clock(&clock)
            .topic(topic)
            .policy(policy)
            .activations(ACTIVATIONS.iter().copied().map(UnixMillis).collect())
            .leader(broker.config.node_id.get())
            .call();
        register(&broker.partitions, &partition);
        install_partition_leader_record(broker_handle, topic, broker.config.node_id.get()).await;
    }

    /// Tell `handle`'s metadata image that `leader` leads `topic`'s single
    /// partition, so the `ListOffsets` leadership gate admits a request this
    /// node answers from a locally spawned test fixture rather than from the
    /// real topic-creation path.
    async fn install_partition_leader_record(handle: &BrokerHandle, topic: &str, leader: u64) {
        handle
            .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Topic(
                krabka_metadata::TopicRecord {
                    name: topic.to_string(),
                    topic_id: uuid::Uuid::new_v4(),
                    partitions: 1,
                    replication_factor: 1,
                },
            ))
            .await
            .expect("submit topic record");
        handle
            .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(
                krabka_metadata::PartitionRecord {
                    directories: vec![uuid::Uuid::nil()],
                    ..replicated_partition()
                        .topic(topic)
                        .leader(leader)
                        .replicas(&[krabka_audit::NodeId(leader)])
                        .call()
                },
            ))
            .await
            .expect("submit partition record");
    }

    #[bon::builder]
    async fn register_and_lead_partition(
        broker: &Broker,
        handle: &BrokerHandle,
        partition: &Arc<Partition>,
        topic: &str,
    ) {
        register(&broker.partitions, partition);
        install_partition_leader_record(handle, topic, broker.config.node_id.get()).await;
        partition
            .install_leader_change(broker.config.node_id.get(), 0)
            .await;
    }

    fn synced_tierable_segments(partition: &Partition) -> Vec<krabka_log::SegmentExport> {
        let mut log = partition.log.lock().unwrap();
        log.sync().unwrap();
        log.tierable_segments()
    }

    async fn list_partition(
        broker: &Broker,
        topic: &str,
        timestamp: i64,
        ctx: &RequestContext<'_>,
    ) -> ListOffsetsPartitionResponse {
        let version = krabka_protocol::owned::list_offsets_response::MAX_VERSION;
        let req = encode_request(
            &ListOffsetsRequest {
                replica_id: -1,
                topics: vec![ListOffsetsTopic {
                    name: topic.to_string(),
                    partitions: vec![ListOffsetsPartition {
                        partition_index: 0,
                        current_leader_epoch: -1,
                        timestamp,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            },
            version,
        );
        let bytes = handle(broker, version, &req, ctx).await.expect("handle");
        let mut response = decode_response(&bytes, version);
        response.topics.remove(0).partitions.remove(0)
    }

    // The whole LATEST row for a healthy partition 0. Every fixture this
    // module builds installs leader epoch 0, and LATEST now reports the
    // partition's live leader epoch rather than leaving it unfilled.
    fn latest_row(offset: i64) -> ListOffsetsPartitionResponse {
        ListOffsetsPartitionResponse {
            partition_index: 0,
            error_code: codes::NONE,
            timestamp: UNKNOWN_TIMESTAMP,
            offset,
            leader_epoch: 0,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn scheduled_latest_answers_the_delivery_watermark_and_leaves_the_rest_alone() {
        const IMMEDIATE: &str = "list-offsets-delivery-immediate";
        const SCHEDULED: &str = "list-offsets-delivery-scheduled";

        broker_fixture!((broker_handle, _dir, broker), allow_all);
        let logs = tempfile::tempdir().expect("log root");
        register_delivery_partition(
            &broker_handle,
            &broker,
            &logs,
            IMMEDIATE,
            DeliveryPolicy::Immediate,
        )
        .await;
        register_delivery_partition(
            &broker_handle,
            &broker,
            &logs,
            SCHEDULED,
            DeliveryPolicy::Scheduled,
        )
        .await;
        test_ctx!(ctx, "admin");

        // Both topics hold the same records, so every sentinel but LATEST
        // answers the same on both. KFC-1 moves where a seek to end lands and
        // nothing else.
        for timestamp in [
            EARLIEST_TIMESTAMP,
            MAX_TIMESTAMP,
            EARLIEST_LOCAL_TIMESTAMP,
            LATEST_TIERED_TIMESTAMP,
            EARLIEST_PENDING_UPLOAD_TIMESTAMP,
            DELIVERED_MS,
            PENDING_MS,
        ] {
            let immediate = list_partition(&broker, IMMEDIATE, timestamp, &ctx).await;
            let scheduled = list_partition(&broker, SCHEDULED, timestamp, &ctx).await;
            check!(scheduled == immediate, "timestamp {timestamp}");
        }

        // LATEST is the log end offset on a topic that delivers immediately,
        // and the first pending offset on one that schedules delivery. A
        // consumer that seeks to end therefore lands on the waiting batch
        // instead of stepping over it.
        check!(list_partition(&broker, IMMEDIATE, LATEST_TIMESTAMP, &ctx).await == latest_row(4));
        check!(list_partition(&broker, SCHEDULED, LATEST_TIMESTAMP, &ctx).await == latest_row(2));

        broker_handle.shutdown().await;
    }

    fn tiered_delivery_partition(
        logs: &tempfile::TempDir,
        topic: &str,
        leader: krabka_ids::NodeId,
        policy: DeliveryPolicy,
    ) -> (
        Arc<qubit_clock::ManualMonotonicClock>,
        Arc<Partition>,
        krabka_log::LogConfig,
    ) {
        use {
            crate::delivery::test_support::{batch_at, partition_with_batches},
            krabka_log::LogConfig,
            krabka_units::{bytes, millis},
        };

        let timeline = qubit_clock::ManualMonotonicClock::new_shared();
        let clock: Arc<dyn qubit_clock::WallClock> = timeline.new_wall_clock(wall_at(NOW_MS));
        let config = LogConfig {
            segment_size: bytes(1),
            remote_storage_enable: true,
            delivery_policy: policy,
            delivery_clock_uncertainty: millis(u32::try_from(BOUND_MS).unwrap()),
            ..LogConfig::default()
        };
        let partition = partition_with_batches()
            .dir(logs)
            .clock(&clock)
            .topic(topic)
            .config(config.clone())
            .leader(leader.0)
            .batches(vec![
                batch_at(NOW_MS - 1_000),
                batch_at(NOW_MS + 10_000),
                batch_at(NOW_MS - 1_000),
            ])
            .call();
        (timeline, partition, config)
    }

    #[tokio::test]
    async fn scheduled_latest_keeps_pending_records_after_local_retention_pressure() {
        use {
            crate::{
                api_catalog::UnstableApiVersions,
                remote_log_manager::{
                    ArchiveMode, LocalRetentionBounds, copy_eligible, local_retention_pass,
                    test_support::{local_backends, tier},
                },
            },
            krabka_ids::{LeaderEpoch, Offset},
            krabka_remote_storage::TopicIdPartition,
            krabka_units::bytes,
        };

        const TOPIC: &str = "scheduled-latest-retention";
        broker_fixture!((broker_handle, _dir, broker), allow_all);
        let logs = tempfile::tempdir().unwrap();
        let remote = tempfile::tempdir().unwrap();
        let (timeline, partition, mut config) = tiered_delivery_partition(
            &logs,
            TOPIC,
            krabka_ids::NodeId(7),
            DeliveryPolicy::Scheduled,
        );
        config.local_retention_size = Some(bytes(0));
        partition.log.lock().unwrap().set_config(config.clone());
        register_and_lead_partition()
            .broker(&broker)
            .handle(&broker_handle)
            .partition(&partition)
            .topic(TOPIC)
            .call()
            .await;
        let topic_id = broker
            .controller
            .current_image()
            .topic(TOPIC)
            .unwrap()
            .topic_id;
        let tp = TopicIdPartition::new(topic_id, TOPIC, 0);
        let (rsm, rlmm) = local_backends(remote.path());
        let exports = synced_tierable_segments(&partition);
        assert!(
            copy_eligible(
                &tier(ArchiveMode::Mutable, &rsm, &rlmm),
                &tp,
                1,
                LeaderEpoch(0),
                exports.clone()
            )
            .await
                == 2
        );
        assert!(
            local_retention_pass(
                &tp,
                &partition,
                &exports,
                &config,
                &rlmm,
                LocalRetentionBounds {
                    now_ms: NOW_MS,
                    high_watermark: Offset(6)
                },
                UnstableApiVersions::Disabled
            ) == 1
        );
        assert!(partition.log.lock().unwrap().local_log_start_offset() == Offset(2));
        test_ctx!(ctx, "admin");
        assert!(list_partition(&broker, TOPIC, LATEST_TIMESTAMP, &ctx).await == latest_row(2));
        timeline
            .advance(std::time::Duration::from_millis(
                u64::try_from(10_000 + BOUND_MS).unwrap(),
            ))
            .unwrap();
        assert!(list_partition(&broker, TOPIC, LATEST_TIMESTAMP, &ctx).await == latest_row(6));
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn scheduled_latest_finds_a_pending_archive_after_legacy_eviction_or_a_mode_change() {
        use {
            crate::{
                handlers::list_offsets::test_support::remote_topic,
                remote_log_manager::{ArchiveMode, copy_eligible, test_support::tier},
            },
            krabka_ids::{LeaderEpoch, Offset},
            krabka_log::LogConfig,
            krabka_remote_storage::TopicIdPartition,
        };
        for initial_policy in [DeliveryPolicy::Scheduled, DeliveryPolicy::Immediate] {
            const TOPIC: &str = "scheduled-latest-archived";
            let (broker_handle, client, _dirs) = remote_topic(TOPIC, vec![]).await;
            let broker = broker_handle.broker_arc_for_test();
            let logs = tempfile::tempdir().unwrap();
            let (timeline, partition, config) = tiered_delivery_partition(
                &logs,
                TOPIC,
                krabka_ids::NodeId(broker.config.node_id.get()),
                initial_policy,
            );
            register(&broker.partitions, &partition);
            let topic_id = broker
                .controller
                .current_image()
                .topic(TOPIC)
                .unwrap()
                .topic_id;
            let tp = TopicIdPartition::new(topic_id, TOPIC, 0);
            let reader = broker.remote_reader.as_ref().unwrap();
            let exports = {
                let mut log = partition.log.lock().unwrap();
                log.sync().unwrap();
                log.tierable_segments()
            };
            assert!(
                copy_eligible(
                    &tier(ArchiveMode::Mutable, &reader.rsm, &reader.rlmm),
                    &tp,
                    1,
                    LeaderEpoch(0),
                    exports
                )
                .await
                    == 2
            );
            {
                let mut log = partition.log.lock().unwrap();
                // Reproduce eviction by older code, or legal eviction while
                // the topic still delivered immediately, then enable scheduling.
                assert!(log.log_end_offset() == Offset(6));
                assert!(log.delete_local_segments_through(Offset(4)).unwrap() == 2);
                assert!(log.local_log_start_offset() == Offset(4));
                assert!(log.log_end_offset() == Offset(6));
                log.set_config(LogConfig {
                    delivery_policy: DeliveryPolicy::Scheduled,
                    ..config.clone()
                });
            }
            assert!(Arc::ptr_eq(
                &broker
                    .partitions
                    .get(TOPIC, krabka_ids::PartitionIndex(0))
                    .unwrap(),
                &partition,
            ));
            assert!(partition.high_watermark().await == Offset(6));
            test_ctx!(ctx, "admin");
            assert!(list_partition(&broker, TOPIC, LATEST_TIMESTAMP, &ctx).await == latest_row(2));
            assert!(
                super::super::v0::latest_offsets_for_test(&broker, TOPIC).await
                    == (codes::NONE, vec![2])
            );
            partition.log.lock().unwrap().set_config(LogConfig {
                delivery_policy: DeliveryPolicy::Immediate,
                ..config.clone()
            });
            // The earlier Scheduled snapshot must not survive a recompute
            // that observes the topic's switch back to immediate delivery.
            assert!(scheduled_latest_offset(&partition, DeliveryPolicy::Scheduled).is_none());
            {
                let log = partition.log.lock().unwrap();
                assert!(log.config_snapshot().delivery_policy == DeliveryPolicy::Immediate);
                assert!(log.log_end_offset() == Offset(6));
            }
            assert!(partition.high_watermark().await == Offset(6));
            assert!(list_partition(&broker, TOPIC, LATEST_TIMESTAMP, &ctx).await == latest_row(6));
            assert!(
                super::super::v0::latest_offsets_for_test(&broker, TOPIC).await
                    == (codes::NONE, vec![6])
            );
            partition.log.lock().unwrap().set_config(LogConfig {
                delivery_policy: DeliveryPolicy::Scheduled,
                ..config
            });
            assert!(list_partition(&broker, TOPIC, LATEST_TIMESTAMP, &ctx).await == latest_row(2));
            timeline
                .advance(std::time::Duration::from_millis(
                    u64::try_from(10_000 + BOUND_MS - 1).unwrap(),
                ))
                .unwrap();
            assert!(list_partition(&broker, TOPIC, LATEST_TIMESTAMP, &ctx).await == latest_row(2));
            timeline
                .advance(std::time::Duration::from_millis(1))
                .unwrap();
            assert!(list_partition(&broker, TOPIC, LATEST_TIMESTAMP, &ctx).await == latest_row(6));
            drop(client);
            broker_handle.shutdown().await;
        }
    }

    #[tokio::test]
    async fn scheduled_latest_recomputes_the_watermark_instead_of_reading_the_mirror() {
        const TOPIC: &str = "list-offsets-delivery-recompute";
        // Another broker leads this partition at construction, so submitting
        // the metadata record below promotes it without the controller's
        // real reconciliation path re-spawning (and so replacing) this test's
        // hand-built fixture -- which would drop its injected manual clock
        // for a real one and report every activation already due. Only once
        // that has settled is the local role flipped directly to this broker,
        // matching what `ListOffsets` now also requires of the image.
        const OTHER_BROKER: u64 = 7;

        broker_fixture!((broker_handle, _dir, broker), allow_all);
        assert!(broker.config.node_id.get() != OTHER_BROKER);
        let logs = tempfile::tempdir().expect("log root");
        let timeline = qubit_clock::ManualMonotonicClock::new_shared();
        let clock: Arc<dyn qubit_clock::WallClock> = timeline.new_wall_clock(wall_at(NOW_MS));
        let partition = scheduled_partition()
            .dir(&logs)
            .clock(&clock)
            .topic(TOPIC)
            .activations(vec![
                crate::test_support::UnixMillis(NOW_MS - 60_000),
                crate::test_support::UnixMillis(NOW_MS + 10_000),
            ])
            .leader(OTHER_BROKER)
            .call();
        register(&broker.partitions, &partition);
        install_partition_leader_record(&broker_handle, TOPIC, broker.config.node_id.get()).await;
        partition
            .install_leader_change(broker.config.node_id.get(), 0)
            .await;
        test_ctx!(ctx, "admin");

        check!(list_partition(&broker, TOPIC, LATEST_TIMESTAMP, &ctx).await == latest_row(2));

        // Cross the activation boundary. The batch comes due on the clock
        // alone: no append and no scheduler sweep publishes the new watermark,
        // so the mirror still reports the old one.
        timeline
            .advance(std::time::Duration::from_millis(
                u64::try_from(10_000 + BOUND_MS).expect("a positive delay"),
            ))
            .expect("manual time moves forward");
        check!(partition.delivery_watermark() == krabka_log::Offset(2));

        check!(list_partition(&broker, TOPIC, LATEST_TIMESTAMP, &ctx).await == latest_row(4));

        broker_handle.shutdown().await;
    }
}
