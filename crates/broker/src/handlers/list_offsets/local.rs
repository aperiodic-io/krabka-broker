//! The offsets that come from a partition's own log rather than from the
//! remote tier: the `LATEST` sentinel's answer, and the leader epoch that
//! belongs with a resolved offset.
//!
//! KFC-1 makes `LATEST` depend on the topic's [`DeliveryPolicy`], which is why
//! the sentinel needs a function of its own rather than the log end offset the
//! rest of the handler reads.

use krabka_log::DeliveryPolicy;

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
pub(super) fn latest_offset(
    partition: &crate::partition::Partition,
    policy: DeliveryPolicy,
    local_end: i64,
) -> i64 {
    if policy == DeliveryPolicy::Scheduled {
        partition
            .delivery
            .publish_now(&partition.log)
            .map_or(local_end, |delivery| delivery.watermark.0)
    } else {
        local_end
    }
}

pub(super) fn leader_epoch_for_offset(partition: &crate::partition::Partition, offset: i64) -> i32 {
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
pub(super) fn epoch_start_offset(partition: &crate::partition::Partition) -> Option<i64> {
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
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_protocol::owned::{
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        list_offsets_response::ListOffsetsPartitionResponse,
    };

    use super::*;
    use crate::{
        broker::Broker,
        codes,
        delivery::test_support::{BOUND_MS, NOW_MS, wall_at},
        handlers::list_offsets::{
            handle,
            sentinels::{
                EARLIEST_LOCAL_TIMESTAMP, EARLIEST_PENDING_UPLOAD_TIMESTAMP, EARLIEST_TIMESTAMP,
                LATEST_TIERED_TIMESTAMP, LATEST_TIMESTAMP, MAX_TIMESTAMP, UNKNOWN_TIMESTAMP,
            },
            test_support::{decode_response, encode_request, test_context},
        },
        test_support::{start_broker_with_authorizer_no_audit as start_broker, test_ctx},
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
        broker_handle: &crate::broker::BrokerHandle,
        broker: &Broker,
        logs: &tempfile::TempDir,
        topic: &str,
        policy: DeliveryPolicy,
    ) {
        let clock: Arc<dyn qubit_clock::WallClock> = Arc::new(qubit_clock::StdWallClock::new());
        let partition = crate::delivery::test_support::scheduled_partition(
            logs,
            &clock,
            crate::delivery::test_support::ScheduleSetup {
                topic,
                policy,
                activations: ACTIVATIONS
                    .iter()
                    .copied()
                    .map(crate::test_support::UnixMillis)
                    .collect(),
                leader: krabka_ids::NodeId(broker.config.node_id.get()),
            },
        );
        crate::delivery::test_support::register(&broker.partitions, &partition);
        install_partition_leader_record(broker_handle, topic, broker.config.node_id.get()).await;
    }

    /// Tell `handle`'s metadata image that `leader` leads `topic`'s single
    /// partition, so the `ListOffsets` leadership gate admits a request this
    /// node answers from a locally spawned test fixture rather than from the
    /// real topic-creation path.
    async fn install_partition_leader_record(
        handle: &crate::broker::BrokerHandle,
        topic: &str,
        leader: u64,
    ) {
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
                    ..crate::handlers::test_support::replicated_partition(
                        crate::handlers::test_support::ReplicatedPartitionSetup {
                            topic,
                            leader: krabka_audit::NodeId(leader),
                            replicas: &[krabka_audit::NodeId(leader)],
                            ..Default::default()
                        },
                    )
                },
            ))
            .await
            .expect("submit partition record");
    }

    async fn list_partition(
        broker: &Broker,
        topic: &str,
        timestamp: i64,
        ctx: &crate::handlers::RequestContext<'_>,
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
        let partition = crate::delivery::test_support::scheduled_partition(
            &logs,
            &clock,
            crate::delivery::test_support::ScheduleSetup {
                topic: TOPIC,
                activations: vec![
                    crate::test_support::UnixMillis(NOW_MS - 60_000),
                    crate::test_support::UnixMillis(NOW_MS + 10_000),
                ],
                leader: krabka_ids::NodeId(OTHER_BROKER),
                ..Default::default()
            },
        );
        crate::delivery::test_support::register(&broker.partitions, &partition);
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
