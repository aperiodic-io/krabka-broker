//! `WriteBarrierMarkers`, api key 1014.
//!
//! This is the receiving side of the marker fan-out. A coordinator sends the
//! request to the broker that leads a target partition, and this handler
//! appends one marker into each named partition and returns the offset that
//! each marker took. [`transport`][crate::barrier::handlers::transport] is the
//! sending side.
//!
//! A client never sends this request. It is inter-broker traffic, and Kafka
//! gates the same kind of traffic on `ClusterAction`.
//!
//! # What the handler refuses
//!
//! - A partition that this broker does not lead takes
//!   `NOT_LEADER_OR_FOLLOWER` (6). The coordinator then reads a fresh metadata
//!   image and retries against the new leader.
//! - A partition whose installed leader or epoch differs from the metadata
//!   image takes a leadership error. This broker has not applied that exact
//!   generation, so the marker batch would carry a false leader epoch.
//!
//! The refusal is per partition. One request that names both a led and an
//! unled partition marks the first and refuses the second.
//!
//! # Fencing
//!
//! Each requested partition carries the leader epoch the coordinator resolved
//! when it froze the target set. This broker refuses a mismatch with
//! `FENCED_LEADER_EPOCH`, because a marker written under one leadership and
//! stamped with another's epoch puts a false epoch in the batch header. A
//! negative or missing epoch is malformed and is fenced too.
//!
//! # Refusing the whole request
//!
//! A caller without `ClusterAction` gets `CLUSTER_AUTHORIZATION_FAILED` on every
//! partition. A group name of more than 32767 bytes cannot go into a marker,
//! whose group string carries an `i16` length, and a compact string on the wire
//! can carry one. It gets `INVALID_REQUEST` (42) on every partition, and no
//! partition is touched.

use std::sync::atomic::Ordering;

use krabka_ids::{NodeId, PartitionIndex};
use krabka_metadata::MetadataImage;
use krabka_protocol::krabka::barrier::{
    WritableBarrierTopic, WriteBarrierMarkersRequest, WriteBarrierMarkersResponse,
    WrittenBarrierPartition, WrittenBarrierTopic,
};
use krabka_verified::{
    BarrierMarkerFenceDecision, BarrierMarkerFenceFacts, barrier_marker_fence_decision,
};
use tracing::warn;

use crate::{
    barrier::{
        handlers::cluster_action_denied,
        injection::{MarkerAppendError, append_marker},
        marker::BarrierMarker,
    },
    codes,
    coordinator::unified::persistence::MAX_STRING_BYTES,
    handlers::encode_response,
    partition::Partition,
    partition_registry::PartitionRegistry,
};

/// The `offset` of a partition row that placed no marker.
const NO_OFFSET: i64 = -1;

wire_handler!(
    handle,
    "handle_write_barrier_markers",
    "WriteBarrierMarkers",
    "debug",
    WriteBarrierMarkersRequest,
    |broker, version, req, ctx| {
        let image = broker.controller.current_image();
        let denied = cluster_action_denied(broker.config.authorizer.as_ref(), &image, ctx);
        if let Some(code) = request_refusal(denied, &req.group) {
            let topics = req
                .topics
                .iter()
                .map(|topic| refused_topic(topic, code))
                .collect();
            return encode_response(&response(topics), version);
        }

        let marker = BarrierMarker {
            group: req.group.clone(),
            epoch: req.epoch,
            triggered_at: req.triggered_at,
        };

        let mut topics = Vec::with_capacity(req.topics.len());
        for topic in &req.topics {
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for requested in &topic.partitions {
                partitions.push(
                    mark(
                        &broker.partitions,
                        &image,
                        broker.config.node_id,
                        &marker,
                        &topic.topic,
                        PartitionIndex(requested.partition),
                        requested.expected_leader_epoch,
                    )
                    .await,
                );
            }
            topics.push(WrittenBarrierTopic {
                topic: topic.topic.clone(),
                partitions,
                ..WrittenBarrierTopic::default()
            });
        }

        encode_response(&response(topics), version)
    }
);

/// The code that refuses the whole request, or `None` when it may go on.
///
/// Authorization comes first, so a caller that may not write markers learns
/// nothing about the group name.
fn request_refusal(denied: bool, group: &str) -> Option<i16> {
    if denied {
        Some(codes::CLUSTER_AUTHORIZATION_FAILED)
    } else if group.len() > MAX_STRING_BYTES {
        Some(codes::INVALID_REQUEST)
    } else {
        None
    }
}

/// Append one marker into one partition, and report the outcome.
async fn mark(
    partitions: &PartitionRegistry,
    image: &MetadataImage,
    node_id: NodeId,
    marker: &BarrierMarker,
    topic: &str,
    index: PartitionIndex,
    expected_leader_epoch: i32,
) -> WrittenBarrierPartition {
    let Some(partition) = partitions.get(topic, index) else {
        return row(index, codes::NOT_LEADER_OR_FOLLOWER, NO_OFFSET);
    };
    if let Some(code) = leadership_fault(
        &partition,
        image,
        node_id,
        topic,
        index,
        expected_leader_epoch,
    ) {
        return row(index, code, NO_OFFSET);
    }
    match append_marker(&partition, marker, node_id, expected_leader_epoch).await {
        Ok(offset) => row(index, codes::NONE, offset.get()),
        Err(MarkerAppendError::Fence(decision)) => row(index, fence_code(decision), NO_OFFSET),
        Err(MarkerAppendError::Broker(error)) => {
            warn!(
                topic,
                partition = index.get(),
                %error,
                "WriteBarrierMarkers: the marker append failed"
            );
            row(index, codes::UNKNOWN_SERVER_ERROR, NO_OFFSET)
        }
    }
}

/// The code that refuses a partition, or `None` when this broker may mark it.
/// `expected_leader_epoch` is the epoch the coordinator resolved when it froze
/// the target set. A value below zero names no generation, so the kernel
/// classifies it `Malformed` and this broker fences it.
fn leadership_fault(
    partition: &Partition,
    image: &MetadataImage,
    node_id: NodeId,
    topic: &str,
    index: PartitionIndex,
    expected_leader_epoch: i32,
) -> Option<i16> {
    let local_epoch = partition.current_leader_epoch.load(Ordering::Acquire);
    let local_leader = partition.current_leader.load(Ordering::Acquire);
    let image_partition = image.partition(topic, index.get());
    let decision = barrier_marker_fence_decision(BarrierMarkerFenceFacts {
        image_present: image_partition.is_some(),
        expected_leader: node_id.get(),
        expected_epoch: expected_leader_epoch,
        image_leader: image_partition.map_or(0, |record| record.leader.get()),
        image_epoch: image_partition.map_or(-1, |record| record.leader_epoch.get()),
        current_leader: local_leader,
        current_epoch: local_epoch,
    });
    if decision == BarrierMarkerFenceDecision::Append {
        None
    } else {
        Some(fence_code(decision))
    }
}

fn fence_code(decision: BarrierMarkerFenceDecision) -> i16 {
    match decision {
        BarrierMarkerFenceDecision::NotLeader => codes::NOT_LEADER_OR_FOLLOWER,
        BarrierMarkerFenceDecision::Malformed | BarrierMarkerFenceDecision::FencedEpoch => {
            codes::FENCED_LEADER_EPOCH
        }
        BarrierMarkerFenceDecision::Append => codes::NONE,
    }
}

/// One partition row of the response.
fn row(index: PartitionIndex, error_code: i16, offset: i64) -> WrittenBarrierPartition {
    WrittenBarrierPartition {
        partition: index.get(),
        error_code,
        offset,
        ..WrittenBarrierPartition::default()
    }
}

/// Every partition of one requested topic, stamped with one code.
fn refused_topic(topic: &WritableBarrierTopic, error_code: i16) -> WrittenBarrierTopic {
    WrittenBarrierTopic {
        topic: topic.topic.clone(),
        partitions: topic
            .partitions
            .iter()
            .map(|requested| row(PartitionIndex(requested.partition), error_code, NO_OFFSET))
            .collect(),
        ..WrittenBarrierTopic::default()
    }
}

/// The response around a list of topic rows.
fn response(topics: Vec<WrittenBarrierTopic>) -> WriteBarrierMarkersResponse {
    WriteBarrierMarkersResponse {
        topics,
        ..WriteBarrierMarkersResponse::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_ids::{NodeId, PartitionIndex};
    use krabka_log::Offset;
    use krabka_metadata::{MetadataImage, MetadataRecord};
    use krabka_protocol::krabka::barrier::{
        WritableBarrierPartition, WritableBarrierTopic, WrittenBarrierPartition,
        WrittenBarrierTopic,
    };
    use krabka_units::mebibytes;
    use tempfile::tempdir;
    use uuid::Uuid;

    /// A malformed epoch value that the receiving broker must fence.
    const NO_EXPECTED_LEADER_EPOCH: i32 = -1;

    use super::{NO_OFFSET, mark, refused_topic, request_refusal, row};
    use crate::{
        barrier::{
            marker::BarrierMarker,
            test_support::{open_partition, topic_records},
        },
        codes,
        coordinator::unified::persistence::MAX_STRING_BYTES,
        partition_registry::PartitionRegistry,
    };

    const LOCAL: NodeId = NodeId(1);

    async fn mark_orders_zero(
        registry: &PartitionRegistry,
        image: &MetadataImage,
        leader_epoch: i32,
    ) -> WrittenBarrierPartition {
        mark(
            registry,
            image,
            LOCAL,
            &marker(),
            "orders",
            PartitionIndex(0),
            leader_epoch,
        )
        .await
    }

    fn marker() -> BarrierMarker {
        BarrierMarker {
            group: "orders-cut".to_owned(),
            epoch: 4,
            triggered_at: 1_724_500_000_000,
        }
    }

    fn image(records: &[MetadataRecord]) -> MetadataImage {
        MetadataImage::from_records(Uuid::nil(), records)
    }

    /// A registry with `orders-0` open, and the local broker installed as its
    /// leader at `leader_epoch`.
    async fn registry_with_leader(
        dir: &std::path::Path,
        leader: NodeId,
        leader_epoch: i32,
    ) -> PartitionRegistry {
        let registry = PartitionRegistry::new();
        open_partition(
            &registry,
            dir,
            crate::test_support::StandalonePartitionSetup::default(),
        );
        let partition = registry
            .get("orders", PartitionIndex(0))
            .expect("the partition is open");
        partition
            .install_leader_change(leader.get(), leader_epoch)
            .await;
        registry
    }

    async fn led_orders_fixture() -> (tempfile::TempDir, PartitionRegistry, MetadataImage) {
        let dir = tempdir().expect("tempdir");
        let registry = registry_with_leader(dir.path(), LOCAL, 3).await;
        let image = image(&topic_records("orders", 1, LOCAL));
        (dir, registry, image)
    }

    async fn check_not_led_here(registry: &PartitionRegistry, image: &MetadataImage) {
        let written = mark_orders_zero(registry, image, 3).await;
        check!(written == row(PartitionIndex(0), codes::NOT_LEADER_OR_FOLLOWER, NO_OFFSET));
    }

    /// The coordinator froze its target set against one leadership and this
    /// broker has since installed another. The marker would carry an epoch
    /// that is already superseded, so the request is refused and the
    /// coordinator comes back with a fresh image.
    #[tokio::test]
    async fn a_stale_expected_leader_epoch_is_fenced() {
        let (_dir, registry, image) = led_orders_fixture().await;

        let cases = [
            (
                "the epoch the coordinator saw is older",
                2,
                codes::FENCED_LEADER_EPOCH,
            ),
            (
                "the epoch the coordinator saw is newer",
                4,
                codes::FENCED_LEADER_EPOCH,
            ),
            ("the epoch matches", 3, codes::NONE),
            (
                "the coordinator supplied a malformed epoch",
                NO_EXPECTED_LEADER_EPOCH,
                codes::FENCED_LEADER_EPOCH,
            ),
        ];
        for (case, expected_epoch, code) in cases {
            let written = mark(
                &registry,
                &image,
                LOCAL,
                &marker(),
                "orders",
                PartitionIndex(0),
                expected_epoch,
            )
            .await;
            check!(written.error_code == code, "{case}");
        }
    }

    #[tokio::test]
    async fn a_partition_that_is_not_open_here_is_not_led_here() {
        let registry = PartitionRegistry::new();
        let image = image(&topic_records("orders", 1, LOCAL));
        check_not_led_here(&registry, &image).await;
    }

    #[tokio::test]
    async fn a_partition_that_another_broker_leads_is_refused() {
        let dir = tempdir().expect("tempdir");
        let registry = registry_with_leader(dir.path(), NodeId(2), 3).await;
        let image = image(&topic_records("orders", 1, NodeId(2)));
        check_not_led_here(&registry, &image).await;
    }

    #[tokio::test]
    async fn a_leader_epoch_below_the_image_is_fenced() {
        let dir = tempdir().expect("tempdir");
        // `topic_records` builds the image at leader epoch 3, and this replica
        // still carries epoch 2.
        let registry = registry_with_leader(dir.path(), LOCAL, 2).await;
        let image = image(&topic_records("orders", 1, LOCAL));
        let written = mark_orders_zero(&registry, &image, NO_EXPECTED_LEADER_EPOCH).await;
        check!(written == row(PartitionIndex(0), codes::FENCED_LEADER_EPOCH, NO_OFFSET));
    }

    #[tokio::test]
    async fn a_led_partition_takes_the_marker_and_returns_its_offset() {
        let (_dir, registry, image) = led_orders_fixture().await;
        let written = mark_orders_zero(&registry, &image, 3).await;
        check!(written == row(PartitionIndex(0), codes::NONE, 0));

        // The record at the returned offset is the marker the request named.
        let partition = registry
            .get("orders", PartitionIndex(0))
            .expect("the partition is open");
        let read = partition
            .read_log(Offset(0), mebibytes(1))
            .expect("read the log back");
        check!(read.batches.len() == 1);
        let batch = &read.batches[0];
        crate::barrier::test_support::check_control_marker(batch, &marker());
    }

    /// Authorization answers first, so a caller that may not write markers
    /// learns nothing about the group name. A group name over 32767 bytes cannot
    /// go into a marker, and it refuses the whole request.
    #[test]
    fn a_request_is_refused_whole_for_a_denial_or_a_group_over_32767_bytes() {
        let longest = "g".repeat(MAX_STRING_BYTES);
        let too_long = "g".repeat(MAX_STRING_BYTES + 1);
        let cases = [
            ("allowed, usual name", false, "orders-cut", None),
            ("allowed, longest name", false, longest.as_str(), None),
            (
                "allowed, name one byte too long",
                false,
                too_long.as_str(),
                Some(codes::INVALID_REQUEST),
            ),
            (
                "denied, usual name",
                true,
                "orders-cut",
                Some(codes::CLUSTER_AUTHORIZATION_FAILED),
            ),
            (
                "denied, name one byte too long",
                true,
                too_long.as_str(),
                Some(codes::CLUSTER_AUTHORIZATION_FAILED),
            ),
        ];
        for (case, denied, group, expected) in cases {
            check!(request_refusal(denied, group) == expected, "{case}");
        }
    }

    /// The whole-request check is the first line of defence. A marker whose
    /// group still cannot be written is an error row and appends nothing.
    #[tokio::test]
    async fn a_marker_that_cannot_be_encoded_is_an_error_row_and_appends_nothing() {
        let (_dir, registry, image) = led_orders_fixture().await;
        let marker = BarrierMarker {
            group: "g".repeat(MAX_STRING_BYTES + 1),
            ..marker()
        };
        let written = mark(
            &registry,
            &image,
            LOCAL,
            &marker,
            "orders",
            PartitionIndex(0),
            3,
        )
        .await;
        check!(written == row(PartitionIndex(0), codes::UNKNOWN_SERVER_ERROR, NO_OFFSET));
        let partition = registry
            .get("orders", PartitionIndex(0))
            .expect("the partition is open");
        check!(partition.log_end_offset() == Offset(0));
    }

    #[test]
    fn a_denied_request_stamps_every_named_partition() {
        let topic = WritableBarrierTopic {
            topic: "orders".to_owned(),
            partitions: vec![
                WritableBarrierPartition {
                    partition: 0,
                    ..WritableBarrierPartition::default()
                },
                WritableBarrierPartition {
                    partition: 2,
                    ..WritableBarrierPartition::default()
                },
            ],
            ..WritableBarrierTopic::default()
        };
        let expected = WrittenBarrierTopic {
            topic: "orders".to_owned(),
            partitions: vec![
                row(
                    PartitionIndex(0),
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    NO_OFFSET,
                ),
                row(
                    PartitionIndex(2),
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    NO_OFFSET,
                ),
            ],
            ..WrittenBarrierTopic::default()
        };
        check!(refused_topic(&topic, codes::CLUSTER_AUTHORIZATION_FAILED) == expected);
    }
}
