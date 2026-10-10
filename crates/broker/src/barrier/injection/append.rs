//! The one local append that every barrier marker goes through.
//!
//! Both the coordinator's own fan-out and the `WriteBarrierMarkers` handler
//! land here, so a marker that a remote coordinator asks for takes exactly the
//! batch shape of one this broker placed itself.

use krabka_ids::NodeId;
use krabka_log::Offset;
use krabka_verified::{
    BarrierMarkerFenceDecision, BarrierMarkerFenceFacts, barrier_marker_fence_decision,
};

use crate::{
    barrier::marker::{BarrierMarker, build_barrier_batch},
    error::BrokerError,
    partition::Partition,
};

/// A marker append rejected by the leadership fence or by the partition
/// writer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum MarkerAppendError {
    #[error("barrier marker append was fenced: {0:?}")]
    Fence(BarrierMarkerFenceDecision),
    #[error(transparent)]
    Broker(#[from] BrokerError),
}

/// Append one barrier marker to a local partition, and return its offset.
///
/// The batch carries the expected leader epoch after the installed partition
/// matches it. The writer does not stamp the epoch, and a default of zero can
/// be false in the header.
///
/// The `WriteBarrierMarkers` handler appends through this function too, so a
/// marker that a remote coordinator asks for takes the same batch shape as one
/// this broker's own coordinator places.
///
/// # Errors
/// Returns a [`MarkerAppendError`] when the installed leadership generation
/// differs from the expected one, when the group name is longer than 32767
/// bytes and so cannot go into a marker, when the partition writer is gone, or
/// when the log rejects the batch.
pub(crate) async fn append_marker(
    partition: &Partition,
    marker: &BarrierMarker,
    expected_leader: NodeId,
    expected_epoch: i32,
) -> Result<Offset, MarkerAppendError> {
    // The read guard linearizes this admission and append with metadata's
    // write-locked leader transition. A marker cannot pass the fence and then
    // enter the writer after another generation is installed.
    let installed = partition.lock_produce_transition().await;
    let decision = barrier_marker_fence_decision(BarrierMarkerFenceFacts {
        image_present: true,
        expected_leader: expected_leader.get(),
        expected_epoch,
        image_leader: expected_leader.get(),
        image_epoch: expected_epoch,
        current_leader: installed.leader_node_id.0,
        current_epoch: installed.leader_epoch.get(),
    });
    if decision != BarrierMarkerFenceDecision::Append {
        return Err(MarkerAppendError::Fence(decision));
    }
    let batch = build_barrier_batch(marker, partition.log_end_offset(), expected_epoch)?;
    Ok(partition.produce_control_batch(batch).await?)
}

#[cfg(test)]
mod tests {
    use krabka_ids::PartitionIndex;

    use super::*;
    use crate::{
        barrier::test_support::open_partition, coordinator::unified::persistence::MAX_STRING_BYTES,
        partition_registry::PartitionRegistry,
    };

    async fn leader_partition(dir: &std::path::Path) -> std::sync::Arc<Partition> {
        let registry = PartitionRegistry::new();
        open_partition(
            &registry,
            dir,
            crate::test_support::StandalonePartitionSetup {
                topic: "test-barrier",
                ..Default::default()
            },
        );
        let partition = registry.get("test-barrier", PartitionIndex(0)).unwrap();
        partition.install_leader_change(1, 5).await;
        partition
    }

    /// The marker value carries the group name with an `i16` length. A name of
    /// 32767 bytes goes into the log, and one of 32768 bytes is an error that
    /// appends nothing. The marker can reach here from a `WriteBarrierMarkers`
    /// request, so a panic would take down that connection.
    #[tokio::test]
    async fn a_group_of_32768_bytes_is_refused_and_appends_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let partition = leader_partition(dir.path()).await;

        for (length, appended) in [(MAX_STRING_BYTES, true), (MAX_STRING_BYTES + 1, false)] {
            let marker = BarrierMarker {
                group: "g".repeat(length),
                epoch: 1,
                triggered_at: 1000,
            };
            let result = append_marker(&partition, &marker, NodeId(1), 5).await;
            assert2::check!(result.is_ok() == appended, "{length} bytes");
        }

        assert2::check!(partition.log_end_offset() == Offset(1));
    }

    #[tokio::test]
    async fn append_marker_appends_when_fencing_matches_and_fences_when_mismatched() {
        let dir = tempfile::tempdir().unwrap();
        let partition = leader_partition(dir.path()).await;

        let marker = BarrierMarker {
            group: "bg-1".into(),
            epoch: 1,
            triggered_at: 1000,
        };

        let err = append_marker(&partition, &marker, NodeId(1), 4).await;
        assert2::check!(matches!(err, Err(MarkerAppendError::Fence(_))));

        let off1 = append_marker(&partition, &marker, NodeId(1), 5)
            .await
            .expect("first append");
        assert2::check!(off1 == Offset(0));

        let off2 = append_marker(&partition, &marker, NodeId(1), 5)
            .await
            .expect("second append");
        assert2::check!(off2 == Offset(1));
    }
}
