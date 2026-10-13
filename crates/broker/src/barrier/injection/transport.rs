//! The seam that carries a marker to the broker that leads the partition.
//!
//! A coordinator marks the partitions it leads itself. Every other partition of
//! the group belongs to another broker, and the marker reaches it through
//! [`RemoteMarkerWriter`]. The broker binds the seam to the
//! `WriteBarrierMarkers` inter-broker request, and a test binds a mock, so the
//! fan-out is exercised without a network.

use {
    crate::{
        barrier::{marker::BarrierMarker, state::TargetPartition},
        error::BrokerError,
    },
    async_trait::async_trait,
    krabka_log::Offset,
    krabka_metadata::NodeId,
};

/// One marker that an append placed, and the offset it took.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MarkerPlacement {
    pub(crate) target: TargetPartition,
    pub(crate) offset: Offset,
}

/// The leg of a marker fan-out that leaves this broker.
///
/// The broker binds this to the `WriteBarrierMarkers` inter-broker request. A
/// coordinator with no binding marks only the partitions it leads, and every
/// other partition of the group lands in the `missing` list of the cut.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub(crate) trait RemoteMarkerWriter: Send + Sync {
    /// Append `marker` into every partition of `targets`, which `leader`
    /// leads.
    ///
    /// The result names the offset of each marker the remote broker placed. A
    /// target that is absent from the result carries no marker, and the
    /// fan-out retries it.
    ///
    /// # Errors
    /// Returns a [`BrokerError`] when the request to `leader` fails. The
    /// fan-out retries every target of that leader.
    async fn write_markers(
        &self,
        leader: NodeId,
        marker: &BarrierMarker,
        targets: &[TargetPartition],
    ) -> Result<Vec<MarkerPlacement>, BrokerError>;
}
