//! The fixtures the `BrokerHeartbeat` controller-side tests share: a
//! `MetadataSource` that captures submitted records, a one-partition metadata
//! image, and a populated liveness state.
//!
//! The offline-log-dir failover tests and the controlled-shutdown drain tests
//! drive the same shapes, so the fixtures live in one module rather than once
//! per test file.

use std::sync::Arc;

use krabka_metadata::{MetadataImage, NodeId};

use crate::{
    heartbeat::controller_state::ControllerLivenessState, test_support::FakeMetadataSource,
};

/// A metadata source over `image`, with this node as the controller leader so
/// that the failover and drain paths act rather than defer. What they submit
/// is captured for the assertions.
pub(super) fn fake_source(image: MetadataImage) -> Arc<FakeMetadataSource> {
    Arc::new(
        FakeMetadataSource::builder()
            .image(image)
            .leader(Some(NodeId(1)))
            .build(),
    )
}

pub(super) use crate::leader_election::test_support::{
    ElectionSetup, img_with_partition as image_with_dir_partition,
};

pub(super) async fn liveness_with(alive: &[NodeId]) -> Arc<ControllerLivenessState> {
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for &n in alive {
        l.record_heartbeat(n.0).await;
    }
    Arc::new(l)
}
