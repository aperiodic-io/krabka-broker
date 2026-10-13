//! The reaction to a `share.version` toggle: Kafka's
//! `SharePartitionManager.onShareVersionToggle`.
//!
//! `BrokerMetadataPublisher` calls it when a new metadata image finalizes a
//! different `share.version`. At a level that no longer supports share groups
//! the manager drops its share sessions and its cached share partitions, so
//! nothing of the old level answers a later request.

use {super::SharePartitionLeaderManager, crate::features::share_groups_enabled, std::sync::Arc};

impl SharePartitionLeaderManager {
    /// Spawns the task that clears the manager when `share.version` drops
    /// below the level that supports share groups.
    ///
    /// The task compares each new image with the one before it, so a broker
    /// that never had share groups on clears nothing, and a level that stays
    /// at 0 clears once. It runs detached for the lifetime of the broker.
    pub(crate) fn spawn_share_version_watcher(self: &Arc<Self>) {
        let mgr = Arc::clone(self);
        let mut images = mgr.controller.watch_image();
        let mut enabled = share_groups_enabled(&images.borrow_and_update());
        tokio::spawn(async move {
            while images.changed().await.is_ok() {
                let now = share_groups_enabled(&images.borrow_and_update());
                if enabled && !now {
                    mgr.clear();
                }
                enabled = now;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            features::SHARE_VERSION,
            partition_registry::PartitionRegistry,
            share_partition::{manager::test_support::manager_over, state::AcquisitionState},
            test_support::FakeMetadataSource,
        },
        assert2::assert,
        krabka_log::Offset,
        krabka_metadata::{FeatureLevelRecord, MetadataImage, MetadataRecord},
        std::{sync::Arc, time::Duration},
    };

    fn image_with_share_version(level: i16) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: SHARE_VERSION.into(),
            level,
        }));
        image
    }

    /// Kafka clears the sessions and the cached share partitions when
    /// `share.version` toggles to 0, and leaves them alone otherwise.
    #[tokio::test(start_paused = true)]
    async fn a_downgrade_to_zero_clears_sessions_and_cells() {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_with_share_version(1))
                .build(),
        );
        let mgr = manager_over(source.clone(), Arc::new(PartitionRegistry::new()));
        mgr.spawn_share_version_watcher();
        let tid = uuid::Uuid::from_bytes([31; 16]);
        mgr.insert_for_test("g1", tid, 0, AcquisitionState::new(Offset(0)));
        let cells = || mgr.peek_for_test("g1", tid, 0).is_some();

        // A change that keeps share groups on clears nothing.
        source.set_image(image_with_share_version(2));
        tokio::time::sleep(Duration::from_millis(10)).await;
        let kept = cells();
        // The downgrade drops the cell.
        source.set_image(image_with_share_version(0));
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert!((kept, cells()) == (true, false));
    }
}
