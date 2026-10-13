//! A thin adapter that lets the broker's [`crate::metadata_source::MetadataSource`]
//! satisfy the narrower controller traits the auto-rebalance, reassignment,
//! delegation-token, and break-glass sweeps depend on. Each trait impl is a
//! mechanical forward of the same handle.

#[cfg(test)]
use crate::test_support::FakeMetadataSource;
use {
    crate::{delegation_token_cleanup::DelegationTokenController, metadata_source::MetadataSource},
    krabka_raft::NodeId,
    std::sync::Arc,
};

/// Wraps a real [`krabka_raft::ControllerHandle`] so it can satisfy the
/// controller traits of the broker's background tasks:
///
/// - [`crate::leader_rebalance::ControllerLike`] for auto-rebalance.
/// - [`crate::reassignment::ReassignmentController`] for reassignment
///   completion.
/// - [`crate::delegation_token_cleanup::DelegationTokenController`] for the
///   KIP-48 delegation-token expiry sweep.
/// - [`crate::break_glass::sweep::BreakGlassController`] for the KFC-9
///   break-glass expiry sweep.
///
/// Every broker runs both expiry sweeps. Raft serializes duplicate
/// tombstones, so each one after the first is a no-op on the apply path.
#[derive(krabka_macros::BackgroundControllers)]
pub(super) struct ControllerAdapter {
    pub(super) handle: Arc<dyn MetadataSource>,
    pub(super) node_id: NodeId,
}

impl ControllerAdapter {
    fn holds_leadership(&self) -> bool {
        *self.handle.watch_leader().borrow() == Some(self.node_id)
    }

    async fn submit(&self, records: Vec<krabka_metadata::MetadataRecord>) -> Result<(), String> {
        self.handle
            .submit_change(records)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl DelegationTokenController for ControllerAdapter {
    fn current_image(&self) -> Arc<krabka_metadata::MetadataImage> { self.handle.current_image() }

    async fn submit_mutations(
        &self,
        mutations: Vec<krabka_raft::DelegationTokenMutation>,
    ) -> Result<(), String> {
        self.handle
            .submit_delegation_token_mutations(mutations)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            break_glass::sweep::BreakGlassController,
            broker::test_support::{fake_source, metadata_topic_record},
            delegation_token_cleanup::DelegationTokenController,
            leader_rebalance::ControllerLike,
            metadata_source::MetadataSource,
            reassignment::ReassignmentController,
        },
        assert2::assert,
    };

    fn adapter(source: &Arc<dyn MetadataSource>, node: u64) -> ControllerAdapter {
        ControllerAdapter {
            handle: Arc::clone(source),
            node_id: krabka_raft::NodeId(node),
        }
    }

    #[test]
    fn controller_adapter_reports_leadership_from_leader_watch() {
        let fake = Arc::new(fake_source(
            krabka_metadata::MetadataImage::new(uuid::Uuid::from_u128(1)),
            Some(krabka_raft::NodeId(7)),
        ));
        let source: Arc<dyn MetadataSource> = fake.clone();
        let leader = adapter(&source, 7);
        let follower = adapter(&source, 8);

        assert!(crate::leader_rebalance::ControllerLike::is_leader(&leader));
        assert!(!crate::leader_rebalance::ControllerLike::is_leader(
            &follower
        ));
        assert!(crate::reassignment::ReassignmentController::is_leader(
            &leader
        ));
        assert!(!crate::reassignment::ReassignmentController::is_leader(
            &follower
        ));
        fake.set_leader(Some(NodeId(8)));
        assert!(!ControllerLike::is_leader(&leader));
        assert!(!ReassignmentController::is_leader(&leader));
        assert!(ControllerLike::is_leader(&follower));
        assert!(ReassignmentController::is_leader(&follower));
        fake.set_leader(None);
        assert!(!ControllerLike::is_leader(&follower));
        assert!(!ReassignmentController::is_leader(&follower));
    }

    #[tokio::test]
    async fn controller_adapter_forwards_current_image() {
        let cluster_id = uuid::Uuid::from_u128(0x5150);
        let fake = Arc::new(fake_source(
            krabka_metadata::MetadataImage::new(cluster_id),
            Some(krabka_raft::NodeId(1)),
        ));
        let source: Arc<dyn MetadataSource> = fake.clone();
        let adapter = adapter(&source, 1);

        let images = [
            ControllerLike::current_image(&adapter),
            ReassignmentController::current_image(&adapter),
            DelegationTokenController::current_image(&adapter),
            BreakGlassController::current_image(&adapter),
        ];
        for image in images {
            assert!(image.cluster_id() == cluster_id);
            assert!(Arc::ptr_eq(&image, &source.current_image()));
        }
        let mut reassignment_rx = ReassignmentController::watch_image(&adapter);
        assert!(reassignment_rx.borrow().cluster_id() == cluster_id);
        let applied = Arc::new(krabka_metadata::MetadataImage::new(uuid::Uuid::from_u128(
            0x5151,
        )));
        fake.set_image(Arc::clone(&applied));
        reassignment_rx
            .changed()
            .await
            .expect("the forwarded watch sees the applied image");
        assert!(Arc::ptr_eq(&reassignment_rx.borrow_and_update(), &applied));
        for image in [
            ControllerLike::current_image(&adapter),
            ReassignmentController::current_image(&adapter),
            DelegationTokenController::current_image(&adapter),
            BreakGlassController::current_image(&adapter),
        ] {
            assert!(Arc::ptr_eq(&image, &applied));
        }
    }

    #[tokio::test]
    async fn controller_adapter_forwards_successful_batches_in_order() {
        let fake = Arc::new(fake_source(
            krabka_metadata::MetadataImage::new(uuid::Uuid::from_u128(1)),
            Some(NodeId(1)),
        ));
        let source: Arc<dyn MetadataSource> = fake.clone();
        let adapter = adapter(&source, 1);
        let records = [
            metadata_topic_record("rebalance", uuid::Uuid::from_u128(1)),
            metadata_topic_record("reassignment", uuid::Uuid::from_u128(2)),
            metadata_topic_record("sweep", uuid::Uuid::from_u128(3)),
        ];
        for result in [
            ControllerLike::submit_change(&adapter, vec![records[0].clone()]).await,
            ReassignmentController::submit_change(&adapter, vec![records[1].clone()]).await,
            BreakGlassController::submit_change(&adapter, vec![records[2].clone()]).await,
        ] {
            assert!(result == Ok(()));
        }
        assert!(
            fake.submitted()
                == records
                    .into_iter()
                    .map(|record| vec![record])
                    .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn controller_adapter_forwards_submit_errors() {
        // The only site that needs a rejecting write: each trait must
        // surface the controller's error rather than swallow it into `Ok`.
        let source: Arc<dyn MetadataSource> = Arc::new(
            FakeMetadataSource::builder()
                .image(krabka_metadata::MetadataImage::new(uuid::Uuid::from_u128(
                    1,
                )))
                .leader(Some(krabka_raft::NodeId(1)))
                .on_submit(|_| Err(krabka_raft::RaftError::Unsupported("adapter test")))
                .build(),
        );
        let record =
            metadata_topic_record("adapter-submit-mutant-topic", uuid::Uuid::from_u128(0xADAD));
        let adapter = adapter(&source, 1);

        let results = [
            ControllerLike::submit_change(&adapter, vec![record.clone()]).await,
            ReassignmentController::submit_change(&adapter, vec![record.clone()]).await,
            BreakGlassController::submit_change(&adapter, vec![record]).await,
        ];
        let expected = krabka_raft::RaftError::Unsupported("adapter test").to_string();
        for result in results {
            assert!(result == Err(expected.clone()));
        }
        let expected = source
            .submit_delegation_token_mutations(Vec::new())
            .await
            .expect_err("the fake source does not support token mutations")
            .to_string();
        assert!(
            DelegationTokenController::submit_mutations(&adapter, Vec::new()).await
                == Err(expected)
        );
    }
}
