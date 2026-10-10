//! A thin adapter that lets the broker's [`crate::metadata_source::MetadataSource`]
//! satisfy the narrower controller traits the auto-rebalance, reassignment,
//! delegation-token, and break-glass sweeps depend on. Each trait impl is a
//! mechanical forward of the same handle.

use std::sync::Arc;

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
pub(super) struct ControllerAdapter {
    pub(super) handle: Arc<dyn crate::metadata_source::MetadataSource>,
    pub(super) node_id: krabka_raft::NodeId,
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

/// Metadata task traits share the same authority and submit error conversion.
macro_rules! controller_adapter {
    ($trait:path, $($extra:item),* $(,)?) => {
        #[async_trait::async_trait]
        impl $trait for ControllerAdapter {
            fn current_image(&self) -> Arc<krabka_metadata::MetadataImage> {
                self.handle.current_image()
            }
            async fn submit_change(&self, records: Vec<krabka_metadata::MetadataRecord>) -> Result<(), String> {
                self.submit(records).await
            }
            $($extra)*
        }
    };
}

controller_adapter!(
    crate::leader_rebalance::ControllerLike,
    fn is_leader(&self) -> bool {
        self.holds_leadership()
    }
);
controller_adapter!(
    crate::reassignment::ReassignmentController,
    fn is_leader(&self) -> bool {
        self.holds_leadership()
    },
    fn watch_image(&self) -> tokio::sync::watch::Receiver<Arc<krabka_metadata::MetadataImage>> {
        self.handle.watch_image()
    }
);

#[async_trait::async_trait]
impl crate::delegation_token_cleanup::DelegationTokenController for ControllerAdapter {
    fn current_image(&self) -> Arc<krabka_metadata::MetadataImage> {
        self.handle.current_image()
    }

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

controller_adapter!(crate::break_glass::sweep::BreakGlassController,);

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::broker::test_support::{fake_source, metadata_topic_record};

    fn adapter(
        source: &Arc<dyn crate::metadata_source::MetadataSource>,
        node: u64,
    ) -> ControllerAdapter {
        ControllerAdapter {
            handle: Arc::clone(source),
            node_id: krabka_raft::NodeId(node),
        }
    }

    #[test]
    fn controller_adapter_reports_leadership_from_leader_watch() {
        let source: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(fake_source(
            krabka_metadata::MetadataImage::new(uuid::Uuid::from_u128(1)),
            Some(krabka_raft::NodeId(7)),
        ));
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
    }

    #[test]
    fn controller_adapter_forwards_current_image() {
        let cluster_id = uuid::Uuid::from_u128(0x5150);
        let source: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(fake_source(
            krabka_metadata::MetadataImage::new(cluster_id),
            Some(krabka_raft::NodeId(1)),
        ));
        let adapter = adapter(&source, 1);

        let images = [
            crate::leader_rebalance::ControllerLike::current_image(&adapter),
            crate::reassignment::ReassignmentController::current_image(&adapter),
            crate::delegation_token_cleanup::DelegationTokenController::current_image(&adapter),
            crate::break_glass::sweep::BreakGlassController::current_image(&adapter),
        ];
        for image in images {
            assert!(image.cluster_id() == cluster_id);
        }
        let reassignment_rx = crate::reassignment::ReassignmentController::watch_image(&adapter);
        assert!(reassignment_rx.borrow().cluster_id() == cluster_id);
    }

    #[tokio::test]
    async fn controller_adapter_forwards_submit_errors() {
        // The only site that needs a rejecting write: each trait must
        // surface the controller's error rather than swallow it into `Ok`.
        let source: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(
            crate::test_support::FakeMetadataSource::builder()
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
            crate::leader_rebalance::ControllerLike::submit_change(&adapter, vec![record.clone()])
                .await,
            crate::reassignment::ReassignmentController::submit_change(
                &adapter,
                vec![record.clone()],
            )
            .await,
            crate::break_glass::sweep::BreakGlassController::submit_change(&adapter, vec![record])
                .await,
            crate::delegation_token_cleanup::DelegationTokenController::submit_mutations(
                &adapter,
                Vec::new(),
            )
            .await,
        ];
        for result in results {
            assert!(result.is_err());
        }
    }
}
