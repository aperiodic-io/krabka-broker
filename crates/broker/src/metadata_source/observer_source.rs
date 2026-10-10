//! The broker-only [`MetadataSource`]: reads come from a `MetadataObserver`
//! and writes go out through a [`MetadataWriter`]. It lives apart from the
//! controller implementation because every controller-only operation here is a
//! deliberate refusal rather than a delegation.

use std::{collections::BTreeSet, net::SocketAddr, sync::Arc};

use krabka_metadata::{MetadataImage, MetadataRecord};
use krabka_raft::{
    AddVoter, DelegationTokenMutation, Node, NodeId, QuorumState, RaftError, ReconfigOutcome,
    RemoveVoter, SnapshotRange, SubmitChangeResult, UpdateVoter,
};
use tokio::sync::watch;

use super::{MetadataSource, MetadataWriter};
use crate::metadata_observer::MetadataObserver;

/// Broker-only metadata source: reads from a [`MetadataObserver`], writes
/// by forwarding to the controller quorum.
pub struct ObserverSource {
    observer: Arc<MetadataObserver>,
    writer: Arc<dyn MetadataWriter>,
}

impl ObserverSource {
    #[must_use]
    pub fn new(observer: Arc<MetadataObserver>, writer: Arc<dyn MetadataWriter>) -> Self {
        Self { observer, writer }
    }
}

#[async_trait::async_trait]
impl MetadataSource for ObserverSource {
    fn current_image(&self) -> Arc<MetadataImage> {
        self.observer.current_image()
    }
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.observer.watch_image()
    }
    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.observer.watch_leader()
    }
    fn current_metadata_offset(&self) -> i64 {
        self.observer.current_metadata_offset()
    }
    fn quorum_committed_offset(&self) -> i64 {
        self.observer.quorum_committed_offset()
    }
    fn last_applied_record_timestamp_ms(&self) -> i64 {
        self.observer.last_applied_record_timestamp_ms()
    }
    fn watch_fatal(&self) -> watch::Receiver<Option<String>> {
        self.observer.watch_fatal()
    }
    fn metadata_load_error_count(&self) -> u64 {
        self.observer.metadata_load_error_count()
    }
    fn pending_record_processing_time_us(&self) -> (u64, u64) {
        self.observer.pending_record_processing_time_us()
    }
    fn quorum_state(&self) -> QuorumState {
        // A broker-only node is not a voter and has no openraft state of its
        // own, so only `current_leader` is meaningful here. DescribeQuorum
        // and KIP-853 voter admin RPCs are forwarded to the controller.
        QuorumState {
            current_term: 0,
            current_leader: *self.observer.watch_leader().borrow(),
            ..Default::default()
        }
    }
    fn current_controller_epoch(&self) -> Option<u64> {
        None
    }
    async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError> {
        self.writer.submit_change(records).await
    }
    async fn submit_delegation_token_mutations(
        &self,
        mutations: Vec<DelegationTokenMutation>,
    ) -> Result<SubmitChangeResult, RaftError> {
        self.writer
            .submit_delegation_token_mutations(mutations)
            .await
    }
    async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
        Err(RaftError::NotLeader {
            current_leader: None,
        })
    }
    async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
        Err(RaftError::NotLeader {
            current_leader: None,
        })
    }
    fn controller_bound_addr(&self) -> SocketAddr {
        // A broker-only node runs no controller listener. The only callers
        // (DescribeQuorum / KIP-853 reconfiguration) live on controllers, so
        // this is never reached in practice; report an unspecified address.
        SocketAddr::from(([0, 0, 0, 0], 0))
    }
    fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
        // A broker-only observer keeps a checkpoint to resume from, but it is
        // not a member of the metadata quorum and does not serve it:
        // FetchSnapshot is answered by the controller quorum.
        SnapshotRange::NoSnapshot
    }
    async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        Err(RaftError::NotLeader {
            current_leader: None,
        })
    }
    async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
        let current_leader = *self.observer.watch_leader().borrow();
        Err(RaftError::NotLeader { current_leader })
    }
    async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
        let current_leader = *self.observer.watch_leader().borrow();
        Err(RaftError::NotLeader { current_leader })
    }
    async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
        let current_leader = *self.observer.watch_leader().borrow();
        Err(RaftError::NotLeader { current_leader })
    }
    async fn forward_raw(
        &self,
        api_key: i16,
        version: i16,
        body: bytes::Bytes,
    ) -> Option<Result<bytes::Bytes, RaftError>> {
        Some(self.writer.forward_raw(api_key, version, body).await)
    }
    async fn cancel(&self) {
        self.observer.cancel().await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use krabka_raft::{BootstrapMode, Controller, ControllerConfig};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::{
        metadata_observer::test_support::observer_config,
        metadata_source::test_support::{topic_record, wait_for_controller_leader},
    };

    struct RecordingWriter {
        calls: Mutex<Vec<Vec<MetadataRecord>>>,
    }

    #[async_trait::async_trait]
    impl MetadataWriter for RecordingWriter {
        async fn submit_change(
            &self,
            records: Vec<MetadataRecord>,
        ) -> Result<SubmitChangeResult, RaftError> {
            self.calls.lock().unwrap().push(records);
            Ok(SubmitChangeResult::default())
        }
        async fn submit_delegation_token_mutations(
            &self,
            _mutations: Vec<DelegationTokenMutation>,
        ) -> Result<SubmitChangeResult, RaftError> {
            Ok(SubmitChangeResult::default())
        }
    }

    fn not_leader_none<T>(result: &Result<T, RaftError>) {
        assert2::assert!(matches!(
            result,
            Err(RaftError::NotLeader {
                current_leader: None
            })
        ));
    }

    #[tokio::test]
    async fn observer_source_uses_observer_writer_and_denies_controller_only_ops() {
        let cluster_id = Uuid::new_v4();
        let dir = TempDir::new().unwrap();
        let observer = crate::metadata_observer::MetadataObserver::start(
            crate::metadata_observer::ObserverConfig {
                client_id: "observer-source-test".into(),
                ..observer_config(cluster_id, dir.path().to_path_buf())
            },
        );
        let writer = Arc::new(RecordingWriter {
            calls: Mutex::new(Vec::new()),
        });
        let source = ObserverSource::new(observer.clone(), writer.clone());

        assert2::assert!((source.current_image().cluster_id()) == (cluster_id));
        assert2::assert!((source.current_metadata_offset()) == (-1));
        // An observer that has reached no controller reports the quorum's
        // committed offset as unknown rather than as caught up: the readiness
        // probe must not read "no lag" out of "no contact". Where the two
        // offsets part company -- a responder behind the quorum -- is covered
        // in `metadata_observer::serve_loop`.
        assert2::assert!((source.quorum_committed_offset()) == (-1));
        source
            .submit_change(vec![topic_record("forwarded-topic")])
            .await
            .expect("submit via writer");
        {
            let calls = writer.calls.lock().unwrap();
            assert2::assert!((calls.len()) == (1));
            assert2::assert!(
                matches!(&calls[0][0], MetadataRecord::V1Topic(t) if t.name == "forwarded-topic")
            );
        }

        not_leader_none(&source.change_membership(BTreeSet::new()).await);
        not_leader_none(&source.add_learner(NodeId(2), Node::default()).await);
        not_leader_none(&source.trigger_snapshot().await);
        source.cancel().await;
        assert2::assert!(observer.task_drained_for_test().await);
    }

    #[tokio::test]
    async fn observer_source_reports_the_replicated_metadata_offset() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("controller");
        wait_for_controller_leader(&ctrl).await;
        ctrl.submit_change(vec![topic_record("observed-through-source")])
            .await
            .expect("submit metadata");
        ctrl.submit_change(vec![topic_record("observed-through-source-2")])
            .await
            .expect("submit second metadata record");
        ctrl.submit_change(vec![topic_record("observed-through-source-3")])
            .await
            .expect("submit third metadata record");
        let expected_offset = i64::try_from(ctrl.quorum_state().last_applied_index)
            .unwrap_or(i64::MAX)
            .saturating_sub(1);
        assert2::assert!(
            ![-1, 0, 1].contains(&expected_offset),
            "test must distinguish every constant-offset mutant"
        );

        let observer_dir = TempDir::new().unwrap();
        let observer = crate::metadata_observer::MetadataObserver::start(
            crate::metadata_observer::ObserverConfig {
                voters: vec![(NodeId(1), ctrl.controller_bound_addr().to_string())],
                client_id: "observer-source-offset-test".into(),
                poll_interval: krabka_units::millis(10),
                ..observer_config(Uuid::nil(), observer_dir.path().to_path_buf())
            },
        );
        let writer = Arc::new(RecordingWriter {
            calls: Mutex::new(Vec::new()),
        });
        let source = ObserverSource::new(observer, writer);

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if source.current_metadata_offset() == expected_offset {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("observer source catches up");

        assert2::assert!((source.current_metadata_offset()) == (expected_offset));
        source.cancel().await;
        ctrl.shutdown().await;
    }
}
