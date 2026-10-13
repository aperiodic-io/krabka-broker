//! The [`MetadataSource`] implementation for a live `ControllerHandle`, which
//! is what combined and controller nodes run. Every method delegates straight
//! to the handwritten `KRaft` voter, so it lives apart from the broker-only observer path.

use {
    super::{MetadataSource, prelude::*},
    krabka_raft::ControllerHandle,
};

#[async_trait::async_trait]
impl MetadataSource for ControllerHandle {
    fn current_image(&self) -> Arc<MetadataImage> { ControllerHandle::current_image(self) }

    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        ControllerHandle::watch_image(self)
    }

    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        ControllerHandle::watch_leader(self)
    }

    fn watch_fatal(&self) -> watch::Receiver<Option<String>> { ControllerHandle::watch_fatal(self) }

    fn quorum_state(&self) -> QuorumState { ControllerHandle::quorum_state(self) }

    fn quorum_committed_offset(&self) -> i64 { ControllerHandle::quorum_committed_offset(self) }

    fn voted_directory_id(&self) -> Option<uuid::Uuid> {
        ControllerHandle::voted_directory_id(self)
    }

    fn quorum_snapshot(&self) -> Option<krabka_raft::QuorumStateSnapshot> {
        Some(ControllerHandle::quorum_snapshot(self))
    }

    fn last_applied_record_timestamp_ms(&self) -> i64 {
        ControllerHandle::quorum_snapshot(self).last_applied_record_timestamp_ms
    }

    fn pending_record_processing_time_us(&self) -> (u64, u64) {
        let snapshot = ControllerHandle::quorum_snapshot(self);
        (
            snapshot.pending_record_processing_time_us_avg,
            snapshot.pending_record_processing_time_us_max,
        )
    }

    async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError> {
        ControllerHandle::submit_change(self, records).await
    }

    async fn submit_change_if_image(
        &self,
        records: Vec<MetadataRecord>,
        expected_image: Arc<MetadataImage>,
    ) -> Result<SubmitChangeResult, RaftError> {
        ControllerHandle::submit_change_if_image(self, records, expected_image).await
    }

    async fn submit_delegation_token_mutations(
        &self,
        mutations: Vec<DelegationTokenMutation>,
    ) -> Result<SubmitChangeResult, RaftError> {
        ControllerHandle::submit_delegation_token_mutations(self, mutations).await
    }

    async fn change_membership(&self, new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
        ControllerHandle::change_membership(self, new_voters).await
    }

    async fn add_learner(&self, node_id: NodeId, node: Node) -> Result<(), RaftError> {
        ControllerHandle::add_learner(self, node_id, node).await
    }

    fn controller_bound_addr(&self) -> SocketAddr { ControllerHandle::controller_bound_addr(self) }

    fn read_snapshot_range(&self, position: i64, max_bytes: i32) -> SnapshotRange {
        ControllerHandle::read_snapshot_range(self, position, max_bytes)
    }

    async fn fetch_snapshot(
        &self,
        version: i16,
        body: bytes::Bytes,
    ) -> Option<Result<bytes::Bytes, RaftError>> {
        Some(ControllerHandle::fetch_snapshot(self, version, body).await)
    }

    async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        ControllerHandle::trigger_snapshot(self).await
    }

    async fn add_voter(&self, req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
        ControllerHandle::add_voter(self, req).await
    }

    async fn check_add_voter(&self, req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
        ControllerHandle::check_add_voter(self, req).await
    }

    async fn remove_voter(&self, req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
        ControllerHandle::remove_voter(self, req).await
    }

    async fn update_voter(&self, req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
        ControllerHandle::update_voter(self, req).await
    }

    async fn finalize_kraft_version(&self, version: u16) -> Result<ReconfigOutcome, RaftError> {
        ControllerHandle::finalize_kraft_version(self, version).await
    }

    async fn validate_kraft_version(&self, version: u16) -> Result<ReconfigOutcome, RaftError> {
        ControllerHandle::validate_kraft_version(self, version).await
    }

    async fn forward_raw(
        &self,
        api_key: i16,
        version: i16,
        body: bytes::Bytes,
    ) -> Option<Result<bytes::Bytes, RaftError>> {
        // `DescribeQuorum` is the one broker-listener admin RPC a
        // combined/controller node forwards even when IT ITSELF runs a
        // raft voter: Kafka's `KafkaApis` forwards `DescribeQuorum`
        // unconditionally (`forwardToController`), unlike the KIP-853
        // voter-admin RPCs (80/81/82), which answer `NOT_LEADER_OR_FOLLOWER`
        // locally instead of forwarding. A node that IS the active
        // controller answers locally, same as a broker-only observer's
        // writer never gets asked to forward its own leader.
        //
        // The handler sends it wrapped in a KIP-590 `Envelope` (api_key=58),
        // not the bare `DescribeQuorum` api_key=55, so the leader authorizes
        // the caller's own identity rather than this node's inter-broker one
        // (`crate::handlers::describe_quorum::forward`, review of #1034).
        // Nothing else in this codebase forwards an `Envelope` through this
        // method, so gating on its api key is exactly as exclusive as gating
        // on `DescribeQuorum`'s own was.
        if api_key != krabka_protocol::owned::envelope_request::API_KEY
            || ControllerHandle::quorum_snapshot(self).is_leader
        {
            return None;
        }
        Some(ControllerHandle::forward_raw(self, api_key, version, body).await)
    }

    async fn cancel(&self) { ControllerHandle::cancel(self).await; }
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        crate::metadata_source::test_support::{StartedController, start_controller, topic_record},
    };

    krabka_macros::bind_retry_fixture!(bind_eventually, ::std::time::Duration::from_secs(2));

    #[tokio::test]
    async fn controller_handle_metadata_source_forwards_snapshot_reconfig_and_cancel() {
        let StartedController {
            dir: _dir,
            controller: ctrl,
        } = start_controller().await;
        let source: &dyn MetadataSource = &ctrl;

        source
            .add_learner(NodeId(2), Node::default())
            .await
            .expect("stage learner identity");
        source
            .submit_change(vec![topic_record("snapshot-topic")])
            .await
            .expect("submit metadata");
        source
            .submit_change(vec![topic_record("snapshot-topic-2")])
            .await
            .expect("submit second metadata record");
        let expected_offset = i64::try_from(source.quorum_state().last_applied_index)
            .unwrap_or(i64::MAX)
            .saturating_sub(1);
        assert2::assert!(
            (expected_offset) != (1),
            "test must distinguish a constant offset"
        );
        assert2::assert!((source.current_metadata_offset()) == (expected_offset));
        // On a leader the quorum's committed offset is this node's own: it is
        // the node that decides what "committed" means, so the readiness probe
        // must measure exactly zero lag here. A follower is where the two part
        // company, which `krabka-raft`'s engine tests cover.
        assert2::assert!((source.quorum_committed_offset()) == (expected_offset));
        source.trigger_snapshot().await.expect("snapshot");
        assert2::assert!(matches!(
            source.read_snapshot_range(0, 1),
            SnapshotRange::Slice(_)
        ));

        let addr = source.controller_bound_addr();
        source.cancel().await;
        let listener = bind_eventually(addr).await;
        drop(listener);
    }
}
