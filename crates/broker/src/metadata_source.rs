//! `MetadataSource`: the metadata authority a broker reads from and
//! writes through.
//!
//! Combined and controller nodes back it with a live `ControllerHandle`,
//! which is a handwritten `KRaft` voter. Broker-only nodes back it with a
//! `MetadataObserver`, a true `KRaft` observer, plus a write-forwarding
//! path to the controller quorum. Handlers depend only on this trait.

use std::{collections::BTreeSet, net::SocketAddr, sync::Arc};

use krabka_metadata::{MetadataImage, MetadataRecord};
use krabka_raft::{
    AddVoter, DelegationTokenMutation, Node, NodeId, QuorumState, RaftError, ReconfigOutcome,
    RemoveVoter, SnapshotRange, SubmitChangeResult, UpdateVoter,
};
use tokio::sync::watch;

mod controller_handle;
mod fatal_fault;
mod image_watch;
mod observer_source;
mod quorum_forwarder;
#[cfg(test)]
mod test_support;

pub(crate) use self::{
    fatal_fault::or_fatal_fault,
    image_watch::{
        next_image_until_shutdown, next_published_image, wait_for_image_change, watch_image_loop,
    },
};
pub use self::{observer_source::ObserverSource, quorum_forwarder::QuorumForwarder};

#[async_trait::async_trait]
pub trait MetadataSource: Send + Sync {
    fn current_image(&self) -> Arc<MetadataImage>;
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>>;
    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>>;
    /// The message of the fatal fault that stopped this source's controller,
    /// and `None` until there is one. Kafka halts the process on such a fault
    /// (`ProcessTerminatingFaultHandler`), so the broker that hosts the source
    /// stops itself when this changes to `Some`.
    ///
    /// A live controller can fault, and so can a broker-only observer that
    /// meets an invalid `KRaft` control record. The default is the channel of a
    /// source that has none: it never carries a value, and its sender is
    /// already dropped, so `changed` on it fails at once.
    fn watch_fatal(&self) -> watch::Receiver<Option<String>> {
        watch::channel(None).1
    }
    /// Committed metadata records this source could not decode and skipped:
    /// Kafka's `metadata-load-error-count`. Only a broker-only observer skips
    /// one; a controller stops on it instead, so the default is zero.
    fn metadata_load_error_count(&self) -> u64 {
        0
    }
    /// Process-lifetime average and maximum microseconds spent applying a
    /// completed set of pending committed metadata records.
    fn pending_record_processing_time_us(&self) -> (u64, u64) {
        (0, 0)
    }
    fn quorum_state(&self) -> QuorumState;
    /// Current controller epoch when this source owns a quorum view.
    /// Broker-only observers return `None` because they track the leader id but
    /// do not own the controller's term state.
    fn current_controller_epoch(&self) -> Option<u64> {
        Some(self.quorum_state().current_term)
    }
    /// Highest metadata-log offset applied to the current image, or `-1`
    /// before the first record.
    fn current_metadata_offset(&self) -> i64 {
        i64::try_from(self.quorum_state().last_applied_index)
            .unwrap_or(i64::MAX)
            .saturating_sub(1)
    }
    /// Unix epoch milliseconds of the latest metadata batch applied by this
    /// source, or zero before it has applied one.
    fn last_applied_record_timestamp_ms(&self) -> i64 {
        0
    }
    /// Highest `__cluster_metadata` offset the metadata quorum has committed,
    /// as this node last heard it, or `-1` before it has heard anything.
    ///
    /// [`Self::current_metadata_offset`] says how far this node has got;
    /// this says how far the quorum has got. The gap between the two is the
    /// metadata lag the readiness probe bounds. The default treats the node's
    /// own applied offset as the quorum's, which is what a source with no
    /// separate view of the quorum can honestly report.
    fn quorum_committed_offset(&self) -> i64 {
        self.current_metadata_offset()
    }
    /// Directory identity voted for in the current controller epoch.
    fn voted_directory_id(&self) -> Option<uuid::Uuid> {
        None
    }
    /// Snapshot the full consensus state if this node runs a controller.
    fn quorum_snapshot(&self) -> Option<krabka_raft::QuorumStateSnapshot> {
        None
    }
    async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError>;
    /// Submit a leader-local mutation only if its admission image remains current.
    /// Observer-backed sources must forward the original request to the controller
    /// before planning such a mutation.
    async fn submit_change_if_image(
        &self,
        _records: Vec<MetadataRecord>,
        _expected_image: Arc<MetadataImage>,
    ) -> Result<SubmitChangeResult, RaftError> {
        Err(RaftError::NotLeader {
            current_leader: *self.watch_leader().borrow(),
        })
    }
    async fn submit_delegation_token_mutations(
        &self,
        _mutations: Vec<DelegationTokenMutation>,
    ) -> Result<SubmitChangeResult, RaftError> {
        Err(unsupported_token_mutations("source"))
    }
    async fn change_membership(&self, new_voters: BTreeSet<NodeId>) -> Result<(), RaftError>;
    async fn add_learner(&self, node_id: NodeId, node: Node) -> Result<(), RaftError>;
    /// The controller listener's bound address. It is meaningful only on
    /// controller and combined nodes. Broker-only observers have no
    /// controller listener and report an unspecified address.
    fn controller_bound_addr(&self) -> SocketAddr;
    /// Read a byte window of the latest metadata snapshot. Controller and
    /// combined nodes back this with their on-disk checkpoint. Broker-only
    /// observers have none to read.
    fn read_snapshot_range(&self, position: i64, max_bytes: i32) -> SnapshotRange;
    /// Answer a `FetchSnapshot` request body at `version` as the controller
    /// listener does, with the response body. `None` means this node keeps no
    /// metadata log of its own to serve, as a broker-only observer.
    async fn fetch_snapshot(
        &self,
        _version: i16,
        _body: bytes::Bytes,
    ) -> Option<Result<bytes::Bytes, RaftError>> {
        None
    }
    /// Schedule a metadata snapshot. It is meaningful only on controller and
    /// combined nodes. Broker-only observers have no log of their own to
    /// snapshot.
    async fn trigger_snapshot(&self) -> Result<(), RaftError>;
    async fn add_voter(&self, req: AddVoter) -> Result<ReconfigOutcome, RaftError>;
    /// Run the leader-local checks of `AddVoterHandler` (pending change, high
    /// watermark, `kraft.version`, uncommitted voters record, duplicate id)
    /// for `req` without contacting the candidate or appending anything.
    /// Only a controller can evaluate them; every other source is not the
    /// leader.
    async fn check_add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
        Ok(ReconfigOutcome::NotLeader { leader: None })
    }
    async fn remove_voter(&self, req: RemoveVoter) -> Result<ReconfigOutcome, RaftError>;
    async fn update_voter(&self, req: UpdateVoter) -> Result<ReconfigOutcome, RaftError>;
    async fn finalize_kraft_version(&self, _version: u16) -> Result<ReconfigOutcome, RaftError> {
        Err(RaftError::NotLeader {
            current_leader: None,
        })
    }
    /// Run the leader-side checks of [`Self::finalize_kraft_version`] and write
    /// nothing: the `validate_only` half of a `kraft.version` upgrade.
    async fn validate_kraft_version(&self, _version: u16) -> Result<ReconfigOutcome, RaftError> {
        Err(RaftError::NotLeader {
            current_leader: None,
        })
    }
    /// Forward a raw Kafka protocol request to the active controller if this
    /// node is a broker-only observer. Controller and combined nodes return
    /// `None` so the caller handles the request locally.
    async fn forward_raw(
        &self,
        _api_key: i16,
        _version: i16,
        _body: bytes::Bytes,
    ) -> Option<Result<bytes::Bytes, RaftError>> {
        None
    }
    async fn cancel(&self);
}

fn unsupported_token_mutations(component: &str) -> RaftError {
    RaftError::ChangeRejected(format!(
        "metadata {component} does not support generation-bound token mutations"
    ))
}

/// Write side for broker-only nodes: forward a batch to the controller
/// quorum leader.
#[async_trait::async_trait]
pub trait MetadataWriter: Send + Sync {
    async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError>;
    async fn submit_delegation_token_mutations(
        &self,
        _mutations: Vec<DelegationTokenMutation>,
    ) -> Result<SubmitChangeResult, RaftError> {
        Err(unsupported_token_mutations("writer"))
    }
    /// Forward a raw request to the controller quorum.
    async fn forward_raw(
        &self,
        _api_key: i16,
        _version: i16,
        _body: bytes::Bytes,
    ) -> Result<bytes::Bytes, RaftError> {
        Err(RaftError::NotLeader {
            current_leader: None,
        })
    }
}
