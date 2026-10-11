//! `Controller` is the public entry point. It owns the async
//! [`KraftController`] consensus engine, the controller TCP listener, and the
//! `submit_change` leader-aware forwarding logic, behind a stable
//! [`ControllerHandle`] API the broker depends on.
//!
//! Cluster formation is driven by `BootstrapMode`: a fresh `Bootstrap`/`Join`
//! node seeds its quorum state from configured static voters or a dynamic
//! bootstrap snapshot; a restarted `Rejoin` node recovers from its on-disk
//! metadata log, checkpoint, and quorum-state file (handled inside
//! [`KraftController::open`]).
//!
//! KIP-853 voter changes are serialized by the same single-owner engine that
//! appends, commits, truncates, and snapshots the metadata log.

use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

use krabka_metadata::MetadataImage;
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod checkpoint;
mod feature_check;
mod membership;
mod metadata_fetch;
mod startup;
mod submit;
#[cfg(test)]
mod test_support;

pub use self::{
    checkpoint::{SnapshotRange, SnapshotSlice},
    startup::{Controller, metadata_log_nonempty},
};
pub use crate::kraft::transport::QuorumStateSnapshot;
use crate::{
    kraft::KraftController,
    network::OutboundDialer,
    types::{Node, NodeId},
};

fn connection_options(
    client_id: &str,
    dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    frame_max: krabka_client_core::ClientFrameMax,
) -> krabka_client_core::ConnectionOptions {
    krabka_client_core::ConnectionOptions {
        client_id: client_id.to_owned(),
        dispatch_queue_capacity,
        frame_max,
        ..krabka_client_core::ConnectionOptions::default()
    }
}

/// Krabka-native view of the controller's current quorum state. Surfaced by
/// [`ControllerHandle::quorum_state`] for the broker's `DescribeQuorum` admin
/// handler so callers don't depend on engine internals directly.
#[derive(Debug, Clone, Default)]
pub struct QuorumState {
    /// `KRaft` leader epoch (on the wire as `leader_epoch`).
    pub current_term: u64,
    /// High watermark — the last committed/applied offset on this node. `0`
    /// until the first commit.
    pub last_applied_index: u64,
    /// Current cluster leader. `None` mid-election.
    pub current_leader: Option<NodeId>,
    /// Voter ids in the current (static) membership.
    pub voters: Vec<NodeId>,
    /// Full voter node identities (directory id + endpoints + kraft.version)
    /// keyed by node id. Mirrors `voters`; carries the KIP-853 voter metadata
    /// the `DescribeQuorum` path needs.
    pub voter_nodes: BTreeMap<NodeId, Node>,
    /// Per-voter fetch offset (matched index). Empty on a follower; callers use
    /// Kafka's unknown sentinel.
    pub per_voter_matched_index: BTreeMap<NodeId, u64>,
    /// Per-replica last fetch timestamp in wall-clock milliseconds.
    pub per_replica_last_fetch_ms: BTreeMap<NodeId, i64>,
    /// Per-replica last caught-up timestamp in wall-clock milliseconds.
    pub per_replica_last_caught_up_ms: BTreeMap<NodeId, i64>,
    /// Discovered directory identities for observer replicas.
    pub observer_directory_ids: BTreeMap<NodeId, uuid::Uuid>,
    /// Whether this node currently leads the metadata quorum.
    pub is_leader: bool,
}

/// Handle returned by [`Controller::start`]. Owns the live [`KraftController`]
/// engine and the listener task. Drop is NOT a clean shutdown — call
/// [`Self::shutdown`] (or [`Self::cancel`]) to drain the listener + stop the
/// engine before the runtime is torn down.
pub struct ControllerHandle {
    engine: KraftController,
    leader: watch::Receiver<Option<NodeId>>,
    shutdown: CancellationToken,
    /// The reason this controller stopped itself over a fatal fault, if it did.
    fatal: watch::Receiver<Option<String>>,
    listener_task: Mutex<Option<JoinHandle<()>>>,
    /// Directory holding the metadata log + KIP-630 `.checkpoint` artifacts.
    data_dir: std::path::PathBuf,
    client_id: String,
    client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    client_frame_max: krabka_client_core::ClientFrameMax,
    /// This node's own id, used for leader and membership checks.
    self_node_id: NodeId,
    /// Configured bootstrap voter set. Dynamic membership comes from the
    /// engine snapshot; this remains only as an address fallback during
    /// initial discovery at kraft.version 0.
    voters: krabka_metadata::VoterSet,
    /// Compatibility staging area for callers that still separate observer
    /// registration from promotion. Membership itself is changed only by an
    /// engine-owned KIP-853 command.
    staged_learners: std::sync::Mutex<BTreeMap<NodeId, Node>>,
    /// Outbound dialer; `forward_submit_to`/`fetch_metadata_from` reach a peer's
    /// controller listener with the same TLS/SASL handshake the engine's RPCs
    /// ride on.
    dialer: Arc<dyn OutboundDialer>,
    /// The address the controller listener actually bound to (resolved port when
    /// `controller_listen_addr` requested port 0).
    controller_bound_addr: SocketAddr,
}

impl ControllerHandle {
    /// Current metadata snapshot (cheap; `Arc` clone).
    #[must_use]
    pub fn current_image(&self) -> Arc<MetadataImage> {
        self.engine.current_image()
    }

    /// The address the controller listener actually bound to.
    #[must_use]
    pub fn controller_bound_addr(&self) -> SocketAddr {
        self.controller_bound_addr
    }

    /// Subscribe to leader-id changes.
    #[must_use]
    pub fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.leader.clone()
    }

    /// Subscribe to the controller's fatal fault: the message of the fault
    /// that made it stop itself, and `None` until then.
    ///
    /// Kafka handles such a fault (a `FeatureControlManager` replay of a
    /// feature level that the controller does not support) with a
    /// `ProcessTerminatingFaultHandler`, which halts the process. A library
    /// cannot halt its host, so the controller stops itself, publishes the
    /// message here before it does, and leaves the process to its embedder:
    /// the broker turns it into its own shutdown. The channel closes without a
    /// value when the controller is shut down for any other reason.
    #[must_use]
    pub fn watch_fatal(&self) -> watch::Receiver<Option<String>> {
        self.fatal.clone()
    }

    /// Subscribe to metadata-image changes.
    #[must_use]
    pub fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.engine.watch_image()
    }

    /// Snapshot the controller's current quorum state. Used by the broker's
    /// `DescribeQuorum` (`api_key=55`, KIP-595) handler. Cheap — a `watch`
    /// borrow of the engine's published snapshot.
    #[must_use]
    pub fn quorum_state(&self) -> QuorumState {
        let snap = self.engine.quorum_snapshot();
        let voter_nodes: BTreeMap<NodeId, Node> = snap
            .voters
            .iter()
            .map(|v| {
                (
                    v.id,
                    Node {
                        directory_id: v.directory_id,
                        endpoints: v.endpoints.clone(),
                        kraft_version: v.kraft_version,
                    },
                )
            })
            .collect();
        let per_voter_matched_index: BTreeMap<NodeId, u64> = snap
            .per_replica_fetch_offset
            .iter()
            .map(|(id, off)| (*id, u64::try_from((*off).max(0)).unwrap_or(0)))
            .collect();
        QuorumState {
            current_term: u64::from(snap.leader_epoch),
            last_applied_index: u64::try_from(snap.high_watermark.max(0)).unwrap_or(0),
            current_leader: snap.leader_id,
            voters: snap.voters.ids().into_iter().collect(),
            voter_nodes,
            per_voter_matched_index,
            per_replica_last_fetch_ms: snap.per_replica_last_fetch_ms,
            per_replica_last_caught_up_ms: snap.per_replica_last_caught_up_ms,
            observer_directory_ids: snap
                .observers
                .iter()
                .map(|observer| (observer.id, observer.directory_id))
                .collect(),
            is_leader: snap.is_leader,
        }
    }

    /// Snapshot the full consensus state.
    #[must_use]
    pub fn quorum_snapshot(&self) -> QuorumStateSnapshot {
        self.engine.quorum_snapshot()
    }

    /// Highest `__cluster_metadata` offset the quorum has committed, as this
    /// node last heard it, or `-1` before the first committed record.
    ///
    /// This is the offset a readiness probe compares against. It differs from
    /// [`QuorumState::last_applied_index`] on a follower that is still
    /// replaying: `last_applied_index` is clamped to what this node has
    /// applied, while this is what the leader said it had committed.
    #[must_use]
    pub fn quorum_committed_offset(&self) -> i64 {
        self.engine
            .quorum_snapshot()
            .quorum_high_watermark
            .saturating_sub(1)
    }

    /// Directory identity voted for in the current leader epoch, if any.
    #[must_use]
    pub fn voted_directory_id(&self) -> Option<Uuid> {
        self.engine.quorum_snapshot().voted_directory_id
    }

    /// Drain the listener and stop the engine. Idempotent in practice.
    pub async fn shutdown(self) {
        self.cancel().await;
    }

    /// Stop the engine and cancel the controller listener without consuming
    /// `self`. Used by `BrokerHandle::shutdown` where the controller is behind
    /// an `Arc`. Idempotent. Awaits the listener task so the OS port is released
    /// before returning.
    pub async fn cancel(&self) {
        self.shutdown.cancel();
        self.engine.shutdown().await;
        if let Some(h) = self.listener_task.lock().await.take() {
            let _ = h.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::prelude::TimeExt as _;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        config::ControllerConfig,
        controller::test_support::{
            FAST_ELECTION_TIMEOUT, TEST_OP_TIMEOUT, bind_eventually, bootstrap_controller,
            committable_topic_record, joining_controller, wait_for_leader,
        },
    };

    #[tokio::test]
    async fn quorum_view_reflects_live_single_voter_state_and_submitted_records() {
        let dir = TempDir::new().unwrap();
        let mut cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
        cfg.election_timeout = FAST_ELECTION_TIMEOUT;
        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        wait_for_leader(&ctrl).await;

        let quorum = ctrl.quorum_state();
        check!(
            (
                quorum.voter_nodes.contains_key(&NodeId(1)),
                quorum.current_leader,
                quorum.current_leader == Some(NodeId(1)),
            ) == (true, Some(NodeId(1)), true)
        );

        tokio::time::timeout(
            TEST_OP_TIMEOUT.to_std(),
            ctrl.submit_change(vec![committable_topic_record("ops-a")]),
        )
        .await
        .expect("submit ops-a timed out")
        .expect("submit ops-a");
        tokio::time::timeout(
            TEST_OP_TIMEOUT.to_std(),
            ctrl.submit_change(vec![committable_topic_record("ops-b")]),
        )
        .await
        .expect("submit ops-b timed out")
        .expect("submit ops-b");

        assert2::assert!(
            (
                ctrl.current_image().topic("ops-a").is_some(),
                ctrl.current_image().topic("ops-b").is_some(),
            ) == (true, true)
        );
        let quorum = ctrl.quorum_state();
        let leader_last = quorum.last_applied_index;
        assert2::assert!(leader_last >= 2);
        assert2::assert!(quorum.per_voter_matched_index.get(&NodeId(1)) == Some(&leader_last));
        assert2::assert!(ctrl.quorum_committed_offset() == 2);
        assert2::assert!(ctrl.voted_directory_id() == Some(Uuid::nil()));
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn quorum_view_reports_join_node_is_not_leader() {
        let (_dir, ctrl) = joining_controller("join start").await;

        assert2::assert!(ctrl.quorum_state().current_leader.is_none());
        assert2::assert!(ctrl.voted_directory_id().is_none());
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_releases_bound_listener_addr() {
        let (_dir, ctrl) = bootstrap_controller("bootstrap").await;
        let addr = ctrl.controller_bound_addr();

        ctrl.shutdown().await;

        let rebound = bind_eventually(addr).await;
        drop(rebound);
    }

    #[tokio::test]
    async fn cancel_releases_bound_listener_addr_without_consuming_handle() {
        let (_dir, ctrl) = bootstrap_controller("bootstrap").await;
        let addr = ctrl.controller_bound_addr();

        ctrl.cancel().await;

        let rebound = bind_eventually(addr).await;
        drop(rebound);
        ctrl.cancel().await;
    }
}
