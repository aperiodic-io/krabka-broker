//! The cloneable [`KraftController`] handle: the accessors that read the
//! engine's published `watch` channels directly, and the command-sending
//! methods that round-trip a request through the engine task.

use {
    super::KraftController,
    crate::{
        DelegationTokenMutation, SubmitChangeResult,
        error::RaftError,
        kraft::{
            event::Event,
            transport::{Command, Inbound, MetadataFetchSlice, QuorumStateSnapshot},
            types::{NodeId, ReplicaKey},
        },
        reconfig::{ReconfigOutcome, VoterChange},
    },
    krabka_metadata::MetadataImage,
    krabka_units::prelude::ByteSize,
    std::sync::Arc,
    tokio::sync::{oneshot, watch},
};

impl KraftController {
    /// Send the command `make` builds around a fresh reply sender to the
    /// engine task and await its reply.
    ///
    /// # Errors
    /// Returns [`RaftError::Shutdown`] if the engine task is gone, before or
    /// after it took the command.
    async fn ask<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<T>) -> Command,
    ) -> Result<T, RaftError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(make(reply))
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)
    }

    /// Epoch milliseconds: the clock the server stamps connection expiries
    /// with, and the engine's clock for delegation-token deadlines and for the
    /// create-time stamped on every batch the leader appends. `Engine::now`
    /// is monotonic from this process's own start, and a snapshot header
    /// timestamp has to mean the same instant on every node that reads it.
    pub(crate) fn wall_clock_ms() -> i64 { krabka_log::epoch_ms(std::time::SystemTime::now()) }

    /// The node id this controller runs as.
    #[must_use]
    pub fn node_id(&self) -> NodeId { self.me }

    /// Probe a proposed voter through the controller's configured TLS/SASL
    /// dialer rather than opening an unauthenticated plaintext connection.
    pub(crate) async fn probe_kraft_version(
        &self,
        address: &str,
        finalized_version: u16,
    ) -> Result<bool, RaftError> {
        self.peers
            .probe_kraft_version(address, finalized_version)
            .await
    }

    /// A snapshot of the latest applied [`MetadataImage`].
    #[must_use]
    pub fn current_image(&self) -> Arc<MetadataImage> { self.image_rx.borrow().clone() }

    /// Watch the published [`MetadataImage`].
    #[must_use]
    pub fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> { self.image_rx.clone() }

    /// Watch the fault that stopped the engine: `None` while it runs, and
    /// the reason once it stops over a fault Kafka halts the process over.
    /// That is the I/O error of the first write to the metadata log
    /// directory that fails (KIP-858), or a controller activation that
    /// failed.
    #[must_use]
    pub fn watch_fault(&self) -> watch::Receiver<Option<String>> { self.fault_rx.clone() }

    /// Watch the current leader id.
    #[must_use]
    pub fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> { self.leader_rx.clone() }

    /// A synchronous snapshot of consensus state (the handle's `quorum_state()`
    /// reads this without an mpsc round-trip — the engine republishes it on
    /// every event). Cheap `watch` borrow + clone.
    #[must_use]
    pub fn quorum_snapshot(&self) -> QuorumStateSnapshot { self.quorum_rx.borrow().clone() }

    /// Submit a metadata change. On the leader, appends the batch at the current
    /// leader epoch and returns once it is committed (HWM ≥ the appended end
    /// offset) AND applied, surfacing the first per-record rejection. On a
    /// follower, returns [`RaftError::NotLeader`] with the leader hint; the
    /// handle layer forwards via `forward_submit_to`.
    ///
    /// # Errors
    /// - [`RaftError::Metadata`] if a record fails `validate`.
    /// - [`RaftError::NotLeader`] if this node is not the leader.
    /// - [`RaftError::Shutdown`] if the engine task is gone.
    pub async fn submit_change(
        &self,
        records: Vec<krabka_metadata::MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError> {
        self.ask(|reply| Command::SubmitChange {
            records,
            expected_image: None,
            reply,
        })
        .await?
    }

    /// Submit a leader-local mutation only while its admission image is current.
    /// Pending metadata must commit before a caller can plan a guarded mutation.
    ///
    /// # Errors
    /// Returns a leadership, stale-image, uncommitted-tail, or submission error.
    pub async fn submit_change_if_image(
        &self,
        records: Vec<krabka_metadata::MetadataRecord>,
        expected_image: Arc<MetadataImage>,
    ) -> Result<SubmitChangeResult, RaftError> {
        self.ask(|reply| Command::SubmitChange {
            records,
            expected_image: Some(expected_image),
            reply,
        })
        .await?
    }

    /// Submit generation-bound delegation-token mutations.
    ///
    /// # Errors
    ///
    /// Returns a leadership, shutdown, or mutation-rejection error.
    pub async fn submit_delegation_token_mutations(
        &self,
        mutations: Vec<DelegationTokenMutation>,
    ) -> Result<SubmitChangeResult, RaftError> {
        self.ask(|reply| Command::SubmitDelegationTokenMutations { mutations, reply })
            .await?
    }

    /// Submit one KIP-853 voter or kraft-version control operation.
    pub(crate) async fn reconfigure(
        &self,
        change: VoterChange,
    ) -> Result<ReconfigOutcome, RaftError> {
        self.ask(|reply| Command::Reconfigure { change, reply })
            .await?
    }

    /// Atomically finalize `kraft.version` in the Raft control log.
    ///
    /// # Errors
    /// Returns an unsupported-version, leadership, timeout, or storage error.
    pub async fn finalize_kraft_version(&self, version: u16) -> Result<ReconfigOutcome, RaftError> {
        self.reconfigure(VoterChange::FinalizeKraftVersion(version))
            .await
    }

    /// Run the leader-side checks of [`Self::finalize_kraft_version`] and
    /// append nothing.
    ///
    /// # Errors
    /// Returns the error `finalize_kraft_version` would return for the same
    /// request.
    pub async fn validate_kraft_version(&self, version: u16) -> Result<ReconfigOutcome, RaftError> {
        self.reconfigure(VoterChange::ValidateKraftVersion(version))
            .await
    }

    /// A structured snapshot of consensus state for the broker's
    /// `DescribeQuorum` admin view.
    ///
    /// # Errors
    /// Returns [`RaftError::Shutdown`] if the engine task is gone.
    pub async fn quorum_state(&self) -> Result<QuorumStateSnapshot, RaftError> {
        self.ask(|reply| Command::QuorumStateSnapshot { reply })
            .await
    }

    /// Read a committed `__cluster_metadata` slice for an observer's
    /// `API_KEY_METADATA_FETCH` (1004). `replica` names the observer that asked,
    /// when it is a replica, so a leader can track its progress.
    ///
    /// # Errors
    /// Returns [`RaftError::Shutdown`] if the engine task is gone.
    pub async fn metadata_fetch(
        &self,
        fetch_offset: i64,
        max_size: ByteSize,
        replica: Option<ReplicaKey>,
    ) -> Result<MetadataFetchSlice, RaftError> {
        self.ask(|reply| Command::MetadataFetch {
            fetch_offset,
            max_size,
            replica,
            reply,
        })
        .await
    }

    /// Serialize the current image to a KIP-630 checkpoint under the data dir.
    ///
    /// # Errors
    /// Returns [`RaftError`] if serialization or the file write fails.
    pub async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        self.ask(|reply| Command::TriggerSnapshot { reply }).await?
    }

    /// Inject a raw core [`Event`] into the loop (test/driver entrypoint and the
    /// internal feedback path for peer-RPC responses).
    ///
    /// # Errors
    /// Returns [`RaftError::Shutdown`] if the engine task is gone.
    pub async fn inject_event(&self, event: Event) -> Result<(), RaftError> {
        self.cmd_tx
            .send(Command::Event(event))
            .await
            .map_err(|_| RaftError::Shutdown)
    }

    /// Deliver an inbound peer RPC to the engine.
    ///
    /// # Errors
    /// Returns [`RaftError::Shutdown`] if the engine task is gone.
    pub async fn deliver(&self, inbound: Inbound) -> Result<(), RaftError> {
        self.cmd_tx
            .send(Command::Inbound(inbound))
            .await
            .map_err(|_| RaftError::Shutdown)
    }

    /// Stop the engine task and wait for its log to close.
    pub async fn shutdown(&self) {
        let mut image = self.image_rx.clone();
        let _ = self.cmd_tx.send(Command::Shutdown).await;
        // The engine owns the image sender. Wait for it to drop before callers
        // can reopen or remove the log directory.
        while image.changed().await.is_ok() {}
    }

    /// Test-only: append `records` as a committed batch and apply them through
    /// the real pipeline; returns the appended base offset.
    #[cfg(test)]
    pub(super) async fn test_append_and_commit(
        &self,
        records: Vec<krabka_metadata::MetadataRecord>,
    ) -> Result<i64, RaftError> {
        self.ask(|reply| Command::TestAppendAndCommit { records, reply })
            .await
    }
}
