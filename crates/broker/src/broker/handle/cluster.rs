//! Test-only [`BrokerHandle`] helpers that observe cluster-wide state: the
//! metadata image, the raft quorum and its reconfiguration, and the awaiters
//! that block until the image or a metric reaches an expected value. They are
//! grouped here so the handle's production surface stays readable.

#[cfg(any(test, feature = "test-helpers"))]
use crate::{error::BrokerError, metrics::BrokerMetrics};
use {
    crate::broker::{Broker, BrokerHandle, TEST_AWAITER_TIMEOUT},
    std::sync::Arc,
};

impl BrokerHandle {
    /// Test-only: the controller's current quorum state (leader epoch, HWM,
    /// per-voter matched index). Used by the mixed-quorum acceptance test to
    /// observe whether the Krabka leader commits/advances and whether a peer
    /// voter (e.g. a JVM follower) is fetching.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn controller_quorum_state_for_test(&self) -> krabka_raft::QuorumState {
        self.broker.controller.quorum_state()
    }

    /// Test-only: submit a [`krabka_metadata::MetadataRecord`] directly to
    /// this broker's controller, bypassing the public Kafka APIs. Used by
    /// integration tests to provision a SCRAM credential before the
    /// `AlterUserScramCredentials` handler exists. Returns an
    /// error if the submit fails (e.g., this broker is not the raft leader
    /// and forwarding fails).
    ///
    /// # Errors
    ///
    /// Forwards the underlying raft errors as [`BrokerError::Replication`].
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn submit_metadata_record_for_test(
        &self,
        rec: krabka_metadata::MetadataRecord,
    ) -> Result<(), BrokerError> {
        use crate::error::BrokerError;

        self.broker
            .controller
            .submit_change(vec![rec])
            .await
            .map(|_| ())
            .map_err(|e| BrokerError::Replication(format!("submit: {e}")))
    }

    /// Test-only: return a snapshot of the current `MetadataImage` as seen by
    /// this broker's controller. Mirrors `partition_leader_for_test` /
    /// `partition_record_for_test` but exposes the whole image so throttle
    /// integration tests can call `broker_throttle_rate` and
    /// `topic_config` directly without adding per-field accessors.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn controller_image_for_test(&self) -> std::sync::Arc<krabka_metadata::MetadataImage> {
        self.broker.controller.current_image()
    }

    /// Test-only: the node ids this broker's committed image reports as
    /// fenced, through the same registration fence the offline-replica
    /// projection uses. The role-separation suite reads it on every node,
    /// because the fencing state is replicated: a controller that fences a
    /// broker it never hears from publishes that to every image in the
    /// cluster.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn fenced_broker_ids_for_test(&self) -> std::collections::BTreeSet<u64> {
        use crate::heartbeat::fencing::fenced_node_ids;

        fenced_node_ids(&self.broker.controller.current_image())
            .into_iter()
            .collect()
    }

    /// Test-only: the highest metadata-log offset this node's image has
    /// applied, or `-1` before the first record. Every node counts from the
    /// same replicated log, so an offset one node reports is a point another
    /// node's image can be compared against.
    ///
    /// Pair it with [`Self::wait_until_metadata_offset_at_least`] to turn "the
    /// controller leader has decided and written it down" into a point a
    /// follower can be waited past. An image that has not yet applied a record
    /// is indistinguishable from one that never will, so a test that reads a
    /// follower's state has to know which side of the record it stands on.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn metadata_offset_for_test(&self) -> i64 {
        self.broker.controller.current_metadata_offset()
    }

    /// Test-only: the raft voter set this node's metadata source reports.
    /// A controller/combined node returns the openraft membership; a
    /// broker-only (observer) node returns an empty set because it never
    /// joins the quorum. The role-separation test uses this to assert that a
    /// broker-only node is absent from the controller's voters.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn quorum_voters_for_test(&self) -> Vec<krabka_raft::NodeId> {
        self.broker.controller.quorum_state().voters
    }

    /// Test-only: clone the inner `Arc<Broker>`. Used by the `auto_join`
    /// unit test (and dynamic-voters integration tests) that need to drive
    /// broker-internal background routines directly.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn broker_arc_for_test(&self) -> Arc<Broker> { self.broker.clone() }

    /// Test-only: the controller voter set's size as seen by this broker's
    /// committed `MetadataImage`. KIP-853 dynamic-voters tests poll this to
    /// observe auto-join grow the quorum and `remove_voter` shrink it.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn voter_count_for_test(&self) -> usize {
        self.broker.controller.current_image().voters().len()
    }

    /// Test-only: finalized `kraft.version` from the committed image.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn kraft_version_for_test(&self) -> u16 {
        self.broker.controller.current_image().kraft_version()
    }

    /// Test-only: the controller voter ids as seen by this broker's
    /// committed `MetadataImage`. The dynamic-voters shrink test uses this
    /// to pick a follower to remove.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn voter_ids_for_test(&self) -> std::collections::BTreeSet<krabka_raft::NodeId> {
        self.broker.controller.current_image().voters().ids()
    }

    /// Test-only: the `directory_id` of voter `id` from this broker's
    /// committed `MetadataImage`, if present. `remove_voter` needs the
    /// voter's directory id to disambiguate.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn voter_directory_id_for_test(&self, id: krabka_raft::NodeId) -> Option<uuid::Uuid> {
        self.broker
            .controller
            .current_image()
            .voters()
            .get(id)
            .map(|v| v.directory_id)
    }

    /// Test-only: run the KIP-853 `remove_voter` reconfiguration on this
    /// broker's controller (must be the raft leader). Returns the coordinator
    /// outcome so the dynamic-voters test can assert `Committed`.
    ///
    /// # Errors
    ///
    /// Forwards the underlying raft error.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn remove_voter_for_test(
        &self,
        req: krabka_raft::reconfig::RemoveVoter,
    ) -> Result<krabka_raft::reconfig::ReconfigOutcome, krabka_raft::RaftError> {
        self.broker.controller.remove_voter(req).await
    }

    /// Test-only: atomically activate dynamic controller membership.
    ///
    /// # Errors
    /// Propagates validation, leadership, and persistence errors from Raft.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn finalize_kraft_version_for_test(
        &self,
        version: u16,
    ) -> Result<krabka_raft::reconfig::ReconfigOutcome, krabka_raft::RaftError> {
        self.broker.controller.finalize_kraft_version(version).await
    }

    /// Test-only: ask this broker's controller to generate a metadata
    /// snapshot. The trigger only schedules the work. The snapshot
    /// completes asynchronously, so callers poll for the result.
    ///
    /// # Errors
    ///
    /// Propagates any error from the underlying raft trigger.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn trigger_snapshot_for_test(&self) -> Result<(), krabka_raft::RaftError> {
        self.broker.controller.trigger_snapshot().await
    }

    /// Test-only: subscribe to the controller's leader watch channel.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn watch_leader_for_test(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<krabka_raft::NodeId>> {
        self.broker.controller.watch_leader()
    }

    /// Test-only: await until `pred` holds for the controller metadata image.
    /// Subscribes to the image watch channel and `.await`s changes. There is no
    /// polling sleep. A 30s bound makes a stuck condition fail the test.
    ///
    /// The timeout message names the caller and the metadata offset the image
    /// had applied, so a failure says which wait stuck and how far the image
    /// got. The method is `#[track_caller]`, and so it is a plain `fn` that
    /// returns the future: an `async fn` cannot capture its caller.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[track_caller]
    pub fn wait_for_image<F>(&self, pred: F) -> impl std::future::Future<Output = ()>
    where
        F: Fn(&krabka_metadata::MetadataImage) -> bool,
    {
        let waiting = image_awaiter(self.broker.controller.watch_image(), pred);
        async move {
            if let Err(timeout) = waiting.await {
                let offset = self.broker.controller.current_metadata_offset();
                panic!("{}", timeout.message(offset));
            }
        }
    }

    /// Test-only: borrow this broker's live [`crate::metrics::BrokerMetrics`]
    /// bundle so integration tests can read counters / gauges in-process.
    ///
    /// Pair with [`Self::wait_for_metrics`] to replace fixed-duration `sleep`s
    /// with a bounded poll on an observable signal, such as a counter that
    /// crosses a threshold or a gauge that reaches an expected value. The
    /// metric moves the instant the awaited work lands, so the wait is
    /// race-free rather than a timing guess.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn metrics(&self) -> &BrokerMetrics { &self.broker.metrics }

    /// Test-only: poll `predicate` against this broker's live metrics every
    /// ~25ms until it returns `true` or [`TEST_AWAITER_TIMEOUT`] elapses.
    ///
    /// Use this instead of a fixed `sleep` in integration tests. A fixed sleep
    /// only guesses that a background loop ran, such as the gauge sampler, disk
    /// scanner, cleaner, ISR-maintenance tick, or audit flush. This method waits
    /// until the counter or gauge that the loop bumps shows the awaited state.
    /// `what` names the condition for the timeout panic message. A Prometheus
    /// metric has no change-notification channel, unlike
    /// [`Self::wait_for_image`], so this method polls. The 25ms cadence is an
    /// internal implementation detail, not a test-visible timing assumption.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_for_metrics<F>(&self, what: &str, mut predicate: F)
    where
        F: FnMut(&BrokerMetrics) -> bool,
    {
        let held = super::await_until(|| std::future::ready(predicate(&self.broker.metrics))).await;
        assert2::assert!(
            held,
            "wait_for_metrics({what}) timed out after {TEST_AWAITER_TIMEOUT:?}"
        );
    }

    /// Test-only: await until a non-zero controller leader is elected.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_controller_leader(&self) -> krabka_raft::NodeId {
        let mut rx = self.watch_leader_for_test();
        let res = tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                if let Some(id) = *rx.borrow_and_update()
                    && id != krabka_raft::NodeId(0)
                {
                    return id;
                }
                if rx.changed().await.is_err() {
                    return krabka_raft::NodeId(0);
                }
            }
        })
        .await;
        let id = res.expect("wait_until_controller_leader timed out after 30s");
        assert2::assert!(
            id != krabka_raft::NodeId(0),
            "leader channel closed before a leader was elected"
        );
        id
    }

    /// Test-only: await until this node's metadata image sees `>= n` brokers
    /// registered and unfenced.
    ///
    /// A new registration is fenced until its broker's first caught-up
    /// heartbeat, and a fenced broker takes no replica, so a test that goes on
    /// to place replicas waits for the unfenced registrations, as Kafka's
    /// harness waits for its brokers to be alive.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[track_caller]
    pub fn wait_until_brokers_registered(&self, n: usize) -> impl std::future::Future<Output = ()> {
        self.wait_for_image(move |img| img.brokers().filter(|broker| !broker.fenced).count() >= n)
    }

    /// Test-only: whether this node's controller liveness registry currently
    /// reports `node` as alive — a heartbeat inside the session window from an
    /// unfenced broker. Only the raft leader receives broker heartbeats, so
    /// this answers meaningfully on the controller leader alone.
    ///
    /// This is the predicate the operator `ElectLeaders` path consults, and it
    /// is *not* implied by the metadata image: a broker's registration and its
    /// ISR re-admission are replicated records, while its liveness is local
    /// heartbeat state that lands afterwards. Tests that drive an election
    /// must settle on this, not on the image.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn broker_alive_for_test(&self, node: u64) -> bool {
        self.broker.liveness.is_alive(node).await
    }

    /// Test-only: await until this node's controller liveness registry reports
    /// `node` as alive. See [`Self::broker_alive_for_test`] for why the
    /// metadata image is not a substitute.
    ///
    /// The registry has no change-notification channel, so this polls on the
    /// same ~25ms cadence as [`Self::wait_for_metrics`].
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_broker_alive(&self, node: u64) {
        let alive = super::await_until(|| self.broker.liveness.is_alive(node)).await;
        assert2::assert!(
            alive,
            "wait_until_broker_alive({node}) timed out after {TEST_AWAITER_TIMEOUT:?}"
        );
    }

    /// Test-only: await until this node would let an operator elect `node` --
    /// the predicate `ElectLeaders` itself reads.
    ///
    /// [`Self::wait_until_broker_alive`] settles on the controller's own
    /// heartbeat registry, which only the quorum leader keeps. `ElectLeaders`
    /// cannot read that, because `controllerId` names a rotating unfenced
    /// broker rather than the quorum leader and an `AdminClient` sends the
    /// election wherever that rotation last pointed. It reads the replicated
    /// set instead, which trails the registry by up to one liveness tick, so a
    /// test that drives an election over the wire settles here.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_broker_electable(&self, node: u64) {
        let electable = super::await_until(|| async {
            use crate::handlers::offline_replicas::live_brokers;

            let image = self.broker.controller.current_image();
            live_brokers(&self.broker, &image).await.contains(&node)
        })
        .await;
        assert2::assert!(
            electable,
            "wait_until_broker_electable({node}) timed out after {TEST_AWAITER_TIMEOUT:?}"
        );
    }

    /// Test-only: await until this node's image has applied metadata offset
    /// `offset` or later. See [`Self::metadata_offset_for_test`] for the offset
    /// to pass.
    ///
    /// The applied index advances inside the raft state machine and has no
    /// change-notification channel of its own, so this polls on the same ~25ms
    /// cadence as [`Self::wait_until_broker_alive`].
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_metadata_offset_at_least(&self, offset: i64) {
        let applied = super::await_until(|| {
            std::future::ready(self.broker.controller.current_metadata_offset() >= offset)
        })
        .await;
        assert2::assert!(
            applied,
            "wait_until_metadata_offset_at_least({offset}) timed out after \
             {TEST_AWAITER_TIMEOUT:?}; the image is at {}",
            self.broker.controller.current_metadata_offset()
        );
    }

    /// Test-only: await until `topic-partition` is present in the metadata image.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[track_caller]
    pub fn wait_until_partition_present(
        &self,
        topic: &str,
        partition: i32,
    ) -> impl std::future::Future<Output = ()> {
        self.wait_for_image(move |img| img.partition(topic, partition).is_some())
    }

    /// Test-only: await until `topic-partition`'s leader is some non-`exclude`
    /// node with a non-zero epoch.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[track_caller]
    pub fn wait_until_partition_leader_changed(
        &self,
        topic: &str,
        partition: i32,
        exclude: krabka_raft::NodeId,
    ) -> impl std::future::Future<Output = ()> {
        self.wait_for_image(move |img| {
            img.partition(topic, partition).is_some_and(|p| {
                p.leader != krabka_raft::NodeId(0)
                    && p.leader != exclude
                    && p.leader_epoch > krabka_metadata::LeaderEpoch(0)
            })
        })
    }

    /// Test-only: await until `topic-partition`'s ISR has exactly `len` members.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[track_caller]
    pub fn wait_until_isr_len(
        &self,
        topic: &str,
        partition: i32,
        len: usize,
    ) -> impl std::future::Future<Output = ()> {
        self.wait_for_image(move |img| {
            img.partition(topic, partition)
                .is_some_and(|p| p.isr.len() == len)
        })
    }
}

/// A [`BrokerHandle::wait_for_image`] that ran out of time, and the call site
/// that started it.
#[cfg(any(test, feature = "test-helpers"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ImageWaitTimeout {
    caller: &'static std::panic::Location<'static>,
}

#[cfg(any(test, feature = "test-helpers"))]
impl ImageWaitTimeout {
    /// The panic message, given the metadata offset the image had applied when
    /// the wait gave up.
    fn message(self, metadata_offset: i64) -> String {
        format!(
            "wait_for_image called at {} timed out after {TEST_AWAITER_TIMEOUT:?}; \
             the image is at metadata offset {metadata_offset}",
            self.caller
        )
    }
}

/// Wait on `rx` until `pred` holds, for at most [`TEST_AWAITER_TIMEOUT`].
///
/// A closed channel ends the wait as a success: the sender goes away only when
/// the broker shuts down. The caller location is taken when this function is
/// called, not when the future first runs.
#[cfg(any(test, feature = "test-helpers"))]
#[track_caller]
fn image_awaiter<F>(
    mut rx: tokio::sync::watch::Receiver<Arc<krabka_metadata::MetadataImage>>,
    pred: F,
) -> impl std::future::Future<Output = Result<(), ImageWaitTimeout>>
where
    F: Fn(&krabka_metadata::MetadataImage) -> bool,
{
    let caller = std::panic::Location::caller();
    async move {
        tokio::time::timeout(TEST_AWAITER_TIMEOUT, async {
            loop {
                // Scope the borrow so it is dropped before the await.
                if pred(&rx.borrow_and_update()) {
                    return;
                }
                if rx.changed().await.is_err() {
                    return;
                }
            }
        })
        .await
        .map_err(|_| ImageWaitTimeout { caller })
    }
}

#[cfg(test)]
mod tests;
