//! Per-broker `TxnCoordinator`.
//!
//! The coordinator owns the in-memory state map of every `transactional_id`
//! whose `__transaction_state` partition this broker leads. It persists every
//! state change as a record in the matching `__transaction_state` partition.
//! It loads a partition when this broker becomes its leader, and it unloads
//! the partition when this broker stops leading it (see `leadership`).

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex as StdMutex, atomic::AtomicU64},
};

use dashmap::DashMap;
use krabka_ids::PartitionIndex;
use krabka_log::ProducerId;
use krabka_security::ListenerProtocol;
use krabka_units::ByteSize;
use tokio::sync::{Mutex, Notify, RwLock};

use crate::{
    partition_registry::PartitionRegistry,
    task_util::cloned_registry_values,
    txn::{partitioner::partition_for_tid, state::TxnEntry},
};

/// The commit rule of a `__transaction_state` write.
mod commit;
/// KIP-98 transactional-id expiry: the decision core and the sweep over the
/// tids this broker coordinates. [`crate::txn::id_expiration`] ticks it.
/// Completion of transactions whose `Prepare*` record is durable.
pub(crate) mod completion;
pub(crate) mod expiry;
#[cfg(any(test, feature = "test-helpers"))]
pub(crate) mod fanout_gate;
pub(crate) mod leadership;
mod load;
mod markers;
mod persistence;
mod pid_index;
pub(crate) mod produce_verification;
mod reaper;
mod registration;

#[cfg(test)]
mod test_support;

/// Per-broker transaction coordinator. `Broker::start` constructs it and
/// shares it with the transaction wire handlers through an `Arc`.
pub(crate) struct TxnCoordinator {
    pub(crate) node_id: krabka_metadata::NodeId,
    pub(crate) partitions: Arc<PartitionRegistry>,
    pub(crate) producer_ids: Arc<crate::producer_id_manager::ProducerIdManager>,
    num_partitions: i32,
    recovery_read_max: ByteSize,
    /// Live in-memory state: `transactional_id` → locked `TxnEntry`.
    state: DashMap<String, Arc<Mutex<TxnEntry>>>,
    /// Serializes durable state writes by `__transaction_state` partition.
    /// The reaper holds the matching lock across its post-marker recheck and
    /// completion append so no staged writer can slip between them.
    state_partition_writes: Vec<Mutex<()>>,
    /// The newest known leadership of each `__transaction_state` partition.
    /// [`leadership::apply_image`] orders updates by leader epoch.
    leader_partitions: RwLock<leadership::StatePartitionLeaders>,
    /// Reverse lookup: `producer_id` → `transactional_id`. The Produce
    /// handler reads it to verify transactional batches (KIP-1319 v2).
    pid_to_tid: DashMap<ProducerId, String>,
    /// Serializes the multi-key PID ownership check and publication after a
    /// durable transaction-state append.
    pid_install: StdMutex<()>,
    /// Source of [`leadership::LeaderTerm::generation`].
    next_generation: AtomicU64,
    /// Wakes every `__transaction_state` write that waits to commit when a
    /// term starts or ends, so a write whose term ended stops waiting.
    leadership_changed: Notify,
    /// The metadata that names the leader of each `__transaction_state`
    /// partition. A write waits to commit only while it names this broker at
    /// the epoch of the write's term. `None` in tests that build no
    /// controller; such a write follows the coordinator's own terms only.
    metadata: Option<Arc<dyn crate::metadata_source::MetadataSource>>,
    /// Wakes the callers of [`Self::wait_for_load`] when a load ends.
    load_finished: Notify,
    /// Transactional ids whose `Prepare*` record is durable, queued for
    /// [`crate::txn::completion`] by recovery or by a failed request.
    pending_completions: StdMutex<BTreeSet<String>>,
    /// Wakes [`crate::txn::completion`] when an id joins
    /// `pending_completions`.
    completion_requested: Notify,
    marker_transport: Option<MarkerTransport>,
    /// The open connection to each remote transaction coordinator that a
    /// `Produce` transaction check reached, reused for the next check.
    verification_connections:
        Mutex<std::collections::HashMap<krabka_metadata::NodeId, VerificationConnection>>,
    group_coordinator: Option<Arc<crate::coordinator::GroupCoordinator>>,
    /// Whether `__transaction_state` values carry `LastProducerEpoch` (tag 4).
    /// Kafka trunk persists it (KAFKA-20357); 4.3.1 keeps it in memory only, so
    /// this is on only under `unstable.api.versions.enable`.
    persist_last_producer_epoch: bool,
    /// Test gate in front of every transaction-marker fan-out.
    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) marker_fanout_gate: fanout_gate::MarkerFanoutGate,
}

/// A connection a transaction check keeps open to a remote coordinator, and
/// the endpoint it was dialed at.
struct VerificationConnection {
    address: (String, u16),
    connection: krabka_client_core::Connection,
}

struct MarkerTransport {
    controller: Arc<dyn crate::metadata_source::MetadataSource>,
    inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
    protocol: ListenerProtocol,
    listener_name: String,
    server_name: String,
}

impl TxnCoordinator {
    pub(crate) fn new(
        node_id: krabka_metadata::NodeId,
        partitions: Arc<PartitionRegistry>,
        producer_ids: Arc<crate::producer_id_manager::ProducerIdManager>,
        num_partitions: i32,
        recovery_read_max: ByteSize,
    ) -> Self {
        let state_partition_count = usize::try_from(num_partitions)
            .expect("transaction state partition count must be nonnegative");
        Self {
            node_id,
            partitions,
            producer_ids,
            num_partitions,
            recovery_read_max,
            state: DashMap::new(),
            state_partition_writes: (0..state_partition_count).map(|_| Mutex::new(())).collect(),
            leader_partitions: RwLock::new(leadership::StatePartitionLeaders::new()),
            pid_to_tid: DashMap::new(),
            pid_install: StdMutex::new(()),
            next_generation: AtomicU64::new(0),
            leadership_changed: Notify::new(),
            metadata: None,
            load_finished: Notify::new(),
            pending_completions: StdMutex::new(BTreeSet::new()),
            completion_requested: Notify::new(),
            marker_transport: None,
            verification_connections: Mutex::new(std::collections::HashMap::new()),
            group_coordinator: None,
            persist_last_producer_epoch: false,
            #[cfg(any(test, feature = "test-helpers"))]
            marker_fanout_gate: fanout_gate::MarkerFanoutGate::default(),
        }
    }

    /// Persist and reload `LastProducerEpoch` (tag 4 of `TransactionLogValue`),
    /// as Kafka trunk does. `Broker::start` turns it on under
    /// `unstable.api.versions.enable`.
    pub(crate) fn set_persist_last_producer_epoch(&mut self, enabled: bool) {
        self.persist_last_producer_epoch = enabled;
    }

    pub(crate) fn configure_marker_transport(
        &mut self,
        controller: Arc<dyn crate::metadata_source::MetadataSource>,
        inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
        protocol: ListenerProtocol,
        listener_name: String,
        server_name: String,
        group_coordinator: Arc<crate::coordinator::GroupCoordinator>,
    ) {
        self.metadata = Some(Arc::clone(&controller));
        self.marker_transport = Some(MarkerTransport {
            controller,
            inter_broker_client,
            protocol,
            listener_name,
            server_name,
        });
        self.group_coordinator = Some(group_coordinator);
    }

    /// Test-only: the metadata a `__transaction_state` write checks its term
    /// against, without the marker transport that sets it in a broker.
    #[cfg(test)]
    pub(crate) fn set_metadata_source_for_test(
        &mut self,
        metadata: Arc<dyn crate::metadata_source::MetadataSource>,
    ) {
        self.metadata = Some(metadata);
    }

    /// Test-only: makes this broker the loaded leader of `partition` without
    /// an image.
    #[cfg(test)]
    pub(crate) async fn lead_state_partition_for_test(&self, partition: PartitionIndex) {
        self.leader_partitions.write().await.insert(
            partition,
            leadership::StatePartitionLeadership {
                topic_id: uuid::Uuid::nil(),
                leader_epoch: krabka_metadata::LeaderEpoch(0),
                term: Some(leadership::LeaderTerm {
                    generation: self
                        .next_generation
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    status: leadership::LoadStatus::Loaded,
                }),
            },
        );
    }

    /// Returns the `__transaction_state` partition index responsible for `tid`.
    pub(crate) fn partition_for(&self, tid: &str) -> PartitionIndex {
        PartitionIndex(partition_for_tid(tid, self.num_partitions))
    }

    /// Returns `true` if this broker is the transaction coordinator for `tid`
    /// and its `__transaction_state` partition is loaded.
    pub(crate) async fn is_coordinator_for(&self, tid: &str) -> bool {
        self.coordinator_error(tid).await.is_none()
    }

    /// The Kafka error code for a request about `tid`, or `None` when this
    /// broker coordinates `tid` and its partition is loaded.
    ///
    /// The code is `COORDINATOR_LOAD_IN_PROGRESS` while the partition loads,
    /// and `NOT_COORDINATOR` when this broker does not lead the partition or
    /// its load failed.
    pub(crate) async fn coordinator_error(&self, tid: &str) -> Option<i16> {
        leadership::coordinator_error(self.load_status(self.partition_for(tid)).await)
    }

    /// The error code for a failed append about `tid`.
    ///
    /// A write that did not commit answers the coordinator error Kafka's
    /// `appendTransactionToLog` answers for it. Any other failure answers the
    /// coordinator error when the coordinator term changed, and
    /// `UNKNOWN_SERVER_ERROR` otherwise.
    pub(crate) async fn append_error_code(
        &self,
        tid: &str,
        error: &crate::error::BrokerError,
    ) -> i16 {
        if let crate::error::BrokerError::TransactionStateWriteUncommitted { code, .. } = error {
            return *code;
        }
        self.coordinator_error(tid)
            .await
            .unwrap_or(crate::codes::UNKNOWN_SERVER_ERROR)
    }

    /// The error code for a request about `tid` whose entry lookup missed.
    ///
    /// A leadership change can unload the coordinator partition, and so evict
    /// its entries, between a caller's `coordinator_error` check and its
    /// lookup. This reads the coordinator status again after the miss: when
    /// it names an error, the miss is the unload, and the caller answers the
    /// retriable `COORDINATOR_LOAD_IN_PROGRESS` or `NOT_COORDINATOR`.
    /// Otherwise the id is unknown, which is `INVALID_PRODUCER_ID_MAPPING`.
    pub(crate) async fn missing_entry_error(&self, tid: &str) -> i16 {
        self.coordinator_error(tid)
            .await
            .unwrap_or(crate::codes::INVALID_PRODUCER_ID_MAPPING)
    }

    /// The load status of `partition`, or `None` when this broker does not
    /// lead it.
    pub(crate) async fn load_status(
        &self,
        partition: PartitionIndex,
    ) -> Option<leadership::LoadStatus> {
        leadership::status(&*self.leader_partitions.read().await, partition)
    }

    /// Returns the locked `TxnEntry` for `tid`, or `None` if `tid` is
    /// unknown.
    pub(crate) fn get(&self, tid: &str) -> Option<Arc<Mutex<TxnEntry>>> {
        self.state.get(tid).map(|e| e.value().clone())
    }

    /// Returns `true` if `handle` is still the entry registered for `tid`.
    ///
    /// Every durable append publishes a new handle, so a caller that read a
    /// handle before it waited on a lock checks this before it acts.
    pub(crate) fn is_current_entry(&self, tid: &str, handle: &Arc<Mutex<TxnEntry>>) -> bool {
        self.get(tid)
            .is_some_and(|current| Arc::ptr_eq(&current, handle))
    }

    /// Returns the `transactional_id` that `producer_id` was registered
    /// under, or `None` if the pid is unknown.
    #[cfg(test)]
    pub(crate) fn tid_for_pid(&self, pid: ProducerId) -> Option<String> {
        self.pid_to_tid.get(&pid).map(|e| e.value().clone())
    }

    /// Snapshots every locally-coordinated `TxnEntry`.
    ///
    /// The KIP-664 admin handlers `ListTransactions` and
    /// `DescribeTransactions` call this to expose the in-memory txn-state map.
    /// The method locks and clones each entry in turn, so the snapshot is
    /// consistent for one tid but not across the whole batch. That is
    /// acceptable for an admin introspection API, and Apache Kafka's JVM
    /// coordinator has the same property.
    pub(crate) async fn snapshot(&self) -> Vec<TxnEntry> {
        // Collect the `Arc<Mutex<_>>` handles first so we don't hold the
        // DashMap shard locks while taking the inner async mutex.
        let handles = cloned_registry_values(&self.state);
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            let entry = h.lock().await;
            out.push(entry.clone());
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::txn::coordinator::test_support::{
        test_coordinator, test_coordinator_with_partitions,
    };

    #[test]
    fn partition_for_maps_tid_via_java_hash_over_num_partitions() {
        // Canonical JVM String.hashCode vectors (see `partitioner` tests) with N=50.
        // Pins the real mapping so a
        // constant `PartitionIndex(0)` (the Default) is caught: none of these
        // hash to 0.
        let coordinator = test_coordinator();
        check!(coordinator.partition_for("my-tid") == PartitionIndex(20));
        check!(coordinator.partition_for("producer-1") == PartitionIndex(30));
        check!(coordinator.partition_for("tx-orders-prod") == PartitionIndex(16));
    }

    /// Regression: a leadership change can unload the coordinator partition
    /// between a caller's `coordinator_error` check and its entry lookup. A
    /// miss answers the fresh coordinator error, and only a coordinator that
    /// still owns the loaded partition answers `INVALID_PRODUCER_ID_MAPPING`.
    #[tokio::test]
    async fn a_missing_entry_answers_the_fresh_coordinator_error() {
        let coordinator = test_coordinator();
        let tid = "tid-missing";
        let unloaded = coordinator.missing_entry_error(tid).await;
        coordinator
            .lead_state_partition_for_test(coordinator.partition_for(tid))
            .await;
        let loaded = coordinator.missing_entry_error(tid).await;
        check!(
            (unloaded, loaded)
                == (
                    crate::codes::NOT_COORDINATOR,
                    crate::codes::INVALID_PRODUCER_ID_MAPPING
                )
        );
    }

    #[test]
    fn nondefault_partition_count_changes_coordinator_routing() {
        let coordinator = test_coordinator_with_partitions(crate::test_support::PartitionCount(7));
        check!(
            coordinator.partition_for("my-tid") == PartitionIndex(partition_for_tid("my-tid", 7))
        );
    }
}
