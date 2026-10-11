//! The load and unload of `__transaction_state` partitions on a leadership
//! change.
//!
//! [`TxnCoordinator::refresh_leader_partitions`] applies an image to the
//! leadership map (see `leadership`). A partition that another broker leads
//! now, or that starts a new term, loses its in-memory transactions at once.
//! A partition that this broker leads in a new term is replayed in a
//! background task, as Kafka's `TransactionStateManager` does on its
//! scheduler. The replay publishes its transactions only if the term is still
//! the one it loads for, and then it queues every `Prepare*` transaction for
//! completion.

use std::{sync::Arc, time::Duration};

use krabka_ids::PartitionIndex;
use krabka_metadata::MetadataImage;
use tracing::{info, warn};

use super::{
    TxnCoordinator,
    leadership::{self, LoadStatus, StatePartitionLeaders},
    persistence::replay_partition,
    pid_index::RecoveredTransactions,
};
use crate::{
    blocking::spawn_blocking,
    error::BrokerError,
    txn::{bootstrap, partitioner::partition_for_tid},
};

crate::task_util::scheduled_tasks_type! {
    /// The load tasks that one refresh started.
    ///
    /// A caller that does not wait drops this value. The tasks run on.
    pub(crate) struct ScheduledLoads;
    /// Waits until every load task of this refresh has ended.
    |error| {
        warn!(%error, "__transaction_state load task failed");
    }
}

impl TxnCoordinator {
    /// Applies the `__transaction_state` leadership of `image`, and starts the
    /// loads that it asks for.
    ///
    /// For a partition that this broker leads at a new leader epoch, the
    /// method drops the in-memory transactions of the partition and replays
    /// its log in a background task (Kafka `TransactionCoordinator.onElection`).
    /// For a partition that another broker leads now, it drops them
    /// (`onResignation`). An image that is older than one already applied
    /// changes nothing. The reconcile loop calls this method on every
    /// metadata change, and the transaction handlers call it before they look
    /// up a transaction.
    pub(crate) async fn refresh_leader_partitions(
        self: &Arc<Self>,
        image: &MetadataImage,
    ) -> ScheduledLoads {
        // Most refreshes change nothing. Check that under the read guard, so
        // a refresh does not wait for the appends that publish.
        let unchanged = {
            let leaders = self.leader_partitions.read().await;
            let mut probe = leaders.clone();
            leadership::apply_image(&mut probe, self.node_id, image, |p| self.is_local(p), || 0)
                .is_empty()
                && probe == *leaders
        };
        if unchanged {
            return ScheduledLoads::default();
        }
        let changes = {
            let mut leaders = self.leader_partitions.write().await;
            let changes = leadership::apply_image(
                &mut leaders,
                self.node_id,
                image,
                |p| self.is_local(p),
                || {
                    self.next_generation
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                },
            );
            // Drop the old transactions while the write guard holds, so no
            // publication sees a half-dropped partition.
            for partition in &changes.unload {
                self.drop_partition_state(*partition);
            }
            changes
        };
        // A write that waits to commit in a term that just ended stops
        // waiting, and the load of the next term can take the partition's
        // write lock from it.
        self.leadership_changed.notify_waiters();
        for partition in &changes.unload {
            info!(
                partition = partition.get(),
                "unloaded __transaction_state partition"
            );
        }
        ScheduledLoads(
            changes
                .load
                .into_iter()
                .map(|(partition, generation)| {
                    let coordinator = Arc::clone(self);
                    tokio::spawn(async move {
                        coordinator.load_partition(partition, generation).await;
                    })
                })
                .collect(),
        )
    }

    /// Loads every `__transaction_state` partition that `image` says this
    /// broker leads, and waits for the loads. `Broker::start` calls it.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Txn`] naming the partitions whose load failed.
    /// Those partitions answer `NOT_COORDINATOR` until the next election.
    #[tracing::instrument(name = "txn_coordinator_recover", level = "info", skip_all, err)]
    pub(crate) async fn recover(
        self: &Arc<Self>,
        image: &MetadataImage,
    ) -> Result<(), BrokerError> {
        self.refresh_leader_partitions(image).await.finished().await;
        let leaders = self.leader_partitions.read().await;
        let mut failed: Vec<i32> = leaders
            .iter()
            .filter(|(_, leadership)| {
                leadership
                    .term
                    .is_some_and(|term| term.status == LoadStatus::Failed)
            })
            .map(|(partition, _)| partition.get())
            .collect();
        drop(leaders);
        info!(
            tids_loaded = self.state.len(),
            "TxnCoordinator recovery complete"
        );
        if failed.is_empty() {
            return Ok(());
        }
        failed.sort_unstable();
        Err(BrokerError::Txn(format!(
            "__transaction_state partitions {failed:?} failed to load"
        )))
    }

    /// Whether any `__transaction_state` partition of this broker is still
    /// loading.
    ///
    /// Kafka's `listTransactionStates` answers
    /// `COORDINATOR_LOAD_IN_PROGRESS` while `loadingPartitions` is not empty,
    /// because the list it could build would be short of the transactions in
    /// those partitions.
    pub(crate) async fn any_partition_loading(&self) -> bool {
        self.leader_partitions
            .read()
            .await
            .values()
            .any(|leadership| {
                leadership.term.is_some_and(|term| {
                    matches!(term.status, LoadStatus::Pending | LoadStatus::Loading)
                })
            })
    }

    /// Waits up to `timeout` until the load of `partition` ends, and returns
    /// its load status then.
    pub(crate) async fn wait_for_load(
        &self,
        partition: PartitionIndex,
        timeout: Duration,
    ) -> Option<LoadStatus> {
        let wait = async {
            loop {
                let finished = self.load_finished.notified();
                let status = self.load_status(partition).await;
                if !matches!(status, Some(LoadStatus::Loading)) {
                    return status;
                }
                finished.await;
            }
        };
        match tokio::time::timeout(timeout, wait).await {
            Ok(status) => status,
            Err(_elapsed) => self.load_status(partition).await,
        }
    }

    /// The partition leader epoch of the loaded term of `partition`. The
    /// marker fan-out sends it as the coordinator epoch.
    pub(super) async fn loaded_leader_epoch(&self, partition: PartitionIndex) -> Option<i32> {
        let leaders = self.leader_partitions.read().await;
        leadership::loaded_generation(&leaders, partition)?;
        leaders
            .get(&partition)
            .map(|leadership| leadership.leader_epoch.0)
    }

    /// Checks that `partition` is still loaded in the term of `generation`.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::TransactionStateWriteUncommitted`] when the term
    /// changed, with the error Kafka's `appendTransactionToLog` callback
    /// answers for a changed coordinator epoch (see
    /// [`TxnCoordinator::term_error`]).
    pub(super) fn require_generation(
        leaders: &StatePartitionLeaders,
        partition: PartitionIndex,
        generation: u64,
    ) -> Result<(), BrokerError> {
        if leadership::loaded_generation(leaders, partition) == Some(generation) {
            return Ok(());
        }
        Err(Self::term_error(leaders, partition))
    }

    fn is_local(&self, partition: PartitionIndex) -> bool {
        self.partitions.get(bootstrap::TOPIC, partition).is_some()
    }

    /// Removes every transaction and producer id mapping of `partition`.
    fn drop_partition_state(&self, partition: PartitionIndex) {
        let _pid_install = self
            .pid_install
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.state
            .retain(|tid, _| self.partition_for(tid) != partition);
        self.pid_to_tid
            .retain(|_, tid| self.partition_for(tid) != partition);
    }

    /// Replays `partition` for the term of `generation` and publishes the
    /// result if the term did not change.
    // cargo-mutants: orchestration over the log, the blocking pool and the
    // leadership lock. `replay_partition`, `RecoveredTransactions` and
    // `leadership::apply_image` carry the decisions.
    #[cfg_attr(test, mutants::skip)]
    async fn load_partition(self: Arc<Self>, partition: PartitionIndex, generation: u64) {
        let index = usize::try_from(partition.get())
            .expect("transaction state partition index must be nonnegative");
        // An append holds this lock across its write, so the replay starts
        // after every append that began in an older term.
        let state_partition_write = self.state_partition_writes[index].lock().await;
        // Kafka's `loadTransactionMetadata` never fails: a replay error ends
        // the replay with what it loaded so far, and a partition without a
        // log loads empty. Only a replay task that dies fails the load here.
        let replay = if let Some(part) = self.partitions.get(bootstrap::TOPIC, partition) {
            let read_max = self.recovery_read_max;
            let last_epoch_tag = self.persist_last_producer_epoch;
            let num_partitions = self.num_partitions;
            spawn_blocking(move || {
                Ok(replay_partition(
                    &part,
                    partition,
                    (read_max, last_epoch_tag),
                    |tid| PartitionIndex(partition_for_tid(tid, num_partitions)),
                ))
            })
            .await
            .unwrap_or_else(|error| {
                Err(BrokerError::Txn(format!(
                    "__transaction_state-{partition} replay task failed: {error}"
                )))
            })
        } else {
            warn!(
                "Attempted to load transaction metadata from {}-{partition}, but found no log",
                bootstrap::TOPIC
            );
            Ok(RecoveredTransactions::default())
        };
        let prepared = self.publish_load(partition, generation, replay).await;
        drop(state_partition_write);
        self.load_finished.notify_waiters();
        // Kafka removes the partition from `loadingPartitions` first, and then
        // hands every loaded `Prepare*` transaction to the marker channel.
        for tid in &prepared {
            self.request_completion(tid);
        }
    }

    /// Publishes a replay if `partition` is still loading for `generation`,
    /// and returns the `Prepare*` transactions it published.
    async fn publish_load(
        &self,
        partition: PartitionIndex,
        generation: u64,
        replay: Result<RecoveredTransactions, BrokerError>,
    ) -> Vec<String> {
        let mut leaders = self.leader_partitions.write().await;
        let Some(term) = leaders
            .get_mut(&partition)
            .and_then(|leadership| leadership.term.as_mut())
            .filter(|term| term.generation == generation && term.status == LoadStatus::Loading)
        else {
            info!(
                partition = partition.get(),
                "a newer term replaced this __transaction_state load"
            );
            return Vec::new();
        };
        let published = replay.and_then(|recovered| self.install_recovered(recovered));
        match published {
            Ok(prepared) => {
                term.status = LoadStatus::Loaded;
                info!(
                    partition = partition.get(),
                    prepared = prepared.len(),
                    "loaded __transaction_state partition"
                );
                prepared
            }
            Err(error) => {
                term.status = LoadStatus::Failed;
                warn!(partition = partition.get(), %error, "__transaction_state load failed");
                Vec::new()
            }
        }
    }

    /// Publishes the transactions of one replayed partition, and returns the
    /// `Prepare*` ones. The caller holds the leadership write guard.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Txn`], and publishes nothing, when a replayed
    /// producer id belongs to a transaction of another partition.
    fn install_recovered(
        &self,
        recovered: RecoveredTransactions,
    ) -> Result<Vec<String>, BrokerError> {
        let _pid_install = self
            .pid_install
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((pid, tid)) = recovered.pid_to_tid.iter().find(|(pid, tid)| {
            self.pid_to_tid
                .get(pid)
                .is_some_and(|owner| owner.value() != *tid)
        }) {
            return Err(BrokerError::Txn(format!(
                "transaction {tid} reuses producer ID {} owned by another transaction",
                pid.get()
            )));
        }
        let mut prepared = Vec::new();
        for (tid, entry) in recovered.state {
            if super::completion::completion_for(entry.state).is_some() {
                prepared.push(tid.clone());
            }
            self.state
                .insert(tid, Arc::new(tokio::sync::Mutex::new(entry)));
        }
        for (pid, tid) in recovered.pid_to_tid {
            self.pid_to_tid.insert(pid, tid);
        }
        prepared.sort_unstable();
        Ok(prepared)
    }
}

#[cfg(test)]
mod tests;
