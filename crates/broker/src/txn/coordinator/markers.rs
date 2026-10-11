//! Transaction-marker fan-out from the transaction coordinator.
//!
//! The module holds the `TxnCoordinator` methods that write `COMMIT` and
//! `ABORT` control markers to every partition a transaction touched. One path
//! fans the markers out to the partition leaders through the inter-broker
//! client, and the fallback path appends to locally-held partitions when no
//! marker transport is configured. Both count a marker as written only once
//! it is committed on its partition.

use super::TxnCoordinator;
use crate::{
    error::BrokerError,
    txn::{
        bootstrap,
        handlers::{
            end_txn::{MarkerDispatchContext, MarkerFanOut, dispatch_markers, write_local_markers},
            write_txn_markers::MarkerAppend,
        },
        marker::MarkerType,
        state::{TopicPartition, TxnEntry, TxnState},
    },
};
#[cfg(test)]
use crate::{log_dir_status::LogDirRegistry, producer_state::ProducerState};

const UNKNOWN_COORDINATOR_EPOCH: i32 = -1;

impl TxnCoordinator {
    /// Writes the markers of the prepared transaction `entry`, and then drops
    /// every partition whose marker is written from `entry` and from the live
    /// entry it was taken from. Kafka's `TransactionMarkerRequestCompletionHandler`
    /// calls `TransactionMetadata.removePartition` for each acknowledged
    /// partition, so `DescribeTransactions` of a preparing transaction lists
    /// only the partitions whose markers are outstanding, and a retry sends
    /// only those.
    pub(crate) async fn dispatch_transaction_markers(
        &self,
        entry: &mut TxnEntry,
        marker_type: MarkerType,
    ) -> Result<(), BrokerError> {
        #[cfg(any(test, feature = "test-helpers"))]
        self.marker_fanout_gate.pass().await?;
        let outcome = self.fan_out_markers(entry, marker_type).await;
        self.remove_marked_partitions(entry, &outcome.written).await;
        match outcome.failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn fan_out_markers(&self, entry: &TxnEntry, marker_type: MarkerType) -> MarkerFanOut {
        let Some(transport) = &self.marker_transport else {
            return self.dispatch_local_markers(entry, marker_type).await;
        };
        let image = transport.controller.current_image();
        let coordinator_partition = self.partition_for(&entry.transactional_id);
        // Kafka sends the coordinator epoch that the partition was loaded at,
        // not the epoch of a later image.
        let Some(coordinator_epoch) = self.loaded_leader_epoch(coordinator_partition).await else {
            return MarkerFanOut {
                written: Vec::new(),
                failure: Some(BrokerError::Txn(format!(
                    "this broker does not coordinate {}-{}",
                    bootstrap::TOPIC,
                    coordinator_partition.get()
                ))),
            };
        };
        dispatch_markers(
            MarkerDispatchContext {
                node_id: self.node_id,
                coordinator_epoch,
                image: &image,
                inter_broker_client: &transport.inter_broker_client,
                inter_broker_protocol: transport.protocol,
                inter_broker_listener_name: &transport.listener_name,
                inter_broker_server_name: &transport.server_name,
                group_coordinator: self.group_coordinator.as_ref(),
            },
            &self.partitions,
            entry,
            marker_type,
        )
        .await
    }

    /// Drops `written` from the prepared snapshot and, while it still holds
    /// the same prepared transaction, from the live entry.
    async fn remove_marked_partitions(&self, snapshot: &mut TxnEntry, written: &[TopicPartition]) {
        if written.is_empty() {
            return;
        }
        if let Some(handle) = self.get(&snapshot.transactional_id) {
            let mut live = handle.lock().await;
            remove_marked_partitions(&mut live, snapshot, written);
        }
        remove_marked_partitions_from(snapshot, written);
    }

    async fn dispatch_local_markers(
        &self,
        entry: &TxnEntry,
        marker_type: MarkerType,
    ) -> MarkerFanOut {
        let mut outcome = MarkerFanOut::default();
        let mut local = Vec::new();
        for tp in &entry.partitions {
            match self.partitions.get(&tp.topic, tp.partition) {
                Some(part) => local.push((tp.clone(), part)),
                None => outcome.fail(BrokerError::Txn(format!(
                    "transaction marker transport is not configured for remote partition {}-{}",
                    tp.topic,
                    tp.partition.get()
                ))),
            }
        }
        let marker = MarkerAppend {
            producer_id: entry.producer_id,
            producer_epoch: entry.producer_epoch,
            marker_type,
            coordinator_epoch: UNKNOWN_COORDINATOR_EPOCH,
            commit_stamp: None,
            transaction_version: entry.client_transaction_version,
        };
        outcome.merge(
            write_local_markers(self.node_id, self.group_coordinator.as_ref(), marker, local).await,
        );
        outcome
    }
}

/// Kafka's `TransactionMetadata.removePartition`: only a transaction that is
/// preparing to commit or abort loses a partition to its marker. `live` loses
/// `written` only while it is the same prepared transaction as `prepared`.
fn remove_marked_partitions(live: &mut TxnEntry, prepared: &TxnEntry, written: &[TopicPartition]) {
    let same_prepared = matches!(live.state, TxnState::PrepareCommit | TxnState::PrepareAbort)
        && live.state == prepared.state
        && live.producer_id == prepared.producer_id
        && live.producer_epoch == prepared.producer_epoch;
    if same_prepared {
        remove_marked_partitions_from(live, written);
    }
}

fn remove_marked_partitions_from(entry: &mut TxnEntry, written: &[TopicPartition]) {
    if matches!(
        entry.state,
        TxnState::PrepareCommit | TxnState::PrepareAbort
    ) {
        for tp in written {
            entry.partitions.remove(tp);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use assert2::{assert, check};
    use krabka_ids::PartitionIndex;
    use krabka_log::{Log, LogConfig, ProducerId};

    use super::*;
    use crate::{
        broker::spawn_partition, log_dir::partition_dir,
        txn::coordinator::test_support::test_coordinator,
    };

    #[test]
    fn local_marker_uses_the_unknown_coordinator_epoch_sentinel() {
        check!(UNKNOWN_COORDINATOR_EPOCH == -1);
    }

    fn tp(topic: &str) -> TopicPartition {
        TopicPartition {
            topic: topic.to_string(),
            partition: PartitionIndex(0),
        }
    }

    fn prepared(state: TxnState, epoch: i16) -> TxnEntry {
        let mut entry = TxnEntry::new_empty("tid-m".into(), ProducerId(9), epoch, 60_000, 0);
        entry.state = state;
        entry.partitions = [tp("a"), tp("b")].into_iter().collect();
        entry
    }

    /// Kafka's `TransactionMetadata.removePartition` runs only while the
    /// transaction prepares, and only for the transaction whose markers were
    /// written.
    #[test]
    fn only_the_same_prepared_transaction_loses_its_marked_partitions() {
        let snapshot = prepared(TxnState::PrepareCommit, 3);
        // (label, live entry, partitions it keeps)
        let cases = [
            (
                "the same prepared transaction",
                prepared(TxnState::PrepareCommit, 3),
                vec![tp("b")],
            ),
            (
                "a completed transaction",
                prepared(TxnState::CompleteCommit, 3),
                vec![tp("a"), tp("b")],
            ),
            (
                "a newer producer epoch",
                prepared(TxnState::PrepareCommit, 4),
                vec![tp("a"), tp("b")],
            ),
            (
                "the other prepared outcome",
                prepared(TxnState::PrepareAbort, 3),
                vec![tp("a"), tp("b")],
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (label, mut live, kept) in cases {
            remove_marked_partitions(&mut live, &snapshot, &[tp("a")]);
            actual.push((label, live.partitions));
            expected.push((label, kept.into_iter().collect::<HashSet<_>>()));
        }
        assert!(actual == expected);
    }

    /// #852: after a fan-out in which one partition takes its marker and the
    /// other does not, the live transaction and the caller's snapshot both
    /// hold only the partition whose marker is outstanding, which is what
    /// `DescribeTransactions` reports.
    #[tokio::test]
    async fn a_written_marker_removes_its_partition_from_the_prepared_transaction() {
        let coordinator = test_coordinator();
        let dir = tempfile::tempdir().expect("temp dir");
        let part_dir = partition_dir(dir.path(), "a", 0);
        std::fs::create_dir_all(&part_dir).expect("partition dir");
        let local = spawn_partition(
            "a".to_string(),
            PartitionIndex(0),
            dir.path().to_path_buf(),
            Log::open(&part_dir, LogConfig::default()).expect("open log"),
            LogDirRegistry::default(),
            Arc::new(ProducerState::new()),
            false,
        );
        // The metadata reconcile installs this broker as the leader.
        local.install_leader_change(1, 0).await;
        coordinator
            .partitions
            .insert("a".into(), PartitionIndex(0), local);
        let mut snapshot = prepared(TxnState::PrepareCommit, 3);
        let live = Arc::new(tokio::sync::Mutex::new(snapshot.clone()));
        coordinator
            .state
            .insert(snapshot.transactional_id.clone(), live.clone());

        // `b-0` is neither local nor reachable, so its marker fails.
        let result = coordinator
            .dispatch_transaction_markers(&mut snapshot, MarkerType::Commit)
            .await;

        let outstanding: HashSet<TopicPartition> = [tp("b")].into_iter().collect();
        assert!(result.is_err());
        assert!(
            (live.lock().await.partitions.clone(), snapshot.partitions)
                == (outstanding.clone(), outstanding)
        );
    }
}
