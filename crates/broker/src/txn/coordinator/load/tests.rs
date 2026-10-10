//! A leadership change moves `__transaction_state-0` between two coordinators
//! that share one partition log, as replication would.

use std::{path::Path, sync::Arc};

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_log::ProducerId;
use krabka_metadata::{MetadataImage, NodeId};
use tempfile::TempDir;

use super::*;
use crate::{
    partition_registry::PartitionRegistry,
    txn::{
        state::{TopicPartition, TxnEntry, TxnState},
        version::TxnVersion,
    },
};

const TID: &str = "tid-moved";
const P0: PartitionIndex = PartitionIndex(0);

fn image(leader: NodeId, leader_epoch: krabka_metadata::LeaderEpoch) -> MetadataImage {
    super::super::test_support::state_image(
        crate::txn::coordinator::test_support::StateImageSetup {
            leader,
            leader_epoch,
            replicas: &[NodeId(1), NodeId(2)],
            ..Default::default()
        },
    )
}

fn open_state_partition(dir: &Path) -> Arc<crate::partition::Partition> {
    crate::test_support::open_partition(
        dir,
        crate::test_support::StandalonePartitionSetup {
            topic: bootstrap::TOPIC,
            ..Default::default()
        },
    )
}

#[derive(Clone, Copy)]
enum ProducerEpochPersistence {
    CurrentOnly,
    IncludeLastEpoch,
}

fn coordinator(node: NodeId, partitions: &Arc<PartitionRegistry>) -> Arc<TxnCoordinator> {
    coordinator_persisting_last_epoch(node, partitions, ProducerEpochPersistence::CurrentOnly)
}

/// A coordinator that writes and reads `LastProducerEpoch` (tag 4) as Kafka
/// trunk does when `persist_last_epoch` is set, and as 4.3.1 does not.
fn coordinator_persisting_last_epoch(
    node: NodeId,
    partitions: &Arc<PartitionRegistry>,
    persist_last_epoch: ProducerEpochPersistence,
) -> Arc<TxnCoordinator> {
    let mut coordinator = super::super::test_support::coordinator_with_registry(
        node,
        Arc::clone(partitions),
        crate::test_support::PartitionCount(1),
    );
    coordinator.set_persist_last_producer_epoch(matches!(
        persist_last_epoch,
        ProducerEpochPersistence::IncludeLastEpoch
    ));
    Arc::new(coordinator)
}

fn prepared_entry() -> TxnEntry {
    let mut entry = TxnEntry::new_empty(TID.to_owned(), ProducerId(1000), 4, 60_000, 0);
    entry.state = TxnState::PrepareCommit;
    // Every append stamps the client's transaction version on the record.
    entry.client_transaction_version = 2;
    entry.partitions.insert(TopicPartition {
        topic: "orders".to_owned(),
        partition: P0,
    });
    entry
}

/// What one coordinator shows for `TID` after a step.
#[derive(Debug, PartialEq, Eq)]
struct View {
    coordinator_error: Option<i16>,
    entry: Option<TxnEntry>,
    producer_id_maps_to: Option<String>,
}

fn unavailable_view(error_code: i16) -> View {
    View {
        coordinator_error: Some(error_code),
        entry: None,
        producer_id_maps_to: None,
    }
}

async fn view(coordinator: &TxnCoordinator) -> View {
    let entry = match coordinator.get(TID) {
        Some(handle) => Some(handle.lock().await.clone()),
        None => None,
    };
    View {
        coordinator_error: coordinator.coordinator_error(TID).await,
        entry,
        producer_id_maps_to: coordinator.tid_for_pid(ProducerId(1000)),
    }
}

/// Kafka `TransactionCoordinator.onElection` loads the partition and hands
/// every `Prepare*` transaction to the marker channel. `onResignation` drops
/// the partition, and `getAndMaybeAddTransactionState` answers
/// `COORDINATOR_LOAD_IN_PROGRESS` while the load runs and `NOT_COORDINATOR`
/// after it.
#[tokio::test]
async fn a_leadership_change_loads_the_new_leader_and_unloads_the_old_one() {
    let dir = TempDir::new().expect("tempdir");
    let partitions = super::super::test_support::state_registry(dir.path());
    let first = coordinator(NodeId(1), &partitions);
    let second = coordinator(NodeId(2), &partitions);

    // Broker 1 leads at epoch 0 and writes a PrepareCommit.
    first
        .refresh_leader_partitions(&image(NodeId(1), krabka_metadata::LeaderEpoch(0)))
        .await
        .finished()
        .await;
    drop(
        second
            .refresh_leader_partitions(&image(NodeId(1), krabka_metadata::LeaderEpoch(0)))
            .await,
    );
    first
        .put(prepared_entry(), TxnVersion::Verified)
        .await
        .expect("persist the prepared transaction");
    check!(first.take_completion_requests().is_empty());
    check!(view(&second).await == unavailable_view(crate::codes::NOT_COORDINATOR));

    // Broker 2 is elected at epoch 1. Its load waits while an append holds
    // the state-partition lock, and it answers COORDINATOR_LOAD_IN_PROGRESS.
    let held_append = second.state_partition_writes[0].lock().await;
    let loads = second
        .refresh_leader_partitions(&image(NodeId(2), krabka_metadata::LeaderEpoch(1)))
        .await;
    check!(view(&second).await == unavailable_view(crate::codes::COORDINATOR_LOAD_IN_PROGRESS));
    drop(held_append);
    loads.finished().await;
    check!(
        view(&second).await
            == View {
                coordinator_error: None,
                entry: Some(prepared_entry()),
                producer_id_maps_to: Some(TID.to_owned()),
            }
    );
    check!(second.take_completion_requests() == vec![TID.to_owned()]);

    // Broker 1 applies the same election. It resigns and drops the
    // transaction and its producer id.
    drop(
        first
            .refresh_leader_partitions(&image(NodeId(2), krabka_metadata::LeaderEpoch(1)))
            .await,
    );
    check!(view(&first).await == unavailable_view(crate::codes::NOT_COORDINATOR));
    let refused = first.put(prepared_entry(), TxnVersion::Verified).await;
    assert!(refused.is_err(), "a resigned coordinator must not append");
}

/// An append checks the generation it started in before it publishes, as
/// Kafka's `appendTransactionToLog` callback refuses a changed coordinator
/// epoch. A new term refuses the generation of the old one.
#[tokio::test]
async fn a_new_term_refuses_the_generation_of_the_old_term() {
    let dir = TempDir::new().expect("tempdir");
    let partitions = super::super::test_support::state_registry(dir.path());
    let coordinator = coordinator(NodeId(1), &partitions);
    coordinator
        .refresh_leader_partitions(&image(NodeId(1), krabka_metadata::LeaderEpoch(0)))
        .await
        .finished()
        .await;

    let generation = coordinator
        .loaded_term(P0)
        .await
        .expect("loaded")
        .generation;
    let loads = coordinator
        .refresh_leader_partitions(&image(NodeId(1), krabka_metadata::LeaderEpoch(1)))
        .await;
    loads.finished().await;
    let newer = coordinator
        .loaded_term(P0)
        .await
        .expect("loaded again")
        .generation;
    let leaders = coordinator.leader_partitions.read().await;
    check!(TxnCoordinator::require_generation(&leaders, P0, generation).is_err());
    check!(TxnCoordinator::require_generation(&leaders, P0, newer).is_ok());
}

/// #892: under `unstable.api.versions.enable`, `last_producer_epoch`
/// (`TransactionLogValue`'s `LastProducerEpoch`, tag 4, which Kafka trunk
/// persists) survives the entry moving to a new coordinator through the log,
/// and a retried `InitProducerId` naming it is still admitted after the
/// reload. Kafka 4.3.1 keeps the last epoch in memory only and reloads
/// `NO_PRODUCER_EPOCH`, so the same retry is fenced there.
#[tokio::test]
async fn a_reload_keeps_last_producer_epoch_only_in_trunk_mode() {
    use krabka_verified::transaction::InitProducerIdIdentityDecision::{Fenced, Retry};

    // (mode, persist the tag, last epoch after the reload, the retry's verdict)
    for (mode, persist_last_epoch, reloaded_last_epoch, verdict) in [
        (
            "4.3.1",
            ProducerEpochPersistence::CurrentOnly,
            super::super::test_support::ProducerEpoch(-1),
            Fenced,
        ),
        (
            "trunk",
            ProducerEpochPersistence::IncludeLastEpoch,
            super::super::test_support::ProducerEpoch(4),
            Retry,
        ),
    ] {
        let dir = TempDir::new().expect("tempdir");
        let partitions = super::super::test_support::state_registry(dir.path());
        let first = coordinator_persisting_last_epoch(NodeId(1), &partitions, persist_last_epoch);
        let second = coordinator_persisting_last_epoch(NodeId(2), &partitions, persist_last_epoch);

        first
            .refresh_leader_partitions(&image(NodeId(1), krabka_metadata::LeaderEpoch(0)))
            .await
            .finished()
            .await;
        let mut entry = prepared_entry();
        // The entry's live epoch has moved past the one a failed epoch fence
        // recorded, per `prepareIncrementProducerEpoch`/`prepareProducerIdRotation`,
        // so a retry naming the recorded epoch is distinct from a fresh bump.
        entry.producer_epoch = 5;
        entry.last_producer_epoch = 4;
        entry.has_failed_epoch_fence = true;
        first
            .put(entry, TxnVersion::Verified)
            .await
            .expect("persist the entry with a recorded last epoch");

        second
            .refresh_leader_partitions(&image(NodeId(2), krabka_metadata::LeaderEpoch(1)))
            .await
            .finished()
            .await;

        let reloaded = view(&second).await.entry.expect("reloaded from disk");
        check!(
            reloaded.last_producer_epoch == reloaded_last_epoch.0,
            "{mode}"
        );

        // The retry names the epoch the entry held before it was bumped to 5.
        let decision = krabka_verified::transaction::init_producer_id_identity_decision(
            reloaded.producer_id.get(),
            reloaded.producer_epoch,
            reloaded.last_producer_epoch,
            reloaded.prev_producer_id.get(),
            reloaded.producer_id.get(),
            4,
        );
        check!(decision == verdict, "{mode}: the retry naming epoch 4");
    }
}

/// What a two-partition coordinator serves for `__transaction_state-0` after
/// its load.
#[derive(Debug, PartialEq, Eq)]
struct PartitionView {
    status: Option<LoadStatus>,
    coordinator_error: Option<i16>,
    entries: Vec<(String, Option<TxnEntry>)>,
}

/// Loads `__transaction_state-0` of a two-partition topic from `batches` and
/// shows what it serves for `tids`.
async fn load_view(
    batches: Vec<krabka_protocol::records::RecordBatch>,
    tids: &[&str],
) -> PartitionView {
    let dir = TempDir::new().expect("tempdir");
    let partitions = Arc::new(PartitionRegistry::new());
    let part = open_state_partition(dir.path());
    partitions.insert(bootstrap::TOPIC.into(), P0, Arc::clone(&part));
    {
        let mut log = part.log.lock().unwrap();
        for mut batch in batches {
            log.append(&mut batch).unwrap();
        }
    }
    let coordinator = Arc::new(TxnCoordinator::new(
        NodeId(1),
        Arc::clone(&partitions),
        Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
        2,
        krabka_units::mebibytes(1),
    ));

    let recovered = coordinator
        .recover(&image(NodeId(1), krabka_metadata::LeaderEpoch(0)))
        .await;

    assert!(recovered.is_ok());
    let mut entries = Vec::new();
    for tid in tids {
        let entry = match coordinator.get(tid) {
            Some(handle) => Some(handle.lock().await.clone()),
            None => None,
        };
        entries.push(((*tid).to_owned(), entry));
    }
    PartitionView {
        status: coordinator.load_status(P0).await,
        coordinator_error: coordinator.coordinator_error(tids[0]).await,
        entries,
    }
}

/// Kafka's `loadTransactionMetadata` catches every error of its replay, logs
/// it, and returns the transactions it loaded before the failing record.
/// `loadTransactionsForTxnTopicPartition` then installs those, takes the
/// partition out of `loadingPartitions`, and serves it. So a bad record leaves
/// the partition loaded with the transactions before it and none after it.
#[tokio::test]
async fn a_bad_record_loads_the_transactions_before_it_and_serves_them() {
    use krabka_protocol::records::{Record, RecordBatch};

    let tid_in = |partition: i32, skip: usize| {
        (0..1_000)
            .map(|n| format!("tid-{n}"))
            .filter(|tid| crate::txn::partitioner::partition_for_tid(tid, 2) == partition)
            .nth(skip)
            .expect("a transactional id in that partition")
    };
    let (first, second, misplaced) = (tid_in(0, 0), tid_in(0, 1), tid_in(1, 0));
    let record = |key: Option<Vec<u8>>, value: Option<Vec<u8>>| RecordBatch {
        records: vec![Record {
            key: key.map(Into::into),
            value: value.map(Into::into),
            ..Default::default()
        }],
        ..RecordBatch::default()
    };
    let key = |tid: &str| Some(crate::txn::log_record::encode_key(tid).unwrap());
    let value = |tid: &str, producer_id: i64| {
        let entry = TxnEntry::new_empty(tid.to_owned(), ProducerId(producer_id), 0, 60_000, 0);
        Some(crate::txn::log_record::encode_value(
            &entry,
            TxnVersion::Verified,
            false,
        ))
    };
    let tids = [first.as_str(), second.as_str(), misplaced.as_str()];
    let good = |tid: &str, producer_id: i64| record(key(tid), value(tid, producer_id));
    let before_only = load_view(vec![good(&first, 1)], &tids).await;
    let both = load_view(vec![good(&first, 1), good(&second, 2)], &tids).await;
    check!(before_only.status == Some(LoadStatus::Loaded));
    check!(before_only.entries[0].1.is_some() && both.entries[1].1.is_some());

    let mut corrupt = value(&misplaced, 3).unwrap();
    corrupt.truncate(9);
    let cases = [
        (
            "a corrupt value",
            record(key(&second), Some(corrupt)),
            &before_only,
        ),
        (
            "a record without a key",
            record(None, value(&second, 3)),
            &before_only,
        ),
        ("a misplaced transaction", good(&misplaced, 3), &before_only),
        (
            "a producer id another transaction holds",
            good(&second, 1),
            &before_only,
        ),
        (
            "an unknown value version, which is skipped",
            record(key(&second), Some(vec![0x00, 0x05])),
            &both,
        ),
    ];
    for (name, bad, expected) in cases {
        let view = load_view(vec![good(&first, 1), bad, good(&second, 2)], &tids).await;

        check!(view == *expected, "{name}");
    }
}
