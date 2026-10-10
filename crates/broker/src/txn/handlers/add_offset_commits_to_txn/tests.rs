use std::sync::Arc;

use assert2::{assert, check};

use super::*;
use crate::txn::bootstrap;

const TID: &str = "tid-offsets";
const NOW: i64 = 1_000_000;

fn offsets_partition(index: i32) -> TopicPartition {
    TopicPartition {
        topic: OFFSETS_TOPIC.to_string(),
        partition: PartitionIndex(index),
    }
}

fn entry(state: TxnState, partitions: &[TopicPartition]) -> TxnEntry {
    let mut entry = TxnEntry::new_empty(TID.into(), ProducerId(7), 3, 60_000, 0);
    entry.state = state;
    // Every append stamps the client's transaction version on the record.
    entry.client_transaction_version = 2;
    entry.partitions.extend(partitions.iter().cloned());
    entry
}

#[test]
fn the_response_carries_the_error_code() {
    for code in [codes::NONE, codes::NOT_COORDINATOR] {
        check!(
            AddOffsetsToTxnResponse::error(code)
                == AddOffsetsToTxnResponse {
                    error_code: code,
                    ..Default::default()
                }
        );
    }
}

/// Kafka `TransactionCoordinator.handleAddPartitionsToTransaction`.
#[test]
fn the_decision_follows_kafka_order() {
    let mut staged = entry(TxnState::Ongoing, &[]);
    staged.next_producer_id = ProducerId(8);
    staged.next_producer_epoch = 0;
    let cases = [
        (
            "a pending transition comes first",
            {
                let mut pending = staged.clone();
                pending.producer_id = ProducerId(99);
                pending
            },
            (ProducerId(7), 3),
            AddOffsetsDecision::Answer(codes::CONCURRENT_TRANSACTIONS),
        ),
        (
            "another producer id",
            entry(TxnState::Ongoing, &[]),
            (ProducerId(8), 3),
            AddOffsetsDecision::Answer(codes::INVALID_PRODUCER_ID_MAPPING),
        ),
        (
            "another epoch",
            entry(TxnState::Ongoing, &[]),
            (ProducerId(7), 2),
            AddOffsetsDecision::Answer(codes::PRODUCER_FENCED),
        ),
        (
            "prepare commit",
            entry(TxnState::PrepareCommit, &[]),
            (ProducerId(7), 3),
            AddOffsetsDecision::Answer(codes::CONCURRENT_TRANSACTIONS),
        ),
        (
            "prepare abort",
            entry(TxnState::PrepareAbort, &[]),
            (ProducerId(7), 3),
            AddOffsetsDecision::Answer(codes::CONCURRENT_TRANSACTIONS),
        ),
        (
            "the ongoing transaction holds the partition",
            entry(TxnState::Ongoing, &[offsets_partition(4)]),
            (ProducerId(7), 3),
            AddOffsetsDecision::Answer(codes::NONE),
        ),
        (
            "the ongoing transaction lacks the partition",
            entry(TxnState::Ongoing, &[offsets_partition(5)]),
            (ProducerId(7), 3),
            AddOffsetsDecision::Append,
        ),
        (
            "an empty transaction",
            entry(TxnState::Empty, &[]),
            (ProducerId(7), 3),
            AddOffsetsDecision::Append,
        ),
        (
            "a completed transaction that held the partition",
            entry(TxnState::CompleteCommit, &[offsets_partition(4)]),
            (ProducerId(7), 3),
            AddOffsetsDecision::Append,
        ),
        (
            "a completed abort",
            entry(TxnState::CompleteAbort, &[]),
            (ProducerId(7), 3),
            AddOffsetsDecision::Append,
        ),
    ];
    for (name, entry, producer, expected) in cases {
        check!(
            decide(&entry, producer, &offsets_partition(4)) == expected,
            "{name}"
        );
    }
}

/// Kafka `TransactionMetadata.prepareAddPartitions` sets the start time only
/// when a transaction starts (#850).
#[test]
fn a_transaction_that_starts_gets_the_start_time() {
    let ongoing_since = |start_ms, partitions: &[TopicPartition]| {
        let mut entry = entry(TxnState::Ongoing, partitions);
        entry.start_ms = start_ms;
        entry.last_update_ms = NOW;
        entry
    };
    let cases = [
        (
            "an empty transaction starts",
            entry(TxnState::Empty, &[]),
            ongoing_since(NOW, &[offsets_partition(4)]),
        ),
        (
            "a completed transaction starts again with only the new partition",
            {
                let mut completed = entry(TxnState::CompleteCommit, &[offsets_partition(1)]);
                completed.start_ms = 5;
                completed
            },
            ongoing_since(NOW, &[offsets_partition(4)]),
        ),
        (
            "an ongoing transaction keeps its start time",
            ongoing_since(5, &[offsets_partition(1)]),
            ongoing_since(5, &[offsets_partition(1), offsets_partition(4)]),
        ),
    ];
    for (name, mut before, expected) in cases {
        add_partition(&mut before, offsets_partition(4), NOW);
        check!(before == expected, "{name}");
    }
}

#[test]
fn a_new_entry_has_no_start_time() {
    check!(TxnEntry::new_empty(TID.into(), ProducerId(7), 0, 60_000, NOW).start_ms == -1);
}

fn coordinator_with_log(
    directory: &std::path::Path,
) -> (Arc<TxnCoordinator>, Arc<crate::partition::Partition>) {
    let coordinator = Arc::new(TxnCoordinator::new(
        krabka_metadata::NodeId(1),
        Arc::new(crate::partition_registry::PartitionRegistry::new()),
        Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
        1,
        krabka_units::mebibytes(1),
    ));
    let partition = crate::test_support::open_partition(
        directory,
        crate::test_support::StandalonePartitionSetup {
            topic: bootstrap::TOPIC,
            ..Default::default()
        },
    );
    coordinator.partitions.insert(
        bootstrap::TOPIC.into(),
        PartitionIndex(0),
        Arc::clone(&partition),
    );
    (coordinator, partition)
}

/// The coordinator answers each case with Kafka's code, and appends only when
/// the partition set or the state changes (#889).
#[tokio::test]
async fn add_offsets_answers_kafka_codes_and_appends_only_a_change() {
    struct Case {
        name: &'static str,
        transactional_id: &'static str,
        seeded: TxnEntry,
        producer: (ProducerId, i16),
        code: i16,
        appends: bool,
    }
    let cases = [
        Case {
            name: "an empty transaction starts",
            transactional_id: TID,
            seeded: entry(TxnState::Empty, &[]),
            producer: (ProducerId(7), 3),
            code: codes::NONE,
            appends: true,
        },
        Case {
            name: "the partition is already in the ongoing transaction",
            transactional_id: TID,
            seeded: entry(TxnState::Ongoing, &[offsets_partition(4)]),
            producer: (ProducerId(7), 3),
            code: codes::NONE,
            appends: false,
        },
        Case {
            name: "prepare commit",
            transactional_id: TID,
            seeded: entry(TxnState::PrepareCommit, &[]),
            producer: (ProducerId(7), 3),
            code: codes::CONCURRENT_TRANSACTIONS,
            appends: false,
        },
        Case {
            name: "another producer id",
            transactional_id: TID,
            seeded: entry(TxnState::Ongoing, &[]),
            producer: (ProducerId(8), 3),
            code: codes::INVALID_PRODUCER_ID_MAPPING,
            appends: false,
        },
        Case {
            name: "another epoch",
            transactional_id: TID,
            seeded: entry(TxnState::Ongoing, &[]),
            producer: (ProducerId(7), 2),
            code: codes::PRODUCER_FENCED,
            appends: false,
        },
        Case {
            name: "an empty transactional id",
            transactional_id: "",
            seeded: entry(TxnState::Ongoing, &[]),
            producer: (ProducerId(7), 3),
            code: codes::INVALID_REQUEST,
            appends: false,
        },
    ];
    let mut expected = Vec::new();
    let mut actual = Vec::new();
    for case in cases {
        let directory = tempfile::tempdir().expect("tempdir");
        let (coordinator, partition) = coordinator_with_log(directory.path());
        coordinator
            .lead_state_partition_for_test(PartitionIndex(0))
            .await;
        coordinator
            .put(case.seeded.clone(), TxnVersion::Verified)
            .await
            .expect("seed the transaction");
        let before = partition.log_end_offset().0;

        let code = add_offsets_partition(
            &coordinator,
            case.transactional_id,
            case.producer,
            offsets_partition(4),
            TxnVersion::Verified,
        )
        .await;

        let stored = coordinator.get(TID).expect("entry").lock().await.clone();
        let after = if case.appends {
            Outcome::Appended {
                state: TxnState::Ongoing,
                started: true,
            }
        } else {
            Outcome::Unchanged(case.seeded.clone())
        };
        expected.push((case.name, case.code, after));
        let appended = partition.log_end_offset().0 > before;
        let observed = if appended {
            Outcome::Appended {
                state: stored.state,
                started: stored.start_ms >= 0,
            }
        } else {
            Outcome::Unchanged(stored)
        };
        actual.push((case.name, code, observed));
    }
    assert!(actual == expected);
}

/// Regression: a caller that already holds the entry handle from before this
/// append, such as `AddPartitionsToTxn`'s `register_partitions` queued on the
/// same mutex, must see the durable post-append state once it gets the lock,
/// not the pre-append snapshot. The append publishes a new handle in the
/// coordinator map, but an already-captured handle stays the old object
/// unless this function writes the staged value back into it too.
#[tokio::test]
async fn a_caller_already_holding_the_handle_sees_the_durable_state() {
    let directory = tempfile::tempdir().expect("tempdir");
    let (coordinator, _partition) = coordinator_with_log(directory.path());
    coordinator
        .lead_state_partition_for_test(PartitionIndex(0))
        .await;
    coordinator
        .put(entry(TxnState::Ongoing, &[]), TxnVersion::Verified)
        .await
        .expect("seed the transaction");

    // Capture the handle the way a concurrent register_partitions call would,
    // before this append runs.
    let pre_append_handle = coordinator.get(TID).expect("entry");

    let code = add_offsets_partition(
        &coordinator,
        TID,
        (ProducerId(7), 3),
        offsets_partition(4),
        TxnVersion::Verified,
    )
    .await;
    assert!(code == codes::NONE);

    let via_old_handle = pre_append_handle.lock().await.clone();
    assert!(via_old_handle.partitions.contains(&offsets_partition(4)));
    assert!(via_old_handle.state == TxnState::Ongoing);
}

/// What one `AddOffsetsToTxn` call left behind.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// A record was appended. The entry has this state, and a start time when
    /// `started` is true.
    Appended { state: TxnState, started: bool },
    /// Nothing was appended, and the entry is this one.
    Unchanged(TxnEntry),
}

/// A transaction that `AddOffsetsToTxn` opens after a long idle period is not
/// reapable at once (#850).
#[tokio::test]
async fn the_reaper_does_not_abort_a_transaction_that_add_offsets_just_opened() {
    let directory = tempfile::tempdir().expect("tempdir");
    let (coordinator, _partition) = coordinator_with_log(directory.path());
    coordinator
        .lead_state_partition_for_test(PartitionIndex(0))
        .await;
    // The previous transaction started and completed long ago.
    let mut completed = entry(TxnState::CompleteCommit, &[]);
    completed.start_ms = 0;
    completed.last_update_ms = 0;
    coordinator
        .put(completed.clone(), TxnVersion::Verified)
        .await
        .expect("seed the transaction");

    let code = add_offsets_partition(
        &coordinator,
        TID,
        (ProducerId(7), 3),
        offsets_partition(4),
        TxnVersion::Verified,
    )
    .await;
    check!(code == codes::NONE);

    let stored = coordinator.get(TID).expect("entry").lock().await.clone();
    let now = now_millis();
    check!(
        !crate::txn::two_pc::should_abort_idle_txn(
            stored.state,
            stored.txn_timeout_ms,
            stored.start_ms,
            now
        ),
        "start_ms={} now={now}",
        stored.start_ms
    );
}
