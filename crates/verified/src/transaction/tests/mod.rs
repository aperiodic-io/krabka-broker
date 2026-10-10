use super::*;

// Kafka `TransactionState` ids.
const ONGOING: i8 = 1;

const PREPARE_COMMIT: i8 = 2;

const PREPARE_ABORT: i8 = 3;

const COMPLETE_COMMIT: i8 = 4;

const COMPLETE_ABORT: i8 = 5;

use krabka_ids::ProducerId;

#[derive(Clone, Copy, Default)]
struct ProducerEpoch(i16);

#[derive(Clone, Copy, Default)]
struct CoordinatorEpoch(i32);

#[derive(Clone, Copy)]
struct TransactionStateCode(i8);

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum TransactionOutcome {
    #[default]
    Commit,
    Abort,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum MarkerTarget {
    #[default]
    Offsets,
    Data,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum PendingTransaction {
    #[default]
    Present,
    Absent,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct MarkerSetup {
    #[default(ProducerId(1))]
    producer_id: ProducerId,
    producer_epoch: ProducerEpoch,
    coordinator_epoch: CoordinatorEpoch,
    outcome: TransactionOutcome,
    target: MarkerTarget,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct MarkerPartitionSetup {
    producer_epoch: ProducerEpoch,
    coordinator_epoch: CoordinatorEpoch,
    pending: PendingTransaction,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct TransactionSnapshotSetup {
    #[default(ProducerId(7))]
    producer_id: ProducerId,
    #[default(ProducerEpoch(3))]
    producer_epoch: ProducerEpoch,
    #[default(TransactionStateCode(PREPARE_ABORT))]
    state: TransactionStateCode,
}

fn marker(setup: MarkerSetup) -> TransactionMarkerRequest {
    TransactionMarkerRequest {
        producer_id: setup.producer_id.0,
        producer_epoch: setup.producer_epoch.0,
        coordinator_epoch: setup.coordinator_epoch.0,
        is_commit: setup.outcome == TransactionOutcome::Commit,
        is_offsets_partition: setup.target == MarkerTarget::Offsets,
    }
}

fn partition(setup: MarkerPartitionSetup) -> TransactionMarkerPartitionState {
    TransactionMarkerPartitionState {
        producer_epoch: setup.producer_epoch.0,
        coordinator_epoch: setup.coordinator_epoch.0,
        has_pending_transaction: setup.pending == PendingTransaction::Present,
    }
}

fn snapshot(setup: TransactionSnapshotSetup) -> TransactionSnapshot {
    TransactionSnapshot {
        pid: setup.producer_id.0,
        epoch: setup.producer_epoch.0,
        state: setup.state.0,
    }
}

mod pid_install_rejects_malformed_misplaced_and_colliding_records;

mod reaper_completion_requires_the_exact_prepared_snapshot;

mod idle_transaction_reaper_matches_kafka_timed_out_transactions;
