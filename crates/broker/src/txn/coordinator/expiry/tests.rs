//! Tests for KIP-98 transactional-id expiry: the pure decision core over
//! every `TxnState`, and the sweep driven against a live coordinator whose
//! `__transaction_state-0` partition is a real log on disk, driven both
//! directly and through one tick of the background task that ticks it.

use std::sync::Arc;

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_log::{Offset, ProducerId};
use krabka_metadata::{MetadataImage, NodeId};
use krabka_protocol::{
    UnknownTaggedFields, owned::describe_transactions_response::TransactionState,
};
use krabka_units::mebibytes;
use tempfile::{TempDir, tempdir};

use super::*;
use crate::{
    codes,
    handlers::describe_transactions::transaction_state_row,
    partition_registry::PartitionRegistry,
    test_support::FakeMetadataSource,
    txn::{bootstrap, state::TxnEntry, two_pc::NO_TIMEOUT_MS, version::TxnVersion},
};

const TID: &str = "tid-expiry";
/// Kafka's `transactional.id.expiration.ms` default.
const EXPIRY_MS: i64 = 604_800_000;

// ── The pure decision core ────────────────────────────────────────────

/// Every `TxnState`, with the elapsed time exactly at the expiry, so the
/// state is the only thing that can decide the answer.
///
/// `Empty`, `CompleteCommit` and `CompleteAbort` are the states
/// `TransactionState.isExpirationAllowed()` reports `true` for in the pinned
/// `apache/kafka:4.3.1` image. `Dead` is krabka's one addition, for the
/// reason [`state_allows_expiration`] gives.
#[test]
fn only_terminal_and_idle_states_expire() {
    for (state, want) in [
        (TxnState::Empty, true),
        (TxnState::Dead, true),
        (TxnState::CompleteCommit, true),
        (TxnState::CompleteAbort, true),
        (TxnState::Ongoing, false),
        (TxnState::PrepareCommit, false),
        (TxnState::PrepareAbort, false),
    ] {
        check!(
            should_expire_transactional_id(state, 0, EXPIRY_MS, EXPIRY_MS) == want,
            "{state:?}"
        );
    }
}

/// KIP-939: no clock, however far in the future, expires a prepared 2PC
/// transaction. `i64::MAX` is the strongest statement of "indefinitely" the
/// type allows.
#[test]
fn a_prepared_transaction_never_expires_at_any_clock() {
    for state in [TxnState::PrepareCommit, TxnState::PrepareAbort] {
        for now_ms in [EXPIRY_MS, EXPIRY_MS * 1_000, i64::MAX] {
            check!(
                !should_expire_transactional_id(state, 0, now_ms, EXPIRY_MS),
                "{state:?} at {now_ms}"
            );
        }
    }
}

/// The boundary Kafka uses is `now - last >= expiration`, so the instant the
/// expiry elapses is already expired and the millisecond before it is not.
#[test]
fn expiry_boundary_is_inclusive() {
    for (now_ms, want) in [
        (EXPIRY_MS - 1, false),
        (EXPIRY_MS, true),
        (EXPIRY_MS + 1, true),
    ] {
        check!(
            should_expire_transactional_id(TxnState::CompleteCommit, 0, now_ms, EXPIRY_MS) == want,
            "{now_ms}"
        );
    }
}

/// A backwards clock must never expire anything, and must not overflow on the
/// way to that answer.
#[test]
fn a_backwards_clock_expires_nothing() {
    check!(!should_expire_transactional_id(
        TxnState::CompleteCommit,
        EXPIRY_MS * 2,
        0,
        EXPIRY_MS
    ));
    check!(!should_expire_transactional_id(
        TxnState::CompleteCommit,
        i64::MAX,
        i64::MIN,
        EXPIRY_MS
    ));
}

// ── The live sweep ────────────────────────────────────────────────────

/// A `TxnEntry` that started at time 0, committed at `last_update_ms` and has
/// sat in `CompleteCommit` ever since.
fn complete_commit_entry(last_update_ms: i64) -> TxnEntry {
    let mut entry = TxnEntry::new_empty(TID.to_owned(), ProducerId(1000), 3, 60_000, 0);
    entry.state = TxnState::CompleteCommit;
    entry.start_ms = 0;
    entry.last_update_ms = last_update_ms;
    entry
}

/// A KIP-939 2PC transaction that started at time 0, that an external
/// transaction manager prepared at `last_update_ms` and has not yet resolved.
/// The [`NO_TIMEOUT_MS`] sentinel is what marks it 2PC on disk.
fn prepared_two_pc_entry(last_update_ms: i64) -> TxnEntry {
    let mut entry = TxnEntry::new_empty(TID.to_owned(), ProducerId(2000), 1, NO_TIMEOUT_MS, 0);
    entry.state = TxnState::PrepareCommit;
    entry.start_ms = 0;
    entry.last_update_ms = last_update_ms;
    entry
}

/// A metadata image where `__transaction_state` has one partition, led by
/// `leader`.
fn image_with_leader(leader: NodeId, leader_epoch: krabka_metadata::LeaderEpoch) -> MetadataImage {
    crate::txn::coordinator::test_support::state_image(
        crate::txn::coordinator::test_support::StateImageSetup {
            leader,
            leader_epoch,
            replicas: &[leader],
            ..Default::default()
        },
    )
}

/// A coordinator that persisted `entry` as the leader of the single
/// `__transaction_state` partition. When `leader` is another broker, that
/// broker is elected afterwards at leader epoch 1. The tempdir must outlive the
/// coordinator, so it comes back with it.
async fn seeded_coordinator(entry: TxnEntry, leader: NodeId) -> (Arc<TxnCoordinator>, TempDir) {
    let dir = tempdir().expect("tempdir");
    let partitions = Arc::new(PartitionRegistry::new());
    partitions.insert(
        bootstrap::TOPIC.into(),
        PartitionIndex(0),
        crate::test_support::open_partition(
            dir.path(),
            crate::test_support::StandalonePartitionSetup {
                topic: bootstrap::TOPIC,
                ..Default::default()
            },
        ),
    );
    let coordinator = Arc::new(TxnCoordinator::new(
        NodeId(1),
        partitions,
        Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
        1,
        mebibytes(1),
    ));
    coordinator
        .refresh_leader_partitions(&image_with_leader(
            NodeId(1),
            krabka_metadata::LeaderEpoch(0),
        ))
        .await
        .finished()
        .await;
    coordinator
        .put(entry, TxnVersion::Verified)
        .await
        .expect("seed __transaction_state");
    if leader != NodeId(1) {
        drop(
            coordinator
                .refresh_leader_partitions(&image_with_leader(
                    leader,
                    krabka_metadata::LeaderEpoch(1),
                ))
                .await,
        );
    }
    (coordinator, dir)
}

/// The `(key, value)` pairs of every record in `__transaction_state-0`, with
/// each key decoded to its transactional id.
fn transaction_state_records(coordinator: &TxnCoordinator) -> Vec<(String, Option<Vec<u8>>)> {
    let part = coordinator
        .partitions
        .get(bootstrap::TOPIC, PartitionIndex(0))
        .expect("partition is local");
    let mut out = Vec::new();
    let mut offset = part.log_start_offset();
    loop {
        let read = part.read_log(offset, mebibytes(1)).expect("read log");
        if read.batches.is_empty() {
            return out;
        }
        for batch in &read.batches {
            for record in &batch.records {
                let key = crate::txn::log_record::decode_key(
                    record.key.as_ref().expect("record carries a key"),
                )
                .expect("record carries a TransactionLogKey");
                out.push((key, record.value.as_ref().map(|v| v.to_vec())));
            }
            offset = Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
        }
    }
}

/// The row `DescribeTransactions` answers for `TID` right now, built from the
/// coordinator's live entry exactly as the handler builds it.
async fn describe_transactions_row(coordinator: &TxnCoordinator) -> TransactionState {
    match coordinator.get(TID) {
        None => transaction_state_row(TID, None),
        Some(handle) => transaction_state_row(TID, Some(&*handle.lock().await)),
    }
}

#[tokio::test]
async fn an_expired_completed_transaction_is_tombstoned_and_dropped() {
    let (coordinator, _dir) = seeded_coordinator(complete_commit_entry(0), NodeId(1)).await;
    let before = transaction_state_records(&coordinator);
    assert!(before.len() == 1, "{before:?}");
    // Before the sweep, `DescribeTransactions` answers with the full entry.
    check!(
        describe_transactions_row(&coordinator).await
            == TransactionState {
                error_code: crate::codes::NONE,
                transactional_id: TID.to_owned(),
                transaction_state: "CompleteCommit".to_owned(),
                transaction_timeout_ms: 60_000,
                transaction_start_time_ms: 0,
                producer_id: 1000,
                producer_epoch: 3,
                topics: Vec::new(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );

    let expired = coordinator
        .expire_transactional_ids(EXPIRY_MS + 1, EXPIRY_MS)
        .await;

    check!(expired == vec![TID.to_string()]);
    // `DescribeTransactions` no longer returns the id: the coordinator holds
    // no entry, so the handler answers TRANSACTIONAL_ID_NOT_FOUND.
    check!(
        describe_transactions_row(&coordinator).await
            == TransactionState {
                error_code: codes::TRANSACTIONAL_ID_NOT_FOUND,
                transactional_id: TID.to_owned(),
                ..Default::default()
            }
    );
    check!(coordinator.snapshot().await.is_empty());
    check!(coordinator.tid_for_pid(ProducerId(1000)).is_none());
    // The seeded value record, then the tombstone: a null value under the same
    // byte-exact `TransactionLogKey`.
    let after = transaction_state_records(&coordinator);
    check!(after.len() == 2, "{after:?}");
    check!(after[1] == (TID.to_string(), None));
}

#[tokio::test]
async fn a_transaction_inside_the_expiry_window_survives() {
    let (coordinator, _dir) = seeded_coordinator(complete_commit_entry(0), NodeId(1)).await;

    let expired = coordinator
        .expire_transactional_ids(EXPIRY_MS - 1, EXPIRY_MS)
        .await;

    check!(expired.is_empty());
    check!(coordinator.get(TID).is_some());
    check!(transaction_state_records(&coordinator).len() == 1);
}

/// KIP-939: a prepared 2PC transaction is the external transaction manager's
/// to resolve. The sweep leaves it alone however long it waits.
#[tokio::test]
async fn a_prepared_two_pc_transaction_survives_indefinitely() {
    let (coordinator, _dir) = seeded_coordinator(prepared_two_pc_entry(0), NodeId(1)).await;

    for now_ms in [EXPIRY_MS + 1, EXPIRY_MS * 1_000, i64::MAX] {
        let expired = coordinator
            .expire_transactional_ids(now_ms, EXPIRY_MS)
            .await;
        check!(expired.is_empty(), "{now_ms}");
        // `DescribeTransactions` still reports the prepared transaction, with
        // the 2PC `NO_TIMEOUT_MS` sentinel it was prepared under.
        check!(
            describe_transactions_row(&coordinator).await
                == TransactionState {
                    error_code: crate::codes::NONE,
                    transactional_id: TID.to_owned(),
                    transaction_state: "PrepareCommit".to_owned(),
                    transaction_timeout_ms: NO_TIMEOUT_MS,
                    transaction_start_time_ms: 0,
                    producer_id: 2000,
                    producer_epoch: 1,
                    topics: Vec::new(),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            "{now_ms}"
        );
    }
    // No tombstone was ever appended: only the seeded value record is there.
    let records = transaction_state_records(&coordinator);
    check!(records.len() == 1, "{records:?}");
    check!(records[0].1.is_some());
}

/// The sweep takes its decision against the *live* entry under the tid's own
/// lock, and holds that lock across the append. A producer that opens a
/// transaction while the sweep is parked on the lock therefore keeps its
/// transaction: the sweep wakes, re-reads `Ongoing`, and refuses.
///
/// The lock is held before the sweep is spawned, so the sweep is parked
/// whatever the scheduler does, and the outcome is the same every run.
#[tokio::test]
async fn a_transaction_that_begins_while_the_sweep_waits_is_not_expired() {
    let (coordinator, _dir) = seeded_coordinator(complete_commit_entry(0), NodeId(1)).await;
    let coordinator = Arc::new(coordinator);
    let handle = coordinator.get(TID).expect("the seeded entry");
    let mut entry = handle.lock().await;

    let sweep = {
        let coordinator = Arc::clone(&coordinator);
        tokio::spawn(async move {
            coordinator
                .expire_transactional_ids(EXPIRY_MS + 1, EXPIRY_MS)
                .await
        })
    };
    // The producer begins a transaction while the sweep waits on the lock.
    entry.state = TxnState::Ongoing;
    entry.last_update_ms = EXPIRY_MS + 1;
    drop(entry);

    let expired = sweep.await.expect("sweep task");

    check!(expired.is_empty());
    check!(coordinator.get(TID).is_some());
    // No tombstone was appended: only the seeded value record is there.
    let records = transaction_state_records(&coordinator);
    check!(records.len() == 1, "{records:?}");
}

/// Replaying a tombstone reclaims the producer-id reverse index too.
///
/// Compaction has not run yet on a broker that restarts right after a sweep,
/// so recovery reads the expired id's value record and then its tombstone. A
/// replay that dropped the state entry alone would rebuild `pid_to_tid` for
/// every transactional id ever expired, and the reverse index -- with the
/// broker-start footprint it is part of -- would keep growing with the
/// historical id count, which is the leak the sweep exists to close.
#[tokio::test]
async fn replaying_a_tombstone_reclaims_the_producer_id_index() {
    let (coordinator, _dir) = seeded_coordinator(complete_commit_entry(0), NodeId(1)).await;
    let expired = coordinator
        .expire_transactional_ids(EXPIRY_MS + 1, EXPIRY_MS)
        .await;
    assert!(expired == vec![TID.to_string()]);

    // The broker is elected again and replays the log: the value record, then
    // the tombstone that follows it.
    coordinator
        .recover(&image_with_leader(
            NodeId(1),
            krabka_metadata::LeaderEpoch(1),
        ))
        .await
        .expect("replay __transaction_state");

    check!(coordinator.get(TID).is_none());
    check!(coordinator.snapshot().await.is_empty());
    check!(coordinator.tid_for_pid(ProducerId(1000)).is_none());
}

/// A pid the coordinator has since handed to another transactional id belongs
/// to that id, so replaying the first id's tombstone must leave it alone.
#[tokio::test]
async fn replaying_a_tombstone_keeps_a_pid_that_now_names_another_id() {
    let (coordinator, _dir) = seeded_coordinator(complete_commit_entry(0), NodeId(1)).await;
    coordinator
        .expire_transactional_ids(EXPIRY_MS + 1, EXPIRY_MS)
        .await;
    // Producer id 1000 is reissued to a different transactional id before the
    // replay reaches the first id's tombstone.
    let mut reissued = TxnEntry::new_empty("tid-other".to_owned(), ProducerId(1000), 0, 60_000, 0);
    reissued.last_update_ms = 0;
    coordinator
        .put(reissued, TxnVersion::Verified)
        .await
        .expect("persist the reissued id");

    coordinator
        .recover(&image_with_leader(
            NodeId(1),
            krabka_metadata::LeaderEpoch(1),
        ))
        .await
        .expect("replay __transaction_state");

    check!(coordinator.tid_for_pid(ProducerId(1000)) == Some("tid-other".to_owned()));
}

/// A `__transaction_state` partition that moved to another broker belongs to
/// that broker's sweep, not this one's. The resignation unloaded it, as Kafka's
/// `TransactionCoordinator.onResignation` does.
#[tokio::test]
async fn a_partition_this_broker_no_longer_leads_is_skipped() {
    let (coordinator, _dir) = seeded_coordinator(complete_commit_entry(0), NodeId(2)).await;

    let expired = coordinator
        .expire_transactional_ids(EXPIRY_MS + 1, EXPIRY_MS)
        .await;

    check!(expired.is_empty());
    check!(coordinator.get(TID).is_none());
    check!(coordinator.tid_for_pid(ProducerId(1000)).is_none());
    check!(transaction_state_records(&coordinator).len() == 1);
}

// ── One tick of the background reaper ─────────────────────────────────

/// One tick of [`crate::txn::id_expiration`], the background task the broker
/// spawns, against the wall clock the task itself reads.
///
/// The coordinator starts believing another broker leads
/// `__transaction_state-0`, so it expires nothing. The tick refreshes that
/// view from the live metadata image *before* it decides, which is what lets
/// the same entry expire on the very next call: a tick that skipped the
/// refresh would leave a transactional id unreclaimed on this broker until it
/// restarted.
#[tokio::test]
async fn a_sweep_tick_refreshes_leadership_before_expiring() {
    let (coordinator, _dir) = seeded_coordinator(complete_commit_entry(0), NodeId(2)).await;
    // Nothing expires while the stale view says this broker does not lead it,
    // however far the clock is pushed.
    check!(
        coordinator
            .expire_transactional_ids(i64::MAX, EXPIRY_MS)
            .await
            .is_empty()
    );

    // Leadership moved here at a new leader epoch. The tick loads the
    // partition, and at a 1ms expiry the entry's `last_update_ms` is long past
    // under any wall clock.
    let source = FakeMetadataSource::builder()
        .image(image_with_leader(
            NodeId(1),
            krabka_metadata::LeaderEpoch(2),
        ))
        .build();
    crate::txn::id_expiration::sweep_once(
        &coordinator,
        &source,
        <krabka_units::Time as krabka_units::convert::TimeExt>::from_millis(1),
    )
    .await;

    check!(coordinator.get(TID).is_none());
    check!(
        describe_transactions_row(&coordinator).await
            == TransactionState {
                error_code: codes::TRANSACTIONAL_ID_NOT_FOUND,
                transactional_id: TID.to_owned(),
                ..Default::default()
            }
    );
    let records = transaction_state_records(&coordinator);
    check!(records.len() == 2, "{records:?}");
    check!(records[1] == (TID.to_string(), None));
    // The tick reads the live image once, to refresh leadership, and writes
    // no metadata of its own: the tombstone goes to `__transaction_state`.
    check!(source.current_image_calls() == 1);
    check!(source.submitted().is_empty());
}
