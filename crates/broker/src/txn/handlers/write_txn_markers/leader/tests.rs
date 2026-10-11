//! The leader rule of a transaction marker: only the installed leader of the
//! partition appends it, and only once the ISR is large enough, and the
//! marker counts only once the high watermark covers it under the leader and
//! leader epoch that took it.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use assert2::assert;
use bytes::Bytes;
use krabka_log::{Offset, ProducerId};
use krabka_protocol::records::{Attributes, Record, RecordBatch};
use krabka_raft::NodeId;

use super::{PendingMarker, append_marker_as_leader};
use crate::{
    codes,
    error::BrokerError,
    partition::{Partition, test_support::test_partition_with_writer},
    replica_state::LeaderPolicy,
    txn::{handlers::write_txn_markers::MarkerAppend, marker::MarkerType},
};

/// This broker.
const NODE: NodeId = NodeId(1);
/// The follower that holds the high watermark back until a case moves it.
const FOLLOWER: NodeId = NodeId(2);
/// The leader epoch that this broker leads the partition at.
const EPOCH: i32 = 7;
/// The data batch below takes offsets 0 and 1, so a marker lands at 2.
const MARKER_END: i64 = 3;

const MARKER: MarkerAppend = MarkerAppend {
    producer_id: ProducerId(40),
    producer_epoch: 3,
    marker_type: MarkerType::Commit,
    coordinator_epoch: 5,
    commit_stamp: None,
    transaction_version: 0,
};

/// A partition with a live writer that this broker leads at [`EPOCH`], whose
/// ISR holds a follower that has not fetched, so the high watermark stays at
/// zero. It holds a two-record transaction of the producer of [`MARKER`].
async fn led_partition() -> (Arc<Partition>, tempfile::TempDir) {
    let (partition, dir) = test_partition_with_writer();
    let partition = Arc::new(partition);
    partition.install_leader_change(NODE.0, EPOCH).await;
    partition
        .install_isr(&[NODE, FOLLOWER], &[NODE, FOLLOWER], NODE)
        .await;
    partition
        .produce_batch(RecordBatch {
            producer_id: MARKER.producer_id.get(),
            producer_epoch: MARKER.producer_epoch,
            base_sequence: 0,
            attributes: Attributes::default().with_transactional(true),
            last_offset_delta: 1,
            records: (0..2)
                .map(|offset_delta| Record {
                    offset_delta,
                    value: Some(Bytes::from_static(b"event")),
                    ..Record::default()
                })
                .collect(),
            ..RecordBatch::default()
        })
        .await
        .expect("append the transaction");
    (partition, dir)
}

/// The Kafka code of a marker write: `NONE`, or the code it was refused with.
fn code(result: &Result<(), BrokerError>) -> i16 {
    match result {
        Ok(()) => codes::NONE,
        Err(BrokerError::MarkerWriteRefused { code, .. }) => *code,
        Err(other) => panic!("not a marker refusal: {other}"),
    }
}

/// The leader epoch on every control batch of `partition`.
fn marker_epochs(partition: &Partition) -> Vec<i32> {
    partition
        .log
        .lock()
        .expect("partition log")
        .read(Offset(0), krabka_units::mebibytes(1))
        .expect("read the log")
        .batches
        .iter()
        .filter(|batch| batch.attributes.is_control_batch())
        .map(|batch| batch.partition_leader_epoch)
        .collect()
}

/// Make the high watermark `hw`, as follower fetches would, and wake the
/// waits.
async fn set_hw(partition: &Partition, hw: i64) {
    partition.replica_state.lock().await.hw = Offset(hw);
    partition.hw_advance_notify.notify_waiters();
}

/// Kafka's `Partition.appendRecordsToLeader` with `requiredAcks=-1`: a
/// follower refuses the marker with `NOT_LEADER_OR_FOLLOWER`, a leader whose
/// ISR is below `min.insync.replicas` refuses it with `NOT_ENOUGH_REPLICAS`,
/// and the leader appends it with its own leader epoch.
#[tokio::test]
async fn only_the_leader_with_enough_replicas_appends_a_marker() {
    enum Setup {
        Leads,
        LedElsewhere,
        UnderMinIsr,
    }
    // (case, setup, admitted, leader epochs of the markers in the log)
    let cases = [
        ("led here", Setup::Leads, codes::NONE, vec![EPOCH]),
        (
            "led by another broker",
            Setup::LedElsewhere,
            codes::NOT_LEADER_OR_FOLLOWER,
            vec![],
        ),
        (
            "ISR below min.insync.replicas",
            Setup::UnderMinIsr,
            codes::NOT_ENOUGH_REPLICAS,
            vec![],
        ),
    ];
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (case, setup, admitted, epochs) in cases {
        let (partition, _dir) = led_partition().await;
        match setup {
            Setup::Leads => {}
            Setup::LedElsewhere => partition.install_leader_change(FOLLOWER.0, EPOCH + 1).await,
            Setup::UnderMinIsr => {
                partition
                    .install_isr(&[NODE], &[NODE, FOLLOWER], NODE)
                    .await;
                partition
                    .replica_state
                    .lock()
                    .await
                    .set_policy(LeaderPolicy {
                        effective_min_isr: 2,
                        replica_lag_time_max: Duration::from_secs(30),
                        brokers: std::collections::HashMap::new(),
                    });
            }
        }
        let appended = append_marker_as_leader(&partition, NODE, None, "t", MARKER)
            .await
            .map(drop);
        actual.push((case, code(&appended), marker_epochs(&partition)));
        expected.push((case, admitted, epochs));
    }
    assert!(actual == expected);
}

/// Kafka's `DelayedProduce` of a marker: it completes once the high
/// watermark covers the marker, fails with `NOT_LEADER_OR_FOLLOWER` when the
/// partition takes another leader or leader epoch first, fails with
/// `REQUEST_TIMED_OUT` at the deadline, and answers
/// `NOT_ENOUGH_REPLICAS_AFTER_APPEND` when the ISR shrank below
/// `min.insync.replicas` by then.
#[tokio::test]
async fn a_marker_counts_only_once_committed_under_its_leader() {
    enum Change {
        FollowersCatchUp,
        LeaderMoves,
        NewEpochHere,
        CaughtUpAfterTheMove,
        IsrShrinksBelowMin,
        Nothing,
    }
    let long = Duration::from_secs(30);
    let cases = [
        (
            "the follower catches up",
            Change::FollowersCatchUp,
            long,
            codes::NONE,
        ),
        (
            "another broker takes the partition",
            Change::LeaderMoves,
            long,
            codes::NOT_LEADER_OR_FOLLOWER,
        ),
        (
            "this broker leads again at a newer epoch",
            Change::NewEpochHere,
            long,
            codes::NOT_LEADER_OR_FOLLOWER,
        ),
        (
            "the high watermark passes the marker after the move",
            Change::CaughtUpAfterTheMove,
            long,
            codes::NOT_LEADER_OR_FOLLOWER,
        ),
        (
            "the ISR shrinks below min.insync.replicas",
            Change::IsrShrinksBelowMin,
            long,
            codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        ),
        (
            "the follower never catches up",
            Change::Nothing,
            Duration::from_millis(100),
            codes::REQUEST_TIMED_OUT,
        ),
    ];
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (case, change, timeout, answer) in cases {
        let (partition, _dir) = led_partition().await;
        let pending = append_marker_as_leader(&partition, NODE, None, "t", MARKER)
            .await
            .expect("the leader appends the marker");
        let changes = {
            let partition = Arc::clone(&partition);
            tokio::spawn(async move {
                // intentional: the wait has to start before the change lands.
                tokio::time::sleep(Duration::from_millis(20)).await;
                match change {
                    Change::FollowersCatchUp => set_hw(&partition, MARKER_END).await,
                    Change::LeaderMoves => {
                        partition.install_leader_change(FOLLOWER.0, EPOCH + 1).await;
                    }
                    Change::NewEpochHere => {
                        partition.install_leader_change(NODE.0, EPOCH + 1).await;
                    }
                    Change::CaughtUpAfterTheMove => {
                        partition.install_leader_change(FOLLOWER.0, EPOCH + 1).await;
                        set_hw(&partition, MARKER_END).await;
                    }
                    Change::IsrShrinksBelowMin => {
                        {
                            let mut state = partition.replica_state.lock().await;
                            state.install_isr(&[NODE], &[NODE, FOLLOWER], NODE, Instant::now());
                            state.set_policy(LeaderPolicy {
                                effective_min_isr: 2,
                                replica_lag_time_max: Duration::from_secs(30),
                                brokers: std::collections::HashMap::new(),
                            });
                        }
                        set_hw(&partition, MARKER_END).await;
                    }
                    Change::Nothing => {}
                }
            })
        };
        let committed = pending.committed(Instant::now() + timeout).await;
        changes.await.expect("the change lands");
        actual.push((case, code(&committed)));
        expected.push((case, answer));
    }
    assert!(actual == expected);
}

/// The coordinator retries a marker that did not commit. The retry finds the
/// marker in the log, so it appends nothing, but it answers `NONE` only once
/// the high watermark covers the marker it found.
#[tokio::test]
async fn a_retried_marker_waits_for_the_marker_the_log_holds() {
    let (partition, _dir) = led_partition().await;
    let first: PendingMarker = append_marker_as_leader(&partition, NODE, None, "t", MARKER)
        .await
        .expect("the leader appends the marker");
    let short = || Instant::now() + Duration::from_millis(50);
    let timed_out = first.committed(short()).await;

    let retry = append_marker_as_leader(&partition, NODE, None, "t", MARKER)
        .await
        .expect("the leader takes the retry");
    let retry_timed_out = retry.committed(short()).await;
    let retry = append_marker_as_leader(&partition, NODE, None, "t", MARKER)
        .await
        .expect("the leader takes the second retry");
    set_hw(&partition, MARKER_END).await;
    let retry_committed = retry.committed(short()).await;

    assert!(
        (
            code(&timed_out),
            code(&retry_timed_out),
            code(&retry_committed),
            marker_epochs(&partition),
            partition.log_end_offset(),
        ) == (
            codes::REQUEST_TIMED_OUT,
            codes::REQUEST_TIMED_OUT,
            codes::NONE,
            vec![EPOCH],
            Offset(MARKER_END),
        )
    );
}
