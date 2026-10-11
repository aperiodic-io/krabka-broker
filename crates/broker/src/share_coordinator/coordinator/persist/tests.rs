//! The write rule of the share coordinator: a record goes to
//! `__share_group_state` only while the partition leads on this broker in the
//! term of the coordinator, and an answer waits until the high watermark
//! covers the records under the same term.

use std::{sync::Arc, time::Duration};

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use tempfile::tempdir;

use super::*;
use crate::share_coordinator::{
    config::ShareCoordinatorConfig,
    coordinator::test_support::{configured_coordinator, image_with_topic},
    persistence::KEY_SHARE_SNAPSHOT,
};

/// Kafka's `handleOperationException` codes for each append failure,
/// with the message of the error before the mapping.
#[test]
fn append_errors_answer_as_kafka() {
    let rows = [
        (
            AppendError::NotLocal(PartitionIndex(3)),
            codes::COORDINATOR_NOT_AVAILABLE,
            message::UNKNOWN_TOPIC_OR_PARTITION,
        ),
        (
            AppendError::NotLeader(PartitionIndex(3)),
            codes::NOT_COORDINATOR,
            message::NOT_COORDINATOR,
        ),
        (
            AppendError::TimedOut,
            codes::COORDINATOR_NOT_AVAILABLE,
            message::REQUEST_TIMED_OUT,
        ),
    ];
    for (error, code, message) in rows {
        check!(
            error.share_error() == ShareStateError::Operation { code, message },
            "{error}"
        );
    }
}

/// A coordinator with one state partition, open locally, and its term at
/// leader epoch 0. The write timeout is `timeout`.
fn one_partition(dir: &std::path::Path, timeout: Duration) -> (ShareCoordinator, Arc<Partition>) {
    let config = ShareCoordinatorConfig {
        state_topic_num_partitions: 1,
        write_timeout: timeout,
        ..ShareCoordinatorConfig::default()
    };
    let (coord, reg, _clock) = configured_coordinator(dir, config);
    let part = reg
        .get(bootstrap::TOPIC, PartitionIndex(0))
        .expect("the state partition is open");
    (coord, part)
}

const TERM: Term = Term {
    partition: PartitionIndex(0),
    leader_epoch: 0,
};

/// The outcome of a write, as a comparable value.
fn outcome<T>(result: Result<T, AppendError>) -> Result<(), &'static str> {
    result.map(drop).map_err(|error| match error {
        AppendError::NotLocal(_) => "not local",
        AppendError::NotLeader(_) => "not leader",
        AppendError::Failed(_) => "failed",
        AppendError::TimedOut => "timed out",
    })
}

/// The leader epoch of every batch of the state partition.
fn logged_epochs(part: &Partition) -> Vec<i32> {
    let read = part
        .read_log(Offset(0), krabka_units::mebibytes(1))
        .expect("read the state partition");
    read.batches
        .iter()
        .map(|batch| batch.partition_leader_epoch)
        .collect()
}

/// Kafka's `Partition.appendRecordsToLeader`: a broker appends only as the
/// leader of the partition, and stamps the leader epoch on the batch. A
/// coordinator whose term has ended writes nothing.
#[tokio::test]
async fn a_record_is_appended_only_as_the_leader_of_the_term() {
    struct Case {
        what: &'static str,
        /// The leader and the epoch that the partition leads at.
        leads: (u64, i32),
        /// The leader epoch of the term.
        term_epoch: i32,
        expected: Result<(), &'static str>,
        logged_epochs: Vec<i32>,
    }
    let cases = [
        Case {
            what: "this broker leads in the term",
            leads: (1, 4),
            term_epoch: 4,
            expected: Ok(()),
            logged_epochs: vec![4],
        },
        Case {
            what: "another broker leads",
            leads: (2, 5),
            term_epoch: 4,
            expected: Err("not leader"),
            logged_epochs: vec![],
        },
        Case {
            what: "this broker leads at a newer epoch",
            leads: (1, 5),
            term_epoch: 4,
            expected: Err("not leader"),
            logged_epochs: vec![],
        },
    ];
    for case in cases {
        let dir = tempdir().unwrap();
        let (coord, part) = one_partition(dir.path(), Duration::from_secs(30));
        part.install_leader_change(case.leads.0, case.leads.1).await;
        let term = Term {
            leader_epoch: case.term_epoch,
            ..TERM
        };
        let key = ShareStateKey {
            record_type: KEY_SHARE_SNAPSHOT,
            group_id: "g".into(),
            topic_id: uuid::Uuid::from_bytes([3; 16]),
            partition: 0,
        };

        let result = coord
            .persist_record(term, key, Some(Bytes::from_static(b"v")))
            .await;

        check!(outcome(result) == case.expected, "{}", case.what);
        check!(logged_epochs(&part) == case.logged_epochs, "{}", case.what);
    }
}

/// Kafka's `CoordinatorRuntime` completes an operation when the high
/// watermark passes the records of the shard, and fails it with
/// `NOT_COORDINATOR` when the broker loses the partition first, or with a
/// timeout. A former leader can see its high watermark pass the records over
/// the log of the next leader, so that is not a commit.
#[tokio::test]
async fn an_answer_waits_for_the_commit_under_its_term() {
    use crate::coordinator::test_support::{CommitWaitOutcome, commit_wait_cases};
    for case in commit_wait_cases() {
        let expected = match case.expected {
            CommitWaitOutcome::Committed => Ok(()),
            CommitWaitOutcome::NotLeader => Err("not leader"),
            CommitWaitOutcome::TimedOut => Err("timed out"),
        };
        let dir = tempdir().unwrap();
        let (coord, part) = one_partition(dir.path(), case.timeout);
        part.install_leader_change(1, 0).await;
        part.replica_state.lock().await.hw = Offset(case.hw_now);

        let changes = crate::coordinator::test_support::schedule_commit_changes!(
            case, part;
            captures {  }
            leader(leader, epoch) { part.install_leader_change(leader, epoch).await; }
            notify { part.hw_advance_notify.notify_waiters(); }
        );
        let result = coord.await_committed(TERM, Offset(2)).await;
        changes.await.expect("the changes land");

        assert!(outcome(result) == expected, "{}", case.what);
    }
}

/// A state partition whose log is not open here commits nothing.
#[tokio::test]
async fn an_answer_on_a_partition_that_is_not_open_is_not_committed() {
    let dir = tempdir().unwrap();
    let (coord, _part) = one_partition(dir.path(), Duration::from_secs(30));
    coord.partitions.remove(bootstrap::TOPIC, PartitionIndex(0));

    check!(outcome(coord.await_committed(TERM, Offset(0)).await) == Err("not leader"));
    check!(coord.last_written(TERM) == None);
}

/// A prune trims the log only once the snapshots it reads are committed: a
/// snapshot that the next leader can lose does not make the one before it
/// redundant.
#[tokio::test]
async fn a_prune_trims_only_committed_snapshots() {
    let rows = [(true, Offset(1)), (false, Offset(0))];
    for (committed, log_start) in rows {
        let dir = tempdir().unwrap();
        // Long enough that the first snapshot commits on a loaded runner; the
        // uncommitted row waits it out once.
        let (coord, part) = one_partition(dir.path(), Duration::from_secs(2));
        coord.lead_all_partitions_for_test().await;
        let image = image_with_topic(uuid::Uuid::from_bytes([3; 16]), 1);
        let topic_id = uuid::Uuid::from_bytes([3; 16]);
        coord
            .initialize(&image, "g", topic_id, 0, 0, Offset(0))
            .await
            .expect("the first snapshot commits");
        if !committed {
            // A follower that does not fetch holds the high watermark at the
            // first snapshot.
            let both = [krabka_raft::NodeId(1), krabka_raft::NodeId(2)];
            part.replica_state.lock().await.install_isr(
                &both,
                &both,
                krabka_raft::NodeId(1),
                std::time::Instant::now(),
            );
        }
        let second = coord
            .initialize(&image, "g", topic_id, 0, 1, Offset(5))
            .await;
        check!(second.is_ok() == committed, "committed {committed}");

        coord.maybe_prune(PartitionIndex(0)).await;

        check!(
            part.log_start_offset() == log_start,
            "committed {committed}"
        );
    }
}
