//! The commit rule of a `__transaction_state` write: a transition reaches the
//! coordinator's map, and its answer reaches the client, only once the high
//! watermark covers its record in the term that wrote it.

use std::{sync::Arc, time::Duration};

use assert2::assert;
use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig, Offset, ProducerId};
use krabka_metadata::{
    LeaderEpoch, MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicConfigRecord,
    TopicRecord,
};

use super::coordinator_append_error;
use crate::{
    codes,
    error::BrokerError,
    metadata_source::MetadataSource as _,
    partition::Partition,
    partition_registry::PartitionRegistry,
    test_support::FakeMetadataSource,
    txn::{bootstrap, coordinator::TxnCoordinator, state::TxnEntry, version::TxnVersion},
};

/// This broker.
const NODE: NodeId = NodeId(1);
/// The one `__transaction_state` partition.
const P0: PartitionIndex = PartitionIndex(0);
/// The leader epoch this broker leads the partition at.
const EPOCH: i32 = 3;
const TID: &str = "tid";

/// Kafka's `appendTransactionToLog` turns the error of its append into the
/// coordinator error the client gets, and passes any other error through.
#[test]
fn append_errors_map_as_kafka_append_transaction_to_log() {
    let cases = [
        (
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            codes::COORDINATOR_NOT_AVAILABLE,
        ),
        (
            codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
            codes::COORDINATOR_NOT_AVAILABLE,
        ),
        (codes::CORRUPT_MESSAGE, codes::CORRUPT_MESSAGE),
    ];
    for (append, expected) in cases
        .into_iter()
        .chain(crate::test_support::COORDINATOR_WRITE_ERROR_CASES)
    {
        assert!(coordinator_append_error(append) == expected, "{append}");
    }
}

/// `__transaction_state` with one partition that `leader` leads at `epoch`
/// over replicas 1 and 2 with `isr`, and a `min.insync.replicas` of 2, as
/// `transaction.state.log.min.isr` sets it.
fn state_image(leader: u64, epoch: i32, isr: &[u64]) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: bootstrap::TOPIC.into(),
        topic_id: uuid::Uuid::from_u128(1),
        partitions: 1,
        replication_factor: 2,
    }));
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: bootstrap::TOPIC.into(),
        overrides: [(
            crate::config_keys::MIN_INSYNC_REPLICAS.to_string(),
            "2".to_string(),
        )]
        .into_iter()
        .collect(),
    }));
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: bootstrap::TOPIC.into(),
        partition: 0,
        leader: NodeId(leader),
        leader_epoch: LeaderEpoch(epoch),
        replicas: vec![NodeId(1), NodeId(2)],
        isr: isr.iter().copied().map(NodeId).collect(),
        ..PartitionRecord::default()
    }));
    image
}

/// A coordinator that leads the state partition at [`EPOCH`], whose log
/// sits on a real partition with a follower that never fetches, so the high
/// watermark stays where the test puts it. With `with_metadata`, the
/// coordinator also checks its writes against `metadata`.
async fn coordinator(
    dir: &std::path::Path,
    metadata: &Arc<FakeMetadataSource>,
    with_metadata: bool,
) -> (Arc<TxnCoordinator>, Arc<Partition>) {
    let part_dir = crate::log_dir::partition_dir(dir, bootstrap::TOPIC, 0);
    std::fs::create_dir_all(&part_dir).expect("create the partition dir");
    let partition = crate::broker::spawn_partition(
        bootstrap::TOPIC.to_owned(),
        P0,
        dir.to_path_buf(),
        Log::open(&part_dir, LogConfig::default()).expect("open the log"),
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    );
    partition
        .install_isr(&[NodeId(1), NodeId(2)], &[NodeId(1), NodeId(2)], NODE)
        .await;
    let partitions = Arc::new(PartitionRegistry::new());
    partitions.insert(bootstrap::TOPIC.into(), P0, Arc::clone(&partition));
    let mut coordinator = TxnCoordinator::new(
        NODE,
        partitions,
        Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
        1,
        krabka_units::mebibytes(1),
    );
    if with_metadata {
        let metadata: Arc<dyn crate::metadata_source::MetadataSource> = metadata.clone();
        coordinator.set_metadata_source_for_test(metadata);
    }
    let coordinator = Arc::new(coordinator);
    coordinator
        .refresh_leader_partitions(&metadata.current_image())
        .await
        .finished()
        .await;
    (coordinator, partition)
}

/// What happens once the record is in the leader's log.
#[derive(Debug, Clone, Copy)]
enum Change {
    Nothing,
    /// The follower fetches the record, and the high watermark covers it.
    FollowersCatchUp,
    /// The image names this leader, epoch and ISR, and the coordinator has
    /// not applied it yet.
    ImageMovesTo(u64, i32, &'static [u64]),
    /// The image moves the ISR, then the follower fetches the record.
    IsrShrinksThenFollowersCatchUp,
    /// The coordinator applies an image in which this leader and epoch lead.
    TermMovesTo(u64, i32),
}

async fn wait_for_log_end(partition: &Partition, end: Offset) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while partition.log_end_offset() < end {
        assert!(
            std::time::Instant::now() < deadline,
            "the record reaches the log"
        );
        // intentional: the log has no awaiter for its end offset.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn catch_up(partition: &Partition) {
    partition.replica_state.lock().await.hw = partition.log_end_offset();
    partition.hw_advance_notify.notify_waiters();
}

/// The leader epoch of every batch in the log of `partition`.
fn logged_epochs(partition: &Partition) -> Vec<i32> {
    let log = partition.log.lock().expect("the log is not poisoned");
    let end = log.log_end_offset();
    if end == Offset(0) {
        return Vec::new();
    }
    crate::coordinator::test_support::read_batch_epochs(&log, end)
}

/// Kafka's `appendTransactionToLog` appends with `acks=-1` and changes the
/// cache only once the append is complete. A transition that was answered at
/// the local append could be lost on failover after its client moved on: an
/// `EndTxn` whose `PrepareCommit` the next coordinator never sees, or an epoch
/// bump that fences nobody.
#[tokio::test]
async fn a_transition_is_published_only_once_it_commits_in_its_term() {
    struct Case {
        what: &'static str,
        /// The ISR of the image the coordinator starts from.
        isr: &'static [u64],
        /// Whether the coordinator checks the metadata image.
        with_metadata: bool,
        change: Change,
        txn_timeout_ms: i32,
        /// The code the client gets, or `Ok` for a committed transition.
        expected: Result<(), i16>,
        /// The leader epoch of every batch in the log afterwards.
        logged_epochs: Vec<i32>,
    }
    let long = 60_000;
    let cases = [
        Case {
            what: "the follower fetches the record",
            isr: &[1, 2],
            with_metadata: true,
            change: Change::FollowersCatchUp,
            txn_timeout_ms: long,
            expected: Ok(()),
            logged_epochs: vec![EPOCH],
        },
        Case {
            what: "another broker leads in the image first",
            isr: &[1, 2],
            with_metadata: true,
            change: Change::ImageMovesTo(2, EPOCH + 1, &[2]),
            txn_timeout_ms: long,
            expected: Err(codes::NOT_COORDINATOR),
            logged_epochs: vec![EPOCH],
        },
        Case {
            what: "this broker leads again at a newer epoch",
            isr: &[1, 2],
            with_metadata: true,
            change: Change::ImageMovesTo(1, EPOCH + 1, &[1, 2]),
            txn_timeout_ms: long,
            expected: Err(codes::NOT_COORDINATOR),
            logged_epochs: vec![EPOCH],
        },
        Case {
            what: "the coordinator's term ends",
            isr: &[1, 2],
            with_metadata: false,
            change: Change::TermMovesTo(2, EPOCH + 1),
            txn_timeout_ms: long,
            expected: Err(codes::NOT_COORDINATOR),
            logged_epochs: vec![EPOCH],
        },
        Case {
            what: "the follower never fetches the record",
            isr: &[1, 2],
            with_metadata: true,
            change: Change::Nothing,
            txn_timeout_ms: 200,
            expected: Err(codes::COORDINATOR_NOT_AVAILABLE),
            logged_epochs: vec![EPOCH],
        },
        Case {
            what: "the ISR is below min.insync.replicas",
            isr: &[1],
            with_metadata: true,
            change: Change::Nothing,
            txn_timeout_ms: long,
            expected: Err(codes::COORDINATOR_NOT_AVAILABLE),
            logged_epochs: vec![],
        },
        Case {
            what: "the ISR shrinks below min.insync.replicas before the record commits",
            isr: &[1, 2],
            with_metadata: true,
            change: Change::IsrShrinksThenFollowersCatchUp,
            txn_timeout_ms: long,
            expected: Err(codes::COORDINATOR_NOT_AVAILABLE),
            logged_epochs: vec![EPOCH],
        },
    ];
    for case in cases {
        let dir = tempfile::tempdir().expect("tempdir");
        let metadata = Arc::new(
            FakeMetadataSource::builder()
                .image(state_image(1, EPOCH, case.isr))
                .build(),
        );
        let (coordinator, partition) = coordinator(dir.path(), &metadata, case.with_metadata).await;
        let entry = TxnEntry::new_empty(TID.into(), ProducerId(7), 0, case.txn_timeout_ms, 0);

        let changes = {
            let (coordinator, partition, metadata) = (
                Arc::clone(&coordinator),
                Arc::clone(&partition),
                Arc::clone(&metadata),
            );
            let change = case.change;
            tokio::spawn(async move {
                if matches!(change, Change::Nothing) {
                    return;
                }
                wait_for_log_end(&partition, Offset(1)).await;
                match change {
                    Change::Nothing => {}
                    Change::FollowersCatchUp => catch_up(&partition).await,
                    Change::ImageMovesTo(leader, epoch, isr) => {
                        metadata.set_image(state_image(leader, epoch, isr));
                    }
                    Change::IsrShrinksThenFollowersCatchUp => {
                        metadata.set_image(state_image(1, EPOCH, &[1]));
                        catch_up(&partition).await;
                    }
                    Change::TermMovesTo(leader, epoch) => {
                        drop(
                            coordinator
                                .refresh_leader_partitions(&state_image(leader, epoch, &[leader]))
                                .await,
                        );
                    }
                }
            })
        };
        let persisted = coordinator.put(entry, TxnVersion::Classic).await;
        changes.await.expect("the changes land");

        let outcome = match persisted {
            Ok(_) => Ok(()),
            Err(BrokerError::TransactionStateWriteUncommitted { partition, code }) => {
                assert!(partition == 0, "{}", case.what);
                Err(code)
            }
            Err(other) => panic!("{}: not a state write error: {other}", case.what),
        };
        let published = coordinator.get(TID).is_some();
        assert!(outcome == case.expected, "{}", case.what);
        assert!(published == case.expected.is_ok(), "{}", case.what);
        assert!(
            logged_epochs(&partition) == case.logged_epochs,
            "{}",
            case.what
        );
    }
}

/// An expired transactional id leaves the map only once its tombstone
/// commits, as Kafka's `removeFromCacheCallback` removes it only on a
/// complete append. A tombstone the next leader may never see leaves the id
/// in place for the next sweep.
#[tokio::test]
async fn a_tombstone_removes_the_id_only_once_it_commits() {
    // (what, the term moves before the commit, the id is still in the map)
    let cases = [
        ("the follower fetches the tombstone", false, false),
        ("another broker takes the partition first", true, true),
    ];
    for (what, moves, kept) in cases {
        let dir = tempfile::tempdir().expect("tempdir");
        let metadata = Arc::new(
            FakeMetadataSource::builder()
                .image(state_image(1, EPOCH, &[1, 2]))
                .build(),
        );
        let (coordinator, partition) = coordinator(dir.path(), &metadata, true).await;
        let entry = TxnEntry::new_empty(TID.into(), ProducerId(7), 0, 60_000, 0);
        let changes = {
            let partition = Arc::clone(&partition);
            tokio::spawn(async move {
                wait_for_log_end(&partition, Offset(1)).await;
                catch_up(&partition).await;
            })
        };
        let entry = coordinator
            .put(entry, TxnVersion::Classic)
            .await
            .expect("the entry commits");
        changes.await.expect("the follower catches up");

        let changes = {
            let (partition, metadata) = (Arc::clone(&partition), Arc::clone(&metadata));
            tokio::spawn(async move {
                wait_for_log_end(&partition, Offset(2)).await;
                if moves {
                    metadata.set_image(state_image(2, EPOCH + 1, &[2]));
                } else {
                    catch_up(&partition).await;
                }
            })
        };
        let tombstoned = coordinator.tombstone(&entry).await;
        changes.await.expect("the changes land");

        assert!(tombstoned.is_ok() == !kept, "{what}");
        assert!(coordinator.get(TID).is_some() == kept, "{what}");
    }
}
