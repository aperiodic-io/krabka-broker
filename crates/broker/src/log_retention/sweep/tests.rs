//! Tests for one retention sweep: that it takes a hosted partition's expired
//! segments off disk through the writer actor, that it does so whether or not
//! this broker leads the partition, that a KFC-9 write freeze stops it, and
//! that a deletion the filesystem refuses takes the log directory offline.

use {
    super::*,
    crate::{
        log_dir_status::LogDirRegistry,
        log_retention::test_support::{
            block_segment_deletion, expired_partition, log_size, segment_files,
        },
        metrics::{CleanerFailureLabel, CleanerFailureReason},
        test_support::{CommittedPartitionSetup, topic_freeze_record, topic_thaw_record},
    },
    assert2::check,
    krabka_ids::PartitionIndex,
    krabka_metadata::{MetadataRecord, NodeId, PatternType},
    uuid::Uuid,
};

/// A partition the sweep is about to trim, with the segment files it had
/// before the sweep.
struct Observed<'a> {
    topic: &'a str,
    partition: Arc<Partition>,
    before: Vec<String>,
}

/// The failure counter's value for one `(topic, partition, reason)`.
fn failures(metrics: &BrokerMetrics, topic: &str, reason: CleanerFailureReason) -> u64 {
    metrics
        .log_retention_failures
        .get_or_create(&CleanerFailureLabel {
            topic: topic.into(),
            partition: 0,
            reason,
        })
        .get()
}

/// An image holding one live freeze entry for `scope`.
fn image_with_freeze(scope: &str) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::from_u128(0x5150));
    image.apply(&MetadataRecord::V1TopicFreeze(
        topic_freeze_record().scope(scope).call(),
    ));
    image
}

/// The sweep deletes expired sealed segments through the writer actor, and the
/// log's reported size follows them off disk.
///
/// This is the behaviour the broker had none of: `Log::tick` did all of it and
/// nothing called `Log::tick`. The assertion is on the files and on
/// `Log::size`, not on a direct call, so a sweep that stopped dispatching
/// would fail here.
#[tokio::test]
async fn a_sweep_deletes_expired_segments_and_the_log_size_follows() {
    let dir = tempfile::tempdir().expect("log root");
    let registry = PartitionRegistry::new();
    let partition = expired_partition(&dir, CommittedPartitionSetup {
        leader: NodeId(7),
        ..Default::default()
    })
    .await;
    registry.insert("orders".into(), PartitionIndex(0), Arc::clone(&partition));
    let before_files = segment_files(&dir, "orders");
    let before_size = log_size(&partition);
    check!(
        before_files.len() >= 2,
        "the fixture must seal segments for retention to evict: {before_files:?}"
    );
    check!(before_size > 0);

    let metrics = BrokerMetrics::new();
    tick_all(&registry, None, &metrics).await;

    let after_files = segment_files(&dir, "orders");
    check!(
        after_files.len() < before_files.len(),
        "expired segments should have left the disk: {before_files:?} -> {after_files:?}"
    );
    check!(
        !after_files.is_empty(),
        "a log must keep a segment to append to"
    );
    check!(
        log_size(&partition) < before_size,
        "the log's reported size must follow the files off disk"
    );
    check!(metrics.log_retention_runs_total.get() == 1);
    check!(failures(&metrics, "orders", CleanerFailureReason::Io) == 0);
}

/// Retention runs over every log the broker hosts, leader or follower.
///
/// Kafka's `LogManager.cleanupLogs` walks the broker's own logs and does not
/// consult leadership; the cleaner's compaction sweep does, and the two must
/// not be made to agree. A follower whose replica is never trimmed is exactly
/// the descriptor climb this loop exists to stop.
#[tokio::test]
async fn a_sweep_trims_a_follower_replica_as_well_as_a_led_one() {
    let dir = tempfile::tempdir().expect("log root");
    let registry = PartitionRegistry::new();
    // This broker is node 7: it leads `led` and merely follows `followed`.
    let cases = [("led", NodeId(7)), ("followed", NodeId(8))];
    let mut observed: Vec<Observed<'_>> = Vec::new();
    for (topic, leader) in cases {
        let partition = expired_partition(&dir, CommittedPartitionSetup {
            topic,
            leader,
            ..Default::default()
        })
        .await;
        let before = segment_files(&dir, topic);
        registry.insert(topic.into(), PartitionIndex(0), Arc::clone(&partition));
        observed.push(Observed {
            topic,
            partition,
            before,
        });
    }

    let metrics = BrokerMetrics::new();
    tick_all(&registry, None, &metrics).await;

    for Observed {
        topic,
        partition,
        before,
    } in observed
    {
        let after = segment_files(&dir, topic);
        check!(
            after.len() < before.len(),
            "{topic}: retention must trim it whether or not this broker leads it: \
             {before:?} -> {after:?}"
        );
        drop(partition);
    }
}

/// A KFC-9 write freeze stops the sweep, and the thaw releases it.
///
/// Retention removes data from the log, which is the whole of the freeze
/// rule's subject: the frozen prefix has to stay byte-identical between the
/// two sites of a disaster-recovery pair, and a retention pass on one side
/// leaves the two logs starting at different offsets.
#[tokio::test]
async fn a_frozen_topic_is_not_trimmed_until_it_thaws() {
    let dir = tempfile::tempdir().expect("log root");
    let registry = PartitionRegistry::new();
    let partition = expired_partition(&dir, CommittedPartitionSetup {
        topic: "frozen",
        leader: NodeId(7),
        ..Default::default()
    })
    .await;
    registry.insert("frozen".into(), PartitionIndex(0), Arc::clone(&partition));
    let before = segment_files(&dir, "frozen");

    let metrics = BrokerMetrics::new();
    let mut image = image_with_freeze("frozen");
    tick_all(&registry, Some(&image), &metrics).await;

    check!(
        segment_files(&dir, "frozen") == before,
        "a frozen topic keeps every segment"
    );
    check!(
        metrics.log_retention_runs_total.get() == 1,
        "a skip is not a failure: the sweep still ran"
    );

    // The thaw record clears the entry, and the next sweep trims with no
    // operator step in between.
    image.apply(&MetadataRecord::V1TopicFreeze(topic_thaw_record(
        "frozen",
        PatternType::Literal,
    )));
    tick_all(&registry, Some(&image), &metrics).await;

    check!(
        segment_files(&dir, "frozen").len() < before.len(),
        "the thaw releases the partition to the next sweep"
    );
}

/// A deletion the filesystem refuses is counted, keeps the sweep from
/// reporting a pass, and takes the log directory offline — the escalation
/// #471 gave the cleaner, now reaching retention too.
#[tokio::test]
async fn a_failed_deletion_is_counted_and_takes_the_log_dir_offline() {
    let dir = tempfile::tempdir().expect("log root");
    let status = LogDirRegistry::probe(&[dir.path().to_path_buf()]);
    let registry = PartitionRegistry::new();
    let partition = expired_partition(&dir, CommittedPartitionSetup {
        leader: NodeId(7),
        registry: status.clone(),
        ..Default::default()
    })
    .await;
    {
        // This fault blocks the immediate-deletion tombstone path. Delayed
        // reclamation uses unique names and runs through writer maintenance.
        let log = partition.log.lock().expect("partition log lock");
        let mut config = log.config_snapshot();
        config.file_delete_delay = krabka_units::millis(0);
        log.set_config(config);
    }
    registry.insert("orders".into(), PartitionIndex(0), Arc::clone(&partition));
    // A directory where the eviction must rename each segment to its
    // `.deleted` tombstone: the rename fails with EISDIR, which is a storage
    // error the filesystem raises rather than one a hook fabricates.
    let blocked = block_segment_deletion(&dir, "orders");
    let before = segment_files(&dir, "orders");

    let metrics = BrokerMetrics::new();
    tick_all(&registry, None, &metrics).await;

    check!(
        segment_files(&dir, "orders").len() >= before.len(),
        "nothing left the disk"
    );
    check!(failures(&metrics, "orders", CleanerFailureReason::Io) == 1);
    check!(
        metrics.log_retention_runs_total.get() == 0,
        "a sweep that failed every partition it swept is not a pass"
    );
    check!(
        status.is_offline(dir.path()),
        "a background deletion failure takes the dir offline, as a produce would"
    );

    // The same partition, once the filesystem takes the rename again: the
    // sweep trims it and counts itself.
    for tombstone in blocked {
        std::fs::remove_dir(&tombstone).expect("unblock the tombstone path");
    }
    let before_retry = segment_files(&dir, "orders");
    tick_all(&registry, None, &metrics).await;

    check!(segment_files(&dir, "orders").len() < before_retry.len());
    check!(failures(&metrics, "orders", CleanerFailureReason::Io) == 1);
    check!(metrics.log_retention_runs_total.get() == 1);
}
