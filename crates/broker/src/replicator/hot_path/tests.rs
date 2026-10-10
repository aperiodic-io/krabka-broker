//! Tests for the replicator's benchmark seam.
//!
//! [`ReplicaSeam::spawn`] rebuilds a [`Partition`] by struct literal rather
//! than through `broker::partition_spawn`, because the production constructor
//! wants a broker, a log-dir registry and a diskless WAL the "what does the
//! extra `encoded_len` walk cost" question has no use for. That literal is the
//! seam's one real hazard: a `Partition` field added to production is a field
//! the seam has to be given a value for, and the value has to be the one
//! production picks.
//!
//! That already went wrong once. `marker_materialization` arrived on
//! `Partition` while this branch was out, and the seam only survived because
//! a missing field is a compile error. A field that *compiled* with the wrong
//! value would have gone through, and the benchmark would have carried on
//! reporting a number for a partition the broker never runs.
//!
//! So these tests stand the seam next to a partition the broker really spawns
//! and compare them: every field of the reconstruction in one projection, and
//! the bytes the two logs hold after replicating the same batches.

use std::{path::PathBuf, sync::Arc};

use assert2::assert;
use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig, Offset};
use krabka_protocol::records::RecordBatch;
use krabka_units::mebibytes;
use tempfile::TempDir;

use super::ReplicaSeam;
use crate::{
    broker::spawn_partition, log_dir_status::LogDirRegistry, partition::Partition,
    producer_state::ProducerState, replica_state::ReplicaState,
};

/// The topic and partition the seam hard-codes, which a production partition
/// has to be spawned under for the two to be comparable.
const TOPIC: &str = "bench-topic";
const PARTITION: PartitionIndex = PartitionIndex(0);

/// The leader epoch a replicated batch carries, as `benches/perf_deferrals.rs`
/// stamps it.
const LEADER_EPOCH: i32 = 3;

/// Writer-channel depth the seam opens.
///
/// This is the single field the seam is allowed to differ from production on
/// (which reads `partition_writer_queue_depth`, 64). `replicate` awaits each
/// batch's acknowledgement before returning, so the channel never holds more
/// than the one message and its depth cannot reach a measurement. Every other
/// field is compared for equality below.
const SEAM_WRITER_QUEUE_DEPTH: usize = 8;

/// Everything a [`Partition`] holds that has an observable value.
///
/// Built by an exhaustive destructure, so a field added to `Partition` cannot
/// reach the seam without a decision being recorded here.
#[derive(Debug, PartialEq)]
struct PartitionShape {
    topic: String,
    index: PartitionIndex,
    log_dir: PathBuf,
    log: LogImage,
    writer_queue_depth: usize,
    materializing_markers: usize,
    replica_state: ReplicaState,
    delivery_watermark: Offset,
    current_leader: u64,
    current_leader_epoch: i32,
    replication_target: crate::partition::ReplicationTarget,
    diskless: bool,
    writer_running: bool,
}

/// A partition's log, as both raw `.log` bytes and decoded batches.
#[derive(Debug, PartialEq)]
struct LogImage {
    start_offset: Offset,
    end_offset: Offset,
    lso: Offset,
    bytes: Bytes,
    batches: Vec<RecordBatch>,
}

fn log_image(partition: &Partition) -> LogImage {
    let log = partition.log.lock().expect("the writer has not panicked");
    let start = log.log_start_offset();
    let end = log.log_end_offset();
    LogImage {
        start_offset: start,
        end_offset: end,
        lso: log.lso(),
        bytes: log
            .read_raw(start, end, mebibytes(1))
            .expect("read the follower log back verbatim")
            .bytes,
        batches: log
            .read(start, mebibytes(1))
            .expect("decode the follower log back")
            .batches,
    }
}

/// Project `partition` onto everything about it that can be observed.
async fn shape(partition: &Partition) -> PartitionShape {
    // Exhaustive on purpose: `..` here would let the next `Partition` field
    // land in the seam unexamined, which is the failure this module exists to
    // catch. The three bindings discarded below are `Notify`s and a clock
    // handle, which hold no value to compare.
    let Partition {
        topic,
        index,
        log_dir,
        log: _,
        writer_tx,
        marker_materialization,
        append_notify: _,
        replica_state,
        hw_advance_notify: _,
        delivery,
        current_leader,
        current_leader_epoch,
        replication_target,
        diskless,
        writer_handle,
    } = partition;

    let writer_running = writer_handle
        .lock()
        .expect("the writer handle is uncontended")
        .as_ref()
        .is_some_and(|handle| !handle.is_finished());

    PartitionShape {
        topic: topic.clone(),
        index: *index,
        log_dir: log_dir.load().as_ref().clone(),
        log: log_image(partition),
        writer_queue_depth: writer_tx.max_capacity(),
        materializing_markers: marker_materialization.lock().await.len(),
        replica_state: replica_state.lock().await.clone(),
        delivery_watermark: delivery.watermark(),
        current_leader: current_leader.load(std::sync::atomic::Ordering::SeqCst),
        current_leader_epoch: current_leader_epoch.load(std::sync::atomic::Ordering::SeqCst),
        replication_target: *replication_target.read().await,
        diskless: *diskless,
        writer_running,
    }
}

/// A follower partition built the way the broker builds one, over `dir`.
///
/// The seam passes its `dir` as both the log's directory and the partition's
/// `log_dir`, so this does the same; production's `log_dir` is normally the
/// parent `log.dir`, and handing the two constructors different arguments
/// would compare a difference this test did not make.
fn production_partition(dir: &TempDir) -> Arc<Partition> {
    spawn_partition(
        TOPIC.to_string(),
        PARTITION,
        dir.path().to_path_buf(),
        Log::open(dir.path(), LogConfig::default()).expect("open the follower log"),
        LogDirRegistry::default(),
        Arc::new(ProducerState::new()),
        false,
    )
}

// Explicit epochs retain the original unclamped last-offset delta.
krabka_macros::record_batch_fixture!(make_batch, LEADER_EPOCH);

/// The batch shapes the bench measures, scaled down to what a unit test should
/// write to disk. The shape mix is what matters here, not the magnitude.
const SHAPES: [(&str, i32, usize); 3] = [
    ("1rec_1KiB", 1, 1024),
    ("16rec_64B", 16, 64),
    ("128rec_8B", 128, 8),
];

struct ReplicaPair {
    seam: ReplicaSeam,
    production: Arc<Partition>,
    seam_dir: TempDir,
    _production_dir: TempDir,
}

fn replica_pair() -> ReplicaPair {
    let seam_dir = tempfile::tempdir().expect("tempdir");
    let production_dir = tempfile::tempdir().expect("tempdir");
    let seam = ReplicaSeam::spawn(seam_dir.path()).expect("open the seam's follower log");
    let production = production_partition(&production_dir);
    ReplicaPair {
        seam,
        production,
        seam_dir,
        _production_dir: production_dir,
    }
}

/// Every field the seam's struct literal fills in holds the value the broker's
/// own constructor puts there.
///
/// Both partitions are freshly spawned over their own empty log directory, so
/// any difference in this comparison is a difference the seam introduced.
#[tokio::test]
async fn the_seam_reconstructs_the_partition_the_broker_spawns() {
    let pair = replica_pair();
    let seam = &pair.seam;
    let production = &pair.production;

    let expected = PartitionShape {
        // The two partitions live in different temp directories, and the
        // writer-queue depth is the documented divergence above. Everything
        // else has to match.
        log_dir: pair.seam_dir.path().to_path_buf(),
        writer_queue_depth: SEAM_WRITER_QUEUE_DEPTH,
        ..shape(production).await
    };

    assert!(shape(&seam.partition).await == expected);
}

/// The same batches replicated through the seam and through a broker-spawned
/// partition land the same bytes, at the same offsets, in the same order.
///
/// This is what makes the benchmark's `replicate_batch` timing a measurement
/// of replication rather than of some adjacent thing: the append the seam runs
/// is byte-for-byte the append the broker runs.
#[tokio::test]
async fn the_seam_replicates_the_bytes_the_broker_partition_replicates() {
    for (name, records, payload) in SHAPES {
        let pair = replica_pair();
        let seam = &pair.seam;
        let production = &pair.production;

        for _ in 0..3 {
            let template = make_batch(records, payload);

            let mut for_seam = template.clone();
            for_seam.base_offset = seam.next_offset().0;
            seam.replicate(for_seam)
                .await
                .unwrap_or_else(|error| panic!("{name}: seam rejected the batch: {error}"));

            let mut for_production = template;
            for_production.base_offset = production.log_end_offset().0;
            production
                .replicate_batch(for_production)
                .await
                .unwrap_or_else(|error| panic!("{name}: broker rejected the batch: {error}"));
        }

        assert!(
            log_image(&seam.partition) == log_image(production),
            "{}",
            name
        );
        assert!(
            seam.next_offset() == production.log_end_offset(),
            "{}",
            name
        );
    }
}

/// `encoded_len` — the walk the PERF note is about — counts the bytes the
/// append behind it actually writes.
///
/// The replicator reports that number as `record_replication_in`, so if the
/// two ever parted the metric would be measuring one thing and the note's
/// benchmark another.
#[tokio::test]
async fn the_walk_the_perf_note_prices_counts_the_bytes_the_append_lands() {
    for (name, records, payload) in SHAPES {
        let dir = tempfile::tempdir().expect("tempdir");
        let seam = ReplicaSeam::spawn(dir.path()).expect("open the seam's follower log");

        let mut walked = 0_usize;
        for _ in 0..3 {
            let mut batch = make_batch(records, payload);
            batch.base_offset = seam.next_offset().0;
            walked += batch.encoded_len();
            seam.replicate(batch)
                .await
                .unwrap_or_else(|error| panic!("{name}: seam rejected the batch: {error}"));
        }

        assert!(log_image(&seam.partition).bytes.len() == walked, "{}", name);
    }
}

/// `next_offset` is the offset the append will accept, and nothing else is.
///
/// The doc comment on it says the caller stamps this into the batch the way
/// the leader already has; this is that claim, on both sides.
#[tokio::test]
async fn the_seam_takes_a_batch_only_at_the_offset_next_offset_reports() {
    let dir = tempfile::tempdir().expect("tempdir");
    let seam = ReplicaSeam::spawn(dir.path()).expect("open the seam's follower log");

    assert!(seam.next_offset() == Offset(0));

    let mut first = make_batch(4, 32);
    first.base_offset = seam.next_offset().0;
    seam.replicate(first)
        .await
        .expect("the first batch appends");
    assert!(seam.next_offset() == Offset(4));

    // Re-stamping at an offset the log has already passed is exactly what a
    // seam that forgot to advance would produce, so it must not be accepted.
    let mut stale = make_batch(4, 32);
    stale.base_offset = 0;
    assert!(seam.replicate(stale).await.is_err());
    assert!(seam.next_offset() == Offset(4));
}
