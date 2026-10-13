//! The copy pass after a leader change, over a replica whose segment
//! boundaries are not the old leader's.
//!
//! A replica rolls its own segments. Two replicas of one partition hold the
//! same offsets and need not agree on where a segment ends, and after any
//! leader election -- a reassignment, a rolling upgrade -- the new leader's
//! boundaries are the ones the copy pass sees. A copy pass that decides what
//! to copy by matching base offsets then re-uploads every sealed segment whose
//! base does not happen to match one already in the tier, and skips outright a
//! segment whose base matches a copied one but that runs further, which leaves
//! the tier a hole nothing ever fills.
//!
//! This suite gives each broker a different `log.segment.bytes` and creates a
//! topic that overrides none of them, so the two replicas of the partition
//! roll at different offsets over the same records. It then tiers, fails the
//! partition over to the survivor, produces and tiers again, and reads the
//! tier back out of the shared object store.

use {
    crate::{
        multi_client::topic_id_for,
        multi_cluster::{ready_admin, start_three_tiered_brokers_with_segment_sizes},
        multi_workload::local_segment_bases,
        support::{
            client::connect_owned, storage::topic_remote_log_files, topics::AssignedTopicSetup,
        },
        topic_fixture::{
            await_tiered_replicas, create_assigned_topic, produce_records as produce_tiered_records,
        },
    },
    assert2::{assert, check},
    krabka_broker::{BrokerHandle, NodeId, metrics::TopicLabel},
    krabka_client_core::Client,
    krabka_protocol::{primitives::uuid::Uuid as WireUuid, records::RecordBatch},
    std::{
        collections::BTreeSet,
        path::Path,
        time::{Duration, Instant},
    },
};

/// The topic this suite produces into. It sets no `segment.bytes`, so each
/// replica rolls on its own broker-level default -- which is the whole point.
const TOPIC: &str = "tiered-misaligned-itest";
/// Records produced before the failover, and again after it. Each one is a
/// batch of its own, so both replicas roll many times over the run.
const RECORDS: usize = 140;

/// One segment the shared object store holds, as the offsets its bytes
/// actually carry rather than as the name it was written under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RemoteSegment {
    first_offset: i64,
    last_offset: i64,
}

/// Every `*.log` object under `root` that belongs to `TOPIC`, decoded into the
/// offset range its record batches cover, ascending.
///
/// The ranges come out of the bytes, not out of the metadata that describes
/// them, so a duplicate here is a duplicate object in the store and a hole
/// here is a hole in the only copy that survives local retention.
fn remote_segments(root: &Path) -> Vec<RemoteSegment> {
    let files = topic_remote_log_files(root, TOPIC);
    let mut segments: Vec<RemoteSegment> = files
        .iter()
        .filter_map(|path| {
            let bytes = std::fs::read(path).ok()?;
            let mut cursor: &[u8] = &bytes;
            let mut first: Option<i64> = None;
            let mut last: Option<i64> = None;
            while !cursor.is_empty() {
                let batch = RecordBatch::decode(&mut cursor).ok()?;
                first.get_or_insert(batch.base_offset);
                last = Some(batch.base_offset + i64::from(batch.last_offset_delta));
            }
            Some(RemoteSegment {
                first_offset: first?,
                last_offset: last?,
            })
        })
        .collect();
    segments.sort_unstable();
    segments
}

/// Produces `count` single-record batches through `client`, one request each,
/// so both replicas see many small batches and roll on their own byte budget.
async fn produce_records(client: &Client, topic_id: WireUuid, prefix: &str, count: usize) {
    produce_tiered_records()
        .client(client)
        .value(|index| bytes::Bytes::from(format!("{prefix}-record-{index}")))
        .topic(TOPIC)
        .topic_id(topic_id)
        .count(count)
        .call()
        .await;
}

/// Creates the tiered topic, deliberately without a `segment.bytes` override,
/// and waits until both replicas hold the tiered configuration.
///
/// The replicas are assigned by hand as `[2, 1]`, so broker 2 is the preferred
/// leader and broker 1 the follower. That is not cosmetic: broker 1 is the
/// address every broker's RLMM client bootstraps against, and this suite kills
/// the partition leader. Killing broker 1 would take the metadata manager's
/// only entry point down with it, and the survivor's copy pass would fail at
/// its `CopySegmentStarted` write before it uploaded anything. Kafka reads a
/// manual assignment as `num_partitions = -1, replication_factor = -1`.
async fn create_misaligned_topic(admin: &Client, b1: &BrokerHandle, b2: &BrokerHandle) {
    let assignment = AssignedTopicSetup::on_replicas(TOPIC, vec![
        krabka_metadata::NodeId(2),
        krabka_metadata::NodeId(1),
    ]);
    create_assigned_topic()
        .client(admin)
        .topic(assignment)
        .maybe_segment_bytes(None)
        .call()
        .await;

    await_tiered_replicas(
        &[b1, b2],
        TOPIC,
        None,
        "the tiered configuration did not reach both replicas",
    )
    .await;
}

/// Polls the shared store until it holds at least `wanted` segments of
/// `TOPIC`, then returns them.
async fn await_remote_segments(remote_dir: &Path, wanted: usize, what: &str) -> Vec<RemoteSegment> {
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let segments = remote_segments(remote_dir);
        if segments.len() >= wanted {
            return segments;
        }
        assert!(
            Instant::now() <= deadline,
            "{what}: the shared store holds {} segment(s), wanted {wanted}",
            segments.len()
        );
        // intentional: the copy task runs on the broker's own 1s interval, and
        // the object store is a directory; there is no in-process signal.
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// The offset through which the tier's coverage runs unbroken from offset 0,
/// and the segments that are redundant: every offset they hold is held by some
/// other segment, so the object is a second copy of data the tier already had.
fn coverage_and_redundancy(segments: &[RemoteSegment]) -> (i64, Vec<RemoteSegment>) {
    let mut covered_through: i64 = -1;
    for segment in segments {
        if segment.first_offset <= covered_through + 1 {
            covered_through = covered_through.max(segment.last_offset);
        } else {
            break;
        }
    }
    let mut redundant = Vec::new();
    for (index, segment) in segments.iter().enumerate() {
        let others: BTreeSet<i64> = segments
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .flat_map(|(_, other)| other.first_offset..=other.last_offset)
            .collect();
        if (segment.first_offset..=segment.last_offset).all(|offset| others.contains(&offset)) {
            redundant.push(*segment);
        }
    }
    (covered_through, redundant)
}

/// The tier a new leader inherits is a set of offset ranges, not a set of
/// segment boundaries, and this is the test that says so.
///
/// One replica rolls at 1 KiB and the other at 1.5 KiB, so their segments
/// cover the same offsets with different boundaries. The elected leader
/// produces and tiers; it is then shut down, the survivor is elected, and it
/// produces and tiers over its own boundaries. What the store then holds must
/// be one unbroken run of offsets with no object that merely repeats offsets
/// another object already holds -- which is what a resume point taken from the
/// tier's coverage produces and what a base-offset skip set does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_leader_resumes_the_copy_from_the_tiers_coverage() {
    let (b1, b2, b3, log_dirs, remote_dir) = start_three_tiered_brokers_with_segment_sizes([
        krabka_units::kibibytes(1),
        krabka_units::bytes(1536),
        krabka_units::kibibytes(1),
    ])
    .await;
    let admin = ready_admin([&b1, &b2, &b3], "tiered-misaligned-admin").await;
    create_misaligned_topic(&admin, &b1, &b2).await;

    // With 3 registered brokers and rf=2 the partition sits on brokers 1 and
    // 2; wait until the image names one of them and read which.
    let (b1_id, b2_id) = (b1.node_id(), b2.node_id());
    b1.wait_for_image(|img| {
        img.partition(TOPIC, 0)
            .is_some_and(|p| p.leader == b1_id || p.leader == b2_id)
    })
    .await;
    let leader_is_b1 = b1.partition_leader_for_test(TOPIC, 0) == Some(b1_id);
    assert!(
        !leader_is_b1,
        "the manual assignment makes broker 2 the preferred leader; broker 1 has to survive \
         the kill because every broker's RLMM bootstraps against its address"
    );
    let (leader_index, survivor_index) = if leader_is_b1 { (0, 1) } else { (1, 0) };

    let topic_id = topic_id_for(&admin, TOPIC).await;
    // Produce through the leader's own listener. `admin` bootstraps against
    // broker 1, which the manual assignment makes the follower.
    let leader_addr = if leader_is_b1 {
        format!("127.0.0.1:{}", b1.listen_addr().port())
    } else {
        format!("127.0.0.1:{}", b2.listen_addr().port())
    };
    let producer = connect_owned(
        &leader_addr,
        "tiered-misaligned-producer",
        "producer client",
    )
    .await;
    let leader_dir = log_dirs[leader_index].path().join(format!("{TOPIC}-0"));
    let survivor_dir = log_dirs[survivor_index].path().join(format!("{TOPIC}-0"));

    // Produce in two halves and read both replicas' boundaries in between.
    // They must differ: that is the condition this test exists for, and if a
    // future change made the two replicas roll at the same offsets the
    // failover below would prove nothing. The sample is taken mid-produce,
    // while both replicas still hold sealed segments to compare.
    produce_records(&producer, topic_id, "before", RECORDS / 2).await;
    let leader_bases = local_segment_bases(&leader_dir);
    let survivor_bases = local_segment_bases(&survivor_dir);
    assert!(
        leader_bases != survivor_bases,
        "the replicas rolled at the same offsets ({leader_bases:?} vs {survivor_bases:?}); \
         this test needs misaligned segment boundaries"
    );
    produce_records(&producer, topic_id, "before-more", RECORDS - RECORDS / 2).await;

    let before = await_remote_segments(remote_dir.path(), 2, "the first leader's copy").await;
    eprintln!("ITEST: the first leader tiered {before:?}");

    // The survivor has to hold the copy metadata before it is elected, or its
    // first copy pass as leader would resume from its own log start over a
    // tier it cannot see. Its own local retention says it does: a follower
    // evicts only what the shared RLMM reports copied, so its oldest local
    // segment moving off zero is that metadata having arrived.
    let propagation_deadline = Instant::now() + Duration::from_mins(2);
    loop {
        if local_segment_bases(&survivor_dir)
            .first()
            .is_some_and(|base| *base > 0)
        {
            break;
        }
        assert!(
            Instant::now() <= propagation_deadline,
            "the follower never evicted a copied segment, so it never saw the copy metadata"
        );
        // intentional: the follower's RLMM consumer and its retention pass
        // both run on the broker's own 1s interval; no in-process signal.
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    drop(admin);
    drop(producer);
    let mut opt_b1 = Some(b1);
    let mut opt_b2 = Some(b2);
    let (leader, leader_id) = if leader_is_b1 {
        (opt_b1.take().expect("broker 1 handle"), b1_id)
    } else {
        (opt_b2.take().expect("broker 2 handle"), b2_id)
    };
    leader.shutdown().await;
    eprintln!("ITEST: the first leader is down; waiting for the failover");
    let survivor = opt_b1
        .as_ref()
        .or(opt_b2.as_ref())
        .expect("one of the two replicas survives");
    survivor
        .wait_until_partition_leader_changed(TOPIC, 0, NodeId(leader_id))
        .await;

    let survivor_bootstrap = format!("127.0.0.1:{}", survivor.listen_addr().port());
    let survivor_client = connect_owned(
        &survivor_bootstrap,
        "tiered-misaligned-survivor",
        "survivor client",
    )
    .await;
    produce_records(&survivor_client, topic_id, "after", RECORDS).await;

    // The new leader's copy pass has to reach past what the old one tiered.
    let deadline = Instant::now() + Duration::from_mins(4);
    let after = loop {
        let segments = remote_segments(remote_dir.path());
        if segments.len() > before.len() {
            break segments;
        }
        assert!(
            Instant::now() <= deadline,
            "the new leader copied nothing past the resume point: the store still holds \
             {segments:?} while the new leader's local segments start at {:?}",
            local_segment_bases(&survivor_dir)
        );
        eprintln!(
            "ITEST: waiting for the new leader's copy: remote={} local={:?} leader={:?} \
             log_start={:?}",
            segments.len(),
            local_segment_bases(&survivor_dir),
            survivor.partition_leader_for_test(TOPIC, 0),
            survivor.partition_log_start_for_test(TOPIC, 0),
        );
        // intentional: the copy task runs on the broker's own 1s interval.
        tokio::time::sleep(Duration::from_secs(5)).await;
    };
    eprintln!("ITEST: the tier after the failover holds {after:?}");

    // KIP-405's copy-lag gauge on the new leader. It counts the sealed local
    // segments the tier does not hold whole, so a new leader holding segments
    // the previous one never copied has to show them here. This is the series
    // that read zero over a segment the old skip set had marked copied by its
    // base offset while nothing had ever been uploaded for it.
    let lag_label = TopicLabel {
        topic: std::sync::Arc::from(TOPIC),
    };
    survivor
        .wait_for_metrics(
            "the survivor counts its uncopied segments as lag",
            move |m| m.remote_copy_lag_segments.get_or_create(&lag_label).get() >= 1,
        )
        .await;

    // Local retention still evicts on the new leader: the same RLMM coverage
    // that placed the resume point is what lets a replica drop its own copies,
    // and it can only reach the new leader's boundaries if the segments over
    // them were really copied. Waiting for it also settles the state the
    // assertions below read.
    let eviction_deadline = Instant::now() + Duration::from_mins(2);
    loop {
        if local_segment_bases(&survivor_dir).len() == 1 {
            break;
        }
        assert!(
            Instant::now() <= eviction_deadline,
            "the new leader still holds {:?}; nothing evicted them, so the tier does not \
             cover them",
            local_segment_bases(&survivor_dir)
        );
        // intentional: local retention runs on the broker's own interval tick.
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    let segments = remote_segments(remote_dir.path());
    let (covered_through, redundant) = coverage_and_redundancy(&segments);
    let highest = segments
        .iter()
        .map(|segment| segment.last_offset)
        .max()
        .expect("the tier holds segments");

    // No gap: the tier's coverage runs unbroken from offset 0 through the
    // highest offset any object holds. A skip set keyed on base offsets leaves
    // a hole here whenever a new leader's segment shares a base with a copied
    // one and runs further than it, and nothing ever fills it.
    check!(
        covered_through == highest,
        "the tier's coverage from 0 stops at {covered_through} of {highest}: {segments:?}"
    );
    // No duplicated data: every object carries at least one offset no other
    // object carries. A skip set keyed on base offsets re-uploads whole
    // segments here, once for every sealed segment the new leader still holds
    // whose base does not line up with a copied one.
    check!(
        redundant.is_empty(),
        "the tier holds {} object(s) whose offsets another object already holds: {redundant:?}",
        redundant.len()
    );

    // And the coverage really does reach the new leader's own segments: its
    // active segment's base is one past the last offset it sealed, and every
    // sealed segment below it has been copied and then evicted.
    let active_base = *local_segment_bases(&survivor_dir)
        .last()
        .expect("the survivor holds an active segment");
    check!(
        covered_through >= active_base - 1,
        "the tier covers only through {covered_through} while the survivor's active segment \
         starts at {active_base}"
    );

    drop(survivor_client);
    if let Some(handle) = opt_b1.take() {
        handle.shutdown().await;
    }
    if let Some(handle) = opt_b2.take() {
        handle.shutdown().await;
    }
    b3.shutdown().await;
}
