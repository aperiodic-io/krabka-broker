//! Behaviour of the two lag samplers, driven through the gauges they publish
//! rather than through their return values: what an operator scrapes is the
//! contract.

use std::{path::Path, sync::Arc, time::Instant};

use assert2::{assert, check};
use krabka_ids::LeaderEpoch;
use krabka_log::Offset;
use krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
use krabka_protocol::{
    owned::fetch_response::{FetchableTopicResponse, PartitionData},
    records::RecordBatch,
};

use super::*;
use crate::{
    coordinator::{
        bootstrap::OFFSETS_TOPIC,
        unified::{
            ImageMetadataProvider,
            actor::{GroupActorHandle, GroupKindTag},
            classic_state::OffsetEntry,
            config::NextGenConfig,
            offsets_log::fake::InMemoryOffsetsLog,
            share::config::ShareGroupConfig,
            streams::config::StreamsGroupConfig,
        },
    },
    partition::Partition,
    test_support::FakeMetadataSource,
};

const TOPIC: &str = "orders";
const LEADER: NodeId = NodeId(1);
const FOLLOWER: NodeId = NodeId(2);
const OTHER_BROKER: NodeId = NodeId(3);

fn lag_metadata(image: MetadataImage) -> Arc<dyn MetadataSource> {
    Arc::new(
        FakeMetadataSource::builder()
            .image(image)
            .leader(Some(LEADER))
            .build(),
    )
}

fn image_with_remote_leader(topic: &str) -> MetadataImage {
    let mut image = coordinator_image();
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.into(),
        partition: 0,
        leader: OTHER_BROKER,
        replicas: vec![OTHER_BROKER],
        isr: vec![OTHER_BROKER],
        leader_epoch: LeaderEpoch(2),
        ..Default::default()
    }));
    image
}

/// A partition backed by a real log under `dir`, registered as led by
/// `LEADER` with `FOLLOWER` as its one in-sync follower.
fn led_partition(dir: &Path, topic: &str, partition: i32) -> Arc<Partition> {
    let part = crate::test_support::open_partition(
        dir,
        crate::test_support::StandalonePartitionSetup {
            topic,
            partition: krabka_ids::PartitionIndex(partition),
            ..Default::default()
        },
    );
    part.current_leader.store(LEADER.0, Ordering::Release);
    part
}

/// Seed `partition`'s replica state with `LEADER` leading and `FOLLOWER` in
/// the ISR, both at offset zero.
async fn install_isr(partition: &Partition) {
    partition.replica_state.lock().await.install_isr(
        &[LEADER, FOLLOWER],
        &[LEADER, FOLLOWER],
        LEADER,
        Instant::now(),
    );
}

/// Append `count` records straight to the log, advancing the leader's log end
/// offset the way a produce does.
fn append_records(partition: &Partition, count: i32) {
    let mut batch = RecordBatch {
        partition_leader_epoch: -1,
        ..crate::test_support::repeated_records_batch(crate::test_support::RepeatedRecordsSetup {
            count: crate::test_support::RecordCount(count),
            timestamp: crate::test_support::UnixMillis(1_700_000_000),
        })
    };
    partition
        .log
        .lock()
        .expect("log mutex")
        .append(&mut batch)
        .expect("append");
}

/// The follower fetches everything the leader holds, as its replica fetcher
/// would.
async fn follower_fetches_everything(partition: &Partition) {
    let leader_log_end = partition.log_end_offset();
    partition.replica_state.lock().await.update_follower_leo(
        FOLLOWER,
        leader_log_end,
        leader_log_end,
        Instant::now(),
    );
}

/// An image assigning both `TOPIC` partitions to `LEADER` and `FOLLOWER`, with
/// `LEADER` leading. The assignment is what justifies a follower's series, so
/// the sampler reads its followers from here rather than from the leader's
/// fetch-driven progress map.
fn replica_image() -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: TOPIC.into(),
        topic_id: uuid::Uuid::from_u128(1),
        partitions: 2,
        replication_factor: 2,
    }));
    for partition in 0..2 {
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: TOPIC.into(),
            partition,
            leader: LEADER,
            replicas: vec![LEADER, FOLLOWER],
            isr: vec![LEADER, FOLLOWER],
            leader_epoch: LeaderEpoch(1),
            ..Default::default()
        }));
    }
    image
}

/// Run one replica-lag pass and publish it, as the poller's tick does.
async fn sample_replica_lag(partitions: &PartitionRegistry, metrics: &BrokerMetrics) {
    metrics.publish_replica_lag(&replica_lag_samples(partitions, LEADER, &replica_image()).await);
}

fn replica_fixture() -> (tempfile::TempDir, PartitionRegistry, Arc<Partition>) {
    let directory = tempfile::tempdir().expect("tempdir");
    let partitions = PartitionRegistry::new();
    let partition = led_partition(directory.path(), TOPIC, 0);
    (directory, partitions, partition)
}

async fn sampled_two_followers() -> (tempfile::TempDir, PartitionRegistry, BrokerMetrics) {
    let directory = tempfile::tempdir().expect("tempdir");
    let partitions = two_follower_lags(directory.path()).await;
    let metrics = BrokerMetrics::new();
    sample_replica_lag(&partitions, &metrics).await;
    (directory, partitions, metrics)
}

async fn committed_classic_poller(
    metadata: Arc<dyn MetadataSource>,
    partitions: Arc<PartitionRegistry>,
    committed: i64,
) -> (LagPoller, BrokerMetrics) {
    let coordinator = coordinator(Arc::clone(&metadata));
    let handle = coordinator.get_or_create_classic("billing");
    commit_offset(&handle, committed).await;
    let metrics = BrokerMetrics::new();
    (
        poller(coordinator, metadata, partitions, metrics.clone()),
        metrics,
    )
}

async fn classic_group_poller(
    dir: &Path,
    high_watermark: i64,
    committed: i64,
) -> (LagPoller, BrokerMetrics) {
    let metadata: Arc<dyn MetadataSource> = lag_metadata(coordinator_image());
    let partitions = partition_at_high_watermark(dir, high_watermark).await;
    committed_classic_poller(metadata, partitions, committed).await
}

/// The published lag of `FOLLOWER` on `TOPIC`-`partition`, or `None` when the
/// family carries no such series.
fn published_replica_lag(metrics: &BrokerMetrics, partition: i32) -> Option<i64> {
    metrics
        .replica_lag
        .get(&ReplicaLagLabel {
            topic: TOPIC.into(),
            partition,
            replica: FOLLOWER.0,
        })
        .map(|gauge| gauge.get())
}

/// The acceptance case: while the follower keeps fetching its lag is zero, and
/// once its fetch is paused every leader append raises the gauge by exactly
/// the records it added.
#[tokio::test]
async fn a_paused_follower_fetch_makes_the_replica_lag_gauge_climb() {
    let (_dir, partitions, partition) = replica_fixture();
    install_isr(&partition).await;
    partitions.insert(TOPIC.into(), PartitionIndex(0), Arc::clone(&partition));
    let metrics = BrokerMetrics::new();

    append_records(&partition, 10);
    follower_fetches_everything(&partition).await;
    sample_replica_lag(&partitions, &metrics).await;
    assert!(published_replica_lag(&metrics, 0) == Some(0));
    check!(metrics.replica_lag_max.get() == 0);

    // The follower's fetch is paused from here on: nothing updates its
    // tracked offset while the leader keeps appending.
    for (appended, expected_lag) in [(5, 5), (5, 10), (20, 30)] {
        append_records(&partition, appended);
        sample_replica_lag(&partitions, &metrics).await;
        check!(published_replica_lag(&metrics, 0) == Some(expected_lag));
        check!(metrics.replica_lag_max.get() == expected_lag);
    }

    // Resuming the fetch takes the gauge back down, so it tracks the follower
    // rather than accumulating.
    follower_fetches_everything(&partition).await;
    sample_replica_lag(&partitions, &metrics).await;
    check!(published_replica_lag(&metrics, 0) == Some(0));
    check!(metrics.replica_lag_max.get() == 0);
}

async fn two_follower_lags(dir: &Path) -> PartitionRegistry {
    let partitions = PartitionRegistry::new();
    for (index, records) in [(0, 3), (1, 40)] {
        let partition = led_partition(dir, TOPIC, index);
        install_isr(&partition).await;
        append_records(&partition, records);
        partitions.insert(TOPIC.into(), PartitionIndex(index), partition);
    }
    partitions
}

/// The max rollup is the largest lag across partitions, not the last one
/// sampled.
#[tokio::test]
async fn the_max_rollup_reports_the_worst_follower_on_the_broker() {
    let (_dir, _partitions, metrics) = sampled_two_followers().await;

    check!(published_replica_lag(&metrics, 0) == Some(3));
    check!(published_replica_lag(&metrics, 1) == Some(40));
    check!(metrics.replica_lag_max.get() == 40);
}

fn append_and_register(partitions: &PartitionRegistry, partition: &Arc<Partition>, records: i32) {
    append_records(partition, records);
    partitions.insert(TOPIC.into(), PartitionIndex(0), Arc::clone(partition));
}

/// Leadership is what justifies a replica-lag series, so the pass that first
/// sees another broker leading the partition takes the series away.
#[tokio::test]
async fn losing_leadership_releases_the_partitions_replica_lag_series() {
    let (_dir, partitions, partition) = replica_fixture();
    install_isr(&partition).await;
    append_and_register(&partitions, &partition, 12);
    let metrics = BrokerMetrics::new();
    sample_replica_lag(&partitions, &metrics).await;
    assert!(published_replica_lag(&metrics, 0) == Some(12));

    partition
        .current_leader
        .store(OTHER_BROKER.0, Ordering::Release);
    sample_replica_lag(&partitions, &metrics).await;

    check!(published_replica_lag(&metrics, 0) == None);
    check!(metrics.replica_lag_max.get() == 0);
}

/// A replica the assignment names but that has never fetched has no entry in
/// the leader's fetch-driven progress map, because `install_isr` seeds only
/// ISR members. It is the follower an operator most needs to see, so it
/// reports the whole log rather than no series at all.
#[tokio::test]
async fn an_assigned_follower_that_has_never_fetched_reports_the_whole_log() {
    let (_dir, partitions, partition) = replica_fixture();
    // The ISR holds the leader alone, which is what a replica added by a
    // reassignment looks like until its first fetch lands.
    partition.replica_state.lock().await.install_isr(
        &[LEADER],
        &[LEADER, FOLLOWER],
        LEADER,
        Instant::now(),
    );
    append_and_register(&partitions, &partition, 12);
    let metrics = BrokerMetrics::new();

    sample_replica_lag(&partitions, &metrics).await;

    assert!(published_replica_lag(&metrics, 0) == Some(12));
    check!(metrics.replica_lag_max.get() == 12);

    // Its first fetch takes the gauge to the real distance rather than
    // creating the series.
    follower_fetches_everything(&partition).await;
    sample_replica_lag(&partitions, &metrics).await;
    check!(published_replica_lag(&metrics, 0) == Some(0));
}

/// Evicting the partition whose follower was supplying the maximum takes the
/// rollup down with it. Only the sampler's next pass would otherwise reset it,
/// and a scrape in between would report a maximum no series carries.
#[tokio::test]
async fn evicting_the_worst_follower_lowers_the_max_rollup() {
    let (_dir, _partitions, metrics) = sampled_two_followers().await;
    assert!(metrics.replica_lag_max.get() == 40);

    metrics.evict_partition_series(&crate::metrics::PartitionLabel {
        topic: TOPIC.into(),
        partition: 1,
    });

    check!(published_replica_lag(&metrics, 1) == None);
    check!(metrics.replica_lag_max.get() == 3);
}

// ── consumer-group lag ───────────────────────────────────────────────────

/// An image holding `TOPIC` with one partition led by `LEADER`, and a
/// single-partition `__consumer_offsets` also led by `LEADER`, which is what
/// makes this broker every group's coordinator.
fn coordinator_image() -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    for (name, topic_id) in [(TOPIC, 1_u128), (OFFSETS_TOPIC, 2_u128)] {
        image.apply(&MetadataRecord::V1Topic(
            crate::test_support::single_partition_topic(name, uuid::Uuid::from_u128(topic_id)),
        ));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: name.into(),
            partition: 0,
            leader: LEADER,
            replicas: vec![LEADER],
            isr: vec![LEADER],
            leader_epoch: LeaderEpoch(1),
            ..Default::default()
        }));
    }
    image
}

/// A coordinator with an in-memory offsets log, so a commit costs no disk.
fn coordinator(metadata: Arc<dyn MetadataSource>) -> Arc<GroupCoordinator> {
    Arc::new(GroupCoordinator::new(
        NextGenConfig::default(),
        ShareGroupConfig::default(),
        Arc::new(ImageMetadataProvider {
            controller: metadata,
        }),
        Arc::new(InMemoryOffsetsLog::default()),
        StreamsGroupConfig::default(),
    ))
}

/// A poller wired to a local-only cluster: every partition it samples is led
/// by this broker, so no probe leaves the process.
fn poller(
    coordinator: Arc<GroupCoordinator>,
    metadata: Arc<dyn MetadataSource>,
    partitions: Arc<PartitionRegistry>,
    metrics: BrokerMetrics,
) -> LagPoller {
    LagPoller {
        node_id: LEADER,
        coordinator,
        metadata,
        partitions,
        inter_broker: Arc::new(InterBrokerClient::new(None, None)),
        listener_protocol: ListenerProtocol::Plaintext,
        listener_name: "PLAINTEXT".into(),
        inter_broker_server_name: "broker.internal".into(),
        period: LAG_POLL_INTERVAL,
        metrics,
        shutdown: CancellationToken::new(),
    }
}

/// Commit `offset` for `TOPIC`-0 through the group's actor, the way
/// `OffsetCommit` does.
async fn commit_offset(handle: &GroupActorHandle, offset: i64) {
    let (reply, done) = oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::UpdateCommitted {
            entries: vec![(
                (TOPIC.to_string(), 0),
                OffsetEntry {
                    offset: Offset(offset),
                    leader_epoch: 1,
                    metadata: String::new(),
                    commit_timestamp_ms: 1_700_000_000_000,
                    expire_timestamp_ms: None,
                    topic_id: None,
                },
            )],
            reply,
        })
        .await
        .expect("the group actor takes the commit");
    done.await.expect("the group actor acknowledges the commit");
}

/// A partition led by this broker whose high watermark is `high_watermark`.
async fn partition_at_high_watermark(dir: &Path, high_watermark: i64) -> Arc<PartitionRegistry> {
    let partitions = Arc::new(PartitionRegistry::new());
    let partition = led_partition(dir, TOPIC, 0);
    partition.replica_state.lock().await.hw = Offset(high_watermark);
    partitions.insert(TOPIC.into(), PartitionIndex(0), partition);
    partitions
}

/// The published lag of `group_id` on `TOPIC`-0.
fn published_group_lag(metrics: &BrokerMetrics, group_id: &str) -> Option<i64> {
    metrics
        .consumer_group_lag
        .get(&ConsumerGroupLabel {
            group_id: group_id.into(),
            topic: TOPIC.into(),
            partition: 0,
        })
        .map(|gauge| gauge.get())
}

/// The acceptance case, over both group protocols: a commit behind the high
/// watermark reports exactly the difference. A classic group and a KIP-848
/// group reach `committed_offsets` by different actor kinds, so both are
/// driven here rather than one standing in for the other.
#[tokio::test]
async fn group_lag_is_the_high_watermark_minus_the_committed_offset() {
    let cases = [
        ("classic-billing", GroupKindTag::Classic, 40_i64, 7_i64, 33),
        ("next-gen-search", GroupKindTag::Consumer, 40, 7, 33),
        ("classic-caught-up", GroupKindTag::Classic, 40, 40, 0),
        ("next-gen-caught-up", GroupKindTag::Consumer, 40, 40, 0),
        // A commit can lead the watermark while the leader catches up. Lag is
        // a backlog, so it floors at zero rather than going negative.
        ("classic-ahead", GroupKindTag::Classic, 40, 45, 0),
    ];
    for (group_id, kind, high_watermark, committed, expected_lag) in cases {
        let dir = tempfile::tempdir().expect("tempdir");
        let metadata: Arc<dyn MetadataSource> = lag_metadata(coordinator_image());
        let partitions = partition_at_high_watermark(dir.path(), high_watermark).await;
        let coordinator = coordinator(Arc::clone(&metadata));
        let handle = coordinator.get_or_create_group(group_id, kind);
        commit_offset(&handle, committed).await;
        let metrics = BrokerMetrics::new();
        let poller = poller(
            Arc::clone(&coordinator),
            metadata,
            partitions,
            metrics.clone(),
        );

        poller.sample().await;

        check!(
            published_group_lag(&metrics, group_id) == Some(expected_lag),
            "{group_id}: high watermark {high_watermark}, committed {committed}"
        );
    }
}

/// Deleting a group takes its lag series with it, without waiting for the
/// sampler's next pass — the pass would never name the group again.
#[tokio::test]
async fn deleting_a_group_releases_its_lag_series() {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata: Arc<dyn MetadataSource> = lag_metadata(coordinator_image());
    let partitions = partition_at_high_watermark(dir.path(), 40).await;
    let coordinator = coordinator(Arc::clone(&metadata));
    let metrics = BrokerMetrics::new();
    coordinator.set_metrics(metrics.clone());
    let handle = coordinator.get_or_create_classic("billing");
    commit_offset(&handle, 7).await;
    let poller = poller(
        Arc::clone(&coordinator),
        metadata,
        partitions,
        metrics.clone(),
    );
    poller.sample().await;
    assert!(published_group_lag(&metrics, "billing") == Some(33));

    coordinator
        .delete_group("billing")
        .await
        .expect("an empty group deletes");

    check!(published_group_lag(&metrics, "billing") == None);
}

/// A group another broker coordinates is not this broker's to report, so no
/// series is created for it even though its actor is still in the registry.
#[tokio::test]
async fn a_group_this_broker_does_not_coordinate_gets_no_series() {
    let dir = tempfile::tempdir().expect("tempdir");
    // The offsets partition that hosts every group moves to another broker.
    let image = image_with_remote_leader(OFFSETS_TOPIC);
    let metadata: Arc<dyn MetadataSource> = lag_metadata(image);
    let partitions = partition_at_high_watermark(dir.path(), 40).await;
    let (poller, metrics) = committed_classic_poller(metadata, partitions, 7).await;

    poller.sample().await;

    check!(published_group_lag(&metrics, "billing") == None);
}

/// A remote leader's reply is read by topic id, because Fetch v13 stopped
/// sending the name and a v13 row carries an empty one. A row that reports an
/// error keeps its own partition out of the result without costing the healthy
/// rows on the same broker theirs.
#[test]
fn a_probe_reply_is_read_by_topic_id_and_drops_only_the_failed_rows() {
    let topic_id = WireUuid(*uuid::Uuid::from_u128(1).as_bytes());
    let response = FetchResponse {
        error_code: codes::NONE,
        responses: vec![FetchableTopicResponse {
            // Fetch v13 and later leave the name out of the response.
            topic: String::new(),
            topic_id,
            partitions: vec![
                PartitionData {
                    partition_index: 0,
                    error_code: codes::NONE,
                    high_watermark: 40,
                    ..Default::default()
                },
                PartitionData {
                    partition_index: 1,
                    error_code: codes::NOT_LEADER_OR_FOLLOWER,
                    high_watermark: -1,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    };

    let watermarks = probed_high_watermarks(&response, &HashMap::from([(topic_id, TOPIC)]));

    check!(watermarks == HashMap::from([((TOPIC.to_string(), 0), 40)]));
}

/// A group that has committed nothing has no backlog to report, so an
/// uncommitted partition creates no series rather than one reading the whole
/// log.
#[tokio::test]
async fn an_uncommitted_partition_gets_no_series() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (poller, metrics) = classic_group_poller(dir.path(), 40, -1).await;

    poller.sample().await;

    check!(published_group_lag(&metrics, "billing") == None);
}

/// The probe asks past the end of every partition it names, at the leader
/// epoch the image holds, and carries the topic ids its reply has to be read
/// back through. Nothing about the request may pull records: it is a metadata
/// read that happens to travel as a `Fetch`.
#[test]
fn the_probe_asks_for_watermarks_and_no_records() {
    let topic_id = WireUuid(*uuid::Uuid::from_u128(1).as_bytes());
    let wanted = [(TOPIC.to_string(), 0)];
    let expected = FetchRequest {
        replica_id: -1,
        max_wait_ms: 0,
        min_bytes: 0,
        max_bytes: 0,
        isolation_level: 0,
        topics: vec![FetchTopic {
            topic: TOPIC.into(),
            topic_id,
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: 1,
                fetch_offset: i64::MAX,
                partition_max_bytes: 0,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let (request, names) =
        probe_request(&coordinator_image(), &wanted).expect("the image names the partition");

    check!(request == expected);
    check!(names == HashMap::from([(topic_id, TOPIC)]));
}

/// A partition the image has stopped naming leaves the probe nothing to ask,
/// so no connection is opened for it at all.
#[test]
fn a_probe_for_a_partition_the_image_dropped_has_nothing_to_ask() {
    let wanted = [("ghost".to_string(), 0)];

    check!(probe_request(&coordinator_image(), &wanted).is_none());
}

/// A watermark this broker cannot read costs that partition its series for the
/// pass and nothing else. Here the image leads the partition from a broker it
/// carries no address for, which is the shape a probe failure takes.
#[tokio::test]
async fn a_watermark_this_broker_cannot_read_yields_no_series() {
    // The topic's partition moves to a broker the image holds no record for,
    // so the probe cannot even be addressed.
    let image = image_with_remote_leader(TOPIC);
    let metadata: Arc<dyn MetadataSource> = lag_metadata(image);
    let (poller, metrics) =
        committed_classic_poller(metadata, Arc::new(PartitionRegistry::new()), 7).await;

    poller.sample().await;

    check!(published_group_lag(&metrics, "billing") == None);
}

/// A group actor that never answers must not hold the pass open: every other
/// group's series, and both replica families, wait behind it. The wait is
/// bounded, and the group reads as having committed nothing this pass.
#[tokio::test(start_paused = true)]
async fn a_group_actor_that_never_answers_yields_no_offsets() {
    let (actor, _mailbox) = tokio::sync::mpsc::channel(1);

    let offsets = committed_offsets(&actor).await;

    check!(offsets.is_empty());
}

/// The poller's own loop: a tick publishes what it can justify, and shutdown
/// releases it rather than freezing the last numbers on the scrape endpoint,
/// where an alert would keep reading them as current.
#[tokio::test(start_paused = true)]
async fn the_poller_publishes_on_a_tick_and_releases_everything_on_shutdown() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut poller, metrics) = classic_group_poller(dir.path(), 40, 7).await;
    let shutdown = CancellationToken::new();
    poller.shutdown = shutdown.clone();

    poller.spawn();
    // A `tokio::time::interval` fires its first tick immediately, so one pass
    // runs before the clock has to move at all.
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    assert!(published_group_lag(&metrics, "billing") == Some(33));

    shutdown.cancel();
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;

    check!(published_group_lag(&metrics, "billing") == None);
}
