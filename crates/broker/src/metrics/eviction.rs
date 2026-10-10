//! Release of the per-partition and per-topic series that a reassigned
//! partition or a deleted topic leaves behind.
//!
//! `Family::get_or_create` materialises a series on first produce, fetch,
//! replication or compaction, and `prometheus-client` never releases one on
//! its own. Without this module the `/metrics` body grows for the life of the
//! process on any cluster that reassigns partitions or deletes topics: nine
//! per-partition series and a dozen per-topic series stay in the body for a
//! partition this broker no longer hosts.
//!
//! The current metadata image is the bound. A partition's series live while
//! this broker sits in that partition's replica set, and a topic's series live
//! while that topic exists. [`MetricSeriesIndex`] records what the data path
//! actually materialised, including rejected client-supplied names, so a pass
//! can release labels no image ever named. The pass also runs periodically to
//! catch a write racing just behind an image update. The index contains live
//! series rather than tombstones, so it has the registry's current bound.
//!
//! Kafka's `BrokerTopicMetrics` marks a rejected request under the topic name
//! the client supplied. Krabka keeps that accounting: handlers create and
//! increment the series before reconciliation. Unlike Kafka's process-lifetime
//! sensor, an unjustified label is then collected on the next pass (within 30
//! seconds), which bounds hostile names without moving rejected traffic onto a
//! synthetic label or silently dropping it at the handler.
//!
//! Five further families are keyed by a partition without taking a
//! [`PartitionLabel`], and each is left to a narrower owner that releases it
//! sooner than this diff could. `share_group_backlog` is pruned by
//! `share_partition::backlog_poller` on its own tick. The four diskless WAL
//! gauges -- `diskless_wal_durable_watermark`,
//! `diskless_wal_index_projection_lag`, `diskless_wal_trim_frontier` and
//! `diskless_wal_voter_lag` -- are keyed by topic id and released by
//! `wal::quorum::registry::WalShardRegistry`, whose `replace_placements`
//! reconfigures every live shard engine against the newest image and whose
//! `remove` clears a shard the supervisor tore down. Routing those through
//! [`BrokerMetrics::evict_partition_series`] would be wrong as well as
//! redundant: a shard's voters are selected from the registered brokers rather
//! than from the partition's replica set, so this broker can still vote on --
//! and still report lag for -- a shard whose replicas no longer name it.
//!
//! The two lag families of `metrics::lag` are keyed that way too, and they
//! join in here rather than being left to a narrower owner. Their samplers
//! already release what a pass stops naming, but a reassignment or a topic
//! delete must not wait for the next pass, and a group that leaves this
//! coordinator is never named by a pass again. Each entry point below reaches
//! the families its own rule justifies: the per-partition one covers replica
//! lag, because "this broker left the replica set" is exactly what ends a
//! follower's series, while consumer-group lag follows the group rather than
//! the host and so is reached only by the per-topic entry point and by
//! `evict_group_series`, which gives group removal -- an event no image
//! records -- its own one call to make.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use dashmap::DashSet;
use krabka_metadata::{MetadataImage, NodeId};
use prometheus_client::metrics::{counter::Counter, family::Family};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{BrokerMetrics, PartitionLabel, QuotaType, SchemaRejectionLabel, TopicLabel};
use crate::schema_validation::RejectReason;

impl BrokerMetrics {
    pub(crate) fn track_topic_series(&self, label: &TopicLabel) {
        self.metric_series.topics.insert(label.clone());
    }

    /// Count nonzero partition activity and retain its series for later eviction.
    pub(crate) fn count_partition(
        &self,
        family: &Family<PartitionLabel, Counter>,
        topic: &Arc<str>,
        partition: i32,
        amount: u64,
    ) {
        if amount == 0 {
            return;
        }
        let label = PartitionLabel {
            topic: Arc::clone(topic),
            partition,
        };
        family.get_or_create(&label).inc_by(amount);
        self.track_partition_series(&label);
    }

    /// Adds `by` to the `topic` series of `family`, and tracks that series
    /// for release when the topic goes away.
    pub(crate) fn count_topic(
        &self,
        family: &Family<TopicLabel, Counter>,
        topic: Arc<str>,
        by: u64,
    ) {
        let label = TopicLabel { topic };
        family.get_or_create(&label).inc_by(by);
        self.track_topic_series(&label);
    }

    pub(crate) fn track_topic_name(&self, topic: &str) {
        self.track_topic_series(&TopicLabel {
            topic: Arc::from(topic),
        });
    }

    pub(crate) fn track_partition_series(&self, label: &PartitionLabel) {
        self.metric_series.partitions.insert(label.clone());
    }

    /// Releases the per-entity throttle series one expired quota bucket
    /// published.
    ///
    /// A bucket materialises its series the first time that entity is
    /// throttled, and `prometheus-client` never releases one on its own. The
    /// quota-expiry sweep calls this so an inactive tenant's label set leaves
    /// the `/metrics` body with its bucket, rather than staying in the body
    /// for the life of the process. The label set is the one
    /// `observe_quota_throttle_for_entity` builds, so a bucket that was never
    /// throttled has no series and the removal is a no-op.
    pub fn evict_quota_entity_series(
        &self,
        quota_type: QuotaType,
        user: Option<String>,
        client_id: Option<String>,
    ) {
        self.quota_entity_throttle_seconds_total
            .remove(&crate::metrics::QuotaEntityLabel {
                quota_type,
                user,
                client_id,
            });
    }

    /// Drop every series that any per-partition family carries for `label`.
    ///
    /// This is the one entry point for partition-series eviction: a caller
    /// that learns a partition left this broker calls it and never has to know
    /// which families take a [`PartitionLabel`].
    pub(crate) fn evict_partition_series(&self, label: &PartitionLabel) {
        for family in [
            &self.partition_bytes_in,
            &self.partition_bytes_out,
            &self.replication_bytes_in,
            &self.replication_bytes_out,
            &self.partition_cpu_micros,
            &self.log_compactions_total,
        ] {
            family.remove(label);
        }
        for family in [
            &self.partition_disk_bytes,
            &self.delivery_watermark,
            &self.delivery_pending_records,
        ] {
            family.remove(label);
        }
        self.evict_partition_lag_series(label);
        self.metric_series.partitions.remove(label);
    }

    /// Drop every series that any per-topic family carries for `topic`.
    ///
    /// The companion of [`BrokerMetrics::evict_partition_series`], and the one
    /// entry point for topic-series eviction. It covers
    /// `schema_validation_rejections` too, whose label set is the topic paired
    /// with each of [`RejectReason::LABELS`].
    pub(crate) fn evict_topic_series(&self, topic: &str) {
        let label = TopicLabel {
            topic: Arc::from(topic),
        };
        for family in [
            &self.topic_bytes_in,
            &self.topic_bytes_out,
            &self.topic_messages_in,
            &self.topic_produce_requests,
            &self.topic_fetch_requests,
            &self.topic_failed_produce_requests,
            &self.topic_failed_fetch_requests,
            &self.produce_message_conversions,
            &self.fetch_message_conversions,
            &self.barrier_markers_written_total,
            &self.topic_freeze_rejections,
            &self.remote_copy_bytes_total,
            &self.remote_fetch_bytes_total,
            &self.remote_copy_requests_total,
            &self.remote_fetch_requests_total,
            &self.remote_delete_requests_total,
            &self.remote_copy_errors_total,
            &self.remote_fetch_errors_total,
            &self.remote_delete_errors_total,
        ] {
            family.remove(&label);
        }
        for family in [
            &self.remote_copy_lag_bytes,
            &self.remote_copy_lag_segments,
            &self.remote_delete_lag_bytes,
            &self.remote_delete_lag_segments,
        ] {
            family.remove(&label);
        }
        for reason in RejectReason::LABELS {
            self.schema_validation_rejections
                .remove(&SchemaRejectionLabel {
                    topic: topic.to_string(),
                    reason: reason.to_string(),
                });
        }
        self.evict_topic_lag_series(topic);
        self.metric_series.topics.remove(&label);
        let partitions: Vec<_> = self
            .metric_series
            .partitions
            .iter()
            .filter(|partition| partition.topic.as_ref() == topic)
            .map(|partition| partition.clone())
            .collect();
        for partition in partitions {
            self.evict_partition_series(&partition);
        }
    }
}

/// Live topic and partition label sets created by broker data paths.
///
/// Public only because [`BrokerMetrics`] exposes it for the compile-time
/// metrics contract. Its sets stay private so every mutation remains paired
/// with a metric-family mutation.
#[derive(Clone, Default)]
pub struct MetricSeriesIndex {
    topics: Arc<DashSet<TopicLabel>>,
    partitions: Arc<DashSet<PartitionLabel>>,
}

/// Reconciles the live metric series against the newest metadata image.
///
/// It compares the labels the data path actually created with the labels the
/// current image permits. This catches invented client labels as well as
/// ordinary metadata removal.
pub(crate) struct MetricSeriesEvictor {
    node_id: NodeId,
    metrics: BrokerMetrics,
    topics: HashMap<String, Uuid>,
    initialized: bool,
}

impl MetricSeriesEvictor {
    pub(crate) fn new(node_id: NodeId, metrics: BrokerMetrics) -> Self {
        Self {
            node_id,
            metrics,
            topics: HashMap::new(),
            initialized: false,
        }
    }

    /// Evict every materialised label set `image` does not justify.
    pub(crate) fn apply(&mut self, image: &MetadataImage) {
        let topics: HashMap<String, Uuid> = image
            .topics()
            .map(|topic| (topic.name.clone(), topic.topic_id))
            .collect();
        if !self.initialized {
            self.topics = topics;
            self.initialized = true;
            return;
        }

        let hosted: HashSet<PartitionLabel> = image
            .all_partitions()
            .filter(|partition| partition.replicas.contains(&self.node_id))
            .map(|partition| PartitionLabel {
                topic: Arc::from(partition.topic.as_str()),
                partition: partition.partition,
            })
            .collect();
        let invalid_partitions: Vec<_> = self
            .metrics
            .metric_series
            .partitions
            .iter()
            .filter(|label| !hosted.contains(label.key()))
            .map(|label| label.clone())
            .collect();
        for label in invalid_partitions {
            tracing::debug!(
                topic = %label.topic,
                partition = label.partition,
                "evicting partition metric series",
            );
            self.metrics.evict_partition_series(&label);
        }

        let replaced: Vec<_> = self
            .topics
            .iter()
            .filter(|(name, id)| topics.get(*name).is_some_and(|live| live != *id))
            .map(|(name, _)| name.clone())
            .collect();
        for topic in replaced {
            self.metrics.evict_topic_series(&topic);
        }
        let invalid_topics: Vec<_> = self
            .metrics
            .metric_series
            .topics
            .iter()
            .filter(|label| !topics.contains_key(label.topic.as_ref()))
            .map(|label| Arc::clone(&label.topic))
            .collect();
        for topic in invalid_topics {
            tracing::debug!(%topic, "evicting topic metric series");
            self.metrics.evict_topic_series(&topic);
        }
        self.topics = topics;
    }
}

/// Run a [`MetricSeriesEvictor`] over every published image until `shutdown`.
///
/// Image changes release ordinary removals immediately. The periodic pass
/// catches a data-path write that races behind the removing image.
pub(crate) fn spawn_metric_series_evictor(
    mut images: watch::Receiver<Arc<MetadataImage>>,
    node_id: NodeId,
    metrics: BrokerMetrics,
    shutdown: CancellationToken,
) {
    let mut evictor = MetricSeriesEvictor::new(node_id, metrics);
    evictor.apply(&images.borrow().clone());
    tokio::spawn(async move {
        let period = Duration::from_secs(30);
        let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = images.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    evictor.apply(&images.borrow_and_update().clone());
                }
                _ = interval.tick() => evictor.apply(&images.borrow().clone()),
                () = shutdown.cancelled() => return,
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_metadata::{
        DeleteTopicRecord, LeaderEpoch, MetadataRecord, PartitionRecord, TopicRecord,
    };

    use super::*;
    use crate::{
        metadata_source::MetadataSource as _,
        metrics::{ConsumerGroupLabel, ReplicaLagLabel},
        test_support::FakeMetadataSource,
    };

    const TOPIC: &str = "orders";
    const THIS_BROKER: NodeId = NodeId(1);
    const OTHER_BROKER: NodeId = NodeId(2);

    /// The `/metrics` body the scrape endpoint would serve.
    fn scrape(metrics: &BrokerMetrics) -> String {
        let mut body = String::new();
        let registry = metrics
            .registry
            .try_lock()
            .expect("no scrape holds the registry");
        prometheus_client::encoding::text::encode(&mut body, &registry).expect("encode metrics");
        body
    }

    /// The `{topic=,partition=}` label pair as the `OpenMetrics` body renders
    /// it.
    fn partition_pair(topic: &str, partition: i32) -> String {
        format!("topic=\"{topic}\",partition=\"{partition}\"")
    }

    /// Records for `TOPIC`, one partition per entry of `partitions` and
    /// indexed from zero, each with that entry as its replica set.
    fn topic_records(partitions: &[&[NodeId]]) -> Vec<MetadataRecord> {
        topic_records_with_id(Uuid::from_u128(1), partitions)
    }

    /// [`topic_records`] under an explicit topic id, for the delete-and-
    /// recreate case where the name stays put and only the id changes.
    fn topic_records_with_id(topic_id: Uuid, partitions: &[&[NodeId]]) -> Vec<MetadataRecord> {
        let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
            name: TOPIC.into(),
            topic_id,
            partitions: i32::try_from(partitions.len()).expect("partition count fits"),
            replication_factor: 1,
        })];
        for (index, replicas) in partitions.iter().enumerate() {
            records.push(MetadataRecord::V1Partition(PartitionRecord {
                topic: TOPIC.into(),
                partition: i32::try_from(index).expect("partition index fits"),
                leader: replicas[0],
                replicas: replicas.to_vec(),
                isr: replicas.to_vec(),
                leader_epoch: LeaderEpoch(4),
                ..Default::default()
            }));
        }
        records
    }

    /// Touch every family that carries a [`PartitionLabel`], the way the
    /// produce, fetch, replication, compaction, disk-scan and delivery paths
    /// each do.
    fn create_partition_series(metrics: &BrokerMetrics, topic: &Arc<str>, partition: i32) {
        metrics.record_partition_produce(topic, partition, 512);
        metrics.record_partition_fetch(topic, partition, 256);
        metrics.record_replication_in(topic, partition, 128);
        metrics.record_replication_out(topic, partition, 64);
        metrics.record_partition_cpu_micros(topic, partition, 32);
        metrics.record_compaction(topic, partition);
        metrics.record_delivery_watermark(topic, partition, 7, 3);
        metrics
            .partition_disk_bytes
            .get_or_create(&PartitionLabel {
                topic: Arc::clone(topic),
                partition,
            })
            .set(4_096);
        metrics.track_partition_series(&PartitionLabel {
            topic: Arc::clone(topic),
            partition,
        });
    }

    /// Touch every family that carries a [`TopicLabel`], plus the
    /// topic-and-reason schema-rejection family.
    fn create_topic_series(metrics: &BrokerMetrics, topic: &Arc<str>) {
        metrics.record_produce(topic, 512);
        metrics.record_produce_messages(topic, 4);
        metrics.record_fetch(topic, 256);
        metrics.record_failed_produce(topic);
        metrics.record_failed_fetch(topic);
        metrics.record_produce_message_conversion(topic);
        metrics.record_fetch_message_conversion(topic);
        metrics.record_topic_freeze_rejection(topic);
        metrics
            .barrier_markers_written_total
            .get_or_create(&TopicLabel {
                topic: Arc::clone(topic),
            })
            .inc();
        metrics.track_topic_series(&TopicLabel {
            topic: Arc::clone(topic),
        });
        for reason in RejectReason::LABELS {
            metrics.record_schema_validation_rejection(topic, reason);
        }
    }

    /// Poll `condition` against fresh scrapes until it holds. The evictor runs
    /// in a spawned task, so the test waits on the body rather than on a
    /// sleep.
    async fn scrape_until(metrics: &BrokerMetrics, condition: impl Fn(&str) -> bool) -> String {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let body = scrape(metrics);
                if condition(&body) {
                    return body;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the scrape body never reached the expected state"))
    }

    /// Start an evictor for `THIS_BROKER` over a fake serving `records`.
    fn evicting_source(
        metrics: &BrokerMetrics,
        records: &[MetadataRecord],
        shutdown: &CancellationToken,
    ) -> FakeMetadataSource {
        let source = FakeMetadataSource::builder().records(records).build();
        spawn_metric_series_evictor(
            source.watch_image(),
            THIS_BROKER,
            metrics.clone(),
            shutdown.clone(),
        );
        source
    }

    /// The acceptance case: a reassignment that drops this broker from the
    /// replica set takes the partition's series out of the scrape body, and
    /// leaves the topic's own series alone because the topic still exists.
    #[tokio::test]
    async fn a_metadata_change_removing_this_replica_evicts_the_partition_series() {
        let metrics = BrokerMetrics::new();
        let shutdown = CancellationToken::new();
        let source = evicting_source(&metrics, &topic_records(&[&[THIS_BROKER]]), &shutdown);

        create_partition_series(&metrics, &Arc::from(TOPIC), 0);
        create_topic_series(&metrics, &Arc::from(TOPIC));
        let pair = partition_pair(TOPIC, 0);
        let topic_label = format!("topic=\"{TOPIC}\"}}");
        let before = scrape(&metrics);
        assert!(before.contains(&pair));
        assert!(before.contains(&topic_label));

        // The reassignment: partition 0 still exists, but its replica set now
        // names only the other broker.
        source.set_records(&topic_records(&[&[OTHER_BROKER]]));

        let after = scrape_until(&metrics, |body| !body.contains(&pair)).await;
        check!(!after.contains(&pair));
        check!(after.contains(&topic_label));
        shutdown.cancel();
    }

    /// A topic delete takes the per-topic series with it, and the partition
    /// series of that topic go with the partitions the delete removed.
    fn delete_topic(source: &FakeMetadataSource, mut live: Vec<MetadataRecord>) {
        live.push(MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: TOPIC.into(),
        }));
        source.set_records(&live);
    }

    #[tokio::test]
    async fn deleting_a_topic_evicts_its_topic_and_partition_series() {
        let metrics = BrokerMetrics::new();
        let shutdown = CancellationToken::new();
        let live = topic_records(&[&[THIS_BROKER]]);
        let source = evicting_source(&metrics, &live, &shutdown);

        create_partition_series(&metrics, &Arc::from(TOPIC), 0);
        create_topic_series(&metrics, &Arc::from(TOPIC));
        let before = scrape(&metrics);
        assert!(before.contains(&partition_pair(TOPIC, 0)));
        assert!(before.contains(&format!("topic=\"{TOPIC}\"")));

        delete_topic(&source, live);

        let after = scrape_until(&metrics, |body| !body.contains(TOPIC)).await;
        check!(!after.contains(TOPIC));
        shutdown.cancel();
    }

    /// A partition this broker still hosts keeps its series when a sibling
    /// partition is reassigned away, so eviction is per label set and not per
    /// family.
    #[tokio::test]
    async fn a_partition_this_broker_still_hosts_keeps_its_series() {
        let metrics = BrokerMetrics::new();
        let shutdown = CancellationToken::new();
        let source = evicting_source(
            &metrics,
            &topic_records(&[&[THIS_BROKER], &[THIS_BROKER]]),
            &shutdown,
        );

        create_partition_series(&metrics, &Arc::from(TOPIC), 0);
        create_partition_series(&metrics, &Arc::from(TOPIC), 1);

        source.set_records(&topic_records(&[&[THIS_BROKER], &[OTHER_BROKER]]));

        let evicted = partition_pair(TOPIC, 1);
        let after = scrape_until(&metrics, |body| !body.contains(&evicted)).await;
        check!(after.contains(&partition_pair(TOPIC, 0)));
        check!(!after.contains(&evicted));
        shutdown.cancel();
    }

    /// A misrouted produce or fetch is accounted for under the partition the
    /// client named, so a partition this broker does not replicate still gets
    /// series here. Deleting the topic has to take those with it, or a
    /// deleted topic leaves behind exactly the partitions the eviction rule
    /// never tracked.
    #[tokio::test]
    async fn deleting_a_topic_evicts_partition_series_this_broker_never_hosted() {
        let metrics = BrokerMetrics::new();
        let shutdown = CancellationToken::new();
        // Partition 0 lives here; partition 1 is replicated only on the other
        // broker, and this broker never hosts it.
        let live = topic_records(&[&[THIS_BROKER], &[OTHER_BROKER]]);
        let source = evicting_source(&metrics, &live, &shutdown);

        create_partition_series(&metrics, &Arc::from(TOPIC), 0);
        create_partition_series(&metrics, &Arc::from(TOPIC), 1);
        let unhosted = partition_pair(TOPIC, 1);
        assert!(scrape(&metrics).contains(&unhosted));

        delete_topic(&source, live);

        let after = scrape_until(&metrics, |body| !body.contains(TOPIC)).await;
        check!(!after.contains(&partition_pair(TOPIC, 0)));
        check!(!after.contains(&unhosted));
        shutdown.cancel();
    }

    /// A `watch` channel publishes only the newest image, so a delete and a
    /// recreate under the same name can arrive as one image in which the name
    /// never disappeared. The topic id is what makes that a removal, and
    /// without it the new topic would inherit the old one's counters.
    #[test]
    fn a_topic_recreated_under_the_same_name_does_not_inherit_the_old_series() {
        let metrics = BrokerMetrics::new();
        let mut evictor = MetricSeriesEvictor::new(THIS_BROKER, metrics.clone());

        let first = Uuid::from_u128(1);
        evictor.apply(&MetadataImage::from_records(
            Uuid::nil(),
            &topic_records_with_id(first, &[&[THIS_BROKER]]),
        ));
        create_partition_series(&metrics, &Arc::from(TOPIC), 0);
        create_topic_series(&metrics, &Arc::from(TOPIC));
        assert!(scrape(&metrics).contains(&partition_pair(TOPIC, 0)));

        // Same name, same partition, different topic id: a different topic.
        let second = Uuid::from_u128(2);
        evictor.apply(&MetadataImage::from_records(
            Uuid::nil(),
            &topic_records_with_id(second, &[&[THIS_BROKER]]),
        ));

        let after = scrape(&metrics);
        check!(!after.contains(&partition_pair(TOPIC, 0)));
        check!(!after.contains(TOPIC));

        // The new incarnation is now tracked in its own right: series it
        // creates survive an image that repeats it, and go when it goes.
        create_topic_series(&metrics, &Arc::from(TOPIC));
        evictor.apply(&MetadataImage::from_records(
            Uuid::nil(),
            &topic_records_with_id(second, &[&[THIS_BROKER]]),
        ));
        check!(scrape(&metrics).contains(TOPIC));
    }

    /// Restored state can publish series before the metadata observer catches
    /// up, so the first image seeds the baseline without evicting them.
    #[test]
    fn the_first_image_preserves_restored_series() {
        let metrics = BrokerMetrics::new();
        create_partition_series(&metrics, &Arc::from(TOPIC), 0);
        create_topic_series(&metrics, &Arc::from(TOPIC));

        let mut evictor = MetricSeriesEvictor::new(THIS_BROKER, metrics.clone());
        // An image that names neither the topic nor the partition.
        evictor.apply(&MetadataImage::new(uuid::Uuid::nil()));

        check!(scrape(&metrics).contains(TOPIC));
    }

    #[test]
    fn invented_topic_and_partition_labels_are_released() {
        let metrics = BrokerMetrics::new();
        let mut evictor = MetricSeriesEvictor::new(THIS_BROKER, metrics.clone());
        let image = MetadataImage::from_records(Uuid::nil(), &topic_records(&[&[THIS_BROKER]]));

        create_topic_series(&metrics, &Arc::from("invented"));
        create_partition_series(&metrics, &Arc::from(TOPIC), 99);
        assert!(scrape(&metrics).contains("invented"));
        assert!(scrape(&metrics).contains(&partition_pair(TOPIC, 99)));

        evictor.apply(&image);
        evictor.apply(&image);

        let after = scrape(&metrics);
        check!(!after.contains("invented"));
        check!(!after.contains(&partition_pair(TOPIC, 99)));
    }

    #[test]
    fn a_series_recreated_after_removal_is_released_by_the_next_pass() {
        let metrics = BrokerMetrics::new();
        let mut evictor = MetricSeriesEvictor::new(THIS_BROKER, metrics.clone());
        let hosted = MetadataImage::from_records(Uuid::nil(), &topic_records(&[&[THIS_BROKER]]));
        let unhosted = MetadataImage::from_records(Uuid::nil(), &topic_records(&[&[OTHER_BROKER]]));

        evictor.apply(&hosted);
        create_partition_series(&metrics, &Arc::from(TOPIC), 0);
        evictor.apply(&unhosted);
        create_partition_series(&metrics, &Arc::from(TOPIC), 0);
        assert!(scrape(&metrics).contains(&partition_pair(TOPIC, 0)));

        evictor.apply(&unhosted);

        check!(!scrape(&metrics).contains(&partition_pair(TOPIC, 0)));
        check!(metrics.metric_series.partitions.is_empty());
    }

    /// Eviction is exhaustive over the families each entry point owns: after
    /// one call the scrape body carries no sample for that label set at all.
    ///
    /// The two `metrics::lag` families are in here too, and they are what
    /// makes "the families each entry point owns" a narrower claim than "every
    /// family keyed by this label set". Replica lag is owned by both entry
    /// points, because a broker that left the replica set has no follower to
    /// report on. Consumer-group lag is owned only by the per-topic one: a
    /// group's lag on a partition follows this broker coordinating the
    /// *group*, not hosting the *partition*, so it is put on a partition the
    /// per-partition call names and asserted to survive it.
    #[test]
    fn the_eviction_entry_points_clear_every_family_they_own() {
        let metrics = BrokerMetrics::new();
        create_partition_series(&metrics, &Arc::from(TOPIC), 0);
        create_topic_series(&metrics, &Arc::from(TOPIC));
        metrics.publish_replica_lag(&HashMap::from([(
            ReplicaLagLabel {
                topic: TOPIC.into(),
                partition: 0,
                replica: OTHER_BROKER.0,
            },
            12,
        )]));
        metrics.publish_consumer_group_lag(&HashMap::from([(
            ConsumerGroupLabel {
                group_id: "billing".into(),
                topic: TOPIC.into(),
                partition: 0,
            },
            7,
        )]));
        // A registered family keeps its `# HELP` line whether or not it
        // carries a sample, so every check below names a label set rather
        // than a family, and each one is asserted present before it is
        // asserted gone.
        let follower = format!(
            "{},replica=\"{}\"",
            partition_pair(TOPIC, 0),
            OTHER_BROKER.0
        );
        let group = format!("group_id=\"billing\",{}", partition_pair(TOPIC, 0));
        let before = scrape(&metrics);
        assert!(before.contains(TOPIC));
        assert!(before.contains(&follower));
        assert!(before.contains(&group));
        metrics.evict_partition_series(&PartitionLabel {
            topic: TOPIC.into(),
            partition: 0,
        });
        let after_partition = scrape(&metrics);
        check!(!after_partition.contains(&follower));
        check!(after_partition.contains(&group));

        metrics.evict_topic_series(TOPIC);
        check!(!scrape(&metrics).contains(TOPIC));
    }
}
