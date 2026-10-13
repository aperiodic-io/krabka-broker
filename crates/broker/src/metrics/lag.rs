//! The two "how far behind" gauges — per-follower replica lag and
//! per-(group, topic, partition) consumer-group lag — the index that makes
//! their series releasable, and the publish entry points their samplers call.
//!
//! Both families are sampled rather than incremented: a pass computes the
//! whole set of label sets it can justify and hands it over at once, so
//! publishing is also what releases the series the pass no longer justifies.
//! That is the same rule `share_partition::backlog_poller` follows for the
//! share-group backlog gauge.
//!
//! The index exists because releasing a series is not always the sampler's
//! job. A group is deleted, a coordinator loses an offsets partition, a topic
//! is deleted, a partition is reassigned away: each of those knows part of a
//! lag label set and none of them knows the rest.
//! `prometheus_client::metrics::family::Family` cannot be enumerated, so
//! without a record of the live label sets those callers have no way to reach
//! the series they must release, and the highest-cardinality family in the
//! broker would keep them until the next pass — or, once the sampler stops
//! naming the group at all, forever.

use {
    super::{BrokerMetrics, ConsumerGroupLabel, PartitionLabel, ReplicaLagLabel},
    dashmap::DashSet,
    std::{collections::HashMap, sync::Arc},
};

/// The label sets the two lag families currently carry.
///
/// It is a cache of what is in the families, kept in step by being written
/// only where the families are: `BrokerMetrics::publish_replica_lag`,
/// `BrokerMetrics::publish_consumer_group_lag`, and the eviction entry
/// points below. A [`BrokerMetrics`] clone shares one index, as it shares one
/// registry.
///
/// The type is public because [`BrokerMetrics`] exposes its field, but it has
/// no public method: outside the crate it is an opaque handle.
#[derive(Clone, Default)]
pub struct LagSeriesIndex {
    replica: Arc<DashSet<ReplicaLagLabel>>,
    group: Arc<DashSet<ConsumerGroupLabel>>,
}

impl BrokerMetrics {
    /// Replace the replica-lag family with `samples`, and set the max rollup
    /// to the largest value in it.
    ///
    /// A label set the previous pass published and this one does not is
    /// released. That is what takes a follower's series away when the replica
    /// set drops it, and every follower's series away when this broker stops
    /// leading the partition.
    pub(crate) fn publish_replica_lag(&self, samples: &HashMap<ReplicaLagLabel, i64>) {
        self.lag_series.replica.retain(|label| {
            let live = samples.contains_key(label);
            if !live {
                self.replica_lag.remove(label);
            }
            live
        });
        for (label, lag) in samples {
            self.replica_lag.get_or_create(label).set(*lag);
            self.lag_series.replica.insert(label.clone());
        }
        self.replica_lag_max
            .set(samples.values().copied().max().unwrap_or(0));
    }

    /// Replace the consumer-group-lag family with `samples`.
    ///
    /// The counterpart of [`Self::publish_replica_lag`]. A tuple the previous
    /// pass published and this one does not is released, which covers an
    /// offset the group stopped committing and a partition whose high
    /// watermark this broker can no longer read.
    pub(crate) fn publish_consumer_group_lag(&self, samples: &HashMap<ConsumerGroupLabel, i64>) {
        self.lag_series.group.retain(|label| {
            let live = samples.contains_key(label);
            if !live {
                self.consumer_group_lag.remove(label);
            }
            live
        });
        for (label, lag) in samples {
            self.consumer_group_lag.get_or_create(label).set(*lag);
            self.lag_series.group.insert(label.clone());
        }
    }

    /// Drop the replica-lag series for one partition.
    ///
    /// Called from [`BrokerMetrics::evict_partition_series`], whose rule is
    /// "this broker left the partition's replica set". That rule justifies the
    /// replica-lag family exactly: a partition this broker no longer hosts has
    /// no follower for it to report on, and waiting for the sampler's next
    /// pass would hold the series for up to
    /// [`crate::lag::LAG_POLL_INTERVAL`].
    ///
    /// It does not justify the consumer-group family, which is left alone
    /// here. A group's lag on a partition depends on this broker coordinating
    /// the *group*, not on it hosting the *partition*: the sampler reads a
    /// remote leader's high watermark with a probe precisely so that a group
    /// can lag on a partition this broker has no replica of. Releasing the
    /// series on a reassignment would blank a still-justified gauge until the
    /// next pass, and would do so only for the partitions this broker happened
    /// to host. The one partition event that does end a group's series —
    /// a topic delete — arrives through [`Self::evict_topic_lag_series`],
    /// which is not filtered by hosting.
    pub(super) fn evict_partition_lag_series(&self, partition: &PartitionLabel) {
        self.retain_replica_lag(|label| {
            label.topic.as_str() != &*partition.topic || label.partition != partition.partition
        });
    }

    /// Drop every lag series either family carries for one topic.
    ///
    /// Called from [`BrokerMetrics::evict_topic_series`], which fires for
    /// every topic the image drops rather than only for the ones this broker
    /// hosted. That is what makes it the right place for the consumer-group
    /// family: a deleted topic has no high watermark left for any group to lag
    /// behind, wherever its partitions lived. For the replica-lag family it
    /// overlaps [`Self::evict_partition_lag_series`], and covers the partition
    /// index the image had already stopped naming when the delete arrived.
    pub(super) fn evict_topic_lag_series(&self, topic: &str) {
        self.retain_replica_lag(|label| label.topic != topic);
        self.retain_group_lag(|label| label.topic != topic);
    }

    /// Drop every consumer-group-lag series for `group_id`.
    ///
    /// Group removal is not a metadata-image event, so this is the entry point
    /// the coordinator calls itself: on `DeleteGroups`, and when losing an
    /// offsets partition takes the group's actor away. Without it a deleted
    /// group's series would survive until the sampler's next pass, and a group
    /// that moved to another coordinator would leave its series here for the
    /// life of the process.
    pub(crate) fn evict_group_series(&self, group_id: &str) {
        self.retain_group_lag(|label| label.group_id != group_id);
    }

    /// Keep the replica-lag series `keep` accepts, and remove the rest from
    /// both the family and the index.
    ///
    /// The max rollup follows, because eviction can take the very follower
    /// that was supplying it. Only the sampler's pass would otherwise reset
    /// it, and until the next one a scrape would report a maximum that no
    /// remaining `replica_lag_records` series carries.
    fn retain_replica_lag(&self, keep: impl Fn(&ReplicaLagLabel) -> bool) {
        self.lag_series.replica.retain(|label| {
            let live = keep(label);
            if !live {
                self.replica_lag.remove(label);
            }
            live
        });
        let max = self
            .lag_series
            .replica
            .iter()
            .filter_map(|label| self.replica_lag.get(label.key()).map(|gauge| gauge.get()))
            .max()
            .unwrap_or(0);
        self.replica_lag_max.set(max);
    }

    /// Keep the consumer-group-lag series `keep` accepts, and remove the rest
    /// from both the family and the index.
    fn retain_group_lag(&self, keep: impl Fn(&ConsumerGroupLabel) -> bool) {
        self.lag_series.group.retain(|label| {
            let live = keep(label);
            if !live {
                self.consumer_group_lag.remove(label);
            }
            live
        });
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    fn replica(topic: &str, partition: i32, node: u64) -> ReplicaLagLabel {
        ReplicaLagLabel {
            topic: topic.to_string(),
            partition,
            replica: node,
        }
    }

    fn group(group_id: &str, topic: &str, partition: i32) -> ConsumerGroupLabel {
        ConsumerGroupLabel {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
        }
    }

    /// The value the family carries for `label`, or `None` when it has no such
    /// series.
    fn replica_lag_of(metrics: &BrokerMetrics, label: &ReplicaLagLabel) -> Option<i64> {
        metrics.replica_lag.get(label).map(|gauge| gauge.get())
    }

    fn group_lag_of(metrics: &BrokerMetrics, label: &ConsumerGroupLabel) -> Option<i64> {
        metrics
            .consumer_group_lag
            .get(label)
            .map(|gauge| gauge.get())
    }

    /// Publishing is a replacement, not an accumulation: a pass that no longer
    /// names a follower takes that follower's series with it, and the max
    /// rollup follows the values that remain.
    #[test]
    fn publishing_replica_lag_releases_what_the_pass_no_longer_names() {
        let metrics = BrokerMetrics::new();
        let (slow, fast) = (replica("orders", 0, 2), replica("orders", 0, 3));

        metrics.publish_replica_lag(&HashMap::from([(slow.clone(), 40), (fast.clone(), 5)]));
        check!(replica_lag_of(&metrics, &slow) == Some(40));
        check!(replica_lag_of(&metrics, &fast) == Some(5));
        check!(metrics.replica_lag_max.get() == 40);

        metrics.publish_replica_lag(&HashMap::from([(fast.clone(), 7)]));
        check!(replica_lag_of(&metrics, &slow) == None);
        check!(replica_lag_of(&metrics, &fast) == Some(7));
        check!(metrics.replica_lag_max.get() == 7);

        metrics.publish_replica_lag(&HashMap::new());
        check!(replica_lag_of(&metrics, &fast) == None);
        check!(metrics.replica_lag_max.get() == 0);
    }

    /// A group's series belong to that group: deleting one group leaves the
    /// other group's series on the same partition alone.
    #[test]
    fn evicting_a_group_leaves_another_groups_series_on_the_same_partition() {
        let metrics = BrokerMetrics::new();
        let (mine, theirs) = (group("billing", "orders", 0), group("search", "orders", 0));
        metrics
            .publish_consumer_group_lag(&HashMap::from([(mine.clone(), 12), (theirs.clone(), 3)]));
        assert!(group_lag_of(&metrics, &mine) == Some(12));

        metrics.evict_group_series("billing");

        check!(group_lag_of(&metrics, &mine) == None);
        check!(group_lag_of(&metrics, &theirs) == Some(3));
    }

    /// Losing a partition releases its follower series and leaves the sibling
    /// partition's alone. A group's lag on the same partition survives: this
    /// broker still coordinates the group, and the sampler reads the new
    /// leader's high watermark over the wire.
    #[test]
    fn evicting_a_partition_releases_its_replica_lag_but_not_a_groups() {
        let metrics = BrokerMetrics::new();
        let (gone, kept) = (replica("orders", 0, 2), replica("orders", 1, 2));
        metrics.publish_replica_lag(&HashMap::from([(gone.clone(), 9), (kept.clone(), 4)]));
        let (reassigned_group, kept_group) =
            (group("billing", "orders", 0), group("billing", "orders", 1));
        metrics.publish_consumer_group_lag(&HashMap::from([
            (reassigned_group.clone(), 9),
            (kept_group.clone(), 4),
        ]));

        metrics.evict_partition_series(&PartitionLabel {
            topic: "orders".into(),
            partition: 0,
        });

        check!(replica_lag_of(&metrics, &gone) == None);
        check!(replica_lag_of(&metrics, &kept) == Some(4));
        check!(group_lag_of(&metrics, &reassigned_group) == Some(9));
        check!(group_lag_of(&metrics, &kept_group) == Some(4));
    }

    /// A topic delete takes every lag series the topic carries, whichever
    /// partition or group it belongs to.
    #[test]
    fn evicting_a_topic_releases_every_lag_series_it_carries() {
        let metrics = BrokerMetrics::new();
        let deleted = replica("orders", 3, 2);
        let survivor = replica("payments", 0, 2);
        metrics.publish_replica_lag(&HashMap::from([
            (deleted.clone(), 11),
            (survivor.clone(), 1),
        ]));
        let deleted_group = group("billing", "orders", 3);
        let surviving_group = group("billing", "payments", 0);
        metrics.publish_consumer_group_lag(&HashMap::from([
            (deleted_group.clone(), 11),
            (surviving_group.clone(), 1),
        ]));

        metrics.evict_topic_series("orders");

        check!(replica_lag_of(&metrics, &deleted) == None);
        check!(replica_lag_of(&metrics, &survivor) == Some(1));
        check!(group_lag_of(&metrics, &deleted_group) == None);
        check!(group_lag_of(&metrics, &surviving_group) == Some(1));
    }

    /// The bundle prints opaquely. It is one registry and some seventy metric
    /// handles, and a `GroupCoordinator` holds one so that it can release a
    /// deleted group's series — a dump of every handle would swamp any log
    /// line that prints the coordinator.
    #[test]
    fn the_metric_bundle_prints_opaquely() {
        let printed = format!("{:?}", BrokerMetrics::new());

        check!(printed == "BrokerMetrics { .. }");
    }

    /// Eviction takes the index with the family, so a later pass that names
    /// the released label set again publishes it afresh rather than treating
    /// it as still live.
    #[test]
    fn a_released_series_is_republished_when_a_later_pass_names_it_again() {
        let metrics = BrokerMetrics::new();
        let label = group("billing", "orders", 0);
        metrics.publish_consumer_group_lag(&HashMap::from([(label.clone(), 12)]));
        metrics.evict_group_series("billing");

        metrics.publish_consumer_group_lag(&HashMap::from([(label.clone(), 20)]));

        check!(group_lag_of(&metrics, &label) == Some(20));
    }
}
