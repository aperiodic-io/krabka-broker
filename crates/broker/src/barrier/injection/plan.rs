//! The pure decisions one injection makes before it writes a marker.
//!
//! Freezing the target set, grouping the partitions that still need a marker by
//! their leader, and computing the wait before the next attempt are all
//! functions over a metadata image and a list of targets. None of them touches
//! a log or a transport, so a unit test drives each one on its own.

use {
    crate::barrier::{persistence::TopicTarget, state::TargetPartition},
    krabka_metadata::{MetadataImage, NodeId},
    krabka_units::{Time, convert::TimeExt as _},
    std::collections::BTreeMap,
    tracing::warn,
};

/// Freeze the target set of one injection from the metadata image.
///
/// The frozen set names each topic of the group and the partition count the
/// image reported at this instant. A topic that the image does not hold has no
/// partition to mark, so the set leaves it out. An edit to the group's topics
/// and a partition-count change both apply from the next epoch.
pub(crate) fn freeze_targets(topics: &[String], image: &MetadataImage) -> Vec<TopicTarget> {
    let mut out = Vec::with_capacity(topics.len());
    for topic in topics {
        if let Some(record) = image.topic(topic) {
            out.push(TopicTarget {
                topic: topic.clone(),
                partition_count: record.partitions,
            });
        } else {
            warn!(topic, "barrier target topic is not in the metadata image");
        }
    }
    out
}

/// Group the partitions that carry no marker yet by their current leader.
///
/// `leader_of` returns `None` for a partition that the metadata image does not
/// hold. Those partitions group under `None`, and the fan-out leaves them for
/// the next attempt.
pub(crate) fn group_by_leader<F>(
    pending: &[TargetPartition],
    leader_of: F,
) -> BTreeMap<Option<NodeId>, Vec<TargetPartition>>
where
    F: Fn(&TargetPartition) -> Option<NodeId>,
{
    let mut out: BTreeMap<Option<NodeId>, Vec<TargetPartition>> = BTreeMap::new();
    for target in pending {
        out.entry(leader_of(target))
            .or_default()
            .push(target.clone());
    }
    out
}

/// The wait before retry number `attempt`, counted from zero.
///
/// The wait doubles per attempt and stops at `max`.
pub(crate) fn backoff_for(attempt: u32, base: Time, max: Time) -> Time {
    let base_ms = base.millis_i64().max(0);
    let max_ms = max.millis_i64().max(0);
    let factor = 1_i64 << attempt.min(20);
    Time::from_millis(base_ms.saturating_mul(factor).min(max_ms))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::barrier::{injection::test_support::at, test_support::topic_records},
        assert2::{assert, check},
        krabka_units::millis,
        uuid::Uuid,
    };

    #[test]
    fn a_frozen_target_set_takes_the_partition_count_of_the_image() {
        let image = MetadataImage::from_records(
            Uuid::nil(),
            &[
                topic_records("orders", 3, NodeId(1)),
                topic_records("payments", 1, NodeId(1)),
            ]
            .concat(),
        );
        let topics = vec![
            "orders".to_owned(),
            "payments".to_owned(),
            "absent".to_owned(),
        ];
        let expected = vec![
            TopicTarget {
                topic: "orders".to_owned(),
                partition_count: 3,
            },
            TopicTarget {
                topic: "payments".to_owned(),
                partition_count: 1,
            },
        ];
        assert!(freeze_targets(&topics, &image) == expected);
    }

    #[test]
    fn the_fan_out_plan_groups_every_partition_by_its_leader() {
        let pending = vec![at("orders", 0), at("orders", 1), at("payments", 0)];
        let plan = group_by_leader(&pending, |target| match target.partition.get() {
            0 if target.topic == "orders" => Some(NodeId(1)),
            0 => Some(NodeId(2)),
            _ => None,
        });
        let expected = maplit::btreemap! {
        None => vec![at("orders", 1)],
        Some(NodeId(1)) => vec![at("orders", 0)],
        Some(NodeId(2)) => vec![at("payments", 0)]};
        assert!(plan == expected);
    }

    #[test]
    fn the_backoff_doubles_and_stops_at_the_maximum() {
        let cases: &[(u32, i64)] = &[
            (0, 100),
            (1, 200),
            (2, 400),
            (3, 800),
            (4, 1000),
            (30, 1000),
        ];
        for (attempt, expected) in cases {
            check!(
                backoff_for(*attempt, millis(100), millis(1000)).millis_i64() == *expected,
                "attempt {attempt}"
            );
        }
    }

    #[test]
    fn a_zero_maximum_backoff_waits_no_time() {
        assert!(backoff_for(3, millis(100), Time::ZERO).millis_i64() == 0);
    }
}
