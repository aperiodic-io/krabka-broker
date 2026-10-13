//! The frozen target set of an injection and the cut record built from it.
//!
//! An injection freezes a list of `TopicTarget`, expands it into one
//! [`TargetPartition`] per partition, and folds the offsets the markers took
//! into a `CutValue`. Both steps are pure functions over that frozen set, so
//! they sit apart from the group state the injection is driven from.

use {
    crate::barrier::persistence::{
        CutStatus, CutValue, MissingPartition, PartitionOffset, TopicOffsets, TopicTarget,
    },
    krabka_ids::PartitionIndex,
    krabka_log::Offset,
    krabka_verified::{
        BarrierCutClassification, BarrierTargetCountDecision, barrier_cut_classification,
        barrier_target_count_decision,
    },
    std::collections::BTreeMap,
};

/// One partition of a frozen target set.
///
/// The type is ordered, so a placement map holds the partitions of a cut in a
/// stable order and the cut record is byte-identical across two runs of the
/// same injection.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TargetPartition {
    pub(crate) topic: String,
    pub(crate) partition: PartitionIndex,
}

/// Expand a frozen target set into the partitions the injection marks.
///
/// A topic with a partition count of zero or below contributes nothing.
pub(crate) fn expand_targets(targets: &[TopicTarget]) -> Vec<TargetPartition> {
    let mut out = Vec::new();
    let mut total = 0;
    for target in targets {
        let count = match barrier_target_count_decision(total, target.partition_count) {
            BarrierTargetCountDecision::Expand { next } => {
                total = next;
                target.partition_count
            }
            BarrierTargetCountDecision::Malformed | BarrierTargetCountDecision::Overflow => 0,
        };
        for partition in 0..count {
            out.push(TargetPartition {
                topic: target.topic.clone(),
                partition: PartitionIndex(partition),
            });
        }
    }
    out
}

/// Build the cut record of one injection.
///
/// `placed` holds the offset that each marker took. A target partition that is
/// absent from `placed` carries no marker, so the cut names it in `missing`
/// and its status is [`CutStatus::Partial`]. The epoch is consumed either way.
pub(crate) fn build_cut(
    triggered_at: i64,
    completed_at: i64,
    targets: &[TopicTarget],
    placed: &BTreeMap<TargetPartition, Offset>,
) -> CutValue {
    let mut topics = Vec::with_capacity(targets.len());
    let mut missing = Vec::new();
    let mut malformed_target = false;

    for target in targets {
        let mut partitions = Vec::new();
        let count = match barrier_target_count_decision(0, target.partition_count) {
            BarrierTargetCountDecision::Expand { .. } => target.partition_count,
            BarrierTargetCountDecision::Malformed | BarrierTargetCountDecision::Overflow => {
                malformed_target = true;
                0
            }
        };
        for partition in 0..count {
            let key = TargetPartition {
                topic: target.topic.clone(),
                partition: PartitionIndex(partition),
            };
            match placed.get(&key) {
                Some(offset) => partitions.push(PartitionOffset {
                    partition: key.partition,
                    offset: *offset,
                }),
                None => missing.push(MissingPartition {
                    topic: key.topic,
                    partition: key.partition,
                }),
            }
        }
        topics.push(TopicOffsets {
            topic: target.topic.clone(),
            partitions,
        });
    }

    let status = match barrier_cut_classification(malformed_target || !missing.is_empty()) {
        BarrierCutClassification::Complete => CutStatus::Complete,
        BarrierCutClassification::Partial => CutStatus::Partial,
    };
    CutValue {
        triggered_at,
        completed_at,
        status,
        topics,
        missing,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{build_cut, expand_targets},
        crate::barrier::{
            persistence::{CutStatus, CutValue, MissingPartition, PartitionOffset, TopicOffsets},
            state::test_support::{at, target},
        },
        assert2::{assert, check},
        krabka_ids::PartitionIndex,
        krabka_log::Offset,
        std::collections::BTreeMap,
    };

    #[test]
    fn a_target_set_expands_to_one_entry_per_partition() {
        let expanded = expand_targets(&[target("orders", 3), target("payments", 1)]);
        assert!(
            expanded
                == vec![
                    at("orders", 0),
                    at("orders", 1),
                    at("orders", 2),
                    at("payments", 0),
                ]
        );
    }

    #[test]
    fn a_topic_with_no_partition_expands_to_nothing() {
        for count in [0, -1] {
            check!(
                expand_targets(&[target("orders", count)]).is_empty(),
                "{count}"
            );
        }
    }

    #[test]
    fn a_malformed_target_cannot_produce_a_complete_cut() {
        for count in [0, -1] {
            let cut = build_cut(1, 2, &[target("orders", count)], &BTreeMap::new());
            check!(cut.status == CutStatus::Partial, "{count}");
        }
    }

    #[test]
    fn a_cut_that_reached_every_partition_is_complete() {
        let targets = vec![target("orders", 2), target("payments", 1)];
        let placed = maplit::btreemap! {
        at("orders", 0) => Offset(10),
        at("orders", 1) => Offset(11),
        at("payments", 0) => Offset(5)};
        let expected = CutValue {
            triggered_at: 100,
            completed_at: 140,
            status: CutStatus::Complete,
            topics: vec![
                TopicOffsets {
                    topic: "orders".to_owned(),
                    partitions: vec![
                        PartitionOffset {
                            partition: PartitionIndex(0),
                            offset: Offset(10),
                        },
                        PartitionOffset {
                            partition: PartitionIndex(1),
                            offset: Offset(11),
                        },
                    ],
                },
                TopicOffsets {
                    topic: "payments".to_owned(),
                    partitions: vec![PartitionOffset {
                        partition: PartitionIndex(0),
                        offset: Offset(5),
                    }],
                },
            ],
            missing: Vec::new(),
        };
        assert!(build_cut(100, 140, &targets, &placed) == expected);
    }

    #[test]
    fn a_cut_that_missed_a_partition_is_partial_and_names_it() {
        let targets = vec![target("orders", 2)];
        let placed = maplit::btreemap! {at("orders", 1) => Offset(11)};
        let expected = CutValue {
            triggered_at: 100,
            completed_at: 140,
            status: CutStatus::Partial,
            topics: vec![TopicOffsets {
                topic: "orders".to_owned(),
                partitions: vec![PartitionOffset {
                    partition: PartitionIndex(1),
                    offset: Offset(11),
                }],
            }],
            missing: vec![MissingPartition {
                topic: "orders".to_owned(),
                partition: PartitionIndex(0),
            }],
        };
        assert!(build_cut(100, 140, &targets, &placed) == expected);
    }

    #[test]
    fn a_cut_that_reached_nothing_names_every_target() {
        let targets = vec![target("orders", 2)];
        let cut = build_cut(1, 2, &targets, &BTreeMap::new());
        let expected = CutValue {
            triggered_at: 1,
            completed_at: 2,
            status: CutStatus::Partial,
            topics: vec![TopicOffsets {
                topic: "orders".to_owned(),
                partitions: Vec::new(),
            }],
            missing: vec![
                MissingPartition {
                    topic: "orders".to_owned(),
                    partition: PartitionIndex(0),
                },
                MissingPartition {
                    topic: "orders".to_owned(),
                    partition: PartitionIndex(1),
                },
            ],
        };
        assert!(cut == expected);
    }

    #[test]
    fn a_cut_ignores_an_offset_that_no_target_names() {
        let targets = vec![target("orders", 1)];
        let placed =
            maplit::btreemap! {at("orders", 0) => Offset(4), at("dropped", 0) => Offset(9)};
        let cut = build_cut(1, 2, &targets, &placed);
        assert!(cut.status == CutStatus::Complete);
        assert!(cut.topics.len() == 1);
        assert!(cut.topics[0].topic == "orders");
    }
}
