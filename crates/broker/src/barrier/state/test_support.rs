//! The fixtures that the barrier-state unit tests share.
//!
//! A target set, a group record, a cut record, and an injection-start record
//! are each built by more than one of the test modules under this module, so
//! the builders live in one file instead of once per module.

pub(super) use crate::barrier::test_support::at;
use {
    crate::barrier::persistence::{
        CutStatus, CutValue, GroupValue, InjectionStartValue, PartitionOffset, TopicOffsets,
        TopicTarget,
    },
    krabka_ids::PartitionIndex,
    krabka_log::Offset,
    krabka_units::millis,
};

pub(super) fn target(topic: &str, count: i32) -> TopicTarget {
    TopicTarget {
        topic: topic.to_owned(),
        partition_count: count,
    }
}

pub(super) fn group_value(last_epoch: i64) -> GroupValue {
    GroupValue {
        topics: vec!["orders".to_owned()],
        interval: Some(millis(60_000)),
        retained_cuts: 4,
        last_epoch,
    }
}

pub(super) fn cut_value(status: CutStatus) -> CutValue {
    CutValue {
        triggered_at: 10,
        completed_at: 20,
        status,
        topics: vec![TopicOffsets {
            topic: "orders".to_owned(),
            partitions: vec![PartitionOffset {
                partition: PartitionIndex(0),
                offset: Offset(7),
            }],
        }],
        missing: Vec::new(),
    }
}

pub(super) fn start_value(coordinator_epoch: i32) -> InjectionStartValue {
    InjectionStartValue {
        coordinator_epoch,
        triggered_at: 10,
        targets: vec![target("orders", 1)],
    }
}
