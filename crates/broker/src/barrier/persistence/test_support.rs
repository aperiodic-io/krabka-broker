//! The sample records that the codec unit tests share.
//!
//! A group value, an injection-start value, and a cut value are each built by
//! more than one of the test modules under this module, so the builders live in
//! one file instead of once per module.

use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_units::{Time, convert::TimeExt};

use super::{
    CutStatus, CutValue, GroupValue, InjectionStartValue, MissingPartition, PartitionOffset,
    TopicOffsets, TopicTarget,
};

pub(super) fn sample_group() -> GroupValue {
    GroupValue {
        topics: vec!["orders".to_owned(), "payments".to_owned()],
        interval: Some(Time::from_millis(60_000)),
        retained_cuts: 32,
        last_epoch: 7,
    }
}

pub(super) fn sample_injection_start() -> InjectionStartValue {
    InjectionStartValue {
        coordinator_epoch: 4,
        triggered_at: 1_724_500_000_000,
        targets: vec![
            TopicTarget {
                topic: "orders".to_owned(),
                partition_count: 3,
            },
            TopicTarget {
                topic: "payments".to_owned(),
                partition_count: 1,
            },
        ],
    }
}

pub(super) fn sample_cut() -> CutValue {
    CutValue {
        triggered_at: 1_724_500_000_000,
        completed_at: 1_724_500_000_042,
        status: CutStatus::Partial,
        topics: vec![
            orders_offsets(),
            TopicOffsets {
                topic: "payments".to_owned(),
                partitions: vec![PartitionOffset {
                    partition: PartitionIndex(0),
                    offset: Offset(9),
                }],
            },
        ],
        missing: vec![MissingPartition {
            topic: "orders".to_owned(),
            partition: PartitionIndex(2),
        }],
    }
}

pub(super) fn orders_offsets() -> TopicOffsets {
    TopicOffsets {
        topic: "orders".to_owned(),
        partitions: vec![
            PartitionOffset {
                partition: PartitionIndex(0),
                offset: Offset(1024),
            },
            PartitionOffset {
                partition: PartitionIndex(1),
                offset: Offset(2048),
            },
        ],
    }
}
