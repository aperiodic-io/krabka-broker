//! Fixtures shared by the `OffsetDelete` submodule tests: a request builder
//! and the fully-specified expected response rows.
//!
//! The response-shaping tests and the row-building tests assert against the
//! same `OffsetDeleteResponse` shapes, so the builders live here once instead
//! of being copied into each sibling test module.

use krabka_protocol::owned::{
    offset_delete_request::{
        OffsetDeleteRequest, OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
    },
    offset_delete_response::{OffsetDeleteResponsePartition, OffsetDeleteResponseTopic},
};

/// Fully-specified expected partition row (no struct-update syntax).
pub(super) fn expected_row(partition_index: i32, error_code: i16) -> OffsetDeleteResponsePartition {
    tagged_wire!(OffsetDeleteResponsePartition {
        partition_index,
        error_code,
    })
}

/// Fully-specified expected topic row (no struct-update syntax).
pub(super) fn expected_topic(
    name: &str,
    partitions: Vec<OffsetDeleteResponsePartition>,
) -> OffsetDeleteResponseTopic {
    tagged_wire!(OffsetDeleteResponseTopic {
        name: name.to_string(),
        partitions,
    })
}

/// A requested topic: its name and the partition indexes it names.
#[derive(Clone, Copy)]
pub(super) struct RequestTopic<'a> {
    pub(super) name: &'a str,
    pub(super) partitions: &'a [i32],
}

/// Builds a decoded `OffsetDeleteRequest` for group `g` from the requested
/// topics.
pub(super) fn req_with_topics(topics: &[RequestTopic<'_>]) -> OffsetDeleteRequest {
    OffsetDeleteRequest {
        group_id: "g".to_string(),
        topics: topics
            .iter()
            .map(|topic| OffsetDeleteRequestTopic {
                name: topic.name.to_string(),
                partitions: topic
                    .partitions
                    .iter()
                    .map(|p| OffsetDeleteRequestPartition {
                        partition_index: *p,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}
