//! The KIP-447 `UNSTABLE_OFFSET_COMMIT` partition row.
//!
//! A `read_committed` EOS consumer sets `require_stable = true` on
//! `OffsetFetch` precisely so that it is told to wait rather than handed the
//! offset a still-open transaction is about to replace. Answering with the
//! older stable offset instead makes the consumer rewind and reprocess records
//! the transaction already committed.
//!
//! Kafka's row carries the invalid-offset sentinels and an empty — not null —
//! metadata string alongside error code 88, and the two response shapes
//! (pre-KIP-516 `Partition`, KIP-516 `Partitions`) must agree on it, so both
//! are built here.

use {
    crate::codes,
    krabka_protocol::owned::offset_fetch_response::{
        OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
    },
};

/// The unstable row in the pre-KIP-516 (v0–v7) response shape.
pub(super) fn legacy_row(partition_index: i32) -> OffsetFetchResponsePartition {
    super::committed::missing_legacy_row(partition_index, codes::UNSTABLE_OFFSET_COMMIT)
}

/// The unstable row in the KIP-516 (v8+) `groups[]` response shape.
pub(super) fn group_row(partition_index: i32) -> OffsetFetchResponsePartitions {
    super::committed::missing_group_row(partition_index, codes::UNSTABLE_OFFSET_COMMIT)
}
