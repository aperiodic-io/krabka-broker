//! Fetch fixtures with explicit limits and caller-owned session and replica fields.

use krabka_ids::{Offset, PartitionIndex};
use krabka_protocol::{
    owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic, ForgottenTopic},
    primitives::uuid::Uuid,
};

pub use super::share::{FetchByteLimit, RequestWaitMillis};

#[derive(Clone, Copy)]
pub struct FetchLimits {
    pub wait: RequestWaitMillis,
    pub minimum: FetchByteLimit,
    pub maximum: FetchByteLimit,
}

impl Default for FetchLimits {
    fn default() -> Self {
        let request = FetchRequest::default();
        Self {
            wait: RequestWaitMillis(request.max_wait_ms),
            minimum: FetchByteLimit(request.min_bytes),
            maximum: FetchByteLimit(request.max_bytes),
        }
    }
}

impl FetchLimits {
    /// Wait for at least one byte, with the protocol's ordinary overall ceiling.
    pub fn wait_for_data(wait: RequestWaitMillis) -> Self {
        Self {
            wait,
            minimum: FetchByteLimit(1),
            ..Self::default()
        }
    }

    pub fn wait_for_data_with_maximum(wait: RequestWaitMillis, maximum: FetchByteLimit) -> Self {
        Self {
            maximum,
            ..Self::wait_for_data(wait)
        }
    }

    /// The ordinary one-mebibyte response ceiling used by these fixtures.
    pub fn one_mebibyte(wait: RequestWaitMillis) -> Self {
        Self::wait_for_data_with_maximum(wait, FetchByteLimit(1 << 20))
    }

    /// Poll immediately even when the session has no changed partition rows.
    pub fn immediate() -> Self {
        Self {
            wait: RequestWaitMillis(0),
            minimum: FetchByteLimit(0),
            ..Self::default()
        }
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct FetchPartitionSetup {
    pub partition: PartitionIndex,
    pub offset: Offset,
    #[default(FetchByteLimit(1 << 20))]
    pub maximum: FetchByteLimit,
}

pub fn fetch_partition(setup: FetchPartitionSetup) -> FetchPartition {
    FetchPartition {
        partition: setup.partition.0,
        fetch_offset: setup.offset.0,
        partition_max_bytes: setup.maximum.0,
        ..Default::default()
    }
}

#[derive(krabka_macros::FieldDefaults)]
pub struct SinglePartitionFetchSetup {
    #[default("orders".into())]
    pub topic: String,
    #[default(Uuid::ZERO)]
    pub topic_id: Uuid,
    #[default(fetch_partition(FetchPartitionSetup::default()))]
    pub partition: FetchPartition,
    #[default(FetchLimits::one_mebibyte(RequestWaitMillis(500)))]
    pub limits: FetchLimits,
}

pub fn single_partition_fetch(setup: SinglePartitionFetchSetup) -> FetchRequest {
    fetch_request_for(
        vec![fetch_topic_row(
            setup.topic,
            setup.topic_id,
            vec![setup.partition],
        )],
        setup.limits,
    )
}

/// Legacy requests identify the topic by name and leave the wire UUID zero.
pub fn named_topic_fetch(topic: impl Into<String>, limits: FetchLimits) -> FetchRequest {
    single_partition_fetch(SinglePartitionFetchSetup {
        topic: topic.into(),
        limits,
        ..Default::default()
    })
}

pub fn fetch_topic_row(
    topic: impl Into<String>,
    topic_id: Uuid,
    partitions: Vec<FetchPartition>,
) -> FetchTopic {
    FetchTopic {
        topic: topic.into(),
        topic_id,
        partitions,
        ..Default::default()
    }
}

pub fn fetch_request_for(topics: Vec<FetchTopic>, limits: FetchLimits) -> FetchRequest {
    FetchRequest {
        max_wait_ms: limits.wait.0,
        min_bytes: limits.minimum.0,
        max_bytes: limits.maximum.0,
        topics,
        ..Default::default()
    }
}

/// Preserve the protocol's ordinary limits for session-only requests.
pub fn default_fetch_limits() -> FetchLimits {
    FetchLimits::default()
}

#[derive(Clone, Copy, Default)]
pub struct FetchSessionId(pub i32);

#[derive(Clone, Copy)]
pub struct FetchSessionEpoch(pub i32);

#[derive(krabka_macros::FieldDefaults)]
pub struct FetchSessionSetup {
    pub id: FetchSessionId,
    #[default(FetchSessionEpoch(FetchRequest::default().session_epoch))]
    pub epoch: FetchSessionEpoch,
    pub forgotten: Vec<ForgottenTopic>,
}

impl FetchSessionSetup {
    pub fn incremental(id: FetchSessionId) -> Self {
        Self::at_epoch(id, FetchSessionEpoch(1))
    }

    /// Explicit epochs include deliberately malformed and stale fixture values.
    pub fn at_epoch(id: FetchSessionId, epoch: FetchSessionEpoch) -> Self {
        Self {
            id,
            epoch,
            ..Self::default()
        }
    }
}

pub fn empty_session_fetch(limits: FetchLimits, session: FetchSessionSetup) -> FetchRequest {
    session_fetch_request(fetch_request_for(Vec::new(), limits), session)
}

pub fn session_fetch_request(request: FetchRequest, setup: FetchSessionSetup) -> FetchRequest {
    FetchRequest {
        session_id: setup.id.0,
        session_epoch: setup.epoch.0,
        forgotten_topics_data: setup.forgotten,
        ..request
    }
}

/// Check a successful partition's v2 record count against the caller's expected count.
pub fn check_record_count(
    partition: &krabka_protocol::owned::fetch_response::PartitionData,
    expected: usize,
) {
    assert2::assert!(partition.error_code == 0);
    assert2::assert!(crate::support::records::record_count(partition.records.as_ref()) == expected);
}
