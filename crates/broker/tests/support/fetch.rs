//! Fetch fixtures with explicit limits and caller-owned session and replica fields.

pub use super::share::{FetchByteLimit, RequestWaitMillis};
use {
    krabka_ids::{Offset, PartitionIndex},
    krabka_protocol::{
        owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic, ForgottenTopic},
        primitives::uuid::Uuid,
    },
};

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

#[bon::builder]
pub fn fetch_partition(
    #[builder(into, default)] partition: PartitionIndex,
    #[builder(into, default)] offset: Offset,
    #[builder(into, default = FetchByteLimit (1 << 20))] maximum: FetchByteLimit,
) -> FetchPartition {
    let setup = FetchPartitionSetup {
        partition,
        offset,
        maximum,
    };
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
    #[default(fetch_partition().call())]
    pub partition: FetchPartition,
    #[default(FetchLimits::one_mebibyte(RequestWaitMillis(500)))]
    pub limits: FetchLimits,
}

#[bon::builder]
pub fn single_partition_fetch(
    #[builder(default = "orders" . into ())] topic: String,
    #[builder(default = Uuid :: ZERO)] topic_id: Uuid,
    #[builder(default = fetch_partition().call())] partition: FetchPartition,
    #[builder(default = FetchLimits :: one_mebibyte (RequestWaitMillis (500)))] limits: FetchLimits,
) -> FetchRequest {
    let setup = SinglePartitionFetchSetup {
        topic,
        topic_id,
        partition,
        limits,
    };
    fetch_request_for(
        vec![fetch_topic_row(setup.topic, setup.topic_id, vec![
            setup.partition,
        ])],
        setup.limits,
    )
}

/// Legacy requests identify the topic by name and leave the wire UUID zero.
pub fn named_topic_fetch(topic: impl Into<String>, limits: FetchLimits) -> FetchRequest {
    single_partition_fetch()
        .topic(topic.into())
        .limits(limits)
        .call()
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
pub fn default_fetch_limits() -> FetchLimits { FetchLimits::default() }

#[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
pub struct FetchSessionId(pub i32);

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
pub struct FetchSessionEpoch(pub i32);

#[derive(krabka_macros::FieldDefaults)]
pub struct FetchSessionSetup {
    pub id: FetchSessionId,
    #[default(FetchSessionEpoch(FetchRequest::default().session_epoch))]
    pub epoch: FetchSessionEpoch,
    pub forgotten: Vec<ForgottenTopic>,
}

impl FetchSessionSetup {
    pub fn incremental(id: FetchSessionId) -> Self { Self::at_epoch(id, FetchSessionEpoch(1)) }

    /// Explicit epochs include deliberately malformed and stale fixture values.
    pub fn at_epoch(id: FetchSessionId, epoch: FetchSessionEpoch) -> Self {
        Self {
            id,
            epoch,
            ..Self::default()
        }
    }
}

#[bon::builder]
pub fn build_fetch_session_request(
    request: Option<FetchRequest>,
    #[builder(default = FetchLimits :: default())] limits: FetchLimits,
    #[builder(into, default)] id: FetchSessionId,
    #[builder(into, default = FetchSessionEpoch (FetchRequest :: default () . session_epoch))]
    epoch: FetchSessionEpoch,
    #[builder(default)] forgotten: Vec<ForgottenTopic>,
) -> FetchRequest {
    let request = request.unwrap_or_else(|| fetch_request_for(Vec::new(), limits));
    let session = FetchSessionSetup {
        id,
        epoch,
        forgotten,
    };
    FetchRequest {
        session_id: session.id.0,
        session_epoch: session.epoch.0,
        forgotten_topics_data: session.forgotten,
        ..request
    }
}

#[bon::builder]
pub fn incremental_session_fetch(
    #[builder(into)] id: FetchSessionId,
    limits: FetchLimits,
) -> FetchRequest {
    build_fetch_session_request()
        .limits(limits)
        .id(id)
        .epoch(FetchSessionEpoch(1))
        .call()
}

/// Check a successful partition's v2 record count against the caller's expected count.
pub fn check_record_count(
    partition: &krabka_protocol::owned::fetch_response::PartitionData,
    expected: usize,
) {
    assert2::assert!(partition.error_code == 0);
    assert2::assert!(crate::support::records::record_count(partition.records.as_ref()) == expected);
}
