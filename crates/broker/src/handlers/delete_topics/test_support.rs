//! Fixtures shared by the `DeleteTopics` tests: the two shapes a v6+
//! `DeleteTopicState` can take, by name and by topic id, the request that wraps
//! them, and the KFC-9 break-glass configuration the gate tests and the
//! end-to-end refusal tests both build a broker from.

pub(super) use crate::test_support::gated_break_glass_config as gated_config;
use krabka_protocol::{
    owned::delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
    primitives::uuid::Uuid as WireUuid,
};

/// The topic every KFC-9 test asks the broker to delete.
pub(super) const DOOMED: &str = "doomed";

/// A `DeleteTopicState` that names its topic and leaves the id zeroed.
pub(super) fn named_state(name: &str) -> DeleteTopicState {
    DeleteTopicState {
        name: Some(name.into()),
        ..Default::default()
    }
}

/// A `DeleteTopicState` that carries only a topic id, as KIP-516 allows.
pub(super) fn id_state(id: WireUuid) -> DeleteTopicState {
    DeleteTopicState {
        name: None,
        topic_id: id,
        ..Default::default()
    }
}

/// A v6+ `DeleteTopicsRequest` over `topics`, with the timeout the handler
/// tests share.
pub(super) fn request(topics: Vec<DeleteTopicState>) -> DeleteTopicsRequest {
    DeleteTopicsRequest {
        topics,
        timeout_ms: 5_000,
        ..Default::default()
    }
}
