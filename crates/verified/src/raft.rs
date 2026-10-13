//! Pure `KRaft` offset-frontier and half-open-window kernels.

#[cfg(creusot)]
use creusot_std::prelude::*;

/// The only response-derived mutation an admitted `KRaft` Fetch may perform.
#[krabka_macros::model_type]
pub enum FetchResponseMutation {
    /// The response belongs to another leader, role, or epoch, or carries an
    /// error; apply nothing.
    Reject,
    /// Attach to the advertised leader and refetch from it. The responder's
    /// identity plays no part: the host reaches the leader through the
    /// leader's own `NodeEndpoints` entry or its voter-set listener.
    Discover,
    Snapshot,
    Truncate,
    Append,
    HighWatermark,
}

/// The receiving node's live fence for one Fetch response.
#[krabka_macros::model_type]
pub struct FetchFence {
    /// The node is an observer with no known leader.
    pub discovering: bool,
    /// The leader the live role fetches from.
    pub role_leader: Option<u64>,
    /// The leader in the durable quorum state.
    pub current_leader: Option<u64>,
    pub current_epoch: u32,
}

/// Who answered a Fetch, and which leader, epoch and error the answer names.
#[krabka_macros::model_type]
pub struct FetchResponseFacts {
    /// The node the request was sent to.
    pub from: u64,
    /// The response's `CurrentLeader.LeaderId`, `None` for Kafka's -1.
    pub leader: Option<u64>,
    /// The response's `CurrentLeader.LeaderEpoch`.
    pub epoch: u32,
    /// The partition's `ErrorCode` is `NONE`. A Kafka `KRaft` replica answers
    /// `NONE` only as the leader of its epoch; any other replica answers
    /// `validateLeaderOnlyRequest`'s error with the leader it knows.
    pub error_none: bool,
}

/// Which mutations the response body carries.
#[krabka_macros::model_type]
pub struct FetchContent {
    pub has_snapshot: bool,
    pub has_divergence: bool,
    pub has_records: bool,
}

mod metadata_record_offset_deltas;
pub use metadata_record_offset_deltas::{
    advance_high_watermark, control_history_frontier, fetch_response_mutation, frontier_reaches,
    in_half_open_window, metadata_record_offset_deltas,
};
#[cfg(creusot)]
pub use metadata_record_offset_deltas::{discovery_applies, leader_fence_holds};

#[cfg(test)]
mod tests;
