//! End-to-end `ListOffsets` request and visibility decisions.

#[cfg(creusot)]
use creusot_std::prelude::{DeepModel, logic};

/// The meaning of one `ListOffsets` request timestamp at its wire version.
#[krabka_macros::model_type]
pub enum ListOffsetsKind {
    Unsupported,
    Earliest,
    Latest,
    MaxTimestamp,
    EarliestLocal,
    LatestTiered,
    EarliestPendingUpload,
    Timestamp,
}

/// KIP-320 leader-epoch admission for one partition row.
#[krabka_macros::model_type]
pub enum ListOffsetsEpochDecision {
    RejectMalformed,
    Proceed,
    Fenced,
    Unknown,
}

/// The offsets that select one request's isolation bound.
#[krabka_macros::model_type]
pub struct ListOffsetsBoundFacts {
    pub replica_id: i32,
    pub isolation_level: i8,
    pub log_end: i64,
    pub high_watermark: i64,
    pub last_stable: i64,
}

/// A valid last-fetchable offset, or a fail-closed malformed-state result.
#[krabka_macros::model_type]
pub enum ListOffsetsBoundDecision {
    RejectMalformed,
    Bound { offset: i64 },
}

/// Local and optional cold-tier candidates for the `EARLIEST` sentinel.
#[krabka_macros::model_type]
pub struct ListOffsetsEarliestFacts {
    pub local: i64,
    pub has_remote: bool,
    pub remote: i64,
    pub has_diskless: bool,
    pub diskless: i64,
}

/// One tier-specific candidate before the final visibility clamp.
#[krabka_macros::model_type]
pub struct ListOffsetsSelectionFacts {
    pub kind: ListOffsetsKind,
    pub candidate_offset: i64,
    pub candidate_timestamp: i64,
    pub candidate_epoch: i32,
    pub last_fetchable: i64,
}

/// The final visible partition-row value.
#[krabka_macros::model_type]
pub enum ListOffsetsSelectionDecision {
    RejectMalformed,
    Unknown,
    Resolved {
        offset: i64,
        timestamp: i64,
        leader_epoch: i32,
    },
}

mod selection_model;
#[cfg(creusot)]
pub use selection_model::list_offsets_selection_model;
pub use selection_model::{
    list_offsets_bound_decision, list_offsets_earliest, list_offsets_epoch_decision,
    list_offsets_kind,
};

mod selection_decision;
pub use selection_decision::list_offsets_selection_decision;

mod timestamp_candidate;
pub use timestamp_candidate::earliest_timestamp_candidate;

#[cfg(test)]
mod tests;
