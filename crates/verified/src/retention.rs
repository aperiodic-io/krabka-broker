//! Retention-prefix selection for the local log and the remote tier.
//!
//! Kafka deletes retained-away segments oldest first and stops at the first
//! segment it keeps, so every selection here is a contiguous prefix. The local
//! walk and the remote walk combine their predicates differently, because
//! Kafka does: `UnifiedLog.deleteOldSegments` runs separate passes over the
//! local log, while `RemoteLogManager`'s `RemoteLogRetentionHandler` checks
//! every predicate per remote segment. Each walk is proved equal to a
//! reference fold that states the Kafka rule.

use creusot_std::prelude::*;

model_types! {
    @copy_only
    /// What the local retention walk knows about one local segment.
    pub struct LocalRetentionSegment {
        /// No pass may delete this segment, and every pass stops at it. The host
        /// sets it for a segment that holds a record whose delivery time has not
        /// arrived, or for a tiered segment that the remote tier does not cover
        /// whole. It plays the part of Kafka's high-watermark bound and its
        /// `isSegmentEligibleForDeletion` check.
        pub blocked: bool,
        /// The segment breaches `retention.ms`, or it lies wholly below the log
        /// start offset.
        pub expired: bool,
        /// The segment's exact size in bytes.
        pub size: u64,
    }

    /// What the remote retention walk knows about one finished remote segment.
    pub struct RemoteRetentionSegment {
        /// A scheduled batch is not due, or its activation cannot be established.
        /// Every retention axis stops before this segment.
        pub blocked: bool,
        /// The segment's whole offset range lies below the log start offset.
        pub log_start_breached: bool,
        /// The segment breaches `retention.ms`.
        pub time_expired: bool,
        /// The segment's exact size in bytes.
        pub size: u64,
    }
}

mod remote_retention_model;
pub use remote_retention_model::{barrier_cut_expired, local_retention_prefix};
#[cfg(creusot)]
pub use remote_retention_model::{
    local_retention_limit, local_retention_model, remote_retention_model,
};

mod delete_target;
pub use delete_target::{remote_retention_prefix, retention_delete_target};

mod coverage;
pub use coverage::remote_covered_through;
#[cfg(creusot)]
pub use coverage::{remote_covers_offset, remote_ranges_valid};

#[cfg(creusot)]
mod local_bytes;
#[cfg(creusot)]
pub use local_bytes::{local_prefix_bytes, local_prefix_bytes_monotone};

#[cfg(creusot)]
mod remote_bytes;
#[cfg(creusot)]
pub use remote_bytes::remote_prefix_charge;

mod remote_floor;
pub use remote_floor::remote_retention_floor_step;

open_logic! {
/// Size retention consumes a row only while positive debt funds its entire charged prefix.
pub fn remote_charge_funded(debt: Int, charged: Int, next_charge: Int) -> bool {
    pearlite! { debt > 0 && charged < debt && next_charge <= debt }
}
}

#[cfg(test)]
mod tests;
