//! Pure, safety-critical decision kernels used by `krabka-broker`.
//!
//! Keeping these small arithmetic decisions here lets Creusot prove the exact
//! executable bodies used by the asynchronous broker.

use creusot_std::prelude::*;

open_logic! {
/// Truncation preserves the floor and clamps each visibility frontier to its end.
pub(crate) fn clamped_fetch_watermarks(
    original: FetchWatermarks,
    bounded: FetchWatermarks,
) -> bool {
    pearlite! {
        bounded.log_start == original.log_start
            && bounded.hw@ == original.hw@.min(bounded.log_end@)
            && bounded.lso@ == original.lso@.min(bounded.log_end@)
            && bounded.deliverable@ == original.deliverable@.min(bounded.log_end@)
    }
}
}

open_logic! {
/// Export the committed response watermarks and empty-view flags from one floor.
pub(crate) fn committed_fetch_response(
    view: FetchVisibility,
    hw: i64,
    lso: i64,
    deliverable: i64,
    floor: Int,
    log_end: Int,
) -> bool {
    pearlite! {
        view.response_hw == hw && view.response_lso@ == hw@.min(lso@)
            && view.effective_lso@ == hw@.min(lso@) && view.read_committed_aborts
            && view.out_of_range == (floor > log_end)
            && view.empty == (floor <= log_end && floor >= hw@.min(deliverable@))
    }
}
}

model_types! {
    @proof (derive(std::clone::Clone, Copy, DeepModel));
    /// Visibility bounds and response watermarks for one Fetch partition.
    pub struct FetchVisibility {
        pub out_of_range: bool,
        pub empty: bool,
        pub limit_offset: i64,
        pub effective_lso: i64,
        pub read_committed_aborts: bool,
        pub response_hw: i64,
        pub response_lso: i64,
    }

    /// The partition offsets one Fetch visibility decision reads.
    ///
    /// They are one struct because they are five `i64` values with five different
    /// meanings, and a transposed call site would compile.
    pub struct FetchWatermarks {
        /// First offset the log still holds. Below it a fetch is out of range.
        pub log_start: i64,
        /// High watermark: the exclusive bound of what the ISR has replicated.
        pub hw: i64,
        /// Last stable offset: the first offset an open transaction may cover.
        pub lso: i64,
        /// Log end offset: the exclusive bound of what the leader holds.
        pub log_end: i64,
        /// KFC-1 delivery watermark: the first offset that is not due yet.
        pub deliverable: i64,
    }

    /// The only direct mutation class selected from one follower Fetch response.
    pub enum ReplicaFetchMutation {
        /// The response is not for the exact live request target.
        Reject,
        /// The response is fenced or otherwise unsuccessful; error handling may
        /// retry or enter a separately guarded recovery path.
        Retry,
        /// Apply the KIP-320 divergence boundary and return without appending.
        Truncate,
        /// Apply the successful response batches, then its high watermark.
        Append,
    }

    /// What a follower knows about one row of a Fetch response when it decides
    /// what that row may change.
    ///
    /// The row's topic and partition identity is not among them: the host looks
    /// the row up by that identity (`ResponseIndex::locate` in the broker's
    /// replicator) and never hands the kernel a row it could not attribute to
    /// exactly one followed partition.
    pub struct ReplicaFetchFacts {
        /// The leader epoch this follower sent for the partition in the request
        /// that the row answers.
        pub request_leader_epoch: i32,
        /// The leader epoch of the partition's current replication target.
        pub current_leader_epoch: i32,
        /// The replication target is the one the request was sent to.
        pub target_matches: bool,
        /// The row's `current_leader` is either absent or exactly the current
        /// target's leader and epoch.
        pub reported_target_matches: bool,
        /// The row's Kafka error code; `0` is `NONE`.
        pub error_code: i16,
        /// The row's KIP-320 `DivergingEpoch.Epoch`; `-1`, the schema default,
        /// means the row carries no divergence. Kafka's
        /// `FetchResponse.isDivergingEpoch` tests the epoch, not the end offset.
        pub diverging_epoch: i32,
    }

    /// What a preferred-leader rebalance scan observed about one change it wants
    /// to submit.
    ///
    /// The host reads every fact off the change record itself and the scan's
    /// liveness and witness snapshots, not off the election that produced it, so
    /// the kernel checks the election's output rather than restating it.
    pub struct PreferredLeaderChange {
        /// The leader the change installs.
        pub new_leader: u64,
        /// The partition's preferred replica, `replicas[0]`, or `None` for an
        /// empty assignment.
        pub preferred_replica: Option<u64>,
        /// The new leader is in the change's ISR.
        pub leader_in_isr: bool,
        /// The new leader is alive in the scan's liveness snapshot.
        pub leader_alive: bool,
        /// The new leader carries the witness role, so it may never lead.
        pub leader_is_witness: bool,
    }

    /// Offset facts used to admit one `DeleteRecords` trim.
    pub struct DeleteRecordsTrimFacts {
        pub requested: i64,
        pub high_watermark: i64,
        pub log_end: i64,
        pub current_start: i64,
        pub has_delivery_watermark: bool,
        pub delivery_watermark: i64,
    }

    /// The complete boundary decision for one `DeleteRecords` trim.
    pub enum DeleteRecordsTrimDecision {
        RejectMalformed,
        RejectOutOfRange,
        Noop { frontier: i64 },
        Apply { frontier: i64 },
    }

    /// The next idempotent step while reconciling WAL and local trim frontiers.
    pub enum DeleteRecordsTrimApplication {
        RejectMalformed,
        TrimWal { frontier: i64 },
        TrimLocal { frontier: i64 },
        Complete { frontier: i64 },
    }

    /// The complete per-key admission outcome for `FindCoordinator`.
    ///
    /// The allow variants preserve the key type for the host adapter. Denials carry
    /// the Kafka authorization domain, while malformed SHARE keys and unknown wire
    /// discriminants fail closed as invalid requests.
    pub enum FindCoordinatorAdmission {
        AllowGroup,
        AllowTransaction,
        AllowShare,
        DenyGroup,
        DenyTransaction,
        DenyCluster,
        InvalidRequest,
    }
}

mod fetch;
#[cfg(creusot)]
pub use fetch::{fetch_limit_model, offset_min, preferred_change_eligible, replica_fetch_fenced};
pub use fetch::{fetch_visibility, preferred_rebalance_admission, replica_fetch_mutation};

mod trim_admission;
#[cfg(creusot)]
pub use trim_admission::effective_share_backlog_model;
pub use trim_admission::{
    delete_records_trim_application, delete_records_trim_decision, effective_share_backlog,
};

mod coordinator_partition;
#[cfg(creusot)]
pub use coordinator_partition::java_string_hash_prefix_model;
pub use coordinator_partition::{
    find_coordinator_admission, java_string_hash_partition, unclean_recovery_commit_admission,
};

#[cfg(test)]
mod tests;
