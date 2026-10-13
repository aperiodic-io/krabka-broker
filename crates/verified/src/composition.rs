//! Cross-module safety theorems, compiled only for proofs and tests.
//!
//! Each theorem states an aggregate guarantee under explicit preconditions.
//! Calls use the kernels' contracts; no implementation is copied into a model.

#![cfg_attr(creusot, allow(dead_code))] // Theorems are checked without a runtime caller.

pub(super) use crate::broker::FetchVisibility;
#[cfg(creusot)]
pub(super) use crate::broker::{clamped_fetch_watermarks, committed_fetch_response};
#[cfg(creusot)]
pub(super) use crate::leader_epoch::kafka_end_offset_for;

pub(super) mod oauth_jwks_prelude {
    pub(super) use crate::{
        jwks::{JwksCacheDecision, JwksCacheFacts, jwks_cache_admission},
        oauth::{
            OAuthSessionDecision, OAuthSessionFacts, oauth_session_admission,
            oauth_validation_admission,
        },
    };
}

use {
    crate::{
        audit::{AuditLosses, settle_loss_batch},
        authz::{
            AclDecision, AclDefault, AclFacts, AclOperationKind, AclPatternKind, AclResourceFacts,
            AclResourceTypeMatch, acl_decision, acl_identity_match, acl_operation_match,
            acl_resource_match,
        },
        broker::{
            DeleteRecordsTrimApplication, DeleteRecordsTrimDecision, DeleteRecordsTrimFacts,
            FetchWatermarks, ReplicaFetchFacts, ReplicaFetchMutation,
            delete_records_trim_application, delete_records_trim_decision, fetch_visibility,
            replica_fetch_mutation,
        },
        consensus::election_has_quorum,
        delegation_token::{
            TokenApi, TokenApiAdmission, token_api_admission, token_describe_visible,
        },
        delivery::{delivery_watermark_advance, scheduled_delivery_visible},
        diskless::diskless_trim_decision,
        leader_epoch::{EpochEntry, epoch_and_offset_for_entries},
        local_recovery::local_recovery_batch_step,
        log_index::{
            IndexEntry, OffsetEntry, TimeEntry, offset_index_lookup,
            offset_index_position_at_or_after, time_index_lookup, time_index_scan_start,
        },
        offset_allocator::{reserve_offsets, wal_reservation_frontier},
        produce::produce_durability_frontier,
        producer::{
            ProducerDecision, ProducerEntryFacts, RetainedSequenceRange, decrement_sequence,
            increment_sequence, producer_decision,
        },
        producer_snapshot::{
            ProducerReloadRange, ProducerSnapshotEntryFacts, producer_snapshot_entry_valid,
            producer_snapshot_latest_index, producer_snapshot_replay_start,
        },
        quota::{quota_charge, quota_credit, quota_debt_cap, quota_refill, quota_whole_request},
        raft::{advance_high_watermark, in_half_open_window},
        remote_read::remote_time_index_candidate_count,
        restore::{
            RestoreBatchFrame, RestoreExclusions, RestoreFilterDecision, RestoreRecordDeltas,
            restore_batch_filter_decision, restore_batch_step, restore_record_coordinates,
            restore_record_selected,
        },
        restore_sidecar::{
            RestoreAbortedTxn, RestoreSegmentExtent, restore_index_frontier,
            restore_leader_epoch_entry_valid, restore_offset_index_entry_valid,
            restore_time_index_entry_valid, restore_txn_index_entry_valid,
        },
        retention::RemoteRetentionSegment,
        storage::{local_append_coordinates, truncation_batch_retained, truncation_frontier},
        throttle::{AvailableTokens, BurstCapacity, RefillTokens, RequestedTokens, plan_consume},
        timestamp::{earliest_max_timestamp_index, first_timestamp_index, timestamp_scan_next},
        transaction::{
            LogBatchKind, aborted_transaction_interval, aborted_transaction_overlaps,
            first_unstable_offset, log_batch_kind, transaction_marker_closes,
        },
        wal::{
            select_wal_voters, wal_batch_equal, wal_checkpoint_range_valid,
            wal_covering_batch_range, wal_voter_set_valid,
        },
    },
    creusot_std::prelude::*,
    krabka_ids::{LeaderEpoch, Offset},
};

/// One User-resource ACL projected for a token owner's `DescribeTokens` check.
/// Equality, prefix and CIDR facts must faithfully describe the stored entry.
#[krabka_macros::model_type(runtime(Clone, Copy, Debug))]
struct TokenDescriptionAcl {
    resource: AclResourceFacts,
    pattern: AclPatternKind,
    operation: AclOperationKind,
    allow: bool,
    principal: (bool, bool),  // wildcard, exact
    host: (bool, bool, bool), // wildcard, exact, supported CIDR match
}

type WalFetchSupport = (i64, i64, Vec<(u64, i64)>);

type WalCopyBatch = (i64, i32, Vec<u8>);

mod quota_time;
use quota_time::capped_charge_is_repaid_by_elapsed_time;

mod quota;
use quota::{
    bounded_quota_debt_cannot_outlast_repayment, quota_charge_refund_restores_consume_budget,
};

mod list_offsets;
use list_offsets::timestamp_list_offsets_finds_first_visible;

mod typed_timestamp;
use typed_timestamp::typed_timestamp_records_preserve_visibility;

mod tiered_timestamp;
use tiered_timestamp::tiered_timestamp_lookup_preserves_first;

mod token_description;
use token_description::token_description_preserves_authentication_and_acl_isolation;

mod append;
use append::{
    append_frontiers_agree, reservations_do_not_overlap,
    reserved_pair_preserves_recovery_and_ack_order,
};

mod producer_replay;
use producer_replay::{
    reloaded_snapshot_preserves_last_batch_retry, replayed_window_preserves_first_retry_coordinates,
};

mod transaction_fetch;
use transaction_fetch::{committed_fetch_excludes_unstable, control_marker_bounds_committed_fetch};

mod marker_admission;
use marker_admission::admitted_marker_bounds_committed_fetch;

mod byte_quorum;
use byte_quorum::durable_matching_copies_bound_fetch;

mod quorum_fetch;
use quorum_fetch::{installed_wal_quorum_bounds_fetch, quorum_commit_bounds_fetch};

mod wal_copy;
use wal_copy::{checked_wal_copy_replays_exactly, covering_copy_preserves_logical_fetch};

mod wal_recovery;
use wal_recovery::{checkpoint_truncation_bounds_fetch, published_trim_bounds_recovery};

mod offset_index;
use offset_index::validated_index_bounds_lookup;

mod offset_seek;
use offset_seek::indexed_offset_scan_preserves_first_batch;

mod audit;
use audit::{admitted_loss_marker_preserves_pending, loss_settlement_is_idempotent};

mod time_index;
use time_index::validated_time_cursors_are_monotone;

mod epoch;
use epoch::validated_epochs_bound_truncated_fetch;

mod epoch_replay;
use epoch_replay::resolved_epoch_bounds_retained_replay;

mod snapshot_replay;
use snapshot_replay::{
    corrupt_snapshot_fallback_preserves_replay, truncated_snapshot_selection_bounds_replay,
};

mod abort_union;
use abort_union::restored_abort_sources_cover_committed_fetch;

mod restored_aborts;
use restored_aborts::restored_aborts_remain_bounded_when_fetch_shrinks;

mod delivery;
use delivery::{scheduled_prefix_bounds_fetch, segment_maximum_proves_delivery};

mod scheduled_stability;
use scheduled_stability::scheduled_stable_prefix_bounds_fetch;

mod replication;
use replication::fenced_replication_bounds_fetch;

mod control_truncation;
use control_truncation::whole_batch_truncation_bounds_controls;

mod snapshot_tail;
use snapshot_tail::loaded_snapshot_bounds_truncated_retry;

mod producer_window;
use producer_window::rebuilt_data_window_bounds_retry;

mod completed_producer;
use completed_producer::completed_batches_preserve_first_retry;

mod completed_eviction;
use completed_eviction::completed_eviction_bounds_waiters;

mod epoch_handoff;
use epoch_handoff::epoch_handoff_distinguishes_identity_and_data_retry;

mod epoch_delayed;
use epoch_delayed::epoch_handoff_survives_delayed_completions;

mod rotation_marker;
use rotation_marker::rotation_marker_bounds_identity_retry;

mod truncated_producer;
use truncated_producer::truncated_replay_bounds_first_retry;

mod restore_retry;
use restore_retry::filtered_restore_preserves_producer_retry;

mod restore_selection;
use restore_selection::restore_selection_respects_batch_extent;

mod wal_placement;
use wal_placement::{
    constructed_wal_placement_is_installable, wal_placement_survives_one_rack_loss,
};

mod trim;
use trim::{
    admitted_trim_bounds_reload_and_retry, diskless_trim_reconciliation_preserves_coverage,
    trim_steps_converge,
};

mod timestamp;
use timestamp::{
    ConstructedTimeIndex, constructed_time_index_preserves_first,
    indexed_timestamp_scan_finds_first, remote_timestamp_scan_preserves_first,
    running_maximum_index_entry, validated_remote_and_local_time_starts_agree,
};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Every finished remote segment has representable inclusive range boundaries.
fn finished_ranges_valid(count: Int, ranges: creusot_std::logic::Mapping<Int, (i64, i64)>) -> bool {
    pearlite! { forall<i: Int> 0 <= i && i < count
    ==> offset_range_valid(ranges.get(i).0@, ranges.get(i).1@) }
}

/// Project completed remote ranges into the facts consumed by retention policy.
#[requires(finished_ranges_valid(finished@.len(), |i: Int| (finished@[i].0, finished@[i].1)))]
#[ensures(result@.len() == finished@.len())]
#[ensures(forall<i: Int> 0 <= i && i < finished@.len() ==> result@[i].blocked == false
    && result@[i].log_start_breached == match deleted_below { None => false, Some(floor) => finished@[i].1@ < floor@ }
    && result@[i].time_expired == finished@[i].3 && result@[i].size == finished@[i].2)]
fn remote_retention_facts(
    finished: &[(i64, i64, u64, bool)],
    deleted_below: Option<i64>,
) -> Vec<RemoteRetentionSegment> {
    let mut facts: Vec<RemoteRetentionSegment> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= finished@.len() && facts@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> facts@[j].blocked == false
        && facts@[j].log_start_breached == match deleted_below { None => false, Some(floor) => finished@[j].1@ < floor@ }
        && facts@[j].time_expired == finished@[j].3 && facts@[j].size == finished@[j].2)]
    #[variant(finished@.len() - i@)]
    while i < finished.len() {
        facts.push(RemoteRetentionSegment {
            blocked: false,
            log_start_breached: matches!(deleted_below, Some(floor) if finished[i].1 < floor),
            time_expired: finished[i].3,
            size: finished[i].2,
        });
        i += 1;
    }
    facts
}

#[cfg(test)]
mod tests;

mod refill;
use refill::refill_partition_preserves_consume_budget;

mod consume_trace;
use consume_trace::metered_consumes_conserve_elapsed_credit;

mod stable_abort;
use stable_abort::stable_abort_sources_cover_fetch;

// Complete ordered relative offsets, decoded timestamps, sparse (indexed, through) rows.
type SparseTimestampWindow<'a> = (&'a [u32], &'a [i64], &'a [(usize, usize)]);

mod retained_timestamp;
use retained_timestamp::constructed_index_retained_candidate;

mod constructed_tiered_timestamp;
use constructed_tiered_timestamp::constructed_tiered_timestamp_preserves_first;

mod validated_time_scan;
use validated_time_scan::validated_retained_time_scan_agrees;

mod trim_timestamp;
use trim_timestamp::completed_trim_preserves_retained_timestamp;

mod eviction_timestamp;
use eviction_timestamp::physical_eviction_routes_retained_timestamp;

mod time_range;
use time_range::constructed_time_range_preserves_first;

mod stable_time_range;
use stable_time_range::stable_time_range_preserves_first;

mod covered_retention;
use covered_retention::remote_coverage_bounds_local_retention;

mod remote_breach;
use remote_breach::published_trim_bounds_remote_breach_cleanup;

mod remote_delete;
use remote_delete::completed_remote_retention_bounds_floor;

mod schema_walk;
use schema_walk::{SchemaWalkField, framed_schema_walk_admission};

mod schema_produce;
use schema_produce::schema_checked_produce_frontier;

mod reconfiguration;
use reconfiguration::{
    constructed_voter_reconfiguration, reconfiguration_control_commit_waiter,
    reconfiguration_control_prefix_support, reconfigured_majorities_overlap,
};

mod token_session;
use token_session::created_token_session_bounds_requests;

mod session_cap;
use session_cap::credential_free_session_cap_bounds_requests;

mod oauth_completion;
use oauth_completion::validated_oauth_snapshot_bounds_session;

mod jwks_publication;
use jwks_publication::published_keys_bound_oauth_session;

mod controller_session;
use controller_session::published_controller_session_bounds_quorum_requests;

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// A nonnegative offset range has an ordered inclusive end.
pub fn offset_range_valid(start: Int, end: Int) -> bool {
    pearlite! { 0 <= start && start <= end }
}
