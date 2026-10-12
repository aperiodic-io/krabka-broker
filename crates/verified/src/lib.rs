//! Formally verified pure kernels shared by Krabka crates.
//!
//! These synchronous kernels have Creusot contracts proved under their stated
//! preconditions. Host state, concurrency, and I/O remain outside that boundary.
//! See `docs/verification.md` and `docs/proof-contract-audit.md`.
#![doc(html_root_url = "https://docs.rs/krabka-verified/1.0.3")]
// Rust's backend does not see names used only in Creusot contracts, and treats
// the grouping parentheses in Pearlite expressions as redundant Rust syntax.
#![cfg_attr(creusot, allow(unused_imports, unused_parens))]

/// Declare an open proof model with the crate's mutation-testing policy.
macro_rules! open_logic {
    ($item:item) => {
        // cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
        #[cfg(creusot)]
        #[cfg_attr(test, mutants::skip)]
        #[logic(open)]
        $item
    };
}

/// Declare model types with the crate's proof and runtime derive policy.
/// A custom pair preserves types whose proof model or runtime traits differ.
macro_rules! model_types {
    (@derives ($proof:meta) ($runtime:meta); $($item:item)*) => {
        $(
            #[cfg_attr(creusot, $proof)]
            #[cfg_attr(not(creusot), $runtime)]
            $item
        )*
    };
    (@copy_only $($item:item)*) => {
        model_types! {
            @derives (derive(std::clone::Clone, Copy)) (derive(Clone, Copy, Debug, PartialEq, Eq));
            $($item)*
        }
    };
    (@proof ($proof:meta); $($item:item)*) => {
        model_types! {
            @derives ($proof) (derive(Clone, Copy, Debug, PartialEq, Eq));
            $($item)*
        }
    };
}

pub mod audit;
pub mod authz;
pub mod barrier;
pub mod break_glass;
pub mod break_glass_persistence;
pub mod broker;
pub mod chain;
pub mod checkpoint;
pub mod compaction;
#[cfg(any(creusot, test))]
mod composition;
pub mod consensus;
pub mod delegation_token;
pub mod delivery;
pub mod directory;
pub mod diskless;
pub mod epoch;
pub mod features;
pub mod freeze;
pub mod group_migration;
pub mod isr;
pub mod jwks;
pub mod leader_epoch;
pub mod list_offsets;
pub mod local_recovery;
pub mod log_index;
pub mod oauth;
pub mod offset_allocator;
pub mod opa;
pub mod produce;
pub mod producer;
pub mod producer_id;
pub mod producer_snapshot;
pub mod quorum_state;
pub mod quota;
pub mod raft;
pub mod reassignment;
pub mod reconfiguration;
pub mod recovery;
pub mod registration;
pub mod remote_metadata;
pub mod remote_read;
pub mod remote_txn;
pub mod restore;
pub mod restore_sidecar;
pub mod retention;
pub mod schema;
pub mod scram;
#[cfg(creusot)]
mod sequence;
pub mod share;
pub mod snapshot;
pub mod stamp;
pub mod storage;
pub mod stretch;
pub mod throttle;
pub mod timestamp;
pub mod transaction;
pub mod uniform_assignor;
pub mod vote;
pub mod voter_set;
pub mod wal;
pub mod worm;

pub use audit::{
    AuditCheckpointAdmission, AuditLossMarkerAdmission, SpoolAppendDecision,
    audit_checkpoint_admission, audit_loss_marker_admission, spool_append_decision,
};
pub use authz::{
    AclDecision, AclOperationKind, AclPatternKind, acl_decision, acl_identity_match,
    acl_operation_match, acl_resource_match,
};
pub use barrier::{
    BarrierCutClassification, BarrierMarkerFenceDecision, BarrierMarkerFenceFacts,
    BarrierPlacementDecision, BarrierTargetCountDecision, barrier_cut_classification,
    barrier_marker_fence_decision, barrier_placement_decision, barrier_target_count_decision,
};
pub use break_glass::{BreakGlassAdmission, break_glass_admission, select_break_glass_candidate};
pub use break_glass_persistence::{
    BreakGlassConsumptionDecision, BreakGlassConsumptionFacts, BreakGlassLocalActionDecision,
    BreakGlassLocalActionFacts, BreakGlassLocalSpendState, BreakGlassProposalState,
    break_glass_consumption_decision, break_glass_local_action_decision,
};
pub use broker::{
    DeleteRecordsTrimApplication, DeleteRecordsTrimDecision, DeleteRecordsTrimFacts,
    FetchVisibility, FetchWatermarks, ReplicaFetchMutation, delete_records_trim_application,
    delete_records_trim_decision, effective_share_backlog, fetch_visibility,
    preferred_rebalance_admission, replica_fetch_mutation, unclean_recovery_commit_admission,
};
pub use chain::{ChainStep, chain_step, select_chain_tip};
pub use checkpoint::{checkpoint_id_newer, checkpoint_id_retained, latest_checkpoint_index};
pub use compaction::{
    BatchMeta, RecordMeta, RetainDecision, TxnDataState, compute_horizon, retain_decision,
};
pub use consensus::{
    election_has_quorum, election_jitter_ms, log_is_up_to_date, majority_size,
    recompute_high_watermark,
};
pub use delegation_token::{
    TokenCreateDecision, TokenDeadlines, TokenExpireDecision, TokenMutationDecision,
    TokenMutationFacts, TokenMutationKind, TokenMutationState, TokenRenewDecision,
    create_token_deadlines, expire_token_deadline, renew_token_expiry, token_is_active,
    token_mutation_decision,
};
pub use delivery::{
    coalesce_delivery_range, delivery_watermark_advance, scheduled_delivery_visible,
};
pub use directory::{
    DirectoryAssignmentDecision, DirectoryResponseDecision, directory_assignment_decision,
    directory_response_decision,
};
pub use diskless::{
    DisklessBatchStep, DisklessTrimDecision, diskless_batch_step, diskless_logical_range,
    diskless_object_reclaimable, diskless_retention_prefix, diskless_span_extension,
    diskless_trim_decision,
};
pub use epoch::exact_epoch_successor;
pub use features::{FeatureUpdateDecision, feature_update_decision};
pub use freeze::{
    FreezeIdentityState, FreezeMutationDecision, FreezeMutationKind, FreezeReplacementDecision,
    FreezeReplacementFacts, FreezeScopeDecision, FreezeScopeRank, FreezeSignatureDecision,
    FreezeSignatureFacts, FreezeStoredState, freeze_mutation_decision, freeze_refuses,
    freeze_replacement_decision, freeze_scope_decision, freeze_signature_decision,
    freeze_timestamp_in_window,
};
pub use group_migration::{
    GroupMigrationDirection, GroupMigrationRecordAction, GroupMigrationRecordPlan,
    classic_upgrade_epoch, consumer_downgrade_epoch, group_migration_record_plan,
};
pub use jwks::{
    JwksCacheDecision, JwksCacheFacts, JwksOnDemandDecision, jwks_cache_admission,
    jwks_on_demand_refresh_decision, jwks_publication_generations,
};
pub use leader_epoch::{EpochEntry, epoch_and_offset_for_entries};
pub use list_offsets::{
    ListOffsetsBoundDecision, ListOffsetsBoundFacts, ListOffsetsEarliestFacts,
    ListOffsetsEpochDecision, ListOffsetsKind, ListOffsetsSelectionDecision,
    ListOffsetsSelectionFacts, list_offsets_bound_decision, list_offsets_earliest,
    list_offsets_epoch_decision, list_offsets_kind, list_offsets_selection_decision,
};
pub use local_recovery::{
    LocalRecoveryStep, LocalRecoverySwapAction, local_recovery_batch_step,
    local_recovery_index_frontier, local_recovery_sealed_last, local_recovery_segment_chain,
    local_recovery_swap_action,
};
pub use log_index::{offset_index_lookup, offset_index_position_at_or_after, time_index_lookup};
pub use oauth::{
    OAuthAuthenticationKind, OAuthExpiryPresence, OAuthPrincipalMatch, OAuthSessionCap,
    OAuthSessionDecision, OAuthSessionFacts, oauth_session_admission,
};
pub use offset_allocator::{
    reserve_offsets, wal_reservation_epoch_ready, wal_reservation_frontier,
    wal_reservation_response,
};
pub use opa::{
    OpaAuthorizationDecision, OpaCacheAdmission, OpaCacheExpiry, opa_cache_admission,
    opa_cache_expiry, opa_error_decision,
};
pub use produce::{ProduceBatchAdmission, produce_batch_admission, produce_durability_frontier};
pub use producer::{
    ProducerDecision, ProducerEntryFacts, RetainedSequenceRange, decrement_sequence,
    increment_sequence, producer_decision,
};
pub use producer_id::{
    ProducerIdBlockAllocationDecision, ProducerIdBlockPlan, producer_id_block_allocation,
};
pub use producer_snapshot::{
    ProducerReloadRange, producer_snapshot_entry_valid, producer_snapshot_latest_index,
    producer_snapshot_reload_keeps, producer_snapshot_reload_log_start,
    producer_snapshot_replay_start, producer_snapshot_stray,
};
pub use quorum_state::{
    QuorumStateLoadDecision, QuorumStateWriteDecision, quorum_state_load_decision,
    quorum_state_write_decision,
};
pub use quota::{
    IpQuotaPrecedence, QuotaCandidatePresence, UserClientQuotaFacts, UserClientQuotaPrecedence,
    ip_quota_precedence, user_client_quota_precedence,
};
pub use raft::{FetchResponseMutation, fetch_response_mutation, metadata_record_offset_deltas};
pub use reassignment::{
    ReassignmentAction, ReassignmentSetMembership, reassignment_action,
    reassignment_plan_admission, reassignment_set_membership,
};
pub use reconfiguration::{
    VoterChangeKind, VoterReconfigurationDecision, VoterReconfigurationPlan,
    voter_reconfiguration_decision,
};
pub use recovery::{
    BarrierRecoveryFinalizeDecision, BarrierRecoveryFoldAction, BarrierRecoveryRecordKind,
    ReplayCursorDecision, ReplayRecordDecision, barrier_recovery_finalize_decision,
    barrier_recovery_fold_action, replay_batch_cursor_decision, replay_cursor_decision,
    replay_record_decision, should_capture_first_downgrade,
};
pub use registration::{BrokerHeartbeatDecision, broker_heartbeat_decision};
pub use remote_metadata::{remote_metadata_partition, remote_metadata_resume_cursor};
pub use remote_read::{
    remote_read_relative_offset, remote_time_index_candidate_count, tiered_earliest_finished_index,
    tiered_latest_finished_index, tiered_owning_epoch_index,
};
pub use remote_txn::{RemoteTxnOverlapDecision, remote_txn_overlap_decision};
pub use restore::{
    RestoreBatchFrame, RestoreContentExclusions, RestoreExclusions, RestoreFilterDecision,
    RestoreLayout, RestoreProducer, RestoreReconcileDecision, RestoreRecordDeltas,
    RestoreSnapshotState, RestoreTimestampType, restore_archive_reconcile,
    restore_batch_filter_decision, restore_batch_past_offset_bound, restore_batch_step,
    restore_record_coordinates, restore_record_selected, restore_rewritten_batch_header,
    restore_rewritten_record,
};
pub use restore_sidecar::{
    RestoreAbortedTxn, RestoreSegmentExtent, restore_index_frontier,
    restore_leader_epoch_entry_valid, restore_offset_index_entry_valid,
    restore_producer_ids_strict, restore_time_index_entry_valid, restore_txn_index_entry_valid,
};
pub use retention::{barrier_cut_expired, local_retention_prefix, retention_delete_target};
pub use schema::{
    SchemaBatchAdmission, SchemaFailureDecision, SchemaFailureKind, SchemaFieldAction,
    SchemaFieldRole, schema_batch_admission, schema_failure_decision, schema_field_action,
    schema_frame_id,
};
pub use scram::{
    ScramAlterationDecision, ScramAlterationFacts, ScramAlterationKind, ScramPriorState,
    scram_alteration_decision,
};
pub use share::{
    ShareOffsetMutationDecision, ShareOffsetMutationGate, share_offset_mutation_decision,
    share_prune_frontier,
};
pub use snapshot::{
    SnapshotChunkDecision, SnapshotInstallDecision, snapshot_chunk_admission,
    snapshot_install_decision, snapshot_prune_admission,
};
pub use stamp::{
    covering_stamp_range_index, exact_stamp_range_index, stamp_range_insertion_index,
    stamp_ranges_valid,
};
pub use storage::{
    LocalTruncationPlan, RemoteCacheAction, future_log_swap_admission, local_append_coordinates,
    local_truncation_plan, remote_cache_action, remote_partition_delete_transition,
    remote_segment_transition, truncation_batch_retained, truncation_frontier,
    truncation_relative_offset,
};
pub use stretch::{
    min_insync_is_site_loss_safe, quorum_survives_any_single_site_loss, site_loss_survivors,
};
pub use timestamp::{
    earliest_max_timestamp_index, first_timestamp_index, timestamp_record_coordinates,
    timestamp_scan_next, timestamp_scan_window,
};
pub use transaction::{
    IdleTransactionState, NO_TRANSACTION_TIMEOUT_MS, TransactionCompletionDecision,
    TransactionIdentity, TransactionMarkerMaterializationDecision,
    TransactionReaperCompletionDecision, TransactionSnapshot, aborted_transaction_interval,
    aborted_transaction_overlaps, first_unstable_offset, should_abort_idle_transaction,
    transaction_completion_decision, transaction_marker_closes,
    transaction_marker_materialization_decision, transaction_reaper_completion_decision,
};
pub use vote::{
    VoteAdmissionDecision, VoteEncodeDecision, VoteWireDecision, vote_admission_decision,
    vote_encode_decision, vote_wire_decision,
};
pub use voter_set::{
    VoterSetWireDecision, VoterWireDecision, voter_set_wire_decision, voter_wire_decision,
};
pub use wal::{
    WalFetchAdmission, exact_wal_batch_range, select_wal_voter_index, wal_fetch_admission,
};
pub use worm::{
    WormDigestFacts, WormObjectAvailabilityFacts, WormObjectIdentityFacts, WormObjectSetDecision,
    WormObjectSetFacts, WormSignatureDecision, worm_object_set_decision, worm_signature_decision,
};
