//! The read-only views the engine serves: the durable quorum-state write, the
//! structured consensus snapshot behind `DescribeQuorum`, and the committed
//! `__cluster_metadata` slice an observer fetches.

use krabka_ids::Offset;
use krabka_protocol::records::RecordBatch;
use krabka_units::prelude::ByteSize;

use super::{
    Engine,
    offsets::{
        fetch_batch_committed_before_hwm, metadata_fetch_offset_below_log_start,
        metadata_fetch_offset_in_committed_window,
    },
    quorum_state_file::save_quorum_state,
    records::encode_batches,
};
use crate::{
    error::RaftError,
    kraft::{
        core::QuorumStateMachine,
        log::KraftLog,
        role::{ReplicaProgress, Role},
        transport::{MetadataFetchSlice, ObserverReplica, QuorumStateSnapshot},
        types::{NodeId, SimInstant},
    },
};

/// Floor on an observer's metadata-fetch budget: at least the first committed
/// batch is always emitted so a zero-budget fetch still makes progress.
const MIN_FETCH_BUDGET: ByteSize = krabka_units::bytes(1);

/// How long an observer may stay silent before the leader stops reporting it:
/// Kafka's `LeaderState.OBSERVER_SESSION_TIMEOUT_MS`.
const OBSERVER_SESSION_TIMEOUT_MS: u64 = 300_000;

/// Kafka's `clearInactiveObservers` test: the observer has not fetched for
/// [`OBSERVER_SESSION_TIMEOUT_MS`], or the leader never saw it fetch.
fn observer_session_expired(progress: &ReplicaProgress, now: SimInstant) -> bool {
    progress.last_fetch.0 == 0
        || now.0.saturating_sub(progress.last_fetch.0) >= OBSERVER_SESSION_TIMEOUT_MS
}

/// Voter ids from the core's current quorum state (for the initial published
/// snapshot, before the loop runs).
pub fn initial_state_voters(core: &QuorumStateMachine) -> Vec<NodeId> {
    core.quorum_state().voters.ids().into_iter().collect()
}

/// Consensus snapshot published before the engine has tracked any replica progress.
pub fn initial_quorum_snapshot(
    core: &QuorumStateMachine,
    log: &KraftLog,
    high_watermark: i64,
    last_snapshot_timestamp_ms: i64,
) -> QuorumStateSnapshot {
    QuorumStateSnapshot {
        leader_id: core.quorum_state().leader_id,
        leader_epoch: core.quorum_state().leader_epoch,
        high_watermark,
        last_applied_record_timestamp_ms: last_snapshot_timestamp_ms,
        pending_record_processing_time_us_avg: 0,
        pending_record_processing_time_us_max: 0,
        quorum_high_watermark: high_watermark,
        log_end_offset: log.log_end_offset().0,
        log_start_offset: log.log_start_offset().0,
        voters: core.quorum_state().voters.clone(),
        voted_directory_id: core
            .quorum_state()
            .voted_key
            .as_ref()
            .map(|key| key.directory_id),
        observers: Vec::new(),
        per_replica_fetch_offset: std::collections::BTreeMap::new(),
        per_replica_last_fetch_ms: std::collections::BTreeMap::new(),
        per_replica_last_caught_up_ms: std::collections::BTreeMap::new(),
        is_leader: core.role().is_leader(),
        current_state: if core.role().is_leader() {
            "leader"
        } else {
            "follower"
        },
    }
}

impl Engine {
    /// Persist the durable quorum state atomically.
    pub fn persist_quorum_state(&self) -> Result<(), RaftError> {
        save_quorum_state(&self.data_dir, self.core.quorum_state())
    }

    /// Highest offset the quorum has committed, as this node knows it: its own
    /// watermark on a leader, and the highest watermark a leader has reported
    /// on a follower, which is the larger of the two while the follower is
    /// still replaying.
    pub fn quorum_high_watermark(&self) -> i64 {
        self.log.hwm().0.max(self.leader_reported_hwm)
    }

    /// Wall-clock milliseconds of `at`, or -1 when the clock base cannot be
    /// mapped to the Unix epoch.
    fn unix_ms(&self, at: SimInstant) -> i64 {
        self.wall_clock_base
            .checked_add(std::time::Duration::from_millis(at.0))
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(-1, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
    }

    /// Wall-clock milliseconds of a progress timestamp the leader tracks, or -1
    /// while it has not recorded that event (a zero instant), as Kafka reports
    /// it.
    fn tracked_ms(&self, at: SimInstant) -> i64 {
        if at.0 == 0 { -1 } else { self.unix_ms(at) }
    }

    /// Snapshot the consensus state for `DescribeQuorum`.
    pub fn quorum_state_snapshot(&self) -> QuorumStateSnapshot {
        let qs = self.core.quorum_state();
        let mut per_replica_fetch_offset = std::collections::BTreeMap::new();
        let mut per_replica_last_fetch_ms = std::collections::BTreeMap::new();
        let mut per_replica_last_caught_up_ms = std::collections::BTreeMap::new();
        let is_leader = self.core.role().is_leader();
        let current_state = match self.core.role() {
            Role::Leader { .. } => "leader",
            // A resigned leader has stepped down and is waiting out its
            // epoch: it leads nothing and it is not campaigning, which is
            // what `DescribeQuorum` calls a follower.
            Role::Follower { .. } | Role::Resigned => "follower",
            Role::Candidate { .. }
            | Role::Prospective { .. }
            | Role::Unattached { .. }
            | Role::Voted { .. } => "candidate",
            Role::Observer { .. } => "observer",
        };
        let now = self.now();

        if let Role::Leader { replicas, .. } = self.core.role() {
            // The leader's own matched index is its log end offset — its local
            // log is, by definition, fully matched against itself. The
            // `replicas` progress map tracks only *peers*, so the leader must
            // insert its own entry explicitly (otherwise a single-voter quorum
            // reports an empty matched-index map and `DescribeQuorum` returns
            // the JVM "unknown" sentinel -1 for the leader).
            // `per_voter_fetch_offset` is a wire-facing DescribeQuorum DTO of raw
            // `i64`s; the peer entries already come from the core as `i64`.
            let now_ms = self.unix_ms(now);
            per_replica_fetch_offset.insert(self.core.me(), self.log.log_end_offset().0);
            per_replica_last_fetch_ms.insert(self.core.me(), now_ms);
            per_replica_last_caught_up_ms.insert(self.core.me(), now_ms);
            for (id, progress) in replicas {
                per_replica_fetch_offset.insert(*id, progress.fetch_offset);
                per_replica_last_fetch_ms.insert(*id, self.tracked_ms(progress.last_fetch));
                per_replica_last_caught_up_ms.insert(*id, self.tracked_ms(progress.last_caught_up));
            }
        }
        let observers = if is_leader {
            self.observers
                .iter()
                .filter(|(_, progress)| !observer_session_expired(progress, now))
                .map(|(key, progress)| ObserverReplica {
                    id: key.id,
                    directory_id: key.directory_id,
                    log_end_offset: progress.fetch_offset,
                    last_fetch_ms: self.tracked_ms(progress.last_fetch),
                    last_caught_up_ms: self.tracked_ms(progress.last_caught_up),
                })
                .collect()
        } else {
            Vec::new()
        };
        QuorumStateSnapshot {
            leader_id: qs.leader_id,
            leader_epoch: qs.leader_epoch,
            high_watermark: self.log.hwm().0,
            last_applied_record_timestamp_ms: self.last_applied_record_timestamp_ms,
            pending_record_processing_time_us_avg: if self.pending_record_processing_time_samples
                == 0
            {
                0
            } else {
                u64::try_from(
                    self.pending_record_processing_time_us_total
                        / u128::from(self.pending_record_processing_time_samples),
                )
                .unwrap_or(u64::MAX)
            },
            pending_record_processing_time_us_max: self.pending_record_processing_time_us_max,
            quorum_high_watermark: self.quorum_high_watermark(),
            log_end_offset: self.log.log_end_offset().0,
            log_start_offset: self.log.log_start_offset().0,
            voters: qs.voters.clone(),
            voted_directory_id: qs.voted_key.as_ref().map(|key| key.directory_id),
            observers,
            per_replica_fetch_offset,
            per_replica_last_fetch_ms,
            per_replica_last_caught_up_ms,
            is_leader,
            current_state,
        }
    }

    /// Serve a committed `__cluster_metadata` slice for an observer's metadata
    /// fetch (1004): read committed batches at/after `fetch_offset` up to the
    /// HWM and concatenate their verbatim `RecordBatch` bytes (the engine's
    /// records are already Kafka record batches). At least the first batch is
    /// always emitted so the observer makes progress.
    ///
    /// A `fetch_offset` below the pruned log start reads nothing: those records
    /// are gone, so the slice carries the latest snapshot id instead and the
    /// observer installs that snapshot before it fetches again (KIP-630). This
    /// is what `Inbound::Fetch` already does for a controller follower; an
    /// observer that was answered with an empty slice here would re-ask for the
    /// same pruned offset forever.
    pub fn metadata_fetch_slice(
        &self,
        fetch_offset: i64,
        max_size: ByteSize,
    ) -> MetadataFetchSlice {
        // `fetch_offset` arrives raw on the observer metadata-fetch wire; wrap it
        // into the `KraftLog` offset domain for the log-bound comparisons/read.
        let fetch_offset = Offset(fetch_offset);
        let high_watermark = self.log.hwm();
        let log_start_offset = self.log.log_start_offset();
        let snapshot_id = if metadata_fetch_offset_below_log_start(fetch_offset, log_start_offset) {
            self.latest_snapshot_id()
        } else {
            None
        };
        let records = if snapshot_id.is_none()
            && metadata_fetch_offset_in_committed_window(fetch_offset, high_watermark)
        {
            match self
                .log
                .read_decoded(fetch_offset, max_size.max(MIN_FETCH_BUDGET))
            {
                Ok(batches) => {
                    let committed: Vec<RecordBatch> = batches
                        .into_iter()
                        .filter(|b| fetch_batch_committed_before_hwm(b.base_offset, high_watermark))
                        .collect();
                    encode_batches(&committed)
                }
                Err(e) => {
                    tracing::error!(?e, "kraft: metadata fetch read failed");
                    bytes::Bytes::new()
                }
            }
        } else {
            bytes::Bytes::new()
        };
        MetadataFetchSlice {
            records,
            snapshot_id,
            // `MetadataFetchSlice` is a wire-facing DTO of raw `i64` offsets.
            log_start_offset: log_start_offset.0,
            high_watermark: high_watermark.0,
            // Every controller serves this fetch, not only the leader, so the
            // observer is told the quorum's committed offset separately: a
            // follower still catching up serves records only up to its own
            // clamped watermark, and an observer that read that as the
            // quorum's would call itself caught up while both were far behind.
            quorum_high_watermark: self.quorum_high_watermark(),
        }
    }
}
