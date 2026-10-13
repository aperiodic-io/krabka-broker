//! The async `KraftController` consensus engine: a single owning tokio task
//! holds all consensus state (the [`QuorumStateMachine`] core, the
//! [`KraftLog`], and the published [`MetadataImage`]) and turns inbound
//! commands/RPCs into core [`Event`]s whose [`Action`]s it executes.
//!
//! Ownership model: one task owns the `Engine`; everything else talks to it
//! over an mpsc of [`Command`]. The public [`KraftController`] handle is a
//! cheap clone holding the command sender plus the `watch` receivers. This is
//! a single-owner actor pattern; the engine is entirely ours.
//!
//! ## Concurrency / no-inline-await invariant
//!
//! The loop is single-threaded over all consensus state, so it never blocks on
//! a peer RPC. Each `Send*` [`Action`] is dispatched **fire-and-forget**: a
//! [`tokio::spawn`]ed task calls [`PeerSender::send`], decodes the response
//! body into the matching `Receive*Response` [`Event`], and posts it back to
//! the loop via a clone of the command sender. This is critical for the
//! in-process multi-node sim, where engines RPC each other reciprocally — a
//! loop that awaited a send inline would deadlock.
//!
//! ## Timers & liveness
//!
//! The loop drives a real monotonic clock and `select!`s over the mpsc plus an
//! election timer, a fetch timer, and a leader heartbeat interval:
//! - on a role transition the now-irrelevant timer is cancelled (a follower has
//!   no election timer; a leader has no fetch timer and runs the heartbeat);
//! - a fetch-timer expiry while the leader is still reachable RE-POLLS
//!   (`SendFetch`), it does not elect; only the configured consecutive
//!   misses feed `Event::FetchTimeout` to start an election;
//! - the leader re-broadcasts `BeginQuorumEpoch` to voters each heartbeat tick.

use {
    crate::{
        OffsetReservation, SubmitChangeResult,
        config::{
            ControllerFetchMissLimit, DEFAULT_METADATA_RAFT_FETCH_MAX, MetadataLogConfig,
            MetadataRaftCommandQueueCapacity, MetadataRaftFetchMax,
        },
        error::{MetadataReplayError, RaftError},
        kraft::{
            action::{Action, TimerKind},
            core::QuorumStateMachine,
            event::{Event, LogEnd},
            log::KraftLog,
            role::{ReplicaProgress, Role},
            snapshot_fetch::{MetadataSnapshotFetchMax, SnapshotFetchState, SnapshotFetchStep},
            transport::{
                Command, Inbound, MetadataFetchSlice, PeerSender, QuorumStateSnapshot, TimerTick,
                api_key, wire,
            },
            types::{Epoch, LogView, NodeId, QuorumState, ReplicaKey, SimInstant},
        },
        reconfig::ReconfigOutcome,
    },
    base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD},
    krabka_ids::Offset,
    krabka_metadata::{
        MetadataImage, MetadataRecord, VoterSet, VotersRecord, from_kraft_value, to_kraft_values,
    },
    krabka_protocol::{
        Decode, Encode,
        owned::{
            k_raft_version_record::KRaftVersionRecord as WireKRaftVersionRecord,
            voters_record::{
                Endpoint as WireVoterEndpoint, KRaftVersionFeature as WireKRaftVersionFeature,
                Voter as WireVoter, VotersRecord as WireVotersRecord,
            },
        },
        records::{
            Record, RecordBatch,
            metadata::control::{ControlRecord, ControlRecordType, control_record_key},
        },
    },
    krabka_units::{
        fmt::Human as _,
        prelude::{ByteSize, Time, TimeExt as _},
    },
    std::{collections::BTreeMap, path::PathBuf, sync::Arc},
    tokio::{
        sync::{mpsc, oneshot, watch},
        time::{Duration, Instant},
    },
    uuid::Uuid,
};

mod activation;
mod apply;
/// The KIP-630 `.checkpoint` artifacts under a node's metadata directory. It
/// is public because a broker-only observer keeps its `__cluster_metadata`
/// snapshot in the same on-disk layout, and reads and writes it with these
/// helpers rather than with a second file format of its own.
pub mod checkpoint;
mod cleaning;
mod control_state;
mod engine_loop;
mod handle;
mod idle;
mod inbound;
mod offsets;
mod peer_rpc;
mod queries;
mod quorum_requests;
mod quorum_state_file;
mod reconfiguration;
mod records;
mod recovery;
mod replication;
mod snapshotting;
mod startup;
mod submit;
mod timing;

pub use self::{
    activation::Activation,
    control_state::control_batch_image_records,
    records::{decode_committed_value, is_kip835_noop},
};

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests_activation;
#[cfg(test)]
mod tests_apply;
#[cfg(test)]
mod tests_broker_registration;
#[cfg(test)]
mod tests_cleaning;
#[cfg(test)]
mod tests_control_records;
#[cfg(test)]
mod tests_downgrade;
#[cfg(test)]
mod tests_fetch;
#[cfg(test)]
mod tests_fetch_requests;
#[cfg(test)]
mod tests_lifecycle;
#[cfg(test)]
mod tests_offsets;
#[cfg(test)]
mod tests_quorum_requests;
#[cfg(test)]
mod tests_recovery;
#[cfg(test)]
mod tests_replay_fault;
#[cfg(test)]
mod tests_snapshotting;
#[cfg(test)]
mod tests_submit;
#[cfg(test)]
mod tests_timing;
#[cfg(test)]
mod tests_truncation;

/// Filename of the node-local durable quorum-state file.
const QUORUM_STATE_FILE: &str = "quorum-state";

/// How often the engine cleans the metadata log by its retention limits:
/// once a minute, the delay of Kafka's `RaftMetadataLogCleanerManager`.
const METADATA_LOG_CLEAN_INTERVAL: Duration = Duration::from_secs(60);

/// All consensus state owned by the single engine task.
struct Engine {
    me: NodeId,
    core: QuorumStateMachine,
    log: KraftLog,
    image: MetadataImage,
    peers: Arc<dyn PeerSender>,
    /// Publishes the latest applied [`MetadataImage`] to readers.
    image_tx: watch::Sender<Arc<MetadataImage>>,
    /// Publishes the current leader id (None while unknown / election running).
    leader_tx: watch::Sender<Option<NodeId>>,
    /// Publishes a structured consensus snapshot for the handle's synchronous
    /// `quorum_state()` (the broker's `DescribeQuorum` reads it without an mpsc
    /// round-trip).
    quorum_tx: watch::Sender<QuorumStateSnapshot>,
    /// Clone of the command sender, handed to fire-and-forget send tasks so
    /// they can post the decoded `Receive*Response` event back to the loop.
    cmd_tx: mpsc::Sender<Command>,
    /// Publishes the fault that stops the engine, which Kafka halts the
    /// process over: the failure of the metadata log directory, once a write
    /// to it returned an I/O error (KIP-858), or a controller activation that
    /// failed. See [`Engine::publish_fault`].
    fault_tx: watch::Sender<Option<String>>,
    /// The metadata partition directory, `__cluster_metadata-0`: it holds the
    /// log segments, the KIP-630 checkpoints and the quorum-state file, as
    /// Kafka's does.
    data_dir: PathBuf,
    /// Monotonic clock base: `SimInstant(ms)` is `(now - base).as_millis()`.
    clock_base: Instant,
    /// Base election timeout (varied per node by the caller for liveness).
    election_timeout: Time,
    heartbeat_interval: Option<Time>,
    controller_fetch_miss_limit: ControllerFetchMissLimit,
    metadata_raft_fetch_max: MetadataRaftFetchMax,
    /// Pending timer deadlines as `tokio::time::Instant`s. `None` = disarmed.
    election_at: Option<Instant>,
    fetch_at: Option<Instant>,
    /// Leader-side check-quorum deadline. Only a leader over a multi-voter
    /// quorum arms it; every other role runs with it disarmed.
    check_quorum_at: Option<Instant>,
    /// Consecutive fetch misses while still believing in a leader.
    fetch_misses: u32,
    /// Discovery Fetches sent while no leader is known. The next one goes to
    /// the discovery peer this count selects, so a dead peer, or this node's
    /// own address in its bootstrap servers, cannot hold discovery for good.
    discovery_attempts: usize,
    /// Outstanding `submit_change` waiters keyed by the end offset they need
    /// committed+applied. Resolved (Ok or per-record rejection) on apply.
    commit_waiters: Vec<CommitWaiter>,
    /// Whether we held leadership as of the last reconcile, and at what epoch.
    /// Used to detect a leadership-loss edge (Leader → non-Leader, or a
    /// leader-epoch bump while still nominally leading) so we can fail parked
    /// `submit_change` waiters instead of leaving them hung (FIX 1).
    was_leader: bool,
    held_epoch: Epoch,
    /// The end offset of the latest batch this leader appended that
    /// registers, amends or unregisters each broker, with the leader epoch it
    /// was appended at. A registration change is decided against the
    /// committed image, so it waits while such a batch is uncommitted: see
    /// [`Engine::registration_change_must_wait`].
    registration_writes: BTreeMap<krabka_metadata::NodeId, (Epoch, Offset)>,
    /// Snapshot every this many committed records past the last snapshot
    /// (KIP-630). The cleaning decides when the log below a snapshot goes.
    /// `0` disables this trigger.
    snapshot_interval_records: u64,
    /// `metadata.log.max.record.bytes.between.snapshots` (KIP-630). `0`
    /// disables the byte-size cap.
    max_bytes_between_snapshots: ByteSize,
    /// `metadata.log.max.snapshot.interval.ms` (KIP-630). `0` disables the
    /// time-based cap.
    max_snapshot_interval: Time,
    metadata_snapshot_fetch_max: MetadataSnapshotFetchMax,
    /// Kafka's `MetadataLogConfig`: the retention limits the cleaning applies
    /// and the KIP-835 idle interval.
    metadata_log: MetadataLogConfig,
    /// When the leader appends its next KIP-835 `NoOpRecord`. `None` while
    /// this node does not lead, or when `metadata.max.idle.interval.ms` is
    /// zero.
    noop_at: Option<Instant>,
    /// When the next periodic metadata log cleaning runs: Kafka's
    /// `RaftMetadataLogCleanerManager`, which cleans once a minute.
    clean_at: Instant,
    /// HWM at which the last checkpoint was written (and the log pruned to).
    /// Seeded from the recovered checkpoint on `open`.
    last_snapshot_end_offset: Offset,
    /// The `last_contained_log_timestamp` the most recent checkpoint this node
    /// wrote or installed was stamped with, seeded on `open` from the
    /// recovered checkpoint's own header. It is the fallback for a snapshot
    /// whose boundary has already been pruned away: the log below it is gone,
    /// so the create-time of the last record it contains is only knowable from
    /// the checkpoint that already named it, and a snapshot rewritten at an
    /// unchanged boundary contains exactly the same last record.
    last_snapshot_timestamp_ms: i64,
    /// Timestamp of the last record the metadata image has applied. This can
    /// lag the quorum high watermark when replay has not caught up yet.
    last_applied_record_timestamp_ms: i64,
    /// Aggregate durations of completed applies that consumed newly
    /// committed records. The average/max are lifetime values for this process.
    pending_record_processing_time_us_total: u128,
    pending_record_processing_time_samples: u64,
    pending_record_processing_time_us_max: u64,
    /// `self.now()` (ms) at which the last checkpoint was written. Seeded to
    /// `0` on construction, which is `clock_base`'s own instant, so a
    /// restarted node measures the time-based cap from its own start rather
    /// than immediately firing.
    last_snapshot_at_ms: u64,
    /// Verbatim log bytes applied since the last checkpoint, tracked
    /// incrementally as batches are applied rather than re-measured by
    /// reading the log: a read bounded by the byte cap can under-count
    /// (a batch that would cross the cap does not fit the remaining budget
    /// and is excluded), and an unbounded read would rescan the whole
    /// pending backlog on every engine tick. Reset to `0` on every snapshot,
    /// including `0` on a fresh restart, so a restart under-counts until
    /// enough new records flow in — the same restart tradeoff
    /// `last_snapshot_at_ms` already makes for the time-based cap.
    bytes_since_snapshot: u64,
    /// The first committed metadata-version downgrade whose mandatory exact
    /// checkpoint, reload, and prune has not completed locally yet. Capturing
    /// the image and post-record boundary prevents later committed records from
    /// changing the checkpoint retried after an I/O failure.
    downgrade_snapshot_pending: Option<PendingDowngradeSnapshot>,
    #[cfg(test)]
    downgrade_snapshot_failures_remaining: usize,
    /// In-flight follower snapshot reassembly, if any.
    snapshot_fetch: Option<SnapshotFetchState>,
    /// Set when a snapshot was just installed; the next follower Fetch carries
    /// this epoch (the log is empty at the snapshot boundary so it has no epoch
    /// of its own). Cleared once a normal fetch advances the log.
    installed_snapshot_epoch: Option<Epoch>,
    /// KIP-853 control records between the latest snapshot and the LEO. The
    /// newest set drives consensus immediately; the committed projection is
    /// mirrored into `MetadataImage` for broker/admin readers.
    controls: KraftControlState,
    /// This replica's directory id, sent as `ReplicaDirectoryId` in its Fetch
    /// while it is not yet in the voter set, so the leader can tell which
    /// `(id, directory)` observer is asking to be added.
    directory_id: uuid::Uuid,
    /// Fetch progress of every replica that fetches from this leader without a
    /// matching voter key, keyed by `(id, directory id)`: Kafka's
    /// `LeaderState.observerStates`. Only a valid fetch moves an entry, an
    /// observer silent for five minutes is dropped from what
    /// `DescribeQuorum` reports, and the map is emptied whenever this node is
    /// not the leader, as a fresh `LeaderState` starts with none.
    observers: BTreeMap<ReplicaKey, ReplicaProgress>,
    /// Base system time for mapping `SimInstant` to wall-clock time.
    wall_clock_base: std::time::SystemTime,
    /// Highest high watermark any leader has reported to this node in a Fetch
    /// response. A follower clamps its own HWM to its log end, so this is the
    /// only place the quorum's committed offset survives while the follower is
    /// still catching up. It is monotonic: a leader's HWM is committed by
    /// definition, so a later leader can never be behind an earlier one.
    leader_reported_hwm: i64,
    /// At most one voter/version control operation may be uncommitted.
    pending_reconfig: Option<PendingReconfig>,
    /// Fetches that found nothing new and wait for new records, a high
    /// watermark change or their `MaxWaitMs`: Kafka's `fetchPurgatory`.
    fetch_purgatory: Vec<inbound::ParkedFetch>,
    /// What this node writes when it becomes leader of a log that holds no
    /// `metadata.version`. See [`Engine::complete_activation`].
    activation: Activation,
    /// The reason the last activation of this node failed, which stops the
    /// engine. `None` while no activation has failed.
    activation_fault: Option<String>,
    /// The committed record this node could not replay, which stops the
    /// engine. `None` while every committed record has replayed.
    replay_fault: Option<MetadataReplayError>,
}

#[derive(Clone)]
struct PendingDowngradeSnapshot {
    image: MetadataImage,
    end_offset: Offset,
    epoch: i32,
}

#[derive(Debug, Clone)]
struct KraftControlState {
    voter_history: BTreeMap<i64, VoterSet>,
    version_history: BTreeMap<i64, u16>,
    committed_voters: VoterSet,
    committed_version: u16,
}

struct PendingReconfig {
    need_offset: Offset,
    reply: Option<oneshot::Sender<Result<ReconfigOutcome, RaftError>>>,
    removed_local_leader: bool,
}

/// A parked `submit_change`: it completes once the HWM reaches `need_offset`
/// AND the records have been run through `validate`/`apply`.
struct CommitWaiter {
    /// Base (append) offset of this waiter's batch. Its appended range is
    /// `[base_offset, need_offset)`; a committed-record rejection only attaches
    /// to a waiter whose range actually contains the failing offset (FIX 2).
    base_offset: Offset,
    need_offset: Offset,
    /// First per-record rejection observed at apply time, if any.
    rejection: Option<RaftError>,
    /// The names of the topics that this batch creates. A name is here when
    /// the batch has a `V1Topic` for it and the applied image did not have
    /// the topic at submit time. While the waiter is parked, a second submit
    /// that creates one of these names gets `TopicExists`.
    creates: Vec<String>,
    result: SubmitChangeResult,
    reply: oneshot::Sender<Result<SubmitChangeResult, RaftError>>,
}

/// Cheap, cloneable handle to the running engine: holds the command sender and
/// the `watch` receivers the broker/handle read.
#[derive(Clone)]
pub struct KraftController {
    cmd_tx: mpsc::Sender<Command>,
    image_rx: watch::Receiver<Arc<MetadataImage>>,
    fault_rx: watch::Receiver<Option<String>>,
    leader_rx: watch::Receiver<Option<NodeId>>,
    quorum_rx: watch::Receiver<QuorumStateSnapshot>,
    peers: Arc<dyn PeerSender>,
    me: NodeId,
}

/// Configuration to build a [`KraftController`].
pub struct KraftConfig {
    pub me: NodeId,
    pub cluster_id: Uuid,
    /// This replica's directory id, from its `meta.properties`.
    pub directory_id: Uuid,
    pub initial_state: QuorumState,
    pub election_timeout: Time,
    pub heartbeat_interval: Option<Time>,
    pub controller_fetch_miss_limit: ControllerFetchMissLimit,
    pub metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity,
    pub metadata_raft_fetch_max: MetadataRaftFetchMax,
    pub peers: Arc<dyn PeerSender>,
    /// Snapshot once committed offset advances this many records past the
    /// last snapshot. The cleaning decides when the log below a snapshot goes.
    /// `0` disables this trigger.
    pub snapshot_interval_records: u64,
    /// `metadata.log.max.record.bytes.between.snapshots` (KIP-630). `0`
    /// disables the byte-size cap.
    pub max_bytes_between_snapshots: ByteSize,
    /// `metadata.log.max.snapshot.interval.ms` (KIP-630). `0` disables the
    /// time-based cap.
    pub max_snapshot_interval: Time,
    /// Validated maximum metadata snapshot size this follower will fetch.
    pub metadata_snapshot_fetch_max: MetadataSnapshotFetchMax,
    /// How the metadata log rolls, how long it keeps the prefix a snapshot
    /// covers, and how often this node appends a KIP-835 `NoOpRecord` while
    /// it leads.
    pub metadata_log: MetadataLogConfig,
    /// What this node writes when it becomes leader of a log that holds no
    /// `metadata.version`: the bootstrap records and the static
    /// `min.insync.replicas`.
    pub activation: Activation,
}
