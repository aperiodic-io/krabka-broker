//! The command and inbound vocabulary that drives one turn of the engine loop.
//!
//! [`Command`] is everything that arrives on the engine's mpsc: an inbound peer
//! RPC ([`Inbound`]), an injected core [`Event`], a decoded Fetch or
//! `FetchSnapshot` response, a [`TimerTick`], or a handle-facing operation. The
//! reply payloads that those handle ops carry back, [`MetadataFetchSlice`] and
//! [`QuorumStateSnapshot`], live here with them.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::{
    DelegationTokenMutation, SubmitChangeResult,
    error::RaftError,
    kraft::{
        event::Event,
        transport::PeerApi,
        types::{Epoch, NodeId, ReplicaKey},
    },
    reconfig::{ReconfigOutcome, VoterChange},
};

/// An inbound KIP-595 RPC body, the request version its header carried, and a
/// oneshot to reply on.
///
/// The event loop decodes the body at `version` into a core [`Event`], runs
/// it, and encodes the produced response at the same version back onto
/// `reply`, as Kafka answers every request at the version it arrived at.
#[derive(Debug)]
pub enum Inbound {
    Vote {
        req: Bytes,
        version: i16,
        reply: oneshot::Sender<Bytes>,
    },
    BeginQuorumEpoch {
        req: Bytes,
        version: i16,
        reply: oneshot::Sender<Bytes>,
    },
    EndQuorumEpoch {
        req: Bytes,
        version: i16,
        reply: oneshot::Sender<Bytes>,
    },
    Fetch {
        req: Bytes,
        version: i16,
        reply: oneshot::Sender<Bytes>,
    },
    FetchSnapshot {
        req: Bytes,
        version: i16,
        reply: oneshot::Sender<Bytes>,
    },
}

impl Inbound {
    /// The inbound RPC of peer api `api`.
    #[must_use]
    pub fn new(api: PeerApi, req: Bytes, version: i16, reply: oneshot::Sender<Bytes>) -> Self {
        match api {
            PeerApi::Fetch => Self::Fetch {
                req,
                version,
                reply,
            },
            PeerApi::Vote => Self::Vote {
                req,
                version,
                reply,
            },
            PeerApi::BeginQuorumEpoch => Self::BeginQuorumEpoch {
                req,
                version,
                reply,
            },
            PeerApi::EndQuorumEpoch => Self::EndQuorumEpoch {
                req,
                version,
                reply,
            },
            PeerApi::FetchSnapshot => Self::FetchSnapshot {
                req,
                version,
                reply,
            },
        }
    }

    /// The peer api, request body, request version and reply oneshot.
    #[must_use]
    pub fn into_parts(self) -> (PeerApi, Bytes, i16, oneshot::Sender<Bytes>) {
        let api = match &self {
            Self::Fetch { .. } => PeerApi::Fetch,
            Self::Vote { .. } => PeerApi::Vote,
            Self::BeginQuorumEpoch { .. } => PeerApi::BeginQuorumEpoch,
            Self::EndQuorumEpoch { .. } => PeerApi::EndQuorumEpoch,
            Self::FetchSnapshot { .. } => PeerApi::FetchSnapshot,
        };
        let (Self::Fetch {
            req,
            version,
            reply,
        }
        | Self::Vote {
            req,
            version,
            reply,
        }
        | Self::BeginQuorumEpoch {
            req,
            version,
            reply,
        }
        | Self::EndQuorumEpoch {
            req,
            version,
            reply,
        }
        | Self::FetchSnapshot {
            req,
            version,
            reply,
        }) = self;
        (api, req, version, reply)
    }
}

/// Everything that arrives on the engine's mpsc and drives one turn of the
/// loop.
pub enum Command {
    /// An inbound peer RPC with a oneshot to reply on.
    Inbound(Inbound),
    /// Injects a core [`Event`] directly. This is the test and driver entry
    /// point. The loop also uses it to feed peer-RPC responses back to itself
    /// as the matching `Receive*Response` event, which is the fire-and-forget
    /// feedback path.
    Event(Event),
    /// A Fetch RESPONSE the follower received from the leader. Other peer
    /// responses decode to a pure core event, but a Fetch response carries log
    /// records. The follower must truncate, append, and apply those records
    /// BEFORE it feeds the `ReceiveFetchResponse` event to the core. This
    /// response therefore gets its own command instead of the pure `Event`
    /// feedback path.
    FetchResponse {
        /// The leader that answered, which is the responder peer.
        from: NodeId,
        /// The raw encoded [`wire::PeerResponse::Fetch`](super::wire::PeerResponse::Fetch)
        /// body.
        body: Bytes,
    },
    /// A `FetchSnapshot` RESPONSE the follower received from the leader. It
    /// carries snapshot bytes that the follower reassembles before it resumes.
    /// This mirrors the dedicated command path of `FetchResponse`.
    FetchSnapshotResponse { from: NodeId, body: Bytes },
    /// An election, fetch, or heartbeat timer fired. The loop maps it to the
    /// right core event after it reads the liveness state. A fetch tick
    /// re-polls instead of electing, unless the leader has been missed enough
    /// times.
    Timer(TimerTick),
    /// Handle op: append and commit a metadata batch as the leader. It replies
    /// once the batch is committed and applied, or it replies with a
    /// rejection.
    SubmitChange {
        records: Vec<krabka_metadata::MetadataRecord>,
        expected_image: Option<Arc<krabka_metadata::MetadataImage>>,
        reply: oneshot::Sender<Result<SubmitChangeResult, RaftError>>,
    },
    /// Handle op: compare and mutate delegation-token state in one engine turn.
    SubmitDelegationTokenMutations {
        mutations: Vec<DelegationTokenMutation>,
        reply: oneshot::Sender<Result<SubmitChangeResult, RaftError>>,
    },
    /// Handle op: append a KIP-853 control batch and optionally wait for it to
    /// commit under the new voter set.
    Reconfigure {
        change: VoterChange,
        reply: oneshot::Sender<Result<ReconfigOutcome, RaftError>>,
    },
    /// Handle op: snapshot the current image to a checkpoint.
    TriggerSnapshot {
        reply: oneshot::Sender<Result<(), RaftError>>,
    },
    /// Handle op: read a structured snapshot of consensus state for
    /// `DescribeQuorum`.
    QuorumStateSnapshot {
        reply: oneshot::Sender<QuorumStateSnapshot>,
    },
    /// Handle op: read a committed `__cluster_metadata` slice for an observer's
    /// `API_KEY_METADATA_FETCH` (1004), encoded as Kafka record batches.
    MetadataFetch {
        fetch_offset: i64,
        max_size: krabka_units::ByteSize,
        /// The observer that asked, when it is a replica. A leader tracks its
        /// progress under this key.
        replica: Option<ReplicaKey>,
        reply: oneshot::Sender<MetadataFetchSlice>,
    },
    /// Test-only: append a metadata batch to the log, the same way the
    /// leader's `submit_change` does, and drive the commit through the real
    /// apply pipeline. Replies with the appended base offset.
    #[cfg(test)]
    TestAppendAndCommit {
        records: Vec<krabka_metadata::MetadataRecord>,
        reply: oneshot::Sender<i64>,
    },
    /// Stop the loop.
    Shutdown,
}

/// A committed-range read result for the observer metadata-fetch path (1004).
///
/// `records` is concatenated Kafka `RecordBatch`es, one for each committed log
/// batch in `[fetch_offset, high_watermark)`. The offsets are `KraftLog`
/// offsets. A `fetch_offset` below `log_start_offset` reads no records at all:
/// the slice then carries `snapshot_id`, and the observer installs that
/// snapshot before it fetches again.
#[derive(Debug, Clone)]
pub struct MetadataFetchSlice {
    pub records: bytes::Bytes,
    /// The KIP-630 snapshot `(end_offset, epoch)` that replaces `records` when
    /// the observer asked for an offset this node has already pruned away.
    /// `records` is empty whenever this is set.
    pub snapshot_id: Option<(i64, i32)>,
    pub log_start_offset: i64,
    pub high_watermark: i64,
    /// Highest offset the quorum has committed, as this node last heard it.
    ///
    /// This node's own `high_watermark` bounds what it can serve; this says
    /// how much more there is. They differ whenever the node answering the
    /// fetch is itself a follower that is still catching up, and an observer
    /// measures its own lag against this one.
    pub quorum_high_watermark: i64,
}

/// Which timer fired. The loop interprets the tick against the current role
/// and liveness state, and does not map it one-to-one to a core event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerTick {
    /// The election timer's deadline passed.
    Election,
    /// The fetch timer's deadline passed. This is the follower and observer
    /// poll watchdog.
    Fetch,
    /// The leader heartbeat interval ticked.
    Heartbeat,
    /// The leader's check-quorum deadline passed. This is the leader-side
    /// watchdog: a majority of the voters has stopped fetching, so the leader
    /// resigns instead of holding an epoch it has lost.
    CheckQuorum,
}

/// One observer row of `DescribeQuorum`: a replica the leader has seen fetch
/// that is not a voter, with what the leader tracks for it. A timestamp is -1
/// while the leader has not recorded that event, as in Kafka.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObserverReplica {
    pub id: NodeId,
    /// The observer's directory id, nil when its fetches carry none.
    pub directory_id: uuid::Uuid,
    /// The offset of the observer's last valid fetch.
    pub log_end_offset: i64,
    /// Wall-clock milliseconds of its last valid fetch.
    pub last_fetch_ms: i64,
    /// Wall-clock milliseconds it was last caught up with the leader's log.
    pub last_caught_up_ms: i64,
}

/// A structured, node-local snapshot of consensus state for the handle, which
/// serves the broker's `DescribeQuorum` admin view.
///
/// This is the engine's own view. The handle maps it into the public
/// `crate::controller::QuorumState`.
#[derive(Debug, Clone)]
pub struct QuorumStateSnapshot {
    pub leader_id: Option<NodeId>,
    pub leader_epoch: Epoch,
    pub high_watermark: i64,
    /// Append timestamp, in Unix epoch milliseconds, of the record at
    /// `high_watermark - 1` when it is still available in the local log (or
    /// the installed snapshot's last contained record timestamp). `0` means
    /// no applied record has a known timestamp.
    pub last_applied_record_timestamp_ms: i64,
    /// Lifetime average elapsed microseconds of completed controller apply
    /// passes that processed newly committed records.
    pub pending_record_processing_time_us_avg: u64,
    /// Lifetime maximum elapsed microseconds of completed controller apply
    /// passes that processed newly committed records.
    pub pending_record_processing_time_us_max: u64,
    /// Highest offset the quorum has committed, as this node last heard it:
    /// this node's own high watermark on a leader, and the leader-reported
    /// high watermark on a follower that is still replaying the log.
    ///
    /// `high_watermark` alone cannot answer "how far behind is this node",
    /// because a follower clamps it to its own log end. A follower that has
    /// replicated 10 of the quorum's 10 000 committed records reports
    /// `high_watermark == 10` and `quorum_high_watermark == 10_000`.
    pub quorum_high_watermark: i64,
    pub log_end_offset: i64,
    /// Log-start offset. It rises past 0 once the log has been pruned below a
    /// snapshot under KIP-630.
    pub log_start_offset: i64,
    pub voters: krabka_metadata::VoterSet,
    /// Directory identity voted for in the current epoch, if any.
    pub voted_directory_id: Option<uuid::Uuid>,
    /// The replicas that fetch from this leader without holding a vote and
    /// fetched within the last five minutes (Kafka's `observerStates`), one per
    /// `(id, directory id)` key. Empty unless this node leads.
    pub observers: Vec<ObserverReplica>,
    /// Per-voter fetch offset, populated on the leader.
    pub per_replica_fetch_offset: std::collections::BTreeMap<NodeId, i64>,
    /// Per-voter last fetch timestamp in wall-clock milliseconds.
    pub per_replica_last_fetch_ms: std::collections::BTreeMap<NodeId, i64>,
    /// Per-voter last caught-up timestamp in wall-clock milliseconds.
    pub per_replica_last_caught_up_ms: std::collections::BTreeMap<NodeId, i64>,
    /// Whether this node currently leads the metadata quorum.
    pub is_leader: bool,
    /// Raft consensus state name (leader, follower, candidate, observer).
    pub current_state: &'static str,
}
