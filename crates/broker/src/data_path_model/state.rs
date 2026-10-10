//! The state the search enumerates, the actions that move between two states,
//! and the in-sync predicate that both the action generator and the ISR
//! bookkeeping read.
//!
//! `DpState` carries the per-broker logs and the leadership bookkeeping
//! alongside the ghost fields — `committed`, `wal_acked`, `assigned`, `lost`
//! and `elr_trace` — that no broker holds but that the
//! properties are stated over. Its equality and hash go through one projection
//! so that a ghost field added to the struct cannot silently drop out of state
//! identity.

use std::hash::{Hash, Hasher};

use krabka_verified::isr::{
    IsrCandidateFacts, IsrEligibilityFacts, IsrMemberRole, isr_candidate_selected,
};

use super::bounds::{NB, model_offset};

#[derive(Clone, Debug)]
pub(super) struct DpState {
    pub(super) log: [Vec<u8>; NB], // log[b] = Vec<epoch>; offset = index
    pub(super) hwm: i64,           // leader-authoritative high watermark
    pub(super) leader: u8,
    pub(super) leader_epoch: u8,
    pub(super) isr: u8,                   // bitmask over brokers
    pub(super) live: u8,                  // bitmask over brokers
    pub(super) elr: u8, // published KIP-966 eligible-leader replicas, bitmask over brokers
    pub(super) committed: Vec<u8>, // ghost: committed[off] = epoch, for offsets ever <= hwm
    pub(super) wal_acked: Vec<u8>, // ghost: wal_acked[off] = epoch, for offsets made WAL-durable (diskless mode)
    pub(super) seq_next: i64,      // ghost: the image's committed next diskless offset
    pub(super) assigned: Vec<(i64, i64)>, // ghost: half-open assigned ranges
    pub(super) lost: bool,         // ghost: an unclean loss has occurred
    pub(super) elr_trace: u8,      // ghost: what the ELR elections so far did, see `elr_trace`
}

// The bits of `DpState::elr_trace`, the ghost record of what the ELR branch
// of the election rule has done along the path to a state.
//
// The search cannot ask "did an election just happen": a property reads one
// state, and an election is a transition. These three bits are what the
// transition leaves behind for it. `ELR_ELECTED` and `ELR_BEAT_LONGER_LOG`
// exist so the `always` property over `ELR_DROPPED_COMMITTED` cannot pass
// vacuously: a model in which no ELR election is reachable, or in which every
// one of them happens to elect the longest log anyway, would satisfy it
// without ever running the rule it is about.
/// An election was decided by the ELR rule -- `failover_one`'s ELR rung, or
/// `select_leader`'s inside a recovery -- rather than by the ISR or by an
/// election reported as losing data.
pub(super) const ELR_ELECTED: u8 = 1 << 0;
/// One of those elections passed over a strictly longer surviving log, so the
/// ELR rule, not log length, is what chose the leader.
pub(super) const ELR_BEAT_LONGER_LOG: u8 = 1 << 1;
/// One of those elections dropped a committed record — the violation. The
/// election that did it reported itself as losing nothing.
pub(super) const ELR_DROPPED_COMMITTED: u8 = 1 << 2;

/// The whole of `DpState`, grouped only because a tuple wider than twelve
/// implements neither `PartialEq` nor `Hash`: leadership, then the two
/// durability records, then the ghosts an election leaves behind.
type StateProjection = (
    Vec<Vec<u8>>,
    i64,
    (u8, u8, u8, u8, u8),
    (Vec<u8>, Vec<u8>),
    i64,
    Vec<(i64, i64)>,
    (bool, u8),
);

impl DpState {
    pub(super) fn in_sync_nodes(&self) -> Vec<krabka_audit::NodeId> {
        (0..super::bounds::NB_U8)
            .filter(|&broker| super::bounds::has(self.isr, broker))
            .map(|broker| krabka_audit::NodeId(super::bounds::node(broker)))
            .collect()
    }

    pub(super) fn leader_leo(&self) -> i64 {
        model_offset(self.log[usize::from(self.leader)].len())
    }
    fn proj(&self) -> StateProjection {
        (
            self.log.to_vec(),
            self.hwm,
            (
                self.leader,
                self.leader_epoch,
                self.isr,
                self.live,
                self.elr,
            ),
            (self.committed.clone(), self.wal_acked.clone()),
            self.seq_next,
            self.assigned.clone(),
            (self.lost, self.elr_trace),
        )
    }
}
impl PartialEq for DpState {
    fn eq(&self, o: &Self) -> bool {
        self.proj() == o.proj()
    }
}
impl Eq for DpState {}
impl Hash for DpState {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.proj().hash(h);
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) enum Act {
    Produce,
    Assign(u8),    // diskless: the controller reserves this many offsets
    Replicate(u8), // follower b fetches one step from the leader
    AdvanceHwm,
    WalSync, // diskless: make the leader's appended prefix fsync-durable
    ConsumerFetch {
        read_committed: bool,
        fetch_offset: i64,
    },
    Die(u8),
    Revive(u8),
    Failover(u8),  // controller reacts to broker `b` being down
    ExpandIsr(u8), // re-admit a caught-up follower to the ISR
}

/// Where the current leader's epoch starts on its log: Kafka's
/// `Partition.leaderEpochStartOffsetOpt`, the leader's log end offset when it
/// took the epoch over. Every record of the current epoch was appended by
/// this leader after that, so it is the first such record's offset, or the
/// log end when the leader has appended nothing yet.
pub(super) fn leader_epoch_start(s: &DpState) -> i64 {
    let l = &s.log[usize::from(s.leader)];
    model_offset(
        l.iter()
            .position(|&e| e == s.leader_epoch)
            .unwrap_or(l.len()),
    )
}

/// Whether the leader's ISR scan admits follower `b`, which is outside the
/// ISR and alive.
///
/// The model's stand-in for truncation comes first: the follower's log must
/// be an epoch-consistent prefix of the leader's. A real follower's reported
/// progress is only ever post-truncation consistent, so a divergent follower
/// can never appear caught up. The rest is the real
/// [`isr_candidate_selected`] kernel, Kafka's `Partition.needsExpandIsr`: the
/// follower's log end reaches both the high watermark and the start of the
/// leader's epoch. A live broker is unfenced and its fetches carry no broker
/// epoch here, so it is ISR-eligible.
///
/// [`isr_candidate_selected`]: krabka_verified::isr::isr_candidate_selected
pub(super) fn isr_eligible(s: &DpState, b: u8) -> bool {
    let f = &s.log[usize::from(b)];
    let l = &s.log[usize::from(s.leader)];
    f.iter().enumerate().all(|(off, &e)| l.get(off) == Some(&e))
        && isr_candidate_selected(IsrCandidateFacts {
            role: IsrMemberRole::OutOfSyncFollower,
            follower_log_end: model_offset(f.len()),
            leader_log_end: s.leader_leo(),
            leader_high_watermark: s.hwm,
            leader_epoch_start: Some(leader_epoch_start(s)),
            caught_up_within_lag: false,
            eligibility: IsrEligibilityFacts {
                fenced: false,
                shutting_down: false,
                fetch_broker_epoch: Some(-1),
                alive_broker_epoch: Some(0),
            },
        })
}
