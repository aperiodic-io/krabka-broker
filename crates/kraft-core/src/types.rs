//! Core data types for the `KRaft` consensus state machine (KIP-595/996).
//! Pure, sans-IO: no clock, no wire, no log bytes.

pub use krabka_voters::NodeId;
use {krabka_voters::VoterSet, uuid::Uuid};

#[derive(derive_more::From, derive_more::Into)]
/// A simulated or logical instant in milliseconds.
///
/// The caller always injects the time. The state machine never reads the
/// system clock, which keeps it deterministic.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SimInstant(pub u64);

impl SimInstant {
    #[must_use]
    pub fn saturating_add_ms(self, ms: u64) -> Self { Self(self.0.saturating_add(ms)) }
}

/// Consensus epoch, always non-negative; the wire leader epoch is
/// `krabka_ids::LeaderEpoch`.
pub type Epoch = u32;

/// Identifies a voter by node id and directory id, as Kafka's `ReplicaKey` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicaKey {
    pub id: NodeId,
    pub directory_id: Uuid,
}

/// A log position: an offset together with the leader epoch that produced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogOffsetMetadata {
    pub offset: i64,
    pub epoch: Epoch,
}

/// Read-only view of the local replicated log that the state machine uses.
///
/// Production uses the real `krabka-log`-backed implementation; tests supply a fake.
pub trait LogView {
    /// The log end offset: one offset after the last appended record.
    fn end_offset(&self) -> i64;
    /// Leader epoch of the last appended record. An empty log gives 0.
    fn last_epoch(&self) -> Epoch;
    /// Kafka's `RaftLog.endOffsetForEpoch`: the largest epoch `<= epoch` that
    /// this log holds, and the offset where that epoch ends.
    ///
    /// The end is the start offset of the first epoch newer than `epoch`, or
    /// the log end when there is none. An `epoch` older than every epoch in
    /// the log answers `epoch` itself with the start of the oldest one. An
    /// `epoch` newer than every epoch in the log answers the log end and
    /// [`last_epoch`](Self::last_epoch), which never equals `epoch`, so the
    /// leader treats a fetch at that epoch as diverging.
    fn end_offset_for_epoch(&self, epoch: Epoch) -> LogOffsetMetadata;
}

impl LogOffsetMetadata {
    /// [`LogView::end_offset_for_epoch`] over a log that records the leader
    /// epoch of every record: `epochs[i]` is the epoch of offset `i`.
    ///
    /// The in-memory logs of the simulators and the unit tests use it, so
    /// their divergence lookup follows the same rule as the on-disk log.
    #[must_use]
    pub fn end_of_epoch_in(epochs: &[Epoch], epoch: Epoch) -> Self {
        let end = i64::try_from(epochs.len()).unwrap_or(i64::MAX);
        let last_epoch = epochs.last().copied().unwrap_or(0);
        if epoch > last_epoch {
            return Self {
                offset: end,
                epoch: last_epoch,
            };
        }
        let offset = epochs
            .iter()
            .position(|&record_epoch| record_epoch > epoch)
            .map_or(end, |index| i64::try_from(index).unwrap_or(i64::MAX));
        let found = epochs
            .iter()
            .copied()
            .filter(|&record_epoch| record_epoch <= epoch)
            .max()
            .unwrap_or(epoch);
        Self {
            offset,
            epoch: found,
        }
    }

    /// Kafka's `RaftLog.truncateToEndOffset`: the offset a follower truncates
    /// its `log` to when the leader answers a Fetch with `self` as the
    /// diverging epoch.
    ///
    /// The leader's end offset for the epoch is only an upper bound. The
    /// follower's own copy of that epoch can end earlier, and the records
    /// after it then belong to an epoch the leader does not have. Truncating
    /// to the leader's offset alone would keep them, and the next Fetch would
    /// diverge at the same point again.
    #[must_use]
    pub fn follower_truncation_offset(self, log: &dyn LogView) -> i64 {
        if self.epoch == 0 {
            return self.offset.min(log.end_offset());
        }
        let local = log.end_offset_for_epoch(self.epoch);
        if local.epoch == self.epoch {
            local.offset.min(self.offset)
        } else {
            local.offset
        }
    }
}

/// The durable quorum state: the logical content of the `quorum-state` file.
///
/// This is the in-memory model. The log layer owns the file persistence.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QuorumState {
    pub cluster_id: Uuid,
    /// Finalized `KRaft` protocol feature level. Level 0 uses configured static
    /// voters; level 1 persists membership in Raft control records.
    pub kraft_version: u16,
    pub leader_epoch: Epoch,
    pub leader_id: Option<NodeId>,
    pub voted_key: Option<ReplicaKey>,
    pub voters: VoterSet,
}

impl QuorumState {
    #[must_use]
    pub fn bootstrap(cluster_id: Uuid, voters: VoterSet) -> Self {
        Self {
            cluster_id,
            kraft_version: 0,
            leader_epoch: 0,
            leader_id: None,
            voted_key: None,
            voters,
        }
    }

    /// Majority size for the current voter set: `floor(n/2) + 1`.
    #[must_use]
    pub fn majority(&self) -> usize { krabka_verified::consensus::majority_size(self.voters.len()) }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::simulation_support::voter_set as test_voter_set, assert2::assert};

    /// `floor(n/2) + 1` over the voter-set sizes a cluster actually runs at.
    ///
    /// A remainder agrees with a halving at three voters -- the size most
    /// tests use -- so the sizes on either side of it are what separate them.
    #[yare::parameterized(
        one = { 1, 1 },
        two = { 2, 2 },
        three = { 3, 2 },
        four = { 4, 3 },
        five = { 5, 3 },
        six = { 6, 4 },
    )]
    fn majority_is_half_the_voters_plus_one(voters: usize, want: usize) {
        let ids: Vec<NodeId> = (1..=voters).map(|i| NodeId(i as u64)).collect();
        let qs = QuorumState::bootstrap(uuid::Uuid::nil(), test_voter_set(&ids));
        assert2::assert!(qs.majority() == want, "{voters} voters");
    }

    #[test]
    fn quorum_state_starts_unattached_at_epoch_zero() {
        let voters = test_voter_set(&[NodeId(1), NodeId(2), NodeId(3)]);
        let qs = QuorumState::bootstrap(uuid::Uuid::nil(), voters.clone());
        assert2::assert!(
            qs == QuorumState {
                cluster_id: uuid::Uuid::nil(),
                kraft_version: 0,
                leader_epoch: 0,
                leader_id: None,
                voted_key: None,
                voters,
            }
        );
        assert2::assert!(qs.voters.contains(NodeId(2)));
    }
}
