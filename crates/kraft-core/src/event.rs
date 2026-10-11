//! Inputs to the consensus state machine.

use uuid::Uuid;

use crate::types::{Epoch, LogOffsetMetadata, NodeId};

/// Where a replica stands in a resigning leader's `EndQuorumEpoch`
/// `PreferredCandidates` list.
///
/// Kafka's `KafkaRaftClient.endEpochElectionBackoff` walks the list for the
/// local replica key and backs the election off by that position, so the most
/// caught-up voter elects first. The default, position 0 of an empty list,
/// elects at once.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SuccessorRank {
    /// The index of the local replica in the list, or the list length when the
    /// list does not name it.
    pub position: u32,
    /// The length of the list.
    pub successors: u32,
}

/// A peer's view of its log tip, carried in Vote/Fetch requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogEnd {
    pub last_epoch: Epoch,
    pub last_offset: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Event {
    /// The election timer fired.
    ElectionTimeout,
    /// The fetch timer fired.
    ///
    /// The follower or observer lost contact with the leader.
    FetchTimeout,
    /// The leader's check-quorum timer fired.
    ///
    /// A majority of the voters has not fetched within the check-quorum
    /// window, so this leader has lost the quorum and resigns (KAFKA-15489).
    CheckQuorumTimeout,
    /// A peer asks us for our vote.
    ReceiveVoteRequest {
        from: NodeId,
        /// Optional cluster identity from the Vote request.
        cluster_id: Option<Uuid>,
        /// The recipient of this Vote, that is, the wire top-level `voterId`.
        ///
        /// KIP-595 and Kafka's `KafkaRaftClient` check that an incoming Vote
        /// targets this node before they consider the grant. They reject a
        /// request that is addressed to a different voter.
        voter_id: NodeId,
        /// KIP-853 directory identity of the receiving voter.
        voter_directory_id: Uuid,
        candidate_epoch: Epoch,
        candidate: NodeId,
        /// KIP-853 directory identity of the candidate.
        candidate_directory_id: Uuid,
        candidate_log_end: LogEnd,
        pre_vote: bool,
    },
    /// A peer answered our Vote.
    ///
    /// The wire does NOT carry the round, that is, pre-vote against real vote.
    /// The candidate infers the round from its own `Prospective` or `Candidate`
    /// role and epoch (KIP-996). This matches Kafka's field-less
    /// `VoteResponse`.
    ReceiveVoteResponse {
        from: NodeId,
        epoch: Epoch,
        vote_granted: bool,
    },
    /// A leader announces its epoch to us.
    ReceiveBeginQuorumEpoch {
        leader_id: NodeId,
        leader_epoch: Epoch,
    },
    /// A resigning leader tells us to start an election.
    ReceiveEndQuorumEpoch {
        leader_id: NodeId,
        leader_epoch: Epoch,
        /// Where this replica stands in the leader's preferred candidates.
        successor_rank: SuccessorRank,
    },
    /// Leader side: a follower fetched at this position.
    ReceiveFetch {
        from: NodeId,
        fetch_epoch: Epoch,
        fetch_offset: i64,
    },
    /// Leader side: a follower asked us for a byte window of a snapshot.
    ///
    /// KIP-630 catch-up carries no fetch offset the leader can score, but it is
    /// still contact from that voter, and Kafka counts it toward check-quorum
    /// exactly as it counts a Fetch.
    ReceiveFetchSnapshot { from: NodeId },
    /// Follower side: the leader answered our Fetch.
    ReceiveFetchResponse {
        leader_id: NodeId,
        leader_epoch: Epoch,
        /// Holds a value when the leader signalled log divergence.
        diverging: Option<LogOffsetMetadata>,
    },
}
