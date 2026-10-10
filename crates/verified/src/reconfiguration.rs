//! `KRaft` voter-reconfiguration admission and result shape.
//!
//! The decision follows the order in which Kafka's `KafkaRaftClient`,
//! `AddVoterHandler`, `RemoveVoterHandler`, `UpdateVoterHandler`, and
//! `LeaderState.maybeAppendUpgradedKRaftVersion` reject a request, and it
//! admits a change only while KIP-853's one-change-at-a-time rule holds: the
//! node leads, its epoch is committed, no earlier change is pending, and no
//! control record is uncommitted.

#[cfg(creusot)]
use creusot_std::prelude::*;

model_types! {
    @proof (derive(std::clone::Clone, Copy, DeepModel));
    /// The KIP-853 control operation being validated.
    pub enum VoterChangeKind {
        Add,
        Remove,
        Update,
        FinalizeKraftVersion,
    }

    /// The receiving node's leadership state for one reconfiguration.
    pub struct ReconfigurationLeadership {
        /// The node is the current `KRaft` leader.
        pub is_leader: bool,
        /// No earlier voter change is waiting for its record to commit.
        pub no_pending_change: bool,
        /// The leader has committed its own epoch, so Kafka's leader HWM exists.
        pub epoch_committed: bool,
    }

    /// The committed voter set the change applies to.
    pub struct CurrentVoterSet {
        pub voter_count: usize,
        /// The committed `kraft.version`.
        pub kraft_version: u16,
        /// The latest voter set and `kraft.version` in the log equal the
        /// committed ones: no control record is uncommitted.
        pub latest_controls_committed: bool,
        /// Every current voter's supported range covers the requested
        /// `kraft.version`. Only a finalization reads it.
        pub all_voters_support_requested: bool,
    }

    /// One requested control operation.
    pub struct VoterChangeRequest {
        pub kind: VoterChangeKind,
        /// The `kraft.version` a finalization asks for.
        pub requested_kraft_version: u16,
    }

    /// How the request's voter key relates to the current voter set.
    pub enum TargetMembership {
        /// No current voter has the request's voter id.
        Absent,
        /// The voter id is current, and its stored directory id is unknown.
        PresentUnknownDirectory,
        /// The voter id is current, with the request's directory id.
        PresentSameDirectory,
        /// The voter id is current, with a different known directory id.
        PresentOtherDirectory,
    }

    /// Facts about the voter an add, remove, or update names. A finalization
    /// names no voter and does not read them.
    pub struct TargetVoter {
        pub membership: TargetMembership,
        /// The voter's supported range covers the committed `kraft.version`.
        pub version_compatible: bool,
        /// The voter has fetched up to the leader's log end.
        pub caught_up: bool,
    }

    /// The exact control-record shape of an admitted change.
    pub struct VoterReconfigurationPlan {
        pub next_voter_count: usize,
        pub next_kraft_version: u16,
        pub write_voters: bool,
        pub write_kraft_version: bool,
        pub preflight_only: bool,
    }

    pub enum VoterReconfigurationDecision {
        NotLeader,
        InProgress,
        EpochUncommitted,
        EmptyCurrentVoterSet,
        UnsupportedKraftVersion,
        DuplicateVoter,
        IncompatibleVoter,
        VoterNotCaughtUp,
        VoterNotFound,
        DirectoryMismatch,
        InvalidVersionTransition,
        Admit(VoterReconfigurationPlan),
    }
}

/// The leadership and committed voter-set snapshot captured for one control change.
pub type ReconfigurationState = (ReconfigurationLeadership, CurrentVoterSet);

mod admitted_plan;
#[cfg(creusot)]
pub use admitted_plan::{
    admitted_plan, is_admit, may_reconfigure, update_key_matches, voter_reconfiguration_rejection,
    voter_set_removes,
};

mod voter_reconfiguration_decision;
pub use voter_reconfiguration_decision::voter_reconfiguration_decision;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod test_support;
