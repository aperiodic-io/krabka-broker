mod control_support;
mod membership;
mod quorum;
mod spec;

pub(super) use control_support::{
    reconfiguration_control_commit_waiter, reconfiguration_control_prefix_support,
};
pub(super) use membership::constructed_voter_reconfiguration;
pub(super) use quorum::reconfigured_majorities_overlap;

use crate::reconfiguration::VoterReconfigurationPlan;

/// Admitted plan, successor voters, control offsets, and actual-prefix support.
pub(super) type SupportedControl = Option<(
    VoterReconfigurationPlan,
    Vec<u64>,
    Vec<i32>,
    Option<(i64, (usize, usize), u64)>,
)>;

// Rows carry (absolute offset, is KRaftVersion, committed). The frontier carries
// (exclusive batch end, high watermark, committed row count, waiter ready, common voter).
pub(super) type CommittedControl = Option<(
    VoterReconfigurationPlan,
    Vec<u64>,
    Vec<(i64, bool, bool)>,
    Option<(i64, i64, usize, bool, u64)>,
)>;
