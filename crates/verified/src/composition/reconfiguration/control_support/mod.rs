#[cfg(creusot)]
use self::spec::{
    common_prefix_reported, control_inputs_coherent, control_prefix_majorities,
    control_record_count, control_supported_request, prefix_count, prefix_grants_agree,
};
#[cfg(creusot)]
use super::spec::membership_matches_change;
#[cfg(creusot)]
use crate::reconfiguration::{VoterReconfigurationPlan, admitted_plan};
use {
    super::{
        super::{constructed_voter_reconfiguration, reconfigured_majorities_overlap},
        CommittedControl, SupportedControl,
    },
    crate::{
        raft::{
            advance_high_watermark, control_history_frontier, frontier_reaches,
            in_half_open_window, metadata_record_offset_deltas,
        },
        reconfiguration::{ReconfigurationState, TargetVoter, VoterChangeRequest},
        storage::local_append_coordinates,
    },
    creusot_std::prelude::*,
};

mod commit;
mod prepare;
#[cfg(creusot)]
mod spec;

pub(crate) use {
    commit::reconfiguration_control_commit_waiter, prepare::reconfiguration_control_prefix_support,
};
