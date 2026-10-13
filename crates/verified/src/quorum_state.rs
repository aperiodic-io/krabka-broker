//! `KRaft` quorum-state persistence admission.

use creusot_std::prelude::ensures;
#[cfg(creusot)]
use creusot_std::prelude::{DeepModel, logic};

model_types! {
    @proof (derive(std::clone::Clone, Copy, DeepModel));
    /// Whether an in-memory quorum state can be represented exactly by Kafka's
    /// signed, versioned JSON fields.
    pub enum QuorumStateWriteDecision {
        Reject,
        Accept,
    }
}

/// The in-memory quorum state has an exact representation in Kafka's
/// quorum-state JSON: a supported schema version (0 or 1), and a term, leader
/// ID, and voted ID that fit their signed `int32` fields when present. Schema
/// v0 also lists the voter IDs, so they must fit too; schema v1 does not
/// carry them.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn fits_signed_json(
    kraft_version: u16,
    leader_epoch: u32,
    has_leader: bool,
    leader_id: u64,
    has_vote: bool,
    voted_id: u64,
    all_voter_ids_fit: bool,
) -> bool {
    pearlite! {
        kraft_version@ <= 1
            && leader_epoch@ <= i32::MAX@
            && (!has_leader || leader_id@ <= i32::MAX@)
            && (!has_vote || voted_id@ <= i32::MAX@)
            && (kraft_version@ == 1 || all_voter_ids_fit)
    }
}

/// Reject states that would be clamped, lose an identity, or select an
/// unsupported schema when persisted. The one ensures pins both variants:
/// `Accept` exactly when `fits_signed_json` holds.
#[ensures((result == QuorumStateWriteDecision::Accept) == fits_signed_json(
    kraft_version,
    leader_epoch,
    has_leader,
    leader_id,
    has_vote,
    voted_id,
    all_voter_ids_fit,
))]
#[must_use]
pub fn quorum_state_write_decision(
    kraft_version: u16,
    leader_epoch: u32,
    has_leader: bool,
    leader_id: u64,
    has_vote: bool,
    voted_id: u64,
    all_voter_ids_fit: bool,
) -> QuorumStateWriteDecision {
    if kraft_version > 1
        || leader_epoch > 2_147_483_647
        || (has_leader && leader_id > 2_147_483_647)
        || (has_vote && voted_id > 2_147_483_647)
        || (kraft_version == 0 && !all_voter_ids_fit)
    {
        QuorumStateWriteDecision::Reject
    } else {
        QuorumStateWriteDecision::Accept
    }
}

model_types! {
    @proof (derive(std::clone::Clone, Copy, DeepModel));
    /// How a parsed quorum-state record may restore the durable vote.
    pub enum QuorumStateLoadDecision {
        Reject,
        RestoreNoVote,
        RestoreVote,
    }
}

/// Admit only schema v0 or v1 records with a nonnegative term, the exact vote
/// sentinel, and valid version-specific fields. Schema v1's no-vote sentinel
/// must carry the nil directory sentinel written by the encoder.
#[ensures((result == QuorumStateLoadDecision::Reject) == (
    (data_version@ != 0 && data_version@ != 1)
        || leader_epoch@ < 0
        || voted_id@ < -1
        || !version_fields_valid
        || (data_version@ == 1 && !voted_directory_valid)
        || (data_version@ == 1 && voted_id@ == -1 && !voted_directory_nil)
))]
#[ensures((result == QuorumStateLoadDecision::RestoreNoVote) == (
    (data_version@ == 0 || data_version@ == 1)
        && leader_epoch@ >= 0
        && voted_id@ == -1
        && version_fields_valid
        && (data_version@ == 0 || (voted_directory_valid && voted_directory_nil))
))]
#[ensures((result == QuorumStateLoadDecision::RestoreVote) == (
    (data_version@ == 0 || data_version@ == 1)
        && leader_epoch@ >= 0
        && voted_id@ >= 0
        && version_fields_valid
        && (data_version@ == 0 || voted_directory_valid)
))]
#[must_use]
pub fn quorum_state_load_decision(
    data_version: i32,
    leader_epoch: i32,
    voted_id: i32,
    version_fields_valid: bool,
    voted_directory_valid: bool,
    voted_directory_nil: bool,
) -> QuorumStateLoadDecision {
    if (data_version != 0 && data_version != 1)
        || leader_epoch < 0
        || voted_id < -1
        || !version_fields_valid
        || (data_version == 1 && !voted_directory_valid)
        || (data_version == 1 && voted_id == -1 && !voted_directory_nil)
    {
        QuorumStateLoadDecision::Reject
    } else if voted_id == -1 {
        QuorumStateLoadDecision::RestoreNoVote
    } else {
        QuorumStateLoadDecision::RestoreVote
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            QuorumStateLoadDecision, QuorumStateWriteDecision, quorum_state_load_decision,
            quorum_state_write_decision,
        },
        assert2::check,
    };

    #[test]
    fn writes_reject_every_lossy_boundary() {
        use QuorumStateWriteDecision::{Accept, Reject};

        let max = 2_147_483_647;
        for (version, epoch, has_leader, leader, has_vote, vote, voters_fit, expected) in [
            (
                0,
                max,
                true,
                u64::from(max),
                true,
                u64::from(max),
                true,
                Accept,
            ),
            (
                1,
                max,
                true,
                u64::from(max),
                true,
                u64::from(max),
                false,
                Accept,
            ),
            (2, 0, false, 0, false, 0, true, Reject),
            (0, max + 1, false, 0, false, 0, true, Reject),
            (0, 0, true, u64::from(max) + 1, false, 0, true, Reject),
            (0, 0, false, 0, true, u64::from(max) + 1, true, Reject),
            (0, 0, false, u64::MAX, false, u64::MAX, true, Accept),
            (0, 0, false, 0, false, 0, false, Reject),
        ] {
            check!(
                quorum_state_write_decision(
                    version, epoch, has_leader, leader, has_vote, vote, voters_fit,
                ) == expected
            );
        }
    }

    #[test]
    fn loads_require_exact_sentinel_and_version_fields() {
        use QuorumStateLoadDecision::{Reject, RestoreNoVote, RestoreVote};

        for (version, epoch, vote, fields, dir_valid, dir_nil, expected) in [
            (0, 0, -1, true, false, false, RestoreNoVote),
            (0, i32::MAX, 0, true, false, false, RestoreVote),
            (1, 0, -1, true, true, true, RestoreNoVote),
            (1, i32::MAX, i32::MAX, true, true, false, RestoreVote),
            (-1, 0, -1, true, false, false, Reject),
            (2, 0, -1, true, true, true, Reject),
            (0, -1, -1, true, false, false, Reject),
            (0, 0, -2, true, false, false, Reject),
            (0, 0, -1, false, false, false, Reject),
            (1, 0, 1, true, false, false, Reject),
            (1, 0, -1, true, true, false, Reject),
        ] {
            check!(
                quorum_state_load_decision(version, epoch, vote, fields, dir_valid, dir_nil)
                    == expected
            );
        }
    }
}
