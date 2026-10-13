//! `KRaft` voter-set wire admission.

use creusot_std::prelude::*;

/// Admission outcome for one voter carried by a `VotersRecord`.
#[krabka_macros::model_type]
pub enum VoterWireDecision {
    NegativeId,
    DirectoryMismatch,
    InvalidEndpoint,
    InvalidVersionRange,
    Accept,
}

/// Validate the signed voter identity, exact directory translation, endpoint
/// set, and inclusive supported-version range before conversion.
#[ensures((result == VoterWireDecision::NegativeId) == (voter_id@ < 0))]
#[ensures((result == VoterWireDecision::DirectoryMismatch) == (
    voter_id@ >= 0 && !directory_id_exact
))]
#[ensures((result == VoterWireDecision::InvalidEndpoint) == (
    voter_id@ >= 0 && directory_id_exact && (endpoint_count@ == 0 || !endpoints_valid)
))]
#[ensures((result == VoterWireDecision::InvalidVersionRange) == (
    voter_id@ >= 0 && directory_id_exact && endpoint_count@ > 0 && endpoints_valid
        && (min_supported_version@ < 0
            || max_supported_version@ < 0
            || min_supported_version@ > max_supported_version@)
))]
#[ensures((result == VoterWireDecision::Accept) == (
    voter_id@ >= 0 && directory_id_exact && endpoint_count@ > 0 && endpoints_valid
        && min_supported_version@ >= 0
        && min_supported_version@ <= max_supported_version@
))]
#[must_use]
pub fn voter_wire_decision(
    voter_id: i32,
    directory_id_exact: bool,
    endpoint_count: usize,
    endpoints_valid: bool,
    min_supported_version: i16,
    max_supported_version: i16,
) -> VoterWireDecision {
    if voter_id < 0 {
        VoterWireDecision::NegativeId
    } else if !directory_id_exact {
        VoterWireDecision::DirectoryMismatch
    } else if endpoint_count == 0 || !endpoints_valid {
        VoterWireDecision::InvalidEndpoint
    } else if min_supported_version < 0
        || max_supported_version < 0
        || min_supported_version > max_supported_version
    {
        VoterWireDecision::InvalidVersionRange
    } else {
        VoterWireDecision::Accept
    }
}

/// Admission outcome for the outer `VotersRecord` collection.
#[krabka_macros::model_type]
pub enum VoterSetWireDecision {
    UnsupportedRecordVersion,
    Empty,
    DuplicateId,
    Accept,
}

/// Require the supported record version and a nonempty collection with one
/// entry per voter ID.
#[ensures((result == VoterSetWireDecision::UnsupportedRecordVersion)
    == (record_version@ != 0))]
#[ensures((result == VoterSetWireDecision::Empty)
    == (record_version@ == 0 && voter_count@ == 0))]
#[ensures((result == VoterSetWireDecision::DuplicateId) == (
    record_version@ == 0 && voter_count@ > 0 && !voter_ids_unique
))]
#[ensures((result == VoterSetWireDecision::Accept) == (
    record_version@ == 0 && voter_count@ > 0 && voter_ids_unique
))]
#[must_use]
pub fn voter_set_wire_decision(
    record_version: i16,
    voter_count: usize,
    voter_ids_unique: bool,
) -> VoterSetWireDecision {
    if record_version != 0 {
        VoterSetWireDecision::UnsupportedRecordVersion
    } else if voter_count == 0 {
        VoterSetWireDecision::Empty
    } else if !voter_ids_unique {
        VoterSetWireDecision::DuplicateId
    } else {
        VoterSetWireDecision::Accept
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            VoterSetWireDecision, VoterWireDecision, voter_set_wire_decision, voter_wire_decision,
        },
        assert2::check,
    };

    #[test]
    fn voter_admission_classifies_every_invalid_boundary() {
        use VoterWireDecision::{
            Accept, DirectoryMismatch, InvalidEndpoint, InvalidVersionRange, NegativeId,
        };

        for (id, directory, count, endpoints, min, max, expected) in [
            (0, true, 1, true, 0, 0, Accept),
            (i32::MAX, true, usize::MAX, true, 0, i16::MAX, Accept),
            (-1, true, 1, true, 0, 0, NegativeId),
            (0, false, 1, true, 0, 0, DirectoryMismatch),
            (0, true, 0, true, 0, 0, InvalidEndpoint),
            (0, true, 1, false, 0, 0, InvalidEndpoint),
            (0, true, 1, true, -1, 0, InvalidVersionRange),
            (0, true, 1, true, 0, -1, InvalidVersionRange),
            (0, true, 1, true, 1, 0, InvalidVersionRange),
        ] {
            check!(voter_wire_decision(id, directory, count, endpoints, min, max) == expected);
        }
    }

    #[test]
    fn voter_set_admission_requires_version_zero_and_unique_members() {
        use VoterSetWireDecision::{Accept, DuplicateId, Empty, UnsupportedRecordVersion};

        for (version, count, unique, expected) in [
            (0, 1, true, Accept),
            (0, usize::MAX, true, Accept),
            (-1, 1, true, UnsupportedRecordVersion),
            (1, 1, true, UnsupportedRecordVersion),
            (0, 0, true, Empty),
            (0, 2, false, DuplicateId),
        ] {
            check!(voter_set_wire_decision(version, count, unique) == expected);
        }
    }
}
