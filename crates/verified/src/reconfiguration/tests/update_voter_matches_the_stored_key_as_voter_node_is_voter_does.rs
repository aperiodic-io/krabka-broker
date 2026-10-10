use assert2::assert;

use super::*;

#[test]
fn update_voter_matches_the_stored_key_as_voter_node_is_voter_does() {
    use TargetMembership::{
        Absent, PresentOtherDirectory, PresentSameDirectory, PresentUnknownDirectory,
    };
    let update = request(VoterChangeKind::Update);
    let incompatible = TargetVoter {
        version_compatible: false,
        ..target(Absent)
    };
    let cases = [
        // The ApiVersions range check precedes the voter lookup.
        (
            "old range",
            voters(VoterCount(3), KraftFeatureLevel(1)),
            incompatible,
            D::IncompatibleVoter,
        ),
        (
            "unknown id",
            voters(VoterCount(3), KraftFeatureLevel(1)),
            target(Absent),
            D::VoterNotFound,
        ),
        (
            "other directory",
            voters(VoterCount(3), KraftFeatureLevel(1)),
            target(PresentOtherDirectory),
            D::DirectoryMismatch,
        ),
        (
            "legacy directory",
            voters(VoterCount(3), KraftFeatureLevel(1)),
            target(PresentUnknownDirectory),
            plan(ReconfigurationPlanSetup::default()),
        ),
        (
            "same directory",
            voters(VoterCount(3), KraftFeatureLevel(1)),
            target(PresentSameDirectory),
            plan(ReconfigurationPlanSetup::default()),
        ),
        (
            "preflight unknown id",
            voters(VoterCount(3), KraftFeatureLevel(0)),
            target(Absent),
            D::VoterNotFound,
        ),
        // The preflight changes the live quorum, so it does not take
        // Kafka's id-only version-0 match.
        (
            "preflight other",
            voters(VoterCount(3), KraftFeatureLevel(0)),
            target(PresentOtherDirectory),
            D::DirectoryMismatch,
        ),
        (
            "preflight same",
            voters(VoterCount(3), KraftFeatureLevel(0)),
            target(PresentSameDirectory),
            plan(ReconfigurationPlanSetup {
                level: KraftFeatureLevel(0),
                voters: ControlRecordWrite::Skip,
                ..Default::default()
            }),
        ),
        (
            "preflight legacy",
            voters(VoterCount(3), KraftFeatureLevel(0)),
            target(PresentUnknownDirectory),
            plan(ReconfigurationPlanSetup {
                level: KraftFeatureLevel(0),
                voters: ControlRecordWrite::Skip,
                ..Default::default()
            }),
        ),
    ];
    for (case, current, voter, expected) in cases {
        assert!(
            voter_reconfiguration_decision(LEADING, current, update, voter) == expected,
            "{case}"
        );
    }
}

#[test]
fn kraft_version_finalization_is_only_the_zero_to_one_upgrade() {
    let lacking = CurrentVoterSet {
        all_voters_support_requested: false,
        ..voters(VoterCount(3), KraftFeatureLevel(0))
    };
    let finalize = |requested_kraft_version| VoterChangeRequest {
        kind: VoterChangeKind::FinalizeKraftVersion,
        requested_kraft_version,
    };
    let cases = [
        (
            "already finalized",
            voters(VoterCount(3), KraftFeatureLevel(1)),
            finalize(1),
            D::InvalidVersionTransition,
        ),
        (
            "no-op level",
            voters(VoterCount(3), KraftFeatureLevel(0)),
            finalize(0),
            D::InvalidVersionTransition,
        ),
        (
            "unknown level",
            voters(VoterCount(3), KraftFeatureLevel(0)),
            finalize(2),
            D::InvalidVersionTransition,
        ),
        (
            "unsupported voter",
            lacking,
            finalize(1),
            D::IncompatibleVoter,
        ),
        (
            "admitted",
            voters(VoterCount(3), KraftFeatureLevel(0)),
            finalize(1),
            plan(ReconfigurationPlanSetup {
                kraft: ControlRecordWrite::Emit,
                ..Default::default()
            }),
        ),
    ];
    for (case, current, change, expected) in cases {
        let got = voter_reconfiguration_decision(
            LEADING,
            current,
            change,
            target(TargetMembership::Absent),
        );
        assert!(got == expected, "{case}");
    }
}
