use super::*;
use crate::reconfiguration::{
    CurrentVoterSet, ReconfigurationLeadership, TargetMembership, TargetVoter, VoterChangeKind,
    VoterChangeRequest, VoterReconfigurationPlan,
    test_support::{KraftFeatureLevel, VoterCount},
};

#[derive(Clone, Copy)]
struct RequestFlags(u16);

#[derive(Clone, Copy)]
#[repr(u16)]
enum RequestProperty {
    Leader = 1 << 0,
    NoPendingChange = 1 << 1,
    EpochCommitted = 1 << 2,
    ControlsCommitted = 1 << 3,
    VotersSupportVersion = 1 << 4,
    CompatibleTargetVersion = 1 << 5,
    TargetCaughtUp = 1 << 6,
}

impl RequestFlags {
    fn contains(self, property: RequestProperty) -> bool {
        self.0 & property as u16 != 0
    }
}

#[derive(Clone, Copy)]
struct GeneratedRequestSetup {
    count: VoterCount,
    flags: RequestFlags,
    kind: VoterChangeKind,
    membership: TargetMembership,
    version: KraftFeatureLevel,
    requested: KraftFeatureLevel,
}

impl Default for GeneratedRequestSetup {
    fn default() -> Self {
        Self {
            count: VoterCount(3),
            flags: RequestFlags(0x7f),
            kind: VoterChangeKind::Add,
            membership: TargetMembership::Absent,
            version: KraftFeatureLevel(1),
            requested: KraftFeatureLevel(1),
        }
    }
}

mod oracle;
use oracle::{check_overlap, current, leading, target};
mod commit_waiter;
mod control_support;
mod sets;

type RequestCase = (
    Vec<u64>,
    u64,
    (
        ReconfigurationLeadership,
        CurrentVoterSet,
        VoterChangeRequest,
        TargetVoter,
    ),
);

fn request_cases() -> impl Strategy<Value = RequestCase> {
    (
        prop::collection::vec(0_u64..12, 0..10),
        0_u64..13,
        any::<u16>(),
        0_u8..4,
        0_u8..4,
        0_u16..3,
        0_u16..3,
    )
        .prop_map(
            |(old, node, bits, operation, membership, version, requested)| {
                let kind = match operation {
                    0 => VoterChangeKind::Add,
                    1 => VoterChangeKind::Remove,
                    2 => VoterChangeKind::Update,
                    _ => VoterChangeKind::FinalizeKraftVersion,
                };
                let membership = match membership {
                    0 => TargetMembership::Absent,
                    1 => TargetMembership::PresentUnknownDirectory,
                    2 => TargetMembership::PresentSameDirectory,
                    _ => TargetMembership::PresentOtherDirectory,
                };
                let facts = generated_request(GeneratedRequestSetup {
                    count: VoterCount(old.len()),
                    flags: RequestFlags(bits),
                    kind,
                    membership,
                    version: KraftFeatureLevel(version),
                    requested: KraftFeatureLevel(requested),
                });
                (old, node, facts)
            },
        )
}

fn generated_request(
    setup: GeneratedRequestSetup,
) -> (
    ReconfigurationLeadership,
    CurrentVoterSet,
    VoterChangeRequest,
    TargetVoter,
) {
    use RequestProperty::{
        CompatibleTargetVersion, ControlsCommitted, EpochCommitted, Leader, NoPendingChange,
        TargetCaughtUp, VotersSupportVersion,
    };
    (
        ReconfigurationLeadership {
            is_leader: setup.flags.contains(Leader),
            no_pending_change: setup.flags.contains(NoPendingChange),
            epoch_committed: setup.flags.contains(EpochCommitted),
        },
        CurrentVoterSet {
            voter_count: setup.count.0,
            kraft_version: setup.version.0,
            latest_controls_committed: setup.flags.contains(ControlsCommitted),
            all_voters_support_requested: setup.flags.contains(VotersSupportVersion),
        },
        VoterChangeRequest {
            kind: setup.kind,
            requested_kraft_version: setup.requested.0,
        },
        TargetVoter {
            membership: setup.membership,
            version_compatible: setup.flags.contains(CompatibleTargetVersion),
            caught_up: setup.flags.contains(TargetCaughtUp),
        },
    )
}

/// One old/new report pair for every possible voter slot, with original full-width bounds.
fn prefix_report_cases() -> impl Strategy<Value = Vec<(i64, i64)>> {
    prop::collection::vec((any::<i64>(), any::<i64>()), 11)
}
