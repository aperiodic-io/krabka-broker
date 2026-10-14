use super::*;

fn dead(
    live_isr: LiveIsr,
    has_electable_elr: bool,
    last_known_leader_electable: bool,
    recovery: FailoverRecovery,
    unclean_election_available: bool,
) -> FailoverFacts {
    FailoverFacts {
        leader_dead: true,
        isr_shrunk: true,
        live_isr,
        out_of_isr: OutOfIsrFacts {
            has_electable_elr,
            last_known_leader_electable,
            recovery,
            unclean_election_available,
        },
    }
}

fn alive(isr_shrunk: bool) -> FailoverFacts {
    FailoverFacts {
        leader_dead: false,
        isr_shrunk,
        live_isr: LiveIsr::Electable,
        out_of_isr: OutOfIsrFacts {
            has_electable_elr: false,
            last_known_leader_electable: false,
            recovery: FailoverRecovery::None,
            unclean_election_available: false,
        },
    }
}

fn candidate(last_epoch: i32, log_end_offset: i64, broker_id: u64) -> RecoveryCandidate {
    RecoveryCandidate {
        last_epoch,
        log_end_offset,
        broker_id,
    }
}

#[yare::parameterized(
    live_isr = {
        dead(LiveIsr::Electable, true, true, FailoverRecovery::Aggressive, true),
        FailoverAction::ElectClean,
    },
    elr_alone = {
        dead(LiveIsr::Empty, true, false, FailoverRecovery::None, false),
        FailoverAction::ElectFromElr,
    },
    elr_over_unclean = {
        dead(LiveIsr::Empty, true, false, FailoverRecovery::None, true),
        FailoverAction::ElectFromElr,
    },
    elr_over_offset_aware = {
        dead(LiveIsr::Empty, true, false, FailoverRecovery::Balanced, false),
        FailoverAction::ElectFromElr,
    },
    last_known_leader = {
        dead(LiveIsr::Empty, false, true, FailoverRecovery::None, false),
        FailoverAction::ElectLastKnown,
    },
    last_known_over_unclean = {
        dead(LiveIsr::Empty, false, true, FailoverRecovery::None, true),
        FailoverAction::ElectLastKnown,
    },
    last_known_over_balanced = {
        dead(LiveIsr::Empty, false, true, FailoverRecovery::Balanced, false),
        FailoverAction::ElectLastKnown,
    },
    last_known_over_aggressive_and_unclean = {
        dead(LiveIsr::Empty, false, true, FailoverRecovery::Aggressive, true),
        FailoverAction::ElectLastKnown,
    },
    elr_over_last_known = {
        dead(LiveIsr::Empty, true, true, FailoverRecovery::None, false),
        FailoverAction::ElectFromElr,
    },
    balanced_recovery = {
        dead(LiveIsr::Empty, false, false, FailoverRecovery::Balanced, false),
        FailoverAction::Recover(FailoverRecovery::Balanced),
    },
    aggressive_recovery = {
        dead(LiveIsr::Empty, false, false, FailoverRecovery::Aggressive, true),
        FailoverAction::Recover(FailoverRecovery::Aggressive),
    },
    unclean_election = {
        dead(LiveIsr::Empty, false, false, FailoverRecovery::None, true),
        FailoverAction::ElectUnclean,
    },
    no_candidate = {
        dead(LiveIsr::Empty, false, false, FailoverRecovery::None, false),
        FailoverAction::Unavailable,
    },
    witness_only_isr = {
        dead(LiveIsr::WitnessesOnly, true, true, FailoverRecovery::Balanced, true),
        FailoverAction::Unavailable,
    },
    shrunk_isr = { alive(true), FailoverAction::ShrinkIsr },
    unchanged_live_isr = { alive(false), FailoverAction::NoChange },
)]
fn failover_action_covers_clean_unclean_recovery_and_shrink_paths(
    facts: FailoverFacts,
    expected: FailoverAction,
) {
    check!(failover_action(facts) == expected);
}

#[yare::parameterized(
    no_candidates = { std::vec![], None },
    newer_epoch_beats_longer_log = {
        std::vec![candidate(4, 100, 2), candidate(5, 10, 3)],
        Some(1),
    },
    longer_log_wins_within_epoch = {
        std::vec![candidate(5, 90, 2), candidate(5, 120, 3)],
        Some(1),
    },
    lowest_id_breaks_full_tie = {
        std::vec![candidate(5, 100, 3), candidate(5, 100, 1), candidate(5, 100, 2)],
        Some(1),
    },
    identical_answers_keep_first = {
        std::vec![candidate(5, 100, 1), candidate(5, 100, 1)],
        Some(0),
    },
)]
fn recovery_replica_ranking_is_epoch_then_offset_then_lowest_node(
    candidates: Vec<RecoveryCandidate>,
    expected: Option<usize>,
) {
    check!(select_best_recovery_replica(&candidates) == expected);
}
