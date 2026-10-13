use super::*;

#[test]
fn failover_action_covers_clean_unclean_recovery_and_shrink_paths() {
    use {
        FailoverAction::{
            ElectClean, ElectFromElr, ElectLastKnown, ElectUnclean, NoChange, Recover, ShrinkIsr,
            Unavailable,
        },
        FailoverRecovery::{Aggressive, Balanced, None},
        LiveIsr::{Electable, Empty, WitnessesOnly},
    };

    // The arguments are the live ISR, then `has_electable_elr`,
    // `last_known_leader_electable`, the recovery strategy and
    // `unclean_election_available`.
    let dead = |live_isr,
                has_electable_elr,
                last_known_leader_electable,
                recovery,
                unclean_election_available| FailoverFacts {
        leader_dead: true,
        isr_shrunk: true,
        live_isr,
        out_of_isr: OutOfIsrFacts {
            has_electable_elr,
            last_known_leader_electable,
            recovery,
            unclean_election_available,
        },
    };
    let alive = |isr_shrunk| FailoverFacts {
        leader_dead: false,
        isr_shrunk,
        live_isr: Electable,
        out_of_isr: OutOfIsrFacts {
            has_electable_elr: false,
            last_known_leader_electable: false,
            recovery: None,
            unclean_election_available: false,
        },
    };
    for (name, facts, expected) in [
        (
            "a live ISR member leads, whatever the out-of-ISR options",
            dead(Electable, true, true, Aggressive, true),
            ElectClean,
        ),
        // An electable ELR member outranks every offset-aware strategy and
        // the KIP-841 election, and never reaches either.
        (
            "ELR alone",
            dead(Empty, true, false, None, false),
            ElectFromElr,
        ),
        (
            "ELR over the unclean election",
            dead(Empty, true, false, None, true),
            ElectFromElr,
        ),
        (
            "ELR over offset-aware recovery",
            dead(Empty, true, false, Balanced, false),
            ElectFromElr,
        ),
        // The last known leader is Kafka's `canElectLastKnownLeader`: it
        // sits under the ELR rung and over both the offset-aware recovery
        // and the KIP-841 election, and neither toggle gates it.
        (
            "last known leader alone",
            dead(Empty, false, true, None, false),
            ElectLastKnown,
        ),
        (
            "last known leader over the unclean election",
            dead(Empty, false, true, None, true),
            ElectLastKnown,
        ),
        (
            "last known leader over balanced recovery",
            dead(Empty, false, true, Balanced, false),
            ElectLastKnown,
        ),
        (
            "last known leader over aggressive recovery and the unclean election",
            dead(Empty, false, true, Aggressive, true),
            ElectLastKnown,
        ),
        (
            "ELR over the last known leader",
            dead(Empty, true, true, None, false),
            ElectFromElr,
        ),
        (
            "balanced recovery",
            dead(Empty, false, false, Balanced, false),
            Recover(Balanced),
        ),
        (
            "aggressive recovery over the unclean election",
            dead(Empty, false, false, Aggressive, true),
            Recover(Aggressive),
        ),
        (
            "KIP-841 unclean election",
            dead(Empty, false, false, None, true),
            ElectUnclean,
        ),
        (
            "nothing to elect",
            dead(Empty, false, false, None, false),
            Unavailable,
        ),
        // A live ISR that holds only witnesses is unavailable even with an
        // ELR, a last known leader, a recovery strategy, and an unclean
        // election: every out-of-ISR rung is guarded on an empty ISR.
        (
            "witness-only ISR",
            dead(WitnessesOnly, true, true, Balanced, true),
            Unavailable,
        ),
        ("a follower left the ISR", alive(true), ShrinkIsr),
        ("nothing changed", alive(false), NoChange),
    ] {
        check!(failover_action(facts) == expected, "case {name}");
    }
}

#[test]
fn recovery_replica_ranking_is_epoch_then_offset_then_lowest_node() {
    let at = |last_epoch, log_end_offset, broker_id| RecoveryCandidate {
        last_epoch,
        log_end_offset,
        broker_id,
    };
    for (name, candidates, expected) in [
        ("nobody answered", std::vec![], None),
        (
            "a newer epoch beats a longer log",
            std::vec![at(4, 100, 2), at(5, 10, 3)],
            Some(1),
        ),
        (
            "the longer log wins within an epoch",
            std::vec![at(5, 90, 2), at(5, 120, 3)],
            Some(1),
        ),
        (
            "the lowest broker id breaks a full tie",
            std::vec![at(5, 100, 3), at(5, 100, 1), at(5, 100, 2)],
            Some(1),
        ),
        (
            "identical answers keep the first",
            std::vec![at(5, 100, 1), at(5, 100, 1)],
            Some(0),
        ),
    ] {
        check!(
            select_best_recovery_replica(&candidates) == expected,
            "case {name}"
        );
    }
}
