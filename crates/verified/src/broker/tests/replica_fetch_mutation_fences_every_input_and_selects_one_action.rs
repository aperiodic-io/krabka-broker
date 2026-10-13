use {super::*, assert2::assert};

#[test]
fn replica_fetch_mutation_fences_every_input_and_selects_one_action() {
    use ReplicaFetchMutation::{Append, Reject, Retry, Truncate};

    for (scenario, facts, expected) in [
        ("records for the live request", LIVE_ROW, Append),
        (
            "KIP-320 divergence at the widest epoch",
            ReplicaFetchFacts {
                request_leader_epoch: i32::MAX,
                current_leader_epoch: i32::MAX,
                diverging_epoch: 0,
                ..LIVE_ROW
            },
            Truncate,
        ),
        (
            "FENCED_LEADER_EPOCH goes to error handling",
            ReplicaFetchFacts {
                error_code: 74,
                ..LIVE_ROW
            },
            Retry,
        ),
        (
            "an error row with a divergence is still only an error",
            ReplicaFetchFacts {
                error_code: 1,
                diverging_epoch: 7,
                ..LIVE_ROW
            },
            Retry,
        ),
        (
            "leader epoch bumped while the request was in flight",
            ReplicaFetchFacts {
                current_leader_epoch: 5,
                ..LIVE_ROW
            },
            Reject,
        ),
        (
            "the replication target moved to another leader",
            ReplicaFetchFacts {
                target_matches: false,
                ..LIVE_ROW
            },
            Reject,
        ),
        (
            "the leader reports a different current leader",
            ReplicaFetchFacts {
                reported_target_matches: false,
                diverging_epoch: 3,
                ..LIVE_ROW
            },
            Reject,
        ),
        (
            "a fenced error row mutates nothing either",
            ReplicaFetchFacts {
                request_leader_epoch: i32::MIN,
                error_code: 6,
                ..LIVE_ROW
            },
            Reject,
        ),
    ] {
        assert!(replica_fetch_mutation(facts) == expected, "{scenario}");
    }
}

#[test]
fn preferred_rebalance_admits_only_capped_preferred_elections() {
    for (scenario, changes, cap, expected) in [
        (
            "one preferred election",
            std::vec![PREFERRED_BACK],
            1000,
            true,
        ),
        (
            "a batch exactly at the cap",
            std::vec![PREFERRED_BACK; 2],
            2,
            true,
        ),
        ("nothing to rebalance", std::vec![], 1000, false),
        (
            "a batch over the cap",
            std::vec![PREFERRED_BACK; 3],
            2,
            false,
        ),
        (
            "the change installs a non-preferred replica",
            std::vec![PreferredLeaderChange {
                new_leader: 2,
                ..PREFERRED_BACK
            }],
            1000,
            false,
        ),
        (
            "the partition has no assignment",
            std::vec![PreferredLeaderChange {
                preferred_replica: None,
                ..PREFERRED_BACK
            }],
            1000,
            false,
        ),
        (
            "the preferred replica fell out of the ISR",
            std::vec![PREFERRED_BACK, PreferredLeaderChange {
                leader_in_isr: false,
                ..PREFERRED_BACK
            },],
            1000,
            false,
        ),
        (
            "the preferred replica is not alive",
            std::vec![PreferredLeaderChange {
                leader_alive: false,
                ..PREFERRED_BACK
            }],
            1000,
            false,
        ),
        (
            "the preferred replica is a witness",
            std::vec![PreferredLeaderChange {
                leader_is_witness: true,
                ..PREFERRED_BACK
            }],
            1000,
            false,
        ),
    ] {
        assert!(
            preferred_rebalance_admission(&changes, cap) == expected,
            "{scenario}"
        );
    }
}
