use super::*;

#[yare::parameterized(
    live_request_records = { LIVE_ROW, ReplicaFetchMutation::Append },
    kip_320_divergence = { ReplicaFetchFacts {
        request_leader_epoch: i32::MAX,
        current_leader_epoch: i32::MAX,
        diverging_epoch: 0,
        ..LIVE_ROW
    }, ReplicaFetchMutation::Truncate },
    fenced_leader_epoch_retries = {
        ReplicaFetchFacts { error_code: 74, ..LIVE_ROW },
        ReplicaFetchMutation::Retry,
    },
    error_row_with_divergence_retries = { ReplicaFetchFacts {
        error_code: 1, diverging_epoch: 7, ..LIVE_ROW
    }, ReplicaFetchMutation::Retry },
    in_flight_leader_epoch_change_rejects = { ReplicaFetchFacts {
        current_leader_epoch: 5, ..LIVE_ROW
    }, ReplicaFetchMutation::Reject },
    changed_replication_target_rejects = { ReplicaFetchFacts {
        target_matches: false, ..LIVE_ROW
    }, ReplicaFetchMutation::Reject },
    reported_leader_change_rejects = { ReplicaFetchFacts {
        reported_target_matches: false, diverging_epoch: 3, ..LIVE_ROW
    }, ReplicaFetchMutation::Reject },
    fenced_error_row_rejects = { ReplicaFetchFacts {
        request_leader_epoch: i32::MIN, error_code: 6, ..LIVE_ROW
    }, ReplicaFetchMutation::Reject },
)]
fn replica_fetch_mutation_fences_every_input_and_selects_one_action(
    facts: ReplicaFetchFacts,
    expected: ReplicaFetchMutation,
) {
    assert2::assert!(replica_fetch_mutation(facts) == expected);
}

#[yare::parameterized(
    one_preferred_election = { std::vec![PREFERRED_BACK], 1000, true },
    batch_exactly_at_cap = { std::vec![PREFERRED_BACK; 2], 2, true },
    empty_batch = { std::vec![], 1000, false },
    batch_over_cap = { std::vec![PREFERRED_BACK; 3], 2, false },
    nonpreferred_replica = { std::vec![PreferredLeaderChange {
        new_leader: 2, ..PREFERRED_BACK
    }], 1000, false },
    no_assignment = { std::vec![PreferredLeaderChange {
        preferred_replica: None, ..PREFERRED_BACK
    }], 1000, false },
    preferred_replica_out_of_isr = { std::vec![PREFERRED_BACK, PreferredLeaderChange {
        leader_in_isr: false, ..PREFERRED_BACK
    }], 1000, false },
    preferred_replica_not_alive = { std::vec![PreferredLeaderChange {
        leader_alive: false, ..PREFERRED_BACK
    }], 1000, false },
    preferred_replica_is_witness = { std::vec![PreferredLeaderChange {
        leader_is_witness: true, ..PREFERRED_BACK
    }], 1000, false },
)]
fn preferred_rebalance_admits_only_capped_preferred_elections(
    changes: Vec<PreferredLeaderChange>,
    cap: usize,
    expected: bool,
) {
    assert2::assert!(preferred_rebalance_admission(&changes, cap) == expected);
}
