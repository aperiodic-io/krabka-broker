use super::*;

fn high_watermark_facts(isr_size: usize, effective_min_isr: usize) -> HighWatermarkFacts {
    HighWatermarkFacts {
        current: 40,
        leader_log_end: 100,
        isr_size,
        effective_min_isr,
    }
}

fn in_isr(log_end: i64) -> HwmReplica {
    HwmReplica {
        log_end,
        in_isr: true,
        caught_up_within_lag: false,
        eligibility: ELIGIBLE,
    }
}

fn outside_isr(
    log_end: i64,
    caught_up_within_lag: bool,
    eligibility: IsrEligibilityFacts,
) -> HwmReplica {
    HwmReplica {
        log_end,
        in_isr: false,
        caught_up_within_lag,
        eligibility,
    }
}

fn current_alter_partition_facts() -> AlterPartitionFacts {
    AlterPartitionFacts {
        request_leader_epoch: 5,
        current_leader_epoch: 5,
        request_partition_epoch: 10,
        current_partition_epoch: 10,
        requester_is_leader: true,
        proposed_isr: ProposedIsr::Valid,
        recovery_state_valid: true,
        replicas_eligible: true,
    }
}

/// `Partition.maybeIncrementLeaderHW` for a leader at log end 100 and
/// high watermark 40, with three assigned replicas and
/// `min.insync.replicas` 2 unless a row says otherwise.
#[yare::parameterized(
    slowest_in_sync = {
        "the slowest in-sync follower sets the watermark",
        high_watermark_facts(3, 2),
        &[in_isr(80), in_isr(60)],
        60,
    },
    leader_alone = {
        "a leader alone in its ISR advances to its log end",
        high_watermark_facts(1, 1),
        &[outside_isr(10, false, ELIGIBLE), outside_isr(20, false, ELIGIBLE)],
        100
    },
    under_min_isr = {
        "under min ISR the watermark does not move",
        high_watermark_facts(1, 2),
        &[outside_isr(100, true, ELIGIBLE), outside_isr(100, true, ELIGIBLE)],
        40
    },
    exactly_at_min_isr = {
        "exactly at min ISR it moves",
        high_watermark_facts(2, 2),
        &[in_isr(70), outside_isr(10, false, ELIGIBLE)],
        70,
    },
    caught_up_outside_isr = {
        "a caught-up eligible replica outside the ISR holds it back",
        high_watermark_facts(2, 2),
        &[in_isr(90), outside_isr(55, true, ELIGIBLE)],
        55,
    },
    not_caught_up_outside_isr = {
        "a replica outside the ISR that has not caught up does not",
        high_watermark_facts(2, 2),
        &[in_isr(90), outside_isr(55, false, ELIGIBLE)],
        90,
    },
    fenced_outside_isr = {
        "a caught-up fenced replica outside the ISR does not",
        high_watermark_facts(2, 2),
        &[in_isr(90), outside_isr(55, true, IsrEligibilityFacts { fenced: true, ..ELIGIBLE })],
        90
    },
    unfetched_isr_member = {
        "an ISR member that has not fetched from this leader holds it",
        high_watermark_facts(3, 2),
        &[in_isr(90), in_isr(-1)],
        40,
    },
    never_regresses = {
        "the watermark never falls",
        high_watermark_facts(3, 2),
        &[in_isr(90), in_isr(30)],
        40,
    },
    log_end_member = {
        "an ISR member at the log end with nothing else caught up",
        high_watermark_facts(2, 2),
        &[in_isr(100), outside_isr(0, false, ELIGIBLE)],
        100,
    },
)]
fn leader_high_watermark_follows_kafka(
    label: &str,
    facts: HighWatermarkFacts,
    replicas: &[HwmReplica],
    expected: i64,
) {
    assert2::check!(
        leader_high_watermark(facts, replicas) == expected,
        "{label}"
    );
}

/// The controller holds leader epoch 5 and partition epoch 10 for a
/// partition led by the requester. Each row is one `AlterPartition` a
/// Kafka controller answers as `expected`, per
/// `ReplicationControlManager.validateAlterPartitionData`.
#[yare::parameterized(
    up_to_date_shrink = {
        "an up-to-date shrink",
        current_alter_partition_facts(),
        IsrAdmission::Admit,
    },
    newer_leader_epoch = {
        "a newer leader epoch than the controller has seen",
        AlterPartitionFacts { request_leader_epoch: 6, ..current_alter_partition_facts() },
        IsrAdmission::NotController
    },
    newer_partition_epoch = {
        "a newer partition epoch than the controller has seen",
        AlterPartitionFacts { request_partition_epoch: 11, ..current_alter_partition_facts() },
        IsrAdmission::NotController
    },
    partition_epoch_precedes_leader_epoch = {
        "a newer partition epoch outranks a fenced leader epoch",
        AlterPartitionFacts {
            request_leader_epoch: 4,
            request_partition_epoch: 11,
            ..current_alter_partition_facts()
        },
        IsrAdmission::NotController,
    },
    old_leader_epoch = {
        "a leader from an older epoch",
        AlterPartitionFacts {
            request_leader_epoch: 4,
            request_partition_epoch: 9,
            requester_is_leader: false,
            ..current_alter_partition_facts()
        },
        IsrAdmission::FencedLeaderEpoch,
    },
    not_leader = {
        "a request from a broker that is not the leader",
        AlterPartitionFacts {
            requester_is_leader: false,
            request_partition_epoch: 9,
            ..current_alter_partition_facts()
        },
        IsrAdmission::InvalidRequest,
    },
    replaced_isr = {
        "a proposal built on an ISR the controller has since replaced",
        AlterPartitionFacts {
            request_partition_epoch: 9,
            proposed_isr: ProposedIsr::Invalid,
            replicas_eligible: false,
            ..current_alter_partition_facts()
        },
        IsrAdmission::InvalidUpdateVersion,
    },
    unassigned_replica = {
        "an ISR naming an unassigned replica",
        AlterPartitionFacts {
            proposed_isr: ProposedIsr::Invalid,
            replicas_eligible: false,
            ..current_alter_partition_facts()
        },
        IsrAdmission::InvalidRequest,
    },
    drops_leader = {
        "an ISR that drops the leader",
        AlterPartitionFacts { proposed_isr: ProposedIsr::WithoutLeader, ..current_alter_partition_facts() },
        IsrAdmission::InvalidRequest
    },
    invalid_recovery = {
        "a recovering leader proposing two members, one ineligible",
        AlterPartitionFacts {
            recovery_state_valid: false,
            replicas_eligible: false,
            ..current_alter_partition_facts()
        },
        IsrAdmission::InvalidRequest,
    },
    ineligible_fenced_broker = {
        "a fenced broker in the proposed ISR",
        AlterPartitionFacts { replicas_eligible: false, ..current_alter_partition_facts() },
        IsrAdmission::IneligibleReplica
    },
)]
fn isr_admission_follows_kafkas_alter_partition_validation(
    label: &str,
    facts: AlterPartitionFacts,
    expected: IsrAdmission,
) {
    assert2::check!(isr_admission(facts) == expected, "{label}");
}
