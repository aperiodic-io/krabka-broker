//! Tests for the dead-broker failover scan: the clean ISR election, the
//! KIP-841 `unclean.leader.election.enable` fall-through, the KIP-966
//! offset-aware strategies that defer to the recovery manager, and the
//! witness role the scan reads out of the metadata image.

use assert2::assert;
use krabka_metadata::LeaderEpoch;

use super::*;
use crate::{
    config_keys::{
        ELIGIBLE_LEADER_REPLICAS, MIN_INSYNC_REPLICAS, UNCLEAN_LEADER_ELECTION_ENABLE,
        UNCLEAN_RECOVERY_STRATEGY,
    },
    leader_election::test_support::{
        ElectionSetup, ExpectedPartitionSetup, elected_partition, expected_clean_election,
        expected_partition, failover, failover_with_alive, img_with_partition, liveness_with_alive,
        mark_witnesses_in_image, one_partition_change, set_cluster_default, set_topic_configs,
    },
};

fn unclean_single_isr_image() -> krabka_metadata::MetadataImage {
    img_with_partition(ElectionSetup {
        isr: &[krabka_raft::NodeId(1)],
        configs: &[(UNCLEAN_LEADER_ELECTION_ENABLE, "true")],
        ..Default::default()
    })
}

/// Only the preferred replica is in the ISR; broker three precedes broker two among followers.
fn preferred_replica_alone_in_isr() -> krabka_metadata::MetadataImage {
    img_with_partition(ElectionSetup {
        replicas: &[NodeId(1), NodeId(3), NodeId(2)],
        isr: &[NodeId(1)],
        ..Default::default()
    })
}

#[tokio::test]
async fn failover_picks_alive_isr_member_when_available() {
    // Leader 1 dies, ISR {1, 2, 3}, both 2 and 3 alive — pick 2.
    let img = img_with_partition(ElectionSetup {
        ..Default::default()
    });
    let plan = failover_with_alive(&img, /*dead=*/ NodeId(1), &[2u64, 3]).await;
    assert!(plan.recoveries.is_empty());
    let pr = one_partition_change(&plan.changes);
    // leader_epoch and partition_epoch must both bump on election.
    let expected = expected_clean_election(
        krabka_raft::NodeId(2),
        &[krabka_raft::NodeId(2), krabka_raft::NodeId(3)],
        vec![],
    );
    assert!(*pr == expected);
}

#[tokio::test]
async fn failover_marks_exhausted_metadata_epochs_unavailable() {
    for (partition_epoch, leader_epoch) in [(i32::MAX, 5), (0, i32::MAX)] {
        let mut image = img_with_partition(ElectionSetup {
            replicas: &[krabka_raft::NodeId(1), krabka_raft::NodeId(2)],
            isr: &[krabka_raft::NodeId(1), krabka_raft::NodeId(2)],
            ..Default::default()
        });
        let mut record = image.partition("t", 0).expect("seeded partition").clone();
        record.partition_epoch = partition_epoch;
        record.leader_epoch = LeaderEpoch(leader_epoch);
        image.apply(&krabka_metadata::MetadataRecord::V1Partition(record));
        let liveness = ControllerLivenessState::new(krabka_units::secs(10));
        liveness.record_heartbeat(2).await;

        let plan = failover(&image, NodeId(1), &liveness).await;

        assert!(plan.changes.is_empty());
        assert!(plan.unavailable == vec![("t".to_owned(), 0)]);
    }
}

#[tokio::test]
async fn failover_processes_dead_replica_even_when_not_in_isr() {
    // Synthetic but valid during ISR churn: dead broker is the current
    // leader/replica, while the ISR already contains only surviving peers.
    let img = img_with_partition(ElectionSetup {
        isr: &[krabka_raft::NodeId(2), krabka_raft::NodeId(3)],
        ..Default::default()
    });
    let plan = failover_with_alive(&img, /*dead=*/ NodeId(1), &[2u64, 3]).await;

    let pr = one_partition_change(&plan.changes);
    assert!(pr.leader == 2);
    assert!(pr.isr == vec![NodeId(2), NodeId(3)]);
    assert!(pr.leader_epoch == 6, "leader_epoch must bump on election");
}

#[tokio::test]
async fn failover_ignores_partition_when_dead_broker_is_unrelated() {
    // Broker 9 is neither a replica nor an ISR member. Even if some other
    // ISR member is dead, this scan must not rewrite the partition.
    let img = img_with_partition(ElectionSetup {
        ..Default::default()
    });
    let plan = failover_with_alive(&img, /*dead=*/ NodeId(9), &[1u64, 3]).await;

    assert!(plan.changes.is_empty());
    assert!(plan.recoveries.is_empty());
}

#[tokio::test]
async fn failover_leaves_partition_unavailable_when_unclean_disabled() {
    // ISR is just {1}, broker 1 dies, brokers 2/3 alive. With
    // `unclean.leader.election.enable=false` (the default) the
    // controller must not elect — partition stays unavailable.
    let img = img_with_partition(ElectionSetup {
        isr: &[krabka_raft::NodeId(1)],
        ..Default::default()
    });
    let plan = failover_with_alive(&img, /*dead=*/ NodeId(1), &[2u64, 3]).await;
    assert!(
        plan.changes.is_empty(),
        "default-off must not emit any change, got {:?}",
        plan.changes,
    );
    assert!(plan.recoveries.is_empty());
}

#[tokio::test]
async fn failover_elects_unclean_when_topic_opts_in() {
    // Same setup, but `unclean.leader.election.enable=true` on the
    // topic. Controller must elect the first alive out-of-ISR replica
    // (broker 2) as leader with singleton ISR.
    let img = unclean_single_isr_image();
    let l = liveness_with_alive(&[2u64, 3]).await;
    let metrics = crate::metrics::BrokerMetrics::new();
    let plan = compute_failover_changes(&img, /*dead=*/ NodeId(1), &l, &metrics).await;
    assert!(plan.recoveries.is_empty());
    let pr = one_partition_change(&plan.changes);
    // Must elect the first alive replica (broker 2) with a singleton
    // ISR (KIP-841) and a bumped leader_epoch.
    let expected =
        expected_clean_election(krabka_raft::NodeId(2), &[krabka_raft::NodeId(2)], vec![]);
    assert!(*pr == expected);
    // Each unclean election bumps the counter exactly once.
    assert!(metrics.unclean_leader_elections_total.get() == 1);
}

#[tokio::test]
async fn failover_clean_does_not_bump_unclean_counter() {
    // Clean failover (ISR non-empty with an alive member) must not
    // bump the unclean-election counter — the metric is reserved
    // for the KIP-841 data-loss footgun path.
    let img = img_with_partition(ElectionSetup {
        ..Default::default()
    });
    let l = liveness_with_alive(&[2u64, 3]).await;
    let metrics = crate::metrics::BrokerMetrics::new();
    let _ = compute_failover_changes(&img, /*dead=*/ NodeId(1), &l, &metrics).await;
    assert!(metrics.unclean_leader_elections_total.get() == 0);
}

#[tokio::test]
async fn failover_unclean_skips_when_no_alive_replica() {
    // Unclean opt-in but ALL replicas dead — no election possible.
    let img = unclean_single_isr_image();
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    // No heartbeats — nobody alive.
    let plan = failover(&img, /*dead=*/ NodeId(1), &l).await;
    assert!(
        plan.changes.is_empty(),
        "no alive replica → no election, got {:?}",
        plan.changes,
    );
    assert!(plan.recoveries.is_empty());
}

#[tokio::test]
async fn failover_unclean_false_string_keeps_default_safe_behavior() {
    // Explicit `false` must behave the same as unset.
    let img = img_with_partition(ElectionSetup {
        isr: &[krabka_raft::NodeId(1)],
        configs: &[(UNCLEAN_LEADER_ELECTION_ENABLE, "false")],
        ..Default::default()
    });
    let plan = failover_with_alive(&img, /*dead=*/ NodeId(1), &[2u64, 3]).await;
    assert!(
        plan.changes.is_empty(),
        "explicit `false` keeps safe default"
    );
    assert!(plan.recoveries.is_empty());
}

#[tokio::test]
async fn failover_unclean_does_not_pick_dead_broker_itself() {
    // Edge case: `dead` is in `replicas`. The unclean fallback must
    // skip it — otherwise we'd re-elect the dead broker.
    let img = unclean_single_isr_image();
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    // Only broker 3 alive — broker 2 also dead.
    l.record_heartbeat(3).await;
    let plan = failover(&img, /*dead=*/ NodeId(1), &l).await;
    assert!(plan.recoveries.is_empty());
    let pr = one_partition_change(&plan.changes);
    assert!(pr.leader == 3);
    assert!(pr.isr == vec![NodeId(3)]);
}

#[tokio::test]
async fn failover_unclean_does_not_apply_when_isr_still_has_alive_member() {
    // Leader 1 dies. ISR {1, 2} but 2 is alive — clean path picks
    // broker 2 even if unclean is enabled. (The unclean branch only
    // fires when alive_isr is empty.)
    let img = img_with_partition(ElectionSetup {
        isr: &[krabka_raft::NodeId(1), krabka_raft::NodeId(2)],
        configs: &[(UNCLEAN_LEADER_ELECTION_ENABLE, "true")],
        ..Default::default()
    });
    let plan = failover_with_alive(&img, /*dead=*/ NodeId(1), &[2u64, 3]).await;
    assert!(plan.recoveries.is_empty());
    let pr = one_partition_change(&plan.changes);
    assert!(pr.leader == 2);
    assert!(
        pr.isr == vec![NodeId(2)],
        "clean ISR-only election keeps the surviving ISR member, not a singleton-of-some-other-replica"
    );
}

#[tokio::test]
async fn failover_shrinks_isr_for_partitions_where_dead_is_non_leader() {
    // Broker 2 dies; partition's leader is 1 (still alive). The
    // dead member must be dropped from ISR without bumping the
    // leader_epoch (the leader isn't changing).
    let img = img_with_partition(ElectionSetup {
        ..Default::default()
    });
    let plan = failover_with_alive(&img, /*dead=*/ NodeId(2), &[1u64, 3]).await;
    assert!(plan.recoveries.is_empty());
    let pr = one_partition_change(&plan.changes);
    // Leader unchanged; a non-leader-change must NOT bump leader_epoch
    // (stays 5) but does bump partition_epoch.
    let expected = expected_partition(ExpectedPartitionSetup {
        leader: krabka_raft::NodeId(1),
        isr: &[krabka_raft::NodeId(1), krabka_raft::NodeId(3)],
        leader_epoch: LeaderEpoch(5),
        ..Default::default()
    });
    assert!(*pr == expected);
}

#[tokio::test]
async fn failover_balanced_strategy_requests_recovery_not_immediate_change() {
    // Leader 1 dies, ISR shrinks to empty after dropping it; the topic
    // opted into `unclean.recovery.strategy=Balanced`, so the failover
    // scan must NOT make a blind immediate change — it hands the
    // partition to the URM via `recoveries`.
    let img = img_with_partition(ElectionSetup {
        isr: &[krabka_raft::NodeId(1)],
        configs: &[(UNCLEAN_RECOVERY_STRATEGY, "Balanced")],
        ..Default::default()
    });
    let plan = failover_with_alive(&img, /*dead=*/ NodeId(1), &[2u64, 3]).await;
    assert!(
        plan.changes.is_empty(),
        "Balanced strategy must defer to the URM, not elect immediately, got {:?}",
        plan.changes,
    );
    assert!(plan.recoveries == vec![("t".to_string(), 0, RecoveryStrategy::Balanced)]);
}

#[tokio::test]
async fn failover_uses_cluster_default_recovery_settings() {
    let mut img = img_with_partition(ElectionSetup {
        isr: &[krabka_raft::NodeId(1)],
        ..Default::default()
    });
    set_cluster_default(&mut img, UNCLEAN_RECOVERY_STRATEGY, "Balanced");
    let plan = failover_with_alive(&img, NodeId(1), &[2u64, 3]).await;

    assert!(plan.changes.is_empty());
    assert!(plan.recoveries == vec![("t".to_string(), 0, RecoveryStrategy::Balanced)]);
}

#[tokio::test]
async fn topic_none_overrides_cluster_strategy_and_uses_cluster_legacy_flag() {
    let mut img = img_with_partition(ElectionSetup {
        isr: &[krabka_raft::NodeId(1)],
        ..Default::default()
    });
    set_cluster_default(&mut img, UNCLEAN_RECOVERY_STRATEGY, "Balanced");
    set_cluster_default(&mut img, UNCLEAN_LEADER_ELECTION_ENABLE, "true");
    set_topic_configs(
        &mut img,
        crate::leader_election::test_support::TopicConfigSetup {
            entries: &[(UNCLEAN_RECOVERY_STRATEGY, "None")],
            ..Default::default()
        },
    );
    let plan = failover_with_alive(&img, NodeId(1), &[2u64, 3]).await;

    assert!(plan.recoveries.is_empty());
    let change = one_partition_change(&plan.changes);
    assert!(change.leader == 2);
    assert!(change.isr == vec![NodeId(2)]);
}

#[tokio::test]
async fn failover_strategy_none_still_uses_legacy_enable_flag() {
    // No recovery strategy set (defaults to None), but the legacy
    // `unclean.leader.election.enable=true` flag is on. The scan keeps
    // the KIP-841 behavior: blind pick of the first alive replica.
    let img = unclean_single_isr_image();
    let plan = failover_with_alive(&img, /*dead=*/ NodeId(1), &[2u64, 3]).await;
    assert!(
        plan.recoveries.is_empty(),
        "strategy None must not enqueue an offset-aware recovery",
    );
    let pr = one_partition_change(&plan.changes);
    assert!(pr.leader == 2, "legacy path picks first alive replica");
    assert!(pr.isr == vec![NodeId(2)]);
}

#[tokio::test]
async fn failover_scan_reads_the_witness_role_out_of_the_image() {
    // End-to-end through `compute_failover_changes`: the witness role
    // arrives as a per-broker config record, exactly as the broker
    // publishes it at registration.
    let mut img = img_with_partition(ElectionSetup {
        ..Default::default()
    });
    mark_witnesses_in_image(&mut img, &[2]);
    let plan = failover_with_alive(&img, /*dead=*/ NodeId(1), &[2u64, 3]).await;
    assert!(plan.recoveries.is_empty());
    let pr = one_partition_change(&plan.changes);
    let expected = expected_clean_election(
        krabka_raft::NodeId(3),
        &[krabka_raft::NodeId(2), krabka_raft::NodeId(3)],
        vec![],
    );
    assert!(*pr == expected);
}

#[tokio::test]
async fn failover_scan_leaves_a_witness_only_survivor_unavailable() {
    let mut img = img_with_partition(ElectionSetup {
        configs: &[(UNCLEAN_LEADER_ELECTION_ENABLE, "true")],
        ..Default::default()
    });
    mark_witnesses_in_image(&mut img, &[2]);
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    l.record_heartbeat(2).await;
    let metrics = crate::metrics::BrokerMetrics::new();
    let plan = compute_failover_changes(&img, /*dead=*/ NodeId(1), &l, &metrics).await;
    assert!(plan.changes.is_empty(), "got {:?}", plan.changes);
    assert!(plan.recoveries.is_empty());
    assert!(plan.unavailable == vec![("t".to_string(), 0)]);
    assert!(metrics.unclean_leader_elections_total.get() == 0);
}

/// One row of the eligible-leader-replica table below.
struct ElrScanCase<'a> {
    label: &'a str,
    /// Extra topic-config overrides, on top of the published ELR.
    policy: &'a [(&'a str, &'a str)],
}

/// KIP-966 through the whole dead-broker scan: the emitted record, the
/// metric, and the two lists the plan carries.
///
/// Broker 1 leads and dies, and the ISR named it alone. Broker 3 comes first
/// in the assignment and is alive, so every pre-ELR path lands on it: the
/// KIP-841 election picks it and calls the result unclean, and the strategies
/// hand the partition to the URM. Only broker 2 is published as eligible, so
/// only broker 2 is known to hold every committed record -- and Kafka's
/// `electAnyLeader` elects it and reports the election clean, under every one
/// of these policies.
#[tokio::test]
async fn failover_elects_an_eligible_leader_replica_cleanly_under_every_policy() {
    let cases = [
        ElrScanCase {
            label: "no unclean election and no offset-aware strategy",
            policy: &[],
        },
        ElrScanCase {
            label: "unclean election enabled",
            policy: &[(UNCLEAN_LEADER_ELECTION_ENABLE, "true")],
        },
        ElrScanCase {
            label: "balanced offset-aware recovery",
            policy: &[(UNCLEAN_RECOVERY_STRATEGY, "Balanced")],
        },
        ElrScanCase {
            label: "aggressive recovery and unclean election together",
            policy: &[
                (UNCLEAN_RECOVERY_STRATEGY, "Aggressive"),
                (UNCLEAN_LEADER_ELECTION_ENABLE, "true"),
            ],
        },
    ];
    for case in cases {
        let mut img = preferred_replica_alone_in_isr();
        let mut overrides: Vec<(&str, &str)> = vec![
            (ELIGIBLE_LEADER_REPLICAS, "0:2:"),
            (MIN_INSYNC_REPLICAS, "2"),
        ];
        overrides.extend_from_slice(case.policy);
        set_topic_configs(
            &mut img,
            crate::leader_election::test_support::TopicConfigSetup {
                entries: &overrides,
                ..Default::default()
            },
        );
        let l = ControllerLivenessState::new(krabka_units::secs(10));
        for n in [2u64, 3] {
            l.record_heartbeat(n).await;
        }
        let metrics = crate::metrics::BrokerMetrics::new();

        let plan = compute_failover_changes(&img, /*dead=*/ NodeId(1), &l, &metrics).await;

        let expected = PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: NodeId(2),
            replicas: vec![NodeId(1), NodeId(3), NodeId(2)],
            isr: vec![NodeId(2)],
            leader_epoch: LeaderEpoch(6),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 1,
        };
        assert!(
            *elected_partition(&plan.changes) == expected,
            "{}",
            case.label
        );
        assert!(plan.recoveries.is_empty(), "{}", case.label);
        assert!(plan.unavailable.is_empty(), "{}", case.label);
        assert!(
            metrics.unclean_leader_elections_total.get() == 0,
            "{}: an ELR election loses nothing and must not meter as unclean",
            case.label
        );
    }
}

/// An eligible leader replica that is not alive fails Kafka's
/// `isAcceptableLeader`, so the decision falls back to the rung below. With
/// the KIP-841 toggle on that is the out-of-ISR election of broker 3, and it
/// is reported as the data loss it is.
#[tokio::test]
async fn a_dead_eligible_leader_replica_leaves_the_unclean_election_to_decide() {
    let mut img = preferred_replica_alone_in_isr();
    set_topic_configs(
        &mut img,
        crate::leader_election::test_support::TopicConfigSetup {
            entries: &[
                (ELIGIBLE_LEADER_REPLICAS, "0:2:"),
                (MIN_INSYNC_REPLICAS, "2"),
                (UNCLEAN_LEADER_ELECTION_ENABLE, "true"),
            ],
            ..Default::default()
        },
    );
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    l.record_heartbeat(3).await;
    let metrics = crate::metrics::BrokerMetrics::new();

    let plan = compute_failover_changes(&img, /*dead=*/ NodeId(1), &l, &metrics).await;

    let pr = elected_partition(&plan.changes);
    assert!(pr.leader == NodeId(3));
    assert!(pr.isr == vec![NodeId(3)]);
    assert!(metrics.unclean_leader_elections_total.get() == 1);
}

/// The value Kafka writes when the last ISR member of a partition dies and
/// nothing can be elected: `leader = -1`, the ISR that lost it, the ELR that
/// ISR implies, and the last leader as `lastKnownElr`
/// (`PartitionChangeBuilder.maybeUpdateLastKnownLeader`).
///
/// The partition record is not rewritten -- a krabka record always names a
/// leader -- so the scan publishes only the ELR, and the last-known ELR that
/// names broker 1 is what says the partition has none. Every row is the state
/// with ELR enabled; without the feature nothing is published (see
/// `failover_leaves_partition_unavailable_when_unclean_disabled`).
#[tokio::test]
async fn a_partition_with_nothing_to_elect_publishes_the_last_leader_as_last_known() {
    struct Case {
        label: &'static str,
        overrides: &'static [(&'static str, &'static str)],
        alive: &'static [u64],
        recoveries: Vec<(String, i32, RecoveryStrategy)>,
    }
    let cases = [
        Case {
            label: "ISR [1], ELR [2, 3], and 2 and 3 are down: ELR [1, 2, 3], last-known [1]",
            overrides: &[
                (ELIGIBLE_LEADER_REPLICAS, "0:2,3:"),
                (MIN_INSYNC_REPLICAS, "2"),
            ],
            alive: &[],
            recoveries: vec![],
        },
        Case {
            label: "an alive replica outside the ISR and the ELR is not elected without the toggle",
            overrides: &[(MIN_INSYNC_REPLICAS, "2")],
            alive: &[2, 3],
            recoveries: vec![],
        },
        Case {
            label: "a partition handed to the offset-aware recovery has no leader either",
            overrides: &[
                (MIN_INSYNC_REPLICAS, "2"),
                (UNCLEAN_RECOVERY_STRATEGY, "Balanced"),
            ],
            alive: &[2, 3],
            recoveries: vec![("t".to_owned(), 0, RecoveryStrategy::Balanced)],
        },
    ];
    for Case {
        label,
        overrides,
        alive,
        recoveries,
    } in cases
    {
        let mut img = img_with_partition(ElectionSetup {
            isr: &[krabka_raft::NodeId(1)],
            elr: crate::leader_election::test_support::ElrFinalization::Enabled,
            configs: overrides,
            ..Default::default()
        });
        let l = ControllerLivenessState::new(krabka_units::secs(10));
        for &n in alive {
            l.record_heartbeat(n).await;
        }
        let published = TopicElr::of_topic(&img, "t").partition(0);
        let eligible_before: Vec<u64> = published
            .eligible_leader_replicas
            .iter()
            .map(|id| u64::try_from(*id).expect("a node id"))
            .collect();
        let mut expected_eligible = eligible_before;
        expected_eligible.push(1);
        expected_eligible.sort_unstable();

        let plan = failover(&img, /*dead=*/ NodeId(1), &l).await;

        assert!(
            plan.changes
                == vec![MetadataRecord::V1PartitionElr(
                    krabka_metadata::PartitionElrRecord {
                        topic: "t".into(),
                        partition: 0,
                        eligible_leader_replicas: expected_eligible
                            .into_iter()
                            .map(NodeId)
                            .collect(),
                        last_known_elr: vec![NodeId(1)],
                    }
                )],
            "{label}"
        );
        assert!(plan.recoveries == recoveries, "{label}");
        assert!(
            plan.unavailable.is_empty() == !recoveries.is_empty(),
            "{label}"
        );

        // Applying it and running the scan again -- the sweep does, every
        // tick -- publishes nothing more.
        for record in &plan.changes {
            img.apply(record);
        }
        let again = failover(&img, NodeId(1), &l).await;
        assert!(again.changes.is_empty(), "{label}: {:?}", again.changes);
        assert!(
            crate::elr::state::is_leaderless(&img, img.partition("t", 0).expect("partition")),
            "{label}"
        );
    }
}

/// A partition that keeps a leader publishes an empty last-known ELR, so a
/// shrink below min ISR does not fill it with the replicas it dropped, which is
/// what the published value used to be.
#[tokio::test]
async fn a_failover_that_keeps_a_leader_leaves_the_last_known_elr_empty() {
    let img = img_with_partition(ElectionSetup {
        elr: crate::leader_election::test_support::ElrFinalization::Enabled,
        configs: &[(MIN_INSYNC_REPLICAS, "3")],
        ..Default::default()
    });
    let plan = failover_with_alive(&img, /*dead=*/ NodeId(3), &[1u64, 2]).await;

    let [MetadataRecord::V1PartitionUpdate(update)] = plan.changes.as_slice() else {
        panic!("expected one partition update, got {:?}", plan.changes)
    };
    assert!(update.partition.isr == vec![NodeId(1), NodeId(2)]);
    assert!(update.eligible_leader_replicas == Some(vec![NodeId(3)]));
    assert!(update.last_known_elr == Some(vec![]));
}
