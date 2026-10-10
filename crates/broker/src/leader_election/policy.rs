//! The pure failover policy. [`failover_one`] answers what to do with one
//! partition when a replica of it is gone, and [`FailoverPlan`] is the shape
//! the two controller scans build out of those answers. Nothing here does
//! I/O, so the policy is unit-testable and model-checkable on its own.

use krabka_metadata::{
    LeaderEpoch, LeaderRecoveryState, MetadataRecord, PartitionRecord, PartitionRecoveryRecord,
};
use krabka_raft::NodeId;
use krabka_verified::consensus::{
    FailoverAction, FailoverFacts, FailoverRecovery, LiveIsr, OutOfIsrFacts, failover_action,
};

use crate::{config_keys::RecoveryStrategy, elr::state::PartitionElr};

/// Output of a failover scan: immediate metadata changes plus partitions
/// that need asynchronous offset-aware recovery through the URM.
#[derive(Default)]
pub(crate) struct FailoverPlan {
    pub changes: Vec<MetadataRecord>,
    pub recoveries: Vec<(String, i32, RecoveryStrategy)>,
    /// Partitions the dead broker leads that have no live ISR replica to
    /// elect. The scan leaves their records alone and marks them leaderless
    /// through the ELR they publish. The caller decides how loudly to
    /// report them: the death edge warns once, the per-tick sweep does not
    /// repeat that warning every second.
    pub unavailable: Vec<(String, i32)>,
}

/// Push the `PartitionRecord` that moves `pr` to `leader` and `isr` at the
/// given epochs, every other field carried over from `pr`. When `recovering`,
/// a `PartitionRecoveryRecord` marking the new leader `RECOVERING` follows it,
/// which is what an unclean election writes.
pub(crate) fn push_partition_change(
    changes: &mut Vec<MetadataRecord>,
    pr: &PartitionRecord,
    leader: NodeId,
    isr: Vec<NodeId>,
    partition_epoch: i32,
    leader_epoch: LeaderEpoch,
    recovering: bool,
) {
    changes.push(MetadataRecord::V1Partition(PartitionRecord {
        leader,
        isr,
        leader_epoch,
        partition_epoch,
        ..pr.clone()
    }));
    if recovering {
        changes.push(MetadataRecord::V1PartitionRecovery(
            PartitionRecoveryRecord {
                topic: pr.topic.clone(),
                partition: pr.partition,
                state: LeaderRecoveryState::Recovering,
            },
        ));
    }
}

/// The pure per-partition failover decision shared by the dead-broker scan
/// (`compute_failover_changes`) and the offline-log-dir scan
/// (`compute_offline_dir_failover_changes`). No I/O: the callers handle
/// partition filtering, the alive snapshot, record construction, metrics, and
/// recovery enqueue. This enum is separate so the failover policy is
/// independently unit-testable and model-checkable, and so the two scans share
/// one copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FailoverDecision {
    /// Elect `leader` with `isr`. The caller bumps `leader_epoch + 1` and, when
    /// `unclean`, records the unclean-election metric.
    Elect {
        leader: NodeId,
        isr: Vec<NodeId>,
        unclean: bool,
    },
    /// Defer to the offset-aware Unclean Recovery Manager (KIP-966).
    Recover(RecoveryStrategy),
    /// Dead broker was a non-leader ISR member: shrink ISR (leader/epoch kept).
    ShrinkIsr { isr: Vec<NodeId> },
    /// Leader is dead, ISR empty, and no unclean path is permitted/available.
    /// Kafka writes `leader = -1` for this; the scans keep the record and
    /// publish the last-known ELR that marks the partition as leaderless.
    Unavailable,
    /// Nothing to do for this partition.
    NoChange,
}

/// Decide the failover action for one partition. `alive` is the controller's
/// snapshot of live brokers. `witnesses` is the set of data-bearing witness
/// nodes, which replicate the partition and count toward
/// `min.insync.replicas` but never lead it. `elr` is the partition's published
/// KIP-966 eligible and last-known replicas. `strategy` and
/// `unclean_enabled` are the topic's resolved recovery policy.
///
/// # One broker leaves, and the leader is re-checked
///
/// This is Apache Kafka's answer to a fenced or unregistered broker:
/// `ReplicationControlManager.handleBrokerFenced` and
/// `handleBrokerUnregistered` both run `generateLeaderAndIsrUpdates` over
/// `brokersToIsrs.partitionsWithBrokerInIsr(brokerId)`, which builds one
/// `PartitionChangeBuilder` per partition with
///
/// ```text
/// targetIsr          = Replicas.copyWithout(partition.isr, brokerId)
/// isAcceptableLeader = r -> r != brokerId && clusterControl.isActive(r)
/// ```
///
/// Three rules follow, and this function keeps all three.
///
/// - Only a partition whose ISR holds `dead` is touched. The leader is always
///   an ISR member, so that includes every partition `dead` leads; a
///   partition `dead` only replicates is [`FailoverDecision::NoChange`].
/// - Only `dead` leaves the ISR. Every other member stays, however this
///   controller rates its liveness right now: its own death is its own event,
///   and a sweep runs [`failover_one`] for it too. A member that is kept
///   although it is down blocks the high watermark, so it still holds every
///   committed record, and keeping it loses nothing.
/// - The leader is re-elected whenever it is not a valid new leader of the
///   target ISR -- `electAnyLeader`'s first test is
///   `isValidNewLeader(partition.leader)`. That covers the dead leader, and
///   also a leader that died earlier and whose own failover has not run yet
///   when a follower's does. The follower's event elects, exactly as Kafka's
///   would, rather than rewriting the ISR under a leader that cannot serve.
///
/// A witness stays in the emitted ISR. It holds every committed record, so it
/// is what keeps `acks=all` writable after a site loss. Only the leader pick
/// excludes it: every election path adds "not a witness" to
/// `isAcceptableLeader`.
///
/// # The election ladder
///
/// `PartitionChangeBuilder.isValidNewLeader`, read out of Kafka trunk, is
///
/// ```text
/// (targetIsr.contains(id) || (targetIsr.isEmpty() && targetElr.contains(id)))
///     && isAcceptableLeader.test(id)
/// ```
///
/// and `electAnyLeader` takes the first replica **in assignment order**
/// (`targetReplicas.stream().filter(this::isValidNewLeader).findFirst()`)
/// that satisfies it, as `ElectionResult(node, false)` -- `false` being
/// `unclean`. The clean pick therefore walks `pr.replicas`, not `pr.isr`, and
/// the emitted ISR is the target ISR itself.
///
/// With no valid ISR member, a surviving ELR member is elected, called clean,
/// and never reaches the last-known-leader branch or the `Election.UNCLEAN`
/// branch below it. Nothing about that pick consults
/// `unclean.leader.election.enable`, and Kafka has no
/// `unclean.recovery.strategy` in front of it either: an ELR member left the
/// ISR while the partition still had `min.insync.replicas` members, so it
/// holds every committed record and electing it loses nothing.
/// [`FailoverAction::ElectFromElr`] is that rung, and it sits above both the
/// KIP-966 offset-aware recovery and the KIP-841 out-of-ISR election for the
/// same reason. Kafka's `tryElection` answers it with `targetIsr =
/// List.of(node)` and leaves `leaderRecoveryState` alone, which is the
/// singleton ISR and the `unclean: false` returned here.
///
/// Kafka opens the ELR disjunct only once `targetIsr` is empty. This opens it
/// once the target ISR has no *live* member, which is the same set in every
/// state Kafka can reach: Kafka removes a broker from every ISR in the same
/// record that fences it, so a target ISR member that is down is one whose
/// own removal krabka's per-broker scans have not run yet. Gating on the
/// literal emptiness would make the answer depend on which of two dead
/// brokers' scans ran first, and could elect uncleanly, or defer to the
/// offset-aware recovery, while an eligible replica was waiting.
///
/// The published set is enough on its own. Kafka recomputes `targetElr` as
/// `(elr ∪ isr) - targetIsr - uncleanShutdownReplicas` immediately before the
/// election, but the only id that recomputation adds is `dead`, which fails
/// the acceptable-leader half of the test regardless.
///
/// The last known leader is the next rung. `canElectLastKnownLeader` fires
/// when ELR is enabled, the target ISR and the target ELR are both empty, the
/// partition's `lastKnownElr` holds exactly one replica, and that replica is
/// acceptable; Kafka elects it as `ElectionResult(node, true)`, so
/// `tryElection` sets the ISR to that replica alone and the leader recovery
/// state to `RECOVERING`. It sits under the ELR pick and above the
/// `Election.UNCLEAN` branch in both `electAnyLeader` and
/// `electPreferredLeader`, and like the ELR pick it consults neither
/// `unclean.leader.election.enable` nor `unclean.recovery.strategy`.
/// [`FailoverAction::ElectLastKnown`] is that rung and it returns
/// `unclean: true`, which is what puts the partition in `Recovering`. The
/// target ELR is the published one plus `dead` when `dead` is in the ISR the
/// change leaves, so an ordinary death never empties it; the rung is open to
/// [`elect_leaderless_one`], and to a replica that restarted uncleanly, which
/// Kafka keeps out of the target ELR.
///
/// # Min ISR, and a partition that cannot elect
///
/// `min.insync.replicas` never blocks the ISR change: Kafka's builder lets the
/// ISR fall below it, and to empty under KIP-966, and only the ELR tracks the
/// shortfall. That bookkeeping is not here. Every scan runs
/// [`ElrPublisher`](crate::elr::ElrPublisher) over the records it emits, which
/// applies `maybePopulateTargetElr`'s rule to the change.
///
/// When no rung can elect, Kafka writes `leader = -1` alongside the target
/// ISR, the ELR that ISR implies, and the last leader as `lastKnownElr`. A
/// krabka partition record always names a leader, so the change is split. A
/// partition led by `dead` is [`FailoverDecision::Unavailable`] and its record
/// is left alone, because a dead leader dropped from its own ISR would be a
/// leader outside its ISR; the scans publish the rest through
/// [`ElrPublisher::leaderless`](crate::elr::ElrPublisher::leaderless), and the
/// one-member last-known ELR is what marks it as having no leader. A partition
/// led by a broker that is down, but whose failover has not run, still loses
/// `dead` from its ISR, which is [`FailoverDecision::ShrinkIsr`]; that leader
/// stays in the ISR, and its own scan reports the partition unavailable.
pub(crate) fn failover_one(
    pr: &PartitionRecord,
    dead: NodeId,
    alive: &std::collections::HashSet<NodeId>,
    witnesses: &std::collections::HashSet<NodeId>,
    elr: &PartitionElr,
    strategy: RecoveryStrategy,
    unclean_enabled: bool,
) -> FailoverDecision {
    // `partitionsWithBrokerInIsr(brokerId)`: the leader is an ISR member, so
    // this keeps every partition `dead` leads.
    if pr.leader != dead && !pr.isr.contains(&dead) {
        return FailoverDecision::NoChange;
    }
    let departure = Departure {
        node: dead,
        down: true,
        // `dead` joins the target ELR: it is in the ISR the change leaves.
        joins_elr: pr.isr.contains(&dead),
    };
    decide(
        pr,
        &departure,
        &Ladder {
            alive,
            witnesses,
            elr,
            strategy,
            unclean_enabled,
        },
    )
}

/// Decide what to do with a partition that has no leader, read out of the
/// last-known ELR that marks it (see [`PartitionElr::is_leaderless`]), now that
/// `alive` is the cluster as the caller sees it.
///
/// This is Kafka's `handleBrokerUnfenced`, which runs
/// `generateLeaderAndIsrUpdates` over `brokersToIsrs.partitionsWithNoLeader()`
/// with the unfencing broker as an acceptable leader, so the election ladder of
/// [`failover_one`] runs again for a partition that had nothing to elect when
/// it lost its leader. Nothing leaves the partition this time. The last leader
/// is out of the ISR Kafka holds, as it was when the partition lost its leader,
/// and it can lead once `alive` names it: from the ELR when the fence put it
/// there, and as the last known leader when an unclean restart kept it out.
/// A partition that is not marked leaderless is [`FailoverDecision::NoChange`].
pub(crate) fn elect_leaderless_one(
    pr: &PartitionRecord,
    alive: &std::collections::HashSet<NodeId>,
    witnesses: &std::collections::HashSet<NodeId>,
    elr: &PartitionElr,
    strategy: RecoveryStrategy,
    unclean_enabled: bool,
) -> FailoverDecision {
    if !elr.is_leaderless(pr.leader) {
        return FailoverDecision::NoChange;
    }
    let departure = Departure {
        node: pr.leader,
        // Whether the last leader is alive is the caller's `alive` to say.
        down: false,
        // It is in the published ELR when it belongs there.
        joins_elr: false,
    };
    decide(
        pr,
        &departure,
        &Ladder {
            alive,
            witnesses,
            elr,
            strategy,
            unclean_enabled,
        },
    )
}

/// What the ladder reads about the cluster and the partition's topic.
struct Ladder<'a> {
    alive: &'a std::collections::HashSet<NodeId>,
    witnesses: &'a std::collections::HashSet<NodeId>,
    elr: &'a PartitionElr,
    strategy: RecoveryStrategy,
    unclean_enabled: bool,
}

/// The replica whose removal from the ISR the ladder answers.
struct Departure {
    node: NodeId,
    /// It cannot lead, whatever the alive set says of it.
    down: bool,
    /// Kafka's `targetElr` gains it.
    joins_elr: bool,
}

/// The election ladder for one partition: `Replicas.copyWithout(partition.isr,
/// brokerId)` is the target ISR, and the rungs are the ones the module docs
/// walk through.
fn decide(pr: &PartitionRecord, gone: &Departure, ctx: &Ladder<'_>) -> FailoverDecision {
    let Ladder {
        alive,
        witnesses,
        elr,
        strategy,
        unclean_enabled,
    } = *ctx;
    // `Replicas.copyWithout(partition.isr, brokerId)`: `gone` alone leaves.
    let target_isr: Vec<NodeId> = pr.isr.iter().copied().filter(|n| *n != gone.node).collect();
    let live = |n: &NodeId| !(gone.down && *n == gone.node) && alive.contains(n);
    // Kafka's `isAcceptableLeader`, plus the witness rule krabka adds to
    // every election path.
    let acceptable = |n: &NodeId| live(n) && !witnesses.contains(n);
    // `isValidNewLeader(partition.leader)`: the leader keeps its place only
    // while it is an acceptable member of the target ISR.
    let leader_stays = target_isr.contains(&pr.leader) && acceptable(&pr.leader);
    // Every pick walks `targetReplicas`, in assignment order, and they differ
    // only in the test.
    let pick = |test: &dyn Fn(&NodeId) -> bool| pr.replicas.iter().copied().find(|n| test(n));
    let clean_candidate = pick(&|n| target_isr.contains(n) && acceptable(n));
    let elr_candidate = pick(&|n| {
        acceptable(n)
            && i32::try_from(n.0).is_ok_and(|id| elr.eligible_leader_replicas.contains(&id))
    });
    // `canElectLastKnownLeader`: the target ELR is empty, and the one replica
    // `lastKnownElr` holds is acceptable.
    let target_elr_empty = elr.eligible_leader_replicas.is_empty() && !gone.joins_elr;
    let last_known_candidate = match elr.last_known_elr.as_slice() {
        [only] if target_elr_empty => pick(&|n| acceptable(n) && u64::try_from(*only) == Ok(n.0)),
        _ => None,
    };
    let unclean_candidate = pick(&acceptable);
    let recovery = match strategy {
        RecoveryStrategy::None => FailoverRecovery::None,
        RecoveryStrategy::Balanced => FailoverRecovery::Balanced,
        RecoveryStrategy::Aggressive => FailoverRecovery::Aggressive,
    };
    let live_isr = if clean_candidate.is_some() {
        LiveIsr::Electable
    } else if target_isr.iter().any(live) {
        LiveIsr::WitnessesOnly
    } else {
        LiveIsr::Empty
    };
    match failover_action(FailoverFacts {
        // The kernel's "the leader went away": the leader must be replaced.
        leader_dead: !leader_stays,
        isr_shrunk: target_isr.len() < pr.isr.len(),
        live_isr,
        out_of_isr: OutOfIsrFacts {
            has_electable_elr: elr_candidate.is_some(),
            last_known_leader_electable: last_known_candidate.is_some(),
            recovery,
            unclean_election_available: unclean_enabled && unclean_candidate.is_some(),
        },
    }) {
        FailoverAction::ElectClean => {
            let new_leader = clean_candidate.expect("verified clean election has a candidate");
            // Clean: the new leader was in the ISR, so it holds every committed
            // record. No data loss, and the ISR is Kafka's target ISR.
            FailoverDecision::Elect {
                leader: new_leader,
                isr: target_isr,
                unclean: false,
            }
        }
        FailoverAction::ElectFromElr => {
            // KIP-966: the live ISR is empty, but a surviving replica is
            // published as eligible to lead, so it holds every record the
            // partition ever acknowledged. Kafka reports this election as
            // clean and gates it on nothing, and so does this: the ISR
            // narrows to the winner, and no unclean-election meter moves.
            let new_leader = elr_candidate.expect("verified ELR election has a candidate");
            FailoverDecision::Elect {
                leader: new_leader,
                isr: vec![new_leader],
                unclean: false,
            }
        }
        FailoverAction::ElectLastKnown => {
            // The one replica that led when the partition lost its leader is
            // back. It may have lost an unflushed tail, so Kafka calls this
            // election unclean -- singleton ISR, `RECOVERING` -- and takes it
            // without `unclean.leader.election.enable` or
            // `unclean.recovery.strategy`.
            let new_leader =
                last_known_candidate.expect("verified last-known election has a candidate");
            FailoverDecision::Elect {
                leader: new_leader,
                isr: vec![new_leader],
                unclean: true,
            }
        }
        FailoverAction::Recover(_) => FailoverDecision::Recover(strategy),
        FailoverAction::ElectUnclean => {
            // KIP-841: ISR is dead but the operator opted into possible data
            // loss. Elect the first alive non-witness replica, singleton ISR.
            let new_leader = unclean_candidate.expect("verified unclean election has a candidate");
            FailoverDecision::Elect {
                leader: new_leader,
                isr: vec![new_leader],
                unclean: true,
            }
        }
        FailoverAction::Unavailable
            if pr.leader != gone.node && target_isr.contains(&pr.leader) =>
        {
            // Nothing can replace a leader that is down but whose own failover
            // has not run. Kafka's record would still carry the target ISR, so
            // `dead` leaves it; the leader stays in it, and its own scan
            // answers for the partition.
            FailoverDecision::ShrinkIsr { isr: target_isr }
        }
        FailoverAction::Unavailable => {
            // Every alive ISR member is a witness, or nothing above this rung
            // could elect: no live ISR, no surviving eligible leader replica,
            // no offset-aware strategy, and no permitted unclean election. The
            // partition is unavailable, and that is the safe answer.
            //
            // A live witness is a full ISR member, so it holds every committed
            // record. An unclean election, or an offset-aware recovery that
            // excludes the witness, would move leadership to a data replica
            // that is behind the witness and would discard those records.
            // Loss of availability is recoverable. Loss of an acknowledged
            // write is not. The partition comes back as soon as one data
            // replica returns, and an operator who prefers availability can
            // still force an unclean election with `kafka-leader-election`.
            FailoverDecision::Unavailable
        }
        FailoverAction::ShrinkIsr => FailoverDecision::ShrinkIsr { isr: target_isr },
        FailoverAction::NoChange => FailoverDecision::NoChange,
    }
}

/// Decide what one partition needs when `returning` re-registers under a new
/// incarnation id, which is a broker that stopped and came back without the
/// controller being able to prove the stop was clean.
///
/// Apache Kafka answers the same event in
/// `ReplicationControlManager.handleBrokerShutdown`, whose unclean branch --
/// read out of `kafka-metadata-4.3.1.jar` -- runs
/// `generateLeaderAndIsrUpdates("handleBrokerUncleanShutdown", -1, -1,
/// brokerId, records, brokersToIsrs.partitionsWithBrokerInIsr(brokerId))`.
/// That call sets `targetIsr` to `Replicas.copyWithout(partition.isr, {-1,
/// brokerId})`, so it removes exactly the returning broker and leaves every
/// other ISR member alone, however the controller currently rates its
/// liveness. This does the same.
///
/// For a partition the returning broker only follows, it stops there, and
/// that is where it is not [`failover_one`]. Kafka's builder would also
/// re-elect a leader its `isActive` no longer accepts, and so would
/// [`failover_one`]. But a registration is answered whenever liveness says
/// the returning broker is dead, and a controller that has just been elected
/// may not have populated its liveness registry yet, so that re-election
/// would be decided against a registry that calls every leader dead. The
/// leader of such a partition is left to its own failover, which runs only
/// once liveness has really declared it dead.
///
/// The one case that does need the full policy is the partition the returning
/// broker is still recorded as leading. A bare ISR rewrite there would leave a
/// leader that is not in its own ISR, so that case is handed to
/// [`failover_one`] unchanged: the broker is dead as far as liveness is
/// concerned -- that is the precondition the registration was accepted under
/// -- so it is the same question the dead-broker scan asks, and it deserves
/// the same answer, up to and including an offset-aware recovery.
///
/// `elr` is the partition's published ELR, and it is read as the image still
/// holds it: the withdrawal this event also performs takes `returning` out of
/// every eligible set, and `returning` is the one node [`failover_one`] will
/// not elect anyway, so the two orders agree. Kafka's `uncleanShutdownReplicas`
/// keeps `returning` out of the target ELR, so it is not what keeps that set
/// from being empty, as a fenced broker is.
pub(crate) fn unclean_restart_one(
    pr: &PartitionRecord,
    returning: NodeId,
    alive: &std::collections::HashSet<NodeId>,
    witnesses: &std::collections::HashSet<NodeId>,
    elr: &PartitionElr,
    strategy: RecoveryStrategy,
    unclean_enabled: bool,
) -> FailoverDecision {
    if pr.leader == returning {
        let departure = Departure {
            node: returning,
            down: true,
            joins_elr: false,
        };
        return decide(
            pr,
            &departure,
            &Ladder {
                alive,
                witnesses,
                elr,
                strategy,
                unclean_enabled,
            },
        );
    }
    if !pr.isr.contains(&returning) {
        return FailoverDecision::NoChange;
    }
    FailoverDecision::ShrinkIsr {
        isr: pr.isr.iter().copied().filter(|n| *n != returning).collect(),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::leader_election::test_support::{ElectionSetup, witnesses};

    /// The full failover decision for one partition, with `witnesses` and the
    /// published eligible-leader-replica set given directly. This keeps the
    /// witness and ELR tests on the pure policy function.
    fn decide_with_elr(
        pr: &PartitionRecord,
        dead: u64,
        alive: &[u64],
        witness_ids: &[u64],
        eligible: &[i32],
        strategy: RecoveryStrategy,
        unclean_enabled: bool,
    ) -> super::FailoverDecision {
        let alive: std::collections::HashSet<NodeId> = alive.iter().copied().map(NodeId).collect();
        failover_one(
            pr,
            NodeId(dead),
            &alive,
            &witnesses(witness_ids),
            &elr(eligible, &[]),
            strategy,
            unclean_enabled,
        )
    }

    /// The published ELR state of a partition: the eligible and the last-known
    /// replicas.
    use crate::test_support::partition_elr as elr;

    /// [`decide_with_elr`] for a partition that publishes no ELR at all, which
    /// is every partition of a healthy cluster.
    fn decide(
        pr: &PartitionRecord,
        dead: u64,
        alive: &[u64],
        witness_ids: &[u64],
        strategy: RecoveryStrategy,
        unclean_enabled: bool,
    ) -> super::FailoverDecision {
        decide_with_elr(pr, dead, alive, witness_ids, &[], strategy, unclean_enabled)
    }

    fn partition_record(leader: u64, replicas: &[u64], isr: &[u64]) -> PartitionRecord {
        crate::leader_election::test_support::seed_partition(ElectionSetup {
            leader: krabka_raft::NodeId(leader),
            replicas: &crate::test_support::replica_nodes(replicas),
            isr: &crate::test_support::replica_nodes(isr),
            ..Default::default()
        })
    }

    #[test]
    fn clean_failover_skips_a_witness_that_sorts_first_in_the_isr() {
        // Leader 1 dies. The ISR order is [1, 2, 3] and broker 2 is the
        // witness, so the pre-witness code would have elected 2. The data
        // replica behind it, broker 3, must take leadership instead. The
        // whole decision is compared, so the emitted ISR is pinned too: it
        // still carries the witness, which is what keeps `acks=all` writable.
        let pr = partition_record(1, &[1, 2, 3], &[1, 2, 3]);
        let decision = decide(
            &pr,
            /*dead*/ 1,
            /*alive*/ &[2, 3],
            /*witness_ids*/ &[2],
            RecoveryStrategy::None,
            false,
        );
        assert!(
            decision
                == super::FailoverDecision::Elect {
                    leader: NodeId(3),
                    isr: vec![NodeId(2), NodeId(3)],
                    unclean: false,
                }
        );
    }

    #[test]
    fn only_witnesses_alive_is_unavailable_whatever_the_unclean_flag_says() {
        // Leader 1 and data replica 3 are dead. Only witness 2 is alive, and
        // it holds every committed record. Electing 3 would discard them, so
        // the answer is Unavailable: never Recover, never an unclean Elect.
        let pr = partition_record(1, &[1, 2, 3], &[1, 2, 3]);
        let cases: [(RecoveryStrategy, bool); 4] = [
            (RecoveryStrategy::None, false),
            (RecoveryStrategy::None, true),
            (RecoveryStrategy::Balanced, false),
            (RecoveryStrategy::Aggressive, true),
        ];
        for (strategy, unclean_enabled) in cases {
            let decision = decide(
                &pr,
                /*dead*/ 1,
                /*alive*/ &[2],
                /*witness_ids*/ &[2],
                strategy,
                unclean_enabled,
            );
            assert!(
                decision == super::FailoverDecision::Unavailable,
                "strategy {strategy:?}, unclean_enabled {unclean_enabled}"
            );
        }
    }

    #[test]
    fn unclean_election_never_picks_a_witness() {
        // ISR is {1} and broker 1 dies, so the KIP-841 out-of-ISR pick runs.
        // Replica 2 is alive but is the witness; the pick must fall through
        // to data replica 3.
        let pr = partition_record(1, &[1, 2, 3], &[1]);
        let decision = decide(
            &pr,
            /*dead*/ 1,
            /*alive*/ &[2, 3],
            /*witness_ids*/ &[2],
            RecoveryStrategy::None,
            true,
        );
        assert!(
            decision
                == super::FailoverDecision::Elect {
                    leader: NodeId(3),
                    isr: vec![NodeId(3)],
                    unclean: true,
                }
        );
    }

    #[test]
    fn unclean_election_is_unavailable_when_every_alive_replica_is_a_witness() {
        // Empty alive ISR and the only alive replica is the witness.
        let pr = partition_record(1, &[1, 2, 3], &[1]);
        let decision = decide(
            &pr,
            /*dead*/ 1,
            /*alive*/ &[2],
            /*witness_ids*/ &[2],
            RecoveryStrategy::None,
            true,
        );
        assert!(decision == super::FailoverDecision::Unavailable);
    }

    #[test]
    fn isr_shrink_for_a_non_leader_death_keeps_the_witness() {
        // Broker 3 dies and the leader is alive, so this is a plain shrink.
        // The witness stays in the emitted ISR.
        let pr = partition_record(1, &[1, 2, 3], &[1, 2, 3]);
        let decision = decide(
            &pr,
            /*dead*/ 3,
            /*alive*/ &[1, 2],
            /*witness_ids*/ &[2],
            RecoveryStrategy::None,
            false,
        );
        assert!(
            decision
                == super::FailoverDecision::ShrinkIsr {
                    isr: vec![NodeId(1), NodeId(2)],
                }
        );
    }

    /// One row of the eligible-leader-replica table.
    struct ElrCase<'a> {
        label: &'a str,
        strategy: RecoveryStrategy,
        unclean_enabled: bool,
        expected: super::FailoverDecision,
    }

    /// KIP-966: a partition whose live ISR has emptied elects a surviving
    /// eligible leader replica, and that election is clean.
    ///
    /// Broker 1 leads and dies; the ISR is `{1}`, so nothing in it survives.
    /// Broker 3 comes first in the assignment and is alive, but only broker 2
    /// is published as eligible, so only broker 2 is known to hold every
    /// committed record. Kafka's `electAnyLeader` takes the first replica that
    /// `isValidNewLeader` accepts -- for an empty target ISR that is the first
    /// ELR member -- and returns it as `ElectionResult(node, false)`.
    ///
    /// Every row is the same partition under a different recovery policy: the
    /// pick is above `unclean.leader.election.enable` and above
    /// `unclean.recovery.strategy`, so all four give the same answer. Without
    /// the ELR rung, the first row is `Unavailable`, the third and fourth
    /// defer to the URM, and the second elects broker 3 and drops whatever
    /// broker 2 held that broker 3 does not.
    #[test]
    fn an_eligible_leader_replica_is_elected_cleanly_whatever_the_recovery_policy_says() {
        let pr = partition_record(1, &[1, 3, 2], &[1]);
        let elected_two = super::FailoverDecision::Elect {
            leader: NodeId(2),
            isr: vec![NodeId(2)],
            unclean: false,
        };
        let cases = [
            ElrCase {
                label: "unclean election off, no offset-aware strategy",
                strategy: RecoveryStrategy::None,
                unclean_enabled: false,
                expected: elected_two.clone(),
            },
            ElrCase {
                label: "unclean election on: the ELR member outranks replica 3",
                strategy: RecoveryStrategy::None,
                unclean_enabled: true,
                expected: elected_two.clone(),
            },
            ElrCase {
                label: "balanced recovery does not defer to the URM",
                strategy: RecoveryStrategy::Balanced,
                unclean_enabled: false,
                expected: elected_two.clone(),
            },
            ElrCase {
                label: "aggressive recovery does not defer to the URM",
                strategy: RecoveryStrategy::Aggressive,
                unclean_enabled: true,
                expected: elected_two,
            },
        ];
        for case in cases {
            let decision = decide_with_elr(
                &pr,
                /*dead*/ 1,
                /*alive*/ &[2, 3],
                /*witness_ids*/ &[],
                /*eligible*/ &[2],
                case.strategy,
                case.unclean_enabled,
            );
            assert!(decision == case.expected, "{}", case.label);
        }
    }

    /// The ELR rung is reachable only once the live ISR is empty, which is the
    /// guard Kafka puts on its `targetElr` disjunct. Broker 3 is published as
    /// eligible here, but brokers 2 and 3 both survive in the ISR, so the
    /// ordinary clean election decides and keeps them both.
    #[test]
    fn a_live_isr_decides_the_election_and_the_published_elr_does_not() {
        let pr = partition_record(1, &[1, 2, 3], &[1, 2, 3]);
        let decision = decide_with_elr(
            &pr,
            /*dead*/ 1,
            /*alive*/ &[2, 3],
            /*witness_ids*/ &[],
            /*eligible*/ &[3],
            RecoveryStrategy::None,
            true,
        );
        assert!(
            decision
                == super::FailoverDecision::Elect {
                    leader: NodeId(2),
                    isr: vec![NodeId(2), NodeId(3)],
                    unclean: false,
                }
        );
    }

    /// An eligible leader replica that cannot serve is no candidate. A dead
    /// one fails Kafka's `isAcceptableLeader`, and a witness fails the rule
    /// krabka adds to every election path: it replicates the partition and
    /// can be published as eligible, but it answers no client. Either way the
    /// decision falls through to the rung below, which here is the KIP-841
    /// election of the one replica that is left.
    #[test]
    fn an_elr_member_that_cannot_lead_falls_through_to_the_unclean_election() {
        let pr = partition_record(1, &[1, 2, 3], &[1]);
        let cases: [(&str, &[u64], &[u64]); 2] = [
            ("the only ELR member is dead", &[3], &[]),
            ("the only ELR member is a witness", &[2, 3], &[2]),
        ];
        for (label, alive, witness_ids) in cases {
            let decision = decide_with_elr(
                &pr,
                /*dead*/ 1,
                alive,
                witness_ids,
                /*eligible*/ &[2],
                RecoveryStrategy::None,
                true,
            );
            assert!(
                decision
                    == super::FailoverDecision::Elect {
                        leader: NodeId(3),
                        isr: vec![NodeId(3)],
                        unclean: true,
                    },
                "{label}"
            );
        }
    }

    /// The full unclean-restart decision for one partition, for a partition
    /// that publishes no ELR.
    fn restart_decide(
        pr: &PartitionRecord,
        returning: u64,
        alive: &[u64],
        strategy: RecoveryStrategy,
        unclean_enabled: bool,
    ) -> super::FailoverDecision {
        let alive: std::collections::HashSet<NodeId> = alive.iter().copied().map(NodeId).collect();
        unclean_restart_one(
            pr,
            NodeId(returning),
            &alive,
            &witnesses(&[]),
            &PartitionElr::default(),
            strategy,
            unclean_enabled,
        )
    }

    /// A returning broker is one event about one broker. The follower case
    /// takes that broker out of the ISR and leaves every other member where
    /// it is, however the controller currently rates its liveness, which is
    /// Kafka's `Replicas.copyWithout(partition.isr, {-1, brokerId})`.
    ///
    /// The second half is why this is not [`failover_one`]: a registration is
    /// answered whenever liveness says the broker is dead, and a controller
    /// that has just been elected has an empty liveness registry. The
    /// dead-broker policy re-elects a leader that registry calls dead, and
    /// under an offset-aware strategy that means handing a partition whose
    /// leader is healthy to the URM.
    #[test]
    fn an_unclean_restart_removes_only_the_returning_broker_from_the_isr() {
        let pr = partition_record(1, &[1, 2, 3], &[1, 2, 3]);

        let decision = restart_decide(
            &pr,
            /*returning*/ 3,
            /*alive*/ &[],
            RecoveryStrategy::Balanced,
            false,
        );

        assert!(
            decision
                == super::FailoverDecision::ShrinkIsr {
                    isr: vec![NodeId(1), NodeId(2)],
                }
        );
        assert!(
            decide(
                &pr,
                /*dead*/ 3,
                /*alive*/ &[],
                &[],
                RecoveryStrategy::Balanced,
                false
            ) == super::FailoverDecision::Recover(RecoveryStrategy::Balanced)
        );
    }

    /// A partition the returning broker is still recorded as leading cannot
    /// take a bare ISR rewrite: it would leave a leader that is not in its own
    /// ISR. That case is the dead-broker policy, unchanged, because the broker
    /// is dead as far as liveness is concerned.
    #[test]
    fn an_unclean_restart_of_a_leader_takes_the_failover_policy() {
        let pr = partition_record(3, &[1, 2, 3], &[1, 2, 3]);

        let decision = restart_decide(
            &pr,
            /*returning*/ 3,
            /*alive*/ &[1, 2],
            RecoveryStrategy::None,
            false,
        );

        assert!(
            decision
                == super::FailoverDecision::Elect {
                    leader: NodeId(1),
                    isr: vec![NodeId(1), NodeId(2)],
                    unclean: false,
                }
        );
    }

    /// A partition the returning broker neither leads nor is in the ISR of has
    /// nothing to withdraw, even when it is still one of the replicas.
    #[test]
    fn an_unclean_restart_leaves_a_partition_it_is_not_in_the_isr_of_alone() {
        let pr = partition_record(1, &[1, 2, 3], &[1, 2]);

        let decision = restart_decide(
            &pr,
            /*returning*/ 3,
            /*alive*/ &[1, 2],
            RecoveryStrategy::None,
            false,
        );

        assert!(decision == super::FailoverDecision::NoChange);
    }

    /// One row of the one-broker-leaves table.
    struct KafkaCase<'a> {
        label: &'a str,
        pr: PartitionRecord,
        dead: u64,
        alive: &'a [u64],
        unclean_enabled: bool,
        expected: super::FailoverDecision,
    }

    /// Kafka's `handleBrokerFenced`: `targetIsr = Replicas.copyWithout(isr,
    /// brokerId)` removes the one broker, and `electAnyLeader` re-elects
    /// whenever the current leader is not a valid new leader of that target,
    /// taking the first valid replica in assignment order.
    ///
    /// The first row is the state the `leader_failover_model` search found:
    /// leader 2 was down but not yet failed over when follower 3's failover
    /// ran, and the old policy dropped every down member, so it emitted an
    /// empty ISR under leader 2. Once every broker was back, that record had a
    /// leader outside its own ISR, and no `AlterPartition` could repair it.
    #[test]
    fn a_failover_removes_only_the_dead_broker_and_re_elects_an_invalid_leader() {
        let cases = [
            KafkaCase {
                label: "follower dies under a down leader with nothing to elect",
                pr: partition_record(2, &[1, 2, 3], &[2, 3]),
                dead: 3,
                alive: &[1],
                unclean_enabled: false,
                expected: super::FailoverDecision::ShrinkIsr {
                    isr: vec![NodeId(2)],
                },
            },
            KafkaCase {
                label: "follower dies under a down leader: a live ISR member takes over",
                pr: partition_record(1, &[1, 2, 3], &[1, 2, 3]),
                dead: 3,
                alive: &[2],
                unclean_enabled: false,
                expected: super::FailoverDecision::Elect {
                    leader: NodeId(2),
                    isr: vec![NodeId(1), NodeId(2)],
                    unclean: false,
                },
            },
            KafkaCase {
                label: "follower dies under a down leader: KIP-841 elects out of the ISR",
                pr: partition_record(2, &[1, 2, 3], &[2, 3]),
                dead: 3,
                alive: &[1],
                unclean_enabled: true,
                expected: super::FailoverDecision::Elect {
                    leader: NodeId(1),
                    isr: vec![NodeId(1)],
                    unclean: true,
                },
            },
            KafkaCase {
                label: "leader dies: a down follower keeps its ISR place for its own event",
                pr: partition_record(1, &[1, 2, 3], &[1, 2, 3]),
                dead: 1,
                alive: &[2],
                unclean_enabled: false,
                expected: super::FailoverDecision::Elect {
                    leader: NodeId(2),
                    isr: vec![NodeId(2), NodeId(3)],
                    unclean: false,
                },
            },
            KafkaCase {
                label: "the clean pick walks the assignment, not the ISR",
                pr: partition_record(1, &[1, 3, 2], &[1, 2, 3]),
                dead: 1,
                alive: &[2, 3],
                unclean_enabled: false,
                expected: super::FailoverDecision::Elect {
                    leader: NodeId(3),
                    isr: vec![NodeId(2), NodeId(3)],
                    unclean: false,
                },
            },
            KafkaCase {
                label: "a replica outside the ISR is not `partitionsWithBrokerInIsr`",
                pr: partition_record(1, &[1, 2, 3], &[1, 2]),
                dead: 3,
                alive: &[1],
                unclean_enabled: false,
                expected: super::FailoverDecision::NoChange,
            },
        ];
        for case in cases {
            let decision = decide(
                &case.pr,
                case.dead,
                case.alive,
                &[],
                RecoveryStrategy::None,
                case.unclean_enabled,
            );
            assert!(decision == case.expected, "{}", case.label);
        }
    }

    /// One row of the no-witness regression table.
    struct FailoverCase<'a> {
        pr: &'a PartitionRecord,
        dead: u64,
        alive: &'a [u64],
        strategy: RecoveryStrategy,
        unclean_enabled: bool,
        expected: super::FailoverDecision,
    }

    #[test]
    fn an_empty_witness_set_leaves_every_failover_decision_unchanged() {
        // The regression guard for non-stretch clusters: each case is decided
        // with no witnesses, and the expected value is the pre-witness answer.
        let clean = partition_record(1, &[1, 2, 3], &[1, 2, 3]);
        let empty_isr = partition_record(1, &[1, 2, 3], &[1]);
        let cases = [
            // Clean election picks the first valid replica in assignment order.
            FailoverCase {
                pr: &clean,
                dead: 1,
                alive: &[2, 3],
                strategy: RecoveryStrategy::None,
                unclean_enabled: false,
                expected: super::FailoverDecision::Elect {
                    leader: NodeId(2),
                    isr: vec![NodeId(2), NodeId(3)],
                    unclean: false,
                },
            },
            // Non-leader death shrinks the ISR and keeps the leader.
            FailoverCase {
                pr: &clean,
                dead: 3,
                alive: &[1, 2],
                strategy: RecoveryStrategy::None,
                unclean_enabled: false,
                expected: super::FailoverDecision::ShrinkIsr {
                    isr: vec![NodeId(1), NodeId(2)],
                },
            },
            // Empty ISR with unclean off stays unavailable.
            FailoverCase {
                pr: &empty_isr,
                dead: 1,
                alive: &[2, 3],
                strategy: RecoveryStrategy::None,
                unclean_enabled: false,
                expected: super::FailoverDecision::Unavailable,
            },
            // Empty ISR with unclean on picks the first alive replica.
            FailoverCase {
                pr: &empty_isr,
                dead: 1,
                alive: &[2, 3],
                strategy: RecoveryStrategy::None,
                unclean_enabled: true,
                expected: super::FailoverDecision::Elect {
                    leader: NodeId(2),
                    isr: vec![NodeId(2)],
                    unclean: true,
                },
            },
            // Empty ISR with an offset-aware strategy defers to the URM.
            FailoverCase {
                pr: &empty_isr,
                dead: 1,
                alive: &[2, 3],
                strategy: RecoveryStrategy::Balanced,
                unclean_enabled: false,
                expected: super::FailoverDecision::Recover(RecoveryStrategy::Balanced),
            },
            // An unrelated broker changes nothing.
            FailoverCase {
                pr: &clean,
                dead: 9,
                alive: &[1, 2, 3],
                strategy: RecoveryStrategy::None,
                unclean_enabled: false,
                expected: super::FailoverDecision::NoChange,
            },
        ];
        for case in cases {
            let decision = decide(
                case.pr,
                case.dead,
                case.alive,
                &[],
                case.strategy,
                case.unclean_enabled,
            );
            assert!(
                decision == case.expected,
                "dead {}, alive {:?}, strategy {:?}, unclean {}",
                case.dead,
                case.alive,
                case.strategy,
                case.unclean_enabled
            );
        }
    }

    /// One row of the leaderless table.
    struct LeaderlessCase<'a> {
        label: &'a str,
        replicas: &'a [u64],
        isr: &'a [u64],
        elr: PartitionElr,
        alive: &'a [u64],
        witness_ids: &'a [u64],
        strategy: RecoveryStrategy,
        unclean_enabled: bool,
        expected: super::FailoverDecision,
    }

    /// The decision for a partition that lost its leader: the record still
    /// names broker 1 as leader and lists it in the ISR, which is how krabka
    /// holds a partition Kafka holds as `leader = -1`.
    fn decide_leaderless(case: &LeaderlessCase<'_>) -> super::FailoverDecision {
        let pr = partition_record(1, case.replicas, case.isr);
        let alive: std::collections::HashSet<NodeId> =
            case.alive.iter().copied().map(NodeId).collect();
        elect_leaderless_one(
            &pr,
            &alive,
            &witnesses(case.witness_ids),
            &case.elr,
            case.strategy,
            case.unclean_enabled,
        )
    }

    /// `PartitionChangeBuilderTest` at 4.3.1: `testEligibleLeaderReplicas_ElectLastKnownLeader`,
    /// `_ElectLastKnownLeaderShouldFail`, `_NotEligibleLastKnownLeader` and
    /// `_ElrCanBeElected`, each with `useLastKnownLeaderInBalancedRecovery` on,
    /// which is how Kafka runs it. The partition there has `leader = -1`, an
    /// empty ISR and replicas `[1, 2, 3, 4]`; the state here is the same one
    /// under krabka's marker.
    #[test]
    fn the_last_known_leader_is_elected_unclean_as_kafka_does() {
        let elected = |leader: u64, unclean: bool| super::FailoverDecision::Elect {
            leader: NodeId(leader),
            isr: vec![NodeId(leader)],
            unclean,
        };
        // The replicas are `[1, 2, 3, 4]` and the record's ISR is `[1]`, the
        // last leader alone, unless a row says otherwise.
        let case =
            |label, elr, alive, witness_ids, strategy, unclean_enabled, expected| LeaderlessCase {
                label,
                replicas: &[1, 2, 3, 4],
                isr: &[1],
                elr,
                alive,
                witness_ids,
                strategy,
                unclean_enabled,
                expected,
            };
        let none = RecoveryStrategy::None;
        let cases = [
            // ElectLastKnownLeader: ISR and ELR empty, `lastKnownElr = [1]`, every
            // broker acceptable. Broker 1 leads under a singleton ISR and
            // `RECOVERING`, which is `unclean: true`.
            case(
                "the last known leader returns",
                elr(&[], &[1]),
                &[1, 2, 3, 4][..],
                &[][..],
                none,
                false,
                elected(1, true),
            ),
            // No `unclean.leader.election.enable` and no
            // `unclean.recovery.strategy` sits in front of it.
            case(
                "with the unclean election on",
                elr(&[], &[1]),
                &[1, 2, 3, 4],
                &[],
                none,
                true,
                elected(1, true),
            ),
            case(
                "over balanced recovery",
                elr(&[], &[1]),
                &[1, 2, 3, 4],
                &[],
                RecoveryStrategy::Balanced,
                false,
                elected(1, true),
            ),
            case(
                "over aggressive recovery and the unclean election",
                elr(&[], &[1]),
                &[1, 2, 3, 4],
                &[],
                RecoveryStrategy::Aggressive,
                true,
                elected(1, true),
            ),
            // It is broker 1 that is elected, though broker 2 comes first in
            // the assignment and the unclean election is on: the rung sits
            // above `Election.UNCLEAN`, which would pick broker 2.
            LeaderlessCase {
                label: "the last known leader wins over the first live replica",
                replicas: &[2, 3, 1, 4],
                isr: &[1],
                elr: elr(&[], &[1]),
                alive: &[1, 2, 3, 4],
                witness_ids: &[],
                strategy: none,
                unclean_enabled: true,
                expected: elected(1, true),
            },
            // ElectLastKnownLeaderShouldFail: the ELR is not empty, and its
            // one member is offline, so nothing is elected.
            case(
                "a non-empty ELR shuts the last-known rung",
                elr(&[3], &[1]),
                &[1, 2, 4],
                &[],
                none,
                false,
                super::FailoverDecision::Unavailable,
            ),
            // ElrCanBeElected: an electable ELR member wins over the last
            // known leader, and its election is clean.
            case(
                "an ELR member outranks the last known leader",
                elr(&[3], &[1]),
                &[1, 3],
                &[],
                none,
                false,
                elected(3, false),
            ),
            case(
                "the last known leader in the ELR is a clean election",
                elr(&[1, 2, 3], &[1]),
                &[1, 2, 3, 4],
                &[],
                none,
                false,
                elected(1, false),
            ),
            // NotEligibleLastKnownLeader: nobody is acceptable, whichever
            // election type the topic asks for.
            case(
                "nobody alive, unclean election off",
                elr(&[], &[1]),
                &[],
                &[],
                none,
                false,
                super::FailoverDecision::Unavailable,
            ),
            case(
                "nobody alive, unclean election on",
                elr(&[], &[1]),
                &[],
                &[],
                none,
                true,
                super::FailoverDecision::Unavailable,
            ),
            // `isAcceptableLeader` is false for the last known leader, so the
            // `Election.UNCLEAN` branch below the rung decides.
            case(
                "the last known leader is down: KIP-841 is what is left",
                elr(&[], &[1]),
                &[2, 3],
                &[],
                none,
                true,
                elected(2, true),
            ),
            case(
                "the last known leader is down and the unclean election is off",
                elr(&[], &[1]),
                &[2, 3],
                &[],
                none,
                false,
                super::FailoverDecision::Unavailable,
            ),
            // krabka adds the witness rule to every election path.
            case(
                "a witness is never the last known leader",
                elr(&[], &[1]),
                &[1, 2, 3, 4],
                &[1],
                none,
                false,
                super::FailoverDecision::Unavailable,
            ),
            // A live ISR member is an ordinary clean election under the ISR
            // Kafka holds, which lost the last leader when the partition lost
            // its leader.
            LeaderlessCase {
                label: "a live ISR member leads before any out-of-ISR rung",
                replicas: &[1, 2, 3, 4],
                isr: &[1, 2],
                elr: elr(&[], &[1]),
                alive: &[1, 2],
                witness_ids: &[],
                strategy: none,
                unclean_enabled: false,
                expected: elected(2, false),
            },
            // `lastKnownElr.length != 1`: not a last known leader, and not a
            // partition that lost its leader either.
            case(
                "a multi-member last-known set is no marker",
                elr(&[], &[1, 2]),
                &[1, 2, 3, 4],
                &[],
                none,
                false,
                super::FailoverDecision::NoChange,
            ),
            case(
                "a partition that has a leader is not touched",
                elr(&[], &[]),
                &[1, 2, 3, 4],
                &[],
                none,
                false,
                super::FailoverDecision::NoChange,
            ),
            case(
                "the marker names the recorded leader and nobody else",
                elr(&[], &[2]),
                &[1, 2, 3, 4],
                &[],
                none,
                false,
                super::FailoverDecision::NoChange,
            ),
        ];
        for case in cases {
            assert!(decide_leaderless(&case) == case.expected, "{}", case.label);
        }
    }

    /// Kafka's target ELR is `(elr ∪ isr) - targetIsr - uncleanShutdownReplicas`
    /// and `canElectLastKnownLeader` needs it empty. A fenced broker in the ISR
    /// lands in it, so an ordinary death keeps the last-known rung shut; an
    /// unclean shutdown keeps the broker out of it, and the rung opens.
    #[test]
    fn only_an_unclean_shutdown_keeps_the_dead_broker_out_of_the_target_elr() {
        let pr = partition_record(1, &[1, 2, 3], &[1]);
        let state = elr(&[], &[2]);
        let alive: std::collections::HashSet<NodeId> = [NodeId(2), NodeId(3)].into();

        let fenced = failover_one(
            &pr,
            NodeId(1),
            &alive,
            &witnesses(&[]),
            &state,
            RecoveryStrategy::None,
            false,
        );
        let restarted = unclean_restart_one(
            &pr,
            NodeId(1),
            &alive,
            &witnesses(&[]),
            &state,
            RecoveryStrategy::None,
            false,
        );

        assert!(fenced == super::FailoverDecision::Unavailable);
        assert!(
            restarted
                == super::FailoverDecision::Elect {
                    leader: NodeId(2),
                    isr: vec![NodeId(2)],
                    unclean: true,
                }
        );
    }
}
