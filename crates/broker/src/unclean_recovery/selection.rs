//! The pure winner-selection helpers of KIP-966 unclean recovery.
//!
//! These functions rank the log states reported by the surviving replicas and
//! detect a recovery that a newer leader has already superseded. They hold no
//! I/O and no controller state, so the stateright models and the unit tests
//! drive them directly.
//!
//! # The most complete log is the last resort, not the first
//!
//! "Most complete surviving log" is a guess. It is the best guess available
//! once every replica that is known to be complete is gone, but it can still
//! drop committed records: the longest surviving log is only the longest one
//! that answered. Two sets of replicas are not a guess. A replica the
//! partition record still names in its ISR holds every committed record by
//! definition, and a KIP-966 eligible leader replica left the ISR while the
//! partition still had `min.insync.replicas` members, so it holds every record
//! the partition ever acknowledged. Electing either loses nothing.
//!
//! Apache Kafka orders the three the same way, in
//! `PartitionChangeBuilder.electAnyLeader`. Its `isValidNewLeader`, read out
//! of `kafka-metadata-4.3.1.jar`, is
//!
//! ```text
//! (targetIsr.contains(id) || (targetIsr.isEmpty() && targetElr.contains(id)))
//!     && isAcceptableLeader.test(id)
//! ```
//!
//! so an unfenced ISR member is a valid new leader outright, an unfenced ELR
//! member is one for a partition whose ISR has emptied, and `electAnyLeader`
//! returns either as `ElectionResult(node, false)` -- `false` being `unclean`.
//! Only when no such replica exists does Kafka reach its unclean branch.
//! [`select_leader`] is that order, and [`ElectionBasis`] is the `unclean`
//! flag under a name that says which rule fired.
//!
//! The ISR rung is narrow here and it is not dead. A recovery starts because
//! the controller's liveness registry called every ISR member dead, and it
//! leaves the ISR in the record alone while it polls, so a member that comes
//! back before the poll lands answers it while still named in that ISR. That
//! replica is complete, and reporting its election as the most complete
//! surviving log would move the unclean-election meter, take the break-glass
//! bypass audit path, and be refused outright under `unclean.recovery.require`
//! -- three wrong answers about an election that lost nothing.
//!
//! The ordering does not depend on `unclean.recovery.strategy`. Kafka's check
//! is strategy-blind, and so is this one: a `Balanced` recovery prefers a
//! complete replica over a longer incomplete log, and so does an `Aggressive`
//! one.
//!
//! # A witness is never a candidate
//!
//! `isAcceptableLeader` is the second half of the test quoted above, and
//! krabka's version of it carries one rule Kafka has no role for. A
//! `broker.witness` node replicates the partition and counts toward
//! `min.insync.replicas`, but it serves no client and must never lead: that is
//! what the role means, and every other election path enforces it through
//! [`witness_node_ids`](crate::config_keys::witness_node_ids) -- `failover_one`
//! for both failover scans, `ElectLeaders` for the operator-typed elections. A
//! witness elected here would be installed as the partition's singleton ISR
//! and would then answer no produce and no fetch, leaving the partition as
//! unusable as the offline one recovery started from.
//!
//! [`select_leader`] therefore drops the witnesses before any rule reads the
//! responses, ISR and ELR membership included. A witness can sit in both sets
//! -- it is a full ISR member and it leaves an under-min-ISR set like any
//! other replica, so its completeness is real -- but completeness is not what
//! disqualifies it. Losing its vote can leave the fallback as the only
//! election available, and that election is then reported as the data-losing
//! one it is rather than hidden behind a leader nobody can reach.

use {
    krabka_raft::NodeId,
    krabka_verified::consensus::RecoveryCandidate,
    std::collections::{BTreeSet, HashSet},
};

/// One replica's reported log state, from a `GetReplicaLogInfo` response.
///
/// This type is separate from the generated wire type, so a unit test can
/// drive the selection logic without building protocol structs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplicaLogInfo {
    pub broker_id: NodeId,
    pub last_written_leader_epoch: i32,
    pub log_end_offset: i64,
    pub current_leader_epoch: i32,
}

/// Picks the replica with the most complete log. It ranks by the highest
/// `last_written_leader_epoch`, then the highest `log_end_offset`, then the
/// lowest `broker_id` for determinism. Returns `None` for an empty input.
pub(crate) fn select_best_replica(responses: &[ReplicaLogInfo]) -> Option<NodeId> {
    let candidates: Vec<RecoveryCandidate> = responses
        .iter()
        .map(|r| RecoveryCandidate {
            last_epoch: r.last_written_leader_epoch,
            log_end_offset: r.log_end_offset,
            broker_id: r.broker_id.0,
        })
        .collect();
    krabka_verified::consensus::select_best_recovery_replica(&candidates)
        .map(|index| responses[index].broker_id)
}

/// Which rule chose the leader, and so whether the election lost anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ElectionBasis {
    /// The partition record still names the winner in its ISR, so it holds
    /// every committed record. This is the first disjunct of Kafka's
    /// `isValidNewLeader`, and Kafka reports the election as clean.
    InSyncReplica,
    /// The winner was in the partition's eligible-leader-replica set, so it
    /// held every committed record when it left the ISR. Kafka reports this
    /// election as clean.
    EligibleLeaderReplica,
    /// Nothing known to be complete answered, so the winner is only the most
    /// complete log among the survivors that did. Committed records the
    /// partition acknowledged may not be in it.
    MostCompleteLog,
}

impl ElectionBasis {
    /// Whether this election may have dropped committed records. It is the
    /// `unclean` flag on Kafka's `PartitionChangeBuilder.ElectionResult`.
    pub(crate) fn loses_data(self) -> bool { self == Self::MostCompleteLog }

    /// The clause an operator-facing log line or audit reason reads, after the
    /// broker id: "elected broker 3 <this>".
    pub(crate) fn describe(self) -> &'static str {
        match self {
            Self::InSyncReplica => {
                "from the partition's in-sync replicas, so no committed record is lost"
            }
            Self::EligibleLeaderReplica => {
                "from the eligible leader replicas, so no committed record is lost"
            }
            Self::MostCompleteLog => {
                "as the most complete surviving log, so committed records may be lost"
            }
        }
    }
}

/// The leader one recovery elects, and the rule that chose it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Election {
    pub leader: NodeId,
    pub basis: ElectionBasis,
}

/// Picks the leader out of the replicas that answered the poll.
///
/// `in_sync` is the ISR the partition record still names and `eligible` is its
/// published eligible-leader-replica set. A responder in either wins outright,
/// however short its log, because it is known to hold every committed record
/// and a longer log that is in neither is not. The ISR comes first, which is
/// the order of the two disjuncts in Kafka's `isValidNewLeader`: its ELR
/// disjunct is guarded on `targetIsr.isEmpty()`, so an ISR member is a valid
/// leader whether or not the ELR names anyone.
///
/// [`select_best_replica`] then ranks within whichever group won, so a
/// partition with several surviving members of it elects one of them
/// deterministically. Kafka takes the first such replica in assignment order
/// instead; every member of the winning group is equally complete, so the two
/// choices differ only in which correct answer they give, and reusing the one
/// comparator keeps a single ranking in the code.
///
/// `witnesses` is the cluster's `broker.witness` set, and no rule may elect
/// one, so they leave before any rule runs. A witness in the ISR or the ELR is
/// still a node that serves no client, and electing it would install a leader
/// no producer or consumer can reach.
///
/// Returns `None` when nothing that may lead answered.
pub(crate) fn select_leader(
    responses: &[ReplicaLogInfo],
    in_sync: &[NodeId],
    eligible: &[i32],
    witnesses: &HashSet<NodeId>,
) -> Option<Election> {
    let in_sync: BTreeSet<NodeId> = in_sync.iter().copied().collect();
    let eligible: BTreeSet<i32> = eligible.iter().copied().collect();
    let electable: Vec<ReplicaLogInfo> = responses
        .iter()
        .copied()
        .filter(|r| !witnesses.contains(&r.broker_id))
        .collect();
    let from_isr: Vec<ReplicaLogInfo> = electable
        .iter()
        .copied()
        .filter(|r| in_sync.contains(&r.broker_id))
        .collect();
    if let Some(leader) = select_best_replica(&from_isr) {
        return Some(Election {
            leader,
            basis: ElectionBasis::InSyncReplica,
        });
    }
    let from_elr: Vec<ReplicaLogInfo> = electable
        .iter()
        .copied()
        .filter(|r| i32::try_from(r.broker_id.0).is_ok_and(|id| eligible.contains(&id)))
        .collect();
    if let Some(leader) = select_best_replica(&from_elr) {
        return Some(Election {
            leader,
            basis: ElectionBasis::EligibleLeaderReplica,
        });
    }
    select_best_replica(&electable).map(|leader| Election {
        leader,
        basis: ElectionBasis::MostCompleteLog,
    })
}

/// Returns true if any responder reports a `current_leader_epoch` strictly
/// greater than the controller's known `leader_epoch` for the partition. A
/// newer leader then already exists, and this recovery is stale.
pub(crate) fn has_newer_leader(responses: &[ReplicaLogInfo], known_leader_epoch: i32) -> bool {
    responses
        .iter()
        .any(|r| r.current_leader_epoch > known_leader_epoch)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::leader_election::test_support::{no_witnesses, witnesses},
        assert2::{assert, check},
    };

    fn ri(broker_id: u64, epoch: i32, leo: i64) -> ReplicaLogInfo {
        ReplicaLogInfo {
            broker_id: NodeId(broker_id),
            last_written_leader_epoch: epoch,
            log_end_offset: leo,
            current_leader_epoch: epoch,
        }
    }

    /// The most-complete-log ranking of KIP-966 unclean recovery, one
    /// surviving-replica scenario per row.
    #[test]
    fn the_most_complete_log_is_the_newest_epoch_then_the_longest_log() {
        let cases = [
            (
                "a newer epoch beats a longer log",
                vec![ri(2, 4, 100), ri(3, 5, 10)],
                Some(NodeId(3)),
            ),
            (
                "the longer log wins within an epoch",
                vec![ri(2, 5, 90), ri(3, 5, 120)],
                Some(NodeId(3)),
            ),
            (
                "the lowest broker id breaks a full tie",
                vec![ri(3, 5, 100), ri(1, 5, 100), ri(2, 5, 100)],
                Some(NodeId(1)),
            ),
            ("nobody answered", vec![], None),
        ];
        for (label, responses, expected) in cases {
            check!(select_best_replica(&responses) == expected, "{label}");
        }
    }

    /// The eligible-leader-replica set outranks the log length. Each case
    /// gives the same three responders a different ELR and names the whole
    /// election it must produce.
    #[test]
    fn the_eligible_leader_replicas_outrank_the_longest_log() {
        // Broker 2 holds the longest log; brokers 1 and 3 are shorter, and 3
        // is shorter than 1.
        let responses = [ri(1, 5, 40), ri(2, 5, 400), ri(3, 5, 20)];
        let elr_of = |leader: u64| Election {
            leader: NodeId(leader),
            basis: ElectionBasis::EligibleLeaderReplica,
        };
        let cases = [
            (
                "the shortest log wins when it is the only ELR member",
                vec![3],
                Some(elr_of(3)),
            ),
            (
                "the best ELR member wins when several answered",
                vec![1, 3],
                Some(elr_of(1)),
            ),
            (
                "an empty ELR falls back to the most complete log",
                vec![],
                Some(Election {
                    leader: NodeId(2),
                    basis: ElectionBasis::MostCompleteLog,
                }),
            ),
            (
                "an ELR member that did not answer cannot be elected",
                vec![9],
                Some(Election {
                    leader: NodeId(2),
                    basis: ElectionBasis::MostCompleteLog,
                }),
            ),
        ];
        for (label, eligible, expected) in cases {
            check!(
                select_leader(&responses, &[], &eligible, &no_witnesses()) == expected,
                "{label}"
            );
        }
    }

    /// A witness replicates the partition and can be published as an eligible
    /// leader replica, and neither rule may elect one: the node serves no
    /// client, so a partition it leads is as unusable as the offline one. Each
    /// case gives the same three responders a different ELR and witness set,
    /// and names the whole election it must produce.
    #[test]
    fn no_rule_elects_a_witness() {
        // Broker 2 holds the longest log, broker 1 is shorter, and broker 3 is
        // shortest.
        let responses = [ri(1, 5, 40), ri(2, 5, 400), ri(3, 5, 20)];
        let fallback_to = |leader: u64| {
            Some(Election {
                leader: NodeId(leader),
                basis: ElectionBasis::MostCompleteLog,
            })
        };
        let cases = [
            (
                "the only ELR member is a witness, so the fallback decides",
                vec![3],
                vec![3],
                fallback_to(2),
            ),
            (
                "a data ELR member still outranks the longest log",
                vec![1, 3],
                vec![3],
                Some(Election {
                    leader: NodeId(1),
                    basis: ElectionBasis::EligibleLeaderReplica,
                }),
            ),
            (
                "the fallback skips the witness that holds the longest log",
                vec![],
                vec![2],
                fallback_to(1),
            ),
            (
                "nothing that may lead answered",
                vec![3],
                vec![1, 2, 3],
                None,
            ),
        ];
        for (label, eligible, witness_ids, expected) in cases {
            check!(
                select_leader(&responses, &[], &eligible, &witnesses(&witness_ids)) == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn no_response_elects_nobody_however_the_isr_and_elr_read() {
        check!(select_leader(&[], &[], &[], &no_witnesses()) == None);
        check!(select_leader(&[], &[], &[1, 2], &no_witnesses()) == None);
        check!(select_leader(&[], &[NodeId(1)], &[2], &no_witnesses()) == None);
    }

    /// The first disjunct of Kafka's `isValidNewLeader`: a responder the
    /// partition record still names in its ISR is a valid new leader outright,
    /// and Kafka's `electAnyLeader` returns it as `ElectionResult(node,
    /// false)`. Its ELR disjunct is guarded on `targetIsr.isEmpty()`, so it
    /// never overrides one. Each case gives the same three responders a
    /// different ISR and ELR and names the whole election it must produce.
    #[test]
    fn an_in_sync_replica_outranks_the_elr_and_the_longest_log() {
        /// One row: the ISR the record names, the published ELR, and the whole
        /// election they must produce out of the responders below.
        struct IsrCase<'a> {
            label: &'a str,
            in_sync: &'a [u64],
            eligible: &'a [i32],
            expected: Option<Election>,
        }

        // Broker 2 holds the longest log, broker 1 is shorter, and broker 3 is
        // shortest.
        let responses = [ri(1, 5, 40), ri(2, 5, 400), ri(3, 5, 20)];
        let in_sync_of = |leader: u64| {
            Some(Election {
                leader: NodeId(leader),
                basis: ElectionBasis::InSyncReplica,
            })
        };
        let cases = [
            IsrCase {
                label: "the shortest log wins when it is the only ISR member left",
                in_sync: &[3],
                eligible: &[],
                expected: in_sync_of(3),
            },
            IsrCase {
                label: "an ISR member beats an ELR member with a longer log",
                in_sync: &[3],
                eligible: &[2],
                expected: in_sync_of(3),
            },
            IsrCase {
                label: "the best ISR member wins when several answered",
                in_sync: &[1, 3],
                eligible: &[2],
                expected: in_sync_of(1),
            },
            IsrCase {
                label: "an ISR member that did not answer decides nothing",
                in_sync: &[9],
                eligible: &[3],
                expected: Some(Election {
                    leader: NodeId(3),
                    basis: ElectionBasis::EligibleLeaderReplica,
                }),
            },
        ];
        for case in cases {
            let in_sync: Vec<NodeId> = case.in_sync.iter().copied().map(NodeId).collect();
            check!(
                select_leader(&responses, &in_sync, case.eligible, &no_witnesses())
                    == case.expected,
                "{}",
                case.label
            );
        }
    }

    /// A witness is a full ISR member, and the rule that keeps it from leading
    /// runs ahead of every election rule, so naming it in the ISR changes
    /// nothing.
    #[test]
    fn an_in_sync_witness_is_still_never_elected() {
        let responses = [ri(1, 5, 40), ri(2, 5, 400)];
        check!(
            select_leader(&responses, &[NodeId(2)], &[], &witnesses(&[2]))
                == Some(Election {
                    leader: NodeId(1),
                    basis: ElectionBasis::MostCompleteLog,
                })
        );
    }

    #[test]
    fn only_the_fallback_loses_data() {
        check!(ElectionBasis::MostCompleteLog.loses_data());
        check!(!ElectionBasis::EligibleLeaderReplica.loses_data());
        check!(!ElectionBasis::InSyncReplica.loses_data());
    }

    #[test]
    fn newer_leader_detected() {
        let r = [ReplicaLogInfo {
            broker_id: NodeId(2),
            last_written_leader_epoch: 5,
            log_end_offset: 10,
            current_leader_epoch: 7,
        }];
        assert!(has_newer_leader(&r, 6));
        assert!(!has_newer_leader(&r, 7));
    }
}
