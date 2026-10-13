//! The controller's reaction to a broker that went down, and the state change
//! an election leaves behind.
//!
//! `do_failover` projects the model state onto a `PartitionRecord` and lets
//! the real `failover_one` — and, on the KIP-966 empty-ISR path, the real
//! `select_leader` — take the decision. It hands `failover_one` the
//! eligible-leader set the model has been maintaining, exactly as the
//! production scan hands it the published one, so a partition whose live ISR
//! has emptied elects a surviving ELR member cleanly before either the
//! offset-aware recovery or the KIP-841 election is consulted.
//!
//! Every election then goes through [`apply_elect`], which is split by the
//! [`Rule`] that chose the leader, because each rule owes the durability
//! ghosts something different. It is the only place where committed data can
//! be declared lost, so the loss characterisation sits next to the election
//! that causes it.
//!
//! # The obligation an ELR election answers to
//!
//! An ELR election is reported as losing nothing: `failover_one` returns it
//! with `unclean: false`, and `select_leader` returns it with a basis whose
//! `ElectionBasis::loses_data` is `false` -- the predicate the
//! unclean-election counter, the audit reason and KFC-9's `require` gate all
//! read. That report is the claim this model exists to check.
//!
//! `committed` is everything that ever reached the HWM, the obligation a
//! consumer sees. The leader's high watermark stands still while the ISR is
//! under `min.insync.replicas`, as Kafka's `Partition.maybeIncrementLeaderHW`
//! does, so every committed record reached the HWM while at least min ISR
//! replicas held it -- and KIP-966 names a replica eligible exactly when it
//! left an ISR that was about to fall below min ISR. An ELR member therefore
//! holds every committed record, and an ELR election may drop none of them:
//! that is the `elr_election_keeps_every_committed_record` property. Only an
//! election reported as losing data may shorten `committed`.

use {
    super::{
        bounds::{NB_U8, has, model_broker, model_offset, node},
        elr::{ids, maintain, partition_record},
        state::{DpState, ELR_BEAT_LONGER_LOG, ELR_DROPPED_COMMITTED, ELR_ELECTED},
    },
    crate::{
        config_keys::RecoveryStrategy,
        elr::state::PartitionElr,
        leader_election::{FailoverDecision, Ladder, failover_one},
        unclean_recovery::{ReplicaLogInfo, select_leader},
    },
    krabka_metadata::MetadataImage,
    std::collections::HashSet,
};

/// The rule that chose a new leader, which is what decides what the election
/// may drop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rule {
    /// The winner was a live member of the ISR the partition record named:
    /// `failover_one`'s clean rung, or `select_leader`'s in-sync one. It holds
    /// every committed record, so nothing is truncated, and the
    /// `committed_durable` property is what checks that it really did.
    InSync,
    /// The winner came out of the published KIP-966 eligible-leader set, with
    /// no live ISR member left. Kafka calls the election clean, so it may drop
    /// no committed record, for the reason the module comment gives.
    Eligible,
    /// The KIP-841 out-of-ISR election, or the most complete surviving log
    /// the offset-aware recovery falls back to. Both are reported as losing
    /// data, and whatever committed record the winner lacks is lost.
    LossReported,
}

/// Apply a leader election chosen by `rule`. It sets the leader and ISR, bumps
/// the epoch, and settles what the election owes `committed`.
///
/// The new leader keeps only the committed prefix that it holds with the same
/// epoch. For [`Rule::InSync`] that prefix must be the whole of `committed`,
/// and nothing is cut here: a clean election that lacked a record would leave
/// `committed` pointing past the leader's log, which `committed_durable`
/// rejects. The other two rules cut `committed` back to that prefix and clamp
/// the HWM to the new leader's log. [`Rule::LossReported`] flags the cut in
/// `lost`; [`Rule::Eligible`] records any cut in `elr_trace`, where it is the
/// violation the ELR properties look for.
fn apply_elect(s: &mut DpState, new_leader: u8, isr_mask: u8, rule: Rule) {
    let winner_leo = model_offset(s.log[usize::from(new_leader)].len());
    let beat_longer = (0..NB_U8).any(|b| {
        b != new_leader && has(s.live, b) && model_offset(s.log[usize::from(b)].len()) > winner_leo
    });
    s.leader = new_leader;
    s.isr = isr_mask;
    s.leader_epoch += 1;
    if rule == Rule::InSync {
        return;
    }
    let nl = &s.log[usize::from(new_leader)];
    let kept = s
        .committed
        .iter()
        .enumerate()
        .take_while(|&(off, e)| nl.get(off) == Some(e))
        .count();
    let committed_dropped = kept < s.committed.len();
    if committed_dropped {
        s.committed.truncate(kept);
        s.hwm = s.hwm.min(winner_leo);
        if rule == Rule::LossReported {
            s.lost = true;
        }
    }
    if rule == Rule::Eligible {
        s.elr_trace |= ELR_ELECTED;
        if beat_longer {
            s.elr_trace |= ELR_BEAT_LONGER_LOG;
        }
        if committed_dropped {
            s.elr_trace |= ELR_DROPPED_COMMITTED;
        }
    }
}

/// The broker bitmask of a list of node ids.
fn mask_of(nodes: &[krabka_audit::NodeId]) -> u8 {
    nodes
        .iter()
        .fold(0u8, |m, &n| m | (1u8 << (model_broker(n.0))))
}

/// The controller's failover reaction when broker `dead` goes down. It drives
/// the real `failover_one` over the model's ISR, liveness and published
/// eligible-leader set, and applies that decision: an election from the ISR,
/// an election from the ELR, a KIP-841 election, or an ISR shrink. In the
/// unclean config it also drives the real KIP-966 `select_leader` for the
/// empty-ISR `Recover` path that `failover_one` defers to once no ELR member
/// can lead either.
///
/// Every decision that moves the leader or the ISR is followed by [`maintain`],
/// because every controller path that submits such a change runs
/// [`ElrPublisher`](crate::elr::ElrPublisher) over it. `image` is `None` for a
/// configuration that maintains no ELR, whose set then stays empty and whose
/// recovery therefore always reaches the most-complete-log fallback.
pub(super) fn do_failover(image: Option<&MetadataImage>, s: &mut DpState, dead: u8, unclean: bool) {
    let pr = partition_record(s.leader, s.isr, s.leader_epoch);
    let alive: HashSet<krabka_audit::NodeId> = (0..NB_U8)
        .filter(|&b| has(s.live, b))
        .map(|b| krabka_audit::NodeId(node(b)))
        .collect();
    // Clean config: strategy None + unclean disabled → only ISR and ELR
    // elections (else Unavailable). Unclean config: Balanced strategy defers
    // an empty-ISR partition with no electable ELR member to KIP-966
    // offset-aware recovery.
    let strategy = if unclean {
        RecoveryStrategy::Balanced
    } else {
        RecoveryStrategy::None
    };
    // This model has no witness broker: every replica can lead.
    let witnesses: HashSet<krabka_audit::NodeId> = HashSet::new();
    let elr = PartitionElr {
        eligible_leader_replicas: ids(s.elr),
        last_known_elr: Vec::new(),
    };
    let ladder = Ladder::builder()
        .alive(&alive)
        .witnesses(&witnesses)
        .elr(&elr)
        .strategy(strategy)
        .unclean_enabled(unclean)
        .build();
    let changed = match failover_one(&pr, krabka_audit::NodeId(node(dead)), &ladder) {
        FailoverDecision::Elect {
            leader,
            isr,
            unclean,
        } => {
            let winner = model_broker(leader.0);
            let rule = if unclean {
                Rule::LossReported
            } else if has(s.isr, winner) {
                Rule::InSync
            } else {
                // `failover_one` elects cleanly out of the ISR only through
                // its ELR rung, so the winner must be a published member.
                assert2::assert!(
                    has(s.elr, winner),
                    "a clean election picked {winner}, outside both the ISR and the ELR"
                );
                Rule::Eligible
            };
            apply_elect(s, winner, mask_of(&isr), rule);
            true
        }
        FailoverDecision::Recover(_) => recover(s, &witnesses),
        FailoverDecision::ShrinkIsr { isr } => {
            s.isr = mask_of(&isr);
            true
        }
        FailoverDecision::Unavailable | FailoverDecision::NoChange => false,
    };
    if let (true, Some(image)) = (changed, image) {
        maintain(image, s, &pr);
    }
}

/// KIP-966 offset-aware recovery: drive the REAL `select_leader` over the live
/// replicas' log info, the recorded ISR and the published eligible-leader set,
/// and install the winner with a singleton ISR.
///
/// `failover_one` reaches this path only once no live ISR member and no live
/// ELR member can lead, and the model runs the poll in the same step, so
/// `select_leader`'s first two rungs find nobody here and the
/// most-complete-log fallback decides. The rule is still read off the
/// election rather than assumed, so a change that let either rung fire would
/// be applied, and checked, as the rule it is.
///
/// Returns whether the partition changed, so the caller knows to republish.
fn recover(s: &mut DpState, witnesses: &HashSet<krabka_audit::NodeId>) -> bool {
    let infos: Vec<ReplicaLogInfo> = (0..NB_U8)
        .filter(|&b| has(s.live, b))
        .map(|b| ReplicaLogInfo {
            broker_id: krabka_audit::NodeId(node(b)),
            last_written_leader_epoch: s.log[usize::from(b)].last().map_or(0, |&e| i32::from(e)),
            log_end_offset: model_offset(s.log[usize::from(b)].len()),
            current_leader_epoch: i32::from(s.leader_epoch),
        })
        .collect();
    // This model does carry an ISR, so give `select_leader` the real one rather
    // than an empty slice: its first rung elects an in-sync survivor cleanly,
    // and feeding it nothing would hide that rung from the model entirely.
    let in_sync = s.in_sync_nodes();
    let Some(election) = select_leader(&infos, &in_sync, &ids(s.elr), witnesses) else {
        return false;
    };
    let winner = model_broker(election.leader.0);
    // `loses_data` is the predicate itself: it is what the unclean-election
    // counter, the audit reason and KFC-9's `require` gate each read, so a
    // basis that answers `false` here is one production has already reported
    // as losing nothing.
    let rule = if election.basis.loses_data() {
        Rule::LossReported
    } else if has(s.isr, winner) {
        Rule::InSync
    } else {
        Rule::Eligible
    };
    apply_elect(s, winner, 1u8 << winner, rule);
    true
}
