//! Decides whether a leader partition's ISR should change. It holds the
//! `replica_state` lock once and returns everything the scan loop needs, so
//! the shrink and expand rules are testable without the surrounding scan.

use {
    crate::{
        partition::Partition,
        replica_state::{FollowerStats, LeaderPolicy, ReplicaState},
    },
    krabka_ids::LeaderEpoch,
    krabka_log::Offset,
    krabka_metadata::PartitionRecord,
    krabka_raft::NodeId,
    krabka_verified::isr::{IsrCandidateFacts, IsrMemberRole},
    std::{
        collections::{BTreeSet, HashSet},
        time::Instant,
    },
};

/// A computed ISR change proposal. `compute_proposal` captures all fields
/// within its single `replica_state` lock scope, so the caller
/// can classify the shrink or expand and submit the proposal without a second
/// lock. That also removes the TOCTOU window in which the ISR could shift
/// between two locks.
#[derive(Debug, PartialEq)]
pub(super) struct Proposal {
    /// The pre-proposal ISR, sorted. The caller uses it to classify the
    /// shrink or expand metric.
    pub(super) prev_isr: Vec<NodeId>,
    /// The proposed new ISR, sorted. It is always `!= prev_isr`.
    pub(super) new_isr: Vec<NodeId>,
    /// Leader epoch to stamp on the `AlterPartition` request.
    pub(super) leader_epoch: LeaderEpoch,
    /// Partition epoch of the committed ISR `prev_isr` came from, to stamp on
    /// the `AlterPartition` request. The controller refuses the proposal with
    /// `INVALID_UPDATE_VERSION` once that ISR has been replaced.
    pub(super) partition_epoch: i32,
}

/// Where this leader's leader epoch starts: Kafka's
/// `Partition.leaderEpochStartOffsetOpt`, which `makeLeader` records as the
/// log end offset at the moment this broker took the epoch over.
///
/// `None` when the log records no start for `epoch`, which Kafka's rule reads
/// as "admit nobody yet".
fn leader_epoch_start(part: &Partition, epoch: LeaderEpoch) -> Option<Offset> {
    let log = part
        .log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    log.epoch_checkpoint()
        .entries()
        .iter()
        .find(|entry| entry.epoch == epoch)
        .map(|entry| entry.start_offset)
}

/// Returns `Some(Proposal)` if the ISR should change, else `None`.
///
/// `record` is the partition as the current metadata image holds it, and
/// `policy` what that image and this broker's configuration say about it.
/// The policy is installed on the partition's replica state first, so the
/// high watermark reads the same `min.insync.replicas`, lag bound and broker
/// standing the scan does.
pub(super) async fn compute_proposal(
    part: &Partition,
    record: &PartitionRecord,
    policy: LeaderPolicy,
) -> Option<Proposal> {
    // Kafka's `Partition.getOutOfSyncReplicas` reads the leader's log end
    // offset once per scan, as here.
    let leader_leo = part.log_end_offset();
    let epoch_start = leader_epoch_start(part, LeaderEpoch(record.leader_epoch.0));
    let mut st = part.replica_state.lock().await;
    st.set_policy(policy);
    proposal_at(&st, record, leader_leo, epoch_start, Instant::now())
}

/// [`compute_proposal`] at the scan instant `now`, with the leader's log
/// ending at `leader_leo` and its epoch starting at `epoch_start`.
fn proposal_at(
    st: &ReplicaState,
    record: &PartitionRecord,
    leader_leo: Offset,
    epoch_start: Option<Offset>,
    now: Instant,
) -> Option<Proposal> {
    // Capture the pre-proposal ISR (sorted) once, inside this lock scope.
    let mut prev_isr: Vec<NodeId> = st.isr.iter().copied().collect();
    prev_isr.sort_unstable();
    let (Some(leader), replicas) = st.leader_and_replicas() else {
        return None;
    };
    // A malformed metadata installation must never propose an ISR without its
    // assigned leader. Normal installations establish both facts together.
    if !replicas.contains(&leader) || !st.isr.contains(&leader) {
        return None;
    }
    // The proposal carries the image's partition epoch, so it must be built
    // on the image's ISR at the image's leader epoch. Until this leader has
    // installed that state, wait for a later scan rather than stamp a newer
    // epoch on an older ISR.
    let image_isr: HashSet<NodeId> = record.isr.iter().copied().collect();
    if st.current_leader_epoch != LeaderEpoch(record.leader_epoch.0) || image_isr != st.isr {
        return None;
    }

    // BTreeSet supplies one sorted decision per assigned/current replica. The
    // kernel then proves the exact keep/remove/add rule for each member.
    let candidates: BTreeSet<NodeId> = replicas.union(&st.isr).copied().collect();
    let mut new_isr = Vec::with_capacity(candidates.len());
    for node in candidates {
        let stats = st.per_follower.get(&node);
        let role = if !replicas.contains(&node) {
            IsrMemberRole::Unassigned
        } else if node == leader {
            IsrMemberRole::Leader
        } else if st.isr.contains(&node) {
            IsrMemberRole::InSyncFollower
        } else {
            IsrMemberRole::OutOfSyncFollower
        };
        if matches!(role, IsrMemberRole::OutOfSyncFollower)
            && !stats.is_some_and(|stats| stats.fetched_since_isr_exit)
        {
            continue;
        }
        // A replica with no recorded progress has log end -1 and is out of
        // sync, as Kafka's `Partition.isFollowerOutOfSync` treats a replica
        // it has no state for.
        if krabka_verified::isr::isr_candidate_selected(IsrCandidateFacts {
            role,
            follower_log_end: stats.map_or(-1, FollowerStats::log_end),
            leader_log_end: leader_leo.0,
            leader_high_watermark: st.hw.0,
            leader_epoch_start: epoch_start.map(|start| start.0),
            caught_up_within_lag: st.within_lag(stats.and_then(|stats| stats.last_caught_up), now),
            eligibility: st.eligibility(node),
        }) {
            new_isr.push(node);
        }
    }
    let removed = prev_isr
        .iter()
        .filter(|node| new_isr.binary_search(node).is_err())
        .count();
    let added = new_isr
        .iter()
        .filter(|node| prev_isr.binary_search(node).is_err())
        .count();
    if krabka_verified::isr::isr_proposal_changed(removed, added) {
        Some(Proposal {
            prev_isr,
            new_isr,
            leader_epoch: st.current_leader_epoch,
            partition_epoch: record.partition_epoch,
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::replica_state::{BrokerStanding, FollowerStats},
        krabka_metadata::PartitionRecord,
        std::{collections::HashMap, time::Duration},
    };

    /// Every scan below: the leader's log ends at 100, its high watermark is
    /// 90, and its epoch began at offset 80.
    const LEADER_LEO: Offset = Offset(100);
    const HW: Offset = Offset(90);
    const EPOCH_START: Option<Offset> = Some(Offset(80));

    fn nodes(ids: &[u64]) -> Vec<NodeId> { ids.iter().copied().map(NodeId).collect() }

    /// Partition 0 of "t", led by broker 1 at leader epoch 7 and partition
    /// epoch 3.
    fn record(isr: &[u64], replicas: &[u64]) -> PartitionRecord {
        PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: nodes(replicas),
            isr: nodes(isr),
            leader_epoch: krabka_metadata::LeaderEpoch(7),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 3,
        }
    }

    /// Brokers 1 to 3 registered and unfenced at broker epoch 5, with a 1 s
    /// `replica.lag.time.max.ms` and `min.insync.replicas` 1.
    fn policy() -> LeaderPolicy {
        LeaderPolicy {
            effective_min_isr: 1,
            replica_lag_time_max: Duration::from_millis(1_000),
            brokers: (1..=3)
                .map(|node| {
                    (NodeId(node), BrokerStanding {
                        fenced: false,
                        shutting_down: false,
                        alive_epoch: Some(5),
                    })
                })
                .collect::<HashMap<_, _>>(),
        }
    }

    /// The leader's state once it installed `record` at `installed` under
    /// `policy`, with its high watermark at [`HW`].
    fn installed(
        record: &PartitionRecord,
        installed: Instant,
        policy: LeaderPolicy,
    ) -> ReplicaState {
        let mut st = ReplicaState::new();
        st.install_isr(&record.isr, &record.replicas, record.leader, installed);
        st.current_leader_epoch = LeaderEpoch(record.leader_epoch.0);
        st.set_policy(policy);
        st.hw = HW;
        st
    }

    /// A follower at `leo` that last fetched `fetched_ms_ago` and last caught
    /// up `caught_up_ms_ago` before `now`; `None` means never. Its fetches
    /// carried its registered broker epoch, 5.
    fn follower(
        now: Instant,
        leo: i64,
        fetched_ms_ago: Option<u64>,
        caught_up_ms_ago: Option<u64>,
    ) -> FollowerStats {
        let ago = |ms: u64| {
            now.checked_sub(Duration::from_millis(ms))
                .expect("representable")
        };
        FollowerStats {
            leo: Offset(leo),
            last_fetch: fetched_ms_ago.map(ago),
            last_fetch_leader_leo: Offset(leo),
            last_caught_up: caught_up_ms_ago.map(ago),
            broker_epoch: Some(5),
            fetched_since_isr_exit: fetched_ms_ago.is_some(),
        }
    }

    fn proposal(prev_isr: &[u64], new_isr: &[u64]) -> Proposal {
        Proposal {
            prev_isr: nodes(prev_isr),
            new_isr: nodes(new_isr),
            leader_epoch: LeaderEpoch(7),
            partition_epoch: 3,
        }
    }

    /// A scan row: its label, the installed ISR, the replica set, the
    /// followers' progress, and the proposal the scan must make.
    type ScanCase<'a> = (
        &'a str,
        &'a [u64],
        &'a [u64],
        Vec<(u64, FollowerStats)>,
        Option<Proposal>,
    );

    /// One scan with a 1 s `replica.lag.time.max.ms`. In-sync followers follow
    /// Kafka's `Partition.getOutOfSyncReplicas`, out-of-sync ones
    /// `Partition.needsExpandIsr`.
    #[test]
    fn a_scan_proposes_the_isr_kafkas_rules_select() {
        let now = Instant::now() + Duration::from_secs(60);
        let follower_pair =
            |candidate| vec![(2, follower(now, 100, Some(10), Some(10))), (3, candidate)];
        let cases: [ScanCase<'_>; 10] = [
            (
                "every follower caught up recently",
                &[1, 2, 3],
                &[1, 2, 3],
                vec![
                    (2, follower(now, 90, Some(10), Some(500))),
                    (3, follower(now, 80, Some(10), Some(1_000))),
                ],
                None,
            ),
            (
                "a follower fetching but not caught up for too long leaves",
                &[1, 2, 3],
                &[1, 2, 3],
                vec![
                    (2, follower(now, 90, Some(10), Some(10))),
                    (3, follower(now, 80, Some(10), Some(1_001))),
                ],
                Some(proposal(&[1, 2, 3], &[1, 2])),
            ),
            (
                "an idle follower at the leader's log end stays",
                &[1, 2],
                &[1, 2],
                vec![(2, follower(now, 100, Some(30_000), Some(30_000)))],
                None,
            ),
            (
                "a follower that never fetched from this leader leaves after the bound",
                &[1, 2],
                &[1, 2],
                vec![(2, follower(now, 100, None, Some(1_001)))],
                Some(proposal(&[1, 2], &[1])),
            ),
            (
                "an out-of-sync follower at the high watermark joins",
                &[1, 2],
                &[1, 2, 3],
                follower_pair(follower(now, 90, Some(30_000), Some(30_000))),
                Some(proposal(&[1, 2], &[1, 2, 3])),
            ),
            (
                "an out-of-sync follower one short of the high watermark stays out",
                &[1, 2],
                &[1, 2, 3],
                follower_pair(follower(now, 89, Some(10), Some(10))),
                None,
            ),
            (
                "a follower past the high watermark that has not fetched from this leader stays out",
                &[1, 2],
                &[1, 2, 3],
                follower_pair(follower(now, 100, None, Some(10))),
                None,
            ),
            (
                "an out-of-sync replica with no progress stays out",
                &[1, 2],
                &[1, 2, 3],
                vec![(2, follower(now, 100, Some(10), Some(10)))],
                None,
            ),
            (
                "a follower whose fetch carried a stale broker epoch stays out",
                &[1, 2],
                &[1, 2, 3],
                vec![
                    (2, follower(now, 100, Some(10), Some(10))),
                    (3, FollowerStats {
                        broker_epoch: Some(4),
                        ..follower(now, 100, Some(10), Some(10))
                    }),
                ],
                None,
            ),
            (
                "the leader's own lag and a duplicate assignment change nothing",
                &[1, 1, 2, 2],
                &[1, 1, 2],
                vec![
                    (1, follower(now, 0, Some(30_000), Some(30_000))),
                    (2, follower(now, 100, Some(10), Some(10))),
                ],
                None,
            ),
        ];
        for (label, isr, replicas, followers, expected) in cases {
            let record = record(isr, replicas);
            let mut st = installed(&record, now, policy());
            for (node, stats) in followers {
                st.per_follower.insert(NodeId(node), stats);
            }
            assert2::check!(
                proposal_at(&st, &record, LEADER_LEO, EPOCH_START, now) == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn a_shrunk_follower_needs_a_new_fetch_before_rejoining() {
        let now = Instant::now() + Duration::from_secs(60);
        let mut committed = record(&[1, 2, 3], &[1, 2, 3]);
        let mut st = installed(&committed, now, policy());
        st.per_follower
            .insert(NodeId(2), follower(now, 90, Some(500), Some(500)));
        st.per_follower
            .insert(NodeId(3), follower(now, 90, Some(1_001), Some(1_001)));
        assert2::assert!(
            proposal_at(&st, &committed, LEADER_LEO, EPOCH_START, now)
                == Some(proposal(&[1, 2, 3], &[1, 2]))
        );
        committed.isr = nodes(&[1, 2]);
        st.install_isr(&committed.isr, &committed.replicas, committed.leader, now);
        let later = now + Duration::from_secs(1);
        assert2::assert!(
            proposal_at(&st, &committed, LEADER_LEO, EPOCH_START, later)
                == Some(proposal(&[1, 2], &[1]))
        );
        st.update_follower_leo(NodeId(3), HW, LEADER_LEO, later);
        assert2::assert!(
            proposal_at(&st, &committed, LEADER_LEO, EPOCH_START, later)
                == Some(proposal(&[1, 2], &[1, 3]))
        );
    }

    /// Kafka's `Partition.isFollowerInSync` also asks for the start of the
    /// leader's epoch, and `isReplicaIsrEligible` for an unfenced broker:
    /// follower 3, at offset 95 past the high watermark, is admitted only
    /// when the leader's epoch began at or below 95 and the image does not
    /// fence it.
    #[test]
    fn expansion_waits_for_the_epoch_start_and_an_unfenced_broker() {
        let now = Instant::now() + Duration::from_secs(60);
        let record = record(&[1, 2], &[1, 2, 3]);
        let fenced_3 = || {
            let mut fenced = policy();
            fenced.brokers.insert(NodeId(3), BrokerStanding {
                fenced: true,
                shutting_down: false,
                alive_epoch: None,
            });
            fenced
        };
        let joins = Some(proposal(&[1, 2], &[1, 2, 3]));
        for (label, epoch_start, policy, expected) in [
            (
                "an epoch that began at 80",
                Some(Offset(80)),
                policy(),
                &joins,
            ),
            (
                "an epoch that began at 95",
                Some(Offset(95)),
                policy(),
                &joins,
            ),
            (
                "an epoch that began at 96",
                Some(Offset(96)),
                policy(),
                &None,
            ),
            ("an unknown epoch start", None, policy(), &None),
            ("a fenced follower", Some(Offset(80)), fenced_3(), &None),
        ] {
            let mut st = installed(&record, now, policy);
            st.per_follower
                .insert(NodeId(2), follower(now, 100, Some(10), Some(10)));
            st.per_follower
                .insert(NodeId(3), follower(now, 95, Some(10), Some(10)));
            assert2::check!(
                &proposal_at(&st, &record, LEADER_LEO, epoch_start, now) == expected,
                "{label}"
            );
        }
    }

    /// A proposal carries the image's partition epoch, so it waits until the
    /// leader has installed the image's ISR at the image's leader epoch.
    #[test]
    fn a_scan_waits_for_the_image_state_before_proposing() {
        let now = Instant::now() + Duration::from_secs(60);
        let stale = record(&[1, 2], &[1, 2]);
        for (label, image) in [
            ("the image has a newer leader epoch", PartitionRecord {
                leader_epoch: krabka_metadata::LeaderEpoch(8),
                ..stale.clone()
            }),
            ("the image has a different ISR", record(&[1], &[1, 2])),
        ] {
            let mut st = installed(&stale, now, policy());
            st.per_follower
                .insert(NodeId(2), follower(now, 0, Some(30_000), Some(30_000)));
            assert2::check!(
                proposal_at(&st, &image, LEADER_LEO, EPOCH_START, now).is_none(),
                "{label}"
            );
        }
    }

    /// The leader appends 100 records every 100 ms and both followers fetch
    /// every 100 ms, right after the append. Follower 2 fetches from the log
    /// end it was last shown, which Kafka credits as caught up at its
    /// previous fetch. Follower 3 always fetches 50 records short of that, so
    /// it never catches up. With a 1 s `replica.lag.time.max.ms`, Kafka's
    /// `Partition.getOutOfSyncReplicas` keeps follower 3 for exactly 1 s after
    /// the ISR was installed and drops it at the first scan past that, however
    /// often it fetches; follower 2 stays throughout.
    #[test]
    fn a_follower_that_fetches_but_never_catches_up_is_removed_after_the_lag_bound() {
        let t0 = Instant::now();
        let record = record(&[1, 2, 3], &[1, 2, 3]);
        let mut st = installed(&record, t0, policy());
        st.hw = Offset(0);
        let mut first_proposal = None;
        for tick in 0..30_i64 {
            let now = t0 + Duration::from_millis(u64::try_from(tick * 100).expect("tick"));
            let leader_leo = Offset(100 * (tick + 1));
            let shown = 100 * tick;
            st.update_follower_leo(NodeId(2), Offset(shown), leader_leo, now);
            st.update_follower_leo(NodeId(3), Offset((shown - 50).max(0)), leader_leo, now);
            if let Some(proposal) = proposal_at(&st, &record, leader_leo, Some(Offset(0)), now) {
                first_proposal = Some((tick, proposal));
                break;
            }
        }
        assert2::assert!(first_proposal == Some((11, proposal(&[1, 2, 3], &[1, 2]))));
    }
}
