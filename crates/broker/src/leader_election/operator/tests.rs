//! Tests for the operator-triggered elections: the preferred-leader happy
//! path and every refusal it can report, the unclean election an operator
//! forces after the ISR is gone, and the witness replica that neither may give
//! leadership to.

use assert2::assert;
use krabka_metadata::{LeaderEpoch, MetadataImage};
use uuid::Uuid;

use super::*;
use crate::leader_election::test_support::{
    ElectionSetup, ExpectedPartitionSetup, alive_set, img_with_partition, no_witnesses,
    register_broker_with_dirs, witnesses,
};

#[tokio::test]
async fn preferred_happy_path() {
    let img = img_with_partition(ElectionSetup {
        topic: "foo",
        leader: krabka_raft::NodeId(2),
        ..Default::default()
    });
    let l = alive_set(&[1, 2, 3]);
    let new_pr = select_new_leader_for_partition(
        &img,
        &l,
        &no_witnesses(),
        "foo",
        0,
        ElectionType::Preferred,
    )
    .expect("should elect");
    let expected =
        crate::leader_election::test_support::expected_partition(ExpectedPartitionSetup {
            topic: "foo",
            leader: krabka_raft::NodeId(1),
            isr: &[
                krabka_raft::NodeId(1),
                krabka_raft::NodeId(2),
                krabka_raft::NodeId(3),
            ],
            ..Default::default()
        });
    assert!(new_pr == expected);
}

#[tokio::test]
async fn preferred_election_rejects_exhausted_metadata_epochs() {
    for (partition_epoch, leader_epoch) in [(i32::MAX, 5), (0, i32::MAX)] {
        let mut img = img_with_partition(ElectionSetup {
            topic: "foo",
            leader: krabka_raft::NodeId(2),
            ..Default::default()
        });
        let mut record = img.partition("foo", 0).expect("seeded partition").clone();
        record.partition_epoch = partition_epoch;
        record.leader_epoch = LeaderEpoch(leader_epoch);
        img.apply(&krabka_metadata::MetadataRecord::V1Partition(record));
        let l = alive_set(&[1, 2, 3]);

        let error = select_new_leader_for_partition(
            &img,
            &l,
            &no_witnesses(),
            "foo",
            0,
            ElectionType::Preferred,
        )
        .expect_err("exhausted epoch must fail closed");

        assert!(error == ElectError::EpochExhausted);
    }
}

#[tokio::test]
async fn preferred_election_error_cases() {
    // Replicas are always [1, 2, 3]; the preferred leader is replica 1.
    // (current_leader, isr, alive, expected)
    let cases: [(u64, &[u64], &[u64], ElectError); 3] = [
        // Preferred replica 1 is already the leader.
        (
            1,
            &[1, 2, 3],
            &[1, 2, 3],
            ElectError::PreferredAlreadyLeader,
        ),
        // Preferred replica 1 is not in the ISR.
        (2, &[2, 3], &[1, 2, 3], ElectError::PreferredNotInIsr),
        // Preferred replica 1 is in the ISR but dead.
        (2, &[1, 2, 3], &[2, 3], ElectError::PreferredNotAlive),
    ];
    for (leader, isr, alive, expected) in cases {
        let img = img_with_partition(ElectionSetup {
            topic: "foo",
            leader: krabka_raft::NodeId(leader),
            isr: &crate::test_support::replica_nodes(isr),
            ..Default::default()
        });
        let l = alive_set(alive);
        let err = select_new_leader_for_partition(
            &img,
            &l,
            &no_witnesses(),
            "foo",
            0,
            ElectionType::Preferred,
        )
        .unwrap_err();
        assert!(
            err == expected,
            "leader {leader}, isr {isr:?}, alive {alive:?}"
        );
    }
}

#[tokio::test]
async fn unclean_happy_path() {
    // ISR is just {1}, broker 1 is dead, brokers 2/3 are alive.
    let img = img_with_partition(ElectionSetup {
        topic: "foo",
        isr: &[krabka_raft::NodeId(1)],
        ..Default::default()
    });
    let l = alive_set(&[2, 3]);
    let new_pr =
        select_new_leader_for_partition(&img, &l, &no_witnesses(), "foo", 0, ElectionType::Unclean)
            .expect("unclean should elect");
    let expected =
        crate::leader_election::test_support::expected_partition(ExpectedPartitionSetup {
            topic: "foo",
            isr: &[krabka_raft::NodeId(2)],
            ..Default::default()
        });
    assert!(new_pr == expected);
}

#[tokio::test]
async fn unclean_no_alive_replicas() {
    let img = img_with_partition(ElectionSetup {
        topic: "foo",
        isr: &[krabka_raft::NodeId(1)],
        ..Default::default()
    });
    let l = alive_set(&[]); // everyone dead
    let err =
        select_new_leader_for_partition(&img, &l, &no_witnesses(), "foo", 0, ElectionType::Unclean)
            .unwrap_err();
    assert!(err == ElectError::NoEligibleReplica);
}

#[tokio::test]
async fn unclean_isr_member_alive_returns_election_not_needed() {
    let img = img_with_partition(ElectionSetup {
        topic: "foo",
        isr: &[krabka_raft::NodeId(1), krabka_raft::NodeId(2)],
        ..Default::default()
    });
    let l = alive_set(&[1, 2]); // ISR has live member
    let err =
        select_new_leader_for_partition(&img, &l, &no_witnesses(), "foo", 0, ElectionType::Unclean)
            .unwrap_err();
    assert!(err == ElectError::ElectionNotNeeded);
}

#[tokio::test]
async fn unknown_topic_returns_error() {
    let img = MetadataImage::new(Uuid::nil());
    let l = alive_set(&[]);
    let err = select_new_leader_for_partition(
        &img,
        &l,
        &no_witnesses(),
        "ghost",
        0,
        ElectionType::Preferred,
    )
    .unwrap_err();
    assert!(err == ElectError::UnknownTopicOrPartition);
}

#[tokio::test]
async fn preferred_election_refuses_a_witness_preferred_replica() {
    // Site-aware placement put the witness first in `replicas`, so the
    // preferred replica can never lead.
    let img = img_with_partition(ElectionSetup {
        topic: "foo",
        leader: krabka_raft::NodeId(2),
        ..Default::default()
    });
    let l = alive_set(&[1, 2, 3]);
    let err = select_new_leader_for_partition(
        &img,
        &l,
        &witnesses(&[1]),
        "foo",
        0,
        ElectionType::Preferred,
    )
    .unwrap_err();
    assert!(err == ElectError::PreferredIsWitness);
}

#[tokio::test]
async fn operator_unclean_election_skips_a_witness_replica() {
    // Every data replica in the ISR is dead and the operator forces an
    // unclean election. The alive witness 2 must not take leadership, and
    // it must not report the election as unneeded either.
    let img = img_with_partition(ElectionSetup {
        topic: "foo",
        isr: &[krabka_raft::NodeId(1), krabka_raft::NodeId(2)],
        ..Default::default()
    });
    let l = alive_set(&[2, 3]);
    let new_pr = select_new_leader_for_partition(
        &img,
        &l,
        &witnesses(&[2]),
        "foo",
        0,
        ElectionType::Unclean,
    )
    .expect("unclean should elect the data replica");
    let expected =
        crate::leader_election::test_support::expected_partition(ExpectedPartitionSetup {
            topic: "foo",
            leader: krabka_raft::NodeId(3),
            isr: &[krabka_raft::NodeId(3)],
            ..Default::default()
        });
    assert!(new_pr == expected);
}

/// Kafka's `LeaderAcceptor` asks `hasOnlineDir` beside `isActive`, so a
/// replica on a dead disk neither leads nor keeps the partition led. Replicas
/// are `[1, 2, 3]` on directories `d1, d2, d3`; every broker is alive.
#[tokio::test]
async fn elections_skip_a_replica_on_a_dead_log_dir() {
    struct Case {
        name: &'static str,
        election: ElectionType,
        leader: u64,
        isr: &'static [u64],
        /// The brokers whose directory of this partition is offline.
        dead_dirs: &'static [u64],
        /// The new leader and ISR.
        expected: Result<(u64, Vec<u64>), ElectError>,
    }
    let case = |name, election, leader, isr, dead_dirs, expected| Case {
        name,
        election,
        leader,
        isr,
        dead_dirs,
        expected,
    };
    let cases = [
        case(
            "unclean: the only in-sync replica is on a dead disk",
            ElectionType::Unclean,
            1,
            &[1],
            &[1],
            Ok((2, vec![2])),
        ),
        case(
            "unclean: nothing is on a dead disk",
            ElectionType::Unclean,
            1,
            &[1],
            &[],
            Err(ElectError::ElectionNotNeeded),
        ),
        case(
            "unclean: another in-sync replica still has its disk",
            ElectionType::Unclean,
            1,
            &[1, 2],
            &[1],
            Err(ElectError::ElectionNotNeeded),
        ),
        case(
            "unclean: every replica is on a dead disk",
            ElectionType::Unclean,
            1,
            &[1],
            &[1, 2, 3],
            Err(ElectError::NoEligibleReplica),
        ),
        case(
            "preferred: the leader is the preferred replica on a dead disk",
            ElectionType::Preferred,
            1,
            &[1, 2],
            &[1],
            Err(ElectError::PreferredNotAlive),
        ),
        case(
            "preferred: the preferred replica is on a dead disk",
            ElectionType::Preferred,
            2,
            &[1, 2],
            &[1],
            Err(ElectError::PreferredNotAlive),
        ),
        case(
            "preferred: the preferred replica has its disk",
            ElectionType::Preferred,
            2,
            &[1, 2],
            &[3],
            Ok((1, vec![1, 2])),
        ),
    ];
    for Case {
        name,
        election,
        leader,
        isr,
        dead_dirs,
        expected,
    } in cases
    {
        let dirs: Vec<Uuid> = (1..=3).map(Uuid::from_u128).collect();
        let mut img = img_with_partition(ElectionSetup {
            topic: "foo",
            leader: krabka_raft::NodeId(leader),
            isr: &crate::test_support::replica_nodes(isr),
            dirs: &dirs,
            ..Default::default()
        });
        for (id, dir) in (1..=3).zip(&dirs) {
            let online = if dead_dirs.contains(&id) {
                Uuid::from_u128(99)
            } else {
                *dir
            };
            register_broker_with_dirs(&mut img, id, vec![online]);
        }
        let alive = alive_set(&[1, 2, 3]);

        let got =
            select_new_leader_for_partition(&img, &alive, &no_witnesses(), "foo", 0, election)
                .map(|pr| (pr.leader.0, pr.isr.iter().map(|n| n.0).collect::<Vec<_>>()));

        assert!(got == expected, "{name}");
    }
}
