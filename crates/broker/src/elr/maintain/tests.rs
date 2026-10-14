//! The KIP-966 state machine: what one partition's ELR becomes when a change
//! applies, and the `V1PartitionElr` records the publisher appends for it.

use {
    super::{ElrPublisher, leaderless_partition_elr, next_partition_elr},
    crate::{
        config_keys::{MIN_INSYNC_REPLICAS, RETENTION_MS},
        elr::state::{PartitionElr, TopicElr},
        test_support::finalize_elr_version,
    },
    assert2::assert,
    krabka_metadata::{
        LeaderEpoch, MetadataImage, MetadataRecord, NodeId, PartitionElrRecord, PartitionRecord,
        PartitionUpdateRecord, TopicConfigRecord, TopicRecord,
    },
};

const TOPIC: &str = "orders";

fn nodes(ids: &[u64]) -> Vec<NodeId> { ids.iter().copied().map(NodeId).collect() }

fn partition(leader: u64, replicas: &[u64], isr: &[u64]) -> PartitionRecord {
    PartitionRecord {
        topic: TOPIC.into(),
        partition: 0,
        leader: NodeId(leader),
        replicas: nodes(replicas),
        isr: nodes(isr),
        leader_epoch: LeaderEpoch(7),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 4,
    }
}

/// The ordinary three-replica fixture led by node 1, with an explicit ISR.
fn three_replica_partition(isr: &[u64]) -> PartitionRecord { partition(1, &[1, 2, 3], isr) }

/// `record` under leader epoch `epoch`: the change that elects, as the
/// scans bump the epoch on every election.
fn at_epoch(record: PartitionRecord, epoch: i32) -> PartitionRecord {
    PartitionRecord {
        leader_epoch: LeaderEpoch(epoch),
        ..record
    }
}

use crate::test_support::partition_elr as elr;

fn check_transition(
    image: &MetadataImage,
    before: &PartitionRecord,
    after: &PartitionRecord,
    published: &PartitionElr,
    expected: &PartitionElr,
    label: &str,
) {
    let got = next_partition_elr(
        image,
        Some(before),
        after,
        published,
        &std::collections::BTreeSet::new(),
    );
    assert!(&got == expected, "{label}");
}

fn update(partition: PartitionRecord, eligible: &[u64], last_known: &[u64]) -> MetadataRecord {
    MetadataRecord::V1PartitionUpdate(PartitionUpdateRecord {
        partition,
        eligible_leader_replicas: Some(nodes(eligible)),
        last_known_elr: Some(nodes(last_known)),
        recovery_state: None,
    })
}

/// An image holding topic `orders` with the given overrides and the given
/// partition state. `min_isr` is published as an ordinary topic override, the
/// way `kafka-configs --alter` sets it.
fn image(
    min_isr: Option<&str>,
    published: Option<&str>,
    current: &PartitionRecord,
) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    finalize_elr_version(&mut image);
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: TOPIC.into(),
        topic_id: uuid::Uuid::from_u128(1),
        partitions: 1,
        replication_factor: i16::try_from(current.replicas.len()).expect("rf fits i16"),
    }));
    image.apply(&MetadataRecord::V1Partition(current.clone()));
    let overrides: std::collections::BTreeMap<String, String> =
        [min_isr.map(|value| (MIN_INSYNC_REPLICAS.to_string(), value.to_string()))]
            .into_iter()
            .flatten()
            .collect();
    if !overrides.is_empty() {
        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: TOPIC.into(),
            overrides,
        }));
    }
    if let Some(value) = published {
        let state = TopicElr::parse(value).partition(current.partition);
        image.apply(&MetadataRecord::V1PartitionElr(PartitionElrRecord {
            topic: TOPIC.into(),
            partition: current.partition,
            eligible_leader_replicas: nodes(
                &state
                    .eligible_leader_replicas
                    .iter()
                    .map(|id| u64::try_from(*id).unwrap())
                    .collect::<Vec<_>>(),
            ),
            last_known_elr: nodes(
                &state
                    .last_known_elr
                    .iter()
                    .map(|id| u64::try_from(*id).unwrap())
                    .collect::<Vec<_>>(),
            ),
        }));
    }
    image
}

/// The rules of `PartitionChangeBuilder.maybePopulateTargetElr`, one row per
/// transition, each starting from the published state the row names.
#[yare::parameterized(
    at_min_isr = {
        "an ISR at min ISR keeps the ELR empty",
        Some("2"),
        elr(&[], &[]),
        three_replica_partition(&[1, 2, 3]),
        three_replica_partition(&[1, 2]),
        elr(&[], &[]),
    },
    shrink_below_min = {
        "a shrink below min ISR makes the replicas it dropped eligible",
        Some("2"),
        elr(&[], &[]),
        three_replica_partition(&[1, 2, 3]),
        three_replica_partition(&[1]),
        elr(&[2, 3], &[]),
    },
    further_shrink = {
        "a further shrink adds to the ELR rather than replacing it",
        Some("3"),
        elr(&[3], &[]),
        three_replica_partition(&[1, 2]),
        three_replica_partition(&[1]),
        elr(&[2, 3], &[]),
    },
    replica_rejoins = {
        "a replica that rejoins the ISR leaves the ELR",
        Some("3"),
        elr(&[2, 3], &[]),
        three_replica_partition(&[1]),
        three_replica_partition(&[1, 2]),
        elr(&[3], &[]),
    },
    expand_to_min = {
        "an expand to min ISR clears both sets",
        Some("2"),
        elr(&[2, 3], &[]),
        three_replica_partition(&[1]),
        three_replica_partition(&[1, 2]),
        elr(&[], &[]),
    },
    removed_replica = {
        "an ELR replica dropped from the replica set leaves the ELR",
        Some("3"),
        elr(&[2, 3], &[]),
        three_replica_partition(&[1]),
        partition(1, &[1, 2], &[1]),
        elr(&[2], &[]),
    },
    leader_clears_last_known = {
        "a change that leaves a leader clears the last-known ELR",
        Some("3"),
        elr(&[2], &[3]),
        partition(1, &[1, 2], &[1]),
        partition(1, &[1, 2], &[1]),
        elr(&[2], &[]),
    },
    unclean_election = {
        "an unclean election clears both sets",
        Some("3"),
        elr(&[2], &[3]),
        partition(1, &[1, 2, 4], &[1]),
        partition(4, &[1, 2, 4], &[4]),
        elr(&[], &[]),
    },
    clean_elr_election = {
        "electing an ELR replica is clean, so the rest stays eligible",
        Some("3"),
        elr(&[2, 3], &[]),
        three_replica_partition(&[1]),
        partition(2, &[1, 2, 3], &[2]),
        elr(&[1, 3], &[]),
    },
    default_min_isr = {
        "Kafka's default min ISR of 1 can never leave a replica eligible",
        None,
        elr(&[], &[]),
        three_replica_partition(&[1, 2, 3]),
        three_replica_partition(&[1]),
        elr(&[], &[]),
    },
    min_isr_above_rf = {
        "a min ISR above the replication factor is capped by it",
        Some("5"),
        elr(&[], &[]),
        three_replica_partition(&[1, 2, 3]),
        three_replica_partition(&[1, 2, 3]),
        elr(&[], &[]),
    },
)]
fn the_elr_follows_the_isr_across_min_insync_replicas(
    label: &str,
    min_isr: Option<&str>,
    published: PartitionElr,
    before: PartitionRecord,
    after: PartitionRecord,
    want: PartitionElr,
) {
    let image = image(min_isr, None, &before);
    check_transition(&image, &before, &after, &published, &want, label);
}

/// A partition the batch creates has no history to remember, so it starts
/// with no ELR whatever its ISR looks like.
#[test]
fn a_new_partition_starts_with_no_elr() {
    let created = three_replica_partition(&[1]);
    let image = image(Some("3"), None, &created);

    let got = next_partition_elr(
        &image,
        None,
        &created,
        &elr(&[], &[]),
        &std::collections::BTreeSet::new(),
    );

    assert!(got == elr(&[], &[]));
}

/// The rows of `PartitionChangeBuilderTest` at Kafka 4.3.1 that describe a
/// partition which keeps a leader, with `useLastKnownLeaderInBalancedRecovery`
/// on, which is how the controller runs it (nothing in
/// `ReplicationControlManager` turns it off). Min ISR is 3 in every test
/// there, and replicas are `[1, 2, 3, 4]`.
///
/// With the flag on, `maybeUpdateRecordElr` never writes the multi-member set
/// `maybePopulateTargetElr` computes, so the last-known ELR of a partition
/// with a leader is empty in every row.
#[yare::parameterized(
    isr_shrink = {
        "IsrShrinkBelowMinISR: the dropped replicas are eligible, the last-known set stays empty",
        elr(&[], &[]),
        partition(1, &[1, 2, 3, 4], &[1, 2, 3, 4]),
        partition(1, &[1, 2, 3, 4], &[1, 2]),
        vec![],
        elr(&[3, 4], &[]),
    },
    isr_expand = {
        "IsrExpandAboveMinISR: both sets end empty",
        elr(&[3], &[4]),
        partition(1, &[1, 2, 3, 4], &[1, 2]),
        partition(1, &[1, 2, 3, 4], &[1, 2, 3]),
        vec![],
        elr(&[], &[]),
    },
    elr_elected = {
        "ElrCanBeElected: the replica that lost the leadership is eligible, the last-known set stays empty",
        elr(&[3], &[]),
        partition(1, &[1, 2, 3, 4], &[1]),
        at_epoch(partition(3, &[1, 2, 3, 4], &[3]), 8),
        vec![],
        elr(&[1], &[]),
    },
    remove_unclean = {
        "RemoveUncleanShutdownReplicasFromElr: the replica leaves the ELR and lands nowhere",
        elr(&[2, 3], &[]),
        partition(1, &[1, 2, 3, 4], &[1]),
        partition(1, &[1, 2, 3, 4], &[1]),
        vec![3],
        elr(&[2], &[]),
    },
    short_isr_add = {
        "IsrAddNewMemberNotInELR: an ISR that stays short changes nothing",
        elr(&[3], &[]),
        partition(1, &[1, 2, 3, 4], &[1]),
        partition(1, &[1, 2, 3, 4], &[1, 4]),
        vec![],
        elr(&[3], &[]),
    },
)]
fn the_rows_of_partition_change_builder_test_for_a_partition_with_a_leader(
    label: &str,
    published: PartitionElr,
    before: PartitionRecord,
    after: PartitionRecord,
    unclean_shutdown: Vec<i32>,
    want: PartitionElr,
) {
    let image = image(Some("3"), None, &before);
    let got = next_partition_elr(
        &image,
        Some(&before),
        &after,
        &published,
        &unclean_shutdown.into_iter().collect(),
    );
    assert2::assert!(got == want, "{label}");
}

/// The rows of `PartitionChangeBuilderTest` at 4.3.1 that leave a partition
/// without a leader: `maybeUpdateLastKnownLeader` writes `[previous leader]`
/// the first time, and `maybeUpdateRecordElr` keeps it while the partition
/// stays that way.
///
/// The partition record is the one krabka keeps for it, which still names the
/// last leader; `isr` is the ISR the change installs, which is Kafka's.
#[yare::parameterized(
    all_offline = {
        "IsrCanShrinkToZero: every replica is offline",
        Some("3"),
        elr(&[], &[]),
        partition(1, &[1, 2, 3, 4], &[1, 2, 3, 4]),
        vec![],
        vec![],
        elr(&[1, 2, 3, 4], &[1]),
    },
    unclean_after = {
        "IsrCanShrinkToZero: an unclean shutdown afterwards leaves the last leader alone",
        Some("3"),
        elr(&[1, 2, 3, 4], &[1]),
        partition(1, &[1, 2, 3, 4], &[1]),
        vec![],
        vec![2],
        elr(&[1, 3, 4], &[1]),
    },
    nobody_acceptable = {
        "lastKnownElrShouldBePopulatedWhenNoLeader: nobody acceptable, the ISR unchanged",
        Some("3"),
        elr(&[2], &[]),
        three_replica_partition(&[1]),
        vec![1],
        vec![],
        elr(&[2], &[1]),
    },
    remains_leaderless = {
        "a partition that stays without a leader keeps the value it has",
        Some("3"),
        elr(&[1, 2], &[1]),
        three_replica_partition(&[1]),
        vec![],
        vec![],
        elr(&[1, 2], &[1]),
    },
    default_min_isr = {
        "Kafka's default min ISR of 1 still records the last leader as eligible",
        None,
        elr(&[], &[]),
        three_replica_partition(&[1]),
        vec![],
        vec![],
        elr(&[1], &[1]),
    },
    unclean_last_leader = {
        "the last leader that shut down uncleanly is not eligible, but is still the last leader",
        Some("2"),
        elr(&[], &[]),
        three_replica_partition(&[1]),
        vec![],
        vec![1],
        elr(&[], &[1]),
    },
    min_isr_satisfied = {
        "an ISR that still meets min ISR is enough on its own, and the last leader stays",
        Some("1"),
        elr(&[2], &[]),
        partition(1, &[1, 2], &[1, 2]),
        vec![2],
        vec![],
        elr(&[], &[1]),
    },
)]
fn the_rows_of_partition_change_builder_test_for_a_partition_without_a_leader(
    label: &str,
    min_isr: Option<&str>,
    published: PartitionElr,
    before: PartitionRecord,
    isr: Vec<u64>,
    unclean_shutdown: Vec<i32>,
    want: PartitionElr,
) {
    let image = image(min_isr, None, &before);
    let got = leaderless_partition_elr(
        &image,
        &before,
        &nodes(&isr),
        &published,
        &unclean_shutdown.into_iter().collect(),
    );
    assert2::assert!(got == want, "{label}");
}

/// A partition the image already records as leaderless -- a one-member
/// last-known ELR naming its leader -- and the changes that reach it.
///
/// A change that keeps the leader under the same leader epoch elects nobody,
/// so the marker stays. One that gives it a leader clears the marker, which is
/// `maybeUpdateLastKnownLeader`'s `record.leader() >= 0` branch, and what the
/// eligible set becomes reads the ISR the way Kafka holds it, without the last
/// leader.
#[yare::parameterized(
    marker_preserved = {
        "an ISR shrink for another replica keeps the marker",
        elr(&[], &[1]),
        partition(1, &[1, 2, 3], &[1, 2]),
        partition(1, &[1, 2, 3], &[1]),
        elr(&[2], &[1]),
    },
    last_leader_eligible = {
        "an ISR shrink for another replica keeps the last leader eligible",
        elr(&[1], &[1]),
        partition(1, &[1, 2, 3], &[1, 2]),
        partition(1, &[1, 2, 3], &[1]),
        elr(&[1, 2], &[1]),
    },
    another_elr_replica_elected = {
        "electing another replica from the ELR clears it and keeps the rest eligible",
        elr(&[1, 2, 3], &[1]),
        partition(1, &[1, 2, 3], &[1]),
        at_epoch(partition(2, &[1, 2, 3], &[2]), 8),
        elr(&[1, 3], &[]),
    },
    last_leader_cleanly_elected = {
        "electing the last leader from the ELR is clean",
        elr(&[1, 2, 3], &[1]),
        partition(1, &[1, 2, 3], &[1]),
        at_epoch(partition(1, &[1, 2, 3], &[1]), 8),
        elr(&[2, 3], &[]),
    },
    last_leader_uncleanly_elected = {
        "electing the last known leader is unclean, so both sets clear",
        elr(&[], &[1]),
        partition(1, &[1, 2, 3], &[1]),
        at_epoch(partition(1, &[1, 2, 3], &[1]), 8),
        elr(&[], &[]),
    },
    leader_clears_marker = {
        "a partition with a leader publishes an empty last-known ELR",
        elr(&[2], &[]),
        partition(1, &[1, 2, 3], &[1, 2]),
        partition(1, &[1, 2, 3], &[1]),
        elr(&[2], &[]),
    },
)]
fn a_change_that_gives_a_leaderless_partition_a_leader_clears_the_last_known_elr(
    label: &str,
    published: PartitionElr,
    before: PartitionRecord,
    after: PartitionRecord,
    want: PartitionElr,
) {
    let image = image(Some("3"), None, &before);
    check_transition(&image, &before, &after, &published, &want, label);
}

/// Kafka's `uncleanShutdownReplicas`, which the batch that reacts to a
/// returning broker names it with.
///
/// The ISR removal and the recompute are the same batch, so without the
/// exclusion the recompute reads the broker straight back out of the ISR the
/// removal is leaving -- `old_isr ∪ eligible_before` -- and publishes it as
/// eligible. The two rows are the same change; only the exclusion differs,
/// and the second is the one the withdrawal survives.
///
/// The excluded id does not land in the last-known set: in Kafka 4.3.1 that
/// set holds the last leader of a leaderless partition, and this partition has
/// a leader. `PartitionChangeBuilder.maybePopulateTargetElr` subtracts
/// `uncleanShutdownReplicas` from `targetElr` and from nothing else, so the
/// second row publishes no record at all: both sets are what they were.
#[test]
fn an_unclean_shutdown_replica_is_not_re_derived_from_the_isr_it_is_leaving() {
    let before = three_replica_partition(&[1, 2, 3]);
    let image = image(Some("3"), None, &before);
    let shrink = MetadataRecord::V1Partition(three_replica_partition(&[1, 2]));

    let mut plain = vec![shrink.clone()];
    ElrPublisher::new(&image).extend(&mut plain);
    assert!(plain == vec![update(three_replica_partition(&[1, 2]), &[3], &[])]);

    let mut excluded = vec![shrink.clone()];
    ElrPublisher::after_unclean_shutdown(&image, NodeId(3)).extend(&mut excluded);
    assert!(excluded == vec![shrink]);
}

/// The election that gives a leaderless partition back the leader its record
/// names keeps the same leader under a higher leader epoch, which a
/// `V1PartitionUpdate` cannot carry: it reaches the log as a
/// `PartitionChangeRecord` that bumps the epoch only when the leader changes.
/// So the partition record stays whole and the ELR and recovery records follow
/// it, where an election of another replica folds all three into one update.
#[test]
fn an_election_of_the_named_leader_keeps_its_epoch_bump_out_of_a_partition_update() {
    let before = three_replica_partition(&[1]);
    let image = image(Some("2"), Some("0::1"), &before);
    let recovering =
        MetadataRecord::V1PartitionRecovery(krabka_metadata::PartitionRecoveryRecord {
            topic: TOPIC.into(),
            partition: 0,
            state: krabka_metadata::LeaderRecoveryState::Recovering,
        });

    let same_leader = at_epoch(three_replica_partition(&[1]), 8);
    let mut whole = vec![
        MetadataRecord::V1Partition(same_leader.clone()),
        recovering.clone(),
    ];
    ElrPublisher::new(&image).extend(&mut whole);
    assert!(
        whole
            == vec![
                MetadataRecord::V1Partition(same_leader),
                MetadataRecord::V1PartitionElr(PartitionElrRecord {
                    topic: TOPIC.into(),
                    partition: 0,
                    eligible_leader_replicas: vec![],
                    last_known_elr: vec![],
                }),
                recovering.clone(),
            ]
    );

    let other_leader = at_epoch(partition(2, &[1, 2, 3], &[2]), 8);
    let mut folded = vec![
        MetadataRecord::V1Partition(other_leader.clone()),
        recovering,
    ];
    ElrPublisher::new(&image).extend(&mut folded);
    assert!(
        folded
            == vec![MetadataRecord::V1PartitionUpdate(PartitionUpdateRecord {
                partition: other_leader,
                eligible_leader_replicas: Some(vec![]),
                last_known_elr: Some(vec![]),
                recovery_state: Some(krabka_metadata::LeaderRecoveryState::Recovering),
            })]
    );
}

/// A batch that leaves a partition without a leader carries no partition
/// record for it, because a krabka partition record always names a leader, so
/// the publisher takes the partition from [`ElrPublisher::leaderless`] and
/// writes the ELR and the last leader.
///
/// This is the state Kafka writes for `leader = -1`, and it is what
/// `is_leaderless` reads back: the last-known ELR names the recorded leader.
/// Repeating the batch changes nothing, which is what lets the failover sweep
/// re-drive a partition every tick.
#[test]
fn a_leaderless_partition_publishes_its_elr_and_its_last_leader_once() {
    let before = three_replica_partition(&[1]);
    let mut image = image(Some("2"), Some("0:2,3:"), &before);

    let mut first = Vec::new();
    let mut publisher = ElrPublisher::new(&image);
    publisher.leaderless(&before, vec![]);
    publisher.extend(&mut first);
    assert!(
        first
            == vec![MetadataRecord::V1PartitionElr(PartitionElrRecord {
                topic: TOPIC.into(),
                partition: 0,
                eligible_leader_replicas: nodes(&[1, 2, 3]),
                last_known_elr: nodes(&[1]),
            })]
    );

    for record in &first {
        image.apply(record);
    }
    let mut second = Vec::new();
    let mut publisher = ElrPublisher::new(&image);
    publisher.leaderless(&before, vec![]);
    publisher.extend(&mut second);
    assert!(second.is_empty());
    assert!(crate::elr::state::is_leaderless(
        &image,
        image.partition(TOPIC, 0).expect("the partition")
    ));
}

/// Kafka builds the recompute of an unclean shutdown with the broker in
/// `uncleanShutdownReplicas`, so the last leader that restarted uncleanly is
/// not eligible: the ELR is empty and the last-known ELR names it, which is
/// the state `canElectLastKnownLeader` acts on.
#[test]
fn an_unclean_restart_of_the_last_leader_leaves_an_empty_elr_and_the_last_leader() {
    let before = three_replica_partition(&[1]);
    let image = image(Some("2"), None, &before);

    let mut changes = Vec::new();
    let mut publisher = ElrPublisher::after_unclean_shutdown(&image, NodeId(1));
    publisher.leaderless(&before, vec![]);
    publisher.extend(&mut changes);

    assert!(
        changes
            == vec![MetadataRecord::V1PartitionElr(PartitionElrRecord {
                topic: TOPIC.into(),
                partition: 0,
                eligible_leader_replicas: vec![],
                last_known_elr: nodes(&[1]),
            })]
    );
}

/// A full ISR shrinking to one member under the two-replica minimum.
fn shrunken_isr_changes() -> ShrunkenIsr {
    let before = three_replica_partition(&[1, 2, 3]);
    let image = image(Some("2"), None, &before);
    let changes = vec![MetadataRecord::V1Partition(three_replica_partition(&[1]))];
    ShrunkenIsr { image, changes }
}

/// The image before the shrink, and the changes that shrink the ISR.
struct ShrunkenIsr {
    image: MetadataImage,
    changes: Vec<MetadataRecord>,
}

/// The published record replaces a topic's whole override map, so it has to
/// carry the topic's other overrides forward alongside the ELR value.
#[test]
fn the_published_record_keeps_the_topics_other_overrides() {
    let ShrunkenIsr { image, mut changes } = shrunken_isr_changes();

    ElrPublisher::new(&image).extend(&mut changes);

    assert!(changes == vec![update(three_replica_partition(&[1]), &[2, 3], &[])]);
}

#[test]
fn a_partition_change_migrates_legacy_elr_without_losing_it() {
    let before = three_replica_partition(&[1]);
    let mut image = image(Some("3"), None, &before);
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: TOPIC.into(),
        overrides: [
            (MIN_INSYNC_REPLICAS.to_string(), "3".to_string()),
            ("krabka.elr".to_string(), "0:2,3:".to_string()),
        ]
        .into_iter()
        .collect(),
    }));
    let mut changes = vec![MetadataRecord::V1Partition(before)];

    ElrPublisher::new(&image).extend(&mut changes);
    for record in changes {
        image.apply(&record);
    }

    assert!(TopicElr::of_topic(&image, TOPIC).partition(0) == elr(&[2, 3], &[]));
    assert!(
        !image
            .topic_config(TOPIC)
            .unwrap()
            .contains_key("krabka.elr")
    );
}

/// A topic that recovers drops the key rather than publishing an entry that
/// says "no ELR", so `DescribeConfigs` stops reporting it at all.
#[test]
fn a_recovered_topic_drops_the_key_and_keeps_the_rest() {
    let before = three_replica_partition(&[1]);
    let image = image(Some("2"), Some("0:2,3:"), &before);
    let mut changes = vec![MetadataRecord::V1Partition(partition(1, &[1, 2, 3], &[
        1, 2,
    ]))];

    ElrPublisher::new(&image).extend(&mut changes);

    assert!(changes == vec![update(three_replica_partition(&[1, 2]), &[], &[])]);
}

/// Nothing is appended when the state does not move. This is what keeps the
/// publisher off every ISR change in a cluster that never set
/// `min.insync.replicas`, and what makes a re-published batch idempotent.
#[test]
fn an_unchanged_state_publishes_nothing() {
    for (label, min_isr, published, before, after) in [
        (
            "a healthy topic that stays healthy",
            Some("2"),
            None,
            three_replica_partition(&[1, 2, 3]),
            three_replica_partition(&[1, 2]),
        ),
        (
            "a topic whose ELR is recomputed to what it already holds",
            Some("3"),
            Some("0:2,3:"),
            three_replica_partition(&[1]),
            three_replica_partition(&[1]),
        ),
        (
            "a topic with no min ISR override at all",
            None,
            None,
            three_replica_partition(&[1, 2, 3]),
            three_replica_partition(&[1]),
        ),
    ] {
        let image = image(min_isr, published, &before);
        let mut changes = vec![MetadataRecord::V1Partition(after)];
        ElrPublisher::new(&image).extend(&mut changes);
        assert!(changes.len() == 1, "{label}");
    }
}

/// A `V1TopicConfig` already in the batch replaces the topic's whole map when
/// it applies, so the appended record has to be built on that map and not on
/// the one the image still holds. Here the batch drops the topic's retention
/// override; the ELR record must not put it back.
#[test]
fn the_appended_record_builds_on_a_topic_config_the_batch_already_carries() {
    let before = three_replica_partition(&[1, 2, 3]);
    let mut image = image(Some("2"), None, &before);
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: TOPIC.into(),
        overrides: [
            (MIN_INSYNC_REPLICAS.to_string(), "2".to_string()),
            (RETENTION_MS.to_string(), "60000".to_string()),
        ]
        .into_iter()
        .collect(),
    }));
    let mut changes = vec![
        MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: TOPIC.into(),
            overrides: [(MIN_INSYNC_REPLICAS.to_string(), "2".to_string())]
                .into_iter()
                .collect(),
        }),
        MetadataRecord::V1Partition(three_replica_partition(&[1])),
    ];

    ElrPublisher::new(&image).extend(&mut changes);

    assert!(changes[1..] == [update(three_replica_partition(&[1]), &[2, 3], &[])]);
}

/// A batch that deletes the topic gets no ELR record: the delete removes the
/// topic's config map, and a record after it would resurrect one.
#[test]
fn a_deleted_topic_publishes_nothing() {
    let ShrunkenIsr { image, mut changes } = shrunken_isr_changes();
    changes.push(MetadataRecord::V1DeleteTopic(
        krabka_metadata::DeleteTopicRecord { name: TOPIC.into() },
    ));

    ElrPublisher::new(&image).extend(&mut changes);

    assert!(changes.len() == 2);
}

/// Two partitions of one topic share a single published value, so a batch
/// that moves both appends one record carrying both entries.
#[test]
fn one_record_carries_every_partition_the_batch_moved() {
    let before = three_replica_partition(&[1, 2, 3]);
    let mut image = image(Some("2"), None, &before);
    let mut sibling = before.clone();
    sibling.partition = 1;
    image.apply(&MetadataRecord::V1Partition(sibling));

    let shrunk_zero = three_replica_partition(&[1]);
    let mut shrunk_one = partition(2, &[1, 2, 3], &[2]);
    shrunk_one.partition = 1;
    let mut changes = vec![
        MetadataRecord::V1Partition(shrunk_zero),
        MetadataRecord::V1Partition(shrunk_one.clone()),
    ];

    ElrPublisher::new(&image).extend(&mut changes);

    assert!(
        changes
            == [
                update(three_replica_partition(&[1]), &[2, 3], &[]),
                update(shrunk_one, &[1, 3], &[]),
            ]
    );
}

/// KIP-966 is gated on `eligible.leader.replicas.version`, the way Kafka's
/// `ReplicationControlManager` builds every `PartitionChangeBuilder` with
/// `setEligibleLeaderReplicasEnabled(isElrEnabled())`. At level 0 the
/// publisher appends nothing, so no partition ever gains an eligible or
/// last-known-eligible set; at level 1 it appends what the rules imply. The
/// same batch and the same image differ only by the finalized level.
#[test]
fn the_publisher_appends_nothing_below_feature_level_one() {
    let before = three_replica_partition(&[1, 2, 3]);
    let shrink = MetadataRecord::V1Partition(three_replica_partition(&[1]));

    for (case, enabled) in [
        ("the feature is off", false),
        ("the feature is finalized at 1", true),
    ] {
        // `image` finalizes the feature; the off case builds the same image
        // without that record.
        let mut img = if enabled {
            image(Some("2"), None, &before)
        } else {
            let mut plain = MetadataImage::new(uuid::Uuid::nil());
            plain.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: TOPIC.into(),
                topic_id: uuid::Uuid::from_u128(1),
                partitions: 1,
                replication_factor: 3,
            }));
            plain.apply(&MetadataRecord::V1Partition(before.clone()));
            plain.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: TOPIC.into(),
                overrides: [(MIN_INSYNC_REPLICAS.to_string(), "2".to_string())]
                    .into_iter()
                    .collect(),
            }));
            plain
        };
        // The publisher reads the image, so it must not be mutated further.
        let _ = &mut img;

        let mut changes = vec![shrink.clone()];
        ElrPublisher::new(&img).extend(&mut changes);

        if enabled {
            assert!(
                changes == vec![update(three_replica_partition(&[1]), &[2, 3], &[])],
                "{case}"
            );
        } else {
            assert!(changes == vec![shrink.clone()], "{case}");
        }
    }
}
