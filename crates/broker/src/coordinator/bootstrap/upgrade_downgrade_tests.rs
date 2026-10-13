//! Tests for the group kind a replay settles on when a group moved between
//! the classic and the KIP-848 next-gen protocols.
//!
//! An upgrade writes next-gen records over a classic group, and a downgrade
//! tombstones them and writes a fresh classic record. Log compaction then
//! leaves only the last value per key, so the tests pin both the compacted
//! residue that must replay as classic and the stray next-gen write that
//! resurrects the group as a consumer.

use {
    super::{
        replay::finalize,
        test_support::{
            bare_coordinator, classic_group_record, consumer_group_records, replay_classic_residue,
            replay_values,
        },
    },
    crate::coordinator::{
        persistence::GroupMetadataValue, unified::actor::test_support::rpc::describe,
    },
    assert2::assert,
};

/// PROBLEM A, the downgrade trap: a group that started classic, then was
/// UPGRADED to next-gen, then was DOWNGRADED back to classic must replay
/// as a CLASSIC group and not as an empty next-gen group.
///
/// The downgrade drops the k3 `GroupMetadata` with a tombstone. Replay
/// must remove the next-gen seed completely, so that the fresh k2 record
/// that comes later rebuilds the classic group. Log order wins.
#[tokio::test]
async fn downgraded_group_replays_as_classic() {
    use crate::coordinator::unified::persistence_next_gen as ng;

    let coord = bare_coordinator();

    let [(group_key, group_value), (member_key, member_value)] = consumer_group_records(1, None);

    // Record stream in log order.
    let (k2_key, k2_val) = classic_group_record("g", "m1");
    let (k2_key2, k2_val2) = classic_group_record("g", "m1");
    let stream: Vec<(bytes::Bytes, Option<bytes::Bytes>)> = vec![
        // 1. initial classic group
        (k2_key, Some(k2_val)),
        // 2. upgrade drops k2 (tombstone)
        (GroupMetadataValue::encode_key("g").unwrap(), None),
        // 3. upgrade: next-gen group metadata
        (group_key.clone(), Some(group_value)),
        // 4. upgrade: next-gen member metadata
        (member_key.clone(), Some(member_value)),
        // 5. the downgrade tombstones the consumer group in the order of
        // Kafka's `ConsumerGroup.createGroupTombstoneRecords`: the member's
        // current assignment (k8), its target (k7), the target metadata
        // (k6), the member (k5), the deprecated k4 record, and the group (k3)
        (
            ng::encode_key(&ng::NextGenKey::CurrentMemberAssignment {
                group_id: "g".into(),
                member_id: "m1".into(),
            })
            .unwrap(),
            None,
        ),
        (
            ng::encode_key(&ng::NextGenKey::TargetAssignmentMember {
                group_id: "g".into(),
                member_id: "m1".into(),
            })
            .unwrap(),
            None,
        ),
        (
            ng::encode_key(&ng::NextGenKey::TargetAssignmentMetadata {
                group_id: "g".into(),
            })
            .unwrap(),
            None,
        ),
        (member_key, None),
        (
            ng::encode_key(&ng::NextGenKey::PartitionMetadata {
                group_id: "g".into(),
            })
            .unwrap(),
            None,
        ),
        (group_key, None),
        // 6. downgrade writes a fresh k2 classic group
        (k2_key2, Some(k2_val2)),
    ];

    let acc = super::test_support::replay_stream(&coord, stream);
    finalize(&coord, acc).await;

    // The group must NOT be next-gen, and the classic describe path must
    // surface it with member "m1".
    super::test_support::assert_classic_replayed(&coord).await;
    // And there is no next-gen consumer actor for "g".
}

/// PROBLEM A under LOG COMPACTION, the resurrection trap.
///
/// Take a downgraded group whose batch tombstoned the k3 `GroupMetadata`
/// but NOT the group-level k6 `TargetAssignmentMetadata`. After compaction
/// collects the tombstoned k3, a k6 write survives. `__consumer_offsets`
/// is compacted by default, so replay then sees the post-compaction
/// residue: the surviving k6 write and the fresh classic k2, with NO k3
/// and NO k3 tombstone.
///
/// `replay_target_assignment_metadata` calls
/// `seeds.entry(..).or_default()`, so that lone k6 re-creates a next-gen
/// seed. `finalize` then classifies the group as next-gen and drops the
/// classic k2. The group comes back as an empty next-gen consumer.
///
/// The fix tombstones k6 in the downgrade batch, so compaction keeps the
/// k6 TOMBSTONE, the last value per key, and not a stale write. This test
/// pins the corrected post-compaction shape and asserts that the group
/// replays CLASSIC.
#[tokio::test]
async fn compacted_downgrade_residue_replays_as_classic() {
    // Post-compaction record stream. Compaction keeps only the LAST value
    // per key, and the k3 + its tombstone both GC away (both gone), leaving
    // the k6 TOMBSTONE the fix emits and the authoritative classic k2.
    // Replaying this surviving group-level tombstone must not create a seed.
    let (coord, acc) = replay_classic_residue(None);
    finalize(&coord, acc).await;

    // The group must replay CLASSIC, not resurrect as next-gen.
    super::test_support::assert_classic_replayed(&coord).await;
}

/// A child record without live k3 group metadata cannot claim the group.
///
/// The downgrade batch still tombstones k6 for compaction hygiene. Replay also
/// fails closed when a malformed or partially retained log leaves only k6.
#[tokio::test]
async fn surviving_k6_write_cannot_resurrect_next_gen_ownership() {
    use crate::coordinator::unified::persistence_next_gen as ng;

    // A surviving k6 WRITE is what compaction retains if the downgrade
    // omitted k6's tombstone. It precedes the authoritative classic k2.
    let (coord, acc) = replay_classic_residue(Some(
        ng::TargetAssignmentMetadataValue {
            assignment_epoch: 1,
            assignment_timestamp_ms: 0,
        }
        .encode(),
    ));

    assert!(!coord.seeds.contains_key("g"));

    finalize(&coord, acc).await;

    // The authoritative k2 snapshot therefore reconstructs a classic group.
    assert!(
        coord
            .find("g")
            .is_some_and(|h| h.kind == crate::coordinator::unified::actor::GroupKindTag::Classic)
    );
}

/// An upgrade-only replay, with k3 live and no tombstone after it, must
/// still give a CONSUMER, that is next-gen, group.
///
/// The test guards the PROBLEM A fix against an over-eager seed removal.
#[tokio::test]
async fn upgraded_group_without_tombstone_replays_as_consumer() {
    use crate::coordinator::unified::GroupType;

    let coord = bare_coordinator();
    let stream = consumer_group_records(1, None);
    let acc = replay_values(&coord, stream);
    finalize(&coord, acc).await;

    assert!(coord.group_type("g") != Some(GroupType::Classic));
    let handle = coord.find("g").expect("consumer actor present");
    assert!(handle.kind == crate::coordinator::unified::actor::GroupKindTag::Consumer);
}

/// PROBLEM B, the facade is not restored: a k5 `MemberMetadataValue` that
/// carries a `classic` block must rebuild the in-memory member's
/// `ClassicMemberFacade` on replay.
///
/// The replayed consumer group's member "m1" must report
/// `is_classic == true` in the next-gen `Describe` view.
#[tokio::test]
async fn member_with_classic_block_replays_facade() {
    use crate::coordinator::unified::{actor::GroupKindTag, persistence_next_gen};

    let coord = bare_coordinator();
    let stream = consumer_group_records(
        2,
        Some(persistence_next_gen::ClassicMemberMetadata {
            session_timeout_ms: 30_000,
            supported_protocols: vec![("range".into(), bytes::Bytes::from_static(b"meta"))],
        }),
    );
    let acc = replay_values(&coord, stream);
    finalize(&coord, acc).await;

    let handle = coord.find("g").expect("consumer actor present");
    assert!(handle.kind == GroupKindTag::Consumer);
    let view = describe(&handle).await;
    let m1 = view
        .members
        .iter()
        .find(|m| m.member_id == "m1")
        .expect("member m1 present");
    assert!(m1.is_classic, "facade reconstructed from k5 classic block");
}
