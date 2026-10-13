//! Unit tests for the KIP-848 downgrade trigger and the classic state it
//! restores.

use {
    crate::{
        codes,
        coordinator::unified::{
            actor::{
                GroupActorMessage, GroupKindTag,
                test_support::{
                    BidirectionalClassicSetup, ClassicMemberSetup, bidirectional_coordinator,
                    bidirectional_with_members, decode_assignment, join_native_consumer,
                    log_has_classic_group_metadata_write, make_coordinator_with_topic,
                    make_coordinator_with_topic_policy, rpc, seed_classic_member,
                    seeded_bidirectional_coordinator, spawn_and_downgrade,
                    upgrade_with_transient_native,
                },
            },
            classic_state::OffsetEntry,
        },
    },
    assert2::{assert, check},
    krabka_log::Offset,
};

/// KIP-848 DOWNGRADE trigger: a consumer group that hosts a classic member
/// must flip back to classic in place when the LAST native consumer member
/// leaves, under the default `Bidirectional` policy. The flip tombstones
/// the next-gen k3 `GroupMetadata`, writes a classic k2, and re-expresses
/// the hosted classic member as a classic member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn last_consumer_member_leaving_downgrades_to_classic() {
    // Default policy is Bidirectional → downgrade is allowed.
    let (coord, log) = make_coordinator_with_topic("t", 2);

    // Seed a classic group with one classic member subscribed to "t".
    let handle = seed_classic_member(&coord, ClassicMemberSetup::default());

    // The only native consumer joins and leaves, driving both migration flips.
    upgrade_with_transient_native(&handle, "t").await;

    // The group is now classic again. `describe_group` only returns
    // classic groups; it must surface "g" with the hosted classic member
    // re-expressed as a classic member.
    let snap = coord
        .describe_group("g")
        .await
        .expect("group downgraded to classic");
    // Exactly the hosted classic member remains (the departed native
    // member is gone), and the downgrade batch tombstoned the next-gen k3
    // GroupMetadata record AND the group-level k6 TargetAssignmentMetadata
    // record (which would otherwise survive log compaction and resurrect
    // the group as next-gen), and wrote a classic k2 GroupMetadata
    // (non-tombstone) for "g".
    check!(snap.members.len() == 1);
    check!(snap.members.iter().any(|m| m.member_id == "m-classic"));
    check!(log.has_next_gen_group_metadata_tombstone("g").await);
    check!(log.has_next_gen_target_metadata_tombstone("g").await);
    check!(log_has_classic_group_metadata_write(&log, "g").await);
}

/// Scenario 1: a full upgrade and downgrade round trip under
/// `Bidirectional`. A classic member "m1" joins. A native consumer "c1"
/// heartbeats, which upgrades the group. Then c1 leaves, which downgrades
/// it. The group must end CLASSIC with "m1" still present and still
/// assigned, with its partitions kept across both flips.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upgrade_then_downgrade_round_trip() {
    let (coord, _log, handle) =
        seeded_bidirectional_coordinator(BidirectionalClassicSetup::default());

    // A native consumer "c1" heartbeats → in-place UPGRADE; the group is now
    // consumer-kind and hosts both m1 (classic facade) and c1.
    let c1 = join_native_consumer(&handle).await.member_id;
    let describe = { rpc::describe(&handle).await };
    assert!(
        describe.members.len() == 2,
        "upgraded group hosts both m1 and c1"
    );

    // c1 leaves (member_epoch -1). It was the only native member → DOWNGRADE
    // back to classic.
    let leave = rpc::consumer_heartbeat(&handle, &c1, -1, None).await;
    assert!(leave.error_code == codes::NONE);

    // The group is classic again, with m1 restored and still assigned.
    let snap = coord
        .describe_group("g")
        .await
        .expect("group downgraded back to classic");
    assert!(
        snap.members.iter().any(|m| m.member_id == "m1"),
        "m1 must survive the upgrade→downgrade round trip"
    );
    let m1 = snap
        .members
        .iter()
        .find(|m| m.member_id == "m1")
        .expect("m1 present");
    // Decode the assignment blob and verify m1's partitions are preserved.
    // `!is_empty()` is insufficient — the blob always has a 2-byte version
    // prefix even if the partition list were empty.
    //
    // After upgrade, the assignor splits {0,1} between m1 and c1, and m1's
    // target is [1]. Kafka's fence downgrades without a new target: its
    // `ClassicGroup.fromConsumerGroup` serializes m1's consumer-group target,
    // and the classic group prepares a rebalance at once that gives m1 both
    // partitions when it rejoins.
    let assignment_bytes = bytes::Bytes::from(m1.assignment.clone());
    let decoded = decode_assignment(&assignment_bytes);
    let tp = decoded
        .assigned_partitions
        .iter()
        .find(|tp| tp.topic == "t")
        .expect("decoded assignment must contain topic t");
    let mut parts = tp.partitions.clone();
    parts.sort_unstable();
    assert!(
        parts == vec![1],
        "m1 keeps its consumer-group target across the downgrade; got {parts:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_leave_of_last_native_member_triggers_downgrade() {
    let (_coord, _log, handle, native) = bidirectional_with_members().await;

    let response = rpc::classic_leave(&handle, &native).await;
    check!(response.len() == 1);
    check!(response[0].error_code == codes::NONE);

    let view = rpc::classic_inspect(&handle).await;
    check!(view.members.len() == 1);
    check!(view.members[0].member_id.as_str() == "m-classic");
}

/// Scenario 2: KIP-345 static identity must survive both flips. A classic
/// member with `group.instance.id = "inst-a"` joins, then the group
/// upgrades, then it downgrades. The restored classic member must still
/// carry `group_instance_id == Some("inst-a")`. The test reads this from
/// the classic inspect view, because `MemberSnapshot` does not carry the
/// instance id but `ClassicMemberView` does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn static_member_identity_survives_both_flips() {
    let (_coord, _log, handle) = seeded_bidirectional_coordinator(BidirectionalClassicSetup {
        instance_id: Some("inst-a"),
        ..Default::default()
    });

    // Upgrade via a native consumer heartbeat, then downgrade by having that
    // native member leave.
    let native = join_native_consumer(&handle).await.member_id;
    let leave = rpc::consumer_heartbeat(&handle, &native, -1, None).await;
    assert!(leave.error_code == codes::NONE);

    // The group is classic again; the restored member must still carry the
    // static identity (convert_classic_to_consumer maps instance_id and
    // convert_consumer_to_classic restores it).
    let view = rpc::classic_inspect(&handle).await;
    let m1 = view
        .members
        .iter()
        .find(|m| m.member_id == "m1")
        .expect("m1 restored as a classic member");
    assert!(
        m1.group_instance_id.as_deref() == Some("inst-a"),
        "the static identity must survive both flips"
    );
}

/// Scenario 3: under the `Disabled` policy a classic group stays classic.
/// The broker REJECTS a native consumer heartbeat for that group instead
/// of upgrading it. This reproduces the hard classic and next-gen
/// separation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn policy_disabled_keeps_group_classic() {
    use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy;
    let (coord, _log) =
        make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Disabled);
    let handle = seed_classic_member(&coord, ClassicMemberSetup::dynamic("m1"));

    // A native consumer heartbeat must be rejected (no upgrade is allowed).
    let resp = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
    assert!(
        resp.error_code != codes::NONE,
        "Disabled policy must reject the upgrade heartbeat"
    );
    assert!(
        resp.error_code == codes::GROUP_ID_NOT_FOUND,
        "an un-upgradable classic group surfaces as GROUP_ID_NOT_FOUND"
    );

    // The group is untouched: still classic, still hosting m1.
    let view = rpc::classic_inspect(&handle).await;
    assert!(
        view.members.iter().any(|m| m.member_id == "m1"),
        "the group must remain classic with m1 intact"
    );
    assert!(handle.kind == GroupKindTag::Classic);
}

/// Scenario 4: committed offsets live on the kind-agnostic `Group`
/// container and must survive both flips unchanged. The test commits an
/// offset for ("t", 0) on a classic group, upgrades, asserts the offset is
/// still readable, downgrades, and asserts it is STILL there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_offsets_survive_a_flip() {
    let (_coord, _log, handle) =
        seeded_bidirectional_coordinator(BidirectionalClassicSetup::default());

    // Record a committed offset for ("t", 0) via the kind-agnostic path.
    let rx = rpc::begin(&handle, |tx| GroupActorMessage::UpdateCommitted {
        entries: vec![(("t".to_string(), 0), OffsetEntry {
            offset: Offset(99),
            leader_epoch: 3,
            metadata: String::new(),
            commit_timestamp_ms: 0,
            expire_timestamp_ms: None,
            topic_id: None,
        })],
        reply: tx,
    })
    .await;
    rx.await.unwrap();

    // Upgrade → the offset must still be readable.
    let native = join_native_consumer(&handle).await.member_id;
    let after_upgrade = rpc::fetch_committed(&handle).await;
    assert!(
        after_upgrade.get(&("t".to_string(), 0)).map(|e| e.offset) == Some(Offset(99)),
        "committed offset must survive the upgrade"
    );

    // Downgrade → the offset must STILL be there.
    let leave = rpc::consumer_heartbeat(&handle, &native, -1, None).await;
    assert!(leave.error_code == codes::NONE);
    let after_downgrade = rpc::fetch_committed(&handle).await;
    assert!(
        after_downgrade.get(&("t".to_string(), 0)).map(|e| e.offset) == Some(Offset(99)),
        "committed offset must survive the downgrade too"
    );
}

/// Regression (user-requested): a group that downgraded in place becomes
/// deletable. The test spawns a CONSUMER group, whose first RPC is a
/// `ConsumerGroupHeartbeat`, hosts a classic member, then downgrades when
/// the native consumer leaves.
///
/// The handle's spawn-time `kind` is a stale `Consumer`, but `delete_group`
/// dispatches on `ClassicDelete`'s live-kind reply. The downgraded group
/// answers as classic, so a non-empty group reports `NonEmpty` and NOT
/// `NotFound`. That proves delete sees it as classic. Before the refactor,
/// the stale `handle.kind == Consumer` gate short-circuited to
/// `NotFound`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn downgraded_group_is_deletable_once_empty() {
    let (coord, _log) = bidirectional_coordinator();

    let (handle, view) = spawn_and_downgrade(&coord).await;
    check!(view.members.iter().any(|m| m.member_id == "m-classic"));

    // The spawn-time kind is the stale `Consumer`; delete must not consult
    // it. A non-empty downgraded (live-classic) group reports `NonEmpty`,
    // NOT `NotFound` — proving delete sees it as classic.
    check!(handle.kind == GroupKindTag::Consumer);
    check!(
        coord.delete_group("g").await == Err(crate::coordinator::DeleteGroupError::NonEmpty),
        "a downgraded non-empty group must report NonEmpty (seen as classic), \
         not the stale-handle.kind NotFound"
    );

    // Drain the last hosted classic member so the group is empty, then it
    // must be deletable.
    let resp = rpc::classic_leave(&handle, "m-classic").await;
    assert!(!resp.is_empty());
    let view = rpc::classic_inspect(&handle).await;
    assert!(
        view.members.is_empty(),
        "the group must be empty after the classic member leaves"
    );
    assert!(
        coord.delete_group("g").await == Ok(()),
        "an empty downgraded group must be deletable"
    );
}
