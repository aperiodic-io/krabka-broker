//! Unit tests for the classic `LeaveGroup` and `DeleteGroups` paths.

use {
    super::*,
    crate::coordinator::unified::{
        ClientIdentity,
        actor::{
            GroupActorHandle, GroupActorMessage,
            member_state::build_member,
            test_support::{
                bidirectional_coordinator, bidirectional_with_members, last_classic_metadata,
                make_coordinator, rpc, seed_completing_classic, seed_stable_classic,
            },
        },
        classic_state::GroupState as ClassicGroupState,
    },
    assert2::check,
    krabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
    std::time::Instant,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_leave_last_member_persists_empty_generation() {
    let (coord, log) = make_coordinator();
    let seeded = seed_stable_classic(&coord, &["m1"]);
    let result = rpc::classic_leave_member(&seeded.handle, "m1").await;
    rpc::check_successful_classic_leave(&result);
    let view = rpc::classic_inspect(&seeded.handle).await;
    check!(view.state == ClassicGroupState::Empty);
    check!(view.generation_id == seeded.generation + 1);
    let persisted = last_classic_metadata(&log).await;
    check!(persisted.generation == seeded.generation + 1);
    check!(persisted.members.is_empty());
    check!(persisted.leader.is_none());
    check!(persisted.protocol_name.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_leave_with_members_remaining_does_not_commit_empty_generation() {
    let (coord, log) = make_coordinator();
    let seeded = seed_stable_classic(&coord, &["m1", "m2"]);
    let result = rpc::classic_leave_member(&seeded.handle, "m1").await;
    rpc::check_successful_classic_leave(&result);
    let view = rpc::classic_inspect(&seeded.handle).await;
    check!(view.generation_id == seeded.generation);
    check!(view.members.len() == 1);
    check!(view.members[0].member_id == "m2");
    check!(log.batches().await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_leave_legacy_append_failure_rolls_back_and_reports_error() {
    let (coord, log) = make_coordinator();
    let seeded = seed_stable_classic(&coord, &["m1"]);
    let handle = seeded.handle;
    log.fail_next
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let result = rpc::classic_leave_request(
        &handle,
        LeaveGroupRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            ..Default::default()
        },
        2,
    )
    .await;
    check!(result.error_code == codes::COORDINATOR_LOAD_IN_PROGRESS);
    check!(result.members.is_empty());
    let view = rpc::classic_inspect(&handle).await;
    rpc::check_stable_classic_member(&view, "m1");
    check!(log.batches().await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_delete_append_failure_keeps_group_registered() {
    let (coord, log) = make_coordinator();
    let _ = coord.get_or_create_classic("g");
    coord.mark_classic("g");
    log.fail_next
        .store(true, std::sync::atomic::Ordering::SeqCst);

    check!(coord.delete_group("g").await == Err(crate::coordinator::DeleteGroupError::Internal));
    check!(coord.describe_group("g").await.is_some());
    check!(coord.group_type("g") == Some(crate::coordinator::unified::GroupType::Classic));
    check!(!log.has_classic_group_metadata_tombstone("g").await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_leave_removes_a_hosted_member_from_an_upgraded_group() {
    let (_coord, _log, handle, native) = bidirectional_with_members().await;

    let response = rpc::classic_leave(&handle, "m-classic").await;
    check!(response.len() == 1);
    check!(response[0].error_code == codes::NONE);

    let view = rpc::describe(&handle).await;
    check!(view.members.len() == 1);
    check!(view.members[0].member_id == native);
    check!(!view.members[0].is_classic);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_leave_consumer_log_failure_stops_the_actor() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_consumer("g");
    let joined = rpc::consumer_heartbeat(&handle, "", 0, None).await;
    check!(joined.error_code == codes::NONE);
    let native = joined.member_id.expect("native member id");
    log.fail_next
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let result = rpc::classic_leave_member(&handle, &native).await;
    check!(result.error_code == codes::COORDINATOR_LOAD_IN_PROGRESS);
    check!(result.members.is_empty());

    tokio::time::timeout(std::time::Duration::from_secs(1), handle.tx.closed())
        .await
        .expect("consumer-kind log failure stops the actor");
    check!(handle.tx.is_closed());
}

#[test]
fn consumer_classic_leave_resolves_batch_and_static_identities() {
    use krabka_protocol::owned::leave_group_request::MemberIdentity;

    let mut group = GroupState::new("g");
    let dynamic = ConsumerGroupHeartbeatRequest {
        member_id: "m1".into(),
        ..Default::default()
    };
    group.add_or_update_member(build_member(
        "m1",
        &dynamic,
        ClientIdentity { id: "c", host: "h" },
        Instant::now(),
    ));
    let static_request = ConsumerGroupHeartbeatRequest {
        member_id: "m-static".into(),
        instance_id: Some("instance-a".into()),
        ..Default::default()
    };
    group.add_or_update_member(build_member(
        "m-static",
        &static_request,
        ClientIdentity { id: "c", host: "h" },
        Instant::now(),
    ));
    let request = LeaveGroupRequest {
        members: vec![
            MemberIdentity {
                member_id: "missing".into(),
                ..Default::default()
            },
            MemberIdentity {
                member_id: String::new(),
                group_instance_id: Some("instance-a".into()),
                ..Default::default()
            },
            MemberIdentity {
                member_id: "stale".into(),
                group_instance_id: Some("instance-a".into()),
                ..Default::default()
            },
            MemberIdentity {
                member_id: "m1".into(),
                ..Default::default()
            },
            MemberIdentity {
                member_id: "m1".into(),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let ConsumerLeave { responses, removed } = resolve_consumer_classic_leave(&group, &request, 3);
    check!(
        responses
            .iter()
            .map(|response| response.error_code)
            .collect::<Vec<_>>()
            == vec![
                codes::UNKNOWN_MEMBER_ID,
                codes::NONE,
                codes::FENCED_INSTANCE_ID,
                codes::NONE,
                codes::NONE,
            ]
    );
    check!(
        responses
            .iter()
            .map(|response| (
                response.member_id.as_str(),
                response.group_instance_id.as_deref(),
            ))
            .collect::<Vec<_>>()
            == vec![
                ("missing", None),
                ("", Some("instance-a")),
                ("stale", Some("instance-a")),
                ("m1", None),
                ("m1", None),
            ]
    );
    check!(removed == vec!["m-static".to_string(), "m1".to_string()]);
    check!(
        group.members.len() == 2,
        "resolution must not mutate the group"
    );

    let legacy = LeaveGroupRequest {
        member_id: "m1".into(),
        ..Default::default()
    };
    let ConsumerLeave { responses, removed } = resolve_consumer_classic_leave(&group, &legacy, 2);
    check!(responses.len() == 1);
    check!(responses[0].error_code == codes::NONE);
    check!(removed == vec!["m1".to_string()]);
}

/// A `TxnOffsetCommit` reserves its keys before its durable append and marks
/// them after it. A `DeleteGroups` that runs between the two must still
/// tombstone those keys: the transactional record is already in the log, and
/// its commit marker would otherwise bring the deleted group back. A released
/// reservation (a failed append) is not tombstoned, and a reservation sent
/// after the delete finds the actor stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_tombstones_the_keys_of_an_in_flight_transactional_commit() {
    use crate::coordinator::unified::{
        actor::TxnOffsetReservation,
        persistence::{Key, parse_key},
    };

    struct Row {
        name: &'static str,
        reserve: &'static [i32],
        release: &'static [i32],
        tombstoned: &'static [i32],
    }

    async fn reservation(
        handle: &GroupActorHandle,
        keys: Vec<(String, i32)>,
        reserve: bool,
    ) -> bool {
        let (reply, ack) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::TxnOffsetReservation(
                TxnOffsetReservation {
                    producer_id: 7,
                    keys,
                    reserve,
                    reply,
                },
            ))
            .await
            .is_ok()
            && ack.await.is_ok()
    }

    let keys = |partitions: &[i32]| -> Vec<(String, i32)> {
        partitions
            .iter()
            .map(|partition| ("orders".to_string(), *partition))
            .collect()
    };
    let rows = [
        Row {
            name: "reserved, not marked",
            reserve: &[1],
            release: &[],
            tombstoned: &[1],
        },
        Row {
            name: "reserved, then released",
            reserve: &[1],
            release: &[1],
            tombstoned: &[],
        },
        Row {
            name: "two reserved, one released",
            reserve: &[1, 2],
            release: &[2],
            tombstoned: &[1],
        },
    ];
    for Row {
        name,
        reserve,
        release,
        tombstoned: expected,
    } in rows
    {
        let (coord, log) = make_coordinator();
        let handle = coord.get_or_create_classic("g");
        coord.mark_classic("g");
        check!(reservation(&handle, keys(reserve), true).await, "{name}");
        if !release.is_empty() {
            check!(reservation(&handle, keys(release), false).await, "{name}");
        }

        check!(coord.delete_group("g").await == Ok(()), "{name}");

        let tombstoned: Vec<i32> = log
            .batches()
            .await
            .iter()
            .flat_map(|batch| &batch.records)
            .filter_map(|record| match parse_key(record.key.as_ref().unwrap()) {
                Ok(Key::OffsetCommit { partition, .. }) => Some(partition),
                _ => None,
            })
            .collect();
        check!(tombstoned == expected, "{name}");
        tokio::time::timeout(std::time::Duration::from_secs(1), handle.tx.closed())
            .await
            .expect("deleted group actor stops");
        check!(!reservation(&handle, keys(&[3]), true).await, "{name}");
    }
}

/// Kafka's `validateDeleteGroup` answers `NON_EMPTY_GROUP` for a group with
/// members, classic or KIP-848 consumer, and deletes an empty group of either
/// kind. A deleted consumer group leaves its group tombstones in the log and
/// no registry entry or seed behind, so no later request re-hydrates it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_answers_by_members_for_classic_and_consumer_groups() {
    use crate::coordinator::{DeleteGroupError, unified::GroupType};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Setup {
        EmptyConsumer,
        ConsumerWithMember,
        EmptyClassic,
    }

    /// Which tombstones a delete appended to the log.
    #[derive(Debug, PartialEq)]
    enum Tombstones {
        None,
        Consumer,
        Classic,
        Both,
    }

    /// What a delete leaves behind.
    #[derive(Debug, PartialEq)]
    struct Outcome {
        delete_result: Result<(), DeleteGroupError>,
        group_still_described: bool,
        type_lock: Option<GroupType>,
        seed_cached: bool,
        tombstones: Tombstones,
    }

    #[derive(Debug, PartialEq)]
    struct Row {
        setup: Setup,
        outcome: Outcome,
    }

    let rows: [Row; 3] = [
        Row {
            setup: Setup::EmptyConsumer,
            outcome: Outcome {
                delete_result: Ok(()),
                group_still_described: false,
                type_lock: None,
                seed_cached: false,
                tombstones: Tombstones::Consumer,
            },
        },
        Row {
            setup: Setup::ConsumerWithMember,
            outcome: Outcome {
                delete_result: Err(DeleteGroupError::NonEmpty),
                group_still_described: true,
                type_lock: Some(GroupType::NextGen),
                seed_cached: true,
                tombstones: Tombstones::None,
            },
        },
        Row {
            setup: Setup::EmptyClassic,
            outcome: Outcome {
                delete_result: Ok(()),
                group_still_described: false,
                type_lock: None,
                seed_cached: false,
                tombstones: Tombstones::Classic,
            },
        },
    ];
    let mut actual = Vec::with_capacity(rows.len());
    let mut expected = Vec::with_capacity(rows.len());
    for Row { setup, outcome } in rows {
        let (coord, log) = bidirectional_coordinator();
        match setup {
            Setup::EmptyConsumer | Setup::ConsumerWithMember => {
                let handle = coord.get_or_create_consumer("g");
                coord.mark_next_gen("g");
                let join = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
                check!(join.error_code == codes::NONE, "{setup:?}");
                if let Setup::EmptyConsumer = setup {
                    let member = join.member_id.expect("member id");
                    let leave = rpc::consumer_heartbeat(&handle, &member, -1, None).await;
                    check!(leave.error_code == codes::NONE, "{setup:?}");
                }
            }
            Setup::EmptyClassic => {
                let _ = coord.get_or_create_classic("g");
                coord.mark_classic("g");
            }
        }

        let result = coord.delete_group("g").await;

        actual.push(Row {
            setup,
            outcome: Outcome {
                delete_result: result,
                group_still_described: coord.describe_group("g").await.is_some(),
                type_lock: coord.group_type("g"),
                seed_cached: coord.cached_seed("g").is_some(),
                tombstones: match (
                    log.has_next_gen_group_metadata_tombstone("g").await
                        && log.has_next_gen_target_metadata_tombstone("g").await,
                    log.has_classic_group_metadata_tombstone("g").await,
                ) {
                    (false, false) => Tombstones::None,
                    (true, false) => Tombstones::Consumer,
                    (false, true) => Tombstones::Classic,
                    (true, true) => Tombstones::Both,
                },
            },
        });
        expected.push(Row { setup, outcome });
    }
    check!(actual == expected);
}

/// A member that leaves a `CompletingRebalance` group reopens the rebalance,
/// as Kafka's `maybePrepareRebalanceOrCompleteJoin` does, and the follower
/// parked in `SyncGroup` is answered `REBALANCE_IN_PROGRESS` rather than an
/// assignment that still names the departed member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_leave_in_completing_rebalance_reopens_the_rebalance() {
    let (coord, _log) = make_coordinator();
    let seeded = seed_completing_classic(&coord, &["m1", "m2", "m3"]);
    let handle = seeded.handle;
    let generation = seeded.generation;
    let sync_rx = rpc::begin_classic_sync(
        &handle,
        krabka_protocol::owned::sync_group_request::SyncGroupRequest {
            group_id: "g".into(),
            member_id: "m2".into(),
            generation_id: generation,
            ..Default::default()
        },
    )
    .await;

    let left = rpc::classic_leave(&handle, "m3").await;

    check!(
        left == vec![MemberResponse {
            member_id: "m3".into(),
            group_instance_id: None,
            error_code: codes::NONE,
            ..Default::default()
        }]
    );
    check!(sync_rx.await.unwrap().error_code == codes::REBALANCE_IN_PROGRESS);
    let view = rpc::classic_inspect(&handle).await;
    check!(view.state == ClassicGroupState::PreparingRebalance);
}
