//! The `ShareGroupHeartbeat` request path: first join, the steady-state
//! subscription and liveness update, and leave. It is the largest single
//! concern of the share-group actor, so it lives in its own file.

#[cfg(test)]
use crate::coordinator::unified::streams::config::StreamsGroupConfig;
use {
    super::{
        assignment::reconcile,
        records::{PendingShareRecords, ShareRecorder, chrono_now_ms, flush_pending},
        response::{build_assignment_resp, error_resp},
        share_state::{cleanup_deleted_topics, prepare_initialize, start_initialize},
    },
    crate::{
        codes,
        coordinator::unified::{
            ClientIdentity, GroupCoordinator,
            actor::MetadataProvider,
            member_helpers::update_present,
            offsets_log::OffsetsLog,
            share::{
                config::ShareGroupConfig,
                state::{ShareGroupState, ShareMemberState},
            },
        },
        error::BrokerError,
        metadata_epoch::next_i32,
    },
    krabka_protocol::owned::{
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
        share_group_heartbeat_response::ShareGroupHeartbeatResponse,
    },
    std::{collections::HashSet, time::Instant},
};

pub(super) async fn handle_heartbeat(
    state: &mut ShareGroupState,
    config: &ShareGroupConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    req: &ShareGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
) -> Result<ShareGroupHeartbeatResponse, BrokerError> {
    let now = Instant::now();
    let now_ms = chrono_now_ms();

    // ─── Leave path ──────────────────────────────────────────────
    if req.member_epoch == -1 {
        return handle_leave(
            state,
            config,
            metadata,
            offsets_log,
            coordinator,
            req,
            now_ms,
        )
        .await;
    }

    // ─── Member lookup ───────────────────────────────────────────
    // KIP-932 mirrors KIP-848: the client mints its own member UUID and
    // sends it with `member_epoch == 0`. Epoch 0 from an unknown member is a
    // first join under the client's id, which the handler has checked is set
    // (`KafkaApis.isMemberIdValid`). Epoch 0 from a known member is a rejoin.
    // Kafka's `getOrMaybeCreateMember` creates a new member with its defaults,
    // and the heartbeat then updates it like any other.
    let joining = req.member_epoch == 0 && !state.members.contains_key(&req.member_id);
    if joining {
        if state.members.len() >= config.max_size {
            return Ok(error_resp(codes::GROUP_MAX_SIZE_REACHED, config));
        }
    } else if let Err(error_code) = state.validate_member_epoch(&req.member_id, req.member_epoch) {
        return Ok(error_resp(error_code, config));
    }
    let recorder = ShareRecorder::start(state, &[&req.member_id]);
    let assigned_before = state
        .members
        .get(&req.member_id)
        .map(|m| m.assigned_partitions.clone());
    if joining {
        state.add_or_update_member(ShareMemberState::joining(
            &req.member_id,
            client.id,
            client.host,
            HashSet::new(),
        ));
    }
    let subscription_changed = update_member(state, req, client, now);
    let Ok(target) = reconcile(
        state,
        metadata,
        config.assignment_interval,
        subscription_changed,
    ) else {
        if joining {
            state.remove_member(&req.member_id);
        }
        return Ok(error_resp(codes::INVALID_REQUEST, config));
    };
    state.reconcile_member(&req.member_id, subscription_changed, metadata);
    let mut pending = recorder.finish(state, target.as_deref());
    // Kafka's `maybeCreateInitializeShareGroupStateRequest` writes the
    // partitions it initializes last in the heartbeat's batch.
    let initialize =
        prepare_initialize(state, config, coordinator, now_ms).map(|(value, initialize)| {
            pending.state_partition_metadata = Some(value);
            initialize
        });
    flush_pending(state, pending, offsets_log, coordinator, now_ms).await?;
    if let Some(initialize) = initialize {
        start_initialize(state, coordinator, initialize);
    }
    cleanup_deleted_topics(state, offsets_log, coordinator, now_ms).await;
    // Kafka sends the assignment only on a full request (a rejoin at epoch 0
    // or a request that carries the subscription) or when it changed.
    let assigned_changed = state
        .members
        .get(&req.member_id)
        .map(|m| &m.assigned_partitions)
        != assigned_before.as_ref();
    let with_assignment =
        req.member_epoch == 0 || req.subscribed_topic_names.is_some() || assigned_changed;
    Ok(build_assignment_resp(
        state,
        &req.member_id,
        config,
        with_assignment,
    ))
}

/// Kafka's `ShareGroupMember.Builder` updates of `shareGroupHeartbeat`, and
/// whether the heartbeat changed the member's subscribed topic names
/// (`hasMemberSubscriptionChanged`).
fn update_member(
    state: &mut ShareGroupState,
    req: &ShareGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    now: Instant,
) -> bool {
    let Some(m) = state.members.get_mut(&req.member_id) else {
        return false;
    };
    m.last_seen = now;
    client.update_metadata(&mut m.client_id, &mut m.client_host);
    // Kafka's `ShareGroupMember.Builder.maybeUpdateRackId`: a heartbeat that
    // carries a rack id replaces the stored one, a rejoin included.
    update_present(&mut m.rack_id, req.rack_id.as_ref());
    if let Some(ref names) = req.subscribed_topic_names {
        let set: HashSet<String> = names.iter().cloned().collect();
        if set != m.subscribed_topic_names {
            m.subscribed_topic_names = set;
            return true;
        }
    }
    false
}

/// Handle a leave-group heartbeat (`member_epoch == -1`).
///
/// It follows Kafka's `GroupMetadataManager.shareGroupLeave`. An unknown
/// member answers `UNKNOWN_MEMBER_ID` with Kafka's message and writes no
/// record. A known member is fenced (`shareGroupFenceMember`).
async fn handle_leave(
    state: &mut ShareGroupState,
    config: &ShareGroupConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    req: &ShareGroupHeartbeatRequest,
    now_ms: i64,
) -> Result<ShareGroupHeartbeatResponse, BrokerError> {
    if !state.members.contains_key(&req.member_id) {
        return Ok(ShareGroupHeartbeatResponse {
            error_code: codes::UNKNOWN_MEMBER_ID,
            error_message: Some(format!(
                "Member {} is not a member of group {}.",
                req.member_id, state.group_id
            )),
            ..Default::default()
        });
    }
    let Some(pending) = fence_member(state, metadata, &req.member_id) else {
        return Ok(error_resp(codes::INVALID_REQUEST, config));
    };
    flush_pending(state, pending, offsets_log, coordinator, now_ms).await?;
    Ok(leave_resp(&req.member_id, req.member_epoch))
}

/// Kafka's `shareGroupFenceMember`: the member's current assignment, target
/// assignment and subscription tombstones, and the group epoch bumped with
/// the metadata hash of the subscriptions that remain. The target waits for
/// the next heartbeat. `None` when the epoch is exhausted; the group is then
/// unchanged.
pub(super) fn fence_member(
    state: &mut ShareGroupState,
    metadata: &dyn MetadataProvider,
    member_id: &str,
) -> Option<PendingShareRecords> {
    next_i32(state.group_epoch)?;
    let recorder = ShareRecorder::start(state, &[member_id]);
    state.remove_member(member_id);
    state.bump_epoch();
    state.metadata_hash = super::assignment::metadata_hash(state, &metadata.snapshot());
    Some(recorder.finish(state, None))
}

/// The response to a successful leave: Kafka's `shareGroupLeave` sets only
/// the member id and the request's member epoch.
fn leave_resp(member_id: &str, member_epoch: i32) -> ShareGroupHeartbeatResponse {
    ShareGroupHeartbeatResponse {
        member_id: Some(member_id.to_owned()),
        member_epoch,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::{
            config::NextGenConfig,
            offsets_log::fake::InMemoryOffsetsLog,
            share::actor::test_support::{
                InitializedTopicSetup, ShareSubscriptionSetup, TopicMetadataSetup, heartbeat,
                make_coordinator, metadata_with_topic, seed_initialized, subscribed_group,
                subscribed_request,
            },
            test_support::{
                MemberEpoch, assert_single_member_limit, assignment_interval_fixture,
                heartbeat_write_failures,
            },
        },
        assert2::{assert, check},
        std::sync::Arc,
    };

    /// Kafka's `shareGroupHeartbeat`: only initialized partitions are
    /// assigned, the assignor keeps what it can when a member joins, and the
    /// response carries the assignment only for a join, a request with a
    /// subscription, or a changed assignment. Each row is one heartbeat and
    /// the whole response it gets.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn heartbeat_assignments_match_kafka() {
        use krabka_protocol::owned::{
            common::share_group_heartbeat_response::topic_partitions::TopicPartitions,
            share_group_heartbeat_response::Assignment,
        };

        let (metadata, id) = metadata_with_topic(TopicMetadataSetup::four_partitions());
        let response =
            |member: &str, epoch: i32, partitions: Option<Vec<i32>>| ShareGroupHeartbeatResponse {
                member_id: Some(member.into()),
                member_epoch: epoch,
                heartbeat_interval_ms: 5_000,
                assignment: partitions.map(|partitions| Assignment {
                    topic_partitions: if partitions.is_empty() {
                        Vec::new()
                    } else {
                        vec![TopicPartitions {
                            topic_id: id,
                            partitions,
                            ..Default::default()
                        }]
                    },
                    ..Default::default()
                }),
                ..Default::default()
            };
        let request = |member: &str, epoch: i32, subscribe: bool| ShareGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: member.into(),
            member_epoch: epoch,
            subscribed_topic_names: subscribe.then(|| vec!["t".into()]),
            ..Default::default()
        };
        // (initialized partitions, [(request, expected response)])
        let scenarios = [
            (None, vec![
                (request("m1", 0, true), response("m1", 2, Some(vec![]))),
                (request("m1", 2, false), response("m1", 2, None)),
            ]),
            (Some(vec![0, 1, 2, 3]), vec![
                (
                    request("m1", 0, true),
                    response("m1", 2, Some(vec![0, 1, 2, 3])),
                ),
                (request("m1", 2, false), response("m1", 2, None)),
                (request("m2", 0, true), response("m2", 3, Some(vec![0, 1]))),
                (request("m1", 2, false), response("m1", 3, Some(vec![2, 3]))),
                (request("m1", 3, false), response("m1", 3, None)),
                (request("m1", 3, true), response("m1", 3, Some(vec![2, 3]))),
            ]),
        ];
        for (index, (initialized, steps)) in scenarios.into_iter().enumerate() {
            let (coord, _log) = make_coordinator(metadata.clone());
            let handle = coord.get_or_create_share("g");
            if let Some(partitions) = initialized {
                seed_initialized(&handle, InitializedTopicSetup {
                    topic_id: id,
                    partitions: partitions
                        .into_iter()
                        .map(krabka_ids::PartitionIndex)
                        .collect(),
                    ..Default::default()
                })
                .await;
            }
            for (step, (req, expected)) in steps.into_iter().enumerate() {
                let resp = heartbeat(&handle, req).await;
                check!(resp == expected, "scenario {index} step {step}");
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn member_limit_rejects_only_new_members() {
        let (metadata, _id) = metadata_with_topic(TopicMetadataSetup::default());
        let log = Arc::new(InMemoryOffsetsLog::default());
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig::assigning_at_once(),
            ShareGroupConfig {
                max_size: 1,
                ..ShareGroupConfig::assigning_at_once()
            },
            metadata,
            log,
            StreamsGroupConfig::default(),
        ));
        let handle = coord.get_or_create_share("g");
        assert_single_member_limit(
            &handle,
            |member_id, epoch| {
                subscribed_request(ShareSubscriptionSetup {
                    member_id,
                    epoch: MemberEpoch(epoch),
                })
            },
            heartbeat,
        )
        .await;
    }

    /// Kafka's `shareGroupLeave`: a known member is fenced, with its records
    /// tombstoned and the group epoch bumped, and the response echoes its id
    /// and epoch `-1`. An unknown member answers `UNKNOWN_MEMBER_ID` and
    /// leaves the group and the log untouched.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn leave_matches_kafka() {
        // (leaving member id, expected response, expected new batches,
        // expected group epoch afterwards)
        let rows = [
            (
                "m1",
                ShareGroupHeartbeatResponse {
                    member_id: Some("m1".into()),
                    member_epoch: -1,
                    ..Default::default()
                },
                1,
                3,
            ),
            (
                "m9",
                ShareGroupHeartbeatResponse {
                    error_code: codes::UNKNOWN_MEMBER_ID,
                    error_message: Some("Member m9 is not a member of group g.".into()),
                    ..Default::default()
                },
                0,
                2,
            ),
        ];
        for (member_id, expected, new_batches, group_epoch) in rows {
            let (metadata, _id) = metadata_with_topic(TopicMetadataSetup::four_partitions());
            let (coord, log) = make_coordinator(metadata);
            let handle = coord.get_or_create_share("g");
            let joined = heartbeat(
                &handle,
                subscribed_request(ShareSubscriptionSetup::default()),
            )
            .await;
            check!(joined.error_code == codes::NONE);
            let pre_leave = log.batches().await.len();

            let resp = heartbeat(&handle, ShareGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: member_id.into(),
                member_epoch: -1,
                ..Default::default()
            })
            .await;

            check!(resp == expected, "{member_id}");
            let batches = log.batches().await;
            check!(batches.len() == pre_leave + new_batches, "{member_id}");
            if new_batches > 0 {
                check!(
                    batches[batches.len() - 1]
                        .records
                        .iter()
                        .any(|r| r.value.is_none()),
                    "leave batch must contain at least one tombstone"
                );
            }
            let seed = coord.cached_share_seed("g").expect("seed cached");
            check!(seed.group_epoch == group_epoch, "{member_id}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_epoch_is_fenced() {
        let (_coord, _log, handle) = subscribed_group(TopicMetadataSetup::four_partitions());
        let joined = heartbeat(
            &handle,
            subscribed_request(ShareSubscriptionSetup::default()),
        )
        .await;
        assert!(joined.member_epoch == 2);
        // Re-send with an epoch ahead of the server → fenced.
        let resp = heartbeat(
            &handle,
            subscribed_request(ShareSubscriptionSetup {
                epoch: MemberEpoch(99),
                ..Default::default()
            }),
        )
        .await;
        assert!(resp.error_code == codes::FENCED_MEMBER_EPOCH);
    }

    /// The member epoch rule of Kafka's `throwIfShareGroupMemberEpochIsInvalid`.
    /// Member `m1` is at epoch 4 with previous epoch 2: it joins at epoch 2,
    /// `m2` and `m3` join (group epochs 3 and 4), and `m1` heartbeats once at
    /// epoch 2. Each row sends one heartbeat on a fresh group and compares the
    /// whole response.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn member_epoch_rule_matches_kafka() {
        let request = subscribed_request;
        // (member id, request epoch, accepted)
        let rows = [
            ("m1", 0, true),
            ("m1", 2, true),
            ("m1", 3, false),
            ("m1", 4, true),
            ("m1", 5, false),
            ("m9", 4, false),
        ];

        for (index, (member_id, member_epoch, accepted)) in rows.into_iter().enumerate() {
            let (_coord, _log, handle) = subscribed_group(TopicMetadataSetup::four_partitions());
            check!(
                heartbeat(&handle, request(ShareSubscriptionSetup::default()))
                    .await
                    .member_epoch
                    == 2
            );
            check!(
                heartbeat(
                    &handle,
                    request(ShareSubscriptionSetup {
                        member_id: "m2",
                        ..Default::default()
                    })
                )
                .await
                .member_epoch
                    == 3
            );
            check!(
                heartbeat(
                    &handle,
                    request(ShareSubscriptionSetup {
                        member_id: "m3",
                        ..Default::default()
                    })
                )
                .await
                .member_epoch
                    == 4
            );
            let advanced = heartbeat(
                &handle,
                request(ShareSubscriptionSetup {
                    epoch: MemberEpoch(2),
                    ..Default::default()
                }),
            )
            .await;
            check!(advanced.member_epoch == 4);

            let resp = heartbeat(
                &handle,
                request(ShareSubscriptionSetup {
                    member_id,
                    epoch: MemberEpoch(member_epoch),
                }),
            )
            .await;

            let config = ShareGroupConfig::assigning_at_once();
            let expected = if accepted {
                // A rejoin and the previous epoch get the current epoch and
                // the full assignment, as a current heartbeat does.
                ShareGroupHeartbeatResponse {
                    member_id: Some("m1".into()),
                    member_epoch: 4,
                    assignment: advanced.assignment.clone(),
                    ..super::super::response::base_resp(codes::NONE, 4, &config)
                }
            } else if member_id == "m1" {
                super::super::response::error_resp(codes::FENCED_MEMBER_EPOCH, &config)
            } else {
                super::super::response::error_resp(codes::UNKNOWN_MEMBER_ID, &config)
            };
            check!(resp == expected, "row {index}");
        }
    }

    /// A heartbeat that carries a rack id replaces the stored one, a rejoin at
    /// epoch 0 included, and the member metadata record carries the new rack.
    /// A heartbeat without a rack id keeps it (`maybeUpdateRackId`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn heartbeat_updates_the_rack_id() {
        let (metadata, _id) = metadata_with_topic(TopicMetadataSetup::default());
        let (coord, _log) = make_coordinator(metadata);
        let handle = coord.get_or_create_share("g");
        let request = |member_epoch, rack_id: Option<&str>| ShareGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch,
            rack_id: rack_id.map(str::to_owned),
            subscribed_topic_names: Some(vec!["t".into()]),
            ..Default::default()
        };
        // (request epoch, request rack id, expected stored rack id)
        let rows = [
            (0, Some("rack-a"), Some("rack-a")),
            (0, Some("rack-b"), Some("rack-b")),
            (2, None, Some("rack-b")),
            (2, Some("rack-c"), Some("rack-c")),
        ];
        for (index, (member_epoch, rack_id, expected)) in rows.into_iter().enumerate() {
            let resp = heartbeat(&handle, request(member_epoch, rack_id)).await;
            check!(resp.error_code == codes::NONE, "row {index}");
            let seed = coord.cached_share_seed("g").expect("seed cached");
            check!(
                seed.members["m1"].rack_id.as_deref() == expected,
                "row {index}"
            );
        }
    }

    /// A heartbeat whose write fails answers the code of the failure, and the
    /// failed write leaves no partial batch. A write that is not committed
    /// answers what Kafka's `CoordinatorOperationExceptionHelper` answers for
    /// it, so a member that the coordinator never committed looks the
    /// coordinator up again rather than keep an epoch the next coordinator
    /// does not know.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_write_answers_its_code_and_writes_no_partial_batch() {
        for case in heartbeat_write_failures() {
            let fixture = case.coordinator(|| {
                let (metadata, _id) = metadata_with_topic(TopicMetadataSetup::default());
                make_coordinator(metadata)
            });
            let handle = fixture.coordinator.get_or_create_share("g");

            let response = heartbeat(
                &handle,
                subscribed_request(ShareSubscriptionSetup::default()),
            )
            .await;

            check!(
                response
                    == ShareGroupHeartbeatResponse {
                        error_code: fixture.expected.0,
                        ..Default::default()
                    },
                "{}",
                fixture.label
            );
            fixture.check_no_batches().await;
        }
    }

    /// KIP-1263: a share group replays the `AssignmentTimestamp` of its target
    /// assignment metadata record, and Kafka's
    /// `canComputeNextTargetAssignment` runs the assignment interval from it.
    /// A stored time inside the interval holds the next assignment back, and
    /// an unknown time (0) or an elapsed interval lets it run, which writes
    /// the time it finished.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_replayed_assignment_timestamp_holds_the_interval() {
        use crate::coordinator::unified::{ShareGroupSeed, wall_clock_ms};

        let (rows, mut cases) = assignment_interval_fixture();

        for (case, ago, wanted) in rows {
            let (coordinator, _) = super::super::test_support::topic_coordinator(
                TopicMetadataSetup::four_partitions(),
                ShareGroupConfig {
                    assignment_interval: std::time::Duration::from_mins(1),
                    ..ShareGroupConfig::default()
                },
            );
            let handle = coordinator.get_or_create_share("g");
            let stored = ago.map_or(0, |ago| wall_clock_ms() - ago);
            handle
                .tx
                .send(super::super::ShareGroupActorMessage::Seed(ShareGroupSeed {
                    group_epoch: 2,
                    target_epoch: 2,
                    assignment_timestamp_ms: stored,
                    ..ShareGroupSeed::default()
                }))
                .await
                .unwrap();
            let before = wall_clock_ms();
            let joined = heartbeat(
                &handle,
                subscribed_request(ShareSubscriptionSetup::default()),
            )
            .await;
            let after = wall_clock_ms();
            let written = coordinator
                .cached_share_seed("g")
                .unwrap()
                .assignment_timestamp_ms;
            cases.record(case, joined.member_epoch, (before, after), written, wanted);
        }
        cases.check();
    }
}
