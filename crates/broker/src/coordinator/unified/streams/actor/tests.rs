//! Unit tests that drive the streams-group actor through its handle: the
//! heartbeat epoch sequence with no connected `MetadataSource`, and the
//! resolution of a persisted per-group config override.

use {
    super::{
        test_support::{
            coordinator_with_log, describe, heartbeat_result_at, make_coordinator, member_request,
            response_tasks, undelayed,
        },
        *,
    },
    crate::{
        api_catalog::UnstableApiVersions,
        coordinator::unified::{
            offsets_log::fake::InMemoryOffsetsLog,
            persistence::parse_key,
            streams::{
                config::KEY_NUM_STANDBY_REPLICAS,
                persistence::{
                    DescriptionEpochs, LastAssignmentConfig, PendingStreamsRecords,
                    StreamsGroupMetadataValue,
                },
                state::{StreamsGroupState, StreamsMemberState},
            },
            test_support::{MemberEpoch, assert_single_member_limit, heartbeat_write_failures},
        },
        test_support::{StreamsJoinSetup, streams_join_request},
    },
    assert2::{assert, check},
};

krabka_macros::single_replica_partition_fixture!(partition_record);

fn epoch_five_member() -> StreamsMemberState {
    let mut member = StreamsMemberState::joining("m1", "client", "/127.0.0.1");
    member.member_epoch = 5;
    member
}

#[test]
fn persisted_group_config_overrides_actor_defaults() {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    image.apply(&krabka_metadata::MetadataRecord::V1GroupConfig(
        krabka_metadata::GroupConfigRecord {
            group_id: "streams-app".into(),
            configs: maplit::btreemap! {KEY_NUM_STANDBY_REPLICAS.into() => "1".into()},
        },
    ));
    let config =
        resolve_group_config_from_image(&StreamsGroupConfig::default(), &image, "streams-app");
    assert!(config.num_standby_replicas == 1);

    let unaffected =
        resolve_group_config_from_image(&StreamsGroupConfig::default(), &image, "other-app");
    assert!(unaffected == StreamsGroupConfig::default());
}

/// The member ids that the group holds, sorted.
async fn describe_member_ids(handle: &StreamsGroupActorHandle) -> Vec<String> {
    let mut ids: Vec<String> = describe(handle)
        .await
        .members
        .into_iter()
        .map(|member| member.member_id)
        .collect();
    ids.sort();
    ids
}

async fn heartbeat_result(
    handle: &StreamsGroupActorHandle,
    req: StreamsGroupHeartbeatRequest,
) -> StreamsHeartbeatResult {
    heartbeat_result_at(
        handle,
        req,
        krabka_protocol::owned::streams_group_heartbeat_request::MAX_VERSION,
    )
    .await
}

async fn heartbeat(
    handle: &StreamsGroupActorHandle,
    req: StreamsGroupHeartbeatRequest,
) -> StreamsGroupHeartbeatResponse {
    heartbeat_result(handle, req).await.response
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_join_advances_epoch_not_ready() {
    let (coord, _log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let resp = heartbeat(&handle, member_request("m1", 0)).await;
    check!(resp.error_code == codes::NONE);
    check!(resp.member_id == "m1");
    // No metadata source / no topology → NotReady, empty assignment, but the
    // member still advances to the group epoch, which the join bumps past
    // the initial epoch 1.
    check!(resp.member_epoch == 2);
    check!(resp.active_tasks == Some(vec![]));
    check!(resp.standby_tasks == Some(vec![]));
    check!(resp.warmup_tasks == Some(vec![]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_heartbeat_at_right_epoch_accepted() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let join = heartbeat(&handle, member_request("m1", 0)).await;
    assert!(join.error_code == codes::NONE);
    let epoch = join.member_epoch;
    let batches = log.batches().await.len();
    let resp = heartbeat(&handle, member_request("m1", epoch)).await;
    assert!(resp.error_code == codes::NONE);
    assert!(resp.member_epoch == epoch);
    // A heartbeat that changes nothing writes nothing, as Kafka's
    // `streamsGroupHeartbeat` adds no record for an unchanged member.
    assert!(log.batches().await.len() == batches);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn member_limit_rejects_only_new_members() {
    let log = Arc::new(InMemoryOffsetsLog::default());
    let coord = coordinator_with_log(
        StreamsGroupConfig {
            max_size: 1,
            ..undelayed()
        },
        log,
    );
    let handle = coord.get_or_create_streams("g");
    assert_single_member_limit(&handle, member_request, heartbeat).await;
}

/// Kafka 4.3.1's `throwIfStreamsGroupIsFull(group)` runs on every join and
/// counts a member that is already in the group, so a rejoin at epoch 0 to a
/// full group is refused. Trunk exempts the known member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_known_member_that_rejoins_a_full_group_is_refused_by_4_3_1_only() {
    use crate::api_catalog::UnstableApiVersions::{Disabled, Enabled};

    for (unstable, expected) in [
        (Disabled, codes::GROUP_MAX_SIZE_REACHED),
        (Enabled, codes::NONE),
    ] {
        let coord = coordinator_with_log(
            StreamsGroupConfig {
                max_size: 1,
                unstable_api_versions: unstable,
                ..undelayed()
            },
            Arc::new(InMemoryOffsetsLog::default()),
        );
        let handle = coord.get_or_create_streams("g");
        let join = member_request("m1", 0);

        let joined = heartbeat(&handle, join.clone()).await;
        let rejoined = heartbeat(&handle, join).await;

        check!(joined.error_code == codes::NONE, "{unstable:?}");
        check!(rejoined.error_code == expected, "{unstable:?}");
    }
}

/// The member epoch rule of Kafka's `throwIfStreamsGroupMemberEpochIsInvalid`.
/// Member `m1` is at epoch 4 with previous epoch 3: it joins at epoch 2, the
/// first bump past the initial group epoch 1, `m2` joins (group epoch 3), `m1`
/// heartbeats at 2, `m3` joins (group epoch 4), and `m1` heartbeats at 3.
/// With no metadata source every assignment is
/// empty. Each row sends one heartbeat on a fresh group. An accepted row must
/// answer exactly what a heartbeat at the member epoch answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn member_epoch_rule_matches_kafka() {
    use krabka_protocol::owned::common::streams_group_heartbeat_request::task_ids::TaskIds;

    // A request that reports owned tasks reports all three lists, as the
    // Streams client does; the standby and warmup lists are empty.
    let request = |member_id: &str, member_epoch, active_tasks: Option<Vec<TaskIds>>| {
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: member_id.into(),
            member_epoch,
            standby_tasks: active_tasks.as_ref().map(|_| vec![]),
            warmup_tasks: active_tasks.as_ref().map(|_| vec![]),
            active_tasks,
            ..Default::default()
        }
    };
    let unassigned = Some(vec![TaskIds {
        subtopology_id: "s".into(),
        partitions: vec![0],
        ..Default::default()
    }]);
    // (request epoch, owned active tasks, expected error code)
    let rows = [
        (0, None, codes::NONE),
        (3, Some(vec![]), codes::NONE),
        (3, unassigned, codes::FENCED_MEMBER_EPOCH),
        (3, None, codes::FENCED_MEMBER_EPOCH),
        (2, Some(vec![]), codes::FENCED_MEMBER_EPOCH),
        (4, None, codes::NONE),
        (5, Some(vec![]), codes::FENCED_MEMBER_EPOCH),
    ];

    for (index, (member_epoch, active_tasks, error_code)) in rows.into_iter().enumerate() {
        let (coord, _log) = make_coordinator();
        let handle = coord.get_or_create_streams("g");
        check!(
            heartbeat(&handle, request("m1", 0, None))
                .await
                .member_epoch
                == 2
        );
        check!(
            heartbeat(&handle, request("m2", 0, None))
                .await
                .member_epoch
                == 3
        );
        check!(
            heartbeat(&handle, request("m1", 2, None))
                .await
                .member_epoch
                == 3
        );
        check!(
            heartbeat(&handle, request("m3", 0, None))
                .await
                .member_epoch
                == 4
        );
        check!(
            heartbeat(&handle, request("m1", 3, None))
                .await
                .member_epoch
                == 4
        );

        let resp = heartbeat(&handle, request("m1", member_epoch, active_tasks)).await;

        let expected = if error_code == codes::NONE {
            // A rejoin at epoch 0 gets the task lists; a heartbeat with an
            // unchanged assignment does not.
            let rejoin = (member_epoch == 0).then(Vec::new);
            StreamsGroupHeartbeatResponse {
                active_tasks: rejoin.clone(),
                standby_tasks: rejoin.clone(),
                warmup_tasks: rejoin,
                ..heartbeat(&handle, request("m1", 4, None)).await
            }
        } else {
            let relation = if member_epoch > 4 {
                "greater"
            } else {
                "smaller"
            };
            super::response::error_resp(
                error_code,
                Some(format!(
                    "The streams group member has a {relation} member epoch ({member_epoch}) than \
                     the one known by the group coordinator (4). The member must abandon all its \
                     partitions and rejoin."
                )),
            )
        };
        check!(resp == expected, "row {index}");
    }
}

/// Kafka's `streamsGroupLeave` answers `UNKNOWN_MEMBER_ID` to a member that
/// the group does not have, and writes no record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leave_of_an_unknown_member_is_refused_and_writes_nothing() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let joined = heartbeat(&handle, member_request("m1", 0)).await;
    check!(joined.error_code == codes::NONE);
    let before = log.batches().await;

    let resp = heartbeat(&handle, StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: "m9".into(),
        member_epoch: -1,
        ..Default::default()
    })
    .await;

    check!(
        resp == super::response::error_resp(
            codes::UNKNOWN_MEMBER_ID,
            Some("Member m9 is not a member of group g.".into())
        )
    );
    check!(log.batches().await == before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_epoch_is_rejected() {
    let (coord, _log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let join = heartbeat(&handle, member_request("m1", 0)).await;
    assert!(join.member_epoch == 2);
    let resp = heartbeat(&handle, member_request("m1", 99)).await;
    assert!(resp.error_code == codes::FENCED_MEMBER_EPOCH);
}

/// A heartbeat whose write fails answers the code of the failure, and the
/// failed write leaves no partial batch. A write that is not committed answers
/// what Kafka's `CoordinatorOperationExceptionHelper` answers for it, so that
/// a member whose join the coordinator never committed looks the coordinator
/// up again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_write_answers_its_code_and_writes_no_partial_batch() {
    for case in heartbeat_write_failures() {
        let fixture = case.coordinator(make_coordinator);
        let handle = fixture.coordinator.get_or_create_streams("g");

        let response = heartbeat(&handle, member_request("m1", 0)).await;

        check!(
            response == super::response::error_resp(fixture.expected.0, None),
            "{}",
            fixture.label
        );
        fixture.check_no_batches().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leave_removes_member() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let join = heartbeat(&handle, StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: String::new(),
        member_epoch: 0,
        ..Default::default()
    })
    .await;
    let mid = join.member_id.clone();
    let pre_leave = log.batches().await.len();

    let resp = heartbeat(&handle, StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: mid,
        member_epoch: -1,
        ..Default::default()
    })
    .await;
    assert!(resp.error_code == codes::NONE);
    assert!(resp.member_epoch == -1);
    let batches = log.batches().await;
    assert!(batches.len() == pre_leave + 1);
    // Kafka's `streamsGroupFenceMember`: the member's current assignment,
    // target and member tombstones, and the group epoch bumped with the
    // group's metadata as it was, in one batch, and no target.
    let seed = coord.cached_streams_seed("g").expect("the group's records");
    let expected = PendingStreamsRecords {
        member_metadata: vec![(join.member_id.clone(), None)],
        target_per_member: vec![(join.member_id.clone(), None)],
        current_per_member: vec![(join.member_id.clone(), None)],
        group_metadata: Some(StreamsGroupMetadataValue {
            epoch: join.member_epoch + 1,
            metadata_hash: seed.metadata_hash,
            validated_topology_epoch: seed.validated_topology_epoch,
            last_assignment_configs: Some(
                seed.last_assignment_configs
                    .iter()
                    .map(|(key, value)| LastAssignmentConfig {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .collect(),
            ),
            description: DescriptionEpochs::default(),
        }),
        ..Default::default()
    }
    .into_batch("g", 0)
    .unwrap();
    let key_values = |batch: &krabka_protocol::records::RecordBatch| -> Vec<_> {
        batch
            .records
            .iter()
            .map(|record| (record.key.clone(), record.value.clone()))
            .collect()
    };
    assert!(key_values(&batches[batches.len() - 1]) == key_values(&expected));
    assert!(
        (seed.group_epoch, seed.assignment_epoch) == (join.member_epoch + 1, join.member_epoch)
    );
}

/// A heartbeat applies the member fields that it carries, a rejoin at epoch 0
/// included, and keeps the fields that it leaves out
/// (`StreamsGroupMember.Builder.maybeUpdate*`). Each row sends one heartbeat
/// and compares the persisted `(process_id, rack_id, rebalance_timeout_ms,
/// user endpoint)` of the member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_applies_member_metadata() {
    use krabka_protocol::owned::common::streams_group_heartbeat_request::endpoint::Endpoint;

    let (coord, _log) = make_coordinator();
    let handle = coord.get_or_create_streams("g");
    let request =
        |member_epoch, process: Option<&str>, rack: Option<&str>, timeout, port: Option<u16>| {
            StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch,
                process_id: process.map(str::to_owned),
                rack_id: rack.map(str::to_owned),
                rebalance_timeout_ms: timeout,
                user_endpoint: port.map(|port| Endpoint {
                    host: "h".into(),
                    port,
                    ..Default::default()
                }),
                ..Default::default()
            }
        };
    let joined = heartbeat(&handle, request(0, Some("p1"), Some("r1"), 1_000, Some(1))).await;
    check!(joined.error_code == codes::NONE);
    let epoch = joined.member_epoch;
    // (request, expected (process id, rack id, rebalance timeout, endpoint port))
    let rows = [
        (
            request(0, Some("p2"), Some("r2"), 2_000, None),
            ("p2", Some("r2"), 2_000, None),
        ),
        (
            request(-2, None, None, -1, Some(7)),
            ("p2", Some("r2"), 2_000, Some(7)),
        ),
        (
            request(-2, Some("p3"), None, 3_000, None),
            ("p3", Some("r2"), 3_000, Some(7)),
        ),
    ];
    for (index, (mut req, (process, rack, timeout, port))) in rows.into_iter().enumerate() {
        if req.member_epoch == -2 {
            req.member_epoch = coord
                .cached_streams_seed("g")
                .and_then(|seed| seed.current_per_member.get("m1").map(|c| c.member_epoch))
                .unwrap_or(epoch);
        }
        let resp = heartbeat(&handle, req).await;
        check!(resp.error_code == codes::NONE, "row {index}");
        let member = coord.cached_streams_seed("g").expect("seed cached").members["m1"].clone();
        check!(
            (
                member.process_id.as_str(),
                member.rack_id.as_deref(),
                member.rebalance_timeout_ms,
                member.user_endpoint.map(|endpoint| endpoint.port),
            ) == (process, rack, timeout, port),
            "row {index}"
        );
    }
}

/// Builds the metadata image of one broker, `broker_rack` its rack, that
/// holds each `(name, id byte, partitions)` topic.
fn image_of(
    broker_rack: Option<&str>,
    topics: &[(&str, u8, i32)],
) -> krabka_metadata::MetadataImage {
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord, TopicRecord};

    let broker = krabka_audit::NodeId(1);
    let mut records = vec![MetadataRecord::V1BrokerRegistration(
        BrokerRegistrationRecord {
            rack: broker_rack.map(str::to_owned),
            ..crate::test_support::broker_registration(broker)
        },
    )];
    for &(name, id, partitions) in topics {
        records.push(MetadataRecord::V1Topic(TopicRecord {
            name: name.into(),
            topic_id: uuid::Uuid::from_bytes([id; 16]),
            partitions,
            replication_factor: 1,
        }));
        for partition in 0..partitions {
            records.push(MetadataRecord::V1Partition(partition_record(
                name,
                krabka_ids::PartitionIndex(partition),
                broker,
            )));
        }
    }
    krabka_metadata::MetadataImage::from_records(uuid::Uuid::nil(), &records)
}

#[derive(Clone, Copy, Default)]
enum TopologyStorage {
    #[default]
    Stateless,
    StoreChangelog,
}

#[derive(Clone, Copy, Default)]
struct SingleSubtopologySetup {
    storage: TopologyStorage,
}

/// A topology with one subtopology `0` that reads `in` and, with a state store,
/// keeps the changelog topic `store-changelog`.
fn one_subtopology(
    setup: SingleSubtopologySetup,
) -> krabka_protocol::owned::streams_group_heartbeat_request::Topology {
    use krabka_protocol::owned::{
        common::streams_group_heartbeat_request::topic_info::TopicInfo,
        streams_group_heartbeat_request::{Subtopology, Topology},
    };

    Topology {
        epoch: 1,
        subtopologies: vec![Subtopology {
            subtopology_id: "0".into(),
            source_topics: vec!["in".into()],
            state_changelog_topics: if matches!(setup.storage, TopologyStorage::StoreChangelog) {
                vec![TopicInfo {
                    name: "store-changelog".into(),
                    ..Default::default()
                }]
            } else {
                vec![]
            },
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Kafka refreshes the topic metadata of a streams group on the next
/// heartbeat after a change (`onMetadataUpdate`, `hasMetadataExpired`,
/// `computeMetadataHash`) and bumps the group epoch when the hash or the
/// member metadata changed (`hasStreamsMemberMetadataChanged`). Each row joins
/// one member, applies one change, sends one heartbeat at the member epoch, and
/// compares the whole response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_heartbeat_after_a_topic_or_member_change_recomputes_the_assignment() {
    use {
        crate::{
            coordinator::unified::streams::topology::status, test_support::FakeMetadataSource,
        },
        krabka_protocol::owned::common::streams_group_heartbeat_response::status::Status,
    };

    enum Change {
        Image(Box<krabka_metadata::MetadataImage>),
        Rack(&'static str),
        Process(&'static str),
    }
    struct Row {
        name: &'static str,
        initial: krabka_metadata::MetadataImage,
        storage: TopologyStorage,
        change: Change,
        /// The owned active tasks of the heartbeat, when it reports them.
        owned_active: Option<Vec<i32>>,
        epoch: i32,
        /// The active tasks, when the response sends the task lists.
        active: Option<Vec<i32>>,
        status: Option<Vec<Status>>,
    }
    let rows = [
        Row {
            name: "the missing source topic is created",
            initial: image_of(None, &[]),
            storage: TopologyStorage::Stateless,
            change: Change::Image(Box::new(image_of(None, &[("in", 1, 2)]))),
            owned_active: None,
            epoch: 3,
            active: Some(vec![0, 1]),
            status: Some(vec![]),
        },
        Row {
            name: "partitions are added to the source topic",
            initial: image_of(None, &[("in", 1, 1)]),
            storage: TopologyStorage::Stateless,
            change: Change::Image(Box::new(image_of(None, &[("in", 1, 2)]))),
            owned_active: Some(vec![0]),
            epoch: 3,
            active: Some(vec![0, 1]),
            status: Some(vec![]),
        },
        Row {
            name: "the source topic is deleted",
            initial: image_of(None, &[("in", 1, 2)]),
            storage: TopologyStorage::Stateless,
            change: Change::Image(Box::new(image_of(None, &[]))),
            owned_active: Some(vec![]),
            epoch: 3,
            active: Some(vec![]),
            status: Some(vec![Status {
                status_code: status::MISSING_SOURCE_TOPICS,
                status_detail: "Source topics in are missing.".into(),
                ..Default::default()
            }]),
        },
        Row {
            name: "the member sends a new rack id",
            initial: image_of(None, &[("in", 1, 1)]),
            storage: TopologyStorage::Stateless,
            change: Change::Rack("rack-b"),
            owned_active: Some(vec![0]),
            epoch: 3,
            active: None,
            status: Some(vec![]),
        },
        Row {
            name: "the member sends a new process id",
            initial: image_of(None, &[("in", 1, 1)]),
            storage: TopologyStorage::Stateless,
            change: Change::Process("process-b"),
            owned_active: Some(vec![0]),
            epoch: 3,
            active: None,
            status: Some(vec![]),
        },
        Row {
            name: "the internal topic is created",
            initial: image_of(None, &[("in", 1, 1)]),
            storage: TopologyStorage::StoreChangelog,
            change: Change::Image(Box::new(image_of(None, &[
                ("in", 1, 1),
                ("store-changelog", 2, 1),
            ]))),
            owned_active: Some(vec![]),
            epoch: 3,
            active: Some(vec![0]),
            status: Some(vec![]),
        },
    ];

    for row in rows {
        let source = Arc::new(FakeMetadataSource::builder().image(row.initial).build());
        let (coord, _log) = make_coordinator();
        coord.set_metadata_source(source.clone());
        let handle = coord.get_or_create_streams("g");
        let request = |member_epoch, rack: &str, process: &str| StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch,
            rack_id: Some(rack.into()),
            process_id: Some(process.into()),
            rebalance_timeout_ms: 1_000,
            ..Default::default()
        };
        let joined = heartbeat(&handle, StreamsGroupHeartbeatRequest {
            topology: Some(one_subtopology(SingleSubtopologySetup {
                storage: row.storage,
            })),
            ..request(0, "rack-a", "process-a")
        })
        .await;
        check!(joined.member_epoch == 2, "{}", row.name);

        let (rack, process) = match row.change {
            Change::Image(image) => {
                source.set_image(*image);
                ("rack-a", "process-a")
            }
            Change::Rack(rack) => (rack, "process-a"),
            Change::Process(process) => ("rack-a", process),
        };
        let owned = row.owned_active.map(|partitions| {
            vec![
                krabka_protocol::owned::common::streams_group_heartbeat_request::task_ids::TaskIds {
                    subtopology_id: "0".into(),
                    partitions,
                    ..Default::default()
                },
            ]
        });
        let resp = heartbeat(&handle, StreamsGroupHeartbeatRequest {
            standby_tasks: owned.as_ref().map(|_| vec![]),
            warmup_tasks: owned.as_ref().map(|_| vec![]),
            active_tasks: owned,
            ..request(joined.member_epoch, rack, process)
        })
        .await;

        let expected = super::test_support::expected_active_response(
            super::test_support::ActiveResponseSetup {
                epoch: MemberEpoch(row.epoch),
                status: row.status,
                active: row.active.map(|partitions| {
                    partitions
                        .into_iter()
                        .map(krabka_ids::PartitionIndex)
                        .collect()
                }),
                ..Default::default()
            },
        );
        check!(resp == expected, "{}", row.name);
    }
}

/// A reconcile that installs a new target changes the assignment of every
/// member, so the record batch of the heartbeat that ran it carries the
/// target assignment of every member, as Kafka's `TargetAssignmentBuilder`
/// writes one record for each member whose target changed. Without them a
/// replay pairs the new assignment epoch with old member targets.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_target_persists_the_target_of_every_member() {
    use crate::{
        coordinator::unified::{persistence::Key, streams::persistence::StreamsGroupKey},
        test_support::FakeMetadataSource,
    };

    let source = Arc::new(
        FakeMetadataSource::builder()
            .image(image_of(None, &[("in", 1, 2)]))
            .build(),
    );
    let (coord, log) = make_coordinator();
    coord.set_metadata_source(source.clone());
    let handle = coord.get_or_create_streams("g");
    let request = |member_id: &str, member_epoch| StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        rebalance_timeout_ms: 1_000,
        topology: (member_epoch == 0).then(|| one_subtopology(SingleSubtopologySetup::default())),
        ..Default::default()
    };
    let m1 = heartbeat(&handle, request("m1", 0)).await;
    let m2 = heartbeat(&handle, request("m2", 0)).await;
    check!((m1.member_epoch, m2.member_epoch) == (2, 3));

    source.set_image(image_of(None, &[("in", 1, 4)]));
    let resp = heartbeat(&handle, request("m1", 2)).await;
    check!(resp.member_epoch == 4);

    let batches = log.batches().await;
    let last = batches.last().expect("the heartbeat wrote a batch");
    let mut targets: Vec<String> = last
        .records
        .iter()
        .filter_map(|record| {
            let key = record.key.as_deref()?;
            match parse_key(key) {
                Ok(Key::Streams(StreamsGroupKey::TargetAssignmentMember { member_id, .. })) => {
                    Some(member_id)
                }
                _ => None,
            }
        })
        .collect();
    targets.sort();
    check!(targets == vec!["m1".to_string(), "m2".to_string()]);
}

/// A seeded group whose internal topics are missing asks for them again on
/// the next heartbeat, as Kafka configures the topology of a loaded group on
/// its first heartbeat and returns the internal topics to create. The seed
/// carries the metadata hash, so the hash alone does not trigger it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seeded_group_asks_for_its_missing_internal_topics_again() {
    use {
        crate::{
            coordinator::unified::streams::topology::{InternalTopicSpec, status},
            test_support::FakeMetadataSource,
        },
        krabka_protocol::owned::common::streams_group_heartbeat_response::status::Status,
    };

    let image = || image_of(None, &[("in", 1, 1)]);
    let (before, _log) = make_coordinator();
    before.set_metadata_source(Arc::new(
        FakeMetadataSource::builder().image(image()).build(),
    ));
    let joined = heartbeat(
        &before.get_or_create_streams("g"),
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 0,
            rebalance_timeout_ms: 1_000,
            topology: Some(one_subtopology(SingleSubtopologySetup {
                storage: TopologyStorage::StoreChangelog,
            })),
            ..Default::default()
        },
    )
    .await;
    check!(joined.member_epoch == 2);
    let seed = before
        .cached_streams_seed("g")
        .expect("the join cached a seed");

    let (after, _log) = make_coordinator();
    after.set_metadata_source(Arc::new(
        FakeMetadataSource::builder().image(image()).build(),
    ));
    after.update_streams_cache("g", seed);
    let result = heartbeat_result(&after.get_or_create_streams("g"), member_request("m1", 2)).await;

    check!(
        result.creatable_topics
            == vec![InternalTopicSpec {
                name: "store-changelog".into(),
                partitions: 1,
                replication_factor: 0,
                configs: std::collections::BTreeMap::new(),
            }]
    );
    let expected = StreamsGroupHeartbeatResponse {
        member_id: "m1".into(),
        status: Some(vec![Status {
            status_code: status::MISSING_INTERNAL_TOPICS,
            status_detail: "Internal topics are missing: store-changelog".into(),
            ..Default::default()
        }]),
        ..super::response::base_resp(codes::NONE, 2, &undelayed())
    };
    check!(result.response == expected);
}

/// The `__consumer_offsets` replay spawns the actor of a loaded group before
/// the broker connects the coordinator's metadata source. The actor still
/// reconciles against the metadata image once the source is there, as Kafka's
/// coordinator gives every loaded group the image: a member that joins after a
/// restart gets the task that the loaded member revokes, and the loaded member
/// keeps the other one.
///
/// Each row is one heartbeat of the exchange after the restart and the whole
/// response it gets. A group that never sees the source assigns nothing, so
/// the loaded member revokes both of its tasks instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_loaded_before_the_metadata_source_connects_assigns_its_tasks() {
    use {
        crate::test_support::FakeMetadataSource,
        krabka_protocol::owned::common::streams_group_heartbeat_request::task_ids::TaskIds as OwnedTaskIds,
    };

    struct Row {
        member_id: &'static str,
        member_epoch: i32,
        /// The active tasks that the heartbeat reports as owned.
        owned: Vec<i32>,
        epoch: i32,
        /// The active tasks, when the response sends the task lists.
        active: Option<Vec<i32>>,
    }

    let source = || {
        Arc::new(
            FakeMetadataSource::builder()
                .image(image_of(None, &[("in", 1, 2)]))
                .build(),
        )
    };
    let request = |member_id: &str, member_epoch, owned: Vec<i32>| StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        process_id: Some(format!("process-{member_id}")),
        rebalance_timeout_ms: 60_000,
        topology: (member_epoch == 0).then(|| one_subtopology(SingleSubtopologySetup::default())),
        active_tasks: Some(if owned.is_empty() {
            vec![]
        } else {
            vec![OwnedTaskIds {
                subtopology_id: "0".into(),
                partitions: owned,
                ..Default::default()
            }]
        }),
        standby_tasks: Some(vec![]),
        warmup_tasks: Some(vec![]),
        ..Default::default()
    };
    let expected = |member_id: &str, epoch, active: Option<Vec<i32>>| {
        super::test_support::expected_active_response(super::test_support::ActiveResponseSetup {
            member_id,
            epoch: MemberEpoch(epoch),
            status: Some(vec![]),
            active: active.map(|partitions| {
                partitions
                    .into_iter()
                    .map(krabka_ids::PartitionIndex)
                    .collect()
            }),
        })
    };

    let (before, _log) = make_coordinator();
    before.set_metadata_source(source());
    let joined = heartbeat(&before.get_or_create_streams("g"), request("m1", 0, vec![])).await;
    check!(joined == expected("m1", 2, Some(vec![0, 1])));
    let seed = before
        .cached_streams_seed("g")
        .expect("the join cached a seed");

    // The order of `Broker::start`: the replay seeds the group and spawns its
    // actor, and only then does the broker connect the metadata source.
    let (after, _log) = make_coordinator();
    after.streams_seeds.insert("g".into(), seed);
    after.finalize_bootstrap();
    after.set_metadata_source(source());
    let handle = after
        .find_streams("g")
        .expect("the replay spawned the actor");

    let rows = [
        Row {
            member_id: "m2",
            member_epoch: 0,
            owned: vec![],
            epoch: 3,
            active: Some(vec![]),
        },
        Row {
            member_id: "m1",
            member_epoch: 2,
            owned: vec![0, 1],
            epoch: 2,
            active: Some(vec![0]),
        },
        Row {
            member_id: "m1",
            member_epoch: 2,
            owned: vec![0],
            epoch: 3,
            active: None,
        },
        Row {
            member_id: "m2",
            member_epoch: 3,
            owned: vec![],
            epoch: 3,
            active: Some(vec![1]),
        },
    ];
    for (step, row) in rows.into_iter().enumerate() {
        let resp = heartbeat(&handle, request(row.member_id, row.member_epoch, row.owned)).await;
        check!(
            resp == expected(row.member_id, row.epoch, row.active),
            "step {step}"
        );
    }
}

/// Kafka sizes the internal topics with `InternalTopicManager`: a repartition
/// topic takes the partition count of its writer, and a copartition group
/// coerces it to the partition count of the external topics. Each row joins
/// one member with a two-subtopology topology and compares the internal topics
/// that the heartbeat asks `CreateTopics` for, and the whole response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_join_sizes_the_internal_topics_as_kafka_does() {
    use {
        crate::{
            coordinator::unified::streams::topology::{InternalTopicSpec, status},
            test_support::FakeMetadataSource,
        },
        krabka_protocol::owned::{
            common::{
                streams_group_heartbeat_request::topic_info::TopicInfo,
                streams_group_heartbeat_response::status::Status,
            },
            streams_group_heartbeat_request::{CopartitionGroup, Subtopology, Topology},
        },
    };

    struct Row {
        name: &'static str,
        /// The partition counts of `orders` and `customers`.
        partitions: (i32, i32),
        copartitioned: bool,
        /// The expected partition counts of `rp` and `store-changelog`.
        rp: i32,
        changelog: i32,
    }
    let rows = [
        Row {
            name: "copartition coerces rp to the customers topic",
            partitions: (6, 3),
            copartitioned: true,
            rp: 3,
            changelog: 3,
        },
        Row {
            name: "rp takes the partition count of its writer",
            partitions: (4, 8),
            copartitioned: false,
            rp: 4,
            changelog: 8,
        },
    ];

    for row in rows {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_of(None, &[
                    ("orders", 1, row.partitions.0),
                    ("customers", 2, row.partitions.1),
                ]))
                .build(),
        );
        let (coord, _log) = make_coordinator();
        coord.set_metadata_source(source);
        let handle = coord.get_or_create_streams("g");
        let topology = Topology {
            epoch: 1,
            subtopologies: vec![crate::test_support::source_to_repartition(), Subtopology {
                subtopology_id: "1".into(),
                source_topics: vec!["customers".into()],
                repartition_source_topics: vec![TopicInfo {
                    name: "rp".into(),
                    ..Default::default()
                }],
                state_changelog_topics: vec![TopicInfo {
                    name: "store-changelog".into(),
                    ..Default::default()
                }],
                copartition_groups: if row.copartitioned {
                    vec![CopartitionGroup {
                        source_topics: vec![0],
                        repartition_source_topics: vec![0],
                        ..Default::default()
                    }]
                } else {
                    vec![]
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        let result = heartbeat_result(&handle, StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 0,
            rebalance_timeout_ms: 1_000,
            topology: Some(topology),
            ..Default::default()
        })
        .await;

        let spec = |name: &str, partitions| InternalTopicSpec {
            name: name.into(),
            partitions,
            replication_factor: 0,
            configs: std::collections::BTreeMap::new(),
        };
        check!(
            result.creatable_topics
                == vec![spec("rp", row.rp), spec("store-changelog", row.changelog)],
            "{}",
            row.name
        );
        let expected = StreamsGroupHeartbeatResponse {
            member_id: "m1".into(),
            status: Some(vec![Status {
                status_code: status::MISSING_INTERNAL_TOPICS,
                status_detail: "Internal topics are missing: rp, store-changelog".into(),
                ..Default::default()
            }]),
            active_tasks: Some(vec![]),
            standby_tasks: Some(vec![]),
            warmup_tasks: Some(vec![]),
            ..super::response::base_resp(codes::NONE, 2, &undelayed())
        };
        check!(result.response == expected, "{}", row.name);
    }
}

/// Kafka builds the `StreamsGroupHeartbeat` status list on every heartbeat:
/// `STALE_TOPOLOGY` for a member behind the group topology, the topology
/// configuration status, and `SHUTDOWN_APPLICATION` while a shutdown request
/// stands. The list is empty, not null, when nothing holds, and the shutdown
/// request ends when the group becomes empty. Each row runs its heartbeats on
/// a fresh group with a one-partition source topic and compares the whole
/// response of the last one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_heartbeat_status_list_follows_kafka() {
    use {
        crate::{
            coordinator::unified::streams::topology::status, test_support::FakeMetadataSource,
        },
        krabka_protocol::owned::{
            common::streams_group_heartbeat_response::status::Status,
            streams_group_heartbeat_request::{Subtopology, Topology},
        },
        std::collections::HashMap,
    };

    #[derive(Clone, Copy)]
    enum Beat {
        /// A join with this topology epoch.
        Join(&'static str, i32),
        /// A heartbeat at the member epoch, with `shutdown_application`.
        Heartbeat(&'static str, bool),
        /// A leave, with `shutdown_application`.
        Leave(&'static str, bool),
    }
    struct Row {
        name: &'static str,
        sources: &'static [&'static str],
        beats: Vec<Beat>,
        member: &'static str,
        epoch: i32,
        /// The active tasks, when the response sends the task lists.
        active: Option<Vec<i32>>,
        status: Vec<(i8, &'static str)>,
    }
    let rows = [
        Row {
            name: "a ready group sends an empty list",
            sources: &["in"],
            beats: vec![Beat::Join("m1", 1)],
            member: "m1",
            epoch: 2,
            active: Some(vec![0]),
            status: vec![],
        },
        Row {
            name: "two missing source topics give one entry",
            sources: &["b", "a"],
            beats: vec![Beat::Join("m1", 1)],
            member: "m1",
            epoch: 2,
            active: Some(vec![]),
            status: vec![(
                status::MISSING_SOURCE_TOPICS,
                "Source topics a, b are missing.",
            )],
        },
        Row {
            name: "a heartbeat requests the shutdown",
            sources: &["in"],
            beats: vec![Beat::Join("m1", 1), Beat::Heartbeat("m1", true)],
            member: "m1",
            epoch: 2,
            active: None,
            status: vec![(
                status::SHUTDOWN_APPLICATION,
                "Streams group member m1 encountered a fatal error and requested a shutdown for \
                 the entire application.",
            )],
        },
        Row {
            name: "a leave requests the shutdown",
            sources: &["in"],
            beats: vec![
                Beat::Join("m1", 1),
                Beat::Join("m2", 1),
                Beat::Leave("m2", true),
                Beat::Heartbeat("m1", false),
            ],
            member: "m1",
            epoch: 4,
            active: None,
            status: vec![(
                status::SHUTDOWN_APPLICATION,
                "Streams group member m2 encountered a fatal error and requested a shutdown for \
                 the entire application.",
            )],
        },
        Row {
            name: "the shutdown request ends when the group becomes empty",
            sources: &["in"],
            beats: vec![
                Beat::Join("m1", 1),
                Beat::Heartbeat("m1", true),
                Beat::Leave("m1", false),
                Beat::Join("m2", 1),
            ],
            member: "m2",
            epoch: 4,
            active: Some(vec![0]),
            status: vec![],
        },
        Row {
            name: "a member behind the group topology gets STALE_TOPOLOGY",
            sources: &["in"],
            beats: vec![Beat::Join("m1", 2), Beat::Join("m2", 1)],
            member: "m2",
            epoch: 3,
            active: Some(vec![]),
            status: vec![(
                status::STALE_TOPOLOGY,
                "The member's topology epoch 1 is behind the group's topology epoch 2.",
            )],
        },
        Row {
            name: "a member at the group topology does not get STALE_TOPOLOGY",
            sources: &["in"],
            beats: vec![
                Beat::Join("m1", 2),
                Beat::Join("m2", 1),
                Beat::Heartbeat("m1", false),
            ],
            member: "m1",
            epoch: 3,
            active: None,
            status: vec![],
        },
    ];

    for row in rows {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_of(None, &[("in", 1, 1)]))
                .build(),
        );
        let (coord, _log) = make_coordinator();
        coord.set_metadata_source(source);
        let handle = coord.get_or_create_streams("g");
        let mut epochs: HashMap<&str, i32> = HashMap::new();
        let mut last = None;
        for beat in &row.beats {
            let (member, member_epoch, shutdown, topology_epoch) = match *beat {
                Beat::Join(member, topology_epoch) => (member, 0, false, Some(topology_epoch)),
                Beat::Heartbeat(member, shutdown) => (member, epochs[member], shutdown, None),
                Beat::Leave(member, shutdown) => (member, -1, shutdown, None),
            };
            let resp = heartbeat(&handle, StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: member.into(),
                member_epoch,
                rebalance_timeout_ms: 1_000,
                shutdown_application: shutdown,
                topology: topology_epoch.map(|epoch| Topology {
                    epoch,
                    subtopologies: vec![Subtopology {
                        subtopology_id: "0".into(),
                        source_topics: row.sources.iter().map(|s| (*s).to_string()).collect(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await;
            check!(resp.error_code == codes::NONE, "{}", row.name);
            epochs.insert(member, resp.member_epoch);
            last = Some(resp);
        }

        let expected = super::test_support::expected_active_response(
            super::test_support::ActiveResponseSetup {
                member_id: row.member,
                epoch: MemberEpoch(row.epoch),
                status: Some(
                    row.status
                        .iter()
                        .map(|(status_code, detail)| Status {
                            status_code: *status_code,
                            status_detail: (*detail).into(),
                            ..Default::default()
                        })
                        .collect(),
                ),
                active: row.active.map(|partitions| {
                    partitions
                        .into_iter()
                        .map(krabka_ids::PartitionIndex)
                        .collect()
                }),
            },
        );
        check!(last == Some(expected), "{}", row.name);
    }
}

/// The parts of Kafka's `StreamsGroupHeartbeat` response beyond the member
/// epoch and the status: the task lists only on a join or a change, the
/// endpoint information for Interactive Queries, the leave response and the
/// error response. Each row runs its heartbeats on a fresh group with a
/// source topic of the given partition count and compares the whole response
/// of the last one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_heartbeat_response_carries_what_kafka_sends() {
    use {
        crate::test_support::FakeMetadataSource,
        krabka_protocol::owned::{
            common::{
                streams_group_heartbeat_request::endpoint::Endpoint as RequestEndpoint,
                streams_group_heartbeat_response::{
                    endpoint::Endpoint, topic_partition::TopicPartition,
                },
            },
            streams_group_heartbeat_response::EndpointToPartitions,
        },
        std::collections::HashMap,
    };

    #[derive(Clone, Copy)]
    enum Beat {
        /// A join with this user endpoint port.
        Join(&'static str, Option<u16>),
        /// A heartbeat at the member epoch, or at this epoch.
        Heartbeat(&'static str, Option<i32>),
        Leave(&'static str),
    }
    let endpoint = |port: u16, active: &[i32]| EndpointToPartitions {
        user_endpoint: Endpoint {
            host: "localhost".into(),
            port,
            ..Default::default()
        },
        active_partitions: if active.is_empty() {
            vec![]
        } else {
            vec![TopicPartition {
                topic: "in".into(),
                partitions: active.to_vec(),
                ..Default::default()
            }]
        },
        standby_partitions: vec![],
        ..Default::default()
    };
    let accepted = accepted_tasks;
    let rows = [
        (
            "the joining member of a new group gets its endpoint information",
            1,
            1,
            vec![Beat::Join("m1", Some(1))],
            StreamsGroupHeartbeatResponse {
                partitions_by_user_endpoint: Some(vec![endpoint(1, &[0])]),
                ..accepted("m1", 2, Some(&[0]))
            },
        ),
        (
            "two members with endpoints",
            10,
            1,
            vec![Beat::Join("m1", Some(1)), Beat::Join("m2", Some(2))],
            StreamsGroupHeartbeatResponse {
                endpoint_information_epoch: 1,
                partitions_by_user_endpoint: Some(vec![endpoint(1, &[0]), endpoint(2, &[])]),
                ..accepted("m2", 3, Some(&[]))
            },
        ),
        (
            "a member that must revoke a task gets its tasks at its epoch",
            10,
            2,
            vec![
                Beat::Join("m1", None),
                Beat::Join("m2", None),
                Beat::Heartbeat("m1", None),
            ],
            accepted("m1", 2, Some(&[0])),
        ),
        (
            "a member with an endpoint that must revoke a task",
            10,
            2,
            vec![
                Beat::Join("m1", Some(1)),
                Beat::Join("m2", None),
                Beat::Heartbeat("m1", None),
            ],
            StreamsGroupHeartbeatResponse {
                endpoint_information_epoch: 1,
                partitions_by_user_endpoint: Some(vec![endpoint(1, &[0])]),
                ..accepted("m1", 2, Some(&[0]))
            },
        ),
        (
            "a heartbeat with an unchanged assignment",
            10,
            1,
            vec![Beat::Join("m1", None), Beat::Heartbeat("m1", None)],
            accepted("m1", 2, None),
        ),
        (
            "a leave",
            10,
            1,
            vec![Beat::Join("m1", None), Beat::Leave("m1")],
            StreamsGroupHeartbeatResponse {
                member_id: "m1".into(),
                member_epoch: -1,
                status: Some(vec![]),
                ..Default::default()
            },
        ),
        (
            "a full group",
            1,
            1,
            vec![Beat::Join("m1", None), Beat::Join("m2", None)],
            super::response::error_resp(
                codes::GROUP_MAX_SIZE_REACHED,
                Some("The streams group has reached its maximum capacity of 1 members.".into()),
            ),
        ),
        (
            "an unknown member",
            10,
            1,
            vec![Beat::Join("m1", None), Beat::Heartbeat("m9", Some(3))],
            super::response::error_resp(
                codes::UNKNOWN_MEMBER_ID,
                Some("Member m9 is not a member of group g.".into()),
            ),
        ),
    ];

    for (name, max_size, partitions, beats, expected) in rows {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_of(None, &[("in", 1, partitions)]))
                .build(),
        );
        let coord = coordinator_with_log(
            StreamsGroupConfig {
                max_size,
                ..undelayed()
            },
            Arc::new(InMemoryOffsetsLog::default()),
        );
        coord.set_metadata_source(source);
        let handle = coord.get_or_create_streams("g");
        let mut epochs: HashMap<&str, i32> = HashMap::new();
        let mut last = None;
        for beat in beats {
            let (member, member_epoch, port) = match beat {
                Beat::Join(member, port) => (member, 0, port),
                Beat::Heartbeat(member, epoch) => {
                    (member, epoch.unwrap_or_else(|| epochs[member]), None)
                }
                Beat::Leave(member) => (member, -1, None),
            };
            let resp = heartbeat(&handle, StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: member.into(),
                member_epoch,
                rebalance_timeout_ms: 1_000,
                user_endpoint: port.map(|port| RequestEndpoint {
                    host: "localhost".into(),
                    port,
                    ..Default::default()
                }),
                topology: (member_epoch == 0)
                    .then(|| one_subtopology(SingleSubtopologySetup::default())),
                ..Default::default()
            })
            .await;
            epochs.insert(member, resp.member_epoch);
            last = Some(resp);
        }
        check!(last == Some(expected), "{name}");
    }
}

/// KIP-1071 reconciliation through the actor, as Kafka's
/// `CurrentAssignmentBuilder` and `scheduleStreamsGroupRebalanceTimeout` run
/// it. `m1` owns both tasks of a two-partition topic and `m2` joins, so the
/// new target moves task 1 to `m2`. `m1` keeps its member epoch until it stops
/// reporting task 1, and it is fenced when it does not revoke within its
/// rebalance timeout. Each row compares the whole responses of a last
/// heartbeat of `m1` and then of `m2`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_member_keeps_its_epoch_until_it_revokes_and_is_fenced_after_its_timeout() {
    use {
        crate::test_support::FakeMetadataSource,
        krabka_protocol::owned::common::streams_group_heartbeat_request::task_ids::TaskIds as OwnedTaskIds,
    };

    let accepted = accepted_tasks;
    // (name, rebalance timeout of m1, m1 revokes task 1, expected last m1 and
    // m2 responses)
    let rows = [
        (
            "revokes before the timeout",
            600_000,
            true,
            accepted("m1", 3, None),
            accepted("m2", 3, Some(&[1])),
        ),
        (
            "does not revoke, the timeout is not reached",
            600_000,
            false,
            accepted("m1", 2, None),
            accepted("m2", 3, None),
        ),
        (
            "does not revoke within the timeout",
            50,
            false,
            super::response::error_resp(
                codes::UNKNOWN_MEMBER_ID,
                Some("Member m1 is not a member of group g.".into()),
            ),
            accepted("m2", 4, Some(&[0, 1])),
        ),
    ];

    for (name, rebalance_timeout_ms, revokes, expected_m1, expected_m2) in rows {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_of(None, &[("in", 1, 2)]))
                .build(),
        );
        let (coord, _log) = make_coordinator();
        coord.set_metadata_source(source);
        let handle = coord.get_or_create_streams("g");
        let request =
            |member_id: &str, member_epoch, owned: Option<&[i32]>| StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: member_id.into(),
                member_epoch,
                rebalance_timeout_ms,
                topology: (member_epoch == 0)
                    .then(|| one_subtopology(SingleSubtopologySetup::default())),
                active_tasks: owned.map(|partitions| {
                    vec![OwnedTaskIds {
                        subtopology_id: "0".into(),
                        partitions: partitions.to_vec(),
                        ..Default::default()
                    }]
                }),
                standby_tasks: owned.map(|_| vec![]),
                warmup_tasks: owned.map(|_| vec![]),
                ..Default::default()
            };

        let m1 = heartbeat(&handle, request("m1", 0, Some(&[]))).await;
        check!(m1 == accepted("m1", 2, Some(&[0, 1])), "{name}");
        let m2 = heartbeat(&handle, request("m2", 0, Some(&[]))).await;
        check!(m2 == accepted("m2", 3, Some(&[])), "{name}");
        let m1 = heartbeat(&handle, request("m1", 2, Some(&[0, 1]))).await;
        check!(m1 == accepted("m1", 2, Some(&[0])), "{name}");
        if revokes {
            let m1 = heartbeat(&handle, request("m1", 2, Some(&[0]))).await;
            check!(m1 == accepted("m1", 3, None), "{name}");
        }
        // A row whose member never revokes within its rebalance timeout waits
        // for the fence, which the actor runs at the deadline.
        if rebalance_timeout_ms < 1_000 {
            for _ in 0..100 {
                if !describe_member_ids(&handle)
                    .await
                    .contains(&"m1".to_string())
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }

        let owned_by_m1: &[i32] = if revokes { &[0] } else { &[0, 1] };
        let m1_epoch = if revokes { 3 } else { 2 };
        let last_m1 = heartbeat(&handle, request("m1", m1_epoch, Some(owned_by_m1))).await;
        let last_m2 = heartbeat(&handle, request("m2", 3, Some(&[]))).await;
        check!((last_m1, last_m2) == (expected_m1, expected_m2), "{name}");
    }
}

/// Kafka refuses, inside the coordinator, a join whose topology differs from
/// the group topology (`maybeUpdateTopology`) and a heartbeat that owns a task
/// the ready topology does not have (`throwIfRequestContainsInvalidTasks`).
/// Each row compares the whole response and the members of the group
/// afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topology_update_or_an_invalid_owned_task_is_refused() {
    use {
        crate::test_support::FakeMetadataSource,
        krabka_protocol::owned::common::streams_group_heartbeat_request::task_ids::TaskIds,
    };

    let join = |member_id: &str, epoch, source: &str| {
        let mut topology = one_subtopology(SingleSubtopologySetup::default());
        topology.epoch = epoch;
        topology.subtopologies[0].source_topics = vec![source.into()];
        streams_join_request(topology, StreamsJoinSetup {
            member_id,
            ..Default::default()
        })
    };
    let owning = |subtopology: &str, partition| StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: "m1".into(),
        member_epoch: 2,
        active_tasks: Some(vec![TaskIds {
            subtopology_id: subtopology.into(),
            partitions: vec![partition],
            ..Default::default()
        }]),
        standby_tasks: Some(vec![]),
        warmup_tasks: Some(vec![]),
        ..Default::default()
    };
    let rows = [
        (
            "a join with another topology at the same epoch",
            join("m2", 1, "other"),
            "Topology updates are not supported yet.",
        ),
        (
            "a join with the same subtopologies at a higher epoch",
            join("m2", 2, "in"),
            "Topology updates are not supported yet.",
        ),
        (
            "an owned task of an unknown subtopology",
            owning("9", 0),
            "Subtopology 9 does not exist in the topology.",
        ),
        (
            "an owned task out of range",
            owning("0", 5),
            "Task 5 for subtopology 0 is invalid. Number of tasks for this subtopology: 1",
        ),
    ];

    for (name, request, message) in rows {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_of(None, &[("in", 1, 1), ("other", 2, 1)]))
                .build(),
        );
        let (coord, _log) = make_coordinator();
        coord.set_metadata_source(source);
        let handle = coord.get_or_create_streams("g");
        check!(
            heartbeat(&handle, join("m1", 1, "in")).await.error_code == codes::NONE,
            "{name}"
        );

        let resp = heartbeat(&handle, request).await;

        check!(
            resp == super::response::error_resp(codes::INVALID_REQUEST, Some(message.into())),
            "{name}"
        );
        let members: Vec<String> = describe(&handle)
            .await
            .members
            .into_iter()
            .map(|m| m.member_id)
            .collect();
        check!(members == vec!["m1".to_string()], "{name}");
    }
}

/// KIP-1071 static membership, as Kafka's
/// `getOrMaybeCreateStaticStreamsGroupMember` and
/// `streamsGroupStaticMemberGroupLeave` run it. Member `m1` holds instance id
/// `i1` in a group with a one-partition topic. Each row sends its heartbeats
/// and compares the whole last response and the members afterwards, as
/// `(member id, member epoch, active tasks)`. Static membership is Kafka
/// trunk's: 4.3.1 refuses an instance id before the group sees the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_static_member_follows_kafka_static_membership() {
    use crate::test_support::FakeMetadataSource;

    let config = StreamsGroupConfig {
        max_size: 2,
        unstable_api_versions: UnstableApiVersions::Enabled,
        ..undelayed()
    };
    let join = |member_id: &str, instance_id: &str| StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch: 0,
        instance_id: Some(instance_id.into()),
        rebalance_timeout_ms: 1_000,
        active_tasks: Some(vec![]),
        standby_tasks: Some(vec![]),
        warmup_tasks: Some(vec![]),
        topology: Some(one_subtopology(SingleSubtopologySetup::default())),
        ..Default::default()
    };
    let beat = |member_id: &str, member_epoch, instance_id: &str| StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        instance_id: Some(instance_id.into()),
        ..Default::default()
    };
    let assigned = |member_id: &str, member_epoch| StreamsGroupHeartbeatResponse {
        member_id: member_id.into(),
        status: Some(vec![]),
        active_tasks: Some(vec![
            krabka_protocol::owned::common::streams_group_heartbeat_response::task_ids::TaskIds {
                subtopology_id: "0".into(),
                partitions: vec![0],
                ..Default::default()
            },
        ]),
        standby_tasks: Some(vec![]),
        warmup_tasks: Some(vec![]),
        ..super::response::base_resp(codes::NONE, member_epoch, &config)
    };
    let rows = [
        (
            "a static member leaves for a while and keeps its tasks",
            vec![beat("m1", -2, "i1")],
            StreamsGroupHeartbeatResponse {
                member_id: "m1".into(),
                member_epoch: -2,
                status: Some(vec![]),
                ..Default::default()
            },
            vec![("m1".to_string(), -2, vec![0])],
        ),
        (
            "a new member replaces the released static member",
            vec![beat("m1", -2, "i1"), join("m2", "i1")],
            assigned("m2", 2),
            vec![("m2".to_string(), 2, vec![0])],
        ),
        (
            "a join with an instance id that a member still holds",
            vec![join("m2", "i1")],
            super::response::error_resp(
                codes::UNRELEASED_INSTANCE_ID,
                Some(
                    "Static member m2 with instance id i1 cannot join the group because the \
                     instance id is owned by m1 member."
                        .into(),
                ),
            ),
            vec![("m1".to_string(), 2, vec![0])],
        ),
        (
            "a heartbeat with the instance id of another member",
            vec![beat("m2", 5, "i1")],
            super::response::error_resp(
                codes::FENCED_INSTANCE_ID,
                Some("Static member m2 with instance id i1 was fenced by member m1.".into()),
            ),
            vec![("m1".to_string(), 2, vec![0])],
        ),
        (
            "a heartbeat with an unknown instance id",
            vec![beat("m1", 2, "i9")],
            super::response::error_resp(
                codes::UNKNOWN_MEMBER_ID,
                Some("Instance id i9 is unknown.".into()),
            ),
            vec![("m1".to_string(), 2, vec![0])],
        ),
        (
            "a released static member does not count against a full group",
            vec![join("m3", "i3"), beat("m1", -2, "i1"), join("m2", "i1")],
            assigned("m2", 3),
            vec![
                ("m2".to_string(), 3, vec![0]),
                ("m3".to_string(), 3, vec![]),
            ],
        ),
        (
            "a replacement takes over a member id that another member holds",
            vec![join("m2", "i2"), beat("m1", -2, "i1"), join("m2", "i1")],
            assigned("m2", 4),
            vec![("m2".to_string(), 4, vec![0])],
        ),
        (
            "a static member leaves for good",
            vec![beat("m1", -1, "i1")],
            StreamsGroupHeartbeatResponse {
                member_id: "m1".into(),
                member_epoch: -1,
                status: Some(vec![]),
                ..Default::default()
            },
            vec![],
        ),
    ];

    for (name, beats, expected, members) in rows {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_of(None, &[("in", 1, 1)]))
                .build(),
        );
        let coord = coordinator_with_log(config.clone(), Arc::new(InMemoryOffsetsLog::default()));
        coord.set_metadata_source(source);
        let handle = coord.get_or_create_streams("g");
        check!(
            heartbeat(&handle, join("m1", "i1")).await == assigned("m1", 2),
            "{name}"
        );

        let mut last = None;
        for request in beats {
            last = Some(heartbeat(&handle, request).await);
        }

        check!(last == Some(expected), "{name}");
        let mut described: Vec<(String, i32, Vec<i32>)> = describe(&handle)
            .await
            .members
            .into_iter()
            .map(|member| {
                let active = member.active.get("0").cloned().unwrap_or_default();
                (member.member_id, member.member_epoch, active)
            })
            .collect();
        described.sort();
        check!(described == members, "{name}");
    }
}

/// Kafka's `StreamsGroup.validateOffsetCommit`. Each row is a group (with
/// member `m1` at epoch 5, or empty), a commit and the expected answer.
#[test]
fn validate_offset_commit_follows_kafka_streams_group() {
    let offset = |api_version| CommitFence::Offset { api_version };
    let txn = CommitFence::Transactional;
    let mut group = StreamsGroupState::new("g");
    let member = epoch_five_member();
    group.members.insert("m1".into(), member);
    let empty = StreamsGroupState::new("g");

    // (row, group, member id, member epoch, fence, expected)
    let rows = [
        (
            "admin commit on an empty group",
            &empty,
            "",
            -1,
            offset(9),
            Ok(()),
        ),
        (
            "admin commit on a live group",
            &group,
            "",
            -1,
            offset(9),
            Err(codes::UNKNOWN_MEMBER_ID),
        ),
        (
            "empty member id at a real epoch",
            &group,
            "",
            5,
            offset(9),
            Err(codes::UNKNOWN_MEMBER_ID),
        ),
        (
            "transactional commit with no member",
            &group,
            "",
            -1,
            txn,
            Ok(()),
        ),
        (
            "unknown member",
            &group,
            "m2",
            5,
            offset(9),
            Err(codes::UNKNOWN_MEMBER_ID),
        ),
        (
            "OffsetCommit before v9",
            &group,
            "m1",
            5,
            offset(8),
            Err(codes::UNSUPPORTED_VERSION),
        ),
        (
            "TxnOffsetCommit has no version floor",
            &group,
            "m1",
            5,
            txn,
            Ok(()),
        ),
        ("current epoch", &group, "m1", 5, offset(9), Ok(())),
        (
            "newer epoch",
            &group,
            "m1",
            6,
            offset(9),
            Err(codes::STALE_MEMBER_EPOCH),
        ),
        (
            "older epoch on a group without a topology",
            &group,
            "m1",
            4,
            offset(9),
            Err(codes::STALE_MEMBER_EPOCH),
        ),
    ];
    let partitions = [("in".to_string(), 0)];
    for (row, state, member_id, member_epoch, fence, expected) in rows {
        check!(
            validate_offset_commit(state, None, member_id, member_epoch, fence, &partitions)
                == expected,
            "{row}"
        );
    }
}

/// KIP-1251: Kafka 4.3.1's `StreamsGroup.createAssignmentEpochValidator`.
/// Member `m1` is at epoch 5. It holds active tasks 0 and 1 of subtopology
/// `0`, assigned at epochs 3 and 5, task 2 pending revocation from epoch 4,
/// and task 3 with no recorded epoch. Subtopology `0` reads `in` and the
/// repartition topic `rep`; subtopology `1` reads `other`, where `m1` holds
/// nothing. Each row commits at epoch 4.
#[test]
fn older_epoch_commit_checks_each_tasks_assignment_epoch() {
    use crate::coordinator::unified::streams::persistence::{StoredSubtopology, StoredTopicInfo};

    type Row = (
        &'static str,
        &'static [(&'static str, i32)],
        Result<(), i16>,
    );

    let subtopology = |id: &str, source: &str, repartition: &[&str]| StoredSubtopology {
        subtopology_id: id.into(),
        source_topics: vec![source.into()],
        source_topic_regex: vec![],
        repartition_sink_topics: vec![],
        state_changelog_topics: vec![],
        repartition_source_topics: repartition
            .iter()
            .map(|name| StoredTopicInfo {
                name: (*name).into(),
                partitions: 4,
                replication_factor: -1,
                topic_configs: vec![],
            })
            .collect(),
        copartition_groups: vec![],
    };
    let topology = StreamsGroupTopologyValue {
        epoch: 1,
        subtopologies: vec![
            subtopology("0", "in", &["rep"]),
            subtopology("1", "other", &[]),
        ],
    };
    let mut group = StreamsGroupState::new("g");
    let mut member = epoch_five_member();
    member.active = maplit::btreemap! {"0".to_string() => vec![0, 1, 3]};
    member.active_pending_revocation = maplit::btreemap! {"0".to_string() => vec![2]};
    member.active_epochs = maplit::btreemap! {
        ("0".to_string(), 0) => 3,
        ("0".to_string(), 1) => 5,
        ("0".to_string(), 2) => 4,
    };
    group.members.insert("m1".into(), member);

    // (row, committed (topic, partition)s, expected)
    let rows: [Row; 10] = [
        ("no partitions", &[], Ok(())),
        ("assigned before the epoch", &[("in", 0)], Ok(())),
        (
            "assigned after the epoch",
            &[("in", 1)],
            Err(codes::STALE_MEMBER_EPOCH),
        ),
        ("pending revocation at the epoch", &[("in", 2)], Ok(())),
        (
            "no recorded epoch reads the member epoch",
            &[("in", 3)],
            Err(codes::STALE_MEMBER_EPOCH),
        ),
        ("a repartition source topic", &[("rep", 0)], Ok(())),
        (
            "one refused partition refuses the commit",
            &[("in", 0), ("in", 1)],
            Err(codes::STALE_MEMBER_EPOCH),
        ),
        (
            "a task the member does not hold",
            &[("in", 4)],
            Err(codes::STALE_MEMBER_EPOCH),
        ),
        (
            "another subtopology's task",
            &[("other", 0)],
            Err(codes::STALE_MEMBER_EPOCH),
        ),
        (
            "a topic outside the topology",
            &[("unknown", 0)],
            Err(codes::STALE_MEMBER_EPOCH),
        ),
    ];
    for (row, committed, expected) in rows {
        let partitions: Vec<(String, i32)> = committed
            .iter()
            .map(|(topic, partition)| ((*topic).to_string(), *partition))
            .collect();
        for fence in [
            CommitFence::Offset { api_version: 9 },
            CommitFence::Transactional,
        ] {
            check!(
                validate_offset_commit(&group, Some(&topology), "m1", 4, fence, &partitions)
                    == expected,
                "{row} {fence:?}"
            );
        }
    }
}

/// #972: Kafka trunk's `MISSING_CLIENT_TAGS` (`streamsGroupHeartbeat`, after
/// the shutdown status). A heartbeat at version 1 or above from a member that
/// has not sent every tag key of `streams.rack.aware.assignment.tags` carries
/// the status, naming the missing keys in the configured order; version 0
/// never does, and neither does a leave. A heartbeat that sends no client tags
/// keeps the member's earlier ones. Each row runs its heartbeats on a fresh
/// ready group and compares the whole status list of the last response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_member_missing_a_rack_aware_tag_gets_missing_client_tags_at_version_1() {
    use {
        crate::{
            coordinator::unified::streams::topology::status, test_support::FakeMetadataSource,
        },
        krabka_protocol::owned::common::{
            streams_group_heartbeat_request::key_value::KeyValue,
            streams_group_heartbeat_response::status::Status,
        },
    };

    /// One heartbeat of member `m1`: its request version, whether it joins,
    /// heartbeats at its epoch or leaves, and the client tags it sends.
    #[derive(Clone, Copy)]
    enum Beat {
        Join(i16, Option<&'static [&'static str]>),
        Heartbeat(i16, Option<&'static [&'static str]>),
        Leave(i16),
    }
    struct Row {
        name: &'static str,
        tags: &'static [&'static str],
        beats: &'static [Beat],
        status: Option<&'static str>,
    }
    let rows = [
        Row {
            name: "version 0 never gets the status",
            tags: &["zone"],
            beats: &[Beat::Join(0, None)],
            status: None,
        },
        Row {
            name: "version 1 names the missing tag",
            tags: &["zone"],
            beats: &[Beat::Join(1, None)],
            status: Some("[zone]"),
        },
        Row {
            name: "the missing tags follow the configured order",
            tags: &["zone", "cell", "rack"],
            beats: &[Beat::Join(1, Some(&["cell"]))],
            status: Some("[zone, rack]"),
        },
        Row {
            name: "a member that sends every tag gets no status",
            tags: &["zone", "rack"],
            beats: &[Beat::Join(1, Some(&["rack", "zone", "extra"]))],
            status: None,
        },
        Row {
            name: "no configured tag gives no status",
            tags: &[],
            beats: &[Beat::Join(1, None)],
            status: None,
        },
        Row {
            name: "a heartbeat without tags keeps the member's tags",
            tags: &["zone"],
            beats: &[Beat::Join(1, Some(&["zone"])), Beat::Heartbeat(1, None)],
            status: None,
        },
        Row {
            name: "a heartbeat without tags keeps the member missing them",
            tags: &["zone"],
            beats: &[Beat::Join(0, None), Beat::Heartbeat(1, None)],
            status: Some("[zone]"),
        },
        Row {
            name: "a heartbeat that sends the tag clears the status",
            tags: &["zone"],
            beats: &[Beat::Join(1, None), Beat::Heartbeat(1, Some(&["zone"]))],
            status: None,
        },
        Row {
            name: "a leave carries no status",
            tags: &["zone"],
            beats: &[Beat::Join(1, None), Beat::Leave(1)],
            status: None,
        },
    ];

    for row in rows {
        let log = Arc::new(InMemoryOffsetsLog::default());
        let coord = coordinator_with_log(
            StreamsGroupConfig {
                rack_aware_assignment_tags: row.tags.iter().map(|tag| (*tag).to_owned()).collect(),
                ..undelayed()
            },
            log,
        );
        coord.set_metadata_source(Arc::new(
            FakeMetadataSource::builder()
                .image(image_of(None, &[("in", 1, 1)]))
                .build(),
        ));
        let handle = coord.get_or_create_streams("g");
        let mut epoch = 0;
        let mut last = None;
        for beat in row.beats {
            let (version, member_epoch, tags, topology) = match *beat {
                Beat::Join(version, tags) => (
                    version,
                    0,
                    tags,
                    Some(one_subtopology(SingleSubtopologySetup::default())),
                ),
                Beat::Heartbeat(version, tags) => (version, epoch, tags, None),
                Beat::Leave(version) => (version, -1, None, None),
            };
            let resp = heartbeat_result_at(
                &handle,
                StreamsGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "m1".into(),
                    member_epoch,
                    rebalance_timeout_ms: 1_000,
                    client_tags: tags.map(|keys| {
                        keys.iter()
                            .map(|key| KeyValue {
                                key: (*key).into(),
                                value: "v".into(),
                                ..Default::default()
                            })
                            .collect()
                    }),
                    topology,
                    ..Default::default()
                },
                version,
            )
            .await
            .response;
            check!(resp.error_code == codes::NONE, "{}: {resp:?}", row.name);
            epoch = resp.member_epoch;
            last = Some(resp);
        }
        let expected: Vec<Status> = row
            .status
            .map(|missing| Status {
                status_code: status::MISSING_CLIENT_TAGS,
                status_detail: format!(
                    "Missing required client tags for rack-aware standby assignment: {missing}. \
                     Configure them via 'client.tag.<tagKey>' in your Streams config."
                ),
                ..Default::default()
            })
            .into_iter()
            .collect();
        check!(
            last.and_then(|resp| resp.status) == Some(expected),
            "{}",
            row.name
        );
    }
}

/// #972: an accepted heartbeat carries the group's `acceptable.recovery.lag`
/// in version 1's `int64` field, as Kafka trunk sets it, and 0 in version 0's
/// `int32` field, which Kafka never sets. On the wire version 0 carries only
/// the 0 and version 1 only the lag, so a decoder reads the other field's
/// default.
#[test]
fn heartbeat_response_carries_the_recovery_lag_at_version_1_only() {
    use krabka_protocol::{Decode as _, Encode as _};

    let config = StreamsGroupConfig {
        acceptable_recovery_lag: 10_000,
        ..undelayed()
    };
    let response = response::base_resp(codes::NONE, 3, &config);
    let expected = StreamsGroupHeartbeatResponse {
        error_code: codes::NONE,
        member_epoch: 3,
        heartbeat_interval_ms: 5_000,
        acceptable_recovery_lag_legacy: 0,
        acceptable_recovery_lag: 10_000,
        // Kafka 4.3.1 never sets the field.
        task_offset_interval_ms: 0,
        ..Default::default()
    };
    assert!(response == expected);
    for (version, legacy, lag) in [(0, 0, -1), (1, 0, 10_000)] {
        let mut bytes = bytes::BytesMut::new();
        response.encode(&mut bytes, version).expect("encode");
        let decoded =
            StreamsGroupHeartbeatResponse::decode(&mut &bytes[..], version).expect("decode");
        assert!(
            (
                decoded.acceptable_recovery_lag_legacy,
                decoded.acceptable_recovery_lag
            ) == (legacy, lag),
            "version {version}"
        );
    }
}

/// Kafka 4.3.1 never sets `TaskOffsetIntervalMs`, so a heartbeat response
/// carries 0 on the wire. Kafka trunk sets it from
/// `streams.task.offset.interval.ms`, whose default is one minute, and the
/// group config answers the same only while `unstable.api.versions.enable` is
/// on.
#[test]
fn the_task_offset_interval_of_a_heartbeat_response_is_trunks_alone() {
    use crate::api_catalog::UnstableApiVersions::{Disabled, Enabled};

    for (unstable, expected) in [(Disabled, 0), (Enabled, 60_000)] {
        let config = StreamsGroupConfig {
            unstable_api_versions: unstable,
            ..undelayed()
        };

        let response = response::base_resp(codes::NONE, 3, &config);

        check!(response.task_offset_interval_ms == expected, "{unstable:?}");
    }
}

/// `partitionsByUserEndpoint` follows the `EndpointToPartitionsManager` of the
/// release. Kafka 4.3.1 lists the standby tasks alone, cuts a task's
/// partitions to the partition count of a topic that has fewer partitions than
/// the task has (and does nothing else), and keeps an entry that is left
/// empty. Kafka trunk lists standby and warmup tasks together, leaves out the
/// task partitions a topic does not have, and drops an entry with no partition.
#[test]
fn endpoint_partitions_follow_the_endpoint_to_partitions_manager_of_each_release() {
    use {
        crate::{
            api_catalog::UnstableApiVersions::{Disabled, Enabled},
            coordinator::unified::streams::{
                state::{StreamsGroupState, StreamsMemberState},
                topology::ConfiguredSubtopology,
            },
        },
        krabka_protocol::owned::{
            common::streams_group_heartbeat_response::{
                endpoint::Endpoint, topic_partition::TopicPartition,
            },
            streams_group_heartbeat_response::EndpointToPartitions,
        },
        std::collections::{BTreeMap, BTreeSet},
    };

    // The topic `in` has two partitions.
    let image = image_of(None, &[("in", 1, 2)]);
    let subtopologies = BTreeMap::from([("0".to_string(), ConfiguredSubtopology {
        number_of_tasks: 4,
        source_topics: BTreeSet::from(["in".to_string()]),
        repartition_source_topics: BTreeMap::new(),
        repartition_sink_topics: BTreeSet::new(),
        state_changelog_topics: BTreeMap::new(),
    })]);
    let entry = |active: &[i32], standby: &[i32]| {
        let partitions = |partitions: &[i32]| {
            vec![TopicPartition {
                topic: "in".into(),
                partitions: partitions.to_vec(),
                ..Default::default()
            }]
        };
        EndpointToPartitions {
            user_endpoint: Endpoint {
                host: "localhost".into(),
                port: 1,
                ..Default::default()
            },
            active_partitions: if active.is_empty() {
                vec![]
            } else {
                partitions(active)
            },
            standby_partitions: if standby.is_empty() {
                vec![]
            } else {
                partitions(standby)
            },
            ..Default::default()
        }
    };
    // (name, active, standby and warmup tasks of subtopology 0, 4.3.1, trunk)
    let rows = [
        (
            "standby tasks over the partition count are cut",
            vec![0, 1],
            (vec![1, 2, 3], vec![0]),
            entry(&[0, 1], &[1, 2]),
            entry(&[0, 1], &[0, 1]),
        ),
        (
            "a warmup task is a standby task for trunk only",
            vec![0],
            (vec![], vec![1]),
            entry(&[0], &[]),
            entry(&[0], &[1]),
        ),
        (
            "a partition the topic lacks is kept below the count and filtered by trunk",
            vec![5],
            (vec![], vec![]),
            entry(&[5], &[]),
            entry(&[], &[]),
        ),
    ];
    for (name, active, (standby, warmup), released, trunk) in rows {
        let mut member = StreamsMemberState::joining("m1", "c", "h");
        member.user_endpoint = Some(("localhost".into(), 1));
        let tasks = |partitions: Vec<i32>| {
            if partitions.is_empty() {
                BTreeMap::new()
            } else {
                BTreeMap::from([("0".to_string(), partitions)])
            }
        };
        member.active = tasks(active);
        member.standby = tasks(standby);
        member.warmup = tasks(warmup);
        let mut state = StreamsGroupState::new("g");
        state.members.insert("m1".into(), member);

        for (unstable, expected) in [(Disabled, released), (Enabled, trunk)] {
            let listed = response::endpoint_to_partitions(
                &state,
                "m1",
                Some(&subtopologies),
                Some(&image),
                unstable,
            );

            check!(listed == vec![expected], "{name}: {unstable:?}");
        }
    }
}

/// Kafka's initial rebalance delay and assignment interval
/// (`maybeUpdateStreamsTargetAssignment`, `computeDelayedTargetAssignment`).
/// Each row configures the delays, then runs timed heartbeats on a paused
/// clock and compares each answer's error code, member epoch, active tasks and
/// status list. With no metadata source every target is empty, so a member's
/// epoch shows whether the assignment of its group epoch was computed. A
/// member that joins while the initial delay holds the assignment back
/// reconciles to Kafka 4.3's initial target assignment epoch 1 with an empty
/// assignment, so its next heartbeat is not a join.
#[tokio::test(start_paused = true)]
async fn assignment_waits_for_the_initial_delay_and_the_interval() {
    use {
        crate::coordinator::unified::streams::{
            actor::reconciliation::{ASSIGNMENT_INTERVAL_DETAIL, INITIAL_DELAY_DETAIL},
            topology::status::ASSIGNMENT_DELAYED,
        },
        std::time::Duration,
    };

    // (member id, member epoch sent, clock advance before the heartbeat,
    // expected member epoch, expected ASSIGNMENT_DELAYED detail)
    type Step = (&'static str, i32, u64, i32, Option<&'static str>);
    let rows: [(&str, u64, u64, Vec<Step>); 3] = [
        (
            // Three members join within one second: one assignment, after
            // the delay, for the group epoch of the third join.
            "initial delay coalesces the joins",
            3_000,
            0,
            vec![
                ("m1", 0, 0, 1, Some(INITIAL_DELAY_DETAIL)),
                ("m2", 0, 500, 1, Some(INITIAL_DELAY_DETAIL)),
                ("m3", 0, 500, 1, Some(INITIAL_DELAY_DETAIL)),
                ("m1", 1, 2_100, 4, None),
                ("m2", 1, 0, 4, None),
            ],
        ),
        (
            // A change 200 ms after an assignment waits for the interval.
            "assignment interval defers a change",
            0,
            1_000,
            vec![
                ("m1", 0, 0, 2, None),
                ("m2", 0, 200, 2, Some(ASSIGNMENT_INTERVAL_DETAIL)),
                ("m2", 2, 900, 3, None),
            ],
        ),
        ("no delay assigns at once", 0, 0, vec![
            ("m1", 0, 0, 2, None),
            ("m2", 0, 200, 3, None),
        ]),
    ];

    for (name, delay_ms, interval_ms, steps) in rows {
        let coord = coordinator_with_log(
            StreamsGroupConfig {
                initial_rebalance_delay: Duration::from_millis(delay_ms),
                assignment_interval: Duration::from_millis(interval_ms),
                ..StreamsGroupConfig::default()
            },
            Arc::new(InMemoryOffsetsLog::default()),
        );
        let handle = coord.get_or_create_streams("g");
        for (member_id, member_epoch, advance_ms, want_epoch, want_delay) in steps {
            tokio::time::sleep(Duration::from_millis(advance_ms)).await;
            let resp = heartbeat(&handle, StreamsGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: member_id.into(),
                member_epoch,
                rebalance_timeout_ms: 1_000,
                ..Default::default()
            })
            .await;
            let delayed: Vec<(i8, String)> = resp
                .status
                .unwrap_or_default()
                .into_iter()
                .filter(|status| status.status_code == ASSIGNMENT_DELAYED)
                .map(|status| (status.status_code, status.status_detail))
                .collect();
            let want: Vec<(i8, String)> = want_delay
                .map(|detail| (ASSIGNMENT_DELAYED, detail.to_owned()))
                .into_iter()
                .collect();
            // Only a join gets the task lists, since every target is empty.
            let want_active = (member_epoch == 0).then(Vec::new);
            check!(
                (
                    resp.error_code,
                    resp.member_epoch,
                    resp.active_tasks,
                    delayed
                ) == (codes::NONE, want_epoch, want_active, want),
                "{name}: {member_id} after {advance_ms} ms"
            );
        }
    }
}

fn accepted_tasks(
    member_id: &str,
    member_epoch: i32,
    tasks: Option<&[i32]>,
) -> StreamsGroupHeartbeatResponse {
    let config = undelayed();
    StreamsGroupHeartbeatResponse {
        member_id: member_id.into(),
        status: Some(vec![]),
        active_tasks: tasks.map(|partitions| response_tasks(partitions.to_vec())),
        standby_tasks: tasks.map(|_| vec![]),
        warmup_tasks: tasks.map(|_| vec![]),
        ..super::response::base_resp(codes::NONE, member_epoch, &config)
    }
}

/// KIP-1263: a streams group replays the `AssignmentTimestamp` of its target
/// assignment metadata record, and Kafka's `canComputeNextTargetAssignment`
/// runs the assignment interval from it. A stored time inside the interval
/// holds the next assignment back, and an unknown one (0) or an elapsed
/// interval lets it run, which writes the time it finished.
#[tokio::test(start_paused = true)]
async fn the_replayed_assignment_timestamp_holds_the_interval() {
    use {
        crate::coordinator::unified::{
            StreamsGroupSeed,
            streams::{
                actor::reconciliation::ASSIGNMENT_INTERVAL_DETAIL,
                topology::status::ASSIGNMENT_DELAYED,
            },
            wall_clock_ms,
        },
        std::time::Duration,
    };

    // (case, milliseconds before now of the stored timestamp, or `None` for
    // 0, the expected (member epoch, ASSIGNMENT_DELAYED detail, whether the
    // group wrote a new timestamp))
    let rows = [
        ("no stored time", None, (3, None, true)),
        (
            "an assignment 200 ms ago",
            Some(200),
            (2, Some(ASSIGNMENT_INTERVAL_DETAIL.to_owned()), false),
        ),
        ("an assignment 1 s ago", Some(1_000), (3, None, true)),
    ];
    for (case, ago, expected) in rows {
        let coord = coordinator_with_log(
            StreamsGroupConfig {
                initial_rebalance_delay: Duration::ZERO,
                assignment_interval: Duration::from_secs(1),
                ..StreamsGroupConfig::default()
            },
            Arc::new(InMemoryOffsetsLog::default()),
        );
        let handle = coord.get_or_create_streams("g");
        let stored = ago.map_or(0, |ago| wall_clock_ms() - ago);
        handle
            .tx
            .send(StreamsGroupActorMessage::Seed(StreamsGroupSeed {
                group_epoch: 2,
                assignment_epoch: 2,
                assignment_timestamp_ms: stored,
                ..StreamsGroupSeed::default()
            }))
            .await
            .unwrap();
        let before = wall_clock_ms();
        let resp = heartbeat(&handle, StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 0,
            rebalance_timeout_ms: 1_000,
            ..Default::default()
        })
        .await;
        let after = wall_clock_ms();
        let delayed = resp
            .status
            .unwrap_or_default()
            .into_iter()
            .find(|status| status.status_code == ASSIGNMENT_DELAYED)
            .map(|status| status.status_detail);
        let written = coord
            .cached_streams_seed("g")
            .unwrap()
            .assignment_timestamp_ms;
        // The paused clock reads the same millisecond throughout, give or
        // take the rounding of the real clocks underneath it.
        let fresh = (before - 1..=after + 1).contains(&written);
        check!((resp.member_epoch, delayed, fresh) == expected, "{case}");
        check!(fresh || written == stored, "{case}");
    }
}

/// Kafka's `streamsGroupHeartbeat` bumps the epoch of a group whose members
/// and topology did not change when the topology epoch that it validates, or
/// its assignment configuration, differs from what the group's last
/// `StreamsGroupMetadataValue` recorded, and the bump records the new
/// values. With no metadata source no topology is ready, so the group
/// validates -1, and the default configuration is `num.standby.replicas=0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_changed_validation_or_assignment_config_bumps_the_epoch() {
    use crate::coordinator::unified::{
        StreamsGroupSeed,
        streams::persistence::{
            StreamsGroupCurrentMemberAssignmentValue, StreamsGroupMemberMetadataValue,
            StreamsMemberWireState,
        },
    };

    let configs = |standby: &str| {
        maplit::btreemap! {"num.standby.replicas".to_string() => standby.to_string()}
    };
    // The record that the bump writes: the epoch after the loaded 2, the
    // validated topology epoch, and the configuration.
    let bump = Some((3, -1, configs("0")));
    // (case, the stored validated topology epoch, the stored configuration,
    // what a steady heartbeat writes)
    let rows = [
        ("as recorded", -1, configs("0"), None),
        ("another configuration", -1, configs("1"), bump.clone()),
        (
            "a null configuration list",
            -1,
            BTreeMap::new(),
            bump.clone(),
        ),
        (
            "a topology validated before the load",
            0,
            configs("0"),
            bump,
        ),
    ];
    for (case, validated, last_configs, expected) in rows {
        let (coord, _log) = make_coordinator();
        let handle = coord.get_or_create_streams("g");
        handle
            .tx
            .send(StreamsGroupActorMessage::Seed(StreamsGroupSeed {
                group_epoch: 2,
                validated_topology_epoch: validated,
                last_assignment_configs: last_configs,
                assignment_epoch: 2,
                members: [("m1".to_owned(), StreamsGroupMemberMetadataValue {
                    instance_id: None,
                    rack_id: None,
                    client_id: "client".into(),
                    client_host: "/127.0.0.1".into(),
                    process_id: "p1".into(),
                    user_endpoint: None,
                    client_tags: vec![],
                    rebalance_timeout_ms: 60_000,
                    topology_epoch: 0,
                })]
                .into(),
                current_per_member: [("m1".to_owned(), StreamsGroupCurrentMemberAssignmentValue {
                    member_epoch: 2,
                    previous_member_epoch: 1,
                    state: StreamsMemberWireState::Stable,
                    ..StreamsGroupCurrentMemberAssignmentValue::default()
                })]
                .into(),
                ..StreamsGroupSeed::default()
            }))
            .await
            .unwrap();
        heartbeat(&handle, StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 2,
            ..Default::default()
        })
        .await;
        let written = coord.cached_streams_seed("g").map(|seed| {
            (
                seed.group_epoch,
                seed.validated_topology_epoch,
                seed.last_assignment_configs,
            )
        });
        check!(written == expected, "{case}");
    }
}

/// The bump of a group whose topology the metadata holds in a valid
/// configuration records the topology epoch as `ValidatedTopologyEpoch`,
/// and every bump records `LastAssignmentConfigs`, as Kafka's
/// `newStreamsGroupMetadataRecord` writes them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bump_records_the_validated_topology_epoch() {
    use crate::test_support::FakeMetadataSource;

    // (case, the topics in the image, the recorded validated topology epoch)
    let rows = [
        ("the source topic exists", vec![("in", 1, 2)], 1),
        ("the source topic is missing", vec![], -1),
    ];
    for (case, topics, validated) in rows {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(image_of(None, &topics))
                .build(),
        );
        let (coord, _log) = make_coordinator();
        coord.set_metadata_source(source);
        let handle = coord.get_or_create_streams("g");
        heartbeat(&handle, StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 0,
            rebalance_timeout_ms: 1_000,
            topology: Some(one_subtopology(SingleSubtopologySetup::default())),
            ..Default::default()
        })
        .await;
        let seed = coord.cached_streams_seed("g").unwrap();
        // The member got its active tasks at its epoch, and its current
        // assignment record lists that epoch for each of them.
        let current = &seed.current_per_member["m1"];
        let at_member_epoch: BTreeMap<String, Vec<i32>> = current
            .active
            .iter()
            .map(|(subtopology, partitions)| {
                (subtopology.clone(), vec![
                    current.member_epoch;
                    partitions.len()
                ])
            })
            .collect();
        check!(current.active_epochs == at_member_epoch, "{case}");
        check!(
            (seed.validated_topology_epoch, seed.last_assignment_configs)
                == (
                    validated,
                    maplit::btreemap! {"num.standby.replicas".to_string() => "0".to_string()}
                ),
            "{case}"
        );
    }
}
