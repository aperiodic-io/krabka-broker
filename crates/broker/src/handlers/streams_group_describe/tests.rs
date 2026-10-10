//! End-to-end tests of the `StreamsGroupDescribe` handler against a running
//! broker, driven over the wire encoding.
//!
//! Each case pins the whole decoded response, so the per-group error rows the
//! KIP-1071 gates produce -- feature disabled, group unknown, streams actor
//! gone -- stay byte-for-byte what the JVM admin client expects.

use std::{collections::HashSet, sync::Arc, time::Duration};

use assert2::{assert, check};
use krabka_metadata::{AclOperation, MetadataRecord, ResourceType};
use krabka_protocol::UnknownTaggedFields;

use super::{
    test_support::{
        describe, describe_as, describe_at, error_group, finalize_streams_version,
        seed_streams_group_topology, start_broker, start_broker_with_authorizer,
        topology_with_source_topic, unfinalize_streams_version,
    },
    *,
};
use crate::{
    authorizer::{AllowAllAuthorizer, SimpleAclAuthorizer},
    codes,
    coordinator::unified::streams::actor::StreamsGroupActorMessage,
    handlers::authorized_operations::authorized_operations_bits,
    test_support::{DenyAll, peer, principal},
};

/// Commit one literal `Allow` ACL for `User:<user>` on `(resource_type,
/// resource_name)`, for the tests that drive a specific ACL gate rather than
/// allow or deny everything.
async fn grant(
    broker_handle: &crate::broker::BrokerHandle,
    resource_type: ResourceType,
    resource_name: &str,
    operation: AclOperation,
    user: &str,
) {
    broker_handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1AccessControlEntry(
            crate::test_support::allow_acl(crate::test_support::AllowAclSetup {
                resource_type,
                resource_name,
                principal: &format!("User:{user}"),
                operation,
            }),
        )])
        .await
        .expect("commit acl");
}

#[tokio::test]
async fn disabled_feature_returns_requested_group_error_rows() {
    broker_fixture!((broker_handle, _dir, broker), start_broker(true));
    unfinalize_streams_version(&broker).await;

    let resp = describe(&broker, &["g-disabled-a", "g-disabled-b"]).await;

    let expected = unthrottled_wire!(StreamsGroupDescribeResponse {
        groups: vec![
            error_group("g-disabled-a", codes::UNSUPPORTED_VERSION),
            error_group("g-disabled-b", codes::UNSUPPORTED_VERSION),
        ],
    });
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn enabled_missing_group_returns_not_found_rows() {
    broker_fixture!((broker_handle, _dir, broker), start_broker(true));
    finalize_streams_version(&broker).await;

    let resp = describe(&broker, &["missing-a", "missing-b"]).await;

    let expected = unthrottled_wire!(StreamsGroupDescribeResponse {
        groups: vec![
            error_group("missing-a", codes::GROUP_ID_NOT_FOUND),
            error_group("missing-b", codes::GROUP_ID_NOT_FOUND),
        ],
    });
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn closed_streams_actor_returns_load_in_progress_row() {
    broker_fixture!((broker_handle, _dir, broker), start_broker(true));
    finalize_streams_version(&broker).await;

    let actor = broker.group_coordinator.get_or_create_streams("stopped");
    let (tx, rx) = tokio::sync::oneshot::channel();
    actor
        .tx
        .send(StreamsGroupActorMessage::Shutdown(tx))
        .await
        .expect("send shutdown");
    rx.await.expect("actor shutdown");
    tokio::time::timeout(Duration::from_secs(5), async {
        while !actor.tx.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actor sender closed");

    let resp = describe(&broker, &["stopped"]).await;

    let expected = unthrottled_wire!(StreamsGroupDescribeResponse {
        groups: vec![error_group("stopped", codes::COORDINATOR_LOAD_IN_PROGRESS)],
    });
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

/// Kafka's `StreamsGroupDescribeRequest.getErrorResponse`: every requested
/// group id gets `UNSUPPORTED_VERSION` (35) when the protocol is disabled,
/// table-driven over one and several requested groups.
#[tokio::test]
async fn disabled_protocol_answers_every_requested_group_with_unsupported_version() {
    let cases: [&[&str]; 2] = [&["solo"], &["g-a", "g-b", "g-c"]];
    for group_ids in cases {
        broker_fixture!((broker_handle, _dir, broker), start_broker(true));
        // `streams.version` is unfinalized, so the protocol gate is off.
        unfinalize_streams_version(&broker).await;

        let resp = describe(&broker, group_ids).await;

        let expected = unthrottled_wire!(StreamsGroupDescribeResponse {
            groups: group_ids
                .iter()
                .map(|gid| error_group(gid, codes::UNSUPPORTED_VERSION))
                .collect(),
        });
        assert!(resp == expected, "{group_ids:?}");
        broker_handle.shutdown().await;
    }
}

/// Kafka's `KafkaApis.handleStreamsGroupDescribe` checks the protocol gate
/// before any ACL check: a principal the authorizer denies on every request
/// still gets `UNSUPPORTED_VERSION`, not `GROUP_AUTHORIZATION_FAILED`, when
/// the protocol is off.
#[tokio::test]
async fn protocol_gate_runs_before_the_group_acl_check() {
    broker_fixture!(
        (broker_handle, _dir, broker),
        start_broker_with_authorizer(Arc::new(DenyAll))
    );
    // Unfinalized: the protocol is off regardless of the authorizer.
    unfinalize_streams_version(&broker).await;
    let alice = principal("alice");

    let resp = describe_as(&broker, &alice, &["g1"], false).await;

    let expected = unthrottled_wire!(StreamsGroupDescribeResponse {
        groups: vec![error_group("g1", codes::UNSUPPORTED_VERSION)],
    });
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

/// Kafka puts `GROUP_AUTHORIZATION_FAILED` rows first, ahead of every other
/// row, rather than preserving request order: the denied group sorts first
/// in the response even though it was requested second.
#[tokio::test]
async fn denied_group_rows_sort_first_regardless_of_request_order() {
    broker_fixture!(
        (broker_handle, _dir, broker),
        start_broker_with_authorizer(Arc::new(SimpleAclAuthorizer::new(HashSet::new())))
    );
    finalize_streams_version(&broker).await;
    grant(
        &broker_handle,
        ResourceType::Group,
        "allowed",
        AclOperation::Describe,
        "alice",
    )
    .await;
    let alice = principal("alice");

    // Requested in ["allowed", "denied"] order; "denied" has no ACL grant.
    let resp = describe_as(&broker, &alice, &["allowed", "denied"], false).await;

    let expected = unthrottled_wire!(StreamsGroupDescribeResponse {
        groups: vec![
            error_group("denied", codes::GROUP_AUTHORIZATION_FAILED),
            error_group("allowed", codes::GROUP_ID_NOT_FOUND),
        ],
    });
    assert!(resp == expected, "{resp:?}");
    broker_handle.shutdown().await;
}

/// Kafka hides a group whose topology names a topic the caller cannot
/// `Describe`: the row becomes `TOPIC_AUTHORIZATION_FAILED` (29) with no
/// topology and no members, and another group in the same request that names
/// only an authorized topic is unaffected.
#[tokio::test]
async fn topology_topic_denied_for_describe_hides_only_that_group() {
    broker_fixture!(
        (broker_handle, _dir, broker),
        start_broker_with_authorizer(Arc::new(SimpleAclAuthorizer::new(HashSet::new())))
    );
    finalize_streams_version(&broker).await;
    grant(
        &broker_handle,
        ResourceType::Group,
        "blocked",
        AclOperation::Describe,
        "alice",
    )
    .await;
    grant(
        &broker_handle,
        ResourceType::Group,
        "open",
        AclOperation::Describe,
        "alice",
    )
    .await;
    // "secret" gets no Describe grant; "public" does.
    grant(
        &broker_handle,
        ResourceType::Topic,
        "public",
        AclOperation::Describe,
        "alice",
    )
    .await;
    seed_streams_group_topology(&broker, "blocked", topology_with_source_topic("secret")).await;
    seed_streams_group_topology(&broker, "open", topology_with_source_topic("public")).await;
    let alice = principal("alice");

    let resp = describe_as(&broker, &alice, &["blocked", "open"], false).await;

    check!(
        resp.groups[0]
            == DescribedGroup {
                group_id: "blocked".into(),
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                error_message: Some(
                    "The described group uses topics that the client is not authorized to \
                     describe."
                        .into()
                ),
                ..Default::default()
            },
        "{:?}",
        resp.groups[0]
    );
    check!(resp.groups[1].group_id.as_str() == "open");
    check!(
        resp.groups[1].error_code == codes::NONE,
        "{:?}",
        resp.groups[1]
    );
    check!(
        resp.groups[1]
            .topology
            .as_ref()
            .and_then(|t| t.subtopologies.as_ref())
            .map(|s| s[0].source_topics.clone())
            == Some(vec!["public".to_string()])
    );
    broker_handle.shutdown().await;
}

/// KIP-430: `include_authorized_operations` fills the Group operations
/// bitfield only when the request opted in, table-driven over both flag
/// values against the same allowed group.
#[tokio::test]
async fn include_authorized_operations_fills_the_bitfield_only_on_opt_in() {
    let authorizer = Arc::new(AllowAllAuthorizer);
    broker_fixture!(
        (broker_handle, _dir, broker),
        start_broker_with_authorizer(authorizer.clone() as _)
    );
    finalize_streams_version(&broker).await;
    seed_streams_group_topology(&broker, "g1", topology_with_source_topic("t")).await;
    let alice = principal("alice");

    for include in [false, true] {
        let resp = describe_as(&broker, &alice, &["g1"], include).await;
        check!(resp.groups.len() == 1, "{resp:?}");
        check!(
            resp.groups[0].error_code == codes::NONE,
            "{:?}",
            resp.groups[0]
        );
        if include {
            let expected = authorized_operations_bits(
                authorizer.as_ref(),
                &broker.controller.current_image(),
                &crate::test_support::request_context(&alice, &peer(), "test-client"),
                ResourceType::Group,
                "g1",
            );
            check!(expected != i32::MIN);
            check!(
                resp.groups[0].authorized_operations == expected,
                "{include}"
            );
        } else {
            check!(
                resp.groups[0].authorized_operations == i32::MIN,
                "{include}"
            );
        }
    }
    broker_handle.shutdown().await;
}

/// Kafka's `StreamsGroup.asDescribedGroup` for a group whose topology is
/// ready: the configured topology with the decided partition count of the
/// changelog topic, and every member field, including the offsets the member
/// reported, its endpoint, its tags and its target assignment.
#[tokio::test]
async fn ready_group_describes_the_configured_topology_and_every_member_field() {
    use krabka_protocol::owned::{
        common::{
            streams_group_describe_response::{
                assignment::Assignment, endpoint::Endpoint, key_value::KeyValue,
                task_offset::TaskOffset, topic_info::TopicInfo,
            },
            streams_group_heartbeat_request as hb,
        },
        streams_group_describe_response::{Member, Subtopology, Topology},
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
    };

    use super::test_support::{create_topic, expected_task_ids, heartbeat};

    // The member reports task offsets, which Kafka 4.3.1 refuses and trunk
    // takes.
    let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
        cfg.streams_group.enable = true;
        cfg.features.unstable_api_versions = crate::api_catalog::UnstableApiVersions::Enabled;
    })
    .await;
    broker_handle.wait_until_group_coordinator_ready().await;
    let broker = broker_handle.broker_arc_for_test();
    finalize_streams_version(&broker).await;
    create_topic(&broker, "in", 2).await;
    create_topic(&broker, "app-store-changelog", 2).await;
    let join = StreamsGroupHeartbeatRequest {
        process_id: Some("process-1".into()),
        user_endpoint: Some(hb::endpoint::Endpoint {
            host: "localhost".into(),
            port: 8080,
            ..Default::default()
        }),
        client_tags: Some(vec![hb::key_value::KeyValue {
            key: "zone".into(),
            value: "z1".into(),
            ..Default::default()
        }]),
        ..crate::handlers::group_heartbeat_test_support::streams_request_with_topology(
            crate::handlers::group_heartbeat_test_support::StreamsTopologySetup {
                group_id: "app",
                state_changelog_topics: vec![hb::topic_info::TopicInfo {
                    name: "app-store-changelog".into(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
    };
    let joined = heartbeat(&broker, &join).await;
    check!(joined.error_code == codes::NONE, "{joined:?}");
    let task_offset = |offset| hb::task_offset::TaskOffset {
        subtopology_id: "0".into(),
        partition: 1,
        offset,
        ..Default::default()
    };
    let steady = heartbeat(
        &broker,
        &StreamsGroupHeartbeatRequest {
            group_id: "app".into(),
            member_id: "m1".into(),
            member_epoch: joined.member_epoch,
            rebalance_timeout_ms: -1,
            task_offsets: Some(vec![task_offset(5)]),
            task_end_offsets: Some(vec![task_offset(10)]),
            ..Default::default()
        },
    )
    .await;
    check!(steady.error_code == codes::NONE, "{steady:?}");

    let resp = describe(&broker, &["app"]).await;

    let none = || UnknownTaggedFields(Vec::new());
    let offsets = |offset| {
        vec![TaskOffset {
            subtopology_id: "0".into(),
            partition: 1,
            offset,
            unknown_tagged_fields: none(),
        }]
    };
    let tasks = Assignment {
        active_tasks: vec![expected_task_ids("0", vec![0, 1])],
        standby_tasks: Vec::new(),
        warmup_tasks: Vec::new(),
        unknown_tagged_fields: none(),
    };
    let expected = StreamsGroupDescribeResponse {
        throttle_time_ms: 0,
        groups: vec![DescribedGroup {
            error_code: codes::NONE,
            error_message: None,
            group_id: "app".into(),
            group_state: "Stable".into(),
            group_epoch: 2,
            assignment_epoch: 2,
            topology: Some(Topology {
                epoch: 1,
                subtopologies: Some(vec![Subtopology {
                    subtopology_id: "0".into(),
                    source_topics: vec!["in".into()],
                    repartition_sink_topics: Vec::new(),
                    state_changelog_topics: vec![TopicInfo {
                        name: "app-store-changelog".into(),
                        partitions: 2,
                        replication_factor: 0,
                        topic_configs: Vec::new(),
                        unknown_tagged_fields: none(),
                    }],
                    repartition_source_topics: Vec::new(),
                    unknown_tagged_fields: none(),
                }]),
                unknown_tagged_fields: none(),
            }),
            members: vec![Member {
                member_id: "m1".into(),
                member_epoch: 2,
                instance_id: None,
                rack_id: None,
                client_id: "streams-client".into(),
                client_host: "/127.0.0.1".into(),
                topology_epoch: 1,
                process_id: "process-1".into(),
                user_endpoint: Some(Endpoint {
                    host: "localhost".into(),
                    port: 8080,
                    unknown_tagged_fields: none(),
                }),
                client_tags: vec![KeyValue {
                    key: "zone".into(),
                    value: "z1".into(),
                    unknown_tagged_fields: none(),
                }],
                task_offsets: offsets(5),
                task_end_offsets: offsets(10),
                assignment: tasks.clone(),
                target_assignment: tasks,
                is_classic: false,
                unknown_tagged_fields: none(),
            }],
            authorized_operations: i32::MIN,
            topology_description: None,
            topology_description_status: 0,
            assignor_name: Some("sticky".into()),
            unknown_tagged_fields: none(),
        }],
        unknown_tagged_fields: none(),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

/// Kafka's `maybeUpdateGroupState`: a group with no members is `Empty`, also
/// when its topology never became ready; and a group id of another type is
/// `GROUP_ID_NOT_FOUND` with Kafka's `castToStreamsGroup` message.
#[tokio::test]
async fn empty_group_is_empty_and_another_group_type_is_not_found() {
    use krabka_protocol::owned::{
        common::streams_group_heartbeat_request as hb,
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
    };

    use super::test_support::heartbeat;

    broker_fixture!((broker_handle, _dir, broker), start_broker(true));
    finalize_streams_version(&broker).await;
    broker.group_coordinator.mark_share("share");
    let join = crate::handlers::group_heartbeat_test_support::streams_request_with_topology(
        crate::handlers::group_heartbeat_test_support::StreamsTopologySetup {
            group_id: "left",
            source_topic: "absent",
            state_changelog_topics: vec![hb::topic_info::TopicInfo {
                name: "left-changelog".into(),
                ..Default::default()
            }],
            ..Default::default()
        },
    );
    check!(heartbeat(&broker, &join).await.error_code == codes::NONE);
    let leave = StreamsGroupHeartbeatRequest {
        group_id: "left".into(),
        member_id: "m1".into(),
        member_epoch: -1,
        ..Default::default()
    };
    check!(heartbeat(&broker, &leave).await.error_code == codes::NONE);

    let resp = describe(&broker, &["left", "share"]).await;

    let groups: Vec<(String, i16, Option<String>, String, usize)> = resp
        .groups
        .into_iter()
        .map(|g| {
            (
                g.group_id,
                g.error_code,
                g.error_message,
                g.group_state,
                g.members.len(),
            )
        })
        .collect();
    assert!(
        groups
            == vec![
                ("left".into(), codes::NONE, None, "Empty".into(), 0),
                (
                    "share".into(),
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Group share is not a streams group.".into()),
                    String::new(),
                    0,
                ),
            ]
    );
    broker_handle.shutdown().await;
}

/// #973, KIP-1331 and KIP-1357: version 1 names the assignor of every
/// described group, "sticky", the assignor krabka runs for every group, and
/// answers a request for the topology description with `NOT_STORED` (1),
/// what Kafka answers without a topology description plugin, or
/// `NOT_REQUESTED` (0) when not asked. Version 0 carries neither field, and an
/// error row carries neither at any version.
#[tokio::test]
async fn version_1_names_the_assignor_and_the_topology_description_status() {
    broker_fixture!((broker_handle, _dir, broker), start_broker(true));
    finalize_streams_version(&broker).await;
    seed_streams_group_topology(&broker, "app", topology_with_source_topic("in")).await;
    let baseline = describe_at(&broker, 0, false, &["app", "missing"]).await;

    for (version, include, assignor_name, status) in [
        (0, false, None, 0),
        (1, false, Some("sticky"), 0),
        (1, true, Some("sticky"), 1),
    ] {
        let resp = describe_at(&broker, version, include, &["app", "missing"]).await;
        let expected = StreamsGroupDescribeResponse {
            groups: vec![
                DescribedGroup {
                    assignor_name: assignor_name.map(str::to_owned),
                    topology_description_status: status,
                    ..baseline.groups[0].clone()
                },
                error_group("missing", codes::GROUP_ID_NOT_FOUND),
            ],
            ..baseline.clone()
        };
        assert!(resp == expected, "version {version}, include {include}");
    }
    assert!(
        (
            baseline.groups[0].error_code,
            baseline.groups[0].group_id.as_str()
        ) == (codes::NONE, "app")
    );
    broker_handle.shutdown().await;
}
