use {
    super::*,
    crate::test_support::{
        ControllerPeerAllowed, GrantsInPrincipalName, start_broker_with, test_ctx,
    },
    assert2::assert,
    krabka_metadata::{FeatureLevelRecord, MetadataRecord},
    krabka_protocol::owned::{
        common::streams_group_topology_description_update_request::{
            topology_description::TopologyDescription,
            topology_description_node::TopologyDescriptionNode,
            topology_description_subtopology::TopologyDescriptionSubtopology,
        },
        streams_group_topology_description_update_response::MAX_VERSION,
    },
    std::sync::Arc,
};

krabka_macros::wire_codec_helpers! {
    test_context ["streams-client"];
}

fn request(group_id: &str) -> StreamsGroupTopologyDescriptionUpdateRequest {
    StreamsGroupTopologyDescriptionUpdateRequest {
        group_id: group_id.into(),
        member_id: "m1".into(),
        topology_epoch: 1,
        topology_description: TopologyDescription {
            subtopologies: vec![TopologyDescriptionSubtopology {
                subtopology_id: "0".into(),
                nodes: vec![TopologyDescriptionNode {
                    name: "KSTREAM-SOURCE-0000000000".into(),
                    node_type: 1,
                    source_topics: vec!["in".into()],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
        ..Default::default()
    }
}

fn response(
    error_code: i16,
    message: Option<&str>,
) -> StreamsGroupTopologyDescriptionUpdateResponse {
    StreamsGroupTopologyDescriptionUpdateResponse {
        error_code,
        error_message: message.map(str::to_owned),
        ..Default::default()
    }
}

async fn set_streams_version(broker: &Broker, level: i16) {
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: crate::features::STREAMS_VERSION.into(),
            level,
        })])
        .await
        .expect("submit streams.version");
}

#[tokio::test]
async fn handle_answers_as_a_trunk_broker_without_a_plugin() {
    let rows = [
        (
            "streams disabled by config",
            false,
            1,
            "Group:Read",
            response(codes::UNSUPPORTED_VERSION, None),
        ),
        (
            "streams.version not finalized",
            true,
            0,
            "Group:Read",
            response(codes::UNSUPPORTED_VERSION, None),
        ),
        (
            "group Read denied",
            true,
            1,
            "Group:Describe",
            response(codes::GROUP_AUTHORIZATION_FAILED, None),
        ),
        (
            "no plugin configured",
            true,
            1,
            "Group:Read",
            response(
                codes::UNSUPPORTED_VERSION,
                Some("The broker has no streams group topology description plugin configured."),
            ),
        ),
    ];
    for (case, enable, streams_version, grants, want) in rows {
        let (broker_handle, _dir) = start_broker_with(|cfg| {
            cfg.streams_group.enable = enable;
            cfg.authorizer = Arc::new(ControllerPeerAllowed(GrantsInPrincipalName));
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        set_streams_version(&broker, streams_version).await;
        test_ctx!(ctx, grants);

        let answer = handle(&broker, request("app"), MAX_VERSION, &ctx)
            .await
            .expect("an answer");

        assert!(answer == want, "{case}");
        broker_handle.shutdown().await;
    }
}

/// With Kafka's in-memory plugin configured, a push gets past the plugin
/// check, and the service's request checks and the group lookup refuse what
/// trunk refuses, with trunk's messages. A push that reaches a group is the
/// actor's to judge; `crates/broker/tests/streams_groups` drives that path
/// over the wire.
#[tokio::test]
async fn handle_refuses_what_a_trunk_broker_with_a_plugin_refuses() {
    use crate::coordinator::unified::streams::description::TopologyDescriptionPlugin;

    let (broker_handle, _dir) = start_broker_with(|cfg| {
        cfg.streams_group.topology_description_plugin = TopologyDescriptionPlugin::InMemory;
        cfg.authorizer = Arc::new(ControllerPeerAllowed(GrantsInPrincipalName));
    })
    .await;
    broker_handle.wait_until_group_coordinator_ready().await;
    let broker = broker_handle.broker_arc_for_test();
    set_streams_version(&broker, 1).await;
    let _classic = broker
        .group_coordinator
        .get_or_create_classic("classic-app");
    test_ctx!(ctx, "Group:Read");

    let rows = [
        (
            "an empty member id",
            StreamsGroupTopologyDescriptionUpdateRequest {
                member_id: String::new(),
                ..request("app")
            },
            response(codes::INVALID_REQUEST, Some("MemberId can't be empty.")),
        ),
        (
            "an empty group id",
            request(""),
            response(codes::INVALID_REQUEST, Some("GroupId can't be empty.")),
        ),
        (
            "a group that does not exist",
            request("app"),
            response(codes::GROUP_ID_NOT_FOUND, Some("Group app not found.")),
        ),
        (
            "a classic group",
            request("classic-app"),
            response(
                codes::GROUP_ID_NOT_FOUND,
                Some("Group classic-app is not a streams group."),
            ),
        ),
    ];
    for (case, push, want) in rows {
        let answer = handle(&broker, push, MAX_VERSION, &ctx)
            .await
            .expect("an answer");

        assert!(answer == want, "{case}");
    }
    broker_handle.shutdown().await;
}
