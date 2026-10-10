//! KIP-1331 topology descriptions over the wire, on a broker that runs as the
//! Apache Kafka system tests run it: trunk's API versions, and Kafka's
//! in-memory topology description plugin named by its Kafka key in
//! `[server_properties]`.
//!
//! This is the broker side of `StreamsTopologyDescriptionPluginTest` in
//! Kafka's system tests: one of two members that join together is asked for
//! the description, its push is stored and described, and a group that is
//! deleted and created again is asked again.

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig, file_config::FileConfig};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    common::{
        streams_group_describe_response as described,
        streams_group_topology_description_update_request as pushed,
    },
    delete_groups_request::DeleteGroupsRequest,
    streams_group_describe_request::StreamsGroupDescribeRequest,
    streams_group_describe_response::DescribedGroup,
    streams_group_topology_description_update_request::StreamsGroupTopologyDescriptionUpdateRequest,
    streams_group_topology_description_update_response::StreamsGroupTopologyDescriptionUpdateResponse,
};

use crate::streams_harness::{
    connect, create_topic, finalize_streams_version, first_join, follow_up, topology,
};

/// The `[server_properties]` that the system-test adapter passes through.
const TRUNK_WITH_PLUGIN: &str = r#"
[server_properties]
"unstable.api.versions.enable" = "true"
"group.streams.topology.description.plugin.class" = "org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin"
"#;

const GROUP: &str = "topology-description-app";
const SOURCE: &str = "topology-description-input";

async fn boot_with_plugin() -> (krabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    let file: FileConfig = toml::from_str(TRUNK_WITH_PLUGIN).expect("parse the properties");
    file.apply_to(&mut config).expect("apply the properties");
    let broker = Broker::start(config).await.unwrap();
    broker.wait_until_group_coordinator_ready().await;
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

/// The description a Streams client pushes for [`topology`]: one source node.
fn pushed_description() -> pushed::topology_description::TopologyDescription {
    pushed::topology_description::TopologyDescription {
        subtopologies: vec![
            pushed::topology_description_subtopology::TopologyDescriptionSubtopology {
                subtopology_id: "0".into(),
                nodes: vec![pushed::topology_description_node::TopologyDescriptionNode {
                    name: "KSTREAM-SOURCE-0000000000".into(),
                    node_type: 1,
                    source_topics: vec![SOURCE.into()],
                    ..Default::default()
                }],
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

fn described_description() -> described::topology_description::TopologyDescription {
    described::topology_description::TopologyDescription {
        subtopologies: vec![
            described::topology_description_subtopology::TopologyDescriptionSubtopology {
                subtopology_id: "0".into(),
                nodes: vec![
                    described::topology_description_node::TopologyDescriptionNode {
                        name: "KSTREAM-SOURCE-0000000000".into(),
                        node_type: 1,
                        source_topics: vec![SOURCE.into()],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

/// The group's describe row, with the topology description asked for.
async fn describe_with_description(client: &Client) -> DescribedGroup {
    let response = client
        .send(StreamsGroupDescribeRequest {
            group_ids: vec![GROUP.into()],
            include_topology_description: true,
            ..Default::default()
        })
        .await
        .expect("StreamsGroupDescribe");
    let [group] = <[DescribedGroup; 1]>::try_from(response.groups).expect("one described group");
    assert!(group.error_code == 0, "{group:?}");
    group
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_member_is_asked_and_its_push_is_described() {
    let (_broker, bootstrap, _dir) = boot_with_plugin().await;
    let client = connect(&bootstrap).await;
    finalize_streams_version(&client).await;
    create_topic(&client, SOURCE, 1).await;

    let first = client
        .send(first_join(GROUP, topology(SOURCE, vec![])))
        .await
        .expect("first join");
    let second = client
        .send(first_join(GROUP, topology(SOURCE, vec![])))
        .await
        .expect("second join");
    assert!(
        first.error_code == 0 && second.error_code == 0,
        "{first:?} {second:?}"
    );
    assert!(
        (
            first.topology_description_required,
            second.topology_description_required
        ) == (true, false)
    );
    let before = describe_with_description(&client).await;
    assert!(
        (
            before.topology_description_status,
            before.topology_description
        ) == (1, None)
    );

    let pushed = client
        .send(StreamsGroupTopologyDescriptionUpdateRequest {
            group_id: GROUP.into(),
            member_id: first.member_id.clone(),
            topology_epoch: 0,
            topology_description: pushed_description(),
            ..Default::default()
        })
        .await
        .expect("StreamsGroupTopologyDescriptionUpdate");

    assert!(pushed == StreamsGroupTopologyDescriptionUpdateResponse::default());
    let after = describe_with_description(&client).await;
    assert!(
        after
            == DescribedGroup {
                topology_description_status: 3,
                topology_description: Some(described_description()),
                ..before
            }
    );
    for member in [&first, &second] {
        let again = client
            .send(follow_up(crate::support::streams::StreamsFollowUpSetup {
                group: GROUP,
                member_id: &member.member_id,
                epoch: crate::support::streams::StreamsMemberEpoch(member.member_epoch),
                ..Default::default()
            }))
            .await
            .expect("heartbeat");
        assert!(again.error_code == 0, "{again:?}");
        assert!(!again.topology_description_required, "{}", member.member_id);
    }
}

/// `DeleteGroups` drops what the plugin held, so the group that the next
/// join creates is asked again and describes nothing stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleted_and_recreated_group_is_asked_again() {
    let (_broker, bootstrap, _dir) = boot_with_plugin().await;
    let client = connect(&bootstrap).await;
    finalize_streams_version(&client).await;
    create_topic(&client, SOURCE, 1).await;
    let member = client
        .send(first_join(GROUP, topology(SOURCE, vec![])))
        .await
        .expect("join");
    assert!(member.topology_description_required, "{member:?}");
    let pushed = client
        .send(StreamsGroupTopologyDescriptionUpdateRequest {
            group_id: GROUP.into(),
            member_id: member.member_id.clone(),
            topology_epoch: 0,
            topology_description: pushed_description(),
            ..Default::default()
        })
        .await
        .expect("StreamsGroupTopologyDescriptionUpdate");
    assert!(pushed.error_code == 0, "{pushed:?}");
    let left = client
        .send(follow_up(crate::support::streams::StreamsFollowUpSetup {
            group: GROUP,
            member_id: &member.member_id,
            epoch: crate::support::streams::StreamsMemberEpoch(-1),
            ..Default::default()
        }))
        .await
        .expect("leave");
    assert!(left.error_code == 0, "{left:?}");
    let deleted = client
        .send(DeleteGroupsRequest {
            groups_names: vec![GROUP.into()],
            ..Default::default()
        })
        .await
        .expect("DeleteGroups");
    assert!(
        deleted
            .results
            .iter()
            .map(|result| result.error_code)
            .eq([0]),
        "{deleted:?}"
    );

    let rejoined = client
        .send(first_join(GROUP, topology(SOURCE, vec![])))
        .await
        .expect("join the new group");

    assert!(rejoined.error_code == 0, "{rejoined:?}");
    assert!(rejoined.topology_description_required);
    let described = describe_with_description(&client).await;
    assert!(
        (
            described.topology_description_status,
            described.topology_description
        ) == (1, None)
    );
}
