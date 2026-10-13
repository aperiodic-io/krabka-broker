//! The KIP-1331 steps of the streams actor, driven through its handle with no
//! connected `MetadataSource`: a joining member installs the topology, which
//! is all the solicitation needs.

use {
    super::*,
    crate::{
        coordinator::unified::{
            StreamsGroupSeed,
            offsets_log::fake::InMemoryOffsetsLog,
            persistence::{Key, parse_key},
            streams::{
                actor::{
                    StreamsGroupActorHandle, StreamsGroupActorMessage,
                    test_support::{
                        coordinator_with_log, describe, heartbeat_result_at, undelayed,
                    },
                },
                description::{
                    DescriptionEpochs, Node, NodeKind, Subtopology, TopologyDescriptionPlugin,
                },
                persistence::{StreamsGroupKey, StreamsGroupMetadataValue},
                topology::to_stored_topology,
            },
        },
        task_util::ask,
    },
    assert2::assert,
    krabka_protocol::owned::{
        common::streams_group_topology_description_update_request::{
            topology_description_node::TopologyDescriptionNode,
            topology_description_subtopology::TopologyDescriptionSubtopology,
        },
        streams_group_heartbeat_request::{Subtopology as WireSubtopology, Topology},
    },
    std::sync::Arc,
};

fn coordinator(plugin: TopologyDescriptionPlugin) -> Arc<GroupCoordinator> {
    coordinator_and_log(plugin).coordinator
}

/// A coordinator whose streams groups use `plugin`, and the offsets log it
/// writes.
struct CoordinatorAndLog {
    coordinator: Arc<GroupCoordinator>,
    log: Arc<InMemoryOffsetsLog>,
}

fn coordinator_and_log(plugin: TopologyDescriptionPlugin) -> CoordinatorAndLog {
    let log = Arc::new(InMemoryOffsetsLog::default());
    let coordinator = coordinator_with_log(
        StreamsGroupConfig {
            topology_description_plugin: plugin,
            ..undelayed()
        },
        Arc::clone(&log),
    );
    CoordinatorAndLog { coordinator, log }
}

/// The value of the last group metadata record that `log` holds.
async fn last_group_metadata(log: &InMemoryOffsetsLog) -> StreamsGroupMetadataValue {
    log.batches()
        .await
        .iter()
        .flat_map(|batch| batch.records.iter())
        .filter(|record| {
            record.key.as_deref().is_some_and(|key| {
                matches!(
                    parse_key(key),
                    Ok(Key::Streams(StreamsGroupKey::GroupMetadata { .. }))
                )
            })
        })
        .filter_map(|record| record.value.as_deref())
        .map(|value| StreamsGroupMetadataValue::decode(value).expect("a group metadata value"))
        .next_back()
        .expect("a group metadata record")
}

/// A join of `member_id` with the one-subtopology topology at epoch 0.
fn join(member_id: &str) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: "app".into(),
        member_id: member_id.into(),
        member_epoch: 0,
        topology: Some(Topology {
            epoch: 0,
            subtopologies: vec![WireSubtopology {
                subtopology_id: "0".into(),
                source_topics: vec!["in".into()],
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The next heartbeat of a member that joined at `member_epoch`.
fn steady(member_id: &str, member_epoch: i32) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: "app".into(),
        member_id: member_id.into(),
        member_epoch,
        ..Default::default()
    }
}

async fn heartbeat(
    handle: &StreamsGroupActorHandle,
    request: StreamsGroupHeartbeatRequest,
    version: i16,
) -> StreamsGroupHeartbeatResponse {
    let response = heartbeat_result_at(handle, request, version).await.response;
    assert!(response.error_code == codes::NONE, "{response:?}");
    response
}

async fn push(handle: &StreamsGroupActorHandle, push: DescriptionPush) -> PushAnswer {
    ask(&handle.tx, |reply| {
        StreamsGroupActorMessage::PushDescription {
            push: Box::new(push),
            reply,
        }
    })
    .await
    .expect("a push answer")
}

/// The source node a Streams client describes for the topology of [`join`].
fn wire_description(node_type: i8) -> WireDescription {
    WireDescription {
        subtopologies: vec![TopologyDescriptionSubtopology {
            subtopology_id: "0".into(),
            nodes: vec![TopologyDescriptionNode {
                name: "KSTREAM-SOURCE-0000000000".into(),
                node_type,
                source_topics: vec!["in".into()],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn description_push(member_id: &str, topology_epoch: i32, node_type: i8) -> DescriptionPush {
    DescriptionPush {
        member_id: member_id.into(),
        topology_epoch,
        description: wire_description(node_type),
    }
}

/// Each row joins two members of a fresh group, one after the other, and
/// sends a heartbeat from the first. Only a broker with a plugin asks, only
/// at version 1, and only the first member: the back-off window holds the
/// others off, as Kafka's `armIfNotActive` does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_member_of_a_group_is_asked_for_the_description() {
    let rows = [
        ("no plugin", TopologyDescriptionPlugin::None, 1, [
            false, false, false,
        ]),
        ("version 0", TopologyDescriptionPlugin::InMemory, 0, [
            false, false, false,
        ]),
        (
            "in-memory plugin",
            TopologyDescriptionPlugin::InMemory,
            1,
            [true, false, false],
        ),
    ];
    for (row, plugin, version, want) in rows {
        let coordinator = coordinator(plugin);
        let handle = coordinator.get_or_create_streams("app");

        let first = heartbeat(&handle, join("a"), version).await;
        let second = heartbeat(&handle, join("b"), version).await;
        let again = heartbeat(&handle, steady("a", first.member_epoch), version).await;

        let asked = [
            first.topology_description_required,
            second.topology_description_required,
            again.topology_description_required,
        ];
        assert!(asked == want, "{row}");
    }
}

/// A push stores the description and records its epoch: a describe serves
/// it, the group metadata record carries the epoch in Kafka trunk's tag 2, and
/// no heartbeat asks again. Any member of the group may push, not only the
/// one asked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_push_is_stored_and_ends_the_asking() {
    let CoordinatorAndLog { coordinator, log } =
        coordinator_and_log(TopologyDescriptionPlugin::InMemory);
    let handle = coordinator.get_or_create_streams("app");
    let first = heartbeat(&handle, join("a"), 1).await;
    assert!(first.topology_description_required);
    let second = heartbeat(&handle, join("b"), 1).await;
    assert!(describe(&handle).await.topology_description.is_none());
    let before = last_group_metadata(&log).await;

    let answer = push(&handle, description_push("b", 0, 1)).await;

    assert!(answer == (codes::NONE, None));
    let expected = TopologyDescription {
        subtopologies: vec![Subtopology {
            id: "0".into(),
            nodes: vec![Node {
                name: "KSTREAM-SOURCE-0000000000".into(),
                kind: NodeKind::Source {
                    topics: vec!["in".into()],
                },
                successors: vec![],
            }],
        }],
        global_stores: vec![],
    };
    assert!(describe(&handle).await.topology_description == Some(expected));
    let stored = DescriptionEpochs {
        stored: 0,
        failed: -1,
    };
    assert!(
        last_group_metadata(&log).await
            == StreamsGroupMetadataValue {
                description: stored,
                ..before
            }
    );
    let seed = coordinator
        .cached_streams_seed("app")
        .expect("a cached seed");
    assert!(seed.description_epochs == stored);
    for (member_id, member_epoch) in [("a", first.member_epoch), ("b", second.member_epoch)] {
        let response = heartbeat(&handle, steady(member_id, member_epoch), 1).await;
        assert!(!response.topology_description_required, "{member_id}");
    }
}

/// The pushes Kafka's `validateStreamsGroupTopologyDescriptionUpdate` and
/// converter refuse, with their messages. None of them stores anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_push_the_group_does_not_accept_stores_nothing() {
    let rows = [
        (
            "an unknown member",
            description_push("z", 0, 1),
            (
                codes::UNKNOWN_MEMBER_ID,
                Some("Member z is not a member of group app.".to_owned()),
            ),
        ),
        (
            "another topology epoch",
            description_push("a", 3, 1),
            (
                codes::INVALID_REQUEST,
                Some(
                    "Topology epoch 3 does not match the group's current topology epoch 0."
                        .to_owned(),
                ),
            ),
        ),
        (
            "an unknown node type",
            description_push("a", 0, 9),
            (
                codes::INVALID_REQUEST,
                Some("Unknown topology node type: 9".to_owned()),
            ),
        ),
    ];
    for (row, refused, want) in rows {
        let coordinator = coordinator(TopologyDescriptionPlugin::InMemory);
        let handle = coordinator.get_or_create_streams("app");
        heartbeat(&handle, join("a"), 1).await;

        assert!(push(&handle, refused).await == want, "{row}");
        assert!(
            describe(&handle).await.topology_description.is_none(),
            "{row}"
        );
        let seed = coordinator
            .cached_streams_seed("app")
            .expect("a cached seed");
        assert!(
            seed.description_epochs == DescriptionEpochs::default(),
            "{row}"
        );
    }

    let coordinator = coordinator(TopologyDescriptionPlugin::InMemory);
    let empty = coordinator.get_or_create_streams("app");
    assert!(
        push(&empty, description_push("a", 0, 1)).await
            == (
                codes::GROUP_ID_NOT_FOUND,
                Some("Group app not found.".to_owned())
            )
    );
}

/// A group loaded from the log keeps the epochs its record stored, so its
/// members are not asked again, as in Kafka trunk. The in-memory plugin did
/// not survive the load, so a describe finds no description, as Kafka's
/// reference plugin loses its map with the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_loaded_group_is_not_asked_for_a_stored_epoch() {
    let epochs = |stored, failed| DescriptionEpochs { stored, failed };
    // (row, the loaded epochs, the topology epoch, whether the member is asked)
    let rows = [
        ("stored", epochs(0, -1), 0, false),
        ("failed for good", epochs(-1, 0), 0, false),
        ("uncertain", epochs(-2, -1), 0, true),
        ("stored at an older epoch", epochs(0, -1), 1, true),
        ("none", epochs(-1, -1), 0, true),
    ];
    for (row, description_epochs, topology_epoch, asked) in rows {
        let mut request = join("a");
        let topology = request
            .topology
            .as_mut()
            .expect("a join sends its topology");
        topology.epoch = topology_epoch;
        let loaded = to_stored_topology(topology);
        let coordinator = coordinator(TopologyDescriptionPlugin::InMemory);
        let handle = coordinator.get_or_create_streams("app");
        handle
            .tx
            .send(StreamsGroupActorMessage::Seed(StreamsGroupSeed {
                group_epoch: 2,
                description_epochs,
                topology: Some(loaded),
                ..StreamsGroupSeed::default()
            }))
            .await
            .expect("the actor runs");

        let response = heartbeat(&handle, request, 1).await;

        assert!(response.topology_description_required == asked, "{row}");
        assert!(
            describe(&handle).await.topology_description.is_none(),
            "{row}"
        );
    }
}

/// A group loaded from a record that Kafka trunk, or a broker with the
/// plugin, wrote: a broker with the plugin writes its description epochs back
/// in tags 2 and 3, and one without it writes Kafka 4.3.1's record, the same
/// value without them, while it keeps the epochs it read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_description_epochs_are_written_with_the_plugin_only() {
    let replayed = DescriptionEpochs {
        stored: 2,
        failed: 1,
    };
    let mut written = Vec::new();
    for plugin in [
        TopologyDescriptionPlugin::None,
        TopologyDescriptionPlugin::InMemory,
    ] {
        let CoordinatorAndLog { coordinator, log } = coordinator_and_log(plugin);
        let handle = coordinator.get_or_create_streams("app");
        let mut request = join("a");
        let loaded = to_stored_topology(request.topology.as_mut().expect("a topology"));
        handle
            .tx
            .send(StreamsGroupActorMessage::Seed(StreamsGroupSeed {
                description_epochs: replayed,
                topology: Some(loaded),
                ..StreamsGroupSeed::new_group()
            }))
            .await
            .expect("the actor runs");

        heartbeat(&handle, request, 1).await;

        let seed = coordinator
            .cached_streams_seed("app")
            .expect("a cached seed");
        assert!(seed.description_epochs == replayed, "{plugin:?}");
        written.push(last_group_metadata(&log).await);
    }
    let [without_plugin, with_plugin] = <[_; 2]>::try_from(written).expect("two records");
    assert!(without_plugin.description == DescriptionEpochs::default());
    assert!(
        with_plugin
            == StreamsGroupMetadataValue {
                description: replayed,
                ..without_plugin
            }
    );
}
