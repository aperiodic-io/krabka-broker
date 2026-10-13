//! Projecting the streams coordinator's describe view onto the
//! `StreamsGroupDescribe` response types.
//!
//! The streams actor answers a `Describe` message with a `StreamsDescribeView`,
//! which is the coordinator's own shape rather than the wire's. This module is
//! the only place that turns that view into a `DescribedGroup`, its members,
//! and the topology, in the field order and the sort order of Kafka's
//! `StreamsGroup.asDescribedGroup`.

use {
    crate::coordinator::unified::streams::{
        actor::{StreamsDescribeMember, StreamsDescribeView},
        persistence::{StoredSubtopology, StoredTopicInfo, StreamsGroupTopologyValue},
        topology::{ConfiguredInternalTopic, ConfiguredTopology},
    },
    krabka_protocol::owned::{
        common::streams_group_describe_response::{
            assignment::Assignment, endpoint::Endpoint, key_value::KeyValue, task_ids::TaskIds,
            task_offset::TaskOffset, topic_info::TopicInfo,
        },
        streams_group_describe_response::{DescribedGroup, Member, Subtopology, Topology},
    },
    std::collections::BTreeMap,
};

/// Map a [`StreamsDescribeView`] into a wire `DescribedGroup`.
///
/// A group whose topology is ready describes its configured topology, with
/// the decided partition count of every internal topic; any other group
/// describes the topology its members sent. Both render the subtopologies by
/// id and every topic list by name.
///
/// [`StreamsDescribeView`]: crate::coordinator::unified::streams::actor::StreamsDescribeView
pub(super) fn render_group(view: StreamsDescribeView) -> DescribedGroup {
    let topology = match (view.configured_topology, view.topology) {
        (Some(configured), _) => Some(render_configured_topology(configured)),
        (None, stored) => stored.map(render_topology),
    };
    DescribedGroup {
        group_id: view.group_id,
        group_state: view.group_state,
        group_epoch: view.group_epoch,
        assignment_epoch: view.assignment_epoch,
        topology,
        members: view.members.into_iter().map(render_member).collect(),
        // KIP-1357: the assignor the next assignment will use. Every group
        // runs krabka's port of Kafka's `StickyTaskAssignor`, whatever its
        // `streams.assignor.name` says, and "sticky" is that assignor's name.
        assignor_name: Some(STICKY_ASSIGNOR_NAME.to_owned()),
        // The handler fills the authorized operations when they are asked
        // for; the wire default (INT32_MIN) means "not set". It also sets the
        // topology description status when the request asks for one.
        ..Default::default()
    }
}

/// Kafka's `StickyTaskAssignor.STICKY_ASSIGNOR_NAME`.
const STICKY_ASSIGNOR_NAME: &str = "sticky";

/// Map a describe-view member into a wire `Member`, as Kafka's
/// `StreamsGroupMember.asStreamsGroupDescribeMember` does.
fn render_member(m: StreamsDescribeMember) -> Member {
    Member {
        member_id: m.member_id,
        member_epoch: m.member_epoch,
        instance_id: m.instance_id,
        rack_id: m.rack_id,
        client_id: m.client_id,
        client_host: m.client_host,
        topology_epoch: m.topology_epoch,
        process_id: m.process_id,
        user_endpoint: m.user_endpoint.map(|(host, port)| Endpoint {
            host,
            port,
            ..Default::default()
        }),
        client_tags: m
            .client_tags
            .into_iter()
            .map(|(key, value)| KeyValue {
                key,
                value,
                ..Default::default()
            })
            .collect(),
        task_offsets: task_offsets(&m.task_offsets),
        task_end_offsets: task_offsets(&m.task_end_offsets),
        assignment: assignment(&m.active, &m.standby, &m.warmup),
        target_assignment: assignment(&m.target_active, &m.target_standby, &m.target_warmup),
        ..Default::default()
    }
}

fn assignment(
    active: &BTreeMap<String, Vec<i32>>,
    standby: &BTreeMap<String, Vec<i32>>,
    warmup: &BTreeMap<String, Vec<i32>>,
) -> Assignment {
    Assignment {
        active_tasks: task_map_to_ids(active),
        standby_tasks: task_map_to_ids(standby),
        warmup_tasks: task_map_to_ids(warmup),
        ..Default::default()
    }
}

/// Kafka's `taskOffsetsFromMap`: by subtopology, then partition.
fn task_offsets(offsets: &BTreeMap<(String, i32), i64>) -> Vec<TaskOffset> {
    offsets
        .iter()
        .map(|((subtopology_id, partition), offset)| TaskOffset {
            subtopology_id: subtopology_id.clone(),
            partition: *partition,
            offset: *offset,
            ..Default::default()
        })
        .collect()
}

fn key_values(configs: impl IntoIterator<Item = (String, String)>) -> Vec<KeyValue> {
    configs
        .into_iter()
        .map(|(key, value)| KeyValue {
            key,
            value,
            ..Default::default()
        })
        .collect()
}

fn sorted<T: Ord>(mut items: Vec<T>) -> Vec<T> {
    items.sort();
    items
}

/// Kafka's `StreamsTopology.asStreamsGroupDescribeTopology`: the topology the
/// members sent. The describe `Subtopology` omits the request-only
/// `source_topic_regex` and `copartition_groups`.
fn render_topology(t: StreamsGroupTopologyValue) -> Topology {
    fn topic_infos(infos: Vec<StoredTopicInfo>) -> Vec<TopicInfo> {
        let mut out: Vec<TopicInfo> = infos
            .into_iter()
            .map(|ti| TopicInfo {
                name: ti.name,
                partitions: ti.partitions,
                replication_factor: ti.replication_factor,
                topic_configs: key_values(ti.topic_configs),
                ..Default::default()
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }
    fn subtopology(s: StoredSubtopology) -> Subtopology {
        Subtopology {
            subtopology_id: s.subtopology_id,
            source_topics: sorted(s.source_topics),
            repartition_sink_topics: sorted(s.repartition_sink_topics),
            state_changelog_topics: topic_infos(s.state_changelog_topics),
            repartition_source_topics: topic_infos(s.repartition_source_topics),
            ..Default::default()
        }
    }
    let mut subtopologies: Vec<Subtopology> =
        t.subtopologies.into_iter().map(subtopology).collect();
    subtopologies.sort_by(|a, b| a.subtopology_id.cmp(&b.subtopology_id));
    Topology {
        epoch: t.epoch,
        subtopologies: Some(subtopologies),
        ..Default::default()
    }
}

/// Kafka's `ConfiguredTopology.asStreamsGroupDescribeTopology`: the
/// subtopologies by id, with the decided partition count of every internal
/// topic and a replication factor of 0 where the topology leaves it unset.
fn render_configured_topology(t: ConfiguredTopology) -> Topology {
    fn topic_info(topic: ConfiguredInternalTopic) -> TopicInfo {
        TopicInfo {
            name: topic.name,
            partitions: topic.partitions,
            replication_factor: topic.replication_factor.unwrap_or(0),
            topic_configs: key_values(topic.configs),
            ..Default::default()
        }
    }
    let subtopologies = t
        .subtopologies
        .unwrap_or_default()
        .into_iter()
        .map(|(subtopology_id, s)| Subtopology {
            subtopology_id,
            source_topics: s.source_topics.into_iter().collect(),
            repartition_sink_topics: s.repartition_sink_topics.into_iter().collect(),
            state_changelog_topics: s
                .state_changelog_topics
                .into_values()
                .map(topic_info)
                .collect(),
            repartition_source_topics: s
                .repartition_source_topics
                .into_values()
                .map(topic_info)
                .collect(),
            ..Default::default()
        })
        .collect();
    Topology {
        epoch: t.topology_epoch,
        subtopologies: Some(subtopologies),
        ..Default::default()
    }
}

/// Render a `subtopology -> partitions` task map as the response `Vec<TaskIds>`.
fn task_map_to_ids(map: &BTreeMap<String, Vec<i32>>) -> Vec<TaskIds> {
    map.iter()
        .map(|(sub, parts)| TaskIds {
            subtopology_id: sub.clone(),
            partitions: parts.clone(),
            ..Default::default()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            codes,
            handlers::streams_group_describe::test_support::{
                describe_member, expected_rendered_topology, expected_task_ids, task_map,
                topology_value,
            },
        },
        assert2::assert,
    };

    #[test]
    fn render_group_preserves_group_member_and_topology_fields() {
        let rendered = render_group(StreamsDescribeView {
            group_id: "streams-app".into(),
            group_epoch: 11,
            assignment_epoch: 10,
            topology_epoch: 9,
            group_state: "Stable".into(),
            topology: Some(topology_value()),
            configured_topology: None,
            members: vec![describe_member()],
            topology_description: None,
        });

        let expected = tagged_wire!(DescribedGroup {
            error_code: codes::NONE,
            error_message: None,
            group_id: "streams-app".into(),
            group_state: "Stable".into(),
            group_epoch: 11,
            assignment_epoch: 10,
            topology: Some(expected_rendered_topology()),
            members: vec![tagged_wire!(Member {
                member_id: "member-1".into(),
                member_epoch: 7,
                instance_id: Some("instance-a".into()),
                rack_id: Some("rack-a".into()),
                client_id: "client-a".into(),
                client_host: "/127.0.0.1".into(),
                topology_epoch: 9,
                process_id: "process-a".into(),
                user_endpoint: Some(tagged_wire!(Endpoint {
                    host: "host-a".into(),
                    port: 8080,
                })),
                client_tags: vec![tagged_wire!(KeyValue {
                    key: "zone".into(),
                    value: "z1".into(),
                })],
                task_offsets: vec![tagged_wire!(TaskOffset {
                    subtopology_id: "sub-a".into(),
                    partition: 0,
                    offset: 5,
                })],
                task_end_offsets: vec![tagged_wire!(TaskOffset {
                    subtopology_id: "sub-a".into(),
                    partition: 0,
                    offset: 10,
                })],
                assignment: tagged_wire!(Assignment {
                    active_tasks: vec![expected_task_ids("sub-a", vec![0, 2])],
                    standby_tasks: vec![expected_task_ids("sub-a", vec![1])],
                    warmup_tasks: vec![expected_task_ids("sub-b", vec![3, 4])],
                }),
                target_assignment: tagged_wire!(Assignment {
                    active_tasks: vec![expected_task_ids("sub-a", vec![0])],
                    standby_tasks: vec![expected_task_ids("sub-a", vec![1, 2])],
                    warmup_tasks: Vec::new(),
                }),
                is_classic: false,
            })],
            // Filled by the handler on request; the wire default otherwise.
            authorized_operations: i32::MIN,
            // v1 fields, at their schema defaults: the handler serves v0.
            topology_description: None,
            topology_description_status: 0,
            assignor_name: Some("sticky".into()),
        });
        assert!(rendered == expected);
    }

    #[test]
    fn render_topology_preserves_subtopology_and_topic_info_fields() {
        let topology = render_topology(topology_value());

        assert!(topology == expected_rendered_topology());
    }

    /// Kafka renders the subtopologies by id and every topic list by name,
    /// for the stored topology (`StreamsTopology`) and for the configured
    /// one (`ConfiguredTopology`), whatever order the members sent.
    #[test]
    fn topologies_render_in_kafka_order() {
        use {
            crate::coordinator::unified::streams::topology::ConfiguredSubtopology,
            std::collections::BTreeSet,
        };

        let info = |name: &str, partitions| StoredTopicInfo {
            name: name.into(),
            partitions,
            replication_factor: 0,
            topic_configs: Vec::new(),
        };
        let stored = |id: &str| StoredSubtopology {
            subtopology_id: id.into(),
            source_topics: vec!["t2".into(), "t1".into()],
            source_topic_regex: Vec::new(),
            repartition_sink_topics: vec!["s2".into(), "s1".into()],
            state_changelog_topics: vec![info("c2", 0), info("c1", 0)],
            repartition_source_topics: vec![info("r2", 0), info("r1", 0)],
            copartition_groups: Vec::new(),
        };
        let internal = |name: &str| ConfiguredInternalTopic {
            name: name.into(),
            partitions: 3,
            replication_factor: None,
            configs: BTreeMap::new(),
        };
        let configured = |id: &str| {
            (id.to_owned(), ConfiguredSubtopology {
                number_of_tasks: 3,
                source_topics: BTreeSet::from(["t2".to_owned(), "t1".to_owned()]),
                repartition_source_topics: [("r2", internal("r2")), ("r1", internal("r1"))]
                    .into_iter()
                    .map(|(name, topic)| (name.to_owned(), topic))
                    .collect(),
                repartition_sink_topics: BTreeSet::from(["s2".to_owned(), "s1".to_owned()]),
                state_changelog_topics: [("c2", internal("c2")), ("c1", internal("c1"))]
                    .into_iter()
                    .map(|(name, topic)| (name.to_owned(), topic))
                    .collect(),
            })
        };
        let wire = |partitions| {
            let topic = |name: &str| TopicInfo {
                name: name.into(),
                partitions,
                replication_factor: 0,
                topic_configs: Vec::new(),
                ..Default::default()
            };
            let subtopology = |id: &str| Subtopology {
                subtopology_id: id.into(),
                source_topics: vec!["t1".into(), "t2".into()],
                repartition_sink_topics: vec!["s1".into(), "s2".into()],
                state_changelog_topics: vec![topic("c1"), topic("c2")],
                repartition_source_topics: vec![topic("r1"), topic("r2")],
                ..Default::default()
            };
            Topology {
                epoch: 4,
                subtopologies: Some(vec![subtopology("a"), subtopology("b")]),
                ..Default::default()
            }
        };

        let rendered_stored = render_topology(StreamsGroupTopologyValue {
            epoch: 4,
            subtopologies: vec![stored("b"), stored("a")],
        });
        let rendered_configured = render_configured_topology(ConfiguredTopology {
            topology_epoch: 4,
            subtopologies: Some([configured("b"), configured("a")].into_iter().collect()),
            internal_topics_to_create: BTreeMap::new(),
            status: None,
        });

        assert!(rendered_stored == wire(0));
        assert!(rendered_configured == wire(3));
    }

    #[test]
    fn task_map_to_ids_preserves_sorted_task_maps() {
        let tasks = task_map_to_ids(&task_map(&[("z", vec![9]), ("a", vec![1, 2])]));

        let expected = vec![
            expected_task_ids("a", vec![1, 2]),
            expected_task_ids("z", vec![9]),
        ];
        assert!(tasks == expected);
    }
}
