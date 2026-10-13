//! Builders for the `StreamsGroupHeartbeat` responses and for the
//! `StreamsGroupDescribe` projection.
//!
//! A heartbeat that the group accepts gets the group's timing configuration,
//! its status list and, when they changed, its tasks and the endpoint
//! information of the group. A refused heartbeat gets only the error code and
//! message, as Kafka's `GroupCoordinatorService` builds it. The builders also
//! render the in-memory `subtopology -> partitions` task maps back into the
//! wire `TaskIds` shape.

use {
    super::{StreamsDescribeMember, StreamsDescribeView},
    crate::{
        api_catalog::UnstableApiVersions,
        codes,
        coordinator::unified::streams::{
            config::StreamsGroupConfig,
            persistence::StreamsGroupTopologyValue,
            state::{StreamsGroupState, StreamsMemberState},
            topology::{ConfiguredSubtopology, ConfiguredTopology, status as topo_status},
        },
    },
    krabka_log::Offset,
    krabka_metadata::MetadataImage,
    krabka_protocol::owned::{
        common::streams_group_heartbeat_response::{
            endpoint::Endpoint, status::Status, task_ids::TaskIds as RespTaskIds,
            topic_partition::TopicPartition,
        },
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
        streams_group_heartbeat_response::{EndpointToPartitions, StreamsGroupHeartbeatResponse},
    },
    std::collections::{BTreeMap, BTreeSet, HashMap},
};

/// Renders a `subtopology -> partitions` task map as a response
/// `Vec<TaskIds>`.
fn map_to_task_ids(map: &BTreeMap<String, Vec<i32>>) -> Vec<RespTaskIds> {
    map.iter()
        .map(|(sub, parts)| RespTaskIds {
            subtopology_id: sub.clone(),
            partitions: parts.clone(),
            ..Default::default()
        })
        .collect()
}

/// The fields of every accepted heartbeat response.
///
/// Kafka trunk's `GroupMetadataManager.streamsGroupHeartbeat` sets the
/// group's `acceptable.recovery.lag` in the `int64` field of version 1
/// (KIP-1331), which version 0 does not encode. It never sets version 0's
/// `int32` field, so that one carries 0 here too, as 4.3.0 sends it.
/// Trunk also sets `TaskOffsetIntervalMs` from `streams.task.offset.interval.ms`,
/// which 4.3.1 leaves at 0, so that field follows the config only while
/// `unstable.api.versions.enable` is on.
/// `TopologyDescriptionRequired` starts false: the actor sets it afterwards,
/// in `description::maybe_request_description`, as Kafka's
/// `maybeSetTopologyDescriptionRequired` decorates the committed heartbeat.
pub(super) fn base_resp(
    error_code: i16,
    member_epoch: i32,
    config: &StreamsGroupConfig,
) -> StreamsGroupHeartbeatResponse {
    let task_offset_interval_ms = match config.unstable_api_versions {
        UnstableApiVersions::Enabled => duration_ms(config.task_offset_interval, 30_000),
        UnstableApiVersions::Disabled => 0,
    };
    StreamsGroupHeartbeatResponse {
        error_code,
        member_epoch,
        heartbeat_interval_ms: duration_ms(config.heartbeat_interval, 5_000),
        task_offset_interval_ms,
        acceptable_recovery_lag_legacy: 0,
        acceptable_recovery_lag: config.acceptable_recovery_lag,
        topology_description_required: false,
        ..Default::default()
    }
}

/// The response to a refused heartbeat: the error code and message, with every
/// other field at the default of Kafka's generated
/// `StreamsGroupHeartbeatResponseData`, where the status list is empty.
pub(crate) fn error_resp(
    error_code: i16,
    error_message: Option<String>,
) -> StreamsGroupHeartbeatResponse {
    StreamsGroupHeartbeatResponse {
        error_code,
        error_message,
        status: Some(Vec::new()),
        ..Default::default()
    }
}

/// What an accepted heartbeat sends beyond the member epoch and the status.
#[derive(Debug, Default)]
pub(super) struct ResponseDelta {
    /// Send the three task lists: the member joined or its assignment
    /// changed. Kafka sends null lists otherwise.
    pub send_tasks: bool,
    /// The detail of Kafka's `ASSIGNMENT_DELAYED` status, when a delay holds
    /// the assignment back.
    pub assignment_delayed: Option<&'static str>,
    pub endpoint_information_epoch: i32,
    pub partitions_by_user_endpoint: Option<Vec<EndpointToPartitions>>,
}

pub(super) fn build_assignment_resp(
    state: &StreamsGroupState,
    member_id: &str,
    config: &StreamsGroupConfig,
    delta: ResponseDelta,
) -> StreamsGroupHeartbeatResponse {
    let m = state
        .members
        .get(member_id)
        .expect("member exists at build_assignment_resp");
    // Kafka builds the list on every heartbeat, in this order, and sends it
    // also when it is empty: the Streams client keeps the last list it saw
    // when the list is null.
    let status_entry = |status_code: i8, status_detail: String| Status {
        status_code,
        status_detail,
        ..Default::default()
    };
    let mut status = Vec::new();
    if state.topology.is_some() && m.topology_epoch < state.topology_epoch {
        status.push(status_entry(
            topo_status::STALE_TOPOLOGY,
            format!(
                "The member's topology epoch {} is behind the group's topology epoch {}.",
                m.topology_epoch, state.topology_epoch
            ),
        ));
    }
    if let Some(detail) = delta.assignment_delayed {
        status.push(status_entry(
            topo_status::ASSIGNMENT_DELAYED,
            detail.to_owned(),
        ));
    }
    if let Some((code, detail)) = &state.status {
        status.push(status_entry(*code, detail.clone()));
    }
    if let Some(requester) = &state.shutdown_request_member_id {
        status.push(status_entry(
            topo_status::SHUTDOWN_APPLICATION,
            format!(
                "Streams group member {requester} encountered a fatal error and requested a \
                 shutdown for the entire application."
            ),
        ));
    }
    StreamsGroupHeartbeatResponse {
        member_id: member_id.to_string(),
        status: Some(status),
        active_tasks: delta.send_tasks.then(|| map_to_task_ids(&m.active)),
        standby_tasks: delta.send_tasks.then(|| map_to_task_ids(&m.standby)),
        warmup_tasks: delta.send_tasks.then(|| map_to_task_ids(&m.warmup)),
        endpoint_information_epoch: delta.endpoint_information_epoch,
        partitions_by_user_endpoint: delta.partitions_by_user_endpoint,
        ..base_resp(codes::NONE, m.member_epoch, config)
    }
}

/// Appends Kafka trunk's `MISSING_CLIENT_TAGS` status to an accepted
/// heartbeat response when the member does not send every tag key that
/// `streams.rack.aware.assignment.tags` names.
///
/// `GroupMetadataManager.streamsGroupHeartbeat` adds it last, after the
/// shutdown request, and only at request version 1 and above: a version 0
/// client throws on a status code it does not know. The detail names the
/// missing keys in the configured order, as Java's `List.toString` renders
/// them. A refused heartbeat and a leave carry no such status.
pub(super) fn add_missing_client_tags(
    response: &mut StreamsGroupHeartbeatResponse,
    state: &StreamsGroupState,
    config: &StreamsGroupConfig,
    request: &StreamsGroupHeartbeatRequest,
    version: i16,
) {
    if version < 1 || response.error_code != codes::NONE || request.member_epoch < 0 {
        return;
    }
    let Some(member) = state.members.get(&response.member_id) else {
        return;
    };
    let missing: Vec<&str> = config
        .rack_aware_assignment_tags
        .iter()
        .filter(|tag| !member.client_tags.iter().any(|(key, _)| key == *tag))
        .map(String::as_str)
        .collect();
    if missing.is_empty() {
        return;
    }
    response.status.get_or_insert_with(Vec::new).push(Status {
        status_code: topo_status::MISSING_CLIENT_TAGS,
        status_detail: format!(
            "Missing required client tags for rack-aware standby assignment: [{}]. Configure \
             them via 'client.tag.<tagKey>' in your Streams config.",
            missing.join(", ")
        ),
        ..Default::default()
    });
}

/// Kafka's `StreamsGroup.buildEndpointToPartitions`: one entry for each member
/// with a user endpoint, the other members in id order and `member_id` last.
///
/// An entry lists the source and repartition source topic partitions of the
/// member's active tasks, and of its standby tasks, as
/// `EndpointToPartitionsManager` does. Kafka throws when the topology is not
/// configured; this builder lists no partitions instead, because a member of
/// such a group has no tasks.
///
/// Kafka 4.3.1 lists the standby tasks alone. It cuts a task's partitions to
/// the partition count of a topic that has fewer partitions than the task
/// has, and keeps an entry that is left empty. Kafka trunk lists standby and
/// warmup tasks together, leaves out a task partition that a topic does not
/// have, and drops an entry with no partition. Those are the rules while
/// `unstable.api.versions.enable` is on.
pub(super) fn endpoint_to_partitions(
    state: &StreamsGroupState,
    member_id: &str,
    subtopologies: Option<&BTreeMap<String, ConfiguredSubtopology>>,
    image: Option<&MetadataImage>,
    unstable: UnstableApiVersions,
) -> Vec<EndpointToPartitions> {
    let mut members: Vec<&StreamsMemberState> = state
        .members
        .values()
        .filter(|member| member.member_id != member_id)
        .collect();
    members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    members.extend(state.members.get(member_id));
    members
        .into_iter()
        .filter_map(|member| {
            let (host, port) = member.user_endpoint.as_ref()?;
            let mut standby = member.standby.clone();
            if unstable == UnstableApiVersions::Enabled {
                for (subtopology, partitions) in &member.warmup {
                    standby
                        .entry(subtopology.clone())
                        .or_default()
                        .extend(partitions.iter().copied());
                }
            }
            Some(EndpointToPartitions {
                user_endpoint: Endpoint {
                    host: host.clone(),
                    port: *port,
                    ..Default::default()
                },
                active_partitions: topic_partitions(&member.active, subtopologies, image, unstable),
                standby_partitions: topic_partitions(&standby, subtopologies, image, unstable),
                ..Default::default()
            })
        })
        .collect()
}

/// Kafka's `EndpointToPartitionsManager.topicPartitions`, with the rules of
/// 4.3.1 or of trunk as `endpoint_to_partitions` describes.
fn topic_partitions(
    tasks: &BTreeMap<String, Vec<i32>>,
    subtopologies: Option<&BTreeMap<String, ConfiguredSubtopology>>,
    image: Option<&MetadataImage>,
    unstable: UnstableApiVersions,
) -> Vec<TopicPartition> {
    let (Some(subtopologies), Some(image)) = (subtopologies, image) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (subtopology_id, task_partitions) in tasks {
        let Some(subtopology) = subtopologies.get(subtopology_id) else {
            continue;
        };
        let topics: BTreeSet<&str> = subtopology
            .source_topics
            .iter()
            .chain(subtopology.repartition_source_topics.keys())
            .map(String::as_str)
            .collect();
        for topic in topics {
            if image.topic(topic).is_none() {
                continue;
            }
            let partition_count = image.topic_partition_count(topic);
            let mut partitions: Vec<i32> = task_partitions.clone();
            partitions.sort_unstable();
            partitions.dedup();
            match unstable {
                UnstableApiVersions::Enabled => {
                    partitions.retain(|partition| *partition < partition_count);
                    if partitions.is_empty() {
                        continue;
                    }
                }
                // Kafka cuts the sorted task partitions to the topic's
                // partition count only when the task has more partitions than
                // the topic, and it keeps the entry when nothing is left.
                UnstableApiVersions::Disabled => {
                    partitions.truncate(usize::try_from(partition_count).unwrap_or(0));
                }
            }
            out.push(TopicPartition {
                topic: topic.to_string(),
                partitions,
                ..Default::default()
            });
        }
    }
    out
}

pub(super) fn build_describe(
    state: &StreamsGroupState,
    topology: Option<&StreamsGroupTopologyValue>,
    configured_topology: Option<&ConfiguredTopology>,
) -> StreamsDescribeView {
    let target = |role: &HashMap<String, BTreeMap<String, Vec<i32>>>, member_id: &str| {
        role.get(member_id).cloned().unwrap_or_default()
    };
    let offsets = |offsets: &BTreeMap<(String, i32), Offset>| {
        offsets
            .iter()
            .map(|(task, offset)| (task.clone(), offset.0))
            .collect()
    };
    let mut members: Vec<StreamsDescribeMember> = state
        .members
        .values()
        .map(|m| StreamsDescribeMember {
            member_id: m.member_id.clone(),
            member_epoch: m.member_epoch,
            instance_id: m.instance_id.clone(),
            rack_id: m.rack_id.clone(),
            client_id: m.client_id.clone(),
            client_host: m.client_host.clone(),
            topology_epoch: m.topology_epoch,
            process_id: m.process_id.clone(),
            user_endpoint: m.user_endpoint.clone(),
            client_tags: m.client_tags.clone(),
            task_offsets: offsets(&m.task_offsets),
            task_end_offsets: offsets(&m.task_end_offsets),
            active: m.active.clone(),
            standby: m.standby.clone(),
            warmup: m.warmup.clone(),
            target_active: target(&state.target.active, &m.member_id),
            target_standby: target(&state.target.standby, &m.member_id),
            target_warmup: target(&state.target.warmup, &m.member_id),
        })
        .collect();
    members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    StreamsDescribeView {
        group_id: state.group_id.clone(),
        group_epoch: state.group_epoch,
        assignment_epoch: state.target.epoch,
        topology_epoch: state.topology_epoch,
        group_state: state.phase.as_str().to_string(),
        topology: topology.cloned(),
        configured_topology: configured_topology.cloned(),
        members,
        topology_description: None,
    }
}

fn duration_ms(d: std::time::Duration, fallback: i32) -> i32 {
    i32::try_from(d.as_millis()).unwrap_or(fallback)
}
