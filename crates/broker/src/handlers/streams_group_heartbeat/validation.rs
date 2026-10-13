//! The request checks that Kafka's `GroupCoordinatorService` runs on a
//! `StreamsGroupHeartbeat` before the group coordinator sees it.
//!
//! A refused request changes no group: Kafka answers it with the error code
//! and message and never schedules the write operation.

use {
    crate::{api_catalog::UnstableApiVersions, codes},
    krabka_protocol::owned::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
};

/// `LEAVE_GROUP_STATIC_MEMBER_EPOCH`: the epoch of a static member's leave.
const LEAVE_GROUP_STATIC_MEMBER_EPOCH: i32 = -2;

/// Kafka's `throwIfStreamsGroupHeartbeatRequestIsUsingUnsupportedFeatures`
/// and then `throwIfStreamsGroupHeartbeatRequestIsInvalid`, in Kafka's order.
/// Returns the error code and message of the first check that the request
/// fails.
///
/// Kafka 4.3.1 refuses static membership, task offsets and warmup tasks, and
/// Kafka trunk drops those three checks, so they hold only while
/// `unstable.api.versions.enable` is off. Both refuse source topic regexes.
pub(super) fn request_error(
    req: &StreamsGroupHeartbeatRequest,
    unstable: UnstableApiVersions,
) -> Option<(i16, String)> {
    let invalid = |message: &str| Some((codes::INVALID_REQUEST, message.to_string()));

    if unstable == UnstableApiVersions::Disabled {
        if req.instance_id.is_some() {
            return invalid("Static membership is not yet supported.");
        }
        if req.task_offsets.is_some() {
            return invalid("TaskOffsets are not supported yet.");
        }
        if req.task_end_offsets.is_some() {
            return invalid("TaskEndOffsets are not supported yet.");
        }
        if req
            .warmup_tasks
            .as_ref()
            .is_some_and(|tasks| !tasks.is_empty())
        {
            return invalid("WarmupTasks are not supported yet.");
        }
    }
    if req
        .topology
        .iter()
        .flat_map(|topology| topology.subtopologies.iter())
        .any(|subtopology| !subtopology.source_topic_regex.is_empty())
    {
        return invalid("Regular expressions for source topics are not supported yet.");
    }

    if is_blank(&req.member_id) {
        return invalid("MemberId can't be empty.");
    }
    if is_blank(&req.group_id) {
        return invalid("GroupId can't be empty.");
    }
    if req.instance_id.as_deref().is_some_and(is_blank) {
        return invalid("InstanceId can't be empty.");
    }
    if req.rack_id.as_deref().is_some_and(is_blank) {
        return invalid("RackId can't be empty.");
    }

    if req.member_epoch == 0 {
        if req.rebalance_timeout_ms == -1 {
            return invalid("RebalanceTimeoutMs must be provided in first request.");
        }
        // Kafka's `throwIfNotEmptyCollection` refuses a null list too.
        for (tasks, name) in [
            (req.active_tasks.as_deref(), "ActiveTasks"),
            (req.standby_tasks.as_deref(), "StandbyTasks"),
            (req.warmup_tasks.as_deref(), "WarmupTasks"),
        ] {
            if tasks.is_none_or(|tasks| !tasks.is_empty()) {
                return invalid(&format!("{name} must be empty when (re-)joining."));
            }
        }
        let Some(topology) = &req.topology else {
            return invalid("Topology must be non-null when (re-)joining.");
        };
        // Kafka's `throwIfInvalidTopology`.
        if let Some(topic) = topology
            .subtopologies
            .iter()
            .flat_map(|subtopology| subtopology.state_changelog_topics.iter())
            .find(|topic| topic.partitions != 0)
        {
            return Some((
                codes::STREAMS_INVALID_TOPOLOGY,
                format!(
                    "Changelog topic {} must have an undefined partition count, but it is set to \
                     {}.",
                    topic.name, topic.partitions
                ),
            ));
        }
    } else if req.member_epoch == LEAVE_GROUP_STATIC_MEMBER_EPOCH {
        if req.instance_id.is_none() {
            return invalid("InstanceId can't be null.");
        }
    } else if req.member_epoch < LEAVE_GROUP_STATIC_MEMBER_EPOCH {
        return invalid(&format!(
            "MemberEpoch is {}, but must be greater than or equal to -2.",
            req.member_epoch
        ));
    }

    let lists = [
        req.active_tasks.is_some(),
        req.standby_tasks.is_some(),
        req.warmup_tasks.is_some(),
    ];
    if lists.contains(&true) && lists.contains(&false) {
        return invalid("If one task-type is non-null, all must be non-null.");
    }

    if req.member_epoch != 0 && req.topology.is_some() {
        return invalid("Topology can only be provided when (re-)joining.");
    }
    None
}

/// Kafka's `throwIfEmptyString`: Java's `String.trim` removes every leading
/// and trailing character at or below U+0020.
fn is_blank(value: &str) -> bool { value.trim_matches(|c: char| c <= ' ').is_empty() }

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::handlers::group_heartbeat_test_support::streams_request,
        assert2::check,
        krabka_protocol::owned::{
            common::streams_group_heartbeat_request::{task_ids::TaskIds, topic_info::TopicInfo},
            streams_group_heartbeat_request::{Subtopology, Topology},
        },
    };

    fn topology() -> Topology {
        Topology {
            epoch: 1,
            subtopologies: vec![Subtopology {
                subtopology_id: "0".into(),
                source_topics: vec!["in".into()],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// A valid join: a member id, a rebalance timeout, three empty task lists
    /// and a topology.
    fn join() -> StreamsGroupHeartbeatRequest { streams_request("g", "m1") }

    fn heartbeat(member_epoch: i32) -> StreamsGroupHeartbeatRequest {
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch,
            ..Default::default()
        }
    }

    fn task() -> Vec<TaskIds> {
        vec![TaskIds {
            subtopology_id: "0".into(),
            partitions: vec![0],
            ..Default::default()
        }]
    }

    type Row = (
        &'static str,
        StreamsGroupHeartbeatRequest,
        Option<(i16, String)>,
    );

    fn invalid(message: &str) -> (i16, String) { (codes::INVALID_REQUEST, message.to_string()) }

    /// The rows of the checks on every request and on a join.
    fn join_rows() -> Vec<Row> {
        vec![
            ("a valid join", join(), None),
            ("a valid heartbeat", heartbeat(3), None),
            (
                "an empty member id",
                StreamsGroupHeartbeatRequest {
                    member_id: String::new(),
                    ..join()
                },
                Some(invalid("MemberId can't be empty.")),
            ),
            (
                "a blank group id",
                StreamsGroupHeartbeatRequest {
                    group_id: " \t".into(),
                    ..join()
                },
                Some(invalid("GroupId can't be empty.")),
            ),
            (
                "an empty instance id",
                StreamsGroupHeartbeatRequest {
                    instance_id: Some(String::new()),
                    ..join()
                },
                Some(invalid("InstanceId can't be empty.")),
            ),
            (
                "an empty rack id",
                StreamsGroupHeartbeatRequest {
                    rack_id: Some(String::new()),
                    ..join()
                },
                Some(invalid("RackId can't be empty.")),
            ),
            (
                "a join without a rebalance timeout",
                StreamsGroupHeartbeatRequest {
                    rebalance_timeout_ms: -1,
                    ..join()
                },
                Some(invalid(
                    "RebalanceTimeoutMs must be provided in first request.",
                )),
            ),
            (
                "a join with active tasks",
                StreamsGroupHeartbeatRequest {
                    active_tasks: Some(task()),
                    ..join()
                },
                Some(invalid("ActiveTasks must be empty when (re-)joining.")),
            ),
            (
                "a join with a null active task list",
                StreamsGroupHeartbeatRequest {
                    active_tasks: None,
                    ..join()
                },
                Some(invalid("ActiveTasks must be empty when (re-)joining.")),
            ),
            (
                "a join with standby tasks",
                StreamsGroupHeartbeatRequest {
                    standby_tasks: Some(task()),
                    ..join()
                },
                Some(invalid("StandbyTasks must be empty when (re-)joining.")),
            ),
            (
                "a join with warmup tasks",
                StreamsGroupHeartbeatRequest {
                    warmup_tasks: Some(task()),
                    ..join()
                },
                Some(invalid("WarmupTasks must be empty when (re-)joining.")),
            ),
            (
                "a join without a topology",
                StreamsGroupHeartbeatRequest {
                    topology: None,
                    ..join()
                },
                Some(invalid("Topology must be non-null when (re-)joining.")),
            ),
        ]
    }

    /// The rows of the topology checks and of the checks on a heartbeat.
    fn topology_and_heartbeat_rows() -> Vec<Row> {
        vec![
            (
                "a changelog topic with a partition count",
                StreamsGroupHeartbeatRequest {
                    topology: Some(Topology {
                        subtopologies: vec![Subtopology {
                            state_changelog_topics: vec![TopicInfo {
                                name: "store-changelog".into(),
                                partitions: 2,
                                ..Default::default()
                            }],
                            ..topology().subtopologies[0].clone()
                        }],
                        ..topology()
                    }),
                    ..join()
                },
                Some((
                    codes::STREAMS_INVALID_TOPOLOGY,
                    "Changelog topic store-changelog must have an undefined partition count, but \
                     it is set to 2."
                        .into(),
                )),
            ),
            (
                "a source topic regex",
                StreamsGroupHeartbeatRequest {
                    member_id: String::new(),
                    topology: Some(Topology {
                        subtopologies: vec![Subtopology {
                            source_topic_regex: vec!["in-.*".into()],
                            ..topology().subtopologies[0].clone()
                        }],
                        ..topology()
                    }),
                    ..join()
                },
                Some(invalid(
                    "Regular expressions for source topics are not supported yet.",
                )),
            ),
            (
                "epoch -3",
                heartbeat(-3),
                Some(invalid(
                    "MemberEpoch is -3, but must be greater than or equal to -2.",
                )),
            ),
            (
                "epoch -2 without an instance id",
                heartbeat(-2),
                Some(invalid("InstanceId can't be null.")),
            ),
            (
                "one task list without the others",
                StreamsGroupHeartbeatRequest {
                    active_tasks: Some(vec![]),
                    ..heartbeat(3)
                },
                Some(invalid(
                    "If one task-type is non-null, all must be non-null.",
                )),
            ),
            (
                "a topology on a heartbeat",
                StreamsGroupHeartbeatRequest {
                    topology: Some(topology()),
                    ..heartbeat(3)
                },
                Some(invalid("Topology can only be provided when (re-)joining.")),
            ),
        ]
    }

    /// Each row is a request and the error that Kafka's
    /// `GroupCoordinatorService` gives it, or `None` for a valid request. The
    /// rows name an instance id in one place, and trunk accepts that, so they
    /// run with `unstable.api.versions.enable` on.
    #[test]
    fn request_checks_follow_kafka() {
        for (name, request, expected) in
            join_rows().into_iter().chain(topology_and_heartbeat_rows())
        {
            check!(
                request_error(&request, UnstableApiVersions::Enabled) == expected,
                "{name}"
            );
        }
    }

    /// Kafka 4.3.1's `throwIfStreamsGroupHeartbeatRequestIsUsingUnsupportedFeatures`
    /// refuses, in this order, an instance id, task offsets, task end offsets,
    /// non-empty warmup tasks and a source topic regex, before it checks
    /// anything else. Kafka trunk keeps only the regex check.
    #[test]
    fn version_4_3_1_refuses_static_membership_offsets_and_warmup_tasks() {
        use krabka_protocol::owned::common::streams_group_heartbeat_request::task_offset::TaskOffset;

        let regex_topology = Topology {
            subtopologies: vec![Subtopology {
                source_topic_regex: vec!["in-.*".into()],
                ..topology().subtopologies[0].clone()
            }],
            ..topology()
        };
        let with_regex = |request: StreamsGroupHeartbeatRequest| StreamsGroupHeartbeatRequest {
            topology: Some(regex_topology.clone()),
            ..request
        };
        let offsets = || {
            Some(vec![TaskOffset {
                subtopology_id: "0".into(),
                partition: 0,
                offset: 5,
                ..Default::default()
            }])
        };
        let instance = Some("i1".to_string());
        // (name, request, the answer of 4.3.1 and the answer of trunk)
        let rows = [
            (
                "an instance id",
                StreamsGroupHeartbeatRequest {
                    instance_id: instance.clone(),
                    ..join()
                },
                Some(invalid("Static membership is not yet supported.")),
                None,
            ),
            (
                "a static member leaving",
                StreamsGroupHeartbeatRequest {
                    instance_id: instance.clone(),
                    ..heartbeat(-2)
                },
                Some(invalid("Static membership is not yet supported.")),
                None,
            ),
            (
                "a static member leaving for good",
                StreamsGroupHeartbeatRequest {
                    instance_id: instance.clone(),
                    ..heartbeat(-1)
                },
                Some(invalid("Static membership is not yet supported.")),
                None,
            ),
            (
                "an empty instance id",
                StreamsGroupHeartbeatRequest {
                    instance_id: Some(String::new()),
                    ..join()
                },
                Some(invalid("Static membership is not yet supported.")),
                Some(invalid("InstanceId can't be empty.")),
            ),
            (
                "task offsets",
                StreamsGroupHeartbeatRequest {
                    task_offsets: offsets(),
                    ..heartbeat(3)
                },
                Some(invalid("TaskOffsets are not supported yet.")),
                None,
            ),
            (
                "empty task offsets",
                StreamsGroupHeartbeatRequest {
                    task_offsets: Some(vec![]),
                    ..heartbeat(3)
                },
                Some(invalid("TaskOffsets are not supported yet.")),
                None,
            ),
            (
                "task end offsets",
                StreamsGroupHeartbeatRequest {
                    task_end_offsets: offsets(),
                    ..heartbeat(3)
                },
                Some(invalid("TaskEndOffsets are not supported yet.")),
                None,
            ),
            (
                "warmup tasks",
                StreamsGroupHeartbeatRequest {
                    active_tasks: Some(vec![]),
                    standby_tasks: Some(vec![]),
                    warmup_tasks: Some(task()),
                    ..heartbeat(3)
                },
                Some(invalid("WarmupTasks are not supported yet.")),
                None,
            ),
            (
                "an empty warmup task list",
                StreamsGroupHeartbeatRequest {
                    active_tasks: Some(vec![]),
                    standby_tasks: Some(vec![]),
                    warmup_tasks: Some(vec![]),
                    ..heartbeat(3)
                },
                None,
                None,
            ),
            (
                "an instance id before task offsets",
                StreamsGroupHeartbeatRequest {
                    instance_id: instance.clone(),
                    task_offsets: offsets(),
                    task_end_offsets: offsets(),
                    ..heartbeat(3)
                },
                Some(invalid("Static membership is not yet supported.")),
                None,
            ),
            (
                "task offsets before task end offsets",
                StreamsGroupHeartbeatRequest {
                    task_offsets: offsets(),
                    task_end_offsets: offsets(),
                    ..heartbeat(3)
                },
                Some(invalid("TaskOffsets are not supported yet.")),
                None,
            ),
            (
                "an unsupported feature before an invalid request",
                StreamsGroupHeartbeatRequest {
                    instance_id: instance.clone(),
                    member_id: String::new(),
                    ..join()
                },
                Some(invalid("Static membership is not yet supported.")),
                Some(invalid("MemberId can't be empty.")),
            ),
            (
                "an unsupported feature before a source topic regex",
                with_regex(StreamsGroupHeartbeatRequest {
                    instance_id: instance,
                    ..join()
                }),
                Some(invalid("Static membership is not yet supported.")),
                Some(invalid(
                    "Regular expressions for source topics are not supported yet.",
                )),
            ),
            (
                "a source topic regex",
                with_regex(join()),
                Some(invalid(
                    "Regular expressions for source topics are not supported yet.",
                )),
                Some(invalid(
                    "Regular expressions for source topics are not supported yet.",
                )),
            ),
        ];
        for (name, request, released, trunk) in rows {
            check!(
                request_error(&request, UnstableApiVersions::Disabled) == released,
                "{name}: 4.3.1"
            );
            check!(
                request_error(&request, UnstableApiVersions::Enabled) == trunk,
                "{name}: trunk"
            );
        }
    }
}
