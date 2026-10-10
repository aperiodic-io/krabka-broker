//! Coordinator fixtures and typed mailbox requests for streams actor tests.

use std::sync::Arc;

use krabka_ids::PartitionIndex;
use krabka_protocol::owned::{
    common::streams_group_heartbeat_response::{status::Status, task_ids::TaskIds},
    streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
};

use super::{
    StreamsDescribeView, StreamsGroupActorHandle, StreamsGroupActorMessage, StreamsHeartbeatResult,
};
use crate::{
    coordinator::unified::{
        GroupCoordinator, actor::MetadataProvider, config::NextGenConfig,
        offsets_log::fake::InMemoryOffsetsLog, reconciler::ReconcileInput,
        share::config::ShareGroupConfig, streams::config::StreamsGroupConfig,
    },
    task_util::ask,
};

pub(super) fn undelayed() -> StreamsGroupConfig {
    StreamsGroupConfig {
        initial_rebalance_delay: std::time::Duration::ZERO,
        assignment_interval: std::time::Duration::ZERO,
        ..StreamsGroupConfig::default()
    }
}

#[derive(Debug)]
struct EmptyMetadata;

impl MetadataProvider for EmptyMetadata {
    fn snapshot(&self) -> ReconcileInput {
        ReconcileInput::default()
    }
}

/// A coordinator with no connected metadata source, retaining the supplied log ownership.
pub(super) fn coordinator_with_log(
    config: StreamsGroupConfig,
    log: Arc<InMemoryOffsetsLog>,
) -> Arc<GroupCoordinator> {
    Arc::new(GroupCoordinator::new(
        NextGenConfig::default(),
        ShareGroupConfig::default(),
        Arc::new(EmptyMetadata),
        log,
        config,
    ))
}

pub(super) fn make_coordinator() -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    let log = Arc::new(InMemoryOffsetsLog::default());
    let coordinator = coordinator_with_log(undelayed(), Arc::clone(&log));
    (coordinator, log)
}

pub(super) fn member_request(member_id: &str, member_epoch: i32) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        ..Default::default()
    }
}

pub(super) async fn describe(handle: &StreamsGroupActorHandle) -> StreamsDescribeView {
    ask(&handle.tx, |reply| StreamsGroupActorMessage::Describe {
        reply,
    })
    .await
    .unwrap()
}

pub(super) async fn heartbeat_result_at(
    handle: &StreamsGroupActorHandle,
    req: StreamsGroupHeartbeatRequest,
    version: i16,
) -> StreamsHeartbeatResult {
    ask(&handle.tx, |reply| StreamsGroupActorMessage::Heartbeat {
        request: Box::new(req),
        version,
        client_id: "client".into(),
        client_host: "/127.0.0.1".into(),
        reply,
    })
    .await
    .unwrap()
}

/// A response task list for the single-subtopology fixtures.
pub(super) fn response_tasks(partitions: Vec<i32>) -> Vec<TaskIds> {
    if partitions.is_empty() {
        vec![]
    } else {
        vec![TaskIds {
            subtopology_id: "0".into(),
            partitions,
            ..Default::default()
        }]
    }
}

/// Independent expected response fields for a single subtopology's active assignment.
#[derive(krabka_macros::FieldDefaults)]
pub(super) struct ActiveResponseSetup<'a> {
    #[default("m1")]
    pub member_id: &'a str,
    #[default(crate::coordinator::unified::test_support::MemberEpoch(1))]
    pub epoch: crate::coordinator::unified::test_support::MemberEpoch,
    pub status: Option<Vec<Status>>,
    pub active: Option<Vec<PartitionIndex>>,
}

pub(super) fn expected_active_response(
    setup: ActiveResponseSetup<'_>,
) -> krabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse {
    let ActiveResponseSetup {
        member_id,
        epoch,
        status,
        active,
    } = setup;
    krabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse {
        member_id: member_id.into(),
        status,
        standby_tasks: active.as_ref().map(|_| vec![]),
        warmup_tasks: active.as_ref().map(|_| vec![]),
        active_tasks: active.map(|partitions| {
            response_tasks(partitions.into_iter().map(|index| index.0).collect())
        }),
        ..super::response::base_resp(crate::codes::NONE, epoch.0, &undelayed())
    }
}
