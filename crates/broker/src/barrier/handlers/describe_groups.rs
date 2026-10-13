//! `DescribeBarrierGroups`, api key 1011.
//!
//! The response holds one row per group. An empty request array asks for every
//! group that this broker coordinates, and a named array asks for those groups
//! only.
//!
//! A named group that another broker coordinates gets `NOT_COORDINATOR` (16),
//! and a named group that no coordinator holds gets `RESOURCE_NOT_FOUND` (91).
//! The two codes tell a caller whether to retry against another broker or to
//! stop.
//!
//! Authorization: `Describe` on `Cluster("kafka-cluster")`. On a deny every row
//! carries `CLUSTER_AUTHORIZATION_FAILED` (31).

use {
    crate::{
        barrier::{
            STATE_TOPIC,
            coordinator::{BarrierCoordinator, GroupDescription},
            handlers::{cluster_describe_denied, interval_to_wire},
        },
        broker::Broker,
        codes,
        handlers::{ApiVersion, DispatchResult, RequestContext, encode_response},
    },
    krabka_metadata::MetadataImage,
    krabka_protocol::krabka::barrier::{
        DescribeBarrierGroupsRequest, DescribeBarrierGroupsResponse, DescribedBarrierGroup,
    },
};

/// The `coordinator_id` of a group that no broker coordinates now.
const NO_COORDINATOR: i32 = -1;

#[krabka_macros::extension_wire(
    name = "handle_describe_barrier_groups",
    api = "DescribeBarrierGroups",
    level = "info"
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: DescribeBarrierGroupsRequest,
    version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> DispatchResult {
    let image = broker.controller.current_image();
    if cluster_describe_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        let groups = req
            .groups
            .iter()
            .map(|group| {
                error_row(
                    group,
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    "describe-barrier-groups denied",
                )
            })
            .collect();
        return encode_response(&response(groups), version);
    }

    let coordinator = &broker.barrier_coordinator;
    let held = coordinator.describe_groups(&req.groups).await;

    let groups = if req.groups.is_empty() {
        held.iter()
            .map(|description| {
                described_row(
                    description,
                    coordinator_id(coordinator, &image, description),
                )
            })
            .collect()
    } else {
        let mut rows = Vec::with_capacity(req.groups.len());
        for name in &req.groups {
            let found = held.iter().find(|entry| &entry.group == name);
            let row = if let Some(description) = found {
                described_row(
                    description,
                    coordinator_id(coordinator, &image, description),
                )
            } else if coordinator.is_coordinator_for(name).await {
                error_row(name, codes::RESOURCE_NOT_FOUND, "no such barrier group")
            } else {
                error_row(
                    name,
                    codes::NOT_COORDINATOR,
                    "this broker does not coordinate the barrier group",
                )
            };
            rows.push(row);
        }
        rows
    };

    encode_response(&response(groups), version)
}

/// The node that leads the `__barrier_state` partition of a group.
fn coordinator_id(
    coordinator: &BarrierCoordinator,
    image: &MetadataImage,
    description: &GroupDescription,
) -> i32 {
    let partition = coordinator.state_partition_for(&description.group);
    image
        .partition(STATE_TOPIC, partition.get())
        .and_then(|record| i32::try_from(record.leader.get()).ok())
        .unwrap_or(NO_COORDINATOR)
}

/// The row of a group that this broker coordinates.
fn described_row(description: &GroupDescription, coordinator_id: i32) -> DescribedBarrierGroup {
    DescribedBarrierGroup {
        group: description.group.clone(),
        error_code: codes::NONE,
        error_message: None,
        topics: description.definition.topics.clone(),
        interval_ms: interval_to_wire(description.definition.interval),
        retained_cuts: description.definition.retained_cuts,
        last_epoch: description.definition.last_epoch,
        coordinator_id,
        ..DescribedBarrierGroup::default()
    }
}

/// The row of a group that the response cannot describe.
fn error_row(group: &str, error_code: i16, error_message: &str) -> DescribedBarrierGroup {
    DescribedBarrierGroup {
        group: group.to_owned(),
        error_code,
        error_message: Some(error_message.to_owned()),
        ..DescribedBarrierGroup::default()
    }
}

/// The response around a list of rows.
fn response(groups: Vec<DescribedBarrierGroup>) -> DescribeBarrierGroupsResponse {
    DescribeBarrierGroupsResponse {
        throttle_time_ms: 0,
        groups,
        ..DescribeBarrierGroupsResponse::default()
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::barrier::persistence::GroupValue, assert2::check, krabka_units::millis};

    fn description() -> GroupDescription {
        GroupDescription {
            group: "orders-cut".to_owned(),
            definition: GroupValue {
                topics: vec!["orders".to_owned(), "payments".to_owned()],
                interval: Some(millis(30_000)),
                retained_cuts: 5,
                last_epoch: 7,
            },
            cut_epochs: vec![6, 7],
            pending_epoch: None,
        }
    }

    #[test]
    fn a_described_group_carries_its_definition_and_its_coordinator() {
        let expected = DescribedBarrierGroup {
            group: "orders-cut".to_owned(),
            error_code: codes::NONE,
            error_message: None,
            topics: vec!["orders".to_owned(), "payments".to_owned()],
            interval_ms: 30_000,
            retained_cuts: 5,
            last_epoch: 7,
            coordinator_id: 2,
            ..DescribedBarrierGroup::default()
        };
        check!(described_row(&description(), 2) == expected);
    }

    #[test]
    fn a_group_that_never_injected_reports_the_interval_as_off() {
        let mut description = description();
        description.definition.interval = None;
        check!(described_row(&description, NO_COORDINATOR).interval_ms == -1);
    }

    #[test]
    fn an_error_row_names_the_group_the_code_and_the_text() {
        let expected = DescribedBarrierGroup {
            group: "orders-cut".to_owned(),
            error_code: codes::NOT_COORDINATOR,
            error_message: Some("this broker does not coordinate the barrier group".to_owned()),
            ..DescribedBarrierGroup::default()
        };
        check!(
            error_row(
                "orders-cut",
                codes::NOT_COORDINATOR,
                "this broker does not coordinate the barrier group",
            ) == expected
        );
    }

    #[test]
    fn an_error_row_leaves_every_definition_field_at_its_sentinel() {
        let row = error_row("orders-cut", codes::RESOURCE_NOT_FOUND, "no such group");
        check!(row.topics.is_empty());
        check!(row.interval_ms == -1);
        check!(row.retained_cuts == 0);
        check!(row.last_epoch == -1);
        check!(row.coordinator_id == NO_COORDINATOR);
    }
}
