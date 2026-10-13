//! `StreamsGroupDescribe` (`api_key` 89), from KIP-1071. It returns one
//! `DescribedGroup` per requested `group_id`, rendered from the streams actor's
//! `Describe` view.
//!
//! Mirrors the KIP-848 consumer-group describe handler, but matches Kafka's
//! `KafkaApis.handleStreamsGroupDescribe` on three points that handler does
//! not need to: the KIP-1071 protocol gate is checked once for the whole
//! request, before any ACL check, and produces `UNSUPPORTED_VERSION` for
//! every requested group when the protocol is off; groups denied `Group:
//! Describe` are gathered separately so their `GROUP_AUTHORIZATION_FAILED`
//! rows sort first in the response, ahead of every other row; and a group
//! whose topology names a topic the caller cannot `Describe` is hidden by
//! `group::describe_group`, which folds in the topic-Describe filter.
//!
//! This file holds only the wire entry point: it decodes the request,
//! resolves the KIP-1071 feature gate once, partitions the requested groups
//! into denied and non-denied, and collects one row per requested group.
//! `group` decides a single non-denied group's row, from the group-coordinator
//! query through the topology topic-Describe filter, and `render` projects
//! the actor's describe view onto the response types.

use {
    crate::{
        broker::Broker,
        error::BrokerError,
        handlers::{ApiVersion, RequestContext, group_describe_denied, streams_protocol_enabled},
    },
    krabka_protocol::owned::{
        streams_group_describe_request::StreamsGroupDescribeRequest,
        streams_group_describe_response::{DescribedGroup, StreamsGroupDescribeResponse},
    },
};

mod group;
mod render;
mod topic_authz;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use {
    self::group::{Included, describe_group},
    crate::codes,
};

// cargo-mutants: streams-coordinator response projection; integration-tested.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn handle(
    broker: &Broker,
    req: StreamsGroupDescribeRequest,
    _version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<StreamsGroupDescribeResponse, BrokerError> {
    let image = broker.controller.current_image();
    let ng = broker.group_coordinator.clone();

    // KIP-1071: same gate as the heartbeat — finalized streams.version >= 1
    // AND the config kill-switch. Kafka's
    // `StreamsGroupDescribeRequest.getErrorResponse` answers every requested
    // group id with UNSUPPORTED_VERSION when the protocol is off, checked
    // once for the whole request and before any ACL check runs.
    if !streams_protocol_enabled(broker, &image) {
        let groups = req
            .group_ids
            .iter()
            .map(|gid| DescribedGroup {
                group_id: gid.clone(),
                error_code: codes::UNSUPPORTED_VERSION,
                ..Default::default()
            })
            .collect();
        let resp = StreamsGroupDescribeResponse {
            groups,
            ..Default::default()
        };
        return Ok(resp);
    }

    // Kafka puts `GROUP_AUTHORIZATION_FAILED` rows first, ahead of
    // successfully-described (or otherwise-errored) rows, rather than
    // preserving request order.
    let mut denied_rows: Vec<DescribedGroup> = Vec::new();
    let mut other_rows: Vec<DescribedGroup> = Vec::new();
    for gid in &req.group_ids {
        if group_describe_denied(broker.config.authorizer.as_ref(), &image, ctx, gid) {
            denied_rows.push(DescribedGroup {
                group_id: gid.clone(),
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
                ..Default::default()
            });
            continue;
        }
        other_rows.push(
            describe_group(
                broker,
                &ng,
                &image,
                ctx,
                Included {
                    authorized_operations: req.include_authorized_operations,
                    topology_description: req.include_topology_description,
                },
                gid,
            )
            .await,
        );
    }
    denied_rows.extend(other_rows);

    Ok(StreamsGroupDescribeResponse {
        groups: denied_rows,
        ..Default::default()
    })
}
