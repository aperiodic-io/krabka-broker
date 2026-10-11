//! One group's row in a `StreamsGroupDescribe` response, from the streams
//! actor query through the topology topic-`Describe` filter.
//!
//! Every exit in this module produces a `DescribedGroup` rather than failing
//! the request, which is what lets a denied, misrouted, or unknown group sit
//! beside fully resolved groups in the same response. The caller
//! (`handle()`) already resolved the KIP-1071 protocol gate and the
//! per-group `Group:Describe` ACL before calling this function, matching
//! Kafka's `handleStreamsGroupDescribe`: the protocol gate is checked once
//! for the whole request, and denied groups are gathered separately so their
//! rows sort first in the response.

use krabka_protocol::owned::streams_group_describe_response::DescribedGroup;

use super::{render::render_group, topic_authz};
use crate::{
    broker::Broker,
    codes,
    coordinator::{
        GroupCoordinator,
        unified::{GroupType, streams::actor::StreamsGroupActorMessage},
    },
    handlers::{
        RequestContext, any_topic_describe_denied,
        authorized_operations::{DescribedGroupRow as _, fill_group_authorized_operations},
        group_coordinator_error,
    },
    task_util::{AskError, ask},
};

/// Kafka's `TopologyDescriptionStatus` `NOT_STORED` (1): no description is
/// recorded for the group.
const TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED: i8 = 1;

/// Kafka's `TopologyDescriptionStatus` `AVAILABLE` (3): the row carries the
/// description.
const TOPOLOGY_DESCRIPTION_STATUS_AVAILABLE: i8 = 3;

/// What the request asks a described row to carry beyond the group.
#[derive(Debug, Clone, Copy)]
pub(super) struct Included {
    /// KIP-430's `IncludeAuthorizedOperations`.
    pub(super) authorized_operations: bool,
    /// KIP-1331's `IncludeTopologyDescription`.
    pub(super) topology_description: bool,
}

/// Kafka's message for a group whose topology names a topic the caller
/// cannot `Describe`.
const TOPIC_AUTHZ_DENIED_MESSAGE: &str =
    "The described group uses topics that the client is not authorized to describe.";

/// Resolve one requested `group_id`, already past the protocol gate and the
/// group `Describe` ACL check, into its `DescribedGroup` row.
// cargo-mutants: streams-coordinator response projection; integration-tested.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn describe_group(
    broker: &Broker,
    ng: &GroupCoordinator,
    image: &krabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
    included: Included,
    gid: &str,
) -> DescribedGroup {
    if let Some(error_code) = group_coordinator_error(broker, gid) {
        return DescribedGroup::error_row(gid, error_code, None);
    }
    let Some(handle) = ng.find_streams(gid) else {
        // Kafka's `getStreamsGroupOrThrow` and `castToStreamsGroup` messages.
        let other_type = ng
            .group_type(gid)
            .is_some_and(|group_type| group_type != GroupType::Streams)
            || ng.find(gid).is_some();
        let message = if other_type {
            format!("Group {gid} is not a streams group.")
        } else {
            format!("Streams group {gid} not found.")
        };
        return DescribedGroup::error_row(gid, codes::GROUP_ID_NOT_FOUND, Some(message));
    };
    let mut view = match ask(&handle.tx, |reply| StreamsGroupActorMessage::Describe {
        reply,
    })
    .await
    {
        Ok(view) => view,
        Err(AskError::Closed) => {
            return DescribedGroup::error_row(gid, codes::COORDINATOR_LOAD_IN_PROGRESS, None);
        }
        Err(AskError::Dropped) => {
            return DescribedGroup::error_row(gid, codes::UNKNOWN_SERVER_ERROR, None);
        }
    };

    // Kafka hides a group whose topology names a topic the caller cannot
    // `Describe` (source, repartition sink, repartition source or changelog):
    // the row becomes `TOPIC_AUTHORIZATION_FAILED` with no topology and no
    // members, instead of disclosing the topic names via the topology.
    if let Some(topology) = view.topology.as_ref() {
        let required = topic_authz::required_topics(topology);
        if any_topic_describe_denied(broker.config.authorizer.as_ref(), image, ctx, &required) {
            return DescribedGroup::error_row(
                gid,
                codes::TOPIC_AUTHORIZATION_FAILED,
                Some(TOPIC_AUTHZ_DENIED_MESSAGE.to_owned()),
            );
        }
    }

    let description = view.topology_description.take();
    let mut row = render_group(view);
    // KIP-1331: Kafka's `StreamsGroupTopologyDescriptionManager.attachTopologyDescriptions`
    // gives a described group the description its plugin holds for the
    // group's topology epoch, or `NOT_STORED`. Without a plugin nothing is
    // ever stored. An error row returned above keeps `NOT_REQUESTED`.
    if included.topology_description {
        row.topology_description_status = if description.is_some() {
            TOPOLOGY_DESCRIPTION_STATUS_AVAILABLE
        } else {
            TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED
        };
        row.topology_description = description.map(|description| description.to_describe());
    }
    fill_group_authorized_operations(
        broker.config.authorizer.as_ref(),
        image,
        ctx,
        included.authorized_operations,
        std::slice::from_mut(&mut row),
    );
    row
}
