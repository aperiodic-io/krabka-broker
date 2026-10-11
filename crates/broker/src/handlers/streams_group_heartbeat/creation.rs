//! Creation of the internal topics that a streams topology needs, through the
//! `CreateTopics` path that serves a client.
//!
//! Kafka's coordinator returns the topics to create with the heartbeat
//! response, and `KafkaApis` checks `Create` on the whole set before it hands
//! them to `AutoTopicCreationManager.createStreamsInternalTopics`: `Create`
//! on the `Cluster` once, else `Create` on each topic. A caller that fails
//! both gets none of the topics created and a `MISSING_INTERNAL_TOPICS`
//! status detail naming them, so a principal with only group `Read` can never
//! make the broker create a topic on its behalf. An authorized creation goes
//! to [`crate::auto_topic_creation::AutoTopicCreation`], which sends a
//! `CreateTopics` request to the active controller with the principal of the
//! caller and does not wait for the answer. The controller validates the
//! configs, applies the topic policy and places the replicas. A failure goes
//! into an error cache for twice the heartbeat interval of the group. When
//! the response of a later heartbeat has a `MISSING_INTERNAL_TOPICS` status,
//! that status names the failure.

use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::owned::{
    common::streams_group_heartbeat_response::status::Status,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};

use crate::{
    broker::Broker,
    coordinator::unified::streams::{
        actor::resolve_group_config_from_image,
        topology::{InternalTopicSpec, status as topo_status},
    },
    handlers::{RequestContext, acl_denied, acl_wire::CLUSTER_RESOURCE_NAME},
    time_util::now_ms,
    topic_creator::ForwardedIdentity,
};

/// How many failures the `MISSING_INTERNAL_TOPICS` status names, as Kafka's
/// `KafkaApis` does.
const MAX_ERRORS_TO_INCLUDE: usize = 3;

/// The heartbeat that asks for the internal topics.
pub(super) struct Heartbeat<'a> {
    /// The context of the request.
    pub(super) ctx: &'a RequestContext<'a>,
    /// The group that the heartbeat is for.
    pub(super) group_id: &'a str,
}

/// Asks for the internal topics of `specs` and adds the cached failures to
/// the `MISSING_INTERNAL_TOPICS` status of `response`.
pub(super) fn create_internal_topics(
    broker: &Broker,
    heartbeat: &Heartbeat<'_>,
    response: &mut StreamsGroupHeartbeatResponse,
    specs: &[InternalTopicSpec],
) {
    // Kafka checks `Create` on the whole `topicsToCreate` set before any of
    // them is created: `Create` on the `Cluster` once, else `Create` on each
    // topic name. A topic that fails the per-topic fallback holds back every
    // topic in this heartbeat, not only itself -- the next heartbeat's
    // `MISSING_INTERNAL_TOPICS` status tries the whole set again.
    let unauthorized = create_unauthorized(broker, heartbeat.ctx, specs);
    if !unauthorized.is_empty() {
        let detail = format!(
            "Unauthorized to CREATE on topics {}.",
            unauthorized.join(", ")
        );
        append_missing_internal_topics_detail(response, &detail);
        return;
    }

    // Kafka caches a failure for twice the heartbeat interval of the group:
    // the group config override, else the broker default.
    let ttl_ms = 2 * heartbeat_interval_ms(broker, heartbeat.group_id);
    broker.auto_topic_creation.create_streams_internal_topics(
        broker,
        specs.iter().map(creatable_topic).collect(),
        ForwardedIdentity::of(heartbeat.ctx),
        ttl_ms,
    );

    // The creation runs in the background. Kafka reads the cached errors
    // only when the response already has a `MISSING_INTERNAL_TOPICS` status,
    // so a failure shows in a later heartbeat.
    let Some(status) = response
        .status
        .iter_mut()
        .flatten()
        .find(|status| status.status_code == topo_status::MISSING_INTERNAL_TOPICS)
    else {
        return;
    };
    let errors = broker
        .auto_topic_creation
        .streams_internal_topic_creation_errors(
            specs.iter().map(|spec| spec.name.as_str()),
            now_ms(),
        );
    if errors.is_empty() {
        return;
    }
    let shown = errors
        .iter()
        .take(MAX_ERRORS_TO_INCLUDE)
        .map(|(topic, error)| format!("{topic} ({error})"))
        .collect::<Vec<_>>()
        .join(", ");
    let detail = if errors.len() > MAX_ERRORS_TO_INCLUDE {
        format!("{shown} and {} more", errors.len() - MAX_ERRORS_TO_INCLUDE)
    } else {
        shown
    };
    status.status_detail = format!("{}; Creation failed: {detail}.", status.status_detail);
}

/// The streams heartbeat interval of `group_id`, in milliseconds, as
/// `KafkaApis.handleStreamsGroupHeartbeat` reads it: the
/// `streams.heartbeat.interval.ms` of the group config, else the broker
/// default.
fn heartbeat_interval_ms(broker: &Broker, group_id: &str) -> i64 {
    let config = resolve_group_config_from_image(
        &broker.config.streams_group,
        &broker.controller.current_image(),
        group_id,
    );
    i64::try_from(config.heartbeat_interval.as_millis()).unwrap_or(i64::MAX)
}

/// Kafka's `CREATE` gate before `createStreamsInternalTopics`: `Create` on
/// the `Cluster` once, else `Create` on each topic name of `specs`. Returns
/// the names creation is not authorized for, in name order -- empty when the
/// whole request may create every one of `specs`, whether because the
/// `Cluster` grant covers them or because each one has its own.
fn create_unauthorized(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    specs: &[InternalTopicSpec],
) -> Vec<String> {
    let image = broker.controller.current_image();
    let cluster_create_denied = acl_denied(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        ResourceType::Cluster,
        CLUSTER_RESOURCE_NAME,
        AclOperation::Create,
    );
    if !cluster_create_denied {
        return Vec::new();
    }
    let mut unauthorized: Vec<String> = specs
        .iter()
        .filter(|spec| {
            acl_denied(
                broker.config.authorizer.as_ref(),
                &image,
                ctx,
                ResourceType::Topic,
                &spec.name,
                AclOperation::Create,
            )
        })
        .map(|spec| spec.name.clone())
        .collect();
    unauthorized.sort_unstable();
    unauthorized
}

/// Kafka's `MISSING_INTERNAL_TOPICS` status append: joins `detail` onto the
/// existing status with `"; "`, or adds a new status holding only `detail`
/// when the response carries none yet.
fn append_missing_internal_topics_detail(
    response: &mut StreamsGroupHeartbeatResponse,
    detail: &str,
) {
    let statuses = response.status.get_or_insert_with(Vec::new);
    if let Some(status) = statuses
        .iter_mut()
        .find(|status| status.status_code == topo_status::MISSING_INTERNAL_TOPICS)
    {
        status.status_detail = format!("{}; {detail}", status.status_detail);
    } else {
        statuses.push(Status {
            status_code: topo_status::MISSING_INTERNAL_TOPICS,
            status_detail: detail.to_string(),
            ..Default::default()
        });
    }
}

/// The `CreateTopics` row of one internal topic, as Kafka's
/// `InternalTopicManager.toCreatableTopic` builds it: the replication factor
/// of the topology when it is not 0, else -1, so that the controller applies
/// `default.replication.factor`.
fn creatable_topic(spec: &InternalTopicSpec) -> CreatableTopic {
    CreatableTopic {
        name: spec.name.clone(),
        num_partitions: spec.partitions,
        replication_factor: if spec.replication_factor == 0 {
            -1
        } else {
            spec.replication_factor
        },
        configs: spec
            .configs
            .iter()
            .map(|(name, value)| CreatableTopicConfig {
                name: name.clone(),
                value: Some(value.clone()),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::check;

    use super::*;

    /// Kafka's `InternalTopicManager.toCreatableTopic` sends -1 for a
    /// topology that names no replication factor, and the controller applies
    /// `default.replication.factor`. A named factor goes out as it is.
    #[test]
    fn creatable_topic_sends_the_topology_replication_factor_or_minus_one() {
        for (replication_factor, expected) in [(0, -1), (1, 1), (3, 3)] {
            let spec = InternalTopicSpec {
                name: "app-store-changelog".into(),
                partitions: 4,
                replication_factor,
                configs: BTreeMap::from([("cleanup.policy".into(), "compact".into())]),
            };
            check!(
                creatable_topic(&spec)
                    == CreatableTopic {
                        name: "app-store-changelog".into(),
                        num_partitions: 4,
                        replication_factor: expected,
                        configs: vec![CreatableTopicConfig {
                            name: "cleanup.policy".into(),
                            value: Some("compact".into()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                "replication_factor = {replication_factor}"
            );
        }
    }
}
