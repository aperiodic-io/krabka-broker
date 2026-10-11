//! `CreateTopics` (`api_key=19`). Routes through `Controller::submit_change`
//! so every topic/partition creation goes through the metadata quorum before
//! the partition directories are materialized on disk.
//!
//! Automatic replica placement is site-aware. See [`crate::site_placement`]
//! for the site spread and the leadership pinning it gives. An explicit
//! `assignments` field still wins, as it does in Kafka.
//!
//! KIP-525: a v5+ row reports what the topic was created as -- its partition
//! count, its replication factor, and its whole effective configuration, the
//! list [`crate::handlers::describe_configs`] answers a `TOPIC` resource with.
//! A client that reads it needs no follow-up `DescribeConfigs`, which is what
//! Terraform's `kafka_topic`, Connect's `TopicAdmin` and Streams'
//! `InternalTopicManager` do. Kafka gates the whole disclosure on a second,
//! per-topic ACL check -- `DescribeConfigs` on `Topic(name)` -- and a denial
//! withholds it behind `topicConfigErrorCode` without failing the create.

use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
    },
    primitives::uuid::Uuid as ProtoUuid,
};

mod authorization;
mod creation;
mod materialize;
mod name;
mod placement;
mod records;
mod response;

#[cfg(test)]
mod tests;

use self::{
    authorization::{authorize_create_topics, describe_configs_denied},
    name::CLUSTER_METADATA_TOPIC,
    response::{
        creatable_topic_configs, create_topics_response, finish_response, topic_error_result,
    },
};
pub(crate) use self::{
    creation::TopicCreation,
    placement::{
        InitialLeadership, automatic_leaderships, automatic_placement_exclusions, inactive_brokers,
        manual_leaderships, placement_failure_message, site_broker_views,
        validate_manual_partition_assignment,
    },
};
use crate::{
    api_catalog::UnstableApiVersions,
    authorizer::AuthorizationResult,
    broker::Broker,
    codes,
    config::BrokerConfig,
    handlers::{
        RequestContext,
        describe_configs::{effective_topic_configs, static_settings},
        request_names::duplicate_names as find_duplicate_names,
    },
    metadata_source::MetadataSource,
    wal::quorum::placement::select_voters_from_sorted_racks,
};

/// Leader epoch that a freshly created partition starts at. The committed
/// `PartitionRecord` and the handler-side leader-cache install must agree.
pub(crate) const INITIAL_LEADER_EPOCH: i32 = 0;

pub(crate) fn diskless_wal_placement_error(
    image: &krabka_metadata::MetadataImage,
    config: &BrokerConfig,
    first_partition: i32,
    leaderships: &[InitialLeadership],
) -> Option<String> {
    let mut brokers = image
        .brokers()
        .map(|broker| (broker.node_id, broker.rack.clone()))
        .collect::<Vec<_>>();
    if brokers.is_empty() {
        brokers.push((config.node_id, config.rack.clone()));
    }
    brokers.sort_by_key(|(node_id, _)| node_id.0);

    let required = config.diskless_wal_local_replica_count;
    leaderships
        .iter()
        .enumerate()
        .find_map(|(offset, leadership)| {
            let leader = leadership.leader;
            let available = select_voters_from_sorted_racks(&brokers, leader, required).len();
            (available != required).then(|| {
                let partition =
                    first_partition.saturating_add(i32::try_from(offset).unwrap_or(i32::MAX));
                format!(
                    "diskless WAL partition {partition} leader {} has {available} eligible \
                     rack-distinct voters, but {required} are required; configure `broker.rack` \
                     on every voter and provide at least {required} distinct racks",
                    leader.0
                )
            })
        })
}

context_handler! {
    #[allow(clippy::too_many_lines)]
    CreateTopicsRequest => CreateTopicsResponse,
    (broker, req, version, ctx),
    {
        // ── ACL preamble ────────────────────────────────────────
        let image = broker.controller.current_image();

        // Kafka removes duplicate names from the request before authorizing
        // (`ControllerApis.createTopics`): every row that shares a duplicated
        // name leaves the request, and the name answers one INVALID_REQUEST row
        // after the controller's rows.
        let duplicate_names = duplicate_names(&req.topics);

        // Cluster `Create` is a shortcut; on Deny, `authorize_create_topics`
        // falls back to topic-level `Create` per surviving name, so a principal
        // with only a topic-scoped ACL can still create the topics it covers.
        // Duplicate and protected names are excluded here exactly as Kafka
        // excludes them from `allowedTopicNames` before authorizing.
        let candidate_names: Vec<&str> = req
            .topics
            .iter()
            .map(|topic| topic.name.as_str())
            .filter(|name| {
                !duplicate_names.iter().any(|duplicate| duplicate == *name)
                    && *name != CLUSTER_METADATA_TOPIC
            })
            .collect();
        let denied_names: std::collections::HashSet<String> =
            authorize_create_topics(broker, &image, ctx, candidate_names.iter().copied())
                .into_iter()
                .filter(|(_, result)| *result == AuthorizationResult::Deny)
                .map(|(name, _)| name.to_owned())
                .collect();

        // Kafka appends the rows the request answers without the controller
        // after the controller's own rows: one per duplicated name, then the
        // protected raft metadata topic and the topics denied `Create`. The
        // controller sees the rest, in request order.
        let mut trailing: Vec<CreatableTopicResult> = duplicate_names
            .iter()
            .map(|name| {
                topic_error_result(
                    name.clone(),
                    codes::INVALID_REQUEST,
                    Some("Duplicate topic name.".into()),
                )
            })
            .collect();
        let mut effective: Vec<CreatableTopic> = Vec::with_capacity(req.topics.len());
        for topic in &req.topics {
            if duplicate_names.contains(&topic.name) {
                continue;
            }
            if topic.name == CLUSTER_METADATA_TOPIC {
                trailing.push(topic_error_result(
                    topic.name.clone(),
                    codes::INVALID_REQUEST,
                    Some(format!(
                        "Creation of internal topic {CLUSTER_METADATA_TOPIC} is prohibited."
                    )),
                ));
            } else if denied_names.contains(&topic.name) {
                // Kafka answers a denial TOPIC_AUTHORIZATION_FAILED with this
                // exact message, never CLUSTER_AUTHORIZATION_FAILED: the cluster
                // check is only a shortcut past the per-topic lookup.
                trailing.push(topic_error_result(
                    topic.name.clone(),
                    codes::TOPIC_AUTHORIZATION_FAILED,
                    Some("Authorization failed.".into()),
                ));
            } else {
                effective.push(topic.clone());
            }
        }

        // Kafka's `validateTotalNumberOfPartitions` refuses the whole request
        // when the topics the controller sees add up to more partitions than one
        // metadata batch may carry. The failure answers every requested row, as
        // `CreateTopicsRequest.getErrorResponse` does, and creates nothing.
        if total_partitions(&effective, broker.config.num_partitions) > MAX_PARTITIONS_PER_REQUEST {
            let message = too_many_partitions_message(broker.config.features.unstable_api_versions);
            let results = req
                .topics
                .iter()
                .map(|topic| {
                    topic_error_result(
                        topic.name.clone(),
                        codes::POLICY_VIOLATION,
                        Some(message.into()),
                    )
                })
                .collect();
            return Ok(create_topics_response(results, 0));
        }

        // KIP-599: Kafka's controller charges each topic with the partitions it
        // creates, after every other check on it has passed
        // (`ReplicationControlManager.createTopic`). A strict version (v6+)
        // refuses the topic that finds the bucket negative, and every topic
        // after it.
        let mut quota = ctx.controller_mutation_quota(broker, &image, version >= 6);

        let mut results: Vec<CreatableTopicResult> = Vec::with_capacity(req.topics.len());
        // KIP-108: a validate-only request runs every check and commits nothing,
        // so the policy below sees it exactly as it sees a committing one.
        let validate_only = req.validate_only;
        let creation = TopicCreation::new(broker, &image, validate_only);

        for topic_req in effective {
            let created = match creation.create(topic_req, &mut quota).await {
                Ok(created) => created,
                Err(failure) => {
                    results.push(*failure);
                    continue;
                }
            };
            let mut result = CreatableTopicResult {
                name: created.name,
                topic_id: ProtoUuid(created.topic_id.into_bytes()),
                error_code: codes::NONE,
                // Kafka sets the message to null on a created row
                // (`ReplicationControlManager.createTopic`), not to the
                // empty string the generated default carries.
                error_message: None,
                ..Default::default()
            };
            disclose_created_topic(
                broker,
                ctx,
                &image,
                version,
                &CreatedTopic {
                    controller: &broker.controller,
                    assignments: &created.assignments,
                    overrides: &created.overrides,
                },
                &mut result,
            );
            results.push(result);
        }
        results.extend(trailing);

        Ok(finish_response(
            broker,
            ctx,
            results,
            validate_only,
            quota.delay(),
        ))
    }
}

/// The most partitions one `CreateTopics` request may create. Kafka 4.3.1
/// fixes it at `MAX_PARTITIONS_PER_BATCH = 10_000`. Trunk reads it from
/// `controller.max.records.per.batch` (KAFKA-20976, default 10000), which
/// krabka does not expose, so the limit is 10000 in both modes.
const MAX_PARTITIONS_PER_REQUEST: u64 = 10_000;

/// The message of Kafka 4.3.1's `validateTotalNumberOfPartitions` refusal.
const TOO_MANY_PARTITIONS_4_3_1: &str = "Excessively large number of partitions per request.";

/// The message of trunk's `validateTotalNumberOfPartitions` refusal, which
/// KAFKA-20976 reworded. It is served only with `unstable.api.versions.enable`,
/// like the other behavior that trunk has and 4.3.1 does not.
const TOO_MANY_PARTITIONS_TRUNK: &str = "Too many partitions in request.";

/// The message of the whole-request refusal for too many partitions.
fn too_many_partitions_message(unstable: UnstableApiVersions) -> &'static str {
    match unstable {
        UnstableApiVersions::Enabled => TOO_MANY_PARTITIONS_TRUNK,
        UnstableApiVersions::Disabled => TOO_MANY_PARTITIONS_4_3_1,
    }
}

/// Kafka's `Errors.THROTTLING_QUOTA_EXCEEDED.message()`, which
/// `StrictControllerMutationQuota.record` puts on the exception.
const THROTTLING_QUOTA_EXCEEDED_MESSAGE: &str = "The throttling quota has been exceeded.";

/// The names that more than one request row carries, in the order of their
/// first row.
fn duplicate_names(topics: &[CreatableTopic]) -> Vec<String> {
    find_duplicate_names(topics.iter().map(|topic| topic.name.as_str()))
}

/// The partitions a request asks for, as Kafka's
/// `validateTotalNumberOfPartitions` counts them: an assignment list counts
/// its length, -1 counts `num.partitions`, and a count that is not positive
/// counts nothing.
fn total_partitions(topics: &[CreatableTopic], default_num_partitions: i32) -> u64 {
    topics
        .iter()
        .map(|topic| {
            let count = if !topic.assignments.is_empty() {
                i64::try_from(topic.assignments.len()).unwrap_or(i64::MAX)
            } else if topic.num_partitions == -1 {
                i64::from(default_num_partitions)
            } else {
                i64::from(topic.num_partitions.max(0))
            };
            u64::try_from(count).unwrap_or(0)
        })
        .fold(0, u64::saturating_add)
}

/// Kafka's row for a topic that already exists: zero topic id, and the
/// message `ReplicationControlManager.createTopics` gives it.
fn topic_exists_result(name: String) -> CreatableTopicResult {
    let message = format!("Topic '{name}' already exists.");
    topic_error_result(name, codes::TOPIC_ALREADY_EXISTS, Some(message))
}

/// Kafka's refusal of a requested topic shape without a manual assignment:
/// `INVALID_REPLICATION_FACTOR` for a replication factor of 0 or below -1,
/// else `INVALID_PARTITIONS` for a partition count of 0 or below -1. The
/// messages are Kafka's.
fn invalid_topic_shape(
    topic: &krabka_protocol::owned::create_topics_request::CreatableTopic,
) -> Option<(i16, &'static str)> {
    if topic.replication_factor < -1 || topic.replication_factor == 0 {
        Some((
            codes::INVALID_REPLICATION_FACTOR,
            "Replication factor must be larger than 0, or -1 to use the default value.",
        ))
    } else if topic.num_partitions < -1 || topic.num_partitions == 0 {
        Some((
            codes::INVALID_PARTITIONS,
            "Number of partitions was set to an invalid non-positive value.",
        ))
    } else {
        None
    }
}

/// KIP-464: a requested value of -1 means the broker default.
fn resolve_default<T: From<i8> + PartialEq>(requested: T, default: T) -> T {
    if requested == T::from(-1) {
        default
    } else {
        requested
    }
}

/// Fill in what KIP-525 discloses about a topic the create just made: its
/// partition count, its replication factor and, on v5+, its whole effective
/// configuration.
///
/// Split out of [`handle`] because the disclosure is one decision with two
/// outcomes -- told or withheld -- and reads none of the create's own state
/// beyond the row it fills.
/// What the create decided about one topic, as the KIP-525 disclosure reads
/// it: where to resolve the effective configuration from, the replica
/// assignment the row's counts come from, and the override map the request
/// carried.
struct CreatedTopic<'a> {
    controller: &'a std::sync::Arc<dyn MetadataSource>,
    assignments: &'a [Vec<krabka_raft::NodeId>],
    overrides: &'a std::collections::BTreeMap<String, String>,
}

fn disclose_created_topic(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    image: &krabka_metadata::MetadataImage,
    version: i16,
    created: &CreatedTopic<'_>,
    result: &mut CreatableTopicResult,
) {
    // KIP-525 (v5+): the row carries what the topic was created as, so
    // a client needs no follow-up DescribeConfigs. Kafka gates the
    // whole disclosure -- the effective configs, the partition count
    // and the replication factor -- on `DescribeConfigs` on
    // `Topic(name)`, and stamps `topicConfigErrorCode` when the
    // principal may not be told. The create itself already happened
    // either way. Below v5 none of those fields are on the wire, so
    // the check is not worth an authorizer call.
    let describable = version < 5 || !describe_configs_denied(broker, image, ctx, &result.name);
    if describable {
        result.num_partitions = i32::try_from(created.assignments.len()).unwrap_or(i32::MAX);
        result.replication_factor = created
            .assignments
            .first()
            .and_then(|replicas| i16::try_from(replicas.len()).ok())
            .unwrap_or(-1);
        if version >= 5 {
            // The overrides the create wrote -- or, on a `validate_only`
            // row, would have written -- resolved against the current
            // image. This is Kafka's
            // `computeEffectiveTopicConfigs(creationConfigs)`: it builds
            // the row from the request's own map, and `validateOnly`
            // discards the records alone.
            result.configs = Some(creatable_topic_configs(effective_topic_configs(
                &created.controller.current_image(),
                broker.config.node_id,
                &result.name,
                created.overrides,
                broker.config.features.unstable_api_versions,
                &static_settings(&broker.config),
            )));
        }
    } else {
        // Kafka leaves the partition count and the replication factor
        // at -1 here too: `AdminClient` fails every accessor on the
        // create result once `topicConfigErrorCode` is set, so a value
        // in either field would never be read.
        result.configs = Some(Vec::new());
        result.topic_config_error_code = codes::TOPIC_AUTHORIZATION_FAILED;
    }
}
