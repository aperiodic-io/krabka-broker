//! `CreatePartitions` (`api_key=37`), which serves
//! `kafka-topics --alter --partitions N`.
//!
//! When the caller omits `assignments`, the automatic replica placement
//! matches the `CreateTopics` path: [`crate::site_placement`] spreads the
//! replicas over the sites and pins leadership to the preferred site. An
//! explicit, validated `assignments` list, with one entry per *new*
//! partition, overrides the placement, and the handler uses it verbatim. That
//! matches the JVM flow
//! `kafka-topics --alter --partitions N --replica-assignment 0:1,1:2,...`.
//!
//! This file holds the request loop. Each stage it walks through lives in its
//! own submodule: `admission` for the duplicate and authorization preamble,
//! `assignment` for the replica placement, `apply` for the metadata records
//! and the local materialization, and `response` for the response and the
//! throttle.

use krabka_protocol::owned::{
    create_partitions_request::CreatePartitionsRequest,
    create_partitions_response::{CreatePartitionsResponse, CreatePartitionsTopicResult},
};
use krabka_raft::RaftError;

mod admission;
mod apply;
mod assignment;
mod response;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::{
    admission::duplicate_names, apply::partition_records,
    assignment::resolve_new_partition_assignments, response::finish_response,
};
use crate::{
    codes,
    config_keys::resolve_preferred_leader_site,
    handlers::{
        create_topics::{
            automatic_leaderships, automatic_placement_exclusions, diskless_wal_placement_error,
            inactive_brokers, manual_leaderships, site_broker_views,
        },
        partition_materialization::PartitionMaterialization,
    },
    quota::ControllerMutationQuota,
    site_placement::PlacementRng,
};

context_handler! {
    CreatePartitionsRequest => CreatePartitionsResponse,
    (broker, req, version, ctx),
    {
        let node_id = broker.config.node_id;
        let log_dirs = broker.config.all_log_dirs();

        let image = broker.controller.current_image();

        // Kafka's `ControllerApis.createPartitions` answers each duplicated name
        // once with INVALID_REQUEST and grows none of its rows. Growing the same
        // topic twice from one image would write its new partitions twice.
        let duplicates = duplicate_names(&req.topics);
        let mut results: Vec<CreatePartitionsTopicResult> = duplicates
            .iter()
            .map(|name| CreatePartitionsTopicResult {
                name: name.clone(),
                error_code: codes::INVALID_REQUEST,
                error_message: Some("Duplicate topic name.".into()),
                ..Default::default()
            })
            .collect();

        // ── ACL preamble ────────────────────────────────────────
        // Batch-authorize every other topic name for `Alter`. Kafka answers the
        // denied names next, with no message, and hands the rest to the
        // controller, whose rows come last.
        let names: Vec<&str> = req
            .topics
            .iter()
            .map(|topic| topic.name.as_str())
            .filter(|name| !duplicates.iter().any(|duplicate| duplicate == name))
            .collect();
        let denied_topics = crate::handlers::denied_topics(
            broker.config.authorizer.as_ref(),
            &image,
            ctx,
            krabka_metadata::AclOperation::Alter,
            names.iter().copied(),
        );
        results.extend(
            names
                .iter()
                .filter(|name| denied_topics.contains(**name))
                .map(|name| CreatePartitionsTopicResult {
                    name: (*name).to_owned(),
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    ..Default::default()
                }),
        );

        // KIP-599: Kafka's controller charges each topic with the partitions it
        // adds, after the count checks on it pass (`createPartitions`). A strict
        // version (v3+) refuses the topic that finds the bucket negative, and
        // every topic after it.
        let mut quota = ctx.controller_mutation_quota(broker, &image, version >= 3);

        let preferred_site = resolve_preferred_leader_site(&image);
        let validate_only = req.validate_only;

        for t in req.topics {
            if duplicates.contains(&t.name) || denied_topics.contains(&t.name) {
                continue;
            }
            let mut out = CreatePartitionsTopicResult {
                name: t.name.clone(),
                ..Default::default()
            };

            let topic_rec = match admit_growth(&t, &image, &mut quota) {
                Ok(topic_rec) => topic_rec,
                Err((error_code, error_message)) => {
                    out.error_code = error_code;
                    out.error_message = error_message;
                    results.push(out);
                    continue;
                }
            };
            let existing = topic_rec.partitions;
            let diskless = crate::config_keys::resolve_diskless(image.topic_config(&t.name));
            let new_partition_indices: Vec<i32> = (existing..t.count).collect();
            let new_partition_count = new_partition_indices.len();

            // The automatic placement leaves out a broker that is in controlled
            // shutdown and not fenced, and one whose log directories are all
            // cordoned (KIP-1066). It takes a fenced broker only as a last resort,
            // as Kafka's placer does. A broker that a controlled shutdown stopped
            // is fenced, so it is such a last resort. The ISR below leaves that
            // broker out. A manual assignment may name any registered broker,
            // because Kafka 4.3.1 checks only that the broker is registered, and
            // the ISR again leaves out the inactive ones.
            let unavailable =
                crate::handlers::offline_replicas::unavailable_brokers(broker, &image).await;
            let inactive = inactive_brokers(&image, &unavailable);
            let no_exclusion = std::collections::HashSet::new();
            let unusable = automatic_placement_exclusions(&image, &unavailable);
            let brokers = site_broker_views(
                &image,
                broker.config.is_broker().then_some(node_id),
                if t.assignments.is_some() {
                    &no_exclusion
                } else {
                    &unusable
                },
                &unavailable,
            );
            let rf = topic_rec.replication_factor;
            let new_assignments = match resolve_new_partition_assignments(
                t.assignments.as_ref(),
                &brokers,
                new_partition_count,
                rf,
                preferred_site,
                &mut PlacementRng::from_entropy(),
            ) {
                Ok(a) => a,
                Err((code, msg)) => {
                    out.error_code = code;
                    out.error_message = Some(msg);
                    results.push(out);
                    continue;
                }
            };

            let leaderships = if t.assignments.is_some() {
                match manual_leaderships(
                    &new_assignments,
                    &inactive,
                    &crate::config_keys::witness_node_ids(&image),
                    existing,
                ) {
                    Ok(leaderships) => leaderships,
                    Err(message) => {
                        out.error_code = codes::INVALID_REPLICA_ASSIGNMENT;
                        out.error_message = Some(message);
                        results.push(out);
                        continue;
                    }
                }
            } else {
                automatic_leaderships(&new_assignments, &inactive)
            };

            if diskless
                && let Some(reason) =
                    diskless_wal_placement_error(&image, &broker.config, existing, &leaderships)
            {
                out.error_code = codes::INVALID_CONFIG;
                out.error_message = Some(reason);
                results.push(out);
                continue;
            }

            if validate_only {
                results.push(out);
                continue;
            }

            // Build batch: one V1Partition per new index. Under KIP-631 framing the
            // topic's partition count IS the number of PartitionRecords (the
            // `TopicRecord` carries no count), so CreatePartitions appends only the
            // new partition records — no `V1Topic` rewrite. The image derives the
            // grown count from the partitions map as these apply. (Re-submitting a
            // `V1Topic` would round-trip back to the pre-grow count and be rejected
            // by the strict-expansion `validate` on the apply path.)
            let records = partition_records(
                &t.name,
                &new_partition_indices,
                &new_assignments,
                &leaderships,
            );

            match broker.controller.submit_change(records).await {
                Ok(_) => {
                    PartitionMaterialization {
                        broker,
                        log_dirs: &log_dirs,
                        diskless,
                        topic_id: topic_rec.topic_id,
                    }
                    .materialize(
                        "CreatePartitions",
                        &t.name,
                        new_partition_indices.iter().copied(),
                        &new_assignments,
                        &leaderships,
                    )
                    .await;
                }
                Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                    out.error_code = codes::NOT_CONTROLLER;
                }
                Err(e) => {
                    tracing::error!(topic = %t.name, error = %e,
                        "CreatePartitions submit_change failed");
                    out.error_code = codes::UNKNOWN_SERVER_ERROR;
                }
            }

            results.push(out);
        }

        // A `--dry-run` request grows nothing, so it changed no topic.
        if !validate_only {
            crate::handlers::audit_admin_success(
                broker.audit_log.as_ref(),
                ctx,
                "CreatePartitions",
                results
                    .iter()
                    .filter(|result| result.error_code == codes::NONE)
                    .map(|result| crate::handlers::audit_resource("Topic", result.name.clone()))
                    .collect(),
            );
        }

        // KIP-599: report the controller_mutation_rate throttle after response
        // assembly. It sets throttle_time_ms and records the window for the
        // connection loop's post-send mute (KIP-219).
        Ok(finish_response(ctx, quota.delay(), results))
    }
}

/// Kafka's `ReplicationControlManager.createPartitions` checks on one
/// authorized row ahead of the placement, in its order: the topic exists, the
/// count grows it, an explicit assignment list holds one entry per new
/// partition, and the KIP-599 quota admits the new partitions. The topic on
/// success, else the row's code and Kafka's message.
fn admit_growth(
    topic: &krabka_protocol::owned::create_partitions_request::CreatePartitionsTopic,
    image: &krabka_metadata::MetadataImage,
    quota: &mut ControllerMutationQuota,
) -> Result<krabka_metadata::TopicRecord, (i16, Option<String>)> {
    // Kafka's `UnknownTopicOrPartitionException()` carries no message.
    let topic_rec = image
        .topic(&topic.name)
        .cloned()
        .ok_or((codes::UNKNOWN_TOPIC_OR_PARTITION, None))?;
    let existing = topic_rec.partitions;
    let count = topic.count;
    let invalid = |message| Err((codes::INVALID_PARTITIONS, Some(message)));
    match count.cmp(&existing) {
        std::cmp::Ordering::Equal => {
            return invalid(format!("Topic already has {existing} partition(s)."));
        }
        std::cmp::Ordering::Less => {
            return invalid(format!(
                "The topic {} currently has {existing} partition(s); {count} would not be an \
                 increase.",
                topic.name
            ));
        }
        std::cmp::Ordering::Greater => {}
    }
    let additional = i64::from(count) - i64::from(existing);
    if let Some(assignments) = &topic.assignments
        && i64::try_from(assignments.len()).ok() != Some(additional)
    {
        return Err((
            codes::INVALID_REPLICA_ASSIGNMENT,
            Some(format!(
                "Attempted to add {additional} additional partition(s), but only {} \
                 assignment(s) were specified.",
                assignments.len()
            )),
        ));
    }
    // KIP-599: charge the partitions this topic adds.
    if quota
        .record(u64::try_from(additional).unwrap_or(0))
        .is_err()
    {
        return Err((
            codes::THROTTLING_QUOTA_EXCEEDED,
            Some("The throttling quota has been exceeded.".into()),
        ));
    }
    Ok(topic_rec)
}
