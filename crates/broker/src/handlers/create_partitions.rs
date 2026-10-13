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

use {
    crate::{
        error::BrokerError,
        handlers::{ApiVersion, DispatchResult, audit_admin_success, audit_resource},
    },
    krabka_metadata::{AclOperation, MetadataImage, TopicRecord},
    krabka_protocol::owned::{
        create_partitions_request::{self, CreatePartitionsRequest, CreatePartitionsTopic},
        create_partitions_response::{CreatePartitionsResponse, CreatePartitionsTopicResult},
    },
    krabka_raft::{NodeId, RaftError},
    krabka_units::Time,
    std::{
        collections::HashSet,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    },
    tokio::sync::Mutex,
};

mod admission;
mod apply;
mod assignment;
mod response;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use {
    self::{
        admission::duplicate_names,
        apply::partition_records,
        assignment::{PlacementRefusal, resolve_new_partition_assignments},
        response::finish_response,
    },
    crate::{
        broker::Broker,
        codes,
        config_keys::{resolve_diskless, resolve_preferred_leader_site, witness_node_ids},
        handlers::{
            RequestContext,
            create_topics::{
                InitialLeadership, automatic_leaderships, automatic_placement_exclusions,
                diskless_wal_placement_error, inactive_brokers, manual_leaderships,
                site_broker_views,
            },
            denied_topics, encode_response,
            forward_to_controller::{
                forward_if_not_controller, guarded_operation_on_source, is_active_controller,
            },
            offline_replicas::unavailable_brokers,
            partition_materialization::PartitionMaterialization,
        },
        quota::ControllerMutationQuota,
        site_placement::PlacementRng,
    },
};

krabka_macros::decoded_dispatch! {
    /// Apply explicit assignments and placement rules after the request reaches the controller.
    pub(crate) async fn dispatch(
        broker: &Broker,
        version: ApiVersion,
        req_bytes: &[u8],
        ctx: &RequestContext<'_>,
    )
        [request = CreatePartitionsRequest, binding(req)] -> DispatchResult
    {
        if let Some(answer) = forward_if_not_controller(
            broker, ctx, create_partitions_request::API_KEY, req_bytes, version, |code, message| encode_response(&request_refusal(&req, code, message), version)
        ).await {
            return answer;
        }
        if !is_active_controller(broker) {
            return encode_response(&request_refusal(&req, codes::NOT_CONTROLLER, None), version);
        }
        encode_response(&handle(broker, req, version, ctx).await?, version)
    }
}

fn request_refusal(
    req: &CreatePartitionsRequest,
    code: i16,
    message: Option<&str>,
) -> CreatePartitionsResponse {
    CreatePartitionsResponse {
        results: req
            .topics
            .iter()
            .map(|t| CreatePartitionsTopicResult {
                name: t.name.clone(),
                error_code: code,
                error_message: message.map(str::to_owned),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

pub(crate) async fn handle(
    broker: &Broker,
    req: CreatePartitionsRequest,
    version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<CreatePartitionsResponse, BrokerError> {
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
    let denied_topics = denied_topics(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        AclOperation::Alter,
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
    let quota = Mutex::new(ctx.controller_mutation_quota(broker, &image, version >= 3));

    let deadline = tokio::time::Instant::now()
        + Duration::from_millis(u64::try_from(req.timeout_ms).unwrap_or(0));
    let validate_only = req.validate_only;

    for t in req.topics {
        if duplicates.contains(&t.name) || denied_topics.contains(&t.name) {
            continue;
        }
        let prior_delay = quota.lock().await.delay();
        let growth = GrowthContext {
            broker,
            ctx,
            quota: &quota,
            charged: AtomicU64::new(0),
            prior_delay,
            validate_only,
        };
        let attempt = guarded_operation_on_source(broker.controller.as_ref(), deadline, |image| {
            grow_topic(&growth, &t, image)
        })
        .await;
        let out = match attempt {
            Ok(Ok(row)) => row,
            failure => {
                let code = match failure {
                    Err(_) => codes::REQUEST_TIMED_OUT,
                    Ok(Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown)) => {
                        codes::NOT_CONTROLLER
                    }
                    Ok(Err(error)) => {
                        tracing::error!(%error, topic = %t.name, "CreatePartitions guarded submission failed");
                        codes::UNKNOWN_SERVER_ERROR
                    }
                    Ok(Ok(_)) => unreachable!("successful response handled above"),
                };
                CreatePartitionsTopicResult {
                    name: t.name,
                    error_code: code,
                    ..Default::default()
                }
            }
        };
        results.push(out);
    }

    // A `--dry-run` request grows nothing, so it changed no topic.
    if !validate_only {
        audit_admin_success(
            broker.audit_log.as_ref(),
            ctx,
            "CreatePartitions",
            results
                .iter()
                .filter(|result| result.error_code == codes::NONE)
                .map(|result| audit_resource("Topic", result.name.clone()))
                .collect(),
        );
    }

    // KIP-599: report the controller_mutation_rate throttle after response
    // assembly. It sets throttle_time_ms and records the window for the
    // connection loop's post-send mute (KIP-219).
    Ok(finish_response(ctx, quota.lock().await.delay(), results))
}

struct GrowthContext<'a, 'ctx> {
    broker: &'a Broker,
    ctx: &'a RequestContext<'ctx>,
    quota: &'a Mutex<ControllerMutationQuota>,
    charged: AtomicU64,
    prior_delay: Time,
    validate_only: bool,
}

/// Each conflicting attempt discards its placement and rechecks admission.
async fn grow_topic(
    growth: &GrowthContext<'_, '_>,
    t: &CreatePartitionsTopic,
    image: Arc<MetadataImage>,
) -> Result<CreatePartitionsTopicResult, RaftError> {
    let broker = growth.broker;
    if !is_active_controller(broker) {
        return Err(RaftError::LeaderUnknown);
    }
    if denied_topics(
        broker.config.authorizer.as_ref(),
        &image,
        growth.ctx,
        AclOperation::Alter,
        [t.name.as_str()],
    )
    .contains(&t.name)
    {
        reconcile_growth_charge(growth, 0).await;
        return Ok(CreatePartitionsTopicResult {
            name: t.name.clone(),
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            ..Default::default()
        });
    }
    let mut out = CreatePartitionsTopicResult {
        name: t.name.clone(),
        ..Default::default()
    };

    let topic_rec = {
        match admit_growth(t, &image, None) {
            Ok(topic) => topic,
            Err(GrowthRefusal { code, message }) => {
                reconcile_growth_charge(growth, 0).await;
                out.error_code = code;
                out.error_message = message;
                return Ok(out);
            }
        }
    };
    let existing = topic_rec.partitions;
    let additional = u64::try_from(i64::from(t.count) - i64::from(existing)).unwrap_or(0);
    if !reconcile_growth_charge(growth, additional).await {
        out.error_code = codes::THROTTLING_QUOTA_EXCEEDED;
        out.error_message = Some("The throttling quota has been exceeded.".into());
        return Ok(out);
    }
    let diskless = resolve_diskless(image.topic_config(&t.name));
    let new_partition_indices: Vec<i32> = (existing..t.count).collect();

    let GrowthPlacement {
        assignments: new_assignments,
        leaderships,
    } = match place_growth(broker, t, &image, &topic_rec).await {
        Ok(placement) => placement,
        Err(PlacementRefusal { code, message }) => {
            out.error_code = code;
            out.error_message = Some(message);
            return Ok(out);
        }
    };

    if diskless
        && let Some(reason) =
            diskless_wal_placement_error(&image, &broker.config, existing, &leaderships)
    {
        out.error_code = codes::INVALID_CONFIG;
        out.error_message = Some(reason);
        return Ok(out);
    }

    if growth.validate_only {
        return Ok(out);
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

    broker
        .controller
        .submit_change_if_image(records, image.clone())
        .await?;
    PartitionMaterialization {
        broker,
        log_dirs: &broker.config.all_log_dirs(),
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
    Ok(out)
}

async fn reconcile_growth_charge(growth: &GrowthContext<'_, '_>, current: u64) -> bool {
    let mut quota = growth.quota.lock().await;
    let previous = growth.charged.load(Ordering::Relaxed);
    if quota
        .reconcile_record(previous, current, growth.prior_delay)
        .is_err()
    {
        // A refused increment was never submitted, and the earlier stale
        // reservation did not commit. Preserve the refusal's response delay.
        let refusal_delay = quota.delay();
        quota
            .reconcile_record(previous, 0, refusal_delay)
            .expect("refunding a reservation adds no mutations");
        growth.charged.store(0, Ordering::Relaxed);
        return false;
    }
    growth.charged.store(current, Ordering::Relaxed);
    true
}

/// A refusal to grow a topic: Kafka's error code and, where Kafka sends one,
/// its message.
struct GrowthRefusal {
    code: i16,
    message: Option<String>,
}

struct GrowthPlacement {
    assignments: Vec<Vec<NodeId>>,
    leaderships: Vec<InitialLeadership>,
}

/// Placement is rebuilt for every guarded attempt, including broker liveness.
async fn place_growth(
    broker: &Broker,
    t: &CreatePartitionsTopic,
    image: &MetadataImage,
    topic_rec: &TopicRecord,
) -> Result<GrowthPlacement, PlacementRefusal> {
    // The automatic placement leaves out a broker that is in controlled
    // shutdown and not fenced, and one whose log directories are all
    // cordoned (KIP-1066). It takes a fenced broker only as a last resort,
    // as Kafka's placer does. A broker that a controlled shutdown stopped
    // is fenced, so it is such a last resort. The ISR below leaves that
    // broker out. A manual assignment may name any registered broker,
    // because Kafka 4.3.1 checks only that the broker is registered, and
    // the ISR again leaves out the inactive ones.
    let unavailable = unavailable_brokers(broker, image).await;
    let inactive = inactive_brokers(image, &unavailable);
    let no_exclusion = HashSet::new();
    let unusable = automatic_placement_exclusions(image, &unavailable);
    let brokers = site_broker_views(
        image,
        broker.config.is_broker().then_some(broker.config.node_id),
        if t.assignments.is_some() {
            &no_exclusion
        } else {
            &unusable
        },
        &unavailable,
    );
    let rf = topic_rec.replication_factor;
    let preferred_site = resolve_preferred_leader_site(image);
    let new_assignments = resolve_new_partition_assignments(
        t.assignments.as_ref(),
        &brokers,
        usize::try_from(t.count - topic_rec.partitions).unwrap_or(0),
        rf,
        preferred_site,
        &mut PlacementRng::from_entropy(),
    )?;

    let leaderships = if t.assignments.is_some() {
        match manual_leaderships(
            &new_assignments,
            &inactive,
            &witness_node_ids(image),
            topic_rec.partitions,
        ) {
            Ok(leaderships) => leaderships,
            Err(message) => {
                return Err(PlacementRefusal {
                    code: codes::INVALID_REPLICA_ASSIGNMENT,
                    message,
                });
            }
        }
    } else {
        automatic_leaderships(&new_assignments, &inactive)
    };
    Ok(GrowthPlacement {
        assignments: new_assignments,
        leaderships,
    })
}

/// Kafka's `ReplicationControlManager.createPartitions` checks on one
/// authorized row ahead of the placement, in its order: the topic exists, the
/// count grows it, an explicit assignment list holds one entry per new
/// partition, and the KIP-599 quota admits the new partitions. The topic on
/// success, else the row's code and Kafka's message.
fn admit_growth(
    topic: &CreatePartitionsTopic,
    image: &MetadataImage,
    quota: Option<&mut ControllerMutationQuota>,
) -> Result<TopicRecord, GrowthRefusal> {
    // Kafka's `UnknownTopicOrPartitionException()` carries no message.
    let topic_rec = image.topic(&topic.name).cloned().ok_or(GrowthRefusal {
        code: codes::UNKNOWN_TOPIC_OR_PARTITION,
        message: None,
    })?;
    let existing = topic_rec.partitions;
    let count = topic.count;
    let invalid = |message| {
        Err(GrowthRefusal {
            code: codes::INVALID_PARTITIONS,
            message: Some(message),
        })
    };
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
        return Err(GrowthRefusal {
            code: codes::INVALID_REPLICA_ASSIGNMENT,
            message: Some(format!(
                "Attempted to add {additional} additional partition(s), but only {} \
                 assignment(s) were specified.",
                assignments.len()
            )),
        });
    }
    // KIP-599: charge the partitions this topic adds.
    if let Some(quota) = quota
        && quota
            .record(u64::try_from(additional).unwrap_or(0))
            .is_err()
    {
        return Err(GrowthRefusal {
            code: codes::THROTTLING_QUOTA_EXCEEDED,
            message: Some("The throttling quota has been exceeded.".into()),
        });
    }
    Ok(topic_rec)
}
