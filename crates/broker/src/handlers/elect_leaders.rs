//! `ElectLeaders` (`api_key` 43, KIP-460).
//!
//! Operator-triggered leader election. PREFERRED type moves leadership
//! back to `replicas[0]` after operator intervention. UNCLEAN type
//! elects outside the ISR when every ISR member is dead.
//!
//! Authorization: `Alter` on `Cluster("kafka-cluster")`. On Deny the
//! whole request answers `CLUSTER_AUTHORIZATION_FAILED (31)` at the top level
//! (v1+) and on every partition row it named.
//!
//! # KFC-9: an unclean election needs two people
//!
//! An unclean election elects a replica that does not hold every committed
//! record, so it is one of the transitions the break-glass two-person rule
//! gates. The request gains no field for it. KIP-460 defines the shape that
//! `kafka-leader-election.sh` sends and there is nowhere in it to name a
//! proposal, so an operator gets the approval out of band through
//! `krabka-guard` and the broker looks it up in its own metadata image.
//!
//! **Preferred election is not gated.** It elects a replica that is already in
//! the ISR, it loses nothing, and gating it would stop routine operation on
//! every cluster that turns the rule on.
//!
//! A refused partition answers `POLICY_VIOLATION` (44) on its own row, with the
//! refusal text in `error_message`. The gate is active only when
//! `[break_glass]` names an approver set, so a stock cluster elects exactly as
//! it does today.

use {
    self::{
        batch::ElectionBatch, env::ElectionEnv, partition::elect_one,
        response::whole_request_error, targets::resolve_targets,
    },
    crate::{
        broker::Broker,
        codes,
        config_keys::witness_node_ids,
        elr::ElrPublisher,
        error::BrokerError,
        handlers::{
            RequestContext, audit_admin_success, audit_resource, cluster_alter_denied,
            decode_request, encode_response,
            forward_to_controller::{
                controller_listener_dispatch, guarded_operation_on_source, is_active_controller,
                to_active_controller,
            },
            offline_replicas::live_brokers,
            stamp_unset, submit_failure_code,
        },
        leader_election::ElectionType,
        metadata_source::MetadataSource,
    },
    bytes::Bytes,
    krabka_audit::AuditError,
    krabka_metadata::{MetadataImage, MetadataRecord},
    krabka_protocol::owned::{
        elect_leaders_request::{self, ElectLeadersRequest},
        elect_leaders_response::{ElectLeadersResponse, ReplicaElectionResult},
    },
    krabka_raft::RaftError,
    std::{future::Future, sync::Arc, time::Duration},
};

mod batch;
mod env;
mod partition;
mod recovery;
mod response;
mod targets;
mod unclean_gate;

#[cfg(test)]
mod tests;

const WIRE_ELECTION_PREFERRED: i8 = 0;
const WIRE_ELECTION_UNCLEAN: i8 = 1;

/// The row message of an authorization refusal. Kafka's `AuthHelper` writes
/// "Request <request> needs ALTER permission.", where `<request>` is the JVM
/// `toString` of the channel request; krabka names the API in its place.
const CLUSTER_ALTER_DENIED_MESSAGE: &str = "Request ElectLeaders needs ALTER permission.";

/// Forward the original operation before selecting leaders from metadata.
pub(crate) async fn dispatch(
    broker: &Broker,
    version: i16,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let request = decode_request::<ElectLeadersRequest>(req_bytes, version)?;
    controller_listener_dispatch!(
        broker,
        ctx,
        request,
        version,
        self::handle,
        self::request_refusal
    );
    if let Some(answer) = to_active_controller(
        broker,
        elect_leaders_request::API_KEY,
        req_bytes,
        version,
        ctx,
        |code, message| encode_response(&request_refusal(&request, code, message), version),
    )
    .await
    {
        return answer;
    }
    if !is_active_controller(broker) {
        return encode_response(
            &request_refusal(&request, codes::NOT_CONTROLLER, None),
            version,
        );
    }
    encode_response(&handle(broker, request, version, ctx).await?, version)
}

fn request_refusal(
    request: &ElectLeadersRequest,
    code: i16,
    message: Option<&str>,
) -> ElectLeadersResponse {
    whole_request_error(request, code, message.unwrap_or_default())
}

context_handler! {
    ElectLeadersRequest => ElectLeadersResponse,
    (broker, req, _version, ctx),
    {
        let budget = Duration::from_millis(req.timeout_ms.max(0).unsigned_abs().into());
        let result = guarded_operation_on_source(
            broker.controller.as_ref(),
            tokio::time::Instant::now() + budget,
            |image| elect_leaders_once(broker, &req, ctx, image),
        ).await;
        Ok(match result {
            Ok(Ok(response)) => response,
            Ok(Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown)) =>
                request_refusal(&req, codes::NOT_CONTROLLER, None),
            Ok(Err(error)) => request_refusal(
                &req,
                submit_failure_code(&error, codes::COORDINATOR_NOT_AVAILABLE),
                Some(&error.to_string()),
            ),
            Err(_) => request_refusal(&req, codes::REQUEST_TIMED_OUT, None),
        })
    }
}

/// Rebuild all targets, authorization, eligibility and proposals on every attempt.
async fn elect_leaders_once(
    broker: &Broker,
    req: &ElectLeadersRequest,
    ctx: &RequestContext<'_>,
    image: Arc<MetadataImage>,
) -> Result<ElectLeadersResponse, RaftError> {
    let denied = cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx);
    let election = match admit(req, denied) {
        Ok(election) => election,
        Err(refusal) => return Ok(refusal),
    };

    // Resolve target partition set:
    //   topic_partitions = None      → every partition in the image
    //   Some([{topic, []}])          → no partition of that topic
    //   Some([{topic, [p, q, ...]}]) → exact set
    let targets = resolve_targets(&image, req);

    // Which brokers an election may elect. Read from replicated state rather
    // than this node's heartbeat registry, because `controllerId` rotates and
    // an `AdminClient` sends the election to whichever broker it last named.
    let alive = live_brokers(broker, &image).await;

    // Run the algorithm per target; accumulate new records to submit
    // and per-partition results to ship back.
    let env = ElectionEnv {
        broker,
        image: &image,
        ctx,
        alive: &alive,
        // Witness nodes never lead a partition. Build the set once for the
        // whole request, not once per target partition.
        witnesses: &witness_node_ids(&image),
        election,
    };
    // KIP-460: a request that named no topics asked about every partition, and
    // Kafka answers such a request only with the partitions it acted on --
    // `ReplicationControlManager.electLeaders` drops every
    // `ELECTION_NOT_NEEDED` row when `topicPartitions` is null, because "we do
    // not return partitions which already have the desired leader". It still
    // answers a row for every topic, with an empty partition list when nothing
    // in it moved. Without the drop, `kafka-leader-election
    // --all-topic-partitions` prints a "valid replica already elected" line for
    // every partition in the cluster, internal topics included, where Kafka
    // prints nothing.
    let elect_all_partitions = req.topic_partitions.is_none();
    let mut by_topic: Vec<ReplicaElectionResult> = Vec::with_capacity(targets.len());
    let mut batch = ElectionBatch::default();
    for (topic, partitions) in targets {
        let mut partition_result = Vec::with_capacity(partitions.len());
        for p in partitions {
            let row = elect_one(&env, &mut batch, &topic, p).await;
            if elect_all_partitions && row.error_code == codes::ELECTION_NOT_NEEDED {
                continue;
            }
            partition_result.push(row);
        }
        by_topic.push(ReplicaElectionResult {
            topic,
            partition_result,
            ..Default::default()
        });
    }

    // KIP-966: an election decides which replicas are still known to hold
    // every committed record, so the ELR state rides the batch the election
    // records go out in.
    ElrPublisher::new(&image).extend(&mut batch.records);

    // Retain the planning image across audit admission. A changed image,
    // controller, or pending tail rejects this batch without forwarding
    // records selected under obsolete authority.
    let mut submit_failure = None;
    if !batch.records.is_empty() {
        let records = std::mem::take(&mut batch.records);
        let admission = submit_election_after_audit(
            broker.controller.as_ref(),
            Arc::clone(&image),
            records,
            batch.require_audit(broker, ctx),
        )
        .await;
        let failure = match admission {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some((
                codes::POLICY_VIOLATION,
                format!("privileged action refused: {error}"),
            )),
            Err(error @ (RaftError::StaleMetadataImage | RaftError::UncommittedTail)) => {
                batch.audit_applied(broker, ctx, Some(&error.to_string()));
                return Err(error);
            }
            Err(error) => Some((
                match error {
                    RaftError::NotLeader { .. } | RaftError::LeaderUnknown => codes::NOT_CONTROLLER,
                    _ => submit_failure_code(&error, codes::COORDINATOR_NOT_AVAILABLE),
                },
                format!("submit failed: {error}"),
            )),
        };
        if let Some((code, failure)) = failure {
            tracing::warn!(error = %failure, "elect-leaders submit refused or failed");
            stamp_unset(
                by_topic
                    .iter_mut()
                    .flat_map(|topic| &mut topic.partition_result),
                code,
                &failure,
            );
            submit_failure = Some(failure);
        }
    }
    // KFC-9: audit the approvals this append spent, now that its outcome is
    // known. An `applied` event for a transition that never committed would be
    // a false record of a data-losing election.
    batch.audit_applied(broker, ctx, submit_failure.as_deref());

    // The gated transitions above audit themselves as `PrivilegedAction`. An
    // ordinary preferred election spends no approval and would otherwise leave
    // no record, so every partition that actually moved is audited here.
    audit_admin_success(
        broker.audit_log.as_ref(),
        ctx,
        "ElectLeaders",
        by_topic
            .iter()
            .flat_map(|topic| {
                topic
                    .partition_result
                    .iter()
                    .filter(|row| row.error_code == codes::NONE)
                    .map(move |row| {
                        audit_resource("Partition", format!("{}-{}", topic.topic, row.partition_id))
                    })
            })
            .collect(),
    );

    Ok(ElectLeadersResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        replica_election_results: by_topic,
        ..Default::default()
    })
}

/// Admission can await durable audit storage; append only against its original image.
async fn submit_election_after_audit(
    controller: &dyn MetadataSource,
    image: Arc<MetadataImage>,
    records: Vec<MetadataRecord>,
    admission: impl Future<Output = Result<(), AuditError>> + Send,
) -> Result<Result<(), AuditError>, RaftError> {
    if let Err(error) = admission.await {
        return Ok(Err(error));
    }
    controller
        .submit_change_if_image(records, image)
        .await
        .map(|_| Ok(()))
}

/// Admit a request, or answer the refusal of the whole of it.
///
/// Kafka's `ControllerApis.handleElectLeaders` authorizes `Alter` on the
/// cluster first and throws `ClusterAuthorizationException`; the controller
/// then reads the election type and throws `InvalidRequestException` for one
/// it does not know. Either exception becomes
/// `ElectLeadersRequest.getErrorResponse`, which [`whole_request_error`]
/// mirrors.
fn admit(req: &ElectLeadersRequest, denied: bool) -> Result<ElectionType, ElectLeadersResponse> {
    if denied {
        return Err(whole_request_error(
            req,
            codes::CLUSTER_AUTHORIZATION_FAILED,
            CLUSTER_ALTER_DENIED_MESSAGE,
        ));
    }
    match req.election_type {
        WIRE_ELECTION_PREFERRED => Ok(ElectionType::Preferred),
        WIRE_ELECTION_UNCLEAN => Ok(ElectionType::Unclean),
        unknown => Err(whole_request_error(
            req,
            codes::INVALID_REQUEST,
            &format!("Unknown election type {unknown}"),
        )),
    }
}
