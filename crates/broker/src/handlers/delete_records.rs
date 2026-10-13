//! `DeleteRecords` (`api_key=21`). The leader trims its local segments, and
//! the row waits until every live follower reports a log start at or above
//! the trim point, as Kafka's `DeleteRecords` purgatory does.
//!
//! Each follower raises its own log start to the leader's `log_start_offset`
//! from its next Fetch response, and reports it in the Fetch after that. The
//! `low_watermark` module holds the wait.
//!
//! A trim is bounded by the current high watermark and, on a topic that
//! schedules delivery, by the partition's delivery watermark. See
//! [`offsets::trim_decision`].
//!
//! # KFC-9: trimming a partition needs two people
//!
//! A trim destroys committed records, so it is one of the transitions the
//! break-glass two-person rule gates. KIP-107 defines the request and it gains
//! no field for this: an operator gets an approval out of band through
//! `krabka-guard`, targeted at `"<topic>-<partition>"` or at the bare topic
//! name, and then runs `kafka-delete-records`.
//!
//! # KFC-9: a frozen partition is never trimmed
//!
//! A write freeze refuses every operation that removes data from the topic it
//! covers, so a frozen partition answers `POLICY_VIOLATION (44)` here whatever
//! the caller holds. The check runs ahead of the two-person rule, because an
//! approval to trim does not defeat a freeze and a refusal must not spend one.
//!
//! A refused partition answers `POLICY_VIOLATION (44)` on its own row. No
//! version of the `DeleteRecords` response carries an `error_message`, so the
//! refusal text reaches the operator through the audit log and the broker's own
//! log rather than the wire.
//!
//! # The trim outlives the broker
//!
//! A trim that lands on a segment boundary is durable in the segment names
//! alone: the files below it are gone. One that lands inside a segment leaves
//! its records on disk, so krabka-log checkpoints the new log start to
//! `log-start-offset-checkpoint` in the partition directory and reads it back
//! in `Log::open`. Without that, a restart would serve records an operator
//! deleted for a compliance request, and `ListOffsets EARLIEST` would move back
//! down. Apache Kafka keeps the same value, per log dir rather than per
//! partition directory, on a `log.flush.start.offset.checkpoint.interval.ms`
//! schedule; krabka writes it on the trim itself.
//!
//! This trim writes no metadata record, so there is nothing for the consumed
//! proposal to ride beside. The broker appends the consume on its own and only
//! then trims. Consume-then-transition is the safe order of the two: a crash
//! between them loses the approval, where the reverse order would leave an
//! approval that a second trim could spend again. The gate is active only when
//! `[break_glass]` names an approver set.
//!
//! This file is the module root and holds the request loop and its ACL
//! preamble. The response constructors live in `response`, the offset
//! boundary decisions in `offsets`, and the break-glass gate in `gate`.

use {
    crate::handlers::{ApiVersion, audit_admin_success, audit_resource, denied_topics},
    krabka_audit::PrivilegedPhase,
    krabka_log::Offset,
    krabka_metadata::{AclOperation, BreakGlassAction, MetadataImage},
    krabka_protocol::owned::{
        delete_records_request::{DeleteRecordsPartition, DeleteRecordsRequest},
        delete_records_response::{
            DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
        },
    },
    krabka_units::convert::TimeExt as _,
    krabka_verified::{DeleteRecordsTrimDecision, FreezeMutationKind},
    std::collections::HashSet,
    uuid::Uuid,
};

mod gate;
mod low_watermark;
mod offsets;
mod response;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use {
    self::{
        gate::{authorize_trim, consumed_proposal_id, refuse_trim, spend_approval, trim_target},
        offsets::trim_decision,
        response::{
            delete_records_response, error_partition_result, partition_result, topic_result,
        },
    },
    crate::{
        break_glass::handlers::audit::{GatedTransition, audit_transition, require_transition},
        broker::Broker,
        codes,
        diskless::read::DisklessReadHandle,
        error::BrokerError,
        freeze::resolve::{FreezeMutationResolution, FreezeVerdict, resolve_freeze_mutation},
        handlers::{RequestContext, submit_failure_code},
        internal_topics::is_internal_topic,
        partition::Partition,
        partition_registry::PartitionRegistry,
    },
};

pub(crate) async fn handle(
    broker: &Broker,
    req: DeleteRecordsRequest,
    _version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<DeleteRecordsResponse, BrokerError> {
    let partitions = broker.partitions.clone();

    let image = broker.controller.current_image();

    // ── ACL preamble ────────────────────────────────────────
    // Batch-authorize every topic name for `Delete`. Topics that come
    // back `Deny` short-circuit the trim loop and emit
    // TOPIC_AUTHORIZATION_FAILED on every partition row for that topic.
    let denied_topics = denied_topics(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        AclOperation::Delete,
        req.topics.iter().map(|t| t.name.as_str()),
    );

    let env = TrimEnv {
        broker,
        image: &image,
        ctx,
        partitions: &partitions,
    };
    // KFC-9: the proposals this request already spent. One proposal on a bare
    // topic name covers every partition of it, and it is spent once.
    let mut spent: HashSet<Uuid> = HashSet::new();
    let mut topic_results: Vec<DeleteRecordsTopicResult> = Vec::with_capacity(req.topics.len());
    // The partitions whose deletion frontier actually moved. A stale request
    // and an exact retry at the current log start answer success and delete
    // nothing, so the wire code cannot say which partitions were trimmed.
    let mut trimmed: Vec<krabka_audit::AuditResource> = Vec::new();
    // The rows that trimmed on this leader and wait for the followers.
    let mut waiting: Vec<low_watermark::Waiting> = Vec::new();
    let timeout_ms = req.timeout_ms;

    for topic in req.topics {
        // Per-topic ACL check: if denied, mark every partition in the topic.
        if denied_topics.contains(&topic.name) {
            let part_results: Vec<DeleteRecordsPartitionResult> = topic
                .partitions
                .iter()
                .map(|fp| {
                    error_partition_result(fp.partition_index, codes::TOPIC_AUTHORIZATION_FAILED)
                })
                .collect();
            topic_results.push(topic_result(topic.name, part_results));
            continue;
        }

        let mut part_results: Vec<DeleteRecordsPartitionResult> =
            Vec::with_capacity(topic.partitions.len());

        for fp in topic.partitions {
            let TrimOutcome {
                row,
                deleted,
                waiting_for,
            } = trim_one(&env, &mut spent, &topic.name, &fp).await;
            if let Some(required) = waiting_for {
                waiting.push(low_watermark::Waiting {
                    row: low_watermark::RowPosition {
                        topic: topic_results.len(),
                        partition: part_results.len(),
                    },
                    topic: topic.name.clone(),
                    partition: row.partition_index,
                    required,
                });
            }
            if deleted {
                trimmed.push(audit_resource(
                    "Partition",
                    format!("{}-{}", topic.name, row.partition_index),
                ));
            }
            part_results.push(row);
        }

        topic_results.push(topic_result(topic.name, part_results));
    }

    low_watermark::await_followers(broker, &mut topic_results, waiting, timeout_ms).await;

    // A gated trim audits itself as a `PrivilegedAction`. On a cluster with no
    // approver set the gate is inert, so every partition that was actually
    // trimmed is audited here.
    audit_admin_success(broker.audit_log.as_ref(), ctx, "DeleteRecords", trimmed);

    let resp = delete_records_response(topic_results);
    Ok(resp)
}

/// Record the trim as the partition's diskless `DeleteRecords` floor, durably.
///
/// A diskless partition's local log start is the flusher's trim frontier, not
/// a delete point, so the object tier cannot read the floor off the log. The
/// index topic carries it instead, as a keyed record the projection replays
/// like any other: a cold read below the floor misses from here on,
/// `ListOffsets(EARLIEST)` answers the floor, and the flusher's next tick
/// tombstones every index range that ends below it.
///
/// The range tombstones could not stand in for the record. A range that
/// straddles the floor keeps live records, so retention must not expire it,
/// and neither may the newest range; both would come back out of a replay
/// still covering the offsets below the floor. So this runs *before* the trim
/// is acknowledged, and a failure fails the row: a client told its records are
/// gone must not see them again after a restart or a leadership move.
///
/// A partition with no object tier behind it has nothing to record.
///
/// # Errors
///
/// Returns an error when the floor cannot be published or does not reach the
/// committed projection in time.
async fn publish_diskless_delete_floor(
    env: &TrimEnv<'_>,
    topic: &str,
    part: &Partition,
    floor: Offset,
) -> Result<(), BrokerError> {
    let Some(DisklessIndex { handle, topic_id }) = diskless_index(env, topic, part) else {
        return Ok(());
    };
    // A rebuilding projection is the one case that must not be mistaken for
    // "nothing to record": there is a floor to persist and nowhere to put it.
    let Some(index_log) = handle.index_log() else {
        return Err(BrokerError::Txn(
            "diskless WAL index log is rebuilding; the delete floor cannot be recorded".into(),
        ));
    };
    index_log
        .publish_delete_floor(
            topic_id,
            part.index.get(),
            floor.0,
            env.broker
                .config
                .diskless_wal_index_projection_timeout
                .to_std(),
        )
        .await
}

/// The offset a diskless partition actually starts at: the lower of the local
/// log start and the first offset the object tier still answers for, which is
/// what [`crate::handlers::list_offsets`] reports for `EARLIEST`.
///
/// `None` for a partition with no object tier behind it, which trims against
/// its local log start like any other.
async fn diskless_logical_start(env: &TrimEnv<'_>, topic: &str, part: &Partition) -> Option<i64> {
    let DisklessIndex { handle, topic_id } = diskless_index(env, topic, part)?;
    let covered = handle
        .index
        .lock()
        .await
        .earliest_covered(topic_id, part.index.get())?;
    Some(covered.min(part.log_start_offset().0))
}

/// The diskless read handle and topic id behind a diskless partition.
struct DisklessIndex<'a> {
    handle: &'a DisklessReadHandle,
    topic_id: Uuid,
}

/// The diskless read handle and topic id behind a diskless partition.
fn diskless_index<'a>(
    env: &'a TrimEnv<'_>,
    topic: &str,
    part: &Partition,
) -> Option<DisklessIndex<'a>> {
    if !part.diskless {
        return None;
    }
    env.broker
        .diskless_read
        .as_deref()
        .zip(env.image.topic(topic).map(|topic| topic.topic_id))
        .map(|(handle, topic_id)| DisklessIndex { handle, topic_id })
}

/// Whether the partition's `cleanup.policy` forbids a `DeleteRecords` trim:
/// Kafka's `!config.delete && config.compact`, which is `compact` alone.
/// `compact,delete` trims like `delete`.
fn trim_forbidden_by_policy(part: &Partition) -> bool {
    let policy = part
        .log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .config_snapshot()
        .cleanup_policy;
    policy.contains_compact() && !policy.contains_delete()
}

/// Everything one partition's trim reads, and nothing it writes.
pub(super) struct TrimEnv<'a> {
    pub(super) broker: &'a Broker,
    pub(super) image: &'a MetadataImage,
    pub(super) ctx: &'a RequestContext<'a>,
    pub(super) partitions: &'a PartitionRegistry,
}

/// What trimming one partition answers: the row the response carries for it,
/// whether the trim moved the partition's deletion frontier, and the offset the
/// row still waits for the followers to reach.
struct TrimOutcome {
    row: DeleteRecordsPartitionResult,
    deleted: bool,
    waiting_for: Option<i64>,
}

/// Trim one partition, and answer the row the response carries for it beside
/// whether the trim moved the partition's deletion frontier.
///
/// The two are not the same answer. A stale request, and an exact retry at the
/// current log start, are a `Noop` that deletes nothing and still answers
/// `NONE` on the wire, so the caller reads the second value rather than the
/// error code when it decides what to audit.
///
/// `spent` carries the proposals this request already consumed, so one approval
/// that covers a whole topic is spent once however many partitions the request
/// names.
async fn trim_one(
    env: &TrimEnv<'_>,
    spent: &mut HashSet<Uuid>,
    topic: &str,
    fp: &DeleteRecordsPartition,
) -> TrimOutcome {
    let index = fp.partition_index;
    let refused = |code| TrimOutcome {
        row: error_partition_result(index, code),
        deleted: false,
        waiting_for: None,
    };

    // Kafka's `ReplicaManager.deleteRecordsOnLocalLog` refuses every partition
    // of an internal topic with `INVALID_TOPIC_EXCEPTION` before it looks for
    // the partition. `KafkaApis` answers a partition the metadata does not
    // hold before that, with `UNKNOWN_TOPIC_OR_PARTITION`. A trim of an
    // internal topic deletes committed offsets, transaction state or share
    // state, and a trim of krabka's own internal topics deletes broker state,
    // so both sets are refused.
    // The partition registry is updated apart from this image, so an internal
    // partition the image lacks never falls through to it.
    if is_internal_topic(&env.broker.config, topic) {
        return refused(if env.image.partition(topic, index).is_some() {
            codes::INVALID_TOPIC_EXCEPTION
        } else {
            codes::UNKNOWN_TOPIC_OR_PARTITION
        });
    }
    // `KafkaApis.handleDeleteRecordsRequest` answers a partition the metadata
    // does not hold with `UNKNOWN_TOPIC_OR_PARTITION`. One the metadata holds
    // and this broker does not host is `NOT_LEADER_OR_FOLLOWER`
    // (`ReplicaManager.getPartitionOrError`), which sends the client to
    // refresh its metadata and find the leader rather than give up.
    if env.image.partition(topic, index).is_none() {
        return refused(codes::UNKNOWN_TOPIC_OR_PARTITION);
    }
    let Some(part) = env.partitions.get(topic, krabka_ids::PartitionIndex(index)) else {
        return refused(codes::NOT_LEADER_OR_FOLLOWER);
    };

    let cur_leader = part
        .current_leader
        .load(std::sync::atomic::Ordering::Acquire);
    if cur_leader != env.broker.config.node_id {
        return refused(codes::NOT_LEADER_OR_FOLLOWER);
    }

    // Kafka's `Partition.deleteRecordsOnLeader` refuses a trim on a log whose
    // `cleanup.policy` compacts and does not delete. A compacted topic holds
    // the latest value of each key, and a trim would lose keys. The leader's
    // own log config is the one Kafka reads.
    if trim_forbidden_by_policy(&part) {
        tracing::warn!(
            %topic,
            partition = index,
            "DeleteRecords refused: cleanup.policy=compact does not allow deletion"
        );
        return refused(codes::POLICY_VIOLATION);
    }

    // KFC-9: a write freeze refuses every operation that removes data from the
    // topic it covers, and it answers ahead of the two-person rule. That order
    // is the rule: a break-glass approval to trim does not defeat a freeze, and
    // a trim the freeze refuses must not spend an approval on its way to being
    // refused.
    //
    // No version of the `DeleteRecords` response carries an `error_message`, so
    // the reason reaches the operator through the broker's log alone. Like the
    // produce gate, this refusal emits no privileged-action audit event: a
    // freeze is not a break-glass act, and the registry entry that caused it is
    // already in the metadata log.
    if let FreezeMutationResolution::Frozen(record) =
        resolve_freeze_mutation(env.image, topic, true, FreezeMutationKind::DeleteRecords)
    {
        let verdict = FreezeVerdict::from(record);
        tracing::warn!(
            %topic,
            partition = index,
            refusal = %verdict.removal_message(),
            "DeleteRecords refused by a freeze"
        );
        return refused(codes::POLICY_VIOLATION);
    }

    // KFC-9: the two-person rule answers here, before the broker reads a single
    // offset. The approval is not spent yet: a request that then fails its
    // range check leaves the proposal usable, so an operator does not have to
    // gather two signatures again over a typo.
    let consumed = match authorize_trim(env.image, &env.broker.config.break_glass, topic, index) {
        Ok(consumed) => consumed,
        Err(denial) => {
            return TrimOutcome {
                row: refuse_trim(env, topic, index, &denial),
                deleted: false,
                waiting_for: None,
            };
        }
    };

    let leo = part.log_end_offset();
    let hw = part.high_watermark().await;
    // Recompute delivery under the log mutex against the partition's own
    // clock. This agrees with the cap a fetch at this instant would apply,
    // rather than with whatever the last scheduler sweep published. It
    // answers `None` on a topic that delivers immediately. The verified
    // decision rejects malformed state, preserves stale retries, and caps
    // every admitted target at both HWM and delivery.
    let delivery_watermark = part
        .delivery
        .publish_now(&part.log)
        .map(|delivery| delivery.watermark);
    // A diskless partition's local log start is the flusher's trim frontier:
    // the records below it are in the object store, not gone. The trim has to
    // measure against the offset the partition actually starts at, which is
    // the same one `ListOffsets(EARLIEST)` answers, or every request below the
    // trim frontier would be a no-op that deletes nothing.
    let current_start = diskless_logical_start(env, topic, &part)
        .await
        .map_or_else(|| part.log_start_offset(), Offset);
    // `deletes` is what the audit trail keys on: a `Noop` answers the current
    // start, trims to where the partition already begins, and removes nothing.
    let (target, deletes) =
        match trim_decision(fp.offset, hw, leo, current_start, delivery_watermark) {
            DeleteRecordsTrimDecision::Noop { frontier } => (Offset(frontier), false),
            DeleteRecordsTrimDecision::Apply { frontier } => (Offset(frontier), true),
            DeleteRecordsTrimDecision::RejectMalformed
            | DeleteRecordsTrimDecision::RejectOutOfRange => {
                return refused(codes::OFFSET_OUT_OF_RANGE);
            }
        };

    let audit_target = trim_target(topic, index);
    if let Err(error) = require_transition(
        &env.broker.audit_log,
        &env.broker.config.break_glass,
        env.ctx,
        &GatedTransition {
            action: BreakGlassAction::DeleteRecords,
            target: &audit_target,
            phase: PrivilegedPhase::Applied,
            proposal_id: consumed.as_ref().and_then(consumed_proposal_id),
            reason: "record trim admitted",
        },
    )
    .await
    {
        tracing::warn!(%topic, partition = index, %error, "DeleteRecords refused by audit policy");
        return refused(codes::POLICY_VIOLATION);
    }

    // KFC-9: spend the approval before the trim removes anything.
    let proposal_id = match spend_approval(env.broker, spent, consumed, &audit_target).await {
        Ok(proposal_id) => proposal_id,
        Err(error) => {
            tracing::warn!(
                %topic, partition = index, %error,
                "DeleteRecords could not spend the break-glass approval"
            );
            return refused(submit_failure_code(
                &error,
                codes::COORDINATOR_NOT_AVAILABLE,
            ));
        }
    };

    // Ahead of the trim, so the acknowledgement below can never outrun the
    // durable record of what was deleted.
    if let Err(error) = publish_diskless_delete_floor(env, topic, &part, target).await {
        tracing::warn!(
            %topic, partition = index, %error,
            "DeleteRecords could not record the diskless delete floor"
        );
        return refused(codes::UNKNOWN_SERVER_ERROR);
    }

    match part.trim_to_offset(target).await {
        Ok(_) => {
            // On a diskless partition the local trim frontier is already past
            // `target` in the steady state, so `new_start` says nothing about
            // what a client can still read. The floor does, and it is also the
            // low watermark the response carries. Its followers learn the
            // floor from the WAL index, so the row does not wait for them.
            //
            // Any other partition answers Kafka's `lowWatermarkIfLeader`: the
            // lowest log start of the leader and of every live follower. When
            // that is still below the offset the request asked for, the row
            // waits for the followers (see `low_watermark::await_followers`).
            // Kafka's required offset is the requested one, with `-1` read as
            // the high watermark; a trim capped below it by the delivery
            // watermark requires only the offset it trimmed to.
            let (low_watermark, waiting_for) = if part.diskless {
                (target.0, None)
            } else {
                let requested = if fp.offset == -1 { hw.0 } else { fp.offset };
                let required = target.0.min(requested);
                let low_watermark = low_watermark::current(env.broker, &part).await.0;
                (
                    low_watermark,
                    (low_watermark < required).then_some(required),
                )
            };
            audit_transition(
                &env.broker.audit_log,
                &env.broker.config.break_glass,
                env.ctx,
                &GatedTransition {
                    action: BreakGlassAction::DeleteRecords,
                    target: &audit_target,
                    phase: PrivilegedPhase::Applied,
                    proposal_id,
                    reason: "records deleted below the trim point",
                },
            );
            let code = if waiting_for.is_some() {
                // Kafka's `DelayedDeleteRecords` starts every waiting row at
                // `REQUEST_TIMED_OUT`, and clears it when the row completes.
                codes::REQUEST_TIMED_OUT
            } else {
                codes::NONE
            };
            TrimOutcome {
                row: partition_result(index, low_watermark, code),
                deleted: deletes,
                waiting_for,
            }
        }
        Err(e) => {
            tracing::warn!(
                %topic, partition = index, error = %e,
                "DeleteRecords: trim_to_offset failed"
            );
            refused(codes::UNKNOWN_SERVER_ERROR)
        }
    }
}
