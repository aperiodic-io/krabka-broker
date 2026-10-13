//! `TxnOffsetCommit` (`api_key=28`). The consumer side of the
//! consume-process-produce pattern. A transactional producer that also
//! reads commits its consumed offsets atomically with its transaction by
//! appending them to `__consumer_offsets` with `is_transactional=true` +
//! the producer's (pid, epoch). The offsets are held under the partition's
//! LSO until a `WriteTxnMarkers` commit or abort marker arrives.
//!
//! Versions 0 to 2 are non-flexible and carry no `generation_id` or
//! `member_id` field. Versions 3 to 5 are flexible, carry tagged fields, and
//! add `generation_id`, `member_id`, and `group_instance_id`. Version 6
//! (KIP-1319, Kafka trunk; 4.3.1 stops at 5) names each topic by `TopicId`
//! instead of `Name`, in the request and in the response. An id the image
//! does not hold, or the zero id, answers `UNKNOWN_TOPIC_ID (100)` on every
//! row of that topic before the topic `Read` gate, and an id it holds is
//! authorized and checked for existence under the topic's name. The committed
//! offset records the topic's id from v6 on, and at every version under
//! `unstable.api.versions.enable`, as Kafka trunk's `KafkaApis` hands it to the
//! coordinator. Kafka 4.3.1 records the zero id for the versions it has, 0 to
//! 5. v6 also answers a missing group `GROUP_ID_NOT_FOUND` and a refused member
//! epoch `STALE_MEMBER_EPOCH`, which older versions answer `ILLEGAL_GENERATION`.
//!
//! On v3 and above, the shared `validate_commit` validates the
//! consumer-group metadata against the classic generation or the KIP-848
//! next-gen member epoch. KIP-447 requires fencing that is "consistent with
//! normal offset fencing".
//!
//! ## ACL preamble
//!
//! Three gates run in order:
//! * `Write` on `TransactionalId(transactional_id)`. A deny gives the whole
//!   response `TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)`.
//! * `Read` on `Group(group_id)`. A deny gives the whole response
//!   `GROUP_AUTHORIZATION_FAILED (30)`.
//! * `Read` on `Topic(name)` for each topic. A deny gives every partition row
//!   of that topic `TOPIC_AUTHORIZATION_FAILED (29)`.
//!
//! ## Existence check
//!
//! After the topic `Read` gate, every partition of an authorized topic goes
//! through the same existence check Kafka runs in
//! `KafkaApis.handleTxnOffsetCommitRequest` (lines 2124-2150): a topic the
//! metadata image does not hold, or a partition index outside that topic's
//! range, answers `UNKNOWN_TOPIC_OR_PARTITION (3)` on that row and is left
//! out of the transactional append. This code survives a later group-fencing
//! failure too: the fenced-request response still carries
//! `UNKNOWN_TOPIC_OR_PARTITION` on these rows rather than the fencing error.
//! See [`existence::unknown_partitions`].
//!
//! ## After the topic sweep
//!
//! The denied and unknown rows keep their own codes on every later exit, as
//! Kafka merges the group coordinator's answer into a response builder that
//! already holds them (`KafkaApis.scala:2185`). The response lists the
//! sweep's rows first, as that builder does; see [`response::build_response`].
//! When no row survives the sweep, the handler answers those rows and stops,
//! as Kafka does not call `commitTransactionalOffsets` then
//! (`KafkaApis.scala:2163-2165`): no routing check, no fencing code, no
//! transaction registration. Otherwise the group
//! routing check runs first, so a client on the wrong broker gets the
//! retriable `NOT_COORDINATOR` and a shard still replaying answers
//! `COORDINATOR_LOAD_IN_PROGRESS`, then the staged producer identity gate, then
//! the producer's verification with the transaction coordinator (KIP-890, see
//! [`verification`]), then the KIP-447 fencing checks.

use {
    krabka_ids::PartitionIndex,
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::owned::{
        txn_offset_commit_request::TxnOffsetCommitRequest,
        txn_offset_commit_response::TxnOffsetCommitResponse,
    },
};

mod batch;
mod existence;
mod response;
mod verification;

#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod ordering_tests;
#[cfg(test)]
mod test_support;

use {
    self::{
        batch::{
            AppendChecks, AppendedTxnOffsets, LocalOffsets, RowSweep, TxnBatchWrite,
            append_txn_batch,
        },
        existence::unknown_partitions,
        response::{build_response, error_response},
    },
    crate::{
        api_catalog::UnstableApiVersions,
        authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics},
        broker::Broker,
        codes,
        coordinator::{
            partitioner::{GroupRoutingError, local_partition_for_group},
            unified::{
                actor::{
                    CommitFence, CommitRequest, GroupActorHandle, GroupActorMessage, GroupKindTag,
                    TxnOffsetReservation, validate_commit,
                },
                streams::actor::validate_streams_group_commit,
            },
        },
        error::BrokerError,
        handlers::{RequestContext, group_partition_loading},
        txn::{state::TopicPartition, util::now_millis, version::resolve_txn_version},
    },
};

pub(crate) async fn handle(
    broker: &Broker,
    mut req: TxnOffsetCommitRequest,
    version: i16,
    ctx: &RequestContext<'_>,
) -> Result<TxnOffsetCommitResponse, BrokerError> {
    let partitions = broker.partitions.clone();

    // ── ACL preamble: Write on TransactionalId, Read on the group ──
    if let Some(code) = preamble_refusal(broker, ctx, &req) {
        return Ok(error_response(&req, code));
    }

    // ── KIP-1319: name every topic and give it its id ──────────
    // ── ACL preamble: per-topic Read ──────────────────────────
    // ── Existence check: authorized topic/partition must be in the image ──
    let topic_ids = version >= FIRST_TOPIC_ID_VERSION;
    let (denied_topics, unknown_rows) = {
        let image = broker.controller.current_image();
        resolve_topics(&mut req, version, &image);
        let topic_names: Vec<&str> = req
            .topics
            .iter()
            .map(|t| t.name.as_str())
            .filter(|name| !(topic_ids && name.is_empty()))
            .collect();
        let topic_decisions = authorize_topics(
            broker.config.authorizer.as_ref(),
            &*image,
            ctx.principal,
            ctx.peer,
            AclOperation::Read,
            topic_names,
        );
        let denied_topics: std::collections::HashSet<String> = topic_decisions
            .into_iter()
            .filter_map(|(name, r)| {
                if r == AuthorizationResult::Deny {
                    Some(name.to_string())
                } else {
                    None
                }
            })
            .collect();
        let unknown_rows = unknown_partitions(&req.topics, &denied_topics, &image);
        (denied_topics, unknown_rows)
    };

    // Every response from here on carries the per-row codes the sweep above
    // settled, and the given code only on the rows that survived it, the way
    // Kafka merges the coordinator's answer into `responseBuilder`
    // (`KafkaApis.scala:2185`).
    let respond = |code: i16| {
        // Kafka's `sendResponse` gives a client below v2 COORDINATOR_NOT_AVAILABLE
        // for COORDINATOR_LOAD_IN_PROGRESS, which those clients do not handle
        // (KAFKA-7296).
        let code = if version < 2 && code == codes::COORDINATOR_LOAD_IN_PROGRESS {
            codes::COORDINATOR_NOT_AVAILABLE
        } else {
            code
        };
        Ok(build_response(
            &req,
            code,
            topic_ids,
            &denied_topics,
            &unknown_rows,
        ))
    };

    // Kafka calls the group coordinator only when at least one row survives
    // the topic sweep (`KafkaApis.scala:2163-2165`). A request whose every row
    // is denied or unknown answers those rows and nothing else: no routing
    // check, no fencing, no transaction registration.
    let reserved = reserved_keys(&req, &denied_topics, &unknown_rows);
    if reserved.is_empty() {
        return respond(codes::NONE);
    }

    // 1. Verify that this broker leads the group's offsets partition before
    //    creating or accessing its actor. This runs ahead of the staged
    //    producer identity gate below, so a client that reached the wrong
    //    broker gets the retriable `NOT_COORDINATOR` and re-finds the
    //    coordinator instead of a fatal `INVALID_TXN_STATE`.
    let (offsets_partition, txnv) = {
        let image = broker.controller.current_image();
        match local_partition_for_group(&image, broker.config.node_id, &req.group_id) {
            Ok(partition) => {
                // A shard still replaying answers `COORDINATOR_LOAD_IN_PROGRESS`
                // on every row, as Kafka's `CoordinatorRuntime` does for a
                // `LOADING` shard, before either coordinator is touched.
                if let Some(code) = group_partition_loading(broker, partition) {
                    return respond(code);
                }
                (partition, resolve_txn_version(&image))
            }
            Err(GroupRoutingError::Unavailable) => {
                return respond(codes::COORDINATOR_NOT_AVAILABLE);
            }
            Err(GroupRoutingError::NotCoordinator) => return respond(codes::NOT_COORDINATOR),
        }
    };

    if let Some(entry) = broker.txn_coordinator.get(&req.transactional_id)
        && entry.lock().await.has_staged_producer_identity()
    {
        return respond(codes::INVALID_TXN_STATE);
    }

    // KIP-890: Kafka verifies the producer with the transaction coordinator
    // before the group coordinator's write operation validates the group, at
    // every version. A wrong producer id, a stale epoch, an unknown
    // transactional id, a partition that `AddOffsetsToTxn` never added, and a
    // transaction in a prepare state fail every row. The log's own producer
    // check then runs under the append lock with the guard the verification
    // started.
    let producer_check =
        match verification::verify_producer(broker, &req, version, offsets_partition, txnv).await {
            Ok(check) => check,
            Err(code) => return respond(code),
        };

    // Kafka's `OffsetMetadataManager.validateTransactionalOffsetCommit`: a
    // group the coordinator does not hold is created as a simple group only
    // for a commit without a generation (-1, and every commit below v3, which
    // has no such field). Any other commit names a group that is gone, which
    // v6 answers `GROUP_ID_NOT_FOUND` (KIP-1319) and older versions
    // `ILLEGAL_GENERATION`.
    let existing = broker.group_coordinator.find(&req.group_id);
    let streams = broker.group_coordinator.find_streams(&req.group_id);
    if existing.is_none() && streams.is_none() && req.generation_id_or_member_epoch >= 0 {
        return respond(if version >= FIRST_TOPIC_ID_VERSION {
            codes::GROUP_ID_NOT_FOUND
        } else {
            codes::ILLEGAL_GENERATION
        });
    }
    let handle = existing.unwrap_or_else(|| {
        broker
            .group_coordinator
            .get_or_create_group(&req.group_id, GroupKindTag::Classic)
    });

    // 2. KIP-447 / KIP-1319 fencing — identical to a regular OffsetCommit
    //    (KIP-447: "consistent with normal offset fencing"). For a classic
    //    group this checks member id + group.instance.id + generation
    //    (ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID / FENCED_INSTANCE_ID); for a
    //    KIP-848 next-gen group the `generation_id_or_member_epoch` field
    //    carries the member epoch, and a mismatch is Kafka's
    //    `StaleMemberEpochException`, which [`fencing_code`] maps by version.
    //    A producer that supplies no metadata (empty member_id,
    //    generation_id_or_member_epoch = -1) is a simple consumer and is not
    //    fenced. The fields only exist on v3+, so older requests carry the
    //    simple-consumer defaults and no-op. `validate_commit` dispatches
    //    on the actor's LIVE `group.kind`, so a KIP-848-flipped group is fenced
    //    against its current protocol, not the stale spawn-time `handle.kind`.
    // KIP-1071: a streams-group consumer's membership lives in the STREAMS
    // group actor, not the classic one. Route its fencing there (member_epoch
    // check) — `validate_commit` only knows the classic/consumer actor,
    // so validating a streams member against the freshly-created empty classic
    // actor would wrongly reject every EOS offset commit with UNKNOWN_MEMBER_ID.
    if version >= 3 {
        let code = if let Some(streams) = streams {
            validate_streams_group_commit(
                &streams,
                &req.member_id,
                req.generation_id_or_member_epoch,
                reserved
                    .iter()
                    .map(|key| (key.topic.clone(), key.partition.get()))
                    .collect(),
            )
            .await
        } else {
            validate_commit(&handle, CommitRequest {
                member_id: req.member_id.clone(),
                group_instance_id: req.group_instance_id.clone(),
                generation_or_epoch: req.generation_id_or_member_epoch,
                fence: CommitFence::Transactional,
                partitions: reserved
                    .iter()
                    .map(|key| (topic_id_of(&req, &key.topic), key.partition.get()))
                    .collect(),
            })
            .await
        };
        if let Some(code) = code {
            return respond(fencing_code(code, version));
        }
    }

    // 3. Append a transactional RecordBatch to __consumer_offsets.
    //    We reuse the OffsetCommitKey/Value layout but stamp the batch with
    //    is_transactional=true + (producer_id, producer_epoch) so the log's
    //    LSO machinery holds the offsets until EndTxn commits/aborts.
    //    Topics denied by the per-topic Read ACL, and rows the existence
    //    check flagged, are skipped from the batch and surfaced as
    //    TOPIC_AUTHORIZATION_FAILED / UNKNOWN_TOPIC_OR_PARTITION in the
    //    response.
    // Reserve the keys on the group actor before the append, so that a
    // concurrent `DeleteGroups` either runs first (the actor stops and the
    // reservation fails before anything is durable) or tombstones these keys
    // after the records.
    if reserve_offsets(&handle, req.producer_id, reserved.clone(), true)
        .await
        .is_err()
    {
        return respond(codes::COORDINATOR_NOT_AVAILABLE);
    }
    let now_ms = now_millis();
    // Kafka trunk's `OffsetAndMetadata.fromRequest(topic.topicId(), ...)` keeps
    // the topic id of every commit. 4.3.1 keeps the zero id, and its versions
    // stop at 5, so only a v6 request and trunk mode record an id.
    let record_topic_ids = version >= FIRST_TOPIC_ID_VERSION
        || broker.config.features.unstable_api_versions == UnstableApiVersions::Enabled;
    let appended = match append_txn_batch(
        &req,
        LocalOffsets {
            registry: &partitions,
            metadata: &*broker.controller,
            node_id: broker.config.node_id,
        },
        offsets_partition,
        now_ms,
        RowSweep {
            denied_topics: &denied_topics,
            unknown_rows: &unknown_rows,
        },
        AppendChecks {
            producer_check,
            record_topic_ids,
        },
    )
    .await
    {
        Ok(appended) => appended,
        Err(code) => {
            // Nothing reached the log, so the reservation goes. An actor that
            // stopped meanwhile took it with it.
            let _ = reserve_offsets(&handle, req.producer_id, reserved, false).await;
            return respond(code);
        }
    };

    // 4. KIP-447: mark those offsets pending on the group actor, so that an
    //    `OffsetFetch` with `require_stable = true` answers
    //    UNSTABLE_OFFSET_COMMIT for them until the transaction's marker
    //    resolves. Marking after the append is what guarantees the marker
    //    path can rediscover the same keys in the log and clear them; a mark
    //    placed before a failed append would never be cleared. The append's
    //    log position travels with the mark, because the marker for this very
    //    transaction can be resolved on the actor in between, and the log
    //    order is what tells the actor that it was. Kafka's runtime applies a
    //    write to the shard when it appends it, before the write commits, so
    //    the mark goes on here too.
    let Some(TxnBatchWrite {
        offsets: appended,
        write,
    }) = appended
    else {
        // 5. Every row was denied or unknown: those rows keep their codes.
        return respond(codes::NONE);
    };
    if let Err(code) = mark_offsets_pending(&handle, req.producer_id, appended, &req.group_id).await
    {
        return respond(code);
    }

    // 5. Kafka's `CoordinatorRuntime` completes the write, and so the request,
    //    once the high watermark covers the batch. An answer at the local
    //    append would acknowledge offsets that the next leader of the
    //    partition may never get, and the transaction would then commit
    //    without them. Per-(topic, partition) error_code = NONE for allowed,
    //    TOPIC_AUTHORIZATION_FAILED for denied, UNKNOWN_TOPIC_OR_PARTITION for
    //    a row the existence check flagged.
    match write.committed().await {
        Ok(()) => respond(codes::NONE),
        Err(error) => {
            tracing::warn!(
                group = %req.group_id,
                tid = %req.transactional_id,
                %error,
                "TxnOffsetCommit: the offsets append did not commit"
            );
            respond(batch::append_error_code(&error))
        }
    }
}

/// The code that refuses every row of `req` before the topic sweep: the
/// principal may not `Write` the transactional id, or may not `Read` the
/// group. `None` when both gates pass.
fn preamble_refusal(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    req: &TxnOffsetCommitRequest,
) -> Option<i16> {
    let image = broker.controller.current_image();
    let authorizer = broker.config.authorizer.as_ref();
    let tid_req = AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type: ResourceType::TransactionalId,
        resource_name: req.transactional_id.as_str(),
        operation: AclOperation::Write,
    };
    if authorizer.authorize(&*image, &tid_req) == AuthorizationResult::Deny {
        return Some(codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
    }
    // Group Read gate.
    let group_req = AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type: ResourceType::Group,
        resource_name: req.group_id.as_str(),
        operation: AclOperation::Read,
    };
    if authorizer.authorize(&*image, &group_req) == AuthorizationResult::Deny {
        return Some(codes::GROUP_AUTHORIZATION_FAILED);
    }
    None
}

/// The first `TxnOffsetCommit` version that names each topic by `TopicId`
/// only (KIP-1319). Versions 0 to 5 name it by `Name`.
const FIRST_TOPIC_ID_VERSION: i16 = 6;

/// Kafka's `KafkaApis.handleTxnOffsetCommitRequest` before its topic sweep:
/// at v6+ each topic id the image knows gives the topic its name, and one it
/// does not know, or the zero id, leaves the name empty for the sweep to
/// answer `UNKNOWN_TOPIC_ID`. Below v6 each topic gets the id the image holds
/// for its name, or the zero id. Either way the id is what the per-partition
/// validator sees, and what the committed offset records where trunk's
/// `OffsetAndMetadata.fromRequest` does: from v6, and under
/// `unstable.api.versions.enable`.
fn resolve_topics(
    req: &mut TxnOffsetCommitRequest,
    version: i16,
    image: &krabka_metadata::MetadataImage,
) {
    for topic in &mut req.topics {
        if version >= FIRST_TOPIC_ID_VERSION {
            let id = uuid::Uuid::from_bytes(topic.topic_id.0);
            if !id.is_nil()
                && let Some(name) = image.topic_name_by_id(&id)
            {
                topic.name = name.to_string();
            }
        } else {
            topic.topic_id = image.topic(&topic.name).map_or_else(Default::default, |t| {
                krabka_protocol::primitives::uuid::Uuid(t.topic_id.into_bytes())
            });
        }
    }
}

/// Kafka's mapping of a member-epoch refusal on the transactional path
/// (`validateTransactionalOffsetCommit`): the consumer and streams groups
/// raise `StaleMemberEpochException` for an epoch that is not the member's,
/// which v6 answers as `STALE_MEMBER_EPOCH` (KIP-1319) and older versions as
/// `ILLEGAL_GENERATION`. The per-partition validator of
/// `commitTransactionalOffset` maps its refusal the same way.
fn fencing_code(code: i16, version: i16) -> i16 {
    match code {
        codes::STALE_MEMBER_EPOCH if version < FIRST_TOPIC_ID_VERSION => codes::ILLEGAL_GENERATION,
        other => other,
    }
}

/// The `(topic, partition)` keys the transactional append will write: every
/// partition of every topic the principal may read, minus the rows the
/// existence check flagged as unknown.
fn reserved_keys(
    req: &TxnOffsetCommitRequest,
    denied_topics: &std::collections::HashSet<String>,
    unknown_rows: &std::collections::HashSet<TopicPartition>,
) -> Vec<TopicPartition> {
    req.topics
        .iter()
        .filter(|topic| !denied_topics.contains(&topic.name))
        .flat_map(|topic| {
            topic.partitions.iter().filter_map(|partition| {
                let key = TopicPartition {
                    topic: topic.name.clone(),
                    partition: PartitionIndex(partition.partition_index),
                };
                (!unknown_rows.contains(&key)).then_some(key)
            })
        })
        .collect()
}

/// The topic id [`resolve_topics`] gave `name`, or the zero id for a topic the
/// request does not name. The per-partition validator of Kafka's
/// `commitTransactionalOffset` sees this id for every `reserved` key.
fn topic_id_of(
    req: &TxnOffsetCommitRequest,
    name: &str,
) -> krabka_protocol::primitives::uuid::Uuid {
    req.topics
        .iter()
        .find(|topic| topic.name == name)
        .map_or_else(Default::default, |topic| topic.topic_id)
}

/// Reserves (`reserve = true`) or releases the keys of an append on the group
/// actor. `Err` means the actor has stopped.
async fn reserve_offsets(
    handle: &GroupActorHandle,
    producer_id: i64,
    keys: Vec<TopicPartition>,
    reserve: bool,
) -> Result<(), ()> {
    let (reply, ack) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::TxnOffsetReservation(
            TxnOffsetReservation {
                producer_id,
                keys: keys
                    .into_iter()
                    .map(|key| (key.topic, key.partition.get()))
                    .collect(),
                reserve,
                reply,
            },
        ))
        .await
        .map_err(|_| ())?;
    ack.await.map_err(|_| ())
}

/// Marks the appended offsets as belonging to an unresolved transaction.
///
/// A group actor that cannot take the mark would leave the group answering a
/// `require_stable` fetch with the pre-transaction offset, which is the
/// rewind KIP-447 exists to prevent, so the commit reports
/// `COORDINATOR_NOT_AVAILABLE` rather than claiming a success the fetch path
/// cannot honour.
async fn mark_offsets_pending(
    handle: &GroupActorHandle,
    producer_id: i64,
    appended: AppendedTxnOffsets,
    group_id: &str,
) -> Result<(), i16> {
    let (reply, ack) = tokio::sync::oneshot::channel();
    if handle
        .tx
        .send(GroupActorMessage::AddPendingTxnOffsets {
            producer_id,
            written_at: appended.written_at,
            keys: appended
                .keys
                .into_iter()
                .map(|key| (key.topic, key.partition.get()))
                .collect(),
            reply,
        })
        .await
        .is_err()
        || ack.await.is_err()
    {
        tracing::warn!(
            group = %group_id,
            producer_id,
            "TxnOffsetCommit: group actor could not record the pending transactional offsets"
        );
        return Err(codes::COORDINATOR_NOT_AVAILABLE);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::coordinator::unified::test_support::make_coord, assert2::check,
        std::collections::HashSet, tokio::sync::oneshot,
    };

    /// `validateTransactionalOffsetCommit` passes a `StaleMemberEpochException`
    /// through at v6 and turns it into `ILLEGAL_GENERATION` below; every other
    /// code is untouched.
    #[test]
    fn member_epoch_refusals_map_by_version() {
        for (code, version, want) in [
            (codes::STALE_MEMBER_EPOCH, 5, codes::ILLEGAL_GENERATION),
            (codes::STALE_MEMBER_EPOCH, 6, codes::STALE_MEMBER_EPOCH),
            (codes::FENCED_MEMBER_EPOCH, 5, codes::FENCED_MEMBER_EPOCH),
            (codes::UNKNOWN_MEMBER_ID, 5, codes::UNKNOWN_MEMBER_ID),
            (codes::UNKNOWN_MEMBER_ID, 6, codes::UNKNOWN_MEMBER_ID),
            (codes::ILLEGAL_GENERATION, 6, codes::ILLEGAL_GENERATION),
        ] {
            check!(
                fencing_code(code, version) == want,
                "code {code} at v{version}"
            );
        }
    }

    /// The mark is what makes a later `require_stable` `OffsetFetch` answer
    /// `UNSTABLE_OFFSET_COMMIT`, so a group actor that cannot take it must not
    /// leave the commit reporting success: the consumer would read the
    /// pre-transaction offset and reprocess the records the transaction had
    /// already handled. A live actor takes it and reports it; a departed one
    /// makes the commit answer `COORDINATOR_NOT_AVAILABLE`.
    #[tokio::test]
    async fn a_live_actor_takes_the_mark_and_a_departed_one_fails_the_commit() {
        let coord = make_coord();
        let handle = coord.get_or_create_group("g", GroupKindTag::Classic);

        mark_offsets_pending(
            &handle,
            7,
            AppendedTxnOffsets {
                written_at: 4,
                keys: vec![TopicPartition {
                    topic: "orders".to_string(),
                    partition: PartitionIndex(0),
                }],
            },
            "g",
        )
        .await
        .expect("a live actor takes the mark");

        let (reply, offsets) = oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::FetchOffsets { reply })
            .await
            .expect("send FetchOffsets");
        check!(
            offsets.await.expect("FetchOffsets reply").pending_txn
                == HashSet::from([("orders".to_string(), 0)])
        );

        let (reply, ack) = oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Shutdown(reply))
            .await
            .expect("send Shutdown");
        ack.await.expect("Shutdown ack");
        tokio::time::timeout(std::time::Duration::from_secs(1), handle.tx.closed())
            .await
            .expect("the actor's handle closes");

        let refused = mark_offsets_pending(
            &handle,
            7,
            AppendedTxnOffsets {
                written_at: 5,
                keys: vec![TopicPartition {
                    topic: "orders".to_string(),
                    partition: PartitionIndex(1),
                }],
            },
            "g",
        )
        .await
        .expect_err("a departed actor cannot take the mark");
        check!(refused == codes::COORDINATOR_NOT_AVAILABLE);
    }
}
