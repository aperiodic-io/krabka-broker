//! The per-partition produce pipeline, which runs one partition's records
//! through every gate in order and returns that partition's response row, or
//! the high-watermark wait that still stands between it and one.

use {
    super::{
        INVALID_OFFSET,
        append::{AppendContext, AppendOutcome, PendingAck, dispatch_prepared},
        delivery::DeliveryGate,
        framing::FramedPartition,
        leadership::{
            BrokerProducePolicy, current_leader_hint, diskless_role_ready,
            replica_state_matches_image, replication_target_matches_image, validate_partition_gate,
        },
        prepare::{DecodeEnv, PreparedBatch, prepare_batch},
        producer_checks::{
            DedupOutcome, TransactionRequest, Verification, handle_duplicate,
            produce_verification_code, verify_transactional_produce, verify_with_coordinator,
        },
        record_limit::exceeds_decompressed_limit,
        schema::{SCHEMA_REJECTION_MESSAGE, validate_batch_schemas},
        topic_settings::TimestampPolicy,
    },
    crate::{
        api_catalog::UnstableApiVersions,
        codes,
        error::BrokerError,
        freeze::resolve::{FreezeMutationResolution, FreezeVerdict},
        log_dir_status::LogDirRegistry,
        metrics::{BrokerMetrics, RequestPhases},
        partition::{Partition, ReplicationTarget},
        partition_registry::PartitionRegistry,
        producer_state::ProducerState,
        schema_validation::{SchemaGate, SchemaValidator},
        txn::{coordinator::TxnCoordinator, state::TopicPartition},
    },
    krabka_compression::RecordDecompressionPolicy,
    krabka_protocol::owned::produce_response::{
        BatchIndexAndErrorMessage, PartitionProduceResponse,
    },
    krabka_units::{ByteSize, convert::ByteSizeExt as _},
    std::sync::Arc,
};

/// Per-partition produce input, held apart so that the call site can time the
/// work and charge it to `partition_cpu_micros_total`.
///
/// The interval the call site measures is this pipeline's own: the gates, the
/// enqueue, and the writer's answer. The `acks=-1` high-watermark wait is not
/// in it, because that wait no longer happens here — the pipeline hands it
/// back and the handler drives every partition's wait together, so charging it
/// per partition would charge one interval N times over.
///
/// The work returns the per-partition response on every path. Only
/// `txn_coordinator.put` errors propagate with `?`.
pub(super) struct PartitionInput<'a> {
    pub(super) part_data: FramedPartition,
    pub(super) topic_compression: Option<krabka_compression::CompressionType>,
    /// The topic's KIP-32 timestamp policy, resolved once per topic:
    /// `message.timestamp.type` plus the two
    /// `message.timestamp.{before,after}.max.ms` windows. The default admits
    /// every timestamp and costs one boolean test per batch.
    pub(super) timestamps: TimestampPolicy,
    /// Whether the topic's `cleanup.policy` holds `compact`, resolved once per
    /// topic. Kafka's `LogValidator` then refuses a record with no key.
    pub(super) compacted_topic: bool,
    /// The topic's `max.message.bytes`, resolved once per topic, with the
    /// broker's `message.max.bytes` behind it. Every topic has one, so unlike
    /// the gates below it is a value and not an `Option`.
    pub(super) max_message_bytes: ByteSize,
    /// The topic's KFC-1 delivery settings, resolved once per topic. `None` is
    /// `delivery.mode=immediate`, and skips the delivery gate entirely.
    pub(super) delivery: Option<DeliveryGate>,
    /// The topic's KFC-7 schema-validation settings, resolved once per topic.
    /// `None` is "neither `schema.validation.key` nor
    /// `schema.validation.value` is set", and skips the check entirely.
    pub(super) schema: Option<SchemaGate>,
    /// The topic name, as the `Arc<str>` the metric label sets clone. The
    /// registry owns the one copy — see `PartitionRegistry::shared_topic_name`
    /// — so passing it down here is what keeps the per-partition accounting
    /// free of allocations.
    pub(super) topic_name: Arc<str>,
    /// The topic's authorization-and-freeze result, resolved once per topic.
    /// Only `Frozen` carries registry detail, after authorization succeeded.
    pub(super) freeze: FreezeMutationResolution<'a>,
    /// Whether this partition's topic is one of [`crate::internal_topics::
    /// INTERNAL_TOPICS`] (or the configured audit topic) and the request's
    /// `client_id` is not Kafka's admin-tooling exception. Resolved once per
    /// topic, beside `freeze`, since it is a property of the topic and the
    /// request, not of a partition or a batch.
    pub(super) internal_topic_denied: bool,
    /// The request fields of the KIP-890 transaction check.
    pub(super) transaction: TransactionRequest<'a>,
    pub(super) acks: i16,
}

#[derive(Clone, Copy)]
pub(super) struct PartitionServices<'a> {
    pub(super) partitions: &'a Arc<PartitionRegistry>,
    pub(super) txn_coordinator: &'a Arc<TxnCoordinator>,
    pub(super) producer_state: &'a Arc<ProducerState>,
    pub(super) log_dir_status: &'a LogDirRegistry,
    pub(super) image: &'a Arc<krabka_metadata::MetadataImage>,
    pub(super) broker_policy: BrokerProducePolicy,
    pub(super) record_decompression_policy: RecordDecompressionPolicy,
    pub(super) metrics: &'a BrokerMetrics,
    /// The request's phase accumulator. The append charges the writer
    /// round-trip and the `acks=-1` high-watermark gate to it, and the handler
    /// observes the totals once the whole request is done.
    pub(super) phases: &'a RequestPhases,
    /// The broker's KFC-7 validator. `None` is "no `[schema_registry]`
    /// section", and a topic that asks for validation on such a broker is
    /// rejected rather than admitted unchecked.
    pub(super) schema_validator: Option<&'a Arc<SchemaValidator>>,
    /// Kafka's `unstable.api.versions.enable`, which decides whether the
    /// idempotent-producer check applies Kafka trunk's empty-log sequence rule.
    pub(super) unstable_api_versions: UnstableApiVersions,
}

/// What one partition's pipeline produced: a finished response row, or the
/// `acks=-1` high-watermark wait the handler has to drive before there is one.
pub(super) enum PartitionOutcome {
    /// The row is complete.
    Done(PartitionProduceResponse),
    /// The batch is on this broker's log; the row is complete once the high
    /// watermark covers it.
    AwaitingHighWatermark(PendingAck),
}

impl PartitionOutcome {
    /// The response row, for a caller that knows this partition finished
    /// without an `acks=-1` wait.
    ///
    /// # Panics
    /// Panics when the partition is still waiting on the high watermark.
    #[cfg(test)]
    pub(super) fn expect_done(self) -> PartitionProduceResponse {
        match self {
            Self::Done(response) => response,
            Self::AwaitingHighWatermark(_) => {
                panic!("partition is still waiting on the high watermark")
            }
        }
    }
}

/// Where one partition stands once every gate before the append has passed
/// it, except the transaction coordinator's answer.
pub(super) enum Admission {
    /// A gate refused the batch; the row is complete.
    Done(PartitionProduceResponse),
    /// The batch may append once its transaction check, if any, succeeds.
    Admitted(Box<AdmittedBatch>),
}

/// A batch that passed every gate up to the transaction coordinator call.
///
/// Kafka's `ReplicaManager.handleProduceAppend` collects every such partition
/// of the request and asks the coordinator about all of them at once
/// ([`verify_admitted`]), then appends ([`complete_partition`]).
pub(super) struct AdmittedBatch {
    prepared: PreparedBatch,
    part: Arc<Partition>,
    shared_topic: Arc<str>,
    delivery: Option<DeliveryGate>,
    acks: i16,
    /// The request's `Produce` version.
    version: i16,
    /// The pre-append row, with the `UNKNOWN_LOG_APPEND_INFO` sentinel.
    out: PartitionProduceResponse,
    verification: Verification,
}

impl AdmittedBatch {
    /// The producer and partition this batch asks the coordinator about, or
    /// `None` when it needs no coordinator call.
    fn coordinator_check(&self) -> Option<((krabka_log::ProducerId, i16), TopicPartition)> {
        let Verification::Coordinator(check) = self.verification else {
            return None;
        };
        Some((
            (check.batch.producer_id, check.batch.producer_epoch),
            TopicPartition {
                topic: self.shared_topic.to_string(),
                partition: krabka_ids::PartitionIndex(self.out.index),
            },
        ))
    }
}

/// The partitions one producer's coordinator call covers, each with the
/// index of its batch.
struct ProducerChecks {
    producer: (krabka_log::ProducerId, i16),
    partitions: Vec<(usize, TopicPartition)>,
}

/// The transaction coordinator's answer for each admitted batch that needs
/// one, in `batches` order, and `None` for a batch that needs none.
///
/// One `AddPartitionsToTxn` call covers every partition of one producer, as
/// Kafka's `ReplicaManager.maybeSendPartitionsToTransactionCoordinator`
/// sends one `addOrVerifyTransaction` for the whole request.
pub(super) async fn verify_admitted(
    batches: &[&AdmittedBatch],
    txn_coordinator: &Arc<TxnCoordinator>,
    image: &krabka_metadata::MetadataImage,
    transaction: TransactionRequest<'_>,
) -> Vec<Option<i16>> {
    let mut answers = vec![None; batches.len()];
    let mut producers: Vec<ProducerChecks> = Vec::new();
    for (index, batch) in batches.iter().enumerate() {
        let Some((producer, partition)) = batch.coordinator_check() else {
            continue;
        };
        match producers
            .iter_mut()
            .find(|known| known.producer == producer)
        {
            Some(known) => known.partitions.push((index, partition)),
            None => producers.push(ProducerChecks {
                producer,
                partitions: vec![(index, partition)],
            }),
        }
    }
    for ProducerChecks {
        producer,
        partitions,
    } in producers
    {
        let answered = verify_with_coordinator(
            txn_coordinator,
            image,
            transaction,
            producer,
            partitions
                .iter()
                .map(|(_, partition)| partition.clone())
                .collect(),
        )
        .await;
        for ((index, _), (_, code)) in partitions.into_iter().zip(answered) {
            answers[index] = Some(code);
        }
    }
    answers
}

async fn local_replica_is_ready(
    part: &Arc<Partition>,
    image: &krabka_metadata::MetadataImage,
    topic_name: &str,
    idx: i32,
) -> bool {
    ready_transition(part, image, topic_name, idx)
        .await
        .is_some()
}

/// The partition's transition barrier, held, when the local replica is ready
/// to lead the partition as the image names it, or `None` when it is not.
async fn ready_transition(
    part: &Arc<Partition>,
    image: &krabka_metadata::MetadataImage,
    topic_name: &str,
    idx: i32,
) -> Option<tokio::sync::OwnedRwLockReadGuard<ReplicationTarget>> {
    let transition = part.lock_produce_transition().await;
    let record = image.partition(topic_name, idx).expect("gate checked");
    let topic_id = image.topic(topic_name).map(|topic| topic.topic_id);
    if !replication_target_matches_image(&transition, topic_id, record)
        || (part.diskless && !diskless_role_ready(part, record))
    {
        return None;
    }
    if !part.diskless {
        let replica_state = part.replica_state.lock().await;
        if !replica_state_matches_image(&replica_state, record) {
            return None;
        }
    }
    Some(transition)
}

/// The KIP-890 transaction check of one batch, after the diskless refusal:
/// a diskless partition takes no transactional batch. A refusal fills the
/// pre-append row `refused`.
async fn verify_before_append(
    prepared: &super::prepare::PreparedBatch,
    part: &Partition,
    (transaction, mut refused): (TransactionRequest<'_>, PartitionProduceResponse),
) -> Result<Verification, Box<PartitionProduceResponse>> {
    let verified = if prepared.attributes.is_transactional() && part.diskless {
        Err((codes::INVALID_TXN_STATE, None))
    } else {
        verify_transactional_produce(prepared, part, transaction).await
    };
    verified.map_err(|(code, message)| {
        refused.error_code = code;
        refused.error_message = message;
        Box::new(refused)
    })
}

/// Run one partition through every stage: [`admit_partition`], the
/// coordinator call of [`verify_admitted`], and [`complete_partition`].
#[cfg(test)]
pub(super) async fn process_partition(
    input: PartitionInput<'_>,
    services: PartitionServices<'_>,
) -> Result<PartitionOutcome, BrokerError> {
    let transaction = input.transaction;
    let batch = match admit_partition(input, services).await? {
        Admission::Done(row) => return Ok(PartitionOutcome::Done(row)),
        Admission::Admitted(batch) => batch,
    };
    let answer = verify_admitted(
        &[&batch],
        services.txn_coordinator,
        services.image,
        transaction,
    )
    .await
    .into_iter()
    .next()
    .flatten();
    complete_partition(*batch, answer, services).await
}

/// Every gate of one partition before the transaction coordinator call.
pub(super) async fn admit_partition(
    input: PartitionInput<'_>,
    services: PartitionServices<'_>,
) -> Result<Admission, BrokerError> {
    let PartitionInput {
        part_data,
        topic_compression,
        timestamps,
        compacted_topic,
        max_message_bytes,
        delivery,
        schema,
        topic_name,
        freeze,
        internal_topic_denied,
        transaction,
        acks,
    } = input;
    // `shared_topic` is the owned handle the metric labels clone;
    // `topic_name` stays the borrowed view every gate and image lookup below
    // already takes.
    let shared_topic = topic_name;
    let topic_name: &str = &shared_topic;
    let PartitionServices {
        partitions,
        log_dir_status,
        image,
        broker_policy,
        record_decompression_policy,
        metrics,
        schema_validator,
        unstable_api_versions,
        ..
    } = services;
    let idx = part_data.index;
    // Every gate below returns this row, and every one of them refuses before
    // any append happened. Kafka fills such a row from
    // `LogAppendInfo.UNKNOWN_LOG_APPEND_INFO`, whose `firstOffset` is -1, so
    // the sentinel is stamped once here rather than at each `return`. The
    // `Default` for the other two offset-ish fields is already -1; only
    // `base_offset` defaults to 0, which would claim the batch landed at the
    // start of the log. The success and dedup paths build their own row and
    // never see this one.
    let mut out = PartitionProduceResponse {
        index: idx,
        base_offset: INVALID_OFFSET,
        ..Default::default()
    };

    // ── KFC-9 write freeze ───────────────────────────────────────────
    // Beside the topic ACL denial, and ahead of `prepare_batch`, because a
    // freeze is an authority gate and not a content gate. It ranks with the
    // denial above rather than with the KFC-1 and KFC-7 gates below, so a
    // frozen topic never pays CRC verification or decompression for a batch
    // the broker will never accept.
    //
    // The position has a second consequence, which the tests assert: the gate
    // returns ahead of the idempotent-sequence gate, so a refused batch leaves
    // the producer state untouched and the log end offset unmoved. A refusal
    // that still appended would be the worst failure this feature can have,
    // and the error code alone does not rule it out.
    match freeze {
        FreezeMutationResolution::AuthorizationDenied => {
            out.error_code = codes::TOPIC_AUTHORIZATION_FAILED;
            return Ok(Admission::Done(out));
        }
        FreezeMutationResolution::Frozen(entry) => {
            metrics.record_topic_freeze_rejection(topic_name);
            out.error_code = codes::POLICY_VIOLATION;
            out.error_message = Some(FreezeVerdict::from(entry).error_message());
            return Ok(Admission::Done(out));
        }
        FreezeMutationResolution::Admit => {}
    }

    // ── internal-topic gate ──────────────────────────────────────────
    // Kafka's `ReplicaManager.appendToLocalLog` refuses every partition of
    // `Topic.isInternal(topic)` with `InvalidTopicException` before it even
    // looks up the partition's local log, unless the request's `client_id` is
    // `"__admin_client"` (`KafkaApis.handleProduceRequest`'s
    // `internalTopicsAllowed`). `__consumer_offsets`, `__transaction_state`
    // and `__share_group_state` are replayed by this broker's own
    // coordinators to rebuild group and transaction state, so an ordinary
    // client append to one of them is a forged coordinator record, not a
    // message. On a cluster with no authorizer configured, the default,
    // nothing else stands between an unauthenticated client and that record,
    // which is what makes this gate an authority gate rather than a content
    // one: it ranks beside the freeze and ACL checks above, and ahead of
    // every gate that reads the batch.
    //
    // The coordinators themselves never take this path: each one appends
    // through the partition writer directly (see
    // `crate::internal_topics::PRODUCE_ADMIN_CLIENT_ID`), so this gate cannot
    // refuse the broker's own replay of its coordinator logs.
    if internal_topic_denied {
        out.error_code = codes::INVALID_TOPIC_EXCEPTION;
        return Ok(Admission::Done(out));
    }

    // ── max.message.bytes ────────────────────────────────────────────
    // Ahead of `prepare_batch`, which is the whole operational point: a batch
    // the broker will never accept must not first cost it a CRC pass and a
    // decompression over however many mebibytes the producer sent. The check
    // reads v2 batch headers and nothing else.
    //
    // Kafka measures each batch on its own, over the batch's entire wire
    // encoding including its 61-byte header, and refuses one that is strictly
    // larger than the cap. `error_message` stays empty because Kafka's
    // `RecordTooLargeException` is not one of the exceptions it attaches a
    // custom message to; the client renders `Errors.MESSAGE_TOO_LARGE`'s own
    // text instead.
    //
    // `base_offset` is the -1 sentinel, not 0. The refusal happens before any
    // append, so Kafka's `LogAppendInfo.UNKNOWN_LOG_APPEND_INFO` supplies the
    // row's offsets and every one of them is -1. A raw `Produce v9` against
    // `apache/kafka:4.3.1` with `max.message.bytes=2048` answers a 2049-byte
    // batch with `base_offset=-1`, and a producer that read 0 would report a
    // record it never wrote as living at the partition's first offset.
    if part_data.payload.largest_batch_len() > max_message_bytes.bytes_usize() {
        out.error_code = codes::MESSAGE_TOO_LARGE;
        out.base_offset = INVALID_OFFSET;
        return Ok(Admission::Done(out));
    }

    // Decide verbatim-passthrough vs owned-decode and extract the HEADER
    // fields the gates below need (producer id/epoch/sequence,
    // last_offset_delta, max_timestamp, attributes). On the verbatim path
    // this verifies the CRC and complete record structure while retaining the
    // original wire bytes. Compressed bodies are transiently decompressed for
    // validation but never re-encoded. The owned fallback fully materializes
    // the records, exactly as before. A null /
    // undecodable field returns INVALID_REQUEST / INVALID_RECORD, preserving
    // the prior error-code ordering (before the leadership gate).
    let prepared = match prepare_batch(
        part_data.payload,
        topic_compression,
        timestamps,
        compacted_topic,
        DecodeEnv {
            topic_name: &shared_topic,
            metrics,
            policy: record_decompression_policy,
        },
        transaction.version,
    ) {
        Ok(p) => p,
        Err(code) => {
            out.error_code = code;
            return Ok(Admission::Done(out));
        }
    };

    // ── max.decompressed.message.bytes (Kafka trunk) ─────────────────
    // A compressed batch with a record whose decompressed body is larger than
    // the topic's limit is `INVALID_RECORD`, with no per-record errors, as
    // `LogValidator` answers it when `DefaultRecord.readFrom` throws while the
    // batch is iterated. Kafka 4.3.1 has no such limit, so only a broker
    // serving trunk's keys reads it, and only a compressed batch pays for the
    // lookup.
    if exceeds_decompressed_limit(
        &prepared,
        (image, broker_policy.node_id, topic_name),
        unstable_api_versions,
        record_decompression_policy,
    ) {
        out.error_code = codes::INVALID_RECORD;
        return Ok(Admission::Done(out));
    }

    // ── max.message.bytes, again, on the re-encoded batch ────────────
    // The check above measured the bytes the producer sent. Those are the
    // bytes that land only on the verbatim path. The owned path re-encodes,
    // and a topic whose `compression.type` forces a codec the producer did not
    // use decides the stored size itself: `compression.type=uncompressed`
    // expands a batch that arrived well under the cap into one the cap exists
    // to keep out, and a legacy `MessageSet` changes size in the v2
    // up-conversion. `stored_len` is `None` on the verbatim path, so this
    // second measurement costs the hot path nothing.
    //
    // Kafka runs the same second check for the same reason:
    // `UnifiedLog.append` re-walks the validated batches and throws
    // `RecordTooLargeException` whenever `LogValidator` reports
    // `messageSizeMaybeChanged`. It sits here, before the producer-state
    // gates, because Kafka's sits before `analyzeAndValidateProducerState`
    // too, and because a refused batch must leave the idempotent sequence and
    // the log end offset exactly where it found them.
    let stored_config = matches!(prepared.source, super::prepare::PreparedSource::Owned(_))
        .then(|| {
            partitions
                .get(topic_name, krabka_ids::PartitionIndex(idx))
                .and_then(|part| part.log.lock().ok().map(|log| log.config_snapshot()))
        })
        .flatten();
    if let Some(stored) = prepared.stored_len(topic_compression, stored_config.as_ref())
        && stored > max_message_bytes.bytes_usize()
    {
        // `out` already carries the -1 `base_offset` sentinel.
        out.error_code = codes::MESSAGE_TOO_LARGE;
        return Ok(Admission::Done(out));
    }

    // ── leadership gate (Kafka: only the LEADER accepts Produce) ──────
    // Only the partition leader may accept a Produce. A Produce misrouted
    // to a non-leader must be rejected so the client refreshes its
    // metadata and re-targets — it must NOT be appended to a local
    // follower replica (the real leader would never see those records and
    // the follower's append would be discarded on its next truncating
    // Fetch from the leader → silent data loss).
    //
    // The authoritative leader is the metadata IMAGE's `partition.leader`,
    // the same source the Fetch handler uses for its KIP-320 / KIP-951
    // `current_leader` hint. We deliberately do NOT gate on the broker's
    // local `leader_partitions` / `is_coordinator_for` set: that set is
    // recomputed on every metadata change and is transiently empty while
    // raft leadership settles on a freshly-booted broker, so it would
    // spuriously reject a legitimate leader's Produces (see the same
    // hazard documented for the transactional path below). The image
    // reflects committed leadership, so a just-elected leader's own image
    // already names it the leader; the only residual window is a follower
    // whose image hasn't yet caught up to a leadership change, which
    // correctly returns NOT_LEADER (the client retries against the new
    // leader) rather than appending to the wrong replica.
    //
    // Partition-level absence in the image (topic exists but this index
    // doesn't, or the topic is unknown) maps to UNKNOWN_TOPIC_OR_PARTITION
    // (3); presence-but-not-leader maps to NOT_LEADER_OR_FOLLOWER (6) with
    // a `current_leader` hint (encodes at Produce v10+, KIP-951) so the
    // client re-routes without a full Metadata round-trip. The hint names a
    // node id, and the response's `NodeEndpoints` carries that node's
    // advertised address: `node_endpoints::produce_node_endpoints` fills it
    // from these rows once every partition has been decided.
    let part = match validate_partition_gate(
        topic_name,
        idx,
        acks,
        partitions,
        log_dir_status,
        image,
        broker_policy,
    ) {
        Ok(ready) => ready,
        Err(error) => {
            out.error_code = error.code;
            if let Some(leader) = error.current_leader {
                out.current_leader = leader;
            }
            return Ok(Admission::Done(out));
        }
    };

    if !local_replica_is_ready(&part, image, topic_name, idx).await {
        out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
        out.current_leader =
            current_leader_hint(image.partition(topic_name, idx).expect("gate checked"));
        return Ok(Admission::Done(out));
    }

    // ── compacted topic: Kafka's `LogValidator.validateKey` ──────────
    if let Some(refusal) = record_validation_refusal(&out, &prepared, &part, topic_name) {
        return Ok(Admission::Done(refusal));
    }

    // ── KFC-7 schema validation ──────────────────────────────────────
    // Registry I/O is allowed only after both the metadata gate and the local
    // replica state have proved this broker is ready to lead the partition.
    // A misrouted Produce therefore cannot make a reachable follower fetch an
    // attacker-selected schema closure. The local state is checked again
    // immediately after the bounded registry operation.
    if let Some(gate) = schema
        && let Err(rejection) = validate_batch_schemas(
            &prepared,
            gate,
            schema_validator,
            topic_name,
            record_decompression_policy,
            metrics,
        )
        .await
    {
        out.error_code = codes::INVALID_RECORD;
        out.error_message = Some(SCHEMA_REJECTION_MESSAGE.to_owned());
        out.record_errors = rejection;
        return Ok(Admission::Done(out));
    }

    // KIP-890: verify before the duplicate lookup, outside the barrier (a
    // coordinator call); the log checks the guard under the append lock.
    let verification =
        match verify_before_append(&prepared, &part, (transaction, out.clone())).await {
            Ok(verification) => verification,
            Err(refused) => return Ok(Admission::Done(*refused)),
        };
    Ok(Admission::Admitted(Box::new(AdmittedBatch {
        prepared,
        part,
        shared_topic,
        delivery,
        acks,
        version: transaction.version,
        out,
        verification,
    })))
}

/// Whether the batch, as the writer will store it, is larger than the
/// partition's `segment.bytes`.
///
/// The limit is read off the partition's own log config, which is what the log
/// rolls segments by, so it is the topic's effective `segment.bytes`
/// whether the topic set it or inherited the broker's.
fn exceeds_segment_size(prepared: &PreparedBatch, part: &Partition) -> bool {
    let Ok(log) = part.log.lock() else {
        return false;
    };
    let config = log.config_snapshot();
    drop(log);
    prepared.appended_len(&config) > config.segment_size.bytes_usize()
}

/// The coordinator's answer applied to an admitted batch, then every stage
/// from the dedup gate to the append.
///
/// `coordinator_answer` is the coordinator's code for this partition from
/// [`verify_admitted`], `None` when the batch asked for none.
pub(super) async fn complete_partition(
    batch: AdmittedBatch,
    coordinator_answer: Option<i16>,
    services: PartitionServices<'_>,
) -> Result<PartitionOutcome, BrokerError> {
    let AdmittedBatch {
        prepared,
        part,
        shared_topic,
        delivery,
        acks,
        version,
        mut out,
        verification,
    } = batch;
    let PartitionServices {
        producer_state,
        image,
        phases,
        unstable_api_versions,
        ..
    } = services;
    let topic_name: &str = &shared_topic;
    let idx = out.index;
    let producer_check = match verification {
        Verification::Settled(check) => check,
        Verification::Coordinator(check) => match produce_verification_code(
            coordinator_answer.unwrap_or(codes::UNKNOWN_SERVER_ERROR),
            version,
        ) {
            (codes::NONE, _) => Some(check),
            (code, message) => {
                out.error_code = code;
                out.error_message = message;
                return Ok(PartitionOutcome::Done(out));
            }
        },
    };

    // Hold the transition barrier through dedup, enqueue, append, and ack.
    // Schema validation and verification released it around network I/O, so
    // repeat the local readiness proof before admitting any stateful gate.
    let Some(transition) = ready_transition(&part, image, topic_name, idx).await else {
        out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
        out.current_leader =
            current_leader_hint(image.partition(topic_name, idx).expect("gate checked"));
        return Ok(PartitionOutcome::Done(out));
    };
    let leader_epoch = part
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);

    // ── segment.bytes ────────────────────────────────────────────────
    // Kafka's `UnifiedLog.append` refuses a record set larger than the topic's
    // `segment.bytes` with `RecordBatchTooLargeException`, which is
    // `RECORD_LIST_TOO_LARGE`, and it does so ahead of the producer-state
    // analysis: a duplicate that is too large is refused too. A batch is never
    // split across segments, so the cap that keeps it out of a segment's
    // roll decision is the segment's own size, and `max.message.bytes` above
    // `segment.bytes` does not lift it. The log measures the whole record set
    // it appends, and `out` already carries the -1 `base_offset` of a row that
    // appended nothing.
    if exceeds_segment_size(&prepared, &part) {
        out.error_code = codes::RECORD_LIST_TOO_LARGE;
        return Ok(PartitionOutcome::Done(out));
    }

    // ── idempotent-producer dedup gate ───────────────────────
    match handle_duplicate(
        &prepared,
        producer_state,
        &part,
        (topic_name, idx),
        acks,
        unstable_api_versions,
    )
    .await
    {
        DedupOutcome::Append => {}
        DedupOutcome::Answered(response) => return Ok(PartitionOutcome::Done(response)),
        DedupOutcome::AwaitDurability { response, target } => {
            // A recognized retry under `acks=-1` waits for the same frontier a
            // fresh append of those records would have waited for, so it joins
            // the request's one overlapped gate rather than opening a second
            // kind of wait. It owes no producer-state commit: the append it
            // deduplicates against already recorded one.
            return Ok(PartitionOutcome::AwaitingHighWatermark(PendingAck::new(
                response,
                Arc::clone(&part),
                target,
                None,
                transition,
            )));
        }
    }

    // ── KFC-1 scheduled-delivery gate ───────────────────────
    // On a topic with `delivery.mode=scheduled` the batch's `max_timestamp` is
    // the time it becomes visible to a consumer. Two settings reject such a
    // batch, and both answer with the existing `INVALID_TIMESTAMP` (32) that
    // every client already classifies: a delivery time further ahead than
    // `delivery.max.delay.ms`, which is this gate, and, under
    // `delivery.schedule.monotonic`, one that precedes the largest delivery
    // time the partition already holds, which the log raises from
    // `Log::append` under the lock that writes the batch.
    //
    // `delivery.max.delay.ms` compares the batch against the broker's clock
    // and reads no log state, so nothing is gained by moving it down beside
    // the other one: a batch scheduled past the bound is refused wherever the
    // test runs, and refusing it here keeps the CRC-verified header the only
    // thing the writer queue ever sees for it.
    //
    // The gate runs after the dedup gate on purpose. An idempotent retry is not
    // a new entry in the schedule, and a partition that accepted a later batch
    // in between would otherwise answer that retry with INVALID_TIMESTAMP
    // instead of the offset it already assigned it. The monotonic check keeps
    // that ordering too: `handle_duplicate` answers a retry above, before
    // anything reaches the writer.
    if let Some(gate) = delivery
        && gate.rejects(prepared.max_timestamp, part.delivery.now_ms())
    {
        out.error_code = codes::INVALID_TIMESTAMP;
        return Ok(PartitionOutcome::Done(out));
    }

    let appended = dispatch_prepared(
        prepared,
        AppendContext {
            partition: &part,
            producer_state,
            partition_index: idx,
            acks,
            leader_epoch,
            phases,
            producer_check,
        },
        &shared_topic,
    )
    .await?;
    Ok(match appended {
        AppendOutcome::Answered(response) => PartitionOutcome::Done(response),
        AppendOutcome::AwaitDurability {
            response,
            target,
            commit,
        } => PartitionOutcome::AwaitingHighWatermark(PendingAck::new(
            response,
            Arc::clone(&part),
            target,
            commit,
            transition,
        )),
    })
}

/// Kafka's `LogValidator.processRecordErrors` message when at least one
/// `RecordError` in the batch came from the timestamp check: a fixed string,
/// not the per-record enumeration the general case uses, even when the same
/// batch also carries a keyless-record error.
const INVALID_TIMESTAMP_MESSAGE: &str = "One or more records have been rejected due to invalid \
                                          timestamp";

/// The row Kafka answers for a batch that carries a per-record validation
/// error: a keyless record on a compacted topic
/// (`LogValidator.validateKey`), a record whose timestamp falls outside the
/// topic's window (`LogValidator.validateTimestamp`), or both. `None` when
/// neither check refused anything.
///
/// Kafka's `processRecordErrors` merges every `RecordError` the batch earned
/// into one list and picks the top-level message by whether any of them came
/// from the timestamp check: `INVALID_TIMESTAMP` with the fixed message if
/// so, `INVALID_RECORD` with the enumerated one otherwise. The merged list
/// itself keeps every entry either way, in the batch's record order, because
/// the two checks never both fire on the same record: `validateRecord` checks
/// a record's key first, and skips the timestamp check for that record when
/// the key check already refused it.
fn record_validation_refusal(
    out: &PartitionProduceResponse,
    prepared: &PreparedBatch,
    part: &Partition,
    topic_name: &str,
) -> Option<PartitionProduceResponse> {
    if prepared.keyless_records.is_empty() && prepared.invalid_timestamp_records.is_empty() {
        return None;
    }
    let partition_label = format!("{topic_name}-{}", out.index);
    let mut out = out.clone();
    let keyless = prepared
        .keyless_records
        .iter()
        .map(|&batch_index| BatchIndexAndErrorMessage {
            batch_index,
            batch_index_error_message: Some(format!(
                "Compacted topic cannot accept message without key in topic partition \
                 {partition_label}"
            )),
            ..Default::default()
        });
    let mut record_errors: Vec<BatchIndexAndErrorMessage> = keyless
        .chain(prepared.invalid_timestamp_records.iter().cloned())
        .collect();
    record_errors.sort_by_key(|error| error.batch_index);
    if prepared.invalid_timestamp_records.is_empty() {
        out.error_code = codes::INVALID_RECORD;
        out.error_message = Some(record_errors_message(&record_errors));
    } else {
        out.error_code = codes::INVALID_TIMESTAMP;
        out.error_message = Some(INVALID_TIMESTAMP_MESSAGE.to_owned());
    }
    out.record_errors = record_errors;
    // Kafka's `processFailedRecord` reads the log start offset for the row.
    out.log_start_offset = part.log_start_offset().0;
    Some(out)
}

/// The message of the `InvalidRecordException` that Kafka's
/// `LogValidator.processRecordErrors` throws, which the partition row carries
/// as `error_message`. Java's `List.toString` renders at most the first three
/// `RecordError`s.
fn record_errors_message(errors: &[BatchIndexAndErrorMessage]) -> String {
    let shown: Vec<String> = errors
        .iter()
        .take(3)
        .map(|error| {
            let message = error
                .batch_index_error_message
                .as_deref()
                .map_or_else(|| "null".to_owned(), |message| format!("'{message}'"));
            format!(
                "RecordError(batchIndex={}, message={message})",
                error.batch_index
            )
        })
        .collect();
    format!(
        "One or more records have been rejected due to {} record errors in total, and only \
         showing the first three errors at most: [{}]",
        errors.len(),
        shown.join(", ")
    )
}

#[cfg(test)]
mod tests;
