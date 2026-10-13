//! `Produce` (`api_key=0`).
//!
//! The handler routes each partition's records to that partition's
//! writer-actor and waits for the assigned base offset.
//!
//! There is one `RecordBatch` per (topic, partition) per request. The
//! generated `PartitionProduceData.records` field is `Option<RecordsPayload>`.
//! Versions 0-2 carry a legacy v0/v1 `MessageSet`, which the handler
//! up-converts to a v2 `RecordBatch` before the append. Versions 3+ carry a
//! native v2 `RecordBatch`. The handler fully supports clients that send a
//! single v2 batch per partition, which is the typical modern case.

#[cfg(test)]
use krabka_protocol::{
    owned::{
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::RecordsPayload,
};
use {
    self::{
        append::PendingAck,
        authorization::{authorize_produce_topics, is_authorized_transactional},
        delivery::resolve_delivery_gate,
        framing::decode_produce_request,
        leadership::{BrokerProducePolicy, current_effective_min_isr},
        pipeline::{
            Admission, AdmittedBatch, PartitionInput, PartitionOutcome, PartitionServices,
            admit_partition, complete_partition, verify_admitted,
        },
        producer_checks::TransactionRequest,
        response::build_topic_error_response,
        throttle::finish_produce_response,
        topic_settings::{
            broker_default_timestamp_policy, resolve_compacted_topic, resolve_timestamp_policy,
            resolve_topic_compression,
        },
    },
    crate::{
        broker::Broker,
        codes,
        config_keys::{BrokerLogDefaults, resolve_max_message_bytes, resolve_schema_validation},
        error::BrokerError,
        freeze::resolve::resolve_freeze_mutation,
        handlers::{ApiKeyCode, RequestContext},
        internal_topics::{is_internal_topic, produce_internal_topics_allowed},
        metrics::RequestPhases,
        partition::Partition,
        topic_resolve::resolve,
    },
    bytes::Bytes,
    krabka_protocol::owned::produce_response::{PartitionProduceResponse, TopicProduceResponse},
    krabka_verified::FreezeMutationKind,
    std::{sync::Arc, time::Duration},
};

mod append;
mod authorization;
mod delivery;
mod framing;
/// Benchmark seam over `prepare_batch`, `build_produce_data` and the log
/// append behind them. `benches/produce.rs` is its only caller; the module is
/// gated so a release build of the broker never carries it.
#[cfg(any(test, feature = "test-helpers"))]
pub mod hot_path;
mod leadership;
mod node_endpoints;
mod owned_decode;
mod pipeline;
mod prepare;
mod producer_checks;
mod record_limit;
mod response;
mod schema;
mod throttle;
mod topic_settings;

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod broker_default_tests;
#[cfg(test)]
mod compacted_key_tests;
#[cfg(test)]
mod topic_resolution_tests;
#[cfg(test)]
mod transaction_check_tests;
#[cfg(test)]
mod transactional_authorization_tests;

/// Kafka `acks` sentinel `-1`, which is producer `acks=all`. The leader must
/// hold the response until the high watermark covers the append, that is,
/// until every in-sync replica has it.
const ACKS_ALL: i16 = -1;

/// Map one admitted batch to the exact exclusive frontier used by both fresh
/// append and duplicate `acks=all` waits.
fn durability_frontier(base_offset: i64, last_offset_delta: i32) -> Option<krabka_log::Offset> {
    krabka_verified::produce_durability_frontier(base_offset, last_offset_delta)
        .map(krabka_log::Offset)
}

/// Kafka's `ReplicaManager.isValidRequiredAcks` admits only 0, 1 and -1.
/// `sendInvalidRequiredAcksResponse` answers every partition of the request
/// with `INVALID_REQUIRED_ACKS`, with every offset field at its -1 sentinel,
/// and appends nothing -- there is no partial credit for an otherwise-valid
/// batch on an invalid `acks`.
///
/// `None` when `acks` is one of the three valid values, so the caller
/// continues the ordinary path. `Some` is the response `handle` returns
/// as-is.
/// The request-specific fields [`invalid_required_acks_response`] needs
/// beside the response-building context every early-return branch of
/// `handle` shares (`broker`, `image`, `ctx`).
/// Bundled into one parameter so the function stays under Clippy's
/// argument-count lint. Every field is a reference or itself `Copy`, so this
/// is too, and passes by value like they would have.
#[derive(Clone, Copy)]
struct AcksRequest<'a> {
    topic_data: &'a [framing::FramedTopic],
    acks: i16,
    version: i16,
    handler_start: std::time::Instant,
}

fn invalid_required_acks_response(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
    request: AcksRequest<'_>,
) -> Option<Result<Bytes, BrokerError>> {
    let AcksRequest {
        topic_data,
        acks,
        version,
        handler_start,
    } = request;
    if matches!(acks, 0 | 1 | ACKS_ALL) {
        return None;
    }
    let topic_results: Vec<TopicProduceResponse> = topic_data
        .iter()
        .map(|topic| build_topic_error_response(topic, codes::INVALID_REQUIRED_ACKS))
        .collect();
    Some(finish_produce_response(
        broker,
        image,
        ctx,
        (acks != 0).then_some(handler_start),
        topic_results,
        version,
    ))
}

/// This handler's own wire api key, for the `api_key` label the request-phase
/// and throttle histograms carry. The dispatcher labels the total latency from
/// the frame it parsed; the handler labels its phases from the same number.
pub(super) const PRODUCE_API_KEY: ApiKeyCode = krabka_protocol::api_key::ApiKey::Produce as i16;

/// The first `Produce` version that names each topic by `topic_id` only
/// (KIP-516). The request schema carries `name` at versions 0-12 and
/// `topic_id` from this version on.
const FIRST_TOPIC_ID_VERSION: i16 = 13;

/// Wire sentinel "no offset assigned", which is
/// `ProduceResponse.INVALID_OFFSET`. The handler stamps it on partition rows
/// that failed before any append happened.
const INVALID_OFFSET: i64 = -1;

/// Wire sentinel "this topic does not stamp a log-append time", which is
/// Kafka's `RecordBatch.NO_TIMESTAMP` in the `logAppendTimeMs` response field.
/// Every `message.timestamp.type=CreateTime` topic answers with it, which is
/// every topic that did not ask for `LogAppendTime`.
const NO_LOG_APPEND_TIME: i64 = -1;

/// Wire sentinel "leader unknown" for the KIP-951 `current_leader` hint. The
/// handler uses it when the leader's `NodeId` does not fit the wire's `i32`.
const NO_LEADER_ID: i32 = -1;

#[tracing::instrument(
    name = "handle_produce",
    level = "info",
    skip_all,
    fields(api = "Produce", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    req_bytes: &[u8],
    body_bytes: Bytes,
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    // KIP-124 request_percentage meters server-side handler time; capture the
    // start so the request throttle can be combined with the byte-rate throttle
    // below (KIP-219).
    let handler_start = std::time::Instant::now();
    // Phase accounting for `request_{local,remote}_duration_seconds`. One
    // Produce appends to many partitions, so the local phase is a sum over the
    // partition loop below: `dispatch_prepared` charges each writer round-trip
    // to it. The remote phase is a single interval, because the `acks=all`
    // high-watermark waits of every appended partition run as one wait.
    let phases = RequestPhases::default();
    let record_decompression_policy = broker.config.record_decompression_policy()?;
    let partitions = broker.partitions.clone();
    let controller = broker.controller.clone();
    let producer_state = broker.producer_state.clone();
    let txn_coordinator = broker.txn_coordinator.clone();
    let log_dir_status = broker.log_dir_status.clone();
    let broker_policy = BrokerProducePolicy {
        node_id: broker.config.node_id,
        default_min_insync_replicas: broker.config.default_min_insync_replicas,
        is_witness: broker.config.is_witness(),
    };
    // ── request decode (header-only on the verbatim-eligible path) ──
    // For v≥3 (native v2 payloads) we decode only the request FRAMING —
    // `transactional_id`, `acks`, `timeout_ms`, and per-topic / per-partition
    // headers plus each partition's `records` field as a zero-copy `Bytes`
    // slice of the request frame — via `produce_framing`. The record BODIES
    // are NOT decoded or decompressed here. Per-partition validation later
    // parses every record and transiently decompresses compressed bodies, but
    // skips owned-record materialization and preserves the original wire
    // bytes whenever no conversion is required. The owned
    // `RecordBatch` is decoded lazily, per partition, ONLY when the
    // verbatim-passthrough predicate fails (legacy magic, control batch,
    // log-append-time, broker-side recompression, multi-batch slice, or a
    // wire-null / undecodable field) — see `process_partition` /
    // `build_produce_data`.
    //
    // Legacy v0-2 requests carry a v0/v1 `MessageSet` that is always
    // up-converted (never passthrough-eligible), so they take the full owned
    // decode and feed every partition the owned path directly.
    let req = decode_produce_request(req_bytes, body_bytes, version)?;
    let acks = req.acks;
    let timeout = Duration::from_millis(u64::try_from(req.timeout_ms.max(0)).unwrap_or(0));

    // ── ACL preamble, part 1: the whole-request transactional check ───
    // Kafka's `KafkaApis.handleProduceRequest`:
    //
    //   if (RequestUtils.hasTransactionalRecords(produceRequest)) {
    //     val isAuthorizedTransactional = produceRequest.transactionalId != null &&
    //       authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, produceRequest.transactionalId)
    //     if (!isAuthorizedTransactional) {
    //       sendErrorResponseMaybeThrottle(request, TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
    //       return
    //     }
    //   }
    //
    // `hasTransactionalRecords` looks at the BATCHES, not at whether the
    // request carries a `transactional_id`. A transactional batch (producer
    // id + the KIP-98 transactional attribute) with no request-level
    // `transactional_id` still trips this gate, and is refused here rather
    // than reaching `verify_transactional_produce`, which would otherwise
    // resolve *some* transactional id for the batch's producer id and let an
    // unauthorized principal write into that transaction. A request that
    // carries a `transactional_id` but only non-transactional batches never
    // reaches this check at all, matching Kafka.
    //
    // Kafka answers every partition row of the request with 53 and returns
    // before any topic is resolved. An id that would otherwise answer
    // UNKNOWN_TOPIC_ID at v13+ still answers 53. So this runs ahead of the
    // topic-resolution loop below, not inside it.
    let image = controller.current_image();
    // The broker-wide defaults of the topic keys checked below: the static
    // startup values under the dynamic broker configs of this node and the
    // cluster, which Kafka applies to every log at once. A topic override wins.
    let log_defaults = BrokerLogDefaults::for_broker(&image, &broker.config);
    let broker_timestamp_default = broker_default_timestamp_policy(
        log_defaults.message_timestamp_type,
        log_defaults.message_timestamp_before_max_ms,
        log_defaults.message_timestamp_after_max_ms,
    );

    if req.has_transactional_batch()
        && !is_authorized_transactional(broker, &image, ctx, req.transactional_id.as_deref())
    {
        let topic_results: Vec<TopicProduceResponse> = req
            .topic_data
            .iter()
            .map(|topic| {
                build_topic_error_response(topic, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
            })
            .collect();
        if acks == 0 {
            ctx.mark_close_after_response();
        }
        return finish_produce_response(
            broker,
            &image,
            ctx,
            (acks != 0).then_some(handler_start),
            topic_results,
            version,
        );
    }

    // ── acks validity ──────────────────────────────────────────────────
    // This runs after the transactional check above, matching Kafka's own
    // order in `KafkaApis.handleProduceRequest`: `hasTransactionalRecords` is
    // checked directly, while `isValidRequiredAcks` is checked later, inside
    // `ReplicaManager.appendRecords`.
    if let Some(result) = invalid_required_acks_response(broker, &image, ctx, AcksRequest {
        topic_data: &req.topic_data,
        acks,
        version,
        handler_start,
    }) {
        return result;
    }

    // ── ACL preamble, part 2: per-topic `Write` ────────────────────────
    // Batch-authorize every topic in the request for `Write` (the operation
    // Produce requires). Topics that come back `Deny` short-circuit the
    // per-partition append below and emit TOPIC_AUTHORIZATION_FAILED on
    // every partition row of that topic.
    let denied_topics = authorize_produce_topics(broker, &image, ctx, &req);

    let mut topic_results: Vec<TopicProduceResponse> = Vec::with_capacity(req.topic_data.len());
    // The `acks=-1` partitions of this request that have appended and are
    // waiting for the high watermark to cover them. Kafka registers one
    // `DelayedProduce` per request covering every such partition, so the waits
    // overlap and the request's `timeout.ms` bounds all of them together; this
    // is that set, and `await_durability` below is that one wait. Each entry
    // remembers where its row goes, because the response is built topic by
    // topic while the waits are driven once, after every partition has
    // appended.
    let mut awaiting: Vec<PendingPartition> = Vec::new();
    // The partitions that passed every gate before the transaction check, in
    // request order, with the rows they reserved.
    let mut admitted: Vec<AdmittedPartition> = Vec::new();
    let transaction =
        TransactionRequest::new(broker, &image, (req.transactional_id.as_deref(), version));
    let services = PartitionServices {
        partitions: &partitions,
        txn_coordinator: &txn_coordinator,
        producer_state: &producer_state,
        log_dir_status: &log_dir_status,
        image: &image,
        broker_policy,
        record_decompression_policy,
        metrics: &broker.metrics,
        phases: &phases,
        schema_validator: broker.config.schema_validator.as_ref(),
        unstable_api_versions: broker.config.features.unstable_api_versions,
    };

    for topic in req.topic_data {
        // v ≤ 12 sends the topic name; v ≥ 13 sends only topic_id and
        // we look it up in the metadata image. Kafka's
        // `KafkaApis.handleProduceRequest` answers UNKNOWN_TOPIC_ID (100) on
        // every partition row when an id-only version names a topic that
        // does not resolve. That includes the zero id, which the resolver
        // sends down the name path with an empty name. At v ≤ 12 a name
        // that does not resolve continues to the ACL and partition gates,
        // which answer TOPIC_AUTHORIZATION_FAILED or
        // UNKNOWN_TOPIC_OR_PARTITION.
        // The metric label sets hold an `Arc<str>`, and this loop body builds
        // one per partition, so take the registry's own copy of the name: for
        // a topic this broker hosts — which is every topic that gets past the
        // gates below — the per-partition accounting is then a refcount bump
        // and no allocation at all.
        let topic_name: Arc<str> = match resolve(&image, &topic.name, topic.topic_id) {
            Ok(rec) => partitions.shared_topic_name(&rec.name),
            Err(codes::UNKNOWN_TOPIC_OR_PARTITION) if version < FIRST_TOPIC_ID_VERSION => {
                partitions.shared_topic_name(&topic.name)
            }
            Err(codes::UNKNOWN_TOPIC_OR_PARTITION) => {
                topic_results.push(build_topic_error_response(&topic, codes::UNKNOWN_TOPIC_ID));
                continue;
            }
            Err(code) => {
                topic_results.push(build_topic_error_response(&topic, code));
                continue;
            }
        };

        // Account for the topic in Prometheus before consuming
        // `partition_data`. We count even for authorize-denied / unknown-topic
        // paths since the produce *request* arrived; that mirrors Kafka's
        // `BrokerTopicMetrics` semantics.
        record_topic_produce_metrics(broker, &topic_name, &topic);

        let mut partition_results: Vec<PartitionProduceResponse> =
            Vec::with_capacity(topic.partition_data.len());

        // If the topic was denied by the ACL preamble, every
        // partition row for it gets TOPIC_AUTHORIZATION_FAILED and the
        // real append is skipped.
        let topic_denied = denied_topics.contains(&*topic_name);

        // Resolve the topic's broker-side `compression.type` once. `None`
        // means Kafka's `producer` pass-through (no recompression). A
        // concrete codec forces recompression of any batch whose codec
        // differs — those batches must take the owned path. Mirrors the
        // writer's `config_snapshot().compression_type` gate so the
        // handler's verbatim decision matches the writer's recompression
        // decision exactly.
        let topic_compression =
            resolve_topic_compression(&image, &topic_name, log_defaults.compression_type);

        // Kafka's `max.message.bytes`, resolved here for the same reason: the
        // cap belongs to the topic. A topic that sets none inherits the
        // broker's `message.max.bytes`, which is the `DEFAULT_CONFIG` synonym
        // `kafka-configs --describe --all` reports for the key.
        let max_message_bytes =
            resolve_max_message_bytes(&image, &topic_name, log_defaults.max_message_size);

        // Resolve the topic's KFC-1 delivery settings once, beside the
        // compression resolve and for the same reason: they are a property of
        // the topic, not of a partition or of a batch. `None` is
        // `delivery.mode=immediate`, the default, and every partition of such a
        // topic then skips the delivery gate without reading a clock, a
        // timestamp, or the log.
        let delivery = resolve_delivery_gate(&image, &topic_name);

        // KIP-32, resolved here for the same reason again: whose clock the
        // records carry, and how far a producer timestamp may sit from the
        // broker's, are properties of the topic. The default is `CreateTime`
        // with both windows open, so a topic that configured neither costs
        // every partition one boolean test and reads no clock.
        let timestamps = resolve_timestamp_policy(&image, &topic_name, broker_timestamp_default);

        // Kafka's `LogConfig.compact`, resolved once per topic: a compacted
        // topic refuses a record with no key.
        let compacted_topic =
            resolve_compacted_topic(&image, &topic_name, log_defaults.cleanup_policy);

        // KFC-7, resolved here for the same reason: schema validation is a
        // property of the topic. `None` is the default, and every partition of
        // such a topic then skips the check without reading a record body.
        let schema = resolve_schema_validation(&image, &topic_name);

        // KFC-9, resolved here for the same reason once more: a write freeze
        // is a property of the topic. `None` is a topic that accepts writes, and
        // the image answers an empty registry in two emptiness tests, so every
        // partition of an unfrozen topic then pays one test of an `Option` and
        // nothing else. The borrow allocates nothing, and only a refused row
        // turns the entry into a `FreezeVerdict` and a message.
        let freeze = resolve_freeze_mutation(
            &image,
            &topic_name,
            !topic_denied,
            FreezeMutationKind::Produce,
        );

        // Kafka's internal-topic gate, resolved here for the same reason as
        // the freeze above: it is a property of the topic and of the request
        // (its `client_id`), not of a partition or a batch. The group,
        // transaction and share coordinators never reach this handler at all.
        // They append to their own topics through the partition writer
        // directly, so this is the only path an ordinary client Produce can
        // take to `__consumer_offsets`, `__transaction_state` or
        // `__share_group_state`, and the one Kafka refuses unless `client_id`
        // is its own admin tooling's `"__admin_client"`.
        let internal_topic_denied = is_internal_topic(&broker.config, &topic_name)
            && !produce_internal_topics_allowed(ctx.client_id.unwrap_or_default());

        for part_data in topic.partition_data {
            let idx = part_data.index;
            // Time the per-partition handler work for the rebalancer's
            // CpuUsage / CpuCapacity goals, the way the fetch read loop times
            // its per-partition read: one `Instant` delta around each stage.
            // The interval covers this partition's gates, its enqueue and the
            // writer's answer, and stops there — the `acks=-1` high-watermark
            // wait is not in it, because that wait is one wait for the whole
            // request and charging it to each partition would count it N times.
            // The transaction coordinator call is not in it either: it is one
            // call for the whole request too.
            let started = std::time::Instant::now();
            let admission = admit_partition(
                PartitionInput {
                    part_data,
                    topic_compression,
                    timestamps,
                    compacted_topic,
                    max_message_bytes,
                    delivery,
                    schema,
                    topic_name: topic_name.clone(),
                    freeze,
                    internal_topic_denied,
                    transaction,
                    acks: req.acks,
                },
                services,
            )
            .await?;
            let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
            match admission {
                Admission::Done(out) => {
                    record_partition_cpu(broker, &topic_name, idx, micros);
                    record_partition_failure(broker, &topic_name, out.error_code);
                    partition_results.push(out);
                }
                Admission::Admitted(batch) => {
                    // The row is decided once the coordinator has answered
                    // and the batch has appended. Reserve its slot with the
                    // `UNKNOWN_LOG_APPEND_INFO` sentinel every pre-append
                    // refusal carries, so a row is never absent.
                    admitted.push(AdmittedPartition {
                        topic_row: topic_results.len(),
                        partition_row: partition_results.len(),
                        topic_name: topic_name.clone(),
                        idx,
                        micros,
                        batch,
                    });
                    partition_results.push(PartitionProduceResponse {
                        index: idx,
                        base_offset: INVALID_OFFSET,
                        ..Default::default()
                    });
                }
            }
        }

        topic_results.push(TopicProduceResponse {
            name: topic_name.to_string(),
            topic_id: topic.topic_id,
            partition_responses: partition_results,
            ..Default::default()
        });
    }

    // ── the one transaction check, then every append ────────────────
    complete_admitted(
        broker,
        admitted,
        services,
        transaction,
        &mut topic_results,
        &mut awaiting,
    )
    .await?;

    // ── the one acks=-1 high-watermark wait ─────────────────────────
    await_durability(broker, awaiting, &mut topic_results, timeout, &phases).await;

    // The suppressed response is the only signal an acks=0 producer gets, so
    // Kafka closes the connection on it rather than mute it. `acks != 0`
    // requests always answer, so the flag is never set for them.
    if acks == 0 && any_partition_failed(&topic_results) {
        ctx.mark_close_after_response();
    }

    // The local and remote phases are complete once the partition loop and the
    // wait above are. The throttle below is the third phase, and
    // `finish_produce_response` observes that one itself.
    broker
        .metrics
        .observe_request_phases(PRODUCE_API_KEY, &phases);

    // ── KIP-13 producer_byte_rate + KIP-124 request_percentage ──────
    // Combine the data (byte-rate) and request (handler-time) throttles as
    // their max, surface it in throttle_time_ms, and record it on `ctx` so the
    // dispatch loop mutes the channel once the response is written (KIP-219).
    // The dispatch loop skips request_percentage for Produce so it is charged
    // exactly once, here.
    finish_produce_response(
        broker,
        &image,
        ctx,
        (acks != 0).then_some(handler_start),
        topic_results,
        version,
    )
}

/// One partition of this request that passed every gate before the
/// transaction check, and the place in the response its row belongs.
struct AdmittedPartition {
    topic_row: usize,
    partition_row: usize,
    topic_name: Arc<str>,
    idx: i32,
    /// The handler time the gates before the transaction check took.
    micros: u64,
    batch: Box<AdmittedBatch>,
}

/// Ask the transaction coordinator about every admitted partition at once,
/// then append each one, and write its row or queue its high-watermark wait.
///
/// Kafka's `ReplicaManager.handleProduceAppend` asks the coordinator about
/// every partition of the request in one `AddPartitionsToTxn` call, and
/// appends once it has answered.
async fn complete_admitted(
    broker: &Broker,
    admitted: Vec<AdmittedPartition>,
    services: PartitionServices<'_>,
    transaction: TransactionRequest<'_>,
    topic_results: &mut [TopicProduceResponse],
    awaiting: &mut Vec<PendingPartition>,
) -> Result<(), BrokerError> {
    let answers = verify_admitted(
        &admitted
            .iter()
            .map(|partition| &*partition.batch)
            .collect::<Vec<_>>(),
        services.txn_coordinator,
        services.image,
        transaction,
    )
    .await;
    for (partition, answer) in admitted.into_iter().zip(answers) {
        let AdmittedPartition {
            topic_row,
            partition_row,
            topic_name,
            idx,
            micros,
            batch,
        } = partition;
        let started = std::time::Instant::now();
        let outcome = complete_partition(*batch, answer, services).await?;
        let micros =
            micros.saturating_add(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
        record_partition_cpu(broker, &topic_name, idx, micros);
        match outcome {
            PartitionOutcome::Done(out) => {
                record_partition_failure(broker, &topic_name, out.error_code);
                topic_results[topic_row].partition_responses[partition_row] = out;
            }
            PartitionOutcome::AwaitingHighWatermark(ack) => {
                // The row is decided by the one high-watermark wait.
                awaiting.push(PendingPartition {
                    topic_row,
                    partition_row,
                    topic_name,
                    ack,
                });
            }
        }
    }
    Ok(())
}

/// Charge `micros` of handler time to one partition for the rebalancer's
/// `CpuUsage` / `CpuCapacity` goals.
fn record_partition_cpu(broker: &Broker, topic_name: &Arc<str>, idx: i32, micros: u64) {
    if !topic_name.is_empty() {
        broker
            .metrics
            .record_partition_cpu_micros(topic_name, idx, micros);
    }
}

/// One partition of this request whose row is decided by the high-watermark
/// wait, and the place in the response that row belongs.
struct PendingPartition {
    topic_row: usize,
    partition_row: usize,
    /// The topic's shared name, for the per-partition failure metric the row
    /// earns once the wait has decided its error code. It is the registry's
    /// own `Arc<str>`, so holding it here costs a refcount and no allocation.
    topic_name: Arc<str>,
    ack: PendingAck,
}

/// Drive every appended `acks=-1` partition's high-watermark wait together and
/// write each answer into the row it reserved.
///
/// This is Kafka's one `DelayedProduce` per request. `ReplicaManager
/// .appendRecords` appends to every partition of the request first and then
/// registers a single delayed operation covering all of them with the
/// request's `timeout.ms`, so N partitions of one request cost one replication
/// round trip and not N of them, and one timeout bounds the whole request
/// rather than each partition in turn.
///
/// The deadline is computed once, here, for the same reason: it starts when
/// the appends are done, which is when Kafka's `DelayedProduce` is registered.
///
/// The whole wait is the request's remote phase. Everything it waits for is a
/// follower taking an append this broker already made durable, and it is
/// charged once because it happened once.
async fn await_durability(
    broker: &Broker,
    awaiting: Vec<PendingPartition>,
    topic_results: &mut [TopicProduceResponse],
    timeout: Duration,
    phases: &RequestPhases,
) {
    if awaiting.is_empty() {
        return;
    }
    let started = std::time::Instant::now();
    let deadline = started + timeout;
    let min_isr_layers = (
        broker.config.node_id,
        broker.config.default_min_insync_replicas,
    );
    let current_min_isr = |partition: &Partition, admitted_topic_id| {
        current_effective_min_isr(
            &broker.controller.current_image(),
            (&partition.topic, partition.index.0),
            admitted_topic_id,
            min_isr_layers,
        )
    };
    let finished = futures_util::future::join_all(awaiting.into_iter().map(|pending| {
        let PendingPartition {
            topic_row,
            partition_row,
            topic_name,
            ack,
        } = pending;
        async move {
            (
                topic_row,
                partition_row,
                topic_name,
                ack.finish(deadline, current_min_isr).await,
            )
        }
    }))
    .await;
    phases.add_remote(started.elapsed());
    for (topic_row, partition_row, topic_name, row) in finished {
        record_partition_failure(broker, &topic_name, row.error_code);
        topic_results[topic_row].partition_responses[partition_row] = row;
    }
}

/// Per-partition failure accounting. Bumps once per partition whose response
/// carries a non-zero error code (`TOPIC_AUTHORIZATION_FAILED`,
/// `NOT_ENOUGH_REPLICAS`, `INVALID_RECORD`, etc.) — mirrors JVM's
/// `failedProduceRequestRate.mark()`.
fn record_partition_failure(broker: &Broker, topic_name: &Arc<str>, error_code: i16) {
    if !topic_name.is_empty() && error_code != 0 {
        broker.metrics.record_failed_produce(topic_name);
    }
}

/// Sum one topic's per-partition records-field wire length into the bytes-in
/// counter and the per-batch record count into `messages_in_total`.
///
/// The wire length is the producer's compressed bytes on the verbatim path —
/// the true on-the-wire payload, no decompression needed. V2 payloads expose
/// `records.len()` directly for the message count; legacy `MessageSet`
/// payloads remain opaque here and the upconversion-time accounting already
/// counts those arrivals. A topic name resolved to empty (an unresolved id at
/// v13+) is not accounted, because there is no topic to label the metric
/// with.
fn record_topic_produce_metrics(
    broker: &Broker,
    topic_name: &Arc<str>,
    topic: &framing::FramedTopic,
) {
    if topic_name.is_empty() {
        return;
    }
    let mut topic_bytes: u64 = 0;
    let mut topic_messages: u64 = 0;
    for p in &topic.partition_data {
        let partition_bytes = p.payload.payload_len() as u64;
        broker
            .metrics
            .record_partition_produce(topic_name, p.index, partition_bytes);
        topic_bytes += partition_bytes;
        topic_messages += p.payload.message_count();
    }
    broker.metrics.record_produce(topic_name, topic_bytes);
    broker
        .metrics
        .record_produce_messages(topic_name, topic_messages);
}

/// Whether any partition row of this response carries a non-zero error code.
fn any_partition_failed(topic_results: &[TopicProduceResponse]) -> bool {
    topic_results
        .iter()
        .flat_map(|topic| &topic.partition_responses)
        .any(|partition| partition.error_code != 0)
}

/// Whether Kafka requires a response for this Produce request. `acks=0`
/// requests are one-way even when an append or authorization check fails.
pub(crate) fn response_required(
    request_bytes: &[u8],
    body_bytes: Bytes,
    version: i16,
) -> Result<bool, BrokerError> {
    Ok(decode_produce_request(request_bytes, body_bytes, version)?.acks != 0)
}

#[cfg(test)]
mod durability_tests {
    use super::durability_frontier;

    #[test]
    fn adapter_rejects_malformed_and_overflowing_frontiers() {
        assert2::assert!(durability_frontier(10, 2) == Some(krabka_log::Offset(13)));
        assert2::assert!(durability_frontier(10, -1).is_none());
        assert2::assert!(durability_frontier(i64::MAX, 0).is_none());
    }
}
