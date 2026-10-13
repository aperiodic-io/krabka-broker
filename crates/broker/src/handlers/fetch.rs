//! `Fetch` (`api_key=1`) with long-poll support through per-partition
//! `Notify::notified()` futures.
//!
//! The handler returns records as verbatim `RecordsPayload::Raw` bytes. These
//! are the on-disk `.log` bytes for whole v2 batches. `Log::read_raw` reads
//! them decode-free, and the handler clamps them at the visibility window:
//! the high watermark for `read_uncommitted` consumer fetches, `lso.min(hw)`
//! for `read_committed`, and the log-end offset (LEO) for follower fetches.
//!
//! KFC-1 adds one more cap to a consumer fetch, and only to a consumer fetch:
//! the partition's delivery watermark, which is the first offset whose batch
//! has not reached its activation time. The handler recomputes it under the log
//! mutex it already holds, so it can never serve a record before it is due. A
//! topic that delivers immediately answers the log end offset for it with no
//! I/O, so that cap never binds and the zero-copy path is untouched.
//!
//! `read_committed` does NO server-side batch filtering. Aborted batches and
//! control batches stay in the byte stream, and the consumer drops them on
//! the client side with the `aborted_transactions` list. This matches Apache
//! Kafka.

use {
    bytes::BytesMut,
    krabka_log::Offset,
    krabka_protocol::{
        Decode, Encode,
        owned::{fetch_request::FetchRequest, fetch_response::FetchResponse},
    },
    krabka_units::convert::ByteSizeExt as _,
};

mod node_endpoints;
mod plan;
mod read;
mod read_loop;
mod remote;
mod request;
mod response;
mod session;
#[cfg(test)]
mod test_support;
mod throttle;

#[cfg(test)]
mod broker_limit_tests;
#[cfg(test)]
mod follower_authorization_tests;
#[cfg(test)]
mod follower_watermark_tests;
#[cfg(test)]
mod response_budget_tests;

#[cfg(test)]
mod leader_read_tests;
#[cfg(test)]
mod topic_resolution_tests;

pub(crate) use self::{plan::PendingRead, read::LiveOffsets};
use {
    self::{
        node_endpoints::fetch_node_endpoints,
        plan::{PendingPlanContext, build_pending_reads},
        read_loop::execute_pending_reads,
        request::{FetchPreparation, prepare_fetch},
        response::{
            charge_fetch_cpu, downconvert_legacy_responses, record_fetch_metrics,
            withhold_current_leader_before_kip_951,
        },
        session::{commit_fetch_session, prepare_fetch_session, refresh_fetch_session_metrics},
        throttle::{
            apply_consumer_fetch_quota, consumer_quota_window_bytes, fetch_response_size,
            throttle_follower_responses,
        },
    },
    crate::{
        broker::Broker,
        codes::CLUSTER_AUTHORIZATION_FAILED,
        error::BrokerError,
        fetch_session::INVALID_SESSION_ID,
        handlers::{ApiKeyCode, RequestContext},
        metrics::RequestPhases,
        network::fetch_writer::records_to_serve,
        wal::quorum::wire::{KIP_595_FETCH_VERSION, decode_fetch_request},
    },
};

/// This handler's own wire api key, for the `api_key` label the request-phase
/// and throttle histograms carry. The dispatcher labels the total latency from
/// the frame it parsed; the handler labels its phases from the same number.
const FETCH_API_KEY: ApiKeyCode = krabka_protocol::api_key::ApiKey::Fetch as i16;

/// First `Fetch` response version that carries the KIP-951 `CurrentLeader`
/// hint and its `NodeEndpoints` companion. `NodeEndpoints` is a tagged field
/// from v16. `CurrentLeader` is one from v12, but Kafka fills it only from
/// v16.
const KIP_951_FETCH_VERSION: i16 = 16;

/// The first `Fetch` version that names each topic by `topic_id` only
/// (KIP-516). The request schema carries `Topic` at versions 0-12 and
/// `TopicId` from this version on.
const FIRST_TOPIC_ID_VERSION: i16 = 13;

/// The whole-response byte budget for one Fetch read. Follower reads use the
/// request budget; consumers also honor the broker and client-quota ceilings.
fn fetch_response_byte_budget(
    request_max_bytes: i32,
    broker_max_bytes: usize,
    quota_window_bytes: usize,
    is_follower_fetch: bool,
) -> usize {
    let request_max_bytes = usize::try_from(request_max_bytes.max(0)).unwrap_or(usize::MAX);
    if is_follower_fetch {
        request_max_bytes
    } else {
        request_max_bytes
            .min(broker_max_bytes)
            .min(quota_window_bytes)
    }
}

/// Handle a `Fetch` request and return the not-yet-encoded response
/// **struct** with the negotiated `version`.
///
/// The dispatch layer turns the struct into a zero-copy write-plan for v4+
/// with the canonical codec, or into a legacy copy-encoded frame for v0–v3.
/// The function returns the struct and not `Bytes`, so the connection writer
/// can split out each partition's records region as a separate write segment.
/// It does not need to materialize the whole body.
#[tracing::instrument(
    name = "handle_fetch",
    level = "info",
    skip_all,
    fields(api = "Fetch", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<(FetchResponse, i16), BrokerError> {
    // KIP-124 request_percentage meters server-side handler time; capture the
    // start so the request throttle can be combined with the consumer
    // byte-rate throttle below (KIP-219).
    let handler_start = std::time::Instant::now();
    // Phase accounting for `request_{local,remote}_duration_seconds`. The read
    // loop charges every partition read to the local phase, and the long poll
    // and the cold-tier round trip to the remote one. Every path that reaches
    // a read observes all three phases exactly once, the failed read and the
    // routed KIP-595 answer included, so the three `_count`s stay equal to one
    // another; the rejection arms below answer before any read happens and
    // observe nothing, so the gap to `request_duration_seconds` is exactly the
    // fetches this handler refused.
    let phases = RequestPhases::default();
    let mut cur: &[u8] = req_bytes;
    let req: FetchRequest = if version < 4 {
        krabka_protocol::kafka_3_6_2::owned::fetch_request::FetchRequest::decode(&mut cur, version)?
            .into()
    } else {
        FetchRequest::decode(&mut cur, version)?
    };

    let preparation = match prepare_fetch(broker, (&req, version), ctx) {
        Ok(preparation) => preparation,
        Err(code) => {
            let resp = FetchResponse {
                error_code: code,
                session_id: INVALID_SESSION_ID,
                responses: Vec::new(),
                ..Default::default()
            };
            return Ok((resp, version));
        }
    };
    let FetchPreparation {
        decision,
        effective_topics,
        image,
        authorization,
        effective_replica_id,
        is_follower_fetch,
        read_committed,
    } = preparation;

    if version >= KIP_595_FETCH_VERSION && decode_fetch_request(&req).is_some() {
        // A KIP-595 fetch always names a replica, so `prepare_fetch` has
        // already checked `ClusterAction` for it.
        if authorization.refuses_every_row() {
            return Ok((cluster_authorization_failed(), version));
        }
        let on_inter_broker_listener =
            ctx.connection_listener_name == broker.config.inter_broker_listener_name;
        let Some(authenticated_node) = broker
            .wal_shards
            .authenticated_node_id(ctx.principal, on_inter_broker_listener)
        else {
            return Ok((cluster_authorization_failed(), version));
        };
        let routing_start = std::time::Instant::now();
        if let Some(response) = broker
            .wal_shards
            .route_fetch_request(&req, authenticated_node)
        {
            // A routed WAL fetch is served straight out of this broker's own
            // shard engine, so the round trip is local work like any other
            // read. Charging and observing it here is what keeps ordinary
            // internal WAL replication traffic from silently depressing all
            // three phase counts below the total.
            phases.add_local(routing_start.elapsed());
            observe_unthrottled_fetch_phases(broker, &phases);
            return Ok((response?, version));
        }
    }

    let plan_context = PendingPlanContext {
        broker,
        image: &image,
        authorization: &authorization,
        rack_id: &req.rack_id,
        version,
        mode: (read_committed, is_follower_fetch),
        follower_id: effective_replica_id,
        follower_broker_epoch: req.replica_state.replica_epoch,
    };
    let pending = build_pending_reads(&plan_context, &effective_topics).await;

    // Kafka's `KafkaApis.handleFetchRequest`: consumer responses use
    // `min(request.maxBytes, config.fetchMaxBytes, quota window bytes)`.
    // Follower fetches retain their replication/request budget and do not use
    // the consumer broker cap or client quota.
    let response_max_bytes = fetch_response_byte_budget(
        req.max_bytes,
        broker.config.fetch_max_bytes.bytes_usize(),
        if is_follower_fetch {
            usize::MAX
        } else {
            consumer_quota_window_bytes(broker, &image, ctx)
        },
        is_follower_fetch,
    );
    // A read that fails answers its own partition's row and never fails the
    // fetch (see `do_read`), so this call has no error to return.
    let (mut responses, cpu_micros_by_idx) = execute_pending_reads(
        broker,
        pending,
        req.min_bytes,
        response_max_bytes,
        req.max_wait_ms,
        ctx.sendfile_capable,
        &phases,
    )
    .await;
    broker
        .metrics
        .observe_request_phases(FETCH_API_KEY, &phases);

    downconvert_legacy_responses(broker, version, &mut responses);
    withhold_current_leader_before_kip_951(version, &mut responses);

    if is_follower_fetch {
        throttle_follower_responses(broker, &image, effective_replica_id, &mut responses);
    }

    let metric_rows = charge_fetch_cpu(broker, &responses, &cpu_micros_by_idx);

    // Kafka's `FetchContext` decides the rows before the quota check, and
    // charges the quota the size of the response those rows make.
    let commit = prepare_fetch_session(&decision, &effective_topics, &mut responses);

    // KIP-951: a partition row that names a new leader is only actionable if
    // the client can resolve that node id to an address. Both halves of the
    // KIP encode at v16+, so a client that cannot read the hint is also not
    // offered the endpoints.
    let node_endpoints = if version >= KIP_951_FETCH_VERSION {
        fetch_node_endpoints(
            &image,
            ctx.connection_listener_name,
            &broker.config.inter_broker_listener_name,
            &responses,
        )
    } else {
        Vec::new()
    };
    let mut resp = FetchResponse {
        error_code: 0,
        responses,
        node_endpoints,
        ..Default::default()
    };

    let throttle_time_ms = if is_follower_fetch {
        // An inter-broker fetch is charged no client quota, so it applies no
        // throttle. Observing the zero anyway keeps the throttle phase's
        // `_count` equal to the other two phases' for this api.
        broker
            .metrics
            .observe_request_throttle_duration(FETCH_API_KEY, 0.0);
        0
    } else {
        let (throttle_time_ms, charge) = apply_consumer_fetch_quota(
            broker,
            &image,
            ctx,
            handler_start,
            fetch_response_size(&resp, version),
        );
        if throttle_time_ms > 0 {
            // Kafka's throttled consumer fetch: the bytes come off the quota
            // again, the session is left as it was, and the response is
            // empty with the throttle in it.
            charge.refund();
            refresh_fetch_session_metrics(broker);
            return Ok((
                FetchResponse {
                    throttle_time_ms,
                    session_id: commit.throttled_session_id(),
                    responses: Vec::new(),
                    ..resp
                },
                version,
            ));
        }
        throttle_time_ms
    };

    record_fetch_metrics(broker, metric_rows, is_follower_fetch);
    resp.throttle_time_ms = throttle_time_ms;
    resp.session_id = commit_fetch_session(
        broker,
        commit,
        (is_follower_fetch, version),
        &ctx.principal.name,
    );
    Ok((resp, version))
}

/// Observe all three phase families for a Fetch that ends before it reaches
/// the quota: the routed KIP-595 answer, which is charged no client quota. It
/// applies no throttle, so the throttle phase takes an explicit zero and its
/// `_count` stays equal to the local and remote counts for this api.
fn observe_unthrottled_fetch_phases(broker: &Broker, phases: &RequestPhases) {
    broker.metrics.observe_request_phases(FETCH_API_KEY, phases);
    broker
        .metrics
        .observe_request_throttle_duration(FETCH_API_KEY, 0.0);
}

fn cluster_authorization_failed() -> FetchResponse {
    FetchResponse {
        error_code: CLUSTER_AUTHORIZATION_FAILED,
        session_id: INVALID_SESSION_ID,
        ..Default::default()
    }
}

/// The pure read-path visibility decision.
///
/// From a partition's watermarks and a fetch's parameters, it gives the
/// offsets that the fetch may expose and the HW and LSO that it reports. It
/// sits apart from [`do_read`], so it is the single source of truth for the
/// response fields on both the `OFFSET_OUT_OF_RANGE` path and the success
/// path. `fetch_visibility_model.rs` covers it with exhaustive tests and
/// property tests in isolation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct VisibilityWindow {
    /// `fetch_offset < log_start` or `fetch_offset > log_end`. The caller
    /// returns `OFFSET_OUT_OF_RANGE`; the log end itself is a valid empty read.
    pub out_of_range: bool,
    /// `fetch_offset >= upper_bound`. There is nothing to read, and no bytes
    /// go out.
    pub empty: bool,
    /// Exclusive upper offset the raw read may expose: `[fetch_offset, limit_offset)`.
    ///
    /// On a consumer fetch this is capped at the delivery watermark as well
    /// (KFC-1), so a batch that has not reached its activation time never
    /// leaves the broker. Every path that turns bytes into a response honours
    /// it, including the diskless hot-tail cache.
    pub limit_offset: Offset,
    /// `read_committed` aborted-txn scan ceiling. It is `lso.min(hw)` for a
    /// `read_committed` consumer, and `lso` in every other case.
    pub effective_lso: Offset,
    /// Whether to fill `aborted_transactions`, which happens for a
    /// `read_committed` consumer.
    pub read_committed_aborts: bool,
    /// `out.high_watermark` to report: the partition's high watermark, for a
    /// follower as for a consumer, as Kafka's `Partition.readRecords` reports
    /// it.
    pub response_hw: Offset,
    /// `out.last_stable_offset` to report: `lso.min(hw)`, Kafka's
    /// `UnifiedLog.lastStableOffset`, for every fetcher and isolation level.
    pub response_lso: Offset,
}

/// The partition offsets a fetch reads to decide what it may expose.
///
/// Kafka keeps `0 <= log_start <= hw <= log_end` and `lso <= hw`, and KFC-1
/// keeps `deliverable <= hw`. The verified decision needs none of them: a
/// consumer never reads at or past `hw`, `deliverable`, or (under
/// `read_committed`) `lso`, whatever the caller passes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct FetchWatermarks {
    /// The floor below which this read is out of range. On a tiered partition
    /// (KIP-405) that is the *local* log start, not the global one: the band
    /// between the two belongs to the remote tier, and calling it out of range
    /// is how the fetch reaches the remote read path. The
    /// response's `log_start_offset` still reports the global floor.
    pub log_start: Offset,
    pub hw: Offset,
    pub lso: Offset,
    pub log_end: Offset,
    /// KFC-1's delivery watermark: the first offset that is not due yet. On a
    /// topic that delivers immediately it is the high watermark, so the window
    /// is the one Kafka computes and the zero-copy read path is unchanged.
    pub deliverable: Offset,
}

/// The caller sets `read_committed` for consumer fetches only, so
/// `read_committed` implies `!is_follower`.
pub(crate) fn compute_visibility_window(
    is_follower: bool,
    read_committed: bool,
    watermarks: FetchWatermarks,
    fetch_offset: Offset,
) -> VisibilityWindow {
    let verified = krabka_verified::fetch_visibility(
        is_follower,
        read_committed,
        krabka_verified::FetchWatermarks {
            log_start: watermarks.log_start.0,
            hw: watermarks.hw.0,
            lso: watermarks.lso.0,
            log_end: watermarks.log_end.0,
            deliverable: watermarks.deliverable.0,
        },
        fetch_offset.0,
    );
    VisibilityWindow {
        out_of_range: verified.out_of_range,
        empty: verified.empty,
        limit_offset: Offset(verified.limit_offset),
        effective_lso: Offset(verified.effective_lso),
        read_committed_aborts: verified.read_committed_aborts,
        response_hw: Offset(verified.response_hw),
        response_lso: Offset(verified.response_lso),
    }
}

/// Encode a `FetchResponse` into a `BytesMut`.
///
/// The function chooses the legacy `kafka_3_6_2` codec for Fetch v0-3 and the
/// current canonical codec for v4+. This version boundary matches the
/// request-decode boundary.
pub(crate) fn encode_fetch_response(
    mut resp: FetchResponse,
    version: i16,
) -> Result<BytesMut, BrokerError> {
    serve_empty_rather_than_null_records(&mut resp);
    if version < 4 {
        let legacy: krabka_protocol::kafka_3_6_2::owned::fetch_response::FetchResponse =
            resp.into();
        let mut buf = BytesMut::with_capacity(legacy.encoded_len(version));
        legacy.encode(&mut buf, version)?;
        Ok(buf)
    } else {
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf)
    }
}

/// Rewrite every partition that has nothing to serve to carry an *empty*
/// record set rather than an absent one.
///
/// The broker spells "this partition has nothing to serve" as `None` -- a
/// throttled partition, a partition whose read came back empty, a tiered read
/// that missed, a partition that only carries an error code.
/// [`records_to_serve`] is what decides that, and its rustdoc carries the
/// reasoning: a null record set on the wire drops sarama connections, so
/// "nothing" must encode as `MemoryRecords.EMPTY`.
///
/// This is the *struct* half of that guarantee, and it exists because the
/// encoders below cannot be given the other half. Fetch v0-3 down-converts
/// through the codegen'd `kafka_3_6_2` encoder, which lives in a pinned
/// sibling crate, is faithful to the nullable schema, and takes no hook; the
/// v4+ arm here is the plain `FetchResponse::encode`, which is equally
/// faithful. So the null has to be gone before either sees the response. Real
/// v4+ traffic does not come through here at all -- it goes through
/// `network::fetch_writer::build_fetch_plan`, whose write plan enforces the
/// same invariant at its own boundary, where it can additionally drop the
/// records op entirely instead of emitting a zero-length one.
fn serve_empty_rather_than_null_records(resp: &mut FetchResponse) {
    for topic in &mut resp.responses {
        for partition in &mut topic.partitions {
            if records_to_serve(partition.records.as_ref()).is_none() {
                partition.records = Some(krabka_protocol::records::RecordsPayload::Raw(
                    bytes::Bytes::new(),
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        assert2::assert,
        bytes::{Bytes, BytesMut},
        krabka_ids::PartitionIndex,
        krabka_log::{Log, LogConfig, Offset},
        krabka_metadata::AclOperation,
        krabka_protocol::{
            Encode as _,
            records::{Record, RecordBatch, RecordsPayload},
        },
        krabka_security::Principal,
        std::sync::Mutex,
    };

    /// A partition with nothing to serve reaches the wire as an empty record
    /// set at every version the handler encodes, the legacy Fetch v0-3 codec
    /// included. The trailing bytes of the body are the records field, so a
    /// null one would end the body `ff ff ff ff` rather than `00 00 00 00`.
    #[test]
    fn no_version_puts_a_null_record_set_on_the_wire() {
        use krabka_protocol::owned::fetch_response::{
            FetchResponse, FetchableTopicResponse, PartitionData,
        };

        for version in [0i16, 1, 2, 3, 4, 7, 11] {
            for records in [None, Some(RecordsPayload::Raw(Bytes::new()))] {
                let resp = FetchResponse {
                    responses: vec![FetchableTopicResponse {
                        topic: "t".to_string(),
                        partitions: vec![PartitionData {
                            partition_index: 0,
                            records,
                            ..PartitionData::default()
                        }],
                        ..FetchableTopicResponse::default()
                    }],
                    ..FetchResponse::default()
                };
                let body = super::encode_fetch_response(resp, version)
                    .expect("the handler encodes its own response");
                let tail = &body[body.len() - 4..];
                assert!(
                    tail == b"\x00\x00\x00\x00",
                    "version {version} ended the body with {tail:?}, not an empty record set"
                );
            }
        }
    }

    use crate::{
        BrokerHandle,
        authorizer::AuthorizationResult,
        broker::Broker,
        config::BrokerConfig,
        handlers::RequestContext,
        test_support::{peer, sasl_principal},
        wal::quorum::{
            engine::WalShardEngine,
            registry::ShardId,
            wire::{KIP_595_FETCH_VERSION, QuorumGroup, fetch_request},
        },
    };

    #[derive(Debug)]
    struct TopicReadOnly;

    test_authorizer!(TopicReadOnly, (self, _source, request), {
        if request.operation == AclOperation::Read {
            AuthorizationResult::Allow
        } else {
            AuthorizationResult::Deny
        }
    });

    /// Start a broker holding one routable diskless-WAL shard, so a KIP-595
    /// Fetch addressed at it takes the routed branch of [`super::handle`].
    /// Returns the handle and the shard's topic id.
    async fn broker_with_routable_wal_shard(dir: &std::path::Path) -> (BrokerHandle, uuid::Uuid) {
        let broker_handle = Broker::start(BrokerConfig::for_tests(dir.to_path_buf()))
            .await
            .expect("start broker");
        let broker = broker_handle.broker_arc_for_test();
        let source = std::sync::Arc::new(Mutex::new(
            Log::open(dir.join("wal-source"), LogConfig::default()).expect("open log"),
        ));
        let mut batch = RecordBatch {
            records: vec![Record {
                attributes: 0,
                offset_delta: 0,
                timestamp_delta: 0,
                key: None,
                value: Some(Bytes::from_static(b"wal")),
                headers: vec![],
            }],
            ..Default::default()
        };
        source
            .lock()
            .expect("log")
            .append_at(&mut batch, Offset(0))
            .expect("append");
        source.lock().expect("log").sync().expect("sync");
        let topic_id = uuid::Uuid::from_u128(0xD15C);
        let local_node_id = broker.config.node_id;
        broker.wal_shards.insert(
            ShardId {
                topic_id,
                partition: PartitionIndex(0),
            },
            std::sync::Arc::new(WalShardEngine::for_logs(
                maplit::btreemap! {krabka_raft::NodeId(1) => source},
            )),
        );
        broker
            .wal_shards
            .replace_placements(&maplit::hashmap! {ShardId {
                topic_id,
                partition: PartitionIndex(0),
            } => crate::wal::quorum::registry::WalPlacement {
                voters: vec![local_node_id, krabka_raft::NodeId(2)],
                leader_epoch: 0,
            }});
        (broker_handle, topic_id)
    }

    /// Keep the WAL broker, directory and request context in their original scopes.
    macro_rules! wal_fixture {
        (($directory:ident, $handle:ident, $topic_id:ident, $broker:ident), ($principal:ident, $peer:ident, $listener:ident, $context:ident)) => {
            wal_fixture!(@broker ($directory, $handle, $topic_id, $broker));
            wal_fixture!(@identity ($principal, $peer));
            let $listener = $broker.config.inter_broker_listener_name.clone();
            wal_fixture!(@context ($principal, $peer, $context), &$listener);
        };
        (($directory:ident, $handle:ident, $topic_id:ident, $broker:ident), ($principal:ident, $peer:ident, $context:ident), $listener:expr) => {
            wal_fixture!(@broker ($directory, $handle, $topic_id, $broker));
            wal_fixture!(@identity ($principal, $peer));
            wal_fixture!(@context ($principal, $peer, $context), $listener);
        };
        (@broker ($directory:ident, $handle:ident, $topic_id:ident, $broker:ident)) => {
            let $directory = tempfile::tempdir().expect("tempdir");
            let ($handle, $topic_id) = broker_with_routable_wal_shard($directory.path()).await;
            let $broker = $handle.broker_arc_for_test();
        };
        (@identity ($principal:ident, $peer:ident)) => {
            let $principal = wal_peer_principal();
            let $peer = peer();
        };
        (@context ($principal:ident, $peer:ident, $context:ident), $listener:expr) => {
            let $context = RequestContext::new(&$principal, &$peer, "wal-fetch", "test", false, $listener);
        };
    }

    async fn routed_wal_response(
        broker: &Broker,
        topic_id: uuid::Uuid,
        version: i16,
        context: &RequestContext<'_>,
        expectation: &str,
    ) -> (krabka_protocol::owned::fetch_response::FetchResponse, i16) {
        let request = fetch_request(
            QuorumGroup::diskless_wal(topic_id, PartitionIndex(0)),
            krabka_raft::NodeId(2),
            0,
            -1,
            0,
            krabka_units::mebibytes(1),
        );
        let mut encoded = BytesMut::new();
        request
            .encode(&mut encoded, version)
            .expect("encode WAL fetch");
        super::handle(broker, version, 1, &encoded, context)
            .await
            .expect(expectation)
    }

    /// The peer principal a routed WAL fetch authenticates as.
    fn wal_peer_principal() -> Principal { sasl_principal("broker-2") }

    #[tokio::test]
    async fn handle_routes_discriminated_wal_fetch_on_broker_listener() {
        wal_fixture!(
            (dir, broker_handle, topic_id, broker),
            (principal, peer, listener, context)
        );

        for version in [KIP_595_FETCH_VERSION, 18] {
            let (response, response_version) =
                routed_wal_response(&broker, topic_id, version, &context, "route WAL fetch").await;

            assert!(response_version == version);
            let partition = &response.responses[0].partitions[0];
            assert!(partition.high_watermark == 1);
            assert!(matches!(
                partition.records,
                Some(RecordsPayload::Raw(ref records)) if !records.is_empty()
            ));
        }
        broker_handle.shutdown().await;
    }

    /// The rendered value of `krabka_broker_<series>{api_key="Fetch"}` in
    /// exposition text.
    ///
    /// It matches a whole line rather than searching for a substring, because
    /// the rendering of zero is a prefix of the rendering of a small non-zero
    /// value: a search for `0.0` finds `0.000045` and calls a charged phase
    /// uncharged.
    fn fetch_series_value<'a>(rendered: &'a str, series: &str) -> &'a str {
        let prefix = format!("krabka_broker_{series}{{api_key=\"Fetch\"}} ");
        rendered
            .lines()
            .find_map(|line| line.strip_prefix(prefix.as_str()))
            .unwrap_or_else(|| panic!("no {prefix} series in:\n{rendered}"))
    }

    /// A routed KIP-595 fetch is answered before the ordinary read loop, but
    /// the dispatcher still counts it in `request_duration_seconds`. It has to
    /// observe its phases too, or steady internal WAL replication traffic
    /// permanently drags all three phase counts below the total and is
    /// indistinguishable there from the fetches the handler refused.
    #[tokio::test]
    async fn routed_wal_fetch_observes_all_three_phases() {
        wal_fixture!(
            (dir, broker_handle, topic_id, broker),
            (principal, peer, listener, context)
        );
        let (response, _) = routed_wal_response(
            &broker,
            topic_id,
            KIP_595_FETCH_VERSION,
            &context,
            "route WAL fetch",
        )
        .await;
        assert!(response.error_code == crate::codes::NONE);

        let mut rendered = String::new();
        {
            let registry = broker.metrics.registry.lock().await;
            prometheus_client::encoding::text::encode(&mut rendered, &registry)
                .expect("encode registry");
        }
        for family in [
            "request_local_duration_seconds",
            "request_remote_duration_seconds",
            "request_throttle_duration_seconds",
        ] {
            let count = fetch_series_value(&rendered, &format!("{family}_count"));
            assert!(count == "1", "{family} was observed {count} times");
        }
        // The routed read is this broker's own shard engine, so it is local
        // work; nothing waited on a peer and no client quota was charged.
        let local = fetch_series_value(&rendered, "request_local_duration_seconds_sum");
        assert!(
            local.parse::<f64>().expect("a sum is a number") > 0.0,
            "the routed read belongs to the local phase, which reads {local}"
        );
        assert!(fetch_series_value(&rendered, "request_remote_duration_seconds_sum") == "0.0");
        assert!(fetch_series_value(&rendered, "request_throttle_duration_seconds_sum") == "0.0");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn wal_fetch_requires_cluster_action() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        config.authorizer = std::sync::Arc::new(TopicReadOnly);
        let broker_handle = Broker::start(config).await.expect("start broker");
        let broker = broker_handle.broker_arc_for_test();
        let request = fetch_request(
            QuorumGroup::diskless_wal(uuid::Uuid::from_u128(0xD15C), PartitionIndex(0)),
            krabka_raft::NodeId(2),
            0,
            0,
            0,
            krabka_units::mebibytes(1),
        );
        let mut encoded = BytesMut::new();
        request
            .encode(&mut encoded, KIP_595_FETCH_VERSION)
            .expect("encode WAL fetch");
        request_identity!(
            (principal, peer),
            crate::test_support::sasl_principal("broker-2")
        );
        let context = RequestContext::new(&principal, &peer, "wal-fetch", "test", false, "");

        let (response, _) = super::handle(&broker, KIP_595_FETCH_VERSION, 1, &encoded, &context)
            .await
            .expect("deny WAL fetch");

        assert!(response.error_code == crate::codes::CLUSTER_AUTHORIZATION_FAILED);
        assert!(response.responses.is_empty());
        broker_handle.shutdown().await;
    }

    /// The `broker-<id>` naming convention only names a WAL voter on the
    /// inter-broker listener. A client listener carries no such promise: any
    /// client free to pick its own SASL username could otherwise call itself
    /// `broker-2` and read another voter's WAL shard.
    #[tokio::test]
    async fn wal_fetch_on_a_client_listener_is_refused() {
        wal_fixture!(
            (dir, broker_handle, topic_id, broker),
            (principal, peer, context),
            "EXTERNAL"
        );

        let (response, _) = routed_wal_response(
            &broker,
            topic_id,
            KIP_595_FETCH_VERSION,
            &context,
            "refuse WAL fetch",
        )
        .await;

        assert!(response.error_code == crate::codes::CLUSTER_AUTHORIZATION_FAILED);
        assert!(response.responses.is_empty());
        broker_handle.shutdown().await;
    }
}

#[cfg(test)]
#[path = "fetch_visibility_model.rs"]
mod fetch_visibility_model;

#[cfg(test)]
mod visibility_fuzz;
