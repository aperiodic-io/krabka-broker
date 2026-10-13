//! Response framing and the KIP-219 throttle. It prepends the response header
//! to a handler's body, charges the request quota, and patches the leading
//! `ThrottleTimeMs` of the responses whose schema carries that field first.
//!
//! Charging a quota never sleeps here. It returns the throttle window
//! alongside the bytes, and the connection loop enforces it by muting the
//! connection *after* the response is written, which is what KIP-219 asks for.

use {
    crate::{
        broker::Broker,
        error::BrokerError,
        handlers::{ApiKeyCode, ApiVersion, CorrelationId},
        metrics::{QuotaCharge, QuotaType},
        network::{
            auth::ConnectionAuth,
            codec::{self, KafkaCodec},
            request::ParsedRequest,
            response_header_len, response_header_v1,
        },
        quota::{QuotaDelay, consume_request_quota, throttle_time_ms},
    },
    bytes::{BufMut, Bytes, BytesMut},
    krabka_protocol::api_key::ApiKey,
    krabka_units::{Time, convert::TimeExt},
};

/// A framed response together with the KIP-219 window the connection must
/// stay muted for once those bytes are on the wire.
#[derive(Debug)]
pub(super) struct ThrottledResponse {
    pub(super) bytes: Bytes,
    pub(super) throttle: Time,
    /// A quota charge the handler left for [`apply_request_quota`] to resolve
    /// together with the request quota. See
    /// [`crate::quota::ThrottleSlot::defer`].
    pub(super) deferred_charge: Option<QuotaCharge>,
    /// Set for an `acks=0` `Produce` whose response carried an error on any
    /// partition. The response body is suppressed either way, but this tells
    /// `send_registry_response` to close the connection instead of muting it,
    /// which is the only signal such a producer gets.
    pub(super) close_after_response: bool,
}

impl ThrottledResponse {
    /// A response that trips no quota, so the connection is not muted.
    pub(super) fn unthrottled(bytes: Bytes) -> Self {
        Self {
            bytes,
            throttle: <Time as TimeExt>::ZERO,
            deferred_charge: None,
            close_after_response: false,
        }
    }
}

/// Write before enforcing the window, and close on exactly the same framing failure.
pub(super) async fn send_throttled_response<S: tokio::io::AsyncWrite + Unpin>(
    framed: &mut tokio_util::codec::Framed<S, KafkaCodec>,
    response: ThrottledResponse,
) -> super::AfterResponse {
    use futures_util::SinkExt as _;
    if let Err(error) = framed.send(response.bytes).await {
        tracing::warn!(%error, "framed.send error, closing");
        return super::AfterResponse::Close;
    }
    super::AfterResponse::Mute(response.throttle)
}

/// The schema version and header flexibility of the response that was
/// actually encoded.
///
/// It is not always the request's. `reject_unsupported_version` replies to
/// an `ApiVersions` request at any unsupported version with a v0 body.
/// `patch_leading_throttle` derives the body offset from the flexibility, and
/// `throttle_is_leading_field` from the version, so both must read the
/// response's values.
#[derive(Clone, Copy)]
pub(super) struct ResponseShape {
    pub(super) version: ApiVersion,
    pub(super) body_flexible: bool,
}

impl ResponseShape {
    /// The ordinary case: the handler answered at the version the client
    /// asked for, with the header flexibility the request header used.
    pub(super) fn mirroring_request(parsed: &ParsedRequest<'_>) -> Self {
        Self {
            version: parsed.api_version,
            body_flexible: parsed.body_flexible,
        }
    }
}

/// Charges the KIP-124 request quota for a finished request and returns the
/// response with the throttle window it earned.
///
/// `handler_time` is the time the handler actually ran, not the time it was
/// parked on a coordinator, a replication wait or a raft commit: Kafka meters
/// request-handler thread time, and a request waiting in a purgatory holds no
/// thread. `deferred_charge` is a quota the handler charged and left for this
/// function, which resolves both in one metrics call.
///
/// The function does not wait. It patches the response's leading
/// `ThrottleTimeMs` where the schema has one, so the client learns how long to
/// back off, and returns the window so the caller can mute the connection
/// after the write.
pub(super) fn apply_request_quota(
    broker: &Broker,
    mut response_bytes: Bytes,
    parsed: &ParsedRequest<'_>,
    shape: ResponseShape,
    auth: &ConnectionAuth,
    handler_time: std::time::Duration,
    deferred_charge: Option<QuotaCharge>,
) -> ThrottledResponse {
    let elapsed_micros = u64::try_from(handler_time.as_micros()).unwrap_or(u64::MAX);
    let self_accounts = matches!(
        ApiKey::from_i16(parsed.api_key),
        Some(ApiKey::Produce | ApiKey::Fetch)
    );
    let mut throttle = <Time as TimeExt>::ZERO;
    if !self_accounts {
        // KIP-124 keys the request quota on the principal. A PLAINTEXT or SSL
        // connection starts as `ANONYMOUS`, so only a SASL connection that has
        // not authenticated yet has none, and Kafka's authenticator answers
        // those requests before `KafkaApis` could charge them. The zero still
        // reaches the throttle histogram below, which is what keeps that
        // family's `_count` equal to the number of requests this path
        // accounted for.
        let charged = match auth.principal() {
            None => QuotaDelay::zero(),
            Some(principal) => {
                let image = broker.controller.current_image();
                consume_request_quota(
                    &image,
                    &broker.quota_buckets,
                    &principal.name,
                    parsed.client_id,
                    elapsed_micros,
                    broker.config.quota_throttle_max,
                )
            }
        };
        // One metrics call resolves the request quota and any quota the
        // handler deferred, and the larger delay is the window this request is
        // muted for.
        let request_charge: QuotaCharge = (QuotaType::Request, charged).into();
        let quota_charges: Vec<QuotaCharge> = deferred_charge
            .into_iter()
            .chain(std::iter::once(request_charge))
            .collect();
        let delay = broker
            .metrics
            .record_applied_throttle(parsed.api_key, &quota_charges);
        if delay > <Time as TimeExt>::ZERO {
            let delay_ms = throttle_time_ms(delay);
            if throttle_is_leading_field(parsed.api_key, shape.version) {
                response_bytes = patch_leading_throttle(
                    response_bytes,
                    parsed.api_key,
                    shape.body_flexible,
                    delay_ms,
                );
            } else if buried_throttle_is_reencoded(parsed.api_key) {
                response_bytes = set_buried_throttle(response_bytes, parsed, shape, delay_ms);
            }
            throttle = delay;
        }
    }
    ThrottledResponse {
        bytes: response_bytes,
        throttle,
        deferred_charge: None,
        close_after_response: false,
    }
}

/// Whether the dispatch loop reports a request-quota delay on `api_key` by
/// decoding the response, setting its `ThrottleTimeMs`, and encoding it again.
///
/// These are the apis whose `ThrottleTimeMs` is not the first field, and whose
/// handler does not set it: the four delegation-token apis carry it last, and
/// `OffsetDelete` carries it behind `ErrorCode`. Kafka sets the field on the
/// typed response in `sendResponseMaybeThrottle`, so the client learns the
/// delay on these apis too.
pub(super) fn buried_throttle_is_reencoded(api_key: ApiKeyCode) -> bool {
    matches!(
        ApiKey::from_i16(api_key),
        Some(
            ApiKey::CreateDelegationToken
                | ApiKey::RenewDelegationToken
                | ApiKey::ExpireDelegationToken
                | ApiKey::DescribeDelegationToken
                | ApiKey::OffsetDelete
        )
    )
}

/// Raises the buried `ThrottleTimeMs` of an encoded response to
/// `max(existing, delay_ms)`, for an api that
/// [`buried_throttle_is_reencoded`] names.
///
/// A body that does not decode at the response's version is left as it is:
/// the connection is still muted for the window, and the response the client
/// gets is the one the handler wrote.
fn set_buried_throttle(
    response_bytes: Bytes,
    parsed: &ParsedRequest<'_>,
    shape: ResponseShape,
    delay_ms: i32,
) -> Bytes {
    use krabka_protocol::owned::{
        create_delegation_token_response::CreateDelegationTokenResponse,
        describe_delegation_token_response::DescribeDelegationTokenResponse,
        expire_delegation_token_response::ExpireDelegationTokenResponse,
        offset_delete_response::OffsetDeleteResponse,
        renew_delegation_token_response::RenewDelegationTokenResponse,
    };

    let header_len = response_header_len(parsed.api_key, shape.body_flexible);
    let Some(body) = response_bytes.get(header_len..) else {
        return response_bytes;
    };
    let version = shape.version;
    let reencoded = match ApiKey::from_i16(parsed.api_key) {
        Some(ApiKey::CreateDelegationToken) => {
            reencode::<CreateDelegationTokenResponse>(body, version, |response| {
                response.throttle_time_ms = response.throttle_time_ms.max(delay_ms);
            })
        }
        Some(ApiKey::RenewDelegationToken) => {
            reencode::<RenewDelegationTokenResponse>(body, version, |response| {
                response.throttle_time_ms = response.throttle_time_ms.max(delay_ms);
            })
        }
        Some(ApiKey::ExpireDelegationToken) => {
            reencode::<ExpireDelegationTokenResponse>(body, version, |response| {
                response.throttle_time_ms = response.throttle_time_ms.max(delay_ms);
            })
        }
        Some(ApiKey::DescribeDelegationToken) => {
            reencode::<DescribeDelegationTokenResponse>(body, version, |response| {
                response.throttle_time_ms = response.throttle_time_ms.max(delay_ms);
            })
        }
        Some(ApiKey::OffsetDelete) => reencode::<OffsetDeleteResponse>(body, version, |response| {
            response.throttle_time_ms = response.throttle_time_ms.max(delay_ms);
        }),
        _ => None,
    };
    let Some(reencoded) = reencoded else {
        tracing::warn!(
            api_key = parsed.api_key,
            version,
            "could not set the request-quota delay on the response; sending it unchanged"
        );
        return response_bytes;
    };
    let mut out = BytesMut::with_capacity(header_len + reencoded.len());
    out.put_slice(&response_bytes[..header_len]);
    out.put_slice(&reencoded);
    out.freeze()
}

/// Decodes `body` as `R` at `version`, applies `set`, and encodes it again.
fn reencode<R>(body: &[u8], version: ApiVersion, set: impl FnOnce(&mut R)) -> Option<BytesMut>
where
    R: for<'de> krabka_protocol::Decode<'de> + krabka_protocol::Encode,
{
    let mut cursor = body;
    let mut response = R::decode(&mut cursor, version).ok()?;
    if !cursor.is_empty() {
        return None;
    }
    set(&mut response);
    let mut out = BytesMut::with_capacity(response.encoded_len(version));
    response.encode(&mut out, version).ok()?;
    Some(out)
}

/// Prepends the response header, the `corr_id` and an optional tagged-fields
/// byte, in front of the handler's body bytes.
// PERF — measured; decision: KEEP.
//
// This copies the whole body to prepend a 4-5 byte header, and the sink copies
// it a second time: the sink is a `Framed<S, KafkaCodec>`, and `KafkaCodec`
// only implements `Encoder<Bytes>` (a single concrete impl), so `framed.send`
// requires a contiguous `Bytes` and will not accept a
// `bytes::Buf::chain(header, body)`. Worse, that `Encoder::encode` itself does
// `dst.extend_from_slice(&frame)` into the codec's write buffer. Removing
// both copies means changing the codec to an `Encoder<impl Buf>` that
// vectored-writes header and body, which reaches `codec.rs`, the roundtrip
// test, and every signature that names `Framed<S, KafkaCodec>`.
//
// `benches/perf_deferrals.rs` prices that chained-`Buf` prototype against this
// path. Both sides write to a sink that keeps no bytes, so the socket write is
// out of the comparison on both and the reported saving is an upper bound:
//
//     body       copy    chained     saved
//     1 KiB     57 ns      32 ns     25 ns   (43%)
//     64 KiB  1557 ns      32 ns    1.5 us   (98%)
//     1 MiB     31 us      32 ns     31 us   (99.9%)
//
// Measured by the `bench` job of the `ci` workflow, which runs this suite on
// the nightly schedule and prints the same table into its job summary; the
// samples behind it are the run's `criterion-baseline` artifact. For the
// current numbers read the latest scheduled `ci` run in the repository's
// Actions tab rather than this table, which records the decision and not a
// particular machine.
//
// Keep. The saving is proportional to the body, and the one API whose bodies
// are unbounded does not come through here: Fetch is written by
// `network::fetch_writer`, which already skips both copies (its module doc has
// the same motivation). What is left on this path is produce, metadata and
// admin responses on the order of a kilobyte, where the whole framing step
// costs under 100 ns against a request that takes tens of microseconds to
// serve. Note the old price estimate of "~50 `framed.send` call sites" is
// stale — the dispatch tree has since funnelled its responses through three —
// but a custom codec is still the wrong trade for ~25 ns a response. Revisit
// only if a non-Fetch response becomes both large and hot; a Metadata response
// for a very large cluster is the plausible candidate, and clients refresh it
// on the order of minutes.
pub(super) fn encode_response(
    api_key: ApiKeyCode,
    correlation_id: CorrelationId,
    body_flexible: bool,
    body: &[u8],
) -> Result<Bytes, BrokerError> {
    let header_len = response_header_len(api_key, body_flexible);
    codec::response_frame_length(header_len + body.len())?;
    let mut buf = BytesMut::with_capacity(header_len + body.len());
    buf.put_i32(correlation_id);
    if response_header_v1(api_key, body_flexible) {
        buf.put_u8(0); // empty tagged fields
    }
    buf.put_slice(body);
    Ok(buf.freeze())
}

/// KIP-219 (throttle-then-respond): returns `true` when the response of
/// `api_key` carries `ThrottleTimeMs` as its FIRST body field at `version`.
/// The dispatch loop then reports the request-quota throttle by patching that
/// leading int32 in place.
///
/// The table is an exhaustive audit of every response schema in the pinned
/// `krabka-protocol` checkout. The `throttle_audit` sibling module pins it: it
/// enumerates every advertised `(api_key, version)` pair and compares this
/// predicate against the byte layout the generated encoder actually produces.
/// Adding an API to [`crate::api_catalog`] without classifying it here fails
/// that test.
///
/// Classifying an API correctly is necessary but not sufficient for it to echo
/// a delay. This predicate is only consulted where `apply_request_quota`
/// runs: the dispatch entries whose policy is
/// `RequestQuotaPolicy::ApplyFallbackAccounting` (every api except the
/// self-accounted and exempt ones) and the unsupported-version reply path,
/// which takes it for every `api_key`. The `SelfAccounted` entries --
/// `Produce`, `Fetch` and `ApiVersions` -- charge the quota in their handler
/// and set `ThrottleTimeMs` on the typed response before encoding, so they
/// never reach this predicate. The `InlineExempt` entries are the apis Kafka
/// exempts from the request quota, and they are neither delayed nor
/// throttle-stamped.
///
/// `Produce` (0) and `Fetch` (1) never reach this predicate. Both their
/// bandwidth quota and their share of the request quota are charged by the
/// handler, which sets `ThrottleTimeMs` on the typed response before encoding,
/// so `apply_request_quota` returns for both before consulting the
/// table. They are still classified below, and the audit still probes them, so
/// a schema move cannot pass unnoticed.
///
/// Everything else that answers `false` falls into three groups.
///
/// * Versions below the one at which the API moved `ThrottleTimeMs` to the
///   front of its response body: `Metadata` v0-2, `JoinGroup` v0-1,
///   `ListOffsets` v0-1, `FindCoordinator` v0 and the rest. The version bounds
///   in the arms below are exactly those boundaries.
/// * No `ThrottleTimeMs` field at any version: `SaslHandshake` (17),
///   `WriteTxnMarkers` (27), `SaslAuthenticate` (36), `DescribeQuorum` (55),
///   the share-coordinator state RPCs (83-87) and `GetReplicaLogInfo` (1020).
///   There is nothing to echo. `Vote` (52), `BeginQuorumEpoch` (53),
///   `EndQuorumEpoch` (54) and `Envelope` (58) are throttle-free as well, but
///   the broker does not advertise them, so the audit does not reach them.
/// * KIP-219 divergences -- the field is present but sits behind another
///   field, so patching a leading int32 would corrupt the response. These are
///   recorded as `THROTTLE_ECHO_DIVERGENCES` in the audit module and as rows
///   in the generated `docs/KIP_MATRIX.md`:
///   `Produce` (0) v1+ and `ApiVersions` (18) v1+ carry it after a
///   variable-length array, the four delegation-token APIs (38-41) carry it
///   last, and `OffsetDelete` (47) leads with `ErrorCode`.
///   What that costs on the wire differs per API, and the audit records it
///   as a `QuotaReach` pinned against the dispatch registry: `Produce` and
///   `ApiVersions` are `SelfAccounted`, so they never reach this predicate
///   and their handlers fill the field in; the other five are re-encoded by
///   [`set_buried_throttle`], so the client sees the delay on them too.
pub(super) fn throttle_is_leading_field(api_key: ApiKeyCode, version: ApiVersion) -> bool {
    // The version bounds are the schema versions at which each API moved
    // `ThrottleTimeMs` to the front of its response. They are deliberately
    // kept as literals and pinned by `throttle_audit`.
    let Some(api) = ApiKey::from_i16(api_key) else {
        return false;
    };
    match api {
        // Leading at every version the pinned schema defines.
        ApiKey::DeleteRecords
        | ApiKey::InitProducerId
        | ApiKey::AddPartitionsToTxn
        | ApiKey::AddOffsetsToTxn
        | ApiKey::EndTxn
        | ApiKey::TxnOffsetCommit
        | ApiKey::AlterConfigs
        | ApiKey::CreatePartitions
        | ApiKey::DeleteGroups
        | ApiKey::ElectLeaders
        | ApiKey::IncrementalAlterConfigs
        | ApiKey::AlterPartitionReassignments
        | ApiKey::ListPartitionReassignments
        | ApiKey::DescribeClientQuotas
        | ApiKey::AlterClientQuotas
        | ApiKey::DescribeUserScramCredentials
        | ApiKey::AlterUserScramCredentials
        | ApiKey::UpdateFeatures
        | ApiKey::FetchSnapshot
        | ApiKey::DescribeCluster
        | ApiKey::DescribeProducers
        | ApiKey::BrokerRegistration
        | ApiKey::BrokerHeartbeat
        | ApiKey::UnregisterBroker
        | ApiKey::DescribeTransactions
        | ApiKey::ListTransactions
        | ApiKey::AllocateProducerIds
        | ApiKey::ConsumerGroupHeartbeat
        | ApiKey::ConsumerGroupDescribe
        | ApiKey::ControllerRegistration
        | ApiKey::GetTelemetrySubscriptions
        | ApiKey::PushTelemetry
        | ApiKey::AssignReplicasToDirs
        | ApiKey::ListConfigResources
        | ApiKey::DescribeTopicPartitions
        | ApiKey::AddRaftVoter
        | ApiKey::RemoveRaftVoter
        | ApiKey::UpdateRaftVoter
        | ApiKey::StreamsGroupHeartbeat
        | ApiKey::StreamsGroupDescribe
        | ApiKey::DescribeShareGroupOffsets
        | ApiKey::AlterShareGroupOffsets
        | ApiKey::DeleteShareGroupOffsets
        | ApiKey::StreamsGroupTopologyDescriptionUpdate
        | ApiKey::UnregisterController => true,
        // Moved to the front at v1.
        ApiKey::Fetch
        | ApiKey::FindCoordinator
        | ApiKey::Heartbeat
        | ApiKey::LeaveGroup
        | ApiKey::SyncGroup
        | ApiKey::DescribeGroups
        | ApiKey::ListGroups
        | ApiKey::DeleteTopics
        | ApiKey::DescribeAcls
        | ApiKey::CreateAcls
        | ApiKey::DeleteAcls
        | ApiKey::DescribeConfigs
        | ApiKey::AlterReplicaLogDirs
        | ApiKey::DescribeLogDirs
        | ApiKey::ShareGroupHeartbeat
        | ApiKey::ShareGroupDescribe
        | ApiKey::ShareFetch
        | ApiKey::ShareAcknowledge => version >= 1,
        // Moved to the front at v2.
        ApiKey::ListOffsets
        | ApiKey::JoinGroup
        | ApiKey::CreateTopics
        | ApiKey::OffsetForLeaderEpoch
        | ApiKey::AlterPartition => version >= 2,
        // Moved to the front at v3.
        ApiKey::Metadata | ApiKey::OffsetCommit | ApiKey::OffsetFetch => version >= 3,
        // Throttle-free, self-accounting, or a recorded divergence; see the
        // three groups in the doc comment. `ApiKey` is `#[non_exhaustive]`, so
        // a future variant lands here until the audit forces it to be
        // classified.
        _ => false,
    }
}

/// Patches the leading `ThrottleTimeMs` int32 of an already-encoded response
/// in place, and raises it to `max(existing, delay_ms)`.
///
/// The body starts right after the response header, whose length mirrors
/// `encode_response`: 5 bytes when the body is flexible and the api is not
/// `ApiVersions`, and 4 bytes otherwise. Callers must first confirm
/// `throttle_is_leading_field`.
fn patch_leading_throttle(
    resp: Bytes,
    api_key: ApiKeyCode,
    body_flexible: bool,
    delay_ms: i32,
) -> Bytes {
    let off = response_header_len(api_key, body_flexible);
    if resp.len() < off + 4 {
        return resp;
    }
    let mut buf = BytesMut::from(resp.as_ref());
    let existing = i32::from_be_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
    let patched = existing.max(delay_ms);
    buf[off..off + 4].copy_from_slice(&patched.to_be_bytes());
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use {super::*, crate::network::dispatch::API_VERSIONS_KEY, assert2::assert};

    #[test]
    fn encode_response_apiversions_uses_v0_header() {
        // ApiVersions response is always header v0 (no tagged byte) even
        // for flexible body versions.
        let body = [0u8, 0u8]; // error_code=0
        let out = encode_response(API_VERSIONS_KEY, 7, true, &body).expect("encode response");
        // 4 byte corr_id + body, no tagged byte.
        assert!(out.len() == 4 + body.len());
    }

    #[test]
    fn patch_leading_throttle_sets_field_flexible_and_nonflexible() {
        let read =
            |b: &[u8], off: usize| i32::from_be_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]]);

        // Flexible response header (ConsumerGroupHeartbeat, flexible v0+):
        // header = 5 bytes (corr_id + tagged byte); throttle int32 at offset 5.
        let mut body = BytesMut::new();
        body.put_i32(0); // ThrottleTimeMs = 0
        let resp = encode_response(68, 7, true, &body).expect("encode response");
        let patched = patch_leading_throttle(resp, 68, true, 250);
        assert!(read(&patched, 5) == 250);
        assert!(read(&patched, 0) == 7); // corr_id preserved

        // Non-flexible response header (Metadata v3): header = 4 bytes.
        let mut body = BytesMut::new();
        body.put_i32(10); // existing throttle 10 < 250
        let resp = encode_response(3, 9, false, &body).expect("encode response");
        let patched = patch_leading_throttle(resp, 3, false, 250);
        assert!(read(&patched, 4) == 250);
        assert!(read(&patched, 0) == 9);
    }

    #[test]
    fn patch_leading_throttle_keeps_existing_when_larger() {
        // max(existing, delay): an already-larger throttle is not lowered.
        let mut body = BytesMut::new();
        body.put_i32(500);
        let resp = encode_response(3, 1, false, &body).expect("encode response");
        let patched = patch_leading_throttle(resp, 3, false, 100);
        let v = i32::from_be_bytes([patched[4], patched[5], patched[6], patched[7]]);
        assert!(v == 500);
    }

    #[test]
    fn encode_response_other_flexible_inserts_tagged_byte() {
        let body = [0u8, 0u8];
        let out = encode_response(3, 7, true, &body).expect("encode response");
        assert!(out.len() == 5 + body.len());
        assert!(out[4] == 0); // tagged byte
    }
}
