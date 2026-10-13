//! Per-connection request loop. Reads a frame, parses the request
//! header, looks up the handler, awaits the response, encodes the
//! response header in front of the handler's bytes, and writes the
//! result back to the client.
//!
//! Header rules, verified against Apache Kafka 4.x:
//! - The request header is v2 when the body is flexible (KIP-482), and v1
//!   otherwise. Note that `client_id` is a `NULLABLE_STRING` with an i16
//!   length in BOTH header versions. See the `RequestHeader.json` schema,
//!   where the field has `flexibleVersions: none`.
//! - The response header is v1, that is, it has a trailing tagged-fields
//!   byte, if and only if the *body* is flexible. `ApiVersions`
//!   (`api_key=18`) is the one EXCEPT case: its response header is always
//!   v0.

use {
    bytes::Bytes,
    futures_util::SinkExt,
    krabka_protocol::{Decode as _, api_key::ApiKey},
    krabka_units::{Time, convert::TimeExt},
    std::{net::SocketAddr, sync::Arc},
    tokio::io::{AsyncRead, AsyncWrite},
    tokio_util::codec::Framed,
    tracing::Instrument as _,
};

mod accept;
mod fetch;
mod guards;
mod receive;
mod registry;
mod response;
/// Benchmark seam over the response-framing path, driven by
/// `benches/perf_deferrals.rs`.
#[cfg(any(test, feature = "test-helpers"))]
pub mod response_framing;
pub(crate) mod sasl;
mod session;
#[cfg(test)]
mod test_support;
mod write_idle;

/// The broker-wide `queued.max.request.bytes` budget (#412).
///
/// The total is carried beside the semaphore because
/// [`tokio::sync::Semaphore::acquire_many`] does not refuse a request for more
/// permits than the semaphore was built with -- it waits, and no response can
/// ever free enough room, so the wait never ends. The comparison against
/// `total` is what turns that into a refusal.
pub(crate) struct RequestByteBudget {
    permits: Arc<tokio::sync::Semaphore>,
    total: usize,
}

impl RequestByteBudget {
    /// One permit per byte. `Semaphore` caps its permit count well below
    /// `usize::MAX`, so a budget above that ceiling is clamped to it rather
    /// than rejected: at that size the knob is off in every sense that
    /// matters.
    pub(crate) fn of(budget: usize) -> Self {
        let total = budget.min(tokio::sync::Semaphore::MAX_PERMITS);
        Self {
            permits: Arc::new(tokio::sync::Semaphore::new(total)),
            total,
        }
    }

    /// The permits not currently spent. Test-only: what production code cares
    /// about is whether a frame is admitted, not how much room is left.
    #[cfg(test)]
    pub(crate) fn available(&self) -> usize { self.permits.available_permits() }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod throttle_audit;

pub use self::accept::serve_connection_on_listener;
use {
    self::{
        fetch::dispatch_fetch,
        guards::{ActiveConnectionGuard, InFlightGuard, QueuedRequestGuard},
        registry::{DispatchContext, send_registry_response},
        response::{ResponseShape, apply_request_quota, encode_response},
        sasl::{SaslConnection, SaslFrameOutcome, SaslListener, try_handle_sasl_frame},
        session::{
            FrameSizeLimit, FrameWaitPolicy, RequestLimits, initial_connection_auth,
            next_connection_frame,
        },
        write_idle::WriteIdle,
    },
    crate::{
        api_catalog::{INTER_BROKER_ONLY_APIS, ListenerKind, is_disabled_version},
        broker::Broker,
        config::ListenerSpec,
        handlers::{
            ApiKeyCode, ApiVersion,
            api_versions::{is_valid_client_info, unsupported_version_response},
            registry::DispatchEntry,
        },
        metrics::ConnectionCloseReason,
        network::{
            auth::ConnectionAuth,
            codec::{self, KafkaCodec},
            fetch_writer::SendfileSink,
            listener::resolve_sasl_mechanisms_for_listener,
            request::{ParsedRequest, parse_request, peek_api_key},
        },
        telemetry::request_span,
        time_util::now_ms,
    },
};

/// What the connection loop does once a response has been written.
///
/// KIP-219 splits a quota violation between the response, which tells the
/// client how long to back off, and the connection, which the broker mutes for
/// exactly that long. The write always happens first; the mute is what
/// enforces the quota.
#[derive(Clone, Copy, Debug)]
pub(super) enum AfterResponse {
    /// Keep serving, but read no further request for this window. It is a zero
    /// extent when the request tripped no quota.
    Mute(Time),
    /// Close the connection.
    Close,
}

/// Turns a KIP-219 throttle window into the deadline the connection stays
/// muted until, measured from now — that is, from the moment the response
/// finished being written.
fn mute_deadline(window: Time) -> Option<tokio::time::Instant> {
    (window > <Time as TimeExt>::ZERO).then(|| tokio::time::Instant::now() + window.to_std())
}

/// `ApiVersions` wire `api_key`. It has its own name because it is the one API
/// whose response header is always v0, whatever the body flexibility, and
/// whose v3+ request carries the KIP-511 client software name and version.
const API_VERSIONS_KEY: ApiKeyCode = ApiKey::ApiVersions as i16;

/// Kafka's `ClientInformation.UNKNOWN_NAME_OR_VERSION`: the KIP-511 software
/// name and version of a client that has not sent them.
const UNKNOWN_CLIENT_SOFTWARE: &str = "unknown";

fn capture_client_software(parsed: &ParsedRequest<'_>, name: &mut String, version: &mut String) {
    if parsed.api_key != API_VERSIONS_KEY || parsed.api_version < 3 {
        return;
    }
    let mut body = parsed.body;
    if let Ok(request) = krabka_protocol::owned::api_versions_request::ApiVersionsRequest::decode(
        &mut body,
        parsed.api_version,
    ) && is_valid_client_info(&request.client_software_name)
        && is_valid_client_info(&request.client_software_version)
    {
        name.clone_from(&request.client_software_name);
        version.clone_from(&request.client_software_version);
    }
}

/// A request parsed off a frame, and the span that traces it.
struct SpannedRequest<'a> {
    parsed: ParsedRequest<'a>,
    span: tracing::Span,
}

fn parse_connection_request<'a>(
    broker: &Broker,
    frame: &'a Bytes,
    peer: &SocketAddr,
) -> Option<SpannedRequest<'a>> {
    let peeked_api_key = match peek_api_key(frame) {
        Ok(api_key) => api_key,
        Err(error) => {
            tracing::warn!(%error, "frame too small to peek api_key, closing");
            return None;
        }
    };
    let parsed = match parse_request(frame, |api_key, version| {
        broker.handlers().body_flexible(api_key, version)
    }) {
        Ok(parsed) => parsed,
        Err(error) => {
            tracing::warn!(%error, "request parse error, closing");
            return None;
        }
    };
    assert2::assert!((parsed.api_key) == (peeked_api_key));
    let span = if tracing::enabled!(
        target: crate::telemetry::REQUEST_TARGET,
        tracing::Level::DEBUG
    ) {
        request_span(
            parsed.api_key,
            parsed.api_version,
            parsed.correlation_id,
            parsed.client_id,
            peer,
        )
    } else {
        tracing::Span::none()
    };
    Some(SpannedRequest { parsed, span })
}

/// The instant a request began and the guard that counts it in flight.
struct BegunRequest<'a> {
    started: std::time::Instant,
    in_flight: InFlightGuard<'a>,
}

fn begin_request<'a>(broker: &'a Broker, parsed: &ParsedRequest<'_>) -> BegunRequest<'a> {
    let started = std::time::Instant::now();
    broker.metrics.record_api_request(parsed.api_key);
    // Kafka's `RequestChannel` logs each request at DEBUG on
    // `kafka.request.logger`, which its default log4j configuration keeps at
    // WARN, so a request is never an INFO line.
    tracing::debug!(
        api_key = parsed.api_key,
        api_version = parsed.api_version,
        correlation_id = parsed.correlation_id,
        body_flexible = parsed.body_flexible,
        body_len = parsed.body.len(),
        "dispatching request"
    );
    BegunRequest {
        started,
        in_flight: InFlightGuard::new(&broker.metrics, parsed.api_key),
    }
}

/// Rejects a request at a version outside its API's range, or at a version --
/// or of an api key -- that the broker's `VersionGates` leave disabled: one
/// Kafka 4.3.1 does not serve, unless `unstable.api.versions.enable` or
/// `legacy_request_versions_enable` opts into it.
///
/// Kafka's `Processor.parseRequestHeader` throws `UnsupportedVersionException`
/// for the first and `InvalidRequestException` for the second, and
/// `SocketServer` closes the channel on either: no response frame.
/// `ApiVersions` is the one exception, since `ApiKeys.isVersionEnabled`
/// accepts every version of it: the request reaches `KafkaApis`, which
/// answers `UNSUPPORTED_VERSION` with a v0 body carrying the one
/// `ApiVersions` range (KIP-511), and the client falls back to that version.
/// Whether the listener serves `parsed` at its version: inside the handler's
/// decodable range, and not a version the broker's `VersionGates` disable.
///
/// `ApiVersions` is never a disabled version (Kafka's `isVersionEnabled`
/// accepts every one of it); a version above the one the listener serves --
/// trunk's KIP-1242 v5 while `unstable.api.versions.enable` is off -- is not
/// served, and [`reject_unsupported_version`] answers it `UNSUPPORTED_VERSION`
/// as it answers an unknown version.
fn is_served_version(broker: &Broker, entry: DispatchEntry, parsed: &ParsedRequest<'_>) -> bool {
    let api_versions_unserved = parsed.api_key == API_VERSIONS_KEY
        && parsed.api_version
            > krabka_raft::api_versions_max_version(broker.config.features.unstable_api_versions);
    entry.supports_version(parsed.api_version)
        && !api_versions_unserved
        && !is_disabled_version(
            parsed.api_key,
            parsed.api_version,
            broker.config.features.version_gates(),
        )
}

async fn reject_unsupported_version<S>(
    framed: &mut Framed<S, KafkaCodec>,
    broker: &Broker,
    entry: DispatchEntry,
    parsed: &ParsedRequest<'_>,
    auth: &ConnectionAuth,
) -> AfterResponse
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    const RESPONSE_VERSION: ApiVersion = 0;
    if parsed.api_key != API_VERSIONS_KEY {
        broker.metrics.record_api_request(parsed.api_key);
        broker
            .metrics
            .record_unsupported_api_request(parsed.api_key);
        tracing::warn!(
            api_key = parsed.api_key,
            api_version = parsed.api_version,
            "unsupported api version, closing connection"
        );
        return AfterResponse::Close;
    }
    let BegunRequest {
        started,
        in_flight: _in_flight,
    } = begin_request(broker, parsed);
    tracing::warn!(
        api_key = parsed.api_key,
        api_version = parsed.api_version,
        "unsupported api version"
    );
    broker
        .metrics
        .record_unsupported_api_request(parsed.api_key);
    let body = match unsupported_version_response(broker.config.features.unstable_api_versions) {
        Ok(body) => body,
        Err(error) => {
            tracing::warn!(%error, "unsupported-version response encode error, closing");
            return AfterResponse::Close;
        }
    };
    // The reply is encoded at v0, not at the version the client asked for,
    // and the throttle patch has to read that version and its flexibility.
    let shape = ResponseShape {
        version: RESPONSE_VERSION,
        body_flexible: entry.body_flexible(RESPONSE_VERSION),
    };
    let response = match encode_response(
        parsed.api_key,
        parsed.correlation_id,
        shape.body_flexible,
        &body,
    ) {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%error, "response exceeds the int32 frame size, closing");
            return AfterResponse::Close;
        }
    };
    let response = apply_request_quota(
        broker,
        response,
        parsed,
        shape,
        auth,
        started.elapsed(),
        None,
    );
    response::send_throttled_response(framed, response).await
}

/// KIP scope check (#683): `crate::api_catalog::INTER_BROKER_ONLY_APIS` is
/// tagged `controller`-only by its request schema, so no Kafka broker listener
/// ever routes it to a handler.
/// `ApiVersionManager.isApiEnabled` closes the connection before the request is
/// even parsed further; krabka does the same on a pure `ListenerKind::Client`
/// listener, and accepts these keys on `InterBroker` and
/// `ClientAndInterBroker` alike, where krabka's own peers send them and the
/// per-handler `ClusterAction` check applies.
fn is_inter_broker_api_on_client_listener(
    broker: &Broker,
    spec: &ListenerSpec,
    api_key: i16,
) -> bool {
    INTER_BROKER_ONLY_APIS.contains(&api_key)
        && broker.config.listener_kind(&spec.name) == ListenerKind::Client
}

/// Generic per-connection request loop.
///
/// `S` is the post-handshake byte stream: `TcpStream` for plaintext listeners,
/// and a rustls stream over a write-idle-tracked socket for userspace TLS. `spec`
/// carries the listener's protocol, so the loop initialises `ConnectionAuth`
/// correctly and gates pre-auth requests on SASL listeners.
// each api_key intercept arm adds ~15 lines.
async fn serve_connection_stream<S>(
    broker: std::sync::Arc<Broker>,
    stream: S,
    spec: ListenerSpec,
    peer: SocketAddr,
    mtls_principal: Option<krabka_security::Principal>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static + SendfileSink,
{
    let limits = RequestLimits::of(&broker.config);
    let stream =
        WriteIdle::for_response(stream, broker.config.connections_max_idle_for(&spec.name));
    let mut framed = Framed::new(stream, codec::codec(limits.request_max()));
    let is_sasl_listener = spec.protocol.requires_sasl();
    let sasl_mechanisms =
        resolve_sasl_mechanisms_for_listener(&spec, &broker.config.enabled_sasl_mechanisms)
            .to_owned();
    let mut auth =
        initial_connection_auth(is_sasl_listener, mtls_principal, &broker.audit_log, &peer);
    let connection_id = uuid::Uuid::new_v4().to_string();
    // Track live connections for the duration of this serve loop. The
    // gauge is decremented when `_conn` drops on any loop exit (EOF,
    // decode/send error, or SASL-session expiry).
    let _conn = ActiveConnectionGuard::new(&broker.metrics);
    // Resolved once: the listener's `connections.max.idle.ms`, with its
    // per-listener override already applied. `next_connection_frame` re-arms
    // the deadline from it on every frame read.
    // Resolved once, like the idle window: the listener's KIP-368
    // re-authentication window, which every mechanism handler clamps its
    // session to.
    let sasl_listener = SaslListener {
        is_sasl: is_sasl_listener,
        mechanisms: &sasl_mechanisms,
        max_reauth: broker.config.connections_max_reauth_for(&spec.name),
    };
    let frame_wait = FrameWaitPolicy {
        idle: broker.config.connections_max_idle_for(&spec.name),
        peer,
        metrics: broker.metrics.clone(),
        audit_log: broker.audit_log.clone(),
        failed_authentication_delay: broker.config.failed_authentication_delay(),
    };
    // Kafka's `SocketServer` `Acceptor` and `Processor` log a new connection
    // at DEBUG, and its `Selector` logs a close at DEBUG, as this loop does.
    tracing::debug!(listener = %spec.name, sasl = is_sasl_listener, "connection opened");

    // KIP-714 client software identity, populated by the first ApiVersions v3+ request.
    // so `GetTelemetrySubscriptions` can be served even on connections that
    // never sent `ApiVersions` (e.g. early-version clients).
    // Kafka's `ClientInformation.EMPTY` names both `unknown` until a KIP-511
    // `ApiVersions` says otherwise, so a `client_software_name=unknown`
    // selector matches such a client.
    let mut client_software = (
        UNKNOWN_CLIENT_SOFTWARE.to_owned(),
        UNKNOWN_CLIENT_SOFTWARE.to_owned(),
    );

    // KIP-219 channel mute. A throttled response is written immediately and
    // the quota is enforced by refusing to read the next request until this
    // deadline passes.
    let mut mute_until: Option<tokio::time::Instant> = None;

    // Raw-token mode (v0 handshake) and the last KIP-368 re-auth start.
    let mut sasl_session = sasl::SaslSession::default();

    loop {
        let Some(received) = next_connection_frame(
            &mut framed,
            &auth,
            mute_until.take(),
            &frame_wait,
            FrameSizeLimit {
                maximum: limits.for_auth(&auth),
                budget: broker.queued_request_bytes.as_ref(),
            },
        )
        .await
        else {
            break;
        };
        let receive::RequestFrame {
            bytes: frame,
            byte_permit,
        } = received;
        // The body already holds its byte budget, including while it waits
        // for request execution capacity.
        // `queued.max.requests`: Kafka's bound is the depth of the request
        // queue, so the wait belongs after the read and not before it. A
        // connection sitting idle has nothing queued and must hold no
        // capacity; taking a permit to wait on the socket would let a broker
        // with more idle connections than permits stop serving anything.
        let Ok(permit) = broker.queued_requests_sem.clone().acquire_owned().await else {
            break;
        };
        let _queued_guard =
            QueuedRequestGuard::new(permit, byte_permit, &broker.metrics, frame.len());
        // After a `SaslHandshake` v0 the exchange carries raw size-prefixed
        // SASL tokens with no Kafka request header, both ways.
        // A failed v0 exchange closes with no response.
        if is_sasl_listener && sasl_session.expects_raw_token(&auth) {
            let session = SaslConnection {
                listener: &sasl_listener,
                session: &mut sasl_session,
            };
            let token = sasl::handle_raw_sasl_token(&broker, &frame, &mut auth, session, &peer);
            match token.await {
                Some(token) if framed.send(token.clone()).await.is_ok() => continue,
                _ => break,
            }
        }
        let Some(SpannedRequest {
            parsed,
            span: req_span,
        }) = parse_connection_request(&broker, &frame, &peer)
        else {
            // Bytes the broker cannot read as a request are the same reason
            // as bytes the codec refused, one layer further in: the peer sent
            // something that is not a Kafka request and the connection ends.
            broker
                .metrics
                .record_connection_close(ConnectionCloseReason::DecodeError);
            break;
        };
        // KIP-368 session expiry, enforced where Kafka enforces it: on the
        // request that arrives past the deadline, not on a timer racing the
        // read. A client re-authenticates only when it next has something to
        // send, so it routinely opens the exchange after its window closed;
        // `expired_for_request` lets the two SASL api_keys through so that
        // exchange runs, and ends the connection on anything else.
        if is_sasl_listener && auth.expired_for_request(parsed.api_key, now_ms()) {
            // Kafka's `Processor.processCompletedReceives` logs this at DEBUG.
            tracing::debug!(
                api_key = parsed.api_key,
                principal = ?auth.principal().map(|p| p.name.as_str()),
                listener = %spec.name,
                "SASL session expired, closing connection (KIP-368)"
            );
            broker
                .metrics
                .record_connection_close(ConnectionCloseReason::SaslSessionExpired);
            break;
        }
        // Per-state request gate after Kafka's `SaslServerAuthenticator`
        // (see `ConnectionAuth::allows_request`). A refused request closes
        // the connection, after the ILLEGAL_SASL_STATE answer Kafka writes
        // when there is one (`sasl::refuse_gated_request`).
        if is_sasl_listener
            && (!auth.allows_request(parsed.api_key)
                || sasl_session.repeats_api_versions(&auth, parsed.api_key))
        {
            if let Some(response) =
                sasl::refuse_gated_request(&broker, &parsed, &auth, &peer, &spec.name).await
            {
                let _ = framed.send(response).await;
            }
            break;
        }
        let Some(entry) = broker.handlers().get(parsed.api_key) else {
            broker.metrics.record_api_request(parsed.api_key);
            tracing::warn!(
                api_key = parsed.api_key,
                api_version = parsed.api_version,
                "unknown api, closing connection"
            );
            break;
        };
        if is_inter_broker_api_on_client_listener(&broker, &spec, parsed.api_key) {
            broker.metrics.record_api_request(parsed.api_key);
            tracing::warn!(
                api_key = parsed.api_key,
                listener = %spec.name,
                "controller-scoped api key received on a client-reachable listener, closing connection"
            );
            break;
        }
        if !is_served_version(&broker, entry, &parsed) {
            match reject_unsupported_version(&mut framed, &broker, entry, &parsed, &auth).await {
                AfterResponse::Close => break,
                AfterResponse::Mute(window) => mute_until = mute_deadline(window),
            }
            continue;
        }
        // SASL frames (api_key 17 / 36) mutate the per-connection auth state,
        // which lives in this loop. They run *before* the regular handler
        // table because handlers receive only `&Broker` and have no way to
        // touch `auth`. Returning `Some(SaslFrameOutcome)` short-circuits
        // the normal registry path for that frame.
        if let Some(outcome) = try_handle_sasl_frame(
            &broker,
            &parsed,
            &mut auth,
            &sasl_listener,
            &mut sasl_session,
            &peer,
        )
        .instrument(req_span.clone())
        .await
        {
            let SaslFrameOutcome {
                response_bytes,
                close_after,
            } = match outcome {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!(error = %e, "SASL dispatch error, closing connection");
                    break;
                }
            };
            if let Err(e) = framed.send(response_bytes).await {
                tracing::warn!(error = %e, "framed.send error during SASL, closing");
                break;
            }
            if close_after {
                tracing::info!("closing connection after a refused SASL frame");
                break;
            }
            continue;
        }

        capture_client_software(&parsed, &mut client_software.0, &mut client_software.1);
        sasl_session.note_api_versions(&auth, &parsed);

        let BegunRequest {
            in_flight: _in_flight,
            ..
        } = begin_request(&broker, &parsed);

        let context = DispatchContext {
            broker: &broker,
            parsed: &parsed,
            frame: &frame,
            auth: &auth,
            peer: &peer,
            connection_id: &connection_id,
            listener_name: &spec.name,
            client_software_name: &client_software.0,
            client_software_version: &client_software.1,
        };

        if matches!(entry.kind(), crate::handlers::DispatchKind::Fetch) {
            match dispatch_fetch(&mut framed, context, req_span.clone()).await {
                AfterResponse::Close => break,
                AfterResponse::Mute(window) => mute_until = mute_deadline(window),
            }
            continue;
        }

        match send_registry_response(&mut framed, entry, context, req_span).await {
            AfterResponse::Close => break,
            AfterResponse::Mute(window) => mute_until = mute_deadline(window),
        }
    }
    broker
        .share_partition_leaders
        .release_connection(&connection_id)
        .await;
    // KIP-714: Kafka registers `ClientMetricsManager.connectionDisconnectListener`
    // with the socket server, which drops the client instance that this
    // connection created.
    broker
        .client_metrics
        .manager
        .connection_closed(&connection_id);
    tracing::debug!("connection closed");
}
