//! Registry-backed dispatch. It builds the per-request context for the
//! handler kind that the registry entry names, calls that handler, frames the
//! body it returns, and writes the response back to the client.

use {
    super::{
        AfterResponse,
        response::{ResponseShape, ThrottledResponse, apply_request_quota, encode_response},
        session::principal_or_anonymous,
    },
    crate::{
        broker::Broker,
        error::BrokerError,
        handlers::{
            DispatchEntry, DispatchKind, RequestContext, RequestQuotaPolicy, TelemetryContext,
            produce::response_required,
        },
        network::{auth::ConnectionAuth, codec::KafkaCodec, request::ParsedRequest},
    },
    bytes::Bytes,
    std::net::SocketAddr,
    tokio::io::{AsyncRead, AsyncWrite},
    tokio_util::codec::Framed,
    tracing::Instrument as _,
};

pub(super) async fn send_registry_response<S>(
    framed: &mut Framed<S, KafkaCodec>,
    entry: DispatchEntry,
    context: DispatchContext<'_, '_>,
    request_span: tracing::Span,
) -> AfterResponse
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // KIP-124 meters the time a request holds a handler thread. A handler
    // future that is parked (on a group rebalance, a replication wait, a raft
    // commit) holds none, so only the time spent polling it is charged.
    let Timed {
        output: dispatched,
        active: handler_time,
    } = with_active_time(dispatch_registry_response(entry, context).instrument(request_span)).await;
    let mut response = match dispatched {
        Ok(Some(response)) => response,
        Ok(None) => {
            tracing::warn!("registry entry has no ordinary dispatcher, closing connection");
            return AfterResponse::Close;
        }
        Err(error) => {
            context
                .broker
                .metrics
                .record_request_error(context.parsed.api_key);
            tracing::warn!(%error, "registry dispatch error, closing connection");
            return AfterResponse::Close;
        }
    };
    if response.bytes.is_empty()
        && matches!(entry.kind(), crate::handlers::DispatchKind::Produce(_))
    {
        // `acks=0`: there is no response frame to write. Kafka's
        // `KafkaApis.handleProduceRequest` closes the connection instead of
        // answering when any partition of such a request carried an error --
        // the close is the only signal an acks=0 producer gets, so it is what
        // makes such a producer refresh metadata after, say, a leader move.
        if response.close_after_response {
            return AfterResponse::Close;
        }
        // Otherwise the handler may still have charged a quota, so the mute
        // window stands.
        return AfterResponse::Mute(response.throttle);
    }
    if entry.quota_policy() == RequestQuotaPolicy::ApplyFallbackAccounting {
        // Kafka mutes the channel once per request, for the longest window
        // any quota asked for, so a handler-charged window is folded in with
        // `max` rather than added. A handler that deferred its own charge
        // (`CreateTopics`, `CreatePartitions`, `DeleteTopics` with KIP-599)
        // has it resolved with the request quota in the same metrics call.
        let handler_throttle = response.throttle;
        response = apply_request_quota(
            context.broker,
            response.bytes,
            context.parsed,
            ResponseShape::mirroring_request(context.parsed),
            context.auth,
            handler_time,
            response.deferred_charge,
        );
        response.throttle = response.throttle.max(handler_throttle);
    }
    super::response::send_throttled_response(framed, response).await
}

#[derive(Clone, Copy)]
pub(super) struct DispatchContext<'a, 'request> {
    pub(super) broker: &'a Broker,
    pub(super) parsed: &'a ParsedRequest<'request>,
    pub(super) frame: &'a Bytes,
    pub(super) auth: &'a ConnectionAuth,
    pub(super) peer: &'a SocketAddr,
    pub(super) connection_id: &'a str,
    pub(super) listener_name: &'a str,
    pub(super) client_software_name: &'a str,
    pub(super) client_software_version: &'a str,
}

async fn dispatch_registered_bytes(
    entry: DispatchEntry,
    context: DispatchContext<'_, '_>,
) -> Option<Result<ThrottledResponse, BrokerError>> {
    let DispatchContext {
        broker,
        parsed,
        frame,
        auth,
        peer,
        connection_id,
        listener_name,
        client_software_name,
        client_software_version,
    } = context;
    match entry.kind() {
        DispatchKind::Context(handler) => {
            let ctx = RequestContext::new(
                principal_or_anonymous(auth),
                peer,
                parsed.client_id,
                connection_id,
                false,
                listener_name,
            )
            // The per-state gate lets only `ApiVersions` through to a handler
            // before authentication, and that handler answers it as Kafka's
            // `SaslServerAuthenticator` does.
            .with_pre_authentication(!auth.is_authenticated())
            .with_correlation_id(parsed.correlation_id);
            let encoded = encode_dispatch_result(
                parsed,
                handler(broker, parsed.api_version, parsed.body, &ctx).await,
            );
            Some(with_recorded_throttle(&ctx, encoded))
        }
        DispatchKind::Auth(handler) => Some(unthrottled(encode_dispatch_result(
            parsed,
            handler(
                broker,
                parsed.api_version,
                parsed.correlation_id,
                parsed.body,
                auth,
                peer,
            )
            .await,
        ))),
        DispatchKind::Produce(handler) => {
            let ctx = RequestContext::new(
                principal_or_anonymous(auth),
                peer,
                parsed.client_id,
                connection_id,
                false,
                "",
            )
            // KIP-13: `Produce` charges `producer_byte_rate` with the whole
            // request, as Kafka's `KafkaApis.handleProduceRequest` charges
            // `request.sizeInBytes`.
            .with_request_size(frame.len())
            .with_correlation_id(parsed.correlation_id);
            let body_offset = frame.len() - parsed.body.len();
            let body_bytes = frame.slice(body_offset..);
            let response_required =
                match response_required(parsed.body, body_bytes.clone(), parsed.api_version) {
                    Ok(required) => required,
                    Err(error) => return Some(Err(error)),
                };
            let encoded = encode_dispatch_result(
                parsed,
                handler(broker, parsed.api_version, parsed.body, body_bytes, &ctx).await,
            );
            let encoded = if response_required {
                encoded
            } else {
                encoded.map(|_| Bytes::new())
            };
            Some(with_recorded_throttle(&ctx, encoded))
        }
        DispatchKind::Telemetry(handler) => {
            let ctx = TelemetryContext::new(
                connection_id,
                peer,
                parsed.client_id.unwrap_or(""),
                client_software_name,
                client_software_version,
            );
            Some(unthrottled(encode_dispatch_result(
                parsed,
                handler(
                    broker,
                    parsed.api_version,
                    parsed.correlation_id,
                    parsed.body,
                    &ctx,
                )
                .await,
            )))
        }
        DispatchKind::Fetch | DispatchKind::SaslMetadata => None,
    }
}

/// The output of a future, and the time spent inside its `poll` calls.
pub(super) struct Timed<T> {
    pub(super) output: T,
    pub(super) active: std::time::Duration,
}

/// Awaits `future` and reports, beside its output, the time spent inside its
/// `poll` calls. The time the future is parked between polls is not counted.
pub(super) async fn with_active_time<F: std::future::Future>(future: F) -> Timed<F::Output> {
    let mut future = std::pin::pin!(future);
    let mut active = std::time::Duration::ZERO;
    let output = std::future::poll_fn(|cx| {
        let polled_at = std::time::Instant::now();
        let poll = future.as_mut().poll(cx);
        active += polled_at.elapsed();
        poll
    })
    .await;
    Timed { output, active }
}

/// Pairs a handler's framed bytes with the KIP-219 window that handler
/// recorded on its [`crate::handlers::RequestContext`].
fn with_recorded_throttle(
    ctx: &RequestContext<'_>,
    encoded: Result<Bytes, BrokerError>,
) -> Result<ThrottledResponse, BrokerError> {
    let throttle = ctx.take_throttle();
    let deferred_charge = ctx.throttle.deferred();
    let close_after_response = ctx.take_close_after_response();
    encoded.map(|bytes| ThrottledResponse {
        bytes,
        throttle,
        deferred_charge,
        close_after_response,
    })
}

/// Wraps the bytes of a handler kind that has no `RequestContext` and so can
/// charge no quota of its own.
fn unthrottled(encoded: Result<Bytes, BrokerError>) -> Result<ThrottledResponse, BrokerError> {
    encoded.map(ThrottledResponse::unthrottled)
}

fn encode_dispatch_result(
    parsed: &ParsedRequest<'_>,
    result: Result<Bytes, BrokerError>,
) -> Result<Bytes, BrokerError> {
    result.and_then(|body| {
        encode_response(
            parsed.api_key,
            parsed.correlation_id,
            parsed.body_flexible,
            &body,
        )
    })
}

async fn dispatch_registry_response(
    entry: DispatchEntry,
    context: DispatchContext<'_, '_>,
) -> Result<Option<ThrottledResponse>, BrokerError> {
    match dispatch_registered_bytes(entry, context).await {
        Some(result) => result.map(Some),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{Timed, with_active_time},
        assert2::check,
        std::time::Duration,
    };

    /// KIP-124 meters the time a request holds a handler thread, so a handler
    /// parked on a timer, a channel or a raft commit is not charged for the
    /// wait. Only the time spent inside `poll` counts.
    #[tokio::test]
    async fn active_time_counts_polling_and_not_parking() {
        let Timed {
            output: value,
            active: parked,
        } = with_active_time(async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            7
        })
        .await;
        check!(value == 7);
        check!(parked < Duration::from_millis(100), "{parked:?}");

        let Timed {
            output: (),
            active: working,
        } = with_active_time(async {
            std::thread::sleep(Duration::from_millis(60));
        })
        .await;
        check!(working >= Duration::from_millis(60), "{working:?}");
    }
}
