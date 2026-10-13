//! Fetch (`api_key` 1) dispatch. It authorizes the request through the
//! per-connection principal and returns the response as an ordered write plan
//! rather than one contiguous buffer, so that records regions stay zero-copy.

use {
    super::{AfterResponse, response::encode_response, session::principal_or_anonymous},
    crate::{
        error::BrokerError,
        handlers::{
            RequestContext,
            fetch::{encode_fetch_response, handle},
        },
        network::{
            codec::{KafkaCodec, response_frame_length},
            fetch_writer::{SendfileSink, WriteOp, resolve_records_inline, write_fetch_plan},
        },
    },
    bytes::{BufMut, Bytes, BytesMut},
    futures_util::SinkExt,
    tokio::io::{AsyncRead, AsyncWrite},
    tokio_util::codec::Framed,
    tracing::Instrument as _,
};

/// A finished fetch: the ordered write plan for its response, and the KIP-219
/// window the fetch quotas charged. The connection loop writes the plan first
/// and mutes for the window afterwards.
struct FetchPlan {
    operations: Vec<WriteOp>,
    throttle: krabka_units::Time,
}

pub(super) async fn dispatch_fetch<S>(
    framed: &mut Framed<S, KafkaCodec>,
    context: super::registry::DispatchContext<'_, '_>,
    request_span: tracing::Span,
) -> AfterResponse
where
    S: AsyncRead + AsyncWrite + Unpin + SendfileSink,
{
    let sendfile_capable = SendfileSink::is_sendfile_capable(framed.get_ref());
    match handle_fetch_frame_from_parsed(context, sendfile_capable)
        .instrument(request_span)
        .await
    {
        Ok(FetchPlan {
            operations,
            throttle,
        }) => {
            if let Err(error) = SinkExt::<Bytes>::flush(framed).await {
                tracing::warn!(%error, "framed.flush error before fetch plan, closing");
                return AfterResponse::Close;
            }
            if let Err(error) =
                write_fetch_plan(framed.get_mut(), operations, &context.broker.metrics).await
            {
                tracing::warn!(%error, "fetch plan write error, closing");
                return AfterResponse::Close;
            }
            // KIP-219: the records are on the wire; the KIP-13 consumer and
            // KIP-124 request quotas are enforced by muting from here.
            AfterResponse::Mute(throttle)
        }
        Err(error) => {
            context
                .broker
                .metrics
                .record_request_error(context.parsed.api_key);
            tracing::warn!(%error, "Fetch dispatch error, closing connection");
            AfterResponse::Close
        }
    }
}

/// Decodes and dispatches a `Fetch` (`api_key` 1) frame.
///
/// The function reads the authenticated principal from the per-connection
/// `auth` state, and the peer `SocketAddr` from the accept-time capture, so
/// the handler can batch-authorize every topic in the request for `Read`. On
/// PLAINTEXT and SSL listeners the loop init makes the connection implicitly
/// `Authenticated { ANONYMOUS / Plain }`, so `principal()` always returns
/// `Some` here. The `unwrap_or_else` fallback covers the defensive SASL
/// pre-auth case.
///
/// The function builds the Fetch response as an ordered [`WriteOp`] plan
/// instead of one contiguous `Bytes`. The first op of the plan carries the
/// 4-byte frame length and the correlation header. The later ops are the
/// response envelope, interleaved with the records region of each partition.
/// A records region is a refcounted view of the verbatim `.log` bytes, with no
/// copy. The connection writer drains the plan directly on the raw stream, and
/// skips `encode_response` and the `Framed` codec copies.
///
/// The legacy v0 to v3 path down-converts and has no canonical write plan.
/// The function encodes it the old way and returns it as a single `Inline` op,
/// which is the existing copy path expressed as a one-element plan.
///
/// It returns the plan together with the KIP-219 throttle window the fetch
/// quotas charged, which the caller applies by muting the connection once the
/// plan is written.
async fn handle_fetch_frame_from_parsed(
    context: super::registry::DispatchContext<'_, '_>,
    sendfile_capable: bool,
) -> Result<FetchPlan, BrokerError> {
    use crate::network::fetch_writer::build_fetch_plan;
    let super::registry::DispatchContext {
        broker,
        parsed,
        auth,
        peer,
        listener_name,
        ..
    } = context;

    assert2::assert!((parsed.api_key) == (1));

    let principal = principal_or_anonymous(auth);
    let ctx = RequestContext::new(
        principal,
        peer,
        parsed.client_id,
        "",
        sendfile_capable && parsed.api_version >= 4,
        listener_name,
    )
    .with_correlation_id(parsed.correlation_id);

    let (resp, version) = handle(
        broker,
        parsed.api_version,
        parsed.correlation_id,
        parsed.body,
        &ctx,
    )
    .await?;
    // Drained before the plan is built: the connection loop mutes for this
    // window only once the whole plan has been written (KIP-219).
    let throttle = ctx.take_throttle();

    if version < 4 {
        // Legacy down-conversion path: encode the whole body the old way and
        // wrap it (plus the response header) as a single inline op.
        let body_bytes = encode_fetch_response(resp, version)?;
        let framed = encode_response(
            parsed.api_key,
            parsed.correlation_id,
            parsed.body_flexible,
            &body_bytes,
        )?;
        // Prepend the 4-byte frame length so the writer path is uniform.
        let mut framed_with_len = BytesMut::with_capacity(4 + framed.len());
        framed_with_len.put_u32(response_frame_length(framed.len())?);
        framed_with_len.put_slice(&framed);
        return Ok(FetchPlan {
            operations: vec![WriteOp::Inline(framed_with_len.freeze())],
            throttle,
        });
    }

    // On plaintext connections (SENDFILE alias: Linux + Apple + FreeBSD/
    // DragonFly), drain file-backed records regions via sendfile; everywhere
    // else (TLS, Windows) use the portable vectored resolver. `do_read` only
    // ever emits `FileRegions` when `sendfile_capable`, so the resolver choice
    // and the payload kind stay in lock-step.
    krabka_macros::sendfile_platform! {
        {
            if sendfile_capable && parsed.api_version >= 4 {
                return build_fetch_plan(
                    &resp,
                    version,
                    parsed.correlation_id,
                    parsed.body_flexible,
                    crate::network::fetch_writer::resolve_records_sendfile,
                )
                .map(|operations| FetchPlan {
                    operations,
                    throttle,
                });
            }
    }
    }

    build_fetch_plan(
        &resp,
        version,
        parsed.correlation_id,
        parsed.body_flexible,
        resolve_records_inline,
    )
    .map(|operations| FetchPlan {
        operations,
        throttle,
    })
}
