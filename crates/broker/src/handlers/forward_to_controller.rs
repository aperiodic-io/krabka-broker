//! The broker-listener half of an admin RPC that Kafka's `KafkaApis` answers
//! with `forwardToController`: this module wraps the request in a KIP-590
//! `Envelope` under the caller's own principal and address, sends it to the
//! active controller, and unwraps the answer.
//!
//! The failure answers are `ForwardingManagerImpl`'s. An `Envelope` refused
//! with any code but `UNSUPPORTED_VERSION` becomes `UNKNOWN_SERVER_ERROR`, an
//! `UNSUPPORTED_VERSION` closes the client connection, and a controller that
//! cannot be reached becomes the `REQUEST_TIMED_OUT` of the forwarding
//! timeout.
//!
//! What runs on the active controller is the API's own handler, reached through
//! `controller_admin`. A handler that mutates state from what it reads of the
//! metadata image must run there: any other node's image can trail the
//! controller's, and a write built from a stale image rolls the newer state
//! back.

use std::{future::Future, sync::Arc, time::Duration};

use bytes::{Bytes, BytesMut};
use krabka_metadata::MetadataImage;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        envelope_request,
        envelope_response::{self, EnvelopeResponse},
    },
};
use krabka_raft::RaftError;

use crate::{
    broker::Broker,
    codes,
    envelope::{self, ForwardedPrincipal, ForwardedRequest},
    error::BrokerError,
    handlers::{ApiKeyCode, RequestContext},
    metadata_source::MetadataSource,
};

/// `Errors.UNKNOWN_SERVER_ERROR.message()`, which the
/// `UnknownServerException` `ForwardingManagerImpl` builds carries.
const UNKNOWN_SERVER_ERROR_MESSAGE: &str =
    "The server experienced an unexpected error when processing the request.";

/// Rebuild a controller mutation after conflicting metadata or a pending tail.
///
/// `operation` must submit with `submit_change_if_image`; each attempt gets a
/// fresh authoritative image. The deadline covers admission and commit waits.
///
/// # Errors
/// Returns the deadline error outside the operation's own result.
pub(crate) async fn guarded_operation<T, F, Fut>(
    broker: &Broker,
    deadline: tokio::time::Instant,
    operation: F,
) -> Result<Result<T, RaftError>, tokio::time::error::Elapsed>
where
    F: FnMut(Arc<MetadataImage>) -> Fut,
    Fut: Future<Output = Result<T, RaftError>> + Send,
{
    guarded_operation_on_source(broker.controller.as_ref(), deadline, operation).await
}

/// Metadata-source seam shared by the broker wrapper and race regressions.
///
/// # Errors
/// Returns the deadline error outside the operation's own result.
pub(super) async fn guarded_operation_on_source<T, F, Fut>(
    source: &dyn MetadataSource,
    deadline: tokio::time::Instant,
    mut operation: F,
) -> Result<Result<T, RaftError>, tokio::time::error::Elapsed>
where
    F: FnMut(Arc<MetadataImage>) -> Fut,
    Fut: Future<Output = Result<T, RaftError>> + Send,
{
    tokio::time::timeout_at(deadline, async {
        loop {
            let mut images = source.watch_image();
            let image = images.borrow_and_update().clone();
            let result = operation(image).await;
            match result {
                Err(RaftError::StaleMetadataImage | RaftError::UncommittedTail) => {
                    tokio::select! {
                        changed = images.changed() => {
                            changed.map_err(|_| RaftError::LeaderUnknown)?;
                        }
                        () = tokio::time::sleep(Duration::from_millis(10)) => {},
                    }
                }
                other => return other,
            }
        }
    })
    .await
}

/// Route a typed controller-listener request through its own response codec.
/// Broker-listener requests continue to preprocessing and forwarding; a
/// controller listener refuses mutations after this node loses leadership.
macro_rules! controller_listener_dispatch {
    ($broker:ident, $ctx:ident, $req:ident, $version:ident, $handle:path, $refusal:path) => {
        if $ctx.connection_id == $crate::controller_admin::CONTROLLER_ADMIN_CONNECTION_ID {
            if !$crate::handlers::forward_to_controller::is_active_controller($broker) {
                return $crate::handlers::encode_response(
                    &$refusal(&$req, $crate::codes::NOT_CONTROLLER, None),
                    $version,
                );
            }
            return $crate::handlers::encode_response(
                &$handle($broker, $req, $version, $ctx).await?,
                $version,
            );
        }
    };
}
pub(super) use controller_listener_dispatch;

/// `ControllerExceptions.newWrongControllerException`'s message, the one a
/// request that only the active controller may run carries when it reaches
/// another node.
pub(crate) fn wrong_controller_message(leader: Option<krabka_metadata::NodeId>) -> String {
    leader.map_or_else(
        || "No controller appears to be active.".to_owned(),
        |leader| format!("The active controller appears to be node {leader}."),
    )
}

/// Whether this node is the active controller, the one node that may run a
/// request that only the active controller answers.
pub(crate) fn is_active_controller(broker: &Broker) -> bool {
    *broker.controller.watch_leader().borrow() == Some(broker.config.node_id)
}

/// Forwards the request of `api_key` to the active controller and returns the
/// answer, or `None` when this node is the active controller and answers it
/// itself.
///
/// `refusal` encodes the API's own response for an error code and message, for
/// the answers that `ForwardingManagerImpl` makes up when the controller gives
/// none.
pub(crate) async fn to_active_controller(
    broker: &Broker,
    api_key: ApiKeyCode,
    req_bytes: &[u8],
    version: i16,
    ctx: &RequestContext<'_>,
    refusal: impl Fn(i16, Option<&str>) -> Result<Bytes, BrokerError>,
) -> Option<Result<Bytes, BrokerError>> {
    let body = match build(broker, api_key, req_bytes, version, ctx) {
        Ok(body) => body,
        Err(error) => return Some(Err(error)),
    };
    let forwarded = broker
        .controller
        .forward_raw(
            envelope_request::API_KEY,
            envelope_request::MIN_VERSION,
            body,
        )
        .await?;
    Some(match forwarded {
        Ok(envelope_response) => unwrap(broker, api_key, &envelope_response, version, refusal),
        Err(_) => refusal(codes::REQUEST_TIMED_OUT, None),
    })
}

/// Wrap `req_bytes` (an `api_key` request body at `version`) in a KIP-590
/// `EnvelopeRequest` carrying `ctx`'s principal and peer address, and encode
/// it at the envelope's one wire version.
///
/// # Errors
/// Returns an error if the generated codec rejects the assembled envelope.
pub(crate) fn build(
    broker: &Broker,
    api_key: ApiKeyCode,
    req_bytes: &[u8],
    version: i16,
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let request_data = envelope::wrap_request(&ForwardedRequest {
        api_key,
        api_version: version,
        correlation_id: 0,
        client_id: ctx.client_id.map(ToOwned::to_owned),
        body: Bytes::copy_from_slice(req_bytes),
        body_flexible: broker.handlers().body_flexible(api_key, version),
    });
    let principal = ForwardedPrincipal {
        name: ctx.principal.name.clone(),
        token_authenticated: false,
    };
    let request = envelope::envelope_request(request_data, &principal, ctx.peer.ip())?;
    let mut out = BytesMut::with_capacity(request.encoded_len(envelope_request::MIN_VERSION));
    request.encode(&mut out, envelope_request::MIN_VERSION)?;
    Ok(out.freeze())
}

fn unwrap(
    broker: &Broker,
    api_key: ApiKeyCode,
    envelope_bytes: &[u8],
    version: i16,
    refusal: impl Fn(i16, Option<&str>) -> Result<Bytes, BrokerError>,
) -> Result<Bytes, BrokerError> {
    let mut cur = envelope_bytes;
    let resp = EnvelopeResponse::decode(&mut cur, envelope_response::MIN_VERSION)?;
    if resp.error_code == codes::UNSUPPORTED_VERSION {
        return Err(BrokerError::UnsupportedApi { api_key, version });
    }
    if resp.error_code != codes::NONE {
        return refusal(
            codes::UNKNOWN_SERVER_ERROR,
            Some(UNKNOWN_SERVER_ERROR_MESSAGE),
        );
    }
    let flexible = broker.handlers().body_flexible(api_key, version);
    let data = resp.response_data.unwrap_or_default();
    let (_, body) = envelope::unwrap_response(api_key, flexible, &data)?;
    Ok(body)
}
