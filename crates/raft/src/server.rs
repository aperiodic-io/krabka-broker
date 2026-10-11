//! Accept loop for the controller TCP listener. Receives inbound KIP-595 RPCs
//! (Fetch=1, Vote=52, BeginQuorumEpoch=53, EndQuorumEpoch=54) plus the
//! Krabka-private observer/forward RPCs and feeds them into the local
//! [`KraftController`] engine.
//!
//! Wire shape matches `krabka_client_core::Connection::raw_request`:
//!
//! - Request: `len(i32) | RequestHeader v1/v2 | body`
//! - Response: `len(i32) | ResponseHeader v0/v1 | body`
//!
//! Both request headers begin with `api_key(i16) api_version(i16)
//! correlation_id(i32) client_id(NULLABLE_STRING)`. Flexible APIs add tagged
//! fields. The body is decoded by the selected controller or Admin handler.

use std::{net::SocketAddr, sync::Arc};

use krabka_units::prelude::ByteSizeExt as _;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

mod api_versions;
mod authorization;
mod describe_cluster;
pub(crate) mod describe_quorum;
mod dispatch;
mod framing;
mod kip853;
mod metadata_rpc;
mod registration;
mod sasl;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests_api_versions;
#[cfg(test)]
mod tests_listener_limits;
mod voter_admin;

pub use self::api_versions::{
    api_versions_max_version, finalized_feature_keys, is_valid_api_versions_request,
    is_valid_client_info, supported_feature_key, supported_feature_keys,
    unsupported_version_response,
};
use self::{
    api_versions::{API_KEY_API_VERSIONS, api_versions_response},
    describe_cluster::{
        API_KEY_DESCRIBE_CLUSTER, CONTROLLER_LISTENER_NAME, describe_cluster_response_body,
    },
    dispatch::{dispatch_with_router, is_native_raft_api},
    framing::{
        is_eof, read_one_request, write_response, write_response_frame,
        write_response_no_tagged_fields,
    },
    kip853::{
        API_KEY_ADD_RAFT_VOTER, API_KEY_DESCRIBE_QUORUM, API_KEY_REMOVE_RAFT_VOTER,
        API_KEY_UPDATE_RAFT_VOTER, kip853_admin_response,
    },
};
use crate::{
    ClusterGrants, ConnectionLimiter, ControllerAdminRouter, ControllerApiVersions, ListenerLimits,
    RaftHandshakeError, RaftListenerHandshake, RaftShardRouter, UnstableApiVersions,
    error::RaftError, kraft::KraftController,
};

/// The listener's own `ApiVersions` answer, handed to the handshake for the
/// requests that arrive before SASL authentication.
struct ListenerApiVersions {
    engine: KraftController,
    admin_router: Option<Arc<dyn ControllerAdminRouter>>,
    unstable: Unstable,
}

/// Kafka's internal `unstable.api.versions.enable`, as the controller listener
/// reads it. Kafka's `ControllerServer` builds the listener's
/// `SimpleApiVersionManager` with it alone: `unstable.feature.versions.enable`
/// governs the controller's `UpdateFeatures` validation, not the listener's
/// `ApiVersions` answer.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Unstable {
    /// `unstable.api.versions.enable`.
    pub(crate) api_versions: UnstableApiVersions,
}

impl ControllerApiVersions for ListenerApiVersions {
    fn respond(
        &self,
        request_version: i16,
        request_body: &[u8],
    ) -> Result<bytes::Bytes, RaftHandshakeError> {
        answer_api_versions(
            &self.engine,
            self.admin_router.as_deref(),
            self.unstable,
            request_version,
            request_body,
        )
        .map_err(|error| RaftHandshakeError::Protocol(error.to_string()))
    }
}

/// The controller listener's `ApiVersions` answer from `engine`'s current
/// state, before or after SASL authentication alike.
fn answer_api_versions(
    engine: &KraftController,
    admin_router: Option<&dyn ControllerAdminRouter>,
    unstable: Unstable,
    request_version: i16,
    request_body: &[u8],
) -> Result<bytes::Bytes, RaftError> {
    // The offset is read before the image, so the epoch never runs ahead of
    // the features it stamps.
    let metadata_offset = api_versions::applied_metadata_offset(engine);
    let image = engine.current_image();
    api_versions_response(
        request_version,
        request_body,
        api_versions::ApiVersionsView {
            image: &image,
            metadata_offset,
            admin_router,
            unstable: unstable.api_versions,
        },
    )
}

struct ConnectionContext {
    peer: SocketAddr,
    principal: Option<krabka_security::Principal>,
    authenticated_via_token: bool,
    expires_at_ms: Option<i64>,
    grants: Arc<dyn ClusterGrants>,
    /// Kafka's `unstable.*.enable` settings for this listener.
    unstable: Unstable,
    /// The request-size and idle limits of this listener.
    limits: ListenerLimits,
}

pub(crate) async fn run(
    listener: TcpListener,
    engine: KraftController,
    shutdown: CancellationToken,
    handshake: Option<Arc<dyn RaftListenerHandshake>>,
    shard_router: Option<Arc<dyn RaftShardRouter>>,
    admin_router: Option<Arc<dyn ControllerAdminRouter>>,
    (unstable, limits): (Unstable, ListenerLimits),
) {
    match listener.local_addr() {
        Ok(addr) => info!(%addr, "controller listener started"),
        Err(e) => info!(error = %e, "controller listener started (addr unknown)"),
    }
    // Kafka's `ControllerServer` builds its own `SocketServer`, whose
    // `ConnectionQuotas` counts this listener's connections apart from any
    // broker listener's.
    let connections = ConnectionLimiter::new(limits.max_connections, limits.max_connections_per_ip);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        // `max.connections` and `max.connections.per.ip`: a
                        // connection over either ceiling is closed at once.
                        // The guard moves into the task, so the slot is
                        // released however the connection ends.
                        let connection_slot = match connections.try_acquire(peer.ip()) {
                            Ok(slot) => slot,
                            Err(limit) => {
                                tracing::debug!(
                                    %peer,
                                    ?limit,
                                    "controller connection limit reached; closing connection"
                                );
                                continue;
                            }
                        };
                        let engine = engine.clone();
                        let shutdown = shutdown.clone();
                        let handshake = handshake.clone();
                        let shard_router = shard_router.clone();
                        let admin_router = admin_router.clone();
                        tokio::spawn(async move {
                            let _connection_slot = connection_slot;
                            let connection = if let Some(hs) = handshake {
                                let api_versions = ListenerApiVersions {
                                    engine: engine.clone(),
                                    admin_router: admin_router.clone(),
                                    unstable,
                                };
                                // Kafka registers a channel with its `Selector` at
                                // accept time, so `connections.max.idle.ms` also
                                // covers a peer that never finishes the TLS or
                                // SASL handshake.
                                let upgraded = within_idle_window(
                                    limits.max_idle,
                                    hs.upgrade(stream, &api_versions),
                                )
                                .await;
                                match upgraded {
                                    Some(Ok(s)) => s,
                                    Some(Err(e)) => {
                                        tracing::debug!(%peer, error = %e, "handshake failed");
                                        return;
                                    }
                                    None => {
                                        tracing::debug!(
                                            %peer,
                                            "handshake idle past connections.max.idle.ms, closing"
                                        );
                                        return;
                                    }
                                }
                            } else {
                                crate::RaftConnection {
                                    stream: Box::new(stream) as Box<dyn krabka_client_core::ClientDuplex>,
                                    principal: None,
                                    authenticated_via_token: false,
                                    expires_at_ms: None,
                                    grants: Arc::new(crate::AllowAllGrants),
                                }
                            };
                            if let Err(e) = handle_conn(
                                connection.stream,
                                engine,
                                shutdown,
                                shard_router,
                                admin_router,
                                ConnectionContext {
                                    peer,
                                    principal: connection.principal,
                                    authenticated_via_token: connection.authenticated_via_token,
                                    expires_at_ms: connection.expires_at_ms,
                                    grants: connection.grants,
                                    unstable,
                                    limits,
                                },
                                crate::kraft::KraftController::wall_clock_ms,
                            ).await {
                                error!(%peer, error = %e, "controller connection error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "controller listener accept failed");
                    }
                }
            }
        }
    }
}

/// Runs `future`, giving up once `window` passes. `None` for the window waits
/// as long as the future takes, and `None` from this function means it gave
/// up.
async fn within_idle_window<F: std::future::Future>(
    window: Option<std::time::Duration>,
    future: F,
) -> Option<F::Output> {
    match window {
        Some(window) => tokio::time::timeout(window, future).await.ok(),
        None => Some(future.await),
    }
}

async fn handle_conn<S>(
    mut stream: S,
    engine: KraftController,
    shutdown: CancellationToken,
    shard_router: Option<Arc<dyn RaftShardRouter>>,
    admin_router: Option<Arc<dyn ControllerAdminRouter>>,
    context: ConnectionContext,
    clock_ms: impl Fn() -> i64 + Send,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // Kafka's `SocketServer` hands `socket.request.max.bytes` to each
    // `Processor`, and its `Selector` closes a channel that goes
    // `connections.max.idle.ms` without a request. The window restarts for
    // every request, since each pass through the loop arms a fresh timer.
    let max_request_bytes =
        usize::try_from(context.limits.max_request_size.bytes_u64()).unwrap_or(usize::MAX);
    loop {
        let request = within_idle_window(
            context.limits.max_idle,
            read_one_request(&mut stream, admin_router.as_deref(), max_request_bytes),
        );
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            res = request => {
                let Some(res) = res else {
                    tracing::debug!(
                        peer = %context.peer,
                        "controller connection idle past connections.max.idle.ms, closing"
                    );
                    return Ok(());
                };
                let (api_key_n, api_version, correlation_id, client_id, body, response_flexible) = match res {
                    Ok(v) => v,
                    Err(e) => {
                        // Treat peer EOF as a clean shutdown of this conn.
                        if is_eof(&e) {
                            return Ok(());
                        }
                        return Err(e);
                    }
                };
                if let Some(expiry) = context.expires_at_ms
                    && !krabka_verified::authz::controller_request_admission(
                        Some(expiry), api_key_n.get(), clock_ms(),
                    )
                {
                    tracing::debug!(peer = %context.peer, "controller credential expired, closing");
                    return Ok(());
                }
                // ApiVersions (18) is the bootstrap handshake performed by
                // `Connection::connect`. It arrives at v0 with a header v1 (no
                // tagged-fields byte) and expects a ResponseHeader v0 reply (also
                // no tagged-fields byte) — the documented Kafka asymmetry. We
                // serialize it separately rather than poisoning the generic codec.
                if api_key_n == API_KEY_API_VERSIONS {
                    // ApiVersionsResponse always uses a v0 ResponseHeader (no
                    // tagged-fields byte), but the BODY shape depends on the
                    // request version: v0..=2 are non-flexible (i32 array), v3+
                    // are flexible (compact array). Krabka's own client asks at
                    // v0; the JVM controller asks at v4. The generated codec
                    // speaks the raw `int16`, so unwrap the version here.
                    let resp = answer_api_versions(
                        &engine,
                        admin_router.as_deref(),
                        context.unstable,
                        api_version.get(),
                        &body,
                    )?;
                    write_response_no_tagged_fields(&mut stream, correlation_id, resp).await?;
                    continue;
                }
                // Kafka's `Processor.parseRequestHeader` refuses a
                // `latestVersionUnstable` version while
                // `unstable.api.versions.enable` is off
                // (`ApiKeys.isVersionEnabled`), and `SocketServer` closes the
                // connection without a response.
                if api_versions::is_disabled_version(
                    api_key_n.get(),
                    api_version.get(),
                    admin_router.as_deref(),
                    context.unstable.api_versions,
                ) {
                    tracing::debug!(
                        peer = %context.peer,
                        api_key = api_key_n.0,
                        api_version = api_version.get(),
                        "disabled unstable api version, closing controller connection"
                    );
                    return Ok(());
                }
                // Kafka's `ControllerApis` authorizes every request with the
                // cluster operation its api needs, and answers a denial in the
                // shape of that api. The apis that the Admin router sends to a
                // broker handler authorize in that handler.
                if let Some(operation) = authorization::required_operation(api_key_n.0)
                    && !context.grants.allows(operation)
                {
                    tracing::debug!(
                        peer = %context.peer,
                        principal = ?context.principal.as_ref().map(|p| p.name.as_str()),
                        api_key = api_key_n.0,
                        ?operation,
                        "controller-listener request denied"
                    );
                    let resp = authorization::refusal(api_key_n.0, api_version.get(), &body)?;
                    write_response_frame(&mut stream, correlation_id, resp, response_flexible)
                        .await?;
                    continue;
                }
                // DescribeCluster (60, KIP-919) is served here rather than in
                // `dispatch` because it needs the request version (for the
                // flexible body codec) and the controller's metadata image. The
                // flexible v1 ResponseHeader is supplied by `write_response`.
                if api_key_n == API_KEY_DESCRIBE_CLUSTER {
                    let resp = describe_cluster_response_body(
                        api_version.get(),
                        &body,
                        &engine,
                        context.grants.as_ref(),
                        CONTROLLER_LISTENER_NAME,
                    )
                    .await?;
                    write_response(&mut stream, correlation_id, resp).await?;
                    continue;
                }
                if matches!(
                    api_key_n.0,
                    API_KEY_DESCRIBE_QUORUM
                        | API_KEY_ADD_RAFT_VOTER
                        | API_KEY_REMOVE_RAFT_VOTER
                        | API_KEY_UPDATE_RAFT_VOTER
                ) {
                    let resp = kip853_admin_response(
                        api_key_n.0,
                        api_version.get(),
                        &body,
                        &engine,
                    )
                    .await?;
                    write_response(&mut stream, correlation_id, resp).await?;
                    continue;
                }
                if sasl::is_sasl_api(api_key_n.0) {
                    let resp = sasl::sasl_response(api_key_n.0, api_version.get(), &body)?;
                    write_response_frame(&mut stream, correlation_id, resp, response_flexible)
                        .await?;
                    continue;
                }
                if registration::is_controller_api(api_key_n.0) {
                    let resp = registration::dispatch(
                        api_key_n.0,
                        api_version.get(),
                        &body,
                        &engine,
                    )
                    .await?;
                    write_response(&mut stream, correlation_id, resp).await?;
                    continue;
                }
                if !is_native_raft_api(api_key_n.0)
                    && let Some(router) = admin_router.as_deref()
                    && let Some(response) = router
                        .route(crate::ControllerAdminRequest {
                            api_key: api_key_n.get(),
                            api_version: api_version.get(),
                            correlation_id,
                            client_id,
                            body: body.clone(),
                            peer: context.peer,
                            principal: context.principal.clone(),
                            authenticated_via_token: context.authenticated_via_token,
                        })
                        .await?
                {
                    write_response_frame(
                        &mut stream,
                        correlation_id,
                        response.body,
                        response.flexible,
                    )
                    .await?;
                    continue;
                }
                let resp = dispatch_with_router(
                    api_key_n,
                    api_version.get(),
                    body,
                    &engine,
                    shard_router.as_deref(),
                    context.principal.as_ref(),
                ).await?;
                write_response_frame(&mut stream, correlation_id, resp, response_flexible).await?;
            }
        }
    }
}
