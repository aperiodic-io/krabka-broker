//! The prelude the three KIP-853 voter handlers share.
//!
//! `AddRaftVoter`, `RemoveRaftVoter` and `UpdateRaftVoter` each decode the
//! body, apply one cluster ACL gate, forward the untouched bytes to the active
//! controller from a broker-only observer, and answer
//! `NOT_LEADER_OR_FOLLOWER` on a node with no quorum state. Only the request
//! checks after that differ.

use {
    crate::{
        authorizer::Authorizer,
        broker::Broker,
        codes::CLUSTER_AUTHORIZATION_FAILED,
        error::BrokerError,
        handlers::{ErrorResponse, RequestContext, decode_request, encode_response},
    },
    bytes::Bytes,
    krabka_metadata::MetadataImage,
    krabka_protocol::{Decode, Encode},
    std::{ops::ControlFlow, sync::Arc},
};

/// A cluster ACL gate: `true` when the authorizer denies `ctx`'s principal.
pub(super) type ClusterGate = fn(&dyn Authorizer, &MetadataImage, &RequestContext<'_>) -> bool;

#[cfg(test)]
macro_rules! test_dispatch {
    ($api:expr, $request:ty, $response:ty) => {
        krabka_macros::wire_codec_helpers! {
            test_context ["admin-client"];
        }
        async fn answer(
            broker: &crate::broker::Broker,
            version: i16,
            request: &$request,
            ctx: &crate::handlers::RequestContext<'_>,
        ) -> $response {
            crate::test_support::dispatch_wire(broker, $api, version, request, ctx).await
        }
    };
}

#[cfg(test)]
pub(super) use test_dispatch;

/// The valid `AddRaftVoter` fixture shared by the voter handler tests.
#[cfg(test)]
#[bon::builder]
pub(crate) fn add_voter_request(
    voter_id: i32,
) -> krabka_protocol::owned::add_raft_voter_request::AddRaftVoterRequest {
    use krabka_protocol::{
        owned::add_raft_voter_request::Listener, primitives::uuid::Uuid as ProtoUuid,
    };

    krabka_protocol::owned::add_raft_voter_request::AddRaftVoterRequest {
        cluster_id: Some("cluster".into()),
        timeout_ms: 1_000,
        voter_id,
        voter_directory_id: ProtoUuid([2; 16]),
        listeners: vec![Listener {
            name: "CONTROLLER".into(),
            host: "127.0.0.1".into(),
            port: 9093,
            ..Default::default()
        }],
        ack_when_committed: true,
        ..Default::default()
    }
}

/// Drive the shared denial scenario while keeping each API's optional message check.
#[cfg(test)]
macro_rules! check_denied_reconfiguration {
    (($handle:ident, $directory:ident, $broker:ident, $context:ident, $response:ident), $version:expr $(, $message:expr)?) => {
        krabka_macros::broker_test_setup! {
            bind($handle, $directory, $broker, $context);
            boot [start_broker] deny_all;
            user ["alice"];
        };
        let $response = answer(&$broker, $version, &request(2), &$context).await;
        assert!($response.error_code == crate::codes::CLUSTER_AUTHORIZATION_FAILED);
        $(assert!($response.error_message.as_deref() == Some($message));)?
        $handle.shutdown().await;
    };
}
#[cfg(test)]
pub(super) use check_denied_reconfiguration;

/// An invalid add/remove id is refused before the controller reconfigures.
#[cfg(test)]
macro_rules! check_invalid_voter {
    (
        (
            $handle:ident,
            $directory:ident,
            $broker:ident,
            $context:ident,
            $request:ident,
            $response:ident
        ),
        $version:expr,
        $response_type:ident,
        $message:expr
    ) => {
        krabka_macros::broker_test_setup! {
            bind($handle, $directory, $broker, $context);
            boot [start_broker] allow_all;
            user ["admin"];
        };
        let mut $request = request(-7);
        stamp_voter_request!($request, $broker);
        let $response = answer(&$broker, $version, &$request, &$context).await;
        assert!(
            $response
                == $response_type {
                    error_code: crate::codes::INVALID_REQUEST,
                    error_message: Some($message.into()),
                    ..Default::default()
                }
        );
        $handle.shutdown().await;
    };
}
#[cfg(test)]
pub(super) use check_invalid_voter;

/// What a voter handler runs its own checks on once the prelude lets the
/// request through.
pub(super) struct Admitted<R> {
    pub(super) req: R,
    pub(super) image: Arc<MetadataImage>,
    pub(super) quorum: krabka_raft::QuorumStateSnapshot,
}

/// The answers the prelude gives on its own: `denied` when `gate` refuses the
/// principal, and `not_leader` when this node holds no quorum state.
pub(super) struct Refusals<R> {
    pub(super) denied: R,
    pub(super) not_leader: R,
}

impl<R: ErrorResponse> Refusals<R> {
    /// Add/remove refusals carry the denial message and Kafka's empty leader-check message.
    pub(super) fn leader_only(denied: &str) -> Self {
        Self {
            denied: R::error(CLUSTER_AUTHORIZATION_FAILED, Some(denied.into())),
            not_leader: R::error(
                krabka_raft::voter_requests::NOT_LEADER_OR_FOLLOWER,
                Some(String::new()),
            ),
        }
    }
}

/// Decodes the request, applies `gate`, forwards the raw body as `api_key`
/// from a broker-only observer, and reads the quorum.
///
/// `Break` carries the encoded answer when the gate refuses, the controller
/// answered a forward, or this node is not the leader.
///
/// # Errors
///
/// Returns [`BrokerError`] when the body does not decode, the forward fails,
/// or a refusal does not encode.
pub(super) async fn prelude<Req: for<'a> Decode<'a>, Resp: Encode>(
    broker: &Broker,
    version: i16,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
    api_key: i16,
    gate: ClusterGate,
    refusals: Refusals<Resp>,
) -> Result<ControlFlow<Bytes, Admitted<Req>>, BrokerError> {
    let req = decode_request::<Req>(req_bytes, version)?;

    let image = broker.controller.current_image();

    if gate(broker.config.authorizer.as_ref(), &image, ctx) {
        return encode_response(&refusals.denied, version).map(ControlFlow::Break);
    }

    // Broker-only observer forward to the active controller quorum (#392)
    if let Some(forwarded) = broker
        .controller
        .forward_raw(api_key, version, Bytes::copy_from_slice(req_bytes))
        .await
    {
        return forwarded.map(ControlFlow::Break).map_err(BrokerError::from);
    }

    // The request checks, their order and their codes are the controller
    // listener's own (`krabka_raft::voter_requests`).
    let Some(quorum) = broker.controller.quorum_snapshot() else {
        return encode_response(&refusals.not_leader, version).map(ControlFlow::Break);
    };
    Ok(ControlFlow::Continue(Admitted { req, image, quorum }))
}

/// Encodes a response that carries only `error_code` and `error_message`.
///
/// # Errors
///
/// Returns [`BrokerError`] when the response does not encode.
pub(super) fn respond<R: ErrorResponse + Encode>(
    version: i16,
    error_code: i16,
    error_message: Option<String>,
) -> Result<Bytes, BrokerError> {
    encode_response(&R::error(error_code, error_message), version)
}
