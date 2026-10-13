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

/// Keep the broker and directory alive through the case's assertions and shutdown.
#[cfg(test)]
pub(super) async fn with_voter_broker(
    authorizer: Arc<dyn Authorizer>,
    check: impl for<'a> std::ops::AsyncFnOnce(&'a Broker),
) {
    use crate::{
        handlers::test_support::StartedBroker, test_support::start_broker_with_authorizer,
    };
    let StartedBroker {
        broker: handle,
        dir: _dir,
    } = start_broker_with_authorizer(authorizer).await;
    let broker = handle.broker_arc_for_test();
    check(&broker).await;
    handle.shutdown().await;
}

#[cfg(test)]
krabka_macros::wire_codec_helpers! { test_context ["admin-client"]; }

/// The two voter refusal scenarios and their original authorization identities.
#[cfg(test)]
pub(super) enum VoterCase {
    Denied,
    Invalid,
}

#[cfg(test)]
impl VoterCase {
    fn authorizer(&self) -> Arc<dyn Authorizer> {
        use crate::{authorizer::AllowAllAuthorizer, test_support::DenyAll};
        match self {
            Self::Denied => Arc::new(DenyAll),
            Self::Invalid => Arc::new(AllowAllAuthorizer),
        }
    }

    fn user(&self) -> &'static str {
        match self {
            Self::Denied => "alice",
            Self::Invalid => "admin",
        }
    }
}

/// Check a refusal before releasing the broker or its directory.
#[cfg(test)]
pub(super) async fn check_voter_case<
    Request: Encode,
    Response: for<'a> Decode<'a> + PartialEq + std::fmt::Debug,
>(
    case: VoterCase,
    api: i16,
    version: i16,
    request: impl FnOnce(&Broker) -> Request,
    expected: impl FnOnce() -> Response,
) {
    use crate::test_support::dispatch_wire;
    with_voter_broker(case.authorizer(), async move |broker| {
        krabka_macros::request_test_identity! { bind(ctx); user [case.user()]; };
        let response: Response = dispatch_wire(broker, api, version, &request(broker), &ctx).await;
        assert2::assert!(response == expected());
    })
    .await;
}

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
