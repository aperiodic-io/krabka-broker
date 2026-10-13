//! The fixtures that the `AssignReplicasToDirs` unit tests share.
//!
//! The protocol version, the request builder, the response decoder, and the
//! single-broker harness are each used by more than one of the test modules
//! under this module, so they live in one file instead of once per module.

use {
    crate::{broker::Broker, error::BrokerError, handlers::assign_replicas_to_dirs::handle},
    krabka_protocol::owned::{
        assign_replicas_to_dirs_request::AssignReplicasToDirsRequest,
        assign_replicas_to_dirs_response::AssignReplicasToDirsResponse,
    },
};

pub(super) const VERSION: i16 = 0;

krabka_macros::assignment_dirs_fixture!(assignment_request);

pub(super) use assignment_request as request;

krabka_macros::wire_codec_helpers! {
    pub(super) decode_response(AssignReplicasToDirsResponse);
    version [VERSION];
}

/// The epoch broker 1 (the started broker itself) registered with. A request
/// naming this epoch is the current, non-stale one.
pub(super) fn own_broker_epoch(broker: &Broker) -> i64 {
    broker
        .controller
        .current_image()
        .broker_epoch(broker.config.node_id)
        .expect("broker 1 is self-registered")
}

/// Dispatches `body` as an `ANONYMOUS` principal that the default
/// `AllowAllAuthorizer` admits.
pub(super) async fn handle_allowed(
    broker: &Broker,
    req: AssignReplicasToDirsRequest,
) -> Result<AssignReplicasToDirsResponse, BrokerError> {
    krabka_macros::request_test_identity! {
        bind(user, address, ctx);
        user ["ANONYMOUS"];
        client ["assign-replicas-test"];
    };
    handle(broker, req, VERSION, &ctx).await
}
