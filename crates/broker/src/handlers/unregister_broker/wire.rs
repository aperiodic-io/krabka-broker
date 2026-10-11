//! The wrong-controller `UnregisterBrokerResponse` refusal.
//!
//! KIP-631 gives the response no per-broker rows, so a success and a refusal
//! differ only in the top-level error code and the optional message, which
//! [`ErrorResponse::error`] builds. Neither throttles.

use krabka_metadata::NodeId;
use krabka_protocol::owned::unregister_broker_response::UnregisterBrokerResponse;

use crate::{
    codes::NOT_CONTROLLER,
    handlers::{ErrorResponse, forward_to_controller::wrong_controller_message},
};

/// The refusal of a node that is not the active controller, or `None` when
/// `node` leads. `leader` is the node that this one believes leads.
///
/// Kafka's `ControllerWriteEvent.run` throws the wrong-controller exception
/// before it reads any state, and `UnregisterBrokerRequest.getErrorResponse`
/// copies its code and message.
pub(super) fn not_controller_refusal(
    leader: Option<NodeId>,
    node: NodeId,
) -> Option<UnregisterBrokerResponse> {
    (leader != Some(node)).then(|| {
        UnregisterBrokerResponse::error(NOT_CONTROLLER, Some(wrong_controller_message(leader)))
    })
}
