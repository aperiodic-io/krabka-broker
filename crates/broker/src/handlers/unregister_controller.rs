//! `UnregisterController` (`api_key` 94, KIP-1312, Kafka trunk).
//!
//! An operator removes a controller from the voter set, shuts it down, and
//! then drops its `ControllerRegistration` with this RPC, as
//! `kafka-cluster unregister-controller` and `kafka-metadata-quorum
//! remove-controller --unregister` do. The controller writes an
//! `UnregisterControllerRecord`, and the image forgets the registration.
//!
//! Kafka trunk tags the request `broker` and `controller`, and `ApiKeys` marks
//! it forwardable. The controller listener answers it in place, as
//! `ControllerApis.handleUnregisterController` does. A broker listener
//! forwards it to the active controller in a KIP-590 `Envelope`, as
//! `KafkaApis` does with `forwardToController`. The forwarding lives in
//! `handlers::forward_to_controller`, shared with the other admin RPCs that
//! need the active controller.
//!
//! The checks run in trunk's order:
//!
//! 1. `Alter` on the cluster, or `CLUSTER_AUTHORIZATION_FAILED` (31).
//! 2. This node is the active controller, or `NOT_CONTROLLER` (41). This is
//!    `ControllerWriteEvent.run`.
//! 3. The id is not a current voter, or `INVALID_REQUEST` (42). This is
//!    `QuorumController.unregisterController`.
//! 4. `metadata.version` is at least `4.4-IV2`, or `UNSUPPORTED_VERSION` (35).
//! 5. The id has a registration, or `CONTROLLER_ID_NOT_REGISTERED` (136).
//!    Checks 4 and 5 are `ClusterControlManager.unregisterController`.
//!
//! Each refusal carries the message of the exception trunk throws, because
//! `UnregisterControllerRequest.getErrorResponse` copies `e.getMessage()`. A
//! success carries the generated empty-string `ErrorMessage`.

use {
    crate::{
        broker::Broker,
        codes,
        controller_admin::CONTROLLER_ADMIN_CONNECTION_ID,
        error::BrokerError,
        features::CONTROLLER_UNREGISTRATION_MIN_LEVEL,
        handlers::{
            cluster_alter_denied, encode_response,
            forward_to_controller::{to_active_controller, wrong_controller_message},
        },
    },
    bytes::Bytes,
    krabka_metadata::{MetadataRecord, NodeId, UnregisterControllerRecord},
    krabka_protocol::owned::{
        unregister_controller_request::{self, UnregisterControllerRequest},
        unregister_controller_response::UnregisterControllerResponse,
    },
    krabka_raft::RaftError,
};

#[cfg(test)]
mod tests;

/// The message of the `ClusterAuthorizationException` trunk's
/// `AuthHelper.authorizeClusterOperation` throws. Kafka names the JVM
/// `toString` of the channel request, and krabka names the API in its place.
const CLUSTER_ALTER_DENIED_MESSAGE: &str = "Request UnregisterController needs ALTER permission.";

/// Trunk's `ClusterControlManager.unregisterController` message below
/// `4.4-IV2`.
const UNSUPPORTED_METADATA_VERSION_MESSAGE: &str =
    "The current MetadataVersion is too old to support controller unregistration.";

wire_handler! {
    async (broker, version, req_bytes, ctx), {
        let req = crate::handlers::decode_request::<UnregisterControllerRequest>(req_bytes, version)?;
        let image = broker.controller.current_image();

        // A broker listener forwards the request untouched, and the controller
        // authorizes the principal the `Envelope` names. A node that is itself
        // the active controller has nowhere to forward to and answers in place.
        if ctx.connection_id != CONTROLLER_ADMIN_CONNECTION_ID
            && let Some(answer) = to_active_controller(
                broker,
                unregister_controller_request::API_KEY,
                req_bytes,
                version,
                ctx,
                |error_code, message| encode(version, error_code, message),
            )
            .await
        {
            return answer;
        }
        if cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
            return encode(
                version,
                codes::CLUSTER_AUTHORIZATION_FAILED,
                Some(CLUSTER_ALTER_DENIED_MESSAGE),
            );
        }

        let leader = *broker.controller.watch_leader().borrow();
        if leader != Some(broker.config.node_id) {
            return encode(
                version,
                codes::NOT_CONTROLLER,
                Some(&wrong_controller_message(leader)),
            );
        }

        let Unregistration {
            error_code,
            message,
        } = match refusal(&image, req.controller_id) {
            Some(refused) => refused,
            None => submit(broker, req.controller_id).await,
        };
        if error_code == codes::NONE {
            crate::handlers::audit_admin_success(
                broker.audit_log.as_ref(),
                ctx,
                "UnregisterController",
                vec![crate::handlers::audit_resource(
                    "Controller",
                    req.controller_id.to_string(),
                )],
            );
        }
        encode(version, error_code, message.as_deref())
    }
}

/// The error code and message that an `UnregisterController` answers with.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Unregistration {
    error_code: i16,
    message: Option<String>,
}

/// The refusal trunk's `QuorumController.unregisterController` and
/// `ClusterControlManager.unregisterController` answer for `controller_id`
/// against `image`, in their order, or `None` when the record may be written.
fn refusal(image: &krabka_metadata::MetadataImage, controller_id: i32) -> Option<Unregistration> {
    let node_id = u64::try_from(controller_id).ok().map(NodeId);
    if node_id.is_some_and(|id| image.voters().contains(id)) {
        return Some(Unregistration {
            error_code: codes::INVALID_REQUEST,
            message: Some(format!(
                "Cannot unregister controller {controller_id} because it is part of the voter set."
            )),
        });
    }
    if image
        .finalized_metadata_version()
        .is_none_or(|level| level < CONTROLLER_UNREGISTRATION_MIN_LEVEL)
    {
        return Some(Unregistration {
            error_code: codes::UNSUPPORTED_VERSION,
            message: Some(UNSUPPORTED_METADATA_VERSION_MESSAGE.to_owned()),
        });
    }
    if node_id.is_none_or(|id| image.controller(id).is_none()) {
        return Some(not_registered(controller_id));
    }
    None
}

/// Writes the `UnregisterControllerRecord` and reports how it went.
async fn submit(broker: &Broker, controller_id: i32) -> Unregistration {
    let Ok(node_id) = u64::try_from(controller_id).map(NodeId) else {
        return not_registered(controller_id);
    };
    let record = MetadataRecord::V1UnregisterController(UnregisterControllerRecord { node_id });
    match broker.controller.submit_change(vec![record]).await {
        Ok(_) => Unregistration {
            error_code: codes::NONE,
            message: Some(String::new()),
        },
        Err(RaftError::NotLeader { current_leader }) => Unregistration {
            error_code: codes::NOT_CONTROLLER,
            message: Some(wrong_controller_message(current_leader)),
        },
        Err(RaftError::LeaderUnknown | RaftError::UncommittedTail) => Unregistration {
            error_code: codes::NOT_CONTROLLER,
            message: Some(wrong_controller_message(None)),
        },
        // The image refuses the record only when the registration went away
        // between the check above and the append.
        Err(RaftError::Metadata(_)) => not_registered(controller_id),
        Err(error) => Unregistration {
            error_code: codes::UNKNOWN_SERVER_ERROR,
            message: Some(error.to_string()),
        },
    }
}

/// Trunk's `ControllerIdNotRegisteredException` for `controller_id`.
fn not_registered(controller_id: i32) -> Unregistration {
    Unregistration {
        error_code: codes::CONTROLLER_ID_NOT_REGISTERED,
        message: Some(format!(
            "Controller ID {controller_id} is not currently registered."
        )),
    }
}

fn response(error_code: i16, error_message: Option<&str>) -> UnregisterControllerResponse {
    UnregisterControllerResponse {
        error_code,
        error_message: error_message.map(str::to_owned),
        ..Default::default()
    }
}

fn encode(
    version: i16,
    error_code: i16,
    error_message: Option<&str>,
) -> Result<Bytes, BrokerError> {
    encode_response(&response(error_code, error_message), version)
}
