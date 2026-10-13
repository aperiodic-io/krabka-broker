//! `DescribeQuorum` (`api_key=55`, KIP-595). It returns the raft-quorum state
//! for the cluster-metadata topic.
//!
//! Krabka's `KRaft` setup runs one raft log, the controller quorum that
//! `controller_quorum_voters` configures, and applies committed records to
//! `MetadataImage`. Clients, such as the JVM `kafka-metadata-quorum
//! --describe` admin tool, ask for `__cluster_metadata` partition 0.
//!
//! Kafka's `KafkaApis` forwards `DescribeQuorum` from the broker listener to
//! the active controller unconditionally (`forwardToController`), so this
//! handler does the same: [`krabka_raft::ControllerHandle::forward_raw`]
//! (reached through [`crate::metadata_source::MetadataSource::forward_raw`])
//! sends the request on to the active controller whenever this node itself
//! is not the leader, whether it is a broker-only observer or a
//! combined/controller node. It goes wrapped in a KIP-590 `Envelope`
//! (`forward` submodule) carrying THIS caller's own principal and address,
//! not this node's inter-broker identity, so the leader authorizes and
//! audits the caller that actually asked (review of #1034). A node that IS
//! the active controller answers locally, from
//! [`krabka_raft::ControllerHandle::quorum_snapshot`], with the same
//! [`krabka_raft::describe_quorum`] builder the controller listener uses for
//! a request that arrives there directly (#814, #1034) -- one
//! implementation on both listeners.
//!
//! The envelope wrap/unwrap lives in `forward`. This file holds the wire
//! entry point: the shared `Cluster` `Describe` gate, the forward, and the
//! local answer.

use krabka_protocol::owned::{
    describe_quorum_request::{API_KEY as DESCRIBE_QUORUM_API_KEY, DescribeQuorumRequest},
    describe_quorum_response::DescribeQuorumResponse,
};

mod forward;

use crate::{
    codes,
    handlers::{ErrorResponse as _, cluster_describe_denied},
};

wire_handler! {
    async (broker, version, req_bytes, ctx), {
        let image = broker.controller.current_image();

        // Whole-request Cluster Describe gate. DescribeQuorum is
        // cluster-wide raft introspection — same gate as DescribeCluster.
        if cluster_describe_denied(broker.config.authorizer.as_ref(), &image, ctx) {
            let resp = top_level_error_response(codes::CLUSTER_AUTHORIZATION_FAILED);
            return crate::handlers::encode_response(&resp, version);
        }

        // Forward to the active controller whenever this node is not it (#392,
        // #1034): a broker-only observer always forwards; a combined/controller
        // node forwards only while it is not the leader. Wrapped in a KIP-590
        // `Envelope` carrying this caller's own identity
        // (`forward_to_controller::build`), not this node's inter-broker one --
        // see the module doc and `forward`'s.
        let envelope_body = crate::handlers::forward_to_controller::build(
            broker,
            DESCRIBE_QUORUM_API_KEY,
            req_bytes,
            version,
            ctx,
        )?;
        if let Some(forwarded) = broker
            .controller
            .forward_raw(
                forward::ENVELOPE_API_KEY,
                forward::ENVELOPE_VERSION,
                envelope_body,
            )
            .await
        {
            return match forwarded {
                Ok(envelope_response) => forward::unwrap_response(broker, &envelope_response, version),
                // No leader known yet (startup or an election in progress), an
                // unresolvable voter address, or the dial/round-trip itself
                // failed: a Kafka client expects a typed
                // `NOT_LEADER_OR_FOLLOWER` for this transient case, not a
                // dropped connection (review of #1034) -- the generic registry
                // dispatch loop has no response shape to build for a bare
                // `Err(BrokerError)` here and just closes the connection.
                Err(_raft_error) => crate::handlers::encode_response(
                    &top_level_error_response(codes::NOT_LEADER_OR_FOLLOWER),
                    version,
                ),
            };
        }

        let req = crate::handlers::decode_request::<DescribeQuorumRequest>(req_bytes, version)?;

        // Reaching here means `forward_raw` answered `None`: this node holds a
        // quorum snapshot and is the active controller, the only case a
        // `MetadataSource` implementer declines to forward on.
        let Some(quorum) = broker.controller.quorum_snapshot() else {
            let resp = top_level_error_response(codes::NOT_LEADER_OR_FOLLOWER);
            return crate::handlers::encode_response(&resp, version);
        };

        let resp = krabka_raft::describe_quorum(&req, &quorum);
        crate::handlers::encode_response(&resp, version)
    }
}

/// Kafka's `Errors.CLUSTER_AUTHORIZATION_FAILED.message()`.
const CLUSTER_AUTHORIZATION_FAILED_MESSAGE: &str = "Cluster authorization failed.";

/// Kafka's `Errors.NOT_LEADER_OR_FOLLOWER.message()`.
const NOT_LEADER_OR_FOLLOWER_MESSAGE: &str = "For requests intended only for the leader, this \
     error indicates that the broker is not the current leader. For requests intended for any \
     replica, this error indicates that the broker is not a replica of the topic partition.";

/// A whole-request `DescribeQuorum` refusal, in the shape of Kafka's
/// `DescribeQuorumRequest.getTopLevelErrorResponse`: the top-level
/// `error_code` and that error's own `Errors.message()`, with no topics.
///
/// Kafka answers every whole-request failure this way, from the
/// `getErrorResponse` that `ControllerApis` and the broker's forwarding path
/// build, so the message is set rather than left at the field's empty
/// default. Only the codes this handler emits carry a message here; any
/// other code gets Kafka's empty default.
pub(super) fn top_level_error_response(error_code: i16) -> DescribeQuorumResponse {
    let error_message = match error_code {
        codes::CLUSTER_AUTHORIZATION_FAILED => CLUSTER_AUTHORIZATION_FAILED_MESSAGE,
        codes::NOT_LEADER_OR_FOLLOWER => NOT_LEADER_OR_FOLLOWER_MESSAGE,
        _ => "",
    };
    DescribeQuorumResponse::error(error_code, Some(error_message.to_owned()))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_protocol::{Decode as _, Encode as _, owned::describe_quorum_response},
    };

    /// Each whole-request refusal carries Kafka's `Errors.message()` for
    /// its code, as `DescribeQuorumRequest.getTopLevelErrorResponse` sets.
    #[test]
    fn top_level_error_response_carries_kafkas_message() {
        for (error_code, message) in [
            (
                codes::CLUSTER_AUTHORIZATION_FAILED,
                "Cluster authorization failed.",
            ),
            (
                codes::NOT_LEADER_OR_FOLLOWER,
                "For requests intended only for the leader, this error indicates that the \
                 broker is not the current leader. For requests intended for any replica, this \
                 error indicates that the broker is not a replica of the topic partition.",
            ),
        ] {
            assert!(
                top_level_error_response(error_code)
                    == DescribeQuorumResponse {
                        error_code,
                        error_message: Some(message.to_owned()),
                        ..Default::default()
                    }
            );
        }
    }

    /// The refusal survives the wire at v2+, where `ErrorMessage` exists,
    /// with the message intact rather than null.
    #[test]
    fn top_level_error_response_round_trips_its_message() {
        let resp = top_level_error_response(codes::CLUSTER_AUTHORIZATION_FAILED);
        let version = describe_quorum_response::MAX_VERSION;
        let mut out = bytes::BytesMut::new();
        resp.encode(&mut out, version).expect("encode");
        let mut cur: &[u8] = &out;
        let decoded = DescribeQuorumResponse::decode(&mut cur, version).expect("decode");
        assert!(decoded == resp);
    }
}
