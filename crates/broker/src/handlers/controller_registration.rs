//! `ControllerRegistration` (`api_key=70`). KIP-919 controller registration.
//!
//! As Kafka's `ClusterControlManager.registerController` does, this registers
//! any controller id the request names (a KIP-853 controller joins the voter
//! set after it registers), refuses a `metadata.version` below 3.7-IV0 (level
//! 15) with `UNSUPPORTED_VERSION`, and writes `zkMigrationReady` false.

use std::collections::BTreeMap;

use krabka_metadata::{BrokerEndpoint, ControllerRegistrationRecord, MetadataRecord, NodeId};
use krabka_protocol::owned::{
    controller_registration_request::ControllerRegistrationRequest,
    controller_registration_response::ControllerRegistrationResponse,
};
use krabka_raft::RaftError;

use crate::codes;

context_handler! {
    ControllerRegistrationRequest => ControllerRegistrationResponse,
    (broker, req, _version, ctx),
    {
        let image = broker.controller.current_image();
        if let Some(code) = super::registration_listeners::registration_refusal(broker, &image, ctx) {
            let message = (code == codes::CLUSTER_AUTHORIZATION_FAILED)
                .then(|| "cluster action denied".into());
            return Ok(response(code, message));
        }
        // Kafka's `MetadataVersion.isControllerRegistrationSupported`. Before the
        // bootstrap records commit there is no finalized level, and Kafka's
        // `metadataVersionOrThrow` refuses the registration as well: a record
        // written now could precede a bootstrap level that does not support it.
        if image
            .finalized_metadata_version()
            .is_none_or(|level| level < krabka_metadata::metadata_version::ONLINE_DOWNGRADE_MIN_LEVEL)
        {
            return Ok(response(
                codes::UNSUPPORTED_VERSION,
                Some(
                    "The current MetadataVersion is too old to support controller registrations."
                        .into(),
                ),
            ));
        }

        let node_id = match u64::try_from(req.controller_id) {
            Ok(id) => NodeId(id),
            Err(_) => {
                return Ok(response(
                    codes::INVALID_REGISTRATION,
                    Some("controller id must be non-negative".into()),
                ));
            }
        };

        let endpoints = match decode_listeners(&req.listeners) {
            Ok(endpoints) => endpoints,
            Err(message) => return Ok(response(codes::INVALID_REGISTRATION, Some(message))),
        };
        let features = req
            .features
            .into_iter()
            .map(|feature| {
                (
                    feature.name,
                    (feature.min_supported_version, feature.max_supported_version),
                )
            })
            .collect::<BTreeMap<_, _>>();
        if features
            .iter()
            .any(|(name, (min, max))| name.is_empty() || min > max)
        {
            return Ok(response(
                codes::INVALID_REGISTRATION,
                Some("invalid controller feature range".into()),
            ));
        }

        let record = ControllerRegistrationRecord {
            node_id,
            incarnation_id: uuid::Uuid::from_bytes(req.incarnation_id.0),
            // ZooKeeper migration is gone. Kafka writes false whatever the request
            // says.
            zk_migration_ready: false,
            endpoints,
            features,
        };
        if image.controller(node_id) == Some(&record) {
            return Ok(success());
        }
        Ok(
            match broker
                .controller
                .submit_change(vec![MetadataRecord::V1ControllerRegistration(record)])
                .await
            {
                Ok(_) => success(),
                Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                    response(codes::NOT_CONTROLLER, None)
                }
                Err(RaftError::Metadata(error)) => {
                    response(codes::INVALID_REGISTRATION, Some(error.to_string()))
                }
                Err(error) => response(codes::UNKNOWN_SERVER_ERROR, Some(error.to_string())),
            },
        )
    }
}

fn decode_listeners(
    listeners: &[krabka_protocol::owned::controller_registration_request::Listener],
) -> Result<Vec<BrokerEndpoint>, String> {
    crate::handlers::registration_listeners::decode!(
        listeners,
        "controller registration has no listeners".into(),
        "invalid or duplicate controller listener".into(),
        "unknown controller listener security protocol".into()
    )
}

/// A registration the controller accepted.
///
/// Kafka's `ControllerApis.handleControllerRegistration` answers success with a
/// bare `ControllerRegistrationResponseData`, so `ErrorMessage` goes out as the
/// generated default: the empty string, not null.
fn success() -> ControllerRegistrationResponse {
    response(0, Some(String::new()))
}

fn response(error_code: i16, error_message: Option<String>) -> ControllerRegistrationResponse {
    ControllerRegistrationResponse {
        throttle_time_ms: 0,
        error_code,
        error_message,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    }
}

#[cfg(test)]
mod tests {
    use krabka_protocol::owned::controller_registration_request::Listener;
    use krabka_security::ListenerProtocol;

    use super::*;
    use crate::test_support::principal;

    crate::test_support::context_helper!(client_id = "controller");

    #[test]
    fn controller_listener_validation_is_strict() {
        let listener = Listener {
            name: "CONTROLLER".into(),
            host: "controller-1".into(),
            port: 9093,
            security_protocol: 0,
            ..Default::default()
        };
        assert2::assert!(decode_listeners(std::slice::from_ref(&listener)).is_ok());
        assert2::assert!(decode_listeners(&[listener.clone(), listener]).is_err());
    }

    /// A controller that is not a voter registers, with `zkMigrationReady`
    /// false whatever it sent.
    #[tokio::test]
    async fn a_controller_that_is_not_a_voter_registers() {
        use std::{net::SocketAddr, sync::Arc};

        broker_fixture!(
            (broker_handle, _dir, broker),
            crate::test_support::start_broker_with_authorizer(Arc::new(
                crate::authorizer::AllowAllAuthorizer,
            ))
        );
        let principal = principal("controller");
        let peer: SocketAddr = "127.0.0.1:9093".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let version = krabka_protocol::owned::controller_registration_request::MAX_VERSION;
        let listener = Listener {
            name: "CONTROLLER".into(),
            host: "controller-7".into(),
            port: 9093,
            security_protocol: 0,
            ..Default::default()
        };
        let req = ControllerRegistrationRequest {
            controller_id: 7,
            incarnation_id: krabka_protocol::primitives::uuid::Uuid([7; 16]),
            zk_migration_ready: true,
            listeners: vec![listener],
            ..Default::default()
        };

        let answer = handle(&broker, req, version, &ctx)
            .await
            .expect("an answer");

        assert2::check!(
            answer
                == unthrottled_wire!(ControllerRegistrationResponse {
                    error_code: 0,
                    error_message: Some(String::new()),
                })
        );
        assert2::check!(
            broker
                .controller
                .current_image()
                .controller(NodeId(7))
                .cloned()
                == Some(ControllerRegistrationRecord {
                    node_id: NodeId(7),
                    incarnation_id: uuid::Uuid::from_bytes([7; 16]),
                    zk_migration_ready: false,
                    endpoints: vec![BrokerEndpoint {
                        name: "CONTROLLER".into(),
                        host: "controller-7".into(),
                        port: 9093,
                        protocol: ListenerProtocol::Plaintext,
                    }],
                    features: BTreeMap::new(),
                })
        );
        broker_handle.shutdown().await;
    }
}
