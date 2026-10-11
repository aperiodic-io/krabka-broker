//! The Kafka registration RPC served by the controller listener itself.
//!
//! This root routes an `api_key` to the handler that answers it, and owns the
//! Kafka error codes it replies with, the declared API table, the leadership
//! guard, and the mapping from a [`RaftError`] to the code a client acts on.
//! The RPC has its own submodule, with the listener grammar in `listeners` and
//! the response encoder in `response`.
//!
//! `BrokerRegistration` and `BrokerHeartbeat` are deliberately absent. Their
//! answers are not a function of the metadata image alone. Registration refuses
//! a new incarnation only while the previous one holds a heartbeat session, and
//! it withdraws the ISR and ELR seats of a broker that cannot prove a clean
//! restart. The heartbeat drives the controller's heartbeat registry, the
//! KIP-112 offline-dir failover, and the controlled-shutdown drain. All of that
//! lives in the broker crate, so both reach the controller listener through the
//! KIP-919 Admin router like the other broker-owned APIs. There is exactly one
//! implementation of each, not a second one here that skips the bookkeeping.

use bytes::Bytes;
use krabka_protocol::owned::controller_registration_request;

mod controller;
mod listeners;
mod response;

use self::controller::controller_registration;
use crate::{RaftError, kraft::KraftController};

const SUCCESS: i16 = 0;
const UNKNOWN_SERVER_ERROR: i16 = -1;
const UNSUPPORTED_VERSION: i16 = 35;
const NOT_CONTROLLER: i16 = 41;
const INVALID_REGISTRATION: i16 = 119;

/// The lifecycle API keys this module answers. The versions they are served at
/// are declared once, with the rest of the listener's surface, in
/// [`super::api_versions::table`].
pub(super) const SUPPORTED_APIS: [i16; 1] = [controller_registration_request::API_KEY];

pub(super) fn is_controller_api(api_key: i16) -> bool {
    SUPPORTED_APIS.contains(&api_key)
}

pub(super) async fn dispatch(
    api_key: i16,
    version: i16,
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    match api_key {
        controller_registration_request::API_KEY => {
            controller_registration(version, body, engine).await
        }
        _ => Err(RaftError::Protocol(
            krabka_protocol::ProtocolError::InvalidValue("unknown controller lifecycle API"),
        )),
    }
}

fn is_leader(engine: &KraftController) -> bool {
    engine.watch_leader().borrow().as_ref() == Some(&engine.node_id())
}

fn raft_error_code(error: &RaftError) -> i16 {
    match error {
        RaftError::NotLeader { .. } | RaftError::LeaderUnknown => NOT_CONTROLLER,
        RaftError::Metadata(_) | RaftError::ChangeRejected(_) => INVALID_REGISTRATION,
        _ => UNKNOWN_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// The controller answers the lifecycle APIs it declares and nothing else.
    #[test]
    fn only_the_declared_apis_are_controller_apis() {
        for key in SUPPORTED_APIS {
            check!(is_controller_api(key), "declared api {key}");
        }
        // A key nothing declares: Produce is a broker API, never a controller one.
        check!(!is_controller_api(0), "Produce is not a controller api");
        check!(!is_controller_api(i16::MAX));
        // `BrokerRegistration` and `BrokerHeartbeat` are answered by the
        // broker's handlers through the Admin router, so this table must not
        // claim them.
        check!(
            !is_controller_api(krabka_protocol::owned::broker_registration_request::API_KEY),
            "BrokerRegistration belongs to the Admin router"
        );
        check!(
            !is_controller_api(krabka_protocol::owned::broker_heartbeat_request::API_KEY),
            "BrokerHeartbeat belongs to the Admin router"
        );
    }

    /// Each raft failure maps to the error code a Kafka client acts on: a
    /// leadership problem tells it to look elsewhere, a rejected registration
    /// tells it not to retry unchanged.
    #[test]
    fn raft_errors_map_to_the_client_visible_code() {
        check!(raft_error_code(&RaftError::LeaderUnknown) == NOT_CONTROLLER);
        check!(raft_error_code(&RaftError::ChangeRejected("no".into())) == INVALID_REGISTRATION,);
        // Anything else is not something the client can act on specifically.
        check!(raft_error_code(&RaftError::Shutdown) == UNKNOWN_SERVER_ERROR);
        check!(UNKNOWN_SERVER_ERROR == -1);
    }

    /// The declared keys are the generated ones. The versions these are served
    /// at are asserted with the rest of the advertised table, in
    /// [`super::super::api_versions::table`].
    #[test]
    fn lifecycle_api_keys_match_generated_schemas() {
        assert2::assert!(SUPPORTED_APIS == [70]);
    }

    #[tokio::test]
    async fn controller_registration_dispatch_and_error_paths() {
        use krabka_protocol::{
            Decode, Encode,
            owned::{
                controller_registration_request::{
                    self, ControllerRegistrationRequest, Listener as WireListener,
                },
                controller_registration_response::ControllerRegistrationResponse,
            },
        };

        use crate::{
            NodeId,
            server::test_support::{single_voter_engine, test_engine_with_voters, wait_for_leader},
        };

        let reg_req = |id: i32| {
            let req = ControllerRegistrationRequest {
                controller_id: id,
                incarnation_id: krabka_protocol::primitives::uuid::Uuid(
                    *uuid::Uuid::from_u128(1).as_bytes(),
                ),
                zk_migration_ready: false,
                listeners: vec![WireListener {
                    name: "CONTROLLER".into(),
                    host: "controller-1".into(),
                    port: 9093,
                    security_protocol: 0,
                    ..Default::default()
                }],
                features: vec![],
                ..Default::default()
            };
            let mut buf = bytes::BytesMut::new();
            req.encode(&mut buf, 0).unwrap();
            buf.freeze()
        };

        // Unknown API returns protocol error
        let (engine_non_leader, _dir1) = test_engine_with_voters(1, std::iter::empty());
        let err_resp = super::dispatch(999, 0, &[], &engine_non_leader).await;
        assert2::assert!(err_resp.is_err());

        // 1. Non-leader engine returns NOT_CONTROLLER (41)
        assert2::assert!(!is_leader(&engine_non_leader));
        let resp_bytes = super::dispatch(
            controller_registration_request::API_KEY,
            0,
            &reg_req(1),
            &engine_non_leader,
        )
        .await
        .expect("dispatch");
        let resp = ControllerRegistrationResponse::decode(&mut resp_bytes.as_ref(), 0).unwrap();
        assert2::assert!(resp.error_code == NOT_CONTROLLER);

        // 2. Leader engine
        let (engine_leader, _dir2) = single_voter_engine();
        wait_for_leader(&engine_leader).await;
        assert2::assert!(is_leader(&engine_leader));
        engine_leader
            .submit_change(vec![krabka_metadata::MetadataRecord::V1FeatureLevel(
                krabka_metadata::FeatureLevelRecord {
                    name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
                    level: 15,
                },
            )])
            .await
            .expect("finalize metadata.version");

        // A controller id that is not a voter registers too: Kafka's
        // `ClusterControlManager.registerController` does not require one, so
        // a KIP-853 observer controller can register before it joins the
        // voter set.
        let resp_bytes2 = super::dispatch(
            controller_registration_request::API_KEY,
            0,
            &reg_req(99),
            &engine_leader,
        )
        .await
        .expect("dispatch");
        let resp2 = ControllerRegistrationResponse::decode(&mut resp_bytes2.as_ref(), 0).unwrap();
        assert2::assert!(resp2.error_code == SUCCESS);

        // Valid controller ID in voters succeeds (0)
        let resp_bytes3 = super::dispatch(
            controller_registration_request::API_KEY,
            0,
            &reg_req(1),
            &engine_leader,
        )
        .await
        .expect("dispatch");
        let resp3 = ControllerRegistrationResponse::decode(&mut resp_bytes3.as_ref(), 0).unwrap();
        assert2::assert!(resp3.error_code == SUCCESS);
        assert2::assert!(
            engine_leader
                .current_image()
                .controller(NodeId(1))
                .is_some()
        );

        // Duplicate registration succeeds without error (0)
        let resp_bytes4 = super::dispatch(
            controller_registration_request::API_KEY,
            0,
            &reg_req(1),
            &engine_leader,
        )
        .await
        .expect("dispatch");
        let resp4 = ControllerRegistrationResponse::decode(&mut resp_bytes4.as_ref(), 0).unwrap();
        assert2::assert!(resp4.error_code == SUCCESS);
    }
}
