//! The `ControllerRegistration` API (KIP-919): records a controller's
//! endpoints and supported feature ranges in the metadata log.
//!
//! Kafka's `ClusterControlManager.registerController` registers any controller
//! id the request names. A KIP-853 controller starts as an observer and joins
//! the voter set later, so the id need not be a voter. The registration needs
//! `metadata.version` 3.7-IV0 (level 15) or later, and the record always says
//! `zkMigrationReady` false.

use {
    super::{
        INVALID_REGISTRATION, NOT_CONTROLLER, SUCCESS, UNSUPPORTED_VERSION, is_leader,
        listeners::decode_controller_listeners, raft_error_code,
        response::controller_registration_response,
    },
    crate::{RaftError, kraft::KraftController},
    bytes::Bytes,
    krabka_metadata::{ControllerRegistrationRecord, MetadataRecord, NodeId},
    krabka_protocol::{
        Decode, owned::controller_registration_request::ControllerRegistrationRequest,
    },
    std::collections::BTreeMap,
};

/// `MetadataVersion.IBP_3_7_IV0`, the first level that supports controller
/// registrations.
const CONTROLLER_REGISTRATION_MIN_LEVEL: i16 = 15;

pub(super) async fn controller_registration(
    version: i16,
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    let mut body = body;
    let request = ControllerRegistrationRequest::decode(&mut body, version)?;
    if !is_leader(engine) {
        return controller_registration_response(version, NOT_CONTROLLER, None);
    }
    // Kafka's `MetadataVersion.isControllerRegistrationSupported`. Before the
    // bootstrap records commit there is no finalized level, and Kafka's
    // `metadataVersionOrThrow` refuses the registration as well: a record
    // written now could precede a bootstrap level that does not support it.
    if engine
        .current_image()
        .finalized_metadata_version()
        .is_none_or(|level| level < CONTROLLER_REGISTRATION_MIN_LEVEL)
    {
        return controller_registration_response(
            version,
            UNSUPPORTED_VERSION,
            Some(
                "The current MetadataVersion is too old to support controller registrations."
                    .into(),
            ),
        );
    }
    let node_id = match u64::try_from(request.controller_id) {
        Ok(id) => NodeId(id),
        Err(_) => {
            return controller_registration_response(
                version,
                INVALID_REGISTRATION,
                Some("controller id must be non-negative".into()),
            );
        }
    };
    let endpoints = match decode_controller_listeners(&request.listeners) {
        Ok(endpoints) => endpoints,
        Err(message) => {
            return controller_registration_response(version, INVALID_REGISTRATION, Some(message));
        }
    };
    let features = match decode_controller_features(&request) {
        Ok(features) => features,
        Err(message) => {
            return controller_registration_response(version, INVALID_REGISTRATION, Some(message));
        }
    };
    let record = ControllerRegistrationRecord {
        node_id,
        incarnation_id: uuid::Uuid::from_bytes(request.incarnation_id.0),
        // ZooKeeper migration is gone. Kafka writes false whatever the request
        // says.
        zk_migration_ready: false,
        endpoints,
        features,
    };
    if engine.current_image().controller(node_id) == Some(&record) {
        // Kafka's `ControllerApis.handleControllerRegistration` answers a
        // success with a bare response, whose `ErrorMessage` keeps the
        // generated default: the empty string.
        return controller_registration_response(version, SUCCESS, Some(String::new()));
    }
    let result = engine
        .submit_change(vec![MetadataRecord::V1ControllerRegistration(record)])
        .await;
    match result {
        Ok(_) => controller_registration_response(version, SUCCESS, Some(String::new())),
        Err(error) => controller_registration_response(
            version,
            raft_error_code(&error),
            Some(error.to_string()),
        ),
    }
}

krabka_macros::registration_feature_projection!(
    registration_features,
    ::krabka_protocol::owned::controller_registration_request::Feature
);

fn decode_controller_features(
    request: &ControllerRegistrationRequest,
) -> Result<BTreeMap<String, (i16, i16)>, String> {
    let features = registration_features(&request.features);
    if features
        .iter()
        .any(|(name, (min, max))| name.is_empty() || min > max)
    {
        return Err("invalid controller feature range".into());
    }
    Ok(features)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check, krabka_protocol::owned::controller_registration_request};

    fn feature(name: &str, min: i16, max: i16) -> controller_registration_request::Feature {
        controller_registration_request::Feature {
            name: name.to_owned(),
            min_supported_version: min,
            max_supported_version: max,
            ..Default::default()
        }
    }

    fn request_with(
        features: Vec<controller_registration_request::Feature>,
    ) -> ControllerRegistrationRequest {
        ControllerRegistrationRequest {
            features,
            ..Default::default()
        }
    }

    /// A controller's advertised feature ranges are taken as given only when
    /// every one of them is named and non-inverted.
    ///
    /// A blank name collides with any other blank name in the map, and a range
    /// whose minimum exceeds its maximum supports nothing -- accepting either
    /// puts a value into the controller's view of the cluster that no version
    /// negotiation can satisfy.
    #[test]
    fn controller_feature_ranges_must_be_named_and_not_inverted() {
        // The features the controller registers with, and whether they are accepted.
        struct Case {
            what: &'static str,
            features: Vec<controller_registration_request::Feature>,
            accepted: bool,
        }
        let cases = vec![
            Case {
                what: "no features at all",
                features: vec![],
                accepted: true,
            },
            Case {
                what: "one ordinary range",
                features: vec![feature("kraft.version", 0, 1)],
                accepted: true,
            },
            Case {
                what: "a range that is a single point",
                features: vec![feature("metadata.version", 7, 7)],
                accepted: true,
            },
            Case {
                what: "several ordinary ranges",
                features: vec![
                    feature("kraft.version", 0, 1),
                    feature("group.version", 0, 1),
                ],
                accepted: true,
            },
            Case {
                what: "a nameless feature",
                features: vec![feature("", 0, 1)],
                accepted: false,
            },
            Case {
                what: "an inverted range",
                features: vec![feature("kraft.version", 2, 1)],
                accepted: false,
            },
            Case {
                what: "one good range and one inverted",
                features: vec![
                    feature("kraft.version", 0, 1),
                    feature("group.version", 5, 4),
                ],
                accepted: false,
            },
        ];
        for Case {
            what,
            features,
            accepted,
        } in cases
        {
            let request = request_with(features);
            let got = decode_controller_features(&request);
            check!(got.is_ok() == accepted, "{what}: {got:?}");
        }
    }

    /// The decoded map carries each feature's range under its own name.
    #[test]
    fn decoded_controller_features_keep_their_ranges() {
        let request = request_with(vec![
            feature("kraft.version", 0, 1),
            feature("metadata.version", 7, 25),
        ]);
        let decoded = decode_controller_features(&request).expect("valid ranges");
        check!(decoded.len() == 2);
        check!(decoded.get("kraft.version") == Some(&(0, 1)));
        check!(decoded.get("metadata.version") == Some(&(7, 25)));
    }

    /// One row per registration, each on a fresh single-voter controller
    /// (node 1) finalized at `level`: the whole response, and the record the
    /// image holds afterwards.
    #[tokio::test]
    async fn any_controller_registers_from_metadata_version_15() {
        use {
            crate::server::test_support::{single_voter_engine, wait_for_leader},
            krabka_protocol::{
                Encode as _,
                owned::controller_registration_response::ControllerRegistrationResponse,
            },
        };

        let version = controller_registration_request::MAX_VERSION;
        let incarnation = uuid::Uuid::from_u128(0xC0);
        let listener = controller_registration_request::Listener {
            name: "CONTROLLER".into(),
            host: "controller-7".into(),
            port: 9093,
            security_protocol: 0,
            ..Default::default()
        };
        let expected_record = |id: u64| ControllerRegistrationRecord {
            node_id: NodeId(id),
            incarnation_id: incarnation,
            zk_migration_ready: false,
            endpoints: vec![krabka_metadata::BrokerEndpoint {
                name: "CONTROLLER".into(),
                host: "controller-7".into(),
                port: 9093,
                protocol: krabka_security::ListenerProtocol::Plaintext,
            }],
            features: BTreeMap::from([("kraft.version".to_owned(), (0, 1))]),
        };
        let too_old = ControllerRegistrationResponse {
            error_code: UNSUPPORTED_VERSION,
            error_message: Some(
                "The current MetadataVersion is too old to support controller registrations."
                    .into(),
            ),
            ..Default::default()
        };

        // (label, controller id, metadata.version, zkMigrationReady sent,
        // response, record afterwards)
        let rows = [
            (
                "a voter",
                1,
                Some(15),
                false,
                ControllerRegistrationResponse::default(),
                Some(expected_record(1)),
            ),
            (
                "a controller that is not a voter",
                7,
                Some(15),
                false,
                ControllerRegistrationResponse::default(),
                Some(expected_record(7)),
            ),
            (
                "zkMigrationReady is stored as false",
                7,
                Some(25),
                true,
                ControllerRegistrationResponse::default(),
                Some(expected_record(7)),
            ),
            (
                "metadata.version 14",
                7,
                Some(14),
                false,
                too_old.clone(),
                None,
            ),
            (
                "metadata.version 14, a voter",
                1,
                Some(14),
                false,
                too_old.clone(),
                None,
            ),
            (
                "no finalized metadata.version",
                7,
                None,
                false,
                too_old,
                None,
            ),
        ];
        for (label, controller_id, level, zk_migration_ready, expected, record) in rows {
            let (engine, _dir) = single_voter_engine();
            wait_for_leader(&engine).await;
            if let Some(level) = level {
                engine
                    .submit_change(vec![MetadataRecord::V1FeatureLevel(
                        krabka_metadata::FeatureLevelRecord {
                            name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE
                                .into(),
                            level,
                        },
                    )])
                    .await
                    .expect("finalize metadata.version");
            }
            let request = ControllerRegistrationRequest {
                controller_id,
                incarnation_id: krabka_protocol::primitives::uuid::Uuid(*incarnation.as_bytes()),
                zk_migration_ready,
                listeners: vec![listener.clone()],
                features: vec![feature("kraft.version", 0, 1)],
                ..Default::default()
            };
            let mut body = bytes::BytesMut::new();
            request.encode(&mut body, version).expect("encode");

            let answer = controller_registration(version, &body, &engine)
                .await
                .expect("an answer");

            check!(
                ControllerRegistrationResponse::decode(&mut &answer[..], version).expect("decode")
                    == expected,
                "{label}"
            );
            let id = NodeId(u64::try_from(controller_id).expect("non-negative id"));
            check!(
                engine.current_image().controller(id).cloned() == record,
                "{label}"
            );
            engine.shutdown().await;
        }
    }
}
