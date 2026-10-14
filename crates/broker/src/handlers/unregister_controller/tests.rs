#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    super::*,
    crate::{
        controller_admin::CONTROLLER_ADMIN_CONNECTION_ID,
        handlers::{RequestContext, update_features::answer},
        test_support::{ControllerPeerAllowed, GrantsInPrincipalName, start_broker_with},
    },
    assert2::assert,
    krabka_metadata::{
        ControllerRegistrationRecord, FeatureLevelRecord, MetadataImage, MetadataRecord, NodeId,
        voters::{KRaftVersionRange, Voter, VoterSet},
    },
    krabka_protocol::owned::unregister_controller_response::MAX_VERSION,
    std::sync::Arc,
};

krabka_macros::wire_codec_helpers! {
    encode_request(UnregisterControllerRequest);
    decode_response(UnregisterControllerResponse);
    version [MAX_VERSION];
}

fn registration(node_id: u64) -> MetadataRecord {
    MetadataRecord::V1ControllerRegistration(ControllerRegistrationRecord {
        node_id: NodeId(node_id),
        incarnation_id: uuid::Uuid::from_u128(u128::from(node_id)),
        zk_migration_ready: false,
        endpoints: Vec::new(),
        features: supported_metadata_versions(),
    })
}

/// A controller that supports every `metadata.version`, so finalizing one
/// with `UpdateFeatures` passes Kafka's per-controller range check.
fn supported_metadata_versions() -> std::collections::BTreeMap<String, (i16, i16)> {
    std::collections::BTreeMap::from([(
        krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.to_owned(),
        (
            krabka_metadata::metadata_version::METADATA_VERSION_MIN,
            krabka_metadata::metadata_version::METADATA_VERSION_MAX,
        ),
    )])
}

fn metadata_version(level: i16) -> MetadataRecord {
    MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
        level,
    })
}

/// An image with voter 1, registered controllers 1 and 7, at `level`.
fn image_at(level: i16) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Voters(krabka_metadata::VotersRecord {
        voters: VoterSet::from_voters([Voter {
            id: NodeId(1),
            directory_id: uuid::Uuid::nil(),
            endpoints: Vec::new(),
            kraft_version: KRaftVersionRange::default(),
        }]),
    }));
    for record in [metadata_version(level), registration(1), registration(7)] {
        image.apply(&record);
    }
    image
}

fn expected(error_code: i16, message: &str) -> Unregistration {
    Unregistration {
        error_code,
        message: Some(message.to_owned()),
    }
}

/// Trunk's checks in trunk's order: a voter is refused before the
/// `metadata.version` gate, and the gate before the registration lookup.
#[test]
fn refusals_follow_trunks_order() {
    let unsupported = Some(expected(
        codes::UNSUPPORTED_VERSION,
        "The current MetadataVersion is too old to support controller unregistration.",
    ));
    let voter = Some(expected(
        codes::INVALID_REQUEST,
        "Cannot unregister controller 1 because it is part of the voter set.",
    ));
    let cases = [
        ("voter at 4.4-IV1", 32, 1, voter.clone()),
        ("voter at 4.4-IV2", 33, 1, voter),
        ("registered at 4.4-IV1", 32, 7, unsupported.clone()),
        ("unknown at 4.4-IV1", 32, 9, unsupported),
        ("registered at 4.4-IV2", 33, 7, None),
        (
            "unknown at 4.4-IV2",
            33,
            9,
            Some(expected(
                codes::CONTROLLER_ID_NOT_REGISTERED,
                "Controller ID 9 is not currently registered.",
            )),
        ),
        (
            "negative at 4.4-IV2",
            33,
            -1,
            Some(expected(
                codes::CONTROLLER_ID_NOT_REGISTERED,
                "Controller ID -1 is not currently registered.",
            )),
        ),
    ];
    for (case, level, controller_id, want) in cases {
        assert!(refusal(&image_at(level), controller_id) == want, "{case}");
    }
}

#[test]
fn wrong_controller_message_names_the_leader_when_there_is_one() {
    assert!(
        wrong_controller_message(Some(NodeId(3))) == "The active controller appears to be node 3."
    );
    assert!(wrong_controller_message(None) == "No controller appears to be active.");
}

/// Finalizes `metadata.version` at `level` through the `UpdateFeatures`
/// handler, as `kafka-features upgrade` does.
async fn finalize_metadata_version(broker: &Broker, level: i16) {
    use krabka_protocol::owned::update_features_request::{
        FeatureUpdateKey, MAX_VERSION as UPDATE_FEATURES_VERSION, UpdateFeaturesRequest,
    };

    krabka_macros::request_test_identity! {
        bind(principal, peer, ctx);
        user ["Cluster:Alter"];
        client ["kafka-features"];
    };
    let answer = answer(
        broker,
        UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
                max_version_level: level,
                upgrade_type: 1,
                ..Default::default()
            }],
            ..Default::default()
        },
        UPDATE_FEATURES_VERSION,
        &ctx,
    )
    .await;
    assert!(answer.error_code == codes::NONE, "{answer:?}");
    assert!(
        broker
            .controller
            .current_image()
            .finalized_metadata_version()
            == Some(level)
    );
}

async fn submit(broker: &Broker, records: Vec<MetadataRecord>) {
    broker
        .controller
        .submit_change(records)
        .await
        .expect("submit metadata records");
}

async fn send(
    broker: &Broker,
    principal: &str,
    controller_id: i32,
) -> UnregisterControllerResponse {
    krabka_macros::request_test_identity! {
        bind(principal, peer);
        user [principal];
    };
    let ctx = RequestContext::new(
        &principal,
        &peer,
        "kafka-cluster",
        CONTROLLER_ADMIN_CONNECTION_ID,
        false,
        "CONTROLLER",
    );
    let body = encode_request(&UnregisterControllerRequest {
        controller_id,
        ..Default::default()
    });
    decode_response(
        &handle(broker, MAX_VERSION, &body, &ctx)
            .await
            .expect("an answer"),
    )
}

/// The issue's two rows, a registered and an unknown controller, with the
/// `metadata.version` gate on either side of 4.4-IV2 and the `Alter` gate.
/// Both levels are finalized through `UpdateFeatures`.
#[tokio::test]
async fn handle_unregisters_a_registered_controller_as_trunk_does() {
    let HandlerStartedBroker {
        broker: handle,
        dir: _dir,
    } = start_broker_with(|cfg| {
        // Trunk's 4.4-IV2 is past 4.3.1's latest production level, so the
        // node has to support unstable feature levels to finalize it.
        cfg.features.unstable_feature_versions = krabka_raft::UnstableFeatureVersions::Enabled;
        cfg.authorizer = Arc::new(ControllerPeerAllowed(GrantsInPrincipalName));
    })
    .await;
    let broker = handle.broker_arc_for_test();
    let operator = "Cluster:Alter";
    submit(&broker, vec![registration(7)]).await;
    finalize_metadata_version(&broker, 32).await;

    let cases = [
        (
            "no Alter grant",
            "none",
            7,
            response(
                codes::CLUSTER_AUTHORIZATION_FAILED,
                Some("Request UnregisterController needs ALTER permission."),
            ),
        ),
        (
            "below 4.4-IV2",
            operator,
            7,
            response(
                codes::UNSUPPORTED_VERSION,
                Some(
                    "The current MetadataVersion is too old to support controller unregistration.",
                ),
            ),
        ),
    ];
    for (case, principal, controller_id, want) in cases {
        assert!(
            send(&broker, principal, controller_id).await == want,
            "{case}"
        );
    }
    assert!(
        broker
            .controller
            .current_image()
            .controller(NodeId(7))
            .is_some()
    );

    finalize_metadata_version(&broker, CONTROLLER_UNREGISTRATION_MIN_LEVEL).await;
    let cases = [
        (
            "the active controller, a voter",
            1,
            response(
                codes::INVALID_REQUEST,
                Some("Cannot unregister controller 1 because it is part of the voter set."),
            ),
        ),
        (
            "an unknown controller",
            9,
            response(
                codes::CONTROLLER_ID_NOT_REGISTERED,
                Some("Controller ID 9 is not currently registered."),
            ),
        ),
        (
            "a registered controller",
            7,
            response(codes::NONE, Some("")),
        ),
        (
            "the same controller again",
            7,
            response(
                codes::CONTROLLER_ID_NOT_REGISTERED,
                Some("Controller ID 7 is not currently registered."),
            ),
        ),
    ];
    for (case, controller_id, want) in cases {
        assert!(
            send(&broker, operator, controller_id).await == want,
            "{case}"
        );
    }
    assert!(
        broker
            .controller
            .current_image()
            .controller(NodeId(7))
            .is_none()
    );
    handle.shutdown().await;
}
