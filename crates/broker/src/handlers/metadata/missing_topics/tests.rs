//! Kafka's `DefaultAutoTopicCreationManager` for a `Metadata` request that
//! asks to auto-create a missing coordinator topic.

use {
    super::missing_topic_rows,
    crate::{
        codes,
        coordinator::bootstrap::{OFFSETS_TOPIC, offsets_topic_configs},
        test_support::start_broker_no_audit_with,
    },
    assert2::{assert, check},
    krabka_protocol::{
        owned::metadata_response::MetadataResponseTopic, primitives::uuid::Uuid as WireUuid,
    },
    std::{collections::BTreeMap, time::Duration},
};

fn row(error_code: i16, name: &str, is_internal: bool) -> MetadataResponseTopic {
    MetadataResponseTopic {
        error_code,
        name: Some(name.to_owned()),
        topic_id: WireUuid::ZERO,
        is_internal,
        ..Default::default()
    }
}

/// `creatableTopic` gives `__consumer_offsets` its configured partition count,
/// replication factor and topic configs, not the broker defaults, and
/// `filterCreatableTopics` skips it while its creation is in flight. The
/// uncreatable rows come first: an invalid name answers 17, and a name in
/// flight answers 3. `createTopics` answers 3 for the name that it sends.
#[tokio::test]
async fn a_coordinator_topic_is_created_with_its_configured_shape_unless_in_flight() {
    let (handle, _dir) = start_broker_no_audit_with(|config| {
        config.num_partitions = 1;
        config.offsets_topic_num_partitions = 7;
    })
    .await;
    let broker = handle.broker_arc_for_test();
    krabka_macros::request_test_identity! {
        bind(user, address, ctx);
        user ["alice"];
        client ["metadata-client"];
    };

    assert!(broker.auto_topic_creation.hold_for_test(OFFSETS_TOPIC));
    let skipped = missing_topic_rows(&broker, &ctx, &["bad name", OFFSETS_TOPIC], true);
    check!(
        skipped
            == vec![
                row(codes::INVALID_TOPIC_EXCEPTION, "bad name", false),
                row(codes::UNKNOWN_TOPIC_OR_PARTITION, OFFSETS_TOPIC, true),
            ]
    );
    check!(broker.auto_topic_creation.started() == 0);
    broker.auto_topic_creation.release_for_test(OFFSETS_TOPIC);

    let created = missing_topic_rows(&broker, &ctx, &[OFFSETS_TOPIC], true);
    check!(created == vec![row(codes::UNKNOWN_TOPIC_OR_PARTITION, OFFSETS_TOPIC, true)]);
    check!(broker.auto_topic_creation.started() == 1);
    tokio::time::timeout(Duration::from_secs(30), async {
        while broker.auto_topic_creation.is_in_flight(OFFSETS_TOPIC) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the creation ends");
    let image = broker.controller.current_image();
    check!(image.topic_partition_count(OFFSETS_TOPIC) == 7);
    let configs: BTreeMap<String, String> =
        offsets_topic_configs(&broker.config).into_iter().collect();
    check!(image.topic_config(OFFSETS_TOPIC) == Some(&configs));
    handle.shutdown().await;
}
