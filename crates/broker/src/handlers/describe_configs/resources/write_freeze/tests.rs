//! The `write.freeze` value a topic reports, and where in the response it
//! sits.

use assert2::check;
use krabka_metadata::{MetadataImage, MetadataRecord, PatternType, TopicConfigRecord};
use uuid::Uuid;

use crate::{
    config_keys,
    handlers::describe_configs::{
        resources::tests::{describe_topic, entry_named},
        test_support::TopicConfigSetup,
        wire::{CONFIG_SOURCE_DEFAULT, CONFIG_SOURCE_DYNAMIC_TOPIC},
    },
};

/// A metadata image whose freeze registry holds `entries`, and nothing else. A
/// freeze never reaches a topic-config record, so the image needs no topic and
/// no override map to answer the synthesised key.
fn image_with_freezes(entries: &[(&str, PatternType)]) -> MetadataImage {
    crate::test_support::frozen_topics_image(
        entries,
        crate::test_support::FrozenTopicsImageSetup {
            cluster: Uuid::nil(),
        },
    )
}

#[test]
fn topic_describe_synthesises_write_freeze_from_the_registry() {
    let image = image_with_freezes(&[
        ("orders", PatternType::Literal),
        ("tenant-a.", PatternType::Prefixed),
    ]);

    for (label, topic, value, config_source) in [
        (
            "a topic that a literal scope names",
            "orders",
            "frozen:literal:orders",
            CONFIG_SOURCE_DYNAMIC_TOPIC,
        ),
        (
            "a topic that a namespace prefix covers",
            "tenant-a.billing",
            "frozen:prefixed:tenant-a.",
            CONFIG_SOURCE_DYNAMIC_TOPIC,
        ),
        (
            "a topic that no scope covers, while the registry holds other entries",
            "tenant-b.orders",
            "false",
            CONFIG_SOURCE_DEFAULT,
        ),
        (
            "an internal topic, which is never freezable",
            "__consumer_offsets",
            "false",
            CONFIG_SOURCE_DEFAULT,
        ),
    ] {
        let result = describe_topic(&image, topic, None);
        let entry = entry_named(&result, config_keys::WRITE_FREEZE);
        check!(entry.value.as_deref() == Some(value), "{label}");
        check!(entry.config_source == config_source, "{label}");
        // The key is controller-managed, so an operator reading it through
        // `kafka-configs` must see that no alter can change it.
        check!(entry.read_only, "{label}");
    }
}

#[test]
fn topic_describe_reports_write_freeze_on_a_cluster_with_no_freeze() {
    // The key must not disappear when nothing is frozen. An absent key reads
    // the same as a broker without the feature.
    let image = MetadataImage::new(Uuid::nil());
    let result = describe_topic(&image, "orders", None);

    check!(
        entry_named(&result, config_keys::WRITE_FREEZE)
            .value
            .as_deref()
            == Some("false")
    );
}

#[test]
fn topic_describe_places_write_freeze_beside_the_stored_overrides() {
    let mut image = image_with_freezes(&[("orders", PatternType::Literal)]);
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "orders".into(),
        overrides: maplit::btreemap! {
        config_keys::CLEANUP_POLICY.to_string() => "compact".to_string(),
        config_keys::RETENTION_MS.to_string() => "60000".to_string()},
    }));

    let result = describe_topic(&image, "orders", None);
    let names: Vec<&str> = result.configs.iter().map(|c| c.name.as_str()).collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();

    check!(names == sorted, "the response is in key order");
    check!(names.contains(&config_keys::WRITE_FREEZE));
    check!(
        entry_named(&result, config_keys::CLEANUP_POLICY)
            .value
            .as_deref()
            == Some("compact")
    );
}

#[test]
fn topic_describe_applies_the_key_filter_to_the_synthesised_key() {
    let mut image = image_with_freezes(&[("orders", PatternType::Literal)]);
    crate::handlers::describe_configs::test_support::set_topic_config(
        &mut image,
        TopicConfigSetup::default(),
    );

    for (label, keys, expected) in [
        (
            "a filter that names the synthesised key alone",
            vec![config_keys::WRITE_FREEZE],
            vec![config_keys::WRITE_FREEZE],
        ),
        (
            "a filter that leaves the synthesised key out",
            vec![config_keys::RETENTION_MS],
            vec![config_keys::RETENTION_MS],
        ),
        (
            "a filter that names both",
            vec![config_keys::RETENTION_MS, config_keys::WRITE_FREEZE],
            vec![config_keys::RETENTION_MS, config_keys::WRITE_FREEZE],
        ),
        (
            "a filter that names a key no topic has",
            vec!["no.such.key"],
            Vec::new(),
        ),
    ] {
        let configuration_keys = Some(keys.iter().map(|key| (*key).to_string()).collect());
        let result = describe_topic(&image, "orders", configuration_keys);
        let names: Vec<&str> = result.configs.iter().map(|c| c.name.as_str()).collect();
        check!(names == expected, "{label}");
    }
}
