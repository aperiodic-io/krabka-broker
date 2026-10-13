//! Fixture builders shared by the unit tests of the `IncrementalAlterConfigs`
//! submodules. They seed a `MetadataImage` with a registered broker or with a
//! topic and its overrides, and they build the request-side resource and
//! per-key config structures that each scope handler consumes.

use {
    super::{OP_DELETE, OP_SET, RESOURCE_TYPE_BROKER, RESOURCE_TYPE_TOPIC},
    crate::{handlers::test_support::metadata_topic_record, test_support::broker_registration},
    krabka_metadata::{MetadataImage, MetadataRecord, NodeId, TopicConfigRecord},
    krabka_protocol::owned::{
        incremental_alter_configs_request::AlterConfigsResource,
        incremental_alter_configs_response::AlterConfigsResourceResponse,
    },
    std::collections::BTreeMap,
};
pub(super) use {
    assert2::{assert, check},
    krabka_protocol::owned::incremental_alter_configs_request::AlterableConfig,
};

pub(super) fn make_image_with_broker(node_id: NodeId) -> MetadataImage {
    let mut img = MetadataImage::new(uuid::Uuid::nil());
    img.apply(&MetadataRecord::V1BrokerRegistration(broker_registration(
        node_id,
    )));
    img
}

pub(super) fn make_resource(name: &str, configs: Vec<AlterableConfig>) -> AlterConfigsResource {
    make_scoped_resource(RESOURCE_TYPE_BROKER, name, configs)
}

pub(super) fn make_set_cfg(key: &str, value: &str) -> AlterableConfig {
    AlterableConfig {
        name: key.into(),
        config_operation: OP_SET,
        value: Some(value.into()),
        ..Default::default()
    }
}

pub(super) fn make_del_cfg(key: &str) -> AlterableConfig {
    AlterableConfig {
        name: key.into(),
        config_operation: OP_DELETE,
        value: None,
        ..Default::default()
    }
}

pub(super) fn make_topic_resource(
    name: &str,
    configs: Vec<AlterableConfig>,
) -> AlterConfigsResource {
    make_scoped_resource(RESOURCE_TYPE_TOPIC, name, configs)
}

/// One stored topic override: a config key and its value.
#[derive(Clone, Copy, Debug)]
pub(super) struct StoredOverride<'a> {
    key: &'a str,
    value: &'a str,
}

impl<'a> StoredOverride<'a> {
    pub(super) fn new(key: &'a str, value: &'a str) -> Self { Self { key, value } }
}

/// The override map that stores `overrides`.
pub(super) fn overrides_map(overrides: &[StoredOverride<'_>]) -> BTreeMap<String, String> {
    overrides
        .iter()
        .map(|stored| (stored.key.to_owned(), stored.value.to_owned()))
        .collect()
}

/// What an `IncrementalAlterConfigs` scope handler answered, and the records
/// it staged.
pub(super) struct Handled {
    pub(super) response: AlterConfigsResourceResponse,
    pub(super) records: Vec<MetadataRecord>,
}

pub(super) fn image_with_topic_config(
    name: &str,
    overrides: &[StoredOverride<'_>],
) -> MetadataImage {
    let mut img = MetadataImage::new(uuid::Uuid::nil());
    img.apply(&MetadataRecord::V1Topic(metadata_topic_record(
        name,
        uuid::Uuid::nil(),
        1,
        1,
    )));
    img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: name.into(),
        overrides: overrides_map(overrides),
    }));
    img
}

/// A request resource with the supplied Kafka scope, name and operations.
pub(super) fn make_scoped_resource(
    resource_type: i8,
    name: &str,
    configs: Vec<AlterableConfig>,
) -> AlterConfigsResource {
    AlterConfigsResource {
        resource_type,
        resource_name: name.into(),
        configs,
        ..Default::default()
    }
}
