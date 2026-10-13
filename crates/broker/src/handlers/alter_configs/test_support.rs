//! Request builders, metadata-image fixtures, and the live-broker harness that
//! the `AlterConfigs` tests share.
//!
//! The topic-record, broker-record, and end-to-end tests build the same
//! resource shapes and the same seeded images, so the fixtures live in one
//! module rather than being duplicated per test file.

use {
    super::{RESOURCE_TYPE_BROKER, RESOURCE_TYPE_TOPIC, handle},
    crate::{
        authorizer::Authorizer,
        handlers::test_support::metadata_topic_record,
        test_support::{
            broker_registration, start_broker_with_authorizer as start_broker, test_ctx,
        },
    },
    krabka_metadata::MetadataRecord,
    krabka_protocol::owned::{
        alter_configs_request::{AlterConfigsRequest, AlterConfigsResource, AlterableConfig},
        alter_configs_response::AlterConfigsResponse,
    },
    std::sync::Arc,
};

krabka_macros::wire_codec_helpers! {
    pub(super) test_context ["admin-client"];
}

pub(super) fn resource(resource_type: i8, resource_name: &str) -> AlterConfigsResource {
    scoped_resource(resource_type, resource_name, &[("retention.ms", "60000")])
}

/// A metadata image that holds one registered broker and nothing else.
pub(super) fn image_with_broker(node_id: u64) -> krabka_metadata::MetadataImage {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1BrokerRegistration(broker_registration(
        krabka_raft::NodeId(node_id),
    )));
    image
}

fn scoped_resource(
    resource_type: i8,
    resource_name: &str,
    configs: &[(&str, &str)],
) -> AlterConfigsResource {
    AlterConfigsResource {
        resource_type,
        resource_name: resource_name.into(),
        configs: configs
            .iter()
            .map(|(name, value)| AlterableConfig {
                name: (*name).into(),
                value: Some((*value).into()),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

pub(super) fn topic_resource(
    resource_name: &str,
    configs: &[(&str, &str)],
) -> AlterConfigsResource {
    scoped_resource(RESOURCE_TYPE_TOPIC, resource_name, configs)
}

pub(super) fn image_with_topic(name: &str) -> krabka_metadata::MetadataImage {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(metadata_topic_record(
        name,
        uuid::Uuid::nil(),
        1,
        1,
    )));
    image
}

config_image_fixture! {
    /// A metadata image that holds one topic and the override map it was created
    /// with. `image_with_topic` covers the topics whose overrides do not matter.
    pub(super) fn image_with_topic_config(name, overrides)
    from image_with_topic(name);
    V1TopicConfig(TopicConfigRecord { topic, overrides })
}

config_image_fixture! {
    /// A metadata image that holds one group and the override map it was
    /// configured with, for the same "does a replacement audit the keys it
    /// deletes by omission" tests `image_with_topic_config` supports.
    pub(super) fn image_with_group_config(group_id, overrides)
    from krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    V1GroupConfig(GroupConfigRecord { group_id, configs })
}

config_image_fixture! {
    /// A metadata image that holds one client-metrics subscription and the
    /// override map it was configured with.
    pub(super) fn image_with_client_metrics_config(name, overrides)
    from krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    V1ClientMetricsConfig(ClientMetricsConfigRecord { name, configs })
}

pub(super) fn group_resource(
    resource_name: &str,
    configs: &[(&str, &str)],
) -> AlterConfigsResource {
    scoped_resource(super::RESOURCE_TYPE_GROUP, resource_name, configs)
}

pub(super) fn client_metrics_resource(
    resource_name: &str,
    configs: &[(&str, &str)],
) -> AlterConfigsResource {
    scoped_resource(super::RESOURCE_TYPE_CLIENT_METRICS, resource_name, configs)
}

pub(super) fn broker_resource(
    resource_name: &str,
    configs: &[(&str, &str)],
) -> AlterConfigsResource {
    scoped_resource(RESOURCE_TYPE_BROKER, resource_name, configs)
}

pub(super) async fn drive_one(
    authorizer: Arc<dyn Authorizer>,
    resource: AlterConfigsResource,
) -> AlterConfigsResponse {
    drive_many(authorizer, vec![resource]).await
}

pub(super) async fn drive_many(
    authorizer: Arc<dyn Authorizer>,
    resources: Vec<AlterConfigsResource>,
) -> AlterConfigsResponse {
    let version = 2;
    let (broker_handle, _dir) = start_broker(authorizer).await;
    let broker = broker_handle.broker_arc_for_test();
    test_ctx!(ctx, "admin");
    let req = AlterConfigsRequest {
        resources,
        validate_only: false,
        ..Default::default()
    };
    let resp = handle(&broker, req, version, &ctx).await.expect("handle");
    broker_handle.shutdown().await;
    resp
}
