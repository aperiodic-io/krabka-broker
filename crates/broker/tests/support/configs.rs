//! Configuration and feature update request fixtures.

use krabka_protocol::owned::{
    describe_configs_request::DescribeConfigsResource,
    incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
    },
    update_features_request::FeatureUpdateKey,
};

#[derive(Clone, Copy, Default)]
pub enum ConfigOperation {
    #[default]
    Set,
    Delete,
    Append,
    Subtract,
    Invalid(ConfigOperationCode),
}

#[derive(Clone, Copy)]
pub struct ConfigOperationCode(pub i8);

impl ConfigOperation {
    pub const fn from_wire(code: i8) -> Self {
        match code {
            0 => Self::Set,
            1 => Self::Delete,
            2 => Self::Append,
            3 => Self::Subtract,
            _ => Self::Invalid(ConfigOperationCode(code)),
        }
    }
    pub const fn wire_code(self) -> i8 {
        match self {
            Self::Set => 0,
            Self::Delete => 1,
            Self::Append => 2,
            Self::Subtract => 3,
            Self::Invalid(code) => code.0,
        }
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct TopicConfigMutationSetup<'a> {
    #[default("orders")]
    pub topic: &'a str,
    #[default("cleanup.policy")]
    pub name: &'a str,
    pub operation: ConfigOperation,
    pub value: Option<&'a str>,
}

pub fn incremental_config(
    name: impl Into<String>,
    value: Option<String>,
    config_operation: ConfigOperation,
) -> AlterableConfig {
    AlterableConfig {
        name: name.into(),
        value,
        config_operation: config_operation.wire_code(),
        ..Default::default()
    }
}

pub fn incremental_resource(
    resource_type: i8,
    resource_name: impl Into<String>,
    configs: Vec<AlterableConfig>,
) -> AlterConfigsResource {
    AlterConfigsResource {
        resource_type,
        resource_name: resource_name.into(),
        configs,
        ..Default::default()
    }
}

#[derive(Clone, Copy, Default)]
pub enum ConfigMutation {
    #[default]
    Apply,
    Validate,
}

#[derive(Default)]
pub struct IncrementalConfigsSetup {
    pub resources: Vec<AlterConfigsResource>,
    pub mutation: ConfigMutation,
}

pub fn incremental_request(setup: IncrementalConfigsSetup) -> IncrementalAlterConfigsRequest {
    IncrementalAlterConfigsRequest {
        resources: setup.resources,
        validate_only: matches!(setup.mutation, ConfigMutation::Validate),
        ..Default::default()
    }
}

pub fn describe_resource(
    resource_type: i8,
    resource_name: impl Into<String>,
    configuration_keys: Option<Vec<String>>,
) -> DescribeConfigsResource {
    DescribeConfigsResource {
        resource_type,
        resource_name: resource_name.into(),
        configuration_keys,
        ..Default::default()
    }
}

pub fn feature_update(
    feature: impl Into<String>,
    max_version_level: i16,
    upgrade_type: i8,
) -> FeatureUpdateKey {
    FeatureUpdateKey {
        feature: feature.into(),
        max_version_level,
        upgrade_type,
        ..Default::default()
    }
}

/// Read a named topic config through the wire API, including its error checks.
///
/// # Panics
/// Panics if the request fails or the requested topic config is absent.
pub async fn topic_config(
    client: &krabka_client_core::Client,
    topic: &str,
    name: &str,
) -> krabka_protocol::owned::describe_configs_response::DescribeConfigsResourceResult {
    let response = client
        .send(
            krabka_protocol::owned::describe_configs_request::DescribeConfigsRequest {
                resources: vec![describe_resource(
                    2,
                    topic.to_owned(),
                    Some(vec![name.to_owned()]),
                )],
                include_synonyms: false,
                include_documentation: false,
                ..Default::default()
            },
        )
        .await
        .expect("DescribeConfigs");
    let result = &response.results[0];
    assert2::assert!(
        result.error_code == krabka_broker::codes::NONE,
        "DescribeConfigs({topic}): {result:?}"
    );
    result
        .configs
        .iter()
        .find(|entry| entry.name == name)
        .cloned()
        .unwrap_or_else(|| panic!("no {name} entry for {topic}"))
}

/// Return one incremental topic-config mutation's code and message.
///
/// # Panics
/// Panics if the request fails or the broker omits the topic response.
pub async fn alter_topic_config(
    client: &krabka_client_core::Client,
    setup: TopicConfigMutationSetup<'_>,
) -> (i16, Option<String>) {
    let TopicConfigMutationSetup {
        topic,
        name,
        operation,
        value,
    } = setup;
    let response = client
        .send(incremental_request(IncrementalConfigsSetup {
            resources: vec![incremental_resource(
                2,
                topic.to_owned(),
                vec![incremental_config(
                    name.to_owned(),
                    value.map(ToOwned::to_owned),
                    operation,
                )],
            )],
            ..Default::default()
        }))
        .await
        .expect("IncrementalAlterConfigs");
    let row = &response.responses[0];
    (row.error_code, row.error_message.clone())
}

/// One legacy `AlterConfigs` key, including deletions expressed as a nullable value.
pub fn legacy_config(
    name: String,
    value: Option<String>,
) -> krabka_protocol::owned::alter_configs_request::AlterableConfig {
    krabka_protocol::owned::alter_configs_request::AlterableConfig {
        name,
        value,
        ..Default::default()
    }
}

/// A legacy configuration resource with caller-owned type, name and ordered keys.
pub fn legacy_resource(
    resource_type: i8,
    resource_name: String,
    configs: Vec<krabka_protocol::owned::alter_configs_request::AlterableConfig>,
) -> krabka_protocol::owned::alter_configs_request::AlterConfigsResource {
    krabka_protocol::owned::alter_configs_request::AlterConfigsResource {
        resource_type,
        resource_name,
        configs,
        ..Default::default()
    }
}

/// Preserve legacy replacement semantics and the explicit validation mode.
pub fn legacy_request(
    resources: Vec<krabka_protocol::owned::alter_configs_request::AlterConfigsResource>,
    validate_only: bool,
) -> krabka_protocol::owned::alter_configs_request::AlterConfigsRequest {
    krabka_protocol::owned::alter_configs_request::AlterConfigsRequest {
        resources,
        validate_only,
        ..Default::default()
    }
}

/// A controller supporting every stable metadata version, without endpoints.
pub fn controller_registration(
    node_id: krabka_metadata::NodeId,
) -> krabka_metadata::MetadataRecord {
    krabka_metadata::MetadataRecord::V1ControllerRegistration(
        krabka_metadata::ControllerRegistrationRecord {
            node_id,
            incarnation_id: uuid::Uuid::from_u128(u128::from(node_id.0)),
            zk_migration_ready: false,
            endpoints: Vec::new(),
            features: std::collections::BTreeMap::from([(
                "metadata.version".to_owned(),
                (7, krabka_metadata::metadata_version::METADATA_VERSION_MAX),
            )]),
        },
    )
}

/// A valid client-metrics subscription input shared by telemetry and config round trips.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct ClientMetricsSubscriptionSetup<'a> {
    #[default("sub-a")]
    pub name: &'a str,
    #[default("*")]
    pub metrics: &'a str,
    #[default("60000")]
    pub interval: &'a str,
}

pub fn client_metrics_subscription(
    setup: ClientMetricsSubscriptionSetup<'_>,
) -> IncrementalAlterConfigsRequest {
    incremental_request(IncrementalConfigsSetup {
        resources: vec![incremental_resource(
            16,
            setup.name,
            vec![
                incremental_config(
                    "metrics",
                    Some(setup.metrics.to_owned()),
                    ConfigOperation::Set,
                ),
                incremental_config(
                    "interval.ms",
                    Some(setup.interval.to_owned()),
                    ConfigOperation::Set,
                ),
            ],
        )],
        ..Default::default()
    })
}
