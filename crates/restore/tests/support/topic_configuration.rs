//! Wire assertions for a topic configuration recovered from an archive.

use assert2::{assert, check};
use krabka_client_core::Client;
use krabka_protocol::owned::describe_configs_request::{
    DescribeConfigsRequest, DescribeConfigsResource,
};

pub(crate) async fn check_topic_configuration(
    client: &Client,
    topic: &str,
    name: &str,
    value: &str,
) {
    let configs = client
        .send(DescribeConfigsRequest {
            resources: vec![DescribeConfigsResource {
                resource_type: 2,
                resource_name: topic.to_owned(),
                configuration_keys: None,
                ..Default::default()
            }],
            include_synonyms: false,
            include_documentation: false,
            ..Default::default()
        })
        .await
        .expect("DescribeConfigs");
    let result = configs.results.first().expect("one config result");
    assert!(result.error_code == 0, "DescribeConfigs failed: {result:?}");
    check!(
        result
            .configs
            .iter()
            .any(|config| { config.name == name && config.value.as_deref() == Some(value) }),
        "the metadata checkpoint did not restore {name}={value}: {result:?}"
    );
}
