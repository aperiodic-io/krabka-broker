//! `CreateTopics` (`api_key` 19), KIP-525: the created topic's effective
//! configuration travels back on the create response, so a client learns what
//! it created without a follow-up `DescribeConfigs`.
//!
//! Terraform's `kafka_topic`, Connect's `TopicAdmin.createOrFindTopics` and
//! Streams' `InternalTopicManager` all read
//! `createTopics(...).config(topic)`. What they must see there is the very
//! list `describeConfigs` answers for the same topic, which is what this test
//! pins.

use assert2::{assert, check};
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopicConfig, CreateTopicsRequest},
    describe_configs_request::DescribeConfigsRequest,
};

use crate::{RESOURCE_TYPE_TOPIC, support::configs::describe_resource};

/// `ConfigSource.DYNAMIC_TOPIC_CONFIG`, the source a value the create request
/// carried reports.
const DYNAMIC_TOPIC_CONFIG: i8 = 1;

/// One entry as both responses state it: the fields `CreatableTopicConfigs`
/// and `DescribeConfigsResourceResult` have in common, which is everything
/// KIP-525 puts on the create response.
type ConfigEntry = (String, Option<String>, i8, bool, bool);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_topics_returns_the_configs_describe_configs_reports() {
    let (_cluster, client) = crate::support::start_n_node_client(1, "admin-handlers-test").await;

    let create = CreateTopicsRequest {
        topics: vec![crate::support::topics::creatable_topic_with_configs(
            crate::support::topics::ConfiguredTopicSetup {
                name: "t-kip525".into(),
                configs: vec![CreatableTopicConfig {
                    name: "retention.ms".into(),
                    value: Some("60000".into()),
                    ..Default::default()
                }],
                ..Default::default()
            },
        )],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let created = client.send(create).await.expect("create_topics");

    let row = &created.topics[0];
    assert!(
        row.error_code == 0,
        "create_topics: {:?}",
        row.error_message
    );
    check!(row.topic_config_error_code == 0);
    check!(row.num_partitions == 1);
    check!(row.replication_factor == 1);
    let from_create: Vec<ConfigEntry> = row
        .configs
        .as_ref()
        .expect("KIP-525 configs list")
        .iter()
        .map(|entry| {
            (
                entry.name.clone(),
                entry.value.clone(),
                entry.config_source,
                entry.is_sensitive,
                entry.read_only,
            )
        })
        .collect();
    check!(
        !from_create.is_empty(),
        "the list carries the topic's values"
    );
    check!(
        from_create.contains(&(
            "retention.ms".to_string(),
            Some("60000".to_string()),
            DYNAMIC_TOPIC_CONFIG,
            false,
            false,
        )),
        "the request's own override, at DYNAMIC_TOPIC_CONFIG"
    );

    let described = client
        .send(DescribeConfigsRequest {
            resources: vec![describe_resource(RESOURCE_TYPE_TOPIC, "t-kip525", None)],
            ..Default::default()
        })
        .await
        .expect("describe_configs");

    let result = &described.results[0];
    assert!(
        result.error_code == 0,
        "describe_configs: {:?}",
        result.error_message
    );
    let from_describe: Vec<ConfigEntry> = result
        .configs
        .iter()
        .map(|entry| {
            (
                entry.name.clone(),
                entry.value.clone(),
                entry.config_source,
                entry.is_sensitive,
                entry.read_only,
            )
        })
        .collect();

    assert!(from_create == from_describe);
}
