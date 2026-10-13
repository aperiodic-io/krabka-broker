//! The config surface of the role: `broker.witness` is controller-managed, so
//! `DescribeConfigs` reports it read-only and `IncrementalAlterConfigs` refuses
//! to change it with `INVALID_CONFIG`.
//!
//! This is the only test in the suite that needs no topic and no traffic, and
//! the only one that speaks the config APIs, so it carries their request shapes
//! on its own.

use {
    crate::{
        BROKER_WITNESS, CONFIG_SOURCE_DYNAMIC_BROKER, CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
        RESOURCE_TYPE_BROKER, SITE_A, STRETCH_PREFERRED_LEADER_SITE, WITNESS_ID, cluster_lock,
        support::configs::{
            ConfigOperation, describe_resource, incremental_config, incremental_request,
            incremental_resource,
        },
        witness_cluster::{client_at, shutdown, start_stretch_cluster},
    },
    assert2::check,
    krabka_broker::codes,
    krabka_protocol::owned::{
        describe_configs_request::DescribeConfigsRequest,
        describe_configs_response::DescribeConfigsResourceResult,
    },
};

/// `DescribeConfigsResponse.ConfigType::BOOLEAN`, the byte the JVM
/// `AdminClient` parses `broker.witness` with.
const CONFIG_TYPE_BOOLEAN: i8 = 1;

/// `DescribeConfigsResponse.ConfigType::STRING`, for the rack name in
/// `stretch.preferred.leader.site`.
const CONFIG_TYPE_STRING: i8 = 2;

/// `broker.witness` is controller-managed: it is published for the witness
/// node, `DescribeConfigs` reports it read-only next to the cluster-default
/// `stretch.preferred.leader.site`, and `IncrementalAlterConfigs` refuses to
/// change it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn witness_role_is_a_read_only_broker_config() {
    let _guard = cluster_lock().lock().await;
    let cluster = start_stretch_cluster().await;

    // Ask the witness itself, the way `kafka-configs --entity-type brokers
    // --entity-name 3 --describe` does.
    let witness = client_at(&cluster[2].1.listen_addr.to_string()).await;
    let described = witness
        .send(DescribeConfigsRequest {
            resources: vec![describe_resource(
                RESOURCE_TYPE_BROKER,
                WITNESS_ID.to_string(),
                None,
            )],
            include_synonyms: false,
            include_documentation: false,
            ..Default::default()
        })
        .await
        .expect("DescribeConfigs for the witness broker");
    let result = &described.results[0];
    check!(result.error_code == codes::NONE, "DescribeConfigs succeeds");

    let witness_entry = result
        .configs
        .iter()
        .find(|entry| entry.name == BROKER_WITNESS)
        .cloned();
    check!(
        witness_entry
            == Some(DescribeConfigsResourceResult {
                name: BROKER_WITNESS.into(),
                value: Some("true".into()),
                read_only: true,
                config_source: CONFIG_SOURCE_DYNAMIC_BROKER,
                config_type: CONFIG_TYPE_BOOLEAN,
                // Not requested, so null, as Kafka sends it.
                documentation: None,
                ..Default::default()
            }),
        "broker.witness is reported, read-only, and typed"
    );
    let site_entry = result
        .configs
        .iter()
        .find(|entry| entry.name == STRETCH_PREFERRED_LEADER_SITE)
        .cloned();
    check!(
        site_entry
            == Some(DescribeConfigsResourceResult {
                name: STRETCH_PREFERRED_LEADER_SITE.into(),
                value: Some(SITE_A.into()),
                read_only: true,
                config_source: CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                config_type: CONFIG_TYPE_STRING,
                // Not requested, so null, as Kafka sends it.
                documentation: None,
                ..Default::default()
            }),
        "the preferred leader site is a read-only cluster default"
    );

    let altered = witness
        .send(
            incremental_request()
                .resources(vec![incremental_resource(
                    RESOURCE_TYPE_BROKER,
                    WITNESS_ID.to_string(),
                    vec![incremental_config(
                        BROKER_WITNESS,
                        Some("false".into()),
                        ConfigOperation::Set,
                    )],
                )])
                .call(),
        )
        .await
        .expect("IncrementalAlterConfigs round-trip");
    check!(
        altered.responses[0].error_code == codes::INVALID_CONFIG,
        "an operator cannot turn the witness role off through the config API: {:?}",
        altered.responses[0].error_message
    );

    shutdown(cluster).await;
}
