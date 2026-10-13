// rustc 1.95 clippy::pedantic ICEs on this file (an upstream bug in
// clippy's body-analysis pass). Disable pedantic locally; the rest of
// the workspace still enforces the full pedantic gate.

//! Broker-side integration test for KIP-714 `CLIENT_METRICS` config round-trip.
//!
//! It drives `IncrementalAlterConfigs` → `DescribeConfigs` →
//! `ListConfigResources` for a `CLIENT_METRICS` subscription over the real
//! in-process broker, and it asserts the full operator-facing round-trip.

use {
    crate::support::{configs::client_metrics_subscription, start_n_node_client},
    assert2::{assert, check},
};
mod support;

use krabka_protocol::owned::{
    describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
    describe_configs_response::DescribeConfigsResponse,
    incremental_alter_configs_response::IncrementalAlterConfigsResponse,
    list_config_resources_request::ListConfigResourcesRequest,
    list_config_resources_response::ListConfigResourcesResponse,
};

/// Kafka resource type id for `CLIENT_METRICS` (KIP-714).
const RESOURCE_TYPE_CLIENT_METRICS: i8 = 16;

/// `config_source` `CLIENT_METRICS_CONFIG` = 7.
const CONFIG_SOURCE_CLIENT_METRICS: i8 = 7;

/// `config_source` `DEFAULT_CONFIG` = 5.
const CONFIG_SOURCE_DEFAULT: i8 = 5;

// ── helpers ───────────────────────────────────────────────────────────────────

fn assert_describe_response(response: &DescribeConfigsResponse) {
    assert!(
        response.results.len() == 1,
        "expected exactly one describe result"
    );
    let result = &response.results[0];
    check!(result.error_code == 0, "DescribeConfigs failed: {result:?}");
    check!(result.resource_type == RESOURCE_TYPE_CLIENT_METRICS);
    check!(result.resource_name == "sub-a");

    let config = |name: &str| {
        result
            .configs
            .iter()
            .find(|config| config.name == name)
            .unwrap_or_else(|| panic!("missing {name} config in {:?}", result.configs))
    };
    let metrics = config("metrics");
    assert!(metrics.value.as_deref() == Some("org.apache.kafka.consumer."));
    assert!(metrics.config_source == CONFIG_SOURCE_CLIENT_METRICS);
    let interval = config("interval.ms");
    assert!(interval.value.as_deref() == Some("60000"));
    assert!(interval.config_source == CONFIG_SOURCE_CLIENT_METRICS);
    assert!(config("match").config_source == CONFIG_SOURCE_DEFAULT);
}

fn assert_list_response(response: &ListConfigResourcesResponse) {
    assert!(
        response.error_code == 0,
        "ListConfigResources failed: {response:?}"
    );
    assert!(
        response.config_resources.iter().any(|resource| {
            resource.resource_type == RESOURCE_TYPE_CLIENT_METRICS
                && resource.resource_name == "sub-a"
        }),
        "CLIENT_METRICS sub-a missing from {response:?}"
    );
}

// ── test ──────────────────────────────────────────────────────────────────────

/// CONFIG ROUND-TRIP:
///
/// 1. `IncrementalAlterConfigs` for `CLIENT_METRICS` "sub-a" with
///    `metrics=org.apache.kafka.consumer.` and `interval.ms=60000` (SET ops).
///    Asserts per-resource `error_code` == NONE (0).
///
/// 2. `DescribeConfigs` for `CLIENT_METRICS` "sub-a". Asserts:
///    - `metrics` value == "org.apache.kafka.consumer.", `config_source` == 7.
///    - `interval.ms` value == "60000", `config_source` == 7.
///    - the `match` entry is present at its default, `config_source` == 5.
///
/// 3. `ListConfigResources` (v1, `resource_types`=[16]). Asserts that the result
///    contains a `ConfigResource` with `resource_type` 16 and `resource_name`
///    "sub-a".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_metrics_config_alter_describe_list_round_trip() {
    let (_cluster, client) = start_n_node_client(1, "client-metrics-config-test").await;

    // ── Step 1: IncrementalAlterConfigs ──────────────────────────────────────
    let alter_req = client_metrics_subscription()
        .metrics("org.apache.kafka.consumer.")
        .call();

    let alter_resp: IncrementalAlterConfigsResponse = client
        .send(alter_req)
        .await
        .expect("IncrementalAlterConfigs");

    assert!(
        alter_resp.responses.len() == 1,
        "expected exactly one resource response, got {}",
        alter_resp.responses.len()
    );
    let resource_resp = &alter_resp.responses[0];
    assert!(
        resource_resp.error_code == 0,
        "IncrementalAlterConfigs CLIENT_METRICS must succeed; error_code={} message={:?}",
        resource_resp.error_code,
        resource_resp.error_message
    );

    // ── Step 2: DescribeConfigs ───────────────────────────────────────────────
    let describe_req = DescribeConfigsRequest {
        resources: vec![DescribeConfigsResource {
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".to_string(),
            configuration_keys: None, // all keys
            ..Default::default()
        }],
        include_synonyms: false,
        include_documentation: false,
        ..Default::default()
    };

    let describe_resp: DescribeConfigsResponse =
        client.send(describe_req).await.expect("DescribeConfigs");

    assert_describe_response(&describe_resp);

    // ── Step 3: ListConfigResources ───────────────────────────────────────────
    let list_req = ListConfigResourcesRequest {
        resource_types: vec![RESOURCE_TYPE_CLIENT_METRICS],
        ..Default::default()
    };

    let list_resp: ListConfigResourcesResponse =
        client.send(list_req).await.expect("ListConfigResources");

    assert_list_response(&list_resp);
}
