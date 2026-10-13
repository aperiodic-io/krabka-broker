//! End-to-end `BROKER_LOGGER` (resource type 8) round-trip over the wire.
//!
//! `kafka-configs --entity-type broker-loggers --alter` is how an operator
//! raises a broker's log level without restarting it. This suite drives that
//! exchange against a real in-process broker: `IncrementalAlterConfigs` sets
//! a level, `DescribeConfigs` reads it back, `ListConfigResources` lists the
//! resource, and a `tracing` event proves the filter actually moved — the
//! whole point of the feature is the events, not the config entry.

use {
    crate::support::{
        client::connect_owned,
        configs::{
            ConfigOperation, describe_resource, incremental_config, incremental_request,
            incremental_resource,
        },
    },
    assert2::{assert, check},
};
mod support;

use {
    krabka_protocol::owned::{
        describe_configs_request::DescribeConfigsRequest,
        describe_configs_response::DescribeConfigsResponse,
        incremental_alter_configs_response::IncrementalAlterConfigsResponse,
        list_config_resources_request::ListConfigResourcesRequest,
        list_config_resources_response::ListConfigResourcesResponse,
    },
    krabka_telemetry::LogLevelController,
    std::sync::{Arc, Mutex},
    support::start_n_node_with,
    tracing_subscriber::{Layer, layer::SubscriberExt as _},
};

/// Kafka resource type id for `BROKER_LOGGER`.
const RESOURCE_TYPE_BROKER_LOGGER: i8 = 8;

/// `config_source` `DYNAMIC_BROKER_LOGGER_CONFIG` = 6.
const CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER: i8 = 6;

/// The target the test raises. It sits under `krabka_broker`, so a level set
/// on that logger covers it the way a log4j2 level on a package covers the
/// classes below it.
const CHILD_TARGET: &str = "krabka_broker::broker_logger_wire_test";

/// The filter this broker starts with. It is the production default rather
/// than a spec written for the test, so the alter below exercises the logger
/// names a real `krabka-broker` process actually has: a seed that stopped
/// naming `krabka_broker` would fail this test with `Logger krabka_broker does
/// not exist!` instead of passing against a fixture nobody ships.
const STARTING_SPEC: &str = krabka_broker::config::DEFAULT_LOG_FILTER;

// Record admitted events as target:LEVEL and panic on a poisoned capture lock.
krabka_macros::capture_layer_fixture!(CaptureLayer, Captured, unwrap);

/// Emit one `DEBUG` event on [`CHILD_TARGET`] through `dispatch` and report
/// whether the broker's live filter let it through.
///
/// The emission is synchronous inside `with_default`, so it cannot migrate to
/// another worker thread between setting the dispatcher and logging.
fn debug_event_is_emitted(dispatch: &tracing::Dispatch, captured: &Captured) -> bool {
    tracing::dispatcher::with_default(dispatch, || {
        tracing::debug!(target: CHILD_TARGET, "broker logger probe");
    });
    let mut seen = captured.lock().unwrap();
    let hit = seen
        .iter()
        .any(|line| line == &format!("{CHILD_TARGET}:DEBUG"));
    seen.clear();
    hit
}

async fn build_client(addr: std::net::SocketAddr) -> krabka_client_core::Client {
    connect_owned(
        format!("127.0.0.1:{}", addr.port()),
        "broker-logger-config-test",
        "client build",
    )
    .await
}

/// A `BROKER_LOGGER` describe for `resource_name`.
fn describe_request(resource_name: &str) -> DescribeConfigsRequest {
    DescribeConfigsRequest {
        resources: vec![describe_resource(
            RESOURCE_TYPE_BROKER_LOGGER,
            resource_name.to_owned(),
            None,
        )],
        include_synonyms: false,
        include_documentation: false,
        ..Default::default()
    }
}

/// `BROKER_LOGGER` ROUND-TRIP:
///
/// 1. A `DEBUG` event on a `krabka_broker::` target is dropped, because the
///    broker starts at `INFO`.
/// 2. `IncrementalAlterConfigs` SETs `krabka_broker=DEBUG` on this node's
///    `BROKER_LOGGER` resource.
/// 3. `DescribeConfigs` reports `krabka_broker` at `DEBUG` and `root` at
///    `INFO`, both at `DYNAMIC_BROKER_LOGGER_CONFIG`.
/// 4. The same `DEBUG` event is now emitted.
/// 5. `ListConfigResources` v1 lists the node as a `BROKER_LOGGER` resource.
/// 6. A describe aimed at another node's id is refused: the resource is
///    node-local, so no node answers for another one's loggers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_logger_alter_moves_the_live_filter_and_the_describe_agrees() {
    // The controller the broker will run with, plus a capturing subscriber
    // over the same live filter. Held for the whole test so a rebuild of the
    // callsite interest cache always sees this dispatcher registered.
    let (levels, _filter) = LogLevelController::new(STARTING_SPEC);
    let captured: Captured = Arc::new(Mutex::new(Vec::new()));
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(CaptureLayer(Arc::clone(&captured)).with_filter(levels.filter())),
    );

    let cluster = start_n_node_with(1, |_, cfg| cfg.log_levels = levels.clone())
        .await
        .expect("start_n_node_with");
    let (_, cfg, _dir) = &cluster[0];
    let node = cfg.broker_id.to_string();
    let client = build_client(cfg.listen_addr).await;

    // ── Step 1: the level starts at INFO, so DEBUG is dropped ────────────────
    assert!(
        !debug_event_is_emitted(&dispatch, &captured),
        "a DEBUG event must be dropped while krabka_broker sits at INFO"
    );

    // ── Step 2: IncrementalAlterConfigs ──────────────────────────────────────
    let alter_resp: IncrementalAlterConfigsResponse = client
        .send(
            incremental_request()
                .resources(vec![incremental_resource(
                    RESOURCE_TYPE_BROKER_LOGGER,
                    node.clone(),
                    vec![incremental_config(
                        "krabka_broker".to_owned(),
                        Some("DEBUG".to_owned()),
                        ConfigOperation::Set,
                    )],
                )])
                .call(),
        )
        .await
        .expect("IncrementalAlterConfigs");

    assert!(alter_resp.responses.len() == 1, "{alter_resp:?}");
    let altered = &alter_resp.responses[0];
    assert!(
        altered.error_code == 0,
        "BROKER_LOGGER alter must succeed; error_code={} message={:?}",
        altered.error_code,
        altered.error_message
    );

    // ── Step 3: DescribeConfigs reflects it ──────────────────────────────────
    let describe_resp: DescribeConfigsResponse = client
        .send(describe_request(&node))
        .await
        .expect("DescribeConfigs");

    assert!(describe_resp.results.len() == 1, "{describe_resp:?}");
    let result = &describe_resp.results[0];
    check!(result.error_code == 0, "DescribeConfigs failed: {result:?}");
    check!(result.resource_type == RESOURCE_TYPE_BROKER_LOGGER);
    check!(result.resource_name == node);

    let logger = |name: &str| {
        result
            .configs
            .iter()
            .find(|config| config.name == name)
            .unwrap_or_else(|| panic!("missing logger {name} in {:?}", result.configs))
    };
    let altered_logger = logger("krabka_broker");
    check!(altered_logger.value.as_deref() == Some("DEBUG"));
    check!(altered_logger.config_source == CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER);
    check!(!altered_logger.is_sensitive);
    check!(!altered_logger.read_only);
    let root = logger("root");
    check!(root.value.as_deref() == Some("INFO"));
    check!(root.config_source == CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER);

    // ── Step 4: and the events move with it ──────────────────────────────────
    assert!(
        debug_event_is_emitted(&dispatch, &captured),
        "the DEBUG event must be emitted once krabka_broker is at DEBUG"
    );

    // ── Step 5: ListConfigResources lists the resource ───────────────────────
    let list_resp: ListConfigResourcesResponse = client
        .send(ListConfigResourcesRequest {
            resource_types: vec![RESOURCE_TYPE_BROKER_LOGGER],
            ..Default::default()
        })
        .await
        .expect("ListConfigResources");

    check!(list_resp.error_code == 0, "{list_resp:?}");
    check!(
        list_resp.config_resources.iter().any(|resource| {
            resource.resource_type == RESOURCE_TYPE_BROKER_LOGGER && resource.resource_name == node
        }),
        "BROKER_LOGGER {node} missing from {list_resp:?}"
    );

    // ── Step 6: the resource is node-local ───────────────────────────────────
    let foreign: DescribeConfigsResponse = client
        .send(describe_request("99"))
        .await
        .expect("DescribeConfigs for another node");
    let refused = &foreign.results[0];
    check!(
        refused.error_code == 42,
        "expected INVALID_REQUEST: {refused:?}"
    );
    check!(
        refused.error_message.as_deref()
            == Some("Unexpected broker id, expected 1 but received 99")
    );
    check!(refused.configs.is_empty());
}
