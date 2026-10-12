//! What the broker does when the registry cannot answer, as opposed to
//! answering "not registered".
//!
//! A broker with no `[schema_registry]` section and a broker whose registry
//! answers 500 both fail closed, because admitting the record would make the
//! topic's setting a lie. `fail_open` is the deliberate opt-out, and it is the
//! only configuration here that admits an unresolvable record.

use krabka_broker::BrokerConfig;
use krabka_client_core::Client;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

use crate::harness::{
    INVALID_RECORD, KNOWN_ID, SchemaTopicSetup, apply_registry_config, boot, boot_config,
    check_value_append, create_topic, framed,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_with_no_registry_rejects_a_topic_that_asks_for_validation() {
    // No `[schema_registry]` section at all.
    let dir = tempfile::tempdir().expect("tempdir");
    let config = BrokerConfig::for_tests(dir.path().to_path_buf());
    let (broker, client) = boot_config(config).await;

    expect_fail_closed(&broker, &client).await;
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreachable_registry_fails_closed_by_default() {
    // A server that accepts the connection and then answers 500 for
    // everything: the registry failing to answer, rather than answering "not
    // registered".
    let registry = unavailable_registry().await;

    let (broker, client, _dir) = boot(&registry.uri()).await;
    expect_fail_closed(&broker, &client).await;
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fail_open_admits_a_record_the_registry_could_not_answer_for() {
    let registry = unavailable_registry().await;

    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    let reg_url = registry.uri();
    let toml = format!("[schema_registry]\nurl = \"{reg_url}\"\nfail_open = true\n");
    apply_registry_config(&mut config, &toml);

    let (broker, client) = boot_config(config).await;
    let id = create_topic(&broker, &client, SchemaTopicSetup::default()).await;

    check_value_append(
        &broker,
        &client,
        "validated",
        id,
        Some(framed(KNOWN_ID, b"anything")),
        (0, Some(1)),
    )
    .await;

    broker.shutdown().await;
}

async fn expect_fail_closed(broker: &krabka_broker::BrokerHandle, client: &Client) {
    let id = create_topic(broker, client, SchemaTopicSetup::default()).await;

    check_value_append(
        broker,
        client,
        "validated",
        id,
        Some(framed(KNOWN_ID, b"anything")),
        (INVALID_RECORD, Some(0)),
    )
    .await;
}

async fn unavailable_registry() -> MockServer {
    let registry = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&registry)
        .await;
    registry
}
