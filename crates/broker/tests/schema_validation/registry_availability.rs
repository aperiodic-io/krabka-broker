//! What the broker does when the registry cannot answer, as opposed to
//! answering "not registered".
//!
//! A broker with no `[schema_registry]` section and a broker whose registry
//! answers 500 both fail closed, because admitting the record would make the
//! topic's setting a lie. `fail_open` is the deliberate opt-out, and it is the
//! only configuration here that admits an unresolvable record.

use {
    crate::harness::{
        INVALID_RECORD, KNOWN_ID, apply_registry_config, boot, boot_config, check_value_append,
        create_topic, framed,
    },
    krabka_broker::BrokerConfig,
    krabka_client_core::Client,
    wiremock::{Mock, MockServer, ResponseTemplate, matchers::method},
};

#[yare::parameterized(
    no_registry_section = { false },
    registry_returns_500 = { true },
)]
#[test_macro(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn a_registry_that_cannot_answer_fails_closed(registry_returns_500: bool) {
    let registry = if registry_returns_500 {
        Some(unavailable_registry().await)
    } else {
        None
    };
    let (broker, client, _dir) = if let Some(registry) = &registry {
        boot(&registry.uri()).await
    } else {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let (broker, client) = boot_config(config).await;
        (broker, client, dir)
    };

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
    let id = create_topic().broker(&broker).client(&client).call().await;

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
    let id = create_topic().broker(broker).client(client).call().await;

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
