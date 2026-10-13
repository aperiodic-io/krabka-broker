//! The config surface the JVM tools drive: `kafka-topics --create --config
//! max.message.bytes=...`, `kafka-configs --alter --add-config`, and the
//! `DescribeConfigs` read-back both of them show.

use {
    crate::{
        support::{
            self,
            configs::{alter_topic_config, topic_config},
        },
        wire::{
            MAX_MESSAGE_BYTES, accepted, create_topic, orders_fixture, produce_batch_of_wire_len,
            too_large,
        },
    },
    assert2::check,
    krabka_broker::codes,
    krabka_client_core::Client,
    krabka_protocol::owned::describe_configs_response::DescribeConfigsResourceResult,
};

/// `ConfigSource::DYNAMIC_TOPIC_CONFIG`, which is what a stored topic
/// override reports.
const CONFIG_SOURCE_DYNAMIC_TOPIC: i8 = 1;

/// `DescribeConfigsResponse.ConfigType::INT`, the byte the JVM `AdminClient`
/// reads out of `ConfigEntry.type()`. Kafka declares `max.message.bytes` as
/// `INT`, so the entry carries `3` rather than the untyped `UNKNOWN`.
const CONFIG_TYPE_INT: i8 = 3;

const CAP: usize = 2048;

/// `CreateTopics` accepts the key, and `DescribeConfigs` echoes it.
///
/// The whole entry is compared rather than its value alone. `kafka-configs
/// --describe` renders the source and the read-only flag beside the value, and
/// a key reported as read-only would tell an operator that the alter in the
/// next case cannot work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_topics_accepts_the_key_and_describe_configs_echoes_it() {
    let p = support::start().await;
    create_topic(&p.broker, &p.client, "orders", &[(
        MAX_MESSAGE_BYTES,
        &CAP.to_string(),
    )])
    .await;

    check!(
        describe_max_message_bytes(&p.client, "orders").await
            == DescribeConfigsResourceResult {
                name: MAX_MESSAGE_BYTES.to_owned(),
                value: Some(CAP.to_string()),
                read_only: false,
                config_source: CONFIG_SOURCE_DYNAMIC_TOPIC,
                is_sensitive: false,
                synonyms: vec![],
                config_type: CONFIG_TYPE_INT,
                documentation: None,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            }
    );

    p.broker.shutdown().await;
}

/// `kafka-configs --alter --add-config max.message.bytes=...` raises the cap
/// on a live topic, and the produce path picks the new value up.
///
/// The refusal before the alter and the acceptance after it are the same
/// batch. That is what separates "the alter was stored" from "the alter took
/// effect": a broker that recorded the override and kept gating on the old
/// value would answer `NONE` to the alter and `MESSAGE_TOO_LARGE` to the
/// produce, which is the failure an operator would be least able to explain.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_incremental_alter_raises_the_cap_for_the_next_produce() {
    let (p, topic) = orders_fixture(&[(MAX_MESSAGE_BYTES, &CAP.to_string())]).await;

    check!(produce_batch_of_wire_len(&p.client, "orders", topic, CAP + 1).await == too_large());

    let raised = (CAP + 1).to_string();
    check!(set_max_message_bytes(&p.client, "orders", &raised).await == (codes::NONE, None));
    check!(describe_max_message_bytes(&p.client, "orders").await.value == Some(raised));
    check!(produce_batch_of_wire_len(&p.client, "orders", topic, CAP + 1).await == accepted(0, 0));

    p.broker.shutdown().await;
}

/// The value check matches Kafka's: `INT` with `atLeast(0)`.
///
/// `apache/kafka:4.3.1` answers "Value must be at least 0" to `-1` and "Not a
/// number of type INT" to `2147483648` and to `abc`, and accepts `0`. A broker
/// that took `-1` would store a cap no batch can satisfy.
///
/// The refusal's error code is asserted, not merely its non-zero-ness, because
/// `INVALID_CONFIG` (40) is the code this whole feature exists to stop the
/// broker sending for a *valid* value. A refusal that arrived as some other
/// code would leave `kafka-configs` printing an unfamiliar failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_alter_path_rejects_the_values_kafka_rejects() {
    let p = support::start().await;
    create_topic(&p.broker, &p.client, "orders", &[]).await;

    for (value, expected) in [
        ("0", codes::NONE),
        ("2147483647", codes::NONE),
        ("-1", codes::INVALID_CONFIG),
        ("2147483648", codes::INVALID_CONFIG),
        ("plenty", codes::INVALID_CONFIG),
    ] {
        let (error_code, error_message) = set_max_message_bytes(&p.client, "orders", value).await;
        check!(
            error_code == expected,
            "max.message.bytes={value} answered {error_code}"
        );
        check!(
            error_message.is_some() == (expected != codes::NONE),
            "max.message.bytes={value} answered {error_message:?}"
        );
    }

    p.broker.shutdown().await;
}

/// Read the topic's `max.message.bytes` entry back through `DescribeConfigs`.
async fn describe_max_message_bytes(client: &Client, topic: &str) -> DescribeConfigsResourceResult {
    topic_config(client, topic, MAX_MESSAGE_BYTES).await
}

/// Drive `kafka-configs --alter --add-config max.message.bytes=<value>`.
async fn set_max_message_bytes(client: &Client, topic: &str, value: &str) -> (i16, Option<String>) {
    alter_topic_config()
        .client(client)
        .topic(topic)
        .name(MAX_MESSAGE_BYTES)
        .maybe_value(Some(value))
        .call()
        .await
}
