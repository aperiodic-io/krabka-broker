//! The two topic configs as the JVM admin tools drive them: `CreateTopics`,
//! `IncrementalAlterConfigs`, the legacy `AlterConfigs`, and the
//! `DescribeConfigs` read-back.

use {
    crate::{
        harness::{
            CONFIG_SOURCE_DEFAULT, CONFIG_SOURCE_TOPIC, TOPIC_RESOURCE, accepted_mutation,
            appended_at, describe_topic, recs, stored_key, topic,
        },
        support::{
            self,
            configs::{
                ConfigMutationOutcome, ConfigOperation, describe_resource, legacy_config,
                legacy_request, legacy_resource,
            },
            dedup::{DEDUP_HEADER, Rec},
            topics::{creatable_topic, create_topic_request, topic_configs},
        },
    },
    assert2::{assert, check},
    krabka_broker::codes,
    krabka_protocol::{
        UnknownTaggedFields,
        owned::{
            describe_configs_request::DescribeConfigsRequest,
            describe_configs_response::DescribeConfigsResourceResult,
        },
    },
};

const WINDOW: &str = "dedup.window.ms";
const HEADER: &str = "dedup.key.header";

/// `DescribeConfigsResponse.ConfigType.LONG`.
const CONFIG_TYPE_LONG: i8 = 5;

/// `DescribeConfigsResponse.ConfigType.STRING`.
const CONFIG_TYPE_STRING: i8 = 2;

const WINDOW_DOCUMENTATION: &str = "Produce-time deduplication window. The leader drops a record \
    whose dedup identity it already admitted within this many milliseconds, and the first record \
    wins. The identity is the record key; a record with a null key is never deduplicated. When \
    dedup.key.header is set, the identity is the value of that header instead. The window \
    compares record timestamps: CreateTime clamped to the broker clock, or the append time on a \
    LogAppendTime topic. A dropped record leaves an offset gap, as on a compacted topic. \
    Transactional produce bypasses the check. Deduplication is best-effort near the window edge \
    and is bounded by the broker's dedup_max_entries_per_partition and dedup_max_entries limits. \
    -1, the default, turns it off. Not supported with krabka.diskless=true.";

const HEADER_DOCUMENTATION: &str = "Name of the record header whose value is the dedup identity \
    under dedup.window.ms. Unset, the default, uses the record key. The first header with this \
    name counts. A record without the header, or with a null header value, fails the produce \
    request with INVALID_RECORD (87). The name is trimmed and must not be empty. It has no effect \
    while dedup.window.ms is -1.";

/// The `DescribeConfigs` entry of `name` with `value` from `source`.
fn entry(name: &str, value: Option<&str>, source: i8) -> DescribeConfigsResourceResult {
    let (config_type, documentation) = if name == WINDOW {
        (CONFIG_TYPE_LONG, WINDOW_DOCUMENTATION)
    } else {
        (CONFIG_TYPE_STRING, HEADER_DOCUMENTATION)
    };
    DescribeConfigsResourceResult {
        name: name.to_owned(),
        value: value.map(str::to_owned),
        read_only: false,
        config_source: source,
        is_sensitive: false,
        synonyms: vec![],
        config_type,
        documentation: Some(documentation.to_owned()),
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    }
}

fn refusal(message: &str) -> ConfigMutationOutcome {
    ConfigMutationOutcome {
        error_code: codes::INVALID_CONFIG,
        error_message: Some(message.to_owned()),
    }
}

const WINDOW_RANGE: &str = "Value must be at least 1, or -1 to disable deduplication";

/// The values each key refuses, with the answer `kafka-topics --create` and
/// `kafka-configs --alter` print for them.
fn invalid_values() -> Vec<(&'static str, &'static str, ConfigMutationOutcome)> {
    vec![
        (
            WINDOW,
            "0",
            refusal(&format!(
                "Invalid value 0 for configuration {WINDOW}: {WINDOW_RANGE}"
            )),
        ),
        (
            WINDOW,
            "-2",
            refusal(&format!(
                "Invalid value -2 for configuration {WINDOW}: {WINDOW_RANGE}"
            )),
        ),
        (
            WINDOW,
            "abc",
            refusal(&format!(
                "Invalid value abc for configuration {WINDOW}: Not a number of type LONG"
            )),
        ),
        (
            HEADER,
            "",
            refusal(&format!(
                "Invalid value  for configuration {HEADER}: Header name must not be empty"
            )),
        ),
        (
            HEADER,
            "   ",
            refusal(&format!(
                "Invalid value  for configuration {HEADER}: Header name must not be empty"
            )),
        ),
    ]
}

/// `CreateTopics` refuses each invalid value and creates nothing, refuses a
/// window on a diskless topic, and accepts the valid shapes, among them a
/// header with the window off.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_topics_validates_both_keys() {
    let p = support::start().await;
    let diskless = refusal(
        "dedup.window.ms is not supported on diskless topics: krabka.diskless=true cannot be \
         combined with a dedup.window.ms other than -1, because produce-time deduplication runs \
         only on the local-log produce path",
    );
    let mut cases: Vec<(Vec<(&str, &str)>, ConfigMutationOutcome)> = invalid_values()
        .into_iter()
        .map(|(key, value, outcome)| (vec![(key, value)], outcome))
        .collect();
    cases.extend([
        (
            vec![("krabka.diskless", "true"), (WINDOW, "60000")],
            diskless,
        ),
        (vec![(WINDOW, "-1")], accepted_mutation()),
        (vec![(WINDOW, "1")], accepted_mutation()),
        (vec![(HEADER, DEDUP_HEADER)], accepted_mutation()),
        (
            vec![(WINDOW, "60000"), (HEADER, DEDUP_HEADER)],
            accepted_mutation(),
        ),
    ]);
    for (index, (configs, expected)) in cases.into_iter().enumerate() {
        let name = format!("create-{index}");
        let response = p
            .client
            .send(create_topic_request(
                creatable_topic()
                    .name(name.clone())
                    .configs(topic_configs(configs.iter().copied()))
                    .call(),
            ))
            .await
            .expect("CreateTopics");
        let row = &response.topics[0];
        check!(
            ConfigMutationOutcome {
                error_code: row.error_code,
                error_message: row.error_message.clone(),
            } == expected,
            "{configs:?}"
        );
        let described = p
            .client
            .send(DescribeConfigsRequest {
                resources: vec![describe_resource(
                    TOPIC_RESOURCE,
                    name,
                    Some(vec![WINDOW.to_owned()]),
                )],
                ..Default::default()
            })
            .await
            .expect("DescribeConfigs");
        let exists = described.results[0].error_code == codes::NONE;
        check!(
            exists == (expected.error_code == codes::NONE),
            "{configs:?}"
        );
    }
    p.broker.shutdown().await;
}

/// `DescribeConfigs` reports both keys on every topic: at their defaults,
/// `-1` and null, before anything sets them, and as topic overrides once set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_configs_reports_both_keys() {
    let p = support::start().await;
    let t = topic().p(&p).name("described").window_ms(None).call().await;

    let defaults = describe_topic(&p.client, t.name, &[WINDOW, HEADER]).await;
    t.set_config(WINDOW, "30000").await;
    t.set_config(HEADER, DEDUP_HEADER).await;
    let overrides = describe_topic(&p.client, t.name, &[WINDOW, HEADER]).await;

    assert!(
        defaults
            == [
                entry(WINDOW, Some("-1"), CONFIG_SOURCE_DEFAULT),
                entry(HEADER, None, CONFIG_SOURCE_DEFAULT),
            ]
    );
    assert!(
        overrides
            == [
                entry(WINDOW, Some("30000"), CONFIG_SOURCE_TOPIC),
                entry(HEADER, Some(DEDUP_HEADER), CONFIG_SOURCE_TOPIC),
            ]
    );
    p.broker.shutdown().await;
}

/// `IncrementalAlterConfigs` turns deduplication on, changes it and turns it
/// off on a live topic, and refuses the values `CreateTopics` refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_alter_configs_enables_changes_and_disables_dedup() {
    let p = support::start().await;
    let t = topic()
        .p(&p)
        .name("incremental")
        .window_ms(None)
        .call()
        .await;

    let off = t.send(&recs(&["a", "a"])).await;
    t.set_config(WINDOW, "60000").await;
    let on = t.send(&recs(&["a", "b", "b"])).await;
    t.set_config(WINDOW, "120000").await;
    let changed = describe_topic(&p.client, t.name, &[WINDOW]).await;
    let still_on = t.send(&recs(&["b"])).await;
    t.delete_config(WINDOW).await;
    let deleted = describe_topic(&p.client, t.name, &[WINDOW]).await;
    let off_again = t.send(&recs(&["b"])).await;
    let mut refusals = Vec::new();
    for (key, value, _) in invalid_values() {
        refusals.push(t.alter(key, ConfigOperation::Set, Some(value)).await);
    }
    let after_refusals = describe_topic(&p.client, t.name, &[WINDOW, HEADER]).await;
    let contents = t.contents().await;

    assert!(
        [off, on, still_on, off_again]
            == [
                appended_at(0),
                appended_at(2),
                appended_at(5),
                appended_at(6)
            ]
    );
    assert!(changed == [entry(WINDOW, Some("120000"), CONFIG_SOURCE_TOPIC)]);
    assert!(deleted == [entry(WINDOW, Some("-1"), CONFIG_SOURCE_DEFAULT)]);
    assert!(
        refusals
            == invalid_values()
                .into_iter()
                .map(|(_, _, outcome)| outcome)
                .collect::<Vec<_>>()
    );
    assert!(
        after_refusals
            == [
                entry(WINDOW, Some("-1"), CONFIG_SOURCE_DEFAULT),
                entry(HEADER, None, CONFIG_SOURCE_DEFAULT),
            ]
    );
    assert!(
        contents.records
            == [
                stored_key(0, "a"),
                stored_key(1, "a"),
                stored_key(3, "b"),
                stored_key(6, "b"),
            ]
    );
    p.broker.shutdown().await;
}

/// The legacy `AlterConfigs` replaces a topic's whole override set: one
/// request turns header-mode deduplication on, and an empty one turns it off
/// again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_alter_configs_sets_and_clears_both_keys() {
    let p = support::start().await;
    let t = topic().p(&p).name("legacy").window_ms(None).call().await;
    let alter = |configs: Vec<(&str, &str)>| {
        legacy_request(
            vec![legacy_resource(
                TOPIC_RESOURCE,
                t.name.to_owned(),
                configs
                    .into_iter()
                    .map(|(key, value)| legacy_config(key.to_owned(), Some(value.to_owned())))
                    .collect(),
            )],
            false,
        )
    };

    let set = p
        .client
        .send(alter(vec![(WINDOW, "45000"), (HEADER, DEDUP_HEADER)]))
        .await
        .expect("AlterConfigs");
    t.await_config(HEADER, |entry| entry.value.as_deref() == Some(DEDUP_HEADER))
        .await;
    let described = describe_topic(&p.client, t.name, &[WINDOW, HEADER]).await;
    let missing_header = t.send(&recs(&["a"])).await;
    let tagged = t
        .send(&[Rec::key("a").dedup_id("h1"), Rec::key("b").dedup_id("h1")])
        .await;
    let cleared = p.client.send(alter(vec![])).await.expect("AlterConfigs");
    t.await_config(WINDOW, |entry| entry.config_source == CONFIG_SOURCE_DEFAULT)
        .await;
    let after_clear = describe_topic(&p.client, t.name, &[WINDOW, HEADER]).await;
    let plain = t.send(&recs(&["a"])).await;
    let contents = t.contents().await;

    assert!(set.responses[0].error_code == codes::NONE);
    assert!(cleared.responses[0].error_code == codes::NONE);
    assert!(
        described
            == [
                entry(WINDOW, Some("45000"), CONFIG_SOURCE_TOPIC),
                entry(HEADER, Some(DEDUP_HEADER), CONFIG_SOURCE_TOPIC),
            ]
    );
    assert!(missing_header.error_code == codes::INVALID_RECORD);
    assert!(tagged == appended_at(0));
    assert!(
        after_clear
            == [
                entry(WINDOW, Some("-1"), CONFIG_SOURCE_DEFAULT),
                entry(HEADER, None, CONFIG_SOURCE_DEFAULT),
            ]
    );
    assert!(plain == appended_at(2));
    assert!(contents.records == [stored_key(0, "a"), stored_key(2, "a")]);
    p.broker.shutdown().await;
}
