//! KIP-211 `offsets.retention.minutes`, end to end over the wire.
//!
//! Two cases read the retention knobs back through `DescribeConfigs`, the way
//! `kafka-configs --entity-type brokers --describe` does: an untuned broker
//! reports both at `DEFAULT_CONFIG`, and a broker whose configuration names a
//! key reports that key at `STATIC_BROKER_CONFIG`. The third lets the broker's
//! own background sweep run and watches a dead group disappear from
//! `ListGroups`.

mod support;

use std::{sync::Arc, time::Duration};

use assert2::{assert, check};
use krabka_broker::{Broker, BrokerConfig, codes};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    describe_configs_request::DescribeConfigsRequest,
    describe_configs_response::DescribeConfigsResourceResult,
    list_groups_request::ListGroupsRequest,
    offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestPartition},
};
use krabka_units::{millis, minutes};
use support::topic_id_for;

use crate::support::{
    client::connect_owned,
    configs::describe_resource,
    offsets::{
        offset_commit_topic, offset_delete_partition, offset_delete_request, offset_delete_topic,
    },
    topics::creatable_topic,
};

/// `ConfigResource.Type.BROKER`.
const RESOURCE_TYPE_BROKER: i8 = 4;
/// `ConfigEntry.ConfigSource.DEFAULT_CONFIG`. Verified against
/// `apache/kafka:4.3.1`: `kafka-configs --describe --all` reports both keys
/// with `DEFAULT_CONFIG` on a broker whose properties do not set them.
const CONFIG_SOURCE_DEFAULT: i8 = 5;
/// `ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG`. Verified against
/// `apache/kafka:4.3.1`: a broker whose properties carry
/// `offsets.retention.minutes=10080` — Kafka's own default — reports that key
/// with `STATIC_BROKER_CONFIG` at the head of its synonym chain, because the
/// source says where the value came from, not whether it differs from the
/// default.
const CONFIG_SOURCE_STATIC_BROKER: i8 = 4;
/// `DescribeConfigsResponse.ConfigType`, which mirrors `ConfigDef.Type`.
/// `GroupCoordinatorConfig` declares `offsets.retention.minutes` an `INT` and
/// `offsets.retention.check.interval.ms` a `LONG`.
const CONFIG_TYPE_INT: i8 = 3;
const CONFIG_TYPE_LONG: i8 = 5;
const OFFSETS_RETENTION_MINUTES: &str = "offsets.retention.minutes";
const OFFSETS_RETENTION_CHECK_INTERVAL_MS: &str = "offsets.retention.check.interval.ms";

async fn client_for(broker: &krabka_broker::BrokerHandle) -> Arc<Client> {
    Arc::new(
        connect_owned(
            broker.listen_addr().to_string().as_str(),
            "retention-test",
            "client",
        )
        .await,
    )
}

/// Both knobs come back on the broker resource, read-only and static, at
/// Kafka's own defaults.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_configs_reports_the_retention_knobs() {
    let (_dir, broker) = crate::support::standalone_broker().await;
    let client = client_for(&broker).await;

    let described = client
        .send(DescribeConfigsRequest {
            resources: vec![describe_resource(
                RESOURCE_TYPE_BROKER,
                "1".to_string(),
                None,
            )],
            include_synonyms: false,
            include_documentation: false,
            ..Default::default()
        })
        .await
        .expect("DescribeConfigs");

    let result = &described.results[0];
    check!(result.error_code == codes::NONE);
    let entry = |name: &str| result.configs.iter().find(|c| c.name == name).cloned();
    check!(
        entry(OFFSETS_RETENTION_MINUTES)
            == Some(DescribeConfigsResourceResult {
                name: OFFSETS_RETENTION_MINUTES.into(),
                // Kafka's default: 10080 minutes, seven days.
                value: Some("10080".into()),
                read_only: true,
                config_source: CONFIG_SOURCE_DEFAULT,
                config_type: CONFIG_TYPE_INT,
                // Not requested, so null, as Kafka sends it.
                documentation: None,
                ..Default::default()
            })
    );
    check!(
        entry(OFFSETS_RETENTION_CHECK_INTERVAL_MS)
            == Some(DescribeConfigsResourceResult {
                name: OFFSETS_RETENTION_CHECK_INTERVAL_MS.into(),
                // Kafka's default: ten minutes.
                value: Some("600000".into()),
                read_only: true,
                config_source: CONFIG_SOURCE_DEFAULT,
                config_type: CONFIG_TYPE_LONG,
                // Not requested, so null, as Kafka sends it.
                documentation: None,
                ..Default::default()
            })
    );

    broker.shutdown().await;
}

/// A knob the broker's configuration names reports `STATIC_BROKER_CONFIG`,
/// even at Kafka's own default value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_configs_reports_a_named_knob_as_static() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    // The same number Kafka defaults to, named explicitly.
    config.offsets_retention_override = Some(minutes(10_080));
    let broker = Broker::start(config).await.unwrap();
    let client = client_for(&broker).await;

    let described = client
        .send(DescribeConfigsRequest {
            resources: vec![describe_resource(
                RESOURCE_TYPE_BROKER,
                "1".to_string(),
                Some(vec![
                    OFFSETS_RETENTION_MINUTES.to_string(),
                    OFFSETS_RETENTION_CHECK_INTERVAL_MS.to_string(),
                ]),
            )],
            include_synonyms: false,
            include_documentation: false,
            ..Default::default()
        })
        .await
        .expect("DescribeConfigs");

    let result = &described.results[0];
    check!(result.error_code == codes::NONE);
    check!(
        result.configs
            == vec![
                DescribeConfigsResourceResult {
                    name: OFFSETS_RETENTION_CHECK_INTERVAL_MS.into(),
                    value: Some("600000".into()),
                    read_only: true,
                    // Untouched, so still inherited.
                    config_source: CONFIG_SOURCE_DEFAULT,
                    config_type: CONFIG_TYPE_LONG,
                    // Not requested, so null, as Kafka sends it.
                    documentation: None,
                    ..Default::default()
                },
                DescribeConfigsResourceResult {
                    name: OFFSETS_RETENTION_MINUTES.into(),
                    value: Some("10080".into()),
                    read_only: true,
                    config_source: CONFIG_SOURCE_STATIC_BROKER,
                    config_type: CONFIG_TYPE_INT,
                    // Not requested, so null, as Kafka sends it.
                    documentation: None,
                    ..Default::default()
                },
            ]
    );

    broker.shutdown().await;
}

/// A group with no members and no committed offsets is dead, and the broker's
/// own sweep drops it from `ListGroups` without an operator running
/// `DeleteGroups`.
///
/// This is the shape a CI job or an ad-hoc `kafka-console-consumer` leaves
/// behind once its offsets are gone. Verified against `apache/kafka:4.3.1`:
/// a group left memberless by `kafka-consumer-groups --delete-offsets`
/// disappears from `--list` after one `offsets.retention.check.interval.ms`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_broker_sweep_reaps_a_dead_group_on_its_own() {
    const GROUP: &str = "leaked-by-ci";
    const TOPIC: &str = "orders";

    let dir = tempfile::TempDir::new().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.offsets_retention_check_interval_override = Some(millis(25));
    let broker = Broker::start(config).await.unwrap();
    broker.wait_until_group_coordinator_ready().await;
    let client = client_for(&broker).await;

    // `OffsetDelete` resolves each partition against the metadata image, so
    // the topic has to exist for the delete to reach the log.
    let created = client
        .send(crate::support::topics::create_topic_request_with_setup(
            creatable_topic(crate::support::topics::ConfiguredTopicSetup {
                name: TOPIC.to_string(),
                ..Default::default()
            }),
            crate::support::topics::CreateTopicRequestSetup {
                timeout: crate::support::topics::CreateTopicsTimeoutMillis(10_000),
            },
        ))
        .await
        .expect("CreateTopics");
    assert!(created.topics[0].error_code == codes::NONE);

    // KIP-516: `OffsetCommit` negotiates to v10, which carries `topic_id`
    // rather than the name, so a commit that names only the topic lands under
    // an empty name and `OffsetDelete` cannot find it again.
    let topic_id = topic_id_for(&client, TOPIC).await;

    let committed = client
        .send(OffsetCommitRequest {
            group_id: GROUP.to_string(),
            generation_id_or_member_epoch: -1,
            member_id: String::new(),
            topics: vec![offset_commit_topic(
                TOPIC.to_string(),
                topic_id,
                vec![OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: 5,
                    committed_leader_epoch: -1,
                    ..Default::default()
                }],
            )],
            ..Default::default()
        })
        .await
        .expect("OffsetCommit");
    assert!(committed.topics[0].partitions[0].error_code == codes::NONE);

    let listed = |client: Arc<Client>| async move {
        client
            .send(ListGroupsRequest::default())
            .await
            .expect("ListGroups")
            .groups
            .iter()
            .any(|group| group.group_id == GROUP)
    };
    assert!(listed(Arc::clone(&client)).await, "the group starts listed");

    // Take the group's last offset away, the way `kafka-consumer-groups
    // --delete-offsets` does. What is left is a memberless group holding
    // nothing.
    let deleted = client
        .send(offset_delete_request(
            GROUP.to_string(),
            vec![offset_delete_topic(
                TOPIC.to_string(),
                vec![offset_delete_partition(0)],
            )],
        ))
        .await
        .expect("OffsetDelete");
    assert!(deleted.error_code == codes::NONE);
    assert!(deleted.topics[0].partitions[0].error_code == codes::NONE);

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while listed(Arc::clone(&client)).await {
        assert!(
            std::time::Instant::now() < deadline,
            "the sweep never reaped the dead group"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    broker.shutdown().await;
}
