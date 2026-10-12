//! KIP-108 / KIP-133: the `[topic_policy]` rule set refusing a `CreateTopics`
//! and a topic `IncrementalAlterConfigs` over the wire, with
//! `POLICY_VIOLATION` (44) and the reason the operator reads.
//!
//! `kafka-topics --create --replication-factor 1` and `kafka-configs --alter
//! --add-config unclean.leader.election.enable=true` send exactly these two
//! requests; the JVM tools turn error 44 plus the message into the
//! `PolicyViolationException` they print.

use assert2::{assert, check};
use krabka_broker::topic_policy::TopicPolicy;
use krabka_protocol::owned::{
    create_topics_request::CreateTopicsRequest,
    incremental_alter_configs_request::IncrementalAlterConfigsRequest,
};

use crate::{
    RESOURCE_TYPE_TOPIC,
    admin_harness::{build_client, create_topic_helper},
    support::{
        configs::{
            ConfigOperation, IncrementalConfigsSetup, incremental_config, incremental_request,
            incremental_resource,
        },
        start_n_node_with,
        topics::{
            ConfiguredTopicSetup, TopicReplicationFactor, creatable_topic, create_topic_request,
        },
    },
};

/// `POLICY_VIOLATION` in the Kafka error table.
const POLICY_VIOLATION: i16 = 44;

fn create_request(name: &str, replication_factor: i16) -> CreateTopicsRequest {
    create_topic_request(creatable_topic(ConfiguredTopicSetup {
        name: (name).into(),
        replicas: TopicReplicationFactor(replication_factor),
        ..Default::default()
    }))
}

/// A replication factor under the policy floor is refused with 44 and a
/// message naming the floor, and no topic is created. A validate-only request
/// for the same topic is refused the same way — Kafka runs the policy on
/// `--dry-run` too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_topics_below_the_replication_floor_is_a_policy_violation() {
    let policy = TopicPolicy {
        min_replication_factor: Some(3),
        ..Default::default()
    };
    let cluster = start_n_node_with(1, |_, cfg| {
        cfg.topic_policy = policy.clone();
    })
    .await
    .expect("start_n_node_with");
    let (broker, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    for validate_only in [false, true] {
        let resp = client
            .send(CreateTopicsRequest {
                validate_only,
                ..create_request("policy-rf", 1)
            })
            .await
            .expect("create_topics");

        let result = &resp.topics[0];
        check!(
            result.error_code == POLICY_VIOLATION,
            "validate_only={validate_only}: {:?}",
            result.error_message
        );
        let message = result.error_message.clone().unwrap_or_default();
        check!(message.contains("replication factor 1"), "{message}");
        check!(message.contains("at least 3"), "{message}");
    }

    assert!(
        broker
            .controller_image_for_test()
            .topic("policy-rf")
            .is_none()
    );
}

/// A topic that satisfies the policy is still created, and a validate-only
/// request for it commits nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_the_policy_allows_is_created_and_validate_only_commits_nothing() {
    let cluster = start_n_node_with(1, |_, cfg| {
        cfg.topic_policy = TopicPolicy {
            min_replication_factor: Some(1),
            max_partitions: Some(4),
            ..TopicPolicy::default()
        };
    })
    .await
    .expect("start_n_node_with");
    let (broker, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    let dry_run = client
        .send(CreateTopicsRequest {
            validate_only: true,
            ..create_request("policy-dry-run", 1)
        })
        .await
        .expect("create_topics");
    check!(dry_run.topics[0].error_code == 0);
    assert!(
        broker
            .controller_image_for_test()
            .topic("policy-dry-run")
            .is_none(),
        "a validate-only create must commit nothing"
    );

    let created = client
        .send(create_request("policy-ok", 1))
        .await
        .expect("create_topics");
    check!(
        created.topics[0].error_code == 0,
        "{:?}",
        created.topics[0].error_message
    );
    check!(created.topics[0].num_partitions == 1);
    assert!(
        broker
            .controller_image_for_test()
            .topic("policy-ok")
            .is_some()
    );
}

/// A config the policy forbids is refused with 44 on the alter path, which is
/// what `kafka-configs --alter --add-config
/// unclean.leader.election.enable=true` sends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn altering_a_topic_to_a_forbidden_config_is_a_policy_violation() {
    let cluster = start_n_node_with(1, |_, cfg| {
        cfg.topic_policy = TopicPolicy {
            forbidden: [(
                "unclean.leader.election.enable".to_owned(),
                "true".to_owned(),
            )]
            .into_iter()
            .collect(),
            ..TopicPolicy::default()
        };
    })
    .await
    .expect("start_n_node_with");
    let (_, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    create_topic_helper(&client, "policy-alter", 1).await;

    let refused = client
        .send(alter_request("policy-alter", "true"))
        .await
        .expect("incremental_alter_configs");
    check!(
        refused.responses[0].error_code == POLICY_VIOLATION,
        "{:?}",
        refused.responses[0].error_message
    );
    let message = refused.responses[0]
        .error_message
        .clone()
        .unwrap_or_default();
    check!(
        message.contains("unclean.leader.election.enable"),
        "{message}"
    );
    check!(message.contains("forbids"), "{message}");

    // The other value of the same key is not what the policy names, so it
    // still commits.
    let accepted = client
        .send(alter_request("policy-alter", "false"))
        .await
        .expect("incremental_alter_configs");
    check!(
        accepted.responses[0].error_code == 0,
        "{:?}",
        accepted.responses[0].error_message
    );
}

fn alter_request(topic: &str, value: &str) -> IncrementalAlterConfigsRequest {
    incremental_request(IncrementalConfigsSetup {
        resources: vec![incremental_resource(
            RESOURCE_TYPE_TOPIC,
            topic,
            vec![incremental_config(
                "unclean.leader.election.enable",
                Some(value.into()),
                ConfigOperation::Set,
            )],
        )],
        ..Default::default()
    })
}
