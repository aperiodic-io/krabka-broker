//! The audit topic itself, and the records that reach it on startup and on an
//! admin operation.
//!
//! The topic has to exist before anything can be audited, the broker's own
//! start has to arrive on it, and every admin mutation has to arrive as an
//! `AdminOperation` record that names the resource it changed. Each case
//! drives the request over a plain client and then reads the record back off
//! the topic, because fetch visibility can lag the durable write.

use {
    crate::support::{
        self, OperatorKeysSetup, OperatorSaslSetup,
        discovery::named_topic_metadata,
        offsets::{delete_records_partition, delete_records_request, delete_records_topic},
        produce::{SinglePartitionProduceSetup, send_batch},
        records::{batch_from_records, value_record},
        sasl::{ScramAlgorithm, ScramUpsertionSetup, scram_upsertion},
        topics::{ConfiguredTopicSetup, creatable_topic, create_topic_request},
    },
    krabka_broker::coordinator::AUDIT_TOPIC,
    krabka_protocol::{
        owned::{
            alter_client_quotas_request::{
                AlterClientQuotasRequest, EntityData, EntryData, OpData as QuotaOp,
            },
            alter_configs_request::{AlterConfigsRequest, AlterConfigsResource, AlterableConfig},
            alter_user_scram_credentials_request::AlterUserScramCredentialsRequest,
            create_partitions_request::{CreatePartitionsRequest, CreatePartitionsTopic},
            delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
            incremental_alter_configs_request::{
                AlterConfigsResource as IncrementalResource, AlterableConfig as IncrementalConfig,
                IncrementalAlterConfigsRequest,
            },
            metadata_request::MetadataRequest,
        },
        records::RecordBatch,
    },
};

/// The KIP-133 `resource_type` discriminant for a topic.
const RESOURCE_TYPE_TOPIC: i8 = 2;
/// The `IncrementalAlterConfigs` `config_operation` for SET.
const OP_SET: i8 = 0;

/// Append one record to partition 0, so that a later `DeleteRecords` has
/// something below the high watermark to delete.
async fn produce_one(
    p: &support::InProcess,
    topic: &str,
    topic_id: krabka_protocol::primitives::uuid::Uuid,
) {
    let batch = RecordBatch {
        last_offset_delta: 0,
        ..batch_from_records(vec![value_record(
            0,
            Some(bytes::Bytes::from_static(b"audited-record")),
        )])
    };
    let resp = send_batch(&p.client, batch, SinglePartitionProduceSetup {
        topic: (topic).into(),
        topic_id,
        ..Default::default()
    })
    .await;
    assert2::check!(resp.responses[0].partition_responses[0].error_code == 0);
}

/// Fill partition 0 and send the same `DeleteRecords` twice.
///
/// A trim only deletes something when the partition holds records below the
/// requested offset, and only the first of two identical requests moves the
/// log start: the second is the idempotent retry that must audit nothing.
async fn trim_twice(p: &support::InProcess, topic: &str) {
    let topic_id = p
        .client
        .send(named_topic_metadata(topic))
        .await
        .expect("Metadata")
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic))
        .map(|t| t.topic_id)
        .expect("topic in Metadata response");
    for _ in 0..2 {
        produce_one(p, topic, topic_id).await;
    }

    let request = delete_records_request(
        vec![delete_records_topic(topic, vec![delete_records_partition(
            0, 2,
        )])],
        5_000,
    );
    for what in ["DeleteRecords", "DeleteRecords (retry)"] {
        let resp = p.client.send(request.clone()).await.expect(what);
        assert2::check!(resp.topics[0].partitions[0].error_code == 0, "{what}");
    }
}

/// The audit records that name one admin operation.
fn records_for(records: &[serde_json::Value], operation: &str) -> usize {
    records
        .iter()
        .filter(|j| j["class_uid"] == 6003 && j["api"]["operation"] == operation)
        .count()
}

#[tokio::test]
async fn audit_topic_exists_after_startup() {
    let p = support::start().await;
    p.broker.wait_until_partition_present(AUDIT_TOPIC, 0).await;

    // Send a Metadata request for `__krabka_audit` and assert the broker
    // returns it with `error_code == 0` and at least one partition.
    let resp = p
        .client
        .send(named_topic_metadata(AUDIT_TOPIC))
        .await
        .expect("MetadataRequest failed");

    let topic = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(AUDIT_TOPIC))
        .expect("__krabka_audit not in Metadata response");

    assert2::check!(
        topic.error_code == 0,
        "unexpected error code: {}",
        topic.error_code
    );
    assert2::check!(
        !topic.partitions.is_empty(),
        "__krabka_audit has no partitions"
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn broker_started_event_is_written_to_audit_topic() {
    let p = support::start().await;

    // Wait for the BrokerStarted event to be durably written to the audit topic
    // (the sink increments `audit_events` on each successful produce).
    p.broker
        .wait_for_metrics("audit event written", |m| m.audit_events.get() >= 1)
        .await;

    // Fetch visibility (the high watermark) can lag the durable write, so retry
    // until the record is consumable rather than single-shot fetching.
    support::wait_for_audit_record(&p.client, "BrokerStarted", |j| {
        j["class_uid"] == 6002 && j["activity_name"] == "BrokerStarted"
    })
    .await;

    p.broker.shutdown().await;
}

/// Every admin mutation this fixture can drive emits one OCSF `ApiActivity`
/// record naming the api and the resource it changed.
///
/// The requests run in order because they build on each other: the topic has
/// to exist before its partitions grow, and it has to go away last. The
/// expectations are then checked as a table. `wait_for_audit_record` reads the
/// whole topic back on each call, so the order the records land in does not
/// matter to the assertions.
///
/// Four apis are out of this fixture's reach. `AlterPartitionReassignments`,
/// `UnregisterBroker`, `AddRaftVoter` and `RemoveRaftVoter` need more than one
/// broker, and the three delegation-token apis need a SASL listener with a
/// master key. `ElectLeaders` is reachable but moves nothing here: a preferred
/// election on a one-broker cluster is already at its preferred leader, and an
/// election that changed no partition is not an audited mutation.
#[tokio::test]
async fn every_admin_mutation_is_audited() {
    let p = support::start().await;
    p.broker.wait_until_partition_present(AUDIT_TOPIC, 0).await;

    let topic = "audited-orders";
    let user = "audited-user";

    let created = p
        .client
        .send(create_topic_request(creatable_topic(
            ConfiguredTopicSetup {
                name: (topic).into(),
                ..Default::default()
            },
        )))
        .await
        .expect("CreateTopics");
    assert2::check!(created.topics[0].error_code == 0);

    let grown = p
        .client
        .send(CreatePartitionsRequest {
            topics: vec![CreatePartitionsTopic {
                name: topic.into(),
                count: 2,
                assignments: None,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreatePartitions");
    assert2::check!(grown.results[0].error_code == 0);

    let altered = p
        .client
        .send(AlterConfigsRequest {
            resources: vec![AlterConfigsResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: topic.into(),
                configs: vec![AlterableConfig {
                    name: "retention.ms".into(),
                    value: Some("604800000".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("AlterConfigs");
    assert2::check!(altered.responses[0].error_code == 0);

    // The complete replacement below drops both keys the two requests above
    // stored, and the audit trail has to say so.
    let dropped = ["retention.ms", "max.message.bytes"];

    let incremental = p
        .client
        .send(IncrementalAlterConfigsRequest {
            resources: vec![IncrementalResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: topic.into(),
                configs: vec![IncrementalConfig {
                    name: "max.message.bytes".into(),
                    config_operation: OP_SET,
                    value: Some("1048576".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("IncrementalAlterConfigs");
    assert2::check!(incremental.responses[0].error_code == 0);

    // A topic `AlterConfigs` replaces the whole override map, so this request
    // deletes both stored keys by omitting them. The record has to name them.
    let replaced = p
        .client
        .send(AlterConfigsRequest {
            resources: vec![AlterConfigsResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: topic.into(),
                configs: vec![AlterableConfig {
                    name: "cleanup.policy".into(),
                    value: Some("delete".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("AlterConfigs (replacement)");
    assert2::check!(replaced.responses[0].error_code == 0);

    let quotas = p
        .client
        .send(AlterClientQuotasRequest {
            entries: vec![EntryData {
                entity: vec![EntityData {
                    entity_type: "user".into(),
                    entity_name: Some(user.into()),
                    ..Default::default()
                }],
                ops: vec![QuotaOp {
                    key: "producer_byte_rate".into(),
                    value: 1_048_576.0,
                    remove: false,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("AlterClientQuotas");
    assert2::check!(quotas.entries[0].error_code == 0);

    // KIP-554 puts PBKDF2 on the client, so these bytes stand in for a
    // client-stretched password. The audit record names the user and nothing
    // else: no salt, no salted password, no derived key.
    let scram = p
        .client
        .send(AlterUserScramCredentialsRequest {
            upsertions: vec![scram_upsertion(ScramUpsertionSetup {
                name: (user).into(),
                mechanism: ScramAlgorithm::Sha256,
                salt: bytes::Bytes::from_static(b"audited-salt"),
                salted_password: bytes::Bytes::from_static(b"audited-salted-password-32-byte"),
                ..Default::default()
            })],
            ..Default::default()
        })
        .await
        .expect("AlterUserScramCredentials");
    assert2::check!(scram.results[0].error_code == 0);

    trim_twice(&p, topic).await;

    let deleted = p
        .client
        .send(DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: Some(topic.into()),
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("DeleteTopics");
    assert2::check!(deleted.responses[0].error_code == 0);

    // One row per audited api: the OCSF operation name, and the resource the
    // record has to name first.
    let expected = [
        ("CreateTopics", topic.to_string()),
        ("CreatePartitions", topic.to_string()),
        ("AlterConfigs", topic.to_string()),
        ("IncrementalAlterConfigs", topic.to_string()),
        ("AlterClientQuotas", format!("user={user}")),
        ("AlterUserScramCredentials", user.to_string()),
        ("DeleteRecords", format!("{topic}-0")),
        ("DeleteTopics", topic.to_string()),
    ];
    for (operation, resource) in expected {
        support::wait_for_audit_record(&p.client, operation, |j| {
            j["class_uid"] == 6003
                && j["api"]["operation"] == operation
                && j["status_id"] == 1
                && j["resources"][0]["name"] == resource
        })
        .await;
    }

    // A replacement deletes every stored key it omits, so the record for it
    // names the keys that disappeared beside the one it set.
    support::wait_for_audit_record(&p.client, "AlterConfigs replacement", |j| {
        j["api"]["operation"] == "AlterConfigs"
            && j["resources"].as_array().is_some_and(|resources| {
                let named = |key: &str| resources.iter().any(|r| r["name"] == key);
                named("cleanup.policy") && dropped.iter().copied().all(named)
            })
    })
    .await;

    // Redaction: nothing the SCRAM request carried reaches the audit topic,
    // and neither do the config values.
    let records = support::consume_audit_records(&p.client).await;

    // The retried trim deleted nothing, and an audit record claiming it did
    // would be false evidence of a second deletion.
    assert2::check!(
        records_for(&records, "DeleteRecords") == 1,
        "an idempotent DeleteRecords retry must audit nothing"
    );
    let dump = serde_json::to_string(&records).expect("audit records serialize");
    for secret in [
        "audited-salt",
        "audited-salted-password-32-byte",
        "604800000",
    ] {
        assert2::check!(!dump.contains(secret), "audit topic leaked {secret}");
    }

    // The changed config *keys* are named, because that is what says a
    // retention or a message-size limit moved.
    assert2::check!(dump.contains("retention.ms"));
    assert2::check!(dump.contains("max.message.bytes"));

    p.broker.shutdown().await;
}

/// A SASL/PLAIN exchange writes one OCSF `Authentication` row per completed
/// attempt, whichever way it ended.
///
/// Both halves run on one broker because each half is one connection, and the
/// row an auditor needs is the one that says which credential the connection
/// presented and from where. The success row is written by the reading
/// client's own login; the failure row by a second connection whose password
/// is wrong, which the broker answers with `SASL_AUTHENTICATION_FAILED` and
/// closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_logins_are_audited_either_way() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let (broker, bootstrap, _config) =
        support::start_with_operator_keys_sasl(&dir.path().join("data"), OperatorSaslSetup {
            trust: OperatorKeysSetup {
                keys: &[],
                approvers: &[],
            },
            users: &[("alice", "alice-secret")],
        })
        .await;
    broker.wait_until_partition_present(AUDIT_TOPIC, 0).await;

    let client = support::sasl_client(&bootstrap, "alice", "alice-secret").await;

    // A wrong password never gets past the SaslAuthenticate frame, so the
    // failed login is all this client ever leaves behind.
    let refused = krabka_client_core::Client::builder()
        .bootstrap(bootstrap)
        .client_id("krabka-broker-test")
        .security(support::sasl_plain_security("alice", "not-the-password"))
        .build()
        .await;
    let served = match refused {
        Ok(client) => client.send(MetadataRequest::default()).await.is_ok(),
        Err(_) => false,
    };
    assert2::check!(!served, "a wrong PLAIN password must serve no request");

    // Both rows name the same principal in the `User:<name>` form the
    // privileged-action rows use, and carry the peer they came from.
    for (what, status_id) in [("the successful login", 1), ("the refused login", 2)] {
        support::wait_for_audit_record(&client, what, |j| {
            j["class_uid"] == 3002
                && j["status_id"] == status_id
                && j["auth_protocol"] == "PLAIN"
                && j["actor"]["user"]["name"] == "User:alice"
                && j["src_endpoint"]["ip"] == "127.0.0.1"
        })
        .await;
    }

    broker.shutdown().await;
}
