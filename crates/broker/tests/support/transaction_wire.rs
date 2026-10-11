//! Protocol and client fixtures shared by transaction coordinator scenarios.

use std::time::{Duration, Instant};

use assert2::assert;
use bytes::Bytes;
use krabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use krabka_protocol::{
    owned::{
        add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction},
        add_partitions_to_txn_response::AddPartitionsToTxnResponse,
        common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
        init_producer_id_request::InitProducerIdRequest,
        produce_request::ProduceRequest,
    },
    primitives::uuid::Uuid,
    records::{Attributes, RecordBatch},
};

use crate::support::{
    produce::{SinglePartitionProduceSetup, batch_request},
    records::{batch_from_records, value_record},
    topics::{
        AssignedTopicSetup, ConfiguredTopicSetup, TopicPartitionCount, assigned_topic,
        creatable_topic, create_topic_request,
    },
    transactions::{
        InitProducerSetup, PartitionRegistration, ProducerIdentity,
        init_producer_request as producer_initialization,
    },
};

pub(crate) fn init_producer_request(transactional_id: &str) -> InitProducerIdRequest {
    producer_initialization(InitProducerSetup {
        transactional_id: Some(transactional_id.into()),
        ..Default::default()
    })
}

/// Supply both layouts so the client can negotiate either protocol version.
pub(crate) fn add_partition_request(
    transactional_id: &str,
    topic: &str,
    (producer_id, epoch): (i64, i16),
) -> AddPartitionsToTxnRequest {
    partitions_request(TransactionPartitionsSetup {
        transactional_id,
        producer: ProducerIdentity::from_wire((producer_id, epoch)),
        topics: vec![transaction_topic(topic, vec![0])],
        ..Default::default()
    })
}

pub(crate) fn transaction_topic(name: &str, partitions: Vec<i32>) -> AddPartitionsToTxnTopic {
    AddPartitionsToTxnTopic {
        name: name.into(),
        partitions,
        ..Default::default()
    }
}

/// Both request layouts carry the same explicit transaction and partition set.
#[derive(krabka_macros::FieldDefaults)]
pub(crate) struct TransactionPartitionsSetup<'a> {
    #[default("transaction")]
    pub transactional_id: &'a str,
    pub producer: ProducerIdentity,
    pub registration: PartitionRegistration,
    #[default(vec![transaction_topic("orders", vec![0])])]
    pub topics: Vec<AddPartitionsToTxnTopic>,
}

pub(crate) fn partitions_request(
    setup: TransactionPartitionsSetup<'_>,
) -> AddPartitionsToTxnRequest {
    let TransactionPartitionsSetup {
        transactional_id,
        producer,
        registration,
        topics,
    } = setup;
    AddPartitionsToTxnRequest {
        transactions: vec![AddPartitionsToTxnTransaction {
            transactional_id: transactional_id.into(),
            producer_id: producer.id.0,
            producer_epoch: producer.epoch.0,
            verify_only: registration == PartitionRegistration::VerifyOnly,
            topics: topics.clone(),
            ..Default::default()
        }],
        v3_and_below_transactional_id: transactional_id.into(),
        v3_and_below_producer_id: producer.id.0,
        v3_and_below_producer_epoch: producer.epoch.0,
        v3_and_below_topics: topics,
        ..Default::default()
    }
}

pub(crate) fn assert_partition_added(response: &AddPartitionsToTxnResponse) {
    let code = partition_error(response, false);
    assert!(code == 0, "AddPartitionsToTxn: {response:?}");
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct TransactionProduceSetup<'a> {
    #[default("transaction")]
    pub transactional_id: &'a str,
    #[default("orders")]
    pub topic: &'a str,
    pub topic_id: Uuid,
    pub producer: Option<ProducerIdentity>,
    #[default(&["v"])]
    pub values: &'a [&'static str],
}

pub(crate) fn produce_request(setup: TransactionProduceSetup<'_>) -> ProduceRequest {
    let TransactionProduceSetup {
        transactional_id,
        topic,
        topic_id,
        producer,
        values,
    } = setup;
    let batch = records_batch(
        producer.map(|identity| (identity.id.0, identity.epoch.0)),
        values,
    );
    ProduceRequest {
        // KIP-890 verification needs the transactional id for transactional batches.
        transactional_id: producer.map(|_| transactional_id.to_string()),
        ..batch_request(
            batch,
            SinglePartitionProduceSetup {
                topic: (topic).into(),
                topic_id,
                ..SinglePartitionProduceSetup::replicated()
            },
        )
    }
}

pub(crate) fn records_batch(producer: Option<(i64, i16)>, values: &[&'static str]) -> RecordBatch {
    let records = i32::try_from(values.len()).expect("record count");
    RecordBatch {
        attributes: Attributes::default().with_transactional(producer.is_some()),
        producer_id: producer.map_or(-1, |(producer_id, _)| producer_id),
        producer_epoch: producer.map_or(-1, |(_, epoch)| epoch),
        base_sequence: if producer.is_some() { 0 } else { -1 },
        last_offset_delta: records - 1,
        max_timestamp: 1,
        ..batch_from_records(
            values
                .iter()
                .zip(0..)
                .map(|(value, offset_delta)| {
                    value_record(offset_delta, Some(Bytes::from_static(value.as_bytes())))
                })
                .collect(),
        )
    }
}

/// Read the first partition error, with the legacy fallback only in fixtures that originally used it.
pub(crate) fn partition_error(response: &AddPartitionsToTxnResponse, legacy: bool) -> i16 {
    let topic = response
        .results_by_transaction
        .first()
        .and_then(|transaction| transaction.topic_results.first());
    let topic = if legacy {
        topic.or(response.results_by_topic_v3_and_below.first())
    } else {
        topic
    };
    topic
        .and_then(|topic| topic.results_by_partition.first())
        .map_or(response.error_code, |row| row.partition_error_code)
}

use krabka_protocol::owned::create_topics_request::CreatableTopicConfig;

#[derive(krabka_macros::FieldDefaults)]
pub struct TransactionTopicSetup<'a> {
    #[default("orders")]
    pub name: &'a str,
    #[default(crate::support::topics::TopicPartitionCount(1))]
    pub partitions: TopicPartitionCount,
    pub configs: Vec<CreatableTopicConfig>,
    #[default("CreateTopics")]
    pub context: &'a str,
}

pub async fn create_topic(client: &krabka_client_core::Client, setup: TransactionTopicSetup<'_>) {
    let TransactionTopicSetup {
        name,
        partitions,
        configs,
        context,
    } = setup;
    let response = client
        .send(create_topic_request(
            krabka_protocol::owned::create_topics_request::CreatableTopic {
                configs,
                ..creatable_topic(ConfiguredTopicSetup {
                    name: (name).into(),
                    partitions: TopicPartitionCount(partitions.0),
                    ..Default::default()
                })
            },
        ))
        .await
        .unwrap();
    assert!(
        response.topics[0].error_code == 0 || response.topics[0].error_code == 36,
        "{context} {name}: error_code={}",
        response.topics[0].error_code
    );
}

/// Loading answers are shared by the coordinator readiness probes, not their response oracles.
pub(crate) fn coordinator_loading(code: i16) -> bool {
    matches!(code, 14..=16)
}

/// Poll the RPC answer: coordinator loading has no materialization awaiter.
pub(crate) async fn retry_coordinator<R, F: std::future::Future<Output = R>>(
    mut send: impl FnMut() -> F,
    code: impl Fn(&R) -> i16,
    retriable: impl Fn(i16) -> bool,
    timeout: std::time::Duration,
) -> R {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let response = send().await;
        if retriable(code(&response)) && std::time::Instant::now() < deadline {
            // Intentional: only the RPC answer signals coordinator readiness.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            continue;
        }
        return response;
    }
}

pub(crate) async fn initialize_producer<F>(
    send: impl FnMut() -> F,
    retriable: impl Fn(i16) -> bool,
    timeout: std::time::Duration,
) -> krabka_protocol::owned::init_producer_id_response::InitProducerIdResponse
where
    F: std::future::Future<
            Output = krabka_protocol::owned::init_producer_id_response::InitProducerIdResponse,
        >,
{
    let response =
        retry_coordinator(send, |response| response.error_code, retriable, timeout).await;
    assert!(response.error_code == 0, "InitProducerId: {response:?}");
    response
}

/// The image watch has no awaiter for any leader, so preserve the bounded 100ms probe.
pub(crate) async fn partition_leader(
    handle: &krabka_broker::BrokerHandle,
    topic: &str,
    timeout: std::time::Duration,
) -> u64 {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Some(leader) = handle.partition_leader_for_test(topic, 0) {
            return leader;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{topic}-0 has no leader"
        );
        // Intentional: the image watch has no awaiter for any leader.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// The same Produce success oracle for client-routed and direct-connection requests.
pub(crate) async fn produce_succeeds<E: std::fmt::Debug>(
    response: impl std::future::Future<
        Output = Result<krabka_protocol::owned::produce_response::ProduceResponse, E>,
    >,
) {
    let response = response.await.expect("Produce");
    let code = response.responses[0].partition_responses[0].error_code;
    assert!(code == 0, "Produce: {response:?}");
}

/// Send the fixture through either a client or a direct connection, keeping the success oracle.
pub(crate) async fn produce_fixture<E: std::fmt::Debug, F>(
    setup: TransactionProduceSetup<'_>,
    send: impl FnOnce(ProduceRequest) -> F,
) where
    F: std::future::Future<
            Output = Result<krabka_protocol::owned::produce_response::ProduceResponse, E>,
        >,
{
    produce_succeeds(send(produce_request(setup))).await;
}

pub(crate) async fn partition_added<E: std::fmt::Debug>(
    response: impl std::future::Future<
        Output = Result<
            krabka_protocol::owned::add_partitions_to_txn_response::AddPartitionsToTxnResponse,
            E,
        >,
    >,
) {
    assert_partition_added(&response.await.expect("AddPartitionsToTxn"));
}

pub(crate) async fn initialized_identity<F>(
    send: impl FnMut() -> F,
    retriable: impl Fn(i16) -> bool,
    timeout: std::time::Duration,
) -> (i64, i16)
where
    F: std::future::Future<
            Output = krabka_protocol::owned::init_producer_id_response::InitProducerIdResponse,
        >,
{
    let response = initialize_producer(send, retriable, timeout).await;
    (response.producer_id, response.producer_epoch)
}

/// Only the three original metadata/leadership readiness errors trigger another Produce.
pub(crate) async fn settled_produce(
    client: &krabka_client_core::Client,
    request: ProduceRequest,
    timeout: std::time::Duration,
) -> krabka_protocol::owned::produce_response::ProduceResponse {
    retry_coordinator(
        || async { client.send(request.clone()).await.expect("Produce") },
        |response| response.responses[0].partition_responses[0].error_code,
        |code| matches!(code, 6 | 3 | 100),
        timeout,
    )
    .await
}

#[derive(krabka_macros::FieldDefaults)]
pub(crate) struct AssignedTransactionTopicSetup<'a> {
    pub topic: AssignedTopicSetup,
    pub context: Option<&'a str>,
}

pub(crate) async fn create_assigned_topic(
    client: &krabka_client_core::Client,
    setup: AssignedTransactionTopicSetup<'_>,
) -> Uuid {
    let created = client
        .send(create_topic_request(assigned_topic(setup.topic)))
        .await
        .expect("CreateTopics");
    match setup.context {
        Some(context) => assert!(created.topics[0].error_code == 0, "{context}: {created:?}"),
        None => assert!(created.topics[0].error_code == 0, "{created:?}"),
    }
    created.topics[0].topic_id
}

/// Retain the 200ms polls and check completion before each poll, including the first.
pub(crate) async fn poll_values_until(
    consumer: &mut Consumer,
    timeout: Duration,
    done: impl Fn(&[String]) -> bool,
) -> Vec<String> {
    poll_values(consumer, timeout, done, Some("poll")).await
}

async fn poll_values(
    consumer: &mut Consumer,
    timeout: Duration,
    done: impl Fn(&[String]) -> bool,
    context: Option<&str>,
) -> Vec<String> {
    let mut seen = Vec::new();
    let deadline = Instant::now() + timeout;
    while !done(&seen) && Instant::now() < deadline {
        let records = consumer.poll(krabka_units::millis(200)).await;
        let records = match context {
            Some(context) => records.expect(context),
            None => records.unwrap(),
        };
        for record in records {
            seen.push(String::from_utf8_lossy(record.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    seen
}

pub(crate) async fn read_committed_through(
    bootstrap: &str,
    topic: &str,
    last: &str,
    timeout: Duration,
) -> Vec<String> {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.to_string())
        .group_id(format!("{topic}-reader"))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe([topic.to_string()])
        .build()
        .await
        .expect("consumer");
    let seen = poll_values_until(&mut consumer, timeout, |seen| {
        seen.last().map(String::as_str) == Some(last)
    })
    .await;
    consumer.close().await.expect("close consumer");
    seen
}

use krabka_client_core::security::ClientSecurity;

#[derive(krabka_macros::FieldDefaults)]
pub(crate) struct TransactionConsumerSetup<'a> {
    #[default("transaction-reader".into())]
    pub group: String,
    #[default("orders")]
    pub topic: &'a str,
    #[default(IsolationLevel::ReadCommitted)]
    pub isolation: IsolationLevel,
    pub security: Option<ClientSecurity>,
}

pub(crate) async fn consumer(
    bootstrap: impl Into<String>,
    setup: TransactionConsumerSetup<'_>,
) -> Result<Consumer, krabka_client_consumer::ConsumerError> {
    let TransactionConsumerSetup {
        group,
        topic,
        isolation,
        security,
    } = setup;
    let builder = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id(group)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(isolation)
        .subscribe([topic.to_string()]);
    match security {
        Some(security) => builder.security(security).build().await,
        None => builder.build().await,
    }
}

/// Common committed-record probe; callers retain the exact value oracle and teardown order.
pub(crate) async fn committed_values(
    bootstrap: impl Into<String>,
    group: impl Into<String>,
    topic: &str,
    security: Option<krabka_client_core::security::ClientSecurity>,
) -> (Consumer, Vec<String>) {
    observed_values(
        bootstrap,
        group,
        topic,
        (IsolationLevel::ReadCommitted, security),
        Duration::from_secs(10),
        |seen| seen.len() >= 3,
        Some("poll"),
    )
    .await
}

/// Poll the same consumer while leaving value expectations and close order with the scenario.
pub(crate) async fn observed_values(
    bootstrap: impl Into<String>,
    group: impl Into<String>,
    topic: &str,
    (isolation, security): (
        IsolationLevel,
        Option<krabka_client_core::security::ClientSecurity>,
    ),
    timeout: Duration,
    done: impl Fn(&[String]) -> bool,
    context: Option<&str>,
) -> (Consumer, Vec<String>) {
    let mut consumer = consumer(
        bootstrap,
        TransactionConsumerSetup {
            group: group.into(),
            topic,
            isolation,
            security,
        },
    )
    .await
    .unwrap();
    let seen = poll_values(&mut consumer, timeout, done, context).await;
    (consumer, seen)
}

/// Initialize a transactional identity through the ordinary client with the caller's exact retry policy.
pub(crate) async fn initialize_transactional(
    client: &krabka_client_core::Client,
    transactional_id: &str,
    retriable: impl Fn(i16) -> bool,
    timeout: Duration,
) -> krabka_protocol::owned::init_producer_id_response::InitProducerIdResponse {
    initialize_producer(
        || async {
            client
                .send(init_producer_request(transactional_id))
                .await
                .expect("InitProducerId")
        },
        retriable,
        timeout,
    )
    .await
}

/// The plain read-committed probes keep their original ten-second bound and unwrap diagnostic.
pub(crate) async fn read_committed_at_least(
    bootstrap: impl Into<String>,
    group: impl Into<String>,
    topic: &str,
    records: usize,
) -> (Consumer, Vec<String>) {
    observed_values(
        bootstrap,
        group,
        topic,
        (IsolationLevel::ReadCommitted, None),
        Duration::from_secs(10),
        |seen| seen.len() >= records,
        None,
    )
    .await
}
