//! Membership and record fixtures for share-group integration scenarios.
use {
    crate::support::{
        client::{connect as connect_client, create_topic_with},
        configs::{incremental_config, incremental_request, incremental_resource},
        records::{batch_from_records, value_record},
        topics::{CreateTopicSetup, TopicPartitionCount},
    },
    assert2::assert,
    krabka_broker::BrokerConfig,
    krabka_client_core::Client,
    krabka_protocol::{
        owned::{
            produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
            share_acknowledge_request::{
                AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch as AckAckBatch,
                ShareAcknowledgeRequest,
            },
            share_acknowledge_response::ShareAcknowledgeResponse,
            share_fetch_request::{
                AcknowledgementBatch as FetchAckBatch, FetchPartition, FetchTopic, ForgottenTopic,
                ShareFetchRequest,
            },
            share_group_heartbeat_request::ShareGroupHeartbeatRequest,
        },
        primitives::uuid::Uuid as WireUuid,
        records::{Record, RecordBatch},
    },
    std::{
        sync::{Arc, OnceLock},
        time::Duration,
    },
};
const SHARE_STATE_PARTITIONS: i32 = 1;
const MAX_CONCURRENT_TEST_BROKERS: usize = 3;
const RESOURCE_TYPE_GROUP: i8 = 32;

pub type ShareBrokerFixture = (
    tokio::sync::OwnedSemaphorePermit,
    krabka_broker::BrokerHandle,
    Arc<Client>,
    tempfile::TempDir,
    uuid::Uuid,
    String,
);

pub fn topic_id(broker: &krabka_broker::BrokerHandle, topic: &str) -> uuid::Uuid {
    let image = broker.controller_image_for_test();
    image
        .topic(topic)
        .map(|t| *t.topic_id.as_bytes())
        .map(uuid::Uuid::from_bytes)
        .expect("topic present in image")
}

pub fn wire(tid: uuid::Uuid) -> WireUuid { WireUuid(*tid.as_bytes()) }

pub async fn broker_test_permit() -> tokio::sync::OwnedSemaphorePermit {
    static GATE: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();

    Arc::clone(
        GATE.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_TEST_BROKERS))),
    )
    .acquire_owned()
    .await
    .expect("broker test concurrency gate remains open")
}

pub fn broker_config(log_dir: std::path::PathBuf) -> BrokerConfig {
    let mut config = BrokerConfig::for_tests(log_dir);
    config.share_coordinator.state_topic_num_partitions = SHARE_STATE_PARTITIONS;
    config
}

pub async fn bootstrap_share_state(
    broker: &krabka_broker::BrokerHandle,
    client: &Client,
    group: &str,
) {
    broker.wait_until_group_coordinator_ready().await;
    broker.wait_until_share_coordinator_ready().await;
    set_auto_offset_reset_earliest(client, group).await;
}

pub async fn set_auto_offset_reset_earliest(client: &Client, group: &str) {
    let resp = client
        .send(
            incremental_request()
                .resources(vec![incremental_resource(
                    RESOURCE_TYPE_GROUP,
                    group,
                    vec![incremental_config(
                        "share.auto.offset.reset",
                        Some("earliest".into()),
                        crate::support::configs::ConfigOperation::Set,
                    )],
                )])
                .call(),
        )
        .await
        .expect("IncrementalAlterConfigs(group)");
    assert!(
        resp.responses[0].error_code == 0,
        "share.auto.offset.reset=earliest rejected: {:?}",
        resp.responses[0].error_message
    );
}

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
pub struct ShareRecordCount(pub i64);

impl Default for ShareRecordCount {
    fn default() -> Self { Self(1) }
}

#[bon::builder]
pub async fn produce_n(
    client: &Client,
    #[builder(default = "t")] topic: &'_ str,
    #[builder(default)] topic_id: uuid::Uuid,
    #[builder(into, default)] partition: krabka_ids::PartitionIndex,
    #[builder(into, default)] count: ShareRecordCount,
) {
    let values = (0..count.0)
        .map(|i| bytes::Bytes::from(format!("v{i}")))
        .collect();
    produce_values()
        .client(client)
        .values(values)
        .topic(topic)
        .topic_id(topic_id)
        .partition(partition.0)
        .call()
        .await;
}

#[bon::builder]
pub async fn produce_values(
    client: &Client,
    values: Vec<bytes::Bytes>,
    #[builder(default = "t")] topic: &'_ str,
    #[builder(default)] topic_id: uuid::Uuid,
    #[builder(into, default)] partition: krabka_ids::PartitionIndex,
) {
    let n = i64::try_from(values.len()).unwrap();
    let records: Vec<Record> = values
        .iter()
        .enumerate()
        .map(|(i, value)| value_record(i32::try_from(i).unwrap(), Some(value.clone())))
        .collect();
    produce_batch()
        .client(client)
        .batch(RecordBatch {
            last_offset_delta: i32::try_from(n - 1).unwrap(),
            ..batch_from_records(records)
        })
        .topic(topic)
        .topic_id(topic_id)
        .partition(partition)
        .call()
        .await;
}
pub async fn join(client: &Client, group: &str, topic: &str) -> (String, i32) {
    let resp = client
        .send(ShareGroupHeartbeatRequest {
            group_id: group.into(),
            member_id: uuid::Uuid::new_v4().to_string(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec![topic.into()]),
            ..Default::default()
        })
        .await
        .expect("ShareGroupHeartbeat");
    assert!(resp.error_code == 0, "join failed: {:?}", resp.error_code);
    let member_id = resp.member_id.expect("the broker echoes the member id");
    let member_epoch = resp.member_epoch;
    (member_id, member_epoch)
}

/// Produce a supplied batch, preserving its timestamps and record geometry on retries.
#[bon::builder]
pub async fn produce_batch(
    client: &Client,
    batch: RecordBatch,
    #[builder(default = "t")] topic: &'_ str,
    #[builder(default)] topic_id: uuid::Uuid,
    #[builder(into, default)] partition: krabka_ids::PartitionIndex,
) {
    let partition = partition.0;
    for _ in 0..40 {
        let resp = client
            .send(ProduceRequest {
                transactional_id: None,
                acks: -1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: topic.to_string(),
                    // Produce negotiates v13, which carries topic_id (not name)
                    // on the wire; the broker resolves the topic by id.
                    topic_id: wire(topic_id),
                    partition_data: vec![PartitionProduceData {
                        index: partition,
                        records: Some(batch.clone().into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("Produce");
        let p = &resp.responses[0].partition_responses[0];
        // 3 = UNKNOWN_TOPIC_OR_PARTITION, 6 = NOT_LEADER_OR_FOLLOWER.
        if p.error_code == 0 {
            return;
        }
        if p.error_code == 3 || p.error_code == 6 {
            // intentional: bounded produce-retry backoff while the partition
            // leader materializes; this helper has no BrokerHandle to await on
            // and mirrors a real producer's retry.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        panic!("produce failed: {p:?}");
    }
    panic!("partition never became produceable for {topic}:{partition}");
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct ShareTopicSetup<'a> {
    #[default("t")]
    pub topic: &'a str,
    pub partitions: TopicPartitionCount,
}

impl<'a> From<ShareTopicSetup<'a>> for CreateTopicSetup<'a> {
    fn from(setup: ShareTopicSetup<'a>) -> Self {
        Self {
            topic: setup.topic,
            num_partitions: setup.partitions,
            ..Default::default()
        }
    }
}

/// A broker, client and materialized data topic, with the directory behind it.
pub struct TopicFixture {
    pub broker: krabka_broker::BrokerHandle,
    pub client: Arc<Client>,
    pub dir: tempfile::TempDir,
    pub tid: uuid::Uuid,
}

/// A broker, client and materialized data topic, retaining the fixture directory.
#[bon::builder]
pub async fn topic_fixture(
    #[builder(default = "t")] topic: &'_ str,
    #[builder(default)] partitions: TopicPartitionCount,
    customize: impl FnOnce(&mut BrokerConfig),
) -> TopicFixture {
    let setup = ShareTopicSetup { topic, partitions };
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = broker_config(dir.path().to_path_buf());
    customize(&mut config);
    let (broker, client, tid) = start_topic()
        .cfg(config)
        .topic(setup.topic)
        .partitions(setup.partitions)
        .call()
        .await;
    TopicFixture {
        broker,
        client,
        dir,
        tid,
    }
}

/// A topic fixture that holds this binary's broker permit for the test's lifetime.
fn no_share_customization(_: &mut BrokerConfig) {}
pub type ShareConfigCustomization = Box<dyn FnOnce(&mut BrokerConfig)>;

#[bon::builder]
pub async fn permitted_topic_fixture(
    #[builder(default = "t")] topic: &'_ str,
    #[builder(default)] partitions: TopicPartitionCount,
    #[builder(default = Box::new(no_share_customization) as ShareConfigCustomization)]
    customize: ShareConfigCustomization,
) -> (
    tokio::sync::OwnedSemaphorePermit,
    krabka_broker::BrokerHandle,
    Arc<Client>,
    tempfile::TempDir,
    uuid::Uuid,
) {
    let setup = ShareTopicSetup { topic, partitions };
    let permit = broker_test_permit().await;
    let TopicFixture {
        broker,
        client,
        dir,
        tid,
    } = Box::pin(
        topic_fixture()
            .topic(setup.topic)
            .partitions(setup.partitions)
            .customize(customize)
            .call(),
    )
    .await;
    (permit, broker, client, dir, tid)
}

pub async fn connect(bootstrap: &str) -> Arc<Client> { connect_client(bootstrap, "c1").await }
#[bon::builder]
pub async fn create_topic(
    broker: &krabka_broker::BrokerHandle,
    client: &Client,
    #[builder(default = "t")] topic: &'_ str,
    #[builder(default)] partitions: TopicPartitionCount,
) {
    let setup = ShareTopicSetup { topic, partitions };
    create_topic_with(client, setup.into()).await;
    broker.wait_until_partition_present(setup.topic, 0).await;
}

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
pub struct ShareSessionEpoch(pub i32);

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
pub struct FetchByteLimit(pub i32);

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
pub struct RequestWaitMillis(pub i32);

#[derive(Clone, Copy)]
pub struct AcknowledgementCode(pub i8);

#[derive(Clone, Copy, Default)]
pub enum RenewalMode {
    #[default]
    ProtocolDefault,
    Settle,
    Renew,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct ShareSessionSetup<'a> {
    #[default("g1")]
    pub group: &'a str,
    #[default("member")]
    pub member: &'a str,
    #[default(uuid::Uuid::from_u128(1))]
    pub topic_id: uuid::Uuid,
    pub partition: krabka_ids::PartitionIndex,
    #[default(ShareSessionEpoch(1))]
    pub epoch: ShareSessionEpoch,
}

impl<'a> ShareSessionSetup<'a> {
    pub fn with_epoch(mut self, epoch: ShareSessionEpoch) -> Self {
        self.epoch = epoch;
        self
    }

    pub fn fetch_at(self, epoch: ShareSessionEpoch) -> ShareFetchSetup<'a> {
        self.with_epoch(epoch).into()
    }

    pub fn joined(member: &'a str, topic_id: uuid::Uuid) -> Self {
        Self {
            member,
            topic_id,
            ..Default::default()
        }
    }

    pub fn opening(member: &'a str, topic_id: uuid::Uuid) -> Self {
        Self::at_epoch(member, topic_id, ShareSessionEpoch(0))
    }

    pub fn at_epoch(member: &'a str, topic_id: uuid::Uuid, epoch: ShareSessionEpoch) -> Self {
        Self {
            member,
            topic_id,
            epoch,
            ..Default::default()
        }
    }
}

#[derive(krabka_macros::FieldDefaults)]
pub struct ShareFetchSetup<'a> {
    pub session: ShareSessionSetup<'a>,
    #[default(RequestWaitMillis(0))]
    pub max_wait: RequestWaitMillis,
    pub acks: Vec<FetchAckBatch>,
}

impl<'a> ShareFetchSetup<'a> {
    pub fn joined(member: &'a str, topic_id: uuid::Uuid) -> Self {
        ShareSessionSetup::joined(member, topic_id).into()
    }

    pub fn at_epoch(member: &'a str, topic_id: uuid::Uuid, epoch: ShareSessionEpoch) -> Self {
        ShareSessionSetup::at_epoch(member, topic_id, epoch).into()
    }
}

impl<'a> From<ShareSessionSetup<'a>> for ShareFetchSetup<'a> {
    fn from(session: ShareSessionSetup<'a>) -> Self {
        Self {
            session,
            ..Default::default()
        }
    }
}

#[bon::builder]
pub fn share_fetch_req(
    #[builder(default)] session: ShareSessionSetup<'_>,
    #[builder(into, default = RequestWaitMillis (0))] max_wait: RequestWaitMillis,
    #[builder(default)] acks: Vec<FetchAckBatch>,
) -> ShareFetchRequest {
    let setup = ShareFetchSetup {
        session,
        max_wait,
        acks,
    };
    let ShareFetchSetup {
        session:
            ShareSessionSetup {
                group,
                member,
                topic_id: tid,
                partition,
                epoch,
            },
        max_wait,
        acks,
    } = setup;
    ShareFetchRequest {
        group_id: Some(group.into()),
        member_id: Some(member.into()),
        share_session_epoch: epoch.0,
        max_wait_ms: max_wait.0,
        min_bytes: 1,
        max_bytes: 1 << 20,
        max_records: 500,
        batch_size: 500,
        share_acquire_mode: 0,
        is_renew_ack: false,
        topics: vec![FetchTopic {
            topic_id: wire(tid),
            partitions: vec![FetchPartition {
                partition_index: partition.0,
                partition_max_bytes: 1 << 20,
                acknowledgement_batches: acks,
                ..Default::default()
            }],
            ..Default::default()
        }],
        forgotten_topics_data: vec![],
        ..Default::default()
    }
}
pub fn acquired_count(p: &krabka_protocol::owned::share_fetch_response::PartitionData) -> i64 {
    p.acquired_records
        .iter()
        .map(|r| r.last_offset - r.first_offset + 1)
        .sum()
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct ShareAck<'a> {
    pub session: ShareSessionSetup<'a>,
    pub first: krabka_ids::Offset,
    pub last: krabka_ids::Offset,
    #[default(AcknowledgementCode(1))]
    pub ack_type: AcknowledgementCode,
}

#[bon::bon]
impl<'a> ShareAck<'a> {
    pub fn single_for(member: &'a str, topic_id: uuid::Uuid, offset: krabka_ids::Offset) -> Self {
        Self::prefix_for(member, topic_id, offset).starting_at(offset)
    }

    pub fn prefix_for(member: &'a str, topic_id: uuid::Uuid, last: krabka_ids::Offset) -> Self {
        Self::prefix()
            .member(member)
            .topic_id(topic_id)
            .last(last.0)
            .call()
    }

    pub fn release(mut self) -> Self {
        self.ack_type = AcknowledgementCode(2);
        self
    }

    pub fn reject(mut self) -> Self {
        self.ack_type = AcknowledgementCode(3);
        self
    }

    pub fn starting_at(mut self, first: krabka_ids::Offset) -> Self {
        self.first = first;
        self
    }

    #[builder]
    pub fn prefix(
        #[builder(default = "g1")] group: &'a str,
        #[builder(default = "member")] member: &'a str,
        #[builder(default = uuid :: Uuid :: from_u128 (1))] topic_id: uuid::Uuid,
        #[builder(into, default)] partition: krabka_ids::PartitionIndex,
        #[builder(into, default = ShareSessionEpoch (1))] epoch: ShareSessionEpoch,
        #[builder(into)] last: krabka_ids::Offset,
    ) -> Self {
        let session = ShareSessionSetup {
            group,
            member,
            topic_id,
            partition,
            epoch,
        };
        Self {
            session,
            last,
            ..Default::default()
        }
    }
}

pub async fn share_ack(
    client: &Client,
    ack: ShareAck<'_>,
) -> krabka_protocol::owned::share_acknowledge_response::PartitionData {
    let count = usize::try_from(ack.last.0 - ack.first.0 + 1).unwrap();
    let req = acknowledge_request()
        .maybe_group_id(Some(Some(ack.session.group.into())))
        .maybe_member_id(Some(Some(ack.session.member.into())))
        .epoch(ack.session.epoch.0)
        .mode(RenewalMode::Settle)
        .topics(vec![acknowledge_topic(wire(ack.session.topic_id), vec![
            acknowledge_partition(ack.session.partition.0, vec![acknowledgement(
                ack.first.0,
                ack.last.0,
                vec![ack.ack_type.0; count],
            )]),
        ])])
        .call();
    let resp: ShareAcknowledgeResponse = client.send(req).await.expect("ShareAcknowledge");
    assert!(
        resp.error_code == 0,
        "ShareAcknowledge top-level error: {}",
        resp.error_code
    );
    resp.responses[0].partitions[0].clone()
}

#[derive(Clone, Copy)]
pub enum FetchSessionMode {
    Initial,
    Incremental,
}

/// Send an acknowledgement whose partition is expected to succeed.
pub async fn acknowledge_success(client: &Client, setup: ShareAck<'_>) {
    let row = share_ack(client, setup).await;
    assert!(row.error_code == 0, "share acknowledgement failed: {row:?}");
}

/// Fetch one row, allowing omitted empty partitions only for incremental sessions.
pub async fn fetch_row(
    client: &Client,
    req: ShareFetchRequest,
    mode: FetchSessionMode,
) -> krabka_protocol::owned::share_fetch_response::PartitionData {
    let partition = req.topics[0].partitions[0].partition_index;
    let resp: krabka_protocol::owned::share_fetch_response::ShareFetchResponse =
        client.send(req).await.expect("ShareFetch");
    assert!(
        resp.error_code == 0,
        "ShareFetch top-level error: {}",
        resp.error_code
    );
    if matches!(mode, FetchSessionMode::Incremental) {
        resp.responses
            .first()
            .and_then(|t| t.partitions.first())
            .cloned()
            .unwrap_or_else(
                || krabka_protocol::owned::share_fetch_response::PartitionData {
                    partition_index: partition,
                    ..Default::default()
                },
            )
    } else {
        resp.responses[0].partitions[0].clone()
    }
}

/// The fetch itself acquires records, so poll the same session-opening RPC.
pub async fn fetch_until_acquired(
    client: &Client,
    req: ShareFetchRequest,
    mode: FetchSessionMode,
) -> krabka_protocol::owned::share_fetch_response::PartitionData {
    for _ in 0..40 {
        let row = fetch_row(client, req.clone(), mode).await;
        if row.error_code == 0 && acquired_count(&row) > 0 {
            return row;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("share fetch never acquired any records: {req:?}");
}

/// Start a data topic on a caller-owned broker config (including restart fixtures).
#[bon::builder]
pub async fn start_topic(
    cfg: BrokerConfig,
    #[builder(default = "t")] topic: &'_ str,
    #[builder(default)] partitions: TopicPartitionCount,
) -> (krabka_broker::BrokerHandle, Arc<Client>, uuid::Uuid) {
    let setup = ShareTopicSetup { topic, partitions };
    // Keep the growing broker config inside a boxed startup future.
    let broker = Box::pin(krabka_broker::Broker::start(cfg)).await.unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic()
        .broker(&broker)
        .client(&client)
        .topic(setup.topic)
        .partitions(setup.partitions)
        .call()
        .await;
    let tid = topic_id(&broker, setup.topic);
    (broker, client, tid)
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct StartGroupTopicSetup<'a> {
    pub topic: ShareTopicSetup<'a>,
    #[default("g1")]
    pub group: &'a str,
}

/// A running broker, a client connected to it and the id of the data topic it
/// hosts.
pub struct StartedShareTopic {
    pub broker: krabka_broker::BrokerHandle,
    pub client: Arc<Client>,
    pub topic_id: uuid::Uuid,
}

/// Bootstrap the share-state topic after creating the requested data topic.
#[bon::builder]
pub async fn start_group_topic(
    config: BrokerConfig,
    #[builder(default)] topic: ShareTopicSetup<'_>,
    #[builder(default = "g1")] group: &'_ str,
) -> StartedShareTopic {
    let setup = StartGroupTopicSetup { topic, group };
    let (broker, client, topic_id) = Box::pin(
        start_topic()
            .cfg(config)
            .topic(setup.topic.topic)
            .partitions(setup.topic.partitions)
            .call(),
    )
    .await;
    bootstrap_share_state(&broker, &client, setup.group).await;
    StartedShareTopic {
        broker,
        client,
        topic_id,
    }
}

pub async fn crash_follower(
    cluster: &mut Vec<(krabka_broker::BrokerHandle, BrokerConfig, tempfile::TempDir)>,
    coordinator: u64,
) -> tempfile::TempDir {
    let raft_leader = cluster[0].0.wait_until_controller_leader().await.0;
    let follower = cluster
        .iter()
        .position(|(h, _, _)| h.node_id() != coordinator && h.node_id() != raft_leader)
        .unwrap_or_else(|| {
            cluster
                .iter()
                .position(|(h, _, _)| h.node_id() != coordinator)
                .expect("a follower")
        });
    let (stopped, _, dir) = cluster.remove(follower);
    stopped.crash_for_test().await;
    dir
}

/// Rejoin a persisted share fixture and wait for its group/coordinator bootstrap.
///
/// # Panics
/// Panics if the broker or client cannot restart from the supplied directory.
pub async fn rejoin_group(
    log_dir: std::path::PathBuf,
    group: &str,
) -> (krabka_broker::BrokerHandle, Arc<Client>) {
    let mut cfg = broker_config(log_dir);
    cfg.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    let broker = krabka_broker::Broker::start(cfg).await.unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    bootstrap_share_state(&broker, &client, group).await;
    (broker, client)
}

/// The URL-safe, unpadded coordinator key for a share partition.
pub fn coordinator_key(group: &str, tid: uuid::Uuid, partition: i32) -> String {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    format!(
        "{group}:{}:{partition}",
        URL_SAFE_NO_PAD.encode(tid.as_bytes())
    )
}

/// Present record values in batch order, preserving the share fixtures' lossy UTF-8 decoding.
pub fn record_values(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| batch.records.iter())
        .filter_map(|record| record.value.as_ref())
        .map(|value| String::from_utf8_lossy(value).into_owned())
        .collect()
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct RetryEpochs {
    #[default(ShareSessionEpoch(3))]
    pub start: ShareSessionEpoch,
    #[default(ShareSessionEpoch(18))]
    pub end: ShareSessionEpoch,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct RefetchSetup<'a> {
    pub session: ShareSessionSetup<'a>,
    pub epochs: RetryEpochs,
}

/// Retry empty acquisitions over explicit session epochs, retaining 100ms spacing.
#[bon::builder]
pub async fn refetch_while_empty(
    client: &Client,
    mut row: krabka_protocol::owned::share_fetch_response::PartitionData,
    #[builder(default)] session: ShareSessionSetup<'_>,
    #[builder(default)] epochs: RetryEpochs,
) -> krabka_protocol::owned::share_fetch_response::PartitionData {
    let setup = RefetchSetup { session, epochs };
    for epoch in setup.epochs.start.0..setup.epochs.end.0 {
        if acquired_count(&row) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        row = fetch_row(
            client,
            share_fetch_req()
                .session(setup.session.fetch_at(ShareSessionEpoch(epoch)).session)
                .call(),
            FetchSessionMode::Incremental,
        )
        .await;
    }
    row
}

/// An empty incremental session request with explicit forgotten topics and byte budget.
#[derive(krabka_macros::FieldDefaults)]
pub struct EmptySessionSetup {
    #[default("g1".into())]
    pub group: String,
    #[default("member".into())]
    pub member: String,
    #[default(ShareSessionEpoch(1))]
    pub epoch: ShareSessionEpoch,
    #[default(FetchByteLimit(1 << 20))]
    pub max_bytes: FetchByteLimit,
    pub forgotten_topics_data: Vec<ForgottenTopic>,
}

#[bon::builder]
pub fn empty_session_request(
    #[builder(default = "g1" . into ())] group: String,
    #[builder(default = "member" . into ())] member: String,
    #[builder(into, default = ShareSessionEpoch (1))] epoch: ShareSessionEpoch,
    #[builder(into, default = FetchByteLimit (1 << 20))] max_bytes: FetchByteLimit,
    #[builder(default)] forgotten_topics_data: Vec<ForgottenTopic>,
) -> ShareFetchRequest {
    let setup = EmptySessionSetup {
        group,
        member,
        epoch,
        max_bytes,
        forgotten_topics_data,
    };
    let EmptySessionSetup {
        group,
        member,
        epoch,
        max_bytes,
        forgotten_topics_data,
    } = setup;
    ShareFetchRequest {
        group_id: Some(group),
        member_id: Some(member),
        share_session_epoch: epoch.0,
        max_wait_ms: 0,
        min_bytes: 1,
        max_bytes: max_bytes.0,
        max_records: 500,
        batch_size: 500,
        topics: vec![],
        forgotten_topics_data,
        ..Default::default()
    }
}

/// Generate a typed first-acquisition driver with explicit incremental-session behavior.
#[macro_export]
macro_rules! share_first_fetch_fixture {
    ($name:ident, $mode:ident) => {
        pub async fn $name(
            client: &::krabka_client_core::Client,
            session: $crate::support::share::ShareSessionSetup<'_>,
        ) -> ::krabka_protocol::owned::share_fetch_response::PartitionData {
            $crate::support::share::fetch_until_acquired(
                client,
                $crate::support::share::share_fetch_req()
                    .session(session)
                    .call(),
                $crate::support::share::FetchSessionMode::$mode,
            )
            .await
        }
    };
}

pub fn acknowledgement(
    first_offset: i64,
    last_offset: i64,
    acknowledge_types: Vec<i8>,
) -> AckAckBatch {
    AckAckBatch {
        first_offset,
        last_offset,
        acknowledge_types,
        ..Default::default()
    }
}

pub fn acknowledge_partition(
    partition_index: i32,
    acknowledgement_batches: Vec<AckAckBatch>,
) -> AcknowledgePartition {
    AcknowledgePartition {
        partition_index,
        acknowledgement_batches,
        ..Default::default()
    }
}

pub fn acknowledge_topic(
    topic_id: WireUuid,
    partitions: Vec<AcknowledgePartition>,
) -> AcknowledgeTopic {
    AcknowledgeTopic {
        topic_id,
        partitions,
        ..Default::default()
    }
}

#[derive(krabka_macros::FieldDefaults)]
pub struct AcknowledgeRequestSetup {
    #[default(Some("g1".into()))]
    pub group_id: Option<String>,
    #[default(Some("member".into()))]
    pub member_id: Option<String>,
    #[default(ShareSessionEpoch(1))]
    pub epoch: ShareSessionEpoch,
    pub mode: RenewalMode,
    pub topics: Vec<AcknowledgeTopic>,
}

#[bon::builder]
pub fn acknowledge_request(
    #[builder(required, default = Some ("g1" . into ()))] group_id: Option<String>,
    #[builder(required, default = Some ("member" . into ()))] member_id: Option<String>,
    #[builder(into, default = ShareSessionEpoch (1))] epoch: ShareSessionEpoch,
    #[builder(default)] mode: RenewalMode,
    #[builder(default)] topics: Vec<AcknowledgeTopic>,
) -> ShareAcknowledgeRequest {
    let setup = AcknowledgeRequestSetup {
        group_id,
        member_id,
        epoch,
        mode,
        topics,
    };
    let AcknowledgeRequestSetup {
        group_id,
        member_id,
        epoch,
        mode,
        topics,
    } = setup;
    let mut request = ShareAcknowledgeRequest {
        group_id,
        member_id,
        share_session_epoch: epoch.0,
        topics,
        ..Default::default()
    };
    match mode {
        RenewalMode::ProtocolDefault => {}
        RenewalMode::Settle => request.is_renew_ack = false,
        RenewalMode::Renew => request.is_renew_ack = true,
    }
    request
}

/// Drive the g1/t consume fixture lifecycle until its persisted share state is ready.
///
/// # Panics
/// Panics if repeated member heartbeats do not initialize the state within 30 seconds.
pub async fn wait_for_share_init(
    broker: &krabka_broker::BrokerHandle,
    client: &Client,
    member_id: &str,
    member_epoch: i32,
    tid: uuid::Uuid,
) {
    let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            // Send a steady-state heartbeat to trigger the lifecycle hook.
            let _ = client
                .send(krabka_protocol::owned::share_group_heartbeat_request::ShareGroupHeartbeatRequest {
                    group_id: "g1".into(),
                    member_id: member_id.into(),
                    member_epoch,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    ..Default::default()
                })
                .await;
            if broker
                .share_state_summary_for_test("g1", tid, 0)
                .await
                .is_some()
            {
                return;
            }
        }
    })
    .await;
    assert!(
        res.is_ok(),
        "share state for g1:{tid}:0 never initialized within 30s"
    );
}

/// Join the consume fixture's g1/t member and finish its heartbeat-driven initialization.
pub async fn join_consume_member(
    broker: &krabka_broker::BrokerHandle,
    client: &Client,
    tid: uuid::Uuid,
) -> (String, i32) {
    let (member, epoch) = join(client, "g1", "t").await;
    wait_for_share_init(broker, client, &member, epoch, tid).await;
    (member, epoch)
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct ConsumptionSetup {
    pub topic_id: uuid::Uuid,
    #[default(ShareRecordCount(3))]
    pub count: ShareRecordCount,
}

/// Owners and initialized membership for a fresh g1/t consume session.
pub struct ConsumptionFixture {
    pub _permit: tokio::sync::OwnedSemaphorePermit,
    pub broker: krabka_broker::BrokerHandle,
    pub client: std::sync::Arc<krabka_client_core::Client>,
    pub _dir: tempfile::TempDir,
    pub tid: uuid::Uuid,
    pub member: String,
}

impl ConsumptionFixture {
    pub fn session(&self) -> ShareSessionSetup<'_> {
        ShareSessionSetup::joined(&self.member, self.tid)
    }
}

/// Retain every topic owner while the suite-specific initializer joins and waits.
#[bon::builder]
pub async fn consumption_fixture(
    #[builder(into, default = ShareRecordCount (3))] count: ShareRecordCount,
    initialize: impl AsyncFn(&krabka_broker::BrokerHandle, &Client, ConsumptionSetup) -> (String, i32),
) -> ConsumptionFixture {
    let (permit, broker, client, dir, tid) = Box::pin(permitted_topic_fixture().call()).await;
    let (member, _) = initialize(&broker, &client, ConsumptionSetup {
        topic_id: tid,
        count,
    })
    .await;
    ConsumptionFixture {
        _permit: permit,
        broker,
        client,
        _dir: dir,
        tid,
        member,
    }
}

/// Generate the common g1/t setup while retaining each suite's join and readiness policy.
#[macro_export]
macro_rules! share_consumption_fixture {
    ($(#[$attributes:meta])* $name:ident, $join:path,
        |$broker:ident, $client:ident, $member:ident, $epoch:ident, $tid:ident| $wait:expr
    ) => {
        $(#[$attributes])*
        pub async fn $name(
            $broker: &::krabka_broker::BrokerHandle,
            $client: &::krabka_client_core::Client,
            setup: $crate::support::share::ConsumptionSetup,
        ) -> (String, i32) {
            let $tid = setup.topic_id;
            $crate::support::share::bootstrap_share_state($broker, $client, "g1").await;
            $crate::support::share::produce_n()
                .client($client)
                .topic_id($tid)
                .count(setup.count.0)
                .call()
                .await;
            let ($member, $epoch) = ($join)($client, "g1", "t").await;
            ($wait).await;
            ($member, $epoch)
        }
    };
}
