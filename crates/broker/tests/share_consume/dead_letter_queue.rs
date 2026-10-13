//! The share-group dead-letter queue (KIP-1191), end to end.
//!
//! At `share.version` 2, a record that a member rejects is written to the
//! group's dead-letter topic before the share partition archives it. The
//! dead-letter record carries six headers that say where the record came from
//! and why it is there, and the key and value of the source record when the
//! group copies them. Kafka archives the record whatever becomes of the write,
//! so a group whose topic cannot be written still moves on.
//!
//! The queue is Kafka trunk's, so each broker here runs with
//! `unstable.api.versions.enable` and `unstable.feature.versions.enable` on.

use {
    crate::{
        NONE, REJECT,
        harness::{broker_config, broker_test_permit, produce_n, produce_values, topic_id, wire},
        share_rpc::{acquired_count, fetch_until_acquired, share_fetch},
        support::{
            configs::{
                ConfigOperation, feature_update, incremental_config, incremental_request,
                incremental_resource,
            },
            fetch::{FetchLimits, RequestWaitMillis, single_partition_fetch},
            offsets::{list_offset_partition, single_partition_list_offsets},
            share::{
                ShareAck, ShareFetchSetup, ShareRecordCount, ShareSessionEpoch, ShareSessionSetup,
                StartedShareTopic, acknowledge_partition, acknowledge_request, acknowledge_success,
                acknowledge_topic, join_consume_member, start_group_topic,
            },
            topics::topic_configs,
        },
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_broker::{
        BrokerConfig, BrokerHandle, RemoteStorageBackend, api_catalog::UnstableApiVersions,
    },
    krabka_client_core::Client,
    krabka_protocol::{
        owned::{
            create_topics_request::CreateTopicsRequest, list_offsets_request::ListOffsetsRequest,
            share_acknowledge_request::AcknowledgementBatch,
            update_features_request::UpdateFeaturesRequest,
        },
        records::{Record, RecordBatch, RecordsPayload},
    },
    std::time::Duration,
};

const GROUP: &str = "g1";
const DLQ_TOPIC: &str = "dlq.g1";

/// A broker that serves what krabka implements from Kafka trunk, over a fresh
/// data directory.
fn trunk_config(dir: &tempfile::TempDir) -> BrokerConfig {
    let mut config = broker_config(dir.path().to_path_buf());
    config.features.unstable_api_versions = UnstableApiVersions::Enabled;
    config.features.unstable_feature_versions = krabka_raft::UnstableFeatureVersions::Enabled;
    config
}

/// Finalizes `share.version` at 2, the level of the dead-letter queue.
async fn finalize_share_version_two(client: &Client) {
    let response = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![feature_update("share.version", 2, 1)],
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    assert!(response.error_code == 0, "{response:?}");
}

/// One config name and the value it is set to.
#[derive(Clone, Copy, Debug)]
struct ConfigEntry<'a> {
    name: &'a str,
    value: &'a str,
}

/// Creates the dead-letter topic with the topic config that opts it in.
async fn create_dead_letter_topic(broker: &BrokerHandle, client: &Client) {
    create_dead_letter_topic_with(broker, client, &[]).await;
}

/// [`create_dead_letter_topic`], with `extra` topic configs beside the one that
/// opts the topic in.
async fn create_dead_letter_topic_with(
    broker: &BrokerHandle,
    client: &Client,
    extra: &[ConfigEntry<'_>],
) {
    let configs = topic_configs(
        std::iter::once(("errors.deadletterqueue.group.enable", "true")).chain(
            extra
                .iter()
                .map(|ConfigEntry { name, value }| (*name, *value)),
        ),
    );
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![
                crate::support::topics::creatable_topic_with_configs()
                    .name(DLQ_TOPIC.into())
                    .configs(configs)
                    .call(),
            ],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == 0, "{response:?}");
    broker.wait_until_partition_present(DLQ_TOPIC, 0).await;
}

#[derive(Clone, Copy, Default)]
enum DeadLetterConfigTarget {
    #[default]
    Group,
    Broker,
    Topic,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct DeadLetterConfigSetup<'a> {
    target: DeadLetterConfigTarget,
    #[default(GROUP)]
    name: &'a str,
    #[default(&[])]
    values: &'a [ConfigEntry<'a>],
}

/// Sets `configs` on the resource of `resource_type` and `resource_name`.
#[bon::builder]
async fn set_configs(
    client: &Client,
    #[builder(default)] target: DeadLetterConfigTarget,
    #[builder(default = GROUP)] name: &'_ str,
    #[builder(default = & [])] values: &'_ [ConfigEntry<'_>],
) {
    let setup = DeadLetterConfigSetup {
        target,
        name,
        values,
    };
    let response = client
        .send(
            incremental_request()
                .resources(vec![incremental_resource(
                    match setup.target {
                        DeadLetterConfigTarget::Group => 32,
                        DeadLetterConfigTarget::Broker => 4,
                        DeadLetterConfigTarget::Topic => 2,
                    },
                    setup.name,
                    setup
                        .values
                        .iter()
                        .map(|ConfigEntry { name, value }| {
                            incremental_config(*name, Some((*value).into()), ConfigOperation::Set)
                        })
                        .collect(),
                )])
                .call(),
        )
        .await
        .expect("IncrementalAlterConfigs");
    assert!(
        response.responses[0].error_code == 0,
        "{:?} rejected: {:?}",
        setup.values,
        response.responses[0].error_message
    );
}

/// Gives the group its dead-letter topic, and asks it to copy the records.
async fn point_group_at_dead_letter_topic(client: &Client) {
    set_configs()
        .client(client)
        .values(&[
            ConfigEntry {
                name: "errors.deadletterqueue.topic.name",
                value: DLQ_TOPIC,
            },
            ConfigEntry {
                name: "errors.deadletterqueue.copy.record.enable",
                value: "true",
            },
        ])
        .call()
        .await;
}

/// The records of the first batch of the dead-letter topic, once there is one.
async fn dead_letter_records(broker: &BrokerHandle, client: &Client) -> Vec<Record> {
    let dlq_id = topic_id(broker, DLQ_TOPIC);
    let response = client
        .send(
            single_partition_fetch()
                .topic(DLQ_TOPIC.into())
                .topic_id(wire(dlq_id))
                .limits(FetchLimits::one_mebibyte(RequestWaitMillis(100)))
                .call(),
        )
        .await
        .expect("Fetch");
    records_of(response.responses[0].partitions[0].records.as_ref())
}

/// The records of a response row, decoded or not.
fn records_of(payload: Option<&RecordsPayload>) -> Vec<Record> {
    match payload {
        Some(RecordsPayload::V2(batches)) => batches
            .iter()
            .flat_map(|batch| batch.records.clone())
            .collect(),
        Some(RecordsPayload::Raw(bytes)) => {
            let mut cursor = bytes.clone();
            let mut records = Vec::new();
            while !cursor.is_empty() {
                let batch = RecordBatch::decode(&mut cursor).expect("decode a record batch");
                records.extend(batch.records);
            }
            records
        }
        _ => Vec::new(),
    }
}

/// Polls the dead-letter topic until it holds `count` records.
async fn wait_for_dead_letters(
    broker: &BrokerHandle,
    client: &Client,
    count: ShareRecordCount,
) -> Vec<Record> {
    let expected = usize::try_from(count.0).expect("dead-letter count fits usize");
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let records = dead_letter_records(broker, client).await;
            if records.len() >= expected {
                return records;
            }
            // intentional: bounded poll of a topic that a background task writes;
            // nothing signals the append to the test.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the dead-letter records reached the topic")
}

// A header of `record` as text.
krabka_macros::record_header_text!(header, lossy);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct RejectRangesSetup<'a> {
    topic_id: uuid::Uuid,
    #[default(crate::support::share::ShareSessionEpoch(1))]
    epoch: ShareSessionEpoch,
    #[default(&[])]
    ranges: &'a [RejectedRange],
}

/// An inclusive range of offsets to reject.
#[derive(Clone, Copy)]
struct RejectedRange {
    first: krabka_ids::Offset,
    last: krabka_ids::Offset,
}

/// One `ShareAcknowledge` that rejects each of `ranges`, as the first and last
/// offsets of partition 0, in a batch of its own.
#[bon::builder]
async fn reject_ranges(
    client: &Client,
    member: &str,
    #[builder(default)] topic_id: uuid::Uuid,
    #[builder(into, default = crate :: support :: share :: ShareSessionEpoch (1))]
    epoch: ShareSessionEpoch,
    #[builder(default = & [])] ranges: &'_ [RejectedRange],
) {
    let setup = RejectRangesSetup {
        topic_id,
        epoch,
        ranges,
    };
    let response = client
        .send(
            acknowledge_request()
                .maybe_group_id(Some(Some(GROUP.into())))
                .maybe_member_id(Some(Some(member.into())))
                .epoch(setup.epoch)
                .topics(vec![acknowledge_topic(wire(setup.topic_id), vec![
                    acknowledge_partition(
                        0,
                        setup
                            .ranges
                            .iter()
                            .map(|RejectedRange { first, last }| AcknowledgementBatch {
                                first_offset: first.0,
                                last_offset: last.0,
                                acknowledge_types: vec![
                                    REJECT;
                                    usize::try_from(last.0 - first.0 + 1)
                                        .unwrap()
                                ],
                                ..Default::default()
                            })
                            .collect(),
                    ),
                ])])
                .call(),
        )
        .await
        .expect("ShareAcknowledge");
    assert!(response.error_code == NONE, "{response:?}");
    assert!(
        response.responses[0].partitions[0].error_code == NONE,
        "{response:?}"
    );
}

krabka_macros::share_dlq_meters!(
    /// What the `DeadLetterQueue*` meters of the group count on the broker: the
    /// records written, the attempts to produce, and the writes that failed.
    meters,
    krabka_broker::metrics::BrokerMetrics,
    krabka_broker::metrics::ShareGroupIdLabel
);

/// Starts a trunk broker with a source topic `t` of `records` records and a
/// member that holds all of them acquired, then returns what a test drives.
struct Cluster {
    broker: BrokerHandle,
    client: std::sync::Arc<Client>,
    tid: uuid::Uuid,
    member: String,
    _dir: tempfile::TempDir,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct RejectArchiveSetup {
    first: krabka_ids::Offset,
    last: krabka_ids::Offset,
    #[default(krabka_ids::Offset(1))]
    expected_spso: krabka_ids::Offset,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct DeadLetterReadSetup {
    count: ShareRecordCount,
    #[default(krabka_ids::Offset(1))]
    expected_spso: krabka_ids::Offset,
}

/// The parts of a [`Cluster`] a test reads, borrowed.
struct ClusterParts<'a> {
    broker: &'a BrokerHandle,
    client: &'a std::sync::Arc<Client>,
    tid: &'a uuid::Uuid,
    member: &'a String,
}

impl Cluster {
    async fn check_empty_dead_letter_topic(&self) {
        check!(
            dead_letter_records(&self.broker, &self.client)
                .await
                .is_empty()
        );
    }

    async fn reject_and_archive(&self, setup: RejectArchiveSetup) {
        self.reject_acquired(setup.first, setup.last).await;
        self.broker
            .wait_until_share_spso(GROUP, self.tid, 0, setup.expected_spso.0)
            .await;
    }

    async fn dead_letters_after_archive(&self, setup: DeadLetterReadSetup) -> Vec<Record> {
        let records = wait_for_dead_letters(&self.broker, &self.client, setup.count).await;
        self.broker
            .wait_until_share_spso(GROUP, self.tid, 0, setup.expected_spso.0)
            .await;
        records
    }

    fn parts(&self) -> ClusterParts<'_> {
        ClusterParts {
            broker: &self.broker,
            client: &self.client,
            tid: &self.tid,
            member: &self.member,
        }
    }

    async fn reject_acquired(&self, first: krabka_ids::Offset, last: krabka_ids::Offset) {
        acknowledge_success(
            &self.client,
            ShareAck::prefix_for(&self.member, self.tid, last)
                .starting_at(first)
                .reject(),
        )
        .await;
    }
}

#[derive(Clone, Copy, Default)]
enum DeadLetterShareVersion {
    One,
    #[default]
    Two,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct DeadLetterQueueSetup<'a> {
    #[default(crate::support::share::ShareRecordCount(3))]
    records: ShareRecordCount,
    version: DeadLetterShareVersion,
    #[default(&[])]
    topic_configs: &'a [ConfigEntry<'a>],
}

/// Keep the feature gate and topic/group configuration in their original order.
async fn configure_dead_letters(
    client: &Client,
    broker: &BrokerHandle,
    version: DeadLetterShareVersion,
    topic_configs: &[ConfigEntry<'_>],
) {
    if matches!(version, DeadLetterShareVersion::Two) {
        finalize_share_version_two(client).await;
    }
    create_dead_letter_topic_with(broker, client, topic_configs).await;
    point_group_at_dead_letter_topic(client).await;
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct AcquiredRecordsSetup {
    #[default(crate::support::share::ShareRecordCount(3))]
    records: ShareRecordCount,
}

/// Required script steps for a custom acquisition, separate from its defaulted record setup.
struct AcquisitionHooks<T, C, P> {
    tweak: T,
    configure: C,
    produce: P,
}

async fn acquired_records(
    records: ShareRecordCount,
    tweak: impl FnOnce(&mut BrokerConfig),
    configure: impl AsyncFnOnce(&Client, &BrokerHandle),
) -> Cluster {
    acquired_records_from()
        .records(records)
        .hooks(AcquisitionHooks {
            tweak,
            configure,
            produce: async |client: &Client, tid| {
                produce_n()
                    .client(client)
                    .topic_id(tid)
                    .count(records.0)
                    .call()
                    .await;
            },
        })
        .call()
        .await
}

/// A joined member and the row of its first acquiring fetch.
struct AcquiredMember {
    member: String,
    row: krabka_protocol::owned::share_fetch_response::PartitionData,
}

/// [`acquired_records`], with the records that `produce` writes to `t` in place
/// of the small ones that `produce_n` writes.
async fn join_and_acquire(
    broker: &BrokerHandle,
    client: &Client,
    tid: uuid::Uuid,
) -> AcquiredMember {
    let (member, _member_epoch) = join_consume_member(broker, client, tid).await;
    let row = fetch_until_acquired(client, ShareSessionSetup {
        group: GROUP,
        ..ShareSessionSetup::opening(&member, tid)
    })
    .await;
    AcquiredMember { member, row }
}

#[bon::builder]
async fn acquired_records_from<T, C, P>(
    #[builder(into, default = crate :: support :: share :: ShareRecordCount (3))]
    records: ShareRecordCount,
    hooks: AcquisitionHooks<T, C, P>,
) -> Cluster
where
    T: FnOnce(&mut BrokerConfig),
    C: AsyncFnOnce(&Client, &BrokerHandle),
    P: AsyncFnOnce(&Client, uuid::Uuid),
{
    let setup = AcquiredRecordsSetup { records };
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = trunk_config(&dir);
    (hooks.tweak)(&mut config);
    let StartedShareTopic {
        broker,
        client,
        topic_id: tid,
    } = start_group_topic().config(config).call().await;
    (hooks.configure)(&client, &broker).await;
    (hooks.produce)(&client, tid).await;
    let AcquiredMember { member, row } = join_and_acquire(&broker, &client, tid).await;
    assert!(acquired_count(&row) == setup.records.0, "{row:?}");
    Cluster {
        broker,
        client,
        tid,
        member,
        _dir: dir,
    }
}

/// Kafka's `initiateDLQAndArchive`, end to end: a rejected batch is written to
/// the group's topic, one record for each offset, with the six headers and the
/// copied key and value, and only then does the SPSO move past it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_batch_is_dead_lettered_then_archived() {
    let _permit = broker_test_permit().await;
    let cluster = acquired_dead_letters().customize(|_| {}).call().await;
    let ClusterParts {
        broker,
        client,
        tid,
        ..
    } = cluster.parts();

    cluster
        .reject_acquired(krabka_ids::Offset(0), krabka_ids::Offset(2))
        .await;
    let records = wait_for_dead_letters(broker, client, ShareRecordCount(3)).await;
    broker.wait_until_share_spso(GROUP, *tid, 0, 3).await;

    let seen: Vec<_> = records
        .iter()
        .map(|record| {
            (
                record.value.clone(),
                header(record, "__dlq.errors.topic"),
                header(record, "__dlq.errors.partition"),
                header(record, "__dlq.errors.offset"),
                header(record, "__dlq.errors.group"),
                header(record, "__dlq.errors.delivery.count"),
                header(record, "__dlq.errors.message"),
            )
        })
        .collect();
    let expected: Vec<_> = (0..3)
        .map(|offset| {
            (
                Some(Bytes::from(format!("v{offset}"))),
                Some("t".to_owned()),
                Some("0".to_owned()),
                Some(offset.to_string()),
                Some(GROUP.to_owned()),
                Some("1".to_owned()),
                Some("Offset rejected by client.".to_owned()),
            )
        })
        .collect();
    check!(seen == expected);
    let summary = broker.share_state_summary_for_test(GROUP, *tid, 0).await;
    check!(summary.map(|(_, _, spso, _)| spso) == Some(3));
    // Kafka's `recordDLQRecordWrite` and `recordDLQProduce`: the three records
    // of the one round, and one attempt to produce it.
    broker
        .wait_for_metrics("the dead-letter meters", |metrics| {
            meters(metrics, GROUP) == (3, 1, 0)
        })
        .await;
    cluster.broker.shutdown().await;
}

/// Runs that are not neighbours are a write each. One record comes out for
/// each offset, and the meters count each round as a produce, however the
/// writes were packed into requests: Kafka's `recordDLQProduce` marks each
/// handler, not each coalesced request. Whether the writes share a request is
/// the race of the writers, so this does not assert it: the unit tests of
/// `dlq::coalesce` and `dlq::writer` do.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_that_are_not_neighbours_are_all_dead_lettered_and_counted() {
    let _permit = broker_test_permit().await;
    let cluster = acquired_dead_letters()
        .customize(|_| {})
        .records(5)
        .call()
        .await;
    let ClusterParts {
        broker,
        client,
        tid,
        member,
    } = cluster.parts();

    reject_ranges()
        .client(client)
        .member(member)
        .topic_id(*tid)
        .ranges(&[
            RejectedRange {
                first: krabka_ids::Offset(0),
                last: krabka_ids::Offset(0),
            },
            RejectedRange {
                first: krabka_ids::Offset(2),
                last: krabka_ids::Offset(2),
            },
            RejectedRange {
                first: krabka_ids::Offset(4),
                last: krabka_ids::Offset(4),
            },
        ])
        .call()
        .await;
    let mut records = wait_for_dead_letters(broker, client, ShareRecordCount(3)).await;
    broker
        .wait_for_metrics("the dead-letter meters", |metrics| {
            meters(metrics, GROUP) == (3, 3, 0)
        })
        .await;
    // The records of the rounds that were packed into one request are one
    // batch, and the order of the requests is the writers' race.
    records.sort_by_key(|record| header(record, "__dlq.errors.offset"));

    check!(
        records
            .iter()
            .map(|record| (
                header(record, "__dlq.errors.offset"),
                record.value.clone(),
                header(record, "__dlq.errors.delivery.count"),
                header(record, "__dlq.errors.message"),
            ))
            .collect::<Vec<_>>()
            == [0, 2, 4]
                .map(|offset| (
                    Some(offset.to_string()),
                    Some(Bytes::from(format!("v{offset}"))),
                    Some("1".to_owned()),
                    Some("Offset rejected by client.".to_owned()),
                ))
                .to_vec()
    );
    cluster.broker.shutdown().await;
}

/// A write that the topic refuses counts as a failed write, and the run is
/// archived regardless. The topic takes no more than 100 bytes for a message,
/// and a dead-letter record with its six headers is more, so the leader answers
/// `MESSAGE_TOO_LARGE`, which Kafka's `handleProduceResponse` does not retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_the_topic_refuses_counts_as_failed_and_still_archives() {
    let _permit = broker_test_permit().await;
    let cluster = acquired_dead_letters()
        .customize(|_| {})
        .records(1)
        .topic_configs(&[ConfigEntry {
            name: "max.message.bytes",
            value: "100",
        }])
        .call()
        .await;
    let ClusterParts { broker, tid, .. } = cluster.parts();

    cluster
        .reject_acquired(krabka_ids::Offset(0), krabka_ids::Offset(0))
        .await;
    broker.wait_until_share_spso(GROUP, *tid, 0, 1).await;

    broker
        .wait_for_metrics("the dead-letter meters", |metrics| {
            meters(metrics, GROUP) == (0, 1, 1)
        })
        .await;
    cluster.check_empty_dead_letter_topic().await;
    cluster.broker.shutdown().await;
}

/// A record that uses up `share.delivery.count.limit` goes to the queue too,
/// with the delivery count as its cause: here its lock runs out on the second
/// delivery, and the record is written with a delivery count of 2 and Kafka's
/// message for the cause.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_that_uses_up_its_deliveries_is_dead_lettered() {
    let _permit = broker_test_permit().await;
    let cluster = acquired_dead_letters()
        .customize(|config| {
            config.share_group.record_lock_duration = Duration::from_millis(150);
            config.share_group.max_delivery_attempts = 2;
        })
        .records(1)
        .call()
        .await;
    let ClusterParts {
        broker,
        client,
        tid,
        member,
    } = cluster.parts();

    // Delivery 1 was taken by the helper. Its lock runs out, and the record is
    // available again for a second delivery, which nobody acknowledges.
    broker
        .wait_until_share_acquired_count(GROUP, *tid, 0, 0)
        .await;
    let second = share_fetch(
        client,
        ShareFetchSetup::from(ShareSessionSetup {
            group: GROUP,
            ..ShareSessionSetup::joined(member, *tid)
        }),
    )
    .await;
    assert!(
        acquired_count(&second) == 1 && second.acquired_records[0].delivery_count == 2,
        "{second:?}"
    );
    let records = cluster
        .dead_letters_after_archive(DeadLetterReadSetup::default())
        .await;

    check!(
        (
            header(&records[0], "__dlq.errors.offset"),
            header(&records[0], "__dlq.errors.delivery.count"),
            header(&records[0], "__dlq.errors.message"),
            records[0].value.clone(),
        ) == (
            Some("0".to_owned()),
            Some("2".to_owned()),
            Some("Offset delivery count exceeded the threshold.".to_owned()),
            Some(Bytes::from_static(b"v0")),
        )
    );
    cluster.broker.shutdown().await;
}

/// The queue is off below `share.version` 2, so a reject archives at once and
/// nothing reaches the topic, even with the group and the topic set up: the
/// gate is the finalized level, not the config.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reject_below_share_version_two_writes_nothing() {
    let _permit = broker_test_permit().await;
    let cluster = acquired_dead_letters()
        .customize(|_| {})
        .records(2)
        .version(DeadLetterShareVersion::One)
        .call()
        .await;

    cluster
        .reject_and_archive(RejectArchiveSetup {
            last: krabka_ids::Offset(1),
            expected_spso: krabka_ids::Offset(2),
            ..Default::default()
        })
        .await;
    // intentional: no signal says that a write that must not happen has not
    // happened yet, so the test gives it time before it reads the topic.
    tokio::time::sleep(Duration::from_millis(500)).await;

    cluster.check_empty_dead_letter_topic().await;
    cluster.broker.shutdown().await;
}

/// A group whose topic cannot be written still moves on: Kafka archives the
/// record when the write fails, and logs the failure. Here the topic does not
/// exist and the cluster does not create it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_write_still_archives_the_record() {
    let _permit = broker_test_permit().await;
    let cluster = acquired_records(
        ShareRecordCount(2),
        |_| {},
        async |client, _| {
            finalize_share_version_two(client).await;
            point_group_at_dead_letter_topic(client).await;
        },
    )
    .await;
    let ClusterParts { broker, .. } = cluster.parts();

    cluster
        .reject_and_archive(RejectArchiveSetup {
            last: krabka_ids::Offset(1),
            expected_spso: krabka_ids::Offset(2),
            ..Default::default()
        })
        .await;

    check!(
        broker
            .controller_image_for_test()
            .topic(DLQ_TOPIC)
            .is_none()
    );
    // A topic that fails validation is refused before any request is made, so
    // no produce is counted, as in Kafka's `dlq`, which fails the future before
    // the handler is queued.
    broker
        .wait_for_metrics("no dead-letter produce", |metrics| {
            meters(metrics, GROUP) == (0, 0, 0)
        })
        .await;
    cluster.broker.shutdown().await;
}

/// With `errors.deadletterqueue.auto.create.topics.enable`, a missing topic is
/// created with the config that opts it in, and the records are written to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_topic_is_created_when_the_cluster_allows_it() {
    let _permit = broker_test_permit().await;
    let cluster = acquired_records(
        ShareRecordCount(1),
        |_| {},
        async |client, _| {
            finalize_share_version_two(client).await;
            set_configs()
                .client(client)
                .target(DeadLetterConfigTarget::Broker)
                .name("")
                .values(&[ConfigEntry {
                    name: "errors.deadletterqueue.auto.create.topics.enable",
                    value: "true",
                }])
                .call()
                .await;
            point_group_at_dead_letter_topic(client).await;
        },
    )
    .await;
    let ClusterParts { broker, .. } = cluster.parts();

    cluster
        .reject_acquired(krabka_ids::Offset(0), krabka_ids::Offset(0))
        .await;
    broker.wait_until_partition_present(DLQ_TOPIC, 0).await;
    let records = cluster
        .dead_letters_after_archive(DeadLetterReadSetup::default())
        .await;

    check!(records[0].value == Some(Bytes::from_static(b"v0")));
    check!(
        broker
            .controller_image_for_test()
            .topic_config(DLQ_TOPIC)
            .and_then(|configs| configs.get("errors.deadletterqueue.group.enable").cloned())
            == Some("true".to_owned())
    );
    cluster.broker.shutdown().await;
}

/// A source record that fits the read budget can be over the topic's
/// `max.message.bytes` once the six headers and the batch header are added. The
/// copy is best effort, so the record is written with its headers alone rather
/// than lost to the broker's `MESSAGE_TOO_LARGE`.
///
/// The source value is 1,000 bytes, which is about 1,010 bytes as a record and
/// fits the topic's 1,100. With the headers, which take about 180 bytes, and
/// the 61-byte batch header it is about 1,250 bytes as a batch, which does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_copied_record_too_big_for_the_topic_is_written_with_headers_alone() {
    let _permit = broker_test_permit().await;
    let cluster = acquired_records_from()
        .records(1)
        .hooks(AcquisitionHooks {
            tweak: |_: &mut BrokerConfig| {},
            configure: async |client: &Client, broker: &BrokerHandle| {
                configure_dead_letters(client, broker, DeadLetterShareVersion::default(), &[
                    ConfigEntry {
                        name: "max.message.bytes",
                        value: "1100",
                    },
                ])
                .await;
            },
            produce: async |client: &Client, tid: uuid::Uuid| {
                let values = vec![Bytes::from(vec![b'x'; 1_000])];
                produce_values()
                    .client(client)
                    .values(values)
                    .topic_id(tid)
                    .call()
                    .await;
            },
        })
        .call()
        .await;

    cluster
        .reject_acquired(krabka_ids::Offset(0), krabka_ids::Offset(0))
        .await;
    let records = cluster
        .dead_letters_after_archive(DeadLetterReadSetup::default())
        .await;

    check!(
        records
            .iter()
            .map(|record| (
                record.key.clone(),
                record.value.clone(),
                header(record, "__dlq.errors.offset"),
                header(record, "__dlq.errors.message"),
            ))
            .collect::<Vec<_>>()
            == vec![(
                None,
                None,
                Some("0".to_owned()),
                Some("Offset rejected by client.".to_owned()),
            )]
    );
    cluster.broker.shutdown().await;
}

/// Kafka's `ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP` (KIP-405).
const EARLIEST_LOCAL_TIMESTAMP: i64 = -4;

// Turns `t` into a tiered topic whose records leave local disk as soon as the
// remote tier holds them: `produce_n` stamps no record timestamp, so every
// segment is past a one-millisecond `local.retention.ms` at once, and
// `retention.ms=-1` keeps the tier from deleting them for the same reason.
async fn tier_the_source_topic(broker: &BrokerHandle, client: &Client) {
    set_configs()
        .client(client)
        .target(DeadLetterConfigTarget::Topic)
        .name("t")
        .values(&[
            ConfigEntry {
                name: "remote.storage.enable",
                value: "true",
            },
            ConfigEntry {
                name: "local.retention.ms",
                value: "1",
            },
            ConfigEntry {
                name: "retention.ms",
                value: "-1",
            },
        ])
        .call()
        .await;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if broker
                .partition_log_config_for_test("t", 0)
                .is_some_and(|config| {
                    config.remote_storage_enable
                        && config.local_retention == Some(krabka_units::millis(1))
                })
            {
                return;
            }
            // intentional: the reconcile loop applies the topic config to the
            // partition's `LogConfig` with no awaiter or metric.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the tiered-storage config reached the partition");
}

// Waits until `ListOffsets(EARLIEST_LOCAL)` on `t` answers `offset`: every
// record below it is in the remote tier and gone from local disk.
async fn wait_until_local_log_start(client: &Client, offset: krabka_ids::Offset) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let mut response = client
                .send(ListOffsetsRequest {
                    replica_id: -1,
                    timeout_ms: 5_000,
                    ..single_partition_list_offsets(
                        "t",
                        list_offset_partition(0, EARLIEST_LOCAL_TIMESTAMP),
                    )
                })
                .await
                .expect("ListOffsets");
            let row = response.topics.remove(0).partitions.remove(0);
            if row.error_code == NONE && row.offset == offset.0 {
                return;
            }
            // intentional: the remote-log manager moves the local log start
            // on its own timer, and nothing signals it to the test.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the records left local disk for the remote tier");
}

// Kafka's `ShareConsumerDLQTieredStorageTest`: every record of the source topic
// is in the remote tier and gone from local disk before the group fetches it.
// The member acquires the records out of the tier, with their values, and
// rejects them, and the dead-letter records carry the values that the copy
// read back from the tier.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_that_only_the_remote_tier_holds_are_acquired_and_dead_lettered() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let remote_dir = tempfile::TempDir::new().unwrap();
    let mut config = trunk_config(&dir);
    config.remote_storage_backend = Some(RemoteStorageBackend::Local {
        dir: remote_dir.path().to_path_buf(),
    });
    config.remote_log_manager_interval = krabka_units::millis(100);
    let StartedShareTopic {
        broker,
        client,
        topic_id: tid,
    } = start_group_topic().config(config).call().await;
    finalize_share_version_two(&client).await;
    create_dead_letter_topic(&broker, &client).await;
    point_group_at_dead_letter_topic(&client).await;
    tier_the_source_topic(&broker, &client).await;
    produce_n()
        .client(&client)
        .topic_id(tid)
        .count(3)
        .call()
        .await;
    wait_until_local_log_start(&client, krabka_ids::Offset(3)).await;

    let AcquiredMember { member, row } = join_and_acquire(&broker, &client, tid).await;
    let values = |records: &[Record]| -> Vec<Option<Bytes>> {
        records.iter().map(|record| record.value.clone()).collect()
    };
    let produced: Vec<_> = (0..3)
        .map(|offset| Some(Bytes::from(format!("v{offset}"))))
        .collect();
    check!(acquired_count(&row) == 3);
    check!(values(&records_of(row.records.as_ref())) == produced);

    acknowledge_success(
        &client,
        ShareAck::prefix_for(&member, tid, krabka_ids::Offset(2)).reject(),
    )
    .await;
    let dead_letters = wait_for_dead_letters(&broker, &client, ShareRecordCount(3)).await;
    check!(values(&dead_letters) == produced);
    broker.shutdown().await;
}

#[bon::builder]
async fn acquired_dead_letters(
    customize: impl FnOnce(&mut BrokerConfig),
    #[builder(into, default = crate :: support :: share :: ShareRecordCount (3))]
    records: ShareRecordCount,
    #[builder(default)] version: DeadLetterShareVersion,
    #[builder(default = & [])] topic_configs: &'_ [ConfigEntry<'_>],
) -> Cluster {
    let setup = DeadLetterQueueSetup {
        records,
        version,
        topic_configs,
    };
    acquired_records(setup.records, customize, async |client, broker| {
        configure_dead_letters(client, broker, setup.version, setup.topic_configs).await;
    })
    .await
}
