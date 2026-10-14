//! Handler tests for the `ClusterAction` gate on a follower `Fetch`.
//!
//! Kafka's `KafkaApis.handleFetchRequest` treats a fetch whose replica id is 0
//! or more as a follower fetch. The replica id is `ReplicaId` before v15 and
//! `ReplicaState.ReplicaId` from v15 on. A follower fetch reads up to the log
//! end offset and moves the follower progress, and so the high watermark.
//! Kafka lets it through only when the principal holds `ClusterAction` on the
//! cluster resource, and then it checks no topic ACL. Without `ClusterAction`,
//! every partition row answers `TOPIC_AUTHORIZATION_FAILED`, and the fetch
//! reads nothing.
//!
//! A consumer fetch keeps its replica id at -1, also when it names a rack to
//! read from a follower replica (KIP-392). It needs topic `Read`.
//!
//! Each case replicates its own topic from this broker (node 1) to a node 2
//! that never fetches, so the high watermark stays at 0 until a fetch as node
//! 2 moves it.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    super::FIRST_TOPIC_ID_VERSION,
    crate::{
        authorizer::AuthorizationResult,
        broker::BrokerHandle,
        codes,
        handlers::{
            acl_wire::CLUSTER_RESOURCE_NAME,
            test_support::{
                CaseOutcomes, StartedBroker, collect_topic_cases, default_records_batch,
                seed_partition_replicas, wait_for_bound_partition,
            },
        },
        partition::Partition,
        test_support::{append_partition_batch, start_broker_no_audit_with},
    },
    assert2::assert,
    bytes::Bytes,
    krabka_log::Offset,
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::{
        owned::{
            fetch_request::{FetchRequest, FetchTopic},
            fetch_response::{FetchResponse, PartitionData},
        },
        primitives::uuid::Uuid as WireUuid,
        records::{RecordBatch, RecordsPayload},
    },
    std::sync::Arc,
};

/// The node id of the follower that the fetches claim to be. The broker under
/// test is node 1.
const FOLLOWER: i32 = 2;

/// The replica id of a consumer fetch.
const CONSUMER: i32 = -1;

/// The records that each case appends to its leader log.
const RECORDS: [&[u8]; 2] = [b"committed-later", b"not-committed-yet"];

/// The log end offset after [`RECORDS`].
const LOG_END: i64 = 2;

/// The principal that sends the fetch, by the grant that it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Caller {
    /// `Read` on every topic, and nothing on the cluster.
    TopicReader,
    /// `ClusterAction` on the cluster, and nothing on any topic.
    Replicator,
    /// No grant.
    Stranger,
}

impl Caller {
    const fn name(self) -> &'static str {
        match self {
            Self::TopicReader => "reader",
            Self::Replicator => "replicator",
            Self::Stranger => "stranger",
        }
    }
}

/// Allows exactly the grants that [`Caller`] documents.
#[derive(Debug)]
struct Grants;

krabka_macros::test_authorizer!(Grants, request; {
    let allowed = match request.principal.name.as_str() {
        "reader" => {
            request.resource_type == ResourceType::Topic && request.operation == AclOperation::Read
        }
        "replicator" => {
            request.resource_type == ResourceType::Cluster
                && request.resource_name == CLUSTER_RESOURCE_NAME
                && request.operation == AclOperation::ClusterAction
        }
        _ => false,
    };
    if allowed {
        AuthorizationResult::Allow
    } else {
        AuthorizationResult::Deny
    }
});

/// How the fetch that a case sends names itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sender {
    /// A follower fetch as node [`FOLLOWER`].
    Follower,
    /// A KIP-392 consumer fetch that names a rack.
    RackAwareConsumer,
}

use crate::test_support::KafkaErrorCode;

#[derive(derive_more::From, derive_more::Into, Debug, Clone, Copy)]
struct FetchApiVersion(i16);

/// A topic a fetch names: its name and its topic id.
#[derive(Debug, Clone, Copy)]
struct TopicRef<'a> {
    name: &'a str,
    id: WireUuid,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct FetchRequestSetup<'a> {
    #[default(FetchApiVersion(12))]
    version: FetchApiVersion,
    #[default(Sender::Follower)]
    sender: Sender,
    #[default(TopicRef { name: "replicated", id: WireUuid::ZERO })]
    topic: TopicRef<'a>,
    offset: Offset,
}

/// What a case expects the fetches to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    /// Kafka reads as a follower: records up to the log end offset, and a high
    /// watermark that follows the follower.
    FollowerRead,
    /// Kafka refuses every partition row with this error code.
    Refused(KafkaErrorCode),
    /// Kafka reads as a consumer: nothing past the high watermark of 0.
    ConsumerRead,
}

#[derive(Debug, Clone, Copy)]
struct Case {
    version: FetchApiVersion,
    caller: Caller,
    sender: Sender,
    expect: Expect,
}

/// The two fetches of one case and the high watermark after them.
///
/// The first fetch starts at offset 0. The second starts at the log end
/// offset, which is what a follower sends once it holds every record.
#[derive(Debug, PartialEq)]
struct Outcome {
    case: String,
    from_start: FetchResponse,
    from_log_end: FetchResponse,
    high_watermark: Offset,
}

async fn start() -> StartedBroker {
    let HandlerStartedBroker {
        broker: handle,
        dir,
    } = start_broker_no_audit_with(|cfg| {
        cfg.authorizer = Arc::new(Grants);
        // Node 2 never fetches. Keep it in the ISR for the whole test, so only
        // a fetch as node 2 can move the high watermark.
        cfg.replica_lag_time_max = krabka_units::secs(600);
    })
    .await;
    StartedBroker {
        broker: handle,
        dir,
    }
}

/// The leader partition a case seeded and the batch as the log stored it.
struct SeededPartition {
    partition: Arc<Partition>,
    batch: RecordBatch,
}

/// Create `topic` with one partition that this broker leads and that node 2
/// follows, and append [`RECORDS`] to the leader log. Return the partition and
/// the batch as the log stored it.
async fn replicated_partition(
    broker: &BrokerHandle,
    topic: &str,
    topic_id: uuid::Uuid,
) -> SeededPartition {
    seed_partition_replicas()
        .broker(broker)
        .topic(topic)
        .topic_id(topic_id)
        .call()
        .await;

    let shared = broker.broker_arc_for_test();
    let follower = krabka_raft::NodeId(2);
    let partition = wait_for_bound_partition(
        &shared,
        topic,
        async |partition| partition.replica_state.lock().await.isr.contains(&follower),
        "the broker leads the partition with node 2 in the ISR",
    )
    .await;

    let mut batch = default_records_batch(&RECORDS);
    append_partition_batch(&partition, &mut batch);
    SeededPartition { partition, batch }
}

/// A sessionless one-row fetch of partition 0 of `topic`, from `fetch_offset`.
#[bon::builder]
fn request(
    #[builder(into, default = FetchApiVersion (12))] version: FetchApiVersion,
    #[builder(default = Sender :: Follower)] sender: Sender,
    #[builder(default = TopicRef { name: "replicated", id: WireUuid::ZERO })] topic: TopicRef<'_>,
    #[builder(into, default)] offset: Offset,
) -> FetchRequest {
    let setup = FetchRequestSetup {
        version,
        sender,
        topic,
        offset,
    };
    let FetchRequestSetup {
        version,
        sender,
        topic: TopicRef { name, id: topic_id },
        offset,
    } = setup;
    let replica_id = match sender {
        Sender::Follower => FOLLOWER,
        Sender::RackAwareConsumer => CONSUMER,
    };
    FetchRequest {
        rack_id: if sender == Sender::RackAwareConsumer {
            "rack-a".to_owned()
        } else {
            String::new()
        },
        topics: vec![FetchTopic {
            topic: if version.0 >= FIRST_TOPIC_ID_VERSION {
                String::new()
            } else {
                name.to_owned()
            },
            topic_id: if version.0 >= FIRST_TOPIC_ID_VERSION {
                topic_id
            } else {
                WireUuid::ZERO
            },
            partitions: vec![super::test_support::request_partition(Offset(offset.0))],
            ..Default::default()
        }],
        ..super::test_support::sessionless_request()
            .version(version.0)
            .replica(replica_id)
            .call()
    }
}

/// Send one fetch as `caller` and return the response as a client decodes it.
async fn fetch(
    broker: &BrokerHandle,
    version: FetchApiVersion,
    caller: Caller,
    request: &FetchRequest,
) -> FetchResponse {
    super::test_support::fetch_wire()
        .broker(broker)
        .request(request)
        .version(version.0)
        .user(caller.name())
        .client_id("fetch-client")
        .call()
        .await
}

/// The one-row response of `case` for partition 0 of `topic`.
fn response(
    version: FetchApiVersion,
    topic: TopicRef<'_>,
    partition: PartitionData,
) -> FetchResponse {
    super::test_support::expected_single_topic(
        super::test_support::TopicRepresentation::for_version(krabka_ids::ApiVersion(version.0)),
        (topic.name, topic.id),
        partition,
    )
}

/// The partition row of Kafka's `FetchResponse.partitionResponse`.
fn refused(error_code: KafkaErrorCode) -> PartitionData {
    super::test_support::expected_refused_partition(krabka_ids::PartitionIndex(0), error_code)
}

/// The record set of a row that carries nothing, as a client decodes it.
fn no_records() -> RecordsPayload { RecordsPayload::Legacy(Bytes::new()) }

fn expected(case: Case, label: String, topic: TopicRef<'_>, batch: RecordBatch) -> Outcome {
    let (from_start, from_log_end, high_watermark) = match case.expect {
        // Kafka reports the leader's high watermark to a follower, not its log
        // end. The first fetch reports node 2 at offset 0, which holds the
        // high watermark at 0 while it reads both records. The second reports
        // node 2 at the log end. `Partition.fetchRecords` reads before it
        // records that position, so the second answer still carries 0, and
        // only the partition's high watermark moves to the log end.
        Expect::FollowerRead => (
            super::test_support::expected_read()
                .watermark(0)
                .records(RecordsPayload::V2(vec![batch]))
                .call(),
            super::test_support::expected_read()
                .watermark(0)
                .records(no_records())
                .call(),
            Offset(LOG_END),
        ),
        Expect::Refused(error_code) => (refused(error_code), refused(error_code), Offset(0)),
        Expect::ConsumerRead => (
            super::test_support::expected_read()
                .watermark(0)
                .records(no_records())
                .call(),
            super::test_support::expected_read()
                .watermark(0)
                .records(no_records())
                .call(),
            Offset(0),
        ),
    };
    Outcome {
        case: label,
        from_start: response(case.version, topic, from_start),
        from_log_end: response(case.version, topic, from_log_end),
        high_watermark,
    }
}

#[tokio::test]
async fn follower_fetch_needs_cluster_action() {
    let max_version = krabka_protocol::owned::fetch_request::MAX_VERSION;
    let mut cases = Vec::new();
    for version in [FetchApiVersion(12), FetchApiVersion(max_version)] {
        cases.extend([
            Case {
                version,
                caller: Caller::TopicReader,
                sender: Sender::Follower,
                expect: Expect::Refused(KafkaErrorCode(codes::TOPIC_AUTHORIZATION_FAILED)),
            },
            Case {
                version,
                caller: Caller::Stranger,
                sender: Sender::Follower,
                expect: Expect::Refused(KafkaErrorCode(codes::TOPIC_AUTHORIZATION_FAILED)),
            },
            Case {
                version,
                caller: Caller::Replicator,
                sender: Sender::Follower,
                expect: Expect::FollowerRead,
            },
            Case {
                version,
                caller: Caller::TopicReader,
                sender: Sender::RackAwareConsumer,
                expect: Expect::ConsumerRead,
            },
            Case {
                version,
                caller: Caller::Replicator,
                sender: Sender::RackAwareConsumer,
                expect: Expect::Refused(KafkaErrorCode(codes::TOPIC_AUTHORIZATION_FAILED)),
            },
        ]);
    }

    let StartedBroker { broker, dir: _dir } = start().await;
    let CaseOutcomes {
        actual,
        expected: want,
    } = collect_topic_cases("replicated", cases, async |row| {
        let topic_id = uuid::Uuid::from_u128(row.index + 1);
        let wire_id = WireUuid(topic_id.into_bytes());
        let SeededPartition { partition, batch } =
            replicated_partition(&broker, &row.topic, topic_id).await;
        let topic = TopicRef {
            name: row.topic.as_str(),
            id: wire_id,
        };
        let fetch_setup = FetchRequestSetup {
            version: row.case.version,
            sender: row.case.sender,
            topic,
            ..Default::default()
        };
        let request_at = |offset| {
            request()
                .version(fetch_setup.version)
                .sender(fetch_setup.sender)
                .topic(fetch_setup.topic)
                .offset(offset)
                .call()
        };

        let from_start = fetch(
            &broker,
            row.case.version,
            row.case.caller,
            &request_at(fetch_setup.offset),
        )
        .await;
        let from_log_end = fetch(
            &broker,
            row.case.version,
            row.case.caller,
            &request_at(Offset(LOG_END)),
        )
        .await;
        CaseOutcomes {
            actual: Outcome {
                case: row.label.clone(),
                from_start,
                from_log_end,
                high_watermark: partition.high_watermark().await,
            },
            expected: expected(row.case, row.label, topic, batch),
        }
    })
    .await;
    broker.shutdown().await;

    assert!(actual == want);
}
