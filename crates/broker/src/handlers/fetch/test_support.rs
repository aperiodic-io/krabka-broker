//! Fetch wire fixtures shared by version and replica-role tests.

use {
    crate::{
        broker::BrokerHandle,
        codes::NONE,
        fetch_session::{FINAL_EPOCH, INVALID_SESSION_ID},
        handlers::test_support::StartedBroker,
        test_support::{KafkaErrorCode, encode_request, start_broker_no_audit_with},
    },
    assert2::assert,
    krabka_ids::{ApiVersion, Offset, PartitionIndex},
    krabka_log::Log,
    krabka_protocol::{
        Decode,
        owned::{fetch_request::FetchRequest, fetch_response::FetchResponse},
        records::{Record, RecordBatch},
    },
};

/// Start a broker whose follower node 2 never fetches on its own. The lag
/// window is wide enough to keep node 2 in the ISR for the whole test, so only
/// the fetches of a case move the high watermark.
pub(super) async fn start_with_idle_follower() -> StartedBroker {
    let (broker, dir) = start_broker_no_audit_with(|cfg| {
        cfg.replica_lag_time_max = krabka_units::secs(600);
    })
    .await;
    StartedBroker { broker, dir }
}

#[bon::builder]
pub(super) fn epoch_record_batch(epoch: i32) -> RecordBatch {
    RecordBatch {
        partition_leader_epoch: epoch,
        records: vec![Record::default()],
        ..Default::default()
    }
}

pub(super) fn append_epoch_batch(log: &mut Log, epoch: i32) -> i64 {
    let mut batch = epoch_record_batch().epoch(epoch).call();
    log.append(&mut batch).expect("append").0.0
}

pub(super) fn append_epoch_batches(log: &mut Log, epochs: &[i32]) {
    for &epoch in epochs {
        append_epoch_batch(log, epoch);
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct FetchWireSetup<'a> {
    #[default(ApiVersion(krabka_protocol::owned::fetch_request::MAX_VERSION))]
    pub version: ApiVersion,
    #[default("client")]
    pub user: &'a str,
    #[default("fetch-client")]
    pub client_id: &'a str,
}

#[derive(derive_more::From, derive_more::Into, Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FetchReplicaId(pub i32);
impl Default for FetchReplicaId {
    fn default() -> Self { Self(-1) }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct SessionlessFetchSetup {
    #[default(ApiVersion(krabka_protocol::owned::fetch_request::MAX_VERSION))]
    pub version: ApiVersion,
    pub replica: FetchReplicaId,
}

#[derive(Clone, Copy)]
pub(super) enum TopicRepresentation {
    Name,
    Id,
}
impl TopicRepresentation {
    pub fn for_version(version: ApiVersion) -> Self {
        if version.0 >= super::FIRST_TOPIC_ID_VERSION {
            Self::Id
        } else {
            Self::Name
        }
    }
}

/// A sessionless fetch with the follower identity in the field served by this version.
#[bon::builder]
pub(super) fn sessionless_request(
    #[builder(into, default = ApiVersion (krabka_protocol :: owned :: fetch_request :: MAX_VERSION))]
    version: ApiVersion,
    #[builder(into, default)] replica: FetchReplicaId,
) -> FetchRequest {
    use krabka_protocol::owned::fetch_request::ReplicaState;

    let setup = SessionlessFetchSetup { version, replica };
    let version = setup.version.0;
    let replica_id = setup.replica.0;
    let (replica_id, replica_state) = if version >= 15 {
        (-1, ReplicaState {
            replica_id,
            ..Default::default()
        })
    } else {
        (replica_id, ReplicaState::default())
    };
    FetchRequest {
        replica_id,
        replica_state,
        max_wait_ms: 0,
        min_bytes: 0,
        max_bytes: 1_048_576,
        session_id: INVALID_SESSION_ID,
        session_epoch: FINAL_EPOCH,
        ..Default::default()
    }
}

#[bon::builder]
pub(super) async fn fetch_wire(
    broker: &BrokerHandle,
    request: &FetchRequest,
    #[builder(into, default = ApiVersion (krabka_protocol :: owned :: fetch_request :: MAX_VERSION))]
    version: ApiVersion,
    #[builder(default = "client")] user: &'_ str,
    #[builder(default = "fetch-client")] client_id: &'_ str,
) -> FetchResponse {
    let setup = FetchWireSetup {
        version,
        user,
        client_id,
    };
    let FetchWireSetup {
        version,
        user,
        client_id,
    } = setup;
    let version = version.0;
    let shared = broker.broker_arc_for_test();
    request_identity!(
        (user, peer, context),
        crate::test_support::principal(user),
        client_id = client_id,
        address = crate::test_support::peer()
    );
    let bytes = encode_request(request, version);
    let (response, response_version) = super::handle(&shared, version, 7, &bytes, &context)
        .await
        .expect("handle fetch");
    let wire = super::encode_fetch_response(response, response_version).expect("encode response");
    let mut cursor: &[u8] = &wire;
    let decoded = FetchResponse::decode(&mut cursor, version).expect("decode response");
    assert!(cursor.is_empty(), "the decoder consumed every byte");
    decoded
}

/// Independent single-topic response model, including the version's name/id representation.
pub(super) fn expected_single_topic(
    representation: TopicRepresentation,
    topic: (&str, krabka_protocol::primitives::uuid::Uuid),
    partition: krabka_protocol::owned::fetch_response::PartitionData,
) -> FetchResponse {
    use krabka_protocol::{owned::fetch_response::FetchableTopicResponse, primitives::uuid::Uuid};
    FetchResponse {
        error_code: NONE,
        session_id: INVALID_SESSION_ID,
        responses: vec![FetchableTopicResponse {
            topic: match representation {
                TopicRepresentation::Id => String::new(),
                TopicRepresentation::Name => topic.0.to_owned(),
            },
            topic_id: match representation {
                TopicRepresentation::Id => topic.1,
                TopicRepresentation::Name => Uuid::ZERO,
            },
            partitions: vec![partition],
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub(super) fn expected_refused_partition(
    partition_index: PartitionIndex,
    error_code: KafkaErrorCode,
) -> krabka_protocol::owned::fetch_response::PartitionData {
    krabka_protocol::owned::fetch_response::PartitionData {
        partition_index: partition_index.0,
        error_code: error_code.0,
        high_watermark: -1,
        last_stable_offset: -1,
        log_start_offset: -1,
        aborted_transactions: Some(Vec::new()),
        preferred_read_replica: -1,
        records: Some(krabka_protocol::records::RecordsPayload::Legacy(
            bytes::Bytes::new(),
        )),
        ..Default::default()
    }
}

/// Default partition-zero row shared by sessionless fetch fixtures.
pub(super) fn request_partition(
    fetch_offset: Offset,
) -> krabka_protocol::owned::fetch_request::FetchPartition {
    krabka_protocol::owned::fetch_request::FetchPartition {
        partition: 0,
        fetch_offset: fetch_offset.0,
        partition_max_bytes: 1_048_576,
        ..Default::default()
    }
}

/// Independent successful read row; neither offset nor records comes from the handler response.
#[derive(krabka_macros::FieldDefaults)]
pub(super) struct ExpectedReadSetup {
    pub watermark: Offset,
    #[default(krabka_protocol::records::RecordsPayload::Legacy(bytes::Bytes::new()))]
    pub records: krabka_protocol::records::RecordsPayload,
}

#[bon::builder]
pub(super) fn expected_read(
    #[builder(into, default)] watermark: Offset,
    #[builder(default = krabka_protocol :: records :: RecordsPayload :: Legacy (bytes :: Bytes :: new ()))]
    records: krabka_protocol::records::RecordsPayload,
) -> krabka_protocol::owned::fetch_response::PartitionData {
    let setup = ExpectedReadSetup { watermark, records };
    krabka_protocol::owned::fetch_response::PartitionData {
        partition_index: 0,
        error_code: NONE,
        high_watermark: setup.watermark.0,
        last_stable_offset: setup.watermark.0,
        log_start_offset: 0,
        aborted_transactions: None,
        preferred_read_replica: -1,
        records: Some(setup.records),
        ..Default::default()
    }
}
