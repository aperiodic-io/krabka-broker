//! Fixtures shared by the controller-listener unit tests: an in-process
//! [`KraftController`] over a temporary directory, the voter records it is
//! opened with, and the waits that let a test observe an elected leader.

use krabka_metadata::NodeId;
use krabka_units::prelude::{Time, TimeExt as _, millis, secs};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::{error::RaftError, kraft::KraftController};

/// Read a complete length-prefixed listener response.
pub(super) async fn read_frame<R: AsyncReadExt + Unpin>(stream: &mut R) -> Vec<u8> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.expect("response length");
    let mut frame = vec![0u8; usize::try_from(i32::from_be_bytes(len)).unwrap()];
    stream.read_exact(&mut frame).await.expect("response frame");
    frame
}

/// Election timeout for the in-test engines: short, so a single voter wins
/// immediately.
const TEST_ELECTION_TIMEOUT: Time = millis(50);

/// How long a test waits for a leader to appear.
const TEST_LEADER_DEADLINE: Time = secs(5);

pub(super) fn voter(
    id: u64,
    endpoints: Vec<krabka_metadata::VoterEndpoint>,
) -> krabka_metadata::Voter {
    krabka_metadata::Voter {
        id: NodeId(id),
        directory_id: Uuid::from_u128(u128::from(id)),
        endpoints,
        kraft_version: krabka_metadata::KRaftVersionRange::default(),
    }
}

pub(super) fn controller_endpoint(host: &str, port: u16) -> krabka_metadata::VoterEndpoint {
    krabka_metadata::VoterEndpoint {
        name: "CONTROLLER".into(),
        host: host.into(),
        port,
    }
}

pub(super) fn test_engine_with_voters(
    me: u64,
    voters: impl IntoIterator<Item = krabka_metadata::Voter>,
) -> (KraftController, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let ctrl = KraftController::open(
        dir.path().to_path_buf(),
        NodeId(me),
        Uuid::nil(),
        Uuid::nil(),
        krabka_metadata::VoterSet::from_voters(voters),
        TEST_ELECTION_TIMEOUT,
        None,
        crate::ControllerFetchMissLimit::default(),
        crate::MetadataRaftCommandQueueCapacity::default(),
        crate::MetadataRaftFetchMax::default(),
        std::sync::Arc::new(crate::kraft::NullPeerSender),
        0,
        krabka_units::prelude::bytes(0),
        millis(0),
        krabka_kraft_core::snapshot_fetch::MetadataSnapshotFetchMax::default(),
        crate::MetadataLogConfig {
            max_idle_interval: millis(0),
            ..crate::MetadataLogConfig::default()
        },
        crate::kraft::Activation::default(),
    )
    .expect("open engine");
    (ctrl, dir)
}

pub(super) fn single_voter_engine() -> (KraftController, tempfile::TempDir) {
    test_engine_with_voters(
        1,
        [voter(1, vec![controller_endpoint("controller-1", 9093)])],
    )
}

pub(super) async fn wait_for_leader(engine: &KraftController) {
    let mut rx = engine.watch_leader();
    tokio::time::timeout(TEST_LEADER_DEADLINE.to_std(), rx.wait_for(Option::is_some))
        .await
        .expect("leader elected")
        .expect("leader channel open");
}

pub(super) async fn activate_dynamic_membership(engine: &KraftController) {
    tokio::time::timeout(TEST_LEADER_DEADLINE.to_std(), async {
        loop {
            match engine.finalize_kraft_version(1).await {
                Ok(_) => return,
                Err(RaftError::ReconfigInProgress) => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => panic!("activate kraft.version 1: {error}"),
            }
        }
    })
    .await
    .expect("dynamic membership activation");
}

pub(super) fn topic_record(name: &str) -> krabka_metadata::MetadataRecord {
    krabka_metadata::MetadataRecord::V1Topic(crate::test_support::single_partition_topic(
        name,
        uuid::Uuid::new_v4(),
    ))
}

/// Encodes `message` at `version`.
pub(super) fn encoded(message: &impl krabka_protocol::Encode, version: i16) -> bytes::Bytes {
    let mut body = bytes::BytesMut::new();
    message.encode(&mut body, version).expect("encode");
    body.freeze()
}

/// Decodes a `T` at `version` from the front of `bytes`.
pub(super) fn decoded<T>(bytes: &[u8], version: i16) -> T
where
    T: for<'de> krabka_protocol::Decode<'de>,
{
    let mut cursor = bytes;
    T::decode(&mut cursor, version).expect("decode")
}

/// A single partition queried through the quorum wire API.
pub(super) fn quorum_request(
    topic: &str,
    partition: i32,
) -> krabka_protocol::owned::describe_quorum_request::DescribeQuorumRequest {
    use krabka_protocol::owned::describe_quorum_request::{
        DescribeQuorumRequest, PartitionData, TopicData,
    };
    DescribeQuorumRequest {
        topics: vec![TopicData {
            topic_name: topic.to_owned(),
            partitions: vec![PartitionData {
                partition_index: partition,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}
