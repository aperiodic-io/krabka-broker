//! The raw-wire drivers this suite speaks to the broker with: one
//! length-prefixed request and response exchange, and the four typed calls
//! built on it (`CreateTopics`, `AlterReplicaLogDirs`, `DescribeLogDirs`).
//!
//! Every API used here is flexible, so the request header always carries its
//! tagged-fields byte and the response header always has one to strip. The
//! protocol versions the suite pins live here too, next to the encoders that
//! read them.

use {
    crate::support::{
        configs::{
            ConfigOperation, IncrementalConfigsSetup, incremental_config, incremental_request,
            incremental_resource,
        },
        wire::ApiRequestSetup,
    },
    bytes::BytesMut,
    krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            alter_replica_log_dirs_request::{
                AlterReplicaLogDir, AlterReplicaLogDirTopic, AlterReplicaLogDirsRequest,
            },
            alter_replica_log_dirs_response::AlterReplicaLogDirsResponse,
            describe_log_dirs_request::DescribeLogDirsRequest,
            describe_log_dirs_response::DescribeLogDirsResponse,
        },
    },
    std::net::SocketAddr,
    tokio::net::TcpStream,
};

pub(crate) const CLIENT_ID: &str = "krabka-arld-test";
const ALTER_VERSION: i16 = 2;
const DESCRIBE_VERSION: i16 = 4;

// Flexible headers and correlation ID 1 for every request in this suite.
crate::flexible_round_trip_fixture!(round_trip, CLIENT_ID, 1);

async fn flexible_exchange<Q: Encode, R: for<'de> Decode<'de>>(
    stream: &mut TcpStream,
    request: &Q,
    setup: ApiRequestSetup,
) -> R {
    let mut body = BytesMut::new();
    request.encode(&mut body, setup.version.0).unwrap();
    let response = round_trip(stream, setup, &body).await.unwrap();
    R::decode(&mut &response[..], setup.version.0).unwrap()
}

pub(crate) async fn alter_replica_log_dirs(
    addr: SocketAddr,
    target_dir: &std::path::Path,
    topic: &str,
    partitions: Vec<i32>,
) -> AlterReplicaLogDirsResponse {
    let req = AlterReplicaLogDirsRequest {
        dirs: vec![AlterReplicaLogDir {
            path: target_dir.to_string_lossy().to_string(),
            topics: vec![AlterReplicaLogDirTopic {
                name: topic.to_string(),
                partitions,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.unwrap();
    flexible_exchange(&mut stream, &req, ApiRequestSetup {
        api_key: WireApiKey(34),
        version: WireApiVersion(ALTER_VERSION),
    })
    .await
}

pub(crate) async fn describe_log_dirs(addr: SocketAddr) -> DescribeLogDirsResponse {
    describe_log_dirs_at(addr, DESCRIBE_VERSION).await
}

/// `DescribeLogDirs` for every partition, at `version`.
pub(crate) async fn describe_log_dirs_at(
    addr: SocketAddr,
    version: i16,
) -> DescribeLogDirsResponse {
    let req = DescribeLogDirsRequest {
        topics: None,
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.unwrap();
    flexible_exchange(&mut stream, &req, ApiRequestSetup {
        api_key: WireApiKey(35),
        version: WireApiVersion(version),
    })
    .await
}

/// `IncrementalAlterConfigs` v1 that SETs one key on broker `broker`, and the
/// per-resource `(error_code, error_message)` it answers.
pub(crate) async fn set_broker_config(
    addr: SocketAddr,
    broker: i32,
    name: &str,
    value: &str,
) -> (i16, Option<String>) {
    use krabka_protocol::owned::incremental_alter_configs_response::IncrementalAlterConfigsResponse;
    let version: i16 = 1;
    let req = incremental_request(IncrementalConfigsSetup {
        resources: vec![incremental_resource(4, broker.to_string(), vec![
            incremental_config(
                name.to_string(),
                Some(value.to_string()),
                ConfigOperation::Set,
            ),
        ])],
        ..Default::default()
    });
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp: IncrementalAlterConfigsResponse =
        flexible_exchange(&mut stream, &req, ApiRequestSetup {
            api_key: WireApiKey(44),
            version: WireApiVersion(version),
        })
        .await;
    (
        resp.responses[0].error_code,
        resp.responses[0].error_message.clone(),
    )
}
