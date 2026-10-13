//! Auto-join mock-controller frames and the default voter-update driver.

use {
    super::AutoJoinParams,
    crate::{
        metadata_source::MetadataSource, network::client::InterBrokerClient,
        test_support::FakeMetadataSource,
    },
    krabka_protocol::{
        Encode,
        owned::api_versions_response::{ApiVersion, ApiVersionsResponse},
    },
    krabka_raft::NodeId,
    krabka_units::{millis, secs},
    std::{net::SocketAddr, sync::Arc},
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    },
};

pub(super) async fn read_frame(socket: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut length = [0u8; 4];
    socket.read_exact(&mut length).await?;
    let mut frame = vec![0u8; u32::from_be_bytes(length) as usize];
    socket.read_exact(&mut frame).await?;
    Ok(frame)
}

#[derive(Clone, Copy, Default)]
pub(super) enum ResponseHeaderMode {
    #[default]
    Legacy,
    Flexible,
}

#[derive(Clone, Copy, Default)]
pub(super) struct ResponseWireSetup {
    pub version: krabka_ids::ApiVersion,
    pub header: ResponseHeaderMode,
}

#[bon::builder]
pub(super) async fn write_response(
    socket: &mut TcpStream,
    request: &[u8],
    response: &impl Encode,
    #[builder(into, default)] version: krabka_ids::ApiVersion,
    #[builder(default)] header: ResponseHeaderMode,
) -> std::io::Result<()> {
    let setup = ResponseWireSetup { version, header };
    let mut body = bytes::BytesMut::new();
    response
        .encode(&mut body, setup.version.0)
        .expect("encode response");
    let flexible = matches!(setup.header, ResponseHeaderMode::Flexible);
    let length =
        u32::try_from(4 + usize::from(flexible) + body.len()).expect("response length fits u32");
    let mut frame = Vec::new();
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&request[4..8]);
    if flexible {
        frame.push(0);
    }
    frame.extend_from_slice(&body);
    socket.write_all(&frame).await
}

pub(super) async fn negotiate_versions(
    socket: &mut TcpStream,
    apis: &[(krabka_ids::ApiKey, krabka_ids::ApiVersion)],
) -> std::io::Result<()> {
    let request = read_frame(socket).await?;
    let response = ApiVersionsResponse {
        error_code: 0,
        api_keys: apis
            .iter()
            .map(|&(api_key, max_version)| ApiVersion {
                api_key: api_key.0,
                min_version: 0,
                max_version: max_version.0,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    write_response()
        .socket(socket)
        .request(&request)
        .response(&response)
        .call()
        .await
}

pub(super) fn voter_update_params(addr: SocketAddr, leader: Option<NodeId>) -> AutoJoinParams {
    let controller = Arc::new(
        FakeMetadataSource::builder()
            .leader(leader)
            .term(1)
            .controller_bound_addr("127.0.0.1:9093".parse().expect("addr"))
            .build(),
    );
    params(controller, vec![addr.to_string()])
}

pub(super) fn params(
    controller: Arc<dyn MetadataSource>,
    bootstrap_servers: Vec<String>,
) -> AutoJoinParams {
    AutoJoinParams {
        auto_join: true,
        retry_backoff: millis(10),
        voter_request_timeout: secs(1),
        node_id: NodeId(2),
        directory_id: uuid::Uuid::from_u128(2),
        cluster_id: None,
        bootstrap_servers,
        advertised_controller: None,
        listener_protocol: krabka_security::ListenerProtocol::Plaintext,
        inter_broker_server_name: "broker.internal".to_string(),
        controller,
        inter_broker_client: Arc::new(InterBrokerClient::new(None, None)),
    }
}
