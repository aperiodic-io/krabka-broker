//! `InterBrokerClient` outbound SASL.
//!
//! The client runs `SaslHandshake` and `SaslAuthenticate` on its own and
//! hands back a stream that still serves ordinary RPCs. The drive helper
//! here proves that with an `ApiVersions` round-trip over the post-auth
//! stream, and `gssapi` reuses it for the Kerberos path.

use std::{io, net::SocketAddr};

use krabka_broker::Broker;
use krabka_protocol::owned::{
    api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
};
use krabka_security::{ListenerProtocol, SaslMechanism};

use crate::harness::admin_plain_password;

/// Start a broker with a `SASL_PLAINTEXT` listener and one PLAIN credential,
/// then dial it with the public `InterBrokerClient` API.
///
/// The client must run `SaslHandshake` and `SaslAuthenticate` on its own, and
/// it must return a stream that the caller can keep using for normal RPCs.
/// The test proves this: it sends an `ApiVersions` request over the returned
/// stream and decodes the response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inter_broker_client_authenticates_via_plain() {
    let (_log_dir, mut cfg) = crate::support::sasl::sasl_temp_config(vec![SaslMechanism::Plain]);
    cfg.plain_credentials
        .insert("broker".to_string(), admin_plain_password());

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let client = krabka_broker::network::client::InterBrokerClient::new(
        None,
        Some(krabka_broker::config::InterBrokerCredentials::Plain {
            username: "broker".to_string(),
            password: admin_plain_password(),
        }),
    );

    let result = drive_inter_broker_client_then_apiversions(&client, addr).await;
    handle.shutdown().await;
    result.expect("InterBrokerClient PLAIN auth + ApiVersions round-trip must succeed");
}

/// Drive `InterBrokerClient::connect` and run one `ApiVersions` round-trip
/// over the post-auth stream to prove that the stream survives.
///
/// The helper works with any mechanism. It dials `localhost:<port>` over
/// `SaslPlaintext`, so a GSSAPI SPN resolves to `kafka/localhost`. It then
/// asserts that the post-auth stream works.
pub async fn drive_inter_broker_client_then_apiversions(
    client: &krabka_broker::network::client::InterBrokerClient,
    addr: SocketAddr,
) -> Result<(), io::Error> {
    let options = krabka_client_core::ConnectionOptions {
        client_id: "krabka-t16-test".to_owned(),
        ..Default::default()
    };
    let mut stream = client
        .connect(
            &addr.ip().to_string(),
            addr.port(),
            ListenerProtocol::SaslPlaintext,
            "localhost",
            &options,
        )
        .await
        .map_err(|e| io::Error::other(format!("InterBrokerClient::connect: {e}")))?;

    // Build an ApiVersions v0 request, frame it, send it through the
    // authenticated stream, decode the response. This proves (a) the
    // client returned a usable stream and (b) the broker treats the
    // stream as fully authenticated.
    let av_req = ApiVersionsRequest::default();
    let av_body = crate::kafka_wire::encode_named(&av_req, 0, "ApiVersions")?;

    let frame = crate::support::wire::request_frame(crate::support::wire::WireFrameSetup {
        correlation: crate::support::wire::CorrelationId(99),
        client_id: "krabka-t16-test",
        body: &av_body,
        capacity: Some(
            <krabka_units::ByteSize as krabka_units::convert::ByteSizeExt>::from_bytes(
                u64::try_from(16 + av_body.len()).expect("frame capacity fits u64"),
            ),
        ),
        ..Default::default()
    });

    crate::support::wire::write_frame(&mut stream, &frame, None).await?;

    let resp = crate::support::wire::read_frame(&mut stream).await?;

    // Non-flexible response: header is v0 (just corr_id).
    let _av_resp: ApiVersionsResponse =
        crate::support::wire::decode_response_frame(&resp, 0, false, "ApiVersions decode")?;
    Ok(())
}
