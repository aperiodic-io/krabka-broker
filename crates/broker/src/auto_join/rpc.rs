//! One-shot Kafka RPCs against a bootstrap server's controller listener.
//!
//! Each function here dials `target` afresh (terminating TLS or SASL as the
//! listener protocol demands), encodes one request, reads one response and
//! closes the connection, mirroring `Controller::forward_submit_to`. A fresh
//! connection per attempt is what keeps the retry loops stateless.

use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        add_raft_voter_request::{self, AddRaftVoterRequest},
        add_raft_voter_response::AddRaftVoterResponse,
        remove_raft_voter_request::{self, RemoveRaftVoterRequest},
        remove_raft_voter_response::RemoveRaftVoterResponse,
        update_raft_voter_request::{self, UpdateRaftVoterRequest},
        update_raft_voter_response::UpdateRaftVoterResponse,
    },
};

pub(super) async fn send_remove_raft_voter(
    client: &crate::network::client::InterBrokerClient,
    protocol: krabka_security::ListenerProtocol,
    server_name: &str,
    target: &str,
    req: &RemoveRaftVoterRequest,
) -> Result<RemoveRaftVoterResponse, String> {
    let version = remove_raft_voter_request::MAX_VERSION;
    let mut body = BytesMut::with_capacity(req.encoded_len(version));
    req.encode(&mut body, version)
        .map_err(|error| format!("RemoveRaftVoter encode: {error}"))?;
    let response = controller_request(
        client,
        protocol,
        server_name,
        target,
        (remove_raft_voter_request::API_KEY, version),
        body.into(),
        "RemoveRaftVoter",
    )
    .await?;
    let mut cursor: &[u8] = &response;
    RemoveRaftVoterResponse::decode(&mut cursor, version)
        .map_err(|error| format!("RemoveRaftVoter decode: {error}"))
}

pub(super) async fn send_update_voter(
    client: &crate::network::client::InterBrokerClient,
    protocol: krabka_security::ListenerProtocol,
    server_name: &str,
    target: &str,
    request: &UpdateRaftVoterRequest,
) -> Result<UpdateRaftVoterResponse, String> {
    let version = update_raft_voter_request::MAX_VERSION;
    let mut body = BytesMut::with_capacity(request.encoded_len(version));
    request
        .encode(&mut body, version)
        .map_err(|error| format!("UpdateVoter encode: {error}"))?;
    let response = controller_request(
        client,
        protocol,
        server_name,
        target,
        (update_raft_voter_request::API_KEY, version),
        body.into(),
        "UpdateVoter",
    )
    .await?;
    UpdateRaftVoterResponse::decode(&mut response.as_ref(), version)
        .map_err(|error| format!("UpdateVoter decode: {error}"))
}

/// Dial `target`'s controller listener (terminating TLS / SASL as the
/// protocol demands) and send a single `AddRaftVoter` request, returning the
/// decoded response. A fresh connection per attempt mirrors
/// `Controller::forward_submit_to`.
pub(super) async fn send_add_raft_voter(
    client: &crate::network::client::InterBrokerClient,
    protocol: krabka_security::ListenerProtocol,
    server_name: &str,
    target: &str,
    req: &AddRaftVoterRequest,
) -> Result<AddRaftVoterResponse, String> {
    let version = add_raft_voter_request::MAX_VERSION;

    let mut body = BytesMut::with_capacity(req.encoded_len(version));
    req.encode(&mut body, version)
        .map_err(|e| format!("AddRaftVoter encode: {e}"))?;

    let resp_body = controller_request(
        client,
        protocol,
        server_name,
        target,
        (add_raft_voter_request::API_KEY, version),
        body.into(),
        "AddRaftVoter",
    )
    .await?;

    let mut cur: &[u8] = &resp_body;
    AddRaftVoterResponse::decode(&mut cur, version).map_err(|e| format!("AddRaftVoter decode: {e}"))
}

/// One fresh connection per attempt; close it even when the request fails.
async fn controller_request(
    client: &crate::network::client::InterBrokerClient,
    protocol: krabka_security::ListenerProtocol,
    server_name: &str,
    target: &str,
    api: (i16, i16),
    body: Bytes,
    operation: &str,
) -> Result<Bytes, String> {
    let (host, port) = split_bootstrap_server(target)?;
    let connection = client
        .connect_as_connection(
            &host,
            port,
            protocol,
            server_name,
            auto_join_connection_options(),
        )
        .await
        .map_err(|error| format!("dial {target}: {error}"))?;
    let response = connection
        .raw_request(api.0, api.1, body)
        .await
        .map_err(|error| format!("{operation} raw_request: {error}"));
    connection.close();
    response
}

/// Splits a `<host>:<port>` bootstrap server, dropping the brackets of an IPv6
/// literal so the dialer resolves the bare address.
fn split_bootstrap_server(target: &str) -> Result<(String, u16), String> {
    let (host, port) = crate::host_port::parse_host_port(target)
        .ok_or_else(|| format!("bootstrap server {target:?} must use <host>:<port>"))?;
    Ok((host.trim_matches(['[', ']']).to_owned(), port))
}

fn auto_join_connection_options() -> krabka_client_core::ConnectionOptions {
    krabka_client_core::ConnectionOptions {
        client_id: "krabka-auto-join".to_string(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_join_connection_options_uses_joiner_client_id() {
        let opts = auto_join_connection_options();

        assert2::assert!((opts.client_id) == ("krabka-auto-join"));
    }

    async fn unreachable_target() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let target = listener.local_addr().expect("local addr");
        drop(listener);
        target.to_string()
    }

    fn check_dial_refusal<R: std::fmt::Debug>(result: Result<R, String>) {
        let err = result.expect_err("closed port must not produce a successful default response");
        assert2::assert!(err.contains("dial"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn send_add_raft_voter_errors_when_target_is_unreachable() {
        let target = unreachable_target().await;

        let client = crate::network::client::InterBrokerClient::new(None, None);
        let req = AddRaftVoterRequest::default();
        check_dial_refusal(
            send_add_raft_voter(
                &client,
                krabka_security::ListenerProtocol::Plaintext,
                "broker.internal",
                &target,
                &req,
            )
            .await,
        );
    }

    #[tokio::test]
    async fn send_update_voter_errors_when_target_is_unreachable() {
        let target = unreachable_target().await;

        let client = crate::network::client::InterBrokerClient::new(None, None);
        let req = UpdateRaftVoterRequest::default();
        check_dial_refusal(
            send_update_voter(
                &client,
                krabka_security::ListenerProtocol::Plaintext,
                "broker.internal",
                &target,
                &req,
            )
            .await,
        );
    }

    #[tokio::test]
    async fn send_remove_raft_voter_errors_when_target_is_unreachable() {
        let target = unreachable_target().await;

        let client = crate::network::client::InterBrokerClient::new(None, None);
        let req = RemoveRaftVoterRequest::default();
        check_dial_refusal(
            send_remove_raft_voter(
                &client,
                krabka_security::ListenerProtocol::Plaintext,
                "broker.internal",
                &target,
                &req,
            )
            .await,
        );
    }
}
