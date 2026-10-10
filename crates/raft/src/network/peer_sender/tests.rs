//! Behaviour tests for the real peer sender: what it puts on the wire for a
//! KIP-595 RPC, what it remembers about a peer it reached through a bootstrap
//! address, and when it reports a candidate as speaking `kraft.version`.
//!
//! The fake peer is a real `TcpListener` that parses the request header itself,
//! so the assertions are about bytes rather than about a mock.

use bytes::BufMut;
use krabka_ids::ApiVersion;
use krabka_protocol::{
    Encode,
    // The generated `ApiVersion` message struct is aliased so the
    // `krabka_ids::ApiVersion` newtype keeps the bare name this module's
    // header assertions use.
    owned::api_versions_response::{ApiVersion as WireApiVersion, ApiVersionsResponse},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;
use crate::{kraft::transport::api_key, network::dialer::PlaintextDialer};

struct TestRecordingDialer {
    options: Arc<std::sync::Mutex<Vec<ConnectionOptions>>>,
}

#[async_trait]
impl OutboundDialer for TestRecordingDialer {
    async fn dial(
        &self,
        node_id: NodeId,
        address: &str,
        options: ConnectionOptions,
    ) -> Result<krabka_client_core::Connection, krabka_client_core::ClientError> {
        self.options.lock().unwrap().push(options.clone());
        PlaintextDialer.dial(node_id, address, options).await
    }
}

fn voter_with_controller(id: NodeId, host: &str, port: u16) -> krabka_metadata::Voter {
    krabka_metadata::Voter {
        id,
        directory_id: uuid::Uuid::nil(),
        endpoints: vec![krabka_metadata::VoterEndpoint {
            name: "CONTROLLER".into(),
            host: host.into(),
            port,
        }],
        kraft_version: krabka_metadata::KRaftVersionRange::default(),
    }
}

fn voter_set_with_controller(id: NodeId, host: &str, port: u16) -> VoterSet {
    VoterSet::from_voters([voter_with_controller(id, host, port)])
}

fn recorded_sender(
    address: std::net::SocketAddr,
) -> (
    RealPeerSender,
    Arc<std::sync::Mutex<Vec<ConnectionOptions>>>,
) {
    let options = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sender = RealPeerSender::new(
        voter_set_with_controller(NodeId(2), &address.ip().to_string(), address.port()),
        &[],
        "raft-client".into(),
        Arc::new(TestRecordingDialer {
            options: options.clone(),
        }),
        krabka_client_core::ConnectionDispatchQueueCapacity::new(7).unwrap(),
        krabka_client_core::ClientFrameMax::try_from(krabka_units::kibibytes(32)).unwrap(),
    );
    (sender, options)
}

async fn answer_fetch_connection(listener: &tokio::net::TcpListener, answer: &[u8]) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let api_versions = read_frame(&mut stream).await;
    let (_, _, correlation, _, _) = parse_request_header(&api_versions);
    write_response_frame(&mut stream, correlation, false, &api_versions_response_v0()).await;
    let request = read_frame(&mut stream).await;
    let (_, _, correlation, _, _) = parse_request_header(&request);
    write_response_frame(&mut stream, correlation, true, answer).await;
}

fn api_versions_response_v0() -> Vec<u8> {
    let resp = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![
            WireApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 4,
                ..Default::default()
            },
            WireApiVersion {
                api_key: api_key::VOTE,
                min_version: 0,
                max_version: 2,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let mut buf = bytes::BytesMut::new();
    resp.encode(&mut buf, 0).unwrap();
    buf.to_vec()
}

#[test]
fn bootstrap_servers_remain_available_and_a_leader_is_addressed_by_its_announced_endpoint() {
    let sender = RealPeerSender::new(
        VoterSet::default(),
        &["controller.example:9093".into()],
        "raft-client".into(),
        Arc::new(PlaintextDialer),
        krabka_client_core::ConnectionDispatchQueueCapacity::default(),
        krabka_client_core::ClientFrameMax::default(),
    );
    assert2::assert!(sender.discovery_peers() == vec![NodeId(u64::MAX)]);
    // The bootstrap address is not the leader's: a follower behind it
    // redirects with the leader's own `NodeEndpoints` entry.
    sender.remember_leader_endpoint(NodeId(7), "leader.example:9093".into());
    assert2::assert!(
        *sender.aliases.read().expect("alias lock")
            == std::collections::BTreeMap::from([(NodeId(7), "leader.example:9093".to_string())])
    );
}

async fn read_frame(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.unwrap();
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame).await.unwrap();
    frame
}

async fn write_response_frame(
    stream: &mut tokio::net::TcpStream,
    correlation_id: i32,
    tagged_fields: bool,
    body: &[u8],
) {
    let mut frame = bytes::BytesMut::new();
    frame.put_i32(correlation_id);
    if tagged_fields {
        frame.put_u8(0);
    }
    frame.put_slice(body);

    let mut out = Vec::with_capacity(frame.len() + 4);
    out.extend_from_slice(&(u32::try_from(frame.len()).unwrap()).to_be_bytes());
    out.extend_from_slice(&frame);
    stream.write_all(&out).await.unwrap();
    stream.flush().await.unwrap();
}

fn parse_request_header(frame: &[u8]) -> (ApiKey, ApiVersion, i32, String, &[u8]) {
    assert2::assert!(frame.len() >= 10);
    let api_key = ApiKey(i16::from_be_bytes([frame[0], frame[1]]));
    let version = ApiVersion(i16::from_be_bytes([frame[2], frame[3]]));
    let correlation_id = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
    let client_len = i16::from_be_bytes([frame[8], frame[9]]);
    assert2::assert!(client_len >= 0);
    let client_start = 10;
    let client_end = client_start + usize::try_from(client_len).unwrap();
    let client_id = std::str::from_utf8(&frame[client_start..client_end])
        .unwrap()
        .to_string();
    let body_start = if frame.get(client_end) == Some(&0) {
        client_end + 1
    } else {
        client_end
    };
    (
        api_key,
        version,
        correlation_id,
        client_id,
        &frame[body_start..],
    )
}

/// The candidate's `kraft.version` support, read off its `ApiVersions`.
///
/// A candidate is admitted only when it advertises the feature *and* the
/// cluster's finalized version falls inside the range it advertises. Both
/// halves are silent alone: matching any feature name would read some
/// other feature's range, and accepting a version on either side of the
/// range would admit a voter that cannot speak the protocol in force.
#[tokio::test]
async fn probe_reports_kraft_support_only_within_the_advertised_range() {
    /// An `ApiVersions` response advertising one supported feature.
    fn response_with_feature(name: &str, min: i16, max: i16) -> Vec<u8> {
        let response = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![WireApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 4,
                ..Default::default()
            }],
            supported_features: vec![
                krabka_protocol::owned::api_versions_response::SupportedFeatureKey {
                    name: name.to_owned(),
                    min_version: min,
                    max_version: max,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        response.encode(&mut buf, 3).expect("encode api versions");
        buf.to_vec()
    }

    use krabka_protocol::{Decode, owned::api_versions_request::ApiVersionsRequest};

    const KRAFT: &str = krabka_metadata::metadata_version::KRAFT_VERSION_FEATURE;
    // (what it is, feature advertised, range, finalized version, supported?)
    let cases: [(&str, &str, i16, i16, u16, bool); 5] = [
        (
            "the version at the bottom of the range",
            KRAFT,
            1,
            3,
            1,
            true,
        ),
        ("the version inside the range", KRAFT, 1, 3, 2, true),
        ("the version at the top of the range", KRAFT, 1, 3, 3, true),
        ("a version above the range", KRAFT, 1, 3, 4, false),
        (
            "some other feature entirely",
            "metadata.version",
            1,
            3,
            2,
            false,
        ),
    ];

    for (what, feature, min, max, finalized, supported) in cases {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = response_with_feature(feature, min, max);

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // The dialer's own handshake first, then the probe's request.
            let handshake = read_frame(&mut stream).await;
            let (_, _, corr, _, _) = parse_request_header(&handshake);
            write_response_frame(&mut stream, corr, false, &api_versions_response_v0()).await;

            let probe = read_frame(&mut stream).await;
            let (key, version, corr, client_id, probe_body) = parse_request_header(&probe);
            assert2::assert!(key == ApiKey(18));
            assert2::assert!(client_id == "krabka-voter-probe");
            let mut cur = probe_body;
            let req = ApiVersionsRequest::decode(&mut cur, version.get()).expect("decode probe");
            assert2::assert!(req.client_software_name == "krabka");
            assert2::assert!(req.client_software_version == env!("CARGO_PKG_VERSION"));
            write_response_frame(&mut stream, corr, false, &body).await;
        });

        let (sender, recorded_options) = recorded_sender(addr);
        let got = PeerSender::probe_kraft_version(&sender, &addr.to_string(), finalized)
            .await
            .expect("probe");
        assert2::check!(got == supported, "{what}");
        {
            let opts = recorded_options.lock().unwrap();
            assert2::assert!(
                opts.last().unwrap().dispatch_queue_capacity
                    == krabka_client_core::ConnectionDispatchQueueCapacity::new(7).unwrap()
            );
            assert2::assert!(
                opts.last().unwrap().frame_max
                    == krabka_client_core::ClientFrameMax::try_from(krabka_units::kibibytes(32))
                        .unwrap()
            );
        }
        server.await.expect("fake peer");
    }
}

#[tokio::test]
async fn real_peer_sender_sends_expected_api_version_client_id_and_body() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (observed_tx, mut observed_rx) = tokio::sync::mpsc::channel(1);

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();

        let api_versions = read_frame(&mut stream).await;
        let (key, _version, corr, client_id, _body) = parse_request_header(&api_versions);
        assert2::assert!((key, client_id.as_str()) == (ApiKey(18), "raft-client"));
        write_response_frame(&mut stream, corr, false, &api_versions_response_v0()).await;

        let request = read_frame(&mut stream).await;
        let (key, version, corr, client_id, body) = parse_request_header(&request);
        observed_tx
            .send((key, version, client_id, bytes::Bytes::copy_from_slice(body)))
            .await
            .unwrap();
        write_response_frame(&mut stream, corr, true, b"raft-response").await;
    });

    let (sender, recorded_options) = recorded_sender(addr);
    assert2::assert!(sender.dispatch_queue_capacity.get() == 7);
    assert2::assert!(sender.frame_max.size() == krabka_units::kibibytes(32));
    let response = sender
        .send(NodeId(2), api_key::VOTE, Bytes::from_static(b"vote-body"))
        .await
        .expect("send");

    assert2::assert!(response == Bytes::from_static(b"raft-response"));
    {
        let opts = recorded_options.lock().unwrap();
        assert2::assert!(
            opts[0].dispatch_queue_capacity
                == krabka_client_core::ConnectionDispatchQueueCapacity::new(7).unwrap()
        );
        assert2::assert!(
            opts[0].frame_max
                == krabka_client_core::ClientFrameMax::try_from(krabka_units::kibibytes(32))
                    .unwrap()
        );
    }
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), observed_rx.recv())
        .await
        .expect("server observed request")
        .expect("server sent request details");
    assert2::assert!(
        observed
            == (
                ApiKey(api_key::VOTE),
                ApiVersion(2),
                "raft-client".to_string(),
                Bytes::from_static(b"vote-body"),
            )
    );

    server.await.unwrap();
}

async fn fetch_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        answer_fetch_connection(&listener, b"fetch-response").await;
    });
    (addr, server)
}

/// A controller that leaves the voter set must stay reachable. A leader that
/// appends its own removal keeps its leadership until the removal commits, and
/// the followers commit it only by fetching from that leader. Kafka's
/// followers address the leader by its announced endpoints, which do not
/// depend on the voter set.
#[tokio::test]
async fn a_peer_stays_reachable_after_it_leaves_the_voter_set() {
    // (what it is, whether the new voter set still holds the peer)
    let cases = [
        ("a voter that stays in the set", true),
        ("a voter that the new set removes", false),
    ];
    for (what, stays) in cases {
        let (addr, server) = fetch_server().await;

        let host = addr.ip().to_string();
        let leader = voter_with_controller(NodeId(1), &host, addr.port());
        // The other voter's port is never dialed.
        let follower = voter_with_controller(NodeId(2), &host, 1);
        let sender = RealPeerSender::new(
            VoterSet::from_voters([leader.clone(), follower.clone()]),
            &[],
            "raft-client".into(),
            Arc::new(PlaintextDialer),
            krabka_client_core::ConnectionDispatchQueueCapacity::default(),
            krabka_client_core::ClientFrameMax::default(),
        );
        let next = if stays {
            VoterSet::from_voters([leader, follower])
        } else {
            VoterSet::from_voters([follower])
        };
        sender.update_voters(&next);

        let response = sender
            .send(NodeId(1), api_key::FETCH, Bytes::from_static(b"fetch-body"))
            .await
            .map_err(|error| format!("{error:?}"));
        if response.is_ok() {
            server.await.expect("fake peer");
        } else {
            // No dial reached the fake peer, so it still waits to accept.
            server.abort();
        }
        assert2::check!(
            response == Ok(Bytes::from_static(b"fetch-response")),
            "{what}"
        );
    }

    // A node that no voter set and no bootstrap server ever named has no
    // address.
    let sender = RealPeerSender::new(
        VoterSet::default(),
        &[],
        "raft-client".into(),
        Arc::new(PlaintextDialer),
        krabka_client_core::ConnectionDispatchQueueCapacity::default(),
        krabka_client_core::ClientFrameMax::default(),
    );
    let response = sender
        .send(NodeId(9), api_key::FETCH, Bytes::from_static(b"fetch-body"))
        .await;
    assert2::assert!(let Err(RaftError::NotLeader { current_leader: None }) = response);

    // Updating voters from an empty set makes the new voter reachable.
    let (addr, server) = fetch_server().await;
    let host = addr.ip().to_string();
    let new_voter = voter_with_controller(NodeId(5), &host, addr.port());
    let sender = RealPeerSender::new(
        VoterSet::default(),
        &[],
        "raft-client".into(),
        Arc::new(PlaintextDialer),
        krabka_client_core::ConnectionDispatchQueueCapacity::default(),
        krabka_client_core::ClientFrameMax::default(),
    );
    sender.update_voters(&VoterSet::from_voters([new_voter]));
    let response = sender
        .send(NodeId(5), api_key::FETCH, Bytes::from_static(b"fetch-body"))
        .await;
    server.await.expect("fake peer");
    assert2::assert!(response.unwrap() == Bytes::from_static(b"fetch-response"));
}

/// A follower-to-follower link that carried nothing closes on the client's
/// idle timeout, and a vote goes out once with no retry. The sender must dial
/// again when it finds the cached connection closed, or the first vote of an
/// election that follows a quiet spell goes into a dead link.
#[tokio::test]
async fn a_send_dials_again_when_the_cached_connection_has_closed() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for answer in [&b"first"[..], &b"second"[..]] {
            answer_fetch_connection(&listener, answer).await;
            // The stream drops here: the peer closes the link.
        }
    });
    let sender = RealPeerSender::new(
        voter_set_with_controller(NodeId(2), &addr.ip().to_string(), addr.port()),
        &[],
        "raft-client".into(),
        Arc::new(PlaintextDialer),
        krabka_client_core::ConnectionDispatchQueueCapacity::default(),
        krabka_client_core::ClientFrameMax::default(),
    );

    let first = sender
        .send(NodeId(2), api_key::VOTE, Bytes::from_static(b"vote-body"))
        .await
        .expect("first send");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !sender
            .connections
            .get(&NodeId(2))
            .is_some_and(|cached| cached.is_closed())
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the client notices that the peer closed the link");
    let second = sender
        .send(NodeId(2), api_key::VOTE, Bytes::from_static(b"vote-body"))
        .await
        .expect("second send dials a fresh connection");

    assert2::assert!(
        (first, second) == (Bytes::from_static(b"first"), Bytes::from_static(b"second"))
    );
    server.await.expect("fake peer");
}
